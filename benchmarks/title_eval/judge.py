"""Score titles against RUBRIC.md with an LLM, and measure the judge itself.

The judge only earns the right to gate a prompt change by first reproducing
the human labels. So this script has two modes and they share one code path:
``calibrate`` scores the labelled set and reports agreement against the human;
``score`` scores any set of titles and just reports the scores.

Two rules keep the measurement honest:

**The judge never sees an existing verdict.** Not the human's, not the earlier
model pass's. It gets the title and the same evidence a person had, and
nothing else. Leaking a verdict would measure obedience, not agreement.

**The judge is not the model that writes the titles.** Generation runs on
``LLM_DEFAULT`` (``gpt-5.6-luna``); a judge on that model would be scoring its
own output, and models prefer their own. The default here is a different
generation on purpose, and ``--model`` is how you re-measure that assumption.

Usage::

    uv run python benchmarks/title_eval/judge.py calibrate --name-check <file>
    uv run python benchmarks/title_eval/judge.py score --input <file.jsonl>
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import pathlib
import statistics
import sys

HERE = pathlib.Path(__file__).parent
RUBRIC = HERE / "RUBRIC.md"
SHEET = HERE / "corpus" / "v1" / "label_sheet.jsonl"

# Deliberately NOT utils.llm_config.LLM_DEFAULT — see the module docstring.
DEFAULT_JUDGE_MODEL = "gpt-5.5"

SYSTEM = """Eres un evaluador de titulares de YouTube para un canal que publica \
vídeos del Congreso de los Diputados de España.

Puntúas un titular contra la rúbrica que viene a continuación, y SOLO contra ella. \
La rúbrica se derivó de juicios humanos sobre titulares realmente publicados en este \
canal: cuando tu intuición y la rúbrica discrepen, gana la rúbrica.

Dos advertencias que la rúbrica hace explícitas y que suelen fallarse:

1. Nombrar a una persona NO es automáticamente mejor. Manda la notoriedad. Para un \
diputado que el gran público no reconoce, el CARGO ("el Ministro de Hacienda") suma \
más que el nombre, y nombrarlo es peso muerto.
2. Un partido o una institución como sujeto NO es un defecto si el hecho es concreto.

Devuelves únicamente un objeto JSON con esta forma exacta:
{"hard_failures": ["H1"|"H2"|"H3"|"H4"], "score": <número 0-10>, \
"verdict": "good"|"weak"|"bad", "why": "<una o dos frases en español>"}

Las bandas son: score >= 8 -> "good"; 5 a 7.5 -> "weak"; <= 4.5 -> "bad". \
El veredicto debe ser coherente con la puntuación.

RÚBRICA:
"""


def build_user_prompt(item: dict, name_info: dict | None) -> str:
    """Assemble the evidence a human reviewer had — and nothing more."""
    lines = [
        f"TITULAR: {item['title']}",
        f"Tipo de vídeo: {item['kind']}",
        f"Longitud: {len(item['title'])} caracteres",
    ]
    if item.get("chapter_title"):
        lines.append(f"Resumen del capítulo: {item['chapter_title']}")
    if item.get("topics"):
        lines.append(f"Temas del capítulo: {', '.join(item['topics'])}")
    if name_info:
        # A mechanical fact, not an opinion: the rubric requires H2 to be
        # checked against the registry rather than by eye.
        if name_info["resolved"]:
            lines.append(f"Nombres que SÍ resuelven en el registro: {', '.join(name_info['resolved'])}")
        if name_info["not_in_registry"]:
            lines.append(
                "Figuras públicas reales ausentes del registro (no es defecto): "
                + ", ".join(name_info["not_in_registry"])
            )
        if name_info["near_miss"]:
            pairs = ", ".join(
                f"{m['written']!r} se parece a {m['closest_registry_name']!r}" for m in name_info["near_miss"]
            )
            lines.append(f"Nombres MUTILADOS detectados contra el registro: {pairs}")
        if not name_info["names_someone"] and not name_info["near_miss"]:
            lines.append("El titular no nombra a ninguna persona del registro.")
    dupes = item.get("_duplicate_count") or 1
    if dupes > 1:
        lines.append(f"ATENCIÓN: este titular exacto se publicó en {dupes} vídeos distintos del canal.")
    else:
        lines.append("Este titular no se repite en ningún otro vídeo del canal.")
    return "\n".join(lines)


def score_one(client, model: str, rubric: str, item: dict, name_info: dict | None) -> dict:
    from openai import OpenAI  # noqa: F401  (imported for type clarity at call sites)

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM + rubric},
            {"role": "user", "content": build_user_prompt(item, name_info)},
        ],
        response_format={"type": "json_object"},
    )
    payload = json.loads(response.choices[0].message.content)
    return {
        "item_id": item["item_id"],
        "score": float(payload.get("score", 0)),
        "verdict": payload.get("verdict"),
        "hard_failures": payload.get("hard_failures") or [],
        "why": payload.get("why", ""),
    }


def run_judge(items: list[dict], name_checks: dict, model: str, workers: int) -> list[dict]:
    from openai import OpenAI

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("OPENAI_API_KEY is not set", file=sys.stderr)
        raise SystemExit(1)

    client = OpenAI(api_key=api_key)
    rubric = RUBRIC.read_text(encoding="utf-8")
    results: list[dict] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(score_one, client, model, rubric, item, name_checks.get(item["item_id"])): item
            for item in items
        }
        for future in concurrent.futures.as_completed(futures):
            item = futures[future]
            try:
                results.append(future.result())
            except Exception as exc:  # noqa: BLE001 - one bad call must not lose the run
                print(f"  {item['item_id']}: {type(exc).__name__}: {exc}", file=sys.stderr)
    return results


def load_sheet() -> list[dict]:
    return [json.loads(line) for line in SHEET.read_text(encoding="utf-8").splitlines() if line.strip()]


def duplicate_map() -> dict[str, int]:
    """How many published videos share each title, keyed by item id.

    H3 is invisible to a judge reading one title at a time — it scored the
    repeat exactly like the original until this was passed in. Counted over
    the whole observed corpus, not just the labelled slice, because a title's
    twin is usually outside the sample.
    """
    observed = HERE / "corpus" / "v1" / "observed.jsonl"
    if not observed.exists():
        return {}

    per_title: dict[str, set[str]] = {}
    for line in observed.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        title = row["baseline"].get("published_title")
        video = row["context"].get("youtube_video_id")
        if title and video:
            per_title.setdefault(title, set()).add(video)

    counts: dict[str, int] = {}
    for row in load_sheet():
        counts[row["item_id"]] = len(per_title.get(row["title"], {None}))
    return counts


def cmd_calibrate(args: argparse.Namespace) -> int:
    rows = load_sheet()
    labelled = [r for r in rows if r["label"].get("verdict")]
    if not labelled:
        print("no human labels to calibrate against", file=sys.stderr)
        return 1

    name_checks = json.loads(args.name_check.read_text(encoding="utf-8")) if args.name_check else {}
    dupes = duplicate_map()
    for row in labelled:
        row["_duplicate_count"] = dupes.get(row["item_id"], 1)
    repeated = sum(1 for r in labelled if r["_duplicate_count"] > 1)
    print(f"judging {len(labelled)} labelled titles with {args.model} …")
    print(f"  ({repeated} of them are titles republished across several videos)")
    results = run_judge(labelled, name_checks, args.model, args.workers)

    by_id = {r["item_id"]: r for r in results}
    human = {r["item_id"]: r["label"] for r in labelled}
    titles = {r["item_id"]: r["title"] for r in labelled}

    agree = 0
    deltas: list[float] = []
    misses: list[tuple] = []
    for item_id, judged in by_id.items():
        truth = human[item_id]
        if judged["verdict"] == truth["verdict"]:
            agree += 1
        else:
            misses.append(
                (
                    item_id,
                    truth["verdict"],
                    judged["verdict"],
                    truth["score"],
                    judged["score"],
                    titles[item_id],
                    judged["why"],
                )
            )
        deltas.append(judged["score"] - float(truth["score"]))

    n = len(by_id)
    within = sum(1 for d in deltas if abs(d) <= 1.0)
    print()
    print(f"judged            {n}/{len(labelled)}")
    print(f"verdict agreement {agree}/{n} = {100 * agree / n:.0f}%   (bar: 88%)")
    print(f"score within 1.0  {within}/{n} = {100 * within / n:.0f}%")
    print(f"score delta       mean {statistics.mean(deltas):+.2f}  median {statistics.median(deltas):+.2f}")
    low = sum(1 for d in deltas if d < 0)
    high = sum(1 for d in deltas if d > 0)
    print(f"judge scored LOW on {low}, HIGH on {high}")
    print()
    print(f"=== {len(misses)} disagreements ===")
    for item_id, hv, jv, hs, js, title, why in sorted(misses):
        print(f"{item_id:<14} humano {hv}/{hs} vs juez {jv}/{js}")
        print(f"   {title[:88]}")
        print(f"   juez: {why[:160]}")

    if args.out:
        args.out.write_text(json.dumps(results, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"\nwrote {args.out}")

    verdict = "USABLE" if 100 * agree / n >= 88 else "NOT USABLE — sharpen the rubric, don't swap the judge"
    print(f"\n{verdict}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    cal = sub.add_parser("calibrate", help="score the labelled set and report agreement")
    cal.add_argument("--model", default=DEFAULT_JUDGE_MODEL)
    cal.add_argument("--name-check", type=pathlib.Path)
    cal.add_argument("--workers", type=int, default=8)
    cal.add_argument("--out", type=pathlib.Path)
    cal.set_defaults(func=cmd_calibrate)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
