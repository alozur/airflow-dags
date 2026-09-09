"""Ask the judge which of two titles is better, and measure how often it agrees.

Absolute scoring asks the judge to place a title on a 0-10 scale it has never
seen calibrated. Pairwise asks the only question the A/B loop actually needs:
did the candidate beat the baseline? People are far more consistent at that
question than at absolute scores, and so are models.

This measures whether pairwise is worth switching to, using the labels we
already have and no new human effort: pairs are drawn from the labelled set
where the human's own scores differ clearly, so the human's preferred title is
known without asking again.

Order is randomised per pair and each pair is asked twice, once in each order.
A judge that changes its mind when the options are swapped has a position bias,
and its agreement rate is luck rather than judgement — that number is reported
separately.

Usage::

    uv run python benchmarks/title_eval/pairwise.py --gap 2.0 --pairs 60
"""

from __future__ import annotations

import argparse
import concurrent.futures
import itertools
import json
import os
import pathlib
import random
import sys

HERE = pathlib.Path(__file__).parent
RUBRIC = HERE / "RUBRIC.md"
SHEET = HERE / "corpus" / "v1" / "label_sheet.jsonl"

DEFAULT_JUDGE_MODEL = "gpt-5.5"

SYSTEM = """Eres un evaluador de titulares de YouTube para un canal que publica \
vídeos del Congreso de los Diputados de España.

Te doy DOS titulares, A y B. Decides cuál funcionaría mejor para este canal \
según la rúbrica que viene abajo, y solo según ella.

Dos advertencias que suelen fallarse:

1. Nombrar a una persona NO es automáticamente mejor. Manda la notoriedad: para \
un diputado que el gran público no reconoce, el CARGO ("el Ministro de Hacienda") \
suma más que el nombre.
2. Un partido o una institución como sujeto NO es un defecto si el hecho es concreto.

Devuelves únicamente un objeto JSON:
{"winner": "A"|"B", "why": "<una frase en español>"}

RÚBRICA:
"""


def describe(row: dict) -> str:
    parts = [row["title"]]
    if row.get("chapter_title"):
        parts.append(f"   (capítulo: {row['chapter_title'][:120]})")
    return "\n".join(parts)


def ask(client, model: str, rubric: str, first: dict, second: dict) -> str | None:
    user = f"A:\n{describe(first)}\n\nB:\n{describe(second)}"
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM + rubric},
            {"role": "user", "content": user},
        ],
        response_format={"type": "json_object"},
    )
    return json.loads(response.choices[0].message.content).get("winner")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", default=DEFAULT_JUDGE_MODEL)
    parser.add_argument("--gap", type=float, default=2.0, help="minimum human score gap for a pair to count as decided")
    parser.add_argument("--pairs", type=int, default=60)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--seed", type=int, default=510)
    args = parser.parse_args()

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("OPENAI_API_KEY is not set", file=sys.stderr)
        return 1

    from openai import OpenAI

    rows = [json.loads(line) for line in SHEET.read_text(encoding="utf-8").splitlines() if line.strip()]
    labelled = [r for r in rows if r["label"].get("verdict")]

    candidates = [
        (a, b)
        for a, b in itertools.combinations(labelled, 2)
        if abs(float(a["label"]["score"]) - float(b["label"]["score"])) >= args.gap
    ]
    rng = random.Random(args.seed)
    rng.shuffle(candidates)
    pairs = candidates[: args.pairs]

    print(
        f"{len(candidates)} pairs differ by >= {args.gap}; asking about {len(pairs)}, both orders, with {args.model} …"
    )

    client = OpenAI(api_key=api_key)
    rubric = RUBRIC.read_text(encoding="utf-8")

    def judge_pair(pair):
        a, b = pair
        # Same pair asked both ways round; a stable judge answers consistently.
        forward = ask(client, args.model, rubric, a, b)
        backward = ask(client, args.model, rubric, b, a)
        better = a if float(a["label"]["score"]) > float(b["label"]["score"]) else b
        picked_forward = a if forward == "A" else b
        picked_backward = b if backward == "A" else a
        return {
            "consistent": picked_forward["item_id"] == picked_backward["item_id"],
            "correct_forward": picked_forward["item_id"] == better["item_id"],
            "correct_backward": picked_backward["item_id"] == better["item_id"],
        }

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(judge_pair, p) for p in pairs]
        for future in concurrent.futures.as_completed(futures):
            try:
                results.append(future.result())
            except Exception as exc:  # noqa: BLE001 - one bad call must not lose the run
                print(f"  pair failed: {type(exc).__name__}: {exc}", file=sys.stderr)

    n = len(results)
    if not n:
        print("no pairs completed", file=sys.stderr)
        return 1

    consistent = sum(1 for r in results if r["consistent"])
    correct = sum(r["correct_forward"] + r["correct_backward"] for r in results)
    stable_correct = sum(1 for r in results if r["consistent"] and r["correct_forward"])

    print()
    print(f"pairs judged            {n}")
    print(f"order-independent       {consistent}/{n} = {100 * consistent / n:.0f}%")
    print(f"picked the human's pick {correct}/{2 * n} = {100 * correct / (2 * n):.0f}%  (chance is 50%)")
    print(f"both consistent AND right {stable_correct}/{n} = {100 * stable_correct / n:.0f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
