"""Run a prompt variant against the live baseline and judge the two pairwise.

This is the gate. A candidate prompt is generated on exactly the inputs the
baseline saw, the two titles for each input go to the pairwise judge in both
orders, and the candidate is promoted only if it wins by more than the
measurement can explain away.

The baseline is regenerated rather than read from the database on purpose. The
stored title was produced by an older prompt, an older model and an older
sibling window, so comparing against it would measure drift as well as the
change. Generating both sides now leaves the prompt as the only difference.

Both sides run on `LLM_DEFAULT` (`gpt-5.6-luna`), the model that actually
publishes; the judge runs on a different model, for the reason RUBRIC.md gives.

Usage::

    uv run python benchmarks/title_eval/ab_test.py --variant notoriety
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import os
import pathlib
import re
import sys
import unicodedata

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from congress_videos.config.ai_prompts import (  # noqa: E402
    THUMBNAIL_TITLE_NAMELESS_INSTRUCTION,
    THUMBNAIL_TITLE_NOTORIETY_RULES,
    THUMBNAIL_TITLE_SPEAKERS_INSTRUCTION,
    THUMBNAIL_TITLE_SYSTEM_PROMPT,
    THUMBNAIL_TITLE_USER_PROMPT_TEMPLATE,
)

HERE = pathlib.Path(__file__).parent
RUBRIC = HERE / "RUBRIC.md"
REPLAY = HERE / "corpus" / "v1" / "replay.jsonl"

GENERATOR_MODEL = os.getenv("LLM_DEFAULT") or "gpt-5.6-luna"
JUDGE_MODEL = "gpt-5.5"

# The baseline is always whatever ships today. A promoted candidate becomes the
# next baseline, which is the whole point of the loop — so a variant here is a
# DELTA on the live prompt, never a fork of it.
#
# `no_notoriety` strips the rules issue #510 promoted, so that comparison stays
# re-runnable against a changed model or a grown replay set. Re-running it is
# how you find out that a rule which won last quarter no longer does.
VARIANTS: dict[str, str] = {
    "baseline": "",
    "no_notoriety": "",  # handled by STRIP_FOR, not by appending
    "curated_names": "",  # handled by relabel_speakers, not by appending
}

STRIP_FOR = {"no_notoriety": THUMBNAIL_TITLE_NOTORIETY_RULES}

# `curated_names` moves the notoriety decision out of the prompt and into the
# data: congress_participants.nickname holds how a person should be named in a
# title, and an empty value means the audience would not recognise them. A
# speaker with no curated name is not named at all — the prompt's existing
# nameless branch takes over and the party is offered as context instead.
#
# Tested here before it is wired into the upload DAG, because a labelling
# strategy that loses the A/B should never reach production code.
RELABEL_VARIANTS = {"curated_names"}

JUDGE_SYSTEM = """Eres un evaluador de titulares de YouTube para un canal que publica \
vídeos del Congreso de los Diputados de España.

Te doy DOS titulares, A y B, generados para el MISMO debate. Decides cuál funcionaría \
mejor para este canal según la rúbrica que viene abajo, y solo según ella.

Dos advertencias que suelen fallarse:

1. Nombrar a una persona NO es automáticamente mejor. Manda la notoriedad: para un \
diputado que el gran público no reconoce, el CARGO suma más que el nombre.
2. Un partido o una institución como sujeto NO es un defecto si el hecho es concreto.

Devuelves únicamente un objeto JSON:
{"winner": "A"|"B", "why": "<una frase en español>"}

RÚBRICA:
"""


def system_prompt_for(variant: str) -> str:
    """The live system prompt with this variant's delta applied."""
    prompt = THUMBNAIL_TITLE_SYSTEM_PROMPT
    strip = STRIP_FOR.get(variant)
    if strip:
        prompt = prompt.replace(strip, "")
    return prompt + VARIANTS.get(variant, "")


ROLE_LABEL = re.compile(
    r"\b(ministr[oa]|president[ea]|vicepresident[ea]|portavoz|secretari[oa]\s+de\s+estado)\b",
    re.IGNORECASE,
)

COURTESY = re.compile(r"^\s*(el\s+se\u00f1or|la\s+se\u00f1ora|se\u00f1or|se\u00f1ora|don|do\u00f1a)\s+", re.IGNORECASE)


def fold_name(text: str) -> str:
    """Lowercase, strip accents and drop the parliamentary courtesy prefix.

    Chapter key_speakers arrive in chamber register ("Se\u00f1ora Belarra"); the
    registry stores "Belarra Urteaga, Ione". Folding both to bare surname
    tokens is what lets one match the other.
    """
    without_title = COURTESY.sub("", text or "")
    decomposed = unicodedata.normalize("NFD", without_title.lower())
    return "".join(c for c in decomposed if unicodedata.category(c) != "Mn").strip()


def build_nickname_index(participants: list[dict], seed: dict[str, str]) -> dict[str, dict]:
    """Map every folded name form of a participant to their title label.

    `seed` supplies curated nicknames for slugs whose column is not yet
    populated in this environment, so the strategy can be measured before the
    migration runs anywhere.
    """
    index: dict[str, dict] = {}
    for person in participants:
        slug = person.get("slug") or ""
        entry = {
            "nickname": person.get("nickname") or seed.get(slug) or "",
            "party": person.get("party") or "",
        }
        display = person.get("display_name") or ""
        surnames = display.split(",")[0] if "," in display else display
        forms = {fold_name(display), fold_name(surnames), fold_name(person.get("normalized_name") or "")}
        # Each individual surname token too: "Se\u00f1ora Belarra" carries only one.
        forms |= {fold_name(tok) for tok in surnames.split() if len(tok) > 3}
        for form in forms:
            if form:
                index.setdefault(form, entry)
    return index


PARTY_HINT = (
    "El ponente pertenece a {parties}. No es una figura que el gran público "
    "reconozca por su nombre, así que NO lo nombres: refiérete a él por su cargo, "
    "su papel o su grupo político."
)


def relabel_speakers(speakers: list[str], nicknames: dict[str, str]) -> tuple[list[str], list[str]]:
    """Split speakers into curated names to use and parties to fall back on.

    `nicknames` maps a folded speaker name to its curated title name; a speaker
    absent from it has no curated name and must not be named.
    """
    named: list[str] = []
    parties: list[str] = []
    for speaker in speakers:
        entry = resolve_speaker(speaker, nicknames)
        if entry and entry.get("nickname"):
            named.append(entry["nickname"])
        elif ROLE_LABEL.search(speaker):
            # Already an office rather than a person — "Ministro del Interior"
            # is exactly the generic label an uncurated speaker should get, so
            # it passes through instead of being discarded.
            named.append(speaker)
        elif entry and entry.get("party"):
            parties.append(entry["party"])
    return named, sorted(set(parties))


def resolve_speaker(speaker: str, nicknames: dict[str, dict]) -> dict | None:
    """Find a speaker in the index, exactly or by surname prefix.

    Chapter key_speakers carry a partial surname ("Se\u00f1ora \u00c1lvarez de Toledo")
    while the registry holds the full one ("\u00c1lvarez de Toledo Peralta-Ramos"),
    so an exact match misses precisely the people most worth naming. The
    prefix fallback is length-guarded to keep short tokens from colliding.
    """
    folded = fold_name(speaker)
    if not folded:
        return None
    exact = nicknames.get(folded)
    if exact:
        return exact
    if len(folded) < 5:
        return None
    matches = [(k, v) for k, v in nicknames.items() if k.startswith(folded) or folded.startswith(k)]
    if not matches:
        return None
    # The most specific key wins: "alvarez de toledo" must beat the bare
    # "alvarez" indexed for a different deputy entirely. Only keys of that same
    # best length can make it ambiguous, and ambiguity is not a match — naming
    # either of two deputies who share a surname would be a guess.
    best = max(len(k) for k, _ in matches)
    finalists = [v for k, v in matches if len(k) == best]
    unique = {(f["nickname"], f["party"]) for f in finalists}
    return finalists[0] if len(unique) == 1 else None


def build_messages(item: dict, variant: str, nicknames: dict | None = None) -> list[dict]:
    """Assemble the exact prompt `generate_title` builds, under this variant."""
    inputs = item["inputs"]
    best = inputs.get("best") or {}
    user = THUMBNAIL_TITLE_USER_PROMPT_TEMPLATE.format(
        summary=inputs.get("summary") or "",
        style=best.get("style") or "",
        prompt=best.get("prompt") or "",
    )

    speakers = [s for s in (inputs.get("key_speakers") or []) if s]
    parties: list[str] = []
    if variant in RELABEL_VARIANTS:
        speakers, parties = relabel_speakers(speakers, nicknames or {})

    if speakers:
        user += "\n\n" + THUMBNAIL_TITLE_SPEAKERS_INSTRUCTION.format(speaker_list="\n".join(f"- {s}" for s in speakers))
    else:
        user += "\n\n" + THUMBNAIL_TITLE_NAMELESS_INSTRUCTION
        if parties:
            user += "\n" + PARTY_HINT.format(parties=", ".join(parties))

    return [
        {"role": "system", "content": system_prompt_for(variant)},
        {"role": "user", "content": user},
    ]


def generate(client, item: dict, variant: str, nicknames: dict | None = None) -> str | None:
    response = client.chat.completions.create(
        model=GENERATOR_MODEL,
        messages=build_messages(item, variant, nicknames),
        response_format={"type": "json_object"},
    )
    return json.loads(response.choices[0].message.content).get("title")


def judge_pair(client, rubric: str, first: str, second: str) -> str | None:
    response = client.chat.completions.create(
        model=JUDGE_MODEL,
        messages=[
            {"role": "system", "content": JUDGE_SYSTEM + rubric},
            {"role": "user", "content": f"A:\n{first}\n\nB:\n{second}"},
        ],
        response_format={"type": "json_object"},
    )
    return json.loads(response.choices[0].message.content).get("winner")


def binomial_p(wins: int, decided: int) -> float:
    """Two-sided probability of a split this lopsided if the two were equal.

    A win rate means nothing without it: 7 of 10 looks convincing and happens
    by chance about a third of the time.
    """
    if decided == 0:
        return 1.0
    tail = sum(math.comb(decided, k) for k in range(min(wins, decided - wins) + 1))
    return min(1.0, 2 * tail / (2**decided))


def report(results: list[dict]) -> None:
    wins = sum(1 for r in results if r["winner"] == "candidate")
    losses = sum(1 for r in results if r["winner"] == "baseline")
    ties = sum(1 for r in results if r["winner"] == "inconsistent")
    decided = wins + losses

    print()
    print(f"items compared     {len(results)}")
    print(f"candidate wins     {wins}")
    print(f"baseline wins      {losses}")
    print(f"order-inconsistent {ties}  (no information — judge flipped on swap)")
    if not decided:
        print("\nNO VERDICT — nothing was decided consistently")
        return

    p = binomial_p(wins, decided)
    print(f"win rate           {wins}/{decided} = {100 * wins / decided:.0f}%   p = {p:.4f}")
    if p >= 0.05:
        print("\nNO VERDICT — the split is within what chance produces; more items or a bigger change")
    elif wins > losses:
        print("\nPROMOTE — the candidate wins by more than chance explains")
    else:
        print("\nREJECT — the baseline wins by more than chance explains")


def load_nicknames(args: argparse.Namespace) -> dict[str, dict] | None:
    """Build the name index for a relabelling variant; {} for the others.

    None signals a usage error the caller should exit on.
    """
    if args.variant not in RELABEL_VARIANTS:
        return {}
    if not args.participants:
        print("--participants is required for a relabelling variant", file=sys.stderr)
        return None
    seed = json.loads(args.seed.read_text(encoding="utf-8")) if args.seed else {}
    index = build_nickname_index(json.loads(args.participants.read_text(encoding="utf-8")), seed)
    curated = sum(1 for v in index.values() if v["nickname"])
    print(f"  ({len(index)} name forms indexed; {curated} carry a curated title name)")
    return index


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", default="no_notoriety", choices=sorted(VARIANTS))
    parser.add_argument("--limit", type=int, default=0, help="0 = every replayable item")
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--out", type=pathlib.Path)
    parser.add_argument("--participants", type=pathlib.Path, help="participant registry, for relabelling variants")
    parser.add_argument("--seed", type=pathlib.Path, help="slug -> curated nickname, for envs where 052 has not run")
    args = parser.parse_args()

    if not os.getenv("OPENAI_API_KEY"):
        print("OPENAI_API_KEY is not set", file=sys.stderr)
        return 1

    from openai import OpenAI

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    rubric = RUBRIC.read_text(encoding="utf-8")

    nicknames = load_nicknames(args)
    if nicknames is None:
        return 1

    items = [json.loads(line) for line in REPLAY.read_text(encoding="utf-8").splitlines() if line.strip()]
    if args.limit:
        items = items[: args.limit]

    print(f"generating both sides on {len(items)} replayable inputs with {GENERATOR_MODEL} …")
    print(f"variant: {args.variant}")

    def run_one(item: dict) -> dict | None:
        base = generate(client, item, "baseline", nicknames)
        cand = generate(client, item, args.variant, nicknames)
        if not base or not cand:
            return None
        # Both orders: a judge that flips when the options swap has a position
        # bias, and its verdict on that pair carries no information.
        forward = judge_pair(client, rubric, base, cand)
        backward = judge_pair(client, rubric, cand, base)
        winner_forward = "candidate" if forward == "B" else "baseline"
        winner_backward = "candidate" if backward == "A" else "baseline"
        return {
            "item_id": item["item_id"],
            "baseline": base,
            "candidate": cand,
            "winner": winner_forward if winner_forward == winner_backward else "inconsistent",
        }

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(run_one, i): i for i in items}
        for future in concurrent.futures.as_completed(futures):
            item = futures[future]
            try:
                out = future.result()
                if out:
                    results.append(out)
            except Exception as exc:  # noqa: BLE001 - one bad item must not lose the run
                print(f"  {item['item_id']}: {type(exc).__name__}: {exc}", file=sys.stderr)

    report(results)

    if args.out:
        args.out.write_text(json.dumps(results, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
