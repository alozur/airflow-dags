"""Compare surface title features against view counts, within one kind.

Read this as hypothesis generation, never as ground truth. Every number below
is a correlation over observational data the channel happened to publish, so
topic, thumbnail, publication date and the recommendation surface itself are
all uncontrolled. Views are also not click-through rate: the analytics
snapshots this project stores carry no impression counts, so a title that
earns few clicks and a title that was rarely shown are indistinguishable here.

Cross-kind comparison is meaningless — Shorts are distributed through a
different surface than long-form — so each kind is bucketed separately, and a
split with fewer than 20 videos on either side is reported as too thin rather
than given a ratio.

Usage::

    uv run python benchmarks/title_eval/feature_lift.py
"""

from __future__ import annotations

import re
import statistics

from analyze_observed import load

MIN_BUCKET = 20

GENERIC_LEAD = re.compile(
    r"^\s*(el|la)\s+(ministr[oa]|president[ea]|diputad[oa]|portavoz|gobierno|congreso|"
    r"parlamento|oposición|señor|señora)\b",
    re.IGNORECASE,
)
# A capitalised word that is neither the opening token nor an acronym: a rough
# proxy for "names a specific person".
PROPER_NAME = re.compile(r"(?<!^)\b[A-ZÁÉÍÓÚÑ][a-záéíóúñ]{2,}\b")

FEATURES = {
    "question mark": lambda t: "?" in t or "¿" in t,
    "emoji": lambda t: any(ord(c) > 0x2100 for c in t),
    "exclamation": lambda t: "!" in t or "¡" in t,
    "quoted speech": lambda t: any(q in t for q in ('"', "«", "“")),
    "colon hook": lambda t: ":" in t,
    "leads with generic role": lambda t: bool(GENERIC_LEAD.match(t)),
    "names a person": lambda t: bool(PROPER_NAME.search(t)),
    "opens with 'debate'": lambda t: t.strip().lower().startswith("debate"),
    "longer than 70 chars": lambda t: len(t) > 70,
}


def median_views(rows: list[dict]) -> int:
    return int(statistics.median([r["views"] for r in rows])) if rows else 0


def main() -> None:
    records = load()
    for kind in ("chapter", "turn", "short"):
        subset = [r for r in records if r["kind"] == kind]
        if len(subset) < MIN_BUCKET * 2:
            print(f"=== {kind.upper()} (n={len(subset)}) — too few videos to split ===\n")
            continue
        baseline = median_views(subset)
        print(f"=== {kind.upper()} (n={len(subset)}, median views={baseline}) ===")
        if baseline < 20:
            print("  NOTE: the median sits near zero, so every ratio below moves on a")
            print("  handful of views and none of it is signal. Reported for completeness.")
        print(f"  {'feature':<24} {'with':>5} {'median':>8} {'without':>8} {'median':>8} {'ratio':>7}")
        for name, predicate in FEATURES.items():
            yes = [r for r in subset if predicate(r["title"])]
            no = [r for r in subset if not predicate(r["title"])]
            if len(yes) < MIN_BUCKET or len(no) < MIN_BUCKET:
                print(f"  {name:<24} {len(yes):>5} {'--':>8} {len(no):>8} {'--':>8} {'thin':>7}")
                continue
            my, mn = median_views(yes), median_views(no)
            ratio = f"{my / mn:.2f}x" if mn else "n/a"
            print(f"  {name:<24} {len(yes):>5} {my:>8} {len(no):>8} {mn:>8} {ratio:>7}")
        print()


if __name__ == "__main__":
    main()
