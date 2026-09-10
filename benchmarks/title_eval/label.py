"""Drive the human labelling pass over the stratified sheet.

The labelling pass is the one step a model cannot do for itself: it is where
the ground truth comes from. This script only moves rows in and out of the
sheet — it never proposes a verdict, because a verdict it proposed would make
the judge agree with the model rather than with the person.

Two subcommands::

    uv run python benchmarks/title_eval/label.py show --batch 10
    uv run python benchmarks/title_eval/label.py record --answers <file.json>

``show`` prints the next unlabelled rows with enough context to judge them.
``record`` merges a JSON object of ``{item_id: {verdict, why, better_title}}``
back into the sheet and reports what is left.
"""

from __future__ import annotations

import argparse
import json
import pathlib

SHEET = pathlib.Path(__file__).parent / "corpus" / "v1" / "label_sheet.jsonl"

VERDICTS = {"good", "weak", "bad"}


def read_sheet() -> list[dict]:
    return [json.loads(line) for line in SHEET.read_text(encoding="utf-8").splitlines() if line.strip()]


def write_sheet(rows: list[dict]) -> None:
    SHEET.write_text(
        "".join(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n" for r in rows),
        encoding="utf-8",
    )


def is_labelled(row: dict) -> bool:
    return bool(row["label"].get("verdict"))


def cmd_show(args: argparse.Namespace) -> int:
    rows = read_sheet()
    pending = [r for r in rows if not is_labelled(r)]
    done = len(rows) - len(pending)
    print(f"labelled {done}/{len(rows)} — showing {min(args.batch, len(pending))} next\n")

    for row in pending[: args.batch]:
        topics = ", ".join(row.get("topics") or [])
        print(f"[{row['item_id']}]  {row['kind']}  ·  {row['views']} views  ·  {row['stratum']}")
        print(f"  TITLE    {row['title']}")
        if row.get("chapter_title"):
            print(f"  CHAPTER  {row['chapter_title']}")
        if topics:
            print(f"  TOPICS   {topics[:110]}")
        if row.get("resolved_name"):
            print(f"  SPEAKER  {row['resolved_name']}")
        print(f"  {row['youtube_url']}")
        print()
    return 0


def cmd_record(args: argparse.Namespace) -> int:
    answers = json.loads(args.answers.read_text(encoding="utf-8"))
    rows = read_sheet()
    index = {r["item_id"]: r for r in rows}

    unknown = [k for k in answers if k not in index]
    if unknown:
        print(f"refusing to record — unknown item ids: {unknown}")
        return 1

    bad_verdicts = {k: v.get("verdict") for k, v in answers.items() if v.get("verdict") not in VERDICTS}
    if bad_verdicts:
        print(f"refusing to record — verdict must be one of {sorted(VERDICTS)}: {bad_verdicts}")
        return 1

    missing_why = [k for k, v in answers.items() if not (v.get("why") or "").strip()]
    if missing_why:
        # The rubric is derived from `why`, so a verdict without one is a vote
        # that teaches nothing.
        print(f"refusing to record — these have no `why`: {missing_why}")
        return 1

    for item_id, answer in answers.items():
        index[item_id]["label"] = {
            "verdict": answer["verdict"],
            "why": answer["why"].strip(),
            "better_title": (answer.get("better_title") or "").strip() or None,
        }

    write_sheet(rows)
    remaining = sum(1 for r in rows if not is_labelled(r))
    print(f"recorded {len(answers)} — {len(rows) - remaining}/{len(rows)} labelled, {remaining} left")

    tally: dict[str, int] = {}
    for row in rows:
        verdict = row["label"].get("verdict")
        if verdict:
            tally[verdict] = tally.get(verdict, 0) + 1
    print(f"verdicts so far: {tally}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    show = sub.add_parser("show", help="print the next unlabelled rows")
    show.add_argument("--batch", type=int, default=10)
    show.set_defaults(func=cmd_show)

    record = sub.add_parser("record", help="merge verdicts back into the sheet")
    record.add_argument("--answers", type=pathlib.Path, required=True)
    record.set_defaults(func=cmd_record)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
