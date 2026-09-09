"""Resolve the person names a title claims against the participant registry.

Rubric hard failure H2 says a name that resolves to nobody is a defect, and
that the check runs mechanically rather than by eye. This module is that
check: it extracts candidate person names from a title and matches them
against `congress_participants`, so the judge is handed a fact instead of an
opinion.

Deliberately conservative. A token only counts as an unresolved person name
when it looks like one and matches nothing — parties, institutions and place
names are excluded by list, and a candidate that matches any registry surname
resolves. Ministers are absent from the registry, so a cabinet surname is
reported as `not_in_registry` rather than as a defect: that is a coverage gap
in the source table, not a bad title.

Usage::

    uv run python benchmarks/title_eval/name_check.py \
        --participants <participants.json> --out <name_check.json>
"""

from __future__ import annotations

import argparse
import difflib
import json
import pathlib
import re
import unicodedata

# Capitalised tokens that are never a deputy's surname. Parties, chambers,
# places and the handful of common words that open a Spanish headline.
NOT_A_PERSON = {
    "el",
    "la",
    "los",
    "las",
    "un",
    "una",
    "de",
    "del",
    "en",
    "y",
    "o",
    "por",
    "para",
    "con",
    "sin",
    "que",
    "qué",
    "quién",
    "quien",
    "cómo",
    "como",
    "dónde",
    "donde",
    "cuándo",
    "es",
    "está",
    "están",
    "ustedes",
    "nosotros",
    "congreso",
    "parlamento",
    "gobierno",
    "senado",
    "españa",
    "español",
    "española",
    "españoles",
    "europa",
    "psoe",
    "pp",
    "vox",
    "sumar",
    "podemos",
    "junts",
    "erc",
    "bildu",
    "pnv",
    "bng",
    "ciudadanos",
    "unidas",
    "ministro",
    "ministra",
    "presidente",
    "presidenta",
    "vicepresidente",
    "vicepresidenta",
    "diputado",
    "diputada",
    "portavoz",
    "señor",
    "señora",
    "don",
    "doña",
    "código",
    "penal",
    "constitución",
    "hacienda",
    "interior",
    "educación",
    "sanidad",
    "vivienda",
    "justicia",
    "defensa",
    "trabajo",
    "inclusión",
    "igualdad",
    "cultura",
    "transportes",
    "agricultura",
    "industria",
    "ceuta",
    "melilla",
    "andalucía",
    "extremadura",
    "cataluña",
    "navarra",
    "madrid",
    "barcelona",
    "gaza",
    "israel",
    "marruecos",
    "ucrania",
    "otan",
    "rtve",
    "dana",
    "ley",
    "real",
    "decreto",
    "debate",
    "reforma",
    "crisis",
    "corrupción",
    "inmigración",
    "pensiones",
    "interviniente",
    "alianza",
    "atlántica",
    "sagrada",
    "familia",
    "papa",
    "estado",
    "plus",
    "ultra",
    "lgtbi",
    "guardia",
    "guardias",
    "civiles",
    "banco",
    "bruselas",
}

# Real public figures the registry may not cover (ministers are absent from
# `congress_participants`) and whose surname in a title is not a defect.
KNOWN_PUBLIC_FIGURES = {
    "marlaska",
    "grande-marlaska",
    "zapatero",
    "rajoy",
    "aznar",
    "iglesias",
    "montero",
    "díaz",
    "calviño",
    "escrivá",
    "albares",
    "bolaños",
    "robles",
    "urtasun",
    "puente",
    "torres",
    "alegría",
    "morant",
    "rego",
}

CANDIDATE = re.compile(r"\b([A-ZÁÉÍÓÚÜÑ][a-záéíóúüñ]{2,})\b")


def fold(text: str) -> str:
    """Lowercase and strip accents so 'Calvó' and 'Calvo' compare equal."""
    stripped = unicodedata.normalize("NFD", text.lower())
    return "".join(c for c in stripped if unicodedata.category(c) != "Mn")


# Both exclusion lists are compared against folded tokens, so they have to be
# folded too — otherwise "España" folds to "espana", misses the accented entry
# above, and gets reported as a near miss for the surname "Esperanza".
NOT_A_PERSON = {fold(w) for w in NOT_A_PERSON}
KNOWN_PUBLIC_FIGURES = {fold(w) for w in KNOWN_PUBLIC_FIGURES}


def registry_tokens(participants: list[dict]) -> set[str]:
    """Every individual name token in the registry, folded."""
    tokens: set[str] = set()
    for person in participants:
        for field in ("normalized_name", "display_name"):
            for token in re.split(r"[\s,]+", person.get(field) or ""):
                if len(token) > 2:
                    tokens.add(fold(token))
    return tokens


def check_title(title: str, tokens: set[str]) -> dict:
    """Classify each capitalised token against the registry.

    Asking "is this token a person's name?" cannot be answered reliably — a
    Spanish headline capitalises after every `:`, `¿` and `¡`, so ordinary
    nouns look exactly like surnames. The answerable question is the one the
    rubric actually needs: does this token look like a registry name without
    being one? A mangled name lands very close to its real spelling ("Calv"
    against "Calvo", "Eser" against "Ester"), while an ordinary noun lands
    far from every surname in the register.
    """
    resolved: list[str] = []
    mangled: list[dict] = []
    public: list[str] = []

    for match in CANDIDATE.finditer(title):
        word = match.group(1)
        folded = fold(word)
        if folded in NOT_A_PERSON or len(folded) < 4:
            continue
        if folded in tokens:
            resolved.append(word)
            continue
        if folded in KNOWN_PUBLIC_FIGURES:
            public.append(word)
            continue
        near = difflib.get_close_matches(folded, tokens, n=1, cutoff=0.8)
        if near:
            mangled.append({"written": word, "closest_registry_name": near[0]})

    return {
        "resolved": resolved,
        "not_in_registry": public,
        "near_miss": mangled,
        "names_someone": bool(resolved or public),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--participants", type=pathlib.Path, required=True)
    parser.add_argument(
        "--sheet", type=pathlib.Path, default=pathlib.Path(__file__).parent / "corpus" / "v1" / "label_sheet.jsonl"
    )
    parser.add_argument("--out", type=pathlib.Path, required=True)
    args = parser.parse_args()

    participants = json.loads(args.participants.read_text(encoding="utf-8"))
    tokens = registry_tokens(participants)

    result = {}
    for line in args.sheet.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        result[row["item_id"]] = check_title(row["title"], tokens)

    args.out.write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")

    flagged = {k: v for k, v in result.items() if v["near_miss"]}
    named = sum(1 for v in result.values() if v["names_someone"])
    print(f"registry tokens: {len(tokens)}   titles checked: {len(result)}")
    print(f"titles naming a real person: {named}")
    print(f"titles with a near-miss name (H2 defect): {len(flagged)}")
    for item_id, info in sorted(flagged.items()):
        for m in info["near_miss"]:
            print(f"  {item_id:<14} {m['written']!r} -> {m['closest_registry_name']!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
