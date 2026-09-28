import re
from typing import Optional
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

SUFFIXES = {"JR", "JR.", "SR", "SR.", "II", "III", "IV", "V"}
_RE_WHITESPACE = re.compile(r"\s+")


def clean_player_name(raw_name: Optional[str]) -> Optional[str]:
    """
    Standardize player names into canonical 'First Last [Suffix]' format.
    Handles 'Last, First', comma variations with suffixes (Jr., III, etc.),
    and already standardized 'First Last'.
    """
    if raw_name is None:
        return None

    # Strip and clean internal whitespace
    name = _RE_WHITESPACE.sub(" ", str(raw_name).strip())
    if not name:
        return None

    # Handle team designations
    if name.upper() in {"TEAM", "TM", "BENCH"}:
        return name.upper()

    # Split by comma
    parts = [p.strip() for p in name.split(",") if p.strip()]

    if len(parts) == 1:
        # Already "First Last", or single name
        return parts[0]

    elif len(parts) == 2:
        part0, part1 = parts[0], parts[1]
        # Check if part1 is a suffix (e.g. "John Smith, Jr.")
        if part1.upper() in SUFFIXES:
            return f"{part0} {part1}"

        # Check if part0 contains a suffix (e.g. "Smith Jr., John")
        part0_tokens = part0.split()
        if len(part0_tokens) > 1 and part0_tokens[-1].upper() in SUFFIXES:
            last = " ".join(part0_tokens[:-1])
            suffix = part0_tokens[-1]
            first = part1
            return f"{first} {last} {suffix}"

        # Standard "Last, First"
        last = part0
        first = part1
        return f"{first} {last}"

    elif len(parts) == 3:
        # e.g., "Smith, Jr., John" or "Smith, John, Jr."
        part0, part1, part2 = parts[0], parts[1], parts[2]
        if part1.upper() in SUFFIXES:
            # "Smith, Jr., John" -> last, suffix, first
            last = part0
            suffix = part1
            first = part2
            return f"{first} {last} {suffix}"
        elif part2.upper() in SUFFIXES:
            # "Smith, John, Jr." -> last, first, suffix
            last = part0
            first = part1
            suffix = part2
            return f"{first} {last} {suffix}"
        else:
            # Fallback: first middle last
            return f"{part1} {part2} {part0}"

    return " ".join(parts)


_SUFFIX_SET = {"JR", "JR.", "SR", "SR.", "II", "III", "IV", "V"}


def clean_player_name_native(col: F.Column) -> F.Column:
    """
    Standardize player names into canonical 'First Last [Suffix]' format
    using 100% native PySpark SQL expressions (eliminating Python UDF IPC overhead).
    Optimized to eliminate redundant regex engine evaluations and Janino 64KB bytecode limits.
    """
    norm = F.regexp_replace(F.trim(col), r"\s+", " ")
    parts = F.split(norm, r",\s*")
    n = F.size(parts)
    p0 = F.trim(F.when(n >= 1, parts.getItem(0)).otherwise(F.lit("")))
    p1 = F.trim(F.when(n >= 2, parts.getItem(1)).otherwise(F.lit("")))
    p2 = F.trim(F.when(n >= 3, parts.getItem(2)).otherwise(F.lit("")))

    # Extract suffix from p0 (e.g. "Smith Jr.") using native array tokens without regex
    p0_tokens = F.split(p0, " ")
    p0_n_tokens = F.size(p0_tokens)
    p0_last_tok = F.element_at(p0_tokens, -1)
    p0_has_suf = (p0_n_tokens > 1) & F.upper(p0_last_tok).isin(_SUFFIX_SET)
    p0_base = F.array_join(F.slice(p0_tokens, 1, p0_n_tokens - 1), " ")
    p0_suf = p0_last_tok

    res_n3 = (
        F.when(F.upper(p1).isin(_SUFFIX_SET), F.concat_ws(" ", p2, p0, p1))
        .when(F.upper(p2).isin(_SUFFIX_SET), F.concat_ws(" ", p1, p0, p2))
        .otherwise(F.concat_ws(" ", p1, p2, p0))
    )

    res_n2 = (
        F.when(F.upper(p1).isin(_SUFFIX_SET), F.concat_ws(" ", p0, p1))
        .when(p0_has_suf, F.concat_ws(" ", p1, p0_base, p0_suf))
        .otherwise(F.concat_ws(" ", p1, p0))
    )

    return (
        F.when(col.isNull() | (norm == ""), F.lit(None).cast(StringType()))
        .when(F.upper(norm).isin("TEAM", "TM", "BENCH"), F.upper(norm))
        .when(n <= 1, norm)
        .when(n == 2, res_n2)
        .when(n >= 3, res_n3)
        .otherwise(norm)
    )


