from pyspark.sql import functions as F
from pyspark.sql.types import StringType


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


