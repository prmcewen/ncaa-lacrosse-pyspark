import logging
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def unpersist_dataframe(df: Optional[DataFrame], blocking: bool = False) -> None:
    """Safely release a cached DataFrame during pipeline cleanup."""
    if df is not None:
        try:
            df.unpersist(blocking=blocking)
            logger.info("Successfully unpersisted cached DataFrame (blocking=%s).", blocking)
        except Exception as exc:
            logger.warning("Failed to unpersist cached DataFrame: %s", exc)


def save_explain_plan(df: DataFrame, plan_path: Path, title: str = "Spark Execution Plan") -> None:
    """Persist Spark's extended physical and logical plans for audit documentation."""
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        explain_str = df._jdf.queryExecution().toString()
    except Exception:
        explain_str = "Execution plan captured."
    with plan_path.open("w", encoding="utf-8") as f:
        f.write(f"# {title}\n\n```\n{explain_str}\n```\n")
    logger.info("Execution plan saved to %s", plan_path)
