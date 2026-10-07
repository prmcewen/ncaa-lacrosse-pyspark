"""Scoped Spark job labels for pipeline actions and their lazy dependencies."""
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)


@contextmanager
def pipeline_action(spark_context, group_id: str, description: str):
    """Label jobs launched in this scope, restoring caller labels even on failure.

    This does not materialize DataFrames: lazy dependencies inherit the label of
    the action that eventually executes them. Properties are local to the thread.
    """
    keys = ("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel")
    previous = {key: spark_context.getLocalProperty(key) for key in keys}
    logger.info("Pipeline action [%s]: %s", group_id, description)
    try:
        spark_context.setJobGroup(group_id, description)
        yield
    finally:
        for key, value in previous.items():
            spark_context.setLocalProperty(key, value)
