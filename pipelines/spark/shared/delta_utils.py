"""Delta Lake utility functions: merge, optimize, vacuum."""

import os
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def merge_into_delta(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    merge_keys: list,
) -> dict:
    """Upsert source_df into Delta table at target_path. Returns operation metrics."""
    merge_condition = " AND ".join(f"target.{k} = source.{k}" for k in merge_keys)

    if DeltaTable.isDeltaTable(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)
        (
            target.alias("target")
            .merge(source_df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        history = target.history(1).collect()[0]
        return {
            "operation": "MERGE",
            "rows_inserted": history["operationMetrics"].get("numTargetRowsInserted", 0),
            "rows_updated": history["operationMetrics"].get("numTargetRowsUpdated", 0),
        }
    else:
        source_df.write.format("delta").mode("overwrite").save(target_path)
        return {"operation": "WRITE", "rows_inserted": source_df.count()}


def optimize_delta_table(
    spark: SparkSession, path: str, zorder_cols: Optional[list] = None
):
    """Run OPTIMIZE and optional ZORDER on a Delta table."""
    if zorder_cols:
        cols_str = ", ".join(zorder_cols)
        spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({cols_str})")
    else:
        spark.sql(f"OPTIMIZE delta.`{path}`")


def vacuum_delta_table(spark: SparkSession, path: str, retention_hours: int = 168):
    """Vacuum old Delta files. Default: retain 7 days."""
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS")


def get_delta_row_count(spark: SparkSession, path: str) -> int:
    return spark.read.format("delta").load(path).count()
