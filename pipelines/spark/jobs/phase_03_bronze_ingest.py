"""
Phase 03 — Bronze Ingestion

Reads raw CSVs from the mounted data directory, applies explicit schemas,
adds ingestion metadata columns, and writes Delta tables to the bronze layer.

Throughput target: >= 1M transactions/minute (PRD §2.1)
"""

import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# Allow imports from shared/ when running via spark-submit
sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.spark_session import get_spark
from shared.schema_registry import IEEE_CIS_TRANSACTION_SCHEMA, IEEE_CIS_IDENTITY_SCHEMA, PAYSIM_SCHEMA
from shared.s3_utils import BRONZE_TRANSACTIONS_IEEE, BRONZE_TRANSACTIONS_PAYSIM, BRONZE_IDENTITY_IEEE
from shared.metrics_writer import write_phase_metrics


IEEE_CIS_DATA_DIR = os.environ.get("IEEE_CIS_DATA_DIR", "/workspace/data/raw/ieee_cis")
PAYSIM_DATA_DIR = os.environ.get("PAYSIM_DATA_DIR", "/workspace/data/raw/paysim")

BATCH_ID = str(uuid.uuid4())[:8]
INGEST_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")


def add_ingestion_metadata(df, source_file: str):
    return (
        df
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_batch_id", F.lit(BATCH_ID))
        .withColumn("_ingest_date", F.lit(INGEST_DATE))
    )


def ingest_ieee_cis_transactions(spark: SparkSession) -> int:
    tx_path = f"{IEEE_CIS_DATA_DIR}/train_transaction.csv"
    if not Path(tx_path).exists():
        print(f"WARNING: IEEE-CIS transactions not found at {tx_path}. Skipping.")
        return 0

    print(f"Ingesting IEEE-CIS transactions from {tx_path}...")
    start = time.time()

    df = (
        spark.read
        .schema(IEEE_CIS_TRANSACTION_SCHEMA)
        .option("header", "true")
        .option("nullValue", "")
        .option("nanValue", "NaN")
        .option("mode", "PERMISSIVE")
        .csv(tx_path)
    )

    df = add_ingestion_metadata(df, "ieee_cis_train_transaction.csv")

    row_count = df.count()
    elapsed = time.time() - start

    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy("_ingest_date")
        .save(BRONZE_TRANSACTIONS_IEEE)
    )

    throughput = row_count / (elapsed / 60)
    print(f"  IEEE-CIS transactions: {row_count:,} rows in {elapsed:.1f}s ({throughput:,.0f} rows/min)")
    return row_count


def ingest_ieee_cis_identity(spark: SparkSession) -> int:
    id_path = f"{IEEE_CIS_DATA_DIR}/train_identity.csv"
    if not Path(id_path).exists():
        print(f"WARNING: IEEE-CIS identity not found at {id_path}. Skipping.")
        return 0

    print(f"Ingesting IEEE-CIS identity from {id_path}...")

    df = (
        spark.read
        .schema(IEEE_CIS_IDENTITY_SCHEMA)
        .option("header", "true")
        .option("nullValue", "")
        .option("mode", "PERMISSIVE")
        .csv(id_path)
    )

    df = add_ingestion_metadata(df, "ieee_cis_train_identity.csv")
    row_count = df.count()

    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy("_ingest_date")
        .save(BRONZE_IDENTITY_IEEE)
    )

    print(f"  IEEE-CIS identity: {row_count:,} rows")
    return row_count


def ingest_paysim(spark: SparkSession) -> int:
    paysim_path = f"{PAYSIM_DATA_DIR}/PS_log.csv"
    if not Path(paysim_path).exists():
        # Try alternate filename
        alt_files = list(Path(PAYSIM_DATA_DIR).glob("*.csv")) if Path(PAYSIM_DATA_DIR).exists() else []
        if alt_files:
            paysim_path = str(alt_files[0])
        else:
            print(f"WARNING: PaySim not found at {PAYSIM_DATA_DIR}. Skipping.")
            return 0

    print(f"Ingesting PaySim from {paysim_path}...")
    start = time.time()

    df = (
        spark.read
        .schema(PAYSIM_SCHEMA)
        .option("header", "true")
        .option("nullValue", "")
        .option("mode", "PERMISSIVE")
        .csv(paysim_path)
    )

    df = add_ingestion_metadata(df, "paysim_PS_log.csv")

    row_count = df.count()
    elapsed = time.time() - start

    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy("_ingest_date")
        .save(BRONZE_TRANSACTIONS_PAYSIM)
    )

    throughput = row_count / (elapsed / 60)
    print(f"  PaySim: {row_count:,} rows in {elapsed:.1f}s ({throughput:,.0f} rows/min)")
    return row_count


def main():
    print("=" * 60)
    print("Phase 03 — Bronze Ingestion")
    print("=" * 60)

    spark = get_spark("sentinel-bronze-ingest", delta=True)
    job_start = time.time()

    ieee_tx_rows = ingest_ieee_cis_transactions(spark)
    ieee_id_rows = ingest_ieee_cis_identity(spark)
    paysim_rows = ingest_paysim(spark)

    total_rows = ieee_tx_rows + ieee_id_rows + paysim_rows
    total_elapsed = time.time() - job_start
    overall_throughput = total_rows / max(total_elapsed / 60, 0.01)

    print(f"\nTotal: {total_rows:,} rows in {total_elapsed:.1f}s ({overall_throughput:,.0f} rows/min)")

    metrics = {
        "prd_metrics_addressed": [
            {
                "name": "Batch pipeline throughput",
                "prd_target": ">= 1M transactions/minute",
                "measured_value": f"{overall_throughput:,.0f} rows/minute",
                "evidence_chart": "charts/ingestion_throughput.png",
                "status": "met" if overall_throughput >= 1_000_000 else "below-target",
            }
        ],
        "additional_observations": [
            f"IEEE-CIS transactions: {ieee_tx_rows:,} rows",
            f"IEEE-CIS identity: {ieee_id_rows:,} rows",
            f"PaySim: {paysim_rows:,} rows",
            f"Total elapsed: {total_elapsed:.1f}s",
            f"Batch ID: {BATCH_ID}",
        ],
        "tests_total": 3,
        "tests_passed": sum([ieee_tx_rows > 0, ieee_id_rows > 0, paysim_rows > 0]),
        "tests_failed": sum([ieee_tx_rows == 0, ieee_id_rows == 0, paysim_rows == 0]),
    }

    write_phase_metrics("03", metrics)
    print("Phase 03 complete. Run 'python scripts/generate_evidence.py --phase 03'")

    spark.stop()


if __name__ == "__main__":
    main()
