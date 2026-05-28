"""
Phase 06b — Gold Fraud Labels

Extracts the fraud label table from silver, combining both IEEE-CIS and PaySim labels.
Produces gold/fraud_labels — the ground truth used for model training.
"""

import sys
import time
from pathlib import Path

import pyspark.sql.functions as F

sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.spark_session import get_spark
from shared.s3_utils import SILVER_TRANSACTIONS, GOLD_FRAUD_LABELS
from shared.metrics_writer import write_phase_metrics


def main():
    print("Phase 06b — Gold Fraud Labels")
    spark = get_spark("sentinel-gold-labels", delta=True)

    silver_df = spark.read.format("delta").load(SILVER_TRANSACTIONS)

    # Only labelled rows (is_fraud is not null)
    labels_df = (
        silver_df
        .filter(F.col("is_fraud").isNotNull())
        .select(
            "transaction_id",
            "customer_id",
            "transaction_ts",
            "is_fraud",
            "data_source",
            F.current_timestamp().alias("label_ts"),
        )
    )

    total = labels_df.count()
    fraud_count = labels_df.filter(F.col("is_fraud") == 1).count()
    fraud_rate = fraud_count / max(total, 1)

    labels_df.write.format("delta").mode("overwrite").partitionBy("data_source").save(GOLD_FRAUD_LABELS)

    print(f"Gold labels: {total:,} rows, {fraud_count:,} fraud ({fraud_rate:.2%})")
    spark.stop()


if __name__ == "__main__":
    main()
