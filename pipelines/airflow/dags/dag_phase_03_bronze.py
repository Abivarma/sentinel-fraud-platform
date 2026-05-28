"""
Airflow DAG — Phase 03: Bronze Ingestion
Triggers PySpark jobs for IEEE-CIS and PaySim ingestion into Delta Lake bronze layer.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

SPARK_MASTER = "spark://spark-master:7077"
SPARK_PKGS = "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4"

default_args = {
    "owner": "sentinel",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="phase_03_bronze_ingestion",
    default_args=default_args,
    description="Bronze ingestion: IEEE-CIS + PaySim → Delta Lake",
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sentinel", "bronze", "phase-03"],
) as dag:

    check_data = BashOperator(
        task_id="check_data_files",
        bash_command=(
            "[ -f ${IEEE_CIS_DATA_DIR}/train_transaction.csv ] && "
            "echo 'IEEE-CIS found' || echo 'WARNING: IEEE-CIS missing'; "
            "[ -f ${PAYSIM_DATA_DIR}/PS_log.csv ] && "
            "echo 'PaySim found' || echo 'WARNING: PaySim missing'"
        ),
    )

    bronze_ingest = BashOperator(
        task_id="bronze_ingest_all",
        bash_command=(
            f"spark-submit "
            f"--master {SPARK_MASTER} "
            f"--packages {SPARK_PKGS} "
            f"--conf spark.hadoop.fs.s3a.endpoint=${{MINIO_ENDPOINT}} "
            f"--conf spark.hadoop.fs.s3a.access.key=${{AWS_ACCESS_KEY_ID}} "
            f"--conf spark.hadoop.fs.s3a.secret.key=${{AWS_SECRET_ACCESS_KEY}} "
            f"/opt/spark/jobs/jobs/phase_03_bronze_ingest.py"
        ),
        execution_timeout=timedelta(hours=2),
    )

    generate_evidence = BashOperator(
        task_id="generate_evidence",
        bash_command="python /opt/airflow/scripts/generate_evidence.py --phase 03",
    )

    check_data >> bronze_ingest >> generate_evidence
