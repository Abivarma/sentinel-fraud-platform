"""
Airflow DAG — Phase 04: Silver Cleaning
Depends on phase_03_bronze_ingestion completing successfully.
Runs in parallel with Phase 05 (streaming infra) — no shared write destinations.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

SPARK_MASTER = "spark://spark-master:7077"
SPARK_PKGS = "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4"

default_args = {
    "owner": "sentinel",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="phase_04_silver_cleaning",
    default_args=default_args,
    description="Silver cleaning: bronze Delta → conformed silver Delta",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sentinel", "silver", "phase-04"],
) as dag:

    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_phase03_bronze",
        external_dag_id="phase_03_bronze_ingestion",
        external_task_id="generate_evidence",
        timeout=7200,
        poke_interval=60,
        mode="reschedule",
    )

    silver_clean = BashOperator(
        task_id="silver_clean",
        bash_command=(
            f"spark-submit "
            f"--master {SPARK_MASTER} "
            f"--packages {SPARK_PKGS} "
            f"--conf spark.hadoop.fs.s3a.endpoint=${{MINIO_ENDPOINT}} "
            f"--conf spark.hadoop.fs.s3a.access.key=${{AWS_ACCESS_KEY_ID}} "
            f"--conf spark.hadoop.fs.s3a.secret.key=${{AWS_SECRET_ACCESS_KEY}} "
            f"/opt/spark/jobs/jobs/phase_04_silver_clean.py"
        ),
        execution_timeout=timedelta(hours=3),
    )

    generate_evidence = BashOperator(
        task_id="generate_evidence",
        bash_command="python /opt/airflow/scripts/generate_evidence.py --phase 04",
    )

    wait_for_bronze >> silver_clean >> generate_evidence
