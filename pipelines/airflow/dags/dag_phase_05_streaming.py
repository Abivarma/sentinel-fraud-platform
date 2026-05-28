"""Airflow DAG — Phase 05: Streaming Infrastructure"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

SPARK_MASTER = "spark://spark-master:7077"
SPARK_PKGS = (
    "io.delta:delta-spark_2.12:3.2.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
)

default_args = {
    "owner": "sentinel",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="phase_05_streaming_infra",
    default_args=default_args,
    description="Streaming infra: Redpanda topics + synthetic producer + Spark SS",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sentinel", "streaming", "phase-05"],
) as dag:

    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_phase03_bronze",
        external_dag_id="phase_03_bronze_ingestion",
        external_task_id="generate_evidence",
        timeout=7200,
        poke_interval=60,
        mode="reschedule",
    )

    start_streaming_job = BashOperator(
        task_id="start_streaming_job",
        bash_command=(
            f"spark-submit "
            f"--master {SPARK_MASTER} "
            f"--packages {SPARK_PKGS} "
            f"--conf spark.hadoop.fs.s3a.endpoint=${{MINIO_ENDPOINT}} "
            f"--conf spark.hadoop.fs.s3a.access.key=${{AWS_ACCESS_KEY_ID}} "
            f"--conf spark.hadoop.fs.s3a.secret.key=${{AWS_SECRET_ACCESS_KEY}} "
            f"/opt/spark/jobs/jobs/phase_05_streaming_ingest.py &"
            f"sleep ${{STREAMING_DURATION_SECONDS:-120}} && "
            f"python /workspace/streaming/consumers/latency_probe.py "
            f"--duration 60 --output /tmp/sentinel_metrics/phase_05.json"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    generate_evidence = BashOperator(
        task_id="generate_evidence",
        bash_command="python /opt/airflow/scripts/generate_evidence.py --phase 05",
    )

    wait_for_bronze >> start_streaming_job >> generate_evidence
