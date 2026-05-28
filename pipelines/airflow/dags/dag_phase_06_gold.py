"""Airflow DAG — Phase 06: Gold Feature Engineering"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

SPARK_MASTER = "spark://spark-master:7077"
SPARK_PKGS = "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4"

default_args = {
    "owner": "sentinel",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

with DAG(
    dag_id="phase_06_gold_features",
    default_args=default_args,
    description="Gold feature engineering: silver → gold Delta tables",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sentinel", "gold", "phase-06"],
) as dag:

    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_phase04_silver",
        external_dag_id="phase_04_silver_cleaning",
        external_task_id="generate_evidence",
        timeout=14400,
        poke_interval=60,
        mode="reschedule",
    )

    wait_for_streaming = ExternalTaskSensor(
        task_id="wait_for_phase05_streaming",
        external_dag_id="phase_05_streaming_infra",
        external_task_id="generate_evidence",
        timeout=14400,
        poke_interval=60,
        mode="reschedule",
    )

    SPARK_CMD = (
        f"spark-submit --master {SPARK_MASTER} --packages {SPARK_PKGS} "
        f"--conf spark.hadoop.fs.s3a.endpoint=${{MINIO_ENDPOINT}} "
        f"--conf spark.hadoop.fs.s3a.access.key=${{AWS_ACCESS_KEY_ID}} "
        f"--conf spark.hadoop.fs.s3a.secret.key=${{AWS_SECRET_ACCESS_KEY}} "
    )

    gold_features = BashOperator(
        task_id="gold_features",
        bash_command=SPARK_CMD + "/opt/spark/jobs/jobs/phase_06_gold_features.py",
        execution_timeout=timedelta(hours=4),
    )

    gold_labels = BashOperator(
        task_id="gold_labels",
        bash_command=SPARK_CMD + "/opt/spark/jobs/jobs/phase_06_gold_labels.py",
        execution_timeout=timedelta(hours=1),
    )

    generate_evidence = BashOperator(
        task_id="generate_evidence",
        bash_command="python /opt/airflow/scripts/generate_evidence.py --phase 06",
    )

    [wait_for_silver, wait_for_streaming] >> gold_features >> gold_labels >> generate_evidence
