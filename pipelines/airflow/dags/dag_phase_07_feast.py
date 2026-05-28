"""Airflow DAG — Phase 07: Feature Store Materialisation"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    "owner": "sentinel",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="phase_07_feast_materialisation",
    default_args=default_args,
    description="Feast apply + materialize-incremental: gold Delta → Redis",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sentinel", "feast", "phase-07"],
) as dag:

    wait_for_gold = ExternalTaskSensor(
        task_id="wait_for_phase06_gold",
        external_dag_id="phase_06_gold_features",
        external_task_id="generate_evidence",
        timeout=14400,
        poke_interval=60,
        mode="reschedule",
    )

    feast_materialise = BashOperator(
        task_id="feast_materialise",
        bash_command="cd /workspace/feature_store && python materialize.py",
        execution_timeout=timedelta(hours=2),
    )

    generate_evidence = BashOperator(
        task_id="generate_evidence",
        bash_command="python /opt/airflow/scripts/generate_evidence.py --phase 07",
    )

    wait_for_gold >> feast_materialise >> generate_evidence
