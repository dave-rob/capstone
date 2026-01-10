from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="curated_to_aggregates",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    wait_for_curated = ExternalTaskSensor(
        task_id="wait_for_raw_to_curated",
        external_dag_id="full_pipeline",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60,
    )

    build_aggregates = SparkSubmitOperator(
        task_id="build_aggregates",
        application="/opt/spark/jobs/age_group_qual.py",
        conn_id="spark_default",
        verbose=True
    )

    wait_for_curated >> build_aggregates