from airflow import DAG
from airflow.decorators import dag
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

@dag(
    dag_id="curated_to_aggregates",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
)
def agg_pipeline():
    # wait_for_curated = ExternalTaskSensor(
    #     task_id="wait_for_raw_to_curated",
    #     # external_dag_id="full_pipeline",
    #     # external_task_id=None,
    #     # allowed_states=["success"],
    #     # failed_states=["failed"],
    #     # mode="reschedule",
    #     # poke_interval=60,
    #     # timeout=60 * 60,
    # )

    build_age_group_agg = SparkSubmitOperator(
        task_id="Build_Age_Group_Qual_Aggregate",
        application="/opt/spark/jobs/age_group_qual.py",
        conn_id="spark_default",
        verbose=True
    )
    build_race_qual_agg = SparkSubmitOperator(
        task_id="Build_Race_Qual_Aggregate",
        application="/opt/spark/jobs/race_qual.py",
        conn_id="spark_default",
        verbose=True
    )
    build_year_qual_agg = SparkSubmitOperator(
        task_id="Build_Year_Qual_Aggregate",
        application="/opt/spark/jobs/year_qual.py",
        conn_id="spark_default",
        verbose=True
    )

    # wait_for_curated >>
    build_age_group_agg
    build_race_qual_agg
    build_year_qual_agg

agg = agg_pipeline()