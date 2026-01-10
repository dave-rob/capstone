from airflow.decorators import dag
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from lib.aggregate_plots import year_plot, plot_age_group, race_plot, plot_multiple_char
from lib.regression import createModel
from datetime import datetime

@dag(
    dag_id="curated_to_aggregates",
    start_date=datetime(2026, 1, 1),
    schedule="5 10 * * *",
    catchup=False,
)
def agg_pipeline():
    
    # wait_for_curated = EmptyOperator(
    #     task_id="start"
    # )

    wait_for_curated = ExternalTaskSensor(
        task_id="wait_for_raw_to_curated",
        external_dag_id="raw_to_curated_pipeline",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60,
    )

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

    build_race_char_qual_agg = SparkSubmitOperator(
        task_id="Build_Race_Characteristics_Qual_Aggregate",
        application="/opt/spark/jobs/age_race_year_qual.py",
        conn_id="spark_default",
        verbose=True
    )

    plot_year = PythonOperator(
        task_id="Plot_Year_vs_Boston_Qualifier",
        python_callable=year_plot
    )

    plot_age_group_qual = PythonOperator(
        task_id="Plot_Age_Group_vs_Boston_Qualifier",
        python_callable=plot_age_group
    )

    plot_race = PythonOperator(
        task_id="Plot_Races_Qualification_Rate",
        python_callable=race_plot
    )

    plot_multiple = PythonOperator(
        task_id="Plot_Race_Characteristics_vs_Qualification_Rate",
        python_callable=plot_multiple_char
    )

    create_regression_model = PythonOperator(
        task_id="Build_Regression_Model",
        python_callable=createModel
    )

    finish = EmptyOperator(
        task_id="finish"
    )
    
    wait_for_curated >> build_age_group_agg >> plot_age_group_qual >> finish
    wait_for_curated >> build_race_qual_agg >> plot_race >> finish
    wait_for_curated >> build_year_qual_agg >> plot_year >> finish
    wait_for_curated >> build_race_char_qual_agg >> plot_multiple >> finish
    build_race_char_qual_agg >> create_regression_model >> finish
agg = agg_pipeline()