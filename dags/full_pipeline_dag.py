from airflow.decorators import dag
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(
    dag_id="full_pipeline",
    start_date=datetime(2026, 1, 7),
    catchup=False
)
def full_pipeline():
    ingest_raw_bqstandards = SparkSubmitOperator(
        task_id="Ingest_Raw_BQ_Standards",
        application="/opt/spark/jobs/ingest_bqstandards.py",
        conn_id="spark_default",
        verbose=True
    )

    ingest_raw_results = SparkSubmitOperator(
        task_id="Ingest_Raw_Results",
        application="/opt/spark/jobs/ingest_results.py",
        conn_id="spark_default",
        verbose=True
    )

    ingest_raw_races= SparkSubmitOperator(
        task_id="Ingest_Raw_Races",
        application="/opt/spark/jobs/ingest_races.py",
        conn_id="spark_default",
        verbose=True
    )

    clean_result_data= SparkSubmitOperator(
        task_id="Clean_Result_Data",
        application="/opt/spark/jobs/clean_results.py",
        conn_id="spark_default",
        verbose=True
    )

    clean_race_data= SparkSubmitOperator(
        task_id="Clean_Race_Data",
        application="/opt/spark/jobs/clean_race.py",
        conn_id="spark_default",
        verbose=True
    )

    clean_bq_standards= SparkSubmitOperator(
        task_id="Clean_BQ_Standards",
        application="/opt/spark/jobs/clean_bqstandards.py",
        conn_id="spark_default",
        verbose=True
    )

    join_race_results= SparkSubmitOperator(
        task_id="Join_Race_Results_Data",
        application="/opt/spark/jobs/join_race_results.py",
        conn_id="spark_default",
        verbose=True
    )

    create_qualified_results= SparkSubmitOperator(
        task_id="Create_Qualified_Results_Data",
        application="/opt/spark/jobs/did_qualify.py",
        conn_id="spark_default",
        verbose=True
    )
    
    ingest_raw_bqstandards >> clean_bq_standards >> create_qualified_results
    ingest_raw_results >> clean_result_data >> join_race_results >> create_qualified_results
    ingest_raw_races >> clean_race_data >> join_race_results

pipeline = full_pipeline()
