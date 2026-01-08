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
    
    ingest_raw_bqstandards
    ingest_raw_results

pipeline = full_pipeline()
