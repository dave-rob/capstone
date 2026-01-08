from pyspark.sql import SparkSession

class IngestRawData():

    def __init__(self,
                        name='AppName',
                        csv = None,
                        drop = None,
                        *args, **kwargs):
        
        super(IngestRawData, self).__init__(*args, **kwargs)
        self.name = name
        self.csv = csv
        self.drop = drop

    def execute(self):
        spark = SparkSession.builder.appName(self.name).getOrCreate()

        path = f"/opt/airflow/data/raw/{self.csv}.csv"
        full_df = spark.read.csv(path, header=True)

        df = full_df.drop(*self.drop)

        output_path = f"/opt/airflow/data/staging/{self.csv}"

        if "Year" in df.columns:
            df.write.mode("overwrite").partitionBy("Year").parquet(output_path)
        else:
            df.write.mode("overwrite").parquet(output_path)

        spark.stop()