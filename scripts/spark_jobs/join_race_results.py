from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("JoinData").getOrCreate()

# Read in parquet files and verify data types
results_df = spark.read.parquet("/opt/airflow/data/staging/Results")
races_df = spark.read.parquet("/opt/airflow/data/staging/Races")

results_df = results_df.withColumn("Age", col("Age").cast("int"))
results_df.printSchema()

races_df = races_df.withColumn("Finishers", col("Finishers").cast("int"))
races_df.printSchema()

race_results_df = (
    results_df.alias("res").join(
        races_df.alias("race"),
        (races_df["Race"] == results_df["Race"]) &
        (races_df["Year"] == results_df["Year"]),
        how = "left"
    )
    .select(
        results_df["*"],
        races_df["Date"],
        races_df["City"],
        races_df["State"]
    )
)

race_results_df.show(10)

output_path = f"/opt/airflow/data/curated/Race_Results"

race_results_df.write.mode("overwrite").partitionBy("Year").parquet(output_path)

spark.stop()