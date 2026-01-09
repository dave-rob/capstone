from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("DidQualify").getOrCreate()

bq_df = spark.read.parquet("/opt/airflow/data/staging/BQStandards")
race_result_df = spark.read.parquet("/opt/airflow/data/curated/Race_Results")

race_result_df = race_result_df.withColumn("Date", col("Date").cast("date"))
race_result_df.printSchema()

bq_df = bq_df.withColumn("2020BQ", col("2020BQ").cast("int"))
bq_df = bq_df.withColumn("2026BQ", col("2026BQ").cast("int"))
bq_df.printSchema()

# Sep 1 2024 is when entries start qualifying for the 2026 Boston Marathon
#Assumption any race after the 1st of September 2024 is going to be an Entry to the new BQ Standards
race_result_df = race_result_df.withColumn("2026_BQ_Entry", col("Date") >= "2024-09-01")
race_result_df.printSchema()

joined_df = (
    race_result_df.join(
        bq_df,
        (race_result_df["Age Group"] == bq_df["Age Group"]) &
        (race_result_df["Gender"] == bq_df["Gender"]),
        how="left"
    )  
    .select(
        race_result_df["*"],
        bq_df["2020BQ"],
        bq_df["2026BQ"]
    )
)

qualified_df = joined_df.withColumn(
    "Qualified",
    when(
        (col("2026_BQ_Entry") == False) & (col("2020BQ") > col("Finish")),
        True)
    .when(
        (col("2026_BQ_Entry") == True) & (col("2026BQ") > col("Finish")),
        True
    )
    .otherwise(False)
)

final_df = qualified_df.drop("2020BQ", "2026BQ")

final_df.show(100)

output_path = f"/opt/airflow/data/curated/Qualified_Results"

final_df.write.mode("overwrite").partitionBy("Qualified").parquet(output_path)

spark.stop()