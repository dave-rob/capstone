from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, col, asc
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("YearQual").getOrCreate()

df = spark.read.parquet("/opt/airflow/data/curated/Qualified_Results")
df = df.withColumn("Qualified", col("Qualified").cast("boolean"))

df.printSchema()
df.show(10)

df_encoded = df.withColumn("Qualified_encoded", col("Qualified").cast(IntegerType()))
df_encoded.show(10)

qual_rate_df = df_encoded.groupBy(
    "Year", "2026_BQ_Entry"
).agg(
    count("*").alias("total_runners"),
    sum(col("Qualified_encoded")).alias("qualified_runners")
).orderBy(
    asc('Year')
)

qual_rate_df.show()

output_path = f"/opt/airflow/data/aggregates/YearQual"

qual_rate_df.write.mode("overwrite").parquet(output_path)

spark.stop()