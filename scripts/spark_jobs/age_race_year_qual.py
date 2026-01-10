from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, col, asc, desc
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("CompareDataQual").getOrCreate()

df = spark.read.parquet("data/curated/Qualified_Results")
df = df.withColumn("Qualified", col("Qualified").cast("boolean"))

df.printSchema()
df.show(10)

df_encoded = df.withColumn("Qualified_encoded", col("Qualified").cast(IntegerType()))
df_encoded.show(10)

qual_rate_df = df_encoded.groupBy(
    "Year", "2026_BQ_Entry", "Age Group",  "Race"
).agg(
    count("*").alias("total_runners"),
    sum(col("Qualified_encoded")).alias("qualified_runners")
).orderBy(
    asc('Year'),
    asc("Race"),
    asc('Age Group')
)

qual_rate_df.show()
print(qual_rate_df.count())
output_path = f"/opt/airflow/data/aggregates/RaceCharQual"

qual_rate_df.write.mode("overwrite").parquet(output_path)

spark.stop()