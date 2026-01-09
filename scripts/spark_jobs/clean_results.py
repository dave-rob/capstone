from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder.appName("CleanResults").getOrCreate()

df = spark.read.csv("data/staging/Results", header=True)

df = df.dropDuplicates()
print(df.count())
df.show(50)

null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
null_counts.show()

missing_age_df=df.select("*").where(col("Age Group").isNull())
missing_age_df.describe().show()

df = df.drop("Country")

#choose rows where there the Age Group column is not Null due to max Age being 1 in those rows
final_df = df.where(col("Age Group").isNotNull())
print(final_df.count())
final_df.printSchema()

output_path = f"/opt/airflow/data/staging/Results"

final_df.write.mode("overwrite").partitionBy("Year").parquet(output_path)

spark.stop()