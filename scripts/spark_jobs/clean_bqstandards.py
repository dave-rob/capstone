from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder.appName("CleanbQStandards").getOrCreate()

df = spark.read.csv("data/staging/BQStandards", header=True)
print(df.count)

df = df.dropDuplicates()
print(df.count())
df.show(50)

null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
null_counts.show()

output_path = f"/opt/airflow/data/staging/BQStandards"

df.write.mode("overwrite").partitionBy("Gender").parquet(output_path)

spark.stop()