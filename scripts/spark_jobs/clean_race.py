from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when
# from itertools import chain

spark = SparkSession.builder.appName("CleanRace").getOrCreate()

df = spark.read.csv("/opt/airflow/data/staging/Races", header=True)

df = df.dropDuplicates()
print(df.count())
# df.show(50)

print("Null values before cleaning:")
null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
null_counts.show()

race_city = {"Tokyo Marathon" : "Tokyo"}

no_null_city_df = df.withColumn(
    "City",
    when(col('Race') == "Tokyo Marathon", "Tokyo").otherwise(col('City'))
)

full_df = no_null_city_df.fillna('OCONUS', subset=['State'])

print("Null values after cleaning")
null_counts = full_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in full_df.columns])
null_counts.show()

output_path = f"/opt/airflow/data/staging/Races"

full_df.write.mode("overwrite").partitionBy("Year").parquet(output_path)

spark.stop()