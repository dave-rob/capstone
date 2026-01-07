from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IngestBQStandards").getOrCreate()

path = "./data/raw/BQStandards.csv"
full_df = spark.read.csv(path, header=True)

#full_df.printSchema()

#drop 2013BQ
df = full_df.drop("2013BQ")

#df.show(5)

output_path = "data/staging/bq_standards"

df.write.mode("append").parquet(output_path)

spark.stop()