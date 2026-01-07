from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IngestResults").getOrCreate()

path = "./data/raw/Results.csv"
full_df = spark.read.csv(path, header=True)

#full_df.printSchema()

#drop name and zip code
df = full_df.drop("Name", "Zip")

#df.show(5)

output_path = "data/staging/results"

df.write.mode("append").partitionBy("year").parquet(output_path)

spark.stop()