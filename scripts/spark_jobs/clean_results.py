from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("CleanResults").getOrCreate()

def detect_iqr_outliers(df, col):

    # Compute quartiles
    Q1, Q3 = df.approxQuantile(col, [0.25, 0.75], 0.01)
    IQR = Q3 - Q1
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR

    print(f'The lower limit is: {lower}\nThe upper limit is: {upper}')
    print(f'Dropping {df.where(F.col(col) < lower).count()} runners for being below the lower limit')
    print(f'Dropping {df.where(F.col(col) > upper).count()} runners for being above the upper limit')
    return  df.where((F.col(col) > lower) & (F.col(col) < upper))
    

df = spark.read.csv("/opt/airflow/data/staging/Results", header=True)

df = df.dropDuplicates()
print(df.count())
df.show(50)

null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
null_counts.show()

missing_age_df=df.select("*").where(col("Age Group").isNull())
missing_age_df.describe().show()

df = df.drop("Country")

#choose rows where there the Age Group column is not Null due to max Age being 1 in those rows
df = df.where(col("Age Group").isNotNull())

df_casted = df.withColumn("Finish", col("Finish").cast("int"))

no_outlier_df = detect_iqr_outliers(df_casted, "Finish")

# fastest marathon time is 1:59:40 or 7180 seconds
final_df = no_outlier_df.where(col("Finish") >= 7180)

final_df = final_df.where(
    (col("Gender") == 'M') |
    (col("Gender") == 'F') |
    (col("Gender") == 'X')
)
output_path = f"/opt/airflow/data/staging/Results"

final_df.write.mode("overwrite").partitionBy("Year").parquet(output_path)

spark.stop()