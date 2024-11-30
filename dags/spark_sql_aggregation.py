from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Aggregation") \
    .getOrCreate()

# Load data from Parquet files
source1 = spark.read.parquet("/opt/airflow/data/source1.parquet")
source2 = spark.read.parquet("/opt/airflow/data/source2.parquet")

# Perform aggregation
aggregated_data = source1.join(source2, "common_key") \
    .groupBy("key_column") \
    .agg({"value_column": "sum"})

# Save the aggregated data
aggregated_data.write.format("parquet").save("/opt/airflow/data/aggregated_data.parquet")

spark.stop()
