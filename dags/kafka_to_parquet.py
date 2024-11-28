import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

OPENWEATHER_API_KEY = '5228ee3a150af754ec138620204936eb'
OPENMETEO_API_URL = 'https://api.open-meteo.com/v1/forecast'
OPENWEATHER_API_URL = 'http://api.openweathermap.org/data/2.5/weather'
KAFKA_SERVER = 'broker:9092'
KAFKA_TOPIC_OPENWEATHER = 'openweathermap_raw'
KAFKA_TOPIC_OPENMETEO = 'openmeteo_raw'

def create_spark_connection():
    """Create a Spark session configured with Kafka dependencies."""
    try:
        spark_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .getOrCreate()

        spark_conn.sparkContext.setLogLevel("ERROR")
        logger.info("Spark connection created successfully")
        return spark_conn

    except Exception as e:
        logger.error(f"Error while creating Spark connection: {e}")
        return None

def connect_to_kafka(spark_conn, topic):
    """Connect to Kafka and create a streaming DataFrame."""
    try:
        kafka_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', KAFKA_SERVER) \
            .option('subscribe', topic) \
            .option('startingOffsets', 'earliest') \
            .load()

        logger.info(f"Kafka dataframe for topic {topic} created successfully")
        return kafka_df

    except Exception as e:
        logger.error(f"Kafka dataframe for topic {topic} could not be created: {e}")
        return None

def transform_openweather_data(kafka_df):
    """Transform OpenWeather data using a predefined schema."""
    schema = StructType([
        StructField("weather", StringType(), True),
        StructField("main", StructType([
            StructField("temp", FloatType(), True),
            StructField("humidity", FloatType(), True)
        ]), True),
        StructField("wind", StructType([
            StructField("speed", FloatType(), True)
        ]), True),
        StructField("name", StringType(), True)
    ])

    transformed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.weather", "data.main.temp", "data.main.humidity", "data.wind.speed", "data.name")

    logger.info("Transformed OpenWeather dataframe created successfully")
    return transformed_df

def transform_openmeteo_data(kafka_df):
    """Transform OpenMeteo data using a predefined schema."""
    schema = StructType([
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("current_weather", StructType([
            StructField("temperature", FloatType(), True),
            StructField("windspeed", FloatType(), True)
        ]), True)
    ])

    transformed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.latitude", "data.longitude", "data.current_weather.temperature", "data.current_weather.windspeed")

    logger.info("Transformed OpenMeteo dataframe created successfully")
    return transformed_df

def write_to_parquet(selection_df, output_path):
    """Write the transformed DataFrame to Parquet format."""
    try:
        query = selection_df.writeStream \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation", f"/tmp/checkpoint_parquet_{output_path.split('/')[-1]}") \
            .outputMode("append") \
            .start()

        logger.info(f"Streaming query started. Writing to Parquet at {output_path}")
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Error while writing to Parquet: {e}")

def main():
    spark = create_spark_connection()

    if not spark:
        logger.error("Spark connection could not be established.")
        return

    # Consommer les données OpenWeather
    openweather_df = connect_to_kafka(spark, KAFKA_TOPIC_OPENWEATHER)
    if openweather_df:
        transformed_openweather = transform_openweather_data(openweather_df)
        openweather_query = transformed_openweather.writeStream \
            .format("parquet") \
            .option("path", "/openweather/") \
            .option("checkpointLocation", "/openweather/") \
            .outputMode("append") \
            .start()
        logger.info("OpenWeather streaming to Parquet started.")
    
    # Consommer les données OpenMeteo
    openmeteo_df = connect_to_kafka(spark, KAFKA_TOPIC_OPENMETEO)
    if openmeteo_df:
        transformed_openmeteo = transform_openmeteo_data(openmeteo_df)
        openmeteo_query = transformed_openmeteo.writeStream \
            .format("parquet") \
            .option("path", "/openmeteo/") \
            .option("checkpointLocation", "/openmeteo/") \
            .outputMode("append") \
            .start()
        logger.info("OpenMeteo streaming to Parquet started.")
    
    # Attendre que les streamings se terminent
    if openweather_df:
        openweather_query.awaitTermination()
    if openmeteo_df:
        openmeteo_query.awaitTermination()

if __name__ == "__main__":
    main()
