import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .config('spark.cassandra.connection.host', 'cassandra_db') \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logger.info("Spark connection created successfully")
    except Exception as e:
        logger.error(f"Error while creating spark connection: {e}")
    
    return s_conn

def connect_to_kafka(spark_conn, topics):
    # Reading from multiple Kafka topics
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:9092') \
            .option('subscribe', ','.join(topics)) \
            .option('startingOffsets', 'earliest') \
            .load()
        logger.info("Kafka dataframe created successfully")
    except Exception as e:
        logger.error(f"Error while connecting to Kafka: {e}")
    
    return spark_df

def create_cassandra_connection():
    try:
        # Connection to Cassandra cluster
        cluster = Cluster(['cassandra_db'])
        cas_session = cluster.connect()
        logger.info("Cassandra connection created successfully")
        return cas_session
    except Exception as e:
        logger.error(f"Error while creating Cassandra connection: {e}")
        return None

def create_keyspace_and_table(session):
    # Create Keyspace if it doesn't exist
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streaming WITH REPLICATION = {
                'class' : 'SimpleStrategy', 'replication_factor' : 3
            };
        """)
        logger.info("Keyspace created or already exists.")
        
        # Use the created keyspace
        session.set_keyspace('spark_streaming')

        # Create table for storing aggregated weather data
        session.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                latitude DOUBLE,
                longitude DOUBLE,
                generationtime_ms DOUBLE,
                timezone TEXT,
                temperature DOUBLE,
                humidity INT,
                clouds INT,
                wind_speed DOUBLE,
                city_name TEXT,
                PRIMARY KEY (latitude, longitude)
            );
        """)
        logger.info("Table created or already exists.")
    except Exception as e:
        logger.error(f"Error while creating keyspace or table: {e}")

def create_aggregation_schema():
    # Define schema for aggregating data from both topics
    schema = StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("generationtime_ms", DoubleType(), True),
        StructField("timezone", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("clouds", IntegerType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("city_name", StringType(), True)
    ])
    return schema

def process_kafka_data(spark_df):
    # Schema for `openmeteo_raw` and `openweathermap_raw`
    openmeteo_schema = StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("generationtime_ms", DoubleType(), True),
        StructField("timezone", StringType(), True),
        StructField("hourly_units", StructType([
            StructField("time", StringType(), True),
            StructField("temperature_2m", StringType(), True),
            StructField("precipitation", StringType(), True),
            StructField("cloudcover", StringType(), True)
        ]), True)
    ])
    
    openweathermap_schema = StructType([
        StructField("coord", StructType([
            StructField("lon", DoubleType(), True),
            StructField("lat", DoubleType(), True)
        ]), True),
        StructField("weather", StructType([
            StructField("id", IntegerType(), True),
            StructField("main", StringType(), True),
            StructField("description", StringType(), True),
            StructField("icon", StringType(), True)
        ]), True),
        StructField("main", StructType([
            StructField("temp", DoubleType(), True),
            StructField("humidity", IntegerType(), True)
        ]), True),
        StructField("wind", StructType([
            StructField("speed", DoubleType(), True),
            StructField("deg", IntegerType(), True)
        ]), True),
        StructField("clouds", StructType([
            StructField("all", IntegerType(), True)
        ]), True),
        StructField("sys", StructType([
            StructField("country", StringType(), True),
            StructField("sunrise", IntegerType(), True),
            StructField("sunset", IntegerType(), True)
        ]), True),
        StructField("timezone", IntegerType(), True),
        StructField("name", StringType(), True)
    ])

    # Convert JSON strings to structured data
    meteo_data = spark_df.filter(spark_df.topic == 'openmeteo_raw').selectExpr("CAST(value AS STRING)").select(from_json(col("value"), openmeteo_schema).alias("data")).select("data.*")
    weather_data = spark_df.filter(spark_df.topic == 'openweathermap_raw').selectExpr("CAST(value AS STRING)").select(from_json(col("value"), openweathermap_schema).alias("data")).select("data.*")
    # Join the data from both streams on latitude and longitude (or any common attribute)
    joined_df = meteo_data.join(weather_data, (meteo_data.latitude == weather_data.coord.lat) & (meteo_data.longitude == weather_data.coord.lon), "outer")

    # Aggregate the data for final processing
    aggregated_df = joined_df.select(
        meteo_data.latitude,
        meteo_data.longitude,
        meteo_data.generationtime_ms,
        meteo_data.timezone,
        weather_data.main.temp.alias("temperature"),
        weather_data.main.humidity.alias("humidity"),
        weather_data.clouds.all.alias("clouds"),
        weather_data.wind.speed.alias("wind_speed"),
        weather_data.name.alias("city_name")
    )
    return aggregated_df

def insert_data_into_cassandra(aggregated_df, session):
    # Write the aggregated data into Cassandra
    aggregated_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option('keyspace', 'spark_streaming') \
        .option('table', 'weather_data') \
        .option('checkpointLocation', '/tmp/checkpoint') \
        .start()

if __name__ == "__main__":
    # Create Spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Define Kafka topics
        topics = ['openmeteo_raw', 'openweathermap_raw']

        # Connect to Kafka and get the dataframe
        spark_df = connect_to_kafka(spark_conn, topics)
        
        # Create Cassandra connection
        session = create_cassandra_connection()
        
        if session is not None:
            # Create keyspace and table in Cassandra
            create_keyspace_and_table(session)

            # Process the Kafka data and create a unified dataframe
            aggregated_df = process_kafka_data(spark_df)

            # Show schema of the resulting dataframe
            logger.info("Aggregated Dataframe Schema:")
            aggregated_df.printSchema()

            # Insert aggregated data into Cassandra
            insert_data_into_cassandra(aggregated_df, session)

            # Await termination
            aggregated_df.awaitTermination()
            session.shutdown()
