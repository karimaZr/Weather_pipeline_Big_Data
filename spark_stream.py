import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, LongType
from cassandra.cluster import Cluster

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streaming
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    logger.info("Keyspace created successfully")

def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1") \
            .config('spark.cassandra.connection.host', 'cassandra_db') \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logger.info("Spark connection created successfully")
    except Exception as e:
        logger.error(f"Error while creating spark connection: {e}")
    return s_conn

def create_cassandra_connection():
    try:
        cluster = Cluster(['cassandra_db'])
        cas_session = cluster.connect()
        logger.info("Cassandra connection created successfully")
        return cas_session
    except Exception as e:
        logger.error(f"Error while creating Cassandra connection: {e}")
        return None

def process_parquet_files(spark_conn):
    # Définir le schéma pour les fichiers .parquet
    schema1 = StructType([  # Schema for file 1
        StructField("coord", StructType([
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True)
        ])),
        StructField("weather", ArrayType(StructType([
            StructField("description", StringType(), True)
        ])), True),
        StructField("main", StructType([
            StructField("temp", DoubleType(), True),
            StructField("humidity", LongType(), True)
        ])),
        StructField("name", StringType(), True)

    ])

    schema2 = StructType([  # Schema for file 2
        # StructField("latitude", DoubleType(), True),
        # StructField("longitude", DoubleType(), True),
        StructField("current", StructType([
            StructField("temperature_2m", DoubleType(), True),
            StructField("relative_humidity_2m", LongType(), True)
        ])),
        StructField("city_name", StringType(), True)
    ])
    df11 = spark_conn.read.parquet("/openmeteo_data.parquet")
    df11.printSchema()
    df22 = spark_conn.read.parquet("/openweathermap_data.parquet")
    df22.printSchema()


    # Lire les fichiers .parquet avec les schémas explicites
    df1 = spark_conn.read.schema(schema1).parquet("/openweathermap_data.parquet")
    df1.show(10)
    df2 = spark_conn.read.schema(schema2).parquet("/openmeteo_data.parquet")
    df2.show(10)
    # Sélectionner et transformer les données
    df1 = df1.select(
        col("coord.lat").cast(DoubleType()).alias("latitude"),
        col("coord.lon").cast(DoubleType()).alias("longitude"),
        col("weather.description").cast(StringType()).alias("weather_description"),
        col("main.temp").cast(DoubleType()).alias("temp"),
        col("main.humidity").cast(LongType()).alias("humidity"),
        col("name").cast(StringType()).alias("city_name"),

    )
    df1.show(20)
    df2 = df2.select(
        col("current.temperature_2m").cast(DoubleType()).alias("temperature_2m"),
        col("current.relative_humidity_2m").cast(LongType()).alias("relative_humidity_2m"),
        col("city_name").cast(StringType()),

    )
    df2.show(20)
    # Joindre les deux DataFrames
    merged_df = df1.join(df2, on="city_name", how="inner")
    logger.info(f"Number of rows in merged_df: {merged_df.count()}")
    merged_df.show(10)

    return merged_df

def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streaming.weather_data (
            latitude DOUBLE,
            longitude DOUBLE,
            temperature_2m DOUBLE,
            temp DOUBLE,
            humidity DOUBLE,
            weather_description TEXT,
            city_name TEXT,
            timestamp TIMESTAMP,
            PRIMARY KEY (city_name, timestamp)
        )
    """)
    logger.info("Table created successfully")

def insert_data(session, merged_df):
    # Collect data from the merged dataframe and insert into Cassandra
    for row in merged_df.collect():
        try:
            # Insertion of data in Cassandra
            session.execute("""
                INSERT INTO spark_streaming.weather_data (latitude, longitude, temperature_2m, temp, humidity, weather_description,city_name, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s,%s, %s)
            """, (row.latitude, row.longitude, row.temperature_2m, row.temp, row.humidity, row.weather_description,row.city_name, datetime.now()))
            logger.info(f"Inserted data: {row}")

            # Optionally, you can verify the data insertion by querying it back
            verify_query = """
                SELECT * FROM spark_streaming.weather_data WHERE city_name = %s AND timestamp = %s
            """
            result = session.execute(verify_query, (row.city_name, datetime.now()))
            if result:
                logger.info(f"Data verified in Cassandra: {row}")
            else:
                logger.warning(f"Data verification failed for: {row}")
                
        except Exception as e:
            logger.error(f"Error inserting data: {e}")
            print(f"Error inserting data: {e}")

if __name__ == "__main__":
    # Create Spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Process parquet files and aggregate data
        merged_df = process_parquet_files(spark_conn)

        logger.info("Aggregated dataframe schema:")
        merged_df.printSchema()

        # Create Cassandra connection
        session = create_cassandra_connection()

        if session is not None:
            # Create the keyspace and table if necessary
            create_keyspace(session)
            create_table(session)

            # Insert data into Cassandra
            insert_data(session, merged_df)

            session.shutdown()