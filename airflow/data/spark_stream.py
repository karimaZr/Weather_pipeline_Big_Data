import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,BinaryType, StructField, StringType, DoubleType, ArrayType, LongType,TimestampType
from cassandra.cluster import Cluster
from pyspark.sql.functions import from_unixtime

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
            StructField("feels_like", DoubleType(), True),
            StructField("humidity", LongType(), True),
            StructField("pressure", LongType(), True),
            StructField("sea_level", LongType(), True),
            StructField("grnd_level", LongType(), True)
        ])),
        StructField("visibility", LongType(), True),
        StructField("wind", StructType([ 
            StructField("speed", DoubleType(), True),
            StructField("deg", LongType(), True)
        ])),
        StructField("clouds", StructType([ 
            StructField("all", LongType(), True)
        ])),
        StructField("sys", StructType([ 
            StructField("country", StringType(), True),
            StructField("sunrise", LongType(), True),
            StructField("sunset", LongType(), True)
        ])),
        StructField("name", StringType(), True),
        StructField("dt", LongType(), True),
    ])

    schema2 = StructType([  # Schema for file 2
        StructField("current", StructType([ 
            StructField("apparent_temperature", DoubleType(), True),
            StructField("is_day", LongType(), True),
            StructField("rain", DoubleType(), True),
            StructField("precipitation", DoubleType(), True),
            StructField("showers", DoubleType(), True),
            StructField("snowfall", DoubleType(), True),
            StructField("temperature_2m", DoubleType(), True),
            StructField("relative_humidity_2m", LongType(), True),
            StructField("pressure_msl", DoubleType(), True),
            StructField("surface_pressure", DoubleType(), True)
        ])),
        StructField("city_name", StringType(), True),
        StructField("elevation", DoubleType(), True)
    ])

    # Lire les fichiers .parquet avec les schémas explicites
    df1 = spark_conn.read.schema(schema1).parquet("/tmp/output/parquet_files/openweathermap_data.parquet")
    df2 = spark_conn.read.schema(schema2).parquet("/tmp/output/parquet_files/openmeteo_data.parquet")
    
    # Sélectionner et transformer les données
    df1 = df1.select(
        col("coord.lat").alias("latitude"),
        col("coord.lon").alias("longitude"),
        col("weather.description").alias("weather_description"),
        col("main.temp").alias("temp"),
        col("main.feels_like").alias("feels_like"),
        col("main.humidity").alias("humidity"),
        col("main.pressure").alias("pressure"),
        col("main.sea_level").alias("sea_level"),
        col("main.grnd_level").alias("grnd_level"),
        col("visibility").alias("visibility"),
        col("wind.speed").alias("wind_speed"),
        col("wind.deg").alias("wind_deg"),
        col("clouds.all").alias("clouds_all"),
        col("sys.country").alias("country"),
        col("sys.sunrise").alias("sunrise"),
        col("sys.sunset").alias("sunset"),
        col("name").alias("city_name"),
        from_unixtime(col("dt")).alias("time")
    )
    
    df2 = df2.select(
        col("current.apparent_temperature").alias("apparent_temperature"),
        col("current.is_day").alias("is_day"),
        col("current.rain").alias("rain"),
        col("current.precipitation").alias("precipitation"),
        col("current.showers").alias("showers"),
        col("current.snowfall").alias("snowfall"),
        col("current.temperature_2m").alias("temperature_2m"),
        col("current.relative_humidity_2m").alias("relative_humidity_2m"),
        col("current.pressure_msl").alias("pressure_msl"),
        col("current.surface_pressure").alias("surface_pressure"),
        col("city_name").alias("city_name"),
        col("elevation").alias("elevation")
    )

    # Joindre les deux DataFrames
    merged_df = df1.join(df2, on="city_name", how="inner")

    # Calcul des moyennes et ajout des colonnes
    merged_df = merged_df.withColumn(
        "humidity", (col("relative_humidity_2m") + col("humidity")) / 2
    ).withColumn(
        "temperature_feels_like", (col("apparent_temperature") + col("feels_like")) / 2
    ).withColumn(
        "temperature_2m", (col("temp") + col("temperature_2m")) / 2
    )

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
            feels_like DOUBLE,
            humidity DOUBLE,
            pressure DOUBLE,
            sea_level DOUBLE,
            grnd_level DOUBLE,
            visibility DOUBLE,
            wind_speed DOUBLE,
            wind_deg DOUBLE,
            clouds_all DOUBLE,
            country TEXT,
            sunrise TIMESTAMP,
            sunset TIMESTAMP,
            city_name TEXT,
            time TIMESTAMP,
            apparent_temperature DOUBLE,
            is_day int,
            rain DOUBLE,
            precipitation DOUBLE,
            showers DOUBLE,
            snowfall DOUBLE,
            relative_humidity_2m DOUBLE,
            pressure_msl DOUBLE,
            surface_pressure DOUBLE,
            elevation DOUBLE,
            PRIMARY KEY (city_name, time)
        )
    """)
    logger.info("Table created successfully")
def insert_data(session, merged_df):
    # Collect data from the merged dataframe and insert into Cassandra
    for row in merged_df.collect():
        try:
            # Vérification des valeurs
            values = (
                row.latitude, row.longitude, row.temperature_2m, row.temp, row.feels_like, row.humidity, row.pressure, 
                row.sea_level, row.grnd_level, row.visibility, row.wind_speed, row.wind_deg, row.clouds_all, row.country, 
                row.sunrise, row.sunset, row.city_name, row.time, row.apparent_temperature, row.is_day, 
                row.rain, row.precipitation, row.showers, row.snowfall, row.relative_humidity_2m, row.pressure_msl, 
                row.surface_pressure, row.elevation
            )
            
            # Afficher les valeurs pour déboguer
            logger.debug(f"Values to insert: {values}")

            # Insertion des données dans Cassandra
            session.execute("""
                INSERT INTO spark_streaming.weather_data (
                    latitude, longitude, temperature_2m, temp, feels_like, humidity, pressure, sea_level, grnd_level,
                    visibility, wind_speed, wind_deg, clouds_all, country, sunrise, sunset, city_name,
                    time, apparent_temperature, is_day, rain, precipitation, showers, snowfall, 
                    relative_humidity_2m, pressure_msl, surface_pressure, elevation
                )
                VALUES ( %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, values)

            logger.info(f"Inserted data: {row.city_name} at {row.time}")

            # Optionnel: Vérifier l'insertion
            verify_query = """
                SELECT * FROM spark_streaming.weather_data WHERE city_name = %s AND time = %s
            """
            result = session.execute(verify_query, (row.city_name, row.time))
            logger.info(f"Data inserted successfully for {row.city_name} at {row.time}")
        except Exception as e:
            logger.error(f"Error inserting data for {row.city_name} at {row.time}: {e}")

def main():
    # Initialize connections
    spark_conn = create_spark_connection()
    if spark_conn is None:
        return
    
    # Process data
    merged_df = process_parquet_files(spark_conn)

    # Create Cassandra connection
    cas_session = create_cassandra_connection()
    if cas_session is None:
        return

    # Create keyspace and table in Cassandra
    create_keyspace(cas_session)
    create_table(cas_session)

    # Insert data into Cassandra
    insert_data(cas_session, merged_df)

if __name__ == "__main__":
    main()
