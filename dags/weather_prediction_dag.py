import logging
import pandas as pd
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import DoubleType, TimestampType
from cassandra.cluster import Cluster
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX
import numpy as np
import pickle
import os
from pyspark.sql import functions as F

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("WeatherPredictionPipeline")

# Global constants
MODEL_DIR = "trained_models"  # Directory to store trained models
MIN_DATA_POINTS = 20  # Minimum number of data points required for training
KEYSPACE = "spark_streaming"  # Keyspace in Cassandra
TABLE_NAME = "weather_data"  # Table in Cassandra

# Ensure model directory exists
os.makedirs(MODEL_DIR, exist_ok=True)

# Function to create Cassandra connection
def create_cassandra_connection():
    try:
        cluster = Cluster(['cassandra_db'])
        cas_session = cluster.connect()
        logger.info("Cassandra connection created successfully")
        return cas_session
    except Exception as e:
        logger.error(f"Error while creating Cassandra connection: {e}")
        return None

# Function to create Spark session
def create_spark_connection():
    try:
        spark_conn = SparkSession.builder \
            .appName("WeatherPredictionPipeline") \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1") \
            .config("spark.cassandra.connection.host", "cassandra_db") \
            .getOrCreate()
        spark_conn.sparkContext.setLogLevel("ERROR")
        logger.info("Spark connection created successfully")
        return spark_conn
    except Exception as e:
        logger.error(f"Error while creating Spark connection: {e}")
        return None

# Function to train models offline and save them
def train_models_offline(spark):
    # Read data from Cassandra
    data = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table=TABLE_NAME, keyspace=KEYSPACE) \
        .load()

    # Get list of unique cities
    cities = data.select("city_name").distinct().rdd.flatMap(lambda x: x).collect()

    # Train models for each city and column
    columns_to_train = ["humidity", "temp", "temperature_2m"]  # Columns for prediction
    success = True  # Track if all models were successfully trained

    for city in cities:
        city_data = data.filter(col("city_name") == city).toPandas()

        # Ensure timestamp column exists
        if "timestamp" not in city_data.columns:
            logger.error(f"No timestamp column for city: {city}")
            success = False
            continue

        # Prepare data
        city_data["timestamp"] = pd.to_datetime(city_data["timestamp"])
        city_data.set_index("timestamp", inplace=True)

        for column in columns_to_train:
            if column not in city_data.columns:
                logger.warning(f"Column {column} missing for city {city}. Skipping.")
                continue

            series = city_data[column].replace([np.inf, -np.inf], np.nan).dropna()

            # Skip training if insufficient data
            if len(series) < MIN_DATA_POINTS:
                logger.warning(f"Insufficient data to train model for {city} - {column} (data points: {len(series)}). Skipping.")
                continue

            # Train ARIMA and SARIMAX models
            try:
                # ARIMA model
                arima_model = ARIMA(series, order=(5, 1, 0)).fit()

                # SARIMAX model
                sarimax_model = SARIMAX(series, order=(5, 1, 0), seasonal_order=(0, 1, 1, 12)).fit()

                # Choose the best model based on AIC
                if arima_model.aic < sarimax_model.aic:
                    best_model = arima_model
                    model_type = "ARIMA"
                else:
                    best_model = sarimax_model
                    model_type = "SARIMAX"

                # Save the model
                model_path = os.path.join(MODEL_DIR, f"{city}_{column}_model.pkl")
                with open(model_path, "wb") as f:
                    pickle.dump((best_model, model_type), f)

                logger.info(f"Trained and saved {model_type} model for {city} - {column}")

            except Exception as e:
                logger.error(f"Error training models for {city} - {column}: {e}")
                success = False

    return success

# UDF for predictions
def create_prediction_udf():
    def predict(city, column, last_value):
        try:
            # Load the trained model
            model_path = os.path.join(MODEL_DIR, f"{city}_{column}_model.pkl")
            if not os.path.exists(model_path):
                logger.error(f"No trained model found for {city} - {column}")
                return None

            with open(model_path, "rb") as f:
                model, model_type = pickle.load(f)

            # Predict the next value (10 minutes ahead)
            if model_type == "ARIMA":
                forecast = model.forecast(steps=1)
            elif model_type == "SARIMAX":
                forecast = model.get_forecast(steps=1).predicted_mean
            else:
                return None

            return float(forecast[0]) if len(forecast) > 0 else None
        except Exception as e:
            logger.error(f"Error predicting for {city} - {column}: {e}")
            return None

    return udf(predict, DoubleType())

# Process predictions and save in a separate table with the same column names
def process_predictions(spark):
    # Read data from the real data table in Cassandra
    data = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table=TABLE_NAME, keyspace=KEYSPACE) \
        .load()

    # Define the UDF for predictions
    prediction_udf = create_prediction_udf()

    # Predict for each city and column
    columns_to_predict = ["humidity", "temp", "temperature_2m"]
    for column in columns_to_predict:
        data = data.withColumn(column, prediction_udf(F.col("city_name"), F.lit(column), F.col(column)))

    # Add timestamp for predictions (10 minutes ahead) using an interval expression
    data = data.withColumn("timestamp", F.expr("timestamp + INTERVAL 10 MINUTES").cast(TimestampType()))

    # Select only the necessary columns (predictions only)
    prediction_data = data.select("city_name", "timestamp", "humidity", "temp", "temperature_2m")

    # Filter out rows where any of the predictions are null
    prediction_data = prediction_data.filter(
        (F.col("humidity").isNotNull()) &
        (F.col("temp").isNotNull()) &
        (F.col("temperature_2m").isNotNull())
    )

    # Create Cassandra connection for inserting predictions
    cassandra_session = create_cassandra_connection()

    if cassandra_session:
        # Create the weather_predictions table if it doesn't exist
        create_table_query = """
            CREATE TABLE IF NOT EXISTS spark_streaming.weather_predictions (
                city_name TEXT,
                timestamp TIMESTAMP,
                humidity FLOAT,
                temp FLOAT,
                temperature_2m FLOAT,
                PRIMARY KEY (city_name, timestamp)
            );
        """
        cassandra_session.execute(create_table_query)

        # Insert the predictions into the weather_predictions table
        insert_query = """
            INSERT INTO spark_streaming.weather_predictions (
                city_name, timestamp, humidity, temp, temperature_2m
            ) VALUES (%s, %s, %s, %s, %s);
        """

        # Process each row and insert predictions
        for row in prediction_data.collect():
            cassandra_session.execute(insert_query, (
                row['city_name'],
                row['timestamp'],
                row['humidity'],
                row['temp'],
                row['temperature_2m']
            ))

        logger.info("Predictions processed and saved to the `weather_predictions` table.")
    else:
        logger.error("Failed to create Cassandra connection.")

# Main execution
if __name__ == "__main__":
    spark = create_spark_connection()
    if spark:
        # Step 1: Train models offline
        if train_models_offline(spark):
            # Step 2: Predict using trained models if training was successful
            process_predictions(spark)
        else:
            logger.error("Model training failed. Predictions will not be processed.")

    logger.info("Pipeline completed.")
