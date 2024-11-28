from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
from kafka import KafkaProducer, KafkaConsumer
import time
import logging
import random
import pandas as pd
from pathlib import Path

# Arguments par défaut pour le DAG
default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 9, 3, 10, 00),  # Exemple de date de démarrage
    'retries': 3,
}
OUTPUT_DIR = Path("/opt/airflow/data/parquet_files")  # Update the path as needed
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)  # Create directory if it doesn't exist

# Configuration des API
OPENWEATHER_API_KEY = '8167e2dfee0bd9f28f8eafbd9e355ad3'
OPENMETEO_API_URL = 'https://api.open-meteo.com/v1/forecast'
OPENWEATHER_API_URL = 'http://api.openweathermap.org/data/2.5/weather'
KAFKA_SERVER = 'broker:9092'  # Adresse de votre serveur Kafka
KAFKA_TOPIC_OPENWEATHER = 'openweathermap_raw'
KAFKA_TOPIC_OPENMETEO = 'openmeteo_raw'


def fetch_openweathermap_data():
    """Récupère les données depuis OpenWeatherMap API et les envoie dans Kafka."""
    try:
        params = {
            'q': 'Paris,fr',
            'appid': OPENWEATHER_API_KEY,
        }
        response = requests.get(OPENWEATHER_API_URL, params=params)
        response.raise_for_status()  # Lève une exception pour une mauvaise réponse

        data = response.json()

        # Initialiser le producteur Kafka
        producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER,
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        producer.send(KAFKA_TOPIC_OPENWEATHER, value=data)
        producer.flush()  # S'assure que les messages sont envoyés
        print("Données envoyées à Kafka pour OpenWeatherMap.")

    except Exception as e:
        logging.error(f"Erreur lors de la récupération des données d'OpenWeatherMap: {e}")

def fetch_openmeteo_data():
    """Récupère les données depuis Open-Meteo API et les envoie dans Kafka."""
    try:
        params = {
            'latitude': 48.8566,  # Paris
            'longitude': 2.3522,
            'hourly': 'temperature_2m',
        }
        response = requests.get(OPENMETEO_API_URL, params=params)
        response.raise_for_status()

        data = response.json()

        # Initialiser le producteur Kafka
        producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER,
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        producer.send(KAFKA_TOPIC_OPENMETEO, value=data)
        producer.flush()
        print("Données envoyées à Kafka pour Open-Meteo.")

    except Exception as e:
        logging.error(f"Erreur lors de la récupération des données d'Open-Meteo: {e}")


def stream_data():
    """Récupère les données de plusieurs API et les envoie dans Kafka."""
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, max_block_ms=5000)
    curr_time = time.time()

    # Envoi de données pendant 2 minutes
    while time.time() < curr_time + 120:
        try:
            # Appel à OpenWeatherMap et Open-Meteo
            fetch_openweathermap_data()
            fetch_openmeteo_data()

            # Simulation d'un délai entre chaque envoi de données
            sleep_duration = random.uniform(0.1, 0.5)  # Shorter sleep duration
            time.sleep(sleep_duration)

        except Exception as e:
            logging.error(f"Erreur dans le processus de streaming des données: {e}")
            continue
    
def read_from_kafka_and_store(topic_name, output_file):
    """
    Reads messages from a Kafka topic and stores them in a Parquet file.
    """
    try:
        # Initialize Kafka Consumer
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset='earliest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )

        # Collect messages
        records = []
        for message in consumer:
            records.append(message.value)  # Extract the message value (JSON object)

            # Stop consuming after a fixed number of messages (or a time limit)
            if len(records) >= 100:  # Example limit
                break

        # Log the number of records consumed
        print(f"Consumed {len(records)} records from topic {topic_name}")

        if records:
            # Convert records to a DataFrame
            df = pd.DataFrame(records)

            # Ensure the DataFrame is not empty before saving
            if not df.empty:
                output_path = OUTPUT_DIR / output_file
                df.to_parquet(output_path, index=False)
                print(f"Data from {topic_name} stored in {output_path}")
            else:
                print(f"No records to store for {topic_name}")
        else:
            print(f"No data consumed from topic {topic_name}")

    except Exception as e:
        logging.error(f"Error reading from Kafka topic {topic_name}: {e}")


def store_openweathermap_data():
    """Reads from OpenWeatherMap Kafka topic and stores data in a Parquet file."""
    read_from_kafka_and_store(KAFKA_TOPIC_OPENWEATHER, "openweathermap_data.parquet")

def store_openmeteo_data():
    """Reads from Open-Meteo Kafka topic and stores data in a Parquet file."""
    read_from_kafka_and_store(KAFKA_TOPIC_OPENMETEO, "openmeteo_data.parquet")


# Création du DAG dans Airflow
with DAG(
    'weather_data_kafka_dag',
    default_args=default_args,
    schedule_interval='@hourly',  # Exécution chaque heure     schedule_interval='*/10 * * * *',  # Exécution toutes les 10 minutes
    catchup=False
) as dag:

    # Définition des tâches dans le DAG
    streaming_task = PythonOperator(
        task_id='stream_data_from_apis',
        python_callable=stream_data
    )
    store_openweathermap_task = PythonOperator(
        task_id='store_openweathermap_data',
        python_callable=store_openweathermap_data
    )
    store_openmeteo_task = PythonOperator(
        task_id='store_openmeteo_data',
        python_callable=store_openmeteo_data
    )
    # Exécution de la tâche
    streaming_task >> [store_openweathermap_task, store_openmeteo_task]
