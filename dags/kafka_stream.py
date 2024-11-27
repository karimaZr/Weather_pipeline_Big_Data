from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
from kafka import KafkaProducer
import time
import logging
import random

# Arguments par défaut pour le DAG
default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 9, 3, 10, 00),  # Exemple de date de démarrage
    'retries': 3,
}

# Configuration des API
OPENWEATHER_API_KEY = '5228ee3a150af754ec138620204936eb'
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
            sleep_duration = random.uniform(0.5, 2.0)
            time.sleep(sleep_duration)

        except Exception as e:
            logging.error(f"Erreur dans le processus de streaming des données: {e}")
            continue


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

    # Exécution de la tâche
    streaming_task
