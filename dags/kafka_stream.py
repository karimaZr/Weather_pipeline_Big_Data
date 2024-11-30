from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
import json
from kafka import KafkaProducer, KafkaConsumer, KafkaConsumer
import time
import logging
import random
import pandas as pd
from pathlib import Path
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
OPENWEATHER_API_KEY = '8167e2dfee0bd9f28f8eafbd9e355ad3'
OPENMETEO_API_URL = 'https://api.open-meteo.com/v1/forecast'
OPENWEATHER_API_URL = 'http://api.openweathermap.org/data/2.5/weather'
KAFKA_SERVER = 'broker:9092'  # Adresse de votre serveur Kafka
KAFKA_TOPIC_OPENWEATHER = 'openweathermap_raw'
KAFKA_TOPIC_OPENMETEO = 'openmeteo_raw'

# Liste des villes à traiter
cities = [
    {'name': 'Casablanca', 'latitude': 33.5945144, 'longitude': -7.6200284},
    {'name': 'Rabat', 'latitude': 34.02236, 'longitude': -6.8340222},
    {'name': 'Fès', 'latitude': 34.0346534, 'longitude': -5.0161926},
    {'name': 'Marrakech', 'latitude': 31.6258257, 'longitude': -7.9891608},
    {'name': 'Agadir', 'latitude': 30.4205162, 'longitude': -9.5838532},
    {'name': 'Tanger', 'latitude': 35.7696302, 'longitude': -5.8033522},
    {'name': 'Meknès', 'latitude': 33.8984131, 'longitude': -5.5321582},
    {'name': 'Oujda', 'latitude': 34.677874, 'longitude': -1.929306},
    {'name': 'Kenitra', 'latitude': 34.26457, 'longitude': -6.570169},
    {'name': 'Tetouan', 'latitude': 35.570175, 'longitude': -5.3742776},
    {'name': 'El Jadida', 'latitude': 33.22932285, 'longitude': -8.499356736652636},
    {'name': 'Safi', 'latitude': 32.299424, 'longitude': -9.239533},
    {'name': 'Beni Mellal', 'latitude': 32.334193, 'longitude': -6.353335},
    {'name': 'Nador', 'latitude': 35.1739922, 'longitude': -2.9281198},
    {'name': 'Taza', 'latitude': 34.230155, 'longitude': -4.010104},
    {'name': 'Essaouira', 'latitude': 31.5118281, 'longitude': -9.7620903},
    {'name': 'Khouribga', 'latitude': 32.8856482, 'longitude': -6.908798},
    {'name': 'Settat', 'latitude': 33.002397, 'longitude': -7.619867},
    {'name': 'Larache', 'latitude': 35.1952327, 'longitude': -6.152913},
    {'name': 'Ksar El Kebir', 'latitude': 34.999218, 'longitude': -5.898724},
    {'name': 'Taourirt', 'latitude': 34.413438, 'longitude': -2.893825},
    {'name': 'Guercif', 'latitude': 34.225576, 'longitude': -3.352345},
    {'name': 'Guelmim', 'latitude': 28.9863852, 'longitude': -10.0574351},
    {'name': 'Dakhla', 'latitude': 23.6940663, 'longitude': -15.9431274},
    {'name': 'Laayoune', 'latitude': 27.154512, 'longitude': -13.1953921},
    {'name': 'Ifrane', 'latitude': 33.527605, 'longitude': -5.107408},
    {'name': 'Azrou', 'latitude': 33.436117, 'longitude': -5.221913},
    {'name': 'Errachidia', 'latitude': 31.929089, 'longitude': -4.4340807},
    {'name': 'Ouarzazate', 'latitude': 30.920193, 'longitude': -6.910923},
    {'name': 'Taroudant', 'latitude': 30.470651, 'longitude': -8.877922},
    {'name': 'Chefchaouen', 'latitude': 35.1693724, 'longitude': -5.275765468193203},
    {'name': 'Berkane', 'latitude': 34.9266755, 'longitude': -2.3294087},
    {'name': 'Sidi Kacem', 'latitude': 34.226412, 'longitude': -5.711434},
    {'name': 'Sidi Slimane', 'latitude': 34.259878, 'longitude': -5.927253},
    {'name': 'Sidi Ifni', 'latitude': 29.3791253, 'longitude': -10.1715632},
    {'name': 'Tiznit', 'latitude': 29.698624, 'longitude': -9.7312815},
    {'name': 'Tan-Tan', 'latitude': 28.437553, 'longitude': -11.098664}
]

def fetch_openweathermap_data(city):
    """Récupère les données depuis OpenWeatherMap API pour une ville donnée et les envoie dans Kafka."""
    try:
        params = {
            'q': f"{city['name']},ma",  # Utiliser le nom de la ville dans la requête
            'appid': OPENWEATHER_API_KEY,
        }
        response = requests.get(OPENWEATHER_API_URL, params=params)
        response.raise_for_status()

        data = response.json()

        # Initialiser le producteur Kafka
        producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER,
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        # Envoi des données pour chaque ville
        producer.send(KAFKA_TOPIC_OPENWEATHER, value=data)
        producer.flush()
        print(f"Données envoyées à Kafka pour OpenWeatherMap - {city['name']}.")

    except Exception as e:
        logging.error(f"Erreur lors de la récupération des données d'OpenWeatherMap pour {city['name']}: {e}")

def fetch_openmeteo_data(city):
    """Récupère les données depuis Open-Meteo API pour une ville donnée et les envoie dans Kafka."""
    try:
        params = {
            'latitude': city['latitude'],
            'longitude': city['longitude'],
            "hourly": "temperature_2m,precipitation,cloudcover",
        }
        response = requests.get(OPENMETEO_API_URL, params=params)
        response.raise_for_status()

        data = response.json()

        # Initialiser le producteur Kafka
        producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER,
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        # Envoi des données pour chaque ville
        producer.send(KAFKA_TOPIC_OPENMETEO, value=data)
        producer.flush()
        print(f"Données envoyées à Kafka pour Open-Meteo - {city['name']}.")

    except Exception as e:
        logging.error(f"Erreur lors de la récupération des données d'Open-Meteo pour {city['name']}: {e}")

def stream_data():
    """Récupère les données de plusieurs API pour plusieurs villes et les envoie dans Kafka."""
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, max_block_ms=5000)
    curr_time = time.time()

    # Envoi de données pendant 2 minutes
    while time.time() < curr_time + 180:
        for city in cities:
            try:
                # Appel à OpenWeatherMap et Open-Meteo pour chaque ville
                fetch_openweathermap_data(city)
                fetch_openmeteo_data(city)

                # Simulation d'un délai entre chaque envoi de données
                sleep_duration = random.uniform(0.5, 2.0)
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
    schedule_interval='@hourly',  # Exécution chaque heure
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
