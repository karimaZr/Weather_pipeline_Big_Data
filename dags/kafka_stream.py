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
import os
from kafka.admin import KafkaAdminClient, NewTopic
from airflow.providers.docker.operators.docker import DockerOperator

from airflow.operators.bash import BashOperator

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
cities =[
    {
        "name": "Casablanca",
        "latitude": 33.5928,
        "longitude": -7.6192
    },
    {
        "name": "Rabat",
        "latitude": 33.9911,
        "longitude": -6.8401
    },
    {
        "name": "Fes",
        "latitude": 34.0372,
        "longitude": -4.9998
    },
    {
        "name": "Marrakesh",
        "latitude": 31.6315,
        "longitude": -8.0083
    },
    {
        "name": "Agadir",
        "latitude": 30.4202,
        "longitude": -9.5982
    },
    {
        "name": "Tangier",
        "latitude": 35.7806,
        "longitude": -5.8137
    },
    {
        "name": "Meknes",
        "latitude": 33.8935,
        "longitude": -5.5473
    },
    {
        "name": "Oujda",
        "latitude": 34.6805,
        "longitude": -1.9076
    },
    {
        "name": "Kenitra",
        "latitude": 34.261,
        "longitude": -6.5802
    },
    {
        "name": "Tétouan",
        "latitude": 35.5785,
        "longitude": -5.3684
    },
    {
        "name": "Safi",
        "latitude": 32.2994,
        "longitude": -9.2372
    },
    {
        "name": "El Jadida",
        "latitude": 33.256,
        "longitude": -8.508
    },
    {
        "name": "Nador",
        "latitude": 35.168,
        "longitude": -2.93
    },
    {
        "name": "Khemisset",
        "latitude": 33.8231,
        "longitude": -6.0667
    },
    {
        "name": "Settat",
        "latitude": 33.001,
        "longitude": -7.616
    },
    {
        "name": "Larache",
        "latitude": 35.1921,
        "longitude": -6.1528
    },
    {
        "name": "Guelmim",
        "latitude": 28.987,
        "longitude": -10.056
    },
    {
        "name": "Taza",
        "latitude": 34.2129,
        "longitude": -3.9962
    },
    {
        "name": "Tiznit Province",
        "latitude": 29.6974,
        "longitude": -9.7316
    },
    {
        "name": "Beni Mellal",
        "latitude": 32.3373,
        "longitude": -6.3498
    },
    {
        "name": "Mohammedia",
        "latitude": 33.6861,
        "longitude": -7.3829
    },
    {
        "name": "Mogador",
        "latitude": 31.5132,
        "longitude": -9.7681
    },
    {
        "name": "Ksar el-Kebir",
        "latitude": 35.0075,
        "longitude": -5.906
    },
    {
        "name": "Ouarzazate Province",
        "latitude": 30.9189,
        "longitude": -6.8936
    },
    {
        "name": "Al Hoceima",
        "latitude": 35.2517,
        "longitude": -3.9372
    },
    {
        "name": "Berkane",
        "latitude": 34.92,
        "longitude": -2.32
    },
    {
        "name": "Taourirt",
        "latitude": 34.404,
        "longitude": -2.895
    },
    {
        "name": "Errachidia",
        "latitude": 31.9314,
        "longitude": -4.4234
    },
    {
        "name": "Khouribga Province",
        "latitude": 32.8811,
        "longitude": -6.9063
    },
    {
        "name": "Oued Zem",
        "latitude": 32.863,
        "longitude": -6.566
    },
    {
        "name": "Sidi Slimane",
        "latitude": 34.259,
        "longitude": -5.927
    },
    {
        "name": "Sidi Kacem",
        "latitude": 34.237,
        "longitude": -5.713
    },
    {
        "name": "Azrou",
        "latitude": 33.433,
        "longitude": -5.221
    },
    {
        "name": "Midelt",
        "latitude": 32.679,
        "longitude": -4.734
    },
    {
        "name": "Ifrane",
        "latitude": 33.532,
        "longitude": -5.108
    },
    {
        "name": "Chefchaouen",
        "latitude": 35.171,
        "longitude": -5.263
    },
    {
        "name": "Laayoune",
        "latitude": 27.125,
        "longitude": -13.162
    }
]
def delete_kafka_topic(topic_name):
    """Delete and recreate a Kafka topic."""
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER)
        existing_topics = admin_client.list_topics()
        
        # Check if the topic exists
        if topic_name in existing_topics:
            # Delete the topic
            admin_client.delete_topics([topic_name])
            print(f"Topic '{topic_name}' deleted.")
        else:
            print(f"Topic '{topic_name}' does not exist. No action taken.")
    

    except Exception as e:
        print(f"Error resetting topic '{topic_name}': {e}")

def fetch_openweathermap_data(city):
    """Récupère les données depuis OpenWeatherMap API pour une ville donnée et les envoie dans Kafka."""
    try:
        
        params = {
            'q': f"{city['name']},ma",  
            'appid': OPENWEATHER_API_KEY,
            'units': 'metric'
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
            "current": "temperature_2m,relative_humidity_2m,apparent_temperature,is_day,precipitation,rain,showers,snowfall,weather_code,cloud_cover,pressure_msl,surface_pressure",
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
    delete_kafka_topic(KAFKA_TOPIC_OPENMETEO)
    delete_kafka_topic(KAFKA_TOPIC_OPENWEATHER)

    for city in cities:
        try:
            # Appel à OpenWeatherMap et Open-Meteo pour chaque ville
            fetch_openweathermap_data(city)
            fetch_openmeteo_data(city)
        except Exception as e:
            logging.error(f"Erreur dans le processus de streaming des données: {e}")
            continue
    

def read_from_kafka_and_store(topic_name, output_file, add_city_name=False):
    """
    Reads messages from a Kafka topic and stores them in a Parquet file.
    If the file already exists, it will be deleted before writing the new records.
    
    :param add_city_name: Si True, ajoute le nom de la ville aux données (utile pour Open-Meteo)
    """
    try:
        # Initialize Kafka Consumer
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset='earliest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )

        # Collect messages and optionally add city name
        records = []
        city_index = 0  # Pour associer chaque message à une ville de la liste
        for message in consumer:
            if topic_name == 'openmeteo_raw' and add_city_name:  # Ajoute le nom de la ville pour Open-Meteo
                if city_index >= len(cities):  # Limiter à la taille de la liste de villes
                    break
                message_value = message.value
                city = cities[city_index]
                message_value['city_name'] = city['name']  # Ajout du nom de la ville
                city_index += 1
            else:
                message_value = message.value

            records.append(message_value)  # Ajouter le message aux enregistrements

            # Stop consuming after a fixed number of messages (or a time limit)
            if len(records) >= 37:  # Exemple de limite de messages
                break

        # Log the number of records consumed
        print(f"Consumed {len(records)} records from topic {topic_name}")

        if records:
            # Convert records to a DataFrame
            df = pd.DataFrame(records)

            # Ensure the DataFrame is not empty before saving
            if not df.empty:
                output_path = OUTPUT_DIR / output_file

                # Delete the existing file if it exists
                if os.path.exists(output_path):
                    os.remove(output_path)
                    print(f"Deleted existing file at {output_path}")

                # Write the new records to the file
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
    """Reads from Open-Meteo Kafka topic and stores data in a Parquet file, adding city name."""
    read_from_kafka_and_store(KAFKA_TOPIC_OPENMETEO, "openmeteo_data.parquet", add_city_name=True)


# Création du DAG dans Airflow
with DAG(
    'weather_data_kafka_dag',
    default_args=default_args,
    schedule_interval='@daily',  # Exécution chaque heure
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
     # Copy openweathermap_data.parquet
    copy_openweathermap = BashOperator(
        task_id='copy_openweathermap',
        bash_command='docker cp /opt/airflow/data/parquet_files/openweathermap_data.parquet spark-master:/openweathermap_data.parquet'
    )

    # Copy openmeteo_data.parquet
    copy_openmeteo = BashOperator(
        task_id='copy_openmeteo',
        bash_command='docker cp /opt/airflow/data/parquet_files/openmeteo_data.parquet spark-master:/openmeteo_data.parquet'
    )

    # Copy spark_stream.py
    copy_spark_stream = BashOperator(
        task_id='copy_spark_stream',
        bash_command='docker cp /opt/airflow/data/spark_stream.py spark-master:/spark_stream.py'
    )

    # Run spark-submit
   
    spark_submit_task = BashOperator(
        task_id='spark_submit',
        bash_command=(
            'docker exec  spark-master spark-submit '
            '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 '
            '--py-files /dependencies.zip /spark_stream.py'
        ),
    )

    # Exécution de la tâche
    streaming_task >> [store_openweathermap_task, store_openmeteo_task]

    # Chaque tâche de la première liste se termine avant que les tâches de la seconde liste commencent
    [store_openweathermap_task, store_openmeteo_task] >> copy_openweathermap
    [store_openweathermap_task, store_openmeteo_task] >> copy_openmeteo
    [store_openweathermap_task, store_openmeteo_task] >> copy_spark_stream

    # Une fois que toutes les copies sont effectuées, on passe à spark_submit
    [copy_openweathermap, copy_openmeteo, copy_spark_stream] >> spark_submit_task
