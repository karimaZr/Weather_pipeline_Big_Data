import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

# Définir le titre de l'application
st.title("Visualisation des données météorologiques")

# Définir le répertoire des fichiers Parquet
DATA_DIR = Path("/opt/airflow/data/parquet_files")

# Charger les fichiers Parquet
def load_data(file_name):
    file_path = DATA_DIR / file_name
    if file_path.exists():
        return pd.read_parquet(file_path)
    else:
        st.error(f"Le fichier {file_name} n'existe pas dans {DATA_DIR}.")
        return pd.DataFrame()

# Charger les deux fichiers
openweather_data = load_data("openweathermap_data.parquet")
openmeteo_data = load_data("openmeteo_data.parquet")

# Vérifier et afficher les données
if not openweather_data.empty:
    st.subheader("Données OpenWeatherMap")
    st.dataframe(openweather_data)

if not openmeteo_data.empty:
    st.subheader("Données Open-Meteo")
    st.dataframe(openmeteo_data)

# Ajouter des visualisations si les données sont disponibles
if not openweather_data.empty:
    # Exemple : graphique de température depuis OpenWeatherMap
    st.subheader("Température OpenWeatherMap")
    if "main" in openweather_data.columns and "temp" in openweather_data["main"].iloc[0]:
        # Extraire les températures
        openweather_data["temperature"] = openweather_data["main"].apply(lambda x: x["temp"] if isinstance(x, dict) else None)
        fig = px.line(openweather_data, y="temperature", title="Températures OpenWeatherMap")
        st.plotly_chart(fig)
    else:
        st.write("Colonne 'temp' non trouvée dans les données OpenWeatherMap.")

if not openmeteo_data.empty:
    # Exemple : graphique des températures horaires depuis Open-Meteo
    st.subheader("Température horaire Open-Meteo")
    if "hourly" in openmeteo_data.columns and "temperature_2m" in openmeteo_data["hourly"].iloc[0]:
        # Décomposer les données horaires
        hourly_data = pd.DataFrame(openmeteo_data["hourly"].iloc[0])
        hourly_data["time"] = pd.to_datetime(hourly_data["time"])
        fig = px.line(hourly_data, x="time", y="temperature_2m", title="Températures horaires Open-Meteo")
        st.plotly_chart(fig)
    else:
        st.write("Colonne 'temperature_2m' non trouvée dans les données Open-Meteo.")