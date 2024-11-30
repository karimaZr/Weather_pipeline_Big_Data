import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

# Définir le répertoire des fichiers Parquet
DATA_DIR = Path("/opt/airflow/data/parquet_files")

# Fonction pour charger les fichiers Parquet
def load_data(file_name):
    file_path = DATA_DIR / file_name
    if file_path.exists():
        return pd.read_parquet(file_path)
    else:
        st.error(f"Le fichier {file_name} n'existe pas dans {DATA_DIR}.")
        return pd.DataFrame()

# Chargement des données
openweather_data = load_data("openweathermap_data.parquet")
openmeteo_data = load_data("openmeteo_data.parquet")

# Barre latérale pour la navigation
st.sidebar.title("Navigation")
section = st.sidebar.radio(
    "Choisir une section :",
    ["Présentation des données", "Comparaison des températures", "Analyse avancée"]
)

# --- Présentation des données ---
def presentation_donnees():
    st.title("Présentation des données")
    if not openweather_data.empty:
        st.subheader("Données OpenWeatherMap")
        st.dataframe(openweather_data.head())
        st.write("**Résumé statistique :**")
        st.write(openweather_data.describe())
    if not openmeteo_data.empty:
        st.subheader("Données Open-Meteo")
        st.dataframe(openmeteo_data.head())
        st.write("**Résumé statistique :**")
        st.write(openmeteo_data.describe())

# --- Comparaison des températures ---
def comparaison_temperatures():
    st.title("Comparaison des températures")
    if not openweather_data.empty and not openmeteo_data.empty:
        st.subheader("Comparaison des températures entre OpenWeatherMap et Open-Meteo")
        
        # Préparer les données
        openweather_data["temperature"] = openweather_data["main"].apply(
            lambda x: x["temp"] if isinstance(x, dict) else None
        )
        openweather_data["time"] = pd.to_datetime(openweather_data["dt"], unit="s")
        hourly_data = pd.DataFrame(openmeteo_data["hourly"].iloc[0])
        hourly_data["time"] = pd.to_datetime(hourly_data["time"])
        hourly_data.rename(columns={"temperature_2m": "temperature_meteo"}, inplace=True)
        
        # Fusionner les deux datasets sur le temps
        comparison_data = pd.merge(
            openweather_data[["time", "temperature"]],
            hourly_data[["time", "temperature_meteo"]],
            on="time",
            how="inner"
        )
        
        # Graphique de comparaison
        fig = px.line(
            comparison_data,
            x="time",
            y=["temperature", "temperature_meteo"],
            labels={"value": "Température (°C)", "variable": "Source", "time": "Temps"},
            title="Comparaison des températures en fonction du temps"
        )
        st.plotly_chart(fig)
    else:
        st.warning("Les données des deux sources ne sont pas disponibles pour comparaison.")

# --- Analyse avancée ---
def analyse_avancee():
    st.title("Analyse avancée")

    # 1. Distribution des températures
    if not openweather_data.empty:
        st.subheader("Distribution des températures (OpenWeatherMap)")
        openweather_data["temperature"] = openweather_data["main"].apply(
            lambda x: x["temp"] if isinstance(x, dict) else None
        )
        fig = px.histogram(openweather_data, x="temperature", nbins=30, title="Distribution des températures")
        st.plotly_chart(fig)
    
    # 2. Variation de la température au cours du temps
    if not openmeteo_data.empty:
        st.subheader("Variation de la température au cours du temps (Open-Meteo)")
        hourly_data = pd.DataFrame(openmeteo_data["hourly"].iloc[0])
        hourly_data["time"] = pd.to_datetime(hourly_data["time"])
        fig = px.line(hourly_data, x="time", y="temperature_2m", title="Variation de la température au cours du temps")
        st.plotly_chart(fig)
    
    # 3. Température moyenne par heure
    if not openmeteo_data.empty:
        st.subheader("Température moyenne par heure (Open-Meteo)")
        hourly_data["hour"] = hourly_data["time"].dt.hour
        hourly_avg_temp = hourly_data.groupby("hour")["temperature_2m"].mean().reset_index()
        fig = px.bar(hourly_avg_temp, x="hour", y="temperature_2m", title="Température moyenne par heure")
        st.plotly_chart(fig)
    
    # 4. Température par jour
    if not openmeteo_data.empty:
        st.subheader("Température par jour (Open-Meteo)")
        hourly_data["date"] = hourly_data["time"].dt.date
        daily_avg_temp = hourly_data.groupby("date")["temperature_2m"].mean().reset_index()
        fig = px.line(daily_avg_temp, x="date", y="temperature_2m", title="Température moyenne quotidienne")
        st.plotly_chart(fig)
    
    # 5. Nuages vs Température
    if not openweather_data.empty:
        st.subheader("Nuages vs Température (OpenWeatherMap)")
        openweather_data["cloudiness"] = openweather_data["clouds"].apply(
            lambda x: x["all"] if isinstance(x, dict) else None
        )
        fig = px.scatter(
            openweather_data,
            x="temperature",
            y="cloudiness",
            title="Relation entre nuages et température",
            labels={"cloudiness": "Nuages (%)", "temperature": "Température (°C)"}
        )
        st.plotly_chart(fig)
    
    # 6. Variation de la pression atmosphérique
    if not openweather_data.empty:
        st.subheader("Variation de la pression atmosphérique (OpenWeatherMap)")
        openweather_data["pressure"] = openweather_data["main"].apply(
            lambda x: x["pressure"] if isinstance(x, dict) else None
        )
        fig = px.box(openweather_data, y="pressure", title="Distribution de la pression atmosphérique")
        st.plotly_chart(fig)

    # 7. Matrice de corrélation complète
    if not openweather_data.empty:
        st.subheader("Matrice de corrélation (OpenWeatherMap)")
        numeric_cols = openweather_data.select_dtypes(include=["number"]).columns
        if len(numeric_cols) > 1:
            corr = openweather_data[numeric_cols].corr()
            fig = px.imshow(corr, text_auto=True, title="Matrice de corrélation complète")
            st.plotly_chart(fig)

# Navigation
if section == "Présentation des données":
    presentation_donnees()
elif section == "Comparaison des températures":
    comparaison_temperatures()
elif section == "Analyse avancée":
    analyse_avancee()
