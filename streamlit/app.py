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

# Fonction pour préparer les données OpenWeatherMap
def expand_openweather_data(data):
    # Extraire les informations nécessaires des colonnes JSON imbriquées
    data["temperature"] = data["main"].apply(lambda x: x["temp"] if isinstance(x, dict) else None)
    data["humidity"] = data["main"].apply(lambda x: x["humidity"] if isinstance(x, dict) else None)
    data["pressure"] = data["main"].apply(lambda x: x["pressure"] if isinstance(x, dict) else None)
    data["wind_speed"] = data["wind"].apply(lambda x: x["speed"] if isinstance(x, dict) else None)
    data["time"] = pd.to_datetime(data["dt"], unit="s")
    return data

# Fonction pour préparer les données Open-Meteo
def expand_openmeteo_data(data):
    hourly_data = pd.DataFrame(data["hourly"].iloc[0])
    hourly_data["time"] = pd.to_datetime(hourly_data["time"])
    return hourly_data

# Chargement des données
openweather_data = load_data("openweathermap_data.parquet")
openmeteo_data = load_data("openmeteo_data.parquet")

# Barre latérale pour la navigation
st.sidebar.title("Navigation")
section = st.sidebar.radio(
    "Choisir une section :",
    ["Présentation des données", "Répartition des températures", "Analyse avancée"]
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

# --- Graphiques --- 

def repartition_temperatures_par_heure():
    st.title("Répartition des températures selon l'heure de la journée")
    
    if not openweather_data.empty:
        # Préparer les données d'OpenWeatherMap
        openweather_expanded = expand_openweather_data(openweather_data)
        
        # Extraire l'heure de la journée
        openweather_expanded['hour'] = openweather_expanded['time'].dt.hour
        
        # Calculer la température moyenne par heure
        hourly_avg_temp = openweather_expanded.groupby('hour')['temperature'].mean().reset_index()
        
        # Graphique en barres pour la répartition des températures par heure
        fig = px.bar(hourly_avg_temp, x='hour', y='temperature', title="Répartition des températures selon l'heure de la journée",
                     labels={'hour': 'Heure de la journée', 'temperature': 'Température Moyenne (°C)'})
        st.plotly_chart(fig)
    else:
        st.warning("Les données d'OpenWeatherMap ne sont pas disponibles.")
        
        
def repartition_plage_temperatures():
    st.title("Répartition des températures par plage de valeurs")
    
    if not openweather_data.empty:
        # Préparer les données d'OpenWeatherMap
        openweather_expanded = expand_openweather_data(openweather_data)
        
        # Créer des plages de températures (par exemple : -10 à 0°C, 0 à 10°C, etc.)
        bins = [-float('inf'), 0, 10, 20, 30, float('inf')]
        labels = ['< 0°C', '0-10°C', '10-20°C', '20-30°C', '> 30°C']
        
        # Catégoriser les températures dans les plages
        openweather_expanded['temperature_range'] = pd.cut(openweather_expanded['temperature'], bins=bins, labels=labels)
        
        # Compter le nombre d'occurrences dans chaque plage
        temp_range_count = openweather_expanded['temperature_range'].value_counts().reset_index()
        temp_range_count.columns = ['Plage de Température', 'Nombre de Jours']
        
        # Graphique circulaire pour la répartition des températures par plage
        fig = px.pie(temp_range_count, names='Plage de Température', values='Nombre de Jours', 
                     title="Répartition des températures par plage de valeurs")
        st.plotly_chart(fig)
    else:
        st.warning("Les données d'OpenWeatherMap ne sont pas disponibles.")

 
# --- Analyse avancée ---
def analyse_avancee():
    st.title("Analyse avancée")

    # Préparer les données OpenWeatherMap
    if not openweather_data.empty:
        openweather_expanded = expand_openweather_data(openweather_data)
        
        
     # 2. Variation de la température au cours du temps
    if not openmeteo_data.empty:
        st.subheader("Variation de la température au cours du temps (Open-Meteo)")
        openmeteo_expanded = expand_openmeteo_data(openmeteo_data)
        fig = px.line(openmeteo_expanded, x="time", y="temperature_2m", title="Variation de la température au cours du temps")
        st.plotly_chart(fig)
    
     # 5. Température moyenne par heure (Open-Meteo)
    if not openmeteo_data.empty:
        st.subheader("Température moyenne par heure (Open-Meteo)")
        openmeteo_expanded = expand_openmeteo_data(openmeteo_data)
        openmeteo_expanded["hour"] = openmeteo_expanded["time"].dt.hour
        hourly_avg_temp = openmeteo_expanded.groupby("hour")["temperature_2m"].mean().reset_index()
        fig = px.bar(hourly_avg_temp, x="hour", y="temperature_2m", title="Température moyenne par heure")
        st.plotly_chart(fig)

    # 6. Matrice de corrélation complète (OpenWeatherMap)
    if not openweather_data.empty:
        st.subheader("Matrice de corrélation (OpenWeatherMap)")
        numeric_cols = openweather_expanded.select_dtypes(include=["number"]).columns
        if len(numeric_cols) > 1:
            corr = openweather_expanded[numeric_cols].corr()
            fig = px.imshow(corr, text_auto=True, title="Matrice de corrélation complète")
            st.plotly_chart(fig)

    # 1. Distribution des températures
    if not openweather_data.empty:
        st.subheader("Distribution des températures (OpenWeatherMap)")
        fig = px.histogram(openweather_expanded, x="temperature", nbins=30, title="Distribution des températures")
        st.plotly_chart(fig)
    
    
    
    # 3. Relation entre humidité et température
    if not openweather_data.empty:
        st.subheader("Relation entre humidité et température (OpenWeatherMap)")
        fig = px.scatter(
            openweather_expanded,
            x="temperature",
            y="humidity",
            title="Relation entre humidité et température",
            labels={"humidity": "Humidité (%)", "temperature": "Température (°C)"}
        )
        st.plotly_chart(fig)
    
    # 4. Variation de la vitesse du vent
    if not openweather_data.empty:
        st.subheader("Variation de la vitesse du vent (OpenWeatherMap)")
        fig = px.line(
            openweather_expanded, 
            x="time", 
            y="wind_speed", 
            title="Variation de la vitesse du vent au cours du temps",
            labels={"wind_speed": "Vitesse du vent (m/s)", "time": "Temps"}
        )
        st.plotly_chart(fig)

# Navigation
if section == "Présentation des données":
    presentation_donnees()
elif section == "Répartition des températures":
    choix = st.radio(
        "Choisir le graphique",
        ("Répartition des températures selon l'heure de la journée",
         "Répartition des températures par plage de valeurs")
    )
    
    if choix == "Répartition des températures par plage de valeurs":
        repartition_plage_temperatures()
    elif choix == "Répartition des températures selon l'heure de la journée":
        repartition_temperatures_par_heure()
    

elif section == "Analyse avancée":
    analyse_avancee()