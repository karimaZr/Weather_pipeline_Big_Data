import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

# Définir le répertoire des fichiers Parquet
DATA_DIR = Path("/opt/airflow/data/parquet_files")
PREDICTIONS_FILE = Path("/opt/airflow/data/output_predictions.csv")
# Fonction pour charger les fichiers Parquet
def load_data(file_name):
    file_path = DATA_DIR / file_name
    if file_path.exists():
        return pd.read_parquet(file_path)
    else:
        st.error(f"Le fichier {file_name} n'existe pas dans {DATA_DIR}.")
        return pd.DataFrame()

def load_all_predictions(directory):
    all_files = directory.glob("*.csv")
    predictions = []

    for file in all_files:
        try:
            data = pd.read_csv(file)
            first_records = data.groupby("city_name").first().reset_index()
            predictions.append(first_records)
        except Exception as e:
            st.error(f"Erreur lors du chargement de {file.name}: {e}")

    if predictions:
        return pd.concat(predictions, ignore_index=True)
    else:
        st.warning("Aucun fichier de prédictions valide trouvé.")
        return pd.DataFrame()
    
# Fonction pour préparer les données OpenWeatherMap
def expand_openweather_data(data):
    data["sea_level"] = data["main"].apply(lambda x: x["sea_level"] if isinstance(x, dict) else None)
    data["humidity"] = data["main"].apply(lambda x: x["humidity"] if isinstance(x, dict) else None)
    data["grnd_level"] = data["main"].apply(lambda x: x["grnd_level"] if isinstance(x, dict) else None)
    data["feels_like"] = data["main"].apply(lambda x: x["feels_like"] if isinstance(x, dict) else None)
    data["temperature"] = data["main"].apply(lambda x: x["temp"] if isinstance(x, dict) else None)
    data["temperature_max"] = data["main"].apply(lambda x: x["temp_max"] if isinstance(x, dict) else None)
    data["temperature_min"] = data["main"].apply(lambda x: x["temp_min"] if isinstance(x, dict) else None)
    
    data["wind_speed"] = data["wind"].apply(lambda x: x["speed"] if isinstance(x, dict) else None)
    data["wind_deg"] = data["wind"].apply(lambda x: x["deg"] if isinstance(x, dict) else None)
    
    data["clouds"] = data["clouds"].apply(lambda x: x["all"] if isinstance(x, dict) else None)
    
    data["country"] = data["sys"].apply(lambda x: x["country"] if isinstance(x, dict) else None)
    data["latitude"] = data["coord"].apply(lambda x: x["lat"] if isinstance(x, dict) else None)
    data["longitude"] = data["coord"].apply(lambda x: x["lon"] if isinstance(x, dict) else None)
    
    data["time"] = pd.to_datetime(data["dt"], unit="s")
    return data

# Fonction pour préparer les données Open-Meteo
def expand_weather_data(data):
    data["apparent_temperature"] = data["current"].apply(lambda x: x["apparent_temperature"] if isinstance(x, dict) else None)
    data["cloud_cover"] = data["current"].apply(lambda x: x["cloud_cover"] if isinstance(x, dict) else None)
    data["precipitation"] = data["current"].apply(lambda x: x["precipitation"] if isinstance(x, dict) else None)
    data["pressure_msl"] = data["current"].apply(lambda x: x["pressure_msl"] if isinstance(x, dict) else None)
    data["rain"] = data["current"].apply(lambda x: x["rain"] if isinstance(x, dict) else None)
    data["relative_humidity_2m"] = data["current"].apply(lambda x: x["relative_humidity_2m"] if isinstance(x, dict) else None)
    data["showers"] = data["current"].apply(lambda x: x["showers"] if isinstance(x, dict) else None)
    data["snowfall"] = data["current"].apply(lambda x: x["snowfall"] if isinstance(x, dict) else None)
    data["surface_pressure"] = data["current"].apply(lambda x: x["surface_pressure"] if isinstance(x, dict) else None)
    data["temperature_2m"] = data["current"].apply(lambda x: x["temperature_2m"] if isinstance(x, dict) else None)
    data["time"] = data["current"].apply(lambda x: x["time"] if isinstance(x, dict) else None)
    data["weather_code"] = data["current"].apply(lambda x: x["weather_code"] if isinstance(x, dict) else None)
    
    return data

# Chargement des données
openweather_data = load_data("openweathermap_data.parquet")
openmeteo_data = load_data("openmeteo_data.parquet")
predictions_data = load_all_predictions(PREDICTIONS_FILE)



# Barre latérale pour la navigation
st.sidebar.title("Explorer les Données 🔍")
section = st.sidebar.radio(
    "Choisir une section :",
    ["🌤️ Présentation des données", "📈 Visualisations OpenWeatherMap et Open-Meteo", "📊 Comparaison des Données" , "📉 Comparaison Réel vs Prédit"]
)

# --- 1. Présentation des données ---
def presentation_donnees():
    st.title("🌦️ Présentation des données")
    
    if not openweather_data.empty:
        st.subheader("Données OpenWeatherMap")
        openweather_expanded = expand_openweather_data(openweather_data)
        # Supprimer les colonnes indésirables et réinitialiser l'index pour éviter son affichage
        openweather_expanded = openweather_expanded.drop(columns=["main", "coord", "wind", "sys", "dt","weather","base"], errors="ignore").reset_index(drop=True)
        openweather_expanded = openweather_expanded.rename(columns={"name": "city"})
        st.dataframe(openweather_expanded)  # Affiche les données nettoyées

    if not openmeteo_data.empty:
        st.subheader("Données Open-Meteo")
        openmeteo_expanded = expand_weather_data(openmeteo_data)
        # Supprimer les colonnes indésirables et réinitialiser l'index pour éviter son affichage
        openmeteo_expanded = openmeteo_expanded.drop(columns=["current_units","current","utc_offset_seconds",""], errors="ignore").reset_index(drop=True)
        openmeteo_expanded= openmeteo_expanded.rename(columns={"city_name": "city"})
        st.dataframe(openmeteo_expanded)  # Affiche les données nettoyées


# --- 2. Visualisations OpenWeatherMap et Open-Meteo ---
def visualisations():
    st.title("📊 Visualisations des Données OpenWeatherMap et Open-Meteo")
    
    # Titre pour OpenWeatherMap
    st.header("OpenWeatherMap")
    
      # 1. Température vs Humidité
    if not openweather_data.empty:
        openweather_expanded = expand_openweather_data(openweather_data)
        fig = px.scatter(openweather_expanded, x="temperature", y="humidity", 
                         title="Variation de l'Humidité en Fonction de la Température")
        st.plotly_chart(fig)

    # 2. Répartition des températures
    if not openweather_data.empty:
        fig = px.histogram(openweather_expanded, x="temperature", nbins=30, 
                            title="Répartition des Températures")
        st.plotly_chart(fig)

    # 3. Distribution des températures par plages (Graphique Circulaire)
    if not openweather_data.empty:
        # Définir les plages de température
        bins = [-10, 0, 10, 20, 30, 40, 50]  # Plages de température
        labels = ['<0°C', '0-10°C', '10-20°C', '20-30°C', '30-40°C', '>=40°C']
        
        # Créer une colonne avec les plages de température
        openweather_expanded['temp_range'] = pd.cut(openweather_expanded['temperature'], bins=bins, labels=labels, right=False)
        
        # Compter les occurrences dans chaque plage
        temperature_counts = openweather_expanded['temp_range'].value_counts().reset_index()
        temperature_counts.columns = ['Temperature Range', 'Count']
        
        # Créer le graphique circulaire
        fig = px.pie(temperature_counts, names='Temperature Range', values='Count', title="Répartition des Températures OpenWeatherMap")
        st.plotly_chart(fig)
        
    # 4. Nuages et couverture nuageuse - OpenWeatherMap
    if not openweather_data.empty:
        fig = px.scatter(openweather_expanded, x="time", y="clouds", title="Variation de la Couverture Nuageuse au Fil du Temps")
        st.plotly_chart(fig)
        
        
    # 10. Graphique à barres - Répartition de la visibilité par ville
    if not openweather_data.empty:
        visibility_counts = openweather_expanded.groupby("name")["visibility"].mean().reset_index()
        fig = px.bar(visibility_counts, x="name", y="visibility", title="Répartition de la Visibilité par Ville")
        st.plotly_chart(fig)
        
     # 11. Graphique à barres - Répartition de l'humidité et température par ville
    if not openweather_data.empty:
        # Calculer la température moyenne et l'humidité moyenne par ville
        city_weather = openweather_expanded.groupby("name")[["temperature", "humidity"]].mean().reset_index()
        
        # Créer un graphique à barres avec deux séries : Humidité et Température
        fig = px.bar(city_weather, x="name", y=["temperature", "humidity"], 
                     title="Répartition de l'Humidité et Température par Ville", 
                     labels={"name": "Ville", "value": "Valeur", "variable": "Type de donnée"})
        st.plotly_chart(fig)
        
    # 5. Distribution de l'humidité - OpenWeatherMap
    if not openweather_data.empty:
        fig = px.histogram(openweather_expanded, x="humidity", nbins=30, title="Distribution de l'Humidité")
        st.plotly_chart(fig)
        
     
    
    # 7. Graphique à barres - Température vs Température ressentie
    if not openweather_data.empty:
        fig = px.bar(openweather_expanded, x="time", y=["temperature", "feels_like"], 
                     title="Comparaison entre Température et Température Ressentie au Cours du Temps")
        st.plotly_chart(fig)
        
    # 8. Graphique à barres - Température minimale vs Température maximale
    if not openweather_data.empty:
        fig = px.bar(openweather_expanded, x="time", y=["temperature_min", "temperature_max"], 
                     title="Évolution de la Température Minimale et Maximale au Cours du Temps")
        st.plotly_chart(fig)
        
        
     

    # Titre pour Open-Meteo
    st.header("Open-Meteo")
    
    # 11. Humidité relative et température
    if not openmeteo_data.empty:
        openmeteo_expanded = expand_weather_data(openmeteo_data)
        fig = px.scatter(openmeteo_expanded, x="temperature_2m", y="relative_humidity_2m", 
                         title="Relation entre Humidité Relative et Température")
        st.plotly_chart(fig)
        
    # 12. Répartition des températures - Open-Meteo
    if not openmeteo_data.empty:
        bins = [-10, 0, 10, 20, 30, 40, 50]  # Plages de température
        labels = ['<0°C', '0-10°C', '10-20°C', '20-30°C', '30-40°C', '>=40°C']
        openmeteo_expanded['temp_range'] = pd.cut(openmeteo_expanded['temperature_2m'], bins=bins, labels=labels, right=False)
        temperature_counts = openmeteo_expanded['temp_range'].value_counts().reset_index()
        temperature_counts.columns = ['Temperature Range', 'Count']
        fig = px.pie(temperature_counts, names='Temperature Range', values='Count', title="Répartition des Températures Open-Meteo")
        st.plotly_chart(fig) 
    
     # 7. Température apparente et pression - Open-Meteo
    if not openmeteo_data.empty:
        openmeteo_expanded = expand_weather_data(openmeteo_data)
        fig = px.scatter(openmeteo_expanded, x="apparent_temperature", y="pressure_msl", title="Relation entre Température Apparente et Pression Atmosphérique")
        st.plotly_chart(fig)
        
        # 15. Graphique circulaire - Distribution de l'humidité
    if not openmeteo_data.empty:
          openmeteo_expanded = expand_weather_data(openmeteo_data)
          # Définir les catégories d'humidité
          bins_humidity = [0, 30, 60, 90, 100]  # Plages d'humidité
          labels_humidity = ['<30%', '30-60%', '60-90%', '>=90%']
    
          # Créer une colonne pour les catégories d'humidité
          openmeteo_expanded['humidity_range'] = pd.cut(openmeteo_expanded['relative_humidity_2m'], bins=bins_humidity, labels=labels_humidity, right=False)
    
          # Compter les occurrences de chaque plage d'humidité
          humidity_counts = openmeteo_expanded['humidity_range'].value_counts().reset_index()
          humidity_counts.columns = ['Humidity Range', 'Count']
    
          # Créer un graphique circulaire
          fig = px.pie(humidity_counts, names='Humidity Range', values='Count', title="Distribution de l'Humidité Open-Meteo")
          st.plotly_chart(fig)
          
# --- 3. Comparaison des données --- 
# --- 3. Comparaison des données --- 
def comparaison_donnees():
    st.title("📊 Comparaison des Données OpenWeatherMap et Open-Meteo")
    st.write("Cette section compare les distributions de la température, de l'humidité et de la pression entre OpenWeatherMap et Open-Meteo.")
    
    if not openweather_data.empty and not openmeteo_data.empty:
        # Étendre les données OpenWeatherMap
        openweather_expanded = expand_openweather_data(openweather_data)
        # Étendre les données Open-Meteo
        openmeteo_expanded = expand_weather_data(openmeteo_data)

        # 1. Comparaison de la distribution des Températures - OpenWeatherMap et Open-Meteo
        st.header("Comparaison des Températures Moyennes par Ville")
        
        # Calcul des températures moyennes par ville pour OpenWeatherMap
        openweather_temp = openweather_expanded.groupby("name")["temperature"].mean().reset_index()
        openweather_temp["source"] = "OpenWeatherMap"
        
        # Calcul des températures moyennes par ville pour Open-Meteo
        openmeteo_temp = openmeteo_expanded.groupby("city_name")["temperature_2m"].mean().reset_index()
        openmeteo_temp["source"] = "Open-Meteo"
        
        # Renommer la colonne 'city_name' de Open-Meteo pour correspondre à 'name' (nom de la colonne dans OpenWeatherMap)
        openmeteo_temp = openmeteo_temp.rename(columns={"city_name": "name"})
        
        # Fusionner les deux DataFrames sur la colonne 'name'
        temp_comparison = pd.concat([openweather_temp[["name", "temperature", "source"]],
                                     openmeteo_temp[["name", "temperature_2m", "source"]].rename(columns={"temperature_2m": "temperature"})])

        # Créer un graphique de comparaison des températures
        fig_temp = px.bar(temp_comparison, 
                          x="name", 
                          y="temperature", 
                          color="source", 
                          title="Comparaison des Températures Moyennes par Ville",
                          labels={"temperature": "Température (°C)", "name": "Ville"},
                          barmode="group",
                          color_discrete_map={"OpenWeatherMap": "blue", "Open-Meteo": "red"})
        st.plotly_chart(fig_temp)

        # 2. Comparaison de l'humidité - OpenWeatherMap et Open-Meteo
        st.header("Comparaison de l'Humidité Moyenne par Ville")
        
        # Calcul de l'humidité moyenne par ville pour OpenWeatherMap
        openweather_humidity = openweather_expanded.groupby("name")["humidity"].mean().reset_index()
        openweather_humidity["source"] = "OpenWeatherMap"
        
        # Calcul de l'humidité moyenne par ville pour Open-Meteo
        openmeteo_humidity = openmeteo_expanded.groupby("city_name")["relative_humidity_2m"].mean().reset_index()
        openmeteo_humidity["source"] = "Open-Meteo"
        
        # Renommer la colonne 'city_name' de Open-Meteo pour correspondre à 'name'
        openmeteo_humidity = openmeteo_humidity.rename(columns={"city_name": "name"})
        
        # Fusionner les deux DataFrames sur la colonne 'name'
        humidity_comparison = pd.concat([openweather_humidity[["name", "humidity", "source"]],
                                        openmeteo_humidity[["name", "relative_humidity_2m", "source"]].rename(columns={"relative_humidity_2m": "humidity"})])

        # Créer un graphique de comparaison de l'humidité
        fig_humidity = px.bar(humidity_comparison, 
                              x="name", 
                              y="humidity", 
                              color="source", 
                              title="Comparaison de l'Humidité Moyenne par Ville",
                              labels={"humidity": "Humidité (%)", "name": "Ville"},
                              barmode="group",
                              color_discrete_map={"OpenWeatherMap": "blue", "Open-Meteo": "red"})
        st.plotly_chart(fig_humidity)
        
        # 3. Comparaison de la Pression - OpenWeatherMap et Open-Meteo
        st.header("Comparaison de la Pression Moyenne par Ville")
        
        # Calcul de la pression moyenne par ville pour OpenWeatherMap
        openweather_pressure = openweather_expanded.groupby("name")["grnd_level"].mean().reset_index()
        openweather_pressure["source"] = "OpenWeatherMap"
        
        # Calcul de la pression moyenne par ville pour Open-Meteo
        openmeteo_pressure = openmeteo_expanded.groupby("city_name")["surface_pressure"].mean().reset_index()
        openmeteo_pressure["source"] = "Open-Meteo"
        
        # Renommer la colonne 'city_name' de Open-Meteo pour correspondre à 'name'
        openmeteo_pressure = openmeteo_pressure.rename(columns={"city_name": "name"})
        
        # Fusionner les deux DataFrames sur la colonne 'name'
        pressure_comparison = pd.concat([openweather_pressure[["name", "grnd_level", "source"]],
                                        openmeteo_pressure[["name", "surface_pressure", "source"]].rename(columns={"surface_pressure": "grnd_level"})])

        # Créer un graphique de comparaison de la pression
        fig_pressure = px.bar(pressure_comparison, 
                              x="name", 
                              y="grnd_level", 
                              color="source", 
                              title="Comparaison de la Pression Moyenne par Ville",
                              labels={"grnd_level": "Pression (hPa)", "name": "Ville"},
                              barmode="group",
                              color_discrete_map={"OpenWeatherMap": "blue", "Open-Meteo": "red"})
        st.plotly_chart(fig_pressure)




import pandas as pd
import plotly.express as px
import streamlit as st

def comparaison_reel_vs_predit():
    st.title("📉 Comparaison Réel vs Prédit")
    
    # Vérification des données
    if openweather_data.empty or predictions_data.empty:
        st.warning("Veuillez charger les données réelles et les prédictions pour effectuer la comparaison.")
        return
    
    # # Affichage des colonnes de chaque dataset pour vérifier leur structure
    # st.subheader("Colonnes des données réelles (OpenWeatherMap):")
    # st.write(openweather_data.columns)  # Affiche les colonnes de openweather_data
    
    # st.subheader("Colonnes des données prédites (Predictions):")
    # st.write(predictions_data.columns)  # Affiche les colonnes de predictions_data
    
    # Préparation des données OpenWeatherMap (Extrait les variables nécessaires du champ "main")
    openweather_expanded = openweather_data[["name", "main"]].copy()
    openweather_expanded = openweather_expanded.rename(columns={"name": "city"})
    
    # Extraire les variables spécifiques de la colonne 'main'
    openweather_expanded["temp_reel"] = openweather_expanded["main"].apply(lambda x: x.get("temp"))
    openweather_expanded["humidity_reel"] = openweather_expanded["main"].apply(lambda x: x.get("humidity"))
    openweather_expanded["pressure_reel"] = openweather_expanded["main"].apply(lambda x: x.get("pressure"))
    openweather_expanded["sea_level_reel"] = openweather_expanded["main"].apply(lambda x: x.get("sea_level"))
    
    # Suppression de la colonne 'main' après extraction des variables
    openweather_expanded = openweather_expanded.drop(columns=["main"])
    
    # Vérification des données après extraction des variables réelles
    st.subheader("Données OpenWeatherMap après extraction des variables:")
    st.write(openweather_expanded.head())  # Affiche les premières lignes de openweather_expanded
    
    # Préparation des données de prédiction
    predictions_filtered = predictions_data[["city_name", "variable", "predicted_value", "time_of_prediction"]]
    predictions_filtered = predictions_filtered.rename(columns={"city_name": "city"})
    
    # Ajouter des suffixes manuellement pour éviter les conflits de colonnes
    openweather_expanded = openweather_expanded.rename(columns=lambda x: x + "_reel" if x != "city" else x)
    predictions_filtered = predictions_filtered.rename(columns=lambda x: x + "_predit" if x != "city" else x)
    
    # Fusion des données sur la colonne "city"
    merged_data = pd.merge(openweather_expanded, predictions_filtered, on="city")
    
    # Vérification des colonnes après fusion
    st.subheader("Données après fusion des réels et des prédictions:")
    st.write(merged_data.head())  # Affiche les premières lignes de merged_data
    
    if merged_data.empty:
        st.error("Les données fusionnées sont vides. Vérifiez que les colonnes 'city' correspondent entre les fichiers réels et prédits.")
        return
    
    # Liste des variables à comparer
    variables = ["temp", "humidity", "pressure", "sea_level"]
    
    # Calcul des erreurs pour chaque variable seulement si les colonnes existent
    for variable in variables:
        if f"{variable}_reel" not in merged_data.columns or f"{variable}_predit" not in merged_data.columns:
            st.warning(f"Les colonnes pour {variable} sont manquantes dans les données fusionnées.")
            continue  # Passe à la prochaine variable s'il y a un problème avec les colonnes
        
        merged_data[f"error_{variable}"] = abs(merged_data[f"{variable}_reel"] - merged_data[f"{variable}_predit"])
    
    # Visualisation des erreurs pour chaque variable si les colonnes existent
    for variable in variables:
        if f"error_{variable}" in merged_data.columns:
            st.subheader(f"Erreur sur {variable.capitalize()}")
            fig_error = px.bar(merged_data, x="city", y=f"error_{variable}", title=f"Erreur Absolue sur {variable.capitalize()} par Ville")
            st.plotly_chart(fig_error)
    
    # Visualisation des valeurs réelles et prédites pour chaque variable si les colonnes existent
    for variable in variables:
        if f"{variable}_reel_reel" in merged_data.columns and f"{variable}_predit" in merged_data.columns:
            st.subheader(f"{variable.capitalize()} Réel vs Prédit")
            fig_comparison = px.line(merged_data, x="time_of_prediction_predit", y=[f"{variable}_reel", f"{variable}_predit"], 
                                     title=f"Comparaison des {variable.capitalize()} Réels et Prédites par Ville", color="city")
            st.plotly_chart(fig_comparison)

    # Visualisation de la tendance des erreurs globales (pour toutes les villes et variables)
    st.subheader("Tendance des erreurs globales")
    fig_error_trend = px.line(merged_data, x="time_of_prediction_predit", y=[f"error_{var}" for var in variables if f"error_{var}" in merged_data.columns], 
                              title="Tendance des Erreurs Globales sur toutes les Variables")
    st.plotly_chart(fig_error_trend)

# Afficher la section sélectionnée
if section == "🌤️ Présentation des données":
    presentation_donnees()
elif section == "📈 Visualisations OpenWeatherMap et Open-Meteo":
    visualisations()
elif section == "📊 Comparaison des Données":
    comparaison_donnees()
elif section == "📉 Comparaison Réel vs Prédit":   
    comparaison_reel_vs_predit()