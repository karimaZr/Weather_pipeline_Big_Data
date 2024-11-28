import requests
import csv

# Liste des villes marocaines (ajoutez-en si nécessaire)
cities = [
    "Casablanca", "Rabat", "Fès", "Marrakech", "Agadir", "Tanger", 
    "Meknès", "Oujda", "Kenitra", "Tetouan", "El Jadida", "Safi", 
    "Beni Mellal", "Nador", "Taza", "Essaouira", "Khouribga", 
    "Settat", "Larache", "Ksar El Kebir", "Taourirt", "Guercif", 
    "Guelmim", "Dakhla", "Laayoune", "Ifrane", "Azrou", 
    "Errachidia", "Ouarzazate", "Taroudant", "Chefchaouen", "Berkane", 
    "Sidi Kacem", "Sidi Slimane", "Sidi Ifni", "Tiznit", "Tan-Tan"
]


# Base URL pour l'API Nominatim
base_url = "https://nominatim.openstreetmap.org/search"

# Fichier de sortie
output_file = "morocco_cities.csv"

# En-têtes pour le fichier CSV
fieldnames = ["City", "Latitude", "Longitude"]

# Initialisation du fichier CSV
with open(output_file, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()
    
    # Requête pour chaque ville
    for city in cities:
        params = {
            "country": "Morocco",
            "city": city,
            "format": "json"
        }
        
        try:
            response = requests.get(base_url, params=params, headers={"User-Agent": "city-fetcher"})
            response.raise_for_status()
            data = response.json()
            
            if data:
                # Récupération de la première correspondance
                city_data = data[0]
                writer.writerow({
                    "City": city,
                    "Latitude": city_data["lat"],
                    "Longitude": city_data["lon"]
                })
                print(f"Added {city}: {city_data['lat']}, {city_data['lon']}")
            else:
                print(f"No data found for {city}")
        
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {city}: {e}")

print(f"Data saved to {output_file}")
