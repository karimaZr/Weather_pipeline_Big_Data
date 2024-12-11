import pandas as pd
import numpy as np
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_squared_error, mean_absolute_percentage_error
from sklearn.model_selection import train_test_split
import joblib
import os

# Paths
model_dir = "/tmp/models"
os.makedirs(model_dir, exist_ok=True)
# Ensure the directory exists, create it if not
output_dir = os.path.dirname(model_dir)
if not os.path.exists(model_dir):
    os.makedirs(model_dir)

# Load data
df = pd.read_csv("/weather_data1.csv")
df['time'] = pd.to_datetime(df['time'])
df = df.sort_values('time')

# Cities and variables
# Liste des villes pour lesquelles faire des prédictions
cities = [
    "Casablanca", "Rabat", "Fes", "Marrakesh", "Agadir", 
    "Tangier", "Meknes", "Oujda", "Kenitra", "Tétouan", 
    "Safi", "El Jadida", "Nador", "Khemisset", "Settat", 
    "Larache", "Guelmim", "Taza", "Tiznit Province", "Beni Mellal", 
    "Mohammedia", "Mogador", "Ksar el-Kebir", "Ouarzazate Province", 
    "Al Hoceima", "Berkane", "Taourirt", "Errachidia", "Khouribga Province", 
    "Oued Zem", "Sidi Slimane", "Sidi Kacem", "Azrou", "Midelt", 
    "Ifrane", "Chefchaouen", "Laayoune"
]

# Liste des variables à prévoir
variables = ["humidity", "pressure", "sea_level","temp", 
             "visibility", "wind_speed"]
             
# Training and saving models
for city_name in cities:
    city_data = df[df['city_name'] == city_name]

    if city_data.empty:
        print(f"No data for city {city_name}")
        continue

    city_data.set_index('time', inplace=True)
    city_data.fillna(method='ffill', inplace=True)

    for var in variables:
        if var not in city_data.columns:
            continue

        # Train/Test split
        train, test = train_test_split(city_data[var], test_size=0.2, shuffle=False)

        # Train ARIMA
        try:
            arima_model = ARIMA(train, order=(1, 1, 1)).fit()
            arima_rmse = np.sqrt(mean_squared_error(test, arima_model.forecast(len(test))))
        except:
            arima_model, arima_rmse = None, float('inf')

        # Train SARIMAX
        try:
            sarimax_model = SARIMAX(train, order=(1, 1, 1), seasonal_order=(1, 1, 1, 12)).fit(disp=False)
            sarimax_rmse = np.sqrt(mean_squared_error(test, sarimax_model.forecast(len(test))))
        except:
            sarimax_model, sarimax_rmse = None, float('inf')

        # Select best model
        best_model = arima_model if arima_rmse < sarimax_rmse else sarimax_model
        model_name = "ARIMA" if arima_rmse < sarimax_rmse else "SARIMAX"

        # Save model
        if best_model:
            model_path = f"{model_dir}/{city_name}_{var}_{model_name}.pkl"
            joblib.dump(best_model, model_path)
            print(f"Saved {model_name} for {city_name}, {var}")
