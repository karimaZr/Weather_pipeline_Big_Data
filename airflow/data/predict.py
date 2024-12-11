from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit, col
from pyspark.sql.types import FloatType, StringType, StructType, StructField
import joblib
import os
from datetime import datetime, timedelta

# Initialize Spark
spark = SparkSession.builder.appName("TimeSeriesUDF").getOrCreate()

# List of cities and variables
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
variables = ["humidity", "pressure", "sea_level", "temp", "visibility", "wind_speed"]

# Define UDF for prediction
def predict(city, var, last_time):
    model_path_arima = f"/tmp/models/{city}_{var}_ARIMA.pkl"
    model_path_sarimax = f"/tmp/models/{city}_{var}_SARIMAX.pkl"

    # Check which model exists
    if os.path.exists(model_path_arima):
        model = joblib.load(model_path_arima)
    elif os.path.exists(model_path_sarimax):
        model = joblib.load(model_path_sarimax)
    else:
        return None, None  # If no model found, return None

    # Make prediction
    forecast = model.forecast(steps=1)
    
    # Calculate predicted time (adding 10 minutes to last time)
    predicted_time = (last_time + timedelta(minutes=10)).strftime('%Y-%m-%d %H:%M:%S')  # Format time as string
    
    if not forecast.empty:
     return float(forecast.iloc[0]), predicted_time
    else:
    # Handle the case where forecast is empty
     return None, predicted_time


# Define the return type of the UDF
schema = StructType([
    StructField("predicted_value", FloatType(), False),
    StructField("time_of_prediction", StringType(), False)
])

# Register UDF
predict_udf = udf(predict, schema)

# Load data into Spark
spark_df = spark.read.csv("/weather_data.csv", header=True, inferSchema=True)

# Add predictions to the DataFrame
predictions = []
for city in cities:
    for var in variables:
        # Filter data for the current city and variable
        city_var_df = spark_df.filter(col("city_name") == city)
        city_var_df = city_var_df.withColumn("variable", lit(var))  # Add variable as a column
        
        # Apply UDF to predict values and predicted time
        city_var_df = city_var_df.withColumn(
            "prediction_and_time", 
            predict_udf(lit(city), lit(var), col("time"))
        )
        
        # Split the tuple into predicted_value and time_of_prediction
        city_var_df = city_var_df.withColumn(
            "predicted_value", col("prediction_and_time.predicted_value")
        ).withColumn(
            "time_of_prediction", col("prediction_and_time.time_of_prediction")
        ).drop("prediction_and_time")
        
        # Select relevant columns
        city_var_df = city_var_df.select(
            "city_name", "time", "variable", "predicted_value", "time_of_prediction"
        )
        
        # Collect predictions for combining later
        predictions.append(city_var_df)

# Combine all predictions into a single DataFrame
final_df = predictions[0]
for df in predictions[1:]:
    final_df = final_df.union(df)

# Get the current timestamp to use in the file name
current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Generate the output file name based on the current time
output_file_name = f"/tmp/output_predictions_{current_time}.csv"

# Save the results with the dynamic file name
final_df.write.csv(output_file_name, header=True)

print(f"Predictions saved to {output_file_name}")
