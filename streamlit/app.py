import streamlit as st
import pandas as pd
from pathlib import Path

# Define the directory where Parquet files are stored
DATA_DIR = Path("/opt/airflow/data/parquet_files")

# App title and description
st.title('Weather Data Visualizer')
st.write('This app visualizes weather data stored in Parquet files.')

# Function to load a Parquet file
def load_data(file_path):
    try:
        return pd.read_parquet(file_path, engine="pyarrow")  # Explicit engine for compatibility
    except Exception as e:
        st.error(f"Error loading {file_path}: {e}")
        return pd.DataFrame()

# Get a list of available Parquet files
parquet_files = list(DATA_DIR.glob("*.parquet"))
if parquet_files:
    selected_file = st.selectbox("Select a Parquet file to visualize:", parquet_files)
    if selected_file:
        # Load data
        data = load_data(selected_file)

        if not data.empty:
            # Display metadata about the dataset
            st.write(f"### Metadata for `{selected_file.name}`")
            st.write(f"- **Number of rows**: {data.shape[0]}")
            st.write(f"- **Number of columns**: {data.shape[1]}")
            st.write(f"- **Column names and types**:")
            st.write(data.dtypes)

            # Display raw data
            st.write(f"### Preview of `{selected_file.name}`")
            st.dataframe(data.head())  # Show the first 5 rows

            # Visualization
            numeric_columns = data.select_dtypes(include=['float64', 'int64']).columns.tolist()
            if numeric_columns:
                selected_column = st.selectbox("Select a numeric column to visualize:", numeric_columns)
                st.write(f"### Line Chart for `{selected_column}`")
                st.line_chart(data[selected_column], use_container_width=True)
            else:
                st.write("No numeric columns available for visualization.")
else:
    st.write("No Parquet files found in the specified directory.")
