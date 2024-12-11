# Orchestration of a Big Data Pipeline for Data Visualization

This project represents an orchestration of a Big Data pipeline for data visualization by integrating various powerful technologies, each playing a specific role in the data processing and visualization workflow.

## Technologies Used

- **Apache Airflow** for task orchestration
- **Kafka** for real-time data streaming
- **Cassandra** for distributed storage
- **Spark SQL** for large-scale data processing
- **Spark ML** for applying machine learning models
- **Streamlit** for interactive data visualization

## Architecture

![image (1)](https://github.com/user-attachments/assets/346eaef9-6fa9-46fb-ada6-760ae4745675)

## how to run

1. Clone this repository: `https://github.com/IkramEddamnati-dev/Weather_pipeline_Big_Data.git`
2. Navigate to the project directory: `cd Weather_pipeline_Big_Data`
3. Convert the file `entrypoint.sh` to Unix format using:  
   `dos2unix ./script/entrypoint.sh`
4. Run: `docker-compose up -d`
5. Run: `docker cp dependencies.zip spark-master:/dependencies.zip`

## Execution

1. Access the Airflow interface in `http://localhost:8086/home` and run the DAG:
   ![Airflow Interface]<img width="959" alt="image" src="https://github.com/user-attachments/assets/8db70e39-d1ae-4958-b23a-977c08a10f89">

2. In the Kafka UI, you can view the topics that have been created. To do so, navigate to the following URL: `http://localhost:8085/ui/clusters/local/all-topics`
   You should be able to see the list of topics created in your Kafka cluster.
   ![Kafka UI Topics](https://github.com/user-attachments/assets/6a0685e8-81db-46bd-8a9f-724ce031a376)
3. The data will be stored locally in Parquet file format.
4. isualizations of the data from the Parquet files will be displayed in Streamlit. ![WhatsApp Image 2024-12-10 at 22 11 28](https://github.com/user-attachments/assets/7dd7aef5-5b65-425c-8f5e-6bdbf2caa4b1) ![WhatsApp Image 2024-12-10 at 22 11 54](https://github.com/user-attachments/assets/90b7e4df-3708-4caf-ad4c-73ad9be50bb6) ![WhatsApp Image 2024-12-10 at 22 12 42](https://github.com/user-attachments/assets/9ec20804-b625-4204-8c4a-3e181165f997) ![WhatsApp Image 2024-12-10 at 22 13 25](https://github.com/user-attachments/assets/bbdb936c-3e6a-4fe9-84e1-711294d55bf7)![WhatsApp Image 2024-12-10 at 22 19 42](https://github.com/user-attachments/assets/38cbe09f-363f-4f0d-8100-00222e6d7f97)

5. The data will then be aggregated using Spark SQL and sent to Cassandra. You can verify this by executing the following commands:`docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042` and `SELECT count(*)FROM spark_streaming.weather_data;` ![image](https://github.com/user-attachments/assets/720fd8ec-326d-4a1c-9b99-973b5a218e5f)
6. Afterward, our pre-trained offline model will predict new factors, and these predictions will be stored in Cassandra.
7. Finally, a visualization comparing the actual values with the predicted ones will be displayed in Streamlit.
