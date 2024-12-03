# Orchestration of a Big Data Pipeline for Data Visualization

This project represents an orchestration of a Big Data pipeline for data visualization by integrating various powerful technologies, each playing a specific role in the data processing and visualization workflow.

## Technologies Used

- **Apache Airflow** for task orchestration
- **Kafka** for real-time data streaming
- **Cassandra** for distributed storage
- **Spark SQL** for large-scale data processing
- **Spark ML** for applying machine learning models
- **Streamlit** for interactive data visualization

## Installation

1. Clone this repository: `https://github.com/IkramEddamnati-dev/Weather_pipeline_Big_Data.git`
2. Navigate to the project directory: `cd Weather_pipeline_Big_Data`
3. Convert the file `entrypoint.sh` to Unix format using:  
   `dos2unix ./script/entrypoint.sh`
4. Run: `docker-compose up -d`
5. Run: `docker cp dependencies.zip spark-master:/dependencies.zip`

## Execution

1. Access the Airflow interface in `http://localhost:8086/home` and run the DAG:
   ![Airflow Interface]<img width="959" alt="image" src="https://github.com/user-attachments/assets/8db70e39-d1ae-4958-b23a-977c08a10f89">

2. In the Kafka UI, you can view the topics that have been created. To do so, navigate to the following URL:  `http://localhost:8085/ui/clusters/local/all-topics?perPage=25`
    You should be able to see the list of topics created in your Kafka cluster.
    ![Kafka UI Topics](https://github.com/user-attachments/assets/6a0685e8-81db-46bd-8a9f-724ce031a376)
