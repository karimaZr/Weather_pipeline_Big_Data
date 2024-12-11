#!/bin/bash

docker cp ./data/parquet_files/openweathermap_data.parquet spark-master:/openweathermap_data.parquet
docker cp ./data/parquet_files/openmeteo_data.parquet spark-master:/openmeteo_data.parquet
docker cp ./data/spark_stream.py spark-master:/spark_stream.py
docker exec -it spark-master spark-submit \
    --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
    --py-files /dependencies.zip \
    /spark_stream.py
