# Pinterest-Pipeline


### Overview 
This project is part of the AiCore career accelerator programme for Data Engineering. The aim of the project is to create an industry grade data pipeline with lambda architecture (batch and streaming processes) that manages high volumnes of data.

#### Technologies Used: 
- FastAPI 
- Kafka, kafka-python
- Amazon S3 - Cloud Datalake
- Spark / PySpark
- MAVEN
- Cassandra
- Presto
- Airflow
- Prometheus and Grafana 
- Spark Streaming
- Postgres
- Postgres-exporter
- Docker

#### Pinterest API (project_pin_API.py)
The API 'listens' for data on port 8000. This simulates the data that would be created when users interact with the pinterest app. The data is converted to a dictionary/json using a defined schema before being sent to a Kafka producer. Each message is encoded as bytes to be sent to a predifined 'topic'. 

________________________________________________________________________________

### Batch Processing 
> Kafka Consumer -> Amazon S3 bucket (datalake) -> PySpark -> Cassandra -> Presto

> Airflow: for orchestration

> Prometheus and Grafana: for monitoring cassandra 

##### batch_consumer.py 
Consume kafka messages via kafka-python in a batch and uploads to S3 using boto3. This is only in a batch of 20 for example purposes.  

##### s3_spark.py
This class reads the content of the S3 bucket, performs some basic data cleaning tasks such as replacing empty values, renaming columns, changing the data type (e.g. to integer) and re-ordering the columns. The data is then pushed to cassandra into a pre-defined table. 

##### cassandra_python.py 
This file is setting up the cassandra table that spark pushes to. 

##### airflowdag.py
This defines the airflow job of running the s3_spark.py file once a day at 0800 using a BashOperator task. 

_______________________________________________________________________________________

### Stream Processing 
> Kafka -> Spark Streaming -> Postgres 

> Prometheus and Grafana: monitoring postgres

##### kafka_sparkstream.py 
This file reads data from our Kafka topic using MAVEN pyspark packages. The data then needs to be converted from a json value into a dataframe by applying a schema. The same data cleaning steps are applied as for the batch job before the data is written to postgresql. This is perfomed in microbatches using the foreachBatch function. The data is overwritten because of the nature of the data simulation used in the project, but would usually be appended.
https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

##### docker-compose.yml
The metrics from postgres are connected to prometheus via the postgres exporter found at: https://github.com/prometheus-community/postgres_exporter. I found that combining the prometheus and postgres-exporter docker images was the most straightforward way to achieve this on a mac, with help from the following article: https://nelsoncode.medium.com/how-to-monitor-posgresql-with-prometheus-and-grafana-docker-36d216532ea2. Prometheus can be accessed by the localhost due to the port matching in the docker-compose file. There are extra metrics in the queries.yaml file which is mounted via a docker volume. 







