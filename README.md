# Pinterest-Pipeline


### Overview 
This project is part of the AiCore career accelerator programme for Data Engineering. The aim of the project is to create an industry grade data pipeline with lambda architecture (batch and streaming processes) that manages high volumnes of data.

#### Technologies Used: 
FastAPI 
Kafka, kafka-python
Amazon S3 - Cloud Datalake
Spark / PySpark
Cassandra
Presto
Airflow
Prometheus and Grafana 

#### Pinterest API (project_pin_API.py)
The API 'listens' for data on port 8000. This data simulates what would be created when users interact with the pinterest app. The data is converted to a dictionary using a defined schema before being sent to a Kafka producer. Each message is encoded as bytes to be sent to a predifined topic. 


________________________________________________________________________________

### Batch processing 
Kafka Consumer -> Amazon S3 bucket (datalake) -> PySpark -> Cassandra -> Presto

Airflow: for orchestration
Prometheus and Grafana: for monitoring cassandra 

##### batch_consumer.py 
Consume kafka messages via kafka-python in a batch and uploads to S3 using boto3. This is only in a batch of 20 for example purposes.  

##### s3_spark.py
This class reads the content of the S3 bucket, performs some basic data cleaning tasks such as replacing empty values, renaming columns, changing the data type (e.g. to integer) and re-ordering the columns. The data is then pushed to cassandra into a pre-defined table. 

##### cassandra_python.py 
This file is setting up the cassandra table that spark pushes to. 

##### airflowdag.py
This defines the airflow job of running the s3_spark.py file once a day at 0800 using a BashOperator task. 

_______________________________________________________________________________________

### Stream processing 
Kafka Consumer -> Spark Streaming -> Postgres 

Prometheus and Grafana: monitoring postgres 

