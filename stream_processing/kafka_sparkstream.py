from pyspark.sql import SparkSession
import os
from pyspark.sql.types import *
from pyspark.sql.column import *
from pyspark.sql.dataframe import *
from pyspark.sql.functions import *
from pyspark.sql.utils import *

# os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 --driver-class-path /Users/emmasamouelle/Desktop/Scratch/pinterest_project/postgresql-42.4.0.jar --jars /Users/emmasamouelle/Desktop/Scratch/pinterest_project/postgresql-42.4.0.jar pyspark-shell" #3.2.0
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.5.0 pyspark-shell"
password = os.environ["POSTGRES_PASSWORD"]

bootstrap_servers="localhost:9092"
kafka_topic_name="PinterestTopic"

spark = SparkSession \
    .builder \
    .appName("Kafka") \
    .getOrCreate()

schema = StructType() \
    .add("index", IntegerType()) \
    .add("unique_id", StringType()) \
    .add("title", StringType()) \
    .add("description", StringType()) \
    .add("follower_count", StringType()) \
    .add("tag_list", StringType()) \
    .add("is_image_or_video", StringType()) \
    .add("image_src", StringType()) \
    .add("downloaded", IntegerType()) \
    .add("save_location", StringType()) \
    .add("category", StringType()) 

data_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", kafka_topic_name) \
    .option("startingOffsets", "latest") \
    .load()


def foreach_batch_function(df, epoch_id):
    df = df.selectExpr("CAST(value as STRING)")
    df = df.withColumn('value', from_json(df.value, schema)).select('value.*')
    df = df.withColumnRenamed('index', 'id_index')
    df = df.withColumn('downloaded', df['downloaded'].cast('int'))
    df = df.withColumn('id_index', df['id_index'].cast('int'))
    df = df.na.replace('Image src error.', None) \
            .na.replace('N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e', None) \
            .na.replace('User Info Error', None) \
            .na.replace('No description available Story format', None) \
            .na.replace('No Title Data Available', None)

    df.write.format('jdbc') \
        .mode("overwrite") \
        .option("url", "jdbc:postgresql://localhost:5432/pinterest") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "pinterest.exp") \
        .option("user", "postgres") \
        .option("password", password) \
        .save()
    # df.write.format('console').mode('append').save()

data_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination() 
