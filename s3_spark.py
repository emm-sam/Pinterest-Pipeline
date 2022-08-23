import findspark
import os
# findspark.init(os.environ["SPARK_HOME"])
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import col 
from pyspark.sql.functions import regexp_replace


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 pyspark-shell'

conf = SparkConf() \
    .setAppName('S3toSparktoCassandra') \
    .setMaster('local[*]')

sc=SparkContext(conf=conf)

accessKeyId=os.environ['aws_access_key_id']
secretAccessKey=os.environ['aws_secret_access_key']
hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', accessKeyId)
hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')

spark=SparkSession(sc)

# reads contents of S3 bucket into a dataframe in spark
df = spark.read.json("s3a://aicpinterest/*").cache()

# df.show()
# df.printSchema()
# df.select("title").show()
# category_count = df.groupby('category').count().show()

df1 = df.na.replace('Image src error.', None)
df2 = df1.na.replace('N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e', None)
df3 = df2.na.replace('User Info Error', None)
df4 = df3.na.replace('No description available Story format', None)
df5 = df4.na.replace('No Title Data Available', None)
df6 = df5.withColumnRenamed('index', 'id_index')
df7 = df6.withColumn('id_index', df6['id_index'].cast('int'))\
    .withColumn('downloaded', df6['downloaded'].cast('int'))
df8 = df7.select('id_index', 'title', 'category', 'unique_id', 'description', 'follower_count', 'tag_list', 'is_image_or_video', 'image_src', 'downloaded', 'save_location')

df8.show()

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.2.0 s3_spark.py pyspark-shell'
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 pyspark-shell'

df8.write\
    .format("org.apache.spark.sql.cassandra") \
    .mode('append') \
    .option('confirm.truncate', 'true') \
    .option("spark.cassandra.connection.host", "127.0.0.1") \
    .option("spark.cassandra.connection.port", "9042") \
    .option('keyspace', 'pinterest') \
    .option('table','pinterest_batch_new') \
    .save()