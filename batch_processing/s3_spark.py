import findspark
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import col, regexp_replace 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 pyspark-shell'

class S3SparkCassandra:
    def __init__(self):
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

        self.spark=SparkSession(sc)

    def __reads_s3(self):
        '''
        Reads contents of S3 bucket (json files) into a spark dataframe
        '''
        self.spark_df = self.spark.read.json("s3a://aicpinterest/*").cache()
        print("Retrieving data from S3 bucket")

    def __spark_transforms_data(self):
        '''
        This method performs some cleaning transformations on the data held in spark
        '''
        # replaces missing data with null value 
        self.spark_df = self.spark_df.na.replace('Image src error.', None) \
            .na.replace('N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e', None) \
            .na.replace('User Info Error', None) \
            .na.replace('No description available Story format', None) \
            .na.replace('No Title Data Available', None) \
        # renames the column 'index'
        self.spark_df = self.spark_df.withColumnRenamed('index', 'id_index')
        # changes data type to int
        self.spark_df = self.spark_df.withColumn('id_index', self.spark_df['id_index'].cast('int'))\
            .withColumn('downloaded', self.spark_df['downloaded'].cast('int'))
        # changes column order
        self.spark_df = self.spark_df.select('id_index', 'title', 'category', 'unique_id', 'description', 'follower_count', 'tag_list', 'is_image_or_video', 'image_src', 'downloaded', 'save_location')
        # displays data
        self.spark_df.show()
        print("Transforming data in spark")

    def __write_to_cassandra(self):
        '''
        This method writes the spark dataframe to cassandra using the configuration below
        keyspace: pinterest
        table: pinterest_batch_new 
        '''
        self.spark_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode('append') \
            .option("spark.cassandra.connection.host", "127.0.0.1") \
            .option("spark.cassandra.connection.port", "9042") \
            .option('keyspace', 'pinterest') \
            .option('table','pinterest_batch_new') \
            .save()
        print("Writing spark data to cassandra")

    def runthrough(self):
        self.__reads_s3()
        self.__spark_transforms_data()
        self.__write_to_cassandra()

if __name__ == '__main__':
    batch_job = S3SparkCassandra()
    batch_job.runthrough() 
