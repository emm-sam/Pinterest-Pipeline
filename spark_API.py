import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import col 
from pyspark.sql.functions import regexp_replace
import os

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1 pyspark-shell"

conf = SparkConf() \
    .setAppName('S3toSpark') \
    .setMaster('local[*]')

sc=SparkContext(conf=conf)

accessKeyId='AKIA4HTQPSIT4S3ITIAN'
secretAccessKey='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'
hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', accessKeyId)
hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')

spark=SparkSession(sc)

# path = 'https://s3.eu-west-2.amazonaws.com/pintaic/0.json'
df = spark.read.json("s3a://pintaic/*").cache()

df.show()
df.printSchema()
df.select("title").show()
category_count = df.groupby('category').count().show()

# df = df.withColumn('image_src2', regexp_replace(df('image_src'), 'Image src error.', 'NA')).show()

df1 = df.na.replace('Image src error.', None)
df2 = df1.na.replace('N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e', None)
df3 = df2.na.replace('User Info Error', None)
df4 = df3.na.replace('No description available Story format', None)
df5 = df4.na.replace('No Title Data Available', None)
df6 = df5.select('category', 'unique_id', 'title', 'description', 'follower_count', 'tag_list', 'index', 'is_image_or_video', 'image_src', 'downloaded')
df6.show()