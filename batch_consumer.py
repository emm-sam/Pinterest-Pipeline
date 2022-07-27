import boto3
import botocore
import json
import os
import shutil
import uuid
from json import loads
from kafka import KafkaConsumer

s3 = boto3.client('s3')
bucket = 'pintaic'
path = os.getcwd()
dir_path = path + '/bucket'

batch_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",    
    value_deserializer=lambda message: json.loads(message), 
    auto_offset_reset="earliest"
)

batch_consumer.subscribe(topics=["PinterestTopic"])
batch_consumer.poll()

def batch_to_directory(consumer : str , dir_path : str, number_facts : int):
    '''
    Consumes set number of facts/messages at once and saves in a directory with unique timestamp id 
    Args:
        consumer: kafka consumer name
        dir_path: directory path for consumer data
        number_facts: number of messages in this batch
    
    '''
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    i = 0
    for fact in batch_consumer:
        file_name = f'{i}.json'
        json_string = fact[6]
        with open(os.path.join(dir_path, file_name), 'w') as f:
            json.dump(json_string, f)
        i += 1
        if i == number_facts:
            break

def upload_dir_S3(dir_path : str):
    '''
    Uploads directory to S3 then deletes
    Args: dir_path: path to directory to be uploaded
    '''
    for (root, dirs, files) in os.walk(dir_path):
        for file in files:
            result = s3.upload_file(os.path.join(root,file),bucket,file)
            if result:
                shutil.rmtree(dir_path)

batch_to_directory(consumer=batch_consumer , dir_path=dir_path, number_facts=10)
upload_dir_S3(dir_path=dir_path)