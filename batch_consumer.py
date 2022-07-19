from kafka import KafkaConsumer
from json import loads
import time
import boto3
import botocore
from botocore.exceptions import ClientError
import json
import schedule
import os
import tempfile

s3 = boto3.client('s3')
bucket = 'pintaic'
path = os.getcwd()

# create our consumer to retrieve the message from the topics
batch_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",    
    value_deserializer=lambda message: loads(message), # turns into a dictionary 
    auto_offset_reset="earliest"
)

batch_consumer.subscribe(topics=["PinterestTopic"])

batch_consumer.poll()

i = 0

for message in batch_consumer:
    timestamp = message.timestamp
    dir_path = path + 'data'
    os.makedirs(dir_path)
    with open(os.path.join(dir_path, f'{timestamp}.json'), 'w') as f:
        json.dump(message, f, ensure_ascii=True)
    i += 1
    if i == 10:
        break


# with open('batch .json', 'w') as write_file:
#     json.dump(batch, write_file, ensure_ascii=True)
# response = s3.upload_file('batch.json', bucket)
