from kafka import KafkaConsumer
from json import loads

# create our consumer to retrieve the message from the topics
data_stream_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",    
    value_deserializer=lambda message: loads(message), # turns into a dictionary 
    auto_offset_reset="earliest" 
)

data_stream_consumer.subscribe(topics=["PinterestTopic"])

for message in data_stream_consumer:
    print(message)