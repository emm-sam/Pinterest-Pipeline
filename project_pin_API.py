from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from kafka import KafkaProducer
from json import dumps

app = FastAPI()

class Data(BaseModel):
    category: str
    index: int
    unique_id: str
    title: str
    description: str
    follower_count: str
    tag_list: str
    is_image_or_video: str
    image_src: str
    downloaded: int
    save_location: str

pinterest_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    client_id="Pinterest Producer",
    value_serializer=lambda message: dumps(message).encode("ascii")
)

@app.post("/pin/") # reading as json
def get_db_row(item: Data):
    data = dict(item)
    pinterest_producer.send(topic="PinterestTopic", value=data)
    return item 

if __name__ == '__main__':
    uvicorn.run("project_pin_API:app", host="localhost", port=8000)

