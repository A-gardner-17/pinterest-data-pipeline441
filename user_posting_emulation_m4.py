import requests
from datetime import datetime
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import yaml


random.seed(100)

# API Invoke URL and Kafka topics
API_URL = "https://5ca8e1ic9e.execute-api.us-east-1.amazonaws.com/Dev/topics"
HEADERS = {"Content-Type": "application/vnd.kafka.json.v2+json"}

TOPICS = {
    "pinterest_data": "57e94de2a910.pin",
    "geolocation_data": "57e94de2a910.geo",
    "user_data": "57e94de2a910.user"
}

class AWSDBConnector:

    def __init__(self,filename):

        self.fileName = filename
        self.credentials = self.read_db_creds()
        self.engine = self.create_db_connector()

    def read_db_creds(self):
        with open(self.fileName, "r") as file:
            credentials = yaml.safe_load(file)
        return credentials

    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.credentials['RDS_USER']}:{self.credentials['RDS_PASSWORD']}@{self.credentials['RDS_HOST']}:{self.credentials['RDS_PORT']}/{self.credentials['RDS_DATABASE']}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector('db_creds.yaml')

def postKafka(topic, data):
    try:
        # Custom JSON serializer to handle datetime objects
        def json_serializer(obj):
            if isinstance(obj, datetime):
                return obj.isoformat() # Convert to ISO 8601 format
            raise TypeError(f"Type {type(obj)} not serializable")
        
        payload = {
            "records": [
                {"value": data}
            ]
        }

        # Serialize payload with custom serializer
        serialized_payload = json.dumps(payload, default=json_serializer)

        response = requests.post(f"{API_URL}/{topic}", headers=HEADERS, data=serialized_payload)
        if response.status_code == 200:
            print(f"Successfully posted to topic {topic}: {data}")
        else:
            print(f"Failed to post to topic {topic}: {response.status_code}, {response.text}")
    except Exception as e:
        print(f"Error posting to Kafka topic {topic}: {e}")


def run_infinite_post_data_loop(limit):
    count = 0
    while count < limit:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                postKafka(TOPICS["pinterest_data"], pin_result)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                postKafka(TOPICS["geolocation_data"], geo_result)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
                postKafka(TOPICS["user_data"], user_result)
        
        count = count + 1 # increment count to limit the data


if __name__ == "__main__":
    limit = 500
    run_infinite_post_data_loop(limit)
    print('Posting Complete')
