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

# AWS Kinesis Configuration
AWS_REGION = "sa-east-1"
STREAM_NAME = "Kinesis-Prod-Stream"

# API Gateway Invoke URL
API_URL = "https://5ca8e1ic9e.execute-api.us-east-1.amazonaws.com/Dev/streams/PutRecord"
HEADERS = {"Content-Type": "application/json"}

TABLES = {
    "pinterest_data": "pinterest",
    "geolocation_data": "geolocation",
    "user_data": "user"
}

class AWSDBConnector:
    def __init__(self, filename):
        self.fileName = filename
        self.credentials = self.read_db_creds()
        self.engine = self.create_db_connector()

    def read_db_creds(self):
        with open(self.fileName, "r") as file:
            credentials = yaml.safe_load(file)
        return credentials

    def create_db_connector(self):
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.credentials['RDS_USER']}:{self.credentials['RDS_PASSWORD']}@"
            f"{self.credentials['RDS_HOST']}:{self.credentials['RDS_PORT']}/{self.credentials['RDS_DATABASE']}?charset=utf8mb4"
        )
        return engine

new_connector = AWSDBConnector('db_creds.yaml')

# Function to describe stream (for debugging IAM permissions)
def describe_kinesis_stream():
    kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)
    try:
        response = kinesis_client.describe_stream(StreamName=STREAM_NAME)
        print("Stream Description:", json.dumps(response, indent=4))
    except Exception as e:
        print("Error describing stream:", e)

def postToKinesis(partition_key, data):
    try:
        def json_serializer(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")
        
        payload = {
            "StreamName": STREAM_NAME,
            "Data": json.dumps(data, default=json_serializer),
            "PartitionKey": partition_key
        }

        response = requests.post(API_URL, headers=HEADERS, data=json.dumps(payload))

        # Debugging: Print raw response
        print("Raw API Response:", response.text)

        if response.status_code == 200:
            response_json = response.json()
            print("Parsed API Response:", json.dumps(response_json, indent=4))

            shard_id = response_json.get("ShardId", "Not Found")
            sequence_number = response_json.get("SequenceNumber", "Not Found")

            print(json.dumps({
                "status": "Success",
                "PartitionKey": partition_key,
                "ShardId": shard_id,
                "SequenceNumber": sequence_number,
                "Data": data
            }, indent=4))
            
            return shard_id, sequence_number
        else:
            print(f"Failed to post to Kinesis: {response.status_code}, {response.text}")

    except Exception as e:
        print(f"Error posting to Kinesis: {e}")


def run_infinite_post_data_loop(limit):
    count = 0
    while count < limit:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()
        
        with engine.connect() as connection:
            for table, partition_key in TABLES.items():
                query = text(f"SELECT * FROM {table} LIMIT {random_row}, 1")
                selected_row = connection.execute(query)
                
                for row in selected_row:
                    result = dict(row._mapping)
                    postToKinesis(partition_key, result)
        
        count += 1
    
    print("Posting Complete")

if __name__ == "__main__":
    describe_kinesis_stream()  # Check IAM permissions
    limit = 500
    run_infinite_post_data_loop(limit)
