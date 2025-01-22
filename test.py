import requests
import json

example_df = {"index": 1, "name": "Maya", "age": 25, "role": "engineer"}
#topics/57e94de2a910.pin
invoke_url = "http://5ca8e1ic9e.execute-api.us-east-1.amazonaws.com:8082/topics/57e94de2a910.pin"
# To send JSON messages you need to follow this structure
payload = json.dumps({
    "records": [
        {
        # Data should be send as pairs of column_name:value, with different columns separated by commas       
        "value": {"index": example_df["index"], "name": example_df["name"], "age": example_df["age"], "role": example_df["role"]}
        }
    ]
})

# This content type is the content type required by the confluent REST proxy
headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
response = requests.request("POST", invoke_url, headers=headers, data=payload)

print(response.status_code)