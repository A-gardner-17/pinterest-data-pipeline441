# Pinterest Data Pipeline
Pinterest crunches billions of data points every day to decide how to provide more value to their users. In this project, I will create a similar system using the AWS Cloud.

# Table of Contents
 1. [Project Description](#project-description)
 2. [Installation Instructions](#installation-instructions)
 3. [Project - Milestone 1](#project-milestone-1)
 4. [Project - Milestone 2](#project-milestone-2)
 5. [Project - Milestone 3](#project-milestone-3)
 6. [Project - Milestone 4](#project-milestone-4)
 7. [Project - Milestone 5](#project-milestone-5)
 8. [Project - Milestone 6](#project-milestone-6)
 9. [Project - Milestone 7](#project-milestone-7)
 10. [Project - Milestone 8](#project-milestone-8)
 11. [License Information](#license-information)

# Project Description

**Tools Used:**  
**Amazon EC2** - cloud computing service that provides resizable and scalable virtual servers (instances) in the AWS cloud.   
**Apache Kafka** - distributed event streaming platform designed for handling real-time data feeds.   
**API Gateway** - API linked to Apache Kafka  
**DataBricks** - for reading batch data from S3 and streaming data from Kinesis; cleaning and saving data  
**AWS Kinesis** - fully managed service for real-time data streaming and processing at scale, enabling applications to ingest, process, and analyze streaming data  

![Pinterest Pipeline](/Images/CloudPinterestPipeline.png)

# Installation Instructions
The project uses the standard Python installation.

# Project Milestone 1
As part of this project I set up a GitHub repo and created an AWS cloud account. 

# Project Milestone 2
I was provided with a python file containing that contained login details for an RDS database. The database contained three tables containing data resembling data received by the Pinterest API when a POST request was made. 

I moved the login details for the database into a separate db_creds.yaml file and added this file to my .gitignore file so that the details were not uploaded to the github repo. I modify the python script to access the yaml file details and output the following data:
1. pinterest_data contains data about posts being updated to Pinterest
2. geolocation_data contains data about the geolocation of each Pinterest post found in pinterest_data
3. user_data contains data about the user that has uploaded each post found in pinterest_data

I saved copies of data from each table to examine the data stored.

I then signed into the AWS account using the provided credentials, update the password and a made a note of the UserId. In all aspects of the project I will be working in the us-east-1 region so will need to set this region when create new services.

# Project Milestone 3
Using the Parameter Store in the AWS account I found the Key Pair value and stored it in the file Key pair name.pem, locally. This file was also added to the .gitignore file.

After starting the EC2 instance I connected to this instance from VSCode using the SSH client. Once the connection was made I created 3 topics as follows:
1. UserId.pin for the Pinterest posts data
2. UserID.geo for the post geolocation data
3. UserId.user for the post user data

# Project Milestone 4
[Milestone 4 Code](user_posting_emulation_m4.py)  
The API was already created for this project so the first step was to find the API and then create a build a PROXY integration. The resource was created followed by an HTTP ANY method using the PublicDNS of the EC2 machine as the Endpoint URL.
This Endpoint URL was in the following format: http://<ec2 instance PublicDNS>:8082/{proxy}.

The API was then deployed into the Dev stage and the generated Invoke URL was noted to be used in the next step.

Send data to API Gateway.
A copy of the user_posting_emulation.py that was provided to access the data resembling data received by the Pinterest API. This file is called user_posting_emulation_m4.py.
The file was modified as follows:

1. The API Invoke URL and Kafka topics were defined:
# API Invoke URL and Kafka topics
API_URL = "https://<invoke url>/Dev/topics"
HEADERS = {"Content-Type": "application/vnd.kafka.json.v2+json"}

TOPICS = {
    "pinterest_data": "<username>.pin",
    "geolocation_data": "<username>.geo",
    "user_data": "<username>.user"
}

2. The infinite loop was changed to limit the number of records to 500 and a call to the function to send the data to the Kafka REST Proxy was added for each topic area. This was in the following format - post_to_kafka(TOPICS["pinterest_data"], pin_result).

3. The post_to_kafka function was created to send the data to the EC2 and the data was stored in the S3 bucket. 
The first time this program was an error was returned in the following format:

Error posting to Kafka topic <username>.geo: Object of type datetime is not JSON serializable.
This indicated that the data being fetched from the database contained datetime objects, which are not JSON-serializable by default. After researching the error it was clear that the datetime objects needed to be converted to ISO 8601 strings. This was achieved with the follwoing function:

def json_serializer(obj):  
    if isinstance(obj, datetime):  
        return obj.isoformat()  # Convert to ISO 8601 format  
    raise TypeError(f"Type {type(obj)} not serializable")  

Once the serializer had been added the program was run again and the data was sent to the EC2 and could be send in the S3 bucket. The data was stored using the following structure: topics/<username>.<topic name>/partition=0/

**Issues**
The main issue was being able to connect to the Endpoint URL as the first time I set this up I used https instead of http. Having finally realised that this should be http I managed to test the connection using:
curl https://<inoke URL>/Dev/topics/<username>.pin

# Project Milestone 5
Batch Processing: Databricks. 
The first task in this milestone was to setup the Databricks account with the provided details.

The data was then read in from the S3 bucket - when reading in the JSONs from S3, I needed make sure to include the complete path to the JSON objects, as seen in the S3 bucket (e.g topics/<your_UserId>.pin/partition=0/). The path was included in the following format:

s3a://user-<username>-bucket/topics/<username>{item}/partition=0/  

All the data files in the partition were then read into the dataframe:

df = spark.read.format("json").load(partition_path + "*.json")  

The data was then saved directly as a Delta table in Databricks (example below).

![Delta table](/Images/Databricks1.png)

The completed code for this task can be found in **milestone5.py** [Milestone 5](milestone5.py)

A screenshot of the code in Databricks can be seen below:

![S3 Bucket to Databricks](/Images/SaveDataBricks.png)

# Project Milestone 6

The evidence for cleaning of the data is found [here](/Milestone6/Milestone_6_Cleaning.ipynb)  
The evidence for querying the data is found [here](/Milestone6/Milestone_6_Query_Data.ipynb)  

# Project Milestone 7

For this milestone I created an Airflow DAG that triggered a Databricks Notebook to be run on a specific schedule. This DAG was uploaded to the dags folder in the mwaa-dags-bucket.


The file 57e94de2a910_dag.py was uploaded and scheduled to run daily using the following format schedule_interval='0 6 * * * '.

The file is accessible [here](57e94de2a910_dag.py).

The dag was tested using the Airflow UI, evidence of which can be seen in the screenshot below. The failed runs were due to an incorrect notebook path.

![Working Airflow](/Images/Airflow_Working.png)

# Project Milestone 8

This was the most challenge part of the project and I encountered a number of issues during the various stages.  
The Kinesis Data Stream was called Kinesis-Prod-Stream.

I then updated the API setup as part of Milestone 4. The final API setup can be seen below:  

![Final API](/Images/Final_API_Kinesis.png)  

The errors encountered included the following:
1. API had incorrect region (sa-east-1) - incorrect selection on setup
2. PUT for record and records.

The script [here](user_posting_emulation_streaming.py) build upon the initial script used in posting to Kafka.  

On testing of the original script that errors with the API were identified and there was also a problem with the endpoint URL.
This should have been in the following format: https://<invoke url>/Dev/streams/{stream}/record but in my initial setup the end point included /PutRecord.
I also needed to change the requests statement to requests.put (this was originally setup incorrectly to get).

Once the changes above were made I used the following statement - print("Raw API Response:", response.text) - to check that the data was being posted.The successful evidence of this can be seen below:  

![ShardID](/Images/ShardID_success.png)

I was then able to see the data being posted on Kinesis:

![Data Stream](/Images/Data_Records.png)

![Put Record](/Images/Put_Record.png)

Once the data was successfully being posted I read the data into DataBricks using the provided authetication credentials.

The notebook for this processing can be found here [Data Bricks](/Milestone8/Milestone_8_Read.ipynb)

I was able to read in the data, decode and save to individual delta tables.

Evidence for the final part of the process can be found here [Data Bricks2](/Milestone8/Milestone_8_Read_Transform_Write.ipynb)  

This notbook combines reading the streaming data into DataBricks, cleaning the data and saving to individual delta tables.  

Evidence for each table can be seen below:  
![geo table](/Images/Geo_Streaming_data.png)

![pin table](/Images/Pin_Streaming_data.png)

![user table](/Images/User_Streaming_data.png)
