# Pinterest Data Pipeline
Pinterest crunches billions of data points every day to decide how to provide more value to their users. In this project, I will create a similar system using the AWS Cloud.

# Table of Contents
 1. [Project Description](#project-description)
 2. [Installation Instructions](#installation-instructions)
 3. [Usage Instructions](#usage-instructions)
 4. [File Structure of the Project - Milestone 1](#file-structure-of-the-project-milestone-1)
 5. [File Structure of the Project - Milestone 2](#file-structure-of-the-project-milestone-2)
 6. [File Structure of the Project - Milestone 3](#file-structure-of-the-project-milestone-3)
 7. [License Information](#license-information)

# Project Description


# Installation Instructions
The project uses the standard Python installation.

# Usage Instructions
The project can be tested using the main.py

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