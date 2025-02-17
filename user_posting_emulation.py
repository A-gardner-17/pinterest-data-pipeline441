from time import sleep
from sqlalchemy import text
from multiprocessing import Process
import random

import requests
import boto3
import json
import sqlalchemy
import yaml


random.seed(100)


class AWSDBConnector:

    def __init__(self,filename):
        """
        Initialises an instance of the class, setting up database connection properties.

        Args:
            fileName (str): The name of the file containing database credentials.

        Attributes:
            fileName (str): Stores the provided file name for database credential lookup.
            credentials (dict): The database credentials read from the specified file.
            engine (sqlalchemy.engine.base.Engine): The database engine initialised using the credentials.
        """

        self.fileName = filename
        self.credentials = self.read_db_creds()
        self.engine = self.create_db_connector()

    def read_db_creds(self):
        """
        
        Method reads database credentials from a YAML file,
        and returns the credentials as a dictionary.

        Returns:
            credentials (dict): A dictionary containing the database credentials.

        """

        with open(self.fileName, "r") as file:
            credentials = yaml.safe_load(file)
        return credentials

    def create_db_connector(self):
        """
        Method Initialises and returns a SQLAlchemy database engine.

        Returns:
            sqlalchemy.engine.base.Engine: A SQLAlchemy engine instance for database operations.
        """ 
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.credentials['RDS_USER']}:{self.credentials['RDS_PASSWORD']}@{self.credentials['RDS_HOST']}:{self.credentials['RDS_PORT']}/{self.credentials['RDS_DATABASE']}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector('db_creds.yaml')


def run_infinite_post_data_loop():
    """
        Procedure to connect to RDS database and read pin, geo and user data.

    """ 
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            print(pin_result)
            print(geo_result)
            print(user_result)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


