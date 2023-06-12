#%%
import pandas as pd
from database_utils import db_connector, pull_engine
import tabula
import requests
import boto3
import csv

class DataExtractor:
    def __init__(self, db_connector):
        '''
        Initializes the DataExtractor instance with a DatabaseConnector instance
        
        Args:
            db_connector (DatabaseConnector): Instance of the DatabaseConnector class
            header: Header for API
        '''
        self.db_connector = db_connector
        self.header = {
            "x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
        }

    def read_rds_table(self, table_name):
        '''
        Reads a table from the database using the DatabaseConnector instance
        
        Args:
            table_name (str): Name of the table to read
            
        Returns:
            pandas.DataFrame: DataFrame containing the table data
        '''
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, con=self.db_connector.pull_engine)
        return df
    
    def retrieve_pdf_data(self, pdf_link):
        '''
        Retrieves data from a PDF document and returns a pandas DataFrame.

        Args:
            pdf_link (str): Link to the PDF document.

        Returns:
            pandas.DataFrame: Extracted data as a DataFrame.
        '''
        # Download PDF to avoid ssh issues
        response = requests.get(pdf_link)
        with open('card_details.pdf', 'wb') as f:
            f.write(response.content)

        # Extract data from PDF using tabula
        dfs = tabula.read_pdf('card_details.pdf', pages='all', multiple_tables=True)

        # Concatenate all extracted DataFrames into a single DataFrame
        extracted_data = pd.concat(dfs, ignore_index=True)

        return extracted_data
    
    def list_number_of_stores(self, endpoint):
        """
        Retrieves the number of stores from the API.

        Args:
            endpoint (str): The API endpoint for retrieving the number of stores.

        Returns:
            int: The number of stores.
        """
        response = requests.get(endpoint, headers=self.header)
        if response.status_code == 200: # Ensures the API is connected
            data = response.json()
            number_of_stores = data['number_stores']
            return number_of_stores
    
    def retrieve_stores_data(self, endpoint):
        """
        Retrieves all the stores from the API and saves them in a pandas DataFrame.

        Args:
            endpoint (str): The API endpoint for retrieving a store.

        Returns:
            pandas.DataFrame: A DataFrame containing the store data.
        """
        stores_data = []
        
        for store_number in range(1, 452): # Checks each number in list of total stores value
            store_endpoint = endpoint + f'/{store_number}' # Creates a new endpoint for every store number value
            response = requests.get(store_endpoint, headers=self.header)
            
            if response.status_code == 200: 
                data = response.json()
                stores_data.append(data)
        
        df = pd.DataFrame(stores_data)
        return df
    
    def extract_from_s3(self, s3_link):
        '''
        Retrieves data from an S3 bucket and returns a pandas DataFrame.
    
        Args:
            s3_link (str): S3 link in the format 's3://bucket_name/key'.
    
        Returns:
            pandas.DataFrame: Extracted data as a DataFrame.
        '''
        # Extract the bucket and key from the S3 link
        s3_parts = s3_link.split('//')[1].split('/')
        s3_bucket = s3_parts[0]
        s3_key = '/'.join(s3_parts[1:])
    
        # Read the access keys from the CSV file
        with open('access_keys.csv', 'r') as file:
            reader = csv.reader(file)
            header = next(reader)  # Get the header row
            access_keys = next(reader)  # Assuming the access keys are in the second row
    
        # Extract the access key ID and secret access key based on column index
        aws_access_key_id = access_keys[0]
        aws_secret_access_key = access_keys[1]
    
        # Create an S3 client
        s3_client = boto3.client('s3', 
                      aws_access_key_id=aws_access_key_id, 
                      aws_secret_access_key=aws_secret_access_key
                      )
    
        # Download the file from S3
        local_file_path = s3_key  # Specify the local file path to save the downloaded file
        s3_client.download_file(s3_bucket, s3_key, local_file_path)
    
        # Read the CSV file into a DataFrame
        #extracted_data = pd.read_csv(local_file_path)

        # Read the JSON file into a DataFrame
        extracted_data = pd.read_json(local_file_path)

        return extracted_data

data_extractor = DataExtractor(db_connector)
events_df = data_extractor.extract_from_s3('s3://data-handling-public/date_details.json')
events_df.to_csv('events.csv', index=False)
# %%
