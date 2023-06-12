# %%

import yaml
from sqlalchemy import create_engine, inspect

import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
        
    def __init__(self):
        self.credentials = self.read_db_creds('db_creds.yaml')
        self.pull_engine = self.init_pull_engine()
        self.upload_engine = self.init_upload_engine()

    def read_db_creds(self, file):
        '''
        Reads the database credentials from the YAML file  
        
        Args:
            file (str): The path to the YAML file containing the credentials
            
        Returns: 
            dict: Database credentials
        '''
        with open(file, 'r') as f:
            credentials = yaml.safe_load(f)
        return credentials
    
    def init_pull_engine(self):
        '''
        Creates the SQLAlchemy engine for pulling data
        
        Returns:
            sqlalchemy.engine.Engine: Database engine object
        '''
        db_host = self.credentials['RDS_HOST']
        db_port = self.credentials['RDS_PORT']
        db_user = self.credentials['RDS_USER']
        db_password = self.credentials['RDS_PASSWORD']
        db_database = self.credentials['RDS_DATABASE']

        connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}'
        engine = create_engine(connection_string)
        return engine
    
    def init_upload_engine(self):
        '''
        Creates the SQLAlchemy engine for uploading data
        
        Returns:
            sqlalchemy.engine.Engine: Database engine object
        '''
        db_host = 'localhost'  # Update with the host of your new database
        db_port = '5432'  # Update with the port of your new database
        db_user = 'postgres'  # Update with the user of your new database
        db_password = 'Postgres*123!'  # Update with the password of your new database
        db_database = 'sales_data'  # Update with the name of your new database

        connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}'
        engine = create_engine(connection_string)
        return engine
    
    def list_db_tables(self, engine):
        '''
        Lists all the tables in the PostgreSQL database
        
        Args:
            engine (sqlalchemy.engine.Engine): Database engine object
        
        Returns:
            list: Table names
        '''
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        return table_names
    
    def upload_to_table(self, df, table_name):
        '''
        Uploads the DataFrame to a table in the database
        
        Args:
            df (pandas.DataFrame): The DataFrame to upload
            table_name (str): The name of the table
        
        Returns:
            None
        '''
        df.to_sql(table_name, self.upload_engine, if_exists='replace', index=False)

db_connector = DatabaseConnector()
pull_engine = db_connector.pull_engine
upload_engine = db_connector.upload_engine
table_names = db_connector.list_db_tables(pull_engine)
# %%
