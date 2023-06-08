# Multinational Retail Data Centralisation Project

## Description

## Features

### data_extraction.py 

This script contains a class called DataExtractor, a utility class to extract data from different data sources. 

The class is initiated with 2 arguments; an instance on the DatabaseConnector class, **dbconnector**, and a dictonary of the header for API access, **header**.

The DataExtractor class contains one function:

- *read_rds_table*: Reads a table from the PostgreSQL database using the DatabaseConnector instance and reuturns a pandas DataFrame containing the table data.

- *retrieve_pdf_data*: Retrieves data from a PDF link and returns a pandas DataFrame.

- *list_number_of_stores*: Retrieves the number of stores from the API.

- *retrieve_stores_data*: Retrieves all the stores from the API and saves them in a pandas DataFrame.

- *extract_from_s3*: Retrieves data from an S3 bucket and returns a pandas DataFrame.


### database_utils.py

This script contains a class called DatabaseConnector to connect and upload data to the database. 

The DatabaseConnector class contains four functions:

- *read_db_creds*: Reads the database credentials for YAML file and returns database credentials.

- *init_pull_engine*: Creates an SQLAlchemy database engine from YAML file and returns an SQLAlchemy engine.

- *init_upload_engine*: Creates the SQLAlchemy engine for uploading data.

- *list_db_tables*: Lists all the tables in the PostgreSQL database and returns the Table names.

- *upload_to_table*: Uploads the DataFrame to a table in the database.

### data_cleaning.py 

This script contains a class called DataCleaning with methods to clean data from each of the data sources. 

The class is initiated with 2 arguements; a Pandas Dataframe, **df**, and with an instance on the DataConnector class, **dbconnector**.

The DataCleaning class contains two functions:

- *clean_user_data*: Cleans the user data in the DataFrame. Drops rows and columns with NULL values, filters out date of birth and join date entries with incorrect format,
and normalizes phone numbers based on country codes.

- *clean_card_data*: Cleans the card data in the DataFrame. Drops rows and columns with NULL values, removes random entries, filters date of payment entries with incorrect format 
and removes special characters from card number entries.

- *clean_store_data*: Cleans the store data in the DataFrame. Drops rows and columns with NULL values, removes random entries, removes duplicate latitude column, removes characters from staff number entries and excess characters from continent.

- *clean_products_data*: Cleans the card details data in the DataFrame. Drops rows and columns with NULL values, removes random entries, reformats date added column.

- *convert_product_weights* : Cleans and converts the product weights to a decimal value representing their weight in kg.

- *clean_orders_data* : Cleans the order data in the DataFrame. Drops first name, last name and 1 columns and reindexes the data.

- *clean_events_data* : Cleans the events data in the DataFrame. Drops rows and columns with NULL values, removes random entries.







