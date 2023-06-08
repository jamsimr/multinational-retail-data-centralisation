#%%
import pandas as pd
from data_extraction import DataExtractor
from database_utils import DatabaseConnector, pull_engine
import phonenumbers
import re

db_connector = DatabaseConnector()
data_extractor = DataExtractor(db_connector)
orders_df = data_extractor.read_rds_table('orders_table')

class DataCleaning:
    def __init__(self, df, db_connector):
        """Initializes the DataCleaning instance with a DataFrame to clean.

        Args:
            df (pandas.DataFrame): The DataFrame to be cleaned.
        """
        self.df = df.copy()  # Create a copy of the DataFrame to clean
        self.db_connector = db_connector

    def clean_user_data(self):
        """
        Cleans the user data in the DataFrame.

        Drops rows and columns with NULL values, filters out date of birth and join date entries with incorrect format,
        and normalizes phone numbers based on country codes.

        Returns:
            pandas.DataFrame: The cleaned DataFrame.
        """
        df = self.df.copy()

        df = df.replace('NULL', pd.NA, regex=False) # Replaces "NULL" entries with NULL 
        df = df.dropna() # Drop rows with NULL values
        df = df.dropna(axis=1) # Drop columns with NULL values
        
        # Define the regex pattern for random entries
        random_pattern = r'^[A-Z0-9]{10}$' # Matches 10-character entries with letters and numbers
        # Identify and drop rows with random entries
        mask = df.apply(lambda row: row.str.match(random_pattern).all(), axis=1)
        df = df[~mask]

        # Parse date of birth column
        df['date_of_birth'] = df['date_of_birth'].apply(lambda x: parser.parse(x, fuzzy=True) if isinstance(x, str) else x)
        df = df[pd.to_datetime(df['date_of_birth'], errors='coerce').notnull()]  # Drop rows with incorrect date formats

        # Parse join date column
        df['join_date'] = df['join_date'].apply(lambda x: parser.parse(x, fuzzy=True) if isinstance(x, str) else x)
        df = df[pd.to_datetime(df['join_date'], errors='coerce').notnull()]  # Drop rows with incorrect date formats

        # Extracts the first line of address
        for index, row in df.iterrows():
            if '\n' in row['address']:
                # Splits the entry at the first line
                df.at[index, 'address'] = row['address'].split('\n')[0]


        # Define the function to normalize phone numbers
        def normalize_phone_number(phone_number, country_code):
            try:
                parsed_number = phonenumbers.parse(phone_number, country_code)
                if phonenumbers.is_valid_number(parsed_number):
                    return phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164)
            except phonenumbers.NumberParseException:
                pass
            return pd.NA

        # Normalize phone numbers
        df['phone_number'] = df.apply(lambda row: normalize_phone_number(row['phone_number'], row['country_code']), axis=1)

        # Drop rows with invalid phone numbers
        df = df.dropna(subset=['phone_number'])

        self.df = df # Updates the dataframe instance of the class
        
        return df
    
    def clean_card_data(self):
        """
        Cleans the card details data in the DataFrame.

        Drops rows and columns with NULL values, removes random entries, filters date of payment entries with incorrect format 
        and removes special characters from card number entries.

        Returns:
            pandas.DataFrame: The cleaned DataFrame.
        """
        df = self.df.copy()

        df = df.replace('NULL', pd.NA, regex=False)  # Replaces "NULL" entries with NULL
        df = df.dropna()  # Drop rows with NULL values
        df = df.dropna(axis=1)  # Drop columns with NULL values

        # Define the regex pattern for random entries
        random_pattern = r'^[A-Z0-9]{10}$'  # Matches 10-character entries with letters and numbers
        # Identify and drop rows with random entries
        mask = df.apply(lambda row: row.str.match(random_pattern).all(), axis=1)
        df = df[~mask]

        # Parse date of payment confirmed column
        df['date_payment_confirmed'] = df['date_payment_confirmed'].apply(
            lambda x: parser.parse(x, fuzzy=True) if isinstance(x, str) else x)
        df = df[pd.to_datetime(df['date_payment_confirmed'], errors='coerce').notnull()]  # Drop rows with incorrect date formats

        # Function to check and remove special characters from card number
        def remove_special_characters(card_number):
            if '?' in str(card_number):
                card_number = str(card_number).replace('?', '')  # Remove question marks
            return card_number

        # Apply remove_special_characters function iteratively
        df['card_number'] = df['card_number'].apply(remove_special_characters)

        self.df = df  # Updates the dataframe instance of the class

        return df
    
    def clean_store_data(self):

        """
        Cleans the card details data in the DataFrame.

        Drops rows and columns with NULL values, removes random entries, removes duplicate latitude column, 
        removes characters from staff number entries and excess characters from continent.

        Returns:
            pandas.DataFrame: The cleaned DataFrame.
        """
        df = self.df.copy()

        # Removes duplicate latitude columns and reorders them
        df = df.drop(columns=['lat']) # Removes old lat column
        column_order = ['index', 'address', 'longitude', 'latitude', 'locality', 'store_code',
                    'staff_numbers', 'opening_date', 'store_type', 'country_code', 'continent']
        df = df[column_order]

        df = df.replace('NULL', pd.NA, regex=False)  # Replaces "NULL" entries with NULL
        df = df.dropna()  # Drop rows with NULL values
        df = df.dropna(axis=1)  # Drop columns with NULL values

        # Define the regex pattern for random entries
        random_pattern = r'^[A-Z0-9]{10}$'  # Matches 10-character entries with letters and numbers
        # Identify and drop rows with random entries
        mask = df.apply(lambda row: row.str.match(random_pattern).all(), axis=1)
        df = df[~mask]

        # Function to check and remove special characters from continent 
        def remove_special_characters(continent):
            if 'ee' in str(continent):
                continent = str(continent).replace('ee', '')  # Remove ee
            return continent
        # Apply remove_special_characters function iteratively
        df['continent'] = df['continent'].apply(remove_special_characters)

        # Remove characters from staff number column
        df['staff_numbers'] = df['staff_numbers'].str.replace('[^0-9]', '', regex=True)

        self.df = df  # Updates the dataframe instance of the class

        return df

    def clean_products_data(self):
        """
        Cleans the card details data in the DataFrame.

        Drops rows and columns with NULL values, removes random entries, reformats date added column.

        Returns:
            pandas.DataFrame: The cleaned DataFrame.
        """

        df = self.df.copy()

        df = df.replace('NULL', pd.NA, regex=False)  # Replaces "NULL" entries with NULL
        df = df.dropna()  # Drop rows with NULL values
        df = df.dropna(axis=1)  # Drop columns with NULL values

        # Define the regex pattern for random entries
        random_pattern = r'^[A-Z0-9]{10}$'  # Matches 10-character entries with letters and numbers
        # Identify and drop rows with random entries
        mask = df.apply(lambda row: row.str.match(random_pattern).rolling(window=3).sum().max() != 3, axis=1)
        df = df[mask]

        # Parse date added column
        df['date_added'] = df['date_added'].apply(lambda x: parser.parse(x, fuzzy=True) if isinstance(x, str) else x)
        df = df[pd.to_datetime(df['date_added'], errors='coerce').notnull()]  # Drop rows with incorrect date formats

        # Name the first column index
        df.rename(columns={df.columns[0]: 'index'}, inplace=True)

        self.df = df  # Updates the dataframe instance of the class

        return df

    def convert_product_weights(self):
        '''
        Cleans and converts the product weights to a decimal value representing their weight in kg.

        Returns:
        pandas.DataFrame: DataFrame with cleaned and converted weight column.
        '''

        def process_weight_string(weight_string):
            matches = re.findall(r'(\d+\.?\d*)(\D+)', weight_string)
            if len(matches) > 1 and 'x' in weight_string:
                result = str(float(matches[0][0]) * float(matches[1][0])) + matches[1][1]
                return result
            elif len(matches) > 0:
                result = str(matches[0][0]) + matches[0][1]
                return result
            else:
                return None

        # Make a copy of the DataFrame to avoid modifying the original data
        df = self.df.copy()

        # Convert weight column to string
        df['weight'] = df['weight'].astype(str)

        # Process values like "3 x 132g" or "5 x 145g"
        df['weight'] = df['weight'].apply(process_weight_string)

        # Convert to kg based on the weight unit
        def convert_to_kg(weight):
            if weight is None:
                return None  # Return None if weight is None
            elif weight.endswith('kg'):
                return weight  # Leave the value as it is if it already has 'kg' unit
            elif weight.endswith('g'):
                value = float(weight[:-1]) / 1000  # Remove 'g' and convert to kg
                return f"{value}kg"
            elif weight.endswith('ml'):
                value = float(weight[:-2]) / 1000  # Remove 'ml' and convert to kg
                return f"{value}kg"
            else:
                return weight  # For any other cases, leave the value as it is

        df['weight'] = df['weight'].apply(convert_to_kg)

        self.df = df  # Updates the dataframe instance of the class

        return df

    def clean_orders_data(self):
        """
        Cleans the order data in the DataFrame.

        Drops first name, last name and 1 columns and reindexes the data

        Returns:
            pandas.DataFrame: The cleaned DataFrame.
        """
        df = self.df.copy()

        # Reindexes the data
        df.sort_values(by='index')

        # Removes first name, last name and 1 columns
        df = df.drop(columns=['first_name', 'last_name', '1'])

        self.df = df  # Updates the dataframe instance of the class

        return df

order_cleaner = DataCleaning(orders_df, db_connector)
order_cleaner_df = order_cleaner.clean_orders_data()
order_cleaner_df.to_csv('cleaned_order_data.csv', index=False)

'''weight_cleaner = DataCleaning(products_df, db_connector)
weight_cleaner_df = weight_cleaner.convert_product_weights()
product_cleaner = DataCleaning(weight_cleaner_df, db_connector)
product_cleaner.clean_products_data()
clean_product_data_df = product_cleaner.df
table_name = 'dim_products'
db_connector.upload_to_table(clean_product_data_df, table_name)'''

# %%
