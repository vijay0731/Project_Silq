import glob
import os
import pandas as pd
import sqlite3
import logging

# Configure logging
logging.basicConfig(filename='data_pipeline.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Function to load CSV data into Pandas DataFrame
def load_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Loaded data from {file_path}")
        return df

    except FileNotFoundError as e:
        logging.error(f"Error loading {file_path}: {e}")
        return None


# Function to handle data cleaning
def clean_data(df):

    # Handle missing values
    df.ffill(inplace=True)  # Filling missing values with the previous value (forward fill)

    # Remove duplicates
    df.drop_duplicates(inplace=True)

    # Remove rows with more than 30% NaN values
    threshold = int(0.3*len(df.columns))
    df.dropna(thresh=(len(df.columns) - threshold), inplace=True)

    logging.info("Data cleaning completed successfully :)")
    return df


# Function to create SQLite database and load data into tables
def create_and_load_database(users_df, orders_df, products_df):
    try:
        # Establish the connection to database
        conn = sqlite3.connect('data_pipeline.db')
        cursor = conn.cursor()

        # Create and load Users table
        users_df.to_sql('Users', conn, if_exists='replace', index=False)
        logging.info("Users table created and data loaded.")

        # Create and load Orders table
        orders_df.to_sql('Orders', conn, if_exists='replace', index=False)
        logging.info("Orders table created and data loaded.")

        # Create and load Products table
        products_df.to_sql('Products', conn, if_exists='replace', index=False)
        logging.info("Products table created and data loaded.")

        conn.commit()
        logging.info("Database created and tables loaded successfully :)")

    except Exception as e:
        logging.error(f"Error creating and loading database: {e}")


# Get the path/dir where data exists
current_file_path = os.path.abspath(__file__)
root_directory = current_file_path

while not os.path.exists(os.path.join(root_directory, "src")):
    root_directory = os.path.dirname(root_directory)

# Load csv files
data_dir = os.path.join(root_directory, "data/")
users_df = load_csv(data_dir + "Users.csv")
products_df = load_csv(data_dir + "Products.csv")
orders_df = load_csv(data_dir + "Orders.csv")


if users_df is not None and products_df is not None and orders_df is not None:
    users_df = clean_data(users_df)
    products_df = clean_data(products_df)
    orders_df = clean_data(orders_df)

    create_and_load_database(users_df, orders_df, products_df)

else:
    logging.error("Failed to load data from csv files :(")

# # Fetch Users data
# conn = sqlite3.connect('data_pipeline.db')
# cursor = conn.cursor()
#
# cursor.execute("select * from Users")
# columns = [description[0] for description in cursor.description]
#
# users = cursor.fetchall()
# print("Users table: ")
# print(','.join(columns))
# for row in users:
#     print(','.join(str(col) for col in row))
#
# # Fetch Products data
# cursor.execute("select * from Products")
# columns = [description[0] for description in cursor.description]
#
# products = cursor.fetchall()
# print("\n products table: ")
# print(','.join(columns))
# for row in products:
#     print(','.join(str(col) for col in row))
#
# # Fetch Orders data
# cursor.execute("select * from Orders")
# columns = [description[0] for description in cursor.description]
#
# orders = cursor.fetchall()
# print("\n Orders table: ")
# print(','.join(columns))
# for row in orders:
#     print(','.join(str(col) for col in row))
#
# conn.close()




