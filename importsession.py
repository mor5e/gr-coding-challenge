import json
import sys
import logging
import pyodbc
from datetime import datetime
import configparser

# Set up logging
logging.basicConfig(filename='car_rental_import.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

import configparser
import logging

def get_connection_string():
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    driver = config['database']['driver']
    server = config['database']['server']
    database = config['database']['database']
    username = config['database']['username']
    password = config['database']['password']
    
    # Log the database details
    logging.info(f"Database Name: {database}")
    logging.info(f"Username: {username}")
    logging.info(f"Password: {'*' * len(password)}")  # Mask the actual password
    
    conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};'
    return conn_str


def insert_into_database(sessions, file_name):
    conn_str = get_connection_string()
    
    try:
        conn = pyodbc.connect(conn_str)
        logging.info("Database connection successful")
        
        cursor = conn.cursor()

        # ... (rest of the function remains the same)

    except pyodbc.Error as e:
        logging.error(f"Database connection failed: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            logging.info("Database connection closed")

