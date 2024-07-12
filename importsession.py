import json
import sys
import logging
import pyodbc
from datetime import datetime
import configparser
from jsonschema import validate

'''
# use pandas to break up injestion into larger chunks: 

import pandas as pd
from sqlalchemy import create_engine

df = pd.DataFrame(your_json_data)
engine = create_engine('mssql+pyodbc://server/database?driver=SQL+Server')
df.to_sql('your_table', engine, if_exists='append', index=False, chunksize=1000)
'''

# Set up logging
logging.basicConfig(filename='car_rental_import.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# JSON schema for validation
schema = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "type": {"type": "string", "enum": ["START", "END"]},
            "id": {"type": "string"},
            "timestamp": {"type": "string", "pattern": "^[0-9]+$"},
            "comments": {"type": "string"}
        },
        "required": ["type", "id", "timestamp", "comments"]
    }
}

def get_connection_string():
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    driver = config['database']['driver']
    server = config['database']['server']
    database = config['database']['database']
    username = config['database']['username']
    password = config['database']['password']
    
    logging.info(f"Connecting to database: {database}")
    logging.info(f"Using username: {username}")
    logging.info(f"Password: {'*' * len(password)}")
    
    return f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};'

def parse_json_file(file_path):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        validate(instance=data, schema=schema)
        return data
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON format in file: {file_path}. Error: {str(e)}")
        return None
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        return None
    except Exception as e:
        logging.error(f"Error validating JSON: {str(e)}")
        return None

def process_sessions(data):
    sessions = {}
    for record in data:
        session_id = record['id']
        if session_id not in sessions:
            sessions[session_id] = {'start_time': None, 'end_time': None, 'comments': ''}
        
        if record['type'] == 'START':
            sessions[session_id]['start_time'] = datetime.fromtimestamp(float(record['timestamp']))
        elif record['type'] == 'END':
            sessions[session_id]['end_time'] = datetime.fromtimestamp(float(record['timestamp']))
            sessions[session_id]['comments'] = record['comments']

    return sessions

def calculate_session_details(sessions):
    for session_id, details in sessions.items():
        start_time = details['start_time']
        end_time = details['end_time']
        
        if start_time and end_time:
            duration = (end_time - start_time).total_seconds() / 3600  # convert to hours
            car_returned_late = duration > 24
        else:
            duration = None
            car_returned_late = None

        car_damaged = bool(details['comments'])

        sessions[session_id].update({
            'duration': duration,
            'car_returned_late': car_returned_late,
            'car_damaged': car_damaged
        })

    return sessions

def insert_into_database(sessions, file_name):
    conn_str = get_connection_string()
    
    try:
        conn = pyodbc.connect(conn_str)
        logging.info("Database connection successful")
        cursor = conn.cursor()

        for session_id, details in sessions.items():
            cursor.execute("""
                INSERT INTO Sessions (SessionID, StartTime, EndTime, SessionDuration, CarReturnedLate, CarWasDamaged, Comments)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                session_id,
                details['start_time'],
                details['end_time'],
                details['duration'],
                details['car_returned_late'],
                details['car_damaged'],
                details['comments']
            ))

        cursor.execute("""
            INSERT INTO FileImportLog (FileName, ImportedSuccessfully)
            VALUES (?, ?)
        """, (file_name, True))

        conn.commit()
        logging.info(f"Successfully imported data from {file_name}")
    except pyodbc.Error as e:
        logging.error(f"Database error: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            logging.info("Database connection closed")

def main(file_path):
    logging.info(f"Starting import process for file: {file_path}")
    
    data = parse_json_file(file_path)
    if not data:
        return

    sessions = process_sessions(data)
    sessions = calculate_session_details(sessions)
    insert_into_database(sessions, file_path)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python importsession.py <path_to_json_file>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    main(file_path)
