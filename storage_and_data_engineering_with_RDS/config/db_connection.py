import logging 

import pyodbc
import os 
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    '''
    Establishes a connection to the SQL Server database using pyodbc and 
    returns the connection object. The connection parameters are retrieved 
    from environment variables, which are loaded using the dotenv library.
    
    Returns:
        pyodbc.Connection: A connection object that can be used to interact 
        with the SQL Server database.
    '''
    try:
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={os.getenv('DB_END_POINT')};"
            f"DATABASE={os.getenv('DATABASE')};"
            f"UID={os.getenv('DB_USER_NAME')};"
            f"PWD={os.getenv('DB_PASSWORD')};"
            "TrustServerCertificate=yes;"
        )  
    
    except pyodbc.Error as e:
        logging.error("Error connecting to database: %s", e)
        raise e
    except Exception as e:
        logging.error("An unexpected error occurred: %s", e)
        raise e
    finally:
        logging.info("Database connection attempt completed.")
          
    return conn
