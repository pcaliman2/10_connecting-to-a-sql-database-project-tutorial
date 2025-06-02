import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables

# 1) Connect to the database with SQLAlchemy
def connect():
    global engine
    try:
        connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
        print("Starting the connection...")
        engine = create_engine(connection_string, isolation_level="AUTOCOMMIT") 
        engine.connect()
        print("Connected successfully!")
        return engine
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None
#Es una buena practica agregarle un Exit a la No conexion en cso de una falla
engine = connect()
if engine is None:
    exit()

# 2) Create the tables

# 3) Insert data

# 4) Use Pandas to read and display a table

