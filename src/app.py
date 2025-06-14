import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Esta Funcion crea la Conexión
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

#Defino un Subalgoritmo para ejecutar los sql
def ejecutar_script_sql(engine, ruta_archivo_sql):
    with open(ruta_archivo_sql, "r", encoding="utf-8") as archivo:
        script = archivo.read()
#En Alchemy hay que ejecutar linea por linea por eso el Split
    with engine.connect() as conexion:
        for sentencia in script.strip().split(";"):
            if sentencia.strip():  # omitir vacíos
                conexion.execute(text(sentencia))
        conexion.commit()

    print(f"✅ Script ejecutado correctamente: {ruta_archivo_sql}")


#-------------------------------------------------------------
#        Ejecucion de las Funciones
#-------------------------------------------------------------


# 1) Connect to the database with SQLAlchemy
load_dotenv()
engine = connect()
# 2) Create the tables
ejecutar_script_sql(engine, "sql/create.sql")
# 3) Insert data
ejecutar_script_sql(engine, "sql/insert.sql")
# 4) Use Pandas to read and display a table
SalidaSQL = pd.read_sql("SELECT * FROM publishers;", engine)
print(SalidaSQL)
