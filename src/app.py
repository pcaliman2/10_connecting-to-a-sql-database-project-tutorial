import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Esta Funcion crea la Conexión y la dio 4Geeks

def connect():
    global engine
    print("entre")
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
    base_dir = os.path.dirname(os.path.abspath(__file__))
    ruta_relativa = os.path.normpath(os.path.join(base_dir, ".", ruta_archivo_sql))

    if not os.path.exists(ruta_relativa):
        raise FileNotFoundError(f"❌ No se encontró el archivo SQL: {ruta_relativa}")

    with open(ruta_relativa, "r", encoding="utf-8") as archivo:
        script = archivo.read()

    with engine.connect() as conexion:
        for sentencia in script.strip().split(";"):
            if sentencia.strip():
                conexion.execute(text(sentencia))
    
    print(f"✅ Script ejecutado correctamente: {ruta_relativa}")


#-------------------------------------------------------------
#        Ejecucion de las Funciones
#-------------------------------------------------------------


# 1) Connect to the database with SQLAlchemy
load_dotenv()
connect()

conn = engine.raw_connection()
#ejecutar_script_sql(engine, "../src/sql/lte1.sql")
# 2) Create the tables
ejecutar_script_sql(engine, "../src/sql/insert_Serving.sql")
# 3) Insert data
#ejecutar_script_sql(engine, "./src/sql/insert.sql")
# 4) Use Pandas to read and display a table

#try:
#    SalidaSQL = pd.read_sql("SELECT * FROM publishers;", conn)
#finally:
#    conn.close()
#print(SalidaSQL)
#ejecutar_script_sql(engine, "./src/sql/drop.sql") #Tengo el Dropo aqui para los Experimentos