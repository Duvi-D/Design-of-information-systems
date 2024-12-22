import pandas as pd
import mysql.connector
from mysql.connector import Error

# Conexión a la base de datos MySQL
def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host='127.0.0.1',  # Cambia según tu configuración
            user='root',       # Usuario de tu MySQL
            password='123456', # Contraseña de tu MySQL
            database='hello_mysql' # Nombre de la base de datos
        )
        if connection.is_connected():
            print("Conexión exitosa a la base de datos")
            return connection
    except Error as e:
        print(f"Error conectando a la base de datos: {e}")
        return None

# Cargar datos desde un archivo CSV a una tabla MySQL
def load_csv_to_mysql(file_path, table_name, columns, connection):
    try:
        # Leer CSV usando pandas
        data = pd.read_csv(file_path)
        cursor = connection.cursor()
        
        # Insertar datos fila por fila
        for _, row in data.iterrows():
            placeholders = ', '.join(['%s'] * len(columns))
            sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            cursor.execute(sql, tuple(row[col] for col in columns))
        
        connection.commit()
        print(f"Datos insertados en la tabla '{table_name}' desde {file_path}")
    except Error as e:
        print(f"Error cargando datos en {table_name}: {e}")

# Ruta de los archivos CSV
files = {
    "Users": {
        "file": "users.csv",
        "columns": ["name", "email", "created_at", "updated_at"]
    },
    "Surveys": {
        "file": "surveys.csv",
        "columns": ["title", "user_id", "created_at", "status"]
    },
    "Questions": {
        "file": "questions.csv",
        "columns": ["survey_id", "question_text", "created_at"]
    },
    "Answers": {
        "file": "answers.csv",
        "columns": ["question_id", "user_id", "answer_text", "created_at"]
    },
    "AnswerOptions": {
        "file": "answeroptions.csv",
        "columns": ["question_id", "option_text"]
    },
    "UserAttempts": {
        "file": "userattempts.csv",
        "columns": ["user_id", "survey_id", "status", "created_at"]
    }
}

# Ejecutar el proceso de carga
connection = connect_to_database()
if connection:
    for table_name, file_info in files.items():
        load_csv_to_mysql(file_info["file"], table_name, file_info["columns"], connection)
    connection.close()


def verify_data(connection):
    if connection is None:
        print("No se puede verificar, la conexión es nula.")
        return

    tables = ["Users", "Surveys", "Questions", "Answers", "AnswerOptions", "UserAttempts"]
    for table in tables:
        print(f"\nDatos en la tabla {table}:")
        cursor = connection.cursor()
        cursor.execute(f"SELECT * FROM {table} LIMIT 10")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
