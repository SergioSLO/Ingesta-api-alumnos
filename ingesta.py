import boto3
import psycopg2  # Usando psycopg2 para PostgreSQL
import csv

# Configuración de la base de datos PostgreSQL
db_host = "3.230.28.178"
db_user = "postgres"
db_password = "utec"
db_name = "rockie"
db_port = 8002

# Configuración de S3 y el archivo CSV
ficheroUpload_activities = "activities.csv"
ficheroUpload_student = "student.csv"
nombreBucket = "bucket-ingesta-parcial"

# Conexión a la base de datos PostgreSQL
conn = psycopg2.connect(
    host=db_host,
    port=db_port,
    user=db_user,
    password=db_password,
    database=db_name
)
cursor = conn.cursor()

# Función para exportar una tabla a un archivo CSV
def export_table_to_csv(table_name, file_name):
    query = f"SELECT * FROM {table_name}"
    cursor.execute(query)

    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]

    with open(file_name, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(column_names)
        writer.writerows(rows)

# Exportar las tablas 'activities' y 'student'
export_table_to_csv('activities', ficheroUpload_activities)
export_table_to_csv('student', ficheroUpload_student)

# Cerrar la conexión a la base de datos
cursor.close()
conn.close()

# Subir los archivos CSV a S3
s3 = boto3.client('s3')
s3.upload_file(ficheroUpload_activities, nombreBucket, ficheroUpload_activities)
s3.upload_file(ficheroUpload_student, nombreBucket, ficheroUpload_student)

print("Ingesta completada y archivos subidos a S3")
