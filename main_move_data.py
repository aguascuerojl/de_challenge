import functions_framework
import tempfile
import pymysql
import os
import csv
from datetime import datetime
from google.cloud import storage
from google.api_core.exceptions import NotFound

def save_invalid_data_to_gcs(datos_invalidos, bucket_name, file_name):
    try:
        # We create the content of the wrong lines that where not inserted
        invalid_data_text = "\n".join([",".join(row) for row in datos_invalidos])

        # Initialize the cloud storage cliente
        gcs_client = storage.Client()
        bucket = gcs_client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        # Upload the file to the bucket
        blob.upload_from_string(invalid_data_text, content_type="text/plain")
        print(f"Archivo {file_name} subido correctamente al bucket {bucket_name}.")
    except Exception as e:
        raise Exception(f"Error saving invalid data to GCS: {e}")

def carga_archivos(tabla):
    try:
        #We use a env variable to reference the name of the bucket
        bucket_name = os.environ["GCS_PATH"]
        encoding = 'utf-8'

        gcs_client = storage.Client()
        gcs = gcs_client.bucket(bucket_name)
        gcs_file = gcs.blob(tabla)
        #We use download_as_string which is a method that belongs to GCS library
        file_data = gcs_file.download_as_string()
        text = file_data.decode(encoding)

        reader = csv.reader(text.splitlines())
        datos_validos = []
        datos_invalidos = []
    #a lis is used as data structure to handle each row to be inserted
        for row in reader:
            if all(field.strip() for field in row):
                datos_validos.append(list(row))
            else:
                datos_invalidos.append(list(row))

        # If there is invalid data, save it to a .txt file in the bucket
        if datos_invalidos:
            save_invalid_data_to_gcs(datos_invalidos, "landing_raw_data", "datos_invalidos.txt")

        return datos_validos

    except NotFound as e:
        raise Exception(f"File {tabla} not found in bucket {bucket_name}: {e}")
    except Exception as e:
        raise Exception(f"Error loading file {tabla}: {e}")

def insertar_datos_en_tabla(departments_list, jobs_list, employee_list):
    try:
        #creation of the database connection we use a env variable to reference the DB pass
        conexion = pymysql.connect(
            unix_socket='/cloudsql/bigdata-migration-446522:us-central1:migration-db',
            user='root',
            password=os.environ["DB_PASS"],
            database='hiring',
            cursorclass=pymysql.cursors.DictCursor
        )

        with conexion:
            with conexion.cursor() as cursor:
                try:
                    #we create an statement for every insert in the respective table
                    for datos in departments_list:
                        id_departamento, nombre_departamento = datos
                        sentencia_insert_depts = "INSERT INTO departments (id, department) VALUES (%s, %s)"
                        valores = (id_departamento, nombre_departamento)
                        cursor.execute(sentencia_insert_depts, valores)

                    for datos_job in jobs_list:
                        id_job, nombre_job = datos_job
                        sentencia_insert_job = "INSERT INTO jobs (id, job) VALUES (%s, %s)"
                        valores_job = (id_job, nombre_job)
                        cursor.execute(sentencia_insert_job, valores_job)

                    for datos_empl in employee_list:
                        id_empl, nombre, fecha, id_dept, id_job = datos_empl
                        fecha = datetime.strptime(fecha, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
                        sentencia_insert_empl = "INSERT INTO employees (id, name, datetime, department_id, job_id) VALUES (%s, %s, %s, %s, %s)"
                        valores_empl = (id_empl, nombre, fecha, id_dept, id_job)
                        cursor.execute(sentencia_insert_empl, valores_empl)

                    conexion.commit()

                except Exception as e:
                    conexion.rollback()
                    raise Exception(f"Error inserting data into tables: {e}")

    except pymysql.MySQLError as e:
        raise Exception(f"Database connection or query error: {e}")
    except Exception as e:
        raise Exception(f"Unexpected error during database operation: {e}")

@functions_framework.http
def move_data(request):
    try:
        #we use the standard method to load the csv file
        departments = carga_archivos('departments.csv')
        jobs = carga_archivos('jobs.csv')
        employees = carga_archivos('hired_employees.csv')

        insertar_datos_en_tabla(departments, jobs, employees)

        return {"Status": "Registros migrados desde departments.csv: " + str(len(departments))+ " | Registros migrados desde jobs.csv: " + str(len(jobs))+ " | Registros migrados desde hired_employees.csv:  " + str(len(employees)) + " | Registros migrados desde hired_employees.csv: " + str(len(employees))+ " | Registros invalidos https://storage.googleapis.com/landing_raw_data/datos_invalidos.txt"}, 200


    except Exception as e:
        return {"error": str(e)}, 500
