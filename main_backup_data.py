import pymysql
import os
from fastavro import writer
from google.cloud import storage
from io import BytesIO

def get_data_from_table(table_name):
    try:
        # validation of the table to name, to avoid sql injection
        allowed_tables = {"employees", "departments", "jobs"}  
        if table_name not in allowed_tables:
            raise ValueError(f"Invalid table name: {table_name}")

        # database connection string
        connection = pymysql.connect(
            unix_socket='/cloudsql/bigdata-migration-446522:us-central1:migration-db',
            user='root',
            password=os.environ["DB_PASSWORD"],  
            database='hiring',
            cursorclass=pymysql.cursors.DictCursor
        )

        with connection:
            with connection.cursor() as cursor:
                # We build the table name dynamically
                query = f"SELECT * FROM `{table_name}`;"
                cursor.execute(query)
                result = cursor.fetchall()
                return result

    except ValueError as e:
        raise Exception(f"Validation error: {e}")
    except pymysql.MySQLError as e:
        raise Exception(f"Database error: {e}")
    except Exception as e:
        raise Exception(f"Unexpected error fetching data: {e}")


def save_avro_to_gcs(data, bucket_name, file_name, table_type):
    try:
        # Define Avro schemas for different table types
        schemas = {
            "employees": {
                "type": "record",
                "name": "Employee",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"},
                    {
                        "name": "datetime",
                        "type": {
                            "type": "long",
                            "logicalType": "timestamp-micros"
                        }
                    },
                    {"name": "department_id", "type": "int"},
                    {"name": "job_id", "type": "int"}
                ]
            },
            "departments": {
                "type": "record",
                "name": "Department",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "department", "type": "string"}
                ]
            },
            "jobs": {
                "type": "record",
                "name": "Job",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "job", "type": "string"}
                ]
            }
        }

        avro_schema = schemas[table_type]

        # Write data to an Avro file in memory
        avro_buffer = BytesIO()
        writer(avro_buffer, avro_schema, data)

        # Upload the Avro file to Google Cloud Storage
        avro_buffer.seek(0)
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_file(avro_buffer, content_type='application/avro')

        print(f"File {file_name} successfully uploaded to bucket {bucket_name}.")
    except ValueError as e:
        raise Exception(f"Validation error: {e}")
    except Exception as e:
        raise Exception(f"Error saving Avro file to GCS: {e}")




def backup_data(request):
    try:

        try:
            # Fetch data from the database
            data_employees = get_data_from_table("employees")
            data_departments = get_data_from_table("departments")
            data_jobs = get_data_from_table("jobs")
        except Exception as e:
            raise Exception(f"Error fetching data from the database: {e}")

        try:
            # Define the bucket and file name
            bucket_name = "landing_raw_data"

            # Save the data to Google Cloud Storage in Avro format
            save_avro_to_gcs(data_employees, bucket_name, "employees.avro", "employees")
            save_avro_to_gcs(data_departments, bucket_name, "departments.avro", "departments")
            save_avro_to_gcs(data_jobs, bucket_name, "jobs.avro", "jobs")

        except Exception as e:
            raise Exception(f"Error saving data to Google Cloud Storage: {e}")

        # Return the status  of the execution and path where the backup files are stored
        return {"Status": "Backup exitoso almacenado en https://storage.googleapis.com/landing_raw_data/(employees.avro, departments.avro, jobs.avro)"}, 200

    except Exception as e:
    
        return {"error": str(e)}, 500


