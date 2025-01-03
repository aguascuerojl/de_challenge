import pymysql
import os

#Main function which perform a simple truncation over the three tables of the project
def truncar_tablas():
    try:
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
                    sentencia_truncar_job = "TRUNCATE TABLE jobs;"
                    cursor.execute(sentencia_truncar_job)

                    sentencia_truncar_departments = "TRUNCATE TABLE departments;"
                    cursor.execute(sentencia_truncar_departments)

                    sentencia_truncar_employees = "TRUNCATE TABLE employees;"
                    cursor.execute(sentencia_truncar_employees)

                    conexion.commit()

                except Exception as e:
                    conexion.rollback()
                    raise Exception(f"Error en el borrado de los datos: {e}")
#here we handle any possible issue with the execution of the delete statements
    except pymysql.MySQLError as e:
        raise Exception(f"Database connection or query error: {e}")
    except Exception as e:
        raise Exception(f"Unexpected error during database operation: {e}")


#Entry point for the execution of the logic
def delete_data(request):
    try:
        truncar_tablas()
        
        return {"Status": "Datos borrados exitosamente." }, 200

    except Exception as e:
        return {"error": str(e)}, 500
