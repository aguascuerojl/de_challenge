import pymysql
import os
from google.cloud import storage
from tabulate import tabulate


def retrieve_data():
    try:

        # database connection string 
        connection = pymysql.connect(
            unix_socket='/cloudsql/bigdata-migration-446522:us-central1:migration-db',
            user='root',
            password=os.environ["DB_PASSWORD"],  
            database='hiring',
            cursorclass=pymysql.cursors.DictCursor
        )
#select stament as a string var to help in the reading of the code
        query = """
            SELECT 
                d.id AS department_id,
                d.department AS department_name,
                COUNT(e.id) AS employees_hired
            FROM employees e
            INNER JOIN departments d ON e.department_id = d.id
            WHERE  YEAR(e.datetime) = 2021
            GROUP BY  d.id, d.department
            HAVING COUNT(e.id) > (
                    SELECT  AVG(department_hired_count)
                    FROM (
                        SELECT COUNT(e.id) AS department_hired_count
                        FROM   employees e
                        WHERE  YEAR(e.datetime) = 2021
                        GROUP BY e.department_id
                    ) subquery
                )
            ORDER BY  employees_hired DESC;
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                return result

    except ValueError as e:
        raise Exception(f"Validation error: {e}")
    except pymysql.MySQLError as e:
        raise Exception(f"Database error: {e}")
    except Exception as e:
        raise Exception(f"Unexpected error fetching data: {e}")

def tabulate_data():
    # Fetch the data using the query function
    data = retrieve_data()
    
    # Extract headers and rows from the query result
    if data:
        headers = data[0].keys() 
        rows = [list(row.values()) for row in data]  
        # Generate the table
        table = tabulate(rows, headers, tablefmt="grid")  # "grid" format for better visualization
        return table
    else:
        return "No data available."

def hiring_by_department(request):
    if request.method == 'GET':
        # Call the  function to return  data table
        tabla = tabulate_data()
        return (f"<pre>{tabla}</pre>", 200, {'Content-Type': 'text/html'})
    else:
        return retrieve_data()   

