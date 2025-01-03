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
            password= os.environ["DB_PASSWORD"],
            database='hiring',
            cursorclass=pymysql.cursors.DictCursor
        )
#query created as a string var to improve the readability if the code
        query = """
        SELECT 
            d.department AS department,
            j.job AS job,
            SUM(CASE WHEN QUARTER(e.datetime) = 1 THEN 1 ELSE 0 END) AS Q1,
            SUM(CASE WHEN QUARTER(e.datetime) = 2 THEN 1 ELSE 0 END) AS Q2,
            SUM(CASE WHEN QUARTER(e.datetime) = 3 THEN 1 ELSE 0 END) AS Q3,
            SUM(CASE WHEN QUARTER(e.datetime) = 4 THEN 1 ELSE 0 END) AS Q4
        FROM  employees e
        INNER JOIN departments d ON e.department_id = d.id
        INNER JOIN jobs j ON e.job_id = j.id
        WHERE  YEAR(e.datetime) = 2021
        GROUP BY d.department, j.job
        ORDER BY d.department ASC, j.job ASC;
        """
        with connection:
            with connection.cursor() as cursor:
           
                cursor.execute(query)
                result = cursor.fetchall()
                return result
#handlin of any possible issue with the execution of the query
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
        table = tabulate(rows, headers, tablefmt="grid")  
        return table
    else:
        return "No data available."

def quarterly_hired(request):
    if request.method == 'GET':
        # we call function to return data in table format
        tabla = tabulate_data()
        return (f"<pre>{tabla}</pre>", 200, {'Content-Type': 'text/html'}) #we convert the output format as html
    else:
        return retrieve_data()   

