from postgres_config import dbConfig
import psycopg2 as pyo
import requests
import concurrent.futures
import json
from decouple import config  
import requests

def get_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status() 
        return response.json()
    except requests.exceptions.HTTPError as errh:
        print(f"Error HTTP: {errh}")
    except requests.exceptions.RequestException as err:
        print(f"Error al realizar la solicitud: {err}")
    except requests.exceptions.JSONDecodeError as errJson:
        print(f"Error al decodificar JSON: {errJson}")
    return None

url_api1 = config('URL_API_1')
url_api2 = config('URL_API_2')

# Utilizando ThreadPoolExecutor para realizar las solicitudes en paralelo
with concurrent.futures.ThreadPoolExecutor() as executor:
    results = list(executor.map(get_data, [url_api1, url_api2]))
   


with open('results.json', 'w') as archivo:
    json.dump(results, archivo)


# Conectar a la base de datos
conexion = pyo.connect(**dbConfig)

cursor = conexion.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS results (
        id SERIAL PRIMARY KEY,
        data JSONB
    )
""")

def view():
    cursor.execute("SELECT * FROM results")
    rows = cursor.fetchall()
    return rows

def insert_results(list):
    for result in results:
        # print(f" resultado {resultado}")
        cursor.execute("INSERT INTO results (data) VALUES (%s)", (json.dumps(result),))

insert_results(results)
print(view())

conexion.commit()
conexion.close()


