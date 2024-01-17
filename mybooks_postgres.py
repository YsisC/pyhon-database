from postgres_config import dbConfig
import psycopg2 as pyo
import requests
import concurrent.futures
import json
from decouple import config  
import requests

def obtener_datos(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Lanza una excepción para errores HTTP
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
    resultados = list(executor.map(obtener_datos, [url_api1, url_api2]))
    # print(resultados)

# Almacenar los resultados en un archivo JSON
with open('resultados.json', 'w') as archivo:
    json.dump(resultados, archivo)


# Conectar a la base de datos
conexion = pyo.connect(**dbConfig)

cursor = conexion.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS resultados (
        id SERIAL PRIMARY KEY,
        data JSONB
    )
""")

# Insertar los resultados en la tabla
for resultado in resultados:
    print(f" resultado {resultado}")
    cursor.execute("INSERT INTO resultados (data) VALUES (%s)", (json.dumps(resultado),))
    


conexion.commit()
conexion.close()

############################################
# for lista in resultados:
#     # Imprimir cada elemento de la lista
#     for i, elemento in enumerate(lista):
#         print(f"Elemento {i}: {elemento}")
#     print("\n") 
