from postgres_config import dbConfig
import psycopg2 as pyo
import requests
import concurrent.futures
import json
from decouple import config  
import requests

con = pyo.connect(**dbConfig)

cursor = con.cursor()


class Bookdb:
    def __init__(self, url_api1, url_api2):
        self.url_api1 = url_api1
        self.url_api2 = url_api2
        self.con = pyo.connect(**dbConfig)
        self.cursor = con.cursor()
        
        print("You have connected to the  database")
        print(con)

    def __del__(self):
        self.con.close()
    
    def get_data(self, url):
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

    def fetch_data_parallel(self):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = list(executor.map(self.get_data, [self.url_api1, self.url_api2]))
        return results
    
    def save_results_to_file(self, results, filename='results.json'):
        with open(filename, 'w') as archivo:
            json.dump(results, archivo)

    def create_database_table(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS results (
                id SERIAL PRIMARY KEY,
                data JSONB
            )
        """)

        self.con.commit()
    def view(self):
        self.cursor.execute("SELECT * FROM results")
        rows = self.cursor.fetchall()
        print(rows)
        return rows
        
    def insert(self,results):
        for item in results:
            self.cursor.execute("INSERT INTO results (data) VALUES (%s)", (json.dumps(item),))
       
        self.con.commit()
    
db = Bookdb(
        url_api1=config('URL_API_1'),
        url_api2=config('URL_API_2')
    )

results= db.fetch_data_parallel()
db.create_database_table()
db.insert(results)
db.view()
db.save_results_to_file(results)