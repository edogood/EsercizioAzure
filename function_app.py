import logging
import os
import azure.functions as func
import pyodbc
import requests

app = func.FunctionApp()

def get_connection():
    try:
        """conn_string = (""
            f"Driver={{ODBC Driver 17 for SQL Server}};"
            f"Server={os.getenv('SQL_SERVER')};"
            f"Database={os.getenv('SQL_DATABASE')};"
            f"UID={os.getenv('SQL_USER')};"
            f"PWD={os.getenv('SQL_PASSWORD')};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )"""""
        conn_string= "Driver={ODBC Driver 18 for SQL Server};Server=yourserver;Database=yourdatabase;Uid=yourid;Pwd=yourpwd;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        return pyodbc.connect(conn_string)
    except pyodbc.Error as e:
        logging.error(f"Errore di connessione al database: {e}")
        raise

def get_weather():
    try:
        response = requests.get(
            "https://api.open-meteo.com/v1/forecast?latitude=45.4642&longitude=9.1900&current_weather=true"
        )
        response.raise_for_status()
        data = response.json()

        temperature = data.get("current_weather", {}).get("temperature")
        windspeed = data.get("current_weather", {}).get("windspeed")
        timestamp = data.get("current_weather", {}).get("time")

        logging.info(f"Weather Data: Temperature={temperature}, Windspeed={windspeed}, Time={timestamp}")

        return temperature, windspeed, timestamp
    except requests.RequestException as e:
        logging.error(f"Errore nel recupero dei dati da Open Meteo: {e}")
        return None, None, None

@app.route(route="test", auth_level=func.AuthLevel.FUNCTION)
def test(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    try:
        # Recupero dati meteo
        temperature, windspeed, timestamp = get_weather()
        if temperature is None or windspeed is None or timestamp is None:
            return func.HttpResponse("Errore nel recupero dei dati meteo", status_code=500)

        # Connessione al database e inserimento dati
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO prova (temperature, windspeed, timestamp) VALUES (?, ?, ?)",
                    (temperature, windspeed, timestamp)
                )
                connection.commit()

        return func.HttpResponse(f"Dati meteo inseriti: {temperature}°C, {windspeed} km/h, {timestamp}", status_code=200)

    except pyodbc.Error as e:
        logging.error(f"Errore SQL: {e}")
        return func.HttpResponse(f"Database error: {str(e)}", status_code=500)

    except Exception as e:
        logging.error(f"Errore generico: {e}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
