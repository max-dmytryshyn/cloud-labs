import logging

import azure.functions as func
from dotenv import load_dotenv
import os
import psycopg2

load_dotenv()


def get_db_connection():
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    dbname = os.getenv("DB_NAME")
    user = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    sslmode = os.getenv("DB_SSLMODE")
    return psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password,  sslmode=sslmode)

def prepare_humidity_sensor_data(humidity_sensor_data_tuple):
    return {
        "id": humidity_sensor_data_tuple[0],
        "humidity": humidity_sensor_data_tuple[1],
        "datetime": str(humidity_sensor_data_tuple[2]),
        "address": humidity_sensor_data_tuple[3]
    }

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    db_connection = get_db_connection()
    db_cursor = db_connection.cursor()

    if req.method == func.HttpMethod.GET.value:
        db_cursor.execute("SELECT id, value, datetime, address FROM sensors_data WHERE type='HUMIDITY';")
        humidity_sensors_data = db_cursor.fetchall()
        humidity_sensors_data_dicts = []
        for humidity_sensor_data in humidity_sensors_data:
            humidity_sensors_data_dicts.append(prepare_humidity_sensor_data(humidity_sensor_data))
        humidity_sensors_data_string = str(humidity_sensors_data_dicts)
        db_cursor.close()
        db_connection.close()
        return func.HttpResponse(
            body=humidity_sensors_data_string,
            status_code=200
        )

    if req.method == func.HttpMethod.POST.value:
        try:
            req_body = req.get_json()
        except ValueError:
            return func.HttpResponse("Body is not a valid JSON", status_code=400)
        humidity = req_body.get("humidity")
        datetime = req_body.get("datetime")
        address = req_body.get("address")

        if humidity is not None and datetime and address:
            if humidity >= 0 and humidity <= 100:
                db_cursor.execute(
                    f"INSERT INTO public.sensors_data(value, datetime, address, type)"
                    f"VALUES ({humidity}, '{datetime}', '{address}', 'HUMIDITY')"
                    f"RETURNING id, value, datetime, address;"
                )
                new_record = db_cursor.fetchone()
                new_record_string = str(prepare_humidity_sensor_data(new_record))
                db_connection.commit()
                db_cursor.close()
                db_connection.close()
                return func.HttpResponse(new_record_string, status_code=200)
            else:
                return func.HttpResponse("Humidity must be between 0 and 100", status_code=400)

        else:
            error_message = ''
            error_message += "Humidity is required. " if humidity is None else ''
            error_message += "Datetime is required. " if not datetime else ''
            error_message += "Address is required. " if not address else ''
            db_cursor.close()
            db_connection.close()
            return func.HttpResponse(error_message, status_code=400)
