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

def prepare_luminosity_sensor_data(luminosity_sensor_data_tuple):
    return {
        "id": luminosity_sensor_data_tuple[0],
        "luminosity": luminosity_sensor_data_tuple[1],
        "datetime": str(luminosity_sensor_data_tuple[2]),
        "address": luminosity_sensor_data_tuple[3]
    }

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    db_connection = get_db_connection()
    db_cursor = db_connection.cursor()

    if req.method == func.HttpMethod.GET.value:
        db_cursor.execute("SELECT id, value, datetime, address FROM sensors_data WHERE type='LUMINOSITY';")
        luminosity_sensors_data = db_cursor.fetchall()
        luminosity_sensors_data_dicts = []
        for luminosity_sensor_data in luminosity_sensors_data:
            luminosity_sensors_data_dicts.append(prepare_luminosity_sensor_data(luminosity_sensor_data))
        luminosity_sensors_data_string = str(luminosity_sensors_data_dicts)
        db_cursor.close()
        db_connection.close()
        return func.HttpResponse(
            body=luminosity_sensors_data_string,
            status_code=200
        )

    if req.method == func.HttpMethod.POST.value:
        try:
            req_body = req.get_json()
        except ValueError:
            return func.HttpResponse("Body is not a valid JSON", status_code=400)
        luminosity = req_body.get("luminosity")
        datetime = req_body.get("datetime")
        address = req_body.get("address")

        if luminosity is not None and datetime and address:
            if luminosity >= 0 and luminosity <= 100:
                db_cursor.execute(
                    f"INSERT INTO public.sensors_data(value, datetime, address, type)"
                    f"VALUES ({luminosity}, '{datetime}', '{address}', 'LUMINOSITY')"
                    f"RETURNING id, value, datetime, address;"
                )
                new_record = db_cursor.fetchone()
                new_record_string = str(prepare_luminosity_sensor_data(new_record))
                db_connection.commit()
                db_cursor.close()
                db_connection.close()
                return func.HttpResponse(new_record_string, status_code=200)
            else:
                return func.HttpResponse("Luminosity must be between 0 and 100", status_code=400)

        else:
            error_message = ''
            error_message += "luminosity is required. " if luminosity is None else ''
            error_message += "Datetime is required. " if not datetime else ''
            error_message += "Address is required. " if not address else ''
            db_cursor.close()
            db_connection.close()
            return func.HttpResponse(error_message, status_code=400)
