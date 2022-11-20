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

def prepare_sensor_data(sensor_data_tuple):
    return {
        "id": sensor_data_tuple[0],
        "value": sensor_data_tuple[1],
        "type": sensor_data_tuple[2],
        "datetime": str(sensor_data_tuple[3]),
        "address": sensor_data_tuple[4]
    }

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    db_connection = get_db_connection()
    db_cursor = db_connection.cursor()

    if req.method == func.HttpMethod.GET.value:
        type = req.params.get('type')
        sql_query_string = "SELECT id, value, type, datetime, address FROM sensors_data"
        sql_query_string += " WHERE type='{}';".format(type) if type else ';'
        db_cursor.execute(sql_query_string)
        sensors_data = db_cursor.fetchall()
        sensors_data_dicts = []
        for sensor_data in sensors_data:
            sensors_data_dicts.append(prepare_sensor_data(sensor_data))
        sensors_data_string = str(sensors_data_dicts)
        db_cursor.close()
        db_connection.close()
        return func.HttpResponse(
            body=sensors_data_string,
            status_code=200
        )

    if req.method == func.HttpMethod.POST.value:
        try:
            req_body = req.get_json()
        except ValueError:
            return func.HttpResponse("Body is not a valid JSON", status_code=400)
        type = req_body.get('type')
        value = req_body.get("value")
        datetime = req_body.get("datetime")
        address = req_body.get("address")

        if value is not None and datetime and address and type:
            if type not in ('TEMPERATURE', 'HUMIDITY', 'LUMINOSITY'):
                return func.HttpResponse("Luminosity must be between 0 and 100", status_code=400)
            if (type == 'HUMIDITY' or type == 'LUMINOSITY') and (value < 0 or value > 100):
                return func.HttpResponse("Value must be between 0 and 100", status_code=400)
                
            db_cursor.execute(
                f"INSERT INTO public.sensors_data(value, datetime, address, type)"
                f"VALUES ({value}, '{datetime}', '{address}', '{type}')"
                f"RETURNING id, value, type, datetime, address;"
            )
            new_record = db_cursor.fetchone()
            new_record_string = str(prepare_sensor_data(new_record))
            db_connection.commit()
            db_cursor.close()
            db_connection.close()
            return func.HttpResponse(new_record_string, status_code=200)
                
        else:
            error_message = ''
            error_message += "Value is required. " if value is None else ''
            error_message += "Datetime is required. " if not datetime else ''
            error_message += "Address is required. " if not address else ''
            error_message += "Type is required. " if not type else ''
            db_cursor.close()
            db_connection.close()
            return func.HttpResponse(error_message, status_code=400)
