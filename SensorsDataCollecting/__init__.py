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
        "datetime": str(sensor_data_tuple[3]),
        "id": sensor_data_tuple[0],
        "value": sensor_data_tuple[1],
        "type": sensor_data_tuple[2],
        "address": sensor_data_tuple[4]
    }


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    db_connection = get_db_connection()
    db_cursor = db_connection.cursor()

    if req.method == func.HttpMethod.GET.value:
        type = req.params.get('type')
        sql_query_string = "SELECT id, value, type, datetime, address FROM sensors_data"
        sql_query_string += " WHERE type='{}'".format(type) if type else ''
        sql_query_string += " ORDER BY datetime DESC;"
        db_cursor.execute(sql_query_string)
        sensors_data = db_cursor.fetchall()
        sensors_data_dicts = []
        for sensor_data in sensors_data:
            sensors_data_dicts.append(prepare_sensor_data(sensor_data))
        sensors_data_string = ''.join(str(sensor_data_dict) + '\n' for sensor_data_dict in sensors_data_dicts)
        db_cursor.close()
        db_connection.close()
        return func.HttpResponse(
            body=sensors_data_string,
            status_code=200
        )
