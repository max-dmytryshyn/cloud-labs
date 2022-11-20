import json
import logging
from typing import List

import azure.functions as func
import os
import psycopg2
from dotenv import load_dotenv


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


def main(events: List[func.EventHubEvent]):
    db_connection = get_db_connection()
    db_cursor = db_connection.cursor()
    for event in events:
        try:
            req_body = json.loads(event.get_body().decode('utf-8').replace("'", '"'))
        except ValueError:
            logging.error("Body is not a valid JSON")
            continue
        type = req_body.get('type')
        value = req_body.get("value")
        datetime = req_body.get("datetime")
        address = req_body.get("address")

        if value is not None and datetime and address and type:
            if type not in ('TEMPERATURE', 'HUMIDITY', 'LUMINOSITY'):
                logging.error("Luminosity must be between 0 and 100")
                continue
            if (type == 'HUMIDITY' or type == 'LUMINOSITY') and (value < 0 or value > 100):
                logging.error("Value must be between 0 and 100")
                continue
                
            db_cursor.execute(
                f"INSERT INTO public.sensors_data(value, datetime, address, type)"
                f"VALUES ({value}, '{datetime}', '{address}', '{type}')"
                f"RETURNING id, value, type, datetime, address;"
            )
            new_record = db_cursor.fetchone()
            new_record_string = str(prepare_sensor_data(new_record))
            db_connection.commit()
            logging.info('Record created: %s', new_record_string)
        else:
            error_message = ''
            error_message += "Value is required. " if value is None else ''
            error_message += "Datetime is required. " if not datetime else ''
            error_message += "Address is required. " if not address else ''
            error_message += "Type is required. " if not type else ''
            logging.error(error_message)
    
    db_cursor.close()
    db_connection.close()        
