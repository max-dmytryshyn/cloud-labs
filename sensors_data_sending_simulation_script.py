import datetime
import json
import random
import time

import argparse
import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData


async def send_data(connection_string, data):
    producer = EventHubProducerClient.from_connection_string(connection_string)
    async with producer:
        event_data_batch = await producer.create_batch()
        event_data_batch.add(EventData(data))
        print("Request with data {} was sent".format(data))
        await producer.send_batch(event_data_batch)

SENSORS_TYPE = ("TEMPERATURE", "HUMIDITY", "LUMINOSITY")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", help="Type of sensor")
    parser.add_argument("--address", help="Address of sensor")
    parser.add_argument("--period", help='Requests sending period in ms')
    parser.add_argument("--queue_connection_string", help='Url for endpoint to send data to')
    args = parser.parse_args()

    type = args.type
    if type not in SENSORS_TYPE:
        raise ValueError("Wrong sensor type")
    address = args.address
    period_in_ms = int(args.period)
    connection_string = args.queue_connection_string
    print(f"connection_string: {connection_string}")
    print(f"address: {address}")
    print(f"period_in_ms: {period_in_ms}")
    print(f"type: {type}")
    while True:
        if type == 'TEMPERATURE':
            value = random.randint(-20, 41)
        else:
            value = random.randint(0, 101)

        data = json.dumps({
            "value": value,
            "datetime": str(datetime.datetime.now()),
            "address": address,
            "type": type
        })
        loop = asyncio.get_event_loop()
        loop.run_until_complete(send_data(connection_string=connection_string,data=data))
        time.sleep(period_in_ms / 1000)

