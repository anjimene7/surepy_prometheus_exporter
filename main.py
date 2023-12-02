import asyncio
import logging
import os.path
import traceback
from os import environ
import csv
import base64
import requests
from datetime import datetime, timezone
import time
from typing import List, Dict
from surepy import Surepy, EntityType
from surepy.exceptions import SurePetcareConnectionError
from prometheus_client import start_http_server, Gauge
from prometheus_client.core import GaugeMetricFamily, REGISTRY
from sys import stdout

# Define logger
logger = logging.getLogger('surepy_prometheus_exporter')

logger.setLevel(logging.INFO) # set logger level
logFormatter = logging.Formatter("%(name)-12s %(asctime)s %(levelname)-8s %(filename)s:%(funcName)s %(message)s")
consoleHandler = logging.StreamHandler(stdout) #set streamhandler to stdout
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

household_status_metric = Gauge('surepy_household_status', 'Status of the SurePet household (online or offline)', labelnames=["serial", "name", "household_id"])
#pet_food_metric = Gauge(name='surepy_pet_food', documentation='Amount of food eaten by the pet', labelnames=["name", "household_id", "photo_url"])
feeder_food_metric = Gauge('surepy_bowls_food', 'Bowls food status', labelnames=["name", "household_id", "serial"])
feeder_battery_metric = Gauge('surepy_bowls_battery', 'Bowls battery status', labelnames=["name", "household_id", "serial"])


def set_value_with_timestamp(metric, labels, value, timestamp):
    labels["timestamp"] = timestamp
    metric.labels(**labels).set(value)

def get_household_and_pet_metrics(households: list[EntityType.HUB], pets: list[EntityType.PET], feeders: list[EntityType.FEEDER]):
    output_household = []
    for household in households:
        report = asyncio.run(surepy.get_report(household.household_id))
        for el in households:
            output_household.append({'labels': {'serial': el.serial, 'name': el.name, 'household_id': el.household_id}, 'value': el.online})
        output_pets = []
        output_feeder_history = []
        for el2 in report.get('data'):
            pet = [x for x in pets if x.id == el2.get('pet_id')]
            feeder = [x for x in feeders if x.id == el2.get('device_id')]
            if pet and feeder:
                pet = pet[0]
                feeder = feeder[0]
            else:
                raise ValueError(f"No pet found for id {el2.get('pet_id')} or device found for id {el2.get('device_id')}")
            feeding_datapoints = el2['feeding']['datapoints']
            feeding_datapoints_ts = [x['from'] for x in feeding_datapoints]
            feeding_datapoints_amount = [x['weights'][0]['change'] for x in feeding_datapoints]
            feeding_datapoints_weights = [x['weights'][0]['weight'] for x in feeding_datapoints]
            for ts, eat_value, weight_value in zip(feeding_datapoints_ts, feeding_datapoints_amount, feeding_datapoints_weights):
                output_pets.append({'labels': {'name': pet.name, 'household_id': pet.household_id, 'photo_url': pet.photo_url}, 'ts': ts, 'value': eat_value})
                output_feeder_history.append({'labels': {'name': feeder.name, 'household_id': feeder.household_id, 'serial': feeder.serial}, 'ts': ts, 'value': weight_value})
    return output_household, output_pets, output_feeder_history

def get_feeder_metrics(data: list[EntityType.FEEDER]) -> dict:
    output_feeder_food, output_feeder_battery = [], []
    for feeder in data:
        output_feeder_food.append({'labels': {'name': feeder.name, 'household_id': feeder.household_id, 'serial': feeder.serial}, 'value': feeder.total_weight})
        output_feeder_battery.append({'labels': {'name': feeder.name, 'household_id': feeder.household_id, 'serial': feeder.serial}, 'value': feeder.battery_level})
    return output_feeder_battery, output_feeder_food

def extract_data(surepy) -> Dict[str, List]:
    result = {}
    count = 0
    retry_count = 7
    try:
        data = asyncio.run(surepy.get_entities())
        households = [v for k, v in data.items() if v.type == EntityType.HUB]
        pets = [v for k, v in data.items() if v.type == EntityType.PET]
        feeders = [v for k, v in data.items() if v.type == EntityType.FEEDER]
    except SurePetcareConnectionError:
        logger.error(f"Error connecting to API, will retry ({count}/{retry_count})")
        if count >= retry_count:
            raise ConnectionError(f"Could not get connection to API after {count} retries. Exiting...")
        time.sleep(300)
        count += 1
    else:
        output_household, output_pets, output_feeder_history = get_household_and_pet_metrics(households, pets, feeders)
        output_feeder_battery, output_feeder_food = get_feeder_metrics(feeders)
    return output_household, output_pets, output_feeder_history, output_feeder_battery, output_feeder_food


def set_metrics(output_household, output_pets, output_feeder_battery, output_feeder_food) -> None:
    for i in output_household:
        household_status_metric.labels(*i['labels'].values()).set(i['value'])
    for i in output_feeder_battery:
        feeder_battery_metric.labels(*i['labels'].values()).set(i['value'])
    for i in output_feeder_food:
        feeder_food_metric.labels(*i['labels'].values()).set(i['value'])
    for i in output_pets:
        datetime_ts = datetime.strptime(i['ts'], "%Y-%m-%dT%H:%M:%S%z")
        datetime_ts_ms = int(datetime_ts.timestamp() * 1000)
        with open(already_written, 'r+') as f:
            if f"{datetime_ts_ms},{i['value']}" not in f.read():
                url = pushgateway_url.replace('prometheus', f'prometheus?timestamp={datetime_ts_ms}')
                payload = f'surepy_pet_food{{monitor="my-project", name="{i["labels"]["name"]}", household_id="{i["labels"]["household_id"]}", photo_url="{i["labels"]["photo_url"]}"}} {i["value"]}'
                #r = requests.post(url, payload)
                #r.raise_for_status()
                f.write(f"{datetime_ts_ms},{i['value']}\n")



def generate_csv_backfill(pet_data, feeder_data):
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backfill_pet.csv'), 'w', newline='') as f:
        writer = csv.writer(f, delimiter=',')
        for row in pet_data:
            writer.writerow([*row['labels'].values()]+[row['value'], (datetime.strptime(row['ts'], "%Y-%m-%dT%H:%M:%S%z")).strftime('%s')])
    cmd_pet = f"curl --data-binary @backfill_pet.csv http://{hostname}:{port}/api/v1/import/csv?format=1:label:name,2:label:household_id,3:label:photo_url,4:metric:surepy_pet_food,5:time:unix_s"

    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backfill_feeder.csv'), 'w', newline='') as f:
        writer = csv.writer(f, delimiter=',')
        for row in feeder_data:
            writer.writerow([*row['labels'].values()] + [row['value'], (datetime.strptime(row['ts'], "%Y-%m-%dT%H:%M:%S%z")).strftime('%s')])

    cmd_feeder = f"curl --data-binary @backfill_feeder.csv http://{hostname}:{port}/api/v1/import/csv?format=1:label:name,2:label:household_id,3:label:serial,4:metric:surepy_bowls_food,5:time:unix_s"

    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backfill.sh'), 'w', newline='') as f:
        f.write(cmd_pet+'\n')
        f.write(cmd_feeder+'\n')
    logger.info(f"Command to backfill history: {cmd_pet}, {cmd_feeder}")



if __name__ == '__main__':
    logger.info("Starting script")
    start_http_server(9000)
    scrape_time = 60
    hostname = '192.168.1.80'
    port = 8428
    pushgateway_url = url = f"http://{hostname}:{port}/api/v1/import/prometheus"
    already_written = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.already_written.txt')
    if not os.path.exists(already_written):
        open(already_written, 'w').close()
    #initial_run = True
    while True:
        surepy = Surepy(auth_token=environ.get("SUREPY_TOKEN"))
        output_household, output_pets, output_feeder_history, output_feeder_battery, output_feeder_food = extract_data(surepy)
        #if initial_run:
        #    generate_csv_backfill(output_pets, output_feeder_history)
        logger.info(f"Extracted metrics: pet food: {len(output_pets)}")
        logger.debug(f"Extracted metrics: feeder battery: {output_feeder_battery}, feeder food: {output_feeder_food}, household: {output_household}, last pet timestamp: {output_pets[-1]}")
        try:
            set_metrics(output_household, output_pets, output_feeder_battery, output_feeder_food)
        except Exception as e:
            traceback.print_exc()
            logger.error(f"Issue setting metric: {e}")
        time.sleep(scrape_time)
        #initial_run = False



