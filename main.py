import asyncio
import logging
from os import environ
from datetime import datetime
import time
from typing import List, Dict
from surepy import Surepy, EntityType
from surepy.exceptions import SurePetcareConnectionError
from prometheus_client import start_http_server, Gauge
from prometheus_client.core import GaugeMetricFamily, REGISTRY
from sys import stdout


class TimestampedGauge(Gauge):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def collect(self):
        metrics = super().collect()
        for metric in metrics:
            samples = []
            for sample in metric.samples:
                timestamp = sample.labels.pop("timestamp", None)
                sample_with_timestamp = type(sample)(sample.name, sample.labels,
                                                     sample.value, timestamp, sample.exemplar)
                samples.append(sample_with_timestamp)
            metric.samples = samples
        return metrics

# Define logger
logger = logging.getLogger('surepy_prometheus_exporter')

logger.setLevel(logging.DEBUG) # set logger level
logFormatter = logging.Formatter("%(name)-12s %(asctime)s %(levelname)-8s %(filename)s:%(funcName)s %(message)s")
consoleHandler = logging.StreamHandler(stdout) #set streamhandler to stdout
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

household_status_metric = Gauge('surepy_household_status', 'Status of the SurePet household (online or offline)', labelnames=["serial", "name", "household_id"])
pet_food_metric = TimestampedGauge(name='surepy_pet_food', documentation='Amount of food eaten by the pet', labelnames=["name", "household_id", "photo_url", "timestamp"])
feeder_food_metric = Gauge('surepy_bowls_food', 'Bowls food status', labelnames=["name", "household_id", "serial"])
feeder_battery_metric = Gauge('surepy_bowls_battery', 'Bowls battery status', labelnames=["name", "household_id", "serial"])



def set_value_with_timestamp(metric, labels, value, timestamp):
    labels["timestamp"] = timestamp
    metric.labels(**labels).set(value)

def get_household_and_pet_metrics(households: list[EntityType.HUB], pets: list[EntityType.PET]) -> (dict[str: dict, str: str], dict[str: dict, str: str]):
    output_household = []
    for household in households:
        report = asyncio.run(surepy.get_report(household.household_id))
        for el in households:
            output_household.append({'labels': {'serial': el.serial, 'name': el.name, 'household_id': el.household_id}, 'value': el.online})
        output_pets = []
        for el2 in report.get('data'):
            pet = [x for x in pets if x.id == el2.get('pet_id')]
            if pet:
                pet = pet[0]
            else:
                raise ValueError(f"No pet found for id {el2.pet_id}")
            feeding_datapoints = el2['feeding']['datapoints']
            feeding_datapoints_ts = [x['from'] for x in feeding_datapoints]
            feeding_datapoints_amount = [x['weights'][0]['change'] for x in feeding_datapoints]
            for ts, eat_value in zip(feeding_datapoints_ts, feeding_datapoints_amount):
                output_pets.append({'labels': {'name': pet.name, 'household_id': pet.household_id, 'photo_url': pet.photo_url}, 'ts': ts, 'value': eat_value})
    return output_household, output_pets

def get_feeder_metrics(data: list[EntityType.FEEDER]) -> dict:
    output_feeder_food, output_feeder_battery = [], []
    for feeder in data:
        output_feeder_food.append({'labels': {'serial': feeder.serial, 'name': feeder.name, 'household_id': feeder.household_id}, 'value': feeder.total_weight})
        output_feeder_battery.append({'labels': {'serial': feeder.serial, 'name': feeder.name, 'household_id': feeder.household_id}, 'value': feeder.battery_level})
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
        output_household, output_pets = get_household_and_pet_metrics(households, pets)
        output_feeder_battery, output_feeder_food = get_feeder_metrics(feeders)
    return output_household, output_pets, output_feeder_battery, output_feeder_food


def set_metrics(output_household, output_pets, output_feeder_battery, output_feeder_food) -> None:
    for i in output_household:
        household_status_metric.labels(*i['labels']).set(i['value'])
    for i in output_pets:
        timestruct = time.strptime(i['ts'], "%Y-%m-%dT%H:%M:%S%z")
        set_value_with_timestamp(pet_food_metric, i['labels'], i['value'], int(time.mktime(timestruct)))
    for i in output_feeder_battery:
        feeder_battery_metric.labels(*i['labels']).set(i['value'])
    for i in output_feeder_food:
        feeder_food_metric.labels(*i['labels']).set(i['value'])


if __name__ == '__main__':
    logger.info("Starting script")
    start_http_server(9000)
    while True:
        surepy = Surepy(auth_token=environ.get("SUREPY_TOKEN"))
        output_household, output_pets, output_feeder_battery, output_feeder_food = extract_data(surepy)
        logger.info(f"Extracted metrics: pet food: {len(output_pets)}")
        logger.debug(f"Extracted metrics: feeder battery: {output_feeder_battery}, feeder food: {output_feeder_food}, household: {output_household}, last pet timestamp: {output_pets[-1]}")
        set_metrics(output_household, output_pets, output_feeder_battery, output_feeder_food)
        time.sleep(30)



