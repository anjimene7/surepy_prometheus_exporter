import asyncio
import logging
from os import environ
import time
from typing import List
from surepy import Surepy, EntityType
from surepy.entities import SurepyEntity
from surepy.exceptions import SurePetcareConnectionError
from prometheus_client import start_http_server, Gauge
from sys import stdout

# Define logger
logger = logging.getLogger('surepy_prometheus_exporter')

logger.setLevel(logging.INFO) # set logger level
logFormatter = logging.Formatter("%(name)-12s %(asctime)s %(levelname)-8s %(filename)s:%(funcName)s %(message)s")
consoleHandler = logging.StreamHandler(stdout) #set streamhandler to stdout
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

class TimestampedGauge(Gauge):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def collect(self):
        metrics = super().collect()
        for metric in metrics:
            samples = []
            for sample in metric.samples:
                timestamp = sample.labels.pop("timestamp", None)
                sample_with_timestamp = type(sample)(sample.name, sample.labels, sample.value, timestamp, sample.exemplar)
                samples.append(sample_with_timestamp)
            metric.samples = samples
        return metrics


def set_value_with_timestamp(metric, labels, value, timestamp):
    labels["timestamp"] = timestamp
    metric.labels(**labels).set(value)

def extract_data(data: List[SurepyEntity]):
    pets = [v for k, v in data.items() if v.type == EntityType.PET]
    feeders = [v for k, v in data.items() if v.type == EntityType.FEEDER]
    logger.debug(f"Got {len(pets)} pets and {len(feeders)} feeders.")
    for pet in pets:
        last_feeding_time = pet.feeding.at.timestamp()
        cat_feed.labels(pet.name, pet.household_id, last_feeding_time).set(pet.feeding.change[0])
        set_value_with_timestamp(cat_feed_ts, {"pet": pet.name, "household_id": pet.household_id, "timestamp": last_feeding_time}, pet.feeding.change[0],
                                 last_feeding_time)
    for feeder in feeders:
        feeder_food_status.labels(feeder.name, feeder.household_id).set(feeder.total_weight)
        feeder_battery_status.labels(feeder.name, feeder.household_id).set(feeder.battery_level)


if __name__ == '__main__':
    logger.info("Starting script")
    surepy = Surepy(auth_token=environ.get("SUREPY_TOKEN"))
    cat_feed = Gauge('surepy_pets_feeding', 'Pets feeding status', ["pet", "household_id", "eat_time"])
    cat_feed_ts = TimestampedGauge('surepy_pets_feeding_ts', 'Pets feeding status', ["pet", "household_id", "timestamp"])
    feeder_food_status = Gauge('surepy_bowls_food', 'Bowls food status', ["name", "household_id"])
    feeder_battery_status = Gauge('surepy_bowls_battery', 'Bowls battery status', ["name", "household_id"])
    start_http_server(9000)
    time_interval = environ.get("SUREPY_INTERVAL")
    retry_count = 7
    if time_interval is None:
        time_interval = 5
    count = 0
    while True:
        logger.info("Extracting data from API")
        try:
            data = asyncio.run(surepy.get_entities())
        except SurePetcareConnectionError:
            logger.error(f"Error connecting to API, will retry ({count}/{retry_count})")
            if count >= retry_count:
                logger.error(f"Could not get connection to API after {count} retries. Exitting...")
                break
            time.sleep(600)
            count += 1
        else:
            count = 0
            extract_data(data)
            cat_feed_ts.collect()
        time.sleep(int(time_interval))


