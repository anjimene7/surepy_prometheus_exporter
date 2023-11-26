import asyncio
import logging
from os import environ
import time
from typing import List, Dict
from surepy import Surepy, EntityType
from surepy.exceptions import SurePetcareConnectionError
from prometheus_client import start_http_server, Gauge
from prometheus_client.core import GaugeMetricFamily, REGISTRY
from sys import stdout


# Define logger
logger = logging.getLogger('surepy_prometheus_exporter')

logger.setLevel(logging.DEBUG) # set logger level
logFormatter = logging.Formatter("%(name)-12s %(asctime)s %(levelname)-8s %(filename)s:%(funcName)s %(message)s")
consoleHandler = logging.StreamHandler(stdout) #set streamhandler to stdout
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

cat_feed_immediate = Gauge(name='surepy_pets_feeding_instant', documentation='Pets feeding status (last)', labelnames=["pet", "household_id"])
feeder_food_status = Gauge('surepy_bowls_food', 'Bowls food status', labelnames=["pet", "household_id"])
feeder_battery_status = Gauge('surepy_bowls_battery', 'Bowls battery status', labelnames=["pet", "household_id"])

def extract_data(surepy) -> Dict[str, List]:
    result = {}
    count = 0
    retry_count = 7
    try:
        data = asyncio.run(surepy.get_entities())
    except SurePetcareConnectionError:
        logger.error(f"Error connecting to API, will retry ({count}/{retry_count})")
        if count >= retry_count:
            raise ConnectionError(f"Could not get connection to API after {count} retries. Exiting...")
        time.sleep(300)
        count += 1
    else:
        pets = [v for k, v in data.items() if v.type == EntityType.PET]
        feeders = [v for k, v in data.items() if v.type == EntityType.FEEDER]
        logger.debug(f"Got {len(pets)} pets and {len(feeders)} feeders.")
        for pet in pets:
            last_feeding_time = pet.feeding.at.timestamp()
            logger.debug(f"Last feeding time ({pet.name}): {last_feeding_time} -> {pet.feeding.change[0]}")
            result.setdefault('feed', []).append([pet.name, pet.household_id, pet.feeding.change[0], last_feeding_time])
        for feeder in feeders:
            result.setdefault('bowl', []).append([feeder.name, feeder.household_id, feeder.total_weight, feeder.battery_level])
    return result


if __name__ == '__main__':
    logger.info("Starting script")
    start_http_server(9000)
    while True:
        surepy = Surepy(auth_token=environ.get("SUREPY_TOKEN"))
        data = extract_data(surepy)
        for k, v in data.items():
            if k == 'feed':
                for val in v:
                    cat_feed_immediate.labels(*val[0:2]).set(val[2])
            else: # k == 'bowl'
                for val in v:
                    feeder_food_status.labels(*val[0:2]).set(val[2])
                    feeder_battery_status.labels(*  val[0:2]).set(val[3])
        time.sleep(30)



