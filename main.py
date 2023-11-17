import asyncio
import logging
from os import environ
import time
from typing import List, Dict
from surepy import Surepy, EntityType
from surepy.exceptions import SurePetcareConnectionError
from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, REGISTRY
from sys import stdout

# Define logger
logger = logging.getLogger('surepy_prometheus_exporter')

logger.setLevel(logging.INFO) # set logger level
logFormatter = logging.Formatter("%(name)-12s %(asctime)s %(levelname)-8s %(filename)s:%(funcName)s %(message)s")
consoleHandler = logging.StreamHandler(stdout) #set streamhandler to stdout
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

class TimestampedGauge2(object):
    def __init__(self):
        pass
    def collect(self):
        logger.info(f"Collecting data")
        data = extract_data()
        cat_feed = GaugeMetricFamily(name='surepy_pets_feeding', documentation='Pets feeding status', labels=["pet", "household_id"])
        cat_feed_immediate = GaugeMetricFamily(name='surepy_pets_feeding_instant', documentation='Pets feeding status (last)', labels=["pet", "household_id"])
        feeder_food_status = GaugeMetricFamily('surepy_bowls_food', 'Bowls food status', labels=["pet", "household_id"])
        feeder_battery_status = GaugeMetricFamily('surepy_bowls_battery', 'Bowls battery status', labels=["pet", "household_id"])
        for k, v in data.items():
            for el in v:
                if "feed" == k:
                    cat_feed.add_metric([str(el[0]), str(el[1])], str(el[2]), str(el[3]))
                    cat_feed_immediate.add_metric([str(el[0]), str(el[1])], str(el[2]))
                    yield cat_feed
                    yield cat_feed_immediate
                elif "bowl" == k:
                    feeder_food_status.add_metric([str(el[0]), str(el[1])], str(el[2]))
                    feeder_battery_status.add_metric([str(el[0]), str(el[1])], str(el[3]))
                    yield feeder_battery_status
                    yield feeder_food_status



def extract_data() -> Dict[str, List]:
    result = {}
    count = 0
    retry_count = 7
    try:
        data = asyncio.run(surepy.get_entities())
    except SurePetcareConnectionError:
        logger.error(f"Error connecting to API, will retry ({count}/{retry_count})")
        if count >= retry_count:
            raise ConnectionError(f"Could not get connection to API after {count} retries. Exiting...")
        time.sleep(600)
        count += 1
    else:
        pets = [v for k, v in data.items() if v.type == EntityType.PET]
        feeders = [v for k, v in data.items() if v.type == EntityType.FEEDER]
        logger.debug(f"Got {len(pets)} pets and {len(feeders)} feeders.")
        for pet in pets:
            last_feeding_time = pet.feeding.at.timestamp()
            result.setdefault('feed', []).append([pet.name, pet.household_id, pet.feeding.change[0], last_feeding_time])
        for feeder in feeders:
            result.setdefault('bowl', []).append([feeder.name, feeder.household_id, feeder.total_weight, feeder.battery_level])
    return result


if __name__ == '__main__':
    logger.info("Starting script")
    surepy = Surepy(auth_token=environ.get("SUREPY_TOKEN"))
    start_http_server(9000)
    REGISTRY.register(TimestampedGauge2())
    while True:
        time.sleep(1)



