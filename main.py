import asyncio
from os import environ
import time
from typing import List
from surepy import Surepy
from surepy.entities.pet import Pet
from prometheus_client import start_http_server, Gauge


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


def set_value_with_timestamp(metric, labels, value, timestamp):
    labels["timestamp"] = timestamp
    metric.labels(**labels).set(value)


if __name__ == '__main__':
    surepy = Surepy(auth_token=environ.get("SUREPY_TOKEN"))
    g = TimestampedGauge('surepy_pets_feeding', 'Pets feeding status', ["pet", "household_id", "timestamp"])
    start_http_server(9000)
    while True:
        pets: List[Pet] = asyncio.run(surepy.get_pets())
        for pet in pets:
            last_feeding_time = pet.feeding.at.timestamp()
            set_value_with_timestamp(g, {"pet": pet.name, "household_id": pet.household_id}, pet.feeding.change[0], last_feeding_time)
            #time.sleep(environ.get(int("SUREPY_INTERVAL")))
            time.sleep(5)
