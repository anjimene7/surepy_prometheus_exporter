import datetime
import os.path
from os import environ
from cachetools import TTLCache
import logging
import requests
import json
from traceback import print_exc
from requests import Session
import time
from collections import defaultdict
from typing import Optional, Any, TypedDict, Tuple, Dict
from enum import Enum
from os import environ
from uuid import uuid1
from sys import stdout
import pandas as pd
from prometheus_client import start_http_server, Gauge, push_to_gateway
from datetime import datetime
from zoneinfo import ZoneInfo
from dateutil.relativedelta import relativedelta

logger = logging.getLogger('surepy_prometheus_exporter')
logFormatter = logging.Formatter("%(name)-12s %(asctime)s %(levelname)-8s %(filename)s:%(funcName)s:%(lineno)d %(message)s")
consoleHandler = logging.StreamHandler(stdout)
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

cache = TTLCache(maxsize=128, ttl=86400)

class SurepyDeviceType(Enum):
    HUB = 1
    FEEDER = 4


class SurePetAPIClient:
    def __init__(self, email: str, password: str, page_size: int):
        self.base_url = 'https://app-api.production.surehub.io/api'  # 'https://app-api.blue.production.surehub.io/api'
        self.client_id = str(uuid1())
        self.session = Session()
        self.session.headers.update({'Content-Type': 'application/json',
                                     'Connection': 'Keep-Alive',
                                     'X-Device-Id': self.client_id,
                                     'X-Requested-With': 'com.sureflap.surepetcare',
                                     'Host': 'app.api.surehub.io',
                                     'Accept': 'application/json',
                                     'Accept-Encoding': 'gzip, deflate'
                                     })
        self.email = email
        self.password = password
        self.page_size = page_size  # not used for now
        self.authenticate()

    def authenticate(self) -> None:
        """
        Login and get token for API calls. Can either provide plain text or file containing the password. This also refreshes based on cache value
        :return: Bearer token
        """
        tk = cache.get("token")
        if tk:
            token = tk
        else:
            if os.path.isfile(self.password):
                with open(self.password, 'r') as f:
                    pwd = f.read().strip()
            logger.debug(f"Credentials : {email}, {pwd}")
            endpoint = 'auth/login'
            payload = {'client_uid': self.client_id, 'email_address': self.email, 'password': pwd}
            response = self._request_and_validate(endpoint=endpoint, method='post', data=payload)
            json_data = json.loads(response.text)
            token = json_data['data']['token']
            cache["token"] = token
        self.session.headers.update({'Authorization': f"Bearer {token}"})

    def load_object(self, endpoint: str, params: Optional[dict] = None) -> list:
        with self._request_and_validate(endpoint=endpoint, params=params) as data:
            try:
                surepy_data = json.loads(data.text).get('data')
            except Exception as e:
                logger.error(f"{e} -> {data} -> {vars(data)}")
            surepy_data = surepy_data if isinstance(surepy_data, list) else [surepy_data]
        return surepy_data

    def curlify(self, response: requests.Response) -> str:
        request = response.request
        method = request.method
        url = request.url
        data = request.body
        headers = request.headers.copy()
        headers.update({'Authorization': '*****'})
        headers_str = ' '.join([' -H "{0}: {1}"'.format(k, v) for k, v in headers.items()])
        command = f"curl -X {method}{headers_str} '{url}'"
        if method == 'post':
            command += f' -d \'{json.dumps(data)}\''
        return command

    def _request_and_validate(self, endpoint: str, method: str = 'get', data: Optional[dict] = None,
                              params: Optional[dict] = None) -> requests.Response:
        url = os.path.join(self.base_url, endpoint)
        if method == 'get':
            response = self.session.get(url, params=params)
        elif method == 'post':
            response = self.session.post(url, json=data)

        try:
            response.raise_for_status()
            logger.debug(self.curlify(response))
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logger.error(f"Could not call API, please check credentials: {e}")
                raise
        else:
            logger.debug(response.request)
        return response


class SurepyDevicePrometheus:
    def __init__(self, **kwargs):
        self.metrics = kwargs['metrics']
        self.api_client = kwargs['client']
        self.last_scrape = kwargs['last_scrape']
        self.pushgateway_url = kwargs['pushgateway_url']
        self.labels_values = defaultdict(list)
        self.values = {}

    def set_attributes(self, data: dict) -> None:
        self.id = data['id']
        self.name = data['name']

    def update_prometheus_metrics_with_labels_values(self) -> None:
        self.labels_values = defaultdict(list)
        grouping_keys = ['bowl', 'context']
        for metric_type, metrics in self.metrics.items():
            for name, metric in metrics.items():
                labels = {}
                value = self.values[name]
                standard_labels = set(metric._labelnames) - set(grouping_keys) - {'timestamp'}
                for label in standard_labels:
                    label_value = getattr(self, label)
                    labels.update({label: label_value})
                if set(grouping_keys).issubset(set(metric._labelnames)):
                    for bowl, context in value.groups.keys():
                        labels.update({'bowl': bowl})
                        labels.update({'context': context})
                        val = value.get_group((bowl, context))
                        self.labels_values[name].append({'labels': labels.copy(), 'value': val})
                else:
                    self.labels_values[name].append({'labels': labels, 'value': float(value)})

    def update_object_attributes(self, endpoint: str) -> None:
        data = client.load_object(f'{endpoint}/{self.id}')
        self.set_attributes(data[0])

    def _push_metric_to_gateway(self, metric: Gauge, element: dict) -> requests.Response:
        for _, row in element['value'].iterrows():
            name = metric._name
            ts = row['from']
            ts_ms = int(datetime.fromisoformat(ts).timestamp()*1000)
            labels = element['labels']
            val_col_name = set(row.index) - set(['from'])
            val = abs(row[list(val_col_name)[0]])
            populated_row = f"{name},{ts},{list(labels.values())},{val}"
            with open(already_written, 'r+') as f:
                lines = f.read().split('\n')
                if populated_row not in lines:
                    logger.debug(f"Pushing metric: {ts} -> labels: {labels} -> value: {val}")
                    labels_formatted = ','.join([f'{key}="{value}"' for key, value in labels.items()])
                    payload = f'{name}{{{labels_formatted}}} {val} {ts_ms}'
                    r = requests.post(self.pushgateway_url, payload)
                    r.raise_for_status()
                    logger.debug(client.curlify(r))
                    f.write(f"{populated_row}\n")
                else:
                    logger.debug(f"{populated_row} already written, skipping")
                    continue

    def set_prometheus_metrics_values(self) -> None:
        for _, metrics in self.metrics.items():
            for name, metric in metrics.items():
                elements = self.labels_values[name]
                for element in elements:
                    logger.info(f"Setting metric: {name}, labels: {element['labels']}")
                    if isinstance(element['value'], pd.DataFrame):
                        self._push_metric_to_gateway(metric, element)
                    else:
                        metric.labels(**element['labels']).set(abs(element['value']))
    def other(self) -> None:
        for name, metric in self.metrics['push'].items():
            value = self.labels_values[name]['value']
        for _, row in self.report.iterrows():
            datetime_ts = datetime.fromisoformat(row['from']).timestamp()
            datetime_ts_ms = int(1000 * datetime_ts)
            if not self.one_time_setup:
                if datetime.fromtimestamp(datetime_ts) < self.last_scrape:
                    logger.debug(f'Skipping {row} as {datetime_ts} < {self.last_scrape}')
                    continue
            url = self.pushgateway_url.replace('prometheus', f'prometheus?timestamp={datetime_ts_ms}')
            for key, metric_value in timeline_metrics.items():
                for idx in [0, 1]:
                    value = abs(row[metric_value[1].format(idx)])
                    populated_row = f"{key},{datetime_ts},{row['name']},{value},{idx}"
                    with open(already_written, 'r+') as f:
                        lines = f.read().split('\n')
                        if populated_row not in lines:
                            logger.info(f"Writing {populated_row} to PushAway")
                            if metric_value[1] != 'surepy_timeline_duration_meal':
                                payload = f'{key}{{monitor="my-project", name="{row["name"]}"}} {value}'
                            else:  # with bowl
                                payload = f'{key}{{monitor="my-project", name="{row["name"]}", bowl="{idx}"}} {value}'
                            r = requests.post(url, payload)
                            r.raise_for_status()
                            logger.debug(self._curlify(r))
                            f.write(f"{populated_row}\n")


class Household(SurepyDevicePrometheus):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.set_attributes(kwargs['data'])

    def set_attributes(self, data: dict) -> None:
        super().set_attributes(data)
        self.created_at = data['created_at']
        self.timezone = data['timezone']
        self.users = data['users']
        self.values.update({'surepy_household_life': datetime.fromisoformat(self.created_at).timestamp()})
        super().update_prometheus_metrics_with_labels_values()


class Pet(SurepyDevicePrometheus):
    def __init__(self, one_time_setup: bool, history_to_pull: int, **kwargs):
        super().__init__(**kwargs)
        self.one_time_setup = one_time_setup
        self.history_to_pull = int(history_to_pull)
        self.bowl = [0, 1]  # Bowl indexes
        self.context = [1, 5]  # Context of bowl activity, 1: feed, 5: reload
        self.report: pd.DataFrame = pd.DataFrame()
        self.set_attributes(kwargs['data'])

    def get_report(self) -> None:
        to_ts = from_ts = datetime.now()
        from_ts_limit = to_ts - relativedelta(months=self.history_to_pull)
        tmp = []
        while from_ts > from_ts_limit:
            if self.one_time_setup:  # initial run, extract full history as per env variable
                from_ts -= relativedelta(months=1)
            else:
                from_ts_limit = from_ts
            params = {'from': from_ts, 'to': to_ts}
            logger.info(f"Extracting report for range [{from_ts}] - [{to_ts}]")
            data = client.load_object(f'report/household/{self.household_id}/pet/{self.id}/device/{self.device_id}', params=params)
            tmp.extend(data[0]['feeding']['datapoints'])
            to_ts = from_ts
        if self.one_time_setup:
            self.one_time_setup = False
        df_json = pd.DataFrame(tmp).explode('weights').to_json(orient='records')
        report = pd.json_normalize(json.loads(df_json), meta=['weights'])
        report.rename(columns={"weights.index": "bowl"}, inplace=True)
        self.report_raw = report
        self.report = report.groupby(['bowl', 'context'], as_index=False)

    def set_attributes(self, data: dict) -> None:
        super().set_attributes(data)
        self.household_id = data['household_id']
        self.device_id = data['status']['feeding']['device_id']
        self.gender = data['gender']
        self.dob = data['date_of_birth']
        self.weight = data['weight']
        self.photo_url: dict = data['photo']['location']  # keys of interest : 'id', 'location', 'created_at'
        self.photo_updated_at = time.mktime(time.strptime(data['photo']['updated_at'], "%Y-%m-%dT%H:%M:%S%z"))
        self.get_report()
        self.values.update({'surepy_pet_dob': time.mktime(time.strptime(self.dob, "%Y-%m-%dT%H:%M:%S%z")),
                            'surepy_pet_photo': self.photo_updated_at,
                            'surepy_pet_weight': self.weight,
                            'surepy_pet_duration_meal_push': self.report[['from', 'duration']],
                            'surepy_pet_curr_weight_push': self.report[['from', 'weights.weight']],
                            'surepy_pet_amount_meal_push': self.report[['from', 'weights.change']]})
        super().update_prometheus_metrics_with_labels_values()


class Device(SurepyDevicePrometheus):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.set_attributes(kwargs['data'])

    def set_attributes(self, data: dict) -> None:
        super().set_attributes(data)
        self.product_id = SurepyDeviceType(data['product_id'])
        self.household_id = data['household_id']
        self.serial_number = data['serial_number']
        self.created_at = data['created_at']
        self.online = data['status']['online']
        self.version = data['status']['version']['device']
        self.battery_voltage = data['status']['battery'] if self.product_id != SurepyDeviceType['HUB'] else 0
        num_batteries = 4
        voltage_max = 1.6
        voltage_low = 1.2
        voltage_diff = voltage_max - voltage_low
        voltage_per_battery = self.battery_voltage / num_batteries
        voltage_per_battery_diff = voltage_per_battery - voltage_low
        self.battery = max(min(int(voltage_per_battery_diff / voltage_diff * 100), 100), 0)
        self.status = data['status']
        self.control = data['control']  # TODO: use these values in metric
        self.values.update({'surepy_device_status': self.online,
                            'surepy_device_life': datetime.fromisoformat(self.created_at).timestamp(),
                            'surepy_device_battery': self.battery,
                            'surepy_device_version_hw': self.version['hardware'],
                            'surepy_device_version_fw': self.version['firmware']
                            })
        super().update_prometheus_metrics_with_labels_values()


class MainWorker:
    def __init__(self, client: SurePetAPIClient, pushgateway_url: str, ots: bool, history_to_pull: int):
        self.api_client = client
        self.pushgateway_url = pushgateway_url
        self.last_scrape = None
        self.page_size = 25
        self.one_time_setup = ots
        self.history_to_pull = int(history_to_pull)
        metrics_definitions = {'household': {'pull': [{'surepy_household_life': {'doc': 'Creation date of household', 'labels': ['id', 'name']}}]},
                               'device': {'pull': [{'surepy_device_status': {'doc': 'Status of the SurePet device (online or offline)', 'labels': ['id', 'name', 'household_id', 'serial_number']}},
                                   {'surepy_device_life': {'doc': 'Time since activation of device', 'labels': ['id', 'name', 'household_id', 'serial_number']}},
                                   {'surepy_device_version_hw': {'doc': 'Version of hardware', 'labels': ['id', 'name', 'household_id', 'serial_number']}},
                                   {'surepy_device_version_fw': {'doc': 'Version of firmware', 'labels': ['id', 'name', 'household_id', 'serial_number']}},
                                   {'surepy_device_battery': {'doc': 'Battery of device', 'labels': ['id', 'name', 'household_id', 'serial_number']}}]},
                               'pet': {'pull': [{'surepy_pet_dob': {'doc': 'Date of birth of pet', 'labels': ['id', 'name', 'household_id']}},
                                                {'surepy_pet_photo': {'doc': 'Photo URL of pet', 'labels': ['id', 'name', 'household_id', 'photo_url']}},
                                                {'surepy_pet_weight': {'doc': 'Weight of pet', 'labels': ['id', 'name', 'household_id']}}],
                                       'push': [{'surepy_pet_duration_meal_push': {'doc': 'Duration of meal in seconds', 'labels': ['id', 'name', 'household_id', 'bowl', 'context']}},
                                                {'surepy_pet_curr_weight_push': {'doc': 'Current weight of bowls', 'labels': ['id', 'name', 'household_id', 'bowl', 'context']}},
                                                {'surepy_pet_amount_meal_push': {'doc': 'Amount of food eaten during meal', 'labels': ['id', 'name', 'household_id', 'bowl', 'context']}}]}}

        self.metrics = defaultdict(dict)
        for endpoint, metrics in metrics_definitions.items():
            for metric_type, metrics_def in metrics.items():
                self.metrics[endpoint].update({metric_type: self._create_metrics(metrics_def)})
        self.households: list[Household] = [Household(data=x, metrics=self.metrics['household'], client=self.api_client, last_scrape=self.last_scrape, pushgateway_url=self.pushgateway_url) for x in client.load_object('household')]  # Initial setup, objects will take care of updating themselves later
        self.devices: list[Device] = [Device(data=x, metrics=self.metrics['device'], client=self.api_client, last_scrape=self.last_scrape, pushgateway_url=self.pushgateway_url) for x in client.load_object('device')]
        self.pets: list[Pet] = [Pet(data=x, metrics=self.metrics['pet'], client=self.api_client, last_scrape=self.last_scrape, pushgateway_url=self.pushgateway_url, one_time_setup=self.one_time_setup, history_to_pull=self.history_to_pull) for x in client.load_object('pet')]
        self.all_objects = {'household': self.households, 'device': self.devices, 'pet': self.pets}
        #self.all_objects = {'pet': self.pets}

    def _create_metrics(self, metrics: list[str: dict]) -> dict[str: Gauge]:
        result = {}
        for i in metrics:
            for k, v in i.items():
                result.update({k: Gauge(k, documentation=v['doc'], labelnames=v['labels'])})
        return result

    def update_data(self):
        logger.info(f"Updating metrics values, last scrape time: {self.last_scrape}")
        client.authenticate()
        for endpoint, object_list in self.all_objects.items():
            for item in object_list:
                item.update_object_attributes(endpoint)
        self.last_scrape = datetime.now()

    def set_metrics(self):
        logger.info("Setting prometheus metrics")
        for endpoint, object_list in self.all_objects.items():
            for item in object_list:
                item.set_prometheus_metrics_values()


def validate_environ(variable_str: str) -> str:
    variable = environ.get(variable_str)
    if variable is None:
        raise EnvironmentError(f'{variable_str} has not been found! Please set it.')
    return variable


if __name__ == "__main__":
    email = validate_environ('SUREPY_EMAIL')
    password = validate_environ('SUREPY_PWD')
    hostname = validate_environ('SUREPY_PROMETHEUS_HOST')
    port = validate_environ('SUREPY_PROMETHEUS_PORT')
    one_time_setup = environ.get('SUREPY_ONE_TIME_SETUP', 'TRUE').lower() == 'true'
    debug_level = environ.get('SUREPY_LOG_LEVEL', 'INFO')
    max_history_to_pull = environ.get('SUREPY_HISTORY_PULL_IN_MONTHS', 1)
    scrape_time = int(environ.get('SUREPY_SCRAPE_S', 300))

    logger.setLevel(debug_level)
    logger.info("Starting script")
    start_http_server(9000)
    pushgateway_url = f"http://{hostname}:{port}/api/v1/import/prometheus"
    already_written = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'already_written.txt')
    if not os.path.exists(already_written):
        logger.info(f'Creating {already_written} file')
        open(already_written, 'w').close()

    client = SurePetAPIClient(email=email, password=password, page_size=25)
    worker = MainWorker(client=client, pushgateway_url=pushgateway_url, ots=one_time_setup,
                        history_to_pull=max_history_to_pull)
    worker.set_metrics()
    while True:
        try:
            worker.update_data()
            worker.set_metrics()
        except Exception as e:
            logger.error(f"Could not get metrics: {e} -> {print_exc()}")
        finally:
            time.sleep(scrape_time)
