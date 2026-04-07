import requests
import datetime
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
import os
import json
import logging
from logging.handlers import RotatingFileHandler
import time
from concurrent.futures import ThreadPoolExecutor




### Load environment variables ###
load_dotenv()
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")
BASE_URL = os.getenv("BASE_URL")
GATEWAY_IDS = json.loads(os.getenv("GATEWAY_IDS"))
SITE_ID = os.getenv("SITE_ID")
ENV_START_DATE_FIRST_OF_MONTH = os.getenv("ENV_START_DATE_FIRST_OF_MONTH")
ENV_START_DATE = os.getenv("ENV_START_DATE")
LOG_LEVEL = os.getenv("LOG_LEVEL")
LOG_FILE = os.getenv("LOG_FILE")
FETCH_ENERGY_GROUP_DATA = os.getenv("FETCH_ENERGY_GROUP_DATA")
MAX_THREADS = int(os.getenv("MAX_THREADS", "1"))
#####################################




### Configure logging ###

def configure_logging():
    log_level = getattr(logging, str(LOG_LEVEL).strip().upper(), logging.WARNING)
    log_handler = RotatingFileHandler(LOG_FILE, maxBytes=1_000_000, backupCount=10)
    log_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(log_level)
    root_logger.addHandler(log_handler)


configure_logging()
logging.info("started")
#####################################



### Global Variables ###

token = None  # Global Token

start_time = time.time()    # Record the start time
api_request_count = 0       # Counter
written_datapoints = 0      # Counter
#####################################

### Retry logic for API requests ###
def request_with_retries(url, headers, retries=3):
    global api_request_count
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            api_request_count += 1  # Count API requests
            return response.json()
        except requests.RequestException as e:
            logging.warning(f"Attempt {attempt + 1}/{retries} failed: {e}")
            time.sleep(2 ** attempt)  # Exponential backoff
    logging.error(f"API request finally failed: {url}")
    return None
#####################################



### Authenticate to get bearer token ###
def get_bearer_token():
    global token
    try:
        response = requests.post(f"{BASE_URL}/en/api/v1/login_check", json={"username": USERNAME, "password": PASSWORD})
        response.raise_for_status()
        token = response.json().get("token")
        if token:
            logging.info("Authentication successful.")
        return token
    except requests.RequestException as e:
        logging.error(f"Authentication error: {e}")
        return None
#####################################



### Fetch energy data from gateway ###
def fetch_energy_data(gateway_id, date):
    global token
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{BASE_URL}/en/api/v1/gateway/{gateway_id}/energy/month?date={date}"
    data = request_with_retries(url, headers)
    if data is None and token:
        logging.info("Renewing token and retrying...")
        get_bearer_token()
        headers = {"Authorization": f"Bearer {token}"}
        data = request_with_retries(url, headers)
    return data
#####################################



### Fetch energy data from whole site ###
def fetch_site_energy_data(site_id, date):
    global token
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{BASE_URL}/en/api/v1/site/{site_id}/energy/month?date={date}"
    data = request_with_retries(url, headers)
    if data is None and token:
        logging.info("Renewing token and retrying...")
        get_bearer_token()
        headers = {"Authorization": f"Bearer {token}"}
        data = request_with_retries(url, headers)
    return data
#####################################



### Fetch gateway data (ids, name) ###
def fetch_gateway_groups(gateway_id):
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{BASE_URL}/en/api/v1/gateway/{gateway_id}/groups"
    return request_with_retries(url, headers)
#####################################



### Fetch energy data from group ###
def fetch_group_energy_data(group_id, date):
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{BASE_URL}/en/api/v1/group/{group_id}/energy/month?date={date}"
    data = request_with_retries(url, headers)
    if data is None and token:
        logging.info("Renewing token and retrying...")
        get_bearer_token()
        headers = {"Authorization": f"Bearer {token}"}
        data = request_with_retries(url, headers)
    return data
#####################################



### Try to convert strings to float, return None if conversion fails ###
def safe_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
#####################################



### Write data to InfluxDB ###
def write_gateway_to_influx(data, gateway_id, write_api):
    unit_name = GATEWAY_IDS.get(str(gateway_id), "Unknown")
    global written_datapoints
    if "periodTotals" in data:
        period_totals = data["periodTotals"]
        point = influxdb_client.Point("energy_summary") \
            .tag("gateway_id", str(gateway_id)) \
            .tag("unit_name", unit_name)
        for key, value in period_totals.items():
            num_value = safe_float(value)
            if num_value is not None:
                point = point.field(key, num_value)
                written_datapoints += 1  # Count written points
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
    
    relevant_keys = ["carbonProduced", "energyCost", "energyUsage", "kilowattHours"]
    for key in relevant_keys:
        if key in data:
            for timestamp, value in data[key].items():
                num_value = safe_float(value)
                if num_value is not None:
                    point = influxdb_client.Point("energy_data") \
                        .tag("gateway_id", str(gateway_id)) \
                        .tag("unit_name", unit_name) \
                        .time(timestamp) \
                        .field(key, num_value)
                    written_datapoints += 1  # Count written points
                    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
    logging.info(f"Data written for {unit_name} (Gateway {gateway_id}) {timestamp}.")
#####################################



### Write whole site data to InfluxDB ###
def write_site_to_influx(data, site_id, write_api):
    global written_datapoints

    if "periodTotals" in data:
        period_totals = data["periodTotals"]
        point = influxdb_client.Point("site_energy_summary") \
            .tag("site_id", str(site_id))
        for key, value in period_totals.items():
            num_value = safe_float(value)
            if num_value is not None:
                point = point.field(key, num_value)
                written_datapoints += 1
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)

    relevant_keys = ["carbonProduced", "energyCost", "energyUsage", "kilowattHours"]
    for key in relevant_keys:
        if key in data:
            for timestamp, value in data[key].items():
                num_value = safe_float(value)
                if num_value is not None:
                    point = influxdb_client.Point("site_energy_data") \
                        .tag("site_id", str(site_id)) \
                        .time(timestamp) \
                        .field(key, num_value)
                    written_datapoints += 1
                    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
    logging.info(f"Data written for site {site_id} {timestamp}.")
#####################################



### Write group data to InfluxDB ###
def write_group_to_influx(data, gateway_id, group_name, group_address, write_api):
    unit_name = GATEWAY_IDS.get(str(gateway_id), "Unknown")
    global written_datapoints
    if "periodTotals" in data:
        period_totals = data["periodTotals"]
        point = influxdb_client.Point("group_energy_summary") \
            .tag("gateway_id", str(gateway_id)) \
            .tag("unit_name", unit_name) 
        for key, value in period_totals.items():
            num_value = safe_float(value)
            if num_value is not None:
                point = point.field(key, num_value)
                written_datapoints += 1  # Count written points
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
    
    relevant_keys = ["carbonProduced", "energyCost", "energyUsage", "kilowattHours"]
    for key in relevant_keys:
        if key in data:
            for timestamp, value in data[key].items():
                num_value = safe_float(value)
                if num_value is not None:
                    point = influxdb_client.Point("group_energy_data") \
                        .tag("gateway_id", str(gateway_id)) \
                        .tag("unit_name", unit_name) \
                        .tag("group_name", group_name) \
                        .tag("group_address", group_address) \
                        .time(timestamp) \
                        .field(key, num_value)
                    written_datapoints += 1  # Count written points
                    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
    logging.info(f"Data written for {unit_name} (Gateway {gateway_id} : Group {group_name}) {timestamp}.")
#####################################



### Process gateway and its groups ###
def process_gateway(gateway_id, write_api, date):
    data = fetch_energy_data(gateway_id, date)
    if data:
        write_gateway_to_influx(data, gateway_id, write_api)

    bool_fetch_group_energy = parse_env_bool(FETCH_ENERGY_GROUP_DATA)
    if not bool_fetch_group_energy:
        return

    groups = fetch_gateway_groups(gateway_id)
    if groups:
        for group in groups:
            group_id = group["id"]
            group_name = group["name"]
            group_address = group["group_address"]
            group_data = fetch_group_energy_data(group_id, date)
            if group_data:
                write_group_to_influx(group_data, gateway_id, group_name, group_address, write_api)
#####################################



### Process whole site ###
def process_site(site_id, write_api, date):
    data = fetch_site_energy_data(site_id, date)
    if data:
        write_site_to_influx(data, site_id, write_api)
#####################################



### Parse strings from .env to bool ###
def parse_env_bool(value):
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}
#####################################



### Parse date string from .env to datetime.date ###
def parse_env_date(date_value):
    if not date_value:
        return None
    return datetime.date.fromisoformat(date_value)
#####################################



### Get first day of the month for a given date ###
def first_day_of_month(date_value):
    return date_value.replace(day=1)
#####################################



### Add one month to a given date, handling month/year rollover ###
def add_month(date_value):
    year = date_value.year + (date_value.month // 12)
    month = date_value.month % 12 + 1
    first_of_next_month = datetime.date(year, month, 1)
    last_day_current_month = first_of_next_month - datetime.timedelta(days=1)
    day = min(date_value.day, last_day_current_month.day)
    return datetime.date(year, month, day)
#####################################



### Main loop ###
def main():
    global token
    logging.info("##################################################################")
    logging.info("Script started.")
    logging.info("Log level: %s", LOG_LEVEL)
    logging.info("FETCH_ENERGY_GROUP_DATA: %s", parse_env_bool(FETCH_ENERGY_GROUP_DATA))
    token = get_bearer_token()
    if not token:
        logging.error("Exiting script: No token received.")
        return

 
    start_date = parse_env_date(ENV_START_DATE)
    if parse_env_bool(ENV_START_DATE_FIRST_OF_MONTH):
        start_date = first_day_of_month(datetime.date.today())
    if not start_date:
        logging.error("Exiting script: No start date configured.")
        return
    client = influxdb_client.InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    today = datetime.date.today()
    current_date = start_date
    while current_date <= today:
        request_date = current_date.strftime("%Y-%m-%d")
        if SITE_ID:
            process_site(SITE_ID, write_api, request_date)
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = [executor.submit(process_gateway, gateway_id, write_api, request_date) for gateway_id in GATEWAY_IDS]
            for future in futures:
                future.result()
        current_date = add_month(current_date)
    
    client.close()
    end_time = time.time()              # Record the end time
    runtime = end_time - start_time     # Calculate runtime
    logging.info("All gateways processed.")
    logging.info(f"Runtime: {runtime:.2f} seconds")
    logging.info(f"Total API requests: {api_request_count}")
    logging.info(f"Total data points written: {written_datapoints}")

if __name__ == "__main__":
    main()





















