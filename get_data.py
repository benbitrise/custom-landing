import os
import argparse
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RetryError
from urllib3.util.retry import Retry
from datetime import date, datetime, timedelta
import json
import csv
import sys
import io
import time

API_BASE_URL = "https://integrations.apptopia.com/api"
USER_AGENT = "insomnia/8.6.0"
client = os.environ.get('APPTOPIA_CLIENT')
secret = os.environ.get('APPTOPIA_SECRET')

def requests_retry_session(retries=10, backoff_factor=1, status_forcelist=(429, 500, 502, 504), session=None):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def safe_request(url, method='GET', **kwargs):
    try:
        session = requests_retry_session()
        response = session.request(method, url, **kwargs)
        response.raise_for_status()
        return response
    except RetryError as e:
        if e.last_response is not None and e.last_response.status_code == 429:
            retry_after = int(e.last_response.headers.get('retry-after', 60))
            print(f"Rate limit exceeded. Retrying after {retry_after} seconds.")
            time.sleep(retry_after)
            return safe_request(url, method, **kwargs)
        else:
            raise

def get_auth_token(client, secret):
    url = f"{API_BASE_URL}/login"
    auth_payload = {
        "client": client,
        "secret": secret
    }
    auth_response = requests_retry_session().post(url, json=auth_payload).json()
    return auth_response['token']

def get_Store_categories(token, store):
    url = f"{API_BASE_URL}/{store}/categories"
    headers = {
        "User-Agent": USER_AGENT,
        "Authorization": token
    }
    
    store_categories = safe_request(url, headers=headers).json()  # Parse the JSON response
    id_map = {item['id']: item for item in store_categories}
    return id_map

def get_top_apps(token, store, country, category_id):
    url = f"{API_BASE_URL}/{store}/rank_lists"
    current_date = (date.today() - timedelta(days=7)).isoformat()
    params = {
        "id": category_id,
        "kind": "free",
        "country_iso": country,
        "date": current_date
    }
    headers = {
        "User-Agent": USER_AGENT,
        "Authorization": token
    }
    response = safe_request(url, headers=headers, params=params).json()
    return response

def get_root_category(store):
    if store == "itunes_connect":
        return 36
    if store == "google_play":
        return 39
    raise f"invalid store name {store}"

def get_all_top_apps(token, store, country, categories):
    root_category = get_root_category(store)
    return {
        key: {
            "apps": get_top_apps(token, store, country, key),
            "name": value["name"],
            "id": key
        }
        for key, value in categories.items() if value['parent_id'] == root_category
    }

def save_top_to_file(top_charts, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(top_charts, f, ensure_ascii=False, indent=4)

def get_app_details(token, store, app_id):
    url = f"{API_BASE_URL}/{store}/app"
    params = {"id": app_id}
    headers = {
        "User-Agent": USER_AGENT,
        "Authorization": token
    }
    response = safe_request(url, headers=headers, params=params).json()
    try:
        details = response[0]
        keys_to_keep = {'screenshot_urls', 'publisher_id', 'publisher_name', 'icon_url', 'name', 'current_version', 'id'}
        return {k: details[k] for k in keys_to_keep}
    except (IndexError, KeyError) as e:
        print(f"Error fetching details for app_id {app_id}: {e}")
        print(response)
        return None

def create_rankings(token, store, country, top_charts, filename):
    fieldnames = ['country', 'store', 'category', 'rank', 'app_id', 'app_name', 'publisher_id', 'publisher_name', 'icon', 'screenshot', 'most_recent_release', 'release_count_in_past_year']
    with open(filename, 'w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        test_ctr=0
        for value in top_charts.values():
            test_ctr+=1
            for idx, app_id in enumerate(value['apps'][0]["app_ids"], start=1):
                app_details = get_app_details(token, store, app_id)
                version_info = get_releases(token, store, country, app_id) if app_details else None
                if app_details and version_info:
                    row = {
                        'country': country,
                        'store': store,
                        'category': value['name'],
                        'rank': idx,
                        'app_id': app_details['id'],
                        'app_name': app_details['name'],
                        'publisher_id': app_details['publisher_id'],
                        'publisher_name': app_details['publisher_name'],
                        'icon': app_details['icon_url'],
                        'screenshot': app_details['screenshot_urls'][0] if app_details['screenshot_urls'] else None,
                        'most_recent_release': version_info[0].isoformat() if version_info[0] else None,
                        'release_count_in_past_year': version_info[1]
                    }
                    writer.writerow(row)
                    print(' '.join(str(value) for value in row.values()))
                print(f"Test: {os.getenv("TEST")} ctr: {test_ctr}")
                if os.getenv("TEST") == "true" and test_ctr>4:
                    break

def get_releases(token, store, country, app_id):
    url = f"{API_BASE_URL}/{store}/app_versions"
    params = {"id": app_id, "country_iso": country}
    headers = {
        "User-Agent": USER_AGENT,
        "Authorization": token
    }
    response = safe_request(url, headers=headers, params=params).json()
    most_recent_date, versions_in_past_year = process_versions(response)
    return (most_recent_date, versions_in_past_year)

def process_versions(versions_data):
    most_recent_date = None
    versions_in_past_year = 0
    one_year_ago = datetime.now() - timedelta(days=365)

    for item in versions_data:
        for version in item.get("versions", []):
            version_date = datetime.strptime(version["date"], "%Y-%m-%d")
            if not most_recent_date or version_date > most_recent_date:
                most_recent_date = version_date
            if version_date > one_year_ago:
                versions_in_past_year += 1

    return most_recent_date, versions_in_past_year



countries = [
    {"country": "United States", "code": "US"},
    {"country": "Mexico", "code": "MX"},
    {"country": "Brazil", "code": "BR"},
    {"country": "Canada", "code": "CA"},
    {"country": "France", "code": "FR"},
    {"country": "Germany", "code": "DE"},
    {"country": "Austria", "code": "AT"},
    {"country": "Italy", "code": "IT"},
    {"country": "Belgium", "code": "BE"},
    {"country": "Spain", "code": "ES"},
    {"country": "Portugal", "code": "PT"},
    {"country": "Indonesia", "code": "ID"},
    {"country": "Singapore", "code": "SG"},
    {"country": "Qatar", "code": "QA"},
    {"country": "United Arab Emirates", "code": "AE"},
    {"country": "Jordan", "code": "JO"},
    {"country": "Israel", "code": "IL"},
    {"country": "Poland", "code": "PL"},
    {"country": "Turkey", "code": "TR"},
    {"country": "Australia", "code": "AU"},
    {"country": "New Zealand", "code": "NZ"},
    {"country": "Japan", "code": "JP"},
    {"country": "India", "code": "IN"},
    {"country": "Norway", "code": "NO"},
    {"country": "Sweden", "code": "SE"},
    {"country": "United Kingdom", "code": "GB"},
    {"country": "Holland", "code": "NL"},
    {"country": "Denmark", "code": "DK"}
]


# example usage python get_data.py --country US --store itunes_connect
parser = argparse.ArgumentParser(description='Get top app charts for a given country and store.')
parser.add_argument('--country', type=str, required=True, help='ISO country code')
parser.add_argument('--store', type=str, required=True, choices=['itunes_connect', 'google_play'], help='Store name (itunes_connect or google_play)')
args = parser.parse_args()

if __name__ == "__main__":
    if not client or not secret:
        raise ValueError("Please set APPTOPIA_CLIENT and APPTOPIA_SECRET environment variables.")
    
    country_code = args.country.upper() 
    store_name = args.store.lower()

    # Validate if the country code is in the supported list
    supported_country_codes = {c["code"] for c in countries}
    if country_code not in supported_country_codes:
        raise ValueError(f"Country code {country_code} is not supported.")

    token = get_auth_token(client, secret)
    categories = get_Store_categories(token, store_name)
    top_charts = get_all_top_apps(token, store_name, country_code, categories)
    filename = f'top_apps_{store_name}_{country_code}.csv'
    file_path = os.path.join("app_data", filename)
    print(f"saving to {filename}")
    create_rankings(token, store_name, country_code, top_charts, file_path)