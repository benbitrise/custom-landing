import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import date, datetime, timedelta
import json
import csv

API_BASE_URL = "https://integrations.apptopia.com/api"
USER_AGENT = "insomnia/8.6.0"
client = os.environ.get('APPTOPIA_CLIENT')
secret = os.environ.get('APPTOPIA_SECRET')

def requests_retry_session(retries=5, backoff_factor=1, status_forcelist=(429, 500, 502, 504), session=None):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(['GET', 'POST'])
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

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
    store_categories = requests_retry_session().get(url, headers=headers).json()
    id_map = {item['id']: item for item in store_categories}
    return id_map

def get_top_apps(token, store, category_id):
    url = f"{API_BASE_URL}/{store}/rank_lists"
    current_date = (date.today() - timedelta(days=7)).isoformat()
    params = {
        "id": category_id,
        "kind": "free",
        "country_iso": "US",
        "date": current_date
    }
    headers = {
        "User-Agent": USER_AGENT,
        "Authorization": token
    }
    response = requests_retry_session().get(url, headers=headers, params=params).json()
    return response

def get_root_category(store):
    if store == "itunes_connect":
        return 36
    if store == "google_play":
        return 98
    raise f"invalid store name {store}"

def get_all_top_apps(token, store, categories):
    root_category = get_root_category(store)
    return {
        key: {
            "apps": get_top_apps(token, store, key),
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
    response = requests_retry_session().get(url, headers=headers, params=params).json()
    try:
        details = response[0]
        keys_to_keep = {'screenshot_urls', 'publisher_id', 'publisher_name', 'icon_url', 'name', 'current_version', 'id'}
        return {k: details[k] for k in keys_to_keep}
    except (IndexError, KeyError) as e:
        print(f"Error fetching details for app_id {app_id}: {e}")
        print(response)
        return None

def create_rankings(token, store, top_charts, filename):
    fieldnames = ['category', 'rank', 'app_id', 'app_name', 'publisher_id', 'publisher_name', 'icon', 'screenshot', 'most_recent_release', 'release_count_in_past_year']
    with open(filename, 'w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for value in top_charts.values():
            for idx, app_id in enumerate(value['apps'][0]["app_ids"], start=1):
                app_details = get_app_details(token, store, app_id)
                version_info = get_releases(token, store, app_id) if app_details else None
                if app_details and version_info:
                    writer.writerow({
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
                    })

def get_releases(token, store, app_id):
    url = f"{API_BASE_URL}/{store}/app_versions"
    params = {"id": app_id, "country_iso": "US"}
    headers = {
        "User-Agent": USER_AGENT,
        "Authorization": token
    }
    response = requests_retry_session().get(url, headers=headers, params=params).json()
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

token = get_auth_token(client, secret)
categories = get_Store_categories(token, "itunes_connect")
top_charts = get_all_top_apps(token, "itunes_connect", categories)
create_rankings(token, "itunes_connect", top_charts, 'app_data/top_apps.csv')