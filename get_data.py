import requests
from datetime import datetime, date, timedelta
import json
import csv

def get_auth_token():
    url = "https://integrations.apptopia.com/api/login"

    querystring = {"":""}

    auth_payload = "-----011000010111000001101001\r\nContent-Disposition: form-data; name=\"client\"\r\n\r\ncCKfOVTx7UbHR\r\n-----011000010111000001101001\r\nContent-Disposition: form-data; name=\"secret\"\r\n\r\nL56tan1zDJ4lm1eB1UJU5m2FH\r\n-----011000010111000001101001--\r\n"
    auth_headers = {
        "Content-Type": "multipart/form-data; boundary=---011000010111000001101001",
        "User-Agent": "insomnia/8.6.0"
    }

    auth_response = requests.request("POST", url, data=auth_payload, headers=auth_headers, params=querystring).json()

    token = auth_response['token']
    return token

def get_iTunes_categories(token):
    url = "https://integrations.apptopia.com/api/itunes_connect/categories"

    headers = {
        "User-Agent": "insomnia/8.6.0",
        "Authorization": token
    }
    itunes_categories = requests.request("GET", url, headers=headers).json()
    id_map = {item['id']: item for item in itunes_categories}
    return id_map

def get_top_apps(token, category_id):
    url = "https://integrations.apptopia.com/api/itunes_connect/rank_lists"
    current_date = (date.today() - timedelta(days=7)).isoformat()
    querystring = {"id":category_id, "kind":"free","country_iso":"US","date":current_date}
    headers = {
        "User-Agent": "insomnia/8.6.0",
        "Authorization": token
    }
    response = requests.request("GET", url, headers=headers, params=querystring).json()
    return response

def get_all_top_apps(token, categories):
    top_charts = {}
    for key, value in categories.items():
        if value['parent_id'] == 36:
            top_apps=get_top_apps(token, key)
            top_charts[key] = {
                "apps": top_apps,
                "name": value["name"],
                "id": key
            }
    return top_charts

def save_top_to_file(top_charts):
    filename = 'app_data/top_charts.json'
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(top_charts, f, ensure_ascii=False, indent=4)

def get_app_details(token, app_id):
    url = "https://integrations.apptopia.com/api/itunes_connect/app"
    querystring = {"id":app_id}
    headers = {
        "User-Agent": "insomnia/8.6.0",
        "Authorization": token
    }
    try:
        response=requests.request("GET", url, headers=headers, params=querystring).json()
        details = response[0]
        keys_to_keep = {'screenshot_urls', 'publisher_id', 'publisher_name', 'icon_url', 'name', 'current_version', 'id'}
        smaller_dict = {k: details[k] for k in keys_to_keep}
        return smaller_dict
    except IndexError as e:
        print(f"caught error fetching : {e}")
        print(f"{response}")

def create_rankings(token, top_charts):
    filename = 'app_data/top_apps.csv'
    with open(filename, 'w', newline='') as csv_file:
        fieldnames = ['category', 'rank', 'app_id', 'app_name', 'publisher_id', 'publisher_name', 'icon', 'screenshot', 'most_recent_release', 'release_count_in_past_year']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for key, value in top_charts.items():
            idx=0
            for app_id in value['apps'][0]["app_ids"]:
                idx+=1
                if idx % 100 == 0:
                    print(f"{value['name']} - {idx}")
                app_details=get_app_details(token, app_id)
                version_info=get_releases(token, app_id)
                if app_details is not None:
                    try:
                        row = {
                            'category': value['name'],
                            'rank': idx,
                            'app_id': app_details['id'],
                            'app_name': app_details['name'],
                            'publisher_id': app_details['publisher_id'],
                            'publisher_name': app_details['publisher_name'],
                            'icon': app_details['icon_url'],
                            'screenshot': app_details['screenshot_urls'][0],
                            'most_recent_release': version_info[0],
                            'release_count_in_past_year': version_info[1]
                        }
                        writer.writerow(row)
                    except IndexError:
                        print("No screenshot for " + str(idx) + " app_id: {app_id}")
                    except TypeError:
                        print(f"No app versions for {app_id}")

def get_releases(token, app_id):
    url = "https://integrations.apptopia.com/api/itunes_connect/app_versions"
    querystring = {"id":app_id,"country_iso":"US"}
    headers = {
        "User-Agent": "insomnia/8.6.0",
        "Authorization": token
    }

    response = requests.request("GET", url, headers=headers, params=querystring).json()

    # Initialize variables
    most_recent_date = None
    versions_in_past_year = 0
    one_year_ago = datetime.now() - timedelta(days=365)

    # Iterate through the items in the JSON data
    for item in response:
        versions = item.get("versions", [])
        for version in versions:
            version_date = datetime.strptime(version["date"], "%Y-%m-%d")
            if most_recent_date is None or version_date > most_recent_date:
                most_recent_date = version_date
            if version_date > one_year_ago:
                versions_in_past_year += 1
    # Output the results
    if most_recent_date is None:
        print(f"No dates found for {app_id}")
        return None
    return (most_recent_date, versions_in_past_year)


token=get_auth_token()
cats=get_iTunes_categories(token)
top_charts=get_all_top_apps(token, cats)

# save_top_to_file(top_charts)
create_rankings(token, top_charts)



# don't add an app if the id is already found in the list



