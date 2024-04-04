import csv
import argparse
import os
import shutil
from collections import defaultdict
import yaml
import hashlib
import re
import base64
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

reps=[
    {
        "full_name": "Lionel Lejeune",
        "linkedin": "https://www.linkedin.com/in/lionelbitrise/",
        "phone": "0044 73 918 00286",
        "email": "lionel.lejeune@bitrise.io",
        "title": "Account Manager",
        "first_name": "Lio",
        "photo": "lio.jpg",
        "ref": "lile",
        "languages": ['en', 'fr'],
        "countries": ['FR', 'AT', 'DE', 'IT', 'BE', 'ES', 'PT', 'ID', 'SG', 'QA', 'AE', 'JO', 'IL', 'PL', 'TR']
    },
    {
        "full_name": "Joe Cillis",
        "linkedin": "https://www.linkedin.com/in/joecillis",
        "phone": "+1 518-258-1902",
        "email": "joseph.cillis@bitrise.io",
        "title": "Account Manager",
        "first_name": "Joe",
        "photo": "joe.jpg",
        "ref": "joci",
        "languages": ['en'],
        "countries": ['US', 'CA', 'MX', 'BR']
    },
    {
        "full_name": "Michael Roguly",
        "linkedin": "https://www.linkedin.com/in/michael-roguly-77376710",
        "phone": "+1 949-233-3404",
        "email": "michael.roguly@bitrise.io",
        "title": "Account Manager",
        "first_name": "Michael",
        "photo": "michael.jpg",
        "ref": "miro",
        "languages": ['en'],
        "countries": ['US', 'CA', 'MX', 'BR']
    },
    {
        "full_name": "Anna Magnussen",
        "linkedin": "https://uk.linkedin.com/in/anna-magnussen-0977131b",
        "phone": "+44 772 2064781",
        "email": "anna.magnussen@bitrise.io",
        "title": "Account Manager",
        "first_name": "Anna",
        "photo": "anna.jpg",
        "ref": "anma",
        "languages": ['en'],
        "countries": ['GB', 'NL', 'SE', 'NO', 'DK', 'IN', 'JP', 'AU', 'NZ']
    },
    {
        "full_name": "Gonzalo Gomez-Ilera",
        "linkedin": "https://uk.linkedin.com/in/anna-magnussen-0977131b",
        "phone": "+353 838374524",
        "email": "gonzalo.gomez-llera@bitrise.io",
        "title": "Business Development Rep",
        "first_name": "Gonzalo",
        "photo": "gonzalo.jpg",
        "ref": "gogo",
        "languages": ['en', 'es'],
        "countries": ['GB', 'NL', 'SE', 'NO', 'DK', 'IN', 'JP', 'AU', 'NZ', 'FR', 'AT', 'DE', 'IT', 'BE', 'ES', 'PT', 'ID', 'SG', 'QA', 'AE', 'JO', 'IL', 'PL', 'TR']
    },
]

def sanitize_string_for_directory_name(input_string):
    # Use a regular expression to remove non-alphanumeric characters
    sanitized_string = re.sub(r'[^A-Za-z0-9]+', '', input_string)
    return sanitized_string

def hash(app_id, store):
    # Convert the integer to a string and encode it to bytes
    integer_bytes = f"{app_id}{store}".encode('utf-8')
    
    # Create a new SHA-256 hash object
    hasher = hashlib.sha256()
    
    # Update the hash object with the bytes of the integer
    hasher.update(integer_bytes)
    encoded_hash = base64.urlsafe_b64encode(hasher.digest()).decode('utf-8')
    return encoded_hash[:12]

def list_file_info(desired_country_code):
    subdir="app_data"
    for filename in os.listdir(subdir):
    # Check if the filename ends with ".csv"
        if filename.endswith(".csv"):
            # Split the filename into parts based on underscores
            parts = filename.split("_")

            # Extract the country code and store from the filename parts
            country_code = parts[-1].split('.')[0]
            if country_code == desired_country_code:
                store = f"{parts[-3]}_{parts[-2]}"
                yield (store, f"{subdir}/{filename}")        

def create_collections_dir(companies_dir):
    # Create the _companies directory if it doesn't exist
    # If it exists, delete everything inside it
    if os.path.isdir(companies_dir):
        shutil.rmtree(companies_dir)
    os.makedirs(companies_dir, exist_ok=True)

def create_averages(country, store, filename):
    category_data = defaultdict(lambda: {'total_releases': 0, 'row_count': 0, 'top_10_total_releases': 0, 'top_10_row_count': 0})

    with open(filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                # Get the release count and category for the current row
                release_count = int(row['release_count_in_past_year'])
                category = row['category']
                
                category_data[category]['total_releases'] += release_count
                category_data[category]['row_count'] += 1
                if int(row['rank']) < 11:
                    category_data[category]['top_10_total_releases'] += release_count
                    category_data[category]['top_10_row_count'] += 1 

            except ValueError:
                # Handle the case where the value is not a number
                print(f"Warning: Could not convert '{row['release_count_in_past_year']}' to an integer.")

    average_releases = {}
    # Calculate and print the average release count for each category
    for category, data in category_data.items():
        if average_releases.get(category, None) is None:
            average_releases[category] = {'all':{}, 'top':{}}

        if data['row_count'] > 0:
            average_releases[category]['all'] = round(data['total_releases'] / data['row_count'])
            average_releases[category]['top'] = round(data['top_10_total_releases'] / data['top_10_row_count'])
        else:
            print(f"No valid rows found for category '{category}'.")

    return average_releases

def create_page(rep, country_code, store, filename, average_releases, companies_dir):
    # Read the CSV file and skip the header line
    with open(filename, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        idx=0
        apps_and_releases={}
        for row in reader:
            idx +=1
            # if idx > 1000:
            #     break
            for language in rep['languages']:
                unique_id=hash(row['app_id'], store)
                filename = f"{unique_id}.md"
                subdir_name = sanitize_string_for_directory_name(row['publisher_name'])
                subdir = os.path.join(companies_dir, language, rep['ref'], store, subdir_name)
                os.makedirs(subdir, exist_ok=True)
                filepath = os.path.join(subdir, filename)

                apps_and_releases[row['app_name']] = row['release_count_in_past_year'];

                # Check if the markdown file already exists
                if not os.path.isfile(filepath):
                    data = {
                        'id': unique_id,
                        'category': row['category'],
                        'country': country_code,
                        'store': store,
                        'app_name': row['app_name'],
                        'app_id': row['app_id'],
                        'app_icon': row['icon'],
                        'app_screenshot': row['screenshot'],
                        'publisher_id': row['publisher_id'],
                        'publisher_name': row['publisher_name'],
                        'rank': row['rank'],
                        'most_recent_release': row['most_recent_release'],
                        'release_count_in_past_year': row['release_count_in_past_year'],
                        'release_count_in_past_year_category': average_releases[row['category']]['all'],
                        'release_count_in_past_year_top_in_category': average_releases[row['category']]['top'],
                        'rep_full_name': rep['full_name'],
                        'rep_linkedin': rep['linkedin'],
                        'rep_phone': rep['phone'],
                        'rep_email': rep['email'],
                        'rep_title': rep['title'],
                        'rep_first_name': rep['first_name'],
                        'rep_photo': rep['photo'],
                        'language': language,
                    }

                    with open(filepath, 'w', encoding='utf-8') as mdfile:
                        mdfile.write('---\n')
                        yaml.dump(data, mdfile, default_flow_style=False, allow_unicode=True)
                        mdfile.write('---\n')

def worker_task(rep, country_code, store, filename, averages, companies_dir):
    logging.info(f"START: {rep['first_name']}, {country_code}, {store} from {filename}")
    create_page(rep, country_code, store, filename, averages, companies_dir)
    logging.info(f"FINISH: {rep['first_name']}, {country_code}, {store} from {filename}")


def main(country_code):
    companies_dir = '_apps'
    create_collections_dir(companies_dir)
    
    # Create a list to hold the futures
    future_tasks = []

    # Use ThreadPoolExecutor to handle file I/O-bound tasks
    with ThreadPoolExecutor(max_workers=6) as executor:  # Adjust the number of workers as needed
        for (store, filename) in list_file_info(country_code):
            averages = create_averages(country_code, store, filename)
            
            # Submit tasks to the executor
            for rep in reps:
                if country_code in rep['countries']:
                    future = executor.submit(worker_task, rep, country_code, store, filename, averages, companies_dir)
                    future_tasks.append(future)

        # Wait for all tasks to complete
        for future in as_completed(future_tasks):
            try:
                future.result()  # Retrieve the result to handle exceptions
            except Exception as exc:
                print(f'Generated an exception: {exc}')


parser = argparse.ArgumentParser(description='create front matter for a country')
parser.add_argument('--country', type=str, required=True, help='ISO country code')
args = parser.parse_args()

if __name__ == "__main__":
    country_code = args.country.upper() 
    main(country_code)
