import csv
import os
import shutil
from collections import defaultdict
import pprint
import hashlib
import re
import base64

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

def list_file_info():
    subdir="app_data"
    for filename in os.listdir(subdir):
    # Check if the filename ends with ".csv"
        if filename.endswith(".csv"):
            # Split the filename into parts based on underscores
            parts = filename.split("_")

            # Extract the country code and store from the filename parts
            country_code = parts[-1].split('.')[0]
            store = f"{parts[-3]}_{parts[-2]}"
            yield (country_code, store, f"{subdir}/{filename}")        

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

def create_page(country, store, filename, average_releases, companies_dir):
    # Read the CSV file and skip the header line
    with open(filename, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        idx=0
        apps_and_releases={}
        for row in reader:
            idx +=1
            # if idx > 1000:
            #     break
            
            unique_id=hash(row['app_id'], store)
            filename = f"{unique_id}.md"
            subdir_name = sanitize_string_for_directory_name(row['publisher_name'])
            subdir = os.path.join(companies_dir, country_code, store, subdir_name)
            os.makedirs(subdir, exist_ok=True)
            filepath = os.path.join(subdir, filename)

            apps_and_releases[row['app_name']] = row['release_count_in_past_year'];

            # Check if the markdown file already exists
            if not os.path.isfile(filepath):
                # Create the markdown file with the proper values
                with open(filepath, 'w', encoding='utf-8') as mdfile:
                    mdfile.write(
                        f"---\n"
                        f"id: \"{unique_id}\"\n"
                        f"category: \"{row['category']}\"\n"
                        f"country: \"{country_code}\"\n"
                        f"store: \"{store}\"\n"
                        f"app_name: \"{row['app_name']}\"\n"
                        f"app_id: {row['app_id']}\n"
                        f"app_icon: {row['icon']}\n"
                        f"app_screenshot: {row['screenshot']}\n"
                        f"publisher_id: {row['publisher_id']}\n"
                        f"publisher_name: \"{row['publisher_name']}\"\n"
                        f"rank: {row['rank']}\n"
                        f"most_recent_release: {row['most_recent_release']}\n"
                        f"release_count_in_past_year: {row['release_count_in_past_year']}\n"
                        f"release_count_in_past_year_category: {average_releases[row['category']]['all']}\n"
                        f"release_count_in_past_year_top_in_category: {average_releases[row['category']]['top']}\n"
                        f"---\n"
                    )
            else:
                print(f"File {filepath} already exists, skipping...")

        top_10 = sorted(apps_and_releases.items(), key=lambda item: item[1], reverse=True)[:10]

if __name__ == "__main__":
    companies_dir = '_app_publishers'
    create_collections_dir(companies_dir)
    for (country_code, store, filename) in list_file_info():
        print(f"{country_code}, {store} from {filename}")
        averages = create_averages(country_code, store, filename)
        create_page(country_code, store, filename, averages, companies_dir)
        