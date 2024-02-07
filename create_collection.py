import csv
import os
import shutil
from collections import defaultdict
import pprint
import hashlib
import re

def sanitize_string_for_directory_name(input_string):
    # Use a regular expression to remove non-alphanumeric characters
    sanitized_string = re.sub(r'[^A-Za-z0-9]+', '', input_string)
    return sanitized_string

def hash_integer(app_id):
    # Convert the integer to a string and encode it to bytes
    integer_bytes = app_id.encode('utf-8')
    
    # Create a new SHA-256 hash object
    hasher = hashlib.sha256()
    
    # Update the hash object with the bytes of the integer
    hasher.update(integer_bytes)
    
    # Return the hexadecimal digest of the hash
    return hasher.hexdigest()

# Define the CSV file name
csv_file = 'app_data/top_apps.csv'

# Create the _companies directory if it doesn't exist
# If it exists, delete everything inside it
companies_dir = '_app_publishers'
if os.path.isdir(companies_dir):
    shutil.rmtree(companies_dir)
os.makedirs(companies_dir, exist_ok=True)

total_releases = 0
row_count = 0
category_data = defaultdict(lambda: {'total_releases': 0, 'row_count': 0, 'top_20_total_releases': 0, 'top_20_row_count': 0})

with open(csv_file, newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        try:
            # Get the release count and category for the current row
            release_count = int(row['release_count_in_past_year'])
            category = row['category']
            rank = row['rank']
            # Update the total releases and row count for this category
            category_data[category]['total_releases'] += release_count
            category_data[category]['row_count'] += 1

            if int(rank) < 21:
                category_data[category]['top_20_total_releases'] += release_count
                category_data[category]['top_20_row_count'] += 1 

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
        average_releases[category]['top'] = round(data['top_20_total_releases'] / data['top_20_row_count'])

    else:
        print(f"No valid rows found for category '{category}'.")

pp = pprint.PrettyPrinter(indent=4, width=1, depth=None, compact=False)
pp.pprint(average_releases)

# Read the CSV file and skip the header line
with open(csv_file, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    idx=0
    for row in reader:
        idx +=1
        # if idx > 1000:
        #     break
        
        unique_id=hash_integer(row['app_id'])
        filename = f"{unique_id}.md"
        subdir_name = sanitize_string_for_directory_name(row['publisher_name'])
        subdir = os.path.join(companies_dir, subdir_name)
        os.makedirs(subdir, exist_ok=True)
        filepath = os.path.join(subdir, filename)

        # Check if the markdown file already exists
        if not os.path.isfile(filepath):
            # Create the markdown file with the proper values
            with open(filepath, 'w', encoding='utf-8') as mdfile:
                mdfile.write(
                    f"---\n"
                    f"id: \"{unique_id}\"\n"
                    f"category: \"{row['category']}\"\n"
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
                    # f"days_between_release: {days_between_release}\n"
                    # f"days_between_release_category: {days_between_release_category}\n"
                    # f"days_between_release_category_top: {days_between_release_category_top}\n"
                    f"---\n"
                )
        else:
            print(f"File {filepath} already exists, skipping...")

print("Markdown files have been created in the _companies directory.")