import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
from concurrent.futures import ThreadPoolExecutor

# Folder containing the CSV files
folder_path = '.'
output_folder = '.'
os.makedirs(output_folder, exist_ok=True)

# Define a function to get the category from a link
def get_category_from_link(link):
    try:
        print(f"Fetching category from: {link}")
        # Make a GET request to the link
        response = requests.get(link, timeout=10)  # Set a timeout to avoid long waits
        response.raise_for_status()  # Raise HTTPError for bad responses

        # Parse the HTML content
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract the category using the selector
        category_element = soup.select_one('#contact-details > div.contact-details > div.media-object.clearfix.inside-gap-medium.image-on-right > div > h2 > a')

        if category_element:
            category = category_element.text.strip()
            print(f"Category found: {category}")
            return category
        else:
            print("Category not found.")
            return None
    except Exception as e:
        print(f"Error fetching category: {e}")
        return f"Error: {e}"

# Process a single file
def process_file(file_name):
    print(f"Processing file: {file_name}")
    file_path = os.path.join(folder_path, file_name)

    # Load the CSV file
    data = pd.read_csv(file_path)

    # Ensure the required columns exist
    if 'Link' not in data.columns or 'Business Name' not in data.columns:
        print(f"Skipping {file_name}: Required columns 'Link' or 'Business Name' are missing.")
        return

    # Extract categories using ThreadPoolExecutor for parallelism
    with ThreadPoolExecutor(max_workers=10) as executor:
        data['Category'] = list(executor.map(get_category_from_link, data['Link']))

    # Append the category next to the business name
    data['Business Name with Category'] = data['Business Name'] + ' - ' + data['Category'].fillna('No Category')

    # Save the updated DataFrame to a new CSV file
    output_file = os.path.join(output_folder, file_name)
    data.to_csv(output_file, index=False)

    print(f"Finished processing {file_name}. Output saved to {output_file}\n")

# Process all files in the folder
csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
for file_name in csv_files:
    process_file(file_name)

print("All files processed.")
