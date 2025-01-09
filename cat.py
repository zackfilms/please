import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
from concurrent.futures import ThreadPoolExecutor

# Folder containing the CSV files
folder_path = '.'
output_file_path = os.path.join(output_folder, file_name)
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

def process_file(file_name):
    print(f"Processing file: {file_name}")
    file_path = os.path.join(folder_path, file_name)

    # Load the CSV file
    data = pd.read_csv(file_path)

    # Display first few rows to confirm the input
    print("Sample of the input file:")
    print(data.head())

    # Check for necessary columns
    if 'Link' not in data.columns or 'Business Name' not in data.columns:
        print(f"File {file_name} is missing required columns.")
        return

    # Process only the first 5 rows for testing
    sample_data = data.head(5).copy()

    # Add a 'Category' column by fetching categories from links
    sample_data['Category'] = sample_data['Link'].apply(get_category_from_link)

    # Save the processed data
    output_file_path = os.path.join(output_folder, file_name)
    sample_data.to_csv(output_file_path, index=False)
    print(f"Processed sample saved to: {output_file_path}")


# Process all files in the folder
csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
for file_name in csv_files:
    process_file(file_name)

print("All files processed.")
