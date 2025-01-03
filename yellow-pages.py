import pandas as pd
import requests
import csv
import re
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import aiohttp
import asyncio

# Function to extract all contact details with correct formatting
def extract_contact_details(soup):
    try:
        contact_container = soup.select_one('div.primary-contacts-container')
        if not contact_container:
            return 'No contact details found'

        contacts = []
        contact_methods = contact_container.select('div.contact, a.contact')

        for method in contact_methods:
            contact_type = method.get('title', 'Unknown').strip()
            contact_value = method.select_one('div.desktop-display-value')

            # Handle specific contact types
            if 'website' in contact_type.lower():
                website_link = method.get('href', '')
                if website_link:
                    contacts.append(f"Website: {website_link}")
            elif contact_value:
                contact_value_text = contact_value.get_text(strip=True)
                if contact_type.lower() == 'fax':
                    contacts.append(f"Fax: {contact_value_text}")
                elif 'email' in contact_type.lower():
                    email_address = method.get('title', '').split(' ')[-1]
                    if email_address:
                        contacts.append(f"Email: {email_address}")
                elif 'phone' in contact_type.lower():
                    contacts.append(f"Phone: {contact_value_text}")
                else:
                    contacts.append(f"{contact_value_text}: {contact_type}")

        # Fallback for directly nested <a> tags with website links
        website_links = contact_container.select('a.contact-url')
        for link in website_links:
            website_link = link.get('href', '').strip()
            if website_link:
                contacts.append(f"Website: {website_link}")

        return '; '.join(contacts) if contacts else 'No contact details found'
    except Exception as e:
        return f"Error occurred: {e}"

# Function to extract location
def extract_location(soup):
    try:
        location_tag = soup.select_one('div.listing-address.mappable-address')
        return location_tag.get_text(strip=True) if location_tag else 'No location found'
    except Exception as e:
        return f"Error occurred: {e}"

# Function to extract category
def extract_category(soup):
    try:
        category_tag = soup.select_one('h2.listing-heading a')
        return category_tag.get_text(strip=True) if category_tag else 'No category found'
    except Exception as e:
        return f"Error occurred: {e}"

def extract_about_and_products(soup):
    about_us = "No information available"
    products_services = "No information available"

    try:
        # Extract 'About Us' content
        about_us_section = soup.select_one("div.about-us-content")
        if about_us_section:
            # Get all text content from the section
            about_us = about_us_section.get_text(separator="\n", strip=True)

        # Extract 'Products and Services' content
        products_section = soup.select_one("div.products-and-services")
        if products_section:
            # Get all text content from the section
            products_services = products_section.get_text(separator="\n", strip=True)

    except Exception as e:
        print(f"Error extracting About Us or Products and Services: {e}")

    return about_us, products_services



# Asynchronous function to fetch a page
async def fetch_page_async(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.text()
            else:
                print(f"Failed to fetch {url}: Status code {response.status}")
                return None

def parse_business_data(soup):
    data = []
    business_cards = soup.select('div.Box__Div-sc-dws99b-0.fYIHHU > a > h3')

    for card in business_cards:
        name = card.get_text(strip=True) if card else None
        parent_link = card.find_parent('a', href=True)
        link = parent_link['href'] if parent_link else None

        if link:
            if not link.startswith(('http://', 'https://')):
                link = 'https://www.yellowpages.com.au' + link

            response = requests.get(link)
            if response.status_code == 200:
                detail_soup = BeautifulSoup(response.text, 'html.parser')
                contact_details = extract_contact_details(detail_soup)
                location = extract_location(detail_soup)
                category = extract_category(detail_soup)
                about_us, products_services = extract_about_and_products(detail_soup)

                # Create initial business entry
                business_entry = {
                    'Business Name': name,
                    'Link': link
                }

                # Apply contact details splitter
                contact_details_split = split_contact_details_dynamic({'Contact Details': contact_details})
                business_entry.update(contact_details_split.to_dict())

                # Apply location splitter
                location_split = split_location(location)
                business_entry.update({
                    'Street': location_split[0],
                    'Suburb': location_split[1],
                    'State': location_split[2],
                    'Postcode': location_split[3]
                })
                business_entry.update({
                    "Full Location": location
                })

                # Add remaining fields
                business_entry.update({
                    'Category': category,
                    'About Us': about_us,
                    'Products and Services': products_services
                })

                data.append(business_entry)

    return data

def split_contact_details_dynamic(row):
    details = row.get("Contact Details", "")
    contact_types = {}

    for detail in details.split(';'):
        detail = detail.strip()
        if ':' in detail:
            key, value = detail.split(':', 1)
            contact_types[key.strip()] = value.strip()

    return pd.Series(contact_types)

def split_location(location):
    # If no location is found, return all parts as 'N/A'
    if location == 'No location found' or pd.isna(location):
        return ['N/A', 'N/A', 'N/A', 'N/A']

    # Regular expression for handling full address (Street, Suburb, State, Postcode)
    location_pattern = r'^(.*?),\s*([A-Za-z\s]+)\s+([A-Za-z]{2,3})\s*(\d{4})$'
    match = re.match(location_pattern, location.strip())

    if match:
        # If full address is matched, return all parts
        street = match.group(1).strip()
        suburb = match.group(2).strip()
        state = match.group(3).strip()
        postcode = match.group(4).strip()
        return [street, suburb, state, postcode]

    # Handle case where only Suburb, State, and Postcode are provided (no Street)
    location_pattern_no_street = r'^[A-Za-z\s]+,\s*([A-Za-z\s]+)\s+([A-Za-z]{2,3})\s*(\d{4})$'
    match_no_street = re.match(location_pattern_no_street, location.strip())

    if match_no_street:
        suburb = match_no_street.group(1).strip()
        state = match_no_street.group(2).strip()
        postcode = match_no_street.group(3).strip()
        return ['N/A', suburb, state, postcode]

    # Handle case where only Suburb and State are present (missing Postcode)
    location_pattern_suburb_state = r'^[A-Za-z\s]+,\s*([A-Za-z\s]+)\s+([A-Za-z]{2,3})$'
    match_suburb_state = re.match(location_pattern_suburb_state, location.strip())

    if match_suburb_state:
        suburb = match_suburb_state.group(1).strip()
        state = match_suburb_state.group(2).strip()
        return ['N/A', suburb, state, 'N/A']

    # Handle case where only Suburb and Postcode are present (missing State)
    location_pattern_suburb_postcode = r'^[A-Za-z\s]+,\s*([A-Za-z\s]+)\s*(\d{4})$'
    match_suburb_postcode = re.match(location_pattern_suburb_postcode, location.strip())

    if match_suburb_postcode:
        suburb = match_suburb_postcode.group(1).strip()
        postcode = match_suburb_postcode.group(2).strip()
        return ['N/A', suburb, 'N/A', postcode]

    # If none of the patterns match, return the location as Street and N/A for others
    return ["N/A", 'N/A', 'N/A', 'N/A']

def fetch_page_soup(url):
    response = requests.get(url)
    if response.status_code == 200:
        return BeautifulSoup(response.text, 'html.parser')
    else:
        print(f"Failed to fetch {url}: Status code {response.status_code}")
        return None

# Fetch and parse pages asynchronously
async def scrape_pages_async(base_url):
    all_data = []
    current_url = base_url
    page = 1  # Start at page 1

    while True:
        print(f"Scraping page {page}")
        soup = fetch_page_soup(current_url)
        if soup:
            # Scrape the current page
            all_data.extend(parse_business_data(soup))

            # Look for the div with display="flex" that contains the "Next" button
            next_button_div = soup.find('div', {'display': 'flex'})

            if next_button_div:
                # Within that div, look for the "Next" button with the text "Next"
                next_button = next_button_div.find('span', class_='MuiButton-label', string="Next")

                if next_button:
                    # Find the parent 'a' tag with the href attribute for the next page
                    next_page_tag = next_button.find_parent('a', href=True)
                    if next_page_tag:
                        # Update the current URL to the next page
                        current_url = 'https://www.yellowpages.com.au' + next_page_tag['href']
                        page += 1  # Increment page counter
                        print(f"Moving to page {page}")
                    else:
                        print("Error: Next page URL is missing.")
                        break  # Exit if the "Next" button link is not found
                else:
                    print("No 'Next' button found within the flex container.")
                    break  # Exit if no "Next" button is found inside the flex container
            else:
                print("No 'flex' container found with the 'Next' button.")
                break  # Exit if no flex container is found

        else:
            print("Failed to fetch the page.")
            break  # Stop if the page cannot be fetched

    return all_data

# Wrapper for asynchronous execution
def scrape_urls_async(urls):
    loop = asyncio.get_event_loop()
    tasks = [scrape_pages_async(url) for url in urls]
    results = loop.run_until_complete(asyncio.gather(*tasks))
    return [item for sublist in results for item in sublist]


def sanitize_filename(value):
    return re.sub(r'[^\w\s-]', '', value).replace(' ', '_')

def save_to_csv(data, industry, job_title, sheet_name, location, file_name='output.csv'):
    try:
        
        for row in data:
            row['Industry'] = industry
            row['Job Title'] = job_title

        df = pd.DataFrame(data)

        if 'Website' in df.columns:
            df['Website'] = df['Website'].apply(lambda x: re.sub(r'\s*\(.*\)\s*', '', str(x)))

        column_order = [
            'Industry', 'Job Title', 'Business Name', 'Link', 'Phone', 'Email', 'Website',
            'Street', 'Suburb', 'State', 'Postcode', 'Full Location',
            'About Us', 'Products and Services'
        ]

        for col in column_order:
            if col not in df.columns:
                df[col] = None

        df = df[column_order]

        # Regenerate file_name after sanitization
        file_name = f"{industry}+{job_title}+{file_name.split('+')[-2]}+{file_name.split('+')[-1]}"

        df.to_csv(file_name, index=False, encoding='utf-8')
        print(f"Data saved to {file_name}")
    except Exception as e:
        print(f"Error saving to CSV: {e}")

# Process the specific sheet 'Retail & Services for part jobs'
def process_sheet2(file_path, column_name='Yellow Pages', sheet_name='Hospitality'):
    print(f"Processing sheet: {sheet_name} from {file_path}")

    try:
        # Read only the second sheet
        data = pd.read_excel(file_path, sheet_name=sheet_name)
    except Exception as e:
        print(f"Failed to read sheet '{sheet_name}' from file '{file_path}': {e}")
        return

    # Ensure the required column exists
    if column_name not in data.columns:
        print(f"Column '{column_name}' not found in sheet '{sheet_name}'.")
        return

    # Extract relevant columns
    urls = data[column_name].dropna().tolist()[499:525]
    industry = data['Industry'].tolist()[499:525]
    job_title = data['Job Title'].tolist()[499:525]

    # Process each URL
    for idx, (url, industry, job_title) in enumerate(zip(urls, industry, job_title), start=500):
        print(f"Processing URL {idx}/{len(urls)}: {url}")

        clue_match = re.search(r'clue=([^&]*)', url)
        location_match = re.search(r'locationClue=([^&]*)', url)
        clue = clue_match.group(1).replace('+', ' ') if clue_match else "Unknown_Clue"
        location = location_match.group(1).replace('+', ' ') if location_match else "Unknown_Location"

        sanitized_clue = re.sub(r'[^\w\s-]', '', clue).replace(' ', '_')
        sanitized_location = re.sub(r'[^\w\s-]', '', location).replace(' ', '_')[:30]

        output_file = f"{industry}+{job_title}+{sanitized_clue}+{sanitized_location}.csv"

        # Scrape the URL and save results
        data = scrape_urls_async([url])
        save_to_csv(data, industry, job_title, sheet_name, location, file_name=output_file)

# Example usage
excel_file = "Yellow Pages Phase 1 Links.xlsx"
url_column = "Yellow Pages"
process_sheet2(excel_file, url_column)
