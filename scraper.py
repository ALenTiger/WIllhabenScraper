import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import re
import json
import os
from urllib.parse import urlencode
import csv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime
import hashlib
from statistics import mean, quantiles
from statistics import median
from pathlib import Path

#listings_data=[]
scrape_first_page_only = False

def testConnectToAtlas():
    uri = "mongodb+srv://banana111x:abalbaba@cluster0.ereatrt.mongodb.net/?retryWrites=true&w=majority"
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    db = client.sample_analytics
    accounts_collection = db.accounts

    # Query for a document with account_id 371138
    account_id = 371138
    document = accounts_collection.find_one({"account_id": account_id})

    # Check if the document was found and print it
    if document:
        print("Document found:", document)
    else:
        print(f"No document found with account_id {account_id}")

    new_document = {
    "account_id": 123456,  # Replace with the desired account_id
    "details": {
        # Add other fields as required
        "field1": "value1",
        "field2": "value2"
        }
    }
    
    insertion_result = accounts_collection.insert_one(new_document)

    print(f"New document inserted with _id: {insertion_result.inserted_id}")

    # Close the connection
    client.close()


def printListings(listings):
    for index, listing in enumerate(listings, start=1):
            print(f"Name: {listing['name']}, Price: {listing['price']}")

def writreListingsToCSV(listings, path, file_name):
    csv_file_path = os.path.join(path, file_name)
    with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=['date_time_posted', 'date_time_mesured_removed', 'up_time_hours', 'time_to_scrape_minutes', 'listing_is_up', 'name', 'price', 'price_string', 'adress', 'link', 'listing_hash_id'])
        writer.writeheader()  # Write the header row                                                                                                            
        for listing in listings:
            writer.writerow(listing)  # Write each'date_time_posted':date_time_posted, 'date_time_mesured_removed':None, 'listing_is_up': True, 'name': name, 'price': price, 'adress':address, 'listing_hash_id': listing_hash_idlisting as a row in the CSV
    
    print(f"Valid listings have been written to {csv_file_path}")

def load_listings_from_csv(csv_file_path):
    valid_listings = []
    try:
        with open(csv_file_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Parse the dates
                date_time_posted = datetime.strptime(row['date_time_posted'], '%Y-%m-%d %H:%M:%S') if row['date_time_posted'] else None
                date_time_mesured_removed = datetime.strptime(row['date_time_mesured_removed'], '%Y-%m-%d %H:%M:%S') if row['date_time_mesured_removed'] else None
                
                # Calculate 'up_time' if date_time_mesured_removed is available
                up_time = None
                if date_time_posted and date_time_mesured_removed:
                    up_time = (date_time_mesured_removed - date_time_posted).total_seconds() / 3600  # Total uptime in seconds

                listing = {
                    'date_time_posted': date_time_posted,
                    'date_time_mesured_removed': date_time_mesured_removed,
                    'up_time_hours': up_time,
                    'time_to_scrape_minutes': row['time_to_scrape_minutes'],
                    'listing_is_up': row['listing_is_up'] == 'True',
                    'name': row['name'],
                    'price': row['price'],
                    'price_string': row['price_string'],
                    'adress': row['adress'],
                    'link': row['link'],
                    'listing_hash_id': row['listing_hash_id']
                }
                valid_listings.append(listing)
        return valid_listings
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return []

def parse_price(price_str):
    """
    Parses the price string and converts it to a float.
    It removes the currency symbol, replaces European decimal commas with a dot,
    and removes thousands separators.
    """
    try:
        # Remove currency symbol, replace commas with dots, remove thousands separator, and strip whitespaces
        price_str = price_str.replace('€ ', '').replace('.', '').replace(',', '.').strip()
        return float(price_str)
    except ValueError as e:
        print(f"Error converting price to float: {e}")
        return None

def parse_willhaben_date(date_time_str):
    # Get the current date and year
    now = datetime.now()
    current_year = now.year
    current_date = now.date()

    if "Heute" in date_time_str:
        # Parse time for today's date
        time_str = date_time_str.split(", ")[1].split(" Uhr")[0]
        return datetime.strptime(f"{now.strftime('%d.%m.')}{current_year} {time_str}", "%d.%m.%Y %H:%M")
    else:
        # Parse date and time, assume current year initially
        date_str, time_str = date_time_str.split(" - ")
        time_str = time_str.split(" Uhr")[0]
        date_time_posted = datetime.strptime(f"{date_str}{current_year} {time_str}", "%d.%m.%Y %H:%M")

        # Adjust the year if the date is later than today (meaning it's from last year)
        if date_time_posted.date() > current_date:
            date_time_posted = date_time_posted.replace(year=current_year - 1)

        return date_time_posted
    
def generate_sha_hash(*args):
    """Generate a SHA-256 hash of the given arguments."""
    hash_input = ''.join(str(arg) for arg in args)
    return hashlib.sha256(hash_input.encode()).hexdigest()

def createMongoDBAndCollections():

    uri = "mongodb+srv://banana111x:abalbaba@cluster0.ereatrt.mongodb.net/?retryWrites=true&w=majority"
    
    # Connect to MongoDB
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Get the list of existing databases
    existing_dbs = client.list_database_names()

    # Database name based on scrape parameters
    db_name = f"{scrape_parameters['mongo_data_base_name_prefix']}_listings"

    # Check if the database already exists
    if db_name in existing_dbs:
        print(f"Database '{db_name}' already exists.")
        return

    print(f"creating Database '{db_name}'.")
    # Access the database
    db_name = f"{scrape_parameters['mongo_data_base_name_prefix']}_listings"
    db = client[db_name]

    # Create normal collections
    last_up_collection_name = f"{scrape_parameters['mongo_collection_name_prefix']}_last_up"
    db.create_collection(last_up_collection_name)

    todays_performance_collection_name = f"{scrape_parameters['mongo_collection_name_prefix']}_todays_performance"
    db.create_collection(todays_performance_collection_name)

    # Create time series collections
    previously_up_collection_name = f"{scrape_parameters['mongo_collection_name_prefix']}_previously_up"
    db.create_collection(previously_up_collection_name, timeseries={'timeField': 'date_time_posted', 'granularity': 'minutes'})

    previous_performances_collection_name = f"{scrape_parameters['mongo_collection_name_prefix']}_previous_performances"
    db.create_collection(previous_performances_collection_name, timeseries={'timeField': 'date_time_last_modified', 'granularity': 'hours'})

    invalid_listings_collection_name = f"{scrape_parameters['mongo_collection_name_prefix']}_zinvalid_listings"
    db.create_collection(invalid_listings_collection_name, timeseries={'timeField': 'date_time_posted', 'granularity': 'minutes'})

    client.close()

    print(f"Database and collections created for {db_name}")


# def setScrapeParameters(file_path, global_file_path):
#     global scrape_parameters
#     global scrape_global_parameters

#     with open(file_path, 'r', encoding='utf-8') as f:
#             scrape_parameters = json.load(f)

#     with open(global_file_path, 'r', encoding='utf-8') as f:
#             scrape_global_parameters = json.load(f)

def setScrapeParameters(file_path, global_file_path):
    global scrape_parameters
    global scrape_global_parameters

    # Function to convert all strings in a list to lowercase
    def lowercase_list(lst):
        return [item.lower() for item in lst]

    with open(file_path, 'r', encoding='utf-8') as f:
        scrape_parameters = json.load(f)
        # Iterate through all keys and convert lists to lowercase
        for key, value in scrape_parameters.items():
            if isinstance(value, list):
                scrape_parameters[key] = lowercase_list(value)

    with open(global_file_path, 'r', encoding='utf-8') as f:
        scrape_global_parameters = json.load(f)
        # Iterate through all keys and convert lists to lowercase
        for key, value in scrape_global_parameters.items():
            if isinstance(value, list):
                scrape_global_parameters[key] = lowercase_list(value)


async def scrapePage(page, base_url, params):
    print("Starting scraping process...")
    listings_data = []
    page_number = 1  # Start with the first page

    while True:
        print(f"Processing page {page_number}...")
        params['page'] = str(page_number)
        await page.goto(f"{base_url}?{urlencode(params)}")
        print(f"Loading page {page_number}")

        # Scroll to the bottom of the page
        last_position = 0
        while True:
            new_position = await page.evaluate('''() => {
                window.scrollBy(0, window.innerHeight);
                return window.scrollY;
            }''')
            if new_position == last_position:
                break
            last_position = new_position
            await asyncio.sleep(0.5)  # Add a delay to allow new content to load
        print("testeroni")
        # Get the final HTML
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        # Step 1: Locate the main div by its id
        main_div = soup.find('div', {'id': 'skip-to-resultlist'})

        # Step 2: Find all the listing divs within the main div
        listing_top_parents = main_div.find_all('div', {'class': 'Box-sc-wfmb7k-0 fEgjWL'})
        #listing_top_parents = main_div.find_all('div', {'class': 'Box-sc-wfmb7k-0 fEgjWL'})


        # If no listings are found, break the loop
        if len(listing_top_parents) == 0:
            print(f"No more listings found on page {page_number}. Ending scrape.")
            break

        # Step 3: Iterate through each listing div to extract the name and price
        
        for listing_div in listing_top_parents:
            # 3.1: Find the h3 tag and extract the name

            name_tag = listing_div.find('h3', {'class': 'Text-sc-10o2fdq-0 eDxJxB'})
            name = name_tag.text

            link_tag = listing_div.find('a', {'class': 'sc-ca51e2d8-0 bQrbGX sc-deecb898-1 bhBHuF'})
            link = "https://www.willhaben.at/" + link_tag.get('href')

            # 3.2: Find the span tag and extract the price
            price_tag = listing_div.find('span', {'class': 'Text-sc-10o2fdq-0 gKjlGm'})
            price_string = price_tag.text
            price = parse_price(price_string)

            date_time_tag = listing_div.find('p', {'class': 'Text-sc-10o2fdq-0 esACfE'})
            if date_time_tag is not None:
                date_time_str = date_time_tag.text
                date_time_posted = parse_willhaben_date(date_time_str)
            else:
                date_time_tag = listing_div.find('p', {'class': 'Text-sc-10o2fdq-0 ilFFJT'})
                date_time_str = date_time_tag.text
                date_time_posted = parse_willhaben_date(date_time_str)

            address_tag = listing_div.find('span', {'class': 'Text-sc-10o2fdq-0 kmXElp'})
            address = address_tag.text

            listing_hash_id = generate_sha_hash(name, price, date_time_posted, address)
            now = datetime.now()
            time_to_scrape_minutes = (now - date_time_posted).total_seconds() / 60

            # Append the name and price to the listings_data array
            listings_data.append({'date_time_posted':date_time_posted, 'date_time_mesured_removed':None, 'up_time_hours':None, 'time_to_scrape_minutes':time_to_scrape_minutes, 'listing_is_up': True, 'name': name, 'price': price, 'price_string': price_string, 'adress':address, 'link': link, 'listing_hash_id': listing_hash_id})

        page_number += 1
        print(f"Page {page_number} processed. Moving to the next page...")
    print("Scraping process completed.")
    return listings_data

async def run(json_parameters_file_path, json_global_parameters_path,  save_to_mongo = True, save_to_csv = False, check_create_db=True, ignore_white_space_for_validity=False):
    
    setScrapeParameters(json_parameters_file_path, json_global_parameters_path)

    listings_data = []
    page_number = 1  # Start with the first page
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        #await page.goto('https://www.willhaben.at/iad/kaufen-und-verkaufen/marktplatz?sfId=bc5a773b-e3bd-4af3-b4a6-fe1065266b64&isNavigation=true&isISRL=true&keyword=gtx%201060')
        base_url = 'https://www.willhaben.at/iad/kaufen-und-verkaufen/marktplatz'
        params = {
            'sfId': 'bc5a773b-e3bd-4af3-b4a6-fe1065266b64',
            'isNavigation': 'true',
            'isISRL': 'true',
            'keyword': scrape_parameters["URL_keyword"],
            'rows': '10000',
            'page': '1'
        }
        listings_data = await scrapePage(page, base_url, params)

        base_defekt_url = 'https://www.willhaben.at/iad/kaufen-und-verkaufen/marktplatz/a/zustand-defekt-24'
        defekt_params = {
            'sfId': '51e3d0a5-b0ed-4e8d-a1f9-3576ee6414db',
            'isNavigation': 'true',
            'keyword': scrape_parameters["URL_keyword"],
            'rows': '30',
            'page': '1',
            'zustand': 'defekt'
        }

        defekt_listings_data = await scrapePage(page, base_defekt_url, defekt_params)
 
        await browser.close()
        
        # Create a set of listing_hash_id from defekt_listings_data
        defekt_hash_ids = {listing['listing_hash_id'] for listing in defekt_listings_data}

        # Filter listings_data to exclude those listings that are in defekt_listings_data
        filtered_listings_data = [listing for listing in listings_data if listing['listing_hash_id'] not in defekt_hash_ids]
        listings_data = filtered_listings_data

        # Print the listings_data array to verify the extracted data
        for index, listing in enumerate(listings_data, start=1):
            print(f"Listing {index}: Name: {listing['name']}, Price: {listing['price']}")


    def insert_optional_spaces(word):
        """Insert optional spaces between each character in the word."""
        return r'\s*'.join(map(re.escape, word))

    def compile_invalid_words_pattern(words_list, ignore_spaces):
        """Compile a regular expression pattern from the list of words, with optional spaces if required."""
        if ignore_spaces:
            pattern = '|'.join(insert_optional_spaces(word) for word in words_list)
        else:
            pattern = '|'.join(map(re.escape, words_list))
        return re.compile(pattern, re.I)
    
    def compile_combo_words_patterns(words_list, ignore_spaces):
        """Compile regular expression patterns for each word in the list, with optional spaces if required."""
        patterns = []
        for word in words_list:
            if ignore_spaces:
                pattern = insert_optional_spaces(word)
            else:
                pattern = re.escape(word)
            patterns.append(re.compile(pattern, re.I))
        return patterns

    invalid_words_combined = scrape_parameters['invalid_product_words'] + \
                         scrape_parameters['invalid_other_listing_words'] + \
                         scrape_parameters['invalid_general_terms'] + \
                         scrape_global_parameters['invalid_product_words'] + \
                         scrape_global_parameters['invalid_general_terms']

    #invalid_words_pattern = compile_invalid_words_pattern(invalid_words_combined, ignore_white_space_for_validity)
    invalid_words_pattern = re.compile(rf"\b(?:{'|'.join(invalid_words_combined)})\b", re.I)

    # and so on for other patterns
    #invalid_words = re.compile(rf"\b(?:{'|'.join(scrape_parameters['invalid_words'])})\b", re.I)
    valid_words_pattern = re.compile(rf"\b(?:{'|'.join(scrape_parameters['valid_words'])})\b", re.I)
    possibly_valid_words_pattern = re.compile(rf"\b(?:{'|'.join(scrape_parameters['possibly_valid_words'])})\b", re.I)

    combo_words_patterns = compile_combo_words_patterns(scrape_parameters['must_have_combo_words'], ignore_white_space_for_validity)


    # Define the arrays to store the categorized listings
    valid_listings = []
    invalid_listings = []
    possibly_valid_listings = []
    remaining_listings = []

    #Define the regex patterns
    #invalid_words = re.compile(r'\b(?:pc|laptop|notebook|ultrabook|computer|3gb|3 gb|amd|radeon|ryzen)\b', re.I)
    #valid_words = re.compile(r'\b1060\b', re.I)
    #possibly_valid_words = re.compile(r'\b(?:gtx|6gb|grafikkarte|gpu)\b', re.I)
    
    # Iterate through the listings_data array
    for listing in listings_data:
        name = listing['name'].lower()  # Decapitalize the name for checking

        if(ignore_white_space_for_validity):
            if any(invalid_word.lower() in name for invalid_word in invalid_words_combined):
                invalid_listings.append(listing)
                continue
        else:
            if invalid_words_pattern.search(name):
                matches = invalid_words_pattern.findall(name)
                listing['name'] += "   REASON REMOVED:"
                for match in matches:
                    listing['name'] += ", " +match
                invalid_listings.append(listing)
                continue

        # Check for valid words
        valid_word_found = valid_words_pattern.search(name)
        #all_combo_words_present = all(word.lower() in name.lower() for word in scrape_parameters['must_have_combo_words'])
        all_combo_words_present = all(pattern.search(name) for pattern in combo_words_patterns)
        len_= len(scrape_parameters['must_have_combo_words'])
        if(len_>0):
            if(all_combo_words_present):
                valid_listings.append(listing)
                continue
        else: 
            if(valid_word_found):
                valid_listings.append(listing)
                continue

        # Check for possibly valid words
        if possibly_valid_words_pattern.search(name):
            possibly_valid_listings.append(listing)
        else:
            # If none of the above conditions are met, add to remaining_listings
            remaining_listings.append(listing)


    # Print the categorized listings to verify
    print(f"Valid listings: {len(valid_listings)}")
    print(f"Invalid listings: {len(invalid_listings)}")
    print(f"Possibly valid listings: {len(possibly_valid_listings)}")
    print(f"Remaining listings: {len(remaining_listings)}")


    print("\nVALID LISTINGS\n")
    print(f"Valid listings: {len(valid_listings)}")
    printListings(valid_listings)

    print("\nINVALID LISTINGS\n")
    print(f"Invalid listings: {len(invalid_listings)}")
    printListings(invalid_listings)
    
    print("\nPOSSIBLE LISTINGS\n")
    print(f"Possibly valid listings: {len(possibly_valid_listings)}")
    printListings(possibly_valid_listings)
    
    print("\nREMAINING\n") 
    print(f"Remaining listings: {len(remaining_listings)}")
    printListings(remaining_listings)
    print("\n\n")

    print(f"Valid listings: {len(valid_listings)}")
    print(f"Invalid listings: {len(invalid_listings)}")
    print(f"Possibly valid listings: {len(possibly_valid_listings)}")
    print(f"Remaining listings: {len(remaining_listings)}")

    # After the valid_listings array is populated, write it to a CSV file
    if(check_create_db): createMongoDBAndCollections()
    if(save_to_mongo): syncDatabaseWithCurrentListings(valid_listings, invalid_listings)
    #else: writreListingsToCSV(valid_listings)
    
    
    # Directory path
    dir_path = Path('last_runs_csv')

    # Create the directory
    dir_path.mkdir(parents=True, exist_ok=True)
    if(save_to_csv):
        prefix = scrape_parameters["mongo_data_base_name_prefix"]
        csv_file_path = prefix+'_valid_listings.csv'  # Define the CSV file name
        writreListingsToCSV(valid_listings, dir_path, csv_file_path)
        csv_file_path = prefix+'_invalid_listings.csv'  # Define the CSV file name
        writreListingsToCSV(invalid_listings, dir_path, csv_file_path)
        csv_file_path = prefix+'_possibly_valid_listings.csv'  # Define the CSV file name
        writreListingsToCSV(possibly_valid_listings, dir_path, csv_file_path)
    # # Povezovalni niz (zamenjajte <password> z vašim geslom in prilagodite preostanek niza)
    # connection_string = "mongodb+srv://banana111x:<abalbaba>@cluster0.ereatrt.mongodb.net/?retryWrites=true&w=majority"

    # # Vzpostavitev povezave
    # client = MongoClient(connection_string, server_api=ServerApi('1'))
    # db = client["ime_baze_podatkov"]
    # collection = db["ime_zbirke"]

    # try:
    #     client.admin.command('ping')
    #     print("Pinged your deployment. You successfully connected to MongoDB!")
    # except Exception as e:
    #     print(e)


def add_invalid_listings_to_mongodb(db, invalid_listings_data):
    #db_prefix = scrape_parameters["mongo_data_base_name_prefix"]
    coll_prefix = scrape_parameters["mongo_collection_name_prefix"]
    # MongoDB Atlas connection URI
    #uri = "mongodb+srv://banana111x:abalbaba@cluster0.ereatrt.mongodb.net/?retryWrites=true&w=majority"
    
    # Create a MongoClient
    #client = MongoClient(uri, server_api=ServerApi('1'))

    # Access the 'Willhaben_Listings' database and 'GTX_1060_6GB_current_listings' collection
    #db = client[db_prefix + "_listings"]
    collection = db[coll_prefix+"_zinvalid_listings"]

    # Insert the listings data
    try:
        if invalid_listings_data:  # Check if listings_data is not empty
            result = collection.insert_many(invalid_listings_data)
            print(f"Inserted {len(result.inserted_ids)} invalid listings into the collection")
        else:
            print("No invalid listings data to insert")
    except Exception as e:
        print(f"An error occurred: {e}")
    


def create_current_listings_dict(db):
    coll_prefix = scrape_parameters["mongo_collection_name_prefix"]
    current_collection = db[coll_prefix + "_last_up"]
    return {doc["listing_hash_id"]: False for doc in current_collection.find({}, {"listing_hash_id": 1})}

def move_outdated_listings_to_previously_up(db, listings_dict):
    coll_prefix = scrape_parameters["mongo_collection_name_prefix"]
    current_collection = db[coll_prefix + "_last_up"]
    previously_up_collection = db[coll_prefix + "_previously_up"]
    for key, is_up in listings_dict.items():
        if not is_up:
            listing = current_collection.find_one_and_delete({"listing_hash_id": key})
            if listing:
                now = datetime.now()
                listing['date_time_mesured_removed'] = now
                # Calculate the difference in time and set it to 'up_time'
                #if listing['date_time_posted'] and isinstance(listing['date_time_posted'], datetime):
                time_diff = now - listing['date_time_posted']
                listing['up_time_hours'] = time_diff.total_seconds() / 3600  # 'up_time' in seconds to hours

                previously_up_collection.insert_one(listing)

def add_new_listings_to_current(db, listings_to_be_added):
    coll_prefix = scrape_parameters["mongo_collection_name_prefix"]
    current_collection = db[coll_prefix+"_last_up"]
    if listings_to_be_added:
        current_collection.insert_many(listings_to_be_added)

def syncDatabaseWithCurrentListings(valid_listings, invalid_listings):
    print("Starting database synchronization...")
    db_prefix = scrape_parameters["mongo_data_base_name_prefix"]
    uri = "mongodb+srv://banana111x:abalbaba@cluster0.ereatrt.mongodb.net/?retryWrites=true&w=majority"
    
    
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client[db_prefix+"_listings"]

    listings_dict = create_current_listings_dict(db)
    listings_to_be_added = []

    for listing in valid_listings:
        if listing['listing_hash_id'] in listings_dict:
            listings_dict[listing['listing_hash_id']] = True
        else:
            listings_to_be_added.append(listing)

    move_outdated_listings_to_previously_up(db, listings_dict)
    add_new_listings_to_current(db, listings_to_be_added)

    update_todays_performance(db, listings_to_be_added)
    
    #add_invalid_listings_to_mongodb(db, invalid_listings)
    print("Database synchronization completed.")
    client.close()

def calculate_quantiles(prices):
    if len(prices) < 2:
        return (0, 0)
    elif len(prices) == 2:
        return (prices[0], prices[1])
    else:
        sorted_prices = sorted(prices)
        mid = len(sorted_prices) // 2
        q1 = median(sorted_prices[:mid])
        q2 = median(sorted_prices[mid:])
        return (q1, q2)

def getPricesArray(todays_listings):
    prices = []  # Initialize an empty list to store prices

    for listing in todays_listings:
        if 'price' in listing and listing['price']:
            try:
                price = float(listing['price'])
                prices.append(price)
            except ValueError as e:
                print(f"Error converting price to float: {e}")

    # Now prices contains all the processed prices
    return prices



def update_todays_performance(db, listings_to_be_added):
    print("Updating today's performance data...")
    coll_prefix = scrape_parameters["mongo_collection_name_prefix"]
    # Get today's date
    today = datetime.now().date()

    # Collections
    current_collection = db[coll_prefix+"_last_up"]
    previously_up_collection = db[coll_prefix+"_previously_up"]
    performance_collection = db[coll_prefix+"_todays_performance"]
    previous_performances_collection = db[coll_prefix+"_previous_performances"]

    # Check if update is needed
    performance_data = performance_collection.find_one({"performance_data": True})

    # Check if update is needed
    performance_data = performance_collection.find_one({"performance_data": True})
    if performance_data and performance_data['date_time_last_modified'].date() != today:
        # Move yesterday's performance data to the time series collection
        performance_data.pop('_id')  # Remove the MongoDB-generated ID
        previous_performances_collection.insert_one(performance_data)
        performance_collection.delete_one({"performance_data": True})

    num_sold_removed = previously_up_collection.count_documents({"date_time_mesured_removed": {"$gte": datetime.combine(today, datetime.min.time())}})

    performance_is_not_present = not performance_data
    performance_different_num_of_sold = (performance_data and performance_data["number_of_listings_sold_or_removed"] != num_sold_removed)
    performance_last_update_not_today = (performance_data and performance_data['date_time_last_modified'].date() != today)
    perforamnce_new_listings_to_be_added = len(listings_to_be_added) > 0
    if performance_is_not_present or performance_different_num_of_sold or performance_last_update_not_today or perforamnce_new_listings_to_be_added:
        # Fetch today's listings from both collections
        todays_listings = list(previously_up_collection.find({"date_time_posted": {"$gte": datetime.combine(today, datetime.min.time())}}))
        todays_listings.extend(list(current_collection.find({})))
        closed_today_listings = list(previously_up_collection.find({"date_time_mesured_removed": {"$gte": datetime.combine(today, datetime.min.time())}}))
        # Calculate statistics
        #prices = [float(listing['price'].replace('€ ', '').replace(',', '.').strip()) for listing in todays_listings if listing['price']]
        prices = getPricesArray(todays_listings)
        lowest_price = min(prices, default=0)
        highest_price = max(prices, default=0)
        average_price = mean(prices) if prices else 0
        
        q1, q2 = calculate_quantiles(prices)

        difference = average_price - lowest_price
        # Count sold or removed listings

        up_time_hours = [float(listing['up_time_hours']) for listing in closed_today_listings if listing['up_time_hours']]
        lowest_up_time_hours = min(up_time_hours, default=0)
        highest_up_time_hours = max(up_time_hours, default=0)
        average_up_time_hours = mean(up_time_hours) if up_time_hours else 0

        time_to_scrape_minutes = [float(listing['time_to_scrape_minutes']) for listing in todays_listings if listing['time_to_scrape_minutes']]
        avrage_time_to_scrape_minutes = mean(time_to_scrape_minutes) if prices else 0
        # Update or create the performance data document
        performance_update = {
            "lowest_price": lowest_price,
            "highest_price": highest_price,
            "q1": q1,
            "q2": q2,
            "average_price": average_price,
            "difference": difference,
            "number_of_listings_sold_or_removed": num_sold_removed,
            "lowest_up_time_hours": lowest_up_time_hours,
            "highest_up_time_hours": highest_up_time_hours,
            "average_up_time_hours": average_up_time_hours,
            "avrage_time_to_scrape_minutes": avrage_time_to_scrape_minutes,
            "date_time_last_modified": datetime.now()
        }

        performance_collection.update_one({"performance_data": True}, {"$set": performance_update}, upsert=True)

        # Insert today's listings into the performance collection
        #performance_collection.insert_many(todays_listings)
        print("Today's performance data updated.")
    else:
        print("No update required for today's performance data.")

def testScenario1():
    #csv_file_path = 'valid_listings.csv'  # Replace with your CSV file path
    setScrapeParameters("test_scrape_listing_parameters", "test GTX 1060 parameters.json")
    csv_file_path = 'valid_listings_test_example1.csv'  # Replace with your CSV file path
    valid_listings = load_listings_from_csv(csv_file_path)
    syncDatabaseWithCurrentListings(valid_listings)
    print("finished running test scenario 1")
    print()


# Run the script
#asyncio.run(run())
#connectToAtlas()
#testScenario1()