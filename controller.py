from scraper import *

# Now you can call run() in main.py
async def main():
    await run()

sub_path = 'scrape_listing_parameters'
testing_sub_path = 'test_scrape_listing_parameters'
json_parameters_global_path = os.path.join("scrape_global_parameters", "global pavrameters.json")
delay_between_scrapes = 20  # Delay in seconds
do_scrape = True

def SynchronizeScrapeProducts(json_files):
    uri = "mongodb+srv://banana111x:abalbaba@cluster0.ereatrt.mongodb.net/?retryWrites=true&w=majority"
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    db_name = "Scraper_Global"
    collection_name = "Control_Parameters"
    
    # Check if the database and collection exist, create if not
    if db_name not in client.list_database_names():
        client[db_name].create_collection(collection_name)
    
    db = client[db_name]
    collection = db[collection_name]

    # Check if control_parameters document exists
    control_params = collection.find_one({"name": "control_parameters"})
    if not control_params:
        control_params = {"name": "control_parameters", "all_current_scraping_products": []}
        collection.insert_one(control_params)

    all_current_scraping_products = control_params.get("all_current_scraping_products", [])
    local_scraping_products = []

    # Read mongo_data_base_name_prefix from each JSON file
    for json_file in json_files:
        with open(os.path.join("scrape_listing_parameters", json_file), 'r') as file:
            data = json.load(file)
            local_scraping_products.append(data["mongo_data_base_name_prefix"])

    # Determine products to sync
    scraping_products_to_sync_to_mongo = [product for product in local_scraping_products if product not in all_current_scraping_products]
    scraping_products_to_sync_localy = [product for product in all_current_scraping_products if product not in local_scraping_products]

    # Update MongoDB document
    if scraping_products_to_sync_to_mongo:
        collection.update_one(
            {"name": "control_parameters"},
            {"$addToSet": {"all_current_scraping_products": {"$each": scraping_products_to_sync_to_mongo}}}
        )

    # For now, we're not doing anything with scraping_products_to_sync_localy
    # Future functionality can be added here

    client.close()

async def check_for_stop_command():
    global do_scrape
    while True:
        user_input = await asyncio.to_thread(input, "Enter 'stop' to end scraping: ")
        if user_input.lower() == 'stop':
            do_scrape = False
            print("Stopping scrapes after the current one completes.")
            break
        await asyncio.sleep(1)  # Prevents this coroutine from hogging the event loop

async def attempt_runs(json_parameters_path, json_parameters_global_path, max_retries, retry_delay):
    for attempt in range(1, max_retries + 1):
        try:
            await run(json_parameters_path, json_parameters_global_path, True, ignore_white_space_for_validity=False)
            break  # If run is successful, break out of retry loop
        except Exception as e:
            print(f"Error on attempt {attempt} for {json_parameters_path}: {e}")
            if attempt < max_retries:
                print(f"Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
            else:
                print("Maximum retries reached. Moving to next listing.")


async def main():
    # Start the command checking in parallel
    asyncio.create_task(check_for_stop_command())
    n=0
    max_retries = 3
    retry_delay = 5  # seconds

    while do_scrape:
        print("Starting new scrape cycle.")
        all_files = os.listdir(sub_path)
        json_files = [file for file in all_files if file.endswith('.json')]
        SynchronizeScrapeProducts(json_files)
        for json_file in json_files:
            json_parameters_path = os.path.join(sub_path, json_file)
            n+=1

            print(f"Starting scrape {n} for {json_file}...")
            await attempt_runs(json_parameters_path, json_parameters_global_path, max_retries, retry_delay)
            
            if not do_scrape:
                print("Scrape stopped by user command.")
                return
            print(f"Finished scrape for {json_file}. Waiting for {delay_between_scrapes} seconds before next scrape.")
            print(datetime.now())
            await asyncio.sleep(delay_between_scrapes)
        print("Completed all scrapes in the current cycle. Starting over.")
        
if __name__ == "__main__":
    print("Initializing scraping process...")
    asyncio.run(main())

    #json_parameters_path = os.path.join(sub_path, "Titan Pascal parameters.json")
    #asyncio.run(run(json_parameters_path, json_parameters_global_path, True, ignore_white_space_for_validity=False))
    

#if __name__ == "__main__":
    #json_parameters_path = os.path.join(sub_path, "QUEST 2 256GB parameters.json")
    #asyncio.run(run(json_parameters_path, json_parameters_global_path, True, ignore_white_space_for_validity=False))
    #createNewDB()
    #testScenario1()