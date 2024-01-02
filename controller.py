from scraper import *

# Now you can call run() in main.py
async def main():
    await run()

sub_path = 'scrape_listing_parameters'
testing_sub_path = 'test_scrape_listing_parameters'
json_parameters_global_path = os.path.join("scrape_global_parameters", "global parameters.json")
delay_between_scrapes = 10  # Delay in seconds
do_scrape = True

async def check_for_stop_command():
    global do_scrape
    while True:
        user_input = await asyncio.to_thread(input, "Enter 'stop' to end scraping: ")
        if user_input.lower() == 'stop':
            do_scrape = False
            print("Stopping scrapes after the current one completes.")
            break
        await asyncio.sleep(1)  # Prevents this coroutine from hogging the event loop

async def main():
    # Start the command checking in parallel
    asyncio.create_task(check_for_stop_command())

    while do_scrape:
        print("Starting new scrape cycle.")
        all_files = os.listdir(sub_path)
        json_files = [file for file in all_files if file.endswith('.json')]
        for json_file in json_files:
            json_parameters_path = os.path.join(sub_path, json_file)
            print(f"Starting scrape for {json_file}...")
            await run(json_parameters_path, json_parameters_global_path, True, ignore_white_space_for_validity=False)
            if not do_scrape:
                print("Scrape stopped by user command.")
                return
            print(f"Finished scrape for {json_file}. Waiting for {delay_between_scrapes} seconds before next scrape.")
            await asyncio.sleep(delay_between_scrapes)
        print("Completed all scrapes in the current cycle. Starting over.")
        
if __name__ == "__main__":
    print("Initializing scraping process...")
    #asyncio.run(main())

    json_parameters_path = os.path.join(sub_path, "Titan Pascal parameters.json")
    asyncio.run(run(json_parameters_path, json_parameters_global_path, True, ignore_white_space_for_validity=False))
    

#if __name__ == "__main__":
    #json_parameters_path = os.path.join(sub_path, "QUEST 2 256GB parameters.json")
    #asyncio.run(run(json_parameters_path, json_parameters_global_path, True, ignore_white_space_for_validity=False))
    #createNewDB()
    #testScenario1()