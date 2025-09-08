import os
import aiohttp
import asyncio
import time
from datetime import datetime, timedelta

# Create the folder 'dataset' if it does not exist
if not os.path.exists('dataset'):
    os.makedirs('dataset')

# Define the start and end dates (adjust as needed)
start_date = datetime(2013, 9, 10)
end_date = datetime(2014, 1, 1)

# Generate all timestamps for images (hourly)
dates = [start_date + timedelta(hours=i) for i in range(int((end_date - start_date).total_seconds() // 3600) + 1)]

async def fetch_image(session, date):
    """Fetch and save an image with retry logic and delay."""
    date_str = date.strftime("%Y-%m-%dT%H:%M:%SZ")
    url = f"https://api.helioviewer.org/v2/getJP2Image/?date={date_str}&sourceId=14"
    filename = f"dataset/{date.strftime('%Y%m%d_%H%M')}.jp2"

    retries = 3  # Number of retries
    delay = 5    # Initial delay in seconds

    for attempt in range(retries):
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    with open(filename, 'wb') as file:
                        file.write(await response.read())
                    print(f"Downloaded: {date_str}")
                    return  # Success, exit retry loop
                else:
                    print(f"Error {response.status} for {date_str}. Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)
                    delay *= 2  # Exponential backoff
        except aiohttp.ClientError as e:
            print(f"Connection error for {date_str}: {e}. Retrying in {delay} seconds...")
            await asyncio.sleep(delay)
            delay *= 2  # Exponential backoff

    print(f"Failed to download {date_str} after {retries} retries.")


async def download_all_images():
    """Main function to handle concurrent downloads with rate limiting."""
    semaphore = asyncio.Semaphore(5)  # Limit concurrent connections to 5 (adjust as needed)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for date in dates:
            async with semaphore:  # Acquire semaphore before fetching
                tasks.append(fetch_image(session, date))
        await asyncio.gather(*tasks)


# Run the async function
asyncio.run(download_all_images())