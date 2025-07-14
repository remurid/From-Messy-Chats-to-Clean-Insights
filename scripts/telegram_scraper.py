import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from telethon.sync import TelegramClient
from telethon.tl.types import MessageMediaPhoto

# --- Configuration ---

# Load environment variables from .env file (for API_ID, API_HASH)
load_dotenv()
API_ID = os.getenv('TELEGRAM_APP_ID')
API_HASH = os.getenv('TELEGRAM_APP_HASH')
SESSION_NAME = 'telegram_scraper_session'

# List of target Telegram channels to scrape
TARGET_CHANNELS = ['chemedapp', 'lobelia4cosmetics', 'tikvahpharma']

# Base directories for our "data lake"
BASE_DATA_DIR = './data/raw'
MESSAGES_DIR = os.path.join(BASE_DATA_DIR, 'telegram_messages')
IMAGES_DIR = os.path.join(BASE_DATA_DIR, 'images')

# --- Setup Logging ---

# Configure logging to provide detailed output of the scraping process
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)

# --- Main Scraping Logic ---

def create_directories():
    """Creates the necessary data directories if they don't exist."""
    os.makedirs(MESSAGES_DIR, exist_ok=True)
    os.makedirs(IMAGES_DIR, exist_ok=True)
    logging.info("Created base data directories.")

async def scrape_channel(client, channel_username):
    """
    Scrapes a single Telegram channel for messages and images.

    Args:
        client (TelegramClient): An active Telethon client instance.
        channel_username (str): The username of the channel to scrape.
    """
    logging.info(f"--- Starting scrape for channel: {channel_username} ---")
    messages_scraped_count = 0
    images_downloaded_count = 0

    try:
        # Get the channel entity
        channel = await client.get_entity(channel_username)

        # Iterate through messages in the channel
        # We can limit the number of messages for testing, e.g., limit=100
        async for message in client.iter_messages(channel, limit=200):
            # Create a partitioned directory structure based on message date
            # e.g., data/raw/telegram_messages/2025-07-14/chemedapp/
            message_date_str = message.date.strftime('%Y-%m-%d')
            channel_message_dir = os.path.join(MESSAGES_DIR, message_date_str, channel_username)
            os.makedirs(channel_message_dir, exist_ok=True)

            # Define the path for the JSON file
            message_json_path = os.path.join(channel_message_dir, f'{message.id}.json')

            # --- 1. Save Message as JSON ---
            # Check if we have already processed this message to avoid re-work
            if not os.path.exists(message_json_path):
                # Convert the message object to a dictionary, which is JSON serializable
                message_dict = message.to_dict()
                
                # Save the dictionary as a JSON file
                with open(message_json_path, 'w', encoding='utf-8') as f:
                    json.dump(message_dict, f, ensure_ascii=False, indent=4, default=str)
                messages_scraped_count += 1

            # --- 2. Download Image if it exists ---
            # As per the requirements, download images from 'chemedapp' and 'lobelia4cosmetics'
            if channel_username in ['chemedapp', 'lobelia4cosmetics', 'tikvahpharma'] and message.media and isinstance(message.media, MessageMediaPhoto):
                # Create a partitioned directory for images
                image_date_dir = os.path.join(IMAGES_DIR, message_date_str)
                os.makedirs(image_date_dir, exist_ok=True)
                
                # Define the path for the image file
                image_path = os.path.join(image_date_dir, f'{message.id}.jpg')

                # Check if we have already downloaded this image
                if not os.path.exists(image_path):
                    # Download the photo
                    await client.download_media(message.media, file=image_path)
                    images_downloaded_count += 1

    except Exception as e:
        logging.error(f"An error occurred while scraping {channel_username}: {e}")
    
    logging.info(f"Finished scraping {channel_username}. "
                 f"New Messages: {messages_scraped_count}, New Images: {images_downloaded_count}")


async def main():
    """Main function to initialize the client and start the scraping process."""
    logging.info("Initializing Telegram Scraper...")
    create_directories()

    # The client is created within an 'async with' block.
    # This ensures it's properly connected and disconnected.
    async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
        for channel in TARGET_CHANNELS:
            await scrape_channel(client, channel)
    
    logging.info("--- Scraping process completed for all channels. ---")


if __name__ == '__main__':
    # Use asyncio to run our asynchronous main function
    import asyncio
    asyncio.run(main())
