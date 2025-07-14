import os
import json
import logging
from dotenv import load_dotenv
import psycopg2

# --- Configuration ---

# Load environment variables from .env file
load_dotenv()
DB_NAME = os.getenv('DB_NAME', 'mydatabase')
DB_USER = os.getenv('DB_USER', 'user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')

# Base directory for the raw message data
MESSAGES_DIR = 'data/raw/telegram_messages'

# --- Setup Logging ---

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("loader.log"),
        logging.StreamHandler()
    ]
)

# --- Database Loading Logic ---

def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        logging.info("Successfully connected to PostgreSQL database.")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Could not connect to PostgreSQL database: {e}")
        return None

def setup_database(conn):
    """Sets up the necessary schema and table in the database."""
    with conn.cursor() as cur:
        logging.info("Setting up database schema and table...")
        # Create schema if it doesn't exist
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        # Create the raw messages table
        # This table stores the raw JSON data and metadata about the load process.
        # The UNIQUE constraint on file_path prevents duplicate loads.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.messages (
                id SERIAL PRIMARY KEY,
                message_id BIGINT NOT NULL,
                channel_id BIGINT,
                raw_data JSONB NOT NULL,
                file_path TEXT NOT NULL UNIQUE,
                loaded_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        # Create an index for faster lookups on message_id
        cur.execute("CREATE INDEX IF NOT EXISTS idx_message_id ON raw.messages (message_id);")
        conn.commit()
        logging.info("Database setup complete.")


def process_and_load_data(conn):
    """Scans the data lake and loads new JSON files into the database."""
    logging.info("Starting data loading process...")
    loaded_count = 0
    skipped_count = 0

    insert_query = """
        INSERT INTO raw.messages (message_id, channel_id, raw_data, file_path)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (file_path) DO NOTHING;
    """

    # Walk through the directory structure
    for root, _, files in os.walk(MESSAGES_DIR):
        for filename in files:
            if filename.endswith('.json'):
                file_path = os.path.join(root, filename)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Extract key identifiers from the JSON data
                    message_id = data.get('id')
                    channel_id = data.get('peer_id', {}).get('channel_id')

                    if not message_id:
                        logging.warning(f"Skipping file without a message ID: {file_path}")
                        continue

                    # Execute the insert command
                    with conn.cursor() as cur:
                        cur.execute(insert_query, (message_id, channel_id, json.dumps(data), file_path))
                        # The rowcount attribute tells us if a row was inserted (1) or skipped (0)
                        if cur.rowcount > 0:
                            loaded_count += 1
                        else:
                            skipped_count += 1
                
                except json.JSONDecodeError:
                    logging.error(f"Could not decode JSON from file: {file_path}")
                except Exception as e:
                    logging.error(f"An error occurred processing file {file_path}: {e}")

    conn.commit()
    logging.info(f"Data loading finished. New files loaded: {loaded_count}. Files skipped (already loaded): {skipped_count}.")


def main():
    """Main function to run the data loading pipeline."""
    conn = get_db_connection()
    if conn:
        setup_database(conn)
        process_and_load_data(conn)
        conn.close()
        logging.info("Database connection closed.")

if __name__ == '__main__':
    main()
