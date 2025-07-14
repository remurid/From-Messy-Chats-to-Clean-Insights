From Messy Chats to Clean Insights: A Telegram Data Pipeline
This project is an end-to-end data pipeline that extracts data from public Telegram channels related to Ethiopian medical businesses, loads it into a data warehouse, transforms it into a clean and usable format, and prepares it for analysis.

This README covers the first two major stages of the project:

Task 1: Data Scraping & Collection (Extract & Load)

Task 2: Data Modeling & Transformation (Transform)

Project Structure
.
├── .env                  # Stores all secrets (API keys, passwords) - NOT committed to Git
├── .gitignore            # Specifies files for Git to ignore
├── data/
│   └── raw/              # The "Data Lake" for raw, unaltered data
│       ├── telegram_messages/
│       └── images/
├── my_telegram_analytics/  # The dbt project for data transformation
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   ├── dbt_project.yml
│   └── ...
├── load_raw_to_postgres.py # Script to load raw data into the database
├── loader.log            # Log file for the data loading script
├── requirements.txt      # Python dependencies
├── telegram_scraper.py   # Script to scrape data from Telegram
└── scraper.log           # Log file for the scraping script

Getting Started
Prerequisites
Python 3.8+

Git

Docker and Docker Compose (to run the PostgreSQL database)

A Telegram account

1. Clone the Repository
git clone <your-repository-url>
cd <your-repository-name>

2. Set Up the Environment
A. Create the .env file:

Create a file named .env in the root of the project and populate it with your credentials. This file is listed in .gitignore and will not be committed to version control.

# Telegram API Credentials (get from my.telegram.org)
API_ID=YOUR_TELEGRAM_API_ID
API_HASH=YOUR_TELEGRAM_API_HASH

# PostgreSQL Database Credentials
DB_NAME=mydatabase
DB_USER=user
DB_PASSWORD=password
DB_HOST=localhost
DB_PORT=5432

B. Start the PostgreSQL Database using Docker:

Create a docker-compose.yml file to easily manage the database service:

# docker-compose.yml
version: '3.8'
services:
  postgres-db:
    image: postgres:13
    container_name: postgres_db
    environment:
      POSTGRES_USER: ${DB_USER}
      POSTGRES_PASSWORD: ${DB_PASSWORD}
      POSTGRES_DB: ${DB_NAME}
    ports:
      - "${DB_PORT}:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:

Now, start the database service from your terminal:

docker-compose up -d

C. Install Python Dependencies:

Create a virtual environment and install the required packages.

python -m venv .venv
source .venv/bin/activate  # On Windows, use: .venv\Scripts\activate
pip install -r requirements.txt

Task 1: Data Scraping (Extract & Load)
This task uses the Telethon library to connect to the Telegram API, scrape messages from a predefined list of public channels, and save the raw data into a structured directory, creating a local data lake.

How to Run
Execute the scraper script from the root of the project:

python telegram_scraper.py

Note: The first time you run this, you will be prompted in the terminal to enter your phone number, a login code sent to your Telegram app, and your 2FA password (if enabled). A telegram_scraper_session.session file will be created to keep you logged in for future runs.

Expected Output
Messages: Raw JSON files will be created in data/raw/telegram_messages/, partitioned by date and channel name (e.g., data/raw/telegram_messages/2025-07-14/chemedapp/12345.json).

Images: For specified channels, images will be downloaded to data/raw/images/, partitioned by date (e.g., data/raw/images/2025-07-14/12345.jpg).

Logs: The process will be logged to the console and to scraper.log.

Task 2: Data Transformation (Transform)
This task is a two-part process that first loads the raw data into the PostgreSQL data warehouse and then uses dbt to transform it into a clean, analytics-ready star schema.

Part A: Load Raw Data into PostgreSQL
This step uses the load_raw_to_postgres.py script to read the raw JSON files from the data lake and insert them into a raw.messages table in the database.

How to Run
python load_raw_to_postgres.py

The script is idempotent, meaning it keeps track of which files have been loaded and will not create duplicate entries if you run it multiple times.

Part B: Transform Data with dbt
This step uses dbt (Data Build Tool) to execute a series of SQL models that clean the data and build our final fact and dimension tables.

dbt Setup
Configure Profile: Make sure your ~/.dbt/profiles.yml file is configured correctly to connect to the PostgreSQL database using the credentials from the .env file.

Install dbt: Ensure dbt-postgres is in your requirements.txt and installed.

How to Run
Navigate to your dbt project directory and run the following commands:

cd my_telegram_analytics

# Run all the transformation models
dbt run

# Run all the data quality tests defined in schema.yml files
dbt test

Expected Output
After a successful dbt run, a new schema (e.g., dbt_yourname) will be created in your database containing the final, clean tables:

dim_channels

dim_dates

fct_messages

The dbt test command will verify the integrity of your data (e.g., checking for nulls and uniqueness in primary keys). All tests should pass.