import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, text

# Load environment variables from the .env file in the project root
# Note the path to find the .env file one level up
load_dotenv(dotenv_path='../.env')

DB_NAME = os.getenv('DB_NAME', 'mydatabase')
DB_USER = os.getenv('DB_USER', 'user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')

# The schema where your final dbt models are stored
DBT_SCHEMA = 'tg_data_warehouse' 

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create the SQLAlchemy engine
engine = create_engine(DATABASE_URL)
metadata = MetaData()

def get_db_connection():
    """Yields a database connection to be used with FastAPI's dependency injection."""
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()

# --- Helper function to test connection ---
def test_connection():
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            print("Database connection successful.")
            return True
    except Exception as e:
        print(f"Database connection failed: {e}")
        return False

if __name__ == "__main__":
    test_connection()