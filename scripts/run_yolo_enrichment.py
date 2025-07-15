import os
import json
import logging
from dotenv import load_dotenv
import psycopg2
from ultralytics import YOLO

# --- Configuration ---

# Load environment variables from .env file
load_dotenv()
DB_NAME = os.getenv('DB_NAME', 'mydatabase')
DB_USER = os.getenv('DB_USER', 'user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')

# Directory where scraped images are stored
IMAGES_DIR = './data/raw/images'

# Initialize the YOLO model. 'yolov8n.pt' is the smallest and fastest version.
# The model will be downloaded automatically on first use.
YOLO_MODEL = YOLO('yolov8n.pt')

# --- Setup Logging ---

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("yolo_enrichment.log"),
        logging.StreamHandler()
    ]
)

# --- Database and Enrichment Logic ---

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
        logging.info("Successfully connected to PostgreSQL database for enrichment.")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Could not connect to PostgreSQL database: {e}")
        return None

def setup_database(conn):
    """Sets up the necessary schema and table for storing detection results."""
    with conn.cursor() as cur:
        logging.info("Setting up enrichment schema and table...")
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw_enrichment;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_enrichment.image_detections (
                id SERIAL PRIMARY KEY,
                message_id BIGINT NOT NULL,
                image_path TEXT NOT NULL,
                detected_object_class TEXT NOT NULL,
                confidence_score REAL NOT NULL,
                bounding_box JSONB,
                loaded_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(image_path, detected_object_class, bounding_box)
            );
        """)
        conn.commit()
        logging.info("Enrichment database setup complete.")

def get_processed_images(conn):
    """Retrieves a set of image paths that have already been processed."""
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT image_path FROM raw_enrichment.image_detections;")
        return {row[0] for row in cur.fetchall()}

def run_enrichment(conn):
    """Scans for new images, runs YOLO detection, and loads results to the DB."""
    logging.info("Starting image enrichment process...")
    processed_images = get_processed_images(conn)
    new_detections_count = 0

    insert_query = """
        INSERT INTO raw_enrichment.image_detections 
        (message_id, image_path, detected_object_class, confidence_score, bounding_box)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (image_path, detected_object_class, bounding_box) DO NOTHING;
    """

    for root, _, files in os.walk(IMAGES_DIR):
        for filename in files:
            if filename.lower().endswith(('.png', '.jpg', '.jpeg')):
                image_path = os.path.join(root, filename)

                if image_path in processed_images:
                    continue

                logging.info(f"Processing new image: {image_path}")
                try:
                    # Extract message_id from the filename (e.g., '12345.jpg')
                    message_id = int(os.path.splitext(filename)[0])

                    # Run YOLO model on the image
                    results = YOLO_MODEL(image_path)
                    
                    detections = []
                    for result in results:
                        # Get class names from the model
                        names = result.names
                        for box in result.boxes:
                            # Prepare data for insertion
                            detection_data = {
                                'message_id': message_id,
                                'image_path': image_path,
                                'class_name': names[int(box.cls[0])],
                                'confidence': float(box.conf[0]),
                                'bbox': json.dumps(box.xyxy[0].tolist()) # Bounding box coordinates
                            }
                            detections.append(detection_data)
                    
                    # Insert detections into the database
                    with conn.cursor() as cur:
                        for det in detections:
                            cur.execute(insert_query, (
                                det['message_id'],
                                det['image_path'],
                                det['class_name'],
                                det['confidence'],
                                det['bbox']
                            ))
                            new_detections_count += cur.rowcount

                    conn.commit()
                    processed_images.add(image_path) # Mark as processed for this run

                except Exception as e:
                    logging.error(f"Failed to process image {image_path}: {e}")
                    conn.rollback() # Rollback transaction on error

    logging.info(f"Enrichment process finished. New detections loaded: {new_detections_count}.")


def main():
    """Main function to run the data enrichment pipeline."""
    conn = get_db_connection()
    if conn:
        try:
            setup_database(conn)
            run_enrichment(conn)
        finally:
            conn.close()
            logging.info("Enrichment database connection closed.")

if __name__ == '__main__':
    main()
