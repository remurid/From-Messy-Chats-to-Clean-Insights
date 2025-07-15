# analytics_api/crud.py
#
# Description: Contains the functions that interact with the database to
# read data. These functions execute raw SQL queries against the dbt models.

from sqlalchemy.orm import Session
from sqlalchemy import text
from .database import DBT_SCHEMA

def search_messages(db: Session, query: str, limit: int = 100):
    """Searches for messages containing a specific keyword."""
    sql_query = text(f"""
        SELECT 
            message_id,
            message_text,
            channel_id,
            message_posted_at
        FROM {DBT_SCHEMA}.fct_messages
        WHERE message_text ILIKE :search_term
        ORDER BY message_posted_at DESC
        LIMIT :limit;
    """)
    result = db.execute(sql_query, {'search_term': f'%{query}%', 'limit': limit})
    return result.fetchall()

def get_top_products(db: Session, limit: int = 10):
    """
    Finds the most frequently mentioned products.
    NOTE: This is a simplified example. A real-world implementation would use
    more advanced NLP to extract entity names. Here, we split by words.
    """
    sql_query = text(f"""
        WITH words AS (
            SELECT unnest(regexp_split_to_array(lower(message_text), '\\s+')) as word
            FROM {DBT_SCHEMA}.fct_messages
            WHERE message_text IS NOT NULL
        )
        SELECT 
            word as product_name,
            COUNT(*) as mention_count
        FROM words
        WHERE length(word) > 3 -- Filter out short common words
        AND word NOT IN ('and', 'the', 'for', 'with', 'http', 'https', 't.me') -- Basic stopword list
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT :limit;
    """)
    result = db.execute(sql_query, {'limit': limit})
    return result.fetchall()

def get_channel_activity(db: Session, channel_id: int):
    """Gets the daily posting activity for a specific channel."""
    sql_query = text(f"""
        SELECT 
            date(message_posted_at)::text as post_date,
            COUNT(message_id) as post_count
        FROM {DBT_SCHEMA}.fct_messages
        WHERE channel_id = :channel_id
        GROUP BY 1
        ORDER BY 1;
    """)
    result = db.execute(sql_query, {'channel_id': channel_id})
    return result.fetchall()

def get_top_detected_objects(db: Session, limit: int = 10):
    """Gets the most frequently detected objects from images."""
    sql_query = text(f"""
        SELECT
            detected_object_class as object_class,
            COUNT(detection_id) as detection_count
        FROM {DBT_SCHEMA}.fct_image_detections
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT :limit;
    """)
    result = db.execute(sql_query, {'limit': limit})
    return result.fetchall()

def get_channel_id_by_name(db: Session, channel_name: str):
    """Helper to get a channel_id from its name."""
    sql_query = text(f"""
        SELECT channel_id FROM {DBT_SCHEMA}.dim_channels
        WHERE channel_name = :channel_name
        LIMIT 1;
    """)
    result = db.execute(sql_query, {'channel_name': channel_name}).fetchone()
    return result[0] if result else None
