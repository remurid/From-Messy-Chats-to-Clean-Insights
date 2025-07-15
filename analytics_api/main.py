# analytics_api/main.py
#
# Description: The main FastAPI application file. It defines the API endpoints,
# handles incoming requests, calls the appropriate CRUD functions, and returns
# the data structured according to the Pydantic schemas.

from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from . import crud, schemas
from .database import get_db_connection

app = FastAPI(
    title="Telegram Medical Analytics API",
    description="An API to serve insights from scraped Telegram data.",
    version="1.0.0"
)

# Dependency for getting a DB session
def get_db():
    db = next(get_db_connection())
    try:
        yield db
    finally:
        db.close()


@app.get("/")
def read_root():
    return {"message": "Welcome to the Telegram Analytics API. Go to /docs to see the endpoints."}


@app.get("/api/search/messages", response_model=List[schemas.MessageSearchResult])
def search_for_messages(query: str, db: Session = Depends(get_db)):
    """
    Searches for messages containing a specific keyword (e.g., 'paracetamol').
    """
    messages = crud.search_messages(db, query=query)
    return messages


@app.get("/api/reports/top-products", response_model=List[schemas.ProductReportItem])
def get_top_products_report(limit: int = 10, db: Session = Depends(get_db)):
    """
    Returns the most frequently mentioned products or drugs across all channels.
    """
    products = crud.get_top_products(db, limit=limit)
    return products


@app.get("/api/channels/{channel_name}/activity", response_model=List[schemas.ChannelActivityItem])
def get_channel_activity_report(channel_name: str, db: Session = Depends(get_db)):
    """
    Returns the daily posting activity for a specific channel.
    (e.g., 'Channel 1399123456')
    """
    channel_id = crud.get_channel_id_by_name(db, channel_name=channel_name)
    if channel_id is None:
        raise HTTPException(status_code=404, detail=f"Channel '{channel_name}' not found.")
    
    activity = crud.get_channel_activity(db, channel_id=channel_id)
    return activity


@app.get("/api/reports/top-detected-objects", response_model=List[schemas.DetectedObjectReportItem])
def get_top_objects_report(limit: int = 10, db: Session = Depends(get_db)):
    """
    Returns the most frequently detected objects in images across all channels.
    """
    objects = crud.get_top_detected_objects(db, limit=limit)
    return objects
