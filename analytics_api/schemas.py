# analytics_api/schemas.py
#
# Description: Defines the Pydantic models for the API. These models determine
# the structure of the JSON responses, ensuring type safety and clear contracts.

from pydantic import BaseModel
from typing import List, Optional

class ProductReportItem(BaseModel):
    product_name: str
    mention_count: int

class ChannelActivityItem(BaseModel):
    post_date: str
    post_count: int

class MessageSearchResult(BaseModel):
    message_id: int
    message_text: Optional[str]
    channel_id: int
    message_posted_at: str

class DetectedObjectReportItem(BaseModel):
    object_class: str
    detection_count: int