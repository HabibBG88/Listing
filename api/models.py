from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel, Field

class Listing(BaseModel):
    # Internal surrogate (optional)
    id: Optional[int] = Field(None, description="Internal surrogate id")

    # Business key from CSV (required)
    external_listing_id: str = Field(..., description="Business key from source CSV")

    transaction_type: str
    item_type: str
    item_subtype: Optional[str] = None
    city: Optional[str] = None
    zipcode: Optional[str] = None
    build_year: Optional[int] = None
    is_new_construction: Optional[bool] = None
    has_passenger_lift: Optional[bool] = None
    has_cellar: Optional[bool] = None
    is_furnished: Optional[bool] = None
    price: Optional[float] = None
    area: Optional[float] = None
    site_area: Optional[float] = None
    floor: Optional[int] = None
    room_count: Optional[int] = None
    balcony_count: Optional[int] = None
    terrace_count: Optional[int] = None
    terrace_area: Optional[float] = None
    valid_from: Optional[datetime] = None

class ListingsResponse(BaseModel):
    total: int = Field(..., description="Total rows that match the filters")
    page: int
    page_size: int
    items: List[Listing]
