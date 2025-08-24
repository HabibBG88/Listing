# api/routers/listings.py
from typing import Optional, Literal, Dict, Any
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.engine import Connection

from api.deps import get_conn
from api.models import Listing, ListingsResponse
from api.repository import listings as repo

router = APIRouter(prefix="/api", tags=["listings"])

@router.get("/listings", response_model=ListingsResponse)
def list_listings(
    # filters (all optional)
    transaction_type: Optional[str] = Query(None, description="SELL or RENT"),
    item_type: Optional[str]        = Query(None, description="APARTMENT, HOUSE, ..."),
    item_subtype: Optional[str]     = Query(None, description="LOFT, DUPLEX, ..."),
    city: Optional[str]             = Query(None),
    zipcode: Optional[str]          = Query(None, description="5-digit code"),

    min_price: Optional[float]      = Query(None, ge=0),
    max_price: Optional[float]      = Query(None, ge=0),
    min_area: Optional[float]       = Query(None, ge=0),
    max_area: Optional[float]       = Query(None, ge=0),
    build_year_min: Optional[int]   = Query(None, ge=1800, le=2100),
    build_year_max: Optional[int]   = Query(None, ge=1800, le=2100),

    has_passenger_lift: Optional[bool] = Query(None),
    is_furnished: Optional[bool]       = Query(None),
    is_new_construction: Optional[bool]= Query(None),

    # paging & sorting
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    sort_by: str = Query("valid_from"),
    sort_dir: Literal["asc", "desc"] = Query("desc"),

    conn: Connection = Depends(get_conn),
):
    """
    Thin endpoint:
      - assemble filters into a dict
      - call repository.search()
      - map raw rows -> Pydantic models
    """
    q: Dict[str, Any] = {
        "transaction_type": transaction_type,
        "item_type": item_type,
        "item_subtype": item_subtype,
        "city": city,
        "zipcode": zipcode,
        "min_price": min_price,
        "max_price": max_price,
        "min_area": min_area,
        "max_area": max_area,
        "build_year_min": build_year_min,
        "build_year_max": build_year_max,
        "has_passenger_lift": has_passenger_lift,
        "is_furnished": is_furnished,
        "is_new_construction": is_new_construction,
        "page": page,
        "page_size": page_size,
        "sort_by": sort_by,
        "sort_dir": sort_dir,
    }

    rows, total = repo.search(conn, q)
    items = [Listing(**row) for row in rows]  # Pydantic validates/serializes

    return ListingsResponse(total=total, page=page, page_size=page_size, items=items)

@router.get("/listings/{external_listing_id}", response_model=Listing)
def get_listing(external_listing_id: str, conn: Connection = Depends(get_conn)):
    row = repo.get_by_external_id(conn, external_listing_id)
    if not row:
        raise HTTPException(status_code=404, detail="Listing not found")
    return Listing(**row)
