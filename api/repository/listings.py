import logging
from typing import Dict, Any, Tuple, List

from sqlalchemy import and_, asc, desc, func
from sqlalchemy.engine import Connection

from api.sql import (
    base_select,          # joined SELECT from listing_current + dims
    detail_select,        # SELECT for a single listing (current)
    listing_current,      # table object for allowed sort columns
    city, zipcode, item_type, item_subtype, transaction_type
)

LOG = logging.getLogger("repo")

_ALLOWED_SORT = {
    "price":       listing_current.c.price,
    "area":        listing_current.c.area,
    "build_year":  listing_current.c.build_year,
    "valid_from":  listing_current.c.valid_from,
}

def _apply_filters(stmt, q: Dict[str, Any]):
    """Attach WHEREs + ORDER BY to a Core statement built by base_select()."""
    conds = []

    # numeric ranges
    if (v := q.get("min_price")) is not None:
        conds.append(listing_current.c.price >= v)
    if (v := q.get("max_price")) is not None:
        conds.append(listing_current.c.price <= v)
    if (v := q.get("min_area")) is not None:
        conds.append(listing_current.c.area >= v)
    if (v := q.get("max_area")) is not None:
        conds.append(listing_current.c.area <= v)

    # build year
    if (v := q.get("build_year_min")) is not None:
        conds.append(listing_current.c.build_year >= v)
    if (v := q.get("build_year_max")) is not None:
        conds.append(listing_current.c.build_year <= v)

    # boolean flags
    for flag in ("has_passenger_lift", "is_furnished", "is_new_construction"):
        if (v := q.get(flag)) is not None:
            conds.append(getattr(listing_current.c, flag) == v)

    # categorical filters (use the joined dim cols present in base_select())
    if (v := q.get("transaction_type")):
        stmt = stmt.where(transaction_type.c.code == v)
    if (v := q.get("item_type")):
        stmt = stmt.where(item_type.c.code == v)
    if (v := q.get("item_subtype")):
        stmt = stmt.where(item_subtype.c.code == v)
    if (v := q.get("city")):
        stmt = stmt.where(city.c.name == v)
    if (v := q.get("zipcode")):
        stmt = stmt.where(zipcode.c.code == v)

    if conds:
        stmt = stmt.where(and_(*conds))

    # Sorting (whitelisted)
    col = _ALLOWED_SORT.get(q.get("sort_by") or "valid_from", listing_current.c.valid_from)
    sort_dir = (q.get("sort_dir") or "desc").lower()
    stmt = stmt.order_by(asc(col) if sort_dir == "asc" else desc(col))

    return stmt

def search(conn: Connection, q: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], int]:
    """
    Returns (rows, total) according to filters/pagination in `q`.
    `rows` are plain dicts with keys matching api.models.Listing.
    """
    page  = int(q.get("page") or 1)
    size  = int(q.get("page_size") or 20)
    offset = (page - 1) * size

    stmt = _apply_filters(base_select(), q)

    # total count without pagination
    total = conn.execute(stmt.with_only_columns(func.count()).order_by(None)).scalar_one()

    # page rows
    rows = conn.execute(stmt.limit(size).offset(offset)).mappings().all()
    return [dict(r) for r in rows], total

def get_by_external_id(conn: Connection, external_listing_id: str) -> Dict[str, Any]:
    """
    Returns one current listing by its external id as a dict,
    or {} if not found.
    """
    stmt = detail_select().where(listing_current.c.external_listing_id == external_listing_id)
    row = conn.execute(stmt).mappings().first()
    return dict(row) if row else {}
