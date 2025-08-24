from sqlalchemy import MetaData, Table, Column, Integer, BigInteger, String, Numeric, Boolean, DateTime
from sqlalchemy.sql import select

metadata = MetaData()

# ---------- Tables (as in the schema) ----------
listing_current = Table(
    "listing_current", metadata,
    Column("id", BigInteger),
    Column("external_listing_id", String),
    Column("transaction_type_id", Integer),
    Column("item_type_id", Integer),          # denormalized in MV
    Column("item_subtype_id", Integer),
    Column("location_id", BigInteger),
    Column("build_year", Integer),
    Column("is_new_construction", Boolean),
    Column("has_passenger_lift", Boolean),
    Column("has_cellar", Boolean),
    Column("is_furnished", Boolean),
    Column("price", Numeric),
    Column("area", Numeric),
    Column("site_area", Numeric),
    Column("floor", Integer),
    Column("room_count", Integer),
    Column("balcony_count", Integer),
    Column("terrace_count", Integer),
    Column("terrace_area", Numeric),
    Column("valid_from", DateTime),  # timestamptz in PG; SQLA DateTime is fine
)

item_subtype = Table(
    "item_subtype", metadata,
    Column("item_subtype_id", Integer, primary_key=True),
    Column("item_type_id", Integer),
    Column("code", String),
)

item_type = Table(
    "item_type", metadata,
    Column("item_type_id", Integer, primary_key=True),
    Column("code", String),
)

transaction_type = Table(
    "transaction_type", metadata,
    Column("transaction_type_id", Integer, primary_key=True),
    Column("code", String),
)

location = Table(
    "location", metadata,
    Column("location_id", BigInteger, primary_key=True),
    Column("city_id", BigInteger),
    Column("zipcode_id", BigInteger),
)

city = Table(
    "city", metadata,
    Column("city_id", BigInteger, primary_key=True),
    Column("name", String),
)

zipcode = Table(
    "zipcode", metadata,
    Column("zipcode_id", BigInteger, primary_key=True),
    Column("code", String),
)

listing_description = Table(
    "listing_description", metadata,
    Column("listing_id", BigInteger, primary_key=True),  # FK to listing.id
    Column("lang", String),
    Column("body", String),
)

# ---------- Shared FROM builders ----------

def _from_core():
    """
    Core join chain for both list & detail selects (excludes description).
    Joins item_type via item_subtype for robust lineage.
    """
    jf = (
        listing_current
        .join(item_subtype, item_subtype.c.item_subtype_id == listing_current.c.item_subtype_id)
        .join(item_type, item_type.c.item_type_id == item_subtype.c.item_type_id)
        .join(transaction_type, transaction_type.c.transaction_type_id == listing_current.c.transaction_type_id)
        .join(location, location.c.location_id == listing_current.c.location_id)
        .join(city, city.c.city_id == location.c.city_id)
        .join(zipcode, zipcode.c.zipcode_id == location.c.zipcode_id)
    )
    return jf

def _from_with_description():
    """
    Same join chain, plus LEFT JOIN on listing_description (matching listing_current.id).
    """
    return _from_core().join(
        listing_description,
        listing_description.c.listing_id == listing_current.c.id,
        isouter=True,
    )

# ---------- Column list reused across queries ----------

BASE_COLS = [
    listing_current.c.id.label("id"),
    listing_current.c.external_listing_id.label("external_listing_id"),

    # human-friendly dimensions
    transaction_type.c.code.label("transaction_type"),
    item_type.c.code.label("item_type"),
    item_subtype.c.code.label("item_subtype"),
    city.c.name.label("city"),
    zipcode.c.code.label("zipcode"),

    # stable attrs
    listing_current.c.build_year,
    listing_current.c.is_new_construction,
    listing_current.c.has_passenger_lift,
    listing_current.c.has_cellar,
    listing_current.c.is_furnished,

    # versioned attrs
    listing_current.c.price,
    listing_current.c.area,
    listing_current.c.site_area,
    listing_current.c.floor,
    listing_current.c.room_count,
    listing_current.c.balcony_count,
    listing_current.c.terrace_count,
    listing_current.c.terrace_area,
    listing_current.c.valid_from,
]

# ---------- Public selectors ----------

def base_select():
    """
    Minimal page/list view (no long text).
    """
    return select(*BASE_COLS).select_from(_from_core())

def detail_select():
    """
    Detailed view with optional description_fr body.
    """
    return select(
        *BASE_COLS,
        listing_description.c.body.label("description_fr"),
    ).select_from(_from_with_description())
