# -*- coding: utf-8 -*-
"""
loader.py — Bulk loader for the final 3NF + SCD-2 schema (PostgreSQL 12, Python 3.8)

Aligned to the cleaned CSV headers (exact):
  listing_id, transaction_type, item_type, item_subtype,
  start_date, change_date,
  price, area, site_area, floor, room_count, balcony_count, terrace_count, terrace_area,
  build_year, is_new_construction, has_passenger_lift, has_cellar, is_furnished,
  city, zipcode, description_fr

What it does
------------
1) COPY CSV to TEMP staging (TEXT columns) using the CSV header order.
2) Normalize zipcode to 5 digits; upsert dimensions (transaction_type, item_type, item_subtype, city, zipcode), then location.
3) Upsert listing by external key (TEXT).
4) Build typed incoming facts; detect deltas vs current; close & insert new SCD-2 rows.
5) Upsert listing_description (longest description_fr per listing).
6) Refresh listing_current (CONCURRENTLY if possible).
7) Emit diagnostics that are actually useful.

Env
---
- DATABASE_URL : e.g. postgresql+psycopg2://user:pass@localhost:5432/testdb
- CLEANED_CSV  : absolute path to the cleaned CSV produced by cleaner.py
- LOADER_LOG_LEVEL (optional): DEBUG/INFO/WARNING (default INFO)

Usage
-----
$ python3 -m pip install --upgrade "sqlalchemy>=1.4,<2.0" psycopg2-binary
$ export DATABASE_URL="postgresql+psycopg2://testuser:test123@localhost:5432/testdb"
$ export CLEANED_CSV="/abs/path/to/clean_artifacts_py/listings_cleaned.csv"
$ export LOADER_LOG_LEVEL="INFO"
$ python3 loader.py
"""

import os
import sys
import csv
import logging
from contextlib import contextmanager
from datetime import datetime

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

# ---------------- Config ----------------

DATABASE_URL = os.getenv("DATABASE_URL")
CLEANED_CSV  = os.getenv("CLEANED_CSV")  #"/XXXX/listings_cleaned.csv" #
LOG_LEVEL    = os.getenv("LOADER_LOG_LEVEL", "INFO").upper()

if not DATABASE_URL:
    print('ERROR: Set DATABASE_URL (e.g. postgresql+psycopg2://user:pass@host:5432/db)')
    sys.exit(1)

if not CLEANED_CSV or not os.path.exists(CLEANED_CSV):
    print('ERROR: Set CLEANED_CSV to the path of listings_cleaned.csv')
    sys.exit(1)

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s | %(levelname)s | %(message)s")
LOG = logging.getLogger("loader3nf")

@contextmanager
def connect() -> Connection:
    eng = create_engine(DATABASE_URL, future=True)
    conn = eng.connect()
    try:
        yield conn
    finally:
        conn.close()
        eng.dispose()

# ---------------- Header helpers ----------------

EXPECTED = [
    "listing_id", "transaction_type", "item_type", "item_subtype",
    "start_date", "change_date",
    "price", "area", "site_area", "floor", "room_count", "balcony_count", "terrace_count", "terrace_area",
    "build_year", "is_new_construction", "has_passenger_lift", "has_cellar", "is_furnished",
    "city", "zipcode", "description_fr"
]

def read_csv_header(path: str):
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        header = next(r)
    cols = [h.strip() for h in header]
    print(cols)
    return cols

def assert_expected_headers(cols):
    lc = [c.lower() for c in cols]
    missing = [c for c in EXPECTED if c not in lc]
    if missing:
        raise RuntimeError(f"CSV missing expected headers: {missing}")
    return {c: cols[lc.index(c)] for c in EXPECTED}  # map logical->actual header

# ---------------- Small SQL expression helpers ----------------

def norm_zip_5(alias, header):
    s = f"{alias}.{header}"
    return f"""
    CASE
      WHEN {s} IS NULL OR btrim({s}) = '' THEN NULL
      ELSE
        CASE
          WHEN regexp_replace({s}, '\\D', '', 'g') ~ '^\\d{{1,5}}$'
          THEN lpad(regexp_replace({s}, '\\D', '', 'g'), 5, '0')
          ELSE NULL
        END
    END
    """.strip()

def bool_or_null(alias, header):
    s = f"{alias}.{header}"
    return f"""
    CASE
      WHEN {s} IS NULL OR btrim({s}) = '' THEN NULL
      WHEN lower({s}) IN ('true','t','1','yes','y','oui') THEN TRUE
      WHEN lower({s}) IN ('false','f','0','no','n','non') THEN FALSE
      ELSE NULL
    END
    """.strip()

def year_1800_2100(alias, header):
    s = f"{alias}.{header}"
    return f"""
    CASE
      WHEN {s} IS NULL OR btrim({s}) = '' THEN NULL
      ELSE
        CASE
          WHEN regexp_replace({s}, '[^0-9]', '', 'g') ~ '^\\d+$'
               AND round(regexp_replace({s}, '[^0-9]', '', 'g')::numeric)::int BETWEEN 1800 AND 2100
          THEN round(regexp_replace({s}, '[^0-9]', '', 'g')::numeric)::smallint
          ELSE NULL
        END
    END
    """.strip()

def num_nonneg(alias, header):
    s = f"{alias}.{header}"
    return f"""
    CASE
      WHEN {s} IS NULL OR btrim({s}) = '' THEN NULL
      ELSE
        CASE
          WHEN regexp_replace({s}, '[^0-9\\.-]', '', 'g') ~ '^-?\\d+(\\.\\d+)?$'
               AND (regexp_replace({s}, '[^0-9\\.-]', '', 'g')::numeric) >= 0
          THEN regexp_replace({s}, '[^0-9\\.-]', '', 'g')::numeric
          ELSE NULL
        END
    END
    """.strip()

def smallint_nonneg(alias, header):
    s = f"{alias}.{header}"
    return f"""
    CASE
      WHEN {s} IS NULL OR btrim({s}) = '' THEN NULL
      ELSE
        CASE
          WHEN regexp_replace({s}, '[^0-9\\.-]', '', 'g') ~ '^-?\\d+(\\.\\d+)?$'
               AND round(regexp_replace({s}, '[^0-9\\.-]', '', 'g')::numeric) >= 0
          THEN round(regexp_replace({s}, '[^0-9\\.-]', '', 'g')::numeric)::smallint
          ELSE NULL
        END
    END
    """.strip()

def floor_bounded(alias, header, lo=-5, hi=300):
    s = f"{alias}.{header}"
    return f"""
    CASE
      WHEN {s} IS NULL OR btrim({s}) = '' THEN NULL
      ELSE
        CASE
          WHEN regexp_replace({s}, '[^0-9\\.-]', '', 'g') ~ '^-?\\d+(\\.\\d+)?$'
               AND round(regexp_replace({s}, '[^0-9\\.-]', '', 'g')::numeric)::int BETWEEN {lo} AND {hi}
          THEN round(regexp_replace({s}, '[^0-9\\.-]', '', 'g')::numeric)::smallint
          ELSE NULL
        END
    END
    """.strip()

def ts_or_null(alias, header):
    return f"NULLIF({alias}.{header}, '')::timestamptz"

# ---------------- DB operations (modular) ----------------

def create_temp_staging(conn: Connection, header_cols):
    conn.execute(text("DROP TABLE IF EXISTS stg_clean_csv"))
    cols_sql = ",\n  ".join(f"{h} TEXT" for h in header_cols)  # use actual header spelling
    conn.execute(text(f"CREATE TEMP TABLE stg_clean_csv (\n  {cols_sql}\n) ON COMMIT PRESERVE ROWS;"))
    LOG.debug("Created TEMP table stg_clean_csv")

def copy_csv_into_staging(conn: Connection, csv_path: str, header_cols):
    raw = conn.connection
    cur = raw.cursor()
    cur.copy_expert(
        f"COPY stg_clean_csv ({', '.join(header_cols)}) FROM STDIN WITH (FORMAT csv, HEADER true)",
        open(csv_path, "r", encoding="utf-8"),
    )
    LOG.info("COPY → stg_clean_csv done")

def upsert_dimensions(conn: Connection, h):
    # transaction_type, item_type, item_subtype
    conn.execute(text(f"""
        INSERT INTO transaction_type (code)
        SELECT DISTINCT btrim(sc.{h['transaction_type']})
        FROM stg_clean_csv sc
        WHERE sc.{h['transaction_type']} IS NOT NULL AND btrim(sc.{h['transaction_type']}) <> ''
        ON CONFLICT (code) DO NOTHING;
    """))

    conn.execute(text(f"""
        INSERT INTO item_type (code)
        SELECT DISTINCT btrim(sc.{h['item_type']})
        FROM stg_clean_csv sc
        WHERE sc.{h['item_type']} IS NOT NULL AND btrim(sc.{h['item_type']}) <> ''
        ON CONFLICT (code) DO NOTHING;
    """))

    conn.execute(text(f"""
        INSERT INTO item_subtype (item_type_id, code)
        SELECT it.item_type_id, s.sub
        FROM (
          SELECT DISTINCT btrim(sc.{h['item_type']}) AS typ, btrim(sc.{h['item_subtype']}) AS sub
          FROM stg_clean_csv sc
          WHERE sc.{h['item_subtype']} IS NOT NULL AND btrim(sc.{h['item_subtype']}) <> ''
        ) s
        JOIN item_type it ON it.code = s.typ
        ON CONFLICT (item_type_id, code) DO NOTHING;
    """))

def upsert_cities_zipcodes_locations(conn: Connection, h):
    # City
    conn.execute(text(f"""
        INSERT INTO city (name)
        SELECT DISTINCT btrim(sc.{h['city']})
        FROM stg_clean_csv sc
        WHERE sc.{h['city']} IS NOT NULL AND btrim(sc.{h['city']}) <> ''
          AND NOT EXISTS (SELECT 1 FROM city c WHERE c.name = btrim(sc.{h['city']}));
    """))

    # Zip normalized to 5 digits
    zip_norm = norm_zip_5("sc", h["zipcode"])
    conn.execute(text(f"""
        INSERT INTO zipcode (code)
        SELECT DISTINCT zc
        FROM (SELECT {zip_norm} AS zc FROM stg_clean_csv sc) s
        WHERE zc IS NOT NULL
        ON CONFLICT (code) DO NOTHING;
    """))

    # Location
    conn.execute(text(f"""
        INSERT INTO location (city_id, zipcode_id)
        SELECT c.city_id, z.zipcode_id
        FROM (
          SELECT DISTINCT btrim(sc.{h['city']}) AS city, {zip_norm} AS zip
          FROM stg_clean_csv sc
        ) s
        JOIN city c    ON c.name = s.city
        JOIN zipcode z ON z.code = s.zip
        ON CONFLICT (city_id, zipcode_id) DO NOTHING;
    """))

def upsert_listings(conn: Connection, h):
    build_year_sql = year_1800_2100("sc", h["build_year"])
    is_new_sql     = bool_or_null("sc", h["is_new_construction"])
    has_lift_sql   = bool_or_null("sc", h["has_passenger_lift"])
    has_cellar_sql = bool_or_null("sc", h["has_cellar"])
    is_furn_sql    = bool_or_null("sc", h["is_furnished"])

    # NOTE: external_listing_id is TEXT; compare as TEXT
    conn.execute(text(f"""
        INSERT INTO listing (
          external_listing_id, transaction_type_id, item_subtype_id, location_id,
          build_year, is_new_construction, has_passenger_lift, has_cellar, is_furnished
        )
        SELECT
          NULLIF(btrim(sc.{h['listing_id']}), '')::text,
          tt.transaction_type_id,
          st.item_subtype_id,
          loc.location_id,
          {build_year_sql},
          {is_new_sql},
          {has_lift_sql},
          {has_cellar_sql},
          {is_furn_sql}
        FROM stg_clean_csv sc
        JOIN transaction_type tt ON tt.code = btrim(sc.{h['transaction_type']})
        JOIN item_type it        ON it.code = btrim(sc.{h['item_type']})
        JOIN item_subtype st     ON st.code = btrim(sc.{h['item_subtype']}) AND st.item_type_id = it.item_type_id
        JOIN city c              ON c.name = btrim(sc.{h['city']})
        JOIN zipcode z           ON z.code = {norm_zip_5('sc', h['zipcode'])}
        JOIN location loc        ON loc.city_id = c.city_id AND loc.zipcode_id = z.zipcode_id
        ON CONFLICT (external_listing_id) DO UPDATE SET
          transaction_type_id = EXCLUDED.transaction_type_id,
          item_subtype_id     = EXCLUDED.item_subtype_id,
          location_id         = EXCLUDED.location_id,
          build_year          = EXCLUDED.build_year,
          is_new_construction = EXCLUDED.is_new_construction,
          has_passenger_lift  = EXCLUDED.has_passenger_lift,
          has_cellar          = EXCLUDED.has_cellar,
          is_furnished        = EXCLUDED.is_furnished;
    """))

def upsert_descriptions(conn: Connection, h):
    # longest non-empty description per listing
    conn.execute(text(f"""
        WITH src AS (
            SELECT
                NULLIF(btrim(sc.{h['listing_id']}), '')::text AS ext_id,
                NULLIF(btrim(sc.{h['description_fr']}), '')::text AS body
            FROM stg_clean_csv sc
            WHERE sc.{h['description_fr']} IS NOT NULL
              AND length(btrim(sc.{h['description_fr']})) > 0
        ),
        pick AS (
            SELECT ext_id, body
            FROM (
                SELECT
                    ext_id, body,
                    ROW_NUMBER() OVER (PARTITION BY ext_id ORDER BY length(body) DESC) AS rn
                FROM src
            ) t
            WHERE rn = 1
        )
        INSERT INTO listing_description (listing_id, lang, body)
        SELECT l.id, 'fr'::text, p.body
        FROM pick p
        JOIN listing l ON l.external_listing_id = p.ext_id
        ON CONFLICT (listing_id) DO UPDATE
          SET body = EXCLUDED.body,
              lang = 'fr';
    """))

def build_incoming_and_apply_scd2(conn: Connection, h):
    change_ts = ts_or_null("sc", h["change_date"])
    start_ts  = ts_or_null("sc", h["start_date"])
    valid_from_sql = f"COALESCE({change_ts}, {start_ts}, NOW())"

    price_sql     = num_nonneg("sc", h["price"])
    area_sql      = num_nonneg("sc", h["area"])
    site_area_sql = num_nonneg("sc", h["site_area"])
    floor_sql     = floor_bounded("sc", h["floor"], -5, 300)
    room_sql      = smallint_nonneg("sc", h["room_count"])
    balc_sql      = smallint_nonneg("sc", h["balcony_count"])
    terr_cnt_sql  = smallint_nonneg("sc", h["terrace_count"])
    terr_area_sql = num_nonneg("sc", h["terrace_area"])

    conn.execute(text("DROP TABLE IF EXISTS stg_listing_incoming"))
    conn.execute(text(f"""
        CREATE TEMP TABLE stg_listing_incoming AS
        SELECT
          l.id AS listing_id,
          {valid_from_sql} AS valid_from,
          {change_ts}      AS source_change_ts,
          {price_sql}      AS price,
          {area_sql}       AS area,
          {site_area_sql}  AS site_area,
          {floor_sql}      AS floor,
          {room_sql}       AS room_count,
          {balc_sql}       AS balcony_count,
          {terr_cnt_sql}   AS terrace_count,
          {terr_area_sql}  AS terrace_area
        FROM stg_clean_csv sc
        JOIN listing l ON l.external_listing_id = NULLIF(btrim(sc.{h['listing_id']}), '')::text;
    """))

    inc = conn.execute(text("SELECT COUNT(*) FROM stg_listing_incoming")).scalar_one()
    t_nonnull = conn.execute(text("SELECT COUNT(*) FROM stg_listing_incoming WHERE terrace_area IS NOT NULL")).scalar_one()
    LOG.info("incoming rows: %s | terrace_area NOT NULL: %s", inc, t_nonnull)

    # Delta vs current
    conn.execute(text("DROP TABLE IF EXISTS stg_delta"))
    conn.execute(text("""
        CREATE TEMP TABLE stg_delta AS
        SELECT inc.*
        FROM stg_listing_incoming inc
        LEFT JOIN listing_version cur
          ON cur.listing_id = inc.listing_id AND cur.valid_to IS NULL
        WHERE cur.listing_id IS NULL
           OR cur.price         IS DISTINCT FROM inc.price
           OR cur.area          IS DISTINCT FROM inc.area
           OR cur.site_area     IS DISTINCT FROM inc.site_area
           OR cur.floor         IS DISTINCT FROM inc.floor
           OR cur.room_count    IS DISTINCT FROM inc.room_count
           OR cur.balcony_count IS DISTINCT FROM inc.balcony_count
           OR cur.terrace_count IS DISTINCT FROM inc.terrace_count
           OR cur.terrace_area  IS DISTINCT FROM inc.terrace_area
    """))
    delta = conn.execute(text("SELECT COUNT(*) FROM stg_delta")).scalar_one()
    LOG.info("delta rows (SCD2): %s", delta)

    if delta > 0:
        conn.execute(text("""
            UPDATE listing_version t
            SET valid_to = d.valid_from
            FROM stg_delta d
            WHERE t.listing_id = d.listing_id
              AND t.valid_to IS NULL
              AND t.valid_from <= d.valid_from;
        """))
        conn.execute(text("""
            INSERT INTO listing_version (
              listing_id, valid_from, valid_to,
              price, area, site_area, floor, room_count,
              balcony_count, terrace_count, terrace_area, source_change_ts
            )
            SELECT
              d.listing_id, d.valid_from, NULL,
              d.price, d.area, d.site_area, d.floor, d.room_count,
              d.balcony_count, d.terrace_count, d.terrace_area, d.source_change_ts
            FROM stg_delta d
            WHERE NOT EXISTS (
              SELECT 1 FROM listing_version t
              WHERE t.listing_id = d.listing_id
                AND t.valid_to IS NULL
            );
        """))
    else:
        LOG.info("No SCD changes detected.")

def refresh_snapshot(conn: Connection):
    try:
        LOG.info("Refreshing listing_current CONCURRENTLY")
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY listing_current;"))
    except SQLAlchemyError as e:
        LOG.warning("Concurrent refresh failed (%s). Fallback to regular refresh.", e)
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(text("REFRESH MATERIALIZED VIEW listing_current;"))

def diagnostics(conn: Connection):
    scd_cnt   = conn.execute(text("SELECT COUNT(*) FROM listing_version")).scalar_one()
    cur_cnt   = conn.execute(text("SELECT COUNT(*) FROM listing_current")).scalar_one()
    terr_ok   = conn.execute(text("SELECT COUNT(*) FROM listing_version WHERE terrace_area IS NOT NULL")).scalar_one()
    terr_null = conn.execute(text("SELECT COUNT(*) FROM listing_version WHERE terrace_area IS NULL")).scalar_one()
    cur_ok    = conn.execute(text("SELECT COUNT(*) FROM listing_current WHERE terrace_area IS NOT NULL")).scalar_one()
    cur_null  = conn.execute(text("SELECT COUNT(*) FROM listing_current WHERE terrace_area IS NULL")).scalar_one()
    desc_cnt  = conn.execute(text("SELECT COUNT(*) FROM listing_description")).scalar_one()
    LOG.info("listing_version rows: %s | listing_current rows: %s", scd_cnt, cur_cnt)
    LOG.info("terrace_area listing_version -> NOT NULL: %s | NULL: %s", terr_ok, terr_null)
    LOG.info("terrace_area listing_current -> NOT NULL: %s | NULL: %s", cur_ok, cur_null)
    LOG.info("listing_description rows: %s", desc_cnt)

# ---------------- Pipeline ----------------

def run_pipeline():
    LOG.info("Loader start (UTC): %s", datetime.utcnow().isoformat() + "Z")
    header_cols = read_csv_header(CLEANED_CSV)
    h = assert_expected_headers(header_cols)  # dict logical->actual header spelling

    with connect() as conn:
        # open transaction explicitly to avoid autobegin conflicts
        with conn.begin():
            # 1) stage
            create_temp_staging(conn, header_cols)
            copy_csv_into_staging(conn, CLEANED_CSV, header_cols)
            total = conn.execute(text("SELECT COUNT(*) FROM stg_clean_csv")).scalar_one()
            LOG.info("stg_clean_csv rows: %s", total)

            # 2) dims + location
            upsert_dimensions(conn, h)
            upsert_cities_zipcodes_locations(conn, h)

            # 3) listing master
            upsert_listings(conn, h)

            # 4) descriptions
            upsert_descriptions(conn, h)

            # 5) SCD2
            build_incoming_and_apply_scd2(conn, h)

        # 6) MV refresh outside tx
        refresh_snapshot(conn)

        # 7) diagnostics
        diagnostics(conn)

    LOG.info("Loader complete")

# ---------------- Entrypoint ----------------

if __name__ == "__main__":
    run_pipeline()
