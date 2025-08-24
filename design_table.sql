-- Create DB and schema=public
-- listings_scd.sql
-- SCD Type 2 table for real estate listings
-- Managed by ETL  

-- =========================
-- 1) Lookup Dimensions
-- =========================

CREATE TABLE IF NOT EXISTS public.transaction_type (
  transaction_type_id   SMALLSERIAL PRIMARY KEY,
  code                  TEXT UNIQUE NOT NULL,
  description           TEXT
);

CREATE TABLE IF NOT EXISTS public.item_type (
  item_type_id          SMALLSERIAL PRIMARY KEY,
  code                  TEXT UNIQUE NOT NULL,
  description           TEXT
);

CREATE TABLE IF NOT EXISTS public.item_subtype (
  item_subtype_id       SMALLSERIAL PRIMARY KEY,
  item_type_id          SMALLINT NOT NULL REFERENCES item_type(item_type_id),
  code                  TEXT NOT NULL,
  description           TEXT,
  UNIQUE (item_type_id, code)
);

CREATE TABLE IF NOT EXISTS public.city (
  city_id               BIGSERIAL PRIMARY KEY,
  name                  TEXT NOT NULL,
  region                TEXT,
  country_code          CHAR(2) DEFAULT 'FR'
);

CREATE TABLE IF NOT EXISTS public.zipcode (
  zipcode_id            BIGSERIAL PRIMARY KEY,
  code                  TEXT UNIQUE NOT NULL         -- normalized to 5 digits by loader
);

CREATE TABLE IF NOT EXISTS location (
  location_id           BIGSERIAL PRIMARY KEY,
  city_id               BIGINT NOT NULL REFERENCES city(city_id),
  zipcode_id            BIGINT NOT NULL REFERENCES zipcode(zipcode_id),
  UNIQUE (city_id, zipcode_id)
);

CREATE INDEX IF NOT EXISTS ix_location_city ON location(city_id);
CREATE INDEX IF NOT EXISTS ix_location_zip  ON location(zipcode_id);

-- =========================
-- 2) Listing Master
-- =========================

CREATE TABLE IF NOT EXISTS public.listing (
  id                    BIGSERIAL PRIMARY KEY,
  external_listing_id   TEXT UNIQUE NOT NULL,              -- match cleaner CSV "listing_id"
  transaction_type_id   SMALLINT NOT NULL REFERENCES transaction_type(transaction_type_id),
  item_subtype_id       SMALLINT NOT NULL REFERENCES item_subtype(item_subtype_id),
  location_id           BIGINT NOT NULL REFERENCES location(location_id),

  build_year            SMALLINT CHECK (build_year BETWEEN 1800 AND 2100),
  is_new_construction   BOOLEAN,
  has_passenger_lift    BOOLEAN,
  has_cellar            BOOLEAN,
  is_furnished          BOOLEAN,

  created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_listing_location         ON listing(location_id);
CREATE INDEX IF NOT EXISTS ix_listing_transaction_type ON listing(transaction_type_id);
CREATE INDEX IF NOT EXISTS ix_listing_subtype          ON listing(item_subtype_id);

-- Convenient derived item_type via view (subtype → type)
CREATE OR REPLACE VIEW public.listing_with_type AS
SELECT l.*,
       st.item_type_id
FROM listing l
JOIN item_subtype st ON st.item_subtype_id = l.item_subtype_id;

-- =========================
-- 3) Listing Versions (SCD-2)
-- =========================

CREATE TABLE IF NOT EXISTS public.listing_version (
  listing_version_id    BIGSERIAL PRIMARY KEY,
  listing_id            BIGINT NOT NULL REFERENCES listing(id) ON DELETE CASCADE,

  valid_from            TIMESTAMPTZ NOT NULL,
  valid_to              TIMESTAMPTZ,                       -- NULL => "current"

  price                 NUMERIC(12,2) CHECK (price >= 0),
  area                  NUMERIC(9,2)  CHECK (area  >= 0),
  site_area             NUMERIC(9,2)  CHECK (site_area >= 0),
  floor                 SMALLINT      CHECK (floor >= -5 AND floor <= 300),
  room_count            SMALLINT      CHECK (room_count >= 0),
  balcony_count         SMALLINT      CHECK (balcony_count >= 0),
  terrace_count         SMALLINT      CHECK (terrace_count >= 0),
  terrace_area          NUMERIC(9,2)  CHECK (terrace_area >= 0),

  source_change_ts      TIMESTAMPTZ
);

-- One current row per listing
CREATE UNIQUE INDEX IF NOT EXISTS ux_listing_version_current
  ON listing_version (listing_id)
  WHERE valid_to IS NULL;

-- Helpful filters
CREATE INDEX IF NOT EXISTS ix_listing_version_from  ON listing_version(valid_from DESC);
CREATE INDEX IF NOT EXISTS ix_listing_version_price ON listing_version(price);
CREATE INDEX IF NOT EXISTS ix_listing_version_area  ON listing_version(area);

-- =========================
-- 4) Descriptions (from description_fr in CSV)
-- =========================

CREATE TABLE IF NOT EXISTS public.listing_description (
  listing_id            BIGINT PRIMARY KEY REFERENCES listing(id) ON DELETE CASCADE,
  lang                  TEXT NOT NULL DEFAULT 'fr',
  body                  TEXT
);

-- =========================
-- 5) Materialized "current" snapshot
-- =========================

DROP MATERIALIZED VIEW IF EXISTS public.listing_current;
CREATE MATERIALIZED VIEW listing_current AS
SELECT
  l.id,
  l.external_listing_id,
  l.transaction_type_id,
  st.item_type_id,                  -- from subtype
  l.item_subtype_id,
  l.location_id,
  l.build_year,
  l.is_new_construction,
  l.has_passenger_lift,
  l.has_cellar,
  l.is_furnished,
  v.price,
  v.area,
  v.site_area,
  v.floor,
  v.room_count,
  v.balcony_count,
  v.terrace_count,
  v.terrace_area,
  v.valid_from
FROM listing l
JOIN listing_version v  ON v.listing_id = l.id AND v.valid_to IS NULL
JOIN item_subtype st    ON st.item_subtype_id = l.item_subtype_id;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX IF NOT EXISTS ux_listing_current_id ON listing_current (id);

-- Common query patterns
CREATE INDEX IF NOT EXISTS ix_listing_current_loc_price ON listing_current (location_id, price);
CREATE INDEX IF NOT EXISTS ix_listing_current_type      ON listing_current (item_type_id, transaction_type_id);

