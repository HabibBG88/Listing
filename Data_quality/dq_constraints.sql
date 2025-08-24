-- dq_constraints.sql
-- Strong, schema-level data-quality checks for the 3NF + SCD2 design.
-- Run once after schema is created (or re-run any time; it's idempotent).
-- psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f dq_constraints.sql

------------------------------------------------------------
-- 0) Helper: add a constraint only if it doesn't already exist
------------------------------------------------------------
-- Usage:
--   SELECT ensure_check_constraint('schema.table','constraint_name','CHECK (...) [NOT VALID]');
--
CREATE OR REPLACE FUNCTION ensure_check_constraint(p_tbl regclass, p_conname text, p_check_sql text)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = p_tbl
      AND conname  = p_conname
  ) THEN
    EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %I %s', p_tbl, p_conname, p_check_sql);
    -- Validate in a separate statement so the NOT VALID variant is supported as well
    IF position('NOT VALID' in upper(p_check_sql)) > 0 THEN
      EXECUTE format('ALTER TABLE %s VALIDATE CONSTRAINT %I', p_tbl, p_conname);
    END IF;
  END IF;
END $$;

------------------------------------------------------------
-- 1) Zip code format (lives in zipcode table)
------------------------------------------------------------
SELECT ensure_check_constraint(
  'public.zipcode',
  'chk_zipcode_code_5digits',
  'CHECK (code ~ ''^[0-9]{5}$'') NOT VALID'
);

------------------------------------------------------------
-- 2) Listing (stable attributes) – build_year plausibility
------------------------------------------------------------
SELECT ensure_check_constraint(
  'public.listing',
  'chk_listing_build_year_range',
  'CHECK (build_year IS NULL OR build_year BETWEEN 1800 AND 2100) NOT VALID'
);

------------------------------------------------------------
-- 3) Listing versions (mutable attributes) – numeric sanity
--    These mirror the rules you already use in cleaning/loader.
------------------------------------------------------------

-- Price ≥ 0
SELECT ensure_check_constraint(
  'public.listing_version',
  'chk_lv_price_nonneg',
  'CHECK (price IS NULL OR price >= 0) NOT VALID'
);

-- Built area ≥ 0
SELECT ensure_check_constraint(
  'public.listing_version',
  'chk_lv_area_nonneg',
  'CHECK (area IS NULL OR area >= 0) NOT VALID'
);

-- Site/plot area ≥ 0
SELECT ensure_check_constraint(
  'public.listing_version',
  'chk_lv_site_area_nonneg',
  'CHECK (site_area IS NULL OR site_area >= 0) NOT VALID'
);

-- Terrace area ≥ 0
SELECT ensure_check_constraint(
  'public.listing_version',
  'chk_lv_terrace_area_nonneg',
  'CHECK (terrace_area IS NULL OR terrace_area >= 0) NOT VALID'
);

-- Floor in plausible bounds (e.g., basements to skyscrapers)
SELECT ensure_check_constraint(
  'public.listing_version',
  'chk_lv_floor_range',
  'CHECK (floor IS NULL OR floor BETWEEN -5 AND 300) NOT VALID'
);

-- Room / balcony / terrace counts ≥ 0
SELECT ensure_check_constraint(
  'public.listing_version',
  'chk_lv_room_count_nonneg',
  'CHECK (room_count IS NULL OR room_count >= 0) NOT VALID'
);

SELECT ensure_check_constraint(
  'public.listing_version',
  'chk_lv_balcony_count_nonneg',
  'CHECK (balcony_count IS NULL OR balcony_count >= 0) NOT VALID'
);

SELECT ensure_check_constraint(
  'public.listing_version',
  'chk_lv_terrace_count_nonneg',
  'CHECK (terrace_count IS NULL OR terrace_count >= 0) NOT VALID'
);


