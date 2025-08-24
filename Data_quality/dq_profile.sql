-- dq_profile.sql
-- Daily profiling metrics for the 3NF + SCD2 design

-- 1) Metrics table (idempotent)
CREATE TABLE IF NOT EXISTS dq_daily_metrics (
  metric_date            date    PRIMARY KEY,
  total_current          bigint  NOT NULL,
  price_p50              numeric,
  price_p95              numeric,
  area_p50               numeric,
  area_p95               numeric,
  zip5_coverage          numeric,      -- fraction [0,1]
  terrace_area_nonnull   bigint,
  desc_nonnull           bigint
);

-- 2) Upsert today's metrics
INSERT INTO dq_daily_metrics AS m (
  metric_date,
  total_current,
  price_p50, price_p95,
  area_p50,  area_p95,
  zip5_coverage,
  terrace_area_nonnull,
  desc_nonnull
)
SELECT
  CURRENT_DATE AS metric_date,

  -- counts are over the current snapshot
  COUNT(*) AS total_current,

  -- robust distribution metrics (NULLs are ignored by percentile_cont)
  PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY lc.price) AS price_p50,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY lc.price) AS price_p95,
  PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY lc.area)  AS area_p50,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY lc.area)  AS area_p95,

  -- zipcode coverage: join via location -> zipcode, test for strict 5 digits
  AVG(
    CASE WHEN z.code ~ '^[0-9]{5}$' THEN 1.0 ELSE 0.0 END
  ) AS zip5_coverage,

  -- terrace area completeness in the current snapshot
  SUM(CASE WHEN lc.terrace_area IS NOT NULL THEN 1 ELSE 0 END) AS terrace_area_nonnull,

  -- description availability (any non-empty body) – independent count
  COALESCE(
    (SELECT COUNT(*)
     FROM listing_description ld
     WHERE ld.body IS NOT NULL AND ld.body <> ''),
    0
  ) AS desc_nonnull

FROM listing_current lc
JOIN location  loc ON loc.location_id  = lc.location_id
JOIN zipcode   z   ON z.zipcode_id     = loc.zipcode_id
-- no WHERE: profile the entire current snapshot

ON CONFLICT (metric_date) DO UPDATE SET
  total_current          = EXCLUDED.total_current,
  price_p50              = EXCLUDED.price_p50,
  price_p95              = EXCLUDED.price_p95,
  area_p50               = EXCLUDED.area_p50,
  area_p95               = EXCLUDED.area_p95,
  zip5_coverage          = EXCLUDED.zip5_coverage,
  terrace_area_nonnull   = EXCLUDED.terrace_area_nonnull,
  desc_nonnull           = EXCLUDED.desc_nonnull;
