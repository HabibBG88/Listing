# -*- coding: utf-8 -*-
"""
Standalone Data-Quality Checker for Postgres (3NF-aware)

Checks:
  • listing_current not empty
  • Zip coverage (join listing_current -> location -> zipcode)
  • terrace_area non-null coverage
  • description presence (join listing_current.id -> listing_description.listing_id)
  • day-over-day volume drift using dq_daily_metrics

Exit codes:
  0 = OK
  1 = DQ issues found
  2 = configuration/connection error
"""

import os
import sys
import psycopg2
from datetime import date

DATABASE_URL = os.getenv("DATABASE_URL")

def _dsn(url: str) -> str:
    # Accept both SQLAlchemy and psycopg2-style URLs
    return url.replace("postgresql+psycopg2://", "postgresql://", 1)

def _fetch_one(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        row = cur.fetchone()
        return None if row is None else row[0]

def run_checks():
    issues = []
    if not DATABASE_URL:
        print("ERROR: Set DATABASE_URL", file=sys.stderr)
        sys.exit(2)

    try:
        with psycopg2.connect(_dsn(DATABASE_URL)) as conn:
            # 1) Non-empty current view?
            total = _fetch_one(conn, "SELECT COUNT(*) FROM listing_current")
            if not total:
                issues.append("listing_current is empty.")

            # 2) Zip coverage (listing_current -> location -> zipcode)
            zip_cov_sql = """
                SELECT AVG(
                         CASE WHEN z.code ~ '^[0-9]{5}$' THEN 1.0 ELSE 0.0 END
                       )::float
                  FROM listing_current lc
                  JOIN location loc ON loc.location_id = lc.location_id
                  JOIN zipcode  z   ON z.zipcode_id   = loc.zipcode_id
            """
            zip_cov = _fetch_one(conn, zip_cov_sql)
            if zip_cov is not None and zip_cov < 0.95:
                issues.append(f"Zip coverage low: {zip_cov:.2%} (<95%)")

            # 3) terrace_area completeness (on view directly)
            terr_nonnull = _fetch_one(
                conn, "SELECT COUNT(*) FROM listing_current WHERE terrace_area IS NOT NULL"
            )
            if terr_nonnull == 0:
                issues.append("All terrace_area values are NULL in listing_current.")

            # 4) description presence (join to listing_description via internal listing id)
            desc_nonempty_sql = """
                SELECT COUNT(*)
                  FROM listing_current lc
                  JOIN listing_description ld
                    ON ld.listing_id = lc.id
                 WHERE ld.body IS NOT NULL AND ld.body <> ''
            """
            desc_nonempty = _fetch_one(conn, desc_nonempty_sql)
            if desc_nonempty == 0:
                issues.append("All descriptions are empty or missing (listing_description.body).")

            # 5) Day-over-day drift (uses dq_daily_metrics you populate separately)
            today_total = _fetch_one(
                conn, "SELECT total_current FROM dq_daily_metrics WHERE metric_date = CURRENT_DATE"
            )
            yday_total = _fetch_one(
                conn, "SELECT total_current FROM dq_daily_metrics WHERE metric_date = CURRENT_DATE - INTERVAL '1 day'"
            )
            if today_total is not None and yday_total is not None:
                denom = max(1, yday_total)
                drift = abs(today_total - yday_total) / denom
                if drift > 0.50:
                    issues.append(
                        f"Total listings drift >50% (yesterday={yday_total}, today={today_total})."
                    )

    except Exception as e:
        print(f"ERROR: DQ checks could not run: {e}", file=sys.stderr)
        sys.exit(2)

    # Report
    if issues:
        print("❌ Data Quality Issues Detected:")
        for i in issues:
            print(" -", i)
        sys.exit(1)
    else:
        print("✅ Data Quality Passed")
        sys.exit(0)

if __name__ == "__main__":
    run_checks()
