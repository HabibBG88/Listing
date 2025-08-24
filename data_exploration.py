# ================================================================
# Data Quality Investigation
# ================================================================
import pandas as pd
import numpy as np
from pathlib import Path

# --------------------
# Configuration
# --------------------
DATA_PATH = "listings.csv"
REPORT_DIR = Path("dq_reports")
REPORT_DIR.mkdir(parents=True, exist_ok=True)

# --------------------
# 1) Load & Basic Structure
# --------------------
df = pd.read_csv(DATA_PATH)
print("-- Loaded file:", DATA_PATH)
print("Shape (rows, cols):", df.shape)
print("\n.info():")
print(df.info(memory_usage="deep"))

print("\nPreview (head):")
print(df.head(5))

# Save head for quick reference
df.head(25).to_csv(REPORT_DIR / "sample_head.csv", index=False)

# --------------------
# 2) Datatypes & Cardinality
# --------------------
dtypes = df.dtypes.rename("dtype")
nunique = df.nunique(dropna=True).rename("nunique_non_null")
nulls = df.isna().sum().rename("null_count")
null_pct = (df.isna().mean() * 100).round(2).rename("null_pct")

schema_report = pd.concat([dtypes, nunique, nulls, null_pct], axis=1).sort_values("null_pct", ascending=False)
print("\n -- Schema report (top missing first):")
print(schema_report.head(20))
schema_report.to_csv(REPORT_DIR / "schema_report.csv")

# --------------------
# 3) Missingness Profile
# --------------------
missing_report = df.isna().sum().sort_values(ascending=False).to_frame("missing_count")
missing_report["missing_pct"] = (missing_report["missing_count"] / len(df) * 100).round(2)
print("\n -- Missingness (all columns):")
print(missing_report.head(30))
missing_report.to_csv(REPORT_DIR / "missingness_report.csv")

# --------------------
# 4) Duplicate Analysis
# --------------------
full_dup_count = df.duplicated().sum()
print(f"\n -- Full-row duplicates: {full_dup_count}")

dup_report = pd.DataFrame({"full_row_duplicates": [full_dup_count]})
# Business key: if a 'listing_id' column exists, validate uniqueness
if "listing_id" in df.columns:
    listing_dup = df["listing_id"].duplicated().sum()
    dup_report["listing_id_duplicates"] = [listing_dup]
    if listing_dup > 0:
        print(f" --listing_id duplicates detected: {listing_dup}")
        df[df["listing_id"].duplicated(keep=False)].sort_values("listing_id").to_csv(
            REPORT_DIR / "listing_id_duplicates.csv", index=False
        )
else:
    dup_report["listing_id_duplicates"] = [np.nan]

dup_report.to_csv(REPORT_DIR / "duplicate_report.csv", index=False)

# --------------------
# 5) Categorical Consistency & Inconsistencies
#    - low-cardinality object columns
#    - case/whitespace variants
# --------------------
cat_cols = df.select_dtypes(include=["object"]).columns.tolist()
cat_summary_rows = []
for col in cat_cols:
    nunq = df[col].nunique(dropna=True)
    # consider "categorical" if cardinality is manageable (heuristic: <= 1000)
    if nunq <= 1000:
        vc = df[col].astype(str).str.strip().value_counts(dropna=False)
        top10 = vc.head(10)
        cat_summary_rows.append({
            "column": col,
            "nunique": nunq,
            "top10_values": "; ".join([f"{k}:{v}" for k, v in top10.items()])
        })
cat_summary = pd.DataFrame(cat_summary_rows).sort_values("nunique", ascending=True)
print("\n -- Categorical summary (low-cardinality objects):")
print(cat_summary.head(30))
cat_summary.to_csv(REPORT_DIR / "categorical_summary.csv", index=False)

# Heuristic: boolean-like columns (names start with is_/has_) — check for mixed encodings
bool_like = [c for c in df.columns if c.lower().startswith(("is_", "has_"))]
bool_check_rows = []
for col in bool_like:
    vals = df[col].astype(str).str.strip().str.lower()
    sample = vals.value_counts(dropna=False).head(10)
    bool_check_rows.append({
        "column": col,
        "distinct_count": vals.nunique(dropna=False),
        "top_values": "; ".join([f"{k}:{v}" for k, v in sample.items()])
    })
bool_check = pd.DataFrame(bool_check_rows)
if not bool_check.empty:
    print("\n -- Boolean-like columns (distribution of encodings):")
    print(bool_check)
    bool_check.to_csv(REPORT_DIR / "boolean_like_check.csv", index=False)

# --------------------
# 6) Date Columns Validation (presence, parseability, range)
# --------------------
date_cols = [c for c in df.columns if "date" in c.lower()]
date_validation_rows = []
for col in date_cols:
    s = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True)
    date_validation_rows.append({
        "column": col,
        "nulls_after_parse": int(s.isna().sum()),
        "min": s.min(),
        "max": s.max()
    })
date_validation = pd.DataFrame(date_validation_rows)
if not date_validation.empty:
    print("\n -- Date columns validation:")
    print(date_validation)
    date_validation.to_csv(REPORT_DIR / "date_validation.csv", index=False)

# Optional cross-date logic: if both start/change exist, ensure start <= change (when both non-null)
if {"start_date", "change_date"}.issubset(df.columns):
    s = pd.to_datetime(df["start_date"], errors="coerce")
    c = pd.to_datetime(df["change_date"], errors="coerce")
    bad_order = ((s.notna()) & (c.notna()) & (s > c)).sum()
    print(f"\n -- Records with start_date > change_date: {bad_order}")
    pd.DataFrame({"start_date": s, "change_date": c})[s > c].to_csv(
        REPORT_DIR / "date_order_violations.csv", index=False
    )

# --------------------
# 7) Numeric Reasonableness & Outliers
# --------------------
num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
qc_rows = []
for col in num_cols:
    s = df[col]
    qc_rows.append({
        "column": col,
        "nulls": int(s.isna().sum()),
        "zeros": int((s == 0).sum()),
        "negatives": int((s < 0).sum()),
        "min": s.min(),
        "q1": s.quantile(0.25),
        "median": s.median(),
        "q3": s.quantile(0.75),
        "max": s.max(),
    })
qc = pd.DataFrame(qc_rows)
print("\n -- Numeric QC summary:")
print(qc.sort_values("nulls", ascending=False).head(30))
qc.to_csv(REPORT_DIR / "numeric_qc.csv", index=False)

# IQR outlier counts
iqr_rows = []
for col in num_cols:
    s = df[col].dropna()
    if s.empty: 
        iqr_rows.append({"column": col, "outlier_count": 0})
        continue
    q1, q3 = s.quantile(0.25), s.quantile(0.75)
    iqr = q3 - q1
    if iqr == 0:
        outlier_count = 0
    else:
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        outlier_count = ((s < lower) | (s > upper)).sum()
    iqr_rows.append({"column": col, "outlier_count": int(outlier_count)})
iqr_report = pd.DataFrame(iqr_rows).sort_values("outlier_count", ascending=False)
print("\n -- Outlier counts by IQR rule:")
print(iqr_report.head(30))
iqr_report.to_csv(REPORT_DIR / "iqr_outliers.csv", index=False)

# --------------------
# 8) Cross-field Business Logic Checks 
# --------------------
issues = []

# Non-positive measurable quantities (common in listings datasets)
for col in [c for c in num_cols if any(k in c.lower() for k in ["price", "area", "room", "floor", "count", "year"])]:
    s = df[col]
    # Common sense: price/area/room counts shouldn't be negative
    neg = int((s < 0).sum())
    if neg > 0:
        issues.append((col, f"negative values: {neg}"))

    # For counts (rooms/balconies/terrace), fractional values are suspicious
    if any(k in col.lower() for k in ["room", "count"]):
        frac = int(((s % 1) != 0).sum())
        if frac > 0:
            issues.append((col, f"non-integer counts: {frac}"))

    # Build year sanity check if plausible
    if "year" in col.lower():
        # Flag < 1800 or > current year+1 as suspicious (no timezone dependency here)
        suspicious = int(((s < 1800) | (s > 2100)).sum())
        if suspicious > 0:
            issues.append((col, f"suspicious years: {suspicious}"))

# Date logic already checked; add price/area related plausibility
for col in [c for c in num_cols if "price" in c.lower()]:
    s = df[col]
    zero_or_null = int((s.isna() | (s <= 0)).sum())
    if zero_or_null > 0:
        issues.append((col, f"zero/negative or null values: {zero_or_null}"))

for col in [c for c in num_cols if "area" in c.lower()]:
    s = df[col]
    non_positive = int((s <= 0).sum())
    if non_positive > 0:
        issues.append((col, f"non-positive areas: {non_positive}"))

# City/Zip consistency heuristic (if both columns exist)
if {"city", "zipcode"}.issubset(df.columns):
    # Treat zipcode as string to preserve leading zeros
    zip_as_str = df["zipcode"].astype(str)
    city_zip_card = df.groupby("city", dropna=False)["zipcode"].nunique(dropna=True).sort_values(ascending=False)
    city_zip_report = city_zip_card.to_frame("zipcode_cardinality_by_city")
    city_zip_report.to_csv(REPORT_DIR / "city_zip_cardinality.csv")
    # Flag city mapping to unusually many zipcodes (heuristic threshold)
    threshold = 50
    flagged = city_zip_card[city_zip_card > threshold]
    if not flagged.empty:
        issues.append(("city/zipcode", f"cities with >{threshold} zipcodes: {len(flagged)} (see city_zip_cardinality.csv)"))

# Persist issues
issues_df = pd.DataFrame(issues, columns=["field", "issue"]).drop_duplicates()
print("\n🔍 Cross-field issues:")
print(issues_df if not issues_df.empty else "No cross-field issues flagged by heuristics.")
issues_df.to_csv(REPORT_DIR / "cross_field_issues.csv", index=False)

# --------------------
# 9) Deliverables / Where to find reports
# --------------------
print("\n -- Reports written to:", REPORT_DIR)
print(" - schema_report.csv")
print(" - missingness_report.csv")
print(" - duplicate_report.csv")
print(" - categorical_summary.csv")
print(" - boolean_like_check.csv (if any)")
print(" - date_validation.csv (if any)")
print(" - date_order_violations.csv (if any)")
print(" - numeric_qc.csv")
print(" - iqr_outliers.csv")
print(" - city_zip_cardinality.csv (if city/zipcode present)")
print(" - cross_field_issues.csv")
print(" - sample_head.csv")
