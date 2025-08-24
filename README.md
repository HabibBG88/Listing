
# Real Estate Listings Data Pipeline & API

## 📌 Overview
This project implements an **end-to-end data pipeline** for processing real estate listings:
- Data Investigation
- Data cleaning & transformation (from raw CSV).
- Database schema design (3NF + SCD2).
- Data loading with **upserts** and **slowly changing dimensions**.
- A REST API built with **FastAPI** to query the processed data.
- Data Quality checks (profiling, constraints, anomaly detection).

---

## 📂 Project Structure

```
project_root(Listing)/
├── data_exploration.py         # Data exploration & profiling (generates dq_report)
├── design_table.sql            # Database schema (3NF + SCD2)
├── cleaner.py                  # Cleans raw listings into normalized format
├── clean_artifacts_py/
│   ├── listings_cleaned.csv
│   └── cleaning_changelog.txt
├── loader.py/                  # Loads cleaned data into Postgres (pipeline)
│                 
├── api/
│   ├── main.py                 # FastAPI entrypoint
│   ├── deps.py                 # Database dependencies
│   ├── models.py               # Pydantic models (schemas)
│   ├── sql.py                  # SQLAlchemy Core table definitions & queries
│   ├── repository/
│   │   └── listings.py         # Query functions
│   └── routers/
│       └── listings.py         # API endpoints
├── Data_quality/
│   ├── dq_profile.sql          # Profiling metrics table
│   ├── dq_constraints.sql      # Hard constraints (DB-level checks)
│   └── dq_checks.py             # Python anomaly detection script
├── requirements.txt            # Requirements for the project
└── README.md                   # Documentation
              
```

---

## 🗄️ Database Design

The schema is **3NF with Slowly Changing Dimension (SCD2)** for listings.

### Tables:
- `transaction_type` → Lookup (SELL, RENT)
- `item_type` → (HOUSE, APARTMENT, …)
- `item_subtype` → Child of item_type (DUPLEX, LOFT…)
- `city` / `zipcode` / `location`
- `listing` → Master record (stable attributes, FK to dimensions)
- `listing_version` → SCD2 table (changing attributes: price, area, terrace…)
- `listing_description` → Optional long text (per language)
- `listing_current` → Materialized view exposing only current active version. Used for API performance 
- `listing_with_type` → View that extends listing with its corresponding item_type_id.

**Key relationships**:
- A `listing` has many `listing_versions`.
- A `listing` belongs to a `location`.
- A `location` links a `city` and a `zipcode`.

---

## ⚙️ Setup Instructions

### 1. Create Database Schema
In **Postbird** or via CLI:
```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f design_table.sql
```

### 2. Run Data Exploration
Generates a **data quality report**:
```bash
python data_exploration.py
```

Output: `dq_reports/`

### 3. Clean Raw Data
```bash
export CLEANED_CSV="/path/to/clean_artifacts_py/listings_cleaned.csv"
python cleaner.py
```
Artifacts:
- `listings_cleaned.csv`
- `cleaning_changelog.txt`

### 4. Load Data
```bash
export DATABASE_URL="postgresql+psycopg2://user:pass@localhost:5432/testdb"
python loader.py
```

### 5. Run API
```bash
uvicorn api.main:app --reload --port 8000
```
Then visit: [http://localhost:8000]

---

## 📊 Data Quality

### Profiling (daily metrics)
```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f Data_quality/dq_profile.sql 
Generates a **dq_daily_metrics table**:
```

### Constraints (hard checks)
```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f Data_quality/dq_constraints.sql
```

### Python DQ Checks
```bash
python Data_quality/dq_checks.py
```

---

## 🚀 Future Improvements
- Add **Prometheus/Grafana** to monitor API-level anomalies (latency, error rates).
- Extend schema to support **multi-country data**.
- Add **CI/CD pipelines** for automated testing & deployment.

---

## ✅ Testing the API

Example request:
```bash
curl "http://localhost:8000/api/listings?transaction_type=SELL&item_type=HOUSE&min_price=200000&max_price=500000&page=1&page_size=10&sort_by=area&sort_dir=desc"
```

---

## 👨‍💻 Key Design Decisions
- **3NF schema with SCD2**: balances normalization + history tracking.
- **FastAPI**: modern async web framework with OpenAPI docs.
- **SQLAlchemy Core**: explicit SQL control, safe and performant.
- **psycopg2**: reliable Postgres driver.
- **Data Quality modules**: detect anomalies early (zip coverage, price drift).

```

