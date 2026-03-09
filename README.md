# Time Series Data Pipeline

Household Electric Power Consumption — EDA, Databases, REST API, and Forecasting

**Group 12** | Machine Learning Pipeline | 8 March 2026  
**Repository:** https://github.com/Chol1000/formative1-time-series-pipeline.git

---

## Problem Statement

This project addresses the problem of one-minute-ahead residential electricity demand forecasting. Given all household electricity measurements available up to time $t$, the objective is to accurately predict the global active power (kW) at time $t+1$. Accurate short-term load forecasting is fundamental to smart-grid demand-response systems, enabling energy providers to balance supply against demand, schedule storage dispatch, and reduce grid stress during peak periods.

---

## Dataset Justification

The [UCI Individual Household Electric Power Consumption dataset](https://www.kaggle.com/datasets/uciml/electric-power-consumption-data-set/data) was selected because it offers nearly four years of one-minute-resolution data spanning December 2006 to November 2010, yielding over two million observations. This volume is sufficient to capture daily, weekly, and seasonal demand cycles simultaneously. The target variable, `global_active_power` (kW), is a direct and physically interpretable measure of household energy demand with no proxy encoding required. The dataset also records seven simultaneous measurement channels — voltage, current intensity, reactive power, and three sub-metering circuits — enabling multivariate correlation and leakage analysis. It is a widely used benchmark in time-series forecasting research, allowing result comparison against published baselines.

| Property | Value |
|----------|-------|
| Source | [UCI ML Repository via Kaggle](https://www.kaggle.com/datasets/uciml/electric-power-consumption-data-set/data) |
| File | `household_power_consumption.txt` (excluded from repo — 127 MB) |
| Time range | 16 Dec 2006 – 26 Nov 2010 (4 years) |
| Frequency | 1-minute intervals |
| Records | 2,075,259 (after resampling to complete 1-min grid) |
| Target | `global_active_power` (kW) |
| Channels | Voltage (V), Global Intensity (A), Reactive Power (kVAR), Sub-metering 1/2/3 (Wh) |

> The raw file is excluded from version control (127 MB exceeds GitHub's file size limit).  
> Use the provided `download_data.py` script to fetch it automatically — see the Quick Start section below.

---

## Project Overview

The pipeline is divided into four sequential tasks:

| Task | Description |
|------|-------------|
| Task 1 | EDA, preprocessing, 6 analytical questions with visualisations, and ML model training |
| Task 2 | MySQL (4 tables) and MongoDB database design, ERD, schema scripts, and query execution |
| Task 3 | FastAPI REST API with full CRUD and time-series endpoints backed by MySQL and MongoDB |
| Task 4 | End-to-end prediction script — fetches from the API, preprocesses, loads the trained model, and forecasts |

---

## Repository Structure

```
TimeSeriesDataPipeline/
├── download_data.py                   # Run this first — auto-downloads the 127 MB dataset
├── requirements.txt
├── README.md
├── task1_eda/
│   └── task1_notebook.ipynb           # EDA, 6 analytical questions, 3 ML experiments
├── task2_databases/
│   ├── schema.sql                     # MySQL DDL (4 tables)
│   ├── collection_schema.js           # MongoDB collection schema documentation
│   ├── query_templates.js             # MongoDB query templates documentation
│   ├── sql_database.py                # MySQL pipeline — schema, data load, 7 queries, ERD
│   ├── mongodb_implementation.py      # MongoDB pipeline — documents, 5 queries, JSON fallback
│   └── task2_main.py                  # Entry point — runs SQL then MongoDB pipeline
├── task3_api/
│   ├── api.py                         # FastAPI CRUD + time-series endpoints (port 8000)
│   └── test_api.py                    # Automated test suite (24 tests)
├── task4_prediction/
│   └── prediction_script.py           # End-to-end prediction + 12-step forecast
├── data/
│   ├── best_model.joblib              # Saved Random Forest model (best by RMSE)
│   ├── best_scaler.joblib             # Fitted MinMaxScaler
│   ├── feature_columns.json           # 15 feature names
│   └── experiment_results.json        # Experiment comparison table (4 experiments)
└── outputs/
    ├── missing_values.png
    ├── distributions.png
    ├── q1_trend_seasonality.png
    ├── q2_daily_weekly_patterns.png
    ├── q3_lag_correlations.png
    ├── q4_moving_averages.png
    ├── q5_submetering_analysis.png
    ├── q6_external_correlations.png
    ├── 10_model_comparison_bar.png
    ├── 11_actual_vs_predicted.png
    ├── 12_feature_importances.png
    ├── erd_diagram.png
    └── task2/
        ├── schema.sql
        ├── erd_diagram.png
        ├── sql_query_results.txt
        ├── mongodb_collection_design.txt
        ├── sample_documents.json
        └── mongodb_query_results.txt
```

---

## Quick Start (for first-time users)

> **The raw dataset is not stored in the repository.**  
> GitHub enforces a 100 MB file size limit and the dataset is 127 MB.  
> The script `download_data.py` (included in the repo root) fetches it automatically from the UCI ML Repository.

### Step-by-step setup

```bash
# 1. Clone the repository
git clone https://github.com/Chol1000/formative1-time-series-pipeline.git
cd formative1-time-series-pipeline

# 2. Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate        # macOS / Linux
# venv\Scripts\activate         # Windows

# 3. Install all dependencies
pip install -r requirements.txt

# 4. Download the dataset (one-time, ~127 MB)
python download_data.py
```

**What `download_data.py` does:**
- Downloads `household_power_consumption.zip` from the UCI ML Repository
- Extracts `household_power_consumption.txt` into the project root
- Deletes the zip file after extraction
- Prints a progress bar during download
- If the file already exists, it skips the download safely

**If the automatic download fails**, download the file manually from:  
https://www.kaggle.com/datasets/uciml/electric-power-consumption-data-set/data  
and place `household_power_consumption.txt` in the project root.

```bash
# 5. Create the MySQL database (one-time)
mysql -u root -e "CREATE DATABASE IF NOT EXISTS household_power;"
```

After these five steps all four tasks can be run in order.

---

## Prerequisites

- Python 3.8+
- MySQL 8.0+ running on `localhost:3306`, root user, no password
- MongoDB (optional) — `mongodb://localhost:27017`. Task 2 falls back to JSON simulation mode automatically if unavailable.

---

## Task 1 — EDA, Preprocessing and Model Training

### How to Run

Open `task1_eda/task1_notebook.ipynb` in VS Code or Jupyter and run all cells from top to bottom.

All plots are saved to `outputs/` and all model artefacts to `data/` automatically.

### What it Does

**Section 1A — Understanding the Dataset**

- Loads and parses the raw semicolon-delimited file
- Reports time range (Dec 2006 – Nov 2010), granularity (1-min), and dataset coverage (98.8%)
- Missing value audit: 25,979 rows (1.25%) — meter-offline gaps across all columns simultaneously
- Linear interpolation chosen over forward fill, mean imputation, and row deletion — preserves 1-minute regularity required by lag features
- Resamples to a complete 1-minute grid (2,075,259 rows after interpolation)
- Saves statistical distributions (mean, std, skewness, kurtosis) for all 7 numeric columns

**Section 1B — Six Analytical Questions**

| # | Question | Key Finding |
|---|----------|-------------|
| Q1 | Does consumption show long-term trend or seasonality? | No trend 2007–2010; peak Dec ~1.49 kW, trough Aug ~0.58 kW (amplitude 0.91 kW) |
| Q2 | What are daily and weekly usage patterns? | Evening peak at 20:00 (1.89 kW); weekends 17.9% higher than weekdays |
| Q3 | Do lagged values predict current consumption? | 1-min lag r = 0.9682; drops below 0.5 only at 1-hour horizon — justifies lag_1/5/15/60 features |
| Q4 | Does a moving average reveal hidden cycles? | 7/30/90-day MAs expose seasonal pattern; highest volatility Feb, lowest Jul |
| Q5 | How do sub-metering circuits relate to total power? | Sub-meter 3 (HVAC/water heater) highest correlation (r = 0.64) |
| Q6 | Do voltage, intensity, reactive power correlate with active power? | Intensity r = +0.999 — voltage and reactive power excluded to prevent data leakage |

**Section 1C — Model Training and Experiments**

- Split: strict chronological 80/20 — Train: 1,660,159 rows / Test: 415,040 rows
- CV: `TimeSeriesSplit(n_splits=3)` on 100K-row chronological tail
- Tuning: `RandomizedSearchCV` — 20 iterations (RF), 25 iterations (GB)

| Model | MAE (kW) | RMSE (kW) | MAPE (%) | R² |
|-------|----------|-----------|----------|----|
| Naive Persistence (baseline) | 0.0692 | 0.2170 | 7.06 | 0.9387 |
| Linear Regression | 0.0831 | 0.2161 | 9.89 | 0.9392 |
| **Random Forest (tuned)** | **0.0694** | **0.2133** | **6.97** | **0.9408** |
| Gradient Boosting (tuned) | 0.0706 | 0.2153 | 7.05 | 0.9397 |

Best RF hyperparameters: `n_estimators=100, max_depth=8, min_samples_split=5, min_samples_leaf=2, max_features=0.7`

**15 leakage-free features:**

```
Cyclic temporal : hour_sin, hour_cos, dow_sin, dow_cos, month_sin, month_cos
Lags            : lag_1, lag_5, lag_15, lag_60
Momentum diffs  : diff_1, diff_5
Moving averages : ma_10, ma_60
Rolling std     : rolling_std_10
```

All lag and rolling operations use `.shift(1)` — no value at time t is used to predict t.

**Artefacts saved:**

```
data/best_model.joblib        RandomForestRegressor
data/best_scaler.joblib       MinMaxScaler
data/feature_columns.json     Feature list (15 names)
data/experiment_results.json  All 4 experiment rows
```

---

## Task 2 — Database Design and Implementation

### Prerequisites

```bash
mysql -u root -e "CREATE DATABASE IF NOT EXISTS household_power;"
```

### How to Run

```bash
python task2_databases/task2_main.py
```

Expected runtime: 5–15 minutes (2,075,259 rows inserted in 5,000-row chunks).

### MySQL — 4 Tables

```
households ──┬──> measurements ──> sub_metering
             └──> hourly_aggregates
```

| Table | Rows | Description |
|-------|------|-------------|
| `households` | 1 | Household metadata (name, location, area_sqm, occupants) |
| `measurements` | 2,075,259 | Per-minute readings — active/reactive power, voltage, intensity |
| `sub_metering` | 2,075,259 | 1:1 with measurements — Kitchen, Laundry/AC, Water Heater (Wh) |
| `hourly_aggregates` | ~34,588 | Pre-aggregated hourly stats — avg, max, min, total, reading count |

**7 SQL queries executed and saved to `outputs/task2/sql_query_results.txt`:**

1. Latest 10 measurements (ORDER BY datetime DESC)
2. Hourly averages for date range 2006-12-16 to 2006-12-19
3. JOIN — measurements + sub-metering (first 15 rows)
4. Top-10 peak demand hours (from `hourly_aggregates`)
5. Monthly consumption trend (~48 months)
6. Sub-metering energy share per appliance group
7. Day-of-week consumption pattern

### MongoDB — 2 Collections

**`power_readings`** — one document per measurement (50,000 sampled documents):

```json
{
  "measurement_id": 1,
  "household_id": 1,
  "household_info": { "name": "Household A", "location": "Sceaux, France" },
  "timestamp": "2006-12-16 17:24:00",
  "date": "2006-12-16",
  "hour": 17,
  "day_of_week": 5,
  "global_active_power": 4.216,
  "sub_metering": [
    { "meter_id": 1, "name": "Kitchen",      "consumption_wh": 0.0 },
    { "meter_id": 2, "name": "Laundry / AC", "consumption_wh": 1.0 },
    { "meter_id": 3, "name": "Water Heater", "consumption_wh": 17.0 }
  ],
  "total_sub_metering_wh": 18.0
}
```

Indexes: `{household_id, timestamp}`, `{timestamp}`, `{date}`, `{hour}`

**`daily_summaries`** — 1,425 day-records built via MongoDB `$out` aggregation pipeline.

**5 MongoDB queries executed and saved to `outputs/task2/mongodb_query_results.txt`:**

1. Latest 10 readings (sort timestamp DESC)
2. Date range query (2006-12-16 to 2006-12-19)
3. Hourly aggregation (`$group` by hour)
4. Sub-metering breakdown (`$unwind` + `$group` by appliance name)
5. Daily summary from `daily_summaries` collection

> No MongoDB server? The script auto-detects this and falls back to JSON simulation mode. All 5 queries run against in-memory Python structures and produce identical output.

**Outputs saved automatically:**

```
outputs/task2/schema.sql                    MySQL DDL script
outputs/task2/erd_diagram.png               Entity-Relationship Diagram
outputs/task2/sql_query_results.txt         All 7 SQL query results
outputs/task2/mongodb_collection_design.txt Collection schema + query templates
outputs/task2/sample_documents.json         3 sample documents
outputs/task2/mongodb_query_results.txt     All 5 MongoDB query results
```

---

## Task 3 — REST API (FastAPI)

### How to Run

```bash
# Terminal 1 — start the server
python task3_api/api.py
# Server  : http://localhost:8000
# Swagger : http://localhost:8000/docs

# Terminal 2 — run automated tests
python task3_api/test_api.py
# Expected: 24/24 PASS
```

On startup the API seeds an in-memory MongoDB store with 1,000 step-sampled documents from MySQL, spanning the full 4-year dataset.

### SQL Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Health check |
| POST | `/sql/households` | Create a household |
| GET | `/sql/households` | List all households |
| GET | `/sql/households/{id}` | Get household by ID |
| PUT | `/sql/households/{id}` | Update household |
| DELETE | `/sql/households/{id}` | Delete household |
| POST | `/sql/measurements` | Create a measurement |
| GET | `/sql/measurements` | List measurements (paginated) |
| GET | `/sql/measurements/{id}` | Get measurement by ID |
| PUT | `/sql/measurements/{id}` | Update measurement |
| DELETE | `/sql/measurements/{id}` | Delete measurement |
| GET | `/sql/latest` | Latest measurement record |
| GET | `/sql/date-range` | Records between `start_date` and `end_date` |
| GET | `/sql/hourly-stats` | Hourly averages across the dataset |
| GET | `/sql/monthly-trend` | Monthly average power trend |
| GET | `/sql/sub-metering` | Sub-metering energy breakdown |

### MongoDB Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/mongo/measurements` | Create a document |
| GET | `/mongo/measurements` | List documents (paginated) |
| GET | `/mongo/measurements/{id}` | Get document by ID |
| PUT | `/mongo/measurements/{id}` | Update document |
| DELETE | `/mongo/measurements/{id}` | Delete document |
| GET | `/mongo/latest` | Latest document by timestamp |
| GET | `/mongo/date-range` | Documents between two dates |
| GET | `/mongo/hourly-stats` | Hourly average stats |
| GET | `/mongo/daily-summary` | Daily summary (last N days) |

### Example Requests

```bash
# Health check
curl http://localhost:8000/

# Create a household
curl -X POST http://localhost:8000/sql/households \
  -H "Content-Type: application/json" \
  -d '{"household_name":"Apartment B","location":"Lyon, France","area_sqm":72.0,"occupants":2}'

# Get records for a date range
curl "http://localhost:8000/sql/date-range?start_date=2006-12-16&end_date=2006-12-17&limit=100"

# Latest MongoDB document
curl http://localhost:8000/mongo/latest
```

---

## Task 4 — Prediction and Forecast Script

### Prerequisites

Task 1 must be completed (model artefacts in `data/`) and the Task 3 API must be running.

```bash
python task3_api/api.py &
```

### How to Run

```bash
# Full pipeline — fetches live from the API
python task4_prediction/prediction_script.py
```

If the API is unavailable, the script automatically falls back to MySQL, then to the raw `.txt` file.

### Pipeline Steps

| Step | Action | Detail |
|------|--------|--------|
| 1 — Fetch | Pull 500 records from `GET /sql/date-range` | Falls back to MySQL then raw `.txt` if API unreachable |
| 2 — Preprocess | Interpolate and build 15 features | Mirrors Task 1 feature pipeline; first 60 rows dropped for lag_60 warm-up |
| 3 — Load Model | Restore `best_model.joblib` and `best_scaler.joblib` | RandomForestRegressor + MinMaxScaler |
| 4 — Predict | Score 440 records against real ground truth | MAE, RMSE, MAPE, R² |
| 5 — Forecast | 12-step autoregressive forecast | Each prediction feeds back as `lag_1`; temporal features updated per minute |

### Sample Output

```
STEP 1: Source           : Task 3 REST API  (GET /sql/date-range)
        Records fetched  : 500  (2006-12-16 17:24 -> 2006-12-17 01:43)

STEP 2: Records processed : 440  (60 lag warm-up rows removed)
        Features          : 15

STEP 3: Model  : best_model.joblib  (RandomForestRegressor)
        Scaler : best_scaler.joblib (MinMaxScaler)

STEP 4: MAE   = 0.4974 kW
        RMSE  = 0.8114 kW
        MAPE  = 36.39 %
        R2    = 0.3263

STEP 5:  t+ 1min  [2006-12-17 01:44]  2.6218 kW
         t+ 6min  [2006-12-17 01:49]  2.5967 kW
         t+12min  [2006-12-17 01:55]  2.5827 kW
```

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `mysql.connector.errors.DatabaseError` | Ensure MySQL is running and the `household_power` database exists |
| Port 8000 already in use | `lsof -ti:8000 \| xargs kill -9` |
| `FileNotFoundError: best_model.joblib` | Run Task 1 notebook first to train and save model artefacts |
| Task 4 connection refused | Start the API (Task 3) first |
| Task 2 MongoDB connection failed | Expected if no server running — simulation mode produces identical results |

---

## References

- [UCI Household Electric Power Consumption Dataset — Kaggle](https://www.kaggle.com/datasets/uciml/electric-power-consumption-data-set/data)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Scikit-learn Documentation](https://scikit-learn.org/)
- [MongoDB Aggregation Pipeline](https://www.mongodb.com/docs/manual/core/aggregation-pipeline/)
