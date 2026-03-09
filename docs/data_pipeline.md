# Data Pipeline — End-to-End Flow

This document traces the full lifecycle of data through the project, from the raw `.txt` file on disk to a live forecast.

---

## Overview

```
household_power_consumption.txt
        │
        ▼
┌─────────────────────────────────────┐
│  Task 1 — EDA & Model Training      │
│  task1_eda/task1_notebook.ipynb     │
│  • parse → interpolate → engineer   │
│  • train RF, GB, LR + baseline      │
│  • save model artefacts to data/    │
└─────────────┬───────────────────────┘
              │
              ▼
┌─────────────────────────────────────┐
│  Task 2 — Database Load             │
│  task2_databases/task2_main.py      │
│  • bulk-insert → MySQL (2.07M rows) │
│  • seed → MongoDB power_readings    │
│  • build daily_summaries via $out   │
└─────────────┬───────────────────────┘
              │
              ▼
┌─────────────────────────────────────┐
│  Task 3 — REST API                  │
│  task3_api/api.py  (port 8000)      │
│  • seed in-memory Mongo on startup  │
│  • expose SQL + MongoDB endpoints   │
└─────────────┬───────────────────────┘
              │
              ▼
┌─────────────────────────────────────┐
│  Task 4 — Prediction Script         │
│  task4_prediction/prediction_script │
│  • fetch 500 rows from /sql/date-   │
│    range (or MySQL / raw file)      │
│  • rebuild 15 features              │
│  • load model → evaluate + forecast │
└─────────────────────────────────────┘
```

---

## Stage 1 — Raw Data Ingestion (Task 1A)

**File:** `household_power_consumption.txt`  
**Format:** semicolon-delimited, 2,075,260 lines (1 header + 2,075,259 data rows)  
**Encoding:** UTF-8

### Parse

```python
df = pd.read_csv(
    "household_power_consumption.txt",
    sep=";",
    low_memory=False,
    na_values=["?"],
    parse_dates={"datetime": ["Date", "Time"]},
    dayfirst=True,
)
df = df.set_index("datetime")
```

`Date` and `Time` columns are merged into a single `datetime` index. Missing values (`?`) become `NaN`.

### Audit

| Check | Result |
|-------|--------|
| Time range | 2006-12-16 17:24 → 2010-11-26 21:02 |
| Expected 1-min rows (Dec 2006 – Nov 2010) | 2,075,259 |
| Actual rows in file | 2,049,280 |
| Missing rows (gaps in index) | 25,979 (1.25%) |
| Columns with NaN | All 7 numeric columns simultaneously |

Missing rows are **not scattered nulls** — they are complete time gaps where the meter was offline. All 7 columns are simultaneously absent.

### Impute

```python
df = df.resample("1min").asfreq()   # restore full 1-min grid
df = df.interpolate(method="time")  # fill gaps linearly by time
```

After resampling: **2,075,259 rows**, zero nulls.

**Why linear interpolation?**
- Maintains the 1-minute regularity required by lag features (`lag_60` needs exactly 60 rows back)
- Forward fill would propagate the last known value indefinitely, creating artificial plateaus
- Mean imputation would insert the global mean mid-consumption, distorting peak events
- Row deletion would discard 25,979 rows and create irregular time gaps

---

## Stage 2 — Feature Engineering (Task 1B / Task 4 Step 2)

The same 15-feature pipeline is applied in the Task 1 notebook **and** reproduced identically in `prediction_script.py` (via `build_features()`).

### Step-by-step

```python
df["hour"]       = df.index.hour
df["dow"]        = df.index.dayofweek
df["month"]      = df.index.month

# 1. Cyclic encodings
df["hour_sin"]  = np.sin(2 * np.pi * df["hour"]  / 24)
df["hour_cos"]  = np.cos(2 * np.pi * df["hour"]  / 24)
df["dow_sin"]   = np.sin(2 * np.pi * df["dow"]   / 7)
df["dow_cos"]   = np.cos(2 * np.pi * df["dow"]   / 7)
df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)

# 2. Lags (all shifted by at least 1 → no data leakage)
df["lag_1"]  = df["global_active_power"].shift(1)
df["lag_5"]  = df["global_active_power"].shift(5)
df["lag_15"] = df["global_active_power"].shift(15)
df["lag_60"] = df["global_active_power"].shift(60)

# 3. Momentum diffs
df["diff_1"] = df["lag_1"] - df["global_active_power"].shift(2)
df["diff_5"] = df["lag_1"] - df["global_active_power"].shift(6)

# 4. Moving averages (computed on shifted series)
df["ma_10"]  = df["global_active_power"].shift(1).rolling(10).mean()
df["ma_60"]  = df["global_active_power"].shift(1).rolling(60).mean()

# 5. Rolling volatility
df["rolling_std_10"] = df["global_active_power"].shift(1).rolling(10).std()

# Drop the first 60 rows (NaN from lag_60 warm-up)
df = df.dropna(subset=FEATURE_COLS)
```

**Warm-up rows dropped:** 60 (in Task 4, where only 500 rows are fetched → 440 usable rows)

---

## Stage 3 — Model Training (Task 1C)

### Training pipeline

```
raw 2,075,259 rows
        │
        ▼  build_features()
2,075,199 rows (60 warm-up rows removed)
        │
        ▼  chronological 80/20 split
Train: 1,660,159  |  Test: 415,040
        │
        ▼  MinMaxScaler.fit_transform(X_train)
        │  MinMaxScaler.transform(X_test)
        │
        ▼  RandomizedSearchCV
        │  (TimeSeriesSplit, n_splits=3, on 100K tail)
        │
        ▼  best estimator → fit on full train set
        │
        ▼  evaluate on test set → MAE, RMSE, MAPE, R²
        │
        ▼  save artefacts
             data/best_model.joblib
             data/best_scaler.joblib
             data/feature_columns.json
             data/experiment_results.json
```

---

## Stage 4 — Database Load (Task 2)

**Entry point:** `python task2_databases/task2_main.py`

### MySQL load sequence

```
1. Connect to household_power (MySQL)
2. CREATE TABLE IF NOT EXISTS  (idempotent)
3. INSERT INTO households (1 row)
4. Read raw .txt in 5000-row chunks
5. For each chunk:
   a. Parse datetime, coerce numeric columns
   b. executemany() into measurements
   c. executemany() into sub_metering
6. Run 7 analytical SQL queries → write to outputs/task2/sql_query_results.txt
7. INSERT INTO hourly_aggregates (SELECT ... GROUP BY HOUR)
8. Generate ERD → outputs/task2/erd_diagram.png
```

**Bulk-insert approach:** `executemany()` with 5000-row batches. Full load takes 5–15 min depending on hardware.

### MongoDB load sequence

```
1. Attempt pymongo connection to localhost:27017
   ├── SUCCESS → use real MongoDB client
   └── FAIL    → switch to JSON simulation mode (identical output)
2. For each measurement row:
   → build document (denormalise household_info + sub_metering array)
   → insert_one() or append to in-memory list
3. Run $out aggregation → populate daily_summaries
4. Run 5 analytical queries → write to outputs/task2/mongodb_query_results.txt
5. Export 3 sample documents → outputs/task2/sample_documents.json
```

---

## Stage 5 — REST API (Task 3)

**Entry point:** `python task3_api/api.py`

### Startup sequence

```python
@asynccontextmanager
async def lifespan(app):
    # 1. Connect to MySQL
    # 2. Step-sample 1000 rows from measurements table
    #    (evenly distributed across 2.07M rows → full 4-year span)
    # 3. For each sampled row: build_mongo_doc() → store in mongo_store dict
    # 4. Set mongo_counter = max(measurement_id) + 1
    yield
    # (cleanup on shutdown)
```

**mongo_store** is a plain Python `dict[str, dict]` keyed by `_id` string. All MongoDB endpoints operate against this dict — no `pymongo` dependency needed at runtime.

### Request lifecycle

```
HTTP request
    │
    ▼  FastAPI router (path matching)
    │
    ├── /sql/* endpoints
    │       │
    │       ▼  mysql.connector.connect()
    │       │  (new connection per request — lightweight for dev use)
    │       ▼  execute parameterised SQL
    │       ▼  cursor.fetchall() → list[dict]
    │       ▼  close connection
    │       ▼  return JSON response
    │
    └── /mongo/* endpoints
            │
            ▼  operate on in-memory mongo_store dict
            ▼  return JSON response
```

---

## Stage 6 — Prediction Script (Task 4)

**Entry point:** `python task4_prediction/prediction_script.py`

### Step 1 — Data Fetch (with fallback chain)

```
Try  → GET /sql/date-range (Task 3 API, 500 rows)
         │
         ├── 200 OK → use API records
         │
         └── Connection refused / error
                 │
                 ├── Try MySQL directly (SELECT ... LIMIT 500)
                 │       │
                 │       └── Connection error
                 │               │
                 │               └── Read raw .txt (first 500 rows)
```

Pass `--no-api` flag to skip the API step entirely.

### Step 2 — Preprocessing

Identical to Stage 2 feature engineering. Applied to the 500-row window:
- Parse `measurement_datetime` → DatetimeIndex
- Sort ascending
- `resample("1min").asfreq()` (restores any missing minutes in the window)
- `interpolate(method="time")`
- Build all 15 features
- Drop first 60 rows → 440 usable rows

### Step 3 — Model Load

```python
model  = joblib.load("data/best_model.joblib")   # RandomForestRegressor
scaler = joblib.load("data/best_scaler.joblib")   # MinMaxScaler
cols   = json.loads(open("data/feature_columns.json").read())  # 15 names
```

### Step 4 — Evaluation

```python
X = scaler.transform(df[cols])
y_pred = model.predict(X)
y_true = df["global_active_power"].values

mae  = mean_absolute_error(y_true, y_pred)
rmse = np.sqrt(mean_squared_error(y_true, y_pred))
mape = np.mean(np.abs((y_true - y_pred) / y_true)) * 100
r2   = r2_score(y_true, y_pred)
```

### Step 5 — Autoregressive Forecast

```
last_row = df.iloc[-1]   # seed state
forecast = []

for step in 1..12:
    1. Build feature vector from last_row
    2. Scale with scaler
    3. y_hat = model.predict(feature_vector)
    4. Advance timestamp by 1 minute
    5. Update lag features:
         lag_60 ← lag_59  (shift chain)
         ...
         lag_1  ← y_hat
    6. Recompute diffs, MAs, rolling_std from updated lags
    7. Append {step, timestamp, predicted_kw} to forecast
    8. last_row = updated state

return forecast as JSON
```

---

## Data Flow Diagram

```
household_power_consumption.txt (127 MB)
    │
    │  pd.read_csv() + resample + interpolate
    ▼
DataFrame (2,075,259 × 7)
    │
    ├──► feature engineering → DataFrame (2,075,199 × 15)
    │         │
    │         ├──► train/test split → fit RF + GB + LR
    │         │                        └──► data/best_model.joblib
    │         │                        └──► data/best_scaler.joblib
    │         │
    │         └──► outputs/  (plots, experiment_results.json)
    │
    ├──► MySQL bulk insert → household_power DB (2,075,259 rows)
    │         │
    │         └──► FastAPI  → /sql/* endpoints
    │                  │
    │                  └──► GET /sql/date-range (500 rows)
    │                              │
    │                              └──► prediction_script.py
    │                                        │
    │                                        ├──► Step 4: MAE/RMSE/MAPE/R²
    │                                        └──► Step 5: 12-step forecast JSON
    │
    └──► MongoDB seed → mongo_store (1,000 docs)
              │
              └──► FastAPI  → /mongo/* endpoints
```

---

## Output Files Reference

| Path | Created by | Contents |
|------|-----------|---------|
| `data/best_model.joblib` | Task 1 notebook | Serialised RandomForestRegressor |
| `data/best_scaler.joblib` | Task 1 notebook | Serialised MinMaxScaler |
| `data/feature_columns.json` | Task 1 notebook | List of 15 feature names |
| `data/experiment_results.json` | Task 1 notebook | 4-row comparison table |
| `outputs/*.png` | Task 1 notebook | EDA and model visualisations (12 files) |
| `outputs/task2/sql_query_results.txt` | Task 2 main | All 7 SQL query results |
| `outputs/task2/mongodb_query_results.txt` | Task 2 main | All 5 MongoDB query results |
| `outputs/task2/sample_documents.json` | Task 2 main | 3 MongoDB sample documents |
| `outputs/task2/schema.sql` | Task 2 main | MySQL DDL output |
| `outputs/task2/erd_diagram.png` | Task 2 main | Auto-generated ERD |
| `outputs/task2/mongodb_collection_design.txt` | Task 2 main | Collection schema + query docs |
