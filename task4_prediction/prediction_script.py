"""
Task 4 — end-to-end prediction pipeline for household power consumption.

Fetches data from the Task 3 REST API (falls back to MySQL then the raw
file), applies the Task 1 feature-engineering pipeline, loads the trained
Random Forest model, evaluates it, and produces a 12-step autoregressive
forecast.

Usage:
    python task3_api/api.py                          # start API first
    python task4_prediction/prediction_script.py
    python task4_prediction/prediction_script.py --no-api
"""

from __future__ import annotations

import argparse
import json
import mysql.connector
import sys
import warnings
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import requests

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# Paths -- resolved relative to this file so the script is portable
# ---------------------------------------------------------------------------
BASE_DIR      = Path(__file__).resolve().parent.parent
API_BASE_URL  = "http://localhost:8000"
DB_CONFIG     = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",
    "password": "",
    "database": "household_power",
}
RAW_DATA_PATH = BASE_DIR / "household_power_consumption.txt"
MODEL_PATH    = BASE_DIR / "data" / "best_model.joblib"
SCALER_PATH   = BASE_DIR / "data" / "best_scaler.joblib"
FEATURE_PATH  = BASE_DIR / "data" / "feature_columns.json"

TARGET = "global_active_power"

# ---------------------------------------------------------------------------
# Feature list -- mirrors Task 1 notebook exactly (15 features)
# ---------------------------------------------------------------------------
FEATURE_COLS = [
    "hour_sin",  "hour_cos",
    "dow_sin",   "dow_cos",
    "month_sin", "month_cos",
    "lag_1",     "lag_5",    "lag_15",  "lag_60",
    "diff_1",    "diff_5",
    "ma_10",     "ma_60",
    "rolling_std_10",
]

# ---------------------------------------------------------------------------
# Column rename map: raw UCI column names -> lowercase snake_case
# ---------------------------------------------------------------------------
_RENAME_MAP = {
    "Global_active_power":   "global_active_power",
    "Global_reactive_power": "global_reactive_power",
    "Voltage":               "voltage",
    "Global_intensity":      "global_intensity",
    "Sub_metering_1":        "sub_metering_1",
    "Sub_metering_2":        "sub_metering_2",
    "Sub_metering_3":        "sub_metering_3",
}


def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename UCI columns to lowercase snake_case where needed."""
    return df.rename(columns=_RENAME_MAP)


# ---------------------------------------------------------------------------
# Feature engineering -- identical to build_features() in Task 1 notebook
# ---------------------------------------------------------------------------

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Build 15 features matching the Task 1 notebook (cyclic encoding, lags, diffs, MAs)."""
    d = df.copy().sort_values("datetime").reset_index(drop=True)

    # Cyclic temporal encoding
    d["hour_sin"]  = np.sin(2 * np.pi * d["datetime"].dt.hour / 24)
    d["hour_cos"]  = np.cos(2 * np.pi * d["datetime"].dt.hour / 24)
    d["dow_sin"]   = np.sin(2 * np.pi * d["datetime"].dt.dayofweek / 7)
    d["dow_cos"]   = np.cos(2 * np.pi * d["datetime"].dt.dayofweek / 7)
    d["month_sin"] = np.sin(2 * np.pi * d["datetime"].dt.month / 12)
    d["month_cos"] = np.cos(2 * np.pi * d["datetime"].dt.month / 12)

    # Autoregressive lag features (past values only via .shift)
    for lag in [1, 5, 15, 60]:
        d[f"lag_{lag}"] = d[TARGET].shift(lag)

    # Momentum: first-difference over 1 and 5 steps
    d["diff_1"] = d[TARGET].shift(1) - d[TARGET].shift(2)
    d["diff_5"] = d[TARGET].shift(1) - d[TARGET].shift(6)

    # Rolling statistics (past values only)
    past = d[TARGET].shift(1)
    d["ma_10"]          = past.rolling(10, min_periods=1).mean()
    d["ma_60"]          = past.rolling(60, min_periods=1).mean()
    d["rolling_std_10"] = past.rolling(10, min_periods=2).std().fillna(0)

    # Drop rows where any feature is NaN (first ~60 rows after sort)
    return d.dropna(subset=FEATURE_COLS).reset_index(drop=True)


# ============================================================================
# PREDICTION PIPELINE
# ============================================================================

class PredictionPipeline:
    """Orchestrates Steps 1-5. Set use_api=False to skip the REST API."""

    def __init__(self, use_api: bool = True) -> None:
        self.use_api      = use_api
        self.model        = None
        self.scaler       = None
        self.feature_cols = FEATURE_COLS   # may be updated from JSON in step 3

    # -------------------------------------------------------------------------
    # STEP 1: FETCH DATA
    # -------------------------------------------------------------------------

    def _fetch_from_api(self, limit: int = 500) -> "pd.DataFrame | None":
        """Fetch records from the Task 3 API (GET /sql/date-range)."""
        print("  Trying Task 3 API  ->  GET /sql/date-range ...")
        try:
            resp = requests.get(
                f"{API_BASE_URL}/sql/date-range",
                params={
                    "start_date": "2006-12-16",
                    "end_date":   "2007-01-16",
                    "limit":      limit,
                },
                timeout=5,
            )
            if resp.status_code != 200:
                raise ValueError(f"HTTP {resp.status_code}")

            records = resp.json().get("data", [])
            if len(records) < 70:
                raise ValueError(
                    f"Only {len(records)} records returned -- need >= 70 "
                    "to compute lag_60 features."
                )

            df = pd.DataFrame(records)
            df["datetime"] = pd.to_datetime(df["datetime"])
            print(
                f"  API returned {len(df):,} records  "
                f"[{df['datetime'].min().date()}  to  "
                f"{df['datetime'].max().date()}]"
            )
            return df

        except requests.exceptions.ConnectionError:
            print("  API is not running (connection refused).")
        except Exception as exc:
            print(f"  API call failed: {exc}")

        return None

    def _fetch_from_db(self, limit: int = 500) -> "pd.DataFrame | None":
        """Fallback: read from the MySQL database."""
        print("  Trying MySQL database (Task 2) ...")
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            df = pd.read_sql(
                f"""
                SELECT
                    m.measurement_datetime       AS datetime,
                    m.global_active_power,
                    m.global_reactive_power,
                    m.voltage,
                    m.global_intensity,
                    sm.sub_metering_1,
                    sm.sub_metering_2,
                    sm.sub_metering_3
                FROM measurements m
                LEFT JOIN sub_metering sm
                    ON m.measurement_id = sm.measurement_id
                ORDER BY m.measurement_datetime
                LIMIT {limit}
                """,
                conn,
            )
            conn.close()
            df["datetime"] = pd.to_datetime(df["datetime"])
            print(f"  Database returned {len(df):,} records")
            return df

        except Exception as exc:
            print(f"  Database unavailable: {exc}")
            return None

    def _fetch_from_raw_file(self, limit: int = 2000) -> pd.DataFrame:
        """Last-resort fallback: read from the raw .txt file."""
        print("  Reading raw dataset file (final fallback) ...")
        df = pd.read_csv(
            str(RAW_DATA_PATH),
            delimiter=";",
            na_values="?",
            nrows=limit,
        )
        df["datetime"] = pd.to_datetime(
            df["Date"] + " " + df["Time"], dayfirst=True
        )
        df = df.drop(columns=["Date", "Time"])
        df = _normalise_columns(df)
        print(f"  Raw file returned {len(df):,} records")
        return df

    def step1_fetch(self, limit: int = 500) -> pd.DataFrame:
        """Pull records via API → MySQL → raw file, whichever responds first."""
        print()
        print("=" * 65)
        print("STEP 1: FETCH DATA")
        print("=" * 65)

        df     = None
        source = "unknown"

        if self.use_api:
            df = self._fetch_from_api(limit=limit)
            if df is not None:
                source = "Task 3 REST API  (GET /sql/date-range)"

        if df is None:
            df = self._fetch_from_db(limit=limit)
            if df is not None:
                source = "MySQL database  (Task 2)"

        if df is None:
            # Read extra rows so lag warm-up does not reduce us below `limit`
            df = self._fetch_from_raw_file(limit=limit + 1500)
            source = "raw dataset file"

        df = _normalise_columns(df)
        df = df.sort_values("datetime").reset_index(drop=True)

        print()
        print(f"  Source          : {source}")
        print(f"  Records fetched : {len(df):,}")
        print(f"  Date range      : {df['datetime'].min()}  to  {df['datetime'].max()}")
        return df

    # -------------------------------------------------------------------------
    # STEP 2: PREPROCESS
    # -------------------------------------------------------------------------

    def step2_preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """Interpolate missing values then call build_features() (mirrors Task 1)."""
        print()
        print("=" * 65)
        print("STEP 2: PREPROCESS")
        print("=" * 65)

        df = df.copy()

        # -- Missing value handling (linear interpolation, same as Task 1) --
        numeric_cols = [
            "global_active_power", "global_reactive_power",
            "voltage", "global_intensity",
        ]
        total_missing = 0
        for col in numeric_cols:
            if col in df.columns:
                n = int(df[col].isnull().sum())
                if n > 0:
                    df[col] = df[col].interpolate(method="linear").ffill().bfill()
                    total_missing += n

        if total_missing:
            print(
                f"  Interpolated {total_missing} missing value(s)  "
                "(linear, matching Task 1 methodology)"
            )
        else:
            print("  No missing values detected")

        before = len(df)
        df = df.dropna(subset=[TARGET])

        # -- Feature engineering (identical to Task 1 notebook) --
        df = build_features(df)

        dropped = before - len(df)
        print(f"  Records before build_features : {before:,}")
        print(
            f"  Records after  build_features : {len(df):,}  "
            f"({dropped} lag warm-up rows removed)"
        )
        print(f"  Feature set ({len(FEATURE_COLS)}) :")
        print(f"    {FEATURE_COLS}")
        return df

    # -------------------------------------------------------------------------
    # STEP 3: LOAD TRAINED MODEL
    # -------------------------------------------------------------------------

    def step3_load_model(self) -> None:
        """Load the Random Forest model and MinMaxScaler saved by Task 1."""
        print()
        print("=" * 65)
        print("STEP 3: LOAD TRAINED MODEL")
        print("=" * 65)

        if not MODEL_PATH.exists():
            raise FileNotFoundError(
                f"Trained model not found: {MODEL_PATH}\n"
                "Run task1_eda/task1_notebook.ipynb (Section 1C) to train "
                "and save the model artefacts."
            )
        if not SCALER_PATH.exists():
            raise FileNotFoundError(f"Scaler not found: {SCALER_PATH}")

        self.model  = joblib.load(str(MODEL_PATH))
        self.scaler = joblib.load(str(SCALER_PATH))

        if FEATURE_PATH.exists():
            with open(str(FEATURE_PATH)) as fh:
                self.feature_cols = json.load(fh)

        print(f"  Model   : {MODEL_PATH.name}  ({type(self.model).__name__})")
        print(f"  Scaler  : {SCALER_PATH.name}  ({type(self.scaler).__name__})")
        print(f"  Features: {len(self.feature_cols)}  {self.feature_cols}")

    # -------------------------------------------------------------------------
    # STEP 4: PREDICT
    # -------------------------------------------------------------------------

    def step4_predict(
        self, df: pd.DataFrame
    ) -> "tuple[np.ndarray, pd.DataFrame]":
        """Score all records and print MAE/RMSE/MAPE plus a 10-row comparison table."""
        print()
        print("=" * 65)
        print("STEP 4: PREDICT")
        print("=" * 65)

        # Guard: fill any unexpectedly absent feature column with 0
        for col in self.feature_cols:
            if col not in df.columns:
                df[col] = 0.0

        X           = df[self.feature_cols].values
        X_scaled    = self.scaler.transform(X)
        predictions = self.model.predict(X_scaled)

        actuals = df[TARGET].values
        mae  = float(np.mean(np.abs(actuals - predictions)))
        rmse = float(np.sqrt(np.mean((actuals - predictions) ** 2)))
        mape = float(
            np.mean(
                np.abs((actuals - predictions) /
                       np.where(actuals == 0, 1e-9, actuals))
            ) * 100
        )
        ss_res = float(np.sum((actuals - predictions) ** 2))
        ss_tot = float(np.sum((actuals - np.mean(actuals)) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        print(f"  Records scored     : {len(predictions):,}")
        print(
            f"  Predicted range    : [{predictions.min():.3f},  "
            f"{predictions.max():.3f}] kW"
        )
        print(f"  Mean predicted     : {predictions.mean():.4f} kW")
        print(f"  MAE                : {mae:.4f} kW")
        print(f"  RMSE               : {rmse:.4f} kW")
        print(f"  MAPE               : {mape:.2f} %")
        print(f"  R\u00b2 (coefficient of determination) : {r2:.4f}")

        # Comparison table -- last 10 records
        print()
        print(
            f"  {'Datetime':<22} {'Actual (kW)':>12} "
            f"{'Predicted (kW)':>15} {'Error (kW)':>11}"
        )
        print("  " + "-" * 63)
        n = len(df)
        for i in range(max(0, n - 10), n):
            dt  = str(df["datetime"].iloc[i])[:19]
            act = actuals[i]
            prd = predictions[i]
            print(f"  {dt:<22} {act:>12.3f} {prd:>15.3f} {act - prd:>11.3f}")

        self._metrics = {"mae": mae, "rmse": rmse, "mape": mape, "r2": r2}
        return predictions, df

    # -------------------------------------------------------------------------
    # STEP 5: FORECAST (autoregressive multi-step ahead)
    # -------------------------------------------------------------------------

    def step5_forecast(
        self, df: pd.DataFrame, steps: int = 12
    ) -> "tuple[list[float], list[str]]":
        """Autoregressively forecast `steps` minutes ahead, feeding each prediction back as lag_1."""
        print()
        print("=" * 65)
        print("STEP 5: AUTOREGRESSIVE FORECAST")
        print("=" * 65)
        print(f"  {steps}-step ahead forecast (1 prediction per minute)")
        print(f"  Each predicted value feeds back as lag_1 for the next step")
        print(f"  Temporal features (hour/dow/month) updated for each future datetime")
        print()
        print(f"  FORECAST  --  next {steps} minute(s) from last observed record")
        print("  " + "-" * 55)

        last_dt = df["datetime"].iloc[-1]

        # Seed the rolling pool with the last 61 known actual values
        # (61 because lag_60 looks back 60 steps from the current position)
        history    = list(df[TARGET].values[-61:])
        forecasts  = []
        timestamps = []

        for step in range(1, steps + 1):
            future_dt = last_dt + pd.Timedelta(minutes=step)
            pool      = history + forecasts   # actual history + forecast buffer

            feat = {
                # Cyclic temporal -- computed for the actual future datetime
                "hour_sin":  np.sin(2 * np.pi * future_dt.hour / 24),
                "hour_cos":  np.cos(2 * np.pi * future_dt.hour / 24),
                "dow_sin":   np.sin(2 * np.pi * future_dt.dayofweek / 7),
                "dow_cos":   np.cos(2 * np.pi * future_dt.dayofweek / 7),
                "month_sin": np.sin(2 * np.pi * future_dt.month / 12),
                "month_cos": np.cos(2 * np.pi * future_dt.month / 12),
                # Autoregressive lags
                "lag_1":  pool[-1]  if len(pool) >= 1  else history[-1],
                "lag_5":  pool[-5]  if len(pool) >= 5  else history[-1],
                "lag_15": pool[-15] if len(pool) >= 15 else history[-1],
                "lag_60": pool[-60] if len(pool) >= 60 else history[-1],
                # Momentum
                "diff_1": (pool[-1] - pool[-2]) if len(pool) >= 2 else 0.0,
                "diff_5": (pool[-1] - pool[-6]) if len(pool) >= 6 else 0.0,
                # Moving averages (past-only)
                "ma_10": float(np.mean(pool[-10:])) if len(pool) >= 2 else pool[-1],
                "ma_60": float(np.mean(pool[-60:])) if len(pool) >= 2 else pool[-1],
                # Rolling standard deviation
                "rolling_std_10": (
                    float(np.std(pool[-10:], ddof=1))
                    if len(pool) >= 2 else 0.0
                ),
            }

            X_future = np.array([[feat[c] for c in self.feature_cols]])
            X_scaled = self.scaler.transform(X_future)
            val      = float(self.model.predict(X_scaled)[0])
            val      = max(0.0, val)   # global active power cannot be negative

            forecasts.append(val)
            timestamps.append(f"+{step}min")

            label = str(future_dt)[:16]
            print(f"  t+{step:2d}min  [{label}]  {val:.4f} kW")

        return forecasts, timestamps

    # -------------------------------------------------------------------------
    # RUN -- orchestrate all five steps
    # -------------------------------------------------------------------------

    def run(self) -> dict:
        """Run Steps 1-5 and return a JSON-serialisable summary dict."""
        print()
        print("=" * 65)
        print("  TASK 4: PREDICTION / FORECAST PIPELINE")
        print("  Dataset : Household Power Consumption (UCI)")
        print("  Target  : global_active_power (kW), 1-minute resolution")
        print(f"  Run     : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 65)

        df_raw       = self.step1_fetch(limit=500)
        df_processed = self.step2_preprocess(df_raw)
        self.step3_load_model()
        predictions, df_scored = self.step4_predict(df_processed)
        forecasts, timestamps  = self.step5_forecast(df_scored, steps=12)

        print()
        print("=" * 65)
        print("  PIPELINE COMPLETE -- SUMMARY")
        print("=" * 65)
        print(f"  Records fetched          : {len(df_raw):,}")
        print(f"  Records after preprocess : {len(df_processed):,}")
        print(f"  Feature count            : {len(self.feature_cols)}")
        print(f"  Model type               : {type(self.model).__name__}")
        print(f"  Predictions made         : {len(predictions):,}")
        print(f"  Mean predicted power     : {np.mean(predictions):.4f} kW")
        print(f"  Forecast: t+1  min       : {forecasts[0]:.4f} kW")
        print(f"  Forecast: t+12 min       : {forecasts[-1]:.4f} kW")
        print("=" * 65)

        return {
            "run_timestamp":            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "records_fetched":          len(df_raw),
            "records_after_preprocess": len(df_processed),
            "features_used":            len(self.feature_cols),
            "model_type":               type(self.model).__name__,
            "n_predictions":            len(predictions),
            "mean_predicted_power_kW":  round(float(np.mean(predictions)), 4),
            "evaluation": {
                "mae_kW":  round(self._metrics["mae"],  4),
                "rmse_kW": round(self._metrics["rmse"], 4),
                "mape_pct": round(self._metrics["mape"], 2),
                "r2":      round(self._metrics["r2"],   4),
            },
            "forecast": [
                {"step": t, "power_kW": round(v, 4)}
                for t, v in zip(timestamps, forecasts)
            ],
        }


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Task 4 prediction pipeline — fetch, preprocess, predict, forecast."
    )
    parser.add_argument(
        "--no-api",
        action="store_true",
        help="Skip the REST API and read directly from MySQL or the raw file.",
    )
    args = parser.parse_args()

    pipeline = PredictionPipeline(use_api=not args.no_api)

    try:
        result = pipeline.run()
    except FileNotFoundError as exc:
        print(f"\nERROR: {exc}")
        sys.exit(1)

    print("\nFINAL RESULT (JSON):")
    print(json.dumps(result, indent=2))
