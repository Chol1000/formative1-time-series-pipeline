"""
Task 3 — REST API for Household Power Consumption (FastAPI, port 8000).

Provides full CRUD and time-series query endpoints backed by both MySQL and
MongoDB. SQL routes are prefixed with /sql/ and operate on the households and
measurements tables. MongoDB routes are prefixed with /mongo/ and operate on
an in-memory document store seeded from MySQL on startup. A health-check
endpoint is available at GET /.

Run with:  python task3_api/api.py
Docs at:   http://localhost:8000/docs
"""

from __future__ import annotations

import sys
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import mysql.connector
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Paths & MySQL connection config
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent

DB_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",
    "password": "",
    "database": "household_power",
}

# ---------------------------------------------------------------------------
# In-memory MongoDB store
# ---------------------------------------------------------------------------
mongo_store:   dict[str, dict[str, Any]] = {}
mongo_counter: int = 0

MONGO_SEED_DOCS = 1_000   # total docs to seed on startup


def get_next_mongo_id() -> str:
    global mongo_counter
    mongo_counter += 1
    return str(mongo_counter)


def _build_mongo_doc(row: dict) -> dict[str, Any]:
    ts_str = row["measurement_datetime"]
    if hasattr(ts_str, 'isoformat'):
        ts_str = ts_str.strftime("%Y-%m-%d %H:%M:%S")
    try:
        ts_dt = datetime.fromisoformat(ts_str)
    except ValueError:
        ts_dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")

    return {
        "_id":                   str(row["measurement_id"]),
        "household_id":          1,
        "household_info": {
            "name":     "Household A",
            "location": "Sceaux, Hauts-de-Seine, France",
        },
        "timestamp":             ts_str,
        "date":                  ts_dt.strftime("%Y-%m-%d"),
        "hour":                  ts_dt.hour,
        "day_of_week":           ts_dt.weekday(),
        "global_active_power":   round(float(row["global_active_power"]   or 0), 3),
        "global_reactive_power": round(float(row["global_reactive_power"] or 0), 3),
        "voltage":               round(float(row["voltage"]               or 0), 2),
        "global_intensity":      round(float(row["global_intensity"]      or 0), 2),
        "sub_metering": [
            {"meter_id": 1, "name": "Kitchen",
             "consumption_wh": round(float(row["sub_metering_1"] or 0), 1)},
            {"meter_id": 2, "name": "Laundry / AC",
             "consumption_wh": round(float(row["sub_metering_2"] or 0), 1)},
            {"meter_id": 3, "name": "Water Heater",
             "consumption_wh": round(float(row["sub_metering_3"] or 0), 1)},
        ],
    }


def init_mongo_store() -> None:
    """Seed in-memory store with step-sampled rows + the actual last row.

    Step-sampling guarantees the full time range is represented so that
    /mongo/latest returns 2010-11-26 (end of dataset), not 2006-12-16.
    """
    global mongo_counter
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cur  = conn.cursor(dictionary=True)

        _sel = (
            "SELECT m.measurement_id,"
            "       m.measurement_datetime,"
            "       m.global_active_power,"
            "       m.global_reactive_power,"
            "       m.voltage,"
            "       m.global_intensity,"
            "       s.sub_metering_1,"
            "       s.sub_metering_2,"
            "       s.sub_metering_3"
            " FROM   measurements m"
            " LEFT JOIN sub_metering s ON m.measurement_id = s.measurement_id"
        )

        # Total rows available
        cur.execute("SELECT COUNT(*) AS n FROM measurements")
        total = cur.fetchone()["n"]
        step  = max(1, total // (MONGO_SEED_DOCS - 1))

        # N-1 step-sampled rows (spans full time range)
        cur.execute(
            f"{_sel} WHERE (m.measurement_id - 1) % {step} = 0"
            f" ORDER BY m.measurement_datetime ASC LIMIT {MONGO_SEED_DOCS - 1}"
        )
        rows = cur.fetchall()

        # Actual last row (ensures /mongo/latest returns dataset end date)
        cur.execute(f"{_sel} ORDER BY m.measurement_datetime DESC LIMIT 1")
        last = cur.fetchone()

        cur.close()
        conn.close()

        for row in rows:
            doc = _build_mongo_doc(row)
            mongo_store[doc["_id"]] = doc

        if last and str(last["measurement_id"]) not in mongo_store:
            doc = _build_mongo_doc(last)
            mongo_store[doc["_id"]] = doc

        mongo_counter = max((int(k) for k in mongo_store), default=0)
        print(f"[startup] MongoDB store seeded: {len(mongo_store)} documents "
              f"(step={step}, total_rows={total})")
    except Exception as exc:
        print(f"[startup] MongoDB seeding failed: {exc}", file=sys.stderr)

# ---------------------------------------------------------------------------
# Lifespan (replaces deprecated @app.on_event)
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_mongo_store()
    yield


app = FastAPI(
    title="Household Power Consumption API",
    description=(
        "Time-series REST API for the UCI Household Electric Power Consumption dataset.\n\n"
        "Provides full **CRUD** and **time-series** endpoints for both "
        "**SQL (MySQL)** and **MongoDB** (in-memory store seeded from MySQL).\n\n"
        "Start: `python task3_api/api.py`  |  Tests: `python task3_api/test_api.py`"
    ),
    version="3.0.0",
    lifespan=lifespan,
)


# ============================================================================
# PYDANTIC MODELS
# ============================================================================

class MeasurementCreate(BaseModel):
    household_id:          int            = Field(..., description="Household identifier")
    measurement_datetime:  str            = Field(..., description="Datetime YYYY-MM-DD HH:MM:SS")
    global_active_power:   float          = Field(..., description="Active power in kW")
    global_reactive_power: Optional[float] = Field(None, description="Reactive power in kW")
    voltage:               Optional[float] = Field(None, description="Voltage in V")
    global_intensity:      Optional[float] = Field(None, description="Current intensity in A")
    sub_metering_1:        Optional[float] = Field(None, description="Sub-metering 1 (Wh)")
    sub_metering_2:        Optional[float] = Field(None, description="Sub-metering 2 (Wh)")
    sub_metering_3:        Optional[float] = Field(None, description="Sub-metering 3 (Wh)")


class MeasurementUpdate(BaseModel):
    global_active_power:   Optional[float] = None
    global_reactive_power: Optional[float] = None
    voltage:               Optional[float] = None
    global_intensity:      Optional[float] = None


class HouseholdCreate(BaseModel):
    household_name: str            = Field(..., description="Name of the household")
    location:       Optional[str]  = Field(None, description="Location / address")
    area_sqm:       Optional[float] = Field(None, description="Area in square metres")
    occupants:      Optional[int]  = Field(None, description="Number of occupants")


class HouseholdUpdate(BaseModel):
    household_name: Optional[str]  = None
    location:       Optional[str]  = None
    area_sqm:       Optional[float] = None
    occupants:      Optional[int]  = None


# ============================================================================
# DB HELPER
# ============================================================================

def get_db_connection() -> mysql.connector.connection.MySQLConnection:
    return mysql.connector.connect(**DB_CONFIG)


# ============================================================================
# HEALTH CHECK
# ============================================================================

@app.get("/", summary="Health Check")
def health_check():
    return {
        "status":            "healthy",
        "api":               "Household Power Consumption API",
        "version":           "3.0.0",
        "db_host":           DB_CONFIG["host"],
        "db_name":           DB_CONFIG["database"],
        "mongo_docs_loaded": len(mongo_store),
        "endpoints": {
            "sql_households":     "POST GET GET/{id} PUT DELETE  /sql/households",
            "sql_measurements":   "POST GET GET/{id} PUT DELETE  /sql/measurements",
            "sql_timeseries":     "GET /sql/latest  /sql/date-range  /sql/hourly-stats  /sql/monthly-trend  /sql/sub-metering",
            "mongo_measurements": "POST GET GET/{id} PUT DELETE  /mongo/measurements",
            "mongo_timeseries":   "GET /mongo/latest  /mongo/date-range  /mongo/hourly-stats  /mongo/daily-summary",
            "docs": "/docs",
        },
    }

# ============================================================================
# SQL — HOUSEHOLD CRUD
# ============================================================================

@app.post("/sql/households", status_code=201, summary="[SQL] Create household")
def create_sql_household(body: HouseholdCreate):
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO households (household_name, location, area_sqm, occupants) "
            "VALUES (%s, %s, %s, %s)",
            (body.household_name, body.location, body.area_sqm, body.occupants),
        )
        conn.commit()
        return {"status": "created", "household_id": cur.lastrowid}
    except Exception as exc:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        conn.close()


@app.get("/sql/households", summary="[SQL] List all households")
def list_sql_households():
    conn = get_db_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT household_id, household_name, location, area_sqm, occupants "
            "FROM households ORDER BY household_id"
        )
        rows = cur.fetchall()
        return {"source": "SQL", "count": len(rows), "data": rows}
    finally:
        conn.close()


@app.get("/sql/households/{household_id}", summary="[SQL] Get household by ID")
def get_sql_household(household_id: int):
    conn = get_db_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT household_id, household_name, location, area_sqm, occupants "
            "FROM households WHERE household_id = %s",
            (household_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Household {household_id} not found")
        return {"source": "SQL", "data": row}
    finally:
        conn.close()


@app.put("/sql/households/{household_id}", summary="[SQL] Update household")
def update_sql_household(household_id: int, body: HouseholdUpdate):
    conn = get_db_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT household_id FROM households WHERE household_id = %s",
                    (household_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail=f"Household {household_id} not found")

        fields = body.model_dump(exclude_none=True)
        if not fields:
            raise HTTPException(status_code=400, detail="No fields provided for update")

        set_clause = ", ".join(f"{col} = %s" for col in fields)
        cur.execute(
            f"UPDATE households SET {set_clause} WHERE household_id = %s",
            list(fields.values()) + [household_id],
        )
        conn.commit()
        return {"status": "updated", "household_id": household_id, "updated_fields": fields}
    except HTTPException:
        raise
    except Exception as exc:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        conn.close()


@app.delete("/sql/households/{household_id}", summary="[SQL] Delete household")
def delete_sql_household(household_id: int):
    if household_id == 1:
        raise HTTPException(
            status_code=403,
            detail="Household 1 is the primary dataset household and cannot be deleted.",
        )
    conn = get_db_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT household_id FROM households WHERE household_id = %s",
                    (household_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail=f"Household {household_id} not found")

        cur.execute(
            "DELETE FROM sub_metering WHERE measurement_id IN "
            "(SELECT measurement_id FROM measurements WHERE household_id = %s)",
            (household_id,),
        )
        cur.execute("DELETE FROM measurements WHERE household_id = %s", (household_id,))
        cur.execute("DELETE FROM households WHERE household_id = %s", (household_id,))
        conn.commit()
        return {
            "status":       "deleted",
            "household_id": household_id,
            "message":      "Household and all related measurements deleted.",
        }
    except HTTPException:
        raise
    except Exception as exc:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        conn.close()
