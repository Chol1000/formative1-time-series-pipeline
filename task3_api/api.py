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

# ============================================================================
# SQL — MEASUREMENT CRUD
# ============================================================================

@app.post("/sql/measurements", status_code=201, summary="[SQL] Create measurement")
def create_sql_measurement(body: MeasurementCreate):
    conn = get_db_connection()
    try:
        cur = conn.cursor(dictionary=True)

        # 409 if this datetime already exists
        cur.execute(
            "SELECT measurement_id FROM measurements WHERE measurement_datetime = %s",
            (body.measurement_datetime,),
        )
        existing = cur.fetchone()
        if existing:
            raise HTTPException(
                status_code=409,
                detail=f"Measurement with datetime '{body.measurement_datetime}' already exists (id={existing['measurement_id']})",
            )

        cur.execute(
            "INSERT INTO measurements "
            "(household_id, measurement_datetime, global_active_power, "
            " global_reactive_power, voltage, global_intensity) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (
                body.household_id,
                body.measurement_datetime,
                body.global_active_power,
                body.global_reactive_power,
                body.voltage,
                body.global_intensity,
            ),
        )
        meas_id = cur.lastrowid

        # Insert sub_metering using the NORMALIZED schema
        if any(v is not None for v in [body.sub_metering_1, body.sub_metering_2, body.sub_metering_3]):
            cur.execute(
                "INSERT INTO sub_metering "
                "(measurement_id, sub_metering_1, sub_metering_2, sub_metering_3) "
                "VALUES (%s, %s, %s, %s)",
                (
                    meas_id,
                    body.sub_metering_1 or 0.0,
                    body.sub_metering_2 or 0.0,
                    body.sub_metering_3 or 0.0,
                ),
            )

        conn.commit()
        return {
            "status":         "created",
            "measurement_id": meas_id,
            "datetime":       body.measurement_datetime,
            "message":        "Measurement created successfully in SQL database",
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception as exc:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        conn.close()


@app.get("/sql/measurements", summary="[SQL] List measurements")
def list_sql_measurements(limit: int = Query(10, ge=1, le=1000)):
    conn = get_db_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT m.measurement_id,
                   h.household_name,
                   m.measurement_datetime AS datetime,
                   m.global_active_power,
                   m.global_reactive_power,
                   m.voltage,
                   m.global_intensity
            FROM measurements m
            JOIN households h ON m.household_id = h.household_id
            ORDER BY m.measurement_datetime DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
        return {"source": "SQL", "count": len(rows), "data": rows}
    finally:
        conn.close()


@app.get("/sql/measurements/{measurement_id}", summary="[SQL] Get measurement by ID")
def get_sql_measurement(measurement_id: int):
    conn = get_db_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT m.measurement_id,
                   m.household_id,
                   h.household_name,
                   m.measurement_datetime AS datetime,
                   m.global_active_power,
                   m.global_reactive_power,
                   m.voltage,
                   m.global_intensity,
                   s.sub_metering_1,
                   s.sub_metering_2,
                   s.sub_metering_3
            FROM measurements m
            JOIN households h ON m.household_id = h.household_id
            LEFT JOIN sub_metering s ON m.measurement_id = s.measurement_id
            WHERE m.measurement_id = %s
            """,
            (measurement_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404,
                                detail=f"Measurement {measurement_id} not found")
        return {"source": "SQL", "data": row}
    finally:
        conn.close()


@app.put("/sql/measurements/{measurement_id}", summary="[SQL] Update measurement")
def update_sql_measurement(measurement_id: int, body: MeasurementUpdate):
    conn = get_db_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT measurement_id FROM measurements WHERE measurement_id = %s",
                    (measurement_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404,
                                detail=f"Measurement {measurement_id} not found")

        fields = body.model_dump(exclude_none=True)
        if not fields:
            raise HTTPException(status_code=400, detail="No fields provided for update")

        set_clause = ", ".join(f"{col} = %s" for col in fields)
        cur.execute(
            f"UPDATE measurements SET {set_clause} WHERE measurement_id = %s",
            list(fields.values()) + [measurement_id],
        )
        conn.commit()
        return {
            "status":         "updated",
            "measurement_id": measurement_id,
            "updated_fields": fields,
        }
    except HTTPException:
        raise
    except Exception as exc:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        conn.close()


@app.delete("/sql/measurements/{measurement_id}", summary="[SQL] Delete measurement")
def delete_sql_measurement(measurement_id: int):
    conn = get_db_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT measurement_id FROM measurements WHERE measurement_id = %s",
                    (measurement_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404,
                                detail=f"Measurement {measurement_id} not found")

        cur.execute("DELETE FROM sub_metering WHERE measurement_id = %s", (measurement_id,))
        cur.execute("DELETE FROM measurements WHERE measurement_id = %s", (measurement_id,))
        conn.commit()
        return {
            "status":         "deleted",
            "measurement_id": measurement_id,
            "message":        "Measurement and sub-metering deleted.",
        }
    except HTTPException:
        raise
    except Exception as exc:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        conn.close()

# ============================================================================
# SQL — TIME-SERIES ENDPOINTS
# ============================================================================

@app.get("/sql/latest", summary="[SQL] Latest measurement record")
def get_sql_latest(household_id: Optional[int] = None):
    conn = get_db_connection()
    try:
        params: list = []
        where = ""
        if household_id:
            where = "WHERE m.household_id = %s"
            params.append(household_id)

        cur = conn.cursor(dictionary=True)
        cur.execute(
            f"""
            SELECT m.measurement_id,
                   m.household_id,
                   h.household_name,
                   m.measurement_datetime AS datetime,
                   m.global_active_power,
                   m.global_reactive_power,
                   m.voltage,
                   m.global_intensity
            FROM measurements m
            JOIN households h ON m.household_id = h.household_id
            {where}
            ORDER BY m.measurement_datetime DESC
            LIMIT 1
            """,
            params,
        )
        row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="No records found")
        return {"source": "SQL", "query": "Latest Record", "data": row}
    finally:
        conn.close()


@app.get("/sql/date-range", summary="[SQL] Records in date range")
def get_sql_date_range(
    start_date: str = Query(..., description="Start date YYYY-MM-DD"),
    end_date:   str = Query(..., description="End date YYYY-MM-DD"),
    household_id: Optional[int] = None,
    limit: int = Query(100, ge=1, le=5000),
):
    conn = get_db_connection()
    try:
        params: list = [start_date + " 00:00:00", end_date + " 23:59:59"]
        extra = ""
        if household_id:
            extra = "AND m.household_id = %s"
            params.append(household_id)
        params.append(limit)

        cur = conn.cursor(dictionary=True)
        cur.execute(
            f"""
            SELECT m.measurement_id,
                   h.household_name,
                   m.measurement_datetime AS datetime,
                   m.global_active_power,
                   m.global_reactive_power,
                   m.voltage,
                   m.global_intensity
            FROM measurements m
            JOIN households h ON m.household_id = h.household_id
            WHERE m.measurement_datetime BETWEEN %s AND %s
            {extra}
            ORDER BY m.measurement_datetime ASC
            LIMIT %s
            """,
            params,
        )
        rows = cur.fetchall()

        return {
            "source":     "SQL",
            "query":      "Date Range",
            "start_date": start_date,
            "end_date":   end_date,
            "count":      len(rows),
            "data":       rows,
        }
    finally:
        conn.close()


@app.get("/sql/hourly-stats", summary="[SQL] Hourly average / peak / min power")
def get_sql_hourly_stats(household_id: Optional[int] = None):
    """Return per-hour (0-23) aggregate stats across the entire dataset."""
    conn = get_db_connection()
    try:
        params: list = []
        where = ""
        if household_id:
            where = "WHERE m.household_id = %s"
            params.append(household_id)

        cur = conn.cursor(dictionary=True)
        cur.execute(
            f"""
            SELECT HOUR(m.measurement_datetime)                               AS hour,
                   COUNT(*)                                                    AS record_count,
                   ROUND(AVG(m.global_active_power), 4)                       AS avg_power_kW,
                   ROUND(MAX(m.global_active_power), 4)                       AS peak_power_kW,
                   ROUND(MIN(m.global_active_power), 4)                       AS min_power_kW,
                   ROUND(AVG(m.voltage), 2)                                   AS avg_voltage_V
            FROM measurements m
            {where}
            GROUP BY hour
            ORDER BY hour
            """,
            params,
        )
        rows = cur.fetchall()

        return {
            "source": "SQL",
            "query":  "Hourly Stats (all-time)",
            "count":  len(rows),
            "data":   rows,
        }
    finally:
        conn.close()


@app.get("/sql/monthly-trend", summary="[SQL] Monthly consumption trend")
def get_sql_monthly_trend(
    household_id: Optional[int] = None,
    months: int = Query(48, ge=1, le=120, description="Number of months to return"),
):
    """Return month-by-month total and average active power (most recent first)."""
    conn = get_db_connection()
    try:
        params: list = []
        where = ""
        if household_id:
            where = "WHERE m.household_id = %s"
            params.append(household_id)
        params.append(months)

        cur = conn.cursor(dictionary=True)
        cur.execute(
            f"""
            SELECT DATE_FORMAT(m.measurement_datetime, '%Y-%m')   AS month,
                   COUNT(*)                                        AS record_count,
                   ROUND(SUM(m.global_active_power) / 60.0, 2)    AS total_energy_kWh,
                   ROUND(AVG(m.global_active_power), 4)            AS avg_power_kW,
                   ROUND(MAX(m.global_active_power), 4)            AS peak_power_kW,
                   ROUND(MIN(m.global_active_power), 4)            AS min_power_kW
            FROM measurements m
            {where}
            GROUP BY month
            ORDER BY month DESC
            LIMIT %s
            """,
            params,
        )
        rows = cur.fetchall()

        return {
            "source": "SQL",
            "query":  "Monthly Trend",
            "count":  len(rows),
            "data":   rows,
        }
    finally:
        conn.close()


@app.get("/sql/sub-metering", summary="[SQL] Sub-metering energy breakdown by appliance group")
def get_sql_sub_metering(household_id: Optional[int] = None):
    """Return total and average energy consumption per sub-metering group
    (Kitchen, Laundry/AC, Water Heater, Other) across the full dataset."""
    conn = get_db_connection()
    try:
        params: list = []
        hh_filter = ""
        if household_id:
            hh_filter = "AND m.household_id = %s"
            params.append(household_id)

        cur = conn.cursor(dictionary=True)
        cur.execute(
            f"""
            SELECT 'Kitchen (Sub-meter 1)'   AS appliance_group,
                   ROUND(AVG(s.sub_metering_1), 4)   AS avg_wh_per_min,
                   ROUND(SUM(s.sub_metering_1)/1000, 2) AS total_kWh,
                   COUNT(*)                           AS readings
            FROM sub_metering s
            JOIN measurements m ON s.measurement_id = m.measurement_id
            WHERE 1=1 {hh_filter}
            UNION ALL
            SELECT 'Laundry / AC (Sub-meter 2)',
                   ROUND(AVG(s.sub_metering_2), 4),
                   ROUND(SUM(s.sub_metering_2)/1000, 2),
                   COUNT(*)
            FROM sub_metering s
            JOIN measurements m ON s.measurement_id = m.measurement_id
            WHERE 1=1 {hh_filter}
            UNION ALL
            SELECT 'Water Heater (Sub-meter 3)',
                   ROUND(AVG(s.sub_metering_3), 4),
                   ROUND(SUM(s.sub_metering_3)/1000, 2),
                   COUNT(*)
            FROM sub_metering s
            JOIN measurements m ON s.measurement_id = m.measurement_id
            WHERE 1=1 {hh_filter}
            ORDER BY total_kWh DESC
            """,
            params * 3,
        )
        rows = cur.fetchall()

        return {
            "source": "SQL",
            "query":  "Sub-metering Energy Breakdown",
            "count":  len(rows),
            "data":   rows,
        }
    finally:
        conn.close()

# ============================================================================
# MONGODB — MEASUREMENT CRUD
# ============================================================================

@app.post("/mongo/measurements", status_code=201, summary="[MongoDB] Create document")
def create_mongo_measurement(body: MeasurementCreate):
    ts_str = body.measurement_datetime
    # 409 if this timestamp already exists in the store
    for existing_doc in mongo_store.values():
        if existing_doc.get("timestamp") == ts_str:
            raise HTTPException(
                status_code=409,
                detail=f"MongoDB document with timestamp '{ts_str}' already exists (_id={existing_doc['_id']})",
            )
    doc_id = get_next_mongo_id()
    try:
        ts_dt  = datetime.fromisoformat(ts_str)
    except ValueError:
        ts_dt  = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")

    doc: dict[str, Any] = {
        "_id":                   doc_id,
        "household_id":          body.household_id,
        "timestamp":             ts_str,
        "date":                  ts_str[:10],
        "hour":                  ts_dt.hour,
        "day_of_week":           ts_dt.weekday(),
        "global_active_power":   body.global_active_power,
        "global_reactive_power": body.global_reactive_power,
        "voltage":               body.voltage,
        "global_intensity":      body.global_intensity,
        "sub_metering": [
            {"meter_id": 1, "name": "Kitchen",
             "consumption_wh": body.sub_metering_1 or 0.0},
            {"meter_id": 2, "name": "Laundry / AC",
             "consumption_wh": body.sub_metering_2 or 0.0},
            {"meter_id": 3, "name": "Water Heater",
             "consumption_wh": body.sub_metering_3 or 0.0},
        ],
        "created_at": datetime.now().isoformat(),
    }
    mongo_store[doc_id] = doc
    return {
        "status":    "created",
        "_id":       doc_id,
        "timestamp": ts_str,
        "message":   "Document created in MongoDB",
    }


@app.get("/mongo/measurements", summary="[MongoDB] List documents")
def list_mongo_measurements(limit: int = Query(10, ge=1, le=1000)):
    """Return most-recent documents, sorted descending by timestamp."""
    total = len(mongo_store)
    sorted_docs = sorted(
        mongo_store.values(),
        key=lambda d: d.get("timestamp", ""),
        reverse=True,
    )
    page = sorted_docs[:limit]
    return {
        "source":         "MongoDB",
        "total_in_store": total,
        "count":          len(page),
        "data":           page,
    }


@app.get("/mongo/measurements/{doc_id}", summary="[MongoDB] Get document by ID")
def get_mongo_measurement(doc_id: str):
    if doc_id not in mongo_store:
        raise HTTPException(status_code=404, detail=f"Document {doc_id} not found")
    return {"source": "MongoDB", "data": mongo_store[doc_id]}


@app.put("/mongo/measurements/{doc_id}", summary="[MongoDB] Update document")
def update_mongo_measurement(doc_id: str, body: MeasurementUpdate):
    if doc_id not in mongo_store:
        raise HTTPException(status_code=404, detail=f"Document {doc_id} not found")
    updates = body.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields provided for update")
    mongo_store[doc_id].update(updates)
    return {"status": "updated", "_id": doc_id, "updated_fields": updates}


@app.delete("/mongo/measurements/{doc_id}", summary="[MongoDB] Delete document")
def delete_mongo_measurement(doc_id: str):
    if doc_id not in mongo_store:
        raise HTTPException(status_code=404, detail=f"Document {doc_id} not found")
    mongo_store.pop(doc_id)
    return {"status": "deleted", "_id": doc_id, "message": "Document deleted from MongoDB"}


# ============================================================================
# MONGODB — TIME-SERIES ENDPOINTS
# ============================================================================

@app.get("/mongo/latest", summary="[MongoDB] Latest document")
def get_mongo_latest(household_id: Optional[int] = None):
    docs = list(mongo_store.values())
    if household_id:
        docs = [d for d in docs if d.get("household_id") == household_id]
    if not docs:
        raise HTTPException(status_code=404, detail="No documents found")
    latest = max(docs, key=lambda d: d.get("timestamp", ""))
    return {"source": "MongoDB", "query": "Latest Record", "data": latest}


@app.get("/mongo/date-range", summary="[MongoDB] Documents in date range")
def get_mongo_date_range(
    start_date:   str = Query(..., description="Start date YYYY-MM-DD"),
    end_date:     str = Query(..., description="End date YYYY-MM-DD"),
    household_id: Optional[int] = None,
    limit:        int = Query(100, ge=1, le=5000),
):
    docs = [
        d for d in mongo_store.values()
        if start_date <= d.get("date", "") <= end_date
    ]
    if household_id:
        docs = [d for d in docs if d.get("household_id") == household_id]
    docs.sort(key=lambda d: d.get("timestamp", ""))
    page = docs[:limit]
    return {
        "source":     "MongoDB",
        "query":      "Date Range",
        "start_date": start_date,
        "end_date":   end_date,
        "count":      len(page),
        "data":       page,
    }


@app.get("/mongo/hourly-stats", summary="[MongoDB] Hourly aggregate stats")
def get_mongo_hourly_stats(household_id: Optional[int] = None):
    """Return per-hour (0-23) avg / peak / min power from the in-memory store."""
    docs = list(mongo_store.values())
    if household_id:
        docs = [d for d in docs if d.get("household_id") == household_id]

    buckets: dict[int, list[float]] = defaultdict(list)
    for d in docs:
        h = d.get("hour")
        p = d.get("global_active_power")
        if h is not None and p is not None:
            buckets[int(h)].append(float(p))

    stats = []
    for hour in sorted(buckets):
        vals = buckets[hour]
        stats.append({
            "hour":          hour,
            "record_count":  len(vals),
            "avg_power_kW":  round(sum(vals) / len(vals), 4),
            "peak_power_kW": round(max(vals), 4),
            "min_power_kW":  round(min(vals), 4),
        })

    return {
        "source": "MongoDB",
        "query":  "Hourly Stats",
        "count":  len(stats),
        "data":   stats,
    }


@app.get("/mongo/daily-summary", summary="[MongoDB] Daily energy summary")
def get_mongo_daily_summary(
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD (optional)"),
    end_date:   Optional[str] = Query(None, description="End date YYYY-MM-DD (optional)"),
    limit:      int            = Query(30, ge=1, le=500),
):
    """Aggregate per-day total/avg/peak power from the in-memory store."""
    docs = list(mongo_store.values())
    if start_date:
        docs = [d for d in docs if d.get("date", "") >= start_date]
    if end_date:
        docs = [d for d in docs if d.get("date", "") <= end_date]

    daily: dict[str, list[float]] = defaultdict(list)
    for d in docs:
        date = d.get("date")
        p    = d.get("global_active_power")
        if date and p is not None:
            daily[date].append(float(p))

    summaries = []
    for date in sorted(daily, reverse=True)[:limit]:
        vals = daily[date]
        summaries.append({
            "date":             date,
            "record_count":     len(vals),
            "total_energy_kWh": round(sum(vals) / 60.0, 4),
            "avg_power_kW":     round(sum(vals) / len(vals), 4),
            "peak_power_kW":    round(max(vals), 4),
        })

    return {
        "source": "MongoDB",
        "query":  "Daily Summary",
        "count":  len(summaries),
        "data":   summaries,
    }


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    print("Starting Household Power Consumption API ...")
    print("Swagger Docs : http://localhost:8000/docs")
    print("Run tests    : python task3_api/test_api.py")
    uvicorn.run(app, host="0.0.0.0", port=8000)
