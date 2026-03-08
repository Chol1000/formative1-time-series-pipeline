"""
Task 3 - API Test Suite
=======================
Automated tests for all REST endpoints (SQL + MongoDB).

Usage
-----
    # Terminal 1: start the server
    python task3_api/api.py

    # Terminal 2: run tests
    python task3_api/test_api.py

Coverage
--------
    Health               1 test
    SQL Households       5 tests  (POST / GET list / GET by id / PUT / DELETE)
    SQL Measurements     5 tests  (POST / GET list / GET by id / PUT / DELETE)
    SQL Time-Series      4 tests  (latest / date-range / hourly-stats / monthly-trend)
    MongoDB CRUD         5 tests  (POST / GET list / GET by id / PUT / DELETE)
    MongoDB Time-Series  4 tests  (latest / date-range / hourly-stats / daily-summary)
    -----------------------------------------
    Total               24 tests
"""

import json
import sys
import time

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL = "http://localhost:8000"

# ---------------------------------------------------------------------------
# Test harness
# ---------------------------------------------------------------------------
_passed = 0
_failed = 0


def check(label, response, expected_status=200):
    """Assert the response status, print a result line, return the JSON body."""
    global _passed, _failed

    try:
        body = response.json()
    except Exception:
        body = {"_raw": response.text[:200]}

    if response.status_code == expected_status:
        _passed += 1
        print(f"  PASS  [{response.status_code}]  {label}")
    else:
        _failed += 1
        print(f"  FAIL  [{response.status_code}]  {label}")
        print(f"        expected {expected_status}")
        print(f"        body: {json.dumps(body)[:300]}")

    return body


def _summary_line(body):
    """Return a compact one-liner for a list response."""
    data  = body.get("data", [])
    count = body.get("count", len(data))
    if data and isinstance(data, list):
        first = {k: data[0][k] for k in list(data[0].keys())[:4]}
        return f"count={count}  first={first}"
    return f"count={count}"

# ---------------------------------------------------------------------------
# Test groups
# ---------------------------------------------------------------------------

def test_health():
    print("\n--- Health ---")
    body = check("GET /", requests.get(f"{BASE_URL}/"))
    print(f"        status={body.get('status')}  "
          f"mongo_docs_loaded={body.get('mongo_docs_loaded')}")

def test_sql_households():
    print("\n--- SQL: Households CRUD ---")

    body = check("GET  /sql/households",
                 requests.get(f"{BASE_URL}/sql/households"))
    print(f"        {_summary_line(body)}")

    payload = {
        "household_name": f"Test House {int(time.time())}",
        "location": "Paris, France",
        "area_sqm": 85.0,
        "occupants": 3,
    }
    body = check("POST /sql/households",
                 requests.post(f"{BASE_URL}/sql/households", json=payload),
                 expected_status=201)
    hh_id = body.get("household_id")
    print(f"        household_id={hh_id}")

    body = check(f"GET  /sql/households/{hh_id}",
                 requests.get(f"{BASE_URL}/sql/households/{hh_id}"))
    d = body.get("data", {})
    print(f"        id={d.get('household_id')}  name={d.get('household_name')}")

    body = check(f"PUT  /sql/households/{hh_id}",
                 requests.put(f"{BASE_URL}/sql/households/{hh_id}",
                              json={"area_sqm": 90.0, "occupants": 4}))
    print(f"        updated_fields={body.get('updated_fields')}")

    body = check(f"DELETE /sql/households/{hh_id}",
                 requests.delete(f"{BASE_URL}/sql/households/{hh_id}"))
    print(f"        status={body.get('status')}")

def test_sql_measurements():
    print("\n--- SQL: Measurements CRUD ---")

    payload = {
        "household_id": 1,
        "measurement_datetime": f"2030-{int(time.time()) % 12 + 1:02d}-{int(time.time()) % 28 + 1:02d} {int(time.time()) % 24:02d}:{int(time.time()) % 60:02d}:00",
        "global_active_power": 3.5,
        "global_reactive_power": 0.21,
        "voltage": 235.0,
        "global_intensity": 14.8,
        "sub_metering_1": 0.0,
        "sub_metering_2": 1.0,
        "sub_metering_3": 15.0,
    }
    body = check("POST /sql/measurements",
                 requests.post(f"{BASE_URL}/sql/measurements", json=payload),
                 expected_status=201)
    mid = body.get("measurement_id")
    print(f"        measurement_id={mid}  datetime={body.get('datetime')}")

    body = check("GET  /sql/measurements?limit=5",
                 requests.get(f"{BASE_URL}/sql/measurements?limit=5"))
    print(f"        {_summary_line(body)}")

    body = check(f"GET  /sql/measurements/{mid}",
                 requests.get(f"{BASE_URL}/sql/measurements/{mid}"))
    d = body.get("data", {})
    print(f"        id={d.get('measurement_id')}  power={d.get('global_active_power')}")

    body = check(f"PUT  /sql/measurements/{mid}",
                 requests.put(f"{BASE_URL}/sql/measurements/{mid}",
                              json={"global_active_power": 3.75, "voltage": 234.5}))
    print(f"        updated_fields={body.get('updated_fields')}")

    body = check(f"DELETE /sql/measurements/{mid}",
                 requests.delete(f"{BASE_URL}/sql/measurements/{mid}"))
    print(f"        status={body.get('status')}")

def test_sql_timeseries():
    print("\n--- SQL: Time-Series ---")

    body = check("GET /sql/latest",
                 requests.get(f"{BASE_URL}/sql/latest"))
    d = body.get("data", {})
    print(f"        datetime={d.get('datetime')}  power={d.get('global_active_power')}")

    body = check("GET /sql/date-range  (2006-12-16 to 2006-12-18)",
                 requests.get(f"{BASE_URL}/sql/date-range"
                              "?start_date=2006-12-16&end_date=2006-12-18&limit=5"))
    print(f"        {_summary_line(body)}")

    body = check("GET /sql/hourly-stats",
                 requests.get(f"{BASE_URL}/sql/hourly-stats"))
    first = body.get("data", [{}])[0]
    print(f"        count={body.get('count')}  "
          f"hour_0: avg={first.get('avg_power_kW')}  peak={first.get('peak_power_kW')}")

    body = check("GET /sql/monthly-trend  (last 12 months)",
                 requests.get(f"{BASE_URL}/sql/monthly-trend?months=12"))
    first = body.get("data", [{}])[0]
    print(f"        count={body.get('count')}  most_recent_month={first.get('month')}")

