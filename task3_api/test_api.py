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


