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
