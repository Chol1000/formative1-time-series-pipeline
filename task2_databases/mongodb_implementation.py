
from __future__ import annotations

import contextlib
import io
import json
import warnings
import mysql.connector
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

warnings.filterwarnings(
    "ignore",
    message=".*pandas only supports SQLAlchemy connectable.*",
)

import pandas as pd

BASE_DIR            = Path(__file__).resolve().parent.parent
TASK2_DIR           = Path(__file__).resolve().parent
OUTPUTS_DIR         = TASK2_DIR.parent / "outputs" / "task2"
RAW_FILE            = BASE_DIR / "household_power_consumption.txt"
SAMPLE_DOCS_PATH    = OUTPUTS_DIR / "sample_documents.json"
MONGO_RESULTS_PATH  = OUTPUTS_DIR / "mongodb_query_results.txt"
MONGO_SCHEMA_PATH   = OUTPUTS_DIR / "mongodb_collection_design.txt"
N_DOCS    = 50_000
MONGO_URI = "mongodb://localhost:27017"
DB_NAME   = "household_power"

# MySQL connection config (matches sql_database.py)
DB_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",
    "password": "",
    "database": "household_power",
}

# Schema and query templates loaded from external .js files
_SCHEMA_FILE    = TASK2_DIR / "collection_schema.js"
_QUERIES_FILE   = TASK2_DIR / "query_templates.js"
COLLECTION_SCHEMA = _SCHEMA_FILE.read_text()
QUERY_TEMPLATES   = _QUERIES_FILE.read_text()



# ---------------------------------------------------------------------------
# Document builder
# ---------------------------------------------------------------------------
def build_document(row: pd.Series, measurement_id: int) -> dict[str, Any]:
    ts = row["measurement_datetime"]
    ts_dt = ts if isinstance(ts, datetime) else pd.Timestamp(ts).to_pydatetime()
    return {
        "measurement_id": measurement_id,
        "household_id": 1,
        "household_info": {
            "name": "Household A",
            "location": "Sceaux, Hauts-de-Seine, France",
            "area_sqm": 95.0,
            "occupants": 4,
        },
        "timestamp": ts_dt,
        "date": ts_dt.strftime("%Y-%m-%d"),
        "hour": ts_dt.hour,
        "day_of_week": ts_dt.weekday(),
        "global_active_power":    round(float(row["global_active_power"]),    3),
        "global_reactive_power":  round(float(row["global_reactive_power"]),  3),
        "voltage":                round(float(row["voltage"]),                2),
        "global_intensity":       round(float(row["global_intensity"]),       2),
        "sub_metering": [
            {"meter_id": 1, "name": "Kitchen",
             "consumption_wh": round(float(row["sub_metering_1"]), 1)},
            {"meter_id": 2, "name": "Laundry / AC",
             "consumption_wh": round(float(row["sub_metering_2"]), 1)},
            {"meter_id": 3, "name": "Water Heater",
             "consumption_wh": round(float(row["sub_metering_3"]), 1)},
        ],
        "total_sub_metering_wh": round(
            float(row["sub_metering_1"] + row["sub_metering_2"] + row["sub_metering_3"]), 1
        ),
    }


def load_documents(n: int = N_DOCS) -> list[dict[str, Any]]:
    """Sample n documents evenly across the full 4-year dataset.
    Reads from the MySQL database (already built by sql_database.py) using a
    modulo-based row selection so documents span Dec 2006 → Nov 2010.
    Falls back to the raw CSV if the database is not yet populated.
    """
    step = 1
    try:
        conn  = mysql.connector.connect(**DB_CONFIG)
        cur   = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM measurements")
        total = cur.fetchone()[0]
        if total == 0:
            raise ValueError("measurements table is empty")
        step  = max(1, total // n)
        _sel  = (
            "SELECT m.measurement_datetime,"
            " m.global_active_power, m.global_reactive_power,"
            " m.voltage, m.global_intensity,"
            " COALESCE(s.sub_metering_1, 0) AS sub_metering_1,"
            " COALESCE(s.sub_metering_2, 0) AS sub_metering_2,"
            " COALESCE(s.sub_metering_3, 0) AS sub_metering_3"
            " FROM measurements m"
            " LEFT JOIN sub_metering s ON m.measurement_id = s.measurement_id"
        )
        # Fetch N-15 evenly-spaced rows spanning the full dataset
        import pandas as pd
        df = pd.read_sql(
            f"{_sel} WHERE (m.measurement_id - 1) % {step} = 0"
            f" ORDER BY m.measurement_datetime ASC LIMIT {n - 15}",
            conn,
        )
        # Append the last 15 actual rows so Q1 "latest 10" shows consecutive
        # end-of-dataset readings rather than a gap back to the step-sample tail
        df_tail = pd.read_sql(
            f"{_sel} ORDER BY m.measurement_datetime DESC LIMIT 15",
            conn,
        )
        cur.close()
        conn.close()
        df = pd.concat([df, df_tail], ignore_index=True)
        df["measurement_datetime"] = pd.to_datetime(df["measurement_datetime"])
    except Exception as exc:
        print(f"[mongo] MySQL unavailable ({exc}) — falling back to raw CSV")
        import pandas as pd
        df = pd.read_csv(RAW_FILE, sep=";", na_values="?", low_memory=False)
        df["measurement_datetime"] = pd.to_datetime(
            df["Date"] + " " + df["Time"], dayfirst=True
        )
        df.columns = [c.lower() for c in df.columns]
        for col in ["global_active_power", "global_reactive_power", "voltage",
                    "global_intensity", "sub_metering_1", "sub_metering_2", "sub_metering_3"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.ffill().dropna(subset=["global_active_power"]).reset_index(drop=True)
        step = max(1, len(df) // n)
        df = df.iloc[::step].head(n).reset_index(drop=True)
    span_start = df["measurement_datetime"].iloc[0].date()
    span_end   = df["measurement_datetime"].iloc[-1].date()
    print(f"[mongo] Sampled {len(df):,} documents "
          f"(1 per ~{step} min) spanning {span_start} → {span_end}")
    return [build_document(row, i + 1) for i, row in enumerate(df.to_dict("records"))]


class MongoDBBackend:

    def __init__(self, uri: str = MONGO_URI, db_name: str = DB_NAME):
        from pymongo import MongoClient, ASCENDING, DESCENDING
        self._ASCENDING  = ASCENDING
        self._DESCENDING = DESCENDING
        self.client = MongoClient(uri, serverSelectionTimeoutMS=3_000)
        self.client.server_info()   # raises ServerSelectionTimeoutError if down
        self.db  = self.client[db_name]
        self.col = self.db["power_readings"]
        print(f"[mongo] Connected  uri={uri}  db={db_name}")

    def setup(self, documents: list[dict]) -> None:
        self.col.drop()
        self.db["daily_summaries"].drop()
        self.col.insert_many(documents)
        self.col.create_index([("household_id", 1), ("timestamp", self._DESCENDING)])
        self.col.create_index([("timestamp", self._ASCENDING)])
        self.col.create_index([("date",  1)])
        self.col.create_index([("hour",  1)])
        print(f"[mongo] Inserted {len(documents)} documents; indexes created")
        # Populate daily_summaries via $out aggregation pipeline
        self.col.aggregate([
            {"$group": {
                "_id":             "$date",
                "household_id":    {"$first": "$household_id"},
                "avg_active_power": {"$avg":  "$global_active_power"},
                "max_active_power": {"$max":  "$global_active_power"},
                "total_readings":   {"$sum":  1},
            }},
            {"$sort":  {"_id": 1}},
            {"$out":   "daily_summaries"},
        ])
        self.db["daily_summaries"].create_index(
            [("household_id", self._ASCENDING), ("_id", self._ASCENDING)]
        )
        n_daily = self.db["daily_summaries"].count_documents({})
        print(f"[mongo] daily_summaries populated: {n_daily} day-records (via $out pipeline)")

    def q1_latest(self, n: int = 10) -> None:
        results = list(
            self.col.find(
                {"household_id": 1},
                {"_id": 0, "timestamp": 1, "global_active_power": 1, "voltage": 1},
            ).sort("timestamp", self._DESCENDING).limit(n)
        )
        print(f"  {len(results)} documents returned (showing all {len(results)}):")
        for r in results:
            print(f"    {str(r['timestamp'])[:19]}  "
                  f"power={r['global_active_power']:.3f} kW  "
                  f"voltage={r['voltage']:.2f} V")

    def q2_date_range(self, start: str, end: str) -> None:
        s = datetime.fromisoformat(start)
        e = datetime.fromisoformat(end + "T23:59:59")
        results = list(
            self.col.find(
                {"household_id": 1, "timestamp": {"$gte": s, "$lte": e}},
                {"_id": 0, "timestamp": 1, "global_active_power": 1},
            ).sort("timestamp", self._ASCENDING)
        )
        print(f"  {len(results)} documents in range {start} → {end}  (first 10 shown):")
        for r in results[:10]:
            print(f"    {str(r['timestamp'])[:19]}  power={r['global_active_power']:.3f} kW")

    def q3_hourly_agg(self) -> None:
        pipeline = [
            {"$group": {"_id": "$hour",
                        "avg_power": {"$avg": "$global_active_power"},
                        "max_power": {"$max": "$global_active_power"},
                        "count":     {"$sum": 1}}},
            {"$sort": {"_id": 1}},
        ]
        rows = list(self.col.aggregate(pipeline))
        _print_hourly_table(rows)

    def q4_sub_metering_agg(self) -> None:
        pipeline = [
            {"$unwind": "$sub_metering"},
            {"$group": {"_id":      "$sub_metering.name",
                        "avg_wh":   {"$avg": "$sub_metering.consumption_wh"},
                        "total_wh": {"$sum": "$sub_metering.consumption_wh"},
                        "count":    {"$sum": 1}}},
            {"$sort": {"total_wh": -1}},
        ]
        rows = list(self.col.aggregate(pipeline))
        _print_submeter_table(rows)

    def q5_daily_summary(self) -> None:
        # Query the pre-aggregated daily_summaries collection (populated in setup() via $out)
        total = self.db["daily_summaries"].count_documents({})
        rows = list(
            self.db["daily_summaries"]
                .find({}, {"_id": 1, "avg_active_power": 1,
                           "max_active_power": 1, "total_readings": 1})
                .sort("_id", 1)
                .limit(20)
        )
        remapped = [{"_id":       r["_id"],
                     "avg_power": r["avg_active_power"],
                     "max_power": r["max_active_power"],
                     "readings":  r["total_readings"]}
                    for r in rows]
        _print_daily_table(remapped)
        if total > 20:
            print(f"  ... ({total - 20} more days not shown; {total} total day-records in collection)")

    def close(self) -> None:
        self.client.close()


class JSONSimulation:

    def __init__(self, documents: list[dict]) -> None:
        self.docs = documents
        print("[mongo] MongoDB not reachable — running in JSON simulation mode")
        print("        (results are identical to what pymongo would return)")
        # Pre-build daily_summaries — mirrors the $out aggregation pipeline in MongoDBBackend.setup()
        buckets: dict[str, list] = defaultdict(list)
        for d in self.docs:
            buckets[d["date"]].append(d["global_active_power"])
        self.daily_summaries = sorted(
            [{"_id":              dt,
              "avg_active_power": sum(v) / len(v),
              "max_active_power": max(v),
              "total_readings":   len(v)}
             for dt, v in buckets.items()],
            key=lambda r: r["_id"],
        )

    def q1_latest(self, n: int = 10) -> None:
        results = sorted(self.docs, key=lambda d: d["timestamp"], reverse=True)[:n]
        print(f"  {len(results)} documents returned:")
        for r in results:
            print(f"    {str(r['timestamp'])[:19]}  "
                  f"power={r['global_active_power']:.3f} kW  "
                  f"voltage={r['voltage']:.2f} V")

    def q2_date_range(self, start: str, end: str) -> None:
        s = datetime.fromisoformat(start)
        e = datetime.fromisoformat(end + "T23:59:59")
        results = sorted(
            [d for d in self.docs if s <= d["timestamp"] <= e],
            key=lambda d: d["timestamp"],
        )
        print(f"  {len(results)} documents in range {start} → {end}  (first 10 shown):")
        for r in results[:10]:
            print(f"    {str(r['timestamp'])[:19]}  power={r['global_active_power']:.3f} kW")

    def q3_hourly_agg(self) -> None:
        buckets: dict = defaultdict(list)
        for d in self.docs:
            buckets[d["hour"]].append(d["global_active_power"])
        rows = [{"_id": h, "avg_power": sum(v) / len(v),
                 "max_power": max(v), "count": len(v)}
                for h, v in buckets.items()]
        rows.sort(key=lambda r: r["_id"])
        _print_hourly_table(rows)

    def q4_sub_metering_agg(self) -> None:
        buckets: dict = defaultdict(lambda: {"wh": [], "count": 0})
        for d in self.docs:
            for m in d["sub_metering"]:
                buckets[m["name"]]["wh"].append(m["consumption_wh"])
                buckets[m["name"]]["count"] += 1
        rows = [{"_id": name,
                 "avg_wh":   sum(v["wh"]) / len(v["wh"]),
                 "total_wh": sum(v["wh"]),
                 "count":    v["count"]}
                for name, v in buckets.items()]
        rows.sort(key=lambda r: -r["total_wh"])
        _print_submeter_table(rows)

    def q5_daily_summary(self) -> None:
        # Read from pre-built daily_summaries (mirrors querying the $out collection)
        total = len(self.daily_summaries)
        remapped = [{"_id":       r["_id"],
                     "avg_power": r["avg_active_power"],
                     "max_power": r["max_active_power"],
                     "readings":  r["total_readings"]}
                    for r in self.daily_summaries[:20]]
        _print_daily_table(remapped)
        if total > 20:
            print(f"  ... ({total - 20} more days not shown; {total} total day-records)")

    def close(self) -> None:
        pass


def _print_hourly_table(rows: list[dict]) -> None:
    hdr = f"  {'Hour':>4}  {'Avg Power (kW)':>15}  {'Max Power (kW)':>15}  {'Count':>7}"
    print(hdr)
    print("  " + "─" * (len(hdr) - 2))
    for r in rows:
        print(f"  {r['_id']:>4}  {r['avg_power']:>15.3f}  {r['max_power']:>15.3f}  {r['count']:>7}")


def _print_submeter_table(rows: list[dict]) -> None:
    hdr = f"  {'Appliance':<22}  {'Avg Wh/min':>12}  {'Total Wh':>12}  {'Readings':>9}"
    print(hdr)
    print("  " + "─" * (len(hdr) - 2))
    for r in rows:
        print(f"  {r['_id']:<22}  {r['avg_wh']:>12.3f}  {r['total_wh']:>12.1f}  {r['count']:>9}")


def _print_daily_table(rows: list[dict]) -> None:
    hdr = f"  {'Date':>12}  {'Avg Power (kW)':>15}  {'Peak (kW)':>10}  {'Readings':>9}"
    print(hdr)
    print("  " + "─" * (len(hdr) - 2))
    for r in rows:
        print(f"  {r['_id']:>12}  {r['avg_power']:>15.3f}  {r['max_power']:>10.3f}  {r['readings']:>9}")


def _hdr(label: str) -> None:
    print("\n" + "=" * 72)
    print(f"  {label}")
    print("=" * 72)


def run_mongodb_pipeline(n_docs: int = N_DOCS) -> None:
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    print("=" * 72)
    print("TASK 2B — MongoDB Collection Design & Queries")
    print("=" * 72)

    MONGO_SCHEMA_PATH.write_text(COLLECTION_SCHEMA + QUERY_TEMPLATES)
    print(f"[mongo] Collection design saved → {MONGO_SCHEMA_PATH}")

    # Load real documents
    documents = load_documents(n_docs)

    # Save 3 sample documents spanning the full date range:
    #   doc[0]       → start of dataset  (Dec 2006)
    #   doc[N//2]    → middle of dataset (~mid-2008)
    #   doc[-1]      → end of dataset    (Nov 2010)
    n = len(documents)
    sample_docs = [documents[0], documents[n // 2], documents[-1]]
    SAMPLE_DOCS_PATH.write_text(json.dumps(sample_docs, indent=2, default=str))
    print(f"[mongo] Sample documents saved → {SAMPLE_DOCS_PATH}")
    span = (f"{documents[0]['date']} / "
            f"{documents[n // 2]['date']} / "
            f"{documents[-1]['date']}")
    print(f"[mongo] Sample doc timestamps: {span}")

    # Print 3 sample documents
    _hdr(f"Sample Documents (3 of {len(documents):,} total — start / mid / end of dataset)")
    for doc in sample_docs:
        print(json.dumps(doc, indent=2, default=str))

    # Connect to MongoDB or fall back to simulation
    try:
        backend: MongoDBBackend | JSONSimulation = MongoDBBackend()
        backend.setup(documents)
    except Exception as exc:
        print(f"\n[mongo] Connection failed ({exc})")
        backend = JSONSimulation(documents)

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        _hdr("QUERY 1 — Latest 10 Readings  (sort timestamp DESC, limit 10)")
        backend.q1_latest(10)
        _hdr("QUERY 2 — Date Range  (2006-12-16 → 2006-12-19)")
        backend.q2_date_range("2006-12-16", "2006-12-19")
        _hdr("QUERY 3 — Hourly Aggregation  ($group by hour)")
        backend.q3_hourly_agg()
        _hdr("QUERY 4 — Sub-metering Breakdown  ($unwind + $group by appliance)")
        backend.q4_sub_metering_agg()
        _hdr("QUERY 5 — Daily Summary  ($group by date)")
        backend.q5_daily_summary()
    query_output = buf.getvalue()
    print(query_output, end="")
    MONGO_RESULTS_PATH.write_text(query_output)
    print(f"[results] MongoDB query results saved → {MONGO_RESULTS_PATH}")

    backend.close()
    print("\n[done] MongoDB implementation complete")


if __name__ == "__main__":
    run_mongodb_pipeline()
