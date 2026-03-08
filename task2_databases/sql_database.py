from __future__ import annotations

import contextlib
import io
import warnings
from pathlib import Path

warnings.filterwarnings(
    "ignore",
    message=".*pandas only supports SQLAlchemy connectable.*",
)

import matplotlib
matplotlib.use("Agg")
import matplotlib.patches as mpatches
import matplotlib.lines   as mlines
import matplotlib.pyplot  as plt
import mysql.connector
import pandas as pd

BASE_DIR         = Path(__file__).resolve().parent.parent
TASK2_DIR        = Path(__file__).resolve().parent
OUTPUTS_DIR      = TASK2_DIR.parent / "outputs" / "task2"
ROOT_OUTPUTS_DIR = TASK2_DIR.parent / "outputs"
RAW_FILE         = BASE_DIR / "household_power_consumption.txt"
ERD_PATH         = OUTPUTS_DIR / "erd_diagram.png"
ROOT_ERD_PATH    = ROOT_OUTPUTS_DIR / "erd_diagram.png"
SCHEMA_PATH      = OUTPUTS_DIR / "schema.sql"
RESULTS_PATH     = OUTPUTS_DIR / "sql_query_results.txt"
NROWS_LOAD       = None   # None = full dataset (~2.07 M rows)

# ---------------------------------------------------------------------------
# MySQL connection config  (edit host/user/password as needed)
# ---------------------------------------------------------------------------
DB_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",
    "password": "",
    "database": "household_power",
}

# DDL loaded from schema_source.sql — one statement per semicolon-delimited block
_DDL_FILE   = TASK2_DIR / "schema.sql"
SCHEMA_DDL  = [s.strip() for s in _DDL_FILE.read_text().split(";") if s.strip()]


def get_connection():
    return mysql.connector.connect(**DB_CONFIG)


def generate_erd_png(output_path: Path = ERD_PATH) -> None:
    """Generate a professional ERD PNG for the MySQL household_power schema."""
    fig, ax = plt.subplots(figsize=(19, 12))
    ax.set_xlim(0, 19)
    ax.set_ylim(0, 12)
    ax.axis("off")
    fig.patch.set_facecolor("#F8FAFC")
    ax.set_facecolor("#F8FAFC")

    ROW_H = 0.44
    HDR_H = 0.62

    def draw_table(x, y, w, title, color, fields):
        """Draw one table box. fields = list of (tag, col_name, dtype)."""
        body_h = len(fields) * ROW_H + 0.14
        total_h = HDR_H + body_h

        # Drop shadow
        ax.add_patch(mpatches.FancyBboxPatch(
            (x + 0.08, y - 0.08), w, total_h,
            boxstyle="round,pad=0.06", fc="#C8D0D8", ec="none", zorder=1,
        ))
        # Header
        ax.add_patch(mpatches.FancyBboxPatch(
            (x, y + body_h), w, HDR_H,
            boxstyle="round,pad=0.06", fc=color, ec="none", zorder=2,
        ))
        ax.text(x + w / 2, y + body_h + HDR_H / 2, title,
                ha="center", va="center", fontsize=10.5,
                fontweight="bold", color="white", zorder=3)
        # Body box
        ax.add_patch(mpatches.FancyBboxPatch(
            (x, y), w, body_h,
            boxstyle="round,pad=0.06", fc="white", ec=color, lw=2.0, zorder=2,
        ))
        # Divider under header
        ax.plot([x + 0.06, x + w - 0.06], [y + body_h, y + body_h],
                color=color, lw=1.2, alpha=0.5, zorder=3)

        for i, (tag, col_name, dtype) in enumerate(fields):
            fy = y + body_h - (i + 0.5) * ROW_H - 0.07
            # Alternating row tint
            if i % 2 == 1:
                ax.add_patch(mpatches.FancyBboxPatch(
                    (x + 0.05, fy - ROW_H * 0.48), w - 0.10, ROW_H * 0.96,
                    boxstyle="round,pad=0.02", fc="#EEF2F7", ec="none", zorder=2,
                ))
            # Tag badge (PK / FK)
            if tag == "PK":
                badge_col, name_col, fw = "#B71C1C", "#B71C1C", "bold"
            elif tag == "FK":
                badge_col, name_col, fw = "#0D47A1", "#0D47A1", "bold"
            else:
                badge_col, name_col, fw = None, "#263238", "normal"

            if badge_col:
                ax.add_patch(mpatches.FancyBboxPatch(
                    (x + 0.10, fy - 0.11), 0.32, 0.22,
                    boxstyle="round,pad=0.03", fc=badge_col, ec="none", zorder=3,
                ))
                ax.text(x + 0.26, fy, tag, ha="center", va="center",
                        fontsize=6.2, color="white", fontweight="bold", zorder=4)
                name_x = x + 0.50
            else:
                name_x = x + 0.18

            ax.text(name_x, fy, col_name, va="center",
                    fontsize=8.4, color=name_col, fontweight=fw, zorder=3)
            ax.text(x + w - 0.12, fy, dtype, va="center", ha="right",
                    fontsize=7.0, color="#78909C", fontstyle="italic", zorder=3)

        return total_h

    # ------------------------------------------------------------------
    # Tables
    # ------------------------------------------------------------------
    draw_table(0.4, 6.8, 4.2, "households", "#1565C0", [
        ("PK", "household_id",   "INT AUTO_INCREMENT"),
        ("",   "household_name", "VARCHAR(255) NOT NULL"),
        ("",   "location",       "VARCHAR(255)"),
        ("",   "area_sqm",       "DOUBLE"),
        ("",   "occupants",      "INT"),
        ("",   "created_date",   "TIMESTAMP"),
    ])

    draw_table(5.6, 4.8, 4.8, "measurements", "#2E7D32", [
        ("PK", "measurement_id",        "INT AUTO_INCREMENT"),
        ("FK", "household_id",          "INT NOT NULL"),
        ("",   "measurement_datetime",  "DATETIME NOT NULL"),
        ("",   "global_active_power",   "DOUBLE  [kW]"),
        ("",   "global_reactive_power", "DOUBLE  [kVAR]"),
        ("",   "voltage",               "DOUBLE  [V]"),
        ("",   "global_intensity",      "DOUBLE  [A]"),
    ])

    draw_table(5.6, 0.3, 4.8, "sub_metering", "#6A1B9A", [
        ("PK", "sub_meter_id",   "INT AUTO_INCREMENT"),
        ("FK", "measurement_id", "INT NOT NULL UNIQUE"),
        ("",   "sub_metering_1", "DOUBLE  [Kitchen Wh]"),
        ("",   "sub_metering_2", "DOUBLE  [Laundry/AC Wh]"),
        ("",   "sub_metering_3", "DOUBLE  [Water Heater Wh]"),
    ])

    draw_table(11.5, 4.8, 5.1, "hourly_aggregates", "#E65100", [
        ("PK", "hourly_id",         "INT AUTO_INCREMENT"),
        ("FK", "household_id",      "INT NOT NULL"),
        ("",   "hour_datetime",     "DATETIME NOT NULL"),
        ("",   "avg_active_power",  "DOUBLE  [kW]"),
        ("",   "max_active_power",  "DOUBLE  [kW]"),
        ("",   "min_active_power",  "DOUBLE  [kW]"),
        ("",   "total_consumption", "DOUBLE  [kWh proxy]"),
        ("",   "reading_count",     "INT"),
    ])

    # ------------------------------------------------------------------
    # Relationship arrows
    # ------------------------------------------------------------------
    def rel_arrow(x1, y1, x2, y2, card1, cardN, rad=0.0, dashed=False):
        col  = "#90A4AE" if dashed else "#455A64"
        ls   = "dashed"  if dashed else "solid"
        lw   = 1.4       if dashed else 2.0
        ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                    arrowprops=dict(
                        arrowstyle="-|>", color=col, lw=lw,
                        mutation_scale=15, linestyle=ls,
                        connectionstyle=f"arc3,rad={rad}",
                    ), zorder=4)
        # Cardinality labels near each endpoint
        dx, dy = x2 - x1, y2 - y1
        ax.text(x1 + dx * 0.09, y1 + dy * 0.09 + 0.20,
                card1, ha="center", fontsize=9, fontweight="bold",
                color=col, zorder=5)
        ax.text(x1 + dx * 0.91, y1 + dy * 0.91 + 0.20,
                cardN, ha="center", fontsize=9, fontweight="bold",
                color=col, zorder=5)

    # households → measurements  (1 : M)
    rel_arrow(4.6, 8.75, 5.6, 8.2, "1", "M")
    # households → hourly_aggregates  (1 : M)
    rel_arrow(4.6, 9.05, 11.5, 8.8, "1", "M", rad=-0.20)
    # measurements → sub_metering  (1 : 1)
    rel_arrow(8.0, 4.8, 8.0, 3.07, "1", "1")
    # measurements → hourly_aggregates  (derived/aggregated — dashed)
    ax.annotate("", xy=(11.5, 7.35), xytext=(10.4, 7.35),
                arrowprops=dict(
                    arrowstyle="-|>", color="#90A4AE", lw=1.3,
                    linestyle="dashed", mutation_scale=12,
                    connectionstyle="arc3,rad=0",
                ), zorder=4)
    ax.text(10.95, 7.58, "aggregated into", ha="center",
            fontsize=7.2, color="#90A4AE", fontstyle="italic", zorder=5)

    # ------------------------------------------------------------------
    # Title, subtitle, legend
    # ------------------------------------------------------------------
    ax.set_title(
        "Entity-Relationship Diagram  —  Household Power Consumption  (MySQL)",
        fontsize=13.5, fontweight="bold", color="#1A237E", pad=20,
    )
    ax.text(9.5, -0.55,
            "Database: household_power  •  4 tables  •  ~2,075,259 measurement rows",
            ha="center", fontsize=8.5, color="#78909C", fontstyle="italic")

    ax.legend(
        handles=[
            mpatches.Patch(fc="#B71C1C", label="PK  Primary Key"),
            mpatches.Patch(fc="#0D47A1", label="FK  Foreign Key"),
            mlines.Line2D([], [], color="#455A64", lw=2.0,
                          label="Relationship  (FK constraint)"),
            mlines.Line2D([], [], color="#90A4AE", lw=1.3, linestyle="dashed",
                          label="Derived  (aggregated from measurements)"),
        ],
        loc="lower left", fontsize=8.5, framealpha=0.95,
        edgecolor="#B0BEC5", fancybox=True,
    )
    plt.tight_layout(pad=1.5)
    plt.savefig(output_path, dpi=150, bbox_inches="tight", facecolor="#F8FAFC")
    plt.close()
    print(f"[ERD]  Saved → {output_path}")


def create_schema(conn) -> None:
    cur = conn.cursor()
    for tbl in ["hourly_aggregates", "sub_metering", "measurements", "households"]:
        cur.execute(f"DROP TABLE IF EXISTS {tbl}")
    conn.commit()
    for stmt in SCHEMA_DDL:
        cur.execute(stmt.strip())
    conn.commit()
    cur.close()
    print("[schema] 4 tables created: households, measurements, sub_metering, hourly_aggregates")


def load_data(conn, nrows=NROWS_LOAD) -> None:
    label = f"{nrows:,}" if nrows else "all (~2.07 M)"
    print(f"[load]  Reading {label} rows from raw file ...")
    df = pd.read_csv(RAW_FILE, sep=";", na_values="?", low_memory=False)
    df["measurement_datetime"] = pd.to_datetime(
        df["Date"] + " " + df["Time"], dayfirst=True
    )
    df.columns = [c.lower() for c in df.columns]
    for col in ["global_active_power", "global_reactive_power", "voltage",
                "global_intensity", "sub_metering_1", "sub_metering_2", "sub_metering_3"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.ffill().dropna(subset=["global_active_power"])
    if nrows:
        df = df.head(nrows)
    df = df.reset_index(drop=True)

    cur = conn.cursor()

    # Household
    cur.execute(
        "INSERT IGNORE INTO households (household_name, location, area_sqm, occupants) "
        "VALUES (%s, %s, %s, %s)",
        ("Household A", "Sceaux, Hauts-de-Seine, France", 95.0, 4),
    )
    conn.commit()
    cur.execute("SELECT household_id FROM households WHERE household_name=%s", ("Household A",))
    hh_id = cur.fetchone()[0]

    # Measurements — bulk insert in chunks of 5000
    meas_rows = list(zip(
        [hh_id] * len(df),
        df["measurement_datetime"].astype(str),
        df["global_active_power"],
        df["global_reactive_power"],
        df["voltage"],
        df["global_intensity"],
    ))
    chunk = 5000
    for i in range(0, len(meas_rows), chunk):
        cur.executemany(
            "INSERT INTO measurements "
            "(household_id, measurement_datetime, global_active_power, "
            " global_reactive_power, voltage, global_intensity) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            meas_rows[i:i+chunk],
        )
        conn.commit()
        if (i // chunk) % 50 == 0:
            print(f"  Inserted {min(i+chunk, len(meas_rows)):,}/{len(meas_rows):,} measurements ...")

    # Get the inserted measurement IDs
    cur.execute(
        "SELECT measurement_id FROM measurements "
        "WHERE household_id=%s ORDER BY measurement_id ASC",
        (hh_id,),
    )
    mids = [r[0] for r in cur.fetchall()]

    # Sub-metering bulk insert
    sub_rows = list(zip(
        mids,
        df["sub_metering_1"],
        df["sub_metering_2"],
        df["sub_metering_3"],
    ))
    for i in range(0, len(sub_rows), chunk):
        cur.executemany(
            "INSERT INTO sub_metering "
            "(measurement_id, sub_metering_1, sub_metering_2, sub_metering_3) "
            "VALUES (%s,%s,%s,%s)",
            sub_rows[i:i+chunk],
        )
        conn.commit()

    cur.close()
    print(f"[load]  Inserted {len(meas_rows):,} measurements + sub-metering rows for household_id={hh_id}")


def build_hourly_aggregates(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        "INSERT IGNORE INTO hourly_aggregates "
        "(household_id, hour_datetime, avg_active_power, max_active_power, "
        " min_active_power, total_consumption, reading_count) "
        "SELECT household_id, "
        "       DATE_FORMAT(measurement_datetime, '%Y-%m-%d %H:00:00'), "
        "       ROUND(AVG(global_active_power),  4), "
        "       ROUND(MAX(global_active_power),  4), "
        "       ROUND(MIN(global_active_power),  4), "
        "       ROUND(SUM(global_active_power),  4), "
        "       COUNT(*) "
        "FROM measurements "
        "GROUP BY household_id, DATE_FORMAT(measurement_datetime, '%Y-%m-%d %H:00:00')"
    )
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM hourly_aggregates")
    n = cur.fetchone()[0]
    cur.close()
    print(f"[aggregates] {n} hourly aggregate rows computed")


def _hdr(label: str) -> None:
    print("\n" + "─" * 72)
    print(f"  {label}")
    print("─" * 72)


def run_queries(conn) -> None:

    # Q1: Latest 10 readings
    _hdr("QUERY 1 — Latest 10 Measurements (ORDER BY datetime DESC)")
    df = pd.read_sql(
        """SELECT h.household_name                               AS Household,
                  m.measurement_datetime                         AS DateTime,
                  ROUND(m.global_active_power,   3)             AS `Active(kW)`,
                  ROUND(m.global_reactive_power, 3)             AS `Reactive(kVAR)`,
                  ROUND(m.voltage,               2)             AS `Voltage(V)`,
                  ROUND(m.global_intensity,      2)             AS `Intensity(A)`
           FROM measurements m
           JOIN households h ON m.household_id = h.household_id
           ORDER BY m.measurement_datetime DESC LIMIT 10""",
        conn,
    )
    print(df.to_string(index=False))

    # Q2: Hourly averages over a date range
    _hdr("QUERY 2 — Hourly Averages for Date Range 2006-12-16 → 2006-12-19")
    df = pd.read_sql(
        """SELECT DATE(m.measurement_datetime)                AS Date,
                  HOUR(m.measurement_datetime)                AS Hour,
                  ROUND(AVG(m.global_active_power), 3)       AS `Avg Power(kW)`,
                  ROUND(MAX(m.global_active_power), 3)       AS `Peak Power(kW)`,
                  COUNT(*)                                    AS Readings
           FROM measurements m
           WHERE m.measurement_datetime BETWEEN '2006-12-16 00:00:00' AND '2006-12-19 23:59:59'
           GROUP BY DATE(m.measurement_datetime), HOUR(m.measurement_datetime)
           ORDER BY Date, Hour""",
        conn,
    )
    print(df.to_string(index=False))

    # Q3: Full join — measurements + sub-metering
    _hdr("QUERY 3 — Join: Measurements + Sub-metering (first 15 rows)")
    df = pd.read_sql(
        """SELECT m.measurement_datetime                     AS DateTime,
                  ROUND(m.global_active_power,  3)           AS `Total(kW)`,
                  ROUND(sm.sub_metering_1,      1)           AS `Kitchen(Wh)`,
                  ROUND(sm.sub_metering_2,      1)           AS `Laundry(Wh)`,
                  ROUND(sm.sub_metering_3,      1)           AS `WaterHtr(Wh)`,
                  ROUND(sm.sub_metering_1
                      + sm.sub_metering_2
                      + sm.sub_metering_3, 1)                AS `SubTotal(Wh)`
           FROM measurements m
           JOIN sub_metering sm ON m.measurement_id = sm.measurement_id
           ORDER BY m.measurement_datetime LIMIT 15""",
        conn,
    )
    print(df.to_string(index=False))

    # Q4: Top-10 peak-demand hours
    _hdr("QUERY 4 — Top-10 Peak Demand Hours (from hourly_aggregates)")
    df = pd.read_sql(
        """SELECT hour_datetime                       AS `Hour`,
                  ROUND(avg_active_power, 3)           AS `Avg Power(kW)`,
                  ROUND(max_active_power, 3)           AS `Peak Power(kW)`,
                  ROUND(total_consumption, 1)          AS `Total kW·min`,
                  reading_count                        AS Readings
           FROM hourly_aggregates
           WHERE reading_count >= 30
           ORDER BY avg_active_power DESC LIMIT 10""",
        conn,
    )
    print(df.to_string(index=False))

    # Q5: Monthly consumption trend
    _hdr("QUERY 5 — Monthly Consumption Trend (full dataset, ~48 months)")
    df = pd.read_sql(
        """SELECT DATE_FORMAT(measurement_datetime, '%Y-%m') AS Month,
                  ROUND(AVG(global_active_power), 3)            AS `Avg Power(kW)`,
                  ROUND(MAX(global_active_power), 3)            AS `Peak Power(kW)`,
                  ROUND(SUM(global_active_power), 1)            AS `Total kW·min`,
                  COUNT(*)                                       AS Readings
           FROM measurements
           GROUP BY DATE_FORMAT(measurement_datetime, '%Y-%m')
           ORDER BY Month""",
        conn,
    )
    print(df.to_string(index=False))

    # Q6: Sub-metering energy share
    _hdr("QUERY 6 — Sub-metering Energy Share per Appliance Group")
    df = pd.read_sql(
        """SELECT 'Kitchen (SM1)'              AS Appliance,
                  ROUND(AVG(sub_metering_1), 3)  AS `Avg Wh/min`,
                  ROUND(SUM(sub_metering_1), 0)  AS `Total Wh`
           FROM sub_metering
           UNION ALL
           SELECT 'Laundry / AC (SM2)',
                  ROUND(AVG(sub_metering_2), 3),
                  ROUND(SUM(sub_metering_2), 0)
           FROM sub_metering
           UNION ALL
           SELECT 'Water Heater (SM3)',
                  ROUND(AVG(sub_metering_3), 3),
                  ROUND(SUM(sub_metering_3), 0)
           FROM sub_metering""",
        conn,
    )
    print(df.to_string(index=False))

    # Q7: Day-of-week consumption pattern
    _hdr("QUERY 7 — Day-of-Week Consumption Pattern (Mon=2 … Sun=1)")
    df = pd.read_sql(
        """SELECT DAYNAME(m.measurement_datetime)          AS DayOfWeek,
                  DAYOFWEEK(m.measurement_datetime)         AS DayNum,
                  ROUND(AVG(m.global_active_power),  3)     AS `Avg Power(kW)`,
                  ROUND(MAX(m.global_active_power),  3)     AS `Peak Power(kW)`,
                  ROUND(MIN(m.global_active_power),  3)     AS `Min Power(kW)`,
                  COUNT(*)                                   AS Readings
           FROM measurements m
           GROUP BY DAYOFWEEK(m.measurement_datetime), DAYNAME(m.measurement_datetime)
           ORDER BY DAYOFWEEK(m.measurement_datetime)""",
        conn,
    )
    print(df.to_string(index=False))


def export_schema_sql(path: Path = SCHEMA_PATH) -> None:
    path.write_text(_DDL_FILE.read_text())
    print(f"[schema] SQL script saved  → {path}")


def run_sql_pipeline(nrows=NROWS_LOAD) -> None:
    print("=" * 72)
    print("TASK 2A — SQL DATABASE  (MySQL)")
    print("=" * 72)

    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    ROOT_OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_connection()

    create_schema(conn)
    load_data(conn, nrows)
    build_hourly_aggregates(conn)

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        run_queries(conn)
    query_output = buf.getvalue()
    print(query_output, end="")
    RESULTS_PATH.write_text(query_output)
    print(f"[results] SQL query results saved → {RESULTS_PATH}")

    export_schema_sql()
    generate_erd_png()                          # outputs/task2/erd_diagram.png
    generate_erd_png(ROOT_ERD_PATH)             # outputs/erd_diagram.png

    conn.close()
    print(f"\n[done] MySQL database \'household_power\'  host=localhost")


if __name__ == "__main__":
    run_sql_pipeline()