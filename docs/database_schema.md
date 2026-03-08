# Database Schema Reference

This document covers both the **MySQL relational schema** and the **MongoDB document schema** used in Task 2 and Task 3.

---

## MySQL — Relational Database (`household_power`)

**Connection:** `localhost:3306`, user `root`, no password  
**Engine:** InnoDB (all tables)

### Entity-Relationship Overview

```
households (1)
    │
    ├── measurements (N) ──── sub_metering (1, 1:1 with measurements)
    │
    └── hourly_aggregates (N)
```

`households` is the root entity. Every measurement belongs to a household. `sub_metering` extends each measurement row with three appliance-level energy readings. `hourly_aggregates` is a pre-computed summary table built from `measurements`.

---
## Table: `households`

Stores metadata for each monitored household.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `household_id` | INT | PK, AUTO_INCREMENT | Surrogate primary key |
| `household_name` | VARCHAR(255) | NOT NULL, UNIQUE | Display name |
| `location` | VARCHAR(255) | nullable | Free-text address |
| `area_sqm` | DOUBLE | nullable | Floor area (m²) |
| `occupants` | INT | nullable | Number of residents |
| `created_date` | TIMESTAMP | DEFAULT NOW() | Row creation time |

**Seed data:** 1 row — `"Household A"`, Sceaux, Hauts-de-Seine, France.

---

### Table: `measurements`

One row per 1-minute electricity reading. Core fact table (~2.07 million rows).

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `measurement_id` | INT | PK, AUTO_INCREMENT | Surrogate primary key |
| `household_id` | INT | FK → `households` | Parent household |
| `measurement_datetime` | DATETIME | NOT NULL | Rounded to the minute |
| `global_active_power` | DOUBLE | NOT NULL | Active power (kW) |
| `global_reactive_power` | DOUBLE | nullable | Reactive power (kVAR) |
| `voltage` | DOUBLE | nullable | Line voltage (V) |
| `global_intensity` | DOUBLE | nullable | Current intensity (A) |

**Indexes:**
- `idx_meas_dt` on `(measurement_datetime)` — date-range queries
- `idx_meas_hh_dt` on `(household_id, measurement_datetime)` — household-scoped date queries

---

### Table: `sub_metering`

Appliance-level energy breakdown, one row per measurement (1:1 relationship).

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `sub_meter_id` | INT | PK, AUTO_INCREMENT | Surrogate primary key |
| `measurement_id` | INT | FK → `measurements`, UNIQUE | Parent measurement |
| `sub_metering_1` | DOUBLE | nullable | Kitchen circuit (Wh) — dishwasher, oven, microwave |
| `sub_metering_2` | DOUBLE | nullable | Laundry / AC circuit (Wh) — washing machine, dryer, fridge, AC |
| `sub_metering_3` | DOUBLE | nullable | Water heater / HVAC circuit (Wh) — electric water heater, air conditioner |

**Index:** `idx_sub_mid` on `(measurement_id)`

**Note:** Sub-metering 1+2+3 covers a fraction of total `global_active_power`. The remainder (`global_active_power × 1000/60 − sum_sub_metering`) represents unlabelled circuits (lighting, TVs, computers, etc.).

---

### Table: `hourly_aggregates`

Pre-computed hourly summary stats built from `measurements` during the initial data load. Eliminates the need for on-the-fly GROUP BY queries on 2M rows.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `hourly_id` | INT | PK, AUTO_INCREMENT | Surrogate primary key |
| `household_id` | INT | FK → `households` | Parent household |
| `hour_datetime` | DATETIME | NOT NULL | Truncated to the hour |
| `avg_active_power` | DOUBLE | | Mean active power over the hour (kW) |
| `max_active_power` | DOUBLE | | Peak active power in the hour (kW) |
| `min_active_power` | DOUBLE | | Minimum active power in the hour (kW) |
| `total_consumption` | DOUBLE | | Sum of active power readings (kWh proxy) |
| `reading_count` | INT | | Number of 1-minute readings in the hour |

**Unique constraint:** `(household_id, hour_datetime)` — prevents duplicate hours  
**Indexes:** `idx_hourly_dt`, `idx_hourly_hh_dt`

---

### Full DDL

```sql
CREATE TABLE IF NOT EXISTS households (
    household_id   INT           NOT NULL AUTO_INCREMENT PRIMARY KEY,
    household_name VARCHAR(255)  NOT NULL UNIQUE,
    location       VARCHAR(255),
    area_sqm       DOUBLE,
    occupants      INT,
    created_date   TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS measurements (
    measurement_id        INT      NOT NULL AUTO_INCREMENT PRIMARY KEY,
    household_id          INT      NOT NULL,
    measurement_datetime  DATETIME NOT NULL,
    global_active_power   DOUBLE   NOT NULL,
    global_reactive_power DOUBLE,
    voltage               DOUBLE,
    global_intensity      DOUBLE,
    FOREIGN KEY (household_id) REFERENCES households(household_id),
    INDEX idx_meas_dt    (measurement_datetime),
    INDEX idx_meas_hh_dt (household_id, measurement_datetime)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sub_metering (
    sub_meter_id   INT    NOT NULL AUTO_INCREMENT PRIMARY KEY,
    measurement_id INT    NOT NULL UNIQUE,
    sub_metering_1 DOUBLE,
    sub_metering_2 DOUBLE,
    sub_metering_3 DOUBLE,
    FOREIGN KEY (measurement_id) REFERENCES measurements(measurement_id),
    INDEX idx_sub_mid (measurement_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS hourly_aggregates (
    hourly_id         INT      NOT NULL AUTO_INCREMENT PRIMARY KEY,
    household_id      INT      NOT NULL,
    hour_datetime     DATETIME NOT NULL,
    avg_active_power  DOUBLE,
    max_active_power  DOUBLE,
    min_active_power  DOUBLE,
    total_consumption DOUBLE,
    reading_count     INT,
    FOREIGN KEY (household_id) REFERENCES households(household_id),
    UNIQUE KEY uq_hourly (household_id, hour_datetime),
    INDEX idx_hourly_dt    (hour_datetime),
    INDEX idx_hourly_hh_dt (household_id, hour_datetime)
) ENGINE=InnoDB;
```

---

### SQL Query Catalogue (Task 2)

Seven queries are executed automatically by `task2_databases/task2_main.py` and results saved to `outputs/task2/sql_query_results.txt`.

| # | Query | Tables Used |
|---|-------|-------------|
| 1 | Latest 10 measurements (ORDER BY datetime DESC) | `measurements` |
| 2 | Hourly averages 2006-12-16 → 2006-12-19 | `measurements` |
| 3 | JOIN: measurements + sub-metering (first 15 rows) | `measurements`, `sub_metering` |
| 4 | Top-10 peak demand hours | `hourly_aggregates` |
| 5 | Monthly consumption trend (48 months) | `measurements` |
| 6 | Sub-metering energy share per appliance (UNION ALL) | `sub_metering` |
| 7 | Average consumption by day of week | `measurements` |

---