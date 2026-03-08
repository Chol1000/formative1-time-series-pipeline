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
