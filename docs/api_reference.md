# API Reference

Base URL: `http://localhost:8000`
Start server: `python task3_api/api.py`

## Endpoints

### Health Check

`GET /` - Returns API status

### SQL Households

`GET /sql/households` - Get all households

`POST /sql/households` - Create household
- Required: household_name
- Optional: location, area_sqm, occupants

`GET /sql/households/{id}` - Get household by ID

`PUT /sql/households/{id}` - Update household

`DELETE /sql/households/{id}` - Delete household

### SQL Measurements

`GET /sql/measurements` - Get measurements (query params: skip, limit)

`POST /sql/measurements` - Create measurement
- Required: household_id, measurement_datetime, global_active_power
- Optional: global_reactive_power, voltage, global_intensity, sub_metering_1/2/3

`GET /sql/measurements/{id}` - Get measurement by ID

`PUT /sql/measurements/{id}` - Update measurement

`DELETE /sql/measurements/{id}` - Delete measurement

### SQL Time-Series Queries

`GET /sql/latest` - Get most recent measurement

`GET /sql/date-range` - Get measurements by date range (params: start_date, end_date, limit)

`GET /sql/hourly-stats` - Get hourly averages by hour of day

`GET /sql/monthly-trend` - Get monthly averages

### MongoDB Measurements

`POST /mongo/measurements` - Create measurement document
- Required: household_id, measurement_datetime, global_active_power

`GET /mongo/measurements` - Get measurements (query params: skip, limit)

`GET /mongo/measurements/{id}` - Get measurement by ID

`PUT /mongo/measurements/{id}` - Update measurement

`DELETE /mongo/measurements/{id}` - Delete measurement

`GET /mongo/latest` - Get most recent measurement

`GET /mongo/date-range` - Get measurements by date range (params: start_date, end_date, limit)

`GET /mongo/hourly-stats` - Get hourly averages

`GET /mongo/daily-summary` - Get daily summary (param: days)

## Tests

Run tests: `python task3_api/test_api.py`
