// Q1 — Latest 10 readings
db.power_readings.find({ household_id: 1 }).sort({ timestamp: -1 }).limit(10)

// Q2 — Date range
db.power_readings.find({
  household_id: 1,
  timestamp: { $gte: ISODate("2006-12-16T00:00:00Z"), $lte: ISODate("2006-12-19T23:59:59Z") }
}).sort({ timestamp: 1 })

// Q3 — Hourly aggregation
db.power_readings.aggregate([
  { $group: { _id: "$hour", avg_power: { $avg: "$global_active_power" },
              max_power: { $max: "$global_active_power" }, count: { $sum: 1 } }},
  { $sort: { _id: 1 } }
])

// Q4 — Sub-metering breakdown
db.power_readings.aggregate([
  { $unwind: "$sub_metering" },
  { $group: { _id: "$sub_metering.name", avg_wh: { $avg: "$sub_metering.consumption_wh" },
              total_wh: { $sum: "$sub_metering.consumption_wh" }, count: { $sum: 1 } }},
  { $sort: { total_wh: -1 } }
])

// Q5 — Daily summary (reads pre-aggregated daily_summaries collection)
db.daily_summaries
  .find({}, { _id: 1, avg_active_power: 1, max_active_power: 1, total_readings: 1 })
  .sort({ _id: 1 })
  .limit(20)