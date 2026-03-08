// Collection: power_readings
db.createCollection("power_readings", {
  validator: { $jsonSchema: {
    bsonType: "object",
    required: ["household_id", "timestamp", "global_active_power"],
    properties: {
      household_id:          { bsonType: "int" },
      household_info:        { bsonType: "object" },
      timestamp:             { bsonType: "date" },
      date:                  { bsonType: "string" },
      hour:                  { bsonType: "int" },
      day_of_week:           { bsonType: "int" },
      global_active_power:   { bsonType: "double" },
      global_reactive_power: { bsonType: "double" },
      voltage:               { bsonType: "double" },
      global_intensity:      { bsonType: "double" },
      sub_metering: {
        bsonType: "array",
        items: {
          bsonType: "object",
          required: ["meter_id", "name", "consumption_wh"],
          properties: {
            meter_id:       { bsonType: "int" },
            name:           { bsonType: "string" },
            consumption_wh: { bsonType: "double" }
          }
        }
      },
      total_sub_metering_wh: { bsonType: "double" }
    }
  }}
})
db.power_readings.createIndex({ household_id: 1, timestamp: -1 })
db.power_readings.createIndex({ timestamp: 1 })
db.power_readings.createIndex({ date: 1 })
db.power_readings.createIndex({ hour: 1 })

// Collection: daily_summaries (pre-aggregated per-day stats, populated via $out pipeline)
db.createCollection("daily_summaries")
db.daily_summaries.createIndex({ household_id: 1, _id: 1 })
db.power_readings.aggregate([
  { $group: { _id: "$date",
              household_id:     { $first: "$household_id" },
              avg_active_power: { $avg:   "$global_active_power" },
              max_active_power: { $max:   "$global_active_power" },
              total_readings:   { $sum:   1 } } },
  { $sort:  { _id: 1 } },
  { $out:   "daily_summaries" }
])