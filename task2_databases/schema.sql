CREATE TABLE IF NOT EXISTS households (
    household_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    household_name VARCHAR(255) NOT NULL UNIQUE,
    location VARCHAR(255),
    area_sqm DOUBLE,
    occupants INT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS measurements (
    measurement_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    household_id INT NOT NULL,
    measurement_datetime DATETIME NOT NULL,
    global_active_power DOUBLE NOT NULL,
    global_reactive_power DOUBLE,
    voltage DOUBLE,
    global_intensity DOUBLE,
    FOREIGN KEY (household_id) REFERENCES households (household_id),
    INDEX idx_meas_dt (measurement_datetime),
    INDEX idx_meas_hh_dt (
        household_id,
        measurement_datetime
    )
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS sub_metering (
    sub_meter_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    measurement_id INT NOT NULL UNIQUE,
    sub_metering_1 DOUBLE,
    sub_metering_2 DOUBLE,
    sub_metering_3 DOUBLE,
    FOREIGN KEY (measurement_id) REFERENCES measurements (measurement_id),
    INDEX idx_sub_mid (measurement_id)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS hourly_aggregates (
    hourly_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    household_id INT NOT NULL,
    hour_datetime DATETIME NOT NULL,
    avg_active_power DOUBLE,
    max_active_power DOUBLE,
    min_active_power DOUBLE,
    total_consumption DOUBLE,
    reading_count INT,
    FOREIGN KEY (household_id) REFERENCES households (household_id),
    UNIQUE KEY uq_hourly (household_id, hour_datetime),
    INDEX idx_hourly_dt (hour_datetime),
    INDEX idx_hourly_hh_dt (household_id, hour_datetime)
) ENGINE = InnoDB;