CREATE KEYSPACE IF NOT EXISTS energy_analysis
WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE energy_analysis;

CREATE TABLE IF NOT EXISTS energy_data (
  timestamp timestamp,
  region text,
  power_plant text,
  consumer_type text,
  production double,
  consumption double,
  voltage double,
  frequency double,
  equipment_status text,
  PRIMARY KEY (region, timestamp)
);

CREATE TABLE IF NOT EXISTS anomaly_data (
  timestamp timestamp,
  region text,
  anomaly_type text,
  description text,
  PRIMARY KEY (region, timestamp)
);

CREATE TABLE IF NOT EXISTS power_forecast (
  timestamp timestamp,
  region text,
  forecasted_production double,
  forecasted_consumption double,
  PRIMARY KEY (region, timestamp)
);
