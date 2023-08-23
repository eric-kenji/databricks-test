-- Databricks notebook source
CREATE OR REPLACE LIVE TABLE silver (
  trip_distance VARCHAR(255),
  fare_amount VARCHAR(255),
  pickup_zip VARCHAR(255),
  dropoff_zip VARCHAR(255),
  city VARCHAR(255),
  created_dt TIMESTAMP
) AS (
  SELECT DISTINCT
    a.trip_distance,
    a.fare_amount,
    a.pickup_zip,
    a.dropoff_zip,
    b.city,
    current_timestamp() as created_dt
  FROM LIVE.bronze a
  INNER JOIN (
SELECT 10037 AS dropoff_zip, 'city_A' as city
UNION ALL
SELECT 10065 AS dropoff_zip, 'city_B' as city
UNION ALL
SELECT 10019 AS dropoff_zip, 'city_C' as city
UNION ALL
SELECT 11201 AS dropoff_zip, 'city_D' as city
UNION ALL
SELECT 10044 AS dropoff_zip, 'city_E' as city) b
ON a.dropoff_zip = b.dropoff_zip
)
