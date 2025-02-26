{{
    config(
        materialized='view'
    )
}}


WITH cte AS (
  SELECT
    service_type,
    EXTRACT(YEAR FROM pickup_datetime) AS trip_year,
    EXTRACT(MONTH FROM pickup_datetime) AS trip_month,

    -- 97th percentile
    PERCENTILE_CONT(fare_amount, 0.97)
      OVER (
        PARTITION BY service_type, EXTRACT(YEAR FROM pickup_datetime), EXTRACT(MONTH FROM pickup_datetime)
      ) AS p97,

    -- 95th percentile
    PERCENTILE_CONT(fare_amount, 0.95)
      OVER (
        PARTITION BY service_type, EXTRACT(YEAR FROM pickup_datetime), EXTRACT(MONTH FROM pickup_datetime)
      ) AS p95,

    -- 90th percentile
    PERCENTILE_CONT(fare_amount, 0.90)
      OVER (
        PARTITION BY service_type, EXTRACT(YEAR FROM pickup_datetime), EXTRACT(MONTH FROM pickup_datetime)
      ) AS p90



  FROM  {{ ref('fact_trips') }}
  where
  fare_amount > 0
  AND trip_distance > 0
 and payment_type_description in ('Cash', 'Credit Card')
)

SELECT DISTINCT
  service_type,
  trip_year AS year,
  trip_month AS month,
  p90,
  p95,
  p97
FROM cte
WHERE trip_year = 2020 and trip_month = 4
ORDER BY service_type, year, month;