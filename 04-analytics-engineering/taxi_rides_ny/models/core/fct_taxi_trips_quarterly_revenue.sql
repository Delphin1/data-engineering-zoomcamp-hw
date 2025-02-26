{{
    config(
        materialized='table'
    )
}}

WITH GR_FACTS AS (

SELECT service_type,
EXTRACT(YEAR FROM pickup_datetime) t_year,
EXTRACT(QUARTER FROM pickup_datetime) t_quarter,
SUM(total_amount) sum_total_amount
FROM  {{ ref('fact_trips') }}
WHERE pickup_datetime >= '2019-01-01' and pickup_datetime < '2021-01-01'
GROUP BY 1,2,3
),

GR_FACTS_2019 AS
(
SELECT * FROM GR_FACTS
WHERE t_year = 2019
),

GR_FACTS_2020 AS
(
SELECT * FROM GR_FACTS
WHERE t_year = 2020
),

GR_JOIN AS (
  SELECT
  F2019.service_type,
  F2019.t_quarter,
--  F2019.t_year,
  F2019.sum_total_amount as sum_total_amount_2019,
--  F2020.t_year,
  F2020.sum_total_amount as sum_total_amount_2020,
  CASE
    WHEN F2019.sum_total_amount > F2020.sum_total_amount THEN -(100-F2020.sum_total_amount*100/F2019.sum_total_amount)
    ELSE 100-F2019.sum_total_amount*100/F2020.sum_total_amount
  END AS PRC
  FROM GR_FACTS_2019 AS F2019
  JOIN GR_FACTS_2020 AS F2020 ON F2019.service_type = F2020.service_type
  and F2019.t_quarter = F2020.t_quarter
  and F2019.t_year <> F2020.t_year
),
RESULT_RANK AS (
  SELECT service_type, t_quarter,
  rank() over (partition by service_type order by sum_total_amount_2020 desc) as max_sum_total_amount_2020,
  rank() over (partition by service_type order by sum_total_amount_2020 asc) as min_sum_total_amount_2020,
  FROM GR_JOIN
)
select * from RESULT_RANK
order by 1,2

