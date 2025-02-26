{{
    config(
        materialized='view'
    )
}}

with
trip_dur as (
  SELECT
  TIMESTAMP_DIFF(dropOff_datetime, pickup_datetime, SECOND) as trip_duration,
  *
  FROM  {{ ref('dim_fhv_trips') }}
),

trip_perc as (
select
PERCENTILE_CONT(trip_duration, 0.90) OVER (PARTITION BY fhv_year, fhv_month, pickup_locationid, dropoff_locationid) AS p90,
*
from trip_dur
),

trip_rank_201911 as (
select distinct
-- pickup_datetime,
pickup_zone,
-- dropoff_datetime,
dropoff_zone,
p90,
dense_rank() over (partition by pickup_zone order by p90 desc) as rank,
from trip_perc
where
fhv_year = 2019
and fhv_month = 11
and pickup_zone in ('Newark Airport','SoHo','Yorkville East')

)

select * from trip_rank_201911
where  rank = 2
