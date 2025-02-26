{{
    config(
        materialized='table'
    )
}}

with tripdata as (
select
    dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropOff_datetime,
   {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }}              as pickup_locationid,
   {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }}              as dropoff_locationid,
   {{ dbt.safe_cast("SR_Flag", api.Column.translate_type("integer")) }}                   as sr_flag,
   Affiliated_base_number                                                                 as affiliated_base_number
from {{ source('staging','fhv_tripdata') }}
where dispatching_base_num is not null
)
select * from tripdata
