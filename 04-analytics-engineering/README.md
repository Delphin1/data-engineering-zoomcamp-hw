## #4 answers

1. 3th  
select * from myproject.raw_nyc_tripdata.ext_green_taxi

2. 4th
Update the WHERE clause to pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY

3. 4th
dbt run --select +models/core/

4. 1,3,4,5

5. 4th
green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}

6. 2th
++ green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}

7. 1th 
LaGuardia Airport, Chinatown, Garment District


