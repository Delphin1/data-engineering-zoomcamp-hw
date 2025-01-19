# Homework #1

```bash
pip install pgcli
pgcli -h localhost -p 5432 -u root -d ny_taxi
```

1. pip 24.3.1
```bash
cd 01-docker-terraform/2_docker_sql
docker run -it --rm --entrypoint=/bin/bash python:3.12.8
```

2. postgres:5432
```bash 
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
gunzip green_tripdata_2019-10.csv.gz 
pip install --upgrade pip setuptools wheel
#LDFLAGS="-I/opt/homebrew/opt/openssl@3/include -L/opt/homebrew/opt/openssl@3/lib" pip install sqlalchemy psycopg2-binary
pip install notebook pandas sqlalchemy psycopg2-binary
jupyter notebook upload-data.ipynb
```

3. 104,802; 198,924; 109,603; 27,678; 35,189
```sql
select 
count(*) FILTER (WHERE trip_distance <=1) as before_1,
count(*) FILTER (WHERE trip_distance > 1 and  trip_distance <= 3) as from_1_to_3,
count(*) FILTER (WHERE trip_distance > 3 and  trip_distance <= 7) as from_3_to_7,
count(*) FILTER (WHERE trip_distance > 7 and  trip_distance <= 10) as from_7_to_10,
count(*) FILTER (WHERE trip_distance > 10) as under_10
from public.green_taxi_data t
where 1=1
and t.lpep_pickup_datetime >= '2019-10-01' 
and t.lpep_dropoff_datetime < '2019-11-01' -- trip shoud be ended till this date 
;
```

4. 2019-10-31
```sql
select cast(t.lpep_pickup_datetime as DATE) "day", max(t.trip_distance) 
from public.green_taxi_data t
group by day
order by 2 desc
limit 1
;
```
5. East Harlem North, East Harlem South, Morningside Heights
```sql
select z."Zone", sum(total_amount) 
from public.green_taxi_data t
join public.zones z on t."PULocationID" = z."LocationID"
where 1=1
and cast(t.lpep_pickup_datetime as DATE) = '2019-10-18'
group by 1
having sum(total_amount) > 13000
order by 2 desc
;
```

6. JFK Airport
```sql
select doz."Zone", max(tip_amount)
from public.green_taxi_data t
join public.zones puz on t."PULocationID" = puz."LocationID"
join public.zones doz on t."DOLocationID" = doz."LocationID"
where 1=1
and t.lpep_pickup_datetime >= '2019-10-01'
and t.lpep_pickup_datetime < '2019-11-01'
and puz."Zone" = 'East Harlem North'
group by 1
order by 2 desc
;
```

7. terraform init, terraform apply -auto-approve, terraform destroy
https://console.cloud.google.com/

TF steps:
- Init 
- Plan
- Apply
- Destroy

GPC roles:
- BigQuery Admin
- Cloud Compute Admin
- Cloud Storage Admin


  