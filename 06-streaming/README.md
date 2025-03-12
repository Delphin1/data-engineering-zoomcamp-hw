## 6 answers

1. v24.2.18
```
docker exec -it redpanda-1 rpk version
Version:     v24.2.18
Git ref:     f9a22d4430
Build date:  2025-02-14T12:59:41Z
OS/Arch:     linux/arm64
Go version:  go1.23.1

Redpanda Cluster
  node-1  v24.2.18 - f9a22d443087b824803638623d6b7492ec8221f9
```
2. green-trips  OK
```
docker exec -it redpanda-1 rpk topic create green-trips   
TOPIC        STATUS
green-trips  OK
```
3. True  
[Solution](./Untitled.ipynb)

4. 25.5 sec  
[Solution](pyflink/src/producers/load_taxi_data.py)
```bash
cd pyflink/src/producer
mkdir data
cd data
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
gzip -d green_tripdata_2019-10.csv.gz
cd ..
python load_taxi_data.py
```

5. Bronx, Unknown  
[Solution](pyflink/src/job/session_job.py)
```bash
docker compose exec jobmanager ./bin/flink run -py /opt/src/job/session_job.py --pyFiles /opt/src -d
```

```sql
CREATE TABLE processed_events_aggregated (
    pu_location_id INTEGER,
    do_location_id INTEGER,
    streak_start TIMESTAMP,
    streak_end TIMESTAMP,
    total_distance DOUBLE PRECISION,
    PRIMARY KEY (pu_location_id, do_location_id)
);


select pu_location_id, do_location_id, max(total_distance)
from processed_events_aggregated
group by 1,2
order by  3 desc
limit 1
;

-- Result: 126|265|95.78

-- in BQ
SELECT * 
FROM `dataengineeringzoomcamp2025.trips_data_all.dim_zones` 
where
locationid in (126, 265);

--Restult: Bronx, Unknown

```



