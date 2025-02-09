## #3 answers

```bash
python -m venv .venv
source ./.venv/bin/activate
pip install --upgrade pip 
pip install -r requirements.txt
python load_yellow_taxi_data.py
```

1. 20332093
```sql
CREATE OR REPLACE EXTERNAL TABLE `nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'Parquet',
  uris = ['gs://nyc-taxi-data-tsg/yellow_tripdata_2024-*.parquet']
);
                                    
select count(*) from `nytaxi.external_yellow_tripdata`;                   
                  
CREATE OR REPLACE TABLE `nytaxi.yellow_tripdata_not_partitioned` AS   
SELECT * FROM `nytaxi.external_yellow_tripdata`;
```

2. 0 MB for the External Table and 155.12 MB for the Materialized Table
```sql
SELECT COUNT(DISTINCT (PULocationID)) FROM  `nytaxi.external_yellow_tripdata`;

SELECT COUNT(DISTINCT (PULocationID)) FROM `nytaxi.yellow_tripdata_not_partitioned`; 
```

3. BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.
```sql
SELECT PULocationID FROM `nytaxi.yellow_tripdata_not_partitioned`; 

SELECT PULocationID, DOLocationID FROM `nytaxi.yellow_tripdata_not_partitioned`; 
```

4. 8333 
```sql
SELECT count(1) FROM `nytaxi.yellow_tripdata_not_partitioned` where fare_amount=0; 
```

5. Partition by tpep_dropoff_datetime and Cluster on VendorID 
```sql
CREATE OR REPLACE TABLE `nytaxi.yellow_tripdata_partitioned_clustered` 
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT * FROM `nytaxi.external_yellow_tripdata`;
```

6. 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table.  
Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)
```sql
SELECT DISTINCT(VendorID) FROM `nytaxi.yellow_tripdata_not_partitioned`
WHERE tpep_dropoff_datetime >= '2024-03-01' AND tpep_dropoff_datetime <= '2024-03-15';

SELECT DISTINCT(VendorID) FROM `nytaxi.yellow_tripdata_partitioned_clustered` 
WHERE tpep_dropoff_datetime >= '2024-03-01' AND tpep_dropoff_datetime <= '2024-03-15';
```

7. GCP Bucket

8. False

9. SELECT count(*) will read the metadata only => **0 bytes**