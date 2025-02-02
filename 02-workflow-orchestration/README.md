1. 128.3 yellow_tripdata_2020-12.csv file size

2. green_tripdata_2020-04.csv

3. 24648499
How many rows are there for the `Yellow` Taxi data for all CSV files in the year 2020?
```sql
select count(*) from public.yellow_tripdata 
where filename  like 'yellow_tripdata_2020%';
```

4. 1734051
How many rows are there for the `Green` Taxi data for all CSV files in the year 2020?
```sql
select count(*) from public.green_tripdata t
where filename like 'green_tripdata_2020%';

```
5. 1925152
How many rows are there for the `Yellow` Taxi data for the March 2021 CSV file?
```sql
select count(*) from public.yellow_tripdata t
where filename = 'yellow_tripdata_2021-03.csv';
```

6. America/New_York
https://kestra.io/plugins/core/triggers/io.kestra.plugin.core.trigger.schedule
https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List
