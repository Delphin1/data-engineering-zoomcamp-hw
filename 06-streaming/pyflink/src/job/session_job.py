from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration

def create_events_aggregated_sink(t_env):
    table_name = 'processed_events_aggregated'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            pu_location_id INTEGER,
            do_location_id INTEGER,
            streak_start TIMESTAMP(3),
            streak_end TIMESTAMP(3),
            total_distance DOUBLE,
            PRIMARY KEY (pu_location_id, do_location_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


# {
#   "lpep_pickup_datetime": "2019-10-31 21:16:00",
#   "lpep_dropoff_datetime": "2019-10-31 21:44:00",
#   "PULocationID": "174",
#   "DOLocationID": "171",
#   "passenger_count": "",
#   "trip_distance": "10.38",
#   "tip_amount": "0"
# }

def create_events_source_kafka(t_env):
    table_name = "events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
--                 lpep_pickup_datetime VARCHAR,
                lpep_dropoff_datetime VARCHAR,
                PULocationID INTEGER,
                DOLocationID INTEGER,
--                 passenger_count VARCHAR,
                trip_distance  DOUBLE,
--                 trip_amount  DOUBLE,
--                 pickupickup_timestampp as TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
                event_watermark as TO_TIMESTAMP(lpep_dropoff_datetime, 'yyyy-MM-dd HH:mm:ss'),
                WATERMARK FOR event_watermark AS event_watermark - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.group.id' = 'flink-consumer-group3',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def taxi_aggregation():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(
            # This lambda is your timestamp assigner:
            #   event -> The data record
            #   timestamp -> The previously assigned (or default) timestamp
            lambda event, timestamp: event[2]  # We treat the second tuple element as the event-time (ms).
        )
    )
    try:

        source_table = create_events_source_kafka(t_env)
        aggregated_table = create_events_aggregated_sink(t_env)

        # print_sink_ddl = """
        #     CREATE TABLE print_sink (
        #         lpep_pickup_datetime VARCHAR,
        #         lpep_dropoff_datetime VARCHAR,
        #         PULocationID  INTEGER,
        #         DOLocationID INTEGER,
        #         passenger_count VARCHAR,
        #         trip_distance DOUBLE,
        #         trip_amount DOUBLE,
        #         pickup_timestamp TIMESTAMP(3),
        #         dropoff_timestamp TIMESTAMP(3)
        #     ) WITH (
        #         'connector' = 'print'
        #     )
        #     """
        #
        # # Execute the sink DDL
        # t_env.execute_sql(print_sink_ddl)
        #
        # t_env.execute_sql(f"""
        # INSERT INTO print_sink
        # SELECT
        #     lpep_pickup_datetime,
        #         lpep_dropoff_datetime,
        #         PULocationID,
        #         DOLocationID,
        #         passenger_count,
        #         trip_distance,
        #         trip_amount,
        #         pickup_timestamp,
        #         dropoff_timestamp
        # FROM {source_table}
        # """).wait()
        # Create Kafka table

        t_env.execute_sql(f"""
                INSERT INTO {aggregated_table}
                SELECT
                    PULocationID,
                    DOLocationID,
                    SESSION_START(event_watermark, INTERVAL '5' MINUTES) AS window_start,
                    SESSION_END(event_watermark, INTERVAL '5' MINUTES) AS window_end,
                    SUM(trip_distance) AS total_distance
                FROM  {source_table}
                GROUP BY
                    PULocationID,
                    DOLocationID,
                    SESSION(event_watermark, INTERVAL '5' MINUTES)
        """).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    taxi_aggregation()
