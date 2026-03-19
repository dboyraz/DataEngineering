import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def run_green_session_aggregation():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # CRITICAL: Keep parallelism at 1 for the watermark to advance on 1 partition
    env.set_parallelism(1)
    env.enable_checkpointing(10 * 1000)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # 1. Define the Kafka Source
    source_ddl = """
        CREATE TABLE green_trips_source (
            lpep_pickup_datetime STRING,
            PULocationID INT,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)

    # 2. Define the PostgreSQL Sink
    sink_ddl = """
        CREATE TABLE processed_green_sessions (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT,
            PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'processed_green_sessions',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)

    # 3. Define the Session Window Query (5-minute gap)
    try:
        t_env.execute_sql("""
            INSERT INTO processed_green_sessions
            SELECT 
                window_start, 
                window_end,
                PULocationID, 
                COUNT(*) as num_trips
            FROM TABLE(
                SESSION(TABLE green_trips_source, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTES)
            )
            GROUP BY window_start, window_end, PULocationID;
        """).wait()
    except Exception as e:
        print(f"Job failed: {str(e)}")

if __name__ == '__main__':
    run_green_session_aggregation()