import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def run_green_tips_aggregation():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Still using parallelism 1 for the single-partition watermark
    env.set_parallelism(1)
    env.enable_checkpointing(10 * 1000)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # 1. Define the Kafka Source (Adding tip_amount)
    source_ddl = """
        CREATE TABLE green_trips_source (
            lpep_pickup_datetime STRING,
            tip_amount DOUBLE,
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
        CREATE TABLE processed_green_tips (
            window_start TIMESTAMP(3),
            total_tips DOUBLE,
            PRIMARY KEY (window_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'processed_green_tips',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)

    # 3. Define the 1-hour Tumbling Window Query
    # We group only by window_start to get the total across all locations
    try:
        t_env.execute_sql("""
            INSERT INTO processed_green_tips
            SELECT 
                window_start, 
                SUM(tip_amount) as total_tips
            FROM TABLE(
                TUMBLE(TABLE green_trips_source, DESCRIPTOR(event_timestamp), INTERVAL '1' HOUR)
            )
            GROUP BY window_start;
        """).wait()
    except Exception as e:
        print(f"Job failed: {str(e)}")

if __name__ == '__main__':
    run_green_tips_aggregation()