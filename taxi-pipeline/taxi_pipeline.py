"""dlt pipeline to ingest NYC taxi trip data from a paginated REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def taxi_api_source():
    """Load NYC taxi trip data from the Data Engineering Zoomcamp API."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
        },
        "resources": [
            {
                "name": "taxi_rides",
                "endpoint": {
                    "path": "",
                    "paginator": {
                        "type": "page_number",
                        "base_page": 1,
                        "page_param": "page",
                        "total_path": None,
                        "stop_after_empty_page": True,
                    },
                },
                "write_disposition": "replace",
            },
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    dev_mode=True,
    progress="log",
)


if __name__ == "__main__":
    # Load data
    load_info = pipeline.run(taxi_api_source())
    print(load_info)

    # Answer analytics questions
    with pipeline.sql_client() as client:
        # Q1: Start and end date of the dataset
        result = client.execute_sql(
            "SELECT MIN(trip_pickup_date_time) AS start_date, "
            "MAX(trip_pickup_date_time) AS end_date "
            "FROM taxi_rides"
        )
        row = result[0]
        print(f"\nQ1: Dataset date range: {row[0]} to {row[1]}")

        # Q2: Proportion of trips paid with credit card
        result = client.execute_sql(
            "SELECT ROUND(COUNT(CASE WHEN payment_type ILIKE '%credit%' THEN 1 END) "
            "* 100.0 / COUNT(*), 2) AS credit_card_pct FROM taxi_rides"
        )
        print(f"Q2: Credit card trip proportion: {result[0][0]}%")

        # Q3: Total amount of money generated in tips
        result = client.execute_sql(
            "SELECT ROUND(SUM(tip_amt), 2) AS total_tips FROM taxi_rides"
        )
        print(f"Q3: Total tips generated: ${result[0][0]}")
