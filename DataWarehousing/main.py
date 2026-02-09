from __future__ import annotations

import argparse
import re
from pathlib import Path

import duckdb

BIGQUERY_TABLE_PATTERN = re.compile(
    r"`(?:(?P<project>[A-Za-z0-9_-]+)\.)?(?P<dataset>[A-Za-z0-9_]+)\.(?P<table>[A-Za-z0-9_]+)`"
)
BIGQUERY_TWO_PART_PATTERN = re.compile(
    r"`(?P<dataset>[A-Za-z0-9_]+)\.(?P<table>[A-Za-z0-9_]+)`"
)


def sanitize_table_name(raw_name: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9_]", "_", raw_name).lower()
    if not sanitized:
        raise ValueError(f"Unable to create table name from: {raw_name!r}")
    if sanitized[0].isdigit():
        sanitized = f"t_{sanitized}"
    return sanitized


def translate_bigquery_sql(sql: str, default_dataset: str) -> str:
    def replace_three_part(match: re.Match[str]) -> str:
        dataset = match.group("dataset")
        table = match.group("table")
        return f'"{dataset}"."{table}"'

    def replace_two_part(match: re.Match[str]) -> str:
        dataset = match.group("dataset") or default_dataset
        table = match.group("table")
        return f'"{dataset}"."{table}"'

    translated = BIGQUERY_TABLE_PATTERN.sub(replace_three_part, sql)
    translated = BIGQUERY_TWO_PART_PATTERN.sub(replace_two_part, translated)
    # Remaining backticks are usually column identifiers.
    translated = translated.replace("`", '"')
    return translated


def register_parquet_tables(
    connection: duckdb.DuckDBPyConnection,
    data_dir: Path,
    dataset: str,
) -> dict[str, Path]:
    files = sorted(data_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {data_dir}")

    connection.execute(f'CREATE SCHEMA IF NOT EXISTS "{dataset}"')

    loaded_tables: dict[str, Path] = {}
    for parquet_file in files:
        base_table_name = sanitize_table_name(parquet_file.stem)
        table_name = base_table_name
        suffix = 1
        while table_name in loaded_tables:
            suffix += 1
            table_name = f"{base_table_name}_{suffix}"

        parquet_path_sql = parquet_file.resolve().as_posix().replace("'", "''")
        connection.execute(
            f"""
            CREATE OR REPLACE VIEW "{dataset}"."{table_name}" AS
            SELECT * FROM read_parquet('{parquet_path_sql}')
            """
        )
        # Mirror table in the default schema so unqualified table names work.
        connection.execute(
            f'CREATE OR REPLACE VIEW "{table_name}" AS SELECT * FROM "{dataset}"."{table_name}"'
        )
        loaded_tables[table_name] = parquet_file

    return loaded_tables


def print_results(cursor: duckdb.DuckDBPyConnection, max_rows: int) -> None:
    if cursor.description is None:
        print("Statement executed successfully.")
        return

    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchmany(max_rows + 1)
    has_more_rows = len(rows) > max_rows
    rows = rows[:max_rows]

    string_rows = [[("NULL" if value is None else str(value)) for value in row] for row in rows]
    widths = [len(col) for col in columns]
    for row in string_rows:
        for i, value in enumerate(row):
            widths[i] = max(widths[i], len(value))

    header = " | ".join(col.ljust(widths[i]) for i, col in enumerate(columns))
    separator = "-+-".join("-" * width for width in widths)
    print(header)
    print(separator)
    for row in string_rows:
        print(" | ".join(value.ljust(widths[i]) for i, value in enumerate(row)))

    if has_more_rows:
        print(f"... showing first {max_rows} rows")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run BigQuery-style SQL locally on parquet data using DuckDB."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Directory that contains parquet files (default: ./data).",
    )
    parser.add_argument(
        "--dataset",
        default="taxi",
        help="Dataset name exposed to queries (default: taxi).",
    )
    parser.add_argument(
        "--dialect",
        choices=("bigquery", "duckdb"),
        default="bigquery",
        help="Input SQL dialect (default: bigquery).",
    )
    parser.add_argument("--query", help="SQL query to execute.")
    parser.add_argument(
        "--query-file",
        type=Path,
        help="Path to a .sql file to execute.",
    )
    parser.add_argument(
        "--show-sql",
        action="store_true",
        help="Print translated SQL before execution.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=20,
        help="Maximum number of rows to display (default: 20).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.query and args.query_file:
        raise SystemExit("Use either --query or --query-file, not both.")

    connection = duckdb.connect(database=":memory:")
    tables = register_parquet_tables(connection, args.data_dir, args.dataset)

    query_text: str | None = args.query
    if args.query_file is not None:
        query_text = args.query_file.read_text(encoding="utf-8")

    if not query_text:
        print("Loaded parquet tables:")
        for table_name, source_file in tables.items():
            print(f"- {args.dataset}.{table_name} <- {source_file.name}")

        example_table = next(iter(tables))
        print("\nExample:")
        print(
            "uv run python main.py --query "
            f"'SELECT COUNT(*) AS trips FROM `local.{args.dataset}.{example_table}`'"
        )
        return

    translated_query = (
        translate_bigquery_sql(query_text, args.dataset)
        if args.dialect == "bigquery"
        else query_text
    )

    if args.show_sql:
        print("SQL sent to DuckDB:")
        print(translated_query)
        print()

    try:
        cursor = connection.execute(translated_query)
    except duckdb.Error as exc:
        raise SystemExit(f"Query failed: {exc}") from exc

    print_results(cursor, args.max_rows)


if __name__ == "__main__":
    main()
