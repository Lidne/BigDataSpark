from __future__ import annotations

import argparse
import csv
import os
import time
from pathlib import Path

import psycopg2
from psycopg2 import OperationalError

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATA_DIR = ROOT / "source_data"

RAW_TABLE_DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.mock_data;

CREATE TABLE raw.mock_data (
    id TEXT,
    customer_first_name TEXT,
    customer_last_name TEXT,
    customer_age TEXT,
    customer_email TEXT,
    customer_country TEXT,
    customer_postal_code TEXT,
    customer_pet_type TEXT,
    customer_pet_name TEXT,
    customer_pet_breed TEXT,
    seller_first_name TEXT,
    seller_last_name TEXT,
    seller_email TEXT,
    seller_country TEXT,
    seller_postal_code TEXT,
    product_name TEXT,
    product_category TEXT,
    product_price TEXT,
    product_quantity TEXT,
    sale_date TEXT,
    sale_customer_id TEXT,
    sale_seller_id TEXT,
    sale_product_id TEXT,
    sale_quantity TEXT,
    sale_total_price TEXT,
    store_name TEXT,
    store_location TEXT,
    store_city TEXT,
    store_state TEXT,
    store_country TEXT,
    store_phone TEXT,
    store_email TEXT,
    pet_category TEXT,
    product_weight TEXT,
    product_color TEXT,
    product_size TEXT,
    product_brand TEXT,
    product_material TEXT,
    product_description TEXT,
    product_rating TEXT,
    product_reviews TEXT,
    product_release_date TEXT,
    product_expiry_date TEXT,
    supplier_name TEXT,
    supplier_contact TEXT,
    supplier_email TEXT,
    supplier_phone TEXT,
    supplier_address TEXT,
    supplier_city TEXT,
    supplier_country TEXT
);
"""

COLUMNS = [
    "id",
    "customer_first_name",
    "customer_last_name",
    "customer_age",
    "customer_email",
    "customer_country",
    "customer_postal_code",
    "customer_pet_type",
    "customer_pet_name",
    "customer_pet_breed",
    "seller_first_name",
    "seller_last_name",
    "seller_email",
    "seller_country",
    "seller_postal_code",
    "product_name",
    "product_category",
    "product_price",
    "product_quantity",
    "sale_date",
    "sale_customer_id",
    "sale_seller_id",
    "sale_product_id",
    "sale_quantity",
    "sale_total_price",
    "store_name",
    "store_location",
    "store_city",
    "store_state",
    "store_country",
    "store_phone",
    "store_email",
    "pet_category",
    "product_weight",
    "product_color",
    "product_size",
    "product_brand",
    "product_material",
    "product_description",
    "product_rating",
    "product_reviews",
    "product_release_date",
    "product_expiry_date",
    "supplier_name",
    "supplier_contact",
    "supplier_email",
    "supplier_phone",
    "supplier_address",
    "supplier_city",
    "supplier_country",
]

INSERT_SQL = f"""
INSERT INTO raw.mock_data ({", ".join(COLUMNS)})
VALUES ({", ".join(["%s"] * len(COLUMNS))})
"""


def wait_for_postgres(timeout_seconds: int, interval_seconds: int) -> None:
    deadline = time.time() + timeout_seconds

    print("waiting postgres...")
    while time.time() < deadline:
        try:
            with psycopg2.connect(postgres_connection_string()):
                return
        except OperationalError:
            time.sleep(interval_seconds)

    raise TimeoutError("Postgres did not become ready in time")


def recreate_table() -> None:
    print("recreating raw.mock_data...")
    with psycopg2.connect(postgres_connection_string()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(RAW_TABLE_DDL)
        connection.commit()


def postgres_connection_string() -> str:
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    dbname = os.getenv("PGDATABASE", "bigdata_lab")
    user = os.getenv("PGUSER", "app")
    password = os.getenv("PGPASSWORD", "app")
    return f"host={host} port={port} dbname={dbname} user={user} password={password}"


def load_csv_files(data_dir: Path) -> None:
    csv_files = sorted(
        file_path for file_path in data_dir.iterdir() if file_path.is_file() and file_path.suffix.lower() == ".csv"
    )
    if not csv_files:
        available_files = sorted(file_path.name for file_path in data_dir.iterdir()) if data_dir.exists() else []
        raise FileNotFoundError(f"No CSV files found in {data_dir}. Available files: {available_files}")

    with psycopg2.connect(postgres_connection_string()) as connection:
        with connection.cursor() as cursor:
            for csv_file in csv_files:
                print(f"loading {csv_file}")
                inserted_rows = 0

                with csv_file.open("r", encoding="utf-8", newline="") as file:
                    reader = csv.DictReader(file)

                    for row in reader:
                        values = [row.get(column) for column in COLUMNS]
                        cursor.execute(INSERT_SQL, values)  # type: ignore
                        inserted_rows += 1

                connection.commit()
                print(f"inserted {inserted_rows} rows from {csv_file.name}")


def print_row_count() -> None:
    print("raw.mock_data rows:")
    with psycopg2.connect(postgres_connection_string()) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM raw.mock_data;")
            row_count = cursor.fetchone()
    print((row_count or [0])[0])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize and load raw.mock_data from CSV files.")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help=f"Directory with CSV files (default: {DEFAULT_DATA_DIR})",
    )
    parser.add_argument("--timeout", type=int, default=300, help="Postgres wait timeout in seconds")
    parser.add_argument("--interval", type=int, default=2, help="Polling interval in seconds")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_dir = args.data_dir.resolve()

    wait_for_postgres(timeout_seconds=args.timeout, interval_seconds=args.interval)
    recreate_table()
    load_csv_files(data_dir)
    print_row_count()


if __name__ == "__main__":
    main()
