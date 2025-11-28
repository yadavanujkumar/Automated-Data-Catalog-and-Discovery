"""
Metadata Extractor and Profiler for Automated Data Catalog

This module provides functionality to:
1. Set up a simulated PostgreSQL database with sample tables
2. Extract metadata (schema information) from database tables
3. Profile tables (row count, last updated timestamp)
4. Stage metadata to JSON for catalog tools (Amundsen, Atlas)

Requirements:
- pyspark
- psycopg2-binary
- pandas
- findspark (optional, for easy Spark setup)
"""

import json
import os
import random
import string
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

import psycopg2
from psycopg2 import sql
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


# ============================================================================
# DATABASE SETUP FUNCTIONS
# ============================================================================


def get_db_connection(
    dbname: str = "postgres",
    user: str = "postgres",
    password: str = "postgres",
    host: str = "localhost",
    port: int = 5432,
) -> psycopg2.extensions.connection:
    """
    Create a connection to PostgreSQL database.

    Args:
        dbname: Database name to connect to
        user: PostgreSQL username
        password: PostgreSQL password
        host: Database host
        port: Database port

    Returns:
        psycopg2 connection object
    """
    return psycopg2.connect(
        dbname=dbname, user=user, password=password, host=host, port=port
    )


def generate_random_string(length: int = 8) -> str:
    """Generate a random string of specified length."""
    return "".join(random.choices(string.ascii_lowercase, k=length))


def generate_random_email() -> str:
    """Generate a random email address."""
    return f"{generate_random_string(6)}@{generate_random_string(4)}.com"


def generate_random_timestamp(days_back: int = 365) -> datetime:
    """Generate a random timestamp within the specified days back."""
    return datetime.now() - timedelta(
        days=random.randint(0, days_back),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )


def setup_database(
    user: str = "postgres",
    password: str = "postgres",
    host: str = "localhost",
    port: int = 5432,
) -> None:
    """
    Set up the metadata_source database with sample tables.

    Creates two tables:
    - users: id, name, email, created_at
    - products: id, item_name, price, category

    Each table is populated with 10 random rows.

    Args:
        user: PostgreSQL username
        password: PostgreSQL password
        host: Database host
        port: Database port
    """
    # First connect to default postgres database to create our target database
    conn = get_db_connection(
        dbname="postgres", user=user, password=password, host=host, port=port
    )
    conn.autocommit = True
    cursor = conn.cursor()

    # Drop database if exists and create new one
    cursor.execute(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
        "WHERE datname = 'metadata_source' AND pid <> pg_backend_pid()"
    )
    cursor.execute("DROP DATABASE IF EXISTS metadata_source")
    cursor.execute("CREATE DATABASE metadata_source")

    cursor.close()
    conn.close()

    # Connect to the new database and create tables
    conn = get_db_connection(
        dbname="metadata_source", user=user, password=password, host=host, port=port
    )
    cursor = conn.cursor()

    # Create users table
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    # Create products table
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            item_name VARCHAR(200) NOT NULL,
            price DECIMAL(10, 2) NOT NULL,
            category VARCHAR(100),
            last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    # Populate users table with 10 random rows
    user_data = []
    for _ in range(10):
        name = generate_random_string(10).capitalize()
        email = generate_random_email()
        created_at = generate_random_timestamp()
        user_data.append((name, email, created_at))

    cursor.executemany(
        "INSERT INTO users (name, email, created_at) VALUES (%s, %s, %s)", user_data
    )

    # Populate products table with 10 random rows
    categories = ["Electronics", "Clothing", "Books", "Home", "Sports"]
    product_data = []
    for _ in range(10):
        item_name = f"{generate_random_string(8).capitalize()} Product"
        price = round(random.uniform(10.0, 999.99), 2)
        category = random.choice(categories)
        last_modified = generate_random_timestamp()
        product_data.append((item_name, price, category, last_modified))

    cursor.executemany(
        "INSERT INTO products (item_name, price, category, last_modified) "
        "VALUES (%s, %s, %s, %s)",
        product_data,
    )

    conn.commit()
    cursor.close()
    conn.close()

    print("Database 'metadata_source' setup completed successfully!")
    print("Tables created: users, products")
    print("Each table populated with 10 random rows.")


# ============================================================================
# PYSPARK METADATA EXTRACTION FUNCTIONS
# ============================================================================


def create_spark_session(app_name: str = "MetadataExtractor") -> SparkSession:
    """
    Create and configure a Spark session for metadata extraction.

    Args:
        app_name: Name for the Spark application

    Returns:
        Configured SparkSession object
    """
    # Try to use findspark if available
    try:
        import findspark

        findspark.init()
    except ImportError:
        pass  # findspark not installed, assume Spark is configured

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.extraClassPath", get_jdbc_driver_path())
        .config("spark.jars", get_jdbc_driver_path())
        .getOrCreate()
    )

    return spark


def get_jdbc_driver_path() -> str:
    """
    Get the path to PostgreSQL JDBC driver.

    Returns:
        Path to the JDBC driver JAR file
    """
    # Common locations for PostgreSQL JDBC driver
    possible_paths = [
        "/usr/share/java/postgresql-jdbc.jar",
        "/usr/share/java/postgresql.jar",
        os.path.expanduser("~/.ivy2/jars/org.postgresql_postgresql-42.6.0.jar"),
        os.path.join(os.getcwd(), "postgresql-42.6.0.jar"),
    ]

    for path in possible_paths:
        if os.path.exists(path):
            return path

    # Return empty string if not found; Spark will try to download
    return ""


def get_jdbc_url(
    host: str = "localhost", port: int = 5432, dbname: str = "metadata_source"
) -> str:
    """
    Construct JDBC URL for PostgreSQL connection.

    Args:
        host: Database host
        port: Database port
        dbname: Database name

    Returns:
        JDBC URL string
    """
    return f"jdbc:postgresql://{host}:{port}/{dbname}"


def extract_table_schema(
    spark: SparkSession,
    jdbc_url: str,
    table_name: str,
    user: str = "postgres",
    password: str = "postgres",
) -> list[dict[str, str]]:
    """
    Extract schema information from a database table using PySpark.

    Args:
        spark: SparkSession object
        jdbc_url: JDBC connection URL
        table_name: Name of the table to extract schema from
        user: Database username
        password: Database password

    Returns:
        List of dictionaries containing column name and data type
    """
    # Read table using JDBC
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    # Extract schema information
    schema_info = []
    for field in df.schema.fields:
        schema_info.append(
            {"column_name": field.name, "data_type": str(field.dataType)}
        )

    return schema_info


def profile_table(
    spark: SparkSession,
    jdbc_url: str,
    table_name: str,
    timestamp_column: str,
    user: str = "postgres",
    password: str = "postgres",
) -> dict[str, Any]:
    """
    Profile a database table to extract statistics.

    Args:
        spark: SparkSession object
        jdbc_url: JDBC connection URL
        table_name: Name of the table to profile
        timestamp_column: Column name to use for last updated timestamp
        user: Database username
        password: Database password

    Returns:
        Dictionary containing profiling statistics
    """
    # Read table using JDBC
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    # Calculate total row count using PySpark action
    row_count = df.count()

    # Get last updated timestamp
    # First check if the timestamp column exists
    if timestamp_column in df.columns:
        max_timestamp = df.agg({timestamp_column: "max"}).collect()[0][0]
        if max_timestamp:
            last_updated = max_timestamp.isoformat()
        else:
            last_updated = datetime.now().isoformat()
    else:
        # Use current timestamp if column doesn't exist
        last_updated = datetime.now().isoformat()

    return {"row_count": row_count, "last_updated": last_updated}


def extract_metadata_with_spark(
    spark: SparkSession,
    jdbc_url: str,
    database_name: str,
    tables_config: list[dict[str, str]],
    user: str = "postgres",
    password: str = "postgres",
) -> list[dict[str, Any]]:
    """
    Extract metadata for multiple tables using PySpark.

    Args:
        spark: SparkSession object
        jdbc_url: JDBC connection URL
        database_name: Name of the database
        tables_config: List of dictionaries with table_name and timestamp_column
        user: Database username
        password: Database password

    Returns:
        List of metadata dictionaries for each table
    """
    metadata_list = []

    for table_config in tables_config:
        table_name = table_config["table_name"]
        timestamp_column = table_config["timestamp_column"]

        print(f"Processing table: {table_name}")

        # Extract schema
        schema_info = extract_table_schema(spark, jdbc_url, table_name, user, password)

        # Profile table
        profile_info = profile_table(
            spark, jdbc_url, table_name, timestamp_column, user, password
        )

        # Aggregate metadata
        table_metadata = {
            "database_name": database_name,
            "table_name": table_name,
            "row_count": profile_info["row_count"],
            "last_updated": profile_info["last_updated"],
            "columns": schema_info,
        }

        metadata_list.append(table_metadata)

    return metadata_list


def create_metadata_dataframe(
    spark: SparkSession, metadata_list: list[dict[str, Any]]
) -> "DataFrame":
    """
    Create a PySpark DataFrame from metadata list.

    Args:
        spark: SparkSession object
        metadata_list: List of metadata dictionaries

    Returns:
        PySpark DataFrame containing the metadata
    """
    # Define schema for the metadata DataFrame
    column_schema = StructType(
        [
            StructField("column_name", StringType(), False),
            StructField("data_type", StringType(), False),
        ]
    )

    metadata_schema = StructType(
        [
            StructField("database_name", StringType(), False),
            StructField("table_name", StringType(), False),
            StructField("row_count", IntegerType(), False),
            StructField("last_updated", StringType(), False),
            StructField("columns", ArrayType(column_schema), False),
        ]
    )

    # Convert metadata list to format suitable for DataFrame creation
    rows = []
    for metadata in metadata_list:
        row = (
            metadata["database_name"],
            metadata["table_name"],
            metadata["row_count"],
            metadata["last_updated"],
            [(col["column_name"], col["data_type"]) for col in metadata["columns"]],
        )
        rows.append(row)

    # Create DataFrame
    df = spark.createDataFrame(rows, schema=metadata_schema)

    return df


def save_metadata_to_json(
    metadata_list: list[dict[str, Any]], output_path: str = "metadata_staging.json"
) -> None:
    """
    Save metadata to a JSON file.

    Args:
        metadata_list: List of metadata dictionaries
        output_path: Path to the output JSON file
    """
    with open(output_path, "w") as f:
        json.dump(metadata_list, f, indent=2, default=str)

    print(f"Metadata saved to {output_path}")


def save_metadata_dataframe_to_json(
    df: "DataFrame", output_path: str = "metadata_staging.json"
) -> None:
    """
    Save PySpark DataFrame containing metadata to a JSON file.

    This method converts the DataFrame to a clean JSON format.

    Args:
        df: PySpark DataFrame containing metadata
        output_path: Path to the output JSON file
    """
    # Collect data from DataFrame
    rows = df.collect()

    # Convert to clean JSON format
    metadata_list = []
    for row in rows:
        table_metadata = {
            "database_name": row["database_name"],
            "table_name": row["table_name"],
            "row_count": row["row_count"],
            "last_updated": row["last_updated"],
            "columns": [
                {"column_name": col["column_name"], "data_type": col["data_type"]}
                for col in row["columns"]
            ],
        }
        metadata_list.append(table_metadata)

    # Save to JSON file
    with open(output_path, "w") as f:
        json.dump(metadata_list, f, indent=2)

    print(f"Metadata DataFrame saved to {output_path}")


# ============================================================================
# ALTERNATIVE: PANDAS-BASED EXTRACTION (for environments without Spark)
# ============================================================================


def extract_metadata_with_pandas(
    dbname: str = "metadata_source",
    user: str = "postgres",
    password: str = "postgres",
    host: str = "localhost",
    port: int = 5432,
) -> list[dict[str, Any]]:
    """
    Extract metadata using pandas and psycopg2 (alternative to PySpark).

    This function provides a fallback for environments where Spark
    is not available.

    Args:
        dbname: Database name
        user: PostgreSQL username
        password: PostgreSQL password
        host: Database host
        port: Database port

    Returns:
        List of metadata dictionaries for each table
    """
    conn = get_db_connection(
        dbname=dbname, user=user, password=password, host=host, port=port
    )
    cursor = conn.cursor()

    # Define tables and their timestamp columns
    tables_config = [
        {"table_name": "users", "timestamp_column": "created_at"},
        {"table_name": "products", "timestamp_column": "last_modified"},
    ]

    metadata_list = []

    for table_config in tables_config:
        table_name = table_config["table_name"]
        timestamp_column = table_config["timestamp_column"]

        print(f"Processing table (pandas): {table_name}")

        # Get schema information
        cursor.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = 'public'
            ORDER BY ordinal_position
        """,
            (table_name,),
        )

        schema_info = [
            {"column_name": row[0], "data_type": row[1]} for row in cursor.fetchall()
        ]

        # Get row count
        cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name)))
        row_count = cursor.fetchone()[0]

        # Get last updated timestamp
        cursor.execute(
            sql.SQL("SELECT MAX({}) FROM {}").format(
                sql.Identifier(timestamp_column), sql.Identifier(table_name)
            )
        )
        last_updated_result = cursor.fetchone()[0]
        last_updated = (
            last_updated_result.isoformat()
            if last_updated_result
            else datetime.now().isoformat()
        )

        # Aggregate metadata
        table_metadata = {
            "database_name": dbname,
            "table_name": table_name,
            "row_count": row_count,
            "last_updated": last_updated,
            "columns": schema_info,
        }

        metadata_list.append(table_metadata)

    cursor.close()
    conn.close()

    return metadata_list


# ============================================================================
# MAIN EXECUTION
# ============================================================================


def run_spark_extraction(
    user: str = "postgres",
    password: str = "postgres",
    host: str = "localhost",
    port: int = 5432,
    output_path: str = "metadata_staging.json",
) -> None:
    """
    Run the complete PySpark metadata extraction pipeline.

    Args:
        user: Database username
        password: Database password
        host: Database host
        port: Database port
        output_path: Path for the output JSON file
    """
    print("=" * 60)
    print("Starting PySpark Metadata Extraction")
    print("=" * 60)

    # Database configuration
    database_name = "metadata_source"
    jdbc_url = get_jdbc_url(host, port, database_name)

    # Tables to extract metadata from
    tables_config = [
        {"table_name": "users", "timestamp_column": "created_at"},
        {"table_name": "products", "timestamp_column": "last_modified"},
    ]

    # Create Spark session
    print("\nInitializing Spark session...")
    spark = create_spark_session()

    try:
        # Extract metadata
        print("\nExtracting metadata from tables...")
        metadata_list = extract_metadata_with_spark(
            spark, jdbc_url, database_name, tables_config, user, password
        )

        # Create PySpark DataFrame with metadata
        print("\nCreating metadata DataFrame...")
        metadata_df = create_metadata_dataframe(spark, metadata_list)

        # Display the DataFrame
        print("\nMetadata DataFrame:")
        metadata_df.show(truncate=False)

        # Save to JSON
        print("\nSaving metadata to JSON...")
        save_metadata_dataframe_to_json(metadata_df, output_path)

        print("\n" + "=" * 60)
        print("Metadata extraction completed successfully!")
        print("=" * 60)

    finally:
        # Stop Spark session
        spark.stop()


def run_pandas_extraction(
    user: str = "postgres",
    password: str = "postgres",
    host: str = "localhost",
    port: int = 5432,
    output_path: str = "metadata_staging.json",
) -> None:
    """
    Run the complete pandas-based metadata extraction pipeline.

    This is an alternative for environments without Spark.

    Args:
        user: Database username
        password: Database password
        host: Database host
        port: Database port
        output_path: Path for the output JSON file
    """
    print("=" * 60)
    print("Starting Pandas Metadata Extraction")
    print("=" * 60)

    # Extract metadata
    print("\nExtracting metadata from tables...")
    metadata_list = extract_metadata_with_pandas(
        dbname="metadata_source",
        user=user,
        password=password,
        host=host,
        port=port,
    )

    # Save to JSON
    print("\nSaving metadata to JSON...")
    save_metadata_to_json(metadata_list, output_path)

    # Display metadata
    print("\nExtracted Metadata:")
    for table_meta in metadata_list:
        print(f"\nTable: {table_meta['table_name']}")
        print(f"  Database: {table_meta['database_name']}")
        print(f"  Row Count: {table_meta['row_count']}")
        print(f"  Last Updated: {table_meta['last_updated']}")
        print("  Columns:")
        for col in table_meta["columns"]:
            print(f"    - {col['column_name']}: {col['data_type']}")

    print("\n" + "=" * 60)
    print("Metadata extraction completed successfully!")
    print("=" * 60)


def main() -> None:
    """
    Main entry point for the metadata extractor.

    This function:
    1. Sets up the PostgreSQL database with sample data
    2. Extracts metadata using PySpark (or pandas as fallback)
    3. Saves the metadata to a JSON file
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Metadata Extractor for Automated Data Catalog"
    )
    parser.add_argument(
        "--setup-db",
        action="store_true",
        help="Set up the database with sample tables",
    )
    parser.add_argument(
        "--extract",
        action="store_true",
        help="Extract metadata from the database",
    )
    parser.add_argument(
        "--use-pandas",
        action="store_true",
        help="Use pandas instead of PySpark for extraction",
    )
    parser.add_argument(
        "--user",
        default="postgres",
        help="PostgreSQL username (default: postgres)",
    )
    parser.add_argument(
        "--password",
        default="postgres",
        help="PostgreSQL password (default: postgres)",
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="PostgreSQL host (default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5432,
        help="PostgreSQL port (default: 5432)",
    )
    parser.add_argument(
        "--output",
        default="metadata_staging.json",
        help="Output JSON file path (default: metadata_staging.json)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run complete pipeline (setup + extract)",
    )

    args = parser.parse_args()

    # If no arguments provided, show help
    if not any([args.setup_db, args.extract, args.all]):
        parser.print_help()
        print("\nExample usage:")
        print("  # Set up database only:")
        print("  python metadata_extractor.py --setup-db")
        print("\n  # Extract metadata only:")
        print("  python metadata_extractor.py --extract")
        print("\n  # Run complete pipeline:")
        print("  python metadata_extractor.py --all")
        print("\n  # Use pandas instead of PySpark:")
        print("  python metadata_extractor.py --all --use-pandas")
        return

    # Run database setup
    if args.setup_db or args.all:
        print("\n" + "=" * 60)
        print("Setting up database...")
        print("=" * 60)
        setup_database(
            user=args.user,
            password=args.password,
            host=args.host,
            port=args.port,
        )

    # Run extraction
    if args.extract or args.all:
        if args.use_pandas:
            run_pandas_extraction(
                user=args.user,
                password=args.password,
                host=args.host,
                port=args.port,
                output_path=args.output,
            )
        else:
            run_spark_extraction(
                user=args.user,
                password=args.password,
                host=args.host,
                port=args.port,
                output_path=args.output,
            )


if __name__ == "__main__":
    main()
