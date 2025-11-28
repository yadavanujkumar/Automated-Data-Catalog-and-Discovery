# Automated Data Catalog and Discovery

An automated data catalog system that connects to databases, extracts schema metadata, profiles tables, and stages the metadata for catalog tools like Amundsen or Apache Atlas.

## Features

- **Database Setup**: Automatically create and populate sample PostgreSQL tables
- **Metadata Extraction**: Extract schema information (column names, data types) using PySpark
- **Table Profiling**: Calculate row counts and last updated timestamps
- **JSON Staging**: Export metadata to clean JSON format for catalog tool integration

## Prerequisites

- Python 3.9+
- PostgreSQL 12+ (running locally or accessible)
- Apache Spark 3.x (optional if using pandas fallback)
- Java 8 or 11 (required for Spark)

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/yadavanujkumar/Automated-Data-Catalog-and-Discovery.git
cd Automated-Data-Catalog-and-Discovery
```

### 2. Install Python Dependencies

```bash
pip install pyspark psycopg2-binary pandas findspark
```

### 3. Set Up Apache Spark (Local Environment)

#### Option A: Using findspark (Recommended for Quick Setup)

1. Download Apache Spark from [https://spark.apache.org/downloads.html](https://spark.apache.org/downloads.html)

2. Extract and set environment variables:
   ```bash
   export SPARK_HOME=/path/to/spark
   export PATH=$SPARK_HOME/bin:$PATH
   ```

3. Install findspark:
   ```bash
   pip install findspark
   ```

4. findspark will automatically locate your Spark installation.

#### Option B: Using PySpark Standalone

PySpark pip package includes a standalone Spark installation:

```bash
pip install pyspark
```

This is the simplest option and works out of the box for local development.

#### Option C: Using Docker

```bash
# Start a PostgreSQL container
docker run --name postgres-catalog -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:15

# Run Spark in Docker (optional)
docker run -it --rm --network host \
  -v $(pwd):/app \
  bitnami/spark:3 \
  spark-submit /app/metadata_extractor.py --all
```

### 4. Download PostgreSQL JDBC Driver

For PySpark to connect to PostgreSQL, you need the JDBC driver:

```bash
# Download the driver
wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Or specify during spark-submit
spark-submit --jars postgresql-42.6.0.jar metadata_extractor.py --all
```

## Usage

### Complete Pipeline (Setup + Extract)

```bash
# Using PySpark
python metadata_extractor.py --all

# Using pandas (no Spark required)
python metadata_extractor.py --all --use-pandas
```

### Database Setup Only

```bash
python metadata_extractor.py --setup-db
```

### Metadata Extraction Only

```bash
# Using PySpark
python metadata_extractor.py --extract

# Using pandas
python metadata_extractor.py --extract --use-pandas
```

### Custom Database Connection

```bash
python metadata_extractor.py --all \
  --user myuser \
  --password mypassword \
  --host localhost \
  --port 5432 \
  --output my_metadata.json
```

## Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--setup-db` | Set up the database with sample tables | - |
| `--extract` | Extract metadata from the database | - |
| `--all` | Run complete pipeline (setup + extract) | - |
| `--use-pandas` | Use pandas instead of PySpark | False |
| `--user` | PostgreSQL username | postgres |
| `--password` | PostgreSQL password | postgres |
| `--host` | PostgreSQL host | localhost |
| `--port` | PostgreSQL port | 5432 |
| `--output` | Output JSON file path | metadata_staging.json |

## Output Format

The extracted metadata is saved in `metadata_staging.json` with the following structure:

```json
[
  {
    "database_name": "metadata_source",
    "table_name": "users",
    "row_count": 10,
    "last_updated": "2024-01-15T10:30:00",
    "columns": [
      {"column_name": "id", "data_type": "IntegerType()"},
      {"column_name": "name", "data_type": "StringType()"},
      {"column_name": "email", "data_type": "StringType()"},
      {"column_name": "created_at", "data_type": "TimestampType()"}
    ]
  },
  {
    "database_name": "metadata_source",
    "table_name": "products",
    "row_count": 10,
    "last_updated": "2024-01-15T10:30:00",
    "columns": [
      {"column_name": "id", "data_type": "IntegerType()"},
      {"column_name": "item_name", "data_type": "StringType()"},
      {"column_name": "price", "data_type": "DecimalType(10,2)"},
      {"column_name": "category", "data_type": "StringType()"},
      {"column_name": "last_modified", "data_type": "TimestampType()"}
    ]
  }
]
```

## Database Schema

### Users Table

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| name | VARCHAR(100) | User name |
| email | VARCHAR(255) | User email |
| created_at | TIMESTAMP | Creation timestamp |

### Products Table

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| item_name | VARCHAR(200) | Product name |
| price | DECIMAL(10,2) | Product price |
| category | VARCHAR(100) | Product category |
| last_modified | TIMESTAMP | Last modification timestamp |

## Integration with Catalog Tools

### Amundsen

The JSON output can be ingested into Amundsen using its databuilder library:

```python
from databuilder.extractor.generic_extractor import GenericExtractor
# Load metadata_staging.json and transform for Amundsen's data model
```

### Apache Atlas

For Atlas integration, transform the JSON to Atlas entity format:

```python
import json

# Load metadata
with open('metadata_staging.json', 'r') as f:
    metadata = json.load(f)

# Transform to Atlas entity format
for table in metadata:
    atlas_entity = {
        "typeName": "hive_table",
        "attributes": {
            "name": table["table_name"],
            "qualifiedName": f"{table['database_name']}.{table['table_name']}",
            # ... additional attributes
        }
    }
```

## Project Structure

```
Automated-Data-Catalog-and-Discovery/
├── README.md                  # This file
├── LICENSE                    # MIT License
├── metadata_extractor.py      # Main extraction script
├── requirements.txt           # Python dependencies
└── metadata_staging.json      # Generated output (after running)
```

## Troubleshooting

### Spark not found

```bash
# Set SPARK_HOME environment variable
export SPARK_HOME=/path/to/spark

# Or use findspark in your code
import findspark
findspark.init()
```

### PostgreSQL connection refused

```bash
# Check if PostgreSQL is running
pg_isready -h localhost -p 5432

# Start PostgreSQL service (Ubuntu/Debian)
sudo systemctl start postgresql
```

### JDBC driver not found

```bash
# Download the driver and place it in the current directory
wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Or use --packages option
spark-submit --packages org.postgresql:postgresql:42.6.0 metadata_extractor.py --all
```

## License

MIT License - see [LICENSE](LICENSE) for details