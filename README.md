# pg_delta_lake_cdc

A C++20 standalone Linux daemon that acts as a PostgreSQL logical replication client, ingesting data directly into local Apache Arrow/Parquet columnar files.

## Overview
This daemon performs Change Data Capture (CDC) utilizing PostgreSQL's native `pgoutput` plugin. It asynchronously pulls WAL insert streams, parses them, and pivots them into heavily optimized columnar layouts utilizing the Apache Arrow ecosystem.

## Features

- **Dynamic Schema Extraction:** On startup, the daemon queries PostgreSQL's `information_schema` and natively maps PostgreSQL data types to Apache Arrow types (`Int32Builder`, `DoubleBuilder`, `BooleanBuilder`, `StringBuilder`, etc.).
- **Multi-Table Streaming:** Actively parses `pgoutput` Relation ('R') messages, mapping binary OIDs directly to schema/table names to support infinite concurrent dynamic tables!
- **Zero-Config Parquet Routing:** Routes all incoming inserts/updates to dedicated `TableWriter` instances per table!
- **CDC Metadata Injection:** Automatically injects `_cdc_op` (INSERT/UPDATE) and `_cdc_timestamp` (event time in ms) into every Parquet row!
- **Sequential Parquet Naming:** Generates clean, sequential files (e.g., `stories_1.parquet`, `stories_2.parquet`) for easier downstream ingestion.
- **Low-Latency Flushing:** Commits highly optimized `.parquet` files to a configurable output directory whenever any individual table hits a threshold (now 100 rows).

## Architecture
To prevent backpressure and out-of-memory errors on high ingestion spikes, a concurrent Bounded Buffer cleanly separates the network listener thread from the disk-writing Parquet conversion thread.

## Dependencies
- **CMake** 3.16+
- **C++20 Compiler**
- **PostgreSQL Client Utilities** (`libpq-dev`)
- **Apache Arrow & Parquet** (`libarrow-dev`, `libparquet-dev`)

### Installation (Ubuntu 24.04 'Noble')
```bash
sudo apt update
sudo apt install -y -V ca-certificates lsb-release wget
wget https://apache.jfrog.io/artifactory/arrow/ubuntu/apache-arrow-apt-source-latest-noble.deb
sudo apt install -y -V ./apache-arrow-apt-source-latest-noble.deb
sudo apt update
sudo apt install -y -V libarrow-dev libparquet-dev libpq-dev
```

## Compilation
```bash
cmake -B build
cmake --build build
```

## Configuration
The daemon utilizes a `.env` file for zero-config startup. Create a `.env` in the project root:

```env
PG_CONNINFO=postgres://user:pass@localhost:5432/my_hn?sslmode=disable
PG_SLOT_NAME=hn_stories_slot
PG_PUBLICATION_NAME=hn_stories_pub
OUTPUT_DIR=data
```

- **PG_SLOT_NAME**: The logical replication slot name.
- **PG_PUBLICATION_NAME**: The Postgres publication name.
- **OUTPUT_DIR**: The directory where `.parquet` files will be stored (automatically created).

## Deployment & Running
1. **Initialize PostgreSQL Publication**:
   ```sql
   CREATE PUBLICATION hn_stories_pub FOR TABLE stories;
   SELECT pg_create_logical_replication_slot('hn_stories_slot', 'pgoutput');
   ```

2. **Start the Daemon**:
   ```bash
   ./build/pg_delta_lake_cdc
   ```

The daemon will now listen for inserts/updates and automatically flush sequential `.parquet` files into your `data/` directory every 100 rows.
