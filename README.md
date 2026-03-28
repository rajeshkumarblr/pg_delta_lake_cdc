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

## Design & Architecture

The daemon utilizes a **Producer-Consumer** pattern separated by a thread-safe **Bounded Buffer**.

### CDC Daemon Internals
![Internal Architecture](images/architecture.png)

### End-to-End Stress Test Environment
![End-to-End Test Architecture](images/overview.png)

For more detailed architecture notes, see [design.md](design.md).
## Quick Start with Docker (Recommended)
The easiest way to see the system in action is using the integrated test infrastructure. This launches PostgreSQL, a high-speed Hacker News ingestion service, and the CDC daemon in a single command.

```bash
cd test
docker-compose up --build
```
Parquet files will be generated in `test/data/`.

## Manual Compilation
### Dependencies
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

### Build
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

## Testing & Stress Testing
The `test/` directory contains a high-speed ingestion service (`hn_ingest`) designed to stress-test the CDC pipeline.

1.  **Stress Mode**: The ingestion service fetches 500 new stories every 10 seconds.
2.  **Row Threshold**: The daemon is configured to flush Parquet files every 100 rows to demonstrate real-time conversion.
3.  **Consolidated Repo**: Everything needed for the test is contained within `test/`, including `docker-compose.yaml` and `init.sql`.

## Deployment
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

## Troubleshooting

### Editor / IntelliSense Errors (`libpq-fe.h` not found)
If your editor (VS Code/Antigravity) shows "file not found" errors for `libpq-fe.h` despite being able to build, this is likely an indexing issue:
1. Ensure `libpq-dev` is installed: `sudo apt install libpq-dev`.
2. This project includes a `.clangd` and `.vscode/c_cpp_properties.json` configured for Ubuntu 24.04.
3. If using Clangd, run `Ctrl+Shift+P` -> `Clangd: Restart Language Server` to refresh the index.
