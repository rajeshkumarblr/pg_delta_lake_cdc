# pg_delta_lake_cdc

A high-performance PostgreSQL-to-Delta Lake CDC (Change Data Capture) pipeline. This project implements a C++20 daemon for real-time "Bronze" ingestion and a Spark Structured Streaming application for "Silver" materialization.

## High-Level Overview

```mermaid
graph LR
    PG[(PostgreSQL)] -- WAL Stream --> CDC[CDC Engine]
    CDC --> DL[Delta Logs + Parquet]
    DL --> AJ[Analytics Jobs]
```

## Overview
This pipeline captures real-time database changes (WAL) utilizing PostgreSQL's native `pgoutput` plugin, pivots them into optimized Apache Arrow/Parquet layouts, and incrementally merges them into a refined Silver layer for analytics.

### Feature Highlights
- **Production-Ready CDC (100% Data Integrity)**: Verified against a 10,015-row stress test covering complex transaction boundaries and schema changes.
- **Transactional Atomicity (ACID)**: Implemented `BEGIN`/`COMMIT` protocol awareness. Data is only flushed at transaction boundaries, ensuring no partial or rolled-back rows are visible.
- **Dynamic Schema Evolution**: Runtime detection of `ALTER TABLE` changes. Automatically handles column additions with robust NULL-padding and Delta Log metadata updates.
- **Parallel Per-Table Streaming**: High-throughput multi-threaded architecture with independent queues for each table.
- **Stability & Protocol Resilience**: Non-blocking Global Epoch synchronization and heartbeat-aware buffer pushes ensure the replication slot stays alive even under heavy backpressure.
- **Integrated Verification Suite**: Native DuckDB-based integration tests for validating data integrity, schema consistency, and rollback handling.
- **Native Delta Lake Producer**: Generates ACID-compliant `_delta_log` transaction entries alongside LSN-named Parquet files.
- **CDC Metadata Injection**: Automatically injects `_cdc_op`, `_cdc_timestamp`, and `_cdc_lsn` into every row for downstream deduplication.

For a deep dive into the internals and sequence diagrams, see [architecture.md](architecture.md).

## Medallion Architecture Support
The system is designed to support a **Medallion Architecture** out of the box:
- **Bronze Layer**: Raw, immutable CDC event stream (Inserts/Updates) written by the C++ daemon into the Delta table.
- **Silver Layer**: A deduplicated, "latest state" view of the data, reconstructed by downstream consumers (like Spark) using the `_cdc_timestamp` metadata.

For more detailed architecture notes, see [architecture.md](architecture.md).
### 1. Start the Stack (Postgres + CDC)
```bash
cd test
docker-compose up --build -d
```

### 2. Start the Silver Materializer
Ensure you have Spark installed in your local environment:
```bash
# From project root with .venv activated
python3 test/silver_materializer.py
```
This will tail the "Bronze" table and materialize changes into `./output/silver_articles`.

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
