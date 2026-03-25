# pg_delta_lake_cdc

A C++20 standalone Linux daemon that acts as a PostgreSQL logical replication client, ingesting data directly into local Apache Arrow/Parquet columnar files.

## Overview
This daemon performs Change Data Capture (CDC) utilizing PostgreSQL's native `pgoutput` plugin. It asynchronously pulls WAL insert streams, parses them, and pivots them into heavily optimized columnar layouts utilizing the Apache Arrow ecosystem.

To prevent backpressure and out-of-memory errors on high ingestion spikes, a concurrent Bounded Buffer cleanly separates the network listener thread from the disk-writing Parquet conversion thread.

## Dependencies
- **CMake** 3.16+
- **C++20 Compiler**
- **PostgreSQL Client Utilities** (`libpq-dev`)
- **Apache Arrow** (`libarrow-dev`, `libparquet-dev`)

### Installation (Ubuntu Ubuntu 24.04 'Noble')
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
The daemon requires a `PG_CONNINFO` environment variable or `.env` file to connect to Postgres. Create a `.env` in the same directory as the executable:

```env
PG_CONNINFO=postgres://youruser:yourpass@localhost:5432/yourdb
# Make sure your Postgres database has wal_level=logical configured!
```

## Running
Before starting the daemon, ensure you've initialized the publication and replication slot in your database:
```sql
CREATE PUBLICATION hn_cdc_stream FOR ALL TABLES;
SELECT pg_create_logical_replication_slot('hn_cdc_stream_slot', 'pgoutput');
```

Start the daemon:
```bash
./build/pg_delta_lake_cdc
```

The daemon will natively listen for inserts and automatically flush `output_<timestamp>.parquet` files into your working directory whenever the in-memory buffered batch reaches 10,000 requests.
