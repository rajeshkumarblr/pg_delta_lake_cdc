# Architecture & Design: pg_delta_lake_cdc

This document describes the high-level architecture, threading model, and data flow of the PostgreSQL CDC daemon.

## High-Level Architecture

The daemon is designed as a **Producer-Consumer** system separated by a thread-safe **Bounded Buffer**. This architecture prevents network ingestion spikes from causing memory overflow or blocking the PostgreSQL replication stream.

```mermaid
graph LR
    subgraph "PostgreSQL"
        WAL["WAL (Write Ahead Log)"]
        PUB["Publication"]
        SLOT["Replication Slot (pgoutput)"]
    end

    subgraph "CDC Daemon"
        NR["NetworkReceiver (Producer Thread)"]
        BB["BoundedBuffer (Thread-Safe Queue)"]
        PW["ParquetWriter (Consumer Thread)"]
        TR["TableRegistry"]
        TW["TableWriter (per-table)"]
    end

    subgraph "Storage"
        PQ["Parquet Files (data/)"]
    end

    WAL --> SLOT
    SLOT --> NR
    NR -- "WalMessage" --> BB
    BB -- "WalMessage" --> PW
    PW -- "Table Map" --> TW
    TW -- "Sequential Writes" --> PQ
    NR -- "Fetch Schema" --> TR
    PW -- "Lookup Mapping" --> TR
```

## Core Components

### 1. NetworkReceiver (Producer)
The `NetworkReceiver` is the entry point for data ingestion.
-   **Schema Fetching**: On startup, it queries `information_schema.columns` to build an initial mapping of table structures.
-   **Replication Loop**: It uses `libpq` to establish a logical replication connection.
-   **Message Handling**: It parses `pgoutput` messages:
    -   **Relation ('R')**: Updates the `TableRegistry` with mapping from OIDs to table names.
    -   **Insert/Update ('I'/'U')**: Extracts the payload and pushes it as a `WalMessage` into the `BoundedBuffer`.

### 2. BoundedBuffer
A thread-safe circular buffer (templated) that provides:
-   **Backpressure**: If the buffer is full, the producer will wait (or drop, depending on config), preventing out-of-memory errors.
-   **Decoupling**: Allows the network thread to remain responsive even if disk I/O is slow.

### 3. ParquetWriter (Consumer)
A dedicated worker thread that:
-   Pops `WalMessage` objects from the buffer.
-   Routes messages to the appropriate `TableWriter` based on the `relation_id`.
-   Automatically manages the lifecycle of multiple `TableWriter` instances (one per table).

### 4. TableWriter
Responsible for the final conversion to columnar format:
-   **Arrow Mapping**: Maps Postgres types (int, float, text, etc.) to Apache Arrow builders.
-   **CDC Metadata Injection**: Automatically appends `_cdc_op` and `_cdc_timestamp` to every row.
-   **Sequential Flushing**: Once a table hits **100 rows**, it flushes a new Parquet file (e.g., `stories_1.parquet`) to the `data/` directory.

## Stress Testing & Verification
A dedicated `test/` directory provides a high-speed ingestion framework:
- **hn_ingest**: A Go service that pulls live Hacker News data.
- **Stress Mode**: Fetches 500 items per interval to test daemon throughput.
- **Simplified Storage**: All AI-related overhead (embeddings/summaries) has been removed to maximize raw ingestion speed.

## Data Flow: From Postgres to Parquet

```mermaid
sequenceDiagram
    participant PG as PostgreSQL
    participant NR as NetworkReceiver
    participant BB as BoundedBuffer
    participant PW as ParquetWriter
    participant TW as TableWriter

    PG->>NR: pgoutput Message (Relation R)
    NR->>NR: Map OID to Table Schema
    PG->>NR: pgoutput Message (Insert I)
    NR->>BB: Push WalMessage
    BB->>PW: Pop WalMessage
    PW->>TW: appendRow(payload)
    TW->>TW: Buffer Arrow Rows (0..99)
    Note over TW: On 100th Row
    TW->>TW: Inject Metadata (_cdc_op, _cdc_timestamp)
    TW->>TW: Write Table to Parquet
    TW-->>TW: Increment file_counter_
```

## Configuration (via .env)
The daemon is highly configurable without re-compilation:
| Variable | Description | Default |
| :--- | :--- | :--- |
| `PG_CONNINFO` | Postgres connection string | `host=localhost...` |
| `PG_SLOT_NAME` | Replication slot to listen on | `hn_cdc_stream_slot` |
| `PG_PUBLICATION_NAME` | Publication name in Postgres | `hn_cdc_stream` |
| `OUTPUT_DIR` | Target directory for Parquet files | `data` |
