# Architecture & Design: pg_delta_lake_cdc

This document describes the high-level architecture, threading model, and the **Medallion Architecture** data flow of the PostgreSQL CDC pipeline.

> [!TIP]
> **Mermaid Support in IDE**: The diagrams below are pre-rendered for maximum compatibility. If you wish to edit them, please modify the Mermaid blocks in the source. To enable native rendering in VS Code, install the **"Markdown Preview Mermaid Support"** extension.

## End-to-End Data Flow (Medallion Architecture)

The pipeline captures real-time changes from a source operational database and materializes them into a refined "Silver" Delta table for analytical use.

![Medallion Architecture](images/mermaid_medallion.png)

<details>
<summary>View Mermaid Source</summary>

```mermaid
graph TD
    subgraph Source
        OP[Operational Data] -->|CRUD| PG[(PostgreSQL)]
    end

    subgraph "CDC Daemon (C++) - Bronze Layer"
        PG -->|pgoutput| WR[WALReceiver]
        WR -->|WAL Messages| BB((Bounded Buffer))
        BB -->|Buffered Pop| PW[ParquetWriter]
        PW -->|Table Routing| TW[TableWriter]
        TW -->|100-row Batches| BR[(Bronze Delta Table)]
    end

    subgraph "Materializer (Spark) - Silver Layer"
        BR -->|Incremental Read| SM[Silver Materializer]
        SM -->|Window Deduplication| MD((Merge Logic))
        MD -->|ACID Merge| SL[(Silver Delta Table)]
    end
```
</details>

| Layer | Type | Responsibility |
| :--- | :--- | :--- |
| **Bronze** | Raw Log | Append-only history of every change. Preserves full audit trail. |
| **Silver** | Materialized | Latest state per record. Deduplicated and ready for BI/Analytics. |

## Component Overview

The system is designed as a producer-consumer architecture using a thread-safe bounded buffer for decoupled processing.

### Class Hierarchy & Organization

![CDC Daemon Architecture](images/mermaid_class.png)

<details>
<summary>View Mermaid Source</summary>

```mermaid
classDiagram
    class main {
        +signalHandler()
        +main()
    }

    class WALReceiver {
        -PGconn* conn
        -BoundedBuffer& buffer
        -TableRegistryPtr registry
        -atomic~uint64_t~* committed_lsn
        +run()
        +stop()
        -receiveLoop()
        -handleCopyData()
        -sendStandbyStatusUpdate(lsn)
    }

    class ParquetWriter {
        -BoundedBuffer& buffer
        -TableRegistryPtr registry
        -atomic~uint64_t~* committed_lsn
        -unordered_map<uint32_t, TableWriterPtr> writers
        +start()
        +stop()
        -run()
        -processMessage()
    }

    class TableWriter {
        -TableInfo info
        -atomic~uint64_t~* committed_lsn
        -uint64_t latest_lsn
        -vector<ArrowBuilderPtr> builders
        +appendRow()
        +flushPartition()
    }

    class WalMessage {
        +uint32_t relation_id
        +uint64_t lsn
        +vector~char~ payload
    }

    class DeltaLogWriter {
        <<static>>
        +writeCommit()
        -escapeJson()
    }

    class TableRegistry {
        -unordered_map<string, TableInfo> tables
        -unordered_map<uint32_t, TableInfo> active_relations
        +addTable()
        +mapRelationId()
        +getTableByRelationId()
    }

    class BoundedBuffer~T~ {
        -queue<T> queue
        -mutex mtx
        -condition_variable cv_full
        -condition_variable cv_empty
        +push()
        +pop()
    }

    main --> WALReceiver : instantiates
    main --> ParquetWriter : instantiates
    WALReceiver ..> BoundedBuffer : pushes WalMessage
    ParquetWriter ..> BoundedBuffer : pops WalMessage
    ParquetWriter --> TableWriter : owns/manages
    WALReceiver --> TableRegistry : uses for lookups
    ParquetWriter --> TableRegistry : uses for lookups
    TableWriter --> TableRegistry : uses metadata
    TableWriter ..> DeltaLogWriter : uses for commits
```
</details>

## Detailed Data Flow (Sequence Diagram)

### I. WAL Capture & Bronze Writing (C++ Daemon)

This diagram illustrates the lifecycle of a WAL event from PostgreSQL to a raw Delta log.

![WAL Capture Flow](images/mermaid_wal.png)

<details>
<summary>View Mermaid Source</summary>

```mermaid
sequenceDiagram
    participant PG as PostgreSQL
    participant WR as WALReceiver
    participant BB as BoundedBuffer
    participant PW as ParquetWriter
    participant TW as TableWriter
    participant DL as DeltaLogWriter

    loop CDC Loop
        PG->>WR: CopyData (WAL Message)
        WR->>WR: Extract LSN & Payload
        WR->>BB: push(WalMessage{lsn, ...})
        
        BB->>PW: pop()
        PW->>TW: appendRow(tuple, lsn)
        
        Note over TW: Buffers rows in memory
        
        opt row_group_size reached
            TW->>TW: flushPartition()
            Note right of TW: Writes Parquet file
            TW->>DL: writeCommit()
            TW->>TW: Update committed_lsn (Atomic)
        end
        
        opt Periodic Heartbeat (5s)
            WR-->>WR: Read committed_lsn
            WR->>PG: StandbyStatusUpdate(lsn)
            Note left of PG: Advances Replication Slot
        end
    end
```
</details>

### II. Silver Materialization (Spark Incremental Merge)

Illustrates how the downstream Spark process reconciles the raw Bronze log into a deduplicated Silver state.

![Silver Materialization Flow](images/mermaid_spark.png)

<details>
<summary>View Mermaid Source</summary>

```mermaid
sequenceDiagram
    participant BR as Bronze Delta
    participant SM as Spark Materializer
    participant WT as Window Transformer
    participant SL as Silver Delta

    SM->>BR: Read Change Feed (Incremental)
    BR-->>SM: Raw CDC Events (Insert/Update/Delete)
    SM->>WT: row_number() OVER (PARTITION BY id ORDER BY _cdc_timestamp DESC)
    WT-->>SM: Deduplicated Batch
    SM->>SL: MERGE (Update where Matched, Insert where Not)
    Note over SL: ACID Materialized State
```
</details>

## Component Details

### 1. WALReceiver
The `WALReceiver` is a networking component responsible for the high-performance ingestion of PostgreSQL change events.
- **Design**: Implemented using `libpq` for the PostgreSQL wire protocol. It establishes a `LOGICAL_REPLICATION` connection using the `pgoutput` plugin.
- **LSN Extraction**: Every WAL message ('w') contains a 64-bit Log Sequence Number (LSN). The receiver parses this from the binary stream (using `be64toh` for endian conversion) and associates it with the data payload.
- **Feedback Loop**: It hosts a 5-second recurring timer that reads a shared `committed_lsn` value (populated by the writers) and sends a `Status Update` back to PostgreSQL. This tells the server it can safely rotate its WAL logs.

### 2. BoundedBuffer
The `BoundedBuffer` is the primary decoupling mechanism between the producer (network) and consumer (disk) threads.
- **Design**: A template-based thread-safe circular buffer. It uses `std::mutex` and `std::condition_variable` to coordinate access.
- **Backpressure**: When the buffer reaches its maximum capacity (default 10,000 messages), the `push()` call will block. This prevents the daemon from consuming all system memory if the disk writing or S3 upload becomes a bottleneck.

### 3. TableWriter
The `TableWriter` is the "transaction" engine for each individual table.
- **Design**: It encapsulates **Apache Arrow** builders to convert PostgreSQL binary tuples into highly compressed columnar format.
- **CDC Metadata**: It automatically appends two metadata columns: `_cdc_op` (the operation type) and `_cdc_timestamp` (the processing time).
- **Atomicity**: The write operation is a two-step process: first, a Parquet file is written to the `/data` directory; second, a Delta Lake commit (JSON) is generated. Only after both succeed is the LSN marked as "committed".

### 4. ParquetWriter
The `ParquetWriter` acts as an orchestrator and multiplexer for all active tables.
- **Design**: It runs on its own background thread, continuously popping messages from the `BoundedBuffer`.
- **Dynamic Routing**: It maintains a map of `Relation ID -> TableWriter`. When a message arrives for a new table, it dynamically instantiates a new `TableWriter` by looking up the schema in the `TableRegistry`.
- **Resource Management**: It ensures that all pending data is flushed to disk before the daemon shuts down cleanly.

### 5. DeltaLogWriter
A static utility class that implements the **Delta Lake Transaction Log Protocol**.
- **Design**: It generates versioned JSON files in the `_delta_log/` directory.
- **ACID Compliance**: Each JSON file represents an atomic commit that adds the newly written Parquet file to the table metadata. This ensures that downstream Spark/DuckDB readers see a consistent, point-in-time snapshot of the data.

## LSN Acknowledgment Flow

Correct LSN handling is critical for preventing data loss and managing database storage. The system implements a **Flush-then-Confirm** strategy:

![LSN Flow](images/lsn_flow.png)

1.  **Extraction**: The `WALReceiver` captures the LSN from the replication stream and tags each message.
2.  **Buffering**: The LSN travels with the data through the `BoundedBuffer`.
3.  **Persistence**: `TableWriter` receives the rows. When a batch (e.g., 100 rows) is reached, it flushes the Parquet file to storage.
4.  **Delta Commit**: After the file is on disk, `TableWriter` generates the Delta Lake commit log.
5.  **Atomic Advancement**: Only after the Delta commit is successful, the `TableWriter` updates a shared `std::atomic<uint64_t>` variable called `committed_lsn_`.
6.  **Server Notification**: In the next 5-second interval, the `WALReceiver` reads this atomic value and sends an `r` message (Standby Status Update) to PostgreSQL.
7.  **Slot Advancement**: PostgreSQL receives the LSN and advances the `confirmed_flush_lsn` for the replication slot, allowing it to safely discard old WAL segments.
