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
        BB -->|Buffered Drain| PW[ParquetWriter]
        PW -->|Epoch Signal| TQ((Per-Table Queues))
        TQ -->|Async Process| TW[TableWriter Threads]
        TW -->|Epoch-aligned Parquet| BR[(Bronze Delta Table)]
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
        -uint64_t current_epoch_id
        +start()
        +stop()
        -run()
        -processMessage()
        -broadcastFlushSignal(epoch_id)
    }

    class TableWriter {
        -TableInfo info
        -shared_ptr~FileSystem~ fs_
        -string base_path_
        -BoundedBuffer~WalMessage~ queue
        -thread worker_thread
        -atomic~uint64_t~ committed_lsn_val
        -uint64_t insert_count_
        -uint64_t update_count_
        -uint64_t delete_count_
        +start()
        +stop()
        +appendRow()
        +sendFlushSignal(epoch_id)
        -run()
        -processInternal()
        -flushPartition(epoch_id)
        -generateDeltaSchemaJSON()
    }

    class WalMessage {
        +uint32_t relation_id
        +uint64_t lsn
        +vector~char~ payload
        +bool is_flush_signal
        +uint64_t epoch_id
    }

    class DeltaLogWriter {
        <<static>>
        +writeCommit(fs, directory, version, ...)
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
        Note over WR: Capture repl_ident + pk_flag
        WR->>BB: push_for(WalMessage, timeout)
        
        Note over PW: Coordinator
        BB->>PW: Drain available messages
        PW->>TW: dispatch to queue
        
        opt Epoch Trigger (Batch or Commit)
            PW->>TW: sendFlushSignal(epoch_id)
            Note over TW: Parallel worker flush
            TW->>TW: flushPartition(epoch_id)
            TW->>DL: writeCommit(fs_, dynamic_schema)
            TW->>TW: Update last_flushed_epoch
            PW->>TW: Check getLastFlushedEpoch()
            PW->>PW: Advance global committed_lsn
        end
        
        Note over WR: Independent Status Loop
        opt Periodic Heartbeat (5s)
            WR->>PG: StandbyStatusUpdate(global_lsn)
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
- **Design**: Implemented using `libpq` for the PostgreSQL wire protocol. Uses a non-blocking IO loop (`PQconsumeInput` + `PQgetCopyData`).
- **Heartbeat Safety**: Implements a dedicated status update mechanism that continues to send heartbeats to PostgreSQL even if the dispatcher pipeline is temporarily blocked (backpressure), preventing slot timeout.
- **LSN Extraction**: Every WAL message ('w') contains a 64-bit Log Sequence Number (LSN). The receiver parses this and associates it with the data payload.

### 2. BoundedBuffer
The `BoundedBuffer` is the primary decoupling mechanism between the producer (network) and consumer (disk) threads.
- **Design**: A template-based thread-safe circular buffer. It uses `std::mutex` and `std::condition_variable` to coordinate access.
- **Backpressure**: When the buffer reaches its maximum capacity (default 10,000 messages), the `push()` call will block. This prevents the daemon from consuming all system memory if the disk writing or S3 upload becomes a bottleneck.

### 3. TableWriter
The `TableWriter` is now a fully asynchronous worker that manages processing for a specific table in its own thread.
- **Design**: Each `TableWriter` maintains its own `BoundedBuffer` of WAL messages. This allows a slow table (due to large schema or high write volume) to buffer independently without blocking other tables.
- **Async Execution**: It converts PostgreSQL binary tuples into Apache Arrow format via a background `run()` loop.
- **Transaction Atomicity**: Implements `BEGIN`/`COMMIT` tracking. Rows are buffered in internal Arrow builders and only flushed to Parquet upon receiving a `COMMIT` signal. Rolled-back transactions are automatically discarded by resetting builders, ensuring ACID compliance at the storage layer.
- **Schema Evolution Support**: When a schema change is detected, the `TableWriter` is gracefully restarted with the new column layout. It uses a robust **NULL-padding** strategy to handle messages that may still be using the previous metadata during the transition window.
- **Dynamic Schema**: Generates the Delta Lake JSON schema dynamically from PostgreSQL metadata on every partition flush.
- **LSN Tracking**: It tracks both the latest LSN pushed to its queue and the latest LSN successfully committed to Delta Lake.
- **Atomicity**: The write operation remains atomic: Parquet flush followed by Delta Log commit.

### 4. ParquetWriter (Coordinator & LSN Aggregator)
The `ParquetWriter` acts as the central coordinator for the parallel pipeline.
- **Global Epoch Coordination**: Implements cross-table ACID consistency by triggering global "epochs".
- **Barrier Synchronization**: Periodically broadcasts flush signals to all active `TableWriter` threads.
- **Non-blocking State Machine**: Uses a non-blocking synchronization loop that allows the engine to continue processing WAL messages while waiting for epoch confirmation, ensuring that replication heartbeats are never interrupted.
- **Min-LSN Safety Mechanism**: Ensures that the global replication slot only advances to the minimum LSN successfully committed across all parallel tables, preventing data loss on restarts.

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
