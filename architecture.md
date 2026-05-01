# Architecture & Design: pg_delta_lake_cdc

This document describes the high-level architecture, threading model, and the **Medallion Architecture** data flow of the PostgreSQL CDC pipeline.

> [!TIP]
> **Mermaid Support in IDE**:
> 1. **Native Rendering**: Install the **"Markdown Preview Mermaid Support"** extension (ID: `bierner.markdown-mermaid`).
> 2. **Pre-rendered Images**: The diagrams are pre-rendered in the `images/` directory. To update them after modifying the source, run:
>    ```bash
>    python3 scripts/render_diagrams.py
>    ```

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
        -uint64_t watermark_lsn_
        +run()
        +stop()
        +connect()
        +startLogicalReplication()
        +performSnapshot()
        +getWatermarkLsn()
        -receiveLoop()
        -handleCopyData()
        -handleRelationMessage()
        -handleDataMessage()
        -handleKeepAliveMessage()
        -sendStandbyStatusUpdate(lsn)
    }

    class ParquetWriter {
        -BoundedBuffer& buffer
        -TableRegistryPtr registry
        -atomic~uint64_t~* committed_lsn
        -unordered_map<uint32_t, TableWriterPtr> writers
        -uint64_t current_epoch_id_
        -uint64_t current_max_lsn_
        -uint64_t pending_epoch_id_
        -uint64_t pending_epoch_lsn_
        +start()
        +stop()
        -run()
        -processMessage()
        -broadcastFlushSignal(epoch_id)
        -stopAllWriters()
    }

    class TableWriter {
        -TableInfo info
        -shared_ptr~FileSystem~ fs_
        -string base_path_
        -BoundedBuffer~WalMessage~ queue
        -thread worker_thread
        -atomic~uint64_t~ committed_lsn_val
        -atomic~uint64_t~ last_flushed_epoch_
        -uint64_t insert_count_
        -uint64_t update_count_
        -uint64_t delete_count_
        +start()
        +stop()
        +appendRow()
        +sendFlushSignal(epoch_id)
        +forceFlush()
        +getOldestPendingLSN()
        +processSnapshotCopy()
        -run()
        -processInternal()
        -flushPartition(epoch_id)
        -setupSchemaAndBuilders()
        -generateDeltaSchemaJSON()
    }

    class WalMessage {
        +uint32_t relation_id
        +uint64_t lsn
        +vector~char~ payload
        +bool is_flush_signal
        +uint64_t epoch_id
        +char pg_msg_type
    }

    class DeltaLogWriter {
        <<static>>
        +writeCommit(fs, directory, version, ...)
    }

    class TableRegistry {
        -unordered_map<string, TableInfo> tables
        -unordered_map<uint32_t, TableInfo> active_relations
        +addTable()
        +getTable()
        +mapRelationId()
        +getTableByRelationId()
        +getAllTables()
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
    ParquetWriter *-- TableWriter : owns pre-initialized workers
    WALReceiver --> TableRegistry : uses for lookups
    ParquetWriter --> TableRegistry : uses for lookups
    TableWriter --> TableRegistry : uses metadata
    TableWriter ..> DeltaLogWriter : uses for commits

    style main fill:#f9f,stroke:#333,stroke-width:2px
    style WALReceiver fill:#bbf,stroke:#333,stroke-width:2px
    style ParquetWriter fill:#bfb,stroke:#333,stroke-width:2px
    style TableWriter fill:#fbb,stroke:#333,stroke-width:2px
    style BoundedBuffer fill:#fff,stroke:#333,stroke-dasharray: 5 5
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
        Note over WR: Extract LSN + Message Type
        WR->>BB: push_for(WalMessage, timeout)
        
        loop Coordinator Tick
            BB->>PW: Drain available messages
            PW->>TW: appendRow(payload, lsn, type)
            Note over PW: Update current_max_lsn_
            
            opt Epoch Trigger (10s or 50k rows)
                PW->>PW: Set pending_epoch_id_
                PW->>TW: sendFlushSignal(epoch_id)
            end

            opt Check Epoch Completion
                PW->>TW: getLastFlushedEpoch()
                Note over PW: All writers finished?
                PW->>PW: committed_lsn.store(pending_lsn)
                PW->>PW: Reset pending_epoch_id_
            end
        end

        Note over TW: Parallel worker execution
        TW->>TW: processInternal()
        opt Flush Condition
            TW->>TW: flushPartition(epoch_id)
            TW->>DL: writeCommit(fs_, dynamic_schema)
            TW->>TW: Update last_flushed_epoch_
        end
        
        Note over WR: Independent Status Loop
        opt Periodic Heartbeat (5s)
            WR->>PG: StandbyStatusUpdate(committed_lsn)
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

#### 3. TableWriter: The Parallel Execution Engine
The `TableWriter` is the heart of the engine's data processing pipeline. It is a fully asynchronous worker that manages processing for a specific table in a dedicated thread.

> [!NOTE]
> **Performance Tip**: By decoupling tables into their own threads, we prevent a single outlier (e.g., a high-volume `logs` table) from backpressuring the ingestion of critical operational data like `orders` or `users`.

- **Stable Worker Lifecycle**: Workers are **pre-initialized** at daemon startup. This architectural shift from dynamic creation eliminates the "Signal Race Conditions" and thread creation overhead that often plague naive CDC implementations during schema refreshes.
- **Asynchronous Queueing**: Every table worker maintains its own `BoundedBuffer` queue. The coordinator pushes messages to these queues, allowing the main ingestion stream to continue uninterrupted even if a specific table's disk IO is slow.
- **Unified Handover Guard**: Uses a strict **LSN Boundary Guard** (`msg.lsn < watermark_lsn`). During the transition from historical binary `COPY` snapshot to real-time stream, the engine perfectly suppresses duplicates while ensuring that even "Edge-Case" deletions happening *exactly* at the slot creation boundary are captured.
- **Transactional Row Commits (Two-Phase Parsing)**: 
    - **Phase 1 (Validation)**: Every PostgreSQL message is parsed into a temporary local buffer.
    - **Phase 2 (Memory Commit)**: Only if the entire row (including metadata and variable-length columns) is valid, it is committed to the Arrow/Parquet builders. 
    - **Impact**: This definitively solves the "Parquet Column Mismatch" corruption that occurs when partial failure leaves builders in an inconsistent state.
- **Dynamic Delta Schema Generation**: On every partition flush, the engine generates a consistent `_delta_log` JSON entry, ensuring the Delta table remains ACID-compliant and queryable by standard lakehouse tools (Spark, Presto, DuckDB).

### 4. ParquetWriter (Coordinator & Epoch Manager)
The `ParquetWriter` acts as the central coordinator for the parallel pipeline.
- **Global Epoch Coordination**: Implements cross-table consistency by triggering and monitoring global "epochs". An epoch is triggered every 10 seconds or every 50,000 processed rows.
- **Barrier Synchronization**: Broadcasts flush signals to all active `TableWriter` threads when an epoch is triggered.
- **Non-blocking State Machine**: Uses an asynchronous check loop to monitor `getLastFlushedEpoch()` from all writers. It only advances the global committed LSN after all writers have successfully flushed the epoch to storage.
- **Min-LSN Safety Mechanism**: This ensures that the global replication slot in PostgreSQL only advances to a point where data is guaranteed to be persistent in Delta Lake.

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
