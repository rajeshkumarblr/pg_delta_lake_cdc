# Architecture & Design: pg_delta_lake_cdc

This document describes the high-level architecture, threading model, and the **Medallion Architecture** data flow of the PostgreSQL CDC pipeline.

## End-to-End Data Flow (Medallion Architecture)

The pipeline captures real-time changes from a source PostgreSQL database and materializes them into a refined "Silver" Delta table for analytical use.

```mermaid
graph TD
    subgraph Source
        HN[Hacker News API] -->|Go Ingest| PG[(PostgreSQL)]
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

| Layer | Type | Responsibility |
| :--- | :--- | :--- |
| **Bronze** | Raw Log | Append-only history of every change. Preserves full audit trail. |
| **Silver** | Materialized | Latest state per record. Deduplicated and ready for BI/Analytics. |

## Component Overview

The system is designed as a producer-consumer architecture using a thread-safe bounded buffer for decoupled processing.

### Class Hierarchy & Organization

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

## Detailed Data Flow (Sequence Diagram)

### I. WAL Capture & Bronze Writing (C++ Daemon)

This diagram illustrates the lifecycle of a WAL event from PostgreSQL to a raw Delta log.

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

### II. Silver Materialization (Spark Incremental Merge)

Illustrates how the downstream Spark process reconciles the raw Bronze log into a deduplicated Silver state.

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

## Core Responsibilities

| Component | Responsibility |
| :--- | :--- |
| **WAL Receiver** | Manages PostgreSQL connection, handles logical replication protocol, extracts LSNs, and sends feedback updates to PostgreSQL to advance the replication slot. |
| **Bounded Buffer** | Thread-safe queue providing backpressure and decoupling the network-bound receiver from the disk-bound writer. |
| **Parquet Writer** | Background worker that dispatches messages to table-specific writers and manages the `TableWriter` lifecycle. |
| **Table Writer** | Encapsulates Apache Arrow builders to construct schemas, injects CDC metadata (`_cdc_op`, `_cdc_timestamp`), and writes Parquet files. |
| **Delta Log Writer**| Static utility for generating Delta Lake protocol JSON files (commits) for ACID compliance. |
| **Table Registry** | Centralized store for mapping PostgreSQL relation OIDs to table schemas and metadata. |
| **Silver Materializer**| Python-based Spark application that performs deduplication and MERGE operations into the Silver layer. |
