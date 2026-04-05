# Project Architecture: pg_delta_lake_cdc

This document provides a high-level overview of the code organization and data flow within the PostgreSQL to Delta Lake CDC (Change Data Capture) pipeline.

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

## Data Flow (Sequence Diagram)

The following diagram illustrates the lifecycle of a WAL event from PostgreSQL to a Parquet/Delta Lake file.

```mermaid
sequenceDiagram
    participant PG as PostgreSQL
    participant WR as WALReceiver
    participant BB as BoundedBuffer
    participant PW as ParquetWriter
    participant TW as TableWriter
    participant DL as DeltaLogWriter
    participant TR as TableRegistry

    Note over WR,PW: Decoupled via BoundedBuffer

    WR->>PG: START_REPLICATION
    PW->>PW: Start Worker Thread
    
    loop CDC Loop
        PG->>WR: CopyData (WAL Message)
        WR->>WR: Extract LSN & Payload
        WR->>TR: Lookup Relation Metadata
        TR-->>WR: TableInfo
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

## Core Responsibilities

| Component | Responsibility |
| :--- | :--- |
| **WAL Receiver** | Manages PostgreSQL connection, handles logical replication protocol, extracts LSNs, and sends feedback updates to PostgreSQL to advance the replication slot. |
| **Bounded Buffer** | Thread-safe queue providing backpressure and decoupling the network-bound receiver from the disk-bound writer. |
| **Parquet Writer** | Background worker that dispatches messages to table-specific writers and manages the `TableWriter` lifecycle. |
| **Table Writer** | Encapsulates Apache Arrow builders to construct schemas and write Parquet files. |
| **Delta Log Writer**| Static utility for generating Delta Lake protocol JSON files (commits) for ACID compliance. |
| **Table Registry** | Centralized store for mapping PostgreSQL relation OIDs to table schemas and metadata. |
