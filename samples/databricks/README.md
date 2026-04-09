# Azure Databricks Zero-ETL Sample

This sample demonstrates how to integrate the `pg_delta_lake_cdc` engine with **Azure Databricks** to create a modern, high-performance Lakehouse architecture.

## Architecture
1. **CDC Daemon**: Runs in a container (AKS, Container Apps, or VM) and streams changes from PostgreSQL to **ADLS Gen2**.
2. **Delta Lake**: Storage layer on ADLS Gen2.
3. **Databricks**: Spark-based materializer deduplicates raw events into a clean "Silver" layer.

## Setup Guide

### 1. Azure Storage (ADLS Gen2)
Create an ADLS Gen2 Storage Account and a container (e.g., `bronze-data`).
- **Endpoint**: `abfss://<container>@<account>.dfs.core.windows.net`

### 2. Configure the CDC Daemon
Run the daemon with the `az://` output directory. You will need to provide Azure credentials (e.g., `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_KEY` or via Managed Identity if running on Azure).

```bash
# Example environment variables
PG_CONNINFO=postgres://user:pass@db-host:5432/db
OUTPUT_DIR=az://bronze-data/cdc_events
```

### 3. Deploy the Databricks Materializer
1. Upload `silver_materializer.py` to your Databricks Workspace.
2. Create a **Databricks Job** or run a **Notebook** with the following parameters:
   - `--bronze_path`: `abfss://bronze-data@<account>.dfs.core.windows.net/cdc_events`
   - `--silver_path`: `abfss://silver-data@<account>.dfs.core.windows.net/materialized_table`
   - `--checkpoint_path`: `abfss://checkpoints@<account>.dfs.core.windows.net/cdc_job`

### 4. Zero-ETL Deduplication
The provided script uses the `MERGE INTO` pattern to ensure that the Silver table always represents the latest state of your PostgreSQL data, correctly handling `DELETE` operations and `UPDATE` ordering via LSNs.
