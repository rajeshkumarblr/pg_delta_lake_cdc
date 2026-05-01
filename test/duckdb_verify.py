import duckdb
import os
import sys
import time

def verify():
    print("Starting DuckDB Verification...")
    
    expected_file = "/app/data/expected.txt"
    while not os.path.exists(expected_file):
        print("Waiting for workload generator to finish...")
        time.sleep(5)
    
    expected = {}
    with open(expected_file, "r") as f:
        for line in f:
            key, val = line.strip().split("=")
            expected[key] = float(val)

    delta_path = "/app/data/integration_test"
    
    # Wait for Delta files to appear
    retries = 20
    while retries > 0:
        if os.path.exists(os.path.join(delta_path, "_delta_log")):
            break
        print(f"Waiting for Delta table at {delta_path}...")
        time.sleep(5)
        retries -= 1

    if retries == 0:
        print("Delta table never appeared!")
        sys.exit(1)

    print("Waiting 30 seconds for final epochs to flush...")
    time.sleep(30)

    # Wait for Parquet files to appear (timeout 60s)
    import glob
    retries = 12
    parquet_pattern = os.path.join(delta_path, "**", "*.parquet")
    while retries > 0:
        files = glob.glob(parquet_pattern, recursive=True)
        if len(files) > 0:
            print(f"Found {len(files)} parquet files. Proceeding with audit...")
            break
        print(f"Waiting for parquet files at {parquet_pattern}... ({retries} retries left)")
        time.sleep(5)
        retries -= 1
    
    if retries == 0:
        print("No parquet files appeared in time!")
        sys.exit(1)

    con = duckdb.connect()
    query = f"""
        SELECT 
            count(*) as cnt,
            sum(score) as score_sum
        FROM (
            SELECT id, score, _cdc_op, _cdc_lsn, _cdc_timestamp,
                   row_number() OVER (PARTITION BY id ORDER BY _cdc_lsn DESC, _cdc_timestamp DESC) as rn
            FROM read_parquet('{parquet_pattern}', union_by_name=True)
        ) 
        WHERE rn = 1 AND _cdc_op != 'DELETE'
    """
    
    stats = con.execute(query).fetchone()
    actual_count = stats[0]
    actual_sum = stats[1]

    # 2. Rollback Verification (Negative Test)
    # Rows 80000-80009 were rolled back in PG, so they should NOT be in Delta
    rollback_check = con.execute(f"""
        SELECT count(*) FROM read_parquet('/app/data/integration_test/**/*.parquet', union_by_name=True)
        WHERE id BETWEEN 80000 AND 80009
    """).fetchone()[0]

    # 3. Schema Evolution Verification
    # Check if 'priority' and 'tags' columns exist and have expected values
    schema_check = con.execute(f"DESCRIBE SELECT * FROM read_parquet('/app/data/integration_test/**/*.parquet', union_by_name=True)").fetchall()
    column_names = [r[0] for r in schema_check]
    has_evolved_cols = "priority" in column_names and "tags" in column_names
    
    # 5. Check for DELETE operations (Bronze layer)
    res_delete = con.execute(f"SELECT count(*) FROM read_parquet('/app/data/integration_test/**/*.parquet', union_by_name=True) WHERE _cdc_op = 'DELETE'").fetchone()[0]
    
    # 7. Snapshot Verification
    res_snapshot = con.execute(f"SELECT count(*) FROM read_parquet('/app/data/integration_test/**/*.parquet', union_by_name=True) WHERE _cdc_op = 'SNAPSHOT'").fetchone()[0]
    expected_deletes = int(res_snapshot) + 1 # 100 historical + 1 workload delete
    print(f"Snapshot Records (Historical): {res_snapshot} (Expected: 100)")

    # 6. Secondary Table Verification
    secondary_path = "/app/data/secondary_test/**/*.parquet"
    res_secondary = 0
    if os.path.exists("/app/data/secondary_test"):
         res_secondary = con.execute(f"""
            SELECT count(*) FROM (
                SELECT id,
                       row_number() OVER (PARTITION BY id ORDER BY _cdc_lsn DESC, _cdc_timestamp DESC) as rn
                FROM read_parquet('{secondary_path}')
            ) WHERE rn = 1
         """).fetchone()[0]
    
    print("-" * 40)
    print("VERIFICATION RESULTS (DUCKDB)")
    print(f"Expected Count (Main): {expected['COUNT']}, Actual: {actual_count}")
    print(f"Expected Sum   (Main): {expected['SUM']:.4f}, Actual: {actual_sum:.4f}")
    print(f"Expected Count (Sec):  {expected['SECONDARY_COUNT']}, Actual: {res_secondary}")
    print(f"Rollback Check: {rollback_check} rows found (Expected: 0)")
    print(f"Delete Check:   {res_delete} rows found (Expected: {expected_deletes})")
    print(f"Snapshot Check: {res_snapshot} rows found (Expected: 100)")
    print(f"Schema Evolution: {'SUCCESS' if has_evolved_cols else 'FAILED'} (Columns: {', '.join(column_names)})")
    print("-" * 40)

    success = True
    if actual_count != int(expected['COUNT']):
        print(f"FAIL: Row count mismatch in primary table! Expected {expected['COUNT']}, got {actual_count}")
        success = False
    
    if res_secondary != int(expected['SECONDARY_COUNT']):
        print(f"FAIL: Row count mismatch in secondary table! Expected {expected['SECONDARY_COUNT']}, got {res_secondary}")
        success = False
    
    if abs(float(actual_sum) - expected['SUM']) > 0.1:
        print("FAIL: Aggregate sum mismatch!")
        success = False

    if rollback_check != 0:
        print(f"FAIL: {rollback_check} rollback rows leaked into Delta!")
        success = False

    if res_snapshot < 100:
        print(f"FAIL: Snapshot missing or incomplete! Expected 100, got {res_snapshot}")
        success = False

    if not has_evolved_cols:
        print("FAIL: Evolved columns (priority/tags) missing!")
        success = False

    if success:
        print("SUCCESS: CDC Engine passed Transaction and Schema Evolution tests!")
        with open("/app/data/test_passed.txt", "w") as f:
            f.write("PASSED")
    else:
        sys.exit(1)

if __name__ == "__main__":
    verify()
