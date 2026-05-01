from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, count, avg
import os
import sys
import time

def verify():
    print("Starting PySpark Verification...")
    
    # Wait for CDC engine to finish processing (simplistic approach: wait for file)
    expected_file = "/app/data/expected.txt"
    while not os.path.exists(expected_file):
        print("Waiting for workload generator to finish...")
        time.sleep(5)
    
    # Read expected values
    expected = {}
    with open(expected_file, "r") as f:
        for line in f:
            key, val = line.strip().split("=")
            expected[key] = float(val)

    # Initialize Spark with Delta Support
    spark = SparkSession.builder \
        .appName("CDC-Verification") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

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

    # Give CDC engine time to finish all epochs
    print("Waiting 30 seconds for final epochs to flush...")
    time.sleep(30)

    # Read Delta Table
    df = spark.read.format("delta").load(delta_path)
    
    # Filter for latest state using _cdc_timestamp or just use raw if we only care about count and logic
    # Actually, the Bronze table has multiple entries for same ID. 
    # We need to deduplicate to compare with PG's final state.
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, col
    
    window = Window.partitionBy("id").orderBy(col("_cdc_lsn").desc(), col("_cdc_timestamp").desc())
    latest_df = df.withColumn("rn", row_number().over(window)).filter("rn = 1").drop("rn", "_cdc_op", "_cdc_timestamp", "_cdc_lsn")
    
    stats = latest_df.select(
        count("*").alias("cnt"),
        _sum("score").alias("score_sum")
    ).collect()[0]

    actual_count = stats["cnt"]
    actual_sum = stats["score_sum"]

    print("-" * 40)
    print(f"VERIFICATION RESULTS")
    print(f"Expected Count: {expected['COUNT']}, Actual: {actual_count}")
    print(f"Expected Sum:   {expected['SUM']:.4f}, Actual: {actual_sum:.4f}")
    print("-" * 40)

    success = True
    if actual_count != int(expected['COUNT']):
        print("FAIL: Row count mismatch!")
        success = False
    
    # Allow small float epsilon
    if abs(float(actual_sum) - expected['SUM']) > 0.1:
        print("FAIL: Aggregate sum mismatch!")
        success = False

    if success:
        print("SUCCESS: CDC Engine converted all data correctly!")
        with open("/app/data/test_passed.txt", "w") as f:
            f.write("PASSED")
    else:
        sys.exit(1)

if __name__ == "__main__":
    verify()
