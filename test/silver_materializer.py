import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number
from delta.tables import DeltaTable

# Path Definitions
# Bronze is read from the Docker volume (owned by root, but world-readable)
BRONZE_PATH = "./test/data/stories"
# Silver and Checkpoints are written to a local directory to avoid Permission Errors
SILVER_PATH = "./output/silver_articles"
CHECKPOINT_PATH = "./output/_checkpoints/silver_materializer"

def main():
    # Initialize SparkSession with Delta support
    # version 3.1.0 and necessary Delta SQL extensions
    spark = SparkSession.builder \
        .appName("SilverMaterializer") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.catalogImplementation", "hive") \
        .getOrCreate()

    # Ensure output directories exist for this simulation
    os.makedirs(os.path.dirname(CHECKPOINT_PATH), exist_ok=True)

    # Silver Table Bootstrapping
    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        print(f"Silver table not found. Bootstrapping at {SILVER_PATH}...")
        
        # Check if Bronze table exists to get schema
        if not DeltaTable.isDeltaTable(spark, BRONZE_PATH):
            raise Exception(f"Bronze table not found at {BRONZE_PATH}. Pipeline cannot start.")
            
        # Read schema from Bronze table
        bronze_df = spark.read.format("delta").load(BRONZE_PATH)
        
        # Filter out CDC metadata fields for the Silver state table
        # We want the pure state in Silver
        target_schema = bronze_df.drop("_cdc_op", "_cdc_timestamp").schema
        
        # Create empty Delta table at SILVER_PATH
        spark.createDataFrame([], target_schema) \
            .write \
            .format("delta") \
            .save(SILVER_PATH)
        print("Silver table bootstrapped successfully.")

    # Instantiate DeltaTable object pointing to SILVER_PATH
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)

    def process_cdc_microbatch(microbatch_df, batch_id):
        """
        Processes a micro-batch of CDC data:
        1. Deduplicates changes within the micro-batch (keep latest by timestamp).
        2. MERGEs deduplicated changes into the Silver table.
        """
        if microbatch_df.isEmpty():
            return

        print(f"Processing micro-batch {batch_id} with {microbatch_df.count()} records...")
        
        # 1. Deduplication: Window by 'id' and order by '_cdc_timestamp' DESC
        window_spec = Window.partitionBy("id").orderBy(col("_cdc_timestamp").desc())
        
        latest_changes_df = microbatch_df \
            .withColumn("row_num", row_number().over(window_spec)) \
            .filter(col("row_num") == 1) \
            .drop("row_num")

        # 2. ACID Merge Operation
        (silver_table.alias("target")
            .merge(
                latest_changes_df.alias("source"),
                "target.id = source.id"
            )
            .whenMatchedUpdateAll(
                condition = "source._cdc_op = 'UPDATE'"
            )
            .whenMatchedDelete(
                condition = "source._cdc_op = 'DELETE'"
            )
            .whenNotMatchedInsertAll(
                condition = "(source._cdc_op = 'INSERT' OR source._cdc_op = 'UPDATE')"
            )
            .execute()
        )
        print(f"Batch {batch_id} merged successfully.")

    # Streaming Execution
    print(f"Starting continuous stream from {BRONZE_PATH}...")
    
    # Read from the Bronze table as a stream
    bronze_stream = spark.readStream \
        .format("delta") \
        .load(BRONZE_PATH)

    # Write the stream using foreachBatch for complex MERGE logic
    query = bronze_stream.writeStream \
        .foreachBatch(process_cdc_microbatch) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .start()

    print("Stream is active. Awaiting termination...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
