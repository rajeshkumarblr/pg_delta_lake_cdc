from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
import argparse

"""
Databricks Silver Materializer for pg_delta_lake_cdc

This script demonstrates how to incrementally process raw CDC events from 
PostgreSQL (Bronze) and materialize a clean, deduplicated "Silver" view.

Logic:
1. Read the stream from the Bronze Delta table (ADLS Gen2).
2. Filter for the latest record per 'id' using the _cdc_lsn and _cdc_timestamp.
3. Merge the changes into the Silver table.
"""

def materialize_cdc(bronze_path, silver_path, checkpoint_path):
    spark = SparkSession.builder.appName("CDC_Silver_Materializer").getOrCreate()

    # 1. Setup the Streaming Read from the Bronze table
    # Databricks Autoloader is recommended for high-volume, but readStream on Delta 
    # provides native incremental support.
    bronze_df = spark.readStream.format("delta").load(bronze_path)

    def process_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        # 2. Window Projection for Deduplication
        # We partition by 'id' and order by LSN (descending) to find the absolute latest event.
        window_spec = Window.partitionBy("id").orderBy(col("_cdc_lsn").desc(), col("_cdc_timestamp").desc())
        
        latest_records = batch_df.withColumn("rn", row_number().over(window_spec)) \
                                 .filter(col("rn") == 1) \
                                 .drop("rn")

        # 3. UPSERT logic into Silver Table
        # In a real Databricks environment, you would use 'MERGE INTO'
        latest_records.createOrReplaceTempView("updates")
        
        spark.sql(f"""
            MERGE INTO delta.`{silver_path}` AS target
            USING updates AS source
            ON target.id = source.id
            WHEN MATCHED AND source._cdc_op = 'DELETE' THEN
                DELETE
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED AND source._cdc_op != 'DELETE' THEN
                INSERT *
        """)

    # 4. Start the Stream
    query = bronze_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(availableNow=True) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Databricks Silver Materializer")
    parser.add_argument("--bronze_path", required=True, help="ADLS Path to Bronze Delta table")
    parser.add_argument("--silver_path", required=True, help="ADLS Path to Silver materialized table")
    parser.add_argument("--checkpoint_path", required=True, help="ADLS Path for streaming checkpoints")
    
    args = parser.parse_args()
    materialize_cdc(args.bronze_path, args.silver_path, args.checkpoint_path)
