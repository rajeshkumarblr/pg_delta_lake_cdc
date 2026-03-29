import pyspark
from pyspark.sql import SparkSession
from delta import *
import os

def main():
    # Initialize Spark session with Delta Lake extensions
    builder = SparkSession.builder.appName("DeltaLakeConsumer") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Path to the Delta table (mounted from Docker via test/data)
    delta_table_path = "test/data/stories"
    
    # If the requested path doesn't exist, try the default daemon path for validation
    if not os.path.exists(delta_table_path):
        default_path = "../data/stories"
        if os.path.exists(default_path):
            delta_table_path = default_path
            print(f"Using default path: {delta_table_path}")

    print(f"Loading Delta table from: {delta_table_path}")
    
    try:
        # Load the Delta table
        df = spark.read.format("delta").load(delta_table_path)

        # Register as a temporary SQL view
        df.createOrReplaceTempView("hn_articles")

        # Print Schema
        print("\n--- Delta Table Schema ---")
        df.printSchema()

        # Deduplicate the CDC log to get the latest state per ID, then find the top 5
        print("\n--- Top 5 Most Discussed Articles (Deduplicated Latest State) ---")
        latest_state_query = """
            SELECT id, title, score, descendants, _cdc_op, _cdc_timestamp
            FROM (
                SELECT *, 
                       row_number() OVER (PARTITION BY id ORDER BY _cdc_timestamp DESC) as rn
                FROM hn_articles
            ) latest_records
            WHERE rn = 1
            ORDER BY descendants DESC
            LIMIT 5
        """
        results = spark.sql(latest_state_query)
        results.show(truncate=False)

        # Also show the summary of Bronze vs Silver for context
        total_rows = df.count()
        distinct_rows = results.count()
        print(f"\nVerification: Processed {total_rows} Bronze events into Silver state.")

    except Exception as e:
        print(f"Error loading or querying Delta table: {e}")
        print("Ensure the C++ daemon has generated the Delta log in the output directory.")

    spark.stop()

if __name__ == "__main__":
    main()
