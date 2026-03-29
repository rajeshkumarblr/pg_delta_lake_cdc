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

        # Execute Spark SQL query: Top 5 most discussed articles
        print("\n--- Top 5 Most Discussed Articles ---")
        query = """
            SELECT id, title, score, descendants, _cdc_op, _cdc_timestamp
            FROM hn_articles
            ORDER BY descendants DESC
            LIMIT 5
        """
        results = spark.sql(query)
        results.show(truncate=False)

    except Exception as e:
        print(f"Error loading or querying Delta table: {e}")
        print("Ensure the C++ daemon has generated the Delta log in the output directory.")

    spark.stop()

if __name__ == "__main__":
    main()
