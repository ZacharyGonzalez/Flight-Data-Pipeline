import sys
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, when

# Configuration
LOCAL_INPUT_PATH = 'c:/Users/leojl/Documents/Training/test2/data/Combined_Flights_2022.csv'
LOCAL_OUTPUT_PATH = 'c:/Users/leojl/Documents/Training/test2/data'
S3_INPUT_PATH = 's3://flight-test-data-bucket/Transformed_Data'
S3_OUTPUT_PATH = 's3://flight-test-data-bucket/Airport_Summary'

# Set to True when running on EMR
RUNNING_ON_EMR = False

# Start Spark Session
spark = SparkSession.builder \
    .appName("Airport-Summary-Statistics") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

try:
    print("="*80, flush=True)
    print("AIRPORT SUMMARY STATISTICS", flush=True)
    print("="*80, flush=True)

    # Load data
    INPUT_PATH = S3_INPUT_PATH if RUNNING_ON_EMR else LOCAL_INPUT_PATH
    print(f"\nLoading flight data from: {INPUT_PATH}", flush=True)

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(INPUT_PATH)

    total_flights = df.count()
    print(f"Loaded {total_flights:,} flight records", flush=True)

    # Calculate summary statistics per airport per day
    print("\nCalculating airport daily summary statistics...", flush=True)

    airport_summary = df.groupBy("Origin", "FlightDate") \
        .agg(
            count("*").alias("total_flights"),
            _sum(when(col("DepDel15") == 1, 1).otherwise(0)).alias("total_delays"),
            _sum(when(col("Cancelled") == True, 1).otherwise(0)).alias("total_cancellations"),
            _sum(when(col("Diverted") == True, 1).otherwise(0)).alias("total_diversions")
        ) \
        .withColumnRenamed("Origin", "airport") \
        .withColumnRenamed("FlightDate", "date") \
        .orderBy(col("date").desc(), col("total_flights").desc())

    print("\nAirport Daily Summary Statistics (most recent days):", flush=True)
    airport_summary.show(20, truncate=False)

    # Calculate totals
    print("\nOverall Statistics:", flush=True)
    total_summary = df.agg(
        count("*").alias("total_flights"),
        _sum(when(col("DepDel15") == 1, 1).otherwise(0)).alias("total_delays"),
        _sum(when(col("Cancelled") == True, 1).otherwise(0)).alias("total_cancellations"),
        _sum(when(col("Diverted") == True, 1).otherwise(0)).alias("total_diversions")
    )
    total_summary.show(truncate=False)

    # Save results
    OUTPUT_PATH = S3_OUTPUT_PATH if RUNNING_ON_EMR else LOCAL_OUTPUT_PATH

    if RUNNING_ON_EMR:
        # On EMR: Write to S3
        print(f"\nWriting airport summary to: {OUTPUT_PATH}/airport_summary", flush=True)
        airport_summary.write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(f"{OUTPUT_PATH}/airport_summary")
    else:
        # Local: Write using Python CSV
        print(f"\nWriting airport summary to: {OUTPUT_PATH}/airport_summary_stats.csv", flush=True)

        # Collect data
        rows = airport_summary.collect()

        # Write CSV
        with open(f"{OUTPUT_PATH}/airport_summary_stats.csv", 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(airport_summary.columns)
            for row in rows:
                writer.writerow(row)

    print("\n" + "="*80, flush=True)
    print("SUCCESS: Airport summary complete!", flush=True)
    print("="*80, flush=True)

except Exception as e:
    print("\n" + "="*80, flush=True)
    print("ERROR OCCURRED", flush=True)
    print("="*80, flush=True)
    print(str(e), flush=True)
    import traceback
    traceback.print_exc()
    sys.exit(1)

finally:
    print("\nShutting down Spark session...", flush=True)
    spark.stop()
