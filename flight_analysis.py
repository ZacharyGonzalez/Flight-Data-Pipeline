import sys
import csv
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, when, round as _round

# Configuration
LOCAL_INPUT_PATH = 'c:/Users/leojl/Documents/Training/test2/data/input'
LOCAL_OUTPUT_PATH = 'c:/Users/leojl/Documents/Training/test2/data/output'
S3_INPUT_PATH = 's3://flight-analysis-s3/data/input'
S3_OUTPUT_PATH = 's3://flight-analysis-s3/data/output'

# Set to True when running on EMR
RUNNING_ON_EMR = True

# Start Spark Session
spark = SparkSession.builder \
    .appName("Flight-Analysis-2019-2020") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

try:
    print("="*80, flush=True)
    print("FLIGHT ANALYSIS - 2019 & 2020", flush=True)
    print("="*80, flush=True)

    # Load data
    INPUT_PATH = S3_INPUT_PATH if RUNNING_ON_EMR else LOCAL_INPUT_PATH
    OUTPUT_PATH = S3_OUTPUT_PATH if RUNNING_ON_EMR else LOCAL_OUTPUT_PATH

    print(f"\nLoading flight data from: {INPUT_PATH}", flush=True)

    if RUNNING_ON_EMR:
        # On EMR: Use wildcard pattern for S3
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(f"{INPUT_PATH}/*.csv")
    else:
        # Local Windows: List CSV files manually to avoid Hadoop glob issues
        csv_files = [os.path.join(INPUT_PATH, f) for f in os.listdir(INPUT_PATH) if f.endswith('.csv')]
        print(f"Found {len(csv_files)} CSV files:", flush=True)
        for f in csv_files:
            print(f"  - {os.path.basename(f)}", flush=True)

        # Read all CSV files
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(csv_files)

    total_flights = df.count()
    print(f"Loaded {total_flights:,} flight records", flush=True)

    # Show sample data
    print("\nSample of loaded data:", flush=True)
    df.select("FlightDate", "Origin", "Cancelled", "Diverted", "DepDel15").show(5, truncate=False)

    # ========================================================================
    # PART 1: Airport Daily Statistics
    # ========================================================================
    print("\n" + "="*80, flush=True)
    print("PART 1: Daily Statistics by Airport", flush=True)
    print("="*80, flush=True)

    airport_daily = df.groupBy("Origin", "FlightDate") \
        .agg(
            count("*").alias("total_flights"),
            _sum(when(col("DepDel15") == 1, 1).otherwise(0)).alias("total_delays"),
            _sum(when(col("Cancelled") == True, 1).otherwise(0)).alias("total_cancellations"),
            _sum(when(col("Diverted") == True, 1).otherwise(0)).alias("total_diversions")
        ) \
        .withColumnRenamed("Origin", "airport") \
        .withColumnRenamed("FlightDate", "date") \
        .withColumn("delay_percentage", _round((col("total_delays") / col("total_flights") * 100), 2)) \
        .withColumn("cancellation_percentage", _round((col("total_cancellations") / col("total_flights") * 100), 2)) \
        .withColumn("diversion_percentage", _round((col("total_diversions") / col("total_flights") * 100), 2)) \
        .orderBy(col("date").desc(), col("airport"))

    print("\nAirport Daily Statistics (most recent days):", flush=True)
    airport_daily.show(20, truncate=False)

    total_rows = airport_daily.count()
    print(f"\nTotal airport-days: {total_rows:,}", flush=True)

    # ========================================================================
    # SAVE RESULTS
    # ========================================================================
    print("\n" + "="*80, flush=True)
    print("Saving Results", flush=True)
    print("="*80, flush=True)

    if RUNNING_ON_EMR:
        # On EMR: Write to S3
        print(f"\nWriting airport daily stats to: {OUTPUT_PATH}/airport_daily_stats", flush=True)
        airport_daily.write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(f"{OUTPUT_PATH}/airport_daily_stats")
    else:
        # Local: Write using Python CSV
        print(f"\nWriting airport daily stats to: {OUTPUT_PATH}/airport_daily_stats.csv", flush=True)
        rows = airport_daily.collect()
        with open(f"{OUTPUT_PATH}/airport_daily_stats.csv", 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(airport_daily.columns)
            for row in rows:
                writer.writerow(row)

    print("\n" + "="*80, flush=True)
    print("SUCCESS: Flight analysis complete!", flush=True)
    print("="*80, flush=True)
    print(f"\nFile saved to: {OUTPUT_PATH}/airport_daily_stats.csv", flush=True)

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
