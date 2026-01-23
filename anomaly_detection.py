import sys
import csv
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, count, sum as _sum, mean, stddev, when,
    lit, abs as _abs, datediff, current_date,
    percent_rank, expr, to_date
)
from pyspark.sql.types import DoubleType

# Configuration
S3_INPUT_PATH = 's3://flight-test-data-bucket/Transformed_Data'  # Or use raw data path
S3_OUTPUT_PATH = 's3://flight-test-data-bucket/Anomaly_Detection'
LOCAL_INPUT_PATH = 'c:/Users/leojl/Documents/Training/test2/data/Combined_Flights_2022.csv'
LOCAL_OUTPUT_PATH = 'c:/Users/leojl/Documents/Training/test2/data'

# Environment configuration
RUNNING_ON_EMR = False  # Set to True when running on EMR cluster

# Anomaly detection parameters
LOOKBACK_DAYS = 30
Z_THRESHOLD = 2.5
IQR_MULTIPLIER = 1.5
MIN_FLIGHTS_PER_DAY = 5  # Minimum flights to consider for anomaly detection

# Start Spark Session
spark = SparkSession.builder \
    .appName("Flight-Anomaly-Detection") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

try:
    print("="*80, flush=True)
    print("FLIGHT ANOMALY DETECTION SYSTEM", flush=True)
    print("="*80, flush=True)

    # ========================================================================
    # STEP 1: Load Data
    # ========================================================================
    print("\n[STEP 1] Loading flight data...", flush=True)

    # For local testing, use LOCAL_INPUT_PATH; for EMR, use S3_INPUT_PATH
    INPUT_PATH = LOCAL_INPUT_PATH  # Change to S3_INPUT_PATH for EMR

    raw_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(INPUT_PATH)

    print(f"Loaded {raw_df.count():,} flight records", flush=True)

    # Convert FlightDate to date type
    df = raw_df.withColumn("FlightDate", to_date(col("FlightDate")))

    print("Sample of loaded data:", flush=True)
    df.select("FlightDate", "Origin", "Dest", "Airline", "Cancelled", "Diverted",
              "DepDel15", "ArrDel15", "DepDelayMinutes", "ArrDelayMinutes").show(5, truncate=False)

    # ========================================================================
    # STEP 2: Calculate Airport-Level Daily Metrics
    # ========================================================================
    print("\n[STEP 2] Calculating airport-level daily metrics...", flush=True)

    # Process ORIGIN airports (departure metrics)
    origin_metrics = df.groupBy("Origin", "FlightDate") \
        .agg(
            count("*").alias("total_flights"),
            _sum(when(col("Cancelled") == True, 1).otherwise(0)).alias("cancellations"),
            _sum(when(col("Diverted") == True, 1).otherwise(0)).alias("diversions"),
            _sum(when(col("DepDel15") == 1, 1).otherwise(0)).alias("delays_15min"),
            mean(when(col("DepDelayMinutes").isNotNull(), col("DepDelayMinutes")).otherwise(0)).alias("avg_delay_minutes")
        ) \
        .withColumn("Airport", col("Origin")) \
        .withColumn("Direction", lit("departure")) \
        .drop("Origin")

    # Process DESTINATION airports (arrival metrics)
    dest_metrics = df.groupBy("Dest", "FlightDate") \
        .agg(
            count("*").alias("total_flights"),
            _sum(when(col("Cancelled") == True, 1).otherwise(0)).alias("cancellations"),
            _sum(when(col("Diverted") == True, 1).otherwise(0)).alias("diversions"),
            _sum(when(col("ArrDel15") == 1, 1).otherwise(0)).alias("delays_15min"),
            mean(when(col("ArrDelayMinutes").isNotNull(), col("ArrDelayMinutes")).otherwise(0)).alias("avg_delay_minutes")
        ) \
        .withColumn("Airport", col("Dest")) \
        .withColumn("Direction", lit("arrival")) \
        .drop("Dest")

    # Combine both
    airport_daily = origin_metrics.union(dest_metrics)

    # Calculate rates
    airport_daily = airport_daily \
        .withColumn("cancellation_rate", (col("cancellations") / col("total_flights") * 100)) \
        .withColumn("diversion_rate", (col("diversions") / col("total_flights") * 100)) \
        .withColumn("delay_rate_15min", (col("delays_15min") / col("total_flights") * 100)) \
        .filter(col("total_flights") >= MIN_FLIGHTS_PER_DAY)  # Filter low-volume days

    airport_daily.cache()
    print(f"Calculated metrics for {airport_daily.select('Airport').distinct().count()} airports", flush=True)
    print(f"Total airport-days: {airport_daily.count():,}", flush=True)

    print("\nSample of airport daily metrics:", flush=True)
    airport_daily.orderBy(col("FlightDate").desc()).show(10, truncate=False)

    # ========================================================================
    # STEP 3: Detect Anomalies using Z-Score Method
    # ========================================================================
    print("\n[STEP 3] Detecting anomalies using Z-Score method...", flush=True)
    print(f"Parameters: lookback={LOOKBACK_DAYS} days, z_threshold={Z_THRESHOLD}", flush=True)

    # Define metrics to analyze
    metrics_to_analyze = [
        "delay_rate_15min",
        "cancellation_rate",
        "diversion_rate",
        "avg_delay_minutes"
    ]

    anomalies_list = []

    for metric_col in metrics_to_analyze:
        print(f"\nAnalyzing: {metric_col}...", flush=True)

        # Create window for historical data (last N days per airport-direction)
        window_spec = Window.partitionBy("Airport", "Direction") \
            .orderBy("FlightDate") \
            .rowsBetween(-LOOKBACK_DAYS, -1)

        # Calculate rolling statistics
        with_stats = airport_daily \
            .withColumn(f"{metric_col}_mean", mean(col(metric_col)).over(window_spec)) \
            .withColumn(f"{metric_col}_stddev", stddev(col(metric_col)).over(window_spec))

        # Calculate Z-score
        z_score_df = with_stats \
            .withColumn(
                "z_score",
                when(col(f"{metric_col}_stddev") > 0,
                     (col(metric_col) - col(f"{metric_col}_mean")) / col(f"{metric_col}_stddev")
                ).otherwise(0)
            ) \
            .withColumn("abs_z_score", _abs(col("z_score")))

        # Detect anomalies (Z-score > threshold)
        anomalies_z = z_score_df \
            .filter(
                (col("abs_z_score") > Z_THRESHOLD) &
                (col(f"{metric_col}_mean").isNotNull())
            ) \
            .select(
                col("FlightDate").alias("date"),
                col("Airport").alias("airport"),
                col("Direction").alias("direction"),
                lit(metric_col).alias("metric"),
                col(metric_col).alias("value"),
                col(f"{metric_col}_mean").alias("historical_mean"),
                col(f"{metric_col}_stddev").alias("historical_stddev"),
                col("z_score"),
                col("total_flights"),
                lit("Z-score").alias("method")
            )

        anomaly_count = anomalies_z.count()
        print(f"  Found {anomaly_count} anomalies for {metric_col}", flush=True)

        if anomaly_count > 0:
            anomalies_list.append(anomalies_z)

    # ========================================================================
    # STEP 4: Detect Anomalies using IQR Method
    # ========================================================================
    print("\n[STEP 4] Detecting anomalies using IQR method...", flush=True)
    print(f"Parameters: lookback={LOOKBACK_DAYS} days, iqr_multiplier={IQR_MULTIPLIER}", flush=True)

    for metric_col in metrics_to_analyze:
        print(f"\nAnalyzing: {metric_col}...", flush=True)

        # Create window for historical data
        window_spec = Window.partitionBy("Airport", "Direction") \
            .orderBy("FlightDate") \
            .rowsBetween(-LOOKBACK_DAYS, -1)

        # Calculate Q1 and Q3 using percentile
        with_quartiles = airport_daily \
            .withColumn(
                f"{metric_col}_q1",
                expr(f"percentile_approx({metric_col}, 0.25)").over(window_spec)
            ) \
            .withColumn(
                f"{metric_col}_q3",
                expr(f"percentile_approx({metric_col}, 0.75)").over(window_spec)
            )

        # Calculate IQR and bounds
        iqr_df = with_quartiles \
            .withColumn(
                "iqr",
                col(f"{metric_col}_q3") - col(f"{metric_col}_q1")
            ) \
            .withColumn(
                "lower_bound",
                col(f"{metric_col}_q1") - (lit(IQR_MULTIPLIER) * col("iqr"))
            ) \
            .withColumn(
                "upper_bound",
                col(f"{metric_col}_q3") + (lit(IQR_MULTIPLIER) * col("iqr"))
            )

        # Detect anomalies (outside bounds)
        anomalies_iqr = iqr_df \
            .filter(
                (col(metric_col) < col("lower_bound")) |
                (col(metric_col) > col("upper_bound"))
            ) \
            .filter(col("iqr").isNotNull()) \
            .select(
                col("FlightDate").alias("date"),
                col("Airport").alias("airport"),
                col("Direction").alias("direction"),
                lit(metric_col).alias("metric"),
                col(metric_col).alias("value"),
                col(f"{metric_col}_q1").alias("q1"),
                col(f"{metric_col}_q3").alias("q3"),
                col("iqr"),
                col("lower_bound"),
                col("upper_bound"),
                col("total_flights"),
                lit("IQR").alias("method")
            )

        anomaly_count = anomalies_iqr.count()
        print(f"  Found {anomaly_count} anomalies for {metric_col}", flush=True)

        if anomaly_count > 0:
            anomalies_list.append(anomalies_iqr)

    # ========================================================================
    # STEP 5: Combine and Save Anomalies
    # ========================================================================
    print("\n[STEP 5] Combining and saving anomalies...", flush=True)

    if len(anomalies_list) > 0:
        # Union all anomaly dataframes
        # First, standardize schemas
        z_score_anomalies = [df for df in anomalies_list if "z_score" in df.columns]
        iqr_anomalies = [df for df in anomalies_list if "iqr" in df.columns]

        # Add missing columns to match schemas
        if z_score_anomalies:
            z_combined = z_score_anomalies[0]
            for df in z_score_anomalies[1:]:
                z_combined = z_combined.union(df)

            # Add IQR columns as null
            z_combined = z_combined \
                .withColumn("q1", lit(None).cast(DoubleType())) \
                .withColumn("q3", lit(None).cast(DoubleType())) \
                .withColumn("iqr", lit(None).cast(DoubleType())) \
                .withColumn("lower_bound", lit(None).cast(DoubleType())) \
                .withColumn("upper_bound", lit(None).cast(DoubleType()))

        if iqr_anomalies:
            iqr_combined = iqr_anomalies[0]
            for df in iqr_anomalies[1:]:
                iqr_combined = iqr_combined.union(df)

            # Add Z-score columns as null
            iqr_combined = iqr_combined \
                .withColumn("historical_mean", lit(None).cast(DoubleType())) \
                .withColumn("historical_stddev", lit(None).cast(DoubleType())) \
                .withColumn("z_score", lit(None).cast(DoubleType()))

        # Combine all
        all_anomalies = None
        if z_score_anomalies and iqr_anomalies:
            all_anomalies = z_combined.union(iqr_combined)
        elif z_score_anomalies:
            all_anomalies = z_combined
        elif iqr_anomalies:
            all_anomalies = iqr_combined

        # Sort by date (most recent first)
        all_anomalies = all_anomalies.orderBy(col("date").desc(), col("airport"))

        total_anomalies = all_anomalies.count()
        print(f"\nTotal anomalies detected: {total_anomalies:,}", flush=True)

        # Show summary
        print("\n" + "="*80, flush=True)
        print("ANOMALY DETECTION SUMMARY", flush=True)
        print("="*80, flush=True)

        print("\nAnomalies by metric:", flush=True)
        all_anomalies.groupBy("metric").count().orderBy(col("count").desc()).show(truncate=False)

        print("\nTop 10 airports with most anomalies:", flush=True)
        all_anomalies.groupBy("airport").count().orderBy(col("count").desc()).show(10, truncate=False)

        print("\nMost recent anomalies:", flush=True)
        all_anomalies.select(
            "date", "airport", "direction", "metric", "value",
            "method", "total_flights"
        ).show(20, truncate=False)

        # Save results
        OUTPUT_PATH = S3_OUTPUT_PATH if RUNNING_ON_EMR else LOCAL_OUTPUT_PATH

        # Calculate airport summary
        airport_summary = all_anomalies.groupBy("airport", "direction") \
            .agg(
                count("*").alias("total_anomalies"),
                _sum(when(col("metric") == "delay_rate_15min", 1).otherwise(0)).alias("delay_anomalies"),
                _sum(when(col("metric") == "cancellation_rate", 1).otherwise(0)).alias("cancellation_anomalies"),
                _sum(when(col("metric") == "diversion_rate", 1).otherwise(0)).alias("diversion_anomalies")
            ) \
            .orderBy(col("total_anomalies").desc())

        if RUNNING_ON_EMR:
            # On EMR: Use standard distributed write to S3
            print(f"\nWriting anomalies to: {OUTPUT_PATH}/anomalies", flush=True)
            all_anomalies.write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(f"{OUTPUT_PATH}/anomalies")

            print(f"Writing airport summary to: {OUTPUT_PATH}/airport_summary", flush=True)
            airport_summary.write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(f"{OUTPUT_PATH}/airport_summary")
        else:
            # Local Windows: Collect data and write manually (avoids Hadoop Windows native library issues)
            print(f"\nWriting anomalies to: {OUTPUT_PATH}/detected_anomalies.csv", flush=True)

            # Collect anomalies data
            anomalies_rows = all_anomalies.collect()

            # Write anomalies CSV
            with open(f"{OUTPUT_PATH}/detected_anomalies.csv", 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                # Write header
                writer.writerow(all_anomalies.columns)
                # Write data
                for row in anomalies_rows:
                    writer.writerow(row)

            print(f"Writing airport summary to: {OUTPUT_PATH}/airport_anomaly_summary.csv", flush=True)

            # Collect summary data
            summary_rows = airport_summary.collect()

            # Write summary CSV
            with open(f"{OUTPUT_PATH}/airport_anomaly_summary.csv", 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                # Write header
                writer.writerow(airport_summary.columns)
                # Write data
                for row in summary_rows:
                    writer.writerow(row)

        print("\n" + "="*80, flush=True)
        print("SUCCESS: Anomaly detection complete!", flush=True)
        print("="*80, flush=True)

    else:
        print("\nNo anomalies detected!", flush=True)

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
