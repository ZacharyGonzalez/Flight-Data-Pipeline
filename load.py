import sys
import json
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# ============================================================================
# CONFIGURATION
# ============================================================================

# AWS Secrets Manager
SECRET_ARN = "arn:aws:secretsmanager:us-east-2:580106620815:secret:Snowflake_conn-DmoAjJ"
AWS_REGION = "us-east-2"

# Snowflake Configuration
SNOWFLAKE_DATABASE = "FLIGHTS_DB"
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_TABLE = "AIRPORT_DAILY_STATS"

# Data Source Configuration
S3_INPUT_PATH = "s3://flight-test-data-bucket/output/airport_daily_stats.csv"
LOCAL_INPUT_PATH = "c:/Users/leojl/Documents/Training/test2/data/output/airport_daily_stats.csv"

# Load Mode Options: "overwrite", "append", "error", "ignore"
LOAD_MODE = "append"

# Environment
RUNNING_ON_EMR = True  # Set to False for local testing

# Schema definition for airport_daily_stats
SCHEMA = StructType([
    StructField("airport", StringType(), False),
    StructField("date", DateType(), False),
    StructField("total_flights", IntegerType(), True),
    StructField("total_delays", IntegerType(), True),
    StructField("total_cancellations", IntegerType(), True),
    StructField("total_diversions", IntegerType(), True),
    StructField("delay_percentage", DoubleType(), True),
    StructField("cancellation_percentage", DoubleType(), True),
    StructField("diversion_percentage", DoubleType(), True)
])

# ============================================================================
# SPARK SESSION
# ============================================================================
spark = SparkSession.builder \
    .appName("Snowflake-Data-Loader") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

try:
    print("=" * 80, flush=True)
    print("SNOWFLAKE DATA LOADER - AIRPORT DAILY STATS", flush=True)
    print("=" * 80, flush=True)

    # ========================================================================
    # STEP 1: Load data from S3 or local
    # ========================================================================
    INPUT_PATH = S3_INPUT_PATH if RUNNING_ON_EMR else LOCAL_INPUT_PATH

    print(f"\n[STEP 1] Loading data from: {INPUT_PATH}", flush=True)

    df = spark.read \
        .option("header", "true") \
        .schema(SCHEMA) \
        .csv(INPUT_PATH)

    record_count = df.count()
    print(f"Loaded {record_count:,} records", flush=True)

    print("\nSample of data to be loaded:", flush=True)
    df.show(10, truncate=False)

    # ========================================================================
    # STEP 2: Load Snowflake credentials from Secrets Manager
    # ========================================================================
    print(f"\n[STEP 2] Loading Snowflake credentials from Secrets Manager...", flush=True)

    client = boto3.client("secretsmanager", region_name=AWS_REGION)
    secret_value = client.get_secret_value(SecretId=SECRET_ARN)
    secret = json.loads(secret_value["SecretString"])

    print("✓ Credentials loaded successfully", flush=True)

    # ========================================================================
    # STEP 3: Configure Snowflake connection
    # ========================================================================
    sfOptions = {
        "sfURL": secret["sfURL"],
        "sfUser": secret["sfUser"],
        "sfPassword": secret["sfPassword"],
        "sfDatabase": SNOWFLAKE_DATABASE,
        "sfSchema": SNOWFLAKE_SCHEMA,
        "sfWarehouse": SNOWFLAKE_WAREHOUSE
    }

    # ========================================================================
    # STEP 4: Write to Snowflake
    # ========================================================================
    print(f"\n[STEP 3] Writing to Snowflake table: {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}", flush=True)
    print(f"Load mode: {LOAD_MODE}", flush=True)

    df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**sfOptions) \
        .option("dbtable", SNOWFLAKE_TABLE) \
        .mode(LOAD_MODE) \
        .save()

    print("\n" + "=" * 80, flush=True)
    print("SUCCESS: Data loaded to Snowflake!", flush=True)
    print("=" * 80, flush=True)
    print(f"\nTable: {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}", flush=True)
    print(f"Records loaded: {record_count:,}", flush=True)
    print(f"Load mode: {LOAD_MODE}", flush=True)

    # ========================================================================
    # STEP 5: Verify load (optional)
    # ========================================================================
    print(f"\n[STEP 4] Verifying data in Snowflake...", flush=True)

    # Read back from Snowflake to verify
    verify_df = spark.read \
        .format("net.snowflake.spark.snowflake") \
        .options(**sfOptions) \
        .option("dbtable", SNOWFLAKE_TABLE) \
        .load()

    verify_count = verify_df.count()
    print(f"Verified {verify_count:,} records in Snowflake table", flush=True)

    if LOAD_MODE == "append":
        print(f"✓ Table now contains {verify_count:,} total records", flush=True)
    elif verify_count == record_count:
        print("✓ Record counts match!", flush=True)
    else:
        print(f"⚠ Warning: Record count mismatch! Loaded: {record_count}, Found: {verify_count}", flush=True)

    print("\nSample from Snowflake table:", flush=True)
    verify_df.orderBy("date", "airport").show(10, truncate=False)

except Exception as e:
    print("\n" + "=" * 80, flush=True)
    print("ERROR OCCURRED", flush=True)
    print("=" * 80, flush=True)
    print(str(e), flush=True)
    import traceback
    traceback.print_exc()
    sys.exit(1)

finally:
    print("\nShutting down Spark session...", flush=True)
    spark.stop()


# ============================================================================
# USAGE INSTRUCTIONS
# ============================================================================
"""
SETUP INSTRUCTIONS:

1. Configure settings at the top of this file:
   - SECRET_ARN: Your AWS Secrets Manager ARN
   - SNOWFLAKE_TABLE: Target table name (default: AIRPORT_DAILY_STATS)
   - S3_INPUT_PATH: S3 path to your CSV file
   - LOAD_MODE: "append" or "overwrite"

2. Create Snowflake table (if it doesn't exist):

   CREATE TABLE AIRPORT_DAILY_STATS (
       airport VARCHAR(3) NOT NULL,
       date DATE NOT NULL,
       total_flights INT,
       total_delays INT,
       total_cancellations INT,
       total_diversions INT,
       delay_percentage DECIMAL(5,2),
       cancellation_percentage DECIMAL(5,2),
       diversion_percentage DECIMAL(5,2),
       PRIMARY KEY (airport, date)
   );

3. Run on EMR:
   - Set RUNNING_ON_EMR = True
   - Upload script to S3
   - Add EMR step:

     aws emr add-steps --cluster-id j-XXXXX --steps \
       Type=Spark,Name="Load to Snowflake",ActionOnFailure=CONTINUE,\
       Args=[--jars,/usr/share/aws/redshift/jdbc/RedshiftJDBC.jar,\
       --packages,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3,\
       s3://your-bucket/load.py]

4. For local testing:
   - Set RUNNING_ON_EMR = False
   - Ensure LOCAL_INPUT_PATH points to your CSV
   - Run: spark-submit --packages net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3 src/load.py

LOAD MODES:
- "append": Add data to existing table (best for incremental loads)
- "overwrite": Replace all data in table (use for full refresh)
- "error": Fail if table already exists
- "ignore": Skip if table already exists
"""