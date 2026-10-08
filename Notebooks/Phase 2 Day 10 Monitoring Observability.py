# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # 📊 Data Engineering Training — Phase 2 Day 10  
# MAGIC ## 🔍 Monitoring, Observability & Reliability  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - Logs & Metrics in Databricks  
# MAGIC - Debugging Failures  
# MAGIC - SLA / SLO Concepts  
# MAGIC - Pipeline Observability  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### ⚙️ Platform: Databricks (Serverless + Unity Catalog + Workflows)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Objective:
# MAGIC Understand how to monitor data pipelines, debug failures, track metrics, and implement basic SLA-driven observability in Databricks.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Engineering Constraints:
# MAGIC - ✅ Databricks Serverless Compute
# MAGIC - ✅ Unity Catalog Volumes for data access
# MAGIC - ✅ PySpark DataFrame API only
# MAGIC - ❌ NO RDDs
# MAGIC - ❌ NO cache() / persist()
# MAGIC - ❌ NO /tmp or local storage

# COMMAND ----------

# DBTITLE 1,Section 1: Observability Fundamentals
# MAGIC %md
# MAGIC # 🔭 Section 1: Observability Fundamentals
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine you're baking cookies. **Observability** is like being able to:
# MAGIC - See the oven temperature (metrics)
# MAGIC - Read the recipe steps you've completed (logs)
# MAGIC - Know exactly when things went wrong (traces)
# MAGIC
# MAGIC Without observability, if cookies burn, you don't know why. With it, you can see "oven was too hot at 3:45 PM"!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Observability** is the ability to understand the internal state of a system by examining its external outputs. In data engineering, this means:
# MAGIC
# MAGIC ### Why Observability Matters:
# MAGIC 1. **Proactive Issue Detection**: Identify problems before users report them
# MAGIC 2. **Faster Root Cause Analysis**: Reduce MTTR (Mean Time To Resolution)
# MAGIC 3. **Data Quality Assurance**: Ensure pipeline outputs meet expectations
# MAGIC 4. **SLA Compliance**: Track and meet service level agreements
# MAGIC 5. **Cost Optimization**: Identify inefficient operations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Three Pillars of Observability:
# MAGIC
# MAGIC ### 1️⃣ **Logs**
# MAGIC - **What**: Discrete events recorded during execution
# MAGIC - **Use**: Debugging, audit trails, understanding flow
# MAGIC - **Example**: "Started processing batch_2024_04_21 at 10:30:00"
# MAGIC
# MAGIC ### 2️⃣ **Metrics**
# MAGIC - **What**: Numerical measurements over time
# MAGIC - **Use**: Performance tracking, trend analysis, alerting
# MAGIC - **Example**: "Processed 1.2M records in 45 seconds"
# MAGIC
# MAGIC ### 3️⃣ **Traces** (Conceptual in Databricks)
# MAGIC - **What**: End-to-end journey of a request through the system
# MAGIC - **Use**: Understanding dependencies, latency analysis
# MAGIC - **Example**: "Request flowed through: API → Bronze → Silver → Gold"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚨 Without Observability:
# MAGIC - ❌ Blind execution: "Did my job complete?"
# MAGIC - ❌ Reactive debugging: "Why did it fail 3 hours ago?"
# MAGIC - ❌ Unknown data quality: "Is this data correct?"
# MAGIC - ❌ No performance insights: "Why is it so slow?"
# MAGIC
# MAGIC ## ✅ With Observability:
# MAGIC - ✅ Transparency: Real-time visibility into pipeline health
# MAGIC - ✅ Confidence: Data quality validated at each step
# MAGIC - ✅ Efficiency: Quick troubleshooting and optimization
# MAGIC - ✅ Reliability: Proactive alerting and SLA tracking

# COMMAND ----------

# DBTITLE 1,Section 2: Logs in Databricks
# MAGIC %md
# MAGIC # 📜 Section 2: Logs in Databricks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC
# MAGIC Logs are like a diary for your data pipeline. Every important thing that happens gets written down:
# MAGIC - "Started reading file"
# MAGIC - "Found 1000 rows"
# MAGIC - "Oops, something broke!"
# MAGIC
# MAGIC Later, you can read this diary to understand what happened.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Types of Logs in Databricks:
# MAGIC
# MAGIC ### 1️⃣ **Driver Logs**
# MAGIC - Main program execution logs
# MAGIC - Captured from the driver node
# MAGIC - Contains high-level job flow
# MAGIC
# MAGIC ### 2️⃣ **Executor Logs**
# MAGIC - Task-level execution logs
# MAGIC - Parallel processing details
# MAGIC - Useful for debugging distributed issues
# MAGIC
# MAGIC ### 3️⃣ **Job Run Logs**
# MAGIC - Workflow execution logs
# MAGIC - Available in Databricks Jobs UI
# MAGIC - Includes stdout, stderr, and log4j outputs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Logging Best Practices:
# MAGIC
# MAGIC 1. **Log at Key Milestones**: Start/end of stages
# MAGIC 2. **Include Context**: Timestamps, record counts, identifiers
# MAGIC 3. **Use Structured Logging**: JSON format for parsing
# MAGIC 4. **Log Levels**: INFO, WARNING, ERROR, DEBUG
# MAGIC 5. **Avoid Sensitive Data**: No PII in logs

# COMMAND ----------

# DBTITLE 1,Demo: Basic Logging in Pipeline
# Basic Logging Example
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('DataPipeline')

# Pipeline Step 1: Start
logger.info("=" * 60)
logger.info("Pipeline Execution Started")
logger.info(f"Execution Time: {datetime.now()}")
logger.info("=" * 60)

# Pipeline Step 2: Data Generation (simulating read)
try:
    logger.info("Step 1: Generating sample data...")
    
    data = [
        ("2024-04-21", "user_001", 150.00, "completed"),
        ("2024-04-21", "user_002", 200.00, "completed"),
        ("2024-04-21", "user_003", 175.00, "pending"),
        ("2024-04-21", "user_004", 300.00, "completed"),
        ("2024-04-21", "user_005", 125.00, "failed")
    ]
    
    df = spark.createDataFrame(data, ["date", "user_id", "amount", "status"])
    row_count = df.count()
    
    logger.info(f"✅ Data loaded successfully: {row_count} records")
    
except Exception as e:
    logger.error(f"❌ Failed to load data: {str(e)}")
    raise

# Pipeline Step 3: Transformation
try:
    logger.info("Step 2: Applying transformations...")
    
    df_filtered = df.filter(df.status == "completed")
    completed_count = df_filtered.count()
    
    logger.info(f"✅ Transformation complete: {completed_count} completed transactions")
    
except Exception as e:
    logger.error(f"❌ Transformation failed: {str(e)}")
    raise

# Pipeline Step 4: Display results
logger.info("Step 3: Displaying results...")
display(df_filtered)

logger.info("=" * 60)
logger.info("Pipeline Execution Completed Successfully")
logger.info("=" * 60)

# COMMAND ----------

# DBTITLE 1,Demo: Structured Logging (JSON Format)
# Structured Logging with JSON format
import json
from datetime import datetime

def log_structured(level, message, **kwargs):
    """Create structured log entry in JSON format"""
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "level": level,
        "message": message,
        "metadata": kwargs
    }
    print(json.dumps(log_entry, indent=2))

# Example: Pipeline with structured logging
log_structured(
    "INFO",
    "Pipeline started",
    pipeline_name="observability_demo",
    environment="production",
    user="trraveendra"
)

# Simulate data processing
log_structured(
    "INFO",
    "Data ingestion started",
    source="unity_catalog_volume",
    expected_records=5000
)

# Simulate success
log_structured(
    "INFO",
    "Data ingestion completed",
    records_ingested=5000,
    duration_seconds=12.5,
    data_quality_score=0.98
)

# Simulate warning
log_structured(
    "WARNING",
    "High null percentage detected",
    column="email",
    null_percentage=15.2,
    threshold=10.0
)

# Simulate error
log_structured(
    "ERROR",
    "Schema validation failed",
    expected_columns=["id", "name", "email"],
    actual_columns=["id", "name"],
    missing_columns=["email"]
)

# COMMAND ----------

# DBTITLE 1,Section 3: Metrics & Monitoring
# MAGIC %md
# MAGIC # 📊 Section 3: Metrics & Monitoring
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC
# MAGIC Metrics are like keeping score in a game:
# MAGIC - How many points did you score? (record count)
# MAGIC - How fast did you run? (runtime)
# MAGIC - Did you win or lose? (success/failure)
# MAGIC
# MAGIC By tracking these numbers, you know if you're getting better or worse!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Key Pipeline Metrics:
# MAGIC
# MAGIC ### 1️⃣ **Performance Metrics**
# MAGIC - **Execution Time**: How long did the pipeline take?
# MAGIC - **Throughput**: Records processed per second
# MAGIC - **Resource Usage**: CPU, memory, shuffle data
# MAGIC
# MAGIC ### 2️⃣ **Data Metrics**
# MAGIC - **Record Counts**: Input vs output records
# MAGIC - **Data Volume**: Size in MB/GB
# MAGIC - **Data Quality**: Null counts, validation failures
# MAGIC
# MAGIC ### 3️⃣ **Reliability Metrics**
# MAGIC - **Success Rate**: % of successful runs
# MAGIC - **Failure Rate**: % of failed runs
# MAGIC - **Retry Count**: How many retries needed
# MAGIC
# MAGIC ### 4️⃣ **Business Metrics**
# MAGIC - **Data Freshness**: How old is the data?
# MAGIC - **SLA Compliance**: Meeting time requirements?
# MAGIC - **Cost per Run**: Resource consumption cost
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Metrics Collection Strategy:
# MAGIC
# MAGIC ```
# MAGIC Pipeline Stage → Capture Metrics → Log/Store → Visualize → Alert
# MAGIC ```
# MAGIC
# MAGIC 1. **Capture**: Collect metrics during execution
# MAGIC 2. **Log**: Write to logs or metrics store
# MAGIC 3. **Visualize**: Create dashboards
# MAGIC 4. **Alert**: Trigger notifications on anomalies

# COMMAND ----------

# DBTITLE 1,Demo: Capturing Pipeline Metrics
# Metrics Collection Demo
import time
from datetime import datetime
from pyspark.sql import functions as F

# Initialize metrics dictionary
metrics = {}

# Start timing
start_time = time.time()
metrics['pipeline_start_time'] = datetime.now().isoformat()

print("=" * 70)
print("PIPELINE METRICS COLLECTION DEMO")
print("=" * 70)

# Step 1: Generate sample data
print("\n➡️ Step 1: Data Ingestion")
data = [
    ("2024-04-21", "product_A", 100, 10.50),
    ("2024-04-21", "product_B", 150, 20.00),
    ("2024-04-21", "product_C", 200, 15.75),
    ("2024-04-21", "product_A", 50, 10.50),
    ("2024-04-21", "product_D", None, 25.00),  # Null quantity
    ("2024-04-21", "product_B", 175, None),     # Null price
]

df = spark.createDataFrame(data, ["date", "product", "quantity", "price"])

# Capture ingestion metrics
metrics['input_record_count'] = df.count()
metrics['input_columns'] = len(df.columns)
print(f"  ✅ Records ingested: {metrics['input_record_count']}")
print(f"  ✅ Columns: {metrics['input_columns']}")

# Step 2: Data Quality Checks
print("\n➡️ Step 2: Data Quality Assessment")
metrics['null_quantity_count'] = df.filter(F.col("quantity").isNull()).count()
metrics['null_price_count'] = df.filter(F.col("price").isNull()).count()
metrics['data_quality_score'] = 1 - ((metrics['null_quantity_count'] + metrics['null_price_count']) / (metrics['input_record_count'] * 2))

print(f"  ⚠️ Null quantities: {metrics['null_quantity_count']}")
print(f"  ⚠️ Null prices: {metrics['null_price_count']}")
print(f"  🎯 Data quality score: {metrics['data_quality_score']:.2%}")

# Step 3: Transformation
print("\n➡️ Step 3: Data Transformation")
transform_start = time.time()

# Clean data and calculate revenue
df_clean = df.filter(
    F.col("quantity").isNotNull() & F.col("price").isNotNull()
).withColumn(
    "revenue", F.col("quantity") * F.col("price")
)

transform_duration = time.time() - transform_start

metrics['output_record_count'] = df_clean.count()
metrics['records_dropped'] = metrics['input_record_count'] - metrics['output_record_count']
metrics['transform_duration_seconds'] = round(transform_duration, 3)

print(f"  ✅ Records after cleaning: {metrics['output_record_count']}")
print(f"  🗑️ Records dropped: {metrics['records_dropped']}")
print(f"  ⏱️ Transform duration: {metrics['transform_duration_seconds']}s")

# Step 4: Aggregation Metrics
print("\n➡️ Step 4: Business Metrics")
total_revenue = df_clean.agg(F.sum("revenue")).collect()[0][0]
avg_quantity = df_clean.agg(F.avg("quantity")).collect()[0][0]

metrics['total_revenue'] = round(total_revenue, 2)
metrics['avg_quantity'] = round(avg_quantity, 2)

print(f"  💰 Total revenue: ${metrics['total_revenue']:,.2f}")
print(f"  📈 Average quantity: {metrics['avg_quantity']}")

# Step 5: Pipeline completion
end_time = time.time()
metrics['pipeline_end_time'] = datetime.now().isoformat()
metrics['total_duration_seconds'] = round(end_time - start_time, 3)
metrics['records_per_second'] = round(metrics['output_record_count'] / metrics['total_duration_seconds'], 2) if metrics['total_duration_seconds'] > 0 else 0
metrics['pipeline_status'] = 'SUCCESS'

print("\n" + "=" * 70)
print("PIPELINE EXECUTION SUMMARY")
print("=" * 70)
for key, value in metrics.items():
    print(f"{key:.<40} {value}")
print("=" * 70)

# Display cleaned data
print("\n📄 Cleaned Data:")
display(df_clean)

# COMMAND ----------

# DBTITLE 1,Demo: Metrics Dashboard (Simulated)
# Create a metrics summary table for dashboard visualization
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Convert metrics dictionary to DataFrame for visualization
metrics_data = [
    ("Data Quality", "Quality Score", metrics['data_quality_score'] * 100, "%"),
    ("Data Quality", "Null Records", float(metrics['null_quantity_count'] + metrics['null_price_count']), "count"),
    ("Performance", "Total Duration", metrics['total_duration_seconds'], "seconds"),
    ("Performance", "Records/Second", metrics['records_per_second'], "records/s"),
    ("Data Volume", "Input Records", float(metrics['input_record_count']), "count"),
    ("Data Volume", "Output Records", float(metrics['output_record_count']), "count"),
    ("Data Volume", "Records Dropped", float(metrics['records_dropped']), "count"),
    ("Business", "Total Revenue", metrics['total_revenue'], "USD"),
    ("Business", "Avg Quantity", metrics['avg_quantity'], "units"),
]

metrics_df = spark.createDataFrame(
    metrics_data,
    ["category", "metric_name", "value", "unit"]
)

print("📊 METRICS DASHBOARD")
print("=" * 70)
display(metrics_df)

# Create status indicator
print("\n🚦 Pipeline Health Status:")
if metrics['data_quality_score'] >= 0.95:
    print("  ✅ HEALTHY - Data quality excellent")
elif metrics['data_quality_score'] >= 0.80:
    print("  ⚠️ WARNING - Data quality acceptable but needs attention")
else:
    print("  ❌ CRITICAL - Data quality below threshold")

if metrics['total_duration_seconds'] < 10:
    print("  ✅ HEALTHY - Performance within SLA")
else:
    print("  ⚠️ WARNING - Performance slower than expected")

# COMMAND ----------

# DBTITLE 1,Section 4: Debugging Failures
# MAGIC %md
# MAGIC # 🔧 Section 4: Debugging Failures
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC
# MAGIC Debugging is like being a detective:
# MAGIC - Something broke 💔
# MAGIC - You look for clues 🔍
# MAGIC - You find out why 💡
# MAGIC - You fix it ✅
# MAGIC
# MAGIC With good observability, you have lots of clues to help you!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Common Failure Types in Data Pipelines:
# MAGIC
# MAGIC ### 1️⃣ **Data Issues**
# MAGIC - Missing or corrupt files
# MAGIC - Unexpected null values
# MAGIC - Data type mismatches
# MAGIC - Duplicate records
# MAGIC
# MAGIC ### 2️⃣ **Schema Issues**
# MAGIC - Schema evolution (column added/removed)
# MAGIC - Type changes (string → int)
# MAGIC - Missing required columns
# MAGIC
# MAGIC ### 3️⃣ **Runtime Errors**
# MAGIC - Out of memory
# MAGIC - Timeout errors
# MAGIC - Network failures
# MAGIC - Permission issues
# MAGIC
# MAGIC ### 4️⃣ **Logic Errors**
# MAGIC - Incorrect transformations
# MAGIC - Wrong filters applied
# MAGIC - Business logic bugs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔍 Root Cause Analysis (RCA) Process:
# MAGIC
# MAGIC 1. **Identify**: What failed?
# MAGIC 2. **Locate**: Where did it fail? (which stage)
# MAGIC 3. **Analyze**: Why did it fail? (logs, metrics)
# MAGIC 4. **Reproduce**: Can you reproduce it?
# MAGIC 5. **Fix**: Implement solution
# MAGIC 6. **Prevent**: Add monitoring to catch early
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Best Practices for Error Handling:
# MAGIC
# MAGIC 1. **Use Try/Except Blocks**: Catch and handle exceptions gracefully
# MAGIC 2. **Log Errors with Context**: Include stack trace and relevant data
# MAGIC 3. **Fail Fast**: Don't continue with bad data
# MAGIC 4. **Retry Logic**: For transient failures
# MAGIC 5. **Dead Letter Queue**: Store failed records for analysis

# COMMAND ----------

# DBTITLE 1,Demo: Error Handling Patterns
# Error Handling Demo
from pyspark.sql import functions as F
import logging

logger = logging.getLogger('ErrorHandling')

print("=" * 70)
print("ERROR HANDLING PATTERNS DEMO")
print("=" * 70)

# Pattern 1: Try/Except for Data Loading
print("\n🔵 Pattern 1: Safe Data Loading with Error Handling")
print("-" * 70)

try:
    # Simulate reading from a non-existent path
    # In real scenario, this would be from Unity Catalog Volume
    print("Attempting to load data...")
    
    # Create sample data instead (in real case, this would fail)
    data = [
        ("2024-04-21", "TX-001", 100),
        ("2024-04-21", "TX-002", 200),
    ]
    df = spark.createDataFrame(data, ["date", "transaction_id", "amount"])
    
    print("✅ Data loaded successfully")
    
except Exception as e:
    logger.error(f"❌ Data loading failed: {str(e)}")
    logger.error(f"Error type: {type(e).__name__}")
    # In production: send alert, write to dead letter queue
    raise

# Pattern 2: Schema Validation
print("\n🔵 Pattern 2: Schema Validation")
print("-" * 70)

expected_columns = ["date", "transaction_id", "amount"]
actual_columns = df.columns

try:
    missing_columns = set(expected_columns) - set(actual_columns)
    extra_columns = set(actual_columns) - set(expected_columns)
    
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    if extra_columns:
        logger.warning(f"Unexpected columns found: {extra_columns}")
    
    print("✅ Schema validation passed")
    print(f"   Expected: {expected_columns}")
    print(f"   Actual:   {actual_columns}")
    
except ValueError as e:
    logger.error(f"❌ Schema validation failed: {str(e)}")
    raise

# Pattern 3: Data Quality Checks with Error Handling
print("\n🔵 Pattern 3: Data Quality Checks")
print("-" * 70)

try:
    # Add some null values for testing
    data_with_nulls = [
        ("2024-04-21", "TX-001", 100),
        ("2024-04-21", "TX-002", None),  # Null amount
        ("2024-04-21", None, 300),       # Null transaction_id
    ]
    df_test = spark.createDataFrame(data_with_nulls, ["date", "transaction_id", "amount"])
    
    # Check for nulls
    null_count = df_test.filter(F.col("amount").isNull()).count()
    null_percentage = (null_count / df_test.count()) * 100
    
    # Define quality threshold
    MAX_NULL_PERCENTAGE = 5.0
    
    if null_percentage > MAX_NULL_PERCENTAGE:
        raise ValueError(
            f"Data quality check failed: {null_percentage:.2f}% null values "
            f"exceeds threshold of {MAX_NULL_PERCENTAGE}%"
        )
    
    print(f"✅ Data quality check passed")
    print(f"   Null percentage: {null_percentage:.2f}%")
    print(f"   Threshold: {MAX_NULL_PERCENTAGE}%")
    
except ValueError as e:
    logger.error(f"❌ {str(e)}")
    # In production: don't raise, instead write to quarantine table
    print(f"   ⚠️ Quality issue detected but continuing...")

# Pattern 4: Division by Zero Handling
print("\n🔵 Pattern 4: Safe Mathematical Operations")
print("-" * 70)

try:
    data_calc = [
        ("product_A", 1000, 10),
        ("product_B", 2000, 0),  # Will cause division by zero
        ("product_C", 1500, 5),
    ]
    df_calc = spark.createDataFrame(data_calc, ["product", "revenue", "units"])
    
    # Safe division using when/otherwise
    df_calc = df_calc.withColumn(
        "price_per_unit",
        F.when(F.col("units") == 0, None)
         .otherwise(F.col("revenue") / F.col("units"))
    )
    
    print("✅ Safe calculation completed")
    display(df_calc)
    
except Exception as e:
    logger.error(f"❌ Calculation failed: {str(e)}")
    raise

print("\n" + "=" * 70)
print("✅ All error handling patterns demonstrated successfully")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Demo: Debugging Failed Pipeline
# Debugging Failed Pipeline - Step by Step
import traceback
from datetime import datetime

print("=" * 70)
print("DEBUGGING FAILED PIPELINE - CASE STUDY")
print("=" * 70)

# Scenario: Pipeline fails intermittently
# Let's debug it step by step

def run_pipeline_with_debugging():
    """Simulated pipeline with debugging instrumentation"""
    
    debug_info = {
        "start_time": datetime.now().isoformat(),
        "stages_completed": [],
        "errors": []
    }
    
    try:
        # Stage 1: Data Ingestion
        print("\n➡️ Stage 1: Data Ingestion")
        data = [
            ("2024-04-21", "user_001", 100, "valid"),
            ("2024-04-21", "user_002", -50, "invalid"),  # Negative amount
            ("2024-04-21", "user_003", 200, "valid"),
            ("2024-04-21", "user_004", 999999, "suspicious"),  # Outlier
        ]
        df = spark.createDataFrame(data, ["date", "user_id", "amount", "flag"])
        
        debug_info["stages_completed"].append("ingestion")
        debug_info["ingestion_record_count"] = df.count()
        print(f"  ✅ Ingested {df.count()} records")
        
    except Exception as e:
        debug_info["errors"].append({
            "stage": "ingestion",
            "error": str(e),
            "traceback": traceback.format_exc()
        })
        print(f"  ❌ Stage 1 failed: {str(e)}")
        return debug_info
    
    try:
        # Stage 2: Data Validation
        print("\n➡️ Stage 2: Data Validation")
        
        # Check for negative amounts
        negative_count = df.filter(F.col("amount") < 0).count()
        if negative_count > 0:
            debug_info["warnings"] = debug_info.get("warnings", [])
            debug_info["warnings"].append(f"Found {negative_count} negative amounts")
            print(f"  ⚠️ WARNING: {negative_count} negative amounts detected")
        
        # Check for outliers
        outlier_count = df.filter(F.col("amount") > 10000).count()
        if outlier_count > 0:
            debug_info["warnings"] = debug_info.get("warnings", [])
            debug_info["warnings"].append(f"Found {outlier_count} outliers")
            print(f"  ⚠️ WARNING: {outlier_count} outliers detected")
        
        debug_info["stages_completed"].append("validation")
        print(f"  ✅ Validation completed")
        
    except Exception as e:
        debug_info["errors"].append({
            "stage": "validation",
            "error": str(e),
            "traceback": traceback.format_exc()
        })
        print(f"  ❌ Stage 2 failed: {str(e)}")
        return debug_info
    
    try:
        # Stage 3: Data Transformation
        print("\n➡️ Stage 3: Data Transformation")
        
        # Filter out invalid data
        df_clean = df.filter(
            (F.col("amount") > 0) & 
            (F.col("amount") < 10000)
        )
        
        clean_count = df_clean.count()
        dropped_count = df.count() - clean_count
        
        debug_info["stages_completed"].append("transformation")
        debug_info["clean_record_count"] = clean_count
        debug_info["dropped_record_count"] = dropped_count
        
        print(f"  ✅ Transformation completed")
        print(f"     Clean records: {clean_count}")
        print(f"     Dropped records: {dropped_count}")
        
    except Exception as e:
        debug_info["errors"].append({
            "stage": "transformation",
            "error": str(e),
            "traceback": traceback.format_exc()
        })
        print(f"  ❌ Stage 3 failed: {str(e)}")
        return debug_info
    
    # Pipeline completed
    debug_info["end_time"] = datetime.now().isoformat()
    debug_info["status"] = "SUCCESS" if not debug_info["errors"] else "FAILED"
    
    return debug_info

# Run the pipeline
debug_result = run_pipeline_with_debugging()

# Print debug report
print("\n" + "=" * 70)
print("DEBUG REPORT")
print("=" * 70)
print(f"Status: {debug_result.get('status', 'UNKNOWN')}")
print(f"Stages Completed: {debug_result.get('stages_completed', [])}")
print(f"Start Time: {debug_result.get('start_time', 'N/A')}")
print(f"End Time: {debug_result.get('end_time', 'N/A')}")

if 'warnings' in debug_result:
    print(f"\n⚠️ Warnings:")
    for warning in debug_result['warnings']:
        print(f"  - {warning}")

if debug_result.get('errors'):
    print(f"\n❌ Errors:")
    for error in debug_result['errors']:
        print(f"  Stage: {error['stage']}")
        print(f"  Error: {error['error']}")
        print(f"  Traceback: {error['traceback'][:200]}...")

print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Section 5: SLA & SLO Concepts
# MAGIC %md
# MAGIC # 📝 Section 5: SLA & SLO Concepts
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC
# MAGIC SLA is like a promise:
# MAGIC - "I promise to deliver your pizza in 30 minutes or less"
# MAGIC - "I promise to finish homework before dinner"
# MAGIC - "I promise my pipeline will finish by 8 AM"
# MAGIC
# MAGIC If you break the promise too many times, people get upset!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ SLA vs SLO vs SLI:
# MAGIC
# MAGIC ### 📜 **SLA (Service Level Agreement)**
# MAGIC - **Definition**: A contract or commitment to meet specific service standards
# MAGIC - **Example**: "Data refresh pipeline must complete by 8:00 AM daily"
# MAGIC - **Business Impact**: If violated, could trigger penalties or loss of trust
# MAGIC
# MAGIC ### 🎯 **SLO (Service Level Objective)**
# MAGIC - **Definition**: Specific measurable targets within an SLA
# MAGIC - **Example**: 
# MAGIC   - Pipeline success rate ≥ 99.5%
# MAGIC   - Processing time ≤ 30 minutes
# MAGIC   - Data freshness ≤ 1 hour
# MAGIC
# MAGIC ### 📊 **SLI (Service Level Indicator)**
# MAGIC - **Definition**: Actual measurements used to track SLO compliance
# MAGIC - **Example**: 
# MAGIC   - Current success rate: 99.7%
# MAGIC   - Average processing time: 25 minutes
# MAGIC   - Current data age: 45 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📈 Relationship:
# MAGIC
# MAGIC ```
# MAGIC SLI (What you measure) → SLO (What you target) → SLA (What you promise)
# MAGIC ```
# MAGIC
# MAGIC **Example:**
# MAGIC - **SLI**: Pipeline completed in 35 minutes (measured)
# MAGIC - **SLO**: Pipeline should complete in ≤ 30 minutes (target)
# MAGIC - **SLA**: Data available by 8 AM (promise)
# MAGIC - **Result**: SLO missed, but SLA met (if job started at 7:20 AM)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Common Data Pipeline SLAs:
# MAGIC
# MAGIC ### 1️⃣ **Latency SLAs**
# MAGIC - "Pipeline completes within X minutes"
# MAGIC - "Data available within Y hours of source update"
# MAGIC
# MAGIC ### 2️⃣ **Availability SLAs**
# MAGIC - "Pipeline succeeds 99.9% of the time"
# MAGIC - "Data quality checks pass 99.5% of runs"
# MAGIC
# MAGIC ### 3️⃣ **Freshness SLAs**
# MAGIC - "Data not older than 2 hours"
# MAGIC - "Daily refresh completed by 8 AM"
# MAGIC
# MAGIC ### 4️⃣ **Correctness SLAs**
# MAGIC - "Data accuracy ≥ 99.9%"
# MAGIC - "Zero critical data quality failures"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔧 Mapping to Databricks:
# MAGIC
# MAGIC ### **SLA Enforcement in Databricks Jobs:**
# MAGIC
# MAGIC | SLA Requirement | Databricks Feature |
# MAGIC |----------------|-------------------|
# MAGIC | Complete by specific time | Job scheduling with time-based triggers |
# MAGIC | Retry on failure | Max retries configuration (0-3 retries) |
# MAGIC | Timeout limits | Job timeout settings |
# MAGIC | Success notifications | Email/webhook alerts on success |
# MAGIC | Failure alerts | Email/webhook alerts on failure |
# MAGIC | Performance tracking | Job run history and metrics |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ SLA Best Practices:
# MAGIC
# MAGIC 1. **Set Realistic Targets**: Based on historical data
# MAGIC 2. **Monitor Continuously**: Track SLI metrics in real-time
# MAGIC 3. **Alert Proactively**: Warn before SLA breach
# MAGIC 4. **Build Error Budgets**: Allow for some failures (e.g., 99.9% = 0.1% failure budget)
# MAGIC 5. **Review Regularly**: Adjust SLOs based on business needs

# COMMAND ----------

# DBTITLE 1,Demo: SLA Tracking Simulation
# SLA Tracking Demo
import time
from datetime import datetime, timedelta
from pyspark.sql import functions as F

print("=" * 70)
print("SLA TRACKING SIMULATION")
print("=" * 70)

# Define SLAs
SLA_CONFIG = {
    "pipeline_name": "daily_sales_aggregation",
    "completion_time_sla_minutes": 30,
    "success_rate_sla_percentage": 99.5,
    "data_freshness_sla_hours": 2,
    "data_quality_sla_percentage": 99.0
}

print("\n📝 SLA Configuration:")
for key, value in SLA_CONFIG.items():
    print(f"  {key}: {value}")

# Simulate pipeline execution
print("\n" + "=" * 70)
print("➡️ Executing Pipeline...")
print("=" * 70)

start_time = time.time()
pipeline_start = datetime.now()

# Simulate processing
data = [
    ("2024-04-21", "region_1", 1000),
    ("2024-04-21", "region_2", 1500),
    ("2024-04-21", "region_3", 2000),
]
df = spark.createDataFrame(data, ["date", "region", "sales"])

total_revenue = df.agg(F.sum("sales")).collect()[0][0]

end_time = time.time()
pipeline_end = datetime.now()
execution_time_minutes = (end_time - start_time) / 60

# Calculate SLIs (Service Level Indicators)
SLIs = {
    "execution_time_minutes": round(execution_time_minutes, 2),
    "pipeline_status": "SUCCESS",
    "records_processed": df.count(),
    "data_quality_score_percentage": 99.8,
    "data_age_hours": 0.5  # Simulated
}

print("\n✅ Pipeline Execution Complete")
print("\n📊 Service Level Indicators (SLIs):")
for key, value in SLIs.items():
    print(f"  {key}: {value}")

# SLA Compliance Check
print("\n" + "=" * 70)
print("SLA COMPLIANCE REPORT")
print("=" * 70)

compliance_results = []

# Check 1: Execution Time
time_compliant = SLIs["execution_time_minutes"] <= SLA_CONFIG["completion_time_sla_minutes"]
compliance_results.append({
    "sla_metric": "Execution Time",
    "slo_target": f"<= {SLA_CONFIG['completion_time_sla_minutes']} minutes",
    "actual_value": f"{SLIs['execution_time_minutes']} minutes",
    "status": "✅ PASS" if time_compliant else "❌ FAIL"
})

# Check 2: Data Quality
quality_compliant = SLIs["data_quality_score_percentage"] >= SLA_CONFIG["data_quality_sla_percentage"]
compliance_results.append({
    "sla_metric": "Data Quality",
    "slo_target": f">= {SLA_CONFIG['data_quality_sla_percentage']}%",
    "actual_value": f"{SLIs['data_quality_score_percentage']}%",
    "status": "✅ PASS" if quality_compliant else "❌ FAIL"
})

# Check 3: Data Freshness
freshness_compliant = SLIs["data_age_hours"] <= SLA_CONFIG["data_freshness_sla_hours"]
compliance_results.append({
    "sla_metric": "Data Freshness",
    "slo_target": f"<= {SLA_CONFIG['data_freshness_sla_hours']} hours",
    "actual_value": f"{SLIs['data_age_hours']} hours",
    "status": "✅ PASS" if freshness_compliant else "❌ FAIL"
})

# Convert to DataFrame for visualization
compliance_df = spark.createDataFrame(compliance_results)

print("\n")
display(compliance_df)

# Overall SLA Status
all_compliant = all(r["status"] == "✅ PASS" for r in compliance_results)

print("\n" + "=" * 70)
if all_compliant:
    print("✅ ✅ ✅ ALL SLAs MET - SYSTEM HEALTHY ✅ ✅ ✅")
else:
    print("❌ ❌ ❌ SLA VIOLATION DETECTED - ALERT TRIGGERED ❌ ❌ ❌")
    print("\n🚨 Recommended Actions:")
    print("  1. Notify stakeholders")
    print("  2. Investigate root cause")
    print("  3. Implement corrective measures")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Section 6: Hands-on Observability Pipeline
# MAGIC %md
# MAGIC # 🚀 Section 6: Hands-on Observability Pipeline
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Objective:
# MAGIC
# MAGIC Build a complete data pipeline with **observability embedded at every stage**:
# MAGIC
# MAGIC 1. 📊 **Ingest data** (simulated from Unity Catalog Volume)
# MAGIC 2. 🔄 **Transform data** with business logic
# MAGIC 3. 📊 **Capture metrics** at each stage
# MAGIC 4. 📜 **Log execution steps** with context
# MAGIC 5. ✅ **Validate SLA compliance**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Pipeline Architecture:
# MAGIC
# MAGIC ```
# MAGIC 📂 Source Data
# MAGIC     ↓
# MAGIC 🔵 Stage 1: Ingestion + Logging + Metrics
# MAGIC     ↓
# MAGIC 🔵 Stage 2: Validation + Quality Checks + Metrics
# MAGIC     ↓
# MAGIC 🔵 Stage 3: Transformation + Metrics
# MAGIC     ↓
# MAGIC 🔵 Stage 4: Aggregation + Business Metrics
# MAGIC     ↓
# MAGIC ✅ SLA Compliance Check + Final Report
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Key Observability Features:
# MAGIC
# MAGIC * **Structured Logging**: JSON format for all stages
# MAGIC * **Comprehensive Metrics**: Timing, counts, quality scores
# MAGIC * **Error Handling**: Try/except with detailed error capture
# MAGIC * **SLA Tracking**: Automated compliance validation
# MAGIC * **Health Status**: Real-time pipeline health indicator

# COMMAND ----------

# DBTITLE 1,Complete Observable Pipeline
# COMPLETE OBSERVABLE DATA PIPELINE
import time
import json
import logging
from datetime import datetime
from pyspark.sql import functions as F

print("="*80)
print(" OBSERVABLE DATA PIPELINE - PRODUCTION READY ")
print("="*80)

# Initialize observability components
logger = logging.getLogger('ObservablePipeline')
logging.basicConfig(level=logging.INFO)

pipeline_metrics = {
    "pipeline_name": "sales_analytics_pipeline",
    "execution_id": f"exec_{int(time.time())}",
    "start_time": datetime.now().isoformat(),
    "stages": {}
}

def log_stage(stage_name, message, level="INFO", **kwargs):
    """Structured logging function"""
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "execution_id": pipeline_metrics["execution_id"],
        "stage": stage_name,
        "level": level,
        "message": message,
        "metadata": kwargs
    }
    print(json.dumps(log_entry, indent=2))

# ============================================================================
# STAGE 1: DATA INGESTION
# ============================================================================
try:
    stage_name = "ingestion"
    stage_start = time.time()
    
    log_stage(stage_name, "Starting data ingestion", source="simulated_volume")
    
    # Simulate reading from Unity Catalog Volume
    # In production: spark.read.format("delta").load("/Volumes/catalog/schema/volume/path")
    raw_data = [
        ("2024-04-21", "product_A", "region_1", 100, 25.50, "completed"),
        ("2024-04-21", "product_B", "region_1", 150, 30.00, "completed"),
        ("2024-04-21", "product_A", "region_2", 200, 25.50, "completed"),
        ("2024-04-21", "product_C", "region_2", None, 40.00, "pending"),      # Null quantity
        ("2024-04-21", "product_B", "region_3", 175, None, "completed"),      # Null price
        ("2024-04-21", "product_D", "region_3", 250, 35.00, "completed"),
        ("2024-04-21", "product_A", "region_1", -50, 25.50, "failed"),       # Negative quantity
        ("2024-04-21", "product_E", "region_2", 300, 45.00, "completed"),
    ]
    
    df_raw = spark.createDataFrame(
        raw_data, 
        ["transaction_date", "product", "region", "quantity", "price", "status"]
    )
    
    # Capture metrics
    stage_metrics = {
        "duration_seconds": round(time.time() - stage_start, 3),
        "records_ingested": df_raw.count(),
        "columns": len(df_raw.columns),
        "status": "SUCCESS"
    }
    pipeline_metrics["stages"][stage_name] = stage_metrics
    
    log_stage(stage_name, "Data ingestion completed", level="INFO", **stage_metrics)
    
except Exception as e:
    log_stage(stage_name, "Data ingestion failed", level="ERROR", error=str(e))
    raise

# ============================================================================
# STAGE 2: DATA QUALITY VALIDATION
# ============================================================================
try:
    stage_name = "validation"
    stage_start = time.time()
    
    log_stage(stage_name, "Starting data quality validation")
    
    # Quality checks
    null_quantity = df_raw.filter(F.col("quantity").isNull()).count()
    null_price = df_raw.filter(F.col("price").isNull()).count()
    negative_quantity = df_raw.filter(F.col("quantity") < 0).count()
    
    total_records = df_raw.count()
    quality_issues = null_quantity + null_price + negative_quantity
    quality_score = 1 - (quality_issues / (total_records * 3))  # 3 checks
    
    # Capture metrics
    stage_metrics = {
        "duration_seconds": round(time.time() - stage_start, 3),
        "null_quantity_count": null_quantity,
        "null_price_count": null_price,
        "negative_quantity_count": negative_quantity,
        "data_quality_score": round(quality_score, 4),
        "status": "SUCCESS"
    }
    pipeline_metrics["stages"][stage_name] = stage_metrics
    
    if quality_score < 0.90:
        log_stage(stage_name, "Data quality below threshold", level="WARNING", **stage_metrics)
    else:
        log_stage(stage_name, "Data quality validation passed", level="INFO", **stage_metrics)
    
except Exception as e:
    log_stage(stage_name, "Validation failed", level="ERROR", error=str(e))
    raise

# ============================================================================
# STAGE 3: DATA TRANSFORMATION
# ============================================================================
try:
    stage_name = "transformation"
    stage_start = time.time()
    
    log_stage(stage_name, "Starting data transformation")
    
    # Clean data: remove nulls, negatives, and failed transactions
    df_clean = df_raw.filter(
        (F.col("quantity").isNotNull()) &
        (F.col("price").isNotNull()) &
        (F.col("quantity") > 0) &
        (F.col("status") == "completed")
    )
    
    # Add calculated columns
    df_transformed = df_clean.withColumn(
        "revenue", F.col("quantity") * F.col("price")
    ).withColumn(
        "processing_timestamp", F.lit(datetime.now())
    )
    
    # Capture metrics
    records_before = df_raw.count()
    records_after = df_transformed.count()
    records_dropped = records_before - records_after
    
    stage_metrics = {
        "duration_seconds": round(time.time() - stage_start, 3),
        "records_input": records_before,
        "records_output": records_after,
        "records_dropped": records_dropped,
        "drop_rate_percentage": round((records_dropped / records_before) * 100, 2),
        "status": "SUCCESS"
    }
    pipeline_metrics["stages"][stage_name] = stage_metrics
    
    log_stage(stage_name, "Data transformation completed", level="INFO", **stage_metrics)
    
except Exception as e:
    log_stage(stage_name, "Transformation failed", level="ERROR", error=str(e))
    raise

# ============================================================================
# STAGE 4: BUSINESS AGGREGATION
# ============================================================================
try:
    stage_name = "aggregation"
    stage_start = time.time()
    
    log_stage(stage_name, "Starting business aggregation")
    
    # Aggregate by product and region
    df_agg = df_transformed.groupBy("product", "region").agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("revenue").alias("total_revenue"),
        F.count("*").alias("transaction_count"),
        F.avg("price").alias("avg_price")
    )
    
    # Calculate overall metrics
    total_revenue = df_transformed.agg(F.sum("revenue")).collect()[0][0]
    total_quantity = df_transformed.agg(F.sum("quantity")).collect()[0][0]
    
    # Capture metrics
    stage_metrics = {
        "duration_seconds": round(time.time() - stage_start, 3),
        "aggregated_groups": df_agg.count(),
        "total_revenue": round(total_revenue, 2),
        "total_quantity": int(total_quantity),
        "avg_revenue_per_transaction": round(total_revenue / df_transformed.count(), 2),
        "status": "SUCCESS"
    }
    pipeline_metrics["stages"][stage_name] = stage_metrics
    
    log_stage(stage_name, "Business aggregation completed", level="INFO", **stage_metrics)
    
    print("\n📊 AGGREGATED RESULTS:")
    display(df_agg)
    
except Exception as e:
    log_stage(stage_name, "Aggregation failed", level="ERROR", error=str(e))
    raise

# ============================================================================
# FINAL: PIPELINE SUMMARY & SLA CHECK
# ============================================================================
pipeline_metrics["end_time"] = datetime.now().isoformat()
total_duration = sum(stage["duration_seconds"] for stage in pipeline_metrics["stages"].values())
pipeline_metrics["total_duration_seconds"] = round(total_duration, 3)
pipeline_metrics["overall_status"] = "SUCCESS"

print("\n" + "="*80)
print(" PIPELINE EXECUTION SUMMARY ")
print("="*80)
print(json.dumps(pipeline_metrics, indent=2))
print("="*80)

# SLA Compliance Check
SLA_THRESHOLD_SECONDS = 30
sla_met = pipeline_metrics["total_duration_seconds"] < SLA_THRESHOLD_SECONDS

print(f"\n🎯 SLA Status: {'✅ MET' if sla_met else '❌ VIOLATED'}")
print(f"   Execution Time: {pipeline_metrics['total_duration_seconds']}s")
print(f"   SLA Threshold: {SLA_THRESHOLD_SECONDS}s")
print("\n" + "="*80)

# COMMAND ----------

# DBTITLE 1,Section 7: Failure Handling & Alerts (Conceptual)
# MAGIC %md
# MAGIC # 🚨 Section 7: Failure Handling & Alerts
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC
# MAGIC Imagine you're baking cookies and something goes wrong:
# MAGIC - **Retry**: Try again if oven was just warming up
# MAGIC - **Alert**: Tell mom if cookies burn
# MAGIC - **Dead Letter**: Put burnt cookies in a special jar to check later
# MAGIC
# MAGIC Pipelines work the same way!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔁 Retry Strategies:
# MAGIC
# MAGIC ### 1️⃣ **Immediate Retry**
# MAGIC - Retry right away
# MAGIC - Good for: Transient network issues
# MAGIC - **Databricks**: Max retries = 0-3 in Job settings
# MAGIC
# MAGIC ### 2️⃣ **Exponential Backoff**
# MAGIC - Wait longer between each retry
# MAGIC - Retry 1: Wait 1 minute
# MAGIC - Retry 2: Wait 2 minutes
# MAGIC - Retry 3: Wait 4 minutes
# MAGIC
# MAGIC ### 3️⃣ **Circuit Breaker**
# MAGIC - Stop retrying if system is clearly down
# MAGIC - Prevents overwhelming failing systems
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔔 Alert Mechanisms:
# MAGIC
# MAGIC ### 📬 **Email Alerts**
# MAGIC ```python
# MAGIC # Databricks Jobs supports email alerts
# MAGIC On Success: send email to team@company.com
# MAGIC On Failure: send email to oncall@company.com
# MAGIC ```
# MAGIC
# MAGIC ### 🔗 **Webhook Alerts**
# MAGIC ```python
# MAGIC # Integration with:
# MAGIC - Slack
# MAGIC - PagerDuty
# MAGIC - Microsoft Teams
# MAGIC - Custom HTTP endpoints
# MAGIC ```
# MAGIC
# MAGIC ### 📊 **Monitoring Dashboard Alerts**
# MAGIC - Integrate with Datadog, New Relic, Grafana
# MAGIC - Real-time metrics visualization
# MAGIC - Threshold-based alerting
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📬 Dead Letter Queue (DLQ) Pattern:
# MAGIC
# MAGIC ### **Concept:**
# MAGIC When records fail processing, don’t lose them—store them for later analysis.
# MAGIC
# MAGIC ```
# MAGIC Input Records
# MAGIC     ↓
# MAGIC Processing Logic
# MAGIC     ↓
# MAGIC   Success? 
# MAGIC   /     \
# MAGIC ✅ Yes    ❌ No
# MAGIC   ↓        ↓
# MAGIC Output   Dead Letter Table
# MAGIC ```
# MAGIC
# MAGIC ### **Implementation:**
# MAGIC ```python
# MAGIC # Pseudo-code
# MAGIC try:
# MAGIC     process_record(record)
# MAGIC     write_to_output(record)
# MAGIC except Exception as e:
# MAGIC     write_to_dead_letter_queue(record, error=str(e))
# MAGIC     log_error(record, e)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Databricks Jobs: Retry Configuration
# MAGIC
# MAGIC ### **Job Settings:**
# MAGIC | Setting | Description | Recommendation |
# MAGIC |---------|-------------|----------------|
# MAGIC | **Max Retries** | Number of automatic retries (0-3) | Start with 2 |
# MAGIC | **Retry on Timeout** | Retry if job times out | Enable |
# MAGIC | **Timeout** | Max execution time | Set based on historical data |
# MAGIC | **Email Notifications** | Alert on success/failure | Configure both |
# MAGIC | **Webhook Notifications** | External integrations | For critical pipelines |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Best Practices:
# MAGIC
# MAGIC 1. **Idempotent Pipelines**: Safe to rerun multiple times
# MAGIC 2. **Graceful Degradation**: Partial success better than total failure
# MAGIC 3. **Alert Fatigue Prevention**: Don't alert on every minor issue
# MAGIC 4. **Runbook Documentation**: Clear steps for on-call engineers
# MAGIC 5. **Post-Mortem Analysis**: Learn from failures

# COMMAND ----------

# DBTITLE 1,Section 8: End-to-End Observable Pipeline Design
# MAGIC %md
# MAGIC # 🏛️ Section 8: End-to-End Observable Pipeline Design
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Enterprise-Grade Observable Pipeline Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌───────────────────────────────────────────────────────────┐
# MAGIC │                   DATA PIPELINE FLOW                          │
# MAGIC └───────────────────────────────────────────────────────────┘
# MAGIC           ↓
# MAGIC    📂 Source Systems (APIs, Databases, Files)
# MAGIC           ↓
# MAGIC   [📝 LOG] Data source identified
# MAGIC   [📊 METRIC] Source record count
# MAGIC           ↓
# MAGIC    ─────────────────────────────────
# MAGIC    BRONZE LAYER (Raw Ingestion)
# MAGIC    ─────────────────────────────────
# MAGIC           ↓
# MAGIC   [📝 LOG] Ingestion started
# MAGIC   [📊 METRIC] Records ingested: X
# MAGIC   [📊 METRIC] Ingestion time: Ys
# MAGIC   [✅ CHECK] Schema validation
# MAGIC   [⚠️  ALERT] If ingestion fails
# MAGIC           ↓
# MAGIC    ─────────────────────────────────
# MAGIC    SILVER LAYER (Cleaned & Validated)
# MAGIC    ─────────────────────────────────
# MAGIC           ↓
# MAGIC   [📝 LOG] Transformation started
# MAGIC   [📊 METRIC] Data quality score: 99.5%
# MAGIC   [📊 METRIC] Records dropped: Z
# MAGIC   [📊 METRIC] Transform time: Ws
# MAGIC   [✅ CHECK] Business rules applied
# MAGIC   [⚠️  ALERT] If quality < threshold
# MAGIC           ↓
# MAGIC    ─────────────────────────────────
# MAGIC    GOLD LAYER (Business Aggregates)
# MAGIC    ─────────────────────────────────
# MAGIC           ↓
# MAGIC   [📝 LOG] Aggregation started
# MAGIC   [📊 METRIC] Business KPIs calculated
# MAGIC   [📊 METRIC] Total revenue, counts, etc.
# MAGIC   [✅ CHECK] SLA compliance
# MAGIC   [✅ SUCCESS] Pipeline completed
# MAGIC           ↓
# MAGIC    📊 Monitoring Dashboard
# MAGIC           ↓
# MAGIC    🔔 Alerts & Notifications
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Observability Components:
# MAGIC
# MAGIC ### 1️⃣ **Logging Strategy**
# MAGIC * Structured JSON logs at each stage
# MAGIC * Include: timestamp, execution_id, stage, status
# MAGIC * Log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL
# MAGIC
# MAGIC ### 2️⃣ **Metrics Collection**
# MAGIC * Performance: duration, throughput
# MAGIC * Data volume: record counts, data size
# MAGIC * Quality: null %, validation pass rate
# MAGIC * Business: revenue, transactions, users
# MAGIC
# MAGIC ### 3️⃣ **Health Checks**
# MAGIC * Schema validation
# MAGIC * Data freshness checks
# MAGIC * SLA compliance monitoring
# MAGIC * Dependency health (upstream systems)
# MAGIC
# MAGIC ### 4️⃣ **Alerting Strategy**
# MAGIC * Critical: Pipeline failure, SLA breach
# MAGIC * Warning: Quality degradation, slow performance
# MAGIC * Info: Successful completion, milestones
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Real-World Production Scenario:
# MAGIC
# MAGIC ### **Use Case: E-Commerce Sales Analytics**
# MAGIC
# MAGIC **Requirements:**
# MAGIC * Ingest sales data every hour
# MAGIC * SLA: Data available within 15 minutes
# MAGIC * Quality: 99.9% accuracy required
# MAGIC * Alerts: Notify on failure or SLA breach
# MAGIC
# MAGIC **Observable Pipeline Design:**
# MAGIC
# MAGIC 1. **Ingestion** (Bronze)
# MAGIC    * Read from Kafka/API
# MAGIC    * Log: source, batch ID, record count
# MAGIC    * Metric: ingestion time, data volume
# MAGIC
# MAGIC 2. **Validation** (Silver)
# MAGIC    * Schema checks
# MAGIC    * Null validation
# MAGIC    * Log: quality score, issues found
# MAGIC    * Alert: if quality < 99%
# MAGIC
# MAGIC 3. **Transformation** (Silver)
# MAGIC    * Currency conversion
# MAGIC    * Deduplication
# MAGIC    * Log: transform steps, records dropped
# MAGIC    * Metric: processing time
# MAGIC
# MAGIC 4. **Aggregation** (Gold)
# MAGIC    * Daily/hourly revenue
# MAGIC    * Top products by region
# MAGIC    * Log: business metrics calculated
# MAGIC    * Alert: if SLA missed
# MAGIC
# MAGIC 5. **Monitoring**
# MAGIC    * Real-time dashboard
# MAGIC    * SLA tracking
# MAGIC    * Historical trend analysis
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Production Checklist:
# MAGIC
# MAGIC * ✅ Structured logging at every stage
# MAGIC * ✅ Comprehensive metrics collection
# MAGIC * ✅ Automated data quality checks
# MAGIC * ✅ SLA monitoring and alerts
# MAGIC * ✅ Error handling with retry logic
# MAGIC * ✅ Dead letter queue for failed records
# MAGIC * ✅ Monitoring dashboard
# MAGIC * ✅ On-call runbook documentation
# MAGIC * ✅ Post-mortem process for incidents

# COMMAND ----------

# DBTITLE 1,Section 9: Genie Code Agent Usage
# MAGIC %md
# MAGIC # 🤖 Section 9: Genie Code Agent Usage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Using Genie Code for Observability
# MAGIC
# MAGIC Genie Code can help you implement monitoring and observability patterns quickly. Here are example prompts:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Example Prompts:
# MAGIC
# MAGIC ### 🔵 **1. Add Observability to Existing Pipeline**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Add observability to my pipeline:
# MAGIC - Add structured logging at each stage
# MAGIC - Capture metrics: record counts, execution time, data quality score
# MAGIC - Include try/except error handling
# MAGIC - Log to JSON format
# MAGIC ```
# MAGIC
# MAGIC **What Genie Does:**
# MAGIC * Wraps each pipeline stage with logging
# MAGIC * Adds timing metrics
# MAGIC * Implements error handling
# MAGIC * Structures logs in JSON format
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔵 **2. Generate Logging Framework**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Create a reusable logging framework for my data pipelines that:
# MAGIC - Uses Python logging module
# MAGIC - Outputs structured JSON logs
# MAGIC - Includes execution ID, timestamp, stage name
# MAGIC - Has helper functions for different log levels
# MAGIC ```
# MAGIC
# MAGIC **What Genie Does:**
# MAGIC * Creates logging utility functions
# MAGIC * Implements JSON formatter
# MAGIC * Adds metadata enrichment
# MAGIC * Provides easy-to-use logging methods
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔵 **3. Implement SLA Monitoring**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Add SLA monitoring to my pipeline:
# MAGIC - Track execution time against 30-minute SLA
# MAGIC - Monitor data quality score (threshold: 99%)
# MAGIC - Check data freshness (max 2 hours old)
# MAGIC - Alert if any SLA is violated
# MAGIC ```
# MAGIC
# MAGIC **What Genie Does:**
# MAGIC * Adds SLA configuration
# MAGIC * Implements metric tracking
# MAGIC * Creates compliance checking logic
# MAGIC * Generates alert messages
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔵 **4. Debug Pipeline Failure**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC My pipeline is failing at the transformation stage.
# MAGIC Add detailed debugging:
# MAGIC - Print schema at each step
# MAGIC - Show sample data before/after transforms
# MAGIC - Add data profiling (nulls, types, counts)
# MAGIC - Capture full error stack trace
# MAGIC ```
# MAGIC
# MAGIC **What Genie Does:**
# MAGIC * Adds schema inspection code
# MAGIC * Implements data sampling
# MAGIC * Creates profiling functions
# MAGIC * Enhances error logging
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔵 **5. Create Metrics Dashboard Data**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Create a metrics summary table for dashboard:
# MAGIC - Group metrics by category (Performance, Quality, Volume)
# MAGIC - Include: metric name, value, unit, status
# MAGIC - Add health indicators (green/yellow/red)
# MAGIC - Format for visualization
# MAGIC ```
# MAGIC
# MAGIC **What Genie Does:**
# MAGIC * Converts metrics dict to DataFrame
# MAGIC * Adds categorization
# MAGIC * Implements status logic
# MAGIC * Formats for display()
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔵 **6. Implement Data Quality Checks**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Add comprehensive data quality checks:
# MAGIC - Null percentage by column
# MAGIC - Duplicate record detection
# MAGIC - Schema validation
# MAGIC - Value range checks for numeric columns
# MAGIC - Overall quality score calculation
# MAGIC ```
# MAGIC
# MAGIC **What Genie Does:**
# MAGIC * Creates quality check functions
# MAGIC * Implements validation logic
# MAGIC * Calculates quality scores
# MAGIC * Generates quality report
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔵 **7. Build Observable ETL Pipeline**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Build a production-ready ETL pipeline with full observability:
# MAGIC - Read from Unity Catalog Volume
# MAGIC - Apply transformations with error handling
# MAGIC - Log all stages in JSON format
# MAGIC - Capture comprehensive metrics
# MAGIC - Track SLA compliance
# MAGIC - Write to Delta table
# MAGIC ```
# MAGIC
# MAGIC **What Genie Does:**
# MAGIC * Generates complete pipeline
# MAGIC * Embeds observability at every stage
# MAGIC * Implements best practices
# MAGIC * Adds documentation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Tips for Effective Prompts:
# MAGIC
# MAGIC 1. **Be Specific**: Mention exact metrics, thresholds, formats
# MAGIC 2. **Provide Context**: Describe your pipeline stages
# MAGIC 3. **State Requirements**: SLAs, quality thresholds, alert conditions
# MAGIC 4. **Request Examples**: Ask for sample data or test cases
# MAGIC 5. **Iterate**: Start simple, then ask for enhancements
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Best Practices with Genie:
# MAGIC
# MAGIC * Start with "Add observability to..." for existing code
# MAGIC * Use "Create..." for new components
# MAGIC * Ask for "production-ready" to get best practices
# MAGIC * Request "serverless-compliant" code (no cache/persist)
# MAGIC * Specify "Unity Catalog Volumes" for data access

# COMMAND ----------

# DBTITLE 1,Section 10: Summary & Key Learnings
# MAGIC %md
# MAGIC # 🎓 Section 10: Summary & Key Learnings
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 What We Learned Today:
# MAGIC
# MAGIC ### 🔭 **1. Observability Fundamentals**
# MAGIC * Observability = ability to understand system state from external outputs
# MAGIC * Three pillars: Logs, Metrics, Traces
# MAGIC * Critical for proactive issue detection and fast debugging
# MAGIC
# MAGIC ### 📜 **2. Logging in Databricks**
# MAGIC * Driver logs, executor logs, job run logs
# MAGIC * Structured logging (JSON format) for better parsing
# MAGIC * Log at key milestones with context (timestamps, counts, IDs)
# MAGIC
# MAGIC ### 📊 **3. Metrics & Monitoring**
# MAGIC * Performance metrics: execution time, throughput
# MAGIC * Data metrics: record counts, data volume, quality scores
# MAGIC * Business metrics: revenue, transactions, KPIs
# MAGIC
# MAGIC ### 🔧 **4. Debugging Failures**
# MAGIC * Common failure types: data issues, schema problems, runtime errors
# MAGIC * Root cause analysis (RCA) process
# MAGIC * Error handling patterns: try/except, validation, graceful degradation
# MAGIC
# MAGIC ### 📝 **5. SLA/SLO Concepts**
# MAGIC * SLA = Service Level Agreement (promise)
# MAGIC * SLO = Service Level Objective (target)
# MAGIC * SLI = Service Level Indicator (measurement)
# MAGIC * Mapping to Databricks Jobs: retries, timeouts, alerts
# MAGIC
# MAGIC ### 🚀 **6. Production Patterns**
# MAGIC * End-to-end observable pipelines
# MAGIC * Retry strategies and dead letter queues
# MAGIC * Alert mechanisms (email, webhooks, dashboards)
# MAGIC * Health monitoring and SLA tracking
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Key Takeaways:
# MAGIC
# MAGIC 1. **Observability is Not Optional**: Production pipelines MUST have logging, metrics, and monitoring
# MAGIC
# MAGIC 2. **Log Everything**: But log smartly - use structured formats and appropriate levels
# MAGIC
# MAGIC 3. **Measure What Matters**: Track metrics that align with business SLAs
# MAGIC
# MAGIC 4. **Fail Gracefully**: Implement error handling, retries, and dead letter queues
# MAGIC
# MAGIC 5. **Alert Proactively**: Warn before problems become incidents
# MAGIC
# MAGIC 6. **Document Everything**: Runbooks, SLAs, escalation procedures
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💼 Production Readiness Checklist:
# MAGIC
# MAGIC ### ✅ **Before Going to Production:**
# MAGIC
# MAGIC * [ ] Structured logging implemented
# MAGIC * [ ] Metrics collection at every stage
# MAGIC * [ ] Data quality checks in place
# MAGIC * [ ] SLA requirements documented
# MAGIC * [ ] Error handling and retry logic
# MAGIC * [ ] Alert configuration (email/webhook)
# MAGIC * [ ] Monitoring dashboard created
# MAGIC * [ ] Dead letter queue for failed records
# MAGIC * [ ] Runbook documentation
# MAGIC * [ ] On-call rotation defined
# MAGIC * [ ] Incident response process
# MAGIC * [ ] Post-mortem template prepared
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Common Mistakes to Avoid:
# MAGIC
# MAGIC ### ❌ **1. No Logging**
# MAGIC * **Problem**: Can't debug issues when they occur
# MAGIC * **Solution**: Add structured logging at every stage
# MAGIC
# MAGIC ### ❌ **2. No Monitoring Metrics**
# MAGIC * **Problem**: Don't know if pipeline is healthy or degrading
# MAGIC * **Solution**: Track performance, quality, and business metrics
# MAGIC
# MAGIC ### ❌ **3. Ignoring SLAs**
# MAGIC * **Problem**: Users lose trust when commitments aren't met
# MAGIC * **Solution**: Define clear SLAs and track compliance
# MAGIC
# MAGIC ### ❌ **4. Poor Error Handling**
# MAGIC * **Problem**: Pipelines fail completely on minor issues
# MAGIC * **Solution**: Implement try/except, retries, graceful degradation
# MAGIC
# MAGIC ### ❌ **5. Alert Fatigue**
# MAGIC * **Problem**: Too many alerts lead to ignoring critical ones
# MAGIC * **Solution**: Alert only on actionable issues, use severity levels
# MAGIC
# MAGIC ### ❌ **6. No Dead Letter Queue**
# MAGIC * **Problem**: Failed records are lost forever
# MAGIC * **Solution**: Store failed records for later analysis
# MAGIC
# MAGIC ### ❌ **7. Hardcoded Thresholds**
# MAGIC * **Problem**: Can't adapt to changing requirements
# MAGIC * **Solution**: Use configuration for SLAs and thresholds
# MAGIC
# MAGIC ### ❌ **8. No Documentation**
# MAGIC * **Problem**: On-call engineers don't know how to respond
# MAGIC * **Solution**: Create runbooks with step-by-step procedures
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Interview Questions (Day 10):
# MAGIC
# MAGIC ### **Basic Level:**
# MAGIC
# MAGIC 1. **What are the three pillars of observability?**
# MAGIC    * Answer: Logs, Metrics, Traces
# MAGIC
# MAGIC 2. **What is the difference between SLA and SLO?**
# MAGIC    * Answer: SLA is the agreement/promise, SLO is the specific measurable target within that agreement
# MAGIC
# MAGIC 3. **What is structured logging?**
# MAGIC    * Answer: Logging in a consistent, parseable format (typically JSON) with standardized fields
# MAGIC
# MAGIC 4. **Why is error handling important in data pipelines?**
# MAGIC    * Answer: Prevents complete failure, enables graceful degradation, helps with debugging
# MAGIC
# MAGIC ### **Intermediate Level:**
# MAGIC
# MAGIC 5. **How would you implement retry logic in a Databricks pipeline?**
# MAGIC    * Answer: Use Databricks Jobs max retries setting (0-3), or implement custom retry logic with exponential backoff
# MAGIC
# MAGIC 6. **What metrics would you track for a daily ETL pipeline?**
# MAGIC    * Answer: Execution time, record counts (input/output), data quality score, error rate, SLA compliance
# MAGIC
# MAGIC 7. **Explain the Dead Letter Queue pattern.**
# MAGIC    * Answer: Failed records are written to a separate table/location instead of being discarded, allowing later analysis and reprocessing
# MAGIC
# MAGIC 8. **How do you calculate a data quality score?**
# MAGIC    * Answer: Define quality checks (null %, schema validation, business rules), calculate pass rate, aggregate into overall score
# MAGIC
# MAGIC ### **Advanced Level:**
# MAGIC
# MAGIC 9. **Design an observable data pipeline for a financial trading system with strict SLAs.**
# MAGIC    * Answer: 
# MAGIC      * Bronze: Ingest with timestamp, log source and count
# MAGIC      * Silver: Validate against schema, check for duplicates, log quality score
# MAGIC      * Gold: Calculate trading metrics, validate against business rules
# MAGIC      * Monitoring: Track latency (< 1 second SLA), accuracy (99.99%), alert on any failure
# MAGIC      * Compliance: Audit logs for regulatory requirements
# MAGIC
# MAGIC 10. **How would you debug a pipeline that's failing intermittently?**
# MAGIC     * Answer:
# MAGIC       * Add detailed logging at each stage
# MAGIC       * Capture input data samples on failure
# MAGIC       * Track correlation between failures and external factors (time, data volume, source)
# MAGIC       * Implement data profiling to identify patterns
# MAGIC       * Use Databricks job run history to analyze failure patterns
# MAGIC       * Check for resource contention or timeout issues
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Next Steps:
# MAGIC
# MAGIC 1. **Practice**: Implement observability in your existing pipelines
# MAGIC 2. **Experiment**: Try different logging formats and metric collection strategies
# MAGIC 3. **Monitor**: Set up dashboards to track pipeline health
# MAGIC 4. **Iterate**: Continuously improve based on production incidents
# MAGIC 5. **Document**: Create runbooks for your team
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Additional Resources:
# MAGIC
# MAGIC * Databricks Documentation: Jobs and Monitoring
# MAGIC * Observability best practices for data pipelines
# MAGIC * SLA/SLO design patterns
# MAGIC * Incident response playbooks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Congratulations!
# MAGIC
# MAGIC You've completed **Phase 2 Day 10: Monitoring & Observability**!
# MAGIC
# MAGIC You now understand:
# MAGIC * How to make pipelines observable
# MAGIC * What metrics to track
# MAGIC * How to debug failures
# MAGIC * How to define and monitor SLAs
# MAGIC * Production-ready observability patterns
# MAGIC
# MAGIC **Next**: Apply these concepts to build reliable, production-grade data pipelines!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏷️ **@TRRaveendra** | Phase 2 Day 10 Complete ✅