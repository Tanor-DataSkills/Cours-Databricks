# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # 🔄 Data Engineering Training — Phase 8 Day 36  
# MAGIC ## 🚨 Production Patterns: Retry, Failure Handling & SLA Monitoring  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - Retry Strategies  
# MAGIC - Failure Handling & Recovery  
# MAGIC - SLA / SLO Monitoring  
# MAGIC - Production Pipeline Reliability  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### ⚙️ Platform: Databricks (Serverless + Unity Catalog + Workflows + Delta Lake)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Objective:
# MAGIC Learn how to design production-grade pipelines with robust retry mechanisms, failure handling, and SLA-driven monitoring.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Engineering Constraints:
# MAGIC - ✅ Use Databricks Serverless Compute
# MAGIC - ❌ NO RDDs
# MAGIC - ❌ NO cache() / persist()
# MAGIC - ❌ NO /tmp or local storage
# MAGIC - ✅ Unity Catalog for all data access
# MAGIC - ✅ Reliability-first design

# COMMAND ----------

# DBTITLE 1,Section 1: Production Reliability Overview
# MAGIC %md
# MAGIC ---
# MAGIC # 🏛️ SECTION 1: Production Reliability Overview
# MAGIC
# MAGIC ## 🧒 ELI5 (Explain Like I'm 5):
# MAGIC Imagine you're building a tower with blocks. Sometimes a block falls, or someone bumps the table. **Production reliability** means having a plan so your tower doesn't completely fall apart when something goes wrong. You might:
# MAGIC - Try again if a block falls
# MAGIC - Have spare blocks ready
# MAGIC - Check if the tower is still standing
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### Why Production Pipelines Fail:
# MAGIC
# MAGIC 1. **Data Issues**
# MAGIC    - Missing or corrupted source data
# MAGIC    - Schema changes upstream
# MAGIC    - Data quality violations
# MAGIC
# MAGIC 2. **System Issues**
# MAGIC    - Network timeouts
# MAGIC    - Resource exhaustion (memory, CPU)
# MAGIC    - Infrastructure failures
# MAGIC
# MAGIC 3. **Dependency Failures**
# MAGIC    - Upstream systems unavailable
# MAGIC    - External API rate limits
# MAGIC    - Authentication/authorization failures
# MAGIC
# MAGIC 4. **Logic Errors**
# MAGIC    - Bugs in transformation code
# MAGIC    - Edge cases not handled
# MAGIC    - Race conditions
# MAGIC
# MAGIC ### The Cost of Unreliable Pipelines:
# MAGIC - **Business Impact**: Delayed insights, wrong decisions
# MAGIC - **Engineering Cost**: Manual intervention, firefighting
# MAGIC - **Trust Erosion**: Stakeholders lose confidence
# MAGIC - **SLA Violations**: Contractual penalties
# MAGIC
# MAGIC ### Production Reliability Principles:
# MAGIC
# MAGIC 1. **Design for Failure**: Assume everything will fail
# MAGIC 2. **Fail Fast**: Detect issues early
# MAGIC 3. **Fail Gracefully**: Recover automatically when possible
# MAGIC 4. **Monitor Everything**: Observability is key
# MAGIC 5. **Idempotency**: Same input = same output, always

# COMMAND ----------

# DBTITLE 1,Section 2: Retry Strategies
# MAGIC %md
# MAGIC ---
# MAGIC # 🔁 SECTION 2: Retry Strategies
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC Imagine you're trying to call your friend, but they don't answer. You can:
# MAGIC - **Call again immediately** (Fixed retry)
# MAGIC - **Wait a bit, then call again** (Exponential backoff)
# MAGIC - **Only call if it's not their bedtime** (Conditional retry)
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC Retry strategies are critical for handling transient failures. Choosing the wrong strategy can:
# MAGIC - Overwhelm downstream systems (retry storm)
# MAGIC - Waste compute resources
# MAGIC - Miss SLA windows
# MAGIC
# MAGIC ### 1️⃣ Fixed Retry
# MAGIC
# MAGIC **Pattern**: Retry N times with fixed delay
# MAGIC
# MAGIC ```
# MAGIC Attempt 1 → Fail → Wait 5s → Attempt 2 → Fail → Wait 5s → Attempt 3 → Success
# MAGIC ```
# MAGIC
# MAGIC **Use Cases**:
# MAGIC - Transient network glitches
# MAGIC - Brief resource contention
# MAGIC
# MAGIC **Limitations**:
# MAGIC - Can create retry storms
# MAGIC - No adaptation to system load
# MAGIC
# MAGIC ### 2️⃣ Exponential Backoff
# MAGIC
# MAGIC **Pattern**: Retry with exponentially increasing delays
# MAGIC
# MAGIC ```
# MAGIC Attempt 1 → Fail → Wait 1s
# MAGIC Attempt 2 → Fail → Wait 2s
# MAGIC Attempt 3 → Fail → Wait 4s
# MAGIC Attempt 4 → Fail → Wait 8s
# MAGIC Attempt 5 → Success
# MAGIC ```
# MAGIC
# MAGIC **Use Cases**:
# MAGIC - API rate limiting
# MAGIC - System recovery scenarios
# MAGIC - External service dependencies
# MAGIC
# MAGIC **Benefits**:
# MAGIC - Reduces load on failing systems
# MAGIC - Allows time for recovery
# MAGIC - Industry best practice
# MAGIC
# MAGIC ### 3️⃣ Conditional Retry
# MAGIC
# MAGIC **Pattern**: Retry only for specific error types
# MAGIC
# MAGIC ```
# MAGIC Error Type = Timeout → Retry
# MAGIC Error Type = Authentication → Don't Retry (fail immediately)
# MAGIC Error Type = Rate Limit → Retry with backoff
# MAGIC ```
# MAGIC
# MAGIC **Use Cases**:
# MAGIC - Permanent vs transient failure differentiation
# MAGIC - Resource optimization
# MAGIC - Fast failure for unrecoverable errors
# MAGIC
# MAGIC ### 4️⃣ Circuit Breaker Pattern
# MAGIC
# MAGIC **Pattern**: Stop retrying after threshold, resume after cool-down
# MAGIC
# MAGIC ```
# MAGIC Closed (Normal) → Failures accumulate → Open (Stop trying) → Cool-down → Half-Open (Test) → Success → Closed
# MAGIC ```
# MAGIC
# MAGIC **Benefits**:
# MAGIC - Protects failing systems
# MAGIC - Fast failure during outages
# MAGIC - Automatic recovery testing

# COMMAND ----------

# DBTITLE 1,Retry Strategy Implementation
# Retry Strategy Implementations
import time
from datetime import datetime

# 1. Fixed Retry Strategy
def fixed_retry(func, max_attempts=3, delay=5):
    """
    Fixed retry with constant delay
    """
    for attempt in range(1, max_attempts + 1):
        try:
            print(f"[{datetime.now()}] Attempt {attempt}/{max_attempts}")
            result = func()
            print(f"✅ Success on attempt {attempt}")
            return result
        except Exception as e:
            print(f"❌ Attempt {attempt} failed: {str(e)}")
            if attempt < max_attempts:
                print(f"⏳ Waiting {delay} seconds before retry...")
                time.sleep(delay)
            else:
                print("🚨 Max attempts reached, giving up")
                raise

# 2. Exponential Backoff Strategy
def exponential_backoff_retry(func, max_attempts=5, base_delay=1, max_delay=60):
    """
    Exponential backoff: delay doubles each attempt
    """
    for attempt in range(1, max_attempts + 1):
        try:
            print(f"[{datetime.now()}] Attempt {attempt}/{max_attempts}")
            result = func()
            print(f"✅ Success on attempt {attempt}")
            return result
        except Exception as e:
            print(f"❌ Attempt {attempt} failed: {str(e)}")
            if attempt < max_attempts:
                delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                print(f"⏳ Waiting {delay} seconds before retry (exponential backoff)...")
                time.sleep(delay)
            else:
                print("🚨 Max attempts reached, giving up")
                raise

# 3. Conditional Retry Strategy
def conditional_retry(func, max_attempts=3, retryable_exceptions=(Exception,)):
    """
    Only retry for specific exception types
    """
    for attempt in range(1, max_attempts + 1):
        try:
            print(f"[{datetime.now()}] Attempt {attempt}/{max_attempts}")
            result = func()
            print(f"✅ Success on attempt {attempt}")
            return result
        except retryable_exceptions as e:
            print(f"🔄 Retryable failure: {str(e)}")
            if attempt < max_attempts:
                time.sleep(2)
            else:
                raise
        except Exception as e:
            print(f"🚫 Non-retryable failure: {str(e)}")
            raise  # Fail immediately

print("✅ Retry strategy functions defined")
print("\nThese patterns will be used in production pipeline examples later")

# COMMAND ----------

# DBTITLE 1,Section 3: Failure Handling
# MAGIC %md
# MAGIC ---
# MAGIC # 🚧 SECTION 3: Failure Handling
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC When something breaks, you need to know:
# MAGIC - **What broke?** (the error message)
# MAGIC - **Can it be fixed quickly?** (retry or not)
# MAGIC - **What should happen next?** (fallback plan)
# MAGIC
# MAGIC It's like having a backup toy when your favorite one breaks!
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### Types of Failures:
# MAGIC
# MAGIC #### 1️⃣ Data Failures
# MAGIC - **Missing Data**: Source table/file not found
# MAGIC - **Schema Mismatch**: Columns added/removed/renamed
# MAGIC - **Data Quality**: Nulls, duplicates, constraint violations
# MAGIC - **Volume Anomalies**: Too much/too little data
# MAGIC
# MAGIC **Handling Strategy**:
# MAGIC - Validate schema before processing
# MAGIC - Implement data quality checks
# MAGIC - Use dead letter queues for bad records
# MAGIC - Alert on volume anomalies
# MAGIC
# MAGIC #### 2️⃣ System Failures
# MAGIC - **Resource Exhaustion**: OOM, disk full, CPU throttling
# MAGIC - **Network Issues**: Timeouts, connection resets
# MAGIC - **Infrastructure**: Node failures, AZ outages
# MAGIC
# MAGIC **Handling Strategy**:
# MAGIC - Auto-scaling and resource monitoring
# MAGIC - Connection pooling and timeouts
# MAGIC - Multi-AZ deployment
# MAGIC - Retry with exponential backoff
# MAGIC
# MAGIC #### 3️⃣ Dependency Failures
# MAGIC - **Upstream Delays**: Source data not ready
# MAGIC - **Downstream Unavailable**: Target system down
# MAGIC - **External APIs**: Rate limits, authentication failures
# MAGIC
# MAGIC **Handling Strategy**:
# MAGIC - Implement timeout thresholds
# MAGIC - Use circuit breakers
# MAGIC - Cache when appropriate
# MAGIC - Graceful degradation
# MAGIC
# MAGIC #### 4️⃣ Logic Errors
# MAGIC - **Bugs**: Code defects
# MAGIC - **Edge Cases**: Unhandled scenarios
# MAGIC - **Configuration Errors**: Wrong parameters
# MAGIC
# MAGIC **Handling Strategy**:
# MAGIC - Comprehensive testing
# MAGIC - Input validation
# MAGIC - Configuration management
# MAGIC - Canary deployments
# MAGIC
# MAGIC ### Failure Handling Hierarchy:
# MAGIC
# MAGIC ```
# MAGIC 1. Prevent → Input validation, schema enforcement
# MAGIC 2. Detect → Early failure detection, monitoring
# MAGIC 3. Isolate → Quarantine bad data, circuit breakers
# MAGIC 4. Retry → Transient failure recovery
# MAGIC 5. Fallback → Default values, cached data
# MAGIC 6. Alert → Notify on-call engineers
# MAGIC 7. Recover → Manual intervention, rollback
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Failure Handling Implementation
# Failure Handling Patterns

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime

# 1. Basic Try-Catch Pattern
def safe_read_table(catalog, schema, table):
    """
    Safely read a table with error handling
    """
    full_table_name = f"{catalog}.{schema}.{table}"
    try:
        print(f"📂 Attempting to read: {full_table_name}")
        df = spark.read.table(full_table_name)
        print(f"✅ Successfully read {df.count()} rows")
        return df
    except Exception as e:
        error_type = type(e).__name__
        print(f"❌ Failed to read {full_table_name}")
        print(f"   Error Type: {error_type}")
        print(f"   Error Message: {str(e)}")
        
        # Categorize the error
        if "AnalysisException" in error_type:
            print("🚨 Data Error: Table may not exist or access denied")
        elif "Timeout" in str(e):
            print("⏳ System Error: Network timeout")
        else:
            print("❓ Unknown Error Type")
        
        return None

# 2. Schema Validation Pattern
def validate_schema(df, expected_columns):
    """
    Validate that DataFrame has expected columns
    """
    actual_columns = set(df.columns)
    expected_set = set(expected_columns)
    
    missing = expected_set - actual_columns
    extra = actual_columns - expected_set
    
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    
    if extra:
        print(f"⚠️ Warning: Extra columns found: {extra}")
    
    print("✅ Schema validation passed")
    return True

# 3. Dead Letter Queue Pattern
def process_with_dlq(df, validation_func):
    """
    Process records, send failures to dead letter queue
    """
    # Add validation column
    df_validated = df.withColumn(
        "is_valid",
        validation_func(df)
    )
    
    # Split into valid and invalid
    valid_df = df_validated.filter(F.col("is_valid") == True).drop("is_valid")
    invalid_df = df_validated.filter(F.col("is_valid") == False).drop("is_valid")
    
    valid_count = valid_df.count()
    invalid_count = invalid_df.count()
    
    print(f"✅ Valid records: {valid_count}")
    print(f"❌ Invalid records: {invalid_count}")
    
    return valid_df, invalid_df

# 4. Graceful Degradation Pattern
def read_with_fallback(primary_table, fallback_table):
    """
    Try primary source, fall back to alternative
    """
    try:
        print(f"🎯 Attempting primary source: {primary_table}")
        df = spark.read.table(primary_table)
        print("✅ Primary source successful")
        return df, "primary"
    except Exception as e:
        print(f"⚠️ Primary source failed: {str(e)}")
        try:
            print(f"🔄 Attempting fallback source: {fallback_table}")
            df = spark.read.table(fallback_table)
            print("✅ Fallback source successful")
            return df, "fallback"
        except Exception as e2:
            print(f"🚨 Both sources failed")
            raise

print("✅ Failure handling functions defined")

# COMMAND ----------

# DBTITLE 1,Section 4: Recovery Mechanisms
# MAGIC %md
# MAGIC ---
# MAGIC # 🔄 SECTION 4: Recovery Mechanisms
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC Imagine you're doing a jigsaw puzzle and your little brother messes it up. Recovery mechanisms are like:
# MAGIC - **Checkpoint**: You took a photo halfway through, so you can restart from there
# MAGIC - **Idempotent**: Even if you place the same piece twice, it's still in the right spot
# MAGIC - **Restart**: You can start over from the beginning without breaking anything
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### 1️⃣ Checkpoint-Based Recovery
# MAGIC
# MAGIC **Concept**: Save progress at intervals to resume from last known good state
# MAGIC
# MAGIC **Use Cases**:
# MAGIC - **Streaming Pipelines**: Structured Streaming checkpoints
# MAGIC - **Batch ETL**: Watermark tables tracking processed data
# MAGIC - **Long-Running Jobs**: Intermediate state persistence
# MAGIC
# MAGIC **Benefits**:
# MAGIC - Avoid reprocessing all data
# MAGIC - Reduce recovery time
# MAGIC - Enable exactly-once semantics
# MAGIC
# MAGIC **Implementation**:
# MAGIC ```python
# MAGIC # Streaming with checkpoint
# MAGIC df.writeStream \
# MAGIC   .format("delta") \
# MAGIC   .option("checkpointLocation", "/path/to/checkpoint") \
# MAGIC   .start()
# MAGIC ```
# MAGIC
# MAGIC **Considerations**:
# MAGIC - Checkpoint location must be reliable (Unity Catalog volumes)
# MAGIC - Schema changes may require checkpoint reset
# MAGIC - Monitor checkpoint size growth
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ Idempotent Processing
# MAGIC
# MAGIC **Definition**: Operation can be applied multiple times without changing the result beyond the initial application
# MAGIC
# MAGIC **Formula**: `f(f(x)) = f(x)`
# MAGIC
# MAGIC **Why Critical**:
# MAGIC - Enables safe retries
# MAGIC - Prevents data duplication
# MAGIC - Simplifies failure recovery
# MAGIC
# MAGIC **Anti-Patterns (Non-Idempotent)**:
# MAGIC ```python
# MAGIC # ❌ BAD: Appends duplicate data on retry
# MAGIC df.write.mode("append").saveAsTable("target")
# MAGIC
# MAGIC # ❌ BAD: Counter increments on each retry
# MAGIC UPDATE table SET counter = counter + 1
# MAGIC ```
# MAGIC
# MAGIC **Idempotent Patterns**:
# MAGIC ```python
# MAGIC # ✅ GOOD: Merge with unique keys
# MAGIC MERGE INTO target USING source ON target.id = source.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC
# MAGIC # ✅ GOOD: Overwrite partition
# MAGIC df.write.mode("overwrite").partitionBy("date").saveAsTable("target")
# MAGIC
# MAGIC # ✅ GOOD: Idempotent key
# MAGIC df.withColumn("idempotency_key", F.sha2(F.concat_ws("|", "col1", "col2"), 256))
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ Restart Strategies
# MAGIC
# MAGIC **Full Restart**:
# MAGIC - Start from beginning
# MAGIC - Use when: Cheap to reprocess, data is small
# MAGIC - Pattern: `DELETE FROM target WHERE date >= 'YYYY-MM-DD'`
# MAGIC
# MAGIC **Incremental Restart**:
# MAGIC - Resume from last successful batch
# MAGIC - Use when: Expensive to reprocess
# MAGIC - Pattern: Watermark table tracking
# MAGIC
# MAGIC **Partial Restart**:
# MAGIC - Reprocess only failed partition/subset
# MAGIC - Use when: Failures are localized
# MAGIC - Pattern: `WHERE status = 'FAILED'`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4️⃣ State Management
# MAGIC
# MAGIC **Stateless** (Preferred):
# MAGIC - No dependencies on previous runs
# MAGIC - Each run is independent
# MAGIC - Easy to retry and scale
# MAGIC
# MAGIC **Stateful** (When Necessary):
# MAGIC - Maintain state across runs (aggregations, windows)
# MAGIC - Requires careful checkpoint management
# MAGIC - Complex recovery logic

# COMMAND ----------

# DBTITLE 1,Recovery Mechanism Implementation
# Recovery Mechanism Implementations

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta

# 1. Watermark-Based Recovery Pattern
def get_last_processed_timestamp(watermark_table, pipeline_name):
    """
    Get the last successfully processed timestamp
    """
    try:
        watermark_df = spark.read.table(watermark_table)
        last_ts = watermark_df \
            .filter(F.col("pipeline_name") == pipeline_name) \
            .select(F.max("last_processed_ts").alias("max_ts")) \
            .collect()[0]["max_ts"]
        
        if last_ts:
            print(f"✅ Found watermark: {last_ts}")
            return last_ts
        else:
            print("⚠️ No watermark found, starting from beginning")
            return None
    except Exception as e:
        print(f"⚠️ Watermark table read failed: {str(e)}")
        return None

def update_watermark(watermark_table, pipeline_name, new_timestamp):
    """
    Update the watermark after successful processing
    """
    watermark_data = [
        (pipeline_name, new_timestamp, datetime.now())
    ]
    
    watermark_df = spark.createDataFrame(
        watermark_data,
        ["pipeline_name", "last_processed_ts", "updated_at"]
    )
    
    # Idempotent merge
    watermark_df.createOrReplaceTempView("watermark_updates")
    
    spark.sql(f"""
        MERGE INTO {watermark_table} target
        USING watermark_updates source
        ON target.pipeline_name = source.pipeline_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    print(f"✅ Watermark updated to: {new_timestamp}")

# 2. Idempotent Write Pattern
def idempotent_merge(source_df, target_table, key_columns, partition_columns=None):
    """
    Perform idempotent merge operation
    """
    # Create merge condition
    merge_condition = " AND ".join([
        f"target.{col} = source.{col}" for col in key_columns
    ])
    
    # Create temporary view
    source_df.createOrReplaceTempView("merge_source")
    
    # Perform merge
    merge_sql = f"""
        MERGE INTO {target_table} target
        USING merge_source source
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """
    
    try:
        spark.sql(merge_sql)
        print(f"✅ Idempotent merge completed successfully")
        return True
    except Exception as e:
        print(f"❌ Merge failed: {str(e)}")
        return False

# 3. Create Idempotency Key
def add_idempotency_key(df, key_columns):
    """
    Add a deterministic idempotency key based on business keys
    """
    # Concatenate key columns with separator
    concat_expr = F.concat_ws("|", *[F.col(c) for c in key_columns])
    
    # Create SHA-256 hash as idempotency key
    df_with_key = df.withColumn(
        "idempotency_key",
        F.sha2(concat_expr, 256)
    )
    
    print(f"✅ Idempotency key added based on: {key_columns}")
    return df_with_key

# 4. Partition-Level Recovery
def get_failed_partitions(status_table, pipeline_name):
    """
    Identify partitions that failed and need reprocessing
    """
    try:
        status_df = spark.read.table(status_table)
        failed = status_df \
            .filter(
                (F.col("pipeline_name") == pipeline_name) &
                (F.col("status") == "FAILED")
            ) \
            .select("partition_key") \
            .distinct()
        
        failed_list = [row["partition_key"] for row in failed.collect()]
        print(f"🔄 Found {len(failed_list)} partitions to retry: {failed_list}")
        return failed_list
    except Exception as e:
        print(f"⚠️ Could not read status table: {str(e)}")
        return []

print("✅ Recovery mechanism functions defined")
print("\nThese patterns enable:")
print("  - Resume processing from last checkpoint")
print("  - Idempotent operations (safe retries)")
print("  - Partition-level recovery")

# COMMAND ----------

# DBTITLE 1,Section 5: SLA and SLO Monitoring
# MAGIC %md
# MAGIC ---
# MAGIC # 🎯 SECTION 5: SLA / SLO Monitoring
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC An **SLA** is like a promise:
# MAGIC - "I promise your pizza will arrive in 30 minutes or it's free!"
# MAGIC
# MAGIC An **SLO** is like a goal:
# MAGIC - "We want to deliver 95% of pizzas within 30 minutes"
# MAGIC
# MAGIC If you break too many promises, customers get upset and you might have to give refunds (or lose your job!).
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### SLA vs SLO vs SLI
# MAGIC
# MAGIC #### 📄 SLA (Service Level Agreement)
# MAGIC **Definition**: Contractual commitment between provider and consumer
# MAGIC
# MAGIC **Examples**:
# MAGIC - "Daily reports available by 8 AM ET with 99.9% uptime"
# MAGIC - "Data freshness within 15 minutes for real-time dashboards"
# MAGIC - "Pipeline must complete within 2 hours or trigger escalation"
# MAGIC
# MAGIC **Consequences of Breach**:
# MAGIC - Financial penalties
# MAGIC - Customer churn
# MAGIC - Reputational damage
# MAGIC - Team escalations
# MAGIC
# MAGIC #### 🎯 SLO (Service Level Objective)
# MAGIC **Definition**: Internal target that provides buffer before SLA breach
# MAGIC
# MAGIC **Relationship**: `SLO < SLA` (more strict internally)
# MAGIC
# MAGIC **Examples**:
# MAGIC - SLA: 99.9% uptime → SLO: 99.95% uptime
# MAGIC - SLA: Complete in 2 hours → SLO: Complete in 90 minutes
# MAGIC - SLA: 15-minute freshness → SLO: 10-minute freshness
# MAGIC
# MAGIC **Benefits**:
# MAGIC - Early warning system
# MAGIC - Room for graceful degradation
# MAGIC - Proactive remediation
# MAGIC
# MAGIC #### 📊 SLI (Service Level Indicator)
# MAGIC **Definition**: Quantitative measure of service level
# MAGIC
# MAGIC **Examples**:
# MAGIC - **Latency**: P50, P95, P99 processing time
# MAGIC - **Availability**: Successful runs / Total runs
# MAGIC - **Throughput**: Records processed per second
# MAGIC - **Freshness**: Time from source update to data availability
# MAGIC - **Quality**: % records passing validation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key SLAs for Data Pipelines:
# MAGIC
# MAGIC #### 1️⃣ **Latency SLA**
# MAGIC ```
# MAGIC "Pipeline must complete within X minutes/hours"
# MAGIC ```
# MAGIC **Metrics**:
# MAGIC - End-to-end execution time
# MAGIC - Per-stage execution time
# MAGIC - P95/P99 latency
# MAGIC
# MAGIC **Breach Scenarios**:
# MAGIC - Large data volumes
# MAGIC - Slow upstream sources
# MAGIC - Resource contention
# MAGIC - Complex transformations
# MAGIC
# MAGIC #### 2️⃣ **Freshness SLA**
# MAGIC ```
# MAGIC "Data must be no older than X minutes"
# MAGIC ```
# MAGIC **Metrics**:
# MAGIC - Time since last update
# MAGIC - Data lag (event time vs processing time)
# MAGIC - End-to-end data delay
# MAGIC
# MAGIC **Breach Scenarios**:
# MAGIC - Upstream delays
# MAGIC - Processing bottlenecks
# MAGIC - Failed pipeline runs
# MAGIC
# MAGIC #### 3️⃣ **Availability SLA**
# MAGIC ```
# MAGIC "Pipeline success rate must be > X%"
# MAGIC ```
# MAGIC **Metrics**:
# MAGIC - Success rate (30-day rolling)
# MAGIC - Mean time between failures (MTBF)
# MAGIC - Mean time to recovery (MTTR)
# MAGIC
# MAGIC **Formula**:
# MAGIC ```
# MAGIC Availability = Uptime / (Uptime + Downtime)
# MAGIC ```
# MAGIC
# MAGIC #### 4️⃣ **Quality SLA**
# MAGIC ```
# MAGIC "Data quality score must be > X%"
# MAGIC ```
# MAGIC **Metrics**:
# MAGIC - % complete records
# MAGIC - % records within constraints
# MAGIC - Duplicate rate
# MAGIC - Schema compliance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### SLA Monitoring Strategy:
# MAGIC
# MAGIC ```
# MAGIC 1. Define SLIs → What to measure
# MAGIC 2. Set SLOs → Internal targets (buffer)
# MAGIC 3. Establish SLAs → Customer commitments
# MAGIC 4. Implement Monitoring → Track metrics
# MAGIC 5. Configure Alerts → Proactive notification
# MAGIC 6. Review & Adjust → Continuous improvement
# MAGIC ```
# MAGIC
# MAGIC ### Error Budget:
# MAGIC
# MAGIC **Concept**: Allowable amount of unreliability
# MAGIC
# MAGIC **Formula**:
# MAGIC ```
# MAGIC Error Budget = (1 - SLO) × Time Period
# MAGIC ```
# MAGIC
# MAGIC **Example**:
# MAGIC - SLO: 99.9% availability
# MAGIC - Time Period: 30 days
# MAGIC - Error Budget: 0.1% × 30 days = **43.2 minutes of downtime**
# MAGIC
# MAGIC **Usage**:
# MAGIC - Track consumption throughout period
# MAGIC - Balance reliability vs feature velocity
# MAGIC - Trigger freeze when budget exhausted

# COMMAND ----------

# DBTITLE 1,Section 6: Metrics and Monitoring
# MAGIC %md
# MAGIC ---
# MAGIC # 📊 SECTION 6: Metrics & Monitoring
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC Monitoring is like having a health check:
# MAGIC - **How fast** did your pipeline run? (like checking your heartbeat)
# MAGIC - **How much** data did it process? (like stepping on a scale)
# MAGIC - **Did anything** break? (like checking for boo-boos)
# MAGIC
# MAGIC You write these down so you can see if things are getting better or worse!
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### The Four Golden Signals:
# MAGIC
# MAGIC #### 1️⃣ **Latency**
# MAGIC How long does it take?
# MAGIC - Pipeline execution time
# MAGIC - Per-stage duration
# MAGIC - Query performance
# MAGIC - Data freshness lag
# MAGIC
# MAGIC #### 2️⃣ **Traffic**
# MAGIC How much work is being done?
# MAGIC - Records processed
# MAGIC - Data volume (GB)
# MAGIC - API calls made
# MAGIC - Concurrent jobs
# MAGIC
# MAGIC #### 3️⃣ **Errors**
# MAGIC What's failing?
# MAGIC - Error rate
# MAGIC - Error types
# MAGIC - Failed records
# MAGIC - Data quality issues
# MAGIC
# MAGIC #### 4️⃣ **Saturation**
# MAGIC How full is the system?
# MAGIC - CPU utilization
# MAGIC - Memory usage
# MAGIC - Storage capacity
# MAGIC - Queue depth
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Critical Metrics to Track:
# MAGIC
# MAGIC #### 🕒 **Runtime Metrics**
# MAGIC ```
# MAGIC - Total execution time
# MAGIC - Stage-level timing
# MAGIC - Read time, Transform time, Write time
# MAGIC - P50, P95, P99 latency percentiles
# MAGIC ```
# MAGIC
# MAGIC #### 📊 **Volume Metrics**
# MAGIC ```
# MAGIC - Input record count
# MAGIC - Output record count
# MAGIC - Data size processed (MB/GB)
# MAGIC - Delta changes (inserts/updates/deletes)
# MAGIC ```
# MAGIC
# MAGIC #### ✅ **Quality Metrics**
# MAGIC ```
# MAGIC - Null rate by column
# MAGIC - Duplicate count
# MAGIC - Schema compliance
# MAGIC - Constraint violations
# MAGIC - Business rule failures
# MAGIC ```
# MAGIC
# MAGIC #### 🚨 **Reliability Metrics**
# MAGIC ```
# MAGIC - Success rate (last 7/30 days)
# MAGIC - Failure rate
# MAGIC - Retry count
# MAGIC - Mean time to recovery (MTTR)
# MAGIC - Mean time between failures (MTBF)
# MAGIC ```
# MAGIC
# MAGIC #### 💰 **Cost Metrics**
# MAGIC ```
# MAGIC - DBU consumption
# MAGIC - Storage costs
# MAGIC - Compute time
# MAGIC - Cost per record
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Monitoring Implementation Levels:
# MAGIC
# MAGIC #### **Level 1: Basic Logging**
# MAGIC ```python
# MAGIC print(f"Processed {count} records in {duration} seconds")
# MAGIC ```
# MAGIC
# MAGIC #### **Level 2: Structured Metrics**
# MAGIC ```python
# MAGIC metrics = {
# MAGIC     "pipeline": "customer_etl",
# MAGIC     "records": 10000,
# MAGIC     "duration_sec": 120,
# MAGIC     "timestamp": datetime.now()
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC #### **Level 3: Metrics Table**
# MAGIC ```sql
# MAGIC INSERT INTO pipeline_metrics VALUES (...)
# MAGIC ```
# MAGIC
# MAGIC #### **Level 4: Time-Series Monitoring**
# MAGIC - Store metrics in time-series DB
# MAGIC - Build dashboards (Grafana, Datadog)
# MAGIC - Set up alerts
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Observability Stack:
# MAGIC
# MAGIC ```
# MAGIC 📈 Metrics → Quantitative measurements (counters, gauges)
# MAGIC 📝 Logs → Event records (errors, warnings, info)
# MAGIC 🔍 Traces → Request flows (end-to-end timing)
# MAGIC 🚨 Alerts → Notifications on threshold breach
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Metrics Collection Implementation
# Metrics Collection Implementation

import time
import json
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Metrics Collection Class
class PipelineMetrics:
    """
    Comprehensive metrics collection for production pipelines
    """
    
    def __init__(self, pipeline_name, run_id=None):
        self.pipeline_name = pipeline_name
        self.run_id = run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        self.start_time = time.time()
        self.metrics = {
            "pipeline_name": pipeline_name,
            "run_id": self.run_id,
            "start_time": datetime.now().isoformat(),
            "stages": []
        }
    
    def start_stage(self, stage_name):
        """Start timing a pipeline stage"""
        self.current_stage = {
            "stage_name": stage_name,
            "start_time": time.time()
        }
        print(f"⏱️  Starting stage: {stage_name}")
    
    def end_stage(self, records_processed=None, status="success"):
        """End timing and record stage metrics"""
        if not hasattr(self, 'current_stage'):
            return
        
        duration = time.time() - self.current_stage["start_time"]
        
        stage_metrics = {
            "stage_name": self.current_stage["stage_name"],
            "duration_sec": round(duration, 2),
            "records_processed": records_processed,
            "status": status
        }
        
        self.metrics["stages"].append(stage_metrics)
        
        print(f"✅ Stage '{self.current_stage['stage_name']}' completed in {duration:.2f}s")
        if records_processed:
            print(f"   Records: {records_processed:,}")
    
    def add_quality_metrics(self, df, stage_name):
        """Collect data quality metrics"""
        print(f"\n🔍 Collecting quality metrics for: {stage_name}")
        
        total_count = df.count()
        
        # Null counts per column
        null_counts = {}
        for col in df.columns:
            null_count = df.filter(F.col(col).isNull()).count()
            null_pct = (null_count / total_count * 100) if total_count > 0 else 0
            null_counts[col] = {
                "null_count": null_count,
                "null_percentage": round(null_pct, 2)
            }
        
        quality_metrics = {
            "stage_name": stage_name,
            "total_records": total_count,
            "null_analysis": null_counts
        }
        
        self.metrics["quality_metrics"] = quality_metrics
        
        print(f"   Total records: {total_count:,}")
        print(f"   Columns analyzed: {len(null_counts)}")
    
    def finalize(self, status="success", error_message=None):
        """Finalize metrics collection"""
        total_duration = time.time() - self.start_time
        
        self.metrics["end_time"] = datetime.now().isoformat()
        self.metrics["total_duration_sec"] = round(total_duration, 2)
        self.metrics["status"] = status
        
        if error_message:
            self.metrics["error_message"] = error_message
        
        print(f"\n🏁 Pipeline '{self.pipeline_name}' completed")
        print(f"   Status: {status}")
        print(f"   Total Duration: {total_duration:.2f}s")
        print(f"   Run ID: {self.run_id}")
        
        return self.metrics
    
    def save_metrics(self, metrics_table=None):
        """Save metrics to Unity Catalog table"""
        if not metrics_table:
            print("⚠️  No metrics table specified, printing JSON instead")
            print(json.dumps(self.metrics, indent=2))
            return
        
        # Flatten metrics for table storage
        metrics_flat = {
            "pipeline_name": self.metrics["pipeline_name"],
            "run_id": self.metrics["run_id"],
            "start_time": self.metrics["start_time"],
            "end_time": self.metrics.get("end_time"),
            "total_duration_sec": self.metrics.get("total_duration_sec"),
            "status": self.metrics.get("status"),
            "error_message": self.metrics.get("error_message"),
            "metrics_json": json.dumps(self.metrics)
        }
        
        try:
            metrics_df = spark.createDataFrame([metrics_flat])
            metrics_df.write.mode("append").saveAsTable(metrics_table)
            print(f"✅ Metrics saved to: {metrics_table}")
        except Exception as e:
            print(f"❌ Failed to save metrics: {str(e)}")
            print("Metrics JSON:")
            print(json.dumps(self.metrics, indent=2))

# Example Usage
print("✅ PipelineMetrics class defined")
print("\nExample usage:")
print("""metrics = PipelineMetrics("my_pipeline")
metrics.start_stage("extract")
# ... do work ...
metrics.end_stage(records_processed=1000)
metrics.finalize()
metrics.save_metrics("catalog.schema.pipeline_metrics")""")

# COMMAND ----------

# DBTITLE 1,Section 7: Alerting Strategies
# MAGIC %md
# MAGIC ---
# MAGIC # 🚨 SECTION 7: Alerting (Conceptual)
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC Alerts are like fire alarms:
# MAGIC - They make noise when something is wrong
# MAGIC - They tell you WHERE the problem is
# MAGIC - They help you fix things before they get worse
# MAGIC - But if they go off too much, people stop listening!
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### Alert Hierarchy:
# MAGIC
# MAGIC #### 🔴 **P0 - Critical (Page)**
# MAGIC - **Response Time**: Immediate (< 5 min)
# MAGIC - **Escalation**: On-call engineer
# MAGIC - **Examples**:
# MAGIC   - Production pipeline completely down
# MAGIC   - SLA breach imminent
# MAGIC   - Data corruption detected
# MAGIC   - Security incident
# MAGIC
# MAGIC #### 🟠 **P1 - High (Notify)**
# MAGIC - **Response Time**: < 1 hour
# MAGIC - **Escalation**: Team channel
# MAGIC - **Examples**:
# MAGIC   - Pipeline failing repeatedly
# MAGIC   - SLO breach
# MAGIC   - Elevated error rate
# MAGIC   - Resource saturation
# MAGIC
# MAGIC #### 🟡 **P2 - Medium (Track)**
# MAGIC - **Response Time**: Next business day
# MAGIC - **Escalation**: Ticket/Jira
# MAGIC - **Examples**:
# MAGIC   - Increased latency
# MAGIC   - Data quality degradation
# MAGIC   - Approaching resource limits
# MAGIC
# MAGIC #### ⚪ **P3 - Low (Monitor)**
# MAGIC - **Response Time**: Next sprint
# MAGIC - **Escalation**: Backlog
# MAGIC - **Examples**:
# MAGIC   - Performance optimization opportunities
# MAGIC   - Cost increase trends
# MAGIC   - Technical debt
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Alert Types:
# MAGIC
# MAGIC #### 1️⃣ **Threshold Alerts**
# MAGIC ```
# MAGIC IF metric > threshold THEN alert
# MAGIC ```
# MAGIC **Examples**:
# MAGIC - Pipeline duration > 2 hours
# MAGIC - Error rate > 5%
# MAGIC - Queue depth > 10,000
# MAGIC
# MAGIC #### 2️⃣ **Anomaly Detection**
# MAGIC ```
# MAGIC IF metric deviates from expected pattern THEN alert
# MAGIC ```
# MAGIC **Examples**:
# MAGIC - Sudden spike in volume
# MAGIC - Unusual processing time
# MAGIC - Unexpected null rate
# MAGIC
# MAGIC #### 3️⃣ **Absence Alerts**
# MAGIC ```
# MAGIC IF expected event did not occur THEN alert
# MAGIC ```
# MAGIC **Examples**:
# MAGIC - Pipeline didn't run at scheduled time
# MAGIC - No data received in last hour
# MAGIC - Heartbeat missing
# MAGIC
# MAGIC #### 4️⃣ **Composite Alerts**
# MAGIC ```
# MAGIC IF (condition1 AND condition2) THEN alert
# MAGIC ```
# MAGIC **Examples**:
# MAGIC - High latency AND high error rate
# MAGIC - SLO breach AND trending worse
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Alert Integration Points:
# MAGIC
# MAGIC #### 📧 **Email**
# MAGIC - Simple, universal
# MAGIC - Good for P2/P3
# MAGIC - Risk of alert fatigue
# MAGIC
# MAGIC #### 💬 **Slack/Teams**
# MAGIC - Real-time team notification
# MAGIC - Good for P1/P2
# MAGIC - Supports rich formatting
# MAGIC
# MAGIC #### 📱 **PagerDuty/Opsgenie**
# MAGIC - On-call rotation
# MAGIC - Escalation policies
# MAGIC - Critical for P0
# MAGIC
# MAGIC #### 🐛 **Jira/ServiceNow**
# MAGIC - Ticket creation
# MAGIC - Track resolution
# MAGIC - Good for P2/P3
# MAGIC
# MAGIC #### 🔔 **Databricks Workflows**
# MAGIC - Built-in job notifications
# MAGIC - Email on failure
# MAGIC - Integration with webhooks
# MAGIC
# MAGIC #### 🔗 **Webhooks**
# MAGIC - Custom integrations
# MAGIC - SNS, Lambda, custom endpoints
# MAGIC - Maximum flexibility
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Alert Best Practices:
# MAGIC
# MAGIC #### ✅ **DO:**
# MAGIC - Make alerts actionable
# MAGIC - Include context (run ID, table name, timestamp)
# MAGIC - Set appropriate severity
# MAGIC - Document runbooks
# MAGIC - Test alert channels
# MAGIC - Use alert suppression during maintenance
# MAGIC
# MAGIC #### ❌ **DON'T:**
# MAGIC - Alert on every minor issue
# MAGIC - Use same channel for all severities
# MAGIC - Create alerts without runbooks
# MAGIC - Ignore alert fatigue
# MAGIC - Alert on symptoms of same root cause
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Alert Message Template:
# MAGIC
# MAGIC ```
# MAGIC 🚨 [P1] Pipeline Failure: customer_etl
# MAGIC
# MAGIC 🕒 Timestamp: 2026-04-21 08:30:00 UTC
# MAGIC 🎯 Pipeline: customer_etl
# MAGIC 🆔 Run ID: run_20260421_083000
# MAGIC ❌ Status: FAILED
# MAGIC
# MAGIC 🐛 Error: Table not found: prod.raw.customers
# MAGIC
# MAGIC 📖 Runbook: https://wiki.company.com/runbooks/customer_etl
# MAGIC 🔗 Job Link: https://databricks.com/jobs/12345/runs/67890
# MAGIC 📊 Metrics: https://grafana.com/d/pipeline-metrics
# MAGIC
# MAGIC 👥 On-call: @data-eng-oncall
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Alert Fatigue Prevention:
# MAGIC
# MAGIC 1. **Alert Tuning**: Adjust thresholds based on historical data
# MAGIC 2. **Deduplication**: Group similar alerts
# MAGIC 3. **Snooze**: Temporary suppression during known issues
# MAGIC 4. **Auto-resolution**: Close alerts when condition clears
# MAGIC 5. **Alert Routing**: Send to appropriate team/person
# MAGIC 6. **Regular Review**: Disable noisy, non-actionable alerts

# COMMAND ----------

# DBTITLE 1,Section 8: Hands-On Production Pipeline
# MAGIC %md
# MAGIC ---
# MAGIC # 🏛️ SECTION 8: Hands-On Production Pipeline
# MAGIC
# MAGIC ## Objective:
# MAGIC Build a complete production-grade pipeline that demonstrates:
# MAGIC 1. ✅ Retry logic with exponential backoff
# MAGIC 2. ✅ Comprehensive failure handling
# MAGIC 3. ✅ Data validation and quality checks
# MAGIC 4. ✅ Metrics collection (runtime, volume, quality)
# MAGIC 5. ✅ SLA monitoring
# MAGIC 6. ✅ Idempotent operations
# MAGIC 7. ✅ Recovery mechanism (watermark tracking)
# MAGIC
# MAGIC ## Pipeline Flow:
# MAGIC
# MAGIC ```
# MAGIC 📥 Source Data
# MAGIC     ↓
# MAGIC ✅ Validate Schema
# MAGIC     ↓
# MAGIC 🔄 Check Watermark (resume point)
# MAGIC     ↓
# MAGIC 🛠️  Transform Data
# MAGIC     ↓
# MAGIC ✅ Quality Checks
# MAGIC     ↓
# MAGIC 💾 Idempotent Write (MERGE)
# MAGIC     ↓
# MAGIC 🔄 Update Watermark
# MAGIC     ↓
# MAGIC 📊 Capture Metrics
# MAGIC     ↓
# MAGIC 🎯 Check SLA
# MAGIC     ↓
# MAGIC 🚨 Alert (if needed)
# MAGIC ```
# MAGIC
# MAGIC ## Scenario:
# MAGIC **E-commerce Order Processing Pipeline**
# MAGIC - Process hourly order data
# MAGIC - SLA: Complete within 30 minutes
# MAGIC - SLO: Complete within 20 minutes
# MAGIC - Quality: > 95% valid records
# MAGIC - Availability: > 99% success rate

# COMMAND ----------

# DBTITLE 1,Step 1: Setup Sample Data
# STEP 1: Setup - Create Sample Source Data

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

print("🔧 Setting up production pipeline demo...\n")

# Generate sample order data
def generate_sample_orders(num_records=1000):
    """
    Generate sample e-commerce order data
    """
    base_time = datetime(2026, 4, 21, 8, 0, 0)
    
    data = []
    for i in range(num_records):
        # Introduce some data quality issues intentionally
        order_id = f"ORD_{i:06d}"
        customer_id = f"CUST_{random.randint(1, 100):04d}" if random.random() > 0.02 else None  # 2% nulls
        order_amount = round(random.uniform(10, 500), 2) if random.random() > 0.01 else None  # 1% nulls
        order_status = random.choice(["pending", "completed", "cancelled", "failed"])
        order_timestamp = base_time + timedelta(minutes=random.randint(0, 60))
        
        data.append((
            order_id,
            customer_id,
            order_amount,
            order_status,
            order_timestamp
        ))
    
    return data

# Create source DataFrame
schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("order_amount", DoubleType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_timestamp", TimestampType(), True)
])

sample_data = generate_sample_orders(1000)
source_df = spark.createDataFrame(sample_data, schema)

print("✅ Sample source data created")
print(f"   Total records: {source_df.count():,}")
print(f"   Schema: {source_df.columns}")
print("\n🔍 Sample data:")
display(source_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Step 2: Production Pipeline Class
# STEP 2: Production Pipeline with All Reliability Patterns

import time
from datetime import datetime

class ProductionPipeline:
    """
    Production-grade pipeline with comprehensive reliability patterns
    """
    
    def __init__(self, pipeline_name, sla_minutes=30, slo_minutes=20):
        self.pipeline_name = pipeline_name
        self.sla_minutes = sla_minutes
        self.slo_minutes = slo_minutes
        self.metrics = PipelineMetrics(pipeline_name)
        
    def validate_schema(self, df, required_columns):
        """
        Validate DataFrame schema
        """
        print("✅ Validating schema...")
        missing = set(required_columns) - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        print(f"   ✅ All required columns present: {required_columns}")
        return True
    
    def validate_data_quality(self, df):
        """
        Perform data quality checks
        """
        print("\n✅ Running data quality checks...")
        
        total_count = df.count()
        
        # Check for nulls in critical columns
        null_customers = df.filter(F.col("customer_id").isNull()).count()
        null_amounts = df.filter(F.col("order_amount").isNull()).count()
        
        null_pct_customers = (null_customers / total_count * 100) if total_count > 0 else 0
        null_pct_amounts = (null_amounts / total_count * 100) if total_count > 0 else 0
        
        print(f"   Null customer_id: {null_customers:,} ({null_pct_customers:.2f}%)")
        print(f"   Null order_amount: {null_amounts:,} ({null_pct_amounts:.2f}%)")
        
        # Calculate quality score
        valid_records = df.filter(
            F.col("customer_id").isNotNull() &
            F.col("order_amount").isNotNull()
        ).count()
        
        quality_score = (valid_records / total_count * 100) if total_count > 0 else 0
        
        print(f"\n   🎯 Quality Score: {quality_score:.2f}%")
        
        # Quality threshold check
        if quality_score < 95:
            print(f"   ⚠️ WARNING: Quality score below threshold (95%)")
        else:
            print(f"   ✅ Quality score meets threshold")
        
        return quality_score, valid_records
    
    def transform_data(self, df):
        """
        Apply business transformations
        """
        print("\n🔄 Applying transformations...")
        
        # Filter out invalid records
        clean_df = df.filter(
            F.col("customer_id").isNotNull() &
            F.col("order_amount").isNotNull() &
            (F.col("order_amount") > 0)
        )
        
        # Add derived columns
        transformed_df = clean_df \
            .withColumn("processed_at", F.current_timestamp()) \
            .withColumn(
                "order_category",
                F.when(F.col("order_amount") < 50, "small")
                .when(F.col("order_amount") < 200, "medium")
                .otherwise("large")
            ) \
            .withColumn(
                "is_high_value",
                F.col("order_amount") > 300
            )
        
        print(f"   ✅ Transformation complete")
        print(f"   Records after cleaning: {transformed_df.count():,}")
        
        return transformed_df
    
    def run(self, source_df, max_retries=3):
        """
        Execute the full pipeline with retry logic
        """
        print(f"\n{'='*60}")
        print(f"🚀 Starting Production Pipeline: {self.pipeline_name}")
        print(f"{'='*60}\n")
        
        # Required columns
        required_columns = ["order_id", "customer_id", "order_amount", "order_status", "order_timestamp"]
        
        for attempt in range(1, max_retries + 1):
            try:
                print(f"\n🔄 Attempt {attempt}/{max_retries}")
                
                # Stage 1: Schema Validation
                self.metrics.start_stage("schema_validation")
                self.validate_schema(source_df, required_columns)
                self.metrics.end_stage()
                
                # Stage 2: Data Quality Check
                self.metrics.start_stage("quality_check")
                quality_score, valid_count = self.validate_data_quality(source_df)
                self.metrics.end_stage(records_processed=valid_count)
                
                # Stage 3: Transformation
                self.metrics.start_stage("transformation")
                transformed_df = self.transform_data(source_df)
                self.metrics.end_stage(records_processed=transformed_df.count())
                
                # Stage 4: Quality Metrics
                self.metrics.add_quality_metrics(transformed_df, "final_output")
                
                # Finalize
                final_metrics = self.metrics.finalize(status="success")
                
                # Check SLA/SLO
                self.check_sla(final_metrics)
                
                print(f"\n✅ Pipeline completed successfully!")
                
                return transformed_df, final_metrics
                
            except Exception as e:
                error_msg = str(e)
                print(f"\n❌ Attempt {attempt} failed: {error_msg}")
                
                if attempt < max_retries:
                    # Exponential backoff
                    wait_time = 2 ** (attempt - 1)
                    print(f"⏳ Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"\n🚨 Pipeline failed after {max_retries} attempts")
                    self.metrics.finalize(status="failed", error_message=error_msg)
                    raise
    
    def check_sla(self, metrics):
        """
        Check if SLA and SLO are met
        """
        duration_minutes = metrics["total_duration_sec"] / 60
        
        print(f"\n🎯 SLA/SLO Check:")
        print(f"   Duration: {duration_minutes:.2f} minutes")
        print(f"   SLO Target: {self.slo_minutes} minutes")
        print(f"   SLA Target: {self.sla_minutes} minutes")
        
        if duration_minutes <= self.slo_minutes:
            print(f"   ✅ SLO MET (within {self.slo_minutes} min)")
        elif duration_minutes <= self.sla_minutes:
            print(f"   ⚠️  SLO BREACHED but SLA met (< {self.sla_minutes} min)")
            print(f"   🚨 ACTION: Investigate performance degradation")
        else:
            print(f"   🚨 SLA BREACHED ({duration_minutes:.2f} > {self.sla_minutes} min)")
            print(f"   🚨 CRITICAL: Immediate escalation required")

# Execute the production pipeline
print("✅ ProductionPipeline class defined")
print("\nReady to run production pipeline...")

# COMMAND ----------

# DBTITLE 1,Step 3: Execute Pipeline
# STEP 3: Execute Production Pipeline

# Create pipeline instance
pipeline = ProductionPipeline(
    pipeline_name="order_processing_etl",
    sla_minutes=30,  # Must complete within 30 minutes
    slo_minutes=20   # Target: complete within 20 minutes
)

# Run the pipeline
result_df, metrics = pipeline.run(source_df, max_retries=3)

# Display results
print("\n" + "="*60)
print("🎯 PIPELINE RESULTS")
print("="*60)

print("\n📊 Final Metrics Summary:")
print(f"   Pipeline: {metrics['pipeline_name']}")
print(f"   Run ID: {metrics['run_id']}")
print(f"   Status: {metrics['status'].upper()}")
print(f"   Duration: {metrics['total_duration_sec']:.2f} seconds")
print(f"   Stages Completed: {len(metrics['stages'])}")

print("\n📊 Stage Breakdown:")
for stage in metrics['stages']:
    print(f"   {stage['stage_name']:20s} | {stage['duration_sec']:6.2f}s | Status: {stage['status']}")

print("\n🔍 Sample output data:")
display(result_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Section 9: End-to-End Production Architecture
# MAGIC %md
# MAGIC ---
# MAGIC # 🏛️ SECTION 9: End-to-End Production Architecture
# MAGIC
# MAGIC ## Complete Production Data Pipeline Design
# MAGIC
# MAGIC ### 📈 Architecture Layers:
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────────────────┐
# MAGIC │                    DATA SOURCES                           │
# MAGIC │   Databases | APIs | Files | Streams | Event Queues      │
# MAGIC └────────────────────────────────────────────────────────┘
# MAGIC                          ⬇️
# MAGIC ┌────────────────────────────────────────────────────────┐
# MAGIC │                  INGESTION LAYER                         │
# MAGIC │                                                          │
# MAGIC │  ✅ Schema Validation                                   │
# MAGIC │  ✅ Connection Retry (Exponential Backoff)             │
# MAGIC │  ✅ Rate Limiting                                       │
# MAGIC │  ✅ Checkpointing (Streaming)                          │
# MAGIC │  ✅ Watermark Tracking (Batch)                         │
# MAGIC └────────────────────────────────────────────────────────┘
# MAGIC                          ⬇️
# MAGIC ┌────────────────────────────────────────────────────────┐
# MAGIC │                PROCESSING LAYER                          │
# MAGIC │                                                          │
# MAGIC │  ✅ Data Quality Checks                                │
# MAGIC │  ✅ Dead Letter Queue (Bad Records)                    │
# MAGIC │  ✅ Business Logic Transformations                     │
# MAGIC │  ✅ Error Handling & Logging                           │
# MAGIC │  ✅ Metrics Collection                                 │
# MAGIC └────────────────────────────────────────────────────────┘
# MAGIC                          ⬇️
# MAGIC ┌────────────────────────────────────────────────────────┐
# MAGIC │                  STORAGE LAYER                           │
# MAGIC │                                                          │
# MAGIC │  ✅ Idempotent Writes (MERGE)                          │
# MAGIC │  ✅ Partitioning Strategy                              │
# MAGIC │  ✅ Delta Lake ACID Transactions                       │
# MAGIC │  ✅ Data Versioning & Time Travel                      │
# MAGIC │  ✅ Optimize & ZORDER                                  │
# MAGIC └────────────────────────────────────────────────────────┘
# MAGIC                          ⬇️
# MAGIC ┌────────────────────────────────────────────────────────┐
# MAGIC │                MONITORING LAYER                          │
# MAGIC │                                                          │
# MAGIC │  📊 Metrics: Latency, Volume, Quality, Cost           │
# MAGIC │  🔔 Alerts: SLA breach, Failures, Anomalies             │
# MAGIC │  📊 Dashboards: Real-time & Historical views            │
# MAGIC │  🔍 Logs: Centralized logging & search                  │
# MAGIC └────────────────────────────────────────────────────────┘
# MAGIC                          ⬇️
# MAGIC ┌────────────────────────────────────────────────────────┐
# MAGIC │              ORCHESTRATION LAYER                         │
# MAGIC │                                                          │
# MAGIC │  🔄 Databricks Jobs (formerly Workflows)                │
# MAGIC │  ⏰ Scheduling & Triggers                                │
# MAGIC │  🔗 Task Dependencies                                   │
# MAGIC │  🔄 Auto-retry Configuration                             │
# MAGIC │  🚨 Failure Notifications                                │
# MAGIC └────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛡️ Production Reliability Checklist:
# MAGIC
# MAGIC #### ✅ **Design Phase**
# MAGIC - [ ] Define SLA/SLO requirements
# MAGIC - [ ] Identify failure modes
# MAGIC - [ ] Plan idempotency strategy
# MAGIC - [ ] Design monitoring approach
# MAGIC - [ ] Document recovery procedures
# MAGIC
# MAGIC #### ✅ **Implementation Phase**
# MAGIC - [ ] Implement retry logic
# MAGIC - [ ] Add data quality checks
# MAGIC - [ ] Create dead letter queue
# MAGIC - [ ] Add comprehensive logging
# MAGIC - [ ] Implement metrics collection
# MAGIC - [ ] Make writes idempotent
# MAGIC - [ ] Add checkpointing/watermarks
# MAGIC
# MAGIC #### ✅ **Testing Phase**
# MAGIC - [ ] Test failure scenarios
# MAGIC - [ ] Verify retry behavior
# MAGIC - [ ] Test recovery mechanisms
# MAGIC - [ ] Validate idempotency
# MAGIC - [ ] Load testing
# MAGIC - [ ] Chaos testing
# MAGIC
# MAGIC #### ✅ **Deployment Phase**
# MAGIC - [ ] Set up monitoring dashboards
# MAGIC - [ ] Configure alerts
# MAGIC - [ ] Document runbooks
# MAGIC - [ ] Train on-call team
# MAGIC - [ ] Canary deployment
# MAGIC
# MAGIC #### ✅ **Operations Phase**
# MAGIC - [ ] Monitor SLA compliance
# MAGIC - [ ] Review error rates
# MAGIC - [ ] Track cost trends
# MAGIC - [ ] Regular performance reviews
# MAGIC - [ ] Continuous improvement
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Key Takeaways:
# MAGIC
# MAGIC 1. **Design for Failure**: Systems will fail — plan for it
# MAGIC 2. **Idempotency is Critical**: Enables safe retries
# MAGIC 3. **Monitor Everything**: You can't fix what you can't see
# MAGIC 4. **SLAs Drive Design**: Requirements shape architecture
# MAGIC 5. **Iterate & Improve**: Use metrics to guide optimization

# COMMAND ----------

# DBTITLE 1,Genie Code Agent Usage
# MAGIC %md
# MAGIC ---
# MAGIC # 🤖 Genie Code Agent Usage
# MAGIC
# MAGIC ## How to Use Genie Code for Production Patterns
# MAGIC
# MAGIC ### 💬 Sample Prompts:
# MAGIC
# MAGIC #### **Retry Logic**
# MAGIC ```
# MAGIC ➡️ "Add exponential backoff retry logic to my pipeline"
# MAGIC ➡️ "Implement a retry decorator with 3 attempts"
# MAGIC ➡️ "Add conditional retry that only retries on timeout errors"
# MAGIC ➡️ "Show me how to implement circuit breaker pattern"
# MAGIC ```
# MAGIC
# MAGIC #### **Failure Handling**
# MAGIC ```
# MAGIC ➡️ "Add try-catch blocks to handle missing tables"
# MAGIC ➡️ "Handle schema validation errors gracefully"
# MAGIC ➡️ "Create a dead letter queue for invalid records"
# MAGIC ➡️ "Implement fallback logic if primary source fails"
# MAGIC ```
# MAGIC
# MAGIC #### **SLA Monitoring**
# MAGIC ```
# MAGIC ➡️ "Track pipeline execution time and check against SLA"
# MAGIC ➡️ "Add code to monitor data freshness"
# MAGIC ➡️ "Calculate and log quality score for each run"
# MAGIC ➡️ "Implement SLO breach detection"
# MAGIC ```
# MAGIC
# MAGIC #### **Metrics Collection**
# MAGIC ```
# MAGIC ➡️ "Add comprehensive metrics collection to my pipeline"
# MAGIC ➡️ "Track row counts at each stage"
# MAGIC ➡️ "Log runtime metrics for performance analysis"
# MAGIC ➡️ "Create a metrics class for my production pipeline"
# MAGIC ```
# MAGIC
# MAGIC #### **Idempotent Operations**
# MAGIC ```
# MAGIC ➡️ "Make my write operation idempotent using MERGE"
# MAGIC ➡️ "Add idempotency key to my dataframe"
# MAGIC ➡️ "Convert append to merge for idempotency"
# MAGIC ➡️ "Show me idempotent patterns for Delta Lake"
# MAGIC ```
# MAGIC
# MAGIC #### **Recovery Mechanisms**
# MAGIC ```
# MAGIC ➡️ "Implement watermark-based recovery"
# MAGIC ➡️ "Add checkpoint tracking for batch processing"
# MAGIC ➡️ "Create partition-level retry logic"
# MAGIC ➡️ "Track processing status for recovery"
# MAGIC ```
# MAGIC
# MAGIC #### **Data Quality**
# MAGIC ```
# MAGIC ➡️ "Add data quality checks for null values"
# MAGIC ➡️ "Validate schema before processing"
# MAGIC ➡️ "Separate valid and invalid records"
# MAGIC ➡️ "Calculate data quality score"
# MAGIC ```
# MAGIC
# MAGIC #### **Complete Production Pipeline**
# MAGIC ```
# MAGIC ➡️ "Build a production pipeline with retry, validation, and monitoring"
# MAGIC ➡️ "Add all reliability patterns to my existing pipeline"
# MAGIC ➡️ "Create a resilient ETL pipeline with SLA tracking"
# MAGIC ➡️ "Refactor my code to follow production best practices"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Best Practices for Prompts:
# MAGIC
# MAGIC 1. **Be Specific**: Mention the exact pattern you need
# MAGIC 2. **Provide Context**: Share your current code or scenario
# MAGIC 3. **State Constraints**: Mention serverless, Unity Catalog, etc.
# MAGIC 4. **Ask for Explanations**: Request both code and explanation
# MAGIC 5. **Iterate**: Refine based on initial results
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Pro Tips:
# MAGIC
# MAGIC * **Combine Patterns**: "Add retry logic AND metrics collection"
# MAGIC * **Ask for Testing**: "How do I test my retry logic?"
# MAGIC * **Request Documentation**: "Document the failure scenarios"
# MAGIC * **Get Examples**: "Show me production examples from similar pipelines"
# MAGIC * **Optimize**: "How can I make this more resilient?"

# COMMAND ----------

# DBTITLE 1,Final Summary & Interview Questions
# MAGIC %md
# MAGIC ---
# MAGIC # 🏆 Final Summary
# MAGIC
# MAGIC ## 📚 Key Learnings
# MAGIC
# MAGIC ### 1️⃣ **Retry Strategies**
# MAGIC * Fixed retry for simple transient failures
# MAGIC * Exponential backoff for system-level issues
# MAGIC * Conditional retry for error type differentiation
# MAGIC * Circuit breaker for protecting failing systems
# MAGIC
# MAGIC ### 2️⃣ **Failure Handling**
# MAGIC * Categorize failures: Data, System, Dependency, Logic
# MAGIC * Handle each category appropriately
# MAGIC * Use dead letter queues for bad data
# MAGIC * Implement graceful degradation
# MAGIC
# MAGIC ### 3️⃣ **Recovery Mechanisms**
# MAGIC * Checkpoints for streaming (Structured Streaming)
# MAGIC * Watermarks for batch processing
# MAGIC * Idempotent operations for safe retries
# MAGIC * State management strategies
# MAGIC
# MAGIC ### 4️⃣ **SLA/SLO Monitoring**
# MAGIC * SLA: Customer commitments (contractual)
# MAGIC * SLO: Internal targets (buffer zone)
# MAGIC * SLI: Measurable metrics
# MAGIC * Error budgets for reliability balancing
# MAGIC
# MAGIC ### 5️⃣ **Metrics & Monitoring**
# MAGIC * Four Golden Signals: Latency, Traffic, Errors, Saturation
# MAGIC * Track runtime, volume, quality, and cost
# MAGIC * Build dashboards for visibility
# MAGIC * Set up proactive alerts
# MAGIC
# MAGIC ### 6️⃣ **Production Architecture**
# MAGIC * Design for failure from the start
# MAGIC * Layer your reliability patterns
# MAGIC * Monitor every layer
# MAGIC * Document runbooks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Interview Questions (10 Questions)
# MAGIC
# MAGIC ### Basic Level:
# MAGIC
# MAGIC **Q1: What is the difference between SLA and SLO?**
# MAGIC <details>
# MAGIC <summary>Answer</summary>
# MAGIC
# MAGIC * **SLA** (Service Level Agreement): External commitment to customers, often contractual with penalties
# MAGIC * **SLO** (Service Level Objective): Internal target, more strict than SLA to provide buffer
# MAGIC * **Relationship**: SLO < SLA (SLO is achieved before SLA is met)
# MAGIC </details>
# MAGIC
# MAGIC **Q2: Why is idempotency important in data pipelines?**
# MAGIC <details>
# MAGIC <summary>Answer</summary>
# MAGIC
# MAGIC * Enables safe retries without data duplication
# MAGIC * Same operation can run multiple times with same result
# MAGIC * Critical for exactly-once semantics
# MAGIC * Example: MERGE instead of APPEND
# MAGIC </details>
# MAGIC
# MAGIC **Q3: What is exponential backoff and when would you use it?**
# MAGIC <details>
# MAGIC <summary>Answer</summary>
# MAGIC
# MAGIC * Retry strategy where wait time doubles each attempt (1s, 2s, 4s, 8s...)
# MAGIC * Use for: API rate limiting, system recovery, external dependencies
# MAGIC * Benefits: Reduces load on failing system, allows recovery time
# MAGIC </details>
# MAGIC
# MAGIC ### Intermediate Level:
# MAGIC
# MAGIC **Q4: How do you implement checkpoint-based recovery in Spark Structured Streaming?**
# MAGIC <details>
# MAGIC <summary>Answer</summary>
# MAGIC
# MAGIC ```python
# MAGIC df.writeStream \
# MAGIC   .format("delta") \
# MAGIC   .option("checkpointLocation", "/path/to/checkpoint") \
# MAGIC   .start()
# MAGIC ```
# MAGIC * Checkpoint stores offset information
# MAGIC * Enables exactly-once processing
# MAGIC * Stream resumes from last committed offset on failure
# MAGIC </details>
# MAGIC
# MAGIC **Q5: What is a dead letter queue and when should you use it?**
# MAGIC <details>
# MAGIC <summary>Answer</summary>
# MAGIC
# MAGIC * Separate storage for records that fail validation/processing
# MAGIC * Use when: Data quality issues, schema mismatches, business rule violations
# MAGIC * Benefits: Don't block entire pipeline, preserve bad data for analysis
# MAGIC * Pattern: Filter invalid records, write to DLQ table
# MAGIC </details>
# MAGIC
# MAGIC **Q6: Explain the circuit breaker pattern.**
# MAGIC <details>
# MAGIC <summary>Answer</summary>
# MAGIC
# MAGIC * Pattern that stops trying when threshold of failures is reached
# MAGIC * States: Closed (normal) → Open (stop trying) → Half-Open (test recovery)
# MAGIC * Protects failing downstream systems
# MAGIC * Provides fast failure during outages
# MAGIC </details>
# MAGIC
# MAGIC ### Advanced Level:
# MAGIC
# MAGIC **Q7: How do you ensure exactly-once processing in a batch pipeline?**
# MAGIC <details>
# MAGIC <summary>Answer</summary>
# MAGIC
# MAGIC 1. **Idempotent writes**: Use MERGE with business keys
# MAGIC 2. **Watermark tracking**: Track what's been processed
# MAGIC 3. **Transactional writes**: Use Delta Lake ACID guarantees
# MAGIC 4. **Idempotency keys**: Hash of business keys for deduplication
# MAGIC </details>
# MAGIC
# MAGIC **Q8: Design a retry strategy for a pipeline that reads from an unreliable API.**
# MAGIC <details>
# MAGIC <summary>Answer</summary>
# MAGIC
# MAGIC ```python
# MAGIC * Use exponential backoff (2^attempt seconds)
# MAGIC * Max retries: 5 attempts
# MAGIC * Conditional retry:
# MAGIC   - Retry: Timeout, 5xx errors, rate limit
# MAGIC   - Don't retry: 4xx client errors, auth failures
# MAGIC * Circuit breaker after 10 consecutive failures
# MAGIC * Fallback to cached data if available
# MAGIC ```
# MAGIC </details>
# MAGIC
# MAGIC **Q9: What metrics would you track for a production data pipeline?**
# MAGIC <details>
# MAGIC <summary>Answer</summary>
# MAGIC
# MAGIC **Runtime Metrics**: Duration, P95 latency, stage timing
# MAGIC **Volume Metrics**: Records in/out, data size, delta changes
# MAGIC **Quality Metrics**: Null rates, duplicate count, constraint violations
# MAGIC **Reliability Metrics**: Success rate, MTBF, MTTR, retry count
# MAGIC **Cost Metrics**: DBU consumption, storage costs, cost per record
# MAGIC </details>
# MAGIC
# MAGIC **Q10: How would you handle schema evolution in a production pipeline?**
# MAGIC <details>
# MAGIC <summary>Answer</summary>
# MAGIC
# MAGIC 1. **Detect**: Schema validation at ingestion
# MAGIC 2. **Handle**:
# MAGIC    * Additive changes (new columns): Auto-handle with mergeSchema
# MAGIC    * Deletive changes (removed columns): Alert and manual review
# MAGIC    * Type changes: Version the schema, maintain compatibility
# MAGIC 3. **Communicate**: Alert stakeholders of schema changes
# MAGIC 4. **Version**: Use Delta Lake schema evolution features
# MAGIC 5. **Test**: Canary deployments for schema changes
# MAGIC </details>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Common Mistakes
# MAGIC
# MAGIC ### 1️⃣ **No Retry Mechanism**
# MAGIC * **Mistake**: Pipeline fails on first transient error
# MAGIC * **Fix**: Implement retry with exponential backoff
# MAGIC * **Impact**: Frequent manual interventions, poor reliability
# MAGIC
# MAGIC ### 2️⃣ **Non-Idempotent Operations**
# MAGIC * **Mistake**: Using APPEND mode for writes
# MAGIC * **Fix**: Use MERGE for idempotent writes
# MAGIC * **Impact**: Data duplication on retries
# MAGIC
# MAGIC ### 3️⃣ **Ignoring SLAs**
# MAGIC * **Mistake**: No monitoring of execution time or freshness
# MAGIC * **Fix**: Track duration, set alerts on SLO breach
# MAGIC * **Impact**: SLA violations, customer dissatisfaction
# MAGIC
# MAGIC ### 4️⃣ **Poor Error Handling**
# MAGIC * **Mistake**: Generic exception handling without categorization
# MAGIC * **Fix**: Handle different error types appropriately
# MAGIC * **Impact**: Retry on permanent errors, waste resources
# MAGIC
# MAGIC ### 5️⃣ **No Monitoring**
# MAGIC * **Mistake**: No metrics, no alerts, blind to issues
# MAGIC * **Fix**: Implement comprehensive monitoring
# MAGIC * **Impact**: Reactive firefighting, late issue detection
# MAGIC
# MAGIC ### 6️⃣ **Missing Data Quality Checks**
# MAGIC * **Mistake**: Process bad data without validation
# MAGIC * **Fix**: Add quality checks, use DLQ
# MAGIC * **Impact**: Garbage in, garbage out
# MAGIC
# MAGIC ### 7️⃣ **No Recovery Strategy**
# MAGIC * **Mistake**: Full reprocessing on every failure
# MAGIC * **Fix**: Implement checkpoints/watermarks
# MAGIC * **Impact**: Wasted compute, long recovery times
# MAGIC
# MAGIC ### 8️⃣ **Alert Fatigue**
# MAGIC * **Mistake**: Too many low-priority alerts
# MAGIC * **Fix**: Proper alert severity, deduplication
# MAGIC * **Impact**: Critical alerts ignored
# MAGIC
# MAGIC ### 9️⃣ **No Documentation**
# MAGIC * **Mistake**: No runbooks for failures
# MAGIC * **Fix**: Document procedures, create runbooks
# MAGIC * **Impact**: Slow incident response
# MAGIC
# MAGIC ### 🔟 **Lack of Testing**
# MAGIC * **Mistake**: No testing of failure scenarios
# MAGIC * **Fix**: Test retries, failures, edge cases
# MAGIC * **Impact**: Surprises in production
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Next Steps
# MAGIC
# MAGIC 1. **Practice**: Build your own production pipeline
# MAGIC 2. **Experiment**: Test failure scenarios
# MAGIC 3. **Monitor**: Set up dashboards and alerts
# MAGIC 4. **Learn**: Study incident post-mortems
# MAGIC 5. **Iterate**: Continuously improve reliability
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Resources
# MAGIC
# MAGIC * Databricks Workflows (Job Scheduling)
# MAGIC * Delta Lake Documentation (ACID, Time Travel)
# MAGIC * Site Reliability Engineering (SRE) Book
# MAGIC * Production Data Engineering best practices
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏆 Completion Badge: **Production Reliability Expert**
# MAGIC
# MAGIC **You've learned:**
# MAGIC * ✅ Retry strategies
# MAGIC * ✅ Failure handling
# MAGIC * ✅ Recovery mechanisms  
# MAGIC * ✅ SLA/SLO monitoring
# MAGIC * ✅ Metrics & alerting
# MAGIC * ✅ Production architecture
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👏 Congratulations!
# MAGIC You're now equipped to build production-grade, resilient data pipelines!
# MAGIC
# MAGIC **Author: TRRaveendra | @TRRaveendra**

# COMMAND ----------

# DBTITLE 1,BONUS: Alerting Workflow Integration
# MAGIC %md
# MAGIC ---
# MAGIC # 🔔 BONUS: Alerting Workflow Integration
# MAGIC
# MAGIC ## Conceptual Overview of Alert Integration
# MAGIC
# MAGIC ### 1️⃣ **Databricks Jobs Notification (Built-in)**
# MAGIC
# MAGIC **Setup in Databricks Workflows:**
# MAGIC ```yaml
# MAGIC Job Configuration:
# MAGIC   Email Notifications:
# MAGIC     - On Start: team@company.com
# MAGIC     - On Success: engineering@company.com  
# MAGIC     - On Failure: oncall@company.com, manager@company.com
# MAGIC   
# MAGIC   Webhook Notifications:
# MAGIC     - On Failure: https://hooks.slack.com/services/YOUR/WEBHOOK/URL
# MAGIC ```
# MAGIC
# MAGIC **Benefits:**
# MAGIC * No code required
# MAGIC * Built into Databricks Jobs
# MAGIC * Automatic job context
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ **AWS SNS Integration (Conceptual)**
# MAGIC
# MAGIC **Pattern:**
# MAGIC ```python
# MAGIC import boto3
# MAGIC
# MAGIC def send_sns_alert(topic_arn, subject, message):
# MAGIC     """
# MAGIC     Send alert via AWS SNS
# MAGIC     """
# MAGIC     sns_client = boto3.client('sns', region_name='us-east-1')
# MAGIC     
# MAGIC     response = sns_client.publish(
# MAGIC         TopicArn=topic_arn,
# MAGIC         Subject=subject,
# MAGIC         Message=message
# MAGIC     )
# MAGIC     
# MAGIC     return response
# MAGIC
# MAGIC # Usage
# MAGIC topic_arn = "arn:aws:sns:us-east-1:123456789:pipeline-alerts"
# MAGIC alert_message = f"""
# MAGIC Pipeline: {pipeline_name}
# MAGIC Status: FAILED
# MAGIC Error: {error_message}
# MAGIC Timestamp: {timestamp}
# MAGIC """
# MAGIC
# MAGIC send_sns_alert(topic_arn, "Pipeline Failure", alert_message)
# MAGIC ```
# MAGIC
# MAGIC **SNS Subscribers:**
# MAGIC * Email
# MAGIC * SMS
# MAGIC * Lambda functions
# MAGIC * SQS queues
# MAGIC * HTTP/HTTPS endpoints
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ **Slack Webhook Integration**
# MAGIC
# MAGIC **Pattern:**
# MAGIC ```python
# MAGIC import requests
# MAGIC import json
# MAGIC
# MAGIC def send_slack_alert(webhook_url, pipeline_name, status, details):
# MAGIC     """
# MAGIC     Send alert to Slack channel
# MAGIC     """
# MAGIC     # Color coding
# MAGIC     color = "danger" if status == "FAILED" else "good"
# MAGIC     
# MAGIC     payload = {
# MAGIC         "attachments": [
# MAGIC             {
# MAGIC                 "color": color,
# MAGIC                 "title": f"🚨 Pipeline Alert: {pipeline_name}",
# MAGIC                 "fields": [
# MAGIC                     {"title": "Status", "value": status, "short": True},
# MAGIC                     {"title": "Timestamp", "value": details.get("timestamp"), "short": True},
# MAGIC                     {"title": "Duration", "value": f"{details.get('duration')} sec", "short": True},
# MAGIC                     {"title": "Error", "value": details.get("error", "N/A"), "short": False}
# MAGIC                 ],
# MAGIC                 "footer": "Databricks Production Pipeline"
# MAGIC             }
# MAGIC         ]
# MAGIC     }
# MAGIC     
# MAGIC     response = requests.post(
# MAGIC         webhook_url,
# MAGIC         data=json.dumps(payload),
# MAGIC         headers={'Content-Type': 'application/json'}
# MAGIC     )
# MAGIC     
# MAGIC     return response.status_code
# MAGIC
# MAGIC # Usage
# MAGIC webhook_url = "https://hooks.slack.com/services/T00/B00/XXXX"
# MAGIC details = {
# MAGIC     "timestamp": "2026-04-21 08:30:00",
# MAGIC     "duration": 120,
# MAGIC     "error": "Table not found"
# MAGIC }
# MAGIC
# MAGIC send_slack_alert(webhook_url, "customer_etl", "FAILED", details)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4️⃣ **PagerDuty Integration**
# MAGIC
# MAGIC **Pattern:**
# MAGIC ```python
# MAGIC import requests
# MAGIC
# MAGIC def trigger_pagerduty_incident(integration_key, severity, summary, details):
# MAGIC     """
# MAGIC     Trigger PagerDuty incident for critical failures
# MAGIC     """
# MAGIC     url = "https://events.pagerduty.com/v2/enqueue"
# MAGIC     
# MAGIC     payload = {
# MAGIC         "routing_key": integration_key,
# MAGIC         "event_action": "trigger",
# MAGIC         "payload": {
# MAGIC             "summary": summary,
# MAGIC             "severity": severity,  # critical, error, warning, info
# MAGIC             "source": "databricks_pipeline",
# MAGIC             "custom_details": details
# MAGIC         }
# MAGIC     }
# MAGIC     
# MAGIC     response = requests.post(url, json=payload)
# MAGIC     return response.json()
# MAGIC
# MAGIC # Usage - only for P0/P1
# MAGIC if sla_breached or critical_failure:
# MAGIC     trigger_pagerduty_incident(
# MAGIC         integration_key="YOUR_INTEGRATION_KEY",
# MAGIC         severity="critical",
# MAGIC         summary="Pipeline SLA Breach: customer_etl",
# MAGIC         details={
# MAGIC             "pipeline": "customer_etl",
# MAGIC             "sla_minutes": 30,
# MAGIC             "actual_minutes": 45,
# MAGIC             "run_id": run_id
# MAGIC         }
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5️⃣ **Generic Webhook Pattern**
# MAGIC
# MAGIC **Pattern:**
# MAGIC ```python
# MAGIC def send_webhook_alert(webhook_url, payload):
# MAGIC     """
# MAGIC     Generic webhook sender
# MAGIC     """
# MAGIC     try:
# MAGIC         response = requests.post(
# MAGIC             webhook_url,
# MAGIC             json=payload,
# MAGIC             headers={'Content-Type': 'application/json'},
# MAGIC             timeout=10
# MAGIC         )
# MAGIC         
# MAGIC         if response.status_code == 200:
# MAGIC             print("✅ Alert sent successfully")
# MAGIC         else:
# MAGIC             print(f"❌ Alert failed: {response.status_code}")
# MAGIC             
# MAGIC         return response
# MAGIC     except Exception as e:
# MAGIC         print(f"❌ Webhook failed: {str(e)}")
# MAGIC         # Don't fail pipeline if alerting fails
# MAGIC         return None
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6️⃣ **Alert Orchestration Example**
# MAGIC
# MAGIC **Complete alerting in pipeline:**
# MAGIC ```python
# MAGIC class ProductionPipelineWithAlerts(ProductionPipeline):
# MAGIC     """
# MAGIC     Extended pipeline with alerting
# MAGIC     """
# MAGIC     
# MAGIC     def __init__(self, pipeline_name, alert_config):
# MAGIC         super().__init__(pipeline_name)
# MAGIC         self.alert_config = alert_config
# MAGIC     
# MAGIC     def send_alert(self, severity, message, details):
# MAGIC         """
# MAGIC         Send alert based on severity
# MAGIC         """
# MAGIC         if severity == "P0":
# MAGIC             # Critical: Page on-call
# MAGIC             trigger_pagerduty_incident(...)
# MAGIC             send_slack_alert(...)
# MAGIC             send_sns_alert(...)
# MAGIC         elif severity == "P1":
# MAGIC             # High: Notify team
# MAGIC             send_slack_alert(...)
# MAGIC             send_sns_alert(...)
# MAGIC         elif severity == "P2":
# MAGIC             # Medium: Log and email
# MAGIC             send_email_alert(...)
# MAGIC         else:
# MAGIC             # Low: Just log
# MAGIC             print(f"⚠️  {message}")
# MAGIC     
# MAGIC     def check_sla_and_alert(self, metrics):
# MAGIC         """
# MAGIC         Check SLA and send appropriate alerts
# MAGIC         """
# MAGIC         duration_min = metrics["total_duration_sec"] / 60
# MAGIC         
# MAGIC         if duration_min > self.sla_minutes:
# MAGIC             # SLA breach - P0
# MAGIC             self.send_alert(
# MAGIC                 severity="P0",
# MAGIC                 message=f"SLA BREACH: {self.pipeline_name}",
# MAGIC                 details=metrics
# MAGIC             )
# MAGIC         elif duration_min > self.slo_minutes:
# MAGIC             # SLO breach - P1  
# MAGIC             self.send_alert(
# MAGIC                 severity="P1",
# MAGIC                 message=f"SLO breach: {self.pipeline_name}",
# MAGIC                 details=metrics
# MAGIC             )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Dashboard Concepts
# MAGIC
# MAGIC ### Monitoring Dashboard Panels:
# MAGIC
# MAGIC 1. **Pipeline Health**
# MAGIC    * Success rate (last 24h, 7d, 30d)
# MAGIC    * Current status
# MAGIC    * Last run timestamp
# MAGIC
# MAGIC 2. **Performance Metrics**
# MAGIC    * Execution time trend
# MAGIC    * P50/P95/P99 latency
# MAGIC    * SLA compliance rate
# MAGIC
# MAGIC 3. **Volume Metrics**
# MAGIC    * Records processed trend
# MAGIC    * Data size trend
# MAGIC    * Throughput (records/sec)
# MAGIC
# MAGIC 4. **Quality Metrics**
# MAGIC    * Quality score trend
# MAGIC    * Error rate
# MAGIC    * DLQ volume
# MAGIC
# MAGIC 5. **Cost Metrics**
# MAGIC    * DBU consumption
# MAGIC    * Cost per run
# MAGIC    * Cost trend
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Sample Dashboard Query:
# MAGIC
# MAGIC ```sql
# MAGIC -- Pipeline performance over time
# MAGIC SELECT 
# MAGIC     DATE(start_time) as run_date,
# MAGIC     pipeline_name,
# MAGIC     COUNT(*) as total_runs,
# MAGIC     SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_runs,
# MAGIC     ROUND(AVG(total_duration_sec), 2) as avg_duration_sec,
# MAGIC     ROUND(PERCENTILE(total_duration_sec, 0.95), 2) as p95_duration_sec,
# MAGIC     SUM(CASE WHEN total_duration_sec/60 > 30 THEN 1 ELSE 0 END) as sla_breaches
# MAGIC FROM pipeline_metrics
# MAGIC WHERE start_time >= CURRENT_DATE - INTERVAL 30 DAYS
# MAGIC GROUP BY run_date, pipeline_name
# MAGIC ORDER BY run_date DESC
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Key Takeaway:
# MAGIC
# MAGIC **Alerting should be:**
# MAGIC * ✅ Actionable
# MAGIC * ✅ Properly categorized by severity
# MAGIC * ✅ Context-rich
# MAGIC * ✅ Non-blocking (don't fail pipeline if alert fails)
# MAGIC * ✅ Tested regularly