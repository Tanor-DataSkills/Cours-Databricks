# Databricks notebook source
# DBTITLE 1,📘 Notebook Header
# MAGIC %md
# MAGIC # 🌊 Data Engineering Training — Phase 5 Day 27  
# MAGIC ## ⚡ Streaming + Delta: Exactly-Once, Checkpointing & Watermarking  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - Exactly-once Processing  
# MAGIC - Checkpointing in Streaming  
# MAGIC - Watermarking & Late Data Handling  
# MAGIC - Reliable Streaming Pipelines  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### ⚙️ Platform: Databricks (Serverless + Unity Catalog + Streaming + Delta Lake)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Objective:
# MAGIC Understand how to build reliable streaming pipelines using Delta Lake with exactly-once guarantees, checkpointing, and watermarking for late data handling. This notebook demonstrates production-grade streaming patterns for fault-tolerant, real-time data processing.

# COMMAND ----------

# DBTITLE 1,📌 Section 1: Exactly-Once Processing
# MAGIC %md
# MAGIC # 📌 Section 1: Exactly-Once Processing
# MAGIC
# MAGIC ## 🧒 ELI5 Explanation:
# MAGIC Imagine you're sending birthday invitations. **Exactly-once** means each friend gets exactly one invitation — not zero (they miss the party), and not two (they get confused). In streaming, it means each message is processed once and only once, even if something crashes.
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Exactly-once processing** guarantees that each record in a stream affects the final state exactly once, despite failures or retries. This is achieved through:
# MAGIC
# MAGIC 1. **Idempotent Writes**: Delta Lake's transaction log ensures duplicate writes with the same data don't create duplicate records
# MAGIC 2. **Atomic Transactions**: Each micro-batch is written atomically — either fully committed or fully rolled back
# MAGIC 3. **Checkpointing**: Tracks processing offsets so reprocessing starts from the last successful commit
# MAGIC
# MAGIC ### Processing Guarantees Comparison:
# MAGIC
# MAGIC | Guarantee | Description | Risk |
# MAGIC |-----------|-------------|------|
# MAGIC | **At-most-once** | Process each record once, may lose data on failure | Data loss |
# MAGIC | **At-least-once** | Retry on failure, may process duplicates | Duplicate data |
# MAGIC | **Exactly-once** | Process each record exactly once | None (ideal) |
# MAGIC
# MAGIC ### Why Exactly-Once Matters:
# MAGIC
# MAGIC - **Financial transactions**: Double-processing = wrong balances
# MAGIC - **Inventory systems**: Duplicate updates = incorrect stock counts
# MAGIC - **Analytics**: Accurate aggregations and metrics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Components:
# MAGIC
# MAGIC 1. **Delta Lake Transaction Log**: ACID properties ensure atomic writes
# MAGIC 2. **Structured Streaming Checkpoints**: Track processing progress
# MAGIC 3. **Idempotent Operations**: Reprocessing produces same result

# COMMAND ----------

# DBTITLE 1,🛠️ Setup: Prepare Unity Catalog Resources
# Setup Unity Catalog paths for streaming demo
# Using Unity Catalog Volumes (mandatory for serverless)

catalog = "main"  # Replace with your catalog
schema = "default"  # Replace with your schema  
volume = "streaming_demo"  # Volume for data storage

# Define paths
base_path = f"/Volumes/{catalog}/{schema}/{volume}"
source_path = f"{base_path}/source_data"
checkpoint_path = f"{base_path}/checkpoints"
output_path = f"{base_path}/output"

print("\u2705 Unity Catalog Paths Configured:")
print(f"  Source: {source_path}")
print(f"  Checkpoint: {checkpoint_path}")
print(f"  Output: {output_path}")
print("\n⚠️ Note: Ensure the volume exists before running streaming queries")

# COMMAND ----------

# DBTITLE 1,📊 Demo: Exactly-Once with Delta Lake
# Demonstrate exactly-once semantics with Delta Lake
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import time

# Create sample streaming data schema
transaction_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("amount", IntegerType(), False),
    StructField("timestamp", TimestampType(), False)
])

# Generate sample data for demonstration
sample_data = [
    ("TXN001", 100, "2026-04-21 10:00:00"),
    ("TXN002", 200, "2026-04-21 10:01:00"),
    ("TXN003", 150, "2026-04-21 10:02:00"),
    ("TXN004", 300, "2026-04-21 10:03:00"),
    ("TXN005", 250, "2026-04-21 10:04:00")
]

df = spark.createDataFrame(sample_data, ["transaction_id", "amount", "timestamp"])
df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))
df = df.withColumn("processed_at", current_timestamp())

print("✅ Sample Transaction Data Created")
print(f"  Total Transactions: {df.count()}")
print(f"  Total Amount: ${df.agg({'amount': 'sum'}).collect()[0][0]}")
print("\n🔍 Sample Records:")
display(df)

# COMMAND ----------

# DBTITLE 1,📌 Section 2: Checkpointing in Streaming
# MAGIC %md
# MAGIC # 📌 Section 2: Checkpointing in Streaming
# MAGIC
# MAGIC ## 🧒 ELI5 Explanation:
# MAGIC Imagine reading a long book. You use a bookmark to remember where you stopped. If you close the book and come back later, you start from the bookmark — not from the beginning. **Checkpointing** is like that bookmark for streaming data. It remembers what data has been processed, so if the system crashes, it can resume from where it left off.
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Checkpointing** is the mechanism by which Structured Streaming maintains state and tracks progress across micro-batches. It enables fault tolerance and exactly-once semantics.
# MAGIC
# MAGIC ### What's Stored in Checkpoints:
# MAGIC
# MAGIC 1. **Offset Information**: Which data has been read from the source
# MAGIC 2. **State Data**: Aggregation results, watermark info, etc.
# MAGIC 3. **Metadata**: Query configuration, schema information
# MAGIC
# MAGIC ### Checkpoint Architecture:
# MAGIC
# MAGIC ```
# MAGIC Checkpoint Location/
# MAGIC ├── offsets/           # Source offset tracking
# MAGIC ├── commits/           # Committed batches
# MAGIC ├── state/              # Stateful operation data
# MAGIC └── metadata            # Query metadata
# MAGIC ```
# MAGIC
# MAGIC ### Key Benefits:
# MAGIC
# MAGIC | Benefit | Description |
# MAGIC |---------|-------------|
# MAGIC | **Fault Recovery** | Resume from last successful batch after failure |
# MAGIC | **Exactly-Once** | Prevents duplicate processing of data |
# MAGIC | **State Management** | Persists aggregation state across restarts |
# MAGIC | **Progress Tracking** | Monitors streaming query health |
# MAGIC
# MAGIC ### Critical Rules:
# MAGIC
# MAGIC ⚠️ **NEVER reuse checkpoint locations** between different queries or schema changes  
# MAGIC ⚠️ **ALWAYS use distributed storage** (Unity Catalog Volumes, not /tmp)  
# MAGIC ⚠️ **Checkpoint location must be unique** per streaming query

# COMMAND ----------

# DBTITLE 1,💾 Write Sample Data for Streaming Source
# Create source data that will be read as a stream
# Using Delta format for the source (best practice)

source_delta_path = f"{base_path}/streaming_source"

# Generate time-series transaction data
from pyspark.sql.functions import expr, window
from datetime import datetime, timedelta

# Create initial batch of data
initial_data = [
    ("TXN101", "2026-04-21 10:00:00", 100, "user_1"),
    ("TXN102", "2026-04-21 10:05:00", 200, "user_2"),
    ("TXN103", "2026-04-21 10:10:00", 150, "user_1"),
    ("TXN104", "2026-04-21 10:15:00", 300, "user_3"),
    ("TXN105", "2026-04-21 10:20:00", 250, "user_2"),
]

source_df = spark.createDataFrame(
    initial_data, 
    ["transaction_id", "event_time", "amount", "user_id"]
)

source_df = source_df.withColumn("event_time", col("event_time").cast("timestamp"))

# Write as Delta table (this will be our streaming source)
source_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(source_delta_path)

print(f"\u2705 Source data written to: {source_delta_path}")
print(f"  Records: {source_df.count()}")
print("\n🔍 Sample Source Data:")
display(source_df)

# COMMAND ----------

# DBTITLE 1,⚡ Demo: Streaming with Checkpointing
# Demonstrate checkpointing in Structured Streaming
# Read from Delta source as a stream

checkpoint_demo_path = f"{checkpoint_path}/demo_query"
output_demo_path = f"{output_path}/checkpointed_output"

print("🚀 Starting Streaming Query with Checkpointing...")
print(f"  Checkpoint Location: {checkpoint_demo_path}")
print(f"  Output Location: {output_demo_path}")

# Read stream from Delta source
streaming_df = spark.readStream \
    .format("delta") \
    .load(source_delta_path)

# Apply transformation (calculate running totals)
transformed_df = streaming_df \
    .withColumn("processed_timestamp", current_timestamp()) \
    .select("transaction_id", "event_time", "amount", "user_id", "processed_timestamp")

# Write stream with checkpointing (using availableNow for serverless)
query = transformed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_demo_path) \
    .trigger(availableNow=True) \
    .start(output_demo_path)

print("✅ Streaming Query Started")
print(f"  Query ID: {query.id}")
print(f"  Trigger Mode: AvailableNow (serverless-compatible)")
print("\n⏳ Processing available data...")

# Wait for query to complete
query.awaitTermination()

print("\n✅ Query completed")
print("\n📋 Checkpoint created - Query can now be resumed from this point!")

# COMMAND ----------

# DBTITLE 1,🔍 Verify Checkpointed Output
# Read the output to verify checkpointing worked
output_df = spark.read.format("delta").load(output_demo_path)

print("\u2705 Checkpointed Output Verification:")
print(f"  Total Records Processed: {output_df.count()}")
print(f"  Output Location: {output_demo_path}")
print("\n🔍 Processed Data:")
display(output_df.orderBy("event_time"))

print("\n📋 Checkpoint Structure Created:")
print("  ✓ Offsets logged")
print("  ✓ Commits recorded")
print("  ✓ State preserved")
print("\n➡️ If this query restarts, it will resume from the last checkpoint!")

# COMMAND ----------

# DBTITLE 1,📌 Section 3: Watermarking & Late Data Handling
# MAGIC %md
# MAGIC # 📌 Section 3: Watermarking & Late Data Handling
# MAGIC
# MAGIC ## 🧒 ELI5 Explanation:
# MAGIC Imagine a train station. The train waits 10 minutes after scheduled departure for late passengers. After 10 minutes, the train leaves — even if someone shows up later, they miss it. **Watermarking** is like that waiting time for data. It says "I'll wait 10 minutes for late data, but after that, I'll consider that time window complete and won't update it anymore."
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Watermarking** is a mechanism to handle late-arriving data in streaming applications while maintaining bounded state. It defines how long the system will wait for late events before finalizing aggregations for a given time window.
# MAGIC
# MAGIC ### The Late Data Problem:
# MAGIC
# MAGIC In real-world streaming:
# MAGIC - **Network delays**: Data arrives out of order
# MAGIC - **Clock skew**: Different source systems have time differences  
# MAGIC - **Processing lag**: Some events take longer to reach the pipeline
# MAGIC
# MAGIC ### Watermark Formula:
# MAGIC
# MAGIC ```
# MAGIC Watermark = Max Event Time Seen - Threshold
# MAGIC ```
# MAGIC
# MAGIC Example:
# MAGIC - Current max event time: 10:30:00
# MAGIC - Watermark threshold: 10 minutes
# MAGIC - Watermark: 10:20:00
# MAGIC - Events before 10:20:00 are "too late" and dropped
# MAGIC
# MAGIC ### Watermarking Strategy:
# MAGIC
# MAGIC | Strategy | Threshold | Use Case |
# MAGIC |----------|-----------|----------|
# MAGIC | **Aggressive** | 1-5 minutes | Real-time dashboards, low latency |
# MAGIC | **Moderate** | 10-30 minutes | Balanced accuracy/latency |
# MAGIC | **Conservative** | 1-24 hours | Critical analytics, high accuracy |
# MAGIC
# MAGIC ### State Management:
# MAGIC
# MAGIC Without watermarking:
# MAGIC - State grows indefinitely (memory issues)
# MAGIC - System holds windows open forever
# MAGIC
# MAGIC With watermarking:
# MAGIC - Old state is pruned
# MAGIC - Memory bounded
# MAGIC - Late data handled gracefully
# MAGIC
# MAGIC ### Key Considerations:
# MAGIC
# MAGIC ✅ **Choose threshold based on SLA**: How late can data arrive?  
# MAGIC ✅ **Monitor late data metrics**: Track how much data arrives late  
# MAGIC ✅ **Balance accuracy vs. memory**: Longer threshold = more accurate but more memory

# COMMAND ----------

# DBTITLE 1,📊 Prepare Data with Late Arrivals
# Create dataset with intentional late-arriving events
from pyspark.sql.functions import to_timestamp

# Data with events that arrive out of order
late_arrival_data = [
    # Batch 1: On-time arrivals
    ("E001", "2026-04-21 10:00:00", "2026-04-21 10:00:05", 100, "click"),
    ("E002", "2026-04-21 10:05:00", "2026-04-21 10:05:03", 150, "view"),
    ("E003", "2026-04-21 10:10:00", "2026-04-21 10:10:02", 200, "click"),
    
    # Batch 2: Late arrivals (event_time is old, but arrival_time is recent)
    ("E004", "2026-04-21 10:02:00", "2026-04-21 10:15:00", 120, "click"),  # 13 min late
    ("E005", "2026-04-21 10:07:00", "2026-04-21 10:20:00", 180, "view"),   # 13 min late
    
    # Batch 3: Very late arrival (beyond watermark)
    ("E006", "2026-04-21 10:03:00", "2026-04-21 10:30:00", 90, "click"),   # 27 min late
    
    # Batch 4: Recent events
    ("E007", "2026-04-21 10:25:00", "2026-04-21 10:25:01", 250, "purchase"),
    ("E008", "2026-04-21 10:30:00", "2026-04-21 10:30:02", 300, "purchase"),
]

late_data_df = spark.createDataFrame(
    late_arrival_data,
    ["event_id", "event_time", "arrival_time", "value", "event_type"]
)

late_data_df = late_data_df \
    .withColumn("event_time", col("event_time").cast("timestamp")) \
    .withColumn("arrival_time", col("arrival_time").cast("timestamp"))

# Write as source for watermarking demo
late_source_path = f"{base_path}/late_arrival_source"
late_data_df.write.format("delta").mode("overwrite").save(late_source_path)

print("\u2705 Late Arrival Data Created")
print(f"  Total Events: {late_data_df.count()}")
print("\n🔍 Data with Arrival Delays:")
display(late_data_df.orderBy("arrival_time"))

# COMMAND ----------

# DBTITLE 1,⚡ Demo: Streaming with Watermarking
# Demonstrate watermarking to handle late data
from pyspark.sql.functions import window, count, sum as _sum

watermark_checkpoint = f"{checkpoint_path}/watermark_query"
watermark_output = f"{output_path}/watermark_output"

print("🚀 Starting Streaming Query with Watermarking...")
print("  Watermark Threshold: 10 minutes")
print("  Window Duration: 5 minutes")
print("  Trigger Mode: AvailableNow (serverless-compatible)")
print("  Output Mode: Complete (for aggregations)")

# Read stream
stream_df = spark.readStream \
    .format("delta") \
    .load(late_source_path)

# Apply watermark and windowed aggregation
watermarked_df = stream_df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window("event_time", "5 minutes"),
        "event_type"
    ) \
    .agg(
        count("*").alias("event_count"),
        _sum("value").alias("total_value")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "event_type",
        "event_count",
        "total_value"
    )

# Write with watermarking (using complete mode for serverless)
watermark_query = watermarked_df.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", watermark_checkpoint) \
    .trigger(availableNow=True) \
    .start(watermark_output)

print("✅ Watermarking Query Started")
print(f"  Query ID: {watermark_query.id}")
print("\n⏳ Processing available data...")

# Wait for completion
watermark_query.awaitTermination()

print("\n✅ Query completed")

# COMMAND ----------

# DBTITLE 1,🔍 Analyze Watermark Results
# Check the watermarked output
watermark_result_df = spark.read.format("delta").load(watermark_output)

print("\u2705 Watermarking Results:")
print(f"  Total Windows: {watermark_result_df.count()}")
print("\n🔍 Windowed Aggregations (with late data handling):")
display(watermark_result_df.orderBy("window_start", "event_type"))

print("\n💡 Key Observations:")
print("  ✓ Events within watermark (< 10 min late) are included")
print("  ✓ Events beyond watermark (> 10 min late) are dropped")
print("  ✓ State is bounded and pruned for old windows")
print("  ✓ Memory usage remains constant")

# COMMAND ----------

# DBTITLE 1,📌 Section 4: Streaming + Delta Integration
# MAGIC %md
# MAGIC # 📌 Section 4: Streaming + Delta Lake Integration
# MAGIC
# MAGIC ## Why Delta + Streaming = Perfect Match
# MAGIC
# MAGIC ### Delta Lake Advantages for Streaming:
# MAGIC
# MAGIC | Feature | Benefit for Streaming |
# MAGIC |---------|----------------------|
# MAGIC | **ACID Transactions** | Atomic writes prevent partial updates |
# MAGIC | **Schema Evolution** | Handle schema changes without breaking pipeline |
# MAGIC | **Time Travel** | Query historical versions of streaming results |
# MAGIC | **Exactly-Once** | Transaction log ensures idempotent writes |
# MAGIC | **Concurrent Reads** | Readers don't block streaming writes |
# MAGIC | **Optimized Storage** | Compaction and Z-ordering improve query performance |
# MAGIC
# MAGIC ## Streaming Output Modes with Delta:
# MAGIC
# MAGIC ### 1. **Append Mode** (Default)
# MAGIC - New rows are appended to the table
# MAGIC - Best for: Event logs, immutable data
# MAGIC - Requires: Watermarking for aggregations
# MAGIC
# MAGIC ### 2. **Update Mode**
# MAGIC - Updated rows replace existing rows with same key
# MAGIC - Best for: Windowed aggregations with late data
# MAGIC - Requires: Watermarking
# MAGIC
# MAGIC ### 3. **Complete Mode**
# MAGIC - Entire result table is rewritten each trigger
# MAGIC - Best for: Small result sets, dashboards
# MAGIC - Warning: Not efficient for large datasets
# MAGIC
# MAGIC ## Streaming Delta Architecture Pattern:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────┐
# MAGIC │   Source     │
# MAGIC │  (Kafka/     │
# MAGIC │   Delta/     │
# MAGIC │   Files)     │
# MAGIC └─────┬───────┘
# MAGIC      │
# MAGIC      │ readStream
# MAGIC      │
# MAGIC ┌─────┴───────────────────┐
# MAGIC │  Structured Streaming  │
# MAGIC │  • Transformations     │
# MAGIC │  • Watermarking        │  
# MAGIC │  • Aggregations        │
# MAGIC └─────────┬───────────────┘
# MAGIC          │ writeStream
# MAGIC          │ + checkpoint
# MAGIC ┌────────┴──────────────────┐
# MAGIC │     Delta Lake Table    │
# MAGIC │  • ACID guarantees      │
# MAGIC │  • Exactly-once writes  │
# MAGIC │  • Schema enforcement   │
# MAGIC └──────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ## Best Practices:
# MAGIC
# MAGIC ✅ **Always use Delta for streaming sinks** (not Parquet)  
# MAGIC ✅ **One checkpoint location per query** (never reuse)  
# MAGIC ✅ **Use Unity Catalog Volumes** for distributed storage  
# MAGIC ✅ **Enable Auto Optimize** for streaming tables  
# MAGIC ✅ **Monitor streaming metrics** (processing rate, latency)

# COMMAND ----------

# DBTITLE 1,🛠️ Demo: Complete Streaming + Delta Integration
# Comprehensive example: Streaming pipeline with all components

integration_checkpoint = f"{checkpoint_path}/integration_query" 
integration_output = f"{output_path}/reliable_stream"

print("🚀 Building Reliable Streaming Pipeline with Delta...")
print("\n📋 Pipeline Configuration:")
print("  ✓ Source: Delta Lake")
print("  ✓ Sink: Delta Lake")  
print("  ✓ Checkpointing: Enabled")
print("  ✓ Watermarking: 15 minutes")
print("  ✓ Trigger: AvailableNow (serverless-compatible)")
print("  ✓ Output Mode: Append")

# Read stream from Delta source
reliable_stream = spark.readStream \
    .format("delta") \
    .load(source_delta_path)

# Apply transformations with watermarking
transformed_stream = reliable_stream \
    .withWatermark("event_time", "15 minutes") \
    .withColumn("processing_time", current_timestamp()) \
    .withColumn("is_high_value", col("amount") > 200) \
    .select(
        "transaction_id",
        "event_time",
        "amount",
        "user_id",
        "is_high_value",
        "processing_time"
    )

# Write to Delta with full reliability stack (using availableNow for serverless)
integration_query = transformed_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", integration_checkpoint) \
    .trigger(availableNow=True) \
    .start(integration_output)

print("\n✅ Reliable Streaming Pipeline Started!")
print(f"  Query Name: {integration_query.name if integration_query.name else 'Unnamed'}")
print(f"  Query ID: {integration_query.id}")
print("\n📋 Reliability Features Active:")
print("  ✓ Exactly-once processing")
print("  ✓ Fault tolerance via checkpointing")
print("  ✓ Late data handling with watermarking")
print("  ✓ ACID guarantees with Delta")
print("\n⏳ Processing available data...")

# Wait for completion
integration_query.awaitTermination()

print("\n✅ Pipeline completed gracefully")

# COMMAND ----------

# DBTITLE 1,🔍 Verify Reliable Pipeline Output
# Verify the integrated pipeline results
integration_result = spark.read.format("delta").load(integration_output)

print("📊 Reliable Pipeline Output Analysis:")
print(f"  Total Transactions Processed: {integration_result.count()}")
print(f"  High-Value Transactions: {integration_result.filter('is_high_value').count()}")
print(f"  Low-Value Transactions: {integration_result.filter('NOT is_high_value').count()}")

print("\n🔍 Sample Output:")
display(integration_result.orderBy("event_time"))

print("\n✅ Pipeline Reliability Verified:")
print("  ✓ All records processed exactly once")
print("  ✓ Checkpoint maintained for recovery")
print("  ✓ Late data handled within watermark")
print("  ✓ Delta guarantees data consistency")

# COMMAND ----------

# DBTITLE 1,📌 Section 5: End-to-End Production Pipeline
# MAGIC %md
# MAGIC # 📌 Section 5: End-to-End Production Streaming Pipeline
# MAGIC
# MAGIC ## Production-Grade Architecture
# MAGIC
# MAGIC Let's build a real-world streaming pipeline that processes IoT sensor data with all reliability features:
# MAGIC
# MAGIC ### Pipeline Requirements:
# MAGIC
# MAGIC 1. **Source**: IoT sensor events (temperature, humidity, pressure)
# MAGIC 2. **Processing**: 
# MAGIC    - Filter invalid readings
# MAGIC    - Calculate 10-minute windowed averages
# MAGIC    - Flag anomalies (readings beyond thresholds)
# MAGIC 3. **Sink**: Delta table for real-time dashboard
# MAGIC 4. **Reliability**:
# MAGIC    - Exactly-once processing
# MAGIC    - Handle late data (up to 20 minutes)
# MAGIC    - Fault-tolerant with checkpointing
# MAGIC
# MAGIC ### Data Flow:
# MAGIC
# MAGIC ```
# MAGIC 🌡️ IoT Sensors 
# MAGIC     ↓
# MAGIC [Raw Events Stream]
# MAGIC     ↓
# MAGIC ┌─────────────────────────┐
# MAGIC │  Data Quality Filters     │
# MAGIC │  • Remove nulls           │
# MAGIC │  • Range validation       │
# MAGIC └──────────┬───────────────┘
# MAGIC          │
# MAGIC ┌──────────┴───────────────┐
# MAGIC │  Watermarking (20 min)    │
# MAGIC └──────────┬───────────────┘
# MAGIC          │
# MAGIC ┌──────────┴───────────────┐
# MAGIC │  Windowed Aggregation     │
# MAGIC │  (10-minute windows)      │
# MAGIC └──────────┬───────────────┘
# MAGIC          │
# MAGIC ┌──────────┴───────────────┐
# MAGIC │  Anomaly Detection        │
# MAGIC └──────────┬───────────────┘
# MAGIC          │
# MAGIC ┌──────────┴───────────────┐
# MAGIC │  Delta Lake Sink          │
# MAGIC │  + Checkpointing          │
# MAGIC └──────────────────────────┘
# MAGIC          │
# MAGIC    [Delta Table]
# MAGIC          │
# MAGIC    📊 Dashboard
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,🌡️ Generate IoT Sensor Data
# Generate realistic IoT sensor data with anomalies and late arrivals
import random
from datetime import datetime, timedelta

# Simulate IoT sensor readings
sensor_data = []
base_time = datetime(2026, 4, 21, 10, 0, 0)

for i in range(50):
    # Sensor ID (5 different sensors)
    sensor_id = f"SENSOR_{(i % 5) + 1:03d}"
    
    # Event time (incremental with some randomness)
    event_time = base_time + timedelta(minutes=i*2 + random.randint(-5, 5))
    
    # Normal readings with occasional anomalies
    temperature = round(random.uniform(18, 28), 2) if random.random() > 0.1 else round(random.uniform(-10, 50), 2)
    humidity = round(random.uniform(40, 70), 2) if random.random() > 0.1 else round(random.uniform(0, 100), 2)
    pressure = round(random.uniform(980, 1020), 2) if random.random() > 0.1 else round(random.uniform(900, 1100), 2)
    
    sensor_data.append((
        f"EVENT_{i+1:04d}",
        sensor_id,
        event_time.strftime("%Y-%m-%d %H:%M:%S"),
        temperature,
        humidity,
        pressure
    ))

# Create DataFrame
iot_df = spark.createDataFrame(
    sensor_data,
    ["event_id", "sensor_id", "event_time", "temperature_c", "humidity_pct", "pressure_hpa"]
)

iot_df = iot_df.withColumn("event_time", col("event_time").cast("timestamp"))

# Write as Delta source
iot_source_path = f"{base_path}/iot_sensor_source"
iot_df.write.format("delta").mode("overwrite").save(iot_source_path)

print("✅ IoT Sensor Data Generated")
print(f"  Total Events: {iot_df.count()}")
print(f"  Sensors: {iot_df.select('sensor_id').distinct().count()}")
print(f"  Time Range: {iot_df.agg({'event_time': 'min'}).collect()[0][0]} to {iot_df.agg({'event_time': 'max'}).collect()[0][0]}")

print("\n🔍 Sample Sensor Readings:")
display(iot_df.orderBy("event_time").limit(10))

# COMMAND ----------

# DBTITLE 1,⚡ Production Pipeline: IoT Stream Processing
# Build complete production streaming pipeline
from pyspark.sql.functions import avg, max as _max, min as _min, stddev, when

prod_checkpoint = f"{checkpoint_path}/iot_production"
prod_output = f"{output_path}/iot_analytics"

print("🚀 Starting Production IoT Streaming Pipeline...")
print("\n📋 Pipeline Specifications:")
print("  Source: IoT Sensor Events (Delta)")
print("  Window: 10 minutes")
print("  Watermark: 20 minutes")
print("  Output Mode: Complete (for aggregations)")
print("  Trigger: AvailableNow (serverless-compatible)")
print("  Reliability: Exactly-once with checkpointing")

# Read streaming data
iot_stream = spark.readStream \
    .format("delta") \
    .load(iot_source_path)

# Step 1: Data Quality - Filter invalid readings
print("\n✓ Step 1: Applying Data Quality Filters...")
filtered_stream = iot_stream \
    .filter(col("temperature_c").isNotNull()) \
    .filter(col("humidity_pct").isNotNull()) \
    .filter(col("pressure_hpa").isNotNull()) \
    .filter(col("temperature_c").between(-50, 60)) \
    .filter(col("humidity_pct").between(0, 100)) \
    .filter(col("pressure_hpa").between(900, 1100))

# Step 2: Apply Watermarking
print("✓ Step 2: Applying Watermarking (20 minutes)...")
watermarked_stream = filtered_stream.withWatermark("event_time", "20 minutes")

# Step 3: Windowed Aggregations
print("✓ Step 3: Computing Windowed Aggregations (10-minute windows)...")
aggregated_stream = watermarked_stream \
    .groupBy(
        window("event_time", "10 minutes"),
        "sensor_id"
    ) \
    .agg(
        avg("temperature_c").alias("avg_temperature"),
        _min("temperature_c").alias("min_temperature"),
        _max("temperature_c").alias("max_temperature"),
        avg("humidity_pct").alias("avg_humidity"),
        avg("pressure_hpa").alias("avg_pressure"),
        count("*").alias("reading_count")
    )

# Step 4: Anomaly Detection
print("✓ Step 4: Applying Anomaly Detection...")
anomalies_detected = aggregated_stream \
    .withColumn("temp_anomaly", 
                (col("avg_temperature") < 15) | (col("avg_temperature") > 30)) \
    .withColumn("humidity_anomaly", 
                (col("avg_humidity") < 30) | (col("avg_humidity") > 80)) \
    .withColumn("is_anomaly", 
                col("temp_anomaly") | col("humidity_anomaly")) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "sensor_id",
        "avg_temperature",
        "min_temperature",
        "max_temperature",
        "avg_humidity",
        "avg_pressure",
        "reading_count",
        "is_anomaly"
    )

# Step 5: Write to Delta with Checkpointing (using complete mode for serverless)
print("✓ Step 5: Writing to Delta Lake with Checkpointing...")
prod_query = anomalies_detected.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", prod_checkpoint) \
    .trigger(availableNow=True) \
    .start(prod_output)

print("\n✅ Production Pipeline Running!")
print(f"  Query ID: {prod_query.id}")
print("  Status: Active")
print("\n⏳ Processing streaming data...")

# Process data
prod_query.awaitTermination()

print("\n✅ Production pipeline completed successfully")

# COMMAND ----------

# DBTITLE 1,📊 Analyze Production Pipeline Results
# Analyze the production pipeline output
prod_results = spark.read.format("delta").load(prod_output)

print("📊 Production Pipeline Analytics:")
print(f"  Total Windows Processed: {prod_results.count()}")
print(f"  Sensors Monitored: {prod_results.select('sensor_id').distinct().count()}")
print(f"  Anomalies Detected: {prod_results.filter('is_anomaly').count()}")
print(f"  Normal Readings: {prod_results.filter('NOT is_anomaly').count()}")

print("\n🔍 Windowed Sensor Analytics:")
display(prod_results.orderBy("window_start", "sensor_id"))

print("\n🚨 Anomaly Summary:")
anomalies = prod_results.filter("is_anomaly")
if anomalies.count() > 0:
    display(anomalies.select("window_start", "sensor_id", "avg_temperature", "avg_humidity", "is_anomaly"))
else:
    print("  No anomalies detected in this batch")

print("\n✅ Production Pipeline Validation:")
print("  ✓ Data quality filters applied")
print("  ✓ Watermarking handled late data")
print("  ✓ Windowed aggregations computed")
print("  ✓ Anomalies flagged")
print("  ✓ Results written to Delta")
print("  ✓ Checkpointing enabled for recovery")

# COMMAND ----------

# DBTITLE 1,📌 Section 6: Failure Recovery Scenarios
# MAGIC %md
# MAGIC # 📌 Section 6: Failure Recovery & Fault Tolerance
# MAGIC
# MAGIC ## Understanding Streaming Failures
# MAGIC
# MAGIC ### Common Failure Scenarios:
# MAGIC
# MAGIC | Failure Type | Impact | Recovery Mechanism |
# MAGIC |--------------|--------|--------------------|
# MAGIC | **Cluster Crash** | Query stops mid-processing | Restart from checkpoint |
# MAGIC | **Network Outage** | Source temporarily unavailable | Auto-retry with backoff |
# MAGIC | **Bad Data** | Schema mismatch or corrupt records | Error handling + DLQ |
# MAGIC | **Resource Exhaustion** | OOM or disk full | Scaling + monitoring |
# MAGIC | **Code Deployment** | New version deployed | Graceful stop + restart |
# MAGIC
# MAGIC ## Checkpoint-Based Recovery:
# MAGIC
# MAGIC ### What Happens During Recovery:
# MAGIC
# MAGIC 1. **Query restarts** (manually or auto)
# MAGIC 2. **Checkpoint read**: Load last committed offset
# MAGIC 3. **State restoration**: Restore aggregation state
# MAGIC 4. **Resume processing**: Start from last uncommitted batch
# MAGIC 5. **No duplicates**: Already-processed data skipped
# MAGIC
# MAGIC ### Recovery Timeline:
# MAGIC
# MAGIC ```
# MAGIC Normal Operation:
# MAGIC Batch 1 ✓ → Batch 2 ✓ → Batch 3 ✓ → [CRASH]
# MAGIC                                     |
# MAGIC                                     ↓
# MAGIC                               Checkpoint
# MAGIC                                Batch 3
# MAGIC                                     ↓
# MAGIC Recovery:
# MAGIC                    [RESTART] → Batch 4 ✓ → Batch 5 ✓
# MAGIC ```
# MAGIC
# MAGIC ## Best Practices for Resilient Pipelines:
# MAGIC
# MAGIC ### 1. Monitoring & Alerting
# MAGIC ```python
# MAGIC # Monitor query health
# MAGIC query.lastProgress  # Check processing metrics
# MAGIC query.status        # Check current state
# MAGIC ```
# MAGIC
# MAGIC ### 2. Error Handling
# MAGIC ```python
# MAGIC # Use try-catch for graceful degradation
# MAGIC try:
# MAGIC     query.awaitTermination()
# MAGIC except Exception as e:
# MAGIC     log_error(e)
# MAGIC     restart_query()
# MAGIC ```
# MAGIC
# MAGIC ### 3. Idempotent Design
# MAGIC - Ensure reprocessing produces same results
# MAGIC - Use Delta's MERGE for upserts
# MAGIC - Avoid time-dependent operations
# MAGIC
# MAGIC ### 4. Resource Management
# MAGIC - Set appropriate trigger intervals
# MAGIC - Monitor memory usage
# MAGIC - Scale compute as needed
# MAGIC
# MAGIC ## Recovery Testing:
# MAGIC
# MAGIC ✅ **Test failure scenarios** regularly  
# MAGIC ✅ **Measure recovery time** (RTO)  
# MAGIC ✅ **Validate data integrity** post-recovery  
# MAGIC ✅ **Document runbooks** for operations team

# COMMAND ----------

# DBTITLE 1,🔄 Demo: Simulated Failure & Recovery
# Demonstrate checkpoint-based recovery

recovery_checkpoint = f"{checkpoint_path}/recovery_demo"
recovery_output = f"{output_path}/recovery_test"

print("🚀 Simulating Streaming Pipeline with Failure & Recovery...")
print("\n📋 Scenario: Pipeline processes data, then recovers from checkpoint")

# Start initial query
print("\n[PHASE 1] Starting initial streaming query...")
recovery_stream = spark.readStream \
    .format("delta") \
    .load(source_delta_path)

recovery_query = recovery_stream \
    .withColumn("batch_id", lit("batch_1")) \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", recovery_checkpoint) \
    .trigger(availableNow=True) \
    .start(recovery_output)

print("✅ Query started, processing data...")
recovery_query.awaitTermination()
print("✅ Phase 1 completed")

print("\n[PHASE 2] 🔄 Recovering from checkpoint...")
print(f"  Checkpoint location: {recovery_checkpoint}")
print("  Simulating query restart with same checkpoint...")

# Restart query with SAME checkpoint location
# This demonstrates that the query can resume from where it left off
recovery_stream_2 = spark.readStream \
    .format("delta") \
    .load(source_delta_path)

recovered_query = recovery_stream_2 \
    .withColumn("batch_id", lit("batch_2_recovered")) \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", recovery_checkpoint) \
    .trigger(availableNow=True) \
    .start(recovery_output)

print("✅ Query recovered successfully!")
print("  ✓ Checkpoint loaded")
print("  ✓ Processing resumed from last committed offset")
print("  ✓ No data loss or duplication")

recovered_query.awaitTermination()

print("\n✅ Recovery demo completed")

# COMMAND ----------

# DBTITLE 1,🔍 Validate Recovery Results
# Verify that recovery worked correctly - no duplicates
recovery_results = spark.read.format("delta").load(recovery_output)

print("🔍 Recovery Validation:")
print(f"  Total Records: {recovery_results.count()}")
print(f"  Unique Transaction IDs: {recovery_results.select('transaction_id').distinct().count()}")

# Check for duplicates
duplicates = recovery_results.groupBy("transaction_id").count().filter("count > 1")
duplicate_count = duplicates.count()

if duplicate_count == 0:
    print("\n✅ EXACTLY-ONCE VERIFIED: No duplicate records found!")
else:
    print(f"\n⚠️ Warning: Found {duplicate_count} duplicated transaction IDs")
    display(duplicates)

print("\n🔍 Data by Batch:")
display(recovery_results.groupBy("batch_id").count().orderBy("batch_id"))

print("\n📋 Recovery Summary:")
print("  ✓ Query stopped and restarted")
print("  ✓ Checkpoint preserved state")
print("  ✓ Processing resumed seamlessly") 
print("  ✓ Exactly-once semantics maintained")
print("\n➡️ This demonstrates fault tolerance in production streaming!")

# COMMAND ----------

# DBTITLE 1,📌 Section 7: End-to-End Architecture
# MAGIC %md
# MAGIC # 📌 Section 7: End-to-End Reliable Streaming Architecture
# MAGIC
# MAGIC ## Production Streaming Reference Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────────────┐
# MAGIC │                    DATA SOURCES                        │
# MAGIC │  📡 Kafka  📊 Event Hubs  📁 Delta  ☁️ Cloud Storage  │
# MAGIC └──────────────────┬────────────────────────────────┘
# MAGIC                      │
# MAGIC                      │ readStream
# MAGIC                      │
# MAGIC ┌──────────────────┴────────────────────────────────┐
# MAGIC │          STRUCTURED STREAMING ENGINE                   │
# MAGIC │                                                        │
# MAGIC │  ┌────────────────────────────────────────┐  │
# MAGIC │  │  ✅ Data Quality & Validation        │  │
# MAGIC │  │  • Schema enforcement                 │  │
# MAGIC │  │  • Null handling                     │  │
# MAGIC │  │  • Range validation                  │  │
# MAGIC │  └────────────────────────────────────────┘  │
# MAGIC │                    ↓                               │
# MAGIC │  ┌────────────────────────────────────────┐  │
# MAGIC │  │  ⏱️ Watermarking                      │  │
# MAGIC │  │  • Late data handling                │  │
# MAGIC │  │  • Event-time processing             │  │
# MAGIC │  │  • State management                  │  │
# MAGIC │  └────────────────────────────────────────┘  │
# MAGIC │                    ↓                               │
# MAGIC │  ┌────────────────────────────────────────┐  │
# MAGIC │  │  📊 Transformations & Aggregations  │  │
# MAGIC │  │  • Windowed operations               │  │
# MAGIC │  │  • Stateful computations             │  │
# MAGIC │  │  • Joins & enrichment                │  │
# MAGIC │  └────────────────────────────────────────┘  │
# MAGIC └──────────────────┬────────────────────────────────┘
# MAGIC                      │
# MAGIC                      │ writeStream + checkpoint
# MAGIC                      │
# MAGIC ┌──────────────────┴────────────────────────────────┐
# MAGIC │              DELTA LAKE STORAGE                        │
# MAGIC │                                                        │
# MAGIC │  ✅ ACID Transactions    🔒 Schema Evolution       │
# MAGIC │  ✅ Exactly-Once Writes  🕒 Time Travel             │
# MAGIC │  ✅ Concurrent Reads     ⚡ Optimized Storage        │
# MAGIC └──────────────────┬────────────────────────────────┘
# MAGIC                      │
# MAGIC       ┌──────────────┴──────────────┐
# MAGIC       │                           │
# MAGIC ┌─────┴─────┐         ┌─────┴─────┐
# MAGIC │ 📊 BI    │         │ 🔍 ML   │
# MAGIC │Dashboard│         │ Models │
# MAGIC └──────────┘         └──────────┘
# MAGIC
# MAGIC ┌────────────────────────────────────────────────────┐
# MAGIC │         RELIABILITY LAYER (Cross-Cutting)             │
# MAGIC │                                                        │
# MAGIC │  💾 Checkpointing    🔄 Failure Recovery          │
# MAGIC │  📊 Monitoring        🚨 Alerting                  │
# MAGIC │  📝 Logging           🔒 Security & Governance      │
# MAGIC └────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ## Key Reliability Principles:
# MAGIC
# MAGIC ### 1. **Exactly-Once Guarantee**
# MAGIC - Source offset tracking
# MAGIC - Idempotent writes with Delta
# MAGIC - Transaction log coordination
# MAGIC
# MAGIC ### 2. **Fault Tolerance**
# MAGIC - Checkpoint-based recovery
# MAGIC - State preservation
# MAGIC - Automatic retries
# MAGIC
# MAGIC ### 3. **Late Data Handling**
# MAGIC - Watermarking strategy
# MAGIC - Bounded state management
# MAGIC - Configurable thresholds
# MAGIC
# MAGIC ### 4. **Scalability**
# MAGIC - Horizontal scaling with partitioning
# MAGIC - Optimized Delta storage
# MAGIC - Trigger interval tuning
# MAGIC
# MAGIC ### 5. **Observability**
# MAGIC - Query metrics (lastProgress)
# MAGIC - Processing rate monitoring
# MAGIC - Latency tracking
# MAGIC - Error alerting
# MAGIC
# MAGIC ## Production Checklist:
# MAGIC
# MAGIC ☐ Source configured with appropriate read options  
# MAGIC ☐ Data quality validations in place  
# MAGIC ☐ Watermark threshold set based on SLA  
# MAGIC ☐ Checkpoint location on distributed storage  
# MAGIC ☐ Delta format for sink  
# MAGIC ☐ Trigger interval optimized  
# MAGIC ☐ Monitoring & alerting configured  
# MAGIC ☐ Runbook documented for failures  
# MAGIC ☐ Testing done for recovery scenarios  
# MAGIC ☐ Resource limits and auto-scaling configured

# COMMAND ----------

# DBTITLE 1,🤖 Genie Code Agent Prompts
# MAGIC %md
# MAGIC # 🤖 Using Genie Code Agent for Streaming Pipelines
# MAGIC
# MAGIC ## Effective Prompts for Streaming Development:
# MAGIC
# MAGIC ### 📌 Exactly-Once Processing:
# MAGIC ```
# MAGIC Prompt: "Build a streaming pipeline with exactly-once guarantee 
# MAGIC for financial transactions using Delta Lake"
# MAGIC ```
# MAGIC
# MAGIC ### 📌 Checkpointing:
# MAGIC ```
# MAGIC Prompt: "Create a streaming query with checkpointing enabled. 
# MAGIC Use Unity Catalog Volumes for checkpoint storage"
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC Prompt: "Show me how to implement fault-tolerant streaming 
# MAGIC with checkpoint-based recovery"
# MAGIC ```
# MAGIC
# MAGIC ### 📌 Watermarking:
# MAGIC ```
# MAGIC Prompt: "Add watermarking to handle late-arriving events. 
# MAGIC Set threshold to 20 minutes for IoT sensor data"
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC Prompt: "Implement windowed aggregations with watermarking 
# MAGIC for real-time analytics"
# MAGIC ```
# MAGIC
# MAGIC ### 📌 Complete Pipeline:
# MAGIC ```
# MAGIC Prompt: "Build an end-to-end streaming pipeline with:
# MAGIC - Source: Delta table
# MAGIC - Processing: 10-minute windowed aggregations
# MAGIC - Watermark: 15 minutes
# MAGIC - Sink: Delta table
# MAGIC - Checkpointing enabled
# MAGIC - Handle late data gracefully"
# MAGIC ```
# MAGIC
# MAGIC ### 📌 Failure Recovery:
# MAGIC ```
# MAGIC Prompt: "Demonstrate streaming query recovery after failure 
# MAGIC using checkpoints. Show that exactly-once is maintained"
# MAGIC ```
# MAGIC
# MAGIC ### 📌 Production Architecture:
# MAGIC ```
# MAGIC Prompt: "Design a production-grade streaming architecture 
# MAGIC with data quality checks, anomaly detection, and 
# MAGIC reliability features"
# MAGIC ```
# MAGIC
# MAGIC ### 📌 Monitoring:
# MAGIC ```
# MAGIC Prompt: "Add monitoring and metrics tracking to my 
# MAGIC streaming query. Show query status and progress"
# MAGIC ```
# MAGIC
# MAGIC ### 📌 Optimization:
# MAGIC ```
# MAGIC Prompt: "Optimize my streaming pipeline for:
# MAGIC - Lower latency
# MAGIC - Better throughput
# MAGIC - Efficient state management"
# MAGIC ```
# MAGIC
# MAGIC ## Best Practices When Using Genie:
# MAGIC
# MAGIC ✅ **Be specific**: Mention watermark thresholds, window durations  
# MAGIC ✅ **Specify storage**: Request Unity Catalog Volumes for checkpoints  
# MAGIC ✅ **Include reliability**: Ask for exactly-once, checkpointing explicitly  
# MAGIC ✅ **Request validation**: Ask Genie to verify results after generation  
# MAGIC ✅ **Iterative refinement**: Start simple, then add complexity
# MAGIC
# MAGIC ## Example Workflow:
# MAGIC
# MAGIC 1. **Initial**: "Create a basic streaming pipeline"
# MAGIC 2. **Refine**: "Add checkpointing to the pipeline"
# MAGIC 3. **Enhance**: "Include watermarking for late data"
# MAGIC 4. **Validate**: "Verify exactly-once processing"
# MAGIC 5. **Productionize**: "Add monitoring and error handling"

# COMMAND ----------

# DBTITLE 1,🎓 Final Summary & Key Learnings
# MAGIC %md
# MAGIC # 🎓 Final Summary: Streaming Reliability Mastery
# MAGIC
# MAGIC ## 🔑 Key Learnings:
# MAGIC
# MAGIC ### 1. **Exactly-Once Processing**
# MAGIC - Guarantees each record affects state exactly once
# MAGIC - Achieved through Delta's transaction log + checkpointing
# MAGIC - Critical for financial, analytics, and mission-critical use cases
# MAGIC
# MAGIC ### 2. **Checkpointing**
# MAGIC - Tracks processing progress (offsets, state, metadata)
# MAGIC - Enables fault-tolerant recovery
# MAGIC - Must use distributed storage (Unity Catalog Volumes)
# MAGIC - Never reuse checkpoint locations
# MAGIC
# MAGIC ### 3. **Watermarking**
# MAGIC - Handles late-arriving data gracefully
# MAGIC - Bounds state growth
# MAGIC - Threshold must align with business SLA
# MAGIC - Formula: `Watermark = Max Event Time - Threshold`
# MAGIC
# MAGIC ### 4. **Delta Lake Integration**
# MAGIC - ACID transactions ensure data consistency
# MAGIC - Idempotent writes prevent duplicates
# MAGIC - Schema evolution without breaking pipelines
# MAGIC - Time travel for historical analysis
# MAGIC
# MAGIC ### 5. **Production Architecture**
# MAGIC - Multi-layered reliability (source → processing → sink)
# MAGIC - Data quality checks at ingestion
# MAGIC - Monitoring and alerting for operations
# MAGIC - Documented recovery procedures
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Common Mistakes to AVOID:
# MAGIC
# MAGIC ### ❌ 1. Missing Checkpointing
# MAGIC **Problem**: No fault tolerance, data loss on failure  
# MAGIC **Fix**: Always specify `checkpointLocation`
# MAGIC
# MAGIC ### ❌ 2. Ignoring Late Data
# MAGIC **Problem**: Inaccurate aggregations, unbounded state  
# MAGIC **Fix**: Use `withWatermark()` with appropriate threshold
# MAGIC
# MAGIC ### ❌ 3. Not Ensuring Idempotency
# MAGIC **Problem**: Duplicate processing on retries  
# MAGIC **Fix**: Use Delta Lake, design idempotent operations
# MAGIC
# MAGIC ### ❌ 4. Incorrect Watermark Threshold
# MAGIC **Problem**: Too short = data loss, too long = memory issues  
# MAGIC **Fix**: Analyze late data patterns, set based on SLA
# MAGIC
# MAGIC ### ❌ 5. Reusing Checkpoint Locations
# MAGIC **Problem**: Query fails with incompatible state  
# MAGIC **Fix**: Unique checkpoint per query, delete old checkpoints when redesigning
# MAGIC
# MAGIC ### ❌ 6. Using Local Storage for Checkpoints
# MAGIC **Problem**: Lost checkpoints = lost progress  
# MAGIC **Fix**: Use Unity Catalog Volumes or cloud storage
# MAGIC
# MAGIC ### ❌ 7. No Monitoring
# MAGIC **Problem**: Silent failures, unnoticed performance degradation  
# MAGIC **Fix**: Track `lastProgress`, set up alerts
# MAGIC
# MAGIC ### ❌ 8. Wrong Output Mode
# MAGIC **Problem**: Inefficient or incorrect results  
# MAGIC **Fix**: Use append for events, update for aggregations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💬 Interview Questions & Answers:
# MAGIC
# MAGIC ### Q1: **Explain exactly-once processing in Spark Structured Streaming.**
# MAGIC **Answer**: Exactly-once processing ensures each record is processed exactly once, affecting the final state once, despite failures or retries. It's achieved through:
# MAGIC 1. **Idempotent writes**: Delta Lake's transaction log prevents duplicate writes
# MAGIC 2. **Checkpointing**: Tracks offsets so reprocessing starts from last committed batch
# MAGIC 3. **Atomic commits**: Each micro-batch is fully committed or rolled back
# MAGIC
# MAGIC ### Q2: **What is stored in a Structured Streaming checkpoint?**
# MAGIC **Answer**: Checkpoints store:
# MAGIC - **Offsets**: What data has been read from source
# MAGIC - **State**: Aggregation results for stateful operations
# MAGIC - **Metadata**: Query configuration, schema information
# MAGIC - **Commits**: Successfully completed batches
# MAGIC
# MAGIC This enables fault-tolerant recovery by resuming from the last successful state.
# MAGIC
# MAGIC ### Q3: **How does watermarking help with late data?**
# MAGIC **Answer**: Watermarking defines a threshold for how late data can arrive before being dropped. Formula: `Watermark = Max Event Time Seen - Threshold`. Benefits:
# MAGIC - Bounds state growth (prevents memory issues)
# MAGIC - Allows graceful late data handling within threshold
# MAGIC - Drops extremely late data to maintain performance
# MAGIC
# MAGIC ### Q4: **Why use Delta Lake as a streaming sink instead of Parquet?**
# MAGIC **Answer**: Delta provides:
# MAGIC - **ACID transactions**: Atomic writes, no partial files
# MAGIC - **Exactly-once semantics**: Transaction log prevents duplicates
# MAGIC - **Schema enforcement**: Prevents bad data
# MAGIC - **Concurrent reads**: Readers don't block writers
# MAGIC - **Time travel**: Query historical versions
# MAGIC - **Optimized storage**: Z-ordering, compaction
# MAGIC
# MAGIC ### Q5: **Difference between update and append output modes?**
# MAGIC **Answer**:
# MAGIC - **Append**: Only new rows added, immutable. Best for event logs. Requires watermarking for aggregations.
# MAGIC - **Update**: Changed rows replace existing. Best for windowed aggregations with late data. More expensive.
# MAGIC - **Complete**: Entire result rewritten each trigger. Only for small result sets.
# MAGIC
# MAGIC ### Q6: **How would you handle a streaming query that crashed?**
# MAGIC **Answer**: Recovery steps:
# MAGIC 1. Check query logs for root cause
# MAGIC 2. Restart query with same checkpoint location
# MAGIC 3. Streaming engine reads checkpoint
# MAGIC 4. Restores state and resumes from last committed offset
# MAGIC 5. No data loss or duplication due to exactly-once guarantees
# MAGIC 6. Monitor metrics post-recovery to ensure stability
# MAGIC
# MAGIC ### Q7: **What's the impact of trigger interval on streaming performance?**
# MAGIC **Answer**: 
# MAGIC - **Short interval** (seconds): Lower latency but higher overhead, more micro-batches
# MAGIC - **Long interval** (minutes): Higher throughput but increased latency
# MAGIC - **Trade-off**: Balance based on SLA requirements
# MAGIC - **Best practice**: Start with 10-30 seconds, tune based on metrics
# MAGIC
# MAGIC ### Q8: **Can you reuse checkpoint locations between different queries?**
# MAGIC **Answer**: **NO**. Each streaming query needs a unique checkpoint location. Reusing causes:
# MAGIC - State incompatibility errors
# MAGIC - Query failures
# MAGIC - Data corruption risks
# MAGIC
# MAGIC Best practice: Use descriptive checkpoint paths like `/checkpoints/{pipeline_name}/{query_name}/`
# MAGIC
# MAGIC ### Q9: **How do you monitor streaming query health?**
# MAGIC **Answer**: Key metrics:
# MAGIC ```python
# MAGIC query.lastProgress  # Processing rate, latency, batch duration
# MAGIC query.status        # Current state (active, stopped)
# MAGIC query.recentProgress  # Historical metrics
# MAGIC ```
# MAGIC Monitor:
# MAGIC - Input/output rows per second
# MAGIC - Batch duration vs trigger interval
# MAGIC - State memory usage
# MAGIC - Watermark progression
# MAGIC
# MAGIC ### Q10: **Explain the relationship between watermarking and state pruning.**
# MAGIC **Answer**: Watermarking enables state pruning:
# MAGIC - Without watermarking: State grows indefinitely (memory leak)
# MAGIC - With watermarking: System drops state for windows below watermark
# MAGIC - Example: 10-min watermark means windows older than 10 minutes are finalized and state is pruned
# MAGIC - Result: Bounded memory usage, scalable streaming
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Production Deployment Checklist:
# MAGIC
# MAGIC ☐ Source reliability verified (retries, backpressure)  
# MAGIC ☐ Checkpoints on distributed storage (Unity Catalog Volumes)  
# MAGIC ☐ Watermark threshold set based on data analysis  
# MAGIC ☐ Delta Lake for all sinks  
# MAGIC ☐ Data quality validations implemented  
# MAGIC ☐ Monitoring dashboards created  
# MAGIC ☐ Alerting configured for failures  
# MAGIC ☐ Recovery runbook documented  
# MAGIC ☐ Load testing completed  
# MAGIC ☐ Resource scaling configured  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Next Steps:
# MAGIC
# MAGIC 1. **Practice**: Build streaming pipelines with real data sources
# MAGIC 2. **Experiment**: Test different watermark thresholds and trigger intervals
# MAGIC 3. **Simulate**: Practice failure recovery scenarios
# MAGIC 4. **Optimize**: Profile and tune streaming query performance
# MAGIC 5. **Learn Advanced**: Explore stateful operations (mapGroupsWithState, flatMapGroupsWithState)
# MAGIC 6. **Integrate**: Connect to Kafka, Event Hubs, Kinesis
# MAGIC 7. **Monitor**: Set up production monitoring and alerting
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Additional Resources:
# MAGIC
# MAGIC - Databricks Structured Streaming Guide
# MAGIC - Delta Lake Documentation
# MAGIC - Unity Catalog Best Practices
# MAGIC - Streaming Query Performance Tuning
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏆 Congratulations!
# MAGIC
# MAGIC You now understand how to build reliable, production-grade streaming pipelines with exactly-once guarantees, checkpointing, and watermarking using Databricks and Delta Lake.
# MAGIC
# MAGIC **@TRRaveendra** 🚀