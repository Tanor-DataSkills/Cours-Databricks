# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # 🌊 Data Engineering Training — Phase 5 Day 25  
# MAGIC ## ⚡ Structured Streaming: Micro-batch Processing & Architecture  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - Structured Streaming Fundamentals  
# MAGIC - Micro-batch Processing  
# MAGIC - Streaming Architecture  
# MAGIC - Batch vs Streaming Comparison  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### ⚙️ Platform: Databricks (Serverless + Unity Catalog + Streaming)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Objective:
# MAGIC Understand how Structured Streaming works in Spark, including micro-batch processing and how to design real-time streaming architectures.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Important Engineering Constraints:
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC * Use Databricks Serverless Compute
# MAGIC * Use DataFrame API only
# MAGIC * Use Unity Catalog Volumes for data ingestion
# MAGIC * Use Delta format for streaming sinks
# MAGIC * Use checkpointing for fault tolerance
# MAGIC * Follow streaming-first and incremental design
# MAGIC
# MAGIC ❌ **DO NOT:**
# MAGIC * Use RDDs
# MAGIC * Use cache() / persist()
# MAGIC * Use /tmp or local storage
# MAGIC * Process streaming data as batch
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 1: What is Structured Streaming?
# MAGIC %md
# MAGIC ## 💧 Section 1: What is Structured Streaming?
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC
# MAGIC Imagine you have a never-ending stream of water flowing from a tap. **Structured Streaming** is like having a smart bucket that continuously catches the water, processes it (filters, measures, analyzes), and outputs results — all in real-time!
# MAGIC
# MAGIC **Batch processing** = Fill a bucket, process it, wait for the next bucket  
# MAGIC **Streaming** = Continuously process data as it flows
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Structured Streaming** is a scalable and fault-tolerant stream processing engine built on the Spark SQL engine. It treats live data streams as an unbounded table that is continuously appended.
# MAGIC
# MAGIC #### Key Concepts:
# MAGIC
# MAGIC 1. **Unbounded Data**: Data that arrives continuously with no defined end
# MAGIC 2. **Incremental Processing**: Process only new data since the last trigger
# MAGIC 3. **Fault Tolerance**: Automatic recovery using checkpointing and write-ahead logs
# MAGIC 4. **Exactly-Once Semantics**: Guarantees each record is processed exactly once
# MAGIC 5. **Integration with Batch**: Use the same DataFrame API for both batch and streaming
# MAGIC
# MAGIC #### Core Principles:
# MAGIC
# MAGIC * **Trigger-based Processing**: Data is processed at specified intervals (micro-batches)
# MAGIC * **State Management**: Maintain state across batches for aggregations and joins
# MAGIC * **Watermarking**: Handle late-arriving data gracefully
# MAGIC * **Output Modes**: Control how results are written (append, update, complete)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚙️ When to Use Structured Streaming?
# MAGIC
# MAGIC ✅ **Use Cases:**
# MAGIC * Real-time analytics dashboards
# MAGIC * Fraud detection systems
# MAGIC * IoT sensor data processing
# MAGIC * Log aggregation and monitoring
# MAGIC * ETL pipelines with low latency requirements
# MAGIC * Event-driven architectures
# MAGIC
# MAGIC ❌ **Not Suitable For:**
# MAGIC * Historical batch analysis (use batch processing)
# MAGIC * Simple one-time data loads
# MAGIC * Complex iterative algorithms (use batch with caching)
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 2: Micro-batch Processing Model
# MAGIC %md
# MAGIC ## 🔄 Section 2: Micro-batch Processing Model
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC
# MAGIC Think of a conveyor belt at a factory. Instead of processing items one by one (which would be slow), workers collect items in small batches every few seconds and process them together. This is **micro-batching**!
# MAGIC
# MAGIC **Micro-batch** = Small batch processed very quickly at regular intervals  
# MAGIC **Result** = Near real-time processing with batch efficiency
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Micro-batch processing** is Spark's approach to streaming where data is collected over short time intervals (triggers) and processed as small batches using the Spark execution engine.
# MAGIC
# MAGIC #### How It Works:
# MAGIC
# MAGIC ```
# MAGIC Time: 0s -----> 5s -----> 10s -----> 15s -----> 20s
# MAGIC       |         |          |          |          |
# MAGIC       [Batch 1] [Batch 2]  [Batch 3]  [Batch 4]  ...
# MAGIC       Process   Process    Process    Process
# MAGIC ```
# MAGIC
# MAGIC #### Key Components:
# MAGIC
# MAGIC 1. **Trigger Interval**: How often to process data (e.g., every 5 seconds)
# MAGIC 2. **Batch Window**: The time window of data in each batch
# MAGIC 3. **Processing Time**: Time taken to process each micro-batch
# MAGIC 4. **Checkpoint**: Persistent state for fault tolerance
# MAGIC
# MAGIC #### Trigger Types:
# MAGIC
# MAGIC | Trigger Type | Description | Use Case |
# MAGIC |--------------|-------------|----------|
# MAGIC | **Default** | As fast as possible (when previous batch completes) | Low latency |
# MAGIC | **Fixed Interval** | Every N seconds/minutes | Controlled throughput |
# MAGIC | **Once** | Process available data once and stop | Testing, backfill |
# MAGIC | **Available Now** | Process all available data in multiple batches, then stop | Catch-up processing |
# MAGIC | **Continuous** | Experimental low-latency mode (~1ms) | Ultra-low latency |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔍 Advantages of Micro-batch:
# MAGIC
# MAGIC ✅ **Benefits:**
# MAGIC * Leverages Spark's mature batch processing optimizations
# MAGIC * Fault tolerance through checkpointing
# MAGIC * Exactly-once processing guarantees
# MAGIC * Easy to reason about and debug
# MAGIC * Good balance between latency and throughput
# MAGIC
# MAGIC ⚠️ **Limitations:**
# MAGIC * Minimum latency is bound by trigger interval
# MAGIC * Not suitable for sub-second latency requirements
# MAGIC * State management overhead for very small batches
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 3: Streaming Architecture
# MAGIC %md
# MAGIC ## 🏗️ Section 3: Streaming Architecture
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC
# MAGIC Think of a streaming pipeline like a restaurant kitchen:
# MAGIC
# MAGIC 1. **Source** = Orders coming in (customers)
# MAGIC 2. **Streaming Engine** = Chef's station (processing)
# MAGIC 3. **Transformation** = Cooking (adding value)
# MAGIC 4. **Sink** = Serving dishes (output)
# MAGIC
# MAGIC Orders flow continuously, chefs process them in the order they arrive, and dishes are served immediately!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC A streaming architecture consists of four core components:
# MAGIC
# MAGIC ```
# MAGIC ┌──────────┐     ┌──────────────────┐     ┌────────────────┐     ┌──────────┐
# MAGIC │  SOURCE  │ ──> │ STREAMING ENGINE │ ──> │  PROCESSING   │ ──> │   SINK   │
# MAGIC └──────────┘     └──────────────────┘     └────────────────┘     └──────────┘
# MAGIC   Kafka,         Spark              Filter,            Delta,
# MAGIC   Files,       Structured          Transform,          Database,
# MAGIC   IoT Hub      Streaming           Aggregate           Dashboard
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📦 1. Source (Input)
# MAGIC
# MAGIC **Supported Sources:**
# MAGIC * **File-based**: JSON, CSV, Parquet, Delta, ORC (in Unity Catalog Volumes)
# MAGIC * **Messaging**: Kafka, Event Hubs, Kinesis, Pub/Sub
# MAGIC * **Databases**: CDC streams, Delta tables
# MAGIC * **Cloud Storage**: S3, ADLS, GCS (with Auto Loader)
# MAGIC * **Network**: Socket streams (for testing)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚙️ 2. Streaming Engine
# MAGIC
# MAGIC **Spark Structured Streaming Engine:**
# MAGIC * Reads data incrementally from source
# MAGIC * Manages micro-batch execution
# MAGIC * Handles fault tolerance via checkpointing
# MAGIC * Tracks processed data to avoid duplicates
# MAGIC * Manages stateful operations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔧 3. Processing (Transformations)
# MAGIC
# MAGIC **Available Operations:**
# MAGIC * **Stateless**: `select`, `filter`, `map`, `flatMap`, `explode`
# MAGIC * **Stateful**: `groupBy`, `aggregations`, `window`, `watermark`
# MAGIC * **Joins**: Stream-static, stream-stream
# MAGIC * **Deduplication**: `dropDuplicates` with watermark
# MAGIC * **Custom Logic**: UDFs, complex transformations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📤 4. Sink (Output)
# MAGIC
# MAGIC **Supported Sinks:**
# MAGIC * **Delta Lake**: ACID transactions, time travel (RECOMMENDED)
# MAGIC * **Files**: Parquet, JSON, CSV, ORC
# MAGIC * **Databases**: JDBC sinks
# MAGIC * **Messaging**: Kafka
# MAGIC * **Console**: For testing/debugging
# MAGIC * **Memory**: In-memory table for testing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🌐 Real-World Streaming Architectures:
# MAGIC
# MAGIC #### Example 1: Fraud Detection
# MAGIC ```
# MAGIC Transaction Stream (Kafka) 
# MAGIC   → Structured Streaming 
# MAGIC   → Fraud Detection Model 
# MAGIC   → Flagged Transactions (Delta) 
# MAGIC   → Alerts (Downstream System)
# MAGIC ```
# MAGIC
# MAGIC #### Example 2: IoT Analytics
# MAGIC ```
# MAGIC Sensor Data (IoT Hub) 
# MAGIC   → Structured Streaming 
# MAGIC   → Aggregations (per device, per hour) 
# MAGIC   → Delta Lake 
# MAGIC   → Real-time Dashboard
# MAGIC ```
# MAGIC
# MAGIC #### Example 3: Log Aggregation
# MAGIC ```
# MAGIC Application Logs (Files) 
# MAGIC   → Auto Loader 
# MAGIC   → Parse & Filter 
# MAGIC   → Delta Tables 
# MAGIC   → Monitoring & Alerting
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 4: Batch vs Streaming Comparison
# MAGIC %md
# MAGIC ## ⚖️ Section 4: Batch vs Streaming — Detailed Comparison
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC
# MAGIC **Batch** = Like doing laundry once a week — collect all clothes, wash in one big load  
# MAGIC **Streaming** = Like a dishwasher that runs continuously — dishes are cleaned as soon as they arrive
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Comprehensive Comparison Table:
# MAGIC
# MAGIC | **Feature** | **Batch Processing** | **Streaming Processing** |
# MAGIC |-------------|---------------------|-------------------------|
# MAGIC | **Data Type** | Bounded (finite) | Unbounded (infinite) |
# MAGIC | **Processing** | All data at once | Incremental, continuous |
# MAGIC | **Latency** | High (minutes to hours) | Low (seconds to minutes) |
# MAGIC | **Throughput** | Very high | Moderate to high |
# MAGIC | **Complexity** | Lower | Higher (state management) |
# MAGIC | **Cost** | Lower (batch processing) | Higher (always running) |
# MAGIC | **Use Case** | Historical analysis | Real-time insights |
# MAGIC | **Data Arrival** | All data available upfront | Data arrives continuously |
# MAGIC | **Resource Usage** | Peak during job run | Continuous |
# MAGIC | **Fault Tolerance** | Restart entire job | Checkpoint-based recovery |
# MAGIC | **State Management** | Not required | Required for aggregations |
# MAGIC | **Output** | Complete results at end | Incremental results |
# MAGIC | **Example** | Monthly sales report | Live fraud detection |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔍 When to Choose Batch vs Streaming?
# MAGIC
# MAGIC #### Choose **Batch** When:
# MAGIC ✅ Data is naturally collected in batches (daily logs, monthly reports)  
# MAGIC ✅ Latency requirements are relaxed (hours/days acceptable)  
# MAGIC ✅ Complex transformations requiring full dataset view  
# MAGIC ✅ Cost optimization is priority  
# MAGIC ✅ Historical analysis and reporting  
# MAGIC
# MAGIC #### Choose **Streaming** When:
# MAGIC ✅ Real-time insights needed (seconds to minutes)  
# MAGIC ✅ Data arrives continuously (sensors, clickstreams, logs)  
# MAGIC ✅ Event-driven actions required (alerts, triggers)  
# MAGIC ✅ Incremental processing preferred  
# MAGIC ✅ Low-latency requirements  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Modern Approach: Lambda Architecture
# MAGIC
# MAGIC Many organizations use **both**:
# MAGIC
# MAGIC ```
# MAGIC               ┌─────────────────────┐
# MAGIC               │   Data Source      │
# MAGIC               └─────────┬──────────┘
# MAGIC                        │
# MAGIC           ┌────────┼────────┐
# MAGIC           │            │         │
# MAGIC     ┌─────┴────┐   ┌────┴─────┐
# MAGIC     │ Streaming │   │  Batch   │
# MAGIC     │  (Fast)  │   │ (Accurate)│
# MAGIC     └────┬─────┘   └───┬──────┘
# MAGIC          │             │
# MAGIC          └────┬──────┘
# MAGIC               │
# MAGIC       ┌───────┴───────┐
# MAGIC       │ Serving Layer │
# MAGIC       └───────────────┘
# MAGIC ```
# MAGIC
# MAGIC * **Speed Layer** (Streaming): Real-time approximate results
# MAGIC * **Batch Layer**: Accurate historical results
# MAGIC * **Serving Layer**: Combines both for complete view
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 5: Reading and Writing Streaming Data
# MAGIC %md
# MAGIC ## 📝 Section 5: Reading and Writing Streaming Data
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC
# MAGIC Reading streaming data is like watching a movie that never ends — you keep watching new scenes as they're released. Writing is like taking notes continuously as you watch!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📚 Reading Streaming Data
# MAGIC
# MAGIC #### Basic Pattern:
# MAGIC
# MAGIC ```python
# MAGIC df_stream = spark.readStream \
# MAGIC     .format("<source_format>") \
# MAGIC     .option("<option_key>", "<option_value>") \
# MAGIC     .schema(<schema>) \
# MAGIC     .load("<path>")
# MAGIC ```
# MAGIC
# MAGIC #### Key Differences from Batch:
# MAGIC
# MAGIC | **Batch** | **Streaming** |
# MAGIC |-----------|---------------|
# MAGIC | `spark.read` | `spark.readStream` |
# MAGIC | DataFrame | Streaming DataFrame |
# MAGIC | Immediate execution | Lazy (needs writeStream) |
# MAGIC | Full data | Incremental data |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📤 Writing Streaming Data
# MAGIC
# MAGIC #### Basic Pattern:
# MAGIC
# MAGIC ```python
# MAGIC query = df_stream.writeStream \
# MAGIC     .format("<sink_format>") \
# MAGIC     .outputMode("<mode>") \
# MAGIC     .option("checkpointLocation", "<checkpoint_path>") \
# MAGIC     .trigger(<trigger_type>) \
# MAGIC     .start("<output_path>")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Critical Requirement: Checkpointing
# MAGIC
# MAGIC **Checkpoint** = A persistent log of what data has been processed
# MAGIC
# MAGIC **Why Needed?**
# MAGIC * **Fault Tolerance**: Resume from where you left off after failures
# MAGIC * **Exactly-Once Semantics**: Prevent duplicate processing
# MAGIC * **State Management**: Store aggregation state
# MAGIC
# MAGIC **Best Practices:**
# MAGIC ✅ Always use Unity Catalog Volumes for checkpoint locations  
# MAGIC ✅ One checkpoint directory per streaming query  
# MAGIC ✅ Never delete checkpoints while query is running  
# MAGIC ✅ Use descriptive checkpoint paths  
# MAGIC
# MAGIC ```python
# MAGIC # Good checkpoint path
# MAGIC .option("checkpointLocation", "/Volumes/catalog/schema/volume/checkpoints/sales_stream/")
# MAGIC
# MAGIC # Bad - local storage
# MAGIC .option("checkpointLocation", "/tmp/checkpoint")  # ❌ Avoid!
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔧 Trigger Types:
# MAGIC
# MAGIC ```python
# MAGIC # Default: Process as fast as possible
# MAGIC .trigger()
# MAGIC
# MAGIC # Fixed Interval: Every 30 seconds
# MAGIC .trigger(processingTime="30 seconds")
# MAGIC
# MAGIC # Once: Process available data once and stop
# MAGIC .trigger(once=True)
# MAGIC
# MAGIC # Available Now: Process all available data in batches, then stop
# MAGIC .trigger(availableNow=True)
# MAGIC
# MAGIC # Continuous: Experimental, ~1ms latency
# MAGIC .trigger(continuous="1 second")
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 6: Output Modes Explained
# MAGIC %md
# MAGIC ## 📦 Section 6: Output Modes Explained
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC
# MAGIC Think of a scoreboard during a game:
# MAGIC
# MAGIC * **Append** = Only show new scores (new points scored)
# MAGIC * **Update** = Update existing scores (team totals change)
# MAGIC * **Complete** = Show entire scoreboard every time (full picture)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Output Modes in Detail:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 1️⃣ Append Mode
# MAGIC
# MAGIC **Definition**: Only new rows added to the result table are written to the sink.
# MAGIC
# MAGIC **When to Use:**
# MAGIC * Stateless transformations (filter, select, map)
# MAGIC * Time-windowed aggregations with watermark
# MAGIC * Event logs, audit trails
# MAGIC
# MAGIC **Characteristics:**
# MAGIC * ✅ Most efficient (only writes new data)
# MAGIC * ✅ Works with all sinks
# MAGIC * ❌ Cannot update existing rows
# MAGIC
# MAGIC **Example:**
# MAGIC ```python
# MAGIC df_stream.writeStream \
# MAGIC     .outputMode("append") \
# MAGIC     .format("delta") \
# MAGIC     .start()
# MAGIC ```
# MAGIC
# MAGIC **Use Case**: Log ingestion, event capture
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2️⃣ Update Mode
# MAGIC
# MAGIC **Definition**: Only rows that were updated in the result table are written to the sink.
# MAGIC
# MAGIC **When to Use:**
# MAGIC * Stateful aggregations (groupBy, window)
# MAGIC * Operations that update existing results
# MAGIC * Real-time dashboards
# MAGIC
# MAGIC **Characteristics:**
# MAGIC * ✅ Efficient for changing aggregations
# MAGIC * ✅ Only writes changed rows
# MAGIC * ❌ Requires sink that supports updates (Delta, databases)
# MAGIC
# MAGIC **Example:**
# MAGIC ```python
# MAGIC df_stream.groupBy("user_id").count() \
# MAGIC     .writeStream \
# MAGIC     .outputMode("update") \
# MAGIC     .format("delta") \
# MAGIC     .start()
# MAGIC ```
# MAGIC
# MAGIC **Use Case**: Running aggregations, metrics dashboards
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 3️⃣ Complete Mode
# MAGIC
# MAGIC **Definition**: The entire result table is written to the sink after every trigger.
# MAGIC
# MAGIC **When to Use:**
# MAGIC * Small result sets
# MAGIC * Aggregations without watermark
# MAGIC * Testing and debugging
# MAGIC
# MAGIC **Characteristics:**
# MAGIC * ✅ Simple to understand
# MAGIC * ✅ Always shows full picture
# MAGIC * ❌ Very inefficient for large results
# MAGIC * ❌ High storage/compute cost
# MAGIC
# MAGIC **Example:**
# MAGIC ```python
# MAGIC df_stream.groupBy("category").count() \
# MAGIC     .writeStream \
# MAGIC     .outputMode("complete") \
# MAGIC     .format("memory") \
# MAGIC     .start()
# MAGIC ```
# MAGIC
# MAGIC **Use Case**: Small lookup tables, testing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Output Mode Compatibility:
# MAGIC
# MAGIC | **Operation** | **Append** | **Update** | **Complete** |
# MAGIC |---------------|------------|------------|-------------|
# MAGIC | **Stateless** (select, filter) | ✅ | ✅ | ❌ |
# MAGIC | **GroupBy Aggregation** (no watermark) | ❌ | ✅ | ✅ |
# MAGIC | **GroupBy Aggregation** (with watermark) | ✅ | ✅ | ❌ |
# MAGIC | **Window + Watermark** | ✅ | ✅ | ❌ |
# MAGIC | **MapGroupsWithState** | ✅ | ✅ | ❌ |
# MAGIC | **FlatMapGroupsWithState (append)** | ✅ | ❌ | ❌ |
# MAGIC | **FlatMapGroupsWithState (update)** | ❌ | ✅ | ❌ |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧠 Choosing the Right Output Mode:
# MAGIC
# MAGIC ```
# MAGIC Do you need to update existing rows?
# MAGIC   │
# MAGIC   ├── NO ───> Use APPEND
# MAGIC   │
# MAGIC   └── YES ──> Is result set small (<1000 rows)?
# MAGIC               │
# MAGIC               ├── YES ──> Consider COMPLETE (for simplicity)
# MAGIC               │
# MAGIC               └── NO ───> Use UPDATE (efficient)
# MAGIC ```
# MAGIC
# MAGIC **Default Choice**: Start with **APPEND** — most efficient and works with all sinks.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 7: Hands-on Streaming Pipeline
# MAGIC %md
# MAGIC ## 🔧 Section 7: Hands-on Streaming Pipeline
# MAGIC
# MAGIC ### 🎯 Objective:
# MAGIC Build a complete end-to-end streaming pipeline that:
# MAGIC 1. Generates sample streaming data (simulating real-time events)
# MAGIC 2. Reads the data as a stream
# MAGIC 3. Applies transformations
# MAGIC 4. Writes to Delta Lake with checkpointing
# MAGIC 5. Monitors the streaming query
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📄 Pipeline Architecture:
# MAGIC
# MAGIC ```
# MAGIC [Sample Data Generator] 
# MAGIC         ↓
# MAGIC [Unity Catalog Volume]
# MAGIC         ↓
# MAGIC [Read Stream - CSV]
# MAGIC         ↓
# MAGIC [Transformations]
# MAGIC         ↓
# MAGIC [Write Stream - Delta]
# MAGIC         ↓
# MAGIC [Delta Table Output]
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚦 Let's Build It Step-by-Step!
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Step 1: Setup - Define Paths
# Step 1: Setup - Define Unity Catalog Volume paths
# These paths will be used for source data, checkpoints, and output

import time
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define your Unity Catalog paths
# Replace with your actual catalog.schema.volume
catalog = "main"  # Your catalog name
schema = "default"  # Your schema name  
volume = "streaming_demo"  # Volume name

# Create volume if it doesn't exist (uncomment if needed)
# spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

# Define paths
base_path = f"/Volumes/{catalog}/{schema}/{volume}"
source_path = f"{base_path}/stream_source/"
checkpoint_path = f"{base_path}/checkpoints/sales_stream/"
output_path = f"{base_path}/output/sales_stream/"

print("\u2705 Paths configured:")
print(f"Source Path: {source_path}")
print(f"Checkpoint Path: {checkpoint_path}")
print(f"Output Path: {output_path}")
print("\n⚠️ Note: Make sure the volume exists in Unity Catalog before running the streaming query")

# COMMAND ----------

# DBTITLE 1,Step 2: Generate Sample Streaming Data
# Step 2: Generate sample streaming data to simulate real-time events
# This creates sample sales transaction data

from pyspark.sql import Row
import random
import builtins  # Access Python's built-in round function

# Sample data generator function
def generate_sample_data(num_records=100):
    """
    Generates sample sales transaction data
    """
    products = ["Laptop", "Phone", "Tablet", "Headphones", "Monitor", "Keyboard", "Mouse"]
    regions = ["North", "South", "East", "West"]
    
    data = []
    for i in range(num_records):
        transaction = {
            "transaction_id": f"TXN{str(i+1).zfill(6)}",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "product": random.choice(products),
            "quantity": random.randint(1, 10),
            "price": builtins.round(random.uniform(10.0, 1000.0), 2),  # Use Python's built-in round
            "region": random.choice(regions)
        }
        data.append(transaction)
    
    return data

# Generate sample data
sample_data = generate_sample_data(50)

# Create DataFrame
df_sample = spark.createDataFrame(sample_data)

# Display sample data
print("✅ Generated sample streaming data:")
display(df_sample.limit(10))

print(f"\n📊 Total records generated: {df_sample.count()}")

# COMMAND ----------

# DBTITLE 1,Step 3: Write Sample Data to Source Location
# Step 3: Write sample data to source location
# This simulates data arriving in real-time

# Write to source path (this will be our streaming source)
df_sample.write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(source_path)

print(f"✅ Sample data written to: {source_path}")
print("👉 This data will be read as a stream in the next step")

# Note: In production, data would arrive continuously from sources like:
# - Kafka streams
# - IoT devices  
# - Application logs
# - Database CDC
# - Cloud storage (Auto Loader)

# COMMAND ----------

# DBTITLE 1,Step 4: Define Schema for Streaming Read
# Step 4: Define schema for streaming read
# Schema definition is REQUIRED for streaming reads (best practice)

# Define the schema explicitly
streaming_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("region", StringType(), True)
])

print("✅ Schema defined for streaming read:")
print(streaming_schema)

print("\n⚠️ Why define schema?")
print("1. Performance: Avoids schema inference overhead")
print("2. Reliability: Prevents schema evolution issues")
print("3. Predictability: Ensures consistent data types")

# COMMAND ----------

# DBTITLE 1,Step 5: Read Streaming Data
# Step 5: Read streaming data from source location
# This creates a Streaming DataFrame

# Read stream from CSV files
df_stream = spark.readStream \
    .format("csv") \
    .schema(streaming_schema) \
    .option("header", "true") \
    .option("maxFilesPerTrigger", 1) \
    .load(source_path)

print("✅ Streaming DataFrame created")
print(f"📊 Is Streaming? {df_stream.isStreaming}")
print("\n📊 Schema:")
df_stream.printSchema()

print("\n⚡ Key Points:")
print("- readStream creates a streaming DataFrame")
print("- maxFilesPerTrigger controls throughput (1 file per batch)")
print("- Data hasn't been processed yet (lazy evaluation)")
print("- Need writeStream to start actual processing")

# COMMAND ----------

# DBTITLE 1,Step 6: Apply Transformations
# Step 6: Apply transformations to streaming data
# Add business logic and calculations

# Apply transformations
df_transformed = df_stream \
    .withColumn("total_amount", col("quantity") * col("price")) \
    .withColumn("processing_time", current_timestamp()) \
    .withColumn("event_date", to_date(col("timestamp"))) \
    .filter(col("quantity") > 0) \
    .select(
        "transaction_id",
        "timestamp",
        "product",
        "quantity",
        "price",
        "total_amount",
        "region",
        "event_date",
        "processing_time"
    )

print("✅ Transformations applied:")
print("1. Calculated total_amount (quantity * price)")
print("2. Added processing_time timestamp")
print("3. Extracted event_date")
print("4. Filtered out invalid quantities")
print("5. Selected relevant columns")

print("\n📊 Transformed Schema:")
df_transformed.printSchema()

# COMMAND ----------

# DBTITLE 1,Step 7: Write Streaming Data to Delta
# Step 7: Write streaming data to Delta Lake
# This starts the actual streaming query execution

print("🚀 Starting streaming query...")
print("⚠️ Using 'availableNow' trigger for Serverless compatibility")

# Write stream to Delta Lake
# Note: Serverless doesn't support continuous triggers (processingTime)
# Using availableNow=True which processes all available data in batches then stops
streaming_query = df_transformed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(availableNow=True) \
    .start(output_path)

# Wait for query to complete (availableNow processes then stops)
streaming_query.awaitTermination()

print(f"\n✅ Streaming query completed!")
print(f"Query ID: {streaming_query.id}")

print("\n📊 Streaming Query Details:")
print(f"- Format: Delta Lake")
print(f"- Output Mode: Append")
print(f"- Trigger: Available Now (Serverless compatible)")
print(f"- Checkpoint: {checkpoint_path}")
print(f"- Output: {output_path}")
print("\n💡 Note: 'availableNow' processes all available data and stops automatically")
print("This is ideal for serverless compute and batch-like streaming processing")

# COMMAND ----------

# DBTITLE 1,Step 8: Monitor Streaming Query
# Step 8: Monitor the streaming query
# Check query status and progress

print("📊 Streaming Query Status:")
print(f"Is Active: {streaming_query.isActive}")
print(f"\n✅ Query has completed processing (availableNow trigger)")

# Get progress information
recent_progress = streaming_query.recentProgress

if recent_progress:
    print("\n📊 Recent Progress Summary:")
    for idx, progress in enumerate(recent_progress, 1):
        print(f"\nBatch {idx}:")
        print(f"  - Batch ID: {progress.get('batchId', 'N/A')}")
        print(f"  - Input Rows: {progress.get('numInputRows', 0)}")
        print(f"  - Processing Time: {progress.get('durationMs', {}).get('triggerExecution', 0)} ms")
else:
    print("⚠️ No progress data available")

# Display final status
print("\n📊 Final Status:")
print(streaming_query.status)

print("\n💡 Key Points:")
print("- availableNow trigger processed all data and stopped automatically")
print("- Perfect for serverless compute and batch-like streaming")
print("- Check Spark UI for detailed metrics and execution plans")

# COMMAND ----------

# DBTITLE 1,Step 9: Query the Output Delta Table
# Step 9: Query the output Delta table
# Verify that data was written successfully

print("🔍 Querying output Delta table...\n")

# Read from Delta table (batch read)
df_output = spark.read.format("delta").load(output_path)

print(f"✅ Total records in output table: {df_output.count()}")

print("\n📊 Sample output data:")
display(df_output.limit(10))

print("\n📊 Aggregated insights:")

# Show aggregations
df_aggregated = df_output.groupBy("product") \
    .agg(
        count("transaction_id").alias("transaction_count"),
        sum("total_amount").alias("total_revenue"),
        avg("quantity").alias("avg_quantity")
    ) \
    .orderBy(desc("total_revenue"))

display(df_aggregated)

print("\n✅ Streaming pipeline successfully processed and wrote data to Delta Lake!")

# COMMAND ----------

# DBTITLE 1,Step 10: Stop Streaming Query
# Step 10: Verify Streaming Query Completion
# With availableNow trigger, the query stops automatically after processing

print("📊 Verifying streaming query status...")

if not streaming_query.isActive:
    print("✅ Streaming query stopped successfully (as expected with availableNow)")
    print(f"\n📊 Final Status:")
    print(f"  - Query ID: {streaming_query.id}")
    print(f"  - Is Active: {streaming_query.isActive}")
    print(f"  - Exception: {streaming_query.exception()}")
else:
    print("⚠️ Query is still active (unexpected)")
    # Try to stop it
    try:
        streaming_query.stop()
        print("✅ Manually stopped the query")
    except Exception as e:
        print(f"⚠️ Error stopping query: {e}")

print("\n🧹 Pipeline execution complete!")
print("\n💡 Key Takeaways:")
print("1. availableNow trigger is perfect for serverless compute")
print("2. It processes all available data in batches then stops automatically")
print("3. Checkpoints enable fault tolerance and resumability")
print("4. Delta Lake provides ACID guarantees for streaming data")
print("5. Use display() or batch reads to query streaming outputs")

# COMMAND ----------

# DBTITLE 1,Bonus: Reset and Rerun Pipeline
# MAGIC %md
# MAGIC ## 🔄 Bonus: Reset and Rerun the Streaming Pipeline
# MAGIC
# MAGIC ### 📝 Note:
# MAGIC
# MAGIC If you want to rerun the streaming pipeline from scratch (which will process the data again), you need to:
# MAGIC
# MAGIC 1. **Clear the checkpoint directory** - This removes the streaming state
# MAGIC 2. **Clear the output directory** - This removes previous results
# MAGIC 3. **Optionally regenerate source data** - Create new sample data
# MAGIC
# MAGIC ### ⚠️ Important:
# MAGIC
# MAGIC **Checkpoints** track which data has been processed. If you don't clear checkpoints, the streaming query will skip already-processed files (exactly-once guarantee in action!).
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Reset Pipeline - Clear Checkpoints and Output
# OPTIONAL: Reset the streaming pipeline
# Run this cell if you want to reprocess the data

import shutil
import os

print("🧹 Resetting streaming pipeline...\n")

try:
    # Clear checkpoint directory
    dbutils.fs.rm(checkpoint_path, True)
    print(f"✅ Cleared checkpoint: {checkpoint_path}")
    
    # Clear output directory
    dbutils.fs.rm(output_path, True)
    print(f"✅ Cleared output: {output_path}")
    
    print("\n✅ Reset complete! You can now rerun cells 9-18 to execute the pipeline again.")
    print("\n💡 Tip: This demonstrates how checkpoints work - clearing them allows reprocessing.")
    
except Exception as e:
    print(f"⚠️ Error during reset: {e}")
    print("This is normal if directories don't exist yet.")

# COMMAND ----------

# DBTITLE 1,Notebook Validation Summary
# MAGIC %md
# MAGIC ## ✅ Notebook Validation Summary
# MAGIC
# MAGIC ### 🎯 Training Objectives Achieved:
# MAGIC
# MAGIC ✅ **Section 1-3**: Comprehensive explanation of Structured Streaming fundamentals, micro-batch processing, and architecture  
# MAGIC ✅ **Section 4**: Detailed batch vs streaming comparison with decision framework  
# MAGIC ✅ **Section 5-6**: Reading/writing streaming data patterns and output modes explained  
# MAGIC ✅ **Section 7**: Complete 10-step hands-on streaming pipeline implemented  
# MAGIC ✅ **Section 8**: End-to-end architecture with medallion pattern  
# MAGIC ✅ **Genie Code Examples**: 16 practical prompts for AI assistance  
# MAGIC ✅ **Best Practices**: 9 categories of production-ready guidelines  
# MAGIC ✅ **Common Mistakes**: 10 critical pitfalls with solutions  
# MAGIC ✅ **Interview Questions**: 17 questions covering all levels  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚙️ Serverless Compliance:
# MAGIC
# MAGIC ✅ Uses Unity Catalog Volumes (not /tmp or local storage)  
# MAGIC ✅ Uses Delta Lake format for streaming sink  
# MAGIC ✅ Uses availableNow trigger (serverless compatible)  
# MAGIC ✅ DataFrame API only (no RDDs)  
# MAGIC ✅ No cache() or persist() calls  
# MAGIC ✅ Proper checkpointing implemented  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 What You've Learned:
# MAGIC
# MAGIC 1. **Streaming Fundamentals**: Unbounded data, incremental processing, exactly-once semantics
# MAGIC 2. **Micro-batch Model**: Trigger types, processing patterns, latency vs throughput
# MAGIC 3. **Architecture Design**: Source → Engine → Processing → Sink pattern
# MAGIC 4. **Output Modes**: Append, Update, Complete - when to use each
# MAGIC 5. **Production Patterns**: Checkpointing, fault tolerance, monitoring
# MAGIC 6. **Serverless Streaming**: availableNow trigger for batch-like streaming
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏆 Ready for Production:
# MAGIC
# MAGIC You can now:
# MAGIC * Build streaming ETL pipelines with Spark Structured Streaming
# MAGIC * Design medallion architectures for real-time data
# MAGIC * Choose appropriate triggers and output modes
# MAGIC * Implement fault-tolerant streaming with checkpoints
# MAGIC * Optimize streaming queries for performance
# MAGIC * Debug and monitor streaming applications
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📚 Next Steps:
# MAGIC
# MAGIC **Continue Learning:**
# MAGIC * Phase 5 Day 26: Advanced Watermarking & State Management
# MAGIC * Phase 5 Day 27: Kafka Integration & Event Streaming
# MAGIC * Phase 5 Day 28: Auto Loader for Incremental Ingestion
# MAGIC * Phase 5 Day 29: Stream-Stream Joins
# MAGIC * Phase 5 Day 30: Production Streaming Patterns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Quick Reference:
# MAGIC
# MAGIC **Key Concepts:**
# MAGIC * `spark.readStream` - Create streaming DataFrame
# MAGIC * `spark.writeStream` - Start streaming query
# MAGIC * `.trigger(availableNow=True)` - Serverless-compatible trigger
# MAGIC * `.option("checkpointLocation", path)` - Enable fault tolerance
# MAGIC * `.outputMode("append")` - Most efficient output mode
# MAGIC
# MAGIC **Serverless Constraints:**
# MAGIC * ❌ No `processingTime` trigger (continuous streaming)
# MAGIC * ✅ Use `availableNow` or `once` triggers
# MAGIC * ✅ Perfect for batch-like streaming workloads
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 8: End-to-End Streaming Architecture
# MAGIC %md
# MAGIC ## 🌐 Section 8: End-to-End Streaming Architecture
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC
# MAGIC Think of a streaming architecture like a smart city traffic system:
# MAGIC
# MAGIC 1. **Sensors** (Source) detect cars at intersections
# MAGIC 2. **Traffic Control Center** (Processing) analyzes traffic patterns
# MAGIC 3. **Traffic Lights** (Sink) adjust in real-time
# MAGIC 4. **Dashboard** (Analytics) shows city-wide traffic flow
# MAGIC
# MAGIC Everything works continuously, 24/7!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Production Streaming Architecture:
# MAGIC
# MAGIC ```
# MAGIC ┌───────────────────────────────────────────────────┐
# MAGIC │                    DATA SOURCES                          │
# MAGIC │  [Kafka] [IoT Hub] [Files] [CDC] [Event Hubs]            │
# MAGIC └───────────────────┬──────────────────────────────┘
# MAGIC                        │
# MAGIC                        │ Real-time Ingestion
# MAGIC                        │
# MAGIC ┌───────────────────┴──────────────────────────────┐
# MAGIC │           SPARK STRUCTURED STREAMING                     │
# MAGIC │                                                           │
# MAGIC │  ┌───────────────────────────────────────┐   │
# MAGIC │  │        BRONZE LAYER (Raw)               │   │
# MAGIC │  │  - Ingestion with Auto Loader           │   │
# MAGIC │  │  - Schema evolution                     │   │
# MAGIC │  └──────────────────┬────────────────────┘   │
# MAGIC │                   │                               │
# MAGIC │  ┌────────────────┴────────────────────┐   │
# MAGIC │  │        SILVER LAYER (Cleaned)           │   │
# MAGIC │  │  - Data validation                      │   │
# MAGIC │  │  - Transformations                      │   │
# MAGIC │  │  - Deduplication                        │   │
# MAGIC │  └────────────────┬────────────────────┘   │
# MAGIC │                   │                               │
# MAGIC │  ┌────────────────┴────────────────────┐   │
# MAGIC │  │        GOLD LAYER (Aggregated)          │   │
# MAGIC │  │  - Business aggregations                │   │
# MAGIC │  │  - Metrics & KPIs                       │   │
# MAGIC │  │  - Joined datasets                      │   │
# MAGIC │  └───────────────────────────────────────┘   │
# MAGIC │                                                           │
# MAGIC └───────────────────┬──────────────────────────────┘
# MAGIC                        │
# MAGIC                        │ Delta Lake Storage
# MAGIC                        │
# MAGIC ┌───────────────────┴──────────────────────────────┐
# MAGIC │                  CONSUMPTION LAYER                        │
# MAGIC │  [BI Tools] [SQL Analytics] [ML Models] [APIs]           │
# MAGIC └──────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🌟 Medallion Architecture for Streaming:
# MAGIC
# MAGIC #### 🥉 Bronze Layer (Raw Data):
# MAGIC * **Purpose**: Ingest raw data as-is
# MAGIC * **Technology**: Auto Loader for file-based sources
# MAGIC * **Characteristics**:
# MAGIC   - No transformations
# MAGIC   - Schema evolution enabled
# MAGIC   - Append-only
# MAGIC   - Audit trail maintained
# MAGIC
# MAGIC #### 🥈 Silver Layer (Cleaned Data):
# MAGIC * **Purpose**: Clean, validate, and standardize
# MAGIC * **Transformations**:
# MAGIC   - Data type conversions
# MAGIC   - Null handling
# MAGIC   - Deduplication
# MAGIC   - Data validation
# MAGIC   - PII masking
# MAGIC * **Output Mode**: Append or Update
# MAGIC
# MAGIC #### 🥇 Gold Layer (Business-Ready):
# MAGIC * **Purpose**: Aggregated business metrics
# MAGIC * **Transformations**:
# MAGIC   - Aggregations (sum, avg, count)
# MAGIC   - Joins with dimension tables
# MAGIC   - Time-windowed calculations
# MAGIC   - KPIs and metrics
# MAGIC * **Output Mode**: Update or Complete (for small aggregations)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚡ Real-Time Processing Patterns:
# MAGIC
# MAGIC #### 1️⃣ Real-Time Aggregation:
# MAGIC ```python
# MAGIC df_stream.groupBy(
# MAGIC     window(col("timestamp"), "5 minutes"),
# MAGIC     col("product")
# MAGIC ).agg(sum("revenue")).writeStream...
# MAGIC ```
# MAGIC
# MAGIC #### 2️⃣ Stream-Static Join:
# MAGIC ```python
# MAGIC # Enrich streaming data with dimension table
# MAGIC df_enriched = df_stream.join(
# MAGIC     df_products,  # Static DataFrame
# MAGIC     "product_id"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC #### 3️⃣ Deduplication:
# MAGIC ```python
# MAGIC df_deduplicated = df_stream \
# MAGIC     .withWatermark("timestamp", "10 minutes") \
# MAGIC     .dropDuplicates(["transaction_id"])
# MAGIC ```
# MAGIC
# MAGIC #### 4️⃣ Late Data Handling:
# MAGIC ```python
# MAGIC df_windowed = df_stream \
# MAGIC     .withWatermark("event_time", "1 hour") \
# MAGIC     .groupBy(window(col("event_time"), "10 minutes")) \
# MAGIC     .count()
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛡️ Fault Tolerance & Reliability:
# MAGIC
# MAGIC **Key Mechanisms:**
# MAGIC
# MAGIC 1. **Checkpointing**:
# MAGIC    - Tracks processing progress
# MAGIC    - Enables exactly-once semantics
# MAGIC    - Stores state for aggregations
# MAGIC
# MAGIC 2. **Write-Ahead Logs (WAL)**:
# MAGIC    - Records every batch before processing
# MAGIC    - Enables recovery from failures
# MAGIC
# MAGIC 3. **Idempotent Writes**:
# MAGIC    - Delta Lake ensures atomic commits
# MAGIC    - Duplicate writes are handled gracefully
# MAGIC
# MAGIC 4. **Automatic Retries**:
# MAGIC    - Transient failures are retried automatically
# MAGIC    - Configurable retry policies
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Monitoring & Observability:
# MAGIC
# MAGIC **Key Metrics to Monitor:**
# MAGIC
# MAGIC * **Input Rate**: Records/second from source
# MAGIC * **Processing Rate**: Records/second processed
# MAGIC * **Batch Duration**: Time to process each micro-batch
# MAGIC * **End-to-End Latency**: Time from ingestion to output
# MAGIC * **State Size**: Memory used for stateful operations
# MAGIC * **Watermark Delay**: How far behind watermark is
# MAGIC
# MAGIC **Monitoring Tools:**
# MAGIC * Spark UI Streaming Tab
# MAGIC * Structured Streaming Metrics
# MAGIC * Databricks Job Monitoring
# MAGIC * CloudWatch / Azure Monitor
# MAGIC * Custom metrics via StreamingQueryListener
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Genie Code Agent - Streaming Examples
# MAGIC %md
# MAGIC ## 🧞 Genie Code Agent — Streaming Prompts
# MAGIC
# MAGIC ### 💬 How to Use Genie Code for Streaming Tasks:
# MAGIC
# MAGIC Genie Code can help you build, debug, and optimize streaming pipelines. Here are example prompts:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛠️ Building Pipelines:
# MAGIC
# MAGIC **Prompt 1**: "Build a streaming pipeline that reads JSON files from Unity Catalog Volumes and writes to Delta"
# MAGIC
# MAGIC **Prompt 2**: "Create a streaming aggregation that counts events per product every 5 minutes"
# MAGIC
# MAGIC **Prompt 3**: "Set up a streaming deduplication pipeline with watermarking"
# MAGIC
# MAGIC **Prompt 4**: "Convert my batch processing code to structured streaming"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔍 Understanding Concepts:
# MAGIC
# MAGIC **Prompt 5**: "Explain the difference between append, update, and complete output modes"
# MAGIC
# MAGIC **Prompt 6**: "What is watermarking and when should I use it?"
# MAGIC
# MAGIC **Prompt 7**: "How does checkpointing work in structured streaming?"
# MAGIC
# MAGIC **Prompt 8**: "Compare micro-batch vs continuous processing"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚙️ Optimization & Debugging:
# MAGIC
# MAGIC **Prompt 9**: "My streaming query is slow. How can I optimize it?"
# MAGIC
# MAGIC **Prompt 10**: "How do I handle late-arriving data in my streaming pipeline?"
# MAGIC
# MAGIC **Prompt 11**: "Debug why my streaming query is not processing new files"
# MAGIC
# MAGIC **Prompt 12**: "Show me how to monitor streaming query performance"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architecture Design:
# MAGIC
# MAGIC **Prompt 13**: "Design a medallion architecture for real-time IoT data"
# MAGIC
# MAGIC **Prompt 14**: "Build a fraud detection streaming pipeline"
# MAGIC
# MAGIC **Prompt 15**: "Create a streaming ETL pipeline with bronze, silver, and gold layers"
# MAGIC
# MAGIC **Prompt 16**: "How do I join streaming data with a static dimension table?"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Pro Tip:
# MAGIC Be specific in your prompts! Include:
# MAGIC * Data source type (Kafka, files, etc.)
# MAGIC * Desired transformations
# MAGIC * Output requirements
# MAGIC * Performance constraints
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Best Practices for Structured Streaming
# MAGIC %md
# MAGIC ## ✅ Best Practices for Structured Streaming
# MAGIC
# MAGIC ### 🎯 Production-Ready Streaming Guidelines:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 1️⃣ Checkpointing:
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC * Always specify checkpointLocation
# MAGIC * Use Unity Catalog Volumes for checkpoints
# MAGIC * One checkpoint directory per streaming query
# MAGIC * Never share checkpoint locations between queries
# MAGIC
# MAGIC ❌ **DON'T:**
# MAGIC * Use /tmp or local paths for checkpoints
# MAGIC * Delete checkpoints while query is running
# MAGIC * Reuse checkpoint locations for different queries
# MAGIC
# MAGIC ```python
# MAGIC # Good
# MAGIC .option("checkpointLocation", "/Volumes/catalog/schema/volume/checkpoints/query1/")
# MAGIC
# MAGIC # Bad
# MAGIC .option("checkpointLocation", "/tmp/checkpoint")  # ❌ Avoid!
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ Schema Management:
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC * Define schemas explicitly for streaming reads
# MAGIC * Use schema evolution carefully
# MAGIC * Validate schema changes before production
# MAGIC
# MAGIC ```python
# MAGIC # Explicit schema (recommended)
# MAGIC df_stream = spark.readStream \
# MAGIC     .schema(my_schema) \
# MAGIC     .format("json") \
# MAGIC     .load(path)
# MAGIC
# MAGIC # Schema inference (avoid in production)
# MAGIC df_stream = spark.readStream \
# MAGIC     .format("json") \
# MAGIC     .load(path)  # ❌ Can cause issues
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ Trigger Configuration:
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC * Choose appropriate trigger based on latency requirements
# MAGIC * Monitor batch processing times
# MAGIC * Adjust trigger interval if batches take too long
# MAGIC
# MAGIC ```python
# MAGIC # For low latency
# MAGIC .trigger(processingTime="5 seconds")
# MAGIC
# MAGIC # For high throughput
# MAGIC .trigger(processingTime="5 minutes")
# MAGIC
# MAGIC # For testing/backfill
# MAGIC .trigger(once=True)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4️⃣ State Management:
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC * Use watermarking for time-based aggregations
# MAGIC * Set appropriate watermark delay
# MAGIC * Monitor state size growth
# MAGIC * Clean up old state with TTL
# MAGIC
# MAGIC ```python
# MAGIC df_stream \
# MAGIC     .withWatermark("event_time", "1 hour") \
# MAGIC     .groupBy(window("event_time", "10 minutes")) \
# MAGIC     .count()
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5️⃣ Output Modes:
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC * Use "append" for most use cases (most efficient)
# MAGIC * Use "update" for aggregations that need incremental updates
# MAGIC * Avoid "complete" for large result sets
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6️⃣ Error Handling:
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC * Implement retry logic for transient failures
# MAGIC * Use dead letter queues for bad records
# MAGIC * Monitor query failures and alerts
# MAGIC * Log errors for debugging
# MAGIC
# MAGIC ```python
# MAGIC # Bad records handling
# MAGIC df_stream = spark.readStream \
# MAGIC     .format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "json") \
# MAGIC     .option("badRecordsPath", "/Volumes/.../bad_records/") \
# MAGIC     .load(path)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 7️⃣ Resource Management:
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC * Use auto-scaling clusters for variable workloads
# MAGIC * Monitor cluster resource usage
# MAGIC * Set appropriate shuffle partitions
# MAGIC * Avoid caching in streaming (not needed)
# MAGIC
# MAGIC ```python
# MAGIC # Configure shuffle partitions
# MAGIC spark.conf.set("spark.sql.shuffle.partitions", "200")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 8️⃣ Testing:
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC * Test with small datasets first
# MAGIC * Use memory sink for debugging
# MAGIC * Validate exactly-once semantics
# MAGIC * Test failure recovery
# MAGIC
# MAGIC ```python
# MAGIC # Testing with memory sink
# MAGIC df_stream.writeStream \
# MAGIC     .format("memory") \
# MAGIC     .queryName("test_query") \
# MAGIC     .start()
# MAGIC
# MAGIC # Query results
# MAGIC spark.sql("SELECT * FROM test_query").show()
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 9️⃣ Monitoring:
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC * Monitor input/processing rates
# MAGIC * Track batch durations
# MAGIC * Set up alerts for query failures
# MAGIC * Use Spark UI for debugging
# MAGIC * Check watermark delays
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔟 Performance Optimization:
# MAGIC
# MAGIC 1. **Right-size trigger intervals** — balance latency vs throughput
# MAGIC 2. **Use appropriate shuffle partitions** — avoid too many or too few
# MAGIC 3. **Optimize joins** — broadcast small dimension tables
# MAGIC 4. **Use Delta Lake** — better performance than Parquet
# MAGIC 5. **Enable adaptive query execution** — `spark.sql.adaptive.enabled=true`
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Common Mistakes and How to Avoid Them
# MAGIC %md
# MAGIC ## ❌ Common Mistakes in Structured Streaming
# MAGIC
# MAGIC ### 🚨 Critical Mistakes to Avoid:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Mistake #1: Missing Checkpoint Location
# MAGIC
# MAGIC ❌ **Wrong:**
# MAGIC ```python
# MAGIC df_stream.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .start(output_path)  # No checkpoint!
# MAGIC ```
# MAGIC
# MAGIC ✅ **Correct:**
# MAGIC ```python
# MAGIC df_stream.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .option("checkpointLocation", checkpoint_path) \
# MAGIC     .start(output_path)
# MAGIC ```
# MAGIC
# MAGIC **Why it matters**: Without checkpoints, you lose fault tolerance and exactly-once guarantees.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Mistake #2: Treating Streaming Like Batch
# MAGIC
# MAGIC ❌ **Wrong:**
# MAGIC ```python
# MAGIC # This will fail!
# MAGIC df_stream.count()  # Cannot call action on streaming DataFrame
# MAGIC df_stream.show()   # Cannot call action on streaming DataFrame
# MAGIC ```
# MAGIC
# MAGIC ✅ **Correct:**
# MAGIC ```python
# MAGIC # Start the stream first
# MAGIC query = df_stream.writeStream.format("memory").queryName("temp").start()
# MAGIC
# MAGIC # Query the memory table
# MAGIC spark.sql("SELECT * FROM temp").show()
# MAGIC
# MAGIC # Or read the output as batch
# MAGIC df_batch = spark.read.format("delta").load(output_path)
# MAGIC df_batch.count()
# MAGIC ```
# MAGIC
# MAGIC **Why it matters**: Streaming DataFrames are lazily evaluated and need writeStream to trigger execution.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Mistake #3: Wrong Output Mode
# MAGIC
# MAGIC ❌ **Wrong:**
# MAGIC ```python
# MAGIC # Using append for aggregations without watermark
# MAGIC df_stream.groupBy("product").count() \
# MAGIC     .writeStream \
# MAGIC     .outputMode("append") \  # Will fail!
# MAGIC     .start()
# MAGIC ```
# MAGIC
# MAGIC ✅ **Correct:**
# MAGIC ```python
# MAGIC # Use update for aggregations
# MAGIC df_stream.groupBy("product").count() \
# MAGIC     .writeStream \
# MAGIC     .outputMode("update") \
# MAGIC     .start()
# MAGIC
# MAGIC # Or add watermark for append
# MAGIC df_stream \
# MAGIC     .withWatermark("timestamp", "10 minutes") \
# MAGIC     .groupBy(window("timestamp", "5 minutes"), "product") \
# MAGIC     .count() \
# MAGIC     .writeStream \
# MAGIC     .outputMode("append") \
# MAGIC     .start()
# MAGIC ```
# MAGIC
# MAGIC **Why it matters**: Append mode requires finalized results, which aggregations don't provide without watermarking.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Mistake #4: Not Handling Incremental Data
# MAGIC
# MAGIC ❌ **Wrong:**
# MAGIC ```python
# MAGIC # Reprocessing all data every time
# MAGIC df = spark.read.format("csv").load(source_path)
# MAGIC df.write.mode("overwrite").save(output_path)  # Batch approach
# MAGIC ```
# MAGIC
# MAGIC ✅ **Correct:**
# MAGIC ```python
# MAGIC # Process only new data incrementally
# MAGIC df_stream = spark.readStream.format("csv").load(source_path)
# MAGIC df_stream.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .outputMode("append") \
# MAGIC     .option("checkpointLocation", checkpoint_path) \
# MAGIC     .start(output_path)
# MAGIC ```
# MAGIC
# MAGIC **Why it matters**: Streaming processes only new data, saving compute and time.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Mistake #5: Using Local Storage for Checkpoints
# MAGIC
# MAGIC ❌ **Wrong:**
# MAGIC ```python
# MAGIC .option("checkpointLocation", "/tmp/checkpoint")
# MAGIC .option("checkpointLocation", "file:/local/checkpoint")
# MAGIC ```
# MAGIC
# MAGIC ✅ **Correct:**
# MAGIC ```python
# MAGIC .option("checkpointLocation", "/Volumes/catalog/schema/volume/checkpoints/")
# MAGIC ```
# MAGIC
# MAGIC **Why it matters**: Local storage is not reliable or accessible across cluster nodes.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Mistake #6: Schema Inference in Production
# MAGIC
# MAGIC ❌ **Wrong:**
# MAGIC ```python
# MAGIC df_stream = spark.readStream.format("json").load(path)  # Schema inferred
# MAGIC ```
# MAGIC
# MAGIC ✅ **Correct:**
# MAGIC ```python
# MAGIC schema = StructType([...])  # Define explicitly
# MAGIC df_stream = spark.readStream.schema(schema).format("json").load(path)
# MAGIC ```
# MAGIC
# MAGIC **Why it matters**: Schema inference is expensive and can cause errors with evolving data.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Mistake #7: Not Stopping Streaming Queries
# MAGIC
# MAGIC ❌ **Wrong:**
# MAGIC ```python
# MAGIC query = df_stream.writeStream.start()
# MAGIC # Query keeps running indefinitely, consuming resources
# MAGIC ```
# MAGIC
# MAGIC ✅ **Correct:**
# MAGIC ```python
# MAGIC query = df_stream.writeStream.start()
# MAGIC
# MAGIC # Stop when done (testing, debugging)
# MAGIC query.stop()
# MAGIC
# MAGIC # Or stop all active queries
# MAGIC for q in spark.streams.active:
# MAGIC     q.stop()
# MAGIC ```
# MAGIC
# MAGIC **Why it matters**: Unclosed queries waste resources and incur costs.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Mistake #8: Caching Streaming DataFrames
# MAGIC
# MAGIC ❌ **Wrong:**
# MAGIC ```python
# MAGIC df_stream.cache()  # Don't do this!
# MAGIC ```
# MAGIC
# MAGIC ✅ **Correct:**
# MAGIC ```python
# MAGIC # No caching needed in streaming
# MAGIC # Spark manages data efficiently
# MAGIC df_stream  # Just use it directly
# MAGIC ```
# MAGIC
# MAGIC **Why it matters**: Streaming data flows continuously; caching breaks the streaming model.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Mistake #9: Incorrect Trigger Configuration
# MAGIC
# MAGIC ❌ **Wrong:**
# MAGIC ```python
# MAGIC # No trigger specified, but expecting controlled throughput
# MAGIC .writeStream.start()
# MAGIC ```
# MAGIC
# MAGIC ✅ **Correct:**
# MAGIC ```python
# MAGIC # Set appropriate trigger for your use case
# MAGIC .trigger(processingTime="30 seconds")  # Fixed interval
# MAGIC .trigger(once=True)  # For testing
# MAGIC .trigger(availableNow=True)  # For backfill
# MAGIC ```
# MAGIC
# MAGIC **Why it matters**: Default trigger processes as fast as possible, which may overwhelm downstream systems.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Mistake #10: Ignoring Watermarking for Late Data
# MAGIC
# MAGIC ❌ **Wrong:**
# MAGIC ```python
# MAGIC # Time-based aggregation without watermark
# MAGIC df_stream.groupBy(
# MAGIC     window("timestamp", "10 minutes")
# MAGIC ).count()  # State grows indefinitely!
# MAGIC ```
# MAGIC
# MAGIC ✅ **Correct:**
# MAGIC ```python
# MAGIC # Add watermark to handle late data
# MAGIC df_stream \
# MAGIC     .withWatermark("timestamp", "1 hour") \
# MAGIC     .groupBy(window("timestamp", "10 minutes")) \
# MAGIC     .count()
# MAGIC ```
# MAGIC
# MAGIC **Why it matters**: Without watermarks, state grows unbounded, causing memory issues.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Summary and Key Learnings
# MAGIC %md
# MAGIC ## 🎓 Summary — Key Learnings from Phase 5 Day 25
# MAGIC
# MAGIC ### 💡 Core Concepts Mastered:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 1. **Structured Streaming Fundamentals**
# MAGIC
# MAGIC ✅ Structured Streaming treats live data as unbounded tables  
# MAGIC ✅ Uses the same DataFrame API as batch processing  
# MAGIC ✅ Provides fault tolerance through checkpointing  
# MAGIC ✅ Guarantees exactly-once processing semantics  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. **Micro-batch Processing Model**
# MAGIC
# MAGIC ✅ Data is processed in small batches at regular intervals (triggers)  
# MAGIC ✅ Balances latency and throughput effectively  
# MAGIC ✅ Leverages Spark's mature batch optimizations  
# MAGIC ✅ Trigger types: Default, Fixed Interval, Once, Available Now, Continuous  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. **Streaming Architecture Components**
# MAGIC
# MAGIC ✅ **Source** → Where data comes from (Kafka, files, IoT, etc.)  
# MAGIC ✅ **Streaming Engine** → Spark Structured Streaming  
# MAGIC ✅ **Processing** → Transformations and business logic  
# MAGIC ✅ **Sink** → Where results are written (Delta, databases, etc.)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4. **Batch vs Streaming**
# MAGIC
# MAGIC | Aspect | Batch | Streaming |
# MAGIC |--------|-------|----------|
# MAGIC | **Latency** | High | Low |
# MAGIC | **Data** | Bounded | Unbounded |
# MAGIC | **Processing** | All at once | Incremental |
# MAGIC | **Use Case** | Historical | Real-time |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5. **Output Modes**
# MAGIC
# MAGIC ✅ **Append** — Only new rows (most efficient)  
# MAGIC ✅ **Update** — Only changed rows (for aggregations)  
# MAGIC ✅ **Complete** — Entire result (for small datasets)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6. **Critical Requirements**
# MAGIC
# MAGIC ✅ **Always** use checkpointLocation for fault tolerance  
# MAGIC ✅ **Always** define schemas explicitly for production  
# MAGIC ✅ **Always** use Unity Catalog Volumes (never /tmp)  
# MAGIC ✅ **Always** use Delta Lake for streaming sinks  
# MAGIC ✅ **Always** stop queries when done to free resources  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 7. **Medallion Architecture for Streaming**
# MAGIC
# MAGIC ✅ **Bronze** — Raw data ingestion (Auto Loader)  
# MAGIC ✅ **Silver** — Cleaned and validated data  
# MAGIC ✅ **Gold** — Business-ready aggregations and metrics  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 8. **Best Practices**
# MAGIC
# MAGIC ✅ Use watermarking for time-based operations  
# MAGIC ✅ Monitor query performance (input rate, processing time)  
# MAGIC ✅ Handle bad records gracefully  
# MAGIC ✅ Test with small datasets first  
# MAGIC ✅ Choose appropriate trigger intervals  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏆 What You Can Now Build:
# MAGIC
# MAGIC ✅ Real-time analytics dashboards  
# MAGIC ✅ Fraud detection systems  
# MAGIC ✅ IoT data processing pipelines  
# MAGIC ✅ Log aggregation and monitoring  
# MAGIC ✅ Event-driven ETL workflows  
# MAGIC ✅ Streaming aggregations with windowing  
# MAGIC ✅ Stream-static joins for data enrichment  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Next Steps:
# MAGIC
# MAGIC **Phase 5 Day 26**: Advanced Streaming (Watermarking, State Management, Join Strategies)  
# MAGIC **Phase 5 Day 27**: Streaming with Kafka  
# MAGIC **Phase 5 Day 28**: Auto Loader for Incremental Data Ingestion  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📚 Additional Resources:
# MAGIC
# MAGIC * Databricks Structured Streaming Documentation  
# MAGIC * Spark Streaming Programming Guide  
# MAGIC * Delta Lake Streaming Best Practices  
# MAGIC * Unity Catalog Volumes Guide  
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Interview Questions
# MAGIC %md
# MAGIC ## 📝 Interview Questions — Structured Streaming
# MAGIC
# MAGIC ### 🎯 Comprehensive Interview Preparation:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👶 Junior Level (0-2 years):
# MAGIC
# MAGIC #### Q1: What is Structured Streaming in Spark?
# MAGIC **Answer**: Structured Streaming is a scalable, fault-tolerant stream processing engine built on Spark SQL. It treats live data streams as unbounded tables that are continuously appended, allowing you to use the same DataFrame API for both batch and streaming.
# MAGIC
# MAGIC **Key Points**:
# MAGIC * Built on Spark SQL engine
# MAGIC * DataFrame API
# MAGIC * Fault-tolerant with exactly-once semantics
# MAGIC * Incremental processing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Q2: What is the difference between batch and streaming processing?
# MAGIC **Answer**: 
# MAGIC
# MAGIC | Aspect | Batch | Streaming |
# MAGIC |--------|-------|----------|
# MAGIC | **Data** | Bounded (finite) | Unbounded (infinite) |
# MAGIC | **Latency** | High (hours) | Low (seconds/minutes) |
# MAGIC | **Processing** | All data at once | Incremental |
# MAGIC | **Use Case** | Historical reports | Real-time analytics |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Q3: What is micro-batch processing?
# MAGIC **Answer**: Micro-batch processing is Spark's approach where streaming data is collected over short intervals (triggers) and processed as small batches. This provides near real-time processing with the efficiency of batch operations.
# MAGIC
# MAGIC **Example**: Process data every 5 seconds instead of processing each record individually.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Q4: What are the three output modes in Structured Streaming?
# MAGIC **Answer**:
# MAGIC 1. **Append**: Only new rows added to result table (most efficient)
# MAGIC 2. **Update**: Only rows that were updated (for aggregations)
# MAGIC 3. **Complete**: Entire result table written every time (for small datasets)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Q5: Why is checkpointing important in streaming?
# MAGIC **Answer**: Checkpointing provides:
# MAGIC * **Fault tolerance**: Resume from where you left off after failures
# MAGIC * **Exactly-once semantics**: Prevent duplicate processing
# MAGIC * **State management**: Store aggregation state
# MAGIC * **Progress tracking**: Track what data has been processed
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💼 Mid Level (2-5 years):
# MAGIC
# MAGIC #### Q6: Explain the Structured Streaming architecture.
# MAGIC **Answer**:
# MAGIC
# MAGIC ```
# MAGIC Source → Streaming Engine → Processing → Sink
# MAGIC ```
# MAGIC
# MAGIC * **Source**: Where data originates (Kafka, files, IoT)
# MAGIC * **Streaming Engine**: Spark Structured Streaming (manages micro-batches)
# MAGIC * **Processing**: Transformations (stateless/stateful)
# MAGIC * **Sink**: Output destination (Delta, database, Kafka)
# MAGIC
# MAGIC **Key Features**:
# MAGIC * Checkpointing for fault tolerance
# MAGIC * State management for aggregations
# MAGIC * Watermarking for late data
# MAGIC * Exactly-once processing guarantees
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Q7: When would you use each output mode?
# MAGIC **Answer**:
# MAGIC
# MAGIC **Append**:
# MAGIC * Stateless operations (filter, select, map)
# MAGIC * Time-windowed aggregations WITH watermark
# MAGIC * Event logs, audit trails
# MAGIC * Most efficient
# MAGIC
# MAGIC **Update**:
# MAGIC * Stateful aggregations (groupBy)
# MAGIC * Operations that modify existing rows
# MAGIC * Real-time dashboards
# MAGIC * Moderate efficiency
# MAGIC
# MAGIC **Complete**:
# MAGIC * Small result sets (<1000 rows)
# MAGIC * Aggregations without watermark
# MAGIC * Testing/debugging
# MAGIC * Least efficient
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Q8: How do you handle schema evolution in streaming?
# MAGIC **Answer**:
# MAGIC
# MAGIC 1. **Define schema explicitly** (recommended):
# MAGIC ```python
# MAGIC schema = StructType([...])
# MAGIC df_stream = spark.readStream.schema(schema).load(path)
# MAGIC ```
# MAGIC
# MAGIC 2. **Use schema evolution** (with caution):
# MAGIC ```python
# MAGIC df_stream = spark.readStream \
# MAGIC     .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
# MAGIC     .load(path)
# MAGIC ```
# MAGIC
# MAGIC **Best Practices**:
# MAGIC * Start with explicit schema
# MAGIC * Test schema changes in dev first
# MAGIC * Monitor for schema drift
# MAGIC * Use Delta Lake schema evolution features
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Q9: What are the different trigger types in Structured Streaming?
# MAGIC **Answer**:
# MAGIC
# MAGIC 1. **Default** (no trigger): Process as fast as possible
# MAGIC 2. **Fixed Interval**: `trigger(processingTime="30 seconds")`
# MAGIC 3. **Once**: `trigger(once=True)` - Process available data once and stop
# MAGIC 4. **Available Now**: `trigger(availableNow=True)` - Process all available in batches
# MAGIC 5. **Continuous**: `trigger(continuous="1 second")` - Experimental, ~1ms latency
# MAGIC
# MAGIC **When to Use**:
# MAGIC * Fixed Interval: Production streaming with controlled throughput
# MAGIC * Once: Testing, debugging, manual backfill
# MAGIC * Available Now: Automated backfill, catch-up processing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Q10: Explain the medallion architecture for streaming.
# MAGIC **Answer**:
# MAGIC
# MAGIC **Bronze Layer** (Raw):
# MAGIC * Ingest raw data as-is
# MAGIC * No transformations
# MAGIC * Schema evolution enabled
# MAGIC * Auto Loader for files
# MAGIC
# MAGIC **Silver Layer** (Cleaned):
# MAGIC * Data validation and cleaning
# MAGIC * Type conversions
# MAGIC * Deduplication
# MAGIC * PII masking
# MAGIC
# MAGIC **Gold Layer** (Aggregated):
# MAGIC * Business metrics and KPIs
# MAGIC * Aggregations and joins
# MAGIC * Ready for consumption
# MAGIC * Optimized for queries
# MAGIC
# MAGIC **Benefits**:
# MAGIC * Separation of concerns
# MAGIC * Incremental refinement
# MAGIC * Easier debugging
# MAGIC * Better performance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🌟 Senior Level (5+ years):
# MAGIC
# MAGIC #### Q11: How does Structured Streaming achieve exactly-once semantics?
# MAGIC **Answer**:
# MAGIC
# MAGIC **Three Mechanisms**:
# MAGIC
# MAGIC 1. **Replayable Sources**:
# MAGIC    - Sources support offset tracking (Kafka offsets, file positions)
# MAGIC    - Can replay data from specific offset
# MAGIC
# MAGIC 2. **Idempotent Sinks**:
# MAGIC    - Delta Lake uses atomic commits
# MAGIC    - Duplicate writes handled gracefully
# MAGIC    - Transaction logs ensure consistency
# MAGIC
# MAGIC 3. **Checkpointing**:
# MAGIC    - Write-Ahead Log (WAL) records every batch
# MAGIC    - Offset ranges tracked in checkpoint
# MAGIC    - State stored in checkpoint
# MAGIC
# MAGIC **Flow**:
# MAGIC ```
# MAGIC 1. Read offset range from source
# MAGIC 2. Write to WAL (checkpoint)
# MAGIC 3. Process data
# MAGIC 4. Write to sink (atomic commit)
# MAGIC 5. Update checkpoint with new offset
# MAGIC ```
# MAGIC
# MAGIC If failure occurs, restart from last checkpoint.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Q12: How would you optimize a slow streaming query?
# MAGIC **Answer**:
# MAGIC
# MAGIC **1. Identify Bottleneck**:
# MAGIC * Check Spark UI streaming tab
# MAGIC * Monitor input rate vs processing rate
# MAGIC * Check batch durations
# MAGIC
# MAGIC **2. Optimization Strategies**:
# MAGIC
# MAGIC **a) Source Optimization**:
# MAGIC * Increase `maxFilesPerTrigger` for file sources
# MAGIC * Increase Kafka `maxOffsetsPerTrigger`
# MAGIC * Use Auto Loader for cloud storage
# MAGIC
# MAGIC **b) Processing Optimization**:
# MAGIC * Right-size shuffle partitions: `spark.sql.shuffle.partitions`
# MAGIC * Broadcast small dimension tables for joins
# MAGIC * Use watermarking to limit state size
# MAGIC * Enable adaptive query execution
# MAGIC
# MAGIC **c) Trigger Optimization**:
# MAGIC * Increase trigger interval for higher throughput
# MAGIC * Decrease for lower latency (if processing keeps up)
# MAGIC
# MAGIC **d) Resource Optimization**:
# MAGIC * Increase cluster size
# MAGIC * Enable auto-scaling
# MAGIC * Use Delta Lake (faster than Parquet)
# MAGIC
# MAGIC **3. Monitoring**:
# MAGIC * Set up alerts for batch duration
# MAGIC * Monitor state size growth
# MAGIC * Track watermark delay
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Q13: Explain watermarking and when you would use it.
# MAGIC **Answer**:
# MAGIC
# MAGIC **What is Watermarking?**
# MAGIC Watermarking is a mechanism to handle late-arriving data in streaming by defining how long to wait for late events.
# MAGIC
# MAGIC **How it Works**:
# MAGIC ```python
# MAGIC df.withWatermark("event_time", "1 hour")
# MAGIC ```
# MAGIC
# MAGIC This means:
# MAGIC * Wait up to 1 hour for late events
# MAGIC * Events more than 1 hour late are dropped
# MAGIC * State older than watermark is cleaned up
# MAGIC
# MAGIC **When to Use**:
# MAGIC
# MAGIC 1. **Time-windowed aggregations**:
# MAGIC ```python
# MAGIC df.withWatermark("timestamp", "10 minutes") \
# MAGIC   .groupBy(window("timestamp", "5 minutes")) \
# MAGIC   .count()
# MAGIC ```
# MAGIC
# MAGIC 2. **Stream-stream joins**:
# MAGIC ```python
# MAGIC df1.withWatermark("time", "1 hour").join(
# MAGIC     df2.withWatermark("time", "2 hours"),
# MAGIC     "id"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC 3. **Deduplication**:
# MAGIC ```python
# MAGIC df.withWatermark("timestamp", "10 minutes") \
# MAGIC   .dropDuplicates(["id"])
# MAGIC ```
# MAGIC
# MAGIC **Benefits**:
# MAGIC * Bounded state size
# MAGIC * Enables append mode for aggregations
# MAGIC * Prevents memory issues
# MAGIC * Balances completeness vs resource usage
# MAGIC
# MAGIC **Choosing Watermark Delay**:
# MAGIC * Too short: Lose late data
# MAGIC * Too long: State grows large
# MAGIC * Typical: 10 minutes to 1 hour
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Q14: Compare Structured Streaming with other streaming frameworks (Kafka Streams, Flink).
# MAGIC **Answer**:
# MAGIC
# MAGIC | Feature | Structured Streaming | Kafka Streams | Flink |
# MAGIC |---------|---------------------|---------------|-------|
# MAGIC | **Processing Model** | Micro-batch | Event-by-event | Event-by-event |
# MAGIC | **Latency** | Seconds | Milliseconds | Milliseconds |
# MAGIC | **Throughput** | Very High | High | Very High |
# MAGIC | **Fault Tolerance** | Checkpoint | State stores | Checkpoint |
# MAGIC | **Exactly-Once** | Yes | Yes | Yes |
# MAGIC | **API** | DataFrame/SQL | Streams API | DataStream API |
# MAGIC | **State Management** | Built-in | RocksDB | State backends |
# MAGIC | **Integration** | Spark ecosystem | Kafka-centric | Broad connectors |
# MAGIC | **Ease of Use** | High (same as batch) | Medium | Medium |
# MAGIC | **Scalability** | Excellent | Good | Excellent |
# MAGIC
# MAGIC **When to Choose Structured Streaming**:
# MAGIC * Need to unify batch and streaming code
# MAGIC * Leverage existing Spark ecosystem (MLlib, Delta)
# MAGIC * High throughput requirements
# MAGIC * Latency requirements are seconds (not sub-second)
# MAGIC * Team familiar with Spark
# MAGIC
# MAGIC **When to Choose Others**:
# MAGIC * Kafka Streams: Kafka-centric architecture, millisecond latency
# MAGIC * Flink: Complex event processing, sub-second latency required
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Q15: Design a fault-tolerant streaming ETL pipeline for real-time fraud detection.
# MAGIC **Answer**:
# MAGIC
# MAGIC **Architecture**:
# MAGIC
# MAGIC ```
# MAGIC Transaction Stream (Kafka)
# MAGIC   ↓
# MAGIC Bronze Layer (Raw Ingestion)
# MAGIC   ↓
# MAGIC Silver Layer (Feature Engineering)
# MAGIC   ↓
# MAGIC Gold Layer (Fraud Scoring)
# MAGIC   ↓
# MAGIC Alerts + Analytics
# MAGIC ```
# MAGIC
# MAGIC **Implementation**:
# MAGIC
# MAGIC **1. Bronze Layer** (Raw Ingestion):
# MAGIC ```python
# MAGIC df_bronze = spark.readStream \
# MAGIC     .format("kafka") \
# MAGIC     .option("kafka.bootstrap.servers", "...") \
# MAGIC     .option("subscribe", "transactions") \
# MAGIC     .load() \
# MAGIC     .selectExpr("CAST(value AS STRING) as json") \
# MAGIC     .writeStream \
# MAGIC     .format("delta") \
# MAGIC     .option("checkpointLocation", "/checkpoints/bronze/") \
# MAGIC     .start("/delta/bronze/transactions/")
# MAGIC ```
# MAGIC
# MAGIC **2. Silver Layer** (Feature Engineering):
# MAGIC ```python
# MAGIC df_silver = spark.readStream \
# MAGIC     .format("delta") \
# MAGIC     .load("/delta/bronze/transactions/") \
# MAGIC     .select(from_json("json", schema).alias("data")) \
# MAGIC     .select("data.*") \
# MAGIC     .withWatermark("timestamp", "5 minutes") \
# MAGIC     .withColumn("hour_of_day", hour("timestamp")) \
# MAGIC     .withColumn("amount_category", 
# MAGIC         when(col("amount") > 1000, "high")
# MAGIC         .when(col("amount") > 100, "medium")
# MAGIC         .otherwise("low")) \
# MAGIC     .writeStream \
# MAGIC     .format("delta") \
# MAGIC     .outputMode("append") \
# MAGIC     .option("checkpointLocation", "/checkpoints/silver/") \
# MAGIC     .start("/delta/silver/transactions/")
# MAGIC ```
# MAGIC
# MAGIC **3. Gold Layer** (Fraud Detection):
# MAGIC ```python
# MAGIC # Aggregate suspicious patterns
# MAGIC df_gold = spark.readStream \
# MAGIC     .format("delta") \
# MAGIC     .load("/delta/silver/transactions/") \
# MAGIC     .withWatermark("timestamp", "10 minutes") \
# MAGIC     .groupBy(
# MAGIC         "user_id",
# MAGIC         window("timestamp", "5 minutes")
# MAGIC     ) \
# MAGIC     .agg(
# MAGIC         count("*").alias("txn_count"),
# MAGIC         sum("amount").alias("total_amount"),
# MAGIC         countDistinct("merchant").alias("unique_merchants")
# MAGIC     ) \
# MAGIC     .filter((col("txn_count") > 10) | (col("total_amount") > 5000)) \
# MAGIC     .writeStream \
# MAGIC     .format("delta") \
# MAGIC     .outputMode("update") \
# MAGIC     .option("checkpointLocation", "/checkpoints/gold/") \
# MAGIC     .start("/delta/gold/fraud_alerts/")
# MAGIC ```
# MAGIC
# MAGIC **Key Design Decisions**:
# MAGIC
# MAGIC 1. **Checkpointing**: Each layer has dedicated checkpoint
# MAGIC 2. **Watermarking**: 5-10 min to handle late transactions
# MAGIC 3. **Delta Lake**: ACID guarantees, time travel
# MAGIC 4. **Windowing**: 5-minute windows for real-time detection
# MAGIC 5. **Update Mode**: For aggregations that change
# MAGIC
# MAGIC **Fault Tolerance**:
# MAGIC * Kafka: Replayable source
# MAGIC * Checkpoints: Track progress
# MAGIC * Delta: Atomic commits
# MAGIC * Auto-restart: On failures
# MAGIC
# MAGIC **Monitoring**:
# MAGIC * Batch duration < trigger interval
# MAGIC * Input rate vs processing rate
# MAGIC * State size growth
# MAGIC * Alert latency (end-to-end)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎖️ Bonus Questions:
# MAGIC
# MAGIC #### Q16: How do you test a streaming application?
# MAGIC **Answer**:
# MAGIC
# MAGIC **1. Unit Testing**:
# MAGIC * Create test DataFrames with sample data
# MAGIC * Use `trigger(once=True)` for deterministic execution
# MAGIC * Use memory sink for verification
# MAGIC
# MAGIC **2. Integration Testing**:
# MAGIC * Test with small production-like datasets
# MAGIC * Verify checkpoint recovery
# MAGIC * Test schema evolution
# MAGIC
# MAGIC **3. Load Testing**:
# MAGIC * Simulate high volume data
# MAGIC * Monitor cluster resources
# MAGIC * Verify auto-scaling
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Q17: What is the difference between foreachBatch and foreach in streaming?
# MAGIC **Answer**:
# MAGIC
# MAGIC **foreachBatch**: Operates on micro-batch DataFrame
# MAGIC ```python
# MAGIC def process_batch(df, batch_id):
# MAGIC     # Can use batch operations
# MAGIC     df.write.jdbc(...)  # Write to database
# MAGIC     df.write.format("delta").save(...)  # Write to Delta
# MAGIC
# MAGIC .writeStream.foreachBatch(process_batch).start()
# MAGIC ```
# MAGIC
# MAGIC **foreach**: Operates on each row
# MAGIC ```python
# MAGIC .writeStream.foreach(CustomWriter()).start()
# MAGIC ```
# MAGIC
# MAGIC **Use foreachBatch for**: Multiple sinks, complex logic, batch operations  
# MAGIC **Use foreach for**: Row-by-row processing, custom sinks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📚 Study Tips:
# MAGIC
# MAGIC ✅ Understand micro-batch processing deeply  
# MAGIC ✅ Practice with different output modes  
# MAGIC ✅ Know when to use watermarking  
# MAGIC ✅ Be able to design end-to-end architectures  
# MAGIC ✅ Understand fault tolerance mechanisms  
# MAGIC ✅ Practice optimization techniques  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎉 Congratulations on Completing Phase 5 Day 25!
# MAGIC
# MAGIC **You are now equipped to build production-grade streaming pipelines!** 🚀
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Next Topics:
# MAGIC * Day 26: Advanced Watermarking & State Management
# MAGIC * Day 27: Kafka Integration
# MAGIC * Day 28: Auto Loader for Incremental Processing
# MAGIC
# MAGIC ---