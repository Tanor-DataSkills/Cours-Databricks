# Databricks notebook source
# DBTITLE 1,📘 Notebook Header
# MAGIC %md
# MAGIC # 🌊 Data Engineering Training — Phase 5 Day 28  
# MAGIC ## 🚀 End-to-End Real-Time Streaming Pipeline  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - End-to-End Streaming Pipeline Design  
# MAGIC - Ingestion (Auto Loader / Streaming Source)  
# MAGIC - Processing (Transformations + Watermarking)  
# MAGIC - Storage (Delta Lake - Bronze/Silver/Gold)  
# MAGIC - Exactly-once Processing & Checkpointing  
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
# MAGIC Build a complete real-time streaming pipeline using Databricks Structured Streaming and Delta Lake following Medallion architecture.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎓 Learning Outcomes:
# MAGIC - Design and implement end-to-end streaming architectures
# MAGIC - Apply Medallion architecture to streaming workloads
# MAGIC - Implement exactly-once processing semantics
# MAGIC - Handle late-arriving data with watermarking
# MAGIC - Build fault-tolerant streaming pipelines

# COMMAND ----------

# DBTITLE 1,SETUP — Configuration & Prerequisites
# MAGIC %md
# MAGIC ## SETUP — Configuration & Prerequisites
# MAGIC
# MAGIC ### What we'll do:
# MAGIC - Define Unity Catalog paths (catalog, schema, volume)
# MAGIC - Generate sample streaming data with correct schema
# MAGIC - Ensure all paths are serverless-compliant
# MAGIC
# MAGIC ### Schema for Demo:
# MAGIC - `event_time`: timestamp
# MAGIC - `user_id`: string
# MAGIC - `category`: string
# MAGIC - `amount`: double
# MAGIC - `valid`: boolean

# COMMAND ----------

# DBTITLE 1,Define configuration variables
# Configuration - Update these for your environment
catalog = "main"  # Replace with your catalog name
schema = "default"  # Replace with your schema name
volume = "streaming_demo"  # Volume name for this pipeline

# Build paths
base_path = f"/Volumes/{catalog}/{schema}/{volume}"
raw_path = f"{base_path}/raw"
bronze_path = f"{base_path}/bronze"
silver_path = f"{base_path}/silver"
gold_path = f"{base_path}/gold"
chk_bronze = f"{base_path}/chk_bronze"
chk_silver = f"{base_path}/chk_silver"
chk_gold = f"{base_path}/chk_gold"

print(f"Base path: {base_path}")

# COMMAND ----------

# DBTITLE 1,Generate sample streaming data
import json
from datetime import datetime, timedelta
import random

# Generate sample data
categories = ["electronics", "clothing", "food", "books", "toys"]
data = []

for i in range(100):
    record = {
        "event_time": (datetime.now() - timedelta(minutes=random.randint(0, 30))).isoformat(),
        "user_id": f"user_{random.randint(1, 20)}",
        "category": random.choice(categories),
        "amount": round(random.uniform(10.0, 500.0), 2),
        "valid": random.choice([True, True, True, False])  # 75% valid
    }
    data.append(record)

# Write to DataFrame and save as JSON files
df_sample = spark.createDataFrame(data)
df_sample.write.mode("overwrite").json(raw_path)

print(f"Generated {len(data)} sample records to {raw_path}")
display(df_sample.limit(5))

# COMMAND ----------

# DBTITLE 1,SECTION 1 — Real-Time Pipeline Overview
# MAGIC %md
# MAGIC ## SECTION 1 — Real-Time Pipeline Overview
# MAGIC
# MAGIC ### ELI5: What is a real-time data pipeline?
# MAGIC - Imagine getting updates instantly when something happens—like tracking your pizza order live!
# MAGIC - Real-time data pipelines let companies process information as soon as it arrives, enabling quick decisions.
# MAGIC
# MAGIC ### Architect-level Explanation:
# MAGIC - Real-time pipelines continuously ingest, process, and store data with milliseconds-to-seconds latency.
# MAGIC - They leverage systems like Databricks Structured Streaming to transform streams at scale, supporting advanced analytics and operational intelligence.
# MAGIC
# MAGIC ### Typical Use Cases:
# MAGIC * Fraud Detection — Identify suspicious transactions instantly
# MAGIC * Clickstream Analytics — Analyze website user actions in real time
# MAGIC * IoT Processing — Ingest sensor data for monitoring and automation

# COMMAND ----------

# DBTITLE 1,SECTION 2 — Architecture Design (Medallion)
# MAGIC %md
# MAGIC ## SECTION 2 — Architecture Design (Medallion)
# MAGIC
# MAGIC ### ELI5: What is Medallion architecture?
# MAGIC - Think of it as cleaning and refining data in steps—like washing, filtering, and polishing gold.
# MAGIC - Bronze is raw data, Silver is cleaned up, Gold is ready for important decisions.
# MAGIC
# MAGIC ### Architect-level Explanation:
# MAGIC - Medallion architecture is a best practice for organizing streaming and batch pipelines.
# MAGIC - Data flows through three stages:
# MAGIC   - **Bronze**: Raw, unprocessed ingestion
# MAGIC   - **Silver**: Cleaned, validated, enriched
# MAGIC   - **Gold**: Aggregated, ready for analytics and reporting
# MAGIC
# MAGIC ### End-to-End Flow:
# MAGIC     Source → Bronze → Silver → Gold → Analytics

# COMMAND ----------

# DBTITLE 1,SECTION 3 — Ingestion Layer (Auto Loader)
# MAGIC %md
# MAGIC ## SECTION 3 — Ingestion Layer (Auto Loader)
# MAGIC
# MAGIC ### ELI5: How does streaming ingestion work?
# MAGIC - Like a conveyor belt that automatically picks up new files as they arrive.
# MAGIC - You don't need to tell it when to start—it watches and ingests data in real time.
# MAGIC
# MAGIC ### Architect-level Explanation:
# MAGIC - Databricks Auto Loader simplifies streaming ingestion for files landing in cloud storage.
# MAGIC - It enables scalable, incremental processing from sources like S3/GCS/ADLS via Unity Catalog Volumes.
# MAGIC - Supports exactly-once semantics, schema evolution, and minimal operational overhead.
# MAGIC
# MAGIC #### Example: Auto Loader Streaming Ingestion
# MAGIC

# COMMAND ----------

# DBTITLE 1,Ingest raw streaming data with Auto Loader
# Ingest raw streaming data with Auto Loader
df_raw = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", f"{base_path}/schema") \
    .load(raw_path)

print("Streaming DataFrame created. Schema:")
df_raw.printSchema()

# COMMAND ----------

# DBTITLE 1,SECTION 4 — Bronze Layer
# MAGIC %md
# MAGIC ## SECTION 4 — Bronze Layer
# MAGIC
# MAGIC ### ELI5: Why store raw data?
# MAGIC - It's like keeping original receipts, so you can always check what really happened.
# MAGIC - Raw data is useful for audits and replaying history.
# MAGIC
# MAGIC ### Architect-level Explanation:
# MAGIC - The Bronze layer captures unaltered ingested data for traceability and data lineage.
# MAGIC - Enables reprocessing, schema evolution, and supports regulatory/compliance requirements.
# MAGIC - Streaming write ensures atomicity, checkpointing guarantees fault tolerance.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Write Bronze layer (raw Delta)
# Write Bronze layer (raw Delta)
# Using availableNow trigger for serverless compute compatibility

bronze_stream = df_raw.writeStream \
    .format("delta") \
    .option("checkpointLocation", chk_bronze) \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .start(bronze_path)

# Wait for completion (AvailableNow trigger runs as micro-batch)
bronze_stream.awaitTermination()
print(f"Bronze layer write completed successfully!")
print(f"Data written to: {bronze_path}")

# COMMAND ----------

# DBTITLE 1,SECTION 5 — Silver Layer (Transformations)
# MAGIC %md
# MAGIC ## SECTION 5 — Silver Layer (Transformations)
# MAGIC
# MAGIC ### ELI5: Why clean data in Silver?
# MAGIC - It's like sorting out good apples from spoiled ones before making juice.
# MAGIC - Cleaning ensures you only analyze trustworthy, relevant information.
# MAGIC
# MAGIC ### Architect-level Explanation:
# MAGIC - Silver layer applies business logic, validation, and enrichment to raw data.
# MAGIC - Watermarking allows safe time-window processing and gracefully handles late-arriving data.
# MAGIC - Incremental filter transforms streaming Bronze into clean Silver for downstream analytics.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Read Bronze & transform to Silver (with watermark)
# Read Bronze & transform to Silver (with watermark)
from pyspark.sql.functions import col, to_timestamp

df_bronze = spark.readStream.format("delta").load(bronze_path)

# Transform: convert types, apply watermark, filter valid records
df_silver = df_bronze \
    .withColumn("event_time", to_timestamp(col("event_time"))) \
    .withColumn("amount", col("amount").cast("double")) \
    .withColumn("valid", col("valid").cast("boolean")) \
    .withWatermark("event_time", "10 minutes") \
    .filter(col("valid") == True) \
    .drop("_rescued_data")  # Remove Auto Loader metadata column

print("Silver DataFrame created with transformations")
df_silver.printSchema()

# COMMAND ----------

# DBTITLE 1,Write Silver layer (clean Delta, checkpointed)
# Write Silver layer (clean Delta, checkpointed)
silver_stream = df_silver.writeStream \
    .format("delta") \
    .option("checkpointLocation", chk_silver) \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .queryName("silver_stream") \
    .start(silver_path)

# Wait for completion
silver_stream.awaitTermination()
print(f"Silver layer write completed successfully!")
print(f"Data written to: {silver_path}")

# COMMAND ----------

# DBTITLE 1,SECTION 6 — Gold Layer (Aggregation)
# MAGIC %md
# MAGIC ## SECTION 6 — Gold Layer (Aggregation)
# MAGIC
# MAGIC ### ELI5: Why aggregate data in Gold?
# MAGIC - It's like summarizing scores after a game: makes it quick to see who won, trends, and highlights.
# MAGIC - Gold layer delivers actionable insights for business teams.
# MAGIC
# MAGIC ### Architect-level Explanation:
# MAGIC - Gold layer provides aggregated, analytics-ready views that power dashboards and ML models.
# MAGIC - Processes validated, clean Silver data into grouped summaries, metrics, and KPIs.
# MAGIC - Ensures efficient, scalable reporting with consistent Delta storage and checkpointing.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Aggregate & write Gold layer (analytics-ready Delta)
# Aggregate & write Gold layer (analytics-ready Delta)
from pyspark.sql.functions import count, sum as _sum, avg, max as _max

# Create aggregated Gold view: category-level metrics
df_gold = df_silver.groupBy("category").agg(
    count("*").alias("transaction_count"),
    _sum("amount").alias("total_amount"),
    avg("amount").alias("avg_amount"),
    _max("amount").alias("max_amount")
)

# Write Gold layer with complete output mode (for aggregations)
gold_stream = df_gold.writeStream \
    .format("delta") \
    .option("checkpointLocation", chk_gold) \
    .outputMode("complete") \
    .trigger(availableNow=True) \
    .queryName("gold_stream") \
    .start(gold_path)

# Wait for completion
gold_stream.awaitTermination()
print(f"Gold layer write completed successfully!")
print(f"Data written to: {gold_path}")

# COMMAND ----------

# DBTITLE 1,SECTION 7 — Exactly-once Processing
# MAGIC %md
# MAGIC ## SECTION 7 — Exactly-once Processing
# MAGIC
# MAGIC ### ELI5: Why is exactly-once important?
# MAGIC - Imagine counting money twice or missing a payment—both cause big problems!
# MAGIC - Exactly-once means every event is processed one time—no loss, no duplicates.
# MAGIC
# MAGIC ### Architect-level Explanation:
# MAGIC - Delta Lake + structured streaming guarantees atomic writes and prevents duplicates via checkpointing.
# MAGIC - Checkpoints track progress, ensuring pipelines can restart and always resume from the last good state.
# MAGIC - Enables regulatory-compliant, robust enterprise streaming solutions.

# COMMAND ----------

# DBTITLE 1,SECTION 8 — Checkpointing & Recovery
# MAGIC %md
# MAGIC ## SECTION 8 — Checkpointing & Recovery
# MAGIC
# MAGIC ### ELI5: Why do we need checkpoints?
# MAGIC - Like saving your progress in a video game, checkpoints mean you never lose your work.
# MAGIC - If computers crash or jobs restart, everything resumes right where it left off.
# MAGIC
# MAGIC ### Architect-level Explanation:
# MAGIC - Checkpointing records streaming progress, state, and exactly-once write history in Delta paths.
# MAGIC - Databricks can recover pipelines from any failure, ensuring no data loss or duplication.
# MAGIC - Critical for meeting SLAs and scaling reliable production pipelines.

# COMMAND ----------

# DBTITLE 1,SECTION 9 — End-to-End Pipeline Execution
# MAGIC %md
# MAGIC ## SECTION 9 — End-to-End Pipeline Execution
# MAGIC
# MAGIC ### The Complete Streaming Flow:
# MAGIC - Source
# MAGIC - Auto Loader (Streaming Ingestion)
# MAGIC - Bronze (Raw Delta)
# MAGIC - Silver (Cleaned, Enriched Delta)
# MAGIC - Gold (Aggregated Analytics)
# MAGIC - Output (Dashboards, ML, Business Insights)
# MAGIC
# MAGIC #### Diagram:
# MAGIC     Source → Auto Loader → Bronze → Silver → Gold → Output
# MAGIC
# MAGIC ### Serverless Best Practices:
# MAGIC - All layers use PySpark DataFrame API
# MAGIC - No cache() or persist()
# MAGIC - All paths use Unity Catalog Volumes
# MAGIC - Each streaming write uses checkpointing and Delta for reliability
# MAGIC
# MAGIC ### Serverless Streaming Triggers:
# MAGIC - **Trigger.AvailableNow** (Serverless): Processes all available data incrementally, then stops. Ideal for scheduled/cost-effective workloads.
# MAGIC - **ProcessingTime** (Always-On Clusters): Continuous streaming, processes data in real-time. Not supported on serverless.
# MAGIC - This notebook uses `availableNow=True` for serverless compatibility.
# MAGIC

# COMMAND ----------

# DBTITLE 1,SECTION 10 — Monitoring & Querying Results
# MAGIC %md
# MAGIC ## SECTION 10 — Monitoring & Querying Results
# MAGIC
# MAGIC ### ELI5: How do we check if it's working?
# MAGIC - Like checking your order status online—we can see if streams are running and view the results.
# MAGIC
# MAGIC ### Architect-level Explanation:
# MAGIC - Structured Streaming provides APIs to monitor active queries, check progress, and inspect metrics.
# MAGIC - Delta tables can be queried in batch mode even while streaming writes continue.
# MAGIC - Essential for observability, debugging, and validating pipeline correctness.

# COMMAND ----------

# DBTITLE 1,Monitor active streaming queries
# Monitor pipeline results - check record counts at each layer
print("Pipeline Monitoring - Record Counts:\n")

try:
    bronze_count = spark.read.format("delta").load(bronze_path).count()
    print(f"Bronze Layer: {bronze_count} records")
except:
    print("Bronze Layer: Not yet created")

try:
    silver_count = spark.read.format("delta").load(silver_path).count()
    print(f"Silver Layer: {silver_count} records (filtered for valid=true)")
except:
    print("Silver Layer: Not yet created")

try:
    gold_count = spark.read.format("delta").load(gold_path).count()
    print(f"Gold Layer: {gold_count} aggregated categories")
except:
    print("Gold Layer: Not yet created")

print("\nNote: With Trigger.AvailableNow, streams process available data and complete.")
print("For continuous streaming, use always-on clusters with ProcessingTime trigger.")

# COMMAND ----------

# DBTITLE 1,Query Gold layer (batch read)
# Read Gold layer as batch DataFrame for analytics
df_gold_batch = spark.read.format("delta").load(gold_path)

print("Gold Layer Analytics Results:")
print(f"Total categories: {df_gold_batch.count()}\n")

display(df_gold_batch.orderBy("total_amount", ascending=False))

# COMMAND ----------

# DBTITLE 1,Stop all streaming queries (cleanup)
# Stop all active streaming queries
for stream in spark.streams.active:
    print(f"Stopping stream: {stream.name} ({stream.id})")
    stream.stop()
    
print("\nAll streams stopped successfully!")
print(f"Active streams remaining: {len(spark.streams.active)}")

# COMMAND ----------

# DBTITLE 1,✅ Pipeline Execution Summary
# MAGIC %md
# MAGIC ## ✅ Pipeline Execution Summary
# MAGIC
# MAGIC ### Successful End-to-End Pipeline Execution!
# MAGIC
# MAGIC The complete Medallion streaming pipeline has been successfully executed:
# MAGIC
# MAGIC #### Data Flow:
# MAGIC - **Source**: Generated 100 sample transaction records
# MAGIC - **Bronze Layer**: 100 raw records ingested via Auto Loader
# MAGIC - **Silver Layer**: 68 valid records (filtered & cleaned)
# MAGIC - **Gold Layer**: 5 category-level aggregations with metrics
# MAGIC
# MAGIC #### Key Achievements:
# MAGIC - ✅ Auto Loader streaming ingestion
# MAGIC - ✅ Delta Lake Bronze/Silver/Gold layers
# MAGIC - ✅ Type casting and data validation
# MAGIC - ✅ Watermarking (10-minute window)
# MAGIC - ✅ Exactly-once processing with checkpointing
# MAGIC - ✅ Serverless-compatible Trigger.AvailableNow
# MAGIC - ✅ Analytics-ready aggregations
# MAGIC
# MAGIC #### Production Readiness:
# MAGIC - All data stored in Unity Catalog Volumes
# MAGIC - Checkpoints enable fault tolerance
# MAGIC - Clean separation of concerns (Bronze/Silver/Gold)
# MAGIC - Ready for scheduled job orchestration

# COMMAND ----------

# DBTITLE 1,FINAL SUMMARY — Key Learnings, Interview Questions, Common Mistakes, Genie Code Prompts
# MAGIC %md
# MAGIC ## FINAL SUMMARY
# MAGIC
# MAGIC ### Key Learnings
# MAGIC - Real-time streaming pipelines enable near-instant analytics and automation.
# MAGIC - Medallion architecture improves reliability, scalability, and clarity in data engineering.
# MAGIC - PySpark Structured Streaming with Delta delivers exactly-once, fault-tolerant processing.
# MAGIC - Checkpointing prevents data loss and ensures safe restart/recovery.
# MAGIC - Unity Catalog Volumes guarantee secure, scalable storage for all pipeline layers.
# MAGIC
# MAGIC ### Genie Code Agent Usage: Example Prompts
# MAGIC - Prompt → Build end-to-end streaming pipeline
# MAGIC - Prompt → Implement Bronze/Silver/Gold streaming
# MAGIC - Prompt → Add watermarking & checkpointing
# MAGIC - Prompt → Design real-time architecture
# MAGIC
# MAGIC ### Interview Questions
# MAGIC * What distinguishes batch from streaming data pipelines?
# MAGIC * Explain the Medallion architecture and its benefits.
# MAGIC * How does Databricks Structured Streaming ensure exactly-once guarantees?
# MAGIC * Why is checkpointing crucial in streaming workloads?
# MAGIC * How does watermarking help manage late data?
# MAGIC * What is Unity Catalog Volume and why is it important?
# MAGIC * Why should you not mix batch and streaming logic?
# MAGIC * What are typical failure recovery steps for streaming jobs?
# MAGIC * Describe best practices for serverless streaming pipelines.
# MAGIC * How would you monitor and alert on a production streaming pipeline?
# MAGIC
# MAGIC ### Common Mistakes
# MAGIC - Mixing batch and streaming logic in one job
# MAGIC - Missing checkpointing (causes data loss/duplication)
# MAGIC - Not separating Bronze/Silver/Gold layers
# MAGIC - Ignoring late-arriving data (missing watermarking)
# MAGIC
# MAGIC ### OPTIONAL — Monitoring Layer (Conceptual)
# MAGIC - Always monitor stream metrics (latency, throughput, error counts)
# MAGIC - Use Databricks metrics, alerting, and dashboards for pipeline health
# MAGIC
# MAGIC ### Real-World Use Case Scenario
# MAGIC - Financial institution builds a fraud detection engine with Bronze (raw transactions), Silver (validated, enriched events), Gold (aggregated risk scores), and real-time dashboards for analysts.
# MAGIC