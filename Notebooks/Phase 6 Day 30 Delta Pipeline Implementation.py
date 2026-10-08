# Databricks notebook source
# DBTITLE 1,📘 Notebook Header
# MAGIC %md
# MAGIC # 🏗 Data Engineering Training — Phase 6 Day 30  
# MAGIC ## ⚙️ Delta Pipeline Implementation (Medallion Architecture)  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC * End-to-End Pipeline using Delta Lake  
# MAGIC * Bronze → Silver → Gold Implementation  
# MAGIC * Incremental Processing  
# MAGIC * Production Pipeline Design  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### ⚙️ Platform: Databricks (Serverless + Unity Catalog + Delta Lake)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Objective:
# MAGIC Build a complete production-grade Delta pipeline using Medallion architecture with incremental processing and best practices.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ IMPORTANT ENGINEERING CONSTRAINTS (MANDATORY):
# MAGIC
# MAGIC * ✅ Use Databricks Serverless Compute
# MAGIC * ❌ DO NOT use RDDs (DataFrame API only)
# MAGIC * ❌ DO NOT use cache() / persist()
# MAGIC * ❌ DO NOT use /tmp or local storage
# MAGIC * ✅ Use Unity Catalog managed tables and Volumes
# MAGIC * ✅ Follow Medallion-first and incremental design

# COMMAND ----------

# DBTITLE 1,Section 1: Pipeline Overview
# MAGIC %md
# MAGIC # SECTION 1 — Pipeline Overview
# MAGIC
# MAGIC ## ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine you're organizing your toy collection:
# MAGIC * **Bronze Box** — You throw all toys in as they come (raw, messy)
# MAGIC * **Silver Box** — You clean them, sort by type (cars, dolls, blocks)
# MAGIC * **Gold Box** — You count how many of each type and make a summary chart
# MAGIC
# MAGIC That's exactly what a data pipeline does with information!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architect-Level Explanation:
# MAGIC
# MAGIC ### **What is a Delta Pipeline?**
# MAGIC
# MAGIC A **Delta Pipeline** is an end-to-end data processing system built on Delta Lake that:
# MAGIC * Ingests raw data from various sources
# MAGIC * Transforms and enriches data through multiple stages
# MAGIC * Delivers production-ready analytics tables
# MAGIC * Provides ACID guarantees, time travel, and schema evolution
# MAGIC
# MAGIC ### **Medallion Architecture**:
# MAGIC
# MAGIC ```
# MAGIC Source Data (CSV, JSON, Parquet, APIs)
# MAGIC         ↓
# MAGIC BRONZE Layer (Raw Ingestion)
# MAGIC    • No transformations
# MAGIC    • Append-only
# MAGIC    • Preserves source lineage
# MAGIC         ↓
# MAGIC SILVER Layer (Cleaned & Conformed)
# MAGIC    • Type casting
# MAGIC    • Deduplication
# MAGIC    • Data quality filters
# MAGIC    • Business logic applied
# MAGIC         ↓
# MAGIC GOLD Layer (Business Aggregates)
# MAGIC    • Aggregations
# MAGIC    • Joins across domains
# MAGIC    • BI-ready tables
# MAGIC    • Optimized for reporting
# MAGIC         ↓
# MAGIC Analytics / Dashboards / ML
# MAGIC ```
# MAGIC
# MAGIC ### **Why This Architecture?**
# MAGIC
# MAGIC * **Separation of Concerns**: Each layer has a clear purpose
# MAGIC * **Reusability**: Silver tables can feed multiple Gold tables
# MAGIC * **Debugging**: Easy to identify where issues occur
# MAGIC * **Incremental Processing**: Process only changed data
# MAGIC * **Auditability**: Full data lineage from source to insights
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Principles:
# MAGIC
# MAGIC 1. **Bronze**: Accept everything, ask questions later
# MAGIC 2. **Silver**: Apply business rules and quality checks
# MAGIC 3. **Gold**: Serve specific business use cases
# MAGIC 4. **Delta**: Every layer uses Delta format for reliability

# COMMAND ----------

# DBTITLE 1,Section 2: Bronze Layer
# MAGIC %md
# MAGIC # SECTION 2 — Bronze Layer (Raw Ingestion)
# MAGIC
# MAGIC ## ELI5:
# MAGIC
# MAGIC The Bronze layer is like a **receiving dock** at a warehouse. You accept all deliveries exactly as they arrive — no sorting, no cleaning, just store everything safely.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architect-Level Explanation:
# MAGIC
# MAGIC ### **Bronze Layer Characteristics**:
# MAGIC
# MAGIC * **Purpose**: Raw data ingestion from source systems
# MAGIC * **Data Format**: Delta Lake (ACID compliance)
# MAGIC * **Write Mode**: Typically `append` (preserve all historical data)
# MAGIC * **Transformations**: Minimal to none (may add ingestion metadata)
# MAGIC * **Schema**: Schema-on-read or enforced schema
# MAGIC
# MAGIC ### **Best Practices**:
# MAGIC
# MAGIC 1. Add ingestion metadata columns:
# MAGIC    * `_ingestion_timestamp`
# MAGIC    * `_source_file`
# MAGIC    * `_source_system`
# MAGIC
# MAGIC 2. Never delete source data
# MAGIC 3. Use partition columns if source has natural partitioning
# MAGIC 4. Handle schema evolution gracefully
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Hands-On Demo: Bronze Layer Implementation

# COMMAND ----------

# DBTITLE 1,Configuration Setup
# Configuration for the pipeline
# Replace these with your actual Unity Catalog values

CATALOG = "main"  # Your catalog name
SCHEMA = "default"  # Your schema name

# Table names
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.sales_bronze"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.sales_silver"
GOLD_TABLE = f"{CATALOG}.{SCHEMA}.sales_gold"

print(f"Configuration loaded:")
print(f"   Bronze: {BRONZE_TABLE}")
print(f"   Silver: {SILVER_TABLE}")
print(f"   Gold: {GOLD_TABLE}")

# COMMAND ----------

# DBTITLE 1,Generate Sample Source Data
# Generate sample sales data for demonstration
# In production, this would be your actual source data

from pyspark.sql.functions import col, current_timestamp, lit
from datetime import datetime, timedelta
import random

# Create sample data
data = []
products = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones", "Webcam", "Charger"]
regions = ["North", "South", "East", "West", "Central"]
customers = [f"CUST{str(i).zfill(4)}" for i in range(1, 101)]

for i in range(1, 1001):
    date = datetime.now().date() - timedelta(days=random.randint(0, 90))
    data.append((
        i,
        random.choice(customers),
        random.choice(products),
        random.randint(1, 10),
        round(random.uniform(10.0, 2000.0), 2),
        random.choice(regions),
        str(date)
    ))

# Create DataFrame
df_source = spark.createDataFrame(data, 
    ["transaction_id", "customer_id", "product_name", "quantity", "price", "region", "transaction_date"])

print(f"Generated {df_source.count()} sample transactions")
display(df_source.limit(10))

# COMMAND ----------

# DBTITLE 1,Write to Bronze Layer
# BRONZE LAYER: Ingest raw data with metadata

from pyspark.sql.functions import current_timestamp, lit

# Add ingestion metadata
df_bronze = df_source \
    .withColumn("_ingestion_timestamp", current_timestamp()) \
    .withColumn("_source_system", lit("sample_generator"))

# Write to Bronze table (append mode)
df_bronze.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(BRONZE_TABLE)

print(f"Data written to Bronze layer: {BRONZE_TABLE}")
print(f"   Mode: append")
print(f"   Format: Delta Lake")
print(f"   Records: {df_bronze.count()}")

# COMMAND ----------

# DBTITLE 1,Section 3: Silver Layer
# MAGIC %md
# MAGIC # SECTION 3 — Silver Layer (Transformation & Quality)
# MAGIC
# MAGIC ## ELI5:
# MAGIC
# MAGIC The Silver layer is like **cleaning and organizing your toys**:
# MAGIC * Remove broken toys (bad data)
# MAGIC * Fix toys that need repair (data quality)
# MAGIC * Put similar toys together (standardization)
# MAGIC * Make sure each toy has a label (proper types)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architect-Level Explanation:
# MAGIC
# MAGIC ### **Silver Layer Characteristics**:
# MAGIC
# MAGIC * **Purpose**: Cleaned, validated, and conformed data
# MAGIC * **Data Format**: Delta Lake
# MAGIC * **Write Mode**: Typically `overwrite` or `merge` for SCD
# MAGIC * **Transformations**: Business logic, quality checks, deduplication
# MAGIC * **Schema**: Strongly typed with proper data types
# MAGIC
# MAGIC ### **Common Transformations**:
# MAGIC
# MAGIC 1. **Data Quality**:
# MAGIC    * Filter out null/invalid records
# MAGIC    * Validate business rules
# MAGIC    * Handle outliers
# MAGIC
# MAGIC 2. **Type Casting**:
# MAGIC    * String to proper types (date, int, double)
# MAGIC    * Standardize formats
# MAGIC
# MAGIC 3. **Deduplication**:
# MAGIC    * Remove duplicate records
# MAGIC    * Keep latest version based on timestamp
# MAGIC
# MAGIC 4. **Enrichment**:
# MAGIC    * Add calculated columns
# MAGIC    * Join with reference data
# MAGIC    * Business logic application
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Hands-On Demo: Silver Layer Implementation

# COMMAND ----------

# DBTITLE 1,Transform to Silver Layer
# SILVER LAYER: Clean and transform data

from pyspark.sql.functions import col, to_date, when, trim, upper, round as spark_round
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Read from Bronze
df_bronze = spark.read.table(BRONZE_TABLE)

# Apply transformations
df_silver = df_bronze \
    .filter(col("transaction_id").isNotNull()) \
    .filter(col("price") > 0) \
    .filter(col("quantity") > 0) \
    .withColumn("transaction_date", to_date(col("transaction_date"))) \
    .withColumn("product_name", trim(upper(col("product_name")))) \
    .withColumn("region", trim(upper(col("region")))) \
    .withColumn("total_amount", spark_round(col("price") * col("quantity"), 2)) \
    .select(
        "transaction_id",
        "customer_id",
        "product_name",
        "quantity",
        "price",
        "total_amount",
        "region",
        "transaction_date",
        "_ingestion_timestamp"
    )

print(f"Silver transformation complete")
print(f"Records after quality filters: {df_silver.count()}")
display(df_silver.limit(10))

# COMMAND ----------

# DBTITLE 1,Deduplication Logic
# Deduplication: Keep only the latest record per transaction_id

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

# Create window spec to identify duplicates
window_spec = Window.partitionBy("transaction_id").orderBy(desc("_ingestion_timestamp"))

# Add row number and keep only the first (latest) record
df_silver_dedup = df_silver \
    .withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

print(f"Deduplication complete")
print(f"Records after deduplication: {df_silver_dedup.count()}")
display(df_silver_dedup.limit(10))

# COMMAND ----------

# DBTITLE 1,Write to Silver Layer
# Write to Silver table

df_silver_dedup.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(SILVER_TABLE)

print(f"Data written to Silver layer: {SILVER_TABLE}")
print(f"   Mode: overwrite")
print(f"   Format: Delta Lake")
print(f"   Records: {df_silver_dedup.count()}")

# COMMAND ----------

# DBTITLE 1,Section 4: Gold Layer
# MAGIC %md
# MAGIC # SECTION 4 — Gold Layer (Business Aggregations)
# MAGIC
# MAGIC ## ELI5:
# MAGIC
# MAGIC The Gold layer is like making a **summary report** of your toys:
# MAGIC * Count how many cars vs dolls you have
# MAGIC * Which color is most common?
# MAGIC * Make charts and graphs to show your collection
# MAGIC
# MAGIC This is what grown-ups show to their bosses!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architect-Level Explanation:
# MAGIC
# MAGIC ### **Gold Layer Characteristics**:
# MAGIC
# MAGIC * **Purpose**: Business-level aggregates optimized for analytics
# MAGIC * **Data Format**: Delta Lake
# MAGIC * **Write Mode**: `overwrite` or incremental aggregations
# MAGIC * **Transformations**: GROUP BY, aggregations, KPIs, metrics
# MAGIC * **Schema**: Business-friendly column names
# MAGIC
# MAGIC ### **Common Patterns**:
# MAGIC
# MAGIC 1. **Aggregations**:
# MAGIC    * Revenue by region/product/time
# MAGIC    * Customer counts and segments
# MAGIC    * Inventory levels
# MAGIC
# MAGIC 2. **Joins**:
# MAGIC    * Combine multiple Silver tables
# MAGIC    * Create denormalized views
# MAGIC
# MAGIC 3. **KPIs & Metrics**:
# MAGIC    * YoY growth
# MAGIC    * Moving averages
# MAGIC    * Ranking and percentiles
# MAGIC
# MAGIC 4. **Optimization**:
# MAGIC    * Pre-aggregated for BI tools
# MAGIC    * Partitioned by common filters
# MAGIC    * Z-ordered for query performance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Hands-On Demo: Gold Layer Implementation

# COMMAND ----------

# DBTITLE 1,Create Gold Aggregation: Sales Summary
# GOLD LAYER: Business aggregations

from pyspark.sql.functions import sum, count, avg, max, min, countDistinct, desc

# Read from Silver
df_silver = spark.read.table(SILVER_TABLE)

# Aggregate: Sales summary by product and region
df_gold_sales = df_silver.groupBy("product_name", "region").agg(
    count("transaction_id").alias("total_transactions"),
    countDistinct("customer_id").alias("unique_customers"),
    sum("quantity").alias("total_quantity_sold"),
    sum("total_amount").alias("total_revenue"),
    avg("total_amount").alias("avg_transaction_value"),
    max("total_amount").alias("max_transaction_value"),
    min("total_amount").alias("min_transaction_value")
).orderBy(desc("total_revenue"))

print(f"Gold aggregation: Sales Summary by Product and Region")
print(f"Total aggregated rows: {df_gold_sales.count()}")
display(df_gold_sales)

# COMMAND ----------

# DBTITLE 1,Create Gold Aggregation: Daily Sales Trends
# GOLD LAYER: Daily sales trends

from pyspark.sql.functions import date_trunc, dayofweek, month, year

# Aggregate: Daily sales trends
df_gold_daily = df_silver.groupBy(
    "transaction_date",
    year("transaction_date").alias("year"),
    month("transaction_date").alias("month")
).agg(
    count("transaction_id").alias("daily_transactions"),
    sum("total_amount").alias("daily_revenue"),
    avg("total_amount").alias("avg_order_value")
).orderBy("transaction_date")

print(f"Gold aggregation: Daily Sales Trends")
print(f"Total days with transactions: {df_gold_daily.count()}")
display(df_gold_daily)

# COMMAND ----------

# DBTITLE 1,Write to Gold Layer
# Write aggregated tables to Gold layer

# Table 1: Sales Summary
df_gold_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(GOLD_TABLE)

print(f"Data written to Gold layer: {GOLD_TABLE}")
print(f"   Records: {df_gold_sales.count()}")

# Table 2: Daily Trends (additional Gold table)
GOLD_DAILY_TABLE = f"{CATALOG}.{SCHEMA}.sales_gold_daily"

df_gold_daily.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(GOLD_DAILY_TABLE)

print(f"\nData written to Gold layer: {GOLD_DAILY_TABLE}")
print(f"   Records: {df_gold_daily.count()}")

# COMMAND ----------

# DBTITLE 1,Section 5: Incremental Processing
# MAGIC %md
# MAGIC # SECTION 5 — Incremental Processing (CRITICAL for Production)
# MAGIC
# MAGIC ## ELI5:
# MAGIC
# MAGIC Imagine you have a toy box and get new toys every day:
# MAGIC * **Bad way**: Throw out ALL toys and reorganize everything from scratch every day
# MAGIC * **Smart way**: Just add the new toys and update any broken ones
# MAGIC
# MAGIC Incremental processing is the smart way!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architect-Level Explanation:
# MAGIC
# MAGIC ### **Why Incremental Processing?**
# MAGIC
# MAGIC **Full Reload Problems**:
# MAGIC * Processes entire dataset every time
# MAGIC * Wastes compute resources
# MAGIC * Increases pipeline runtime
# MAGIC * Higher costs
# MAGIC * Not scalable
# MAGIC
# MAGIC **Incremental Benefits**:
# MAGIC * Process only changed data
# MAGIC * Faster execution
# MAGIC * Lower costs
# MAGIC * Scalable to billions of records
# MAGIC * Supports real-time/near-real-time
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Incremental Patterns**:
# MAGIC
# MAGIC 1. **Append-Only** (Bronze layer):
# MAGIC    * Simple append of new records
# MAGIC    * Filter by ingestion timestamp
# MAGIC
# MAGIC 2. **Upsert (MERGE)** (Silver layer):
# MAGIC    * Insert new records
# MAGIC    * Update existing records
# MAGIC    * Handles late-arriving data
# MAGIC
# MAGIC 3. **Time-Based Watermarking**:
# MAGIC    * Track last processed timestamp
# MAGIC    * Process only new data since watermark
# MAGIC
# MAGIC 4. **Change Data Capture (CDC)**:
# MAGIC    * Process insert/update/delete operations
# MAGIC    * Maintain history in Silver/Gold
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Hands-On Demo: Incremental Processing with MERGE

# COMMAND ----------

# DBTITLE 1,Generate New Incremental Data
# Simulate new batch of data arriving

from datetime import datetime, timedelta
import random

print("Generating incremental data batch...\n")

# New transactions
new_data = []
for i in range(1001, 1201):  # 200 new transactions
    date = datetime.now().date() - timedelta(days=random.randint(0, 7))
    new_data.append((
        i,
        random.choice(customers),
        random.choice(products),
        random.randint(1, 10),
        round(random.uniform(10.0, 2000.0), 2),
        random.choice(regions),
        str(date)
    ))

# Some updates to existing transactions (simulating corrections)
for i in range(1, 21):  # Update 20 existing transactions
    date = datetime.now().date() - timedelta(days=random.randint(0, 7))
    new_data.append((
        i,  # Same transaction_id as before
        random.choice(customers),
        random.choice(products),
        random.randint(1, 10),
        round(random.uniform(10.0, 2000.0), 2),
        random.choice(regions),
        str(date)
    ))

df_incremental = spark.createDataFrame(new_data, 
    ["transaction_id", "customer_id", "product_name", "quantity", "price", "region", "transaction_date"])

print(f"Generated {df_incremental.count()} incremental records")
print(f"  - New transactions: 200")
print(f"  - Updated transactions: 20")
display(df_incremental.limit(10))

# COMMAND ----------

# DBTITLE 1,Append to Bronze (Incremental)
# INCREMENTAL: Append new data to Bronze

from pyspark.sql.functions import current_timestamp, lit

# Add metadata
df_incremental_bronze = df_incremental \
    .withColumn("_ingestion_timestamp", current_timestamp()) \
    .withColumn("_source_system", lit("sample_generator_batch2"))

# Append to Bronze (incremental)
df_incremental_bronze.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(BRONZE_TABLE)

print(f"Incremental data appended to Bronze: {BRONZE_TABLE}")
print(f"New records: {df_incremental_bronze.count()}")

# Check total count
total_bronze = spark.read.table(BRONZE_TABLE).count()
print(f"Total records in Bronze: {total_bronze}")

# COMMAND ----------

# DBTITLE 1,MERGE into Silver (Upsert Pattern)
# INCREMENTAL: MERGE (Upsert) into Silver layer

from delta.tables import DeltaTable
from pyspark.sql.functions import col, to_date, when, trim, upper, round as spark_round

# Transform incremental data (same transformations as before)
df_incremental_silver = df_incremental_bronze \
    .filter(col("transaction_id").isNotNull()) \
    .filter(col("price") > 0) \
    .filter(col("quantity") > 0) \
    .withColumn("transaction_date", to_date(col("transaction_date"))) \
    .withColumn("product_name", trim(upper(col("product_name")))) \
    .withColumn("region", trim(upper(col("region")))) \
    .withColumn("total_amount", spark_round(col("price") * col("quantity"), 2)) \
    .select(
        "transaction_id",
        "customer_id",
        "product_name",
        "quantity",
        "price",
        "total_amount",
        "region",
        "transaction_date",
        "_ingestion_timestamp"
    )

# MERGE logic
silver_table = DeltaTable.forName(spark, SILVER_TABLE)

silver_table.alias("target").merge(
    df_incremental_silver.alias("source"),
    "target.transaction_id = source.transaction_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

print(f"MERGE completed into Silver: {SILVER_TABLE}")
print(f"  - Inserted new records")
print(f"  - Updated existing records")
print(f"\nTotal records in Silver: {spark.read.table(SILVER_TABLE).count()}")

# COMMAND ----------

# DBTITLE 1,Section 6: End-to-End Pipeline
# MAGIC %md
# MAGIC # SECTION 6 — End-to-End Pipeline Execution
# MAGIC
# MAGIC ## Complete Pipeline Flow:
# MAGIC
# MAGIC ```
# MAGIC    Raw Data
# MAGIC       ↓
# MAGIC   [BRONZE]
# MAGIC    Append
# MAGIC       ↓
# MAGIC   [SILVER]
# MAGIC    Transform + MERGE
# MAGIC       ↓
# MAGIC    [GOLD]
# MAGIC    Aggregate
# MAGIC       ↓
# MAGIC BI / Analytics
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Pipeline Summary:
# MAGIC
# MAGIC ### **Layer 1: Bronze (Ingestion)**
# MAGIC * Source: Raw CSV/JSON/Parquet files or streaming sources
# MAGIC * Operation: Append-only with metadata
# MAGIC * Result: Complete historical data preserved
# MAGIC
# MAGIC ### **Layer 2: Silver (Transformation)**
# MAGIC * Source: Bronze table
# MAGIC * Operation: Clean, validate, deduplicate, type-cast
# MAGIC * Result: High-quality conformed data
# MAGIC * Mode: MERGE for updates
# MAGIC
# MAGIC ### **Layer 3: Gold (Aggregation)**
# MAGIC * Source: Silver table(s)
# MAGIC * Operation: Business aggregations and joins
# MAGIC * Result: BI-ready analytics tables
# MAGIC * Mode: Overwrite or incremental aggregation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Takeaways:
# MAGIC
# MAGIC 1. Each layer has a **clear purpose**
# MAGIC 2. Delta Lake provides **ACID guarantees** at every layer
# MAGIC 3. **Incremental processing** minimizes compute costs
# MAGIC 4. **Separation of concerns** enables debugging and reusability
# MAGIC 5. **Time travel** allows rollback and audit

# COMMAND ----------

# DBTITLE 1,Section 7: Data Validation
# MAGIC %md
# MAGIC # SECTION 7 — Data Validation & Quality Checks
# MAGIC
# MAGIC ## ELI5:
# MAGIC
# MAGIC Before you submit your homework, you:
# MAGIC * Check if you answered all questions
# MAGIC * Make sure your name is on it
# MAGIC * Count that all pages are there
# MAGIC
# MAGIC Data validation is the same - checking everything is correct!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architect-Level Explanation:
# MAGIC
# MAGIC ### **Validation Types**:
# MAGIC
# MAGIC 1. **Schema Validation**:
# MAGIC    * Column names match
# MAGIC    * Data types are correct
# MAGIC    * No unexpected columns
# MAGIC
# MAGIC 2. **Data Quality Checks**:
# MAGIC    * Null checks on required fields
# MAGIC    * Range validation (price > 0)
# MAGIC    * Referential integrity
# MAGIC    * Duplicate detection
# MAGIC
# MAGIC 3. **Business Rule Validation**:
# MAGIC    * Logical constraints
# MAGIC    * Cross-field validation
# MAGIC    * Temporal consistency
# MAGIC
# MAGIC 4. **Volume Checks**:
# MAGIC    * Row count comparison
# MAGIC    * Expected data arrival
# MAGIC    * Anomaly detection
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Hands-On Demo: Data Quality Checks

# COMMAND ----------

# DBTITLE 1,Data Quality Checks
# VALIDATION: Comprehensive data quality checks

from pyspark.sql.functions import col, count, when, isnan, isnull, sum

print("=" * 60)
print("DATA QUALITY VALIDATION REPORT")
print("=" * 60)

# Bronze validation
print("\n[1] BRONZE LAYER VALIDATION")
df_bronze = spark.read.table(BRONZE_TABLE)
print(f"   Total Records: {df_bronze.count()}")
print(f"   Expected: >= 1000")
print(f"   Status: {'PASS' if df_bronze.count() >= 1000 else 'FAIL'}")

# Silver validation
print("\n[2] SILVER LAYER VALIDATION")
df_silver = spark.read.table(SILVER_TABLE)
silver_count = df_silver.count()
print(f"   Total Records: {silver_count}")

# Null checks
null_checks = df_silver.select(
    count(when(col("transaction_id").isNull(), 1)).alias("null_transaction_id"),
    count(when(col("customer_id").isNull(), 1)).alias("null_customer_id"),
    count(when(col("price").isNull(), 1)).alias("null_price"),
    count(when(col("quantity").isNull(), 1)).alias("null_quantity")
).collect()[0]

print(f"   Null transaction_id: {null_checks['null_transaction_id']}")
print(f"   Null customer_id: {null_checks['null_customer_id']}")
print(f"   Null price: {null_checks['null_price']}")
print(f"   Null quantity: {null_checks['null_quantity']}")
print(f"   Status: {'PASS' if all(v == 0 for v in null_checks.asDict().values()) else 'FAIL'}")

# Business rule validation
invalid_prices = df_silver.filter(col("price") <= 0).count()
invalid_quantities = df_silver.filter(col("quantity") <= 0).count()
print(f"\n   Invalid prices (<= 0): {invalid_prices}")
print(f"   Invalid quantities (<= 0): {invalid_quantities}")
print(f"   Status: {'PASS' if invalid_prices == 0 and invalid_quantities == 0 else 'FAIL'}")

# Gold validation
print("\n[3] GOLD LAYER VALIDATION")
df_gold = spark.read.table(GOLD_TABLE)
print(f"   Total Records: {df_gold.count()}")
print(f"   Expected: > 0")
print(f"   Status: {'PASS' if df_gold.count() > 0 else 'FAIL'}")

# Revenue reconciliation
total_silver_revenue = df_silver.select(sum("total_amount")).collect()[0][0]
total_gold_revenue = df_gold.select(sum("total_revenue")).collect()[0][0]
print(f"\n[4] REVENUE RECONCILIATION")
print(f"   Silver total revenue: ${total_silver_revenue:,.2f}")
print(f"   Gold total revenue: ${total_gold_revenue:,.2f}")
print(f"   Difference: ${abs(total_silver_revenue - total_gold_revenue):,.2f}")
print(f"   Status: {'PASS' if abs(total_silver_revenue - total_gold_revenue) < 0.01 else 'FAIL'}")

print("\n" + "=" * 60)

# COMMAND ----------

# DBTITLE 1,Schema Validation
# Schema validation across layers

print("=" * 60)
print("SCHEMA VALIDATION")
print("=" * 60)

print("\n[BRONZE SCHEMA]")
df_bronze = spark.read.table(BRONZE_TABLE)
df_bronze.printSchema()

print("\n[SILVER SCHEMA]")
df_silver = spark.read.table(SILVER_TABLE)
df_silver.printSchema()

print("\n[GOLD SCHEMA]")
df_gold = spark.read.table(GOLD_TABLE)
df_gold.printSchema()

print("\n" + "=" * 60)

# COMMAND ----------

# DBTITLE 1,Section 8: SDP Integration
# MAGIC %md
# MAGIC # SECTION 8 — Lakeflow Spark Declarative Pipeline (SDP) Integration
# MAGIC
# MAGIC ## ELI5:
# MAGIC
# MAGIC Imagine instead of manually moving toys between boxes, you have a **toy robot** that:
# MAGIC * Automatically checks for new toys
# MAGIC * Cleans and organizes them
# MAGIC * Updates your summary chart
# MAGIC * All by itself, 24/7!
# MAGIC
# MAGIC That's what Lakeflow Spark Declarative Pipelines (SDP) does!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architect-Level Explanation:
# MAGIC
# MAGIC ### **What is Lakeflow Spark Declarative Pipelines (SDP)?**
# MAGIC
# MAGIC **Lakeflow Spark Declarative Pipelines** (formerly Delta Live Tables / DLT) is a declarative ETL framework that:
# MAGIC * Manages end-to-end Delta pipelines
# MAGIC * Provides automatic orchestration
# MAGIC * Handles incremental processing
# MAGIC * Monitors data quality
# MAGIC * Manages dependencies automatically
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Mapping Our Pipeline to SDP**:
# MAGIC
# MAGIC #### **1. Bronze Layer** → SDP Streaming Table:
# MAGIC ```python
# MAGIC @dlt.table(
# MAGIC   name="sales_bronze",
# MAGIC   comment="Raw sales data with ingestion metadata"
# MAGIC )
# MAGIC def bronze_sales():
# MAGIC     return (
# MAGIC         spark.readStream
# MAGIC         .format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "csv")
# MAGIC         .load("/path/to/source")
# MAGIC         .withColumn("_ingestion_timestamp", current_timestamp())
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC #### **2. Silver Layer** → SDP Streaming Table with Expectations:
# MAGIC ```python
# MAGIC @dlt.table(
# MAGIC   name="sales_silver",
# MAGIC   comment="Cleaned and validated sales data"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_price", "price > 0")
# MAGIC @dlt.expect_or_drop("valid_quantity", "quantity > 0")
# MAGIC def silver_sales():
# MAGIC     return (
# MAGIC         dlt.read_stream("sales_bronze")
# MAGIC         .withColumn("transaction_date", to_date(col("transaction_date")))
# MAGIC         .withColumn("total_amount", col("price") * col("quantity"))
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC #### **3. Gold Layer** → SDP Materialized View:
# MAGIC ```python
# MAGIC @dlt.table(
# MAGIC   name="sales_gold",
# MAGIC   comment="Sales aggregations by product and region"
# MAGIC )
# MAGIC def gold_sales():
# MAGIC     return (
# MAGIC         dlt.read("sales_silver")
# MAGIC         .groupBy("product_name", "region")
# MAGIC         .agg(
# MAGIC             count("transaction_id").alias("total_transactions"),
# MAGIC             sum("total_amount").alias("total_revenue")
# MAGIC         )
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **SDP Benefits**:
# MAGIC
# MAGIC 1. **Automatic Orchestration**:
# MAGIC    * No need to manage task dependencies
# MAGIC    * SDP figures out execution order
# MAGIC
# MAGIC 2. **Built-in Data Quality**:
# MAGIC    * `@dlt.expect` decorators
# MAGIC    * Automatic quarantine of bad data
# MAGIC
# MAGIC 3. **Incremental Processing**:
# MAGIC    * Automatic watermarking
# MAGIC    * Process only new data
# MAGIC
# MAGIC 4. **Monitoring & Observability**:
# MAGIC    * Built-in lineage tracking
# MAGIC    * Data quality metrics
# MAGIC    * Pipeline health dashboard
# MAGIC
# MAGIC 5. **Simplified Code**:
# MAGIC    * Declarative syntax
# MAGIC    * Less boilerplate
# MAGIC    * Focus on business logic
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **When to Use Each Approach**:
# MAGIC
# MAGIC | **Traditional Notebook Pipeline** | **Lakeflow SDP** |
# MAGIC |----------------------------------|------------------|
# MAGIC | Learning and prototyping | Production pipelines |
# MAGIC | One-time migrations | Continuous ingestion |
# MAGIC | Complex custom logic | Standard ETL patterns |
# MAGIC | Maximum control | Managed infrastructure |
# MAGIC | Scheduled batch jobs | Real-time streaming |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Next Steps**:
# MAGIC
# MAGIC To convert this notebook pipeline to SDP:
# MAGIC 1. Create a new Lakeflow Spark Declarative Pipeline
# MAGIC 2. Define streaming tables for Bronze
# MAGIC 3. Add expectations for Silver
# MAGIC 4. Create materialized views for Gold
# MAGIC 5. Configure pipeline settings (target schema, compute)
# MAGIC 6. Run and monitor

# COMMAND ----------

# DBTITLE 1,Section 9: Production Architecture
# MAGIC %md
# MAGIC # SECTION 9 — Production Architecture Design
# MAGIC
# MAGIC ## Complete Production Architecture:
# MAGIC
# MAGIC ```
# MAGIC                     PRODUCTION DATA PIPELINE
# MAGIC                    (Medallion Architecture)
# MAGIC
# MAGIC ┌───────────────────────────────────────────────┐
# MAGIC │                 SOURCE SYSTEMS                         │
# MAGIC │  • REST APIs    • Databases    • File Uploads       │
# MAGIC │  • Streaming    • Event Hubs   • Cloud Storage     │
# MAGIC └───────────────────────────────────────────────┘
# MAGIC               │
# MAGIC               ↓ (Ingestion: Auto Loader / Streaming)
# MAGIC               │
# MAGIC ┌───────────────────────────────────────────────┐
# MAGIC │         📦 BRONZE LAYER (Raw Zone)               │
# MAGIC │  • Raw data preservation                           │
# MAGIC │  • Append-only Delta tables                       │
# MAGIC │  • Source lineage tracking                        │
# MAGIC │  • Partitioned by date                            │
# MAGIC └───────────────────────────────────────────────┘
# MAGIC               │
# MAGIC               ↓ (Transform: Clean + Validate + Dedupe)
# MAGIC               │
# MAGIC ┌───────────────────────────────────────────────┐
# MAGIC │      📦 SILVER LAYER (Refined Zone)             │
# MAGIC │  • Cleaned and validated data                     │
# MAGIC │  • MERGE (Upsert) operations                      │
# MAGIC │  • SCD Type 1 or Type 2                          │
# MAGIC │  • Data quality expectations                     │
# MAGIC └───────────────────────────────────────────────┘
# MAGIC               │
# MAGIC               ↓ (Aggregate: Business Logic + KPIs)
# MAGIC               │
# MAGIC ┌───────────────────────────────────────────────┐
# MAGIC │       📦 GOLD LAYER (Consumption Zone)          │
# MAGIC │  • Business-ready aggregates                     │
# MAGIC │  • Denormalized for BI                           │
# MAGIC │  • Optimized (Z-Order, partitioning)            │
# MAGIC │  • Role-based access control                     │
# MAGIC └───────────────────────────────────────────────┘
# MAGIC               │
# MAGIC               ↓ (Consumption)
# MAGIC               │
# MAGIC ┌───────────────────────────────────────────────┐
# MAGIC │              CONSUMPTION LAYER                      │
# MAGIC │  • BI Dashboards (Power BI, Tableau)            │
# MAGIC │  • SQL Analytics                                 │
# MAGIC │  • ML Feature Store                             │
# MAGIC │  • Reports & Alerts                             │
# MAGIC └───────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Production Best Practices:
# MAGIC
# MAGIC ### **1. Data Organization**:
# MAGIC * Use Unity Catalog for governance
# MAGIC * Three-level namespace: `catalog.schema.table`
# MAGIC * Separate dev/staging/prod catalogs
# MAGIC * Apply data classification tags
# MAGIC
# MAGIC ### **2. Performance Optimization**:
# MAGIC * Partition Bronze by ingestion date
# MAGIC * Partition Silver/Gold by business date
# MAGIC * Z-Order frequently filtered columns
# MAGIC * Enable auto-optimize and auto-compaction
# MAGIC * Liquid clustering for high-cardinality columns
# MAGIC
# MAGIC ### **3. Monitoring & Observability**:
# MAGIC * Log pipeline execution metrics
# MAGIC * Track data quality scores
# MAGIC * Set up alerting for failures
# MAGIC * Monitor SLA compliance
# MAGIC * Lineage tracking with Unity Catalog
# MAGIC
# MAGIC ### **4. Error Handling**:
# MAGIC * Quarantine bad records in separate tables
# MAGIC * Implement retry logic with exponential backoff
# MAGIC * Dead letter queues for failed messages
# MAGIC * Graceful degradation strategies
# MAGIC
# MAGIC ### **5. Security & Governance**:
# MAGIC * Row-level security with dynamic views
# MAGIC * Column-level encryption for PII
# MAGIC * Audit logs for all data access
# MAGIC * Data retention policies
# MAGIC * GDPR/CCPA compliance
# MAGIC
# MAGIC ### **6. Cost Optimization**:
# MAGIC * Use Photon acceleration
# MAGIC * Serverless compute for variable workloads
# MAGIC * Schedule non-critical jobs during off-peak
# MAGIC * Archive cold data to cheaper storage
# MAGIC * Optimize file sizes (1GB ideal)
# MAGIC
# MAGIC ### **7. Testing Strategy**:
# MAGIC * Unit tests for transformation logic
# MAGIC * Integration tests for end-to-end flow
# MAGIC * Data quality tests with expectations
# MAGIC * Schema evolution tests
# MAGIC * Performance regression tests

# COMMAND ----------

# DBTITLE 1,Final Summary & Key Learnings
# MAGIC %md
# MAGIC # 🎓 FINAL SUMMARY — Phase 6 Day 30
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📌 Key Learnings:
# MAGIC
# MAGIC ### **1. Medallion Architecture**:
# MAGIC * **Bronze**: Raw data preservation with minimal transformation
# MAGIC * **Silver**: Cleaned, validated, and conformed data
# MAGIC * **Gold**: Business-ready aggregates and analytics
# MAGIC * Each layer serves a distinct purpose and audience
# MAGIC
# MAGIC ### **2. Delta Lake Fundamentals**:
# MAGIC * ACID transactions for data reliability
# MAGIC * Time travel for auditing and rollback
# MAGIC * Schema evolution for flexibility
# MAGIC * MERGE operations for efficient upserts
# MAGIC * Automatic optimization features
# MAGIC
# MAGIC ### **3. Incremental Processing**:
# MAGIC * Process only changed data (not full reloads)
# MAGIC * Use MERGE for upserts in Silver layer
# MAGIC * Watermarking for tracking processed data
# MAGIC * Significantly reduces compute costs and runtime
# MAGIC
# MAGIC ### **4. Production Patterns**:
# MAGIC * Separation of concerns across layers
# MAGIC * Declarative pipelines with SDP for production
# MAGIC * Built-in data quality with expectations
# MAGIC * Monitoring and observability from day one
# MAGIC
# MAGIC ### **5. Serverless Best Practices**:
# MAGIC * No RDD usage (DataFrame API only)
# MAGIC * No cache/persist operations
# MAGIC * Unity Catalog managed tables
# MAGIC * Volumes for file storage
# MAGIC * Auto-scaling compute
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Genie Code Agent - Example Prompts:
# MAGIC
# MAGIC Use these prompts with Genie Code to accelerate development:
# MAGIC
# MAGIC 1. **"Build a Delta pipeline with Bronze, Silver, and Gold layers for customer data"**
# MAGIC
# MAGIC 2. **"Add MERGE logic to handle updates in my Silver table"**
# MAGIC
# MAGIC 3. **"Create data quality checks to validate null values and business rules"**
# MAGIC
# MAGIC 4. **"Convert my batch pipeline to incremental processing"**
# MAGIC
# MAGIC 5. **"Optimize my Delta table with Z-ordering and partitioning"**
# MAGIC
# MAGIC 6. **"Add schema evolution handling to my pipeline"**
# MAGIC
# MAGIC 7. **"Create a production-ready pipeline with monitoring and alerts"**
# MAGIC
# MAGIC 8. **"Implement SCD Type 2 in my Silver layer"**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚠️ Common Mistakes to Avoid:
# MAGIC
# MAGIC ### **1. Mixing Layer Responsibilities**:
# MAGIC * ❌ Doing transformations in Bronze
# MAGIC * ❌ Keeping raw data in Gold
# MAGIC * ✅ Keep each layer focused on its purpose
# MAGIC
# MAGIC ### **2. Using Full Overwrite Unnecessarily**:
# MAGIC * ❌ `mode("overwrite")` on large tables
# MAGIC * ✅ Use MERGE for incremental updates
# MAGIC * ✅ Append for Bronze, MERGE for Silver
# MAGIC
# MAGIC ### **3. Ignoring Data Quality**:
# MAGIC * ❌ No validation of incoming data
# MAGIC * ❌ Assuming data is always clean
# MAGIC * ✅ Implement expectations and quality checks
# MAGIC * ✅ Quarantine bad records
# MAGIC
# MAGIC ### **4. Not Validating Outputs**:
# MAGIC * ❌ Running pipeline without verification
# MAGIC * ❌ No row count reconciliation
# MAGIC * ✅ Compare counts across layers
# MAGIC * ✅ Validate business metrics
# MAGIC
# MAGIC ### **5. Poor Performance Patterns**:
# MAGIC * ❌ Using cache/persist on serverless
# MAGIC * ❌ No partitioning strategy
# MAGIC * ❌ Small file problem
# MAGIC * ✅ Optimize file sizes
# MAGIC * ✅ Use appropriate partitioning
# MAGIC
# MAGIC ### **6. Security Oversights**:
# MAGIC * ❌ Exposing PII in logs
# MAGIC * ❌ No access controls on tables
# MAGIC * ✅ Apply column/row-level security
# MAGIC * ✅ Use Unity Catalog governance
# MAGIC
# MAGIC ### **7. Lack of Monitoring**:
# MAGIC * ❌ No pipeline health tracking
# MAGIC * ❌ Silent failures
# MAGIC * ✅ Implement logging and alerts
# MAGIC * ✅ Track SLA metrics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Interview Questions:
# MAGIC
# MAGIC ### **Basic Level**:
# MAGIC
# MAGIC 1. **What is the Medallion Architecture and why is it used?**
# MAGIC    * Bronze = Raw, Silver = Cleaned, Gold = Aggregated
# MAGIC    * Separation of concerns, debugging ease, reusability
# MAGIC
# MAGIC 2. **What are the benefits of Delta Lake over Parquet?**
# MAGIC    * ACID transactions, time travel, schema evolution, MERGE support
# MAGIC
# MAGIC 3. **What is the difference between append and overwrite modes?**
# MAGIC    * Append adds new data, overwrite replaces all data
# MAGIC
# MAGIC 4. **What metadata should you add to Bronze tables?**
# MAGIC    * Ingestion timestamp, source system, source file
# MAGIC
# MAGIC 5. **Why is incremental processing important?**
# MAGIC    * Reduces compute costs, faster execution, scalability
# MAGIC
# MAGIC ### **Intermediate Level**:
# MAGIC
# MAGIC 6. **How does MERGE operation work in Delta Lake?**
# MAGIC    * Upsert: Insert new + Update existing in single transaction
# MAGIC    * Uses join condition to match records
# MAGIC
# MAGIC 7. **What transformations belong in Silver layer?**
# MAGIC    * Type casting, deduplication, validation, standardization
# MAGIC
# MAGIC 8. **How do you handle late-arriving data?**
# MAGIC    * MERGE with timestamp-based logic
# MAGIC    * Watermarking in streaming
# MAGIC
# MAGIC 9. **What is Z-ordering and when should you use it?**
# MAGIC    * Co-locates related data for faster queries
# MAGIC    * Use on frequently filtered columns
# MAGIC
# MAGIC 10. **How do you implement data quality checks?**
# MAGIC     * Expectations/assertions in code
# MAGIC     * Null checks, range validation, business rules
# MAGIC
# MAGIC ### **Advanced Level**:
# MAGIC
# MAGIC 11. **Compare Notebook pipelines vs Lakeflow Spark Declarative Pipelines**
# MAGIC     * SDP: Declarative, managed, auto-orchestration
# MAGIC     * Notebooks: More control, complex custom logic
# MAGIC
# MAGIC 12. **How would you optimize a slow Delta pipeline?**
# MAGIC     * Partitioning strategy
# MAGIC     * Z-ordering
# MAGIC     * File size optimization
# MAGIC     * Incremental processing
# MAGIC     * Photon acceleration
# MAGIC
# MAGIC 13. **Explain SCD Type 2 implementation in Silver layer**
# MAGIC     * Track history with start/end dates
# MAGIC     * MERGE with WHEN NOT MATCHED BY SOURCE
# MAGIC     * Maintain current flag
# MAGIC
# MAGIC 14. **How do you ensure exactly-once processing?**
# MAGIC     * Idempotent operations
# MAGIC     * Delta transaction log
# MAGIC     * Deduplication keys
# MAGIC
# MAGIC 15. **Design a pipeline for 100TB daily ingestion**
# MAGIC     * Streaming ingestion with Auto Loader
# MAGIC     * Partitioning by date
# MAGIC     * Incremental MERGE to Silver
# MAGIC     * Materialized views for Gold
# MAGIC     * SDP for orchestration
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Next Steps:
# MAGIC
# MAGIC 1. **Practice**: Build pipelines with your own data
# MAGIC 2. **Explore**: Try Lakeflow Spark Declarative Pipelines (SDP)
# MAGIC 3. **Optimize**: Experiment with partitioning and Z-ordering
# MAGIC 4. **Scale**: Test with larger datasets
# MAGIC 5. **Monitor**: Add logging and alerts
# MAGIC 6. **Secure**: Implement Unity Catalog governance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Additional Resources:
# MAGIC
# MAGIC * Databricks Documentation: Delta Lake
# MAGIC * Databricks Documentation: Lakeflow Spark Declarative Pipelines
# MAGIC * Unity Catalog Best Practices
# MAGIC * Delta Lake Performance Tuning Guide
# MAGIC * Medallion Architecture Reference
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏆 Congratulations!
# MAGIC
# MAGIC You've completed **Phase 6 Day 30** - Delta Pipeline Implementation!
# MAGIC
# MAGIC You now have the skills to build production-grade data pipelines using:
# MAGIC * ✅ Medallion Architecture
# MAGIC * ✅ Delta Lake
# MAGIC * ✅ Incremental Processing
# MAGIC * ✅ Data Quality Checks
# MAGIC * ✅ Best Practices
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Remember:
# MAGIC **"A great pipeline is not just about moving data - it's about building trust in your data."**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Author**: TRRaveendra | **Watermark**: @TRRaveendra | **Platform**: Databricks

# COMMAND ----------

# DBTITLE 1,Verify Incremental Processing
# Verify the MERGE operation

df_silver_final = spark.read.table(SILVER_TABLE)

print("=" * 60)
print("INCREMENTAL PROCESSING VERIFICATION")
print("=" * 60)

print(f"\nSilver Table: {SILVER_TABLE}")
print(f"Total Records: {df_silver_final.count()}")

# Check for updated records (transaction_id 1-20)
print("\nSample of updated transactions (ID 1-20):")
updated_records = df_silver_final.filter(col("transaction_id").between(1, 20)).orderBy("transaction_id")
display(updated_records.limit(10))

# Check for new records (transaction_id > 1000)
print("\nSample of new transactions (ID > 1000):")
new_records = df_silver_final.filter(col("transaction_id") > 1000).orderBy("transaction_id")
display(new_records.limit(10))

print(f"\nNew records count: {new_records.count()}")

# COMMAND ----------

# DBTITLE 1,Verify Gold Layer
# Verify Gold tables

from pyspark.sql.functions import desc

print("=" * 60)
print("GOLD LAYER VERIFICATION")
print("=" * 60)

# Table 1: Sales Summary
df_gold = spark.read.table(GOLD_TABLE)
print(f"\nTable: {GOLD_TABLE}")
print(f"Records: {df_gold.count()}")
print("\nTop 5 Product-Region combinations by revenue:")
display(df_gold.limit(5))

# Table 2: Daily Trends
df_gold_daily_read = spark.read.table(GOLD_DAILY_TABLE)
print(f"\nTable: {GOLD_DAILY_TABLE}")
print(f"Records: {df_gold_daily_read.count()}")
print("\nRecent daily trends:")
display(df_gold_daily_read.orderBy(desc("transaction_date")).limit(10))

# COMMAND ----------

# DBTITLE 1,Verify Silver Layer
# Verify Silver table
df_silver_read = spark.read.table(SILVER_TABLE)

print(f"Silver Table: {SILVER_TABLE}")
print(f"Total Records: {df_silver_read.count()}")
print(f"\nSchema:")
df_silver_read.printSchema()
print(f"\nData Quality Summary:")
df_silver_read.select(
    "transaction_date",
    "product_name", 
    "region",
    "total_amount"
).describe().display()

# COMMAND ----------

# DBTITLE 1,Verify Bronze Layer
# Verify Bronze table
df_bronze_read = spark.read.table(BRONZE_TABLE)

print(f"Bronze Table: {BRONZE_TABLE}")
print(f"Total Records: {df_bronze_read.count()}")
print(f"\nSchema:")
df_bronze_read.printSchema()
print(f"\nSample Data:")
display(df_bronze_read.limit(10))