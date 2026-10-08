# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # 🏗 Data Engineering Training — Phase 6 Day 29  
# MAGIC ## 🧱 Medallion Architecture: Bronze, Silver & Gold Layers  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - Medallion Architecture (Lakehouse Design)  
# MAGIC - Bronze, Silver, Gold Layers  
# MAGIC - Data Transformation Patterns  
# MAGIC - Layered Data Processing  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### ⚙️ Platform: Databricks (Serverless + Unity Catalog + Delta Lake + Streaming)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Objective:
# MAGIC Understand and implement Medallion architecture for scalable, maintainable, and production-grade data pipelines.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Engineering Constraints:
# MAGIC - ✅ Use Databricks Serverless Compute
# MAGIC - ❌ DO NOT use RDDs (DataFrame API only)
# MAGIC - ❌ DO NOT use cache() / persist()
# MAGIC - ❌ DO NOT use /tmp or local storage
# MAGIC - ✅ Use Unity Catalog managed tables and Volumes
# MAGIC - ✅ Follow Medallion-first and incremental design
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 1: What is Medallion Architecture?
# MAGIC %md
# MAGIC # 📚 SECTION 1: What is Medallion Architecture?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine you're organizing your toys:
# MAGIC - **Bronze Box** 📦 = Dump all toys here first (messy, everything mixed)
# MAGIC - **Silver Box** ✨ = Sort and clean the toys (remove broken ones, organize by type)
# MAGIC - **Gold Box** 🏆 = Only your favorite, ready-to-play-with toys (grouped by category)
# MAGIC
# MAGIC Medallion Architecture does the same with data!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Medallion Architecture** is a data design pattern that organizes data into three progressive layers:
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────┐
# MAGIC │   RAW DATA SOURCES    │
# MAGIC │  (APIs, Files, DBs)  │
# MAGIC └─────────┬────────────┘
# MAGIC          │
# MAGIC          ↓
# MAGIC ┌─────────┴────────────┐
# MAGIC │   🥉 BRONZE LAYER   │
# MAGIC │   (Raw Ingestion)    │
# MAGIC │  - Schema-on-read   │
# MAGIC │  - No transformations│
# MAGIC │  - Audit trail      │
# MAGIC └─────────┬────────────┘
# MAGIC          │
# MAGIC          ↓
# MAGIC ┌─────────┴────────────┐
# MAGIC │   🥈 SILVER LAYER   │
# MAGIC │   (Cleaned Data)     │
# MAGIC │  - Validated        │
# MAGIC │  - Deduplicated     │
# MAGIC │  - Conformed schema │
# MAGIC └─────────┬────────────┘
# MAGIC          │
# MAGIC          ↓
# MAGIC ┌─────────┴────────────┐
# MAGIC │   🥇 GOLD LAYER     │
# MAGIC │   (Business Ready)   │
# MAGIC │  - Aggregated       │
# MAGIC │  - Business metrics │
# MAGIC │  - Optimized for BI │
# MAGIC └─────────┬────────────┘
# MAGIC          │
# MAGIC          ↓
# MAGIC ┌─────────┴────────────┐
# MAGIC │  BI / ANALYTICS     │
# MAGIC │  (Dashboards, ML)   │
# MAGIC └──────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Layer Definitions:
# MAGIC
# MAGIC | Layer | Purpose | Characteristics | Use Case |
# MAGIC |-------|---------|----------------|----------|
# MAGIC | **🥉 Bronze** | Raw data ingestion | Immutable, unprocessed, schema-on-read | Audit trail, data recovery |
# MAGIC | **🥈 Silver** | Cleaned & validated | Deduplicated, validated, conformed | Analytics-ready data |
# MAGIC | **🥇 Gold** | Business aggregates | Aggregated, optimized, business metrics | Dashboards, reports, ML |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❓ Why Medallion Architecture?
# MAGIC
# MAGIC 1. **Separation of Concerns**: Each layer has a distinct purpose
# MAGIC 2. **Data Quality**: Progressive refinement ensures quality
# MAGIC 3. **Reusability**: Silver layer can feed multiple Gold tables
# MAGIC 4. **Debugging**: Easy to trace issues back to raw data
# MAGIC 5. **Incremental Processing**: Each layer can be processed independently
# MAGIC 6. **Performance**: Gold layer optimized for query performance
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 2: Bronze Layer - Raw Ingestion
# MAGIC %md
# MAGIC # 🥉 SECTION 2: Bronze Layer (Raw Ingestion)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Concept:
# MAGIC
# MAGIC The **Bronze Layer** is the landing zone for all raw data. Think of it as your "data lake" within the Lakehouse.
# MAGIC
# MAGIC ### Key Characteristics:
# MAGIC - **Raw & Unprocessed**: Data is ingested as-is
# MAGIC - **Schema-on-Read**: Flexible schema (can store JSON, nested structures)
# MAGIC - **Immutable**: Original data is never modified
# MAGIC - **Append-Only**: New data is appended, not overwritten
# MAGIC - **Audit Trail**: Complete history of all data received
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC
# MAGIC Bronze Layer = The inbox where all mail arrives. You don't sort it yet, just collect everything.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 👨‍💻 Architect Insight:
# MAGIC
# MAGIC **Bronze Layer Design Principles:**
# MAGIC 1. Preserve data lineage with ingestion metadata
# MAGIC 2. Store raw data in Delta format for ACID guarantees
# MAGIC 3. Use partitioning for large-scale ingestion
# MAGIC 4. Never delete or modify bronze data (immutability)
# MAGIC 5. Add metadata columns: `ingestion_time`, `source_system`, `file_name`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Common Bronze Patterns:
# MAGIC
# MAGIC ```
# MAGIC ✅ Pattern 1: Batch Ingestion
# MAGIC    - Read files from cloud storage
# MAGIC    - Append to Bronze Delta table
# MAGIC
# MAGIC ✅ Pattern 2: Streaming Ingestion  
# MAGIC    - Auto Loader for continuous ingestion
# MAGIC    - Stream to Bronze table
# MAGIC
# MAGIC ✅ Pattern 3: Change Data Capture
# MAGIC    - CDC events from source systems
# MAGIC    - Append to Bronze with operation type
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Bronze Layer - Hands-on Demo
# 🥉 BRONZE LAYER: Raw Data Ingestion
# Simulating raw customer order data from multiple sources

from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# 📊 Generate sample raw data (simulating API/file ingestion)
print("🔄 Step 1: Simulating raw data ingestion...\n")

# Create sample raw order data with realistic variability
raw_data = [
    ("ORD001", "CUST123", "Electronics", 599.99, "2026-04-15", "source_api_v1", "SUCCESS"),
    ("ORD002", "CUST456", "Clothing", 129.50, "2026-04-16", "source_api_v1", "SUCCESS"),
    ("ORD003", "CUST123", "Books", 45.00, "2026-04-16", "source_file_csv", "SUCCESS"),
    ("ORD004", "CUST789", "Electronics", 1299.99, "2026-04-17", "source_api_v2", "SUCCESS"),
    ("ORD005", "CUST456", None, 0.0, "2026-04-17", "source_api_v1", "FAILED"),  # Invalid record
    ("ORD006", None, "Home", 89.99, "2026-04-18", "source_file_json", "SUCCESS"),  # Missing customer
    ("ORD003", "CUST123", "Books", 45.00, "2026-04-16", "source_file_csv", "SUCCESS"),  # Duplicate
    ("ORD007", "CUST999", "Sports", 249.99, "2026-04-19", "source_api_v2", "SUCCESS"),
    ("ORD008", "CUST123", "Electronics", 799.99, "2026-04-20", "source_kafka", "SUCCESS"),
    ("ORD009", "CUST789", "Clothing", 175.00, "2026-04-20", "source_api_v1", "SUCCESS"),
]

# Define schema
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("order_date", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("status", StringType(), True)
])

# Create DataFrame
df_raw = spark.createDataFrame(raw_data, schema)

# 🏷️ Add Bronze Layer metadata (critical for audit trail)
df_bronze = df_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("bronze_layer_id", lit("bronze_orders_v1")) \
    .withColumn("data_quality_flag", lit("RAW"))

print("\n✅ Bronze Layer DataFrame created with metadata:\n")
display(df_bronze)

# 💾 Write to Unity Catalog Bronze Table
print("\n💾 Step 2: Writing to Bronze Delta Table...\n")

try:
    df_bronze.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("main.default.bronze_orders")
    
    print("✅ Bronze layer data successfully written to: main.default.bronze_orders")
    print("\n📊 Bronze Layer Statistics:")
    print(f"   - Total Records Ingested: {df_bronze.count()}")
    print(f"   - Includes: Duplicates, nulls, and invalid records (as-is)")
    print(f"   - Immutable: Original data preserved for audit")
    
except Exception as e:
    print(f"⚠️ Note: {e}")
    print("Continuing with demonstration...")

print("\n" + "="*60)
print("🎯 KEY TAKEAWAY: Bronze = Raw + Metadata (No Transformations!)")
print("="*60)

# COMMAND ----------

# DBTITLE 1,Section 3: Silver Layer - Cleaned Data
# MAGIC %md
# MAGIC # 🥈 SECTION 3: Silver Layer (Cleaned & Validated Data)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Concept:
# MAGIC
# MAGIC The **Silver Layer** transforms raw Bronze data into clean, validated, and analytics-ready data.
# MAGIC
# MAGIC ### Key Characteristics:
# MAGIC - **Data Quality**: Remove nulls, fix data types, validate business rules
# MAGIC - **Deduplication**: Remove duplicate records
# MAGIC - **Conformed Schema**: Standardized column names and types
# MAGIC - **Enrichment**: Join with reference data if needed
# MAGIC - **Filtered**: Remove invalid or failed records
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC
# MAGIC Silver Layer = Cleaning your room. Remove broken toys, organize by type, throw away trash.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 👨‍💻 Architect Insight:
# MAGIC
# MAGIC **Silver Layer Design Principles:**
# MAGIC 1. **Idempotent Transformations**: Re-running should produce same results
# MAGIC 2. **Data Contracts**: Enforce schema and data quality rules
# MAGIC 3. **Type Safety**: Convert strings to proper types (dates, numbers)
# MAGIC 4. **Business Logic**: Apply domain-specific validation
# MAGIC 5. **Incremental Processing**: Process only new/changed data
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Common Silver Transformations:
# MAGIC
# MAGIC ```
# MAGIC ✅ Data Quality Checks:
# MAGIC    - Remove nulls in critical fields
# MAGIC    - Validate date ranges
# MAGIC    - Filter invalid status codes
# MAGIC
# MAGIC ✅ Deduplication:
# MAGIC    - Use window functions with ROW_NUMBER()
# MAGIC    - Keep latest record per key
# MAGIC
# MAGIC ✅ Type Conversions:
# MAGIC    - String → Date
# MAGIC    - String → Numeric
# MAGIC    - Standardize categorical values
# MAGIC
# MAGIC ✅ Enrichment:
# MAGIC    - Join with dimension tables
# MAGIC    - Add calculated columns
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Silver Layer - Hands-on Demo
# 🥈 SILVER LAYER: Clean & Validate Data
# Transform Bronze data into analytics-ready Silver data

from pyspark.sql.functions import col, to_date, when, row_number, count
from pyspark.sql.window import Window

print("🔄 Step 1: Reading from Bronze Layer...\n")

# Read from Bronze layer (in real scenario, this would read from the Delta table)
df_silver_input = df_bronze  # Using the DataFrame from previous cell

print("📊 Initial Bronze Data Statistics:")
print(f"   - Total Records: {df_silver_input.count()}")
print(f"   - Columns: {len(df_silver_input.columns)}")

print("\n" + "="*60)
print("🧼 SILVER TRANSFORMATION PIPELINE")
print("="*60)

# 🛡️ TRANSFORMATION 1: Data Quality - Filter valid records
print("\n✅ Transformation 1: Filter valid records (remove FAILED status)")
df_valid = df_silver_input.filter(col("status") == "SUCCESS")
print(f"   - Records after filtering: {df_valid.count()}")

# 🛡️ TRANSFORMATION 2: Remove records with critical nulls
print("\n✅ Transformation 2: Remove nulls in critical fields")
df_no_nulls = df_valid.filter(
    col("order_id").isNotNull() & 
    col("customer_id").isNotNull() & 
    col("category").isNotNull()
)
print(f"   - Records after null removal: {df_no_nulls.count()}")

# 🛡️ TRANSFORMATION 3: Type conversion (String to Date)
print("\n✅ Transformation 3: Convert order_date to proper date type")
df_typed = df_no_nulls.withColumn(
    "order_date_typed", 
    to_date(col("order_date"), "yyyy-MM-dd")
).drop("order_date").withColumnRenamed("order_date_typed", "order_date")

# 🛡️ TRANSFORMATION 4: Deduplication (keep latest record)
print("\n✅ Transformation 4: Deduplicate records (keep latest by ingestion_timestamp)")

window_spec = Window.partitionBy("order_id").orderBy(col("ingestion_timestamp").desc())

df_deduped = df_typed \
    .withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

print(f"   - Records after deduplication: {df_deduped.count()}")

# 🛡️ TRANSFORMATION 5: Business logic - Categorize amounts
print("\n✅ Transformation 5: Add business logic (order size category)")
df_silver = df_deduped.withColumn(
    "order_size_category",
    when(col("amount") < 100, "Small")
    .when((col("amount") >= 100) & (col("amount") < 500), "Medium")
    .when(col("amount") >= 500, "Large")
    .otherwise("Unknown")
).withColumn("silver_layer_id", lit("silver_orders_v1"))

print("\n✅ SILVER LAYER: Clean Data Ready!\n")
display(df_silver.select(
    "order_id", "customer_id", "category", "amount", 
    "order_date", "order_size_category", "ingestion_timestamp"
))

# 💾 Write to Unity Catalog Silver Table
print("\n💾 Step 2: Writing to Silver Delta Table...\n")

try:
    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("main.default.silver_orders")
    
    print("✅ Silver layer data successfully written to: main.default.silver_orders")
    print("\n📊 Silver Layer Statistics:")
    print(f"   - Clean Records: {df_silver.count()}")
    print(f"   - Data Quality: No nulls, no duplicates, validated")
    print(f"   - Enrichment: Added order_size_category")
    
except Exception as e:
    print(f"⚠️ Note: {e}")
    print("Continuing with demonstration...")

print("\n" + "="*60)
print("🎯 KEY TAKEAWAY: Silver = Bronze + Quality + Dedup + Type Safety")
print("="*60)

# COMMAND ----------

# DBTITLE 1,Section 4: Gold Layer - Business Aggregates
# MAGIC %md
# MAGIC # 🥇 SECTION 4: Gold Layer (Business-Ready Aggregates)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Concept:
# MAGIC
# MAGIC The **Gold Layer** contains curated, business-ready data optimized for analytics, reporting, and ML.
# MAGIC
# MAGIC ### Key Characteristics:
# MAGIC - **Aggregated**: Pre-computed metrics and KPIs
# MAGIC - **Denormalized**: Optimized for query performance
# MAGIC - **Business Context**: Meaningful names and structure
# MAGIC - **Optimized**: Partitioned and indexed for fast queries
# MAGIC - **Multiple Views**: Different aggregations for different use cases
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC
# MAGIC Gold Layer = Your organized toy collection ready to play. Grouped by type, counted, ready to show friends!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 👨‍💻 Architect Insight:
# MAGIC
# MAGIC **Gold Layer Design Principles:**
# MAGIC 1. **Purpose-Built**: Each Gold table serves specific business needs
# MAGIC 2. **Denormalized**: Pre-join dimensions for query performance
# MAGIC 3. **Pre-Aggregated**: Common calculations done once
# MAGIC 4. **Partitioned**: Use business-relevant partitioning (date, region, etc.)
# MAGIC 5. **Documented**: Clear business definitions and ownership
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Common Gold Patterns:
# MAGIC
# MAGIC ```
# MAGIC ✅ Pattern 1: Aggregated Metrics
# MAGIC    - Daily/Monthly sales totals
# MAGIC    - Customer lifetime value
# MAGIC    - Product performance metrics
# MAGIC
# MAGIC ✅ Pattern 2: Dimensional Models
# MAGIC    - Star schema for BI tools
# MAGIC    - Denormalized fact tables
# MAGIC
# MAGIC ✅ Pattern 3: Feature Stores
# MAGIC    - ML-ready features
# MAGIC    - Real-time + batch features
# MAGIC
# MAGIC ✅ Pattern 4: Business KPIs
# MAGIC    - Executive dashboards
# MAGIC    - Regulatory reporting
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Gold Layer Use Cases:
# MAGIC
# MAGIC | Use Case | Description | Example |
# MAGIC |----------|-------------|--------|
# MAGIC | **Dashboards** | Power BI, Tableau | Daily sales by region |
# MAGIC | **Reports** | Scheduled reports | Monthly revenue report |
# MAGIC | **ML Features** | Model training | Customer churn features |
# MAGIC | **APIs** | Data products | Customer 360 API |
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Gold Layer - Hands-on Demo
# 🥇 GOLD LAYER: Business Aggregations & Metrics
# Create business-ready aggregated tables from Silver data

from pyspark.sql.functions import sum as _sum, avg, count, max as _max, min as _min, round as _round

print("🔄 Step 1: Reading from Silver Layer...\n")

# Read from Silver layer
df_gold_input = df_silver  # Using clean Silver DataFrame

print("="*60)
print("🏆 GOLD LAYER: BUSINESS AGGREGATIONS")
print("="*60)

# 📊 GOLD TABLE 1: Customer Analytics
print("\n📈 Gold Table 1: Customer Summary Metrics\n")

df_gold_customer = df_gold_input.groupBy("customer_id").agg(
    count("order_id").alias("total_orders"),
    _sum("amount").alias("total_spent"),
    _round(avg("amount"), 2).alias("avg_order_value"),
    _max("amount").alias("max_order_value"),
    _min("order_date").alias("first_order_date"),
    _max("order_date").alias("last_order_date")
).withColumn("customer_segment",
    when(col("total_spent") >= 1000, "Premium")
    .when((col("total_spent") >= 500) & (col("total_spent") < 1000), "Standard")
    .otherwise("Basic")
)

print("✅ Customer Summary Table:")
display(df_gold_customer.orderBy(col("total_spent").desc()))

# 📊 GOLD TABLE 2: Category Performance
print("\n📈 Gold Table 2: Category Performance Metrics\n")

df_gold_category = df_gold_input.groupBy("category").agg(
    count("order_id").alias("total_orders"),
    _sum("amount").alias("total_revenue"),
    _round(avg("amount"), 2).alias("avg_order_value"),
    count("customer_id").alias("unique_customers")
).withColumn("revenue_rank", 
    row_number().over(Window.orderBy(col("total_revenue").desc()))
)

print("✅ Category Performance Table:")
display(df_gold_category.orderBy("revenue_rank"))

# 📊 GOLD TABLE 3: Daily Business Metrics
print("\n📈 Gold Table 3: Daily Business Metrics\n")

df_gold_daily = df_gold_input.groupBy("order_date").agg(
    count("order_id").alias("orders_count"),
    _sum("amount").alias("daily_revenue"),
    _round(avg("amount"), 2).alias("avg_order_value"),
    count("customer_id").alias("customers_served")
).orderBy("order_date")

print("✅ Daily Metrics Table:")
display(df_gold_daily)

# 💾 Write Gold Tables to Unity Catalog
print("\n💾 Step 2: Writing Gold tables to Unity Catalog...\n")

try:
    # Write Customer Gold Table
    df_gold_customer.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("main.default.gold_customer_summary")
    print("✅ Written: main.default.gold_customer_summary")
    
    # Write Category Gold Table
    df_gold_category.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("main.default.gold_category_performance")
    print("✅ Written: main.default.gold_category_performance")
    
    # Write Daily Gold Table
    df_gold_daily.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("main.default.gold_daily_metrics")
    print("✅ Written: main.default.gold_daily_metrics")
    
    print("\n📊 Gold Layer Statistics:")
    print(f"   - Customer Summaries: {df_gold_customer.count()} customers")
    print(f"   - Category Performance: {df_gold_category.count()} categories")
    print(f"   - Daily Metrics: {df_gold_daily.count()} days")
    print("\n✅ Gold tables ready for BI/Analytics/ML!")
    
except Exception as e:
    print(f"⚠️ Note: {e}")
    print("Continuing with demonstration...")

print("\n" + "="*60)
print("🎯 KEY TAKEAWAY: Gold = Silver + Aggregations + Business Context")
print("="*60)

# COMMAND ----------

# DBTITLE 1,Section 5: Transformation Patterns
# MAGIC %md
# MAGIC # 🔄 SECTION 5: Transformation Patterns in Medallion Architecture
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Common Transformation Patterns:
# MAGIC
# MAGIC ### 1️⃣ **Ingestion Pattern** (Source → Bronze)
# MAGIC
# MAGIC ```python
# MAGIC # Pattern: Raw ingestion with metadata
# MAGIC df.withColumn("ingestion_time", current_timestamp()) \
# MAGIC   .withColumn("source_file", input_file_name()) \
# MAGIC   .write.format("delta").mode("append").saveAsTable("bronze_table")
# MAGIC ```
# MAGIC
# MAGIC **Use When**: Initial data landing from external sources
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ **Validation Pattern** (Bronze → Silver)
# MAGIC
# MAGIC ```python
# MAGIC # Pattern: Filter + Validate + Type conversion
# MAGIC df_silver = df_bronze \
# MAGIC   .filter(col("status") == "VALID") \
# MAGIC   .filter(col("amount") > 0) \
# MAGIC   .withColumn("order_date", to_date(col("order_date_str")))
# MAGIC ```
# MAGIC
# MAGIC **Use When**: Ensuring data quality before analytics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ **Deduplication Pattern** (Bronze → Silver)
# MAGIC
# MAGIC ```python
# MAGIC # Pattern: Window function to keep latest record
# MAGIC window = Window.partitionBy("id").orderBy(col("timestamp").desc())
# MAGIC df_deduped = df.withColumn("rn", row_number().over(window)) \
# MAGIC   .filter(col("rn") == 1).drop("rn")
# MAGIC ```
# MAGIC
# MAGIC **Use When**: Multiple sources provide same record
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4️⃣ **Enrichment Pattern** (Silver → Silver)
# MAGIC
# MAGIC ```python
# MAGIC # Pattern: Join with dimension tables
# MAGIC df_enriched = df_silver.join(df_customers, "customer_id", "left") \
# MAGIC   .join(df_products, "product_id", "left")
# MAGIC ```
# MAGIC
# MAGIC **Use When**: Adding context from reference data
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5️⃣ **Aggregation Pattern** (Silver → Gold)
# MAGIC
# MAGIC ```python
# MAGIC # Pattern: Group by dimensions and aggregate metrics
# MAGIC df_gold = df_silver.groupBy("date", "category").agg(
# MAGIC     sum("amount").alias("total_revenue"),
# MAGIC     count("order_id").alias("order_count"),
# MAGIC     avg("amount").alias("avg_order_value")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Use When**: Creating business metrics for dashboards
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6️⃣ **Incremental Processing Pattern** (All Layers)
# MAGIC
# MAGIC ```python
# MAGIC # Pattern: Process only new data using watermarks
# MAGIC max_timestamp = spark.sql(
# MAGIC     "SELECT MAX(ingestion_time) FROM silver_table"
# MAGIC ).collect()[0][0]
# MAGIC
# MAGIC df_incremental = df_bronze.filter(col("ingestion_time") > max_timestamp)
# MAGIC ```
# MAGIC
# MAGIC **Use When**: Processing large tables efficiently
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔀 Pattern Flow Summary:
# MAGIC
# MAGIC ```
# MAGIC Source → [Ingestion] → Bronze 
# MAGIC                           │
# MAGIC                           ↓ [Validation]
# MAGIC                         Silver
# MAGIC                           │
# MAGIC                           ↓ [Deduplication]
# MAGIC                         Silver (Clean)
# MAGIC                           │
# MAGIC                           ↓ [Enrichment]
# MAGIC                         Silver (Enriched)
# MAGIC                           │
# MAGIC                           ↓ [Aggregation]
# MAGIC                          Gold
# MAGIC                           │
# MAGIC                           ↓
# MAGIC                       BI / Analytics
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 6: Complete Medallion Pipeline
# MAGIC %md
# MAGIC # 🛤️ SECTION 6: Complete End-to-End Medallion Pipeline
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Enterprise Medallion Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────────────┐
# MAGIC │                    DATA SOURCES                         │
# MAGIC │   (APIs, Databases, Files, Streams, CDC Feeds)          │
# MAGIC └─────────────────┬──────────────────────────────────┘
# MAGIC                  │
# MAGIC                  │ Ingestion (Auto Loader / COPY INTO)
# MAGIC                  │
# MAGIC                  ↓
# MAGIC ┌────────────────┴──────────────────────────────────┐
# MAGIC │           🥉 BRONZE LAYER (Raw Zone)               │
# MAGIC │                                                          │
# MAGIC │  Tables:                                                │
# MAGIC │  • bronze_orders          (Raw order data)             │
# MAGIC │  • bronze_customers       (Raw customer data)          │
# MAGIC │  • bronze_products        (Raw product catalog)        │
# MAGIC │                                                          │
# MAGIC │  Characteristics:                                       │
# MAGIC │  ✓ Immutable            ✓ Schema-on-read              │
# MAGIC │  ✓ Append-only          ✓ Complete audit trail       │
# MAGIC │  ✓ No transformations   ✓ Metadata enriched          │
# MAGIC └────────────────┬──────────────────────────────────┘
# MAGIC                  │
# MAGIC                  │ Transformation (Validate, Clean, Dedupe)
# MAGIC                  │
# MAGIC                  ↓
# MAGIC ┌────────────────┴──────────────────────────────────┐
# MAGIC │         🥈 SILVER LAYER (Clean Zone)              │
# MAGIC │                                                          │
# MAGIC │  Tables:                                                │
# MAGIC │  • silver_orders          (Validated orders)           │
# MAGIC │  • silver_customers       (Clean customer master)      │
# MAGIC │  • silver_products        (Standardized products)      │
# MAGIC │                                                          │
# MAGIC │  Characteristics:                                       │
# MAGIC │  ✓ Validated            ✓ Type-safe                  │
# MAGIC │  ✓ Deduplicated         ✓ Business rules applied     │
# MAGIC │  ✓ Conformed schema     ✓ Analytics-ready            │
# MAGIC └────────────────┬──────────────────────────────────┘
# MAGIC                  │
# MAGIC                  │ Aggregation (Business Metrics)
# MAGIC                  │
# MAGIC                  ↓
# MAGIC ┌────────────────┴──────────────────────────────────┐
# MAGIC │          🥇 GOLD LAYER (Business Zone)            │
# MAGIC │                                                          │
# MAGIC │  Tables:                                                │
# MAGIC │  • gold_customer_summary  (Customer 360 view)          │
# MAGIC │  • gold_daily_revenue     (Daily business metrics)     │
# MAGIC │  • gold_category_perf     (Category performance)       │
# MAGIC │  • gold_customer_ltv      (Lifetime value)             │
# MAGIC │                                                          │
# MAGIC │  Characteristics:                                       │
# MAGIC │  ✓ Aggregated           ✓ Business KPIs              │
# MAGIC │  ✓ Denormalized         ✓ Query-optimized            │
# MAGIC │  ✓ Purpose-built        ✓ BI-ready                   │
# MAGIC └────────────────┬──────────────────────────────────┘
# MAGIC                  │
# MAGIC                  │ Consumption
# MAGIC                  │
# MAGIC                  ↓
# MAGIC ┌────────────────┴──────────────────────────────────┐
# MAGIC │              CONSUMPTION LAYER                          │
# MAGIC │                                                          │
# MAGIC │  📊 Power BI / Tableau Dashboards                    │
# MAGIC │  🤖 Machine Learning Models                            │
# MAGIC │  📝 Ad-hoc Analytics (SQL)                            │
# MAGIC │  🌐 APIs / Data Products                               │
# MAGIC │  📧 Scheduled Reports                                 │
# MAGIC └────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Data Flow:
# MAGIC
# MAGIC 1. **Ingestion**: Raw data lands in Bronze (immutable)
# MAGIC 2. **Validation**: Bronze → Silver (clean, typed, deduplicated)
# MAGIC 3. **Enrichment**: Silver → Silver (joins, calculations)
# MAGIC 4. **Aggregation**: Silver → Gold (business metrics)
# MAGIC 5. **Consumption**: Gold → BI/ML/Analytics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏆 Benefits Achieved:
# MAGIC
# MAGIC ✅ **Maintainability**: Clear separation of concerns  
# MAGIC ✅ **Debuggability**: Can trace back to raw data  
# MAGIC ✅ **Performance**: Optimized Gold layer for queries  
# MAGIC ✅ **Quality**: Progressive data quality improvement  
# MAGIC ✅ **Flexibility**: Multiple Gold tables from one Silver  
# MAGIC ✅ **Governance**: Clear data lineage and ownership  
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 7: Incremental Processing
# MAGIC %md
# MAGIC # ⏱️ SECTION 7: Incremental Processing in Medallion Architecture
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Why Incremental Processing?
# MAGIC
# MAGIC **Problem**: Re-processing entire datasets is:
# MAGIC - 🐌 **Slow**: Takes hours for large tables
# MAGIC - 💸 **Expensive**: Wastes compute resources
# MAGIC - 🔄 **Inefficient**: Processes same data repeatedly
# MAGIC
# MAGIC **Solution**: Process only **new or changed data**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC
# MAGIC Instead of washing ALL your clothes every day, you only wash the NEW dirty clothes.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 👨‍💻 Incremental Processing Strategies:
# MAGIC
# MAGIC ### 1️⃣ **Watermark-Based Processing**
# MAGIC
# MAGIC ```python
# MAGIC # Get the last processed timestamp
# MAGIC max_ts = spark.sql(
# MAGIC     "SELECT MAX(ingestion_timestamp) FROM silver_table"
# MAGIC ).collect()[0][0]
# MAGIC
# MAGIC # Process only newer records
# MAGIC df_new = df_bronze.filter(col("ingestion_timestamp") > max_ts)
# MAGIC ```
# MAGIC
# MAGIC **Best For**: Time-series data with timestamps
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ **Change Data Capture (CDC)**
# MAGIC
# MAGIC ```python
# MAGIC # Process CDC events (INSERT, UPDATE, DELETE)
# MAGIC df_cdc = df_bronze.filter(col("operation_type").isin(["I", "U", "D"]))
# MAGIC
# MAGIC # Apply changes to target table
# MAGIC target_table.merge(
# MAGIC     df_cdc,
# MAGIC     condition="target.id = source.id",
# MAGIC     whenMatchedUpdate={"*": "*"},
# MAGIC     whenNotMatchedInsert={"*": "*"}
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Best For**: Database replication, real-time sync
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ **Delta Lake Merge (UPSERT)**
# MAGIC
# MAGIC ```python
# MAGIC # Upsert pattern: Update if exists, Insert if not
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC target = DeltaTable.forName(spark, "silver_table")
# MAGIC
# MAGIC target.alias("target").merge(
# MAGIC     df_source.alias("source"),
# MAGIC     "target.id = source.id"
# MAGIC ).whenMatchedUpdateAll() \
# MAGIC  .whenNotMatchedInsertAll() \
# MAGIC  .execute()
# MAGIC ```
# MAGIC
# MAGIC **Best For**: Slowly Changing Dimensions (SCD Type 1)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4️⃣ **Streaming Incremental Processing**
# MAGIC
# MAGIC ```python
# MAGIC # Structured Streaming for continuous processing
# MAGIC df_stream = spark.readStream \
# MAGIC     .format("delta") \
# MAGIC     .table("bronze_table")
# MAGIC
# MAGIC df_stream.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .outputMode("append") \
# MAGIC     .option("checkpointLocation", "/checkpoint/path") \
# MAGIC     .table("silver_table")
# MAGIC ```
# MAGIC
# MAGIC **Best For**: Real-time, continuous pipelines
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Key Benefits:
# MAGIC
# MAGIC | Benefit | Description |
# MAGIC |---------|-------------|
# MAGIC | **Performance** | 10-100x faster than full reprocessing |
# MAGIC | **Cost** | Lower compute costs |
# MAGIC | **Freshness** | Near real-time data updates |
# MAGIC | **Scalability** | Handles growing data volumes |
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Incremental Processing - Demo
# ⏱️ INCREMENTAL PROCESSING: Process Only New Data
# Demonstrating watermark-based incremental pattern

from pyspark.sql.functions import lit, current_timestamp
from datetime import datetime, timedelta

print("="*60)
print("⏱️ INCREMENTAL PROCESSING DEMONSTRATION")
print("="*60)

print("\n📋 Scenario: New orders arrived after initial Bronze load\n")

# Simulate NEW batch of orders (arriving later)
new_orders_data = [
    ("ORD010", "CUST123", "Electronics", 899.99, "2026-04-21", "source_api_v2", "SUCCESS"),
    ("ORD011", "CUST456", "Books", 55.00, "2026-04-21", "source_api_v1", "SUCCESS"),
    ("ORD012", "CUST999", "Clothing", 199.99, "2026-04-21", "source_api_v2", "SUCCESS"),
]

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("order_date", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("status", StringType(), True)
])

df_new_batch = spark.createDataFrame(new_orders_data, schema) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("bronze_layer_id", lit("bronze_orders_v1")) \
    .withColumn("data_quality_flag", lit("RAW"))

print("✅ Step 1: New batch of orders simulated")
print(f"   - New records: {df_new_batch.count()}")

# 🔍 Step 2: Watermark-based incremental processing
print("\n🔍 Step 2: Incremental Processing Pattern\n")

# Simulate getting max timestamp from existing Silver table
# In production, this would query the actual Silver table
if 'df_silver' in locals():
    # Get the last processed timestamp
    from pyspark.sql.functions import max as _max
    last_processed = df_silver.select(_max("ingestion_timestamp")).collect()[0][0]
    print(f"📅 Last processed timestamp: {last_processed}")
    
    # Filter only NEW records from Bronze
    df_incremental = df_bronze.union(df_new_batch).filter(
        col("ingestion_timestamp") > last_processed
    )
    
    print(f"✅ Incremental records to process: {df_incremental.count()}")
    print("\n📊 Only NEW data is processed (not re-processing old data!)")
else:
    print("⚠️ Simulating incremental processing...")
    df_incremental = df_new_batch

# 🧼 Step 3: Apply Silver transformations to incremental data only
print("\n🧼 Step 3: Transform incremental data (Silver layer logic)\n")

from pyspark.sql.functions import to_date, when

df_silver_incremental = df_incremental \
    .filter(col("status") == "SUCCESS") \
    .filter(col("customer_id").isNotNull()) \
    .filter(col("category").isNotNull()) \
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
    .withColumn(
        "order_size_category",
        when(col("amount") < 100, "Small")
        .when((col("amount") >= 100) & (col("amount") < 500), "Medium")
        .when(col("amount") >= 500, "Large")
        .otherwise("Unknown")
    ).withColumn("silver_layer_id", lit("silver_orders_v1"))

print("✅ Incremental Silver transformation complete")
display(df_silver_incremental.select(
    "order_id", "customer_id", "category", "amount", 
    "order_date", "order_size_category"
))

print("\n" + "="*60)
print("🎯 KEY TAKEAWAY: Incremental = Process Only NEW Data")
print("   ✅ Faster execution")
print("   ✅ Lower costs")
print("   ✅ Scalable for large datasets")
print("="*60)

print("\n📝 Incremental Processing Strategies:")
print("   1️⃣ Watermark-based (timestamp filtering)")
print("   2️⃣ CDC (Change Data Capture)")
print("   3️⃣ Delta MERGE (UPSERT pattern)")
print("   4️⃣ Structured Streaming (continuous)")
print("\n✅ Choose strategy based on your data characteristics!")

# COMMAND ----------

# DBTITLE 1,Section 8: SDP Integration
# MAGIC %md
# MAGIC # 🔄 SECTION 8: Medallion + Lakeflow Spark Declarative Pipelines (SDP)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Integration Overview:
# MAGIC
# MAGIC **Lakeflow Spark Declarative Pipelines (SDP)** - formerly known as Delta Live Tables (DLT) - is Databricks' declarative ETL framework that naturally aligns with Medallion architecture.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ SDP + Medallion Synergy:
# MAGIC
# MAGIC ```
# MAGIC SDP Concepts          ↔️   Medallion Layers
# MAGIC ─────────────────────────────────────────────
# MAGIC
# MAGIC Streaming Tables      →      Bronze Layer
# MAGIC   (raw ingestion)              (immutable raw data)
# MAGIC
# MAGIC Materialized Views    →      Silver Layer
# MAGIC   (cleaned data)               (validated, deduplicated)
# MAGIC
# MAGIC Materialized Views    →      Gold Layer
# MAGIC   (aggregated)                 (business metrics)
# MAGIC
# MAGIC Expectations          →      Data Quality
# MAGIC   (validation rules)           (quality checks)
# MAGIC
# MAGIC Auto CDC              →      Incremental Processing
# MAGIC   (change tracking)            (process changes only)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 👨‍💻 SDP Medallion Example (Conceptual):
# MAGIC
# MAGIC ### Bronze Layer (Streaming Table):
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table(name="bronze_orders")
# MAGIC def bronze_orders():
# MAGIC     return (
# MAGIC         spark.readStream
# MAGIC             .format("cloudFiles")
# MAGIC             .option("cloudFiles.format", "json")
# MAGIC             .load("/path/to/source")
# MAGIC             .withColumn("ingestion_time", current_timestamp())
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Silver Layer (Materialized View with Quality Checks):
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table(name="silver_orders")
# MAGIC @dlt.expect_or_drop("valid_amount", "amount > 0")
# MAGIC @dlt.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
# MAGIC def silver_orders():
# MAGIC     return (
# MAGIC         dlt.read_stream("bronze_orders")
# MAGIC             .filter(col("status") == "SUCCESS")
# MAGIC             .withColumn("order_date", to_date(col("order_date_str")))
# MAGIC             .dropDuplicates(["order_id"])
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Gold Layer (Materialized View with Aggregations):
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table(name="gold_daily_revenue")
# MAGIC def gold_daily_revenue():
# MAGIC     return (
# MAGIC         dlt.read("silver_orders")
# MAGIC             .groupBy("order_date")
# MAGIC             .agg(
# MAGIC                 sum("amount").alias("daily_revenue"),
# MAGIC                 count("order_id").alias("order_count")
# MAGIC             )
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Key SDP Features for Medallion:
# MAGIC
# MAGIC | SDP Feature | Medallion Benefit |
# MAGIC |-------------|------------------|
# MAGIC | **Streaming Tables** | Automatic Bronze ingestion |
# MAGIC | **Materialized Views** | Incremental Silver/Gold processing |
# MAGIC | **Expectations** | Built-in data quality for Silver |
# MAGIC | **Auto CDC** | Automatic change tracking |
# MAGIC | **Lineage** | Automatic dependency tracking |
# MAGIC | **Monitoring** | Built-in pipeline observability |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 When to Use SDP vs Manual Medallion:
# MAGIC
# MAGIC ### Use SDP When:
# MAGIC ✅ Building production ETL pipelines  
# MAGIC ✅ Need automatic dependency management  
# MAGIC ✅ Want built-in monitoring and lineage  
# MAGIC ✅ Require data quality expectations  
# MAGIC ✅ Working with streaming data  
# MAGIC
# MAGIC ### Use Manual Medallion When:
# MAGIC ✅ Ad-hoc analysis and exploration  
# MAGIC ✅ One-time data transformations  
# MAGIC ✅ Learning and prototyping  
# MAGIC ✅ Complex custom logic  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Learn More:
# MAGIC
# MAGIC For production pipelines, consider migrating to **Lakeflow Spark Declarative Pipelines** for:
# MAGIC - Automatic dependency resolution
# MAGIC - Built-in data quality
# MAGIC - Pipeline monitoring
# MAGIC - Automatic retries and error handling
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 9: Genie Code Agent Examples
# MAGIC %md
# MAGIC # 🧞 SECTION 9: Using Genie Code Agent for Medallion Architecture
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💬 Genie Code Agent: Your Medallion Architecture Assistant
# MAGIC
# MAGIC The Databricks Genie Code Agent can help you build, optimize, and troubleshoot Medallion pipelines using natural language prompts.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Example Prompts for Medallion Tasks:
# MAGIC
# MAGIC ### 🥉 Bronze Layer Prompts:
# MAGIC
# MAGIC ```
# MAGIC 💬 "Create a Bronze table that ingests JSON files from 
# MAGIC     /Volumes/main/data/orders with Auto Loader"
# MAGIC
# MAGIC 💬 "Add ingestion metadata (timestamp, source file, source system) 
# MAGIC     to my Bronze table"
# MAGIC
# MAGIC 💬 "Set up a streaming Bronze table that appends new data 
# MAGIC     from cloud storage"
# MAGIC
# MAGIC 💬 "Show me Bronze layer best practices for schema evolution"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🥈 Silver Layer Prompts:
# MAGIC
# MAGIC ```
# MAGIC 💬 "Transform Bronze orders to Silver: remove nulls, deduplicate 
# MAGIC     by order_id, convert date strings to date type"
# MAGIC
# MAGIC 💬 "Create a Silver table that filters out invalid records 
# MAGIC     where status != 'SUCCESS'"
# MAGIC
# MAGIC 💬 "Add data quality checks: amount > 0, customer_id not null, 
# MAGIC     valid date range"
# MAGIC
# MAGIC 💬 "Deduplicate my Bronze data keeping the latest record 
# MAGIC     per order_id based on timestamp"
# MAGIC
# MAGIC 💬 "Enrich Silver customers with geographic data from 
# MAGIC     dim_locations table"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🥇 Gold Layer Prompts:
# MAGIC
# MAGIC ```
# MAGIC 💬 "Create Gold table with daily revenue by category 
# MAGIC     from Silver orders"
# MAGIC
# MAGIC 💬 "Build customer 360 view: total orders, total spent, 
# MAGIC     average order value, customer segment"
# MAGIC
# MAGIC 💬 "Generate Gold table for top 10 customers by revenue 
# MAGIC     per month"
# MAGIC
# MAGIC 💬 "Create aggregated metrics for BI dashboard: 
# MAGIC     daily orders, revenue, unique customers"
# MAGIC
# MAGIC 💬 "Build a Gold feature table for ML model with customer 
# MAGIC     RFM (Recency, Frequency, Monetary) metrics"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⏱️ Incremental Processing Prompts:
# MAGIC
# MAGIC ```
# MAGIC 💬 "Set up incremental processing for my Silver table 
# MAGIC     using watermark on ingestion_timestamp"
# MAGIC
# MAGIC 💬 "Convert my full-load pipeline to incremental using 
# MAGIC     Delta MERGE"
# MAGIC
# MAGIC 💬 "Show me how to process only new records from Bronze 
# MAGIC     since last run"
# MAGIC
# MAGIC 💬 "Implement CDC pattern for Silver table with INSERT, 
# MAGIC     UPDATE, DELETE operations"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔄 Complete Pipeline Prompts:
# MAGIC
# MAGIC ```
# MAGIC 💬 "Build complete Medallion pipeline: ingest JSON files 
# MAGIC     to Bronze, clean to Silver, aggregate to Gold"
# MAGIC
# MAGIC 💬 "Design Bronze-Silver-Gold architecture for e-commerce 
# MAGIC     order data"
# MAGIC
# MAGIC 💬 "Create end-to-end pipeline with data quality checks 
# MAGIC     at each layer"
# MAGIC
# MAGIC 💬 "Optimize my Medallion pipeline for large-scale data 
# MAGIC     processing"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔍 Troubleshooting Prompts:
# MAGIC
# MAGIC ```
# MAGIC 💬 "My Silver table has duplicates, help me add proper 
# MAGIC     deduplication logic"
# MAGIC
# MAGIC 💬 "Optimize Gold aggregation query that's running slowly"
# MAGIC
# MAGIC 💬 "Debug schema evolution issue in Bronze layer"
# MAGIC
# MAGIC 💬 "Add proper error handling for null values in Silver 
# MAGIC     transformations"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Pro Tips for Using Genie Code:
# MAGIC
# MAGIC 1. **Be Specific**: Include table names, columns, and transformations
# MAGIC 2. **Mention Medallion Layer**: Specify Bronze/Silver/Gold context
# MAGIC 3. **Include Constraints**: Mention serverless, Unity Catalog, Delta
# MAGIC 4. **Request Best Practices**: Ask for production-ready patterns
# MAGIC 5. **Iterate**: Start simple, then refine with follow-up prompts
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✨ Example Workflow:
# MAGIC
# MAGIC ```
# MAGIC Step 1: 💬 "Create Bronze table for customer orders"
# MAGIC Step 2: 💬 "Now transform to Silver with quality checks"
# MAGIC Step 3: 💬 "Add Gold aggregation for daily metrics"
# MAGIC Step 4: 💬 "Make it incremental using watermark pattern"
# MAGIC Step 5: 💬 "Optimize for production use"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Remember:
# MAGIC
# MAGIC Genie Code understands:
# MAGIC - Medallion architecture concepts
# MAGIC - Delta Lake best practices
# MAGIC - Unity Catalog patterns
# MAGIC - Serverless constraints
# MAGIC - Production-ready code
# MAGIC
# MAGIC **Just ask in natural language!**
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 10: Summary & Interview Questions
# MAGIC %md
# MAGIC # 🎓 SECTION 10: Summary, Key Learnings & Interview Prep
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Key Learnings:
# MAGIC
# MAGIC ### 1️⃣ **Medallion Architecture Fundamentals**
# MAGIC - Three-layer design: Bronze (raw) → Silver (clean) → Gold (aggregated)
# MAGIC - Progressive data refinement ensures quality and performance
# MAGIC - Clear separation of concerns enables maintainability
# MAGIC
# MAGIC ### 2️⃣ **Bronze Layer Principles**
# MAGIC - Immutable raw data storage
# MAGIC - Preserve complete audit trail
# MAGIC - Schema-on-read flexibility
# MAGIC - Add ingestion metadata (timestamp, source, file)
# MAGIC
# MAGIC ### 3️⃣ **Silver Layer Principles**
# MAGIC - Data quality enforcement (validation, deduplication)
# MAGIC - Type safety (convert strings to proper types)
# MAGIC - Business rule application
# MAGIC - Analytics-ready clean data
# MAGIC
# MAGIC ### 4️⃣ **Gold Layer Principles**
# MAGIC - Purpose-built aggregations for specific use cases
# MAGIC - Denormalized for query performance
# MAGIC - Pre-computed business metrics
# MAGIC - Optimized for BI, ML, and reporting
# MAGIC
# MAGIC ### 5️⃣ **Incremental Processing**
# MAGIC - Process only new/changed data
# MAGIC - Use watermarks, CDC, or Delta MERGE
# MAGIC - Critical for scalability and cost optimization
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💼 Interview Questions & Answers:
# MAGIC
# MAGIC ### Q1: **What is Medallion Architecture and why is it important?**
# MAGIC
# MAGIC **Answer:**  
# MAGIC Medallion Architecture is a data design pattern that organizes data into three progressive layers:
# MAGIC - **Bronze**: Raw, immutable data with complete audit trail
# MAGIC - **Silver**: Cleaned, validated, and deduplicated data
# MAGIC - **Gold**: Aggregated business metrics optimized for consumption
# MAGIC
# MAGIC **Importance:**
# MAGIC - **Maintainability**: Clear separation of concerns
# MAGIC - **Debuggability**: Can trace back to raw data
# MAGIC - **Performance**: Optimized layers for different use cases
# MAGIC - **Quality**: Progressive data quality improvement
# MAGIC - **Reusability**: One Silver table can feed multiple Gold tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q2: **What's the difference between Bronze, Silver, and Gold layers?**
# MAGIC
# MAGIC **Answer:**
# MAGIC
# MAGIC | Aspect | Bronze | Silver | Gold |
# MAGIC |--------|--------|--------|------|
# MAGIC | **Data State** | Raw, unprocessed | Cleaned, validated | Aggregated, business-ready |
# MAGIC | **Mutability** | Immutable | Can be updated | Updated as Silver changes |
# MAGIC | **Schema** | Schema-on-read | Fixed schema | Denormalized schema |
# MAGIC | **Quality** | No validation | Quality checks applied | High quality metrics |
# MAGIC | **Use Case** | Audit trail | Analytics queries | BI dashboards, ML |
# MAGIC | **Processing** | Append-only | Incremental updates | Aggregations |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q3: **How do you implement incremental processing in Medallion architecture?**
# MAGIC
# MAGIC **Answer:**  
# MAGIC Four main strategies:
# MAGIC
# MAGIC 1. **Watermark-Based**: Track max timestamp, process only newer records
# MAGIC    ```python
# MAGIC    max_ts = spark.sql("SELECT MAX(ts) FROM silver").collect()[0][0]
# MAGIC    df_new = df_bronze.filter(col("ts") > max_ts)
# MAGIC    ```
# MAGIC
# MAGIC 2. **CDC (Change Data Capture)**: Process INSERT/UPDATE/DELETE events
# MAGIC    ```python
# MAGIC    df_cdc.filter(col("operation").isin(["I", "U", "D"]))
# MAGIC    ```
# MAGIC
# MAGIC 3. **Delta MERGE**: Upsert pattern for SCD Type 1
# MAGIC    ```python
# MAGIC    target.merge(source, "id").whenMatchedUpdateAll()
# MAGIC          .whenNotMatchedInsertAll().execute()
# MAGIC    ```
# MAGIC
# MAGIC 4. **Streaming**: Continuous processing with checkpoints
# MAGIC    ```python
# MAGIC    spark.readStream.format("delta").table("bronze")
# MAGIC         .writeStream.table("silver")
# MAGIC    ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q4: **What are common mistakes in implementing Medallion architecture?**
# MAGIC
# MAGIC **Answer:**
# MAGIC
# MAGIC 1. **Mixing Layer Logic**: Doing aggregations in Silver or cleaning in Gold
# MAGIC 2. **Skipping Silver**: Going directly Bronze → Gold loses maintainability
# MAGIC 3. **Mutating Bronze**: Modifying raw data breaks audit trail
# MAGIC 4. **No Incremental Processing**: Full reprocessing is expensive and slow
# MAGIC 5. **Overloading Gold**: Too many transformations; keep it simple
# MAGIC 6. **No Data Quality Checks**: Silver should enforce quality rules
# MAGIC 7. **Poor Partitioning**: Not partitioning by date/business key affects performance
# MAGIC 8. **No Documentation**: Not documenting business logic and ownership
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q5: **How does Medallion Architecture integrate with Spark Declarative Pipelines (SDP)?**
# MAGIC
# MAGIC **Answer:**  
# MAGIC SDP (formerly Delta Live Tables) naturally aligns with Medallion:
# MAGIC
# MAGIC - **Streaming Tables** → Bronze Layer (raw ingestion)
# MAGIC - **Materialized Views** → Silver Layer (cleaned data)
# MAGIC - **Materialized Views** → Gold Layer (aggregations)
# MAGIC - **Expectations** → Data quality checks in Silver
# MAGIC - **Auto CDC** → Incremental processing
# MAGIC
# MAGIC SDP provides:
# MAGIC - Automatic dependency management
# MAGIC - Built-in monitoring and lineage
# MAGIC - Data quality expectations
# MAGIC - Automatic retries and error handling
# MAGIC
# MAGIC **Use SDP for production pipelines, Manual Medallion for exploration/prototyping.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q6: **What metadata should you add to Bronze layer tables?**
# MAGIC
# MAGIC **Answer:**  
# MAGIC Critical Bronze metadata columns:
# MAGIC
# MAGIC ```python
# MAGIC df_bronze = df_raw \
# MAGIC     .withColumn("ingestion_timestamp", current_timestamp()) \
# MAGIC     .withColumn("ingestion_date", current_date()) \
# MAGIC     .withColumn("source_system", lit("api_v1")) \
# MAGIC     .withColumn("source_file", input_file_name()) \
# MAGIC     .withColumn("pipeline_id", lit("bronze_pipeline_v1")) \
# MAGIC     .withColumn("data_quality_flag", lit("RAW"))
# MAGIC ```
# MAGIC
# MAGIC **Purpose:**
# MAGIC - Audit trail and compliance
# MAGIC - Debugging and troubleshooting
# MAGIC - Incremental processing (watermarks)
# MAGIC - Data lineage tracking
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q7: **How do you handle late-arriving data in Medallion architecture?**
# MAGIC
# MAGIC **Answer:**
# MAGIC
# MAGIC 1. **Bronze Layer**: Always accept late data (append-only)
# MAGIC 2. **Silver Layer**: Use Delta MERGE to update existing records
# MAGIC    ```python
# MAGIC    target.merge(source, "id AND date") \
# MAGIC        .whenMatchedUpdateAll() \
# MAGIC        .whenNotMatchedInsertAll()
# MAGIC    ```
# MAGIC 3. **Gold Layer**: Recompute affected aggregations
# MAGIC    - Use incremental aggregation patterns
# MAGIC    - Partition by date to limit reprocessing scope
# MAGIC
# MAGIC **Best Practice**: Design Silver/Gold for idempotency — re-running produces same results.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q8: **What are the performance optimization strategies for each Medallion layer?**
# MAGIC
# MAGIC **Answer:**
# MAGIC
# MAGIC **Bronze Layer:**
# MAGIC - Partition by ingestion date for easy pruning
# MAGIC - Use Auto Loader for efficient file ingestion
# MAGIC - Enable schema evolution with `mergeSchema=true`
# MAGIC
# MAGIC **Silver Layer:**
# MAGIC - Partition by business date (order_date, transaction_date)
# MAGIC - Use Z-ORDER for frequently filtered columns
# MAGIC - Implement incremental processing (watermarks)
# MAGIC - Deduplicate efficiently with window functions
# MAGIC
# MAGIC **Gold Layer:**
# MAGIC - Denormalize to avoid joins at query time
# MAGIC - Partition by report date/dimension
# MAGIC - Use OPTIMIZE and VACUUM regularly
# MAGIC - Consider materialized views for expensive aggregations
# MAGIC
# MAGIC **General:**
# MAGIC - Avoid `cache()`/`persist()` on serverless
# MAGIC - Use Delta Lake's `OPTIMIZE` and `ZORDER`
# MAGIC - Monitor with query history and Spark UI
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q9: **How do you ensure data quality in Medallion architecture?**
# MAGIC
# MAGIC **Answer:**
# MAGIC
# MAGIC **Bronze Layer:**
# MAGIC - No quality checks (accept all data as-is)
# MAGIC - Add metadata for traceability
# MAGIC
# MAGIC **Silver Layer** (Primary quality enforcement):
# MAGIC ```python
# MAGIC # Null checks
# MAGIC df.filter(col("customer_id").isNotNull())
# MAGIC
# MAGIC # Range validation
# MAGIC df.filter((col("amount") > 0) & (col("amount") < 1000000))
# MAGIC
# MAGIC # Business rules
# MAGIC df.filter(col("order_date") >= current_date() - expr("INTERVAL 2 YEARS"))
# MAGIC
# MAGIC # Deduplication
# MAGIC df.dropDuplicates(["order_id"])
# MAGIC
# MAGIC # Type safety
# MAGIC df.withColumn("order_date", to_date(col("order_date_str")))
# MAGIC ```
# MAGIC
# MAGIC **Gold Layer:**
# MAGIC - Validate aggregation logic
# MAGIC - Add data quality metrics (record counts, null counts)
# MAGIC - Monitor for anomalies
# MAGIC
# MAGIC **Best Practice**: Use SDP Expectations for automated quality checks.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q10: **When should you use multiple Gold tables vs a single Gold table?**
# MAGIC
# MAGIC **Answer:**
# MAGIC
# MAGIC **Use Multiple Gold Tables When:**
# MAGIC ✅ Different business use cases (customer analytics vs product analytics)  
# MAGIC ✅ Different aggregation levels (daily vs monthly)  
# MAGIC ✅ Different consumers (BI dashboards vs ML features)  
# MAGIC ✅ Different access controls (finance vs marketing)  
# MAGIC ✅ Different refresh frequencies (real-time vs daily batch)  
# MAGIC
# MAGIC **Example Structure:**
# MAGIC ```
# MAGIC Silver:
# MAGIC   • silver_orders (cleaned orders)
# MAGIC
# MAGIC Gold:
# MAGIC   • gold_customer_summary (customer 360 view)
# MAGIC   • gold_daily_revenue (daily business metrics)
# MAGIC   • gold_product_performance (product analytics)
# MAGIC   • gold_ml_features (ML feature store)
# MAGIC   • gold_executive_dashboard (executive KPIs)
# MAGIC ```
# MAGIC
# MAGIC **Benefits:**
# MAGIC - Optimized for specific queries
# MAGIC - Independent refresh schedules
# MAGIC - Better access control
# MAGIC - Easier to maintain and understand
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Common Mistakes to Avoid:
# MAGIC
# MAGIC ### 1. **Mixing Bronze/Silver Logic**
# MAGIC ❌ Don't clean data in Bronze  
# MAGIC ✅ Keep Bronze raw, do cleaning in Silver
# MAGIC
# MAGIC ### 2. **Skipping Silver Layer**
# MAGIC ❌ Don't go Bronze → Gold directly  
# MAGIC ✅ Silver provides reusable cleaned data
# MAGIC
# MAGIC ### 3. **Overloading Gold Layer**
# MAGIC ❌ Don't do complex transformations in Gold  
# MAGIC ✅ Gold should be simple aggregations from Silver
# MAGIC
# MAGIC ### 4. **Not Using Incremental Processing**
# MAGIC ❌ Don't reprocess entire tables every time  
# MAGIC ✅ Use watermarks, CDC, or streaming
# MAGIC
# MAGIC ### 5. **Poor Naming Conventions**
# MAGIC ❌ Don't use generic names (table1, data_final)  
# MAGIC ✅ Use clear layer prefixes (bronze_orders, silver_orders, gold_daily_revenue)
# MAGIC
# MAGIC ### 6. **No Data Quality Checks**
# MAGIC ❌ Don't skip validation in Silver  
# MAGIC ✅ Enforce quality rules at Silver layer
# MAGIC
# MAGIC ### 7. **Mutating Bronze Data**
# MAGIC ❌ Don't update or delete Bronze records  
# MAGIC ✅ Keep Bronze immutable for audit trail
# MAGIC
# MAGIC ### 8. **Not Using Unity Catalog**
# MAGIC ❌ Don't use legacy Hive metastore  
# MAGIC ✅ Use Unity Catalog for governance and lineage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Best Practices Summary:
# MAGIC
# MAGIC ✅ **Always separate Bronze, Silver, Gold layers**  
# MAGIC ✅ **Keep Bronze immutable (append-only)**  
# MAGIC ✅ **Enforce data quality in Silver**  
# MAGIC ✅ **Use incremental processing for scale**  
# MAGIC ✅ **Partition by business-relevant keys**  
# MAGIC ✅ **Document business logic and ownership**  
# MAGIC ✅ **Use Unity Catalog for governance**  
# MAGIC ✅ **Monitor pipeline performance and data quality**  
# MAGIC ✅ **Use SDP for production pipelines**  
# MAGIC ✅ **Test with small datasets first**  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Next Steps:
# MAGIC
# MAGIC 1. **Practice**: Build your own Medallion pipeline with real data
# MAGIC 2. **Explore SDP**: Learn Lakeflow Spark Declarative Pipelines
# MAGIC 3. **Advanced Topics**: 
# MAGIC    - Slowly Changing Dimensions (SCD Type 2)
# MAGIC    - Real-time streaming pipelines
# MAGIC    - ML feature stores
# MAGIC    - Data mesh patterns
# MAGIC 4. **Production**: Apply to enterprise use cases
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎆 Congratulations!
# MAGIC
# MAGIC You now understand:
# MAGIC ✅ Medallion Architecture principles  
# MAGIC ✅ Bronze, Silver, Gold layer design  
# MAGIC ✅ Transformation patterns  
# MAGIC ✅ Incremental processing strategies  
# MAGIC ✅ SDP integration  
# MAGIC ✅ Production best practices  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏷️ **@TRRaveendra**
# MAGIC ### 📚 Phase 6 Day 29 Complete!
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Bonus: Enterprise Use Case
# MAGIC %md
# MAGIC # 🏢 BONUS: Real-World Enterprise Use Case
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💼 Use Case: E-Commerce Order Analytics Platform
# MAGIC
# MAGIC ### Business Context:
# MAGIC A global e-commerce company needs to build a scalable analytics platform to:
# MAGIC - Track real-time order metrics
# MAGIC - Analyze customer behavior
# MAGIC - Optimize inventory and supply chain
# MAGIC - Power executive dashboards
# MAGIC - Feed ML models for recommendations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Enterprise Medallion Design:
# MAGIC
# MAGIC ```
# MAGIC ┌───────────────────────────────────────────────────────────────────────┐
# MAGIC │                      DATA SOURCES                                   │
# MAGIC │  • Website Orders (REST API - JSON)                                │
# MAGIC │  • Mobile App Orders (Kafka Stream)                               │
# MAGIC │  • Store POS Systems (CSV Files)                                  │
# MAGIC │  • Customer Database (CDC from PostgreSQL)                        │
# MAGIC │  • Inventory System (Delta Sharing)                               │
# MAGIC └─────────────────────────────┬─────────────────────────────────────────┘
# MAGIC                              │
# MAGIC                              │ Ingestion Layer
# MAGIC                              │ (Auto Loader + Streaming)
# MAGIC                              ↓
# MAGIC ┌─────────────────────────────┴─────────────────────────────────────────┐
# MAGIC │           🥉 BRONZE LAYER - Raw Data Lake                    │
# MAGIC │                                                                    │
# MAGIC │  catalog: production                                               │
# MAGIC │  schema: bronze                                                    │
# MAGIC │                                                                    │
# MAGIC │  Tables:                                                           │
# MAGIC │  • production.bronze.orders_web_api                              │
# MAGIC │  • production.bronze.orders_mobile_stream                        │
# MAGIC │  • production.bronze.orders_pos_files                            │
# MAGIC │  • production.bronze.customers_cdc                               │
# MAGIC │  • production.bronze.inventory_snapshot                          │
# MAGIC │                                                                    │
# MAGIC │  Retention: 30 days (compliance)                                   │
# MAGIC │  Partitioned by: ingestion_date                                    │
# MAGIC └─────────────────────────────┬─────────────────────────────────────────┘
# MAGIC                              │
# MAGIC                              │ Transformation Layer
# MAGIC                              │ (Validation, Deduplication, Enrichment)
# MAGIC                              ↓
# MAGIC ┌─────────────────────────────┴─────────────────────────────────────────┐
# MAGIC │         🥈 SILVER LAYER - Analytics Foundation              │
# MAGIC │                                                                    │
# MAGIC │  catalog: production                                               │
# MAGIC │  schema: silver                                                    │
# MAGIC │                                                                    │
# MAGIC │  Core Tables:                                                      │
# MAGIC │  • production.silver.orders_unified                              │
# MAGIC │     - Merged from all sources                                      │
# MAGIC │     - Deduplicated by order_id                                     │
# MAGIC │     - Validated (amount > 0, valid dates)                          │
# MAGIC │     - Enriched with customer/product dimensions                    │
# MAGIC │                                                                    │
# MAGIC │  • production.silver.customers_master                            │
# MAGIC │     - SCD Type 2 (historical tracking)                             │
# MAGIC │     - Merged from CDC events                                       │
# MAGIC │                                                                    │
# MAGIC │  • production.silver.inventory_current                           │
# MAGIC │     - Current stock levels                                         │
# MAGIC │     - Real-time updates                                            │
# MAGIC │                                                                    │
# MAGIC │  Retention: 2 years                                                │
# MAGIC │  Partitioned by: order_date                                        │
# MAGIC │  Z-Ordered by: customer_id, product_id                             │
# MAGIC └─────────────────────────────┬─────────────────────────────────────────┘
# MAGIC                              │
# MAGIC                              │ Aggregation Layer
# MAGIC                              │ (Business Metrics, KPIs)
# MAGIC                              ↓
# MAGIC ┌─────────────────────────────┴─────────────────────────────────────────┐
# MAGIC │          🥇 GOLD LAYER - Business Products                  │
# MAGIC │                                                                    │
# MAGIC │  catalog: production                                               │
# MAGIC │  schema: gold                                                      │
# MAGIC │                                                                    │
# MAGIC │  Business Dashboards:                                              │
# MAGIC │  • production.gold.executive_daily_kpis                          │
# MAGIC │     - Total revenue, orders, customers                             │
# MAGIC │     - YoY, MoM comparisons                                         │
# MAGIC │     - Refresh: Every 15 minutes                                    │
# MAGIC │                                                                    │
# MAGIC │  • production.gold.customer_360_view                             │
# MAGIC │     - Customer lifetime value                                      │
# MAGIC │     - RFM segmentation                                             │
# MAGIC │     - Churn risk scores                                            │
# MAGIC │                                                                    │
# MAGIC │  • production.gold.product_performance                           │
# MAGIC │     - Sales by product/category                                    │
# MAGIC │     - Inventory turnover                                           │
# MAGIC │     - Price elasticity                                             │
# MAGIC │                                                                    │
# MAGIC │  • production.gold.regional_metrics                              │
# MAGIC │     - Sales by region/country                                      │
# MAGIC │     - Shipping performance                                         │
# MAGIC │                                                                    │
# MAGIC │  ML Feature Store:                                                 │
# MAGIC │  • production.gold.ml_recommendation_features                    │
# MAGIC │  • production.gold.ml_churn_prediction_features                  │
# MAGIC │  • production.gold.ml_demand_forecast_features                   │
# MAGIC │                                                                    │
# MAGIC │  Retention: 5 years                                                │
# MAGIC │  Optimized: OPTIMIZE + ZORDER                                      │
# MAGIC └─────────────────────────────┬─────────────────────────────────────────┘
# MAGIC                              │
# MAGIC                              │ Consumption
# MAGIC                              ↓
# MAGIC ┌─────────────────────────────┴─────────────────────────────────────────┐
# MAGIC │               CONSUMPTION & APPLICATIONS                          │
# MAGIC │                                                                    │
# MAGIC │  📊 Tableau Dashboards:                                          │
# MAGIC │     - Executive dashboard (C-suite)                                │
# MAGIC │     - Operations dashboard (warehouse managers)                    │
# MAGIC │     - Marketing dashboard (campaign teams)                         │
# MAGIC │                                                                    │
# MAGIC │  🤖 ML Models:                                                    │
# MAGIC │     - Product recommendation engine                                │
# MAGIC │     - Customer churn prediction                                    │
# MAGIC │     - Demand forecasting                                           │
# MAGIC │     - Dynamic pricing optimization                                 │
# MAGIC │                                                                    │
# MAGIC │  🌐 Data Products:                                                │
# MAGIC │     - Customer API (real-time customer 360)                        │
# MAGIC │     - Inventory API (stock availability)                           │
# MAGIC │     - Analytics API (external partners)                            │
# MAGIC │                                                                    │
# MAGIC │  📧 Automated Reports:                                            │
# MAGIC │     - Daily sales summary (email)                                  │
# MAGIC │     - Weekly performance report                                    │
# MAGIC │     - Monthly board report                                         │
# MAGIC └───────────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Pipeline Implementation:
# MAGIC
# MAGIC ### Phase 1: Bronze Layer (Week 1)
# MAGIC ```python
# MAGIC # Auto Loader for continuous ingestion
# MAGIC spark.readStream \
# MAGIC     .format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "json") \
# MAGIC     .load("/source/orders/") \
# MAGIC     .writeStream \
# MAGIC     .format("delta") \
# MAGIC     .option("checkpointLocation", "/checkpoints/bronze_orders") \
# MAGIC     .table("production.bronze.orders_web_api")
# MAGIC ```
# MAGIC
# MAGIC ### Phase 2: Silver Layer (Week 2-3)
# MAGIC ```python
# MAGIC # Data quality and transformation
# MAGIC df_silver = df_bronze \
# MAGIC     .filter(col("status") == "SUCCESS") \
# MAGIC     .filter(col("amount") > 0) \
# MAGIC     .dropDuplicates(["order_id"]) \
# MAGIC     .join(df_customers, "customer_id", "left") \
# MAGIC     .join(df_products, "product_id", "left")
# MAGIC
# MAGIC # Merge into Silver (SCD Type 1)
# MAGIC DeltaTable.forName(spark, "production.silver.orders_unified") \
# MAGIC     .merge(df_silver, "target.order_id = source.order_id") \
# MAGIC     .whenMatchedUpdateAll() \
# MAGIC     .whenNotMatchedInsertAll() \
# MAGIC     .execute()
# MAGIC ```
# MAGIC
# MAGIC ### Phase 3: Gold Layer (Week 4)
# MAGIC ```python
# MAGIC # Executive daily KPIs
# MAGIC df_gold = df_silver \
# MAGIC     .filter(col("order_date") == current_date()) \
# MAGIC     .groupBy("order_date") \
# MAGIC     .agg(
# MAGIC         sum("amount").alias("total_revenue"),
# MAGIC         count("order_id").alias("total_orders"),
# MAGIC         countDistinct("customer_id").alias("unique_customers"),
# MAGIC         avg("amount").alias("avg_order_value")
# MAGIC     )
# MAGIC
# MAGIC df_gold.write \
# MAGIC     .format("delta") \
# MAGIC     .mode("overwrite") \
# MAGIC     .saveAsTable("production.gold.executive_daily_kpis")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Business Outcomes:
# MAGIC
# MAGIC ### Achieved Results:
# MAGIC ✅ **Performance**: Reduced query time from 10min → 5sec (Gold layer)  
# MAGIC ✅ **Cost**: 60% reduction in compute costs (incremental processing)  
# MAGIC ✅ **Freshness**: Real-time data (15min latency → streaming)  
# MAGIC ✅ **Quality**: 99.9% data quality (Silver validation)  
# MAGIC ✅ **Scalability**: Handle 10M orders/day  
# MAGIC ✅ **Governance**: Complete data lineage with Unity Catalog  
# MAGIC
# MAGIC ### Business Impact:
# MAGIC 💰 **Revenue**: +12% from ML-powered recommendations  
# MAGIC 📊 **Efficiency**: 80% faster report generation  
# MAGIC 👥 **Adoption**: 500+ users across organization  
# MAGIC 🎯 **Accuracy**: 95% forecast accuracy for demand  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Key Takeaways from Enterprise Implementation:
# MAGIC
# MAGIC 1. **Start Simple**: Begin with one data source, expand incrementally
# MAGIC 2. **Governance First**: Use Unity Catalog from day one
# MAGIC 3. **Incremental Always**: Never full-load in production
# MAGIC 4. **Monitor Everything**: Set up data quality alerts
# MAGIC 5. **Document Well**: Business logic, ownership, SLAs
# MAGIC 6. **Test Thoroughly**: Validate each layer before promotion
# MAGIC 7. **Iterate**: Continuous improvement based on user feedback
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏆 Enterprise Best Practices Applied:
# MAGIC
# MAGIC ✅ **Separation by Environment**: dev / staging / production catalogs  
# MAGIC ✅ **Access Control**: RBAC with Unity Catalog  
# MAGIC ✅ **Data Quality**: Automated validation and alerts  
# MAGIC ✅ **Cost Optimization**: Serverless + incremental processing  
# MAGIC ✅ **Monitoring**: Built-in observability with SDP  
# MAGIC ✅ **Documentation**: Catalog comments and tagging  
# MAGIC ✅ **CI/CD**: Databricks Asset Bundles for deployment  
# MAGIC ✅ **Testing**: Unit tests for transformation logic  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 This is Production-Grade Medallion Architecture!
# MAGIC
# MAGIC ---