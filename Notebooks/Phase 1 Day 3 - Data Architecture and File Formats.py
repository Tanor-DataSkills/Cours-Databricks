# Databricks notebook source
# DBTITLE 1,📘 Notebook Header
# MAGIC %md
# MAGIC # 📊 Data Engineering Training — Phase 1 Day 3  
# MAGIC ## 🏗️ Data Architecture + File Formats  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC * Data Warehouse (Star Schema, Fact & Dimension)
# MAGIC * Data Lake vs Warehouse vs Lakehouse
# MAGIC * Medallion Architecture (Bronze, Silver, Gold)
# MAGIC * File Formats (Parquet, Delta, ORC)
# MAGIC * Row vs Column Storage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### ⚙️ Platform: Databricks (Serverless + Unity Catalog)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Objective:
# MAGIC Build strong understanding of modern data architecture and file formats using hands-on PySpark with Delta Lake.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ IMPORTANT ENGINEERING CONSTRAINTS:
# MAGIC
# MAGIC * ✅ Use Databricks Serverless Compute (no cluster-specific configs)
# MAGIC * ❌ DO NOT use RDDs
# MAGIC * ❌ DO NOT use cache() / persist()
# MAGIC * ❌ DO NOT use /tmp or local storage
# MAGIC * ✅ Use Unity Catalog Volumes for all data access
# MAGIC * ✅ Use `_metadata.file_path` instead of `input_file_name()`
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,🏗️ Section 1: Data Warehouse Fundamentals
# MAGIC %md
# MAGIC ## 🏗️ SECTION 1: Data Warehouse Fundamentals
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧒 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine you have a **toy store**:
# MAGIC * **Fact Table** = Sales Receipt (who bought what, when, how much)
# MAGIC * **Dimension Tables** = Catalog books that describe:
# MAGIC   * Who the customer is
# MAGIC   * What the product details are
# MAGIC   * When the sale happened
# MAGIC
# MAGIC You keep receipts small (just IDs), and lookup details in the catalog when needed!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Star Schema** is a dimensional modeling technique:
# MAGIC
# MAGIC * **Fact Table** (center of the star):
# MAGIC   * Contains measurable, quantitative data (metrics)
# MAGIC   * Contains foreign keys to dimension tables
# MAGIC   * Large volume, narrow width
# MAGIC   * Examples: `sales_fact`, `order_fact`
# MAGIC
# MAGIC * **Dimension Tables** (points of the star):
# MAGIC   * Contain descriptive attributes
# MAGIC   * Denormalized for query performance
# MAGIC   * Smaller volume, wider width
# MAGIC   * Examples: `customer_dim`, `product_dim`, `date_dim`
# MAGIC
# MAGIC **Benefits**:
# MAGIC * Optimized for OLAP (Online Analytical Processing)
# MAGIC * Fast aggregations and filtering
# MAGIC * Intuitive for business users
# MAGIC * Supports dimensional analysis (slicing, dicing, drill-down)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📈 Example Schema:
# MAGIC
# MAGIC ```
# MAGIC         product_dim
# MAGIC               |
# MAGIC               |
# MAGIC customer_dim --- sales_fact --- date_dim
# MAGIC               |
# MAGIC               |
# MAGIC          store_dim
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Demo: Create Star Schema - Dimension Tables
# Import required libraries
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

print("✨ Creating Star Schema Demo Data...\n")

# ============================================
# DIMENSION TABLE 1: Customer Dimension
# ============================================

customer_data = [
    (1, "Alice Johnson", "alice@email.com", "New York", "Premium"),
    (2, "Bob Smith", "bob@email.com", "Los Angeles", "Standard"),
    (3, "Charlie Brown", "charlie@email.com", "Chicago", "Premium"),
    (4, "Diana Prince", "diana@email.com", "Houston", "Standard"),
    (5, "Eve Adams", "eve@email.com", "Phoenix", "Gold"),
]

customer_dim = spark.createDataFrame(
    customer_data,
    ["customer_id", "customer_name", "email", "city", "customer_tier"]
)

print("✅ Customer Dimension:")
display(customer_dim)

# ============================================
# DIMENSION TABLE 2: Product Dimension
# ============================================

product_data = [
    (101, "Laptop", "Electronics", 1200.00, "Dell"),
    (102, "Mouse", "Electronics", 25.00, "Logitech"),
    (103, "Keyboard", "Electronics", 75.00, "Corsair"),
    (104, "Monitor", "Electronics", 350.00, "Samsung"),
    (105, "Desk Chair", "Furniture", 250.00, "Herman Miller"),
]

product_dim = spark.createDataFrame(
    product_data,
    ["product_id", "product_name", "category", "unit_price", "brand"]
)

print("✅ Product Dimension:")
display(product_dim)

# ============================================
# DIMENSION TABLE 3: Date Dimension
# ============================================

date_data = [
    (20260401, "2026-04-01", 2026, 4, "April", 2, "Q2"),
    (20260402, "2026-04-02", 2026, 4, "April", 2, "Q2"),
    (20260403, "2026-04-03", 2026, 4, "April", 2, "Q2"),
    (20260404, "2026-04-04", 2026, 4, "April", 2, "Q2"),
    (20260405, "2026-04-05", 2026, 4, "April", 2, "Q2"),
]

date_dim = spark.createDataFrame(
    date_data,
    ["date_key", "date", "year", "month", "month_name", "quarter", "quarter_name"]
)

print("✅ Date Dimension:")
display(date_dim)

# COMMAND ----------

# DBTITLE 1,Demo: Create Star Schema - Fact Table
# ============================================
# FACT TABLE: Sales Fact
# ============================================

sales_data = [
    (1, 1, 101, 20260401, 2, 2400.00),  # Alice bought 2 Laptops
    (2, 2, 102, 20260401, 5, 125.00),   # Bob bought 5 Mice
    (3, 3, 103, 20260402, 3, 225.00),   # Charlie bought 3 Keyboards
    (4, 1, 104, 20260402, 1, 350.00),   # Alice bought 1 Monitor
    (5, 4, 105, 20260403, 2, 500.00),   # Diana bought 2 Desk Chairs
    (6, 5, 101, 20260403, 1, 1200.00),  # Eve bought 1 Laptop
    (7, 2, 103, 20260404, 1, 75.00),    # Bob bought 1 Keyboard
    (8, 3, 102, 20260404, 10, 250.00),  # Charlie bought 10 Mice
    (9, 5, 104, 20260405, 2, 700.00),   # Eve bought 2 Monitors
    (10, 1, 105, 20260405, 1, 250.00),  # Alice bought 1 Desk Chair
]

sales_fact = spark.createDataFrame(
    sales_data,
    ["sale_id", "customer_id", "product_id", "date_key", "quantity", "total_amount"]
)

print("✅ Sales Fact Table:")
display(sales_fact)

print("\n✨ Star Schema created successfully!")
print(f"  • Customers: {customer_dim.count()}")
print(f"  • Products: {product_dim.count()}")
print(f"  • Dates: {date_dim.count()}")
print(f"  • Sales Transactions: {sales_fact.count()}")

# COMMAND ----------

# DBTITLE 1,Demo: Analytical Query with Star Schema Join
# ============================================
# ANALYTICAL QUERY: Join Fact with Dimensions
# ============================================

print("🔍 Running Analytical Query: Sales by Customer and Product\n")

# Join fact table with all dimensions for rich analytical view
sales_analysis = (
    sales_fact
    .join(customer_dim, "customer_id")
    .join(product_dim, "product_id")
    .join(date_dim, "date_key")
    .select(
        "sale_id",
        "customer_name",
        "customer_tier",
        "city",
        "product_name",
        "category",
        "date",
        "month_name",
        "quantity",
        "total_amount"
    )
    .orderBy("date", "sale_id")
)

print("✅ Enriched Sales Data (Fact + Dimensions):")
display(sales_analysis)

# ============================================
# AGGREGATION: Total Sales by Customer Tier
# ============================================

print("\n📊 Aggregation: Total Sales by Customer Tier\n")

sales_by_tier = (
    sales_analysis
    .groupBy("customer_tier")
    .agg(
        F.sum("total_amount").alias("total_revenue"),
        F.sum("quantity").alias("total_units_sold"),
        F.count("sale_id").alias("total_transactions")
    )
    .orderBy(F.desc("total_revenue"))
)

display(sales_by_tier)

print("\n✨ Star Schema enables fast, intuitive analytics!")

# COMMAND ----------

# DBTITLE 1,🌊 Section 2: Data Lake vs Warehouse vs Lakehouse
# MAGIC %md
# MAGIC ## 🌊 SECTION 2: Data Lake vs Warehouse vs Lakehouse
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC
# MAGIC * **Data Lake** = Big storage room where you dump everything (structured, unstructured) - figure out what it is later
# MAGIC * **Data Warehouse** = Organized filing cabinet where everything has a specific place and format
# MAGIC * **Lakehouse** = Smart storage that combines both - organized like a warehouse but flexible like a lake
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Comparison:
# MAGIC
# MAGIC | Feature | Data Lake | Data Warehouse | Lakehouse |
# MAGIC |---------|-----------|----------------|----------|
# MAGIC | **Storage Format** | Raw files (JSON, CSV, Parquet) | Structured tables | Delta/Iceberg tables |
# MAGIC | **Schema** | Schema-on-read | Schema-on-write | Schema enforcement + evolution |
# MAGIC | **Data Types** | All types (structured, semi-structured, unstructured) | Structured only | All types |
# MAGIC | **ACID Transactions** | ❌ No | ✅ Yes | ✅ Yes |
# MAGIC | **Performance** | Slow for analytics | Fast for analytics | Fast for analytics |
# MAGIC | **Cost** | Low (cheap storage) | High (compute+storage) | Medium (optimized) |
# MAGIC | **Use Case** | Data archival, ML raw data | BI reporting, analytics | Unified analytics + ML |
# MAGIC | **Updates/Deletes** | Difficult | Easy | Easy (MERGE, DELETE) |
# MAGIC | **Versioning** | ❌ No | ❌ No | ✅ Yes (Time Travel) |
# MAGIC | **Governance** | Limited | Strong | Strong (Unity Catalog) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Key Concepts:
# MAGIC
# MAGIC **Storage vs Compute Separation**:
# MAGIC * Data Lake: Storage and compute are separate (flexibility)
# MAGIC * Data Warehouse: Often tightly coupled (limited scalability)
# MAGIC * Lakehouse: Decoupled storage + compute (best of both)
# MAGIC
# MAGIC **Schema Approach**:
# MAGIC * **Schema-on-read** (Lake): Define structure when reading data
# MAGIC * **Schema-on-write** (Warehouse): Define structure before writing data
# MAGIC * **Lakehouse**: Supports both + schema evolution
# MAGIC
# MAGIC **Why Lakehouse Wins**:
# MAGIC * Combines flexibility of lakes with reliability of warehouses
# MAGIC * Single source of truth for BI and ML
# MAGIC * ACID guarantees for data quality
# MAGIC * Cost-effective at scale
# MAGIC * Open formats (Delta, Iceberg)
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Demo: Data Lake Style - Raw File Storage
# ============================================
# DATA LAKE APPROACH: Store raw data files
# ============================================

print("🌊 Demonstrating Data Lake Pattern (Raw Files)\n")

# Create sample raw data (like data from API/logs)
raw_events = [
    {"event_id": "evt_001", "user_id": 101, "action": "login", "timestamp": "2026-04-21T10:30:00", "device": "mobile"},
    {"event_id": "evt_002", "user_id": 102, "action": "view_page", "timestamp": "2026-04-21T10:31:15", "device": "desktop"},
    {"event_id": "evt_003", "user_id": 101, "action": "add_to_cart", "timestamp": "2026-04-21T10:32:30", "device": "mobile", "product_id": "P123"},
    {"event_id": "evt_004", "user_id": 103, "action": "login", "timestamp": "2026-04-21T10:33:00", "device": "tablet"},
    {"event_id": "evt_005", "user_id": 102, "action": "purchase", "timestamp": "2026-04-21T10:35:00", "device": "desktop", "amount": 99.99},
]

raw_df = spark.createDataFrame(raw_events)

print("✅ Raw Event Data (Lake Style - flexible schema):")
display(raw_df)

print("\n⚠️ Notice:")
print("  • Inconsistent schema (some records have 'product_id', some have 'amount')")
print("  • String timestamps (not typed)")
print("  • No validation or constraints")
print("  • Schema discovered on read (schema-on-read)")
print("\n📝 This is typical Data Lake pattern - store first, structure later")

# COMMAND ----------

# DBTITLE 1,Demo: Lakehouse Style - Delta Lake with ACID
# ============================================
# LAKEHOUSE APPROACH: Delta Lake with schema enforcement
# ============================================

print("🏘️ Demonstrating Lakehouse Pattern (Delta Lake)\n")

# Transform raw data into structured format with proper typing
from pyspark.sql.functions import col, to_timestamp

# Clean and type the data
structured_events = (
    raw_df
    .withColumn("timestamp", to_timestamp(col("timestamp")))
    .withColumn("user_id", col("user_id").cast("integer"))
    .select(
        "event_id",
        "user_id",
        "action",
        "timestamp",
        "device"
    )
)

print("✅ Structured Event Data (Lakehouse Style - enforced schema):")
display(structured_events)

print("\n✨ Lakehouse Benefits:")
print("  • Consistent schema enforcement")
print("  • Proper data types (timestamp is DateTimeType)")
print("  • ACID transactions (can safely update/delete)")
print("  • Schema evolution support")
print("  • Time travel and versioning")
print("  • Optimized for analytics (like warehouse)")
print("  • Flexible storage (like lake)")

print("\n🎯 Result: Best of both worlds - Delta Lake = Lakehouse!")

# COMMAND ----------

# DBTITLE 1,🥇 Section 3: Medallion Architecture
# MAGIC %md
# MAGIC ## 🥇 SECTION 3: Medallion Architecture (Bronze → Silver → Gold)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC
# MAGIC Think of a **jewelry making process**:
# MAGIC * **🥉 Bronze** = Raw ore (just mined, unprocessed)
# MAGIC * **🥈 Silver** = Refined metal (cleaned, shaped)
# MAGIC * **🥇 Gold** = Final jewelry (polished, ready to wear)
# MAGIC
# MAGIC Data goes through the same refinement process!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC Medallion Architecture is a **data design pattern** for organizing data in a Lakehouse:
# MAGIC
# MAGIC #### 🥉 Bronze Layer (Raw/Landing):
# MAGIC * **Purpose**: Ingestion and historical archive
# MAGIC * **Characteristics**:
# MAGIC   * Exact copy of source data
# MAGIC   * No transformations (preserve original)
# MAGIC   * Append-only (immutable)
# MAGIC   * Includes metadata (source, load time)
# MAGIC * **Format**: Delta (for ACID guarantees)
# MAGIC * **Schema**: Schema-on-read or flexible
# MAGIC
# MAGIC #### 🥈 Silver Layer (Cleaned/Conformed):
# MAGIC * **Purpose**: Cleaned, validated, deduplicated
# MAGIC * **Characteristics**:
# MAGIC   * Data quality rules applied
# MAGIC   * Standardized formats
# MAGIC   * Type casting and validation
# MAGIC   * Business logic applied
# MAGIC   * Deduplication
# MAGIC * **Format**: Delta with constraints
# MAGIC * **Schema**: Enforced and validated
# MAGIC
# MAGIC #### 🥇 Gold Layer (Business/Aggregated):
# MAGIC * **Purpose**: Business-ready, aggregated metrics
# MAGIC * **Characteristics**:
# MAGIC   * Aggregated views
# MAGIC   * Denormalized for performance
# MAGIC   * Optimized for specific use cases
# MAGIC   * Ready for BI/ML
# MAGIC * **Format**: Delta with Z-ordering
# MAGIC * **Schema**: Star/Snowflake schema
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Data Flow:
# MAGIC
# MAGIC ```
# MAGIC Source System
# MAGIC      |
# MAGIC      v
# MAGIC 🥉 BRONZE (raw_orders)
# MAGIC   * Ingest as-is
# MAGIC   * Add metadata
# MAGIC      |
# MAGIC      v
# MAGIC 🥈 SILVER (cleaned_orders)
# MAGIC   * Data quality checks
# MAGIC   * Type casting
# MAGIC   * Deduplication
# MAGIC   * Standardization
# MAGIC      |
# MAGIC      v
# MAGIC 🥇 GOLD (order_metrics)
# MAGIC   * Business aggregations
# MAGIC   * KPI calculations
# MAGIC   * Ready for dashboards
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✨ Benefits:
# MAGIC * **Separation of concerns**: Each layer has clear purpose
# MAGIC * **Data lineage**: Track data transformations
# MAGIC * **Incremental processing**: Process only new data
# MAGIC * **Reusability**: Silver layer serves multiple gold tables
# MAGIC * **Quality control**: Catch issues early
# MAGIC * **Recovery**: Can rebuild downstream from upstream
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Demo: Bronze Layer - Raw Ingestion
# ============================================
# MEDALLION DEMO: Bronze Layer
# ============================================

from pyspark.sql.functions import current_timestamp, lit

print("🥉 BRONZE LAYER: Raw Data Ingestion\n")

# Simulate raw order data from source system (e.g., API, database, files)
raw_order_data = [
    {"order_id": "ORD001", "customer_id": "101", "product": "Laptop", "quantity": "2", "price": "1200.00", "order_date": "2026-04-15"},
    {"order_id": "ORD002", "customer_id": "102", "product": "Mouse", "quantity": "5", "price": "25.50", "order_date": "2026-04-16"},
    {"order_id": "ORD003", "customer_id": "101", "product": "Keyboard", "quantity": "1", "price": "75", "order_date": "2026-04-16"},
    {"order_id": "ORD004", "customer_id": "103", "product": "Monitor", "quantity": "1", "price": "350.00", "order_date": "2026-04-17"},
    {"order_id": "ORD005", "customer_id": "", "product": "Webcam", "quantity": "3", "price": "invalid", "order_date": "2026-04-18"},  # Bad data
    {"order_id": "ORD003", "customer_id": "101", "product": "Keyboard", "quantity": "1", "price": "75", "order_date": "2026-04-16"},  # Duplicate
]

# Create raw DataFrame
raw_orders = spark.createDataFrame(raw_order_data)

# Add Bronze layer metadata
bronze_orders = (
    raw_orders
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_system", lit("ecommerce_api"))
)

print("✅ Bronze Layer Data (Raw + Metadata):")
display(bronze_orders)

print("\n📌 Bronze Layer Characteristics:")
print("  • Data stored AS-IS from source")
print("  • All fields are strings (no type casting yet)")
print("  • Contains bad data (empty customer_id, invalid price)")
print("  • Contains duplicates (ORD003 appears twice)")
print("  • Added ingestion metadata for lineage")
print("\n✨ Bronze = Historical archive + audit trail")

# COMMAND ----------

# DBTITLE 1,Demo: Silver Layer - Cleaned and Validated
# ============================================
# MEDALLION DEMO: Silver Layer
# ============================================

from pyspark.sql.functions import col, to_date, trim

print("🥈 SILVER LAYER: Data Cleaning and Validation\n")

# Transform Bronze to Silver with data quality rules
silver_orders = (
    bronze_orders
    
    # Remove duplicates (keep first occurrence)
    .dropDuplicates(["order_id"])
    
    # Filter out records with missing critical fields
    .filter(
        (col("customer_id") != "") & 
        (col("customer_id").isNotNull())
    )
    
    # Type casting and standardization
    .withColumn("customer_id", col("customer_id").cast("integer"))
    .withColumn("quantity", col("quantity").cast("integer"))
    .withColumn("order_date", to_date(col("order_date")))
    
    # Handle price - use try_cast to handle invalid values gracefully
    .withColumn("price", F.expr("try_cast(price as double)"))
    .filter(col("price").isNotNull())
    
    # Trim whitespace from strings
    .withColumn("product", trim(col("product")))
    
    # Calculate total amount
    .withColumn("total_amount", col("quantity") * col("price"))
    
    # Select final columns
    .select(
        "order_id",
        "customer_id",
        "product",
        "quantity",
        "price",
        "total_amount",
        "order_date",
        "ingestion_timestamp",
        "source_system"
    )
)

print("✅ Silver Layer Data (Cleaned and Validated):")
display(silver_orders)

print("\n📌 Silver Layer Transformations Applied:")
print("  • Removed duplicates (ORD003 deduplicated)")
print("  • Filtered out bad data (ORD005 with empty customer_id and invalid price)")
print("  • Type casting (strings → proper types)")
print("  • Calculated derived field (total_amount)")
print("  • Data quality enforced")
print(f"\n📊 Records: Bronze = {bronze_orders.count()}, Silver = {silver_orders.count()} (quality filter applied)")

# COMMAND ----------

# DBTITLE 1,Demo: Gold Layer - Business Aggregations
# ============================================
# MEDALLION DEMO: Gold Layer
# ============================================

from pyspark.sql.functions import sum as _sum, avg, count, max as _max

print("🥇 GOLD LAYER: Business Metrics and Aggregations\n")

# Create Gold table 1: Daily Order Metrics
gold_daily_metrics = (
    silver_orders
    .groupBy("order_date")
    .agg(
        count("order_id").alias("total_orders"),
        _sum("total_amount").alias("daily_revenue"),
        avg("total_amount").alias("avg_order_value"),
        _sum("quantity").alias("total_items_sold")
    )
    .orderBy("order_date")
)

print("✅ Gold Layer 1 - Daily Order Metrics:")
display(gold_daily_metrics)

# Create Gold table 2: Product Performance
gold_product_metrics = (
    silver_orders
    .groupBy("product")
    .agg(
        count("order_id").alias("orders_count"),
        _sum("quantity").alias("units_sold"),
        _sum("total_amount").alias("total_revenue"),
        avg("price").alias("avg_price")
    )
    .orderBy(F.desc("total_revenue"))
)

print("\n✅ Gold Layer 2 - Product Performance:")
display(gold_product_metrics)

print("\n📌 Gold Layer Characteristics:")
print("  • Aggregated metrics (ready for dashboards)")
print("  • Business KPIs calculated")
print("  • Optimized for specific analytics use cases")
print("  • Denormalized for query performance")
print("\n✨ Gold = Business-ready data for BI and reporting!")

print("\n" + "="*60)
print("🏆 MEDALLION ARCHITECTURE COMPLETE")
print("="*60)
print("🥉 Bronze: 6 raw records ingested")
print("🥈 Silver: 4 cleaned records (2 rejected for quality)")
print("🥇 Gold: 2 business metric tables created")
print("="*60)

# COMMAND ----------

# DBTITLE 1,🗄️ Section 4: File Formats and Storage
# MAGIC %md
# MAGIC ## 🗄️ SECTION 4: File Formats & Storage Concepts
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## PART A: Row vs Column Storage
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC
# MAGIC Imagine a spreadsheet with students' data:
# MAGIC
# MAGIC **Row Storage** (like CSV):
# MAGIC ```
# MAGIC Alice,25,NYC,Engineer | Bob,30,LA,Doctor | Charlie,28,SF,Teacher
# MAGIC ```
# MAGIC Read one student at a time (good for transactions)
# MAGIC
# MAGIC **Column Storage** (like Parquet):
# MAGIC ```
# MAGIC Names: Alice,Bob,Charlie
# MAGIC Ages: 25,30,28
# MAGIC Cities: NYC,LA,SF
# MAGIC Jobs: Engineer,Doctor,Teacher
# MAGIC ```
# MAGIC Read one column at a time (good for analytics)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC #### Row-Based Storage:
# MAGIC
# MAGIC **Structure**: Stores complete records together
# MAGIC
# MAGIC **Best For**:
# MAGIC * OLTP (Online Transaction Processing)
# MAGIC * Frequent INSERT/UPDATE/DELETE
# MAGIC * Retrieving entire records
# MAGIC * Low latency point queries
# MAGIC
# MAGIC **Examples**: 
# MAGIC * Traditional databases (MySQL, PostgreSQL)
# MAGIC * CSV files
# MAGIC * JSON files
# MAGIC
# MAGIC **Performance**:
# MAGIC * Fast for row retrieval: `SELECT * FROM users WHERE id = 123`
# MAGIC * Slow for column analytics: `SELECT AVG(age) FROM users`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Column-Based Storage:
# MAGIC
# MAGIC **Structure**: Stores each column separately
# MAGIC
# MAGIC **Best For**:
# MAGIC * OLAP (Online Analytical Processing)
# MAGIC * Aggregations (SUM, AVG, COUNT)
# MAGIC * Scanning subset of columns
# MAGIC * Compression (similar data types together)
# MAGIC
# MAGIC **Examples**:
# MAGIC * Parquet
# MAGIC * ORC
# MAGIC * Delta Lake (built on Parquet)
# MAGIC
# MAGIC **Performance**:
# MAGIC * Slow for full row retrieval
# MAGIC * Fast for column analytics: `SELECT AVG(age) FROM users`
# MAGIC * Better compression (10x+ smaller)
# MAGIC * I/O optimized (read only needed columns)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Performance Impact:
# MAGIC
# MAGIC | Operation | Row Storage | Column Storage |
# MAGIC |-----------|-------------|----------------|
# MAGIC | Read single record | ✅ Fast | ❌ Slow |
# MAGIC | Aggregate column | ❌ Slow | ✅ Fast |
# MAGIC | Insert record | ✅ Fast | ❌ Slow |
# MAGIC | Storage size | ❌ Large | ✅ Small (compressed) |
# MAGIC | Analytics queries | ❌ Slow | ✅ Fast |
# MAGIC
# MAGIC **🎯 Rule of Thumb**:
# MAGIC * Row storage → OLTP / transactional workloads
# MAGIC * Column storage → OLAP / analytical workloads
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## PART B: File Format Comparison
# MAGIC
# MAGIC ### 📋 Comparison Table:
# MAGIC
# MAGIC | Feature | Parquet | Delta Lake | ORC |
# MAGIC |---------|---------|------------|-----|
# MAGIC | **Storage Type** | Columnar | Columnar (Parquet + metadata) | Columnar |
# MAGIC | **Compression** | ✅ Excellent | ✅ Excellent | ✅ Excellent |
# MAGIC | **Schema Evolution** | ⚠️ Limited | ✅ Full support | ⚠️ Limited |
# MAGIC | **ACID Transactions** | ❌ No | ✅ Yes | ❌ No |
# MAGIC | **Time Travel** | ❌ No | ✅ Yes | ❌ No |
# MAGIC | **UPDATE/DELETE** | ❌ No | ✅ Yes | ❌ No |
# MAGIC | **MERGE (Upsert)** | ❌ No | ✅ Yes | ❌ No |
# MAGIC | **Change Data Feed** | ❌ No | ✅ Yes | ❌ No |
# MAGIC | **Predicate Pushdown** | ✅ Yes | ✅ Yes (enhanced) | ✅ Yes |
# MAGIC | **Statistics** | Basic | Advanced (data skipping) | Basic |
# MAGIC | **Ecosystem** | Universal | Databricks, Apache Spark | Hive, Hadoop |
# MAGIC | **Performance** | Fast | Fastest (optimized) | Fast |
# MAGIC | **Governance** | ❌ No | ✅ Unity Catalog | ❌ No |
# MAGIC | **Streaming** | ⚠️ Limited | ✅ Native support | ❌ No |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Format Details:
# MAGIC
# MAGIC #### 1️⃣ Parquet
# MAGIC * **Type**: Open-source columnar format
# MAGIC * **Use Case**: Static datasets, batch analytics
# MAGIC * **Pros**: Universal support, good compression
# MAGIC * **Cons**: Immutable (no updates), no ACID
# MAGIC
# MAGIC #### 2️⃣ Delta Lake
# MAGIC * **Type**: Open-source (Linux Foundation) storage layer on Parquet
# MAGIC * **Use Case**: Lakehouse, ACID transactions, streaming+batch
# MAGIC * **Pros**: ACID, versioning, schema evolution, DML operations
# MAGIC * **Cons**: Requires Delta protocol support
# MAGIC * **🎯 Recommended for Databricks**
# MAGIC
# MAGIC #### 3️⃣ ORC (Optimized Row Columnar)
# MAGIC * **Type**: Columnar format from Hadoop ecosystem
# MAGIC * **Use Case**: Hive, legacy Hadoop workloads
# MAGIC * **Pros**: Good compression, Hive optimized
# MAGIC * **Cons**: Less common in modern stacks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✨ Why Delta Lake Wins:
# MAGIC
# MAGIC ```
# MAGIC Parquet Limitations:
# MAGIC   ❌ Cannot update or delete rows
# MAGIC   ❌ No ACID guarantees
# MAGIC   ❌ Difficult schema changes
# MAGIC   ❌ No audit history
# MAGIC   
# MAGIC          ⤵️
# MAGIC          
# MAGIC Delta Lake = Parquet + Transaction Log
# MAGIC   ✅ Full DML support (UPDATE, DELETE, MERGE)
# MAGIC   ✅ ACID transactions
# MAGIC   ✅ Schema evolution and enforcement
# MAGIC   ✅ Time travel (audit trail)
# MAGIC   ✅ Change Data Feed
# MAGIC   ✅ Optimized performance
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Demo: Create Sample Dataset for Format Comparison
# ============================================
# Create sample dataset for format comparison
# ============================================

from pyspark.sql.functions import col, rand, expr
from pyspark.sql.types import *

print("🔧 Creating sample dataset for file format comparison...\n")

# Create a larger dataset to demonstrate storage characteristics
print("⚙️ Generating 10,000 records...")

sample_data = (
    spark.range(0, 10000)
    .withColumn("employee_id", col("id"))
    .withColumn("first_name", expr("concat('Employee_', id)"))
    .withColumn("department", expr("CASE WHEN id % 5 = 0 THEN 'Engineering' "
                                   "WHEN id % 5 = 1 THEN 'Sales' "
                                   "WHEN id % 5 = 2 THEN 'Marketing' "
                                   "WHEN id % 5 = 3 THEN 'HR' "
                                   "ELSE 'Finance' END"))
    .withColumn("salary", (50000 + (rand() * 100000)).cast("decimal(10,2)"))
    .withColumn("hire_date", expr("date_add('2020-01-01', cast(rand() * 1500 as int))"))
    .withColumn("is_active", expr("CASE WHEN rand() > 0.1 THEN true ELSE false END"))
    .drop("id")
)

print("✅ Sample dataset created!\n")
print("Schema:")
sample_data.printSchema()

print("\nSample data:")
display(sample_data.limit(10))

print(f"\n📊 Total Records: {sample_data.count():,}")

# COMMAND ----------

# DBTITLE 1,Demo: Column vs Row Storage - Query Performance
# ============================================
# Demonstrate Column Storage Advantage
# ============================================

print("📈 Demonstrating Column Storage Benefits\n")

# Analytical Query 1: Aggregate on one column
print("🔍 Query 1: Calculate average salary by department\n")

avg_salary_by_dept = (
    sample_data
    .groupBy("department")
    .agg(F.avg("salary").alias("avg_salary"))
    .orderBy("department")
)

display(avg_salary_by_dept)

print("\n✨ Column Storage Advantage:")
print("  • Only 'department' and 'salary' columns are read")
print("  • Other columns (first_name, hire_date, etc.) are SKIPPED")
print("  • Massive I/O savings for wide tables")
print("  • Better compression (similar values grouped)")

# Analytical Query 2: Filter + Aggregate
print("\n" + "="*60)
print("🔍 Query 2: Count active employees by department\n")

active_by_dept = (
    sample_data
    .filter(col("is_active") == True)
    .groupBy("department")
    .count()
    .orderBy(F.desc("count"))
)

display(active_by_dept)

print("\n✨ Columnar Format Benefits:")
print("  • Predicate pushdown: Filter applied early")
print("  • Only necessary columns scanned")
print("  • Compression: Similar boolean values compress well")
print("  • Statistics: Min/max values enable data skipping")

print("\n🏆 Result: Column storage (Parquet/Delta) is 10-100x faster for analytics!")

# COMMAND ----------

# DBTITLE 1,⚔️ Section 5: Parquet vs Delta Hands-On
# MAGIC %md
# MAGIC ## ⚔️ SECTION 5: Hands-On File Format Comparison
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC In this section, we'll:
# MAGIC 1. Write the same data in **Parquet** and **Delta** formats
# MAGIC 2. Compare their capabilities
# MAGIC 3. Demonstrate Delta's advantages (UPDATE, DELETE, MERGE)
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Demo: Write Data in Parquet Format
# ============================================
# Write data in PARQUET format
# ============================================

print("📋 Writing data in PARQUET format...\n")

# Create a simple dataset
employee_data = [
    (1, "Alice", "Engineering", 120000, True),
    (2, "Bob", "Sales", 90000, True),
    (3, "Charlie", "Engineering", 115000, True),
    (4, "Diana", "Marketing", 95000, False),
    (5, "Eve", "HR", 85000, True),
]

schema = ["emp_id", "name", "department", "salary", "is_active"]
employees_df = spark.createDataFrame(employee_data, schema)

print("✅ Original Employee Data:")
display(employees_df)

# Note: In Unity Catalog, managed tables must be Delta format
# For this demo, we'll keep Parquet data in-memory and create a temp view
employees_df.createOrReplaceTempView("employees_parquet")

print("\n✅ Data available as temp view (Parquet concept)!")
print("View: employees_parquet")
print("\n📌 Note: Unity Catalog requires Delta for managed tables.")
print("This view demonstrates Parquet's limitations compared to Delta.")

# COMMAND ----------

# DBTITLE 1,Demo: Write Data in Delta Format
# ============================================
# Write data in DELTA format
# ============================================

print("🔺 Writing data in DELTA format...\n")

# Same dataset, saved as Delta
employees_df.write.mode("overwrite").format("delta").saveAsTable("employees_delta")

print("✅ Data written to Delta format!")
print("Table: employees_delta")

print("\n📊 Both tables contain identical data at this point.")

# COMMAND ----------

# DBTITLE 1,Demo: Try UPDATE on Parquet (Will Fail)
# ============================================
# Attempt UPDATE on Parquet table
# ============================================

print("⚠️ Attempting UPDATE on Parquet table...\n")

try:
    # Try to give Bob a raise
    spark.sql("""
        UPDATE employees_parquet
        SET salary = 95000
        WHERE name = 'Bob'
    """)
    print("✅ Update succeeded")
except Exception as e:
    print("❌ UPDATE FAILED on Parquet!")
    print(f"Error: {str(e)[:200]}...")
    print("\n📌 Reason: Parquet is IMMUTABLE - no UPDATE/DELETE support")
    print("You would need to:")
    print("  1. Read entire table")
    print("  2. Apply transformation in DataFrame")
    print("  3. Overwrite entire table")
    print("  4. No transactional guarantees during this process")

# COMMAND ----------

# DBTITLE 1,Demo: UPDATE on Delta (Will Succeed)
# ============================================
# UPDATE on Delta table
# ============================================

print("✨ Performing UPDATE on Delta table...\n")

# Give Bob a raise
spark.sql("""
    UPDATE employees_delta
    SET salary = 95000
    WHERE name = 'Bob'
""")

print("✅ UPDATE succeeded on Delta!\n")

# Verify the change
print("Updated Delta table:")
display(spark.table("employees_delta"))

print("\n✨ Delta Lake Benefits:")
print("  • ACID transactions (atomic update)")
print("  • No need to rewrite entire table")
print("  • Concurrent reads continue during update")
print("  • Transactional guarantees")

# COMMAND ----------

# DBTITLE 1,Demo: DELETE on Delta
# ============================================
# DELETE on Delta table
# ============================================

print("✨ Performing DELETE on Delta table...\n")

# Remove inactive employee
spark.sql("""
    DELETE FROM employees_delta
    WHERE is_active = false
""")

print("✅ DELETE succeeded on Delta!\n")

print("Updated Delta table (Diana removed):")
display(spark.table("employees_delta"))

print("\n📉 Record count:") 
print(f"  Before DELETE: 5 employees")
print(f"  After DELETE: {spark.table('employees_delta').count()} employees")
print("\n⚠️ Same DELETE would FAIL on Parquet!")

# COMMAND ----------

# DBTITLE 1,Demo: MERGE (Upsert) on Delta
# ============================================
# MERGE (Upsert) on Delta table
# ============================================

print("✨ Performing MERGE (Upsert) on Delta table...\n")

# New data: update existing employee + insert new employee
new_data = [
    (3, "Charlie", "Engineering", 125000, True),  # Existing - UPDATE
    (6, "Frank", "Finance", 100000, True),        # New - INSERT
]

new_employees = spark.createDataFrame(new_data, schema)

print("📄 New/Updated Data:")
display(new_employees)

# Create temp view for merge
new_employees.createOrReplaceTempView("new_employees")

# Perform MERGE
print("\n⏩ Executing MERGE...\n")

spark.sql("""
    MERGE INTO employees_delta AS target
    USING new_employees AS source
    ON target.emp_id = source.emp_id
    WHEN MATCHED THEN
        UPDATE SET 
            target.salary = source.salary,
            target.department = source.department,
            target.is_active = source.is_active
    WHEN NOT MATCHED THEN
        INSERT (emp_id, name, department, salary, is_active)
        VALUES (source.emp_id, source.name, source.department, source.salary, source.is_active)
""")

print("✅ MERGE completed!\n")

print("Final Delta table:")
display(spark.table("employees_delta").orderBy("emp_id"))

print("\n✨ MERGE Operation Results:")
print("  • Charlie's salary updated: 115000 → 125000")
print("  • Frank inserted as new employee")
print("  • All in ONE atomic transaction")
print("\n⚠️ MERGE is NOT possible with Parquet!")

# COMMAND ----------

# DBTITLE 1,Demo: Delta Time Travel
# ============================================
# Delta Time Travel (Version History)
# ============================================

print("⏳ Delta Lake Time Travel Demo\n")

# Show version history
print("📜 Delta Table History:")
history_df = spark.sql("DESCRIBE HISTORY employees_delta")
display(history_df.select("version", "timestamp", "operation", "operationMetrics"))

print("\n🔍 Reading previous version (before all changes)...\n")

# Read version 0 (original data)
original_data = spark.read.format("delta").option("versionAsOf", 0).table("employees_delta")

print("✅ Original Data (Version 0):")
display(original_data)

print("\n✨ Time Travel Benefits:")
print("  • Audit trail of all changes")
print("  • Rollback to any previous version")
print("  • Compare data across versions")
print("  • Reproduce historical reports")
print("  • Disaster recovery")
print("\n⚠️ Parquet has NO version history!")

print("\n" + "="*60)
print("🏆 DELTA LAKE ADVANTAGES DEMONSTRATED")
print("="*60)
print("✅ UPDATE support (Parquet: ❌)")
print("✅ DELETE support (Parquet: ❌)")
print("✅ MERGE/Upsert support (Parquet: ❌)")
print("✅ Time Travel (Parquet: ❌)")
print("✅ ACID transactions (Parquet: ❌)")
print("="*60)

# COMMAND ----------

# DBTITLE 1,📍 Section 6: Metadata Best Practices
# MAGIC %md
# MAGIC ## 📍 SECTION 6: Metadata Usage (MANDATORY BEST PRACTICE)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❗ Important for Serverless Compute:
# MAGIC
# MAGIC When working with Databricks Serverless, always use:
# MAGIC * ✅ **`_metadata.file_path`** → Correct approach
# MAGIC * ❌ **`input_file_name()`** → Legacy function (avoid)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Use Cases for Metadata:
# MAGIC
# MAGIC 1. **Data Lineage**: Track which source file produced each record
# MAGIC 2. **Incremental Processing**: Identify which files have been processed
# MAGIC 3. **Auditing**: Know data origin for compliance
# MAGIC 4. **Debugging**: Trace data quality issues to source
# MAGIC 5. **Partition Discovery**: Extract date/time from file paths
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔧 Available Metadata Columns:
# MAGIC
# MAGIC When reading files, Spark provides:
# MAGIC * `_metadata.file_path` → Full path to source file
# MAGIC * `_metadata.file_name` → Just the filename
# MAGIC * `_metadata.file_size` → File size in bytes
# MAGIC * `_metadata.file_modification_time` → Last modified timestamp
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Demo: Using _metadata.file_path
# ============================================
# Demonstrate metadata usage with _metadata.file_path
# ============================================

from pyspark.sql.functions import col, current_timestamp, lit

print("📍 Demonstrating Metadata Usage\n")

# Create sample data representing multiple source files
print("⏩ Step 1: Create sample datasets (simulating different source files)...\n")

file1_data = [
    ("A001", "Product Alpha", 100),
    ("A002", "Product Beta", 200),
]

file2_data = [
    ("B001", "Product Gamma", 150),
    ("B002", "Product Delta", 250),
]

schema = ["product_id", "product_name", "quantity"]

df1 = spark.createDataFrame(file1_data, schema)
df2 = spark.createDataFrame(file2_data, schema)

print("✅ Sample files created!\n")

# Read back and add metadata (simulated approach for Unity Catalog)
print("⏩ Step 2: Read data with metadata tracking...\n")

# Add simulated source file information
source1 = df1.withColumn("source_file", lit("warehouse_file_1.parquet"))
source2 = df2.withColumn("source_file", lit("warehouse_file_2.parquet"))

# Combine with source tracking
all_data = source1.union(source2).withColumn("ingestion_time", current_timestamp())

print("✅ Data with Metadata:")
display(all_data)

print("\n✨ Metadata Benefits:")
print("  • Track which source file each record came from")
print("  • Enable incremental processing (process only new files)")
print("  • Audit trail for compliance")
print("  • Debug data quality issues by source")

# COMMAND ----------

# DBTITLE 1,Demo: Extract Information from File Paths
# ============================================
# Extract date/partition info from file paths
# ============================================

from pyspark.sql.functions import regexp_extract, split

print("🔍 Extracting Information from File Paths\n")

# Simulate data with realistic file path patterns
data_with_paths = [
    (1, "Record 1", "s3://bucket/data/year=2026/month=04/day=20/file1.parquet"),
    (2, "Record 2", "s3://bucket/data/year=2026/month=04/day=20/file2.parquet"),
    (3, "Record 3", "s3://bucket/data/year=2026/month=04/day=21/file1.parquet"),
]

df_paths = spark.createDataFrame(data_with_paths, ["id", "data", "file_path"])

print("✅ Original data with file paths:")
display(df_paths)

# Extract partition information from paths
df_extracted = (
    df_paths
    .withColumn("year", regexp_extract(col("file_path"), r"year=(\d+)", 1))
    .withColumn("month", regexp_extract(col("file_path"), r"month=(\d+)", 1))
    .withColumn("day", regexp_extract(col("file_path"), r"day=(\d+)", 1))
    .withColumn("filename", regexp_extract(col("file_path"), r"/([^/]+\.parquet)$", 1))
)

print("\n✅ Data with extracted partition info:")
display(df_extracted)

print("\n✨ Use Cases:")
print("  • Partition discovery from file paths")
print("  • Time-based filtering without reading file content")
print("  • Incremental processing (process only new dates)")
print("  • Data organization and cataloging")

print("\n🎯 Best Practice: Always include source metadata in Bronze layer!")

# COMMAND ----------

# DBTITLE 1,🗂️ Section 7: Unity Catalog Volumes
# MAGIC %md
# MAGIC ## 🗂️ SECTION 7: Unity Catalog Volume Usage (MANDATORY)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC
# MAGIC **Unity Catalog Volumes** = Organized storage lockers with security guards
# MAGIC * Each locker (volume) belongs to a building (schema) in a campus (catalog)
# MAGIC * Security guards control who can access what
# MAGIC * Everything is tracked and audited
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Unity Catalog Volumes** provide governed access to non-tabular data (files):
# MAGIC
# MAGIC **Key Features**:
# MAGIC * **Governance**: ACLs, audit logs, data lineage
# MAGIC * **Organization**: 3-level namespace (catalog.schema.volume)
# MAGIC * **Flexibility**: Store any file type (CSV, JSON, Parquet, images, models)
# MAGIC * **Security**: Fine-grained access control
# MAGIC * **Portability**: Cloud-agnostic paths
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📍 Path Structure:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/<catalog>/<schema>/<volume>/<path>
# MAGIC
# MAGIC  Example:
# MAGIC  /Volumes/main/raw_data/landing_zone/2026/04/21/data.parquet
# MAGIC          └────  └───────  └────────────  └──────────────────
# MAGIC        catalog  schema      volume           file path
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ CRITICAL RULES:
# MAGIC
# MAGIC * ✅ **USE**: `/Volumes/<catalog>/<schema>/<volume>/`
# MAGIC * ❌ **AVOID**: `/tmp/` (not persistent)
# MAGIC * ❌ **AVOID**: `/dbfs/` (legacy, not governed)
# MAGIC * ❌ **AVOID**: Local file paths
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Common Patterns:
# MAGIC
# MAGIC **Bronze Layer**:
# MAGIC ```
# MAGIC /Volumes/main/bronze/raw_events/
# MAGIC ```
# MAGIC
# MAGIC **Silver Layer**:
# MAGIC ```
# MAGIC /Volumes/main/silver/cleaned_events/
# MAGIC ```
# MAGIC
# MAGIC **Gold Layer**:
# MAGIC ```
# MAGIC /Volumes/main/gold/aggregated_metrics/
# MAGIC ```
# MAGIC
# MAGIC **Model Artifacts**:
# MAGIC ```
# MAGIC /Volumes/main/ml_models/production/model_v1.pkl
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔧 Operations:
# MAGIC
# MAGIC **Read from Volume**:
# MAGIC ```python
# MAGIC df = spark.read.format("parquet").load(
# MAGIC     "/Volumes/main/raw_data/landing_zone/data.parquet"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Write to Volume**:
# MAGIC ```python
# MAGIC df.write.format("delta").mode("overwrite").save(
# MAGIC     "/Volumes/main/bronze/events/"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **List Files in Volume**:
# MAGIC ```python
# MAGIC dbutils.fs.ls("/Volumes/main/raw_data/landing_zone/")
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Demo: Unity Catalog Volume Concepts
# ============================================
# Unity Catalog Volume Usage Demonstration
# ============================================

print("🗂️ Unity Catalog Volume Demonstration\n")

print("📌 Volume Path Structure:\n")
print("Standard Format:")
print("  /Volumes/<catalog>/<schema>/<volume>/<path>")
print("\nExample Paths:")
print("  • Bronze: /Volumes/main/bronze/raw_orders/2026/04/21/")
print("  • Silver: /Volumes/main/silver/cleaned_orders/")
print("  • Gold: /Volumes/main/gold/order_metrics/")
print("  • ML Models: /Volumes/main/ml_artifacts/models/")

print("\n" + "="*60)
print("✨ Benefits of Unity Catalog Volumes")
print("="*60)
print("✅ Governance: ACLs and audit logs")
print("✅ Organization: 3-level namespace")
print("✅ Security: Fine-grained access control")
print("✅ Portability: Cloud-agnostic paths")
print("✅ Integration: Seamless with Delta tables")
print("="*60)

print("\n⚠️ AVOID These Paths:")
print("❌ /tmp/ (not persistent, lost after cluster restart)")
print("❌ /dbfs/ (legacy, not Unity Catalog governed)")
print("❌ Local file paths (not shared across compute)")

print("\n🎯 Best Practice: Always use Unity Catalog Volumes for production workloads!")

# COMMAND ----------

# DBTITLE 1,Demo: Simulated Volume Usage Pattern
# ============================================
# Demonstrate Volume Usage Pattern
# ============================================

print("🛠️ Demonstrating Volume Usage Pattern\n")

# In a real scenario, you would:
# 1. Create volumes through Unity Catalog UI or SQL
# 2. Set appropriate permissions
# 3. Use the volumes in your pipelines

print("⏩ Typical Workflow:\n")

print("1️⃣ CREATE VOLUME (SQL):")
print("""
   CREATE VOLUME IF NOT EXISTS main.bronze.raw_data;
   CREATE VOLUME IF NOT EXISTS main.silver.processed_data;
   CREATE VOLUME IF NOT EXISTS main.gold.business_metrics;
""")

print("\n2️⃣ WRITE DATA TO VOLUME (PySpark):")
print("""
   # Write to Bronze
   raw_df.write.format("delta") \
       .mode("append") \
       .save("/Volumes/main/bronze/raw_data/events/")
""")

print("\n3️⃣ READ FROM VOLUME (PySpark):")
print("""
   # Read from Bronze
   bronze_df = spark.read.format("delta") \
       .load("/Volumes/main/bronze/raw_data/events/")
""")

print("\n4️⃣ PROCESS AND WRITE TO NEXT LAYER:")
print("""
   # Transform and write to Silver
   cleaned_df = bronze_df.dropDuplicates() \
       .filter(col("status").isNotNull())
   
   cleaned_df.write.format("delta") \
       .mode("overwrite") \
       .save("/Volumes/main/silver/processed_data/events/")
""")

print("\n" + "="*60)
print("✨ Volume Organization Best Practices")
print("="*60)
print("• Use separate volumes for Bronze/Silver/Gold layers")
print("• Organize by data domain within volumes")
print("• Use date partitioning for time-series data")
print("• Set appropriate ACLs per layer")
print("• Document volume purpose in metadata")
print("="*60)

# COMMAND ----------

# DBTITLE 1,🚀 Section 8: End-to-End Mini Pipeline
# MAGIC %md
# MAGIC ## 🚀 SECTION 8: End-to-End Mini Pipeline
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Objective:
# MAGIC
# MAGIC Build a complete data pipeline that demonstrates:
# MAGIC * ✅ Medallion Architecture (Bronze → Silver → Gold)
# MAGIC * ✅ Delta Lake format
# MAGIC * ✅ Metadata tracking
# MAGIC * ✅ Data quality enforcement
# MAGIC * ✅ Business aggregations
# MAGIC * ✅ Serverless compatible
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Pipeline Flow:
# MAGIC
# MAGIC ```
# MAGIC 📥 Source Data (Simulated)
# MAGIC       |
# MAGIC       v
# MAGIC 🥉 BRONZE: Raw ingestion
# MAGIC   * Ingest as-is
# MAGIC   * Add metadata (source, timestamp)
# MAGIC   * Store in Delta format
# MAGIC       |
# MAGIC       v
# MAGIC 🥈 SILVER: Cleansing & Validation
# MAGIC   * Remove duplicates
# MAGIC   * Filter invalid records
# MAGIC   * Type casting
# MAGIC   * Data quality checks
# MAGIC       |
# MAGIC       v
# MAGIC 🥇 GOLD: Business Metrics
# MAGIC   * Aggregate KPIs
# MAGIC   * Join dimensions
# MAGIC   * Create analytical views
# MAGIC       |
# MAGIC       v
# MAGIC 📊 Analytics & Reporting
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Pipeline Step 1: Generate Source Data
# ============================================
# END-TO-END PIPELINE: Step 1 - Generate Source Data
# ============================================

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

print("🚀 END-TO-END DATA PIPELINE\n")
print("="*60)
print("Step 1: Generate Source Data (Simulating Raw Data Source)")
print("="*60 + "\n")

# Simulate e-commerce transaction data
print("⏩ Generating sample e-commerce transactions...\n")

# Create realistic transaction data with some quality issues
transaction_data = [
    # Good records
    {"transaction_id": "TXN001", "customer_id": "C001", "product_id": "P101", "quantity": "3", "amount": "299.99", "transaction_date": "2026-04-15", "status": "completed"},
    {"transaction_id": "TXN002", "customer_id": "C002", "product_id": "P102", "quantity": "1", "amount": "49.99", "transaction_date": "2026-04-15", "status": "completed"},
    {"transaction_id": "TXN003", "customer_id": "C001", "product_id": "P103", "quantity": "2", "amount": "159.98", "transaction_date": "2026-04-16", "status": "completed"},
    {"transaction_id": "TXN004", "customer_id": "C003", "product_id": "P101", "quantity": "1", "amount": "99.99", "transaction_date": "2026-04-16", "status": "completed"},
    {"transaction_id": "TXN005", "customer_id": "C004", "product_id": "P104", "quantity": "5", "amount": "249.95", "transaction_date": "2026-04-17", "status": "completed"},
    
    # Data quality issues
    {"transaction_id": "TXN006", "customer_id": "", "product_id": "P102", "quantity": "2", "amount": "99.98", "transaction_date": "2026-04-17", "status": "completed"},  # Missing customer
    {"transaction_id": "TXN007", "customer_id": "C002", "product_id": "P105", "quantity": "-1", "amount": "79.99", "transaction_date": "2026-04-18", "status": "completed"},  # Invalid quantity
    {"transaction_id": "TXN008", "customer_id": "C005", "product_id": "P103", "quantity": "1", "amount": "invalid", "transaction_date": "2026-04-18", "status": "pending"},  # Invalid amount
    {"transaction_id": "TXN003", "customer_id": "C001", "product_id": "P103", "quantity": "2", "amount": "159.98", "transaction_date": "2026-04-16", "status": "completed"},  # Duplicate
    {"transaction_id": "TXN009", "customer_id": "C006", "product_id": "P106", "quantity": "3", "amount": "449.97", "transaction_date": "2026-04-19", "status": "completed"},
]

source_df = spark.createDataFrame(transaction_data)

print("✅ Raw Source Data (with quality issues):")
display(source_df)

print(f"\n📊 Total raw records: {source_df.count()}")
print("\n⚠️ Data quality issues present:")
print("  • Empty customer_id (TXN006)")
print("  • Negative quantity (TXN007)")
print("  • Invalid amount format (TXN008)")
print("  • Duplicate transaction (TXN003)")

# COMMAND ----------

# DBTITLE 1,Pipeline Step 2: Bronze Layer - Raw Ingestion
# ============================================
# END-TO-END PIPELINE: Step 2 - Bronze Layer
# ============================================

print("\n" + "="*60)
print("🥉 Step 2: BRONZE LAYER - Raw Data Ingestion")
print("="*60 + "\n")

# Add Bronze layer metadata
bronze_transactions = (
    source_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_system", lit("ecommerce_api"))
    .withColumn("data_source", lit("production_database"))
)

print("⏩ Writing to Bronze layer (Delta format)...\n")

# Write to Delta table
bronze_transactions.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_transactions")

print("✅ Bronze layer created!\n")
print("✅ Bronze Layer Data:")
display(spark.table("bronze_transactions"))

print("\n📌 Bronze Layer Characteristics:")
print("  • Format: Delta Lake")
print("  • Raw data preserved exactly as received")
print("  • Metadata added for lineage")
print("  • All quality issues still present (intentional)")
print(f"  • Record count: {spark.table('bronze_transactions').count()}")

# COMMAND ----------

# DBTITLE 1,Pipeline Step 3: Silver Layer - Cleaning & Validation
# ============================================
# END-TO-END PIPELINE: Step 3 - Silver Layer
# ============================================

print("\n" + "="*60)
print("🥈 Step 3: SILVER LAYER - Data Cleaning & Validation")
print("="*60 + "\n")

bronze_df = spark.table("bronze_transactions")

print("⏩ Applying data quality transformations...\n")

# Apply comprehensive data quality rules
silver_transactions = (
    bronze_df
    
    # 1. Remove exact duplicates
    .dropDuplicates(["transaction_id"])
    
    # 2. Filter out records with missing critical fields
    .filter(
        (col("transaction_id").isNotNull()) &
        (col("customer_id") != "") & 
        (col("customer_id").isNotNull()) &
        (col("product_id").isNotNull())
    )
    
    # 3. Type casting with validation
    .withColumn("quantity", col("quantity").cast("integer"))
    # Handle amount - use try_cast to handle invalid values gracefully
    .withColumn("amount", F.expr("try_cast(amount as decimal(10,2))"))
    .withColumn("transaction_date", to_date(col("transaction_date")))
    
    # 4. Filter out invalid values
    .filter(
        (col("quantity").isNotNull()) &
        (col("quantity") > 0) &  # No negative quantities
        (col("amount").isNotNull()) &
        (col("amount") > 0)  # No negative amounts
    )
    
    # 5. Filter only completed transactions
    .filter(col("status") == "completed")
    
    # 6. Add derived fields
    .withColumn("unit_price", (col("amount") / col("quantity")).cast("decimal(10,2)"))
    .withColumn("processing_timestamp", current_timestamp())
    
    # 7. Select final schema
    .select(
        "transaction_id",
        "customer_id",
        "product_id",
        "quantity",
        "amount",
        "unit_price",
        "transaction_date",
        "status",
        "source_system",
        "ingestion_timestamp",
        "processing_timestamp"
    )
)

print("⏩ Writing to Silver layer...\n")

silver_transactions.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_transactions")

print("✅ Silver layer created!\n")
print("✅ Silver Layer Data (Cleaned):")
display(spark.table("silver_transactions"))

print("\n📌 Silver Layer Transformations:")
print("  • Duplicates removed (TXN003)")
print("  • Invalid customer_id filtered (TXN006)")
print("  • Negative quantity filtered (TXN007)")
print("  • Invalid amount filtered (TXN008)")
print("  • Type casting applied")
print("  • Derived field added (unit_price)")
print(f"\n📊 Records: Bronze = {bronze_df.count()}, Silver = {spark.table('silver_transactions').count()}")
print(f"   → {bronze_df.count() - spark.table('silver_transactions').count()} records filtered for quality issues")

# COMMAND ----------

# DBTITLE 1,Pipeline Step 4: Gold Layer - Business Metrics
# ============================================
# END-TO-END PIPELINE: Step 4 - Gold Layer
# ============================================

print("\n" + "="*60)
print("🥇 Step 4: GOLD LAYER - Business Metrics & Aggregations")
print("="*60 + "\n")

silver_df = spark.table("silver_transactions")

# Gold Table 1: Daily Sales Metrics
print("⏩ Creating Gold Table 1: Daily Sales Metrics...\n")

gold_daily_sales = (
    silver_df
    .groupBy("transaction_date")
    .agg(
        count("transaction_id").alias("total_transactions"),
        sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_transaction_value"),
        sum("quantity").alias("total_items_sold"),
        countDistinct("customer_id").alias("unique_customers"),
        countDistinct("product_id").alias("unique_products")
    )
    .orderBy("transaction_date")
)

gold_daily_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_daily_sales")

print("✅ Gold Table 1: Daily Sales Metrics")
display(spark.table("gold_daily_sales"))

# Gold Table 2: Customer Metrics
print("\n⏩ Creating Gold Table 2: Customer Metrics...\n")

gold_customer_metrics = (
    silver_df
    .groupBy("customer_id")
    .agg(
        count("transaction_id").alias("total_orders"),
        sum("amount").alias("total_spent"),
        avg("amount").alias("avg_order_value"),
        sum("quantity").alias("total_items_purchased"),
        min("transaction_date").alias("first_purchase_date"),
        max("transaction_date").alias("last_purchase_date")
    )
    .orderBy(F.desc("total_spent"))
)

gold_customer_metrics.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_customer_metrics")

print("✅ Gold Table 2: Customer Metrics")
display(spark.table("gold_customer_metrics"))

# Gold Table 3: Product Performance
print("\n⏩ Creating Gold Table 3: Product Performance...\n")

gold_product_performance = (
    silver_df
    .groupBy("product_id")
    .agg(
        count("transaction_id").alias("times_sold"),
        sum("quantity").alias("units_sold"),
        sum("amount").alias("total_revenue"),
        avg("unit_price").alias("avg_unit_price")
    )
    .orderBy(F.desc("total_revenue"))
)

gold_product_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_product_performance")

print("✅ Gold Table 3: Product Performance")
display(spark.table("gold_product_performance"))

print("\n📌 Gold Layer Characteristics:")
print("  • Business-ready aggregations")
print("  • Optimized for analytics and reporting")
print("  • Multiple views for different use cases")
print("  • Ready for BI dashboards and ML features")

# COMMAND ----------

# DBTITLE 1,Pipeline Summary: Data Quality Report
# ============================================
# END-TO-END PIPELINE: Summary & Quality Report
# ============================================

print("\n" + "="*60)
print("🏆 PIPELINE EXECUTION SUMMARY")
print("="*60 + "\n")

# Count records at each layer
bronze_count = spark.table("bronze_transactions").count()
silver_count = spark.table("silver_transactions").count()

print("📊 RECORD FLOW:")
print(f"  📥 Source Data:     10 records")
print(f"  🥉 Bronze Layer:    {bronze_count} records")
print(f"  🥈 Silver Layer:    {silver_count} records")
print(f"  🥇 Gold Tables:     3 analytical views")

rejected = bronze_count - silver_count
print(f"\n  ❌ Rejected:        {rejected} records ({rejected/bronze_count*100:.1f}%)")

print("\n" + "-"*60)
print("🛡️ DATA QUALITY CHECKS APPLIED:")
print("-"*60)
print("✅ Deduplication")
print("✅ Missing field validation")
print("✅ Type casting and validation")
print("✅ Business rule enforcement (positive quantities/amounts)")
print("✅ Status filtering (completed only)")

print("\n" + "-"*60)
print("🎯 PIPELINE CHARACTERISTICS:")
print("-"*60)
print("✅ Medallion Architecture (Bronze → Silver → Gold)")
print("✅ Delta Lake format (ACID transactions)")
print("✅ Metadata tracking for lineage")
print("✅ Data quality enforcement")
print("✅ Serverless compatible")
print("✅ Production-ready design")

print("\n" + "="*60)
print("✨ END-TO-END PIPELINE COMPLETE!")
print("="*60)

print("\n📊 Tables Created:")
print("  • bronze_transactions (raw layer)")
print("  • silver_transactions (cleaned layer)")
print("  • gold_daily_sales (daily metrics)")
print("  • gold_customer_metrics (customer analytics)")
print("  • gold_product_performance (product analytics)")

print("\n🎯 Ready for: BI Dashboards, ML Feature Engineering, Analytics")

# COMMAND ----------

# DBTITLE 1,🎓 Final Summary and Interview Prep
# MAGIC %md
# MAGIC ## 🎓 FINAL SUMMARY & KEY LEARNINGS
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Key Concepts Covered:
# MAGIC
# MAGIC #### 1️⃣ Data Warehouse Fundamentals
# MAGIC * **Star Schema**: Fact tables (center) + Dimension tables (points)
# MAGIC * **Fact Tables**: Measurable metrics with foreign keys
# MAGIC * **Dimension Tables**: Descriptive attributes
# MAGIC * **Use Case**: OLAP, business analytics, BI reporting
# MAGIC
# MAGIC #### 2️⃣ Data Lake vs Warehouse vs Lakehouse
# MAGIC * **Data Lake**: Cheap storage, schema-on-read, all data types
# MAGIC * **Data Warehouse**: Structured, schema-on-write, expensive
# MAGIC * **Lakehouse**: Best of both - Delta Lake combines flexibility + reliability
# MAGIC
# MAGIC #### 3️⃣ Medallion Architecture
# MAGIC * **🥉 Bronze**: Raw ingestion, as-is from source
# MAGIC * **🥈 Silver**: Cleaned, validated, typed
# MAGIC * **🥇 Gold**: Aggregated, business-ready
# MAGIC * **Benefits**: Separation of concerns, lineage, reusability
# MAGIC
# MAGIC #### 4️⃣ Storage Concepts
# MAGIC * **Row Storage**: OLTP, transactional workloads, full record access
# MAGIC * **Column Storage**: OLAP, analytics, better compression
# MAGIC * **Rule**: Use columnar (Parquet/Delta) for analytics
# MAGIC
# MAGIC #### 5️⃣ File Formats
# MAGIC * **Parquet**: Columnar, immutable, universal
# MAGIC * **Delta Lake**: Parquet + ACID + versioning + DML
# MAGIC * **ORC**: Columnar, Hadoop ecosystem
# MAGIC * **Winner**: Delta Lake for Databricks Lakehouse
# MAGIC
# MAGIC #### 6️⃣ Best Practices
# MAGIC * Use Delta Lake for all layers
# MAGIC * Track metadata (`_metadata.file_path`)
# MAGIC * Use Unity Catalog Volumes (not /tmp or /dbfs)
# MAGIC * Implement data quality checks
# MAGIC * Design for serverless compute
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✨ Delta Lake Advantages:
# MAGIC
# MAGIC | Feature | Benefit |
# MAGIC |---------|--------|
# MAGIC | ACID Transactions | Data consistency and reliability |
# MAGIC | UPDATE/DELETE/MERGE | DML operations on data lake |
# MAGIC | Time Travel | Audit trail and rollback capability |
# MAGIC | Schema Evolution | Adapt to changing requirements |
# MAGIC | Data Skipping | Performance optimization |
# MAGIC | Change Data Feed | Track row-level changes |
# MAGIC | Unity Catalog Integration | Governance and security |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛠️ Production Readiness Checklist:
# MAGIC
# MAGIC ✅ Use Delta Lake format for all layers  
# MAGIC ✅ Implement Medallion Architecture  
# MAGIC ✅ Add metadata tracking  
# MAGIC ✅ Apply data quality checks  
# MAGIC ✅ Use Unity Catalog Volumes  
# MAGIC ✅ Design for serverless compute  
# MAGIC ✅ Document data lineage  
# MAGIC ✅ Set up monitoring and alerts  
# MAGIC ✅ Implement error handling  
# MAGIC ✅ Test with production-like data volumes  
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,🧠 Interview Questions
# MAGIC %md
# MAGIC ## 🧠 INTERVIEW QUESTIONS
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📚 Technical Interview Questions:
# MAGIC
# MAGIC #### **Question 1**: What is the difference between a Fact table and a Dimension table?
# MAGIC **Answer**: 
# MAGIC * Fact table contains measurable, quantitative data (metrics) and foreign keys to dimensions. It's large in rows, narrow in columns.
# MAGIC * Dimension table contains descriptive attributes for analysis. It's smaller in rows, wider in columns, and denormalized for performance.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Question 2**: Explain Medallion Architecture and its benefits.
# MAGIC **Answer**:
# MAGIC * Bronze Layer: Raw data ingestion, as-is from source
# MAGIC * Silver Layer: Cleaned, validated, typed data
# MAGIC * Gold Layer: Aggregated, business-ready metrics
# MAGIC * Benefits: Clear separation of concerns, data lineage, reusability, quality control, ability to rebuild downstream from upstream
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Question 3**: Why use columnar storage for analytics?
# MAGIC **Answer**:
# MAGIC * Columnar storage stores each column separately
# MAGIC * Benefits: (1) Read only needed columns (I/O optimization), (2) Better compression (similar data together), (3) Faster aggregations, (4) Predicate pushdown for filtering
# MAGIC * 10-100x faster for analytical queries compared to row storage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Question 4**: What are the advantages of Delta Lake over Parquet?
# MAGIC **Answer**:
# MAGIC Delta Lake provides:
# MAGIC * ACID transactions for data consistency
# MAGIC * UPDATE/DELETE/MERGE operations (DML support)
# MAGIC * Time travel and versioning
# MAGIC * Schema evolution and enforcement
# MAGIC * Data skipping with statistics
# MAGIC * Change Data Feed
# MAGIC * Better performance with optimization commands
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Question 5**: What is the difference between Data Lake and Lakehouse?
# MAGIC **Answer**:
# MAGIC * Data Lake: Just storage with files, no ACID, schema-on-read, difficult updates
# MAGIC * Lakehouse: Storage + transaction layer (Delta), ACID guarantees, supports both BI and ML, schema enforcement + evolution, DML operations
# MAGIC * Lakehouse = Data Lake + Data Warehouse capabilities
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Question 6**: How do you handle data quality in a data pipeline?
# MAGIC **Answer**:
# MAGIC * Implement quality checks in Silver layer
# MAGIC * Validate: null checks, type casting, range validation
# MAGIC * Deduplicate records
# MAGIC * Apply business rules
# MAGIC * Track rejected records for investigation
# MAGIC * Monitor quality metrics over time
# MAGIC * Use Delta Lake constraints for enforcement
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Question 7**: Why use Unity Catalog Volumes instead of /tmp or /dbfs?
# MAGIC **Answer**:
# MAGIC * /tmp: Not persistent, lost on cluster restart, no governance
# MAGIC * /dbfs: Legacy, not Unity Catalog governed
# MAGIC * Unity Catalog Volumes: Governed, ACLs, audit logs, persistent, portable, 3-level namespace
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Question 8**: What is schema-on-read vs schema-on-write?
# MAGIC **Answer**:
# MAGIC * Schema-on-write: Define schema before writing (Data Warehouse approach, enforced structure)
# MAGIC * Schema-on-read: Define schema when reading (Data Lake approach, flexible but risky)
# MAGIC * Lakehouse: Supports both + schema evolution and enforcement
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Question 9**: When would you use row storage vs column storage?
# MAGIC **Answer**:
# MAGIC * Row storage: OLTP systems, frequent INSERT/UPDATE/DELETE, need full record access
# MAGIC * Column storage: OLAP systems, analytics, aggregations, scanning subsets of columns
# MAGIC * Rule: Row for transactions, Column for analytics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Question 10**: How does Delta Lake achieve ACID transactions?
# MAGIC **Answer**:
# MAGIC Delta Lake uses:
# MAGIC * Transaction log (\_delta\_log directory) to track all changes
# MAGIC * Atomic commits via file operations
# MAGIC * Optimistic concurrency control
# MAGIC * Each operation recorded as JSON in transaction log
# MAGIC * Readers see consistent snapshot
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,⚠️ Common Mistakes to Avoid
# MAGIC %md
# MAGIC ## ⚠️ COMMON MISTAKES TO AVOID
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚫 Architecture Mistakes:
# MAGIC
# MAGIC #### 1️⃣ Using Row Format for Analytics
# MAGIC * **Mistake**: Storing analytical data in CSV or row-based format
# MAGIC * **Impact**: 10-100x slower queries, massive I/O waste
# MAGIC * **Solution**: Use columnar format (Delta Lake) for all analytical workloads
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2️⃣ Not Using Delta Lake
# MAGIC * **Mistake**: Using plain Parquet for data pipelines
# MAGIC * **Impact**: No ACID, can't UPDATE/DELETE, no time travel, data corruption risk
# MAGIC * **Solution**: Always use Delta Lake format in Databricks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 3️⃣ Ignoring Medallion Architecture
# MAGIC * **Mistake**: Mixing raw and processed data, no clear layers
# MAGIC * **Impact**: Hard to maintain, no lineage, can't recover from errors
# MAGIC * **Solution**: Implement Bronze/Silver/Gold pattern
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 4️⃣ Using /tmp or Local Paths
# MAGIC * **Mistake**: Writing data to /tmp/ or local file system
# MAGIC * **Impact**: Data loss on cluster restart, no governance, not shared
# MAGIC * **Solution**: Use Unity Catalog Volumes: `/Volumes/<catalog>/<schema>/<volume>/`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 5️⃣ No Metadata Tracking
# MAGIC * **Mistake**: Not tracking source files or ingestion time
# MAGIC * **Impact**: No lineage, hard to debug, can't identify data source
# MAGIC * **Solution**: Add metadata in Bronze layer (source, timestamp)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚫 Coding Mistakes:
# MAGIC
# MAGIC #### 6️⃣ Using input_file_name() Instead of _metadata.file_path
# MAGIC * **Mistake**: `input_file_name()` function (legacy)
# MAGIC * **Impact**: Not optimized for serverless compute
# MAGIC * **Solution**: Use `_metadata.file_path` column
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 7️⃣ Using RDDs
# MAGIC * **Mistake**: Working with RDDs instead of DataFrames
# MAGIC * **Impact**: Not optimized, no Catalyst optimizer, verbose code
# MAGIC * **Solution**: Always use DataFrame API
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 8️⃣ Overusing cache() / persist()
# MAGIC * **Mistake**: Caching everything
# MAGIC * **Impact**: Memory issues, not needed with Delta caching
# MAGIC * **Solution**: Avoid cache/persist in serverless, rely on Delta caching
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚫 Data Quality Mistakes:
# MAGIC
# MAGIC #### 9️⃣ No Data Quality Checks
# MAGIC * **Mistake**: Passing raw data directly to Gold layer
# MAGIC * **Impact**: Bad data in reports, incorrect analytics
# MAGIC * **Solution**: Implement quality checks in Silver layer
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 🔟 Not Handling Duplicates
# MAGIC * **Mistake**: Ignoring duplicate records
# MAGIC * **Impact**: Inflated metrics, incorrect aggregations
# MAGIC * **Solution**: Use `dropDuplicates()` or business logic for deduplication
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Key Takeaway:
# MAGIC
# MAGIC **Production-Ready = Delta Lake + Medallion + Unity Catalog + Quality Checks + Metadata**
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,🤖 Genie Code Agent Prompts
# MAGIC %md
# MAGIC ## 🤖 GENIE CODE AGENT: Example Prompts
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Use these prompts to accelerate your development:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 📊 Data Architecture:
# MAGIC
# MAGIC ```
# MAGIC Generate a Star Schema with fact_sales and dimensions
# MAGIC for customer, product, and date
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC Create a Medallion pipeline (Bronze/Silver/Gold) for
# MAGIC processing customer transaction data
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC Implement a data warehouse model for e-commerce
# MAGIC with proper fact and dimension tables
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 🔄 File Format Conversion:
# MAGIC
# MAGIC ```
# MAGIC Convert this Parquet table to Delta Lake format
# MAGIC with optimization
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC Read Parquet files from /Volumes/main/raw/ and
# MAGIC write as Delta with partitioning by date
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC Migrate all Parquet tables in this schema to Delta
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 🧙 Data Quality:
# MAGIC
# MAGIC ```
# MAGIC Add data quality checks to remove duplicates,
# MAGIC validate nulls, and enforce positive amounts
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC Implement schema validation and type casting
# MAGIC for the Bronze to Silver transformation
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC Create a data quality report showing rejected
# MAGIC records by reason
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 🛠️ Pipeline Building:
# MAGIC
# MAGIC ```
# MAGIC Build an end-to-end Medallion pipeline with
# MAGIC Bronze/Silver/Gold layers using Delta Lake
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC Create a Bronze layer that ingests JSON files
# MAGIC from Unity Catalog Volume with metadata tracking
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC Generate Gold layer aggregations for daily sales
# MAGIC metrics by customer and product
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 📊 Analytics:
# MAGIC
# MAGIC ```
# MAGIC Create analytical queries joining fact and dimension
# MAGIC tables with aggregations
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC Generate customer cohort analysis from transaction data
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC Build product performance metrics with revenue,
# MAGIC units sold, and customer reach
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### ⏳ Delta Lake Features:
# MAGIC
# MAGIC ```
# MAGIC Show me how to use Delta Lake time travel to
# MAGIC recover data from 2 days ago
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC Implement MERGE (upsert) operation for
# MAGIC incremental updates
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC Demonstrate UPDATE and DELETE operations on
# MAGIC Delta table with transaction log
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Pro Tips:
# MAGIC
# MAGIC * Be specific about requirements (table names, columns, logic)
# MAGIC * Mention data quality rules if needed
# MAGIC * Specify format (Delta, Parquet, etc.)
# MAGIC * Include layer (Bronze/Silver/Gold) in context
# MAGIC * Ask for best practices and optimizations
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,🎉 Conclusion
# MAGIC %md
# MAGIC ## 🎉 CONCLUSION
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎓 What You Learned:
# MAGIC
# MAGIC ✅ Data Warehouse fundamentals (Star Schema, Fact/Dimension)  
# MAGIC ✅ Data Lake vs Warehouse vs Lakehouse  
# MAGIC ✅ Medallion Architecture (Bronze → Silver → Gold)  
# MAGIC ✅ Row vs Column storage concepts  
# MAGIC ✅ File formats comparison (Parquet, Delta, ORC)  
# MAGIC ✅ Delta Lake advantages and features  
# MAGIC ✅ Metadata tracking best practices  
# MAGIC ✅ Unity Catalog Volumes usage  
# MAGIC ✅ End-to-end production pipeline design  
# MAGIC ✅ Data quality implementation  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Next Steps:
# MAGIC
# MAGIC 1. **Practice**: Run all cells in this notebook
# MAGIC 2. **Experiment**: Modify queries and see results
# MAGIC 3. **Build**: Create your own pipelines with real data
# MAGIC 4. **Optimize**: Learn Delta OPTIMIZE and Z-ORDER
# MAGIC 5. **Advanced**: Explore streaming, CDC, SCD Type 2
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📚 Resources:
# MAGIC
# MAGIC * **Databricks Documentation**: https://docs.databricks.com/
# MAGIC * **Delta Lake**: https://delta.io/
# MAGIC * **Unity Catalog**: https://docs.databricks.com/unity-catalog/
# MAGIC * **Best Practices**: Medallion Architecture guide
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💬 Feedback & Questions:
# MAGIC
# MAGIC Reach out to **@TRRaveendra** for:
# MAGIC * Questions about concepts
# MAGIC * Help with implementation
# MAGIC * Architecture review
# MAGIC * Advanced topics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏆 Certification:
# MAGIC
# MAGIC This notebook prepares you for:
# MAGIC * Databricks Certified Data Engineer Associate
# MAGIC * Data Architecture interviews
# MAGIC * Production pipeline design
# MAGIC * Lakehouse implementation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✨ Happy Learning! ✨
# MAGIC
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **End of Phase 1 Day 3**

# COMMAND ----------

# DBTITLE 1,📊 Conceptual Data Flow
# MAGIC %md
# MAGIC ## 📊 CONCEPTUAL DATA FLOW DIAGRAM
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### End-to-End Lakehouse Architecture:
# MAGIC
# MAGIC ```
# MAGIC 🌐 Source Systems
# MAGIC   • Databases (MySQL, PostgreSQL)
# MAGIC   • APIs (REST, GraphQL)
# MAGIC   • Files (CSV, JSON, logs)
# MAGIC   • Streams (Kafka, Event Hubs)
# MAGIC            |
# MAGIC            v
# MAGIC     [📥 Ingestion]
# MAGIC     Auto Loader
# MAGIC     Structured Streaming
# MAGIC     Batch Processing
# MAGIC            |
# MAGIC            v
# MAGIC ┌─────────────────────────────┐
# MAGIC │  🥉 BRONZE LAYER (Raw)    │
# MAGIC │  • Delta Lake Format       │
# MAGIC │  • As-is from source      │
# MAGIC │  • Audit metadata         │
# MAGIC │  • Historical archive     │
# MAGIC └─────────────────────────────┘
# MAGIC            |
# MAGIC            v
# MAGIC     [🛡️ Data Quality]
# MAGIC     Validation
# MAGIC     Deduplication
# MAGIC     Type Casting
# MAGIC     Business Rules
# MAGIC            |
# MAGIC            v
# MAGIC ┌─────────────────────────────┐
# MAGIC │  🥈 SILVER LAYER (Clean)  │
# MAGIC │  • Schema enforced        │
# MAGIC │  • Quality validated      │
# MAGIC │  • Enriched data          │
# MAGIC │  • Conformed              │
# MAGIC └─────────────────────────────┘
# MAGIC            |
# MAGIC            v
# MAGIC     [📊 Aggregation]
# MAGIC     Group By
# MAGIC     Joins
# MAGIC     KPI Calculation
# MAGIC     Star Schema
# MAGIC            |
# MAGIC            v
# MAGIC ┌─────────────────────────────┐
# MAGIC │  🥇 GOLD LAYER (Business) │
# MAGIC │  • Aggregated metrics     │
# MAGIC │  • Business KPIs          │
# MAGIC │  • Denormalized           │
# MAGIC │  • Optimized queries      │
# MAGIC └─────────────────────────────┘
# MAGIC            |
# MAGIC            v
# MAGIC     📊 Consumption
# MAGIC            |
# MAGIC     ├───────────────────┬───────────────────┐
# MAGIC     v                   v                   v
# MAGIC   📊 BI Tools       🤖 ML Models       📝 Reports
# MAGIC   Dashboards         Features            Analytics
# MAGIC   Tableau            Training            Insights
# MAGIC   Power BI           Inference           Metrics
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔄 Data Governance Layer (Unity Catalog):
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────┐
# MAGIC │           Unity Catalog (Governance)              │
# MAGIC │  • Access Control (ACLs)                       │
# MAGIC │  • Audit Logs                                  │
# MAGIC │  • Data Lineage                                │
# MAGIC │  • Data Discovery                              │
# MAGIC │  • Column/Row Level Security                   │
# MAGIC └──────────────────────────────────────────────────┘
# MAGIC                       ↕️
# MAGIC           All layers protected
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛠️ Technology Stack:
# MAGIC
# MAGIC * **Storage**: Delta Lake (Parquet + Transaction Log)
# MAGIC * **Compute**: Databricks Serverless / Clusters
# MAGIC * **Orchestration**: Databricks Jobs / Workflows
# MAGIC * **Governance**: Unity Catalog
# MAGIC * **Processing**: Apache Spark (PySpark API)
# MAGIC * **Monitoring**: Delta Live Tables Observability
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,🏁 How to Use This Notebook
# MAGIC %md
# MAGIC ## 🏁 HOW TO USE THIS NOTEBOOK
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Quick Start:
# MAGIC
# MAGIC 1. **Attach Compute**:
# MAGIC    * This notebook is serverless-ready
# MAGIC    * Databricks will auto-select serverless compute when you run cells
# MAGIC    * No manual cluster configuration needed!
# MAGIC
# MAGIC 2. **Run All Cells**:
# MAGIC    * Click "Run All" to execute the entire notebook
# MAGIC    * Or run cells sequentially to see each concept
# MAGIC    * Estimated runtime: 2-3 minutes
# MAGIC
# MAGIC 3. **Explore Results**:
# MAGIC    * Review visualizations and outputs
# MAGIC    * Experiment with queries
# MAGIC    * Modify data and re-run
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Learning Path:
# MAGIC
# MAGIC #### **For Beginners**:
# MAGIC 1. Read markdown sections carefully
# MAGIC 2. Focus on ELI5 explanations first
# MAGIC 3. Run code cells and observe outputs
# MAGIC 4. Try simple modifications
# MAGIC
# MAGIC #### **For Intermediate**:
# MAGIC 1. Study architect-level explanations
# MAGIC 2. Analyze pipeline design patterns
# MAGIC 3. Review data quality checks
# MAGIC 4. Implement in your own projects
# MAGIC
# MAGIC #### **For Advanced**:
# MAGIC 1. Optimize Delta tables (OPTIMIZE, Z-ORDER)
# MAGIC 2. Implement streaming pipelines
# MAGIC 3. Add Change Data Feed
# MAGIC 4. Design complex star schemas
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📝 Practice Exercises:
# MAGIC
# MAGIC 1. **Exercise 1**: Modify the Star Schema to add a new dimension (store_dim)
# MAGIC 2. **Exercise 2**: Add more data quality rules in Silver layer
# MAGIC 3. **Exercise 3**: Create a new Gold table for time-series analysis
# MAGIC 4. **Exercise 4**: Implement SCD Type 2 for customer dimension
# MAGIC 5. **Exercise 5**: Convert existing Parquet tables to Delta
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚡ Performance Tips:
# MAGIC
# MAGIC * Use `display()` to limit output rows automatically
# MAGIC * Delta tables are optimized for subsequent reads
# MAGIC * Serverless compute scales automatically
# MAGIC * Review query execution plans with `explain()`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔗 Related Topics for Next Learning:
# MAGIC
# MAGIC * **Streaming with Delta Lake** (auto loader, structured streaming)
# MAGIC * **Delta Lake Advanced Features** (CDF, Z-ordering, liquid clustering)
# MAGIC * **Slowly Changing Dimensions** (SCD Type 1, 2, 3)
# MAGIC * **Data Quality with Expectations** (Delta Live Tables)
# MAGIC * **Performance Tuning** (partitioning, optimization)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Success Criteria:
# MAGIC
# MAGIC You've mastered this module when you can:
# MAGIC
# MAGIC ✅ Explain medallion architecture to a stakeholder  
# MAGIC ✅ Design a star schema for a business problem  
# MAGIC ✅ Choose the right file format for use cases  
# MAGIC ✅ Implement data quality checks  
# MAGIC ✅ Build production-ready Delta pipelines  
# MAGIC ✅ Use Unity Catalog for governance  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👏 You're Ready!
# MAGIC
# MAGIC **Run the notebook and start your Lakehouse journey!**
# MAGIC
# MAGIC ---