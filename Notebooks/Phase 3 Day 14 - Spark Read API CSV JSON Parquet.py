# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ⚡ Data Engineering Training — Phase 3 Day 14  
# MAGIC ## 📥 Spark Read API: CSV, JSON, Parquet & Schema Handling  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC * Reading CSV, JSON, Parquet in Spark  
# MAGIC * Schema Handling (Inference vs Explicit)  
# MAGIC * File Format Differences  
# MAGIC * Best Practices for Data Ingestion  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### ⚙️ Platform: Databricks (Serverless + Unity Catalog + Spark)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Objective:
# MAGIC Understand how to ingest data using Spark Read API from different file formats and apply proper schema handling techniques for production pipelines.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚙️ Engineering Constraints:
# MAGIC * ✅ Use Databricks Serverless Compute
# MAGIC * ✅ DataFrame API only (No RDDs)
# MAGIC * ✅ No cache() / persist()
# MAGIC * ✅ No /tmp or local storage
# MAGIC * ✅ Unity Catalog Volumes for all data access
# MAGIC * ✅ Schema-first ingestion approach

# COMMAND ----------

# DBTITLE 1,Setup: Create Sample Data
# MAGIC %md
# MAGIC ## 🛠️ Setup: Create Sample Data for Demonstrations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Important Note:
# MAGIC
# MAGIC This section creates sample datasets in Unity Catalog Volumes that will be used throughout the notebook demonstrations. **Run this section first** before executing other cells.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💾 What We'll Create:
# MAGIC
# MAGIC * **Sample CSV data** — Sales transactions
# MAGIC * **Sample JSON data** — Event logs
# MAGIC * **Sample Parquet data** — Transaction records
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📝 Volume Structure:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/workspace/default/sample_data/
# MAGIC   ├── sales.csv
# MAGIC   ├── products.csv
# MAGIC   ├── events.json
# MAGIC   ├── config.json
# MAGIC   └── transactions.parquet
# MAGIC ```

# COMMAND ----------

# Spark as ETL with Dataframe API

# Extract
df = spark.createDataFrame(datainalist,schemainlist) # if we have data in a list (max 128MB) then use this to create dataframe
df = spark.read.format(csv/parquet/orc/avro/xml/delta/json).load(path) # if we have data in a file then use this to create dataframe (volume/external location)
df = spark.sql(sqlquery) # if we have data in a sql query then use this to create dataframe
df = spark.table(catalog.schema.table)
# Transform
df1 = df.transformats # any transformations to be applied on dataframe


# Load
df2 = df1.write.format(csv/parquet/orc/avro/xml/delta/json).save(path) # if we have data in a file then use this to write dataframe(volume or external path)
df2 = df1.write.format(csv/parquet/orc/avro/xml/delta/json).saveAsTable(catalog.schema.table) # if we have data in a file then use this to write dataframe

# COMMAND ----------

# DBTITLE 1,Create Volume and Sample Data
# 📌 Setup: Create Unity Catalog Volume and Sample Data

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, to_date, lit
from datetime import date, timedelta
import random

# Use workspace catalog (current catalog)
catalog_name = "workspace"
schema_name = "default"

# Create sample_data volume if it doesn't exist
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.sample_data")
print(f"✅ Volume created: {catalog_name}.{schema_name}.sample_data")

# Define base path
base_path = f"/Volumes/{catalog_name}/{schema_name}/sample_data"

print(f"\n📋 Base path: {base_path}")
print("\n🚀 Creating sample datasets...\n")

# ============================================
# 1. Create Sample CSV Data - Sales
# ============================================
print("1️⃣ Creating sales.csv...")

sales_data = [
    (1, 101, "Product A", 150.50, "2024-01-15", "completed"),
    (2, 102, "Product B", 299.99, "2024-01-16", "completed"),
    (3, 103, "Product C", 75.00, "2024-01-17", "pending"),
    (4, 101, "Product D", 450.00, "2024-01-18", "completed"),
    (5, 104, "Product A", 150.50, "2024-01-19", "completed"),
    (6, 105, "Product E", 999.99, "2024-01-20", "cancelled"),
    (7, 102, "Product B", 299.99, "2024-01-21", "completed"),
    (8, 103, "Product C", 75.00, "2024-01-22", "completed"),
    (9, 106, "Product F", 125.00, "2024-01-23", "pending"),
    (10, 101, "Product A", 150.50, "2024-01-24", "completed")
]

sales_schema = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("product_name", StringType(), True),
    StructField("amount", DoubleType(), False),
    StructField("transaction_date", StringType(), False),
    StructField("status", StringType(), True)
])
# Extract
df_sales = spark.createDataFrame(sales_data, sales_schema)
# Load/save
df_sales.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{base_path}/sales.csv")
print(f"   ✅ Created: sales.csv ({df_sales.count()} records)")

# ============================================
# 2. Create Sample CSV Data - Products
# ============================================
print("\n2️⃣ Creating products.csv...")

products_data = [
    (1, "Laptop", "Electronics", 1299.99),
    (2, "Mouse", "Accessories", 29.99),
    (3, "Keyboard", "Accessories", 79.99),
    (4, "Monitor", "Electronics", 399.99),
    (5, "Headphones", "Accessories", 199.99)
]

products_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), False)
])

df_products = spark.createDataFrame(products_data, products_schema)
df_products.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{base_path}/products.csv")
print(f"   ✅ Created: products.csv ({df_products.count()} records)")

# ============================================
# 3. Create Sample JSON Data - Events
# ============================================
print("\n3️⃣ Creating events.json...")

events_data = [
    {"event_id": 1, "user_id": 101, "event_type": "login", "timestamp": "2024-01-15T08:30:00", "details": {"ip": "192.168.1.1", "device": "mobile"}},
    {"event_id": 2, "user_id": 102, "event_type": "purchase", "timestamp": "2024-01-15T09:45:00", "details": {"ip": "192.168.1.2", "device": "desktop"}},
    {"event_id": 3, "user_id": 103, "event_type": "logout", "timestamp": "2024-01-15T10:15:00", "details": {"ip": "192.168.1.3", "device": "tablet"}},
    {"event_id": 4, "user_id": 101, "event_type": "view", "timestamp": "2024-01-15T11:00:00", "details": {"ip": "192.168.1.1", "device": "mobile"}},
    {"event_id": 5, "user_id": 104, "event_type": "purchase", "timestamp": "2024-01-15T12:30:00", "details": {"ip": "192.168.1.4", "device": "desktop"}}
]

df_events = spark.createDataFrame(events_data)
df_events.coalesce(1).write.mode("overwrite").json(f"{base_path}/events.json")
print(f"   ✅ Created: events.json ({df_events.count()} records)")

# ============================================
# 4. Create Sample JSON Data - Config (Multi-line)
# ============================================
print("\n4️⃣ Creating config.json (multi-line)...")

config_data = [
    {"app_name": "DataPipeline", "version": "1.0", "config": {"max_retries": 3, "timeout": 30}},
    {"app_name": "Analytics", "version": "2.1", "config": {"max_retries": 5, "timeout": 60}}
]

df_config = spark.createDataFrame(config_data)
df_config.coalesce(1).write.mode("overwrite").json(f"{base_path}/config.json")
print(f"   ✅ Created: config.json ({df_config.count()} records)")

# ============================================
# 5. Create Sample Parquet Data - Transactions
# ============================================
print("\n5️⃣ Creating transactions.parquet...")

transactions_data = [
    (1001, 201, 301, "Laptop Pro", 2, 1299.99, 2598.98, date(2024, 1, 15), "completed"),
    (1002, 202, 302, "Wireless Mouse", 1, 29.99, 29.99, date(2024, 1, 16), "completed"),
    (1003, 203, 303, "Mechanical Keyboard", 1, 79.99, 79.99, date(2024, 1, 17), "pending"),
    (1004, 201, 304, "4K Monitor", 2, 399.99, 799.98, date(2024, 1, 18), "completed"),
    (1005, 204, 305, "Noise-Canceling Headphones", 1, 199.99, 199.99, date(2024, 1, 19), "completed"),
    (1006, 205, 301, "Laptop Pro", 1, 1299.99, 1299.99, date(2024, 1, 20), "cancelled"),
    (1007, 202, 302, "Wireless Mouse", 3, 29.99, 89.97, date(2024, 1, 21), "completed"),
    (1008, 203, 306, "USB-C Hub", 2, 49.99, 99.98, date(2024, 1, 22), "completed"),
    (1009, 206, 307, "Webcam HD", 1, 79.99, 79.99, date(2024, 1, 23), "pending"),
    (1010, 201, 305, "Noise-Canceling Headphones", 1, 199.99, 199.99, date(2024, 1, 24), "completed")
]

transactions_schema = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), True),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("total_amount", DoubleType(), False),
    StructField("transaction_date", DateType(), False),
    StructField("status", StringType(), True)
])

df_transactions = spark.createDataFrame(transactions_data, transactions_schema)
df_transactions.write.mode("overwrite").parquet(f"{base_path}/transactions.parquet")
print(f"   ✅ Created: transactions.parquet ({df_transactions.count()} records)")

# ============================================
# 6. Create additional data for raw_data folder
# ============================================
print("\n6️⃣ Creating raw_data folders...")

# Create directories for pipeline demo
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.raw_data")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.processed_data")

# Write sample data to raw_data locations
df_transactions.write.mode("overwrite").option("header", "true").csv(f"/Volumes/{catalog_name}/{schema_name}/raw_data/csv/transactions.csv")
df_transactions.write.mode("overwrite").json(f"/Volumes/{catalog_name}/{schema_name}/raw_data/json/transactions.json")
df_transactions.write.mode("overwrite").parquet(f"/Volumes/{catalog_name}/{schema_name}/raw_data/parquet/transactions.parquet")

print("   ✅ Created: raw_data/csv/transactions.csv")
print("   ✅ Created: raw_data/json/transactions.json")
print("   ✅ Created: raw_data/parquet/transactions.parquet")

print("\n" + "="*60)
print("🎉 Sample data creation complete!")
print("="*60)
print("\n✅ All demonstration files are ready")
print("✅ You can now run all subsequent cells in this notebook")
print(f"\n📂 Created volumes:")
print(f"   - {catalog_name}.{schema_name}.sample_data")
print(f"   - {catalog_name}.{schema_name}.raw_data")
print(f"   - {catalog_name}.{schema_name}.processed_data")

# COMMAND ----------

# MAGIC %fs ls /Volumes/workspace/default/sample_data/sales.csv/

# COMMAND ----------

# MAGIC %fs head dbfs:/Volumes/workspace/default/sample_data/sales.csv/part-00000-tid-3316770590239205109-940b8743-802f-4d3b-b4a0-598d212be36b-124-1-c000.csv

# COMMAND ----------

# DBTITLE 1,Setup Path Variables
# 📌 Setup: Define Path Variables for Demo Cells
# All subsequent cells will use these variables

# Catalog and schema names
CATALOG = "workspace"
SCHEMA = "default"

# Volume paths
SAMPLE_DATA_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/sample_data"
RAW_DATA_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"
PROCESSED_DATA_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/processed_data"

print("✅ Path variables configured:")
print(f"   Sample Data: {SAMPLE_DATA_PATH}")
print(f"   Raw Data: {RAW_DATA_PATH}")
print(f"   Processed Data: {PROCESSED_DATA_PATH}")
print("\n📌 Note: All demonstration cells will use these paths")

# COMMAND ----------

# DBTITLE 1,Section 1: Spark Read API Overview
# MAGIC %md
# MAGIC ## 📖 Section 1: Spark Read API Overview
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧒 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine you have different types of books — some are picture books (CSV), some are comic books (JSON), and some are organized encyclopedias (Parquet). The **Spark Read API** is like a smart librarian who knows how to read all these different types of books and turn them into a format you can understand.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC The **Spark Read API** is the unified interface for ingesting data from various file formats into Spark DataFrames. It provides:
# MAGIC
# MAGIC * **Format-agnostic abstraction** — `spark.read.format("<format>")`
# MAGIC * **Lazy evaluation** — No data is read until an action is triggered
# MAGIC * **Distributed reading** — Partitions are read in parallel across executors
# MAGIC * **Schema handling** — Supports both inference and explicit schema definition
# MAGIC * **Optimizations** — Predicate pushdown, column pruning for supported formats
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📋 Supported File Formats:
# MAGIC
# MAGIC | Format | Extension | Schema Storage | Performance | Use Case |
# MAGIC |--------|-----------|----------------|-------------|----------|
# MAGIC | **CSV** | .csv | No (external) | Slow | Human-readable, legacy data |
# MAGIC | **JSON** | .json | No (external) | Moderate | Semi-structured, nested data |
# MAGIC | **Parquet** | .parquet | Yes (embedded) | Fast | Columnar analytics, production |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔑 Key Concepts:
# MAGIC
# MAGIC 1. **DataFrameReader** — `spark.read` returns a DataFrameReader object
# MAGIC 2. **Format Specification** — `.format("csv")` or shorthand `.csv()`
# MAGIC 3. **Options** — `.option("key", "value")` for format-specific configs
# MAGIC 4. **Load Path** — `.load("path")` to specify data location
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Read API Pattern:
# MAGIC
# MAGIC ```python
# MAGIC df = spark.read \
# MAGIC     .format("<format>") \
# MAGIC     .option("<key>", "<value>") \
# MAGIC     .load("<path>")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 2: Reading CSV Files
# MAGIC %md
# MAGIC ## 📄 Section 2: Reading CSV Files
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC
# MAGIC CSV files are like simple tables written in a notebook — each row is on a new line, and columns are separated by commas. But Spark doesn't know if the first row contains column names or data, so we need to tell it!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC CSV (Comma-Separated Values) is a **row-based text format** with:
# MAGIC
# MAGIC * **No embedded schema** — Requires header row or external schema definition
# MAGIC * **Text-based storage** — All values stored as strings unless schema is provided
# MAGIC * **Delimiter flexibility** — Can use comma, pipe, tab, or custom delimiters
# MAGIC * **Performance trade-offs**:
# MAGIC   * ❌ Cannot skip unnecessary columns (must read entire row)
# MAGIC   * ❌ No compression benefits
# MAGIC   * ❌ Slower parsing (string to type conversion)
# MAGIC   * ✅ Human-readable for debugging
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔧 Common CSV Options:
# MAGIC
# MAGIC | Option | Description | Example |
# MAGIC |--------|-------------|--------|
# MAGIC | `header` | First row contains column names | `"true"` or `"false"` |
# MAGIC | `inferSchema` | Automatically detect column types | `"true"` (expensive!) |
# MAGIC | `delimiter` | Column separator character | `","` or `"\|"` or `"\t"` |
# MAGIC | `quote` | Character for quoting values | `'"'` |
# MAGIC | `escape` | Escape character | `'\\'` |
# MAGIC | `nullValue` | String representing NULL | `"NULL"` or `""` |
# MAGIC | `dateFormat` | Date parsing pattern | `"yyyy-MM-dd"` |
# MAGIC | `mode` | Error handling mode | `"PERMISSIVE"`, `"DROPMALFORMED"`, `"FAILFAST"` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Schema Inference Warning:
# MAGIC
# MAGIC **Schema inference scans the entire dataset** to determine types — this is:
# MAGIC * 🔴 **Expensive** on large files (double read)
# MAGIC * 🔴 **Unreliable** with dirty data
# MAGIC * 🔴 **Non-deterministic** across runs
# MAGIC
# MAGIC ➡️ **Best Practice**: Always define schema explicitly in production!

# COMMAND ----------

# DBTITLE 1,Demo: Reading CSV with Schema Inference
# 📌 Demo: Reading CSV Files with Schema Inference
# Note: This is for demonstration only - NOT recommended for production!

# Method 1: Using format() with options
df_csv_inferred = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ",") \
    .load("/Volumes/workspace/default/sample_data/sales.csv")  # Replace with your volume path

print("✅ CSV loaded with schema inference")
print(f"Schema inferred: {df_csv_inferred.schema}")
print(f"Row count: {df_csv_inferred.count()}")

# Display sample
display(df_csv_inferred.limit(5))

# COMMAND ----------

# DBTITLE 1,Demo: Reading CSV with Shorthand Syntax
# 📌 Demo: Shorthand CSV Reading Syntax

# Method 2: Using shorthand .csv() method
df_csv_shorthand = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/Volumes/workspace/default/sample_data/sales.csv")  # Replace with your volume path

print("✅ CSV loaded using shorthand syntax")
print("\n📊 Schema:")
df_csv_shorthand.printSchema()

# Show data types
print("\n🏷️ Column Names and Types:")
for field in df_csv_shorthand.schema.fields:
    print(f"  - {field.name}: {field.dataType}")

# COMMAND ----------

# DBTITLE 1,Section 3: Reading JSON Files
# MAGIC %md
# MAGIC ## 📋 Section 3: Reading JSON Files
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC
# MAGIC JSON files are like Russian nesting dolls — you can have boxes inside boxes! Spark is smart enough to understand these nested structures and turn them into organized tables.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC JSON (JavaScript Object Notation) is a **semi-structured text format** with:
# MAGIC
# MAGIC * **Nested schema support** — Objects can contain other objects and arrays
# MAGIC * **Self-describing** — Keys are embedded in the data
# MAGIC * **Schema inference** — Spark can infer complex nested structures
# MAGIC * **Flexible schema** — Different records can have different fields
# MAGIC * **Storage characteristics**:
# MAGIC   * ❌ Row-based (cannot skip columns)
# MAGIC   * ❌ Text-based (no native compression)
# MAGIC   * ✅ Human-readable
# MAGIC   * ✅ Handles nested/hierarchical data naturally
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔧 Common JSON Options:
# MAGIC
# MAGIC | Option | Description | Example |
# MAGIC |--------|-------------|--------|
# MAGIC | `multiLine` | Parse multi-line JSON objects | `"true"` for pretty-printed JSON |
# MAGIC | `allowComments` | Allow comments in JSON | `"true"` |
# MAGIC | `allowUnquotedFieldNames` | Allow unquoted keys | `"true"` |
# MAGIC | `allowSingleQuotes` | Allow single quotes | `"true"` |
# MAGIC | `primitivesAsString` | Read all primitives as strings | `"true"` |
# MAGIC | `dateFormat` | Date parsing pattern | `"yyyy-MM-dd'T'HH:mm:ss"` |
# MAGIC | `mode` | Error handling | `"PERMISSIVE"`, `"DROPMALFORMED"`, `"FAILFAST"` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 JSON Format Types:
# MAGIC
# MAGIC **1. JSON Lines (JSONL)** — One JSON object per line:
# MAGIC ```json
# MAGIC {"id": 1, "name": "Alice"}
# MAGIC {"id": 2, "name": "Bob"}
# MAGIC ```
# MAGIC
# MAGIC **2. Multi-line JSON** — Pretty-printed format:
# MAGIC ```json
# MAGIC [
# MAGIC   {"id": 1, "name": "Alice"},
# MAGIC   {"id": 2, "name": "Bob"}
# MAGIC ]
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Demo: Reading JSON Lines Format
# 📌 Demo: Reading JSON Lines (JSONL) Format
# Each line is a complete JSON object

df_json = spark.read.format("json") \
    .load("/Volumes/workspace/default/sample_data/events.json")  # Replace with your volume path

print("✅ JSON loaded successfully")
print("\n📊 Schema (notice nested structures):")
df_json.printSchema()

print(f"\n📊 Row count: {df_json.count()}")
display(df_json.limit(5))

# COMMAND ----------

# DBTITLE 1,Demo: Reading Multi-line JSON
# 📌 Demo: Reading Multi-line JSON (Pretty-Printed)
# Requires multiLine option set to true

df_json_multiline = spark.read.format("json") \
    .option("multiLine", "true") \
    .load("/Volumes/workspace/default/sample_data/config.json")  # Replace with your volume path

print("✅ Multi-line JSON loaded")
df_json_multiline.printSchema()

# Display sample
display(df_json_multiline)

# COMMAND ----------

# DBTITLE 1,Demo: Handling Nested JSON Structures
# 📌 Demo: Working with Nested JSON Structures

from pyspark.sql.functions import col, explode

# Sample nested JSON structure demonstration
# Assuming JSON has structure like:
# {"user": {"id": 1, "name": "Alice"}, "orders": [{"order_id": 101}, {"order_id": 102}]}

# Access nested fields using dot notation
# df_nested = df_json.select(
#     col("user.id").alias("user_id"),
#     col("user.name").alias("user_name"),
#     col("orders")
# )

# Explode array fields
# df_exploded = df_nested.select(
#     "user_id",
#     "user_name",
#     explode("orders").alias("order")
# ).select(
#     "user_id",
#     "user_name",
#     col("order.order_id")
# )

print("✅ Nested JSON handling pattern demonstrated")
print("\n📊 Use dot notation: col('parent.child')")
print("📊 Use explode() for arrays: explode(col('array_field'))")
print("📊 Chain selections for deep nesting")

# COMMAND ----------

# DBTITLE 1,Section 4: Reading Parquet Files
# MAGIC %md
# MAGIC ## 📦 Section 4: Reading Parquet Files
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC
# MAGIC Parquet files are like organized filing cabinets where each drawer (column) is stored separately. If you only need information from one drawer, you don't have to open all the others — making it super fast!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC Parquet is a **columnar storage format** optimized for analytics:
# MAGIC
# MAGIC * **Schema embedded** — No external schema needed, metadata stored in file footer
# MAGIC * **Columnar storage** — Data organized by column, not row
# MAGIC * **Compression** — Efficient encoding per column (RLE, dictionary, etc.)
# MAGIC * **Performance optimizations**:
# MAGIC   * ✅ **Column pruning** — Read only needed columns
# MAGIC   * ✅ **Predicate pushdown** — Filter at file level before reading
# MAGIC   * ✅ **Vectorized reading** — Batch processing for speed
# MAGIC   * ✅ **Splittable** — Parallel reads across partitions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📈 Parquet vs Row-Based Formats:
# MAGIC
# MAGIC **Row-Based (CSV, JSON)**:
# MAGIC ```
# MAGIC [id=1, name=Alice, age=25]
# MAGIC [id=2, name=Bob, age=30]
# MAGIC [id=3, name=Carol, age=28]
# MAGIC ```
# MAGIC ➡️ Must read entire row even if you only need `name`
# MAGIC
# MAGIC **Columnar (Parquet)**:
# MAGIC ```
# MAGIC id:   [1, 2, 3]
# MAGIC name: [Alice, Bob, Carol]
# MAGIC age:  [25, 30, 28]
# MAGIC ```
# MAGIC ➡️ Read only the `name` column if that's all you need!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏆 Why Parquet for Production:
# MAGIC
# MAGIC * **10-100x smaller** than CSV/JSON (compression)
# MAGIC * **10-100x faster** for analytical queries (column pruning)
# MAGIC * **Type safety** — Schema enforced at storage level
# MAGIC * **Ecosystem support** — Native support in Spark, Hive, Presto, etc.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔧 Parquet Read Options:
# MAGIC
# MAGIC | Option | Description | Example |
# MAGIC |--------|-------------|--------|
# MAGIC | `mergeSchema` | Merge schemas from multiple files | `"true"` |
# MAGIC | `pathGlobFilter` | Filter files by pattern | `"*.parquet"` |
# MAGIC | `modifiedBefore` | Filter by modification time | Timestamp |
# MAGIC | `modifiedAfter` | Filter by modification time | Timestamp |

# COMMAND ----------

# DBTITLE 1,Demo: Reading Parquet Files
# 📌 Demo: Reading Parquet Files
# Parquet is the preferred format for production pipelines

df_parquet = spark.read.format("parquet") \
    .load("/Volumes/workspace/default/sample_data/transactions.parquet")  # Replace with your volume path

print("✅ Parquet loaded successfully")
print("\n✨ Notice: Schema is automatically available (embedded in Parquet)")
df_parquet.printSchema()

print(f"\n📊 Row count: {df_parquet.count()}")

# Display sample
display(df_parquet.limit(5))

# COMMAND ----------

df_pruned = spark.read.format("parquet") \
    .load("/Volumes/workspace/default/sample_data/transactions.parquet")

# COMMAND ----------

# DBTITLE 1,Demo: Parquet Column Pruning
# 📌 Demo: Parquet Column Pruning (Performance Optimization)
# Only specified columns are read from storage

# Extraction
# Example: Read only 2 columns from a wide table
df_pruned = spark.read.format("parquet") \
    .load("/Volumes/workspace/default/sample_data/transactions.parquet")
# Transformation
#Transformations are lazy evaluated. it wont run any job until there is an action(display data or save data )
df2 = df_pruned.select("transaction_id", "total_amount")  # Only these columns are read!

print("✅ Parquet with column pruning")
print("\n⚡ Performance Benefit: Only selected columns are read from disk")
print(f"Columns selected: {df2.columns}")

# Actions are going to trigger job
# Action for displaying data 
# Action we can use for saving data/ writing into file or table
df2.write.format("delta").mode("overwrite").saveAsTable("workspace.default.transactions")
display(df2.limit(5))

# COMMAND ----------

# DBTITLE 1,Demo: Parquet Shorthand Syntax
# 📌 Demo: Parquet Shorthand Reading Syntax

# Method 2: Using shorthand .parquet() method
df_parquet_shorthand = spark.read.parquet("/Volumes/workspace/default/sample_data/transactions.parquet")

print("✅ Parquet loaded using shorthand syntax")
print(f"Schema fields: {len(df_parquet_shorthand.schema.fields)}")
print(f"Columns: {df_parquet_shorthand.columns}")

# COMMAND ----------

# DBTITLE 1,Section 5: Schema Handling (Critical)
# MAGIC %md
# MAGIC ## 🏛️ Section 5: Schema Handling — Inference vs Explicit (CRITICAL)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC
# MAGIC Imagine building a LEGO castle. You can either:
# MAGIC * **Guess** what pieces you need by looking at a picture (schema inference) — slow and might be wrong
# MAGIC * **Follow the instruction manual** (explicit schema) — fast, accurate, and repeatable!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Schema** defines the structure of your DataFrame:
# MAGIC * Column names
# MAGIC * Data types (IntegerType, StringType, TimestampType, etc.)
# MAGIC * Nullability (can column contain NULL?)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 Part A: Schema Inference
# MAGIC
# MAGIC #### How It Works:
# MAGIC 1. Spark scans the **entire dataset** (or sample)
# MAGIC 2. Infers types based on values found
# MAGIC 3. Chooses the "safest" type (e.g., String if mixed types)
# MAGIC
# MAGIC #### Pros:
# MAGIC * ✅ Quick for exploration
# MAGIC * ✅ No manual schema definition
# MAGIC
# MAGIC #### Cons:
# MAGIC * ❌ **Double I/O** — Reads data twice (inference + actual read)
# MAGIC * ❌ **Expensive** on large files (GBs/TBs)
# MAGIC * ❌ **Unreliable** with dirty/inconsistent data
# MAGIC * ❌ **Non-deterministic** — Schema can change between runs
# MAGIC * ❌ **Type issues** — May infer String when you need Integer
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟢 Part B: Explicit Schema (PRODUCTION BEST PRACTICE)
# MAGIC
# MAGIC #### How It Works:
# MAGIC 1. Define schema upfront using `StructType` and `StructField`
# MAGIC 2. Spark applies schema during read (no inference scan)
# MAGIC 3. Type enforcement and validation at ingestion
# MAGIC
# MAGIC #### Pros:
# MAGIC * ✅ **Single I/O** — Reads data only once
# MAGIC * ✅ **Fast** — No inference overhead
# MAGIC * ✅ **Deterministic** — Same schema every time
# MAGIC * ✅ **Type safety** — Enforces expected types
# MAGIC * ✅ **Fails fast** — Catches schema violations early
# MAGIC * ✅ **Documentation** — Schema serves as data contract
# MAGIC
# MAGIC #### Cons:
# MAGIC * ❌ Requires upfront schema definition (worth it!)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏆 Schema Strategy:
# MAGIC
# MAGIC | Environment | Recommendation | Reason |
# MAGIC |-------------|----------------|--------|
# MAGIC | **Development/EDA** | Schema inference OK | Fast prototyping |
# MAGIC | **Testing** | Explicit schema | Catch issues early |
# MAGIC | **Production** | **Explicit schema MANDATORY** | Performance, reliability, type safety |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📝 Schema Definition Syntax:
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.types import (
# MAGIC     StructType, StructField, 
# MAGIC     StringType, IntegerType, DoubleType, 
# MAGIC     TimestampType, BooleanType, DateType
# MAGIC )
# MAGIC
# MAGIC schema = StructType([
# MAGIC     StructField("column_name", DataType(), nullable=True/False),
# MAGIC     ...
# MAGIC ])
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Demo: Explicit Schema Definition
# 📌 Demo: Defining Explicit Schema (BEST PRACTICE)

from pyspark.sql.types import (
    StructType, StructField, 
    StringType, IntegerType, DoubleType, TimestampType, DateType
)

# Define schema for sales data
sales_schema = StructType([
    StructField("transaction_id", IntegerType(), nullable=False),
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("product_name", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=False),
    StructField("transaction_date", DateType(), nullable=False),
    StructField("status", StringType(), nullable=True)
])

print("✅ Schema defined explicitly")
print("\n📊 Schema structure:")
print(sales_schema)

# Display schema in readable format
print("\n📄 Schema fields:")
for field in sales_schema.fields:
    nullable_str = "nullable" if field.nullable else "NOT NULL"
    print(f"  - {field.name}: {field.dataType} ({nullable_str})")

# COMMAND ----------

# DBTITLE 1,Demo: Reading CSV with Explicit Schema
# 📌 Demo: Reading CSV with Explicit Schema
# This is the PRODUCTION-RECOMMENDED approach

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Define schema
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), False)
])

# Read CSV with explicit schema
df_with_schema = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("/Volumes/workspace/default/sample_data/products.csv")  # Replace with your volume path

print("✅ CSV loaded with explicit schema")
print("\n⚡ Performance: Single I/O (no inference scan)")
print("✅ Type Safety: Schema enforced during read")

print("\n📊 Applied schema:")
df_with_schema.printSchema()

# Display sample
display(df_with_schema.limit(5))

# COMMAND ----------

# DBTITLE 1,Demo: Comparison - Inference vs Explicit
# 📌 Demo: Performance Comparison - Inference vs Explicit Schema

import time
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

data_path = "/Volumes/workspace/default/sample_data/large_dataset.csv"  # Replace with your volume path

# Schema for testing
test_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("value", StringType(), True)
])

print("🔍 Performance Comparison: Schema Inference vs Explicit Schema\n")

# Method 1: Schema Inference
print("1️⃣ Schema Inference (inferSchema=true):")
start = time.time()
try:
    df_infer = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(data_path)
    df_infer.count()  # Trigger action
    inference_time = time.time() - start
    print(f"   ⏱️ Time taken: {inference_time:.2f} seconds")
    print("   📊 Reads data TWICE (inference + actual load)")
except Exception as e:
    print(f"   ⚠️ File not found (demo path) - {str(e)}")
    inference_time = None

print()

# Method 2: Explicit Schema
print("2️⃣ Explicit Schema:")
start = time.time()
try:
    df_explicit = spark.read.format("csv") \
        .option("header", "true") \
        .schema(test_schema) \
        .load(data_path)
    df_explicit.count()  # Trigger action
    explicit_time = time.time() - start
    print(f"   ⏱️ Time taken: {explicit_time:.2f} seconds")
    print("   📊 Reads data ONCE (direct load with schema)")
except Exception as e:
    print(f"   ⚠️ File not found (demo path) - {str(e)}")
    explicit_time = None

print()

if inference_time and explicit_time:
    speedup = inference_time / explicit_time
    print(f"🏆 Result: Explicit schema is {speedup:.2f}x FASTER!")
else:
    print("📌 Note: Replace data_path with actual file to see performance difference")

print("\n💡 Key Takeaway: Always use explicit schema in production pipelines!")

# COMMAND ----------

# DBTITLE 1,Demo: Complex Schema with Nested Types
# 📌 Demo: Complex Schema Definition with Nested Types

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    ArrayType, MapType, TimestampType
)

# Define complex schema with nested structures
complex_schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("username", StringType(), False),
    
    # Nested struct
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("zipcode", StringType(), True)
    ]), True),
    
    # Array type
    StructField("tags", ArrayType(StringType()), True),
    
    # Map type
    StructField("metadata", MapType(StringType(), StringType()), True),
    
    StructField("created_at", TimestampType(), False)
])

print("✅ Complex schema with nested types defined")
print("\n📊 Schema structure:")
print(complex_schema.simpleString())

print("\n📝 Supports:")
print("  ✅ Nested structs (address.city)")
print("  ✅ Arrays (tags[0])")
print("  ✅ Maps (metadata['key'])")
print("\n💡 Use this for JSON with nested structures!")

# COMMAND ----------

# DBTITLE 1,Section 6: Metadata Usage
# MAGIC %md
# MAGIC ## 📍 Section 6: File Metadata — Tracking Data Lineage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC
# MAGIC Imagine reading multiple books and wanting to remember which book each fact came from. File metadata is like writing "Source: Book Title, Page 42" next to each note you take!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC Spark provides **hidden metadata columns** for tracking file-level information:
# MAGIC
# MAGIC * `_metadata.file_path` — Full path to source file
# MAGIC * `_metadata.file_name` — Filename only
# MAGIC * `_metadata.file_size` — File size in bytes
# MAGIC * `_metadata.file_modification_time` — Last modified timestamp
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Use Cases:
# MAGIC
# MAGIC 1. **Data Lineage** — Track which file each record came from
# MAGIC 2. **Incremental Processing** — Identify already-processed files
# MAGIC 3. **Debugging** — Trace data quality issues to specific files
# MAGIC 4. **Audit Trail** — Compliance and regulatory requirements
# MAGIC 5. **File-based Watermarking** — Track ingestion progress
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Best Practice:
# MAGIC
# MAGIC **Always capture source file metadata** in production pipelines:
# MAGIC * Add during initial read
# MAGIC * Store in target table
# MAGIC * Enable troubleshooting and reprocessing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Important Notes:
# MAGIC
# MAGIC * Metadata columns are **pseudo-columns** (not in DataFrame schema)
# MAGIC * Must explicitly select to include them
# MAGIC * Available for file-based sources (CSV, JSON, Parquet, etc.)

# COMMAND ----------

# DBTITLE 1,Demo: Reading File Metadata
# 📌 Demo: Capturing File Metadata for Data Lineage

from pyspark.sql.functions import col, input_file_name, current_timestamp

# Read data with metadata
df_with_metadata = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/workspace/default/sample_data/*.csv")  # Multiple files

# Add metadata columns
df_enriched = df_with_metadata \
    .withColumn("source_file_path", col("_metadata.file_path")) \
    .withColumn("source_file_name", col("_metadata.file_name")) \
    .withColumn("source_file_size", col("_metadata.file_size")) \
    .withColumn("source_file_modified_time", col("_metadata.file_modification_time")) \
    .withColumn("ingestion_timestamp", current_timestamp())

print("✅ Data loaded with file metadata captured")
print("\n📍 Metadata columns added:")
print("  - source_file_path")
print("  - source_file_name")
print("  - source_file_size")
print("  - source_file_modified_time")
print("  - ingestion_timestamp")

# Display with metadata
print("\n📊 Sample data with lineage:")
display(df_enriched.select(
    "*",  # Original columns
    "source_file_name",
    "ingestion_timestamp"
).limit(5))

# COMMAND ----------

# DBTITLE 1,Demo: Alternative - input_file_name()
# 📌 Demo: Using input_file_name() Function
# Alternative approach for simpler use cases

from pyspark.sql.functions import input_file_name, current_timestamp

df = spark.read.format("parquet") \
    .load("/Volumes/workspace/default/sample_data/*.parquet")

# Add source file using input_file_name()
df_with_source = df \
    .withColumn("source_file", input_file_name()) \
    .withColumn("processed_at", current_timestamp())

print("✅ Source file tracked using input_file_name()")
print("\n💡 This function returns the full file path")
print("\n📊 Sample:")
display(df_with_source.select("source_file", "processed_at").limit(3))

# COMMAND ----------

# DBTITLE 1,Demo: File-Level Aggregation
# 📌 Demo: File-Level Aggregation and Analysis

from pyspark.sql.functions import col, count, sum as _sum

# Assuming df_with_metadata from previous cell
df_file_stats = df_with_metadata \
    .withColumn("source_file", col("_metadata.file_name")) \
    .groupBy("source_file") \
    .agg(
        count("*").alias("record_count")
    ) \
    .orderBy(col("record_count").desc())

print("✅ File-level statistics computed")
print("\n📊 Records per source file:")
# display(df_file_stats)

print("\n💡 Use Case: Identify files with data quality issues or unusual patterns")

# COMMAND ----------

# DBTITLE 1,Section 7: File Format Comparison
# MAGIC %md
# MAGIC ## 🔍 Section 7: File Format Comparison
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Comprehensive Format Comparison:
# MAGIC
# MAGIC | Feature | CSV | JSON | Parquet |
# MAGIC |---------|-----|------|--------|
# MAGIC | **Storage Model** | Row-based text | Row-based text | Columnar binary |
# MAGIC | **Schema Storage** | ❌ External only | ❌ External only | ✅ Embedded in file |
# MAGIC | **Human Readable** | ✅ Yes | ✅ Yes | ❌ No (binary) |
# MAGIC | **Compression** | ❌ Poor | ❌ Poor | ✅ Excellent |
# MAGIC | **Read Performance** | 🔴 Slow | 🟡 Moderate | 🟢 Fast |
# MAGIC | **Write Performance** | 🟢 Fast | 🟡 Moderate | 🟡 Moderate |
# MAGIC | **Column Pruning** | ❌ No | ❌ No | ✅ Yes |
# MAGIC | **Predicate Pushdown** | ❌ No | ❌ No | ✅ Yes |
# MAGIC | **Nested Structures** | ❌ No | ✅ Yes | ✅ Yes |
# MAGIC | **Schema Evolution** | ❌ Difficult | ❌ Difficult | ✅ Supported |
# MAGIC | **Splittable** | ✅ Yes | ⚠️ Depends | ✅ Yes |
# MAGIC | **Type Safety** | ❌ Weak | ❌ Weak | ✅ Strong |
# MAGIC | **Storage Size** | 🔴 Large | 🔴 Large | 🟢 Small |
# MAGIC | **Analytics Use Case** | ❌ Poor | ⚠️ OK | ✅ Excellent |
# MAGIC | **Debugging** | ✅ Easy | ✅ Easy | ❌ Harder |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏆 Format Selection Guidelines:
# MAGIC
# MAGIC | Scenario | Recommended Format | Reason |
# MAGIC |----------|-------------------|--------|
# MAGIC | **Production analytics** | 🥇 Parquet / Delta | Performance, compression, schema |
# MAGIC | **Data exchange** | JSON | Interoperability, nested data |
# MAGIC | **Human inspection** | CSV | Readability |
# MAGIC | **Legacy systems** | CSV | Compatibility |
# MAGIC | **Nested/hierarchical** | JSON or Parquet | Structural support |
# MAGIC | **Archival storage** | Parquet | Compression, self-describing |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Migration Path:
# MAGIC
# MAGIC ```
# MAGIC CSV/JSON (raw) → Parquet (processed) → Delta (production)
# MAGIC ```
# MAGIC
# MAGIC **Why Delta as final format?**
# MAGIC * All benefits of Parquet PLUS:
# MAGIC   * ACID transactions
# MAGIC   * Time travel
# MAGIC   * Schema enforcement
# MAGIC   * DML operations (UPDATE/DELETE/MERGE)
# MAGIC   * Audit history
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📈 Performance Example:
# MAGIC
# MAGIC **Same dataset (1 GB raw data):**
# MAGIC
# MAGIC * CSV: 1000 MB, 60 sec read
# MAGIC * JSON: 800 MB, 45 sec read
# MAGIC * Parquet: 200 MB, 5 sec read
# MAGIC * Delta: 200 MB, 5 sec read + ACID + versioning
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Common Mistake:
# MAGIC
# MAGIC 🔴 **Using CSV for production analytics pipelines**
# MAGIC * Slow reads
# MAGIC * No schema enforcement
# MAGIC * No compression benefits
# MAGIC * Type inference issues
# MAGIC
# MAGIC ➡️ **Solution**: Convert to Parquet/Delta during ingestion

# COMMAND ----------

# DBTITLE 1,Section 8: Hands-on Ingestion Pipeline
# MAGIC %md
# MAGIC ## 🔧 Section 8: Hands-On Ingestion Pipeline
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Objective:
# MAGIC
# MAGIC Build a practical ingestion pipeline that:
# MAGIC 1. Reads data from multiple formats (CSV, JSON, Parquet)
# MAGIC 2. Applies explicit schema
# MAGIC 3. Adds metadata tracking
# MAGIC 4. Writes to Delta Lake
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📝 Pipeline Architecture:
# MAGIC
# MAGIC ```
# MAGIC ┌───────────────────┐
# MAGIC │  Source Files      │
# MAGIC │  (CSV/JSON/Parquet)│
# MAGIC └────────┬─────────┘
# MAGIC          │
# MAGIC          ↓
# MAGIC ┌────────┴─────────┐
# MAGIC │  Read with Schema  │
# MAGIC │  + Validation      │
# MAGIC └────────┬─────────┘
# MAGIC          │
# MAGIC          ↓
# MAGIC ┌────────┴─────────┐
# MAGIC │  Add Metadata      │
# MAGIC │  (Lineage)         │
# MAGIC └────────┬─────────┘
# MAGIC          │
# MAGIC          ↓
# MAGIC ┌────────┴─────────┐
# MAGIC │  Write to Delta    │
# MAGIC │  (Production)      │
# MAGIC └───────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛠️ Components:
# MAGIC
# MAGIC 1. **Schema Definition** — Explicit data contract
# MAGIC 2. **Data Validation** — Type enforcement
# MAGIC 3. **Metadata Enrichment** — Source tracking
# MAGIC 4. **Delta Write** — ACID-compliant storage

# COMMAND ----------

# DBTITLE 1,Pipeline Step 1: Define Schema
# 📌 STEP 1: Define Explicit Schema

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, DateType, TimestampType
)

# Define schema for incoming data
ingestion_schema = StructType([
    StructField("transaction_id", IntegerType(), nullable=False),
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("product_id", IntegerType(), nullable=False),
    StructField("product_name", StringType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=False),
    StructField("unit_price", DoubleType(), nullable=False),
    StructField("total_amount", DoubleType(), nullable=False),
    StructField("transaction_date", DateType(), nullable=False),
    StructField("status", StringType(), nullable=True)
])

print("✅ Schema defined for ingestion pipeline")
print("\n📊 Schema structure:")
for field in ingestion_schema.fields:
    nullable_str = "NULL" if field.nullable else "NOT NULL"
    print(f"  {field.name}: {field.dataType.simpleString()} [{nullable_str}]")

print("\n💡 This schema will be applied to all incoming files")

# COMMAND ----------

# DBTITLE 1,Pipeline Step 2: Read Multiple Formats
# 📌 STEP 2: Read Data from Multiple File Formats

from pyspark.sql.functions import lit

# Read CSV with explicit schema
df_csv_source = spark.read.format("csv") \
    .option("header", "true") \
    .schema(ingestion_schema) \
    .load("/Volumes/workspace/default/raw_data/csv/transactions.csv") \
    .withColumn("source_format", lit("CSV"))

print("✅ CSV data loaded with explicit schema")

# Read JSON with explicit schema
df_json_source = spark.read.format("json") \
    .schema(ingestion_schema) \
    .load("/Volumes/workspace/default/raw_data/json/transactions.json") \
    .withColumn("source_format", lit("JSON"))

print("✅ JSON data loaded with explicit schema")

# Read Parquet (schema embedded, but we validate it matches)
df_parquet_source = spark.read.format("parquet") \
    .load("/Volumes/workspace/default/raw_data/parquet/transactions.parquet") \
    .withColumn("source_format", lit("PARQUET"))

print("✅ Parquet data loaded")

print("\n⚡ All formats loaded with consistent schema enforcement")
print(f"CSV records: {df_csv_source.count() if df_csv_source else 0}")
print(f"JSON records: {df_json_source.count() if df_json_source else 0}")
print(f"Parquet records: {df_parquet_source.count() if df_parquet_source else 0}")

# COMMAND ----------

# DBTITLE 1,Pipeline Step 3: Union All Sources
# 📌 STEP 3: Union All Sources into Single DataFrame

from pyspark.sql.functions import col, current_timestamp

# Union all sources (schemas must match)
df_unified = df_csv_source \
    .unionAll(df_json_source) \
    .unionAll(df_parquet_source)

print("✅ All sources unified into single DataFrame")
print(f"\n📊 Total records: {df_unified.count()}")
print(f"Schema consistency: {len(df_unified.schema.fields)} columns")

# Add metadata columns
df_enriched = df_unified \
    .withColumn("source_file_path", col("_metadata.file_path")) \
    .withColumn("source_file_name", col("_metadata.file_name")) \
    .withColumn("ingestion_timestamp", current_timestamp())

print("✅ Metadata columns added for lineage tracking")

# Display sample
print("\n📊 Sample unified data:")
display(df_enriched.select(
    "transaction_id",
    "customer_id",
    "total_amount",
    "source_format",
    "source_file_name",
    "ingestion_timestamp"
).limit(10))

# COMMAND ----------

# DBTITLE 1,Pipeline Step 4: Write to Delta Lake
# 📌 STEP 4: Write to Delta Lake (Production Storage)

# Define output path
output_path = "/Volumes/workspace/default/processed_data/transactions_delta"

# Write as Delta table
df_enriched.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(output_path)

print("✅ Data written to Delta Lake")
print(f"\n📍 Output path: {output_path}")
print("\n✨ Benefits:")
print("  ✅ ACID transactions")
print("  ✅ Schema enforcement")
print("  ✅ Time travel")
print("  ✅ Audit history")
print("  ✅ DML operations (UPDATE/DELETE/MERGE)")

# Read back to verify
df_verify = spark.read.format("delta").load(output_path)
print(f"\n🔍 Verification: {df_verify.count()} records in Delta table")

# COMMAND ----------

# DBTITLE 1,Pipeline Step 5: Create Unity Catalog Table
# 📌 STEP 5: Register as Unity Catalog Managed Table

# Create or replace managed Delta table in Unity Catalog
df_enriched.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("main.default.transactions_processed")

print("✅ Delta table registered in Unity Catalog")
print("\n🏷️ Table: main.default.transactions_processed")
print("\n🔑 Accessible via:")
print("  - SQL: SELECT * FROM main.default.transactions_processed")
print("  - Python: spark.table('main.default.transactions_processed')")
print("  - Unity Catalog governance and access controls")

# Query the table
df_table = spark.table("main.default.transactions_processed")
print(f"\n📊 Records in managed table: {df_table.count()}")

# COMMAND ----------

# DBTITLE 1,Section 9: End-to-End Production Pipeline
# MAGIC %md
# MAGIC ## 🚀 Section 9: End-to-End Production Ingestion Pipeline
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Production Pipeline Pattern:
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────┐
# MAGIC │  Raw Files (Landing)   │  ← CSV/JSON/Parquet
# MAGIC │  /Volumes/.../raw/     │
# MAGIC └──────────┬───────────┘
# MAGIC            │
# MAGIC            ↓ [Read with Schema]
# MAGIC            │
# MAGIC ┌──────────┴───────────┐
# MAGIC │  Bronze Layer         │  ← Raw + Metadata
# MAGIC │  (Delta)              │
# MAGIC └──────────┬───────────┘
# MAGIC            │
# MAGIC            ↓ [Cleanse + Validate]
# MAGIC            │
# MAGIC ┌──────────┴───────────┐
# MAGIC │  Silver Layer         │  ← Cleaned + Conformed
# MAGIC │  (Delta)              │
# MAGIC └──────────┬───────────┘
# MAGIC            │
# MAGIC            ↓ [Aggregate + Transform]
# MAGIC            │
# MAGIC ┌──────────┴───────────┐
# MAGIC │  Gold Layer           │  ← Business Aggregates
# MAGIC │  (Delta)              │
# MAGIC └──────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Medallion Architecture:
# MAGIC
# MAGIC **Bronze (Raw)**
# MAGIC * Exact copy of source data
# MAGIC * Minimal transformations
# MAGIC * Add metadata (source, timestamp)
# MAGIC * Store as Delta
# MAGIC
# MAGIC **Silver (Cleansed)**
# MAGIC * Data quality checks
# MAGIC * Type conversions
# MAGIC * Deduplication
# MAGIC * Standardization
# MAGIC
# MAGIC **Gold (Business)**
# MAGIC * Aggregations
# MAGIC * Business logic
# MAGIC * Denormalized for analytics
# MAGIC * Optimized for consumption
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛠️ Production Best Practices:
# MAGIC
# MAGIC ✅ **Schema Management**
# MAGIC * Define explicit schema
# MAGIC * Version schema definitions
# MAGIC * Validate on read
# MAGIC
# MAGIC ✅ **Error Handling**
# MAGIC * Use `.option("mode", "PERMISSIVE")` to capture bad records
# MAGIC * Add `_corrupt_record` column
# MAGIC * Log and alert on failures
# MAGIC
# MAGIC ✅ **Metadata Tracking**
# MAGIC * Capture source file path
# MAGIC * Add ingestion timestamp
# MAGIC * Track processing status
# MAGIC
# MAGIC ✅ **Performance**
# MAGIC * Prefer Parquet/Delta over CSV/JSON
# MAGIC * Partition large tables
# MAGIC * Optimize file sizes (128MB - 1GB)
# MAGIC
# MAGIC ✅ **Monitoring**
# MAGIC * Track ingestion metrics
# MAGIC * Alert on schema violations
# MAGIC * Monitor file counts and sizes

# COMMAND ----------

# DBTITLE 1,Complete Production Pipeline Function
# 📌 Complete Production Ingestion Pipeline Function

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, current_timestamp, lit

def ingest_to_bronze(
    source_path: str,
    source_format: str,
    schema: StructType,
    target_table: str,
    read_options: dict = None
) -> DataFrame:
    """
    Production ingestion pipeline: Read raw files and write to Bronze layer.
    
    Args:
        source_path: Path to source files (Unity Catalog Volume)
        source_format: File format (csv, json, parquet)
        schema: Explicit schema definition
        target_table: Target Unity Catalog table (catalog.schema.table)
        read_options: Optional read options dict
    
    Returns:
        DataFrame written to Bronze
    """
    
    print(f"🚀 Starting ingestion pipeline for {source_format.upper()}")
    print(f"Source: {source_path}")
    print(f"Target: {target_table}")
    
    # Default options
    options = read_options or {}
    
    # Read with explicit schema
    reader = spark.read.format(source_format).schema(schema)
    
    # Apply options
    for key, value in options.items():
        reader = reader.option(key, value)
    
    # Load data
    df = reader.load(source_path)
    
    print(f"✅ Data loaded: {df.count()} records")
    
    # Add metadata columns
    df_enriched = df \
        .withColumn("source_file_path", col("_metadata.file_path")) \
        .withColumn("source_file_name", col("_metadata.file_name")) \
        .withColumn("source_format", lit(source_format.upper())) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("processing_status", lit("INGESTED"))
    
    print("✅ Metadata columns added")
    
    # Write to Delta (Bronze layer)
    df_enriched.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "false") \
        .saveAsTable(target_table)
    
    print(f"✅ Data written to Bronze: {target_table}")
    
    return df_enriched

# Example usage:
print("✅ Production ingestion function defined")
print("\n📝 Usage example:")
print("""
df_bronze = ingest_to_bronze(
    source_path="/Volumes/workspace/default/raw/transactions.csv",
    source_format="csv",
    schema=ingestion_schema,
    target_table="main.bronze.transactions",
    read_options={"header": "true"}
)
""")

# COMMAND ----------

# DBTITLE 1,Genie Code Agent Examples
# MAGIC %md
# MAGIC ## 🧞 Genie Code Agent — Prompt Examples
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💬 How to Use Genie Code for Data Ingestion:
# MAGIC
# MAGIC Genie Code can help you generate ingestion code, optimize pipelines, and troubleshoot issues.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Example Prompts:
# MAGIC
# MAGIC #### 🟢 Basic Ingestion:
# MAGIC
# MAGIC **Prompt:**
# MAGIC > *"Generate code to read a CSV file from `/Volumes/workspace/default/data/sales.csv` with explicit schema including columns: order_id (int), customer_name (string), amount (double), order_date (date)"*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 🟢 Nested JSON:
# MAGIC
# MAGIC **Prompt:**
# MAGIC > *"Read a nested JSON file from Volume with multi-line format. Extract user.id, user.name, and explode the orders array to create one row per order"*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 🟢 Parquet Optimization:
# MAGIC
# MAGIC **Prompt:**
# MAGIC > *"Read Parquet files from `/Volumes/workspace/default/transactions/` and select only transaction_id, amount, and date columns. Add source file metadata and write to Delta table main.bronze.transactions"*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 🟢 Schema Definition:
# MAGIC
# MAGIC **Prompt:**
# MAGIC > *"Define an explicit schema for a customer table with: customer_id (integer, not null), name (string), email (string), signup_date (date), account_balance (double). Then read from CSV with this schema."*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 🟢 Multi-Format Pipeline:
# MAGIC
# MAGIC **Prompt:**
# MAGIC > *"Create a pipeline that reads CSV, JSON, and Parquet files from different Volume paths, applies the same schema to all, unions them together, adds source format and file metadata, and writes to a Delta table"*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 🟢 Error Handling:
# MAGIC
# MAGIC **Prompt:**
# MAGIC > *"Read CSV with PERMISSIVE mode to capture corrupt records. Show me how to filter out bad records and log them to a separate error table"*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 🟢 Schema Evolution:
# MAGIC
# MAGIC **Prompt:**
# MAGIC > *"Help me handle schema evolution when ingesting Parquet files. Enable mergeSchema option and explain how to add new columns without breaking existing data"*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 🟢 Performance Optimization:
# MAGIC
# MAGIC **Prompt:**
# MAGIC > *"My CSV ingestion is slow. Show me how to convert CSV to Parquet with optimal partitioning and compression. Compare the performance difference."*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Key Tips for Genie Code:
# MAGIC
# MAGIC * ✅ **Be specific** about file paths and formats
# MAGIC * ✅ **Mention Unity Catalog** Volumes for data access
# MAGIC * ✅ **Specify schema requirements** explicitly
# MAGIC * ✅ **Ask for metadata tracking** if needed
# MAGIC * ✅ **Request performance optimizations** when relevant

# COMMAND ----------

# DBTITLE 1,Best Practices Summary
# MAGIC %md
# MAGIC ## 🏆 Data Ingestion Best Practices Summary
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟢 DO — Recommended Practices:
# MAGIC
# MAGIC ✅ **Schema Management**
# MAGIC * Always define explicit schema for production pipelines
# MAGIC * Document schema as data contract
# MAGIC * Version schema definitions
# MAGIC * Validate schema on read
# MAGIC
# MAGIC ✅ **File Format Selection**
# MAGIC * Use Parquet/Delta for analytics workloads
# MAGIC * Prefer columnar formats for production
# MAGIC * Convert CSV/JSON to Parquet early in pipeline
# MAGIC
# MAGIC ✅ **Metadata Tracking**
# MAGIC * Capture source file path and name
# MAGIC * Add ingestion timestamp
# MAGIC * Track file modification time
# MAGIC * Store processing status
# MAGIC
# MAGIC ✅ **Error Handling**
# MAGIC * Use PERMISSIVE mode to capture bad records
# MAGIC * Log corrupt records separately
# MAGIC * Implement retry logic
# MAGIC * Alert on schema violations
# MAGIC
# MAGIC ✅ **Performance**
# MAGIC * Read only required columns (column pruning)
# MAGIC * Apply filters early (predicate pushdown)
# MAGIC * Partition large tables appropriately
# MAGIC * Optimize file sizes (128MB - 1GB)
# MAGIC
# MAGIC ✅ **Unity Catalog**
# MAGIC * Use Volumes for file storage
# MAGIC * Register tables in Unity Catalog
# MAGIC * Apply governance and access controls
# MAGIC * Enable audit logging
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 DON'T — Anti-Patterns:
# MAGIC
# MAGIC ❌ **Schema Inference in Production**
# MAGIC * Doubles I/O operations
# MAGIC * Unreliable with dirty data
# MAGIC * Non-deterministic results
# MAGIC * Type inference issues
# MAGIC
# MAGIC ❌ **CSV for Analytics**
# MAGIC * Slow read performance
# MAGIC * No column pruning
# MAGIC * Poor compression
# MAGIC * No predicate pushdown
# MAGIC
# MAGIC ❌ **Missing Metadata**
# MAGIC * Cannot trace data lineage
# MAGIC * Difficult to debug issues
# MAGIC * No audit trail
# MAGIC * Cannot reprocess specific files
# MAGIC
# MAGIC ❌ **No Error Handling**
# MAGIC * Silent data loss
# MAGIC * Production failures
# MAGIC * No visibility into issues
# MAGIC
# MAGIC ❌ **Local Storage**
# MAGIC * Not accessible across cluster
# MAGIC * No ACID guarantees
# MAGIC * No governance
# MAGIC * Data loss risk
# MAGIC
# MAGIC ❌ **Using RDDs**
# MAGIC * No optimizations
# MAGIC * More code to write
# MAGIC * Harder to maintain
# MAGIC * Not recommended for new code
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛣️ Production Ingestion Checklist:
# MAGIC
# MAGIC - [ ] Explicit schema defined
# MAGIC - [ ] Source file metadata captured
# MAGIC - [ ] Error handling implemented
# MAGIC - [ ] Data validation applied
# MAGIC - [ ] Write to Delta format
# MAGIC - [ ] Register in Unity Catalog
# MAGIC - [ ] Partitioning strategy defined
# MAGIC - [ ] Monitoring and alerts configured
# MAGIC - [ ] Documentation updated
# MAGIC - [ ] Testing completed

# COMMAND ----------

# DBTITLE 1,Interview Questions
# MAGIC %md
# MAGIC ## 🎯 Interview Questions — Spark Read API
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📝 Technical Interview Questions:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Question 1:** What is the difference between schema inference and explicit schema definition? When would you use each?
# MAGIC
# MAGIC **Answer:**
# MAGIC * **Schema Inference:** Spark scans data to automatically detect column types. Use for exploration/development only.
# MAGIC   * Pros: Quick for prototyping
# MAGIC   * Cons: Double I/O, unreliable, expensive on large files
# MAGIC * **Explicit Schema:** Define schema upfront using StructType/StructField. **Always use in production.**
# MAGIC   * Pros: Single I/O, deterministic, type safety, better performance
# MAGIC   * Cons: Requires upfront definition (worth it)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Question 2:** Explain the difference between CSV, JSON, and Parquet file formats. Which would you choose for a production analytics pipeline?
# MAGIC
# MAGIC **Answer:**
# MAGIC * **CSV:** Row-based text format. Human-readable but slow, no schema, no compression benefits.
# MAGIC * **JSON:** Row-based text format with nested structure support. Semi-structured but inefficient for analytics.
# MAGIC * **Parquet:** Columnar binary format with embedded schema, excellent compression, and optimizations (column pruning, predicate pushdown).
# MAGIC * **Choice:** **Parquet (or Delta)** for production analytics due to performance, compression, and schema enforcement.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Question 3:** What is column pruning and predicate pushdown? Which file formats support these optimizations?
# MAGIC
# MAGIC **Answer:**
# MAGIC * **Column Pruning:** Reading only the required columns from storage, skipping unnecessary columns.
# MAGIC * **Predicate Pushdown:** Applying filter conditions at the storage layer before loading data into memory.
# MAGIC * **Support:** Parquet and Delta support both optimizations. CSV and JSON do NOT (must read entire rows).
# MAGIC * **Benefit:** Significant performance improvement — 10-100x faster for selective queries.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Question 4:** How do you track data lineage in Spark ingestion pipelines?
# MAGIC
# MAGIC **Answer:**
# MAGIC * Use `_metadata` pseudo-columns:
# MAGIC   * `_metadata.file_path` — Full source file path
# MAGIC   * `_metadata.file_name` — File name only
# MAGIC   * `_metadata.file_size` — File size
# MAGIC   * `_metadata.file_modification_time` — Last modified time
# MAGIC * Add these as regular columns during ingestion:
# MAGIC   ```python
# MAGIC   df.withColumn("source_file", col("_metadata.file_path"))
# MAGIC   ```
# MAGIC * Also add ingestion timestamp for complete audit trail.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Question 5:** What are the key differences between reading a single file vs. multiple files in Spark?
# MAGIC
# MAGIC **Answer:**
# MAGIC * **Single file:** `load("path/to/file.csv")`
# MAGIC * **Multiple files:** `load("path/to/directory/")` or `load("path/to/*.csv")`
# MAGIC * **Behavior:** Spark automatically parallelizes reads across multiple files, creating one partition per file (or more).
# MAGIC * **Schema:** All files must have compatible schema (same columns, types).
# MAGIC * **Metadata:** Use `_metadata.file_name` to distinguish records from different files.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Question 6:** How would you handle schema evolution when ingesting Parquet files?
# MAGIC
# MAGIC **Answer:**
# MAGIC * Use `.option("mergeSchema", "true")` when reading:
# MAGIC   ```python
# MAGIC   df = spark.read.format("parquet") \
# MAGIC       .option("mergeSchema", "true") \
# MAGIC       .load("path")
# MAGIC   ```
# MAGIC * Spark will merge schemas from all Parquet files
# MAGIC * New columns appear as NULL for older files
# MAGIC * **Caution:** Can be expensive; prefer explicit schema management
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Question 7:** What is the PERMISSIVE mode in Spark read options? When would you use it?
# MAGIC
# MAGIC **Answer:**
# MAGIC * **PERMISSIVE mode** (default): Puts corrupt/unparseable records into a special column `_corrupt_record`, keeps processing
# MAGIC * **DROPMALFORMED mode:** Silently drops bad records
# MAGIC * **FAILFAST mode:** Throws exception on first bad record
# MAGIC * **Use case:** Use PERMISSIVE in production to capture bad records for later analysis without failing the entire job.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Question 8:** Why is Parquet faster than CSV for analytical queries?
# MAGIC
# MAGIC **Answer:**
# MAGIC * **Columnar storage:** Parquet stores data by column, CSV by row
# MAGIC * **Column pruning:** Parquet can read only needed columns; CSV must read entire rows
# MAGIC * **Compression:** Parquet uses columnar compression (better ratios); CSV has poor compression
# MAGIC * **Encoding:** Parquet uses efficient encoding (dictionary, RLE); CSV is plain text
# MAGIC * **Predicate pushdown:** Parquet can filter at file level; CSV cannot
# MAGIC * **Result:** Parquet is typically 10-100x faster and 5-10x smaller
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Question 9:** What is the difference between `.format("parquet").load()` and `.parquet()` in Spark?
# MAGIC
# MAGIC **Answer:**
# MAGIC * **Functionally identical** — both read Parquet files
# MAGIC * `.parquet()` is shorthand for `.format("parquet").load()`
# MAGIC * Same for `.csv()`, `.json()`, `.orc()`, etc.
# MAGIC * **Recommendation:** Use whichever is more readable for your team
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Question 10:** How would you design a production ingestion pipeline following the Medallion Architecture?
# MAGIC
# MAGIC **Answer:**
# MAGIC * **Bronze Layer:** Raw data with minimal transformation
# MAGIC   * Read with explicit schema
# MAGIC   * Add source metadata (file path, timestamp)
# MAGIC   * Write as Delta
# MAGIC * **Silver Layer:** Cleansed and validated data
# MAGIC   * Data quality checks
# MAGIC   * Type conversions
# MAGIC   * Deduplication
# MAGIC   * Standardization
# MAGIC * **Gold Layer:** Business-ready aggregates
# MAGIC   * Aggregations
# MAGIC   * Joins with dimensions
# MAGIC   * Denormalized for analytics
# MAGIC * **Benefits:** Clear separation of concerns, incremental quality improvement, easy to debug
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Common Mistakes to Avoid:
# MAGIC
# MAGIC 1. 🔴 **Using schema inference in production pipelines**
# MAGIC 2. 🔴 **Not capturing source file metadata**
# MAGIC 3. 🔴 **Using CSV for large-scale analytics**
# MAGIC 4. 🔴 **Ignoring error handling (PERMISSIVE mode)**
# MAGIC 5. 🔴 **Not defining nullable constraints properly**
# MAGIC 6. 🔴 **Reading all columns when only few are needed**
# MAGIC 7. 🔴 **Not using Unity Catalog Volumes for file storage**
# MAGIC 8. 🔴 **Mixing different schema files without validation**

# COMMAND ----------

# DBTITLE 1,Final Summary & Next Steps
# MAGIC %md
# MAGIC ## 🏁 Final Summary — Phase 3 Day 14
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📚 Key Learnings:
# MAGIC
# MAGIC ✅ **Spark Read API**
# MAGIC * Unified interface for ingesting multiple file formats
# MAGIC * Format-agnostic with `.format("csv/json/parquet")`
# MAGIC * Lazy evaluation — no data read until action
# MAGIC
# MAGIC ✅ **File Formats**
# MAGIC * **CSV:** Human-readable, slow, no schema, poor compression
# MAGIC * **JSON:** Semi-structured, nested support, text-based
# MAGIC * **Parquet:** Columnar, fast, embedded schema, excellent compression
# MAGIC * **Recommendation:** Parquet/Delta for production
# MAGIC
# MAGIC ✅ **Schema Handling**
# MAGIC * **Schema inference:** Quick but unreliable, expensive (double I/O)
# MAGIC * **Explicit schema:** Production best practice, deterministic, type-safe
# MAGIC * Always define schema in production pipelines
# MAGIC
# MAGIC ✅ **Metadata Tracking**
# MAGIC * Use `_metadata.file_path` and `_metadata.file_name`
# MAGIC * Add ingestion timestamps
# MAGIC * Essential for data lineage and debugging
# MAGIC
# MAGIC ✅ **Performance Optimizations**
# MAGIC * Column pruning (Parquet only)
# MAGIC * Predicate pushdown (Parquet only)
# MAGIC * Prefer columnar formats for analytics
# MAGIC
# MAGIC ✅ **Production Patterns**
# MAGIC * Medallion Architecture (Bronze/Silver/Gold)
# MAGIC * Explicit schema enforcement
# MAGIC * Error handling with PERMISSIVE mode
# MAGIC * Unity Catalog integration
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Next Steps:
# MAGIC
# MAGIC 1. **Practice:** Create your own ingestion pipelines with different formats
# MAGIC 2. **Experiment:** Compare CSV vs Parquet performance on your data
# MAGIC 3. **Build:** Implement Medallion Architecture for a real dataset
# MAGIC 4. **Learn Next:**
# MAGIC    * Auto Loader for incremental ingestion
# MAGIC    * Schema evolution strategies
# MAGIC    * Streaming ingestion patterns
# MAGIC    * Delta Lake advanced features
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Additional Resources:
# MAGIC
# MAGIC * Databricks Documentation: Read API
# MAGIC * Databricks Documentation: File Formats
# MAGIC * Databricks Documentation: Unity Catalog Volumes
# MAGIC * Databricks Documentation: Delta Lake
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✨ Congratulations!
# MAGIC
# MAGIC You've completed **Phase 3 Day 14** — Spark Read API!
# MAGIC
# MAGIC You now understand:
# MAGIC * How to read multiple file formats
# MAGIC * Schema handling best practices
# MAGIC * Production ingestion patterns
# MAGIC * Performance optimization techniques
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👏 Well Done!
# MAGIC
# MAGIC **@TRRaveendra** — Keep building! 🚀