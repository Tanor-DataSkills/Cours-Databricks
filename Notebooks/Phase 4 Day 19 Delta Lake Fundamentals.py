# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # 🧊 Data Engineering Training — Phase 4 Day 19  
# MAGIC ## ⚙️ Delta Lake Fundamentals: ACID & Architecture  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC * ACID Transactions in Delta Lake  
# MAGIC * Delta Lake Architecture  
# MAGIC * Delta vs Parquet  
# MAGIC * Transaction Log (_delta_log)  
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
# MAGIC Understand how Delta Lake provides ACID guarantees on data lakes and how its architecture enables reliable, scalable data pipelines.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚠️ IMPORTANT ENGINEERING CONSTRAINTS:
# MAGIC
# MAGIC * ✅ Use Databricks Serverless Compute
# MAGIC * ❌ DO NOT use RDDs (DataFrame API only)
# MAGIC * ❌ DO NOT use cache() / persist()
# MAGIC * ❌ DO NOT use /tmp or local storage
# MAGIC * ✅ Use Unity Catalog Volumes and managed tables
# MAGIC * ✅ Follow Delta-first architecture

# COMMAND ----------

# DBTITLE 1,Section 1: What is Delta Lake?
# MAGIC %md
# MAGIC # 📖 Section 1: What is Delta Lake?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 👶 ELI5 (Explain Like I'm 5):
# MAGIC Imagine you have a toy box (data lake) where you throw all your toys. Sometimes toys get lost, sometimes you put the same toy twice, and sometimes your sibling takes a toy while you're organizing. **Delta Lake** is like having a smart toy box that:
# MAGIC * Remembers every toy you add or remove
# MAGIC * Doesn't let toys get lost
# MAGIC * Keeps a diary of all changes
# MAGIC * Lets only one person organize at a time
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### **The Problem with Traditional Data Lakes:**
# MAGIC
# MAGIC | Challenge | Impact |
# MAGIC |-----------|--------|
# MAGIC | **No ACID Guarantees** | Data corruption during failures |
# MAGIC | **No Schema Enforcement** | Inconsistent data quality |
# MAGIC | **Difficult Updates/Deletes** | Full table rewrites required |
# MAGIC | **No Time Travel** | Cannot audit historical data |
# MAGIC | **Concurrency Issues** | Race conditions in multi-writer scenarios |
# MAGIC | **Small File Problem** | Poor query performance |
# MAGIC
# MAGIC ### **Delta Lake Solution:**
# MAGIC
# MAGIC Delta Lake is an **open-source storage layer** that brings **ACID transactions** to Apache Spark and big data workloads. It runs on top of your existing data lake (S3, ADLS, GCS) and provides:
# MAGIC
# MAGIC * ✅ **ACID Transactions**: Atomicity, Consistency, Isolation, Durability
# MAGIC * ✅ **Schema Enforcement & Evolution**: Prevent bad data from entering
# MAGIC * ✅ **Time Travel**: Access historical versions of data
# MAGIC * ✅ **Unified Batch & Streaming**: Single API for both
# MAGIC * ✅ **Scalable Metadata**: Transaction log handles petabyte-scale tables
# MAGIC * ✅ **Audit History**: Complete lineage of all changes
# MAGIC
# MAGIC ### **Delta Lake = Data Lake + ACID + Governance + Performance**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏭 Lakehouse Architecture:
# MAGIC
# MAGIC ```
# MAGIC ┌───────────────────────────────────────┐
# MAGIC │         BI / ML / Analytics          │
# MAGIC │      (Direct queries on Delta)       │
# MAGIC ├───────────────────────────────────────┤
# MAGIC │           DELTA LAKE LAYER           │
# MAGIC │  (ACID + Schema + Time Travel)       │
# MAGIC ├───────────────────────────────────────┤
# MAGIC │        Cloud Object Storage          │
# MAGIC │      (S3 / ADLS / GCS / DBFS)        │
# MAGIC └───────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Key Insight**: Delta Lake transforms a data lake into a Lakehouse by adding a transaction layer on top of cheap object storage.

# COMMAND ----------

# DBTITLE 1,Section 2: ACID Transactions
# MAGIC %md
# MAGIC # 🔒 Section 2: ACID Transactions in Delta Lake
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 👶 ELI5 (Explain Like I'm 5):
# MAGIC ACID is like having super strict rules for your toy box:
# MAGIC * **A**tomicity: Either all toys go in, or none do (no half-done work)
# MAGIC * **C**onsistency: Toy box always makes sense (no broken rules)
# MAGIC * **I**solation: When you organize toys, nobody else can mess with them
# MAGIC * **D**urability: Once toys are in, they stay in (even if power goes out)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ ACID Properties Explained:
# MAGIC
# MAGIC ### 🎯 **A = Atomicity** (All or Nothing)
# MAGIC
# MAGIC **Definition**: A transaction either completes fully or not at all. No partial writes.
# MAGIC
# MAGIC **Example Scenario**:
# MAGIC ```
# MAGIC You're inserting 1 million records into a Delta table.
# MAGIC Halfway through, the cluster crashes.
# MAGIC
# MAGIC With Parquet: ❌ You get 500K records (corrupted state)
# MAGIC With Delta:   ✅ You get 0 records (transaction rolled back)
# MAGIC ```
# MAGIC
# MAGIC **How Delta Does It**: Uses transaction log to track commits. If commit file isn't written, the operation never happened.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 **C = Consistency** (Data Integrity)
# MAGIC
# MAGIC **Definition**: Data always moves from one valid state to another. Schema and constraints are enforced.
# MAGIC
# MAGIC **Example Scenario**:
# MAGIC ```python
# MAGIC # Schema: {id: INT, name: STRING, age: INT}
# MAGIC
# MAGIC # Attempt to insert invalid data
# MAGIC df_bad = spark.createDataFrame([(1, "Alice", "twenty")], ["id", "name", "age"])
# MAGIC df_bad.write.format("delta").mode("append").save("path") # ❌ FAILS
# MAGIC
# MAGIC # Delta enforces schema - prevents data quality issues
# MAGIC ```
# MAGIC
# MAGIC **How Delta Does It**: Schema enforcement and evolution. Rejects writes that violate schema unless explicitly allowed.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 **I = Isolation** (Concurrent Safety)
# MAGIC
# MAGIC **Definition**: Concurrent operations don't interfere with each other. Readers see consistent snapshots.
# MAGIC
# MAGIC **Example Scenario**:
# MAGIC ```
# MAGIC Writer A: Updating 10M records
# MAGIC Reader B: Running analytics query
# MAGIC
# MAGIC With Parquet: ❌ Reader sees partial updates (dirty reads)
# MAGIC With Delta:   ✅ Reader sees snapshot from before write started
# MAGIC ```
# MAGIC
# MAGIC **How Delta Does It**: Optimistic concurrency control using transaction log. Each operation gets a version number.
# MAGIC
# MAGIC **Isolation Levels**:
# MAGIC * **Snapshot Isolation**: Default in Delta Lake
# MAGIC * **Serializable Isolation**: Available for critical operations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 **D = Durability** (Guaranteed Persistence)
# MAGIC
# MAGIC **Definition**: Once a transaction is committed, it's permanent (even after system failure).
# MAGIC
# MAGIC **Example Scenario**:
# MAGIC ```
# MAGIC You commit a critical financial transaction.
# MAGIC Immediately after, datacenter has power outage.
# MAGIC
# MAGIC With Parquet: ❌ Data might be lost (no guarantees)
# MAGIC With Delta:   ✅ Data is durable (written to object storage + log)
# MAGIC ```
# MAGIC
# MAGIC **How Delta Does It**: Two-phase commit:
# MAGIC 1. Write data files to object storage
# MAGIC 2. Write commit entry to transaction log
# MAGIC 3. Only after both succeed is transaction visible
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛡️ ACID Benefits in Real Pipelines:
# MAGIC
# MAGIC | Operation | Without ACID | With Delta ACID |
# MAGIC |-----------|--------------|----------------|
# MAGIC | **Insert 1M rows** | Partial writes on failure | All or nothing |
# MAGIC | **Update customer data** | Must rewrite entire table | In-place updates |
# MAGIC | **Delete GDPR records** | Copy all non-deleted data | Efficient deletes |
# MAGIC | **Concurrent reads/writes** | Data corruption risk | Safe concurrent access |
# MAGIC | **Schema changes** | Manual validation needed | Automatic enforcement |
# MAGIC | **Audit trail** | External logging required | Built-in transaction log |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Key Insight:
# MAGIC
# MAGIC **Traditional Data Lake**: 
# MAGIC > "We wrote some data... probably... maybe... check back later?"
# MAGIC
# MAGIC **Delta Lake**: 
# MAGIC > "Transaction 42 committed at 2026-04-21 20:35:00 UTC. 1,000,000 records inserted. Schema validated. Available for queries now."

# COMMAND ----------

# DBTITLE 1,Section 3: Delta vs Parquet
# MAGIC %md
# MAGIC # ⚖️ Section 3: Delta Lake vs Parquet
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 👶 ELI5 (Explain Like I'm 5):
# MAGIC **Parquet** = A box that stores toys efficiently (compressed)
# MAGIC **Delta** = A smart box that stores toys efficiently AND remembers every change, checks toys are correct, and keeps a diary
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Comprehensive Comparison:
# MAGIC
# MAGIC | Feature | Parquet | Delta Lake |
# MAGIC |---------|---------|------------|
# MAGIC | **File Format** | Columnar storage format | Parquet files + Transaction log |
# MAGIC | **ACID Transactions** | ❌ No | ✅ Yes (Full ACID) |
# MAGIC | **Schema Enforcement** | ❌ No | ✅ Yes (prevents bad data) |
# MAGIC | **Schema Evolution** | ⚠️ Manual | ✅ Automatic (with options) |
# MAGIC | **Time Travel** | ❌ No | ✅ Yes (query historical versions) |
# MAGIC | **Updates** | ❌ Full table rewrite | ✅ Efficient in-place updates |
# MAGIC | **Deletes** | ❌ Full table rewrite | ✅ Efficient deletes (MERGE, DELETE) |
# MAGIC | **Upserts (MERGE)** | ❌ Not supported | ✅ Native MERGE support |
# MAGIC | **Concurrent Writes** | ❌ Data corruption risk | ✅ Safe (optimistic concurrency) |
# MAGIC | **Streaming Support** | ⚠️ Basic | ✅ Unified batch + streaming |
# MAGIC | **Data Versioning** | ❌ No | ✅ Every write creates new version |
# MAGIC | **Audit Trail** | ❌ No | ✅ Complete history in _delta_log |
# MAGIC | **Small File Handling** | ❌ Manual compaction | ✅ Auto-optimize + ZORDER |
# MAGIC | **Indexing** | ❌ None | ✅ Data skipping via stats |
# MAGIC | **Read Performance** | ✅ Excellent | ✅ Excellent (+ optimizations) |
# MAGIC | **Write Performance** | ✅ Fast | ✅ Fast (+ transactional overhead) |
# MAGIC | **Metadata Size** | ⚠️ Can grow large | ✅ Checkpoint mechanism |
# MAGIC | **Use Case** | Read-only analytical data | Production data pipelines |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔍 Detailed Insights:
# MAGIC
# MAGIC ### **1. File Structure:**
# MAGIC
# MAGIC **Parquet:**
# MAGIC ```
# MAGIC s3://bucket/table/
# MAGIC   ├── part-00000.parquet
# MAGIC   ├── part-00001.parquet
# MAGIC   └── part-00002.parquet
# MAGIC ```
# MAGIC
# MAGIC **Delta:**
# MAGIC ```
# MAGIC s3://bucket/table/
# MAGIC   ├── part-00000.parquet          (data files)
# MAGIC   ├── part-00001.parquet
# MAGIC   └── _delta_log/
# MAGIC       ├── 00000000000000000000.json   (version 0)
# MAGIC       ├── 00000000000000000001.json   (version 1)
# MAGIC       └── 00000000000000000002.json   (version 2)
# MAGIC ```
# MAGIC
# MAGIC ### **2. Schema Enforcement Example:**
# MAGIC
# MAGIC **Parquet**: Accepts mismatched data → Query-time failures
# MAGIC ```python
# MAGIC # Write with schema A
# MAGIC df1.write.parquet("path")
# MAGIC
# MAGIC # Append with schema B (incompatible)
# MAGIC df2.write.mode("append").parquet("path")  # ⚠️ Succeeds but creates mess
# MAGIC
# MAGIC # Read fails or returns nulls
# MAGIC ```
# MAGIC
# MAGIC **Delta**: Rejects incompatible writes
# MAGIC ```python
# MAGIC # Write with schema A
# MAGIC df1.write.format("delta").save("path")
# MAGIC
# MAGIC # Append with schema B (incompatible)
# MAGIC df2.write.format("delta").mode("append").save("path")  # ❌ Fails immediately
# MAGIC ```
# MAGIC
# MAGIC ### **3. Update Operations:**
# MAGIC
# MAGIC **Parquet**: Full table rewrite (expensive)
# MAGIC ```python
# MAGIC # Must read entire table, filter, and rewrite
# MAGIC df = spark.read.parquet("path")
# MAGIC df_updated = df.filter(col("id") != 123).union(updated_row)
# MAGIC df_updated.write.mode("overwrite").parquet("path")  # Rewrites everything!
# MAGIC ```
# MAGIC
# MAGIC **Delta**: Efficient updates
# MAGIC ```python
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC delta_table = DeltaTable.forPath(spark, "path")
# MAGIC delta_table.update(
# MAGIC     condition = "id = 123",
# MAGIC     set = {"status": "'inactive'"}  # Only touches affected files
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 When to Use What?
# MAGIC
# MAGIC ### **Use Parquet When:**
# MAGIC * Data is immutable (write-once, read-many)
# MAGIC * No updates/deletes needed
# MAGIC * Simple archival storage
# MAGIC * Sharing data outside Databricks ecosystem
# MAGIC
# MAGIC ### **Use Delta Lake When:**
# MAGIC * Production data pipelines
# MAGIC * Data needs updates/deletes
# MAGIC * Multiple writers or concurrent access
# MAGIC * Streaming workloads
# MAGIC * Audit trail required
# MAGIC * Schema governance needed
# MAGIC * **99% of Databricks use cases** ✅
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Best Practice:
# MAGIC
# MAGIC > **In Databricks, ALWAYS use Delta Lake unless you have a specific reason not to.**
# MAGIC
# MAGIC Parquet is a storage format. Delta is a **data management platform** built on Parquet.

# COMMAND ----------

# DBTITLE 1,Section 4: Delta Architecture
# MAGIC %md
# MAGIC # 🏛️ Section 4: Delta Lake Architecture
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 👶 ELI5 (Explain Like I'm 5):
# MAGIC Delta Lake is like a library:
# MAGIC * **Books on shelves** = Data files (Parquet)
# MAGIC * **Library catalog** = Transaction log (_delta_log)
# MAGIC * Every time you add/remove a book, the librarian updates the catalog
# MAGIC * The catalog tells you exactly what books exist and where they are
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Core Components:
# MAGIC
# MAGIC ### **1. Data Files (Parquet)**
# MAGIC * Actual data stored in **columnar Parquet format**
# MAGIC * Compressed and optimized for analytics
# MAGIC * Immutable (never modified, only added or removed)
# MAGIC * Can be read by any Parquet-compatible tool
# MAGIC
# MAGIC ### **2. Transaction Log (_delta_log)**
# MAGIC * **The "source of truth"** for table state
# MAGIC * JSON files recording every transaction
# MAGIC * Enables ACID guarantees
# MAGIC * Provides complete audit trail
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📋 Delta Table Structure:
# MAGIC
# MAGIC ```
# MAGIC my_delta_table/
# MAGIC │
# MAGIC ├── part-00000-<uuid>.snappy.parquet    (Data file 1)
# MAGIC ├── part-00001-<uuid>.snappy.parquet    (Data file 2)
# MAGIC ├── part-00002-<uuid>.snappy.parquet    (Data file 3)
# MAGIC │
# MAGIC └── _delta_log/
# MAGIC     ├── 00000000000000000000.json      (Version 0: Initial CREATE)
# MAGIC     ├── 00000000000000000001.json      (Version 1: INSERT)
# MAGIC     ├── 00000000000000000002.json      (Version 2: UPDATE)
# MAGIC     ├── 00000000000000000003.json      (Version 3: DELETE)
# MAGIC     ├── 00000000000000000010.checkpoint.parquet
# MAGIC     └── _last_checkpoint                (Points to latest checkpoint)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📖 Transaction Log Deep Dive:
# MAGIC
# MAGIC ### **What's in a Transaction Log Entry?**
# MAGIC
# MAGIC Each JSON file contains:
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "commitInfo": {
# MAGIC     "timestamp": 1713730500000,
# MAGIC     "operation": "WRITE",
# MAGIC     "operationParameters": {"mode": "Append"},
# MAGIC     "readVersion": 0,
# MAGIC     "isBlindAppend": true
# MAGIC   },
# MAGIC   "add": {
# MAGIC     "path": "part-00000-abc123.snappy.parquet",
# MAGIC     "size": 12345678,
# MAGIC     "partitionValues": {},
# MAGIC     "modificationTime": 1713730500000,
# MAGIC     "dataChange": true,
# MAGIC     "stats": "{\"numRecords\":1000000,\"minValues\":{...},\"maxValues\":{...}}"
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### **Key Fields:**
# MAGIC * **commitInfo**: Metadata about the operation
# MAGIC * **add**: Files added in this transaction
# MAGIC * **remove**: Files removed (for updates/deletes)
# MAGIC * **metaData**: Schema and table properties
# MAGIC * **protocol**: Delta protocol version
# MAGIC * **stats**: Min/max statistics for data skipping
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚡ How Delta Tracks Changes:
# MAGIC
# MAGIC ### **Scenario: Multiple Operations**
# MAGIC
# MAGIC ```
# MAGIC Version 0: CREATE TABLE (schema defined)
# MAGIC Version 1: INSERT 1M rows → adds 10 Parquet files
# MAGIC Version 2: UPDATE 100K rows → marks old files for removal, adds new files
# MAGIC Version 3: DELETE 50K rows → marks affected files for removal
# MAGIC Version 4: OPTIMIZE → compacts files, removes small files
# MAGIC ```
# MAGIC
# MAGIC ### **How Reads Work:**
# MAGIC
# MAGIC 1. Reader opens table
# MAGIC 2. Reads transaction log from version 0 to latest
# MAGIC 3. Reconstructs current state: which files are active
# MAGIC 4. Reads only active Parquet files
# MAGIC 5. Filters out logically deleted rows (if any)
# MAGIC
# MAGIC ### **How Writes Work:**
# MAGIC
# MAGIC 1. Writer reads current version (e.g., version 4)
# MAGIC 2. Performs operation (generates new Parquet files)
# MAGIC 3. Writes new transaction log entry (version 5)
# MAGIC 4. Atomically commits by successfully writing the log file
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Checkpointing:
# MAGIC
# MAGIC ### **The Problem:**
# MAGIC Reading 10,000 JSON files to determine table state is slow.
# MAGIC
# MAGIC ### **The Solution: Checkpoints**
# MAGIC
# MAGIC * Every 10 versions (configurable), Delta creates a **checkpoint**
# MAGIC * Checkpoint = Parquet file with aggregated state
# MAGIC * Readers start from latest checkpoint + remaining JSON files
# MAGIC
# MAGIC **Example:**
# MAGIC ```
# MAGIC Table at version 1,523:
# MAGIC → Read checkpoint at version 1,520 (1 Parquet file)
# MAGIC → Read JSON logs 1521, 1522, 1523 (3 small files)
# MAGIC → Total: 4 files instead of 1,523!
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Transaction Flow:
# MAGIC
# MAGIC ```
# MAGIC                     WRITE OPERATION
# MAGIC                           │
# MAGIC                           ↓
# MAGIC             ┌──────────────────┐
# MAGIC             │ 1. Read Current  │
# MAGIC             │    Version       │
# MAGIC             └──────────────────┘
# MAGIC                           │
# MAGIC                           ↓
# MAGIC             ┌──────────────────┐
# MAGIC             │ 2. Write Data   │
# MAGIC             │    Files (Parq) │
# MAGIC             └──────────────────┘
# MAGIC                           │
# MAGIC                           ↓
# MAGIC             ┌──────────────────┐
# MAGIC             │ 3. Attempt      │
# MAGIC             │    Log Commit   │
# MAGIC             └──────────────────┘
# MAGIC                           │
# MAGIC                 ┌─────────┼─────────┐
# MAGIC                 │               │
# MAGIC           SUCCESS          CONFLICT
# MAGIC                 │               │
# MAGIC                 ↓               ↓
# MAGIC         ┌──────────┐   ┌───────────┐
# MAGIC         │ Committed │   │ Retry or │
# MAGIC         │ (Visible) │   │   Fail   │
# MAGIC         └──────────┘   └───────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Key Architecture Insights:
# MAGIC
# MAGIC 1. **Data and Metadata Separation**: Data in Parquet, metadata in JSON log
# MAGIC 2. **Immutable Files**: Files never modified, only added/removed
# MAGIC 3. **Optimistic Concurrency**: Multiple writers can work, last commit wins
# MAGIC 4. **No Central Coordinator**: Fully distributed, uses object storage atomicity
# MAGIC 5. **Backward Compatible**: Parquet files readable by any tool
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Why This Architecture Wins:
# MAGIC
# MAGIC * ✅ **Scalable**: Transaction log is just files, no database needed
# MAGIC * ✅ **Reliable**: ACID through atomic file operations
# MAGIC * ✅ **Fast**: Checkpoint mechanism keeps metadata small
# MAGIC * ✅ **Simple**: No external dependencies beyond object storage
# MAGIC * ✅ **Auditable**: Complete history preserved

# COMMAND ----------

# DBTITLE 1,Section 5: Creating Delta Tables
# MAGIC %md
# MAGIC # ⚙️ Section 5: Creating Delta Tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Three Ways to Create Delta Tables:
# MAGIC
# MAGIC 1. **DataFrame API** (External/Unmanaged)
# MAGIC 2. **SQL - Managed Tables** (Unity Catalog)
# MAGIC 3. **SQL - External Tables** (LOCATION specified)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Best Practice:
# MAGIC **Use Unity Catalog managed tables** for production workloads. They provide:
# MAGIC * Automatic metadata management
# MAGIC * Access control via Unity Catalog
# MAGIC * Simplified lifecycle management
# MAGIC * Better discoverability

# COMMAND ----------

# DBTITLE 1,Create Sample Data and Delta Tables
# ===================================================================
# Section 5: Creating Delta Tables - Hands-On Demonstration
# ===================================================================

from pyspark.sql.functions import col, expr, current_timestamp
from datetime import datetime, timedelta
import random

# -------------------------------------------------------------------
# Step 1: Create Sample Customer Data
# -------------------------------------------------------------------

print("\n" + "="*70)
print("STEP 1: Creating Sample Customer Dataset")
print("="*70)

# Generate sample data
customer_data = [
    (1, "Alice Johnson", "alice@email.com", "Premium", 15000, "2024-01-15"),
    (2, "Bob Smith", "bob@email.com", "Standard", 8500, "2024-02-20"),
    (3, "Carol White", "carol@email.com", "Premium", 22000, "2024-01-10"),
    (4, "David Brown", "david@email.com", "Basic", 3500, "2024-03-05"),
    (5, "Eve Davis", "eve@email.com", "Standard", 12000, "2024-02-15"),
    (6, "Frank Miller", "frank@email.com", "Premium", 18000, "2024-01-20"),
    (7, "Grace Lee", "grace@email.com", "Basic", 4200, "2024-03-10"),
    (8, "Henry Wilson", "henry@email.com", "Standard", 9500, "2024-02-25"),
    (9, "Iris Moore", "iris@email.com", "Premium", 25000, "2024-01-05"),
    (10, "Jack Taylor", "jack@email.com", "Basic", 3000, "2024-03-15")
]

schema = ["customer_id", "name", "email", "tier", "lifetime_value", "join_date"]

df_customers = spark.createDataFrame(customer_data, schema)

print(f"\n✅ Created DataFrame with {df_customers.count()} customers")
print("\nSample Data:")
display(df_customers)

# -------------------------------------------------------------------
# Step 2: Create Managed Delta Table (Unity Catalog Compliant)
# -------------------------------------------------------------------

print("\n" + "="*70)
print("STEP 2: Creating Managed Delta Table - Unity Catalog")
print("="*70)

# Write as managed Delta table (no external path needed)
df_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("default.customers_delta")

print("\n✅ Managed Delta table created: default.customers_delta")
print("   - Managed by Unity Catalog (or Hive Metastore)")
print("   - Format: Delta Lake (implicit)")
print("   - ACID: Enabled")
print("   - No external path management needed")

# Verify by reading back
df_read = spark.table("default.customers_delta")
print(f"\n✅ Verified: Read back {df_read.count()} records from Delta table")

# -------------------------------------------------------------------
# Step 3: Alternative - SQL Method
# -------------------------------------------------------------------

print("\n" + "="*70)
print("STEP 3: Creating Delta Table using SQL")
print("="*70)

# Create another table using SQL
spark.sql("""
    CREATE OR REPLACE TABLE default.customers_delta_v2
    USING DELTA
    AS
    SELECT * FROM default.customers_delta
""")

print("\n✅ Delta table created via SQL: default.customers_delta_v2")

# Verify
result = spark.sql("SELECT COUNT(*) as count FROM default.customers_delta_v2").collect()
print(f"\n✅ Verified: Table contains {result[0]['count']} records")

# -------------------------------------------------------------------
# Step 4: View Table Properties
# -------------------------------------------------------------------

print("\n" + "="*70)
print("STEP 4: Viewing Delta Table Properties")
print("="*70)

print("\nTable Schema:")
display(spark.sql("DESCRIBE default.customers_delta"))

print("\nTable Details (Delta-specific):")
display(spark.sql("DESCRIBE DETAIL default.customers_delta"))

print("\n" + "="*70)
print("✅ SECTION 5 COMPLETE: Delta Tables Created Successfully!")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Section 6: Reading Delta Tables
# MAGIC %md
# MAGIC # 📚 Section 6: Reading Delta Tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Multiple Ways to Read:
# MAGIC
# MAGIC 1. **DataFrame API** - Load from path
# MAGIC 2. **SQL** - Query managed table
# MAGIC 3. **Time Travel** - Query historical versions
# MAGIC 4. **Streaming** - Read as stream

# COMMAND ----------

# DBTITLE 1,Read Delta Tables - Multiple Methods
# ===================================================================
# Section 6: Reading Delta Tables - Hands-On Demonstration
# ===================================================================

from pyspark.sql.functions import col, sum as _sum, avg, count

print("\n" + "="*70)
print("SECTION 6: Reading Delta Tables")
print("="*70)

# -------------------------------------------------------------------
# Method 1: Read using DataFrame API (managed table)
# -------------------------------------------------------------------

print("\n" + "-"*70)
print("Method 1: DataFrame API - Read Managed Table")
print("-"*70)

df_from_table = spark.table("default.customers_delta")

print(f"\n✅ Loaded Delta table: default.customers_delta")
print(f"   Records: {df_from_table.count()}")
print(f"   Columns: {len(df_from_table.columns)}")

print("\nTop 5 Records:")
display(df_from_table.limit(5))

# -------------------------------------------------------------------
# Method 2: Read using SQL (managed table)
# -------------------------------------------------------------------

print("\n" + "-"*70)
print("Method 2: SQL Query - Managed Table")
print("-"*70)

df_from_sql = spark.sql("""
    SELECT 
        customer_id,
        name,
        tier,
        lifetime_value,
        join_date
    FROM default.customers_delta
    ORDER BY lifetime_value DESC
""")

print("\n✅ Queried managed table using SQL")
print("\nTop Customers by Lifetime Value:")
display(df_from_sql)

# -------------------------------------------------------------------
# Method 3: Analytical Queries on Delta
# -------------------------------------------------------------------

print("\n" + "-"*70)
print("Method 3: Analytical Queries")
print("-"*70)

# Query 1: Aggregation by tier
df_tier_analysis = spark.sql("""
    SELECT 
        tier,
        COUNT(*) as customer_count,
        ROUND(AVG(lifetime_value), 2) as avg_lifetime_value,
        ROUND(SUM(lifetime_value), 2) as total_value
    FROM default.customers_delta
    GROUP BY tier
    ORDER BY total_value DESC
""")

print("\n✅ Customer Analysis by Tier:")
display(df_tier_analysis)

# Query 2: Filter Premium customers
df_premium = df_from_table.filter(col("tier") == "Premium")

print(f"\n✅ Premium Customers: {df_premium.count()}")
print("\nPremium Customer Details:")
display(df_premium.select("name", "email", "lifetime_value", "join_date"))

# -------------------------------------------------------------------
# Method 4: DataFrame Transformations
# -------------------------------------------------------------------

print("\n" + "-"*70)
print("Method 4: DataFrame Transformations")
print("-"*70)

from pyspark.sql.functions import when, lit

# Add customer segment based on lifetime value
df_segmented = df_from_table.withColumn(
    "segment",
    when(col("lifetime_value") >= 20000, "High Value")
    .when(col("lifetime_value") >= 10000, "Medium Value")
    .otherwise("Low Value")
)

print("\n✅ Added customer segmentation based on lifetime value")
print("\nSegmented Customer View:")
display(df_segmented.select("name", "tier", "lifetime_value", "segment").orderBy(col("lifetime_value").desc()))

print("\n" + "="*70)
print("✅ SECTION 6 COMPLETE: Multiple Read Methods Demonstrated!")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Section 7: Hands-on Delta Pipeline
# MAGIC %md
# MAGIC # 🔧 Section 7: Hands-On Delta Pipeline
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Medallion Architecture:
# MAGIC
# MAGIC ```
# MAGIC 🥉 BRONZE (Raw Data)  →  🥈 SILVER (Refined)  →  🥇 GOLD (Business-Level)
# MAGIC ```
# MAGIC
# MAGIC ### Pipeline Flow:
# MAGIC 1. **Raw Data** → Ingest as-is
# MAGIC 2. **Bronze Layer** → Store in Delta (exact copy)
# MAGIC 3. **Silver Layer** → Clean, transform, enrich
# MAGIC 4. **Gold Layer** → Business aggregates
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Key Principle:
# MAGIC Each layer is a Delta table with full ACID guarantees!

# COMMAND ----------

# DBTITLE 1,Build Complete Delta Pipeline (Bronze to Silver)
# ===================================================================
# Section 7: Hands-On Delta Pipeline - Medallion Architecture
# ===================================================================

from pyspark.sql.functions import (
    col, current_timestamp, lit, when, 
    regexp_replace, lower, trim, to_date
)

print("\n" + "="*70)
print("BUILDING DELTA PIPELINE: Bronze → Silver → Gold")
print("="*70)

# Clean up any existing tables from previous runs
spark.sql("DROP TABLE IF EXISTS default.bronze_transactions")
spark.sql("DROP TABLE IF EXISTS default.silver_transactions")
spark.sql("DROP TABLE IF EXISTS default.gold_customer_summary")

print("\n✅ Cleaned up existing tables for fresh pipeline run")

# -------------------------------------------------------------------
# STEP 1: BRONZE LAYER - Raw Data Ingestion
# -------------------------------------------------------------------

print("\n" + "="*70)
print("🥉 BRONZE LAYER: Ingesting Raw Data")
print("="*70)

# Simulate raw data from external source (with quality issues)
raw_transactions = [
    (101, 1, "2024-04-01", 250.50, "completed", "credit_card"),
    (102, 2, "2024-04-01", 180.00, "COMPLETED", "PayPal"),
    (103, 3, "2024-04-02", 420.75, "completed", "credit_card"),
    (104, 1, "2024-04-02", 95.00, "failed", "credit_card"),
    (105, 4, "2024-04-03", 310.20, "COMPLETED", "debit_card"),
    (106, 5, "2024-04-03", 125.50, "completed", "credit_card"),
    (107, 3, "2024-04-04", 890.00, "completed", "PayPal"),
    (108, 6, "2024-04-04", 445.30, "pending", "credit_card"),
    (109, 2, "2024-04-05", None, "failed", "debit_card"),  # Null amount
    (110, 7, "2024-04-05", 175.00, "COMPLETED", "credit_card")
]

schema = ["transaction_id", "customer_id", "transaction_date", "amount", "status", "payment_method"]
df_raw = spark.createDataFrame(raw_transactions, schema)

print(f"\n✅ Created raw transaction dataset: {df_raw.count()} records")
print("\nRaw Data (with quality issues):")
display(df_raw)

# Write to Bronze Delta table (exact copy, no transformations)
df_bronze = df_raw.withColumn("ingestion_timestamp", current_timestamp())

df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("default.bronze_transactions")

print(f"\n✅ Bronze Delta table created: default.bronze_transactions")
print("   - Raw data preserved as-is")
print("   - Ingestion timestamp added")
print("   - Full ACID guarantees")
print("   - Managed by Unity Catalog")

# -------------------------------------------------------------------
# STEP 2: SILVER LAYER - Data Cleaning & Transformation
# -------------------------------------------------------------------

print("\n" + "="*70)
print("🥈 SILVER LAYER: Cleaning & Transforming Data")
print("="*70)

# Read from Bronze
df_bronze_read = spark.table("default.bronze_transactions")

print("\nApplying transformations:")
print("  1. Standardizing status values (lowercase)")
print("  2. Filtering out failed transactions")
print("  3. Removing null amounts")
print("  4. Converting date strings to date type")
print("  5. Adding data quality flags")

# Transformations
df_silver = df_bronze_read \
    .filter(col("amount").isNotNull()) \
    .filter(col("status").isin(["completed", "COMPLETED"])) \
    .withColumn("status_clean", lower(trim(col("status")))) \
    .withColumn("transaction_date_clean", to_date(col("transaction_date"))) \
    .withColumn("payment_method_clean", lower(regexp_replace(col("payment_method"), "_", " "))) \
    .withColumn("is_high_value", when(col("amount") >= 400, True).otherwise(False)) \
    .withColumn("processing_timestamp", current_timestamp()) \
    .select(
        "transaction_id",
        "customer_id",
        col("transaction_date_clean").alias("transaction_date"),
        "amount",
        col("status_clean").alias("status"),
        col("payment_method_clean").alias("payment_method"),
        "is_high_value",
        "ingestion_timestamp",
        "processing_timestamp"
    )

print(f"\n✅ Cleaned data: {df_silver.count()} records (filtered from {df_bronze_read.count()})")
print("\nSilver Layer Data (Cleaned):")
display(df_silver)

# Write to Silver Delta table
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("default.silver_transactions")

print(f"\n✅ Silver Delta table created: default.silver_transactions")
print("   - Cleaned and validated data")
print("   - Business rules applied")
print("   - Managed by Unity Catalog")

# -------------------------------------------------------------------
# STEP 3: GOLD LAYER - Business Aggregates
# -------------------------------------------------------------------

print("\n" + "="*70)
print("🥇 GOLD LAYER: Business-Level Aggregates")
print("="*70)

# Read from Silver
df_silver_read = spark.table("default.silver_transactions")

from pyspark.sql.functions import sum as _sum, avg, count

# Create business-level aggregates
df_gold = df_silver_read.groupBy("customer_id") \
    .agg(
        count("transaction_id").alias("total_transactions"),
        _sum("amount").alias("total_spend"),
        avg("amount").alias("avg_transaction_amount"),
        _sum(when(col("is_high_value"), 1).otherwise(0)).alias("high_value_transactions")
    ) \
    .withColumn("aggregation_timestamp", current_timestamp())

print("\nGold Layer: Customer Transaction Summary")
display(df_gold.orderBy(col("total_spend").desc()))

# Write to Gold Delta table
df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("default.gold_customer_summary")

print(f"\n✅ Gold Delta table created: default.gold_customer_summary")
print("   - Business-level aggregates")
print("   - Optimized for analytics")
print("   - Managed by Unity Catalog")

# -------------------------------------------------------------------
# STEP 4: Pipeline Summary
# -------------------------------------------------------------------

print("\n" + "="*70)
print("PIPELINE SUMMARY")
print("="*70)

print("\n🥉 BRONZE: Raw data ingestion")
print(f"   Table: default.bronze_transactions")
print(f"   Records: {spark.table('default.bronze_transactions').count()}")

print("\n🥈 SILVER: Cleaned and transformed")
print(f"   Table: default.silver_transactions")
print(f"   Records: {spark.table('default.silver_transactions').count()}")

print("\n🥇 GOLD: Business aggregates")
print(f"   Table: default.gold_customer_summary")
print(f"   Records: {spark.table('default.gold_customer_summary').count()}")

print("\n" + "="*70)
print("✅ SECTION 7 COMPLETE: Delta Pipeline Built Successfully!")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Section 8: Transaction Log Exploration
# MAGIC %md
# MAGIC # 📜 Section 8: Transaction Log (_delta_log)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 👶 ELI5:
# MAGIC The _delta_log folder is like a diary that writes down everything that happens to your data:
# MAGIC * "Added 1000 toys on Monday at 9am"
# MAGIC * "Removed broken toy #42 on Tuesday"
# MAGIC * "Organized all red toys on Wednesday"
# MAGIC
# MAGIC Anyone can read the diary to know exactly what toys are in the box RIGHT NOW!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ The Transaction Log Explained:
# MAGIC
# MAGIC ### **What is it?**
# MAGIC * A directory named `_delta_log` in every Delta table
# MAGIC * Contains JSON files (and Parquet checkpoints)
# MAGIC * Each file = one transaction/version
# MAGIC * Provides complete audit trail
# MAGIC
# MAGIC ### **Why is it critical?**
# MAGIC 1. **ACID Guarantees**: Source of truth for table state
# MAGIC 2. **Time Travel**: Historical versions preserved
# MAGIC 3. **Concurrency Control**: Detects conflicts
# MAGIC 4. **Performance**: Data skipping via statistics
# MAGIC 5. **Audit Trail**: Complete lineage
# MAGIC
# MAGIC ### **File Naming Convention:**
# MAGIC ```
# MAGIC _delta_log/
# MAGIC   00000000000000000000.json    ← Version 0
# MAGIC   00000000000000000001.json    ← Version 1
# MAGIC   00000000000000000002.json    ← Version 2
# MAGIC   ...
# MAGIC   00000000000000000010.checkpoint.parquet  ← Checkpoint at version 10
# MAGIC   _last_checkpoint              ← Points to latest checkpoint
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📖 Transaction Log Contents:
# MAGIC
# MAGIC ### **Actions Recorded:**
# MAGIC
# MAGIC | Action | Description | Example |
# MAGIC |--------|-------------|----------|
# MAGIC | **metaData** | Schema, partitioning, properties | Table created with 5 columns |
# MAGIC | **protocol** | Delta protocol version | Reader v1, Writer v2 |
# MAGIC | **add** | File added to table | part-00001.parquet added |
# MAGIC | **remove** | File logically deleted | part-00003.parquet removed |
# MAGIC | **commitInfo** | Operation metadata | MERGE executed at timestamp X |
# MAGIC | **txn** | Application transaction ID | Streaming app checkpoint |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔍 Let's Explore the Transaction Log!

# COMMAND ----------

# DBTITLE 1,Explore Transaction Log
# ===================================================================
# Section 8: Exploring the Transaction Log
# ===================================================================

print("\n" + "="*70)
print("EXPLORING DELTA TRANSACTION LOG")
print("="*70)

# -------------------------------------------------------------------
# STEP 1: View Table History
# -------------------------------------------------------------------

print("\n" + "="*70)
print("STEP 1: Delta Table History")
print("="*70)

# Get history of silver table
df_history = spark.sql("""
    DESCRIBE HISTORY default.silver_transactions
""")

print("\n✅ Transaction history retrieved")
print("\nDelta Table Versions:")
display(df_history.select(
    "version", 
    "timestamp", 
    "operation", 
    "operationParameters",
    "readVersion"
))

# -------------------------------------------------------------------
# STEP 2: View Table Detail (Delta-specific metadata)
# -------------------------------------------------------------------

print("\n" + "="*70)
print("STEP 2: Delta Table Details")
print("="*70)

df_detail = spark.sql("""
    DESCRIBE DETAIL default.silver_transactions
""")

print("\n✅ Table metadata retrieved")
print("\nDelta Table Metadata:")
display(df_detail.select(
    "format",
    "location",
    "numFiles",
    "sizeInBytes",
    "minReaderVersion",
    "minWriterVersion"
))

# -------------------------------------------------------------------
# STEP 3: Conceptual - Transaction Log Structure
# -------------------------------------------------------------------

print("\n" + "="*70)
print("STEP 3: Understanding Transaction Log Structure")
print("="*70)

print("""
📁 Transaction Log Structure (_delta_log/):

Each JSON file contains actions like:

{
  "commitInfo": {
    "timestamp": 1713730500000,
    "operation": "WRITE",
    "operationMetrics": {
      "numFiles": "1",
      "numOutputRows": "7"
    }
  },
  "add": {
    "path": "part-00000-xxx.snappy.parquet",
    "size": 987654,
    "modificationTime": 1713730500000,
    "dataChange": true,
    "stats": {
      "numRecords": 7,
      "minValues": {"transaction_id": 101},
      "maxValues": {"transaction_id": 110}
    }
  }
}

Key Points:
✅ Every write operation creates a new log entry
✅ Log contains file paths, statistics, and metadata
✅ Statistics enable data skipping (performance optimization)
✅ Complete audit trail of all operations
""")

# -------------------------------------------------------------------
# STEP 4: View Table Properties
# -------------------------------------------------------------------

print("\n" + "="*70)
print("STEP 4: Delta Table Properties")
print("="*70)

df_properties = spark.sql("""
    SHOW TBLPROPERTIES default.silver_transactions
""")

print("\n✅ Table properties retrieved")
print("\nDelta Configuration:")
display(df_properties)

# -------------------------------------------------------------------
# STEP 5: Demonstrate Version Information
# -------------------------------------------------------------------

print("\n" + "="*70)
print("STEP 5: Current Table Version")
print("="*70)

# Get current version
current_version = spark.sql("""
    DESCRIBE HISTORY default.silver_transactions LIMIT 1
""").select("version").collect()[0][0]

print(f"\n✅ Current table version: {current_version}")
print("\nWhat this means:")
print(f"   - Table has undergone {current_version + 1} transaction(s)")
print(f"   - Transaction log contains entries 0 through {current_version}")
print("   - Each entry represents an atomic operation")
print("   - Complete history is preserved for time travel")

# -------------------------------------------------------------------
# STEP 6: Key Insights
# -------------------------------------------------------------------

print("\n" + "="*70)
print("💡 KEY INSIGHTS ABOUT TRANSACTION LOG")
print("="*70)

print("""
1. 🔒 ACID Guarantee:
   Every successful operation = One committed transaction log entry
   
2. 🔄 Version Control:
   Each write increments version number
   Previous versions remain accessible
   
3. ⚡ Performance:
   Statistics in log enable data skipping
   Only scan relevant files based on filters
   
4. 🔍 Audit Trail:
   Who, what, when for every operation
   Complete lineage from creation to current state
   
5. 🛡️ Concurrency:
   Optimistic concurrency control
   Conflicts detected and resolved automatically
   
6. 💾 Checkpoints:
   Every 10 versions (configurable)
   Aggregates log state into Parquet
   Prevents reading thousands of JSON files
""")

print("\n" + "="*70)
print("✅ SECTION 8 COMPLETE: Transaction Log Explored!")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Section 9: End-to-End Delta Architecture
# MAGIC %md
# MAGIC # 🏛️ Section 9: End-to-End Delta Lake Architecture Flow
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Complete Lakehouse Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────┐
# MAGIC │                      DATA SOURCES                              │
# MAGIC │   Databases | APIs | Files | Streams | IoT | SaaS Apps        │
# MAGIC └──────────────────────────────────────────────────────────────┘
# MAGIC                                 │
# MAGIC                                 ↓ INGESTION
# MAGIC                                 │
# MAGIC ┌──────────────────────────────────────────────────────────────┐
# MAGIC │              🥉 BRONZE LAYER (Raw Data)                    │
# MAGIC │                                                              │
# MAGIC │  • Exact copy of source data                               │
# MAGIC │  • No transformations                                       │
# MAGIC │  • Append-only (immutable)                                 │
# MAGIC │  • Schema enforcement                                       │
# MAGIC │  • Format: Delta Lake                                       │
# MAGIC │                                                              │
# MAGIC │  Tables: bronze_orders, bronze_customers, bronze_events     │
# MAGIC └──────────────────────────────────────────────────────────────┘
# MAGIC                                 │
# MAGIC                                 ↓ CLEANSING & ENRICHMENT
# MAGIC                                 │
# MAGIC ┌──────────────────────────────────────────────────────────────┐
# MAGIC │            🥈 SILVER LAYER (Refined Data)                   │
# MAGIC │                                                              │
# MAGIC │  • Cleaned and validated                                    │
# MAGIC │  • Standardized formats                                     │
# MAGIC │  • Deduplicated                                             │
# MAGIC │  • Enriched with business logic                            │
# MAGIC │  • Updates/Deletes via MERGE                               │
# MAGIC │  • Format: Delta Lake                                       │
# MAGIC │                                                              │
# MAGIC │  Tables: silver_orders, silver_customers, silver_events     │
# MAGIC └──────────────────────────────────────────────────────────────┘
# MAGIC                                 │
# MAGIC                                 ↓ AGGREGATION & BUSINESS LOGIC
# MAGIC                                 │
# MAGIC ┌──────────────────────────────────────────────────────────────┐
# MAGIC │              🥇 GOLD LAYER (Business Data)                  │
# MAGIC │                                                              │
# MAGIC │  • Business-level aggregates                               │
# MAGIC │  • Optimized for analytics                                 │
# MAGIC │  • Denormalized for performance                            │
# MAGIC │  • KPIs and metrics                                         │
# MAGIC │  • Ready for BI tools                                       │
# MAGIC │  • Format: Delta Lake                                       │
# MAGIC │                                                              │
# MAGIC │  Tables: gold_daily_sales, gold_customer_360, gold_kpis     │
# MAGIC └──────────────────────────────────────────────────────────────┘
# MAGIC                                 │
# MAGIC                                 ↓ CONSUMPTION
# MAGIC                                 │
# MAGIC ┌──────────────────────────────────────────────────────────────┐
# MAGIC │                     CONSUMERS                                │
# MAGIC │   BI Tools | ML Models | APIs | Reports | Dashboards        │
# MAGIC └──────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Data Flow Details:
# MAGIC
# MAGIC ### **1. BRONZE → SILVER Transformation**
# MAGIC
# MAGIC ```python
# MAGIC # Read from Bronze
# MAGIC df_bronze = spark.read.format("delta").load("bronze_path")
# MAGIC
# MAGIC # Apply business rules
# MAGIC df_silver = df_bronze \
# MAGIC     .filter(col("status") == "active") \
# MAGIC     .dropDuplicates(["customer_id"]) \
# MAGIC     .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name"))) \
# MAGIC     .withColumn("processed_timestamp", current_timestamp())
# MAGIC
# MAGIC # Write to Silver (with MERGE for upserts)
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC silver_table = DeltaTable.forPath(spark, "silver_path")
# MAGIC silver_table.alias("target").merge(
# MAGIC     df_silver.alias("source"),
# MAGIC     "target.customer_id = source.customer_id"
# MAGIC ).whenMatchedUpdateAll() \
# MAGIC  .whenNotMatchedInsertAll() \
# MAGIC  .execute()
# MAGIC ```
# MAGIC
# MAGIC ### **2. SILVER → GOLD Aggregation**
# MAGIC
# MAGIC ```python
# MAGIC # Read from Silver
# MAGIC df_silver = spark.read.format("delta").load("silver_path")
# MAGIC
# MAGIC # Create business-level metrics
# MAGIC df_gold = df_silver.groupBy("region", "product_category") \
# MAGIC     .agg(
# MAGIC         count("order_id").alias("total_orders"),
# MAGIC         sum("revenue").alias("total_revenue"),
# MAGIC         avg("customer_satisfaction").alias("avg_satisfaction")
# MAGIC     )
# MAGIC
# MAGIC # Write to Gold
# MAGIC df_gold.write \
# MAGIC     .format("delta") \
# MAGIC     .mode("overwrite") \
# MAGIC     .option("overwriteSchema", "true") \
# MAGIC     .save("gold_path")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚙️ Delta Lake Features at Each Layer:
# MAGIC
# MAGIC | Feature | Bronze | Silver | Gold |
# MAGIC |---------|--------|--------|------|
# MAGIC | **ACID Transactions** | ✅ | ✅ | ✅ |
# MAGIC | **Schema Enforcement** | ✅ | ✅ | ✅ |
# MAGIC | **Time Travel** | ✅ | ✅ | ✅ |
# MAGIC | **Updates/Deletes** | Rare | ✅ Frequent | ✅ | 
# MAGIC | **MERGE Operations** | No | ✅ Yes | ✅ |
# MAGIC | **Partitioning** | Optional | ✅ Recommended | ✅ |
# MAGIC | **Z-Ordering** | No | ✅ Yes | ✅ |
# MAGIC | **Optimize** | No | ✅ Yes | ✅ |
# MAGIC | **Vacuum** | No | ✅ Yes | ✅ |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Pipeline Orchestration:
# MAGIC
# MAGIC ### **Option 1: Databricks Jobs**
# MAGIC ```
# MAGIC Job: Daily_Customer_ETL
# MAGIC ├── Task 1: Ingest to Bronze (Notebook)
# MAGIC ├── Task 2: Transform to Silver (Notebook) [depends on Task 1]
# MAGIC └── Task 3: Aggregate to Gold (Notebook) [depends on Task 2]
# MAGIC ```
# MAGIC
# MAGIC ### **Option 2: Lakeflow Spark Declarative Pipelines**
# MAGIC ```python
# MAGIC # Bronze
# MAGIC @dlt.table
# MAGIC def bronze_orders():
# MAGIC     return spark.readStream.format("cloudFiles") \
# MAGIC         .option("cloudFiles.format", "json") \
# MAGIC         .load("/source/orders/")
# MAGIC
# MAGIC # Silver
# MAGIC @dlt.table
# MAGIC def silver_orders():
# MAGIC     return dlt.read_stream("bronze_orders") \
# MAGIC         .filter(col("status").isNotNull()) \
# MAGIC         .dropDuplicates(["order_id"])
# MAGIC
# MAGIC # Gold
# MAGIC @dlt.table
# MAGIC def gold_daily_sales():
# MAGIC     return dlt.read("silver_orders") \
# MAGIC         .groupBy("date", "region") \
# MAGIC         .agg(sum("amount").alias("daily_revenue"))
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Best Practices:
# MAGIC
# MAGIC ### **✅ DO:**
# MAGIC * Use Delta for ALL layers (Bronze, Silver, Gold)
# MAGIC * Implement schema enforcement from Bronze onwards
# MAGIC * Use MERGE for Silver layer updates
# MAGIC * Partition large tables appropriately
# MAGIC * Run OPTIMIZE regularly on Silver/Gold
# MAGIC * Enable Auto Optimize for write-heavy tables
# MAGIC * Use Unity Catalog for governance
# MAGIC * Document schema changes
# MAGIC
# MAGIC ### **❌ DON'T:**
# MAGIC * Mix Parquet and Delta in the same pipeline
# MAGIC * Skip Bronze layer (always preserve raw data)
# MAGIC * Overwrite Bronze tables (append-only)
# MAGIC * Create too many partitions (aim for 1GB+ per partition)
# MAGIC * Forget to VACUUM old versions (retention policy)
# MAGIC * Use Delta without proper access controls
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Production Architecture Checklist:
# MAGIC
# MAGIC - [ ] All layers use Delta Lake format
# MAGIC - [ ] Schema enforcement enabled
# MAGIC - [ ] Transaction log monitored
# MAGIC - [ ] Time travel retention configured
# MAGIC - [ ] OPTIMIZE scheduled regularly
# MAGIC - [ ] VACUUM configured with appropriate retention
# MAGIC - [ ] Partitioning strategy defined
# MAGIC - [ ] Unity Catalog integration enabled
# MAGIC - [ ] Access controls implemented
# MAGIC - [ ] Monitoring and alerting set up
# MAGIC - [ ] Data quality checks in place
# MAGIC - [ ] Disaster recovery plan defined
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Performance Optimizations:
# MAGIC
# MAGIC ### **1. Z-Ordering (Multi-dimensional clustering)**
# MAGIC ```sql
# MAGIC OPTIMIZE silver.orders
# MAGIC ZORDER BY (customer_id, order_date)
# MAGIC ```
# MAGIC
# MAGIC ### **2. Auto Optimize**
# MAGIC ```sql
# MAGIC ALTER TABLE silver.orders
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### **3. Data Skipping**
# MAGIC Automatic via statistics in transaction log - no configuration needed!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✨ The Delta Lake Advantage:
# MAGIC
# MAGIC With Delta Lake across all layers:
# MAGIC * **Reliability**: ACID guarantees end-to-end
# MAGIC * **Performance**: Optimized reads via data skipping
# MAGIC * **Flexibility**: Time travel for debugging/auditing
# MAGIC * **Governance**: Schema enforcement prevents bad data
# MAGIC * **Simplicity**: One format, consistent APIs
# MAGIC * **Scalability**: Handles petabyte-scale tables

# COMMAND ----------

# DBTITLE 1,Genie Code Agent Usage Examples
# MAGIC %md
# MAGIC # 🤖 Genie Code Agent - Delta Lake Use Cases
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 How to Use Genie Code Agent for Delta Lake Tasks:
# MAGIC
# MAGIC ### **📝 Creating Delta Tables**
# MAGIC
# MAGIC **Example Prompts:**
# MAGIC ```
# MAGIC ➡️ "Create a Delta table from this CSV file at /path/to/data.csv"
# MAGIC ➡️ "Convert my Parquet table to Delta format"
# MAGIC ➡️ "Create a partitioned Delta table by date and region"
# MAGIC ➡️ "Build a managed Delta table in Unity Catalog: catalog.schema.table_name"
# MAGIC ```
# MAGIC
# MAGIC **What Genie Does:**
# MAGIC * Reads source data
# MAGIC * Infers or validates schema
# MAGIC * Writes as Delta format
# MAGIC * Creates managed table if requested
# MAGIC * Configures partitioning if specified
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **🔄 Delta Pipeline Development**
# MAGIC
# MAGIC **Example Prompts:**
# MAGIC ```
# MAGIC ➡️ "Build a Bronze-Silver-Gold pipeline for customer data"
# MAGIC ➡️ "Create a streaming Delta pipeline from Kafka topic"
# MAGIC ➡️ "Implement CDC using Delta MERGE operation"
# MAGIC ➡️ "Design an incremental load pipeline with Delta"
# MAGIC ```
# MAGIC
# MAGIC **What Genie Does:**
# MAGIC * Designs medallion architecture
# MAGIC * Writes Bronze ingestion code
# MAGIC * Implements Silver transformations
# MAGIC * Creates Gold aggregations
# MAGIC * Adds error handling and logging
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **🔍 Explaining Delta Concepts**
# MAGIC
# MAGIC **Example Prompts:**
# MAGIC ```
# MAGIC ➡️ "Explain how Delta Lake transaction log works"
# MAGIC ➡️ "What's the difference between Delta and Parquet?"
# MAGIC ➡️ "How does Delta achieve ACID guarantees?"
# MAGIC ➡️ "Explain Delta Lake time travel with examples"
# MAGIC ```
# MAGIC
# MAGIC **What Genie Does:**
# MAGIC * Provides conceptual explanations
# MAGIC * Shows practical examples
# MAGIC * Compares with alternatives
# MAGIC * Demonstrates with code samples
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **⚡ Optimizing Delta Tables**
# MAGIC
# MAGIC **Example Prompts:**
# MAGIC ```
# MAGIC ➡️ "Optimize my Delta table for query performance"
# MAGIC ➡️ "Run Z-ORDER on customer_id and transaction_date"
# MAGIC ➡️ "Show me how to enable Auto Optimize"
# MAGIC ➡️ "Compact small files in my Delta table"
# MAGIC ```
# MAGIC
# MAGIC **What Genie Does:**
# MAGIC * Analyzes table structure
# MAGIC * Runs OPTIMIZE command
# MAGIC * Configures Z-ORDERING
# MAGIC * Enables Auto Optimize properties
# MAGIC * Provides performance recommendations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **🛡️ Delta Table Operations**
# MAGIC
# MAGIC **Example Prompts:**
# MAGIC ```
# MAGIC ➡️ "Update records where status = 'pending' to 'completed'"
# MAGIC ➡️ "Delete records older than 90 days"
# MAGIC ➡️ "Perform UPSERT (MERGE) on customer_id"
# MAGIC ➡️ "Show table history and versions"
# MAGIC ```
# MAGIC
# MAGIC **What Genie Does:**
# MAGIC * Writes DeltaTable API code
# MAGIC * Implements UPDATE/DELETE/MERGE
# MAGIC * Shows DESCRIBE HISTORY output
# MAGIC * Handles transaction safely
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **🔙 Time Travel & Versioning**
# MAGIC
# MAGIC **Example Prompts:**
# MAGIC ```
# MAGIC ➡️ "Query version 5 of my Delta table"
# MAGIC ➡️ "Show data as it was yesterday at 3pm"
# MAGIC ➡️ "Restore table to previous version"
# MAGIC ➡️ "Compare current version with version 10"
# MAGIC ```
# MAGIC
# MAGIC **What Genie Does:**
# MAGIC * Uses VERSION AS OF syntax
# MAGIC * Uses TIMESTAMP AS OF for point-in-time queries
# MAGIC * Implements RESTORE command
# MAGIC * Creates comparison analysis
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **📊 Analyzing Delta Tables**
# MAGIC
# MAGIC **Example Prompts:**
# MAGIC ```
# MAGIC ➡️ "Analyze my Delta table structure and metadata"
# MAGIC ➡️ "Show table statistics and file counts"
# MAGIC ➡️ "Check Delta table health"
# MAGIC ➡️ "Compare table sizes before/after OPTIMIZE"
# MAGIC ```
# MAGIC
# MAGIC **What Genie Does:**
# MAGIC * Runs DESCRIBE DETAIL
# MAGIC * Shows transaction log info
# MAGIC * Analyzes file distribution
# MAGIC * Provides optimization recommendations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **🧹 Delta Maintenance**
# MAGIC
# MAGIC **Example Prompts:**
# MAGIC ```
# MAGIC ➡️ "Clean up old versions (VACUUM)"
# MAGIC ➡️ "Set retention period to 7 days"
# MAGIC ➡️ "Check what VACUUM would delete (dry run)"
# MAGIC ➡️ "Schedule regular OPTIMIZE and VACUUM"
# MAGIC ```
# MAGIC
# MAGIC **What Genie Does:**
# MAGIC * Runs VACUUM with proper retention
# MAGIC * Shows dry run results first
# MAGIC * Configures table properties
# MAGIC * Creates maintenance job templates
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **📄 Schema Management**
# MAGIC
# MAGIC **Example Prompts:**
# MAGIC ```
# MAGIC ➡️ "Add a new column to my Delta table"
# MAGIC ➡️ "Enable schema evolution for this table"
# MAGIC ➡️ "Show schema changes over time"
# MAGIC ➡️ "Enforce strict schema validation"
# MAGIC ```
# MAGIC
# MAGIC **What Genie Does:**
# MAGIC * Uses ALTER TABLE commands
# MAGIC * Configures merge schema options
# MAGIC * Shows schema history from transaction log
# MAGIC * Sets schema enforcement properties
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **🌐 Migration Tasks**
# MAGIC
# MAGIC **Example Prompts:**
# MAGIC ```
# MAGIC ➡️ "Convert all Parquet tables to Delta"
# MAGIC ➡️ "Migrate Hive tables to Unity Catalog Delta tables"
# MAGIC ➡️ "Convert in-place: Parquet to Delta without copying"
# MAGIC ➡️ "Migrate streaming checkpoint to Delta format"
# MAGIC ```
# MAGIC
# MAGIC **What Genie Does:**
# MAGIC * Scans existing tables
# MAGIC * Generates conversion code
# MAGIC * Handles in-place conversion
# MAGIC * Validates after migration
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Pro Tips for Working with Genie:
# MAGIC
# MAGIC ### **✅ Be Specific:**
# MAGIC ```
# MAGIC ❌ "Create a table"  
# MAGIC ✅ "Create a partitioned Delta table by date with schema: id INT, name STRING, date DATE"
# MAGIC ```
# MAGIC
# MAGIC ### **✅ Provide Context:**
# MAGIC ```
# MAGIC ❌ "Optimize table"  
# MAGIC ✅ "Optimize my Delta table at /path/table for queries filtering on customer_id and date"
# MAGIC ```
# MAGIC
# MAGIC ### **✅ Mention Constraints:**
# MAGIC ```
# MAGIC ❌ "Build pipeline"  
# MAGIC ✅ "Build streaming Bronze-Silver pipeline using Auto Loader, no cache/persist, Unity Catalog"
# MAGIC ```
# MAGIC
# MAGIC ### **✅ Ask for Best Practices:**
# MAGIC ```
# MAGIC ➡️ "What are Delta Lake best practices for production?"
# MAGIC ➡️ "How should I partition my 10TB Delta table?"
# MAGIC ➡️ "Best way to handle late-arriving data in Delta?"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚡ Quick Reference - Common Commands:
# MAGIC
# MAGIC | Task | Prompt Template |
# MAGIC |------|----------------|
# MAGIC | **Create** | "Create Delta table from [source] with [specs]" |
# MAGIC | **Read** | "Query Delta table [name] with [conditions]" |
# MAGIC | **Update** | "Update [table] set [column]=[value] where [condition]" |
# MAGIC | **Delete** | "Delete from [table] where [condition]" |
# MAGIC | **Merge** | "UPSERT into [table] on [key] from [source]" |
# MAGIC | **Optimize** | "Optimize [table] ZORDER by [columns]" |
# MAGIC | **Time Travel** | "Query [table] version [N] / as of [timestamp]" |
# MAGIC | **Vacuum** | "VACUUM [table] retain [N] hours" |
# MAGIC | **Schema** | "Add column [name] [type] to [table]" |
# MAGIC | **Analyze** | "Show metadata and stats for [table]" |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Advanced Use Cases:
# MAGIC
# MAGIC ### **1. Complex Pipeline with Error Handling:**
# MAGIC ```
# MAGIC "Build a production-ready Bronze-Silver-Gold pipeline with:
# MAGIC - Auto Loader for ingestion
# MAGIC - Data quality checks
# MAGIC - Error quarantine table
# MAGIC - Idempotent writes
# MAGIC - Delta MERGE for Silver
# MAGIC - Monitoring metrics"
# MAGIC ```
# MAGIC
# MAGIC ### **2. Performance Troubleshooting:**
# MAGIC ```
# MAGIC "My Delta query is slow. Help me:
# MAGIC - Analyze table statistics
# MAGIC - Check partition strategy
# MAGIC - Recommend ZORDER columns
# MAGIC - Identify small files
# MAGIC - Suggest OPTIMIZE schedule"
# MAGIC ```
# MAGIC
# MAGIC ### **3. Compliance & Auditing:**
# MAGIC ```
# MAGIC "Implement GDPR-compliant pipeline:
# MAGIC - Track all PII access in audit log
# MAGIC - Enable Delta time travel for 90 days
# MAGIC - Implement DELETE for right-to-be-forgotten
# MAGIC - Show complete data lineage"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎓 Learning Path with Genie:
# MAGIC
# MAGIC **Beginner:**
# MAGIC 1. "Explain Delta Lake basics"
# MAGIC 2. "Create my first Delta table"
# MAGIC 3. "Show me CRUD operations"
# MAGIC
# MAGIC **Intermediate:**
# MAGIC 1. "Build medallion architecture pipeline"
# MAGIC 2. "Implement MERGE for CDC"
# MAGIC 3. "Optimize for performance"
# MAGIC
# MAGIC **Advanced:**
# MAGIC 1. "Design multi-hop streaming architecture"
# MAGIC 2. "Implement complex data quality framework"
# MAGIC 3. "Production-grade error handling and monitoring"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Remember:
# MAGIC
# MAGIC Genie Code Agent is your **Delta Lake expert** - ask for:
# MAGIC * Explanations and documentation
# MAGIC * Code generation and best practices
# MAGIC * Performance optimization advice
# MAGIC * Architecture and design guidance
# MAGIC * Debugging and troubleshooting help

# COMMAND ----------

# DBTITLE 1,Final Summary and Interview Prep
# MAGIC %md
# MAGIC # 🎓 Final Summary: Delta Lake Fundamentals
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📌 Key Learnings - Phase 4 Day 19:
# MAGIC
# MAGIC ### **1. Delta Lake = ACID + Data Lake**
# MAGIC * Brings database reliability to data lakes
# MAGIC * Built on top of Parquet files
# MAGIC * Uses transaction log for ACID guarantees
# MAGIC * Open-source storage layer
# MAGIC
# MAGIC ### **2. ACID Guarantees:**
# MAGIC * **A**tomicity: All-or-nothing transactions
# MAGIC * **C**onsistency: Schema enforcement
# MAGIC * **I**solation: Snapshot isolation for concurrent access
# MAGIC * **D**urability: Committed data persists
# MAGIC
# MAGIC ### **3. Delta vs Parquet:**
# MAGIC * Parquet = Storage format (columnar compression)
# MAGIC * Delta = Management layer on top of Parquet
# MAGIC * Delta adds: ACID, schema, versioning, updates/deletes
# MAGIC * **In Databricks: Always use Delta**
# MAGIC
# MAGIC ### **4. Transaction Log (_delta_log):**
# MAGIC * JSON files recording every operation
# MAGIC * Source of truth for table state
# MAGIC * Enables time travel and auditing
# MAGIC * Checkpoints for performance
# MAGIC
# MAGIC ### **5. Medallion Architecture:**
# MAGIC * 🥉 Bronze: Raw data (immutable)
# MAGIC * 🥈 Silver: Cleaned and refined
# MAGIC * 🥇 Gold: Business-level aggregates
# MAGIC * **All layers use Delta format**
# MAGIC
# MAGIC ### **6. Key Operations:**
# MAGIC * CREATE: `df.write.format("delta").save(path)`
# MAGIC * READ: `spark.read.format("delta").load(path)`
# MAGIC * UPDATE: `DeltaTable.forPath().update()`
# MAGIC * DELETE: `DeltaTable.forPath().delete()`
# MAGIC * MERGE: `DeltaTable.forPath().merge()`
# MAGIC
# MAGIC ### **7. Production Best Practices:**
# MAGIC * Use Unity Catalog managed tables
# MAGIC * Enable Auto Optimize
# MAGIC * Regular OPTIMIZE and VACUUM
# MAGIC * Proper partitioning strategy
# MAGIC * Monitor transaction log size
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Interview Questions:
# MAGIC
# MAGIC ### **🟢 Beginner Level:**
# MAGIC
# MAGIC **Q1: What is Delta Lake?**
# MAGIC > A: Delta Lake is an open-source storage layer that brings ACID transactions to Apache Spark and big data workloads. It's built on top of Parquet files and uses a transaction log to provide reliability, consistency, and performance optimizations.
# MAGIC
# MAGIC **Q2: What are the main differences between Delta Lake and Parquet?**
# MAGIC > A: Parquet is a columnar storage format optimized for analytics. Delta Lake uses Parquet for data files but adds: ACID transactions, schema enforcement, time travel, efficient updates/deletes, and unified batch/streaming support via a transaction log.
# MAGIC
# MAGIC **Q3: What does ACID stand for and why is it important?**
# MAGIC > A: ACID stands for Atomicity, Consistency, Isolation, and Durability. It ensures data reliability: transactions either fully complete or fully rollback (A), data stays valid (C), concurrent operations don't interfere (I), and committed data persists (D).
# MAGIC
# MAGIC **Q4: How do you create a Delta table in PySpark?**
# MAGIC > A: `df.write.format("delta").mode("overwrite").save("/path/to/table")` for external tables, or `df.write.format("delta").saveAsTable("catalog.schema.table")` for managed tables.
# MAGIC
# MAGIC **Q5: What is the transaction log in Delta Lake?**
# MAGIC > A: The transaction log (_delta_log) is a directory containing JSON files that record every transaction on the table. Each file represents one version/operation and contains metadata about files added, removed, and table properties. It's the source of truth for table state.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **🟡 Intermediate Level:**
# MAGIC
# MAGIC **Q6: Explain the Medallion Architecture (Bronze-Silver-Gold).**
# MAGIC > A: It's a data design pattern with three layers: Bronze (raw, immutable ingestion), Silver (cleaned, validated, conformed data), and Gold (business-level aggregates). Each layer is a Delta table, enabling ACID guarantees and time travel at every stage.
# MAGIC
# MAGIC **Q7: How does Delta Lake handle concurrent writes?**
# MAGIC > A: Delta uses optimistic concurrency control. Writers read the current version, perform changes, then attempt to commit by writing a new transaction log entry. If another writer committed in between, the operation retries or fails. The atomic write operation (cloud storage PUT) ensures only one writer succeeds.
# MAGIC
# MAGIC **Q8: What is the purpose of checkpoints in Delta Lake?**
# MAGIC > A: Checkpoints aggregate the transaction log state into a single Parquet file (created every 10 versions by default). Instead of reading thousands of JSON log files, readers can start from the latest checkpoint and read only subsequent logs, significantly improving read performance.
# MAGIC
# MAGIC **Q9: How do you perform updates and deletes in Delta Lake?**
# MAGIC > A: Use the DeltaTable API:
# MAGIC ```python
# MAGIC from delta.tables import DeltaTable
# MAGIC dt = DeltaTable.forPath(spark, "path")
# MAGIC dt.update(condition="id = 5", set={"status": "'inactive'"})
# MAGIC dt.delete(condition="created_date < '2020-01-01'")
# MAGIC ```
# MAGIC
# MAGIC **Q10: What is MERGE operation and when would you use it?**
# MAGIC > A: MERGE (upsert) handles inserts and updates in one operation. Use it for CDC (Change Data Capture), slowly changing dimensions, or any scenario where you need to update existing records and insert new ones based on a key match:
# MAGIC ```python
# MAGIC target.merge(source, "target.id = source.id")
# MAGIC   .whenMatchedUpdate(set={...})
# MAGIC   .whenNotMatchedInsert(values={...})
# MAGIC   .execute()
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **🔴 Advanced Level:**
# MAGIC
# MAGIC **Q11: How does Delta Lake achieve Atomicity without a centralized transaction coordinator?**
# MAGIC > A: Delta leverages atomic PUT operations provided by cloud object storage (S3, ADLS, GCS). After writing data files, the transaction commits by atomically writing a log entry. If the log entry write fails or conflicts, the transaction never happened. If it succeeds, the transaction is instantly visible to all readers. The filename (version number) prevents overwrites.
# MAGIC
# MAGIC **Q12: Explain data skipping in Delta Lake and how it improves query performance.**
# MAGIC > A: Delta automatically collects statistics (min/max values, null counts) for each data file and stores them in the transaction log. When queries have filters, Delta reads these statistics first and skips entire files that can't contain matching data. This dramatically reduces I/O without requiring manual indexing.
# MAGIC
# MAGIC **Q13: What are the trade-offs between Snapshot Isolation and Serializable Isolation in Delta?**
# MAGIC > A: Snapshot Isolation (default) allows higher concurrency - readers see a consistent snapshot from when their query started, concurrent writes don't block reads. Serializable Isolation prevents write-write conflicts more strictly but reduces concurrency. SI can have write skew anomalies; Serializable prevents them but may cause more retries.
# MAGIC
# MAGIC **Q14: How would you design a production Delta Lake pipeline that handles late-arriving data and schema evolution?**
# MAGIC > A: 
# MAGIC ```
# MAGIC 1. Bronze: Use Auto Loader with schema inference and evolution enabled
# MAGIC 2. Silver: Use MERGE with WHEN NOT MATCHED BY SOURCE for late arrivals
# MAGIC 3. Enable mergeSchema option for schema evolution
# MAGIC 4. Use watermarking for streaming to handle late events
# MAGIC 5. Implement quarantine tables for schema violations
# MAGIC 6. Monitor transaction log and file sizes
# MAGIC 7. Schedule OPTIMIZE with ZORDER on query columns
# MAGIC 8. Configure appropriate retention for VACUUM
# MAGIC ```
# MAGIC
# MAGIC **Q15: Explain the vacuum process and its retention implications for time travel.**
# MAGIC > A: VACUUM physically deletes data files no longer referenced by the transaction log (old versions, deleted files). Default retention is 7 days. Vacuum preserves time travel within retention period but removes older versions. Running VACUUM with short retention can break long-running queries or time travel. Best practice: set retention >= longest running query duration + desired time travel window.
# MAGIC
# MAGIC **Q16: How do you troubleshoot and optimize a slow Delta Lake query?**
# MAGIC > A:
# MAGIC ```
# MAGIC 1. Check DESCRIBE DETAIL for file count (small file problem)
# MAGIC 2. Run OPTIMIZE to compact files
# MAGIC 3. Analyze query predicates and apply ZORDER
# MAGIC 4. Review partitioning strategy (1GB+ per partition)
# MAGIC 5. Check transaction log size (checkpoint if needed)
# MAGIC 6. Verify data skipping is working (check statistics)
# MAGIC 7. Consider Auto Optimize for write-heavy tables
# MAGIC 8. Review concurrency conflicts in table history
# MAGIC 9. Check if VACUUM is needed (orphaned files)
# MAGIC 10. Monitor query plans for full table scans
# MAGIC ```
# MAGIC
# MAGIC **Q17: What happens under the hood when you run OPTIMIZE ZORDER BY?**
# MAGIC > A: OPTIMIZE reads existing data files, coalesces them into larger files (target: 1GB), and during the write, Z-ORDER applies a space-filling curve algorithm to co-locate related data (multi-dimensional clustering). It then creates a transaction log entry marking old files for removal (logical delete) and new optimized files as added. This improves data skipping for multi-column filters.
# MAGIC
# MAGIC **Q18: How does Delta Lake handle schema evolution safely?**
# MAGIC > A: Delta enforces schema by default - writes with incompatible schemas fail. For evolution: 
# MAGIC - `.option("mergeSchema", "true")` allows adding new columns
# MAGIC - `.option("overwriteSchema", "true")` replaces schema (dangerous)
# MAGIC - Column additions are safe (nulls for old records)
# MAGIC - Column removals require overwriteSchema
# MAGIC - Type changes require explicit casting
# MAGIC - All changes are versioned in transaction log
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚠️ Common Mistakes to Avoid:
# MAGIC
# MAGIC ### **❌ Mistake #1: Using Parquet Instead of Delta**
# MAGIC **Problem:** Lose ACID, versioning, efficient updates, schema enforcement.
# MAGIC **Solution:** Always use Delta in Databricks unless specifically exporting data.
# MAGIC
# MAGIC ### **❌ Mistake #2: Overwriting Bronze Layer**
# MAGIC **Problem:** Lose raw data history, can't reprocess from source.
# MAGIC **Solution:** Bronze should be append-only. Use timestamp/batch_id partitioning.
# MAGIC
# MAGIC ### **❌ Mistake #3: Ignoring Small File Problem**
# MAGIC **Problem:** Thousands of small files slow down queries.
# MAGIC **Solution:** Run OPTIMIZE regularly. Enable Auto Optimize for write-heavy tables.
# MAGIC
# MAGIC ### **❌ Mistake #4: Not Using MERGE for Upserts**
# MAGIC **Problem:** Expensive full table rewrites or complex logic.
# MAGIC **Solution:** Use Delta's native MERGE operation for CDC and updates.
# MAGIC
# MAGIC ### **❌ Mistake #5: Aggressive VACUUM**
# MAGIC **Problem:** Breaking time travel, crashing long-running queries.
# MAGIC **Solution:** Set retention >= longest query time + desired time travel window. Default 7 days is usually good.
# MAGIC
# MAGIC ### **❌ Mistake #6: Poor Partitioning Strategy**
# MAGIC **Problem:** Too many small partitions or too few large ones.
# MAGIC **Solution:** Aim for 1GB+ per partition. Partition on commonly filtered columns.
# MAGIC
# MAGIC ### **❌ Mistake #7: Not Monitoring Transaction Log**
# MAGIC **Problem:** Log grows too large, slow reads.
# MAGIC **Solution:** Monitor checkpoint creation. Manually checkpoint if needed.
# MAGIC
# MAGIC ### **❌ Mistake #8: Skipping Schema Enforcement**
# MAGIC **Problem:** Bad data enters pipeline, causes downstream failures.
# MAGIC **Solution:** Let Delta enforce schema. Use explicit schema definition.
# MAGIC
# MAGIC ### **❌ Mistake #9: Using Cache/Persist with Delta**
# MAGIC **Problem:** Stale cached data, wasted cluster memory.
# MAGIC **Solution:** Delta's data skipping is more efficient. Avoid cache/persist.
# MAGIC
# MAGIC ### **❌ Mistake #10: Not Using Unity Catalog**
# MAGIC **Problem:** No centralized governance, access control, or discovery.
# MAGIC **Solution:** Always create managed tables in Unity Catalog for production.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Next Steps in Your Delta Lake Journey:
# MAGIC
# MAGIC ### **Tomorrow (Day 20):**
# MAGIC * **Delta Lake Time Travel** - Query historical versions, RESTORE
# MAGIC * **Advanced DML** - Complex MERGE, DELETE, UPDATE scenarios
# MAGIC
# MAGIC ### **Upcoming Topics:**
# MAGIC * **Delta Live Tables** (Lakeflow Spark Declarative Pipelines)
# MAGIC * **Streaming with Delta Lake**
# MAGIC * **Change Data Feed (CDF)**
# MAGIC * **Delta Sharing**
# MAGIC * **Liquid Clustering**
# MAGIC * **Unity Catalog Integration**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Key Takeaway:
# MAGIC
# MAGIC > **Delta Lake transforms data lakes into Lakehouses by adding ACID transactions, schema enforcement, and performance optimizations on top of existing object storage. The transaction log is the foundation that enables all Delta Lake features while maintaining simplicity and scalability.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Additional Resources:
# MAGIC
# MAGIC * **Delta Lake Official Docs:** https://docs.delta.io/
# MAGIC * **Databricks Delta Guide:** https://docs.databricks.com/delta/
# MAGIC * **Delta Lake GitHub:** https://github.com/delta-io/delta
# MAGIC * **Best Practices:** https://docs.databricks.com/delta/best-practices.html
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Training Complete!
# MAGIC
# MAGIC **Watermark: @TRRaveendra**
# MAGIC
# MAGIC You now understand:
# MAGIC * ✅ ACID transactions in Delta Lake
# MAGIC * ✅ Delta Lake architecture and transaction log
# MAGIC * ✅ Delta vs Parquet differences
# MAGIC * ✅ Creating and managing Delta tables
# MAGIC * ✅ Medallion architecture (Bronze-Silver-Gold)
# MAGIC * ✅ Production best practices
# MAGIC * ✅ Common pitfalls and how to avoid them
# MAGIC
# MAGIC **Keep practicing and building Delta Lake pipelines! 🚀**

# COMMAND ----------

# DBTITLE 1,Quick Reference - Delta Lake Cheat Sheet
# MAGIC %md
# MAGIC # 📑 BONUS: Delta Lake Quick Reference Cheat Sheet
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Common Delta Lake Commands:
# MAGIC
# MAGIC ### **CREATE Operations:**
# MAGIC
# MAGIC ```python
# MAGIC # Create Delta table from DataFrame
# MAGIC df.write.format("delta").mode("overwrite").save("/path/to/table")
# MAGIC
# MAGIC # Create partitioned Delta table
# MAGIC df.write.format("delta") \
# MAGIC   .partitionBy("date", "region") \
# MAGIC   .save("/path/to/table")
# MAGIC
# MAGIC # Create managed table
# MAGIC df.write.format("delta").saveAsTable("catalog.schema.table")
# MAGIC
# MAGIC # Create table with SQL
# MAGIC spark.sql("""
# MAGIC   CREATE TABLE catalog.schema.table
# MAGIC   USING DELTA
# MAGIC   LOCATION '/path'
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **READ Operations:**
# MAGIC
# MAGIC ```python
# MAGIC # Read Delta table
# MAGIC df = spark.read.format("delta").load("/path/to/table")
# MAGIC
# MAGIC # Read managed table
# MAGIC df = spark.table("catalog.schema.table")
# MAGIC
# MAGIC # Read specific version (Time Travel)
# MAGIC df = spark.read.format("delta").option("versionAsOf", 5).load("/path")
# MAGIC
# MAGIC # Read as of timestamp
# MAGIC df = spark.read.format("delta") \
# MAGIC   .option("timestampAsOf", "2024-04-20") \
# MAGIC   .load("/path")
# MAGIC
# MAGIC # Read with SQL
# MAGIC spark.sql("SELECT * FROM catalog.schema.table")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **UPDATE Operations:**
# MAGIC
# MAGIC ```python
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC dt = DeltaTable.forPath(spark, "/path/to/table")
# MAGIC
# MAGIC # Update records
# MAGIC dt.update(
# MAGIC   condition = "status = 'pending'",
# MAGIC   set = {"status": "'completed'", "updated_at": "current_timestamp()"}
# MAGIC )
# MAGIC
# MAGIC # Update with SQL
# MAGIC spark.sql("""
# MAGIC   UPDATE catalog.schema.table
# MAGIC   SET status = 'completed'
# MAGIC   WHERE status = 'pending'
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **DELETE Operations:**
# MAGIC
# MAGIC ```python
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC dt = DeltaTable.forPath(spark, "/path/to/table")
# MAGIC
# MAGIC # Delete records
# MAGIC dt.delete("date < '2020-01-01'")
# MAGIC
# MAGIC # Delete with SQL
# MAGIC spark.sql("""
# MAGIC   DELETE FROM catalog.schema.table
# MAGIC   WHERE date < '2020-01-01'
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **MERGE (Upsert) Operations:**
# MAGIC
# MAGIC ```python
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC target = DeltaTable.forPath(spark, "/path/to/target")
# MAGIC
# MAGIC target.alias("target").merge(
# MAGIC   source.alias("source"),
# MAGIC   "target.customer_id = source.customer_id"
# MAGIC ).whenMatchedUpdate(set = {
# MAGIC   "status": "source.status",
# MAGIC   "updated_at": "current_timestamp()"
# MAGIC }).whenNotMatchedInsert(values = {
# MAGIC   "customer_id": "source.customer_id",
# MAGIC   "status": "source.status",
# MAGIC   "created_at": "current_timestamp()"
# MAGIC }).execute()
# MAGIC
# MAGIC # MERGE with SQL
# MAGIC spark.sql("""
# MAGIC   MERGE INTO target t
# MAGIC   USING source s
# MAGIC   ON t.id = s.id
# MAGIC   WHEN MATCHED THEN UPDATE SET *
# MAGIC   WHEN NOT MATCHED THEN INSERT *
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **OPTIMIZE Operations:**
# MAGIC
# MAGIC ```python
# MAGIC # Basic optimize
# MAGIC spark.sql("OPTIMIZE catalog.schema.table")
# MAGIC
# MAGIC # Optimize with Z-ORDER
# MAGIC spark.sql("""
# MAGIC   OPTIMIZE catalog.schema.table
# MAGIC   ZORDER BY (customer_id, transaction_date)
# MAGIC """)
# MAGIC
# MAGIC # Enable Auto Optimize
# MAGIC spark.sql("""
# MAGIC   ALTER TABLE catalog.schema.table
# MAGIC   SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC   )
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **VACUUM Operations:**
# MAGIC
# MAGIC ```python
# MAGIC # Vacuum (default 7 days retention)
# MAGIC spark.sql("VACUUM catalog.schema.table")
# MAGIC
# MAGIC # Vacuum with custom retention (in hours)
# MAGIC spark.sql("VACUUM catalog.schema.table RETAIN 168 HOURS")  # 7 days
# MAGIC
# MAGIC # Dry run (see what would be deleted)
# MAGIC spark.sql("VACUUM catalog.schema.table DRY RUN")
# MAGIC
# MAGIC # Using DeltaTable API
# MAGIC from delta.tables import DeltaTable
# MAGIC dt = DeltaTable.forPath(spark, "/path")
# MAGIC dt.vacuum(168)  # hours
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **HISTORY & METADATA Operations:**
# MAGIC
# MAGIC ```python
# MAGIC # View table history
# MAGIC spark.sql("DESCRIBE HISTORY catalog.schema.table")
# MAGIC
# MAGIC # View table details
# MAGIC spark.sql("DESCRIBE DETAIL catalog.schema.table")
# MAGIC
# MAGIC # View table schema
# MAGIC spark.sql("DESCRIBE catalog.schema.table")
# MAGIC
# MAGIC # View table properties
# MAGIC spark.sql("SHOW TBLPROPERTIES catalog.schema.table")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **RESTORE Operations:**
# MAGIC
# MAGIC ```python
# MAGIC # Restore to specific version
# MAGIC spark.sql("""
# MAGIC   RESTORE TABLE catalog.schema.table
# MAGIC   TO VERSION AS OF 10
# MAGIC """)
# MAGIC
# MAGIC # Restore to timestamp
# MAGIC spark.sql("""
# MAGIC   RESTORE TABLE catalog.schema.table
# MAGIC   TO TIMESTAMP AS OF '2024-04-20 10:00:00'
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **SCHEMA Operations:**
# MAGIC
# MAGIC ```python
# MAGIC # Add column
# MAGIC spark.sql("""
# MAGIC   ALTER TABLE catalog.schema.table
# MAGIC   ADD COLUMN new_column STRING
# MAGIC """)
# MAGIC
# MAGIC # Rename column
# MAGIC spark.sql("""
# MAGIC   ALTER TABLE catalog.schema.table
# MAGIC   RENAME COLUMN old_name TO new_name
# MAGIC """)
# MAGIC
# MAGIC # Drop column
# MAGIC spark.sql("""
# MAGIC   ALTER TABLE catalog.schema.table
# MAGIC   DROP COLUMN column_name
# MAGIC """)
# MAGIC
# MAGIC # Change column type (requires rewrite)
# MAGIC spark.sql("""
# MAGIC   ALTER TABLE catalog.schema.table
# MAGIC   ALTER COLUMN column_name TYPE BIGINT
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **STREAMING Operations:**
# MAGIC
# MAGIC ```python
# MAGIC # Write stream to Delta
# MAGIC stream_df.writeStream \
# MAGIC   .format("delta") \
# MAGIC   .outputMode("append") \
# MAGIC   .option("checkpointLocation", "/checkpoint/path") \
# MAGIC   .start("/path/to/table")
# MAGIC
# MAGIC # Read stream from Delta
# MAGIC stream_df = spark.readStream \
# MAGIC   .format("delta") \
# MAGIC   .load("/path/to/table")
# MAGIC
# MAGIC # Streaming with MERGE (foreachBatch)
# MAGIC def upsert_to_delta(batch_df, batch_id):
# MAGIC     batch_df.createOrReplaceTempView("updates")
# MAGIC     spark.sql("""
# MAGIC         MERGE INTO target t
# MAGIC         USING updates s ON t.id = s.id
# MAGIC         WHEN MATCHED THEN UPDATE SET *
# MAGIC         WHEN NOT MATCHED THEN INSERT *
# MAGIC     """)
# MAGIC
# MAGIC stream_df.writeStream \
# MAGIC   .foreachBatch(upsert_to_delta) \
# MAGIC   .start()
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **CONVERSION Operations:**
# MAGIC
# MAGIC ```python
# MAGIC # Convert Parquet to Delta (in-place)
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC DeltaTable.convertToDelta(
# MAGIC   spark,
# MAGIC   "parquet.`/path/to/parquet`",
# MAGIC   "date DATE, region STRING"  # partition schema
# MAGIC )
# MAGIC
# MAGIC # Convert with SQL
# MAGIC spark.sql("""
# MAGIC   CONVERT TO DELTA parquet.`/path/to/parquet`
# MAGIC   PARTITIONED BY (date DATE, region STRING)
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **TABLE PROPERTIES:**
# MAGIC
# MAGIC ```python
# MAGIC # Set table properties
# MAGIC spark.sql("""
# MAGIC   ALTER TABLE catalog.schema.table
# MAGIC   SET TBLPROPERTIES (
# MAGIC     'delta.logRetentionDuration' = '30 days',
# MAGIC     'delta.deletedFileRetentionDuration' = '7 days',
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'delta.columnMapping.mode' = 'name',
# MAGIC     'delta.minReaderVersion' = '2',
# MAGIC     'delta.minWriterVersion' = '5'
# MAGIC   )
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Configuration Settings:
# MAGIC
# MAGIC ```python
# MAGIC # Session-level configurations
# MAGIC spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
# MAGIC spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
# MAGIC spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
# MAGIC spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚡ Performance Best Practices:
# MAGIC
# MAGIC | Scenario | Recommendation |
# MAGIC |----------|----------------|
# MAGIC | **Small files** | Run OPTIMIZE weekly |
# MAGIC | **High cardinality filters** | Use ZORDER on filter columns |
# MAGIC | **Write-heavy workload** | Enable Auto Optimize |
# MAGIC | **Large tables** | Partition on commonly filtered column |
# MAGIC | **Concurrent writes** | Use MERGE instead of separate INSERT/UPDATE |
# MAGIC | **Time travel not needed** | Run VACUUM with 7-day retention |
# MAGIC | **Streaming** | Use foreachBatch for complex logic |
# MAGIC | **Schema changes** | Use mergeSchema option cautiously |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔒 Security & Governance:
# MAGIC
# MAGIC ```sql
# MAGIC -- Grant permissions (Unity Catalog)
# MAGIC GRANT SELECT ON TABLE catalog.schema.table TO `user@domain.com`;
# MAGIC GRANT MODIFY ON TABLE catalog.schema.table TO `group_name`;
# MAGIC
# MAGIC -- Row-level security (Dynamic Views)
# MAGIC CREATE VIEW catalog.schema.filtered_view AS
# MAGIC SELECT * FROM catalog.schema.table
# MAGIC WHERE region = current_user();
# MAGIC
# MAGIC -- Column masking
# MAGIC CREATE VIEW catalog.schema.masked_view AS
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   CASE WHEN is_member('admins') THEN ssn ELSE 'XXX-XX-XXXX' END as ssn,
# MAGIC   name
# MAGIC FROM catalog.schema.table;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Monitoring Queries:
# MAGIC
# MAGIC ```sql
# MAGIC -- Check table size and file count
# MAGIC SELECT 
# MAGIC   format,
# MAGIC   numFiles,
# MAGIC   ROUND(sizeInBytes / 1024 / 1024 / 1024, 2) as sizeInGB
# MAGIC FROM delta.`/path/to/table`;
# MAGIC
# MAGIC -- Find tables needing optimization
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   numFiles,
# MAGIC   CASE 
# MAGIC     WHEN numFiles > 1000 THEN 'OPTIMIZE NOW'
# MAGIC     WHEN numFiles > 500 THEN 'OPTIMIZE SOON'
# MAGIC     ELSE 'OK'
# MAGIC   END as recommendation
# MAGIC FROM (
# MAGIC   SELECT 'my_table' as table_name, 
# MAGIC          (SELECT COUNT(*) FROM delta.`/path`) as numFiles
# MAGIC );
# MAGIC
# MAGIC -- Check operation history
# MAGIC SELECT 
# MAGIC   version,
# MAGIC   timestamp,
# MAGIC   operation,
# MAGIC   operationMetrics
# MAGIC FROM (DESCRIBE HISTORY catalog.schema.table)
# MAGIC ORDER BY version DESC
# MAGIC LIMIT 10;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Essential Commands Summary:
# MAGIC
# MAGIC ```bash
# MAGIC # Create
# MAGIC df.write.format("delta").save(path)
# MAGIC
# MAGIC # Read
# MAGIC spark.read.format("delta").load(path)
# MAGIC
# MAGIC # Update
# MAGIC DeltaTable.forPath().update()
# MAGIC
# MAGIC # Delete
# MAGIC DeltaTable.forPath().delete()
# MAGIC
# MAGIC # Merge
# MAGIC DeltaTable.forPath().merge().execute()
# MAGIC
# MAGIC # Optimize
# MAGIC OPTIMIZE table ZORDER BY (col)
# MAGIC
# MAGIC # Vacuum
# MAGIC VACUUM table RETAIN 168 HOURS
# MAGIC
# MAGIC # History
# MAGIC DESCRIBE HISTORY table
# MAGIC
# MAGIC # Time Travel
# MAGIC SELECT * FROM table VERSION AS OF 5
# MAGIC
# MAGIC # Restore
# MAGIC RESTORE TABLE table TO VERSION AS OF 10
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📌 Print This Reference!
# MAGIC
# MAGIC **Bookmark this cell for quick access to Delta Lake commands.**
# MAGIC
# MAGIC **Watermark: @TRRaveendra**