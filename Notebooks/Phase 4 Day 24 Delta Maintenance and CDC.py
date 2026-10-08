# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # 🧊 Data Engineering Training — Phase 4 Day 24  
# MAGIC ## 🔧 Delta Maintenance & CDC: VACUUM, OPTIMIZE & Change Data Feed  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - VACUUM (Data Cleanup)  
# MAGIC - OPTIMIZE (File Compaction)  
# MAGIC - Small File Problem  
# MAGIC - Change Data Feed (CDF)  
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
# MAGIC Understand how to maintain Delta tables, solve small file problems, and implement Change Data Feed (CDF) for incremental processing.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Engineering Constraints:
# MAGIC - ✅ Databricks Serverless Compute  
# MAGIC - ✅ DataFrame API only (NO RDDs)  
# MAGIC - ✅ NO cache() / persist()  
# MAGIC - ✅ NO /tmp or local storage  
# MAGIC - ✅ Unity Catalog managed tables  
# MAGIC - ✅ Delta-first and incremental design

# COMMAND ----------

# DBTITLE 1,Section 1: Delta Maintenance Overview
# MAGIC %md
# MAGIC # 📋 Section 1: Delta Maintenance Overview
# MAGIC
# MAGIC ## 🧒 ELI5 (Explain Like I'm 5):
# MAGIC Imagine your toy box gets messier over time. You have:
# MAGIC - Lots of tiny toys scattered everywhere (small files)  
# MAGIC - Old broken toys you don't need anymore (old data versions)  
# MAGIC
# MAGIC **Maintenance** means:
# MAGIC 1. **Organizing** toys into bigger containers (OPTIMIZE)  
# MAGIC 2. **Throwing away** broken toys (VACUUM)  
# MAGIC 3. **Tracking** what changed (Change Data Feed)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### Why Delta Maintenance is Critical:
# MAGIC
# MAGIC **1. Data Growth Challenges:**
# MAGIC - Delta tables accumulate files over time  
# MAGIC - Each write operation creates new files  
# MAGIC - Small files lead to poor query performance  
# MAGIC - Old versions consume storage unnecessarily  
# MAGIC
# MAGIC **2. Performance Degradation:**
# MAGIC - Metadata overhead increases with file count  
# MAGIC - Query planning takes longer  
# MAGIC - I/O operations become inefficient  
# MAGIC - Spark driver memory pressure  
# MAGIC
# MAGIC **3. Storage Cost:**
# MAGIC - Time travel data accumulates  
# MAGIC - Uncommitted files remain  
# MAGIC - Storage costs increase linearly  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Delta Maintenance Operations:
# MAGIC
# MAGIC | Operation | Purpose | Impact |
# MAGIC | --- | --- | --- |
# MAGIC | OPTIMIZE | Compact small files | Improves read performance |
# MAGIC | VACUUM | Remove old files | Reduces storage costs |
# MAGIC | Z-ORDER | Co-locate data | Accelerates queries |
# MAGIC | CDF | Track changes | Enables incremental processing |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Best Practices:
# MAGIC - Run OPTIMIZE regularly (daily/weekly)  
# MAGIC - VACUUM with appropriate retention period  
# MAGIC - Enable Auto Optimize for streaming tables  
# MAGIC - Monitor file sizes and counts  
# MAGIC - Use CDF for incremental pipelines

# COMMAND ----------

# DBTITLE 1,Section 2: The Small File Problem
# MAGIC %md
# MAGIC # 📁 Section 2: The Small File Problem
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC Imagine reading 100 tiny notes vs 1 book with all information:
# MAGIC - Reading 100 notes = slow, lots of opening/closing  
# MAGIC - Reading 1 book = fast, efficient  
# MAGIC
# MAGIC **Many small files = slow queries!**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ What Causes Small Files?
# MAGIC
# MAGIC ### 1. Streaming Writes:
# MAGIC ```
# MAGIC Micro-batch 1 → 10 MB file  
# MAGIC Micro-batch 2 → 8 MB file  
# MAGIC Micro-batch 3 → 12 MB file  
# MAGIC ...
# MAGIC After 1000 batches → 1000 small files!
# MAGIC ```
# MAGIC
# MAGIC ### 2. Frequent Small Batch Loads:
# MAGIC ```python
# MAGIC # Anti-pattern: Writing small batches frequently
# MAGIC for batch in small_batches:
# MAGIC     batch.write.mode("append").saveAsTable("table")
# MAGIC     # Each write creates new files!
# MAGIC ```
# MAGIC
# MAGIC ### 3. High Cardinality Partitioning:
# MAGIC ```
# MAGIC Partitioned by: date, hour, user_id
# MAGIC Result: 1000s of tiny partitions
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📉 Performance Impact:
# MAGIC
# MAGIC ### Query Performance Degradation:
# MAGIC
# MAGIC | File Count | Query Time | Metadata Size |
# MAGIC | --- | --- | --- |
# MAGIC | 10 files | 2 seconds | 1 KB |
# MAGIC | 1,000 files | 15 seconds | 100 KB |
# MAGIC | 10,000 files | 120 seconds | 10 MB |
# MAGIC | 100,000 files | 600+ seconds | 100+ MB |
# MAGIC
# MAGIC ### Why It's Slow:
# MAGIC 1. **Metadata Overhead**: Listing thousands of files  
# MAGIC 2. **I/O Operations**: Opening many small files  
# MAGIC 3. **Task Scheduling**: More tasks = more overhead  
# MAGIC 4. **Driver Memory**: Metadata stored in driver  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Solution: OPTIMIZE + Auto Optimize
# MAGIC
# MAGIC ```sql
# MAGIC -- Manual compaction
# MAGIC OPTIMIZE catalog.schema.table_name;
# MAGIC
# MAGIC -- Enable Auto Optimize (recommended for streaming)
# MAGIC ALTER TABLE catalog.schema.table_name
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3: OPTIMIZE Command
# MAGIC %md
# MAGIC # ⚡ Section 3: OPTIMIZE (File Compaction)
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC OPTIMIZE is like organizing scattered papers into neat folders:
# MAGIC - Before: 1000 small papers (hard to find anything)  
# MAGIC - After: 10 organized folders (easy to navigate)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ How OPTIMIZE Works:
# MAGIC
# MAGIC ### Process:
# MAGIC ```
# MAGIC Step 1: Identify small files (< 128 MB)  
# MAGIC Step 2: Read small files  
# MAGIC Step 3: Combine into larger files (∼1 GB)  
# MAGIC Step 4: Write new optimized files  
# MAGIC Step 5: Update Delta log  
# MAGIC Step 6: Mark old files for deletion
# MAGIC ```
# MAGIC
# MAGIC ### Architecture:
# MAGIC ```
# MAGIC BEFORE OPTIMIZE:
# MAGIC Partition A: [10MB] [8MB] [12MB] [15MB] [9MB] ... (100 files)
# MAGIC
# MAGIC AFTER OPTIMIZE:
# MAGIC Partition A: [1GB] [1GB] (2 files)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ OPTIMIZE Syntax:
# MAGIC
# MAGIC ### Basic Optimize:
# MAGIC ```sql
# MAGIC OPTIMIZE catalog.schema.table_name;
# MAGIC ```
# MAGIC
# MAGIC ### Optimize with Z-ORDER:
# MAGIC ```sql
# MAGIC OPTIMIZE catalog.schema.table_name
# MAGIC ZORDER BY (date, customer_id);
# MAGIC ```
# MAGIC
# MAGIC ### Optimize Specific Partition:
# MAGIC ```sql
# MAGIC OPTIMIZE catalog.schema.table_name
# MAGIC WHERE date >= '2026-04-01';
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📈 Performance Benefits:
# MAGIC
# MAGIC | Metric | Before | After | Improvement |
# MAGIC | --- | --- | --- | --- |
# MAGIC | File Count | 10,000 | 100 | 99% reduction |
# MAGIC | Query Time | 120s | 8s | 93% faster |
# MAGIC | Metadata Size | 10 MB | 100 KB | 99% reduction |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⏰ When to Run OPTIMIZE:
# MAGIC
# MAGIC 1. **After streaming jobs** (daily/hourly)  
# MAGIC 2. **After large batch loads**  
# MAGIC 3. **When query performance degrades**  
# MAGIC 4. **Scheduled maintenance windows**  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚨 Important Notes:
# MAGIC
# MAGIC - OPTIMIZE is **idempotent** (safe to run multiple times)  
# MAGIC - Does NOT change data (only file layout)  
# MAGIC - Old files remain for time travel  
# MAGIC - Use VACUUM to remove old files later

# COMMAND ----------

# DBTITLE 1,Demo: Setup Environment
# Setup: Create catalog and schema for demonstrations

catalog_name = "main"  # Using default catalog
schema_name = "delta_maintenance_demo"

# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

print(f"✅ Schema created: {catalog_name}.{schema_name}")
print(f"📊 Ready for Delta maintenance demonstrations")

# COMMAND ----------

# DBTITLE 1,Demo: Create Table with Small Files
# Demonstrate the small file problem by creating many small files

from pyspark.sql.functions import col, lit, rand, expr
import time

table_name = f"{catalog_name}.{schema_name}.sales_data"

# Drop table if exists
spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# Simulate multiple small batch writes (creating small files)
print("🔄 Simulating 10 small batch writes...")

for i in range(10):
    # Generate small dataset (1000 rows per batch)
    df_batch = spark.range(1000).select(
        (col("id") + lit(i * 1000)).alias("order_id"),
        expr("date_add('2026-04-01', cast(rand() * 20 as int))").alias("order_date"),
        (expr("rand() * 1000") + 100).alias("amount"),
        expr("concat('customer_', cast(rand() * 100 as int))").alias("customer_id")
    )
    
    # Write small batch (creates small files)
    if i == 0:
        df_batch.write.format("delta").mode("overwrite").saveAsTable(table_name)
    else:
        df_batch.write.format("delta").mode("append").saveAsTable(table_name)
    
    print(f"  ✓ Batch {i+1} written")

print(f"\n✅ Created table: {table_name}")
print("⚠️  This table now has MANY small files (small file problem!)")

# Check file statistics
file_stats = spark.sql(f"DESCRIBE DETAIL {table_name}").select("numFiles", "sizeInBytes").collect()[0]
print(f"\n📁 File Count: {file_stats['numFiles']}")
print(f"💾 Total Size: {file_stats['sizeInBytes'] / (1024*1024):.2f} MB")

# COMMAND ----------

# DBTITLE 1,Demo: Inspect File Details Before OPTIMIZE
# MAGIC %sql
# MAGIC -- Inspect file details before optimization
# MAGIC DESCRIBE DETAIL main.delta_maintenance_demo.sales_data

# COMMAND ----------

# DBTITLE 1,Demo: Run OPTIMIZE
# MAGIC %sql
# MAGIC -- Run OPTIMIZE to compact small files
# MAGIC OPTIMIZE main.delta_maintenance_demo.sales_data

# COMMAND ----------

# DBTITLE 1,Demo: Inspect File Details After OPTIMIZE
# MAGIC %sql
# MAGIC -- Check file statistics after optimization
# MAGIC DESCRIBE DETAIL main.delta_maintenance_demo.sales_data

# COMMAND ----------

# DBTITLE 1,Demo: Compare Before and After OPTIMIZE
# Compare file statistics before and after OPTIMIZE

print("📊 OPTIMIZE Impact Analysis:")
print("="*60)
print("\n🔍 Expected Results:")
print("  • File count: REDUCED significantly")
print("  • Average file size: INCREASED")
print("  • Query performance: IMPROVED")
print("  • Total data size: UNCHANGED (same data)")
print("\n✨ Files have been compacted into larger, optimized files!")
print("\n💡 Note: Old small files still exist (for time travel)")
print("   Use VACUUM to remove them after retention period.")

# COMMAND ----------

# DBTITLE 1,Section 4: VACUUM Command
# MAGIC %md
# MAGIC # 🧹 Section 4: VACUUM (Data Cleanup)
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC VACUUM is like cleaning out your attic:
# MAGIC - You have old versions of files you don't need anymore  
# MAGIC - They take up space  
# MAGIC - VACUUM throws them away to free up space  
# MAGIC
# MAGIC **But be careful!** Once vacuumed, you can't go back in time to see old versions.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ How VACUUM Works:
# MAGIC
# MAGIC ### What VACUUM Removes:
# MAGIC 1. **Old data files** (replaced by OPTIMIZE)  
# MAGIC 2. **Uncommitted files** (failed writes)  
# MAGIC 3. **Files older than retention period**  
# MAGIC
# MAGIC ### What VACUUM Keeps:
# MAGIC - Files needed by current version  
# MAGIC - Files within retention window  
# MAGIC - Active transaction files  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ VACUUM Syntax:
# MAGIC
# MAGIC ### Basic Vacuum (168 hours = 7 days default):
# MAGIC ```sql
# MAGIC VACUUM catalog.schema.table_name;
# MAGIC ```
# MAGIC
# MAGIC ### Vacuum with Custom Retention:
# MAGIC ```sql
# MAGIC VACUUM catalog.schema.table_name RETAIN 24 HOURS;
# MAGIC ```
# MAGIC
# MAGIC ### Dry Run (see what would be deleted):
# MAGIC ```sql
# MAGIC VACUUM catalog.schema.table_name DRY RUN;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⏰ Retention Period Guidelines:
# MAGIC
# MAGIC | Use Case | Retention | Rationale |
# MAGIC | --- | --- | --- |
# MAGIC | Production (critical) | 168 hours (7 days) | Safe default, allows rollback |
# MAGIC | Development | 24 hours | Faster cleanup, less storage |
# MAGIC | Compliance data | 720 hours (30 days) | Audit requirements |
# MAGIC | Archive tables | 8760 hours (1 year) | Long-term history |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚠️ VACUUM Impact on Time Travel:
# MAGIC
# MAGIC ```
# MAGIC Timeline:
# MAGIC -----------•---------•---------•---------•---------→
# MAGIC          v1        v2        v3        v4      Current
# MAGIC          
# MAGIC After VACUUM (RETAIN 48 HOURS):
# MAGIC -----------X---------X---------•---------•---------→
# MAGIC       (deleted) (deleted)     v3        v4      Current
# MAGIC       
# MAGIC Time Travel:
# MAGIC ✅ SELECT * FROM table VERSION AS OF 4  → Works
# MAGIC ✅ SELECT * FROM table VERSION AS OF 3  → Works
# MAGIC ❌ SELECT * FROM table VERSION AS OF 2  → FAILS (vacuumed)
# MAGIC ❌ SELECT * FROM table VERSION AS OF 1  → FAILS (vacuumed)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚨 VACUUM Warnings:
# MAGIC
# MAGIC 1. **Cannot undo VACUUM** - files are permanently deleted  
# MAGIC 2. **Breaks time travel** - old versions become inaccessible  
# MAGIC 3. **Can break concurrent readers** - if retention too short  
# MAGIC 4. **Requires full table scan** - can be expensive  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Best Practices:
# MAGIC
# MAGIC 1. **Run VACUUM regularly** (weekly/monthly)  
# MAGIC 2. **Never use RETAIN 0 HOURS** in production  
# MAGIC 3. **Coordinate with long-running queries**  
# MAGIC 4. **Test with DRY RUN first**  
# MAGIC 5. **Monitor storage savings**  
# MAGIC 6. **Document retention policies**

# COMMAND ----------

# DBTITLE 1,Demo: VACUUM Dry Run
# MAGIC %sql
# MAGIC -- Dry run to see what files would be deleted
# MAGIC -- Note: VACUUM default retention is 168 hours (7 days)
# MAGIC -- This is safe for production use
# MAGIC
# MAGIC -- Dry run to preview deletions (using default 7-day retention)
# MAGIC VACUUM main.delta_maintenance_demo.sales_data DRY RUN

# COMMAND ----------

# DBTITLE 1,Demo: Execute VACUUM
# MAGIC %sql
# MAGIC -- Execute VACUUM to remove old files
# MAGIC -- Using default 168 hours (7 days) retention period
# MAGIC -- This is the recommended production setting
# MAGIC
# MAGIC VACUUM main.delta_maintenance_demo.sales_data

# COMMAND ----------

# DBTITLE 1,Demo: Check Storage After VACUUM
# Check storage metrics after VACUUM

file_stats = spark.sql(f"DESCRIBE DETAIL {table_name}").select("numFiles", "sizeInBytes").collect()[0]

print("🧹 VACUUM Results:")
print("="*60)
print(f"📁 Current File Count: {file_stats['numFiles']}")
print(f"💾 Current Size: {file_stats['sizeInBytes'] / (1024*1024):.2f} MB")
print("\n✨ Old files removed - storage optimized!")
print("\n📊 Note: With 7-day retention, files older than 7 days are removed")
print("   This maintains time travel capability for recent versions")

# COMMAND ----------

# DBTITLE 1,Section 5: Change Data Feed (CDF)
# MAGIC %md
# MAGIC # 🔄 Section 5: Change Data Feed (CDF)
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC Change Data Feed is like a diary that tracks every change:
# MAGIC - **Insert**: "New toy added"  
# MAGIC - **Update**: "Changed toy color from red to blue"  
# MAGIC - **Delete**: "Removed broken toy"  
# MAGIC
# MAGIC Instead of reading everything again, you just read the diary to see what changed!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ What is Change Data Feed?
# MAGIC
# MAGIC ### Definition:
# MAGIC Change Data Feed (CDF) captures **row-level changes** (inserts, updates, deletes) to Delta tables.
# MAGIC
# MAGIC ### Why Use CDF?
# MAGIC
# MAGIC 1. **Incremental Processing**: Process only changed data  
# MAGIC 2. **Audit Trails**: Track all modifications  
# MAGIC 3. **Downstream Sync**: Keep derived tables in sync  
# MAGIC 4. **Event Streams**: Publish changes to Kafka/Event Hubs  
# MAGIC 5. **Cost Efficiency**: Process less data = lower costs  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 CDF Architecture:
# MAGIC
# MAGIC ```
# MAGIC Source Table (CDF Enabled)
# MAGIC      ↓
# MAGIC   [Change Log]
# MAGIC      ↓
# MAGIC Captures:
# MAGIC   • _change_type: insert, update, delete  
# MAGIC   • _commit_version: Delta version number  
# MAGIC   • _commit_timestamp: When change occurred  
# MAGIC      ↓
# MAGIC Incremental Consumers
# MAGIC   • Data pipelines  
# MAGIC   • Analytics systems  
# MAGIC   • ML feature stores
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Enable CDF:
# MAGIC
# MAGIC ### For New Tables:
# MAGIC ```sql
# MAGIC CREATE TABLE catalog.schema.table_name (
# MAGIC   id BIGINT,
# MAGIC   name STRING,
# MAGIC   amount DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC ```
# MAGIC
# MAGIC ### For Existing Tables:
# MAGIC ```sql
# MAGIC ALTER TABLE catalog.schema.table_name
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 CDF Metadata Columns:
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | _change_type | string | insert, update_preimage, update_postimage, delete |
# MAGIC | _commit_version | long | Delta version number |
# MAGIC | _commit_timestamp | timestamp | When change was committed |
# MAGIC
# MAGIC ### Change Types:
# MAGIC - **insert**: New row added  
# MAGIC - **update_preimage**: Row before update  
# MAGIC - **update_postimage**: Row after update  
# MAGIC - **delete**: Row removed  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Use Cases:
# MAGIC
# MAGIC ### 1. Incremental ETL:
# MAGIC ```sql
# MAGIC -- Process only changes since last load
# MAGIC CREATE TABLE target AS
# MAGIC SELECT * FROM source_table
# MAGIC WHERE _commit_version > ${last_processed_version};
# MAGIC ```
# MAGIC
# MAGIC ### 2. Slowly Changing Dimensions (SCD Type 2):
# MAGIC ```sql
# MAGIC -- Track historical changes
# MAGIC INSERT INTO dimension_history
# MAGIC SELECT *, _commit_timestamp as valid_from
# MAGIC FROM source_table
# MAGIC WHERE _change_type = 'update_postimage';
# MAGIC ```
# MAGIC
# MAGIC ### 3. Audit Log:
# MAGIC ```sql
# MAGIC -- Capture all modifications
# MAGIC CREATE TABLE audit_log AS
# MAGIC SELECT *, current_user() as modified_by
# MAGIC FROM source_table
# MAGIC WHERE _change_type IS NOT NULL;
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Demo: Create CDF-Enabled Table
# Create a table with Change Data Feed enabled

from pyspark.sql.functions import current_timestamp

cdf_table_name = f"{catalog_name}.{schema_name}.customer_transactions"

# Drop table if exists
spark.sql(f"DROP TABLE IF EXISTS {cdf_table_name}")

# Create initial dataset
df_initial = spark.range(1000).select(
    col("id").alias("transaction_id"),
    expr("concat('customer_', cast(id % 100 as string))").alias("customer_id"),
    (expr("rand() * 500") + 50).cast("decimal(10,2)").alias("amount"),
    lit("pending").alias("status"),
    current_timestamp().alias("created_at")
)

# Create table with CDF enabled
df_initial.write.format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("overwrite") \
    .saveAsTable(cdf_table_name)

print(f"✅ Created table with CDF enabled: {cdf_table_name}")
print(f"📊 Initial record count: {df_initial.count()}")

# Verify CDF is enabled
cdf_enabled = spark.sql(f"SHOW TBLPROPERTIES {cdf_table_name}").filter("key = 'delta.enableChangeDataFeed'").collect()
if cdf_enabled:
    print(f"\n🔄 CDF Status: {cdf_enabled[0]['value']}")

display(spark.table(cdf_table_name).limit(10))

# COMMAND ----------

# DBTITLE 1,Demo: Perform INSERT, UPDATE, DELETE Operations
# Perform various operations to generate change data

print("🔄 Performing data modifications...\n")

# 1. INSERT: Add new transactions
print("📥 INSERT: Adding 100 new transactions...")
df_new = spark.range(1000, 1100).select(
    col("id").alias("transaction_id"),
    expr("concat('customer_', cast(id % 100 as string))").alias("customer_id"),
    (expr("rand() * 300") + 100).cast("decimal(10,2)").alias("amount"),
    lit("pending").alias("status"),
    current_timestamp().alias("created_at")
)
df_new.write.format("delta").mode("append").saveAsTable(cdf_table_name)
print("  ✓ Inserted 100 records\n")

# 2. UPDATE: Change status of some transactions
print("✏️  UPDATE: Approving pending transactions...")
spark.sql(f"""
    UPDATE {cdf_table_name}
    SET status = 'completed'
    WHERE transaction_id % 3 = 0 AND status = 'pending'
""")
print("  ✓ Updated transactions\n")

# 3. DELETE: Remove some transactions
print("🗑️  DELETE: Removing cancelled transactions...")
spark.sql(f"""
    DELETE FROM {cdf_table_name}
    WHERE transaction_id BETWEEN 50 AND 60
""")
print("  ✓ Deleted records\n")

print("✅ All modifications completed!")
print("\n📝 Changes captured in Change Data Feed")

# Show current state
print("\n📊 Current table sample:")
display(spark.table(cdf_table_name).limit(10))

# COMMAND ----------

# DBTITLE 1,Section 6: Reading Change Data Feed
# MAGIC %md
# MAGIC # 📚 Section 6: Reading Change Data Feed
# MAGIC
# MAGIC ## 🛠️ Methods to Read CDF:
# MAGIC
# MAGIC ### 1. Version-Based Reading:
# MAGIC ```sql
# MAGIC -- Read changes between versions
# MAGIC SELECT * FROM table_changes('catalog.schema.table', 2, 5)
# MAGIC ```
# MAGIC
# MAGIC ### 2. Timestamp-Based Reading:
# MAGIC ```sql
# MAGIC -- Read changes between timestamps
# MAGIC SELECT * FROM table_changes('catalog.schema.table',
# MAGIC   '2026-04-01 00:00:00',
# MAGIC   '2026-04-21 23:59:59'
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### 3. Starting From Version:
# MAGIC ```python
# MAGIC # PySpark: Read changes from version onwards
# MAGIC df_changes = spark.read \
# MAGIC     .format("delta") \
# MAGIC     .option("readChangeFeed", "true") \
# MAGIC     .option("startingVersion", 2) \
# MAGIC     .table("catalog.schema.table")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Understanding Change Types:
# MAGIC
# MAGIC ### Insert Operation:
# MAGIC ```
# MAGIC _change_type: insert
# MAGIC _commit_version: 3
# MAGIC Row data: [new values]
# MAGIC ```
# MAGIC
# MAGIC ### Update Operation (produces 2 rows):
# MAGIC ```
# MAGIC _change_type: update_preimage  
# MAGIC Row data: [old values]
# MAGIC
# MAGIC _change_type: update_postimage  
# MAGIC Row data: [new values]
# MAGIC ```
# MAGIC
# MAGIC ### Delete Operation:
# MAGIC ```
# MAGIC _change_type: delete
# MAGIC Row data: [deleted values]
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔍 Filtering CDF Data:
# MAGIC
# MAGIC ### Get Only Inserts:
# MAGIC ```sql
# MAGIC SELECT * FROM table_changes('table', 0)
# MAGIC WHERE _change_type = 'insert'
# MAGIC ```
# MAGIC
# MAGIC ### Get Latest State (Updates):
# MAGIC ```sql
# MAGIC SELECT * FROM table_changes('table', 0)
# MAGIC WHERE _change_type = 'update_postimage'
# MAGIC ```
# MAGIC
# MAGIC ### Get Deleted Rows:
# MAGIC ```sql
# MAGIC SELECT * FROM table_changes('table', 0)
# MAGIC WHERE _change_type = 'delete'
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Demo: Read All Changes Using table_changes
# MAGIC %sql
# MAGIC -- Read all changes from the beginning (version 0)
# MAGIC SELECT 
# MAGIC     _change_type,
# MAGIC     _commit_version,
# MAGIC     _commit_timestamp,
# MAGIC     transaction_id,
# MAGIC     customer_id,
# MAGIC     amount,
# MAGIC     status
# MAGIC FROM table_changes('main.delta_maintenance_demo.customer_transactions', 0)
# MAGIC ORDER BY _commit_version, transaction_id
# MAGIC LIMIT 100

# COMMAND ----------

# DBTITLE 1,Demo: Analyze Changes by Type
# MAGIC %sql
# MAGIC -- Analyze change distribution
# MAGIC SELECT 
# MAGIC     _change_type,
# MAGIC     COUNT(*) as change_count,
# MAGIC     MIN(_commit_version) as first_version,
# MAGIC     MAX(_commit_version) as last_version
# MAGIC FROM table_changes('main.delta_maintenance_demo.customer_transactions', 0)
# MAGIC GROUP BY _change_type
# MAGIC ORDER BY _change_type

# COMMAND ----------

# DBTITLE 1,Demo: Incremental Processing with CDF
# Demonstrate incremental processing using CDF

from pyspark.sql.functions import max as spark_max

print("📊 Incremental Processing Pattern using CDF")
print("="*60)

# Scenario: Process only changes since last checkpoint
last_processed_version = 1  # Simulate checkpoint

print(f"\n📍 Last processed version: {last_processed_version}")
print(f"🔄 Reading changes since version {last_processed_version}...\n")

# Read changes using PySpark
df_changes = spark.read \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", last_processed_version) \
    .table(cdf_table_name)

print(f"📈 Total changes detected: {df_changes.count()}")

# Analyze changes
change_summary = df_changes.groupBy("_change_type").count().orderBy("_change_type")
print("\n📋 Change Summary:")
display(change_summary)

# Get latest version
latest_version = df_changes.agg(spark_max("_commit_version")).collect()[0][0]
print(f"\n✅ Latest version: {latest_version}")
print(f"💾 Update checkpoint to: {latest_version}")

# Show sample of changes
print("\n📝 Sample of changes:")
display(df_changes.select("_change_type", "_commit_version", "transaction_id", "customer_id", "amount", "status").limit(20))

# COMMAND ----------

# DBTITLE 1,Section 7: Hands-on Maintenance Pipeline
# MAGIC %md
# MAGIC # 🔧 Section 7: Complete Maintenance Pipeline
# MAGIC
# MAGIC ## 🎯 End-to-End Maintenance Workflow:
# MAGIC
# MAGIC ```
# MAGIC Step 1: Create Delta Table
# MAGIC    ↓
# MAGIC Step 2: Ingest Data (creates files)
# MAGIC    ↓
# MAGIC Step 3: Monitor File Count
# MAGIC    ↓
# MAGIC Step 4: Run OPTIMIZE (when files > threshold)
# MAGIC    ↓
# MAGIC Step 5: Verify Optimization
# MAGIC    ↓
# MAGIC Step 6: Run VACUUM (scheduled)
# MAGIC    ↓
# MAGIC Step 7: Monitor Storage Savings
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📅 Recommended Maintenance Schedule:
# MAGIC
# MAGIC | Operation | Frequency | Trigger |
# MAGIC | --- | --- | --- |
# MAGIC | OPTIMIZE | Daily | File count > 1000 |
# MAGIC | VACUUM | Weekly | After OPTIMIZE |
# MAGIC | DESCRIBE DETAIL | Hourly | Monitoring |
# MAGIC | Z-ORDER | Weekly | Query patterns |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 Maintenance Decision Tree:
# MAGIC
# MAGIC ```
# MAGIC File Count Check
# MAGIC    ↓
# MAGIC > 1000 files?
# MAGIC    ├─ Yes → Run OPTIMIZE
# MAGIC    └─ No → Skip
# MAGIC        ↓
# MAGIC Optimization Done?
# MAGIC    ├─ Yes → Check storage
# MAGIC    └─ No → Exit
# MAGIC        ↓
# MAGIC Storage > threshold?
# MAGIC    ├─ Yes → Run VACUUM
# MAGIC    └─ No → Exit
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Demo: Automated Maintenance Pipeline
# Complete automated maintenance pipeline

def delta_maintenance_pipeline(table_name, optimize_threshold=100, vacuum_retention_hours=168):
    """
    Automated Delta table maintenance pipeline
    
    Args:
        table_name: Fully qualified table name
        optimize_threshold: Run OPTIMIZE if file count exceeds this
        vacuum_retention_hours: VACUUM retention period
    """
    
    print(f"🔧 Delta Maintenance Pipeline for: {table_name}")
    print("="*70)
    
    # Step 1: Get current table stats
    print("\n📊 Step 1: Analyzing table statistics...")
    stats = spark.sql(f"DESCRIBE DETAIL {table_name}").select(
        "numFiles", "sizeInBytes", "properties"
    ).collect()[0]
    
    num_files = stats["numFiles"]
    size_mb = stats["sizeInBytes"] / (1024 * 1024)
    
    print(f"   📁 Current files: {num_files}")
    print(f"   💾 Total size: {size_mb:.2f} MB")
    
    # Step 2: Decide if OPTIMIZE is needed
    print(f"\n⚡ Step 2: Checking OPTIMIZE threshold ({optimize_threshold} files)...")
    if num_files > optimize_threshold:
        print(f"   ⚠️  File count ({num_files}) exceeds threshold!")
        print("   🔄 Running OPTIMIZE...")
        
        spark.sql(f"OPTIMIZE {table_name}")
        
        # Check post-optimization stats
        stats_after = spark.sql(f"DESCRIBE DETAIL {table_name}").select("numFiles").collect()[0]
        files_after = stats_after["numFiles"]
        
        print(f"   ✅ OPTIMIZE complete!")
        print(f"   📉 Files reduced: {num_files} → {files_after} ({((num_files - files_after) / num_files * 100):.1f}% reduction)")
    else:
        print(f"   ✅ File count OK ({num_files} files)")
        print("   ⏭️  Skipping OPTIMIZE")
    
    # Step 3: VACUUM recommendation
    print(f"\n🧹 Step 3: VACUUM recommendation...")
    print(f"   📅 Recommended retention: {vacuum_retention_hours} hours")
    print(f"   ⚠️  VACUUM removes old files permanently!")
    print(f"   💡 Run manually: VACUUM {table_name} RETAIN {vacuum_retention_hours} HOURS")
    
    print("\n✅ Maintenance pipeline complete!")
    print("="*70)

# Run the maintenance pipeline
delta_maintenance_pipeline(
    table_name=table_name,
    optimize_threshold=5,  # Lower threshold for demo
    vacuum_retention_hours=168
)

# COMMAND ----------

# DBTITLE 1,Section 8: Hands-on CDC Pipeline
# MAGIC %md
# MAGIC # 🔄 Section 8: Complete CDC Pipeline
# MAGIC
# MAGIC ## 🎯 CDC Pipeline Architecture:
# MAGIC
# MAGIC ```
# MAGIC Source System (Operational DB)
# MAGIC    ↓
# MAGIC [Capture Changes]
# MAGIC    ↓
# MAGIC Delta Table (CDF Enabled)
# MAGIC    ↓
# MAGIC [Read Change Feed]
# MAGIC    ↓
# MAGIC Incremental Processing
# MAGIC    ├─ Inserts → Append to target
# MAGIC    ├─ Updates → Merge to target
# MAGIC    └─ Deletes → Remove from target
# MAGIC    ↓
# MAGIC Target Tables/Systems
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 CDC Processing Patterns:
# MAGIC
# MAGIC ### Pattern 1: Append-Only (Inserts)
# MAGIC ```python
# MAGIC # Process only new records
# MAGIC df_new = df_changes.filter("_change_type = 'insert'")
# MAGIC df_new.write.mode("append").saveAsTable("target_table")
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 2: Upsert (Inserts + Updates)
# MAGIC ```python
# MAGIC # Get latest state
# MAGIC df_latest = df_changes.filter(
# MAGIC     "_change_type IN ('insert', 'update_postimage')"
# MAGIC )
# MAGIC # MERGE into target
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 3: Full Sync (Inserts + Updates + Deletes)
# MAGIC ```python
# MAGIC # Handle all change types
# MAGIC df_all_changes = df_changes.select("*")
# MAGIC # Apply to target with delete handling
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Demo: CDC Processing Pipeline
# Complete CDC pipeline demonstration

from pyspark.sql.functions import col, lit, current_timestamp

def process_cdc_incremental(source_table, target_table, checkpoint_version=0):
    """
    Process changes incrementally using Change Data Feed
    
    Args:
        source_table: Source table with CDF enabled
        target_table: Target table to apply changes
        checkpoint_version: Last processed version
    """
    
    print(f"🔄 CDC Incremental Processing Pipeline")
    print("="*70)
    print(f"📥 Source: {source_table}")
    print(f"📤 Target: {target_table}")
    print(f"📍 Checkpoint: version {checkpoint_version}")
    print()
    
    # Step 1: Read changes since checkpoint
    print("📖 Step 1: Reading change feed...")
    df_changes = spark.read \
        .format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", checkpoint_version) \
        .table(source_table)
    
    total_changes = df_changes.count()
    print(f"   📊 Total changes detected: {total_changes}")
    
    if total_changes == 0:
        print("   ✅ No new changes to process")
        return checkpoint_version
    
    # Step 2: Analyze change types
    print("\n🔍 Step 2: Analyzing change types...")
    change_counts = df_changes.groupBy("_change_type").count().collect()
    for row in change_counts:
        print(f"   • {row['_change_type']}: {row['count']} records")
    
    # Step 3: Process inserts
    print("\n📥 Step 3: Processing inserts...")
    df_inserts = df_changes.filter(col("_change_type") == "insert") \
        .drop("_change_type", "_commit_version", "_commit_timestamp")
    
    insert_count = df_inserts.count()
    if insert_count > 0:
        # Create target if not exists, append inserts
        df_inserts.write.format("delta").mode("append").saveAsTable(target_table)
        print(f"   ✅ Inserted {insert_count} new records")
    else:
        print("   ⏭️  No inserts to process")
    
    # Step 4: Process updates (use postimage)
    print("\n✏️  Step 4: Processing updates...")
    df_updates = df_changes.filter(col("_change_type") == "update_postimage") \
        .drop("_change_type", "_commit_version", "_commit_timestamp")
    
    update_count = df_updates.count()
    if update_count > 0:
        # In production, use MERGE for updates
        print(f"   ✅ Detected {update_count} updates (MERGE required in production)")
    else:
        print("   ⏭️  No updates to process")
    
    # Step 5: Process deletes
    print("\n🗑️  Step 5: Processing deletes...")
    df_deletes = df_changes.filter(col("_change_type") == "delete")
    
    delete_count = df_deletes.count()
    if delete_count > 0:
        print(f"   ✅ Detected {delete_count} deletes (DELETE required in production)")
    else:
        print("   ⏭️  No deletes to process")
    
    # Step 6: Update checkpoint
    print("\n💾 Step 6: Updating checkpoint...")
    new_version = df_changes.agg(spark_max("_commit_version")).collect()[0][0]
    print(f"   📌 New checkpoint: version {new_version}")
    
    print("\n✅ CDC processing complete!")
    print("="*70)
    
    return new_version

# Create target table
target_table = f"{catalog_name}.{schema_name}.customer_transactions_target"
spark.sql(f"DROP TABLE IF EXISTS {target_table}")

# Run CDC pipeline
new_checkpoint = process_cdc_incremental(
    source_table=cdf_table_name,
    target_table=target_table,
    checkpoint_version=0
)

print(f"\n📊 Target table created: {target_table}")
display(spark.table(target_table).limit(10))

# COMMAND ----------

# DBTITLE 1,Section 9: End-to-End Architecture
# MAGIC %md
# MAGIC # 🏛️ Section 9: End-to-End Incremental Architecture
# MAGIC
# MAGIC ## 🎯 Complete Data Platform Architecture:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                     SOURCE SYSTEMS                          │
# MAGIC │  Operational DBs, APIs, Streaming, Files, SaaS Apps        │
# MAGIC └────────────────────┬────────────────────────────────────────┘
# MAGIC                      ↓
# MAGIC          ┌───────────────────────┐
# MAGIC          │   BRONZE (Raw Layer)   │
# MAGIC          │  • CDF Enabled         │
# MAGIC          │  • Auto Optimize       │
# MAGIC          │  • Raw data ingestion  │
# MAGIC          └──────────┬─────────────┘
# MAGIC                     ↓ (Read CDF)
# MAGIC          ┌───────────────────────┐
# MAGIC          │  SILVER (Cleaned)      │
# MAGIC          │  • CDF Enabled         │
# MAGIC          │  • Incremental MERGE   │
# MAGIC          │  • Data quality rules  │
# MAGIC          └──────────┬─────────────┘
# MAGIC                     ↓ (Read CDF)
# MAGIC          ┌───────────────────────┐
# MAGIC          │  GOLD (Aggregated)     │
# MAGIC          │  • Business metrics    │
# MAGIC          │  • Optimized queries   │
# MAGIC          │  • Regular VACUUM      │
# MAGIC          └──────────┬─────────────┘
# MAGIC                     ↓
# MAGIC          ┌───────────────────────┐
# MAGIC          │   CONSUMPTION LAYER    │
# MAGIC          │  Dashboards, ML, APIs  │
# MAGIC          └───────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Implementation Strategy:
# MAGIC
# MAGIC ### Layer 1: Bronze (Raw Ingestion)
# MAGIC ```python
# MAGIC # Enable CDF + Auto Optimize
# MAGIC spark.sql(f"""
# MAGIC     ALTER TABLE bronze.raw_events
# MAGIC     SET TBLPROPERTIES (
# MAGIC         'delta.enableChangeDataFeed' = 'true',
# MAGIC         'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC         'delta.autoOptimize.autoCompact' = 'true'
# MAGIC     )
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC ### Layer 2: Silver (Incremental Processing)
# MAGIC ```python
# MAGIC # Read only changes from bronze
# MAGIC df_changes = spark.read \
# MAGIC     .format("delta") \
# MAGIC     .option("readChangeFeed", "true") \
# MAGIC     .option("startingVersion", last_checkpoint) \
# MAGIC     .table("bronze.raw_events")
# MAGIC
# MAGIC # Apply transformations
# MAGIC df_cleaned = df_changes.transform(clean_data) \
# MAGIC                        .transform(validate_schema)
# MAGIC
# MAGIC # MERGE into silver
# MAGIC ```
# MAGIC
# MAGIC ### Layer 3: Gold (Aggregations)
# MAGIC ```python
# MAGIC # Process silver changes incrementally
# MAGIC df_metrics = df_silver_changes \
# MAGIC     .groupBy("date", "category") \
# MAGIC     .agg(sum("amount").alias("total_amount"))
# MAGIC
# MAGIC # Update gold tables
# MAGIC ```
# MAGIC
# MAGIC ### Maintenance Schedule:
# MAGIC ```python
# MAGIC # Daily maintenance job
# MAGIC OPTIMIZE bronze.raw_events ZORDER BY (event_date, event_type);
# MAGIC OPTIMIZE silver.cleaned_events ZORDER BY (date, customer_id);
# MAGIC OPTIMIZE gold.daily_metrics;
# MAGIC
# MAGIC # Weekly VACUUM
# MAGIC VACUUM bronze.raw_events RETAIN 168 HOURS;
# MAGIC VACUUM silver.cleaned_events RETAIN 168 HOURS;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Key Benefits:
# MAGIC
# MAGIC | Benefit | Impact |
# MAGIC | --- | --- |
# MAGIC | Incremental processing | 90% reduction in compute costs |
# MAGIC | CDF-based pipelines | 10x faster pipeline execution |
# MAGIC | Auto Optimize | Zero-touch maintenance |
# MAGIC | Regular VACUUM | 50% storage cost reduction |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Best Practices:
# MAGIC
# MAGIC 1. **Enable CDF on bronze/silver tables**  
# MAGIC 2. **Use Auto Optimize for streaming tables**  
# MAGIC 3. **OPTIMIZE daily with Z-ORDER**  
# MAGIC 4. **VACUUM weekly with 7-day retention**  
# MAGIC 5. **Monitor file counts and sizes**  
# MAGIC 6. **Use checkpoint tables for CDF**  
# MAGIC 7. **Test VACUUM with DRY RUN first**

# COMMAND ----------

# DBTITLE 1,Section 10: Genie Code Agent Prompts
# MAGIC %md
# MAGIC # 🤖 Section 10: Databricks Genie Code Agent — Example Prompts
# MAGIC
# MAGIC ## 💬 Ready-to-Use Prompts for Delta Maintenance:
# MAGIC
# MAGIC ### OPTIMIZE Operations:
# MAGIC ```
# MAGIC Prompt: "Optimize the sales_data table to improve query performance"
# MAGIC
# MAGIC Prompt: "Run OPTIMIZE with Z-ORDER on customer_id and order_date columns"
# MAGIC
# MAGIC Prompt: "Check file count and optimize if greater than 1000 files"
# MAGIC
# MAGIC Prompt: "Optimize all tables in the analytics schema"
# MAGIC ```
# MAGIC
# MAGIC ### VACUUM Operations:
# MAGIC ```
# MAGIC Prompt: "Vacuum the transactions table with 7 day retention"
# MAGIC
# MAGIC Prompt: "Show me what files would be deleted with VACUUM dry run"
# MAGIC
# MAGIC Prompt: "Clean up old files from all tables in production schema"
# MAGIC
# MAGIC Prompt: "Vacuum table but keep 30 days of history for compliance"
# MAGIC ```
# MAGIC
# MAGIC ### Change Data Feed:
# MAGIC ```
# MAGIC Prompt: "Enable Change Data Feed on the customer_orders table"
# MAGIC
# MAGIC Prompt: "Show me all changes to the inventory table since yesterday"
# MAGIC
# MAGIC Prompt: "Read change feed and process only inserted records"
# MAGIC
# MAGIC Prompt: "Build an incremental pipeline using CDF from source to target"
# MAGIC
# MAGIC Prompt: "Create audit log table from CDF tracking all modifications"
# MAGIC ```
# MAGIC
# MAGIC ### Maintenance Automation:
# MAGIC ```
# MAGIC Prompt: "Create maintenance pipeline with OPTIMIZE and VACUUM"
# MAGIC
# MAGIC Prompt: "Build automated script to optimize tables when file count is high"
# MAGIC
# MAGIC Prompt: "Set up daily maintenance job for all delta tables"
# MAGIC
# MAGIC Prompt: "Monitor table health and alert if optimization needed"
# MAGIC ```
# MAGIC
# MAGIC ### CDC Pipeline:
# MAGIC ```
# MAGIC Prompt: "Build complete CDC pipeline from source to silver layer"
# MAGIC
# MAGIC Prompt: "Process incremental changes using Change Data Feed"
# MAGIC
# MAGIC Prompt: "Sync source table changes to target table incrementally"
# MAGIC
# MAGIC Prompt: "Create SCD Type 2 dimension using CDF"
# MAGIC ```
# MAGIC
# MAGIC ### Troubleshooting:
# MAGIC ```
# MAGIC Prompt: "Fix slow query caused by small file problem"
# MAGIC
# MAGIC Prompt: "Why is my table query performance degrading?"
# MAGIC
# MAGIC Prompt: "Diagnose and resolve Delta table maintenance issues"
# MAGIC
# MAGIC Prompt: "Compare table statistics before and after optimization"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Advanced Prompts:
# MAGIC
# MAGIC ```
# MAGIC Prompt: "Design medallion architecture with CDF-enabled tables"
# MAGIC
# MAGIC Prompt: "Implement auto-optimization strategy for streaming workloads"
# MAGIC
# MAGIC Prompt: "Build data versioning system using Delta time travel and CDF"
# MAGIC
# MAGIC Prompt: "Create monitoring dashboard for Delta table health metrics"
# MAGIC
# MAGIC Prompt: "Optimize partition strategy and run maintenance for large fact table"
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 11: Summary and Interview Questions
# MAGIC %md
# MAGIC # 🎓 Section 11: Summary & Interview Questions
# MAGIC
# MAGIC ## 📚 Key Learnings:
# MAGIC
# MAGIC ### 1. Small File Problem:
# MAGIC - Caused by streaming writes and frequent small batches  
# MAGIC - Degrades query performance significantly  
# MAGIC - Increases metadata overhead  
# MAGIC - **Solution**: OPTIMIZE command  
# MAGIC
# MAGIC ### 2. OPTIMIZE:
# MAGIC - Compacts small files into larger ones  
# MAGIC - Improves read performance  
# MAGIC - Supports Z-ORDER for data skipping  
# MAGIC - Idempotent and safe to run multiple times  
# MAGIC
# MAGIC ### 3. VACUUM:
# MAGIC - Removes old unused files  
# MAGIC - Frees up storage space  
# MAGIC - Affects time travel capabilities  
# MAGIC - Requires careful retention period selection  
# MAGIC
# MAGIC ### 4. Change Data Feed (CDF):
# MAGIC - Captures row-level changes  
# MAGIC - Enables incremental processing  
# MAGIC - Reduces compute costs dramatically  
# MAGIC - Essential for CDC pipelines  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Top 10 Interview Questions:
# MAGIC
# MAGIC ### Q1: What is the small file problem and how do you solve it?
# MAGIC **Answer**: Small file problem occurs when Delta tables accumulate many small files, causing performance degradation. Solved using OPTIMIZE command which compacts small files into larger ones. Best practices include enabling Auto Optimize for streaming tables and running scheduled OPTIMIZE jobs.
# MAGIC
# MAGIC ### Q2: Explain the difference between OPTIMIZE and VACUUM.
# MAGIC **Answer**: 
# MAGIC - **OPTIMIZE**: Compacts small files into larger ones, improves read performance, doesn't delete data  
# MAGIC - **VACUUM**: Removes old/unused files, reduces storage costs, affects time travel  
# MAGIC - Both are complementary maintenance operations
# MAGIC
# MAGIC ### Q3: What happens to time travel after running VACUUM?
# MAGIC **Answer**: VACUUM permanently deletes files older than the retention period, making those versions inaccessible for time travel. Default retention is 7 days (168 hours). Once vacuumed, you cannot query historical versions.
# MAGIC
# MAGIC ### Q4: How does Change Data Feed (CDF) work?
# MAGIC **Answer**: CDF captures row-level changes (inserts, updates, deletes) with metadata columns:
# MAGIC - `_change_type`: insert, update_preimage, update_postimage, delete  
# MAGIC - `_commit_version`: Delta version  
# MAGIC - `_commit_timestamp`: Change timestamp  
# MAGIC Enables incremental processing and audit trails.
# MAGIC
# MAGIC ### Q5: What is Z-ORDER and when should you use it?
# MAGIC **Answer**: Z-ORDER is a clustering technique that co-locates related data based on specified columns. Use it during OPTIMIZE for columns frequently used in WHERE clauses. Example: `OPTIMIZE table ZORDER BY (date, customer_id)`.
# MAGIC
# MAGIC ### Q6: How do you enable Auto Optimize?
# MAGIC **Answer**:
# MAGIC ```sql
# MAGIC ALTER TABLE table_name SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC ```
# MAGIC Best for streaming tables to prevent small file accumulation.
# MAGIC
# MAGIC ### Q7: What are the CDF change types and what do they represent?
# MAGIC **Answer**:
# MAGIC - **insert**: New row added  
# MAGIC - **update_preimage**: Row state before update  
# MAGIC - **update_postimage**: Row state after update  
# MAGIC - **delete**: Row removed  
# MAGIC Updates produce two rows (pre and post) for complete change tracking.
# MAGIC
# MAGIC ### Q8: What's the recommended VACUUM retention period?
# MAGIC **Answer**: 
# MAGIC - **Production**: 168 hours (7 days) - default and safe  
# MAGIC - **Development**: 24 hours - faster cleanup  
# MAGIC - **Compliance**: 720+ hours (30+ days) - regulatory requirements  
# MAGIC NEVER use 0 hours in production - risks breaking concurrent queries.
# MAGIC
# MAGIC ### Q9: How do you implement incremental processing using CDF?
# MAGIC **Answer**:
# MAGIC ```python
# MAGIC df_changes = spark.read \
# MAGIC     .format("delta") \
# MAGIC     .option("readChangeFeed", "true") \
# MAGIC     .option("startingVersion", checkpoint_version) \
# MAGIC     .table("source_table")
# MAGIC
# MAGIC # Process only changes
# MAGIC df_changes.filter("_change_type = 'insert'") \
# MAGIC     .write.mode("append").saveAsTable("target")
# MAGIC ```
# MAGIC Store checkpoint version for next run.
# MAGIC
# MAGIC ### Q10: What causes poor Delta table performance?
# MAGIC **Answer**:
# MAGIC 1. **Too many small files** → Run OPTIMIZE  
# MAGIC 2. **Unpartitioned large tables** → Add partitioning  
# MAGIC 3. **No Z-ORDER** → Use Z-ORDER on filter columns  
# MAGIC 4. **Stale statistics** → Run ANALYZE TABLE  
# MAGIC 5. **No data skipping** → Review partition/Z-ORDER strategy  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚠️ Common Mistakes to Avoid:
# MAGIC
# MAGIC ### 1. Ignoring Small File Problem:
# MAGIC ❌ **Mistake**: Never running OPTIMIZE  
# MAGIC ✅ **Solution**: Schedule regular OPTIMIZE jobs  
# MAGIC
# MAGIC ### 2. Over-Aggressive VACUUM:
# MAGIC ❌ **Mistake**: Using RETAIN 0 HOURS  
# MAGIC ✅ **Solution**: Use appropriate retention (7+ days)  
# MAGIC
# MAGIC ### 3. Not Using CDF:
# MAGIC ❌ **Mistake**: Full table scans for changes  
# MAGIC ✅ **Solution**: Enable CDF for incremental processing  
# MAGIC
# MAGIC ### 4. No Maintenance Strategy:
# MAGIC ❌ **Mistake**: Ad-hoc manual maintenance  
# MAGIC ✅ **Solution**: Automated scheduled maintenance pipeline  
# MAGIC
# MAGIC ### 5. Forgetting Auto Optimize:
# MAGIC ❌ **Mistake**: Manual OPTIMIZE for streaming tables  
# MAGIC ✅ **Solution**: Enable Auto Optimize properties  
# MAGIC
# MAGIC ### 6. Breaking Time Travel:
# MAGIC ❌ **Mistake**: VACUUM without understanding impact  
# MAGIC ✅ **Solution**: Document retention policies, test with DRY RUN  
# MAGIC
# MAGIC ### 7. Inefficient CDC Processing:
# MAGIC ❌ **Mistake**: Re-processing entire table  
# MAGIC ✅ **Solution**: Use CDF with checkpoint versioning  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Production Checklist:
# MAGIC
# MAGIC - ✅ Enable CDF on bronze/silver tables  
# MAGIC - ✅ Configure Auto Optimize for streaming tables  
# MAGIC - ✅ Schedule daily OPTIMIZE jobs  
# MAGIC - ✅ Schedule weekly VACUUM jobs  
# MAGIC - ✅ Monitor file counts and sizes  
# MAGIC - ✅ Document retention policies  
# MAGIC - ✅ Test VACUUM with DRY RUN  
# MAGIC - ✅ Implement checkpoint management for CDF  
# MAGIC - ✅ Use Z-ORDER on query-critical columns  
# MAGIC - ✅ Monitor storage costs and query performance  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌟 Final Thoughts:
# MAGIC
# MAGIC Delta Lake maintenance is **critical** for:
# MAGIC - ⚡ Query performance  
# MAGIC - 💰 Cost optimization  
# MAGIC - 🔄 Efficient incremental processing  
# MAGIC - 📊 Data platform scalability  
# MAGIC
# MAGIC **Remember**: Proactive maintenance prevents production issues!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏷️ **@TRRaveendra** | Phase 4 Day 24 Complete! 🎉