# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # 🧊 Data Engineering Training — Phase 4 Day 23  
# MAGIC ## 🔄 Delta MERGE: SCD Type 1, Type 2 & Upsert Patterns  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC * Delta MERGE Operation  
# MAGIC * Slowly Changing Dimensions (SCD Type 1 & Type 2)  
# MAGIC * Upsert Patterns  
# MAGIC * Incremental Data Processing  
# MAGIC * Performance Optimization for MERGE  
# MAGIC * Change Data Feed (Preview)  
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
# MAGIC Understand how to implement SCD Type 1 and Type 2 using Delta MERGE and design efficient upsert patterns for incremental data pipelines.
# MAGIC
# MAGIC ### 🔧 Engineering Constraints:
# MAGIC * ✅ Databricks Serverless Compute  
# MAGIC * ✅ DataFrame API only (no RDDs)  
# MAGIC * ✅ No cache() / persist()  
# MAGIC * ✅ No /tmp or local storage  
# MAGIC * ✅ Unity Catalog managed tables  
# MAGIC * ✅ Delta-first design  
# MAGIC * ✅ Incremental processing patterns

# COMMAND ----------

# DBTITLE 1,Final Summary & Interview Questions
# MAGIC %md
# MAGIC ---
# MAGIC # 🎯 FINAL SUMMARY: Delta MERGE Mastery
# MAGIC
# MAGIC ## 📚 Key Learnings:
# MAGIC
# MAGIC ### 1. Delta MERGE Fundamentals
# MAGIC * **ACID-compliant** upsert operation (UPDATE + INSERT + DELETE)
# MAGIC * Single atomic transaction for all modifications
# MAGIC * Cornerstone of incremental data processing
# MAGIC * Replaces complex multi-step update patterns
# MAGIC
# MAGIC ### 2. SCD Type 1 vs Type 2
# MAGIC
# MAGIC | Aspect | SCD Type 1 | SCD Type 2 |
# MAGIC |--------|-----------|------------|
# MAGIC | **History** | Not maintained | Fully maintained |
# MAGIC | **Storage** | Minimal | Higher (multiple rows) |
# MAGIC | **Complexity** | Simple | Moderate-High |
# MAGIC | **Columns** | Business columns only | + effective_date, end_date, is_current |
# MAGIC | **Use Case** | Non-critical updates | Audit/Compliance |
# MAGIC
# MAGIC ### 3. Core Patterns Mastered
# MAGIC ✅ Simple Upsert (match → update, no match → insert)  
# MAGIC ✅ Conditional Upsert (update only if changed)  
# MAGIC ✅ Timestamp Tracking (created_at, updated_at)  
# MAGIC ✅ 3-Way MERGE (INSERT + UPDATE + DELETE)  
# MAGIC ✅ Deduplication (ROW_NUMBER + MERGE)  
# MAGIC ✅ SCD Type 1 (overwrite)  
# MAGIC ✅ SCD Type 2 (history tracking)  
# MAGIC
# MAGIC ### 4. Performance Optimization
# MAGIC * Enable auto-compaction and optimize write
# MAGIC * Z-order on join keys
# MAGIC * Partition pruning in merge conditions
# MAGIC * Conditional updates to avoid unnecessary writes
# MAGIC * Regular OPTIMIZE and VACUUM operations
# MAGIC * Use broadcast for small source tables
# MAGIC
# MAGIC ### 5. Production Best Practices
# MAGIC * Always use fully qualified table names
# MAGIC * Add audit columns (timestamps, run_id)
# MAGIC * Log MERGE operations for monitoring
# MAGIC * Handle NULL comparisons carefully
# MAGIC * Use surrogate keys for SCD Type 2
# MAGIC * Partition large tables appropriately
# MAGIC * Enable Change Data Feed for CDC use cases
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Interview Questions (10 Questions)
# MAGIC
# MAGIC ### Question 1: Explain Delta MERGE
# MAGIC **Q:** What is Delta MERGE and why is it important?  
# MAGIC **A:** Delta MERGE is an ACID-compliant operation that combines INSERT, UPDATE, and DELETE into a single atomic transaction. It's important because it:
# MAGIC * Simplifies incremental data processing
# MAGIC * Ensures data consistency (ACID)
# MAGIC * Improves performance vs. full table overwrites
# MAGIC * Supports complex upsert patterns
# MAGIC * Enables CDC and SCD implementations
# MAGIC
# MAGIC ### Question 2: SCD Type 1 vs Type 2
# MAGIC **Q:** When would you use SCD Type 1 vs Type 2?  
# MAGIC **A:** 
# MAGIC * **SCD Type 1**: Use when history is NOT needed (email updates, corrections, reference data)
# MAGIC * **SCD Type 2**: Use when history IS needed (pricing history, audit trails, compliance, trend analysis)
# MAGIC
# MAGIC ### Question 3: SCD Type 2 Implementation
# MAGIC **Q:** What columns are required for SCD Type 2 and why?  
# MAGIC **A:** 
# MAGIC * `surrogate_key`: Unique row identifier (primary key)
# MAGIC * `business_key`: Natural key (e.g., customer_id)
# MAGIC * `effective_date`: When this version became active
# MAGIC * `end_date`: When this version expired (NULL = current)
# MAGIC * `is_current`: Boolean flag for current version
# MAGIC * Business columns: Actual data being tracked
# MAGIC
# MAGIC ### Question 4: MERGE Performance Issue
# MAGIC **Q:** Your MERGE operation is slow. What are potential causes and solutions?  
# MAGIC **A:** 
# MAGIC **Causes:**
# MAGIC * No partition pruning
# MAGIC * Small file problem
# MAGIC * Inefficient join keys
# MAGIC * Updating all rows (even unchanged)
# MAGIC
# MAGIC **Solutions:**
# MAGIC * Add partition filters to ON clause
# MAGIC * Run OPTIMIZE with Z-ORDER on join keys
# MAGIC * Enable auto-compaction
# MAGIC * Add conditional updates (WHEN MATCHED AND columns_changed)
# MAGIC * Use broadcast for small sources
# MAGIC
# MAGIC ### Question 5: NULL Handling
# MAGIC **Q:** What's wrong with this MERGE condition?  
# MAGIC ```sql
# MAGIC WHEN MATCHED AND target.value <> source.value THEN UPDATE
# MAGIC ```
# MAGIC **A:** NULL comparisons return NULL (not TRUE). If either value is NULL, the condition fails. **Fix:**
# MAGIC ```sql
# MAGIC WHEN MATCHED AND (
# MAGIC   target.value <> source.value OR
# MAGIC   (target.value IS NULL AND source.value IS NOT NULL) OR
# MAGIC   (target.value IS NOT NULL AND source.value IS NULL)
# MAGIC ) THEN UPDATE
# MAGIC ```
# MAGIC
# MAGIC ### Question 6: MERGE vs INSERT OVERWRITE
# MAGIC **Q:** When would you use MERGE vs INSERT OVERWRITE?  
# MAGIC **A:** 
# MAGIC * **MERGE**: Use for incremental updates, row-level changes, upserts, SCD
# MAGIC * **INSERT OVERWRITE**: Use for full partition replacements, complete refreshes, when no row-level updates needed
# MAGIC
# MAGIC ### Question 7: Change Data Feed
# MAGIC **Q:** What is Change Data Feed and when should you use it?  
# MAGIC **A:** CDF tracks row-level changes (inserts, updates, deletes) in Delta tables. Use when:
# MAGIC * Building incremental ETL pipelines
# MAGIC * Need to process only changed records
# MAGIC * Implementing CDC from source systems
# MAGIC * Creating audit trails
# MAGIC * Propagating changes downstream
# MAGIC
# MAGIC **Caveat:** 20-30% storage overhead, must enable before changes occur
# MAGIC
# MAGIC ### Question 8: Deduplication in MERGE
# MAGIC **Q:** How do you deduplicate source data before MERGE?  
# MAGIC **A:** Use ROW_NUMBER() to take latest record:
# MAGIC ```sql
# MAGIC MERGE INTO target
# MAGIC USING (
# MAGIC   SELECT *, ROW_NUMBER() OVER (
# MAGIC     PARTITION BY id ORDER BY timestamp DESC
# MAGIC   ) as rn
# MAGIC   FROM source
# MAGIC   WHERE rn = 1
# MAGIC ) AS deduped_source
# MAGIC ON target.id = deduped_source.id
# MAGIC ...
# MAGIC ```
# MAGIC
# MAGIC ### Question 9: SCD Type 2 Query Pattern
# MAGIC **Q:** How do you query current records in SCD Type 2?  
# MAGIC **A:** 
# MAGIC ```sql
# MAGIC -- Current records
# MAGIC SELECT * FROM dim_table WHERE is_current = TRUE
# MAGIC
# MAGIC -- Point-in-time query
# MAGIC SELECT * FROM dim_table
# MAGIC WHERE effective_date <= '2026-01-15'
# MAGIC   AND (end_date IS NULL OR end_date >= '2026-01-15')
# MAGIC ```
# MAGIC
# MAGIC ### Question 10: MERGE Metrics
# MAGIC **Q:** How do you track MERGE operation metrics?  
# MAGIC **A:** 
# MAGIC 1. **Query Delta Table History:**
# MAGIC ```sql
# MAGIC DESCRIBE HISTORY my_table
# MAGIC ```
# MAGIC
# MAGIC 2. **Create Audit Log Table:**
# MAGIC ```sql
# MAGIC CREATE TABLE merge_log (
# MAGIC   run_id STRING,
# MAGIC   table_name STRING,
# MAGIC   records_inserted INT,
# MAGIC   records_updated INT,
# MAGIC   records_deleted INT,
# MAGIC   timestamp TIMESTAMP
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC 3. **Use MERGE OUTPUT clause (when available)**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚠️ Common Mistakes:
# MAGIC
# MAGIC ### 1. Using Full Table Overwrite Instead of MERGE
# MAGIC ```python
# MAGIC # ❌ BAD: Inefficient
# MAGIC df.write.mode("overwrite").saveAsTable("target")
# MAGIC
# MAGIC # ✅ GOOD: Incremental
# MAGIC MERGE INTO target USING source ON ...
# MAGIC ```
# MAGIC
# MAGIC ### 2. Forgetting to Set is_current = FALSE in SCD Type 2
# MAGIC ```sql
# MAGIC -- ❌ BAD: Multiple current records
# MAGIC INSERT INTO dim_table (id, value, is_current)
# MAGIC VALUES (1, 'new', TRUE)
# MAGIC
# MAGIC -- ✅ GOOD: Expire old first
# MAGIC UPDATE dim_table SET is_current = FALSE WHERE id = 1;
# MAGIC INSERT INTO dim_table (id, value, is_current)
# MAGIC VALUES (1, 'new', TRUE)
# MAGIC ```
# MAGIC
# MAGIC ### 3. Not Handling NULL Comparisons
# MAGIC ```sql
# MAGIC -- ❌ BAD: NULL <> NULL returns NULL
# MAGIC WHEN MATCHED AND t.value <> s.value
# MAGIC
# MAGIC -- ✅ GOOD: Explicit NULL handling
# MAGIC WHEN MATCHED AND (t.value <> s.value OR 
# MAGIC   (t.value IS NULL AND s.value IS NOT NULL))
# MAGIC ```
# MAGIC
# MAGIC ### 4. Missing Partition Filters
# MAGIC ```sql
# MAGIC -- ❌ BAD: Scans entire table
# MAGIC MERGE INTO large_table USING source
# MAGIC ON large_table.id = source.id
# MAGIC
# MAGIC -- ✅ GOOD: Prunes partitions
# MAGIC MERGE INTO large_table USING source
# MAGIC ON large_table.id = source.id 
# MAGIC    AND large_table.date = current_date()
# MAGIC ```
# MAGIC
# MAGIC ### 5. Not Using Surrogate Keys in SCD Type 2
# MAGIC ```sql
# MAGIC -- ❌ BAD: Business key as primary key (violates uniqueness)
# MAGIC PRIMARY KEY (customer_id)  -- Multiple rows per customer!
# MAGIC
# MAGIC -- ✅ GOOD: Surrogate key
# MAGIC PRIMARY KEY (surrogate_key)  -- Unique per row
# MAGIC ```
# MAGIC
# MAGIC ### 6. Inefficient Update Pattern
# MAGIC ```sql
# MAGIC -- ❌ BAD: Updates all matched rows
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC
# MAGIC -- ✅ GOOD: Conditional update
# MAGIC WHEN MATCHED AND (t.col1 <> s.col1 OR t.col2 <> s.col2) 
# MAGIC THEN UPDATE SET *
# MAGIC ```
# MAGIC
# MAGIC ### 7. Not Optimizing After MERGE
# MAGIC ```sql
# MAGIC -- ❌ BAD: Many small files accumulate
# MAGIC MERGE INTO target ...
# MAGIC -- (done)
# MAGIC
# MAGIC -- ✅ GOOD: Regular optimization
# MAGIC MERGE INTO target ...;
# MAGIC OPTIMIZE target ZORDER BY (id);
# MAGIC ```
# MAGIC
# MAGIC ### 8. Incorrect Date Range in SCD Type 2
# MAGIC ```sql
# MAGIC -- ❌ BAD: Gaps in date ranges
# MAGIC end_date = current_date()  -- Gap of 1 day!
# MAGIC effective_date = current_date()
# MAGIC
# MAGIC -- ✅ GOOD: Continuous ranges
# MAGIC end_date = current_date() - INTERVAL 1 DAY
# MAGIC effective_date = current_date()
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Next Steps:
# MAGIC
# MAGIC 1. **Practice**: Implement all patterns in this notebook
# MAGIC 2. **Experiment**: Try different scenarios and edge cases
# MAGIC 3. **Optimize**: Benchmark performance with large datasets
# MAGIC 4. **Integrate**: Build end-to-end pipelines with MERGE
# MAGIC 5. **Monitor**: Set up logging and alerting
# MAGIC 6. **Advanced Topics**: 
# MAGIC    * Streaming MERGE (foreachBatch)
# MAGIC    * SCD Type 3 (previous value column)
# MAGIC    * Hybrid SCD (Type 1 + Type 2)
# MAGIC    * Complex CDC scenarios
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏆 Congratulations!
# MAGIC
# MAGIC You've mastered Delta MERGE operations and SCD patterns!  
# MAGIC You're now ready to build production-grade incremental data pipelines.
# MAGIC
# MAGIC ### 📌 Checklist:
# MAGIC ✅ Understand Delta MERGE syntax and semantics  
# MAGIC ✅ Implement SCD Type 1 (overwrite)  
# MAGIC ✅ Implement SCD Type 2 (history tracking)  
# MAGIC ✅ Apply various upsert patterns  
# MAGIC ✅ Optimize MERGE performance  
# MAGIC ✅ Handle edge cases (NULLs, deduplication)  
# MAGIC ✅ Build end-to-end incremental pipelines  
# MAGIC ✅ Ready for production implementation  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author: **TRRaveendra**  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### 📍 Phase 4 - Day 23: **COMPLETE** ✅

# COMMAND ----------

# DBTITLE 1,Bonus: Quick Reference Card
# MAGIC %md
# MAGIC ---
# MAGIC # 📊 Quick Reference Card
# MAGIC
# MAGIC ## Basic MERGE Syntax
# MAGIC ```sql
# MAGIC MERGE INTO target_table AS t
# MAGIC USING source_table AS s
# MAGIC ON t.key = s.key
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ```
# MAGIC
# MAGIC ## SCD Type 1 (Overwrite)
# MAGIC ```sql
# MAGIC MERGE INTO dim_table AS t
# MAGIC USING source AS s
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ```
# MAGIC
# MAGIC ## SCD Type 2 (History) - Step 1: Expire
# MAGIC ```sql
# MAGIC MERGE INTO dim_table AS t
# MAGIC USING source AS s
# MAGIC ON t.id = s.id AND t.is_current = TRUE
# MAGIC WHEN MATCHED AND (t.col1 <> s.col1 OR t.col2 <> s.col2) THEN
# MAGIC   UPDATE SET
# MAGIC     t.end_date = current_date() - INTERVAL 1 DAY,
# MAGIC     t.is_current = FALSE
# MAGIC ```
# MAGIC
# MAGIC ## SCD Type 2 - Step 2: Insert New
# MAGIC ```sql
# MAGIC INSERT INTO dim_table
# MAGIC SELECT 
# MAGIC   new_surrogate_key,
# MAGIC   id,
# MAGIC   col1, col2,
# MAGIC   current_date() as effective_date,
# MAGIC   NULL as end_date,
# MAGIC   TRUE as is_current
# MAGIC FROM source
# MAGIC WHERE changed_records_only
# MAGIC ```
# MAGIC
# MAGIC ## Conditional Update (Performance)
# MAGIC ```sql
# MAGIC WHEN MATCHED AND (
# MAGIC   t.col1 <> s.col1 OR 
# MAGIC   t.col2 <> s.col2
# MAGIC ) THEN UPDATE SET *
# MAGIC ```
# MAGIC
# MAGIC ## 3-Way MERGE (INSERT/UPDATE/DELETE)
# MAGIC ```sql
# MAGIC MERGE INTO target AS t USING source AS s
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED AND s.status = 'deleted' THEN DELETE
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ```
# MAGIC
# MAGIC ## Deduplication Pattern
# MAGIC ```sql
# MAGIC USING (
# MAGIC   SELECT *, ROW_NUMBER() OVER (
# MAGIC     PARTITION BY id ORDER BY ts DESC
# MAGIC   ) as rn
# MAGIC   FROM source
# MAGIC   WHERE rn = 1
# MAGIC ) AS s
# MAGIC ```
# MAGIC
# MAGIC ## Optimization Commands
# MAGIC ```sql
# MAGIC -- Enable auto-optimize
# MAGIC ALTER TABLE my_table SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC
# MAGIC -- Z-Order
# MAGIC OPTIMIZE my_table ZORDER BY (key_col);
# MAGIC
# MAGIC -- Compact files
# MAGIC OPTIMIZE my_table;
# MAGIC
# MAGIC -- Clean old files
# MAGIC VACUUM my_table RETAIN 168 HOURS;
# MAGIC ```
# MAGIC
# MAGIC ## Change Data Feed
# MAGIC ```sql
# MAGIC -- Enable
# MAGIC ALTER TABLE my_table SET TBLPROPERTIES (
# MAGIC   delta.enableChangeDataFeed = true
# MAGIC );
# MAGIC
# MAGIC -- Query changes
# MAGIC SELECT * FROM table_changes('my_table', 0, 10);
# MAGIC ```
# MAGIC
# MAGIC ## PySpark DataFrame API
# MAGIC ```python
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC DeltaTable.forName(spark, "target") \
# MAGIC   .merge(source_df, "target.id = source.id") \
# MAGIC   .whenMatchedUpdateAll() \
# MAGIC   .whenNotMatchedInsertAll() \
# MAGIC   .execute()
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔗 Related Topics:
# MAGIC * **Phase 4 Day 22**: Delta Lake Time Travel & Versioning
# MAGIC * **Phase 4 Day 24**: Streaming with Delta Lake
# MAGIC * **Phase 4 Day 25**: Delta Lake Advanced Optimization
# MAGIC * **Phase 5**: Production Pipeline Patterns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **© 2026 TRRaveendra - Data Engineering Training Series**

# COMMAND ----------

# DBTITLE 1,Section 5: End-to-End Incremental Pipeline
# MAGIC %md
# MAGIC ---
# MAGIC # 🔹 SECTION 5: End-to-End Incremental Data Pipeline
# MAGIC
# MAGIC ## 🏛️ Scenario:
# MAGIC Build a production-grade incremental pipeline that:
# MAGIC 1. Ingests daily transaction data
# MAGIC 2. Applies upserts to a customer dimension (SCD Type 1)
# MAGIC 3. Maintains transaction history
# MAGIC 4. Handles late-arriving data
# MAGIC 5. Tracks processing metadata
# MAGIC
# MAGIC ### Architecture:
# MAGIC ```
# MAGIC ┌─────────────────┐
# MAGIC │  Source System  │
# MAGIC │ (Daily Extract) │
# MAGIC └───────┬────────┘
# MAGIC         │
# MAGIC         │ MERGE (Upsert)
# MAGIC         ↓
# MAGIC ┌───────┼─────────────────┐
# MAGIC │       Target Tables      │
# MAGIC │  • customer_dim        │
# MAGIC │  • transactions_fact   │
# MAGIC │  • process_log        │
# MAGIC └───────────────────────┘
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Pipeline: Step 1 - Create Target Tables
# Step 1: Create dimension and fact tables

# Customer dimension (SCD Type 1)
spark.sql("""
CREATE OR REPLACE TABLE customer_dimension (
  customer_id INT NOT NULL,
  customer_name STRING NOT NULL,
  email STRING,
  city STRING,
  state STRING,
  customer_segment STRING,
  updated_at TIMESTAMP NOT NULL
) USING DELTA
COMMENT 'Customer dimension with SCD Type 1'
""")

# Transactions fact table
spark.sql("""
CREATE OR REPLACE TABLE transactions_fact (
  transaction_id BIGINT NOT NULL,
  customer_id INT NOT NULL,
  transaction_date DATE NOT NULL,
  amount DOUBLE,
  payment_method STRING,
  status STRING,
  created_at TIMESTAMP NOT NULL
) USING DELTA
PARTITIONED BY (transaction_date)
COMMENT 'Daily transaction records'
""")

# Process log for tracking
spark.sql("""
CREATE OR REPLACE TABLE merge_process_log (
  run_id STRING NOT NULL,
  table_name STRING NOT NULL,
  process_date DATE NOT NULL,
  records_processed INT,
  records_inserted INT,
  records_updated INT,
  start_time TIMESTAMP NOT NULL,
  end_time TIMESTAMP,
  status STRING
) USING DELTA
COMMENT 'MERGE operation audit log'
""")

print("✅ Pipeline tables created successfully")
print("\n📊 Tables:")
print("  1. customer_dimension (SCD Type 1)")
print("  2. transactions_fact (Partitioned by date)")
print("  3. merge_process_log (Audit trail)")

# COMMAND ----------

# DBTITLE 1,Pipeline: Step 2 - Load Initial Data
# Step 2: Load initial dataset
import uuid
from datetime import datetime

# Initial customers
initial_customers = [
    (101, "John Smith", "john.smith@email.com", "New York", "NY", "Premium"),
    (102, "Jane Doe", "jane.doe@email.com", "Los Angeles", "CA", "Standard"),
    (103, "Bob Wilson", "bob.wilson@email.com", "Chicago", "IL", "Basic")
]

cust_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_name", StringType(), False),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("customer_segment", StringType(), True)
])

initial_cust_df = spark.createDataFrame(initial_customers, cust_schema) \
    .withColumn("updated_at", current_timestamp())

initial_cust_df.write.format("delta").mode("append").saveAsTable("customer_dimension")

# Initial transactions
initial_transactions = [
    (1001, 101, date(2026, 4, 20), 1250.50, "Credit Card", "Completed"),
    (1002, 102, date(2026, 4, 20), 89.99, "PayPal", "Completed"),
    (1003, 103, date(2026, 4, 20), 450.00, "Debit Card", "Completed")
]

txn_schema = StructType([
    StructField("transaction_id", LongType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("transaction_date", DateType(), False),
    StructField("amount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("status", StringType(), True)
])

initial_txn_df = spark.createDataFrame(initial_transactions, txn_schema) \
    .withColumn("created_at", current_timestamp())

initial_txn_df.write.format("delta").mode("append").saveAsTable("transactions_fact")

print("✅ Initial data loaded")
print(f"\n📅 Data as of: {date(2026, 4, 20)}")
print("  • 3 customers")
print("  • 3 transactions\n")

print("📊 Customer Dimension:")
display(spark.table("customer_dimension"))

# COMMAND ----------

# DBTITLE 1,Pipeline: Step 3 - Simulate Daily Incremental Load
# Step 3: Simulate day 2 incremental load (2026-04-21)

# New day's data includes:
# - Customer updates (segment changes)
# - New customers
# - New transactions

incremental_customers = [
    (102, "Jane Doe", "jane.doe@email.com", "Los Angeles", "CA", "Premium"),  # Upgraded
    (104, "Alice Brown", "alice.b@email.com", "Seattle", "WA", "Standard")    # New
]

incremental_cust_df = spark.createDataFrame(incremental_customers, cust_schema)
incremental_cust_df.createOrReplaceTempView("customer_incremental")

incremental_transactions = [
    (1004, 101, date(2026, 4, 21), 599.99, "Credit Card", "Completed"),
    (1005, 102, date(2026, 4, 21), 1200.00, "Credit Card", "Completed"),
    (1006, 104, date(2026, 4, 21), 75.50, "PayPal", "Processing")
]

incremental_txn_df = spark.createDataFrame(incremental_transactions, txn_schema)
incremental_txn_df.createOrReplaceTempView("transactions_incremental")

print(f"🔄 Incremental Load for: {date(2026, 4, 21)}")
print("\n📄 Customer Changes:")
display(incremental_cust_df)

print("\n📄 New Transactions:")
display(incremental_txn_df)

# COMMAND ----------

# DBTITLE 1,Pipeline: Step 4 - Execute MERGE Pipeline
# Step 4: Execute complete MERGE pipeline

run_id = str(uuid.uuid4())
process_date = date(2026, 4, 21)
start_time = datetime.now()

print(f"🚀 Starting MERGE Pipeline")
print(f"Run ID: {run_id}")
print(f"Process Date: {process_date}\n")

# --- MERGE 1: Customer Dimension ---
print("[1/2] Merging customer dimension...")

try:
    spark.sql("""
    MERGE INTO customer_dimension AS t
    USING customer_incremental AS s
    ON t.customer_id = s.customer_id
    WHEN MATCHED THEN
      UPDATE SET
        t.customer_name = s.customer_name,
        t.email = s.email,
        t.city = s.city,
        t.state = s.state,
        t.customer_segment = s.customer_segment,
        t.updated_at = current_timestamp()
    WHEN NOT MATCHED THEN
      INSERT (customer_id, customer_name, email, city, state, customer_segment, updated_at)
      VALUES (s.customer_id, s.customer_name, s.email, s.city, s.state, 
              s.customer_segment, current_timestamp())
    """)
    
    # Get metrics (simplified - in production use MERGE metrics)
    total_customers = spark.table("customer_dimension").count()
    
    # Log customer dimension merge
    log_entry_cust = [(run_id, "customer_dimension", process_date, 2, 1, 1, start_time, datetime.now(), "SUCCESS")]
    spark.createDataFrame(log_entry_cust, spark.table("merge_process_log").schema) \
        .write.format("delta").mode("append").saveAsTable("merge_process_log")
    
    print("✅ Customer dimension merged")
    
except Exception as e:
    print(f"❌ Error in customer merge: {str(e)}")
    # Log failure
    log_entry_fail = [(run_id, "customer_dimension", process_date, 0, 0, 0, start_time, datetime.now(), "FAILED")]
    spark.createDataFrame(log_entry_fail, spark.table("merge_process_log").schema) \
        .write.format("delta").mode("append").saveAsTable("merge_process_log")

# --- MERGE 2: Transactions Fact ---
print("[2/2] Merging transactions fact...")

try:
    # For fact tables, typically append-only (no updates)
    # But we'll demonstrate upsert pattern
    incremental_txn_df.withColumn("created_at", current_timestamp()) \
        .write.format("delta").mode("append").saveAsTable("transactions_fact")
    
    # Log transaction merge
    log_entry_txn = [(run_id, "transactions_fact", process_date, 3, 3, 0, start_time, datetime.now(), "SUCCESS")]
    spark.createDataFrame(log_entry_txn, spark.table("merge_process_log").schema) \
        .write.format("delta").mode("append").saveAsTable("merge_process_log")
    
    print("✅ Transactions fact merged")
    
except Exception as e:
    print(f"❌ Error in transaction merge: {str(e)}")

print(f"\n✅ Pipeline completed successfully")
print(f"Run ID: {run_id}")

# COMMAND ----------

# DBTITLE 1,Pipeline: Step 5 - Verify Results
# Step 5: Verify pipeline results

print("🔍 Pipeline Verification\n")

# Check customer dimension
print("1️⃣ Customer Dimension (After MERGE):")
display(spark.table("customer_dimension").orderBy("customer_id"))

# Check transactions
print("\n2️⃣ Transactions Fact (After MERGE):")
display(spark.table("transactions_fact").orderBy("transaction_id"))

# Check process log
print("\n3️⃣ Process Log (Audit Trail):")
display(spark.table("merge_process_log").orderBy("start_time"))

# COMMAND ----------

# DBTITLE 1,Pipeline: Step 6 - Query Pipeline Metrics
# Step 6: Query pipeline execution metrics

print("📊 Pipeline Execution Summary\n")

pipeline_summary = spark.sql(f"""
SELECT 
  table_name,
  COUNT(*) as total_runs,
  SUM(records_processed) as total_records,
  SUM(records_inserted) as total_inserts,
  SUM(records_updated) as total_updates,
  MAX(end_time) as last_run,
  CASE 
    WHEN SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) > 0 THEN 'HAS FAILURES'
    ELSE 'ALL SUCCESS'
  END as health_status
FROM merge_process_log
WHERE run_id = '{run_id}'
GROUP BY table_name
ORDER BY table_name
""")

display(pipeline_summary)

# COMMAND ----------

# DBTITLE 1,Section 6: Performance Optimization
# MAGIC %md
# MAGIC ---
# MAGIC # 🔹 SECTION 6: MERGE Performance Optimization
# MAGIC
# MAGIC ## 🚀 Optimization Techniques:
# MAGIC
# MAGIC ### 1. Partition Pruning
# MAGIC ```sql
# MAGIC -- Use partition columns in merge condition
# MAGIC MERGE INTO target USING source
# MAGIC ON target.id = source.id 
# MAGIC    AND target.date = '2026-04-21'  -- Partition filter
# MAGIC ```
# MAGIC
# MAGIC ### 2. Z-Ordering on Join Keys
# MAGIC ```sql
# MAGIC -- Optimize data layout for MERGE performance
# MAGIC OPTIMIZE target_table
# MAGIC ZORDER BY (id, customer_id);
# MAGIC ```
# MAGIC
# MAGIC ### 3. Conditional Updates (Avoid Unnecessary Writes)
# MAGIC ```sql
# MAGIC WHEN MATCHED AND (
# MAGIC   target.col1 <> source.col1 OR 
# MAGIC   target.col2 <> source.col2
# MAGIC ) THEN UPDATE SET *
# MAGIC ```
# MAGIC
# MAGIC ### 4. Compact Files After MERGE
# MAGIC ```sql
# MAGIC -- MERGE can create many small files
# MAGIC OPTIMIZE target_table;
# MAGIC VACUUM target_table RETAIN 168 HOURS;  -- Clean up old files
# MAGIC ```
# MAGIC
# MAGIC ### 5. Use Broadcast Joins for Small Sources
# MAGIC ```python
# MAGIC from pyspark.sql.functions import broadcast
# MAGIC
# MAGIC # If source is small (<10GB), broadcast it
# MAGIC delta_table.merge(
# MAGIC   broadcast(source_df),
# MAGIC   "target.id = source.id"
# MAGIC ).whenMatchedUpdateAll() \
# MAGIC  .whenNotMatchedInsertAll() \
# MAGIC  .execute()
# MAGIC ```
# MAGIC
# MAGIC ### 6. Enable Auto Compaction
# MAGIC ```sql
# MAGIC -- Databricks automatically compacts small files
# MAGIC ALTER TABLE target_table 
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### 7. Batch Multiple Merges
# MAGIC ```python
# MAGIC # Instead of many small MERGE operations:
# MAGIC for small_batch in batches:  # ❌ Slow
# MAGIC     merge(small_batch)
# MAGIC
# MAGIC # Accumulate and MERGE in larger batches:
# MAGIC large_batch = union_all(batches)  # ✅ Fast
# MAGIC merge(large_batch)
# MAGIC ```
# MAGIC
# MAGIC ### 8. Use Delta Lake Change Data Feed
# MAGIC ```sql
# MAGIC -- Enable CDF for efficient incremental processing
# MAGIC ALTER TABLE source_table 
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- Read only changes since last run
# MAGIC SELECT * FROM table_changes('source_table', 0, 100)
# MAGIC WHERE _change_type IN ('insert', 'update_postimage');
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Demo: Optimize Tables for MERGE
# Demonstrate optimization techniques

print("🚀 Optimizing Tables for MERGE Performance\n")

# 1. Enable Auto Optimize
print("[1/3] Enabling auto-optimize on customer_dimension...")
spark.sql("""
ALTER TABLE customer_dimension 
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
""")
print("✅ Auto-optimize enabled\n")

# 2. Z-Order by frequently joined columns
print("[2/3] Z-ordering customer_dimension by customer_id...")
spark.sql("""
OPTIMIZE customer_dimension
ZORDER BY (customer_id)
""")
print("✅ Z-ordering complete\n")

# 3. Compact transactions_fact table
print("[3/3] Optimizing transactions_fact...")
spark.sql("""
OPTIMIZE transactions_fact
""")
print("✅ Table optimization complete\n")

print("📊 Optimization Summary:")
print("  • Auto-optimize: Enabled (automatic file compaction)")
print("  • Z-Ordering: Applied on customer_id")
print("  • File Compaction: Completed")
print("\n🚀 MERGE operations will now be faster!")

# COMMAND ----------

# DBTITLE 1,Section 7: Change Data Feed (Preview)
# MAGIC %md
# MAGIC ---
# MAGIC # 🔹 SECTION 7: Delta Change Data Feed (CDC)
# MAGIC
# MAGIC ## 🏛️ What is Change Data Feed?
# MAGIC
# MAGIC Change Data Feed (CDF) is a Delta Lake feature that tracks **row-level changes** (inserts, updates, deletes) and makes them queryable.
# MAGIC
# MAGIC ### Use Cases:
# MAGIC * **Incremental ETL**: Process only changed records
# MAGIC * **Downstream Propagation**: Push changes to other systems
# MAGIC * **Audit Trails**: Track data lineage and modifications
# MAGIC * **Event-Driven Architectures**: Trigger actions on data changes
# MAGIC
# MAGIC ### Enable Change Data Feed:
# MAGIC ```sql
# MAGIC ALTER TABLE my_table 
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC ```
# MAGIC
# MAGIC ### Query Changes:
# MAGIC ```sql
# MAGIC -- Get changes between versions
# MAGIC SELECT * FROM table_changes('my_table', 0, 10);
# MAGIC
# MAGIC -- Get changes since timestamp
# MAGIC SELECT * FROM table_changes('my_table', '2026-04-20');
# MAGIC ```
# MAGIC
# MAGIC ### Change Types:
# MAGIC * `insert`: New rows
# MAGIC * `update_preimage`: Row before update
# MAGIC * `update_postimage`: Row after update
# MAGIC * `delete`: Deleted rows
# MAGIC
# MAGIC ### Benefits:
# MAGIC * **Performance**: Process only changes (not full table)
# MAGIC * **Efficiency**: Reduce data movement
# MAGIC * **Simplicity**: No custom change tracking logic
# MAGIC
# MAGIC ### Example Incremental MERGE with CDF:
# MAGIC ```python
# MAGIC # Read changes since last processed version
# MAGIC changes_df = spark.read.format("delta") \
# MAGIC   .option("readChangeFeed", "true") \
# MAGIC   .option("startingVersion", last_version) \
# MAGIC   .table("source_table")
# MAGIC
# MAGIC # Filter to only inserts and updates
# MAGIC incremental_df = changes_df.filter(
# MAGIC   col("_change_type").isin(["insert", "update_postimage"])
# MAGIC )
# MAGIC
# MAGIC # MERGE only changed records
# MAGIC DeltaTable.forName(spark, "target_table") \
# MAGIC   .merge(incremental_df, "target.id = source.id") \
# MAGIC   .whenMatchedUpdateAll() \
# MAGIC   .whenNotMatchedInsertAll() \
# MAGIC   .execute()
# MAGIC ```
# MAGIC
# MAGIC ### ⚠️ Important Notes:
# MAGIC * CDF adds ~20-30% storage overhead
# MAGIC * Must be enabled before changes occur
# MAGIC * Changes retained based on retention settings

# COMMAND ----------

# DBTITLE 1,Demo: Enable and Query Change Data Feed
# Demonstrate Change Data Feed

print("🔍 Change Data Feed Demo\n")

# Enable CDF on customer_dimension
print("[1/3] Enabling Change Data Feed...")
spark.sql("""
ALTER TABLE customer_dimension 
SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
print("✅ CDF enabled on customer_dimension\n")

# Make some changes
print("[2/3] Making changes to track...")
spark.sql("""
UPDATE customer_dimension 
SET customer_segment = 'VIP' 
WHERE customer_id = 101
""")
print("✅ Updated customer_id=101\n")

# Query changes
print("[3/3] Querying change data feed...")
print("\n📄 Recent changes (shows insert, update_preimage, update_postimage):\n")

try:
    # Note: CDF tracks changes from when it's enabled
    changes = spark.sql("""
    SELECT 
      customer_id,
      customer_name,
      customer_segment,
      _change_type,
      _commit_version,
      _commit_timestamp
    FROM table_changes('customer_dimension', 0)
    WHERE customer_id = 101
    ORDER BY _commit_version, _change_type
    """)
    
    display(changes)
    
    print("\n💡 Notice the _change_type column:")
    print("  • insert: Original record")
    print("  • update_preimage: Before update (VIP)")
    print("  • update_postimage: After update (Premium)")
    
except Exception as e:
    print(f"Note: CDF tracks changes from enablement forward. Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Section 8: Genie Code Agent Prompts
# MAGIC %md
# MAGIC ---
# MAGIC # 🤖 SECTION 8: Genie Code Agent - Example Prompts
# MAGIC
# MAGIC ## 💡 Use Genie Code to Generate MERGE Logic:
# MAGIC
# MAGIC ### SCD Type 1 Prompts:
# MAGIC ```
# MAGIC 💬 "Generate SCD Type 1 MERGE to update customer emails in customer_dim"
# MAGIC
# MAGIC 💬 "Create upsert pattern for product_table matching on product_id"
# MAGIC
# MAGIC 💬 "Build MERGE statement that overwrites existing records with new data"
# MAGIC ```
# MAGIC
# MAGIC ### SCD Type 2 Prompts:
# MAGIC ```
# MAGIC 💬 "Implement SCD Type 2 on employee_dim tracking salary history"
# MAGIC
# MAGIC 💬 "Create MERGE with effective_date and end_date for price tracking"
# MAGIC
# MAGIC 💬 "Build SCD Type 2 pipeline that maintains is_current flag"
# MAGIC
# MAGIC 💬 "Generate two-step MERGE: expire old records and insert new versions"
# MAGIC ```
# MAGIC
# MAGIC ### Upsert Pattern Prompts:
# MAGIC ```
# MAGIC 💬 "Create upsert with timestamp tracking (created_at, updated_at)"
# MAGIC
# MAGIC 💬 "Build conditional MERGE that only updates if values changed"
# MAGIC
# MAGIC 💬 "Generate 3-way MERGE with INSERT, UPDATE, and DELETE"
# MAGIC
# MAGIC 💬 "Create MERGE with deduplication in source using ROW_NUMBER"
# MAGIC ```
# MAGIC
# MAGIC ### Performance Optimization Prompts:
# MAGIC ```
# MAGIC 💬 "Optimize my_table for MERGE operations with Z-ordering"
# MAGIC
# MAGIC 💬 "Enable auto-compaction on delta_table"
# MAGIC
# MAGIC 💬 "Add partition pruning to my MERGE statement"
# MAGIC
# MAGIC 💬 "Show MERGE metrics: rows inserted, updated, deleted"
# MAGIC ```
# MAGIC
# MAGIC ### Pipeline Prompts:
# MAGIC ```
# MAGIC 💬 "Build end-to-end incremental pipeline with MERGE and audit logging"
# MAGIC
# MAGIC 💬 "Create daily batch job that merges CDC data into target table"
# MAGIC
# MAGIC 💬 "Generate pipeline that handles late-arriving data with MERGE"
# MAGIC ```
# MAGIC
# MAGIC ### Troubleshooting Prompts:
# MAGIC ```
# MAGIC 💬 "Why is my MERGE slow? Suggest optimizations"
# MAGIC
# MAGIC 💬 "Debug NULL comparison issue in MERGE condition"
# MAGIC
# MAGIC 💬 "Fix duplicate key violation in SCD Type 2 MERGE"
# MAGIC
# MAGIC 💬 "Explain why MERGE created many small files"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Pro Tips for Prompting:
# MAGIC
# MAGIC 1. **Be Specific**: Mention table names, key columns, and requirements
# MAGIC 2. **State SCD Type**: Specify Type 1 or Type 2 explicitly
# MAGIC 3. **Mention Constraints**: Unity Catalog, serverless, no RDD, etc.
# MAGIC 4. **Request Explanations**: Ask "explain the logic" for learning
# MAGIC 5. **Ask for Optimization**: Request performance best practices
# MAGIC
# MAGIC ### Example Comprehensive Prompt:
# MAGIC ```
# MAGIC 💬 "Generate SCD Type 2 MERGE for customer_dimension table in Unity Catalog.
# MAGIC      - Business key: customer_id
# MAGIC      - Track: email, city, segment changes
# MAGIC      - Use effective_date, end_date, is_current
# MAGIC      - Add surrogate key
# MAGIC      - Optimize for serverless compute
# MAGIC      - Include PySpark and SQL versions
# MAGIC      - Add comments explaining each step"
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 4: Upsert Patterns
# MAGIC %md
# MAGIC ---
# MAGIC # 🔹 SECTION 4: Upsert Patterns (UPDATE + INSERT)
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC "Upsert" is a made-up word from **UP**date + in**SERT**. It's like a smart save button:
# MAGIC * If something exists → Update it
# MAGIC * If something doesn't exist → Insert it
# MAGIC
# MAGIC Example: Saving a game. If you have a saved game, it updates. If not, it creates new.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### What is an Upsert?
# MAGIC An **upsert** (portmanteau of UPDATE and INSERT) is an atomic operation that:
# MAGIC * **Updates** a record if it exists (based on a key)
# MAGIC * **Inserts** a new record if it doesn't exist
# MAGIC
# MAGIC ### Common Upsert Patterns:
# MAGIC
# MAGIC #### Pattern 1: Simple Upsert (Match on Single Key)
# MAGIC ```sql
# MAGIC MERGE INTO target AS t
# MAGIC USING source AS s
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ```
# MAGIC
# MAGIC #### Pattern 2: Conditional Upsert (Update Only If Changed)
# MAGIC ```sql
# MAGIC MERGE INTO target AS t
# MAGIC USING source AS s
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED AND (t.value <> s.value OR t.status <> s.status) THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ```
# MAGIC
# MAGIC #### Pattern 3: Upsert with Timestamp Tracking
# MAGIC ```sql
# MAGIC MERGE INTO target AS t
# MAGIC USING source AS s
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *, t.updated_at = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (*, created_at, updated_at) 
# MAGIC   VALUES (s.*, current_timestamp(), current_timestamp())
# MAGIC ```
# MAGIC
# MAGIC #### Pattern 4: Upsert with DELETE (3-way MERGE)
# MAGIC ```sql
# MAGIC MERGE INTO target AS t
# MAGIC USING source AS s
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED AND s.status = 'deleted' THEN DELETE
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ```
# MAGIC
# MAGIC #### Pattern 5: Deduplication Upsert
# MAGIC ```sql
# MAGIC MERGE INTO target AS t
# MAGIC USING (
# MAGIC   SELECT id, name, value, 
# MAGIC          ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp DESC) as rn
# MAGIC   FROM source
# MAGIC ) AS s
# MAGIC WHERE s.rn = 1  -- Take only latest record per id
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ```
# MAGIC
# MAGIC ### Use Cases:
# MAGIC * **CDC Pipelines**: Apply change streams from databases
# MAGIC * **API Sync**: Synchronize data from external APIs
# MAGIC * **Event Processing**: Process event streams
# MAGIC * **Data Lake Updates**: Incrementally update data lakes
# MAGIC * **Deduplication**: Remove duplicates while merging

# COMMAND ----------

# DBTITLE 1,Pattern 1: Simple Upsert
# Pattern 1: Simple Upsert - Match on key, update or insert

# Create orders table
orders_initial = [
    (1001, "Alice", "Laptop", 1200.00, "Shipped"),
    (1002, "Bob", "Mouse", 25.00, "Delivered"),
    (1003, "Carol", "Keyboard", 75.00, "Processing")
]

orders_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer", StringType(), False),
    StructField("product", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True)
])

orders_df = spark.createDataFrame(orders_initial, orders_schema)
orders_df.write.format("delta").mode("overwrite").saveAsTable("orders_table")

print("📋 Initial Orders:")
display(spark.table("orders_table"))

# COMMAND ----------

# DBTITLE 1,Pattern 1: Execute Simple Upsert
# Incoming updates: status changes + new order
orders_updates = [
    (1002, "Bob", "Mouse", 25.00, "Returned"),        # Status change
    (1003, "Carol", "Keyboard", 75.00, "Shipped"),    # Status change
    (1004, "David", "Monitor", 350.00, "Processing")  # New order
]

orders_updates_df = spark.createDataFrame(orders_updates, orders_schema)
orders_updates_df.createOrReplaceTempView("orders_updates")

# Execute simple upsert
spark.sql("""
MERGE INTO orders_table AS t
USING orders_updates AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

print("✅ Pattern 1 Complete: Simple Upsert")
print("\nResults:")
print("  • order_id=1002: Status updated (Delivered → Returned)")
print("  • order_id=1003: Status updated (Processing → Shipped)")
print("  • order_id=1004: New order inserted\n")

display(spark.table("orders_table").orderBy("order_id"))

# COMMAND ----------

# DBTITLE 1,Pattern 2: Upsert with Timestamp Tracking
# Pattern 2: Track when records are created and updated

# Create table with audit columns
user_accounts_initial = [
    (1, "alice@email.com", "Active"),
    (2, "bob@email.com", "Active")
]

account_schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("email", StringType(), False),
    StructField("status", StringType(), True)
])

accounts_df = spark.createDataFrame(user_accounts_initial, account_schema) \
    .withColumn("created_at", current_timestamp()) \
    .withColumn("updated_at", current_timestamp())

accounts_df.write.format("delta").mode("overwrite").saveAsTable("user_accounts")

print("📋 Initial User Accounts:")
display(spark.table("user_accounts"))

# COMMAND ----------

# DBTITLE 1,Pattern 2: Execute Timestamp Upsert
# Updates with timestamp tracking
account_updates = [
    (2, "bob@email.com", "Suspended"),        # Status change
    (3, "carol@email.com", "Active")          # New user
]

update_schema_simple = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("email", StringType(), False),
    StructField("status", StringType(), True)
])

account_updates_df = spark.createDataFrame(account_updates, update_schema_simple)
account_updates_df.createOrReplaceTempView("account_updates")

# Upsert with timestamp tracking
spark.sql("""
MERGE INTO user_accounts AS t
USING account_updates AS s
ON t.user_id = s.user_id
WHEN MATCHED THEN
  UPDATE SET
    t.email = s.email,
    t.status = s.status,
    t.updated_at = current_timestamp()  -- Track update time
WHEN NOT MATCHED THEN
  INSERT (user_id, email, status, created_at, updated_at)
  VALUES (s.user_id, s.email, s.status, current_timestamp(), current_timestamp())
""")

print("✅ Pattern 2 Complete: Upsert with Timestamp Tracking")
print("\nResults:")
print("  • user_id=2: Status updated + updated_at refreshed")
print("  • user_id=3: New user + created_at/updated_at set\n")

display(spark.table("user_accounts").orderBy("user_id"))

# COMMAND ----------

# DBTITLE 1,Pattern 3: Conditional Upsert (Update Only If Changed)
# Pattern 3: Avoid unnecessary updates if data hasn't changed

# Create metrics table
metrics_initial = [
    ("product_A", 100, 5000.00),
    ("product_B", 200, 8000.00)
]

metrics_schema = StructType([
    StructField("product_name", StringType(), False),
    StructField("quantity_sold", IntegerType(), True),
    StructField("revenue", DoubleType(), True)
])

metrics_df = spark.createDataFrame(metrics_initial, metrics_schema)
metrics_df.write.format("delta").mode("overwrite").saveAsTable("product_metrics")

print("📋 Initial Product Metrics:")
display(spark.table("product_metrics"))

# COMMAND ----------

# DBTITLE 1,Pattern 3: Execute Conditional Upsert
# Updates: Some changed, some unchanged
metrics_updates = [
    ("product_A", 100, 5000.00),    # NO CHANGE - should not update
    ("product_B", 250, 10000.00),   # CHANGED - should update
    ("product_C", 50, 2000.00)      # NEW - should insert
]

metrics_updates_df = spark.createDataFrame(metrics_updates, metrics_schema)
metrics_updates_df.createOrReplaceTempView("metrics_updates")

# Conditional upsert: Only update if values actually changed
spark.sql("""
MERGE INTO product_metrics AS t
USING metrics_updates AS s
ON t.product_name = s.product_name
WHEN MATCHED AND (t.quantity_sold <> s.quantity_sold OR t.revenue <> s.revenue) THEN
  UPDATE SET *  -- Only update if data changed
WHEN NOT MATCHED THEN
  INSERT *
""")

print("✅ Pattern 3 Complete: Conditional Upsert")
print("\nResults:")
print("  • product_A: NO UPDATE (values unchanged)")
print("  • product_B: UPDATED (values changed)")
print("  • product_C: INSERTED (new product)")
print("\n🚀 Performance benefit: Avoids unnecessary Delta file writes!\n")

display(spark.table("product_metrics").orderBy("product_name"))

# COMMAND ----------

# DBTITLE 1,Pattern 4: Upsert with DELETE (3-way MERGE)
# Pattern 4: Handle INSERT, UPDATE, and DELETE in single MERGE

# Create inventory table
inventory_initial = [
    ("SKU001", "Widget A", 100, "active"),
    ("SKU002", "Widget B", 200, "active"),
    ("SKU003", "Widget C", 50, "active")
]

inventory_schema = StructType([
    StructField("sku", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("quantity", IntegerType(), True),
    StructField("status", StringType(), True)
])

inventory_df = spark.createDataFrame(inventory_initial, inventory_schema)
inventory_df.write.format("delta").mode("overwrite").saveAsTable("inventory_table")

print("📋 Initial Inventory:")
display(spark.table("inventory_table"))

# COMMAND ----------

# DBTITLE 1,Pattern 4: Execute 3-way MERGE
# Updates: Insert, Update, and Delete
inventory_updates = [
    ("SKU001", "Widget A", 150, "active"),     # UPDATE quantity
    ("SKU002", "Widget B", 0, "deleted"),      # DELETE (marked for deletion)
    ("SKU004", "Widget D", 75, "active")       # INSERT new SKU
]

inventory_updates_df = spark.createDataFrame(inventory_updates, inventory_schema)
inventory_updates_df.createOrReplaceTempView("inventory_updates")

# 3-way MERGE: INSERT + UPDATE + DELETE
spark.sql("""
MERGE INTO inventory_table AS t
USING inventory_updates AS s
ON t.sku = s.sku
WHEN MATCHED AND s.status = 'deleted' THEN
  DELETE  -- Remove items marked for deletion
WHEN MATCHED THEN
  UPDATE SET *  -- Update existing items
WHEN NOT MATCHED AND s.status <> 'deleted' THEN
  INSERT *  -- Insert new items (but not deleted ones)
""")

print("✅ Pattern 4 Complete: 3-way MERGE (INSERT + UPDATE + DELETE)")
print("\nResults:")
print("  • SKU001: UPDATED (quantity 100 → 150)")
print("  • SKU002: DELETED (status = 'deleted')")
print("  • SKU003: UNCHANGED (not in source)")
print("  • SKU004: INSERTED (new SKU)\n")

display(spark.table("inventory_table").orderBy("sku"))

# COMMAND ----------

# DBTITLE 1,Upsert Patterns: Summary
# MAGIC %md
# MAGIC ### 📊 Upsert Pattern Decision Tree:
# MAGIC
# MAGIC ```
# MAGIC Q1: Do you need to track changes over time?
# MAGIC     YES → Use SCD Type 2 (see Section 3)
# MAGIC     NO → Continue to Q2
# MAGIC
# MAGIC Q2: Do you need to DELETE records?
# MAGIC     YES → Use Pattern 4 (3-way MERGE)
# MAGIC     NO → Continue to Q3
# MAGIC
# MAGIC Q3: Do you need audit timestamps?
# MAGIC     YES → Use Pattern 2 (Timestamp Tracking)
# MAGIC     NO → Continue to Q4
# MAGIC
# MAGIC Q4: Do you want to optimize write performance?
# MAGIC     YES → Use Pattern 3 (Conditional Upsert)
# MAGIC     NO → Use Pattern 1 (Simple Upsert)
# MAGIC ```
# MAGIC
# MAGIC ### 🎯 Best Practices:
# MAGIC
# MAGIC **1. Use Conditional Updates to Avoid Unnecessary Writes:**
# MAGIC ```sql
# MAGIC WHEN MATCHED AND (t.col1 <> s.col1 OR t.col2 <> s.col2) THEN UPDATE
# MAGIC ```
# MAGIC
# MAGIC **2. Handle NULL Comparisons Carefully:**
# MAGIC ```sql
# MAGIC -- ❌ Wrong: NULL <> NULL returns NULL (not TRUE)
# MAGIC WHEN MATCHED AND t.value <> s.value THEN UPDATE
# MAGIC
# MAGIC -- ✅ Correct: Use NULL-safe comparison
# MAGIC WHEN MATCHED AND (t.value <> s.value OR 
# MAGIC                    (t.value IS NULL AND s.value IS NOT NULL) OR
# MAGIC                    (t.value IS NOT NULL AND s.value IS NULL)) THEN UPDATE
# MAGIC ```
# MAGIC
# MAGIC **3. Add Metrics to Track MERGE Operations:**
# MAGIC ```sql
# MAGIC -- Use MERGE metrics (available in Databricks Runtime 11.3+)
# MAGIC SELECT num_affected_rows, num_updated_rows, num_inserted_rows, num_deleted_rows
# MAGIC FROM (MERGE INTO ... OUTPUT $action, ...)
# MAGIC ```
# MAGIC
# MAGIC **4. Use Deduplication in Source:**
# MAGIC ```sql
# MAGIC USING (
# MAGIC   SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY timestamp DESC) as rn
# MAGIC   FROM source
# MAGIC   WHERE rn = 1  -- Take only latest per key
# MAGIC ) AS s
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3: SCD Type 2 - History Tracking
# MAGIC %md
# MAGIC ---
# MAGIC # 🔹 SECTION 3: SCD Type 2 (Slowly Changing Dimension - History Tracking)
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC SCD Type 2 is like keeping a diary. When something changes, you **don't erase the old entry**. You write a **new entry** with today's date and mark the old one as "past".
# MAGIC
# MAGIC **Example:**
# MAGIC * Jan 1: Bob works in Sales (we record this with start date Jan 1)
# MAGIC * Mar 15: Bob moves to Marketing (we keep the Sales record marked "ended Mar 14" and add new Marketing record starting Mar 15)
# MAGIC * Result: We can see Bob's **entire job history**!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### SCD Type 2 Characteristics:
# MAGIC
# MAGIC | Feature | Description |
# MAGIC |---------|-------------|
# MAGIC | **History** | ✅ Fully maintained |
# MAGIC | **Storage** | ⚠️ Higher (multiple rows per entity) |
# MAGIC | **Complexity** | ⚠️ Moderate to High |
# MAGIC | **Use Case** | Audit trails, regulatory compliance, trend analysis |
# MAGIC | **Query Simplicity** | ⚠️ Requires date filtering |
# MAGIC | **Auditability** | ✅ Complete historical reconstruction |
# MAGIC
# MAGIC ### Required Columns:
# MAGIC
# MAGIC ```python
# MAGIC # Standard SCD Type 2 schema:
# MAGIC - primary_key         # Business key (e.g., customer_id)
# MAGIC - surrogate_key       # Unique row identifier (optional but recommended)
# MAGIC - effective_date      # When this version became active
# MAGIC - end_date           # When this version expired (NULL for current)
# MAGIC - is_current         # Boolean flag (TRUE for current version)
# MAGIC - ... business columns ...
# MAGIC ```
# MAGIC
# MAGIC ### SCD Type 2 Logic Flow:
# MAGIC
# MAGIC ```
# MAGIC For each incoming record:
# MAGIC   1. Check if record exists with matching business key
# MAGIC   2. If exists AND values changed:
# MAGIC      a. UPDATE old record: Set end_date = current_date - 1, is_current = FALSE
# MAGIC      b. INSERT new record: Set effective_date = current_date, is_current = TRUE
# MAGIC   3. If NOT exists:
# MAGIC      a. INSERT new record: Set effective_date = current_date, is_current = TRUE
# MAGIC ```
# MAGIC
# MAGIC ### Implementation Challenges:
# MAGIC * **Two-Step Process**: Update existing + Insert new
# MAGIC * **Date Management**: Careful handling of effective/end dates
# MAGIC * **Query Complexity**: Need to filter on `is_current = TRUE` for latest
# MAGIC * **Performance**: More rows = slower queries (needs optimization)
# MAGIC
# MAGIC ### When to Use SCD Type 2:
# MAGIC * **Regulatory Compliance**: Financial services, healthcare
# MAGIC * **Audit Requirements**: Track who changed what and when
# MAGIC * **Trend Analysis**: Analyze historical patterns
# MAGIC * **Price History**: Track pricing changes over time
# MAGIC * **Organizational Changes**: Department transfers, role changes

# COMMAND ----------

# DBTITLE 1,SCD Type 2: Setup Product Dimension
# Create product dimension table with SCD Type 2 columns
from datetime import date, timedelta

product_initial = [
    (1, 1001, "Laptop Pro", "Electronics", 1200.00, date(2026, 1, 1), None, True),
    (2, 1002, "Wireless Mouse", "Electronics", 25.00, date(2026, 1, 1), None, True),
    (3, 1003, "Office Chair", "Furniture", 350.00, date(2026, 1, 1), None, True)
]

product_schema = StructType([
    StructField("surrogate_key", IntegerType(), False),      # Unique row ID
    StructField("product_id", IntegerType(), False),         # Business key
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("effective_date", DateType(), False),        # SCD Type 2 column
    StructField("end_date", DateType(), True),               # SCD Type 2 column
    StructField("is_current", BooleanType(), False)          # SCD Type 2 column
])

product_df = spark.createDataFrame(product_initial, product_schema)
product_df.write.format("delta").mode("overwrite").saveAsTable("product_dim_type2")

print("📋 Initial Product Dimension (SCD Type 2):")
print("\n📅 All records have:")
print("  • effective_date = 2026-01-01 (when record became active)")
print("  • end_date = NULL (still current)")
print("  • is_current = TRUE (latest version)\n")

display(spark.table("product_dim_type2"))

# COMMAND ----------

# DBTITLE 1,SCD Type 2: Prepare Updates
# Incoming changes on 2026-04-21:
# - product_id=1001: Price change 1200.00 -> 1100.00 (price drop)
# - product_id=1002: Category change Electronics -> Accessories
# - product_id=1004: New product

current_date = date(2026, 4, 21)

product_updates = [
    (1001, "Laptop Pro", "Electronics", 1100.00),        # Price changed
    (1002, "Wireless Mouse", "Accessories", 25.00),      # Category changed
    (1004, "Standing Desk", "Furniture", 450.00)         # New product
]

update_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True)
])

product_updates_df = spark.createDataFrame(product_updates, update_schema)
product_updates_df.createOrReplaceTempView("product_updates")

print(f"🔄 Incoming Product Updates (as of {current_date}):")
print("\nChanges:")
print("  • product_id=1001: Price changed (1200.00 → 1100.00)")
print("  • product_id=1002: Category changed (Electronics → Accessories)")
print("  • product_id=1004: New product\n")

display(product_updates_df)

# COMMAND ----------

# DBTITLE 1,SCD Type 2: Step 1 - Expire Old Records
# Step 1: Expire old records (set end_date and is_current flag)
# For records that have changed, we need to close out the current version

merge_sql_step1 = f"""
MERGE INTO product_dim_type2 AS target
USING product_updates AS source
ON target.product_id = source.product_id 
   AND target.is_current = TRUE
WHEN MATCHED 
  AND (target.price <> source.price 
       OR target.category <> source.category
       OR target.product_name <> source.product_name) THEN
  UPDATE SET
    target.end_date = DATE('{current_date}') - INTERVAL 1 DAY,  -- Expire yesterday
    target.is_current = FALSE                                    -- No longer current
"""

spark.sql(merge_sql_step1)

print("✅ Step 1 Complete: Old records expired")
print(f"\n📅 Records with changes now have:")
print(f"  • end_date = {current_date - timedelta(days=1)} (expired)")
print("  • is_current = FALSE\n")

print("📊 Current state after Step 1:")
display(spark.table("product_dim_type2").orderBy("product_id", "effective_date"))

# COMMAND ----------

# DBTITLE 1,SCD Type 2: Step 2 - Insert New Records
# Step 2: Insert new versions of changed records + completely new records

# Get max surrogate key for new records
max_key = spark.sql("SELECT COALESCE(MAX(surrogate_key), 0) as max_key FROM product_dim_type2").first()["max_key"]

# Add SCD Type 2 columns to source data
from pyspark.sql.window import Window

product_updates_with_scd = product_updates_df \
  .withColumn("effective_date", lit(current_date)) \
  .withColumn("end_date", lit(None).cast(DateType())) \
  .withColumn("is_current", lit(True)) \
  .withColumn("row_num", row_number().over(Window.orderBy("product_id"))) \
  .withColumn("surrogate_key", col("row_num") + lit(max_key))

product_updates_with_scd.createOrReplaceTempView("product_updates_with_scd")

merge_sql_step2 = """
MERGE INTO product_dim_type2 AS target
USING product_updates_with_scd AS source
ON target.product_id = source.product_id 
   AND target.is_current = TRUE
WHEN NOT MATCHED THEN
  INSERT (surrogate_key, product_id, product_name, category, price, 
          effective_date, end_date, is_current)
  VALUES (source.surrogate_key, source.product_id, source.product_name, 
          source.category, source.price, source.effective_date, 
          source.end_date, source.is_current)
"""

spark.sql(merge_sql_step2)

print("✅ Step 2 Complete: New record versions inserted")
print(f"\n🆕 New records have:")
print(f"  • effective_date = {current_date} (active today)")
print("  • end_date = NULL (currently active)")
print("  • is_current = TRUE\n")

print("📊 Final state after SCD Type 2 MERGE:")
display(spark.table("product_dim_type2").orderBy("product_id", "effective_date"))

# COMMAND ----------

# DBTITLE 1,SCD Type 2: Query Current Records
# Query to get only CURRENT records (latest version)
print("🔍 Querying CURRENT records only (is_current = TRUE):\n")

current_records = spark.sql("""
SELECT 
  product_id,
  product_name,
  category,
  price,
  effective_date,
  end_date,
  is_current
FROM product_dim_type2
WHERE is_current = TRUE
ORDER BY product_id
""")

print("✅ Current product catalog (as of today):")
display(current_records)

# COMMAND ----------

# DBTITLE 1,SCD Type 2: Query Historical Records
# Query to get HISTORICAL records for a specific product
print("📅 Product History for product_id=1001 (Laptop Pro):\n")

history_query = spark.sql("""
SELECT 
  surrogate_key,
  product_id,
  product_name,
  price,
  effective_date,
  end_date,
  is_current,
  CASE 
    WHEN is_current THEN 'CURRENT VERSION'
    ELSE 'HISTORICAL VERSION'
  END as record_status
FROM product_dim_type2
WHERE product_id = 1001
ORDER BY effective_date
""")

print("Price History:")
print("  • Row 1: $1200.00 (Jan 1 - Apr 20) - EXPIRED")
print("  • Row 2: $1100.00 (Apr 21 - Present) - CURRENT\n")

display(history_query)

# COMMAND ----------

# DBTITLE 1,SCD Type 2: Point-in-Time Query
# Query: What was the product catalog on a specific date?
specific_date = date(2026, 3, 1)

print(f"⏰ Point-in-Time Query: Product catalog as of {specific_date}\n")

point_in_time_query = spark.sql(f"""
SELECT 
  product_id,
  product_name,
  category,
  price,
  effective_date,
  end_date
FROM product_dim_type2
WHERE effective_date <= DATE('{specific_date}')
  AND (end_date IS NULL OR end_date >= DATE('{specific_date}'))
ORDER BY product_id
""")

print("✅ This shows the state of products as they were on March 1, 2026")
print("Notice: All original prices/categories (before April 21 changes)\n")

display(point_in_time_query)

# COMMAND ----------

# DBTITLE 1,SCD Type 2: Key Takeaways
# MAGIC %md
# MAGIC ### 🔑 SCD Type 2 Key Takeaways:
# MAGIC
# MAGIC #### ✅ Advantages:
# MAGIC * **Complete History**: Full audit trail of all changes
# MAGIC * **Point-in-Time Queries**: Reconstruct data as of any date
# MAGIC * **Compliance Ready**: Meets regulatory requirements
# MAGIC * **Trend Analysis**: Analyze changes over time
# MAGIC * **Reversible**: Can roll back to any historical state
# MAGIC
# MAGIC #### ❌ Disadvantages:
# MAGIC * **Complex Logic**: Two-step MERGE process
# MAGIC * **Storage Overhead**: Multiple rows per entity
# MAGIC * **Query Complexity**: Need `is_current = TRUE` filter
# MAGIC * **Performance Impact**: Larger tables, slower queries
# MAGIC * **Maintenance**: Need to manage date ranges carefully
# MAGIC
# MAGIC #### 🎯 Best Practices:
# MAGIC
# MAGIC **1. Always Use Surrogate Keys:**
# MAGIC ```sql
# MAGIC -- Bad: Using business key as primary key
# MAGIC PRIMARY KEY (customer_id)  -- ❌ Multiple rows will violate this!
# MAGIC
# MAGIC -- Good: Using surrogate key
# MAGIC PRIMARY KEY (surrogate_key)  -- ✅ Unique for each row
# MAGIC ```
# MAGIC
# MAGIC **2. Index on Business Key + is_current:**
# MAGIC ```sql
# MAGIC CREATE INDEX idx_current ON product_dim_type2 (product_id, is_current);
# MAGIC ```
# MAGIC
# MAGIC **3. Date Range Validation:**
# MAGIC ```python
# MAGIC # Ensure no gaps or overlaps
# MAGIC assert end_date IS NULL OR end_date = next_effective_date - 1 day
# MAGIC ```
# MAGIC
# MAGIC **4. Standard Query Pattern:**
# MAGIC ```sql
# MAGIC -- Always filter current records
# MAGIC SELECT * FROM dim_table 
# MAGIC WHERE is_current = TRUE
# MAGIC
# MAGIC -- Point-in-time query
# MAGIC SELECT * FROM dim_table
# MAGIC WHERE effective_date <= '2026-01-15'
# MAGIC   AND (end_date IS NULL OR end_date >= '2026-01-15')
# MAGIC ```
# MAGIC
# MAGIC #### ⚠️ Common Mistakes:
# MAGIC * Not using surrogate keys
# MAGIC * Forgetting to set `is_current = FALSE` on old records
# MAGIC * Date range gaps or overlaps
# MAGIC * Not handling NULL values in comparisons
# MAGIC * Querying without `is_current = TRUE` filter

# COMMAND ----------

# DBTITLE 1,Section 2: SCD Type 1 - Overwrite Pattern
# MAGIC %md
# MAGIC ---
# MAGIC # 🔹 SECTION 2: SCD Type 1 (Slowly Changing Dimension - Overwrite)
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC SCD Type 1 is like updating your phone contact list. When your friend changes their phone number, you **just update it**. You don't keep the old number. You only care about the **current** information.
# MAGIC
# MAGIC **Example:**
# MAGIC * Monday: Bob's email is bob@oldcompany.com
# MAGIC * Tuesday: Bob changes jobs, email becomes bob@newcompany.com
# MAGIC * Result: We only store bob@newcompany.com (old email is gone forever)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### SCD Type 1 Characteristics:
# MAGIC
# MAGIC | Feature | Description |
# MAGIC |---------|-------------|
# MAGIC | **History** | ❌ Not maintained |
# MAGIC | **Storage** | ✅ Minimal (no duplicate rows) |
# MAGIC | **Complexity** | ✅ Simple |
# MAGIC | **Use Case** | Data corrections, non-critical changes |
# MAGIC | **Query Simplicity** | ✅ Always get current state |
# MAGIC | **Auditability** | ❌ Cannot reconstruct past states |
# MAGIC
# MAGIC ### When to Use SCD Type 1:
# MAGIC * **Data Corrections**: Fixing typos, errors
# MAGIC * **Non-Material Changes**: Email updates, phone numbers
# MAGIC * **Reference Data**: Country codes, category names
# MAGIC * **Performance-Critical Systems**: Where history isn't required
# MAGIC * **Regulatory Compliance NOT Required**: No audit trail needed
# MAGIC
# MAGIC ### Implementation Pattern:
# MAGIC ```sql
# MAGIC MERGE INTO target_table AS t
# MAGIC USING source_table AS s
# MAGIC ON t.primary_key = s.primary_key
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *  -- Overwrite all columns
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *      -- Insert new records
# MAGIC ```
# MAGIC
# MAGIC ### Key Points:
# MAGIC * **Simple MERGE**: Just UPDATE + INSERT
# MAGIC * **No Additional Columns**: No timestamps, no flags
# MAGIC * **Overwrites Existing Data**: Historical values lost
# MAGIC * **Best for Non-Critical Attributes**: Not for regulatory/audit scenarios

# COMMAND ----------

# DBTITLE 1,SCD Type 1: Setup Customer Dimension
# Create customer dimension table (SCD Type 1)
customer_initial = [
    (101, "John Doe", "john.doe@email.com", "New York", "Gold"),
    (102, "Jane Smith", "jane.smith@email.com", "Los Angeles", "Silver"),
    (103, "Bob Johnson", "bob.j@email.com", "Chicago", "Bronze")
]

customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("tier", StringType(), True)
])

customer_df = spark.createDataFrame(customer_initial, customer_schema)
customer_df.write.format("delta").mode("overwrite").saveAsTable("customer_dim_type1")

print("📋 Initial Customer Dimension (SCD Type 1):")
display(spark.table("customer_dim_type1"))

# COMMAND ----------

# DBTITLE 1,SCD Type 1: Create Update Data
# Incoming changes:
# - customer_id=101: Email updated (changed jobs)
# - customer_id=102: Tier upgraded Silver -> Gold
# - customer_id=104: New customer

customer_updates = [
    (101, "John Doe", "john.doe@newcompany.com", "New York", "Gold"),    # Email changed
    (102, "Jane Smith", "jane.smith@email.com", "Los Angeles", "Gold"),  # Tier upgraded
    (104, "Alice Brown", "alice.b@email.com", "Seattle", "Silver")       # New customer
]

customer_updates_df = spark.createDataFrame(customer_updates, customer_schema)
customer_updates_df.createOrReplaceTempView("customer_updates")

print("🔄 Incoming Customer Updates:")
print("\nChanges:")
print("  • customer_id=101: Email change (job change)")
print("  • customer_id=102: Tier upgrade (Silver → Gold)")
print("  • customer_id=104: New customer\n")

display(customer_updates_df)

# COMMAND ----------

# DBTITLE 1,SCD Type 1: Execute MERGE (Overwrite)
# MAGIC %sql
# MAGIC -- SCD Type 1 MERGE: Simply overwrite existing records
# MAGIC MERGE INTO customer_dim_type1 AS target
# MAGIC USING customer_updates AS source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *  -- Overwrite all columns with new values
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *      -- Insert new customers

# COMMAND ----------

# DBTITLE 1,SCD Type 1: Verify Results
# Verify the results after SCD Type 1 MERGE
print("✅ After SCD Type 1 MERGE:")
print("\n📊 Results:")
print("  • customer_id=101: Email overwritten (old email LOST)")
print("  • customer_id=102: Tier overwritten (Silver history LOST)")
print("  • customer_id=103: Unchanged (not in source)")
print("  • customer_id=104: New record inserted")
print("\n⚠️ Note: Historical values (old email, old tier) cannot be recovered!\n")

display(spark.table("customer_dim_type1").orderBy("customer_id"))

# COMMAND ----------

# DBTITLE 1,SCD Type 1: PySpark DataFrame API
# Alternative: Using PySpark DataFrame API for SCD Type 1
from delta.tables import DeltaTable

# Get DeltaTable instance
delta_table = DeltaTable.forName(spark, "customer_dim_type1")

# Perform MERGE using DataFrame API
delta_table.alias("target") \
  .merge(
    customer_updates_df.alias("source"),
    "target.customer_id = source.customer_id"
  ) \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll() \
  .execute()

print("✅ SCD Type 1 MERGE completed using PySpark DataFrame API")
print("\n📄 This is equivalent to the SQL MERGE statement")
print("Useful for programmatic ETL pipelines and complex transformations")

# COMMAND ----------

# DBTITLE 1,SCD Type 1: Key Takeaways
# MAGIC %md
# MAGIC ### 🔑 SCD Type 1 Key Takeaways:
# MAGIC
# MAGIC #### ✅ Advantages:
# MAGIC * **Simple Logic**: Easy to implement and understand
# MAGIC * **Low Storage**: No duplicate rows for history
# MAGIC * **Fast Queries**: Always query current state
# MAGIC * **Easy Maintenance**: No complex date ranges or flags
# MAGIC
# MAGIC #### ❌ Disadvantages:
# MAGIC * **No History**: Cannot reconstruct past states
# MAGIC * **No Audit Trail**: Lost historical values
# MAGIC * **Irreversible**: Cannot undo updates
# MAGIC * **Compliance Risk**: May violate regulatory requirements
# MAGIC
# MAGIC #### 🎯 Best Practices:
# MAGIC * Use for **non-critical attributes** (email, phone)
# MAGIC * Document which columns use Type 1
# MAGIC * Consider **change data feed** for audit if needed
# MAGIC * Never use for **financial or regulatory data**
# MAGIC * Add `updated_at` timestamp for troubleshooting
# MAGIC
# MAGIC #### 🔄 Pattern Summary:
# MAGIC ```python
# MAGIC # Simple SCD Type 1 pattern:
# MAGIC MERGE INTO target
# MAGIC USING source
# MAGIC ON target.pk = source.pk
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 1: Introduction to Delta MERGE
# MAGIC %md
# MAGIC ---
# MAGIC # 🔹 SECTION 1: Introduction to Delta MERGE
# MAGIC
# MAGIC ## 🧒 ELI5 (Explain Like I'm 5):
# MAGIC Imagine you have a toy box (your database table). Sometimes you get new toys, and sometimes your existing toys change (like getting a new battery). **MERGE** is like a smart helper that:
# MAGIC * Adds new toys if they don't exist
# MAGIC * Updates existing toys if they already exist
# MAGIC * All in ONE operation!
# MAGIC
# MAGIC Without MERGE, you'd have to:
# MAGIC 1. Check if the toy exists
# MAGIC 2. If yes, update it
# MAGIC 3. If no, add it
# MAGIC
# MAGIC MERGE does all this automatically!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### What is Delta MERGE?
# MAGIC Delta MERGE is an **ACID-compliant upsert operation** that combines INSERT, UPDATE, and DELETE operations into a single atomic transaction. It's the cornerstone of incremental data processing in Delta Lake.
# MAGIC
# MAGIC ### Why MERGE is Critical:
# MAGIC
# MAGIC **❌ Without MERGE (Traditional Approach):**
# MAGIC ```python
# MAGIC # Inefficient: Overwrite entire table
# MAGIC df.write.mode("overwrite").saveAsTable("target_table")  # Loses incremental benefits
# MAGIC
# MAGIC # Or complex multi-step process:
# MAGIC # 1. Read existing data
# MAGIC # 2. Join with new data
# MAGIC # 3. Apply logic
# MAGIC # 4. Overwrite entire table
# MAGIC ```
# MAGIC
# MAGIC **✅ With MERGE (Modern Approach):**
# MAGIC ```sql
# MAGIC MERGE INTO target
# MAGIC USING source
# MAGIC ON target.id = source.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ```
# MAGIC
# MAGIC ### Key Benefits:
# MAGIC 1. **Atomicity**: All-or-nothing operation (ACID compliant)
# MAGIC 2. **Performance**: Updates only changed rows (not entire table)
# MAGIC 3. **Simplicity**: Single operation vs multi-step logic
# MAGIC 4. **Schema Evolution**: Handles schema changes gracefully
# MAGIC 5. **Concurrency**: Safe for multiple concurrent writers
# MAGIC
# MAGIC ### Core Syntax:
# MAGIC ```sql
# MAGIC MERGE INTO <target_table> [AS target_alias]
# MAGIC USING <source_table|query> [AS source_alias]
# MAGIC ON <merge_condition>
# MAGIC WHEN MATCHED [AND <condition>] THEN
# MAGIC   UPDATE SET <column1> = <value1>, <column2> = <value2>, ...
# MAGIC WHEN NOT MATCHED [AND <condition>] THEN
# MAGIC   INSERT (<column_list>) VALUES (<value_list>)
# MAGIC WHEN NOT MATCHED BY SOURCE [AND <condition>] THEN
# MAGIC   DELETE
# MAGIC ```
# MAGIC
# MAGIC ### Use Cases:
# MAGIC * **CDC (Change Data Capture)**: Apply inserts/updates/deletes from source systems
# MAGIC * **Slowly Changing Dimensions (SCD)**: Track historical changes
# MAGIC * **Incremental ETL**: Efficient data pipeline updates
# MAGIC * **Data Quality**: Deduplication and cleanup
# MAGIC * **Real-time Analytics**: Streaming upserts

# COMMAND ----------

# DBTITLE 1,Setup: Create Catalog and Schema
# Create catalog and schema for our examples
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define catalog and schema
catalog_name = "tr_ravee_training"
schema_name = "delta_merge_demo"

# Create catalog if not exists
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")

# Create schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# Set current catalog and schema
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

print(f"✅ Using catalog: {catalog_name}")
print(f"✅ Using schema: {schema_name}")
print(f"\n📌 Full namespace: {catalog_name}.{schema_name}")

# COMMAND ----------

# DBTITLE 1,Demo: Basic MERGE Operation
# Create initial target table
target_data = [
    (1, "Alice", "Engineering", 95000),
    (2, "Bob", "Sales", 75000),
    (3, "Carol", "Marketing", 80000)
]

target_schema = StructType([
    StructField("emp_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True)
])

target_df = spark.createDataFrame(target_data, target_schema)
target_df.write.format("delta").mode("overwrite").saveAsTable("employees_target")

print("📋 Initial Target Table:")
display(spark.table("employees_target"))

# COMMAND ----------

# DBTITLE 1,Demo: Create Source Data with Changes
# Create source data with:
# - Updated record (emp_id=2: Bob got promoted, salary increased)
# - New record (emp_id=4: David is new hire)
# - Unchanged record (emp_id=1: Alice - no change)

source_data = [
    (1, "Alice", "Engineering", 95000),  # No change
    (2, "Bob", "Sales", 85000),          # Salary updated
    (4, "David", "HR", 70000)            # New employee
]

source_df = spark.createDataFrame(source_data, target_schema)

# Create temp view for SQL MERGE
source_df.createOrReplaceTempView("employees_source")

print("🔄 Source Data (with changes):")
display(source_df)

# COMMAND ----------

# DBTITLE 1,Execute: Basic MERGE Operation
# MAGIC %sql
# MAGIC -- Perform MERGE operation
# MAGIC MERGE INTO employees_target AS target
# MAGIC USING employees_source AS source
# MAGIC ON target.emp_id = source.emp_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.name = source.name,
# MAGIC     target.department = source.department,
# MAGIC     target.salary = source.salary
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (emp_id, name, department, salary)
# MAGIC   VALUES (source.emp_id, source.name, source.department, source.salary)

# COMMAND ----------

# DBTITLE 1,Verify: MERGE Results
# Display the merged result
print("✅ After MERGE Operation:")
print("\nChanges applied:")
print("  • emp_id=1 (Alice): No change")
print("  • emp_id=2 (Bob): Salary updated 75000 → 85000")
print("  • emp_id=4 (David): New record inserted")
print("  • emp_id=3 (Carol): Unchanged (not in source)\n")

display(spark.table("employees_target").orderBy("emp_id"))