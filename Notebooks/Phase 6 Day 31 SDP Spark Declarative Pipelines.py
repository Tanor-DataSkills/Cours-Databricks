# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # 🏗 Data Engineering Training — Phase 6 Day 31  
# MAGIC ## ⚡ Spark Declarative Pipelines (SDP): Automation & Governance  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - Spark Declarative Pipelines (SDP)  
# MAGIC - Constraints & Available Options  
# MAGIC - Pipeline Automation  
# MAGIC - Medallion Integration  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### ⚙️ Platform: Databricks (Serverless + Unity Catalog + Delta Lake + SDP)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Objective:
# MAGIC Understand how to build declarative data pipelines using SDP, enforce constraints, and automate pipeline execution in a governed Lakehouse.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Engineering Constraints (MANDATORY):
# MAGIC
# MAGIC ✅ Use Databricks Serverless Compute  
# MAGIC ❌ DO NOT use RDDs (DataFrame API only)  
# MAGIC ❌ DO NOT use cache() / persist()  
# MAGIC ❌ DO NOT use /tmp or local storage  
# MAGIC ✅ Use Unity Catalog managed tables  
# MAGIC ✅ Follow declarative and Medallion-first design  
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 1: What is SDP?
# MAGIC %md
# MAGIC # 📖 SECTION 1: What is Spark Declarative Pipelines (SDP)?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine you want a pizza:
# MAGIC
# MAGIC * **Imperative Way (Old)**: You tell the chef step-by-step — "Get flour, add water, knead dough, spread sauce, add cheese, bake for 15 minutes"
# MAGIC * **Declarative Way (SDP)**: You just say "I want a Margherita pizza" and the chef figures out all the steps automatically
# MAGIC
# MAGIC **SDP is like ordering pizza — you declare WHAT you want, not HOW to make it!**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### Evolution Timeline:
# MAGIC ```
# MAGIC Delta Live Tables (DLT) → Spark Declarative Pipelines (SDP)
# MAGIC ```
# MAGIC
# MAGIC **Spark Declarative Pipelines (SDP)** is Databricks' next-generation framework for building **declarative, self-optimizing data pipelines**.
# MAGIC
# MAGIC ### Key Characteristics:
# MAGIC
# MAGIC 1. **Declarative Paradigm**:
# MAGIC    * Define WHAT data transformations you need
# MAGIC    * System determines HOW to execute them efficiently
# MAGIC    * Automatic dependency resolution
# MAGIC
# MAGIC 2. **Auto-Managed Execution**:
# MAGIC    * No manual orchestration required
# MAGIC    * Built-in retry logic
# MAGIC    * Automatic scaling
# MAGIC
# MAGIC 3. **Built-in Data Quality**:
# MAGIC    * Constraints and expectations
# MAGIC    * Data validation at pipeline level
# MAGIC    * Quarantine bad records automatically
# MAGIC
# MAGIC 4. **Unity Catalog Native**:
# MAGIC    * Automatic lineage tracking
# MAGIC    * Governance integration
# MAGIC    * Managed tables by default
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🆚️ Imperative vs Declarative:
# MAGIC
# MAGIC | Aspect | Imperative Pipelines | Declarative Pipelines (SDP) |
# MAGIC |--------|---------------------|-----------------------------|
# MAGIC | **Focus** | HOW to execute | WHAT to achieve |
# MAGIC | **Code Style** | Step-by-step instructions | Desired end state |
# MAGIC | **Dependencies** | Manual management | Auto-resolved |
# MAGIC | **Orchestration** | Explicit (Workflows/Jobs) | Built-in |
# MAGIC | **Data Quality** | Manual validation | Built-in constraints |
# MAGIC | **Optimization** | Manual tuning | Auto-optimized |
# MAGIC | **Example** | `df.filter().join().write()` | `CREATE LIVE TABLE AS SELECT...` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Why SDP?
# MAGIC
# MAGIC ✅ **Less Code**: Focus on business logic, not infrastructure  
# MAGIC ✅ **Auto-Optimization**: System handles performance tuning  
# MAGIC ✅ **Built-in Quality**: Data validation at pipeline level  
# MAGIC ✅ **Auto-Lineage**: Full traceability out-of-the-box  
# MAGIC ✅ **Governance**: Unity Catalog integration  
# MAGIC ✅ **Reduced Ops**: No manual orchestration needed  
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 2: Declarative Pipeline Concepts
# MAGIC %md
# MAGIC # 🧠 SECTION 2: Declarative Pipeline Concepts
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔑 Core Principle: Define WHAT, Not HOW
# MAGIC
# MAGIC ### Traditional Imperative Approach:
# MAGIC ```python
# MAGIC # Step 1: Read source
# MAGIC df_source = spark.read.table("source_table")
# MAGIC
# MAGIC # Step 2: Apply transformations
# MAGIC df_clean = df_source.filter(col("value").isNotNull())
# MAGIC
# MAGIC # Step 3: Write to target
# MAGIC df_clean.write.mode("overwrite").saveAsTable("target_table")
# MAGIC
# MAGIC # Step 4: Manually schedule this script
# MAGIC # Step 5: Handle retries manually
# MAGIC # Step 6: Track lineage manually
# MAGIC ```
# MAGIC
# MAGIC ### Declarative Approach (SDP):
# MAGIC ```sql
# MAGIC -- Just declare what you want!
# MAGIC CREATE OR REFRESH LIVE TABLE target_table
# MAGIC AS SELECT * 
# MAGIC FROM source_table 
# MAGIC WHERE value IS NOT NULL;
# MAGIC
# MAGIC -- System handles:
# MAGIC -- ✓ Execution scheduling
# MAGIC -- ✓ Dependency resolution
# MAGIC -- ✓ Retries on failure
# MAGIC -- ✓ Lineage tracking
# MAGIC -- ✓ Performance optimization
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔗 Automatic Dependency Resolution
# MAGIC
# MAGIC SDP automatically determines the execution order based on table dependencies:
# MAGIC
# MAGIC ```
# MAGIC 🟢 Bronze (Source Data)
# MAGIC     ↓
# MAGIC 🟡 Silver (Cleaned & Enriched)
# MAGIC     ↓
# MAGIC 🟠 Gold (Business Aggregates)
# MAGIC ```
# MAGIC
# MAGIC **You just define all three layers — SDP figures out the execution order!**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Pipeline Execution Model:
# MAGIC
# MAGIC ### 1. **LIVE TABLES** (Materialized Views):
# MAGIC    * Always reflect the latest state
# MAGIC    * Automatically refresh when upstream changes
# MAGIC    * Stored as Delta tables
# MAGIC
# MAGIC ### 2. **STREAMING TABLES** (Continuous Processing):
# MAGIC    * Process data incrementally as it arrives
# MAGIC    * Exactly-once processing guarantees
# MAGIC    * Automatic checkpointing
# MAGIC
# MAGIC ### 3. **VIEWS** (Non-Materialized):
# MAGIC    * Query-time evaluation
# MAGIC    * No storage overhead
# MAGIC    * Good for simple transformations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Key Concepts:
# MAGIC
# MAGIC ### 1. **Declarative SQL**:
# MAGIC ```sql
# MAGIC CREATE OR REFRESH LIVE TABLE <table_name>
# MAGIC AS <query>
# MAGIC ```
# MAGIC
# MAGIC ### 2. **Automatic Refresh**:
# MAGIC * System detects upstream changes
# MAGIC * Triggers downstream refreshes automatically
# MAGIC * No manual scheduling needed
# MAGIC
# MAGIC ### 3. **Incremental Processing**:
# MAGIC * Only process new/changed data
# MAGIC * Automatic state management
# MAGIC * Cost-efficient
# MAGIC
# MAGIC ### 4. **Built-in Expectations**:
# MAGIC * Data quality checks at pipeline level
# MAGIC * Fail fast or quarantine bad data
# MAGIC * Automatic monitoring
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 3: Creating Declarative Tables
# MAGIC %md
# MAGIC # 🛠️ SECTION 3: Creating Declarative Tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Basic Syntax:
# MAGIC
# MAGIC ### 1. **CREATE LIVE TABLE** (Materialized):
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH LIVE TABLE bronze_customers
# MAGIC COMMENT "Raw customer data from source system"
# MAGIC AS SELECT 
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   email,
# MAGIC   signup_date,
# MAGIC   current_timestamp() as ingestion_time
# MAGIC FROM cloud_files(
# MAGIC   '/mnt/source/customers',
# MAGIC   'json',
# MAGIC   map('cloudFiles.inferColumnTypes', 'true')
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC **What happens here?**
# MAGIC * ✓ Auto-creates Delta table in Unity Catalog
# MAGIC * ✓ Auto-discovers schema
# MAGIC * ✓ Auto-handles new files
# MAGIC * ✓ Auto-tracks lineage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. **CREATE STREAMING TABLE** (Continuous):
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE bronze_events
# MAGIC COMMENT "Streaming events from Kafka"
# MAGIC AS SELECT 
# MAGIC   event_id,
# MAGIC   event_type,
# MAGIC   event_timestamp,
# MAGIC   payload
# MAGIC FROM cloud_files(
# MAGIC   '/mnt/streaming/events',
# MAGIC   'json'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC **Streaming vs Live:**
# MAGIC * **STREAMING TABLE**: Processes data incrementally, maintains checkpoints
# MAGIC * **LIVE TABLE**: Full refresh or incremental based on system optimization
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. **CREATE VIEW** (Non-Materialized):
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH LIVE VIEW customer_summary
# MAGIC AS SELECT 
# MAGIC   DATE(signup_date) as signup_day,
# MAGIC   COUNT(*) as customer_count
# MAGIC FROM LIVE.bronze_customers
# MAGIC GROUP BY DATE(signup_date);
# MAGIC ```
# MAGIC
# MAGIC **When to use VIEW:**
# MAGIC * Simple transformations
# MAGIC * No need to store results
# MAGIC * Query-time freshness required
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Referencing Other Tables:
# MAGIC
# MAGIC To reference other live tables in the same pipeline:
# MAGIC
# MAGIC ```sql
# MAGIC -- Use LIVE schema prefix
# MAGIC SELECT * FROM LIVE.bronze_customers
# MAGIC
# MAGIC -- Or use STREAM() for streaming reads
# MAGIC SELECT * FROM STREAM(LIVE.bronze_events)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚙️ Table Properties:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH LIVE TABLE silver_customers
# MAGIC COMMENT "Cleaned and validated customer data"
# MAGIC TBLPROPERTIES (
# MAGIC   'quality' = 'silver',
# MAGIC   'pipelines.autoOptimize.managed' = 'true'
# MAGIC )
# MAGIC AS SELECT 
# MAGIC   customer_id,
# MAGIC   UPPER(TRIM(customer_name)) as customer_name,
# MAGIC   LOWER(TRIM(email)) as email,
# MAGIC   signup_date
# MAGIC FROM LIVE.bronze_customers;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Auto-Managed Execution:
# MAGIC
# MAGIC **You DON'T need to:**
# MAGIC * Write explicit `.write()` commands
# MAGIC * Manage checkpoints manually
# MAGIC * Handle schema evolution
# MAGIC * Schedule execution
# MAGIC * Implement retry logic
# MAGIC
# MAGIC **SDP handles ALL of this automatically!**
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 4: Constraints & Expectations
# MAGIC %md
# MAGIC # ✅ SECTION 4: Constraints & Expectations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔍 What are Expectations?
# MAGIC
# MAGIC Expectations are **data quality rules** that you declare at the pipeline level. They validate data as it flows through your pipeline.
# MAGIC
# MAGIC ### Three Enforcement Modes:
# MAGIC
# MAGIC 1. **WARN** (⚠️): Log violations but allow data to pass
# MAGIC 2. **DROP** (🗑️): Filter out invalid records
# MAGIC 3. **FAIL** (❌): Stop pipeline execution on violations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Constraint Syntax:
# MAGIC
# MAGIC ### 1. **Basic EXPECT (WARN)**:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH LIVE TABLE silver_orders
# MAGIC (
# MAGIC   CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL),
# MAGIC   CONSTRAINT positive_amount EXPECT (order_amount > 0)
# MAGIC )
# MAGIC COMMENT "Orders with basic validation"
# MAGIC AS SELECT 
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_amount,
# MAGIC   order_date
# MAGIC FROM LIVE.bronze_orders;
# MAGIC ```
# MAGIC
# MAGIC **Behavior**: Violations are logged in metrics, but data passes through.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. **EXPECT ... ON VIOLATION DROP ROW** (🗑️):
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH LIVE TABLE silver_customers_clean
# MAGIC (
# MAGIC   CONSTRAINT valid_email 
# MAGIC     EXPECT (email IS NOT NULL AND email LIKE '%@%') 
# MAGIC     ON VIOLATION DROP ROW,
# MAGIC   
# MAGIC   CONSTRAINT valid_age 
# MAGIC     EXPECT (age BETWEEN 0 AND 120) 
# MAGIC     ON VIOLATION DROP ROW
# MAGIC )
# MAGIC COMMENT "Only valid customers pass through"
# MAGIC AS SELECT 
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   email,
# MAGIC   age
# MAGIC FROM LIVE.bronze_customers;
# MAGIC ```
# MAGIC
# MAGIC **Behavior**: Invalid records are filtered out, valid records continue.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. **EXPECT ... ON VIOLATION FAIL UPDATE** (❌):
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH LIVE TABLE gold_revenue_critical
# MAGIC (
# MAGIC   CONSTRAINT no_null_revenue 
# MAGIC     EXPECT (revenue IS NOT NULL) 
# MAGIC     ON VIOLATION FAIL UPDATE,
# MAGIC   
# MAGIC   CONSTRAINT revenue_in_range 
# MAGIC     EXPECT (revenue >= 0 AND revenue <= 1000000000) 
# MAGIC     ON VIOLATION FAIL UPDATE
# MAGIC )
# MAGIC COMMENT "Critical revenue data - must be 100% valid"
# MAGIC AS SELECT 
# MAGIC   DATE(order_date) as date,
# MAGIC   SUM(order_amount) as revenue
# MAGIC FROM LIVE.silver_orders
# MAGIC GROUP BY DATE(order_date);
# MAGIC ```
# MAGIC
# MAGIC **Behavior**: Pipeline execution fails if ANY violation occurs.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Multiple Constraints:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH LIVE TABLE silver_transactions
# MAGIC (
# MAGIC   -- Critical constraints (FAIL)
# MAGIC   CONSTRAINT valid_transaction_id 
# MAGIC     EXPECT (transaction_id IS NOT NULL) 
# MAGIC     ON VIOLATION FAIL UPDATE,
# MAGIC   
# MAGIC   -- Filter bad data (DROP)
# MAGIC   CONSTRAINT valid_amount 
# MAGIC     EXPECT (amount > 0) 
# MAGIC     ON VIOLATION DROP ROW,
# MAGIC   
# MAGIC   CONSTRAINT valid_currency 
# MAGIC     EXPECT (currency IN ('USD', 'EUR', 'GBP')) 
# MAGIC     ON VIOLATION DROP ROW,
# MAGIC   
# MAGIC   -- Monitor anomalies (WARN)
# MAGIC   CONSTRAINT amount_reasonable 
# MAGIC     EXPECT (amount <= 1000000)
# MAGIC )
# MAGIC AS SELECT * FROM LIVE.bronze_transactions;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 When to Use Each Mode:
# MAGIC
# MAGIC | Mode | Use Case | Example |
# MAGIC |------|----------|----------|
# MAGIC | **WARN** | Anomaly detection | Unusually high values |
# MAGIC | **DROP ROW** | Cleansing pipelines | Invalid email formats |
# MAGIC | **FAIL UPDATE** | Critical business data | Financial reports |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Quarantine Pattern:
# MAGIC
# MAGIC You can create separate tables for invalid records:
# MAGIC
# MAGIC ```sql
# MAGIC -- Valid records
# MAGIC CREATE OR REFRESH LIVE TABLE silver_valid_orders
# MAGIC (
# MAGIC   CONSTRAINT valid_data 
# MAGIC     EXPECT (order_id IS NOT NULL AND amount > 0) 
# MAGIC     ON VIOLATION DROP ROW
# MAGIC )
# MAGIC AS SELECT * FROM LIVE.bronze_orders;
# MAGIC
# MAGIC -- Invalid records (quarantine)
# MAGIC CREATE OR REFRESH LIVE TABLE quarantine_orders
# MAGIC AS SELECT * 
# MAGIC FROM LIVE.bronze_orders
# MAGIC WHERE order_id IS NULL OR amount <= 0;
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 5: Pipeline Automation
# MAGIC %md
# MAGIC # ⏱️ SECTION 5: Pipeline Automation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🤖 What Gets Automated?
# MAGIC
# MAGIC When you define SDP pipelines, Databricks automatically handles:
# MAGIC
# MAGIC ### 1. **Execution Orchestration**:
# MAGIC * ✓ Determines optimal execution order
# MAGIC * ✓ Parallelizes independent tasks
# MAGIC * ✓ Handles dependencies automatically
# MAGIC
# MAGIC ### 2. **State Management**:
# MAGIC * ✓ Tracks processed data (checkpoints)
# MAGIC * ✓ Resumes from last successful state
# MAGIC * ✓ Handles incremental updates
# MAGIC
# MAGIC ### 3. **Error Handling**:
# MAGIC * ✓ Automatic retries on transient failures
# MAGIC * ✓ Detailed error logs
# MAGIC * ✓ Graceful degradation
# MAGIC
# MAGIC ### 4. **Performance Optimization**:
# MAGIC * ✓ Auto-scaling compute resources
# MAGIC * ✓ Intelligent caching
# MAGIC * ✓ Adaptive query execution
# MAGIC
# MAGIC ### 5. **Monitoring & Observability**:
# MAGIC * ✓ Pipeline execution metrics
# MAGIC * ✓ Data quality metrics
# MAGIC * ✓ Lineage visualization
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📅 Scheduling Options:
# MAGIC
# MAGIC ### 1. **Triggered (On-Demand)**:
# MAGIC * Manual execution via UI/API
# MAGIC * Good for development/testing
# MAGIC
# MAGIC ### 2. **Scheduled (Cron)**:
# MAGIC * Run at specific times/intervals
# MAGIC * Good for batch processing
# MAGIC
# MAGIC ```
# MAGIC Schedule Examples:
# MAGIC - Every hour: 0 0 * * * *
# MAGIC - Daily at 2 AM: 0 0 2 * * *
# MAGIC - Every 15 minutes: 0 */15 * * * *
# MAGIC ```
# MAGIC
# MAGIC ### 3. **Continuous (Streaming)**:
# MAGIC * Always running, processing new data
# MAGIC * Good for real-time pipelines
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Automatic Dependency Resolution:
# MAGIC
# MAGIC ```
# MAGIC Pipeline Definition:
# MAGIC
# MAGIC 🟢 bronze_sales (from cloud_files)
# MAGIC 🟡 silver_sales (from bronze_sales)
# MAGIC 🟠 gold_daily_revenue (from silver_sales)
# MAGIC 🟠 gold_monthly_revenue (from silver_sales)
# MAGIC ```
# MAGIC
# MAGIC **Execution Flow** (determined automatically):
# MAGIC ```
# MAGIC 1. bronze_sales (no dependencies) → Runs first
# MAGIC 2. silver_sales (depends on bronze) → Runs after bronze
# MAGIC 3. gold_daily_revenue & gold_monthly_revenue (both depend on silver) → Run in parallel
# MAGIC ```
# MAGIC
# MAGIC **You don't specify execution order — SDP figures it out!**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛡️ Built-in Resilience:
# MAGIC
# MAGIC ### Automatic Handling:
# MAGIC
# MAGIC 1. **Transient Failures**:
# MAGIC    * Network timeouts → Auto-retry
# MAGIC    * Cluster issues → Auto-restart
# MAGIC    * Resource contention → Auto-queue
# MAGIC
# MAGIC 2. **Data Quality Issues**:
# MAGIC    * Use expectations to handle
# MAGIC    * Quarantine or drop bad records
# MAGIC    * Continue processing valid data
# MAGIC
# MAGIC 3. **Schema Evolution**:
# MAGIC    * Auto-detect new columns
# MAGIC    * Handle type changes gracefully
# MAGIC    * Maintain compatibility
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚡ Reduced Manual Effort:
# MAGIC
# MAGIC ### Traditional Approach:
# MAGIC ```python
# MAGIC # You manually write:
# MAGIC - Job scheduling code
# MAGIC - Dependency management
# MAGIC - Checkpoint handling
# MAGIC - Error recovery logic
# MAGIC - Performance tuning
# MAGIC - Monitoring setup
# MAGIC
# MAGIC Total: 500+ lines of orchestration code
# MAGIC ```
# MAGIC
# MAGIC ### SDP Approach:
# MAGIC ```sql
# MAGIC -- Just declare your transformations:
# MAGIC CREATE LIVE TABLE bronze_data AS SELECT * FROM source;
# MAGIC CREATE LIVE TABLE silver_data AS SELECT * FROM LIVE.bronze_data;
# MAGIC CREATE LIVE TABLE gold_data AS SELECT * FROM LIVE.silver_data;
# MAGIC
# MAGIC -- System handles everything else!
# MAGIC Total: 15 lines of business logic
# MAGIC ```
# MAGIC
# MAGIC 💡 **97% less orchestration code!**
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 6: Medallion Architecture with SDP
# MAGIC %md
# MAGIC # 🏅 SECTION 6: Medallion Architecture with SDP
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Medallion Pattern Recap:
# MAGIC
# MAGIC ```
# MAGIC 🟢 BRONZE (Raw Data)
# MAGIC    ↓
# MAGIC 🟡 SILVER (Cleaned & Validated)
# MAGIC    ↓
# MAGIC 🟠 GOLD (Business Aggregates)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔗 SDP + Medallion = Perfect Match
# MAGIC
# MAGIC SDP is **designed** for Medallion architecture:
# MAGIC
# MAGIC * **Automatic dependency flow**: Bronze → Silver → Gold
# MAGIC * **Built-in quality gates**: Expectations at each layer
# MAGIC * **Incremental processing**: Only process changed data
# MAGIC * **Clear layer separation**: Enforced by table structure
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Complete Medallion Pipeline Example:
# MAGIC
# MAGIC ### 🟢 **BRONZE Layer** (Raw Ingestion):
# MAGIC
# MAGIC ```sql
# MAGIC -- Ingest raw data with minimal transformation
# MAGIC CREATE OR REFRESH STREAMING TABLE bronze_customer_events
# MAGIC COMMENT "Raw customer events from source system"
# MAGIC TBLPROPERTIES ('quality' = 'bronze')
# MAGIC AS SELECT 
# MAGIC   *,
# MAGIC   current_timestamp() as ingestion_timestamp,
# MAGIC   input_file_name() as source_file
# MAGIC FROM cloud_files(
# MAGIC   '/mnt/raw/customer_events',
# MAGIC   'json',
# MAGIC   map(
# MAGIC     'cloudFiles.inferColumnTypes', 'true',
# MAGIC     'cloudFiles.schemaHints', 'event_timestamp TIMESTAMP'
# MAGIC   )
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC **Bronze Characteristics:**
# MAGIC * ✓ Preserves raw data exactly as received
# MAGIC * ✓ Adds audit columns (ingestion time, source)
# MAGIC * ✓ Minimal validation
# MAGIC * ✓ Schema-on-read approach
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟡 **SILVER Layer** (Cleaned & Validated):
# MAGIC
# MAGIC ```sql
# MAGIC -- Clean, validate, and enrich data
# MAGIC CREATE OR REFRESH STREAMING TABLE silver_customer_events
# MAGIC (
# MAGIC   -- Data Quality Expectations
# MAGIC   CONSTRAINT valid_customer_id 
# MAGIC     EXPECT (customer_id IS NOT NULL) 
# MAGIC     ON VIOLATION DROP ROW,
# MAGIC   
# MAGIC   CONSTRAINT valid_event_type 
# MAGIC     EXPECT (event_type IN ('login', 'purchase', 'logout', 'view')) 
# MAGIC     ON VIOLATION DROP ROW,
# MAGIC   
# MAGIC   CONSTRAINT valid_timestamp 
# MAGIC     EXPECT (event_timestamp IS NOT NULL AND event_timestamp <= current_timestamp()) 
# MAGIC     ON VIOLATION DROP ROW
# MAGIC )
# MAGIC COMMENT "Cleaned and validated customer events"
# MAGIC TBLPROPERTIES ('quality' = 'silver')
# MAGIC AS SELECT 
# MAGIC   -- Standardize and clean fields
# MAGIC   CAST(customer_id AS BIGINT) as customer_id,
# MAGIC   LOWER(TRIM(event_type)) as event_type,
# MAGIC   event_timestamp,
# MAGIC   
# MAGIC   -- Parse nested JSON if needed
# MAGIC   event_data:product_id::STRING as product_id,
# MAGIC   event_data:amount::DECIMAL(10,2) as amount,
# MAGIC   
# MAGIC   -- Add derived fields
# MAGIC   DATE(event_timestamp) as event_date,
# MAGIC   HOUR(event_timestamp) as event_hour,
# MAGIC   
# MAGIC   -- Preserve audit trail
# MAGIC   ingestion_timestamp,
# MAGIC   source_file
# MAGIC FROM STREAM(LIVE.bronze_customer_events);
# MAGIC ```
# MAGIC
# MAGIC **Silver Characteristics:**
# MAGIC * ✓ Data cleansing (trim, case normalization)
# MAGIC * ✓ Type casting and validation
# MAGIC * ✓ Schema enforcement
# MAGIC * ✓ Derived/calculated fields
# MAGIC * ✓ Quality constraints
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟠 **GOLD Layer** (Business Aggregates):
# MAGIC
# MAGIC ```sql
# MAGIC -- Daily customer activity metrics
# MAGIC CREATE OR REFRESH LIVE TABLE gold_daily_customer_metrics
# MAGIC (
# MAGIC   CONSTRAINT positive_metrics 
# MAGIC     EXPECT (total_events > 0) 
# MAGIC     ON VIOLATION FAIL UPDATE
# MAGIC )
# MAGIC COMMENT "Daily aggregated customer activity metrics"
# MAGIC TBLPROPERTIES ('quality' = 'gold')
# MAGIC AS SELECT 
# MAGIC   event_date,
# MAGIC   customer_id,
# MAGIC   
# MAGIC   -- Event counts by type
# MAGIC   COUNT(*) as total_events,
# MAGIC   SUM(CASE WHEN event_type = 'login' THEN 1 ELSE 0 END) as login_count,
# MAGIC   SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchase_count,
# MAGIC   SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as view_count,
# MAGIC   
# MAGIC   -- Revenue metrics
# MAGIC   SUM(COALESCE(amount, 0)) as total_revenue,
# MAGIC   AVG(COALESCE(amount, 0)) as avg_transaction_value,
# MAGIC   
# MAGIC   -- Timing metrics
# MAGIC   MIN(event_timestamp) as first_event_time,
# MAGIC   MAX(event_timestamp) as last_event_time,
# MAGIC   
# MAGIC   current_timestamp() as calculated_at
# MAGIC FROM LIVE.silver_customer_events
# MAGIC GROUP BY event_date, customer_id;
# MAGIC
# MAGIC -- High-level business KPIs
# MAGIC CREATE OR REFRESH LIVE TABLE gold_daily_kpis
# MAGIC COMMENT "Daily business KPIs"
# MAGIC TBLPROPERTIES ('quality' = 'gold')
# MAGIC AS SELECT 
# MAGIC   event_date,
# MAGIC   
# MAGIC   -- User metrics
# MAGIC   COUNT(DISTINCT customer_id) as active_customers,
# MAGIC   COUNT(DISTINCT CASE WHEN purchase_count > 0 THEN customer_id END) as purchasing_customers,
# MAGIC   
# MAGIC   -- Revenue metrics
# MAGIC   SUM(total_revenue) as daily_revenue,
# MAGIC   AVG(total_revenue) as avg_revenue_per_customer,
# MAGIC   
# MAGIC   -- Engagement metrics
# MAGIC   SUM(total_events) as total_events,
# MAGIC   AVG(total_events) as avg_events_per_customer,
# MAGIC   
# MAGIC   -- Conversion rate
# MAGIC   COUNT(DISTINCT CASE WHEN purchase_count > 0 THEN customer_id END) * 100.0 / 
# MAGIC     NULLIF(COUNT(DISTINCT customer_id), 0) as conversion_rate_pct,
# MAGIC   
# MAGIC   current_timestamp() as calculated_at
# MAGIC FROM LIVE.gold_daily_customer_metrics
# MAGIC GROUP BY event_date;
# MAGIC ```
# MAGIC
# MAGIC **Gold Characteristics:**
# MAGIC * ✓ Business-level aggregations
# MAGIC * ✓ Denormalized for analytics
# MAGIC * ✓ Pre-calculated KPIs
# MAGIC * ✓ Optimized for consumption
# MAGIC * ✓ Strict quality requirements
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Medallion Best Practices with SDP:
# MAGIC
# MAGIC 1. **Bronze**: 
# MAGIC    * Use STREAMING TABLE for real-time ingestion
# MAGIC    * Preserve all source data
# MAGIC    * Minimal expectations (mostly WARN)
# MAGIC
# MAGIC 2. **Silver**: 
# MAGIC    * Use STREAMING TABLE for incremental processing
# MAGIC    * Apply data quality rules (DROP ROW)
# MAGIC    * Standardize schema
# MAGIC
# MAGIC 3. **Gold**: 
# MAGIC    * Use LIVE TABLE for aggregations
# MAGIC    * Strict expectations (FAIL UPDATE for critical metrics)
# MAGIC    * Business-friendly naming
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚡ Automatic Benefits:
# MAGIC
# MAGIC ```
# MAGIC ✓ Bronze → Silver → Gold dependency chain: AUTO-RESOLVED
# MAGIC ✓ Incremental processing at each layer: AUTO-MANAGED
# MAGIC ✓ Data quality enforcement: BUILT-IN
# MAGIC ✓ Lineage tracking: AUTO-TRACKED
# MAGIC ✓ Performance optimization: AUTO-TUNED
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 7: Hands-On SDP Pipeline
# MAGIC %md
# MAGIC # 🛠️ SECTION 7: Hands-On SDP Pipeline Demonstration
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Objective:
# MAGIC
# MAGIC Build a complete end-to-end SDP pipeline:
# MAGIC
# MAGIC 1. 🟢 **Bronze**: Ingest sample e-commerce data
# MAGIC 2. 🟡 **Silver**: Clean and validate
# MAGIC 3. 🟠 **Gold**: Create business aggregates
# MAGIC 4. ✅ **Add data quality constraints**
# MAGIC 5. ⚡ **Auto-execute pipeline**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Important Notes:
# MAGIC
# MAGIC ⚠️ **SDP Pipelines are created and executed through the Databricks Pipeline UI, NOT in notebooks.**
# MAGIC
# MAGIC This section provides:
# MAGIC * **Conceptual understanding** of SDP syntax
# MAGIC * **Example pipeline definitions** you can use
# MAGIC * **Sample data generation** for testing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚨 To Actually Create an SDP Pipeline:
# MAGIC
# MAGIC 1. **Navigate to**: Databricks UI → Workflows → Lakeflow Pipelines → Create Pipeline
# MAGIC 2. **Create a notebook** with your SDP definitions (using the syntax below)
# MAGIC 3. **Configure the pipeline** to use that notebook
# MAGIC 4. **Run the pipeline** from the Pipeline UI
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Let's Prepare Sample Data First:
# MAGIC
# MAGIC We'll create sample data that simulates an e-commerce system.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Setup: Generate Sample E-Commerce Data
# Generate sample e-commerce data for SDP pipeline demonstration
import random
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Create sample customer events
events_data = []
base_date = datetime(2026, 4, 1)

for i in range(1000):
    event = {
        'event_id': i + 1,
        'customer_id': random.randint(1, 100),
        'event_type': random.choice(['login', 'view', 'purchase', 'logout']),
        'product_id': f'PROD_{random.randint(1, 50):03d}',
        'amount': round(random.uniform(10, 500), 2) if random.random() > 0.7 else None,
        'event_timestamp': base_date + timedelta(
            days=random.randint(0, 20),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        ),
        'user_agent': random.choice(['mobile', 'web', 'tablet'])
    }
    events_data.append(event)

# Create DataFrame
df_events = spark.createDataFrame(events_data)

# Add some invalid records for data quality demonstration
invalid_data = [
    {'event_id': None, 'customer_id': 101, 'event_type': 'login', 'event_timestamp': base_date, 'product_id': None, 'amount': None, 'user_agent': 'web'},
    {'event_id': 1001, 'customer_id': None, 'event_type': 'purchase', 'event_timestamp': base_date, 'product_id': 'PROD_001', 'amount': 100.0, 'user_agent': 'mobile'},
    {'event_id': 1002, 'customer_id': 102, 'event_type': 'invalid_event', 'event_timestamp': base_date, 'product_id': 'PROD_002', 'amount': 50.0, 'user_agent': 'tablet'},
]

df_invalid = spark.createDataFrame(invalid_data)
df_all_events = df_events.union(df_invalid.select(df_events.columns))

# Save to Unity Catalog table (simulating source data)
catalog_name = "main"  # Using default catalog
schema_name = "default"  # Using default schema

# Write sample source data
df_all_events.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.source_customer_events")

print(f"✅ Sample data created: {catalog_name}.{schema_name}.source_customer_events")
print(f"   Total records: {df_all_events.count()}")
print(f"   Valid records: {df_events.count()}")
print(f"   Invalid records: {len(invalid_data)}")

display(df_all_events.limit(10))

# COMMAND ----------

# DBTITLE 1,SDP Pipeline Definition Examples
# MAGIC %md
# MAGIC # 📝 SDP Pipeline Definition (For Pipeline Notebook)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Complete Pipeline Definition:
# MAGIC
# MAGIC Below are the SDP definitions you would put in a **dedicated pipeline notebook**:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟢 BRONZE Layer Definition:
# MAGIC
# MAGIC ```python
# MAGIC import dlt
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC @dlt.table(
# MAGIC     name="bronze_customer_events",
# MAGIC     comment="Raw customer events from source system",
# MAGIC     table_properties={"quality": "bronze"}
# MAGIC )
# MAGIC def bronze_customer_events():
# MAGIC     return (
# MAGIC         spark.readStream
# MAGIC         .format("delta")
# MAGIC         .table("main.default.source_customer_events")
# MAGIC         .withColumn("ingestion_timestamp", F.current_timestamp())
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC **Or in SQL:**
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE bronze_customer_events
# MAGIC COMMENT "Raw customer events from source system"
# MAGIC TBLPROPERTIES ('quality' = 'bronze')
# MAGIC AS SELECT 
# MAGIC   *,
# MAGIC   current_timestamp() as ingestion_timestamp
# MAGIC FROM stream(main.default.source_customer_events);
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟡 SILVER Layer Definition:
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table(
# MAGIC     name="silver_customer_events",
# MAGIC     comment="Cleaned and validated customer events",
# MAGIC     table_properties={"quality": "silver"}
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
# MAGIC @dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
# MAGIC @dlt.expect_or_drop("valid_event_type", "event_type IN ('login', 'view', 'purchase', 'logout')")
# MAGIC @dlt.expect_or_drop("valid_timestamp", "event_timestamp IS NOT NULL")
# MAGIC def silver_customer_events():
# MAGIC     return (
# MAGIC         dlt.read_stream("bronze_customer_events")
# MAGIC         .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
# MAGIC         .withColumn("event_date", F.to_date(F.col("event_timestamp")))
# MAGIC         .withColumn("event_hour", F.hour(F.col("event_timestamp")))
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC **Or in SQL:**
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE silver_customer_events
# MAGIC (
# MAGIC   CONSTRAINT valid_event_id EXPECT (event_id IS NOT NULL) ON VIOLATION DROP ROW,
# MAGIC   CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
# MAGIC   CONSTRAINT valid_event_type EXPECT (event_type IN ('login', 'view', 'purchase', 'logout')) ON VIOLATION DROP ROW,
# MAGIC   CONSTRAINT valid_timestamp EXPECT (event_timestamp IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC COMMENT "Cleaned and validated customer events"
# MAGIC TBLPROPERTIES ('quality' = 'silver')
# MAGIC AS SELECT 
# MAGIC   event_id,
# MAGIC   customer_id,
# MAGIC   LOWER(TRIM(event_type)) as event_type,
# MAGIC   product_id,
# MAGIC   amount,
# MAGIC   event_timestamp,
# MAGIC   DATE(event_timestamp) as event_date,
# MAGIC   HOUR(event_timestamp) as event_hour,
# MAGIC   user_agent,
# MAGIC   ingestion_timestamp
# MAGIC FROM STREAM(LIVE.bronze_customer_events);
# MAGIC ```
# MAGIC ---
# MAGIC
# MAGIC ### 🟠 GOLD Layer Definitions:
# MAGIC
# MAGIC ```python
# MAGIC # Daily customer metrics
# MAGIC @dlt.table(
# MAGIC     name="gold_daily_customer_metrics",
# MAGIC     comment="Daily aggregated customer activity",
# MAGIC     table_properties={"quality": "gold"}
# MAGIC )
# MAGIC @dlt.expect_or_fail("has_activity", "total_events > 0")
# MAGIC def gold_daily_customer_metrics():
# MAGIC     return (
# MAGIC         dlt.read("silver_customer_events")
# MAGIC         .groupBy("event_date", "customer_id")
# MAGIC         .agg(
# MAGIC             F.count("*").alias("total_events"),
# MAGIC             F.sum(F.when(F.col("event_type") == "login", 1).otherwise(0)).alias("login_count"),
# MAGIC             F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
# MAGIC             F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
# MAGIC             F.sum(F.coalesce(F.col("amount"), F.lit(0))).alias("total_revenue"),
# MAGIC             F.avg(F.coalesce(F.col("amount"), F.lit(0))).alias("avg_transaction_value"),
# MAGIC             F.min("event_timestamp").alias("first_event_time"),
# MAGIC             F.max("event_timestamp").alias("last_event_time")
# MAGIC         )
# MAGIC         .withColumn("calculated_at", F.current_timestamp())
# MAGIC     )
# MAGIC
# MAGIC # Daily business KPIs
# MAGIC @dlt.table(
# MAGIC     name="gold_daily_kpis",
# MAGIC     comment="Daily business KPIs",
# MAGIC     table_properties={"quality": "gold"}
# MAGIC )
# MAGIC def gold_daily_kpis():
# MAGIC     return (
# MAGIC         dlt.read("gold_daily_customer_metrics")
# MAGIC         .groupBy("event_date")
# MAGIC         .agg(
# MAGIC             F.countDistinct("customer_id").alias("active_customers"),
# MAGIC             F.sum(F.when(F.col("purchase_count") > 0, 1).otherwise(0)).alias("purchasing_customers"),
# MAGIC             F.sum("total_revenue").alias("daily_revenue"),
# MAGIC             F.avg("total_revenue").alias("avg_revenue_per_customer"),
# MAGIC             F.sum("total_events").alias("total_events"),
# MAGIC             F.avg("total_events").alias("avg_events_per_customer")
# MAGIC         )
# MAGIC         .withColumn(
# MAGIC             "conversion_rate_pct",
# MAGIC             (F.col("purchasing_customers") * 100.0) / F.col("active_customers")
# MAGIC         )
# MAGIC         .withColumn("calculated_at", F.current_timestamp())
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC **Or in SQL:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Daily customer metrics
# MAGIC CREATE OR REFRESH LIVE TABLE gold_daily_customer_metrics
# MAGIC (
# MAGIC   CONSTRAINT has_activity EXPECT (total_events > 0) ON VIOLATION FAIL UPDATE
# MAGIC )
# MAGIC COMMENT "Daily aggregated customer activity"
# MAGIC TBLPROPERTIES ('quality' = 'gold')
# MAGIC AS SELECT 
# MAGIC   event_date,
# MAGIC   customer_id,
# MAGIC   COUNT(*) as total_events,
# MAGIC   SUM(CASE WHEN event_type = 'login' THEN 1 ELSE 0 END) as login_count,
# MAGIC   SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchase_count,
# MAGIC   SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as view_count,
# MAGIC   SUM(COALESCE(amount, 0)) as total_revenue,
# MAGIC   AVG(COALESCE(amount, 0)) as avg_transaction_value,
# MAGIC   MIN(event_timestamp) as first_event_time,
# MAGIC   MAX(event_timestamp) as last_event_time,
# MAGIC   current_timestamp() as calculated_at
# MAGIC FROM LIVE.silver_customer_events
# MAGIC GROUP BY event_date, customer_id;
# MAGIC
# MAGIC -- Daily KPIs
# MAGIC CREATE OR REFRESH LIVE TABLE gold_daily_kpis
# MAGIC COMMENT "Daily business KPIs"
# MAGIC TBLPROPERTIES ('quality' = 'gold')
# MAGIC AS SELECT 
# MAGIC   event_date,
# MAGIC   COUNT(DISTINCT customer_id) as active_customers,
# MAGIC   COUNT(DISTINCT CASE WHEN purchase_count > 0 THEN customer_id END) as purchasing_customers,
# MAGIC   SUM(total_revenue) as daily_revenue,
# MAGIC   AVG(total_revenue) as avg_revenue_per_customer,
# MAGIC   SUM(total_events) as total_events,
# MAGIC   AVG(total_events) as avg_events_per_customer,
# MAGIC   (COUNT(DISTINCT CASE WHEN purchase_count > 0 THEN customer_id END) * 100.0) / 
# MAGIC     NULLIF(COUNT(DISTINCT customer_id), 0) as conversion_rate_pct,
# MAGIC   current_timestamp() as calculated_at
# MAGIC FROM LIVE.gold_daily_customer_metrics
# MAGIC GROUP BY event_date;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 How to Deploy This Pipeline:
# MAGIC
# MAGIC ### Step 1: Create Pipeline Notebook
# MAGIC 1. Create a new notebook called `SDP_Customer_Events_Pipeline`
# MAGIC 2. Copy the Python or SQL definitions above
# MAGIC 3. Save the notebook
# MAGIC
# MAGIC ### Step 2: Create SDP Pipeline
# MAGIC 1. Go to **Workflows** → **Lakeflow Pipelines**
# MAGIC 2. Click **Create Pipeline**
# MAGIC 3. Configure:
# MAGIC    * **Pipeline Name**: `customer_events_pipeline`
# MAGIC    * **Notebook Library**: Select your pipeline notebook
# MAGIC    * **Target**: `main.default` (your Unity Catalog schema)
# MAGIC    * **Storage Location**: Leave default (Unity Catalog managed)
# MAGIC    * **Pipeline Mode**: Triggered or Continuous
# MAGIC
# MAGIC ### Step 3: Run Pipeline
# MAGIC 1. Click **Start**
# MAGIC 2. Watch the DAG visualization
# MAGIC 3. Monitor data quality metrics
# MAGIC 4. Check lineage graph
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 8: Governance & Monitoring
# MAGIC %md
# MAGIC # 🔒 SECTION 8: Governance & Monitoring
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Unity Catalog Integration
# MAGIC
# MAGIC SDP pipelines are **deeply integrated** with Unity Catalog:
# MAGIC
# MAGIC ### 1. **Automatic Table Registration**:
# MAGIC * All LIVE tables → Registered in Unity Catalog
# MAGIC * Managed Delta format
# MAGIC * Full ACID guarantees
# MAGIC
# MAGIC ### 2. **Automatic Lineage Tracking**:
# MAGIC ```
# MAGIC Source Table
# MAGIC     ↓ (captured automatically)
# MAGIC Bronze Table
# MAGIC     ↓ (captured automatically)
# MAGIC Silver Table
# MAGIC     ↓ (captured automatically)
# MAGIC Gold Table
# MAGIC ```
# MAGIC
# MAGIC **You get complete data lineage without any extra code!**
# MAGIC
# MAGIC ### 3. **Governance Features**:
# MAGIC * ✓ Fine-grained access control (GRANT/REVOKE)
# MAGIC * ✓ Column-level security
# MAGIC * ✓ Row-level filters
# MAGIC * ✓ Audit logs
# MAGIC * ✓ Data classification tags
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Pipeline Monitoring Dashboard
# MAGIC
# MAGIC The SDP UI provides real-time monitoring:
# MAGIC
# MAGIC ### 1. **Pipeline Status**:
# MAGIC * 🟪 Running / ✅ Succeeded / ❌ Failed
# MAGIC * Start time, duration, next scheduled run
# MAGIC * Cluster information
# MAGIC
# MAGIC ### 2. **Data Flow Visualization**:
# MAGIC ```
# MAGIC                     🟢 Bronze
# MAGIC                        ↓
# MAGIC                     🟡 Silver
# MAGIC                     /      \
# MAGIC                    /        \
# MAGIC          🟠 Gold_1    🟠 Gold_2
# MAGIC ```
# MAGIC
# MAGIC ### 3. **Data Quality Metrics**:
# MAGIC * Total records processed
# MAGIC * Records passed validation
# MAGIC * Records dropped (by constraint)
# MAGIC * Records failed (causing pipeline failure)
# MAGIC
# MAGIC ### 4. **Per-Table Metrics**:
# MAGIC ```
# MAGIC Table: silver_customer_events
# MAGIC ✓ Records processed: 1,000
# MAGIC ✓ Records passed: 997
# MAGIC ⚠️ Records dropped: 3
# MAGIC   - Constraint "valid_event_id": 1 violation
# MAGIC   - Constraint "valid_customer_id": 1 violation
# MAGIC   - Constraint "valid_event_type": 1 violation
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔍 Monitoring Best Practices:
# MAGIC
# MAGIC ### 1. **Set Up Alerts**:
# MAGIC ```python
# MAGIC # Configure pipeline to send alerts on:
# MAGIC - Pipeline failures
# MAGIC - Data quality violations above threshold
# MAGIC - Processing delays
# MAGIC - Unexpected data volume changes
# MAGIC ```
# MAGIC
# MAGIC ### 2. **Monitor Key Metrics**:
# MAGIC * **Throughput**: Records/second processed
# MAGIC * **Latency**: End-to-end pipeline duration
# MAGIC * **Data Quality**: Violation rates by constraint
# MAGIC * **Resource Usage**: Cluster utilization
# MAGIC
# MAGIC ### 3. **Review Data Quality Reports**:
# MAGIC * Check daily/weekly quality trends
# MAGIC * Investigate spikes in violations
# MAGIC * Adjust constraints based on business rules
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📋 Event Logs:
# MAGIC
# MAGIC SDP maintains detailed event logs:
# MAGIC
# MAGIC ```
# MAGIC [2026-04-21 10:00:00] Pipeline started
# MAGIC [2026-04-21 10:00:05] bronze_customer_events: Processing 1000 records
# MAGIC [2026-04-21 10:00:10] bronze_customer_events: Completed - 1000 records written
# MAGIC [2026-04-21 10:00:15] silver_customer_events: Processing 1000 records
# MAGIC [2026-04-21 10:00:16] silver_customer_events: Constraint violation - valid_event_id (1 record dropped)
# MAGIC [2026-04-21 10:00:16] silver_customer_events: Constraint violation - valid_customer_id (1 record dropped)
# MAGIC [2026-04-21 10:00:16] silver_customer_events: Constraint violation - valid_event_type (1 record dropped)
# MAGIC [2026-04-21 10:00:20] silver_customer_events: Completed - 997 records written
# MAGIC [2026-04-21 10:00:25] gold_daily_customer_metrics: Processing 997 records
# MAGIC [2026-04-21 10:00:30] gold_daily_customer_metrics: Completed - 85 records written
# MAGIC [2026-04-21 10:00:35] gold_daily_kpis: Processing 85 records
# MAGIC [2026-04-21 10:00:40] gold_daily_kpis: Completed - 21 records written
# MAGIC [2026-04-21 10:00:45] Pipeline completed successfully
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔎 Data Lineage Visualization:
# MAGIC
# MAGIC Unity Catalog automatically tracks:
# MAGIC
# MAGIC * **Table-level lineage**: Which tables depend on which
# MAGIC * **Column-level lineage**: How columns flow through transformations
# MAGIC * **Job lineage**: Which pipelines/jobs update which tables
# MAGIC
# MAGIC **Access via**: Unity Catalog UI → Select Table → Lineage Tab
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Sample Monitoring Query:
# MAGIC
# MAGIC ```sql
# MAGIC -- Query pipeline event logs (if exposed)
# MAGIC SELECT 
# MAGIC   event_timestamp,
# MAGIC   pipeline_id,
# MAGIC   table_name,
# MAGIC   event_type,
# MAGIC   records_processed,
# MAGIC   records_passed,
# MAGIC   records_dropped,
# MAGIC   records_failed
# MAGIC FROM system.pipeline_events
# MAGIC WHERE pipeline_id = '<your_pipeline_id>'
# MAGIC   AND event_timestamp >= current_date() - INTERVAL 7 DAYS
# MAGIC ORDER BY event_timestamp DESC;
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 9: End-to-End Architecture
# MAGIC %md
# MAGIC # 🏛️ SECTION 9: End-to-End Declarative Architecture
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Complete Data Architecture with SDP
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────┐
# MAGIC │              DATA SOURCES (External)                     │
# MAGIC │  📊 Kafka Streams  📊 Cloud Storage  📊 Databases  │
# MAGIC └───────────────────────┬──────────────────────────┘
# MAGIC                         │
# MAGIC                         │ Auto Loader / Streaming
# MAGIC                         │
# MAGIC                         ↓
# MAGIC ┌──────────────────────────────────────────────────┐
# MAGIC │          🟢 BRONZE LAYER (Raw Data)                 │
# MAGIC │                                                  │
# MAGIC │  • Preserve source data exactly as-is           │
# MAGIC │  • Add audit columns (ingestion time, source)   │
# MAGIC │  • Minimal validation (WARN mode)               │
# MAGIC │  • Schema-on-read                               │
# MAGIC │                                                  │
# MAGIC │  CREATE STREAMING TABLE bronze_events AS ...    │
# MAGIC └────────────────────────┬─────────────────────────┘
# MAGIC                         │
# MAGIC                         │ Data Quality Layer
# MAGIC                         │ (Constraints Applied)
# MAGIC                         ↓
# MAGIC ┌──────────────────────────────────────────────────┐
# MAGIC │      🟡 SILVER LAYER (Cleaned & Validated)       │
# MAGIC │                                                  │
# MAGIC │  • Data cleansing (trim, case normalization)   │
# MAGIC │  • Type casting and validation                 │
# MAGIC │  • Schema enforcement                           │
# MAGIC │  • Business rules (DROP ROW mode)              │
# MAGIC │  • Derived/enriched fields                     │
# MAGIC │                                                  │
# MAGIC │  CREATE STREAMING TABLE silver_events          │
# MAGIC │  (CONSTRAINT ... ON VIOLATION DROP ROW)        │
# MAGIC │  AS SELECT ... FROM STREAM(LIVE.bronze_events) │
# MAGIC └────────────────────────┬─────────────────────────┘
# MAGIC                         │
# MAGIC                         │ Business Logic Layer
# MAGIC                         │ (Aggregations & KPIs)
# MAGIC                         ↓
# MAGIC ┌──────────────────────────────────────────────────┐
# MAGIC │        🟠 GOLD LAYER (Business Metrics)          │
# MAGIC │                                                  │
# MAGIC │  • Pre-calculated KPIs                         │
# MAGIC │  • Denormalized for analytics                  │
# MAGIC │  • Business-friendly naming                    │
# MAGIC │  • Strict quality (FAIL UPDATE mode)           │
# MAGIC │  • Optimized for consumption                   │
# MAGIC │                                                  │
# MAGIC │  CREATE LIVE TABLE gold_daily_kpis             │
# MAGIC │  (CONSTRAINT ... ON VIOLATION FAIL UPDATE)     │
# MAGIC │  AS SELECT ... FROM LIVE.silver_events         │
# MAGIC └────────────────────────┬─────────────────────────┘
# MAGIC                         │
# MAGIC            ┌────────────┼────────────┐
# MAGIC            │            │            │
# MAGIC            ↓            ↓            ↓
# MAGIC      ┌─────────┐  ┌─────────┐  ┌─────────┐
# MAGIC      │   BI    │  │  Genie  │  │ ML/AI  │
# MAGIC      │ Dashbrd │  │  Spaces │  │ Models │
# MAGIC      └─────────┘  └─────────┘  └─────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔗 Cross-Cutting Concerns (Handled by SDP):
# MAGIC
# MAGIC ### 1. **Unity Catalog Governance**:
# MAGIC ```
# MAGIC ✓ All tables registered in Unity Catalog
# MAGIC ✓ Fine-grained access control (GRANT/REVOKE)
# MAGIC ✓ Automatic lineage tracking
# MAGIC ✓ Audit logs for all operations
# MAGIC ✓ Data classification tags
# MAGIC ```
# MAGIC
# MAGIC ### 2. **Data Quality Framework**:
# MAGIC ```
# MAGIC ✓ Expectations at Bronze (WARN)
# MAGIC ✓ Expectations at Silver (DROP ROW)
# MAGIC ✓ Expectations at Gold (FAIL UPDATE)
# MAGIC ✓ Automated quality metrics
# MAGIC ✓ Quarantine tables for investigation
# MAGIC ```
# MAGIC
# MAGIC ### 3. **Operational Excellence**:
# MAGIC ```
# MAGIC ✓ Auto-scaling compute
# MAGIC ✓ Automatic retries
# MAGIC ✓ Checkpoint management
# MAGIC ✓ Incremental processing
# MAGIC ✓ Cost optimization (only process changes)
# MAGIC ```
# MAGIC
# MAGIC ### 4. **Observability**:
# MAGIC ```
# MAGIC ✓ Real-time pipeline monitoring
# MAGIC ✓ Data quality dashboards
# MAGIC ✓ Event logs and alerts
# MAGIC ✓ Performance metrics
# MAGIC ✓ Lineage visualization
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Enterprise Deployment Pattern:
# MAGIC
# MAGIC ```
# MAGIC Development Environment:
# MAGIC   • dev.bronze.* → dev.silver.* → dev.gold.*
# MAGIC   • Rapid iteration
# MAGIC   • Full pipeline testing
# MAGIC   
# MAGIC   ↓ CI/CD Pipeline
# MAGIC   
# MAGIC  Staging Environment:
# MAGIC   • staging.bronze.* → staging.silver.* → staging.gold.*
# MAGIC   • Integration testing
# MAGIC   • Performance validation
# MAGIC   
# MAGIC   ↓ Approval Process
# MAGIC   
# MAGIC  Production Environment:
# MAGIC   • prod.bronze.* → prod.silver.* → prod.gold.*
# MAGIC   • Continuous monitoring
# MAGIC   • SLA enforcement
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚡ Key Architectural Benefits:
# MAGIC
# MAGIC ✅ **Declarative**: Focus on WHAT, not HOW  
# MAGIC ✅ **Automated**: No manual orchestration  
# MAGIC ✅ **Governed**: Built-in Unity Catalog integration  
# MAGIC ✅ **Quality-First**: Expectations at every layer  
# MAGIC ✅ **Observable**: Real-time monitoring & lineage  
# MAGIC ✅ **Scalable**: Auto-scaling & incremental processing  
# MAGIC ✅ **Reliable**: Automatic retries & checkpointing  
# MAGIC ✅ **Cost-Efficient**: Only process changed data  
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 10: Genie Code Agent Examples
# MAGIC %md
# MAGIC # 🧞 SECTION 10: Databricks Genie Code Agent Usage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💬 How to Use Genie Code with SDP:
# MAGIC
# MAGIC Databricks Genie Code (AI Assistant) can help you build, debug, and optimize SDP pipelines.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Example Prompts for SDP Development:
# MAGIC
# MAGIC ### 1. **Creating a New Pipeline**:
# MAGIC
# MAGIC 🗣️ **Prompt**:
# MAGIC ```
# MAGIC "Build an SDP pipeline with Bronze-Silver-Gold layers for customer transaction data. 
# MAGIC Source: main.raw.transactions table.
# MAGIC Bronze: Ingest with audit columns.
# MAGIC Silver: Clean data, drop null customer_ids and negative amounts.
# MAGIC Gold: Daily revenue by customer."
# MAGIC ```
# MAGIC
# MAGIC 🤖 **Genie Code will**:
# MAGIC * Generate complete pipeline definitions
# MAGIC * Add appropriate constraints
# MAGIC * Set up proper table properties
# MAGIC * Include data quality expectations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. **Adding Data Quality Constraints**:
# MAGIC
# MAGIC 🗣️ **Prompt**:
# MAGIC ```
# MAGIC "Add data quality constraints to my silver_orders table:
# MAGIC - Drop rows where order_id is null
# MAGIC - Drop rows where order_amount <= 0
# MAGIC - Fail pipeline if order_date is in the future"
# MAGIC ```
# MAGIC
# MAGIC 🤖 **Genie Code will**:
# MAGIC * Add CONSTRAINT clauses with appropriate violation actions
# MAGIC * Explain the impact of each constraint
# MAGIC * Show how to monitor violations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. **Converting Imperative ETL to Declarative**:
# MAGIC
# MAGIC 🗣️ **Prompt**:
# MAGIC ```
# MAGIC "Convert this imperative Spark code to SDP declarative pipeline:
# MAGIC
# MAGIC df = spark.read.table('source')
# MAGIC df_clean = df.filter(col('value') > 0)
# MAGIC df_agg = df_clean.groupBy('date').agg(sum('amount'))
# MAGIC df_agg.write.saveAsTable('target')
# MAGIC "
# MAGIC ```
# MAGIC
# MAGIC 🤖 **Genie Code will**:
# MAGIC * Transform imperative code to declarative SQL/Python
# MAGIC * Add appropriate table types (LIVE vs STREAMING)
# MAGIC * Include data quality best practices
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4. **Debugging Pipeline Issues**:
# MAGIC
# MAGIC 🗣️ **Prompt**:
# MAGIC ```
# MAGIC "My SDP pipeline is failing at the silver layer with constraint violations. 
# MAGIC Help me understand why and how to fix it."
# MAGIC ```
# MAGIC
# MAGIC 🤖 **Genie Code will**:
# MAGIC * Analyze error messages
# MAGIC * Suggest constraint adjustments
# MAGIC * Recommend quarantine pattern if needed
# MAGIC * Provide debugging queries
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5. **Optimizing Pipeline Performance**:
# MAGIC
# MAGIC 🗣️ **Prompt**:
# MAGIC ```
# MAGIC "My gold layer aggregation is slow. How can I optimize it?"
# MAGIC ```
# MAGIC
# MAGIC 🤖 **Genie Code will**:
# MAGIC * Analyze query patterns
# MAGIC * Suggest partition strategies
# MAGIC * Recommend Z-ordering
# MAGIC * Propose incremental aggregation patterns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6. **Creating Quarantine Tables**:
# MAGIC
# MAGIC 🗣️ **Prompt**:
# MAGIC ```
# MAGIC "Create a quarantine table to capture records that fail validation 
# MAGIC in my silver_customers table (where email is invalid or age is out of range)."
# MAGIC ```
# MAGIC
# MAGIC 🤖 **Genie Code will**:
# MAGIC * Generate quarantine table definition
# MAGIC * Set up filtering logic
# MAGIC * Add useful debugging columns
# MAGIC * Show how to analyze quarantined data
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 7. **Setting Up Monitoring**:
# MAGIC
# MAGIC 🗣️ **Prompt**:
# MAGIC ```
# MAGIC "Show me how to monitor data quality metrics for my pipeline and 
# MAGIC set up alerts when violation rates exceed 5%."
# MAGIC ```
# MAGIC
# MAGIC 🤖 **Genie Code will**:
# MAGIC * Show how to query pipeline metrics
# MAGIC * Provide alerting configuration
# MAGIC * Suggest dashboard visualizations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 8. **Implementing Slowly Changing Dimensions**:
# MAGIC
# MAGIC 🗣️ **Prompt**:
# MAGIC ```
# MAGIC "Implement SCD Type 2 for customer dimension in my SDP pipeline."
# MAGIC ```
# MAGIC
# MAGIC 🤖 **Genie Code will**:
# MAGIC * Generate SCD Type 2 logic using SDP
# MAGIC * Add effective date columns
# MAGIC * Implement merge logic
# MAGIC * Handle updates properly
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Pro Tips for Using Genie Code with SDP:
# MAGIC
# MAGIC 1. **Be Specific**: Include source/target table names, catalogs, schemas
# MAGIC 2. **Mention Constraints**: Specify data quality requirements explicitly
# MAGIC 3. **State Layer**: Clarify if Bronze/Silver/Gold
# MAGIC 4. **Include Context**: Share error messages when debugging
# MAGIC 5. **Ask for Explanations**: Request "Explain why..." for learning
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔥 Sample Conversation Flow:
# MAGIC
# MAGIC ```
# MAGIC You: "Create an SDP pipeline for IoT sensor data"
# MAGIC
# MAGIC Genie: [Generates basic pipeline structure]
# MAGIC
# MAGIC You: "Add constraints to drop sensors with invalid IDs"
# MAGIC
# MAGIC Genie: [Adds appropriate constraints]
# MAGIC
# MAGIC You: "Create gold table with hourly aggregates by sensor"
# MAGIC
# MAGIC Genie: [Creates gold layer aggregation]
# MAGIC
# MAGIC You: "Show me how to deploy this to production"
# MAGIC
# MAGIC Genie: [Provides deployment steps and best practices]
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 11: Summary & Interview Questions
# MAGIC %md
# MAGIC # 🎓 SECTION 11: Summary, Key Learnings & Interview Prep
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Key Learnings Summary:
# MAGIC
# MAGIC ### 1. **SDP Fundamentals**:
# MAGIC * Spark Declarative Pipelines = Define WHAT, not HOW
# MAGIC * Automatic dependency resolution
# MAGIC * Built-in data quality framework
# MAGIC * Unity Catalog native integration
# MAGIC
# MAGIC ### 2. **Table Types**:
# MAGIC * **LIVE TABLE**: Materialized, auto-refreshed
# MAGIC * **STREAMING TABLE**: Incremental, continuous processing
# MAGIC * **VIEW**: Non-materialized, query-time evaluation
# MAGIC
# MAGIC ### 3. **Constraint Modes**:
# MAGIC * **WARN**: Log violations, allow data
# MAGIC * **DROP ROW**: Filter invalid records
# MAGIC * **FAIL UPDATE**: Stop pipeline on violations
# MAGIC
# MAGIC ### 4. **Medallion Integration**:
# MAGIC * 🟢 Bronze: Raw ingestion (WARN)
# MAGIC * 🟡 Silver: Cleansing (DROP ROW)
# MAGIC * 🟠 Gold: Aggregates (FAIL UPDATE)
# MAGIC
# MAGIC ### 5. **Automation Benefits**:
# MAGIC * No manual orchestration
# MAGIC * Automatic retries
# MAGIC * Built-in monitoring
# MAGIC * Cost-efficient incremental processing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Interview Questions & Answers:
# MAGIC
# MAGIC ### ❓ Q1: What is the main difference between SDP and traditional ETL?
# MAGIC
# MAGIC **Answer**: 
# MAGIC SDP is **declarative** (define WHAT you want) while traditional ETL is **imperative** (define HOW to do it). SDP automatically handles orchestration, dependencies, retries, and optimization, whereas traditional ETL requires manual implementation of these concerns.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❓ Q2: What are the three constraint violation modes in SDP?
# MAGIC
# MAGIC **Answer**:
# MAGIC 1. **WARN**: Log violations but allow data to pass (for monitoring)
# MAGIC 2. **DROP ROW**: Filter out invalid records (for cleansing)
# MAGIC 3. **FAIL UPDATE**: Stop pipeline execution (for critical data)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❓ Q3: When should you use STREAMING TABLE vs LIVE TABLE?
# MAGIC
# MAGIC **Answer**:
# MAGIC * **STREAMING TABLE**: Use when you need:
# MAGIC   * Incremental processing with exactly-once semantics
# MAGIC   * Low-latency continuous processing
# MAGIC   * Explicit checkpoint management
# MAGIC   * Reading from streaming sources (Kafka, cloud_files)
# MAGIC
# MAGIC * **LIVE TABLE**: Use when you need:
# MAGIC   * Batch processing or aggregations
# MAGIC   * Full table refreshes are acceptable
# MAGIC   * System-managed optimization
# MAGIC   * Final aggregation layers (Gold)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❓ Q4: How does SDP integrate with Unity Catalog?
# MAGIC
# MAGIC **Answer**:
# MAGIC * All tables automatically registered in Unity Catalog
# MAGIC * Automatic lineage tracking (table and column level)
# MAGIC * Governance features (access control, audit logs)
# MAGIC * Managed Delta format with ACID guarantees
# MAGIC * Automatic schema evolution handling
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❓ Q5: Explain the quarantine pattern in SDP.
# MAGIC
# MAGIC **Answer**:
# MAGIC Quarantine pattern separates valid and invalid records into different tables:
# MAGIC
# MAGIC ```sql
# MAGIC -- Valid records
# MAGIC CREATE LIVE TABLE silver_valid
# MAGIC (CONSTRAINT valid_data EXPECT (...) ON VIOLATION DROP ROW)
# MAGIC AS SELECT * FROM LIVE.bronze;
# MAGIC
# MAGIC -- Invalid records (quarantine)
# MAGIC CREATE LIVE TABLE quarantine_invalid
# MAGIC AS SELECT * FROM LIVE.bronze
# MAGIC WHERE NOT (...);
# MAGIC ```
# MAGIC
# MAGIC Benefits: Maintain data quality while preserving problematic records for investigation.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❓ Q6: How does SDP handle schema evolution?
# MAGIC
# MAGIC **Answer**:
# MAGIC SDP automatically detects and handles schema changes:
# MAGIC * New columns: Automatically added
# MAGIC * Type changes: Handled gracefully with casting
# MAGIC * Column renames: Tracked via lineage
# MAGIC * No manual ALTER TABLE needed
# MAGIC * Schema inference for cloud_files sources
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❓ Q7: What are the benefits of SDP over manually orchestrated pipelines?
# MAGIC
# MAGIC **Answer**:
# MAGIC 1. **Less Code**: 90%+ reduction in orchestration code
# MAGIC 2. **Auto-Optimization**: System handles performance tuning
# MAGIC 3. **Built-in Quality**: Data validation framework included
# MAGIC 4. **Auto-Lineage**: Complete traceability out-of-the-box
# MAGIC 5. **Reduced Ops**: No manual scheduling, retries, or checkpointing
# MAGIC 6. **Cost Efficiency**: Incremental processing by default
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❓ Q8: How do you reference other tables in SDP?
# MAGIC
# MAGIC **Answer**:
# MAGIC ```sql
# MAGIC -- For LIVE tables (batch read)
# MAGIC SELECT * FROM LIVE.bronze_table
# MAGIC
# MAGIC -- For STREAMING tables (incremental read)
# MAGIC SELECT * FROM STREAM(LIVE.bronze_table)
# MAGIC ```
# MAGIC
# MAGIC Use `LIVE.` prefix to reference pipeline tables.
# MAGIC Use `STREAM()` function for incremental processing.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❓ Q9: What monitoring capabilities does SDP provide?
# MAGIC
# MAGIC **Answer**:
# MAGIC * Real-time pipeline status and execution logs
# MAGIC * Data quality metrics per constraint
# MAGIC * Records processed/passed/dropped/failed
# MAGIC * Visual DAG of table dependencies
# MAGIC * Performance metrics (throughput, latency)
# MAGIC * Unity Catalog lineage visualization
# MAGIC * Alert configuration for failures and quality issues
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❓ Q10: How do you implement Medallion architecture with SDP?
# MAGIC
# MAGIC **Answer**:
# MAGIC ```sql
# MAGIC -- Bronze: Raw ingestion
# MAGIC CREATE STREAMING TABLE bronze_data AS 
# MAGIC SELECT * FROM source;
# MAGIC
# MAGIC -- Silver: Cleansing
# MAGIC CREATE STREAMING TABLE silver_data
# MAGIC (CONSTRAINT valid EXPECT (...) ON VIOLATION DROP ROW)
# MAGIC AS SELECT * FROM STREAM(LIVE.bronze_data);
# MAGIC
# MAGIC -- Gold: Aggregations
# MAGIC CREATE LIVE TABLE gold_metrics
# MAGIC (CONSTRAINT critical EXPECT (...) ON VIOLATION FAIL UPDATE)
# MAGIC AS SELECT ... FROM LIVE.silver_data GROUP BY ...;
# MAGIC ```
# MAGIC
# MAGIC SDP automatically resolves Bronze → Silver → Gold dependencies.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Common Mistakes to Avoid:
# MAGIC
# MAGIC ### 1. **❌ Using Imperative Style Instead of Declarative**:
# MAGIC
# MAGIC **Wrong**:
# MAGIC ```python
# MAGIC df = spark.read.table("source")
# MAGIC df.write.saveAsTable("target")
# MAGIC # Manual scheduling
# MAGIC # Manual retries
# MAGIC # Manual lineage
# MAGIC ```
# MAGIC
# MAGIC **Right**:
# MAGIC ```sql
# MAGIC CREATE LIVE TABLE target AS 
# MAGIC SELECT * FROM source;
# MAGIC -- Everything else is automatic!
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. **❌ Ignoring Data Quality Constraints**:
# MAGIC
# MAGIC **Wrong**:
# MAGIC ```sql
# MAGIC CREATE LIVE TABLE silver_data AS 
# MAGIC SELECT * FROM LIVE.bronze_data;
# MAGIC -- No validation!
# MAGIC ```
# MAGIC
# MAGIC **Right**:
# MAGIC ```sql
# MAGIC CREATE LIVE TABLE silver_data
# MAGIC (
# MAGIC   CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
# MAGIC   CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC AS SELECT * FROM LIVE.bronze_data;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. **❌ Poor Layer Separation**:
# MAGIC
# MAGIC **Wrong**:
# MAGIC ```sql
# MAGIC -- Mixing Bronze and Gold logic
# MAGIC CREATE LIVE TABLE bronze_with_aggregates AS 
# MAGIC SELECT date, COUNT(*) FROM source GROUP BY date;
# MAGIC ```
# MAGIC
# MAGIC **Right**:
# MAGIC ```sql
# MAGIC -- Bronze: Raw data
# MAGIC CREATE STREAMING TABLE bronze AS SELECT * FROM source;
# MAGIC
# MAGIC -- Silver: Cleaned
# MAGIC CREATE STREAMING TABLE silver AS SELECT * FROM STREAM(LIVE.bronze);
# MAGIC
# MAGIC -- Gold: Aggregates
# MAGIC CREATE LIVE TABLE gold AS 
# MAGIC SELECT date, COUNT(*) FROM LIVE.silver GROUP BY date;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4. **❌ Manual Orchestration**:
# MAGIC
# MAGIC **Wrong**:
# MAGIC ```python
# MAGIC # Manually chaining pipeline stages
# MAGIC run_bronze()
# MAGIC if bronze_success:
# MAGIC     run_silver()
# MAGIC     if silver_success:
# MAGIC         run_gold()
# MAGIC ```
# MAGIC
# MAGIC **Right**:
# MAGIC ```sql
# MAGIC -- Just define dependencies, SDP orchestrates
# MAGIC CREATE LIVE TABLE bronze AS ...;
# MAGIC CREATE LIVE TABLE silver AS SELECT * FROM LIVE.bronze;
# MAGIC CREATE LIVE TABLE gold AS SELECT * FROM LIVE.silver;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5. **❌ Not Using Appropriate Table Types**:
# MAGIC
# MAGIC **Wrong**:
# MAGIC ```sql
# MAGIC -- Using LIVE TABLE for streaming source
# MAGIC CREATE LIVE TABLE bronze AS 
# MAGIC SELECT * FROM kafka_stream;  -- Should be STREAMING TABLE!
# MAGIC ```
# MAGIC
# MAGIC **Right**:
# MAGIC ```sql
# MAGIC CREATE STREAMING TABLE bronze AS 
# MAGIC SELECT * FROM kafka_stream;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6. **❌ Hardcoding Environment-Specific Values**:
# MAGIC
# MAGIC **Wrong**:
# MAGIC ```sql
# MAGIC CREATE LIVE TABLE silver AS 
# MAGIC SELECT * FROM prod.schema.bronze;  -- Hardcoded!
# MAGIC ```
# MAGIC
# MAGIC **Right**:
# MAGIC ```sql
# MAGIC -- Use LIVE prefix for pipeline tables
# MAGIC CREATE LIVE TABLE silver AS 
# MAGIC SELECT * FROM LIVE.bronze;
# MAGIC
# MAGIC -- Or use pipeline parameters for external references
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 7. **❌ Overusing FAIL UPDATE**:
# MAGIC
# MAGIC **Wrong**:
# MAGIC ```sql
# MAGIC CREATE LIVE TABLE bronze
# MAGIC (
# MAGIC   -- Too strict for raw data!
# MAGIC   CONSTRAINT perfect_data EXPECT (everything_is_valid) ON VIOLATION FAIL UPDATE
# MAGIC )
# MAGIC AS SELECT * FROM source;
# MAGIC ```
# MAGIC
# MAGIC **Right**:
# MAGIC ```sql
# MAGIC -- Bronze: WARN (monitor issues)
# MAGIC CREATE STREAMING TABLE bronze
# MAGIC (CONSTRAINT check_data EXPECT (id IS NOT NULL))
# MAGIC AS SELECT * FROM source;
# MAGIC
# MAGIC -- Silver: DROP ROW (filter bad data)
# MAGIC CREATE STREAMING TABLE silver
# MAGIC (CONSTRAINT valid_data EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW)
# MAGIC AS SELECT * FROM STREAM(LIVE.bronze);
# MAGIC
# MAGIC -- Gold: FAIL UPDATE (critical metrics)
# MAGIC CREATE LIVE TABLE gold
# MAGIC (CONSTRAINT critical_data EXPECT (revenue > 0) ON VIOLATION FAIL UPDATE)
# MAGIC AS SELECT ... FROM LIVE.silver;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 8. **❌ Not Monitoring Data Quality Metrics**:
# MAGIC
# MAGIC **Wrong**: Define constraints but never check violation rates.
# MAGIC
# MAGIC **Right**: 
# MAGIC * Review pipeline quality metrics regularly
# MAGIC * Set up alerts for high violation rates
# MAGIC * Investigate quarantined records
# MAGIC * Adjust constraints based on business needs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Next Steps:
# MAGIC
# MAGIC 1. **Practice**: Create SDP pipelines in your workspace
# MAGIC 2. **Experiment**: Try different constraint modes
# MAGIC 3. **Monitor**: Review pipeline execution and quality metrics
# MAGIC 4. **Optimize**: Tune performance based on monitoring data
# MAGIC 5. **Learn More**: Explore advanced features (CDC, SCD, incremental aggregations)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Additional Resources:
# MAGIC
# MAGIC * Databricks Documentation: Lakeflow Spark Declarative Pipelines
# MAGIC * Unity Catalog Best Practices
# MAGIC * Delta Lake Optimization Guide
# MAGIC * Medallion Architecture Patterns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Congratulations!
# MAGIC
# MAGIC You've completed **Phase 6 Day 31: Spark Declarative Pipelines (SDP)**!
# MAGIC
# MAGIC ### You now understand:
# MAGIC * ✓ Declarative vs Imperative pipelines
# MAGIC * ✓ SDP table types and syntax
# MAGIC * ✓ Data quality constraints and expectations
# MAGIC * ✓ Pipeline automation and monitoring
# MAGIC * ✓ Medallion architecture with SDP
# MAGIC * ✓ Enterprise deployment patterns
# MAGIC * ✓ Best practices and common pitfalls
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍🏫 **@TRRaveendra** | Databricks Training Series
# MAGIC
# MAGIC ---