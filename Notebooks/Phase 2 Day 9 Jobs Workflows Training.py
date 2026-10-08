# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # 🔄 Data Engineering Training — Phase 2 Day 9  
# MAGIC ## ⚙️ Jobs, Tasks & Workflows in Databricks  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - Databricks Jobs & Tasks  
# MAGIC - Workflow Orchestration (DAG concepts)  
# MAGIC - Scheduling Pipelines  
# MAGIC - Parameter Passing  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### ⚙️ Platform: Databricks (Serverless + Unity Catalog + Workflows)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Objective:
# MAGIC Learn how to orchestrate data pipelines using Databricks Jobs and Workflows, including task dependencies, scheduling, and parameterization.

# COMMAND ----------

# DBTITLE 1,Section 1: Introduction to Jobs & Workflows
# MAGIC %md
# MAGIC ## 📘 Section 1: Introduction to Jobs & Workflows
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧒 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine you have chores to do:
# MAGIC 1. Clean your room
# MAGIC 2. Do homework
# MAGIC 3. Play outside
# MAGIC
# MAGIC A **Job** is like a list of chores you need to complete.  
# MAGIC Each **Task** is one chore (clean room, do homework).  
# MAGIC A **Workflow** is the order you do them in — maybe you can't play outside until homework is done!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Job**: A Databricks Job is an automated, scheduled, or on-demand execution unit that orchestrates one or more tasks.
# MAGIC
# MAGIC **Task**: A task is an individual unit of work within a job (e.g., running a notebook, executing a Python script, or running SQL queries).
# MAGIC
# MAGIC **Workflow**: A workflow is a Directed Acyclic Graph (DAG) of tasks with defined dependencies, enabling complex data pipeline orchestration.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Conceptual Flow:
# MAGIC
# MAGIC ```
# MAGIC ┌──────────┐
# MAGIC │  Task A   │
# MAGIC │ (Ingest) │
# MAGIC └────┬─────┘
# MAGIC      │
# MAGIC      ↓
# MAGIC ┌────┴──────────┐
# MAGIC │    Task B      │
# MAGIC │ (Transform)   │
# MAGIC └────┬──────────┘
# MAGIC      │
# MAGIC      ↓
# MAGIC ┌────┴──────────┐
# MAGIC │    Task C      │
# MAGIC │ (Aggregate)   │
# MAGIC └───────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Key Insight**: Tasks execute in sequence based on dependencies, ensuring data is ready before downstream processing begins.

# COMMAND ----------

# DBTITLE 1,Section 2: Jobs & Task Types
# MAGIC %md
# MAGIC ## 📝 Section 2: Jobs & Task Types
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📦 Types of Tasks in Databricks:
# MAGIC
# MAGIC #### 1. **Notebook Task**
# MAGIC - Executes a Databricks notebook
# MAGIC - Best for: Interactive development, complex transformations
# MAGIC - Example: ETL pipeline implemented in Python/SQL notebook
# MAGIC
# MAGIC #### 2. **Python Script Task**
# MAGIC - Runs a standalone Python file (.py)
# MAGIC - Best for: Production scripts, modular code
# MAGIC - Example: Data validation scripts
# MAGIC
# MAGIC #### 3. **SQL Task**
# MAGIC - Executes SQL queries directly
# MAGIC - Best for: Simple transformations, reporting queries
# MAGIC - Example: Aggregation queries, view creation
# MAGIC
# MAGIC #### 4. **JAR Task** (Scala/Java)
# MAGIC - Runs compiled Scala/Java applications
# MAGIC - Best for: Performance-critical workloads
# MAGIC
# MAGIC #### 5. **Pipeline Task**
# MAGIC - Triggers Lakeflow Spark Declarative Pipelines (formerly DLT)
# MAGIC - Best for: Streaming + batch data pipelines
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏭 Use Cases:
# MAGIC
# MAGIC | **Use Case** | **Recommended Task Type** |
# MAGIC |---|---|
# MAGIC | ETL Pipeline | Notebook Task |
# MAGIC | Data Validation | Python Script Task |
# MAGIC | Aggregation Reports | SQL Task |
# MAGIC | Streaming Pipeline | Pipeline Task |
# MAGIC | ML Model Training | Notebook Task |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✅ Best Practice:
# MAGIC - Use **Notebook tasks** for development and iterative work
# MAGIC - Use **Python script tasks** for production-grade, version-controlled code
# MAGIC - Use **SQL tasks** for simple, declarative transformations

# COMMAND ----------

# DBTITLE 1,Section 3: Workflow Orchestration (DAG)
# MAGIC %md
# MAGIC ## 🔀 Section 3: Workflow Orchestration (DAG)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC
# MAGIC A **DAG** (Directed Acyclic Graph) is like a recipe:
# MAGIC - You can't frost a cake before baking it
# MAGIC - You can't bake it before mixing ingredients
# MAGIC - Some steps can happen at the same time (chopping vegetables while water boils)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level:
# MAGIC
# MAGIC **Directed Acyclic Graph (DAG)** is a computational model where:
# MAGIC - **Directed**: Tasks flow in one direction (A → B → C)
# MAGIC - **Acyclic**: No circular dependencies (no loops)
# MAGIC - **Graph**: Tasks are nodes, dependencies are edges
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Example DAG:
# MAGIC
# MAGIC ```
# MAGIC        ┌──────────────┐
# MAGIC        │  Ingestion   │
# MAGIC        │  (Task A)    │
# MAGIC        └──────┬───────┘
# MAGIC               │
# MAGIC               ↓
# MAGIC        ┌──────┴────────────────┐
# MAGIC        │  Transformation      │
# MAGIC        │  (Task B)            │
# MAGIC        └─────┬─────────┬───────┘
# MAGIC              │            │
# MAGIC       ┌──────┴──────┐   │
# MAGIC       │  Aggregate 1 │   │
# MAGIC       │  (Task C1)   │   │
# MAGIC       └─────┬───────┘   │
# MAGIC             │    ┌──────┴──────┐
# MAGIC             │    │  Aggregate 2 │
# MAGIC             │    │  (Task C2)   │
# MAGIC             │    └─────┬───────┘
# MAGIC             └────────┴────────┐
# MAGIC                      │
# MAGIC               ┌──────┴───────┐
# MAGIC               │   Output     │
# MAGIC               │  (Task D)    │
# MAGIC               └──────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚡ Sequential vs Parallel Execution:
# MAGIC
# MAGIC **Sequential**: Task B waits for Task A to complete  
# MAGIC **Parallel**: Task C1 and C2 run simultaneously after Task B
# MAGIC
# MAGIC **Benefit**: Reduced pipeline execution time through parallelization

# COMMAND ----------

# DBTITLE 1,Section 4: Scheduling Jobs
# MAGIC %md
# MAGIC ## ⏰ Section 4: Scheduling Jobs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📅 Scheduling Mechanisms:
# MAGIC
# MAGIC #### 1. **Time-Based Scheduling (Cron)**
# MAGIC - Runs on a fixed schedule (hourly, daily, weekly)
# MAGIC - Uses cron expressions
# MAGIC - Example: Daily at 2 AM
# MAGIC
# MAGIC #### 2. **Trigger-Based Scheduling**
# MAGIC - Runs when a specific event occurs
# MAGIC - Example: File arrival, upstream job completion
# MAGIC
# MAGIC #### 3. **Continuous Scheduling**
# MAGIC - Runs continuously (for streaming workloads)
# MAGIC - Example: Real-time event processing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🕒 Common Scheduling Patterns:
# MAGIC
# MAGIC | **Pattern** | **Cron Expression** | **Description** |
# MAGIC |---|---|---|
# MAGIC | Every hour | `0 0 * * * ?` | Top of every hour |
# MAGIC | Daily at 2 AM | `0 0 2 * * ?` | 2:00 AM every day |
# MAGIC | Every Monday at 8 AM | `0 0 8 ? * MON` | Monday mornings |
# MAGIC | Every 15 minutes | `0 */15 * * * ?` | Quarter-hour intervals |
# MAGIC | First of month | `0 0 0 1 * ?` | Midnight on 1st |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏭 Use Cases:
# MAGIC
# MAGIC **Daily Batch Job**:  
# MAGIC - Load yesterday's transactions  
# MAGIC - Transform and aggregate  
# MAGIC - Update reporting tables  
# MAGIC
# MAGIC **Hourly Pipeline**:  
# MAGIC - Ingest streaming data  
# MAGIC - Apply real-time transformations  
# MAGIC - Update dashboards  
# MAGIC
# MAGIC **Event-Driven**:  
# MAGIC - New file arrives in cloud storage  
# MAGIC - Trigger ingestion workflow  
# MAGIC - Process and load into data warehouse

# COMMAND ----------

# DBTITLE 1,Section 5: Parameter Passing - Concepts
# MAGIC %md
# MAGIC ## 📥 Section 5: Parameter Passing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC
# MAGIC Imagine you have a toy robot that can fetch things. Instead of programming it to fetch only "red ball," you give it a parameter: "fetch [color] [object]." Now it can fetch blue ball, green car, etc.
# MAGIC
# MAGIC **Parameters make your code flexible!**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level:
# MAGIC
# MAGIC **Parameterization** enables dynamic pipeline execution by passing runtime values to tasks. This approach:
# MAGIC - Eliminates hardcoded values
# MAGIC - Enables reusability across environments (dev, staging, prod)
# MAGIC - Supports dynamic date ranges, paths, and configuration
# MAGIC - Facilitates testing with different input sets
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔑 Why Parameterization Matters:
# MAGIC
# MAGIC ❌ **Bad Practice** (Hardcoded):
# MAGIC ```python
# MAGIC df = spark.read.parquet("/Volumes/catalog/schema/volume/data/2026-04-20/")
# MAGIC ```
# MAGIC
# MAGIC ✅ **Good Practice** (Parameterized):
# MAGIC ```python
# MAGIC date = dbutils.widgets.get("process_date")
# MAGIC df = spark.read.parquet(f"/Volumes/catalog/schema/volume/data/{date}/")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📦 Parameter Types:
# MAGIC
# MAGIC 1. **Input Paths**: Source data location
# MAGIC 2. **Output Paths**: Destination for processed data
# MAGIC 3. **Date Ranges**: Time-based filtering
# MAGIC 4. **Configuration**: Thresholds, flags, environment settings
# MAGIC 5. **Business Logic**: Product IDs, customer segments

# COMMAND ----------

# DBTITLE 1,Section 5: Parameter Demo Setup
# MAGIC %md
# MAGIC ### 🛠️ Hands-On: Parameter Passing Demo
# MAGIC
# MAGIC We'll demonstrate parameter passing using **widgets** in Databricks. Widgets allow you to:
# MAGIC - Define input parameters
# MAGIC - Pass values at runtime
# MAGIC - Make notebooks reusable across different contexts

# COMMAND ----------

# DBTITLE 1,Create Parameter Widgets
# Create parameter widgets for our workflow
# These will appear at the top of the notebook for easy input

# Remove any existing widgets first (to handle re-runs)
try:
    dbutils.widgets.removeAll()
except:
    pass

# Create fresh widgets
dbutils.widgets.text("catalog_name", "main", "1. Catalog Name")
dbutils.widgets.text("schema_name", "default", "2. Schema Name")
dbutils.widgets.text("process_date", "2026-04-21", "3. Process Date")
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "4. Environment")

print("✅ Widgets created successfully!")
print("\n📌 You can now modify parameter values using the widgets above.")

# COMMAND ----------

# DBTITLE 1,Retrieve and Use Parameters
# Retrieve parameter values from widgets
catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
process_date = dbutils.widgets.get("process_date")
environment = dbutils.widgets.get("environment")

# Display retrieved parameters
print("=" * 60)
print("📦 WORKFLOW PARAMETERS")
print("=" * 60)
print(f"Catalog:       {catalog}")
print(f"Schema:        {schema}")
print(f"Process Date:  {process_date}")
print(f"Environment:   {environment}")
print("=" * 60)

# Construct dynamic paths using parameters
base_path = f"/Volumes/{catalog}/{schema}/workflows"
print(f"\n📋 Base Path: {base_path}")
print(f"📅 Date-specific Path: {base_path}/data_{process_date}")

# COMMAND ----------

# DBTITLE 1,Parameter Passing Best Practices
# MAGIC %md
# MAGIC ### 🎯 Best Practices for Parameter Passing:
# MAGIC
# MAGIC 1. **Always provide default values** for widgets
# MAGIC 2. **Use descriptive parameter names** (avoid abbreviations)
# MAGIC 3. **Validate parameters** before using them
# MAGIC 4. **Document expected formats** (e.g., date format: YYYY-MM-DD)
# MAGIC 5. **Use dropdown widgets** for fixed value sets
# MAGIC 6. **Never hardcode credentials** — use secrets instead
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔄 Passing Parameters Between Tasks:
# MAGIC
# MAGIC In Databricks Jobs, you can:
# MAGIC - **Task A** sets a value: `dbutils.jobs.taskValues.set("key", "value")`
# MAGIC - **Task B** retrieves it: `dbutils.jobs.taskValues.get(taskKey="TaskA", key="key")`
# MAGIC
# MAGIC This enables dynamic workflows where downstream tasks adapt based on upstream results.

# COMMAND ----------

# DBTITLE 1,Section 6: Hands-on Workflow Simulation
# MAGIC %md
# MAGIC ## 🛠️ Section 6: Hands-On Workflow Simulation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Objective:
# MAGIC Simulate a 3-task workflow:
# MAGIC 1. **Task 1**: Ingest sample data
# MAGIC 2. **Task 2**: Transform the data
# MAGIC 3. **Task 3**: Aggregate results
# MAGIC
# MAGIC In a real Databricks Job, each task would be a separate notebook or script. Here, we simulate the workflow in sequential cells.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Workflow DAG:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────┐
# MAGIC │   Task 1:      │
# MAGIC │   Ingest Data  │
# MAGIC └───────┬─────────┘
# MAGIC         │
# MAGIC         ↓
# MAGIC ┌───────┴─────────────────┐
# MAGIC │   Task 2:              │
# MAGIC │   Transform Data       │
# MAGIC └───────┬─────────────────┘
# MAGIC         │
# MAGIC         ↓
# MAGIC ┌───────┴─────────────────┐
# MAGIC │   Task 3:              │
# MAGIC │   Aggregate Results    │
# MAGIC └────────────────────────┘
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Task 1: Data Ingestion
# =============================================================================
# TASK 1: DATA INGESTION
# =============================================================================
# In a real job, this would be a separate notebook/script
# Purpose: Load or generate raw data for processing

from pyspark.sql import functions as F
from datetime import datetime

print("🚀 Starting Task 1: Data Ingestion")
print("=" * 60)

# Retrieve parameter for process date
process_date = dbutils.widgets.get("process_date")

# Simulate ingestion by creating sample sales data
raw_data = [
    (1, "2026-04-21", "Electronics", "Laptop", 1200.00, "CA"),
    (2, "2026-04-21", "Electronics", "Mouse", 25.00, "NY"),
    (3, "2026-04-21", "Furniture", "Desk", 350.00, "TX"),
    (4, "2026-04-21", "Electronics", "Keyboard", 75.00, "CA"),
    (5, "2026-04-21", "Furniture", "Chair", 150.00, "FL"),
    (6, "2026-04-21", "Electronics", "Monitor", 300.00, "NY"),
    (7, "2026-04-21", "Office", "Notebook", 5.00, "TX"),
    (8, "2026-04-21", "Office", "Pen", 2.00, "CA"),
]

schema = ["order_id", "order_date", "category", "product", "amount", "state"]
df_raw = spark.createDataFrame(raw_data, schema)

# Add ingestion metadata
df_raw = df_raw.withColumn("ingestion_timestamp", F.current_timestamp()) \
               .withColumn("process_date", F.lit(process_date))

print(f"✅ Ingested {df_raw.count()} records for date: {process_date}")
print("\n📊 Sample Data:")
display(df_raw)

# Store as temporary view for next task
df_raw.createOrReplaceTempView("bronze_sales")
print("\n✅ Task 1 Complete: Data stored in 'bronze_sales' view")

# COMMAND ----------

# DBTITLE 1,Task 2: Data Transformation
# =============================================================================
# TASK 2: DATA TRANSFORMATION
# =============================================================================
# In a real job, this would be a separate notebook/script dependent on Task 1
# Purpose: Clean and enrich data

from pyspark.sql import functions as F

print("🚀 Starting Task 2: Data Transformation")
print("=" * 60)

# Read from previous task output
df_bronze = spark.table("bronze_sales")

print(f"📊 Processing {df_bronze.count()} records from Task 1")

# Apply transformations:
# 1. Add revenue tier classification
# 2. Create product_key
# 3. Add region mapping

df_silver = df_bronze.withColumn(
    "revenue_tier",
    F.when(F.col("amount") >= 500, "High")
     .when(F.col("amount") >= 100, "Medium")
     .otherwise("Low")
).withColumn(
    "product_key",
    F.concat(F.col("category"), F.lit("_"), F.col("product"))
).withColumn(
    "region",
    F.when(F.col("state").isin("CA", "NY"), "East")
     .when(F.col("state").isin("TX", "FL"), "South")
     .otherwise("Other")
).withColumn(
    "transformation_timestamp",
    F.current_timestamp()
)

print("\n✅ Transformations Applied:")
print("  • Revenue tier classification")
print("  • Product key generation")
print("  • Region mapping")

print("\n📊 Transformed Data:")
display(df_silver)

# Store as temporary view for next task
df_silver.createOrReplaceTempView("silver_sales")
print("\n✅ Task 2 Complete: Data stored in 'silver_sales' view")

# COMMAND ----------

# DBTITLE 1,Task 3: Data Aggregation
# =============================================================================
# TASK 3: DATA AGGREGATION
# =============================================================================
# In a real job, this would be a separate notebook/script dependent on Task 2
# Purpose: Generate business metrics and insights

from pyspark.sql import functions as F

print("🚀 Starting Task 3: Data Aggregation")
print("=" * 60)

# Read from previous task output
df_silver = spark.table("silver_sales")

print(f"📊 Aggregating {df_silver.count()} records from Task 2")

# Aggregation 1: Revenue by Category
df_category_metrics = df_silver.groupBy("category") \
    .agg(
        F.count("*").alias("order_count"),
        F.sum("amount").alias("total_revenue"),
        F.avg("amount").alias("avg_order_value"),
        F.max("amount").alias("max_order_value")
    ) \
    .orderBy(F.desc("total_revenue"))

print("\n📊 Revenue by Category:")
display(df_category_metrics)

# Aggregation 2: Revenue by Region
df_region_metrics = df_silver.groupBy("region") \
    .agg(
        F.count("*").alias("order_count"),
        F.sum("amount").alias("total_revenue")
    ) \
    .orderBy(F.desc("total_revenue"))

print("\n📊 Revenue by Region:")
display(df_region_metrics)

# Aggregation 3: Revenue Tier Distribution
df_tier_metrics = df_silver.groupBy("revenue_tier") \
    .agg(
        F.count("*").alias("order_count"),
        F.sum("amount").alias("total_revenue")
    ) \
    .orderBy(F.desc("total_revenue"))

print("\n📊 Revenue Tier Distribution:")
display(df_tier_metrics)

# Store aggregated results
df_category_metrics.createOrReplaceTempView("gold_category_metrics")
df_region_metrics.createOrReplaceTempView("gold_region_metrics")

print("\n" + "=" * 60)
print("✅ Task 3 Complete: Aggregated metrics stored in gold views")
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,Workflow Simulation Summary
# MAGIC %md
# MAGIC ### ✅ Workflow Simulation Complete!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **What just happened?**
# MAGIC
# MAGIC We simulated a 3-task workflow:
# MAGIC
# MAGIC 1. 📥 **Task 1 (Ingestion)**: Created sample sales data → `bronze_sales`
# MAGIC 2. ♻️ **Task 2 (Transformation)**: Applied business logic → `silver_sales`
# MAGIC 3. 📊 **Task 3 (Aggregation)**: Generated metrics → `gold_*_metrics`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **In a Real Databricks Job:**
# MAGIC
# MAGIC * Each task would be a **separate notebook or script**
# MAGIC * Tasks would run on **different clusters** (right-sized for the workload)
# MAGIC * **Dependencies** would be explicitly defined (Task 2 depends on Task 1)
# MAGIC * **Failure handling**: If Task 2 fails, Task 1 doesn't rerun
# MAGIC * **Parameters** would be passed from the job configuration
# MAGIC * **Results** would be stored in Delta tables (not temp views)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛠️ How to Convert This to a Real Job:
# MAGIC
# MAGIC 1. Split each task into separate notebooks
# MAGIC 2. Create a Databricks Job
# MAGIC 3. Add three tasks:
# MAGIC    * **Task 1**: Run ingestion notebook
# MAGIC    * **Task 2**: Run transformation notebook (depends on Task 1)
# MAGIC    * **Task 3**: Run aggregation notebook (depends on Task 2)
# MAGIC 4. Configure schedule (e.g., daily at 2 AM)
# MAGIC 5. Set parameters (catalog, schema, process_date)

# COMMAND ----------

# DBTITLE 1,Section 7: End-to-End Orchestrated Pipeline
# MAGIC %md
# MAGIC ## 🔄 Section 7: End-to-End Orchestrated Pipeline
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Enterprise Pipeline Architecture:
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────┐
# MAGIC │                    DATA SOURCES                          │
# MAGIC │  (Cloud Storage | Databases | APIs | Streaming)         │
# MAGIC └────────────────────────┴─────────────────────────────┘
# MAGIC                               │
# MAGIC                               ↓
# MAGIC          ┌──────────────────┴──────────────────┐
# MAGIC          │      INGESTION LAYER              │
# MAGIC          │  • Auto Loader / Copy Into       │
# MAGIC          │  • Schema Evolution             │
# MAGIC          │  • Data Validation              │
# MAGIC          └────────────────┬──────────────────┘
# MAGIC                          │
# MAGIC                          ↓
# MAGIC          ┌───────────────┴─────────────────┐
# MAGIC          │    BRONZE LAYER              │
# MAGIC          │  (Raw / Unprocessed)        │
# MAGIC          └────────────────┬────────────────┘
# MAGIC                          │
# MAGIC                          ↓
# MAGIC          ┌───────────────┴─────────────────┐
# MAGIC          │   TRANSFORMATION LAYER       │
# MAGIC          │  • Cleaning                  │
# MAGIC          │  • Business Logic           │
# MAGIC          │  • Enrichment               │
# MAGIC          └────────────────┬────────────────┘
# MAGIC                          │
# MAGIC                          ↓
# MAGIC          ┌───────────────┴─────────────────┐
# MAGIC          │    SILVER LAYER              │
# MAGIC          │  (Cleaned / Validated)      │
# MAGIC          └────────────────┬────────────────┘
# MAGIC                          │
# MAGIC               ┌──────────┴──────────┐
# MAGIC               │                     │
# MAGIC               ↓                     ↓
# MAGIC     ┌───────────────┐   ┌───────────────┐
# MAGIC     │ AGGREGATION 1 │   │ AGGREGATION 2 │
# MAGIC     │   (Metrics)   │   │   (Reports)   │
# MAGIC     └───────┬───────┘   └───────┬───────┘
# MAGIC             └───────────┬──────────┘
# MAGIC                         │
# MAGIC                         ↓
# MAGIC          ┌──────────────┴──────────────┐
# MAGIC          │     GOLD LAYER            │
# MAGIC          │  (Business-Ready)       │
# MAGIC          └──────────────┬──────────────┘
# MAGIC                         │
# MAGIC                         ↓
# MAGIC          ┌──────────────┴──────────────┐
# MAGIC          │   CONSUMPTION LAYER      │
# MAGIC          │  (BI Tools | ML Models) │
# MAGIC          └────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛠️ Mapping Pipeline to Job Tasks:
# MAGIC
# MAGIC | **Pipeline Stage** | **Job Task** | **Task Type** |
# MAGIC |---|---|---|
# MAGIC | Data Ingestion | `task_ingest` | Notebook |
# MAGIC | Bronze Layer | `task_bronze` | Notebook |
# MAGIC | Transformation | `task_transform` | Notebook |
# MAGIC | Silver Layer | `task_silver` | Notebook |
# MAGIC | Aggregation 1 | `task_agg_metrics` | SQL |
# MAGIC | Aggregation 2 | `task_agg_reports` | SQL |
# MAGIC | Gold Layer | `task_gold` | Notebook |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔗 Task Dependencies:
# MAGIC
# MAGIC ```python
# MAGIC task_ingest → task_bronze → task_transform → task_silver → [task_agg_metrics, task_agg_reports] → task_gold
# MAGIC ```
# MAGIC
# MAGIC **Note**: `task_agg_metrics` and `task_agg_reports` run **in parallel** after `task_silver` completes.

# COMMAND ----------

# DBTITLE 1,Genie Code Agent Usage Examples
# MAGIC %md
# MAGIC ## 🧞 Genie Code Agent Usage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💬 Example Prompts for Job & Workflow Creation:
# MAGIC
# MAGIC 1. **"Create a Databricks job that runs this notebook daily at 2 AM"**
# MAGIC    * Genie will create a job with cron schedule and link your notebook
# MAGIC
# MAGIC 2. **"Generate a parameterized pipeline with date and environment parameters"**
# MAGIC    * Genie will add widgets and parameterized code
# MAGIC
# MAGIC 3. **"Design a DAG-based ETL pipeline with ingestion, transformation, and aggregation tasks"**
# MAGIC    * Genie will create multi-task workflow structure
# MAGIC
# MAGIC 4. **"Schedule a production job that processes yesterday's data every morning"**
# MAGIC    * Genie will create job with dynamic date logic
# MAGIC
# MAGIC 5. **"Create a workflow with task dependencies: Task A → Task B → Task C"**
# MAGIC    * Genie will set up proper task sequencing
# MAGIC
# MAGIC 6. **"Add retry logic to my job: retry 3 times with 5-minute delays"**
# MAGIC    * Genie will configure retry policy
# MAGIC
# MAGIC 7. **"Create separate dev and prod job configurations"**
# MAGIC    * Genie will set up environment-specific parameters
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✨ Pro Tips:
# MAGIC
# MAGIC * Be specific about scheduling requirements
# MAGIC * Mention dependencies explicitly
# MAGIC * Specify parameter names and default values
# MAGIC * Include environment (dev/staging/prod) if relevant

# COMMAND ----------

# DBTITLE 1,Summary: Key Learnings
# MAGIC %md
# MAGIC ## 🎓 Summary: Key Learnings
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Core Concepts Covered:
# MAGIC
# MAGIC 1. **Jobs & Tasks**
# MAGIC    * Jobs orchestrate multiple tasks
# MAGIC    * Tasks are individual units of work
# MAGIC    * Tasks can be notebooks, scripts, SQL, or pipelines
# MAGIC
# MAGIC 2. **Workflow Orchestration (DAG)**
# MAGIC    * Directed Acyclic Graph defines task flow
# MAGIC    * Tasks have dependencies
# MAGIC    * Parallel execution improves performance
# MAGIC
# MAGIC 3. **Scheduling**
# MAGIC    * Time-based (cron expressions)
# MAGIC    * Trigger-based (event-driven)
# MAGIC    * Continuous (streaming)
# MAGIC
# MAGIC 4. **Parameterization**
# MAGIC    * Makes pipelines reusable and flexible
# MAGIC    * Use widgets for parameter input
# MAGIC    * Pass values between tasks using taskValues
# MAGIC
# MAGIC 5. **Production Best Practices**
# MAGIC    * Avoid hardcoding values
# MAGIC    * Use modular task design
# MAGIC    * Implement proper error handling
# MAGIC    * Monitor job runs and failures
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Key Takeaways:
# MAGIC
# MAGIC ✅ **Jobs replace manual notebook execution** in production  
# MAGIC ✅ **Workflows enable complex orchestration** with dependencies  
# MAGIC ✅ **Parameters make pipelines dynamic** and environment-agnostic  
# MAGIC ✅ **DAG structure optimizes execution** through parallelization  
# MAGIC ✅ **Scheduling automates data processing** without human intervention

# COMMAND ----------

# DBTITLE 1,Interview Questions
# MAGIC %md
# MAGIC ## 📝 Interview Questions: Jobs & Workflows
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 1. **What is the difference between a Job and a Task in Databricks?**
# MAGIC **Answer**: A Job is an automated execution unit that can contain one or more Tasks. A Task is an individual unit of work (notebook, script, SQL query). Jobs orchestrate multiple tasks with dependencies.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. **Explain what a DAG is and why it's important in workflow orchestration.**
# MAGIC **Answer**: A Directed Acyclic Graph (DAG) is a computational model where tasks flow in one direction without circular dependencies. It's important because it defines execution order, enables parallel processing, and prevents deadlocks.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. **How do you pass parameters to a Databricks notebook in a Job?**
# MAGIC **Answer**: Parameters can be passed through:
# MAGIC * Job configuration (key-value pairs)
# MAGIC * Widgets (`dbutils.widgets.text("param", "default")`)
# MAGIC * Task values (`dbutils.jobs.taskValues.set()` / `.get()`)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4. **What are the benefits of using parameterized pipelines?**
# MAGIC **Answer**: 
# MAGIC * Reusability across environments (dev/prod)
# MAGIC * Dynamic date range processing
# MAGIC * Easier testing with different inputs
# MAGIC * No hardcoded values
# MAGIC * Simplified configuration management
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5. **How do you handle task failures in a Databricks Job?**
# MAGIC **Answer**: 
# MAGIC * Configure retry policies (max retries, timeout)
# MAGIC * Set up email/webhook alerts
# MAGIC * Use try-except blocks in code
# MAGIC * Implement idempotent operations
# MAGIC * Monitor job run history
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6. **Explain the difference between sequential and parallel task execution.**
# MAGIC **Answer**: Sequential execution runs tasks one after another (Task A → Task B → Task C). Parallel execution runs independent tasks simultaneously after their dependencies complete, reducing total pipeline runtime.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 7. **When would you use a Notebook task vs a Python script task?**
# MAGIC **Answer**: 
# MAGIC * **Notebook task**: Development, interactive work, documentation alongside code
# MAGIC * **Python script task**: Production code, version control, CI/CD integration, modular libraries
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 8. **How do you schedule a job to run every day at 2 AM EST?**
# MAGIC **Answer**: Use cron expression: `0 0 2 * * ?` with timezone set to `America/New_York`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 9. **What is the medallion architecture (Bronze/Silver/Gold) in data pipelines?**
# MAGIC **Answer**: 
# MAGIC * **Bronze**: Raw, unprocessed data
# MAGIC * **Silver**: Cleaned, validated, conformed data
# MAGIC * **Gold**: Business-ready aggregated data for analytics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 10. **How do you pass data between tasks in a Databricks Job?**
# MAGIC **Answer**: 
# MAGIC * Write to Delta tables (recommended)
# MAGIC * Use task values for small metadata
# MAGIC * Store paths/references in task outputs
# MAGIC * Avoid passing large datasets directly

# COMMAND ----------

# DBTITLE 1,Common Mistakes to Avoid
# MAGIC %md
# MAGIC ## ⚠️ Common Mistakes to Avoid
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 1: Hardcoding Parameters
# MAGIC
# MAGIC **Bad**:
# MAGIC ```python
# MAGIC df = spark.read.parquet("/Volumes/prod/sales/data/2026-04-21/")
# MAGIC ```
# MAGIC
# MAGIC **Good**:
# MAGIC ```python
# MAGIC date = dbutils.widgets.get("process_date")
# MAGIC env = dbutils.widgets.get("environment")
# MAGIC df = spark.read.parquet(f"/Volumes/{env}/sales/data/{date}/")
# MAGIC ```
# MAGIC
# MAGIC **Impact**: Hardcoded values make code non-reusable and error-prone.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 2: Not Using Workflows for Orchestration
# MAGIC
# MAGIC **Bad**:
# MAGIC * Running notebooks manually
# MAGIC * Using `%run` to chain notebooks
# MAGIC * No dependency management
# MAGIC
# MAGIC **Good**:
# MAGIC * Create Databricks Jobs
# MAGIC * Define task dependencies
# MAGIC * Use proper orchestration
# MAGIC
# MAGIC **Impact**: Manual execution is error-prone, not scalable, and lacks monitoring.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 3: Poor Task Dependency Design
# MAGIC
# MAGIC **Bad**:
# MAGIC * All tasks depend on all other tasks
# MAGIC * Circular dependencies
# MAGIC * Overly sequential when parallelization is possible
# MAGIC
# MAGIC **Good**:
# MAGIC * Clear DAG structure
# MAGIC * Parallel tasks where independent
# MAGIC * Minimal dependencies
# MAGIC
# MAGIC **Impact**: Poor design increases runtime and creates fragile pipelines.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 4: Running Pipelines Manually in Production
# MAGIC
# MAGIC **Bad**:
# MAGIC * Engineer runs notebook every day at 2 AM
# MAGIC * No automation
# MAGIC * Human intervention required
# MAGIC
# MAGIC **Good**:
# MAGIC * Schedule job to run automatically
# MAGIC * Set up alerts for failures
# MAGIC * Monitor execution history
# MAGIC
# MAGIC **Impact**: Manual execution is unreliable and not sustainable.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 5: No Error Handling or Retry Logic
# MAGIC
# MAGIC **Bad**:
# MAGIC * Single failure stops entire pipeline
# MAGIC * No retry mechanism
# MAGIC * No alerting
# MAGIC
# MAGIC **Good**:
# MAGIC * Configure retry policies
# MAGIC * Implement idempotent operations
# MAGIC * Set up failure notifications
# MAGIC * Use try-except for known failure points
# MAGIC
# MAGIC **Impact**: Transient failures cause unnecessary pipeline failures.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 6: Improper Cluster Sizing
# MAGIC
# MAGIC **Bad**:
# MAGIC * Using large clusters for small tasks
# MAGIC * Using small clusters for heavy transformations
# MAGIC * Same cluster size for all tasks
# MAGIC
# MAGIC **Good**:
# MAGIC * Right-size clusters per task
# MAGIC * Use serverless when possible
# MAGIC * Scale up for heavy workloads
# MAGIC
# MAGIC **Impact**: Wastes resources or causes slow execution.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Mistake 7: Not Using Delta Tables for Intermediate Results
# MAGIC
# MAGIC **Bad**:
# MAGIC * Using temporary views between tasks
# MAGIC * Relying on in-memory data
# MAGIC
# MAGIC **Good**:
# MAGIC * Write to Delta tables
# MAGIC * Enable task re-runs without reprocessing upstream
# MAGIC * Maintain data lineage
# MAGIC
# MAGIC **Impact**: Tasks cannot be re-run independently; data persistence issues.

# COMMAND ----------

# DBTITLE 1,Best Practices Summary
# MAGIC %md
# MAGIC ## ✅ Data Engineering Best Practices
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Design Principles:
# MAGIC
# MAGIC 1. **Modular Task Design**
# MAGIC    * Each task has a single responsibility
# MAGIC    * Tasks are independently testable
# MAGIC    * Clear input/output contracts
# MAGIC
# MAGIC 2. **Parameterized Pipelines**
# MAGIC    * Use widgets for configuration
# MAGIC    * Support multiple environments
# MAGIC    * Dynamic date range processing
# MAGIC
# MAGIC 3. **Avoid Hardcoding**
# MAGIC    * No hardcoded paths
# MAGIC    * No hardcoded dates
# MAGIC    * No hardcoded credentials (use secrets)
# MAGIC
# MAGIC 4. **Use Workflows for Orchestration**
# MAGIC    * Create Databricks Jobs for production
# MAGIC    * Define explicit task dependencies
# MAGIC    * Leverage parallel execution
# MAGIC
# MAGIC 5. **Implement Error Handling**
# MAGIC    * Configure retry policies
# MAGIC    * Set up alerting
# MAGIC    * Log errors appropriately
# MAGIC    * Make operations idempotent
# MAGIC
# MAGIC 6. **Right-Size Resources**
# MAGIC    * Match cluster size to workload
# MAGIC    * Use serverless when possible
# MAGIC    * Optimize task-level compute
# MAGIC
# MAGIC 7. **Store Intermediate Results**
# MAGIC    * Write to Delta tables
# MAGIC    * Enable task re-runs
# MAGIC    * Maintain data lineage
# MAGIC
# MAGIC 8. **Monitor and Observe**
# MAGIC    * Track job run history
# MAGIC    * Set up alerts for failures
# MAGIC    * Monitor execution time trends
# MAGIC    * Log key metrics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Production-Ready Checklist:
# MAGIC
# MAGIC ☐ All parameters externalized via widgets  
# MAGIC ☐ Task dependencies clearly defined  
# MAGIC ☐ Retry policies configured  
# MAGIC ☐ Failure alerts set up  
# MAGIC ☐ Delta tables used for intermediate results  
# MAGIC ☐ Appropriate cluster sizes configured  
# MAGIC ☐ Scheduled (not manual execution)  
# MAGIC ☐ Tested with sample data  
# MAGIC ☐ Documentation provided  
# MAGIC ☐ Monitoring dashboards created

# COMMAND ----------

# DBTITLE 1,Optional: Retry & Failure Handling
# MAGIC %md
# MAGIC ## 🔄 Optional Add-On: Retry & Failure Handling
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC
# MAGIC Sometimes things fail for silly reasons (internet hiccup, server busy). Instead of giving up immediately, we try again a few times. Like knocking on a door — if nobody answers, wait a bit and knock again.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛠️ Retry Configuration in Databricks Jobs:
# MAGIC
# MAGIC When creating a job, you can configure:
# MAGIC
# MAGIC * **Max Retries**: How many times to retry (e.g., 3)
# MAGIC * **Retry Timeout**: How long to wait between retries (e.g., 300 seconds)
# MAGIC * **Retry Policy**: Linear or exponential backoff
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Retry Logic Example (In Code):
# MAGIC
# MAGIC ```python
# MAGIC import time
# MAGIC from pyspark.sql.utils import AnalysisException
# MAGIC
# MAGIC def read_with_retry(path, max_retries=3):
# MAGIC     for attempt in range(max_retries):
# MAGIC         try:
# MAGIC             df = spark.read.parquet(path)
# MAGIC             return df
# MAGIC         except AnalysisException as e:
# MAGIC             if attempt < max_retries - 1:
# MAGIC                 print(f"⚠️ Attempt {attempt + 1} failed. Retrying...")
# MAGIC                 time.sleep(5 * (attempt + 1))  # Exponential backoff
# MAGIC             else:
# MAGIC                 print(f"❌ All retries failed.")
# MAGIC                 raise e
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📧 Failure Alerting:
# MAGIC
# MAGIC **Email Notifications**:
# MAGIC * Configure in Job settings
# MAGIC * Specify recipients
# MAGIC * Choose events: failure, success, start
# MAGIC
# MAGIC **Webhook Notifications**:
# MAGIC * Integrate with Slack, PagerDuty, etc.
# MAGIC * Send custom payloads
# MAGIC * Enable real-time incident response
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✅ Idempotent Operations:
# MAGIC
# MAGIC **Definition**: An operation that produces the same result regardless of how many times it's executed.
# MAGIC
# MAGIC **Example**:
# MAGIC ```python
# MAGIC # Idempotent: Overwrite mode
# MAGIC df.write.mode("overwrite").saveAsTable("target_table")
# MAGIC
# MAGIC # NOT Idempotent: Append mode (duplicates data on retry)
# MAGIC df.write.mode("append").saveAsTable("target_table")
# MAGIC ```
# MAGIC
# MAGIC **Best Practice**: Design tasks to be idempotent so retries don't corrupt data.

# COMMAND ----------

# DBTITLE 1,Final Summary & Next Steps
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC # 🎉 Congratulations!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## You've Completed Phase 2 Day 9: Jobs & Workflows
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📚 What You Learned:
# MAGIC
# MAGIC ✅ Databricks Jobs and Tasks  
# MAGIC ✅ Workflow Orchestration with DAGs  
# MAGIC ✅ Scheduling Mechanisms (Cron, Trigger-based)  
# MAGIC ✅ Parameter Passing and Dynamic Pipelines  
# MAGIC ✅ End-to-End Pipeline Architecture  
# MAGIC ✅ Production Best Practices  
# MAGIC ✅ Error Handling and Retry Logic  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Next Steps:
# MAGIC
# MAGIC 1. **Practice**: Create your own multi-task workflow
# MAGIC 2. **Schedule**: Set up a scheduled job with parameters
# MAGIC 3. **Optimize**: Identify opportunities for parallel execution
# MAGIC 4. **Monitor**: Set up alerting for job failures
# MAGIC 5. **Scale**: Apply these concepts to real production pipelines
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Resources:
# MAGIC
# MAGIC * Databricks Jobs Documentation
# MAGIC * Workflow Orchestration Best Practices
# MAGIC * Scheduling Guide
# MAGIC * Parameter Passing Examples
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👋 Thank You!
# MAGIC
# MAGIC **Author**: TRRaveendra  
# MAGIC **Watermark**: @TRRaveendra  
# MAGIC **Platform**: Databricks  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC 🔥 **Ready to orchestrate production data pipelines!** 🔥