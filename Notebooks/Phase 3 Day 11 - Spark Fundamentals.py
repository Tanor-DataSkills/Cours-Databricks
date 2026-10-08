# Databricks notebook source
# DBTITLE 1,📘 Notebook Header
# MAGIC %md
# MAGIC # ⚡ Data Engineering Training — Phase 3 Day 11  
# MAGIC ## 🚀 Spark Fundamentals & Distributed Processing  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - Spark Architecture (Driver, Executors)  
# MAGIC - Distributed Processing Concepts  
# MAGIC - Spark Execution Flow (High-Level)  
# MAGIC - DataFrame-based Processing (Best Practice)  
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
# MAGIC Understand how Apache Spark works internally, including driver/executor architecture and distributed processing, using modern DataFrame-based approach.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ IMPORTANT ENGINEERING CONSTRAINTS:
# MAGIC - ✅ Use Databricks Serverless Compute
# MAGIC - ✅ DataFrame API only (NO RDDs)
# MAGIC - ✅ NO cache() / persist()
# MAGIC - ✅ NO /tmp or local storage
# MAGIC - ✅ Unity Catalog Volumes for all data access
# MAGIC - ✅ Distributed-first design

# COMMAND ----------

# DBTITLE 1,DataFrame Operations: SELECT
# DataFrame Operation 1: SELECT
# Choose specific columns from your dataset

from pyspark.sql.functions import col, upper, concat, lit

print("🔵 DataFrame Operation: SELECT\n")

# Basic select
print("1️⃣ Basic Column Selection:")
df_basic = df_sales.select("transaction_id", "customer_id", "amount", "country")
display(df_basic.limit(5))

# Select with expressions
print("\n2️⃣ Select with Expressions:")
df_transformed = df_sales.select(
    col("transaction_id"),
    col("amount").alias("original_amount"),
    (col("amount") * 1.1).alias("amount_with_tax"),
    upper(col("country")).alias("country_code"),
    concat(lit("CUST-"), col("customer_id")).alias("customer_code")
)
display(df_transformed.limit(5))

print("\n✅ All transformations executed in distributed fashion across executors!")

# COMMAND ----------

# DBTITLE 1,DataFrame Operations: FILTER
# DataFrame Operation 2: FILTER (WHERE)
# Filter rows based on conditions

from pyspark.sql.functions import col

print("🟬 DataFrame Operation: FILTER\n")

# Single condition
print("1️⃣ Single Condition Filter:")
df_electronics = df_sales.filter(col("product_category") == "Electronics")
print(f"   Electronics transactions: {df_electronics.count():,}")
display(df_electronics.limit(5))

# Multiple conditions (AND)
print("\n2️⃣ Multiple Conditions (AND):")
df_premium = df_sales.filter(
    (col("product_category") == "Electronics") & 
    (col("amount") > 700) & 
    (col("country") == "USA")
)
print(f"   Premium USA Electronics: {df_premium.count():,}")
display(df_premium.limit(5))

# Multiple conditions (OR)
print("\n3️⃣ Multiple Conditions (OR):")
df_luxury = df_sales.filter(
    (col("country") == "USA") | 
    (col("country") == "UK")
).filter(col("amount") > 800)
print(f"   Luxury purchases (USA/UK): {df_luxury.count():,}")

print("\n💡 Predicate Pushdown: Filters are applied as early as possible in the execution!")

# COMMAND ----------

# DBTITLE 1,DataFrame Operations: GROUPBY & AGGREGATION
# DataFrame Operation 3: GROUPBY & AGGREGATION
# Aggregate data by groups

from pyspark.sql.functions import sum, avg, count, min, max, round, countDistinct

print("📊 DataFrame Operation: GROUP BY & AGGREGATION\n")

# Single column grouping
print("1️⃣ Group by Product Category:")
df_category_stats = df_sales.groupBy("product_category").agg(
    count("*").alias("total_transactions"),
    countDistinct("customer_id").alias("unique_customers"),
    round(sum("amount"), 2).alias("total_revenue"),
    round(avg("amount"), 2).alias("avg_order_value"),
    round(min("amount"), 2).alias("min_purchase"),
    round(max("amount"), 2).alias("max_purchase")
).orderBy(col("total_revenue").desc())

display(df_category_stats)

# Multiple column grouping
print("\n2️⃣ Group by Multiple Columns (Country + Category):")
df_detailed_stats = df_sales.groupBy("country", "product_category").agg(
    count("*").alias("transactions"),
    round(avg("amount"), 2).alias("avg_value")
).orderBy("country", col("avg_value").desc())

display(df_detailed_stats.limit(10))

print("\n⚡ Shuffle Operation: Data is redistributed across executors during groupBy!")
print("   └─ All records for each group end up on the same executor")
print("   └─ This enables parallel aggregation")

# COMMAND ----------

# DBTITLE 1,🏭 Section 7: End-to-End Mini Pipeline
# MAGIC %md
# MAGIC # 🟬 SECTION 7: End-to-End Distributed ETL Pipeline
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Objective:
# MAGIC Build a complete distributed data pipeline that:
# MAGIC 1. **Extracts** data (read)  
# MAGIC 2. **Transforms** data (business logic)  
# MAGIC 3. **Loads** data (write to Delta table)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Pipeline Architecture:
# MAGIC
# MAGIC ```
# MAGIC 💾 SOURCE DATA
# MAGIC       │
# MAGIC       ↓
# MAGIC ┌─────┼─────┐
# MAGIC │  EXTRACT  │ ← Read data (distributed)
# MAGIC └─────┼─────┘
# MAGIC       │
# MAGIC       ↓
# MAGIC ┌─────┼────────────┐
# MAGIC │  TRANSFORM   │ ← Apply business logic (parallel)
# MAGIC │  - Filter    │
# MAGIC │  - Enrich    │
# MAGIC │  - Aggregate │
# MAGIC └─────┼────────────┘
# MAGIC       │
# MAGIC       ↓
# MAGIC ┌─────┼─────┐
# MAGIC │   LOAD     │ ← Write to Delta (distributed)
# MAGIC └─────┼─────┘
# MAGIC       │
# MAGIC       ↓
# MAGIC 📊 DELTA TABLE
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✨ Distributed Execution:
# MAGIC **Every step** in this pipeline executes in a **distributed manner** across all executors automatically!

# COMMAND ----------

# DBTITLE 1,Pipeline Step 1: Extract (Source Data)
# STEP 1: EXTRACT - Source Data
# In production, you'd read from Unity Catalog Volumes
# For this demo, we'll use our existing dataset

print("⚡ STEP 1: EXTRACT\n")
print("📂 Reading source data...")

# Simulate reading from a source (we already have df_sales)
df_source = df_sales

print(f"   ✔ Source records loaded: {df_source.count():,}")
print(f"   ✔ Data distributed across {df_source.rdd.getNumPartitions()} partitions")
print("\n🔍 Source Data Schema:")
df_source.printSchema()

print("\n✅ EXTRACT Complete - Data ready for transformation!")

# COMMAND ----------

# DBTITLE 1,Pipeline Step 2: Transform (Business Logic)
# STEP 2: TRANSFORM - Apply Business Logic
# All transformations execute in parallel across executors

from pyspark.sql.functions import when, col, current_timestamp, round

print("⚡ STEP 2: TRANSFORM\n")

# Transformation 1: Data Quality - Filter out invalid records
print("🔵 Transformation 1: Data Quality Checks")
df_cleaned = df_source.filter(
    (col("amount") > 0) &  # Valid amounts only
    (col("customer_id").isNotNull()) &  # No null customers
    (col("country").isNotNull())  # No null countries
)
print(f"   ✔ Valid records: {df_cleaned.count():,}")

# Transformation 2: Enrichment - Add business logic
print("\n🟬 Transformation 2: Data Enrichment")
df_enriched = df_cleaned.withColumn(
    "customer_segment",
    when(col("amount") > 800, "Premium")
    .when(col("amount") > 400, "Standard")
    .otherwise("Basic")
).withColumn(
    "region",
    when(col("country").isin("USA", "Canada"), "North America")
    .when(col("country").isin("UK", "Germany", "France"), "Europe")
    .otherwise("Other")
).withColumn(
    "tax_amount",
    round(col("amount") * 0.1, 2)
).withColumn(
    "total_amount",
    round(col("amount") + col("tax_amount"), 2)
).withColumn(
    "processed_timestamp",
    current_timestamp()
)

print("   ✔ Data enriched with segment, region, tax calculations")
print("\n🔍 Enriched Data Sample:")
display(df_enriched.limit(5))

# Transformation 3: Aggregation - Business metrics
print("\n📊 Transformation 3: Calculate Business Metrics")
df_metrics = df_enriched.groupBy("region", "customer_segment", "product_category").agg(
    count("*").alias("transaction_count"),
    countDistinct("customer_id").alias("unique_customers"),
    round(sum("amount"), 2).alias("total_revenue"),
    round(sum("tax_amount"), 2).alias("total_tax"),
    round(avg("amount"), 2).alias("avg_transaction_value")
).orderBy(col("total_revenue").desc())

print("   ✔ Metrics calculated and aggregated")
print("\n📈 Top Business Metrics:")
display(df_metrics.limit(10))

print("\n✅ TRANSFORM Complete - Data ready for loading!")

# COMMAND ----------

# DBTITLE 1,Pipeline Step 3: Load (Write to Delta)
# STEP 3: LOAD - Write to Delta Table
# Delta Lake provides ACID transactions and time travel

print("⚡ STEP 3: LOAD\n")

# Define output path (in production, use Unity Catalog table)
output_table = "spark_fundamentals_metrics"

print(f"💾 Writing data to Delta table: {output_table}")

# Write enriched data to Delta (distributed write)
df_enriched.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(output_table)

print(f"   ✔ Enriched data written successfully")

# Write aggregated metrics to another Delta table
metrics_table = "spark_fundamentals_summary"

print(f"\n📊 Writing metrics to Delta table: {metrics_table}")

df_metrics.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(metrics_table)

print(f"   ✔ Metrics written successfully")

print("\n✅ LOAD Complete - Pipeline execution finished!")
print("\n" + "="*60)
print("🎉 END-TO-END PIPELINE EXECUTED SUCCESSFULLY!")
print("="*60)
print("\n💡 Key Points:")
print("   • All operations executed in DISTRIBUTED fashion")
print("   • Driver orchestrated, Executors processed in parallel")
print("   • Data was partitioned and processed across the cluster")
print("   • Shuffles happened automatically during groupBy operations")
print("   • Delta format provides ACID guarantees and optimizations")
print("   • You didn't manage any of this manually - Spark did it! ⚡")

# COMMAND ----------

# DBTITLE 1,Verify Pipeline Output
# Verify Pipeline Output
# Read back the Delta tables to confirm success

print("🔍 Verifying Pipeline Output\n")

# Verify enriched data table
print("1️⃣ Enriched Data Table:")
df_verify_enriched = spark.read.table("spark_fundamentals_metrics")
print(f"   ✔ Total records: {df_verify_enriched.count():,}")
print(f"   ✔ Columns: {len(df_verify_enriched.columns)}")
display(df_verify_enriched.limit(5))

# Verify metrics table
print("\n2️⃣ Summary Metrics Table:")
df_verify_metrics = spark.read.table("spark_fundamentals_summary")
print(f"   ✔ Total metric rows: {df_verify_metrics.count()}")
display(df_verify_metrics)

print("\n✅ Pipeline verification complete! All tables are accessible and queryable.")

# COMMAND ----------

# DBTITLE 1,🤖 Genie Code Agent Usage Examples
# MAGIC %md
# MAGIC # 🤖 Working with Databricks Genie Code Agent
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💬 Example Prompts for Spark Fundamentals:
# MAGIC
# MAGIC You can use natural language to work with Spark. Here are some example prompts:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟢 Understanding Spark Architecture:
# MAGIC
# MAGIC * **Prompt:** "Explain the difference between Spark driver and executors"  
# MAGIC * **Prompt:** "How does Spark distribute data across a cluster?"  
# MAGIC * **Prompt:** "What happens during a shuffle operation in Spark?"  
# MAGIC * **Prompt:** "Show me how to check partition count of a DataFrame"  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟬 Distributed Processing:
# MAGIC
# MAGIC * **Prompt:** "Create a distributed dataset with 1 million records"  
# MAGIC * **Prompt:** "Demonstrate parallel processing with groupBy aggregation"  
# MAGIC * **Prompt:** "Simulate a distributed ETL pipeline with transformations"  
# MAGIC * **Prompt:** "Repartition my DataFrame to optimize performance"  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 DataFrame Operations:
# MAGIC
# MAGIC * **Prompt:** "Filter records where amount is greater than 500"  
# MAGIC * **Prompt:** "Group sales data by country and calculate total revenue"  
# MAGIC * **Prompt:** "Join two DataFrames on customer_id"  
# MAGIC * **Prompt:** "Add a new column with conditional logic using when()"  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚡ Performance Optimization:
# MAGIC
# MAGIC * **Prompt:** "Optimize my Spark query for better performance"  
# MAGIC * **Prompt:** "Explain the execution plan for my DataFrame operations"  
# MAGIC * **Prompt:** "How can I avoid shuffle operations?"  
# MAGIC * **Prompt:** "Show me best practices for partition sizing"  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💾 Data Engineering:
# MAGIC
# MAGIC * **Prompt:** "Build a distributed ETL pipeline to process CSV files"  
# MAGIC * **Prompt:** "Write this DataFrame to Delta Lake with partitioning"  
# MAGIC * **Prompt:** "Read data from Unity Catalog Volume and apply transformations"  
# MAGIC * **Prompt:** "Create a Delta table with schema evolution enabled"  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔑 Tips for Working with Genie:
# MAGIC
# MAGIC ✅ **Be Specific:** Include details about data sources, transformations, and output format  
# MAGIC ✅ **Ask for Explanations:** Request both code AND explanation of concepts  
# MAGIC ✅ **Iterate:** Start simple, then add complexity  
# MAGIC ✅ **Request Best Practices:** Ask for enterprise-ready, production-quality code  
# MAGIC ✅ **Learn Incrementally:** Build understanding step-by-step

# COMMAND ----------

# DBTITLE 1,Demo 1: Create Sample Distributed Dataset
# Demo 1: Create Sample Distributed Dataset
# Simulating a large dataset with multiple partitions

from pyspark.sql.functions import col, rand, expr, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Create a sample dataset with 100,000 records
print("⚡ Creating distributed dataset with 100,000 records...\n")

# Generate sample sales data
df_sales = spark.range(0, 100000) \
    .withColumn("transaction_id", col("id")) \
    .withColumn("customer_id", (rand() * 10000).cast("int")) \
    .withColumn("product_category", 
                expr("CASE WHEN rand() < 0.25 THEN 'Electronics' "
                     "WHEN rand() < 0.50 THEN 'Clothing' "
                     "WHEN rand() < 0.75 THEN 'Food' "
                     "ELSE 'Books' END")) \
    .withColumn("amount", (rand() * 1000).cast("double")) \
    .withColumn("country", 
                expr("CASE WHEN rand() < 0.4 THEN 'USA' "
                     "WHEN rand() < 0.7 THEN 'UK' "
                     "WHEN rand() < 0.85 THEN 'Germany' "
                     "ELSE 'France' END")) \
    .drop("id")

print("\u2705 Dataset created successfully!")
print(f"\n📊 Total Records: {df_sales.count():,}")
print("\n🔍 Sample Data:")
display(df_sales.limit(10))

# COMMAND ----------

# DBTITLE 1,Demo 2: Check Data Distribution (Partitions)
# Demo 2: Check Data Distribution (Partitions)
# Understanding how data is distributed across the cluster

print("🔍 Analyzing Data Distribution...\n")

# Check number of partitions
num_partitions = df_sales.rdd.getNumPartitions()
print(f"🍰 Number of Partitions: {num_partitions}")
print(f"   └─ This means data is split into {num_partitions} chunks")
print(f"   └─ Each partition can be processed by a different executor\n")

# Explain what this means
print("💡 What does this mean?")
print(f"   • Your 100,000 records are distributed across {num_partitions} partitions")
print(f"   • Each partition contains ~{100000 // num_partitions:,} records")
print(f"   • Spark can process {num_partitions} partitions in parallel")
print(f"   • With multiple executors, this enables true distributed processing!\n")

# Show partition information
print("📋 Partition Details:")
print("=" * 50)
for i in range(min(5, num_partitions)):  # Show first 5 partitions
    print(f"   Partition {i}: Can be processed by Executor {i % 4 + 1}")
if num_partitions > 5:
    print(f"   ... and {num_partitions - 5} more partitions")

# COMMAND ----------

# DBTITLE 1,Demo 3: Distributed Transformations
# Demo 3: Distributed Transformations
# These operations run in parallel across all executors

from pyspark.sql.functions import sum, avg, count, round

print("⚡ Applying Distributed Transformations...\n")

# Transformation 1: Filter (runs on each partition in parallel)
print("🔵 Transformation 1: FILTER")
print("   Operation: Keep only high-value transactions (> $500)")
df_high_value = df_sales.filter(col("amount") > 500)
high_value_count = df_high_value.count()
print(f"   Result: {high_value_count:,} high-value transactions found\n")

# Transformation 2: GroupBy + Aggregation (triggers shuffle)
print("🟬 Transformation 2: GROUP BY + AGGREGATION")
print("   Operation: Calculate total sales by country")
df_by_country = df_sales.groupBy("country").agg(
    count("*").alias("transaction_count"),
    round(sum("amount"), 2).alias("total_sales"),
    round(avg("amount"), 2).alias("avg_transaction_value")
).orderBy(col("total_sales").desc())

print("   Result: Sales aggregated by country")
display(df_by_country)

print("\n💡 Note: The groupBy() operation triggered a SHUFFLE")
print("   └─ Data was redistributed across executors by country")
print("   └─ This ensures all records for each country are on the same executor")

# COMMAND ----------

# DBTITLE 1,Demo 4: Multi-Dimensional Analysis
# Demo 4: Multi-Dimensional Analysis
# Complex distributed processing

print("📊 Multi-Dimensional Analysis: Category Performance by Country\n")

# Analyze sales by both category and country
df_category_country = df_sales.groupBy("country", "product_category").agg(
    count("*").alias("transactions"),
    round(sum("amount"), 2).alias("revenue"),
    round(avg("amount"), 2).alias("avg_order_value")
).orderBy("country", col("revenue").desc())

print("✅ Analysis Complete!")
print("\n🔍 Top Performing Categories by Country:")
display(df_category_country)

print("\n⚡ Behind the Scenes:")
print("   • Driver split this job into multiple stages")
print("   • Executors processed partitions in parallel")
print("   • Data was shuffled and aggregated across the cluster")
print("   • Final results were collected by the driver")
print("   • All of this happened automatically! 🎉")

# COMMAND ----------

# DBTITLE 1,📚 Section 6: DataFrame API Best Practices
# MAGIC %md
# MAGIC # 🟬 SECTION 6: DataFrame API (Best Practice)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❓ Why DataFrames Over RDDs?
# MAGIC
# MAGIC | Feature | RDD (Old Way) | DataFrame (Modern Way) |
# MAGIC |---------|---------------|------------------------|
# MAGIC | **API** | Low-level, complex | High-level, SQL-like |
# MAGIC | **Optimization** | Manual | Automatic (Catalyst Optimizer) |
# MAGIC | **Performance** | Slower | 2-10x faster |
# MAGIC | **Code Readability** | Harder to understand | Easy to read/maintain |
# MAGIC | **Schema** | No schema | Strongly typed schema |
# MAGIC | **Language** | Python/Scala/Java | Python/Scala/Java/SQL/R |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Catalyst Optimizer (High-Level):
# MAGIC
# MAGIC **What is it?**  
# MAGIC Spark's **query optimizer** that automatically improves your code
# MAGIC
# MAGIC **What does it do?**
# MAGIC ✅ **Predicate Pushdown** - Filters data as early as possible  
# MAGIC ✅ **Column Pruning** - Only reads columns you need  
# MAGIC ✅ **Constant Folding** - Pre-calculates constant expressions  
# MAGIC ✅ **Join Optimization** - Chooses best join strategy  
# MAGIC
# MAGIC **Example:**
# MAGIC ```python
# MAGIC # Your code:
# MAGIC df.select("col1", "col2", "col3").filter(col("col1") > 100)
# MAGIC
# MAGIC # Catalyst optimizes to:
# MAGIC # 1. Read only col1, col2, col3 (not all columns)
# MAGIC # 2. Apply filter while reading (predicate pushdown)
# MAGIC # 3. Result: Much faster!
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔧 Tungsten Engine (High-Level):
# MAGIC
# MAGIC **What is it?**  
# MAGIC Spark's **execution engine** for memory and CPU optimization
# MAGIC
# MAGIC **Benefits:**
# MAGIC ✅ **Binary Processing** - Works directly with bytes, not objects  
# MAGIC ✅ **Memory Management** - Efficient memory layout  
# MAGIC ✅ **Code Generation** - Generates optimized bytecode at runtime  
# MAGIC
# MAGIC **Result:** 5-10x faster execution!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 DataFrame API: Core Operations
# MAGIC
# MAGIC Let's explore essential DataFrame operations

# COMMAND ----------

# DBTITLE 1,⏱️ Section 4: Spark Execution Flow
# MAGIC %md
# MAGIC # 🟡 SECTION 4: Spark Execution Flow (High-Level)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Execution Hierarchy:
# MAGIC
# MAGIC Spark organizes work into a **3-level hierarchy**:
# MAGIC
# MAGIC ```
# MAGIC 📋 JOB
# MAGIC    │
# MAGIC    ├── 📂 STAGE 1
# MAGIC    │     ├── 🎯 Task 1
# MAGIC    │     ├── 🎯 Task 2
# MAGIC    │     └── 🎯 Task N
# MAGIC    │
# MAGIC    └── 📂 STAGE 2
# MAGIC          ├── 🎯 Task 1
# MAGIC          ├── 🎯 Task 2
# MAGIC          └── 🎯 Task N
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1️⃣ **JOB** 📋
# MAGIC
# MAGIC **Definition:** A **Job** is triggered by an **action** (like `.count()`, `.show()`, `.write()`)
# MAGIC
# MAGIC **Example:**
# MAGIC ```python
# MAGIC df.filter(...).groupBy(...).count()  # ← This .count() triggers a JOB
# MAGIC ```
# MAGIC
# MAGIC ⚡ **Key Point:** Transformations alone don't trigger jobs!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2️⃣ **STAGE** 📂
# MAGIC
# MAGIC **Definition:** A **Stage** is a set of tasks that can run in parallel without shuffling data
# MAGIC
# MAGIC **Why Multiple Stages?**
# MAGIC - Operations like `groupBy()`, `join()` require **shuffling** data across executors
# MAGIC - Spark creates a **new stage** at each shuffle boundary
# MAGIC
# MAGIC **Example:**
# MAGIC ```python
# MAGIC df.filter(...)      # Stage 1: No shuffle
# MAGIC   .groupBy(...)     # ← SHUFFLE! New stage needed
# MAGIC   .agg(...)         # Stage 2: Aggregation
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3️⃣ **TASK** 🎯
# MAGIC
# MAGIC **Definition:** A **Task** is the smallest unit of work, executed on **one partition**
# MAGIC
# MAGIC **Formula:**
# MAGIC ```
# MAGIC Number of Tasks = Number of Partitions
# MAGIC ```
# MAGIC
# MAGIC **Example:**
# MAGIC - Dataset has **100 partitions**
# MAGIC - Stage will have **100 tasks** (one per partition)
# MAGIC - If you have **10 executors**, each processes ~10 tasks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🐢 Lazy Evaluation:
# MAGIC
# MAGIC **Concept:** Spark doesn't execute transformations immediately
# MAGIC
# MAGIC ### 🔵 Transformations (Lazy):
# MAGIC ```python
# MAGIC df2 = df.filter(...)     # ❌ Not executed yet
# MAGIC df3 = df2.select(...)    # ❌ Not executed yet
# MAGIC df4 = df3.groupBy(...)   # ❌ Not executed yet
# MAGIC ```
# MAGIC
# MAGIC ### 🟢 Actions (Eager):
# MAGIC ```python
# MAGIC df4.count()              # ✅ NOW everything executes!
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🤔 Why Lazy Evaluation?
# MAGIC
# MAGIC ✅ **Optimization:** Spark can optimize the entire plan before execution  
# MAGIC ✅ **Efficiency:** Combines multiple operations into fewer passes  
# MAGIC ✅ **Performance:** Avoids unnecessary computations  
# MAGIC
# MAGIC **Example Optimization:**
# MAGIC ```python
# MAGIC df.select("col1", "col2", "col3", "col4") \
# MAGIC   .filter(col("col1") > 100) \
# MAGIC   .select("col1", "col2")  # Spark optimizes: only reads col1, col2!
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Execution Flow Example:
# MAGIC
# MAGIC ```python
# MAGIC # CODE:
# MAGIC df = spark.read.csv("/path/to/data")
# MAGIC result = df.filter(col("age") > 25) \
# MAGIC            .groupBy("country") \
# MAGIC            .count() \
# MAGIC            .show()  # ← ACTION triggers execution
# MAGIC ```
# MAGIC
# MAGIC **Execution Breakdown:**
# MAGIC ```
# MAGIC 1. Driver receives code
# MAGIC    ↓
# MAGIC 2. Driver builds execution plan
# MAGIC    ↓
# MAGIC 3. Identifies shuffle at groupBy() → Creates 2 stages
# MAGIC    ↓
# MAGIC 4. STAGE 1: Read + Filter
# MAGIC    - Creates tasks (1 per partition)
# MAGIC    - Executors process in parallel
# MAGIC    ↓
# MAGIC 5. STAGE 2: GroupBy + Count
# MAGIC    - Shuffles data across executors
# MAGIC    - Executors aggregate in parallel
# MAGIC    ↓
# MAGIC 6. Driver collects results
# MAGIC    ↓
# MAGIC 7. show() displays output
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔑 Key Takeaways:
# MAGIC
# MAGIC * **Job** = Triggered by action  
# MAGIC * **Stage** = Group of tasks between shuffles  
# MAGIC * **Task** = Work on one partition  
# MAGIC * **Lazy Evaluation** = Optimize before execution  
# MAGIC
# MAGIC 🔴 **Remember:** We'll dive deeper into DAG and optimization in later sessions!

# COMMAND ----------

# DBTITLE 1,🛠️ Section 5: Hands-on Demo - Setup
# MAGIC %md
# MAGIC # 🟬 SECTION 5: Hands-on Demo (Distributed Processing)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Objective:
# MAGIC Demonstrate distributed processing concepts using real Spark operations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Demo Plan:
# MAGIC
# MAGIC 1. **Create sample dataset** (simulate distributed data)  
# MAGIC 2. **Check partition count** (understand data distribution)  
# MAGIC 3. **Apply transformations** (distributed operations)  
# MAGIC 4. **Verify distributed execution**  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚨 Note:
# MAGIC We'll create data programmatically since we're using serverless compute. In production, you'd read from Unity Catalog Volumes.

# COMMAND ----------

# DBTITLE 1,📚 Section 1: What is Apache Spark?
# MAGIC %md
# MAGIC # 🔵 SECTION 1: What is Apache Spark?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine you have a **huge LEGO castle** to build (millions of pieces).  
# MAGIC - **One person** building it = takes weeks 🐢  
# MAGIC - **100 people** building it together = takes hours ⚡  
# MAGIC
# MAGIC **Spark is like having 100 helpers** who work on different parts of your data at the same time!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Apache Spark** is a **distributed data processing engine** designed for:
# MAGIC
# MAGIC ✅ **Big Data Processing** - Handle datasets that don't fit on a single machine  
# MAGIC ✅ **In-Memory Computation** - 100x faster than disk-based processing (like MapReduce)  
# MAGIC ✅ **Unified Analytics** - Batch, streaming, SQL, ML in one framework  
# MAGIC ✅ **Fault Tolerance** - Automatic recovery from node failures  
# MAGIC ✅ **Scalability** - Process terabytes to petabytes of data  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 Why Do We Need Spark?
# MAGIC
# MAGIC | Challenge | Traditional System | Apache Spark |
# MAGIC |-----------|-------------------|---------------|
# MAGIC | **1 TB dataset** | Single machine crashes | Distributed across 100 machines |
# MAGIC | **Processing time** | Hours to days | Minutes to hours |
# MAGIC | **Scalability** | Vertical (bigger machine) | Horizontal (more machines) |
# MAGIC | **Cost** | Expensive single server | Commodity hardware cluster |
# MAGIC | **Fault tolerance** | Single point of failure | Automatic recovery |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Traditional vs Distributed Processing:
# MAGIC
# MAGIC **Traditional (Single-Node):**
# MAGIC ```
# MAGIC [💾 Database] → [💻 Single Server] → [📄 Results]
# MAGIC           ╰────────╯
# MAGIC           Limited by:
# MAGIC           - RAM size
# MAGIC           - CPU cores
# MAGIC           - Disk I/O
# MAGIC ```
# MAGIC
# MAGIC **Distributed (Spark):**
# MAGIC ```
# MAGIC [💾 Big Data] → [🧠 Driver] → Splits work → [💻 Executor 1] →
# MAGIC                                     └→ [💻 Executor 2] → Combine → [📄 Results]
# MAGIC                                     └→ [💻 Executor N] →
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,🏛️ Section 2: Spark Architecture
# MAGIC %md
# MAGIC # 🟢 SECTION 2: Spark Architecture
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧑‍🏫 Core Components:
# MAGIC
# MAGIC Spark has **3 main components**:
# MAGIC
# MAGIC ### 1️⃣ **Driver Program** 🧠
# MAGIC - **Role:** The "manager" or "orchestrator"  
# MAGIC - **Responsibilities:**
# MAGIC   - Analyzes your code (DataFrame operations)
# MAGIC   - Creates execution plan
# MAGIC   - Distributes tasks to executors
# MAGIC   - Monitors progress
# MAGIC   - Collects final results
# MAGIC   
# MAGIC **Think of it as:** Project Manager coordinating a construction team
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ **Executors** 💻
# MAGIC - **Role:** The "workers" or "processors"  
# MAGIC - **Responsibilities:**
# MAGIC   - Execute tasks assigned by driver
# MAGIC   - Store data in memory/disk
# MAGIC   - Return results to driver
# MAGIC   - Run in parallel across cluster
# MAGIC   
# MAGIC **Think of it as:** Construction workers building different parts
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ **Cluster Manager** 🏛️ (Conceptual)
# MAGIC - **Role:** Resource manager  
# MAGIC - **Examples:** 
# MAGIC   - Databricks (managed)
# MAGIC   - Kubernetes
# MAGIC   - YARN
# MAGIC   - Standalone
# MAGIC   
# MAGIC **Think of it as:** HR department allocating workers to projects
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Execution Flow:
# MAGIC
# MAGIC ```
# MAGIC ┌───────────────────────────────┐
# MAGIC │  👤 USER CODE (Your Notebook)  │
# MAGIC │   df.filter().groupBy()        │
# MAGIC └───────────┬───────────────────┘
# MAGIC             │
# MAGIC             ↓
# MAGIC ┌───────────┼───────────────────┐
# MAGIC │  🧠 DRIVER PROGRAM             │
# MAGIC │  - Analyzes code               │
# MAGIC │  - Creates execution plan      │
# MAGIC │  - Splits into tasks           │
# MAGIC └───────────┼───────────────────┘
# MAGIC             │
# MAGIC       Distributes Tasks
# MAGIC             │
# MAGIC     ┌───────┼───────┐
# MAGIC     │       │       │
# MAGIC     ↓       ↓       ↓
# MAGIC ┌───────┴─┐ ┌───┴───┐ ┌─┴───────┐
# MAGIC │ 💻      │ │ 💻    │ │ 💻      │
# MAGIC │ Executor│ │ Executor│ │ Executor│
# MAGIC │ 1       │ │ 2      │ │ N       │
# MAGIC └────┬────┘ └──┬────┘ └──┬──────┘
# MAGIC      │         │         │
# MAGIC      └─────────┼─────────┘
# MAGIC                │
# MAGIC           Results Back
# MAGIC                │
# MAGIC                ↓
# MAGIC       ┌────────────────┐
# MAGIC       │ 📄 FINAL RESULT │
# MAGIC       └────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Role Summary:
# MAGIC
# MAGIC | Component | Role | Analogy |
# MAGIC |-----------|------|----------|
# MAGIC | **Driver** | Orchestrates execution | Orchestra conductor |
# MAGIC | **Executors** | Process data in parallel | Orchestra musicians |
# MAGIC | **Cluster Manager** | Allocates resources | Concert hall manager |

# COMMAND ----------

# DBTITLE 1,🌐 Section 3: Distributed Processing
# MAGIC %md
# MAGIC # 🟬 SECTION 3: Distributed Processing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧩 What is Distributed Processing?
# MAGIC
# MAGIC **Distributed Processing** means splitting a large task into smaller pieces and processing them **simultaneously** across multiple machines.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Key Concepts:
# MAGIC
# MAGIC ### 1️⃣ **Data Partitioning** 🍰
# MAGIC
# MAGIC **Concept:** Split large dataset into smaller chunks (partitions)
# MAGIC
# MAGIC **Example:**  
# MAGIC You have **1 million records**:
# MAGIC
# MAGIC ```
# MAGIC Original Dataset (1M records):
# MAGIC [📦📦📦📦📦📦📦📦📦📦...] 
# MAGIC
# MAGIC After Partitioning (10 partitions):
# MAGIC Partition 1: [📦📦] (100K records) → Executor 1
# MAGIC Partition 2: [📦📦] (100K records) → Executor 2
# MAGIC Partition 3: [📦📦] (100K records) → Executor 3
# MAGIC ...
# MAGIC Partition 10: [📦📦] (100K records) → Executor 10
# MAGIC
# MAGIC Result: 10x faster processing!
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ **Parallel Execution** ⚡
# MAGIC
# MAGIC **Concept:** Multiple executors work on different partitions **at the same time**
# MAGIC
# MAGIC ```
# MAGIC Sequential (1 executor):
# MAGIC [████████████████████] 20 seconds
# MAGIC
# MAGIC Parallel (4 executors):
# MAGIC Executor 1: [█████] 5 seconds
# MAGIC Executor 2: [█████] 5 seconds
# MAGIC Executor 3: [█████] 5 seconds
# MAGIC Executor 4: [█████] 5 seconds
# MAGIC
# MAGIC Result: 4x speedup!
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ **Task Execution** 🎯
# MAGIC
# MAGIC **Concept:** Each executor runs **tasks** on its assigned partition
# MAGIC
# MAGIC **Example Task Flow:**
# MAGIC ```
# MAGIC Task = filter() + groupBy() + count()
# MAGIC
# MAGIC Executor 1:
# MAGIC   Input: Partition 1 (100K records)
# MAGIC   Task: Apply filter + groupBy + count
# MAGIC   Output: Partial result 1
# MAGIC
# MAGIC Executor 2:
# MAGIC   Input: Partition 2 (100K records)
# MAGIC   Task: Apply filter + groupBy + count
# MAGIC   Output: Partial result 2
# MAGIC
# MAGIC ...
# MAGIC
# MAGIC Driver:
# MAGIC   Combines all partial results
# MAGIC   Returns final aggregated result
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛡️ Fault Tolerance:
# MAGIC
# MAGIC **What if an executor fails?**
# MAGIC
# MAGIC ✅ Spark **automatically retries** the task on another executor  
# MAGIC ✅ Uses **lineage information** (tracks how data was created)  
# MAGIC ✅ No data loss!  
# MAGIC
# MAGIC ```
# MAGIC Executor 2 crashes ❌
# MAGIC ↓
# MAGIC Driver detects failure
# MAGIC ↓
# MAGIC Reassigns Partition 2 to Executor 5 ✅
# MAGIC ↓
# MAGIC Processing continues
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔑 Key Takeaway:
# MAGIC
# MAGIC **Distributed Processing = Divide + Conquer + Combine**
# MAGIC
# MAGIC 1. **Divide:** Split data into partitions
# MAGIC 2. **Conquer:** Process partitions in parallel
# MAGIC 3. **Combine:** Merge results