# Databricks notebook source
# DBTITLE 1,Notebook Header (Mandatory)
# MAGIC %md
# MAGIC # ⚡ Data Engineering Training — Phase 3 Day 17
# MAGIC ## 🚀 Advanced Spark Concepts: Shuffle, Transformations & UDF Pitfalls
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - Shuffle Operations  
# MAGIC - Narrow vs Wide Transformations  
# MAGIC - UDF Pitfalls & Alternatives  
# MAGIC - Performance Optimization Basics  
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
# MAGIC Understand advanced Spark execution concepts such as shuffle, transformation types, and why UDFs can degrade performance, along with best practices to optimize pipelines.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Section 1 — What is Shuffle?
# MAGIC %md
# MAGIC ## Section 1 — What is Shuffle?
# MAGIC
# MAGIC **ELI5 Explanation:**
# MAGIC - Shuffle is like rearranging all your toys between boxes. Spark moves data between computer nodes (partitions) to group or join it, which takes time and effort.
# MAGIC
# MAGIC **Architect-level Explanation:**
# MAGIC - Shuffle occurs when Spark must redistribute data across partitions—usually for wide transformations such as `groupBy`, `join`, or `distinct`. This process involves sorting and moving data via network/disk, increasing latency and resource usage.
# MAGIC
# MAGIC **Why Shuffle is Expensive:**
# MAGIC - Significant network I/O as data is transferred across the cluster
# MAGIC - Disk I/O when intermediate data is written/read
# MAGIC - Can cause straggler tasks and uneven workload
# MAGIC
# MAGIC **Examples That Cause Shuffle:**
# MAGIC - `groupBy()` — Spark groups data by key, which requires moving similar keys together
# MAGIC - `join()` — Spark must pair matching rows, which necessitates partition realignment
# MAGIC
# MAGIC **Impact:**
# MAGIC - Increased execution time
# MAGIC - More memory/disk consumption
# MAGIC - Higher risk of bottlenecks and failures
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Section 2 — Narrow vs Wide Transformations
# MAGIC %md
# MAGIC ## Section 2 — Narrow vs Wide Transformations
# MAGIC
# MAGIC **ELI5 Explanation:**
# MAGIC - Narrow transformations: Only work with what’s already in each box (partition); nothing is moved.
# MAGIC - Wide transformations: Need to reorganize what’s in all boxes; lots of data gets moved around.
# MAGIC
# MAGIC **Architect-level Explanation:**
# MAGIC - Narrow transformations operate only on data within each partition (e.g. `select`, `filter`). These are fast and avoid network/disk overhead.
# MAGIC - Wide transformations require Spark to shuffle data between partitions (e.g. `groupBy`, `join`), causing expensive operations and potential bottlenecks.
# MAGIC
# MAGIC | Feature              | Narrow            | Wide                |
# MAGIC |---------------------|-------------------|---------------------|
# MAGIC | Data movement       | No                | Yes (Shuffle)       |
# MAGIC | Example operations  | select, filter    | groupBy, join       |
# MAGIC | Performance impact  | Minimal           | High                |
# MAGIC | Resource usage      | Low               | High                |
# MAGIC | Scalability         | Easy              | Challenging         |
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Section 3 — Identifying Shuffle in Code
# MAGIC %md
# MAGIC ## Section 3 — Identifying Shuffle in Code
# MAGIC
# MAGIC **Code Example:**
# MAGIC ```python
# MAGIC from pyspark.sql.functions import sum
# MAGIC
# MAGIC df.groupBy("category").agg(sum("amount"))
# MAGIC ```
# MAGIC
# MAGIC **ELI5 Explanation:**
# MAGIC - Whenever you group things (like toys) by color, you need to find all same-colored toys from all boxes and reorganize. That’s shuffle.
# MAGIC
# MAGIC **Architect-level Explanation:**
# MAGIC - `groupBy` triggers a shuffle because Spark must collect all values of each key (`category`) across partitions. Data is redistributed, sorted, and aggregated, causing substantial network and disk activity.
# MAGIC
# MAGIC **How to Identify Shuffle:**
# MAGIC - Operations like `groupBy`, `join`, `distinct`, or sorting by non-partition columns typically invoke a shuffle. Check the physical plan (explain) for "Exchange" or "Shuffle" markers.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Section 4 — UDF (User Defined Function) Pitfalls
# MAGIC %md
# MAGIC ## Section 4 — UDF (User Defined Function) Pitfalls
# MAGIC
# MAGIC **ELI5 Explanation:**
# MAGIC - Using your own recipes (UDFs) in Spark is like asking each computer to cook something by itself; it takes extra time and is less efficient than using built-in shortcuts.
# MAGIC
# MAGIC **Architect-level Explanation:**
# MAGIC - UDFs allow custom logic in Spark, but they bypass Catalyst optimizer, leading to slower execution. UDFs require more serialization/deserialization between JVM and Python, increasing overhead and diminishing performance. Not recommended unless absolutely necessary.
# MAGIC
# MAGIC **Conceptual Demonstration:**
# MAGIC ```python
# MAGIC from pyspark.sql.functions import udf
# MAGIC
# MAGIC def custom_logic(value):
# MAGIC     return value * 2
# MAGIC
# MAGIC my_udf = udf(custom_logic)
# MAGIC df.withColumn("new_col", my_udf(df["amount"]))
# MAGIC ```
# MAGIC
# MAGIC **Pitfalls:**
# MAGIC - No query plan optimization
# MAGIC - High serialization overhead
# MAGIC - Poor scalability
# MAGIC

# COMMAND ----------

# DBTITLE 1,Section 5 — UDF Alternatives (Best Practice)
# MAGIC %md
# MAGIC ## Section 5 — UDF Alternatives (Best Practice)
# MAGIC
# MAGIC **ELI5 Explanation:**
# MAGIC - Using Spark’s built-in functions is like using ready-made shortcuts that make your job faster and easier.
# MAGIC
# MAGIC **Architect-level Explanation:**
# MAGIC - Built-in Spark SQL functions are fully optimized by Catalyst—the Spark query engine—enabling vectorized execution and efficient processing. These functions avoid Python serialization and are much faster than UDFs.
# MAGIC
# MAGIC **Demonstration:**
# MAGIC ```python
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC df.withColumn("new_col", col("amount") * 2)
# MAGIC ```
# MAGIC
# MAGIC **Best Practice:**
# MAGIC - Always use native functions like `col()`, `when()`, `sum()`, etc., for transformations.
# MAGIC - Avoid UDFs unless absolutely necessary.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Section 6 — Performance Optimization Techniques
# MAGIC %md
# MAGIC ## Section 6 — Performance Optimization Techniques
# MAGIC
# MAGIC **ELI5 Explanation:**
# MAGIC - Sort your toys before organizing—they’re easier to count and group. Do simple things first before doing heavy work.
# MAGIC
# MAGIC **Architect-level Explanation:**
# MAGIC - Optimize Spark pipelines by filtering early, minimizing wide transformations, and combining operations efficiently. Reduce shuffle by avoiding unnecessary groupBy or joins. Use built-in functions and exploit partitioning strategies when possible.
# MAGIC
# MAGIC **Pipeline Comparison:**
# MAGIC | Step                         | Inefficient Pipeline             | Optimized Pipeline             |
# MAGIC |------------------------------|----------------------------------|-------------------------------|
# MAGIC | Read                         | Raw, no filter                   | Early filtering               |
# MAGIC | Narrow Transformation        | After wide transformation        | Before wide transformation    |
# MAGIC | Wide Transformation          | Multiple groupBy/join            | Minimal, well-planned         |
# MAGIC | Output                       | High resource use                | Fast & resource-efficient     |
# MAGIC

# COMMAND ----------

# DBTITLE 1,Section 7 — Hands-on Performance Pipeline
# MAGIC %md
# MAGIC ## Section 7 — Hands-on Performance Pipeline
# MAGIC
# MAGIC **Step-by-step Pipeline:**
# MAGIC 1. Read Delta data from Unity Catalog Volume (serverless)
# MAGIC 2. Apply narrow transformations (filter, select)
# MAGIC 3. Apply wide transformation (groupBy)
# MAGIC 4. Observe performance impacts (shuffle)
# MAGIC
# MAGIC **Conceptual Impact:**
# MAGIC - Early filtering reduces shuffle footprint
# MAGIC - Wide transformation (groupBy) moves data across cluster
# MAGIC - Observe resource usage and explain plan for evidence of shuffle
# MAGIC
# MAGIC **Demo will use PySpark DataFrame API and display() for outputs.**
# MAGIC

# COMMAND ----------

# DBTITLE 1,Hands-on Demo: Shuffle Pipeline
# Section 7 Hands-on: Performance Pipeline Demo

from pyspark.sql.functions import col, sum, current_date, expr

# 1. Read Delta table from Unity Catalog (last 30 days)
df = spark.read.format("delta").table("workspace.default.spark_fundamentals_metrics")
df_recent = df.filter(col("processed_timestamp") >= current_date() - expr("INTERVAL 30 DAYS"))

# 2. Apply narrow transformations (filter for Electronics)
electronics_df = df_recent.filter(col("product_category") == "Electronics")

# 3. Wide transformation: groupBy region, aggregate total amount
region_summary = electronics_df.groupBy("region").agg(sum("amount").alias("total_sales"))

# 4. Display output (observe shuffle on groupBy)
display(region_summary)


# COMMAND ----------

# DBTITLE 1,Section 8 — End-to-End Optimized Pipeline
# MAGIC %md
# MAGIC ## Section 8 — End-to-End Optimized Pipeline
# MAGIC
# MAGIC **Design:**
# MAGIC ```
# MAGIC Source (Delta, Unity Catalog)
# MAGIC    ↓
# MAGIC Early Filter (processed_timestamp & product_category)
# MAGIC    ↓
# MAGIC Narrow Transform (select, derive columns)
# MAGIC    ↓
# MAGIC Wide Transform (groupBy & aggregate)
# MAGIC    ↓
# MAGIC Output (display results)
# MAGIC ```
# MAGIC
# MAGIC **Highlights:**
# MAGIC - Minimize shuffle by applying filters before wide transformations
# MAGIC - Use built-in functions, avoid UDFs
# MAGIC - Efficient use of compute and storage
# MAGIC
# MAGIC Next: hands-on showcase with clean PySpark DataFrame API code.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Optimized ETL Pipeline
# Section 8 Hands-on: Optimized Pipeline

from pyspark.sql.functions import col, sum, when, expr, current_date

# Read table, filter for last 30 days and validated categories
valid_categories = ["Books", "Clothing", "Electronics", "Food"]
df = spark.read.format("delta").table("workspace.default.spark_fundamentals_metrics")
df_filtered = df.filter(
    (col("processed_timestamp") >= current_date() - expr("INTERVAL 30 DAYS")) & 
    (col("product_category").isin(valid_categories))
)

# Narrow transform: add high_value flag
pipeline_df = df_filtered.withColumn("high_value", when(col("amount") > 1000, 1).otherwise(0))

# Wide transform: groupBy region & category, aggregate
result = pipeline_df.groupBy("region", "product_category") \
    .agg(
        sum("amount").alias("total_sales"),
        sum("high_value").alias("high_value_count")
    )

# Output
display(result)


# COMMAND ----------

# DBTITLE 1,Data Engineering Best Practices
# MAGIC %md
# MAGIC ## Section 9 — Data Engineering Best Practices
# MAGIC
# MAGIC - Avoid unnecessary shuffles (groupBy, join)
# MAGIC - Prefer narrow transformations (select, filter)
# MAGIC - Use built-in Spark SQL functions; avoid UDFs
# MAGIC - Optimize pipeline logic before scaling compute
# MAGIC - Leverage Unity Catalog Volumes and Delta format for scalable storage
# MAGIC - Validate categories and date ranges before analysis
# MAGIC

# COMMAND ----------

# DBTITLE 1,Genie Code Agent Usage Prompts
# MAGIC %md
# MAGIC ## Section 10 — Genie Code Agent Usage Prompts
# MAGIC
# MAGIC Examples for interacting with Genie Code Agent:
# MAGIC * Prompt → Optimize Spark pipeline
# MAGIC * Prompt → Identify shuffle operations
# MAGIC * Prompt → Replace UDF with native functions
# MAGIC * Prompt → Improve transformation performance
# MAGIC

# COMMAND ----------

# DBTITLE 1,Final Summary and Interview Prep
# MAGIC %md
# MAGIC ## Section 11 — Final Summary
# MAGIC
# MAGIC **Key Learnings:**
# MAGIC - Shuffle operations are expensive due to data movement across partitions
# MAGIC - Narrow transformations avoid shuffle, are more scalable
# MAGIC - Wide transformations (groupBy/join) trigger shuffle and higher resource usage
# MAGIC - UDFs slow down Spark pipelines; prefer built-in functions
# MAGIC - Filter early, optimize logic; avoid unnecessary wide operations
# MAGIC
# MAGIC **Interview Questions:**
# MAGIC 1. What is a shuffle in Spark and why is it costly?
# MAGIC 2. Explain the difference between narrow and wide transformations.
# MAGIC 3. How do UDFs impact Spark performance?
# MAGIC 4. Why are built-in functions preferred over UDFs?
# MAGIC 5. How do you minimize shuffle in a Spark pipeline?
# MAGIC 6. Give examples of narrow and wide transformations.
# MAGIC 7. What is Catalyst and how does it optimize queries?
# MAGIC 8. Explain the risks of groupBy on large datasets.
# MAGIC 9. What are best practices for scalable Spark data pipelines?
# MAGIC 10. How do you identify shuffle operations in code?
# MAGIC
# MAGIC **Common Mistakes:**
# MAGIC - Overusing groupBy (shuffle-heavy)
# MAGIC - Using UDF unnecessarily
# MAGIC - Ignoring transformation types
# MAGIC - Not optimizing pipeline before scaling or deploying
# MAGIC

# COMMAND ----------

# DBTITLE 1,Shuffle Diagram & Partitioning Strategies
# MAGIC %md
# MAGIC ## Section 12 — Shuffle Diagram & Partitioning Strategies (Optional)
# MAGIC
# MAGIC **Text-based Shuffle Diagram:**
# MAGIC ```
# MAGIC Partition 1      Partition 2      Partition 3
# MAGIC    |                |                |
# MAGIC    |                |                |
# MAGIC    v                v                v
# MAGIC Shuffle (data moves across partitions)
# MAGIC    |                |                |
# MAGIC    |                |                |
# MAGIC New Partitions    New Partitions    New Partitions
# MAGIC ```
# MAGIC
# MAGIC **Partitioning Strategies (Intro):**
# MAGIC - Use partitionBy when writing Delta tables to optimize read/write
# MAGIC - Choose partition columns that are queried frequently (e.g., date, region)
# MAGIC - Avoid too many small partitions; balance is key
# MAGIC - Repartition for aggregation to minimize shuffle skew
# MAGIC
# MAGIC