# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ⚡ Data Engineering Training — Phase 3 Day 18  
# MAGIC ## 🚀 Spark Performance Tuning: Partitioning, Joins & Data Skew  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - Partitioning Strategies  
# MAGIC - Broadcast Joins & Join Strategies  
# MAGIC - Data Skew Handling  
# MAGIC - Performance Optimization Best Practices  
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
# MAGIC Learn how to optimize Spark pipelines using partitioning strategies, efficient join techniques, and handling data skew in distributed environments.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔧 Engineering Constraints:
# MAGIC - ✅ Databricks Serverless Compute
# MAGIC - ✅ DataFrame API (No RDDs)
# MAGIC - ✅ Unity Catalog Volumes
# MAGIC - ✅ Delta Lake Format
# MAGIC - ❌ No cache/persist (conceptual only)
# MAGIC - ❌ No /tmp or local storage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Let's optimize Spark performance! 🚀**

# COMMAND ----------

# DBTITLE 1,Section 1: Partitioning in Spark
# MAGIC %md
# MAGIC # 📦 Section 1: Partitioning in Spark
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine you have 1000 candies to count. If you count them alone, it takes forever. But if you divide them into 10 bags and give each bag to a friend, everyone counts 100 candies at the same time. That's **partitioning**!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Partitioning** is the fundamental unit of parallelism in Apache Spark. It divides data into logical chunks (partitions) that can be processed independently across executor nodes.
# MAGIC
# MAGIC ### 🔑 Key Concepts:
# MAGIC
# MAGIC 1. **Partition = Unit of Parallelism**  
# MAGIC    - Each partition is processed by one task on one executor core  
# MAGIC    - More partitions = more parallelism (up to available cores)
# MAGIC
# MAGIC 2. **Default Partitioning**  
# MAGIC    - Spark auto-determines partition count based on:  
# MAGIC      - Data source (file size, block size)  
# MAGIC      - `spark.sql.shuffle.partitions` (default: 200)  
# MAGIC      - `spark.default.parallelism`
# MAGIC
# MAGIC 3. **Optimal Partition Size**  
# MAGIC    - **Too few partitions**: Underutilization of cluster resources  
# MAGIC    - **Too many partitions**: Excessive scheduling overhead  
# MAGIC    - **Rule of thumb**: 128 MB – 1 GB per partition  
# MAGIC    - **Partition count**: 2-4x number of executor cores
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚖️ Repartition vs Coalesce:
# MAGIC
# MAGIC | Feature | `repartition(n)` | `coalesce(n)` |
# MAGIC |---------|-----------------|---------------|
# MAGIC | **Purpose** | Increase/decrease partitions | Decrease partitions only |
# MAGIC | **Shuffle** | Full shuffle (expensive) | Minimal shuffle |
# MAGIC | **Data Distribution** | Even distribution | May be uneven |
# MAGIC | **Use Case** | Need balanced partitions | Optimize before write |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 When to Repartition:
# MAGIC
# MAGIC ✅ Before expensive operations (joins, aggregations)  
# MAGIC ✅ After filtering large datasets  
# MAGIC ✅ Before writing to optimize file count  
# MAGIC ❌ Avoid unnecessary repartitioning (adds shuffle overhead)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Next: Let's see partitioning in action!**

# COMMAND ----------

# DBTITLE 1,Demo: Understanding Default Partitions
# Create sample data
from pyspark.sql.functions import *

# Generate sample dataset
df = spark.range(0, 10000000).toDF("id") \
    .withColumn("value", (col("id") * 2) % 1000) \
    .withColumn("category", (col("id") % 10).cast("string"))

# Note: On serverless compute, we cannot directly access RDD API
# Partitioning happens automatically based on data size and cluster configuration

print(f"✅ Sample Data Created")
print(f"✅ Total Records: {df.count():,}")
print("💡 Spark automatically partitions data for parallel processing")

display(df.limit(10))

# COMMAND ----------

# DBTITLE 1,Demo: Repartitioning Strategy
# 🔄 Increase partitions for better parallelism
df_repartitioned = df.repartition(8)

print("✅ Repartitioned to 8 partitions")
print("💡 Impact: More tasks can run in parallel across executors")
print("💡 Data is distributed across 8 partitions for better parallelism\n")

# Repartition by column (hash partitioning)
df_hash_partitioned = df.repartition(8, "category")

print("✅ Hash Partitioned by 'category' column")
print("💡 All records with same category go to same partition")
print("💡 Useful for subsequent joins and aggregations on 'category'")

display(df_repartitioned.limit(10))

# COMMAND ----------

# DBTITLE 1,Demo: Coalesce for Optimization
# 🔽 Reduce partitions before writing (avoids small files)
df_coalesced = df.coalesce(2)

print("✅ Coalesced to 2 partitions")
print("💡 Use before write operations to control output file count")
print("⚡ Coalesce is faster than repartition (minimal shuffle)\n")

# Demonstrate partition distribution
print("🔍 Partition Distribution Check:")
partition_counts = df_coalesced.withColumn("partition_id", spark_partition_id()) \
    .groupBy("partition_id").count() \
    .orderBy("partition_id")

print("💡 Each partition processes a portion of the data")
display(partition_counts)

# COMMAND ----------

# DBTITLE 1,Section 2: Join Strategies in Spark
# MAGIC %md
# MAGIC # 🔗 Section 2: Join Strategies in Spark
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine matching students with their backpacks:
# MAGIC
# MAGIC 1. **Broadcast Join**: The teacher holds a small list of backpack colors and tells everyone at once (fast!)  
# MAGIC 2. **Shuffle Join**: Students move around the room to find their backpack (slow, lots of movement)  
# MAGIC 3. **Sort-Merge Join**: Students line up by ID, backpacks line up by ID, then match (organized but takes setup time)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### 🔑 Join Strategies:
# MAGIC
# MAGIC Spark automatically selects join strategies based on:
# MAGIC - Dataset sizes  
# MAGIC - Available memory  
# MAGIC - Statistics  
# MAGIC - Cost-based optimization (CBO)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Comprehensive Comparison:
# MAGIC
# MAGIC | Feature | Broadcast Join | Shuffle Hash Join | Sort-Merge Join |
# MAGIC |---------|----------------|-------------------|------------------|
# MAGIC | **Also Known As** | Map-side join | Hash join | Merge join |
# MAGIC | **Data Movement** | Small table copied to all nodes | Both tables shuffled | Both tables shuffled + sorted |
# MAGIC | **Best For** | Small table (<10MB default) | Medium datasets | Large-large joins |
# MAGIC | **Memory Usage** | Low (broadcast fits in memory) | High | Medium |
# MAGIC | **Network I/O** | Minimal | High (shuffle) | High (shuffle) |
# MAGIC | **Sort Required** | No | No | Yes |
# MAGIC | **Performance** | ⚡⚡⚡ Fastest | ⚡⚡ Moderate | ⚡ Slower |
# MAGIC | **Shuffle** | ❌ No shuffle | ✅ Full shuffle | ✅ Full shuffle |
# MAGIC | **Constraint** | One table must be small | None | Equi-joins only |
# MAGIC | **Spark Config** | `spark.sql.autoBroadcastJoinThreshold` | Automatic | Automatic |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 How Spark Chooses:
# MAGIC
# MAGIC ```
# MAGIC 1. Can one table fit in memory? → Broadcast Join
# MAGIC 2. Are both tables large? → Sort-Merge Join (default for large joins)
# MAGIC 3. No statistics available? → Sort-Merge Join (safest)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔍 Broadcast Join Deep Dive:
# MAGIC
# MAGIC **How it works:**
# MAGIC 1. Small table is collected to driver  
# MAGIC 2. Broadcasted to all executor nodes  
# MAGIC 3. Each partition of large table joins locally (no shuffle!)
# MAGIC
# MAGIC **Configuration:**
# MAGIC ```python
# MAGIC # Default threshold: 10 MB
# MAGIC spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
# MAGIC ```
# MAGIC
# MAGIC **When to use:**
# MAGIC ✅ Dimension tables (customers, products)  
# MAGIC ✅ Lookup tables  
# MAGIC ✅ Configuration/mapping tables  
# MAGIC ❌ Large fact tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Common Pitfalls:
# MAGIC
# MAGIC ❌ **Broadcasting large tables**: Causes OOM errors  
# MAGIC ❌ **Not broadcasting small tables**: Unnecessary shuffle overhead  
# MAGIC ❌ **Ignoring skew**: One partition takes forever  
# MAGIC ❌ **Cartesian joins**: Exponential explosion
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Next: Let's implement these join strategies!**

# COMMAND ----------

# DBTITLE 1,Section 3: Broadcast Join Optimization
# MAGIC %md
# MAGIC # 📡 Section 3: Broadcast Join Optimization
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 When to Use Broadcast Joins:
# MAGIC
# MAGIC ✅ **Small dimension table** + Large fact table  
# MAGIC ✅ **Lookup tables** (status codes, categories)  
# MAGIC ✅ **Reference data** (countries, products)  
# MAGIC ✅ **Table size** < 10 MB (configurable)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Performance Impact:
# MAGIC
# MAGIC **Without Broadcast:**
# MAGIC - Full shuffle of both tables  
# MAGIC - Network I/O overhead  
# MAGIC - Slower execution  
# MAGIC
# MAGIC **With Broadcast:**
# MAGIC - ⚡ 10-100x faster  
# MAGIC - ⚡ No shuffle overhead  
# MAGIC - ⚡ Reduced network I/O  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Let's see the difference!**

# COMMAND ----------

# DBTITLE 1,Setup: Create Large and Small Tables
from pyspark.sql.functions import *
from pyspark.sql.functions import broadcast

# Create a LARGE fact table (orders)
large_df = spark.range(0, 1000000).toDF("order_id") \
    .withColumn("customer_id", (col("order_id") % 1000).cast("int")) \
    .withColumn("amount", (col("order_id") % 500) + 100) \
    .withColumn("order_date", expr("date_add('2024-01-01', cast(rand() * 365 as int))"))

print(f"✅ Large Table (Orders): {large_df.count():,} records")

# Create a SMALL dimension table (customers)
small_df = spark.range(0, 1000).toDF("customer_id") \
    .withColumn("customer_name", concat(lit("Customer_"), col("customer_id"))) \
    .withColumn("segment", expr("case when customer_id % 3 = 0 then 'Premium' when customer_id % 3 = 1 then 'Standard' else 'Basic' end"))

print(f"\n✅ Small Table (Customers): {small_df.count():,} records")
print("\n💡 Large table = Fact table with transactions")
print("💡 Small table = Dimension table with customer details")

display(large_df.limit(5))
display(small_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Demo: Regular Join (Shuffle Join)
# 🐢 Regular join (Spark decides - likely shuffle join)
regular_join_df = large_df.join(small_df, "customer_id")

print("🔍 Regular Join Execution:")
print("💡 Without broadcast hint, Spark may shuffle both tables")
print("💡 This is less efficient for small dimension tables\n")

# Show query plan
print("📊 Query Execution Plan:")
regular_join_df.explain()

# Display sample
print("\n✅ Join completed - displaying sample results:")
display(regular_join_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Demo: Broadcast Join (Optimized)
# ⚡ Broadcast join (Explicit optimization)
broadcast_join_df = large_df.join(broadcast(small_df), "customer_id")

print("⚡ Broadcast Join Execution:")
print("💡 Small customer table is broadcasted to all executors")
print("💡 Large orders table stays distributed - NO shuffle!\n")

# Show query plan (notice "BroadcastHashJoin")
print("📊 Query Execution Plan:")
broadcast_join_df.explain()

print("\n💡 Notice: Look for 'BroadcastHashJoin' vs 'SortMergeJoin' in the plan")
print("⚡ Broadcast = NO SHUFFLE = 10-100x FASTER!")

# Display sample
print("\n✅ Optimized join completed - displaying results:")
display(broadcast_join_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Performance Comparison
# 📊 Compare strategies
print("⚖️ Join Strategy Comparison:\n")

print("🐢 Regular Join (Shuffle):")
print("  - Shuffles BOTH tables across network")
print("  - High network I/O")
print("  - Slower for small dimension tables")
print(f"  - Result: {regular_join_df.count():,} records\n")

print("⚡ Broadcast Join (Optimized):")
print("  - Small table sent to ALL executors once")
print("  - NO shuffle of large table")
print("  - 10-100x faster")
print(f"  - Result: {broadcast_join_df.count():,} records\n")

print("💡 Best Practice:")
print("  ✅ Always broadcast dimension tables < 10 MB")
print("  ✅ Use broadcast() function explicitly for predictability")
print("  ❌ Never broadcast large tables (causes OOM)")

# Verify both produce same results
print(f"\n✅ Results Match: {regular_join_df.count() == broadcast_join_df.count()}")

# COMMAND ----------

# DBTITLE 1,Advanced: Multiple Broadcasts
# 🌐 Multiple small table joins (all broadcasted)

# Create another small lookup table (product categories)
product_lookup = spark.createDataFrame([
    (1, "Electronics"), (2, "Clothing"), (3, "Home"), 
    (4, "Sports"), (5, "Books")
], ["category_id", "category_name"])

# Add category to orders
orders_with_category = large_df.withColumn("category_id", (col("order_id") % 5) + 1)

# Multiple broadcast joins
full_enriched_df = orders_with_category \
    .join(broadcast(small_df), "customer_id") \
    .join(broadcast(product_lookup), "category_id")

print("⚡ Multiple Broadcast Joins:")
print(f"✅ Result Count: {full_enriched_df.count():,}")
print("💡 Both dimension tables broadcasted - NO shuffle!")

display(full_enriched_df.select(
    "order_id", "customer_name", "segment", 
    "category_name", "amount", "order_date"
).limit(10))

# COMMAND ----------

# DBTITLE 1,Section 4: Data Skew Handling
# MAGIC %md
# MAGIC # ⚖️ Section 4: Data Skew Handling
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine 10 kids sharing candy:
# MAGIC - **Good**: Each kid gets 10 candies (even!)  
# MAGIC - **Bad (Skewed)**: One kid gets 90 candies, others get 1 each  
# MAGIC
# MAGIC The kid with 90 candies takes FOREVER to count, while others finish fast and wait. That's **data skew**!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### 🔑 What is Data Skew?
# MAGIC
# MAGIC **Data Skew** occurs when data is unevenly distributed across partitions, causing:
# MAGIC - One or few tasks process most of the data  
# MAGIC - Remaining tasks finish quickly and stay idle  
# MAGIC - Overall job time = time of slowest task (stragglers)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 Symptoms of Data Skew:
# MAGIC
# MAGIC ⚠️ Tasks complete at different rates (1 task takes 10x longer)  
# MAGIC ⚠️ Executor OOM errors on specific partitions  
# MAGIC ⚠️ Long-running stages in Spark UI  
# MAGIC ⚠️ Uneven shuffle read/write sizes  
# MAGIC ⚠️ High GC time on specific executors
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔍 Common Causes:
# MAGIC
# MAGIC 1. **Key Distribution Skew**  
# MAGIC    - Few keys dominate (e.g., 80% of users in one city)  
# MAGIC    - Hot keys in joins/aggregations
# MAGIC
# MAGIC 2. **Join Skew**  
# MAGIC    - One key appears in millions of records  
# MAGIC    - Cross join explosion
# MAGIC
# MAGIC 3. **Null Keys**  
# MAGIC    - Many null values grouped together  
# MAGIC
# MAGIC 4. **Time-based Skew**  
# MAGIC    - Recent data heavily skewed (yesterday vs last year)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚖️ Impact:
# MAGIC
# MAGIC | Metric | Normal | Skewed |
# MAGIC |--------|--------|--------|
# MAGIC | Task Duration | Even (~5 min) | Uneven (1 min to 2 hours) |
# MAGIC | Executor Utilization | High | Low (waiting) |
# MAGIC | Memory Usage | Balanced | Spikes (OOM) |
# MAGIC | Network I/O | Balanced | Concentrated |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛠️ Solutions:
# MAGIC
# MAGIC #### 1. **Salting** (Key Randomization)
# MAGIC - Add random suffix to skewed keys  
# MAGIC - Redistribute hot keys across partitions  
# MAGIC - Requires 2-phase aggregation
# MAGIC
# MAGIC #### 2. **Broadcast Join**
# MAGIC - If one side is small, broadcast it  
# MAGIC - Eliminates shuffle entirely
# MAGIC
# MAGIC #### 3. **Filtering Skewed Keys**
# MAGIC - Handle skewed keys separately  
# MAGIC - Process normal keys efficiently  
# MAGIC - Union results
# MAGIC
# MAGIC #### 4. **Adaptive Query Execution (AQE)**
# MAGIC - Spark 3.0+ feature  
# MAGIC - Automatically detects and handles skew  
# MAGIC - Enable: `spark.sql.adaptive.enabled = true`
# MAGIC
# MAGIC #### 5. **Increase Partitions**
# MAGIC - More partitions = smaller partition sizes  
# MAGIC - May help mild skew
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Next: Let's detect and fix data skew!**

# COMMAND ----------

# DBTITLE 1,Demo: Creating Skewed Dataset
from pyspark.sql.functions import *

# Create SKEWED data (80% of orders belong to customer_id = 999)
skewed_data = spark.range(0, 1000000).toDF("order_id") \
    .withColumn(
        "customer_id",
        when(col("order_id") % 5 != 0, lit(999))  # 80% go to customer 999
        .otherwise((col("order_id") % 100).cast("int"))  # 20% distributed
    ) \
    .withColumn("amount", (col("order_id") % 1000) + 50)

print("⚠️ Skewed Dataset Created:")
print(f"✅ Total Records: {skewed_data.count():,}\n")

# Detect skew
skew_analysis = skewed_data.groupBy("customer_id").count() \
    .orderBy(col("count").desc())

print("🔍 Skew Analysis (Top 10 Customers):")
display(skew_analysis.limit(10))

print("\n⚠️ Notice: Customer 999 has 800,000 records (80%)!")
print("⚠️ This will cause performance bottleneck in joins/aggregations")

# COMMAND ----------

# DBTITLE 1,Demo: Impact of Skew on Joins
# Create customer dimension table
customers = spark.range(0, 1000).toDF("customer_id") \
    .withColumn("customer_name", concat(lit("Customer_"), col("customer_id")))

# 🐢 Join with skewed data (will be slow due to skew)
print("🐢 Performing Join with Skewed Data...")
print("⚠️ One partition will process 800K records while others process ~2K each")
print("⚠️ This causes stragglers and poor performance\n")

skewed_join = skewed_data.join(customers, "customer_id")

print(f"✅ Join Result: {skewed_join.count():,} records")
print("\n💡 Check Spark UI: You'll see one task taking much longer!")

# Analyze partition distribution
partition_dist = skewed_join.withColumn("partition_id", spark_partition_id()) \
    .groupBy("partition_id").count() \
    .orderBy("partition_id")

print("\n📊 Partition Distribution (uneven):")
display(partition_dist)

# COMMAND ----------

# DBTITLE 1,Solution 1: Broadcast Join for Skew
# ⚡ Solution 1: Use broadcast join to eliminate skew
print("⚡ Solution 1: Broadcast Join")
print("💡 Broadcasting small customer table eliminates shuffle entirely\n")

optimized_join = skewed_data.join(broadcast(customers), "customer_id")

print(f"✅ Optimized Join Result: {optimized_join.count():,} records")
print("⚡ NO shuffle = NO skew impact!")
print("⚡ All partitions process locally without waiting\n")

# Verify partition distribution (should be even from source)
partition_dist_optimized = optimized_join.withColumn("partition_id", spark_partition_id()) \
    .groupBy("partition_id").count() \
    .orderBy("partition_id")

print("📊 Partition Distribution (better):")
display(partition_dist_optimized)

print("\n💡 Key Insight: Broadcast joins bypass skew in shuffle joins!")

# COMMAND ----------

# DBTITLE 1,Solution 2: Salting Technique (Conceptual)
# ⚡ Solution 2: Salting (for large-large joins)
print("⚡ Solution 2: Salting Technique")
print("💡 Add random suffix to hot keys to distribute load\n")

# Step 1: Add salt to skewed keys
salt_range = 10  # Use 10 salts

skewed_with_salt = skewed_data \
    .withColumn("salt", (rand() * salt_range).cast("int")) \
    .withColumn("salted_key", concat(col("customer_id"), lit("_"), col("salt")))

print("✅ Added salt to hot keys:")
display(skewed_with_salt.select("order_id", "customer_id", "salt", "salted_key").limit(10))

# Step 2: Explode customer table with all salt values
customers_salted = customers \
    .withColumn("salt_id", explode(array([lit(i) for i in range(salt_range)]))) \
    .withColumn("salted_key", concat(col("customer_id"), lit("_"), col("salt_id")))

print("\n✅ Exploded customer table with salts:")
print(f"Original Customers: {customers.count()}")
print(f"Salted Customers: {customers_salted.count()} (10x replication)")

# Step 3: Join on salted key
salted_join = skewed_with_salt.join(
    customers_salted.select("salted_key", "customer_name"), 
    "salted_key"
)

print(f"\n✅ Salted Join Result: {salted_join.count():,} records")
print("⚡ Hot key (999) now distributed across 10 partitions!")
print("💡 Each partition processes \u007e80K instead of 800K")

# Verify distribution
salt_distribution = salted_join.groupBy("salt").count().orderBy("salt")
print("\n📊 Salt Distribution:")
display(salt_distribution)

# COMMAND ----------

# DBTITLE 1,Solution 3: Filter and Union Pattern
# ⚡ Solution 3: Handle hot keys separately
print("⚡ Solution 3: Filter and Union Pattern")
print("💡 Process hot keys separately with broadcast\n")

# Identify hot key
hot_key = 999

# Split data
hot_data = skewed_data.filter(col("customer_id") == hot_key)
normal_data = skewed_data.filter(col("customer_id") != hot_key)

print(f"✅ Hot Key Data: {hot_data.count():,} records")
print(f"✅ Normal Data: {normal_data.count():,} records\n")

# Process separately
hot_customer = customers.filter(col("customer_id") == hot_key)
normal_customers = customers.filter(col("customer_id") != hot_key)

# Broadcast for hot key
hot_join = hot_data.join(broadcast(hot_customer), "customer_id")

# Regular join for normal data
normal_join = normal_data.join(broadcast(normal_customers), "customer_id")

# Union results
final_result = hot_join.union(normal_join)

print(f"✅ Final Result: {final_result.count():,} records")
print("⚡ Hot key processed efficiently without impacting normal keys!")
print("💡 This pattern works well when you can identify hot keys upfront")

display(final_result.limit(10))

# COMMAND ----------

# DBTITLE 1,Section 5: Performance Anti-Patterns
# MAGIC %md
# MAGIC # ❌ Section 5: Performance Anti-Patterns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚨 Common Mistakes That Kill Performance:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 1️⃣ Anti-Pattern: Unnecessary Shuffles
# MAGIC
# MAGIC **❌ Bad:**
# MAGIC ```python
# MAGIC # Multiple repartitions and shuffles
# MAGIC df.repartition(10).groupBy("id").count().repartition(5).write.parquet("...")
# MAGIC ```
# MAGIC
# MAGIC **✅ Good:**
# MAGIC ```python
# MAGIC # Single optimal partitioning
# MAGIC df.repartition(10, "id").groupBy("id").count().coalesce(5).write.parquet("...")
# MAGIC ```
# MAGIC
# MAGIC 💡 **Why**: Minimize shuffles; use coalesce before write
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ Anti-Pattern: Not Broadcasting Small Tables
# MAGIC
# MAGIC **❌ Bad:**
# MAGIC ```python
# MAGIC # Shuffle join for small dimension table
# MAGIC large_df.join(small_df, "id")  # Shuffles BOTH tables
# MAGIC ```
# MAGIC
# MAGIC **✅ Good:**
# MAGIC ```python
# MAGIC # Broadcast small table
# MAGIC large_df.join(broadcast(small_df), "id")  # No shuffle!
# MAGIC ```
# MAGIC
# MAGIC 💡 **Why**: 10-100x faster for small dimension tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ Anti-Pattern: Collecting Large Datasets
# MAGIC
# MAGIC **❌ Bad:**
# MAGIC ```python
# MAGIC # Brings ALL data to driver
# MAGIC data = df.collect()  # OOM if large!
# MAGIC ```
# MAGIC
# MAGIC **✅ Good:**
# MAGIC ```python
# MAGIC # Process distributed
# MAGIC df.write.parquet("...")  # Or use limit for sampling
# MAGIC samples = df.limit(1000).collect()
# MAGIC ```
# MAGIC
# MAGIC 💡 **Why**: Driver has limited memory
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4️⃣ Anti-Pattern: UDFs Instead of Built-in Functions
# MAGIC
# MAGIC **❌ Bad:**
# MAGIC ```python
# MAGIC # Python UDF (slow, no optimization)
# MAGIC from pyspark.sql.functions import udf
# MAGIC
# MAGIC @udf
# MAGIC def compute(x):
# MAGIC     return x * 2
# MAGIC
# MAGIC df.withColumn("result", compute(col("value")))
# MAGIC ```
# MAGIC
# MAGIC **✅ Good:**
# MAGIC ```python
# MAGIC # Built-in functions (optimized)
# MAGIC df.withColumn("result", col("value") * 2)
# MAGIC ```
# MAGIC
# MAGIC 💡 **Why**: Built-in functions leverage Catalyst optimizer
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5️⃣ Anti-Pattern: Over-Partitioning
# MAGIC
# MAGIC **❌ Bad:**
# MAGIC ```python
# MAGIC # 10 MB data with 1000 partitions
# MAGIC df.repartition(1000)  # Each partition = 10 KB (overhead!)
# MAGIC ```
# MAGIC
# MAGIC **✅ Good:**
# MAGIC ```python
# MAGIC # Optimal partition size: 128 MB - 1 GB
# MAGIC df.coalesce(2)  # 5 MB per partition
# MAGIC ```
# MAGIC
# MAGIC 💡 **Why**: Too many partitions = scheduling overhead
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6️⃣ Anti-Pattern: Ignoring Data Skew
# MAGIC
# MAGIC **❌ Bad:**
# MAGIC ```python
# MAGIC # Direct join with skewed keys
# MAGIC skewed_df.join(other_df, "hot_key")  # One task processes 90% of data
# MAGIC ```
# MAGIC
# MAGIC **✅ Good:**
# MAGIC ```python
# MAGIC # Broadcast or salting
# MAGIC skewed_df.join(broadcast(other_df), "hot_key")  # OR use salting
# MAGIC ```
# MAGIC
# MAGIC 💡 **Why**: Prevents stragglers and OOM
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 7️⃣ Anti-Pattern: Writing Small Files
# MAGIC
# MAGIC **❌ Bad:**
# MAGIC ```python
# MAGIC # 10,000 small files (1 MB each)
# MAGIC df.coalesce(10000).write.parquet("...")  # Small file problem
# MAGIC ```
# MAGIC
# MAGIC **✅ Good:**
# MAGIC ```python
# MAGIC # Optimal file size: 128 MB - 1 GB
# MAGIC df.coalesce(10).write.parquet("...")  # 100 MB files
# MAGIC ```
# MAGIC
# MAGIC 💡 **Why**: Small files hurt downstream read performance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Performance Comparison:
# MAGIC
# MAGIC | Anti-Pattern | Impact | Time | Solution |
# MAGIC |-------------|---------|------|----------|
# MAGIC | Multiple shuffles | High | 10x slower | Minimize shuffles |
# MAGIC | No broadcast | High | 10x slower | Use broadcast |
# MAGIC | Large collect() | Critical | OOM crash | Never collect large data |
# MAGIC | Python UDFs | Medium | 5x slower | Use built-in functions |
# MAGIC | Over-partitioning | Medium | 2-3x slower | Right-size partitions |
# MAGIC | Data skew | Critical | 10-100x slower | Broadcast/salting |
# MAGIC | Small files | Medium | 3-5x slower | Coalesce before write |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Next: Let's build an optimized pipeline!**

# COMMAND ----------

# DBTITLE 1,Section 6: Hands-on Optimized Pipeline
# MAGIC %md
# MAGIC # 🚀 Section 6: Hands-on Optimized Pipeline
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Objective:
# MAGIC
# MAGIC Build a production-grade pipeline applying ALL optimization techniques:
# MAGIC
# MAGIC ✅ Proper partitioning  
# MAGIC ✅ Broadcast joins  
# MAGIC ✅ Skew handling  
# MAGIC ✅ Efficient aggregations  
# MAGIC ✅ Optimal file writes  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Pipeline Architecture:
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────┐
# MAGIC │  Raw Fact Data  │
# MAGIC │  (Large Table) │
# MAGIC └───────┬────────┘
# MAGIC         │
# MAGIC         │ ✅ Partition by key
# MAGIC         ↓
# MAGIC ┌───────┴───────────────────────┐
# MAGIC │  Broadcast Join (Dimensions)  │
# MAGIC │  ⚡ No Shuffle!               │
# MAGIC └────────────┬─────────────────┘
# MAGIC               │
# MAGIC               │ ✅ Pre-aggregation
# MAGIC               ↓
# MAGIC ┌────────────┴────────────────┐
# MAGIC │  Group By + Aggregation      │
# MAGIC │  (Optimized Shuffle)         │
# MAGIC └────────────┬────────────────┘
# MAGIC               │
# MAGIC               │ ✅ Coalesce
# MAGIC               ↓
# MAGIC ┌────────────┴────────────────┐
# MAGIC │  Write Delta (Optimized)     │
# MAGIC │  128MB-1GB files             │
# MAGIC └─────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Let's implement this pipeline!**

# COMMAND ----------

# DBTITLE 1,Step 1: Create Sample Data (Production-like)
from pyspark.sql.functions import *
from pyspark.sql.functions import broadcast
from datetime import datetime

print("✅ Step 1: Creating Production-like Datasets\n")

# Fact Table: Sales Transactions (Large)
sales_df = spark.range(0, 5000000).toDF("transaction_id") \
    .withColumn("customer_id", (col("transaction_id") % 10000).cast("int")) \
    .withColumn("product_id", (col("transaction_id") % 500).cast("int")) \
    .withColumn("store_id", (col("transaction_id") % 100).cast("int")) \
    .withColumn("quantity", (rand() * 10 + 1).cast("int")) \
    .withColumn("unit_price", (rand() * 100 + 10).cast("decimal(10,2)")) \
    .withColumn("transaction_date", expr("date_add('2024-01-01', cast(rand() * 365 as int))")) \
    .withColumn("transaction_ts", current_timestamp())

print(f"✅ Sales Transactions: {sales_df.count():,} records")
print("💡 Large fact table with sales transactions\n")

# Dimension Table: Customers (Small)
customers_df = spark.range(0, 10000).toDF("customer_id") \
    .withColumn("customer_name", concat(lit("Customer_"), col("customer_id"))) \
    .withColumn("customer_segment", 
                expr("case when customer_id % 4 = 0 then 'Premium' when customer_id % 4 = 1 then 'Gold' when customer_id % 4 = 2 then 'Silver' else 'Bronze' end")) \
    .withColumn("region", expr("case when customer_id % 5 = 0 then 'North' when customer_id % 5 = 1 then 'South' when customer_id % 5 = 2 then 'East' when customer_id % 5 = 3 then 'West' else 'Central' end"))

print(f"✅ Customers: {customers_df.count():,} records")
print("💡 Small dimension table with customer details\n")

# Dimension Table: Products (Small)
products_df = spark.range(0, 500).toDF("product_id") \
    .withColumn("product_name", concat(lit("Product_"), col("product_id"))) \
    .withColumn("category", expr("case when product_id % 5 = 0 then 'Electronics' when product_id % 5 = 1 then 'Clothing' when product_id % 5 = 2 then 'Home' when product_id % 5 = 3 then 'Sports' else 'Books' end")) \
    .withColumn("cost_price", (col("product_id") % 50 + 10).cast("decimal(10,2)"))

print(f"✅ Products: {products_df.count():,} records")
print("💡 Small dimension table with product catalog\n")

# Dimension Table: Stores (Small)
stores_df = spark.range(0, 100).toDF("store_id") \
    .withColumn("store_name", concat(lit("Store_"), col("store_id"))) \
    .withColumn("city", concat(lit("City_"), (col("store_id") % 20).cast("string")))

print(f"✅ Stores: {stores_df.count():,} records")
print("💡 Small dimension table with store locations\n")

print("✅ All datasets created successfully!")
display(sales_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Step 2: Optimize Partitioning
print("✅ Step 2: Optimizing Partitioning Strategy\n")

# Repartition sales by customer_id for efficient join
# This co-locates records with same customer_id in same partition
sales_partitioned = sales_df.repartition(16, "customer_id")

print("⚡ Sales Repartitioned by customer_id: 16 partitions")
print("💡 Benefit: Records with same customer_id are in same partition")
print("💡 Impact: Reduces shuffle in subsequent joins/aggregations\n")

# Verify partitioning distribution
partition_check = sales_partitioned.withColumn("partition_id", spark_partition_id()) \
    .groupBy("partition_id").agg(
        count("*").alias("record_count"),
        countDistinct("customer_id").alias("unique_customers")
    ).orderBy("partition_id")

print("📊 Partition Distribution (showing first 10):")
display(partition_check.limit(10))

print("\n✅ Partitioning optimized for efficient downstream processing!")

# COMMAND ----------

# DBTITLE 1,Step 3: Broadcast Joins (Dimension Enrichment)
print("✅ Step 3: Enriching with Broadcast Joins\n")

# ⚡ Broadcast ALL dimension tables (they're small)
enriched_df = sales_partitioned \
    .join(broadcast(customers_df), "customer_id") \
    .join(broadcast(products_df), "product_id") \
    .join(broadcast(stores_df), "store_id")

print("⚡ Three broadcast joins executed:")
print("   1. Customers (10K records) - BROADCASTED")
print("   2. Products (500 records) - BROADCASTED")
print("   3. Stores (100 records) - BROADCASTED\n")

print("💡 Benefit: ZERO shuffle overhead!")
print("💡 All joins happen locally on each executor\n")

# Calculate revenue
enriched_df = enriched_df.withColumn(
    "revenue",
    (col("quantity") * col("unit_price")).cast("decimal(10,2)")
).withColumn(
    "profit",
    ((col("unit_price") - col("cost_price")) * col("quantity")).cast("decimal(10,2)")
)

print(f"✅ Enriched DataFrame: {enriched_df.count():,} records")
print(f"✅ Columns: {len(enriched_df.columns)}\n")

display(enriched_df.select(
    "transaction_id", "customer_name", "customer_segment", 
    "product_name", "category", "store_name", 
    "quantity", "revenue", "profit"
).limit(10))

print("\n✅ Enrichment complete with optimal performance!")

# COMMAND ----------

# DBTITLE 1,Step 4: Efficient Aggregation
print("✅ Step 4: Performing Aggregations\n")

# Multi-level aggregation
agg_result = enriched_df.groupBy(
    "customer_segment",
    "region",
    "category",
    date_trunc("month", col("transaction_date")).alias("month")
).agg(
    count("transaction_id").alias("transaction_count"),
    sum("quantity").alias("total_quantity"),
    sum("revenue").cast("decimal(15,2)").alias("total_revenue"),
    sum("profit").cast("decimal(15,2)").alias("total_profit"),
    avg("revenue").cast("decimal(10,2)").alias("avg_transaction_value"),
    countDistinct("customer_id").alias("unique_customers")
).orderBy(col("total_revenue").desc())

print("⚡ Aggregation Strategy:")
print("   - Group by: segment, region, category, month")
print("   - Metrics: count, sum, avg, distinct count")

print(f"\n✅ Aggregated Records: {agg_result.count():,}")
print("💡 Aggregation significantly reduced dataset size from 5M to a few thousand\n")

print("📊 Top Revenue Segments:")
display(agg_result.limit(20))

print("\n✅ Aggregation complete!")

# COMMAND ----------

# DBTITLE 1,Step 5: Optimize for Write (Coalesce)
print("✅ Step 5: Optimizing for Write Operations\n")

# Coalesce to reduce file count
# Target: 128 MB - 1 GB per file
optimal_partitions = 4
final_df = agg_result.coalesce(optimal_partitions)

print(f"⚡ Coalesced to {optimal_partitions} partitions")
print("💡 Aggregated results are now in 4 partitions for optimal write\n")

print("💡 Benefits of Coalesce:")
print("   ✅ Reduces number of output files")
print("   ✅ Minimal shuffle (faster than repartition)")
print("   ✅ Optimal file size for downstream reads")
print("   ✅ Prevents small file problem\n")

print("✅ Ready for write!")
display(final_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Step 6: Write to Delta (Production-Ready)
print("✅ Step 6: Writing to Delta Table\n")

# Note: This is a demonstration - in production, use Unity Catalog Volumes
# Example path: /Volumes/catalog/schema/volume/sales_summary

# For this demo, we'll show the write command structure
print("📝 Production Write Pattern:")
print("""
final_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("month", "category") \
    .option("overwriteSchema", "true") \
    .save("/Volumes/catalog/schema/volume/sales_summary")
""")

print("\n✅ Write Optimizations Applied:")
print("   ⚡ Delta format (ACID, time travel)")
print("   ⚡ Partitioned by month and category")
print("   ⚡ Coalesced to optimal file count (4 partitions)")
print("   ⚡ File size: 128 MB - 1 GB each\n")

print("📊 Final Statistics:")
print(f"   Records to write: {final_df.count():,}")
print(f"   Output partitions: 4")
print(f"   Estimated files: 4 base files (before data partitioning)\n")

print("✅ Pipeline complete! All optimizations applied.")

# Show final schema
print("\n📊 Final Schema:")
final_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Section 7: Performance Comparison
# MAGIC %md
# MAGIC # ⏱️ Section 7: End-to-End Performance Comparison
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔴 BAD Pipeline (Anti-Patterns)
# MAGIC
# MAGIC ```python
# MAGIC # ❌ Multiple unnecessary operations
# MAGIC result = sales_df \
# MAGIC     .repartition(100) \
# MAGIC     .join(customers_df, "customer_id") \
# MAGIC     .join(products_df, "product_id") \
# MAGIC     .join(stores_df, "store_id") \
# MAGIC     .repartition(50) \
# MAGIC     .groupBy("customer_segment", "category") \
# MAGIC     .count() \
# MAGIC     .repartition(10) \
# MAGIC     .write.parquet("output")
# MAGIC ```
# MAGIC
# MAGIC ### ❌ Problems:
# MAGIC
# MAGIC 1. 🔴 **Unnecessary repartition(100)** - wasteful shuffle  
# MAGIC 2. 🔴 **No broadcast joins** - shuffles ALL tables  
# MAGIC 3. 🔴 **Multiple repartitions** - 3 shuffles total!  
# MAGIC 4. 🔴 **No coalesce** - creates too many small files  
# MAGIC 5. 🔴 **Ignores data skew**  
# MAGIC
# MAGIC ### ⏱️ Performance:
# MAGIC
# MAGIC - **Execution Time**: 15-20 minutes  
# MAGIC - **Shuffles**: 5+ shuffle stages  
# MAGIC - **Network I/O**: Very high  
# MAGIC - **Output Files**: 10 small files  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🟢 OPTIMIZED Pipeline (Best Practices)
# MAGIC
# MAGIC ```python
# MAGIC # ✅ Efficient operations
# MAGIC result = sales_df \
# MAGIC     .repartition(16, "customer_id") \
# MAGIC     .join(broadcast(customers_df), "customer_id") \
# MAGIC     .join(broadcast(products_df), "product_id") \
# MAGIC     .join(broadcast(stores_df), "store_id") \
# MAGIC     .groupBy("customer_segment", "category") \
# MAGIC     .count() \
# MAGIC     .coalesce(4) \
# MAGIC     .write.parquet("output")
# MAGIC ```
# MAGIC
# MAGIC ### ✅ Optimizations:
# MAGIC
# MAGIC 1. 🟢 **Single repartition by join key** - efficient partitioning  
# MAGIC 2. 🟢 **Broadcast all dimension tables** - no shuffle!  
# MAGIC 3. 🟢 **No intermediate repartitions** - single shuffle for groupBy  
# MAGIC 4. 🟢 **Coalesce before write** - optimal file count  
# MAGIC 5. 🟢 **Handles data distribution**  
# MAGIC
# MAGIC ### ⚡ Performance:
# MAGIC
# MAGIC - **Execution Time**: 2-3 minutes  
# MAGIC - **Shuffles**: 2 shuffle stages only  
# MAGIC - **Network I/O**: Minimal  
# MAGIC - **Output Files**: 4 optimal-sized files  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Performance Metrics Comparison:
# MAGIC
# MAGIC | Metric | Bad Pipeline | Optimized Pipeline | Improvement |
# MAGIC |--------|-------------|-------------------|-------------|
# MAGIC | **Execution Time** | 15-20 min | 2-3 min | 🟢 **6-8x faster** |
# MAGIC | **Shuffle Stages** | 5+ stages | 2 stages | 🟢 **60% reduction** |
# MAGIC | **Network I/O** | ~10 GB | ~2 GB | 🟢 **80% reduction** |
# MAGIC | **Memory Usage** | High spikes | Stable | 🟢 **No OOM** |
# MAGIC | **Output Files** | 10 small | 4 optimal | 🟢 **Better reads** |
# MAGIC | **CPU Utilization** | Uneven | Balanced | 🟢 **Efficient** |
# MAGIC | **Task Duration** | Skewed | Even | 🟢 **No stragglers** |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Key Takeaways:
# MAGIC
# MAGIC ✅ **Broadcast small tables** → Eliminates shuffle overhead  
# MAGIC ✅ **Partition by join/group keys** → Reduces data movement  
# MAGIC ✅ **Minimize shuffles** → Fewer expensive operations  
# MAGIC ✅ **Coalesce before write** → Optimal file sizes  
# MAGIC ✅ **Handle data skew early** → Prevents bottlenecks  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Performance Optimization Checklist:
# MAGIC
# MAGIC ☐ Profile your data (size, distribution, skew)  
# MAGIC ☐ Identify small tables for broadcasting  
# MAGIC ☐ Partition by frequently-used join/group keys  
# MAGIC ☐ Use broadcast() explicitly for predictability  
# MAGIC ☐ Minimize unnecessary repartitions  
# MAGIC ☐ Use coalesce (not repartition) before writes  
# MAGIC ☐ Monitor Spark UI for shuffle sizes  
# MAGIC ☐ Check for task skew in Spark UI  
# MAGIC ☐ Optimize file sizes (128 MB - 1 GB)  
# MAGIC ☐ Enable AQE for automatic optimizations  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Next: Genie Code Agent usage!**

# COMMAND ----------

# DBTITLE 1,Section 8: Genie Code Agent Usage
# MAGIC %md
# MAGIC # 🤖 Section 8: Genie Code Agent for Performance Tuning
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 How Genie Helps with Performance Optimization:
# MAGIC
# MAGIC Genie Code can analyze your Spark code and suggest performance improvements. Here are practical prompts:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔍 1. Performance Analysis Prompts:
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Analyze this Spark code for performance bottlenecks and suggest optimizations
# MAGIC ```
# MAGIC
# MAGIC **What Genie Does:**
# MAGIC - Identifies missing broadcast hints  
# MAGIC - Detects unnecessary shuffles  
# MAGIC - Suggests better partitioning strategies  
# MAGIC - Flags potential skew issues  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚡  2. Join Optimization Prompts:
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Optimize this join operation - which strategy should I use?
# MAGIC ```
# MAGIC
# MAGIC **What Genie Does:**
# MAGIC - Recommends broadcast vs shuffle join  
# MAGIC - Suggests proper join keys  
# MAGIC - Identifies opportunities for broadcast  
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Convert this to use broadcast joins where appropriate
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔧 3. Partition Tuning Prompts:
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC What's the optimal partition count for a 10 GB dataset?
# MAGIC ```
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Should I use repartition or coalesce here?
# MAGIC ```
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Partition this DataFrame efficiently for downstream aggregations
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚖️ 4. Data Skew Detection Prompts:
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Detect data skew in this DataFrame and suggest solutions
# MAGIC ```
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Implement salting technique to handle skewed join keys
# MAGIC ```
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC My job has one slow task - how do I diagnose and fix data skew?
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 5. End-to-End Pipeline Optimization:
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Optimize this entire Spark pipeline for production performance
# MAGIC ```
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Rewrite this code following Spark performance best practices
# MAGIC ```
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Reduce shuffle operations in this pipeline
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 6. Query Plan Analysis:
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Explain this Spark query execution plan
# MAGIC ```
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Why is this query slow? Analyze the execution plan
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📄 7. File Optimization Prompts:
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC How many files should I write for optimal performance?
# MAGIC ```
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Fix small file problem in this write operation
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚙️ 8. Configuration Tuning:
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC What Spark configs should I tune for large joins?
# MAGIC ```
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Recommend optimal spark.sql.shuffle.partitions for my workload
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Example Interaction:
# MAGIC
# MAGIC **User**: "This join is taking 30 minutes, how can I make it faster?"
# MAGIC
# MAGIC **Genie**:
# MAGIC 1. Analyzes your code  
# MAGIC 2. Identifies dimension table (small)  
# MAGIC 3. Suggests adding `broadcast()`  
# MAGIC 4. Shows before/after comparison  
# MAGIC 5. Explains expected performance gain  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Pro Tips for Using Genie:
# MAGIC
# MAGIC ✅ **Be specific**: Include dataset sizes in your prompt  
# MAGIC ✅ **Share context**: Mention if you see skew, OOM, or slow tasks  
# MAGIC ✅ **Ask for explanations**: "Why is broadcast better here?"  
# MAGIC ✅ **Request alternatives**: "What are 3 ways to optimize this?"  
# MAGIC ✅ **Include error messages**: If you get OOM, share the error  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Try these prompts in your next performance tuning session!**

# COMMAND ----------

# DBTITLE 1,Section 9: Summary & Interview Questions
# MAGIC %md
# MAGIC # 🎓 Section 9: Summary & Interview Questions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Key Learnings Summary:
# MAGIC
# MAGIC ### 1️⃣ Partitioning:
# MAGIC - ✅ Partitions = unit of parallelism in Spark  
# MAGIC - ✅ Optimal size: 128 MB - 1 GB per partition  
# MAGIC - ✅ Use `repartition()` to increase/balance  
# MAGIC - ✅ Use `coalesce()` to decrease (before writes)  
# MAGIC - ✅ Partition by frequently-used join/group keys  
# MAGIC
# MAGIC ### 2️⃣ Join Strategies:
# MAGIC - ✅ **Broadcast Join**: Best for small tables (<10 MB)  
# MAGIC - ✅ **Shuffle Join**: Default for large-large joins  
# MAGIC - ✅ **Sort-Merge Join**: Fallback for equi-joins  
# MAGIC - ✅ Always broadcast dimension tables explicitly  
# MAGIC
# MAGIC ### 3️⃣ Data Skew:
# MAGIC - ✅ Causes: Uneven key distribution  
# MAGIC - ✅ Symptoms: One slow task, OOM errors  
# MAGIC - ✅ Solutions: Broadcast, salting, filter-union pattern  
# MAGIC - ✅ Prevention: Profile data early  
# MAGIC
# MAGIC ### 4️⃣ Performance Optimization:
# MAGIC - ✅ Minimize shuffles (most expensive operation)  
# MAGIC - ✅ Use broadcast for small tables  
# MAGIC - ✅ Partition by join/aggregation keys  
# MAGIC - ✅ Coalesce before writes  
# MAGIC - ✅ Use built-in functions (avoid UDFs)  
# MAGIC - ✅ Enable AQE (Adaptive Query Execution)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❓ Interview Questions:
# MAGIC
# MAGIC ### 🟢 Basic Level:
# MAGIC
# MAGIC 1. **Q**: What is a partition in Spark?  
# MAGIC    **A**: A logical chunk of data that can be processed independently on an executor. It's the unit of parallelism in Spark.
# MAGIC
# MAGIC 2. **Q**: What's the difference between `repartition()` and `coalesce()`?  
# MAGIC    **A**: `repartition()` does full shuffle (can increase/decrease), `coalesce()` minimizes shuffle (only decreases).
# MAGIC
# MAGIC 3. **Q**: When should you use a broadcast join?  
# MAGIC    **A**: When one table is small enough to fit in memory (typically <10 MB) and can be sent to all executors.
# MAGIC
# MAGIC 4. **Q**: What is data skew?  
# MAGIC    **A**: Uneven distribution of data across partitions, causing some tasks to process much more data than others.
# MAGIC
# MAGIC 5. **Q**: What's the default broadcast threshold in Spark?  
# MAGIC    **A**: 10 MB (`spark.sql.autoBroadcastJoinThreshold`)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟡 Intermediate Level:
# MAGIC
# MAGIC 6. **Q**: How do you detect data skew in Spark?  
# MAGIC    **A**: Check Spark UI for uneven task durations, shuffle read/write sizes, and use `.groupBy().count()` to analyze key distribution.
# MAGIC
# MAGIC 7. **Q**: Explain the salting technique for skew handling.  
# MAGIC    **A**: Add random suffix to hot keys to distribute them across partitions, then explode the other table with all salt values for matching.
# MAGIC
# MAGIC 8. **Q**: What are the three main join strategies in Spark?  
# MAGIC    **A**: Broadcast Hash Join, Shuffle Hash Join, Sort-Merge Join.
# MAGIC
# MAGIC 9. **Q**: How does `spark.sql.shuffle.partitions` affect performance?  
# MAGIC    **A**: Controls partition count after shuffle operations. Default 200 may be too high for small data or too low for large data.
# MAGIC
# MAGIC 10. **Q**: What's the optimal partition size and why?  
# MAGIC     **A**: 128 MB - 1 GB. Smaller = overhead, larger = memory pressure and reduced parallelism.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 Advanced Level:
# MAGIC
# MAGIC 11. **Q**: How does Spark's Catalyst optimizer choose join strategies?  
# MAGIC     **A**: Uses statistics (size estimates), cost-based optimization, and configuration thresholds to select optimal join strategy.
# MAGIC
# MAGIC 12. **Q**: Explain Adaptive Query Execution (AQE) and its benefits.  
# MAGIC     **A**: Runtime optimization that adjusts execution plan based on actual runtime statistics. Handles skew, optimizes joins, coalesces partitions automatically.
# MAGIC
# MAGIC 13. **Q**: How would you optimize a join with both tables having billions of records?  
# MAGIC     **A**: 1) Partition both by join key, 2) Filter data early, 3) Check for skew and handle separately, 4) Enable AQE, 5) Consider bucketing for repeated joins.
# MAGIC
# MAGIC 14. **Q**: What causes shuffle in Spark and how do you minimize it?  
# MAGIC     **A**: Caused by operations requiring data redistribution (joins, groupBy, repartition). Minimize by: broadcast joins, pre-partitioning, reducing wide transformations.
# MAGIC
# MAGIC 15. **Q**: Design a performance-optimized pipeline for joining 1 TB fact table with 10 MB dimension tables.  
# MAGIC     **A**: 
# MAGIC     ```python
# MAGIC     fact.repartition(100, "key") \
# MAGIC         .join(broadcast(dim1), "key") \
# MAGIC         .join(broadcast(dim2), "key2") \
# MAGIC         .groupBy("category") \
# MAGIC         .agg(...) \
# MAGIC         .coalesce(10) \
# MAGIC         .write.partitionBy("date").parquet(...)
# MAGIC     ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Common Mistakes to Avoid:
# MAGIC
# MAGIC ### 1️⃣ Over-Partitioning:
# MAGIC **Mistake**: Creating thousands of tiny partitions  
# MAGIC **Impact**: Excessive scheduling overhead  
# MAGIC **Fix**: Keep partitions 128 MB - 1 GB each
# MAGIC
# MAGIC ### 2️⃣ Wrong Join Strategy:
# MAGIC **Mistake**: Not broadcasting small dimension tables  
# MAGIC **Impact**: Unnecessary shuffle, 10x slower  
# MAGIC **Fix**: Always use `broadcast()` for small tables
# MAGIC
# MAGIC ### 3️⃣ Ignoring Data Skew:
# MAGIC **Mistake**: Running joins/aggregations on skewed data  
# MAGIC **Impact**: Stragglers, OOM, poor parallelism  
# MAGIC **Fix**: Detect early, use broadcast/salting
# MAGIC
# MAGIC ### 4️⃣ Overusing `repartition()`:
# MAGIC **Mistake**: Repartitioning multiple times unnecessarily  
# MAGIC **Impact**: Multiple expensive shuffles  
# MAGIC **Fix**: Partition once strategically
# MAGIC
# MAGIC ### 5️⃣ Small File Problem:
# MAGIC **Mistake**: Writing thousands of small files  
# MAGIC **Impact**: Poor downstream read performance  
# MAGIC **Fix**: Use `coalesce()` before write
# MAGIC
# MAGIC ### 6️⃣ Using Python UDFs:
# MAGIC **Mistake**: Using Python UDFs instead of built-in functions  
# MAGIC **Impact**: Serialization overhead, no optimization  
# MAGIC **Fix**: Use Spark SQL built-in functions
# MAGIC
# MAGIC ### 7️⃣ Not Monitoring Execution:
# MAGIC **Mistake**: Running jobs without checking Spark UI  
# MAGIC **Impact**: Miss performance issues and opportunities  
# MAGIC **Fix**: Always review Spark UI for bottlenecks
# MAGIC
# MAGIC ### 8️⃣ Incorrect Partition Count:
# MAGIC **Mistake**: Using default 200 partitions for all workloads  
# MAGIC **Impact**: Inefficient resource utilization  
# MAGIC **Fix**: Tune based on data size and cores
# MAGIC
# MAGIC ### 9️⃣ Large Collects:
# MAGIC **Mistake**: Calling `.collect()` on large DataFrames  
# MAGIC **Impact**: Driver OOM  
# MAGIC **Fix**: Process distributed or use `.limit()`
# MAGIC
# MAGIC ### 🔟 Forgetting AQE:
# MAGIC **Mistake**: Not enabling Adaptive Query Execution  
# MAGIC **Impact**: Missing automatic optimizations  
# MAGIC **Fix**: Enable AQE for Spark 3.0+
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Best Practices Checklist:
# MAGIC
# MAGIC ☐ Profile your data before optimization  
# MAGIC ☐ Broadcast all dimension tables  
# MAGIC ☐ Partition by join/aggregation keys  
# MAGIC ☐ Use `coalesce()` before writes  
# MAGIC ☐ Monitor Spark UI regularly  
# MAGIC ☐ Handle data skew proactively  
# MAGIC ☐ Minimize shuffle operations  
# MAGIC ☐ Use built-in functions  
# MAGIC ☐ Enable AQE  
# MAGIC ☐ Optimize file sizes (128 MB - 1 GB)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Advanced Topics (For Further Learning):
# MAGIC
# MAGIC 1. **Bucketing**: Pre-partitioning tables for repeated joins  
# MAGIC 2. **Z-Ordering**: Data layout optimization for Delta Lake  
# MAGIC 3. **Bloom Filters**: Skip data files during reads  
# MAGIC 4. **Photon Engine**: Vectorized query execution  
# MAGIC 5. **Dynamic Partition Pruning**: Runtime optimization  
# MAGIC 6. **Column Statistics**: Enhanced CBO decisions  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎓 Congratulations!
# MAGIC
# MAGIC You've completed **Phase 3 Day 18: Spark Performance Tuning**!
# MAGIC
# MAGIC ### 🔑 Key Achievements:
# MAGIC ✅ Understand partitioning strategies  
# MAGIC ✅ Master join optimization techniques  
# MAGIC ✅ Handle data skew effectively  
# MAGIC ✅ Build production-grade optimized pipelines  
# MAGIC ✅ Apply performance best practices  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📚 Next Steps:
# MAGIC 1. Practice optimizing your existing pipelines  
# MAGIC 2. Monitor Spark UI to identify bottlenecks  
# MAGIC 3. Experiment with different optimization techniques  
# MAGIC 4. Learn advanced topics (AQE, bucketing, Z-ordering)  
# MAGIC 5. Apply these principles to real-world data  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Happy Optimizing! 🚀**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC **© 2026 TRRaveendra - Data Engineering Training**

# COMMAND ----------

# DBTITLE 1,Bonus: Adaptive Query Execution (AQE)
# MAGIC %md
# MAGIC # 🌟 Bonus: Adaptive Query Execution (AQE)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine planning a road trip before you leave. AQE is like having a smart GPS that **changes your route while driving** based on real-time traffic, not just the map you printed before leaving!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Adaptive Query Execution (AQE)** is a Spark 3.0+ feature that **re-optimizes query plans at runtime** based on accurate statistics collected during execution.
# MAGIC
# MAGIC ### 🔑 Why AQE Matters:
# MAGIC
# MAGIC **Traditional Spark (without AQE):**
# MAGIC - Plans query ONCE before execution  
# MAGIC - Uses estimated statistics (often inaccurate)  
# MAGIC - Can't adapt to actual runtime conditions  
# MAGIC - Stuck with suboptimal decisions  
# MAGIC
# MAGIC **With AQE:**
# MAGIC - Re-plans query during execution  
# MAGIC - Uses ACTUAL runtime statistics  
# MAGIC - Adapts to real data patterns  
# MAGIC - Automatically applies optimizations  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Three Key AQE Features:
# MAGIC
# MAGIC ### 1️⃣ Dynamic Coalescing of Shuffle Partitions:
# MAGIC
# MAGIC **Problem**: Default 200 shuffle partitions often wasteful  
# MAGIC **Solution**: AQE automatically reduces partitions if data is small
# MAGIC
# MAGIC ```python
# MAGIC # Without AQE: 200 partitions (many empty or tiny)
# MAGIC # With AQE: Automatically coalesces to optimal count
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ Dynamic Switch of Join Strategies:
# MAGIC
# MAGIC **Problem**: Initial plan chose shuffle join, but table is actually small  
# MAGIC **Solution**: AQE switches to broadcast join at runtime
# MAGIC
# MAGIC ```python
# MAGIC # Initial Plan: Sort-Merge Join (shuffle)
# MAGIC # Runtime: Table smaller than expected
# MAGIC # AQE: Switches to Broadcast Join (no shuffle!)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ Dynamic Handling of Skew Joins:
# MAGIC
# MAGIC **Problem**: Data skew causes stragglers  
# MAGIC **Solution**: AQE detects skewed partitions and splits them
# MAGIC
# MAGIC ```python
# MAGIC # AQE detects partition with 90% of data
# MAGIC # Automatically splits into smaller sub-partitions
# MAGIC # Processes in parallel instead of single slow task
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚙️ Enabling AQE:
# MAGIC
# MAGIC ```python
# MAGIC # Enable AQE (Spark 3.0+)
# MAGIC spark.conf.set("spark.sql.adaptive.enabled", "true")
# MAGIC
# MAGIC # Enable skew join optimization
# MAGIC spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# MAGIC
# MAGIC # Enable dynamic coalescing
# MAGIC spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
# MAGIC
# MAGIC # Auto broadcast threshold during execution
# MAGIC spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "10MB")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Performance Impact:
# MAGIC
# MAGIC | Scenario | Without AQE | With AQE | Improvement |
# MAGIC |----------|------------|----------|-------------|
# MAGIC | Small result after filter | 200 partitions | 10 partitions | 5x faster |
# MAGIC | Table smaller than expected | Shuffle join | Broadcast join | 10x faster |
# MAGIC | Skewed join | 1 task = 2 hours | Split evenly | 20x faster |
# MAGIC | Overall queries | Baseline | Optimized | 2-5x average |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 When AQE Helps Most:
# MAGIC
# MAGIC ✅ **Complex multi-stage queries** (many joins/aggregations)  
# MAGIC ✅ **Data with unknown characteristics** (can't profile upfront)  
# MAGIC ✅ **Queries with filters** (result size unpredictable)  
# MAGIC ✅ **Skewed data** (AQE handles automatically)  
# MAGIC ✅ **Dynamic workloads** (different data sizes)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚠️ When AQE May Not Help:
# MAGIC
# MAGIC ❌ **Very simple queries** (single scan/filter)  
# MAGIC ❌ **Small datasets** (optimization overhead not worth it)  
# MAGIC ❌ **Already optimized code** (broadcast explicitly set)  
# MAGIC ❌ **Streaming queries** (AQE for batch only)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔍 Monitoring AQE:
# MAGIC
# MAGIC Check Spark UI SQL tab:
# MAGIC - Look for "AQE" markers  
# MAGIC - "Coalesced" partitions  
# MAGIC - "Skew" handling annotations  
# MAGIC - Join strategy changes  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Best Practice:
# MAGIC
# MAGIC ✅ **Enable AQE by default** (Spark 3.0+)  
# MAGIC ✅ **Still write optimized code** (AQE is not magic)  
# MAGIC ✅ **Use explicit broadcast** for known small tables  
# MAGIC ✅ **Monitor AQE decisions** in Spark UI  
# MAGIC ✅ **Combine with manual optimizations**  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Key Insight:
# MAGIC
# MAGIC **AQE is a safety net, not a replacement for good code!**
# MAGIC
# MAGIC - Write optimized code (broadcast, partitioning, etc.)  
# MAGIC - AQE handles edge cases and dynamic scenarios  
# MAGIC - Together = best performance  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **AQE = Automatic optimization insurance for your Spark jobs! 🚀**

# COMMAND ----------

# DBTITLE 1,Demo: AQE Configuration
# ⚡ Adaptive Query Execution (AQE) on Serverless
print("⚡ AQE on Databricks Serverless Compute\n")

print("💡 Good News: AQE is ENABLED BY DEFAULT on Serverless!")
print("\n✅ On Databricks Serverless:")
print("   - AQE is automatically enabled")
print("   - No manual configuration needed")
print("   - Optimizations happen automatically at runtime")
print("   - Dynamic coalescing, join switching, skew handling built-in\n")

print("🚀 Benefits Already Active:")
print("   ⚡ Dynamic partition coalescing")
print("   ⚡ Runtime join strategy optimization")
print("   ⚡ Automatic skew handling")
print("   ⚡ Query re-optimization during execution\n")

print("💡 Note: On traditional clusters, you would configure:")
print("""   spark.conf.set("spark.sql.adaptive.enabled", "true")
   spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
   spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")""")

print("\n✅ But on Serverless, these optimizations work automatically!")
print("⚡ Your queries are already benefiting from AQE!")

# COMMAND ----------

# DBTITLE 1,Quick Reference: Performance Tuning Cheat Sheet
# MAGIC %md
# MAGIC # 📝 Quick Reference: Performance Tuning Cheat Sheet
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚡ Partitioning:
# MAGIC
# MAGIC ```python
# MAGIC # Increase partitions (full shuffle)
# MAGIC df.repartition(16)
# MAGIC df.repartition(16, "key")  # Hash partition by key
# MAGIC
# MAGIC # Decrease partitions (minimal shuffle)
# MAGIC df.coalesce(4)
# MAGIC ```
# MAGIC
# MAGIC **When to use:**
# MAGIC - `repartition()`: After filters, before joins/groupBy  
# MAGIC - `coalesce()`: Before writes  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📡 Broadcast Joins:
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import broadcast
# MAGIC
# MAGIC # Explicit broadcast (recommended)
# MAGIC large_df.join(broadcast(small_df), "key")
# MAGIC
# MAGIC # Configure threshold
# MAGIC spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)  # 10 MB
# MAGIC ```
# MAGIC
# MAGIC **Rule:** Broadcast if table < 10 MB
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚖️ Data Skew Solutions:
# MAGIC
# MAGIC ```python
# MAGIC # 1. Broadcast (best for small tables)
# MAGIC large.join(broadcast(small), "key")
# MAGIC
# MAGIC # 2. Salting
# MAGIC skewed.withColumn("salt", rand() * 10)
# MAGIC
# MAGIC # 3. Filter-Union Pattern
# MAGIC hot = df.filter(col("key") == hot_value)
# MAGIC normal = df.filter(col("key") != hot_value)
# MAGIC result = hot.join(broadcast(dim)).union(normal.join(dim))
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Optimization Checklist:
# MAGIC
# MAGIC ```python
# MAGIC # ✅ Optimized Pipeline Template
# MAGIC result = source_df \
# MAGIC     .repartition(N, "join_key") \
# MAGIC     .join(broadcast(dim1), "key1") \
# MAGIC     .join(broadcast(dim2), "key2") \
# MAGIC     .groupBy("category") \
# MAGIC     .agg(...) \
# MAGIC     .coalesce(4) \
# MAGIC     .write.partitionBy("date").parquet("...")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚙️ Critical Configs:
# MAGIC
# MAGIC ```python
# MAGIC # Adaptive Query Execution
# MAGIC spark.conf.set("spark.sql.adaptive.enabled", "true")
# MAGIC
# MAGIC # Shuffle partitions
# MAGIC spark.conf.set("spark.sql.shuffle.partitions", "200")  # Tune based on data
# MAGIC
# MAGIC # Broadcast threshold
# MAGIC spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Monitoring:
# MAGIC
# MAGIC ```python
# MAGIC # Check partitions
# MAGIC df.rdd.getNumPartitions()
# MAGIC
# MAGIC # Explain query plan
# MAGIC df.explain()
# MAGIC df.explain("extended")
# MAGIC
# MAGIC # Partition distribution
# MAGIC df.withColumn("partition", spark_partition_id()) \
# MAGIC   .groupBy("partition").count().show()
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Avoid:
# MAGIC
# MAGIC ```python
# MAGIC # ❌ Multiple repartitions
# MAGIC df.repartition(100).groupBy(...).repartition(50)
# MAGIC
# MAGIC # ❌ No broadcast on small tables
# MAGIC large.join(small, "key")  # Should be broadcast!
# MAGIC
# MAGIC # ❌ Large collects
# MAGIC df.collect()  # OOM if large!
# MAGIC
# MAGIC # ❌ Python UDFs
# MAGIC @udf
# MAGIC def slow_func(x): ...  # Use built-in functions instead
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Golden Rules:
# MAGIC
# MAGIC 1. **Broadcast small tables** (<10 MB)  
# MAGIC 2. **Partition by join/group keys**  
# MAGIC 3. **Minimize shuffles**  
# MAGIC 4. **Coalesce before writes**  
# MAGIC 5. **Handle skew early**  
# MAGIC 6. **Monitor Spark UI**  
# MAGIC 7. **Enable AQE**  
# MAGIC 8. **Use built-in functions**  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Bookmark this for quick reference! 🔖**

# COMMAND ----------

# DBTITLE 1,Visual: Partitioning Strategy Diagrams
# MAGIC %md
# MAGIC # 📊 Visual: Partitioning Strategy Diagrams
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1️⃣ Default Partitioning (No Optimization)
# MAGIC
# MAGIC ```
# MAGIC ┌───────────────────────────┐
# MAGIC │   Source Data (10 GB)      │
# MAGIC │   Random Distribution       │
# MAGIC └────────────┬──────────────┘
# MAGIC               │
# MAGIC     ┌─────────┼─────────┐
# MAGIC     │         │         │
# MAGIC ┌───┴──┐  ┌──┴──┐  ┌──┴──┐
# MAGIC │ P0   │  │ P1   │  │ P2   │
# MAGIC │ 800MB│  │ 200MB│  │ 150MB│  ...
# MAGIC │ 🔴   │  │ 🟢   │  │ 🟢   │
# MAGIC └──────┘  └──────┘  └──────┘
# MAGIC
# MAGIC ⚠️ Problem: Uneven distribution
# MAGIC ⚠️ P0 takes 4x longer than others
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2️⃣ Hash Partitioning by Key (Optimized)
# MAGIC
# MAGIC ```
# MAGIC ┌───────────────────────────┐
# MAGIC │   Source Data (10 GB)      │
# MAGIC │   .repartition(4, "key")   │
# MAGIC └────────────┬──────────────┘
# MAGIC               │ hash(key) % 4
# MAGIC     ┌─────────┼─────────┐
# MAGIC     │         │         │
# MAGIC ┌───┴──┐  ┌──┴──┐  ┌──┴──┐  ┌──────┐
# MAGIC │ P0   │  │ P1   │  │ P2   │  │ P3   │
# MAGIC │Key:A │  │Key:B │  │Key:C │  │Key:D │
# MAGIC │ 2.5GB│  │ 2.5GB│  │ 2.5GB│  │ 2.5GB│
# MAGIC │ 🟢   │  │ 🟢   │  │ 🟢   │  │ 🟢   │
# MAGIC └──────┘  └──────┘  └──────┘  └──────┘
# MAGIC
# MAGIC ✅ Benefit: Even distribution
# MAGIC ✅ All tasks complete in similar time
# MAGIC ✅ Same keys co-located in partition
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3️⃣ Broadcast Join (No Shuffle)
# MAGIC
# MAGIC ```
# MAGIC Large Table (Fact)          Small Table (Dimension)
# MAGIC ┌──────────────┐           ┌────────────┐
# MAGIC │1 TB Partitioned│           │ 10 MB Total │
# MAGIC └──────┬───────┘           └──────┬─────┘
# MAGIC        │                        │ broadcast()
# MAGIC    ┌───┼───┐                    │
# MAGIC    │   │   │                    │
# MAGIC ┌──┴─┐┌┴─┐┌┴──┐    ┌────────────┴────────────┐
# MAGIC │ P0 ││P1││ P2│    │ Broadcasted to ALL  │
# MAGIC └──┬─┘└┬─┘└┬──┘    │    Executors        │
# MAGIC    │   │   │         └──────┬──────────────┘
# MAGIC    │   │   │                │
# MAGIC    │   │   └──── copy ──────┼────────┐
# MAGIC    │   └────── copy ────────┼────────┤
# MAGIC    └─────── copy ──────────┼────────┘
# MAGIC                               │
# MAGIC                   ┌───────────┴───────────┐
# MAGIC                   │ Local Joins (Fast!) │
# MAGIC                   │ NO Network Shuffle  │
# MAGIC                   └──────────────────────┘
# MAGIC
# MAGIC ⚡ Key: Each partition joins locally
# MAGIC ⚡ No shuffle of large table
# MAGIC ⚡ 10-100x faster!
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4️⃣ Shuffle Join (Expensive)
# MAGIC
# MAGIC ```
# MAGIC Large Table A              Large Table B
# MAGIC ┌────────────┐           ┌────────────┐
# MAGIC │  1 TB Data  │           │  1 TB Data  │
# MAGIC └──────┬─────┘           └──────┬─────┘
# MAGIC        │ SHUFFLE                │ SHUFFLE
# MAGIC        │ (Network I/O)          │ (Network I/O)
# MAGIC    ┌───┼───┐                  ┌───┼───┐
# MAGIC    │   │   │                  │   │   │
# MAGIC ┌──┴─┐┌┴─┐┌┴──┐          ┌──┴─┐┌┴─┐┌┴──┐
# MAGIC │ P0 ││P1││ P2│          │ P0 ││P1││ P2│
# MAGIC │Key:││Key││Key│          │Key:││Key││Key│
# MAGIC │ A  ││ B ││ C │          │ A  ││ B ││ C │
# MAGIC └──┬─┘└┬─┘└┬──┘          └──┬─┘└┬─┘└┬──┘
# MAGIC    │   │   │                │   │   │
# MAGIC    └───┼───┼───── JOIN ─────┼───┼───┘
# MAGIC        │   │                │   │
# MAGIC        └───┼────────────────┼───┘
# MAGIC            │                │
# MAGIC      ┌─────┴────────────────┴─────┐
# MAGIC      │   Hash Join on Same Key     │
# MAGIC      │   (After Shuffle)            │
# MAGIC      └────────────────────────────┘
# MAGIC
# MAGIC ⚠️ Both tables shuffled across network
# MAGIC ⚠️ High I/O overhead
# MAGIC ⚠️ Slower execution
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5️⃣ Data Skew Visualization
# MAGIC
# MAGIC ```
# MAGIC 🔴 SKEWED DISTRIBUTION:
# MAGIC
# MAGIC ┌────────────────────────────────┐
# MAGIC │   Data by Key (1M records)     │
# MAGIC └──────────────┬─────────────────┘
# MAGIC                 │
# MAGIC     ┌───────────┼───────────────┐
# MAGIC     │           │               │
# MAGIC ┌───┴────┐  ┌──┴───┐  ┌───┴───┐  ┌──────┐
# MAGIC │  P0    │  │ P1  │  │  P2  │  │  P3  │
# MAGIC │        │  │     │  │     │  │      │
# MAGIC │ 800K   │  │ 50K │  │ 100K│  │  50K │
# MAGIC │ 🔴🔴🔴 │  │ 🟢  │  │ 🟢  │  │ 🟢   │
# MAGIC │ 🔴🔴🔴 │  │     │  │     │  │      │
# MAGIC │ 🔴🔴🔴 │  │     │  │     │  │      │
# MAGIC │ 🔴🔴🔴 │  │     │  │     │  │      │
# MAGIC │ 2 hours│  │15min│  │30min│  │15min │
# MAGIC └────────┘  └─────┘  └─────┘  └──────┘
# MAGIC    ⚠️       ✅      ✅      ✅
# MAGIC
# MAGIC Job Time = 2 hours (P0 bottleneck!)
# MAGIC
# MAGIC ────────────────────────────────────
# MAGIC
# MAGIC 🟢 AFTER SALTING:
# MAGIC
# MAGIC ┌────────────────────────────────┐
# MAGIC │ Hot key split with salting    │
# MAGIC └──────────────┬─────────────────┘
# MAGIC                 │
# MAGIC     ┌───────────┼───────────────┐
# MAGIC     │           │               │
# MAGIC ┌───┴────┐  ┌──┴───┐  ┌───┴───┐  ┌──────┐
# MAGIC │  P0    │  │ P1  │  │  P2  │  │  P3  │
# MAGIC │        │  │     │  │     │  │      │
# MAGIC │ 250K   │  │ 250K│  │ 250K│  │ 250K │
# MAGIC │ 🟢🟢   │  │ 🟢🟢│  │ 🟢🟢│  │ 🟢🟢 │
# MAGIC │        │  │     │  │     │  │      │
# MAGIC │ 30 min │  │30min│  │30min│  │30min │
# MAGIC └────────┘  └─────┘  └─────┘  └──────┘
# MAGIC    ✅       ✅      ✅      ✅
# MAGIC
# MAGIC Job Time = 30 minutes (4x faster!)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 6️⃣ Optimal Write Strategy
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────┐
# MAGIC │ Aggregated Result (400 MB)     │
# MAGIC │ 200 partitions (2 MB each)     │ ❌ TOO MANY!
# MAGIC └──────────────┬─────────────────┘
# MAGIC                 │
# MAGIC                 │ .coalesce(4)
# MAGIC                 │ (Minimal shuffle)
# MAGIC                 │
# MAGIC     ┌───────────┼───────────────┐
# MAGIC     │           │               │
# MAGIC ┌───┴────┐  ┌──┴───┐  ┌───┴───┐  ┌──────┐
# MAGIC │  P0    │  │ P1  │  │  P2  │  │  P3  │
# MAGIC │ 100 MB │  │100MB│  │100MB│  │100MB │
# MAGIC │ ✅     │  │ ✅  │  │ ✅  │  │ ✅   │
# MAGIC └───┬────┘  └──┬──┘  └───┬──┘  └──┬───┘
# MAGIC     │        │        │        │
# MAGIC     │        │        │        │
# MAGIC     └────────┼────────┼────────┘
# MAGIC              │                │
# MAGIC              write.parquet()
# MAGIC              │                │
# MAGIC     ┌────────┴────────────────┴─────┐
# MAGIC     │  4 Optimal Files (100 MB each) │
# MAGIC     │  ✅ Perfect for downstream reads │
# MAGIC     └───────────────────────────────┘
# MAGIC
# MAGIC ✅ Target: 128 MB - 1 GB per file
# MAGIC ✅ Avoids small file problem
# MAGIC ✅ Optimal for reads
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **These diagrams visualize the core performance concepts! 📊**