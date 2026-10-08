# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ⚡ Data Engineering Training — Phase 3 Day 12  
# MAGIC ## 🔍 Spark Internals: DAG, Lazy Evaluation, Stages & Tasks  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - Directed Acyclic Graph (DAG)  
# MAGIC - Lazy Evaluation  
# MAGIC - Stages & Tasks  
# MAGIC - Spark Execution Flow (Detailed)  
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
# MAGIC Understand how Spark executes jobs internally using DAG, lazy evaluation, and how jobs are broken into stages and tasks.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 What You'll Learn:
# MAGIC 1. **How Spark thinks** — DAG construction
# MAGIC 2. **Why Spark waits** — Lazy evaluation principles
# MAGIC 3. **How Spark executes** — Stages, tasks, and parallelism
# MAGIC 4. **Real execution flow** — From code to distributed compute
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **🔥 Important:** This notebook uses **DataFrame API only** (no RDDs) and is optimized for **Databricks Serverless** with **Unity Catalog**.

# COMMAND ----------

# DBTITLE 1,Section 1: What Happens When You Run Spark Code?
# MAGIC %md
# MAGIC ## 💡 Section 1: What Happens When You Run Spark Code?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧒 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine you're telling your friend a recipe:
# MAGIC - **You:** "Get flour, mix with water, add sugar, then bake."
# MAGIC - **Your friend:** *Writes it down but doesn't start cooking*
# MAGIC
# MAGIC Spark works the same way! When you write transformations, Spark just **writes them down** (builds a plan). It only **starts cooking** (executing) when you ask for the result.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC When you write Spark transformations:
# MAGIC ```python
# MAGIC df = spark.read.csv("path")
# MAGIC df2 = df.filter(...)
# MAGIC df3 = df2.select(...)
# MAGIC ```
# MAGIC
# MAGIC **Spark does NOT execute immediately.** Instead:
# MAGIC 1. ✅ Builds a **logical plan** (sequence of operations)
# MAGIC 2. ✅ Validates the plan (checks column names, types)
# MAGIC 3. ❌ Does **NOT** read data yet
# MAGIC 4. ❌ Does **NOT** apply transformations yet
# MAGIC
# MAGIC **Execution happens only when you trigger an ACTION** (like `.count()`, `.show()`, `.write()`).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❓ Why This Design?
# MAGIC
# MAGIC 1. **Optimization** — Spark can reorder and optimize operations
# MAGIC 2. **Efficiency** — Avoids redundant computations
# MAGIC 3. **Pipelining** — Combines multiple operations into single pass
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛠️ Key Concept:
# MAGIC
# MAGIC **Transformations** = Lazy (just build plan)  
# MAGIC **Actions** = Eager (trigger execution)
# MAGIC
# MAGIC This is where the **DAG** (Directed Acyclic Graph) comes in.

# COMMAND ----------

# DBTITLE 1,Demo: Spark Does Not Execute Immediately
# Let's demonstrate that Spark doesn't execute immediately

print("✅ Step 1: Create a simple DataFrame (in-memory)")
data = [
    (1, "Alice", "Engineering", 95000),
    (2, "Bob", "Marketing", 65000),
    (3, "Charlie", "Engineering", 85000),
    (4, "Diana", "Sales", 75000),
    (5, "Eve", "Engineering", 90000)
]

df = spark.createDataFrame(data, ["id", "name", "department", "salary"])

print("❌ Notice: No execution happened yet!")
print("\n" + "="*60)

print("✅ Step 2: Apply transformations (filter + select)")
df_filtered = df.filter(df.department == "Engineering")
df_result = df_filtered.select("name", "salary")

print("❌ Still no execution! Spark is just building a plan.")
print("\n" + "="*60)

print("✅ Step 3: Trigger an ACTION (show)")
print("🚀 NOW Spark executes everything!\n")

display(df_result)

# COMMAND ----------

# DBTITLE 1,Section 2: DAG (Directed Acyclic Graph)
# MAGIC %md
# MAGIC ## 🌳 Section 2: DAG (Directed Acyclic Graph)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC
# MAGIC A **DAG** is like a **recipe flowchart**:
# MAGIC - Each step depends on the previous step
# MAGIC - You can't go backward (no loops)
# MAGIC - All steps flow in one direction →
# MAGIC
# MAGIC Example:
# MAGIC ```
# MAGIC Get Ingredients → Mix → Bake → Serve
# MAGIC ```
# MAGIC
# MAGIC Spark uses this flowchart to understand **what to do** and **in what order**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **DAG** = **D**irected **A**cyclic **G**raph
# MAGIC
# MAGIC - **Directed:** Operations flow in one direction (read → transform → write)
# MAGIC - **Acyclic:** No cycles/loops (can't go back to previous step)
# MAGIC - **Graph:** Visual representation of dependencies
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 DAG Components:
# MAGIC
# MAGIC 1. **Vertices (Nodes)** = RDDs or DataFrames (data)
# MAGIC 2. **Edges (Arrows)** = Transformations (operations)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔄 How Spark Uses DAG:
# MAGIC
# MAGIC ```
# MAGIC User Code:
# MAGIC   df.read() → filter() → groupBy() → write()
# MAGIC
# MAGIC Spark Builds DAG:
# MAGIC   [Read] → [Filter] → [GroupBy] → [Write]
# MAGIC           ↓
# MAGIC     Logical Plan
# MAGIC           ↓
# MAGIC     Physical Plan (optimized)
# MAGIC           ↓
# MAGIC       Execution
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✨ Benefits of DAG:
# MAGIC
# MAGIC 1. **Optimization** — Spark can rearrange operations for efficiency
# MAGIC 2. **Fault Tolerance** — If a task fails, Spark can replay from DAG
# MAGIC 3. **Lazy Evaluation** — Build full plan before executing
# MAGIC 4. **Pipeline Fusion** — Combine operations to minimize data shuffling
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛠️ Logical vs Physical Plan:
# MAGIC
# MAGIC | **Logical Plan** | **Physical Plan** |
# MAGIC |------------------|-------------------|
# MAGIC | What to do | How to do it |
# MAGIC | High-level operations | Low-level execution steps |
# MAGIC | Filter, Select, Join | Scan, Exchange, HashAggregate |
# MAGIC | Not optimized | Optimized by Catalyst |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔍 Example DAG Flow:
# MAGIC
# MAGIC ```
# MAGIC          [Read CSV]
# MAGIC               |
# MAGIC               v
# MAGIC          [Filter dept=Engineering]
# MAGIC               |
# MAGIC               v
# MAGIC          [Select name, salary]
# MAGIC               |
# MAGIC               v
# MAGIC          [OrderBy salary DESC]
# MAGIC               |
# MAGIC               v
# MAGIC          [Write Delta]
# MAGIC ```
# MAGIC
# MAGIC Each arrow represents a **transformation**, and Spark builds this graph **before** executing anything.

# COMMAND ----------

# DBTITLE 1,Demo: View Logical and Physical Plans
# Let's see how Spark builds execution plans

print("✅ Creating a DataFrame with multiple transformations\n")

data = [
    (1, "Alice", "Engineering", 95000),
    (2, "Bob", "Marketing", 65000),
    (3, "Charlie", "Engineering", 85000),
    (4, "Diana", "Sales", 75000),
    (5, "Eve", "Engineering", 90000),
    (6, "Frank", "Engineering", 88000),
    (7, "Grace", "Marketing", 70000)
]

df = spark.createDataFrame(data, ["id", "name", "department", "salary"])

# Apply transformations
df_result = (df
    .filter(df.department == "Engineering")
    .select("name", "salary")
    .orderBy("salary", ascending=False)
)

print("="*70)
print("📊 LOGICAL PLAN (What to do)")
print("="*70)
df_result.explain(mode="simple")

print("\n" + "="*70)
print("🚀 PHYSICAL PLAN (How to execute - optimized)")
print("="*70)
df_result.explain(mode="formatted")

print("\n" + "="*70)
print("✅ Notice: Spark optimizes the plan before execution!")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Section 3: Lazy Evaluation
# MAGIC %md
# MAGIC ## 😴 Section 3: Lazy Evaluation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC
# MAGIC **Lazy evaluation** = "I'll do it later!"
# MAGIC
# MAGIC Imagine your mom asks you to:
# MAGIC 1. Clean your room
# MAGIC 2. Do homework
# MAGIC 3. Take out trash
# MAGIC
# MAGIC You say "OK" to each task but don't do anything **until** she says "Show me your clean room!"
# MAGIC
# MAGIC Spark is the same — it waits until you ask for results.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Lazy Evaluation** means:
# MAGIC - **Transformations** are recorded but NOT executed
# MAGIC - Execution happens ONLY when an **Action** is triggered
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔄 Transformations vs Actions:
# MAGIC
# MAGIC | **Transformations (Lazy)** | **Actions (Eager)** |
# MAGIC |---------------------------|---------------------|
# MAGIC | `select()` | `show()` |
# MAGIC | `filter()` | `count()` |
# MAGIC | `groupBy()` | `collect()` |
# MAGIC | `join()` | `write()` |
# MAGIC | `orderBy()` | `take()` |
# MAGIC | `withColumn()` | `first()` |
# MAGIC
# MAGIC **Key Rule:**
# MAGIC - **Transformations** return a DataFrame (lazy)
# MAGIC - **Actions** return a result or write data (eager)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✨ Benefits of Lazy Evaluation:
# MAGIC
# MAGIC 1. **Optimization**
# MAGIC    - Spark sees the entire pipeline before executing
# MAGIC    - Can combine operations (filter + select = single pass)
# MAGIC    - Can eliminate redundant steps
# MAGIC
# MAGIC 2. **Efficiency**
# MAGIC    - Avoids loading data multiple times
# MAGIC    - Minimizes data movement
# MAGIC    - Reduces memory usage
# MAGIC
# MAGIC 3. **Pipelining**
# MAGIC    - Multiple transformations can be executed in a single stage
# MAGIC    - Data flows through transformations without materialization
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Execution Flow:
# MAGIC
# MAGIC ```
# MAGIC Transformations (Lazy):
# MAGIC   df.read()           → Recorded
# MAGIC   df.filter()         → Recorded
# MAGIC   df.select()         → Recorded
# MAGIC   df.groupBy()        → Recorded
# MAGIC                          ↓
# MAGIC                     Build DAG
# MAGIC                          ↓
# MAGIC                     Optimize
# MAGIC                          ↓
# MAGIC Action (Trigger):
# MAGIC   df.count()          → EXECUTE ALL!
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Common Mistake:
# MAGIC
# MAGIC ```python
# MAGIC # This does NOT execute:
# MAGIC df_filtered = df.filter(col("age") > 25)
# MAGIC
# MAGIC # Still does NOT execute:
# MAGIC df_selected = df_filtered.select("name", "age")
# MAGIC
# MAGIC # NOW it executes (action triggered):
# MAGIC df_selected.show()
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔑 Key Takeaway:
# MAGIC
# MAGIC **Think of Spark as a planner:**
# MAGIC - You give it instructions (transformations)
# MAGIC - It plans the best execution path
# MAGIC - It executes only when you demand results (actions)

# COMMAND ----------

# DBTITLE 1,Demo: Lazy Evaluation in Action
from pyspark.sql.functions import col, avg
import time

print("✅ Creating DataFrame\n")
data = [(i, f"Person_{i}", "Dept_" + str(i % 3), 50000 + (i * 1000)) 
        for i in range(1, 101)]

df = spark.createDataFrame(data, ["id", "name", "department", "salary"])

print("="*70)
print("🔵 PHASE 1: Applying Transformations (LAZY - No Execution)")
print("="*70)

start_time = time.time()

print("\n1️⃣ Applying filter()...")
df_filtered = df.filter(col("salary") > 60000)
print("   ✅ Transformation recorded (not executed)")

print("\n2️⃣ Applying select()...")
df_selected = df_filtered.select("name", "department", "salary")
print("   ✅ Transformation recorded (not executed)")

print("\n3️⃣ Applying groupBy() and agg()...")
df_aggregated = df_selected.groupBy("department").agg(avg("salary").alias("avg_salary"))
print("   ✅ Transformation recorded (not executed)")

transform_time = time.time() - start_time

print(f"\n⏱️ Time taken for ALL transformations: {transform_time:.4f} seconds")
print("➡️ Notice: Almost instant! No actual computation happened.")

print("\n" + "="*70)
print("🔴 PHASE 2: Triggering Action (EAGER - Execution Starts)")
print("="*70)

start_time = time.time()

print("\n🚀 Calling show() - This triggers execution of ALL transformations!\n")
display(df_aggregated)

action_time = time.time() - start_time

print(f"\n⏱️ Time taken for action (actual execution): {action_time:.4f} seconds")
print("➡️ Notice: This took longer because Spark executed everything!")

print("\n" + "="*70)
print("💡 Key Insight:")
print("="*70)
print("Transformations = Plan building (instant)")
print("Actions = Execution (takes time)")

# COMMAND ----------

# DBTITLE 1,Demo: Multiple Actions = Multiple Executions
# Important: Each action triggers a separate execution!

print("⚠️ Demonstrating Multiple Actions\n")

data = [(i, f"Employee_{i}", 40000 + (i * 2000)) for i in range(1, 51)]
df = spark.createDataFrame(data, ["id", "name", "salary"])

df_high_salary = df.filter(col("salary") > 60000)

print("="*70)
print("ACTION 1: count()")
print("="*70)
count = df_high_salary.count()
print(f"🚀 Execution triggered! Count: {count}")

print("\n" + "="*70)
print("ACTION 2: show()")
print("="*70)
print("🚀 Execution triggered AGAIN!\n")
display(df_high_salary.limit(5))

print("\n" + "="*70)
print("💡 Key Lesson:")
print("="*70)
print("❌ Each action re-executes the entire DAG (unless using cache/persist)")
print("✅ For production: Minimize actions, or use cache() when needed")
print("\n📌 In this training: We avoid cache() for Serverless optimization")

# COMMAND ----------

# DBTITLE 1,Section 4: Stages & Tasks
# MAGIC %md
# MAGIC ## 🏛️ Section 4: Stages & Tasks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC
# MAGIC Imagine building a LEGO house with your friends:
# MAGIC - **Job** = Build the entire house
# MAGIC - **Stage** = Build one floor (everyone can work on different parts)
# MAGIC - **Task** = Each person builds one room
# MAGIC
# MAGIC Spark breaks big jobs into stages, and stages into tasks so multiple workers can help!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC When Spark executes a job, it organizes work into a hierarchy:
# MAGIC
# MAGIC ```
# MAGIC           JOB
# MAGIC            |
# MAGIC     ----------------
# MAGIC     |      |       |
# MAGIC   Stage  Stage  Stage
# MAGIC     |      |       |
# MAGIC   Tasks Tasks  Tasks
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Job → Stage → Task Hierarchy:
# MAGIC
# MAGIC #### 1️⃣ **Job**
# MAGIC - Triggered by an **action** (count, show, write)
# MAGIC - One action = One job
# MAGIC - Consists of one or more stages
# MAGIC
# MAGIC #### 2️⃣ **Stage**
# MAGIC - A set of tasks that can run in parallel
# MAGIC - Separated by **shuffle boundaries**
# MAGIC - Stages execute sequentially (Stage 1 → Stage 2 → Stage 3)
# MAGIC
# MAGIC #### 3️⃣ **Task**
# MAGIC - Smallest unit of execution
# MAGIC - Runs on a single partition of data
# MAGIC - Multiple tasks run in parallel within a stage
# MAGIC - **Number of tasks = Number of partitions**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔄 What Creates Stage Boundaries?
# MAGIC
# MAGIC **Stages are separated by SHUFFLE operations:**
# MAGIC
# MAGIC **Narrow Transformations** (No Shuffle → Same Stage):
# MAGIC - `filter()`
# MAGIC - `select()`
# MAGIC - `map()`
# MAGIC - `withColumn()`
# MAGIC
# MAGIC **Wide Transformations** (Shuffle Required → New Stage):
# MAGIC - `groupBy()`
# MAGIC - `join()`
# MAGIC - `orderBy()`
# MAGIC - `repartition()`
# MAGIC - `distinct()`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Example Execution Flow:
# MAGIC
# MAGIC ```
# MAGIC Code:
# MAGIC   df.read()              ← Stage 0
# MAGIC     .filter()            ← Stage 0 (narrow)
# MAGIC     .select()            ← Stage 0 (narrow)
# MAGIC     .groupBy()           ← Stage 1 (shuffle boundary!)
# MAGIC     .orderBy()           ← Stage 2 (shuffle boundary!)
# MAGIC     .write()             ← Stage 2
# MAGIC ```
# MAGIC
# MAGIC **Result:** 3 Stages created!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Parallelism:
# MAGIC
# MAGIC | **Level** | **Parallelism** |
# MAGIC |-----------|----------------|
# MAGIC | **Job** | Sequential (one at a time per action) |
# MAGIC | **Stage** | Sequential (Stage 1 → Stage 2) |
# MAGIC | **Task** | **Parallel** (all tasks in stage run together) |
# MAGIC
# MAGIC **Example:**
# MAGIC - Data has 200 partitions
# MAGIC - Stage 1 creates **200 tasks** (all run in parallel)
# MAGIC - If you have 8 executor cores, 8 tasks run at once
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔑 Key Formula:
# MAGIC
# MAGIC ```
# MAGIC Number of Tasks in Stage = Number of Partitions
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Why This Matters:
# MAGIC
# MAGIC 1. **Performance:**
# MAGIC    - More partitions = More parallelism
# MAGIC    - Too few partitions = Underutilized cluster
# MAGIC    - Too many partitions = Overhead
# MAGIC
# MAGIC 2. **Debugging:**
# MAGIC    - Failed task? Check that partition's data
# MAGIC    - Slow stage? Identify shuffle operations
# MAGIC
# MAGIC 3. **Optimization:**
# MAGIC    - Reduce shuffles = Fewer stages = Faster execution
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Narrow vs Wide Transformations:
# MAGIC
# MAGIC #### **Narrow Transformation:**
# MAGIC ```
# MAGIC Partition 1  →  Transformation  →  Partition 1
# MAGIC Partition 2  →  Transformation  →  Partition 2
# MAGIC Partition 3  →  Transformation  →  Partition 3
# MAGIC ```
# MAGIC - No data movement between partitions
# MAGIC - Fast and efficient
# MAGIC - Same stage
# MAGIC
# MAGIC #### **Wide Transformation:**
# MAGIC ```
# MAGIC Partition 1  ↓
# MAGIC Partition 2  →  Shuffle  →  New Partitions
# MAGIC Partition 3  ↑
# MAGIC ```
# MAGIC - Data moves across partitions (shuffle)
# MAGIC - Expensive (network I/O, disk writes)
# MAGIC - Creates new stage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Key Takeaways:
# MAGIC
# MAGIC 1. **Action** → Creates **Job**
# MAGIC 2. **Shuffle** → Creates new **Stage**
# MAGIC 3. **Partition** → Creates **Task**
# MAGIC 4. **More parallelism** = More tasks running simultaneously

# COMMAND ----------

# DBTITLE 1,Section 5: Spark Execution Flow (Detailed)
# MAGIC %md
# MAGIC ## 🚀 Section 5: Spark Execution Flow (Detailed)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC
# MAGIC When you order food delivery:
# MAGIC 1. You place order (write code)
# MAGIC 2. Restaurant receives order (builds plan)
# MAGIC 3. Chef optimizes cooking (Catalyst optimizer)
# MAGIC 4. Assigns to delivery drivers (executors)
# MAGIC 5. Food delivered in parallel (tasks)
# MAGIC
# MAGIC Spark follows the same process!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Complete Execution Lifecycle:
# MAGIC
# MAGIC ```
# MAGIC 👨‍💻 USER CODE
# MAGIC     |
# MAGIC     v
# MAGIC 📋 LOGICAL PLAN (Unresolved)
# MAGIC     |
# MAGIC     v
# MAGIC 🔍 ANALYSIS (Validate columns, types)
# MAGIC     |
# MAGIC     v
# MAGIC 📋 LOGICAL PLAN (Resolved)
# MAGIC     |
# MAGIC     v
# MAGIC ✨ CATALYST OPTIMIZER
# MAGIC     |
# MAGIC     v
# MAGIC 📋 OPTIMIZED LOGICAL PLAN
# MAGIC     |
# MAGIC     v
# MAGIC 🚀 PHYSICAL PLANNING
# MAGIC     |
# MAGIC     v
# MAGIC 📊 PHYSICAL PLAN
# MAGIC     |
# MAGIC     v
# MAGIC 🏛️ CODE GENERATION (Whole-Stage Codegen)
# MAGIC     |
# MAGIC     v
# MAGIC 📦 EXECUTION (Stages → Tasks → Executors)
# MAGIC     |
# MAGIC     v
# MAGIC ✅ RESULT
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔑 Key Components:
# MAGIC
# MAGIC #### 1️⃣ **Logical Plan**
# MAGIC - High-level operations (filter, select, join)
# MAGIC - Language-agnostic representation
# MAGIC - Not executable yet
# MAGIC
# MAGIC #### 2️⃣ **Catalyst Optimizer**
# MAGIC - Rule-based optimization engine
# MAGIC - Applies optimization rules:
# MAGIC   - **Predicate Pushdown** (filter early)
# MAGIC   - **Column Pruning** (read only needed columns)
# MAGIC   - **Constant Folding** (pre-compute constants)
# MAGIC   - **Join Reordering** (optimize join order)
# MAGIC
# MAGIC #### 3️⃣ **Physical Plan**
# MAGIC - Low-level execution operations
# MAGIC - Chooses algorithms (hash join vs sort merge join)
# MAGIC - Decides data exchange strategies
# MAGIC
# MAGIC #### 4️⃣ **Code Generation (Whole-Stage Codegen)**
# MAGIC - Generates optimized Java bytecode
# MAGIC - Combines multiple operations into single function
# MAGIC - Eliminates virtual function calls
# MAGIC - Near-native performance
# MAGIC
# MAGIC #### 5️⃣ **Execution**
# MAGIC - Breaks into stages and tasks
# MAGIC - Distributes to executors
# MAGIC - Executes in parallel
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✨ Catalyst Optimizer (High-Level):
# MAGIC
# MAGIC **What it does:**
# MAGIC - Takes your logical plan
# MAGIC - Applies 100+ optimization rules
# MAGIC - Produces most efficient execution plan
# MAGIC
# MAGIC **Common Optimizations:**
# MAGIC
# MAGIC 1. **Predicate Pushdown:**
# MAGIC ```python
# MAGIC # Your code:
# MAGIC df.select("name", "age").filter(col("age") > 25)
# MAGIC
# MAGIC # Optimized:
# MAGIC df.filter(col("age") > 25).select("name", "age")
# MAGIC # ✅ Filter early = Less data to process
# MAGIC ```
# MAGIC
# MAGIC 2. **Column Pruning:**
# MAGIC ```python
# MAGIC # Your code:
# MAGIC df.select("name")  # Only need name
# MAGIC
# MAGIC # Optimized:
# MAGIC # Reads ONLY "name" column from storage
# MAGIC # ✅ Doesn't read unnecessary columns
# MAGIC ```
# MAGIC
# MAGIC 3. **Constant Folding:**
# MAGIC ```python
# MAGIC # Your code:
# MAGIC df.filter(col("price") > 100 * 2)
# MAGIC
# MAGIC # Optimized:
# MAGIC df.filter(col("price") > 200)
# MAGIC # ✅ Pre-computes 100 * 2 = 200
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Flow with Example:
# MAGIC
# MAGIC **User Code:**
# MAGIC ```python
# MAGIC df = spark.read.parquet("path")
# MAGIC df.filter(col("age") > 25).select("name").show()
# MAGIC ```
# MAGIC
# MAGIC **Step-by-Step:**
# MAGIC
# MAGIC 1. **Logical Plan:**
# MAGIC    - Read Parquet
# MAGIC    - Filter age > 25
# MAGIC    - Select name
# MAGIC
# MAGIC 2. **Catalyst Optimization:**
# MAGIC    - Pushdown filter to Parquet reader
# MAGIC    - Prune unnecessary columns
# MAGIC
# MAGIC 3. **Physical Plan:**
# MAGIC    - FileScan (with filter pushed)
# MAGIC    - Project (select name)
# MAGIC
# MAGIC 4. **Execution:**
# MAGIC    - Create tasks per partition
# MAGIC    - Execute on executors
# MAGIC    - Collect results
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔍 Observability:
# MAGIC
# MAGIC **You can inspect each phase:**
# MAGIC
# MAGIC ```python
# MAGIC # Logical Plan
# MAGIC df.explain(mode="simple")
# MAGIC
# MAGIC # Optimized + Physical Plan
# MAGIC df.explain(mode="extended")
# MAGIC
# MAGIC # Cost-based optimization details
# MAGIC df.explain(mode="cost")
# MAGIC
# MAGIC # Formatted for readability
# MAGIC df.explain(mode="formatted")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚡ Whole-Stage Code Generation:
# MAGIC
# MAGIC **Traditional Execution:**
# MAGIC ```
# MAGIC For each row:
# MAGIC   Call filter()
# MAGIC   Call select()
# MAGIC   Call map()
# MAGIC ```
# MAGIC ❌ Many function calls = Slow
# MAGIC
# MAGIC **Whole-Stage Codegen:**
# MAGIC ```
# MAGIC Generated single function:
# MAGIC   For each row:
# MAGIC     filter + select + map in one pass
# MAGIC ```
# MAGIC ✅ Single function = Fast
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Key Insights:
# MAGIC
# MAGIC 1. **You write high-level code** → Spark optimizes automatically
# MAGIC 2. **Catalyst is smart** → Trust the optimizer
# MAGIC 3. **Physical plan matters** → Check with `.explain()`
# MAGIC 4. **Whole-stage codegen** → Major performance boost
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Summary:
# MAGIC
# MAGIC ```
# MAGIC Your Code → Logical Plan → Optimization → Physical Plan → Execution
# MAGIC    |
# MAGIC    ↓
# MAGIC Spark figures out the BEST way to execute your intent!
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 6: Hands-on Demo - Execution Behavior
# MAGIC %md
# MAGIC ## 🔧 Section 6: Hands-on Demo - Execution Behavior
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Now let's see everything in action with real data operations.
# MAGIC
# MAGIC We'll:
# MAGIC 1. Create sample data
# MAGIC 2. Apply multiple transformations
# MAGIC 3. Trigger actions and observe execution
# MAGIC 4. Analyze the execution plans
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 What We'll Demonstrate:
# MAGIC
# MAGIC - Lazy evaluation in practice
# MAGIC - DAG construction
# MAGIC - Stage boundaries (narrow vs wide transformations)
# MAGIC - Task parallelism

# COMMAND ----------

# DBTITLE 1,Setup: Create Sample Sales Data
from pyspark.sql.functions import col, sum, avg, count, when, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import random
import time

print("✅ Creating Sample Sales Data\n")

# Generate realistic sales data
random.seed(42)

products = ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse"]
regions = ["North", "South", "East", "West"]
status = ["Completed", "Pending", "Cancelled"]

data = []
for i in range(1, 501):
    data.append((
        i,
        f"ORD-{i:05d}",
        random.choice(products),
        random.choice(regions),
        random.randint(1, 10),
        round(random.uniform(100, 2000), 2),
        random.choice(status)
    ))

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("order_id", StringType(), True),
    StructField("product", StringType(), True),
    StructField("region", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("status", StringType(), True)
])

df_sales = spark.createDataFrame(data, schema)

print(f"✅ Created DataFrame with {len(data)} records")
print("\n📊 Sample Data:\n")
display(df_sales.limit(10))

# COMMAND ----------

# DBTITLE 1,Demo: Apply Transformations (Lazy)
print("="*70)
print("🔵 APPLYING TRANSFORMATIONS (LAZY - No Execution Yet)")
print("="*70)

start_time = time.time()

print("\n1️⃣ Transformation: Filter completed orders")
df_completed = df_sales.filter(col("status") == "Completed")
print("   ✅ Recorded (not executed)")

print("\n2️⃣ Transformation: Add total_amount column")
df_with_total = df_completed.withColumn("total_amount", col("quantity") * col("price"))
print("   ✅ Recorded (not executed)")

print("\n3️⃣ Transformation: Filter high-value orders (>5000)")
df_high_value = df_with_total.filter(col("total_amount") > 5000)
print("   ✅ Recorded (not executed)")

print("\n4️⃣ Transformation: Select relevant columns")
df_result = df_high_value.select("order_id", "product", "region", "quantity", "price", "total_amount")
print("   ✅ Recorded (not executed)")

transform_time = time.time() - start_time

print(f"\n⏱️ Total time for 4 transformations: {transform_time:.4f} seconds")
print("➡️ Notice: Instant! Just building the plan.")

print("\n" + "="*70)
print("📊 LOGICAL PLAN (What Spark plans to do)")
print("="*70)
df_result.explain(mode="simple")

# COMMAND ----------

# DBTITLE 1,Demo: Trigger Action - Execution Starts
print("="*70)
print("🔴 TRIGGERING ACTION (Execution Starts Now!)")
print("="*70)

start_time = time.time()

print("\n🚀 Calling count() - This triggers execution of entire DAG!\n")

record_count = df_result.count()

action_time = time.time() - start_time

print(f"\n✅ Result: {record_count} high-value orders found")
print(f"⏱️ Time taken: {action_time:.4f} seconds")
print("➡️ This is when actual computation happened!")

print("\n" + "="*70)
print("💡 Key Observation")
print("="*70)
print("All 4 transformations + action executed together")
print("Spark optimized the entire pipeline before execution")

# COMMAND ----------

# DBTITLE 1,Demo: Wide Transformation - Creates New Stage
print("="*70)
print("🔵 DEMONSTRATING WIDE TRANSFORMATION (Shuffle)")
print("="*70)

print("\n📊 Applying groupBy() - This creates a shuffle boundary!\n")

# Wide transformation: groupBy aggregation
df_aggregated = (df_sales
    .filter(col("status") == "Completed")
    .withColumn("total_amount", col("quantity") * col("price"))
    .groupBy("region", "product")
    .agg(
        sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_order_value"),
        count("*").alias("order_count")
    )
    .orderBy(col("total_revenue").desc())
)

print("="*70)
print("📊 PHYSICAL PLAN (Notice the Exchange/Shuffle operations)")
print("="*70)
df_aggregated.explain(mode="formatted")

print("\n" + "="*70)
print("🚀 Executing aggregation...")
print("="*70)

display(df_aggregated.limit(15))

print("\n" + "="*70)
print("💡 Key Insight")
print("="*70)
print("➡️ groupBy() and orderBy() created shuffle operations")
print("➡️ This results in multiple stages")
print("➡️ Check Spark UI to see stage boundaries!")

# COMMAND ----------

# DBTITLE 1,Section 7: Observing Execution (Conceptual)
# MAGIC %md
# MAGIC ## 👁️ Section 7: Observing Execution (Conceptual)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Spark UI Overview:
# MAGIC
# MAGIC The **Spark UI** provides real-time visibility into job execution.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📍 What to Look For:
# MAGIC
# MAGIC #### 1️⃣ **Jobs Tab**
# MAGIC - Shows all jobs triggered by actions
# MAGIC - Displays job duration and status
# MAGIC - Links to associated stages
# MAGIC
# MAGIC #### 2️⃣ **Stages Tab**
# MAGIC - Shows stages within each job
# MAGIC - Displays:
# MAGIC   - Number of tasks per stage
# MAGIC   - Task duration (min, median, max)
# MAGIC   - Data shuffle metrics
# MAGIC   - Input/output sizes
# MAGIC
# MAGIC #### 3️⃣ **Tasks Tab**
# MAGIC - Individual task execution details
# MAGIC - Shows:
# MAGIC   - Task duration
# MAGIC   - GC time
# MAGIC   - Shuffle read/write
# MAGIC   - Errors and failures
# MAGIC
# MAGIC #### 4️⃣ **SQL Tab** (for DataFrame operations)
# MAGIC - Displays execution plan as visual DAG
# MAGIC - Shows metrics per operator
# MAGIC - Highlights expensive operations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔍 Key Metrics to Monitor:
# MAGIC
# MAGIC | **Metric** | **What It Means** | **Good vs Bad** |
# MAGIC |-----------|------------------|----------------|
# MAGIC | **Number of Stages** | Shuffle operations | Fewer = Better |
# MAGIC | **Task Duration** | Time per task | Balanced = Good |
# MAGIC | **Shuffle Read/Write** | Data movement | Less = Better |
# MAGIC | **GC Time** | Memory pressure | <10% = Good |
# MAGIC | **Task Skew** | Unbalanced partitions | Low skew = Good |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 How to Access Spark UI:
# MAGIC
# MAGIC 1. **In Databricks:**
# MAGIC    - Click on **Cluster** name
# MAGIC    - Go to **Spark UI** tab
# MAGIC    - Navigate to **Jobs**, **Stages**, **SQL** tabs
# MAGIC
# MAGIC 2. **Or:**
# MAGIC    - At bottom of notebook cell execution
# MAGIC    - Click "View" link next to job completion message
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Example: What Good Execution Looks Like:
# MAGIC
# MAGIC ```
# MAGIC Job 1:
# MAGIC   Stage 0: 200 tasks (all ~2 seconds each)  ✅ Good!
# MAGIC   Stage 1: 200 tasks (all ~3 seconds each)  ✅ Good!
# MAGIC
# MAGIC Shuffle: 100 MB read, 80 MB written  ✅ Acceptable
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Example: What Bad Execution Looks Like:
# MAGIC
# MAGIC ```
# MAGIC Job 1:
# MAGIC   Stage 0: 200 tasks
# MAGIC     - 190 tasks: ~2 seconds  ✅
# MAGIC     - 10 tasks: ~60 seconds   ❌ Skew!
# MAGIC
# MAGIC Shuffle: 10 GB read, 8 GB written  ❌ Too much data movement!
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Optimization Tips:
# MAGIC
# MAGIC 1. **Reduce Stages:**
# MAGIC    - Minimize shuffle operations
# MAGIC    - Combine transformations
# MAGIC
# MAGIC 2. **Balance Tasks:**
# MAGIC    - Repartition skewed data
# MAGIC    - Use appropriate partition count
# MAGIC
# MAGIC 3. **Minimize Shuffles:**
# MAGIC    - Use broadcast joins for small tables
# MAGIC    - Filter early (predicate pushdown)
# MAGIC    - Select only needed columns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Key Takeaway:
# MAGIC
# MAGIC The Spark UI is your **execution magnifying glass** — use it to:
# MAGIC - Understand what Spark is doing
# MAGIC - Identify bottlenecks
# MAGIC - Optimize performance

# COMMAND ----------

# DBTITLE 1,Section 8: End-to-End Execution Pipeline
# MAGIC %md
# MAGIC ## 🔄 Section 8: End-to-End Execution Pipeline
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Let's build a complete pipeline demonstrating:
# MAGIC 1. Multiple transformations
# MAGIC 2. Wide and narrow transformations
# MAGIC 3. Final action (write to Delta)
# MAGIC 4. Execution triggered only at final step
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Pipeline Flow:
# MAGIC
# MAGIC ```
# MAGIC Read Data
# MAGIC     ↓
# MAGIC Filter (Narrow)
# MAGIC     ↓
# MAGIC WithColumn (Narrow)
# MAGIC     ↓
# MAGIC GroupBy (Wide - Shuffle)
# MAGIC     ↓
# MAGIC OrderBy (Wide - Shuffle)
# MAGIC     ↓
# MAGIC Write Delta (Action - Triggers Execution)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,End-to-End Pipeline Demo
from pyspark.sql.functions import col, sum, avg, count, round as spark_round, current_timestamp

print("="*70)
print("🚀 BUILDING END-TO-END EXECUTION PIPELINE")
print("="*70)

# Step 1: Create source data
print("\n1️⃣ Creating source data (this executes immediately for createDataFrame)")

products = ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse", "Headphones"]
regions = ["North", "South", "East", "West", "Central"]
status_list = ["Completed", "Pending", "Cancelled"]

import random
random.seed(100)

data = []
for i in range(1, 1001):
    data.append((
        i,
        f"ORD-{i:05d}",
        random.choice(products),
        random.choice(regions),
        random.randint(1, 15),
        round(random.uniform(50, 3000), 2),
        random.choice(status_list),
        f"2026-04-{random.randint(1, 20):02d}"
    ))

df_orders = spark.createDataFrame(data, 
    ["id", "order_id", "product", "region", "quantity", "unit_price", "status", "order_date"])

print(f"   ✅ Created {df_orders.count()} orders")

# Step 2-6: Build transformation pipeline (LAZY)
print("\n2️⃣ Applying filter (Narrow - Same Stage)")
print("   ✅ Transformation recorded")

print("\n3️⃣ Adding calculated columns (Narrow - Same Stage)")
print("   ✅ Transformation recorded")

print("\n4️⃣ Applying groupBy + aggregation (Wide - New Stage!)")
print("   ✅ Transformation recorded")

print("\n5️⃣ Applying orderBy (Wide - New Stage!)")
print("   ✅ Transformation recorded")

print("\n6️⃣ Adding metadata columns (Narrow - Same Stage)")
print("   ✅ Transformation recorded")

# Complete pipeline
df_pipeline = (df_orders
    .filter(col("status") == "Completed")  # Narrow
    .withColumn("total_amount", col("quantity") * col("unit_price"))  # Narrow
    .withColumn("revenue_category", 
        when(col("total_amount") > 10000, "High")
        .when(col("total_amount") > 5000, "Medium")
        .otherwise("Low"))  # Narrow
    .groupBy("region", "product", "revenue_category")  # Wide - Shuffle!
    .agg(
        sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_revenue"),
        count("*").alias("order_count")
    )
    .withColumn("avg_revenue_rounded", spark_round(col("avg_revenue"), 2))  # Narrow
    .orderBy(col("total_revenue").desc())  # Wide - Shuffle!
    .withColumn("processed_at", current_timestamp())  # Narrow
)

print("\n" + "="*70)
print("➡️ All transformations recorded. NO EXECUTION YET!")
print("="*70)

print("\n" + "="*70)
print("📊 EXECUTION PLAN (Before Execution)")
print("="*70)
df_pipeline.explain(mode="simple")

# COMMAND ----------

# DBTITLE 1,Trigger Execution and Display Results
print("="*70)
print("🔴 TRIGGERING EXECUTION (Action: show)")
print("="*70)

print("\n🚀 Calling show() - This triggers execution of ENTIRE pipeline!\n")

import time
start_time = time.time()

display(df_pipeline.limit(20))

execution_time = time.time() - start_time

print(f"\n⏱️ Total execution time: {execution_time:.2f} seconds")

print("\n" + "="*70)
print("💡 KEY INSIGHTS")
print("="*70)
print("✅ All transformations executed together")
print("✅ Spark optimized the entire DAG before execution")
print("✅ Multiple stages created due to shuffle operations (groupBy, orderBy)")
print("✅ Tasks executed in parallel within each stage")
print("\n➡️ Check Spark UI to see Jobs, Stages, and Tasks!")

# COMMAND ----------

# DBTITLE 1,Optional: Write to Delta (Another Action)
# MAGIC %md
# MAGIC ### 💾 Optional: Write to Delta
# MAGIC
# MAGIC Uncomment below to write results to Unity Catalog.
# MAGIC
# MAGIC **Note:** This would trigger another complete execution of the pipeline!
# MAGIC
# MAGIC ```python
# MAGIC # Define output path
# MAGIC # output_path = "/Volumes/<catalog>/<schema>/<volume>/sales_summary"
# MAGIC
# MAGIC # Write as Delta (This is an ACTION - triggers execution)
# MAGIC # df_pipeline.write.format("delta").mode("overwrite").save(output_path)
# MAGIC
# MAGIC # print(f"✅ Data written to: {output_path}")
# MAGIC ```
# MAGIC
# MAGIC ⚠️ **Important:** Each action (show, write, count) triggers a separate execution unless you use cache/persist.

# COMMAND ----------

# DBTITLE 1,Genie Code Agent Usage Examples
# MAGIC %md
# MAGIC ## 🧞 Genie Code Agent Usage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 How to Use Genie Code for Spark Internals:
# MAGIC
# MAGIC You can ask Genie Code to help with understanding and optimizing Spark execution:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Example Prompts:**
# MAGIC
# MAGIC 1. **Explain Spark DAG:**
# MAGIC    ```
# MAGIC    Prompt: "Explain how Spark builds a DAG for my transformations"
# MAGIC    ```
# MAGIC
# MAGIC 2. **Demonstrate Lazy Evaluation:**
# MAGIC    ```
# MAGIC    Prompt: "Show me an example of lazy evaluation with filter and groupBy"
# MAGIC    ```
# MAGIC
# MAGIC 3. **Analyze Execution Plan:**
# MAGIC    ```
# MAGIC    Prompt: "Analyze the execution plan for this DataFrame and explain the stages"
# MAGIC    ```
# MAGIC
# MAGIC 4. **Optimize Pipeline:**
# MAGIC    ```
# MAGIC    Prompt: "Optimize this Spark pipeline to reduce shuffle operations"
# MAGIC    ```
# MAGIC
# MAGIC 5. **Explain Physical Plan:**
# MAGIC    ```
# MAGIC    Prompt: "What does this physical plan tell me about my query performance?"
# MAGIC    ```
# MAGIC
# MAGIC 6. **Debug Performance:**
# MAGIC    ```
# MAGIC    Prompt: "Why is my groupBy operation slow? How can I optimize it?"
# MAGIC    ```
# MAGIC
# MAGIC 7. **Understand Stages:**
# MAGIC    ```
# MAGIC    Prompt: "Explain why this query creates 3 stages"
# MAGIC    ```
# MAGIC
# MAGIC 8. **Compare Approaches:**
# MAGIC    ```
# MAGIC    Prompt: "Compare narrow vs wide transformations with examples"
# MAGIC    ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Pro Tips:
# MAGIC
# MAGIC - Ask Genie to **explain execution plans** from your queries
# MAGIC - Request **optimization suggestions** for slow queries
# MAGIC - Get **visual explanations** of DAG construction
# MAGIC - Ask for **best practices** to minimize shuffles
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Genie Code is your Spark execution expert!** 🚀

# COMMAND ----------

# DBTITLE 1,Final Summary and Key Takeaways
# MAGIC %md
# MAGIC ## 🎓 Final Summary and Key Takeaways
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Key Learnings:
# MAGIC
# MAGIC #### 1️⃣ **DAG (Directed Acyclic Graph)**
# MAGIC - Spark builds a DAG of operations before execution
# MAGIC - DAG represents dependencies between transformations
# MAGIC - Allows Spark to optimize the entire pipeline
# MAGIC - Enables fault tolerance through lineage tracking
# MAGIC
# MAGIC #### 2️⃣ **Lazy Evaluation**
# MAGIC - **Transformations** are lazy (recorded, not executed)
# MAGIC - **Actions** trigger execution
# MAGIC - Benefits: optimization, efficiency, pipelining
# MAGIC - Spark sees the complete picture before executing
# MAGIC
# MAGIC #### 3️⃣ **Stages & Tasks**
# MAGIC - **Job** = triggered by action
# MAGIC - **Stage** = group of tasks between shuffle boundaries
# MAGIC - **Task** = smallest unit of work (1 task per partition)
# MAGIC - **Shuffle** operations create stage boundaries
# MAGIC
# MAGIC #### 4️⃣ **Execution Flow**
# MAGIC ```
# MAGIC Code → Logical Plan → Catalyst Optimizer → Physical Plan → Execution
# MAGIC ```
# MAGIC
# MAGIC #### 5️⃣ **Narrow vs Wide Transformations**
# MAGIC - **Narrow** (no shuffle): filter, select, map, withColumn
# MAGIC - **Wide** (shuffle): groupBy, join, orderBy, repartition
# MAGIC - Wide transformations are expensive (network I/O)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Optimization Principles:
# MAGIC
# MAGIC 1. **Minimize Shuffles**
# MAGIC    - Reduce wide transformations
# MAGIC    - Use broadcast joins for small tables
# MAGIC    - Filter early, aggregate late
# MAGIC
# MAGIC 2. **Leverage Catalyst Optimizer**
# MAGIC    - Trust Spark's optimizer
# MAGIC    - Write clear, logical code
# MAGIC    - Check execution plans with `.explain()`
# MAGIC
# MAGIC 3. **Understand Parallelism**
# MAGIC    - Tasks = Partitions
# MAGIC    - Balance partition sizes
# MAGIC    - Avoid partition skew
# MAGIC
# MAGIC 4. **Monitor Execution**
# MAGIC    - Use Spark UI
# MAGIC    - Check stage durations
# MAGIC    - Identify bottlenecks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Common Mistakes:
# MAGIC
# MAGIC 1. **Triggering Multiple Actions Unnecessarily**
# MAGIC    ```python
# MAGIC    df.count()  # Action 1 - Full execution
# MAGIC    df.show()   # Action 2 - Full execution again!
# MAGIC    ```
# MAGIC    ❌ Each action re-executes the DAG
# MAGIC
# MAGIC 2. **Not Understanding Lazy Execution**
# MAGIC    ```python
# MAGIC    df_filtered = df.filter(...)  # Thinks execution happened
# MAGIC    ```
# MAGIC    ❌ No execution yet! Only recorded.
# MAGIC
# MAGIC 3. **Misinterpreting Stages/Tasks**
# MAGIC    - Thinking stages run in parallel (they don't)
# MAGIC    - Confusing tasks with stages
# MAGIC
# MAGIC 4. **Ignoring Execution Plans**
# MAGIC    - Not using `.explain()` to check optimization
# MAGIC    - Missing shuffle operations
# MAGIC
# MAGIC 5. **Over-shuffling Data**
# MAGIC    - Too many groupBy, join, orderBy operations
# MAGIC    - Not filtering before aggregation
# MAGIC
# MAGIC 6. **Assuming Immediate Execution**
# MAGIC    - Debugging transformations without triggering actions
# MAGIC    - Not understanding when computation actually happens
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👥 Interview Questions (10 Essential):
# MAGIC
# MAGIC #### **Conceptual Questions:**
# MAGIC
# MAGIC 1. **What is a DAG in Spark, and why is it important?**
# MAGIC    - Expected: Explain directed acyclic graph, lineage, optimization
# MAGIC
# MAGIC 2. **Explain lazy evaluation in Spark. What are its benefits?**
# MAGIC    - Expected: Transformations vs actions, optimization opportunities
# MAGIC
# MAGIC 3. **What is the difference between narrow and wide transformations?**
# MAGIC    - Expected: Data movement, shuffle, stage boundaries
# MAGIC
# MAGIC 4. **How does Spark break a job into stages and tasks?**
# MAGIC    - Expected: Job hierarchy, shuffle boundaries, partitions
# MAGIC
# MAGIC 5. **What is the Catalyst Optimizer? Name 3 optimizations it performs.**
# MAGIC    - Expected: Predicate pushdown, column pruning, constant folding
# MAGIC
# MAGIC #### **Practical Questions:**
# MAGIC
# MAGIC 6. **When you call `df.filter().select().groupBy().show()`, when does execution happen?**
# MAGIC    - Expected: At `.show()` (the action)
# MAGIC
# MAGIC 7. **How many stages would this create: `read → filter → groupBy → orderBy → write`?**
# MAGIC    - Expected: Likely 3-4 stages (groupBy and orderBy cause shuffles)
# MAGIC
# MAGIC 8. **What happens if you call `.count()` and then `.show()` on the same DataFrame?**
# MAGIC    - Expected: Two separate executions (unless cached)
# MAGIC
# MAGIC 9. **How can you view the execution plan of a DataFrame operation?**
# MAGIC    - Expected: `.explain()`, `.explain(mode="formatted")`, Spark UI
# MAGIC
# MAGIC 10. **What is whole-stage code generation, and why is it beneficial?**
# MAGIC     - Expected: Combines operations into single function, reduces overhead
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔑 Quick Reference:
# MAGIC
# MAGIC | **Concept** | **Key Point** |
# MAGIC |------------|---------------|
# MAGIC | **DAG** | Blueprint of operations, enables optimization |
# MAGIC | **Lazy Eval** | Transformations recorded, actions execute |
# MAGIC | **Stages** | Groups of tasks between shuffles |
# MAGIC | **Tasks** | One task per partition, run in parallel |
# MAGIC | **Narrow** | No shuffle, fast, same stage |
# MAGIC | **Wide** | Shuffle required, expensive, new stage |
# MAGIC | **Catalyst** | Rule-based optimizer |
# MAGIC | **Codegen** | Generates optimized bytecode |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Next Steps:
# MAGIC
# MAGIC 1. Practice with Spark UI to observe execution
# MAGIC 2. Experiment with `.explain()` on different queries
# MAGIC 3. Identify shuffle operations in your pipelines
# MAGIC 4. Learn advanced optimization techniques (broadcast joins, bucketing)
# MAGIC 5. Study performance tuning and partition management
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✅ Congratulations!
# MAGIC
# MAGIC You now understand:
# MAGIC - How Spark thinks (DAG)
# MAGIC - Why Spark waits (Lazy Evaluation)
# MAGIC - How Spark executes (Stages, Tasks, Parallelism)
# MAGIC - How to optimize (Minimize shuffles, leverage Catalyst)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **👨‍💻 Author: TRRaveendra**  
# MAGIC **🏷️ Watermark: @TRRaveendra**  
# MAGIC **🚀 Platform: Databricks Serverless + Unity Catalog**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Happy Sparking!** ⚡🚀