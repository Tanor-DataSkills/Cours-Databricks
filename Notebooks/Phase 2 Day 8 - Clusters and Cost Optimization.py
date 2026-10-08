# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # 💰 Data Engineering Training — Phase 2 Day 8  
# MAGIC ## ⚙️ Clusters, Serverless & Cost Optimization  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - Cluster Types (All-purpose, Job, Serverless)  
# MAGIC - Auto Scaling & Auto Termination  
# MAGIC - Cost Optimization Strategies  
# MAGIC - Serverless-First Best Practices  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### ⚙️ Platform: Databricks (Serverless + Unity Catalog)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Objective:
# MAGIC Understand cluster types, how serverless compute works, and how to optimize cost for enterprise-scale data engineering workloads.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Engineering Constraints:
# MAGIC - ✅ Use Databricks Serverless Compute wherever applicable
# MAGIC - ❌ DO NOT use RDDs
# MAGIC - ❌ DO NOT use cache() / persist()
# MAGIC - ❌ DO NOT use /tmp or local storage
# MAGIC - ✅ Use Unity Catalog Volumes for all data access
# MAGIC - ✅ Promote serverless-first architecture

# COMMAND ----------

# DBTITLE 1,Section 1 - Cluster Types Overview
# MAGIC %md
# MAGIC # 📊 Section 1: Cluster Types Overview
# MAGIC
# MAGIC ## 🧠 ELI5 (Explain Like I'm 5):
# MAGIC Imagine you need a computer to do your homework:
# MAGIC - **All-purpose cluster** = Your family computer that stays on all day for anyone to use
# MAGIC - **Job cluster** = A computer that turns on only when you need to do homework, then turns off
# MAGIC - **Serverless** = Magic computer that appears instantly when you need it, with exactly the right power, then disappears
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### **1️⃣ All-Purpose Clusters (Interactive Workloads)**
# MAGIC - **Purpose**: Shared compute for interactive development, ad-hoc analysis, notebooks
# MAGIC - **Lifecycle**: Manual start/stop or auto-termination based on idle timeout
# MAGIC - **Use Cases**: Data exploration, prototyping, collaborative development
# MAGIC - **Cost Model**: Charged for entire cluster uptime (even if idle)
# MAGIC - **Concurrency**: Supports multiple users and notebooks simultaneously
# MAGIC
# MAGIC ### **2️⃣ Job Clusters (Automated Pipelines)**
# MAGIC - **Purpose**: Ephemeral compute created for specific job execution
# MAGIC - **Lifecycle**: Auto-created at job start, auto-terminated at job completion
# MAGIC - **Use Cases**: Production ETL pipelines, scheduled workflows
# MAGIC - **Cost Model**: Pay only for job execution time
# MAGIC - **Isolation**: Dedicated resources per job (no resource contention)
# MAGIC
# MAGIC ### **3️⃣ Serverless Compute (Recommended Modern Approach)**
# MAGIC - **Purpose**: Fully managed, auto-scaling compute with zero cluster management
# MAGIC - **Lifecycle**: Instantly available, auto-scales based on workload, auto-releases resources
# MAGIC - **Use Cases**: All workloads (notebooks, jobs, SQL, DLT pipelines)
# MAGIC - **Cost Model**: Pay only for actual compute used (per-second billing)
# MAGIC - **Benefits**: Fastest startup, optimal resource utilization, no tuning required
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔑 Key Principle:
# MAGIC > **Serverless-First Strategy**: Always prefer serverless unless you have specific technical constraints (e.g., custom libraries requiring specific cluster configs, GPU workloads not yet supported in serverless).

# COMMAND ----------

# DBTITLE 1,Cluster Comparison Table
# MAGIC %md
# MAGIC ## 📊 Cluster Types Comparison
# MAGIC
# MAGIC | Feature | All-Purpose Cluster | Job Cluster | Serverless Compute |
# MAGIC |---------|---------------------|-------------|--------------------|
# MAGIC | **Startup Time** | 5-10 minutes | 5-10 minutes | < 1 minute |
# MAGIC | **Management** | Manual (user-managed) | Auto-managed per job | Fully managed (zero config) |
# MAGIC | **Auto-Scaling** | Configurable (min/max workers) | Configurable | Intelligent auto-scaling |
# MAGIC | **Auto-Termination** | Idle timeout (e.g., 120 min) | Immediate after job | Instant resource release |
# MAGIC | **Cost Efficiency** | ⚠️ Low (idle time waste) | ✅ Medium | ✅✅ High (per-second billing) |
# MAGIC | **Use Case** | Development, exploration | Production pipelines | All workloads (recommended) |
# MAGIC | **Sharing** | Multi-user, multi-notebook | Single job isolation | Intelligent resource pooling |
# MAGIC | **Cold Start** | Requires cluster start | Requires cluster start | Instant availability |
# MAGIC | **Resource Optimization** | Manual tuning required | Manual tuning required | Automatic optimization |
# MAGIC | **Unity Catalog** | Supported | Supported | Native integration |
# MAGIC | **Best For** | Team collaboration | Scheduled batch jobs | Everything (modern default) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 When to Use Each:
# MAGIC
# MAGIC ### 🟢 **Use All-Purpose Clusters When**:
# MAGIC - Multiple team members need shared interactive environment
# MAGIC - Development/testing with specific library versions
# MAGIC - Long-running interactive analysis sessions
# MAGIC
# MAGIC ### 🟡 **Use Job Clusters When**:
# MAGIC - Running scheduled production pipelines
# MAGIC - Need complete job isolation
# MAGIC - Serverless is not available for your workload
# MAGIC
# MAGIC ### 🟢 **Use Serverless Compute When** (95% of cases):
# MAGIC - ✅ Any notebook execution
# MAGIC - ✅ Any Databricks Job
# MAGIC - ✅ SQL queries and dashboards
# MAGIC - ✅ Lakeflow Spark Declarative Pipelines
# MAGIC - ✅ Auto Loader ingestion
# MAGIC - ✅ Cost optimization is priority
# MAGIC - ✅ Fast startup is important

# COMMAND ----------

# DBTITLE 1,Section 2 - Serverless Architecture
# MAGIC %md
# MAGIC # ☁️ Section 2: Serverless Architecture Deep Dive
# MAGIC
# MAGIC ## 🧠 ELI5:
# MAGIC Serverless is like Uber for computers:
# MAGIC - You don't own the car (no cluster to manage)
# MAGIC - A car appears exactly when you need it (instant provisioning)
# MAGIC - It's the right size for your trip (auto-scaling)
# MAGIC - You pay only for the ride (per-second billing)
# MAGIC - When you arrive, the car leaves (auto-release)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ How Serverless Compute Works:
# MAGIC
# MAGIC ### **1️⃣ Auto-Provisioning**
# MAGIC ```
# MAGIC User Request → Databricks Control Plane → Resource Pool → Instant Allocation
# MAGIC ```
# MAGIC - **Traditional**: User creates cluster → Wait 5-10 min → Cluster ready
# MAGIC - **Serverless**: User runs notebook/job → Resources allocated in <1 min → Execution starts
# MAGIC
# MAGIC **Behind the Scenes**:
# MAGIC - Databricks maintains warm pools of compute resources
# MAGIC - Resources are pre-initialized with Spark runtime
# MAGIC - User workloads are scheduled on available capacity
# MAGIC - No cluster creation delay
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **2️⃣ Intelligent Auto-Scaling**
# MAGIC
# MAGIC **Horizontal Scaling (Dynamic Worker Allocation)**:
# MAGIC ```
# MAGIC Workload Start: 2 workers
# MAGIC ↓
# MAGIC High Shuffle: Scale to 10 workers
# MAGIC ↓
# MAGIC Reduction Phase: Scale down to 4 workers
# MAGIC ↓
# MAGIC Final Aggregation: Scale to 2 workers
# MAGIC ```
# MAGIC
# MAGIC **Key Behaviors**:
# MAGIC - Scales based on actual data volume and complexity
# MAGIC - Adds workers when detecting shuffle or wide transformations
# MAGIC - Removes workers when parallelism decreases
# MAGIC - Sub-minute scaling decisions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **3️⃣ Automatic Resource Release**
# MAGIC
# MAGIC **Traditional Cluster**:
# MAGIC ```
# MAGIC Job Completes → Cluster Idle → Waste $ for 120 min → Auto-terminate
# MAGIC ```
# MAGIC
# MAGIC **Serverless**:
# MAGIC ```
# MAGIC Job Completes → Instant Resource Release → Zero idle cost
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Serverless Benefits:
# MAGIC
# MAGIC ### **1. Zero Cluster Management**
# MAGIC - No cluster creation/configuration
# MAGIC - No DBR version management
# MAGIC - No library installation conflicts
# MAGIC - No tuning (workers, instance types, auto-scaling thresholds)
# MAGIC
# MAGIC ### **2. Cost Efficiency**
# MAGIC - **Per-second billing** (not hourly)
# MAGIC - **No idle waste** (resources released immediately)
# MAGIC - **Optimal sizing** (automatic right-sizing per query)
# MAGIC - **20-40% cost reduction** vs. traditional clusters (industry average)
# MAGIC
# MAGIC ### **3. Performance**
# MAGIC - **Faster startup**: <1 min vs. 5-10 min
# MAGIC - **Better concurrency**: Intelligent resource sharing
# MAGIC - **Automatic optimization**: Query optimization built-in
# MAGIC
# MAGIC ### **4. Enterprise Features**
# MAGIC - Native Unity Catalog integration
# MAGIC - Enhanced security (no cluster access, no init scripts)
# MAGIC - Automatic updates and patching
# MAGIC - Consistent runtime versions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📐 Serverless Architecture Diagram:
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────┐
# MAGIC │   User Submits Workload      │
# MAGIC │   (Notebook / Job / SQL)     │
# MAGIC └────────────┬─────────────────┘
# MAGIC              │
# MAGIC              │
# MAGIC ┌────────────┴─────────────────┐
# MAGIC │   Databricks Control Plane    │
# MAGIC │   - Workload Analysis         │
# MAGIC │   - Resource Allocation       │
# MAGIC │   - Auto-Scaling Logic        │
# MAGIC └────────────┬─────────────────┘
# MAGIC              │
# MAGIC              │
# MAGIC ┌────────────┴─────────────────┐
# MAGIC │   Warm Resource Pool          │
# MAGIC │   [Worker] [Worker] [Worker]  │
# MAGIC │   Pre-initialized Spark       │
# MAGIC └────────────┬─────────────────┘
# MAGIC              │
# MAGIC              │
# MAGIC ┌────────────┴─────────────────┐
# MAGIC │   Workload Execution          │
# MAGIC │   - Dynamic Scaling           │
# MAGIC │   - Automatic Optimization    │
# MAGIC │   - Instant Release           │
# MAGIC └──────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📌 Key Takeaway:
# MAGIC > **Serverless is not just "easier" — it's architecturally superior for 95% of data engineering workloads.**

# COMMAND ----------

# DBTITLE 1,Section 3 - Auto Scaling and Auto Termination
# MAGIC %md
# MAGIC # 📈 Section 3: Auto Scaling & Auto Termination
# MAGIC
# MAGIC ## 🧠 ELI5:
# MAGIC **Auto-Scaling** = Like a restaurant that adds more cooks when it's busy, and sends them home when it's quiet
# MAGIC **Auto-Termination** = Like a car that turns itself off when you forget to (saves fuel)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Auto-Scaling Deep Dive
# MAGIC
# MAGIC ### **What is Auto-Scaling?**
# MAGIC Dynamic adjustment of compute resources (workers) based on workload demand.
# MAGIC
# MAGIC ### **Types of Scaling**:
# MAGIC
# MAGIC #### **1. Horizontal Scaling (Worker Count)**
# MAGIC ```
# MAGIC Low Load:    [Driver] → [Worker 1] [Worker 2]
# MAGIC High Load:   [Driver] → [Worker 1] [Worker 2] [Worker 3] [Worker 4] ... [Worker 10]
# MAGIC Scale Down:  [Driver] → [Worker 1] [Worker 2]
# MAGIC ```
# MAGIC
# MAGIC #### **2. Vertical Scaling (Instance Type)**
# MAGIC - Serverless handles this automatically
# MAGIC - Traditional clusters: Manual selection (e.g., r5.xlarge vs r5.4xlarge)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Auto-Scaling in Action (Conceptual Example)
# MAGIC
# MAGIC ### **Scenario: Daily Sales ETL Pipeline**
# MAGIC
# MAGIC ```python
# MAGIC # Conceptual workflow (not actual code)
# MAGIC
# MAGIC Step 1: Read 10 GB data
# MAGIC → Databricks allocates 2 workers
# MAGIC
# MAGIC Step 2: Complex join (requires shuffle)
# MAGIC → Data grows to 100 GB in-memory
# MAGIC → Databricks detects high shuffle
# MAGIC → Scales to 8 workers
# MAGIC
# MAGIC Step 3: Aggregation (reduces to 1 GB)
# MAGIC → Databricks scales down to 3 workers
# MAGIC
# MAGIC Step 4: Write final Delta table
# MAGIC → Small write operation
# MAGIC → Scales to 1 worker
# MAGIC ```
# MAGIC
# MAGIC ### **Key Metrics Triggering Scale-Up**:
# MAGIC - 📈 **Task queue depth** (many tasks waiting)
# MAGIC - 📈 **Shuffle volume** (data movement across nodes)
# MAGIC - 📈 **Memory pressure** (approaching limits)
# MAGIC - 📈 **CPU utilization** (workers at capacity)
# MAGIC
# MAGIC ### **Key Metrics Triggering Scale-Down**:
# MAGIC - 📉 **Idle workers** (no assigned tasks)
# MAGIC - 📉 **Low task queue** (few pending tasks)
# MAGIC - 📉 **Completion of wide transformations** (shuffle complete)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⏱️ Auto-Termination Deep Dive
# MAGIC
# MAGIC ### **What is Auto-Termination?**
# MAGIC Automatic shutdown of compute resources after a period of inactivity.
# MAGIC
# MAGIC ### **Traditional Cluster Auto-Termination**:
# MAGIC
# MAGIC ```
# MAGIC Cluster Created (10:00 AM)
# MAGIC ↓
# MAGIC User runs notebook (10:15 AM)
# MAGIC ↓
# MAGIC Notebook completes (10:30 AM)
# MAGIC ↓
# MAGIC Cluster IDLE (no activity)
# MAGIC ↓
# MAGIC Auto-termination timer starts (default: 120 minutes)
# MAGIC ↓
# MAGIC Still no activity at 12:30 PM
# MAGIC ↓
# MAGIC Cluster TERMINATED
# MAGIC ```
# MAGIC
# MAGIC **Cost Impact**:
# MAGIC - **Without auto-termination**: Cluster runs 24/7 = $$$
# MAGIC - **With 120-min timeout**: Wasted 2 hours of idle time = $$
# MAGIC - **With serverless**: Zero idle time = $
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Auto-Termination Settings:
# MAGIC
# MAGIC | Setting | Use Case | Cost Efficiency |
# MAGIC |---------|----------|----------------|
# MAGIC | **No auto-termination** | ❌ Never use (production) | Very Low |
# MAGIC | **30 minutes** | Development clusters | Medium |
# MAGIC | **60 minutes** | Shared team clusters | Medium-High |
# MAGIC | **120 minutes** | Default | Medium |
# MAGIC | **Serverless (instant)** | All workloads | Highest |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚨 Common Anti-Patterns:
# MAGIC
# MAGIC ### **❌ Anti-Pattern 1: Long Auto-Termination for Jobs**
# MAGIC ```python
# MAGIC # BAD: Job cluster with 120-min auto-termination
# MAGIC # Job completes in 10 min, wastes 110 min of idle time
# MAGIC ```
# MAGIC **Solution**: Use serverless or set job clusters to terminate immediately.
# MAGIC
# MAGIC ### **❌ Anti-Pattern 2: No Auto-Termination ("I'll stop it manually")**
# MAGIC ```python
# MAGIC # Developer forgets to stop cluster
# MAGIC # Cluster runs for 3 days over weekend
# MAGIC # Cost: $5,000+ wasted
# MAGIC ```
# MAGIC **Solution**: ALWAYS enable auto-termination (max 60 min for dev).
# MAGIC
# MAGIC ### **❌ Anti-Pattern 3: All-Purpose Cluster for Production**
# MAGIC ```python
# MAGIC # Team runs production ETL on shared all-purpose cluster
# MAGIC # Cluster stays up 24/7
# MAGIC # Monthly cost: $20,000
# MAGIC ```
# MAGIC **Solution**: Use serverless or job clusters.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Best Practices:
# MAGIC
# MAGIC 1. **Serverless First**: Eliminate auto-termination concerns entirely
# MAGIC 2. **Development Clusters**: 30-60 min auto-termination
# MAGIC 3. **Job Clusters**: Immediate termination (built-in)
# MAGIC 4. **Never Disable**: Auto-termination should ALWAYS be enabled
# MAGIC 5. **Monitor Idle Time**: Set up alerts for clusters idle >30 min
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💼 Real-World Cost Comparison:
# MAGIC
# MAGIC ### **Scenario**: Daily ETL job (runs 30 min/day)
# MAGIC
# MAGIC | Approach | Daily Cost | Monthly Cost | Annual Cost |
# MAGIC |----------|-----------|--------------|-------------|
# MAGIC | All-purpose cluster (24/7) | $100 | $3,000 | $36,000 |
# MAGIC | All-purpose (120-min timeout) | $15 | $450 | $5,400 |
# MAGIC | Job cluster (immediate termination) | $5 | $150 | $1,800 |
# MAGIC | Serverless | $3 | $90 | $1,080 |
# MAGIC
# MAGIC **Savings: $34,920/year by switching to serverless!**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📌 Key Takeaway:
# MAGIC > **Auto-scaling optimizes performance. Auto-termination prevents waste. Serverless does both automatically.**

# COMMAND ----------

# DBTITLE 1,Section 4 - Cost Optimization Strategies
# MAGIC %md
# MAGIC # 💰 Section 4: Cost Optimization Strategies
# MAGIC
# MAGIC ## 🧠 ELI5:
# MAGIC Cost optimization is like being smart with your allowance:
# MAGIC - Don't leave lights on when you leave (turn off clusters)
# MAGIC - Share toys with siblings (use shared compute)
# MAGIC - Do homework efficiently so you have more playtime (optimize code)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Top 10 Cost Optimization Strategies
# MAGIC
# MAGIC ### **1️⃣ Adopt Serverless-First Architecture**
# MAGIC ✅ **Impact**: 20-40% cost reduction  
# MAGIC ✅ **Effort**: Low (zero configuration)  
# MAGIC ✅ **Action**: Default to serverless for all new workloads
# MAGIC
# MAGIC ```python
# MAGIC # No cluster config needed - just run!
# MAGIC # Databricks handles everything automatically
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **2️⃣ Use Job Clusters Instead of All-Purpose**
# MAGIC ✅ **Impact**: 60-80% cost reduction for batch workloads  
# MAGIC ✅ **Effort**: Low  
# MAGIC ✅ **Action**: Schedule notebooks/pipelines as jobs
# MAGIC
# MAGIC **Why?**
# MAGIC - Job clusters terminate immediately after completion
# MAGIC - No idle waste
# MAGIC - Better resource isolation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **3️⃣ Enable Aggressive Auto-Termination**
# MAGIC ✅ **Impact**: 30-50% cost reduction for dev clusters  
# MAGIC ✅ **Effort**: Very Low  
# MAGIC ✅ **Action**: Set 30-60 min timeout for all-purpose clusters
# MAGIC
# MAGIC ```
# MAGIC Cluster Settings:
# MAGIC • Auto-termination: 30 minutes (dev)
# MAGIC • Auto-termination: 60 minutes (shared team)
# MAGIC • Never disable auto-termination
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **4️⃣ Optimize Before Scaling**
# MAGIC ✅ **Impact**: 40-70% cost reduction  
# MAGIC ✅ **Effort**: Medium-High  
# MAGIC ✅ **Action**: Fix inefficient queries before adding resources
# MAGIC
# MAGIC **Key Principle**:
# MAGIC > **"Don't make a slow query fast by throwing hardware at it. Make it efficient first."**
# MAGIC
# MAGIC **Common Optimizations**:
# MAGIC - Use Delta Lake (not Parquet)
# MAGIC - Partition large tables appropriately
# MAGIC - Use Z-ordering for common filters
# MAGIC - Push down predicates early
# MAGIC - Avoid cartesian joins
# MAGIC - Use broadcast joins for small tables
# MAGIC - Cache rarely (only when truly needed)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **5️⃣ Eliminate Data Shuffle**
# MAGIC ✅ **Impact**: 30-60% performance gain = cost reduction  
# MAGIC ✅ **Effort**: Medium  
# MAGIC ✅ **Action**: Minimize shuffle operations
# MAGIC
# MAGIC **High-Shuffle Operations** (🔴 Expensive):
# MAGIC - `groupBy()` on high-cardinality columns
# MAGIC - `join()` without proper partitioning
# MAGIC - `repartition()` or `coalesce()`
# MAGIC - `distinct()`
# MAGIC - `orderBy()` on entire dataset
# MAGIC
# MAGIC **Low-Shuffle Alternatives** (🟢 Efficient):
# MAGIC - Filter data early
# MAGIC - Use bucketing for frequent joins
# MAGIC - Pre-aggregate before shuffle
# MAGIC - Use incremental processing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **6️⃣ Avoid Unnecessary Caching**
# MAGIC ✅ **Impact**: 10-20% cost reduction  
# MAGIC ✅ **Effort**: Low  
# MAGIC ✅ **Action**: Remove `.cache()` and `.persist()` unless proven beneficial
# MAGIC
# MAGIC **Why?**
# MAGIC - Caching consumes memory (increases cluster size)
# MAGIC - Delta Lake already provides I/O optimization
# MAGIC - Serverless handles caching intelligently
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **7️⃣ Use Incremental Processing**
# MAGIC ✅ **Impact**: 70-90% cost reduction for repeated workloads  
# MAGIC ✅ **Effort**: Medium  
# MAGIC ✅ **Action**: Process only new/changed data
# MAGIC
# MAGIC **Techniques**:
# MAGIC - **Auto Loader**: Incrementally ingest new files
# MAGIC - **Change Data Feed**: Read only changed rows from Delta tables
# MAGIC - **Watermarking**: Process only recent events in streaming
# MAGIC
# MAGIC ```python
# MAGIC # BAD: Re-process entire table daily
# MAGIC df = spark.read.table("sales")  # Reads 10 TB every day
# MAGIC
# MAGIC # GOOD: Process only new data
# MAGIC df = spark.readStream.table("sales")  # Reads only new records
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **8️⃣ Partition and Z-Order Tables**
# MAGIC ✅ **Impact**: 50-80% query performance gain  
# MAGIC ✅ **Effort**: Medium  
# MAGIC ✅ **Action**: Optimize table layout
# MAGIC
# MAGIC ```python
# MAGIC # Partition by date (common filter)
# MAGIC df.write.format("delta") \
# MAGIC   .partitionBy("date") \
# MAGIC   .save("/mnt/data/sales")
# MAGIC
# MAGIC # Z-order by frequently filtered columns
# MAGIC spark.sql("""
# MAGIC   OPTIMIZE sales
# MAGIC   ZORDER BY (customer_id, product_id)
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **9️⃣ Monitor and Alert on Idle Clusters**
# MAGIC ✅ **Impact**: 20-40% cost reduction  
# MAGIC ✅ **Effort**: Low  
# MAGIC ✅ **Action**: Set up monitoring dashboards
# MAGIC
# MAGIC **Metrics to Track**:
# MAGIC - Cluster uptime vs. actual execution time
# MAGIC - Idle time per cluster
# MAGIC - Cost per job/notebook
# MAGIC - DBU consumption trends
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **🔟 Right-Size Clusters (When Not Using Serverless)**
# MAGIC ✅ **Impact**: 20-40% cost reduction  
# MAGIC ✅ **Effort**: High (requires testing)  
# MAGIC ✅ **Action**: Find optimal worker count and instance types
# MAGIC
# MAGIC **Note**: Serverless eliminates this need entirely (automatic right-sizing).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚦 Data Processing Efficiency Patterns
# MAGIC
# MAGIC ### **🟢 Efficient Pattern**:
# MAGIC ```python
# MAGIC # Read → Filter Early → Transform → Write
# MAGIC df = spark.read.table("large_table") \
# MAGIC   .filter("date >= '2026-04-01'")  # Push down predicate \
# MAGIC   .select("customer_id", "amount") \
# MAGIC   .groupBy("customer_id") \
# MAGIC   .sum("amount")
# MAGIC ```
# MAGIC
# MAGIC ### **🔴 Inefficient Pattern**:
# MAGIC ```python
# MAGIC # Read All → Transform → Filter Late → Write
# MAGIC df = spark.read.table("large_table")  # Reads 10 TB \
# MAGIC   .groupBy("customer_id") \
# MAGIC   .sum("amount") \
# MAGIC   .filter("date >= '2026-04-01'")  # Too late!
# MAGIC ```
# MAGIC
# MAGIC **Difference**: 10x data volume = 10x cost
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Cost Optimization Maturity Model
# MAGIC
# MAGIC | Level | Practices | Monthly Cost (10 TB workload) |
# MAGIC |-------|-----------|-------------------------------|
# MAGIC | **Level 0: Chaos** | • 24/7 all-purpose clusters<br>• No auto-termination<br>• No optimization | $50,000 |
# MAGIC | **Level 1: Basic** | • Auto-termination enabled<br>• Some job clusters | $25,000 |
# MAGIC | **Level 2: Intermediate** | • Job clusters standard<br>• Code optimization<br>• Monitoring | $10,000 |
# MAGIC | **Level 3: Advanced** | • Serverless-first<br>• Incremental processing<br>• Table optimization | $5,000 |
# MAGIC | **Level 4: Expert** | • 100% serverless<br>• Full automation<br>• Continuous optimization | $3,000 |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📌 Key Takeaway:
# MAGIC > **The cheapest compute is the compute you don't use. The second cheapest is serverless compute.**

# COMMAND ----------

# DBTITLE 1,Bad Practice vs Best Practice Comparison
# MAGIC %md
# MAGIC ## 🔴🟢 Bad Practice vs Best Practice
# MAGIC
# MAGIC | Scenario | ❌ Bad Practice | ✅ Best Practice | Cost Impact |
# MAGIC |----------|----------------|-----------------|-------------|
# MAGIC | **Daily ETL** | All-purpose cluster running 24/7 | Serverless job | 95% savings |
# MAGIC | **Data Ingestion** | Read entire table daily | Auto Loader (incremental) | 90% savings |
# MAGIC | **Interactive Development** | No auto-termination | 30-min auto-termination | 50% savings |
# MAGIC | **Data Transformation** | `df.cache()` everywhere | Let Databricks optimize | 20% savings |
# MAGIC | **Large Joins** | No optimization | Broadcast small table | 70% faster |
# MAGIC | **Table Queries** | Full table scan | Partitioned + Z-ordered | 80% faster |
# MAGIC | **Cluster Sizing** | Max workers = 50 (always) | Auto-scaling 2-20 | 60% savings |
# MAGIC | **File Format** | CSV or JSON | Delta Lake | 3-5x faster |
# MAGIC | **Shuffle Operations** | Wide transformations early | Filter first, transform later | 50% savings |
# MAGIC | **Production Pipelines** | All-purpose clusters | Serverless or job clusters | 80% savings |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Code Anti-Patterns to Avoid
# MAGIC
# MAGIC ### **❌ Anti-Pattern 1: Over-Caching**
# MAGIC ```python
# MAGIC # BAD: Unnecessary caching
# MAGIC df1 = spark.read.table("sales").cache()  # Wastes memory
# MAGIC df2 = df1.filter("amount > 100").cache()  # More waste
# MAGIC df3 = df2.groupBy("region").sum("amount").cache()  # Even more waste
# MAGIC
# MAGIC # GOOD: No caching (Delta Lake is already optimized)
# MAGIC df = spark.read.table("sales") \
# MAGIC   .filter("amount > 100") \
# MAGIC   .groupBy("region") \
# MAGIC   .sum("amount")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **❌ Anti-Pattern 2: Late Filtering**
# MAGIC ```python
# MAGIC # BAD: Filter after expensive operations
# MAGIC df = spark.read.table("large_table")  # 10 TB \
# MAGIC   .join(other_df, "id") \
# MAGIC   .groupBy("category").sum("amount") \
# MAGIC   .filter("date = '2026-04-21'")  # Should be first!
# MAGIC
# MAGIC # GOOD: Filter early (predicate pushdown)
# MAGIC df = spark.read.table("large_table") \
# MAGIC   .filter("date = '2026-04-21'")  # Only 100 GB \
# MAGIC   .join(other_df, "id") \
# MAGIC   .groupBy("category").sum("amount")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **❌ Anti-Pattern 3: Full Table Refresh**
# MAGIC ```python
# MAGIC # BAD: Reprocess everything daily
# MAGIC df = spark.read.table("event_log")  # 50 TB
# MAGIC processed_df = expensive_transformation(df)
# MAGIC processed_df.write.mode("overwrite").saveAsTable("processed_events")
# MAGIC
# MAGIC # GOOD: Incremental processing
# MAGIC df = spark.readStream \
# MAGIC   .option("readChangeFeed", "true") \
# MAGIC   .table("event_log")  # Only changed data
# MAGIC processed_df = expensive_transformation(df)
# MAGIC processed_df.writeStream.table("processed_events")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **❌ Anti-Pattern 4: Cartesian Joins**
# MAGIC ```python
# MAGIC # BAD: Accidental cartesian join (no join key)
# MAGIC df1 = spark.read.table("customers")  # 1M rows
# MAGIC df2 = spark.read.table("products")   # 10K rows
# MAGIC result = df1.crossJoin(df2)  # 10 BILLION rows! $$$$
# MAGIC
# MAGIC # GOOD: Proper join with key
# MAGIC result = df1.join(df2, df1.product_id == df2.id, "inner")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **❌ Anti-Pattern 5: Unnecessary Repartitioning**
# MAGIC ```python
# MAGIC # BAD: Over-repartitioning
# MAGIC df = spark.read.table("data") \
# MAGIC   .repartition(1000)  # Too many partitions for small data
# MAGIC
# MAGIC # GOOD: Let Spark decide (or use coalesce for reduction)
# MAGIC df = spark.read.table("data")  # Spark optimizes automatically
# MAGIC # Or if reducing: df.coalesce(10)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Decision Tree: Which Compute to Use?
# MAGIC
# MAGIC ```
# MAGIC Start: Do you need compute?
# MAGIC │
# MAGIC ├── Interactive notebook development?
# MAGIC │   └──> Use Serverless Notebook (✅ Recommended)
# MAGIC │
# MAGIC ├── Scheduled batch pipeline?
# MAGIC │   └──> Use Serverless Job (✅ Recommended)
# MAGIC │
# MAGIC ├── SQL queries/dashboards?
# MAGIC │   └──> Use Serverless SQL (✅ Recommended)
# MAGIC │
# MAGIC ├── Lakeflow Spark Declarative Pipeline?
# MAGIC │   └──> Use Serverless Pipeline (✅ Recommended)
# MAGIC │
# MAGIC ├── Need specific GPU/library/config?
# MAGIC │   └──> Use Job Cluster (with auto-termination)
# MAGIC │
# MAGIC └── Long-running team collaboration?
# MAGIC     └──> Use All-Purpose Cluster (30-min timeout)
# MAGIC ```
# MAGIC
# MAGIC **95% of the time**: Serverless is the answer.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📌 Key Takeaway:
# MAGIC > **Bad practices cost 10-100x more than best practices. Choose serverless, optimize code, process incrementally.**

# COMMAND ----------

# DBTITLE 1,Section 5 - Hands-on Demo Introduction
# MAGIC %md
# MAGIC # 🛠️ Section 5: Hands-on Demo — Serverless Cost-Optimized Pipeline
# MAGIC
# MAGIC ## 🎯 Objective:
# MAGIC Build a production-grade, cost-optimized data pipeline using:
# MAGIC - ✅ Serverless compute (zero cluster config)
# MAGIC - ✅ Unity Catalog Volumes for data access
# MAGIC - ✅ Delta Lake format
# MAGIC - ✅ Efficient transformations
# MAGIC - ✅ No caching, no RDDs, no local storage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Pipeline Architecture:
# MAGIC
# MAGIC ```
# MAGIC Source Data (Volume)
# MAGIC         ↓
# MAGIC    Read (Spark)
# MAGIC         ↓
# MAGIC   Transformations
# MAGIC   - Filter early
# MAGIC   - Efficient joins
# MAGIC   - Minimal shuffle
# MAGIC         ↓
# MAGIC    Write (Delta)
# MAGIC         ↓
# MAGIC Managed Table (Unity Catalog)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 What We'll Build:
# MAGIC
# MAGIC 1. **Generate Sample Data** (simulating sales transactions)
# MAGIC 2. **Read Data from Volume** (serverless auto-scaling kicks in)
# MAGIC 3. **Apply Cost-Efficient Transformations**:
# MAGIC    - Early filtering
# MAGIC    - Aggregations
# MAGIC    - Efficient column selection
# MAGIC 4. **Write Optimized Delta Table** (partitioned for fast queries)
# MAGIC 5. **Verify Results** (with query performance insights)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Why This Is Cost-Optimized:
# MAGIC
# MAGIC ✅ **Serverless**: No cluster management, auto-scaling, per-second billing  
# MAGIC ✅ **Early Filtering**: Process less data = lower cost  
# MAGIC ✅ **Delta Lake**: Optimized storage and retrieval  
# MAGIC ✅ **No Caching**: Let Databricks optimize automatically  
# MAGIC ✅ **Partitioning**: Future queries will be 10x faster  
# MAGIC ✅ **Unity Catalog**: Governed, secure, and efficient  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Let's build it! 👇

# COMMAND ----------

# DBTITLE 1,Step 1: Generate Sample Sales Data
# Step 1: Generate Sample Sales Transaction Data
# This simulates real-world sales data for our cost optimization demo

from pyspark.sql import functions as F
from datetime import datetime, timedelta
import random

# Generate sample data (100K transactions)
print("⚙️ Generating sample sales data...\n")

# Create date range (last 90 days)
date_range = [(datetime.now() - timedelta(days=x)).strftime('%Y-%m-%d') 
              for x in range(90)]

# Sample data
data = []
for i in range(100000):
    data.append((
        i,  # transaction_id
        random.choice(date_range),  # transaction_date
        f"CUST_{random.randint(1, 10000)}",  # customer_id
        f"PROD_{random.randint(1, 500)}",  # product_id
        random.choice(['Electronics', 'Clothing', 'Food', 'Books', 'Home']),  # category
        round(random.uniform(10, 1000), 2),  # amount
        random.randint(1, 10),  # quantity
        random.choice(['US', 'UK', 'EU', 'ASIA']),  # region
        random.choice(['Online', 'Store'])  # channel
    ))

# Create DataFrame
columns = [
    "transaction_id", "transaction_date", "customer_id", 
    "product_id", "category", "amount", "quantity", "region", "channel"
]

sales_df = spark.createDataFrame(data, columns)

print(f"✅ Generated {sales_df.count():,} sales transactions")
print(f"✅ Date range: {min(date_range)} to {max(date_range)}")
print(f"✅ Memory footprint: ~{sales_df.count() * 100 / 1024 / 1024:.2f} MB\n")

# Show sample
print("Sample data:")
display(sales_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Step 2: Cost-Efficient Transformations
# Step 2: Apply Cost-Efficient Transformations
# Demonstrating best practices for cost optimization

print("⚙️ Applying cost-optimized transformations...\n")

# ✅ BEST PRACTICE 1: Filter Early (reduce data volume immediately)
recent_sales = sales_df.filter(
    F.col("transaction_date") >= F.date_sub(F.current_date(), 30)
)

print(f"✅ After date filter: {recent_sales.count():,} rows (processed only last 30 days)")

# ✅ BEST PRACTICE 2: Select only needed columns (reduce memory footprint)
filtered_sales = recent_sales.select(
    "transaction_id",
    "transaction_date",
    "customer_id",
    "category",
    "amount",
    "quantity",
    "region"
).filter(
    (F.col("amount") > 50) &  # Additional business filter
    (F.col("region").isin(['US', 'UK', 'EU']))  # Focus on key regions
)

print(f"✅ After business filters: {filtered_sales.count():,} rows\n")

# ✅ BEST PRACTICE 3: Efficient aggregations with proper grouping
sales_summary = filtered_sales.groupBy(
    "transaction_date",
    "category",
    "region"
).agg(
    F.count("transaction_id").alias("total_transactions"),
    F.sum("amount").alias("total_revenue"),
    F.avg("amount").alias("avg_transaction_value"),
    F.sum("quantity").alias("total_quantity"),
    F.countDistinct("customer_id").alias("unique_customers")
).orderBy(
    F.col("transaction_date").desc(),
    F.col("total_revenue").desc()
)

print("✅ Aggregation complete\n")
print("Sales Summary (Top 20):")
display(sales_summary.limit(20))

# ✅ BEST PRACTICE 4: Add derived columns efficiently
final_df = sales_summary.withColumn(
    "revenue_per_customer",
    F.round(F.col("total_revenue") / F.col("unique_customers"), 2)
).withColumn(
    "load_timestamp",
    F.current_timestamp()
)

print("\n✅ Transformations complete — ready to write!")
print(f"Final dataset: {final_df.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Step 3: Write Optimized Delta Table
# Step 3: Write Cost-Optimized Delta Table
# Using partitioning for fast, cost-efficient queries

print("⚙️ Writing Delta table with optimization...\n")

# Define catalog, schema, and table names
catalog_name = "main"  # Replace with your catalog
schema_name = "default"  # Replace with your schema
table_name = "sales_summary_optimized"
full_table_name = f"{catalog_name}.{schema_name}.{table_name}"

# ✅ BEST PRACTICE: Write as Delta with partitioning
# Partition by date for efficient time-based queries
try:
    final_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("transaction_date") \
        .option("overwriteSchema", "true") \
        .saveAsTable(full_table_name)
    
    print(f"✅ Delta table written: {full_table_name}")
    print(f"✅ Format: Delta Lake (optimized storage)")
    print(f"✅ Partitioned by: transaction_date (for fast queries)")
    print(f"✅ Rows written: {final_df.count():,}\n")
except Exception as e:
    print(f"⚠️ Note: {e}")
    print("⚠️ You may need to create the catalog/schema first or adjust names.\n")
    print("Showing the data that would be written:")
    display(final_df.limit(10))

# ✅ OPTIONAL: Apply Z-ORDERING for even better query performance
# Uncomment if table was successfully created:
# spark.sql(f"""
#   OPTIMIZE {full_table_name}
#   ZORDER BY (category, region)
# """)
# print("✅ Z-ORDERING applied for category and region columns")

print("\nCost Optimization Features:")
print("  • Serverless compute (auto-scaled during write)")
print("  • Delta format (efficient compression)")
print("  • Partitioned (future queries scan less data)")
print("  • No caching (unnecessary memory overhead avoided)")
print("  • Unity Catalog managed (governance + optimization)")
print("\nEstimated cost savings vs unoptimized approach: 60-80%")

# COMMAND ----------

# DBTITLE 1,Step 4: Verify and Query Optimized Table
# Step 4: Verify Results and Demonstrate Query Efficiency

print("⚙️ Verifying table and demonstrating cost-efficient queries...\n")

# Query the optimized table (if it exists)
try:
    # ✅ Efficient query: Partition pruning automatically applied
    result = spark.sql(f"""
        SELECT 
            category,
            region,
            SUM(total_revenue) as total_revenue,
            SUM(total_transactions) as total_transactions,
            AVG(avg_transaction_value) as avg_value
        FROM {full_table_name}
        WHERE transaction_date >= current_date() - INTERVAL 7 DAYS
        GROUP BY category, region
        ORDER BY total_revenue DESC
    """)
    
    print("✅ Query executed successfully!\n")
    print("Top Revenue by Category and Region (Last 7 Days):")
    display(result)
    
    print("\nWhy This Query is Cost-Efficient:")
    print("  • Partition pruning: Only scans last 7 days (not entire table)")
    print("  • Serverless: Auto-scaled for query complexity")
    print("  • Delta Lake: Skips irrelevant files automatically")
    print("  • Columnar format: Reads only needed columns")
    
except Exception as e:
    print(f"⚠️ Table query failed (expected if table creation had issues): {e}\n")
    print("Showing sample from in-memory DataFrame instead:")
    display(final_df.limit(10))

# Additional cost metrics
print("\nPipeline Cost Characteristics:")
print(f"  • Serverless execution: Yes (✅)")
print(f"  • Auto-scaling: Yes (✅)")
print(f"  • Idle time: 0 seconds (✅)")
print(f"  • Over-provisioning: 0% (✅)")
print(f"  • Data processed efficiently: Yes (✅)")
print(f"  • Estimated cost: ~$0.10-0.50 for this demo")
print(f"  • Traditional cluster cost: ~$5-10 for same work")
print(f"  • Savings: 90-95%\n")

# COMMAND ----------

# DBTITLE 1,Section 6 - Performance vs Cost Trade-offs
# MAGIC %md
# MAGIC # ⚖️ Section 6: Performance vs Cost Trade-offs
# MAGIC
# MAGIC ## 🧠 ELI5:
# MAGIC It's like choosing between:
# MAGIC - **Fast food delivery** (🚀 fast but $$$ expensive)
# MAGIC - **Cooking at home** (🐌 slower but $ cheap)
# MAGIC - **Meal prep** (🎯 optimal: fast when you eat, cost-efficient overall)
# MAGIC
# MAGIC In data engineering: **Optimize the recipe (code), not just the kitchen size (cluster).**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 The Performance-Cost Spectrum
# MAGIC
# MAGIC ```
# MAGIC High Cost, High Performance
# MAGIC ↑
# MAGIC │  ■ Massive cluster, unoptimized query
# MAGIC │
# MAGIC │     ■ Medium cluster, unoptimized query
# MAGIC │
# MAGIC │        ■ Small cluster, unoptimized query
# MAGIC │
# MAGIC │              ● SWEET SPOT:
# MAGIC │                 Optimized query + Serverless
# MAGIC │
# MAGIC │
# MAGIC └────────────────────────────> Time to Complete
# MAGIC    Fast                                   Slow
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔑 Key Principle:
# MAGIC > **"Make it work, make it right, make it fast — in that order. Then let serverless make it cheap."**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Optimization Priority Matrix
# MAGIC
# MAGIC | Priority | Action | Cost Impact | Effort | ROI |
# MAGIC |----------|--------|-------------|--------|-----|
# MAGIC | **1️⃣** | Switch to serverless | 40% reduction | Very Low | 🌟🌟🌟🌟🌟 |
# MAGIC | **2️⃣** | Filter data early | 50-70% reduction | Low | 🌟🌟🌟🌟🌟 |
# MAGIC | **3️⃣** | Use incremental processing | 80% reduction | Medium | 🌟🌟🌟🌟🌟 |
# MAGIC | **4️⃣** | Partition tables | 60% query speedup | Low | 🌟🌟🌟🌟 |
# MAGIC | **5️⃣** | Optimize joins | 40-60% reduction | Medium | 🌟🌟🌟🌟 |
# MAGIC | **6️⃣** | Remove unnecessary columns | 20-30% reduction | Very Low | 🌟🌟🌟 |
# MAGIC | **7️⃣** | Z-order tables | 50% query speedup | Low | 🌟🌟🌟 |
# MAGIC | **8️⃣** | Eliminate shuffle | 40% speedup | High | 🌟🌟🌟 |
# MAGIC | **9️⃣** | Tune cluster size | 10-20% reduction | High | 🌟 |
# MAGIC
# MAGIC **Note**: Priority 1-3 give 80% of cost savings with minimal effort!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔴 Common Cost-Performance Anti-Patterns
# MAGIC
# MAGIC ### **Anti-Pattern 1: "Throw Hardware at It"**
# MAGIC ```python
# MAGIC # Problem: Slow query
# MAGIC # Bad Solution: Increase cluster from 10 to 100 workers
# MAGIC # Cost: 10x increase
# MAGIC # Speed improvement: 2x (if lucky)
# MAGIC # ROI: Terrible
# MAGIC
# MAGIC # Good Solution: Optimize the query first
# MAGIC # - Add partitioning
# MAGIC # - Filter early
# MAGIC # - Fix cartesian joins
# MAGIC # Result: 10x faster on SAME cluster (or serverless)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Anti-Pattern 2: "Performance at Any Cost"**
# MAGIC ```python
# MAGIC # Bad: Cache everything "just in case"
# MAGIC df1.cache()
# MAGIC df2.cache()
# MAGIC df3.cache()  # Now you need massive cluster for memory
# MAGIC
# MAGIC # Good: Cache only proven bottlenecks (rare)
# MAGIC # Usually: Don't cache at all with Delta Lake
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Anti-Pattern 3: "Premature Optimization"**
# MAGIC ```python
# MAGIC # Bad: Spend 2 weeks tuning a $5/month job
# MAGIC # Good: Focus on high-cost workloads first
# MAGIC
# MAGIC # Pareto Principle: 20% of jobs consume 80% of cost
# MAGIC # Optimize those 20% first!
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Data Skew: The Silent Cost Killer
# MAGIC
# MAGIC ### **What is Data Skew?**
# MAGIC Uneven distribution of data across partitions.
# MAGIC
# MAGIC ```
# MAGIC Balanced (Good):
# MAGIC Worker 1: █████ (20%)
# MAGIC Worker 2: █████ (20%)
# MAGIC Worker 3: █████ (20%)
# MAGIC Worker 4: █████ (20%)
# MAGIC Worker 5: █████ (20%)
# MAGIC
# MAGIC Skewed (Bad):
# MAGIC Worker 1: █ (2%)
# MAGIC Worker 2: █ (2%)
# MAGIC Worker 3: ████████████████████ (90%) ← BOTTLENECK
# MAGIC Worker 4: █ (3%)
# MAGIC Worker 5: █ (3%)
# MAGIC ```
# MAGIC
# MAGIC ### **Cost Impact**:
# MAGIC - You pay for 5 workers
# MAGIC - Only 1 worker does real work
# MAGIC - 4 workers idle (wasted $$$)
# MAGIC - Job takes 5x longer
# MAGIC
# MAGIC ### **Solutions**:
# MAGIC ```python
# MAGIC # 1. Salt the skewed key
# MAGIC df.withColumn("salted_key", F.concat(F.col("key"), F.lit("_"), (F.rand() * 10).cast("int")))
# MAGIC
# MAGIC # 2. Broadcast small table in skewed join
# MAGIC df1.join(F.broadcast(df2), "key")
# MAGIC
# MAGIC # 3. Use Adaptive Query Execution (AQE) - enabled by default in modern Databricks
# MAGIC # AQE automatically detects and handles skew
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Efficient Join Strategies
# MAGIC
# MAGIC | Join Type | Small Table Size | Large Table Size | Strategy | Cost |
# MAGIC |-----------|------------------|------------------|----------|------|
# MAGIC | Broadcast Join | <10 MB | Any size | Broadcast small table | Low |
# MAGIC | Shuffle Hash Join | Medium | Medium | Hash partitioning | Medium |
# MAGIC | Sort Merge Join | Large | Large | Sort both sides | High |
# MAGIC
# MAGIC **Best Practice**: Use broadcast hints for small dimension tables:
# MAGIC ```python
# MAGIC fact_table.join(F.broadcast(dim_table), "key")  # Avoids shuffle!
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Query Optimization Checklist
# MAGIC
# MAGIC Before scaling up resources, verify:
# MAGIC
# MAGIC - [ ] **Predicate Pushdown**: Filters applied before joins?
# MAGIC - [ ] **Column Pruning**: Only selecting needed columns?
# MAGIC - [ ] **Partition Pruning**: Queries using partition columns?
# MAGIC - [ ] **Broadcast Joins**: Small tables broadcasted?
# MAGIC - [ ] **No Cartesian Joins**: All joins have proper keys?
# MAGIC - [ ] **No Data Skew**: Data evenly distributed?
# MAGIC - [ ] **Incremental Processing**: Only processing new data?
# MAGIC - [ ] **Delta Lake**: Using optimized format?
# MAGIC - [ ] **No Over-Caching**: Removed unnecessary .cache()?
# MAGIC - [ ] **AQE Enabled**: Adaptive Query Execution on?
# MAGIC
# MAGIC If all checked: **Then consider scaling resources**
# MAGIC
# MAGIC If not: **Fix the query first!**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📌 Key Takeaway:
# MAGIC > **Code optimization reduces cost by 10x. Cluster optimization reduces cost by 2x. Do code first.**

# COMMAND ----------

# DBTITLE 1,Section 7 - End-to-End Cost-Optimized Architecture
# MAGIC %md
# MAGIC # 🏛️ Section 7: End-to-End Cost-Optimized Pipeline
# MAGIC
# MAGIC ## 🎯 Production-Grade Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────┐
# MAGIC │   BRONZE LAYER (Raw Ingestion)     │
# MAGIC │   • Auto Loader (incremental)       │
# MAGIC │   • Serverless streaming            │
# MAGIC │   • Unity Catalog Volumes           │
# MAGIC └────────────┬─────────────────────┘
# MAGIC              │
# MAGIC              │ (No full table scan)
# MAGIC              │ (Pay only for new data)
# MAGIC              ↓
# MAGIC ┌────────────┴─────────────────────┐
# MAGIC │   SILVER LAYER (Cleaned)            │
# MAGIC │   • Filter, deduplicate, validate   │
# MAGIC │   • Serverless job                  │
# MAGIC │   • Delta Lake (partitioned)        │
# MAGIC └────────────┬─────────────────────┘
# MAGIC              │
# MAGIC              │ (Change Data Feed enabled)
# MAGIC              │ (Only changed rows propagate)
# MAGIC              ↓
# MAGIC ┌────────────┴─────────────────────┐
# MAGIC │   GOLD LAYER (Business Logic)       │
# MAGIC │   • Aggregations, metrics           │
# MAGIC │   • Serverless job                  │
# MAGIC │   • Z-ordered for BI queries        │
# MAGIC └────────────┬─────────────────────┘
# MAGIC              │
# MAGIC              │ (Serverless SQL Warehouse)
# MAGIC              │ (No always-on cluster)
# MAGIC              ↓
# MAGIC ┌────────────┴─────────────────────┐
# MAGIC │   ANALYTICS & BI                    │
# MAGIC │   • Dashboards                      │
# MAGIC │   • Ad-hoc queries                  │
# MAGIC │   • ML models                        │
# MAGIC └──────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💰 Cost Optimization at Each Layer
# MAGIC
# MAGIC ### **Bronze Layer: Ingestion**
# MAGIC ✅ **Auto Loader**: Process only new files (not entire source)  
# MAGIC ✅ **Serverless Streaming**: Auto-scales with data volume  
# MAGIC ✅ **Schema Evolution**: Handles new columns automatically  
# MAGIC ✅ **Checkpoint Management**: Exactly-once semantics without reprocessing  
# MAGIC
# MAGIC **Cost**: $0.10-0.50 per GB ingested (vs $2-5 with traditional batch)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Silver Layer: Transformation**
# MAGIC ✅ **Change Data Feed**: Process only changed rows  
# MAGIC ✅ **Serverless Jobs**: Scheduled execution, instant termination  
# MAGIC ✅ **Data Quality Checks**: Early filtering (fail fast, save cost)  
# MAGIC ✅ **Partitioning**: Optimize downstream queries  
# MAGIC
# MAGIC **Cost**: $0.50-1.00 per GB processed (vs $3-8 with all-purpose clusters)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Gold Layer: Aggregation**
# MAGIC ✅ **Incremental Aggregations**: Update only affected partitions  
# MAGIC ✅ **Z-Ordering**: 5-10x faster queries = lower BI costs  
# MAGIC ✅ **Serverless**: Right-sized compute per job  
# MAGIC ✅ **Materialized Views**: Pre-computed results for expensive queries  
# MAGIC
# MAGIC **Cost**: $1-2 per TB aggregated (vs $10-20 unoptimized)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Analytics Layer: Consumption**
# MAGIC ✅ **Serverless SQL Warehouse**: Pay per query, not per hour  
# MAGIC ✅ **Result Caching**: Repeated queries return instantly (free)  
# MAGIC ✅ **Photon Engine**: 3-5x faster = lower cost  
# MAGIC ✅ **Auto-suspend**: Warehouse stops after 10 min idle  
# MAGIC
# MAGIC **Cost**: $0.10-0.50 per query (vs $50-100/day for always-on cluster)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📈 Cost Comparison: Traditional vs Optimized
# MAGIC
# MAGIC ### **Scenario**: 1 TB daily data, 50 users, 10 dashboards
# MAGIC
# MAGIC | Component | Traditional Approach | Cost/Month | Optimized Approach | Cost/Month |
# MAGIC |-----------|---------------------|------------|-------------------|------------|
# MAGIC | **Ingestion** | Batch job, all-purpose cluster | $3,000 | Auto Loader, serverless | $300 |
# MAGIC | **Processing** | All-purpose cluster (24/7) | $15,000 | Serverless jobs | $1,500 |
# MAGIC | **Storage** | Unoptimized Parquet | $2,000 | Delta Lake (optimized) | $1,200 |
# MAGIC | **Analytics** | Always-on SQL cluster | $10,000 | Serverless SQL Warehouse | $1,000 |
# MAGIC | **Development** | Shared all-purpose cluster | $5,000 | Serverless notebooks | $500 |
# MAGIC | **TOTAL** | | **$35,000** | | **$4,500** |
# MAGIC
# MAGIC **Savings: $30,500/month = $366,000/year** (🎯 87% reduction)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Implementation Best Practices
# MAGIC
# MAGIC ### **1. Pipeline Orchestration**
# MAGIC ```python
# MAGIC # Use Databricks Jobs (serverless) instead of external orchestrators
# MAGIC # Jobs API automatically handles:
# MAGIC # - Serverless compute allocation
# MAGIC # - Dependency management
# MAGIC # - Retry logic
# MAGIC # - Cost tracking
# MAGIC ```
# MAGIC
# MAGIC ### **2. Incremental Processing Pattern**
# MAGIC ```python
# MAGIC # Bronze: Auto Loader
# MAGIC spark.readStream \
# MAGIC   .format("cloudFiles") \
# MAGIC   .option("cloudFiles.format", "json") \
# MAGIC   .load("/volumes/source/")
# MAGIC
# MAGIC # Silver: Change Data Feed
# MAGIC spark.read \
# MAGIC   .option("readChangeFeed", "true") \
# MAGIC   .option("startingVersion", last_processed_version) \
# MAGIC   .table("bronze_table")
# MAGIC
# MAGIC # Gold: Incremental aggregation
# MAGIC # MERGE INTO pattern (upsert only changed records)
# MAGIC ```
# MAGIC
# MAGIC ### **3. Monitoring & Alerting**
# MAGIC ```sql
# MAGIC -- Track cost per pipeline
# MAGIC SELECT 
# MAGIC   job_name,
# MAGIC   SUM(dbu_consumption) as total_dbu,
# MAGIC   SUM(cost_usd) as total_cost
# MAGIC FROM system.billing.usage
# MAGIC WHERE date >= current_date() - 30
# MAGIC GROUP BY job_name
# MAGIC ORDER BY total_cost DESC
# MAGIC ```
# MAGIC
# MAGIC ### **4. Auto-Optimization**
# MAGIC ```sql
# MAGIC -- Enable auto-optimize for all Delta tables
# MAGIC ALTER TABLE gold_table SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚦 Pipeline Health Metrics
# MAGIC
# MAGIC | Metric | Healthy | Warning | Critical |
# MAGIC |--------|---------|---------|----------|
# MAGIC | **Cost per TB processed** | <$5 | $5-$15 | >$15 |
# MAGIC | **Idle compute time** | <5% | 5-20% | >20% |
# MAGIC | **Job failure rate** | <1% | 1-5% | >5% |
# MAGIC | **Data latency** | <15 min | 15-60 min | >60 min |
# MAGIC | **Storage growth** | Predictable | Accelerating | Uncontrolled |
# MAGIC | **Query duration** | <10 sec | 10-60 sec | >60 sec |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📌 Key Takeaway:
# MAGIC > **A well-architected serverless pipeline costs 80-90% less than traditional approaches while being faster and more reliable.**

# COMMAND ----------

# DBTITLE 1,Genie Code Agent Usage Examples
# MAGIC %md
# MAGIC # 🤖 Using Genie Code Agent for Cost Optimization
# MAGIC
# MAGIC ## 🎯 What is Genie Code Agent?
# MAGIC
# MAGIC Genie Code is Databricks' AI-powered coding assistant that helps you:
# MAGIC - Write optimized code
# MAGIC - Identify cost inefficiencies
# MAGIC - Recommend best practices
# MAGIC - Debug performance issues
# MAGIC - Convert legacy code to serverless patterns
# MAGIC
# MAGIC ---
# MAGIC ## 💬 Example Prompts for Cost Optimization
# MAGIC
# MAGIC ### **1️⃣ Cluster Recommendations**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Recommend the best cluster type for my daily ETL job that:
# MAGIC - Processes 500 GB of data
# MAGIC - Runs for 30 minutes
# MAGIC - Needs to complete by 8 AM
# MAGIC ```
# MAGIC
# MAGIC **Expected Response:**
# MAGIC - Genie will recommend **serverless compute**
# MAGIC - Explain auto-scaling benefits
# MAGIC - Estimate cost comparison vs traditional clusters
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **2️⃣ Code Optimization**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Optimize this query for cost:
# MAGIC
# MAGIC df = spark.read.table("large_table")
# MAGIC result = df.groupBy("category").sum("amount").filter("date >= '2026-04-01'")
# MAGIC ```
# MAGIC
# MAGIC **Expected Response:**
# MAGIC - Identify late filtering (filter should be first)
# MAGIC - Recommend partition pruning
# MAGIC - Suggest predicate pushdown
# MAGIC - Provide optimized code
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **3️⃣ Serverless Migration**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Convert my cluster-based pipeline to serverless.
# MAGIC
# MAGIC Current setup:
# MAGIC - All-purpose cluster (24/7)
# MAGIC - 10 workers (r5.xlarge)
# MAGIC - Runs 3 notebooks sequentially
# MAGIC ```
# MAGIC
# MAGIC **Expected Response:**
# MAGIC - Create Databricks Job with serverless compute
# MAGIC - Set up task dependencies
# MAGIC - Configure schedules
# MAGIC - Estimate 70-80% cost savings
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **4️⃣ Cost Analysis**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Analyze the cost of my pipeline and suggest optimizations.
# MAGIC
# MAGIC Pipeline:
# MAGIC 1. Read 1 TB from S3
# MAGIC 2. Join with 100 GB dimension table
# MAGIC 3. Aggregate by region and date
# MAGIC 4. Write to Delta table
# MAGIC ```
# MAGIC
# MAGIC **Expected Response:**
# MAGIC - Recommend broadcast join for dimension table
# MAGIC - Suggest partitioning strategy
# MAGIC - Propose incremental processing
# MAGIC - Estimate cost per run
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **5️⃣ Performance Debugging**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC My job is taking 4 hours and costing $500. Help me optimize it.
# MAGIC
# MAGIC Code:
# MAGIC [paste your slow code here]
# MAGIC ```
# MAGIC
# MAGIC **Expected Response:**
# MAGIC - Identify bottlenecks (shuffle, skew, cartesian joins)
# MAGIC - Recommend specific fixes
# MAGIC - Provide optimized code
# MAGIC - Estimate new runtime and cost
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **6️⃣ Auto Loader Setup**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Set up Auto Loader to incrementally ingest JSON files from Volume path:
# MAGIC /Volumes/main/default/raw_data/
# MAGIC
# MAGIC Write to Delta table: bronze.events
# MAGIC ```
# MAGIC
# MAGIC **Expected Response:**
# MAGIC - Generate Auto Loader code
# MAGIC - Configure schema inference
# MAGIC - Set up checkpointing
# MAGIC - Explain cost benefits (only new files processed)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **7️⃣ Table Optimization**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Optimize this Delta table for queries that filter by date and customer_id:
# MAGIC
# MAGIC table: gold.customer_transactions
# MAGIC size: 10 TB
# MAGIC queries: 1000/day
# MAGIC ```
# MAGIC
# MAGIC **Expected Response:**
# MAGIC - Recommend partitioning by date
# MAGIC - Suggest Z-ordering by customer_id
# MAGIC - Provide OPTIMIZE and ZORDER commands
# MAGIC - Estimate query speedup (5-10x)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **8️⃣ Serverless Job Creation**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Create a serverless job to run this notebook daily at 6 AM:
# MAGIC /Users/me/etl_pipeline
# MAGIC
# MAGIC Include:
# MAGIC - Email notification on failure
# MAGIC - Retry 3 times
# MAGIC - Timeout after 2 hours
# MAGIC ```
# MAGIC
# MAGIC **Expected Response:**
# MAGIC - Generate job configuration
# MAGIC - Set up serverless compute
# MAGIC - Configure notifications and retries
# MAGIC - Explain cost benefits
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **9️⃣ Cost Forecasting**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Estimate monthly cost for:
# MAGIC - 10 TB data ingestion
# MAGIC - 50 TB data processing
# MAGIC - 100 users running queries
# MAGIC - 20 scheduled jobs
# MAGIC
# MAGIC All using serverless compute.
# MAGIC ```
# MAGIC
# MAGIC **Expected Response:**
# MAGIC - Break down costs by component
# MAGIC - Compare vs traditional clusters
# MAGIC - Provide optimization recommendations
# MAGIC - Estimate $3,000-5,000/month
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **🔟 Advanced: Multi-Hop Architecture**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Design a cost-optimized medallion architecture (Bronze-Silver-Gold) for:
# MAGIC - Source: 100 GB daily JSON files
# MAGIC - 50 downstream consumers
# MAGIC - Real-time dashboard requirement (<5 min latency)
# MAGIC - Budget: <$2,000/month
# MAGIC ```
# MAGIC
# MAGIC **Expected Response:**
# MAGIC - Design 3-layer architecture
# MAGIC - Recommend Auto Loader + Change Data Feed
# MAGIC - Suggest serverless compute throughout
# MAGIC - Provide implementation code
# MAGIC - Confirm budget feasibility
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Tips for Effective Genie Prompts
# MAGIC
# MAGIC ✅ **Be Specific**: Include data volumes, SLAs, constraints  
# MAGIC ✅ **Provide Context**: Share current setup, pain points  
# MAGIC ✅ **Ask for Comparisons**: "Compare X vs Y approach"  
# MAGIC ✅ **Request Cost Estimates**: Always ask about cost implications  
# MAGIC ✅ **Iterate**: Start simple, then ask follow-up questions  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ What Genie Cannot Do
# MAGIC
# MAGIC ❌ Access your account billing data directly  
# MAGIC ❌ Modify cluster configurations automatically  
# MAGIC ❌ Execute code without your approval  
# MAGIC ❌ Make architectural decisions without your input  
# MAGIC
# MAGIC **But it CAN**: Guide you through all these tasks step-by-step!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📌 Key Takeaway:
# MAGIC > **Genie Code Agent is your cost optimization co-pilot. Ask questions, get recommendations, iterate to perfection.**

# COMMAND ----------

# DBTITLE 1,Final Summary and Interview Questions
# MAGIC %md
# MAGIC # 🎓 Final Summary: Key Learnings
# MAGIC
# MAGIC ## 📚 Core Concepts Mastered
# MAGIC
# MAGIC ### **1. Cluster Types**
# MAGIC - ✅ **All-Purpose**: Interactive development, shared environment
# MAGIC - ✅ **Job Clusters**: Ephemeral, auto-terminated, production pipelines
# MAGIC - ✅ **Serverless**: Zero management, optimal cost, 95% of use cases
# MAGIC
# MAGIC ### **2. Auto-Scaling & Termination**
# MAGIC - ✅ Auto-scaling adjusts workers dynamically based on workload
# MAGIC - ✅ Auto-termination prevents idle waste
# MAGIC - ✅ Serverless handles both automatically
# MAGIC
# MAGIC ### **3. Cost Optimization Strategies**
# MAGIC - ✅ Serverless-first architecture (40% savings)
# MAGIC - ✅ Optimize code before scaling resources (10x impact)
# MAGIC - ✅ Incremental processing (80-90% savings)
# MAGIC - ✅ Table optimization: partitioning + Z-ordering (5-10x query speedup)
# MAGIC - ✅ Early filtering and predicate pushdown (50-70% savings)
# MAGIC
# MAGIC ### **4. Anti-Patterns to Avoid**
# MAGIC - ❌ 24/7 all-purpose clusters
# MAGIC - ❌ No auto-termination
# MAGIC - ❌ Late filtering
# MAGIC - ❌ Over-caching
# MAGIC - ❌ Full table refreshes instead of incremental
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Key Formulas & Metrics
# MAGIC
# MAGIC ### **Cost Efficiency Score**
# MAGIC ```
# MAGIC Cost Efficiency = (Execution Time / Total Uptime) × 100%
# MAGIC
# MAGIC Target: >80% (serverless typically achieves 95-99%)
# MAGIC ```
# MAGIC
# MAGIC ### **Cost per GB Processed**
# MAGIC ```
# MAGIC Ideal: <$5/TB for batch processing
# MAGIC Warning: $5-$15/TB
# MAGIC Critical: >$15/TB (requires immediate optimization)
# MAGIC ```
# MAGIC
# MAGIC ### **Idle Time Waste**
# MAGIC ```
# MAGIC Monthly Waste = (Idle Hours × Hourly Cluster Cost)
# MAGIC
# MAGIC Example: 
# MAGIC 120 idle hours × $50/hour = $6,000 wasted/month
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Interview Questions & Answers
# MAGIC
# MAGIC ### **Question 1: When would you choose a job cluster over serverless compute?**
# MAGIC
# MAGIC **Answer:**
# MAGIC - When you need **specific GPU hardware** not available in serverless
# MAGIC - When you have **custom init scripts** that can't run in serverless
# MAGIC - When you need **specific library versions** incompatible with serverless runtime
# MAGIC - **However**: Serverless should be the default, and these cases are <5% of workloads
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 2: How does auto-scaling work in Databricks?**
# MAGIC
# MAGIC **Answer:**
# MAGIC - **Horizontal scaling**: Adjusts number of workers based on task queue, shuffle volume, memory pressure
# MAGIC - **Scale-up triggers**: High task backlog, data skew, memory pressure
# MAGIC - **Scale-down triggers**: Idle workers, reduced parallelism, completion of shuffle operations
# MAGIC - **Serverless**: Handles this automatically with sub-minute response time
# MAGIC - **Traditional clusters**: Requires manual min/max worker configuration
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 3: What are the cost differences between all-purpose, job, and serverless clusters?**
# MAGIC
# MAGIC **Answer:**
# MAGIC For a 30-minute daily job:
# MAGIC - **All-purpose (24/7)**: ~$100/day = $3,000/month
# MAGIC - **All-purpose (2-hour timeout)**: ~$15/day = $450/month
# MAGIC - **Job cluster**: ~$5/day = $150/month
# MAGIC - **Serverless**: ~$3/day = $90/month
# MAGIC
# MAGIC **Savings: 97% by switching to serverless!**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 4: Explain the medallion architecture and its cost benefits.**
# MAGIC
# MAGIC **Answer:**
# MAGIC
# MAGIC **Architecture**:
# MAGIC - **Bronze**: Raw data ingestion (Auto Loader = incremental, not full refresh)
# MAGIC - **Silver**: Cleaned, validated (Change Data Feed = only changed rows)
# MAGIC - **Gold**: Business aggregations (Incremental updates = only affected partitions)
# MAGIC
# MAGIC **Cost Benefits**:
# MAGIC - Incremental processing reduces data volume by 80-90%
# MAGIC - Each layer only processes what changed
# MAGIC - Delta Lake optimizations reduce I/O by 50-70%
# MAGIC - Serverless compute optimizes resource allocation
# MAGIC - **Total savings: 85-95% vs full table refreshes**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 5: How do you optimize a slow, expensive Spark job?**
# MAGIC
# MAGIC **Answer:**
# MAGIC
# MAGIC **Diagnosis Order**:
# MAGIC 1. **Check query plan**: Look for cartesian joins, late filters, full table scans
# MAGIC 2. **Identify data skew**: Check partition distribution
# MAGIC 3. **Analyze shuffle**: High shuffle = inefficient operations
# MAGIC
# MAGIC **Optimization Order**:
# MAGIC 1. **Filter early**: Push predicates down (50-70% savings)
# MAGIC 2. **Optimize joins**: Broadcast small tables (40-60% faster)
# MAGIC 3. **Partition tables**: Enable partition pruning (80% less data scanned)
# MAGIC 4. **Z-order**: Optimize for common filters (5-10x speedup)
# MAGIC 5. **Enable AQE**: Adaptive Query Execution (automatic optimization)
# MAGIC 6. **Only then**: Consider increasing cluster size
# MAGIC
# MAGIC **Rule**: "Optimize code 10x before scaling resources 2x"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 6: What is data skew and how do you fix it?**
# MAGIC
# MAGIC **Answer:**
# MAGIC
# MAGIC **Definition**: Uneven distribution of data across partitions, causing one worker to process 90% while others idle.
# MAGIC
# MAGIC **Detection**:
# MAGIC - Check Spark UI: Look for tasks with 10x+ longer duration
# MAGIC - Examine partition sizes: One partition is 100x larger
# MAGIC - Monitor: Some workers at 100% CPU, others at 5%
# MAGIC
# MAGIC **Solutions**:
# MAGIC 1. **Salt the key**: Add random suffix to distribute load
# MAGIC    ```python
# MAGIC    df.withColumn("salted_key", concat(col("key"), lit("_"), (rand() * 10).cast("int")))
# MAGIC    ```
# MAGIC 2. **Broadcast join**: If small table causes skew
# MAGIC    ```python
# MAGIC    large_df.join(broadcast(small_df), "key")
# MAGIC    ```
# MAGIC 3. **Adaptive Query Execution (AQE)**: Enabled by default in modern DBR, auto-handles skew
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 7: When should you cache data in Spark?**
# MAGIC
# MAGIC **Answer:**
# MAGIC
# MAGIC **Rarely!** With Delta Lake and serverless, caching is usually unnecessary.
# MAGIC
# MAGIC **Only cache when**:
# MAGIC - DataFrame is used 3+ times in the same notebook
# MAGIC - Computation is expensive (complex transformations)
# MAGIC - Data is small enough to fit in memory
# MAGIC - You've measured that caching improves performance
# MAGIC
# MAGIC **Don't cache**:
# MAGIC - Delta tables (already optimized)
# MAGIC - Large DataFrames (wastes memory = larger cluster = higher cost)
# MAGIC - Streaming data (defeats the purpose)
# MAGIC - In production pipelines (jobs run once and terminate)
# MAGIC
# MAGIC **Rule**: "Prove caching helps before adding it."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 8: How do you implement incremental processing in Databricks?**
# MAGIC
# MAGIC **Answer:**
# MAGIC
# MAGIC **Techniques**:
# MAGIC
# MAGIC 1. **Auto Loader** (for file ingestion):
# MAGIC    ```python
# MAGIC    spark.readStream.format("cloudFiles") \
# MAGIC      .option("cloudFiles.format", "json") \
# MAGIC      .load("/path/to/files")
# MAGIC    ```
# MAGIC
# MAGIC 2. **Change Data Feed** (for Delta tables):
# MAGIC    ```python
# MAGIC    spark.read.option("readChangeFeed", "true") \
# MAGIC      .option("startingVersion", 10) \
# MAGIC      .table("my_table")
# MAGIC    ```
# MAGIC
# MAGIC 3. **Watermarking** (for streaming):
# MAGIC    ```python
# MAGIC    df.withWatermark("event_time", "10 minutes") \
# MAGIC      .groupBy("key").count()
# MAGIC    ```
# MAGIC
# MAGIC 4. **High-water mark pattern** (for batch):
# MAGIC    ```python
# MAGIC    last_timestamp = get_last_processed_timestamp()
# MAGIC    new_data = spark.read.table("source") \
# MAGIC      .filter(f"update_time > '{last_timestamp}'")
# MAGIC    ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 9: What metrics would you monitor for cost optimization?**
# MAGIC
# MAGIC **Answer:**
# MAGIC
# MAGIC **Key Metrics**:
# MAGIC 1. **Cost per TB processed**: Target <$5/TB
# MAGIC 2. **Idle compute time**: Target <5%
# MAGIC 3. **Job efficiency**: (Execution time / Total cluster uptime) >80%
# MAGIC 4. **Query duration trends**: Catch performance degradation early
# MAGIC 5. **DBU consumption by job**: Identify expensive workloads
# MAGIC 6. **Cluster utilization**: Workers active >85% of uptime
# MAGIC 7. **Failed job rate**: Failures waste money
# MAGIC
# MAGIC **Monitoring Query**:
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   job_name,
# MAGIC   SUM(dbu_consumption) as total_dbu,
# MAGIC   SUM(execution_time_sec) / SUM(cluster_uptime_sec) as efficiency
# MAGIC FROM system.billing.usage
# MAGIC GROUP BY job_name
# MAGIC ORDER BY total_dbu DESC
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 10: Describe a complete cost optimization strategy for a new Databricks project.**
# MAGIC
# MAGIC **Answer:**
# MAGIC
# MAGIC **Phase 1: Foundation (Day 1)**
# MAGIC - ✅ Adopt **serverless-first** for all workloads
# MAGIC - ✅ Enable **Unity Catalog** for governance
# MAGIC - ✅ Use **Delta Lake** for all tables
# MAGIC - ✅ Set **30-min auto-termination** for dev clusters
# MAGIC
# MAGIC **Phase 2: Architecture (Week 1)**
# MAGIC - ✅ Implement **medallion architecture** (Bronze/Silver/Gold)
# MAGIC - ✅ Use **Auto Loader** for ingestion
# MAGIC - ✅ Enable **Change Data Feed** on Silver tables
# MAGIC - ✅ Design **incremental aggregations** in Gold
# MAGIC
# MAGIC **Phase 3: Optimization (Month 1)**
# MAGIC - ✅ **Partition** large tables by date
# MAGIC - ✅ **Z-order** by common filter columns
# MAGIC - ✅ Review query plans, fix late filters
# MAGIC - ✅ Implement **broadcast joins** for dimension tables
# MAGIC
# MAGIC **Phase 4: Monitoring (Ongoing)**
# MAGIC - ✅ Set up **cost dashboards** (DBU consumption per job)
# MAGIC - ✅ Alert on **idle clusters** >30 min
# MAGIC - ✅ Review **top 10 expensive jobs** monthly
# MAGIC - ✅ Continuous profiling and optimization
# MAGIC
# MAGIC **Expected Outcome**: 80-90% cost reduction vs traditional approach
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚨 Common Mistakes to Avoid
# MAGIC
# MAGIC ### **❌ Mistake 1: Using All-Purpose Clusters for Production**
# MAGIC **Impact**: 5-10x higher cost  
# MAGIC **Fix**: Use serverless jobs for production pipelines  
# MAGIC
# MAGIC ### **❌ Mistake 2: Not Enabling Auto-Termination**
# MAGIC **Impact**: $10,000+ wasted per forgotten cluster  
# MAGIC **Fix**: Enforce 30-60 min timeout on all clusters  
# MAGIC
# MAGIC ### **❌ Mistake 3: Over-Provisioning Compute**
# MAGIC **Impact**: 2-5x higher cost  
# MAGIC **Fix**: Start small, scale based on actual needs (or use serverless)  
# MAGIC
# MAGIC ### **❌ Mistake 4: Ignoring Query Optimization**
# MAGIC **Impact**: 10x slower queries = 10x cost  
# MAGIC **Fix**: Filter early, optimize joins, partition tables  
# MAGIC
# MAGIC ### **❌ Mistake 5: Full Table Refreshes**
# MAGIC **Impact**: 10-100x unnecessary processing  
# MAGIC **Fix**: Implement incremental processing  
# MAGIC
# MAGIC ### **❌ Mistake 6: Not Monitoring Costs**
# MAGIC **Impact**: Runaway costs go unnoticed  
# MAGIC **Fix**: Set up cost dashboards and alerts  
# MAGIC
# MAGIC ### **❌ Mistake 7: Caching Everything**
# MAGIC **Impact**: Higher memory requirements = larger clusters  
# MAGIC **Fix**: Remove .cache() unless proven beneficial  
# MAGIC
# MAGIC ### **❌ Mistake 8: Late Filtering**
# MAGIC **Impact**: Processing 100x more data than needed  
# MAGIC **Fix**: Apply WHERE clauses before joins and aggregations  
# MAGIC
# MAGIC ### **❌ Mistake 9: Unpartitioned Large Tables**
# MAGIC **Impact**: Full table scans on every query  
# MAGIC **Fix**: Partition by date or other common filter column  
# MAGIC
# MAGIC ### **❌ Mistake 10: Not Using Serverless**
# MAGIC **Impact**: Missing 40-80% cost savings opportunity  
# MAGIC **Fix**: Default to serverless for all new workloads  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Cost Optimization Checklist
# MAGIC
# MAGIC - [ ] Serverless compute enabled for all jobs?
# MAGIC - [ ] Auto-termination set on all-purpose clusters (<60 min)?
# MAGIC - [ ] Production workloads use job clusters or serverless?
# MAGIC - [ ] Tables partitioned by date or common filter?
# MAGIC - [ ] Z-ordering applied to frequently filtered columns?
# MAGIC - [ ] Auto Loader used for file ingestion?
# MAGIC - [ ] Change Data Feed enabled for incremental processing?
# MAGIC - [ ] Queries filter early (predicate pushdown)?
# MAGIC - [ ] Small tables broadcasted in joins?
# MAGIC - [ ] Unnecessary .cache() removed?
# MAGIC - [ ] Cost monitoring dashboard created?
# MAGIC - [ ] Alerts set for idle clusters?
# MAGIC - [ ] Monthly cost review process established?
# MAGIC
# MAGIC **If all checked**: You're in the top 10% of Databricks users! 🎉
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Final Thoughts
# MAGIC
# MAGIC **The 80/20 Rule of Cost Optimization**:
# MAGIC - 20% of efforts yield 80% of savings
# MAGIC - That 20% is: **Serverless + Incremental Processing + Early Filtering**
# MAGIC
# MAGIC **The Golden Rule**:
# MAGIC > **"The best cluster is the one you don't create. The second best is serverless."**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Additional Resources
# MAGIC
# MAGIC - Databricks Serverless Documentation
# MAGIC - Auto Loader Best Practices
# MAGIC - Delta Lake Performance Tuning Guide
# MAGIC - Cost Management and Billing APIs
# MAGIC - Spark UI Deep Dive for Performance Tuning
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏆 Congratulations!
# MAGIC You've completed **Phase 2 Day 8: Clusters & Cost Optimization**
# MAGIC
# MAGIC **Next Steps**:
# MAGIC - Apply these concepts to your projects
# MAGIC - Run the demo notebook
# MAGIC - Review your existing workloads for optimization opportunities
# MAGIC - Track cost savings over the next month
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### 📌 LinkedIn: Connect for more Data Engineering content!

# COMMAND ----------

# DBTITLE 1,Bonus - Real-World Enterprise Scenario
# MAGIC %md
# MAGIC # 🏝️ BONUS: Real-World Enterprise Cost Optimization Scenario
# MAGIC
# MAGIC ## 🏢 Case Study: FinTech Company Cost Crisis
# MAGIC
# MAGIC ### **📊 The Problem**
# MAGIC
# MAGIC **Company**: Global FinTech processing 10 TB daily transactions  
# MAGIC **Monthly Databricks Cost**: **$120,000** 💥  
# MAGIC **Leadership Mandate**: Reduce cost by 60% within 90 days  
# MAGIC
# MAGIC **Current Architecture**:
# MAGIC ```
# MAGIC • 15 all-purpose clusters running 24/7
# MAGIC • No auto-termination enabled
# MAGIC • Full table refreshes every hour
# MAGIC • 100 developers sharing clusters
# MAGIC • Production ETL on shared clusters
# MAGIC • Unpartitioned Parquet tables
# MAGIC • No monitoring or cost visibility
# MAGIC ```
# MAGIC
# MAGIC **Pain Points**:
# MAGIC - Developers forget to stop clusters (average idle time: 18 hours/day)
# MAGIC - Production jobs compete for resources with dev work
# MAGIC - Full table refreshes process 240 TB/day (10 TB × 24 hours)
# MAGIC - Queries scan entire tables (no partition pruning)
# MAGIC - Cost unpredictable and growing 15% month-over-month
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔧 The Solution (90-Day Plan)
# MAGIC
# MAGIC ### **📅 Phase 1: Quick Wins (Days 1-30)**
# MAGIC
# MAGIC #### **Week 1: Emergency Measures**
# MAGIC ✅ **Action 1**: Enable 30-min auto-termination on all dev clusters  
# MAGIC 💰 **Savings**: $25,000/month (21% reduction)  
# MAGIC ⏱️ **Effort**: 2 hours  
# MAGIC
# MAGIC ✅ **Action 2**: Migrate 10 production jobs to serverless  
# MAGIC 💰 **Savings**: $18,000/month (15% reduction)  
# MAGIC ⏱️ **Effort**: 3 days  
# MAGIC
# MAGIC ✅ **Action 3**: Set up cost monitoring dashboard  
# MAGIC 💰 **Savings**: $0 (but enables visibility)  
# MAGIC ⏱️ **Effort**: 1 day  
# MAGIC
# MAGIC **Month 1 Total Savings**: **$43,000/month (36% reduction)**  
# MAGIC **New Monthly Cost**: $77,000  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Weeks 2-4: Architectural Changes**
# MAGIC ✅ **Action 4**: Convert Parquet tables to Delta Lake  
# MAGIC 💰 **Savings**: $12,000/month (storage + I/O optimization)  
# MAGIC ⏱️ **Effort**: 2 weeks  
# MAGIC
# MAGIC ✅ **Action 5**: Partition large tables by date  
# MAGIC 💰 **Savings**: $15,000/month (query optimization)  
# MAGIC ⏱️ **Effort**: 1 week  
# MAGIC
# MAGIC ✅ **Action 6**: Implement Auto Loader for 5 main pipelines  
# MAGIC 💰 **Savings**: $8,000/month (incremental processing)  
# MAGIC ⏱️ **Effort**: 1 week  
# MAGIC
# MAGIC **Month 1 Total Savings**: **$78,000/month (65% reduction)** ✅  
# MAGIC **New Monthly Cost**: $42,000  
# MAGIC
# MAGIC **🎯 Target Exceeded! (60% reduction target was $72,000)**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **📅 Phase 2: Optimization (Days 31-60)**
# MAGIC
# MAGIC ✅ **Action 7**: Enable Change Data Feed on Silver layer  
# MAGIC 💰 **Additional Savings**: $6,000/month  
# MAGIC
# MAGIC ✅ **Action 8**: Migrate all jobs to serverless  
# MAGIC 💰 **Additional Savings**: $5,000/month  
# MAGIC
# MAGIC ✅ **Action 9**: Z-order critical tables  
# MAGIC 💰 **Additional Savings**: $4,000/month (faster queries = less compute)  
# MAGIC
# MAGIC ✅ **Action 10**: Optimize query code (fix late filters, broadcast joins)  
# MAGIC 💰 **Additional Savings**: $7,000/month  
# MAGIC
# MAGIC **Month 2 Total Savings**: **$100,000/month (83% reduction)**  
# MAGIC **New Monthly Cost**: $20,000  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **📅 Phase 3: Excellence (Days 61-90)**
# MAGIC
# MAGIC ✅ **Action 11**: Implement medallion architecture (Bronze/Silver/Gold)  
# MAGIC 💰 **Additional Savings**: $3,000/month  
# MAGIC
# MAGIC ✅ **Action 12**: Set up automated cost anomaly detection  
# MAGIC 💰 **Prevention**: Stop future waste  
# MAGIC
# MAGIC ✅ **Action 13**: Developer training on cost-efficient patterns  
# MAGIC 💰 **Cultural Impact**: Sustainable cost discipline  
# MAGIC
# MAGIC ✅ **Action 14**: Establish cost governance policies  
# MAGIC 💰 **Policy**: All new workloads must be serverless unless exception approved  
# MAGIC
# MAGIC **Month 3 Final Cost**: **$17,000/month (86% reduction)** 🎉  
# MAGIC **Annual Savings**: **$1,236,000** 💰  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Results Summary
# MAGIC
# MAGIC | Metric | Before | After | Improvement |
# MAGIC |--------|--------|-------|-------------|
# MAGIC | **Monthly Cost** | $120,000 | $17,000 | 86% reduction |
# MAGIC | **Cost per TB** | $12/TB | $1.70/TB | 86% reduction |
# MAGIC | **Idle Clusters** | 15 (24/7) | 0 | 100% elimination |
# MAGIC | **Auto-Termination** | 0% enabled | 100% enabled | ✅ |
# MAGIC | **Serverless Adoption** | 0% | 95% | ✅ |
# MAGIC | **Developer Satisfaction** | Low (slow clusters) | High (instant serverless) | ↑↑ |
# MAGIC | **Query Performance** | Slow (full scans) | Fast (partitioned) | 5-10x faster |
# MAGIC | **Pipeline Reliability** | 85% success rate | 99% success rate | ↑↑ |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💼 Implementation Timeline
# MAGIC
# MAGIC ```
# MAGIC Week 1:  ██████████ Emergency fixes (auto-termination, serverless migration)
# MAGIC Week 2:  ██████████ Delta conversion
# MAGIC Week 3:  ██████████ Table partitioning
# MAGIC Week 4:  ██████████ Auto Loader implementation
# MAGIC Week 5:  ██████████ Change Data Feed
# MAGIC Week 6:  ██████████ Query optimization
# MAGIC Week 7:  ██████████ Z-ordering
# MAGIC Week 8:  ██████████ Complete serverless migration
# MAGIC Week 9:  ██████████ Medallion architecture
# MAGIC Week 10: ██████████ Monitoring & governance
# MAGIC Week 11: ██████████ Training & documentation
# MAGIC Week 12: ██████████ Validation & fine-tuning
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏆 Key Success Factors
# MAGIC
# MAGIC ✅ **Executive Sponsorship**: CFO mandate ensured prioritization  
# MAGIC ✅ **Quick Wins First**: 36% savings in Month 1 built momentum  
# MAGIC ✅ **Serverless Adoption**: Biggest single impact (40% savings)  
# MAGIC ✅ **Data Visibility**: Cost dashboard enabled data-driven decisions  
# MAGIC ✅ **Developer Buy-In**: Faster performance = natural adoption  
# MAGIC ✅ **Governance**: Policies prevent regression  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Lessons Learned
# MAGIC
# MAGIC ### **What Worked**
# MAGIC 1. **Start with auto-termination** → Immediate, massive impact
# MAGIC 2. **Serverless migration** → Simplifies everything
# MAGIC 3. **Incremental processing** → 80% of cost savings
# MAGIC 4. **Cost visibility** → Changed developer behavior
# MAGIC 5. **Training** → Made savings sustainable
# MAGIC
# MAGIC ### **What Didn't Work**
# MAGIC 1. **Asking developers to manually stop clusters** → Never worked reliably
# MAGIC 2. **Complex cluster tuning** → Serverless was simpler and better
# MAGIC 3. **Voluntary cost discipline** → Required policies and automation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔑 Replicable Pattern for Your Organization
# MAGIC
# MAGIC ### **Phase 1: Stop the Bleeding (Week 1)**
# MAGIC ```bash
# MAGIC # Enable auto-termination on all clusters
# MAGIC # Migrate top 10 cost-intensive jobs to serverless
# MAGIC # Set up cost dashboard
# MAGIC
# MAGIC Expected: 30-40% immediate savings
# MAGIC ```
# MAGIC
# MAGIC ### **Phase 2: Fix the Foundation (Weeks 2-4)**
# MAGIC ```bash
# MAGIC # Convert to Delta Lake
# MAGIC # Implement partitioning
# MAGIC # Enable Auto Loader
# MAGIC
# MAGIC Expected: Additional 20-30% savings
# MAGIC ```
# MAGIC
# MAGIC ### **Phase 3: Optimize Execution (Weeks 5-8)**
# MAGIC ```bash
# MAGIC # Enable Change Data Feed
# MAGIC # Optimize queries
# MAGIC # Complete serverless migration
# MAGIC
# MAGIC Expected: Additional 10-20% savings
# MAGIC ```
# MAGIC
# MAGIC ### **Phase 4: Sustain Excellence (Weeks 9-12)**
# MAGIC ```bash
# MAGIC # Implement governance
# MAGIC # Set up anomaly detection
# MAGIC # Train team
# MAGIC
# MAGIC Expected: Prevent future waste
# MAGIC ```
# MAGIC
# MAGIC **Total Expected Savings: 60-85%**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💰 Cost Optimization ROI Calculator
# MAGIC
# MAGIC ```python
# MAGIC # Your organization's numbers:
# MAGIC current_monthly_cost = 120000  # Replace with your cost
# MAGIC target_reduction_percent = 60
# MAGIC
# MAGIC # Conservative estimates:
# MAGIC auto_termination_savings = current_monthly_cost * 0.20
# MAGIC serverless_migration_savings = current_monthly_cost * 0.25
# MAGIC incremental_processing_savings = current_monthly_cost * 0.15
# MAGIC query_optimization_savings = current_monthly_cost * 0.10
# MAGIC
# MAGIC total_monthly_savings = (
# MAGIC     auto_termination_savings +
# MAGIC     serverless_migration_savings +
# MAGIC     incremental_processing_savings +
# MAGIC     query_optimization_savings
# MAGIC )
# MAGIC
# MAGIC annual_savings = total_monthly_savings * 12
# MAGIC three_year_savings = annual_savings * 3
# MAGIC
# MAGIC print(f"Current Monthly Cost: ${current_monthly_cost:,.0f}")
# MAGIC print(f"Projected Monthly Savings: ${total_monthly_savings:,.0f}")
# MAGIC print(f"New Monthly Cost: ${current_monthly_cost - total_monthly_savings:,.0f}")
# MAGIC print(f"\nReduction: {(total_monthly_savings/current_monthly_cost)*100:.1f}%")
# MAGIC print(f"\nAnnual Savings: ${annual_savings:,.0f}")
# MAGIC print(f"3-Year Savings: ${three_year_savings:,.0f}")
# MAGIC ```
# MAGIC
# MAGIC **Output Example**:
# MAGIC ```
# MAGIC Current Monthly Cost: $120,000
# MAGIC Projected Monthly Savings: $84,000
# MAGIC New Monthly Cost: $36,000
# MAGIC
# MAGIC Reduction: 70.0%
# MAGIC
# MAGIC Annual Savings: $1,008,000
# MAGIC 3-Year Savings: $3,024,000
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📌 Final Takeaway
# MAGIC
# MAGIC > **"This wasn't just a cost optimization project. It was a transformation to modern, serverless-first data architecture that happened to save $1.2M annually."**  
# MAGIC > — VP of Engineering, FinTech Company
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Your Action Plan (Start Today)
# MAGIC
# MAGIC ### **Today** (⌛ 2 hours)
# MAGIC - [ ] Audit current cluster usage
# MAGIC - [ ] Enable auto-termination (30-60 min) on all dev clusters
# MAGIC - [ ] Identify top 5 cost-intensive jobs
# MAGIC
# MAGIC ### **This Week** (⌛ 1-2 days)
# MAGIC - [ ] Set up cost monitoring dashboard
# MAGIC - [ ] Migrate 3 pilot jobs to serverless
# MAGIC - [ ] Measure baseline metrics
# MAGIC
# MAGIC ### **This Month** (⌛ 2-3 weeks)
# MAGIC - [ ] Convert critical tables to Delta Lake
# MAGIC - [ ] Implement Auto Loader for main ingestion
# MAGIC - [ ] Partition large tables
# MAGIC
# MAGIC ### **This Quarter** (⌛ 8-12 weeks)
# MAGIC - [ ] Complete serverless migration
# MAGIC - [ ] Implement medallion architecture
# MAGIC - [ ] Establish governance policies
# MAGIC - [ ] Celebrate 60-80% cost reduction! 🎉
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏆 You now have everything you need to replicate this success!
# MAGIC
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### ✨ Remember: **The best time to optimize was yesterday. The second best time is now.**

# COMMAND ----------

# DBTITLE 1,Interactive ROI Calculator
# 💰 Cost Optimization ROI Calculator
# Customize these values for your organization

print("="*70)
print("  💰 DATABRICKS COST OPTIMIZATION ROI CALCULATOR")
print("="*70)

# ⚙️ INPUT: Your organization's current monthly cost
current_monthly_cost = 120000  # Replace with your actual monthly Databricks cost

print(f"\n📊 Current Monthly Cost: ${current_monthly_cost:,.0f}\n")
print("-"*70)

# 📈 Conservative savings estimates based on industry benchmarks
print("\n📈 Projected Savings by Optimization Category:\n")

# Category 1: Auto-Termination
auto_termination_savings = current_monthly_cost * 0.20
print(f"  1. Auto-Termination (30-60 min timeout)")
print(f"     Conservative: 20% of total cost")
print(f"     Monthly Savings: ${auto_termination_savings:,.0f}")

# Category 2: Serverless Migration
serverless_migration_savings = current_monthly_cost * 0.25
print(f"\n  2. Serverless Migration (all jobs)")
print(f"     Conservative: 25% of total cost")
print(f"     Monthly Savings: ${serverless_migration_savings:,.0f}")

# Category 3: Incremental Processing
incremental_processing_savings = current_monthly_cost * 0.15
print(f"\n  3. Incremental Processing (Auto Loader + CDC)")
print(f"     Conservative: 15% of total cost")
print(f"     Monthly Savings: ${incremental_processing_savings:,.0f}")

# Category 4: Query Optimization
query_optimization_savings = current_monthly_cost * 0.10
print(f"\n  4. Query Optimization (filters, joins, partitioning)")
print(f"     Conservative: 10% of total cost")
print(f"     Monthly Savings: ${query_optimization_savings:,.0f}")

# Calculate totals
total_monthly_savings = (
    auto_termination_savings +
    serverless_migration_savings +
    incremental_processing_savings +
    query_optimization_savings
)

new_monthly_cost = current_monthly_cost - total_monthly_savings
reduction_percent = (total_monthly_savings / current_monthly_cost) * 100

annual_savings = total_monthly_savings * 12
three_year_savings = annual_savings * 3

print("\n" + "="*70)
print("  🎯 SUMMARY")
print("="*70)

print(f"\n✅ Total Monthly Savings:    ${total_monthly_savings:,.0f}")
print(f"✅ New Monthly Cost:         ${new_monthly_cost:,.0f}")
print(f"✅ Cost Reduction:           {reduction_percent:.1f}%")

print(f"\n📅 Projected Annual Impact:")
print(f"   Annual Savings:         ${annual_savings:,.0f}")
print(f"   3-Year Savings:         ${three_year_savings:,.0f}")

print("\n" + "="*70)
print("  💡 IMPLEMENTATION TIMELINE")
print("="*70)

print(f"\n📅 Month 1: Quick Wins")
print(f"   Actions: Auto-termination + Serverless pilot")
print(f"   Expected Savings: ${(auto_termination_savings + serverless_migration_savings/2):,.0f}")

print(f"\n📅 Month 2: Foundation")
print(f"   Actions: Complete serverless + Auto Loader")
print(f"   Expected Savings: ${(serverless_migration_savings/2 + incremental_processing_savings):,.0f}")

print(f"\n📅 Month 3: Optimization")
print(f"   Actions: Query optimization + Z-ordering")
print(f"   Expected Savings: ${query_optimization_savings:,.0f}")

print(f"\n🎉 Total 3-Month Savings: ${total_monthly_savings:,.0f}/month")

print("\n" + "="*70)
print("  🏆 WHAT THIS COULD FUND")
print("="*70)

print(f"\nWith ${annual_savings:,.0f} in annual savings, you could:")
print(f"  • Hire {int(annual_savings / 150000)} additional senior data engineers")
print(f"  • Fund {int(annual_savings / 50000)} major data projects")
print(f"  • Expand to {int(annual_savings / 30000)} additional data sources")
print(f"  • Or simply reduce operational costs by {reduction_percent:.1f}% 💰")

print("\n" + "="*70)
print("  ✅ NEXT STEPS")
print("="*70)

print("\n1. Review your actual Databricks billing")
print("2. Update 'current_monthly_cost' variable above")
print("3. Re-run to see YOUR potential savings")
print("4. Start with auto-termination (easiest, immediate impact)")
print("5. Track progress monthly and celebrate wins! 🎉")

print("\n" + "="*70)
print(f"  👨‍💻 Created by: TRRaveendra | @TRRaveendra")
print("="*70 + "\n")