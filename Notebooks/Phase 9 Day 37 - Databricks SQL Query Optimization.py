# Databricks notebook source
# DBTITLE 1,📘 Notebook Header
# MAGIC %md
# MAGIC # 🧮 Data Engineering Training — Phase 9 Day 37  
# MAGIC ## ⚡ Databricks SQL: Serverless Warehouses & Query Optimization  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - Databricks SQL Overview  
# MAGIC - Serverless SQL Warehouses  
# MAGIC - Query Optimization Techniques  
# MAGIC - Performance Tuning in SQL  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### ⚙️ Platform: Databricks (Serverless SQL + Unity Catalog + Delta Lake)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Objective:
# MAGIC Understand how to use Databricks SQL for analytics, leverage serverless SQL warehouses, and optimize queries for performance.

# COMMAND ----------

# DBTITLE 1,📊 Section 1: What is Databricks SQL?
# MAGIC %md
# MAGIC ## 📊 SECTION 1 — What is Databricks SQL?
# MAGIC
# MAGIC ### 🧒 ELI5 Explanation:
# MAGIC Imagine you have a massive library with millions of books. Databricks SQL is like having a super-smart librarian who can instantly find any information you need, no matter how complex your question is. Instead of manually searching through books, you just ask questions in simple language (SQL), and the librarian brings you exactly what you need.
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC Databricks SQL is a **serverless analytics layer** built on top of Delta Lake that provides:
# MAGIC
# MAGIC * **Unified SQL Interface**: Query data lakes using standard ANSI SQL
# MAGIC * **BI & Reporting Integration**: Connect tools like Tableau, Power BI, Looker
# MAGIC * **SQL Warehouses**: Dedicated compute clusters optimized for SQL workloads
# MAGIC * **Query Federation**: Query across multiple data sources (Delta, Parquet, JSON, external DBs)
# MAGIC * **Built-in Optimization**: Automatic query optimization, caching, and cost management
# MAGIC
# MAGIC ### 🎯 Key Use Cases:
# MAGIC 1. **Ad-hoc Analytics**: Exploratory data analysis
# MAGIC 2. **Business Intelligence**: Dashboards and reports
# MAGIC 3. **Data Science Support**: Data preparation for ML
# MAGIC 4. **Operational Reporting**: Real-time metrics
# MAGIC 5. **Data Validation**: Quality checks and audits
# MAGIC
# MAGIC ### 🔗 Architecture:
# MAGIC ```
# MAGIC Data Sources (Delta Tables) 
# MAGIC     ↓
# MAGIC Databricks SQL Warehouse
# MAGIC     ↓
# MAGIC SQL Editor / BI Tools / APIs
# MAGIC     ↓
# MAGIC End Users & Dashboards
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,⚡ Section 2: Serverless SQL Warehouses
# MAGIC %md
# MAGIC ## ⚡ SECTION 2 — Serverless SQL Warehouses
# MAGIC
# MAGIC ### 🧒 ELI5 Explanation:
# MAGIC Think of a traditional warehouse where you need to hire workers, manage shifts, and pay them even when they're idle. A **serverless SQL warehouse** is like having workers who magically appear exactly when you need them, work super fast, and disappear when done. You only pay for the actual work they do!
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC Serverless SQL Warehouses provide **fully managed, auto-scaling compute** for SQL workloads:
# MAGIC
# MAGIC #### **🆚 Serverless vs Classic:**
# MAGIC
# MAGIC | Feature | Serverless | Classic |
# MAGIC |---------|-----------|----------|
# MAGIC | **Infrastructure** | Fully managed by Databricks | User-managed clusters |
# MAGIC | **Startup Time** | ∼5 seconds | 2-5 minutes |
# MAGIC | **Scaling** | Instant auto-scaling | Manual or slower auto-scaling |
# MAGIC | **Idle Cost** | Zero (auto-suspend) | Charged during idle time |
# MAGIC | **Management** | Zero ops overhead | Requires configuration |
# MAGIC | **Performance** | Optimized by default | Requires tuning |
# MAGIC
# MAGIC ### ✅ Benefits:
# MAGIC
# MAGIC 1. **🚀 Instant Startup**: No waiting for cluster provisioning
# MAGIC 2. **💰 Cost Efficiency**: Pay only for query execution time
# MAGIC 3. **🔄 Auto-Scaling**: Handles variable workloads automatically
# MAGIC 4. **🔒 Built-in Security**: Unity Catalog integration
# MAGIC 5. **🧠 Intelligent Caching**: Result caching across queries
# MAGIC 6. **⚙️ Zero Configuration**: No cluster sizing decisions
# MAGIC
# MAGIC ### 📝 Configuration Best Practices:
# MAGIC
# MAGIC ```
# MAGIC Warehouse Size Selection:
# MAGIC - X-Small: Dev/testing, <10 concurrent users
# MAGIC - Small: Small teams, <20 concurrent users
# MAGIC - Medium: Standard analytics, <50 users
# MAGIC - Large: Heavy workloads, >50 users
# MAGIC - X-Large/2X-Large: Enterprise-scale analytics
# MAGIC ```
# MAGIC
# MAGIC ### 🔑 Key Settings:
# MAGIC * **Auto Stop**: 10-15 minutes for interactive work, 5 minutes for scheduled jobs
# MAGIC * **Scaling**: Min clusters = 1, Max = based on concurrency needs
# MAGIC * **Spot Instances**: Enable for cost savings (serverless uses them automatically)

# COMMAND ----------

# DBTITLE 1,🔄 Section 3: Query Execution Basics
# MAGIC %md
# MAGIC ## 🔄 SECTION 3 — Query Execution Basics
# MAGIC
# MAGIC ### 🧒 ELI5 Explanation:
# MAGIC When you ask a question (SQL query), Databricks acts like a smart detective:
# MAGIC 1. It reads your question
# MAGIC 2. Figures out the fastest way to get the answer
# MAGIC 3. Only looks at the data it absolutely needs
# MAGIC 4. Brings you the result super fast!
# MAGIC
# MAGIC ### 🏛️ Query Execution Flow:
# MAGIC
# MAGIC ```
# MAGIC 1. SQL Query Submitted
# MAGIC    ↓
# MAGIC 2. Query Parser (validate syntax)
# MAGIC    ↓
# MAGIC 3. Query Optimizer (Catalyst)
# MAGIC    - Predicate Pushdown
# MAGIC    - Column Pruning
# MAGIC    - Join Reordering
# MAGIC    ↓
# MAGIC 4. Physical Plan Generation
# MAGIC    ↓
# MAGIC 5. Delta Lake Metadata Check
# MAGIC    - Read transaction log
# MAGIC    - Identify data files
# MAGIC    - Apply data skipping (statistics)
# MAGIC    ↓
# MAGIC 6. Read Only Required Files
# MAGIC    ↓
# MAGIC 7. Execute Transformations
# MAGIC    ↓
# MAGIC 8. Return Results (with caching)
# MAGIC ```
# MAGIC
# MAGIC ### ⚡ How Delta Tables Optimize Queries:
# MAGIC
# MAGIC 1. **Transaction Log**: Stores metadata about data files
# MAGIC 2. **File-level Statistics**: Min/max/count for each column per file
# MAGIC 3. **Data Skipping**: Skip files that don't contain relevant data
# MAGIC 4. **Z-Ordering**: Co-locate related data for faster reads
# MAGIC 5. **Partition Pruning**: Skip entire partitions based on filters
# MAGIC 6. **Columnar Format**: Read only needed columns (Parquet)
# MAGIC
# MAGIC ### 📊 Performance Factors:
# MAGIC * Query complexity
# MAGIC * Data volume
# MAGIC * Partition strategy
# MAGIC * File sizes (optimal: 100MB - 1GB)
# MAGIC * Predicate selectivity
# MAGIC * Join types and size

# COMMAND ----------

# DBTITLE 1,🎯 Section 4: Query Optimization Techniques
# MAGIC %md
# MAGIC ## 🎯 SECTION 4 — Query Optimization Techniques
# MAGIC
# MAGIC ### 📌 Golden Rules of SQL Optimization:
# MAGIC
# MAGIC #### 1️⃣ **Select Only Required Columns**
# MAGIC ❌ **Bad**: `SELECT * FROM large_table`
# MAGIC ✅ **Good**: `SELECT col1, col2, col3 FROM large_table`
# MAGIC
# MAGIC **Why?** Parquet is columnar - reading fewer columns = less I/O = faster queries
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2️⃣ **Filter Early (Predicate Pushdown)**
# MAGIC ❌ **Bad**: 
# MAGIC ```sql
# MAGIC SELECT * FROM sales
# MAGIC ORDER BY date
# MAGIC LIMIT 100
# MAGIC ```
# MAGIC ✅ **Good**: 
# MAGIC ```sql
# MAGIC SELECT product_id, revenue 
# MAGIC FROM sales
# MAGIC WHERE date >= '2026-04-01'
# MAGIC   AND region = 'US'
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 100
# MAGIC ```
# MAGIC
# MAGIC **Why?** Filtering reduces data processed in subsequent operations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 3️⃣ **Use Partition Columns in WHERE Clause**
# MAGIC ✅ **Good**: 
# MAGIC ```sql
# MAGIC SELECT * FROM events
# MAGIC WHERE event_date = '2026-04-21'  -- partition column
# MAGIC   AND user_id = 'user_123'
# MAGIC ```
# MAGIC
# MAGIC **Why?** Entire partitions are skipped, massive I/O reduction
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 4️⃣ **Avoid Functions on Indexed/Partition Columns**
# MAGIC ❌ **Bad**: 
# MAGIC ```sql
# MAGIC WHERE YEAR(event_date) = 2026
# MAGIC ```
# MAGIC ✅ **Good**: 
# MAGIC ```sql
# MAGIC WHERE event_date >= '2026-01-01' 
# MAGIC   AND event_date < '2027-01-01'
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 5️⃣ **Leverage LIMIT for Exploration**
# MAGIC ```sql
# MAGIC SELECT * FROM massive_table LIMIT 1000
# MAGIC ```
# MAGIC
# MAGIC **Why?** Quick data preview without full table scan
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛠️ Optimization Checklist:
# MAGIC - [ ] Avoid SELECT *
# MAGIC - [ ] Add WHERE clauses with partition columns
# MAGIC - [ ] Select specific columns only
# MAGIC - [ ] Use appropriate JOINs
# MAGIC - [ ] Filter before aggregating
# MAGIC - [ ] Use LIMIT during development
# MAGIC - [ ] Check query execution plans

# COMMAND ----------

# DBTITLE 1,💻 Column Selection Demo
# MAGIC %md
# MAGIC ### 💻 Demonstration: Column Selection Optimization
# MAGIC
# MAGIC Let's demonstrate the impact of column selection:

# COMMAND ----------

# DBTITLE 1,SQL: Optimized Column Selection
# MAGIC %sql
# MAGIC -- Example: Inefficient Query (reads all columns)
# MAGIC -- SELECT * FROM catalog.schema.large_table
# MAGIC -- WHERE date = '2026-04-21'
# MAGIC
# MAGIC -- Example: Optimized Query (reads only needed columns)
# MAGIC -- SELECT customer_id, order_id, total_amount 
# MAGIC -- FROM catalog.schema.orders
# MAGIC -- WHERE order_date = '2026-04-21'
# MAGIC --   AND status = 'completed'
# MAGIC
# MAGIC -- For demonstration purposes:
# MAGIC SELECT 
# MAGIC   'Optimized queries read only required columns' as best_practice,
# MAGIC   'Reduces I/O by 70-90% compared to SELECT *' as impact,
# MAGIC   'Always specify exact columns needed' as recommendation

# COMMAND ----------

# DBTITLE 1,🎯 Section 5: Partition Pruning & Data Skipping
# MAGIC %md
# MAGIC ## 🎯 SECTION 5 — Partition Pruning & Data Skipping
# MAGIC
# MAGIC ### 🧒 ELI5 Explanation:
# MAGIC Imagine you have a filing cabinet with 1000 drawers organized by year. If you need documents from 2026, you only open the "2026" drawer instead of checking all 1000 drawers. That's **partition pruning**!
# MAGIC
# MAGIC Now imagine each drawer has a label saying "contains invoices #1000-2000". If you need invoice #5000, you can skip that drawer entirely. That's **data skipping**!
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC #### **Partition Pruning:**
# MAGIC Delta Lake organizes data into **partitions** based on column values (typically date/region). When you filter on a partition column, only relevant partitions are read.
# MAGIC
# MAGIC ```
# MAGIC Table Structure:
# MAGIC /table/
# MAGIC   event_date=2026-04-19/
# MAGIC   event_date=2026-04-20/
# MAGIC   event_date=2026-04-21/  ← Only this is read!
# MAGIC   event_date=2026-04-22/
# MAGIC ```
# MAGIC
# MAGIC #### **Data Skipping (via Statistics):**
# MAGIC Delta Lake stores **min/max statistics** for each data file. When you filter on a non-partition column, files are skipped if statistics prove they can't contain matching data.
# MAGIC
# MAGIC ```
# MAGIC File Statistics:
# MAGIC - file_001.parquet: user_id [1000 - 5000]
# MAGIC - file_002.parquet: user_id [5001 - 10000]  ← Read this!
# MAGIC - file_003.parquet: user_id [10001 - 15000]
# MAGIC
# MAGIC Query: WHERE user_id = 7500
# MAGIC Result: Only file_002 is read
# MAGIC ```
# MAGIC
# MAGIC ### ⚡ Performance Impact:
# MAGIC
# MAGIC | Technique | I/O Reduction | Typical Speedup |
# MAGIC |-----------|---------------|------------------|
# MAGIC | Partition Pruning | 90-99% | 10x - 100x |
# MAGIC | Data Skipping | 50-80% | 2x - 5x |
# MAGIC | Combined | 95-99% | 20x - 200x |
# MAGIC
# MAGIC ### 🛠️ Best Practices:
# MAGIC
# MAGIC 1. **Partition on columns used frequently in WHERE clauses**
# MAGIC    - Date columns (most common)
# MAGIC    - Region/country
# MAGIC    - Department/category
# MAGIC    
# MAGIC 2. **Don't over-partition**
# MAGIC    - Target: 100MB - 1GB per file
# MAGIC    - Too many small files = slow queries
# MAGIC    
# MAGIC 3. **Use Z-ORDERING for multi-column filters**
# MAGIC    ```sql
# MAGIC    OPTIMIZE table_name
# MAGIC    ZORDER BY (col1, col2)
# MAGIC    ```
# MAGIC
# MAGIC 4. **Always filter on partition columns first**

# COMMAND ----------

# DBTITLE 1,💻 Partition Pruning Demo
# MAGIC %md
# MAGIC ### 💻 Demonstration: Partition Pruning in Action

# COMMAND ----------

# DBTITLE 1,SQL: Partition Pruning Example
# MAGIC %sql
# MAGIC -- Example: Query with Partition Pruning
# MAGIC -- Assumes table is partitioned by event_date
# MAGIC
# MAGIC -- Inefficient: Full table scan
# MAGIC -- SELECT * FROM catalog.schema.events
# MAGIC
# MAGIC -- Efficient: Partition pruning applied
# MAGIC -- SELECT event_id, user_id, event_type
# MAGIC -- FROM catalog.schema.events
# MAGIC -- WHERE event_date = '2026-04-21'  -- Partition column filter!
# MAGIC --   AND event_type = 'purchase'
# MAGIC
# MAGIC -- Performance Comparison:
# MAGIC SELECT 
# MAGIC   'Without partition filter' as scenario,
# MAGIC   'Scans ALL data files' as behavior,
# MAGIC   'Slow, expensive' as result
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'With partition filter',
# MAGIC   'Scans ONLY files in event_date=2026-04-21 partition',
# MAGIC   'Fast, 10-100x speedup'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Best Practice',
# MAGIC   'Always include partition column in WHERE clause',
# MAGIC   'Maximum performance'

# COMMAND ----------

# DBTITLE 1,🔥 Z-Ordering
# MAGIC %md
# MAGIC ### 🔥 Advanced: Z-Ordering for Multi-Column Filters
# MAGIC
# MAGIC When you frequently filter on **non-partition columns**, use Z-ORDERING:
# MAGIC
# MAGIC ```sql
# MAGIC -- Optimize table with Z-ORDERING
# MAGIC OPTIMIZE catalog.schema.events
# MAGIC ZORDER BY (user_id, product_id)
# MAGIC ```
# MAGIC
# MAGIC **Benefits:**
# MAGIC - Co-locates related data within files
# MAGIC - Improves data skipping effectiveness
# MAGIC - Reduces I/O for multi-column filters
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - High-cardinality columns (user_id, product_id)
# MAGIC - Frequently used together in queries
# MAGIC - Complement to partitioning

# COMMAND ----------

# DBTITLE 1,🔗 Section 6: Join Optimization
# MAGIC %md
# MAGIC ## 🔗 SECTION 6 — Join Optimization
# MAGIC
# MAGIC ### 🧒 ELI5 Explanation:
# MAGIC Imagine you need to match students with their test scores. If you have 10 students and 1 million test records, it's faster to:
# MAGIC 1. Filter the 1 million records to only the 10 students FIRST
# MAGIC 2. Then match them
# MAGIC
# MAGIC Rather than trying to match all 1 million records!
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC #### **Join Types & Performance:**
# MAGIC
# MAGIC | Join Type | When Spark Uses It | Performance |
# MAGIC |-----------|-------------------|-------------|
# MAGIC | **Broadcast Join** | Small table (<10MB default) | ⭐⭐⭐⭐⭐ Fastest |
# MAGIC | **Sort-Merge Join** | Large tables, sorted keys | ⭐⭐⭐ Good |
# MAGIC | **Shuffle Hash Join** | Medium tables | ⭐⭐ Slower |
# MAGIC | **Cartesian Join** | No join condition | ❌ Avoid! |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚡ Join Optimization Strategies:
# MAGIC
# MAGIC #### 1️⃣ **Filter Before Joining**
# MAGIC
# MAGIC ❌ **Inefficient:**
# MAGIC ```sql
# MAGIC SELECT *
# MAGIC FROM large_orders o
# MAGIC JOIN large_customers c ON o.customer_id = c.customer_id
# MAGIC WHERE o.order_date = '2026-04-21'
# MAGIC ```
# MAGIC
# MAGIC ✅ **Optimized:**
# MAGIC ```sql
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC   SELECT * FROM large_orders 
# MAGIC   WHERE order_date = '2026-04-21'  -- Filter first!
# MAGIC ) o
# MAGIC JOIN large_customers c ON o.customer_id = c.customer_id
# MAGIC ```
# MAGIC
# MAGIC **Impact**: Reduces data shuffled across network by 90%+
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2️⃣ **Broadcast Small Tables**
# MAGIC
# MAGIC ```sql
# MAGIC -- Databricks automatically broadcasts tables <10MB
# MAGIC -- Manual hint (if needed):
# MAGIC SELECT /*+ BROADCAST(dim_products) */
# MAGIC   s.*, p.product_name
# MAGIC FROM large_sales s
# MAGIC JOIN dim_products p ON s.product_id = p.product_id
# MAGIC ```
# MAGIC
# MAGIC **Why?** Small table is copied to all nodes, avoiding shuffle
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 3️⃣ **Join on Partition Keys**
# MAGIC
# MAGIC ✅ **Fast:**
# MAGIC ```sql
# MAGIC -- Both tables partitioned by date
# MAGIC SELECT *
# MAGIC FROM orders_partitioned_by_date o
# MAGIC JOIN shipments_partitioned_by_date s 
# MAGIC   ON o.order_date = s.ship_date  -- Partition key join
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 4️⃣ **Avoid Multiple Large Table Joins**
# MAGIC
# MAGIC ❌ **Bad:**
# MAGIC ```sql
# MAGIC SELECT *
# MAGIC FROM huge_table1 t1
# MAGIC JOIN huge_table2 t2 ON t1.id = t2.id
# MAGIC JOIN huge_table3 t3 ON t2.id = t3.id
# MAGIC JOIN huge_table4 t4 ON t3.id = t4.id
# MAGIC ```
# MAGIC
# MAGIC ✅ **Better:** Pre-aggregate or filter each table first
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛠️ Join Performance Checklist:
# MAGIC - [ ] Filter tables before joining
# MAGIC - [ ] Join on indexed/partition columns
# MAGIC - [ ] Use broadcast for small dimension tables
# MAGIC - [ ] Avoid joining on non-equality conditions
# MAGIC - [ ] Check for data skew in join keys
# MAGIC - [ ] Consider denormalization for repeated joins

# COMMAND ----------

# DBTITLE 1,💻 Join Demo
# MAGIC %md
# MAGIC ### 💻 Demonstration: Efficient Join Pattern

# COMMAND ----------

# DBTITLE 1,SQL: Optimized Join Pattern
# MAGIC %sql
# MAGIC -- Example: Optimized Join Strategy
# MAGIC
# MAGIC -- Step 1: Filter fact table first
# MAGIC -- WITH filtered_orders AS (
# MAGIC --   SELECT order_id, customer_id, total_amount
# MAGIC --   FROM catalog.schema.orders
# MAGIC --   WHERE order_date >= '2026-04-01'
# MAGIC --     AND status = 'completed'
# MAGIC -- )
# MAGIC
# MAGIC -- Step 2: Join with dimension table (auto-broadcast if small)
# MAGIC -- SELECT 
# MAGIC --   o.order_id,
# MAGIC --   c.customer_name,
# MAGIC --   o.total_amount
# MAGIC -- FROM filtered_orders o
# MAGIC -- JOIN catalog.schema.customers c 
# MAGIC --   ON o.customer_id = c.customer_id
# MAGIC
# MAGIC -- Demonstration output:
# MAGIC SELECT
# MAGIC   'Filter First' as step,
# MAGIC   'Reduces data volume before shuffle' as benefit,
# MAGIC   '90%+ reduction in network I/O' as impact
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Broadcast Join',
# MAGIC   'Small dimension table copied to all nodes',
# MAGIC   'No shuffle required'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Result',
# MAGIC   'Combined optimization',
# MAGIC   '10-50x faster join performance'

# COMMAND ----------

# DBTITLE 1,📊 Section 7: Aggregation Optimization
# MAGIC %md
# MAGIC ## 📊 SECTION 7 — Aggregation Optimization
# MAGIC
# MAGIC ### 🧒 ELI5 Explanation:
# MAGIC If you need to count red candies in a jar, you could:
# MAGIC 1. ❌ Count ALL candies, then filter for red ones
# MAGIC 2. ✅ Only look at red candies and count them
# MAGIC
# MAGIC Option 2 is way faster!
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC Aggregations (COUNT, SUM, AVG, etc.) can be expensive on large datasets. Optimization is crucial.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚡ Aggregation Best Practices:
# MAGIC
# MAGIC #### 1️⃣ **Filter Before Aggregating**
# MAGIC
# MAGIC ❌ **Inefficient:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   category,
# MAGIC   COUNT(*) as total_count
# MAGIC FROM large_table
# MAGIC GROUP BY category
# MAGIC HAVING COUNT(*) > 1000  -- Aggregates ALL data first!
# MAGIC ```
# MAGIC
# MAGIC ✅ **Optimized:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   category,
# MAGIC   COUNT(*) as total_count
# MAGIC FROM large_table
# MAGIC WHERE date >= '2026-04-01'  -- Filter first!
# MAGIC GROUP BY category
# MAGIC HAVING COUNT(*) > 1000
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2️⃣ **Use Approximate Aggregations (When Appropriate)**
# MAGIC
# MAGIC ```sql
# MAGIC -- Exact COUNT DISTINCT (slow for high cardinality)
# MAGIC SELECT COUNT(DISTINCT user_id) FROM huge_table
# MAGIC
# MAGIC -- Approximate COUNT DISTINCT (much faster, ~2% error)
# MAGIC SELECT APPROX_COUNT_DISTINCT(user_id) FROM huge_table
# MAGIC ```
# MAGIC
# MAGIC **Use Cases:** Dashboards, exploratory analysis, large-scale metrics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 3️⃣ **Partition-Aware Aggregations**
# MAGIC
# MAGIC ✅ **Efficient:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   event_date,  -- Partition column in GROUP BY
# MAGIC   event_type,
# MAGIC   COUNT(*) as event_count
# MAGIC FROM events
# MAGIC WHERE event_date >= '2026-04-01'
# MAGIC GROUP BY event_date, event_type
# MAGIC ```
# MAGIC
# MAGIC **Why?** Each partition is aggregated independently, then combined
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 4️⃣ **Avoid Repeated Aggregations**
# MAGIC
# MAGIC ❌ **Inefficient:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   (SELECT AVG(amount) FROM sales) as avg_amount,
# MAGIC   (SELECT MAX(amount) FROM sales) as max_amount,
# MAGIC   (SELECT MIN(amount) FROM sales) as min_amount
# MAGIC ```
# MAGIC
# MAGIC ✅ **Optimized:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   AVG(amount) as avg_amount,
# MAGIC   MAX(amount) as max_amount,
# MAGIC   MIN(amount) as min_amount
# MAGIC FROM sales
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Performance Comparison:
# MAGIC
# MAGIC | Aggregation Type | Small Data | Large Data | Optimization Needed |
# MAGIC |------------------|-----------|------------|---------------------|
# MAGIC | COUNT(*) | Fast | Medium | Filter first |
# MAGIC | COUNT(DISTINCT) | Medium | Very Slow | Use APPROX |
# MAGIC | SUM/AVG | Fast | Fast | Filter first |
# MAGIC | PERCENTILE | Slow | Very Slow | Sample if possible |
# MAGIC | Multiple aggregates | Fast | Medium | Combine in one pass |

# COMMAND ----------

# DBTITLE 1,💻 Aggregation Demo
# MAGIC %md
# MAGIC ### 💻 Demonstration: Aggregation Patterns

# COMMAND ----------

# DBTITLE 1,SQL: Aggregation Best Practices
# MAGIC %sql
# MAGIC -- Example: Efficient Aggregation Query
# MAGIC
# MAGIC -- Optimized aggregation pattern:
# MAGIC -- SELECT 
# MAGIC --   category,
# MAGIC --   region,
# MAGIC --   COUNT(*) as order_count,
# MAGIC --   SUM(total_amount) as revenue,
# MAGIC --   AVG(total_amount) as avg_order_value,
# MAGIC --   APPROX_COUNT_DISTINCT(customer_id) as unique_customers
# MAGIC -- FROM catalog.schema.orders
# MAGIC -- WHERE order_date >= '2026-04-01'  -- Filter first!
# MAGIC --   AND status = 'completed'
# MAGIC -- GROUP BY category, region
# MAGIC -- HAVING COUNT(*) >= 100  -- Post-aggregation filter
# MAGIC -- ORDER BY revenue DESC
# MAGIC
# MAGIC -- Best Practices Demonstrated:
# MAGIC SELECT
# MAGIC   'Filter Early' as technique,
# MAGIC   'WHERE clause before GROUP BY' as implementation,
# MAGIC   'Reduces data to aggregate' as benefit
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Combine Aggregates',
# MAGIC   'Multiple aggregations in one query',
# MAGIC   'Single pass over data'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Approximate Functions',
# MAGIC   'APPROX_COUNT_DISTINCT for high cardinality',
# MAGIC   '10-100x faster than exact COUNT DISTINCT'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'HAVING Clause',
# MAGIC   'Filter after aggregation',
# MAGIC   'Reduces result set size'

# COMMAND ----------

# DBTITLE 1,🛠️ Section 8: Hands-on Optimization Exercise
# MAGIC %md
# MAGIC ## 🛠️ SECTION 8 — Hands-on Query Optimization Exercise
# MAGIC
# MAGIC ### 🎯 Scenario:
# MAGIC You need to analyze sales data to find:
# MAGIC - Top 10 products by revenue
# MAGIC - Only for Q1 2026
# MAGIC - Only for 'Electronics' category
# MAGIC - Only for 'US' region
# MAGIC
# MAGIC Let's compare **inefficient** vs **optimized** approaches!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Inefficient Query (Anti-Pattern):

# COMMAND ----------

# DBTITLE 1,SQL: Inefficient Query Pattern
# MAGIC %sql
# MAGIC -- INEFFICIENT VERSION - What NOT to do!
# MAGIC
# MAGIC -- SELECT *  -- Reading ALL columns (wasteful)
# MAGIC -- FROM catalog.schema.sales s
# MAGIC -- JOIN catalog.schema.products p ON s.product_id = p.product_id  -- Join without filtering
# MAGIC -- JOIN catalog.schema.regions r ON s.region_id = r.region_id
# MAGIC -- WHERE p.category = 'Electronics'  -- Filtering AFTER join
# MAGIC --   AND r.region_name = 'US'
# MAGIC --   AND s.sale_date >= '2026-01-01' 
# MAGIC --   AND s.sale_date < '2026-04-01'
# MAGIC -- ORDER BY s.revenue DESC
# MAGIC -- LIMIT 10
# MAGIC
# MAGIC -- Problems:
# MAGIC -- 1. SELECT * reads unnecessary columns
# MAGIC -- 2. Joins ALL data before filtering
# MAGIC -- 3. Multiple large table joins
# MAGIC -- 4. No partition pruning optimization
# MAGIC
# MAGIC SELECT 
# MAGIC   'Inefficient Query' as query_type,
# MAGIC   'Reads all columns, joins before filtering' as issues,
# MAGIC   'Slow, expensive, 100% of data processed' as performance

# COMMAND ----------

# DBTITLE 1,✅ Optimized Version
# MAGIC %md
# MAGIC ### ✅ Optimized Query (Best Practice):

# COMMAND ----------

# DBTITLE 1,SQL: Optimized Query Pattern
# MAGIC %sql
# MAGIC -- OPTIMIZED VERSION - Best Practices Applied!
# MAGIC
# MAGIC -- WITH filtered_sales AS (
# MAGIC --   SELECT 
# MAGIC --     product_id,
# MAGIC --     SUM(revenue) as total_revenue  -- Aggregate early
# MAGIC --   FROM catalog.schema.sales
# MAGIC --   WHERE sale_date >= '2026-01-01'  -- Filter on partition column FIRST
# MAGIC --     AND sale_date < '2026-04-01'
# MAGIC --     AND region_id = (SELECT region_id FROM catalog.schema.regions WHERE region_name = 'US')
# MAGIC --   GROUP BY product_id
# MAGIC -- ),
# MAGIC -- top_products AS (
# MAGIC --   SELECT 
# MAGIC --     fs.product_id,
# MAGIC --     p.product_name,  -- Only needed columns
# MAGIC --     fs.total_revenue
# MAGIC --   FROM filtered_sales fs
# MAGIC --   JOIN catalog.schema.products p  -- Small dimension table (broadcast join)
# MAGIC --     ON fs.product_id = p.product_id
# MAGIC --   WHERE p.category = 'Electronics'  -- Filter dimension table
# MAGIC -- )
# MAGIC -- SELECT *
# MAGIC -- FROM top_products
# MAGIC -- ORDER BY total_revenue DESC
# MAGIC -- LIMIT 10
# MAGIC
# MAGIC -- Optimizations Applied:
# MAGIC SELECT 
# MAGIC   'Optimization' as technique,
# MAGIC   'Impact' as result
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   '1. Partition filter (sale_date)',
# MAGIC   '90% less data scanned'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   '2. Filter before join',
# MAGIC   '80% reduction in shuffle data'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   '3. Select only needed columns',
# MAGIC   '70% less I/O'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   '4. Aggregate early',
# MAGIC   'Smaller dataset for join'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   '5. Broadcast small tables',
# MAGIC   'No network shuffle'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Combined Result',
# MAGIC   '10-100x faster performance!'

# COMMAND ----------

# DBTITLE 1,📊 Performance Impact
# MAGIC %md
# MAGIC ### 📊 Performance Comparison:
# MAGIC
# MAGIC | Metric | Inefficient Query | Optimized Query | Improvement |
# MAGIC |--------|------------------|-----------------|-------------|
# MAGIC | **Data Scanned** | 1 TB | 10 GB | 100x less |
# MAGIC | **Execution Time** | 5 minutes | 5 seconds | 60x faster |
# MAGIC | **Compute Cost** | $5.00 | $0.05 | 100x cheaper |
# MAGIC | **Network Shuffle** | 500 GB | 5 GB | 100x less |
# MAGIC
# MAGIC ### 🎯 Key Takeaways:
# MAGIC 1. **Filter Early**: Apply WHERE clauses before JOINs
# MAGIC 2. **Select Specific Columns**: Avoid SELECT *
# MAGIC 3. **Use Partition Columns**: Massive data skipping
# MAGIC 4. **Aggregate Early**: Reduce data volume for downstream operations
# MAGIC 5. **Broadcast Small Tables**: Eliminate shuffle for dimension tables
# MAGIC
# MAGIC ### ⚡ Real-World Impact:
# MAGIC - **Development**: Faster iteration cycles
# MAGIC - **Production**: Lower costs, better user experience
# MAGIC - **Scale**: Queries that work on TBs of data

# COMMAND ----------

# DBTITLE 1,🔗 Section 9: End-to-End Analytics Flow
# MAGIC %md
# MAGIC ## 🔗 SECTION 9 — End-to-End SQL Analytics Flow
# MAGIC
# MAGIC ### 🏛️ Complete Architecture:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────┐
# MAGIC │  Data Sources        │
# MAGIC │  - Kafka             │
# MAGIC │  - S3                │
# MAGIC │  - Databases         │
# MAGIC └──────────┬──────────┘
# MAGIC            │
# MAGIC            ↓ (Ingestion)
# MAGIC ┌──────────┴──────────┐
# MAGIC │   Delta Lake         │
# MAGIC │   (Unity Catalog)    │
# MAGIC │                      │
# MAGIC │  Bronze → Silver    │
# MAGIC │  Silver → Gold      │
# MAGIC └──────────┬──────────┘
# MAGIC            │
# MAGIC            ↓ (SQL Queries)
# MAGIC ┌──────────┴──────────┐
# MAGIC │ Databricks SQL      │
# MAGIC │ Serverless Warehouse│
# MAGIC │                      │
# MAGIC │ - Query Editor      │
# MAGIC │ - SQL Queries       │
# MAGIC │ - Saved Queries     │
# MAGIC └──────────┬──────────┘
# MAGIC            │
# MAGIC            ↓ (Visualization)
# MAGIC ┌──────────┴──────────┐
# MAGIC │  Analytics Layer    │
# MAGIC │                      │
# MAGIC │ - Dashboards        │
# MAGIC │ - Alerts            │
# MAGIC │ - BI Tools          │
# MAGIC │   (Tableau, PBI)    │
# MAGIC └──────────┬──────────┘
# MAGIC            │
# MAGIC            ↓
# MAGIC ┌──────────┴──────────┐
# MAGIC │   End Users          │
# MAGIC │   - Analysts         │
# MAGIC │   - Business Teams   │
# MAGIC │   - Executives       │
# MAGIC └─────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔄 Workflow Steps:
# MAGIC
# MAGIC #### **1. Data Ingestion**
# MAGIC ```sql
# MAGIC -- Data lands in Bronze layer
# MAGIC CREATE TABLE catalog.bronze.raw_events
# MAGIC USING DELTA
# MAGIC LOCATION 's3://bucket/bronze/events'
# MAGIC ```
# MAGIC
# MAGIC #### **2. Data Transformation**
# MAGIC ```sql
# MAGIC -- Clean and enrich to Silver
# MAGIC CREATE TABLE catalog.silver.cleaned_events AS
# MAGIC SELECT 
# MAGIC   event_id,
# MAGIC   user_id,
# MAGIC   CAST(event_timestamp AS TIMESTAMP) as event_time,
# MAGIC   event_type
# MAGIC FROM catalog.bronze.raw_events
# MAGIC WHERE event_id IS NOT NULL
# MAGIC ```
# MAGIC
# MAGIC #### **3. Business Aggregations**
# MAGIC ```sql
# MAGIC -- Create Gold layer aggregates
# MAGIC CREATE TABLE catalog.gold.daily_metrics AS
# MAGIC SELECT 
# MAGIC   DATE(event_time) as date,
# MAGIC   event_type,
# MAGIC   COUNT(*) as event_count,
# MAGIC   COUNT(DISTINCT user_id) as unique_users
# MAGIC FROM catalog.silver.cleaned_events
# MAGIC GROUP BY DATE(event_time), event_type
# MAGIC ```
# MAGIC
# MAGIC #### **4. SQL Analytics**
# MAGIC ```sql
# MAGIC -- Analysts query Gold tables
# MAGIC SELECT 
# MAGIC   date,
# MAGIC   SUM(event_count) as total_events,
# MAGIC   SUM(unique_users) as total_users
# MAGIC FROM catalog.gold.daily_metrics
# MAGIC WHERE date >= CURRENT_DATE - INTERVAL 30 DAYS
# MAGIC GROUP BY date
# MAGIC ORDER BY date DESC
# MAGIC ```
# MAGIC
# MAGIC #### **5. Dashboard Creation**
# MAGIC - Create visualizations in Databricks SQL
# MAGIC - Schedule queries for automatic refresh
# MAGIC - Set up alerts for anomalies
# MAGIC - Share with stakeholders
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Best Practices:
# MAGIC
# MAGIC 1. **Medallion Architecture**: Bronze → Silver → Gold
# MAGIC 2. **Optimize Gold Layer**: Pre-aggregate for dashboards
# MAGIC 3. **Use Serverless Warehouses**: Auto-scaling for variable loads
# MAGIC 4. **Query Performance**: Apply all optimization techniques
# MAGIC 5. **Data Governance**: Unity Catalog for access control
# MAGIC 6. **Cost Management**: Auto-suspend warehouses, monitor usage

# COMMAND ----------

# DBTITLE 1,🤖 Genie Code Agent Examples
# MAGIC %md
# MAGIC ## 🤖 Databricks Genie Code Agent - SQL Optimization Examples
# MAGIC
# MAGIC ### 👥 What is Genie Code Agent?
# MAGIC
# MAGIC Genie Code is your **AI-powered SQL assistant** built into Databricks that helps you:
# MAGIC - Write optimized SQL queries
# MAGIC - Debug performance issues
# MAGIC - Convert code between languages
# MAGIC - Explain complex queries
# MAGIC - Suggest best practices
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💬 Example Prompts for SQL Optimization:
# MAGIC
# MAGIC #### 1️⃣ **Query Optimization**
# MAGIC ```
# MAGIC Prompt: "Optimize this SQL query for better performance"
# MAGIC
# MAGIC Your Query:
# MAGIC SELECT * FROM large_table WHERE date > '2026-01-01'
# MAGIC
# MAGIC Genie Response:
# MAGIC SELECT col1, col2, col3 
# MAGIC FROM large_table 
# MAGIC WHERE date >= '2026-01-01' 
# MAGIC   AND date < '2026-05-01'
# MAGIC LIMIT 10000
# MAGIC
# MAGIC -- Optimizations applied:
# MAGIC -- 1. Selected specific columns instead of *
# MAGIC -- 2. Added upper bound for better partition pruning
# MAGIC -- 3. Added LIMIT for development testing
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2️⃣ **Convert PySpark to SQL**
# MAGIC ```
# MAGIC Prompt: "Convert this PySpark code to optimized SQL"
# MAGIC
# MAGIC PySpark Code:
# MAGIC df = spark.table("sales") \
# MAGIC   .filter(col("date") == "2026-04-21") \
# MAGIC   .groupBy("region") \
# MAGIC   .agg(sum("revenue").alias("total_revenue"))
# MAGIC
# MAGIC Genie Response:
# MAGIC SELECT 
# MAGIC   region,
# MAGIC   SUM(revenue) as total_revenue
# MAGIC FROM catalog.schema.sales
# MAGIC WHERE date = '2026-04-21'
# MAGIC GROUP BY region
# MAGIC ORDER BY total_revenue DESC
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 3️⃣ **Design SQL Warehouse**
# MAGIC ```
# MAGIC Prompt: "What SQL Warehouse size do I need for 50 concurrent analysts 
# MAGIC querying 10TB of data?"
# MAGIC
# MAGIC Genie Response:
# MAGIC - Warehouse Size: Large or X-Large
# MAGIC - Enable Auto-scaling: Min 1, Max 4 clusters
# MAGIC - Enable Serverless: Yes (recommended)
# MAGIC - Auto-stop: 15 minutes
# MAGIC - Spot instances: Enabled
# MAGIC
# MAGIC Reasoning:
# MAGIC - 50 users = high concurrency need
# MAGIC - 10TB data = need for parallel processing
# MAGIC - Auto-scaling handles peak loads
# MAGIC - Serverless reduces operational overhead
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 4️⃣ **Improve Query Performance**
# MAGIC ```
# MAGIC Prompt: "Why is this query slow and how can I fix it?"
# MAGIC
# MAGIC Slow Query:
# MAGIC SELECT * 
# MAGIC FROM orders o
# MAGIC JOIN customers c ON o.customer_id = c.customer_id
# MAGIC WHERE o.total > 1000
# MAGIC
# MAGIC Genie Analysis:
# MAGIC 🔴 Issues Found:
# MAGIC 1. SELECT * reading unnecessary columns
# MAGIC 2. No date filter (scanning entire table)
# MAGIC 3. Join before filtering
# MAGIC 4. No partition pruning
# MAGIC
# MAGIC ✅ Optimized Version:
# MAGIC SELECT 
# MAGIC   o.order_id,
# MAGIC   o.total,
# MAGIC   c.customer_name
# MAGIC FROM (
# MAGIC   SELECT order_id, customer_id, total
# MAGIC   FROM orders
# MAGIC   WHERE order_date >= '2026-04-01'
# MAGIC     AND total > 1000
# MAGIC ) o
# MAGIC JOIN customers c ON o.customer_id = c.customer_id
# MAGIC
# MAGIC 📊 Expected Improvement: 10-50x faster
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 5️⃣ **Explain Query Execution**
# MAGIC ```
# MAGIC Prompt: "Explain what this query does and how it executes"
# MAGIC
# MAGIC Query:
# MAGIC SELECT region, COUNT(DISTINCT user_id)
# MAGIC FROM events
# MAGIC WHERE event_date = '2026-04-21'
# MAGIC GROUP BY region
# MAGIC
# MAGIC Genie Explanation:
# MAGIC 1. Partition Pruning: Only reads event_date='2026-04-21' partition
# MAGIC 2. Column Pruning: Reads only 'region' and 'user_id' columns
# MAGIC 3. Hash Aggregate: Groups data by region
# MAGIC 4. COUNT DISTINCT: Counts unique users per region
# MAGIC 5. Result: Small result set returned
# MAGIC
# MAGIC Performance: Fast (partition + column pruning)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Pro Tips for Using Genie Code:
# MAGIC
# MAGIC 1. **Be Specific**: Include table names, schemas, and requirements
# MAGIC 2. **Share Context**: Mention data size, partition columns, frequency
# MAGIC 3. **Ask Follow-ups**: Genie can iterate and refine solutions
# MAGIC 4. **Request Explanations**: Ask "why" to learn optimization principles
# MAGIC 5. **Test Suggestions**: Always validate Genie's recommendations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Common Genie Prompts:
# MAGIC
# MAGIC | Task | Example Prompt |
# MAGIC |------|----------------|
# MAGIC | **Optimization** | "Optimize this query for a 5TB table" |
# MAGIC | **Debugging** | "Why is my query taking 10 minutes?" |
# MAGIC | **Conversion** | "Convert this DataFrame code to SQL" |
# MAGIC | **Design** | "Design a star schema for sales analytics" |
# MAGIC | **Best Practice** | "What's the best way to handle SCD Type 2?" |
# MAGIC | **Performance** | "How do I reduce shuffle in this join?" |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚡ Integration with Databricks SQL:
# MAGIC
# MAGIC * **SQL Editor**: Genie available in query editor
# MAGIC * **Notebooks**: %sql cells supported
# MAGIC * **Dashboards**: Query optimization suggestions
# MAGIC * **AI/BI**: Natural language to SQL generation

# COMMAND ----------

# DBTITLE 1,🎯 Final Summary
# MAGIC %md
# MAGIC ## 🎯 FINAL SUMMARY
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📚 Key Learnings:
# MAGIC
# MAGIC #### **1. Databricks SQL Fundamentals**
# MAGIC * Unified SQL interface for data lakes
# MAGIC * Serverless architecture for zero-ops analytics
# MAGIC * Built on Delta Lake for ACID guarantees
# MAGIC * Unity Catalog for governance and security
# MAGIC
# MAGIC #### **2. Serverless SQL Warehouses**
# MAGIC * Instant startup (∼5 seconds)
# MAGIC * Auto-scaling based on demand
# MAGIC * Pay only for query execution time
# MAGIC * No infrastructure management required
# MAGIC * Optimal for variable workloads
# MAGIC
# MAGIC #### **3. Query Optimization Techniques**
# MAGIC * **Column Selection**: Select only required columns, avoid SELECT *
# MAGIC * **Early Filtering**: Apply WHERE clauses before JOINs and aggregations
# MAGIC * **Partition Pruning**: Use partition columns in filters (90-99% I/O reduction)
# MAGIC * **Data Skipping**: Leverage Delta statistics for file-level skipping
# MAGIC * **Efficient Joins**: Filter first, broadcast small tables, avoid cartesian joins
# MAGIC * **Smart Aggregations**: Use approximate functions, combine multiple aggregates
# MAGIC
# MAGIC #### **4. Performance Tuning**
# MAGIC * Z-ORDERING for multi-column filters
# MAGIC * Proper file sizing (100MB - 1GB)
# MAGIC * Table maintenance (OPTIMIZE, VACUUM)
# MAGIC * Query result caching
# MAGIC * Predicate pushdown and column pruning
# MAGIC
# MAGIC #### **5. End-to-End Analytics**
# MAGIC * Medallion architecture (Bronze → Silver → Gold)
# MAGIC * Gold layer optimized for BI workloads
# MAGIC * Integration with Tableau, Power BI, Looker
# MAGIC * Automated refresh and alerting
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Optimization Checklist:
# MAGIC
# MAGIC - [ ] Use Serverless SQL Warehouse
# MAGIC - [ ] Select specific columns (no SELECT *)
# MAGIC - [ ] Filter on partition columns
# MAGIC - [ ] Apply WHERE before JOIN
# MAGIC - [ ] Broadcast small dimension tables
# MAGIC - [ ] Use approximate aggregations when possible
# MAGIC - [ ] Add LIMIT during development
# MAGIC - [ ] OPTIMIZE tables regularly
# MAGIC - [ ] Monitor query performance
# MAGIC - [ ] Use Genie Code for optimization suggestions

# COMMAND ----------

# DBTITLE 1,📝 Interview Questions
# MAGIC %md
# MAGIC ## 🎯 INTERVIEW QUESTIONS
# MAGIC
# MAGIC ### 📝 Top 10 Databricks SQL Interview Questions:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **1. What is the difference between Serverless and Classic SQL Warehouses?**
# MAGIC
# MAGIC **Answer:**
# MAGIC * **Serverless**: Fully managed, instant startup (∼5s), auto-scaling, pay-per-query, zero ops
# MAGIC * **Classic**: User-managed clusters, 2-5 min startup, manual scaling, charged during idle time
# MAGIC * **Use Serverless for**: Variable workloads, dev/test, ad-hoc analytics
# MAGIC * **Use Classic for**: Predictable 24/7 workloads (rare)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **2. How does Delta Lake improve query performance?**
# MAGIC
# MAGIC **Answer:**
# MAGIC * **Transaction Log**: Fast metadata lookup without scanning files
# MAGIC * **File Statistics**: Min/max/count per column per file (data skipping)
# MAGIC * **Partition Pruning**: Skip entire partitions based on filters
# MAGIC * **Z-Ordering**: Co-locate related data for better locality
# MAGIC * **Time Travel**: Query historical versions without data duplication
# MAGIC * **ACID Transactions**: Safe concurrent reads/writes
# MAGIC
# MAGIC **Impact**: 10-100x query speedup on large tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **3. Explain partition pruning with an example.**
# MAGIC
# MAGIC **Answer:**
# MAGIC ```sql
# MAGIC -- Table partitioned by date
# MAGIC CREATE TABLE events (id INT, date DATE, data STRING)
# MAGIC PARTITIONED BY (date)
# MAGIC
# MAGIC -- Query with partition filter
# MAGIC SELECT * FROM events
# MAGIC WHERE date = '2026-04-21'  -- Only reads 1 partition!
# MAGIC
# MAGIC -- Without filter
# MAGIC SELECT * FROM events  -- Reads ALL partitions (slow)
# MAGIC ```
# MAGIC
# MAGIC **Result**: 99% less data scanned when filtering on partition column
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **4. What is data skipping and how does it work?**
# MAGIC
# MAGIC **Answer:**
# MAGIC Delta Lake stores **min/max statistics** for each data file. When you query:
# MAGIC ```sql
# MAGIC SELECT * FROM users WHERE user_id = 12345
# MAGIC ```
# MAGIC
# MAGIC Delta checks statistics:
# MAGIC * File 1: user_id [1 - 10000] → Skip
# MAGIC * File 2: user_id [10001 - 20000] → **Read this!**
# MAGIC * File 3: user_id [20001 - 30000] → Skip
# MAGIC
# MAGIC **Benefit**: Reads only 1 file instead of all 3 (66% reduction)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **5. How do you optimize JOIN queries?**
# MAGIC
# MAGIC **Answer:**
# MAGIC 1. **Filter Before Joining**: Reduce data volume first
# MAGIC    ```sql
# MAGIC    SELECT * FROM (
# MAGIC      SELECT * FROM large_table WHERE date = '2026-04-21'
# MAGIC    ) t1 JOIN t2 ON t1.id = t2.id
# MAGIC    ```
# MAGIC
# MAGIC 2. **Broadcast Small Tables**: Avoid shuffle
# MAGIC    ```sql
# MAGIC    SELECT /*+ BROADCAST(small_dim) */ *
# MAGIC    FROM large_fact f
# MAGIC    JOIN small_dim d ON f.id = d.id
# MAGIC    ```
# MAGIC
# MAGIC 3. **Join on Partition Keys**: Parallel processing
# MAGIC 4. **Avoid Cartesian Joins**: Always include join condition
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **6. What is APPROX_COUNT_DISTINCT and when should you use it?**
# MAGIC
# MAGIC **Answer:**
# MAGIC Approximate version of COUNT DISTINCT using HyperLogLog algorithm.
# MAGIC
# MAGIC ```sql
# MAGIC -- Exact (slow for high cardinality)
# MAGIC SELECT COUNT(DISTINCT user_id) FROM events  -- 10 minutes
# MAGIC
# MAGIC -- Approximate (fast, ~2% error)
# MAGIC SELECT APPROX_COUNT_DISTINCT(user_id) FROM events  -- 10 seconds
# MAGIC ```
# MAGIC
# MAGIC **Use Cases:**
# MAGIC * Dashboards and reports
# MAGIC * High-cardinality columns (user_id, session_id)
# MAGIC * Exploratory analysis
# MAGIC * Real-time metrics
# MAGIC
# MAGIC **Avoid for:** Financial calculations, exact compliance reports
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **7. Explain the Medallion Architecture.**
# MAGIC
# MAGIC **Answer:**
# MAGIC ```
# MAGIC Bronze Layer (Raw):
# MAGIC - Ingested as-is from sources
# MAGIC - Minimal transformation
# MAGIC - Full history retained
# MAGIC
# MAGIC Silver Layer (Cleaned):
# MAGIC - Data quality applied
# MAGIC - Schema enforcement
# MAGIC - Deduplicated
# MAGIC - Enriched with business logic
# MAGIC
# MAGIC Gold Layer (Business):
# MAGIC - Aggregated for specific use cases
# MAGIC - Optimized for BI tools
# MAGIC - Pre-joined dimension tables
# MAGIC - Dashboard-ready
# MAGIC ```
# MAGIC
# MAGIC **Benefits**: Separation of concerns, reusability, performance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **8. What are the best practices for SQL query optimization?**
# MAGIC
# MAGIC **Answer:**
# MAGIC 1. **Select specific columns**: Avoid SELECT *
# MAGIC 2. **Filter early**: WHERE before JOIN/GROUP BY
# MAGIC 3. **Use partition columns**: Massive I/O reduction
# MAGIC 4. **Broadcast small tables**: Eliminate shuffle
# MAGIC 5. **Aggregate early**: Reduce data volume
# MAGIC 6. **Add LIMIT during dev**: Fast iteration
# MAGIC 7. **Use approximate functions**: When accuracy is flexible
# MAGIC 8. **OPTIMIZE tables**: Regular maintenance
# MAGIC 9. **Monitor query plans**: Identify bottlenecks
# MAGIC 10. **Leverage caching**: Result reuse
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **9. How do you troubleshoot a slow query?**
# MAGIC
# MAGIC **Answer:**
# MAGIC
# MAGIC **Step 1**: Check Query Profile
# MAGIC * Identify longest-running stage
# MAGIC * Look for data skew
# MAGIC * Check shuffle size
# MAGIC
# MAGIC **Step 2**: Apply Optimizations
# MAGIC * Add partition filters
# MAGIC * Select specific columns
# MAGIC * Reorder joins (small tables first)
# MAGIC * Check for cartesian joins
# MAGIC
# MAGIC **Step 3**: Table Maintenance
# MAGIC * Run OPTIMIZE to compact files
# MAGIC * Check partition count (not too many)
# MAGIC * Verify statistics are current
# MAGIC
# MAGIC **Step 4**: Warehouse Sizing
# MAGIC * Scale up if consistently slow
# MAGIC * Enable auto-scaling for spikes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **10. What is Z-ORDERING and when should you use it?**
# MAGIC
# MAGIC **Answer:**
# MAGIC Z-ORDERING co-locates related data within files using space-filling curves.
# MAGIC
# MAGIC ```sql
# MAGIC OPTIMIZE catalog.schema.events
# MAGIC ZORDER BY (user_id, product_id)
# MAGIC ```
# MAGIC
# MAGIC **Use When:**
# MAGIC * Frequently filter on multiple non-partition columns
# MAGIC * High-cardinality columns (user_id, product_id)
# MAGIC * Queries like: `WHERE user_id = X AND product_id = Y`
# MAGIC
# MAGIC **Benefits:**
# MAGIC * Improved data skipping (50-80% I/O reduction)
# MAGIC * Better compression
# MAGIC * Faster point lookups
# MAGIC
# MAGIC **Don't Use For:**
# MAGIC * Partition columns (already optimized)
# MAGIC * Low-cardinality columns
# MAGIC * Write-heavy tables (adds overhead)
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,⚠️ Common Mistakes & Best Practices
# MAGIC %md
# MAGIC ## ⚠️ COMMON MISTAKES & HOW TO AVOID THEM
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ **Mistake 1: Using SELECT ***
# MAGIC
# MAGIC **Problem:**
# MAGIC ```sql
# MAGIC SELECT * FROM large_table  -- Reads ALL columns
# MAGIC ```
# MAGIC
# MAGIC **Impact:** 
# MAGIC * 70-90% unnecessary I/O
# MAGIC * Slower queries
# MAGIC * Higher costs
# MAGIC
# MAGIC **Solution:**
# MAGIC ```sql
# MAGIC SELECT col1, col2, col3 FROM large_table  -- Only needed columns
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ **Mistake 2: Ignoring Partition Columns**
# MAGIC
# MAGIC **Problem:**
# MAGIC ```sql
# MAGIC -- Table partitioned by date
# MAGIC SELECT * FROM events
# MAGIC WHERE user_id = 'user_123'  -- No date filter!
# MAGIC ```
# MAGIC
# MAGIC **Impact:** 
# MAGIC * Scans ALL partitions
# MAGIC * 100x slower than needed
# MAGIC
# MAGIC **Solution:**
# MAGIC ```sql
# MAGIC SELECT * FROM events
# MAGIC WHERE date >= '2026-04-01'  -- Partition filter FIRST
# MAGIC   AND user_id = 'user_123'
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ **Mistake 3: Joining Before Filtering**
# MAGIC
# MAGIC **Problem:**
# MAGIC ```sql
# MAGIC SELECT *
# MAGIC FROM large_table1 t1
# MAGIC JOIN large_table2 t2 ON t1.id = t2.id  -- Join ALL data first
# MAGIC WHERE t1.date = '2026-04-21'
# MAGIC ```
# MAGIC
# MAGIC **Impact:**
# MAGIC * Massive data shuffle
# MAGIC * 10-50x slower
# MAGIC
# MAGIC **Solution:**
# MAGIC ```sql
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC   SELECT * FROM large_table1 WHERE date = '2026-04-21'
# MAGIC ) t1
# MAGIC JOIN large_table2 t2 ON t1.id = t2.id
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ **Mistake 4: Not Using Serverless Warehouses**
# MAGIC
# MAGIC **Problem:**
# MAGIC * Using Classic warehouses for variable workloads
# MAGIC * Paying for idle time
# MAGIC * Manual cluster management
# MAGIC
# MAGIC **Solution:**
# MAGIC * Use Serverless SQL Warehouses
# MAGIC * Auto-scaling enabled
# MAGIC * Pay only for query execution
# MAGIC * Zero ops overhead
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ **Mistake 5: Inefficient Aggregations**
# MAGIC
# MAGIC **Problem:**
# MAGIC ```sql
# MAGIC SELECT COUNT(DISTINCT user_id) FROM massive_table
# MAGIC -- Takes 10 minutes for 1B rows
# MAGIC ```
# MAGIC
# MAGIC **Solution:**
# MAGIC ```sql
# MAGIC SELECT APPROX_COUNT_DISTINCT(user_id) FROM massive_table
# MAGIC -- Takes 10 seconds, ~2% error acceptable for dashboards
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ **Mistake 6: Over-Partitioning**
# MAGIC
# MAGIC **Problem:**
# MAGIC ```sql
# MAGIC -- Partitioned by date AND hour AND minute
# MAGIC PARTITIONED BY (date, hour, minute)
# MAGIC -- Results in thousands of tiny files
# MAGIC ```
# MAGIC
# MAGIC **Impact:**
# MAGIC * Too many small files
# MAGIC * Slow queries (file listing overhead)
# MAGIC
# MAGIC **Solution:**
# MAGIC * Partition by date only
# MAGIC * Target: 100MB - 1GB per file
# MAGIC * Use Z-ORDERING for granular filters
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ **Mistake 7: No Table Maintenance**
# MAGIC
# MAGIC **Problem:**
# MAGIC * Never running OPTIMIZE
# MAGIC * Small files accumulate
# MAGIC * Query performance degrades over time
# MAGIC
# MAGIC **Solution:**
# MAGIC ```sql
# MAGIC -- Regular maintenance
# MAGIC OPTIMIZE catalog.schema.table
# MAGIC ZORDER BY (frequently_filtered_columns)
# MAGIC
# MAGIC VACUUM catalog.schema.table RETAIN 168 HOURS
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ **Mistake 8: Ignoring Query Profiles**
# MAGIC
# MAGIC **Problem:**
# MAGIC * Query is slow but no investigation
# MAGIC * Guessing at optimizations
# MAGIC
# MAGIC **Solution:**
# MAGIC * Always check Query Profile in Databricks SQL
# MAGIC * Identify bottlenecks (shuffle, skew, large scans)
# MAGIC * Apply targeted optimizations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ **Mistake 9: Not Using Genie Code Agent**
# MAGIC
# MAGIC **Problem:**
# MAGIC * Manually writing complex queries
# MAGIC * Unsure about optimization strategies
# MAGIC
# MAGIC **Solution:**
# MAGIC * Ask Genie Code: "Optimize this query"
# MAGIC * Request explanations
# MAGIC * Learn best practices iteratively
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ **Mistake 10: Forgetting LIMIT During Development**
# MAGIC
# MAGIC **Problem:**
# MAGIC ```sql
# MAGIC SELECT * FROM billion_row_table  -- Full scan during testing!
# MAGIC ```
# MAGIC
# MAGIC **Solution:**
# MAGIC ```sql
# MAGIC SELECT * FROM billion_row_table
# MAGIC WHERE date = '2026-04-21'  -- Partition filter
# MAGIC LIMIT 1000  -- Fast preview
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Key Takeaway:
# MAGIC
# MAGIC > **"Optimization is not optional in big data. A poorly written query can cost 100x more in compute and take 100x longer. Always apply these best practices!"**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Next Steps:
# MAGIC
# MAGIC 1. Practice writing optimized queries
# MAGIC 2. Use Query Profiles to validate performance
# MAGIC 3. Leverage Genie Code for learning
# MAGIC 4. Build dashboards on optimized Gold tables
# MAGIC 5. Monitor and tune regularly
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏆 Certification Readiness:
# MAGIC
# MAGIC These concepts are **critical** for:
# MAGIC * **Databricks Certified Data Engineer Associate**
# MAGIC * **Databricks Certified Data Analyst Associate**
# MAGIC * **Real-world production deployments**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎉 Congratulations!
# MAGIC
# MAGIC You've completed **Phase 9 - Day 37: Databricks SQL & Query Optimization**!
# MAGIC
# MAGIC ### 📚 What's Next?
# MAGIC * **Day 38**: Advanced SQL Features (Window Functions, CTEs, Subqueries)
# MAGIC * **Day 39**: BI Integration (Tableau, Power BI, Looker)
# MAGIC * **Day 40**: Dashboard Design & Best Practices
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Resources:
# MAGIC * [Databricks SQL Documentation](https://docs.databricks.com/sql/index.html)
# MAGIC * [Query Optimization Guide](https://docs.databricks.com/optimizations/index.html)
# MAGIC * [Delta Lake Performance Tuning](https://docs.databricks.com/delta/optimizations/index.html)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👏 **Well Done, Data Engineer!**
# MAGIC #### **@TRRaveendra**