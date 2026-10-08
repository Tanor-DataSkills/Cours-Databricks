# Databricks notebook source
# DBTITLE 1,📘 Notebook Header
# MAGIC %md
# MAGIC # 🔄 Data Engineering Training — Phase 8 Day 35  
# MAGIC ## ⚙️ Jobs & DAG: Multi-task Pipelines & Dependencies  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC * Databricks Jobs & Multi-task Pipelines  
# MAGIC * DAG (Directed Acyclic Graph) Design  
# MAGIC * Task Dependencies & Execution Flow  
# MAGIC * Orchestration Best Practices  
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
# MAGIC Understand how to orchestrate complex data pipelines using Databricks Jobs with DAG-based task dependencies and multi-task workflows.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Engineering Constraints:
# MAGIC * ✅ Use Databricks Serverless Compute
# MAGIC * ✅ Use Unity Catalog for all data access
# MAGIC * ✅ Use Delta format (mandatory)
# MAGIC * ✅ Use PySpark DataFrame API
# MAGIC * ❌ NO RDD usage
# MAGIC * ❌ NO cache() / persist()
# MAGIC * ❌ NO /tmp or local storage
# MAGIC * ✅ Follow modular and orchestrated pipeline design

# COMMAND ----------

# DBTITLE 1,🌐 Section 8: End-to-End DAG Pipeline
# MAGIC %md
# MAGIC # 🌐 SECTION 8 — End-to-End DAG Pipeline Design
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Complete Enterprise Data Pipeline
# MAGIC
# MAGIC Let's design a production-grade DAG for an e-commerce analytics platform.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pipeline Architecture:
# MAGIC
# MAGIC ```
# MAGIC ┌───────────────────────────────────────────────────────┐
# MAGIC │             E-COMMERCE ANALYTICS PIPELINE                  │
# MAGIC │                 (Multi-source, Multi-layer)                  │
# MAGIC └──────────────────────────┬───────────────────────────┘
# MAGIC                            │
# MAGIC          ┌─────────────────┼─────────────────┐
# MAGIC          │                  │                  │
# MAGIC     ┌────▼────┐      ┌────▼────┐      ┌────▼────┐
# MAGIC     │  Task 1  │      │  Task 2  │      │  Task 3  │
# MAGIC     │ Ingest  │      │ Ingest  │      │ Ingest  │
# MAGIC     │ Events  │      │ Users   │      │Products │
# MAGIC     │ (S3)    │      │ (API)   │      │ (JDBC)  │
# MAGIC     └────┬────┘      └────┬────┘      └────┬────┘
# MAGIC          │                  │                  │
# MAGIC          └─────────────────┼─────────────────┘
# MAGIC                            │
# MAGIC                     ┌──────▼──────┐
# MAGIC                     │   Task 4    │
# MAGIC                     │  Validate  │
# MAGIC                     │   Bronze   │
# MAGIC                     └──────┬──────┘
# MAGIC                            │
# MAGIC          ┌─────────────────┼─────────────────┐
# MAGIC          │                  │                  │
# MAGIC     ┌────▼────┐      ┌────▼────┐      ┌────▼────┐
# MAGIC     │  Task 5  │      │  Task 6  │      │  Task 7  │
# MAGIC     │ Silver  │      │ Silver  │      │ Silver  │
# MAGIC     │ Clean   │      │ Enrich  │      │ Join    │
# MAGIC     │ Events  │      │ w/Dims  │      │Products │
# MAGIC     └────┬────┘      └────┬────┘      └────┬────┘
# MAGIC          │                  │                  │
# MAGIC          └─────────────────┼─────────────────┘
# MAGIC                            │
# MAGIC          ┌─────────────────┼─────────────────┐
# MAGIC          │                  │                  │
# MAGIC     ┌────▼────┐      ┌────▼────┐      ┌────▼────┐
# MAGIC     │  Task 8  │      │  Task 9  │      │ Task 10 │
# MAGIC     │  Gold   │      │  Gold   │      │  Gold   │
# MAGIC     │ Revenue │      │ User    │      │ Product │
# MAGIC     │Metrics  │      │Cohorts  │      │Insights │
# MAGIC     └────┬────┘      └────┬────┘      └────┬────┘
# MAGIC          │                  │                  │
# MAGIC          └─────────────────┼─────────────────┘
# MAGIC                            │
# MAGIC                     ┌──────▼──────┐
# MAGIC                     │  Task 11   │
# MAGIC                     │  Update   │
# MAGIC                     │Dashboard │
# MAGIC                     └──────┬──────┘
# MAGIC                            │
# MAGIC                     ┌──────▼──────┐
# MAGIC                     │  Task 12   │
# MAGIC                     │   Send    │
# MAGIC                     │  Alerts   │
# MAGIC                     └─────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Task Breakdown:
# MAGIC
# MAGIC | Task | Layer | Purpose | Dependencies | Can Parallel? |
# MAGIC |------|-------|---------|--------------|---------------|
# MAGIC | Task 1 | Bronze | Ingest events from S3 | None | Yes (1,2,3) |
# MAGIC | Task 2 | Bronze | Ingest users from API | None | Yes (1,2,3) |
# MAGIC | Task 3 | Bronze | Ingest products from DB | None | Yes (1,2,3) |
# MAGIC | Task 4 | Bronze | Validate all bronze data | 1,2,3 | No |
# MAGIC | Task 5 | Silver | Clean events | 4 | Yes (5,6,7) |
# MAGIC | Task 6 | Silver | Enrich with dimensions | 4 | Yes (5,6,7) |
# MAGIC | Task 7 | Silver | Join product data | 4 | Yes (5,6,7) |
# MAGIC | Task 8 | Gold | Revenue metrics | 5,6,7 | Yes (8,9,10) |
# MAGIC | Task 9 | Gold | User cohort analysis | 5,6,7 | Yes (8,9,10) |
# MAGIC | Task 10 | Gold | Product insights | 5,6,7 | Yes (8,9,10) |
# MAGIC | Task 11 | Serving | Update dashboards | 8,9,10 | No |
# MAGIC | Task 12 | Notify | Send alerts | 11 | No |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Execution Characteristics:
# MAGIC
# MAGIC **Total Tasks:** 12  
# MAGIC **Parallelization Points:** 3 (Bronze ingestion, Silver processing, Gold aggregation)  
# MAGIC **Critical Path:** Task 1 → 4 → 5 → 8 → 11 → 12  
# MAGIC **Max Parallel Tasks:** 3 tasks at once
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Resource Optimization:
# MAGIC
# MAGIC ```python
# MAGIC # Compute cluster configuration per task type
# MAGIC
# MAGIC # Heavy ingestion tasks (1,2,3)
# MAGIC Task 1-3: {
# MAGIC     "node_type": "i3.xlarge",
# MAGIC     "num_workers": 5,
# MAGIC     "autoscale": {"min_workers": 2, "max_workers": 8}
# MAGIC }
# MAGIC
# MAGIC # Transformation tasks (5,6,7)
# MAGIC Task 5-7: {
# MAGIC     "node_type": "r5.2xlarge",
# MAGIC     "num_workers": 10,
# MAGIC     "autoscale": {"min_workers": 5, "max_workers": 15}
# MAGIC }
# MAGIC
# MAGIC # Aggregation tasks (8,9,10)
# MAGIC Task 8-10: {
# MAGIC     "node_type": "c5.xlarge",
# MAGIC     "num_workers": 3
# MAGIC }
# MAGIC
# MAGIC # Lightweight tasks (11,12)
# MAGIC Task 11-12: {
# MAGIC     "node_type": "m5.large",
# MAGIC     "num_workers": 1
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Monitoring Metrics:
# MAGIC
# MAGIC * **Task Duration:** Track execution time for each task
# MAGIC * **Success Rate:** % of successful runs
# MAGIC * **Data Volume:** Rows processed per task
# MAGIC * **Resource Utilization:** CPU, memory, I/O
# MAGIC * **SLA Compliance:** Meeting business deadlines

# COMMAND ----------

# DBTITLE 1,✅ Section 9: Workflow Best Practices
# MAGIC %md
# MAGIC # ✅ SECTION 9 — Workflow Best Practices
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Enterprise-Grade Orchestration Patterns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 1️⃣ **Design Principles**
# MAGIC
# MAGIC #### Modularity
# MAGIC * **One task, one responsibility**
# MAGIC * Each task should be independently testable
# MAGIC * Reusable tasks across multiple jobs
# MAGIC
# MAGIC #### Idempotency
# MAGIC * Tasks should produce the same result when run multiple times
# MAGIC * Use **MERGE** instead of **INSERT** for incremental loads
# MAGIC * Use **CREATE OR REPLACE** for views and tables
# MAGIC
# MAGIC #### Clear Naming Convention
# MAGIC ```
# MAGIC <layer>_<action>_<entity>
# MAGIC
# MAGIC Examples:
# MAGIC - bronze_ingest_events
# MAGIC - silver_clean_users
# MAGIC - gold_aggregate_revenue
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ **Dependency Management**
# MAGIC
# MAGIC #### Do's:
# MAGIC * ✅ Make dependencies **explicit** in job configuration
# MAGIC * ✅ Use **fan-out** for parallel processing
# MAGIC * ✅ Keep dependency **depth manageable** (max 5-7 levels)
# MAGIC * ✅ Document **why** dependencies exist
# MAGIC
# MAGIC #### Don'ts:
# MAGIC * ❌ Avoid **circular dependencies**
# MAGIC * ❌ Don't create **monolithic tasks** that do too much
# MAGIC * ❌ Avoid **implicit dependencies** through shared state
# MAGIC * ❌ Don't create **unnecessary sequential steps**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ **Performance Optimization**
# MAGIC
# MAGIC | Strategy | Impact | When to Use |
# MAGIC |----------|--------|-------------|
# MAGIC | **Parallel Tasks** | 30-70% faster | Independent transformations |
# MAGIC | **Right-sized Clusters** | Cost savings | Match cluster to workload |
# MAGIC | **Delta Optimize** | Faster reads | After large writes |
# MAGIC | **Partition Pruning** | Query speedup | Large tables with time dimension |
# MAGIC | **Broadcast Joins** | 2-10x faster | Small dimension tables |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4️⃣ **Data Quality Gates**
# MAGIC
# MAGIC Implement validation checkpoints between layers:
# MAGIC
# MAGIC ```python
# MAGIC # Example validation gate
# MAGIC def validate_bronze_data(df):
# MAGIC     checks = {
# MAGIC         "null_check": df.filter(F.col("id").isNull()).count() == 0,
# MAGIC         "duplicate_check": df.groupBy("id").count().filter("count > 1").count() == 0,
# MAGIC         "schema_check": len(df.columns) == expected_column_count
# MAGIC     }
# MAGIC     
# MAGIC     if not all(checks.values()):
# MAGIC         raise ValueError(f"Validation failed: {checks}")
# MAGIC     
# MAGIC     return df
# MAGIC ```
# MAGIC
# MAGIC Stop pipeline progression if validation fails!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5️⃣ **Error Handling Strategy**
# MAGIC
# MAGIC #### Transient Errors (Retry)
# MAGIC * Network timeouts
# MAGIC * API rate limits
# MAGIC * Resource unavailability
# MAGIC
# MAGIC #### Permanent Errors (Fail Fast)
# MAGIC * Schema mismatches
# MAGIC * Missing source data
# MAGIC * Authorization failures
# MAGIC
# MAGIC #### Partial Failures (Continue with Alerts)
# MAGIC * Optional enrichment failures
# MAGIC * Non-critical data sources
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6️⃣ **Monitoring & Observability**
# MAGIC
# MAGIC **Metrics to Track:**
# MAGIC
# MAGIC * **Task-level:**
# MAGIC   * Execution duration
# MAGIC   * Success/failure rate
# MAGIC   * Data volume (rows in/out)
# MAGIC   * Resource utilization
# MAGIC
# MAGIC * **Job-level:**
# MAGIC   * End-to-end latency
# MAGIC   * SLA compliance
# MAGIC   * Cost per run
# MAGIC   * Retry frequency
# MAGIC
# MAGIC **Alerting Rules:**
# MAGIC ```
# MAGIC IF task_duration > 2 * avg_duration THEN alert
# MAGIC IF task_failure_rate > 5% THEN alert
# MAGIC IF job_latency > SLA THEN page_oncall
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 7️⃣ **Testing Strategy**
# MAGIC
# MAGIC #### Unit Testing (Individual Tasks)
# MAGIC ```python
# MAGIC def test_bronze_ingestion():
# MAGIC     # Arrange
# MAGIC     test_data = create_test_data()
# MAGIC     
# MAGIC     # Act
# MAGIC     result = bronze_ingest_task(test_data)
# MAGIC     
# MAGIC     # Assert
# MAGIC     assert result.count() == expected_count
# MAGIC     assert result.schema == expected_schema
# MAGIC ```
# MAGIC
# MAGIC #### Integration Testing (Task Dependencies)
# MAGIC ```python
# MAGIC def test_bronze_to_silver_flow():
# MAGIC     # Test that silver task correctly processes bronze output
# MAGIC     bronze_output = run_bronze_task()
# MAGIC     silver_output = run_silver_task(bronze_output)
# MAGIC     assert validate_silver_quality(silver_output)
# MAGIC ```
# MAGIC
# MAGIC #### End-to-End Testing
# MAGIC * Run entire pipeline on subset of data
# MAGIC * Validate final outputs
# MAGIC * Check data lineage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 8️⃣ **Deployment Best Practices**
# MAGIC
# MAGIC #### Environment Strategy
# MAGIC ```
# MAGIC dev → staging → production
# MAGIC ```
# MAGIC
# MAGIC * **Dev:** Individual developer testing
# MAGIC * **Staging:** Integration testing with production-like data
# MAGIC * **Production:** Live customer-facing pipelines
# MAGIC
# MAGIC #### CI/CD Pipeline
# MAGIC ```
# MAGIC 1. Code commit
# MAGIC 2. Run unit tests
# MAGIC 3. Deploy to dev
# MAGIC 4. Run integration tests
# MAGIC 5. Deploy to staging
# MAGIC 6. Run E2E tests
# MAGIC 7. Manual approval
# MAGIC 8. Deploy to production
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 9️⃣ **Documentation Requirements**
# MAGIC
# MAGIC For each task, document:
# MAGIC
# MAGIC * **Purpose:** What does this task do?
# MAGIC * **Inputs:** Source tables/files
# MAGIC * **Outputs:** Destination tables/files
# MAGIC * **Dependencies:** Upstream tasks
# MAGIC * **SLA:** Expected completion time
# MAGIC * **Owner:** Team/individual responsible
# MAGIC * **Runbook:** How to debug failures
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚫 Common Anti-Patterns
# MAGIC
# MAGIC | Anti-Pattern | Problem | Solution |
# MAGIC |--------------|---------|----------|
# MAGIC | **God Task** | One task does everything | Split into modular tasks |
# MAGIC | **Hidden Dependencies** | Tasks depend on external state | Make dependencies explicit |
# MAGIC | **No Retry Logic** | Transient failures break pipeline | Add retry configuration |
# MAGIC | **Manual Triggers** | Requires human intervention | Automate with schedules/triggers |
# MAGIC | **No Monitoring** | Can't detect issues | Add metrics and alerts |
# MAGIC | **Hardcoded Values** | Not reusable | Use parameters |
# MAGIC | **No Testing** | Bugs in production | Implement test suite |

# COMMAND ----------

# DBTITLE 1,🤖 Section 10: Genie Code Agent Prompts
# MAGIC %md
# MAGIC # 🤖 SECTION 10 — Genie Code Agent Prompts
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Using Genie Code Agent for Jobs & DAG Orchestration
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Sample Prompts for Multi-task Workflows:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 1️⃣ **Creating Multi-task Pipelines**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Create a Databricks Job with 3 tasks:
# MAGIC 1. Bronze ingestion from S3
# MAGIC 2. Silver transformation
# MAGIC 3. Gold aggregation
# MAGIC
# MAGIC Task 2 depends on Task 1, Task 3 depends on Task 2.
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2️⃣ **Designing DAG Pipelines**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Design a DAG pipeline for medallion architecture:
# MAGIC - Bronze: Ingest events, users, products (parallel)
# MAGIC - Silver: Clean and enrich (depends on bronze validation)
# MAGIC - Gold: Daily metrics, user cohorts (parallel, depends on silver)
# MAGIC - Serving: Update dashboard (depends on all gold tasks)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 3️⃣ **Adding Task Dependencies**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Add task dependencies to my job:
# MAGIC - transform_events depends on ingest_events
# MAGIC - transform_users depends on ingest_users
# MAGIC - join_events_users depends on both transform tasks
# MAGIC - aggregate_metrics depends on join_events_users
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 4️⃣ **Orchestrating Medallion Pipeline**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Orchestrate my medallion pipeline notebooks:
# MAGIC - Bronze: /bronze/ingest_raw_data
# MAGIC - Silver: /silver/clean_data and /silver/enrich_data (parallel)
# MAGIC - Gold: /gold/aggregate_metrics (depends on both silver)
# MAGIC
# MAGIC Schedule to run daily at 2 AM UTC.
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 5️⃣ **Adding Error Handling**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Add retry logic to my job:
# MAGIC - Max 3 retries for all tasks
# MAGIC - Wait 5 minutes between retries
# MAGIC - Send email alert on failure to data-team@company.com
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 6️⃣ **Parallel Processing**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Optimize my pipeline for parallel execution:
# MAGIC - Ingest 5 data sources simultaneously
# MAGIC - Process each source independently
# MAGIC - Merge results after all sources complete
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 7️⃣ **Configuring Job Clusters**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Create job with cluster configuration:
# MAGIC - Ingestion tasks: 5 workers, i3.xlarge
# MAGIC - Transformation tasks: 10 workers, r5.2xlarge with autoscaling
# MAGIC - Aggregation tasks: Serverless compute
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 8️⃣ **Debugging Failed Tasks**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Analyze why task 'silver_transformation' is failing in job run 12345.
# MAGIC Show me:
# MAGIC - Error message
# MAGIC - Task logs
# MAGIC - Input data statistics
# MAGIC - Suggestions to fix
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 9️⃣ **Setting Up Incremental Processing**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Create an incremental pipeline:
# MAGIC - Process only new data since last run
# MAGIC - Use watermark column: updated_at
# MAGIC - Store checkpoint in /checkpoints/my_job
# MAGIC - Run every 15 minutes
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 🔟 **Complex Multi-layer DAG**
# MAGIC
# MAGIC **Prompt:**
# MAGIC ```
# MAGIC Create production-grade e-commerce analytics job:
# MAGIC
# MAGIC **Bronze Layer (Parallel):**
# MAGIC - Ingest orders from S3
# MAGIC - Ingest customers from Salesforce API
# MAGIC - Ingest products from MySQL
# MAGIC
# MAGIC **Validation:**
# MAGIC - Validate all bronze tables (depends on all ingestion)
# MAGIC
# MAGIC **Silver Layer (Parallel after validation):**
# MAGIC - Clean orders (dedupe, standardize)
# MAGIC - Enrich orders with customer data
# MAGIC - Join with product catalog
# MAGIC
# MAGIC **Gold Layer (Parallel):**
# MAGIC - Daily revenue metrics
# MAGIC - Customer lifetime value
# MAGIC - Product performance insights
# MAGIC
# MAGIC **Serving:**
# MAGIC - Update Tableau dashboard
# MAGIC - Send summary email to executives
# MAGIC
# MAGIC **Configuration:**
# MAGIC - Schedule: Daily at 3 AM EST
# MAGIC - Retry: Max 3 attempts
# MAGIC - Alerts: Slack channel #data-alerts
# MAGIC - Timeout: 2 hours
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,📚 Section 11: Summary & Interview Questions
# MAGIC %md
# MAGIC # 📚 SECTION 11 — Summary & Interview Questions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Key Learnings Recap
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 What We Covered:
# MAGIC
# MAGIC 1. **Orchestration Fundamentals**
# MAGIC    * Why orchestration is essential for data pipelines
# MAGIC    * Automated coordination of dependent tasks
# MAGIC
# MAGIC 2. **Databricks Jobs & Multi-task Pipelines**
# MAGIC    * Job components: tasks, clusters, schedules, dependencies
# MAGIC    * Multi-task pipeline architecture
# MAGIC
# MAGIC 3. **DAG (Directed Acyclic Graph)**
# MAGIC    * Nodes = tasks, edges = dependencies
# MAGIC    * No cycles, clear execution flow
# MAGIC
# MAGIC 4. **Task Dependencies**
# MAGIC    * Sequential vs parallel execution
# MAGIC    * Fan-out and fan-in patterns
# MAGIC
# MAGIC 5. **Pipeline Design**
# MAGIC    * Medallion architecture with task-based orchestration
# MAGIC    * Bronze → Silver → Gold layer tasks
# MAGIC
# MAGIC 6. **Error Handling & Retry**
# MAGIC    * Automatic retry logic
# MAGIC    * Failure notifications
# MAGIC    * Dead letter queues
# MAGIC
# MAGIC 7. **End-to-End DAG**
# MAGIC    * Multi-source, multi-layer enterprise pipeline
# MAGIC    * Resource optimization strategies
# MAGIC
# MAGIC 8. **Workflow Best Practices**
# MAGIC    * Modularity, idempotency, clear naming
# MAGIC    * Testing, monitoring, documentation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Interview Questions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟢 Junior Level:
# MAGIC
# MAGIC **Q1: What is a Databricks Job?**
# MAGIC
# MAGIC **A:** A Databricks Job is an automated, scheduled execution of one or more tasks (notebooks, Python scripts, JARs, pipelines). Jobs enable orchestration of data pipelines with dependency management.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q2: What does DAG stand for and why is "acyclic" important?**
# MAGIC
# MAGIC **A:** DAG stands for Directed Acyclic Graph. "Acyclic" means no cycles/loops — this ensures tasks don't depend on themselves and the pipeline has a clear start and end, preventing infinite execution loops.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q3: What's the difference between sequential and parallel task execution?**
# MAGIC
# MAGIC **A:** 
# MAGIC * **Sequential:** Tasks run one after another (Task A → Task B → Task C)
# MAGIC * **Parallel:** Independent tasks run simultaneously (Task A → [Task B, Task C])
# MAGIC
# MAGIC Parallel execution is faster but requires tasks to be independent.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q4: What are the three main components of a task dependency?**
# MAGIC
# MAGIC **A:**
# MAGIC 1. **Parent task(s):** Tasks that must complete first
# MAGIC 2. **Child task:** Task that depends on parent(s)
# MAGIC 3. **Dependency rule:** When child can start (e.g., "after all parents succeed")
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q5: Why should tasks be idempotent?**
# MAGIC
# MAGIC **A:** Idempotency ensures tasks produce the same result when run multiple times. This is critical for retry logic — if a task fails and is retried, it won't create duplicate data or incorrect state.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟡 Mid Level:
# MAGIC
# MAGIC **Q6: How would you design a multi-task pipeline for medallion architecture?**
# MAGIC
# MAGIC **A:** 
# MAGIC ```
# MAGIC Bronze Layer:
# MAGIC - Task: bronze_ingest (no dependencies)
# MAGIC - Task: bronze_validate (depends on ingest)
# MAGIC
# MAGIC Silver Layer:
# MAGIC - Task: silver_clean (depends on validate)
# MAGIC - Task: silver_enrich (depends on validate)
# MAGIC - These can run in parallel
# MAGIC
# MAGIC Gold Layer:
# MAGIC - Task: gold_aggregate (depends on clean & enrich)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q7: What's the difference between retry and failure handling?**
# MAGIC
# MAGIC **A:**
# MAGIC * **Retry:** Automatically re-run a task after transient failure (network timeout, resource unavailable). Use for recoverable errors.
# MAGIC * **Failure Handling:** Define what happens when retries exhaust (alert, fail pipeline, run alternate path). Use for permanent errors.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q8: How do you optimize a pipeline with long sequential tasks?**
# MAGIC
# MAGIC **A:** Strategies:
# MAGIC 1. **Break into smaller parallel tasks:** Split monolithic task into independent subtasks
# MAGIC 2. **Enable autoscaling:** Scale clusters dynamically
# MAGIC 3. **Optimize SQL/Spark:** Use partition pruning, broadcast joins
# MAGIC 4. **Right-size clusters:** Match compute to workload
# MAGIC 5. **Use Delta Lake optimizations:** OPTIMIZE, Z-ORDER
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q9: What metrics should you monitor for a production job?**
# MAGIC
# MAGIC **A:** 
# MAGIC * **Reliability:** Success rate, failure rate, retry frequency
# MAGIC * **Performance:** Task duration, end-to-end latency
# MAGIC * **Data Quality:** Row counts, null rates, schema changes
# MAGIC * **Resources:** Cluster utilization, cost per run
# MAGIC * **SLA:** % of runs completing on time
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q10: How would you handle a task that depends on an external API with rate limits?**
# MAGIC
# MAGIC **A:**
# MAGIC 1. Implement **exponential backoff** retry logic
# MAGIC 2. Set **max retries** (e.g., 5 attempts)
# MAGIC 3. Use **timeout** configuration
# MAGIC 4. Add **circuit breaker** pattern (stop if API is down)
# MAGIC 5. Implement **checkpointing** to resume from last successful point
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 Senior Level:
# MAGIC
# MAGIC **Q11: Design a fault-tolerant pipeline for ingesting data from 10 sources.**
# MAGIC
# MAGIC **A:**
# MAGIC ```
# MAGIC Architecture:
# MAGIC 1. Create 10 parallel ingestion tasks (one per source)
# MAGIC 2. Each task writes to separate bronze table
# MAGIC 3. Add validation task that depends on ALL ingestion tasks
# MAGIC 4. Validation checks:
# MAGIC    - All sources ingested successfully
# MAGIC    - Expected row counts within threshold
# MAGIC    - Schema compliance
# MAGIC 5. If any source fails:
# MAGIC    - Retry that specific task (max 3 attempts)
# MAGIC    - If still fails, write to dead letter table
# MAGIC    - Continue pipeline with successful sources
# MAGIC    - Alert data team
# MAGIC 6. Downstream tasks read from bronze, excluding failed sources
# MAGIC 7. Dashboard shows source health status
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q12: How do you implement incremental processing in a multi-task pipeline?**
# MAGIC
# MAGIC **A:**
# MAGIC ```python
# MAGIC # Task 1: Identify new data
# MAGIC max_timestamp = spark.sql(
# MAGIC     "SELECT MAX(processed_at) FROM silver.events"
# MAGIC ).collect()[0][0]
# MAGIC
# MAGIC # Task 2: Process only new data
# MAGIC new_data = spark.read.table("bronze.events") \
# MAGIC     .filter(f"ingestion_time > '{max_timestamp}'")
# MAGIC
# MAGIC # Task 3: Merge into silver (idempotent)
# MAGIC new_data.write.format("delta").mode("append") \
# MAGIC     .option("mergeSchema", "true") \
# MAGIC     .saveAsTable("silver.events")
# MAGIC
# MAGIC # Use Delta Lake time travel for audit trail
# MAGIC ```
# MAGIC
# MAGIC Key points:
# MAGIC * **Watermark column** tracks processed data
# MAGIC * **Merge operation** prevents duplicates
# MAGIC * **Checkpointing** enables resume on failure
# MAGIC * **Idempotent** design allows safe retries
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q13: Explain the trade-offs between fan-out parallelism and sequential processing.**
# MAGIC
# MAGIC **A:**
# MAGIC
# MAGIC **Fan-out (Parallel) Advantages:**
# MAGIC * Faster execution (3x-5x speedup possible)
# MAGIC * Better resource utilization
# MAGIC * Independent failure isolation
# MAGIC
# MAGIC **Fan-out Disadvantages:**
# MAGIC * Higher resource costs (multiple clusters)
# MAGIC * Complex dependency management
# MAGIC * Harder to debug
# MAGIC * Potential resource contention
# MAGIC
# MAGIC **Sequential Advantages:**
# MAGIC * Simpler logic and debugging
# MAGIC * Lower resource usage
# MAGIC * Easier to reason about state
# MAGIC
# MAGIC **Sequential Disadvantages:**
# MAGIC * Slower execution
# MAGIC * Single point of failure blocks everything
# MAGIC
# MAGIC **When to use:**
# MAGIC * **Parallel:** Independent transformations, time-sensitive pipelines
# MAGIC * **Sequential:** Small data, complex state management, tight resource budgets
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q14: How do you handle schema evolution in a multi-task pipeline?**
# MAGIC
# MAGIC **A:**
# MAGIC
# MAGIC ```python
# MAGIC # Strategy 1: Schema enforcement in Bronze
# MAGIC bronze_df.write.format("delta") \
# MAGIC     .option("mergeSchema", "false")  # Reject schema changes
# MAGIC     .mode("append") \
# MAGIC     .saveAsTable("bronze.events")
# MAGIC
# MAGIC # Strategy 2: Schema evolution with validation
# MAGIC if new_schema != expected_schema:
# MAGIC     added_cols = new_schema - expected_schema
# MAGIC     removed_cols = expected_schema - new_schema
# MAGIC     
# MAGIC     if removed_cols:  # Breaking change
# MAGIC         raise SchemaEvolutionError("Columns removed")
# MAGIC     elif added_cols:  # Non-breaking change
# MAGIC         # Allow evolution, add nulls for existing rows
# MAGIC         bronze_df.write.option("mergeSchema", "true")
# MAGIC         notify_downstream_teams(added_cols)
# MAGIC
# MAGIC # Strategy 3: Schema versioning
# MAGIC bronze_df.withColumn("schema_version", lit("v2"))
# MAGIC
# MAGIC # Downstream tasks handle multiple versions
# MAGIC silver_df = spark.read.table("bronze.events") \
# MAGIC     .withColumn("normalized_col", 
# MAGIC         when(col("schema_version") == "v1", col("old_col_name"))
# MAGIC         .otherwise(col("new_col_name"))
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q15: Design a CI/CD pipeline for deploying Databricks Jobs.**
# MAGIC
# MAGIC **A:**
# MAGIC
# MAGIC ```yaml
# MAGIC # .github/workflows/deploy-jobs.yml
# MAGIC
# MAGIC name: Deploy Databricks Jobs
# MAGIC
# MAGIC on:
# MAGIC   push:
# MAGIC     branches: [main]
# MAGIC
# MAGIC jobs:
# MAGIC   test:
# MAGIC     runs-on: ubuntu-latest
# MAGIC     steps:
# MAGIC       - name: Run unit tests
# MAGIC         run: pytest tests/
# MAGIC       
# MAGIC       - name: Run integration tests on dev workspace
# MAGIC         run: |
# MAGIC           databricks jobs run-now --job-id $DEV_JOB_ID
# MAGIC           databricks jobs get-run --run-id $RUN_ID
# MAGIC           # Assert success
# MAGIC   
# MAGIC   deploy-staging:
# MAGIC     needs: test
# MAGIC     steps:
# MAGIC       - name: Deploy to staging
# MAGIC         run: |
# MAGIC           databricks jobs create --json-file jobs/staging-job.json
# MAGIC       
# MAGIC       - name: Run smoke tests
# MAGIC         run: pytest tests/smoke/
# MAGIC   
# MAGIC   deploy-prod:
# MAGIC     needs: deploy-staging
# MAGIC     steps:
# MAGIC       - name: Manual approval
# MAGIC         uses: trstringer/manual-approval@v1
# MAGIC       
# MAGIC       - name: Deploy to production
# MAGIC         run: |
# MAGIC           databricks jobs create --json-file jobs/prod-job.json
# MAGIC       
# MAGIC       - name: Monitor first run
# MAGIC         run: |
# MAGIC           databricks jobs run-now --job-id $PROD_JOB_ID
# MAGIC           # Monitor for 30 minutes
# MAGIC ```
# MAGIC
# MAGIC Key components:
# MAGIC * **Unit tests** before deployment
# MAGIC * **Dev workspace** for integration testing
# MAGIC * **Staging** environment with smoke tests
# MAGIC * **Manual approval** gate for production
# MAGIC * **Monitoring** of first production run
# MAGIC * **Rollback** mechanism if issues detected
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚫 Common Mistakes to Avoid
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 1️⃣ **Monolithic Pipelines**
# MAGIC **Mistake:** One giant notebook/task that does everything  
# MAGIC **Impact:** Hard to debug, can't parallelize, failure breaks everything  
# MAGIC **Solution:** Break into modular tasks with clear responsibilities
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ **Poor Dependency Design**
# MAGIC **Mistake:** Creating unnecessary sequential dependencies  
# MAGIC **Impact:** Longer execution time, wasted resources  
# MAGIC **Solution:** Analyze task independence, enable parallelism where possible
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ **No Retry Handling**
# MAGIC **Mistake:** Not configuring retry logic  
# MAGIC **Impact:** Transient failures cause complete pipeline failure  
# MAGIC **Solution:** Add retry configuration with exponential backoff
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4️⃣ **Manual Execution**
# MAGIC **Mistake:** Running pipelines manually  
# MAGIC **Impact:** Human error, missed runs, no audit trail  
# MAGIC **Solution:** Automate with schedules or event triggers
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5️⃣ **Lack of Monitoring**
# MAGIC **Mistake:** No metrics, logs, or alerts  
# MAGIC **Impact:** Issues go undetected, SLA breaches  
# MAGIC **Solution:** Implement comprehensive monitoring and alerting
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6️⃣ **Hardcoded Values**
# MAGIC **Mistake:** Hardcoding paths, dates, thresholds in code  
# MAGIC **Impact:** Not reusable, requires code changes for different environments  
# MAGIC **Solution:** Use job parameters and environment-specific configs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 7️⃣ **Ignoring Idempotency**
# MAGIC **Mistake:** Tasks create duplicate data when re-run  
# MAGIC **Impact:** Data quality issues, incorrect metrics  
# MAGIC **Solution:** Use MERGE instead of INSERT, CREATE OR REPLACE for views
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 8️⃣ **No Testing**
# MAGIC **Mistake:** Deploying untested pipelines to production  
# MAGIC **Impact:** Bugs discovered in production, data corruption  
# MAGIC **Solution:** Implement unit, integration, and E2E tests
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 9️⃣ **Over-parallelization**
# MAGIC **Mistake:** Running too many tasks in parallel  
# MAGIC **Impact:** Resource contention, higher costs, potential failures  
# MAGIC **Solution:** Balance parallelism with resource availability
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔟 **Poor Documentation**
# MAGIC **Mistake:** No documentation of task purposes, dependencies, SLAs  
# MAGIC **Impact:** Hard to debug, knowledge silos, onboarding difficulties  
# MAGIC **Solution:** Document every task with purpose, I/O, dependencies, runbook
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎉 Congratulations!
# MAGIC
# MAGIC You now understand how to design, implement, and optimize multi-task data pipelines with DAG-based orchestration in Databricks!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Next Steps:
# MAGIC
# MAGIC 1. Create your first multi-task job in Databricks UI
# MAGIC 2. Design a DAG for your current project
# MAGIC 3. Implement retry and error handling
# MAGIC 4. Set up monitoring and alerts
# MAGIC 5. Practice with Genie Code Agent prompts
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📚 Additional Resources:
# MAGIC
# MAGIC * [Databricks Jobs Documentation](https://docs.databricks.com/workflows/jobs/jobs.html)
# MAGIC * [Workflow Orchestration Best Practices](https://docs.databricks.com/workflows/jobs/workflows-best-practices.html)
# MAGIC * [Delta Live Tables for Declarative Pipelines](https://docs.databricks.com/delta-live-tables/index.html)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **@TRRaveendra** | Phase 8 Day 35 | Jobs & DAG Orchestration

# COMMAND ----------

# DBTITLE 1,Task 1: Bronze Ingestion
# ============================================================
# TASK 1: BRONZE INGESTION
# Purpose: Simulate ingesting raw data from source
# Dependencies: None (root task)
# ============================================================

from pyspark.sql import functions as F
from datetime import datetime, timedelta
import random

print("⚙️ Task 1: Bronze Ingestion - STARTED")
print("=" * 60)

# Simulate raw event data from e-commerce platform
raw_data = []
base_date = datetime(2026, 4, 1)

for i in range(1000):
    event_date = base_date + timedelta(days=random.randint(0, 20))
    raw_data.append((
        f"event_{i+1}",
        random.choice(["page_view", "add_to_cart", "purchase", "checkout"]),
        f"user_{random.randint(1, 100)}",
        random.choice(["mobile", "desktop", "tablet"]),
        round(random.uniform(10, 500), 2),
        event_date,
        random.choice(["US", "UK", "CA", "AU", "DE"]),
        random.choice([None, "promo_10", "promo_20", None, None])  # Some nulls
    ))

# Create DataFrame
schema = ["event_id", "event_type", "user_id", "device", "value", "timestamp", "country", "promo_code"]
bronze_df = spark.createDataFrame(raw_data, schema)

# Add metadata columns (common in Bronze layer)
bronze_df = bronze_df \
    .withColumn("ingestion_timestamp", F.current_timestamp()) \
    .withColumn("source_system", F.lit("ecommerce_api"))

print(f"✅ Ingested {bronze_df.count()} raw events")
print(f"📋 Schema: {', '.join(bronze_df.columns)}")

# Display sample
print("\n🔍 Sample Bronze Data:")
display(bronze_df.limit(5))

print("\n✅ Task 1: Bronze Ingestion - COMPLETED")

# COMMAND ----------

# DBTITLE 1,Task 2: Bronze Validation
# ============================================================
# TASK 2: BRONZE VALIDATION
# Purpose: Validate schema and data quality
# Dependencies: Task 1 (bronze_df must exist)
# ============================================================

from pyspark.sql import functions as F

print("⚙️ Task 2: Bronze Validation - STARTED")
print("=" * 60)

# Validation checks
validation_results = {}

# 1. Check required columns
required_columns = ["event_id", "event_type", "user_id", "timestamp"]
missing_columns = [col for col in required_columns if col not in bronze_df.columns]
validation_results["missing_columns"] = missing_columns

# 2. Check for nulls in critical columns
null_counts = bronze_df.select(
    F.sum(F.when(F.col("event_id").isNull(), 1).otherwise(0)).alias("null_event_id"),
    F.sum(F.when(F.col("event_type").isNull(), 1).otherwise(0)).alias("null_event_type"),
    F.sum(F.when(F.col("user_id").isNull(), 1).otherwise(0)).alias("null_user_id")
).collect()[0]

validation_results["null_counts"] = null_counts.asDict()

# 3. Check data ranges
stats = bronze_df.select(
    F.min("timestamp").alias("min_date"),
    F.max("timestamp").alias("max_date"),
    F.min("value").alias("min_value"),
    F.max("value").alias("max_value")
).collect()[0]

validation_results["data_ranges"] = stats.asDict()

# 4. Check for duplicates
duplicate_count = bronze_df.groupBy("event_id").count().filter(F.col("count") > 1).count()
validation_results["duplicate_event_ids"] = duplicate_count

# Print validation report
print("\n📊 VALIDATION REPORT:")
print("=" * 60)
print(f"Missing Critical Columns: {validation_results['missing_columns'] if validation_results['missing_columns'] else 'None ✅'}")
print(f"Null Event IDs: {validation_results['null_counts']['null_event_id']}")
print(f"Null Event Types: {validation_results['null_counts']['null_event_type']}")
print(f"Null User IDs: {validation_results['null_counts']['null_user_id']}")
print(f"Date Range: {validation_results['data_ranges']['min_date']} to {validation_results['data_ranges']['max_date']}")
print(f"Value Range: ${validation_results['data_ranges']['min_value']} to ${validation_results['data_ranges']['max_value']}")
print(f"Duplicate Event IDs: {validation_results['duplicate_event_ids']}")

# Overall validation status
validation_passed = (
    len(validation_results['missing_columns']) == 0 and
    validation_results['null_counts']['null_event_id'] == 0 and
    validation_results['duplicate_event_ids'] == 0
)

if validation_passed:
    print("\n✅ VALIDATION PASSED - Proceeding to Silver layer")
else:
    print("\n⚠️ VALIDATION ISSUES DETECTED - Review required")

print("\n✅ Task 2: Bronze Validation - COMPLETED")

# COMMAND ----------

# DBTITLE 1,Task 3: Silver Clean (Parallel)
# ============================================================
# TASK 3: SILVER CLEAN
# Purpose: Clean, deduplicate, standardize data
# Dependencies: Task 2 (validation passed)
# Execution: Can run in PARALLEL with Task 4
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("⚙️ Task 3: Silver Clean - STARTED (Parallel Track 1)")
print("=" * 60)

# Start cleaning operations
silver_clean_df = bronze_df

# 1. Remove duplicates (keep latest by ingestion_timestamp)
window_spec = Window.partitionBy("event_id").orderBy(F.col("ingestion_timestamp").desc())
silver_clean_df = silver_clean_df \
    .withColumn("row_num", F.row_number().over(window_spec)) \
    .filter(F.col("row_num") == 1) \
    .drop("row_num")

print(f"✅ Deduplication complete: {silver_clean_df.count()} unique events")

# 2. Standardize column values
silver_clean_df = silver_clean_df \
    .withColumn("event_type", F.lower(F.col("event_type"))) \
    .withColumn("country", F.upper(F.col("country"))) \
    .withColumn("device", F.initcap(F.col("device")))

print("✅ Standardization complete")

# 3. Handle nulls - fill promo_code with 'NO_PROMO'
silver_clean_df = silver_clean_df \
    .withColumn("promo_code", F.coalesce(F.col("promo_code"), F.lit("NO_PROMO")))

print("✅ Null handling complete")

# 4. Add data quality flags
silver_clean_df = silver_clean_df \
    .withColumn("is_high_value", F.when(F.col("value") > 100, True).otherwise(False)) \
    .withColumn("is_mobile", F.when(F.col("device") == "Mobile", True).otherwise(False))

print("✅ Quality flags added")

# Display sample
print("\n🔍 Sample Silver Clean Data:")
display(silver_clean_df.select("event_id", "event_type", "user_id", "device", "value", "promo_code", "is_high_value").limit(5))

print("\n✅ Task 3: Silver Clean - COMPLETED")

# COMMAND ----------

# DBTITLE 1,Task 4: Silver Enrich (Parallel)
# ============================================================
# TASK 4: SILVER ENRICH
# Purpose: Enrich data with reference tables and derived fields
# Dependencies: Task 2 (validation passed)
# Execution: Can run in PARALLEL with Task 3
# ============================================================

from pyspark.sql import functions as F

print("⚙️ Task 4: Silver Enrich - STARTED (Parallel Track 2)")
print("=" * 60)

# Create reference data (simulating dimension tables)
print("📋 Creating reference data...")

# Country reference with region mapping
country_ref = spark.createDataFrame([
    ("US", "North America", "America/New_York"),
    ("CA", "North America", "America/Toronto"),
    ("UK", "Europe", "Europe/London"),
    ("DE", "Europe", "Europe/Berlin"),
    ("AU", "Asia Pacific", "Australia/Sydney")
], ["country", "region", "timezone"])

# User segment mapping (simulated)
user_segments = spark.createDataFrame([
    (f"user_{i}", random.choice(["Premium", "Standard", "Basic"])) 
    for i in range(1, 101)
], ["user_id", "user_segment"])

print("✅ Reference data created")

# Start enrichment
silver_enrich_df = bronze_df

# 1. Join with country reference
silver_enrich_df = silver_enrich_df.join(
    country_ref,
    on="country",
    how="left"
)

print("✅ Country enrichment complete")

# 2. Join with user segments
silver_enrich_df = silver_enrich_df.join(
    user_segments,
    on="user_id",
    how="left"
)

print("✅ User segment enrichment complete")

# 3. Add derived temporal fields
silver_enrich_df = silver_enrich_df \
    .withColumn("event_date", F.to_date(F.col("timestamp"))) \
    .withColumn("event_hour", F.hour(F.col("timestamp"))) \
    .withColumn("day_of_week", F.dayofweek(F.col("timestamp"))) \
    .withColumn("is_weekend", F.when(F.col("day_of_week").isin([1, 7]), True).otherwise(False))

print("✅ Temporal enrichment complete")

# 4. Calculate event sequence for each user
window_spec = Window.partitionBy("user_id").orderBy("timestamp")
silver_enrich_df = silver_enrich_df \
    .withColumn("user_event_sequence", F.row_number().over(window_spec))

print("✅ Sequence enrichment complete")

# Display sample
print("\n🔍 Sample Silver Enriched Data:")
display(silver_enrich_df.select(
    "event_id", "user_id", "user_segment", "region", "event_date", 
    "day_of_week", "is_weekend", "user_event_sequence"
).limit(5))

print("\n✅ Task 4: Silver Enrich - COMPLETED")

# COMMAND ----------

# DBTITLE 1,Task 5: Gold Aggregate
# ============================================================
# TASK 5: GOLD AGGREGATE
# Purpose: Create business-level aggregations and metrics
# Dependencies: Task 3 AND Task 4 (both must complete)
# ============================================================

from pyspark.sql import functions as F

print("⚙️ Task 5: Gold Aggregate - STARTED")
print("=" * 60)

# Combine insights from both silver tables
# In production, we'd read from silver.clean_events and silver.enriched_events
# Here we'll use silver_clean_df and silver_enrich_df

print("🔗 Merging Silver Clean + Silver Enrich data...")

# Join clean and enriched data
gold_base = silver_clean_df.join(
    silver_enrich_df.select(
        "event_id", "region", "user_segment", "event_date", 
        "day_of_week", "is_weekend", "user_event_sequence"
    ),
    on="event_id",
    how="inner"
)

print("✅ Data merge complete")

# Create aggregations for business intelligence
print("\n📊 Creating Gold Layer Aggregations...\n")

# Aggregation 1: Daily metrics by region
print("1️⃣ Daily Metrics by Region")
daily_region_metrics = gold_base.groupBy("event_date", "region").agg(
    F.count("event_id").alias("total_events"),
    F.countDistinct("user_id").alias("unique_users"),
    F.sum("value").alias("total_value"),
    F.avg("value").alias("avg_value"),
    F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
    F.sum(F.when(F.col("is_high_value"), F.col("value")).otherwise(0)).alias("high_value_revenue")
).orderBy("event_date", "region")

display(daily_region_metrics.limit(10))

# Aggregation 2: User segment performance
print("\n2️⃣ User Segment Performance")
segment_metrics = gold_base.groupBy("user_segment").agg(
    F.countDistinct("user_id").alias("users"),
    F.count("event_id").alias("total_events"),
    F.sum("value").alias("total_revenue"),
    F.avg("value").alias("avg_transaction_value"),
    (F.sum("value") / F.countDistinct("user_id")).alias("revenue_per_user")
).orderBy(F.desc("total_revenue"))

display(segment_metrics)

# Aggregation 3: Device and promo analysis
print("\n3️⃣ Device & Promo Code Analysis")
device_promo_metrics = gold_base.groupBy("device", "promo_code").agg(
    F.count("event_id").alias("events"),
    F.sum("value").alias("revenue"),
    F.avg("value").alias("avg_value")
).orderBy(F.desc("revenue"))

display(device_promo_metrics.limit(10))

# Aggregation 4: Weekend vs Weekday comparison
print("\n4️⃣ Weekend vs Weekday Comparison")
weekend_comparison = gold_base.groupBy("is_weekend").agg(
    F.count("event_id").alias("total_events"),
    F.countDistinct("user_id").alias("unique_users"),
    F.sum("value").alias("total_revenue"),
    F.avg("value").alias("avg_transaction")
).withColumn("period", F.when(F.col("is_weekend"), "Weekend").otherwise("Weekday")) \
 .select("period", "total_events", "unique_users", "total_revenue", "avg_transaction")

display(weekend_comparison)

print("\n✅ All Gold aggregations complete!")
print("\n✅ Task 5: Gold Aggregate - COMPLETED")
print("\n" + "=" * 60)
print("🎉 ENTIRE PIPELINE EXECUTION SUCCESSFUL!")
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,⚙️ Section 7: Error Handling & Retry
# MAGIC %md
# MAGIC # ⚙️ SECTION 7 — Error Handling & Retry Logic
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine you're trying to build a tall tower:
# MAGIC * If a block falls, you **pick it up and try again** (retry)
# MAGIC * If it keeps falling, you **tell someone** (alert)
# MAGIC * You **don't start over from the beginning** — just fix the broken part!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### Why Error Handling Matters:
# MAGIC
# MAGIC * **Transient Failures:** Network issues, resource unavailability (temporary)
# MAGIC * **Data Issues:** Corrupted files, schema mismatches
# MAGIC * **Resource Constraints:** Memory, CPU, storage limits
# MAGIC * **External Dependencies:** API rate limits, service outages
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Error Handling Strategies:
# MAGIC
# MAGIC #### 1️⃣ **Automatic Retry**
# MAGIC
# MAGIC **Configuration:**
# MAGIC ```json
# MAGIC {
# MAGIC   "task_key": "bronze_ingest",
# MAGIC   "max_retries": 3,
# MAGIC   "min_retry_interval_millis": 60000,
# MAGIC   "retry_on_timeout": true
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Use Cases:**
# MAGIC * Transient network failures
# MAGIC * Temporary resource unavailability
# MAGIC * API rate limiting
# MAGIC
# MAGIC **Best Practices:**
# MAGIC * Use **exponential backoff** (wait longer between retries)
# MAGIC * Set **max retries** (avoid infinite loops)
# MAGIC * Retry **only retriable errors** (don't retry data validation failures)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2️⃣ **Task Timeout**
# MAGIC
# MAGIC **Configuration:**
# MAGIC ```json
# MAGIC {
# MAGIC   "task_key": "transform",
# MAGIC   "timeout_seconds": 3600
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Prevents tasks from running indefinitely.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 3️⃣ **Failure Notifications**
# MAGIC
# MAGIC **Configuration:**
# MAGIC ```json
# MAGIC {
# MAGIC   "email_notifications": {
# MAGIC     "on_failure": ["data-team@company.com"],
# MAGIC     "on_success": ["data-team@company.com"]
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 4️⃣ **Dead Letter Queue (DLQ)**
# MAGIC
# MAGIC Store failed records separately for investigation:
# MAGIC
# MAGIC ```python
# MAGIC try:
# MAGIC     # Process record
# MAGIC     process_record(record)
# MAGIC except Exception as e:
# MAGIC     # Write to dead letter table
# MAGIC     write_to_dlq(record, error=str(e))
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Retry Decision Tree:
# MAGIC
# MAGIC ```
# MAGIC        Task Failed?
# MAGIC             │
# MAGIC        Yes  │  No
# MAGIC       ┌────┼────┐
# MAGIC       │         │
# MAGIC   Retriable?  Success!
# MAGIC       │
# MAGIC  Yes  │  No
# MAGIC ┌───┼───┐
# MAGIC │       │
# MAGIC Retry  Alert &
# MAGIC        Fail
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example: Retry Logic in Code
# MAGIC
# MAGIC ```python
# MAGIC from tenacity import retry, stop_after_attempt, wait_exponential
# MAGIC
# MAGIC @retry(
# MAGIC     stop=stop_after_attempt(3),
# MAGIC     wait=wait_exponential(multiplier=1, min=4, max=10)
# MAGIC )
# MAGIC def fetch_data_with_retry(url):
# MAGIC     response = requests.get(url)
# MAGIC     response.raise_for_status()
# MAGIC     return response.json()
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Best Practices:
# MAGIC
# MAGIC 1. **Idempotency** — Ensure tasks can be safely retried without side effects
# MAGIC 2. **Checkpointing** — Save progress to resume from last successful point
# MAGIC 3. **Circuit Breaker** — Stop retrying if a service is consistently down
# MAGIC 4. **Monitoring** — Track retry rates to identify systemic issues
# MAGIC 5. **Alerting** — Notify on-call teams for critical failures

# COMMAND ----------

# DBTITLE 1,⌛ Section 4: Task Dependencies
# MAGIC %md
# MAGIC # ⌛ SECTION 4 — Task Dependencies
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine building with LEGO blocks:
# MAGIC * **Sequential:** Build the base first, then add walls, then add the roof (one after another)
# MAGIC * **Parallel:** You and your friend can build two different walls at the same time!
# MAGIC
# MAGIC Same with tasks — some must wait, others can run together!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### What are Task Dependencies?
# MAGIC
# MAGIC **Dependencies** define the execution order of tasks in a pipeline.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Dependency Types:
# MAGIC
# MAGIC #### 1️⃣ **Sequential Dependencies** (Linear)
# MAGIC
# MAGIC Tasks execute **one after another** in a strict order.
# MAGIC
# MAGIC ```
# MAGIC Task A → Task B → Task C
# MAGIC ```
# MAGIC
# MAGIC **Use Case:** When each task depends on the output of the previous one.
# MAGIC
# MAGIC **Example:**
# MAGIC ```
# MAGIC Ingest Raw Data → Transform Data → Load to Warehouse
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2️⃣ **Parallel Dependencies** (Fan-out)
# MAGIC
# MAGIC Multiple tasks execute **simultaneously** after a parent task completes.
# MAGIC
# MAGIC ```
# MAGIC        Task A
# MAGIC        /    \
# MAGIC   Task B    Task C
# MAGIC        \    /
# MAGIC        Task D
# MAGIC ```
# MAGIC
# MAGIC **Use Case:** Independent transformations on the same source data.
# MAGIC
# MAGIC **Example:**
# MAGIC ```
# MAGIC          Ingest Data
# MAGIC          /          \
# MAGIC Aggregate Sales   Calculate Metrics
# MAGIC          \          /
# MAGIC       Update Dashboard
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Dependency Patterns:
# MAGIC
# MAGIC | Pattern | Structure | Use Case |
# MAGIC |---------|-----------|----------|
# MAGIC | **Linear** | A → B → C | Simple ETL |
# MAGIC | **Fan-out** | A → [B, C, D] | Parallel processing |
# MAGIC | **Fan-in** | [A, B, C] → D | Aggregating results |
# MAGIC | **Diamond** | A → [B, C] → D | Split-process-merge |
# MAGIC | **Complex** | Multi-level DAG | Enterprise pipelines |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Execution Timeline Comparison:
# MAGIC
# MAGIC **Sequential Execution:**
# MAGIC ```
# MAGIC Time: 0s    5s    10s   15s
# MAGIC       |-----|-----|-----|
# MAGIC Task: [ A ] [ B ] [ C ]
# MAGIC Total: 15 seconds
# MAGIC ```
# MAGIC
# MAGIC **Parallel Execution:**
# MAGIC ```
# MAGIC Time: 0s    5s
# MAGIC       |-----|
# MAGIC Task: [ A ]
# MAGIC         [ B ]
# MAGIC         [ C ]
# MAGIC Total: 5 seconds
# MAGIC ```
# MAGIC
# MAGIC 🚀 **3x faster with parallel execution!**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Defining Dependencies in Databricks Jobs:
# MAGIC
# MAGIC **Job Configuration (JSON):**
# MAGIC ```json
# MAGIC {
# MAGIC   "tasks": [
# MAGIC     {
# MAGIC       "task_key": "ingest",
# MAGIC       "notebook_task": {"notebook_path": "/ingest"}
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "transform",
# MAGIC       "depends_on": [{"task_key": "ingest"}],
# MAGIC       "notebook_task": {"notebook_path": "/transform"}
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "aggregate",
# MAGIC       "depends_on": [{"task_key": "transform"}],
# MAGIC       "notebook_task": {"notebook_path": "/aggregate"}
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Best Practices:
# MAGIC
# MAGIC 1. **Minimize Sequential Steps** — Enable parallelism where possible
# MAGIC 2. **Avoid Over-parallelization** — Too many parallel tasks can overwhelm resources
# MAGIC 3. **Use Clear Task Names** — `bronze_ingest` not `task1`
# MAGIC 4. **Define Explicit Dependencies** — Don't rely on implicit ordering
# MAGIC 5. **Keep Dependencies Simple** — Complex DAGs are hard to debug

# COMMAND ----------

# DBTITLE 1,🎯 Section 5: Multi-task Pipeline Design
# MAGIC %md
# MAGIC # 🎯 SECTION 5 — Multi-task Pipeline Design
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Medallion Architecture Pipeline
# MAGIC
# MAGIC Let's design a complete **Bronze → Silver → Gold** pipeline with DAG dependencies.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pipeline Overview:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────┐
# MAGIC │  BRONZE LAYER (Raw Ingestion)     │
# MAGIC └─────────────┬───────────────────┘
# MAGIC              │
# MAGIC     ┌────────┼────────┐
# MAGIC     │                │
# MAGIC ┌───▼────────┐  ┌─────▼───────┐
# MAGIC │ Task 1:      │  │ Task 2:       │
# MAGIC │ Bronze Ingest│  │ Bronze Validate│
# MAGIC └─────────────┘  └─────┬───────┘
# MAGIC                        │
# MAGIC           ┌────────────┼────────────┐
# MAGIC           │                            │
# MAGIC   ┌───────▼────────┐       ┌──────▼────────┐
# MAGIC   │  SILVER LAYER   │       │  SILVER LAYER   │
# MAGIC   │  (Clean Data)   │       │ (Enrich Data)  │
# MAGIC   │                 │       │                 │
# MAGIC   │ Task 3:         │       │ Task 4:         │
# MAGIC   │ Silver Clean    │       │ Silver Enrich   │
# MAGIC   └────────┬────────┘       └────────┬────────┘
# MAGIC           │                            │
# MAGIC           └────────────┬────────────┘
# MAGIC                        │
# MAGIC              ┌─────────▼──────────┐
# MAGIC              │   GOLD LAYER        │
# MAGIC              │ (Business Metrics) │
# MAGIC              │                     │
# MAGIC              │ Task 5:             │
# MAGIC              │ Gold Aggregate      │
# MAGIC              └────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Task Breakdown:
# MAGIC
# MAGIC #### **Task 1: Bronze Ingestion**
# MAGIC * **Purpose:** Ingest raw data from source
# MAGIC * **Input:** S3/ADLS/Cloud Storage
# MAGIC * **Output:** `bronze.raw_events`
# MAGIC * **Dependencies:** None (root task)
# MAGIC * **Runtime:** ~5 min
# MAGIC
# MAGIC #### **Task 2: Bronze Validation**
# MAGIC * **Purpose:** Validate schema and data quality
# MAGIC * **Input:** `bronze.raw_events`
# MAGIC * **Output:** Validation report
# MAGIC * **Dependencies:** Task 1
# MAGIC * **Runtime:** ~2 min
# MAGIC
# MAGIC #### **Task 3: Silver Clean**
# MAGIC * **Purpose:** Deduplicate, standardize, clean
# MAGIC * **Input:** `bronze.raw_events`
# MAGIC * **Output:** `silver.clean_events`
# MAGIC * **Dependencies:** Task 2
# MAGIC * **Runtime:** ~8 min
# MAGIC
# MAGIC #### **Task 4: Silver Enrich**
# MAGIC * **Purpose:** Join with reference data
# MAGIC * **Input:** `silver.clean_events`
# MAGIC * **Output:** `silver.enriched_events`
# MAGIC * **Dependencies:** Task 2
# MAGIC * **Runtime:** ~8 min
# MAGIC
# MAGIC **🚀 Note:** Tasks 3 and 4 can run in **parallel**!
# MAGIC
# MAGIC #### **Task 5: Gold Aggregate**
# MAGIC * **Purpose:** Business-level aggregations
# MAGIC * **Input:** `silver.clean_events`, `silver.enriched_events`
# MAGIC * **Output:** `gold.daily_metrics`
# MAGIC * **Dependencies:** Tasks 3 & 4
# MAGIC * **Runtime:** ~6 min
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Total Pipeline Runtime:
# MAGIC
# MAGIC **Sequential Execution:**  
# MAGIC 5 + 2 + 8 + 8 + 6 = **29 minutes**
# MAGIC
# MAGIC **With Parallelism:**  
# MAGIC 5 + 2 + max(8, 8) + 6 = **21 minutes**
# MAGIC
# MAGIC 🚀 **28% faster!**

# COMMAND ----------

# DBTITLE 1,💻 Section 6: Hands-on Workflow Simulation
# MAGIC %md
# MAGIC # 💻 SECTION 6 — Hands-on Workflow Simulation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Simulating Multi-task Pipeline
# MAGIC
# MAGIC Since we can't create actual Databricks Jobs in a notebook, we'll **simulate** the workflow with modular code representing each task.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pipeline Simulation Approach:
# MAGIC
# MAGIC 1. **Task 1:** Bronze Ingestion (Create sample data)
# MAGIC 2. **Task 2:** Bronze Validation (Check schema)
# MAGIC 3. **Tasks 3 & 4:** Silver Clean & Enrich (Parallel processing)
# MAGIC 4. **Task 5:** Gold Aggregate (Final metrics)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Note:
# MAGIC In production, each task would be a **separate notebook** or **Python script** orchestrated by a Databricks Job.

# COMMAND ----------

# DBTITLE 1,📚 Section 1: What is Orchestration?
# MAGIC %md
# MAGIC # 📚 SECTION 1 — What is Orchestration?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine you're baking a cake:
# MAGIC 1. **First**, you mix the ingredients (flour, eggs, sugar)
# MAGIC 2. **Then**, you bake it in the oven
# MAGIC 3. **Finally**, you decorate it with frosting
# MAGIC
# MAGIC You **can't decorate before baking**, and you **can't bake before mixing**!
# MAGIC
# MAGIC **Orchestration** is like having a recipe that tells you:
# MAGIC * What to do
# MAGIC * In what order
# MAGIC * What depends on what
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Orchestration** is the automated coordination and management of multiple tasks, services, or processes to achieve a desired outcome.
# MAGIC
# MAGIC In data engineering:
# MAGIC * **Tasks** = Individual data processing steps (ingestion, transformation, aggregation)
# MAGIC * **Dependencies** = The order in which tasks must execute
# MAGIC * **Workflow** = The complete end-to-end pipeline
# MAGIC
# MAGIC ### Why is Orchestration Needed?
# MAGIC
# MAGIC | Challenge | Solution with Orchestration |
# MAGIC |-----------|----------------------------|
# MAGIC | Manual execution is error-prone | Automated task scheduling |
# MAGIC | Complex dependencies | DAG-based dependency management |
# MAGIC | No visibility into failures | Centralized monitoring & alerts |
# MAGIC | Scalability issues | Parallel execution where possible |
# MAGIC | No retry mechanism | Automated retry logic |
# MAGIC
# MAGIC ### Key Benefits:
# MAGIC 1. **Automation** — No manual intervention
# MAGIC 2. **Reliability** — Retry failed tasks
# MAGIC 3. **Visibility** — Track progress in real-time
# MAGIC 4. **Scalability** — Handle complex pipelines
# MAGIC 5. **Maintainability** — Modular design
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Real-World Example:
# MAGIC
# MAGIC **E-commerce Data Pipeline:**
# MAGIC ```
# MAGIC Task 1: Ingest raw orders from S3
# MAGIC          ↓
# MAGIC Task 2: Clean & transform orders
# MAGIC          ↓
# MAGIC Task 3: Join with customer data
# MAGIC          ↓
# MAGIC Task 4: Aggregate daily metrics
# MAGIC          ↓
# MAGIC Task 5: Update dashboard tables
# MAGIC ```
# MAGIC
# MAGIC Each task **depends** on the previous one completing successfully.

# COMMAND ----------

# DBTITLE 1,🛠️ Section 2: Jobs & Multi-task Pipelines
# MAGIC %md
# MAGIC # 🛠️ SECTION 2 — Databricks Jobs & Multi-task Pipelines
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC A **Job** is like a **to-do list** for your computer:
# MAGIC * Task 1: Download data
# MAGIC * Task 2: Clean data
# MAGIC * Task 3: Save results
# MAGIC
# MAGIC A **Multi-task Pipeline** means multiple tasks run in a specific order — like following a recipe step-by-step!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### What is a Databricks Job?
# MAGIC
# MAGIC A **Databricks Job** is a scheduled, automated execution of one or more tasks (notebooks, JARs, Python scripts, pipelines).
# MAGIC
# MAGIC ### Job Components:
# MAGIC
# MAGIC | Component | Description |
# MAGIC |-----------|-------------|
# MAGIC | **Task** | A single unit of work (e.g., notebook, Python script) |
# MAGIC | **Cluster** | Compute resource (serverless or job cluster) |
# MAGIC | **Schedule** | When to run (cron expression or trigger) |
# MAGIC | **Dependencies** | Which tasks depend on others |
# MAGIC | **Parameters** | Input values passed to tasks |
# MAGIC | **Notifications** | Alerts on success/failure |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Multi-task Pipeline Architecture:
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────┐
# MAGIC │   JOB: ETL Pipeline   │
# MAGIC └────────────────────┘
# MAGIC          │
# MAGIC     ┌────┼────┐
# MAGIC     │         │
# MAGIC ┌───▼───┐ ┌──▼───┐
# MAGIC │ Task 1 │ │ Task 2│
# MAGIC │Ingest │ │Clean  │
# MAGIC └───────┘ └──┬───┘
# MAGIC               │
# MAGIC           ┌───▼───┐
# MAGIC           │ Task 3 │
# MAGIC           │Aggregate
# MAGIC           └───────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example: Medallion Architecture Pipeline
# MAGIC
# MAGIC **Job Name:** `medallion_etl_pipeline`
# MAGIC
# MAGIC **Tasks:**
# MAGIC 1. **Task: bronze_ingestion**
# MAGIC    * Notebook: `bronze_ingest.py`
# MAGIC    * Action: Read raw data from S3/ADLS
# MAGIC    * Output: `bronze.raw_events`
# MAGIC
# MAGIC 2. **Task: silver_transformation** (depends on Task 1)
# MAGIC    * Notebook: `silver_transform.py`
# MAGIC    * Action: Clean, deduplicate, validate
# MAGIC    * Output: `silver.clean_events`
# MAGIC
# MAGIC 3. **Task: gold_aggregation** (depends on Task 2)
# MAGIC    * Notebook: `gold_aggregate.py`
# MAGIC    * Action: Business-level aggregations
# MAGIC    * Output: `gold.daily_metrics`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Benefits of Multi-task Pipelines:
# MAGIC
# MAGIC * **Modularity** — Each task is independent and reusable
# MAGIC * **Parallel Execution** — Run independent tasks simultaneously
# MAGIC * **Fault Isolation** — One task failure doesn't break the entire pipeline
# MAGIC * **Clear Ownership** — Different teams can own different tasks
# MAGIC * **Easy Debugging** — Pinpoint failures to specific tasks

# COMMAND ----------

# DBTITLE 1,🕸️ Section 3: DAG (Directed Acyclic Graph)
# MAGIC %md
# MAGIC # 🕸️ SECTION 3 — DAG (Directed Acyclic Graph)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine a **flowchart** with arrows:
# MAGIC * ● Circles = Tasks
# MAGIC * → Arrows = "Do this next"
# MAGIC
# MAGIC **Rules:**
# MAGIC * Arrows only go **forward** (never backward)
# MAGIC * No **loops** (you can't go in circles)
# MAGIC * Each task runs **after** its parent finishes
# MAGIC
# MAGIC This is a **DAG**!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### What is a DAG?
# MAGIC
# MAGIC A **DAG (Directed Acyclic Graph)** is a mathematical structure where:
# MAGIC * **Nodes** = Tasks
# MAGIC * **Edges** = Dependencies (directional arrows)
# MAGIC * **Directed** = Edges have a direction (A → B)
# MAGIC * **Acyclic** = No cycles (no task depends on itself)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### DAG Terminology:
# MAGIC
# MAGIC | Term | Definition | Example |
# MAGIC |------|------------|----------|
# MAGIC | **Node** | A task in the pipeline | `ingest_data` |
# MAGIC | **Edge** | Dependency between tasks | `ingest_data → transform_data` |
# MAGIC | **Root Node** | Task with no dependencies | `ingest_data` (starts first) |
# MAGIC | **Leaf Node** | Task with no downstream tasks | `send_email` (ends last) |
# MAGIC | **Path** | Sequence of tasks | `A → B → C` |
# MAGIC | **Depth** | Number of levels in the DAG | 3 levels |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example DAG Structure:
# MAGIC
# MAGIC ```
# MAGIC          ┌────────────────┐
# MAGIC          │   Task A:      │
# MAGIC          │ Ingest Data   │  <-- Root Node
# MAGIC          └───────┬───────┘
# MAGIC                 │
# MAGIC        ┌────────┼───────┐
# MAGIC        │                │
# MAGIC    ┌───▼────┐      ┌────▼───┐
# MAGIC    │ Task B: │      │ Task C: │
# MAGIC    │Transform│      │ Validate│
# MAGIC    └───┬────┘      └───┬────┘
# MAGIC        │                │
# MAGIC        └────────┬───────┘
# MAGIC                 │
# MAGIC          ┌──────▼──────┐
# MAGIC          │   Task D:     │
# MAGIC          │  Aggregate   │  <-- Leaf Node
# MAGIC          └─────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Execution Flow:**
# MAGIC 1. Task A runs first (no dependencies)
# MAGIC 2. Tasks B and C run **in parallel** (both depend only on A)
# MAGIC 3. Task D runs last (depends on both B and C)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Why "Acyclic" Matters:
# MAGIC
# MAGIC ❌ **BAD (Cyclic):**
# MAGIC ```
# MAGIC Task A → Task B → Task C → Task A  (infinite loop!)
# MAGIC ```
# MAGIC
# MAGIC ✅ **GOOD (Acyclic):**
# MAGIC ```
# MAGIC Task A → Task B → Task C  (clear end)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### DAG Advantages:
# MAGIC
# MAGIC 1. **Clear Dependencies** — Visual representation of workflow
# MAGIC 2. **Parallel Execution** — Independent tasks run simultaneously
# MAGIC 3. **Failure Handling** — Retry only failed tasks
# MAGIC 4. **Optimization** — Identify bottlenecks
# MAGIC 5. **Predictability** — No infinite loops