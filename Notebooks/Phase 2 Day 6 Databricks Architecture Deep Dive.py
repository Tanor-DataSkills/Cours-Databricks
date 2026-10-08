# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ⚙️ Data Engineering Training — Phase 2 Day 6  
# MAGIC ## 🏗️ Databricks Architecture Deep Dive  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - Control Plane vs Data Plane  
# MAGIC - Databricks Workspace Components  
# MAGIC - Serverless Architecture Overview  
# MAGIC - Governance with Unity Catalog  
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
# MAGIC Understand Databricks architecture, how control plane and data plane operate, and how workspace components enable scalable data engineering.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Engineering Constraints:
# MAGIC ✅ Use Databricks Serverless Compute (no cluster configs)  
# MAGIC ❌ DO NOT use RDDs  
# MAGIC ❌ DO NOT use cache() / persist()  
# MAGIC ❌ DO NOT use /tmp or local storage  
# MAGIC ✅ Use Unity Catalog Volumes for all data access  
# MAGIC ✅ Follow governance-first design

# COMMAND ----------

# DBTITLE 1,Section 1: Databricks Architecture Overview
# MAGIC %md
# MAGIC # 📚 SECTION 1: Databricks Architecture Overview
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What is Databricks Lakehouse Platform?
# MAGIC
# MAGIC ### 🧒 ELI5 Explanation:
# MAGIC Think of Databricks like a **smart kitchen**:
# MAGIC - The **recipe book** (Control Plane) tells you what to cook
# MAGIC - The **kitchen equipment** (Data Plane) does the actual cooking
# MAGIC - The **pantry** (Storage) holds all your ingredients
# MAGIC
# MAGIC You don't need to own the kitchen — Databricks manages it for you!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC Databricks is a **unified lakehouse platform** that combines:
# MAGIC - **Data Warehouse** capabilities (structured analytics)
# MAGIC - **Data Lake** capabilities (raw, unstructured storage)
# MAGIC - **Apache Spark** processing engine
# MAGIC - **Delta Lake** ACID transactions
# MAGIC
# MAGIC **Key Architectural Principle:**
# MAGIC > **Separation of Compute and Storage**
# MAGIC
# MAGIC This means:
# MAGIC - Storage is independent (S3, ADLS, GCS)
# MAGIC - Compute scales independently (Serverless, Clusters)
# MAGIC - You pay only for what you use
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Conceptual Flow:
# MAGIC
# MAGIC ```
# MAGIC User → Workspace UI → Control Plane → Data Plane → Cloud Storage (Delta Lake)
# MAGIC ```
# MAGIC
# MAGIC **Flow Explanation:**
# MAGIC 1. **User** writes code in Notebook
# MAGIC 2. **Workspace** sends instructions to Control Plane
# MAGIC 3. **Control Plane** orchestrates and schedules execution
# MAGIC 4. **Data Plane** executes Spark jobs
# MAGIC 5. **Storage** persists data in Delta format
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Benefits:
# MAGIC ✅ **Scalability** — Auto-scale compute independently
# MAGIC ✅ **Performance** — Optimized Spark runtime
# MAGIC ✅ **Governance** — Unity Catalog for centralized security
# MAGIC ✅ **Collaboration** — Shared workspace for teams
# MAGIC ✅ **Cost Optimization** — Pay-per-use serverless model

# COMMAND ----------

# DBTITLE 1,Section 2: Control Plane vs Data Plane
# MAGIC %md
# MAGIC # 🔄 SECTION 2: Control Plane vs Data Plane
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🖥️ PART A — Control Plane (Brain)
# MAGIC
# MAGIC ### What is Control Plane?
# MAGIC
# MAGIC The **Control Plane** is the **management and orchestration layer** of Databricks.
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC The control plane is like a **project manager**:
# MAGIC - Tells workers what to do
# MAGIC - Schedules tasks
# MAGIC - Keeps track of progress
# MAGIC - Manages resources
# MAGIC
# MAGIC **BUT** — it doesn't do the actual work!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Responsibilities:
# MAGIC
# MAGIC | Responsibility | Description |
# MAGIC |---|---|
# MAGIC | 📓 **Notebook Management** | Store, version, and manage notebooks |
# MAGIC | 📅 **Job Orchestration** | Schedule and monitor job runs |
# MAGIC | ⚙️ **Compute Management** | Provision serverless compute or clusters |
# MAGIC | 🗃️ **Metadata Storage** | Store table metadata, permissions, lineage |
# MAGIC | 🔐 **Security & Governance** | Manage authentication, authorization via Unity Catalog |
# MAGIC | 📊 **Monitoring & Logging** | Track job execution, metrics, audit logs |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Location:
# MAGIC - **Managed by Databricks** in Databricks cloud account
# MAGIC - **Region-specific** but separate from your data
# MAGIC - Accessible via **Workspace UI**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚡ PART B — Data Plane (Muscle)
# MAGIC
# MAGIC ### What is Data Plane?
# MAGIC
# MAGIC The **Data Plane** is where **actual data processing happens**.
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC The data plane is like the **construction workers**:
# MAGIC - They do the heavy lifting
# MAGIC - Read data from storage
# MAGIC - Transform and process data
# MAGIC - Write results back
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Responsibilities:
# MAGIC
# MAGIC | Responsibility | Description |
# MAGIC |---|---|
# MAGIC | 💥 **Spark Execution** | Run Spark jobs for data processing |
# MAGIC | 💾 **Data Access** | Read/Write from S3, ADLS, GCS |
# MAGIC | 🧠 **In-Memory Processing** | Cache intermediate results in RAM |
# MAGIC | 🔄 **Shuffle Operations** | Handle joins, aggregations, sorts |
# MAGIC | 📦 **Compute Resources** | Manage VMs, containers for execution |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Location:
# MAGIC - **Runs in your cloud account** (AWS, Azure, GCP)
# MAGIC - Directly accesses **your storage**
# MAGIC - Can be **Serverless** (managed by Databricks) or **Customer-managed clusters**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎭 Comparison Table
# MAGIC
# MAGIC | Feature | Control Plane | Data Plane |
# MAGIC |---------|---------------|------------|
# MAGIC | **Purpose** | Management & Orchestration | Data Processing & Execution |
# MAGIC | **Location** | Databricks Cloud Account | Customer Cloud Account |
# MAGIC | **Responsibilities** | Schedule jobs, manage metadata | Execute Spark, access storage |
# MAGIC | **Components** | Workspace UI, Job Scheduler | Spark Runtime, Compute VMs |
# MAGIC | **Manages** | What to do, when to do | How to do, actual execution |
# MAGIC | **Data Access** | Metadata only | Full data access |
# MAGIC | **Scaling** | Fixed (managed by Databricks) | Auto-scales with workload |
# MAGIC | **Cost** | Included in platform | Based on compute usage |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔒 Security Boundary
# MAGIC
# MAGIC **Key Point:**
# MAGIC ✅ Your data **never leaves** your cloud account
# MAGIC ✅ Control plane only sees **metadata**
# MAGIC ✅ Data plane runs **in your VPC** with your security rules
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Architecture Diagram:
# MAGIC
# MAGIC ```
# MAGIC Control Plane (Databricks Managed)
# MAGIC   - Workspace UI
# MAGIC   - Job Scheduler
# MAGIC   - Metadata Store
# MAGIC       |
# MAGIC       | Secure API Calls
# MAGIC       v
# MAGIC Data Plane (Customer Cloud Account)
# MAGIC   - Spark Clusters
# MAGIC   - Compute VMs
# MAGIC   - Your Data (S3/ADLS/GCS)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 3: Workspace Components
# MAGIC %md
# MAGIC # 🗂️ SECTION 3: Databricks Workspace Components
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What is a Workspace?
# MAGIC
# MAGIC A **Workspace** is your **collaborative development environment** in Databricks.
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC Think of it as your **team's project folder** where everyone can:
# MAGIC - Write code together
# MAGIC - Share notebooks
# MAGIC - Run data pipelines
# MAGIC - View dashboards
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Components:
# MAGIC
# MAGIC ### 1️⃣ 📓 **Notebooks**
# MAGIC
# MAGIC **Purpose:** Interactive development environment for writing code
# MAGIC
# MAGIC **Features:**
# MAGIC * Multi-language support (Python, SQL, Scala, R)
# MAGIC * Real-time collaboration
# MAGIC * Visualizations and charts
# MAGIC * Version control integration
# MAGIC
# MAGIC **Architecture Mapping:**
# MAGIC * Notebooks are stored in **Control Plane**
# MAGIC * Code execution happens in **Data Plane**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ 📅 **Jobs (Workflows)**
# MAGIC
# MAGIC **Purpose:** Schedule and orchestrate data pipelines
# MAGIC
# MAGIC **Features:**
# MAGIC * Task orchestration (multi-step workflows)
# MAGIC * Dependency management
# MAGIC * Retry logic and error handling
# MAGIC * Email/webhook notifications
# MAGIC
# MAGIC **Architecture Mapping:**
# MAGIC * Job definitions stored in **Control Plane**
# MAGIC * Job execution runs in **Data Plane**
# MAGIC
# MAGIC **Example Use Case:**
# MAGIC ```
# MAGIC Job: Daily ETL Pipeline
# MAGIC   Task 1: Ingest raw data
# MAGIC   Task 2: Transform and clean
# MAGIC   Task 3: Load to Delta tables
# MAGIC   Task 4: Run data quality checks
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ 💾 **Repos (Git Integration)**
# MAGIC
# MAGIC **Purpose:** Version control for notebooks and code
# MAGIC
# MAGIC **Features:**
# MAGIC * Connect to GitHub, GitLab, Bitbucket, Azure DevOps
# MAGIC * Branch management
# MAGIC * Pull/push changes
# MAGIC * CI/CD integration
# MAGIC
# MAGIC **Best Practice:**
# MAGIC ✅ Always use Repos for production code
# MAGIC ✅ Develop in feature branches
# MAGIC ✅ Use pull requests for code review
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4️⃣ 📊 **SQL Warehouses**
# MAGIC
# MAGIC **Purpose:** Serverless SQL analytics engine
# MAGIC
# MAGIC **Features:**
# MAGIC * BI tool connectivity (Tableau, Power BI)
# MAGIC * Query federation across catalogs
# MAGIC * Built-in query optimizer
# MAGIC * Automatic scaling
# MAGIC
# MAGIC **Architecture Mapping:**
# MAGIC * Warehouse configuration in **Control Plane**
# MAGIC * Query execution in **Data Plane** (serverless)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5️⃣ 📊 **Dashboards**
# MAGIC
# MAGIC **Purpose:** Visual analytics and reporting
# MAGIC
# MAGIC **Features:**
# MAGIC * Drag-and-drop interface
# MAGIC * Parameterized queries
# MAGIC * Auto-refresh schedules
# MAGIC * Share with stakeholders
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6️⃣ 🔄 **Delta Live Tables (Pipelines)**
# MAGIC
# MAGIC **Purpose:** Declarative ETL framework
# MAGIC
# MAGIC **Features:**
# MAGIC * Auto-scaling pipelines
# MAGIC * Data quality expectations
# MAGIC * Lineage tracking
# MAGIC * Incremental processing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 7️⃣ 🔒 **Unity Catalog**
# MAGIC
# MAGIC **Purpose:** Unified governance for data and AI
# MAGIC
# MAGIC **Features:**
# MAGIC * Centralized metadata
# MAGIC * Fine-grained access control
# MAGIC * Data lineage
# MAGIC * Audit logging
# MAGIC
# MAGIC **Structure:**
# MAGIC ```
# MAGIC Catalog
# MAGIC   └─ Schema
# MAGIC       └─ Tables
# MAGIC       └─ Views
# MAGIC       └─ Functions
# MAGIC       └─ Volumes (file storage)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## How Components Work Together:
# MAGIC
# MAGIC ```
# MAGIC Developer writes code in NOTEBOOK
# MAGIC         ↓
# MAGIC Code saved in REPOS (Git)
# MAGIC         ↓
# MAGIC Scheduled as JOB
# MAGIC         ↓
# MAGIC Reads data via UNITY CATALOG
# MAGIC         ↓
# MAGIC Processes using SERVERLESS COMPUTE
# MAGIC         ↓
# MAGIC Writes to DELTA TABLES
# MAGIC         ↓
# MAGIC Queried by SQL WAREHOUSE
# MAGIC         ↓
# MAGIC Visualized in DASHBOARDS
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Component Mapping to Architecture:
# MAGIC
# MAGIC | Component | Control Plane | Data Plane |
# MAGIC |-----------|---------------|------------|
# MAGIC | Notebooks (storage) | ✅ | ❌ |
# MAGIC | Notebooks (execution) | ❌ | ✅ |
# MAGIC | Jobs (scheduling) | ✅ | ❌ |
# MAGIC | Jobs (execution) | ❌ | ✅ |
# MAGIC | Repos | ✅ | ❌ |
# MAGIC | SQL Warehouse (config) | ✅ | ❌ |
# MAGIC | SQL Warehouse (queries) | ❌ | ✅ |
# MAGIC | Unity Catalog (metadata) | ✅ | ❌ |
# MAGIC | Unity Catalog (data access) | ❌ | ✅ |

# COMMAND ----------

# DBTITLE 1,Section 4: Serverless Architecture
# MAGIC %md
# MAGIC # ☁️ SECTION 4: Serverless Architecture
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What is Serverless Compute?
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC Serverless is like **calling an Uber** instead of owning a car:
# MAGIC * No maintenance
# MAGIC * Pay only when you use it
# MAGIC * Automatically sized for your trip
# MAGIC * Someone else manages the vehicle
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Serverless Compute** is a managed execution environment where Databricks:
# MAGIC * Automatically provisions compute resources
# MAGIC * Scales up/down based on workload
# MAGIC * Eliminates cluster configuration and management
# MAGIC * Optimizes cost by shutting down when idle
# MAGIC
# MAGIC **Key Difference from Traditional Clusters:**
# MAGIC * **Traditional:** You provision a cluster, it runs 24/7 (expensive)
# MAGIC * **Serverless:** Databricks provisions compute on-demand, auto-terminates (cost-effective)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Benefits of Serverless:
# MAGIC
# MAGIC ### 1️⃣ ⚡ **Auto-Scaling**
# MAGIC * Automatically adds/removes compute based on workload
# MAGIC * No manual intervention needed
# MAGIC * Handles spiky workloads efficiently
# MAGIC
# MAGIC ### 2️⃣ 💰 **Cost Optimization**
# MAGIC * Pay only for actual compute time (per-second billing)
# MAGIC * No idle cluster costs
# MAGIC * Automatic termination when not in use
# MAGIC
# MAGIC ### 3️⃣ 🚀 **Faster Startup**
# MAGIC * Optimized cold start times
# MAGIC * Pre-warmed resource pools
# MAGIC * Instant availability
# MAGIC
# MAGIC ### 4️⃣ 🔧 **Zero Maintenance**
# MAGIC * No cluster sizing decisions
# MAGIC * No DBR version management
# MAGIC * No library installation conflicts
# MAGIC
# MAGIC ### 5️⃣ 🔒 **Enhanced Security**
# MAGIC * Isolated execution environments
# MAGIC * Automatic security patches
# MAGIC * No shared cluster vulnerabilities
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Comparison: Serverless vs Cluster-Based
# MAGIC
# MAGIC | Feature | Serverless | Cluster-Based |
# MAGIC |---------|------------|---------------|
# MAGIC | **Setup Time** | Instant (< 30 sec) | 5-10 minutes |
# MAGIC | **Management** | Fully managed | Manual configuration |
# MAGIC | **Scaling** | Automatic | Manual or autoscaling rules |
# MAGIC | **Cost Model** | Pay-per-second | Pay for uptime (even if idle) |
# MAGIC | **Idle Costs** | Zero (€0 when not running) | Full cluster cost |
# MAGIC | **Use Case** | Ad-hoc queries, notebooks, jobs | Long-running streaming, custom configs |
# MAGIC | **Library Installation** | Built-in libraries | Manual %pip install |
# MAGIC | **Cluster Sizing** | Automatic | Manual (small, medium, large) |
# MAGIC | **Cold Start** | ∼ 20-30 seconds | 5-10 minutes |
# MAGIC | **Best For** | 95% of workloads | Specialized requirements |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## When to Use Serverless?
# MAGIC
# MAGIC ✅ **Ideal for:**
# MAGIC * Interactive notebook development
# MAGIC * Ad-hoc data exploration
# MAGIC * Scheduled batch jobs
# MAGIC * SQL analytics (SQL Warehouses)
# MAGIC * Short to medium-duration workloads
# MAGIC
# MAGIC ❌ **Not ideal for:**
# MAGIC * 24/7 streaming workloads
# MAGIC * Custom network configurations (VPC peering)
# MAGIC * Specialized library requirements
# MAGIC * GPU-intensive ML training
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Serverless Architecture Flow:
# MAGIC
# MAGIC ```
# MAGIC User executes code in Notebook
# MAGIC         ↓
# MAGIC Control Plane receives request
# MAGIC         ↓
# MAGIC Serverless Compute Manager checks resource pool
# MAGIC         ↓
# MAGIC Allocates compute from warm pool (or provisions new)
# MAGIC         ↓
# MAGIC Data Plane executes Spark job
# MAGIC         ↓
# MAGIC Results returned to user
# MAGIC         ↓
# MAGIC Compute auto-terminates after idle timeout
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Why Serverless-First is Best Practice:
# MAGIC
# MAGIC 1. **Cost Efficiency:** No wasted spend on idle clusters
# MAGIC 2. **Developer Productivity:** Focus on code, not infrastructure
# MAGIC 3. **Scalability:** Automatically handles varying workloads
# MAGIC 4. **Security:** Isolated execution environments
# MAGIC 5. **Simplicity:** No cluster management overhead
# MAGIC
# MAGIC **Recommendation:**
# MAGIC > Start with serverless for 95% of workloads. Only use clusters for specialized requirements.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Serverless in Databricks:
# MAGIC
# MAGIC * **Notebooks:** Serverless by default (no cluster selection needed)
# MAGIC * **SQL Warehouses:** Fully serverless
# MAGIC * **Jobs:** Can be configured to use serverless compute
# MAGIC * **Delta Live Tables:** Serverless pipelines available

# COMMAND ----------

# DBTITLE 1,Section 5: Unity Catalog Integration
# MAGIC %md
# MAGIC # 🔒 SECTION 5: Unity Catalog Integration (Governance Layer)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What is Unity Catalog?
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC Unity Catalog is like a **library system** for your data:
# MAGIC * **Catalog** = Library building
# MAGIC * **Schema** = Floor/section
# MAGIC * **Tables** = Individual books
# MAGIC * **Librarian** = Access control (who can read what)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Unity Catalog** is Databricks' unified governance solution providing:
# MAGIC * **Centralized metadata management**
# MAGIC * **Fine-grained access control** (table, column, row-level)
# MAGIC * **Data lineage tracking**
# MAGIC * **Audit logging**
# MAGIC * **Cross-workspace governance**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Unity Catalog Structure:
# MAGIC
# MAGIC ```
# MAGIC Catalog (Top-level namespace)
# MAGIC   ├─ Schema (Database)
# MAGIC   │   ├─ Tables (Managed or External)
# MAGIC   │   ├─ Views
# MAGIC   │   ├─ Functions (UDFs)
# MAGIC   │   └─ Volumes (File storage)
# MAGIC   │
# MAGIC   └─ Schema
# MAGIC       ├─ Tables
# MAGIC       └─ Volumes
# MAGIC ```
# MAGIC
# MAGIC **Example:**
# MAGIC ```
# MAGIC enterprise_data (Catalog)
# MAGIC   ├─ sales (Schema)
# MAGIC   │   ├─ orders (Table)
# MAGIC   │   ├─ customers (Table)
# MAGIC   │   └─ raw_files (Volume)
# MAGIC   │
# MAGIC   └─ marketing (Schema)
# MAGIC       ├─ campaigns (Table)
# MAGIC       └─ landing_files (Volume)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Features:
# MAGIC
# MAGIC ### 1️⃣ **Centralized Governance**
# MAGIC * Single source of truth for metadata
# MAGIC * Consistent security policies across workspaces
# MAGIC * Unified data discovery
# MAGIC
# MAGIC ### 2️⃣ **Access Control**
# MAGIC * Grant permissions at catalog, schema, table, or column level
# MAGIC * Row-level and column-level security
# MAGIC * Attribute-based access control (ABAC)
# MAGIC
# MAGIC ### 3️⃣ **Data Lineage**
# MAGIC * Automatic tracking of data flow
# MAGIC * Column-level lineage
# MAGIC * Impact analysis
# MAGIC
# MAGIC ### 4️⃣ **Audit Logging**
# MAGIC * Track all data access
# MAGIC * Compliance reporting
# MAGIC * Security monitoring
# MAGIC
# MAGIC ### 5️⃣ **Volumes**
# MAGIC * Managed file storage (alternative to direct cloud storage)
# MAGIC * Governed file access
# MAGIC * Supports any file format
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Unity Catalog in Architecture:
# MAGIC
# MAGIC | Component | Role |
# MAGIC |-----------|------|
# MAGIC | **Control Plane** | Stores metadata (table schemas, permissions) |
# MAGIC | **Data Plane** | Enforces access control during query execution |
# MAGIC | **Storage** | Physical data location (S3/ADLS/GCS) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Benefits:
# MAGIC
# MAGIC ✅ **Single governance model** across all clouds
# MAGIC ✅ **Fine-grained security** without performance impact
# MAGIC ✅ **Automatic lineage** for compliance
# MAGIC ✅ **Data discovery** through catalog explorer
# MAGIC ✅ **Cross-workspace sharing** with Delta Sharing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Three-Level Namespace:
# MAGIC
# MAGIC **Old Hive Metastore:**
# MAGIC ```sql
# MAGIC SELECT * FROM schema.table
# MAGIC ```
# MAGIC
# MAGIC **Unity Catalog:**
# MAGIC ```sql
# MAGIC SELECT * FROM catalog.schema.table
# MAGIC ```
# MAGIC
# MAGIC **Why the extra level?**
# MAGIC * Enables multi-tenant governance
# MAGIC * Separates dev/test/prod environments
# MAGIC * Allows fine-grained access control at catalog level

# COMMAND ----------

# DBTITLE 1,Section 6: Hands-on Architecture Simulation
# MAGIC %md
# MAGIC # 👨‍💻 SECTION 6: Hands-on Architecture Simulation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Objective:
# MAGIC Simulate the complete data flow through Databricks architecture:
# MAGIC
# MAGIC ```
# MAGIC Ingestion → Processing → Storage
# MAGIC ```
# MAGIC
# MAGIC **Map each step to:**
# MAGIC * Control Plane responsibilities
# MAGIC * Data Plane responsibilities
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Workflow:
# MAGIC
# MAGIC 1. **Explore Unity Catalog structure** (Control Plane metadata)
# MAGIC 2. **Create sample data** (Data Plane processing)
# MAGIC 3. **Write to Delta table** (Data Plane + Storage)
# MAGIC 4. **Query the data** (Data Plane processing)
# MAGIC 5. **Verify lineage** (Control Plane tracking)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 1: List Available Catalogs

# COMMAND ----------

# DBTITLE 1,List Catalogs - Control Plane Metadata
# This query hits Control Plane (metadata only)
# No data processing happens here

catalogs_df = spark.sql("SHOW CATALOGS")
display(catalogs_df)

print("\n✅ Control Plane Operation: Retrieved catalog metadata")

# COMMAND ----------

# DBTITLE 1,Current Catalog and Schema
# Check current catalog and schema context
current_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
current_schema = spark.sql("SELECT current_schema()").collect()[0][0]

print(f"📌 Current Catalog: {current_catalog}")
print(f"📌 Current Schema: {current_schema}")
print(f"\n🔗 Fully Qualified Path: {current_catalog}.{current_schema}")
print("\n✅ Control Plane: Metadata lookup")

# COMMAND ----------

# DBTITLE 1,Step 2: Create Sample Data
# MAGIC %md
# MAGIC ### Step 2: Create Sample Data (Simulating Ingestion)
# MAGIC
# MAGIC We'll create sample data to simulate the **Data Plane processing**.

# COMMAND ----------

# DBTITLE 1,Generate Sample Sales Data
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format
from datetime import datetime, timedelta
import random

# Generate sample sales data
data = [
    (1, "Laptop", 1200.00, "Electronics", "2026-04-15"),
    (2, "Mouse", 25.00, "Electronics", "2026-04-16"),
    (3, "Keyboard", 75.00, "Electronics", "2026-04-17"),
    (4, "Monitor", 300.00, "Electronics", "2026-04-18"),
    (5, "Desk Chair", 250.00, "Furniture", "2026-04-19"),
    (6, "Desk", 400.00, "Furniture", "2026-04-20"),
    (7, "Notebook", 5.00, "Stationery", "2026-04-21"),
    (8, "Pen Set", 15.00, "Stationery", "2026-04-21"),
]

schema = ["product_id", "product_name", "price", "category", "sale_date"]

# Create DataFrame - This happens in Data Plane
sales_df = spark.createDataFrame(data, schema=schema)

print("✅ Data Plane: Created DataFrame in Spark memory")
print(f"\n📊 Total Records: {sales_df.count()}")
print("\n🔍 Sample Data:")
display(sales_df)

# COMMAND ----------

# DBTITLE 1,Step 3: Transform Data
# MAGIC %md
# MAGIC ### Step 3: Transform Data (Data Plane Processing)
# MAGIC
# MAGIC Apply transformations to enrich the data.

# COMMAND ----------

# DBTITLE 1,Apply Transformations
from pyspark.sql.functions import when, round as spark_round, upper

# Transform data - Data Plane processing
transformed_df = sales_df \
    .withColumn("price_category",
                when(col("price") < 50, "Budget")
                .when(col("price") < 200, "Mid-Range")
                .otherwise("Premium")) \
    .withColumn("discounted_price", spark_round(col("price") * 0.9, 2)) \
    .withColumn("category_upper", upper(col("category"))) \
    .withColumn("ingestion_timestamp", current_timestamp())

print("✅ Data Plane: Transformations applied in Spark")
print("\n🔄 Transformations:")
print("  * Added price_category (Budget/Mid-Range/Premium)")
print("  * Calculated discounted_price (10% off)")
print("  * Uppercase category")
print("  * Added ingestion timestamp")
print("\n🔍 Transformed Data:")
display(transformed_df)

# COMMAND ----------

# DBTITLE 1,Step 4: Write to Delta Table
# MAGIC %md
# MAGIC ### Step 4: Write to Delta Table (Data Plane + Storage)
# MAGIC
# MAGIC Persist the transformed data to Delta Lake.

# COMMAND ----------

# DBTITLE 1,Write Data to Delta Table
# Define table name with Unity Catalog three-level namespace
table_name = f"{current_catalog}.{current_schema}.architecture_demo_sales"

print(f"📦 Writing to Delta table: {table_name}")
print("\n🔄 Architecture Flow:")
print("  1. Control Plane: Registers table metadata in Unity Catalog")
print("  2. Data Plane: Writes data files to cloud storage")
print("  3. Storage: Persists Parquet files in Delta format")
print("\n⏳ Writing...\n")

# Write to Delta table
transformed_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(table_name)

print(f"✅ SUCCESS: Table created at {table_name}")
print("\n📊 Table is now:")
print("  * Registered in Unity Catalog (Control Plane)")
print("  * Stored in Delta format (Storage Layer)")
print("  * Queryable via SQL or DataFrame API (Data Plane)")

# COMMAND ----------

# DBTITLE 1,Step 5: Query the Data
# MAGIC %md
# MAGIC ### Step 5: Query the Delta Table (Data Plane Processing)
# MAGIC
# MAGIC Run analytics queries on the persisted data.

# COMMAND ----------

# DBTITLE 1,Query Using SQL
# SQL query - Data Plane processes this
query = f"""
SELECT 
    category,
    price_category,
    COUNT(*) as product_count,
    ROUND(SUM(price), 2) as total_revenue,
    ROUND(AVG(price), 2) as avg_price
FROM {table_name}
GROUP BY category, price_category
ORDER BY total_revenue DESC
"""

print("⚡ Data Plane: Executing SQL query")
print("\n🔍 Query Results:\n")

result_df = spark.sql(query)
display(result_df)

print("\n✅ Data Plane: Query executed successfully")

# COMMAND ----------

# DBTITLE 1,Query Using DataFrame API
# Read and query using DataFrame API
print(f"💾 Reading from Delta table: {table_name}")

# Read from Unity Catalog table
sales_df_read = spark.table(table_name)

# Filter premium products
premium_products = sales_df_read \
    .filter(col("price_category") == "Premium") \
    .select("product_name", "price", "discounted_price", "category") \
    .orderBy(col("price").desc())

print("\n💰 Premium Products (Price > \u20ac200):\n")
display(premium_products)

print("\n✅ Data Plane: DataFrame operations executed")

# COMMAND ----------

# DBTITLE 1,Step 6: Verify Metadata
# MAGIC %md
# MAGIC ### Step 6: Verify Metadata (Control Plane)
# MAGIC
# MAGIC Inspect table metadata stored in Unity Catalog.

# COMMAND ----------

# DBTITLE 1,Show Table Metadata
# Describe table - Control Plane metadata
print(f"📊 Table Metadata for: {table_name}\n")

table_info = spark.sql(f"DESCRIBE EXTENDED {table_name}")
display(table_info)

print("\n✅ Control Plane: Retrieved table metadata from Unity Catalog")

# COMMAND ----------

# DBTITLE 1,Show Table History - Delta Time Travel
# Show Delta table history
print(f"🕒 Delta Table History for: {table_name}\n")

history_df = spark.sql(f"DESCRIBE HISTORY {table_name}")
display(history_df)

print("\n📝 Version History:")
print("  * Delta Lake automatically tracks all versions")
print("  * Time travel enables querying historical data")
print("  * Control Plane manages version metadata")
print("\n✅ Control Plane: Version history retrieved")

# COMMAND ----------

# DBTITLE 1,Section 7: End-to-End Architecture Flow
# MAGIC %md
# MAGIC # 🔄 SECTION 7: End-to-End Architecture Flow
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Complete Data Pipeline Architecture
# MAGIC
# MAGIC ### Conceptual Pipeline:
# MAGIC
# MAGIC ```
# MAGIC Data Source → Databricks Workspace → Serverless Execution → Delta Lake → Analytics
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Detailed Flow with Architecture Mapping:
# MAGIC
# MAGIC ### 🟢 Phase 1: Development (Control Plane)
# MAGIC
# MAGIC ```
# MAGIC 1. Developer opens Workspace UI
# MAGIC    └─ Control Plane: Authenticates user
# MAGIC    └─ Control Plane: Loads notebook from storage
# MAGIC
# MAGIC 2. Developer writes code in Notebook
# MAGIC    └─ Control Plane: Saves notebook content
# MAGIC    └─ Control Plane: Version control (if using Repos)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔵 Phase 2: Execution Request (Control Plane → Data Plane)
# MAGIC
# MAGIC ```
# MAGIC 3. Developer runs cell / triggers job
# MAGIC    └─ Control Plane: Receives execution request
# MAGIC    └─ Control Plane: Checks permissions (Unity Catalog)
# MAGIC    └─ Control Plane: Allocates serverless compute
# MAGIC    └─ Control Plane: Sends job to Data Plane
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟡 Phase 3: Data Processing (Data Plane)
# MAGIC
# MAGIC ```
# MAGIC 4. Spark job executes
# MAGIC    └─ Data Plane: Provisions compute resources
# MAGIC    └─ Data Plane: Reads data from cloud storage (S3/ADLS)
# MAGIC    └─ Data Plane: Applies transformations
# MAGIC    └─ Data Plane: Performs aggregations/joins
# MAGIC    └─ Data Plane: Writes results to Delta Lake
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟣 Phase 4: Storage (Storage Layer)
# MAGIC
# MAGIC ```
# MAGIC 5. Data persisted
# MAGIC    └─ Storage: Delta Lake format (Parquet + transaction log)
# MAGIC    └─ Storage: Stored in customer cloud account
# MAGIC    └─ Control Plane: Registers metadata in Unity Catalog
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟠 Phase 5: Analytics (Data Plane + Control Plane)
# MAGIC
# MAGIC ```
# MAGIC 6. Query execution
# MAGIC    └─ Control Plane: Receives SQL query
# MAGIC    └─ Control Plane: Optimizes query plan
# MAGIC    └─ Data Plane: Executes query on Delta tables
# MAGIC    └─ Data Plane: Returns results
# MAGIC    └─ Control Plane: Displays in UI / Dashboard
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architecture Layers:
# MAGIC
# MAGIC | Layer | Components | Managed By |
# MAGIC |-------|------------|------------|
# MAGIC | **Presentation** | Workspace UI, Dashboards | Databricks (Control Plane) |
# MAGIC | **Orchestration** | Job Scheduler, Workflows | Databricks (Control Plane) |
# MAGIC | **Governance** | Unity Catalog | Databricks (Control Plane) |
# MAGIC | **Processing** | Spark Runtime, Serverless Compute | Databricks (Data Plane) |
# MAGIC | **Storage** | Delta Lake, Cloud Storage | Customer (AWS/Azure/GCP) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Example: Real-World ETL Pipeline
# MAGIC
# MAGIC ### Scenario: Daily Sales Analytics
# MAGIC
# MAGIC **Step 1: Ingestion (Data Plane)**
# MAGIC * Auto Loader reads new CSV files from S3
# MAGIC * Serverless compute processes files
# MAGIC * Raw data written to Bronze table
# MAGIC
# MAGIC **Step 2: Transformation (Data Plane)**
# MAGIC * Read Bronze table
# MAGIC * Apply data quality rules
# MAGIC * Join with dimension tables
# MAGIC * Write to Silver table
# MAGIC
# MAGIC **Step 3: Aggregation (Data Plane)**
# MAGIC * Read Silver table
# MAGIC * Compute daily aggregates
# MAGIC * Write to Gold table
# MAGIC
# MAGIC **Step 4: Scheduling (Control Plane)**
# MAGIC * Job scheduler triggers pipeline at 2 AM daily
# MAGIC * Monitors execution
# MAGIC * Sends alerts on failure
# MAGIC
# MAGIC **Step 5: Analytics (Data Plane + Control Plane)**
# MAGIC * SQL Warehouse queries Gold table
# MAGIC * Dashboard refreshes
# MAGIC * Business users view insights
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Control Plane vs Data Plane in Action:
# MAGIC
# MAGIC | Action | Control Plane | Data Plane |
# MAGIC |--------|---------------|------------|
# MAGIC | User logs in | ✅ Authenticates | ❌ |
# MAGIC | Opens notebook | ✅ Loads content | ❌ |
# MAGIC | Runs code cell | ✅ Schedules | ✅ Executes |
# MAGIC | Reads S3 data | ❌ | ✅ Reads files |
# MAGIC | Transforms data | ❌ | ✅ Processes |
# MAGIC | Writes Delta table | ✅ Registers metadata | ✅ Writes files |
# MAGIC | Queries table | ✅ Optimizes query | ✅ Executes query |
# MAGIC | Views results | ✅ Displays UI | ❌ |
# MAGIC | Checks lineage | ✅ Shows lineage | ❌ |
# MAGIC | Monitors job | ✅ Tracks status | ✅ Executes tasks |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Takeaway:
# MAGIC
# MAGIC > **Control Plane** = Management, orchestration, metadata, governance
# MAGIC > 
# MAGIC > **Data Plane** = Execution, processing, data access
# MAGIC > 
# MAGIC > **Storage** = Persistence layer (Delta Lake in your cloud)
# MAGIC
# MAGIC **They work together seamlessly but serve distinct purposes!**

# COMMAND ----------

# DBTITLE 1,Section 8: Best Practices and Common Mistakes
# MAGIC %md
# MAGIC # ✅ SECTION 8: Data Engineering Best Practices
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architectural Best Practices:
# MAGIC
# MAGIC ### 1️⃣ **Separate Control and Data Responsibilities**
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC * Let Control Plane handle orchestration, scheduling, metadata
# MAGIC * Let Data Plane handle execution, processing, data access
# MAGIC * Don't try to store data in Control Plane
# MAGIC
# MAGIC ❌ **DON'T:**
# MAGIC * Store large datasets in notebook variables
# MAGIC * Use Control Plane APIs for data processing
# MAGIC * Bypass Unity Catalog for data access
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ **Use Serverless-First Design**
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC * Default to serverless for 95% of workloads
# MAGIC * Use clusters only for specialized requirements
# MAGIC * Let Databricks manage compute resources
# MAGIC
# MAGIC ❌ **DON'T:**
# MAGIC * Create clusters unnecessarily
# MAGIC * Keep clusters running 24/7 without need
# MAGIC * Over-provision cluster resources
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ **Govern All Data Using Unity Catalog**
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC * Use three-level namespace (catalog.schema.table)
# MAGIC * Grant permissions at appropriate levels
# MAGIC * Use Volumes for file storage
# MAGIC * Track lineage automatically
# MAGIC
# MAGIC ❌ **DON'T:**
# MAGIC * Bypass Unity Catalog with direct storage access
# MAGIC * Use legacy Hive metastore for new projects
# MAGIC * Hard-code storage paths in production code
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4️⃣ **Avoid Direct Storage Access Without Governance**
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC ```python
# MAGIC # Use Unity Catalog Volumes
# MAGIC df = spark.read.parquet("/Volumes/catalog/schema/volume/data/")
# MAGIC
# MAGIC # Or use managed tables
# MAGIC df = spark.table("catalog.schema.table_name")
# MAGIC ```
# MAGIC
# MAGIC ❌ **DON'T:**
# MAGIC ```python
# MAGIC # Don't bypass Unity Catalog
# MAGIC df = spark.read.parquet("s3://bucket/data/")  # No governance!
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5️⃣ **Separate Compute and Storage**
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC * Store data in Delta Lake (durable)
# MAGIC * Use serverless compute (ephemeral)
# MAGIC * Scale compute independently from storage
# MAGIC
# MAGIC ❌ **DON'T:**
# MAGIC * Store data in cluster local storage (/tmp)
# MAGIC * Use cache() unnecessarily
# MAGIC * Couple compute lifetime with data lifetime
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6️⃣ **Use Delta Lake Format**
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC * Always write to Delta format
# MAGIC * Leverage ACID transactions
# MAGIC * Use time travel for auditing
# MAGIC * Enable change data feed for streaming
# MAGIC
# MAGIC ❌ **DON'T:**
# MAGIC * Use Parquet directly (lose ACID)
# MAGIC * Use CSV for large datasets (no schema enforcement)
# MAGIC * Skip Delta optimizations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 7️⃣ **Implement Medallion Architecture**
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC ```
# MAGIC Bronze (Raw) → Silver (Cleaned) → Gold (Aggregated)
# MAGIC ```
# MAGIC * Bronze: Ingest raw data as-is
# MAGIC * Silver: Clean, validate, deduplicate
# MAGIC * Gold: Business-level aggregates
# MAGIC
# MAGIC ❌ **DON'T:**
# MAGIC * Mix raw and processed data
# MAGIC * Skip data quality checks
# MAGIC * Create monolithic pipelines
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 8️⃣ **Use Jobs for Production Pipelines**
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC * Schedule notebooks as jobs
# MAGIC * Use task dependencies
# MAGIC * Configure retries and alerts
# MAGIC * Use serverless compute for jobs
# MAGIC
# MAGIC ❌ **DON'T:**
# MAGIC * Run production pipelines manually
# MAGIC * Use interactive clusters for scheduled workloads
# MAGIC * Skip error handling
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Common Mistakes:
# MAGIC
# MAGIC ### ❌ **Mistake 1: Confusing Control vs Data Plane**
# MAGIC
# MAGIC **Problem:**
# MAGIC * Trying to process data in Control Plane
# MAGIC * Expecting Control Plane to hold large datasets
# MAGIC
# MAGIC **Solution:**
# MAGIC * Understand the separation of concerns
# MAGIC * Use Control Plane for orchestration only
# MAGIC * Let Data Plane handle all processing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ **Mistake 2: Using Cluster-Based Approach Unnecessarily**
# MAGIC
# MAGIC **Problem:**
# MAGIC * Creating clusters for simple workloads
# MAGIC * Leaving clusters running 24/7
# MAGIC * Manual cluster sizing
# MAGIC
# MAGIC **Solution:**
# MAGIC * Default to serverless
# MAGIC * Use clusters only for specialized needs
# MAGIC * Enable auto-termination if using clusters
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ **Mistake 3: Ignoring Governance**
# MAGIC
# MAGIC **Problem:**
# MAGIC * Bypassing Unity Catalog
# MAGIC * Using direct S3/ADLS paths
# MAGIC * No access control
# MAGIC * No lineage tracking
# MAGIC
# MAGIC **Solution:**
# MAGIC * Always use Unity Catalog
# MAGIC * Use three-level namespace
# MAGIC * Grant appropriate permissions
# MAGIC * Leverage automatic lineage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ **Mistake 4: Tight Coupling of Compute and Storage**
# MAGIC
# MAGIC **Problem:**
# MAGIC * Storing data in cluster local storage
# MAGIC * Using .cache() everywhere
# MAGIC * Losing data when cluster terminates
# MAGIC
# MAGIC **Solution:**
# MAGIC * Always persist to Delta Lake
# MAGIC * Use cache() only for iterative ML
# MAGIC * Separate data lifecycle from compute
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ **Mistake 5: Not Using Delta Lake**
# MAGIC
# MAGIC **Problem:**
# MAGIC * Writing to Parquet/CSV directly
# MAGIC * No ACID transactions
# MAGIC * No time travel
# MAGIC * No schema enforcement
# MAGIC
# MAGIC **Solution:**
# MAGIC * Always use Delta format
# MAGIC * Leverage Delta features (time travel, ACID)
# MAGIC * Use MERGE for upserts
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ **Mistake 6: Poor Pipeline Design**
# MAGIC
# MAGIC **Problem:**
# MAGIC * Monolithic notebooks
# MAGIC * No separation of concerns
# MAGIC * No data quality checks
# MAGIC * No incremental processing
# MAGIC
# MAGIC **Solution:**
# MAGIC * Implement Medallion architecture
# MAGIC * Use Delta Live Tables for complex pipelines
# MAGIC * Add data quality expectations
# MAGIC * Process incrementally
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ **Mistake 7: Security Antipatterns**
# MAGIC
# MAGIC **Problem:**
# MAGIC * Hard-coded credentials
# MAGIC * Overly permissive access
# MAGIC * No audit logging
# MAGIC
# MAGIC **Solution:**
# MAGIC * Use Databricks Secrets
# MAGIC * Grant least privilege access
# MAGIC * Enable Unity Catalog audit logs
# MAGIC * Use service principals for automation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Optimization Tips:
# MAGIC
# MAGIC ### Performance:
# MAGIC 1. Use Delta Lake Z-Ordering for frequent filters
# MAGIC 2. Partition large tables appropriately
# MAGIC 3. Use broadcast joins for small dimension tables
# MAGIC 4. Enable Adaptive Query Execution (AQE)
# MAGIC 5. Use Photon engine (automatic in serverless)
# MAGIC
# MAGIC ### Cost:
# MAGIC 1. Use serverless to avoid idle costs
# MAGIC 2. Enable auto-termination on clusters
# MAGIC 3. Use spot instances for non-critical workloads
# MAGIC 4. Optimize file sizes (1GB target)
# MAGIC 5. Use incremental processing
# MAGIC
# MAGIC ### Governance:
# MAGIC 1. Use Unity Catalog for all data access
# MAGIC 2. Implement row-level and column-level security
# MAGIC 3. Enable audit logging
# MAGIC 4. Tag sensitive data (PII)
# MAGIC 5. Use data classification
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architecture Checklist:
# MAGIC
# MAGIC ☑️ Understand Control Plane vs Data Plane separation
# MAGIC ☑️ Use serverless compute by default
# MAGIC ☑️ Govern all data with Unity Catalog
# MAGIC ☑️ Use Delta Lake format
# MAGIC ☑️ Implement Medallion architecture
# MAGIC ☑️ Separate compute and storage
# MAGIC ☑️ Schedule production workloads as Jobs
# MAGIC ☑️ Enable monitoring and alerting
# MAGIC ☑️ Follow security best practices
# MAGIC ☑️ Document data lineage

# COMMAND ----------

# DBTITLE 1,Section 9: Genie Code Agent Usage
# MAGIC %md
# MAGIC # 🤖 SECTION 9: Genie Code Agent Usage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What is Genie Code?
# MAGIC
# MAGIC Genie Code is Databricks' AI assistant that helps with:
# MAGIC * Code generation
# MAGIC * Architecture guidance
# MAGIC * Query optimization
# MAGIC * Troubleshooting
# MAGIC * Best practices
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Example Prompts for Architecture Tasks:
# MAGIC
# MAGIC ### 📝 **Prompt 1: Explain Architecture**
# MAGIC ```
# MAGIC "Explain the difference between Control Plane and Data Plane in Databricks"
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC * Detailed comparison
# MAGIC * Use cases
# MAGIC * Architecture diagrams
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📝 **Prompt 2: Generate Serverless Pipeline**
# MAGIC ```
# MAGIC "Generate a serverless data pipeline that reads from Unity Catalog Volume,
# MAGIC transforms the data, and writes to a Delta table"
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC * PySpark code using serverless compute
# MAGIC * Unity Catalog integration
# MAGIC * Delta Lake write operations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📝 **Prompt 3: Map Control Plane vs Data Plane**
# MAGIC ```
# MAGIC "Map each step of my ETL pipeline to Control Plane or Data Plane operations"
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC * Step-by-step mapping
# MAGIC * Architecture explanation
# MAGIC * Optimization suggestions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📝 **Prompt 4: Design Enterprise Architecture**
# MAGIC ```
# MAGIC "Design a multi-environment Databricks architecture for my enterprise
# MAGIC with dev, test, and prod workspaces using Unity Catalog governance"
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC * Environment separation strategy
# MAGIC * Unity Catalog structure
# MAGIC * Workspace organization
# MAGIC * Security model
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📝 **Prompt 5: Optimize Existing Pipeline**
# MAGIC ```
# MAGIC "Review my pipeline code and suggest architectural improvements
# MAGIC for better performance and cost optimization"
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC * Performance bottlenecks
# MAGIC * Cost optimization tips
# MAGIC * Architecture refactoring suggestions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📝 **Prompt 6: Troubleshoot Architecture Issues**
# MAGIC ```
# MAGIC "Why is my data not appearing in Unity Catalog after writing to Delta table?"
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC * Root cause analysis
# MAGIC * Control Plane vs Data Plane diagnostics
# MAGIC * Resolution steps
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## How Genie Code Understands Architecture:
# MAGIC
# MAGIC 1. **Context Awareness**: Knows your workspace configuration
# MAGIC 2. **Governance Integration**: Suggests Unity Catalog best practices
# MAGIC 3. **Serverless-First**: Generates serverless-compliant code
# MAGIC 4. **Best Practices**: Follows Databricks architectural patterns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Tips for Effective Prompts:
# MAGIC
# MAGIC ✅ **Be specific** about your requirements
# MAGIC ✅ **Mention constraints** (serverless, Unity Catalog, etc.)
# MAGIC ✅ **Ask for explanations** when learning architecture
# MAGIC ✅ **Request best practices** for production-ready code
# MAGIC ✅ **Provide context** about your environment
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Example Workflow with Genie Code:
# MAGIC
# MAGIC 1. Ask Genie to explain architecture concepts
# MAGIC 2. Request code generation with architectural constraints
# MAGIC 3. Get architecture review and optimization suggestions
# MAGIC 4. Troubleshoot issues with architecture-aware debugging
# MAGIC 5. Learn best practices from Genie's suggestions

# COMMAND ----------

# DBTITLE 1,Section 10: Summary and Interview Questions
# MAGIC %md
# MAGIC # 🎯 SECTION 10: Summary and Key Learnings
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Learnings:
# MAGIC
# MAGIC ### 1️⃣ **Databricks Architecture**
# MAGIC * Lakehouse platform combining warehouse and lake capabilities
# MAGIC * Built on Apache Spark and Delta Lake
# MAGIC * Separation of compute and storage
# MAGIC
# MAGIC ### 2️⃣ **Control Plane vs Data Plane**
# MAGIC * **Control Plane**: Management, orchestration, metadata (Databricks cloud)
# MAGIC * **Data Plane**: Execution, processing, data access (customer cloud)
# MAGIC * Clear separation ensures security and scalability
# MAGIC
# MAGIC ### 3️⃣ **Workspace Components**
# MAGIC * Notebooks for development
# MAGIC * Jobs for orchestration
# MAGIC * SQL Warehouses for analytics
# MAGIC * Unity Catalog for governance
# MAGIC * All work together seamlessly
# MAGIC
# MAGIC ### 4️⃣ **Serverless Architecture**
# MAGIC * Auto-scaling compute
# MAGIC * Pay-per-second billing
# MAGIC * Zero maintenance
# MAGIC * Cost-effective for 95% of workloads
# MAGIC
# MAGIC ### 5️⃣ **Unity Catalog**
# MAGIC * Centralized governance
# MAGIC * Three-level namespace
# MAGIC * Fine-grained access control
# MAGIC * Automatic lineage tracking
# MAGIC
# MAGIC ### 6️⃣ **Best Practices**
# MAGIC * Serverless-first design
# MAGIC * Unity Catalog for all data access
# MAGIC * Delta Lake format
# MAGIC * Medallion architecture (Bronze/Silver/Gold)
# MAGIC * Separation of concerns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architecture Principles Summary:
# MAGIC
# MAGIC | Principle | Description |
# MAGIC |-----------|-------------|
# MAGIC | **Separation of Compute & Storage** | Scale independently, pay separately |
# MAGIC | **Serverless-First** | Default to managed compute |
# MAGIC | **Governance-First** | Use Unity Catalog for all data |
# MAGIC | **Delta Lake Standard** | ACID transactions, time travel |
# MAGIC | **Medallion Architecture** | Bronze → Silver → Gold layers |
# MAGIC | **Control vs Data Plane** | Clear separation of responsibilities |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Interview Questions:
# MAGIC
# MAGIC ### 💬 **Question 1: Control Plane vs Data Plane**
# MAGIC **Q:** Explain the difference between Control Plane and Data Plane in Databricks.
# MAGIC
# MAGIC **A:** Control Plane is the management layer (Databricks cloud) that handles orchestration, metadata, and scheduling. Data Plane is the execution layer (customer cloud) that processes data and accesses storage. Control Plane manages *what* and *when*; Data Plane executes *how*.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💬 **Question 2: Serverless Benefits**
# MAGIC **Q:** What are the benefits of using Serverless compute over traditional clusters?
# MAGIC
# MAGIC **A:** Serverless provides: (1) Auto-scaling without configuration, (2) Pay-per-second billing with zero idle costs, (3) Faster startup times, (4) Zero maintenance, (5) Enhanced security with isolation. It's cost-effective and eliminates cluster management overhead.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💬 **Question 3: Unity Catalog Structure**
# MAGIC **Q:** What is the three-level namespace in Unity Catalog and why is it important?
# MAGIC
# MAGIC **A:** Unity Catalog uses `catalog.schema.table` structure (vs old `schema.table`). The extra catalog level enables: (1) Multi-tenant governance, (2) Environment separation (dev/test/prod), (3) Fine-grained access control at catalog level, (4) Cross-workspace data sharing.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💬 **Question 4: Data Security**
# MAGIC **Q:** How does Databricks ensure data security with the Control/Data Plane architecture?
# MAGIC
# MAGIC **A:** Data never leaves customer cloud account. Control Plane only sees metadata; Data Plane runs in customer VPC with customer security rules. This separation ensures: (1) Data sovereignty, (2) Compliance with regulations, (3) Customer-controlled encryption, (4) Network isolation.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💬 **Question 5: Workspace Components**
# MAGIC **Q:** How do Notebooks, Jobs, and SQL Warehouses work together in Databricks architecture?
# MAGIC
# MAGIC **A:** Notebooks are for development (stored in Control Plane, executed in Data Plane). Jobs orchestrate notebook execution on schedule (scheduling in Control Plane, execution in Data Plane). SQL Warehouses query results (configuration in Control Plane, queries in Data Plane). All share Unity Catalog governance.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💬 **Question 6: Medallion Architecture**
# MAGIC **Q:** Explain the Medallion architecture and its benefits.
# MAGIC
# MAGIC **A:** Medallion has three layers: Bronze (raw ingestion), Silver (cleaned/validated), Gold (business aggregates). Benefits: (1) Clear separation of concerns, (2) Incremental processing, (3) Data quality checkpoints, (4) Reusable transformations, (5) Easy troubleshooting.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💬 **Question 7: Delta Lake Advantages**
# MAGIC **Q:** Why use Delta Lake instead of Parquet files directly?
# MAGIC
# MAGIC **A:** Delta Lake provides: (1) ACID transactions for reliability, (2) Time travel for auditing, (3) Schema enforcement/evolution, (4) Efficient MERGE operations, (5) Change Data Feed for streaming, (6) Optimizations (Z-ordering, compaction). Essential for production data lakes.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💬 **Question 8: Serverless vs Clusters**
# MAGIC **Q:** When would you choose traditional clusters over serverless compute?
# MAGIC
# MAGIC **A:** Use clusters for: (1) 24/7 streaming workloads, (2) Custom network configs (VPC peering), (3) Specialized libraries not in serverless, (4) GPU-intensive ML training, (5) Very specific resource requirements. Otherwise, serverless is preferred.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💬 **Question 9: Unity Catalog Governance**
# MAGIC **Q:** How does Unity Catalog enable centralized governance?
# MAGIC
# MAGIC **A:** Unity Catalog provides: (1) Single metadata store across workspaces, (2) Fine-grained permissions (catalog/schema/table/column/row), (3) Automatic lineage tracking, (4) Audit logging for compliance, (5) Data classification and tagging, (6) Cross-cloud governance.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💬 **Question 10: Architecture Design**
# MAGIC **Q:** Design a Databricks architecture for a multi-environment enterprise setup.
# MAGIC
# MAGIC **A:** 
# MAGIC * **Structure:** Separate workspaces for dev/test/prod
# MAGIC * **Governance:** Single Unity Catalog metastore across all
# MAGIC * **Catalogs:** `dev_catalog`, `test_catalog`, `prod_catalog`
# MAGIC * **Compute:** Serverless for all environments
# MAGIC * **Security:** RBAC with least privilege, separate service principals per environment
# MAGIC * **CI/CD:** Git repos with branch policies, automated deployments
# MAGIC * **Networking:** Separate VPCs per environment, controlled peering
# MAGIC * **Monitoring:** Centralized logging, alerts for production
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Common Mistakes (Recap):
# MAGIC
# MAGIC ❌ Confusing Control Plane vs Data Plane responsibilities
# MAGIC ❌ Using clusters unnecessarily instead of serverless
# MAGIC ❌ Bypassing Unity Catalog governance
# MAGIC ❌ Tight coupling of compute and storage
# MAGIC ❌ Not using Delta Lake format
# MAGIC ❌ Ignoring Medallion architecture
# MAGIC ❌ Hard-coding credentials and storage paths
# MAGIC ❌ No incremental processing strategy
# MAGIC ❌ Poor separation of dev/test/prod
# MAGIC ❌ Lack of monitoring and alerting
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Next Steps:
# MAGIC
# MAGIC 1. ✅ Practice with hands-on exercises in this notebook
# MAGIC 2. ✅ Explore Unity Catalog in your workspace
# MAGIC 3. ✅ Create a serverless job pipeline
# MAGIC 4. ✅ Implement Medallion architecture
# MAGIC 5. ✅ Study Delta Lake features (time travel, MERGE)
# MAGIC 6. ✅ Design a multi-environment architecture
# MAGIC 7. ✅ Learn about Delta Live Tables
# MAGIC 8. ✅ Review Databricks security best practices
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Additional Resources:
# MAGIC
# MAGIC * Databricks Documentation: Architecture Overview
# MAGIC * Unity Catalog Documentation
# MAGIC * Delta Lake Documentation
# MAGIC * Databricks Academy: Architecture Course
# MAGIC * Databricks Blog: Architecture Best Practices
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎉 Congratulations!
# MAGIC
# MAGIC You've completed **Phase 2 Day 6: Databricks Architecture Deep Dive**
# MAGIC
# MAGIC **Key Achievement:** Understanding how Databricks components work together to enable scalable, secure, and governed data engineering.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📝 Watermark: **@TRRaveendra**
# MAGIC ### ✅ Training Complete: Databricks Architecture Mastery

# COMMAND ----------

# DBTITLE 1,BONUS: Architecture Diagrams and Real-World Example
# MAGIC %md
# MAGIC # 🌟 BONUS SECTION: Architecture Diagrams & Real-World Example
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Complete Databricks Architecture Diagram
# MAGIC
# MAGIC ```
# MAGIC ╭────────────────────────────────────────────────────────╮
# MAGIC │         CONTROL PLANE (Databricks Managed Cloud)           │
# MAGIC │                                                              │
# MAGIC │  ┌────────────────────────────────────────────┐  │
# MAGIC │  │  🖥️  Workspace UI                         │  │
# MAGIC │  │  📓 Notebook Storage                       │  │
# MAGIC │  │  📅 Job Scheduler & Orchestration          │  │
# MAGIC │  │  🗃️  Unity Catalog Metadata               │  │
# MAGIC │  │  🔐 Authentication & Authorization         │  │
# MAGIC │  │  📊 Monitoring & Audit Logs                │  │
# MAGIC │  └────────────────────────────────────────────┘  │
# MAGIC ╰────────────────────────────────────────────────────────╯
# MAGIC                          │
# MAGIC                          │ 🔒 Secure REST APIs
# MAGIC                          ↓
# MAGIC ╭────────────────────────────────────────────────────────╮
# MAGIC │         DATA PLANE (Customer Cloud Account - VPC)           │
# MAGIC │                                                              │
# MAGIC │  ┌────────────────────────────────────────────┐  │
# MAGIC │  │  ⚡ Serverless Compute Pools             │  │
# MAGIC │  │     ├─ Spark Executors                   │  │
# MAGIC │  │     ├─ Photon Engine                    │  │
# MAGIC │  │     └─ Auto-scaling Logic               │  │
# MAGIC │  └────────────────────────────────────────────┘  │
# MAGIC │                         │                                │
# MAGIC │                         │ Direct Access                 │
# MAGIC │                         ↓                                │
# MAGIC │  ┌────────────────────────────────────────────┐  │
# MAGIC │  │  💾 STORAGE (Delta Lake)                 │  │
# MAGIC │  │     ├─ AWS S3 / Azure ADLS / GCS       │  │
# MAGIC │  │     ├─ Parquet Files + Transaction Log │  │
# MAGIC │  │     └─ Unity Catalog Governed          │  │
# MAGIC │  └────────────────────────────────────────────┘  │
# MAGIC ╰────────────────────────────────────────────────────────╯
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Real-World Enterprise Architecture Example
# MAGIC
# MAGIC ### Scenario: Global E-Commerce Analytics Platform
# MAGIC
# MAGIC **Company:** GlobalRetail Corp
# MAGIC **Challenge:** Process 10TB daily transactions across 50 countries for real-time analytics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Architecture Design:
# MAGIC
# MAGIC #### 1️⃣ **Multi-Environment Setup**
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────────────┐
# MAGIC │           Unity Catalog (Single Metastore)              │
# MAGIC │                                                          │
# MAGIC │  dev_catalog  │  test_catalog  │  prod_catalog        │
# MAGIC │       ↓            ↓               ↓                 │
# MAGIC │  Dev Workspace  Test Workspace  Prod Workspace          │
# MAGIC └────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2️⃣ **Data Architecture (Medallion)**
# MAGIC
# MAGIC **Bronze Layer (Raw Ingestion):**
# MAGIC ```
# MAGIC prod_catalog.bronze
# MAGIC   ├─ transactions_raw
# MAGIC   ├─ customers_raw
# MAGIC   ├─ products_raw
# MAGIC   └─ inventory_raw
# MAGIC ```
# MAGIC
# MAGIC **Silver Layer (Cleaned):**
# MAGIC ```
# MAGIC prod_catalog.silver
# MAGIC   ├─ transactions_cleaned
# MAGIC   ├─ customers_validated
# MAGIC   ├─ products_enriched
# MAGIC   └─ inventory_current
# MAGIC ```
# MAGIC
# MAGIC **Gold Layer (Analytics):**
# MAGIC ```
# MAGIC prod_catalog.gold
# MAGIC   ├─ daily_sales_summary
# MAGIC   ├─ customer_360_view
# MAGIC   ├─ product_performance
# MAGIC   └─ inventory_forecast
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 3️⃣ **Compute Strategy**
# MAGIC
# MAGIC | Workload | Compute Type | Rationale |
# MAGIC |----------|--------------|------------|
# MAGIC | **Development** | Serverless | Cost-effective, auto-scales |
# MAGIC | **Batch ETL** | Serverless Jobs | Scheduled daily, auto-terminates |
# MAGIC | **Streaming** | Dedicated Cluster | 24/7 requirement |
# MAGIC | **SQL Analytics** | SQL Warehouse | BI tool connectivity |
# MAGIC | **ML Training** | GPU Cluster | Specialized hardware |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 4️⃣ **Data Flow**
# MAGIC
# MAGIC ```
# MAGIC Operational DBs (MySQL, PostgreSQL)
# MAGIC         ↓
# MAGIC Auto Loader (Serverless)
# MAGIC         ↓
# MAGIC Bronze Tables (Raw JSON)
# MAGIC         ↓
# MAGIC Delta Live Tables Pipeline
# MAGIC         ↓
# MAGIC Silver Tables (Cleaned)
# MAGIC         ↓
# MAGIC Spark Jobs (Serverless)
# MAGIC         ↓
# MAGIC Gold Tables (Aggregated)
# MAGIC         ↓
# MAGIC SQL Warehouse
# MAGIC         ↓
# MAGIC BI Dashboards (Tableau, Power BI)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 5️⃣ **Governance Model**
# MAGIC
# MAGIC **Access Control:**
# MAGIC * **Data Engineers:** Full access to Bronze/Silver
# MAGIC * **Data Analysts:** Read access to Silver/Gold
# MAGIC * **Business Users:** Read access to Gold only
# MAGIC * **Data Scientists:** Read Silver/Gold, Write to ML catalog
# MAGIC
# MAGIC **Row-Level Security:**
# MAGIC * Regional managers see only their region's data
# MAGIC * Country-specific compliance rules applied automatically
# MAGIC
# MAGIC **Audit & Compliance:**
# MAGIC * All data access logged
# MAGIC * PII columns tagged and masked
# MAGIC * Quarterly compliance reports automated
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 6️⃣ **Jobs & Orchestration**
# MAGIC
# MAGIC **Daily ETL Job:**
# MAGIC ```
# MAGIC Job: daily_etl_pipeline
# MAGIC   Task 1: ingest_bronze (Serverless)
# MAGIC     └─ Reads from operational DBs
# MAGIC     └─ Writes to Bronze tables
# MAGIC   
# MAGIC   Task 2: transform_silver (Delta Live Tables)
# MAGIC     └─ depends_on: Task 1
# MAGIC     └─ Data quality checks
# MAGIC     └─ Writes to Silver tables
# MAGIC   
# MAGIC   Task 3: aggregate_gold (Serverless)
# MAGIC     └─ depends_on: Task 2
# MAGIC     └─ Business metrics calculation
# MAGIC     └─ Writes to Gold tables
# MAGIC   
# MAGIC   Task 4: refresh_dashboards (SQL)
# MAGIC     └─ depends_on: Task 3
# MAGIC     └─ Refreshes BI dashboards
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 7️⃣ **Cost Optimization**
# MAGIC
# MAGIC * **Serverless:** 70% cost reduction vs always-on clusters
# MAGIC * **Spot Instances:** Dev/test environments use spot for 80% savings
# MAGIC * **Auto-termination:** All clusters terminate after 15 min idle
# MAGIC * **Photon:** Enabled for all SQL workloads (2x performance)
# MAGIC * **Z-Ordering:** Optimized frequent query patterns
# MAGIC
# MAGIC **Monthly Cost:**
# MAGIC * Before Databricks: \$250K (self-managed Spark)
# MAGIC * After Databricks: \$100K (60% reduction)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 8️⃣ **Monitoring & Alerting**
# MAGIC
# MAGIC **Alerts Configured:**
# MAGIC * Job failures → Slack + PagerDuty
# MAGIC * Data quality issues → Email to data team
# MAGIC * Cost spike (>20%) → Email to finance team
# MAGIC * Pipeline latency (>2 hours) → Engineering team
# MAGIC
# MAGIC **Dashboards:**
# MAGIC * Executive: High-level KPIs
# MAGIC * Operations: Pipeline health, SLAs
# MAGIC * Engineering: Performance metrics, costs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Results:
# MAGIC
# MAGIC ✅ **Performance:** 5x faster query performance
# MAGIC ✅ **Cost:** 60% reduction in data infrastructure costs
# MAGIC ✅ **Scalability:** Handles 10TB daily (up from 2TB)
# MAGIC ✅ **Governance:** 100% data lineage tracked
# MAGIC ✅ **Time-to-Insight:** Reduced from 24 hours to 2 hours
# MAGIC ✅ **Developer Productivity:** 3x more pipelines built/month
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Architecture Decisions:
# MAGIC
# MAGIC 1. **Serverless-First:** Default for 90% of workloads
# MAGIC 2. **Unity Catalog:** Single source of truth for governance
# MAGIC 3. **Medallion Architecture:** Clear separation of data quality layers
# MAGIC 4. **Delta Lake:** ACID transactions critical for financial data
# MAGIC 5. **Multi-Environment:** Strict separation of dev/test/prod
# MAGIC 6. **CI/CD:** Automated deployments via Git repos
# MAGIC 7. **Monitoring:** Comprehensive observability for proactive issues
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Takeaway:
# MAGIC
# MAGIC This real-world example demonstrates:
# MAGIC * Proper separation of Control Plane (orchestration) and Data Plane (execution)
# MAGIC * Serverless-first approach for cost optimization
# MAGIC * Unity Catalog for enterprise governance
# MAGIC * Medallion architecture for data quality
# MAGIC * Scalable design handling massive data volumes
# MAGIC
# MAGIC **Your architecture should follow these same principles!**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📝 Watermark: **@TRRaveendra**