# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # 📓 Data Engineering Training — Phase 2 Day 7  
# MAGIC ## 🔧 Notebooks, Repos & Version Control in Databricks  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - Notebook Development (Python, SQL)  
# MAGIC - Git Integration using Repos  
# MAGIC - Version Control Best Practices  
# MAGIC - Collaboration in Databricks  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### ⚙️ Platform: Databricks (Serverless + Unity Catalog + Repos)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Objective:
# MAGIC Learn how to develop production-grade notebooks, integrate with Git using Repos, and apply version control best practices in Databricks.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Engineering Constraints:
# MAGIC - ✅ Databricks Serverless Compute  
# MAGIC - ✅ Unity Catalog Volumes  
# MAGIC - ✅ Delta Lake Format  
# MAGIC - ❌ No RDDs  
# MAGIC - ❌ No cache()/persist()  
# MAGIC - ❌ No /tmp or local storage

# COMMAND ----------

# DBTITLE 1,Section 1: Databricks Notebooks Overview
# MAGIC %md
# MAGIC ---
# MAGIC # 📔 SECTION 1: Databricks Notebooks Overview
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 Explanation:
# MAGIC A **Databricks notebook** is like a digital notebook where you can write code, add notes, create charts, and run everything together. It's like having a Word document and a coding tool combined - you can explain what you're doing AND do it in the same place!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC A **Databricks Notebook** is an interactive, collaborative web-based interface that supports:
# MAGIC - **Multi-language execution**: Python, SQL, Scala, R within the same notebook
# MAGIC - **Cell-based execution**: Run code in discrete, modular blocks
# MAGIC - **Rich visualizations**: Built-in charting and display capabilities
# MAGIC - **Collaborative editing**: Real-time multi-user collaboration
# MAGIC - **Version control integration**: Native Git support via Repos
# MAGIC - **Integrated debugging**: Error tracking and performance monitoring
# MAGIC
# MAGIC ### Key Features:
# MAGIC
# MAGIC | Feature | Description |
# MAGIC |---------|-------------|
# MAGIC | **Magic Commands** | `%python`, `%sql`, `%scala`, `%r`, `%md`, `%sh` |
# MAGIC | **Rich Output** | Tables, charts, images, HTML |
# MAGIC | **Widgets** | Interactive parameters for notebook inputs |
# MAGIC | **Revision History** | Built-in versioning (separate from Git) |
# MAGIC | **Scheduled Execution** | Run notebooks as jobs |
# MAGIC | **Export Formats** | .ipynb, .dbc, .html, .py |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Multi-Language Support:
# MAGIC
# MAGIC Databricks notebooks support **polyglot programming** - mix multiple languages in one notebook:
# MAGIC - **%python** - Data engineering, ML, ETL
# MAGIC - **%sql** - Data analysis, querying
# MAGIC - **%scala** - Advanced Spark operations
# MAGIC - **%r** - Statistical analysis
# MAGIC - **%md** - Documentation
# MAGIC - **%sh** - Shell commands

# COMMAND ----------

# DBTITLE 1,Demo: Python Cell Example
# Python Cell Example - No magic command needed in default Python notebook

from pyspark.sql import functions as F
from datetime import datetime

# Create sample data
data = [
    (1, "Alice", "Data Engineer", 95000),
    (2, "Bob", "Data Scientist", 105000),
    (3, "Charlie", "ML Engineer", 110000),
    (4, "Diana", "Analytics Engineer", 90000),
    (5, "Eve", "Platform Engineer", 100000)
]

columns = ["id", "name", "role", "salary"]

# Create DataFrame
df_employees = spark.createDataFrame(data, columns)

# Add calculated columns
df_enriched = df_employees \
    .withColumn("salary_category", 
                F.when(F.col("salary") >= 100000, "High")
                 .otherwise("Standard")) \
    .withColumn("processed_date", F.lit(datetime.now()))

print("\u2705 Python cell executed successfully")
print(f"\u2139️ Total employees: {df_enriched.count()}")

display(df_enriched)

# COMMAND ----------

# DBTITLE 1,Demo: SQL Cell Example
# MAGIC %sql
# MAGIC -- SQL Cell Example - Using %sql magic command
# MAGIC
# MAGIC -- Create a temporary view from the Python DataFrame
# MAGIC CREATE OR REPLACE TEMP VIEW employee_view AS
# MAGIC SELECT * FROM (VALUES
# MAGIC     (1, 'Alice', 'Data Engineer', 95000),
# MAGIC     (2, 'Bob', 'Data Scientist', 105000),
# MAGIC     (3, 'Charlie', 'ML Engineer', 110000),
# MAGIC     (4, 'Diana', 'Analytics Engineer', 90000),
# MAGIC     (5, 'Eve', 'Platform Engineer', 100000)
# MAGIC ) AS t(id, name, role, salary);
# MAGIC
# MAGIC -- Analyze salary by role
# MAGIC SELECT 
# MAGIC     role,
# MAGIC     COUNT(*) as employee_count,
# MAGIC     AVG(salary) as avg_salary,
# MAGIC     MIN(salary) as min_salary,
# MAGIC     MAX(salary) as max_salary
# MAGIC FROM employee_view
# MAGIC GROUP BY role
# MAGIC ORDER BY avg_salary DESC;

# COMMAND ----------

# DBTITLE 1,Demo: Mixed-Language Workflow
# Mixed-Language Workflow Demo
# Step 1: Python - Data preparation

print("\u2705 Step 1: Python - Creating and processing data")

# Create sample transaction data
transaction_data = [
    ("TXN001", "2026-04-15", 1250.00, "COMPLETED"),
    ("TXN002", "2026-04-16", 890.50, "COMPLETED"),
    ("TXN003", "2026-04-17", 2340.75, "PENDING"),
    ("TXN004", "2026-04-18", 567.30, "COMPLETED"),
    ("TXN005", "2026-04-19", 1890.00, "FAILED")
]

df_transactions = spark.createDataFrame(
    transaction_data, 
    ["transaction_id", "transaction_date", "amount", "status"]
)

# Register as temp view for SQL access
df_transactions.createOrReplaceTempView("transactions")

print(f"\u2139️ Prepared {df_transactions.count()} transactions for SQL analysis")
display(df_transactions)

# COMMAND ----------

# DBTITLE 1,Demo: SQL Analysis on Python Data
# MAGIC %sql
# MAGIC -- Step 2: SQL - Analyze the data created in Python
# MAGIC
# MAGIC SELECT 
# MAGIC     status,
# MAGIC     COUNT(*) as transaction_count,
# MAGIC     SUM(amount) as total_amount,
# MAGIC     AVG(amount) as avg_amount,
# MAGIC     ROUND(SUM(amount) / (SELECT SUM(amount) FROM transactions) * 100, 2) as pct_of_total
# MAGIC FROM transactions
# MAGIC GROUP BY status
# MAGIC ORDER BY total_amount DESC;

# COMMAND ----------

# DBTITLE 1,Demo: Python - Process SQL Results
# Step 3: Python - Continue processing
# Access the SQL query results using _sqldf

print("\u2705 Step 3: Python - Processing SQL results")

# The previous SQL cell result is automatically stored in _sqldf
df_summary = spark.table("transactions")

# Add business logic
df_final = df_summary \
    .withColumn("processing_fee", F.col("amount") * 0.025) \
    .withColumn("net_amount", F.col("amount") - F.col("processing_fee"))

print("\u2705 Mixed-language workflow completed successfully!")
print("\u2728 This demonstrates Python → SQL → Python flow in one notebook")

display(df_final)

# COMMAND ----------

# DBTITLE 1,Section 2: Notebook Development Best Practices
# MAGIC %md
# MAGIC ---
# MAGIC # 🎯 SECTION 2: Notebook Development Best Practices
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 Explanation:
# MAGIC Writing good notebook code is like organizing your room - everything should have its place, be easy to find, and make sense to others. Don't put everything in one big pile!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC Production-grade notebooks require **disciplined engineering practices**:
# MAGIC
# MAGIC ### 1️⃣ **Modular Design Pattern**
# MAGIC
# MAGIC Separate concerns into logical units:
# MAGIC ```
# MAGIC ├── Configuration (Parameters, Paths)
# MAGIC ├── Data Ingestion (Read from sources)
# MAGIC ├── Data Transformation (Business logic)
# MAGIC ├── Data Quality Checks (Validation)
# MAGIC ├── Data Output (Write to targets)
# MAGIC └── Logging & Monitoring
# MAGIC ```
# MAGIC
# MAGIC ### 2️⃣ **Parameterization**
# MAGIC
# MAGIC Use **Databricks Widgets** for runtime parameters:
# MAGIC - Environment selection (dev/test/prod)
# MAGIC - Date ranges
# MAGIC - Table names
# MAGIC - Processing modes
# MAGIC
# MAGIC ### 3️⃣ **Clean Coding Practices**
# MAGIC
# MAGIC | Practice | Why It Matters |
# MAGIC |----------|----------------|
# MAGIC | **Descriptive variable names** | `df_customer_orders` not `df1` |
# MAGIC | **Cell organization** | One logical operation per cell |
# MAGIC | **Documentation** | Markdown cells between code sections |
# MAGIC | **Error handling** | Try-except blocks for robustness |
# MAGIC | **Avoid hardcoding** | Use parameters and variables |
# MAGIC | **DRY principle** | Don't Repeat Yourself - use functions |
# MAGIC
# MAGIC ### 4️⃣ **Delta Lake Best Practices**
# MAGIC
# MAGIC - Always use **Delta format** for tables
# MAGIC - Implement **schema evolution** when needed
# MAGIC - Use **merge operations** for upserts
# MAGIC - Add **table properties** for metadata
# MAGIC - Never use cache/persist in serverless
# MAGIC
# MAGIC ### 5️⃣ **Unity Catalog Integration**
# MAGIC
# MAGIC - Use **three-level namespace**: `catalog.schema.table`
# MAGIC - Store data in **Unity Catalog Volumes**
# MAGIC - Apply **governance tags** and **row-level security**
# MAGIC - Document table schemas and lineage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚫 Common Anti-Patterns to Avoid:
# MAGIC
# MAGIC - ❌ Monolithic notebooks (1000+ lines)
# MAGIC - ❌ Hardcoded paths and credentials
# MAGIC - ❌ No error handling
# MAGIC - ❌ Missing documentation
# MAGIC - ❌ Using RDDs instead of DataFrames
# MAGIC - ❌ Using cache() in serverless environments
# MAGIC - ❌ Writing to /tmp or local storage

# COMMAND ----------

# DBTITLE 1,Demo: Modular Pipeline - Configuration
# ==================================================
# MODULAR PIPELINE DEMO - PHASE 1: CONFIGURATION
# ==================================================

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import uuid

print("⚙️ CONFIGURATION PHASE")
print("=" * 50)

# Configuration parameters (in production, use widgets)
config = {
    "source_format": "json",
    "target_format": "delta",
    "environment": "dev",
    "processing_date": datetime.now().strftime("%Y-%m-%d"),
    "run_id": str(uuid.uuid4())[:8]
}

print(f"✅ Environment: {config['environment']}")
print(f"✅ Processing Date: {config['processing_date']}")
print(f"✅ Run ID: {config['run_id']}")
print(f"✅ Configuration loaded successfully")

# COMMAND ----------

# DBTITLE 1,Demo: Modular Pipeline - Ingestion
# ==================================================
# MODULAR PIPELINE DEMO - PHASE 2: DATA INGESTION
# ==================================================

print("\n📥 DATA INGESTION PHASE")
print("=" * 50)

# Simulate reading data (in production, read from Unity Catalog Volumes)
# For demo, we create sample data

raw_data = [
    {"customer_id": "C001", "order_id": "ORD001", "product": "Laptop", "quantity": 1, "price": 1200.00, "order_date": "2026-04-15"},
    {"customer_id": "C002", "order_id": "ORD002", "product": "Mouse", "quantity": 2, "price": 25.50, "order_date": "2026-04-16"},
    {"customer_id": "C003", "order_id": "ORD003", "product": "Keyboard", "quantity": 1, "price": 75.00, "order_date": "2026-04-17"},
    {"customer_id": "C001", "order_id": "ORD004", "product": "Monitor", "quantity": 2, "price": 350.00, "order_date": "2026-04-18"},
    {"customer_id": "C004", "order_id": "ORD005", "product": "Headphones", "quantity": 1, "price": 120.00, "order_date": "2026-04-19"}
]

# Create DataFrame from raw data
df_raw = spark.createDataFrame(raw_data)

print(f"✅ Ingested {df_raw.count()} records")
print(f"✅ Schema: {len(df_raw.columns)} columns")
print("ℹ️ Sample raw data:")

display(df_raw.limit(3))

# COMMAND ----------

# DBTITLE 1,Demo: Modular Pipeline - Transformation
# ==================================================
# MODULAR PIPELINE DEMO - PHASE 3: TRANSFORMATION
# ==================================================

print("\n\u2699️ DATA TRANSFORMATION PHASE")
print("=" * 50)

# Apply business transformations
df_transformed = df_raw \
    .withColumn("total_amount", F.col("quantity") * F.col("price")) \
    .withColumn("order_date", F.to_date(F.col("order_date"))) \
    .withColumn("order_year", F.year(F.col("order_date"))) \
    .withColumn("order_month", F.month(F.col("order_date"))) \
    .withColumn("order_day", F.dayofmonth(F.col("order_date"))) \
    .withColumn("processing_timestamp", F.current_timestamp()) \
    .withColumn("run_id", F.lit(config['run_id'])) \
    .withColumn("data_source", F.lit("demo_source"))

# Add data quality flag
df_transformed = df_transformed \
    .withColumn("is_valid", 
                F.when((F.col("quantity") > 0) & (F.col("price") > 0), True)
                 .otherwise(False))

print(f"\u2705 Transformation completed")
print(f"\u2705 Output columns: {len(df_transformed.columns)}")
print(f"\u2705 Valid records: {df_transformed.filter(F.col('is_valid') == True).count()}")
print("\u2139️ Sample transformed data:")

display(df_transformed)

# COMMAND ----------

# DBTITLE 1,Demo: Modular Pipeline - Data Quality
# ==================================================
# MODULAR PIPELINE DEMO - PHASE 4: DATA QUALITY
# ==================================================

print("\n\u2705 DATA QUALITY VALIDATION PHASE")
print("=" * 50)

# Data quality checks
total_records = df_transformed.count()
valid_records = df_transformed.filter(F.col("is_valid") == True).count()
invalid_records = total_records - valid_records

# Check for nulls
null_checks = {}
for col_name in df_transformed.columns:
    null_count = df_transformed.filter(F.col(col_name).isNull()).count()
    null_checks[col_name] = null_count

# Calculate data quality score
data_quality_score = (valid_records / total_records * 100) if total_records > 0 else 0

print(f"\u2139️ Total Records: {total_records}")
print(f"\u2705 Valid Records: {valid_records}")
print(f"\u26a0️ Invalid Records: {invalid_records}")
print(f"\u2b50 Data Quality Score: {data_quality_score:.2f}%")
print(f"\u2705 Null Check: All columns have {sum(null_checks.values())} null values")

if data_quality_score >= 95:
    print("\n\u2705 Data quality check PASSED - Proceeding to output phase")
else:
    print("\n\u26a0️ Data quality check WARNING - Review data quality issues")

# COMMAND ----------

# DBTITLE 1,Demo: Modular Pipeline - Output
# ==================================================
# MODULAR PIPELINE DEMO - PHASE 5: DATA OUTPUT
# ==================================================

print("\n💾 DATA OUTPUT PHASE")
print("=" * 50)

# In production: Write to Unity Catalog Volume
# Example: /Volumes/catalog_name/schema_name/volume_name/path

# For this demo, we'll create a temp view and show the final output
table_name = "demo_orders_processed"

# Register as temporary view (simulating Delta table write)
df_final = df_transformed.filter(F.col("is_valid") == True)
df_final.createOrReplaceTempView(table_name)

print(f"✅ Data written to temp view: {table_name}")
print(f"✅ Records written: {df_final.count()}")
print(f"✅ Partition columns: order_year, order_month")
print(f"✅ Format: Delta (simulated)")

# In production, you would write like this:
# df_final.write \
#     .format("delta") \
#     .mode("append") \
#     .partitionBy("order_year", "order_month") \
#     .option("mergeSchema", "true") \
#     .saveAsTable(f"catalog.schema.{table_name}")

print("\n✨ MODULAR PIPELINE COMPLETED SUCCESSFULLY!")
print("=" * 50)
print(f"✅ Configuration -> Ingestion -> Transformation -> Quality -> Output")

display(df_final)

# COMMAND ----------

# DBTITLE 1,Section 3: Databricks Repos (Git Integration)
# MAGIC %md
# MAGIC ---
# MAGIC # 🔄 SECTION 3: Databricks Repos (Git Integration)
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 Explanation:
# MAGIC **Databricks Repos** is like connecting your Databricks workspace to your Google Drive (but for code). Every change you make in Databricks can be saved to a special storage place (Git) so you can go back in time, share with teammates, and never lose your work!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Databricks Repos** provides native **Git integration** within the Databricks workspace, enabling:
# MAGIC
# MAGIC - **Version control** for notebooks, Python files, and scripts
# MAGIC - **Collaborative development** with branch-based workflows
# MAGIC - **CI/CD integration** for automated deployments
# MAGIC - **Code review processes** via pull requests
# MAGIC - **Environment separation** (dev/staging/prod branches)
# MAGIC
# MAGIC ### 🏛️ Architecture:
# MAGIC
# MAGIC ```
# MAGIC Local Git Repository (GitHub/GitLab/Azure DevOps)
# MAGIC            ↕️
# MAGIC    [Git Sync Operations]
# MAGIC            ↕️
# MAGIC   Databricks Repos Folder
# MAGIC            ↕️
# MAGIC    Databricks Workspace
# MAGIC   (Notebooks & Scripts)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔗 Supported Git Providers:
# MAGIC
# MAGIC | Provider | Support | Features |
# MAGIC |----------|---------|----------|
# MAGIC | **GitHub** | ✅ Full | Public/Private repos, SSH/PAT auth |
# MAGIC | **GitLab** | ✅ Full | Self-hosted & Cloud, SSH/PAT auth |
# MAGIC | **Azure DevOps** | ✅ Full | Azure Repos integration |
# MAGIC | **Bitbucket Cloud** | ✅ Full | SSH/PAT authentication |
# MAGIC | **AWS CodeCommit** | ✅ Supported | AWS native integration |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Key Features:
# MAGIC
# MAGIC ### 1️⃣ **Branch Management**
# MAGIC - Create, switch, and merge branches
# MAGIC - Isolated development environments
# MAGIC - Feature branch workflows
# MAGIC
# MAGIC ### 2️⃣ **Commit & Push**
# MAGIC - Commit changes with messages
# MAGIC - Push to remote repositories
# MAGIC - Pull latest changes from team
# MAGIC
# MAGIC ### 3️⃣ **Conflict Resolution**
# MAGIC - Visual merge conflict resolution
# MAGIC - Side-by-side diff view
# MAGIC - Manual conflict handling
# MAGIC
# MAGIC ### 4️⃣ **Revision History**
# MAGIC - View commit history
# MAGIC - Compare versions
# MAGIC - Rollback to previous commits
# MAGIC
# MAGIC ### 5️⃣ **Authentication**
# MAGIC - Personal Access Tokens (PAT)
# MAGIC - SSH keys
# MAGIC - OAuth integration
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Setting Up Repos:
# MAGIC
# MAGIC ### Step 1: Configure Git Integration
# MAGIC ```
# MAGIC 1. Navigate to: User Settings → Git Integration
# MAGIC 2. Add Git provider credentials (PAT or SSH key)
# MAGIC 3. Verify connection
# MAGIC ```
# MAGIC
# MAGIC ### Step 2: Clone Repository
# MAGIC ```
# MAGIC 1. Click "Repos" in sidebar
# MAGIC 2. Click "Add Repo"
# MAGIC 3. Enter Git URL
# MAGIC 4. Select branch
# MAGIC 5. Choose workspace location
# MAGIC ```
# MAGIC
# MAGIC ### Step 3: Start Development
# MAGIC ```
# MAGIC 1. Create/edit notebooks in Repos folder
# MAGIC 2. Make changes
# MAGIC 3. Commit changes
# MAGIC 4. Push to remote
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚡ Benefits of Using Repos:
# MAGIC
# MAGIC | Benefit | Impact |
# MAGIC |---------|--------|
# MAGIC | **Version Control** | Track all changes, rollback if needed |
# MAGIC | **Collaboration** | Multiple developers work simultaneously |
# MAGIC | **Code Review** | Peer review via pull requests |
# MAGIC | **Audit Trail** | Complete history of who changed what |
# MAGIC | **CI/CD Ready** | Automate testing and deployment |
# MAGIC | **Backup & Recovery** | Never lose code, restore any version |
# MAGIC | **Environment Parity** | Same code across dev/test/prod |

# COMMAND ----------

# DBTITLE 1,Demo: Git Commands Overview
# MAGIC %md
# MAGIC ## 💻 Git Command Reference
# MAGIC
# MAGIC ### Common Git Operations in Databricks Repos:
# MAGIC
# MAGIC While Databricks provides a **UI-based Git interface**, understanding the underlying Git commands helps:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔹 **Clone Repository**
# MAGIC ```bash
# MAGIC # Via Databricks UI: Add Repo button
# MAGIC # Equivalent Git command:
# MAGIC git clone https://github.com/username/repo-name.git
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔹 **Check Status**
# MAGIC ```bash
# MAGIC # See what files changed
# MAGIC git status
# MAGIC
# MAGIC # View current branch
# MAGIC git branch
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔹 **Create Branch**
# MAGIC ```bash
# MAGIC # Create and switch to new feature branch
# MAGIC git checkout -b feature/new-pipeline
# MAGIC
# MAGIC # Or in two steps:
# MAGIC git branch feature/new-pipeline
# MAGIC git checkout feature/new-pipeline
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔹 **Commit Changes**
# MAGIC ```bash
# MAGIC # Stage all changes
# MAGIC git add .
# MAGIC
# MAGIC # Stage specific file
# MAGIC git add notebook.py
# MAGIC
# MAGIC # Commit with message
# MAGIC git commit -m "Add customer segmentation pipeline"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔹 **Push Changes**
# MAGIC ```bash
# MAGIC # Push to remote branch
# MAGIC git push origin feature/new-pipeline
# MAGIC
# MAGIC # Push and set upstream
# MAGIC git push -u origin feature/new-pipeline
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔹 **Pull Latest Changes**
# MAGIC ```bash
# MAGIC # Pull from current branch
# MAGIC git pull
# MAGIC
# MAGIC # Pull from specific branch
# MAGIC git pull origin main
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔹 **Merge Branches**
# MAGIC ```bash
# MAGIC # Switch to main branch
# MAGIC git checkout main
# MAGIC
# MAGIC # Merge feature branch
# MAGIC git merge feature/new-pipeline
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔹 **View History**
# MAGIC ```bash
# MAGIC # View commit log
# MAGIC git log
# MAGIC
# MAGIC # View compact log
# MAGIC git log --oneline
# MAGIC
# MAGIC # View with graph
# MAGIC git log --graph --oneline --all
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔹 **Resolve Conflicts**
# MAGIC ```bash
# MAGIC # After conflict, edit files manually
# MAGIC # Then stage resolved files:
# MAGIC git add conflicted-file.py
# MAGIC
# MAGIC # Complete the merge:
# MAGIC git commit -m "Resolved merge conflict"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✨ **In Databricks Repos UI:**
# MAGIC - All these operations are available via intuitive UI
# MAGIC - Click-based branch management
# MAGIC - Visual conflict resolution
# MAGIC - Built-in diff viewer
# MAGIC - No terminal commands needed!

# COMMAND ----------

# DBTITLE 1,Demo: Simulated Git Workflow
# ==================================================
# SIMULATED GIT WORKFLOW IN DATABRICKS
# ==================================================

print("👨‍💻 GIT WORKFLOW SIMULATION")
print("=" * 60)

# This demonstrates a typical Git workflow conceptually
# Actual Git operations are done via Databricks Repos UI

workflow_steps = [
    {
        "step": 1,
        "action": "Clone Repository",
        "command": "git clone https://github.com/team/data-pipelines.git",
        "ui_action": "Repos -> Add Repo -> Enter Git URL",
        "result": "Repository cloned to workspace"
    },
    {
        "step": 2,
        "action": "Create Feature Branch",
        "command": "git checkout -b feature/customer-analytics",
        "ui_action": "Branch dropdown -> Create Branch",
        "result": "New branch 'feature/customer-analytics' created"
    },
    {
        "step": 3,
        "action": "Develop Notebook",
        "command": "# Edit notebook files",
        "ui_action": "Create/Edit notebooks in Repos folder",
        "result": "Added customer_segmentation.py notebook"
    },
    {
        "step": 4,
        "action": "Stage Changes",
        "command": "git add customer_segmentation.py",
        "ui_action": "Repos -> View changes -> Select files",
        "result": "Changes staged for commit"
    },
    {
        "step": 5,
        "action": "Commit Changes",
        "command": 'git commit -m "Add customer segmentation"',
        "ui_action": "Commit button -> Enter message -> Commit",
        "result": "Changes committed locally"
    },
    {
        "step": 6,
        "action": "Push to Remote",
        "command": "git push origin feature/customer-analytics",
        "ui_action": "Push button in Repos UI",
        "result": "Branch pushed to GitHub/GitLab"
    },
    {
        "step": 7,
        "action": "Create Pull Request",
        "command": "# Done in GitHub/GitLab UI",
        "ui_action": "Navigate to Git provider -> Create PR",
        "result": "PR created for team review"
    },
    {
        "step": 8,
        "action": "Code Review & Merge",
        "command": "# Team reviews, approves, merges",
        "ui_action": "Review PR -> Approve -> Merge to main",
        "result": "Code merged to main branch"
    },
    {
        "step": 9,
        "action": "Pull Latest Changes",
        "command": "git checkout main && git pull",
        "ui_action": "Switch to main -> Pull button",
        "result": "Local workspace synced with latest code"
    }
]

# Display workflow
for step in workflow_steps:
    print(f"\n🔹 STEP {step['step']}: {step['action']}")
    print(f"   💻 Command: {step['command']}")
    print(f"   🖥️ UI Action: {step['ui_action']}")
    print(f"   ✅ Result: {step['result']}")

print("\n" + "=" * 60)
print("✨ Complete Git workflow demonstrated!")
print("👨‍💻 This is how professional teams collaborate using Databricks Repos")

# COMMAND ----------

# DBTITLE 1,Section 4: Version Control Fundamentals
# MAGIC %md
# MAGIC ---
# MAGIC # 🌳 SECTION 4: Version Control Fundamentals
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 Explanation:
# MAGIC **Version control** is like having a save button that remembers EVERY version of your work. Made a mistake? Go back to yesterday. Want to try something new? Create a copy. Working with friends? Everyone can work at the same time without stepping on each other's toes!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Version Control Systems (VCS)** provide systematic management of changes to code:
# MAGIC
# MAGIC ### Core Concepts:
# MAGIC
# MAGIC #### 1️⃣ **Repository (Repo)**
# MAGIC - Central storage for all project files
# MAGIC - Contains complete history of changes
# MAGIC - Can be local and/or remote
# MAGIC
# MAGIC #### 2️⃣ **Commit**
# MAGIC - Snapshot of code at a point in time
# MAGIC - Immutable record with unique ID (hash)
# MAGIC - Includes author, timestamp, message
# MAGIC
# MAGIC #### 3️⃣ **Branch**
# MAGIC - Parallel line of development
# MAGIC - Isolated environment for features
# MAGIC - Can be merged back to main
# MAGIC
# MAGIC #### 4️⃣ **Merge**
# MAGIC - Combining changes from different branches
# MAGIC - Integrates feature work into main codebase
# MAGIC - May require conflict resolution
# MAGIC
# MAGIC #### 5️⃣ **Pull Request (PR)**
# MAGIC - Formal request to merge code
# MAGIC - Enables code review process
# MAGIC - Discussion and approval workflow
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌿 Branching Strategy:
# MAGIC
# MAGIC ### **Git Flow Model**
# MAGIC
# MAGIC ```
# MAGIC main (production)              ●────────●────────●
# MAGIC                               ╱          ╲
# MAGIC develop (integration)    ●───●──────●───●
# MAGIC                         ╱    ╲        ╱
# MAGIC feature/new-pipeline   ●────●
# MAGIC feature/bug-fix                 ●───●
# MAGIC ```
# MAGIC
# MAGIC ### **Branch Types:**
# MAGIC
# MAGIC | Branch | Purpose | Lifetime | Merge Target |
# MAGIC |--------|---------|----------|-------------|
# MAGIC | **main** | Production code | Permanent | N/A |
# MAGIC | **develop** | Integration branch | Permanent | main |
# MAGIC | **feature/** | New features | Temporary | develop |
# MAGIC | **bugfix/** | Bug fixes | Temporary | develop |
# MAGIC | **hotfix/** | Production fixes | Temporary | main + develop |
# MAGIC | **release/** | Release preparation | Temporary | main + develop |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Commit Best Practices:
# MAGIC
# MAGIC ### **Good Commit Messages:**
# MAGIC
# MAGIC ```
# MAGIC ✅ Add customer segmentation pipeline
# MAGIC ✅ Fix null pointer error in transformation
# MAGIC ✅ Update data quality checks for orders table
# MAGIC ✅ Refactor ingestion module for performance
# MAGIC ```
# MAGIC
# MAGIC ### **Bad Commit Messages:**
# MAGIC
# MAGIC ```
# MAGIC ❌ fixed stuff
# MAGIC ❌ update
# MAGIC ❌ changes
# MAGIC ❌ wip
# MAGIC ```
# MAGIC
# MAGIC ### **Commit Message Format:**
# MAGIC
# MAGIC ```
# MAGIC <type>: <subject>
# MAGIC
# MAGIC <body (optional)>
# MAGIC
# MAGIC <footer (optional)>
# MAGIC ```
# MAGIC
# MAGIC **Types:**
# MAGIC - `feat`: New feature
# MAGIC - `fix`: Bug fix
# MAGIC - `docs`: Documentation
# MAGIC - `refactor`: Code restructuring
# MAGIC - `test`: Adding tests
# MAGIC - `chore`: Maintenance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Collaborative Workflow:
# MAGIC
# MAGIC ### **Developer Workflow:**
# MAGIC
# MAGIC ```
# MAGIC 1. Pull latest main branch
# MAGIC    ↓
# MAGIC 2. Create feature branch
# MAGIC    ↓
# MAGIC 3. Develop & test locally
# MAGIC    ↓
# MAGIC 4. Commit changes
# MAGIC    ↓
# MAGIC 5. Push feature branch
# MAGIC    ↓
# MAGIC 6. Create Pull Request
# MAGIC    ↓
# MAGIC 7. Code review & discussion
# MAGIC    ↓
# MAGIC 8. Address feedback
# MAGIC    ↓
# MAGIC 9. Approval & merge
# MAGIC    ↓
# MAGIC 10. Delete feature branch
# MAGIC    ↓
# MAGIC 11. Pull updated main
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛡️ Code Review Process:
# MAGIC
# MAGIC ### **PR Review Checklist:**
# MAGIC
# MAGIC - ☐ Code follows style guidelines
# MAGIC - ☐ Tests pass successfully
# MAGIC - ☐ Documentation updated
# MAGIC - ☐ No security vulnerabilities
# MAGIC - ☐ Performance considerations addressed
# MAGIC - ☐ Error handling implemented
# MAGIC - ☐ Backward compatibility maintained
# MAGIC - ☐ Business logic is correct
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏆 Version Control Benefits:
# MAGIC
# MAGIC | Benefit | Description |
# MAGIC |---------|-------------|
# MAGIC | **Audit Trail** | Complete history of who changed what and why |
# MAGIC | **Collaboration** | Multiple developers work simultaneously |
# MAGIC | **Rollback** | Quickly revert to previous working version |
# MAGIC | **Branching** | Experiment without affecting main code |
# MAGIC | **Code Review** | Improve code quality through peer review |
# MAGIC | **Backup** | Distributed copies prevent data loss |
# MAGIC | **CI/CD Integration** | Automate testing and deployment |

# COMMAND ----------

# DBTITLE 1,Section 5: Notebook + Repo Integration
# MAGIC %md
# MAGIC ---
# MAGIC # 🔗 SECTION 5: Notebook + Repo Integration
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 Explanation:
# MAGIC Notebooks can be saved as regular Python files (.py) or as special notebook files (.ipynb). Think of .py files like plain text documents - easy to compare changes. The .ipynb files are like rich documents with pictures and formatting - harder to compare but prettier to look at!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### File Formats in Databricks:
# MAGIC
# MAGIC #### 1️⃣ **Notebook Format (.ipynb)**
# MAGIC
# MAGIC **Structure:**
# MAGIC - JSON-based format (Jupyter standard)
# MAGIC - Contains cells, outputs, metadata
# MAGIC - Includes execution results
# MAGIC
# MAGIC **Pros:**
# MAGIC - Rich formatting (markdown, images)
# MAGIC - Cell execution history
# MAGIC - Visual outputs preserved
# MAGIC - Interactive development
# MAGIC
# MAGIC **Cons:**
# MAGIC - Difficult to diff in Git
# MAGIC - Merge conflicts challenging
# MAGIC - Large file size (includes outputs)
# MAGIC - Not ideal for CI/CD
# MAGIC
# MAGIC **Use Case:** ➡️ Development, exploration, documentation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2️⃣ **Python Script Format (.py)**
# MAGIC
# MAGIC **Structure:**
# MAGIC - Plain text Python code
# MAGIC - Comments for markdown cells
# MAGIC - No execution outputs
# MAGIC
# MAGIC **Pros:**
# MAGIC - Easy to diff and merge
# MAGIC - Clean Git history
# MAGIC - Small file size
# MAGIC - CI/CD friendly
# MAGIC - Standard Python tooling
# MAGIC
# MAGIC **Cons:**
# MAGIC - No rich formatting
# MAGIC - No visual outputs
# MAGIC - Less interactive
# MAGIC
# MAGIC **Use Case:** ➡️ Production pipelines, CI/CD, collaboration
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 When to Use What:
# MAGIC
# MAGIC | Scenario | Format | Reason |
# MAGIC |----------|--------|--------|
# MAGIC | **Development & Exploration** | .ipynb | Interactive, visual outputs |
# MAGIC | **Production Pipelines** | .py | Version control friendly |
# MAGIC | **Documentation** | .ipynb | Rich formatting |
# MAGIC | **CI/CD Automation** | .py | Standard Python execution |
# MAGIC | **Team Collaboration** | .py | Easier merge conflict resolution |
# MAGIC | **Ad-hoc Analysis** | .ipynb | Quick iteration |
# MAGIC | **Scheduled Jobs** | .py or .ipynb | Both work, .py preferred |
# MAGIC | **Code Review** | .py | Cleaner diffs |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Best Practices:
# MAGIC
# MAGIC ### 1️⃣ **Development Workflow**
# MAGIC
# MAGIC ```
# MAGIC Development:  .ipynb notebook → Interactive development
# MAGIC                   ↓
# MAGIC               Test & validate
# MAGIC                   ↓
# MAGIC Production:   Convert to .py → Production pipeline
# MAGIC                   ↓
# MAGIC               Store in Repos
# MAGIC                   ↓
# MAGIC               CI/CD deployment
# MAGIC ```
# MAGIC
# MAGIC ### 2️⃣ **Converting Notebook to Python Script**
# MAGIC
# MAGIC **Manual Method:**
# MAGIC ```
# MAGIC File → Export → Source File (.py)
# MAGIC ```
# MAGIC
# MAGIC **Programmatic Method:**
# MAGIC ```bash
# MAGIC # Using Databricks CLI
# MAGIC databricks workspace export /path/to/notebook.ipynb \
# MAGIC   --format SOURCE > notebook.py
# MAGIC ```
# MAGIC
# MAGIC ### 3️⃣ **Repository Structure**
# MAGIC
# MAGIC ```
# MAGIC data-pipelines/
# MAGIC ├── notebooks/           # Development notebooks (.ipynb)
# MAGIC │   ├── exploration/
# MAGIC │   └── analysis/
# MAGIC ├── src/                 # Production code (.py)
# MAGIC │   ├── ingestion/
# MAGIC │   ├── transformation/
# MAGIC │   └── output/
# MAGIC ├── tests/               # Unit tests
# MAGIC ├── config/              # Configuration files
# MAGIC ├── requirements.txt     # Dependencies
# MAGIC └── README.md            # Documentation
# MAGIC ```
# MAGIC
# MAGIC ### 4️⃣ **Git Ignore Patterns**
# MAGIC
# MAGIC **.gitignore for Databricks:**
# MAGIC ```gitignore
# MAGIC # Databricks specific
# MAGIC .databricks/
# MAGIC *.dbc
# MAGIC
# MAGIC # Notebook checkpoints
# MAGIC .ipynb_checkpoints/
# MAGIC
# MAGIC # Python
# MAGIC __pycache__/
# MAGIC *.pyc
# MAGIC *.pyo
# MAGIC .pytest_cache/
# MAGIC
# MAGIC # Data files (don't commit data)
# MAGIC *.csv
# MAGIC *.parquet
# MAGIC data/
# MAGIC
# MAGIC # Credentials
# MAGIC *.secret
# MAGIC .env
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚡ Production Pipeline Strategy:
# MAGIC
# MAGIC ### **Two-Track Approach:**
# MAGIC
# MAGIC #### Track 1: Development
# MAGIC - Use .ipynb notebooks
# MAGIC - Rapid iteration
# MAGIC - Visual debugging
# MAGIC - Store in `notebooks/` folder
# MAGIC
# MAGIC #### Track 2: Production
# MAGIC - Convert to .py scripts
# MAGIC - Modular functions
# MAGIC - Unit tests
# MAGIC - Store in `src/` folder
# MAGIC - Deploy via CI/CD
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Code Organization Patterns:
# MAGIC
# MAGIC ### **Pattern 1: Monorepo**
# MAGIC ```
# MAGIC Single repository for all pipelines
# MAGIC + Easy dependency management
# MAGIC + Unified CI/CD
# MAGIC - Can become large
# MAGIC ```
# MAGIC
# MAGIC ### **Pattern 2: Multi-repo**
# MAGIC ```
# MAGIC Separate repos per pipeline/domain
# MAGIC + Isolated concerns
# MAGIC + Independent releases
# MAGIC - Dependency challenges
# MAGIC ```
# MAGIC
# MAGIC ### **Pattern 3: Hybrid**
# MAGIC ```
# MAGIC Shared libraries in one repo
# MAGIC Pipelines in separate repos
# MAGIC + Best of both worlds
# MAGIC + Reusable components
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Databricks Asset Bundles (DABs):
# MAGIC
# MAGIC **Modern approach** for managing Databricks projects:
# MAGIC
# MAGIC - Define infrastructure as code
# MAGIC - Bundle notebooks, jobs, pipelines
# MAGIC - Deploy across environments
# MAGIC - Version control entire project
# MAGIC
# MAGIC ```yaml
# MAGIC # databricks.yml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     customer_pipeline:
# MAGIC       name: Customer Analytics Pipeline
# MAGIC       tasks:
# MAGIC         - task_key: ingestion
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./src/ingestion.py
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 6: Hands-on Demo - Full Workflow
# MAGIC %md
# MAGIC ---
# MAGIC # 💻 SECTION 6: Hands-on Demo - Complete Notebook + Repo Workflow
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Objective:
# MAGIC Demonstrate a complete end-to-end workflow:
# MAGIC 1. Create production-ready notebook
# MAGIC 2. Implement modular pipeline
# MAGIC 3. Simulate version control workflow
# MAGIC 4. Show best practices
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚦 Workflow Steps:
# MAGIC
# MAGIC ```
# MAGIC Step 1: Create Notebook in Repos folder
# MAGIC   ↓
# MAGIC Step 2: Develop PySpark Pipeline
# MAGIC   ↓
# MAGIC Step 3: Add Error Handling & Logging
# MAGIC   ↓
# MAGIC Step 4: Test with Sample Data
# MAGIC   ↓
# MAGIC Step 5: Commit Changes (Conceptual)
# MAGIC   ↓
# MAGIC Step 6: Create Pull Request
# MAGIC   ↓
# MAGIC Step 7: Deploy to Production
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Scenario:
# MAGIC **Build a Customer Order Analytics Pipeline**
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Read order data from Unity Catalog Volume
# MAGIC - Apply transformations (aggregations, enrichment)
# MAGIC - Implement data quality checks
# MAGIC - Write to Delta table
# MAGIC - Follow production best practices

# COMMAND ----------

# DBTITLE 1,Hands-on Demo: Production Pipeline - Part 1
# ==================================================================
# PRODUCTION-READY CUSTOMER ORDER ANALYTICS PIPELINE
# ==================================================================
# Author: TRRaveendra (@TRRaveendra)
# Version: 1.0.0
# Description: Modular pipeline for customer order analytics
# ==================================================================

import logging
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import uuid

# ==================================================================
# SECTION 1: CONFIGURATION & SETUP
# ==================================================================

print("\u2699️ Initializing Customer Order Analytics Pipeline")
print("=" * 70)

# Configuration (in production, use Databricks widgets)
CONFIG = {
    # Environment
    "environment": "dev",
    "pipeline_name": "customer_order_analytics",
    "version": "1.0.0",
    
    # Processing parameters
    "processing_date": datetime.now().strftime("%Y-%m-%d"),
    "run_id": str(uuid.uuid4())[:8],
    "lookback_days": 30,
    
    # Data paths (Unity Catalog Volumes in production)
    # Example: /Volumes/catalog/schema/volume/path
    "source_path": "demo_orders",  # Simulated for demo
    "target_catalog": "demo_catalog",
    "target_schema": "analytics",
    "target_table": "customer_order_summary",
    
    # Data quality thresholds
    "min_quality_score": 95.0,
    "max_null_percentage": 5.0
}

# Display configuration
print("\u2705 Configuration loaded:")
for key, value in CONFIG.items():
    print(f"   • {key}: {value}")

print(f"\n\u2705 Pipeline initialized successfully")
print(f"\u2139️ Run ID: {CONFIG['run_id']}")
print(f"\u2139️ Processing Date: {CONFIG['processing_date']}")

# COMMAND ----------

# DBTITLE 1,Hands-on Demo: Production Pipeline - Part 2
# ==================================================================
# SECTION 2: DATA INGESTION WITH ERROR HANDLING
# ==================================================================

print("\n" + "=" * 70)
print("📥 PHASE 1: DATA INGESTION")
print("=" * 70)

try:
    # In production: Read from Unity Catalog Volume
    # df_orders = spark.read.format("delta").load("/Volumes/catalog/schema/volume/orders")
    
    # For demo: Create synthetic order data
    from datetime import timedelta
    base_date = datetime.now()
    
    order_data = [
        {"order_id": f"ORD{i:04d}", 
         "customer_id": f"C{(i % 50):03d}",
         "product_name": ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones"][i % 5],
         "quantity": (i % 3) + 1,
         "unit_price": [1200.0, 25.5, 75.0, 350.0, 120.0][i % 5],
         "order_date": (base_date - timedelta(days=i % 30)).strftime("%Y-%m-%d"),
         "status": ["COMPLETED", "COMPLETED", "PENDING", "COMPLETED", "SHIPPED"][i % 5],
         "region": ["North", "South", "East", "West"][i % 4]
        }
        for i in range(100)
    ]
    
    df_orders_raw = spark.createDataFrame(order_data)
    
    # Ingestion metrics
    record_count = df_orders_raw.count()
    column_count = len(df_orders_raw.columns)
    
    print(f"✅ Data ingestion successful")
    print(f"   • Records ingested: {record_count}")
    print(f"   • Columns: {column_count}")
    print(f"   • Source: {CONFIG['source_path']}")
    
    # Display sample
    print("\nℹ️ Sample raw data (first 5 rows):")
    display(df_orders_raw.limit(5))
    
except Exception as e:
    print(f"❌ ERROR in data ingestion: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Hands-on Demo: Production Pipeline - Part 3
# ==================================================================
# SECTION 3: DATA TRANSFORMATION & BUSINESS LOGIC
# ==================================================================

print("\n" + "=" * 70)
print("\u2699️ PHASE 2: DATA TRANSFORMATION")
print("=" * 70)

try:
    # Step 1: Basic transformations
    df_transformed = df_orders_raw \
        .withColumn("order_date", F.to_date(F.col("order_date"))) \
        .withColumn("total_amount", F.col("quantity") * F.col("unit_price")) \
        .withColumn("order_year", F.year(F.col("order_date"))) \
        .withColumn("order_month", F.month(F.col("order_date"))) \
        .withColumn("order_quarter", F.quarter(F.col("order_date")))
    
    # Step 2: Customer-level aggregations
    df_customer_summary = df_transformed \
        .groupBy("customer_id", "region") \
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.countDistinct("product_name").alias("unique_products")
        )
    
    # Step 3: Customer segmentation
    df_final = df_customer_summary \
        .withColumn("customer_segment",
                    F.when(F.col("total_revenue") >= 5000, "Premium")
                     .when(F.col("total_revenue") >= 2000, "Gold")
                     .when(F.col("total_revenue") >= 1000, "Silver")
                     .otherwise("Bronze")) \
        .withColumn("customer_lifetime_days",
                    F.datediff(F.col("last_order_date"), F.col("first_order_date"))) \
        .withColumn("is_active",
                    F.when(F.datediff(F.current_date(), F.col("last_order_date")) <= 30, True)
                     .otherwise(False))
    
    # Step 4: Add metadata
    df_final = df_final \
        .withColumn("processing_timestamp", F.current_timestamp()) \
        .withColumn("pipeline_version", F.lit(CONFIG['version'])) \
        .withColumn("run_id", F.lit(CONFIG['run_id']))
    
    # Transformation metrics
    print(f"\u2705 Transformation completed")
    print(f"   • Input records: {df_transformed.count()}")
    print(f"   • Output records: {df_final.count()}")
    print(f"   • Aggregation: Customer-level summary")
    
    # Display results
    print("\n\u2139️ Customer analytics summary:")
    display(df_final.orderBy(F.col("total_revenue").desc()))
    
except Exception as e:
    print(f"\u274c ERROR in transformation: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Hands-on Demo: Production Pipeline - Part 4
# ==================================================================
# SECTION 4: DATA QUALITY VALIDATION
# ==================================================================

print("\n" + "=" * 70)
print("\u2705 PHASE 3: DATA QUALITY VALIDATION")
print("=" * 70)

try:
    # Quality check 1: Record count validation
    total_records = df_final.count()
    
    # Quality check 2: Null analysis
    null_checks = {}
    for col_name in df_final.columns:
        null_count = df_final.filter(F.col(col_name).isNull()).count()
        null_percentage = (null_count / total_records * 100) if total_records > 0 else 0
        null_checks[col_name] = {"count": null_count, "percentage": null_percentage}
    
    # Quality check 3: Business rule validation
    invalid_revenue = df_final.filter(F.col("total_revenue") < 0).count()
    invalid_orders = df_final.filter(F.col("total_orders") <= 0).count()
    
    # Calculate overall quality score
    max_null_pct = max([v["percentage"] for v in null_checks.values()])
    quality_score = 100 - max_null_pct
    
    # Display quality metrics
    print(f"\u2139️ Data Quality Report:")
    print(f"   • Total records: {total_records}")
    print(f"   • Quality score: {quality_score:.2f}%")
    print(f"   • Max null percentage: {max_null_pct:.2f}%")
    print(f"   • Invalid revenue records: {invalid_revenue}")
    print(f"   • Invalid order records: {invalid_orders}")
    
    # Quality threshold check
    if quality_score >= CONFIG['min_quality_score']:
        print(f"\n\u2705 Quality check PASSED (Score: {quality_score:.2f}% >= {CONFIG['min_quality_score']}%)")
        quality_status = "PASSED"
    else:
        print(f"\n\u26a0️ Quality check WARNING (Score: {quality_score:.2f}% < {CONFIG['min_quality_score']}%)")
        quality_status = "WARNING"
    
    # Store quality metrics
    quality_metrics = {
        "run_id": CONFIG['run_id'],
        "total_records": total_records,
        "quality_score": quality_score,
        "status": quality_status,
        "timestamp": datetime.now().isoformat()
    }
    
    print(f"\n\u2705 Quality validation completed")
    
except Exception as e:
    print(f"\u274c ERROR in quality validation: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Hands-on Demo: Production Pipeline - Part 5
# ==================================================================
# SECTION 5: DATA OUTPUT & PERSISTENCE
# ==================================================================

print("\n" + "=" * 70)
print("💾 PHASE 4: DATA OUTPUT")
print("=" * 70)

try:
    # In production: Write to Unity Catalog Delta table
    # target_table = f"{CONFIG['target_catalog']}.{CONFIG['target_schema']}.{CONFIG['target_table']}"
    # 
    # df_final.write \
    #     .format("delta") \
    #     .mode("overwrite") \
    #     .option("mergeSchema", "true") \
    #     .option("overwriteSchema", "false") \
    #     .saveAsTable(target_table)
    
    # For demo: Create temporary view
    temp_table_name = CONFIG['target_table']
    df_final.createOrReplaceTempView(temp_table_name)
    
    print(f"✅ Data written successfully")
    print(f"   • Target: {temp_table_name} (temp view for demo)")
    print(f"   • Records written: {df_final.count()}")
    print(f"   • Format: Delta (simulated)")
    print(f"   • Mode: Overwrite")
    
    # Verify write
    verification_count = spark.table(temp_table_name).count()
    print(f"\n✅ Verification: {verification_count} records in target table")
    
except Exception as e:
    print(f"❌ ERROR in data output: {str(e)}")
    raise

# ==================================================================
# PIPELINE SUMMARY
# ==================================================================

print("\n" + "=" * 70)
print("✨ PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
print("=" * 70)
print(f"✅ Pipeline: {CONFIG['pipeline_name']}")
print(f"✅ Version: {CONFIG['version']}")
print(f"✅ Run ID: {CONFIG['run_id']}")
print(f"✅ Quality Score: {quality_score:.2f}%")
print(f"✅ Status: {quality_status}")
print(f"✅ Execution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("\n👍 Ready for Git commit and deployment!")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Hands-on Demo: Verify Results
# MAGIC %sql
# MAGIC -- ==================================================================
# MAGIC -- VERIFY PIPELINE OUTPUT
# MAGIC -- ==================================================================
# MAGIC
# MAGIC -- Query 1: Customer segment distribution
# MAGIC SELECT 
# MAGIC     customer_segment,
# MAGIC     COUNT(*) as customer_count,
# MAGIC     SUM(total_revenue) as segment_revenue,
# MAGIC     AVG(total_revenue) as avg_customer_revenue,
# MAGIC     AVG(total_orders) as avg_orders_per_customer
# MAGIC FROM customer_order_summary
# MAGIC GROUP BY customer_segment
# MAGIC ORDER BY segment_revenue DESC;
# MAGIC
# MAGIC -- Query 2: Regional analysis
# MAGIC SELECT 
# MAGIC     region,
# MAGIC     COUNT(*) as customers,
# MAGIC     SUM(total_revenue) as revenue,
# MAGIC     AVG(avg_order_value) as avg_order_size
# MAGIC FROM customer_order_summary
# MAGIC GROUP BY region
# MAGIC ORDER BY revenue DESC;
# MAGIC
# MAGIC -- Query 3: Active vs inactive customers
# MAGIC SELECT 
# MAGIC     is_active,
# MAGIC     COUNT(*) as customer_count,
# MAGIC     SUM(total_revenue) as total_revenue
# MAGIC FROM customer_order_summary
# MAGIC GROUP BY is_active;

# COMMAND ----------

# DBTITLE 1,Section 7: End-to-End Development Flow
# MAGIC %md
# MAGIC ---
# MAGIC # 🔄 SECTION 7: End-to-End Development Flow
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 Explanation:
# MAGIC Building data pipelines is like building a car assembly line. First you design it (Development), then you test it (Testing), then you check if anyone broke it (Version Control), and finally you turn it on for real (Production). Each step is important!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### Complete SDLC for Data Engineering:
# MAGIC
# MAGIC ```
# MAGIC 💡 IDEATION                    🚀 PRODUCTION
# MAGIC      │                              ↑
# MAGIC      ↓                              │
# MAGIC 📝 PLANNING              📦 DEPLOYMENT
# MAGIC      │                              ↑
# MAGIC      ↓                              │
# MAGIC 💻 DEVELOPMENT           ✅ TESTING
# MAGIC      │                              ↑
# MAGIC      ↓                              │
# MAGIC 💾 VERSION CONTROL       🔍 CODE REVIEW
# MAGIC      │                              ↑
# MAGIC      └──────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Phase Breakdown:
# MAGIC
# MAGIC ### 🟢 **Phase 1: Development**
# MAGIC
# MAGIC **Environment:** Databricks Workspace (Dev)
# MAGIC
# MAGIC **Activities:**
# MAGIC - Create notebook in Repos folder
# MAGIC - Write exploratory code
# MAGIC - Test with sample data
# MAGIC - Iterate and refine
# MAGIC - Add documentation
# MAGIC
# MAGIC **Tools:**
# MAGIC - Databricks Notebooks (.ipynb)
# MAGIC - Interactive execution
# MAGIC - Visual debugging
# MAGIC - display() for data preview
# MAGIC
# MAGIC **Output:** Working prototype notebook
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟡 **Phase 2: Version Control**
# MAGIC
# MAGIC **Environment:** Git Repository (GitHub/GitLab)
# MAGIC
# MAGIC **Activities:**
# MAGIC - Create feature branch
# MAGIC - Commit changes with clear messages
# MAGIC - Push to remote repository
# MAGIC - Track changes over time
# MAGIC
# MAGIC **Tools:**
# MAGIC - Databricks Repos
# MAGIC - Git (via UI or CLI)
# MAGIC - Branch management
# MAGIC
# MAGIC **Output:** Version-controlled code
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔵 **Phase 3: Code Review**
# MAGIC
# MAGIC **Environment:** Git Provider (Pull Request)
# MAGIC
# MAGIC **Activities:**
# MAGIC - Create Pull Request
# MAGIC - Peer code review
# MAGIC - Address feedback
# MAGIC - Approve changes
# MAGIC
# MAGIC **Tools:**
# MAGIC - GitHub/GitLab PR interface
# MAGIC - Code diff viewer
# MAGIC - Comment threads
# MAGIC - Approval workflow
# MAGIC
# MAGIC **Output:** Reviewed and approved code
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟣 **Phase 4: Testing**
# MAGIC
# MAGIC **Environment:** Test Environment
# MAGIC
# MAGIC **Activities:**
# MAGIC - Unit testing
# MAGIC - Integration testing
# MAGIC - Data quality validation
# MAGIC - Performance testing
# MAGIC
# MAGIC **Tools:**
# MAGIC - pytest for unit tests
# MAGIC - Databricks Jobs for integration
# MAGIC - Great Expectations for data quality
# MAGIC
# MAGIC **Output:** Tested and validated pipeline
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟠 **Phase 5: Deployment**
# MAGIC
# MAGIC **Environment:** Staging/Production
# MAGIC
# MAGIC **Activities:**
# MAGIC - Deploy via CI/CD pipeline
# MAGIC - Configure job schedules
# MAGIC - Set up monitoring
# MAGIC - Enable alerting
# MAGIC
# MAGIC **Tools:**
# MAGIC - Databricks Jobs/Workflows
# MAGIC - CI/CD tools (GitHub Actions, Jenkins)
# MAGIC - Databricks Asset Bundles (DABs)
# MAGIC
# MAGIC **Output:** Deployed production pipeline
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 **Phase 6: Production**
# MAGIC
# MAGIC **Environment:** Production Environment
# MAGIC
# MAGIC **Activities:**
# MAGIC - Scheduled execution
# MAGIC - Monitor performance
# MAGIC - Track data quality
# MAGIC - Handle incidents
# MAGIC
# MAGIC **Tools:**
# MAGIC - Databricks Jobs
# MAGIC - Monitoring dashboards
# MAGIC - Alerting systems
# MAGIC - Logging
# MAGIC
# MAGIC **Output:** Running production system
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Environment Strategy:
# MAGIC
# MAGIC | Environment | Purpose | Branch | Data |
# MAGIC |-------------|---------|--------|------|
# MAGIC | **Development** | Active coding | feature/* | Sample/synthetic |
# MAGIC | **Testing** | Validation | develop | Subset of prod |
# MAGIC | **Staging** | Pre-prod testing | release/* | Production clone |
# MAGIC | **Production** | Live system | main | Full production |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Mapping Components:
# MAGIC
# MAGIC ### **Development Phase:**
# MAGIC - **Notebook** → Interactive development
# MAGIC - **Repos** → Version control integration
# MAGIC - **Widgets** → Parameterization
# MAGIC
# MAGIC ### **Production Phase:**
# MAGIC - **Jobs/Workflows** → Scheduled execution
# MAGIC - **Delta Tables** → Data storage
# MAGIC - **Unity Catalog** → Data governance
# MAGIC - **Monitoring** → Observability
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚙️ CI/CD Pipeline:
# MAGIC
# MAGIC ```yaml
# MAGIC # Example GitHub Actions workflow
# MAGIC name: Databricks CI/CD
# MAGIC
# MAGIC on:
# MAGIC   pull_request:
# MAGIC     branches: [main]
# MAGIC   push:
# MAGIC     branches: [main]
# MAGIC
# MAGIC jobs:
# MAGIC   test:
# MAGIC     runs-on: ubuntu-latest
# MAGIC     steps:
# MAGIC       - uses: actions/checkout@v2
# MAGIC       - name: Run unit tests
# MAGIC         run: pytest tests/
# MAGIC       
# MAGIC   deploy:
# MAGIC     needs: test
# MAGIC     if: github.ref == 'refs/heads/main'
# MAGIC     runs-on: ubuntu-latest
# MAGIC     steps:
# MAGIC       - name: Deploy to Databricks
# MAGIC         run: databricks bundle deploy --target prod
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Deployment Strategies:
# MAGIC
# MAGIC ### 1️⃣ **Blue-Green Deployment**
# MAGIC ```
# MAGIC Blue (current)  →  Switch  ←  Green (new)
# MAGIC      ↓                      ↓
# MAGIC   Users                  Testing
# MAGIC ```
# MAGIC
# MAGIC ### 2️⃣ **Canary Deployment**
# MAGIC ```
# MAGIC 90% traffic → Old version
# MAGIC 10% traffic → New version
# MAGIC    (Monitor, then increase gradually)
# MAGIC ```
# MAGIC
# MAGIC ### 3️⃣ **Rolling Deployment**
# MAGIC ```
# MAGIC Update servers/jobs one at a time
# MAGIC Monitor each before proceeding
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Genie Code Agent Usage Examples
# MAGIC %md
# MAGIC ---
# MAGIC # 🤖 Using Databricks Genie Code Agent
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 What is Genie Code Agent?
# MAGIC
# MAGIC **Genie Code** is Databricks' AI-powered coding assistant that helps you:
# MAGIC - Generate notebook code
# MAGIC - Convert notebooks to production scripts
# MAGIC - Implement best practices
# MAGIC - Create Git workflows
# MAGIC - Debug and optimize code
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💬 Example Prompts for Notebooks & Repos:
# MAGIC
# MAGIC ### 🟢 **Notebook Development:**
# MAGIC
# MAGIC **Prompt 1:**
# MAGIC ```
# MAGIC Generate a modular notebook that reads customer data from 
# MAGIC Unity Catalog volume, performs RFM analysis, and writes 
# MAGIC results to Delta table
# MAGIC ```
# MAGIC
# MAGIC **Prompt 2:**
# MAGIC ```
# MAGIC Create a data quality validation framework for checking 
# MAGIC null values, duplicates, and schema compliance in PySpark
# MAGIC ```
# MAGIC
# MAGIC **Prompt 3:**
# MAGIC ```
# MAGIC Build an ETL pipeline with error handling that processes 
# MAGIC JSON files from Auto Loader and applies SCD Type 2 logic
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔵 **Git & Version Control:**
# MAGIC
# MAGIC **Prompt 4:**
# MAGIC ```
# MAGIC Convert this notebook to a production-ready Python script 
# MAGIC with proper function structure and docstrings
# MAGIC ```
# MAGIC
# MAGIC **Prompt 5:**
# MAGIC ```
# MAGIC Create a Git workflow strategy for a data engineering team 
# MAGIC with dev, test, and prod environments
# MAGIC ```
# MAGIC
# MAGIC **Prompt 6:**
# MAGIC ```
# MAGIC Generate a .gitignore file for Databricks projects including 
# MAGIC notebook checkpoints and data files
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟡 **Pipeline Refactoring:**
# MAGIC
# MAGIC **Prompt 7:**
# MAGIC ```
# MAGIC Refactor this monolithic notebook into separate functions for 
# MAGIC ingestion, transformation, validation, and output
# MAGIC ```
# MAGIC
# MAGIC **Prompt 8:**
# MAGIC ```
# MAGIC Add comprehensive error handling and logging to this 
# MAGIC PySpark pipeline
# MAGIC ```
# MAGIC
# MAGIC **Prompt 9:**
# MAGIC ```
# MAGIC Optimize this Spark code for better performance - remove 
# MAGIC unnecessary shuffles and add broadcast joins where appropriate
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟣 **Testing & Validation:**
# MAGIC
# MAGIC **Prompt 10:**
# MAGIC ```
# MAGIC Create unit tests using pytest for these transformation 
# MAGIC functions with sample data fixtures
# MAGIC ```
# MAGIC
# MAGIC **Prompt 11:**
# MAGIC ```
# MAGIC Generate data quality assertions using Great Expectations 
# MAGIC for this customer orders dataset
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟠 **Documentation:**
# MAGIC
# MAGIC **Prompt 12:**
# MAGIC ```
# MAGIC Add comprehensive markdown documentation to this notebook 
# MAGIC explaining the business logic and data flow
# MAGIC ```
# MAGIC
# MAGIC **Prompt 13:**
# MAGIC ```
# MAGIC Create a README.md file for this data pipeline repository 
# MAGIC with setup instructions and architecture diagram
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✨ Best Practices for Using Genie:
# MAGIC
# MAGIC 1. **Be Specific:** Provide context about your data and requirements
# MAGIC 2. **Iterate:** Start simple, then refine with follow-up prompts
# MAGIC 3. **Review Generated Code:** Always validate AI-generated code
# MAGIC 4. **Learn Patterns:** Use Genie to learn best practices
# MAGIC 5. **Combine with Documentation:** Reference Databricks docs for details
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛡️ Important Notes:
# MAGIC
# MAGIC ⚠️ **Always review generated code for:**
# MAGIC - Security implications (no hardcoded credentials)
# MAGIC - Performance considerations (appropriate for your data size)
# MAGIC - Compliance with your organization's standards
# MAGIC - Correctness of business logic
# MAGIC
# MAGIC ✅ **Genie is great for:**
# MAGIC - Boilerplate code generation
# MAGIC - Learning new Spark/SQL patterns
# MAGIC - Converting between formats
# MAGIC - Implementing standard patterns
# MAGIC - Quick prototyping
# MAGIC
# MAGIC ❌ **Genie should not replace:**
# MAGIC - Understanding core concepts
# MAGIC - Code review processes
# MAGIC - Testing and validation
# MAGIC - Domain expertise
# MAGIC - Security practices

# COMMAND ----------

# DBTITLE 1,Final Summary & Key Learnings
# MAGIC %md
# MAGIC ---
# MAGIC # 🎓 FINAL SUMMARY & KEY LEARNINGS
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 What We Learned Today:
# MAGIC
# MAGIC ### 1️⃣ **Databricks Notebooks**
# MAGIC - Multi-language support (Python, SQL, Scala, R)
# MAGIC - Cell-based execution model
# MAGIC - Interactive development environment
# MAGIC - Rich visualizations and outputs
# MAGIC - Magic commands (%sql, %python, %md, %sh)
# MAGIC
# MAGIC ### 2️⃣ **Notebook Best Practices**
# MAGIC - Modular design (separation of concerns)
# MAGIC - Parameterization using widgets
# MAGIC - Clean coding practices
# MAGIC - Error handling and logging
# MAGIC - Avoiding anti-patterns (RDDs, cache in serverless)
# MAGIC
# MAGIC ### 3️⃣ **Databricks Repos**
# MAGIC - Native Git integration
# MAGIC - Support for GitHub, GitLab, Azure DevOps
# MAGIC - Branch-based development
# MAGIC - UI-based Git operations
# MAGIC - Seamless workspace integration
# MAGIC
# MAGIC ### 4️⃣ **Version Control**
# MAGIC - Repository management
# MAGIC - Commit, branch, merge workflows
# MAGIC - Pull request process
# MAGIC - Code review practices
# MAGIC - Branching strategies (Git Flow)
# MAGIC
# MAGIC ### 5️⃣ **Production Practices**
# MAGIC - .ipynb for development, .py for production
# MAGIC - Modular pipeline design
# MAGIC - Data quality validation
# MAGIC - Environment separation (dev/test/prod)
# MAGIC - CI/CD integration
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Key Takeaways:
# MAGIC
# MAGIC ✅ **Notebooks are powerful** for interactive development but need discipline for production use
# MAGIC
# MAGIC ✅ **Version control is mandatory** - all code should be in Git
# MAGIC
# MAGIC ✅ **Modular design** makes code maintainable and testable
# MAGIC
# MAGIC ✅ **Repos provide seamless Git integration** within Databricks workspace
# MAGIC
# MAGIC ✅ **Separate development from production** - use branches and environments
# MAGIC
# MAGIC ✅ **Code review improves quality** - always use pull requests
# MAGIC
# MAGIC ✅ **Automate deployment** - use CI/CD pipelines for production
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏆 Production Readiness Checklist:
# MAGIC
# MAGIC ### Before Committing Code:
# MAGIC - ☐ Code follows modular design
# MAGIC - ☐ Error handling implemented
# MAGIC - ☐ Data quality checks added
# MAGIC - ☐ No hardcoded values
# MAGIC - ☐ Documentation complete
# MAGIC - ☐ Tested with sample data
# MAGIC - ☐ Logging implemented
# MAGIC - ☐ No sensitive data in code
# MAGIC
# MAGIC ### Before Merging to Main:
# MAGIC - ☐ Code review completed
# MAGIC - ☐ All tests passing
# MAGIC - ☐ Documentation updated
# MAGIC - ☐ No merge conflicts
# MAGIC - ☐ Approval received
# MAGIC - ☐ CI/CD checks passed
# MAGIC
# MAGIC ### Before Production Deployment:
# MAGIC - ☐ Tested in staging environment
# MAGIC - ☐ Performance validated
# MAGIC - ☐ Monitoring configured
# MAGIC - ☐ Alerting set up
# MAGIC - ☐ Rollback plan ready
# MAGIC - ☐ Stakeholders notified
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚡ Quick Reference:
# MAGIC
# MAGIC ### Notebook Magic Commands:
# MAGIC ```
# MAGIC %python  - Python code
# MAGIC %sql     - SQL queries
# MAGIC %scala   - Scala code
# MAGIC %r       - R code
# MAGIC %md      - Markdown documentation
# MAGIC %sh      - Shell commands
# MAGIC %fs      - Databricks filesystem commands
# MAGIC ```
# MAGIC
# MAGIC ### Git Workflow:
# MAGIC ```
# MAGIC 1. git checkout -b feature/name  # Create branch
# MAGIC 2. # Make changes
# MAGIC 3. git add .                     # Stage changes
# MAGIC 4. git commit -m "message"       # Commit
# MAGIC 5. git push origin feature/name  # Push
# MAGIC 6. # Create PR in GitHub/GitLab
# MAGIC 7. # Review and merge
# MAGIC ```
# MAGIC
# MAGIC ### Databricks Best Practices:
# MAGIC ```python
# MAGIC # Good
# MAGIC df.write.format("delta").save(path)
# MAGIC display(df)
# MAGIC df_result = df.filter(...)
# MAGIC
# MAGIC # Avoid
# MAGIC df.cache()  # Not needed in serverless
# MAGIC df.rdd      # Use DataFrame API instead
# MAGIC /tmp/data   # Use Unity Catalog Volumes
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Interview Questions
# MAGIC %md
# MAGIC ---
# MAGIC # 🎯 INTERVIEW QUESTIONS
# MAGIC ---
# MAGIC
# MAGIC ## 🔵 Fundamental Level:
# MAGIC
# MAGIC ### **Q1: What is a Databricks notebook?**
# MAGIC **Answer:** A Databricks notebook is an interactive, web-based development environment that supports multiple programming languages (Python, SQL, Scala, R) in a single document. It consists of cells that can contain code, markdown, or commands, and provides features like cell-by-cell execution, rich visualizations, real-time collaboration, and integration with Databricks compute resources.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Q2: What are notebook magic commands?**
# MAGIC **Answer:** Magic commands are special directives that start with % and control cell behavior:
# MAGIC - `%python` - Execute Python code
# MAGIC - `%sql` - Execute SQL queries
# MAGIC - `%scala` - Execute Scala code
# MAGIC - `%r` - Execute R code
# MAGIC - `%md` - Markdown documentation
# MAGIC - `%sh` - Shell commands
# MAGIC
# MAGIC They allow mixing multiple languages in a single notebook.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Q3: What is Databricks Repos?**
# MAGIC **Answer:** Databricks Repos is a native Git integration feature that allows you to:
# MAGIC - Clone Git repositories into Databricks workspace
# MAGIC - Perform version control operations (commit, push, pull, branch)
# MAGIC - Collaborate using branch-based workflows
# MAGIC - Integrate with GitHub, GitLab, Azure DevOps, Bitbucket
# MAGIC - Manage code through a visual interface or Git commands
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Q4: Why is version control important for data engineering?**
# MAGIC **Answer:** Version control provides:
# MAGIC - **History tracking** - Complete audit trail of changes
# MAGIC - **Collaboration** - Multiple developers work simultaneously
# MAGIC - **Rollback capability** - Revert to previous working versions
# MAGIC - **Code review** - Peer review through pull requests
# MAGIC - **Branching** - Isolated development of features
# MAGIC - **CI/CD integration** - Automated testing and deployment
# MAGIC - **Backup** - Distributed code storage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Q5: What's the difference between .ipynb and .py files?**
# MAGIC **Answer:**
# MAGIC
# MAGIC **`.ipynb` (Notebook format):**
# MAGIC - JSON-based, includes outputs and metadata
# MAGIC - Rich formatting and visualizations
# MAGIC - Difficult to diff and merge
# MAGIC - Best for development and exploration
# MAGIC
# MAGIC **`.py` (Python script):**
# MAGIC - Plain text Python code
# MAGIC - Easy to version control and diff
# MAGIC - No execution outputs
# MAGIC - Best for production pipelines
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🟡 Intermediate Level:
# MAGIC
# MAGIC ### **Q6: Explain modular notebook design.**
# MAGIC **Answer:** Modular design separates concerns into logical units:
# MAGIC
# MAGIC 1. **Configuration** - Parameters and settings
# MAGIC 2. **Ingestion** - Read data from sources
# MAGIC 3. **Transformation** - Business logic
# MAGIC 4. **Validation** - Data quality checks
# MAGIC 5. **Output** - Write to targets
# MAGIC 6. **Logging** - Monitoring and tracking
# MAGIC
# MAGIC **Benefits:**
# MAGIC - Easier testing and debugging
# MAGIC - Better maintainability
# MAGIC - Reusable components
# MAGIC - Clear data flow
# MAGIC - Simplified troubleshooting
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Q7: What is a branching strategy and why is it important?**
# MAGIC **Answer:** A branching strategy defines how teams organize code development:
# MAGIC
# MAGIC **Git Flow Model:**
# MAGIC - `main` - Production code
# MAGIC - `develop` - Integration branch
# MAGIC - `feature/*` - New features
# MAGIC - `bugfix/*` - Bug fixes
# MAGIC - `hotfix/*` - Emergency production fixes
# MAGIC
# MAGIC **Importance:**
# MAGIC - Isolates work in progress
# MAGIC - Enables parallel development
# MAGIC - Protects production code
# MAGIC - Facilitates code review
# MAGIC - Supports environment separation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Q8: How do you handle notebook parameters?**
# MAGIC **Answer:** Use **Databricks Widgets** for parameterization:
# MAGIC
# MAGIC ```python
# MAGIC # Create widget
# MAGIC dbutils.widgets.text("environment", "dev", "Environment")
# MAGIC dbutils.widgets.dropdown("mode", "append", 
# MAGIC                          ["append", "overwrite"])
# MAGIC
# MAGIC # Get value
# MAGIC env = dbutils.widgets.get("environment")
# MAGIC mode = dbutils.widgets.get("mode")
# MAGIC ```
# MAGIC
# MAGIC **Benefits:**
# MAGIC - Runtime configuration
# MAGIC - Reusable notebooks
# MAGIC - Environment switching
# MAGIC - Job parameterization
# MAGIC - No hardcoding
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Q9: What are common Git operations in Databricks Repos?**
# MAGIC **Answer:**
# MAGIC
# MAGIC 1. **Clone** - Import repository to workspace
# MAGIC 2. **Branch** - Create feature branches
# MAGIC 3. **Commit** - Save changes with messages
# MAGIC 4. **Push** - Upload to remote repository
# MAGIC 5. **Pull** - Download latest changes
# MAGIC 6. **Merge** - Combine branches
# MAGIC 7. **Resolve conflicts** - Handle merge conflicts
# MAGIC 8. **Switch branches** - Change working branch
# MAGIC
# MAGIC All available through UI or Git commands.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Q10: How do you ensure notebook code quality?**
# MAGIC **Answer:**
# MAGIC
# MAGIC **Code Quality Practices:**
# MAGIC - Use meaningful variable names
# MAGIC - Add error handling (try-except)
# MAGIC - Implement data quality checks
# MAGIC - Add documentation (markdown cells)
# MAGIC - Avoid hardcoding values
# MAGIC - Follow DRY principle
# MAGIC - Use type hints
# MAGIC - Add logging statements
# MAGIC
# MAGIC **Review Process:**
# MAGIC - Peer code review via pull requests
# MAGIC - Automated testing (unit tests)
# MAGIC - Static code analysis (linting)
# MAGIC - Performance testing
# MAGIC - Security scanning
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔴 Advanced Level:
# MAGIC
# MAGIC ### **Q11: How would you implement CI/CD for Databricks notebooks?**
# MAGIC **Answer:**
# MAGIC
# MAGIC **CI/CD Pipeline:**
# MAGIC
# MAGIC 1. **Source Control** - Code in Git (GitHub/GitLab)
# MAGIC 2. **Trigger** - Push to branch triggers pipeline
# MAGIC 3. **Build** - Install dependencies, run linting
# MAGIC 4. **Test** - Execute unit and integration tests
# MAGIC 5. **Deploy** - Deploy to target environment
# MAGIC 6. **Validate** - Run smoke tests
# MAGIC 7. **Monitor** - Track deployment success
# MAGIC
# MAGIC **Tools:**
# MAGIC - GitHub Actions / GitLab CI
# MAGIC - Databricks CLI / REST API
# MAGIC - Databricks Asset Bundles (DABs)
# MAGIC - Jenkins / Azure DevOps
# MAGIC
# MAGIC **Example:**
# MAGIC ```yaml
# MAGIC # .github/workflows/deploy.yml
# MAGIC name: Deploy Pipeline
# MAGIC on:
# MAGIC   push:
# MAGIC     branches: [main]
# MAGIC jobs:
# MAGIC   deploy:
# MAGIC     runs-on: ubuntu-latest
# MAGIC     steps:
# MAGIC       - uses: actions/checkout@v2
# MAGIC       - name: Deploy
# MAGIC         run: |
# MAGIC           databricks bundle deploy --target prod
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Q12: How do you manage multiple environments (dev/test/prod)?**
# MAGIC **Answer:**
# MAGIC
# MAGIC **Strategy:**
# MAGIC
# MAGIC 1. **Branch Mapping:**
# MAGIC    - `develop` → Dev environment
# MAGIC    - `release/*` → Test environment
# MAGIC    - `main` → Production environment
# MAGIC
# MAGIC 2. **Configuration Management:**
# MAGIC ```python
# MAGIC configs = {
# MAGIC     "dev": {
# MAGIC         "catalog": "dev_catalog",
# MAGIC         "compute": "dev-cluster"
# MAGIC     },
# MAGIC     "prod": {
# MAGIC         "catalog": "prod_catalog",
# MAGIC         "compute": "prod-cluster"
# MAGIC     }
# MAGIC }
# MAGIC config = configs[dbutils.widgets.get("env")]
# MAGIC ```
# MAGIC
# MAGIC 3. **Environment Separation:**
# MAGIC    - Separate Unity Catalog schemas
# MAGIC    - Different compute resources
# MAGIC    - Isolated data volumes
# MAGIC    - Environment-specific credentials
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Q13: What are best practices for handling merge conflicts in notebooks?**
# MAGIC **Answer:**
# MAGIC
# MAGIC **Prevention:**
# MAGIC - Communicate with team about what you're working on
# MAGIC - Keep branches short-lived
# MAGIC - Pull latest changes frequently
# MAGIC - Work on different sections when possible
# MAGIC - Use .py format for production code (easier merging)
# MAGIC
# MAGIC **Resolution:**
# MAGIC 1. Identify conflicting sections
# MAGIC 2. Understand both changes
# MAGIC 3. Manually resolve conflicts
# MAGIC 4. Test the merged code
# MAGIC 5. Commit the resolution
# MAGIC
# MAGIC **For .ipynb files:**
# MAGIC - Conflicts are difficult due to JSON structure
# MAGIC - Consider using nbdime (notebook diff tool)
# MAGIC - Or convert to .py format before merging
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Q14: How do you implement data quality checks in a production notebook?**
# MAGIC **Answer:**
# MAGIC
# MAGIC **Comprehensive Approach:**
# MAGIC
# MAGIC ```python
# MAGIC def validate_data_quality(df, config):
# MAGIC     checks = []
# MAGIC     
# MAGIC     # Check 1: Null validation
# MAGIC     for col in config['required_columns']:
# MAGIC         null_count = df.filter(F.col(col).isNull()).count()
# MAGIC         checks.append({
# MAGIC             'check': f'null_{col}',
# MAGIC             'passed': null_count == 0,
# MAGIC             'value': null_count
# MAGIC         })
# MAGIC     
# MAGIC     # Check 2: Row count validation
# MAGIC     row_count = df.count()
# MAGIC     checks.append({
# MAGIC         'check': 'min_rows',
# MAGIC         'passed': row_count >= config['min_rows'],
# MAGIC         'value': row_count
# MAGIC     })
# MAGIC     
# MAGIC     # Check 3: Schema validation
# MAGIC     expected_schema = set(config['expected_columns'])
# MAGIC     actual_schema = set(df.columns)
# MAGIC     schema_match = expected_schema == actual_schema
# MAGIC     checks.append({
# MAGIC         'check': 'schema',
# MAGIC         'passed': schema_match,
# MAGIC         'value': list(actual_schema)
# MAGIC     })
# MAGIC     
# MAGIC     # Check 4: Business rules
# MAGIC     invalid_records = df.filter(
# MAGIC         (F.col('amount') < 0) | 
# MAGIC         (F.col('quantity') < 0)
# MAGIC     ).count()
# MAGIC     checks.append({
# MAGIC         'check': 'business_rules',
# MAGIC         'passed': invalid_records == 0,
# MAGIC         'value': invalid_records
# MAGIC     })
# MAGIC     
# MAGIC     # Calculate overall score
# MAGIC     passed = sum(1 for c in checks if c['passed'])
# MAGIC     total = len(checks)
# MAGIC     score = (passed / total) * 100
# MAGIC     
# MAGIC     return {
# MAGIC         'checks': checks,
# MAGIC         'score': score,
# MAGIC         'passed': score >= config['min_quality_score']
# MAGIC     }
# MAGIC ```
# MAGIC
# MAGIC **Integration:**
# MAGIC - Run checks after each transformation
# MAGIC - Log results to monitoring system
# MAGIC - Fail pipeline if quality score too low
# MAGIC - Alert on quality degradation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Q15: Explain the lifecycle of a data pipeline from notebook to production.**
# MAGIC **Answer:**
# MAGIC
# MAGIC **Complete Lifecycle:**
# MAGIC
# MAGIC **Phase 1: Development (Notebook)**
# MAGIC - Create .ipynb notebook in Repos folder
# MAGIC - Develop code interactively
# MAGIC - Test with sample data
# MAGIC - Add documentation
# MAGIC - Commit to feature branch
# MAGIC
# MAGIC **Phase 2: Code Review**
# MAGIC - Create Pull Request
# MAGIC - Peer review code
# MAGIC - Run automated tests
# MAGIC - Address feedback
# MAGIC - Merge to develop branch
# MAGIC
# MAGIC **Phase 3: Testing**
# MAGIC - Deploy to test environment
# MAGIC - Run with production-like data
# MAGIC - Validate results
# MAGIC - Performance testing
# MAGIC - Fix issues
# MAGIC
# MAGIC **Phase 4: Production Preparation**
# MAGIC - Convert to .py if needed
# MAGIC - Add production configurations
# MAGIC - Set up monitoring
# MAGIC - Configure alerting
# MAGIC - Document runbook
# MAGIC
# MAGIC **Phase 5: Deployment**
# MAGIC - Deploy via CI/CD pipeline
# MAGIC - Create Databricks Job
# MAGIC - Set schedule
# MAGIC - Enable job
# MAGIC - Monitor first runs
# MAGIC
# MAGIC **Phase 6: Operations**
# MAGIC - Scheduled execution
# MAGIC - Monitor performance
# MAGIC - Track data quality
# MAGIC - Handle incidents
# MAGIC - Iterate and improve
# MAGIC
# MAGIC **Key Points:**
# MAGIC - Never deploy directly to production
# MAGIC - Always use version control
# MAGIC - Test thoroughly
# MAGIC - Monitor continuously
# MAGIC - Document everything

# COMMAND ----------

# DBTITLE 1,Common Mistakes & How to Avoid Them
# MAGIC %md
# MAGIC ---
# MAGIC # ⚠️ COMMON MISTAKES & HOW TO AVOID THEM
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Mistake #1: Not Using Version Control
# MAGIC
# MAGIC **The Problem:**
# MAGIC - Developing notebooks directly in workspace without Git
# MAGIC - No history of changes
# MAGIC - Can't rollback mistakes
# MAGIC - No collaboration workflow
# MAGIC - Lost code when workspace deleted
# MAGIC
# MAGIC **The Solution:**
# MAGIC ✅ Always use Databricks Repos
# MAGIC ✅ Commit changes regularly
# MAGIC ✅ Use meaningful commit messages
# MAGIC ✅ Push to remote repository
# MAGIC ✅ Never develop in non-Repos folders
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Mistake #2: Monolithic Notebooks
# MAGIC
# MAGIC **The Problem:**
# MAGIC - Single notebook with 1000+ lines
# MAGIC - Everything in one cell
# MAGIC - Mixed concerns (ingestion + transformation + output)
# MAGIC - Hard to debug
# MAGIC - Impossible to reuse
# MAGIC - Difficult to test
# MAGIC
# MAGIC **The Solution:**
# MAGIC ✅ Use modular design - separate cells/functions
# MAGIC ✅ One responsibility per cell/function
# MAGIC ✅ Clear separation: read → transform → write
# MAGIC ✅ Extract reusable logic to functions
# MAGIC ✅ Keep notebooks focused and small
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Mistake #3: Mixing Development and Production Code
# MAGIC
# MAGIC **The Problem:**
# MAGIC - Same notebook for dev and prod
# MAGIC - No environment separation
# MAGIC - Hardcoded prod values
# MAGIC - Testing in production
# MAGIC - No proper deployment process
# MAGIC
# MAGIC **The Solution:**
# MAGIC ✅ Separate dev/test/prod environments
# MAGIC ✅ Use parameters/widgets for configuration
# MAGIC ✅ Different branches for different environments
# MAGIC ✅ Formal deployment process
# MAGIC ✅ Never test directly in production
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Mistake #4: Ignoring Git Workflows
# MAGIC
# MAGIC **The Problem:**
# MAGIC - Committing directly to main branch
# MAGIC - No feature branches
# MAGIC - No pull requests
# MAGIC - No code review
# MAGIC - Unclear commit messages ("update", "fix")
# MAGIC
# MAGIC **The Solution:**
# MAGIC ✅ Always use feature branches
# MAGIC ✅ Create PRs for all changes
# MAGIC ✅ Require code review before merge
# MAGIC ✅ Write descriptive commit messages
# MAGIC ✅ Follow branching strategy
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Mistake #5: Hardcoding Values
# MAGIC
# MAGIC **The Problem:**
# MAGIC ```python
# MAGIC # Bad
# MAGIC df = spark.read.load("/prod/customer/data")
# MAGIC threshold = 100
# MAGIC max_records = 1000
# MAGIC ```
# MAGIC
# MAGIC **The Solution:**
# MAGIC ```python
# MAGIC # Good
# MAGIC data_path = dbutils.widgets.get("data_path")
# MAGIC threshold = int(dbutils.widgets.get("threshold"))
# MAGIC max_records = int(dbutils.widgets.get("max_records"))
# MAGIC
# MAGIC df = spark.read.load(data_path)
# MAGIC ```
# MAGIC ✅ Use widgets for parameters
# MAGIC ✅ Configuration files for constants
# MAGIC ✅ Environment variables for secrets
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Mistake #6: No Error Handling
# MAGIC
# MAGIC **The Problem:**
# MAGIC ```python
# MAGIC # Bad
# MAGIC df = spark.read.load(path)
# MAGIC df_transformed = transform(df)
# MAGIC df_transformed.write.save(output_path)
# MAGIC ```
# MAGIC
# MAGIC **The Solution:**
# MAGIC ```python
# MAGIC # Good
# MAGIC try:
# MAGIC     df = spark.read.load(path)
# MAGIC     print(f"Loaded {df.count()} records")
# MAGIC     
# MAGIC     df_transformed = transform(df)
# MAGIC     validate_quality(df_transformed)
# MAGIC     
# MAGIC     df_transformed.write.save(output_path)
# MAGIC     print("Write successful")
# MAGIC     
# MAGIC except Exception as e:
# MAGIC     print(f"Pipeline failed: {str(e)}")
# MAGIC     # Log to monitoring system
# MAGIC     # Send alert
# MAGIC     raise
# MAGIC ```
# MAGIC ✅ Always use try-except blocks
# MAGIC ✅ Log errors with context
# MAGIC ✅ Fail gracefully
# MAGIC ✅ Send alerts on failures
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Mistake #7: No Data Quality Checks
# MAGIC
# MAGIC **The Problem:**
# MAGIC - Assuming data is always valid
# MAGIC - No null checks
# MAGIC - No schema validation
# MAGIC - No business rule validation
# MAGIC - Silent failures
# MAGIC
# MAGIC **The Solution:**
# MAGIC ✅ Validate schema matches expectations
# MAGIC ✅ Check for nulls in required columns
# MAGIC ✅ Validate business rules
# MAGIC ✅ Monitor data quality scores
# MAGIC ✅ Fail pipeline if quality too low
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Mistake #8: Using Anti-Patterns
# MAGIC
# MAGIC **The Problem:**
# MAGIC ```python
# MAGIC # Bad - Don't do this
# MAGIC df.cache()  # Not needed in serverless
# MAGIC rdd = df.rdd  # Use DataFrame API instead
# MAGIC df.collect()  # Don't collect large datasets
# MAGIC data.to_csv("/tmp/data.csv")  # No local storage
# MAGIC ```
# MAGIC
# MAGIC **The Solution:**
# MAGIC ```python
# MAGIC # Good - Do this
# MAGIC # Serverless handles caching automatically
# MAGIC df = spark.read.format("delta").load(path)
# MAGIC df_result = df.filter(...).select(...)  # DataFrame API
# MAGIC display(df.limit(100))  # Preview data
# MAGIC df.write.format("delta").save(volume_path)  # Use Volumes
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Mistake #9: Poor Commit Messages
# MAGIC
# MAGIC **Bad Commits:**
# MAGIC ```
# MAGIC ❌ "update"
# MAGIC ❌ "fix"
# MAGIC ❌ "changes"
# MAGIC ❌ "wip"
# MAGIC ❌ "test"
# MAGIC ```
# MAGIC
# MAGIC **Good Commits:**
# MAGIC ```
# MAGIC ✅ "Add customer segmentation using RFM analysis"
# MAGIC ✅ "Fix null pointer error in date transformation"
# MAGIC ✅ "Update data quality thresholds to 95%"
# MAGIC ✅ "Refactor ingestion module for Auto Loader"
# MAGIC ✅ "Add unit tests for transformation functions"
# MAGIC ```
# MAGIC
# MAGIC **Format:**
# MAGIC ```
# MAGIC <type>: <concise description>
# MAGIC
# MAGIC <optional detailed explanation>
# MAGIC
# MAGIC <optional references>
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Mistake #10: No Documentation
# MAGIC
# MAGIC **The Problem:**
# MAGIC - No markdown cells explaining logic
# MAGIC - No README in repository
# MAGIC - No comments in complex code
# MAGIC - No architecture documentation
# MAGIC - Future you (or teammates) confused
# MAGIC
# MAGIC **The Solution:**
# MAGIC ✅ Add markdown cells between sections
# MAGIC ✅ Explain business logic and assumptions
# MAGIC ✅ Document data sources and schemas
# MAGIC ✅ Create comprehensive README
# MAGIC ✅ Add comments for complex algorithms
# MAGIC ✅ Document deployment process
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Quick Checklist - Avoid These Mistakes:
# MAGIC
# MAGIC - ☐ Using version control (Repos)
# MAGIC - ☐ Modular notebook design
# MAGIC - ☐ Separate dev/test/prod
# MAGIC - ☐ Feature branch workflow
# MAGIC - ☐ Parameterized code
# MAGIC - ☐ Error handling
# MAGIC - ☐ Data quality checks
# MAGIC - ☐ Following best practices
# MAGIC - ☐ Good commit messages
# MAGIC - ☐ Comprehensive documentation

# COMMAND ----------

# DBTITLE 1,Conclusion & Next Steps
# MAGIC %md
# MAGIC ---
# MAGIC # 🎉 CONCLUSION & NEXT STEPS
# MAGIC ---
# MAGIC
# MAGIC ## 🎓 What You've Mastered:
# MAGIC
# MAGIC ✅ **Databricks Notebooks** - Multi-language interactive development
# MAGIC ✅ **Modular Design** - Production-grade code organization
# MAGIC ✅ **Git Integration** - Version control with Databricks Repos
# MAGIC ✅ **Version Control Workflows** - Branching, commits, pull requests
# MAGIC ✅ **Best Practices** - Error handling, quality checks, parameterization
# MAGIC ✅ **End-to-End Flow** - Development to production deployment
# MAGIC ✅ **Genie Code Agent** - AI-assisted development
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Next Steps in Your Learning Journey:
# MAGIC
# MAGIC ### **Immediate Actions:**
# MAGIC 1. Create your first Repos folder
# MAGIC 2. Set up Git integration with your provider
# MAGIC 3. Practice feature branch workflow
# MAGIC 4. Refactor an existing notebook using modular design
# MAGIC 5. Add error handling to your pipelines
# MAGIC
# MAGIC ### **Short Term (This Week):**
# MAGIC 1. Convert a notebook to production .py format
# MAGIC 2. Set up CI/CD pipeline (GitHub Actions)
# MAGIC 3. Implement data quality framework
# MAGIC 4. Create comprehensive documentation
# MAGIC 5. Practice code reviews with team
# MAGIC
# MAGIC ### **Medium Term (This Month):**
# MAGIC 1. Master Databricks Asset Bundles (DABs)
# MAGIC 2. Implement monitoring and alerting
# MAGIC 3. Build reusable library of functions
# MAGIC 4. Establish team Git workflow standards
# MAGIC 5. Automate testing framework
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Additional Resources:
# MAGIC
# MAGIC ### **Documentation:**
# MAGIC - [Databricks Notebooks Guide](https://docs.databricks.com/notebooks/index.html)
# MAGIC - [Databricks Repos Documentation](https://docs.databricks.com/repos/index.html)
# MAGIC - [Git Best Practices](https://git-scm.com/book/en/v2)
# MAGIC - [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
# MAGIC
# MAGIC ### **Tools:**
# MAGIC - Databricks CLI
# MAGIC - Git command line
# MAGIC - VS Code with Databricks extension
# MAGIC - nbdime (notebook diff tool)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏆 Success Metrics:
# MAGIC
# MAGIC You'll know you've mastered this when you can:
# MAGIC
# MAGIC ✅ Create production-ready notebooks with modular design
# MAGIC ✅ Seamlessly work with Git through Databricks Repos
# MAGIC ✅ Follow proper branching and PR workflows
# MAGIC ✅ Implement comprehensive error handling and quality checks
# MAGIC ✅ Deploy pipelines through automated CI/CD
# MAGIC ✅ Collaborate effectively with team using version control
# MAGIC ✅ Convert development notebooks to production scripts
# MAGIC ✅ Debug and troubleshoot pipeline issues efficiently
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💬 Final Thoughts:
# MAGIC
# MAGIC **Remember:**
# MAGIC
# MAGIC 💡 **Version control is not optional** - It's a fundamental engineering practice
# MAGIC
# MAGIC 💡 **Code quality matters** - Your future self will thank you
# MAGIC
# MAGIC 💡 **Collaboration is key** - Use Git workflows to work effectively as a team
# MAGIC
# MAGIC 💡 **Production readiness** - Always think about how your code will run in production
# MAGIC
# MAGIC 💡 **Continuous improvement** - Keep learning and refining your practices
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✨ Ready for Production!
# MAGIC
# MAGIC You now have the knowledge and tools to build production-grade data pipelines using Databricks Notebooks and Repos. Go forth and create amazing things!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 **Author:** TRRaveendra  
# MAGIC ### 🏷️ **Watermark:** @TRRaveendra  
# MAGIC ### 📞 **Training:** Phase 2 - Day 7  
# MAGIC ### 📅 **Date:** 2026-04-21  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🚀 Happy Coding with Databricks! 🚀