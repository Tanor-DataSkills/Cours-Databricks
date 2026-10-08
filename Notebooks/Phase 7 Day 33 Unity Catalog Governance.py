# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # 🔐 Data Engineering Training — Phase 7 Day 33  
# MAGIC ## 🛡️ Unity Catalog Governance: Catalog, Schema, Tables & Access Control  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - Unity Catalog Fundamentals  
# MAGIC - Catalog → Schema → Table Hierarchy  
# MAGIC - Access Control (RBAC / Privileges)  
# MAGIC - Data Governance Best Practices  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### ⚙️ Platform: Databricks (Serverless + Unity Catalog + Delta Lake)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Objective:
# MAGIC Understand how to implement enterprise-grade governance using Unity Catalog, including object hierarchy and fine-grained access control.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Engineering Constraints:
# MAGIC - ✅ Use Databricks Serverless Compute
# MAGIC - ✅ Use Unity Catalog (mandatory for all objects)
# MAGIC - ❌ DO NOT use RDDs
# MAGIC - ❌ DO NOT use cache() / persist()
# MAGIC - ❌ DO NOT use /tmp or local storage
# MAGIC - ✅ Follow governance-first design

# COMMAND ----------

# DBTITLE 1,Section 1: What is Unity Catalog?
# MAGIC %md
# MAGIC # 📖 Section 1: What is Unity Catalog?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine you have a **big library** with thousands of books. Unity Catalog is like a **super librarian** who:
# MAGIC - Knows where every book is  
# MAGIC - Decides who can read which books  
# MAGIC - Tracks who borrowed what book  
# MAGIC - Makes sure no one loses or damages books  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Unity Catalog** is Databricks' **unified governance solution** for data and AI assets across clouds.
# MAGIC
# MAGIC ### Key Characteristics:
# MAGIC
# MAGIC 1. **Centralized Metadata Store**
# MAGIC    - Single source of truth for all data assets
# MAGIC    - Works across AWS, Azure, and GCP
# MAGIC
# MAGIC 2. **Fine-Grained Access Control**
# MAGIC    - ANSI SQL standard GRANT/REVOKE
# MAGIC    - Role-based and attribute-based access control (RBAC/ABAC)
# MAGIC
# MAGIC 3. **Data Discovery & Lineage**
# MAGIC    - Automated lineage tracking
# MAGIC    - Search and discovery capabilities
# MAGIC
# MAGIC 4. **Audit Logging**
# MAGIC    - Who accessed what, when, and how
# MAGIC    - Compliance-ready audit trails
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌟 Why Unity Catalog?
# MAGIC
# MAGIC | Challenge | Unity Catalog Solution |
# MAGIC |-----------|------------------------|
# MAGIC | Data silos across workspaces | Unified namespace |
# MAGIC | Inconsistent access control | Centralized RBAC |
# MAGIC | No audit trail | Built-in audit logging |
# MAGIC | Poor data discovery | Searchable catalog |
# MAGIC | Manual governance | Automated governance |

# COMMAND ----------

# DBTITLE 1,Section 2: Unity Catalog Hierarchy
# MAGIC %md
# MAGIC # 🏛️ Section 2: Unity Catalog Hierarchy
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📈 Three-Level Hierarchy:
# MAGIC
# MAGIC ```
# MAGIC Catalog               (Top Level - Environment/Business Domain)
# MAGIC   │
# MAGIC   ├─ Schema          (Mid Level - Project/Team/Purpose)
# MAGIC   │    │
# MAGIC   │    ├─ Table    (Data Asset)
# MAGIC   │    ├─ View     (Virtual Dataset)
# MAGIC   │    └─ Function (User-Defined Function)
# MAGIC   │
# MAGIC   └─ Schema
# MAGIC        ├─ Table
# MAGIC        └─ View
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC
# MAGIC - **Catalog** = Building  
# MAGIC - **Schema** = Floor in the building  
# MAGIC - **Table** = Room on that floor  
# MAGIC
# MAGIC You always need to know: **Which building? Which floor? Which room?**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect View:
# MAGIC
# MAGIC ### 1️₃ CATALOG
# MAGIC - Highest level of organization
# MAGIC - Represents an environment (dev, test, prod) or business domain
# MAGIC - Examples: `prod_catalog`, `dev_catalog`, `finance_catalog`
# MAGIC
# MAGIC ### 2️₃ SCHEMA (Database)
# MAGIC - Logical grouping within a catalog
# MAGIC - Represents a project, team, or data domain
# MAGIC - Examples: `bronze`, `silver`, `gold`, `sales`, `marketing`
# MAGIC
# MAGIC ### 3️₃ TABLE/VIEW/FUNCTION
# MAGIC - Actual data assets
# MAGIC - Fully qualified name: `catalog.schema.table`
# MAGIC - Example: `prod_catalog.gold.customer_analytics`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📌 Naming Convention Best Practice:
# MAGIC
# MAGIC ```
# MAGIC <environment>_catalog.<layer>_<domain>.<entity>_<type>
# MAGIC
# MAGIC Example:
# MAGIC prod_catalog.silver_sales.transactions_cleaned
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Demo: Create Catalog and Schema
# MAGIC %sql
# MAGIC -- 🔨 SECTION 2 DEMO: Create Unity Catalog Objects
# MAGIC -- @TRRaveendra
# MAGIC
# MAGIC -- Step 1: Create a Catalog
# MAGIC -- Note: You need CREATE CATALOG privilege (usually admin)
# MAGIC CREATE CATALOG IF NOT EXISTS demo_catalog
# MAGIC COMMENT 'Training catalog for Unity Catalog governance - @TRRaveendra';
# MAGIC
# MAGIC -- Step 2: Use the catalog
# MAGIC USE CATALOG demo_catalog;
# MAGIC
# MAGIC -- Step 3: Create schemas for medallion architecture
# MAGIC CREATE SCHEMA IF NOT EXISTS demo_catalog.bronze
# MAGIC COMMENT 'Bronze layer - raw ingested data';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS demo_catalog.silver
# MAGIC COMMENT 'Silver layer - cleaned and validated data';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS demo_catalog.gold
# MAGIC COMMENT 'Gold layer - business-level aggregates';
# MAGIC
# MAGIC -- Step 4: Show catalogs and schemas
# MAGIC SHOW CATALOGS;
# MAGIC SHOW SCHEMAS IN demo_catalog;

# COMMAND ----------

# DBTITLE 1,Section 3: Working with Tables
# MAGIC %md
# MAGIC # 📋 Section 3: Working with Tables in Unity Catalog
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Table Types:
# MAGIC
# MAGIC ### 1️₃ Managed Tables
# MAGIC - **Unity Catalog manages both metadata AND data**
# MAGIC - Data stored in Unity Catalog's managed location
# MAGIC - When you `DROP TABLE`, both metadata and data are deleted
# MAGIC - ✅ **Recommended for most use cases**
# MAGIC
# MAGIC ### 2️₃ External Tables
# MAGIC - **Unity Catalog manages only metadata**
# MAGIC - Data stored in external location (S3, ADLS, GCS)
# MAGIC - When you `DROP TABLE`, only metadata is deleted, data remains
# MAGIC - ✅ Use when data is shared across systems
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC
# MAGIC **Managed Table** = You rent an apartment, and when you leave, everything is cleared out  
# MAGIC **External Table** = You own a house, the property management just has your address
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Comparison:
# MAGIC
# MAGIC | Feature | Managed Table | External Table |
# MAGIC |---------|--------------|----------------|
# MAGIC | Data Location | UC-managed | User-specified |
# MAGIC | DROP behavior | Deletes data | Keeps data |
# MAGIC | Performance | Optimized | Depends on location |
# MAGIC | Governance | Full control | Limited control |
# MAGIC | Use Case | Default choice | Legacy data, shared data |

# COMMAND ----------

# DBTITLE 1,Demo: Create Managed Tables
# MAGIC %sql
# MAGIC -- 🔨 SECTION 3 DEMO: Create and Manage Tables
# MAGIC -- @TRRaveendra
# MAGIC
# MAGIC -- Create a managed table in Bronze layer
# MAGIC CREATE TABLE IF NOT EXISTS demo_catalog.bronze.sales_raw (
# MAGIC   transaction_id STRING,
# MAGIC   customer_id STRING,
# MAGIC   product_id STRING,
# MAGIC   amount DECIMAL(10,2),
# MAGIC   transaction_date DATE,
# MAGIC   region STRING,
# MAGIC   ingestion_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Raw sales transactions - @TRRaveendra';
# MAGIC
# MAGIC -- Insert sample data
# MAGIC INSERT INTO demo_catalog.bronze.sales_raw VALUES
# MAGIC   ('TXN001', 'C001', 'P001', 150.00, '2026-04-01', 'North', current_timestamp()),
# MAGIC   ('TXN002', 'C002', 'P002', 200.00, '2026-04-02', 'South', current_timestamp()),
# MAGIC   ('TXN003', 'C003', 'P001', 175.00, '2026-04-03', 'East', current_timestamp()),
# MAGIC   ('TXN004', 'C001', 'P003', 300.00, '2026-04-04', 'West', current_timestamp()),
# MAGIC   ('TXN005', 'C004', 'P002', 225.00, '2026-04-05', 'North', current_timestamp());
# MAGIC
# MAGIC -- Query the table
# MAGIC SELECT * FROM demo_catalog.bronze.sales_raw;

# COMMAND ----------

# DBTITLE 1,Verify Table with PySpark
# 🐍 PySpark: Read and Verify Table
# @TRRaveendra

# Read the table using Spark
df = spark.table("demo_catalog.bronze.sales_raw")

# Display data
print("\n📊 Sample Data:")
display(df)

# Show schema
print("\n📝 Table Schema:")
df.printSchema()

# Basic statistics
print("\n📊 Statistics:")
print(f"Total Records: {df.count()}")
print(f"Total Revenue: ${df.selectExpr('sum(amount) as total').collect()[0]['total']}")

# COMMAND ----------

# DBTITLE 1,Create Silver Layer Table
# MAGIC %sql
# MAGIC -- 🔨 Create Silver Layer Table (Cleaned Data)
# MAGIC -- @TRRaveendra
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS demo_catalog.silver.sales_cleaned (
# MAGIC   transaction_id STRING,
# MAGIC   customer_id STRING,
# MAGIC   product_id STRING,
# MAGIC   amount DECIMAL(10,2),
# MAGIC   transaction_date DATE,
# MAGIC   region STRING,
# MAGIC   processing_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Cleaned and validated sales data - @TRRaveendra';
# MAGIC
# MAGIC -- Transform and load from Bronze to Silver
# MAGIC INSERT INTO demo_catalog.silver.sales_cleaned
# MAGIC SELECT 
# MAGIC   transaction_id,
# MAGIC   customer_id,
# MAGIC   product_id,
# MAGIC   amount,
# MAGIC   transaction_date,
# MAGIC   UPPER(region) as region,  -- Standardize region names
# MAGIC   current_timestamp() as processing_timestamp
# MAGIC FROM demo_catalog.bronze.sales_raw
# MAGIC WHERE amount > 0  -- Data quality check
# MAGIC   AND transaction_date IS NOT NULL;
# MAGIC
# MAGIC SELECT * FROM demo_catalog.silver.sales_cleaned;

# COMMAND ----------

# DBTITLE 1,Section 4: Access Control (RBAC)
# MAGIC %md
# MAGIC # 🔐 Section 4: Access Control - RBAC (Role-Based Access Control)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC
# MAGIC Imagine a school:
# MAGIC - **Students** can read books from the library  
# MAGIC - **Teachers** can read books AND add new books  
# MAGIC - **Principal** can do everything AND decide who can access what  
# MAGIC
# MAGIC Unity Catalog works the same way — **you decide who can do what!**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect View:
# MAGIC
# MAGIC **Role-Based Access Control (RBAC)** in Unity Catalog uses **ANSI SQL standard GRANT/REVOKE** statements.
# MAGIC
# MAGIC ### Key Concepts:
# MAGIC
# MAGIC 1. **Principals** (Who?)
# MAGIC    - Users: `user@domain.com`
# MAGIC    - Groups: `data_analysts`, `data_engineers`
# MAGIC    - Service Principals: Apps and automation
# MAGIC
# MAGIC 2. **Securables** (What?)
# MAGIC    - Catalog, Schema, Table, View, Function
# MAGIC    - Volume, External Location, Storage Credential
# MAGIC
# MAGIC 3. **Privileges** (Can do what?)
# MAGIC    - `SELECT`, `INSERT`, `UPDATE`, `DELETE`
# MAGIC    - `CREATE`, `USE`, `MODIFY`, `READ_METADATA`
# MAGIC    - `ALL PRIVILEGES`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📜 GRANT Syntax:
# MAGIC
# MAGIC ```sql
# MAGIC GRANT <privilege> ON <securable_type> <securable_name> TO `<principal>`;
# MAGIC ```
# MAGIC
# MAGIC ## 🚫 REVOKE Syntax:
# MAGIC
# MAGIC ```sql
# MAGIC REVOKE <privilege> ON <securable_type> <securable_name> FROM `<principal>`;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌐 Privilege Hierarchy:
# MAGIC
# MAGIC ```
# MAGIC Catalog Level
# MAGIC   │
# MAGIC   ├─ USE CATALOG
# MAGIC   ├─ CREATE SCHEMA
# MAGIC   └─ USE SCHEMA (inherited to all schemas)
# MAGIC       │
# MAGIC       ├─ CREATE TABLE
# MAGIC       ├─ CREATE VIEW
# MAGIC       └─ SELECT/INSERT/UPDATE/DELETE (inherited to all tables)
# MAGIC ```
# MAGIC
# MAGIC **💡 Important**: Privileges are **NOT automatically inherited**. You must explicitly grant at each level.

# COMMAND ----------

# DBTITLE 1,Demo: Basic Access Control
# MAGIC %sql
# MAGIC -- 🔐 SECTION 4 DEMO: Access Control
# MAGIC -- @TRRaveendra
# MAGIC -- NOTE: These commands require admin privileges
# MAGIC
# MAGIC -- ========================================
# MAGIC -- SCENARIO 1: Grant Read-Only Access
# MAGIC -- ========================================
# MAGIC
# MAGIC -- Grant USE privilege on catalog (required to see the catalog)
# MAGIC -- GRANT USE CATALOG ON CATALOG demo_catalog TO `data_analysts`;
# MAGIC
# MAGIC -- Grant USE privilege on schema (required to see the schema)
# MAGIC -- GRANT USE SCHEMA ON SCHEMA demo_catalog.silver TO `data_analysts`;
# MAGIC
# MAGIC -- Grant SELECT privilege on specific table
# MAGIC -- GRANT SELECT ON TABLE demo_catalog.silver.sales_cleaned TO `data_analysts`;
# MAGIC
# MAGIC -- ========================================
# MAGIC -- SCENARIO 2: Grant Read-Write Access
# MAGIC -- ========================================
# MAGIC
# MAGIC -- For data engineers who need to insert/update data
# MAGIC -- GRANT SELECT, INSERT, MODIFY ON TABLE demo_catalog.silver.sales_cleaned TO `data_engineers`;
# MAGIC
# MAGIC -- ========================================
# MAGIC -- SCENARIO 3: Grant Full Access
# MAGIC -- ========================================
# MAGIC
# MAGIC -- For data owners who need full control
# MAGIC -- GRANT ALL PRIVILEGES ON SCHEMA demo_catalog.silver TO `data_owners`;
# MAGIC
# MAGIC -- ========================================
# MAGIC -- REVOKE Access
# MAGIC -- ========================================
# MAGIC
# MAGIC -- Remove access when no longer needed
# MAGIC -- REVOKE SELECT ON TABLE demo_catalog.silver.sales_cleaned FROM `data_analysts`;
# MAGIC
# MAGIC -- Display current grants on the table
# MAGIC SHOW GRANTS ON TABLE demo_catalog.silver.sales_cleaned;

# COMMAND ----------

# DBTITLE 1,Section 5: Privileges and Permissions
# MAGIC %md
# MAGIC # 📜 Section 5: Privileges & Permissions Deep Dive
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📦 Common Privileges by Level:
# MAGIC
# MAGIC ### 📚 CATALOG Level:
# MAGIC
# MAGIC | Privilege | Description | Use Case |
# MAGIC |-----------|-------------|----------|
# MAGIC | `USE CATALOG` | View and access catalog | Required for all users |
# MAGIC | `CREATE SCHEMA` | Create new schemas | Data architects |
# MAGIC | `USE SCHEMA` | Access all schemas | Broad access users |
# MAGIC | `ALL PRIVILEGES` | Full catalog control | Catalog owners |
# MAGIC
# MAGIC ### 📁 SCHEMA Level:
# MAGIC
# MAGIC | Privilege | Description | Use Case |
# MAGIC |-----------|-------------|----------|
# MAGIC | `USE SCHEMA` | View and access schema | Required for all users |
# MAGIC | `CREATE TABLE` | Create tables in schema | Data engineers |
# MAGIC | `CREATE VIEW` | Create views in schema | Analytics engineers |
# MAGIC | `CREATE FUNCTION` | Create UDFs | Advanced users |
# MAGIC | `ALL PRIVILEGES` | Full schema control | Schema owners |
# MAGIC
# MAGIC ### 📋 TABLE Level:
# MAGIC
# MAGIC | Privilege | Description | Use Case |
# MAGIC |-----------|-------------|----------|
# MAGIC | `SELECT` | Read data | Analysts, BI tools |
# MAGIC | `INSERT` | Add new rows | ETL pipelines |
# MAGIC | `UPDATE` | Modify existing rows | Data correction |
# MAGIC | `DELETE` | Remove rows | Data cleanup |
# MAGIC | `MODIFY` | Alter table structure | Data engineers |
# MAGIC | `READ_METADATA` | View table metadata | Discovery tools |
# MAGIC | `ALL PRIVILEGES` | Full table control | Table owners |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚠️ Least Privilege Principle:
# MAGIC
# MAGIC **Grant ONLY the minimum privileges required for a role to function.**
# MAGIC
# MAGIC ❌ **Bad Practice**:
# MAGIC ```sql
# MAGIC GRANT ALL PRIVILEGES ON CATALOG prod_catalog TO `data_analysts`;
# MAGIC ```
# MAGIC
# MAGIC ✅ **Good Practice**:
# MAGIC ```sql
# MAGIC GRANT USE CATALOG ON CATALOG prod_catalog TO `data_analysts`;
# MAGIC GRANT USE SCHEMA ON SCHEMA prod_catalog.gold TO `data_analysts`;
# MAGIC GRANT SELECT ON TABLE prod_catalog.gold.customer_analytics TO `data_analysts`;
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Demo: Comprehensive Access Control
# MAGIC %sql
# MAGIC -- 🔨 SECTION 5 DEMO: Comprehensive Privilege Management
# MAGIC -- @TRRaveendra
# MAGIC
# MAGIC -- ========================================
# MAGIC -- Create Gold Layer Aggregated Table
# MAGIC -- ========================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS demo_catalog.gold.sales_summary (
# MAGIC   region STRING,
# MAGIC   total_transactions BIGINT,
# MAGIC   total_revenue DECIMAL(10,2),
# MAGIC   avg_transaction_value DECIMAL(10,2),
# MAGIC   report_date DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Business-level sales analytics - @TRRaveendra';
# MAGIC
# MAGIC -- Populate gold table
# MAGIC INSERT INTO demo_catalog.gold.sales_summary
# MAGIC SELECT 
# MAGIC   region,
# MAGIC   COUNT(*) as total_transactions,
# MAGIC   SUM(amount) as total_revenue,
# MAGIC   AVG(amount) as avg_transaction_value,
# MAGIC   CURRENT_DATE() as report_date
# MAGIC FROM demo_catalog.silver.sales_cleaned
# MAGIC GROUP BY region;
# MAGIC
# MAGIC SELECT * FROM demo_catalog.gold.sales_summary;
# MAGIC
# MAGIC -- ========================================
# MAGIC -- Privilege Recommendations by Role
# MAGIC -- ========================================
# MAGIC
# MAGIC -- DATA ANALYSTS (Read-only on Gold)
# MAGIC -- GRANT USE CATALOG ON CATALOG demo_catalog TO `data_analysts`;
# MAGIC -- GRANT USE SCHEMA ON SCHEMA demo_catalog.gold TO `data_analysts`;
# MAGIC -- GRANT SELECT ON TABLE demo_catalog.gold.sales_summary TO `data_analysts`;
# MAGIC
# MAGIC -- DATA ENGINEERS (Read-Write on Silver/Gold)
# MAGIC -- GRANT USE CATALOG ON CATALOG demo_catalog TO `data_engineers`;
# MAGIC -- GRANT USE SCHEMA ON SCHEMA demo_catalog.silver TO `data_engineers`;
# MAGIC -- GRANT USE SCHEMA ON SCHEMA demo_catalog.gold TO `data_engineers`;
# MAGIC -- GRANT SELECT, INSERT, MODIFY ON SCHEMA demo_catalog.silver TO `data_engineers`;
# MAGIC -- GRANT SELECT, INSERT, MODIFY ON SCHEMA demo_catalog.gold TO `data_engineers`;
# MAGIC
# MAGIC -- DATA SCIENTISTS (Read across all layers)
# MAGIC -- GRANT USE CATALOG ON CATALOG demo_catalog TO `data_scientists`;
# MAGIC -- GRANT USE SCHEMA ON SCHEMA demo_catalog.bronze TO `data_scientists`;
# MAGIC -- GRANT USE SCHEMA ON SCHEMA demo_catalog.silver TO `data_scientists`;
# MAGIC -- GRANT USE SCHEMA ON SCHEMA demo_catalog.gold TO `data_scientists`;
# MAGIC -- GRANT SELECT ON SCHEMA demo_catalog.bronze TO `data_scientists`;
# MAGIC -- GRANT SELECT ON SCHEMA demo_catalog.silver TO `data_scientists`;
# MAGIC -- GRANT SELECT ON SCHEMA demo_catalog.gold TO `data_scientists`;

# COMMAND ----------

# DBTITLE 1,Section 6: Hands-on Governance Setup
# MAGIC %md
# MAGIC # 🛠️ Section 6: Hands-on Governance Setup
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Scenario: Setting Up Governed Analytics Environment
# MAGIC
# MAGIC **Business Requirement**:
# MAGIC Create a governed environment where:
# MAGIC 1. Data Engineers can ingest and transform data
# MAGIC 2. Data Analysts can only read final analytics tables
# MAGIC 3. All access is tracked and auditable
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Step-by-Step Implementation:
# MAGIC
# MAGIC ### Step 1: Create Organizational Structure
# MAGIC - Create catalog for the department
# MAGIC - Create schemas for different data layers
# MAGIC
# MAGIC ### Step 2: Create Data Assets
# MAGIC - Create tables with proper metadata
# MAGIC - Add business-friendly comments
# MAGIC
# MAGIC ### Step 3: Configure Access Control
# MAGIC - Define user groups
# MAGIC - Grant appropriate privileges
# MAGIC - Follow least privilege principle
# MAGIC
# MAGIC ### Step 4: Validate Permissions
# MAGIC - Test access with different roles
# MAGIC - Verify audit logs
# MAGIC
# MAGIC ### Step 5: Document & Monitor
# MAGIC - Document access patterns
# MAGIC - Set up alerts for unusual access

# COMMAND ----------

# DBTITLE 1,Hands-on: Complete Governance Setup
# MAGIC %sql
# MAGIC -- 🛠️ SECTION 6 HANDS-ON: Complete Governance Implementation
# MAGIC -- @TRRaveendra
# MAGIC
# MAGIC -- ========================================
# MAGIC -- STEP 1: Create Organizational Structure
# MAGIC -- ========================================
# MAGIC
# MAGIC -- Create department catalog
# MAGIC CREATE CATALOG IF NOT EXISTS finance_catalog
# MAGIC COMMENT 'Finance department data - Governed by CFO office - @TRRaveendra';
# MAGIC
# MAGIC USE CATALOG finance_catalog;
# MAGIC
# MAGIC -- Create schemas for data layers
# MAGIC CREATE SCHEMA IF NOT EXISTS raw_data
# MAGIC COMMENT 'Raw financial data from source systems';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS curated_data
# MAGIC COMMENT 'Cleaned and validated financial data';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS analytics
# MAGIC COMMENT 'Business-ready financial analytics';
# MAGIC
# MAGIC -- ========================================
# MAGIC -- STEP 2: Create Data Assets
# MAGIC -- ========================================
# MAGIC
# MAGIC -- Raw layer: Transaction data
# MAGIC CREATE TABLE IF NOT EXISTS finance_catalog.raw_data.transactions (
# MAGIC   transaction_id STRING,
# MAGIC   account_id STRING,
# MAGIC   amount DECIMAL(18,2),
# MAGIC   transaction_type STRING,
# MAGIC   transaction_timestamp TIMESTAMP,
# MAGIC   source_system STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Raw financial transactions - PII/Sensitive';
# MAGIC
# MAGIC -- Curated layer: Validated transactions
# MAGIC CREATE TABLE IF NOT EXISTS finance_catalog.curated_data.validated_transactions (
# MAGIC   transaction_id STRING,
# MAGIC   account_id STRING,
# MAGIC   amount DECIMAL(18,2),
# MAGIC   transaction_type STRING,
# MAGIC   transaction_date DATE,
# MAGIC   validation_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Validated financial transactions';
# MAGIC
# MAGIC -- Analytics layer: Monthly summary
# MAGIC CREATE TABLE IF NOT EXISTS finance_catalog.analytics.monthly_summary (
# MAGIC   year_month STRING,
# MAGIC   transaction_type STRING,
# MAGIC   transaction_count BIGINT,
# MAGIC   total_amount DECIMAL(18,2),
# MAGIC   avg_amount DECIMAL(18,2)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Monthly financial summary - Safe for analysts';
# MAGIC
# MAGIC -- Insert sample data
# MAGIC INSERT INTO finance_catalog.raw_data.transactions VALUES
# MAGIC   ('FIN001', 'ACC001', 5000.00, 'CREDIT', '2026-04-21 10:00:00', 'ERP'),
# MAGIC   ('FIN002', 'ACC002', 3000.00, 'DEBIT', '2026-04-21 11:00:00', 'ERP'),
# MAGIC   ('FIN003', 'ACC001', 2000.00, 'CREDIT', '2026-04-21 12:00:00', 'ERP');
# MAGIC
# MAGIC -- Transform to curated
# MAGIC INSERT INTO finance_catalog.curated_data.validated_transactions
# MAGIC SELECT 
# MAGIC   transaction_id,
# MAGIC   account_id,
# MAGIC   amount,
# MAGIC   transaction_type,
# MAGIC   CAST(transaction_timestamp AS DATE) as transaction_date,
# MAGIC   current_timestamp() as validation_timestamp
# MAGIC FROM finance_catalog.raw_data.transactions
# MAGIC WHERE amount > 0;
# MAGIC
# MAGIC -- Aggregate to analytics
# MAGIC INSERT INTO finance_catalog.analytics.monthly_summary
# MAGIC SELECT 
# MAGIC   DATE_FORMAT(transaction_date, 'yyyy-MM') as year_month,
# MAGIC   transaction_type,
# MAGIC   COUNT(*) as transaction_count,
# MAGIC   SUM(amount) as total_amount,
# MAGIC   AVG(amount) as avg_amount
# MAGIC FROM finance_catalog.curated_data.validated_transactions
# MAGIC GROUP BY DATE_FORMAT(transaction_date, 'yyyy-MM'), transaction_type;
# MAGIC
# MAGIC SELECT * FROM finance_catalog.analytics.monthly_summary;

# COMMAND ----------

# DBTITLE 1,Configure Role-Based Access
# MAGIC %sql
# MAGIC -- 🔐 SECTION 6: Configure Role-Based Access Control
# MAGIC -- @TRRaveendra
# MAGIC
# MAGIC -- ========================================
# MAGIC -- ROLE 1: Finance Data Engineers
# MAGIC -- ========================================
# MAGIC -- Privileges: Full access to raw and curated layers
# MAGIC
# MAGIC -- GRANT USE CATALOG ON CATALOG finance_catalog TO `finance_data_engineers`;
# MAGIC -- GRANT USE SCHEMA ON SCHEMA finance_catalog.raw_data TO `finance_data_engineers`;
# MAGIC -- GRANT USE SCHEMA ON SCHEMA finance_catalog.curated_data TO `finance_data_engineers`;
# MAGIC -- GRANT ALL PRIVILEGES ON SCHEMA finance_catalog.raw_data TO `finance_data_engineers`;
# MAGIC -- GRANT ALL PRIVILEGES ON SCHEMA finance_catalog.curated_data TO `finance_data_engineers`;
# MAGIC
# MAGIC -- ========================================
# MAGIC -- ROLE 2: Finance Analysts
# MAGIC -- ========================================
# MAGIC -- Privileges: Read-only access to analytics layer ONLY
# MAGIC
# MAGIC -- GRANT USE CATALOG ON CATALOG finance_catalog TO `finance_analysts`;
# MAGIC -- GRANT USE SCHEMA ON SCHEMA finance_catalog.analytics TO `finance_analysts`;
# MAGIC -- GRANT SELECT ON SCHEMA finance_catalog.analytics TO `finance_analysts`;
# MAGIC
# MAGIC -- ========================================
# MAGIC -- ROLE 3: Finance Managers
# MAGIC -- ========================================
# MAGIC -- Privileges: Read access across all layers for oversight
# MAGIC
# MAGIC -- GRANT USE CATALOG ON CATALOG finance_catalog TO `finance_managers`;
# MAGIC -- GRANT USE SCHEMA ON SCHEMA finance_catalog.raw_data TO `finance_managers`;
# MAGIC -- GRANT USE SCHEMA ON SCHEMA finance_catalog.curated_data TO `finance_managers`;
# MAGIC -- GRANT USE SCHEMA ON SCHEMA finance_catalog.analytics TO `finance_managers`;
# MAGIC -- GRANT SELECT ON SCHEMA finance_catalog.raw_data TO `finance_managers`;
# MAGIC -- GRANT SELECT ON SCHEMA finance_catalog.curated_data TO `finance_managers`;
# MAGIC -- GRANT SELECT ON SCHEMA finance_catalog.analytics TO `finance_managers`;
# MAGIC
# MAGIC -- ========================================
# MAGIC -- Validate Current Permissions
# MAGIC -- ========================================
# MAGIC
# MAGIC -- Check grants on analytics table
# MAGIC SHOW GRANTS ON TABLE finance_catalog.analytics.monthly_summary;
# MAGIC
# MAGIC -- Check grants on analytics schema
# MAGIC SHOW GRANTS ON SCHEMA finance_catalog.analytics;

# COMMAND ----------

# DBTITLE 1,Validate Governance Implementation
# ✅ SECTION 6: Validate Governance Setup
# @TRRaveendra

# Verify catalog structure
print("📚 Catalogs:")
display(spark.sql("SHOW CATALOGS"))

print("\n📁 Schemas in finance_catalog:")
display(spark.sql("SHOW SCHEMAS IN finance_catalog"))

print("\n📋 Tables in analytics schema:")
display(spark.sql("SHOW TABLES IN finance_catalog.analytics"))

# Verify data lineage
print("\n🔍 Data Flow Verification:")
raw_count = spark.table("finance_catalog.raw_data.transactions").count()
curated_count = spark.table("finance_catalog.curated_data.validated_transactions").count()
analytics_count = spark.table("finance_catalog.analytics.monthly_summary").count()

print(f"Raw Layer: {raw_count} transactions")
print(f"Curated Layer: {curated_count} validated transactions")
print(f"Analytics Layer: {analytics_count} summary records")

# Display final analytics
print("\n📊 Final Analytics View:")
display(spark.table("finance_catalog.analytics.monthly_summary"))

# COMMAND ----------

# DBTITLE 1,Section 7: Governance Best Practices
# MAGIC %md
# MAGIC # ⭐ Section 7: Data Governance Best Practices
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1️₃ Least Privilege Principle
# MAGIC
# MAGIC **📌 Rule**: Grant only the minimum permissions required for a user to perform their job.
# MAGIC
# MAGIC ### Implementation:
# MAGIC - Start with NO access
# MAGIC - Grant permissions incrementally
# MAGIC - Review and revoke unused permissions regularly
# MAGIC - Use groups instead of individual users
# MAGIC
# MAGIC ```sql
# MAGIC -- ❌ Bad: Over-permissioning
# MAGIC GRANT ALL PRIVILEGES ON CATALOG prod_catalog TO `analyst@company.com`;
# MAGIC
# MAGIC -- ✅ Good: Specific permissions
# MAGIC GRANT USE CATALOG ON CATALOG prod_catalog TO `analysts_group`;
# MAGIC GRANT USE SCHEMA ON SCHEMA prod_catalog.gold TO `analysts_group`;
# MAGIC GRANT SELECT ON TABLE prod_catalog.gold.sales_dashboard TO `analysts_group`;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2️₃ Role-Based Access Control (RBAC)
# MAGIC
# MAGIC **📌 Rule**: Organize users into groups based on their roles, not individuals.
# MAGIC
# MAGIC ### Common Role Structure:
# MAGIC
# MAGIC ```
# MAGIC 👥 User Groups:
# MAGIC   ├─ data_engineers        (Build pipelines)
# MAGIC   ├─ data_analysts         (Query gold tables)
# MAGIC   ├─ data_scientists       (ML experiments)
# MAGIC   ├─ business_users        (Dashboards only)
# MAGIC   ├─ data_stewards         (Governance)
# MAGIC   └─ admins                (Full access)
# MAGIC ```
# MAGIC
# MAGIC ### Benefits:
# MAGIC - Easier to manage (modify group, not 100 users)
# MAGIC - Consistent permissions across team
# MAGIC - Clear audit trail
# MAGIC - Easier onboarding/offboarding
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3️₃ Environment Separation
# MAGIC
# MAGIC **📌 Rule**: Use separate catalogs for dev, test, and production.
# MAGIC
# MAGIC ```
# MAGIC 🏛️ Catalog Structure:
# MAGIC   ├─ dev_catalog          (Developers have full access)
# MAGIC   ├─ test_catalog         (QA team access)
# MAGIC   └─ prod_catalog         (Restricted, read-only for most)
# MAGIC ```
# MAGIC
# MAGIC ### Benefits:
# MAGIC - Prevent accidental production changes
# MAGIC - Enable safe experimentation in dev
# MAGIC - Clear promotion path (dev → test → prod)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4️₃ Metadata Documentation
# MAGIC
# MAGIC **📌 Rule**: Always add meaningful comments to all objects.
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE catalog.schema.table (...)
# MAGIC COMMENT 'Customer transactions | Owner: Finance Team | PII: Yes | Updated: Daily';
# MAGIC ```
# MAGIC
# MAGIC ### Include:
# MAGIC - Purpose of the data
# MAGIC - Data owner/team
# MAGIC - Sensitivity level (PII, confidential, public)
# MAGIC - Update frequency
# MAGIC - Contact for questions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5️₃ Naming Conventions
# MAGIC
# MAGIC **📌 Rule**: Use consistent, descriptive naming.
# MAGIC
# MAGIC ```
# MAGIC ✅ Good Naming:
# MAGIC   prod_catalog.gold_sales.customer_lifetime_value
# MAGIC   prod_catalog.silver_finance.validated_transactions
# MAGIC   dev_catalog.bronze_logs.raw_application_events
# MAGIC
# MAGIC ❌ Bad Naming:
# MAGIC   catalog1.schema2.table3
# MAGIC   my_data.temp.test_table
# MAGIC   prod.data.final_final_v2
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 6️₃ Regular Access Audits
# MAGIC
# MAGIC **📌 Rule**: Review and audit permissions quarterly.
# MAGIC
# MAGIC ### Audit Checklist:
# MAGIC - [ ] Who has access to sensitive data?
# MAGIC - [ ] Are there any orphaned accounts?
# MAGIC - [ ] Are service principals properly secured?
# MAGIC - [ ] Are there any excessive privileges?
# MAGIC - [ ] Is audit logging enabled?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 7️₃ Data Classification
# MAGIC
# MAGIC **📌 Rule**: Tag data by sensitivity level.
# MAGIC
# MAGIC | Level | Example Data | Access |
# MAGIC |-------|-------------|--------|
# MAGIC | 🔴 Critical | SSN, Credit Cards | Highly restricted |
# MAGIC | 🟡 Sensitive | Email, Phone | Restricted |
# MAGIC | 🟢 Internal | Sales figures | Internal only |
# MAGIC | ⚪ Public | Product catalog | Open |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 8️▃ Avoid Common Anti-Patterns
# MAGIC
# MAGIC ### ❌ Anti-Pattern 1: Shared Admin Accounts
# MAGIC ```sql
# MAGIC -- Don't create generic admin users
# MAGIC CREATE USER admin@company.com;
# MAGIC GRANT ALL PRIVILEGES TO admin@company.com;
# MAGIC ```
# MAGIC
# MAGIC ### ❌ Anti-Pattern 2: Hardcoded Usernames
# MAGIC ```sql
# MAGIC -- Don't grant to individuals
# MAGIC GRANT SELECT ON TABLE ... TO `john.doe@company.com`;
# MAGIC ```
# MAGIC
# MAGIC ### ❌ Anti-Pattern 3: No Documentation
# MAGIC ```sql
# MAGIC -- Don't skip comments
# MAGIC CREATE TABLE catalog.schema.table (...);  -- No comment!
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 8: Unity Catalog + Medallion Architecture
# MAGIC %md
# MAGIC # 🏅 Section 8: Unity Catalog + Medallion Architecture Integration
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC
# MAGIC Imagine organizing your toys:
# MAGIC - **Bronze Box** 📦 = All toys dumped in (messy, but everything is there)  
# MAGIC - **Silver Box** 📦 = Toys cleaned and sorted  
# MAGIC - **Gold Box** 🎁 = Only the best toys, ready to play  
# MAGIC
# MAGIC Unity Catalog puts labels on each box so only the right people can open them!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Medallion Architecture with Unity Catalog:
# MAGIC
# MAGIC ```
# MAGIC 🏛️ prod_catalog
# MAGIC     │
# MAGIC     ├─🟤 bronze (Raw/Landing Zone)
# MAGIC     │    ├─ logs_raw
# MAGIC     │    ├─ transactions_raw
# MAGIC     │    └─ events_raw
# MAGIC     │    Access: Data Engineers ONLY
# MAGIC     │
# MAGIC     ├─🥈 silver (Cleaned/Validated)
# MAGIC     │    ├─ logs_cleaned
# MAGIC     │    ├─ transactions_validated
# MAGIC     │    └─ events_enriched
# MAGIC     │    Access: Data Engineers + Data Scientists
# MAGIC     │
# MAGIC     └─🥇 gold (Business/Analytics)
# MAGIC          ├─ customer_360
# MAGIC          ├─ sales_dashboard
# MAGIC          └─ revenue_analytics
# MAGIC          Access: Everyone (Read-Only)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Access Control Strategy:
# MAGIC
# MAGIC ### Bronze Layer (Raw Data)
# MAGIC - **Purpose**: Store raw, unprocessed data
# MAGIC - **Access**: Data Engineers (Read/Write)
# MAGIC - **Reason**: Contains PII, duplicates, and data quality issues
# MAGIC
# MAGIC ### Silver Layer (Curated Data)
# MAGIC - **Purpose**: Cleaned, deduplicated, validated data
# MAGIC - **Access**: Data Engineers (Read/Write) + Data Scientists (Read)
# MAGIC - **Reason**: Still technical, but safe for advanced users
# MAGIC
# MAGIC ### Gold Layer (Business Data)
# MAGIC - **Purpose**: Aggregated, business-ready analytics
# MAGIC - **Access**: All users (Read-Only)
# MAGIC - **Reason**: Safe, aggregated, no PII exposure
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛡️ Security Benefits:
# MAGIC
# MAGIC 1. **Data Isolation**
# MAGIC    - Sensitive raw data isolated in Bronze
# MAGIC    - Only cleaned data exposed to analysts
# MAGIC
# MAGIC 2. **Progressive Access**
# MAGIC    - Junior analysts: Gold only
# MAGIC    - Senior analysts: Silver + Gold
# MAGIC    - Engineers: All layers
# MAGIC
# MAGIC 3. **Compliance**
# MAGIC    - PII stays in Bronze/Silver
# MAGIC    - Gold contains aggregated, anonymized data
# MAGIC    - Easy to prove compliance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Naming Convention:
# MAGIC
# MAGIC ```sql
# MAGIC <environment>_catalog.<layer>_<domain>.<entity>_<state>
# MAGIC
# MAGIC Examples:
# MAGIC prod_catalog.bronze_sales.transactions_raw
# MAGIC prod_catalog.silver_sales.transactions_cleaned
# MAGIC prod_catalog.gold_sales.revenue_summary
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Demo: Medallion with Governance
# MAGIC %sql
# MAGIC -- 🏅 SECTION 8 DEMO: Medallion Architecture with Unity Catalog
# MAGIC -- @TRRaveendra
# MAGIC
# MAGIC -- ========================================
# MAGIC -- Create Medallion Structure
# MAGIC -- ========================================
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS medallion_demo
# MAGIC COMMENT 'Medallion architecture demonstration - @TRRaveendra';
# MAGIC
# MAGIC USE CATALOG medallion_demo;
# MAGIC
# MAGIC -- Bronze Layer
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze
# MAGIC COMMENT 'Bronze - Raw data landing zone';
# MAGIC
# MAGIC -- Silver Layer
# MAGIC CREATE SCHEMA IF NOT EXISTS silver
# MAGIC COMMENT 'Silver - Cleaned and validated data';
# MAGIC
# MAGIC -- Gold Layer
# MAGIC CREATE SCHEMA IF NOT EXISTS gold
# MAGIC COMMENT 'Gold - Business-ready analytics';
# MAGIC
# MAGIC -- ========================================
# MAGIC -- Bronze: Raw Customer Data (with PII)
# MAGIC -- ========================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS medallion_demo.bronze.customers_raw (
# MAGIC   customer_id STRING,
# MAGIC   email STRING,                    -- PII
# MAGIC   phone STRING,                    -- PII
# MAGIC   full_name STRING,                -- PII
# MAGIC   signup_date DATE,
# MAGIC   country STRING,
# MAGIC   ingestion_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT '🟤 BRONZE: Raw customer data - Contains PII - RESTRICTED';
# MAGIC
# MAGIC -- Insert sample data
# MAGIC INSERT INTO medallion_demo.bronze.customers_raw VALUES
# MAGIC   ('C001', 'john.doe@email.com', '+1-555-0101', 'John Doe', '2026-01-15', 'USA', current_timestamp()),
# MAGIC   ('C002', 'jane.smith@email.com', '+1-555-0102', 'Jane Smith', '2026-02-20', 'Canada', current_timestamp()),
# MAGIC   ('C003', 'bob.wilson@email.com', '+1-555-0103', 'Bob Wilson', '2026-03-10', 'USA', current_timestamp());
# MAGIC
# MAGIC -- ========================================
# MAGIC -- Silver: Cleaned Customer Data (PII masked)
# MAGIC -- ========================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS medallion_demo.silver.customers_cleaned (
# MAGIC   customer_id STRING,
# MAGIC   email_domain STRING,             -- Derived, not PII
# MAGIC   country STRING,
# MAGIC   signup_year INT,
# MAGIC   signup_month INT,
# MAGIC   processing_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT '🥈 SILVER: Cleaned customer data - PII removed';
# MAGIC
# MAGIC -- Transform: Remove PII, extract features
# MAGIC INSERT INTO medallion_demo.silver.customers_cleaned
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   SUBSTRING_INDEX(email, '@', -1) as email_domain,  -- Extract domain only
# MAGIC   country,
# MAGIC   YEAR(signup_date) as signup_year,
# MAGIC   MONTH(signup_date) as signup_month,
# MAGIC   current_timestamp() as processing_timestamp
# MAGIC FROM medallion_demo.bronze.customers_raw;
# MAGIC
# MAGIC -- ========================================
# MAGIC -- Gold: Customer Analytics (Aggregated)
# MAGIC -- ========================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS medallion_demo.gold.customer_analytics (
# MAGIC   country STRING,
# MAGIC   signup_year INT,
# MAGIC   customer_count BIGINT,
# MAGIC   top_email_domain STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT '🥇 GOLD: Customer analytics - Safe for all users';
# MAGIC
# MAGIC -- Aggregate for business users
# MAGIC INSERT INTO medallion_demo.gold.customer_analytics
# MAGIC SELECT 
# MAGIC   country,
# MAGIC   signup_year,
# MAGIC   COUNT(DISTINCT customer_id) as customer_count,
# MAGIC   MODE(email_domain) as top_email_domain
# MAGIC FROM medallion_demo.silver.customers_cleaned
# MAGIC GROUP BY country, signup_year;
# MAGIC
# MAGIC SELECT * FROM medallion_demo.gold.customer_analytics;
# MAGIC
# MAGIC -- ========================================
# MAGIC -- Access Control by Layer
# MAGIC -- ========================================
# MAGIC
# MAGIC -- Bronze: Engineers ONLY
# MAGIC -- GRANT USE CATALOG ON CATALOG medallion_demo TO `data_engineers`;
# MAGIC -- GRANT USE SCHEMA ON SCHEMA medallion_demo.bronze TO `data_engineers`;
# MAGIC -- GRANT ALL PRIVILEGES ON SCHEMA medallion_demo.bronze TO `data_engineers`;
# MAGIC
# MAGIC -- Silver: Engineers + Scientists
# MAGIC -- GRANT USE SCHEMA ON SCHEMA medallion_demo.silver TO `data_scientists`;
# MAGIC -- GRANT SELECT ON SCHEMA medallion_demo.silver TO `data_scientists`;
# MAGIC
# MAGIC -- Gold: Everyone (Read-Only)
# MAGIC -- GRANT USE CATALOG ON CATALOG medallion_demo TO `all_users`;
# MAGIC -- GRANT USE SCHEMA ON SCHEMA medallion_demo.gold TO `all_users`;
# MAGIC -- GRANT SELECT ON SCHEMA medallion_demo.gold TO `all_users`;

# COMMAND ----------

# DBTITLE 1,Section 9: End-to-End Governance Architecture
# MAGIC %md
# MAGIC # 🏛️ Section 9: End-to-End Governance Architecture
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔍 Complete Governance Flow:
# MAGIC
# MAGIC ```
# MAGIC 👥 USERS & APPLICATIONS
# MAGIC           │
# MAGIC           │ (Authentication)
# MAGIC           ↓
# MAGIC     [🔐 Identity Provider]
# MAGIC       (Azure AD / Okta / AWS IAM)
# MAGIC           │
# MAGIC           │ (Authorization)
# MAGIC           ↓
# MAGIC     [🛡️ Unity Catalog]
# MAGIC       ├─ Check User Groups
# MAGIC       ├─ Verify Privileges
# MAGIC       ├─ Log Access
# MAGIC       └─ Apply Row/Column Filters
# MAGIC           │
# MAGIC           │ (Data Access)
# MAGIC           ↓
# MAGIC     [💾 Data Storage]
# MAGIC       ├─ Delta Tables
# MAGIC       ├─ Parquet Files
# MAGIC       └─ External Sources
# MAGIC           │
# MAGIC           ↓
# MAGIC     [📊 Analytics & AI]
# MAGIC       ├─ SQL Queries
# MAGIC       ├─ Dashboards
# MAGIC       ├─ ML Models
# MAGIC       └─ BI Tools
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔑 Key Components:
# MAGIC
# MAGIC ### 1️▃ Authentication Layer
# MAGIC - **Who are you?**
# MAGIC - SSO integration (Azure AD, Okta, Google)
# MAGIC - Service principal authentication for apps
# MAGIC - Multi-factor authentication (MFA)
# MAGIC
# MAGIC ### 2️▃ Authorization Layer (Unity Catalog)
# MAGIC - **What can you do?**
# MAGIC - GRANT/REVOKE privileges
# MAGIC - Group-based access control
# MAGIC - Dynamic views with row-level security
# MAGIC
# MAGIC ### 3️▃ Audit Layer
# MAGIC - **What did you do?**
# MAGIC - System tables: `system.access.audit`
# MAGIC - Track all data access
# MAGIC - Compliance reporting
# MAGIC
# MAGIC ### 4️▃ Data Masking Layer
# MAGIC - **What can you see?**
# MAGIC - Column masking functions
# MAGIC - Row-level filters
# MAGIC - Dynamic data masking
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔍 Data Access Journey:
# MAGIC
# MAGIC ### Example: Data Analyst queries customer data
# MAGIC
# MAGIC ```
# MAGIC 1. 👤 User: analyst@company.com logs in
# MAGIC    → Authenticated via Azure AD
# MAGIC
# MAGIC 2. 📝 User runs: SELECT * FROM prod.gold.customers
# MAGIC    → Unity Catalog checks:
# MAGIC       • Does user have USE CATALOG on 'prod'? ✅
# MAGIC       • Does user have USE SCHEMA on 'prod.gold'? ✅
# MAGIC       • Does user have SELECT on 'prod.gold.customers'? ✅
# MAGIC       • Are there row filters? ✅ (only show US customers)
# MAGIC       • Are there column masks? ✅ (mask email field)
# MAGIC
# MAGIC 3. 📊 Query executes with applied filters
# MAGIC    → User sees only authorized subset
# MAGIC
# MAGIC 4. 📝 Audit log created
# MAGIC    → Recorded: WHO, WHAT, WHEN, WHERE
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌐 Multi-Environment Architecture:
# MAGIC
# MAGIC ```
# MAGIC 🏛️ PRODUCTION
# MAGIC   prod_catalog
# MAGIC     ├─ bronze (Raw)
# MAGIC     ├─ silver (Curated)
# MAGIC     └─ gold (Analytics)
# MAGIC   Access: Read-only for most users
# MAGIC
# MAGIC 🧪 TEST
# MAGIC   test_catalog
# MAGIC     ├─ bronze
# MAGIC     ├─ silver
# MAGIC     └─ gold
# MAGIC   Access: QA team read-write
# MAGIC
# MAGIC 🛠️ DEVELOPMENT
# MAGIC   dev_catalog
# MAGIC     ├─ bronze
# MAGIC     ├─ silver
# MAGIC     └─ gold
# MAGIC   Access: Developers full control
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔒 Governance Checklist:
# MAGIC
# MAGIC - [ ] **Identity Management**: SSO configured
# MAGIC - [ ] **Access Control**: RBAC groups defined
# MAGIC - [ ] **Data Classification**: Sensitive data tagged
# MAGIC - [ ] **Audit Logging**: Enabled and monitored
# MAGIC - [ ] **Data Masking**: PII protected
# MAGIC - [ ] **Environment Separation**: Dev/Test/Prod isolated
# MAGIC - [ ] **Documentation**: All objects commented
# MAGIC - [ ] **Regular Reviews**: Quarterly access audits
# MAGIC - [ ] **Incident Response**: Process defined
# MAGIC - [ ] **Compliance**: GDPR/HIPAA requirements met

# COMMAND ----------

# DBTITLE 1,Verify Governance Architecture
# 🔍 SECTION 9 DEMO: Verify Governance Architecture
# @TRRaveendra

# Display all catalogs in the environment
print("🏛️ All Catalogs:")
display(spark.sql("SHOW CATALOGS"))

# Check schemas across catalogs
print("\n📁 Governance Structure:")
for catalog in ['demo_catalog', 'finance_catalog', 'medallion_demo']:
    try:
        print(f"\n📚 {catalog}:")
        schemas_df = spark.sql(f"SHOW SCHEMAS IN {catalog}")
        display(schemas_df)
    except:
        print(f"  ⚠️ Catalog {catalog} not accessible or doesn't exist")

# Verify medallion architecture
print("\n🏅 Medallion Architecture Verification:")
try:
    bronze_tables = spark.sql("SHOW TABLES IN medallion_demo.bronze")
    silver_tables = spark.sql("SHOW TABLES IN medallion_demo.silver")
    gold_tables = spark.sql("SHOW TABLES IN medallion_demo.gold")
    
    print(f"🟤 Bronze Layer: {bronze_tables.count()} tables")
    print(f"🥈 Silver Layer: {silver_tables.count()} tables")
    print(f"🥇 Gold Layer: {gold_tables.count()} tables")
    
    print("\n🥇 Gold Layer Tables (Business-Ready):")
    display(gold_tables)
except Exception as e:
    print(f"⚠️ Error: {e}")

# Sample governance metadata
print("\n📝 Governance Metadata Example:")
try:
    metadata_query = """
    SELECT 
      'medallion_demo' as catalog_name,
      'gold' as schema_name,
      'customer_analytics' as table_name,
      'Aggregated customer data - Safe for all users' as purpose,
      'Public' as classification,
      'Data Engineering Team' as owner,
      'Daily' as update_frequency
    """
    display(spark.sql(metadata_query))
except Exception as e:
    print(f"⚠️ Error: {e}")

# COMMAND ----------

# DBTITLE 1,Summary and Key Learnings
# MAGIC %md
# MAGIC # 🎓 Summary and Key Learnings
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔑 Key Takeaways:
# MAGIC
# MAGIC 1. **Unity Catalog** is the centralized governance solution for Databricks
# MAGIC 2. **Three-tier hierarchy**: Catalog → Schema → Table
# MAGIC 3. **RBAC** using GRANT/REVOKE for fine-grained access control
# MAGIC 4. **Medallion Architecture** + Unity Catalog = Governed data pipeline
# MAGIC 5. **Least Privilege** principle is critical for security
# MAGIC 6. **Environment Separation** (dev/test/prod) prevents accidents
# MAGIC 7. **Audit Logging** provides compliance and security monitoring
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 What You've Learned:
# MAGIC
# MAGIC ✅ How to create and organize catalogs, schemas, and tables  
# MAGIC ✅ How to implement role-based access control  
# MAGIC ✅ How to apply the least privilege principle  
# MAGIC ✅ How to integrate governance with medallion architecture  
# MAGIC ✅ How to set up a complete governed environment  
# MAGIC ✅ How to validate and audit permissions  
# MAGIC ✅ Best practices for enterprise data governance  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Quick Reference Commands:
# MAGIC
# MAGIC ```sql
# MAGIC -- Create governance structure
# MAGIC CREATE CATALOG <name>;
# MAGIC CREATE SCHEMA <catalog>.<name>;
# MAGIC CREATE TABLE <catalog>.<schema>.<name> USING DELTA;
# MAGIC
# MAGIC -- Grant permissions
# MAGIC GRANT USE CATALOG ON CATALOG <name> TO `<principal>`;
# MAGIC GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<principal>`;
# MAGIC GRANT SELECT ON TABLE <catalog>.<schema>.<table> TO `<principal>`;
# MAGIC
# MAGIC -- Revoke permissions
# MAGIC REVOKE <privilege> ON <object> FROM `<principal>`;
# MAGIC
# MAGIC -- View permissions
# MAGIC SHOW GRANTS ON TABLE <catalog>.<schema>.<table>;
# MAGIC
# MAGIC -- View catalogs/schemas/tables
# MAGIC SHOW CATALOGS;
# MAGIC SHOW SCHEMAS IN <catalog>;
# MAGIC SHOW TABLES IN <catalog>.<schema>;
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Interview Questions
# MAGIC %md
# MAGIC # 📝 Interview Questions - Unity Catalog Governance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔴 Fundamental Questions:
# MAGIC
# MAGIC ### 1. What is Unity Catalog and why is it important?
# MAGIC **Answer**: Unity Catalog is Databricks' unified governance solution that provides centralized metadata management, fine-grained access control, data lineage, and audit logging across all data and AI assets. It's important because it enables enterprise-grade governance, compliance, and security in a multi-cloud environment.
# MAGIC
# MAGIC ### 2. Explain the Unity Catalog hierarchy.
# MAGIC **Answer**: Unity Catalog has a three-tier hierarchy:
# MAGIC - **Catalog** (top level) - represents an environment or business domain
# MAGIC - **Schema** (mid level) - logical grouping within a catalog
# MAGIC - **Table/View/Function** (bottom level) - actual data assets
# MAGIC Fully qualified name: `catalog.schema.table`
# MAGIC
# MAGIC ### 3. What is the difference between managed and external tables in Unity Catalog?
# MAGIC **Answer**: 
# MAGIC - **Managed tables**: Unity Catalog manages both metadata and data. When dropped, both are deleted. Data stored in UC-managed location.
# MAGIC - **External tables**: Unity Catalog manages only metadata. When dropped, data remains in external location (S3/ADLS/GCS).
# MAGIC
# MAGIC ### 4. What is the Least Privilege Principle?
# MAGIC **Answer**: Grant only the minimum permissions required for a user/group to perform their job. Start with no access and add permissions incrementally. This minimizes security risks and blast radius of compromised accounts.
# MAGIC
# MAGIC ### 5. Explain RBAC in Unity Catalog.
# MAGIC **Answer**: Role-Based Access Control (RBAC) organizes users into groups based on their roles (e.g., data_engineers, data_analysts) and grants permissions to groups rather than individuals. This simplifies management, ensures consistency, and improves security.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🟡 Intermediate Questions:
# MAGIC
# MAGIC ### 6. How do you implement medallion architecture with Unity Catalog governance?
# MAGIC **Answer**: Create separate schemas for each layer:
# MAGIC - **Bronze** (raw data): Access only for data engineers
# MAGIC - **Silver** (cleaned): Access for engineers and data scientists
# MAGIC - **Gold** (analytics): Read-only for all users
# MAGIC Use GRANT statements to control access at each layer based on data sensitivity.
# MAGIC
# MAGIC ### 7. What privileges are required to query a table in Unity Catalog?
# MAGIC **Answer**: 
# MAGIC 1. `USE CATALOG` on the catalog
# MAGIC 2. `USE SCHEMA` on the schema
# MAGIC 3. `SELECT` on the table
# MAGIC All three are required in sequence.
# MAGIC
# MAGIC ### 8. How do you separate environments (dev/test/prod) in Unity Catalog?
# MAGIC **Answer**: Create separate catalogs:
# MAGIC - `dev_catalog`: Full access for developers
# MAGIC - `test_catalog`: Access for QA team
# MAGIC - `prod_catalog`: Restricted, read-only for most users
# MAGIC This prevents accidental production changes and enables safe experimentation.
# MAGIC
# MAGIC ### 9. What is the difference between GRANT and REVOKE?
# MAGIC **Answer**:
# MAGIC - **GRANT**: Adds privileges to a principal (user/group)
# MAGIC - **REVOKE**: Removes privileges from a principal
# MAGIC Syntax: `GRANT/REVOKE <privilege> ON <object> TO/FROM <principal>`
# MAGIC
# MAGIC ### 10. How do you audit data access in Unity Catalog?
# MAGIC **Answer**: Unity Catalog automatically logs all access in system tables (`system.access.audit`). These logs track who accessed what data, when, and how. Query these tables to generate compliance reports and detect anomalies.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔴 Advanced Questions:
# MAGIC
# MAGIC ### 11. How would you design a governance model for a multi-team organization?
# MAGIC **Answer**:
# MAGIC 1. Create separate catalogs per department or business unit
# MAGIC 2. Define standard groups (engineers, analysts, scientists, viewers)
# MAGIC 3. Implement least privilege at each layer
# MAGIC 4. Use consistent naming conventions
# MAGIC 5. Document all objects with comments
# MAGIC 6. Set up quarterly access reviews
# MAGIC 7. Enable audit logging and monitoring
# MAGIC 8. Implement row-level security for sensitive data
# MAGIC
# MAGIC ### 12. What are common governance anti-patterns to avoid?
# MAGIC **Answer**:
# MAGIC - Over-permissioning users (granting ALL PRIVILEGES unnecessarily)
# MAGIC - Not using Unity Catalog (using legacy Hive metastore)
# MAGIC - Mixing environments (testing in production)
# MAGIC - Ignoring audit logs
# MAGIC - Granting to individuals instead of groups
# MAGIC - No documentation/comments
# MAGIC - Shared admin accounts
# MAGIC - No separation between raw and curated data
# MAGIC
# MAGIC ### 13. How do you migrate from Hive metastore to Unity Catalog?
# MAGIC **Answer**:
# MAGIC 1. Enable Unity Catalog in workspace
# MAGIC 2. Create target catalog/schema structure
# MAGIC 3. Use `SYNC` or `CREATE TABLE AS SELECT` to migrate tables
# MAGIC 4. Update queries to use three-part names
# MAGIC 5. Migrate permissions using GRANT statements
# MAGIC 6. Test thoroughly in dev environment
# MAGIC 7. Communicate changes to users
# MAGIC 8. Gradually transition workloads
# MAGIC
# MAGIC ### 14. Explain how to implement row-level and column-level security.
# MAGIC **Answer**:
# MAGIC - **Row-level**: Create views with WHERE clauses filtering based on current_user() or session variables. Grant access to views, not base tables.
# MAGIC - **Column-level**: Use column masking functions or create views selecting only authorized columns. Unity Catalog also supports dynamic views with built-in masking.
# MAGIC
# MAGIC ### 15. How do you handle PII data in a governed environment?
# MAGIC **Answer**:
# MAGIC 1. Classify and tag PII columns
# MAGIC 2. Store raw PII only in Bronze layer with restricted access
# MAGIC 3. Mask or hash PII in Silver layer
# MAGIC 4. Aggregate/anonymize in Gold layer
# MAGIC 5. Use column-level security for necessary PII access
# MAGIC 6. Enable audit logging
# MAGIC 7. Document data retention policies
# MAGIC 8. Implement data deletion workflows for GDPR/CCPA compliance

# COMMAND ----------

# DBTITLE 1,Common Mistakes and Genie Code Examples
# MAGIC %md
# MAGIC # ⚠️ Common Mistakes to Avoid
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚫 Top 10 Governance Mistakes:
# MAGIC
# MAGIC ### 1️▃ Over-Permissioning Users
# MAGIC ```sql
# MAGIC -- ❌ Bad: Giving too much access
# MAGIC GRANT ALL PRIVILEGES ON CATALOG prod_catalog TO `analysts`;
# MAGIC
# MAGIC -- ✅ Good: Specific, limited access
# MAGIC GRANT USE CATALOG ON CATALOG prod_catalog TO `analysts`;
# MAGIC GRANT USE SCHEMA ON SCHEMA prod_catalog.gold TO `analysts`;
# MAGIC GRANT SELECT ON TABLE prod_catalog.gold.dashboard_data TO `analysts`;
# MAGIC ```
# MAGIC
# MAGIC ### 2️▃ Not Using Unity Catalog
# MAGIC ```python
# MAGIC # ❌ Bad: Using legacy Hive metastore
# MAGIC df = spark.table("default.my_table")
# MAGIC
# MAGIC # ✅ Good: Using Unity Catalog
# MAGIC df = spark.table("prod_catalog.gold.my_table")
# MAGIC ```
# MAGIC
# MAGIC ### 3️▃ Mixing Environments
# MAGIC ```sql
# MAGIC -- ❌ Bad: Testing in production
# MAGIC USE CATALOG prod_catalog;
# MAGIC DROP TABLE gold.important_table;  -- Disaster!
# MAGIC
# MAGIC -- ✅ Good: Use dev catalog for testing
# MAGIC USE CATALOG dev_catalog;
# MAGIC DROP TABLE gold.test_table;  -- Safe
# MAGIC ```
# MAGIC
# MAGIC ### 4️▃ Ignoring Audit Logs
# MAGIC - ❌ Not reviewing access logs
# MAGIC - ❌ No alerting on suspicious activity
# MAGIC - ✅ Regular audit log reviews
# MAGIC - ✅ Automated alerts for sensitive data access
# MAGIC
# MAGIC ### 5️▃ No Documentation
# MAGIC ```sql
# MAGIC -- ❌ Bad: No context
# MAGIC CREATE TABLE catalog.schema.table (...);
# MAGIC
# MAGIC -- ✅ Good: Clear documentation
# MAGIC CREATE TABLE catalog.schema.table (...)
# MAGIC COMMENT 'Customer orders | Owner: Sales Team | PII: Yes | Updated: Hourly';
# MAGIC ```
# MAGIC
# MAGIC ### 6️▃ Hardcoding Individual Users
# MAGIC ```sql
# MAGIC -- ❌ Bad: Granting to individuals
# MAGIC GRANT SELECT ON TABLE ... TO `john.doe@company.com`;
# MAGIC GRANT SELECT ON TABLE ... TO `jane.smith@company.com`;
# MAGIC
# MAGIC -- ✅ Good: Use groups
# MAGIC GRANT SELECT ON TABLE ... TO `sales_analysts`;
# MAGIC ```
# MAGIC
# MAGIC ### 7️▃ No Environment Separation
# MAGIC - ❌ Single catalog for all work
# MAGIC - ✅ Separate dev_catalog, test_catalog, prod_catalog
# MAGIC
# MAGIC ### 8️▃ Exposing Raw PII in Gold Layer
# MAGIC ```sql
# MAGIC -- ❌ Bad: PII in analytics layer
# MAGIC CREATE TABLE gold.customers AS
# MAGIC SELECT customer_id, email, ssn, ...  -- PII exposed!
# MAGIC
# MAGIC -- ✅ Good: Aggregate and anonymize
# MAGIC CREATE TABLE gold.customer_summary AS
# MAGIC SELECT country, COUNT(*) as customer_count, AVG(age) as avg_age
# MAGIC GROUP BY country;
# MAGIC ```
# MAGIC
# MAGIC ### 9️▃ No Access Reviews
# MAGIC - ❌ Set permissions and forget
# MAGIC - ✅ Quarterly access audits
# MAGIC - ✅ Remove unused accounts
# MAGIC
# MAGIC ### 🔟 Inconsistent Naming
# MAGIC ```sql
# MAGIC -- ❌ Bad: Random names
# MAGIC table1, my_data, final_v2, temp_test
# MAGIC
# MAGIC -- ✅ Good: Clear conventions
# MAGIC prod_catalog.silver_sales.transactions_validated
# MAGIC dev_catalog.bronze_logs.events_raw
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🤖 Genie Code Agent - Example Prompts
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 How to Use Databricks Genie Code Agent:
# MAGIC
# MAGIC Genie Code is your AI pair programmer for Databricks. Use natural language to accomplish governance tasks.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📢 Example Prompts:
# MAGIC
# MAGIC ### 🏛️ Catalog & Schema Management:
# MAGIC ```
# MAGIC 🗣️ "Create a Unity Catalog called 'analytics_prod' with bronze, silver, and gold schemas"
# MAGIC 🗣️ "Show me all schemas in the prod_catalog"
# MAGIC 🗣️ "Create a schema called 'customer_data' with a comment explaining it's for customer analytics"
# MAGIC ```
# MAGIC
# MAGIC ### 📋 Table Operations:
# MAGIC ```
# MAGIC 🗣️ "Create a Delta table in prod_catalog.gold called 'revenue_summary' with columns for date, region, and revenue"
# MAGIC 🗣️ "Query the top 10 rows from demo_catalog.silver.sales_cleaned"
# MAGIC 🗣️ "Show me the schema of finance_catalog.bronze.transactions"
# MAGIC ```
# MAGIC
# MAGIC ### 🔐 Access Control:
# MAGIC ```
# MAGIC 🗣️ "Grant SELECT permission on prod_catalog.gold.sales_summary to the data_analysts group"
# MAGIC 🗣️ "Show me all grants on the table demo_catalog.silver.sales_cleaned"
# MAGIC 🗣️ "Revoke INSERT privilege on finance_catalog.raw_data.transactions from contractors group"
# MAGIC 🗣️ "Create a governance model where analysts can only read gold tables"
# MAGIC ```
# MAGIC
# MAGIC ### 🏅 Medallion Architecture:
# MAGIC ```
# MAGIC 🗣️ "Set up a medallion architecture in catalog 'sales_data' with appropriate access controls"
# MAGIC 🗣️ "Create a bronze table for raw sales, transform it to silver by removing nulls, and aggregate to gold by region"
# MAGIC 🗣️ "Implement a pipeline that moves data from bronze to silver to gold with PII masking"
# MAGIC ```
# MAGIC
# MAGIC ### 🔍 Governance & Compliance:
# MAGIC ```
# MAGIC 🗣️ "Show me how to implement least privilege access for a team of analysts"
# MAGIC 🗣️ "Create a data governance checklist for my Unity Catalog environment"
# MAGIC 🗣️ "How do I audit who accessed prod_catalog.gold.customer_data in the last 30 days?"
# MAGIC 🗣️ "Help me separate dev and prod environments with proper access controls"
# MAGIC ```
# MAGIC
# MAGIC ### 🛡️ Security:
# MAGIC ```
# MAGIC 🗣️ "How do I protect PII data in Unity Catalog?"
# MAGIC 🗣️ "Create a view that masks email addresses for analysts"
# MAGIC 🗣️ "Show me best practices for securing sensitive financial data"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌟 Pro Tips for Using Genie Code:
# MAGIC
# MAGIC 1. **Be Specific**: Include catalog, schema, and table names
# MAGIC 2. **Mention Context**: Specify if it's for prod, dev, or test
# MAGIC 3. **Ask for Explanations**: "Explain how Unity Catalog privileges work"
# MAGIC 4. **Request Best Practices**: "What's the best way to..."
# MAGIC 5. **Iterate**: Start simple, then refine based on results
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✨ Advanced Genie Prompts:
# MAGIC
# MAGIC ```
# MAGIC 🗣️ "Create a complete governed data pipeline:
# MAGIC    1. Ingest raw sales data to bronze
# MAGIC    2. Clean and validate in silver
# MAGIC    3. Aggregate by region in gold
# MAGIC    4. Set up access controls so analysts only see gold"
# MAGIC
# MAGIC 🗣️ "Audit my Unity Catalog setup and identify any security risks or over-permissioned users"
# MAGIC
# MAGIC 🗣️ "Migrate my tables from the default Hive metastore to Unity Catalog with proper governance"
# MAGIC
# MAGIC 🗣️ "Design a multi-tenant architecture where different teams can't see each other's data"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **👉 Try these prompts in your Databricks notebook with Genie Code enabled!**

# COMMAND ----------

# DBTITLE 1,Final Watermark and Credits
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC # 🎓 Training Complete!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏆 What You've Mastered:
# MAGIC
# MAGIC ✅ Unity Catalog fundamentals and architecture  
# MAGIC ✅ Catalog, Schema, and Table hierarchy  
# MAGIC ✅ Role-Based Access Control (RBAC)  
# MAGIC ✅ GRANT and REVOKE privilege management  
# MAGIC ✅ Medallion architecture with governance  
# MAGIC ✅ Data security and PII protection  
# MAGIC ✅ Environment separation (dev/test/prod)  
# MAGIC ✅ Governance best practices  
# MAGIC ✅ Common mistakes and how to avoid them  
# MAGIC ✅ Using Genie Code for governance tasks  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Next Steps:
# MAGIC
# MAGIC 1. **Practice**: Implement governance in your own workspace
# MAGIC 2. **Experiment**: Try different access control patterns
# MAGIC 3. **Document**: Add comments to all your objects
# MAGIC 4. **Audit**: Review permissions regularly
# MAGIC 5. **Learn More**: Explore row-level security and dynamic views
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Additional Resources:
# MAGIC
# MAGIC - [Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/index.html)
# MAGIC - [Best Practices for Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/best-practices.html)
# MAGIC - [Medallion Architecture Guide](https://www.databricks.com/glossary/medallion-architecture)
# MAGIC - [Data Governance on Databricks](https://www.databricks.com/product/unity-catalog)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📌 Remember:
# MAGIC
# MAGIC > **"With great data comes great responsibility"**  
# MAGIC > — Implement governance from day one!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🏷️ Watermark & Credits
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 **Created By**: TRRaveendra
# MAGIC ### 🏷️ **Watermark**: @TRRaveendra
# MAGIC ### 📅 **Date**: April 21, 2026
# MAGIC ### 📚 **Phase**: 7 - Day 33
# MAGIC ### 🎯 **Topic**: Unity Catalog Governance
# MAGIC ### ⚙️ **Platform**: Databricks (Serverless + Unity Catalog + Delta Lake)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👏 Special Thanks:
# MAGIC - Databricks Unity Catalog Team
# MAGIC - Data Governance Community
# MAGIC - All Data Engineers learning governance!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **💡 Questions or Feedback?**  
# MAGIC Reach out to TRRaveendra for training sessions and consultations.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⭐ If this training helped you, please:
# MAGIC - ⭐ Star the repository
# MAGIC - 👍 Share with your team
# MAGIC - 💬 Provide feedback
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🎉 Happy Governing! 🎉
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **@TRRaveendra** | Data Engineering Training Series