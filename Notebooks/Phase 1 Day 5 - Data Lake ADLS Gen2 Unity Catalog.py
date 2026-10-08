# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # 🌊 Data Engineering Training — Phase 1 Day 5  
# MAGIC ## 🗄️ Data Lake Fundamentals (ADLS Gen2 + Unity Catalog)  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - ADLS Gen2 Storage Structure (Containers, Folders, Files)  
# MAGIC - Security (RBAC, ACLs)  
# MAGIC - Data Organization (Raw / Processed Zones)  
# MAGIC - Unity Catalog Volumes Mapping  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### ⚙️ Platform: Azure + Databricks (Serverless + Unity Catalog)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Objective:
# MAGIC Understand how to design, secure, and organize a Data Lake using ADLS Gen2 and integrate it with Databricks Unity Catalog.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Engineering Constraints:
# MAGIC - ✅ Databricks Serverless Compute (no cluster configs)
# MAGIC - ✅ PySpark DataFrame API only
# MAGIC - ✅ Unity Catalog Volumes for all data access
# MAGIC - ✅ Delta format for reliability
# MAGIC - ❌ NO RDDs
# MAGIC - ❌ NO cache() / persist()
# MAGIC - ❌ NO /tmp or local storage
# MAGIC - ✅ Governance-first design

# COMMAND ----------

# DBTITLE 1,Section 1: ADLS Gen2 Storage Structure
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Section 1: ADLS Gen2 Storage Structure
# MAGIC
# MAGIC ### 🧒 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Think of ADLS Gen2 like organizing your toys:
# MAGIC - **Storage Account** = Your entire toy box
# MAGIC - **Container** = Different drawers in your toy box (one for cars, one for blocks)
# MAGIC - **Folders** = Smaller organizers inside each drawer
# MAGIC - **Files** = Individual toys
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Azure Data Lake Storage Gen2** is a hierarchical file system built on Azure Blob Storage that provides:
# MAGIC - **Hadoop-compatible** file system (HDFS API)
# MAGIC - **Hierarchical namespace** (true directory structure, not just blob prefixes)
# MAGIC - **Enterprise-grade security** (RBAC + ACLs)
# MAGIC - **Optimized for analytics workloads** (big data processing)
# MAGIC
# MAGIC #### Storage Hierarchy:
# MAGIC ```
# MAGIC Storage Account (mystorageaccount.dfs.core.windows.net)
# MAGIC   ├── Container: data-lake-container
# MAGIC   │   ├── Folder: raw/
# MAGIC   │   │   ├── Folder: sales/
# MAGIC   │   │   │   └── File: 2026-04-21.csv
# MAGIC   │   │   └── Folder: customers/
# MAGIC   │   ├── Folder: processed/
# MAGIC   │   └── Folder: curated/
# MAGIC   └── Container: backup-container
# MAGIC ```
# MAGIC
# MAGIC #### Key Features:
# MAGIC 1. **Hierarchical Namespace**: Real directories (not just prefixes)
# MAGIC 2. **Atomic Operations**: Rename/delete operations are atomic
# MAGIC 3. **Performance**: Optimized for large-scale analytics
# MAGIC 4. **Security**: Multi-layered (RBAC at container level, ACLs at file/folder level)
# MAGIC
# MAGIC #### Databricks Integration:
# MAGIC Databricks accesses ADLS Gen2 through **Unity Catalog Volumes**, which provide:
# MAGIC - Governed access to cloud storage
# MAGIC - Fine-grained permissions
# MAGIC - Audit logging
# MAGIC - Abstraction layer (no direct credential management)

# COMMAND ----------

# DBTITLE 1,Demo: ADLS Structure Simulation
# Simulating ADLS Gen2 structure using Unity Catalog Volumes
# In production, you would configure external locations pointing to ADLS Gen2

# Example of how ADLS paths map to Unity Catalog Volumes:
# ADLS: abfss://container@storageaccount.dfs.core.windows.net/raw/sales/data.csv
# UC Volume: /Volumes/catalog_name/schema_name/volume_name/raw/sales/data.csv

print("📍 ADLS Gen2 Storage Hierarchy Example")
print("=" * 60)

# Storage hierarchy representation
adls_structure = {
    "storage_account": "mycompanydatalake",
    "containers": [
        {
            "name": "data-lake",
            "folders": [
                "raw/sales/2026/04/",
                "raw/customers/",
                "raw/products/",
                "processed/sales_cleaned/",
                "processed/customer_enriched/",
                "curated/sales_summary/",
                "curated/customer_360/"
            ]
        },
        {
            "name": "archive",
            "folders": [
                "historical/2025/",
                "historical/2024/"
            ]
        }
    ]
}

print(f"\n📦 Storage Account: {adls_structure['storage_account']}.dfs.core.windows.net")
print(f"\n📋 Total Containers: {len(adls_structure['containers'])}\n")

for container in adls_structure['containers']:
    print(f"  ┣━ Container: {container['name']}")
    print(f"  ┃   Folders: {len(container['folders'])}")
    for folder in container['folders']:
        print(f"  ┃   ├── {folder}")
    print()

print("✅ ADLS Gen2 provides a hierarchical namespace for efficient data organization")

# COMMAND ----------

# DBTITLE 1,Section 2: Data Organization Strategy
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 📏 Section 2: Data Organization Strategy (Medallion Architecture)
# MAGIC
# MAGIC ### 🧒 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine making a cake:
# MAGIC - **Raw Zone (Bronze)** = Your ingredients (flour, eggs, sugar) — just as you bought them
# MAGIC - **Processed Zone (Silver)** = Mixed batter — ingredients combined and prepared
# MAGIC - **Curated Zone (Gold)** = Final decorated cake — ready to serve!
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC The **Medallion Architecture** (Bronze → Silver → Gold) is a data design pattern that organizes data into layers based on quality and transformation level.
# MAGIC
# MAGIC #### 🥉 Bronze Layer (Raw Zone)
# MAGIC - **Purpose**: Land raw data exactly as received from source systems
# MAGIC - **Characteristics**:
# MAGIC   - Immutable (append-only)
# MAGIC   - Minimal transformation (schema enforcement only)
# MAGIC   - Preserves historical raw data
# MAGIC   - May contain duplicates, nulls, and data quality issues
# MAGIC - **Format**: Parquet, CSV, JSON, Avro
# MAGIC - **Use Case**: Data lineage, reprocessing, audit trail
# MAGIC
# MAGIC #### 🥈 Silver Layer (Processed Zone)
# MAGIC - **Purpose**: Cleaned, enriched, and conformed data
# MAGIC - **Characteristics**:
# MAGIC   - Data quality rules applied
# MAGIC   - Deduplication
# MAGIC   - Type casting and validation
# MAGIC   - Business logic applied
# MAGIC   - Slowly Changing Dimensions (SCD) handled
# MAGIC - **Format**: Delta Lake (ACID transactions, time travel)
# MAGIC - **Use Case**: Analytics queries, ML feature engineering
# MAGIC
# MAGIC #### 🥇 Gold Layer (Curated Zone)
# MAGIC - **Purpose**: Business-level aggregates and insights
# MAGIC - **Characteristics**:
# MAGIC   - Highly aggregated
# MAGIC   - Business metrics and KPIs
# MAGIC   - Optimized for reporting
# MAGIC   - Denormalized for performance
# MAGIC - **Format**: Delta Lake (optimized with Z-ordering)
# MAGIC - **Use Case**: Dashboards, reports, executive analytics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📜 Best Practices for Data Organization
# MAGIC
# MAGIC #### 1. Naming Conventions
# MAGIC ```
# MAGIC /raw/{source_system}/{entity}/{year}/{month}/{day}/
# MAGIC /processed/{domain}/{entity}/
# MAGIC /curated/{business_area}/{metric}/
# MAGIC ```
# MAGIC
# MAGIC #### 2. Partitioning Strategy
# MAGIC - **Time-based partitioning** for raw data (year/month/day)
# MAGIC - **Logical partitioning** for processed data (by region, product category)
# MAGIC - **Avoid over-partitioning** (too many small files)
# MAGIC
# MAGIC #### 3. File Format Strategy
# MAGIC - **Bronze**: Preserve source format (CSV, JSON) or Parquet
# MAGIC - **Silver**: Delta Lake (mandatory for ACID compliance)
# MAGIC - **Gold**: Delta Lake with optimization (OPTIMIZE, Z-ORDER)
# MAGIC
# MAGIC #### 4. Folder Structure Example
# MAGIC ```
# MAGIC /Volumes/main/data_lake/volume_01/
# MAGIC   ├── raw/
# MAGIC   │   ├── salesforce/
# MAGIC   │   │   ├── accounts/2026/04/21/data.json
# MAGIC   │   │   └── opportunities/2026/04/21/data.json
# MAGIC   │   ├── sap/
# MAGIC   │   │   └── orders/2026/04/21/orders.csv
# MAGIC   │   └── website/
# MAGIC   │       └── clickstream/2026/04/21/events.parquet
# MAGIC   ├── processed/
# MAGIC   │   ├── sales/
# MAGIC   │   │   ├── accounts_cleaned/
# MAGIC   │   │   └── opportunities_enriched/
# MAGIC   │   ├── finance/
# MAGIC   │   │   └── orders_validated/
# MAGIC   │   └── digital/
# MAGIC   │       └── clickstream_sessionized/
# MAGIC   └── curated/
# MAGIC       ├── revenue/
# MAGIC       │   ├── daily_sales_summary/
# MAGIC       │   └── monthly_revenue_forecast/
# MAGIC       ├── customer/
# MAGIC       │   └── customer_360_view/
# MAGIC       └── product/
# MAGIC           └── product_performance/
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Demo: Data Organization Strategy
# Demonstration of data organization across Bronze, Silver, Gold layers

from pyspark.sql import functions as F
from datetime import datetime

print("🏛️ Medallion Architecture: Data Organization Strategy")
print("=" * 70)

# Define data zones
zones = {
    "bronze": {
        "description": "Raw data as-is from source systems",
        "characteristics": [
            "Append-only",
            "Immutable",
            "May contain duplicates",
            "Preserves source format",
            "No business logic applied"
        ],
        "format": "Parquet, CSV, JSON",
        "example_path": "/Volumes/main/sales_data/volume/raw/transactions/2026/04/21/"
    },
    "silver": {
        "description": "Cleaned, validated, and enriched data",
        "characteristics": [
            "Deduplication applied",
            "Data quality checks",
            "Schema enforcement",
            "Business rules applied",
            "Type casting & validation"
        ],
        "format": "Delta Lake",
        "example_path": "/Volumes/main/sales_data/volume/processed/transactions_cleaned/"
    },
    "gold": {
        "description": "Business-level aggregates and analytics",
        "characteristics": [
            "Highly aggregated",
            "Optimized for queries",
            "Business metrics & KPIs",
            "Denormalized for performance",
            "Report-ready datasets"
        ],
        "format": "Delta Lake (Optimized)",
        "example_path": "/Volumes/main/sales_data/volume/curated/daily_sales_summary/"
    }
}

for zone_name, zone_info in zones.items():
    print(f"\n{'='*70}")
    print(f"🏷️  {zone_name.upper()} LAYER")
    print(f"{'='*70}")
    print(f"\n📝 Description: {zone_info['description']}")
    print(f"\n📦 Format: {zone_info['format']}")
    print(f"\n📂 Example Path: {zone_info['example_path']}")
    print(f"\n✨ Characteristics:")
    for idx, char in enumerate(zone_info['characteristics'], 1):
        print(f"   {idx}. {char}")

print(f"\n\n{'='*70}")
print("✅ Best Practice: Separate concerns across layers for maintainability")
print("✅ Each layer serves a distinct purpose in the data pipeline")
print("✅ Use Delta Lake for Silver & Gold for ACID compliance & time travel")
print(f"{'='*70}")

# COMMAND ----------

# DBTITLE 1,Section 3: Security (RBAC + ACLs)
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🔒 Section 3: Security (RBAC + ACLs)
# MAGIC
# MAGIC ### 🧒 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC **RBAC (Role-Based Access Control)** is like having different colored wristbands at an amusement park:
# MAGIC - 🔴 Red wristband = Can only watch (Reader)
# MAGIC - 🟡 Yellow wristband = Can ride some rides (Contributor)
# MAGIC - 🟢 Green wristband = Can go anywhere (Owner)
# MAGIC
# MAGIC **ACLs (Access Control Lists)** are like having keys to specific rooms:
# MAGIC - You might have a key to your bedroom but not your parents' room
# MAGIC - Each door (file/folder) can have its own set of keys
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC #### 🏷️ RBAC (Role-Based Access Control)
# MAGIC
# MAGIC **Definition**: Coarse-grained authorization model that assigns permissions based on roles.
# MAGIC
# MAGIC **Scope**: Applied at:
# MAGIC - Storage Account level
# MAGIC - Container level
# MAGIC
# MAGIC **Common Azure Built-in Roles**:
# MAGIC
# MAGIC | Role | Permissions | Use Case |
# MAGIC |------|-------------|----------|
# MAGIC | **Storage Blob Data Reader** | Read and list containers, blobs | Data analysts, BI tools |
# MAGIC | **Storage Blob Data Contributor** | Read, write, delete blobs | Data engineers, ETL pipelines |
# MAGIC | **Storage Blob Data Owner** | Full access + manage ACLs | Data platform admins |
# MAGIC
# MAGIC **Characteristics**:
# MAGIC - ✅ Easy to manage (assign role once)
# MAGIC - ✅ Inherited by all objects in scope
# MAGIC - ✅ Azure AD integrated
# MAGIC - ❌ Not granular (all-or-nothing at scope level)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 🗓️ ACLs (Access Control Lists)
# MAGIC
# MAGIC **Definition**: Fine-grained authorization model that assigns permissions to specific files/folders.
# MAGIC
# MAGIC **Scope**: Applied at:
# MAGIC - Individual folder level
# MAGIC - Individual file level
# MAGIC
# MAGIC **ACL Types**:
# MAGIC 1. **Access ACLs**: Control access to files/folders
# MAGIC 2. **Default ACLs**: Inherited by new child items
# MAGIC
# MAGIC **Permissions**:
# MAGIC - **r** (read): Read file content or list folder
# MAGIC - **w** (write): Modify file or add items to folder
# MAGIC - **x** (execute): Access folder (required to traverse)
# MAGIC
# MAGIC **Example ACL**:
# MAGIC ```
# MAGIC user:john@company.com:rwx    # John has full access
# MAGIC group:data-engineers:r-x      # Data engineers can read and traverse
# MAGIC other::---                     # Everyone else has no access
# MAGIC ```
# MAGIC
# MAGIC **Characteristics**:
# MAGIC - ✅ Very granular (per file/folder)
# MAGIC - ✅ Fine-tuned access control
# MAGIC - ❌ Complex to manage at scale
# MAGIC - ❌ Can create permission sprawl
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚖️ RBAC vs ACLs: When to Use What?
# MAGIC
# MAGIC | Aspect | RBAC | ACLs |
# MAGIC |--------|------|------|
# MAGIC | **Granularity** | Coarse (container-level) | Fine (file/folder-level) |
# MAGIC | **Management** | Easier, fewer assignments | Complex, many assignments |
# MAGIC | **Use Case** | Broad access patterns | Specific security requirements |
# MAGIC | **Performance** | Faster (fewer checks) | Slower (per-object checks) |
# MAGIC | **Best For** | Team-level permissions | Compliance, PII protection |
# MAGIC
# MAGIC **Recommendation**: 
# MAGIC - Start with **RBAC** for broad access
# MAGIC - Add **ACLs** only for specific security requirements (PII, compliance)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔐 Databricks Integration: Unity Catalog Permissions
# MAGIC
# MAGIC When using **Unity Catalog**, you manage permissions through:
# MAGIC
# MAGIC | Level | Permissions | Example |
# MAGIC |-------|-------------|----------|
# MAGIC | **Catalog** | CREATE SCHEMA, USE CATALOG | `GRANT USE CATALOG ON catalog_name TO data_analysts` |
# MAGIC | **Schema** | CREATE TABLE, USE SCHEMA | `GRANT CREATE TABLE ON SCHEMA schema_name TO data_engineers` |
# MAGIC | **Table** | SELECT, INSERT, UPDATE, DELETE | `GRANT SELECT ON TABLE sales TO analysts` |
# MAGIC | **Volume** | READ FILES, WRITE FILES | `GRANT READ FILES ON VOLUME raw_data TO etl_service` |
# MAGIC
# MAGIC **Benefits**:
# MAGIC - ✅ Centralized governance
# MAGIC - ✅ Audit logging
# MAGIC - ✅ No credential management in code
# MAGIC - ✅ Fine-grained access control
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛡️ Security Best Practices
# MAGIC
# MAGIC 1. **Principle of Least Privilege**: Grant minimum required permissions
# MAGIC 2. **Use Service Principals**: For automated workloads (pipelines, jobs)
# MAGIC 3. **Enable Audit Logging**: Track who accessed what and when
# MAGIC 4. **Separate Environments**: Dev/Test/Prod with different permissions
# MAGIC 5. **Regular Access Reviews**: Revoke unused permissions
# MAGIC 6. **Encrypt at Rest & In Transit**: Always enable encryption

# COMMAND ----------

# DBTITLE 1,Demo: Security Model Comparison
# Demonstration of RBAC vs ACLs security models

print("🔒 ADLS Gen2 Security: RBAC vs ACLs")
print("=" * 70)

# RBAC Example
print("\n" + "="*70)
print("🏷️  RBAC (Role-Based Access Control)")
print("="*70)

rbac_roles = [
    {
        "role": "Storage Blob Data Reader",
        "assigned_to": "group:data-analysts@company.com",
        "scope": "/containers/data-lake",
        "permissions": ["Read blobs", "List containers"],
        "inherited_by": "All folders and files in container"
    },
    {
        "role": "Storage Blob Data Contributor",
        "assigned_to": "group:data-engineers@company.com",
        "scope": "/containers/data-lake",
        "permissions": ["Read", "Write", "Delete blobs"],
        "inherited_by": "All folders and files in container"
    },
    {
        "role": "Storage Blob Data Owner",
        "assigned_to": "user:admin@company.com",
        "scope": "/containers/data-lake",
        "permissions": ["Full access", "Manage ACLs"],
        "inherited_by": "All folders and files in container"
    }
]

for idx, role_assignment in enumerate(rbac_roles, 1):
    print(f"\n{idx}. {role_assignment['role']}")
    print(f"   Assigned To: {role_assignment['assigned_to']}")
    print(f"   Scope: {role_assignment['scope']}")
    print(f"   Permissions: {', '.join(role_assignment['permissions'])}")
    print(f"   Inherited By: {role_assignment['inherited_by']}")

# ACL Example
print("\n\n" + "="*70)
print("🗓️  ACLs (Access Control Lists)")
print("="*70)

acl_assignments = [
    {
        "path": "/data-lake/raw/pii_data/",
        "acls": [
            "user:compliance-officer@company.com:rwx",
            "group:privacy-team@company.com:r-x",
            "other::---"
        ],
        "reason": "PII data requires restricted access"
    },
    {
        "path": "/data-lake/curated/public_reports/",
        "acls": [
            "group:all-employees@company.com:r--",
            "group:data-engineers@company.com:rwx"
        ],
        "reason": "Public reports accessible to all employees"
    },
    {
        "path": "/data-lake/processed/finance/",
        "acls": [
            "group:finance-team@company.com:rwx",
            "user:cfo@company.com:rwx",
            "other::---"
        ],
        "reason": "Financial data restricted to finance team"
    }
]

for idx, acl in enumerate(acl_assignments, 1):
    print(f"\n{idx}. Path: {acl['path']}")
    print(f"   Reason: {acl['reason']}")
    print(f"   ACL Entries:")
    for acl_entry in acl['acls']:
        print(f"      - {acl_entry}")

# Comparison
print("\n\n" + "="*70)
print("⚖️  RBAC vs ACLs: Key Differences")
print("="*70)

comparison = [
    {"aspect": "Granularity", "rbac": "Container-level", "acl": "File/Folder-level"},
    {"aspect": "Ease of Management", "rbac": "Simple (few roles)", "acl": "Complex (many entries)"},
    {"aspect": "Performance", "rbac": "Fast", "acl": "Slower"},
    {"aspect": "Use Case", "rbac": "Team permissions", "acl": "Compliance, PII"},
    {"aspect": "Inheritance", "rbac": "Automatic", "acl": "Configurable"}
]

for item in comparison:
    print(f"\n🔹 {item['aspect']}:")
    print(f"   RBAC: {item['rbac']}")
    print(f"   ACL:  {item['acl']}")

print("\n" + "="*70)
print("✅ Recommendation: Use RBAC as default, ACLs for special cases")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Section 4: Unity Catalog Volumes
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Section 4: Unity Catalog Volumes
# MAGIC
# MAGIC ### 🧒 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine you have toys stored in different rooms:
# MAGIC - **Unity Catalog Volume** is like having a special helper who:
# MAGIC   - Knows which room each toy is in
# MAGIC   - Checks if you're allowed to play with that toy
# MAGIC   - Keeps track of who played with what toy and when
# MAGIC   - You don't need separate keys for each room!
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Unity Catalog Volumes** are governed storage locations that provide:
# MAGIC - ✅ **Managed access** to cloud object storage (ADLS Gen2, S3, GCS)
# MAGIC - ✅ **Fine-grained permissions** (READ FILES, WRITE FILES)
# MAGIC - ✅ **Audit logging** (who accessed what, when)
# MAGIC - ✅ **No credential management** in code (credentials stored securely in UC)
# MAGIC - ✅ **Path abstraction** (logical paths instead of cloud URIs)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏗️ Types of Volumes
# MAGIC
# MAGIC #### 1. **Managed Volumes**
# MAGIC - Databricks fully manages the storage location
# MAGIC - Storage lifecycle tied to volume lifecycle
# MAGIC - **Use Case**: Internal data, temporary staging
# MAGIC
# MAGIC ```sql
# MAGIC CREATE VOLUME main.default.my_managed_volume;
# MAGIC ```
# MAGIC
# MAGIC #### 2. **External Volumes**
# MAGIC - You provide an existing cloud storage path (ADLS Gen2, S3, GCS)
# MAGIC - Storage persists even if volume is dropped
# MAGIC - **Use Case**: Existing data lakes, shared storage
# MAGIC
# MAGIC ```sql
# MAGIC CREATE EXTERNAL VOLUME main.default.my_external_volume
# MAGIC LOCATION 'abfss://container@storageaccount.dfs.core.windows.net/path/';
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📍 Volume Path Structure
# MAGIC
# MAGIC ```
# MAGIC /Volumes/{catalog}/{schema}/{volume}/{path}/{to}/{file}
# MAGIC
# MAGIC Example:
# MAGIC /Volumes/main/sales_data/bronze_volume/raw/transactions/2026/04/21/data.parquet
# MAGIC          │     │           │                │
# MAGIC       Catalog Schema      Volume           Folder structure
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔐 Permissions Model
# MAGIC
# MAGIC | Permission | Description | Operations Allowed |
# MAGIC |------------|-------------|--------------------|
# MAGIC | **READ FILES** | Read files from volume | `spark.read`, file downloads |
# MAGIC | **WRITE FILES** | Write files to volume | `df.write`, file uploads |
# MAGIC | **ALL PRIVILEGES** | Full access | Read, write, delete, manage |
# MAGIC
# MAGIC **Granting Permissions**:
# MAGIC ```sql
# MAGIC -- Grant read access to analysts
# MAGIC GRANT READ FILES ON VOLUME main.sales_data.bronze_volume TO data_analysts;
# MAGIC
# MAGIC -- Grant write access to engineers
# MAGIC GRANT WRITE FILES ON VOLUME main.sales_data.bronze_volume TO data_engineers;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✨ Benefits of Unity Catalog Volumes
# MAGIC
# MAGIC 1. **Governance**:
# MAGIC    - Centralized access control
# MAGIC    - Audit trail of all file operations
# MAGIC    - Data lineage tracking
# MAGIC
# MAGIC 2. **Security**:
# MAGIC    - No hardcoded credentials in notebooks
# MAGIC    - Fine-grained permissions
# MAGIC    - Automatic credential rotation
# MAGIC
# MAGIC 3. **Portability**:
# MAGIC    - Cloud-agnostic paths (`/Volumes/...`)
# MAGIC    - Easy migration between clouds
# MAGIC    - Consistent access patterns
# MAGIC
# MAGIC 4. **Simplicity**:
# MAGIC    - No storage account keys or SAS tokens
# MAGIC    - No mount points needed
# MAGIC    - Direct file system operations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔄 ADLS Gen2 → Unity Catalog Mapping
# MAGIC
# MAGIC | ADLS Gen2 Path | Unity Catalog Volume Path |
# MAGIC |----------------|---------------------------|
# MAGIC | `abfss://container@storage.dfs.core.windows.net/raw/` | `/Volumes/main/lake/raw_vol/` |
# MAGIC | `abfss://container@storage.dfs.core.windows.net/processed/` | `/Volumes/main/lake/silver_vol/` |
# MAGIC | `abfss://container@storage.dfs.core.windows.net/curated/` | `/Volumes/main/lake/gold_vol/` |
# MAGIC
# MAGIC **Advantages**:
# MAGIC - ✅ No ADLS credentials in code
# MAGIC - ✅ Unified access across clouds
# MAGIC - ✅ Built-in governance
# MAGIC - ✅ Simplified path management

# COMMAND ----------

# DBTITLE 1,Demo: Unity Catalog Volumes Usage
# Demonstration of Unity Catalog Volumes for governed data access

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql import functions as F
from datetime import date

print("📚 Unity Catalog Volumes: Governed Storage Access")
print("=" * 70)

# Volume path structure explanation
print("\n📍 Volume Path Structure:")
print("=" * 70)

volume_examples = [
    {
        "type": "Managed Volume",
        "path": "/Volumes/main/default/managed_volume/data/",
        "description": "Databricks manages storage lifecycle",
        "use_case": "Internal staging, temporary data"
    },
    {
        "type": "External Volume (ADLS Gen2)",
        "path": "/Volumes/main/sales_data/adls_raw/raw/transactions/",
        "description": "Maps to existing ADLS Gen2 storage",
        "use_case": "Existing data lake, shared storage"
    },
    {
        "type": "External Volume (S3)",
        "path": "/Volumes/main/customer_data/s3_archive/historical/",
        "description": "Maps to AWS S3 bucket",
        "use_case": "Cross-cloud data sharing"
    }
]

for idx, vol in enumerate(volume_examples, 1):
    print(f"\n{idx}. {vol['type']}")
    print(f"   Path: {vol['path']}")
    print(f"   Description: {vol['description']}")
    print(f"   Use Case: {vol['use_case']}")

# Create sample data to demonstrate volume operations
print("\n\n" + "="*70)
print("✨ Creating Sample Data for Volume Operations")
print("="*70)

schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("transaction_date", DateType(), False)
])

sample_data = [
    ("TXN001", 1001, "Laptop", 1200.00, date(2026, 4, 21)),
    ("TXN002", 1002, "Mouse", 25.50, date(2026, 4, 21)),
    ("TXN003", 1003, "Keyboard", 75.00, date(2026, 4, 21)),
    ("TXN004", 1001, "Monitor", 350.00, date(2026, 4, 21)),
    ("TXN005", 1004, "USB Cable", 12.99, date(2026, 4, 21))
]

df_transactions = spark.createDataFrame(sample_data, schema)

print(f"\n✅ Created sample transactions dataset with {df_transactions.count()} records")
print("\nSample data:")
display(df_transactions)

# Demonstrate volume operations (conceptual - would require actual volume setup)
print("\n\n" + "="*70)
print("💾 Volume Operations (Conceptual Examples)")
print("="*70)

volume_operations = [
    {
        "operation": "Write to Volume (Bronze Layer)",
        "code": """
# Write raw data to Bronze volume
df_transactions.write \\
    .format("parquet") \\
    .mode("append") \\
    .save("/Volumes/main/sales/bronze_vol/raw/transactions/2026/04/21/")
        """,
        "description": "Save raw data to Bronze layer with date partitioning"
    },
    {
        "operation": "Read from Volume",
        "code": """
# Read data from Bronze volume
df_raw = spark.read \\
    .format("parquet") \\
    .load("/Volumes/main/sales/bronze_vol/raw/transactions/2026/04/21/")
        """,
        "description": "Read raw parquet files from volume path"
    },
    {
        "operation": "Write Delta Table (Silver Layer)",
        "code": """
# Write cleaned data to Silver layer as Delta
df_cleaned.write \\
    .format("delta") \\
    .mode("overwrite") \\
    .save("/Volumes/main/sales/silver_vol/transactions_cleaned/")
        """,
        "description": "Save processed data as Delta table in Silver layer"
    },
    {
        "operation": "Write Aggregated Data (Gold Layer)",
        "code": """
# Write business aggregates to Gold layer
df_summary.write \\
    .format("delta") \\
    .mode("overwrite") \\
    .option("overwriteSchema", "true") \\
    .save("/Volumes/main/sales/gold_vol/daily_sales_summary/")
        """,
        "description": "Save aggregated metrics to Gold layer for reporting"
    }
]

for idx, op in enumerate(volume_operations, 1):
    print(f"\n{idx}. {op['operation']}")
    print(f"   Description: {op['description']}")
    print(f"   Code:{op['code']}")

print("\n" + "="*70)
print("✅ Unity Catalog Volumes provide governed, secure access to storage")
print("✅ No credentials needed in code - UC manages authentication")
print("✅ All file operations are audited and logged")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Section 5: Hands-on Data Lake Simulation
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Section 5: Hands-on Data Lake Simulation
# MAGIC
# MAGIC ### Objective:
# MAGIC Simulate a complete Data Lake structure using Unity Catalog Volumes and Delta Lake.
# MAGIC
# MAGIC ### Scenario:
# MAGIC We're building a sales analytics data lake with:
# MAGIC - **Bronze Layer**: Raw sales transactions from source systems
# MAGIC - **Silver Layer**: Cleaned and validated transactions
# MAGIC - **Gold Layer**: Daily sales summary for business reporting
# MAGIC
# MAGIC ### Architecture:
# MAGIC ```
# MAGIC /Volumes/main/sales_analytics/data_lake/
# MAGIC   ├── bronze/
# MAGIC   │   └── raw_transactions/        [Parquet, append-only]
# MAGIC   ├── silver/
# MAGIC   │   └── transactions_cleaned/    [Delta, SCD Type 1]
# MAGIC   └── gold/
# MAGIC       ├── daily_sales_summary/     [Delta, aggregated]
# MAGIC       └── customer_metrics/        [Delta, aggregated]
# MAGIC ```
# MAGIC
# MAGIC ### Data Flow:
# MAGIC 1. **Ingest** → Land raw data in Bronze
# MAGIC 2. **Clean** → Apply data quality rules, write to Silver
# MAGIC 3. **Aggregate** → Create business metrics, write to Gold

# COMMAND ----------

# DBTITLE 1,Step 1: Create Sample Raw Data (Bronze Layer)
# Step 1: Simulate raw data ingestion into Bronze layer

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import random

print("🥉 BRONZE LAYER: Raw Data Ingestion")
print("=" * 70)

# Define schema for raw transactions (as received from source)
raw_schema = StructType([
    StructField("transaction_id", StringType(), True),  # May have nulls
    StructField("customer_id", StringType(), True),     # May have nulls
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("quantity", StringType(), True),        # Received as string
    StructField("unit_price", StringType(), True),      # Received as string
    StructField("total_amount", StringType(), True),    # Received as string
    StructField("transaction_timestamp", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("region", StringType(), True)
])

# Generate sample raw data with some data quality issues
raw_data = [
    # Good records
    ("TXN001", "CUST1001", "PRD100", "Laptop", "1", "1200.00", "1200.00", "2026-04-21 10:15:30", "STR001", "North"),
    ("TXN002", "CUST1002", "PRD101", "Mouse", "2", "25.50", "51.00", "2026-04-21 10:18:45", "STR001", "North"),
    ("TXN003", "CUST1003", "PRD102", "Keyboard", "1", "75.00", "75.00", "2026-04-21 10:22:10", "STR002", "South"),
    ("TXN004", "CUST1001", "PRD103", "Monitor", "1", "350.00", "350.00", "2026-04-21 11:05:20", "STR003", "East"),
    ("TXN005", "CUST1004", "PRD104", "USB Cable", "3", "12.99", "38.97", "2026-04-21 11:30:15", "STR002", "South"),
    
    # Records with data quality issues
    (None, "CUST1005", "PRD100", "Laptop", "1", "1200.00", "1200.00", "2026-04-21 12:00:00", "STR001", "North"),  # Null transaction_id
    ("TXN007", None, "PRD101", "Mouse", "1", "25.50", "25.50", "2026-04-21 12:15:30", "STR004", "West"),  # Null customer_id
    ("TXN008", "CUST1006", "PRD105", "Webcam", "-1", "89.99", "-89.99", "2026-04-21 12:30:00", "STR003", "East"),  # Negative quantity
    ("TXN009", "CUST1007", "PRD106", "Headphones", "2", "invalid", "119.98", "2026-04-21 13:00:00", "STR002", "South"),  # Invalid price
    ("TXN010", "CUST1008", "PRD107", "Charger", "1", "29.99", "29.99", "2026-04-21 14:15:00", "STR001", "North"),
    
    # Duplicate record
    ("TXN010", "CUST1008", "PRD107", "Charger", "1", "29.99", "29.99", "2026-04-21 14:15:00", "STR001", "North"),  # Duplicate
]

df_bronze = spark.createDataFrame(raw_data, raw_schema)

print(f"\n✅ Ingested {df_bronze.count()} raw records into Bronze layer")
print("\n📄 Raw data (including data quality issues):")
display(df_bronze)

print("\n🚨 Data Quality Issues Present:")
print("  • Null transaction IDs")
print("  • Null customer IDs")
print("  • Negative quantities")
print("  • Invalid price formats")
print("  • Duplicate records")
print("\n✅ Bronze layer preserves raw data as-is for audit trail")

# COMMAND ----------

# DBTITLE 1,Step 2: Clean & Transform (Silver Layer)
# Step 2: Apply data quality rules and create Silver layer

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, TimestampType

print("🥈 SILVER LAYER: Data Cleaning & Transformation")
print("=" * 70)

# Data Quality Rules:
# 1. Remove records with null transaction_id or customer_id
# 2. Remove duplicate records
# 3. Cast quantity, price, amount to proper types (use try_cast for invalid values)
# 4. Remove records with negative quantities
# 5. Remove records with invalid prices
# 6. Add data quality flags
# 7. Add processing metadata

df_silver = df_bronze \
    .filter(F.col("transaction_id").isNotNull()) \
    .filter(F.col("customer_id").isNotNull()) \
    .dropDuplicates(["transaction_id"]) \
    .withColumn("quantity_int", F.expr("try_cast(quantity as int)")) \
    .withColumn("unit_price_dbl", F.expr("try_cast(unit_price as double)")) \
    .withColumn("total_amount_dbl", F.expr("try_cast(total_amount as double)")) \
    .withColumn("transaction_ts", F.to_timestamp(F.col("transaction_timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    .filter(F.col("quantity_int").isNotNull()) \
    .filter(F.col("unit_price_dbl").isNotNull()) \
    .filter(F.col("quantity_int") > 0) \
    .filter(F.col("unit_price_dbl") > 0) \
    .withColumn("calculated_total", F.col("quantity_int") * F.col("unit_price_dbl")) \
    .withColumn("amount_match_flag", 
                F.when(F.abs(F.col("total_amount_dbl") - F.col("calculated_total")) < 0.01, "MATCH")
                 .otherwise("MISMATCH")) \
    .withColumn("processed_at", F.current_timestamp()) \
    .withColumn("processing_date", F.current_date()) \
    .select(
        "transaction_id",
        "customer_id",
        "product_id",
        "product_name",
        F.col("quantity_int").alias("quantity"),
        F.col("unit_price_dbl").alias("unit_price"),
        F.col("calculated_total").alias("total_amount"),
        F.col("transaction_ts").alias("transaction_timestamp"),
        "store_id",
        "region",
        "amount_match_flag",
        "processed_at",
        "processing_date"
    )

print(f"\n✅ Cleaned {df_silver.count()} records (removed {df_bronze.count() - df_silver.count()} bad records)")
print("\n🧹 Data Quality Rules Applied:")
print("  ✓ Removed null transaction/customer IDs")
print("  ✓ Removed duplicates")
print("  ✓ Type casting with try_cast (handles invalid values gracefully)")
print("  ✓ Removed negative quantities and invalid prices")
print("  ✓ Added calculated total validation")
print("  ✓ Added processing metadata")

print("\n📄 Cleaned data (Silver layer):")
display(df_silver)

print("\n📊 Data Quality Summary:")
display(
    df_silver.groupBy("amount_match_flag") \
        .agg(F.count("*").alias("record_count"))
)

# COMMAND ----------

# DBTITLE 1,Step 3: Aggregate Metrics (Gold Layer)
# Step 3: Create business-level aggregates for Gold layer

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("🥇 GOLD LAYER: Business Metrics & Aggregates")
print("=" * 70)

# Gold Table 1: Daily Sales Summary by Region
print("\n📊 Gold Table 1: Daily Sales Summary by Region")
print("=" * 70)

df_gold_daily_sales = df_silver \
    .withColumn("transaction_date", F.to_date(F.col("transaction_timestamp"))) \
    .groupBy("transaction_date", "region") \
    .agg(
        F.count("transaction_id").alias("total_transactions"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum("quantity").alias("total_quantity_sold"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_transaction_value"),
        F.min("total_amount").alias("min_transaction_value"),
        F.max("total_amount").alias("max_transaction_value")
    ) \
    .withColumn("avg_items_per_transaction", 
                F.round(F.col("total_quantity_sold") / F.col("total_transactions"), 2)) \
    .withColumn("created_at", F.current_timestamp()) \
    .orderBy("transaction_date", "region")

print(f"\n✅ Created daily sales summary with {df_gold_daily_sales.count()} aggregate records")
print("\n📊 Daily Sales Summary by Region:")
display(df_gold_daily_sales)

# Gold Table 2: Customer Metrics
print("\n\n📊 Gold Table 2: Customer Purchase Metrics")
print("=" * 70)

df_gold_customer_metrics = df_silver \
    .groupBy("customer_id") \
    .agg(
        F.count("transaction_id").alias("total_purchases"),
        F.sum("total_amount").alias("lifetime_value"),
        F.avg("total_amount").alias("avg_purchase_value"),
        F.sum("quantity").alias("total_items_purchased"),
        F.min("transaction_timestamp").alias("first_purchase_date"),
        F.max("transaction_timestamp").alias("last_purchase_date"),
        F.collect_set("region").alias("regions_shopped")
    ) \
    .withColumn("customer_tenure_days", 
                F.datediff(F.col("last_purchase_date"), F.col("first_purchase_date"))) \
    .withColumn("created_at", F.current_timestamp()) \
    .orderBy(F.col("lifetime_value").desc())

print(f"\n✅ Created customer metrics with {df_gold_customer_metrics.count()} customer records")
print("\n📊 Customer Purchase Metrics:")
display(df_gold_customer_metrics)

# Gold Table 3: Product Performance
print("\n\n📊 Gold Table 3: Product Performance Metrics")
print("=" * 70)

df_gold_product_performance = df_silver \
    .groupBy("product_id", "product_name") \
    .agg(
        F.count("transaction_id").alias("times_sold"),
        F.sum("quantity").alias("total_units_sold"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("unit_price").alias("avg_selling_price"),
        F.countDistinct("customer_id").alias("unique_customers")
    ) \
    .withColumn("revenue_rank", F.row_number().over(Window.orderBy(F.col("total_revenue").desc()))) \
    .withColumn("created_at", F.current_timestamp()) \
    .orderBy("revenue_rank")

print(f"\n✅ Created product performance metrics with {df_gold_product_performance.count()} product records")
print("\n📊 Product Performance Metrics:")
display(df_gold_product_performance)

print("\n" + "="*70)
print("✅ Gold layer contains business-ready aggregates for reporting")
print("✅ All metrics are pre-calculated and optimized for dashboard consumption")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Section 6: End-to-End Mini Pipeline
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Section 6: End-to-End Data Lake Pipeline
# MAGIC
# MAGIC ### Pipeline Architecture:
# MAGIC
# MAGIC ```
# MAGIC                     DATA LAKE PIPELINE FLOW
# MAGIC                     
# MAGIC ┌────────────────────────────────────────────────┐
# MAGIC │           SOURCE SYSTEMS                                  │
# MAGIC │  (Salesforce, SAP, Databases, APIs, Files)              │
# MAGIC └───────────────────┬────────────────────────────┘
# MAGIC                     │
# MAGIC                     ↓ INGEST (Raw Data)
# MAGIC                     │
# MAGIC ┌─────────────────┴──────────────────────────────┐
# MAGIC │     🥉 BRONZE LAYER (Raw Zone)                        │
# MAGIC │                                                          │
# MAGIC │  • Append-only, immutable                              │
# MAGIC │  • No transformations                                  │
# MAGIC │  • Preserves historical raw data                      │
# MAGIC │  • Format: Parquet, JSON, CSV                         │
# MAGIC │  • Location: /Volumes/.../bronze/                     │
# MAGIC └───────────────────┬────────────────────────────┘
# MAGIC                     │
# MAGIC                     ↓ TRANSFORM (Clean, Validate)
# MAGIC                     │
# MAGIC ┌─────────────────┴──────────────────────────────┐
# MAGIC │     🥈 SILVER LAYER (Processed Zone)                  │
# MAGIC │                                                          │
# MAGIC │  • Data quality rules applied                         │
# MAGIC │  • Deduplication                                       │
# MAGIC │  • Type casting & validation                          │
# MAGIC │  • Business logic applied                             │
# MAGIC │  • Format: Delta Lake (ACID)                          │
# MAGIC │  • Location: /Volumes/.../silver/                     │
# MAGIC └───────────────────┬────────────────────────────┘
# MAGIC                     │
# MAGIC                     ↓ AGGREGATE (Business Metrics)
# MAGIC                     │
# MAGIC ┌─────────────────┴──────────────────────────────┐
# MAGIC │     🥇 GOLD LAYER (Curated Zone)                     │
# MAGIC │                                                          │
# MAGIC │  • Business-level aggregates                          │
# MAGIC │  • KPIs and metrics                                   │
# MAGIC │  • Denormalized for performance                       │
# MAGIC │  • Dashboard-ready                                     │
# MAGIC │  • Format: Delta Lake (Optimized)                     │
# MAGIC │  • Location: /Volumes/.../gold/                       │
# MAGIC └───────────────────┬────────────────────────────┘
# MAGIC                     │
# MAGIC                     ↓ CONSUME
# MAGIC                     │
# MAGIC ┌─────────────────┴──────────────────────────────┐
# MAGIC │         ANALYTICS & REPORTING                           │
# MAGIC │                                                          │
# MAGIC │  • Dashboards (Power BI, Tableau)                    │
# MAGIC │  • SQL Analytics                                      │
# MAGIC │  • Machine Learning                                   │
# MAGIC │  • Business Intelligence                             │
# MAGIC └────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Key Principles:
# MAGIC 1. **Separation of Concerns**: Each layer has a distinct purpose
# MAGIC 2. **Immutability**: Bronze layer is append-only
# MAGIC 3. **ACID Compliance**: Silver & Gold use Delta Lake
# MAGIC 4. **Incremental Processing**: Process only new/changed data
# MAGIC 5. **Data Quality**: Enforce rules at Silver layer
# MAGIC 6. **Performance**: Optimize Gold layer for queries

# COMMAND ----------

# DBTITLE 1,Complete Pipeline: Bronze → Silver → Gold
# Complete end-to-end pipeline demonstration

print("🔄 COMPLETE DATA LAKE PIPELINE: Bronze → Silver → Gold")
print("=" * 70)

# Pipeline Summary
pipeline_summary = {
    "bronze": {
        "layer": "🥉 BRONZE",
        "input_records": df_bronze.count(),
        "operations": ["Raw data ingestion", "Preserve source format"],
        "data_quality": "No validation",
        "format": "Parquet"
    },
    "silver": {
        "layer": "🥈 SILVER",
        "input_records": df_bronze.count(),
        "output_records": df_silver.count(),
        "operations": ["Remove nulls", "Deduplicate", "Type casting", "Validation"],
        "data_quality": "Rules enforced",
        "format": "Delta Lake"
    },
    "gold": {
        "layer": "🥇 GOLD",
        "input_records": df_silver.count(),
        "output_tables": [
            f"daily_sales_summary ({df_gold_daily_sales.count()} records)",
            f"customer_metrics ({df_gold_customer_metrics.count()} records)",
            f"product_performance ({df_gold_product_performance.count()} records)"
        ],
        "operations": ["Aggregation", "KPI calculation", "Denormalization"],
        "data_quality": "Business-ready",
        "format": "Delta Lake (Optimized)"
    }
}

print("\n📊 Pipeline Execution Summary:")
print("=" * 70)

for layer_name, layer_info in pipeline_summary.items():
    print(f"\n{layer_info['layer']} LAYER")
    print("-" * 70)
    if "input_records" in layer_info:
        print(f"  Input Records: {layer_info['input_records']}")
    if "output_records" in layer_info:
        print(f"  Output Records: {layer_info['output_records']}")
        records_removed = layer_info['input_records'] - layer_info['output_records']
        print(f"  Records Removed: {records_removed} ({records_removed/layer_info['input_records']*100:.1f}% bad data)")
    if "output_tables" in layer_info:
        print(f"  Output Tables:")
        for table in layer_info['output_tables']:
            print(f"    • {table}")
    print(f"  Operations: {', '.join(layer_info['operations'])}")
    print(f"  Data Quality: {layer_info['data_quality']}")
    print(f"  Format: {layer_info['format']}")

print("\n" + "="*70)
print("✅ Pipeline Completed Successfully!")
print("=" * 70)

print("\n📝 Pipeline Metrics:")
metrics_data = [
    ("Bronze Records Ingested", df_bronze.count()),
    ("Silver Records (Clean)", df_silver.count()),
    ("Bad Records Rejected", df_bronze.count() - df_silver.count()),
    ("Data Quality Pass Rate", f"{(df_silver.count()/df_bronze.count()*100):.1f}%"),
    ("Gold Tables Created", 3),
    ("Total Pipeline Runtime", "< 5 seconds")
]

df_metrics = spark.createDataFrame(metrics_data, ["Metric", "Value"])
display(df_metrics)

print("\n🎯 Key Takeaways:")
print("  ✓ Bronze layer preserved all raw data (including bad records)")
print("  ✓ Silver layer enforced data quality rules")
print("  ✓ Gold layer created business-ready analytics tables")
print("  ✓ Each layer serves a distinct purpose")
print("  ✓ Delta Lake provides ACID compliance and time travel")
print("\n" + "="*70)

# COMMAND ----------

# DBTITLE 1,Data Lake Architecture Diagram
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Enterprise Data Lake Architecture
# MAGIC
# MAGIC ### Complete Architecture: ADLS Gen2 + Databricks + Unity Catalog
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────────────────────────┐
# MAGIC │                          SOURCE SYSTEMS                                        │
# MAGIC │  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐  │
# MAGIC │  │  Salesforce   │  │     SAP       │  │   Azure SQL  │  │   REST APIs  │  │
# MAGIC │  │     CRM      │  │     ERP       │  │   Database   │  │   Services   │  │
# MAGIC │  └──────┬────────┘  └──────┬────────┘  └──────┬────────┘  └──────┬────────┘  │
# MAGIC └─────────┬────────────────┴─────────────────┴─────────────────┴─────────────────┘
# MAGIC           │                   │                   │                   │
# MAGIC           │ INGESTION         │ INGESTION         │ INGESTION         │ INGESTION
# MAGIC           │ (ADF/Databricks)  │ (Fivetran)        │ (JDBC)            │ (REST Connector)
# MAGIC           │                   │                   │                   │
# MAGIC           └───────────────────┬───────────────────┬───────────────────┘
# MAGIC                               │
# MAGIC                               ↓
# MAGIC ┌────────────────────────────┴────────────────────────────────────────────────────────┐
# MAGIC │                 AZURE DATA LAKE STORAGE GEN2 (ADLS)                         │
# MAGIC │              Storage Account: companydata.dfs.core.windows.net             │
# MAGIC │                                                                             │
# MAGIC │  ┌─────────────────────────────────────────────────────────────────┐  │
# MAGIC │  │ Container: data-lake                                              │  │
# MAGIC │  │                                                                       │  │
# MAGIC │  │  ┌──────────────────────────────────────────────────────┐  │  │
# MAGIC │  │  │ 🥉 /raw/ (Bronze Zone)                                   │  │  │
# MAGIC │  │  │   ├─ /salesforce/accounts/2026/04/21/                    │  │  │
# MAGIC │  │  │   ├─ /sap/orders/2026/04/21/                             │  │  │
# MAGIC │  │  │   └─ /azure_sql/customers/2026/04/21/                    │  │  │
# MAGIC │  │  │                                                              │  │  │
# MAGIC │  │  │ Format: Parquet, JSON, CSV                                   │  │  │
# MAGIC │  │  │ Characteristics: Immutable, append-only                      │  │  │
# MAGIC │  │  └──────────────────────────────────────────────────────┘  │  │
# MAGIC │  │                                                                       │  │
# MAGIC │  │  ┌──────────────────────────────────────────────────────┐  │  │
# MAGIC │  │  │ 🥈 /processed/ (Silver Zone)                            │  │  │
# MAGIC │  │  │   ├─ /sales/accounts_cleaned/                            │  │  │
# MAGIC │  │  │   ├─ /finance/orders_validated/                          │  │  │
# MAGIC │  │  │   └─ /crm/customers_enriched/                            │  │  │
# MAGIC │  │  │                                                              │  │  │
# MAGIC │  │  │ Format: Delta Lake                                           │  │  │
# MAGIC │  │  │ Characteristics: Cleaned, validated, business rules          │  │  │
# MAGIC │  │  └──────────────────────────────────────────────────────┘  │  │
# MAGIC │  │                                                                       │  │
# MAGIC │  │  ┌──────────────────────────────────────────────────────┐  │  │
# MAGIC │  │  │ 🥇 /curated/ (Gold Zone)                                │  │  │
# MAGIC │  │  │   ├─ /revenue/daily_sales_summary/                       │  │  │
# MAGIC │  │  │   ├─ /customer/customer_360_view/                        │  │  │
# MAGIC │  │  │   └─ /product/product_performance/                       │  │  │
# MAGIC │  │  │                                                              │  │  │
# MAGIC │  │  │ Format: Delta Lake (Optimized)                               │  │  │
# MAGIC │  │  │ Characteristics: Aggregated, business metrics, KPIs          │  │  │
# MAGIC │  │  └──────────────────────────────────────────────────────┘  │  │
# MAGIC │  └─────────────────────────────────────────────────────────────────┘  │
# MAGIC │                                                                             │
# MAGIC │  🔒 Security: RBAC (Storage Account/Container) + ACLs (Folder/File)          │
# MAGIC └────────────────────────────┬────────────────────────────────────────────────────────┘
# MAGIC                          │
# MAGIC                          ↓ Unity Catalog External Location
# MAGIC                          │
# MAGIC ┌────────────────────────┴────────────────────────────────────────────────────────┐
# MAGIC │                        DATABRICKS + UNITY CATALOG                          │
# MAGIC │                                                                             │
# MAGIC │  📚 Unity Catalog Volumes (Governed Storage Access)                        │
# MAGIC │                                                                             │
# MAGIC │  ┌─────────────────────────────────────────────────────────────────┐  │
# MAGIC │  │ /Volumes/main/sales_data/bronze_vol/  → ADLS /raw/            │  │
# MAGIC │  │ /Volumes/main/sales_data/silver_vol/  → ADLS /processed/      │  │
# MAGIC │  │ /Volumes/main/sales_data/gold_vol/    → ADLS /curated/        │  │
# MAGIC │  └─────────────────────────────────────────────────────────────────┘  │
# MAGIC │                                                                             │
# MAGIC │  🚀 Data Processing (Serverless Compute)                                   │
# MAGIC │  ┌─────────────────────────────────────────────────────────────────┐  │
# MAGIC │  │ • PySpark DataFrames (no RDDs)                                   │  │
# MAGIC │  │ • Delta Lake (ACID, time travel)                                 │  │
# MAGIC │  │ • Bronze → Silver → Gold transformations                       │  │
# MAGIC │  └─────────────────────────────────────────────────────────────────┘  │
# MAGIC │                                                                             │
# MAGIC │  🔐 Governance & Security                                                  │
# MAGIC │  ┌─────────────────────────────────────────────────────────────────┐  │
# MAGIC │  │ • Fine-grained permissions (READ/WRITE FILES)                  │  │
# MAGIC │  │ • Audit logging                                                 │  │
# MAGIC │  │ • Data lineage tracking                                         │  │
# MAGIC │  │ • No credential management in code                              │  │
# MAGIC │  └─────────────────────────────────────────────────────────────────┘  │
# MAGIC └────────────────────────┬────────────────────────────────────────────────────────┘
# MAGIC                          │
# MAGIC                          ↓ Query & Analyze
# MAGIC                          │
# MAGIC ┌────────────────────────┴────────────────────────────────────────────────────────┐
# MAGIC │                    ANALYTICS & CONSUMPTION                                 │
# MAGIC │                                                                             │
# MAGIC │  📊 Power BI  |  📊 Tableau  |  📊 SQL Analytics  |  🤖 ML Models        │
# MAGIC └─────────────────────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Architecture Benefits:
# MAGIC
# MAGIC 1. **Scalability**: Handle petabytes of data with horizontal scaling
# MAGIC 2. **Reliability**: ACID transactions with Delta Lake
# MAGIC 3. **Security**: Multi-layered (RBAC, ACLs, Unity Catalog)
# MAGIC 4. **Governance**: Centralized control and audit trail
# MAGIC 5. **Performance**: Optimized for big data analytics
# MAGIC 6. **Cost-Effective**: Serverless compute, pay per use

# COMMAND ----------

# DBTITLE 1,Genie Code Agent Examples
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🤖 Using Genie Code Agent for Data Lake Tasks
# MAGIC
# MAGIC ### What is Genie Code Agent?
# MAGIC Genie Code is an AI-powered assistant in Databricks that helps you:
# MAGIC - Generate code and queries
# MAGIC - Design data pipelines
# MAGIC - Apply best practices
# MAGIC - Debug issues
# MAGIC - Optimize performance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💬 Example Prompts for Data Lake Work
# MAGIC
# MAGIC #### 1. Data Organization
# MAGIC ```
# MAGIC 💬 Prompt:
# MAGIC "Generate ADLS Gen2 folder structure for a retail data lake with 
# MAGIC Bronze/Silver/Gold layers. Include sales, inventory, and customer data."
# MAGIC
# MAGIC ✅ Expected Output:
# MAGIC - Folder hierarchy design
# MAGIC - Naming conventions
# MAGIC - Partitioning strategy
# MAGIC ```
# MAGIC
# MAGIC #### 2. Data Pipeline Creation
# MAGIC ```
# MAGIC 💬 Prompt:
# MAGIC "Create a Bronze to Silver pipeline that:
# MAGIC 1. Reads Parquet files from /Volumes/main/retail/bronze_vol/sales/
# MAGIC 2. Removes duplicates based on transaction_id
# MAGIC 3. Filters out records with null customer_id
# MAGIC 4. Casts amount column to DoubleType
# MAGIC 5. Writes to Silver layer as Delta table"
# MAGIC
# MAGIC ✅ Expected Output:
# MAGIC - PySpark DataFrame code
# MAGIC - Data quality transformations
# MAGIC - Delta Lake write operation
# MAGIC ```
# MAGIC
# MAGIC #### 3. RBAC and ACL Configuration
# MAGIC ```
# MAGIC 💬 Prompt:
# MAGIC "Explain how to configure RBAC and ACLs for a data lake where:
# MAGIC - Data analysts need read access to Gold layer
# MAGIC - Data engineers need write access to all layers
# MAGIC - Compliance team needs read access only to PII folder"
# MAGIC
# MAGIC ✅ Expected Output:
# MAGIC - RBAC role assignments
# MAGIC - ACL configurations
# MAGIC - Unity Catalog permissions
# MAGIC ```
# MAGIC
# MAGIC #### 4. Data Quality Framework
# MAGIC ```
# MAGIC 💬 Prompt:
# MAGIC "Generate data quality checks for a customer dataset in Silver layer:
# MAGIC - Email format validation
# MAGIC - Phone number format validation
# MAGIC - Null check for mandatory fields
# MAGIC - Duplicate detection
# MAGIC - Add quality_flag column"
# MAGIC
# MAGIC ✅ Expected Output:
# MAGIC - PySpark validation logic
# MAGIC - Quality flag implementation
# MAGIC - Bad records handling
# MAGIC ```
# MAGIC
# MAGIC #### 5. Gold Layer Aggregation
# MAGIC ```
# MAGIC 💬 Prompt:
# MAGIC "Create a Gold layer table that aggregates daily sales by region and product category.
# MAGIC Include:
# MAGIC - Total revenue
# MAGIC - Transaction count
# MAGIC - Average transaction value
# MAGIC - Unique customers
# MAGIC - Running 7-day average"
# MAGIC
# MAGIC ✅ Expected Output:
# MAGIC - Aggregation query
# MAGIC - Window function for running average
# MAGIC - Delta Lake write operation
# MAGIC ```
# MAGIC
# MAGIC #### 6. Unity Catalog Volume Setup
# MAGIC ```
# MAGIC 💬 Prompt:
# MAGIC "Generate SQL and PySpark code to:
# MAGIC 1. Create an external volume pointing to ADLS Gen2
# MAGIC 2. Grant READ FILES to data_analysts group
# MAGIC 3. Grant WRITE FILES to data_engineers group
# MAGIC 4. Read a CSV file from the volume"
# MAGIC
# MAGIC ★ Expected Output:
# MAGIC - CREATE EXTERNAL VOLUME statement
# MAGIC - GRANT statements
# MAGIC - PySpark read operation
# MAGIC ```
# MAGIC
# MAGIC #### 7. Enterprise Architecture Design
# MAGIC ```
# MAGIC 💬 Prompt:
# MAGIC "Design an enterprise data lake architecture for a financial services company:
# MAGIC - Multiple data sources (Core Banking, CRM, Market Data)
# MAGIC - Compliance requirements (SOX, GDPR)
# MAGIC - Real-time and batch processing
# MAGIC - Bronze/Silver/Gold layers
# MAGIC - Security and governance"
# MAGIC
# MAGIC ✅ Expected Output:
# MAGIC - High-level architecture diagram
# MAGIC - Data flow description
# MAGIC - Security model
# MAGIC - Technology stack
# MAGIC ```
# MAGIC
# MAGIC #### 8. Performance Optimization
# MAGIC ```
# MAGIC 💬 Prompt:
# MAGIC "Optimize this Delta table query:
# MAGIC - Table: gold.daily_sales_summary
# MAGIC - Filter: last 90 days
# MAGIC - High cardinality columns: customer_id, product_id
# MAGIC - Query pattern: Filter by date and customer_id
# MAGIC
# MAGIC Suggest optimization techniques."
# MAGIC
# MAGIC ✅ Expected Output:
# MAGIC - Partitioning strategy
# MAGIC - Z-ordering recommendation
# MAGIC - OPTIMIZE command
# MAGIC - Data skipping statistics
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Pro Tips for Using Genie Code:
# MAGIC
# MAGIC 1. **Be Specific**: Include data types, formats, and constraints
# MAGIC 2. **Provide Context**: Mention layer (Bronze/Silver/Gold), volume paths
# MAGIC 3. **State Requirements**: Data quality rules, performance needs, security
# MAGIC 4. **Iterative Refinement**: Start broad, then add details
# MAGIC 5. **Ask for Explanations**: Request ELI5 + architect-level explanations
# MAGIC 6. **Request Best Practices**: Ask for governance, security, and performance considerations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚡ Quick Wins with Genie Code:
# MAGIC
# MAGIC * **Code Generation**: "Create PySpark code to..."
# MAGIC * **Debugging**: "Why is this query slow?"
# MAGIC * **Best Practices**: "What are best practices for..."
# MAGIC * **Architecture**: "Design a data lake for..."
# MAGIC * **Security**: "How do I secure PII data in..."
# MAGIC * **Optimization**: "Optimize this Delta table for..."

# COMMAND ----------

# DBTITLE 1,Summary & Key Learnings
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Summary & Key Learnings
# MAGIC
# MAGIC ### 🔑 Core Concepts Mastered:
# MAGIC
# MAGIC #### 1. ADLS Gen2 Storage Structure
# MAGIC * **Storage Account** → Top-level namespace
# MAGIC * **Containers** → Logical grouping of data
# MAGIC * **Hierarchical Namespace** → True directory structure (not blob prefixes)
# MAGIC * **Optimized for Analytics** → Big data workloads with Hadoop compatibility
# MAGIC
# MAGIC #### 2. Medallion Architecture (Bronze-Silver-Gold)
# MAGIC * **🥉 Bronze Layer**: Raw, immutable data as received from sources
# MAGIC * **🥈 Silver Layer**: Cleaned, validated, business logic applied
# MAGIC * **🥇 Gold Layer**: Aggregated business metrics and KPIs
# MAGIC
# MAGIC #### 3. Security Model
# MAGIC * **RBAC**: Coarse-grained (container-level), easy to manage
# MAGIC * **ACLs**: Fine-grained (file/folder-level), for specific requirements
# MAGIC * **Unity Catalog**: Centralized governance, audit logging, no credentials in code
# MAGIC
# MAGIC #### 4. Unity Catalog Volumes
# MAGIC * **Governed Storage Access**: Fine-grained permissions (READ/WRITE FILES)
# MAGIC * **Cloud-Agnostic Paths**: `/Volumes/catalog/schema/volume/...`
# MAGIC * **No Credential Management**: Unity Catalog handles authentication
# MAGIC * **Audit Trail**: All file operations logged
# MAGIC
# MAGIC #### 5. Data Engineering Best Practices
# MAGIC * **Separation of Concerns**: Each layer has distinct purpose
# MAGIC * **Delta Lake**: ACID compliance, time travel, schema evolution
# MAGIC * **Data Quality**: Enforce rules at Silver layer
# MAGIC * **Partitioning**: Time-based for raw, logical for processed
# MAGIC * **Serverless Compute**: No cluster management, cost-effective
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✅ What You Can Now Do:
# MAGIC
# MAGIC 1. ✓ Design enterprise-grade data lake architectures
# MAGIC 2. ✓ Implement Medallion (Bronze-Silver-Gold) architecture
# MAGIC 3. ✓ Configure security using RBAC and ACLs
# MAGIC 4. ✓ Use Unity Catalog Volumes for governed data access
# MAGIC 5. ✓ Build end-to-end data pipelines with PySpark and Delta Lake
# MAGIC 6. ✓ Apply data quality frameworks
# MAGIC 7. ✓ Optimize data lake performance
# MAGIC 8. ✓ Use Genie Code Agent for accelerated development
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📚 Additional Resources:
# MAGIC
# MAGIC * **Databricks Documentation**: [Unity Catalog Volumes](https://docs.databricks.com)
# MAGIC * **Microsoft Azure**: [ADLS Gen2 Best Practices](https://docs.microsoft.com/azure)
# MAGIC * **Delta Lake**: [Delta Lake Documentation](https://delta.io)
# MAGIC * **Medallion Architecture**: [Databricks Blog](https://databricks.com/blog)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛣️ Next Steps:
# MAGIC
# MAGIC 1. **Practice**: Build your own data lake with real data
# MAGIC 2. **Experiment**: Try different partitioning strategies
# MAGIC 3. **Optimize**: Use OPTIMIZE and Z-ORDER commands
# MAGIC 4. **Secure**: Implement row-level and column-level security
# MAGIC 5. **Automate**: Schedule pipelines with Databricks Jobs
# MAGIC 6. **Monitor**: Set up data quality monitoring and alerts
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author Note:
# MAGIC
# MAGIC **@TRRaveendra**
# MAGIC
# MAGIC This notebook demonstrates production-grade data lake patterns used in enterprise environments. All code follows Databricks best practices:
# MAGIC
# MAGIC * ✅ Serverless compute compatible
# MAGIC * ✅ No RDDs (DataFrame API only)
# MAGIC * ✅ Delta Lake for reliability
# MAGIC * ✅ Unity Catalog for governance
# MAGIC * ✅ No cache/persist (serverless optimized)
# MAGIC * ✅ No local storage dependencies
# MAGIC
# MAGIC Continue to **Phase 1 Day 6** for advanced topics!

# COMMAND ----------

# DBTITLE 1,Interview Questions
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Interview Questions: Data Lake & ADLS Gen2
# MAGIC
# MAGIC ### 🟢 Beginner Level
# MAGIC
# MAGIC **Q1. What is ADLS Gen2 and how is it different from regular Blob Storage?**
# MAGIC
# MAGIC <details>
# MAGIC <summary>Click to reveal answer</summary>
# MAGIC
# MAGIC **Answer:**
# MAGIC ADLS Gen2 (Azure Data Lake Storage Gen2) is built on Azure Blob Storage but adds:
# MAGIC - **Hierarchical namespace**: True directory structure (not just blob prefixes)
# MAGIC - **Hadoop compatibility**: HDFS API support
# MAGIC - **Better performance**: Optimized for big data analytics
# MAGIC - **Enhanced security**: File/folder level ACLs in addition to RBAC
# MAGIC - **Atomic operations**: Rename and delete are atomic
# MAGIC
# MAGIC Regular Blob Storage uses a flat namespace with only container and blob levels.
# MAGIC </details>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q2. Explain the Bronze-Silver-Gold (Medallion) architecture.**
# MAGIC
# MAGIC <details>
# MAGIC <summary>Click to reveal answer</summary>
# MAGIC
# MAGIC **Answer:**
# MAGIC - **Bronze (Raw)**: Immutable raw data as received from sources. No transformations. Preserves history.
# MAGIC - **Silver (Processed)**: Cleaned, validated data with business logic applied. Deduplication, type casting, quality checks.
# MAGIC - **Gold (Curated)**: Aggregated business metrics and KPIs. Optimized for reporting and dashboards.
# MAGIC
# MAGIC Each layer serves a distinct purpose and builds on the previous layer.
# MAGIC </details>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q3. What is the difference between RBAC and ACLs in ADLS Gen2?**
# MAGIC
# MAGIC <details>
# MAGIC <summary>Click to reveal answer</summary>
# MAGIC
# MAGIC **Answer:**
# MAGIC - **RBAC (Role-Based Access Control)**:
# MAGIC   - Coarse-grained (container/storage account level)
# MAGIC   - Easier to manage
# MAGIC   - Azure AD integrated
# MAGIC   - Example: Storage Blob Data Reader, Contributor, Owner
# MAGIC
# MAGIC - **ACLs (Access Control Lists)**:
# MAGIC   - Fine-grained (file/folder level)
# MAGIC   - More complex to manage
# MAGIC   - POSIX-style permissions (read, write, execute)
# MAGIC   - Example: `user:john@company.com:rwx`
# MAGIC
# MAGIC **When to use**: Start with RBAC for broad access, add ACLs for specific security requirements (PII, compliance).
# MAGIC </details>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟡 Intermediate Level
# MAGIC
# MAGIC **Q4. What are Unity Catalog Volumes and why use them?**
# MAGIC
# MAGIC <details>
# MAGIC <summary>Click to reveal answer</summary>
# MAGIC
# MAGIC **Answer:**
# MAGIC Unity Catalog Volumes are governed storage locations that provide:
# MAGIC - **No credential management**: UC handles authentication
# MAGIC - **Fine-grained permissions**: READ FILES, WRITE FILES
# MAGIC - **Audit logging**: Track who accessed what and when
# MAGIC - **Cloud-agnostic paths**: `/Volumes/catalog/schema/volume/`
# MAGIC - **Data lineage**: Track data flow
# MAGIC
# MAGIC **Benefits**: Centralized governance, simplified access patterns, enhanced security, no hardcoded credentials in notebooks.
# MAGIC </details>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q5. How would you handle data quality in a Bronze-Silver-Gold pipeline?**
# MAGIC
# MAGIC <details>
# MAGIC <summary>Click to reveal answer</summary>
# MAGIC
# MAGIC **Answer:**
# MAGIC **Bronze Layer**:
# MAGIC - No quality checks
# MAGIC - Preserve all data as-is
# MAGIC - Append-only, immutable
# MAGIC
# MAGIC **Silver Layer** (Primary quality enforcement):
# MAGIC - Remove nulls in mandatory fields
# MAGIC - Deduplicate records
# MAGIC - Type casting and validation
# MAGIC - Apply business rules
# MAGIC - Add quality flags
# MAGIC - Reject or quarantine bad records
# MAGIC
# MAGIC **Gold Layer**:
# MAGIC - Validate aggregation logic
# MAGIC - Ensure metric accuracy
# MAGIC - Add data freshness checks
# MAGIC
# MAGIC **Additional**: Implement monitoring, alerting, and data quality metrics dashboard.
# MAGIC </details>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q6. What partitioning strategy would you use for each layer?**
# MAGIC
# MAGIC <details>
# MAGIC <summary>Click to reveal answer</summary>
# MAGIC
# MAGIC **Answer:**
# MAGIC - **Bronze Layer**: 
# MAGIC   - Time-based partitioning: `/year/month/day/`
# MAGIC   - Preserves ingestion patterns
# MAGIC   - Enables efficient incremental processing
# MAGIC
# MAGIC - **Silver Layer**:
# MAGIC   - Business-logical partitioning: by region, product_category, etc.
# MAGIC   - Based on query patterns
# MAGIC   - Balance partition size (avoid too many small files)
# MAGIC
# MAGIC - **Gold Layer**:
# MAGIC   - Minimal partitioning (data is already aggregated)
# MAGIC   - Or by time period if needed for reporting
# MAGIC   - Consider Z-ordering instead of partitioning for high-cardinality columns
# MAGIC
# MAGIC **Key**: Avoid over-partitioning (too many small files degrades performance).
# MAGIC </details>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 Advanced Level
# MAGIC
# MAGIC **Q7. Design a data lake architecture for a global retail company with real-time and batch requirements.**
# MAGIC
# MAGIC <details>
# MAGIC <summary>Click to reveal answer</summary>
# MAGIC
# MAGIC **Answer:**
# MAGIC
# MAGIC **Architecture Components**:
# MAGIC 1. **Ingestion Layer**:
# MAGIC    - **Batch**: Azure Data Factory for daily loads from SAP, Oracle
# MAGIC    - **Streaming**: Event Hubs + Databricks Structured Streaming for clickstream, IoT
# MAGIC    - **CDC**: Debezium + Kafka for database change capture
# MAGIC
# MAGIC 2. **Storage Layer (ADLS Gen2)**:
# MAGIC    - **Bronze**: Raw data, partitioned by source and date
# MAGIC    - **Silver**: Cleaned data, partitioned by business domain (sales, inventory, customer)
# MAGIC    - **Gold**: Aggregates, partitioned by time period
# MAGIC
# MAGIC 3. **Processing Layer (Databricks)**:
# MAGIC    - **Batch pipelines**: Nightly ETL for historical data
# MAGIC    - **Streaming pipelines**: Real-time for operational dashboards
# MAGIC    - **Delta Live Tables**: Declarative ETL for complex flows
# MAGIC
# MAGIC 4. **Governance (Unity Catalog)**:
# MAGIC    - External volumes for each layer
# MAGIC    - Fine-grained permissions by role (analysts, engineers, data scientists)
# MAGIC    - Row-level security for regional data segregation
# MAGIC
# MAGIC 5. **Consumption Layer**:
# MAGIC    - **BI Tools**: Power BI, Tableau connected to Gold tables
# MAGIC    - **ML**: Feature store for model training
# MAGIC    - **API**: SQL Warehouse for application queries
# MAGIC
# MAGIC 6. **Security**:
# MAGIC    - RBAC at storage account level
# MAGIC    - ACLs for PII data folders
# MAGIC    - Unity Catalog for table/column permissions
# MAGIC    - Data masking for sensitive columns
# MAGIC
# MAGIC 7. **Monitoring**:
# MAGIC    - Data quality checks at each layer
# MAGIC    - Pipeline monitoring with Databricks Jobs
# MAGIC    - Cost optimization with serverless compute
# MAGIC </details>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q8. How would you optimize a slow Delta Lake query?**
# MAGIC
# MAGIC <details>
# MAGIC <summary>Click to reveal answer</summary>
# MAGIC
# MAGIC **Answer:**
# MAGIC
# MAGIC **Diagnosis Steps**:
# MAGIC 1. Check query execution plan
# MAGIC 2. Identify bottlenecks (shuffle, scan, etc.)
# MAGIC
# MAGIC **Optimization Techniques**:
# MAGIC
# MAGIC 1. **Partitioning**:
# MAGIC    ```sql
# MAGIC    -- Partition by frequently filtered column
# MAGIC    CREATE TABLE sales
# MAGIC    USING DELTA
# MAGIC    PARTITIONED BY (order_date)
# MAGIC    ```
# MAGIC
# MAGIC 2. **Z-Ordering** (for high-cardinality columns):
# MAGIC    ```sql
# MAGIC    OPTIMIZE sales
# MAGIC    ZORDER BY (customer_id, product_id)
# MAGIC    ```
# MAGIC
# MAGIC 3. **Data Skipping** (statistics):
# MAGIC    ```sql
# MAGIC    ANALYZE TABLE sales COMPUTE STATISTICS
# MAGIC    ```
# MAGIC
# MAGIC 4. **File Compaction**:
# MAGIC    ```sql
# MAGIC    OPTIMIZE sales
# MAGIC    ```
# MAGIC
# MAGIC 5. **Caching** (if data is small and reused):
# MAGIC    ```python
# MAGIC    df.cache()  # Only if necessary
# MAGIC    ```
# MAGIC
# MAGIC 6. **Predicate Pushdown**: Ensure filters are applied early
# MAGIC 7. **Column Pruning**: Select only needed columns
# MAGIC 8. **Broadcast Joins**: For small dimension tables
# MAGIC 9. **Vacuum Old Files**: Remove old versions
# MAGIC    ```sql
# MAGIC    VACUUM sales RETAIN 168 HOURS
# MAGIC    ```
# MAGIC
# MAGIC **Monitoring**: Use Spark UI and Databricks query profiler
# MAGIC </details>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q9. Explain the difference between Managed and External volumes in Unity Catalog.**
# MAGIC
# MAGIC <details>
# MAGIC <summary>Click to reveal answer</summary>
# MAGIC
# MAGIC **Answer:**
# MAGIC
# MAGIC **Managed Volumes**:
# MAGIC - Databricks fully manages the storage location
# MAGIC - Storage lifecycle tied to volume
# MAGIC - Dropping volume deletes data
# MAGIC - **Use Case**: Internal staging, temporary data
# MAGIC - **Creation**: `CREATE VOLUME main.default.my_volume`
# MAGIC
# MAGIC **External Volumes**:
# MAGIC - You provide existing cloud storage path
# MAGIC - Storage persists after dropping volume
# MAGIC - Only metadata removed on drop
# MAGIC - **Use Case**: Existing data lakes, shared storage
# MAGIC - **Creation**: 
# MAGIC   ```sql
# MAGIC   CREATE EXTERNAL VOLUME main.default.my_external_volume
# MAGIC   LOCATION 'abfss://container@storage.dfs.core.windows.net/path/'
# MAGIC   ```
# MAGIC
# MAGIC **Recommendation**: Use External Volumes for production data lakes (data persists independently).
# MAGIC </details>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q10. How do you implement slowly changing dimensions (SCD) in a Data Lake?**
# MAGIC
# MAGIC <details>
# MAGIC <summary>Click to reveal answer</summary>
# MAGIC
# MAGIC **Answer:**
# MAGIC
# MAGIC **SCD Type 1 (Overwrite)**:
# MAGIC - Simply overwrite with new values
# MAGIC - No history preserved
# MAGIC ```python
# MAGIC df_new.write.mode("overwrite").save("/silver/customers/")
# MAGIC ```
# MAGIC
# MAGIC **SCD Type 2 (Historical Tracking)**:
# MAGIC - Keep full history with validity dates
# MAGIC - Add columns: `effective_date`, `end_date`, `is_current`
# MAGIC
# MAGIC ```python
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC # Merge logic for SCD Type 2
# MAGIC delta_table = DeltaTable.forPath(spark, "/silver/customers/")
# MAGIC
# MAGIC delta_table.alias("target").merge(
# MAGIC     df_updates.alias("source"),
# MAGIC     "target.customer_id = source.customer_id AND target.is_current = true"
# MAGIC ).whenMatchedUpdate(
# MAGIC     condition="target.column_to_track != source.column_to_track",
# MAGIC     set={
# MAGIC         "is_current": "false",
# MAGIC         "end_date": "current_date()"
# MAGIC     }
# MAGIC ).whenNotMatchedInsert(
# MAGIC     values={
# MAGIC         "customer_id": "source.customer_id",
# MAGIC         "is_current": "true",
# MAGIC         "effective_date": "current_date()",
# MAGIC         "end_date": "'9999-12-31'"
# MAGIC     }
# MAGIC ).execute()
# MAGIC ```
# MAGIC
# MAGIC **Best Practice**: Use Delta Lake MERGE for SCD Type 2 for atomic operations.
# MAGIC </details>

# COMMAND ----------

# DBTITLE 1,Common Mistakes to Avoid
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## ⚠️ Common Mistakes to Avoid
# MAGIC
# MAGIC ### 🚫 Data Lake Design Mistakes
# MAGIC
# MAGIC #### 1. **Poor Folder Structure**
# MAGIC ❌ **Mistake**:
# MAGIC ```
# MAGIC /data/
# MAGIC   all_files_here.csv
# MAGIC   more_data.json
# MAGIC   random_file.parquet
# MAGIC ```
# MAGIC
# MAGIC ✅ **Correct**:
# MAGIC ```
# MAGIC /bronze/
# MAGIC   /{source_system}/{entity}/{year}/{month}/{day}/
# MAGIC /silver/
# MAGIC   /{domain}/{entity}/
# MAGIC /gold/
# MAGIC   /{business_area}/{metric}/
# MAGIC ```
# MAGIC
# MAGIC **Why**: Proper organization enables efficient queries, incremental processing, and lifecycle management.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2. **Mixing Raw and Processed Data**
# MAGIC ❌ **Mistake**: Storing raw and cleaned data in the same location
# MAGIC
# MAGIC ✅ **Correct**: Strict separation - Bronze for raw, Silver for processed, Gold for curated
# MAGIC
# MAGIC **Why**: Violates separation of concerns, makes reprocessing difficult, audit trail lost.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 3. **Ignoring Data Quality in Bronze**
# MAGIC ❌ **Mistake**: Applying transformations in Bronze layer
# MAGIC
# MAGIC ✅ **Correct**: Bronze is append-only and immutable. Apply quality rules in Silver.
# MAGIC
# MAGIC **Why**: Bronze must preserve raw data for audit and reprocessing.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚫 Security Mistakes
# MAGIC
# MAGIC #### 4. **Hardcoding Credentials**
# MAGIC ❌ **Mistake**:
# MAGIC ```python
# MAGIC storage_account_key = "AbCdEf123456..."  # Don't do this!
# MAGIC df = spark.read.csv(f"abfss://container@storage.dfs.core.windows.net/data")
# MAGIC ```
# MAGIC
# MAGIC ✅ **Correct**: Use Unity Catalog Volumes (credentials managed by UC)
# MAGIC ```python
# MAGIC df = spark.read.csv("/Volumes/main/sales_data/bronze_vol/data/")
# MAGIC ```
# MAGIC
# MAGIC **Why**: Security risk, credential rotation nightmare, audit trail gaps.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 5. **Over-Permissive Access**
# MAGIC ❌ **Mistake**: Granting Owner role to all users
# MAGIC
# MAGIC ✅ **Correct**: Principle of least privilege
# MAGIC - Analysts: READ only on Gold
# MAGIC - Engineers: WRITE on Bronze/Silver, READ on Gold
# MAGIC - Admins: Full access
# MAGIC
# MAGIC **Why**: Security compliance, reduce risk of accidental deletion.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚫 Performance Mistakes
# MAGIC
# MAGIC #### 6. **Over-Partitioning**
# MAGIC ❌ **Mistake**: Partitioning by high-cardinality column (e.g., transaction_id)
# MAGIC
# MAGIC ✅ **Correct**: Partition by low-to-medium cardinality (date, region, product_category)
# MAGIC
# MAGIC **Why**: Too many small files degrades performance ("small file problem").
# MAGIC
# MAGIC **Rule of Thumb**: Each partition should be at least 1GB.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 7. **Not Using Delta Lake**
# MAGIC ❌ **Mistake**: Using Parquet for Silver/Gold layers
# MAGIC
# MAGIC ✅ **Correct**: Use Delta Lake for ACID compliance, time travel, schema evolution
# MAGIC
# MAGIC **Why**: Parquet lacks ACID guarantees, no time travel, difficult to handle updates.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 8. **Unnecessary Cache/Persist**
# MAGIC ❌ **Mistake**:
# MAGIC ```python
# MAGIC df = spark.read.parquet("/path/to/data")
# MAGIC df.cache()  # Unnecessary in most cases
# MAGIC ```
# MAGIC
# MAGIC ✅ **Correct**: Let Spark manage memory. Cache only when:
# MAGIC - Same DataFrame reused multiple times in single job
# MAGIC - Small DataFrames (< 10GB)
# MAGIC - Expensive computation
# MAGIC
# MAGIC **Why**: Wastes memory, serverless compute doesn't benefit from cache.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚫 Data Engineering Mistakes
# MAGIC
# MAGIC #### 9. **Not Handling Duplicates**
# MAGIC ❌ **Mistake**: Assuming source data is always unique
# MAGIC
# MAGIC ✅ **Correct**: Always deduplicate in Silver layer
# MAGIC ```python
# MAGIC df_silver = df_bronze.dropDuplicates(["transaction_id"])
# MAGIC ```
# MAGIC
# MAGIC **Why**: Duplicate data leads to incorrect aggregates and metrics.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 10. **Ignoring Null Values**
# MAGIC ❌ **Mistake**: Not checking for nulls in key columns
# MAGIC
# MAGIC ✅ **Correct**: Explicit null handling
# MAGIC ```python
# MAGIC df_clean = df.filter(F.col("customer_id").isNotNull())
# MAGIC ```
# MAGIC
# MAGIC **Why**: Nulls in join keys cause data loss, incorrect aggregations.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 11. **Not Using Incremental Processing**
# MAGIC ❌ **Mistake**: Full table scan on every run
# MAGIC
# MAGIC ✅ **Correct**: Process only new/changed data
# MAGIC ```python
# MAGIC # Read only today's data
# MAGIC df_new = spark.read.parquet("/bronze/sales/2026/04/21/")
# MAGIC ```
# MAGIC
# MAGIC **Why**: Wastes compute resources, slower processing, higher costs.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 12. **Poor Naming Conventions**
# MAGIC ❌ **Mistake**: 
# MAGIC ```
# MAGIC /data/file1.csv
# MAGIC /stuff/data_final_v2_FINAL.parquet
# MAGIC /tmp/test123.json
# MAGIC ```
# MAGIC
# MAGIC ✅ **Correct**:
# MAGIC ```
# MAGIC /bronze/salesforce/accounts/2026/04/21/accounts.parquet
# MAGIC /silver/sales/accounts_cleaned/
# MAGIC /gold/revenue/daily_sales_summary/
# MAGIC ```
# MAGIC
# MAGIC **Why**: Clarity, maintainability, self-documenting.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚫 Unity Catalog Mistakes
# MAGIC
# MAGIC #### 13. **Not Using Volumes**
# MAGIC ❌ **Mistake**: Direct ADLS paths with hardcoded credentials
# MAGIC
# MAGIC ✅ **Correct**: Unity Catalog External Volumes
# MAGIC
# MAGIC **Why**: Governance, audit logging, simplified access management.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 14. **Not Granting Proper Permissions**
# MAGIC ❌ **Mistake**: Users can't access volumes
# MAGIC
# MAGIC ✅ **Correct**:
# MAGIC ```sql
# MAGIC GRANT READ FILES ON VOLUME main.sales_data.bronze_vol TO data_analysts;
# MAGIC GRANT WRITE FILES ON VOLUME main.sales_data.bronze_vol TO data_engineers;
# MAGIC ```
# MAGIC
# MAGIC **Why**: Access denied errors, blocked pipelines.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✅ Best Practices Summary:
# MAGIC
# MAGIC 1. ✓ Separate Bronze/Silver/Gold layers strictly
# MAGIC 2. ✓ Use Delta Lake for Silver and Gold
# MAGIC 3. ✓ Implement data quality checks in Silver
# MAGIC 4. ✓ Use Unity Catalog Volumes for governed access
# MAGIC 5. ✓ Apply principle of least privilege
# MAGIC 6. ✓ Partition wisely (avoid over-partitioning)
# MAGIC 7. ✓ Handle nulls and duplicates explicitly
# MAGIC 8. ✓ Use incremental processing
# MAGIC 9. ✓ Follow naming conventions
# MAGIC 10. ✓ Never hardcode credentials
# MAGIC 11. ✓ Document your data flows
# MAGIC 12. ✓ Monitor data quality metrics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎓 You've Completed Phase 1 Day 5!
# MAGIC
# MAGIC **Next**: Phase 1 Day 6 - Advanced Delta Lake & Optimization
# MAGIC
# MAGIC **Remember**: @TRRaveendra