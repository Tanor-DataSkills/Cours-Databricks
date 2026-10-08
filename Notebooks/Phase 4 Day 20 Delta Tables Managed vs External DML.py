# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # 🧊 Data Engineering Training — Phase 4 Day 20  
# MAGIC ## 📊 Delta Tables: Managed vs External & DML Operations  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - Managed vs External Delta Tables  
# MAGIC - Insert, Update, Delete Operations  
# MAGIC - Unity Catalog Table Governance  
# MAGIC - Data Modification Best Practices  
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
# MAGIC Understand different types of Delta tables and how to perform DML operations (INSERT, UPDATE, DELETE) in a governed Lakehouse environment.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Engineering Constraints:
# MAGIC - ✅ Databricks Serverless Compute
# MAGIC - ✅ DataFrame API (No RDDs)
# MAGIC - ✅ No cache() / persist()
# MAGIC - ✅ Unity Catalog governance
# MAGIC - ✅ Delta Lake format (mandatory)
# MAGIC - ❌ No /tmp or local storage
# MAGIC - ❌ No direct file manipulation

# COMMAND ----------

# DBTITLE 1,Section 1: Delta Table Types - Concept
# MAGIC %md
# MAGIC # 📚 Section 1: Delta Table Types
# MAGIC
# MAGIC ## 🧒 ELI5 Explanation:
# MAGIC Imagine you have two ways to organize your toy collection:
# MAGIC
# MAGIC 1. **Managed Tables** = Toys stored in your mom's special closet. She decides where they go, keeps them organized, and when you throw away a toy, she cleans up the space too.
# MAGIC
# MAGIC 2. **External Tables** = Toys stored in YOUR own box under the bed. You control where the box is, and even if you stop playing with certain toys, they stay in your box until YOU decide to clean them up.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### **Managed Tables (Recommended)**
# MAGIC - **Definition**: Delta tables where Databricks Unity Catalog manages both the metadata AND the underlying data files.
# MAGIC - **Storage**: Automatically stored in the Unity Catalog managed storage location.
# MAGIC - **Lifecycle**: When you `DROP TABLE`, both metadata and data files are deleted.
# MAGIC - **Governance**: Full Unity Catalog governance (ACLs, lineage, audit logs).
# MAGIC - **Use Case**: Production environments, governed enterprise data, audit-compliant workloads.
# MAGIC
# MAGIC ### **External Tables**
# MAGIC - **Definition**: Delta tables where metadata is managed by Unity Catalog, but data files reside in an external location (S3, ADLS, GCS).
# MAGIC - **Storage**: User-specified location (must have access permissions).
# MAGIC - **Lifecycle**: When you `DROP TABLE`, only metadata is deleted; data files remain in the external location.
# MAGIC - **Governance**: Unity Catalog manages metadata and access control, but storage lifecycle is independent.
# MAGIC - **Use Case**: When you need to share data with external systems, legacy data migration, or require control over storage lifecycle.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚠️ Key Differences:
# MAGIC
# MAGIC | Feature | Managed Tables | External Tables |
# MAGIC |---------|----------------|------------------|
# MAGIC | **Metadata Management** | Unity Catalog | Unity Catalog |
# MAGIC | **Data Storage** | Unity Catalog managed location | User-specified external location |
# MAGIC | **DROP TABLE Behavior** | Deletes metadata + data | Deletes metadata only (data persists) |
# MAGIC | **Governance** | Full Unity Catalog governance | Metadata governed, storage independent |
# MAGIC | **Access Control** | Unity Catalog ACLs | Unity Catalog ACLs + Cloud IAM |
# MAGIC | **Data Lifecycle** | Managed by Databricks | Managed by user |
# MAGIC | **Best For** | Production, governed environments | Data sharing, legacy migrations |
# MAGIC | **Storage Location Visibility** | Hidden (managed) | Explicit LOCATION clause |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Recommendation:
# MAGIC **Use Managed Tables by default** for governance, simplicity, and Delta Lake optimization. Use External Tables only when you have a specific requirement for external storage control.

# COMMAND ----------

# DBTITLE 1,Section 2: Creating Managed Tables - Introduction
# MAGIC %md
# MAGIC # 🛠️ Section 2: Creating Managed Delta Tables (BEST PRACTICE)
# MAGIC
# MAGIC ## 🎯 Why Managed Tables?
# MAGIC
# MAGIC 1. **Automatic Storage Management**: Databricks handles storage locations
# MAGIC 2. **Complete Lifecycle Control**: DROP TABLE cleans up everything
# MAGIC 3. **Full Governance**: Unity Catalog tracks lineage, access, and audit logs
# MAGIC 4. **Optimized Performance**: Automatic optimization and maintenance
# MAGIC 5. **Enterprise Ready**: Compliance, security, and governance built-in
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Syntax:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE <catalog>.<schema>.<table_name> (
# MAGIC   column1 datatype,
# MAGIC   column2 datatype
# MAGIC )
# MAGIC USING DELTA;
# MAGIC ```
# MAGIC
# MAGIC **Key Points**:
# MAGIC - No LOCATION clause = Managed table
# MAGIC - Storage automatically managed by Unity Catalog
# MAGIC - Follows three-level namespace: `catalog.schema.table`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Let's create managed tables using both SQL and PySpark:

# COMMAND ----------

# DBTITLE 1,Setup: Create Catalog and Schema
# Setup: Create catalog and schema for demonstrations
# Note: You'll need CREATE CATALOG privilege, or use existing catalog

# For this demo, we'll use an existing catalog or create one
# Adjust based on your workspace setup

catalog_name = "main"  # Use your catalog
schema_name = "phase4_day20_delta_demo"

# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

print(f"✅ Schema created: {catalog_name}.{schema_name}")
print(f"📋 Full path for tables: {catalog_name}.{schema_name}.<table_name>")

# COMMAND ----------

# DBTITLE 1,Create Managed Table - SQL Method
# MAGIC %sql
# MAGIC -- Method 1: Create Managed Table using SQL
# MAGIC
# MAGIC CREATE OR REPLACE TABLE main.phase4_day20_delta_demo.customers_managed (
# MAGIC   customer_id BIGINT COMMENT 'Unique customer identifier',
# MAGIC   customer_name STRING COMMENT 'Customer full name',
# MAGIC   email STRING COMMENT 'Customer email address',
# MAGIC   signup_date DATE COMMENT 'Account creation date',
# MAGIC   country STRING COMMENT 'Customer country',
# MAGIC   lifetime_value DECIMAL(10,2) COMMENT 'Total customer value'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Managed Delta table for customer data - storage handled by Unity Catalog';
# MAGIC
# MAGIC -- Verify table creation
# MAGIC DESCRIBE EXTENDED main.phase4_day20_delta_demo.customers_managed;

# COMMAND ----------

# DBTITLE 1,Create Managed Table - PySpark Method
# Method 2: Create Managed Table using PySpark DataFrame API

from pyspark.sql.types import StructType, StructField, LongType, StringType, DecimalType
from pyspark.sql.functions import current_date, expr, to_date
from decimal import Decimal

# Create sample data (without schema - let Spark infer)
orders_data = [
    (1001, 101, "Laptop", 1, Decimal("1200.00"), "2026-04-15"),
    (1002, 102, "Mouse", 2, Decimal("50.00"), "2026-04-16"),
    (1003, 103, "Keyboard", 1, Decimal("80.00"), "2026-04-17"),
    (1004, 101, "Monitor", 1, Decimal("350.00"), "2026-04-18"),
    (1005, 104, "Headphones", 1, Decimal("150.00"), "2026-04-19")
]

orders_df = spark.createDataFrame(
    orders_data,
    ["order_id", "customer_id", "product_name", "quantity", "order_amount", "order_date"]
).withColumn("order_date", to_date("order_date"))

# Write as managed Delta table
table_path = "main.phase4_day20_delta_demo.orders_managed"

orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_path)

print(f"✅ Managed table created: {table_path}")
print(f"\n📊 Row count: {spark.table(table_path).count()}")

# Display the table
display(spark.table(table_path))

# COMMAND ----------

# DBTITLE 1,Verify Managed Table Properties
# Verify that these are MANAGED tables (no external location)

print("="*70)
print("CUSTOMERS_MANAGED TABLE PROPERTIES")
print("="*70)

customers_details = spark.sql("""
    DESCRIBE EXTENDED main.phase4_day20_delta_demo.customers_managed
""").collect()

for row in customers_details:
    if 'Location' in row.col_name or 'Type' in row.col_name or 'Provider' in row.col_name:
        print(f"{row.col_name}: {row.data_type}")

print("\n" + "="*70)
print("ORDERS_MANAGED TABLE PROPERTIES")
print("="*70)

orders_details = spark.sql("""
    DESCRIBE EXTENDED main.phase4_day20_delta_demo.orders_managed
""").collect()

for row in orders_details:
    if 'Location' in row.col_name or 'Type' in row.col_name or 'Provider' in row.col_name:
        print(f"{row.col_name}: {row.data_type}")

print("\n✅ Both tables are MANAGED - Unity Catalog controls storage location")

# COMMAND ----------

# DBTITLE 1,Section 3: Creating External Tables
# MAGIC %md
# MAGIC # 🌍 Section 3: Creating External Delta Tables
# MAGIC
# MAGIC ## 🧒 ELI5 Explanation:
# MAGIC External tables are like having a labeled box in YOUR room. You tell your parents (Unity Catalog) what's in the box and they keep track of it, but YOU decide when to add or remove things from YOUR box. If your parents forget about the box (DROP TABLE), the box and toys are still there in your room!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### When to Use External Tables:
# MAGIC 1. **Data Sharing**: Need to access data from external systems (data lakes, object storage)
# MAGIC 2. **Legacy Migration**: Migrating existing data to Unity Catalog without moving files
# MAGIC 3. **Multi-Workspace Access**: Same data accessed by multiple Databricks workspaces
# MAGIC 4. **Compliance Requirements**: Regulatory requirements to maintain data in specific storage locations
# MAGIC 5. **Separation of Concerns**: Data lifecycle managed independently from metadata
# MAGIC
# MAGIC ### Key Characteristics:
# MAGIC - Requires explicit LOCATION clause pointing to cloud storage (S3, ADLS, GCS)
# MAGIC - Must have proper cloud IAM permissions (in addition to Unity Catalog ACLs)
# MAGIC - DROP TABLE only removes metadata; data files remain
# MAGIC - Useful for external data integration
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Syntax:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE <catalog>.<schema>.<table_name> (
# MAGIC   column1 datatype,
# MAGIC   column2 datatype
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '<cloud_storage_path>';
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚠️ Important Notes:
# MAGIC
# MAGIC 1. **Storage Location**: Must be accessible cloud storage (not /tmp or local paths)
# MAGIC 2. **Permissions**: Need both Unity Catalog and cloud IAM permissions
# MAGIC 3. **Volumes**: For external tables, use Unity Catalog Volumes for better governance
# MAGIC 4. **Lifecycle**: You manage data file lifecycle independently
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Let's demonstrate external table creation:

# COMMAND ----------

# DBTITLE 1,Note: External Table Demonstration
# MAGIC %md
# MAGIC ## 🚧 Note on External Tables in This Demo:
# MAGIC
# MAGIC For this training notebook, we'll demonstrate the **syntax and concepts** of external tables.
# MAGIC
# MAGIC In a production environment, you would:
# MAGIC 1. Create a Unity Catalog Volume
# MAGIC 2. Use the Volume path as the LOCATION
# MAGIC 3. Ensure proper cloud storage permissions
# MAGIC
# MAGIC **Example production pattern**:
# MAGIC ```sql
# MAGIC -- Step 1: Create Volume (one-time setup)
# MAGIC CREATE VOLUME IF NOT EXISTS main.phase4_day20_delta_demo.external_data;
# MAGIC
# MAGIC -- Step 2: Create external table pointing to Volume
# MAGIC CREATE TABLE catalog.schema.external_table
# MAGIC USING DELTA
# MAGIC LOCATION '/Volumes/main/phase4_day20_delta_demo/external_data/products/';
# MAGIC ```
# MAGIC
# MAGIC **For this demo**, we'll focus on managed tables (best practice) and show external table syntax for reference.

# COMMAND ----------

# DBTITLE 1,Section 4: INSERT Operations
# MAGIC %md
# MAGIC # ➕ Section 4: INSERT Operations (Data Ingestion)
# MAGIC
# MAGIC ## 🧒 ELI5 Explanation:
# MAGIC INSERT is like adding new toys to your toy box. You can add one toy at a time, or dump a whole bag of toys into the box. The important thing is — you're NOT throwing away the old toys, just adding new ones!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC Delta Lake supports **ACID-compliant INSERT** operations:
# MAGIC
# MAGIC ### INSERT Methods:
# MAGIC 1. **INSERT INTO**: Append new rows to existing table
# MAGIC 2. **INSERT OVERWRITE**: Replace all existing data (use with caution)
# MAGIC 3. **DataFrame.write.mode("append")**: PySpark append mode
# MAGIC 4. **DataFrame.write.mode("overwrite")**: PySpark overwrite mode
# MAGIC
# MAGIC ### Key Benefits:
# MAGIC - **ACID Transactions**: All-or-nothing semantics
# MAGIC - **Schema Enforcement**: Validates data against table schema
# MAGIC - **Schema Evolution**: Can evolve schema with merge options
# MAGIC - **Concurrent Writes**: Multiple writers can safely insert data
# MAGIC
# MAGIC ### Best Practices:
# MAGIC - Use `INSERT INTO` for incremental data loads (append)
# MAGIC - Avoid `INSERT OVERWRITE` in production (data loss risk)
# MAGIC - Prefer DML operations over full table rewrites
# MAGIC - Use DataFrame API for programmatic inserts
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,INSERT Operation - Initial Data Load
# First, let's add initial data to our customers_managed table

from pyspark.sql.functions import lit, current_date, to_date, col
from pyspark.sql.types import DecimalType
from decimal import Decimal

# Create initial customer data
initial_customers = [
    (101, "Alice Johnson", "alice@example.com", "2026-01-15", "USA", Decimal("5000.00")),
    (102, "Bob Smith", "bob@example.com", "2026-02-20", "UK", Decimal("3500.00")),
    (103, "Carol White", "carol@example.com", "2026-03-10", "Canada", Decimal("4200.00")),
    (104, "David Brown", "david@example.com", "2026-03-25", "Australia", Decimal("2800.00"))
]

customers_df = spark.createDataFrame(
    initial_customers,
    ["customer_id", "customer_name", "email", "signup_date", "country", "lifetime_value"]
).withColumn("signup_date", to_date("signup_date")) \
 .withColumn("lifetime_value", col("lifetime_value").cast(DecimalType(10, 2)))

# INSERT using DataFrame API (append mode)
customers_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("main.phase4_day20_delta_demo.customers_managed")

print("✅ Initial customer data inserted")
print(f"\n📊 Total customers: {spark.table('main.phase4_day20_delta_demo.customers_managed').count()}")

# Display current data
display(spark.table("main.phase4_day20_delta_demo.customers_managed"))

# COMMAND ----------

# DBTITLE 1,INSERT Operation - SQL Method
# MAGIC %sql
# MAGIC -- INSERT using SQL - Add new customers
# MAGIC
# MAGIC INSERT INTO main.phase4_day20_delta_demo.customers_managed
# MAGIC VALUES 
# MAGIC   (105, 'Emma Davis', 'emma@example.com', '2026-04-10', 'USA', 6000.00),
# MAGIC   (106, 'Frank Miller', 'frank@example.com', '2026-04-12', 'Germany', 4500.00),
# MAGIC   (107, 'Grace Lee', 'grace@example.com', '2026-04-15', 'Singapore', 5500.00);
# MAGIC
# MAGIC -- Verify the insert
# MAGIC SELECT 
# MAGIC   '📊 Total Customers' as metric,
# MAGIC   COUNT(*) as count
# MAGIC FROM main.phase4_day20_delta_demo.customers_managed
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   '🌍 Countries' as metric,
# MAGIC   COUNT(DISTINCT country) as count
# MAGIC FROM main.phase4_day20_delta_demo.customers_managed;

# COMMAND ----------

# DBTITLE 1,INSERT Operation - Conditional Append
# INSERT with conditional logic (avoiding duplicates)

from decimal import Decimal
from pyspark.sql.functions import to_date, col
from pyspark.sql.types import DecimalType

# New customer data (some may be duplicates)
new_customers = [
    (108, "Henry Wilson", "henry@example.com", "2026-04-18", "USA", Decimal("3800.00")),
    (109, "Iris Martinez", "iris@example.com", "2026-04-19", "Mexico", Decimal("4100.00")),
    (101, "Alice Johnson", "alice@example.com", "2026-01-15", "USA", Decimal("5000.00"))  # Duplicate!
]

new_customers_df = spark.createDataFrame(
    new_customers,
    ["customer_id", "customer_name", "email", "signup_date", "country", "lifetime_value"]
).withColumn("signup_date", to_date("signup_date")) \
 .withColumn("lifetime_value", col("lifetime_value").cast(DecimalType(10, 2)))

# Get existing customer IDs (avoid RDD - not allowed on serverless)
existing_ids = [row.customer_id for row in spark.table("main.phase4_day20_delta_demo.customers_managed").select("customer_id").collect()]

# Filter out duplicates
filtered_customers_df = new_customers_df.filter(
    ~new_customers_df.customer_id.isin(existing_ids)
)

print(f"🚨 Duplicate check: {new_customers_df.count()} new records, {filtered_customers_df.count()} after deduplication")

# Insert only non-duplicates
if filtered_customers_df.count() > 0:
    filtered_customers_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("main.phase4_day20_delta_demo.customers_managed")
    print(f"✅ Inserted {filtered_customers_df.count()} new customers (duplicates skipped)")
else:
    print("⚠️ All records were duplicates - no insert performed")

print(f"\n📊 Final customer count: {spark.table('main.phase4_day20_delta_demo.customers_managed').count()}")
display(spark.table("main.phase4_day20_delta_demo.customers_managed").orderBy("customer_id"))

# COMMAND ----------

# DBTITLE 1,Section 5: UPDATE Operations
# MAGIC %md
# MAGIC # ✏️ Section 5: UPDATE Operations (Data Modification)
# MAGIC
# MAGIC ## 🧒 ELI5 Explanation:
# MAGIC UPDATE is like fixing or changing your toys. You found out your red car is actually blue? You UPDATE its color! You don't throw away the car and get a new one — you just change the color information.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC Delta Lake supports **ACID-compliant UPDATE** operations with:
# MAGIC
# MAGIC ### Key Features:
# MAGIC 1. **Row-Level Updates**: Modify specific rows based on conditions
# MAGIC 2. **Column Updates**: Update one or multiple columns
# MAGIC 3. **Conditional Updates**: Use WHERE clause for targeted updates
# MAGIC 4. **ACID Guarantees**: Transactional consistency
# MAGIC 5. **Versioning**: Delta maintains history (time travel enabled)
# MAGIC
# MAGIC ### Syntax:
# MAGIC ```sql
# MAGIC UPDATE <catalog>.<schema>.<table>
# MAGIC SET column1 = value1, column2 = value2
# MAGIC WHERE condition;
# MAGIC ```
# MAGIC
# MAGIC ### Best Practices:
# MAGIC - **Always use WHERE clause** to avoid updating entire table
# MAGIC - **Test with SELECT first** to verify affected rows
# MAGIC - **Update in batches** for large datasets
# MAGIC - **Use Delta history** to track changes
# MAGIC - **Avoid frequent small updates** (merge them when possible)
# MAGIC
# MAGIC ### Performance Considerations:
# MAGIC - UPDATE rewrites affected data files
# MAGIC - Use OPTIMIZE after many small updates
# MAGIC - Consider partition pruning for better performance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Advantage over Parquet:
# MAGIC Parquet files are **immutable** — you can't update them. You'd have to:
# MAGIC 1. Read entire table
# MAGIC 2. Modify data in memory
# MAGIC 3. Rewrite entire table
# MAGIC
# MAGIC Delta Lake enables **efficient row-level updates** without full table rewrites!

# COMMAND ----------

# DBTITLE 1,UPDATE Operation - SQL Method
# MAGIC %sql
# MAGIC -- Example 1: Update single column for specific customers
# MAGIC
# MAGIC -- First, let's see customers from USA
# MAGIC SELECT customer_id, customer_name, country, lifetime_value
# MAGIC FROM main.phase4_day20_delta_demo.customers_managed
# MAGIC WHERE country = 'USA'
# MAGIC ORDER BY customer_id;

# COMMAND ----------

# DBTITLE 1,UPDATE - Increase Lifetime Value
# MAGIC %sql
# MAGIC -- UPDATE: Increase lifetime value by 10% for USA customers
# MAGIC
# MAGIC UPDATE main.phase4_day20_delta_demo.customers_managed
# MAGIC SET lifetime_value = lifetime_value * 1.10
# MAGIC WHERE country = 'USA';
# MAGIC
# MAGIC -- Verify the update
# MAGIC SELECT 
# MAGIC   customer_id, 
# MAGIC   customer_name, 
# MAGIC   country, 
# MAGIC   lifetime_value,
# MAGIC   '✅ Updated +10%' as status
# MAGIC FROM main.phase4_day20_delta_demo.customers_managed
# MAGIC WHERE country = 'USA'
# MAGIC ORDER BY customer_id;

# COMMAND ----------

# DBTITLE 1,UPDATE Operation - PySpark Method
# UPDATE using PySpark with Delta Lake API

from delta.tables import DeltaTable

# Get Delta table reference
delta_table = DeltaTable.forName(spark, "main.phase4_day20_delta_demo.customers_managed")

# Update: Add suffix to email for customers with high lifetime value
delta_table.update(
    condition = "lifetime_value > 5000",
    set = {
        "email": "concat(email, '-VIP')"
    }
)

print("✅ Updated email addresses for VIP customers (lifetime_value > 5000)")

# Display updated records
display(
    spark.table("main.phase4_day20_delta_demo.customers_managed")
    .filter("lifetime_value > 5000")
    .orderBy("customer_id")
)

# COMMAND ----------

# DBTITLE 1,UPDATE - Multiple Columns
# MAGIC %sql
# MAGIC -- UPDATE: Multiple columns at once
# MAGIC
# MAGIC UPDATE main.phase4_day20_delta_demo.customers_managed
# MAGIC SET 
# MAGIC   country = 'United States',
# MAGIC   lifetime_value = ROUND(lifetime_value, 2)
# MAGIC WHERE country = 'USA';
# MAGIC
# MAGIC -- Verify multi-column update
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   country,
# MAGIC   lifetime_value
# MAGIC FROM main.phase4_day20_delta_demo.customers_managed
# MAGIC WHERE country = 'United States'
# MAGIC ORDER BY customer_id;

# COMMAND ----------

# DBTITLE 1,Section 6: DELETE Operations
# MAGIC %md
# MAGIC # 🗑️ Section 6: DELETE Operations (Data Removal)
# MAGIC
# MAGIC ## 🧒 ELI5 Explanation:
# MAGIC DELETE is like throwing away broken toys. You pick specific toys (using WHERE) and remove them. Without WHERE, you'd throw away ALL toys — so be careful!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC Delta Lake supports **ACID-compliant DELETE** operations:
# MAGIC
# MAGIC ### Key Features:
# MAGIC 1. **Row-Level Deletes**: Remove specific rows based on conditions
# MAGIC 2. **Conditional Deletes**: Use WHERE clause for targeted removal
# MAGIC 3. **ACID Guarantees**: Transactional consistency
# MAGIC 4. **Time Travel**: Deleted data can be recovered from history
# MAGIC 5. **Soft Deletes**: Can implement soft delete patterns
# MAGIC
# MAGIC ### Syntax:
# MAGIC ```sql
# MAGIC DELETE FROM <catalog>.<schema>.<table>
# MAGIC WHERE condition;
# MAGIC ```
# MAGIC
# MAGIC ### ⚠️ Critical Warning:
# MAGIC **ALWAYS use WHERE clause** unless you intentionally want to delete ALL rows!
# MAGIC
# MAGIC ```sql
# MAGIC -- ❌ DANGEROUS: Deletes ALL rows
# MAGIC DELETE FROM table_name;
# MAGIC
# MAGIC -- ✅ SAFE: Deletes specific rows
# MAGIC DELETE FROM table_name WHERE condition;
# MAGIC ```
# MAGIC
# MAGIC ### Best Practices:
# MAGIC 1. **Test with SELECT first**: Verify which rows will be deleted
# MAGIC 2. **Use transactions**: Wrap in explicit transactions for complex operations
# MAGIC 3. **Consider soft deletes**: Use is_deleted flag instead of hard deletes
# MAGIC 4. **Backup critical data**: Use Delta history or backups
# MAGIC 5. **Optimize after deletes**: Run OPTIMIZE to reclaim space
# MAGIC
# MAGIC ### Performance:
# MAGIC - DELETE rewrites affected data files
# MAGIC - VACUUM removes deleted data files (after retention period)
# MAGIC - Use partition pruning for better performance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Advantage over Parquet:
# MAGIC Parquet doesn't support DELETE. You'd have to:
# MAGIC 1. Read entire table
# MAGIC 2. Filter out unwanted rows
# MAGIC 3. Rewrite entire table
# MAGIC
# MAGIC Delta Lake enables **efficient row-level deletes**!

# COMMAND ----------

# DBTITLE 1,DELETE Operation - Preview First
# MAGIC %sql
# MAGIC -- BEST PRACTICE: Always SELECT first to preview what will be deleted
# MAGIC
# MAGIC -- Preview: See customers with low lifetime value
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   country,
# MAGIC   lifetime_value,
# MAGIC   '🚨 Will be deleted' as status
# MAGIC FROM main.phase4_day20_delta_demo.customers_managed
# MAGIC WHERE lifetime_value < 3000
# MAGIC ORDER BY customer_id;

# COMMAND ----------

# DBTITLE 1,DELETE Operation - Execute
# MAGIC %sql
# MAGIC -- DELETE: Remove customers with low lifetime value (< 3000)
# MAGIC
# MAGIC DELETE FROM main.phase4_day20_delta_demo.customers_managed
# MAGIC WHERE lifetime_value < 3000;
# MAGIC
# MAGIC -- Verify deletion
# MAGIC SELECT 
# MAGIC   COUNT(*) as remaining_customers,
# MAGIC   MIN(lifetime_value) as min_ltv,
# MAGIC   MAX(lifetime_value) as max_ltv,
# MAGIC   AVG(lifetime_value) as avg_ltv
# MAGIC FROM main.phase4_day20_delta_demo.customers_managed;

# COMMAND ----------

# DBTITLE 1,DELETE Operation - PySpark Method
# DELETE using PySpark with Delta Lake API

from delta.tables import DeltaTable

# Get Delta table reference
delta_table = DeltaTable.forName(spark, "main.phase4_day20_delta_demo.customers_managed")

# Preview: Count customers from specific country
country_to_remove = "Germany"
count_before = spark.table("main.phase4_day20_delta_demo.customers_managed").count()
count_to_delete = spark.table("main.phase4_day20_delta_demo.customers_managed") \
    .filter(f"country = '{country_to_remove}'").count()

print(f"📊 Total customers before delete: {count_before}")
print(f"🚨 Customers to be deleted from {country_to_remove}: {count_to_delete}")

# Execute DELETE
if count_to_delete > 0:
    delta_table.delete(f"country = '{country_to_remove}'")
    print(f"\n✅ Deleted {count_to_delete} customers from {country_to_remove}")
    
    count_after = spark.table("main.phase4_day20_delta_demo.customers_managed").count()
    print(f"📊 Total customers after delete: {count_after}")
    print(f"✅ Verification: {count_before - count_after} rows deleted")
else:
    print(f"\n⚠️ No customers found from {country_to_remove}")

# Display remaining customers
display(
    spark.table("main.phase4_day20_delta_demo.customers_managed")
    .orderBy("customer_id")
)

# COMMAND ----------

# DBTITLE 1,Section 7: Hands-on Delta Table Pipeline
# MAGIC %md
# MAGIC # 🚀 Section 7: Hands-on Delta Table Pipeline
# MAGIC
# MAGIC ## 🎯 Objective:
# MAGIC Build a complete end-to-end Delta table pipeline demonstrating:
# MAGIC 1. Create managed Delta table
# MAGIC 2. Insert initial data
# MAGIC 3. Update records based on business logic
# MAGIC 4. Delete obsolete records
# MAGIC 5. Query final dataset
# MAGIC 6. Verify data integrity
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📋 Pipeline Workflow:
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────┐
# MAGIC │  1. CREATE TABLE  │
# MAGIC └────────┬─────────┘
# MAGIC          │
# MAGIC          │
# MAGIC ┌────────┴─────────┐
# MAGIC │  2. INSERT DATA   │
# MAGIC └────────┬─────────┘
# MAGIC          │
# MAGIC          │
# MAGIC ┌────────┴─────────┐
# MAGIC │  3. UPDATE ROWS   │
# MAGIC └────────┬─────────┘
# MAGIC          │
# MAGIC          │
# MAGIC ┌────────┴─────────┐
# MAGIC │  4. DELETE ROWS   │
# MAGIC └────────┬─────────┘
# MAGIC          │
# MAGIC          │
# MAGIC ┌────────┴─────────┐
# MAGIC │  5. QUERY DATA    │
# MAGIC └──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Let's build a **Product Inventory Management** pipeline:

# COMMAND ----------

# DBTITLE 1,Step 1: Create Product Inventory Table
# Step 1: Create Product Inventory Managed Table

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, TimestampType
from pyspark.sql.functions import current_timestamp, lit

print("✅ STEP 1: Creating Product Inventory Table")
print("="*70)

# Create empty DataFrame with schema
inventory_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("stock_quantity", IntegerType(), False),
    StructField("unit_price", DecimalType(10,2), False),
    StructField("last_updated", TimestampType(), False),
    StructField("status", StringType(), False)
])

# Create empty DataFrame
empty_df = spark.createDataFrame([], inventory_schema)

# Write as managed Delta table
table_name = "main.phase4_day20_delta_demo.product_inventory"

empty_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(table_name)

print(f"✅ Created managed Delta table: {table_name}")
print(f"📋 Initial row count: {spark.table(table_name).count()}")
print("\n✅ STEP 1 COMPLETE\n")

# COMMAND ----------

# DBTITLE 1,Step 2: Insert Initial Inventory Data
# Step 2: Insert Initial Product Inventory

from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import IntegerType, DecimalType
from decimal import Decimal

print("✅ STEP 2: Inserting Initial Inventory Data")
print("="*70)

# Create initial inventory data
initial_inventory = [
    (1001, "Laptop Pro 15", "Electronics", 50, Decimal("1299.99"), "Active"),
    (1002, "Wireless Mouse", "Accessories", 200, Decimal("29.99"), "Active"),
    (1003, "USB-C Cable", "Accessories", 500, Decimal("12.99"), "Active"),
    (1004, "Monitor 27in", "Electronics", 75, Decimal("349.99"), "Active"),
    (1005, "Mechanical Keyboard", "Accessories", 120, Decimal("89.99"), "Active"),
    (1006, "Webcam HD", "Electronics", 80, Decimal("79.99"), "Active"),
    (1007, "Desk Lamp", "Furniture", 150, Decimal("45.00"), "Active"),
    (1008, "Office Chair", "Furniture", 30, Decimal("299.99"), "Active"),
    (1009, "Headphones", "Electronics", 100, Decimal("149.99"), "Active"),
    (1010, "Phone Stand", "Accessories", 250, Decimal("19.99"), "Active")
]

df = spark.createDataFrame(
    initial_inventory,
    ["product_id", "product_name", "category", "stock_quantity", "unit_price", "status"]
).withColumn("last_updated", current_timestamp()) \
 .withColumn("product_id", col("product_id").cast(IntegerType())) \
 .withColumn("stock_quantity", col("stock_quantity").cast(IntegerType())) \
 .withColumn("unit_price", col("unit_price").cast(DecimalType(10, 2)))

# Insert data
df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(table_name)

print(f"✅ Inserted {df.count()} products")
print(f"📋 Total row count: {spark.table(table_name).count()}")

# Display inventory
print("\n📊 Current Inventory:")
display(spark.table(table_name).orderBy("product_id"))
print("\n✅ STEP 2 COMPLETE\n")

# COMMAND ----------

# DBTITLE 1,Step 3: UPDATE - Adjust Stock and Prices
# Step 3: UPDATE - Business Logic Updates

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

print("✅ STEP 3: Applying Business Updates")
print("="*70)

delta_table = DeltaTable.forName(spark, table_name)

# Update 1: Reduce stock for products that were sold
print("\n1️⃣ Simulating sales - reducing stock...")
delta_table.update(
    condition = "product_id IN (1001, 1002, 1005, 1009)",
    set = {
        "stock_quantity": "stock_quantity - 10",
        "last_updated": "current_timestamp()"
    }
)
print("✅ Stock reduced for sold items")

# Update 2: Apply discount to slow-moving furniture items
print("\n2️⃣ Applying 20% discount to Furniture category...")
delta_table.update(
    condition = "category = 'Furniture'",
    set = {
        "unit_price": "unit_price * 0.80",
        "last_updated": "current_timestamp()"
    }
)
print("✅ Discount applied to furniture items")

# Update 3: Mark low-stock items
print("\n3️⃣ Marking low-stock items...")
delta_table.update(
    condition = "stock_quantity < 50",
    set = {
        "status": "'Low Stock'",
        "last_updated": "current_timestamp()"
    }
)
print("✅ Low-stock items marked")

print("\n📊 Updated Inventory:")
display(
    spark.table(table_name)
    .orderBy("category", "product_id")
)
print("\n✅ STEP 3 COMPLETE\n")

# COMMAND ----------

# DBTITLE 1,Step 4: DELETE - Remove Discontinued Products
# Step 4: DELETE - Remove obsolete products

print("✅ STEP 4: Removing Discontinued Products")
print("="*70)

# First, mark certain products as discontinued
print("\n1️⃣ Marking products for discontinuation...")
delta_table.update(
    condition = "product_id IN (1003, 1010)",  # USB-C Cable, Phone Stand
    set = {
        "status": "'Discontinued'",
        "last_updated": "current_timestamp()"
    }
)
print("✅ Products marked as discontinued")

# Preview products to be deleted
print("\n2️⃣ Products to be deleted:")
display(
    spark.table(table_name)
    .filter("status = 'Discontinued'")
    .select("product_id", "product_name", "status")
)

count_before = spark.table(table_name).count()

# Delete discontinued products
print("\n3️⃣ Deleting discontinued products...")
delta_table.delete("status = 'Discontinued'")

count_after = spark.table(table_name).count()

print(f"\n✅ Deleted {count_before - count_after} discontinued products")
print(f"📋 Remaining products: {count_after}")

print("\n📊 Final Inventory:")
display(
    spark.table(table_name)
    .orderBy("category", "product_id")
)
print("\n✅ STEP 4 COMPLETE\n")

# COMMAND ----------

# DBTITLE 1,Step 5: Query and Analyze Final Dataset
# MAGIC %sql
# MAGIC -- Step 5: Query and Analyze the Final Dataset
# MAGIC
# MAGIC -- Query 1: Inventory summary by category
# MAGIC SELECT 
# MAGIC   category,
# MAGIC   COUNT(*) as product_count,
# MAGIC   SUM(stock_quantity) as total_stock,
# MAGIC   ROUND(AVG(unit_price), 2) as avg_price,
# MAGIC   ROUND(SUM(stock_quantity * unit_price), 2) as inventory_value
# MAGIC FROM main.phase4_day20_delta_demo.product_inventory
# MAGIC GROUP BY category
# MAGIC ORDER BY inventory_value DESC;

# COMMAND ----------

# DBTITLE 1,Final Analysis: Low Stock Alert
# MAGIC %sql
# MAGIC -- Query 2: Low stock alerts
# MAGIC
# MAGIC SELECT 
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   category,
# MAGIC   stock_quantity,
# MAGIC   unit_price,
# MAGIC   status,
# MAGIC   '🚨 Reorder Required' as alert
# MAGIC FROM main.phase4_day20_delta_demo.product_inventory
# MAGIC WHERE status = 'Low Stock'
# MAGIC ORDER BY stock_quantity ASC;

# COMMAND ----------

# DBTITLE 1,Pipeline Summary
# Pipeline Execution Summary

print("="*70)
print("🏆 DELTA TABLE PIPELINE - EXECUTION SUMMARY")
print("="*70)

final_df = spark.table(table_name)

print("\n📊 Final Statistics:")
print(f"  • Total Products: {final_df.count()}")
print(f"  • Categories: {final_df.select('category').distinct().count()}")
print(f"  • Active Products: {final_df.filter("status = 'Active'").count()}")
print(f"  • Low Stock Products: {final_df.filter("status = 'Low Stock'").count()}")
print(f"  • Total Inventory Value: ${final_df.selectExpr('sum(stock_quantity * unit_price)').first()[0]:,.2f}")

print("\n✅ Operations Completed Successfully:")
print("  1️⃣ CREATE - Managed Delta table created")
print("  2️⃣ INSERT - Initial data loaded (10 products)")
print("  3️⃣ UPDATE - Stock adjusted, prices discounted, status updated")
print("  4️⃣ DELETE - Discontinued products removed (2 products)")
print("  5️⃣ QUERY - Analytics and reporting completed")

print("\n🎉 PIPELINE COMPLETE - All DML operations executed successfully!")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Section 8: Unity Catalog Governance
# MAGIC %md
# MAGIC # 🔒 Section 8: Unity Catalog Governance for Delta Tables
# MAGIC
# MAGIC ## 🧒 ELI5 Explanation:
# MAGIC Unity Catalog is like a super organized library system. It knows where every book (table) is, who can read it, who wrote it, and keeps a history of who checked it out. Everything is tracked and protected!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### Three-Level Namespace:
# MAGIC ```
# MAGIC Catalog → Schema → Table
# MAGIC ```
# MAGIC
# MAGIC **Example**: `main.phase4_day20_delta_demo.customers_managed`
# MAGIC
# MAGIC * **Catalog**: Top-level container (e.g., `main`, `dev`, `prod`)
# MAGIC * **Schema**: Logical grouping within catalog (e.g., `sales`, `marketing`)
# MAGIC * **Table**: Actual Delta table
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔑 Governance Features:
# MAGIC
# MAGIC ### 1. **Access Control (RBAC)**
# MAGIC ```sql
# MAGIC -- Grant read access
# MAGIC GRANT SELECT ON TABLE catalog.schema.table TO `user@company.com`;
# MAGIC
# MAGIC -- Grant write access
# MAGIC GRANT INSERT, UPDATE, DELETE ON TABLE catalog.schema.table TO `data_engineers`;
# MAGIC
# MAGIC -- Revoke access
# MAGIC REVOKE SELECT ON TABLE catalog.schema.table FROM `user@company.com`;
# MAGIC ```
# MAGIC
# MAGIC ### 2. **Lineage Tracking**
# MAGIC * Automatic tracking of data flow
# MAGIC * See which tables feed into others
# MAGIC * Identify downstream impact of changes
# MAGIC * Accessible via Unity Catalog UI
# MAGIC
# MAGIC ### 3. **Audit Logging**
# MAGIC * All access is logged
# MAGIC * Who accessed what, when
# MAGIC * What operations were performed
# MAGIC * Compliance and security tracking
# MAGIC
# MAGIC ### 4. **Data Discovery**
# MAGIC * Searchable metadata
# MAGIC * Column-level comments
# MAGIC * Table descriptions
# MAGIC * Owner information
# MAGIC
# MAGIC ### 5. **Schema Evolution**
# MAGIC * Track schema changes over time
# MAGIC * Version history maintained
# MAGIC * Safe schema modifications
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Managed Tables Governance Advantages:
# MAGIC
# MAGIC | Feature | Managed Tables | External Tables |
# MAGIC |---------|----------------|------------------|
# MAGIC | **Metadata Governance** | ✅ Full | ✅ Full |
# MAGIC | **Data Governance** | ✅ Full | ⚠️ Partial (storage independent) |
# MAGIC | **Lifecycle Management** | ✅ Automatic | ❌ Manual |
# MAGIC | **DROP TABLE Behavior** | Deletes data + metadata | Deletes metadata only |
# MAGIC | **Audit Logging** | ✅ Complete | ✅ Complete (metadata only) |
# MAGIC | **Access Control** | ✅ Unity Catalog | ✅ Unity Catalog + Cloud IAM |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛡️ Best Practices:
# MAGIC
# MAGIC 1. **Use Managed Tables**: Default choice for production
# MAGIC 2. **Organize by Catalog**: `dev`, `staging`, `prod`
# MAGIC 3. **Logical Schema Design**: Group related tables
# MAGIC 4. **Add Comments**: Document tables and columns
# MAGIC 5. **Set Owners**: Clear ownership and responsibility
# MAGIC 6. **Regular Access Reviews**: Audit and revoke unused permissions
# MAGIC 7. **Tag Critical Data**: Use Unity Catalog tags for PII, sensitive data
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 9: Genie Code Agent Examples
# MAGIC %md
# MAGIC # 🧞 Genie Code Agent — Prompt Examples
# MAGIC
# MAGIC ## 🚀 How to Use Genie Code for Delta Table Operations:
# MAGIC
# MAGIC ### 📝 Example Prompts:
# MAGIC
# MAGIC #### 1️⃣ Create Managed Delta Table
# MAGIC ```
# MAGIC Prompt: "Create a managed Delta table for storing customer transactions 
# MAGIC with columns: transaction_id, customer_id, amount, transaction_date, 
# MAGIC and payment_method"
# MAGIC
# MAGIC Expected Output:
# MAGIC - Creates table in Unity Catalog
# MAGIC - Uses managed storage
# MAGIC - Adds appropriate data types
# MAGIC - Includes comments for documentation
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2️⃣ Convert External to Managed Table
# MAGIC ```
# MAGIC Prompt: "I have an external Delta table at /mnt/data/sales/. 
# MAGIC Convert it to a managed table in Unity Catalog"
# MAGIC
# MAGIC Expected Output:
# MAGIC - Reads data from external location
# MAGIC - Creates managed table
# MAGIC - Migrates data
# MAGIC - Verifies migration
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 3️⃣ Perform Delta UPDATE Operation
# MAGIC ```
# MAGIC Prompt: "Update the customer_status to 'VIP' for all customers 
# MAGIC with lifetime_value > 10000 in the customers table"
# MAGIC
# MAGIC Expected Output:
# MAGIC - Uses Delta UPDATE syntax
# MAGIC - Applies WHERE clause
# MAGIC - Shows before/after counts
# MAGIC - Verifies the update
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 4️⃣ Safe DELETE with Preview
# MAGIC ```
# MAGIC Prompt: "Delete customers who haven't made a purchase in the last 2 years. 
# MAGIC Show me which records will be deleted first"
# MAGIC
# MAGIC Expected Output:
# MAGIC - SELECT query to preview deletions
# MAGIC - Confirmation before DELETE
# MAGIC - DELETE with proper WHERE clause
# MAGIC - Verification of deletion count
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 5️⃣ Design Governed Data Model
# MAGIC ```
# MAGIC Prompt: "Design a 3-table data model for e-commerce (customers, orders, products) 
# MAGIC using managed Delta tables with proper governance"
# MAGIC
# MAGIC Expected Output:
# MAGIC - Creates catalog and schema
# MAGIC - Defines 3 related tables
# MAGIC - Sets up foreign key relationships
# MAGIC - Adds comments and documentation
# MAGIC - Suggests access control policies
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 6️⃣ Optimize Delta Table
# MAGIC ```
# MAGIC Prompt: "My orders table has many small files after frequent updates. 
# MAGIC Optimize it for better query performance"
# MAGIC
# MAGIC Expected Output:
# MAGIC - Runs OPTIMIZE command
# MAGIC - Explains Z-ordering for commonly filtered columns
# MAGIC - Shows file statistics before/after
# MAGIC - Suggests VACUUM to clean up old files
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Pro Tips:
# MAGIC
# MAGIC * **Be Specific**: Mention table names, columns, and conditions clearly
# MAGIC * **Ask for Explanations**: "Explain why..." or "What's the difference between..."
# MAGIC * **Request Best Practices**: "What's the best way to..."
# MAGIC * **Iterate**: Start with simple prompts, refine based on results
# MAGIC * **Leverage Governance**: Ask about access control, lineage, audit logs
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Final Summary and Interview Questions
# MAGIC %md
# MAGIC # 🎯 Final Summary: Delta Tables & DML Operations
# MAGIC
# MAGIC ## 📝 Key Learnings:
# MAGIC
# MAGIC ### 1. Delta Table Types
# MAGIC * **Managed Tables**: Unity Catalog manages both metadata and data (RECOMMENDED)
# MAGIC * **External Tables**: Unity Catalog manages metadata; user manages data storage
# MAGIC * **Default Choice**: Managed tables for governance, simplicity, and optimization
# MAGIC
# MAGIC ### 2. DML Operations
# MAGIC * **INSERT**: Add new rows (ACID-compliant, schema enforcement)
# MAGIC * **UPDATE**: Modify existing rows (row-level updates, conditional logic)
# MAGIC * **DELETE**: Remove rows (safe with WHERE clause, time travel recovery)
# MAGIC
# MAGIC ### 3. Unity Catalog Governance
# MAGIC * Three-level namespace: Catalog → Schema → Table
# MAGIC * RBAC access control
# MAGIC * Lineage tracking and audit logging
# MAGIC * Managed tables offer superior governance
# MAGIC
# MAGIC ### 4. Best Practices
# MAGIC * ✅ Use managed tables by default
# MAGIC * ✅ Always use WHERE clause for UPDATE/DELETE
# MAGIC * ✅ Test with SELECT before DELETE
# MAGIC * ✅ Leverage Unity Catalog for governance
# MAGIC * ✅ Use DML operations instead of full table rewrites
# MAGIC * ❌ Avoid INSERT OVERWRITE in production
# MAGIC * ❌ Never use DELETE without WHERE (unless intentional)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎬 Interview Questions (10):
# MAGIC
# MAGIC ### 1. **Basic Understanding**
# MAGIC **Q**: What is the main difference between managed and external Delta tables in Unity Catalog?
# MAGIC
# MAGIC **A**: Managed tables have both metadata and data files managed by Unity Catalog. When you DROP a managed table, both are deleted. External tables only have metadata managed by Unity Catalog; data files remain in the external location after DROP TABLE. Managed tables are recommended for production due to better governance.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. **DML Operations**
# MAGIC **Q**: Why can Delta Lake perform UPDATE and DELETE operations while Parquet cannot?
# MAGIC
# MAGIC **A**: Parquet files are immutable — once written, they cannot be modified. Delta Lake maintains a transaction log that tracks changes. For UPDATE/DELETE, Delta Lake rewrites only affected data files and updates the log, enabling efficient row-level modifications without full table rewrites.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. **Safety & Best Practices**
# MAGIC **Q**: What is the most critical safety practice when using DELETE in production?
# MAGIC
# MAGIC **A**: Always use a WHERE clause unless you intentionally want to delete all rows. Before executing DELETE, run a SELECT with the same WHERE condition to preview which rows will be deleted. This prevents accidental data loss.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4. **Governance**
# MAGIC **Q**: Explain Unity Catalog's three-level namespace and its benefits.
# MAGIC
# MAGIC **A**: Unity Catalog uses `catalog.schema.table` hierarchy. **Catalog** is the top-level container (e.g., dev/prod), **Schema** groups related tables, and **Table** is the actual Delta table. Benefits include organized data structure, environment isolation, granular access control, and clear ownership.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5. **Performance**
# MAGIC **Q**: When should you run OPTIMIZE on a Delta table?
# MAGIC
# MAGIC **A**: Run OPTIMIZE after many small UPDATE/DELETE operations that create many small files. OPTIMIZE compacts small files into larger ones for better query performance. For tables with frequent updates, schedule OPTIMIZE regularly (e.g., daily).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6. **ACID Properties**
# MAGIC **Q**: What ACID guarantees does Delta Lake provide for DML operations?
# MAGIC
# MAGIC **A**: 
# MAGIC * **Atomicity**: All-or-nothing — entire operation succeeds or fails
# MAGIC * **Consistency**: Schema enforcement and data validation
# MAGIC * **Isolation**: Concurrent readers see consistent snapshots
# MAGIC * **Durability**: Changes are permanently stored with transaction log
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 7. **Schema Management**
# MAGIC **Q**: How does Delta Lake handle schema enforcement during INSERT operations?
# MAGIC
# MAGIC **A**: Delta Lake validates incoming data against the table schema. If data types don't match or required columns are missing, the operation fails. You can use `mergeSchema` option for schema evolution, but it must be explicitly enabled.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 8. **Time Travel**
# MAGIC **Q**: If you accidentally DELETE rows from a Delta table, how can you recover them?
# MAGIC
# MAGIC **A**: Use Delta Lake time travel:
# MAGIC ```sql
# MAGIC -- View table at previous version
# MAGIC SELECT * FROM table_name VERSION AS OF <version_number>
# MAGIC
# MAGIC -- Or use timestamp
# MAGIC SELECT * FROM table_name TIMESTAMP AS OF '<timestamp>'
# MAGIC
# MAGIC -- Restore to previous version
# MAGIC RESTORE TABLE table_name TO VERSION AS OF <version_number>
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 9. **Architecture Decision**
# MAGIC **Q**: When would you choose an external table over a managed table?
# MAGIC
# MAGIC **A**: Use external tables when:
# MAGIC 1. Data must remain in specific cloud storage for compliance
# MAGIC 2. Multiple systems need access to the same data files
# MAGIC 3. Migrating legacy data to Unity Catalog without moving files
# MAGIC 4. Sharing data across multiple Databricks workspaces
# MAGIC
# MAGIC Otherwise, prefer managed tables for better governance.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 10. **Real-World Scenario**
# MAGIC **Q**: You need to update 1 million rows in a 100 million row Delta table. What's the best approach?
# MAGIC
# MAGIC **A**: 
# MAGIC 1. Use UPDATE with a precise WHERE clause to target only affected rows
# MAGIC 2. Delta Lake rewrites only data files containing modified rows (not entire table)
# MAGIC 3. After UPDATE, monitor file sizes; run OPTIMIZE if many small files are created
# MAGIC 4. Consider partitioning strategy for better performance on future updates
# MAGIC 5. Use Delta history to track the change and enable rollback if needed
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚠️ Common Mistakes to Avoid:
# MAGIC
# MAGIC ### 1. **Using External Tables Unnecessarily**
# MAGIC * **Mistake**: Creating external tables by default
# MAGIC * **Impact**: Lose automatic lifecycle management, harder governance
# MAGIC * **Solution**: Use managed tables unless specific requirement exists
# MAGIC
# MAGIC ### 2. **Full Table Overwrite Instead of DML**
# MAGIC * **Mistake**: Reading entire table, modifying in memory, rewriting with mode("overwrite")
# MAGIC * **Impact**: Inefficient, breaks time travel history, loses concurrent write safety
# MAGIC * **Solution**: Use UPDATE/DELETE for modifications
# MAGIC
# MAGIC ### 3. **Ignoring Governance**
# MAGIC * **Mistake**: Creating tables without comments, owners, or access controls
# MAGIC * **Impact**: Unclear ownership, compliance issues, security risks
# MAGIC * **Solution**: Always add COMMENT, set owners, configure permissions
# MAGIC
# MAGIC ### 4. **Direct File Manipulation**
# MAGIC * **Mistake**: Manually deleting/modifying Delta files in cloud storage
# MAGIC * **Impact**: Corrupts Delta transaction log, breaks table
# MAGIC * **Solution**: Always use Delta SQL/API; use VACUUM for cleanup
# MAGIC
# MAGIC ### 5. **DELETE Without WHERE**
# MAGIC * **Mistake**: `DELETE FROM table_name;` (deletes all rows)
# MAGIC * **Impact**: Catastrophic data loss
# MAGIC * **Solution**: Always use WHERE clause; test with SELECT first
# MAGIC
# MAGIC ### 6. **Not Using OPTIMIZE**
# MAGIC * **Mistake**: Ignoring small file problem after frequent updates
# MAGIC * **Impact**: Degraded query performance
# MAGIC * **Solution**: Schedule regular OPTIMIZE, especially after heavy DML
# MAGIC
# MAGIC ### 7. **Mixing Managed and External**
# MAGIC * **Mistake**: Inconsistent table type strategy
# MAGIC * **Impact**: Confusion, governance gaps, operational complexity
# MAGIC * **Solution**: Standardize on managed tables with documented exceptions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏆 Congratulations!
# MAGIC
# MAGIC You've completed **Phase 4 Day 20** on Delta Tables and DML Operations. You now understand:
# MAGIC
# MAGIC ✅ Managed vs External Delta tables  
# MAGIC ✅ INSERT, UPDATE, DELETE operations  
# MAGIC ✅ Unity Catalog governance  
# MAGIC ✅ Best practices for production environments  
# MAGIC ✅ Common pitfalls and how to avoid them  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Next Steps:
# MAGIC 1. Run all cells in this notebook to see operations in action
# MAGIC 2. Experiment with your own data
# MAGIC 3. Explore Delta Lake time travel (future topic)
# MAGIC 4. Learn about MERGE operations (upserts)
# MAGIC 5. Study partition strategies for large tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📚 Additional Resources:
# MAGIC * [Databricks Delta Lake Documentation](https://docs.databricks.com/delta/index.html)
# MAGIC * [Unity Catalog Best Practices](https://docs.databricks.com/data-governance/unity-catalog/best-practices.html)
# MAGIC * [Delta Lake Transaction Log Deep Dive](https://docs.databricks.com/delta/delta-transactions.html)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **🏷️ Watermark: @TRRaveendra | Phase 4 Day 20 | Delta Tables & DML Operations**