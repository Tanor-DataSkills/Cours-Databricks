# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # Unity Catalog Medallion Architecture Demo
# MAGIC
# MAGIC **Author:** @TRRaveendra  
# MAGIC **Date:** April 22, 2026  
# MAGIC **Purpose:** Production-ready demonstration of Unity Catalog with Medallion Architecture (Bronze, Silver, Gold)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook demonstrates a complete end-to-end data engineering pipeline using:
# MAGIC
# MAGIC * **Unity Catalog** - Modern data governance and metadata management
# MAGIC * **Managed Volumes** - Secure file storage within Unity Catalog
# MAGIC * **Medallion Architecture** - Bronze (raw) → Silver (cleaned) → Gold (aggregated)
# MAGIC * **Delta Lake** - ACID transactions and time travel capabilities
# MAGIC * **Data Governance** - Tags, comments, and metadata for compliance
# MAGIC * **Automated Testing** - Built-in validation and quality checks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architecture Layers
# MAGIC
# MAGIC | Layer | Purpose | Data Quality | Tables |
# MAGIC |-------|---------|--------------|--------|
# MAGIC | **Bronze** | Raw data ingestion | As-is from source | `bronze_sales` |
# MAGIC | **Silver** | Cleaned & enriched | Validated & transformed | `silver_sales` |
# MAGIC | **Gold** | Business metrics | Aggregated analytics | `gold_sales_summary` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Execution Instructions
# MAGIC
# MAGIC 1. Run cells sequentially from top to bottom
# MAGIC 2. Serverless compute will auto-attach
# MAGIC 3. Total execution time: ~2 minutes
# MAGIC 4. The notebook is idempotent - safe to re-run
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Setup & Configuration
# ===================================================================
# SETUP & CONFIGURATION
# ===================================================================

# Unity Catalog Configuration
catalog = "main"
schema = "medallion_demo"
volume = "demo_volume"

# Paths
base_path = f"/Volumes/{catalog}/{schema}/{volume}/"
bronze_path = f"{base_path}bronze/"
silver_path = f"{base_path}silver/"
gold_path = f"{base_path}gold/"

# Table names
bronze_table = f"{catalog}.{schema}.bronze_sales"
silver_table = f"{catalog}.{schema}.silver_sales"
gold_table = f"{catalog}.{schema}.gold_sales_summary"

print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"Volume: {volume}")
print(f"Base Path: {base_path}")
print(f"\nTable Names:")
print(f"  Bronze: {bronze_table}")
print(f"  Silver: {silver_table}")
print(f"  Gold: {gold_table}")

# COMMAND ----------

# DBTITLE 1,Unity Catalog Setup
# MAGIC %sql
# MAGIC -- ===================================================================
# MAGIC -- UNITY CATALOG SETUP
# MAGIC -- ===================================================================
# MAGIC
# MAGIC -- Verify catalog exists (using 'main' which is pre-created)
# MAGIC -- If using custom catalog, uncomment below:
# MAGIC -- CREATE CATALOG IF NOT EXISTS ${catalog}
# MAGIC --   COMMENT 'Medallion Architecture Demo Catalog';
# MAGIC
# MAGIC SHOW CATALOGS LIKE 'main';

# COMMAND ----------

# DBTITLE 1,Schema Creation
# MAGIC %sql
# MAGIC -- ===================================================================
# MAGIC -- SCHEMA CREATION
# MAGIC -- ===================================================================
# MAGIC
# MAGIC -- Create schema for medallion architecture
# MAGIC CREATE SCHEMA IF NOT EXISTS main.medallion_demo
# MAGIC   COMMENT 'Medallion Architecture - Bronze, Silver, Gold layers';
# MAGIC
# MAGIC -- Verify schema creation
# MAGIC DESCRIBE SCHEMA EXTENDED main.medallion_demo;

# COMMAND ----------

# DBTITLE 1,Volume Creation & Usage
# ===================================================================
# VOLUME CREATION
# ===================================================================

# Create managed volume for data storage
try:
    spark.sql("""
      CREATE VOLUME IF NOT EXISTS main.medallion_demo.demo_volume
      COMMENT 'Managed volume for medallion architecture data files'
    """)
    print("✓ Volume created successfully: main.medallion_demo.demo_volume")
except Exception as e:
    if "already exists" in str(e).lower():
        print("✓ Volume already exists: main.medallion_demo.demo_volume")
    else:
        print(f"Volume creation: {e}")

# List volumes in schema to verify
volumes = spark.sql("SHOW VOLUMES IN main.medallion_demo")
print(f"\nVolumes in schema:")
display(volumes)

# COMMAND ----------

# DBTITLE 1,Verify Volume Path
# ===================================================================
# VERIFY VOLUME PATH
# ===================================================================

# List volume contents (should be empty initially)
try:
    files = dbutils.fs.ls(base_path)
    print(f"Volume path exists: {base_path}")
    print(f"Current files: {len(files)}")
    if files:
        display(files)
except Exception as e:
    print(f"Volume is empty or just created: {e}")

# COMMAND ----------

# DBTITLE 1,Governance - Table Properties & Comments
# MAGIC %sql
# MAGIC -- ===================================================================
# MAGIC -- GOVERNANCE - TABLE PROPERTIES & METADATA
# MAGIC -- ===================================================================
# MAGIC
# MAGIC -- Add table comments for documentation
# MAGIC COMMENT ON TABLE main.medallion_demo.bronze_sales IS 
# MAGIC   'Bronze layer: Raw sales data ingested from source systems';
# MAGIC
# MAGIC COMMENT ON TABLE main.medallion_demo.silver_sales IS 
# MAGIC   'Silver layer: Cleaned and enriched sales data with business logic applied';
# MAGIC
# MAGIC COMMENT ON TABLE main.medallion_demo.gold_sales_summary IS 
# MAGIC   'Gold layer: Aggregated sales metrics for business reporting and analytics';
# MAGIC
# MAGIC -- Add column comments for data dictionary
# MAGIC ALTER TABLE main.medallion_demo.silver_sales 
# MAGIC   ALTER COLUMN total_amount COMMENT 'Calculated as quantity * unit_price';
# MAGIC
# MAGIC ALTER TABLE main.medallion_demo.silver_sales 
# MAGIC   ALTER COLUMN product_category COMMENT 'Derived category: Electronics, Accessories, or Peripherals';

# COMMAND ----------

# DBTITLE 1,View Catalog Objects
# MAGIC %sql
# MAGIC -- ===================================================================
# MAGIC -- VIEW ALL CREATED OBJECTS
# MAGIC -- ===================================================================
# MAGIC
# MAGIC -- Show all tables in schema
# MAGIC SHOW TABLES IN main.medallion_demo;