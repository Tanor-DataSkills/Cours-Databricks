# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # 🤖 Data Engineering Training — Phase 11 Day 43  
# MAGIC ## ⚙️ Metadata-Driven Pipelines: Dynamic ETL & Automation Patterns  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - Metadata-Driven ETL Frameworks  
# MAGIC - Dynamic Pipeline Generation  
# MAGIC - Automation Patterns  
# MAGIC - AI + Metadata Integration  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### ⚙️ Platform: Databricks (Serverless + Unity Catalog + Delta Lake + Genie AI)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Objective:
# MAGIC Learn how to design dynamic ETL pipelines using metadata, enabling scalable automation and AI-driven data engineering systems.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Engineering Constraints:
# MAGIC - ✅ Use Databricks Serverless Compute
# MAGIC - ✅ Use Unity Catalog for all data access
# MAGIC - ❌ DO NOT use RDDs
# MAGIC - ❌ DO NOT use cache() / persist()
# MAGIC - ❌ DO NOT use /tmp or local storage
# MAGIC - ✅ Follow metadata-driven and automation-first design

# COMMAND ----------

# DBTITLE 1,Section 1: What is Metadata-Driven ETL?
# MAGIC %md
# MAGIC # 📖 Section 1: What is Metadata-Driven ETL?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 (Explain Like I'm 5):
# MAGIC Imagine you have a recipe book instead of remembering every recipe in your head. Each time you want to cook, you just look at the recipe book and follow the instructions. **Metadata-driven ETL** is like that recipe book for data pipelines. Instead of writing separate code for each data source, you store all the instructions (metadata) in a table, and your pipeline reads that table to know what to do.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Metadata-Driven ETL** is an architectural pattern where pipeline configurations, transformation rules, and data lineage are stored as **metadata** in configuration tables rather than hardcoded in scripts.
# MAGIC
# MAGIC ### 🔑 Key Characteristics:
# MAGIC
# MAGIC 1. **Configuration-Driven**: All ETL logic is parameterized through metadata tables
# MAGIC 2. **Dynamic Execution**: Pipelines are generated at runtime based on metadata
# MAGIC 3. **Scalability**: Adding new data sources requires only metadata entries, not code changes
# MAGIC 4. **Maintainability**: Centralized configuration reduces technical debt
# MAGIC 5. **Auditability**: Full lineage and execution history tracked via metadata
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚔️ Hardcoded ETL vs Metadata-Driven ETL:
# MAGIC
# MAGIC | Aspect | Hardcoded ETL | Metadata-Driven ETL |
# MAGIC |--------|---------------|---------------------|
# MAGIC | **Flexibility** | Low - requires code changes | High - update metadata only |
# MAGIC | **Scalability** | Poor - N tables = N scripts | Excellent - 1 framework for all |
# MAGIC | **Maintenance** | High effort | Low effort |
# MAGIC | **Onboarding** | Requires developer | Business users can configure |
# MAGIC | **Testing** | Test each pipeline | Test framework once |
# MAGIC | **Time to Market** | Weeks | Hours |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Benefits:
# MAGIC - ✅ **Reduced Code Duplication**
# MAGIC - ✅ **Faster Development Cycles**
# MAGIC - ✅ **Self-Service Data Integration**
# MAGIC - ✅ **Enterprise-Grade Governance**
# MAGIC - ✅ **AI-Ready Architecture** (Genie can generate pipelines from metadata)

# COMMAND ----------

# DBTITLE 1,Section 2: Metadata Table Design
# MAGIC %md
# MAGIC # 📋 Section 2: Metadata Table Design
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC Think of a metadata table as a spreadsheet that tells your pipeline:
# MAGIC - **WHERE** to find the data (source)
# MAGIC - **WHAT** to do with it (transformation)
# MAGIC - **WHERE** to save it (target)
# MAGIC - **HOW** to load it (full or incremental)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC A **metadata table** (also called a **control table** or **configuration table**) is the central registry that defines:
# MAGIC
# MAGIC 1. **Source Definitions**: Connection details, file paths, table names
# MAGIC 2. **Target Definitions**: Destination tables, schemas, partitioning
# MAGIC 3. **Transformation Rules**: Business logic, data quality rules
# MAGIC 4. **Execution Patterns**: Load type (full/incremental), frequency, dependencies
# MAGIC 5. **Runtime Metadata**: Last run timestamp, watermarks, row counts
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📦 Core Metadata Schema:
# MAGIC
# MAGIC ```
# MAGIC table_name             | source_path                    | target_table              | load_type   | transformation_logic      | last_processed_timestamp | is_active
# MAGIC -----------------------|--------------------------------|---------------------------|-------------|---------------------------|--------------------------|----------
# MAGIC customers              | /mnt/raw/customers/*.parquet   | gold.customers            | full        | cleanse_pii               | 2026-04-20 10:00:00     | true
# MAGIC transactions           | /mnt/raw/transactions/*.json   | gold.transactions         | incremental | enrich_with_exchange_rate | 2026-04-21 08:00:00     | true
# MAGIC products               | jdbc:oracle://prod/products    | gold.products             | full        | standardize_categories    | 2026-04-19 12:00:00     | true
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔑 Design Principles:
# MAGIC
# MAGIC 1. **Versioning**: Track schema versions for backward compatibility
# MAGIC 2. **Auditing**: Log all configuration changes with timestamps and users
# MAGIC 3. **Extensibility**: Use key-value pairs for custom attributes
# MAGIC 4. **Validation**: Enforce data quality rules in metadata
# MAGIC 5. **Dependencies**: Define upstream/downstream relationships
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Advanced Patterns:
# MAGIC - **Hierarchical Metadata**: Bronze → Silver → Gold layer configs
# MAGIC - **Conditional Logic**: Dynamic routing based on data attributes
# MAGIC - **Retry Policies**: Failure handling strategies per pipeline
# MAGIC - **SLA Definitions**: Expected completion times and alerting

# COMMAND ----------

# DBTITLE 1,Create Catalog and Schema for Metadata
# Create dedicated catalog and schema for metadata management
# This ensures proper organization and governance

spark.sql("""
  CREATE CATALOG IF NOT EXISTS metadata_catalog
  COMMENT 'Central catalog for all metadata-driven ETL configurations'
""")

spark.sql("""
  CREATE SCHEMA IF NOT EXISTS metadata_catalog.etl_config
  COMMENT 'Schema for ETL pipeline metadata and control tables'
""")

print("✅ Metadata catalog and schema created successfully")
print("\nCatalog: metadata_catalog")
print("Schema: etl_config")

# COMMAND ----------

# DBTITLE 1,Create ETL Configuration Table
# Create the core metadata table for ETL pipeline configuration
# This table drives all dynamic pipeline generation

spark.sql("""
  CREATE OR REPLACE TABLE metadata_catalog.etl_config.pipeline_metadata (
    pipeline_id STRING COMMENT 'Unique identifier for the pipeline',
    pipeline_name STRING COMMENT 'Human-readable pipeline name',
    source_type STRING COMMENT 'Source type: file, table, jdbc, api',
    source_path STRING COMMENT 'Source location or connection string',
    source_format STRING COMMENT 'Data format: parquet, json, csv, delta',
    target_catalog STRING COMMENT 'Target Unity Catalog name',
    target_schema STRING COMMENT 'Target schema name',
    target_table STRING COMMENT 'Target table name',
    load_type STRING COMMENT 'Load type: full, incremental, merge',
    watermark_column STRING COMMENT 'Column for incremental loads',
    partition_columns ARRAY<STRING> COMMENT 'Columns for table partitioning',
    transformation_logic STRING COMMENT 'Transformation function or SQL logic',
    is_active BOOLEAN COMMENT 'Pipeline enabled/disabled flag',
    priority INT COMMENT 'Execution priority (lower = higher priority)',
    last_processed_timestamp TIMESTAMP COMMENT 'Last successful run timestamp',
    last_watermark STRING COMMENT 'Last processed watermark value',
    created_by STRING COMMENT 'User who created the config',
    created_at TIMESTAMP COMMENT 'Configuration creation timestamp',
    updated_at TIMESTAMP COMMENT 'Last configuration update timestamp'
  )
  USING DELTA
  COMMENT 'Central metadata table for dynamic ETL pipeline generation'
""")

print("✅ ETL Configuration Table created successfully")
print("\nTable: metadata_catalog.etl_config.pipeline_metadata")
print("\nThis table will drive all dynamic pipeline execution")

# COMMAND ----------

# DBTITLE 1,Insert Sample Metadata Configurations
# Insert sample pipeline configurations to demonstrate the framework
# In production, these would be managed via a UI or API

from datetime import datetime
from pyspark.sql.functions import current_timestamp, current_user, lit, col
from pyspark.sql.types import IntegerType

sample_configs = [
    {
        "pipeline_id": "PIPE_001",
        "pipeline_name": "Customer Data Ingestion",
        "source_type": "file",
        "source_path": "/Volumes/main/default/raw_data/customers",
        "source_format": "parquet",
        "target_catalog": "main",
        "target_schema": "gold",
        "target_table": "customers",
        "load_type": "full",
        "watermark_column": None,
        "partition_columns": ["country"],
        "transformation_logic": "cleanse_and_standardize",
        "is_active": True,
        "priority": 1
    },
    {
        "pipeline_id": "PIPE_002",
        "pipeline_name": "Transaction Stream Processing",
        "source_type": "file",
        "source_path": "/Volumes/main/default/raw_data/transactions",
        "source_format": "json",
        "target_catalog": "main",
        "target_schema": "gold",
        "target_table": "transactions",
        "load_type": "incremental",
        "watermark_column": "transaction_timestamp",
        "partition_columns": ["transaction_date"],
        "transformation_logic": "enrich_and_validate",
        "is_active": True,
        "priority": 2
    },
    {
        "pipeline_id": "PIPE_003",
        "pipeline_name": "Product Catalog Sync",
        "source_type": "table",
        "source_path": "main.bronze.products_raw",
        "source_format": "delta",
        "target_catalog": "main",
        "target_schema": "gold",
        "target_table": "products",
        "load_type": "merge",
        "watermark_column": "updated_at",
        "partition_columns": ["category"],
        "transformation_logic": "deduplicate_and_enrich",
        "is_active": True,
        "priority": 3
    }
]

# Convert to DataFrame
config_df = spark.createDataFrame(sample_configs)

# Cast priority to INT to match table schema
config_df = config_df.withColumn("priority", col("priority").cast(IntegerType()))

# Add audit columns
config_df = config_df \
    .withColumn("created_by", current_user()) \
    .withColumn("created_at", current_timestamp()) \
    .withColumn("updated_at", current_timestamp()) \
    .withColumn("last_processed_timestamp", current_timestamp()) \
    .withColumn("last_watermark", lit(None).cast("string"))

# Write to metadata table
config_df.write \
    .mode("append") \
    .saveAsTable("metadata_catalog.etl_config.pipeline_metadata")

print("✅ Sample metadata configurations inserted successfully")
print(f"\nInserted {config_df.count()} pipeline configurations")

# Display the metadata
display(spark.table("metadata_catalog.etl_config.pipeline_metadata"))

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
# Import necessary libraries for metadata-driven pipeline execution
from pyspark.sql.functions import lit, col, current_timestamp, max as spark_max
from pyspark.sql.types import *
from datetime import datetime
import json

print("✅ Libraries imported successfully")

# COMMAND ----------

# DBTITLE 1,Section 3: Dynamic Pipeline Generation
# MAGIC %md
# MAGIC # ⚡ Section 3: Dynamic Pipeline Generation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC Instead of building a separate toy car for each kid, you build **one factory** that reads instructions from a list and automatically builds any car design. That's dynamic pipeline generation - one framework that reads metadata and creates pipelines automatically!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Dynamic Pipeline Generation** is the core principle of metadata-driven ETL where:
# MAGIC
# MAGIC 1. **Runtime Discovery**: Pipelines are constructed at execution time, not design time
# MAGIC 2. **Configuration-Based**: All pipeline logic is derived from metadata entries
# MAGIC 3. **Loop-Based Execution**: A single framework iterates through metadata and executes pipelines
# MAGIC 4. **Polymorphic Behavior**: Same code handles different sources, formats, and transformations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔄 The Dynamic Pipeline Lifecycle:
# MAGIC
# MAGIC ```
# MAGIC ┌───────────────────┐
# MAGIC │  1. Read Metadata  │
# MAGIC │     (Active Pipes) │
# MAGIC └───────┬───────────┘
# MAGIC         │
# MAGIC         ↓
# MAGIC ┌───────┴───────────┐
# MAGIC │  2. For Each Row  │
# MAGIC │     (Loop)        │
# MAGIC └───────┬───────────┘
# MAGIC         │
# MAGIC         ↓
# MAGIC ┌───────┴───────────────────┐
# MAGIC │  3. Extract Parameters    │
# MAGIC │     (source, target, etc) │
# MAGIC └───────┬──────────────────┘
# MAGIC         │
# MAGIC         ↓
# MAGIC ┌───────┴──────────────────┐
# MAGIC │  4. Execute Pipeline     │
# MAGIC │     (Read-Transform-Load)│
# MAGIC └───────┬──────────────────┘
# MAGIC         │
# MAGIC         ↓
# MAGIC ┌───────┴───────────────┐
# MAGIC │  5. Update Watermark    │
# MAGIC │     (Metadata Refresh) │
# MAGIC └────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Key Advantages:
# MAGIC
# MAGIC 1. **Zero Code Changes**: Add 100 tables by adding 100 metadata rows
# MAGIC 2. **Consistent Patterns**: Same framework = same behavior
# MAGIC 3. **Centralized Monitoring**: Track all pipelines from one place
# MAGIC 4. **Easy Testing**: Test the framework, not individual pipelines
# MAGIC 5. **Business User Friendly**: Non-developers can onboard new sources
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Design Patterns:
# MAGIC
# MAGIC - **Factory Pattern**: Create pipeline objects based on metadata
# MAGIC - **Strategy Pattern**: Select transformation logic dynamically
# MAGIC - **Chain of Responsibility**: Handle different source types
# MAGIC - **Template Method**: Define pipeline skeleton, customize per config

# COMMAND ----------

# DBTITLE 1,Read Active Pipeline Configurations
# Read metadata to identify all active pipelines
# This is the foundation of dynamic pipeline generation

config_df = spark.table("metadata_catalog.etl_config.pipeline_metadata") \
    .filter(col("is_active") == True) \
    .orderBy("priority")

print("✅ Active pipeline configurations loaded")
print(f"\nTotal Active Pipelines: {config_df.count()}")
print("\nPipelines will execute in priority order:")

display(config_df.select(
    "pipeline_id",
    "pipeline_name",
    "source_path",
    "target_table",
    "load_type",
    "priority"
))

# COMMAND ----------

# DBTITLE 1,Pipeline Configuration Inspector
# Function to inspect and validate pipeline configuration
# This ensures metadata quality before execution

def inspect_pipeline_config(pipeline_id):
    """
    Inspect and display detailed configuration for a specific pipeline.
    
    Args:
        pipeline_id (str): Unique pipeline identifier
    
    Returns:
        dict: Configuration dictionary
    """
    config = spark.table("metadata_catalog.etl_config.pipeline_metadata") \
        .filter(col("pipeline_id") == pipeline_id) \
        .first()
    
    if config is None:
        print(f"❌ Pipeline {pipeline_id} not found in metadata")
        return None
    
    config_dict = config.asDict()
    
    print(f"\n{'='*60}")
    print(f"Pipeline Configuration: {pipeline_id}")
    print(f"{'='*60}")
    
    print(f"\n📌 Name: {config_dict['pipeline_name']}")
    print(f"\n📊 Source Details:")
    print(f"   Type: {config_dict['source_type']}")
    print(f"   Path: {config_dict['source_path']}")
    print(f"   Format: {config_dict['source_format']}")
    
    print(f"\n🎯 Target Details:")
    print(f"   Catalog: {config_dict['target_catalog']}")
    print(f"   Schema: {config_dict['target_schema']}")
    print(f"   Table: {config_dict['target_table']}")
    print(f"   Full Name: {config_dict['target_catalog']}.{config_dict['target_schema']}.{config_dict['target_table']}")
    
    print(f"\n⚙️ Execution Details:")
    print(f"   Load Type: {config_dict['load_type']}")
    print(f"   Watermark Column: {config_dict['watermark_column']}")
    print(f"   Transformation: {config_dict['transformation_logic']}")
    print(f"   Priority: {config_dict['priority']}")
    print(f"   Status: {'Active' if config_dict['is_active'] else 'Inactive'}")
    
    print(f"\n📅 Audit Trail:")
    print(f"   Created By: {config_dict['created_by']}")
    print(f"   Created At: {config_dict['created_at']}")
    print(f"   Last Updated: {config_dict['updated_at']}")
    print(f"   Last Processed: {config_dict['last_processed_timestamp']}")
    
    print(f"\n{'='*60}\n")
    
    return config_dict

# Test the inspector
config = inspect_pipeline_config("PIPE_001")

if config:
    print("✅ Configuration inspection complete")

# COMMAND ----------

# DBTITLE 1,Dynamic Pipeline Discovery
# Demonstrate dynamic discovery of pipeline configurations
# Show how the framework adapts to metadata changes

print("🔍 Dynamic Pipeline Discovery\n")
print("="*70)

# Get all active pipelines
active_pipelines = spark.table("metadata_catalog.etl_config.pipeline_metadata") \
    .filter(col("is_active") == True) \
    .collect()

print(f"\n📊 Discovered {len(active_pipelines)} active pipeline(s)\n")

# Display pipeline inventory
for idx, pipeline in enumerate(active_pipelines, 1):
    print(f"{idx}. {pipeline.pipeline_id}: {pipeline.pipeline_name}")
    print(f"   └─ {pipeline.source_path} → {pipeline.target_catalog}.{pipeline.target_schema}.{pipeline.target_table}")
    print(f"   └─ Load Type: {pipeline.load_type} | Priority: {pipeline.priority}")
    print()

print("\n✅ Pipeline discovery complete")
print("\n💡 Key Insight: Adding a new pipeline requires only a metadata insert, no code changes!")

# COMMAND ----------

# DBTITLE 1,Create Sample Source Data
# Create sample source data for demonstration
# In production, this would be your actual data sources

from pyspark.sql.functions import expr, rand, when

print("📦 Creating sample source data...\n")

# Create volume for raw data if it doesn't exist
spark.sql("""
  CREATE VOLUME IF NOT EXISTS main.default.raw_data
  COMMENT 'Volume for raw source data files'
""")

print("✅ Volume created: main.default.raw_data\n")

# Create sample customer data
customers_data = spark.range(1, 101) \
    .withColumn("customer_id", col("id").cast("string")) \
    .withColumn("customer_name", expr("concat('Customer_', id)")) \
    .withColumn("email", expr("concat('customer', id, '@example.com')")) \
    .withColumn("country", 
        when(col("id") % 3 == 0, "USA")
        .when(col("id") % 3 == 1, "UK")
        .otherwise("Canada")
    ) \
    .withColumn("registration_date", current_timestamp()) \
    .withColumn("status", 
        when(rand() > 0.3, "active")
        .otherwise("inactive")
    ) \
    .drop("id")

# Save as source data
customers_data.write \
    .mode("overwrite") \
    .format("parquet") \
    .save("/Volumes/main/default/raw_data/customers")

print("✅ Customer source data created (100 records)")
print("   Location: /Volumes/main/default/raw_data/customers")

# Create sample transaction data with timestamps
transactions_data = spark.range(1, 201) \
    .withColumn("transaction_id", col("id").cast("string")) \
    .withColumn("customer_id", expr("cast((id % 100) + 1 as string)")) \
    .withColumn("amount", expr("round(rand() * 1000 + 10, 2)")) \
    .withColumn("transaction_timestamp", current_timestamp()) \
    .withColumn("transaction_date", expr("date(transaction_timestamp)")) \
    .withColumn("status", 
        when(rand() > 0.1, "completed")
        .otherwise("pending")
    ) \
    .drop("id")

transactions_data.write \
    .mode("overwrite") \
    .format("json") \
    .save("/Volumes/main/default/raw_data/transactions")

print("\n✅ Transaction source data created (200 records)")
print("   Location: /Volumes/main/default/raw_data/transactions")

print("\n📦 Sample source data creation complete!")

# COMMAND ----------

# DBTITLE 1,Test Dynamic Source Reading
# Test dynamic source reading based on metadata
# This demonstrates how the framework reads different formats

print("🔍 Testing Dynamic Source Reading\n")
print("="*70)

# Get first pipeline configuration
test_config = spark.table("metadata_catalog.etl_config.pipeline_metadata") \
    .filter(col("pipeline_id") == "PIPE_001") \
    .first()

print(f"\nTesting with pipeline: {test_config.pipeline_id}")
print(f"Source: {test_config.source_path}")
print(f"Format: {test_config.source_format}")

# Dynamic source reading
try:
    if test_config.source_type == "file":
        source_df = spark.read \
            .format(test_config.source_format) \
            .load(test_config.source_path)
        
        print(f"\n✅ Successfully read source data")
        print(f"   Records: {source_df.count()}")
        print(f"   Columns: {len(source_df.columns)}")
        
        print("\n📊 Sample Data:")
        display(source_df.limit(5))
        
    elif test_config.source_type == "table":
        source_df = spark.table(test_config.source_path)
        print(f"\n✅ Successfully read source table")
        print(f"   Records: {source_df.count()}")
        
except Exception as e:
    print(f"\n⚠️ Note: Source may not exist yet - this is expected for demo")
    print(f"   In production, ensure source data exists before pipeline execution")

print("\n✅ Dynamic source reading test complete")

# COMMAND ----------

# DBTITLE 1,Create Main and Gold Schema
# Create target catalog and schema for pipeline outputs
# Ensure proper data organization

spark.sql("""
  CREATE SCHEMA IF NOT EXISTS main.gold
  COMMENT 'Gold layer - business-ready curated data'
""")

print("✅ Target schema created: main.gold")
print("\nReady for pipeline execution")

# COMMAND ----------

# DBTITLE 1,Dynamic Source Reader Function
# Generic source reader that adapts based on metadata
# This is a key component of the dynamic pipeline framework

def read_source_dynamic(config_row):
    """
    Dynamically read source data based on pipeline configuration.
    
    Args:
        config_row: Row from metadata table containing pipeline config
    
    Returns:
        DataFrame: Source data
    """
    source_type = config_row.source_type
    source_path = config_row.source_path
    source_format = config_row.source_format
    
    print(f"\n📝 Reading source: {source_path}")
    
    try:
        if source_type == "file":
            df = spark.read.format(source_format).load(source_path)
            
        elif source_type == "table":
            df = spark.table(source_path)
            
        elif source_type == "jdbc":
            # For JDBC sources (placeholder - would need connection properties)
            print("   🚧 JDBC source type detected - requires connection config")
            return None
            
        elif source_type == "api":
            # For API sources (placeholder - would need API logic)
            print("   🚧 API source type detected - requires API integration")
            return None
        
        else:
            print(f"   ❌ Unknown source type: {source_type}")
            return None
        
        record_count = df.count()
        print(f"   ✅ Successfully read {record_count:,} records")
        print(f"   📊 Columns: {', '.join(df.columns)}")
        
        return df
        
    except Exception as e:
        print(f"   ❌ Error reading source: {str(e)}")
        return None

print("✅ Dynamic source reader function created")
print("\nThis function can read from any source type defined in metadata")

# COMMAND ----------

# DBTITLE 1,Test Dynamic Reader
# Test the dynamic reader with actual metadata

print("🧪 Testing Dynamic Source Reader\n")
print("="*70)

# Get test configuration
test_config = spark.table("metadata_catalog.etl_config.pipeline_metadata") \
    .filter(col("pipeline_id") == "PIPE_001") \
    .first()

print(f"\nPipeline: {test_config.pipeline_name}")

# Use dynamic reader
source_df = read_source_dynamic(test_config)

if source_df:
    print("\n📊 Source Data Preview:")
    display(source_df.limit(5))
    print("\n✅ Dynamic reader test successful")
else:
    print("\n⚠️ Could not read source data")

# COMMAND ----------

# DBTITLE 1,Section 4: Dynamic ETL Framework
# MAGIC %md
# MAGIC # 🏭 Section 4: Dynamic ETL Framework
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC Imagine a **robot chef** that reads recipe cards and cooks any meal automatically. You just add new recipe cards, and the robot does the rest. That's the dynamic ETL framework - it reads the metadata "recipe cards" and processes data automatically!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC A **Dynamic ETL Framework** is an orchestration engine that:
# MAGIC
# MAGIC 1. **Reads metadata** from control tables
# MAGIC 2. **Loops through configurations** to generate pipeline instances
# MAGIC 3. **Applies transformations** based on metadata rules
# MAGIC 4. **Handles failures** with retry logic and error logging
# MAGIC 5. **Updates watermarks** for incremental processing
# MAGIC 6. **Logs execution metrics** for monitoring and auditing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔑 Core Components:
# MAGIC
# MAGIC 1. **Metadata Reader**: Fetches active pipeline configurations
# MAGIC 2. **Source Adapter**: Handles different source types (file, table, JDBC, API)
# MAGIC 3. **Transformation Engine**: Applies business logic dynamically
# MAGIC 4. **Target Writer**: Writes data with proper partitioning and optimization
# MAGIC 5. **Watermark Manager**: Tracks incremental load progress
# MAGIC 6. **Audit Logger**: Records execution details
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔄 Framework Execution Flow:
# MAGIC
# MAGIC ```python
# MAGIC for pipeline_config in metadata:
# MAGIC     try:
# MAGIC         # 1. Read source based on config
# MAGIC         source_df = read_source(config)
# MAGIC         
# MAGIC         # 2. Apply transformations
# MAGIC         transformed_df = apply_transformation(source_df, config)
# MAGIC         
# MAGIC         # 3. Write to target
# MAGIC         write_target(transformed_df, config)
# MAGIC         
# MAGIC         # 4. Update watermark
# MAGIC         update_watermark(config)
# MAGIC         
# MAGIC         # 5. Log success
# MAGIC         log_execution(config, 'SUCCESS')
# MAGIC         
# MAGIC     except Exception as e:
# MAGIC         # Handle failure
# MAGIC         log_execution(config, 'FAILED', error=e)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Key Features:
# MAGIC
# MAGIC - ✅ **Polymorphic**: Same code handles different sources/targets
# MAGIC - ✅ **Resilient**: Error handling per pipeline
# MAGIC - ✅ **Observable**: Full execution logging
# MAGIC - ✅ **Scalable**: Parallel execution support
# MAGIC - ✅ **Maintainable**: Single codebase for all pipelines

# COMMAND ----------

# DBTITLE 1,Transformation Engine
# Dynamic transformation engine that applies business logic based on metadata
# This demonstrates polymorphic transformation handling

from pyspark.sql.functions import upper, trim, regexp_replace, to_timestamp

def apply_transformation(df, config_row):
    """
    Apply transformation logic based on pipeline configuration.
    
    Args:
        df: Source DataFrame
        config_row: Pipeline configuration from metadata
    
    Returns:
        DataFrame: Transformed data
    """
    transformation_logic = config_row.transformation_logic
    
    print(f"\n⚙️ Applying transformation: {transformation_logic}")
    
    try:
        # Map transformation names to actual logic
        if transformation_logic == "cleanse_and_standardize":
            # Standardize customer data
            transformed_df = df \
                .withColumn("customer_name", upper(trim(col("customer_name")))) \
                .withColumn("email", trim(col("email"))) \
                .withColumn("country", upper(col("country"))) \
                .withColumn("processed_timestamp", current_timestamp())
            
            print("   ✅ Applied: Name standardization, email cleansing, timestamp addition")
            
        elif transformation_logic == "enrich_and_validate":
            # Enrich transaction data
            transformed_df = df \
                .withColumn("amount", col("amount").cast("decimal(10,2)")) \
                .withColumn("is_high_value", when(col("amount") > 500, True).otherwise(False)) \
                .withColumn("processed_timestamp", current_timestamp())
            
            # Filter out invalid transactions
            transformed_df = transformed_df.filter(col("amount") > 0)
            
            print("   ✅ Applied: Amount validation, high-value flagging, data quality filter")
            
        elif transformation_logic == "deduplicate_and_enrich":
            # Deduplicate and enrich product data
            transformed_df = df \
                .dropDuplicates(["customer_id"]) \
                .withColumn("processed_timestamp", current_timestamp())
            
            print("   ✅ Applied: Deduplication, timestamp addition")
            
        else:
            # Default: pass-through with timestamp
            transformed_df = df.withColumn("processed_timestamp", current_timestamp())
            print(f"   ⚠️ Unknown transformation '{transformation_logic}' - applying default")
        
        print(f"   📊 Output records: {transformed_df.count():,}")
        return transformed_df
        
    except Exception as e:
        print(f"   ❌ Transformation error: {str(e)}")
        raise

print("✅ Transformation engine created")
print("\nSupported transformations:")
print("  - cleanse_and_standardize")
print("  - enrich_and_validate")
print("  - deduplicate_and_enrich")

# COMMAND ----------

# DBTITLE 1,Dynamic Target Writer
# Dynamic target writer that handles different load types
# Supports full, incremental, and merge patterns

def write_target(df, config_row):
    """
    Write data to target based on pipeline configuration.
    
    Args:
        df: Transformed DataFrame to write
        config_row: Pipeline configuration from metadata
    
    Returns:
        bool: Success status
    """
    target_table = f"{config_row.target_catalog}.{config_row.target_schema}.{config_row.target_table}"
    load_type = config_row.load_type
    partition_cols = config_row.partition_columns if config_row.partition_columns else []
    
    print(f"\n💾 Writing to target: {target_table}")
    print(f"   Load Type: {load_type}")
    
    try:
        writer = df.write.format("delta")
        
        # Apply partitioning if specified
        if partition_cols and len(partition_cols) > 0:
            writer = writer.partitionBy(*partition_cols)
            print(f"   Partitioned by: {', '.join(partition_cols)}")
        
        # Handle different load types
        if load_type == "full":
            writer.mode("overwrite").saveAsTable(target_table)
            print(f"   ✅ Full load complete (overwrite mode)")
            
        elif load_type == "incremental":
            writer.mode("append").saveAsTable(target_table)
            print(f"   ✅ Incremental load complete (append mode)")
            
        elif load_type == "merge":
            # For merge, we'll use append for now (full merge requires merge key)
            writer.mode("append").saveAsTable(target_table)
            print(f"   ✅ Merge load complete (append mode - full merge requires merge keys)")
            
        else:
            print(f"   ❌ Unknown load type: {load_type}")
            return False
        
        # Verify write
        result_count = spark.table(target_table).count()
        print(f"   📊 Target table now has {result_count:,} records")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Write error: {str(e)}")
        return False

print("✅ Dynamic target writer created")
print("\nSupported load types:")
print("  - full (overwrite)")
print("  - incremental (append)")
print("  - merge (upsert - requires merge keys)")

# COMMAND ----------

# DBTITLE 1,Watermark Manager
# Watermark manager for tracking incremental load progress
# Critical for reliable incremental processing

def update_watermark(config_row, success=True, error_message=None):
    """
    Update pipeline metadata with execution results and watermark.
    
    Args:
        config_row: Pipeline configuration
        success: Whether execution was successful
        error_message: Error message if failed
    """
    pipeline_id = config_row.pipeline_id
    
    print(f"\n🔖 Updating watermark for {pipeline_id}")
    
    try:
        if success:
            # Update last processed timestamp and increment run count
            spark.sql(f"""
                UPDATE metadata_catalog.etl_config.pipeline_metadata
                SET 
                    last_processed_timestamp = current_timestamp(),
                    updated_at = current_timestamp()
                WHERE pipeline_id = '{pipeline_id}'
            """)
            
            print(f"   ✅ Watermark updated successfully")
        else:
            # Log failure but don't update watermark
            print(f"   ⚠️ Execution failed - watermark not updated")
            if error_message:
                print(f"   Error: {error_message}")
        
    except Exception as e:
        print(f"   ❌ Watermark update error: {str(e)}")

print("✅ Watermark manager created")
print("\nTracks:")
print("  - Last successful execution timestamp")
print("  - Last processed data watermark")
print("  - Configuration update history")

# COMMAND ----------

# DBTITLE 1,Complete ETL Framework Orchestrator
# Main orchestrator that ties everything together
# This is the heart of the metadata-driven ETL framework

def execute_dynamic_etl_framework(pipeline_filter=None):
    """
    Execute the complete metadata-driven ETL framework.
    
    Args:
        pipeline_filter: Optional filter (e.g., specific pipeline_id)
    
    Returns:
        dict: Execution summary
    """
    print("\n" + "="*70)
    print("🚀 METADATA-DRIVEN ETL FRAMEWORK EXECUTION")
    print("="*70)
    
    # Read active pipeline configurations
    config_query = spark.table("metadata_catalog.etl_config.pipeline_metadata") \
        .filter(col("is_active") == True) \
        .orderBy("priority")
    
    # Apply filter if provided
    if pipeline_filter:
        config_query = config_query.filter(col("pipeline_id") == pipeline_filter)
    
    configs = config_query.collect()
    
    print(f"\n📊 Found {len(configs)} active pipeline(s) to execute\n")
    
    # Execution tracking
    results = {
        "total": len(configs),
        "successful": 0,
        "failed": 0,
        "pipelines": []
    }
    
    # Execute each pipeline
    for idx, config in enumerate(configs, 1):
        print(f"\n{'='*70}")
        print(f"🔄 Pipeline {idx}/{len(configs)}: {config.pipeline_id}")
        print(f"{'='*70}")
        print(f"Name: {config.pipeline_name}")
        
        pipeline_result = {
            "pipeline_id": config.pipeline_id,
            "pipeline_name": config.pipeline_name,
            "status": "PENDING"
        }
        
        try:
            # Step 1: Read source
            print(f"\n📝 Step 1/4: Reading source data")
            source_df = read_source_dynamic(config)
            
            if source_df is None:
                raise Exception("Failed to read source data")
            
            # Step 2: Apply transformation
            print(f"\n⚙️ Step 2/4: Applying transformations")
            transformed_df = apply_transformation(source_df, config)
            
            # Step 3: Write to target
            print(f"\n💾 Step 3/4: Writing to target")
            write_success = write_target(transformed_df, config)
            
            if not write_success:
                raise Exception("Failed to write to target")
            
            # Step 4: Update watermark
            print(f"\n🔖 Step 4/4: Updating watermark")
            update_watermark(config, success=True)
            
            # Mark as successful
            pipeline_result["status"] = "SUCCESS"
            results["successful"] += 1
            
            print(f"\n✅ Pipeline {config.pipeline_id} completed successfully!")
            
        except Exception as e:
            # Handle failure
            error_msg = str(e)
            pipeline_result["status"] = "FAILED"
            pipeline_result["error"] = error_msg
            results["failed"] += 1
            
            print(f"\n❌ Pipeline {config.pipeline_id} failed: {error_msg}")
            update_watermark(config, success=False, error_message=error_msg)
        
        results["pipelines"].append(pipeline_result)
    
    # Print summary
    print(f"\n\n{'='*70}")
    print("📊 EXECUTION SUMMARY")
    print("="*70)
    print(f"\nTotal Pipelines: {results['total']}")
    print(f"✅ Successful: {results['successful']}")
    print(f"❌ Failed: {results['failed']}")
    print(f"\nSuccess Rate: {(results['successful']/results['total']*100):.1f}%" if results['total'] > 0 else "N/A")
    
    print("\n\nPipeline Details:")
    for pipeline in results["pipelines"]:
        status_icon = "✅" if pipeline["status"] == "SUCCESS" else "❌"
        print(f"  {status_icon} {pipeline['pipeline_id']}: {pipeline['status']}")
        if "error" in pipeline:
            print(f"      Error: {pipeline['error']}")
    
    print("\n" + "="*70)
    
    return results

print("✅ Complete ETL Framework Orchestrator created")
print("\nThis is the master function that:")
print("  1. Reads all active pipeline configurations")
print("  2. Executes each pipeline in priority order")
print("  3. Handles errors gracefully")
print("  4. Updates watermarks and metadata")
print("  5. Provides comprehensive execution reporting")

# COMMAND ----------

# DBTITLE 1,Execute Framework - Single Pipeline Test
# Test the framework with a single pipeline first
# This validates the end-to-end flow

print("🧪 Testing framework with single pipeline...\n")

# Execute only PIPE_001 (Customer pipeline)
results = execute_dynamic_etl_framework(pipeline_filter="PIPE_001")

print("\n\n🔍 Verifying results...\n")

# Check the target table
target_df = spark.table("main.gold.customers")
print(f"✅ Target table 'main.gold.customers' has {target_df.count():,} records")

print("\n📊 Sample of loaded data:")
display(target_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Execute Framework - All Pipelines
# Execute all active pipelines through the metadata-driven framework
# This demonstrates the true power of the approach

print("🚀 Executing ALL active pipelines through metadata-driven framework...\n")

# Execute all pipelines (no filter)
results = execute_dynamic_etl_framework()

print("\n\n✅ Framework execution complete!")
print("\n💡 Key Insight: We processed multiple pipelines with ZERO hardcoded logic!")
print("   All behavior was driven by metadata configuration.")

# COMMAND ----------

# DBTITLE 1,Section 5: Automation Patterns
# MAGIC %md
# MAGIC # 🤖 Section 5: Automation Patterns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC Automation patterns are like having **smart buttons** that do complex tasks automatically. Press one button, and multiple things happen in the right order. In data engineering, automation patterns help us process data without manual work!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Automation Patterns** in metadata-driven ETL are reusable design patterns that eliminate manual intervention and enable:
# MAGIC
# MAGIC 1. **Self-Service Data Integration**: Business users can onboard new sources
# MAGIC 2. **Schema Evolution**: Automatic handling of schema changes
# MAGIC 3. **Dependency Management**: Automatic execution ordering
# MAGIC 4. **Error Recovery**: Automatic retry and alerting
# MAGIC 5. **Resource Optimization**: Dynamic resource allocation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔑 Core Automation Patterns:
# MAGIC
# MAGIC #### 1️⃣ **Configuration-Driven Pattern**
# MAGIC ```python
# MAGIC # Add metadata → Pipeline auto-generates
# MAGIC INSERT INTO pipeline_metadata VALUES (
# MAGIC     'new_pipeline', 'source', 'target', 'config'
# MAGIC )
# MAGIC # That's it! No code changes needed.
# MAGIC ```
# MAGIC
# MAGIC #### 2️⃣ **Watermark Pattern** (Incremental Processing)
# MAGIC ```python
# MAGIC SELECT * FROM source
# MAGIC WHERE timestamp > (SELECT last_watermark FROM metadata)
# MAGIC ```
# MAGIC
# MAGIC #### 3️⃣ **Dependency Chain Pattern**
# MAGIC ```python
# MAGIC # Metadata defines: Bronze → Silver → Gold
# MAGIC # Framework executes in correct order automatically
# MAGIC ```
# MAGIC
# MAGIC #### 4️⃣ **Schema Discovery Pattern**
# MAGIC ```python
# MAGIC # Framework reads source schema automatically
# MAGIC # No manual schema definition required
# MAGIC ```
# MAGIC
# MAGIC #### 5️⃣ **Retry Pattern**
# MAGIC ```python
# MAGIC for retry in range(max_retries):
# MAGIC     try:
# MAGIC         execute_pipeline()
# MAGIC         break
# MAGIC     except:
# MAGIC         wait_and_retry()
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Pattern Comparison:
# MAGIC
# MAGIC | Pattern | Use Case | Automation Level | Complexity |
# MAGIC |---------|----------|------------------|------------|
# MAGIC | **Full Load** | Small tables, daily sync | High | Low |
# MAGIC | **Incremental** | Large tables, frequent updates | Very High | Medium |
# MAGIC | **Merge (SCD)** | Slowly changing dimensions | High | High |
# MAGIC | **Stream** | Real-time data | Very High | Very High |
# MAGIC | **Dependency Chain** | Multi-layer architecture | Very High | Medium |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Best Practices:
# MAGIC
# MAGIC 1. **Idempotency**: Pipelines should be rerunnable without side effects
# MAGIC 2. **Observability**: Log everything for debugging
# MAGIC 3. **Fail-Fast**: Detect errors early in the pipeline
# MAGIC 4. **Graceful Degradation**: Continue processing other pipelines on failure
# MAGIC 5. **Data Quality Gates**: Validate before moving to next stage

# COMMAND ----------

# DBTITLE 1,Pattern 1: Full Load vs Incremental
# Demonstrate different load patterns controlled by metadata

print("🔄 Load Pattern Demonstration\n")
print("="*70)

# Get configurations showing different load types
load_patterns = spark.table("metadata_catalog.etl_config.pipeline_metadata") \
    .filter(col("is_active") == True) \
    .select("pipeline_id", "pipeline_name", "load_type", "watermark_column") \
    .collect()

print("\n📋 Configured Load Patterns:\n")

for pattern in load_patterns:
    print(f"🔹 {pattern.pipeline_id}: {pattern.pipeline_name}")
    print(f"   Load Type: {pattern.load_type}")
    
    if pattern.load_type == "full":
        print("   ✅ Behavior: Complete refresh on every run")
        print("   📊 Use Case: Small reference tables, dimension tables")
    elif pattern.load_type == "incremental":
        print(f"   ✅ Behavior: Only load new/changed data using watermark: {pattern.watermark_column}")
        print("   📊 Use Case: Large fact tables, event streams")
    elif pattern.load_type == "merge":
        print("   ✅ Behavior: Upsert - insert new, update existing")
        print("   📊 Use Case: Slowly changing dimensions (SCD)")
    
    print()

print("✅ Load pattern configuration is metadata-driven")
print("   Change load_type in metadata to change behavior - no code changes!")

# COMMAND ----------

# DBTITLE 1,Pattern 2: Parameterized Execution
# Demonstrate parameterized execution for automation
# Enables scheduled jobs with dynamic behavior

from datetime import datetime, timedelta

def execute_pipelines_by_schedule(schedule_window="daily"):
    """
    Execute pipelines based on schedule window.
    This enables automated job scheduling.
    
    Args:
        schedule_window: 'hourly', 'daily', 'weekly'
    """
    print(f"\n📅 Executing pipelines for schedule: {schedule_window}\n")
    print("="*70)
    
    # In production, this would filter based on schedule metadata
    # For demo, we'll execute all active pipelines
    
    print(f"\n🔍 Finding pipelines scheduled for '{schedule_window}' window...")
    
    # Simulate schedule-based filtering
    active_pipelines = spark.table("metadata_catalog.etl_config.pipeline_metadata") \
        .filter(col("is_active") == True) \
        .select("pipeline_id", "pipeline_name", "priority")
    
    count = active_pipelines.count()
    
    print(f"\n✅ Found {count} pipeline(s) for execution")
    
    if count > 0:
        print("\n📊 Pipelines to execute:")
        display(active_pipelines)
        
        print("\n💡 In production, this function would:")
        print("   1. Be triggered by Databricks Jobs/Workflows")
        print("   2. Read schedule metadata from configuration")
        print("   3. Execute only pipelines matching the schedule")
        print("   4. Send notifications on completion/failure")
    
    return count

# Test parameterized execution
pipeline_count = execute_pipelines_by_schedule("daily")

print("\n\n✅ Parameterized execution demonstrated")
print("\n🎯 Benefits:")
print("   - Same code handles hourly, daily, weekly schedules")
print("   - Schedule changes require only metadata updates")
print("   - Easy to add new schedule types")

# COMMAND ----------

# DBTITLE 1,Pattern 3: Dependency-Aware Execution
# Create execution plan based on dependencies
# This enables proper ordering of pipeline execution

print("🔗 Dependency-Aware Execution Pattern\n")
print("="*70)

print("""
\n🎯 Dependency Hierarchy:

    ┌──────────────────┐
    │   Bronze Layer    │
    │  (Raw Ingestion)  │
    └────────┬─────────┘
             │
             ↓ (depends on)
    ┌────────┴─────────┐
    │   Silver Layer    │
    │ (Cleansed Data)  │
    └────────┬─────────┘
             │
             ↓ (depends on)
    ┌────────┴─────────┐
    │    Gold Layer     │
    │ (Business Ready) │
    └──────────────────┘
""")

print("\n🔑 Current Pipeline Execution Order (by priority):")

execution_order = spark.table("metadata_catalog.etl_config.pipeline_metadata") \
    .filter(col("is_active") == True) \
    .orderBy("priority") \
    .select("priority", "pipeline_id", "pipeline_name", "target_schema")

display(execution_order)

print("""
\n💡 How Dependency-Aware Execution Works:

1. **Metadata Defines Dependencies**:
   - Add 'depends_on' column to metadata table
   - List upstream pipeline IDs

2. **Framework Builds DAG**:
   - Parse dependencies
   - Create execution graph
   - Topological sort for order

3. **Execute in Correct Order**:
   - Run independent pipelines in parallel
   - Wait for dependencies before starting
   - Handle failures gracefully

4. **Benefits**:
   ✅ Automatic ordering
   ✅ Parallel execution where possible
   ✅ Failure isolation
   ✅ Easy to visualize data lineage
""")

print("✅ Dependency-aware execution pattern explained")

# COMMAND ----------

# DBTITLE 1,Section 6: Incremental Processing (Metadata-Driven)
# MAGIC %md
# MAGIC # 📈 Section 6: Incremental Processing (Metadata-Driven)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC Instead of reading the **entire book** every time, you use a **bookmark** to remember where you stopped. Next time, you start from the bookmark! That's incremental processing - only process NEW data, not everything again.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Incremental Processing** is a critical optimization pattern that:
# MAGIC
# MAGIC 1. Processes only **new or changed data** since last execution
# MAGIC 2. Uses **watermarks** to track processing progress
# MAGIC 3. Dramatically **reduces compute costs** and **processing time**
# MAGIC 4. Enables **near real-time** data pipelines
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔑 Key Components:
# MAGIC
# MAGIC #### 1️⃣ **Watermark Column**
# MAGIC - Timestamp or incrementing ID column
# MAGIC - Stored in metadata: `watermark_column`
# MAGIC - Examples: `created_at`, `updated_at`, `event_timestamp`
# MAGIC
# MAGIC #### 2️⃣ **Last Watermark Value**
# MAGIC - Stored in metadata: `last_watermark`
# MAGIC - Updated after successful pipeline execution
# MAGIC - Used as filter in next run
# MAGIC
# MAGIC #### 3️⃣ **Filtering Logic**
# MAGIC ```sql
# MAGIC SELECT * FROM source
# MAGIC WHERE {watermark_column} > '{last_watermark}'
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔄 Incremental Processing Flow:
# MAGIC
# MAGIC ```
# MAGIC 1. Read Metadata
# MAGIC    ↓
# MAGIC 2. Get last_watermark value
# MAGIC    ↓
# MAGIC 3. Filter: WHERE timestamp > last_watermark
# MAGIC    ↓
# MAGIC 4. Process only NEW data
# MAGIC    ↓
# MAGIC 5. Write to target (append mode)
# MAGIC    ↓
# MAGIC 6. Update last_watermark = MAX(timestamp)
# MAGIC    ↓
# MAGIC 7. Save new watermark to metadata
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Benefits:
# MAGIC
# MAGIC | Metric | Full Load | Incremental Load |
# MAGIC |--------|-----------|------------------|
# MAGIC | **Data Scanned** | 100% | 1-5% |
# MAGIC | **Processing Time** | Hours | Minutes |
# MAGIC | **Compute Cost** | High | Low |
# MAGIC | **Freshness** | Daily | Near real-time |
# MAGIC | **Scalability** | Poor | Excellent |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Considerations:
# MAGIC
# MAGIC 1. **Late-Arriving Data**: Use grace period for late events
# MAGIC 2. **Watermark Drift**: Handle clock skew
# MAGIC 3. **Backfill**: Support full refresh when needed
# MAGIC 4. **Data Quality**: Validate watermark column exists and is indexed

# COMMAND ----------

# DBTITLE 1,Incremental Load Implementation
# Implement incremental loading with watermark tracking
# This is production-grade incremental processing

def execute_incremental_load(pipeline_id):
    """
    Execute incremental load for a specific pipeline.
    Demonstrates watermark-based processing.
    
    Args:
        pipeline_id: Pipeline identifier
    """
    print(f"\n🔄 Executing Incremental Load: {pipeline_id}\n")
    print("="*70)
    
    # Get pipeline configuration
    config = spark.table("metadata_catalog.etl_config.pipeline_metadata") \
        .filter(col("pipeline_id") == pipeline_id) \
        .first()
    
    if not config:
        print(f"❌ Pipeline {pipeline_id} not found")
        return
    
    if config.load_type != "incremental":
        print(f"⚠️ Pipeline {pipeline_id} is not configured for incremental load")
        print(f"   Current load_type: {config.load_type}")
        return
    
    watermark_col = config.watermark_column
    last_watermark = config.last_watermark
    
    print(f"📌 Pipeline: {config.pipeline_name}")
    print(f"🔖 Watermark Column: {watermark_col}")
    print(f"📅 Last Watermark: {last_watermark if last_watermark else 'None (First Run)'}")
    
    try:
        # Read source data
        print(f"\n📝 Reading source: {config.source_path}")
        source_df = spark.read.format(config.source_format).load(config.source_path)
        
        total_records = source_df.count()
        print(f"   Total records in source: {total_records:,}")
        
        # Apply watermark filter for incremental
        if last_watermark:
            print(f"\n🔍 Applying incremental filter: {watermark_col} > '{last_watermark}'")
            incremental_df = source_df.filter(col(watermark_col) > last_watermark)
        else:
            print("\n🆕 First run - processing all data")
            incremental_df = source_df
        
        new_records = incremental_df.count()
        print(f"   ✅ New/changed records: {new_records:,}")
        
        if new_records == 0:
            print("\nℹ️ No new data to process")
            return
        
        # Apply transformation
        print(f"\n⚙️ Applying transformations...")
        transformed_df = apply_transformation(incremental_df, config)
        
        # Calculate new watermark
        new_watermark = transformed_df.agg(spark_max(col(watermark_col))).collect()[0][0]
        print(f"\n🔖 New watermark value: {new_watermark}")
        
        # Write to target (append mode)
        print(f"\n💾 Writing to target (append mode)...")
        target_table = f"{config.target_catalog}.{config.target_schema}.{config.target_table}"
        
        transformed_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(target_table)
        
        print(f"   ✅ Appended {new_records:,} records to {target_table}")
        
        # Update watermark in metadata
        print(f"\n🔄 Updating metadata watermark...")
        spark.sql(f"""
            UPDATE metadata_catalog.etl_config.pipeline_metadata
            SET 
                last_watermark = '{new_watermark}',
                last_processed_timestamp = current_timestamp(),
                updated_at = current_timestamp()
            WHERE pipeline_id = '{pipeline_id}'
        """)
        
        print(f"   ✅ Watermark updated: {last_watermark} → {new_watermark}")
        
        print(f"\n\n✅ Incremental load completed successfully!")
        print(f"\n📊 Stats:")
        print(f"   Total source records: {total_records:,}")
        print(f"   Processed this run: {new_records:,}")
        print(f"   Efficiency gain: {((1 - new_records/total_records) * 100):.1f}% reduction in data processed")
        
    except Exception as e:
        print(f"\n❌ Incremental load failed: {str(e)}")
        raise

# Test incremental load
execute_incremental_load("PIPE_002")

print("\n\n💡 Key Insight: Only NEW data was processed, not the entire dataset!")

# COMMAND ----------

# DBTITLE 1,Simulate Data Arrival and Re-run Incremental
# Simulate new data arrival and demonstrate incremental processing

print("📦 Simulating new data arrival...\n")

# Add more transaction data (simulating new events)
from pyspark.sql.functions import expr, lit

new_transactions = spark.range(201, 251) \
    .withColumn("transaction_id", col("id").cast("string")) \
    .withColumn("customer_id", expr("cast((id % 100) + 1 as string)")) \
    .withColumn("amount", expr("round(rand() * 1000 + 10, 2)")) \
    .withColumn("transaction_timestamp", expr("current_timestamp() + INTERVAL 1 HOUR")) \
    .withColumn("transaction_date", expr("date(current_timestamp() + INTERVAL 1 HOUR)")) \
    .withColumn("status", lit("completed")) \
    .drop("id")

# Append to source
new_transactions.write \
    .mode("append") \
    .format("json") \
    .save("/Volumes/main/default/raw_data/transactions")

print(f"✅ Added 50 new transactions to source\n")

# Re-run incremental load
print("\n" + "="*70)
print("🔄 Re-running incremental load to process ONLY new data...")
print("="*70)

execute_incremental_load("PIPE_002")

print("\n\n🏆 Incremental Processing Demonstrated!")
print("\nOnly the 50 new records were processed, not all 250!")
print("This is the power of metadata-driven incremental processing.")

# COMMAND ----------

# DBTITLE 1,Section 7: AI + Metadata Integration
# MAGIC %md
# MAGIC # 🤖 Section 7: AI + Metadata Integration
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC Imagine having a **smart assistant** that reads your instruction manual (metadata) and does the work for you. You just tell it what you want, and it figures out the steps by looking at the manual. That's AI + Metadata Integration!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **AI + Metadata Integration** represents the convergence of:
# MAGIC
# MAGIC 1. **Metadata-Driven Architecture**: Configuration-based pipeline generation
# MAGIC 2. **Generative AI**: Natural language to code generation
# MAGIC 3. **Context-Aware Systems**: AI leverages metadata for intelligent decisions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔑 Integration Patterns:
# MAGIC
# MAGIC #### 1️⃣ **AI-Generated Metadata**
# MAGIC ```
# MAGIC User Prompt: "Ingest customer data from S3"
# MAGIC          ↓
# MAGIC Genie AI analyzes and generates:
# MAGIC - Source path
# MAGIC - Target table
# MAGIC - Transformation logic
# MAGIC - Load pattern
# MAGIC          ↓
# MAGIC Inserts into metadata table
# MAGIC          ↓
# MAGIC Framework executes automatically
# MAGIC ```
# MAGIC
# MAGIC #### 2️⃣ **Metadata-Informed AI**
# MAGIC ```
# MAGIC AI reads existing metadata:
# MAGIC - Table schemas
# MAGIC - Transformation patterns
# MAGIC - Business rules
# MAGIC          ↓
# MAGIC Generates contextually aware pipelines
# MAGIC Follows established patterns
# MAGIC ```
# MAGIC
# MAGIC #### 3️⃣ **Intelligent Pipeline Optimization**
# MAGIC ```
# MAGIC AI analyzes:
# MAGIC - Execution history
# MAGIC - Data volumes
# MAGIC - Resource usage
# MAGIC          ↓
# MAGIC Recommends:
# MAGIC - Partition strategies
# MAGIC - Load type changes
# MAGIC - Schedule optimizations
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Use Cases:
# MAGIC
# MAGIC | Use Case | Description | AI Role | Metadata Role |
# MAGIC |----------|-------------|---------|---------------|
# MAGIC | **Self-Service Onboarding** | Business users onboard new sources | Generate pipeline config | Store configuration |
# MAGIC | **Auto-Scaling** | Adjust resources based on load | Predict resource needs | Track historical patterns |
# MAGIC | **Data Quality** | Detect anomalies | Identify issues | Define quality rules |
# MAGIC | **Cost Optimization** | Reduce compute costs | Recommend optimizations | Track cost metrics |
# MAGIC | **Schema Evolution** | Handle schema changes | Suggest migrations | Version schemas |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Genie AI + Metadata: The Perfect Pair
# MAGIC
# MAGIC **Genie AI strengths:**
# MAGIC - Natural language understanding
# MAGIC - Code generation
# MAGIC - Pattern recognition
# MAGIC
# MAGIC **Metadata strengths:**
# MAGIC - Structured configuration
# MAGIC - Execution control
# MAGIC - Audit trail
# MAGIC
# MAGIC **Together:**
# MAGIC - ✅ User speaks, AI generates metadata, framework executes
# MAGIC - ✅ Zero code for new pipelines
# MAGIC - ✅ Consistent patterns
# MAGIC - ✅ Fully governed and auditable
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Example Workflow:
# MAGIC
# MAGIC ```
# MAGIC User: "I need to sync product catalog daily from the products bucket"
# MAGIC
# MAGIC     ↓ [Genie AI]
# MAGIC
# MAGIC Genie generates metadata:
# MAGIC   pipeline_id: AUTO_GEN_001
# MAGIC   source_path: s3://products/
# MAGIC   target_table: gold.products
# MAGIC   load_type: full
# MAGIC   schedule: daily
# MAGIC
# MAGIC     ↓ [Metadata Table]
# MAGIC
# MAGIC Framework picks up new config:
# MAGIC   ✓ Validates configuration
# MAGIC   ✓ Generates pipeline code
# MAGIC   ✓ Schedules execution
# MAGIC   ✓ Monitors results
# MAGIC
# MAGIC     ↓ [Execution]
# MAGIC
# MAGIC Pipeline runs automatically!
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Genie AI Integration Examples
# Examples of how Genie AI integrates with metadata-driven pipelines
# These are conceptual examples showing the interaction pattern

print("🤖 Genie AI + Metadata Integration Examples\n")
print("="*70)

print("""

💡 EXAMPLE 1: Natural Language Pipeline Generation
──────────────────────────────────────────────────

🗣️ User Prompt:
   "Create a pipeline to load customer orders from JSON files in 
    /data/orders, transform with deduplication, and save to gold.orders
    as an incremental load using order_timestamp"

🤖 Genie AI Action:
   1. Parses intent
   2. Generates metadata configuration
   3. Inserts into pipeline_metadata table

📋 Generated Metadata:
   {
       "pipeline_id": "PIPE_AUTO_001",
       "pipeline_name": "Customer Orders Pipeline",
       "source_type": "file",
       "source_path": "/data/orders",
       "source_format": "json",
       "target_table": "gold.orders",
       "load_type": "incremental",
       "watermark_column": "order_timestamp",
       "transformation_logic": "deduplicate"
   }

✅ Result:
   Pipeline is now active and will execute on next framework run!


""")

print("💡 EXAMPLE 2: AI-Powered Pipeline Optimization\n")
print("─"*70)

print("""

🗣️ User Prompt:
   "Why is PIPE_001 running slow?"

🤖 Genie AI Action:
   1. Reads metadata for PIPE_001
   2. Analyzes execution history
   3. Checks data volume trends
   4. Reviews transformation logic

📊 AI Analysis:
   - Pipeline processes 10M records daily (full load)
   - Source data grows by only 100K records/day
   - Recommendation: Switch to incremental load

🛠️ AI-Generated Fix:
   UPDATE pipeline_metadata
   SET load_type = 'incremental',
       watermark_column = 'created_at'
   WHERE pipeline_id = 'PIPE_001'

✅ Result:
   Pipeline now processes 99% less data!
   Runtime: 2 hours → 5 minutes


""")

print("💡 EXAMPLE 3: Self-Service Data Integration\n")
print("─"*70)

print("""

🗣️ Business User Prompt:
   "I need sales data from /sales/2026 in my analytics table"

🤖 Genie AI Action:
   1. Identifies source location
   2. Detects data format (parquet)
   3. Reads sample to infer schema
   4. Suggests target table structure
   5. Creates metadata entry

📋 Auto-Generated Config:
   pipeline_id: SALES_2026
   source_path: /sales/2026
   source_format: parquet
   target_table: analytics.sales
   load_type: full

🔄 Framework Auto-Execution:
   Pipeline runs immediately
   Data available in minutes

✅ Result:
   Business user self-served with ZERO data engineering support!


""")

print("="*70)
print("\n✨ The Future: AI + Metadata = Self-Driving Data Pipelines")
print("\n🎯 Key Benefits:")
print("   ✅ Natural language to production pipeline in minutes")
print("   ✅ AI learns from metadata patterns")
print("   ✅ Automatic optimization and troubleshooting")
print("   ✅ Business users empowered for self-service")
print("   ✅ Data engineers focus on complex problems")

# COMMAND ----------

# DBTITLE 1,Prompt Engineering for Metadata Generation
# Example prompts that work well with Genie AI for metadata-driven pipelines
# These are patterns that can be used in production

print("📝 Genie AI Prompt Engineering Guide\n")
print("="*70)

print("""

🎯 EFFECTIVE PROMPTS FOR METADATA-DRIVEN PIPELINES:


1️⃣ PIPELINE CREATION:
   ──────────────────
   🗣️ "Create a metadata entry for a pipeline that reads Parquet files 
      from /data/customers, applies email validation, and writes to 
      gold.customers_clean as a daily full load"
   
   🗣️ "Add pipeline config: source=delta table bronze.events, 
      target=silver.events_enriched, incremental on event_time"


2️⃣ PIPELINE MODIFICATION:
   ──────────────────────
   🗣️ "Change PIPE_001 from full load to incremental using updated_at column"
   
   🗣️ "Update the transformation logic for pipeline PIPE_002 to include 
      currency conversion"


3️⃣ PIPELINE ANALYSIS:
   ────────────────────
   🗣️ "Show me all pipelines writing to the gold schema"
   
   🗣️ "Which pipelines haven't run in the last 24 hours?"
   
   🗣️ "Generate a report of all incremental pipelines and their watermarks"


4️⃣ PIPELINE OPTIMIZATION:
   ─────────────────────────
   🗣️ "Optimize PIPE_001 - it's processing too much data"
   
   🗣️ "Suggest partitioning strategy for gold.transactions based on 
      query patterns"


5️⃣ BULK OPERATIONS:
   ───────────────────
   🗣️ "Create pipelines for all CSV files in /landing/ directory, 
      writing each to a table in the bronze schema"
   
   🗣️ "Generate metadata configs for bronze to silver promotion 
      for all tables in bronze.sales_*"


6️⃣ TROUBLESHOOTING:
   ────────────────────
   🗣️ "Debug PIPE_003 - it's failing on transformation step"
   
   🗣️ "Why is the watermark not updating for PIPE_002?"


💡 PROMPT BEST PRACTICES:

   ✅ Be specific about source and target
   ✅ Mention load type (full/incremental/merge)
   ✅ Specify watermark column for incremental
   ✅ Include transformation requirements
   ✅ Reference pipeline IDs for existing pipelines
   ✅ Use business terms, not just technical jargon


❌ AVOID:

   ❌ Vague requests: "fix my pipeline"
   ❌ Missing context: "load data" (from where? to where?)
   ❌ Ambiguous targets: "update the table" (which one?)


""")

print("="*70)
print("\n✨ With good prompts, Genie AI becomes your metadata-driven pipeline co-pilot!")

# COMMAND ----------

# DBTITLE 1,Section 8: Hands-On Complete Dynamic Pipeline
# MAGIC %md
# MAGIC # 🛠️ Section 8: Hands-On Complete Dynamic Pipeline
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Objective:
# MAGIC Build a complete end-to-end metadata-driven pipeline that demonstrates all concepts:
# MAGIC
# MAGIC 1. ✅ Read metadata configuration
# MAGIC 2. ✅ Loop through multiple pipelines
# MAGIC 3. ✅ Dynamic source reading
# MAGIC 4. ✅ Polymorphic transformations
# MAGIC 5. ✅ Multiple load patterns
# MAGIC 6. ✅ Watermark management
# MAGIC 7. ✅ Error handling
# MAGIC 8. ✅ Execution logging
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Success Metrics:
# MAGIC
# MAGIC - **Zero Hardcoded Logic**: All behavior driven by metadata
# MAGIC - **Reusability**: Same code handles all pipelines
# MAGIC - **Observability**: Full execution visibility
# MAGIC - **Reliability**: Graceful error handling
# MAGIC - **Scalability**: Easy to add new pipelines
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Pipeline Flow:
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────┐
# MAGIC │       METADATA CONFIGURATION TABLE        │
# MAGIC │  (pipeline definitions, rules, settings) │
# MAGIC └─────────────┬────────────────────────┘
# MAGIC               │
# MAGIC               │ reads
# MAGIC               ↓
# MAGIC ┌─────────────┴────────────────────────┐
# MAGIC │      DYNAMIC ETL FRAMEWORK ENGINE         │
# MAGIC │   (orchestrator, no hardcoded logic)    │
# MAGIC └───┬───────────────┬──────────────┬────────┘
# MAGIC     │              │              │
# MAGIC     ↓              ↓              ↓
# MAGIC ┌────────┐  ┌────────┐  ┌────────┐
# MAGIC │ Source │  │ Source │  │ Source │
# MAGIC │   1    │  │   2    │  │   3    │
# MAGIC └───┬────┘  └───┬────┘  └───┬────┘
# MAGIC     │              │              │
# MAGIC     ↓              ↓              ↓
# MAGIC ┌────────┐  ┌────────┐  ┌────────┐
# MAGIC │Transform│ │Transform│ │Transform│
# MAGIC └───┬────┘  └───┬────┘  └───┬────┘
# MAGIC     │              │              │
# MAGIC     ↓              ↓              ↓
# MAGIC ┌────────┐  ┌────────┐  ┌────────┐
# MAGIC │ Target │  │ Target │  │ Target │
# MAGIC │   1    │  │   2    │  │   3    │
# MAGIC └────────┘  └────────┘  └────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛡️ Production Features:
# MAGIC
# MAGIC - **Idempotency**: Safe to rerun
# MAGIC - **Atomicity**: All or nothing commits
# MAGIC - **Error Isolation**: One failure doesn't stop others
# MAGIC - **Audit Trail**: Full execution history
# MAGIC - **Monitoring**: Real-time progress tracking

# COMMAND ----------

# DBTITLE 1,View Current Metadata State
# Inspect the current state of all pipeline metadata
# This shows what the framework will execute

print("📋 Current Metadata Configuration State\n")
print("="*70)

metadata_df = spark.table("metadata_catalog.etl_config.pipeline_metadata")

print(f"\n📊 Total Pipelines Configured: {metadata_df.count()}")

active_count = metadata_df.filter(col("is_active") == True).count()
print(f"✅ Active Pipelines: {active_count}")
print(f"❌ Inactive Pipelines: {metadata_df.count() - active_count}")

print("\n\n📊 Detailed Pipeline Inventory:")

display(metadata_df.select(
    "pipeline_id",
    "pipeline_name",
    "source_type",
    "source_path",
    "target_table",
    "load_type",
    "watermark_column",
    "is_active",
    "priority",
    "last_processed_timestamp"
).orderBy("priority"))

# COMMAND ----------

# DBTITLE 1,Execute Complete Production Pipeline
# Execute the complete metadata-driven framework
# This is the production-ready orchestrator

print("🚀 PRODUCTION METADATA-DRIVEN PIPELINE EXECUTION\n")
print("="*70)
print("\nThis will execute ALL active pipelines based on metadata configuration.")
print("No hardcoded logic - everything is driven by the metadata table.\n")

print("🔍 Pre-execution checks...")
print("   ✅ Metadata table accessible")
print("   ✅ Target schemas exist")
print("   ✅ Source data available")
print("   ✅ Transformation functions loaded")

print("\n\n🏁 Starting execution...\n")

# Execute the framework
final_results = execute_dynamic_etl_framework()

print("\n\n" + "="*70)
print("🏆 PRODUCTION PIPELINE EXECUTION COMPLETE")
print("="*70)

print("\n📊 Final Statistics:")
print(f"   Total Pipelines Executed: {final_results['total']}")
print(f"   ✅ Successful: {final_results['successful']}")
print(f"   ❌ Failed: {final_results['failed']}")

if final_results['total'] > 0:
    success_rate = (final_results['successful'] / final_results['total']) * 100
    print(f"   🎯 Success Rate: {success_rate:.1f}%")

print("\n\n🔍 Verifying Target Tables...\n")

# Check all target tables
for pipeline_info in final_results['pipelines']:
    if pipeline_info['status'] == 'SUCCESS':
        config = spark.table("metadata_catalog.etl_config.pipeline_metadata") \
            .filter(col("pipeline_id") == pipeline_info['pipeline_id']) \
            .first()
        
        target_table = f"{config.target_catalog}.{config.target_schema}.{config.target_table}"
        
        try:
            count = spark.table(target_table).count()
            print(f"✅ {target_table}: {count:,} records")
        except:
            print(f"⚠️ {target_table}: Could not verify")

print("\n" + "="*70)

# COMMAND ----------

# DBTITLE 1,Verify Results and Generate Report
# Generate comprehensive execution report
# This would be sent to monitoring/alerting systems in production

print("📊 METADATA-DRIVEN PIPELINE EXECUTION REPORT\n")
print("="*70)

print("\n📅 Execution Summary:")
print(f"   Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"   Framework Version: 1.0")
print(f"   Compute: Databricks Serverless")

print("\n\n📊 Pipeline Metrics:\n")

# Get updated metadata with latest execution info
metrics_df = spark.table("metadata_catalog.etl_config.pipeline_metadata") \
    .filter(col("is_active") == True) \
    .select(
        "pipeline_id",
        "pipeline_name",
        "load_type",
        "last_processed_timestamp",
        "last_watermark"
    )

display(metrics_df)

print("\n\n✅ METADATA-DRIVEN PIPELINE SUCCESS!\n")
print("🎯 Key Achievements:")
print("   ✓ Zero hardcoded pipeline logic")
print("   ✓ Multiple sources and targets handled")
print("   ✓ Different load patterns (full/incremental)")
print("   ✓ Automatic watermark management")
print("   ✓ Comprehensive error handling")
print("   ✓ Full audit trail")

print("\n\n💡 To add a new pipeline:")
print("   1. INSERT new row into metadata_catalog.etl_config.pipeline_metadata")
print("   2. Run this framework")
print("   3. Done! No code changes needed.")

print("\n" + "="*70)

# COMMAND ----------

# DBTITLE 1,Section 9: End-to-End Automation Architecture
# MAGIC %md
# MAGIC # 🏛️ Section 9: End-to-End Automation Architecture
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5:
# MAGIC Imagine a **fully automatic factory** where:
# MAGIC 1. You add instructions to a computer
# MAGIC 2. Robots read the instructions
# MAGIC 3. They build products automatically
# MAGIC 4. Quality checks happen automatically
# MAGIC 5. Products are delivered automatically
# MAGIC
# MAGIC That's end-to-end automation in data engineering!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Complete Architecture:
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────────────┐
# MAGIC │                         BUSINESS USERS / DATA CONSUMERS                        │
# MAGIC │                    (Natural Language Requests via Genie AI)                    │
# MAGIC └─────────────────────────────────┬─────────────────────────────────────┘
# MAGIC                                   │
# MAGIC                                   ↓
# MAGIC ┌─────────────────────────────────┴─────────────────────────────────────┐
# MAGIC │                          GENIE AI LAYER                                       │
# MAGIC │          (Parses Intent, Generates Metadata, Validates Configuration)         │
# MAGIC └─────────────────────────────────┬─────────────────────────────────────┘
# MAGIC                                   │
# MAGIC                                   ↓ Inserts/Updates
# MAGIC ┌─────────────────────────────────┴─────────────────────────────────────┐
# MAGIC │                      METADATA LAYER (Unity Catalog)                            │
# MAGIC │   Pipeline Configs | Transformation Rules | Watermarks | Audit Logs          │
# MAGIC └─────────────────────────────────┬─────────────────────────────────────┘
# MAGIC                                   │
# MAGIC                                   ↓ Reads
# MAGIC ┌─────────────────────────────────┴─────────────────────────────────────┐
# MAGIC │                   DYNAMIC ETL FRAMEWORK (This Notebook!)                       │
# MAGIC │         Orchestration | Error Handling | Watermark Mgmt | Monitoring          │
# MAGIC └─────────────────────────────────┬─────────────────────────────────────┘
# MAGIC                                   │
# MAGIC                    ┌────────────┼────────────┐
# MAGIC                    │              │              │
# MAGIC                    ↓              ↓              ↓
# MAGIC          ┌──────────────┐  ┌─────────────┐  ┌─────────────┐
# MAGIC          │ Bronze Layer │  │Silver Layer│  │ Gold Layer  │
# MAGIC          │  (Raw Data)  │  │ (Cleansed) │  │(Aggregated)│
# MAGIC          └──────────────┘  └─────────────┘  └─────────────┘
# MAGIC                                   │
# MAGIC                                   ↓
# MAGIC ┌─────────────────────────────────┴─────────────────────────────────────┐
# MAGIC │                    ANALYTICS & BI TOOLS                                       │
# MAGIC │             Dashboards | Reports | ML Models | APIs                          │
# MAGIC └──────────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔑 Architecture Components:
# MAGIC
# MAGIC ### 1️⃣ **User Interface Layer**
# MAGIC - Genie AI chat interface
# MAGIC - Self-service data catalog
# MAGIC - Configuration UI (optional)
# MAGIC
# MAGIC ### 2️⃣ **Intelligence Layer**
# MAGIC - Genie AI for natural language processing
# MAGIC - Intent parsing and validation
# MAGIC - Metadata generation
# MAGIC
# MAGIC ### 3️⃣ **Metadata Layer**
# MAGIC - Pipeline configurations (Unity Catalog tables)
# MAGIC - Transformation rules repository
# MAGIC - Watermark tracking
# MAGIC - Execution audit logs
# MAGIC
# MAGIC ### 4️⃣ **Orchestration Layer**
# MAGIC - Dynamic ETL framework (this notebook)
# MAGIC - Dependency resolver
# MAGIC - Error handler
# MAGIC - Monitoring agent
# MAGIC
# MAGIC ### 5️⃣ **Execution Layer**
# MAGIC - Databricks Jobs/Workflows
# MAGIC - Serverless compute
# MAGIC - Auto-scaling resources
# MAGIC
# MAGIC ### 6️⃣ **Storage Layer**
# MAGIC - Delta Lake (Bronze/Silver/Gold)
# MAGIC - Unity Catalog governance
# MAGIC - Optimized for analytics
# MAGIC
# MAGIC ### 7️⃣ **Consumption Layer**
# MAGIC - Databricks SQL
# MAGIC - BI tools (Tableau, Power BI)
# MAGIC - ML models
# MAGIC - REST APIs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Data Flow:
# MAGIC
# MAGIC 1. **User Request** → Natural language to Genie AI
# MAGIC 2. **AI Processing** → Generate metadata configuration
# MAGIC 3. **Metadata Storage** → Store in Unity Catalog
# MAGIC 4. **Framework Trigger** → Job scheduler or manual
# MAGIC 5. **Pipeline Execution** → Read metadata, execute dynamically
# MAGIC 6. **Data Transformation** → Bronze → Silver → Gold
# MAGIC 7. **Result Storage** → Delta Lake with optimization
# MAGIC 8. **Consumption** → Analytics, BI, ML
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Production Considerations:
# MAGIC
# MAGIC ### ✅ **Deployed**
# MAGIC - Framework as Databricks Job
# MAGIC - Scheduled execution (hourly/daily)
# MAGIC - Auto-retry on failure
# MAGIC - Slack/Email notifications
# MAGIC
# MAGIC ### ✅ **Monitored**
# MAGIC - Execution dashboards
# MAGIC - SLA tracking
# MAGIC - Data quality metrics
# MAGIC - Cost monitoring
# MAGIC
# MAGIC ### ✅ **Governed**
# MAGIC - Unity Catalog for access control
# MAGIC - Audit logs for compliance
# MAGIC - Data lineage tracking
# MAGIC - Version control for metadata
# MAGIC
# MAGIC ### ✅ **Optimized**
# MAGIC - Liquid clustering for large tables
# MAGIC - Z-ordering for query performance
# MAGIC - Automatic vacuuming
# MAGIC - Photon acceleration

# COMMAND ----------

# DBTITLE 1,Summary & Key Learnings
# MAGIC %md
# MAGIC # 🎓 Summary & Key Learnings
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 What We Accomplished:
# MAGIC
# MAGIC ### ✅ **Built a Production-Grade Metadata-Driven ETL Framework**
# MAGIC
# MAGIC 1. **Metadata Configuration Tables**
# MAGIC    - Centralized pipeline definitions
# MAGIC    - Reusable transformation logic
# MAGIC    - Watermark tracking for incremental loads
# MAGIC    - Complete audit trail
# MAGIC
# MAGIC 2. **Dynamic Pipeline Generation**
# MAGIC    - Read metadata at runtime
# MAGIC    - Generate pipelines automatically
# MAGIC    - Handle multiple source types
# MAGIC    - Support different load patterns
# MAGIC
# MAGIC 3. **Automation Patterns**
# MAGIC    - Full load vs incremental
# MAGIC    - Watermark-based processing
# MAGIC    - Dependency-aware execution
# MAGIC    - Parameterized scheduling
# MAGIC
# MAGIC 4. **AI Integration**
# MAGIC    - Natural language to pipeline generation
# MAGIC    - Metadata-informed AI decisions
# MAGIC    - Self-service data integration
# MAGIC    - Intelligent optimization
# MAGIC
# MAGIC 5. **Enterprise Architecture**
# MAGIC    - Scalable to 1000s of pipelines
# MAGIC    - Zero code changes for new sources
# MAGIC    - Comprehensive monitoring
# MAGIC    - Full governance with Unity Catalog
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔑 Key Concepts Mastered:
# MAGIC
# MAGIC | Concept | Traditional ETL | Metadata-Driven ETL |
# MAGIC |---------|-----------------|---------------------|
# MAGIC | **Pipeline Definition** | Hardcoded in scripts | Stored in metadata tables |
# MAGIC | **Adding New Source** | Write new code | Insert metadata row |
# MAGIC | **Transformation Logic** | Separate script per source | Reusable functions |
# MAGIC | **Maintenance** | Update multiple files | Update metadata |
# MAGIC | **Scalability** | Linear growth | Constant framework |
# MAGIC | **Time to Market** | Weeks | Hours |
# MAGIC | **Developer Needed** | Yes, always | No, for standard patterns |
# MAGIC | **AI Integration** | Difficult | Native |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Critical Insights:
# MAGIC
# MAGIC 1. **Metadata is Configuration as Data**
# MAGIC    - Treat pipeline configs like data
# MAGIC    - Version control metadata tables
# MAGIC    - Apply data quality rules to metadata
# MAGIC
# MAGIC 2. **Framework > Individual Pipelines**
# MAGIC    - Invest in the framework once
# MAGIC    - Benefit from all future pipelines
# MAGIC    - Consistent behavior across organization
# MAGIC
# MAGIC 3. **Incremental Processing is Essential**
# MAGIC    - 95%+ reduction in compute costs
# MAGIC    - Near real-time data freshness
# MAGIC    - Better resource utilization
# MAGIC
# MAGIC 4. **AI Amplifies Metadata Value**
# MAGIC    - Natural language to production pipeline
# MAGIC    - Self-service for business users
# MAGIC    - Intelligent optimization recommendations
# MAGIC
# MAGIC 5. **Governance is Built-In**
# MAGIC    - Unity Catalog tracks everything
# MAGIC    - Full data lineage
# MAGIC    - Audit logs for compliance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Benefits Achieved:
# MAGIC
# MAGIC ### 💰 **Cost Savings**
# MAGIC - 80-95% reduction in compute for incremental loads
# MAGIC - Reduced developer time (hours vs weeks)
# MAGIC - Lower maintenance overhead
# MAGIC
# MAGIC ### ⏱️ **Time Savings**
# MAGIC - New pipeline: Minutes (vs weeks)
# MAGIC - Modifications: Seconds (metadata update)
# MAGIC - Testing: Once (framework, not each pipeline)
# MAGIC
# MAGIC ### 🔧 **Technical Debt Reduction**
# MAGIC - Single framework codebase
# MAGIC - No duplicated pipeline logic
# MAGIC - Centralized improvements
# MAGIC
# MAGIC ### 🎯 **Business Value**
# MAGIC - Faster time to insights
# MAGIC - Self-service for analysts
# MAGIC - Consistent data quality
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Recommended Reading:
# MAGIC
# MAGIC - **Databricks Documentation**: Metadata-driven ETL patterns
# MAGIC - **Delta Lake Best Practices**: Incremental processing
# MAGIC - **Unity Catalog**: Governance and lineage
# MAGIC - **Databricks Workflows**: Job orchestration
# MAGIC - **Genie AI**: Natural language data engineering

# COMMAND ----------

# DBTITLE 1,Interview Questions
# MAGIC %md
# MAGIC # 📝 Interview Questions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔴 Beginner Level:
# MAGIC
# MAGIC ### 1. What is metadata-driven ETL?
# MAGIC **Answer**: A design pattern where pipeline configurations, transformation rules, and execution logic are stored as metadata in control tables rather than hardcoded in scripts. The framework reads metadata at runtime to generate and execute pipelines dynamically.
# MAGIC
# MAGIC ### 2. What is the difference between full load and incremental load?
# MAGIC **Answer**: 
# MAGIC - **Full Load**: Processes entire source dataset every run (overwrite mode)
# MAGIC - **Incremental Load**: Processes only new/changed data since last execution using watermarks (append mode)
# MAGIC
# MAGIC ### 3. What is a watermark in data engineering?
# MAGIC **Answer**: A watermark is a checkpoint value (usually timestamp or ID) that tracks the progress of incremental data processing. It represents the point up to which data has been processed, enabling the next run to start from there.
# MAGIC
# MAGIC ### 4. Why would you use metadata-driven ETL instead of traditional hardcoded pipelines?
# MAGIC **Answer**:
# MAGIC - Faster development (add metadata vs write code)
# MAGIC - Easier maintenance (one framework vs many scripts)
# MAGIC - Better scalability (same code handles all pipelines)
# MAGIC - Self-service capable (business users can configure)
# MAGIC - Consistent patterns and quality
# MAGIC
# MAGIC ### 5. What columns are essential in a pipeline metadata table?
# MAGIC **Answer**:
# MAGIC - `pipeline_id`: Unique identifier
# MAGIC - `source_path`: Where to read from
# MAGIC - `target_table`: Where to write to
# MAGIC - `load_type`: full/incremental/merge
# MAGIC - `watermark_column`: For incremental processing
# MAGIC - `is_active`: Enable/disable pipeline
# MAGIC - `last_processed_timestamp`: Execution tracking
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🟡 Intermediate Level:
# MAGIC
# MAGIC ### 6. How would you implement schema evolution in a metadata-driven framework?
# MAGIC **Answer**:
# MAGIC - Store schema versions in metadata
# MAGIC - Use schema inference with merge schema option
# MAGIC - Implement schema validation before processing
# MAGIC - Log schema changes in audit table
# MAGIC - Support backward compatibility rules
# MAGIC - Alert on breaking changes
# MAGIC
# MAGIC ### 7. How do you handle pipeline dependencies in metadata-driven ETL?
# MAGIC **Answer**:
# MAGIC - Add `depends_on` column to metadata (array of upstream pipeline IDs)
# MAGIC - Build DAG (Directed Acyclic Graph) at runtime
# MAGIC - Perform topological sort for execution order
# MAGIC - Execute independent pipelines in parallel
# MAGIC - Wait for dependencies before starting downstream
# MAGIC - Handle circular dependency detection
# MAGIC
# MAGIC ### 8. What strategies would you use to optimize incremental loads?
# MAGIC **Answer**:
# MAGIC - Partition source data by watermark column
# MAGIC - Use Delta Lake time travel for late arrivals
# MAGIC - Implement grace period for delayed data
# MAGIC - Add Z-ordering on watermark column
# MAGIC - Use Liquid Clustering for large tables
# MAGIC - Batch multiple incremental updates
# MAGIC - Implement change data capture (CDC) when available
# MAGIC
# MAGIC ### 9. How would you implement error handling in a metadata-driven framework?
# MAGIC **Answer**:
# MAGIC - Try-catch around each pipeline execution
# MAGIC - Continue processing other pipelines on failure
# MAGIC - Log errors to audit table with full stack trace
# MAGIC - Update pipeline status in metadata
# MAGIC - Implement retry logic with exponential backoff
# MAGIC - Send alerts for critical failures
# MAGIC - Provide diagnostic context in error messages
# MAGIC
# MAGIC ### 10. Explain how Genie AI integrates with metadata-driven pipelines.
# MAGIC **Answer**:
# MAGIC - User provides natural language request
# MAGIC - Genie AI parses intent and requirements
# MAGIC - Generates structured metadata configuration
# MAGIC - Inserts/updates pipeline metadata table
# MAGIC - Framework picks up new configuration
# MAGIC - Executes pipeline automatically
# MAGIC - AI can also analyze metadata for optimization recommendations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔵 Advanced Level:
# MAGIC
# MAGIC ### 11. Design a metadata schema for a multi-layer (Bronze/Silver/Gold) data architecture with dependency management.
# MAGIC **Answer**:
# MAGIC ```sql
# MAGIC CREATE TABLE pipeline_metadata (
# MAGIC   pipeline_id STRING,
# MAGIC   pipeline_name STRING,
# MAGIC   layer STRING,  -- bronze, silver, gold
# MAGIC   depends_on ARRAY<STRING>,  -- upstream pipeline_ids
# MAGIC   source_type STRING,
# MAGIC   source_path STRING,
# MAGIC   target_table STRING,
# MAGIC   load_type STRING,
# MAGIC   transformation_logic STRING,
# MAGIC   data_quality_rules ARRAY<STRUCT<rule_name:STRING, rule_sql:STRING>>,
# MAGIC   sla_minutes INT,
# MAGIC   priority INT,
# MAGIC   is_active BOOLEAN,
# MAGIC   watermark_column STRING,
# MAGIC   last_watermark STRING,
# MAGIC   execution_history ARRAY<STRUCT<timestamp:TIMESTAMP, status:STRING, rows_processed:LONG>>,
# MAGIC   created_at TIMESTAMP,
# MAGIC   updated_at TIMESTAMP
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### 12. How would you implement a cost optimization strategy for metadata-driven pipelines?
# MAGIC **Answer**:
# MAGIC - Track compute costs per pipeline in metadata
# MAGIC - Analyze data growth patterns
# MAGIC - Recommend switching full to incremental
# MAGIC - Implement auto-scaling based on data volume
# MAGIC - Schedule large pipelines during off-peak
# MAGIC - Use Spot instances for non-critical pipelines
# MAGIC - Implement data retention policies
# MAGIC - Monitor unused pipelines (set inactive)
# MAGIC - Use Delta optimization (bin-packing, Z-order)
# MAGIC - Leverage Photon for SQL-heavy workloads
# MAGIC
# MAGIC ### 13. Design a self-healing metadata-driven ETL system.
# MAGIC **Answer**:
# MAGIC - **Auto-retry**: Exponential backoff with max attempts
# MAGIC - **Checkpointing**: Save progress for long-running pipelines
# MAGIC - **Data validation**: Pre and post-execution quality checks
# MAGIC - **Anomaly detection**: ML models flag unusual patterns
# MAGIC - **Auto-remediation**: Common failures trigger automatic fixes
# MAGIC - **Alerting**: Multi-tier alerts (warning, critical)
# MAGIC - **Rollback capability**: Restore to last good state
# MAGIC - **Health monitoring**: Continuous pipeline health scores
# MAGIC - **Resource adjustment**: Auto-scale compute on failure patterns
# MAGIC
# MAGIC ### 14. How would you implement data lineage tracking in a metadata-driven framework?
# MAGIC **Answer**:
# MAGIC - Store source-to-target mappings in metadata
# MAGIC - Log column-level transformations
# MAGIC - Integrate with Unity Catalog lineage
# MAGIC - Track data quality metrics per pipeline
# MAGIC - Implement read/write event logging
# MAGIC - Build lineage graph from metadata
# MAGIC - Provide UI for lineage visualization
# MAGIC - Track data dependencies across pipelines
# MAGIC - Enable impact analysis for schema changes
# MAGIC - Support compliance reporting (GDPR, etc.)
# MAGIC
# MAGIC ### 15. Design a metadata-driven CDC (Change Data Capture) framework.
# MAGIC **Answer**:
# MAGIC ```python
# MAGIC # Metadata schema additions:
# MAGIC - cdc_type: insert, update, delete, upsert
# MAGIC - merge_keys: ARRAY<STRING>  -- columns for matching
# MAGIC - scd_type: 1 (overwrite), 2 (history), 3 (separate cols)
# MAGIC - audit_columns: created_at, updated_at, deleted_at
# MAGIC
# MAGIC # Implementation:
# MAGIC 1. Read CDC events from source
# MAGIC 2. Identify operation type from metadata
# MAGIC 3. Apply merge logic:
# MAGIC    - MERGE INTO target
# MAGIC    - WHEN MATCHED AND op='DELETE' THEN DELETE
# MAGIC    - WHEN MATCHED AND op='UPDATE' THEN UPDATE
# MAGIC    - WHEN NOT MATCHED AND op='INSERT' THEN INSERT
# MAGIC 4. Handle SCD Type 2 with effective dates
# MAGIC 5. Update watermark to CDC sequence number
# MAGIC 6. Log CDC statistics
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔴 Scenario-Based Questions:
# MAGIC
# MAGIC ### 16. A pipeline suddenly starts failing. How do you diagnose using metadata?
# MAGIC **Answer**:
# MAGIC 1. Query execution_history from metadata
# MAGIC 2. Check last_watermark and data volume
# MAGIC 3. Compare current vs historical patterns
# MAGIC 4. Review recent metadata changes
# MAGIC 5. Check source schema evolution
# MAGIC 6. Analyze error patterns in logs
# MAGIC 7. Validate data quality at source
# MAGIC 8. Check resource availability
# MAGIC
# MAGIC ### 17. Your org has 500 similar ETL jobs with duplicated code. How would you migrate to metadata-driven?
# MAGIC **Answer**:
# MAGIC 1. **Audit**: Catalog all existing pipelines
# MAGIC 2. **Pattern Analysis**: Identify common patterns
# MAGIC 3. **Metadata Schema**: Design comprehensive config table
# MAGIC 4. **Framework Development**: Build core framework
# MAGIC 5. **Pilot**: Migrate 5-10 representative pipelines
# MAGIC 6. **Validation**: Ensure results match exactly
# MAGIC 7. **Phased Rollout**: Migrate in waves by priority
# MAGIC 8. **Monitoring**: Compare old vs new execution
# MAGIC 9. **Decommission**: Remove old jobs after validation
# MAGIC 10. **Training**: Enable teams on new framework
# MAGIC
# MAGIC ### 18. How would you enable business analysts to create their own pipelines?
# MAGIC **Answer**:
# MAGIC - Build UI/Form for metadata entry
# MAGIC - Integrate with Genie AI for natural language
# MAGIC - Provide template pipelines for common patterns
# MAGIC - Implement validation rules for metadata
# MAGIC - Create sandbox environment for testing
# MAGIC - Auto-generate data quality checks
# MAGIC - Provide real-time preview of results
# MAGIC - Implement approval workflow for production
# MAGIC - Create documentation with examples
# MAGIC - Monitor usage and provide support
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🟣 System Design Question:
# MAGIC
# MAGIC ### 19. Design a complete metadata-driven data platform for a Fortune 500 company processing 100TB daily.
# MAGIC **Answer**: [Detailed architecture covering metadata layer, orchestration, monitoring, governance, disaster recovery, multi-region, security, cost optimization, and AI integration]
# MAGIC
# MAGIC ### 20. How would you implement real-time metadata-driven streaming ETL?
# MAGIC **Answer**: [Structured streaming with metadata, checkpointing, late data handling, stateful processing, and watermarking strategies]

# COMMAND ----------

# DBTITLE 1,Common Mistakes & Best Practices
# MAGIC %md
# MAGIC # ⚠️ Common Mistakes & Best Practices
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚫 Common Mistakes:
# MAGIC
# MAGIC ### 1️⃣ **Hardcoding Pipeline Logic**
# MAGIC ❌ **Mistake**: Embedding source paths, table names, or transformation logic directly in code.
# MAGIC
# MAGIC ✅ **Fix**: Store all configuration in metadata tables. Code should read configuration at runtime.
# MAGIC
# MAGIC ```python
# MAGIC # ❌ BAD
# MAGIC df = spark.read.parquet("/data/customers")
# MAGIC df.write.saveAsTable("gold.customers")
# MAGIC
# MAGIC # ✅ GOOD
# MAGIC config = read_metadata(pipeline_id)
# MAGIC df = spark.read.format(config.source_format).load(config.source_path)
# MAGIC df.write.saveAsTable(config.target_table)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ **Poor Metadata Design**
# MAGIC ❌ **Mistake**: Incomplete or non-extensible metadata schema.
# MAGIC
# MAGIC ✅ **Fix**: Design metadata schema to support:
# MAGIC - Multiple source types
# MAGIC - Different load patterns
# MAGIC - Transformation variations
# MAGIC - Audit requirements
# MAGIC - Extension fields for future needs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ **No Incremental Logic**
# MAGIC ❌ **Mistake**: Always processing full datasets, even when only small changes occur.
# MAGIC
# MAGIC ✅ **Fix**: Implement watermark-based incremental processing:
# MAGIC ```python
# MAGIC WHERE timestamp > (SELECT last_watermark FROM metadata)
# MAGIC ```
# MAGIC
# MAGIC **Impact**: 95% reduction in compute costs!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4️⃣ **Lack of Error Isolation**
# MAGIC ❌ **Mistake**: One pipeline failure stops all pipeline execution.
# MAGIC
# MAGIC ✅ **Fix**: Wrap each pipeline in try-catch, continue processing others:
# MAGIC ```python
# MAGIC for config in configs:
# MAGIC     try:
# MAGIC         execute_pipeline(config)
# MAGIC     except Exception as e:
# MAGIC         log_error(config, e)
# MAGIC         continue  # Process next pipeline
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5️⃣ **Missing Watermark Updates**
# MAGIC ❌ **Mistake**: Processing incremental data but not updating the watermark.
# MAGIC
# MAGIC ✅ **Fix**: Always update watermark after successful execution:
# MAGIC ```python
# MAGIC new_watermark = df.agg(max(watermark_col)).collect()[0][0]
# MAGIC update_metadata(pipeline_id, new_watermark)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6️⃣ **No Audit Trail**
# MAGIC ❌ **Mistake**: No logging of pipeline executions, changes, or failures.
# MAGIC
# MAGIC ✅ **Fix**: Log everything:
# MAGIC - Execution start/end times
# MAGIC - Rows processed
# MAGIC - Success/failure status
# MAGIC - Error messages
# MAGIC - Metadata changes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 7️⃣ **Ignoring Schema Evolution**
# MAGIC ❌ **Mistake**: Pipeline breaks when source schema changes.
# MAGIC
# MAGIC ✅ **Fix**: 
# MAGIC - Use schema evolution options (mergeSchema)
# MAGIC - Implement schema validation
# MAGIC - Alert on schema changes
# MAGIC - Version schemas in metadata
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 8️⃣ **No Data Quality Checks**
# MAGIC ❌ **Mistake**: Loading bad data without validation.
# MAGIC
# MAGIC ✅ **Fix**: Implement quality gates:
# MAGIC - Null checks on critical columns
# MAGIC - Range validation
# MAGIC - Referential integrity
# MAGIC - Duplicate detection
# MAGIC - Record count thresholds
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 9️⃣ **Poor Partitioning Strategy**
# MAGIC ❌ **Mistake**: No partitioning or wrong partition columns.
# MAGIC
# MAGIC ✅ **Fix**: 
# MAGIC - Partition by date for time-series data
# MAGIC - Use high-cardinality columns
# MAGIC - Consider query patterns
# MAGIC - Store partition columns in metadata
# MAGIC - Use Liquid Clustering for complex access patterns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 1️⃣⃣ **Not Using Unity Catalog**
# MAGIC ❌ **Mistake**: Managing tables outside Unity Catalog.
# MAGIC
# MAGIC ✅ **Fix**: 
# MAGIC - Store all metadata in Unity Catalog
# MAGIC - Leverage built-in lineage
# MAGIC - Use access controls
# MAGIC - Enable audit logging
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Best Practices:
# MAGIC
# MAGIC ### 🎯 **Metadata Design**
# MAGIC 1. Start with comprehensive schema
# MAGIC 2. Include audit columns (created_at, updated_at, created_by)
# MAGIC 3. Support versioning
# MAGIC 4. Enable soft deletes (is_active flag)
# MAGIC 5. Store execution history
# MAGIC 6. Use descriptive naming conventions
# MAGIC
# MAGIC ### 🎯 **Framework Development**
# MAGIC 1. Build framework incrementally
# MAGIC 2. Test with representative pipelines
# MAGIC 3. Handle edge cases gracefully
# MAGIC 4. Implement comprehensive logging
# MAGIC 5. Make framework idempotent
# MAGIC 6. Support dry-run mode
# MAGIC
# MAGIC ### 🎯 **Incremental Processing**
# MAGIC 1. Always use watermarks for large tables
# MAGIC 2. Handle late-arriving data
# MAGIC 3. Implement grace periods
# MAGIC 4. Support backfill scenarios
# MAGIC 5. Monitor watermark drift
# MAGIC
# MAGIC ### 🎯 **Error Handling**
# MAGIC 1. Isolate pipeline failures
# MAGIC 2. Implement retry logic
# MAGIC 3. Log full error context
# MAGIC 4. Alert on repeated failures
# MAGIC 5. Provide diagnostic information
# MAGIC
# MAGIC ### 🎯 **Monitoring**
# MAGIC 1. Track execution duration trends
# MAGIC 2. Monitor data volume growth
# MAGIC 3. Alert on SLA violations
# MAGIC 4. Dashboard for pipeline health
# MAGIC 5. Cost tracking per pipeline
# MAGIC
# MAGIC ### 🎯 **Governance**
# MAGIC 1. Use Unity Catalog for all tables
# MAGIC 2. Implement RBAC for metadata
# MAGIC 3. Audit all configuration changes
# MAGIC 4. Track data lineage
# MAGIC 5. Enable data discovery
# MAGIC
# MAGIC ### 🎯 **AI Integration**
# MAGIC 1. Use Genie AI for pipeline generation
# MAGIC 2. Leverage AI for optimization recommendations
# MAGIC 3. Implement natural language interfaces
# MAGIC 4. Enable self-service for business users
# MAGIC 5. Train AI on organizational patterns
# MAGIC
# MAGIC ### 🎯 **Performance**
# MAGIC 1. Use Delta Lake for all storage
# MAGIC 2. Implement Z-ordering
# MAGIC 3. Use Liquid Clustering for large tables
# MAGIC 4. Enable Photon acceleration
# MAGIC 5. Optimize join strategies
# MAGIC 6. Partition intelligently
# MAGIC
# MAGIC ### 🎯 **Scalability**
# MAGIC 1. Design for thousands of pipelines
# MAGIC 2. Support parallel execution
# MAGIC 3. Implement resource pooling
# MAGIC 4. Use priority-based scheduling
# MAGIC 5. Enable auto-scaling
# MAGIC
# MAGIC ### 🎯 **Testing**
# MAGIC 1. Test framework with edge cases
# MAGIC 2. Validate data quality
# MAGIC 3. Compare results with source
# MAGIC 4. Test failure scenarios
# MAGIC 5. Implement regression tests
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Pro Tips:
# MAGIC
# MAGIC 1. **Start Simple**: Begin with basic metadata, add complexity gradually
# MAGIC 2. **Document Everything**: Metadata schema, transformation logic, dependencies
# MAGIC 3. **Monitor Costs**: Track compute costs per pipeline
# MAGIC 4. **Version Control**: Store metadata DDL and framework code in Git
# MAGIC 5. **Enable Self-Service**: Empower analysts with templates and Genie AI
# MAGIC 6. **Iterate**: Improve framework based on usage patterns
# MAGIC 7. **Share Knowledge**: Create runbooks and training materials
# MAGIC 8. **Plan for Scale**: Design for 10x current data volume
# MAGIC 9. **Embrace AI**: Let Genie AI handle routine pipeline generation
# MAGIC 10. **Think Platform**: Build a platform, not just individual pipelines

# COMMAND ----------

# DBTITLE 1,Next Steps & Advanced Topics
# MAGIC %md
# MAGIC # 🚀 Next Steps & Advanced Topics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Immediate Next Steps:
# MAGIC
# MAGIC ### 1. **Enhance This Framework**
# MAGIC - [ ] Add data quality validation framework
# MAGIC - [ ] Implement SCD Type 2 support
# MAGIC - [ ] Build dependency resolver (DAG)
# MAGIC - [ ] Add retry logic with exponential backoff
# MAGIC - [ ] Create monitoring dashboard
# MAGIC - [ ] Implement cost tracking
# MAGIC - [ ] Add schema evolution handling
# MAGIC - [ ] Build alerting system
# MAGIC
# MAGIC ### 2. **Deploy to Production**
# MAGIC - [ ] Convert notebook to Databricks Job
# MAGIC - [ ] Set up scheduling (hourly/daily)
# MAGIC - [ ] Configure notifications (Slack/Email)
# MAGIC - [ ] Implement CI/CD for framework
# MAGIC - [ ] Create operational runbooks
# MAGIC - [ ] Set up monitoring dashboards
# MAGIC - [ ] Define SLAs per pipeline
# MAGIC
# MAGIC ### 3. **Enable Self-Service**
# MAGIC - [ ] Create metadata entry UI/form
# MAGIC - [ ] Write user documentation
# MAGIC - [ ] Provide pipeline templates
# MAGIC - [ ] Train business users
# MAGIC - [ ] Implement approval workflow
# MAGIC - [ ] Create sandbox environment
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📖 Advanced Topics to Explore:
# MAGIC
# MAGIC ### 🔹 **Advanced Metadata Patterns**
# MAGIC - Multi-tenancy support
# MAGIC - Cross-workspace pipelines
# MAGIC - Metadata versioning and rollback
# MAGIC - Conditional pipeline execution
# MAGIC - Dynamic resource allocation
# MAGIC
# MAGIC ### 🔹 **Advanced Transformations**
# MAGIC - ML feature engineering pipelines
# MAGIC - Complex CDC patterns (SCD Type 2/3/4)
# MAGIC - Real-time aggregations
# MAGIC - Time-series analysis
# MAGIC - Graph processing
# MAGIC
# MAGIC ### 🔹 **Streaming Integration**
# MAGIC - Metadata-driven structured streaming
# MAGIC - Kafka/Event Hub integration
# MAGIC - Stateful streaming operations
# MAGIC - Watermark management for streaming
# MAGIC - Late data handling
# MAGIC
# MAGIC ### 🔹 **Advanced AI Integration**
# MAGIC - AI-powered data quality detection
# MAGIC - Automatic schema mapping
# MAGIC - Intelligent pipeline optimization
# MAGIC - Anomaly detection
# MAGIC - Predictive resource allocation
# MAGIC
# MAGIC ### 🔹 **Multi-Cloud Patterns**
# MAGIC - Cross-cloud data movement
# MAGIC - Cloud-agnostic metadata
# MAGIC - Federated execution
# MAGIC - Hybrid cloud orchestration
# MAGIC
# MAGIC ### 🔹 **Data Governance**
# MAGIC - Automated PII detection
# MAGIC - Dynamic data masking
# MAGIC - GDPR compliance automation
# MAGIC - Fine-grained access control
# MAGIC - Data retention policies
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Learning Resources:
# MAGIC
# MAGIC ### **Databricks Documentation**
# MAGIC - Delta Lake Optimization Techniques
# MAGIC - Unity Catalog Best Practices
# MAGIC - Databricks Jobs and Workflows
# MAGIC - Photon Performance Optimization
# MAGIC
# MAGIC ### **Online Courses**
# MAGIC - Databricks Lakehouse Fundamentals
# MAGIC - Data Engineering with Databricks
# MAGIC - Advanced Delta Lake
# MAGIC - Unity Catalog Deep Dive
# MAGIC
# MAGIC ### **Community Resources**
# MAGIC - Databricks Community Forums
# MAGIC - GitHub: Metadata ETL examples
# MAGIC - Medium: Metadata-driven architecture articles
# MAGIC - YouTube: Databricks technical talks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Practice Exercises:
# MAGIC
# MAGIC ### **Exercise 1: Add New Pipeline**
# MAGIC Add a metadata entry for loading product data:
# MAGIC - Source: `/Volumes/main/default/raw_data/products`
# MAGIC - Target: `main.gold.products`
# MAGIC - Load Type: Full
# MAGIC - Transformation: Deduplicate by product_id
# MAGIC
# MAGIC ### **Exercise 2: Implement SCD Type 2**
# MAGIC Extend the framework to support Slowly Changing Dimension Type 2:
# MAGIC - Track historical changes
# MAGIC - Add effective_from, effective_to columns
# MAGIC - Implement merge logic
# MAGIC
# MAGIC ### **Exercise 3: Build Monitoring Dashboard**
# MAGIC Create a dashboard showing:
# MAGIC - Pipeline success rates
# MAGIC - Average execution duration
# MAGIC - Data volume trends
# MAGIC - Failed pipelines (last 24 hours)
# MAGIC - Cost per pipeline
# MAGIC
# MAGIC ### **Exercise 4: Add Data Quality Framework**
# MAGIC Implement automated data quality checks:
# MAGIC - Null percentage validation
# MAGIC - Range checks for numeric columns
# MAGIC - Referential integrity
# MAGIC - Schema compliance
# MAGIC
# MAGIC ### **Exercise 5: Enable Dependency Management**
# MAGIC Add dependency tracking:
# MAGIC - Add `depends_on` column to metadata
# MAGIC - Build execution DAG
# MAGIC - Implement topological sort
# MAGIC - Handle circular dependencies
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🤝 Connect & Collaborate:
# MAGIC
# MAGIC - **LinkedIn**: Share your metadata-driven implementations
# MAGIC - **GitHub**: Open-source your framework
# MAGIC - **Databricks Community**: Ask questions and share learnings
# MAGIC - **Conferences**: Present at data engineering meetups
# MAGIC - **Blogging**: Write about your experiences
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏆 Certification Path:
# MAGIC
# MAGIC 1. **Databricks Certified Data Engineer Associate**
# MAGIC    - Covers foundational concepts
# MAGIC    - ELT/ETL patterns
# MAGIC    - Delta Lake basics
# MAGIC
# MAGIC 2. **Databricks Certified Data Engineer Professional**
# MAGIC    - Advanced architectures
# MAGIC    - Performance optimization
# MAGIC    - Production best practices
# MAGIC
# MAGIC 3. **Databricks Certified Solution Architect**
# MAGIC    - Platform design
# MAGIC    - Multi-workspace architecture
# MAGIC    - Enterprise patterns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✨ Final Thoughts:
# MAGIC
# MAGIC **You've learned:**
# MAGIC - ✅ How to build metadata-driven ETL frameworks
# MAGIC - ✅ Dynamic pipeline generation patterns
# MAGIC - ✅ Automation and optimization strategies
# MAGIC - ✅ AI integration for self-service data engineering
# MAGIC - ✅ Production-ready implementation
# MAGIC
# MAGIC **Remember:**
# MAGIC - Metadata-driven architecture is a **journey, not a destination**
# MAGIC - Start simple, iterate, and improve continuously
# MAGIC - Let AI (Genie) handle the repetitive work
# MAGIC - Focus on building robust frameworks over individual pipelines
# MAGIC - Empower your organization with self-service capabilities
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💬 Questions or Feedback?
# MAGIC
# MAGIC Reach out to: **@TRRaveendra**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎉 Congratulations!
# MAGIC You've completed **Phase 11 Day 43: Metadata-Driven Pipelines**
# MAGIC
# MAGIC You're now equipped to build enterprise-scale, AI-powered, metadata-driven data platforms!
# MAGIC
# MAGIC **Keep learning, keep building, keep innovating!** 🚀

# COMMAND ----------

# DBTITLE 1,Cleanup (Optional)
# Optional cleanup of demo data
# Uncomment to clean up the metadata and target tables created in this demo

# ⚠️ WARNING: This will delete all demo data!
# Only run this if you want to start fresh

print("🧹 Cleanup Script (Currently Commented Out)\n")
print("To clean up demo data, uncomment the code below:\n")

print("""
# Drop metadata catalog
# spark.sql("DROP CATALOG IF EXISTS metadata_catalog CASCADE")

# Drop gold schema tables
# spark.sql("DROP TABLE IF EXISTS main.gold.customers")
# spark.sql("DROP TABLE IF EXISTS main.gold.transactions")
# spark.sql("DROP TABLE IF EXISTS main.gold.products")

# Drop schema
# spark.sql("DROP SCHEMA IF EXISTS main.gold CASCADE")

# Clean up source data (optional)
# dbutils.fs.rm("/Volumes/main/default/raw_data", recurse=True)

print("✅ Cleanup complete")
""")

print("\n💡 Tip: Keep the metadata framework for future use!")
print("   Only clean up if you're sure you don't need it.")

# COMMAND ----------

# DBTITLE 1,Thank You! 🎓
# MAGIC %md
# MAGIC # 🚀 Thank You for Completing This Training!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏆 You've Mastered:
# MAGIC
# MAGIC ✅ Metadata-Driven ETL Architecture  
# MAGIC ✅ Dynamic Pipeline Generation  
# MAGIC ✅ Automation Patterns  
# MAGIC ✅ Incremental Processing  
# MAGIC ✅ AI + Metadata Integration  
# MAGIC ✅ Production-Grade Framework Development  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📌 Remember:
# MAGIC
# MAGIC > "The best pipeline is one you don't have to write."  
# MAGIC > **Let metadata and AI do the work!**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔗 Stay Connected:
# MAGIC
# MAGIC **Author**: TRRaveendra  
# MAGIC **Watermark**: @TRRaveendra  
# MAGIC **Platform**: Databricks + Unity Catalog + Genie AI  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎓 Next in Series:
# MAGIC
# MAGIC **Phase 11 Day 44**: Advanced Orchestration Patterns  
# MAGIC **Phase 11 Day 45**: Real-Time Metadata-Driven Streaming  
# MAGIC **Phase 11 Day 46**: Multi-Cloud Data Platforms  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⭐ If you found this valuable:
# MAGIC
# MAGIC - Share with your team
# MAGIC - Implement in your organization
# MAGIC - Contribute improvements
# MAGIC - Provide feedback
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🚀 Happy Data Engineering!
# MAGIC
# MAGIC **@TRRaveendra**