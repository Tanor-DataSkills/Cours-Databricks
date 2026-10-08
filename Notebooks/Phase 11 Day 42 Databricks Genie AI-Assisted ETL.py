# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # 🤖 Data Engineering Training — Phase 11 Day 42  
# MAGIC ## 🧠 Databricks Genie: AI-Assisted ETL & Prompt-Based Development  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC * Databricks Genie Overview  
# MAGIC * AI-Assisted ETL Development  
# MAGIC * Prompt-Based Code Generation  
# MAGIC * Metadata-Driven Automation  
# MAGIC * Genie + Medallion Architecture Integration  
# MAGIC * AI-Powered Data Quality & Governance  
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
# MAGIC Understand how to leverage Databricks Genie for AI-assisted ETL development, enabling prompt-driven automation of data pipelines while maintaining governance and best practices.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Engineering Constraints:
# MAGIC * ✅ Use Databricks Serverless Compute
# MAGIC * ✅ Use Unity Catalog for all data access
# MAGIC * ✅ Use Delta Lake format (mandatory)
# MAGIC * ✅ Follow AI-first + metadata-driven design
# MAGIC * ❌ DO NOT use RDDs
# MAGIC * ❌ DO NOT use cache() / persist()
# MAGIC * ❌ DO NOT use /tmp or local storage

# COMMAND ----------

# DBTITLE 1,Section 1 - What is Databricks Genie?
# MAGIC %md
# MAGIC # 📦 Section 1: What is Databricks Genie?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌟 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC **Genie is like a smart robot assistant that writes code for you!**
# MAGIC
# MAGIC Imagine you have a magic helper who understands what you want to do with data. Instead of writing complicated code yourself, you just tell Genie in plain English:
# MAGIC * "Show me sales by region"
# MAGIC * "Find customers who bought more than 3 times"
# MAGIC * "Create a pipeline to clean my data"
# MAGIC
# MAGIC And Genie writes the code for you! It's like having an expert programmer sitting next to you.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Databricks Genie** is an AI-powered intelligent data assistant that leverages:
# MAGIC
# MAGIC **1. Large Language Models (LLMs):**
# MAGIC * Built on foundation models trained on code patterns
# MAGIC * Understands natural language queries
# MAGIC * Translates business intent into executable code
# MAGIC
# MAGIC **2. Context-Aware Intelligence:**
# MAGIC * Automatically detects table schemas in Unity Catalog
# MAGIC * Understands relationships between tables
# MAGIC * Maintains conversation context for iterative development
# MAGIC
# MAGIC **3. Multi-Modal Code Generation:**
# MAGIC * Generates PySpark DataFrame API code
# MAGIC * Generates SQL queries
# MAGIC * Creates visualizations
# MAGIC * Builds complete ETL pipelines
# MAGIC
# MAGIC **4. Enterprise Integration:**
# MAGIC * Unity Catalog-aware (respects permissions)
# MAGIC * Audit logging enabled
# MAGIC * Governance-compliant code generation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 How Genie Accelerates ETL:
# MAGIC
# MAGIC | Traditional Approach | Genie Approach |
# MAGIC |---------------------|----------------|
# MAGIC | Manual schema discovery | Automatic schema detection |
# MAGIC | Write boilerplate code | Prompt-based generation |
# MAGIC | Debug syntax errors | Pre-validated code |
# MAGIC | Manual documentation | Auto-generated explanations |
# MAGIC | Hours to build pipeline | Minutes to build pipeline |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔑 Key Capabilities:
# MAGIC
# MAGIC 1. **Natural Language to Code**: `"Show top 10 customers by revenue"` → SQL Query
# MAGIC 2. **Schema-Aware Queries**: Automatically joins tables based on foreign keys
# MAGIC 3. **Data Quality Checks**: Generates validation logic
# MAGIC 4. **Pipeline Automation**: Creates Bronze-Silver-Gold pipelines
# MAGIC 5. **Optimization Suggestions**: Recommends partitioning, Z-ordering
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚡ Genie vs Traditional Development:
# MAGIC
# MAGIC ```
# MAGIC Traditional:
# MAGIC   Read docs → Write code → Debug → Test → Deploy (Hours/Days)
# MAGIC
# MAGIC Genie:
# MAGIC   Prompt → Review code → Execute → Iterate (Minutes)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 2 - AI-Assisted ETL Fundamentals
# MAGIC %md
# MAGIC # ⚙️ Section 2: AI-Assisted ETL Fundamentals
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌟 ELI5:
# MAGIC
# MAGIC **ETL** means:
# MAGIC * **E**xtract: Get data from somewhere
# MAGIC * **T**ransform: Clean and organize it
# MAGIC * **L**oad: Put it somewhere useful
# MAGIC
# MAGIC **AI-Assisted ETL** means Genie helps you do all three steps automatically!
# MAGIC
# MAGIC Instead of writing 100 lines of code, you say:
# MAGIC * "Load customer data from S3"
# MAGIC * "Remove duplicates and fix dates"
# MAGIC * "Save cleaned data to Delta table"
# MAGIC
# MAGIC Genie does the hard work!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **AI-Assisted ETL** transforms traditional data engineering by:
# MAGIC
# MAGIC ### **1. Automated Pipeline Generation:**
# MAGIC * **Input**: Natural language requirements + metadata
# MAGIC * **Process**: LLM analyzes schema, relationships, business logic
# MAGIC * **Output**: Production-ready PySpark/SQL code
# MAGIC
# MAGIC ### **2. Intelligent Transformation Logic:**
# MAGIC * **Schema Evolution**: Auto-detects schema changes
# MAGIC * **Data Quality**: Generates validation rules based on data profiling
# MAGIC * **Type Inference**: Automatically casts and converts data types
# MAGIC
# MAGIC ### **3. Metadata-Driven Approach:**
# MAGIC ```
# MAGIC Metadata (Unity Catalog)
# MAGIC     ↓
# MAGIC AI Analysis (Genie)
# MAGIC     ↓
# MAGIC Code Generation (PySpark/SQL)
# MAGIC     ↓
# MAGIC Execution (Serverless Compute)
# MAGIC     ↓
# MAGIC Governed Data Assets (Delta Tables)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Benefits of AI-Assisted ETL:
# MAGIC
# MAGIC | Benefit | Traditional ETL | AI-Assisted ETL |
# MAGIC |---------|----------------|------------------|
# MAGIC | **Development Time** | 2-5 days | 30 minutes - 2 hours |
# MAGIC | **Code Quality** | Varies by developer | Consistent best practices |
# MAGIC | **Documentation** | Manual | Auto-generated |
# MAGIC | **Error Handling** | Manual implementation | Built-in validation |
# MAGIC | **Schema Changes** | Manual updates | Auto-detection |
# MAGIC | **Testing** | Write unit tests | Generated test cases |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Use Cases for AI-Assisted ETL:
# MAGIC
# MAGIC 1. **Rapid Prototyping**: Build POC pipelines in minutes
# MAGIC 2. **Boilerplate Reduction**: Eliminate repetitive code patterns
# MAGIC 3. **Learning Tool**: See best practices in generated code
# MAGIC 4. **Migration Assistance**: Convert legacy ETL to modern patterns
# MAGIC 5. **Data Exploration**: Quickly understand new datasets
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 When to Use AI-Assisted ETL:
# MAGIC
# MAGIC ✅ **Good Use Cases:**
# MAGIC * Standard ETL patterns (Bronze → Silver → Gold)
# MAGIC * Data exploration and profiling
# MAGIC * Simple transformations (filtering, aggregations, joins)
# MAGIC * Schema validation and type conversion
# MAGIC
# MAGIC ⚠️ **Use with Caution:**
# MAGIC * Complex business logic requiring domain expertise
# MAGIC * Performance-critical pipelines (review generated code)
# MAGIC * Regulatory/compliance scenarios (human validation required)
# MAGIC * Novel algorithms or custom transformations

# COMMAND ----------

# DBTITLE 1,Section 3 - Prompt-Based Development
# MAGIC %md
# MAGIC # 📝 Section 3: Prompt-Based Development
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌟 ELI5:
# MAGIC
# MAGIC **Prompt** = The instructions you give to Genie
# MAGIC
# MAGIC Good prompts = Good code
# MAGIC Bad prompts = Confused robot
# MAGIC
# MAGIC **Example:**
# MAGIC * ❌ Bad: "data thing"
# MAGIC * ✅ Good: "Load sales data from catalog.schema.sales_raw and calculate total revenue by product category"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Prompt Engineering for Data Pipelines** requires:
# MAGIC
# MAGIC ### **1. Structured Prompt Components:**
# MAGIC
# MAGIC ```
# MAGIC [CONTEXT] + [ACTION] + [CONSTRAINTS] + [OUTPUT]
# MAGIC ```
# MAGIC
# MAGIC **Example:**
# MAGIC ```
# MAGIC CONTEXT: "Using Unity Catalog table main.bronze.customer_events"
# MAGIC ACTION: "Create a Silver layer transformation"
# MAGIC CONSTRAINTS: "Remove duplicates, validate email format, convert timestamps to UTC"
# MAGIC OUTPUT: "Save to main.silver.customers as Delta table with partitioning by date"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **2. Effective Prompt Patterns:**
# MAGIC
# MAGIC #### **Pattern 1: Pipeline Generation**
# MAGIC ```
# MAGIC "Create a Medallion pipeline for [DOMAIN] data:
# MAGIC - Bronze: Read from [SOURCE]
# MAGIC - Silver: [TRANSFORMATIONS]
# MAGIC - Gold: [AGGREGATIONS]
# MAGIC Use Delta format with [OPTIMIZATION]"
# MAGIC ```
# MAGIC
# MAGIC #### **Pattern 2: Data Quality**
# MAGIC ```
# MAGIC "Add data quality checks to [TABLE]:
# MAGIC - Validate [COLUMN] is not null
# MAGIC - Check [COLUMN] matches pattern [REGEX]
# MAGIC - Ensure [COLUMN] is within range [MIN-MAX]"
# MAGIC ```
# MAGIC
# MAGIC #### **Pattern 3: Optimization**
# MAGIC ```
# MAGIC "Optimize [TABLE] for query pattern:
# MAGIC - Partition by [COLUMN]
# MAGIC - Z-order by [COLUMNS]
# MAGIC - Enable liquid clustering"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Prompt Quality Spectrum:
# MAGIC
# MAGIC | Level | Prompt | Result Quality |
# MAGIC |-------|--------|----------------|
# MAGIC | ❌ **Poor** | "sales data" | Ambiguous, may error |
# MAGIC | ⚠️ **Basic** | "Show sales data" | Generic query |
# MAGIC | ✅ **Good** | "Show total sales by region for Q1 2024" | Specific, executable |
# MAGIC | 🏆 **Excellent** | "Analyze sales trends by region for Q1 2024, filtering out returns, grouping by week, and displaying as time series with variance" | Comprehensive, optimized |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Prompt Best Practices:
# MAGIC
# MAGIC ### **DO:**
# MAGIC 1. ✅ **Be Specific**: Mention table names, columns, transformations
# MAGIC 2. ✅ **Provide Context**: Specify catalog, schema, file locations
# MAGIC 3. ✅ **Define Constraints**: Data types, validation rules, performance needs
# MAGIC 4. ✅ **Specify Output**: Desired format, partitioning, storage location
# MAGIC 5. ✅ **Include Examples**: Sample data or expected results
# MAGIC
# MAGIC ### **DON'T:**
# MAGIC 1. ❌ **Be Vague**: "do something with data"
# MAGIC 2. ❌ **Assume Context**: Genie doesn't remember previous sessions
# MAGIC 3. ❌ **Skip Validation**: Always review generated code
# MAGIC 4. ❌ **Ignore Errors**: Refine prompts if output is incorrect
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Real-World Prompt Examples:
# MAGIC
# MAGIC ### **Example 1: Simple Query**
# MAGIC ```
# MAGIC Prompt: "Show top 10 products by revenue from main.gold.sales_summary"
# MAGIC
# MAGIC Generated: SELECT product_name, SUM(revenue) as total_revenue 
# MAGIC            FROM main.gold.sales_summary 
# MAGIC            GROUP BY product_name 
# MAGIC            ORDER BY total_revenue DESC 
# MAGIC            LIMIT 10
# MAGIC ```
# MAGIC
# MAGIC ### **Example 2: ETL Pipeline**
# MAGIC ```
# MAGIC Prompt: "Create a Bronze to Silver pipeline:
# MAGIC - Read from main.bronze.raw_transactions
# MAGIC - Remove rows where amount is null or negative
# MAGIC - Convert transaction_date to date type
# MAGIC - Add processed_timestamp column
# MAGIC - Write to main.silver.validated_transactions with date partitioning"
# MAGIC
# MAGIC Generated: [Complete PySpark DataFrame transformation code]
# MAGIC ```
# MAGIC
# MAGIC ### **Example 3: Data Quality**
# MAGIC ```
# MAGIC Prompt: "Add data quality validation to main.silver.customers:
# MAGIC - Email must match regex pattern
# MAGIC - Phone must be 10 digits
# MAGIC - Age must be between 18 and 120
# MAGIC - Record validation failures to main.quality.validation_errors"
# MAGIC
# MAGIC Generated: [Data quality framework code with exception handling]
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 Iterative Prompt Refinement:
# MAGIC
# MAGIC **Scenario**: Building a customer segmentation pipeline
# MAGIC
# MAGIC **Iteration 1**: "Segment customers"
# MAGIC * Result: Too vague, Genie asks for clarification
# MAGIC
# MAGIC **Iteration 2**: "Segment customers by purchase behavior"
# MAGIC * Result: Basic grouping, but missing criteria
# MAGIC
# MAGIC **Iteration 3**: "Segment customers from main.gold.customer_orders into:
# MAGIC - High Value: >$10K annual spend
# MAGIC - Medium Value: $1K-$10K
# MAGIC - Low Value: <$1K
# MAGIC Include customer count and average order value per segment"
# MAGIC * Result: ✅ Complete, accurate segmentation query
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚡ Advanced Prompt Techniques:
# MAGIC
# MAGIC ### **1. Chain-of-Thought Prompting:**
# MAGIC ```
# MAGIC "Step by step:
# MAGIC 1. Read raw data from S3 path s3://bucket/data/
# MAGIC 2. Parse JSON with schema inference
# MAGIC 3. Flatten nested structures
# MAGIC 4. Remove duplicates based on id and timestamp
# MAGIC 5. Write to main.bronze.events as Delta"
# MAGIC ```
# MAGIC
# MAGIC ### **2. Few-Shot Learning:**
# MAGIC ```
# MAGIC "Create transformations similar to this pattern:
# MAGIC Example: customer_id -> cust_id (rename)
# MAGIC          order_date -> order_dt (rename + convert to date)
# MAGIC          
# MAGIC Apply to: transaction_timestamp, user_identifier, product_code"
# MAGIC ```
# MAGIC
# MAGIC ### **3. Constraint-Based:**
# MAGIC ```
# MAGIC "Generate ETL code with these requirements:
# MAGIC - Must use serverless compute (no .cache() or .persist())
# MAGIC - Must use Unity Catalog paths
# MAGIC - Must include error handling
# MAGIC - Must log execution metrics"
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 4 - Genie Code Generation Examples
# MAGIC %md
# MAGIC # 💻 Section 4: Genie Code Generation in Action
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌟 ELI5:
# MAGIC
# MAGIC Let's see Genie actually write code for us!
# MAGIC
# MAGIC We'll give Genie instructions, and it will create:
# MAGIC * Python code (PySpark)
# MAGIC * SQL queries
# MAGIC * Complete data pipelines
# MAGIC
# MAGIC Then we'll check if the code is good!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ What Genie Generates:
# MAGIC
# MAGIC ### **1. PySpark DataFrame API Code**
# MAGIC ### **2. SQL Queries**
# MAGIC ### **3. Data Quality Checks**
# MAGIC ### **4. Complete ETL Pipelines**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Let's simulate what Genie would generate for common scenarios

# COMMAND ----------

# DBTITLE 1,Example 1 - Genie Generated: Bronze Layer Ingestion
# ============================================================
# PROMPT: "Create a Bronze layer to read JSON files from 
# /mnt/raw/sales/ and write to main.bronze.sales_raw"
# ============================================================
# Generated by: Databricks Genie (Simulated)
# Purpose: Bronze layer ingestion from JSON files
# Author: @TRRaveendra (Training Demo)
# ============================================================

from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Define schema for JSON data (Genie can infer this from sample data)
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("transaction_date", StringType(), True)
])

# Note: Using /mnt/ as example path - in production, use Unity Catalog Volumes
print("\u26a0️ Note: This is a simulated example demonstrating Genie code generation patterns")
print("In production, use Unity Catalog Volumes: /Volumes/catalog/schema/volume/path")
print("\nGenie would generate code like this:")
print("""
# Read JSON files from source
df_bronze = (spark.read
    .format("json")
    .schema(schema)
    .option("multiLine", "true")
    .load("/Volumes/main/raw/sales/")
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name())
)

# Write to Bronze Delta table
(df_bronze.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("main.bronze.sales_raw")
)

print(f"✅ Loaded {df_bronze.count()} records to Bronze layer")
""")

print("\n" + "="*60)
print("💡 KEY FEATURES OF GENIE-GENERATED CODE:")
print("="*60)
print("1. Schema definition included")
print("2. Audit columns added (ingestion_timestamp, source_file)")
print("3. Delta format with schema evolution")
print("4. Error handling and logging")
print("5. Serverless-compatible (no cache/persist)")

# COMMAND ----------

# DBTITLE 1,Example 2 - Genie Generated: Silver Layer Transformation
# ============================================================
# PROMPT: "Transform Bronze to Silver: clean data, remove 
# duplicates, validate amounts, parse dates"
# ============================================================
# Generated by: Databricks Genie (Simulated)
# Purpose: Silver layer data quality and transformation
# Author: @TRRaveendra (Training Demo)
# ============================================================

from pyspark.sql.functions import (
    col, to_date, trim, upper, when, 
    current_timestamp, sha2, concat_ws, row_number
)
from pyspark.sql.window import Window

print("\u26a0️ Simulated Genie-Generated Silver Layer Code:")
print("\nGenie would analyze Bronze schema and generate:")
print("""
# Read from Bronze
df_bronze = spark.table("main.bronze.sales_raw")

# Data quality transformations
df_silver = (df_bronze
    # Remove duplicates based on transaction_id and timestamp
    .withColumn(
        "row_num",
        row_number().over(
            Window.partitionBy("transaction_id")
            .orderBy(col("ingestion_timestamp").desc())
        )
    )
    .filter(col("row_num") == 1)
    .drop("row_num")
    
    # Data cleaning
    .withColumn("customer_id", trim(upper(col("customer_id"))))
    .withColumn("product_id", trim(upper(col("product_id"))))
    
    # Date parsing
    .withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))
    
    # Data validation
    .filter(col("amount").isNotNull())
    .filter(col("amount") > 0)
    .filter(col("quantity").isNotNull())
    .filter(col("quantity") > 0)
    
    # Add quality flags
    .withColumn(
        "is_valid",
        when(
            (col("customer_id").isNotNull()) &
            (col("product_id").isNotNull()) &
            (col("transaction_date").isNotNull()),
            True
        ).otherwise(False)
    )
    
    # Add derived columns
    .withColumn("total_amount", col("amount") * col("quantity"))
    .withColumn("processed_timestamp", current_timestamp())
    
    # Add surrogate key
    .withColumn(
        "transaction_key",
        sha2(concat_ws("||", col("transaction_id"), col("transaction_date")), 256)
    )
)

# Write to Silver Delta table with partitioning
(df_silver.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("transaction_date")
    .option("overwriteSchema", "true")
    .saveAsTable("main.silver.sales_clean")
)

print(f"✅ Processed {df_silver.count()} valid records to Silver layer")
print(f"✅ Partitioned by transaction_date for optimized queries")
""")

print("\n" + "="*60)
print("🔍 GENIE'S INTELLIGENT DECISIONS:")
print("="*60)
print("1. Auto-detected duplicate handling strategy")
print("2. Inferred date format from Bronze data")
print("3. Applied business rule validations (amount > 0)")
print("4. Added surrogate key for dimensional modeling")
print("5. Chose optimal partitioning strategy")
print("6. Included audit trail columns")

# COMMAND ----------

# DBTITLE 1,Example 3 - Genie Generated: Gold Layer Aggregation (SQL)
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- PROMPT: "Create Gold layer aggregations: daily sales by 
# MAGIC -- product category with running totals"
# MAGIC -- ============================================================
# MAGIC -- Generated by: Databricks Genie (Simulated)
# MAGIC -- Purpose: Gold layer analytics-ready aggregations
# MAGIC -- Author: @TRRaveendra (Training Demo)
# MAGIC -- ============================================================
# MAGIC
# MAGIC -- Note: This is a demonstration of Genie-generated SQL patterns
# MAGIC -- In actual use, Genie would connect to your Unity Catalog tables
# MAGIC
# MAGIC SELECT 
# MAGIC   'SIMULATED GENIE SQL GENERATION' as demo_note,
# MAGIC   'Below is the pattern Genie would generate' as explanation;
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- GENIE WOULD GENERATE SQL LIKE THIS:
# MAGIC -- ============================================================
# MAGIC /*
# MAGIC CREATE OR REPLACE TABLE main.gold.daily_sales_summary
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (sale_date)
# MAGIC AS
# MAGIC
# MAGIC WITH daily_aggregates AS (
# MAGIC   SELECT 
# MAGIC     transaction_date as sale_date,
# MAGIC     product_id,
# MAGIC     COUNT(DISTINCT transaction_id) as transaction_count,
# MAGIC     COUNT(DISTINCT customer_id) as unique_customers,
# MAGIC     SUM(total_amount) as daily_revenue,
# MAGIC     AVG(total_amount) as avg_transaction_value,
# MAGIC     SUM(quantity) as total_quantity_sold,
# MAGIC     MIN(total_amount) as min_transaction,
# MAGIC     MAX(total_amount) as max_transaction,
# MAGIC     CURRENT_TIMESTAMP() as aggregation_timestamp
# MAGIC   FROM main.silver.sales_clean
# MAGIC   WHERE is_valid = true
# MAGIC   GROUP BY transaction_date, product_id
# MAGIC ),
# MAGIC
# MAGIC running_totals AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     SUM(daily_revenue) OVER (
# MAGIC       PARTITION BY product_id 
# MAGIC       ORDER BY sale_date 
# MAGIC       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) as cumulative_revenue,
# MAGIC     
# MAGIC     AVG(daily_revenue) OVER (
# MAGIC       PARTITION BY product_id 
# MAGIC       ORDER BY sale_date 
# MAGIC       ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
# MAGIC     ) as revenue_7day_moving_avg,
# MAGIC     
# MAGIC     AVG(daily_revenue) OVER (
# MAGIC       PARTITION BY product_id 
# MAGIC       ORDER BY sale_date 
# MAGIC       ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
# MAGIC     ) as revenue_30day_moving_avg
# MAGIC   FROM daily_aggregates
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   sale_date,
# MAGIC   product_id,
# MAGIC   transaction_count,
# MAGIC   unique_customers,
# MAGIC   daily_revenue,
# MAGIC   avg_transaction_value,
# MAGIC   total_quantity_sold,
# MAGIC   min_transaction,
# MAGIC   max_transaction,
# MAGIC   cumulative_revenue,
# MAGIC   revenue_7day_moving_avg,
# MAGIC   revenue_30day_moving_avg,
# MAGIC   aggregation_timestamp
# MAGIC FROM running_totals
# MAGIC ORDER BY product_id, sale_date;
# MAGIC
# MAGIC -- Add table properties for documentation
# MAGIC ALTER TABLE main.gold.daily_sales_summary 
# MAGIC SET TBLPROPERTIES (
# MAGIC   'description' = 'Daily sales aggregations with running totals by product',
# MAGIC   'created_by' = 'Databricks Genie',
# MAGIC   'quality_level' = 'gold',
# MAGIC   'refresh_frequency' = 'daily'
# MAGIC );
# MAGIC */
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- KEY FEATURES:
# MAGIC -- ============================================================
# MAGIC SELECT 
# MAGIC   'Feature' as aspect,
# MAGIC   'Description' as details
# MAGIC UNION ALL
# MAGIC SELECT '1. Window Functions', 'Running totals and moving averages'
# MAGIC UNION ALL  
# MAGIC SELECT '2. CTEs', 'Readable, modular query structure'
# MAGIC UNION ALL
# MAGIC SELECT '3. Partitioning', 'Optimized for date-based queries'
# MAGIC UNION ALL
# MAGIC SELECT '4. Metadata', 'Table properties for governance'
# MAGIC UNION ALL
# MAGIC SELECT '5. Quality Filter', 'Only processes validated Silver data';

# COMMAND ----------

# DBTITLE 1,Example 4 - Genie Generated: Data Quality Framework
# ============================================================
# PROMPT: "Create a data quality validation framework with 
# configurable rules and error logging"
# ============================================================
# Generated by: Databricks Genie (Simulated)
# Purpose: Reusable data quality validation framework
# Author: @TRRaveendra (Training Demo)
# ============================================================

from pyspark.sql.functions import col, when, lit, current_timestamp, concat_ws
from pyspark.sql.types import StringType, BooleanType

print("\u26a0️ Simulated Genie-Generated Data Quality Framework:")
print("\nGenie would create a reusable validation class:")
print("""
class DataQualityValidator:
    ''''
    AI-Generated Data Quality Framework
    Supports configurable validation rules with error logging
    ''''
    
    def __init__(self, spark, error_table="main.quality.validation_errors"):
        self.spark = spark
        self.error_table = error_table
    
    def validate_not_null(self, df, columns):
        '''Check specified columns for null values'''
        validation_name = "not_null_check"
        
        for column in columns:
            df = df.withColumn(
                f"{column}_is_valid",
                when(col(column).isNull(), False).otherwise(True)
            )
        return df
    
    def validate_range(self, df, column, min_val, max_val):
        '''Check if numeric column values are within specified range'''
        return df.withColumn(
            f"{column}_range_valid",
            when(
                (col(column) >= min_val) & (col(column) <= max_val),
                True
            ).otherwise(False)
        )
    
    def validate_pattern(self, df, column, pattern):
        '''Check if string column matches regex pattern'''
        return df.withColumn(
            f"{column}_pattern_valid",
            col(column).rlike(pattern)
        )
    
    def log_validation_errors(self, df, validation_columns, primary_key):
        '''Log validation failures to error table'''
        
        # Create error condition
        error_condition = None
        for val_col in validation_columns:
            condition = (col(val_col) == False)
            error_condition = condition if error_condition is None else (error_condition | condition)
        
        # Extract failed records
        errors_df = (df
            .filter(error_condition)
            .withColumn("error_timestamp", current_timestamp())
            .withColumn("validation_rule", lit("multiple_rules"))
            .withColumn("error_severity", lit("WARNING"))
        )
        
        if errors_df.count() > 0:
            (errors_df.write
                .format("delta")
                .mode("append")
                .saveAsTable(self.error_table)
            )
            return errors_df.count()
        return 0
    
    def get_quality_metrics(self, df, validation_columns):
        '''Calculate data quality metrics'''
        total_rows = df.count()
        
        metrics = {}
        for val_col in validation_columns:
            valid_count = df.filter(col(val_col) == True).count()
            metrics[val_col] = {
                'total_rows': total_rows,
                'valid_rows': valid_count,
                'invalid_rows': total_rows - valid_count,
                'validity_percentage': (valid_count / total_rows * 100) if total_rows > 0 else 0
            }
        
        return metrics

# Example usage:
# validator = DataQualityValidator(spark)
# df_validated = validator.validate_not_null(df, ['customer_id', 'amount'])
# df_validated = validator.validate_range(df_validated, 'amount', 0, 1000000)
# error_count = validator.log_validation_errors(df_validated, ['customer_id_is_valid', 'amount_range_valid'], 'transaction_id')
# metrics = validator.get_quality_metrics(df_validated, ['customer_id_is_valid', 'amount_range_valid'])
""")

print("\n" + "="*60)
print("🎯 GENIE'S ARCHITECTURAL DECISIONS:")
print("="*60)
print("1. Created reusable class structure")
print("2. Implemented configurable validation rules")
print("3. Added error logging capability")
print("4. Included quality metrics calculation")
print("5. Used Delta tables for error storage")
print("6. Made framework extensible for custom rules")

# COMMAND ----------

# DBTITLE 1,Section 5 - Validating AI-Generated Code
# MAGIC %md
# MAGIC # ✅ Section 5: Validating AI-Generated Code
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌟 ELI5:
# MAGIC
# MAGIC Just because Genie wrote the code doesn't mean it's perfect!
# MAGIC
# MAGIC Always check:
# MAGIC * Does it do what I asked?
# MAGIC * Does it use the right tables?
# MAGIC * Will it work on real data?
# MAGIC * Is it fast enough?
# MAGIC
# MAGIC Think of Genie like a helpful friend who's really smart but might make mistakes!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Code Validation Framework:
# MAGIC
# MAGIC ### **1. Functional Validation**
# MAGIC
# MAGIC ✅ **Check:**
# MAGIC * Does the code match the prompt requirements?
# MAGIC * Are all business rules correctly implemented?
# MAGIC * Does it handle edge cases?
# MAGIC
# MAGIC ❌ **Common Issues:**
# MAGIC * Misinterpreted business logic
# MAGIC * Missing edge case handling
# MAGIC * Incorrect aggregation logic
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **2. Technical Validation**
# MAGIC
# MAGIC ✅ **Check:**
# MAGIC * Is the code serverless-compatible? (no cache/persist)
# MAGIC * Are Unity Catalog paths used correctly?
# MAGIC * Is Delta format specified?
# MAGIC * Are partitioning strategies optimal?
# MAGIC
# MAGIC ❌ **Common Issues:**
# MAGIC * Using deprecated APIs
# MAGIC * Non-serverless patterns (RDD, cache)
# MAGIC * Incorrect path formats
# MAGIC * Missing schema evolution handling
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **3. Performance Validation**
# MAGIC
# MAGIC ✅ **Check:**
# MAGIC * Are there unnecessary shuffles?
# MAGIC * Is the query optimized?
# MAGIC * Are filters pushed down?
# MAGIC * Is partitioning leveraged?
# MAGIC
# MAGIC ❌ **Common Issues:**
# MAGIC * Multiple passes over same data
# MAGIC * Unpartitioned large table joins
# MAGIC * Missing filter pushdown
# MAGIC * Broadcast join opportunities missed
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **4. Security & Governance Validation**
# MAGIC
# MAGIC ✅ **Check:**
# MAGIC * Are Unity Catalog permissions respected?
# MAGIC * Is PII handled correctly?
# MAGIC * Are audit columns included?
# MAGIC * Is lineage trackable?
# MAGIC
# MAGIC ❌ **Common Issues:**
# MAGIC * Direct S3 access bypassing Unity Catalog
# MAGIC * Missing audit trails
# MAGIC * PII not masked/encrypted
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔍 Validation Checklist:
# MAGIC
# MAGIC ### **Before Running Genie Code:**
# MAGIC
# MAGIC | Aspect | Question | Action |
# MAGIC |--------|----------|--------|
# MAGIC | **Functionality** | Does it solve the problem? | Review logic flow |
# MAGIC | **Tables** | Are table references correct? | Verify catalog.schema.table |
# MAGIC | **Schema** | Will it handle schema changes? | Check mergeSchema option |
# MAGIC | **Data Types** | Are type conversions correct? | Review cast operations |
# MAGIC | **Filters** | Are filters applied early? | Check filter placement |
# MAGIC | **Joins** | Are join keys correct? | Verify join conditions |
# MAGIC | **Partitions** | Is partitioning optimal? | Review partition strategy |
# MAGIC | **Error Handling** | What happens on failure? | Check exception handling |
# MAGIC | **Testing** | Can I test with sample data? | Create test dataset |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛡️ Common AI Code Generation Pitfalls:
# MAGIC
# MAGIC ### **Pitfall 1: Over-Optimization**
# MAGIC ```python
# MAGIC # Genie might generate:
# MAGIC df.cache()  # ❌ Not serverless compatible!
# MAGIC
# MAGIC # Instead use:
# MAGIC df  # ✅ Let Spark optimize automatically
# MAGIC ```
# MAGIC
# MAGIC ### **Pitfall 2: Incorrect Path Formats**
# MAGIC ```python
# MAGIC # Genie might generate:
# MAGIC df = spark.read.parquet("/tmp/data")  # ❌ Local storage!
# MAGIC
# MAGIC # Should be:
# MAGIC df = spark.read.table("main.bronze.data")  # ✅ Unity Catalog
# MAGIC # OR
# MAGIC df = spark.read.parquet("/Volumes/main/raw/data/")  # ✅ UC Volumes
# MAGIC ```
# MAGIC
# MAGIC ### **Pitfall 3: Missing Business Logic**
# MAGIC ```python
# MAGIC # Genie might generate:
# MAGIC df_agg = df.groupBy("category").sum("amount")
# MAGIC
# MAGIC # But you needed:
# MAGIC df_agg = (df
# MAGIC     .filter(col("status") == "completed")  # Business rule
# MAGIC     .filter(col("is_valid") == True)       # Quality filter
# MAGIC     .groupBy("category")
# MAGIC     .sum("amount")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Testing AI-Generated Code:
# MAGIC
# MAGIC ### **Step 1: Dry Run with Sample Data**
# MAGIC ```python
# MAGIC # Create small test dataset
# MAGIC df_test = df.limit(100)
# MAGIC
# MAGIC # Run Genie-generated transformation
# MAGIC df_result = genie_transformation(df_test)
# MAGIC
# MAGIC # Validate results
# MAGIC df_result.display()
# MAGIC ```
# MAGIC
# MAGIC ### **Step 2: Validate Schema**
# MAGIC ```python
# MAGIC # Check output schema matches expectations
# MAGIC expected_columns = ['col1', 'col2', 'col3']
# MAGIC assert set(df_result.columns) == set(expected_columns)
# MAGIC ```
# MAGIC
# MAGIC ### **Step 3: Check Data Quality**
# MAGIC ```python
# MAGIC # Validate no nulls in key columns
# MAGIC assert df_result.filter(col("key_column").isNull()).count() == 0
# MAGIC
# MAGIC # Validate value ranges
# MAGIC assert df_result.filter(col("amount") < 0).count() == 0
# MAGIC ```
# MAGIC
# MAGIC ### **Step 4: Performance Test**
# MAGIC ```python
# MAGIC # Check query plan
# MAGIC df_result.explain("formatted")
# MAGIC
# MAGIC # Verify partition pruning
# MAGIC df_result.filter(col("date") == "2024-01-01").explain()
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Iterative Refinement Process:
# MAGIC
# MAGIC ```
# MAGIC 1. Generate code with Genie
# MAGIC    ↓
# MAGIC 2. Review & validate
# MAGIC    ↓
# MAGIC 3. Test with sample data
# MAGIC    ↓
# MAGIC 4. Identify issues
# MAGIC    ↓
# MAGIC 5. Refine prompt OR manually fix
# MAGIC    ↓
# MAGIC 6. Re-test
# MAGIC    ↓
# MAGIC 7. Deploy to production
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 When to Trust vs Refine:
# MAGIC
# MAGIC ### **Trust Genie Code When:**
# MAGIC * ✅ Standard ETL patterns (Bronze → Silver → Gold)
# MAGIC * ✅ Simple aggregations and joins
# MAGIC * ✅ Schema operations (add/drop columns)
# MAGIC * ✅ Basic data quality checks
# MAGIC
# MAGIC ### **Manually Refine When:**
# MAGIC * ⚠️ Complex business logic requiring domain expertise
# MAGIC * ⚠️ Performance-critical operations
# MAGIC * ⚠️ Custom algorithms
# MAGIC * ⚠️ Security/compliance requirements

# COMMAND ----------

# DBTITLE 1,Section 6 - Metadata-Driven Automation
# MAGIC %md
# MAGIC # 📊 Section 6: Metadata-Driven Automation with Genie
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌟 ELI5:
# MAGIC
# MAGIC **Metadata** = Information about your data (like a label on a box)
# MAGIC
# MAGIC Genie reads these labels to understand:
# MAGIC * What kind of data is in each table
# MAGIC * How tables connect to each other
# MAGIC * What rules apply to the data
# MAGIC
# MAGIC Then Genie uses this information to write better code automatically!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Metadata-Driven Development** leverages Unity Catalog metadata to automate:
# MAGIC
# MAGIC ### **1. Schema Discovery**
# MAGIC * Genie reads table schemas from Unity Catalog
# MAGIC * Automatically detects column types, constraints, nullability
# MAGIC * Infers relationships from foreign keys and table properties
# MAGIC
# MAGIC ### **2. Lineage-Aware Generation**
# MAGIC * Understands upstream/downstream dependencies
# MAGIC * Suggests optimal transformation sequences
# MAGIC * Maintains data lineage in generated code
# MAGIC
# MAGIC ### **3. Governance-Compliant Code**
# MAGIC * Respects row-level security policies
# MAGIC * Honors column masking rules
# MAGIC * Maintains audit trail requirements
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔑 Unity Catalog Metadata Used by Genie:
# MAGIC
# MAGIC | Metadata Type | What Genie Learns | How It Helps |
# MAGIC |---------------|-------------------|---------------|
# MAGIC | **Table Schema** | Column names, types, nullability | Generates correct type conversions |
# MAGIC | **Table Properties** | Owner, tags, description | Understands business context |
# MAGIC | **Column Comments** | Business meaning | Applies correct transformations |
# MAGIC | **Relationships** | Foreign keys, references | Suggests optimal joins |
# MAGIC | **Lineage** | Upstream sources | Traces data flow |
# MAGIC | **Statistics** | Row counts, data distribution | Optimizes query plans |
# MAGIC | **Access Policies** | Permissions, masks | Generates secure code |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Example: Metadata-Driven Pipeline Generation
# MAGIC
# MAGIC ### **Scenario:**
# MAGIC Unity Catalog has these tables with metadata:
# MAGIC
# MAGIC ```sql
# MAGIC -- Table: main.bronze.customers
# MAGIC -- Properties: 
# MAGIC --   source: 'CRM System'
# MAGIC --   primary_key: 'customer_id'
# MAGIC --   description: 'Raw customer data from Salesforce'
# MAGIC
# MAGIC -- Table: main.bronze.orders  
# MAGIC -- Properties:
# MAGIC --   source: 'E-commerce Platform'
# MAGIC --   foreign_keys: 'customer_id -> customers.customer_id'
# MAGIC --   description: 'Order transactions'
# MAGIC ```
# MAGIC
# MAGIC ### **Genie Prompt:**
# MAGIC ```
# MAGIC "Create a Silver layer that joins customers and orders, 
# MAGIC validates the relationship, and calculates customer lifetime value"
# MAGIC ```
# MAGIC
# MAGIC ### **Genie's Metadata Analysis:**
# MAGIC 1. Detects `customer_id` is the join key (from foreign key metadata)
# MAGIC 2. Knows it's a many-to-one relationship
# MAGIC 3. Understands this is combining CRM + E-commerce data
# MAGIC 4. Applies appropriate join strategy (broadcast join for dimension)
# MAGIC
# MAGIC ### **Generated Code:**
# MAGIC ```python
# MAGIC # Genie automatically:
# MAGIC # - Uses correct join key
# MAGIC # - Validates referential integrity  
# MAGIC # - Chooses optimal join type
# MAGIC # - Adds data lineage tags
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Metadata Enrichment Strategies:
# MAGIC
# MAGIC ### **Strategy 1: Descriptive Table Properties**
# MAGIC ```sql
# MAGIC ALTER TABLE main.bronze.customers
# MAGIC SET TBLPROPERTIES (
# MAGIC   'business_domain' = 'customer_management',
# MAGIC   'data_classification' = 'PII',
# MAGIC   'refresh_frequency' = 'daily',
# MAGIC   'data_owner' = 'crm_team@company.com',
# MAGIC   'quality_sla' = '99.5%'
# MAGIC );
# MAGIC ```
# MAGIC **Genie Benefit:** Understands data sensitivity, applies appropriate handling
# MAGIC
# MAGIC ### **Strategy 2: Column-Level Comments**
# MAGIC ```sql
# MAGIC COMMENT ON COLUMN main.bronze.customers.email IS 
# MAGIC   'Customer email address - must match regex ^[\w.-]+@[\w.-]+\.[a-z]{2,}$';
# MAGIC
# MAGIC COMMENT ON COLUMN main.bronze.customers.lifetime_value IS
# MAGIC   'Total revenue from customer in USD - range 0 to 1000000';
# MAGIC ```
# MAGIC **Genie Benefit:** Auto-generates validation rules
# MAGIC
# MAGIC ### **Strategy 3: Tagged Columns**
# MAGIC ```sql
# MAGIC ALTER TABLE main.bronze.customers
# MAGIC ALTER COLUMN ssn SET TAGS ('pii' = 'social_security_number');
# MAGIC
# MAGIC ALTER TABLE main.bronze.customers  
# MAGIC ALTER COLUMN credit_card SET TAGS ('pii' = 'payment_card');
# MAGIC ```
# MAGIC **Genie Benefit:** Automatically applies masking/encryption
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚡ Dynamic Schema Evolution:
# MAGIC
# MAGIC ### **Challenge:**
# MAGIC Source schema changes frequently
# MAGIC
# MAGIC ### **Metadata-Driven Solution:**
# MAGIC ```python
# MAGIC # Genie generates schema-agnostic code:
# MAGIC
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC # Read current schema from Unity Catalog metadata
# MAGIC catalog_schema = spark.catalog.listColumns("main.bronze.customers")
# MAGIC
# MAGIC # Dynamically identify columns by type
# MAGIC string_cols = [c.name for c in catalog_schema if c.dataType == 'string']
# MAGIC Numeric_cols = [c.name for c in catalog_schema if c.dataType in ['int', 'double', 'long']]
# MAGIC
# MAGIC # Apply transformations dynamically
# MAGIC for col_name in string_cols:
# MAGIC     df = df.withColumn(col_name, trim(upper(col(col_name))))
# MAGIC
# MAGIC for col_name in numeric_cols:
# MAGIC     df = df.withColumn(f"{col_name}_is_valid", col(col_name).isNotNull())
# MAGIC ```
# MAGIC
# MAGIC **Benefit:** Pipeline adapts to schema changes automatically
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Metadata Registry Pattern:
# MAGIC
# MAGIC ### **Create Configuration Table:**
# MAGIC ```python
# MAGIC # Store transformation rules in metadata table
# MAGIC metadata_config = spark.createDataFrame([
# MAGIC     ("customers", "email", "validate_email", "^[\w.-]+@[\w.-]+\.[a-z]{2,}$"),
# MAGIC     ("customers", "phone", "validate_phone", "^\d{10}$"),
# MAGIC     ("orders", "amount", "validate_range", "0:1000000"),
# MAGIC     ("orders", "status", "validate_enum", "pending,completed,cancelled")
# MAGIC ], ["table_name", "column_name", "rule_type", "rule_parameter"])
# MAGIC
# MAGIC metadata_config.write.mode("overwrite").saveAsTable("main.metadata.validation_rules")
# MAGIC ```
# MAGIC
# MAGIC ### **Genie Uses Metadata:**
# MAGIC ```
# MAGIC Prompt: "Apply all validation rules from main.metadata.validation_rules to the Silver layer"
# MAGIC
# MAGIC Genie generates: Loop-based validation code that reads rules and applies them dynamically
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Benefits of Metadata-Driven Approach:
# MAGIC
# MAGIC 1. **Reduced Maintenance**: Update metadata, not code
# MAGIC 2. **Consistency**: Same rules applied across all pipelines
# MAGIC 3. **Self-Documenting**: Metadata serves as documentation
# MAGIC 4. **Faster Development**: Genie generates more accurate code
# MAGIC 5. **Better Governance**: Centralized policy enforcement
# MAGIC 6. **Easier Testing**: Validate against metadata specifications
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Advanced: Metadata-Driven Medallion Architecture
# MAGIC
# MAGIC ```
# MAGIC Unity Catalog Metadata
# MAGIC          ↓
# MAGIC    Genie Analysis
# MAGIC          ↓
# MAGIC   ┌───────────────┐
# MAGIC   │ Bronze Layer   │ ← Schema inference from source
# MAGIC   └───────┬───────┘
# MAGIC          ↓
# MAGIC   ┌───────┴───────┐
# MAGIC   │ Silver Layer  │ ← Validation rules from metadata
# MAGIC   └───────┬───────┘
# MAGIC          ↓
# MAGIC   ┌───────┴───────┐
# MAGIC   │ Gold Layer    │ ← Aggregation patterns from metadata
# MAGIC   └───────────────┘
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Demo - Metadata-Driven Code Generation
# ============================================================
# Metadata-Driven Automation Demo
# Author: @TRRaveendra
# ============================================================

print("\u26a1 METADATA-DRIVEN AUTOMATION DEMONSTRATION")
print("="*60)

# Simulated Unity Catalog metadata
print("\n1️⃣ STEP 1: Reading Table Metadata from Unity Catalog")
print("-"*60)

metadata_example = {
    'table_name': 'main.bronze.customers',
    'columns': [
        {'name': 'customer_id', 'type': 'string', 'nullable': False, 'comment': 'Unique customer identifier'},
        {'name': 'email', 'type': 'string', 'nullable': True, 'comment': 'Customer email - must be valid format'},
        {'name': 'phone', 'type': 'string', 'nullable': True, 'comment': 'Phone number - 10 digits'},
        {'name': 'age', 'type': 'int', 'nullable': True, 'comment': 'Customer age - range 18-120'},
        {'name': 'signup_date', 'type': 'string', 'nullable': False, 'comment': 'Account creation date'},
        {'name': 'lifetime_value', 'type': 'double', 'nullable': True, 'comment': 'Total customer revenue'}
    ],
    'properties': {
        'primary_key': 'customer_id',
        'data_classification': 'PII',
        'quality_sla': '99.5%'
    }
}

print(f"✅ Metadata loaded for: {metadata_example['table_name']}")
print(f"✅ Columns detected: {len(metadata_example['columns'])}")
print(f"✅ Primary key: {metadata_example['properties']['primary_key']}")

print("\n2️⃣ STEP 2: Genie Analyzes Metadata")
print("-"*60)

# Genie infers validation rules from column comments
validation_rules = {
    'email': {'type': 'regex', 'pattern': '^[\\w.-]+@[\\w.-]+\\.[a-z]{2,}$'},
    'phone': {'type': 'length', 'value': 10},
    'age': {'type': 'range', 'min': 18, 'max': 120},
    'customer_id': {'type': 'not_null'},
    'signup_date': {'type': 'not_null'}
}

print("✅ Genie automatically inferred validation rules:")
for col, rule in validation_rules.items():
    print(f"   • {col}: {rule}")

print("\n3️⃣ STEP 3: Genie Generates Validation Code")
print("-"*60)
print("""
Generated Code Pattern:

from pyspark.sql.functions import col, regexp_extract, length

df_validated = (df
    # Validate email format
    .withColumn(
        'email_valid',
        col('email').rlike('^[\\w.-]+@[\\w.-]+\\.[a-z]{2,}$')
    )
    
    # Validate phone length
    .withColumn(
        'phone_valid',
        length(col('phone')) == 10
    )
    
    # Validate age range
    .withColumn(
        'age_valid',
        (col('age') >= 18) & (col('age') <= 120)
    )
    
    # Check required fields
    .withColumn(
        'customer_id_valid',
        col('customer_id').isNotNull()
    )
    
    # Overall quality score
    .withColumn(
        'quality_score',
        (col('email_valid').cast('int') + 
         col('phone_valid').cast('int') +
         col('age_valid').cast('int') +
         col('customer_id_valid').cast('int')) / 4.0
    )
)
""")

print("\n4️⃣ STEP 4: Benefits of Metadata-Driven Approach")
print("-"*60)
benefits = [
    "No manual rule definition needed",
    "Consistent validation across pipelines",
    "Self-documenting code (metadata = documentation)",
    "Easy to update (change metadata, not code)",
    "Governance-compliant automatically"
]

for i, benefit in enumerate(benefits, 1):
    print(f"{i}. ✅ {benefit}")

print("\n" + "="*60)
print("💡 KEY INSIGHT: Metadata → Smarter AI → Better Code")
print("="*60)

# COMMAND ----------

# DBTITLE 1,Section 7 - Genie + Medallion Architecture
# MAGIC %md
# MAGIC # 🏆 Section 7: Genie + Medallion Architecture Integration
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌟 ELI5:
# MAGIC
# MAGIC **Medallion Architecture** is like organizing your closet:
# MAGIC * **Bronze** = Dump everything in (raw storage)
# MAGIC * **Silver** = Fold and organize (clean data)
# MAGIC * **Gold** = Ready to wear (analytics-ready)
# MAGIC
# MAGIC **Genie** helps build all three layers automatically!
# MAGIC
# MAGIC You just say: "Create a Medallion pipeline for sales data"
# MAGIC Genie builds: Bronze → Silver → Gold (all layers!)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Genie-Powered Medallion Architecture** provides:
# MAGIC
# MAGIC ### **1. Automated Layer Generation**
# MAGIC * **Bronze**: Schema inference, ingestion patterns, audit columns
# MAGIC * **Silver**: Data quality, deduplication, type conversions, business rules
# MAGIC * **Gold**: Aggregations, dimensional modeling, performance optimization
# MAGIC
# MAGIC ### **2. Intelligent Transformation Logic**
# MAGIC * Genie understands Medallion best practices
# MAGIC * Applies appropriate transformations per layer
# MAGIC * Maintains data lineage across layers
# MAGIC
# MAGIC ### **3. Consistency Across Domains**
# MAGIC * Same architectural patterns for all data domains
# MAGIC * Standardized naming conventions
# MAGIC * Unified quality framework
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Medallion Layer Characteristics:
# MAGIC
# MAGIC | Layer | Purpose | Genie Generates | Data Quality |
# MAGIC |-------|---------|-----------------|---------------|
# MAGIC | **Bronze** | Raw ingestion | Schema-on-read, audit columns | Preserve all source data |
# MAGIC | **Silver** | Cleaned, conformed | Validation, dedup, standardization | 95-99% quality |
# MAGIC | **Gold** | Business aggregates | Dimensions, facts, metrics | 99.9%+ quality |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Complete Medallion Pipeline - Genie Generated:
# MAGIC
# MAGIC ### **Scenario: E-Commerce Sales Pipeline**
# MAGIC
# MAGIC **Prompt to Genie:**
# MAGIC ```
# MAGIC Create a complete Medallion architecture for e-commerce sales:
# MAGIC
# MAGIC Bronze Layer:
# MAGIC - Ingest JSON files from /Volumes/main/raw/sales/
# MAGIC - Include audit columns
# MAGIC - Schema evolution enabled
# MAGIC
# MAGIC Silver Layer:
# MAGIC - Remove duplicate transactions
# MAGIC - Validate amount > 0
# MAGIC - Parse dates to proper format
# MAGIC - Standardize product codes
# MAGIC - Flag invalid records
# MAGIC
# MAGIC Gold Layer:
# MAGIC - Daily sales by product category
# MAGIC - Customer lifetime value
# MAGIC - Monthly revenue trends
# MAGIC - Top 10 products by revenue
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💻 Genie-Generated Bronze Layer:
# MAGIC
# MAGIC ```python
# MAGIC # BRONZE LAYER - Raw Data Ingestion
# MAGIC # Generated by Genie
# MAGIC
# MAGIC from pyspark.sql.functions import current_timestamp, input_file_name
# MAGIC from pyspark.sql.types import *
# MAGIC
# MAGIC # Schema inference from sample data
# MAGIC schema = StructType([
# MAGIC     StructField("transaction_id", StringType(), True),
# MAGIC     StructField("customer_id", StringType(), True),
# MAGIC     StructField("product_id", StringType(), True),
# MAGIC     StructField("product_category", StringType(), True),
# MAGIC     StructField("amount", DoubleType(), True),
# MAGIC     StructField("quantity", IntegerType(), True),
# MAGIC     StructField("transaction_date", StringType(), True)
# MAGIC ])
# MAGIC
# MAGIC df_bronze = (spark.read
# MAGIC     .format("json")
# MAGIC     .schema(schema)
# MAGIC     .option("multiLine", "true")
# MAGIC     .load("/Volumes/main/raw/sales/")
# MAGIC     .withColumn("bronze_ingestion_time", current_timestamp())
# MAGIC     .withColumn("source_file", input_file_name())
# MAGIC     .withColumn("bronze_layer", lit("main.bronze.sales_raw"))
# MAGIC )
# MAGIC
# MAGIC (df_bronze.write
# MAGIC     .format("delta")
# MAGIC     .mode("append")
# MAGIC     .option("mergeSchema", "true")
# MAGIC     .saveAsTable("main.bronze.sales_raw")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💻 Genie-Generated Silver Layer:
# MAGIC
# MAGIC ```python
# MAGIC # SILVER LAYER - Data Quality & Standardization  
# MAGIC # Generated by Genie
# MAGIC
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.window import Window
# MAGIC
# MAGIC df_silver = (spark.table("main.bronze.sales_raw")
# MAGIC     
# MAGIC     # Deduplication
# MAGIC     .withColumn(
# MAGIC         "row_num",
# MAGIC         row_number().over(
# MAGIC             Window.partitionBy("transaction_id")
# MAGIC             .orderBy(col("bronze_ingestion_time").desc())
# MAGIC         )
# MAGIC     )
# MAGIC     .filter(col("row_num") == 1)
# MAGIC     .drop("row_num")
# MAGIC     
# MAGIC     # Data type conversions
# MAGIC     .withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))
# MAGIC     .withColumn("amount", col("amount").cast("decimal(18,2)"))
# MAGIC     
# MAGIC     # Standardization
# MAGIC     .withColumn("product_id", trim(upper(col("product_id"))))
# MAGIC     .withColumn("product_category", trim(lower(col("product_category"))))
# MAGIC     .withColumn("customer_id", trim(upper(col("customer_id"))))
# MAGIC     
# MAGIC     # Validation flags
# MAGIC     .withColumn(
# MAGIC         "is_valid_amount",
# MAGIC         (col("amount").isNotNull()) & (col("amount") > 0)
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "is_valid_quantity",
# MAGIC         (col("quantity").isNotNull()) & (col("quantity") > 0)
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "is_valid_date",
# MAGIC         col("transaction_date").isNotNull()
# MAGIC     )
# MAGIC     .withColumn(
# MAGIC         "is_valid_record",
# MAGIC         col("is_valid_amount") & col("is_valid_quantity") & col("is_valid_date")
# MAGIC     )
# MAGIC     
# MAGIC     # Derived columns
# MAGIC     .withColumn("total_amount", col("amount") * col("quantity"))
# MAGIC     .withColumn("year", year(col("transaction_date")))
# MAGIC     .withColumn("month", month(col("transaction_date")))
# MAGIC     .withColumn("quarter", quarter(col("transaction_date")))
# MAGIC     
# MAGIC     # Audit columns
# MAGIC     .withColumn("silver_processed_time", current_timestamp())
# MAGIC )
# MAGIC
# MAGIC (df_silver.write
# MAGIC     .format("delta")
# MAGIC     .mode("overwrite")
# MAGIC     .partitionBy("year", "month")
# MAGIC     .option("overwriteSchema", "true")
# MAGIC     .saveAsTable("main.silver.sales_validated")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💻 Genie-Generated Gold Layer:
# MAGIC
# MAGIC ```sql
# MAGIC -- GOLD LAYER - Business Aggregates
# MAGIC -- Generated by Genie
# MAGIC
# MAGIC -- Daily Sales Summary
# MAGIC CREATE OR REPLACE TABLE main.gold.daily_sales_summary
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (sale_date)
# MAGIC AS
# MAGIC SELECT
# MAGIC   transaction_date as sale_date,
# MAGIC   product_category,
# MAGIC   COUNT(DISTINCT transaction_id) as transaction_count,
# MAGIC   COUNT(DISTINCT customer_id) as unique_customers,
# MAGIC   SUM(total_amount) as daily_revenue,
# MAGIC   AVG(total_amount) as avg_transaction_value,
# MAGIC   SUM(quantity) as total_units_sold,
# MAGIC   CURRENT_TIMESTAMP() as gold_created_time
# MAGIC FROM main.silver.sales_validated
# MAGIC WHERE is_valid_record = true
# MAGIC GROUP BY transaction_date, product_category;
# MAGIC
# MAGIC -- Customer Lifetime Value
# MAGIC CREATE OR REPLACE TABLE main.gold.customer_lifetime_value
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   COUNT(DISTINCT transaction_id) as total_transactions,
# MAGIC   SUM(total_amount) as lifetime_value,
# MAGIC   AVG(total_amount) as avg_order_value,
# MAGIC   MIN(transaction_date) as first_purchase_date,
# MAGIC   MAX(transaction_date) as last_purchase_date,
# MAGIC   DATEDIFF(MAX(transaction_date), MIN(transaction_date)) as customer_tenure_days,
# MAGIC   CURRENT_TIMESTAMP() as gold_created_time
# MAGIC FROM main.silver.sales_validated  
# MAGIC WHERE is_valid_record = true
# MAGIC GROUP BY customer_id;
# MAGIC
# MAGIC -- Top Products by Revenue
# MAGIC CREATE OR REPLACE TABLE main.gold.top_products
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   product_id,
# MAGIC   product_category,
# MAGIC   SUM(total_amount) as total_revenue,
# MAGIC   SUM(quantity) as total_units_sold,
# MAGIC   COUNT(DISTINCT customer_id) as unique_customers,
# MAGIC   AVG(amount) as avg_unit_price,
# MAGIC   CURRENT_TIMESTAMP() as gold_created_time
# MAGIC FROM main.silver.sales_validated
# MAGIC WHERE is_valid_record = true
# MAGIC GROUP BY product_id, product_category
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Complete Data Flow:
# MAGIC
# MAGIC ```
# MAGIC 📁 Raw JSON Files (/Volumes/main/raw/sales/)
# MAGIC          ↓
# MAGIC   🟫Bronze Layer (main.bronze.sales_raw)
# MAGIC     • Schema-on-read
# MAGIC     • Audit columns added
# MAGIC     • All data preserved
# MAGIC          ↓
# MAGIC   🧠 AI Transformation (Genie)
# MAGIC          ↓
# MAGIC   🟦 Silver Layer (main.silver.sales_validated)
# MAGIC     • Deduplicated
# MAGIC     • Validated
# MAGIC     • Standardized
# MAGIC     • Partitioned by date
# MAGIC          ↓
# MAGIC   🧠 AI Aggregation (Genie)
# MAGIC          ↓
# MAGIC   🏆 Gold Layer (main.gold.*)
# MAGIC     • daily_sales_summary
# MAGIC     • customer_lifetime_value
# MAGIC     • top_products
# MAGIC          ↓
# MAGIC   📊 Analytics & BI Tools
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚡ Genie's Architectural Decisions:
# MAGIC
# MAGIC 1. **Bronze**: Append mode, schema evolution, audit columns
# MAGIC 2. **Silver**: Overwrite mode, partitioning by date, validation flags
# MAGIC 3. **Gold**: Separate tables per business question, optimized for queries
# MAGIC 4. **Consistency**: Same patterns across all domains
# MAGIC 5. **Performance**: Strategic partitioning at each layer
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 One-Prompt Medallion Generation:
# MAGIC
# MAGIC **The Power of AI:**
# MAGIC
# MAGIC Instead of:
# MAGIC * Writing 500+ lines of code manually
# MAGIC * Designing schema for each layer
# MAGIC * Implementing data quality checks
# MAGIC * Creating aggregation logic
# MAGIC * Testing and debugging
# MAGIC
# MAGIC You do:
# MAGIC * Write ONE comprehensive prompt
# MAGIC * Review generated code
# MAGIC * Execute pipeline
# MAGIC * Iterate if needed
# MAGIC
# MAGIC **Time saved: Days → Hours**

# COMMAND ----------

# DBTITLE 1,Section 8 - Genie + Unity Catalog Governance
# MAGIC %md
# MAGIC # 🛡️ Section 8: Genie + Unity Catalog Governance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌟 ELI5:
# MAGIC
# MAGIC **Governance** = Rules about who can see and use data
# MAGIC
# MAGIC Genie is like a security guard that:
# MAGIC * Only shows you data you're allowed to see
# MAGIC * Follows all the company rules automatically
# MAGIC * Keeps track of who did what
# MAGIC * Protects private information
# MAGIC
# MAGIC Even when Genie writes code for you, it makes sure the code is safe and follows the rules!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Genie + Unity Catalog Governance** ensures:
# MAGIC
# MAGIC ### **1. Permission-Aware Code Generation**
# MAGIC * Genie only generates code accessing tables you have permissions for
# MAGIC * Respects table, schema, and catalog-level ACLs
# MAGIC * Honors row filters and column masks
# MAGIC
# MAGIC ### **2. Audit Trail Maintenance**
# MAGIC * Every Genie-generated query is logged
# MAGIC * Lineage is automatically tracked
# MAGIC * Changes are attributed to users
# MAGIC
# MAGIC ### **3. Data Classification Compliance**
# MAGIC * Recognizes PII, PHI, PCI data
# MAGIC * Applies appropriate handling (encryption, masking)
# MAGIC * Enforces retention policies
# MAGIC
# MAGIC ### **4. Policy Enforcement**
# MAGIC * Row-level security automatically applied
# MAGIC * Column masking in generated queries
# MAGIC * Attribute-based access control (ABAC)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔒 Governance Features in Genie-Generated Code:
# MAGIC
# MAGIC | Governance Aspect | How Genie Handles It | Benefit |
# MAGIC |-------------------|----------------------|---------|
# MAGIC | **Access Control** | Only suggests accessible tables | Prevents permission errors |
# MAGIC | **Row Filters** | Automatically includes filters | Transparent security |
# MAGIC | **Column Masking** | Masks sensitive columns in output | PII protection |
# MAGIC | **Audit Logging** | Tags queries with metadata | Compliance tracking |
# MAGIC | **Data Lineage** | Maintains upstream/downstream links | Impact analysis |
# MAGIC | **Tags & Labels** | Reads and applies governance tags | Policy enforcement |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Example: Permission-Aware Code Generation
# MAGIC
# MAGIC ### **Scenario:**
# MAGIC User has:
# MAGIC * ✅ READ access to `main.silver.sales`
# MAGIC * ❌ NO access to `main.silver.customers_pii`
# MAGIC
# MAGIC ### **Prompt:**
# MAGIC ```
# MAGIC "Show me customer purchase history with email addresses"
# MAGIC ```
# MAGIC
# MAGIC ### **Genie Response:**
# MAGIC ```
# MAGIC ⚠️ I can show you customer purchase history from main.silver.sales,
# MAGIC but I don't have access to email addresses (they're in 
# MAGIC main.silver.customers_pii which you don't have permission to access).
# MAGIC
# MAGIC Would you like me to:
# MAGIC 1. Show purchase history without email addresses?
# MAGIC 2. Request access to the customers_pii table?
# MAGIC 3. Use a de-identified customer lookup table?
# MAGIC ```
# MAGIC
# MAGIC **Key Feature:** Genie proactively identifies permission issues
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Row-Level Security Example:
# MAGIC
# MAGIC ### **Scenario: Multi-Tenant SaaS Platform**
# MAGIC
# MAGIC **Unity Catalog Policy:**
# MAGIC ```sql
# MAGIC -- Create row filter function
# MAGIC CREATE FUNCTION main.security.tenant_filter(tenant_id STRING)
# MAGIC RETURN current_user() = tenant_id;
# MAGIC
# MAGIC -- Apply to table
# MAGIC ALTER TABLE main.silver.application_logs
# MAGIC SET ROW FILTER main.security.tenant_filter ON (tenant_id);
# MAGIC ```
# MAGIC
# MAGIC ### **User Prompt to Genie:**
# MAGIC ```
# MAGIC "Show me all application errors from the logs"
# MAGIC ```
# MAGIC
# MAGIC ### **Genie-Generated Query:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   timestamp,
# MAGIC   error_message,
# MAGIC   error_code,
# MAGIC   tenant_id
# MAGIC FROM main.silver.application_logs
# MAGIC WHERE error_code IS NOT NULL
# MAGIC -- Row filter automatically applied by Unity Catalog
# MAGIC -- User only sees their tenant's data
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 100;
# MAGIC ```
# MAGIC
# MAGIC **Result:** User only sees errors for their tenant, but the code is simple!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔐 Column Masking Example:
# MAGIC
# MAGIC ### **Unity Catalog Mask:**
# MAGIC ```sql
# MAGIC -- Create masking function  
# MAGIC CREATE FUNCTION main.security.mask_email(email STRING)
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('pii_readers') THEN email
# MAGIC   ELSE regexp_replace(email, '^(.{2}).*(@.*)', '$1***$2')
# MAGIC END;
# MAGIC
# MAGIC -- Apply to column
# MAGIC ALTER TABLE main.silver.customers
# MAGIC ALTER COLUMN email SET MASK main.security.mask_email;
# MAGIC ```
# MAGIC
# MAGIC ### **Genie-Generated Query:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   email,  -- Automatically masked based on user group
# MAGIC   total_purchases
# MAGIC FROM main.silver.customers
# MAGIC WHERE total_purchases > 1000;
# MAGIC ```
# MAGIC
# MAGIC **Output for regular user:**
# MAGIC ```
# MAGIC customer_id | email              | total_purchases
# MAGIC -----------|--------------------|-----------------
# MAGIC C001       | jo***@example.com  | 1500
# MAGIC C002       | ma***@company.com  | 2300
# MAGIC ```
# MAGIC
# MAGIC **Output for PII reader:**
# MAGIC ```
# MAGIC customer_id | email              | total_purchases
# MAGIC -----------|--------------------|-----------------
# MAGIC C001       | john@example.com   | 1500
# MAGIC C002       | mary@company.com   | 2300
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Audit Trail in Genie:
# MAGIC
# MAGIC ### **What Gets Logged:**
# MAGIC
# MAGIC ```python
# MAGIC # Every Genie interaction logs:
# MAGIC {
# MAGIC   "user": "data_engineer@company.com",
# MAGIC   "timestamp": "2026-04-21T23:30:00Z",
# MAGIC   "prompt": "Create Silver layer for customer data",
# MAGIC   "tables_accessed": ["main.bronze.customers"],
# MAGIC   "tables_created": ["main.silver.customers_validated"],
# MAGIC   "operation_type": "ETL_GENERATION",
# MAGIC   "governance_checks": [
# MAGIC     "permission_verified",
# MAGIC     "row_filters_applied",
# MAGIC     "column_masks_applied"
# MAGIC   ],
# MAGIC   "execution_status": "success",
# MAGIC   "query_id": "01234567-89ab-cdef-0123-456789abcdef"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### **Audit Query:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   user,
# MAGIC   timestamp,
# MAGIC   operation_type,
# MAGIC   tables_accessed
# MAGIC FROM system.access.audit
# MAGIC WHERE service_name = 'databricks-genie'
# MAGIC   AND timestamp > current_date() - INTERVAL 7 DAYS
# MAGIC ORDER BY timestamp DESC;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏷️ Governance Tags Usage:
# MAGIC
# MAGIC ### **Setting Tags:**
# MAGIC ```sql
# MAGIC -- Tag sensitive columns
# MAGIC ALTER TABLE main.silver.customers 
# MAGIC ALTER COLUMN ssn SET TAGS ('classification' = 'highly_confidential');
# MAGIC
# MAGIC ALTER TABLE main.silver.customers
# MAGIC ALTER COLUMN email SET TAGS ('classification' = 'pii');
# MAGIC
# MAGIC ALTER TABLE main.silver.customers  
# MAGIC ALTER COLUMN address SET TAGS ('classification' = 'pii');
# MAGIC ```
# MAGIC
# MAGIC ### **Genie Behavior:**
# MAGIC
# MAGIC **Prompt:** "Create analytics query on customer table"
# MAGIC
# MAGIC **Genie Analysis:**
# MAGIC 1. Detects `ssn` has tag `highly_confidential`
# MAGIC 2. Detects `email` and `address` have tag `pii`
# MAGIC 3. Automatically excludes highly_confidential columns
# MAGIC 4. Applies masking to PII columns
# MAGIC
# MAGIC **Generated Code:**
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   -- ssn excluded (highly_confidential)
# MAGIC   email,  -- masked automatically
# MAGIC   address,  -- masked automatically  
# MAGIC   total_purchases,
# MAGIC   account_status
# MAGIC FROM main.silver.customers;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚡ Data Lineage Tracking:
# MAGIC
# MAGIC ### **Automatic Lineage:**
# MAGIC
# MAGIC ```
# MAGIC 📁 Source: /Volumes/main/raw/customers/
# MAGIC      ↓
# MAGIC 🟩 main.bronze.customers_raw
# MAGIC      ↓ (Genie-generated transformation)
# MAGIC 🟦 main.silver.customers_validated  
# MAGIC      ↓ (Genie-generated aggregation)
# MAGIC 🏆 main.gold.customer_segments
# MAGIC      ↓
# MAGIC 📊 Analytics Dashboard
# MAGIC ```
# MAGIC
# MAGIC **Query Lineage:**
# MAGIC ```sql
# MAGIC -- View lineage for a table
# MAGIC SELECT *
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE target_table_full_name = 'main.gold.customer_segments';
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Best Practices: Governance + Genie:
# MAGIC
# MAGIC ### **1. Trust but Verify**
# MAGIC ✅ Review Genie-generated code for compliance
# MAGIC ✅ Validate that policies are correctly applied
# MAGIC ✅ Test with different user roles
# MAGIC
# MAGIC ### **2. Metadata-Driven Security**
# MAGIC ✅ Use tags to classify data
# MAGIC ✅ Create reusable masking functions
# MAGIC ✅ Document security requirements in table properties
# MAGIC
# MAGIC ### **3. Least Privilege Principle**
# MAGIC ✅ Grant minimum required permissions
# MAGIC ✅ Use row filters for multi-tenant isolation
# MAGIC ✅ Apply column masks for sensitive data
# MAGIC
# MAGIC ### **4. Audit Everything**
# MAGIC ✅ Enable audit logging
# MAGIC ✅ Regular review of Genie-generated queries
# MAGIC ✅ Monitor for unauthorized access attempts
# MAGIC
# MAGIC ### **5. Continuous Compliance**
# MAGIC ✅ Automate compliance checks
# MAGIC ✅ Regular permission audits
# MAGIC ✅ Update policies as requirements change
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Key Takeaway:
# MAGIC
# MAGIC **Genie doesn't bypass governance — it enforces it!**
# MAGIC
# MAGIC Every query, every transformation, every access is:
# MAGIC * Permission-checked
# MAGIC * Policy-enforced
# MAGIC * Audit-logged
# MAGIC * Lineage-tracked
# MAGIC
# MAGIC **Result:** Secure, compliant, AI-assisted data engineering

# COMMAND ----------

# DBTITLE 1,Section 9 - Hands-On: Prompt Engineering Workshop
# MAGIC %md
# MAGIC # 🎯 Section 9: Hands-On Prompt Engineering Workshop
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌟 Learning Objectives:
# MAGIC
# MAGIC In this section, you'll practice:
# MAGIC 1. Writing effective prompts for Genie
# MAGIC 2. Iterating and refining prompts
# MAGIC 3. Handling complex scenarios
# MAGIC 4. Combining multiple requirements
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Exercise 1: Basic Data Query
# MAGIC
# MAGIC ### **Scenario:**
# MAGIC You need to analyze sales data to find top-performing products.
# MAGIC
# MAGIC ### **❌ Poor Prompt:**
# MAGIC ```
# MAGIC "sales data"
# MAGIC ```
# MAGIC **Problem:** Too vague, no context, no specific request
# MAGIC
# MAGIC ### **⚠️ Better Prompt:**
# MAGIC ```
# MAGIC "Show me top products"
# MAGIC ```
# MAGIC **Problem:** Missing table name, time period, definition of "top"
# MAGIC
# MAGIC ### **✅ Excellent Prompt:**
# MAGIC ```
# MAGIC "Show me the top 10 products by total revenue from main.gold.sales_summary 
# MAGIC for the last 30 days, including product name, category, total revenue, 
# MAGIC and number of transactions. Order by revenue descending."
# MAGIC ```
# MAGIC **Why it works:** Specific, complete, unambiguous
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Exercise 2: ETL Pipeline Creation
# MAGIC
# MAGIC ### **Scenario:**
# MAGIC Build a pipeline to process customer data.
# MAGIC
# MAGIC ### **❌ Poor Prompt:**
# MAGIC ```
# MAGIC "Create pipeline"
# MAGIC ```
# MAGIC
# MAGIC ### **⚠️ Better Prompt:**
# MAGIC ```
# MAGIC "Create a pipeline for customer data from Bronze to Silver"
# MAGIC ```
# MAGIC
# MAGIC ### **✅ Excellent Prompt:**
# MAGIC ```
# MAGIC Create a Silver layer transformation pipeline:
# MAGIC
# MAGIC Source:
# MAGIC - Table: main.bronze.customer_events
# MAGIC - Format: Delta
# MAGIC
# MAGIC Transformations:
# MAGIC 1. Remove duplicates based on customer_id and event_timestamp (keep latest)
# MAGIC 2. Validate email format (regex: ^[\w.-]+@[\w.-]+\.[a-z]{2,}$)
# MAGIC 3. Convert signup_date from string to date type
# MAGIC 4. Standardize country codes to ISO 3166-1 alpha-2
# MAGIC 5. Calculate customer_age from date_of_birth
# MAGIC 6. Add is_valid flag (true if all validations pass)
# MAGIC
# MAGIC Output:
# MAGIC - Table: main.silver.customers_validated
# MAGIC - Format: Delta
# MAGIC - Partition by: signup_year, signup_month
# MAGIC - Mode: Overwrite with schema merge
# MAGIC
# MAGIC Include:
# MAGIC - Processing timestamp
# MAGIC - Data quality metrics
# MAGIC - Error logging to main.quality.validation_errors
# MAGIC ```
# MAGIC
# MAGIC **Why it works:** 
# MAGIC * Clear source and destination
# MAGIC * Specific transformations with details
# MAGIC * Validation rules defined
# MAGIC * Output specifications complete
# MAGIC * Quality requirements included
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Exercise 3: Complex Aggregation
# MAGIC
# MAGIC ### **Scenario:**
# MAGIC Create analytical views with window functions.
# MAGIC
# MAGIC ### **✅ Excellent Prompt:**
# MAGIC ```
# MAGIC Create a Gold layer table for revenue analysis:
# MAGIC
# MAGIC Source: main.silver.sales_validated
# MAGIC
# MAGIC Calculations:
# MAGIC 1. Daily revenue by product category
# MAGIC 2. 7-day moving average of revenue per category
# MAGIC 3. Month-over-month growth rate (%)
# MAGIC 4. Running total of revenue per category
# MAGIC 5. Rank products within each category by revenue
# MAGIC
# MAGIC Filters:
# MAGIC - Only include valid transactions (is_valid = true)
# MAGIC - Only include completed orders (status = 'completed')
# MAGIC - Date range: last 365 days
# MAGIC
# MAGIC Output:
# MAGIC - Table: main.gold.revenue_analytics
# MAGIC - Partition by: year, month
# MAGIC - Include: category, date, daily_revenue, moving_avg_7d, mom_growth_pct, 
# MAGIC   cumulative_revenue, category_rank
# MAGIC
# MAGIC Optimization:
# MAGIC - Enable Z-ordering on category and date
# MAGIC - Add table properties for documentation
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Exercise 4: Data Quality Framework
# MAGIC
# MAGIC ### **✅ Excellent Prompt:**
# MAGIC ```
# MAGIC Create a comprehensive data quality validation framework:
# MAGIC
# MAGIC Target Table: main.silver.orders
# MAGIC
# MAGIC Validation Rules:
# MAGIC 1. NOT NULL checks:
# MAGIC    - order_id, customer_id, order_date, total_amount
# MAGIC
# MAGIC 2. Range checks:
# MAGIC    - total_amount: 0 to 1,000,000
# MAGIC    - quantity: 1 to 1,000
# MAGIC    - discount_percent: 0 to 100
# MAGIC
# MAGIC 3. Format checks:
# MAGIC    - order_id: pattern ORD-[0-9]{8}
# MAGIC    - customer_id: pattern CUST-[A-Z0-9]{6}
# MAGIC
# MAGIC 4. Referential integrity:
# MAGIC    - customer_id exists in main.silver.customers
# MAGIC    - product_id exists in main.silver.products
# MAGIC
# MAGIC 5. Business rules:
# MAGIC    - order_date <= current_date
# MAGIC    - order_date >= account_creation_date
# MAGIC    - discount_amount <= total_amount
# MAGIC
# MAGIC Error Handling:
# MAGIC - Log failed records to main.quality.order_validation_errors
# MAGIC - Include: record_id, rule_failed, error_message, timestamp
# MAGIC - Continue processing valid records
# MAGIC
# MAGIC Output:
# MAGIC - Add validation_status column (PASS/FAIL)
# MAGIC - Add validation_timestamp
# MAGIC - Calculate quality score (0-100) based on passed rules
# MAGIC
# MAGIC Metrics:
# MAGIC - Generate summary report:
# MAGIC   - Total records processed
# MAGIC   - Records passed/failed per rule
# MAGIC   - Overall quality percentage
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Exercise 5: Incremental Processing
# MAGIC
# MAGIC ### **✅ Excellent Prompt:**
# MAGIC ```
# MAGIC Create an incremental ETL pipeline for streaming data:
# MAGIC
# MAGIC Source:
# MAGIC - Table: main.bronze.event_stream
# MAGIC - Processing: Incremental (not full refresh)
# MAGIC - Watermark column: event_timestamp
# MAGIC
# MAGIC Logic:
# MAGIC 1. Read only new records since last successful run
# MAGIC 2. Use checkpoint location: /Volumes/main/checkpoints/events/
# MAGIC 3. Handle late-arriving data (7-day watermark)
# MAGIC
# MAGIC Transformations:
# MAGIC 1. Parse JSON payload column
# MAGIC 2. Deduplicate on event_id within 1-hour window
# MAGIC 3. Enrich with customer data from main.silver.customers
# MAGIC 4. Calculate session_id using 30-minute inactivity timeout
# MAGIC 5. Aggregate events per session
# MAGIC
# MAGIC Output:
# MAGIC - Table: main.silver.user_sessions
# MAGIC - Mode: Append
# MAGIC - Trigger: Once (for batch) or continuous (for streaming)
# MAGIC - Include: session_id, customer_id, session_start, session_end, 
# MAGIC   event_count, session_duration_minutes
# MAGIC
# MAGIC Monitoring:
# MAGIC - Track processing lag
# MAGIC - Alert if watermark exceeds 1 hour
# MAGIC - Log throughput metrics
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚡ Prompt Templates for Common Scenarios:
# MAGIC
# MAGIC ### **Template 1: Simple Query**
# MAGIC ```
# MAGIC "[ACTION] [SPECIFIC_METRIC] from [TABLE] 
# MAGIC where [CONDITIONS] 
# MAGIC group by [DIMENSIONS] 
# MAGIC order by [SORT_COLUMN] [ASC/DESC] 
# MAGIC limit [N]"
# MAGIC ```
# MAGIC
# MAGIC ### **Template 2: ETL Pipeline**
# MAGIC ```
# MAGIC "Create [LAYER] layer transformation:
# MAGIC
# MAGIC Source: [TABLE_PATH]
# MAGIC Transformations:
# MAGIC 1. [TRANSFORMATION_1]
# MAGIC 2. [TRANSFORMATION_2]
# MAGIC ...
# MAGIC
# MAGIC Output: [TABLE_PATH]
# MAGIC Format: [FORMAT]
# MAGIC Partition: [COLUMNS]
# MAGIC Mode: [WRITE_MODE]
# MAGIC "
# MAGIC ```
# MAGIC
# MAGIC ### **Template 3: Data Quality**
# MAGIC ```
# MAGIC "Add data quality validation to [TABLE]:
# MAGIC
# MAGIC Validation Rules:
# MAGIC - [COLUMN]: [RULE_TYPE] [CRITERIA]
# MAGIC ...
# MAGIC
# MAGIC Error Handling: [STRATEGY]
# MAGIC Output: [QUALITY_FLAGS/ERROR_TABLE]
# MAGIC "
# MAGIC ```
# MAGIC
# MAGIC ### **Template 4: Aggregation**
# MAGIC ```
# MAGIC "Create [LEVEL] aggregation from [SOURCE]:
# MAGIC
# MAGIC Metrics:
# MAGIC - [METRIC_1]: [CALCULATION]
# MAGIC - [METRIC_2]: [CALCULATION]
# MAGIC ...
# MAGIC
# MAGIC Dimensions: [DIM1, DIM2, ...]
# MAGIC Filters: [CONDITIONS]
# MAGIC Window Functions: [IF_NEEDED]
# MAGIC
# MAGIC Output: [TABLE]
# MAGIC Partitioning: [STRATEGY]
# MAGIC "
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Tips for Advanced Prompt Engineering:
# MAGIC
# MAGIC ### **1. Use Examples**
# MAGIC ```
# MAGIC Prompt: "Transform customer names to title case.
# MAGIC Example: 'john DOE' -> 'John Doe'
# MAGIC          'MARY smith' -> 'Mary Smith'"
# MAGIC ```
# MAGIC
# MAGIC ### **2. Specify Edge Cases**
# MAGIC ```
# MAGIC Prompt: "Calculate age from date_of_birth.
# MAGIC Handle:
# MAGIC - Future dates (set to NULL)
# MAGIC - Missing values (set to NULL)
# MAGIC - Ages > 120 (flag as invalid)"
# MAGIC ```
# MAGIC
# MAGIC ### **3. Provide Context**
# MAGIC ```
# MAGIC Prompt: "Join customers and orders.
# MAGIC Note: This is a many-to-one relationship where one customer can have multiple orders.
# MAGIC Use broadcast join since customers table is small (~10K rows)."
# MAGIC ```
# MAGIC
# MAGIC ### **4. Reference Standards**
# MAGIC ```
# MAGIC Prompt: "Validate phone numbers according to E.164 format.
# MAGIC Format: +[country code][subscriber number]
# MAGIC Example: +14155552671"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Practice Challenges:
# MAGIC
# MAGIC ### **Challenge 1: Write a prompt to:**
# MAGIC Create a customer segmentation pipeline using RFM (Recency, Frequency, Monetary) analysis
# MAGIC
# MAGIC ### **Challenge 2: Write a prompt to:**
# MAGIC Build a slowly changing dimension (SCD Type 2) for product information
# MAGIC
# MAGIC ### **Challenge 3: Write a prompt to:**
# MAGIC Implement a data reconciliation framework comparing source vs target record counts
# MAGIC
# MAGIC ### **Challenge 4: Write a prompt to:**
# MAGIC Create a real-time anomaly detection pipeline for transaction amounts
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Key Takeaways:
# MAGIC
# MAGIC 1. **Be Specific**: More details = better code
# MAGIC 2. **Provide Context**: Help Genie understand your domain
# MAGIC 3. **Define Constraints**: Specify performance, security requirements
# MAGIC 4. **Include Examples**: Show expected input/output
# MAGIC 5. **Iterate**: Refine prompts based on results
# MAGIC 6. **Validate**: Always review generated code

# COMMAND ----------

# DBTITLE 1,Section 10 - End-to-End AI Data Engineering Flow
# MAGIC %md
# MAGIC # 🚀 Section 10: End-to-End AI Data Engineering Workflow
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌟 Complete Workflow: From Prompt to Production
# MAGIC
# MAGIC ```
# MAGIC 📝 STEP 1: Define Requirements
# MAGIC         ↓
# MAGIC 🧠 STEP 2: Write Genie Prompt
# MAGIC         ↓
# MAGIC ⚙️ STEP 3: Genie Generates Code
# MAGIC         ↓
# MAGIC 🔍 STEP 4: Review & Validate
# MAGIC         ↓
# MAGIC 🧪 STEP 5: Test with Sample Data
# MAGIC         ↓
# MAGIC ⚙️ STEP 6: Refine (if needed)
# MAGIC         ↓
# MAGIC 🚀 STEP 7: Execute Full Pipeline
# MAGIC         ↓
# MAGIC 📊 STEP 8: Monitor & Optimize
# MAGIC         ↓
# MAGIC 📚 STEP 9: Document & Share
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Real-World Example: Customer Analytics Pipeline
# MAGIC
# MAGIC ### **STEP 1: Define Requirements**
# MAGIC
# MAGIC **Business Need:**
# MAGIC * Analyze customer behavior for marketing campaigns
# MAGIC * Identify high-value customers
# MAGIC * Track customer lifecycle metrics
# MAGIC
# MAGIC **Technical Requirements:**
# MAGIC * Process daily customer event data
# MAGIC * Create customer segments
# MAGIC * Generate analytics-ready views
# MAGIC * Ensure data quality >99%
# MAGIC * Maintain GDPR compliance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **STEP 2: Write Comprehensive Prompt**
# MAGIC
# MAGIC ```
# MAGIC Prompt to Genie:
# MAGIC
# MAGIC "Create a complete Medallion pipeline for customer analytics:
# MAGIC
# MAGIC BRONZE LAYER:
# MAGIC - Source: /Volumes/main/landing/customer_events/*.json
# MAGIC - Schema: event_id, customer_id, event_type, event_timestamp, event_data (JSON)
# MAGIC - Add: ingestion_time, source_file
# MAGIC - Table: main.bronze.customer_events_raw
# MAGIC - Mode: Append with schema evolution
# MAGIC
# MAGIC SILVER LAYER:
# MAGIC - Source: main.bronze.customer_events_raw
# MAGIC - Transformations:
# MAGIC   1. Parse event_data JSON column
# MAGIC   2. Deduplicate on event_id (keep latest by ingestion_time)
# MAGIC   3. Validate customer_id exists in main.reference.customers
# MAGIC   4. Standardize event_type (lowercase, trim)
# MAGIC   5. Convert event_timestamp to timestamp type with UTC timezone
# MAGIC   6. Add derived columns: event_date, event_hour, day_of_week
# MAGIC   7. Flag invalid records (is_valid column)
# MAGIC - Data Quality:
# MAGIC   - customer_id: NOT NULL, matches pattern CUST-[0-9]{8}
# MAGIC   - event_type: NOT NULL, in ('page_view', 'purchase', 'cart_add', 'search')
# MAGIC   - event_timestamp: NOT NULL, not future-dated
# MAGIC - Error Handling: Log failures to main.quality.event_validation_errors
# MAGIC - Output: main.silver.customer_events_validated
# MAGIC - Partition: event_date
# MAGIC - Mode: Overwrite daily partition
# MAGIC
# MAGIC GOLD LAYER - Customer Segmentation:
# MAGIC - Source: main.silver.customer_events_validated (valid records only)
# MAGIC - Business Logic:
# MAGIC   1. Calculate per customer (last 90 days):
# MAGIC      - Recency: Days since last event
# MAGIC      - Frequency: Count of events
# MAGIC      - Monetary: Total purchase amount
# MAGIC   2. Segment customers:
# MAGIC      - VIP: Recency ≤ 7 days, Frequency ≥ 20, Monetary ≥ $1000
# MAGIC      - Active: Recency ≤ 30 days, Frequency ≥ 5
# MAGIC      - At Risk: Recency > 30 days, previously Active
# MAGIC      - Inactive: Recency > 90 days
# MAGIC   3. Calculate:
# MAGIC      - Customer lifetime value
# MAGIC      - Average purchase frequency
# MAGIC      - Preferred event type
# MAGIC      - Most active day of week
# MAGIC      - Last event date
# MAGIC - Output: main.gold.customer_segments
# MAGIC - Refresh: Daily
# MAGIC - Optimization: Z-order by customer_id, segment
# MAGIC
# MAGIC GOLD LAYER - Daily Metrics:
# MAGIC - Aggregate daily:
# MAGIC   - Event counts by type
# MAGIC   - Unique active customers
# MAGIC   - New customers
# MAGIC   - Revenue by customer segment
# MAGIC - Output: main.gold.daily_customer_metrics
# MAGIC - Partition: metric_date
# MAGIC
# MAGIC GOVERNANCE:
# MAGIC - Apply row filter: Users only see their region's data
# MAGIC - Mask PII columns in Silver/Gold layers
# MAGIC - Tag all Gold tables with 'domain:marketing'
# MAGIC - Maintain full data lineage
# MAGIC
# MAGIC DOCUMENTATION:
# MAGIC - Add table/column comments
# MAGIC - Set table properties: owner, refresh_schedule, sla
# MAGIC "
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **STEP 3: Genie Generates Code**
# MAGIC
# MAGIC Genie produces:
# MAGIC * 4 Python notebooks (Bronze, Silver, Gold segments, Gold metrics)
# MAGIC * Data quality validation framework
# MAGIC * SQL views for analytics
# MAGIC * Monitoring queries
# MAGIC * Documentation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **STEP 4: Review & Validate**
# MAGIC
# MAGIC **Checklist:**
# MAGIC - ✅ All tables use Unity Catalog paths
# MAGIC - ✅ No cache/persist (serverless compatible)
# MAGIC - ✅ Delta format specified
# MAGIC - ✅ Partitioning strategy optimal
# MAGIC - ✅ Business logic correct
# MAGIC - ✅ Error handling included
# MAGIC - ✅ Governance policies applied
# MAGIC - ✅ Code is readable and maintainable
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **STEP 5: Test with Sample Data**
# MAGIC
# MAGIC ```python
# MAGIC # Create sample test data
# MAGIC test_data = spark.createDataFrame([
# MAGIC     ("E001", "CUST-12345678", "purchase", "2026-04-21T10:00:00Z", '{"amount": 150.00}'),
# MAGIC     ("E002", "CUST-87654321", "page_view", "2026-04-21T10:05:00Z", '{"page": "homepage"}'),
# MAGIC     # ... more test records
# MAGIC ], ["event_id", "customer_id", "event_type", "event_timestamp", "event_data"])
# MAGIC
# MAGIC test_data.write.mode("overwrite").saveAsTable("main.test.customer_events_sample")
# MAGIC
# MAGIC # Run pipeline on test data
# MAGIC # Validate:
# MAGIC # 1. Record counts match
# MAGIC # 2. No errors in quality log
# MAGIC # 3. Segments calculated correctly
# MAGIC # 4. Performance acceptable
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **STEP 6: Refine (if needed)**
# MAGIC
# MAGIC **Discovered Issues:**
# MAGIC * JSON parsing failed for malformed records → Add try-catch
# MAGIC * Segmentation logic needed adjustment → Update thresholds
# MAGIC * Performance slow on large dates → Optimize partition pruning
# MAGIC
# MAGIC **Refined Prompt:**
# MAGIC ```
# MAGIC "Update Silver layer:
# MAGIC - Add error handling for JSON parsing (log malformed records, continue processing)
# MAGIC - Handle null values in event_data gracefully
# MAGIC - Add schema hints for JSON parsing to improve performance"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **STEP 7: Execute Full Pipeline**
# MAGIC
# MAGIC ```python
# MAGIC # Execute pipeline in order
# MAGIC dbutils.notebook.run("01_Bronze_Ingestion", 0)
# MAGIC dbutils.notebook.run("02_Silver_Validation", 0)
# MAGIC dbutils.notebook.run("03_Gold_Segmentation", 0)
# MAGIC dbutils.notebook.run("04_Gold_Metrics", 0)
# MAGIC
# MAGIC print("✅ Pipeline completed successfully")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **STEP 8: Monitor & Optimize**
# MAGIC
# MAGIC **Monitoring Queries:**
# MAGIC ```sql
# MAGIC -- Data freshness
# MAGIC SELECT 
# MAGIC   MAX(event_date) as latest_data_date,
# MAGIC   DATEDIFF(CURRENT_DATE(), MAX(event_date)) as data_age_days
# MAGIC FROM main.silver.customer_events_validated;
# MAGIC
# MAGIC -- Data quality metrics
# MAGIC SELECT 
# MAGIC   event_date,
# MAGIC   COUNT(*) as total_records,
# MAGIC   SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) as valid_records,
# MAGIC   SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as quality_pct
# MAGIC FROM main.silver.customer_events_validated
# MAGIC GROUP BY event_date
# MAGIC ORDER BY event_date DESC
# MAGIC LIMIT 7;
# MAGIC
# MAGIC -- Pipeline performance
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   row_count,
# MAGIC   size_mb,
# MAGIC   last_updated
# MAGIC FROM main.metadata.table_stats
# MAGIC WHERE table_name LIKE '%customer%'
# MAGIC ORDER BY last_updated DESC;
# MAGIC ```
# MAGIC
# MAGIC **Optimization:**
# MAGIC ```sql
# MAGIC -- Optimize Gold tables
# MAGIC OPTIMIZE main.gold.customer_segments
# MAGIC ZORDER BY (customer_id, segment);
# MAGIC
# MAGIC OPTIMIZE main.gold.daily_customer_metrics
# MAGIC ZORDER BY (metric_date);
# MAGIC
# MAGIC -- Vacuum old versions (retain 7 days)
# MAGIC VACUUM main.gold.customer_segments RETAIN 168 HOURS;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **STEP 9: Document & Share**
# MAGIC
# MAGIC ```sql
# MAGIC -- Add comprehensive table comments
# MAGIC COMMENT ON TABLE main.gold.customer_segments IS 
# MAGIC 'Customer segmentation based on RFM analysis. 
# MAGIC Refreshed daily at 2 AM UTC. 
# MAGIC Segments: VIP, Active, At Risk, Inactive. 
# MAGIC Data retention: 2 years. 
# MAGIC Owner: marketing_analytics@company.com';
# MAGIC
# MAGIC -- Add column comments
# MAGIC COMMENT ON COLUMN main.gold.customer_segments.customer_lifetime_value IS
# MAGIC 'Total revenue from customer in USD since account creation';
# MAGIC
# MAGIC -- Set table properties
# MAGIC ALTER TABLE main.gold.customer_segments
# MAGIC SET TBLPROPERTIES (
# MAGIC   'quality_level' = 'gold',
# MAGIC   'refresh_schedule' = 'daily_2am_utc',
# MAGIC   'data_owner' = 'marketing_analytics',
# MAGIC   'sla' = '99.9%',
# MAGIC   'created_by' = 'databricks_genie',
# MAGIC   'business_domain' = 'customer_analytics'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚡ Key Success Factors:
# MAGIC
# MAGIC | Factor | Importance | How to Achieve |
# MAGIC |--------|-----------|----------------|
# MAGIC | **Clear Requirements** | Critical | Document business needs before coding |
# MAGIC | **Detailed Prompts** | Critical | Use structured prompt templates |
# MAGIC | **Iterative Refinement** | High | Test → Validate → Refine → Repeat |
# MAGIC | **Code Review** | High | Always validate generated code |
# MAGIC | **Testing** | Critical | Test with realistic sample data |
# MAGIC | **Monitoring** | High | Set up alerts and dashboards |
# MAGIC | **Documentation** | Medium | Auto-document with metadata |
# MAGIC | **Governance** | Critical | Apply from day one |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Workflow Tips:
# MAGIC
# MAGIC 1. **Start Simple**: Build Bronze first, then add layers
# MAGIC 2. **Validate Early**: Test each layer before moving to next
# MAGIC 3. **Use Checkpoints**: Save working versions before major changes
# MAGIC 4. **Monitor Continuously**: Don't wait for issues to appear
# MAGIC 5. **Document as You Go**: Add comments and metadata immediately
# MAGIC 6. **Automate Testing**: Create data quality test suites
# MAGIC 7. **Version Control**: Track prompt versions and generated code
# MAGIC 8. **Collaborate**: Share prompts and patterns with team
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Measuring Success:
# MAGIC
# MAGIC **Development Metrics:**
# MAGIC * Time to build pipeline: Traditional (5 days) vs Genie (6 hours)
# MAGIC * Lines of code: Traditional (800+) vs Genie (500, more readable)
# MAGIC * Bugs found: Reduced by ~40% (validated patterns)
# MAGIC
# MAGIC **Operational Metrics:**
# MAGIC * Data quality: >99.5% (automated validation)
# MAGIC * Pipeline reliability: 99.9% uptime
# MAGIC * Processing time: <30 minutes for daily refresh
# MAGIC * Cost: Optimized with serverless compute
# MAGIC
# MAGIC **Business Metrics:**
# MAGIC * Faster insights: Same-day instead of weekly
# MAGIC * Better decisions: Higher quality data
# MAGIC * Increased productivity: Engineers focus on complex problems

# COMMAND ----------

# DBTITLE 1,Complete Workflow Demo - Code Example
# ============================================================
# END-TO-END WORKFLOW DEMONSTRATION
# Complete Customer Analytics Pipeline
# Author: @TRRaveendra
# ============================================================

print("🚀 COMPLETE AI-ASSISTED DATA ENGINEERING WORKFLOW")
print("="*70)

print("""
📝 WORKFLOW STEPS:

1. Define Requirements          ✅
2. Write Genie Prompt           ✅  
3. Generate Code               ✅
4. Review & Validate           ✅
5. Test with Sample Data       ✅
6. Refine if Needed            ✅
7. Execute Full Pipeline       ✅
8. Monitor & Optimize          ✅
9. Document & Share            ✅

""")

print("\n" + "="*70)
print("🎯 KEY OUTPUTS FROM THIS WORKFLOW:")
print("="*70)

outputs = {
    "Bronze Layer": "main.bronze.customer_events_raw",
    "Silver Layer": "main.silver.customer_events_validated",
    "Gold - Segments": "main.gold.customer_segments",
    "Gold - Metrics": "main.gold.daily_customer_metrics",
    "Quality Log": "main.quality.event_validation_errors",
    "Monitoring": "Real-time dashboards + alerts"
}

for i, (key, value) in enumerate(outputs.items(), 1):
    print(f"{i}. {key:20} → {value}")

print("\n" + "="*70)
print("📊 EXPECTED RESULTS:")
print("="*70)

results = [
    "95-99% reduction in development time",
    "Consistent, best-practice code patterns",
    "Automated data quality validation",
    "Full governance and audit compliance",
    "Production-ready pipeline in hours, not days",
    "Self-documenting code with metadata",
    "Optimized for Databricks serverless",
    "Easily maintainable and extensible"
]

for i, result in enumerate(results, 1):
    print(f"  {i}. ✅ {result}")

print("\n" + "="*70)
print("⚡ GENIE'S VALUE PROPOSITION:")
print("="*70)
print("""
Traditional Approach:
- Days/weeks of manual coding
- Inconsistent patterns across teams  
- Manual testing and debugging
- Documentation often skipped
- Governance added as afterthought

Genie-Assisted Approach:
- Hours to working pipeline
- Standardized, validated patterns
- Built-in quality checks
- Auto-generated documentation
- Governance-first by default

🏆 Result: 10x faster, higher quality, lower risk
""")

print("\n" + "="*70)
print("🚀 READY FOR PRODUCTION!")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Final Summary & Key Learnings
# MAGIC %md
# MAGIC # 🎓 Final Summary: Databricks Genie for Data Engineering
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔑 Key Learnings:
# MAGIC
# MAGIC ### **1. What is Databricks Genie?**
# MAGIC * AI-powered code assistant for data engineering
# MAGIC * Converts natural language to executable code
# MAGIC * Understands Unity Catalog metadata and context
# MAGIC * Enforces governance and best practices automatically
# MAGIC
# MAGIC ### **2. Core Capabilities:**
# MAGIC * ✅ Automated pipeline generation (Bronze → Silver → Gold)
# MAGIC * ✅ Intelligent code suggestions and optimization
# MAGIC * ✅ Data quality framework creation
# MAGIC * ✅ Schema-aware transformations
# MAGIC * ✅ Governance-compliant code
# MAGIC * ✅ Performance optimization recommendations
# MAGIC
# MAGIC ### **3. When to Use Genie:**
# MAGIC * ✅ Rapid prototyping and POCs
# MAGIC * ✅ Standard ETL patterns
# MAGIC * ✅ Data exploration and profiling
# MAGIC * ✅ Boilerplate code generation
# MAGIC * ✅ Learning best practices
# MAGIC * ✅ Metadata-driven automation
# MAGIC
# MAGIC ### **4. Best Practices:**
# MAGIC * Write specific, detailed prompts
# MAGIC * Always review and validate generated code
# MAGIC * Test with sample data before full deployment
# MAGIC * Leverage metadata for better code generation
# MAGIC * Iterate and refine prompts as needed
# MAGIC * Maintain governance from day one
# MAGIC * Document prompt patterns for reuse
# MAGIC
# MAGIC ### **5. Limitations & Considerations:**
# MAGIC * ⚠️ Not suitable for highly specialized algorithms
# MAGIC * ⚠️ Requires human validation for business logic
# MAGIC * ⚠️ May need refinement for edge cases
# MAGIC * ⚠️ Performance tuning may require manual optimization
# MAGIC * ⚠️ Domain expertise still needed for complex scenarios
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Impact Metrics:
# MAGIC
# MAGIC | Metric | Before Genie | With Genie | Improvement |
# MAGIC |--------|--------------|------------|-------------|
# MAGIC | **Development Time** | 3-5 days | 4-8 hours | 85-90% faster |
# MAGIC | **Code Quality** | Variable | Consistent | Standardized |
# MAGIC | **Data Quality** | 90-95% | 99%+ | Higher reliability |
# MAGIC | **Documentation** | Often incomplete | Auto-generated | Complete |
# MAGIC | **Time to Production** | Weeks | Days | 10x faster |
# MAGIC | **Developer Productivity** | 1x | 5-10x | Significant gain |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Technology Stack:
# MAGIC
# MAGIC ```
# MAGIC 🧠 Databricks Genie (AI Layer)
# MAGIC         ↓
# MAGIC 📊 Unity Catalog (Metadata & Governance)
# MAGIC         ↓
# MAGIC ⚡ Serverless Compute (Execution Engine)
# MAGIC         ↓
# MAGIC 🟦 Delta Lake (Storage Layer)
# MAGIC         ↓
# MAGIC 📊 Analytics & BI (Consumption Layer)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Architectural Patterns:
# MAGIC
# MAGIC ### **Pattern 1: Medallion Architecture**
# MAGIC ```
# MAGIC Bronze (Raw) → Silver (Validated) → Gold (Analytics)
# MAGIC ```
# MAGIC * Genie automates all three layers
# MAGIC * Maintains data lineage
# MAGIC * Enforces quality at each stage
# MAGIC
# MAGIC ### **Pattern 2: Metadata-Driven**
# MAGIC ```
# MAGIC Unity Catalog Metadata → Genie Analysis → Optimized Code
# MAGIC ```
# MAGIC * Leverages schema information
# MAGIC * Applies validation rules from metadata
# MAGIC * Generates context-aware code
# MAGIC
# MAGIC ### **Pattern 3: Governance-First**
# MAGIC ```
# MAGIC Permissions + Policies → Genie → Compliant Code
# MAGIC ```
# MAGIC * Respects access controls
# MAGIC * Applies row filters and column masks
# MAGIC * Maintains audit trail
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Future of AI-Assisted Data Engineering:
# MAGIC
# MAGIC ### **Emerging Trends:**
# MAGIC 1. **Self-Healing Pipelines**: AI detects and fixes issues automatically
# MAGIC 2. **Predictive Optimization**: AI suggests optimizations before bottlenecks occur
# MAGIC 3. **Natural Language Analytics**: Non-technical users query data via conversation
# MAGIC 4. **Automated Testing**: AI generates comprehensive test cases
# MAGIC 5. **Intelligent Monitoring**: AI-powered anomaly detection and alerting
# MAGIC
# MAGIC ### **Genie Evolution:**
# MAGIC * More sophisticated code generation
# MAGIC * Better understanding of business context
# MAGIC * Deeper integration with development workflows
# MAGIC * Advanced performance optimization
# MAGIC * Collaborative AI for team environments
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Resources for Further Learning:
# MAGIC
# MAGIC 1. **Databricks Documentation**:
# MAGIC    * Genie AI Assistant Guide
# MAGIC    * Unity Catalog Best Practices
# MAGIC    * Delta Lake Optimization
# MAGIC
# MAGIC 2. **Training Paths**:
# MAGIC    * Databricks Certified Data Engineer Associate
# MAGIC    * Advanced Spark & Delta Lake
# MAGIC    * AI-Assisted Development Patterns
# MAGIC
# MAGIC 3. **Community**:
# MAGIC    * Databricks Community Forums
# MAGIC    * GitHub Examples Repository
# MAGIC    * Tech Talks and Webinars
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚡ Quick Reference: Common Genie Prompts
# MAGIC
# MAGIC ### **Data Exploration:**
# MAGIC ```
# MAGIC "Show me schema and sample data from [table]"
# MAGIC "Profile data quality for [table]"
# MAGIC "Find relationships between [table1] and [table2]"
# MAGIC ```
# MAGIC
# MAGIC ### **ETL Development:**
# MAGIC ```
# MAGIC "Create Bronze layer for [source] to [target]"
# MAGIC "Build Silver layer with validations: [rules]"
# MAGIC "Generate Gold aggregations: [metrics]"
# MAGIC ```
# MAGIC
# MAGIC ### **Data Quality:**
# MAGIC ```
# MAGIC "Add validation rules: [rules] to [table]"
# MAGIC "Create data quality dashboard for [table]"
# MAGIC "Log validation errors to [error_table]"
# MAGIC ```
# MAGIC
# MAGIC ### **Optimization:**
# MAGIC ```
# MAGIC "Optimize [table] for query pattern: [pattern]"
# MAGIC "Add partitioning to [table] by [column]"
# MAGIC "Suggest performance improvements for [query]"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Final Thoughts:
# MAGIC
# MAGIC **Databricks Genie is not about replacing data engineers — it's about amplifying their capabilities.**
# MAGIC
# MAGIC * ✅ Focus on high-value work (architecture, business logic)
# MAGIC * ✅ Eliminate repetitive boilerplate coding
# MAGIC * ✅ Enforce best practices consistently
# MAGIC * ✅ Accelerate time to value
# MAGIC * ✅ Improve code quality and maintainability
# MAGIC
# MAGIC **The future of data engineering is collaborative:**
# MAGIC * Humans provide domain expertise and strategic thinking
# MAGIC * AI handles implementation and optimization
# MAGIC * Together, they deliver faster, better, more reliable data products
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Next Steps:
# MAGIC
# MAGIC 1. 📝 Practice prompt engineering with your data
# MAGIC 2. 🛠️ Build sample pipelines using Genie
# MAGIC 3. 🔍 Review and learn from generated code
# MAGIC 4. 📚 Document effective prompt patterns
# MAGIC 5. 🤝 Share learnings with your team
# MAGIC 6. 🚀 Deploy to production with confidence
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **🎓 Congratulations on completing Phase 11 Day 42!**
# MAGIC
# MAGIC **@TRRaveendra | Databricks Data Engineering Training Series**

# COMMAND ----------

# DBTITLE 1,Interview Questions & Common Mistakes
# MAGIC %md
# MAGIC # 🎯 Interview Questions: Databricks Genie & AI-Assisted ETL
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Interview Questions (10 Questions):
# MAGIC
# MAGIC ### **Question 1: Conceptual Understanding**
# MAGIC **Q: What is Databricks Genie and how does it differ from traditional code autocomplete features?**
# MAGIC
# MAGIC **A:** Databricks Genie is an AI-powered intelligent data assistant that:
# MAGIC * Uses Large Language Models (LLMs) to understand natural language requirements
# MAGIC * Generates complete, production-ready code (not just snippets)
# MAGIC * Is context-aware of Unity Catalog metadata, table schemas, and relationships
# MAGIC * Understands business logic and data engineering patterns
# MAGIC * Enforces governance policies automatically
# MAGIC
# MAGIC Unlike traditional autocomplete (syntax-based suggestions), Genie:
# MAGIC * Understands intent, not just syntax
# MAGIC * Generates entire pipelines from descriptions
# MAGIC * Applies best practices and optimization automatically
# MAGIC * Maintains lineage and governance compliance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 2: Prompt Engineering**
# MAGIC **Q: What makes a good prompt for Genie when creating an ETL pipeline?**
# MAGIC
# MAGIC **A:** A good prompt includes:
# MAGIC 1. **Context**: Source and target tables with catalog.schema.table format
# MAGIC 2. **Transformations**: Specific operations (dedupe, validate, aggregate)
# MAGIC 3. **Constraints**: Data quality rules, validation criteria
# MAGIC 4. **Output Specifications**: Format (Delta), partitioning, write mode
# MAGIC 5. **Business Logic**: Domain-specific rules and calculations
# MAGIC 6. **Error Handling**: How to handle failures and edge cases
# MAGIC
# MAGIC Example:
# MAGIC ```
# MAGIC "Create Silver layer from main.bronze.sales:
# MAGIC - Remove duplicates by transaction_id
# MAGIC - Validate amount > 0
# MAGIC - Parse date from string to timestamp
# MAGIC - Partition by date
# MAGIC - Log errors to main.quality.validation_log"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 3: Medallion Architecture**
# MAGIC **Q: How does Genie assist in building a Medallion architecture? Describe the role of Genie in each layer.**
# MAGIC
# MAGIC **A:**
# MAGIC
# MAGIC **Bronze Layer:**
# MAGIC * Genie generates schema inference code
# MAGIC * Adds audit columns (ingestion_time, source_file)
# MAGIC * Implements schema evolution handling
# MAGIC * Creates append-mode ingestion logic
# MAGIC
# MAGIC **Silver Layer:**
# MAGIC * Generates data quality validation rules
# MAGIC * Implements deduplication logic
# MAGIC * Creates type conversion and standardization code
# MAGIC * Adds derived columns and business logic
# MAGIC * Implements error logging
# MAGIC
# MAGIC **Gold Layer:**
# MAGIC * Generates aggregation queries
# MAGIC * Creates dimensional models
# MAGIC * Implements window functions for analytics
# MAGIC * Optimizes with partitioning and Z-ordering
# MAGIC * Adds table documentation and properties
# MAGIC
# MAGIC **Key Benefit:** Genie ensures consistency across all layers following best practices.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 4: Metadata-Driven Development**
# MAGIC **Q: How does Unity Catalog metadata enhance Genie's code generation capabilities?**
# MAGIC
# MAGIC **A:** Unity Catalog metadata provides:
# MAGIC
# MAGIC 1. **Schema Information:**
# MAGIC    * Column names, types, nullability
# MAGIC    * Genie uses this for correct type conversions
# MAGIC
# MAGIC 2. **Table Properties:**
# MAGIC    * Primary keys, foreign keys
# MAGIC    * Genie infers join relationships automatically
# MAGIC
# MAGIC 3. **Column Comments:**
# MAGIC    * Business rules, validation patterns
# MAGIC    * Genie extracts rules and generates validation code
# MAGIC
# MAGIC 4. **Governance Tags:**
# MAGIC    * PII, confidential data classification
# MAGIC    * Genie applies masking/encryption automatically
# MAGIC
# MAGIC 5. **Lineage Information:**
# MAGIC    * Upstream/downstream dependencies
# MAGIC    * Genie suggests optimal transformation order
# MAGIC
# MAGIC 6. **Statistics:**
# MAGIC    * Row counts, data distribution
# MAGIC    * Genie optimizes join strategies (broadcast vs shuffle)
# MAGIC
# MAGIC **Result:** More accurate, optimized, and governance-compliant code.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 5: Data Quality**
# MAGIC **Q: What validation strategies can Genie generate automatically? How do you ensure they're comprehensive?**
# MAGIC
# MAGIC **A:** Genie can generate:
# MAGIC
# MAGIC 1. **NOT NULL Checks**: Validates required fields
# MAGIC 2. **Range Validations**: Numeric bounds (age between 0-120)
# MAGIC 3. **Pattern Matching**: Regex for emails, phones, IDs
# MAGIC 4. **Referential Integrity**: Foreign key existence checks
# MAGIC 5. **Business Rules**: Domain-specific validations
# MAGIC 6. **Data Type Validations**: Proper casting with error handling
# MAGIC 7. **Duplicate Detection**: Based on composite keys
# MAGIC
# MAGIC **Ensuring Comprehensiveness:**
# MAGIC * Start with metadata (column comments with validation rules)
# MAGIC * Review generated validation logic against requirements
# MAGIC * Test with real data samples including edge cases
# MAGIC * Add custom validations for complex business rules
# MAGIC * Monitor validation failure rates in production
# MAGIC * Iterate and refine based on data profiling results
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 6: Governance & Security**
# MAGIC **Q: How does Genie ensure generated code is governance-compliant?**
# MAGIC
# MAGIC **A:**
# MAGIC
# MAGIC 1. **Permission-Aware:**
# MAGIC    * Only generates code for accessible tables
# MAGIC    * Respects Unity Catalog ACLs
# MAGIC
# MAGIC 2. **Policy Enforcement:**
# MAGIC    * Automatically includes row filters
# MAGIC    * Applies column masking for sensitive data
# MAGIC
# MAGIC 3. **Audit Logging:**
# MAGIC    * Tags generated queries with metadata
# MAGIC    * Tracks lineage automatically
# MAGIC
# MAGIC 4. **Data Classification:**
# MAGIC    * Recognizes PII/PHI tags
# MAGIC    * Applies appropriate handling (encryption, masking)
# MAGIC
# MAGIC 5. **Compliance Patterns:**
# MAGIC    * GDPR: right to be forgotten, consent tracking
# MAGIC    * HIPAA: de-identification, access logging
# MAGIC
# MAGIC 6. **Secure Defaults:**
# MAGIC    * Uses Unity Catalog paths (not direct S3)
# MAGIC    * Enforces Delta format with versioning
# MAGIC    * Includes audit columns by default
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 7: Code Validation**
# MAGIC **Q: What are the key areas to review when validating Genie-generated code?**
# MAGIC
# MAGIC **A:**
# MAGIC
# MAGIC **1. Functional Validation:**
# MAGIC * Does it implement the business logic correctly?
# MAGIC * Are all requirements from the prompt addressed?
# MAGIC * Are edge cases handled?
# MAGIC
# MAGIC **2. Technical Validation:**
# MAGIC * Serverless-compatible (no RDD, cache, persist)?
# MAGIC * Unity Catalog paths used correctly?
# MAGIC * Delta format specified?
# MAGIC * Optimal partitioning strategy?
# MAGIC
# MAGIC **3. Performance Validation:**
# MAGIC * Are there unnecessary shuffles?
# MAGIC * Is filter pushdown happening?
# MAGIC * Are broadcast joins used appropriately?
# MAGIC * Is the query plan optimal?
# MAGIC
# MAGIC **4. Security Validation:**
# MAGIC * Are permissions respected?
# MAGIC * Is PII handled correctly?
# MAGIC * Are audit columns included?
# MAGIC
# MAGIC **5. Maintainability:**
# MAGIC * Is the code readable?
# MAGIC * Are there comments for complex logic?
# MAGIC * Is it modular and reusable?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 8: Performance Optimization**
# MAGIC **Q: How does Genie optimize generated code for performance? What manual optimizations might still be needed?**
# MAGIC
# MAGIC **A:**
# MAGIC
# MAGIC **Genie's Automatic Optimizations:**
# MAGIC * Partitioning strategies based on query patterns
# MAGIC * Z-ordering recommendations
# MAGIC * Broadcast join suggestions for small tables
# MAGIC * Filter pushdown in queries
# MAGIC * Column pruning
# MAGIC * Predicate pushdown to Delta
# MAGIC * Optimal shuffle partitions
# MAGIC
# MAGIC **Manual Optimizations Still Needed:**
# MAGIC * Domain-specific caching strategies (where business logic permits)
# MAGIC * Custom UDF optimization
# MAGIC * Complex windowing operation tuning
# MAGIC * Cluster sizing for specific workloads
# MAGIC * Advanced partitioning for skewed data
# MAGIC * Custom shuffle partition configuration
# MAGIC * Liquid clustering for specific access patterns
# MAGIC
# MAGIC **Validation:**
# MAGIC * Review query plans with `.explain()`
# MAGIC * Monitor execution metrics
# MAGIC * Profile actual data volumes
# MAGIC * Benchmark against performance SLAs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 9: Limitations**
# MAGIC **Q: What are the limitations of Genie? When should you NOT rely solely on Genie-generated code?**
# MAGIC
# MAGIC **A:**
# MAGIC
# MAGIC **Limitations:**
# MAGIC 1. **Complex Business Logic:**
# MAGIC    * Highly specialized algorithms
# MAGIC    * Domain-specific calculations requiring deep expertise
# MAGIC    * Novel approaches not in training data
# MAGIC
# MAGIC 2. **Performance-Critical Code:**
# MAGIC    * May not achieve absolute optimal performance
# MAGIC    * Requires manual tuning for extreme scale
# MAGIC
# MAGIC 3. **Ambiguous Requirements:**
# MAGIC    * Vague prompts lead to generic code
# MAGIC    * May make incorrect assumptions
# MAGIC
# MAGIC 4. **Novel Technologies:**
# MAGIC    * Limited knowledge of very new features
# MAGIC    * May use outdated patterns
# MAGIC
# MAGIC **When NOT to Rely Solely on Genie:**
# MAGIC * ❌ Mission-critical financial calculations
# MAGIC * ❌ Regulatory compliance logic (requires legal review)
# MAGIC * ❌ Performance at extreme scale (PB+ data)
# MAGIC * ❌ Custom ML algorithms
# MAGIC * ❌ Security-sensitive operations
# MAGIC * ❌ Integration with proprietary systems
# MAGIC
# MAGIC **Best Practice:** Use Genie for scaffolding, human expertise for critical logic.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Question 10: Real-World Application**
# MAGIC **Q: Design a complete data pipeline using Genie for a real-time fraud detection system. What prompts would you use at each stage?**
# MAGIC
# MAGIC **A:**
# MAGIC
# MAGIC **Architecture:**
# MAGIC ```
# MAGIC Events (Streaming) → Bronze → Silver → Gold → ML Model → Alerts
# MAGIC ```
# MAGIC
# MAGIC **Prompt 1 - Bronze (Streaming Ingestion):**
# MAGIC ```
# MAGIC "Create streaming Bronze layer:
# MAGIC - Source: Event Hub/Kafka topic 'transactions'
# MAGIC - Schema: transaction_id, user_id, amount, merchant, timestamp, location
# MAGIC - Add: ingestion_time, source_offset
# MAGIC - Checkpoint: /Volumes/main/checkpoints/bronze/
# MAGIC - Output: main.bronze.transactions_stream
# MAGIC - Trigger: 10 seconds
# MAGIC "
# MAGIC ```
# MAGIC
# MAGIC **Prompt 2 - Silver (Feature Engineering):**
# MAGIC ```
# MAGIC "Create streaming Silver layer with fraud features:
# MAGIC - Source: main.bronze.transactions_stream
# MAGIC - Features to generate:
# MAGIC   1. User's transaction velocity (count in last 5 minutes)
# MAGIC   2. Amount deviation from user's average (last 30 days)
# MAGIC   3. Distance from user's typical location
# MAGIC   4. Time since last transaction (seconds)
# MAGIC   5. Merchant risk score (join with main.reference.merchant_risk)
# MAGIC   6. Cross-border transaction flag
# MAGIC - Watermark: 5 minutes on event timestamp
# MAGIC - Output: main.silver.fraud_features
# MAGIC - Stateful aggregations with checkpointing
# MAGIC "
# MAGIC ```
# MAGIC
# MAGIC **Prompt 3 - Gold (Aggregations for Model):**
# MAGIC ```
# MAGIC "Create real-time aggregations for fraud monitoring:
# MAGIC - Source: main.silver.fraud_features
# MAGIC - Aggregations:
# MAGIC   1. Transactions per user per minute
# MAGIC   2. Average transaction amount per merchant per hour
# MAGIC   3. High-risk transaction count by region
# MAGIC   4. Anomaly score distribution
# MAGIC - Sliding window: 15 minutes, slide every 1 minute
# MAGIC - Output: main.gold.fraud_metrics
# MAGIC - Enable for ML model consumption
# MAGIC "
# MAGIC ```
# MAGIC
# MAGIC **Prompt 4 - Quality Monitoring:**
# MAGIC ```
# MAGIC "Create data quality monitoring for fraud pipeline:
# MAGIC - Monitor:
# MAGIC   1. Event processing lag (< 30 seconds)
# MAGIC   2. Null rate in critical features (< 1%)
# MAGIC   3. Feature value anomalies (outside 3 sigma)
# MAGIC   4. Duplicate event detection
# MAGIC - Alert if:
# MAGIC   - Lag > 1 minute
# MAGIC   - Null rate > 5%
# MAGIC   - Throughput drops > 50%
# MAGIC - Log to: main.quality.fraud_pipeline_health
# MAGIC "
# MAGIC ```
# MAGIC
# MAGIC **Result:** Complete real-time fraud detection pipeline in hours, not weeks!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Common Mistakes to Avoid:
# MAGIC
# MAGIC ### **Mistake 1: Vague Prompts**
# MAGIC ❌ **Wrong:** "Create a data pipeline"
# MAGIC ✅ **Right:** "Create Bronze layer reading JSON from /Volumes/main/raw/ and writing to main.bronze.events with schema evolution"
# MAGIC
# MAGIC ### **Mistake 2: Blindly Trusting Output**
# MAGIC ❌ **Wrong:** Copy-paste Genie code without review
# MAGIC ✅ **Right:** Review for business logic, test with samples, validate performance
# MAGIC
# MAGIC ### **Mistake 3: Ignoring Governance**
# MAGIC ❌ **Wrong:** Focus only on functionality, add security later
# MAGIC ✅ **Right:** Include governance requirements in prompt from start
# MAGIC
# MAGIC ### **Mistake 4: Not Leveraging Metadata**
# MAGIC ❌ **Wrong:** Generate code without populating table comments and properties
# MAGIC ✅ **Right:** Enrich Unity Catalog metadata first, then use Genie for better results
# MAGIC
# MAGIC ### **Mistake 5: Skipping Testing**
# MAGIC ❌ **Wrong:** Deploy generated code directly to production
# MAGIC ✅ **Right:** Test with sample data, validate edge cases, check performance
# MAGIC
# MAGIC ### **Mistake 6: Poor Error Handling**
# MAGIC ❌ **Wrong:** Accept generated code without explicit error handling
# MAGIC ✅ **Right:** Request error logging, retry logic, and failure notifications in prompt
# MAGIC
# MAGIC ### **Mistake 7: Ignoring Performance**
# MAGIC ❌ **Wrong:** Assume generated code is optimally performant
# MAGIC ✅ **Right:** Review query plans, add optimization hints in prompt, benchmark
# MAGIC
# MAGIC ### **Mistake 8: Not Iterating**
# MAGIC ❌ **Wrong:** Give up if first generated code isn't perfect
# MAGIC ✅ **Right:** Refine prompt based on results, iterate until satisfactory
# MAGIC
# MAGIC ### **Mistake 9: Overlooking Documentation**
# MAGIC ❌ **Wrong:** Generate code without requesting documentation
# MAGIC ✅ **Right:** Ask Genie to include comments, table properties, and usage examples
# MAGIC
# MAGIC ### **Mistake 10: Using for Wrong Scenarios**
# MAGIC ❌ **Wrong:** Use Genie for highly specialized algorithms or compliance-critical logic
# MAGIC ✅ **Right:** Use Genie for standard patterns, human expertise for critical/novel logic
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏆 Best Practices Summary:
# MAGIC
# MAGIC 1. ✅ **Write detailed, structured prompts**
# MAGIC 2. ✅ **Always review and validate generated code**
# MAGIC 3. ✅ **Test with realistic data samples**
# MAGIC 4. ✅ **Leverage Unity Catalog metadata**
# MAGIC 5. ✅ **Include governance from day one**
# MAGIC 6. ✅ **Monitor and optimize in production**
# MAGIC 7. ✅ **Document effective prompt patterns**
# MAGIC 8. ✅ **Iterate and refine as needed**
# MAGIC 9. ✅ **Combine AI with human expertise**
# MAGIC 10. ✅ **Share learnings with team**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **🎓 End of Phase 11 Day 42 - Databricks Genie Training**
# MAGIC
# MAGIC **@TRRaveendra | Data Engineering Excellence**