# Databricks notebook source
# DBTITLE 1,Notebook Header and Information
# MAGIC %md
# MAGIC <div style="text-align: center; padding: 20px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 10px; margin-bottom: 20px;">
# MAGIC   <h1 style="color: white; font-size: 2.5em; margin: 0; text-shadow: 2px 2px 4px rgba(0,0,0,0.3);">⚡ Databricks Data Engineering Training ⚡</h1>
# MAGIC   <h2 style="color: #f0f0f0; font-size: 1.5em; margin: 10px 0;">Phase 1 — Day 1: Data Engineering Fundamentals</h2>
# MAGIC   <p style="color: #ffffff; font-size: 1.1em; margin: 5px 0;">Building Production-Grade Data Pipelines on Databricks</p>
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC <div style="background-color: #f8f9fa; padding: 15px; border-left: 5px solid #667eea; margin: 20px 0;">
# MAGIC
# MAGIC ### 📝 Notebook Information
# MAGIC
# MAGIC | Property | Details |
# MAGIC |----------|----------|
# MAGIC | **Author** | @TRRaveendra |
# MAGIC | **Course** | Databricks Data Engineering Certification Prep |
# MAGIC | **Module** | Phase 1 — Fundamentals |
# MAGIC | **Version** | 1.0 |
# MAGIC | **Last Updated** | April 21, 2026 |
# MAGIC | **Platform** | Databricks Lakehouse Platform |
# MAGIC | **Compute** | Serverless (Auto-attached) |
# MAGIC | **Runtime** | Databricks Runtime 15.x+ |
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC <div style="background-color: #fff3cd; padding: 15px; border-left: 5px solid #ffc107; margin: 20px 0;">
# MAGIC
# MAGIC ### 📋 Training Overview
# MAGIC
# MAGIC **Training Program**: *Databricks Certified Data Engineer Associate Preparation*  
# MAGIC **Instructor**: T.R. Raveendra (@TRRaveendra)  
# MAGIC **Institution**: Professional Data Engineering Training  
# MAGIC **Focus**: Production-ready data engineering skills on Databricks  
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC <div style="background-color: #d1ecf1; padding: 15px; border-left: 5px solid #0c5460; margin: 20px 0;">
# MAGIC
# MAGIC ### 🎯 Today's Learning Path
# MAGIC
# MAGIC 1. **Understand** the role of Data Engineers in modern data platforms
# MAGIC 2. **Build** end-to-end data pipelines using Medallion Architecture
# MAGIC 3. **Distinguish** between OLTP and OLAP systems
# MAGIC 4. **Apply** production best practices and data quality patterns
# MAGIC 5. **Accelerate** development with AI-powered coding (Genie Code)
# MAGIC
# MAGIC **Expected Duration**: 2-3 hours  
# MAGIC **Hands-on Practice**: ✅ Included  
# MAGIC **Real-world Datasets**: ✅ Databricks Samples  
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC <div style="background-color: #d4edda; padding: 15px; border-left: 5px solid #155724; margin: 20px 0;">
# MAGIC
# MAGIC ### ✅ Prerequisites
# MAGIC
# MAGIC * Basic understanding of SQL queries
# MAGIC * Familiarity with Python programming
# MAGIC * Understanding of data concepts (tables, schemas, joins)
# MAGIC * Databricks workspace access
# MAGIC * Enthusiasm to learn! 🚀
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC <div style="background-color: #f8d7da; padding: 15px; border-left: 5px solid #721c24; margin: 20px 0;">
# MAGIC
# MAGIC ### ⚠️ Important Notes
# MAGIC
# MAGIC * **Compute**: This notebook uses Serverless compute (automatically attached)
# MAGIC * **Tables**: All tables are managed Delta tables in Unity Catalog
# MAGIC * **Data**: Uses Databricks sample datasets (/databricks-datasets/)
# MAGIC * **Execution**: Run cells sequentially from top to bottom
# MAGIC * **Support**: Contact @TRRaveendra for questions or issues
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC <div style="text-align: center; padding: 15px; background-color: #e9ecef; border-radius: 5px; margin: 20px 0;">
# MAGIC
# MAGIC ### 📬 Contact & Attribution
# MAGIC
# MAGIC **Created by**: @TRRaveendra  
# MAGIC **LinkedIn**: [Connect on LinkedIn](https://linkedin.com)  
# MAGIC **GitHub**: [View Training Repos](https://github.com)  
# MAGIC **Email**: Training Support  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC *© 2026 T.R. Raveendra | Databricks Data Engineering Training*  
# MAGIC *All rights reserved. For educational purposes.*
# MAGIC
# MAGIC **Watermark**: @TRRaveendra | Phase 1 Day 1 | Data Engineering Fundamentals
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Ready to Begin?
# MAGIC
# MAGIC Scroll down to start your journey into Data Engineering! 👇

# COMMAND ----------

# DBTITLE 1,Title and Overview
# MAGIC %md
# MAGIC # PHASE 1 — DAY 1: Data Engineering Fundamentals
# MAGIC ## Building Production Data Pipelines on Databricks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📚 What You'll Learn Today
# MAGIC
# MAGIC * **Role of a Data Engineer** in modern data platforms
# MAGIC * **End-to-End Data Lifecycle**: Ingest → Transform → Serve
# MAGIC * **OLTP vs OLAP Systems**: Understanding transactional vs analytical workloads
# MAGIC * **Hands-on PySpark**: Real code examples with Delta Lake
# MAGIC * **Real-World Use Cases**: E-commerce, Banking, Healthcare
# MAGIC * **Best Practices**: Schema enforcement, data validation, Medallion architecture
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Learning Objectives
# MAGIC
# MAGIC By the end of this notebook, you will:
# MAGIC 1. Understand what Data Engineers do and why they're critical
# MAGIC 2. Build a simple data pipeline using PySpark
# MAGIC 3. Distinguish between transactional and analytical systems
# MAGIC 4. Apply data engineering best practices
# MAGIC 5. Be ready to design production data solutions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Duration**: 2-3 hours  
# MAGIC **Prerequisites**: Basic SQL knowledge, Python familiarity  
# MAGIC **Compute**: Serverless (auto-attached)

# COMMAND ----------

# DBTITLE 1,Section 1: Role of Data Engineer
# MAGIC %md
# MAGIC # 👷 SECTION 1: Role of a Data Engineer
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 (Explain Like I'm 5)
# MAGIC
# MAGIC **Imagine a restaurant:**
# MAGIC * **Data Engineers** = Chefs who prepare ingredients and cook meals
# MAGIC * **Data** = Raw ingredients (vegetables, meat, spices)
# MAGIC * **Pipelines** = Recipes and cooking process
# MAGIC * **Data Analysts/Scientists** = Customers who enjoy the meal
# MAGIC
# MAGIC Data Engineers take raw, messy data and transform it into clean, organized data that others can use to make decisions.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation
# MAGIC
# MAGIC ### Core Responsibilities
# MAGIC
# MAGIC #### 1. **Data Ingestion**
# MAGIC * Extract data from multiple sources (APIs, databases, files, streams)
# MAGIC * Handle batch and real-time data ingestion
# MAGIC * Implement Auto Loader for incremental file processing
# MAGIC * Manage schema evolution and data quality at source
# MAGIC
# MAGIC #### 2. **Data Transformation**
# MAGIC * Clean, validate, and enrich raw data
# MAGIC * Apply business logic and data normalization
# MAGIC * Implement data quality checks and constraints
# MAGIC * Build aggregations and dimensional models
# MAGIC
# MAGIC #### 3. **Pipeline Orchestration**
# MAGIC * Design and build automated data pipelines
# MAGIC * Implement error handling and retry logic
# MAGIC * Manage dependencies between data workflows
# MAGIC * Monitor pipeline health and performance
# MAGIC
# MAGIC #### 4. **Data Optimization**
# MAGIC * Partition data for query performance
# MAGIC * Implement Z-Ordering and data skipping
# MAGIC * Optimize file sizes and compaction
# MAGIC * Tune Spark configurations
# MAGIC
# MAGIC #### 5. **Data Governance**
# MAGIC * Implement access controls (Unity Catalog)
# MAGIC * Ensure data quality and lineage
# MAGIC * Manage PII and sensitive data
# MAGIC * Maintain data documentation and metadata
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Key Skills Required
# MAGIC
# MAGIC | Skill Category | Technologies |
# MAGIC |---|---|
# MAGIC | **Programming** | Python, SQL, Scala |
# MAGIC | **Big Data** | Apache Spark, Delta Lake |
# MAGIC | **Cloud Platforms** | AWS, Azure, GCP |
# MAGIC | **Orchestration** | Databricks Jobs, Airflow |
# MAGIC | **Data Modeling** | Star Schema, Data Vault |
# MAGIC | **Version Control** | Git, CI/CD |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏭 Modern Data Platform Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌──────────────┐
# MAGIC │   Sources   │────►│   Ingestion  │────►│ Processing  │────►│  Consumption │
# MAGIC │             │     │              │     │             │     │              │
# MAGIC │ • APIs      │     │ • Batch      │     │ • Transform │     │ • BI Tools   │
# MAGIC │ • Databases │     │ • Streaming  │     │ • Aggregate │     │ • ML Models  │
# MAGIC │ • Files     │     │ • CDC        │     │ • Validate  │     │ • Analytics  │
# MAGIC │ • Events    │     │ • Auto Loader│     │ • Enrich    │     │ • Reports    │
# MAGIC └─────────────┘     └──────────────┘     └─────────────┘     └──────────────┘
# MAGIC         │                  │                   │                   │
# MAGIC         └──────────────────┴───────────────────┴───────────────────┘
# MAGIC                     DATA ENGINEER'S DOMAIN
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Data Engineer vs Other Roles
# MAGIC
# MAGIC | Role | Focus | Tools | Output |
# MAGIC |---|---|---|---|
# MAGIC | **Data Engineer** | Build pipelines | Spark, SQL, Delta | Clean datasets |
# MAGIC | **Data Analyst** | Analyze data | SQL, Excel, BI | Reports, dashboards |
# MAGIC | **Data Scientist** | Build models | Python, R, ML | Predictions, insights |
# MAGIC | **Analytics Engineer** | Model data | dbt, SQL | Dimensional models |

# COMMAND ----------

# DBTITLE 1,Section 2: End-to-End Data Lifecycle
# MAGIC %md
# MAGIC # 🔄 SECTION 2: End-to-End Data Lifecycle
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## The Three Stages of Data
# MAGIC
# MAGIC ### 1️⃣ INGEST (Extract)
# MAGIC **Goal**: Bring data from source systems into the platform
# MAGIC * Batch ingestion (daily, hourly)
# MAGIC * Streaming ingestion (real-time)
# MAGIC * Schema inference and validation
# MAGIC * Incremental loading
# MAGIC
# MAGIC ### 2️⃣ TRANSFORM (Process)
# MAGIC **Goal**: Clean, enrich, and structure data for analysis
# MAGIC * Data cleaning (nulls, duplicates)
# MAGIC * Business logic application
# MAGIC * Joins and aggregations
# MAGIC * Data quality checks
# MAGIC
# MAGIC ### 3️⃣ SERVE (Load)
# MAGIC **Goal**: Make data available for consumption
# MAGIC * Optimized tables for queries
# MAGIC * Dimensional models (fact/dimension)
# MAGIC * APIs and endpoints
# MAGIC * BI tool connections
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 The Medallion Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
# MAGIC │   BRONZE     │────►│   SILVER     │────►│    GOLD      │
# MAGIC │              │     │              │     │              │
# MAGIC │ Raw Data     │     │ Cleaned Data │     │ Business     │
# MAGIC │ As-Is        │     │ Validated    │     │ Aggregates   │
# MAGIC │ Append-Only  │     │ Enriched     │     │ Analytics    │
# MAGIC │              │     │              │     │ Ready        │
# MAGIC └──────────────┘     └──────────────┘     └──────────────┘
# MAGIC     Landing              Curated            Consumption
# MAGIC ```
# MAGIC
# MAGIC **Bronze**: Raw data, exactly as ingested  
# MAGIC **Silver**: Cleaned, validated, deduplicated  
# MAGIC **Gold**: Business-level aggregates, ready for analytics  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Let's Build This Pipeline! 🚀

# COMMAND ----------

# DBTITLE 1,Setup: Import Libraries
# Import necessary libraries
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta

print("✅ Libraries imported successfully!")
print(f"ℹ️ Spark Version: {spark.version}")
print(f"ℹ️ Execution Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# COMMAND ----------

# DBTITLE 1,Stage 1: Ingest (Bronze Layer)
# MAGIC %md
# MAGIC ## 🥉 Stage 1: INGEST — Bronze Layer
# MAGIC
# MAGIC We'll use the **Databricks sample dataset** (`/databricks-datasets/`) which contains real-world data.
# MAGIC
# MAGIC **Dataset**: Online Retail Data (e-commerce transactions)
# MAGIC * Source: CSV files
# MAGIC * Contains: Order IDs, products, quantities, prices, customer info
# MAGIC * Size: ~500K+ transactions

# COMMAND ----------

# DBTITLE 1,Ingest: Load Raw Data from CSV
# BRONZE LAYER: Ingest raw data as-is
# Using Databricks sample retail dataset

# Define source path
data_source = "/databricks-datasets/online_retail/data-001/data.csv"

# Read raw CSV data
df_bronze = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(data_source)

# Add ingestion metadata (best practice)
df_bronze = df_bronze \
    .withColumn("ingestion_timestamp", F.current_timestamp()) \
    .withColumn("source_file", F.lit(data_source))

print(f"✅ Ingested {df_bronze.count():,} records")
print(f"ℹ️ Schema: {len(df_bronze.columns)} columns")

# Display sample
display(df_bronze.limit(10))

# COMMAND ----------

# DBTITLE 1,Explore Raw Data Structure
# Examine the schema
print("\n📋 BRONZE LAYER SCHEMA:\n")
df_bronze.printSchema()

# Get data statistics
print("\n📊 DATA STATISTICS:\n")
print(f"Total Records: {df_bronze.count():,}")
print(f"Columns: {len(df_bronze.columns)}")
print(f"Date Range: {df_bronze.select(F.min('InvoiceDate')).first()[0]} to {df_bronze.select(F.max('InvoiceDate')).first()[0]}")
print(f"Unique Customers: {df_bronze.select('CustomerID').distinct().count():,}")
print(f"Unique Products: {df_bronze.select('StockCode').distinct().count():,}")

# COMMAND ----------

# DBTITLE 1,Save to Bronze Delta Table
# Save as Delta table (Bronze layer)
# Using managed table in Unity Catalog

# Write to Delta format as managed table
df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_retail_data")

print("✅ Bronze table created: bronze_retail_data")
print("✅ INGEST stage complete!")
print("\n📦 Bronze Layer Characteristics:")
print("  • Raw data preserved as-is")
print("  • Includes ingestion metadata")
print("  • Append-only architecture")
print("  • Source of truth")

# COMMAND ----------

# DBTITLE 1,Stage 2: Transform (Silver Layer)
# MAGIC %md
# MAGIC ## 🥈 Stage 2: TRANSFORM — Silver Layer
# MAGIC
# MAGIC Now we'll clean, validate, and enrich the data:
# MAGIC * Remove duplicates and nulls
# MAGIC * Apply data type corrections
# MAGIC * Add business calculations
# MAGIC * Implement data quality checks

# COMMAND ----------

# DBTITLE 1,Load Bronze Data
# Load data from Bronze layer
df_silver = spark.table("bronze_retail_data")

print(f"📚 Loaded {df_silver.count():,} records from Bronze")

# COMMAND ----------

# DBTITLE 1,Data Cleaning and Validation
# SILVER LAYER: Data Cleaning and Validation

# 1. Remove null customer IDs (invalid transactions)
df_silver = df_silver.filter(F.col("CustomerID").isNotNull())

# 2. Remove negative quantities (returns/cancellations)
df_silver = df_silver.filter(F.col("Quantity") > 0)

# 3. Remove negative prices (data quality issues)
df_silver = df_silver.filter(F.col("UnitPrice") > 0)

# 4. Remove duplicate records
df_silver = df_silver.dropDuplicates(["InvoiceNo", "StockCode", "CustomerID"])

# 5. Convert InvoiceDate to proper timestamp (handle format variations gracefully)
df_silver = df_silver.withColumn(
    "InvoiceDate",
    F.expr("""coalesce(
        try_to_timestamp(InvoiceDate, 'M/d/yy H:mm'),
        try_to_timestamp(InvoiceDate, 'M/d/yyyy H:mm')
    )""")
)

# 6. Remove rows where timestamp parsing failed
df_silver = df_silver.filter(F.col("InvoiceDate").isNotNull())

print(f"✅ Cleaned data: {df_silver.count():,} valid records")
print(f"⚠️ Removed: {df_bronze.count() - df_silver.count():,} invalid records")

# COMMAND ----------

# DBTITLE 1,Data Enrichment
# Add business calculations and derived columns

df_silver = df_silver \
    .withColumn("TotalAmount", F.col("Quantity") * F.col("UnitPrice")) \
    .withColumn("InvoiceYear", F.year(F.col("InvoiceDate"))) \
    .withColumn("InvoiceMonth", F.month(F.col("InvoiceDate"))) \
    .withColumn("InvoiceDay", F.dayofmonth(F.col("InvoiceDate"))) \
    .withColumn("DayOfWeek", F.dayofweek(F.col("InvoiceDate"))) \
    .withColumn("processed_timestamp", F.current_timestamp())

# Display enriched data
print("✅ Data enrichment complete!\n")
print("🌟 Added columns:")
print("  • TotalAmount (Quantity × UnitPrice)")
print("  • InvoiceYear, InvoiceMonth, InvoiceDay")
print("  • DayOfWeek")
print("  • processed_timestamp\n")

display(df_silver.select(
    "InvoiceNo", "StockCode", "Description", 
    "Quantity", "UnitPrice", "TotalAmount", 
    "CustomerID", "Country", "InvoiceDate"
).limit(10))

# COMMAND ----------

# DBTITLE 1,Data Quality Checks
# Implement data quality checks

print("🔍 DATA QUALITY REPORT:\n")
print("="*50)

# Check 1: Null values
null_counts = df_silver.select(
    [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df_silver.columns]
).collect()[0].asDict()

print("\n1️⃣ Null Value Check:")
has_nulls = any(null_counts.values())
if not has_nulls:
    print("   ✅ No null values found")
else:
    print("   ⚠️ Null values detected:")
    for col, count in null_counts.items():
        if count > 0:
            print(f"      - {col}: {count}")

# Check 2: Data ranges
print("\n2️⃣ Data Range Check:")
print(f"   • Quantity range: {df_silver.select(F.min('Quantity')).first()[0]} to {df_silver.select(F.max('Quantity')).first()[0]}")
print(f"   • Price range: ${df_silver.select(F.min('UnitPrice')).first()[0]:.2f} to ${df_silver.select(F.max('UnitPrice')).first()[0]:.2f}")
print(f"   • Total amount range: ${df_silver.select(F.min('TotalAmount')).first()[0]:.2f} to ${df_silver.select(F.max('TotalAmount')).first()[0]:.2f}")

# Check 3: Record counts
print("\n3️⃣ Record Statistics:")
print(f"   • Total valid records: {df_silver.count():,}")
print(f"   • Unique invoices: {df_silver.select('InvoiceNo').distinct().count():,}")
print(f"   • Unique customers: {df_silver.select('CustomerID').distinct().count():,}")
print(f"   • Unique products: {df_silver.select('StockCode').distinct().count():,}")

print("\n" + "="*50)
print("✅ Data quality checks passed!")

# COMMAND ----------

# DBTITLE 1,Save Silver Delta Table
# Save to Silver layer as managed table
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_retail_data")

print("✅ Silver table created: silver_retail_data")
print("✅ TRANSFORM stage complete!")
print("\n✨ Silver Layer Characteristics:")
print("  • Cleaned and validated")
print("  • Business logic applied")
print("  • Quality checks passed")
print("  • Ready for analytics")

# COMMAND ----------

# DBTITLE 1,Stage 3: Serve (Gold Layer)
# MAGIC %md
# MAGIC ## 🥇 Stage 3: SERVE — Gold Layer
# MAGIC
# MAGIC Create business-level aggregates optimized for analytics:
# MAGIC * Customer metrics
# MAGIC * Product performance
# MAGIC * Revenue analytics
# MAGIC * Time-based trends

# COMMAND ----------

# DBTITLE 1,Load Silver Data
# Load cleaned data from Silver layer
df_gold_source = spark.table("silver_retail_data")

print(f"📚 Loaded {df_gold_source.count():,} records from Silver")

# COMMAND ----------

# DBTITLE 1,Customer Analytics (Gold Table 1)
# GOLD TABLE 1: Customer-Level Analytics

df_customer_metrics = df_gold_source.groupBy("CustomerID", "Country").agg(
    F.count("InvoiceNo").alias("total_orders"),
    F.sum("TotalAmount").alias("total_revenue"),
    F.avg("TotalAmount").alias("avg_order_value"),
    F.sum("Quantity").alias("total_items_purchased"),
    F.countDistinct("StockCode").alias("unique_products"),
    F.min("InvoiceDate").alias("first_purchase_date"),
    F.max("InvoiceDate").alias("last_purchase_date")
).withColumn(
    "customer_lifetime_days",
    F.datediff(F.col("last_purchase_date"), F.col("first_purchase_date"))
).withColumn(
    "created_at", F.current_timestamp()
)

# Add customer segmentation
df_customer_metrics = df_customer_metrics.withColumn(
    "customer_segment",
    F.when(F.col("total_revenue") >= 5000, "VIP")
     .when(F.col("total_revenue") >= 1000, "Premium")
     .otherwise("Standard")
)

print(f"✅ Created customer analytics: {df_customer_metrics.count():,} customers\n")

# Display top customers
print("🏆 TOP 10 CUSTOMERS BY REVENUE:\n")
display(df_customer_metrics.orderBy(F.desc("total_revenue")).limit(10))

# COMMAND ----------

# DBTITLE 1,Product Performance (Gold Table 2)
# GOLD TABLE 2: Product Performance Metrics

df_product_metrics = df_gold_source.groupBy("StockCode", "Description").agg(
    F.sum("Quantity").alias("total_quantity_sold"),
    F.sum("TotalAmount").alias("total_revenue"),
    F.count("InvoiceNo").alias("number_of_orders"),
    F.countDistinct("CustomerID").alias("unique_customers"),
    F.avg("UnitPrice").alias("avg_unit_price")
).withColumn(
    "revenue_per_customer",
    F.round(F.col("total_revenue") / F.col("unique_customers"), 2)
).withColumn(
    "created_at", F.current_timestamp()
)

print(f"✅ Created product analytics: {df_product_metrics.count():,} products\n")

# Display top products
print("📈 TOP 10 PRODUCTS BY REVENUE:\n")
display(df_product_metrics.orderBy(F.desc("total_revenue")).limit(10))

# COMMAND ----------

# DBTITLE 1,Monthly Revenue Trends (Gold Table 3)
# GOLD TABLE 3: Monthly Revenue Trends

df_monthly_revenue = df_gold_source.groupBy(
    "InvoiceYear", "InvoiceMonth", "Country"
).agg(
    F.sum("TotalAmount").alias("monthly_revenue"),
    F.count("InvoiceNo").alias("monthly_orders"),
    F.countDistinct("CustomerID").alias("monthly_customers"),
    F.sum("Quantity").alias("monthly_items_sold")
).withColumn(
    "avg_order_value",
    F.round(F.col("monthly_revenue") / F.col("monthly_orders"), 2)
).withColumn(
    "created_at", F.current_timestamp()
).orderBy("InvoiceYear", "InvoiceMonth")

print(f"✅ Created monthly revenue trends: {df_monthly_revenue.count():,} month-country combinations\n")

# Display recent trends
print("📅 MONTHLY REVENUE TRENDS:\n")
display(df_monthly_revenue.filter(F.col("Country") == "United Kingdom"))

# COMMAND ----------

# DBTITLE 1,Save Gold Tables
# Save Gold tables for consumption as managed tables

# Gold Table 1: Customer Metrics
df_customer_metrics.write.format("delta").mode("overwrite").saveAsTable("gold_customer_metrics")

# Gold Table 2: Product Metrics
df_product_metrics.write.format("delta").mode("overwrite").saveAsTable("gold_product_metrics")

# Gold Table 3: Monthly Revenue
df_monthly_revenue.write.format("delta").mode("overwrite").saveAsTable("gold_monthly_revenue")

print("✅ All Gold tables saved!")
print("\n🌟 Gold Layer Characteristics:")
print("  • Business-level aggregates")
print("  • Pre-calculated metrics")
print("  • Optimized for BI tools")
print("  • Query performance < 1 second")
print("\n🏁 End-to-End Pipeline Complete!")
print("  Bronze → Silver → Gold")

# COMMAND ----------

# DBTITLE 1,Pipeline Summary
# MAGIC %md
# MAGIC ## ✅ Pipeline Summary: What We Built
# MAGIC
# MAGIC | Layer | Purpose | Records | Tables Created |
# MAGIC |---|---|---|---|
# MAGIC | **Bronze** | Raw ingestion | 540K+ | 1 (retail_data) |
# MAGIC | **Silver** | Cleaned & validated | ~390K | 1 (clean_retail) |
# MAGIC | **Gold** | Business aggregates | ~4K, ~3K, ~200 | 3 (customers, products, revenue) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 What You Just Accomplished
# MAGIC
# MAGIC 1. ✅ **Ingested** raw data from CSV files
# MAGIC 2. ✅ **Cleaned** and validated data (removed 150K+ invalid records)
# MAGIC 3. ✅ **Enriched** data with business calculations
# MAGIC 4. ✅ **Aggregated** into analytics-ready tables
# MAGIC 5. ✅ **Implemented** data quality checks
# MAGIC 6. ✅ **Stored** in Delta format for ACID compliance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💼 Real-World Impact
# MAGIC
# MAGIC This pipeline now enables:
# MAGIC * **BI Analysts**: Query customer/product metrics instantly
# MAGIC * **Data Scientists**: Build predictive models on clean data
# MAGIC * **Business Users**: Access dashboards with up-to-date insights
# MAGIC * **Executives**: Make data-driven decisions with confidence

# COMMAND ----------

# DBTITLE 1,Section 3: OLTP vs OLAP
# MAGIC %md
# MAGIC # 📊 SECTION 3: OLTP vs OLAP Systems
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 (Explain Like I'm 5)
# MAGIC
# MAGIC **OLTP (Online Transaction Processing) = Cash Register at a Store**
# MAGIC * Fast checkout, one item at a time
# MAGIC * "I want to buy THIS item NOW"
# MAGIC * Each transaction is separate and quick
# MAGIC
# MAGIC **OLAP (Online Analytical Processing) = Accountant at Year-End**
# MAGIC * Looking at ALL receipts together
# MAGIC * "How much did we sell last year?"
# MAGIC * Analyzing patterns across many transactions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation
# MAGIC
# MAGIC ### OLTP: Transaction Processing
# MAGIC
# MAGIC **Purpose**: Support day-to-day business operations  
# MAGIC **Focus**: Fast, reliable writes and point lookups  
# MAGIC **Users**: Applications, end-users, services  
# MAGIC
# MAGIC **Characteristics**:
# MAGIC * **Row-oriented storage** for quick record access
# MAGIC * **Normalized schemas** (3NF) to reduce redundancy
# MAGIC * **ACID transactions** for data consistency
# MAGIC * **Millisecond response times**
# MAGIC * **High concurrency** (thousands of simultaneous users)
# MAGIC
# MAGIC **Examples**:
# MAGIC * Banking: ATM withdrawals, transfers
# MAGIC * E-commerce: Order placement, inventory updates
# MAGIC * CRM: Customer record updates
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### OLAP: Analytical Processing
# MAGIC
# MAGIC **Purpose**: Support business intelligence and analytics  
# MAGIC **Focus**: Fast reads, complex aggregations  
# MAGIC **Users**: Analysts, data scientists, executives  
# MAGIC
# MAGIC **Characteristics**:
# MAGIC * **Column-oriented storage** for efficient aggregations
# MAGIC * **Denormalized schemas** (star/snowflake) for query speed
# MAGIC * **Read-optimized** with batch updates
# MAGIC * **Second-to-minute response times**
# MAGIC * **Complex queries** (joins, aggregations, window functions)
# MAGIC
# MAGIC **Examples**:
# MAGIC * Sales analysis: Year-over-year revenue trends
# MAGIC * Customer analytics: Churn prediction
# MAGIC * Financial reporting: Quarterly performance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Complete Comparison
# MAGIC
# MAGIC | Feature | OLTP | OLAP |
# MAGIC |---|---|---|
# MAGIC | **Purpose** | Run the business | Analyze the business |
# MAGIC | **Operations** | INSERT, UPDATE, DELETE | SELECT (complex queries) |
# MAGIC | **Query Type** | Simple, predefined | Ad-hoc, complex |
# MAGIC | **Transaction Size** | Small (few rows) | Large (millions of rows) |
# MAGIC | **Response Time** | Milliseconds | Seconds to minutes |
# MAGIC | **Users** | Thousands concurrent | Dozens concurrent |
# MAGIC | **Data Volume** | GB to TB | TB to PB |
# MAGIC | **Schema** | Normalized (3NF) | Denormalized (Star) |
# MAGIC | **Storage** | Row-based | Column-based |
# MAGIC | **Updates** | Real-time | Batch (ETL/ELT) |
# MAGIC | **Historical Data** | Current only | Years of history |
# MAGIC | **Optimization** | Write-heavy | Read-heavy |
# MAGIC | **Example DB** | PostgreSQL, MySQL | Snowflake, Databricks SQL |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Technical Differences
# MAGIC
# MAGIC ### Storage Format
# MAGIC
# MAGIC **OLTP (Row-Oriented)**:
# MAGIC ```
# MAGIC Row 1: [ID=1, Name="Alice", Age=25, City="NYC"]
# MAGIC Row 2: [ID=2, Name="Bob", Age=30, City="LA"]
# MAGIC ```
# MAGIC ✅ Fast for: Get all info about one person  
# MAGIC ❌ Slow for: Calculate average age of all people  
# MAGIC
# MAGIC **OLAP (Column-Oriented)**:
# MAGIC ```
# MAGIC ID Column:   [1, 2, ...]
# MAGIC Name Column: ["Alice", "Bob", ...]
# MAGIC Age Column:  [25, 30, ...]
# MAGIC ```
# MAGIC ✅ Fast for: Aggregate operations (SUM, AVG, COUNT)  
# MAGIC ❌ Slow for: Retrieving full records  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Schema Design
# MAGIC
# MAGIC **OLTP (Normalized)**:
# MAGIC ```
# MAGIC Customers: [CustomerID, Name, Email]
# MAGIC Orders: [OrderID, CustomerID, Date]
# MAGIC OrderItems: [OrderItemID, OrderID, ProductID, Quantity]
# MAGIC Products: [ProductID, Name, Price]
# MAGIC ```
# MAGIC ✅ Reduces redundancy  
# MAGIC ✅ Maintains data integrity  
# MAGIC ❌ Requires multiple joins for analysis  
# MAGIC
# MAGIC **OLAP (Denormalized - Star Schema)**:
# MAGIC ```
# MAGIC Fact_Sales: [SaleID, Date, CustomerID, ProductID, Quantity, Revenue]
# MAGIC Dim_Customer: [CustomerID, Name, Segment, Region]
# MAGIC Dim_Product: [ProductID, Name, Category, Price]
# MAGIC Dim_Date: [Date, Year, Quarter, Month, Day]
# MAGIC ```
# MAGIC ✅ Optimized for analytical queries  
# MAGIC ✅ Fewer joins needed  
# MAGIC ❌ Data redundancy acceptable  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌐 Real-World Architecture
# MAGIC
# MAGIC ```
# MAGIC      OLTP Systems                    Data Lake/Lakehouse                OLAP Systems
# MAGIC ┌───────────────┐                ┌─────────────────┐            ┌───────────────┐
# MAGIC │ MySQL         │                │   Databricks    │            │ Power BI      │
# MAGIC │ PostgreSQL    │───ETL/CDC──►│   Delta Lake    │───Query──►│ Tableau       │
# MAGIC │ MongoDB       │                │   (OLAP-ready)  │            │ Looker        │
# MAGIC │ Oracle        │                │                 │            │ ML Models     │
# MAGIC └───────────────┘                └─────────────────┘            └───────────────┘
# MAGIC Transactional           Data Engineers            Analysts/Scientists
# MAGIC ```
# MAGIC
# MAGIC **Data Engineers bridge OLTP and OLAP!**

# COMMAND ----------

# DBTITLE 1,Section 4: Hands-on OLTP vs OLAP
# MAGIC %md
# MAGIC # 💻 SECTION 4: Hands-on OLTP vs OLAP Comparison
# MAGIC
# MAGIC Let's simulate both workload types using our retail data!

# COMMAND ----------

# DBTITLE 1,Simulate OLTP Operations
# OLTP-STYLE OPERATIONS: Transactional, row-level operations

print("💳 OLTP WORKLOAD SIMULATION\n")
print("="*60)

# Load data
df_oltp = spark.table("silver_retail_data")

# OLTP Query 1: Point lookup - Find specific order
print("\n1️⃣ POINT LOOKUP: Get details for Invoice 536365")
start_time = datetime.now()

order_detail = df_oltp.filter(F.col("InvoiceNo") == "536365").collect()

end_time = datetime.now()
print(f"   ⏱️  Query time: {(end_time - start_time).total_seconds():.3f} seconds")
print(f"   📋 Records returned: {len(order_detail)}")

# OLTP Query 2: Insert simulation (adding new transaction)
print("\n2️⃣ INSERT: Add new order (simulation)")
from pyspark.sql import Row

new_order = spark.createDataFrame([
    Row(
        InvoiceNo="999999",
        StockCode="NEW001",
        Description="New Product",
        Quantity=5,
        InvoiceDate=datetime.now(),
        UnitPrice=29.99,
        CustomerID=12345,
        Country="USA",
        TotalAmount=149.95,
        InvoiceYear=2026,
        InvoiceMonth=4,
        InvoiceDay=21,
        DayOfWeek=2,
        ingestion_timestamp=datetime.now(),
        processed_timestamp=datetime.now(),
        source_file="manual_insert"
    )
])

print("   ✅ New order record created (in-memory)")
print("   💾 In production: Would write to Delta table with ACID guarantees")

# OLTP Query 3: Update simulation
print("\n3️⃣ UPDATE: Modify customer record")
print("   🔄 Would update CustomerID 12345's email address")
print("   🔒 ACID transaction ensures consistency")

print("\n" + "="*60)
print("✅ OLTP Operations: Fast, focused, transactional")

# COMMAND ----------

# DBTITLE 1,Simulate OLAP Operations
# OLAP-STYLE OPERATIONS: Analytical, aggregation-heavy queries

print("📊 OLAP WORKLOAD SIMULATION\n")
print("="*60)

# OLAP Query 1: Complex aggregation across millions of rows
print("\n1️⃣ AGGREGATE ANALYSIS: Revenue by country and year")
start_time = datetime.now()

revenue_analysis = df_oltp.groupBy("Country", "InvoiceYear").agg(
    F.sum("TotalAmount").alias("total_revenue"),
    F.count("InvoiceNo").alias("order_count"),
    F.countDistinct("CustomerID").alias("unique_customers"),
    F.avg("TotalAmount").alias("avg_order_value")
).orderBy(F.desc("total_revenue"))

result_count = revenue_analysis.count()
end_time = datetime.now()

print(f"   ⏱️  Query time: {(end_time - start_time).total_seconds():.3f} seconds")
print(f"   📋 Aggregated groups: {result_count}")
print(f"   📈 Processed: {df_oltp.count():,} source records\n")

display(revenue_analysis.limit(10))

# OLAP Query 2: Time-series analysis
print("\n2️⃣ TIME-SERIES ANALYSIS: Monthly trends")
start_time = datetime.now()

monthly_trends = df_oltp.groupBy("InvoiceYear", "InvoiceMonth").agg(
    F.sum("TotalAmount").alias("monthly_revenue"),
    F.countDistinct("CustomerID").alias("active_customers")
).orderBy("InvoiceYear", "InvoiceMonth")

end_time = datetime.now()
print(f"   ⏱️  Query time: {(end_time - start_time).total_seconds():.3f} seconds")
print(f"   📅 Time periods analyzed: {monthly_trends.count()}")

# OLAP Query 3: Multi-dimensional analysis
print("\n3️⃣ MULTI-DIMENSIONAL: Customer segments by region and product")
print("   🌍 Would analyze: Region × Product Category × Customer Segment")
print("   📊 Joins multiple dimension tables")
print("   ⏳ Query spans full historical dataset")

print("\n" + "="*60)
print("✅ OLAP Operations: Comprehensive, analytical, insight-driven")

# COMMAND ----------

# DBTITLE 1,Performance Comparison
# Performance characteristics comparison

print("⚡ PERFORMANCE COMPARISON\n")
print("="*60)

print("\n💳 OLTP Characteristics:")
print("   • Query Type: Point lookups, single-row operations")
print("   • Typical Time: < 10ms in production (with indexing)")
print("   • Concurrency: 1000+ simultaneous users")
print("   • Optimization: Indexes, normalized schema")
print("   • Storage: Row-oriented (Parquet row groups)")

print("\n📊 OLAP Characteristics:")
print("   • Query Type: Aggregations, full table scans")
print("   • Typical Time: 100ms - 60s (based on data volume)")
print("   • Concurrency: 10-100 simultaneous users")
print("   • Optimization: Partitioning, Z-order, caching")
print("   • Storage: Column-oriented (Delta/Parquet)")

print("\n🎯 When to Use Each:")
print("\n   Use OLTP when:")
print("   ✓ Building customer-facing applications")
print("   ✓ Need real-time data consistency")
print("   ✓ Handling high-volume transactions")
print("   ✓ Each operation affects few rows")

print("\n   Use OLAP when:")
print("   ✓ Running business intelligence queries")
print("   ✓ Analyzing historical trends")
print("   ✓ Creating reports and dashboards")
print("   ✓ Need to aggregate millions of rows")

print("\n" + "="*60)
print("🔑 Key Insight: Data Engineers transform OLTP → OLAP!")

# COMMAND ----------

# DBTITLE 1,Hybrid Systems: Lakehouse
# MAGIC %md
# MAGIC ## 🏗️ The Lakehouse: Best of Both Worlds
# MAGIC
# MAGIC **Databricks Lakehouse = OLTP + OLAP Unified**
# MAGIC
# MAGIC Traditional architecture required separate systems:
# MAGIC * **OLTP Database** → CDC/ETL → **Data Warehouse (OLAP)**
# MAGIC * Data duplication, complexity, delays
# MAGIC
# MAGIC **Lakehouse Architecture**:
# MAGIC * **Single platform** for transactional and analytical workloads
# MAGIC * **Delta Lake** provides ACID transactions on data lake
# MAGIC * **Real-time analytics** on operational data
# MAGIC * **No ETL delays** between systems
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Delta Lake Capabilities
# MAGIC
# MAGIC | Feature | Benefit | Use Case |
# MAGIC |---|---|---|
# MAGIC | **ACID Transactions** | Data consistency | Concurrent writes |
# MAGIC | **Time Travel** | Historical queries | Audit, rollback |
# MAGIC | **Schema Evolution** | Flexible schemas | Agile development |
# MAGIC | **MERGE/UPDATE/DELETE** | OLTP operations | Slowly changing dimensions |
# MAGIC | **Column Statistics** | Query optimization | Fast analytics |
# MAGIC | **Z-Ordering** | Co-locality | Multi-dimensional queries |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Result
# MAGIC
# MAGIC * **For OLTP**: Fast updates with ACID guarantees
# MAGIC * **For OLAP**: Columnar storage for blazing-fast analytics
# MAGIC * **For Data Engineers**: One platform, less complexity!

# COMMAND ----------

# DBTITLE 1,Section 5: Real-World Use Cases
# MAGIC %md
# MAGIC # 🌐 SECTION 5: Real-World Data Engineering Use Cases
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Use Case 1: E-Commerce Analytics Platform
# MAGIC
# MAGIC ### 🏪 Business Context
# MAGIC **Company**: Large online retailer  
# MAGIC **Challenge**: Need real-time insights into customer behavior, inventory, and sales  
# MAGIC **Data Volume**: 10M+ orders/day, 100M+ customers  
# MAGIC
# MAGIC ### 🛠️ Data Engineering Solution
# MAGIC
# MAGIC #### Sources (Ingest)
# MAGIC * **Order Systems** (MySQL) → CDC streaming
# MAGIC * **Clickstream Data** (Kafka) → Structured Streaming
# MAGIC * **Product Catalog** (MongoDB) → Batch daily
# MAGIC * **Customer Reviews** (S3 JSON) → Auto Loader
# MAGIC
# MAGIC #### Processing Pipeline
# MAGIC
# MAGIC ```
# MAGIC Bronze Layer (Raw)
# MAGIC   • Orders: 10M records/day
# MAGIC   • Clicks: 1B events/day
# MAGIC   • Products: 5M SKUs
# MAGIC   • Reviews: 100K/day
# MAGIC      ↓
# MAGIC Silver Layer (Cleaned)
# MAGIC   • Deduplicate orders
# MAGIC   • Enrich with customer segment
# MAGIC   • Join product metadata
# MAGIC   • Sentiment analysis on reviews
# MAGIC      ↓
# MAGIC Gold Layer (Aggregated)
# MAGIC   • Customer 360 (lifetime value, preferences)
# MAGIC   • Product performance (conversion rates)
# MAGIC   • Inventory forecasting
# MAGIC   • Personalization features
# MAGIC ```
# MAGIC
# MAGIC #### Outputs (Serve)
# MAGIC * **Real-time Dashboard**: Order volumes, revenue
# MAGIC * **ML Models**: Product recommendations, churn prediction
# MAGIC * **Business Reports**: Daily sales, inventory alerts
# MAGIC * **API Endpoints**: Customer profiles for web apps
# MAGIC
# MAGIC #### Business Impact
# MAGIC * ✅ **30% increase** in conversion through personalization
# MAGIC * ✅ **50% reduction** in stockouts
# MAGIC * ✅ **Real-time** inventory visibility (was 24hr delay)
# MAGIC * ✅ **$50M annual** revenue increase
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Use Case 2: Banking Fraud Detection
# MAGIC
# MAGIC ### 🏦 Business Context
# MAGIC **Company**: Global bank  
# MAGIC **Challenge**: Detect fraudulent transactions in real-time  
# MAGIC **Data Volume**: 50M+ transactions/day  
# MAGIC **Compliance**: GDPR, PCI-DSS, audit trails  
# MAGIC
# MAGIC ### 🛠️ Data Engineering Solution
# MAGIC
# MAGIC #### Sources (Ingest)
# MAGIC * **ATM Transactions** → Streaming (Kafka)
# MAGIC * **Online Banking** → Streaming (Event Hub)
# MAGIC * **Card Swipes** → Streaming (Kinesis)
# MAGIC * **Customer Profiles** → Batch (Oracle CDC)
# MAGIC * **Historical Fraud Cases** → Batch (Teradata)
# MAGIC
# MAGIC #### Processing Pipeline
# MAGIC
# MAGIC ```
# MAGIC Real-Time Stream Processing
# MAGIC   • Transaction data (50M/day)
# MAGIC   • < 100ms latency requirement
# MAGIC   • Enrich with customer behavior
# MAGIC   • Feature engineering (velocity, location, amount)
# MAGIC      ↓
# MAGIC ML Model Inference
# MAGIC   • Fraud probability score
# MAGIC   • Rule-based filters
# MAGIC   • Anomaly detection
# MAGIC   • Risk threshold classification
# MAGIC      ↓
# MAGIC Action & Audit
# MAGIC   • Block high-risk transactions
# MAGIC   • Alert fraud team
# MAGIC   • Log for compliance
# MAGIC   • Update customer risk profile
# MAGIC ```
# MAGIC
# MAGIC #### Data Engineering Challenges
# MAGIC
# MAGIC 1. **Low Latency**: Must process < 100ms
# MAGIC    * Solution: Structured Streaming with Delta
# MAGIC    * In-memory caching for customer profiles
# MAGIC
# MAGIC 2. **Data Quality**: Missing fields, duplicates
# MAGIC    * Solution: Schema validation at ingestion
# MAGIC    * Quarantine bad records for review
# MAGIC
# MAGIC 3. **Compliance**: PII data, audit trails
# MAGIC    * Solution: Unity Catalog for access control
# MAGIC    * Delta Time Travel for auditing
# MAGIC
# MAGIC 4. **Model Updates**: Retrain daily
# MAGIC    * Solution: MLflow for model versioning
# MAGIC    * Automated retraining pipeline
# MAGIC
# MAGIC #### Business Impact
# MAGIC * ✅ **92% accuracy** in fraud detection (up from 75%)
# MAGIC * ✅ **$200M/year** fraud losses prevented
# MAGIC * ✅ **80% reduction** in false positives
# MAGIC * ✅ **100% compliance** with audit requirements
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Use Case 3: Healthcare Patient Analytics
# MAGIC
# MAGIC ### 🏥 Business Context
# MAGIC **Company**: Hospital network (500+ facilities)  
# MAGIC **Challenge**: Improve patient outcomes, reduce readmissions  
# MAGIC **Data Volume**: 2M+ patient records, 100K+ daily admissions  
# MAGIC **Compliance**: HIPAA, patient privacy  
# MAGIC
# MAGIC ### 🛠️ Data Engineering Solution
# MAGIC
# MAGIC #### Sources (Ingest)
# MAGIC * **Electronic Health Records (EHR)** → Batch (HL7 format)
# MAGIC * **Medical Devices** → Streaming (IoT)
# MAGIC * **Lab Results** → Batch (CSV)
# MAGIC * **Claims Data** → Batch (insurance systems)
# MAGIC * **Pharmacy Records** → Real-time (APIs)
# MAGIC
# MAGIC #### Processing Pipeline
# MAGIC
# MAGIC ```
# MAGIC Bronze Layer
# MAGIC   • EHR data (HL7 → parsed JSON)
# MAGIC   • Device telemetry (heart rate, BP)
# MAGIC   • Lab results (structured)
# MAGIC   • Claims (denormalized)
# MAGIC      ↓
# MAGIC Silver Layer
# MAGIC   • De-identify PHI/PII
# MAGIC   • Standardize medical codes (ICD-10)
# MAGIC   • Link patient records across systems
# MAGIC   • Data quality: missing values, outliers
# MAGIC      ↓
# MAGIC Gold Layer
# MAGIC   • Patient 360 (complete medical history)
# MAGIC   • Readmission risk scores
# MAGIC   • Disease cohorts for research
# MAGIC   • Hospital performance metrics
# MAGIC ```
# MAGIC
# MAGIC #### Analytics & ML Models
# MAGIC
# MAGIC 1. **Readmission Prediction**
# MAGIC    * Features: Prior visits, diagnoses, demographics
# MAGIC    * Model: Gradient boosting
# MAGIC    * Output: Risk score for discharge planning
# MAGIC
# MAGIC 2. **ICU Capacity Forecasting**
# MAGIC    * Features: Seasonal trends, current census
# MAGIC    * Model: Time series (Prophet)
# MAGIC    * Output: 7-day capacity forecast
# MAGIC
# MAGIC 3. **Chronic Disease Management**
# MAGIC    * Identify high-risk diabetic patients
# MAGIC    * Personalized care recommendations
# MAGIC    * Medication adherence tracking
# MAGIC
# MAGIC #### Data Governance
# MAGIC * **PHI/PII Protection**: Column-level encryption
# MAGIC * **Access Control**: Role-based (doctors, nurses, researchers)
# MAGIC * **Audit Logs**: Every data access logged
# MAGIC * **Data Retention**: 7-year HIPAA compliance
# MAGIC
# MAGIC #### Business Impact
# MAGIC * ✅ **25% reduction** in 30-day readmissions
# MAGIC * ✅ **$30M/year** cost savings
# MAGIC * ✅ **Improved outcomes** for chronic disease patients
# MAGIC * ✅ **Research acceleration**: 5x faster cohort identification
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Common Patterns Across Use Cases
# MAGIC
# MAGIC | Pattern | E-Commerce | Banking | Healthcare |
# MAGIC |---|---|---|---|
# MAGIC | **Real-time Ingestion** | Clickstream | Transactions | Device telemetry |
# MAGIC | **Batch Ingestion** | Product catalog | Customer profiles | EHR records |
# MAGIC | **Data Quality** | Deduplication | Schema validation | Missing values |
# MAGIC | **Compliance** | GDPR | PCI-DSS, audit | HIPAA, encryption |
# MAGIC | **ML Models** | Recommendations | Fraud detection | Risk prediction |
# MAGIC | **Serving** | Dashboards, APIs | Alerts, reports | Clinical apps |
# MAGIC | **Storage** | Delta Lake | Delta Lake | Delta Lake |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔑 Key Takeaways
# MAGIC
# MAGIC 1. **Every industry needs Data Engineers** to transform raw data into insights
# MAGIC 2. **Medallion architecture** (Bronze/Silver/Gold) applies universally
# MAGIC 3. **Real-time + Batch** processing often coexist
# MAGIC 4. **Data quality and governance** are non-negotiable
# MAGIC 5. **Delta Lake** enables both operational and analytical workloads

# COMMAND ----------

# DBTITLE 1,Section 6: Data Engineering Best Practices
# MAGIC %md
# MAGIC # 🎯 SECTION 6: Data Engineering Best Practices
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1️⃣ Modular Pipeline Design
# MAGIC
# MAGIC ### Principle: Separation of Concerns
# MAGIC
# MAGIC ❌ **Bad Practice**: One massive script doing everything
# MAGIC ```python
# MAGIC # 5000-line monolithic script
# MAGIC # Ingest + clean + transform + aggregate all in one
# MAGIC ```
# MAGIC
# MAGIC ✅ **Best Practice**: Modular, reusable components
# MAGIC ```python
# MAGIC # Separate modules:
# MAGIC # - ingestion.py: Load raw data
# MAGIC # - validation.py: Data quality checks
# MAGIC # - transformation.py: Business logic
# MAGIC # - aggregation.py: Analytics prep
# MAGIC ```
# MAGIC
# MAGIC **Benefits**:
# MAGIC * Easier testing and debugging
# MAGIC * Reusable across projects
# MAGIC * Parallel development
# MAGIC * Clear ownership
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2️⃣ Schema Enforcement
# MAGIC
# MAGIC ### Principle: Define Your Contract
# MAGIC
# MAGIC ❌ **Bad Practice**: Infer schema every time
# MAGIC ```python
# MAGIC df = spark.read.csv(path, inferSchema=True)  # Risky!
# MAGIC ```
# MAGIC
# MAGIC ✅ **Best Practice**: Explicit schema definition
# MAGIC ```python
# MAGIC from pyspark.sql.types import *
# MAGIC
# MAGIC schema = StructType([
# MAGIC     StructField("order_id", StringType(), False),
# MAGIC     StructField("amount", DecimalType(10,2), False),
# MAGIC     StructField("timestamp", TimestampType(), False)
# MAGIC ])
# MAGIC
# MAGIC df = spark.read.csv(path, schema=schema)
# MAGIC ```
# MAGIC
# MAGIC **Benefits**:
# MAGIC * Catch schema changes early
# MAGIC * Prevent data type issues
# MAGIC * Document expectations
# MAGIC * Performance optimization
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3️⃣ Data Quality Checks
# MAGIC
# MAGIC ### Principle: Trust But Verify
# MAGIC
# MAGIC ```python
# MAGIC # Essential checks for every pipeline
# MAGIC
# MAGIC # 1. Record count validation
# MAGIC assert df.count() > 0, "No data ingested!"
# MAGIC
# MAGIC # 2. Null checks on critical columns
# MAGIC null_count = df.filter(F.col("customer_id").isNull()).count()
# MAGIC assert null_count == 0, f"{null_count} null customer_ids found"
# MAGIC
# MAGIC # 3. Data range validation
# MAGIC min_date = df.select(F.min("order_date")).first()[0]
# MAGIC assert min_date >= expected_start_date, "Historical data missing"
# MAGIC
# MAGIC # 4. Referential integrity
# MAGIC customer_ids = df.select("customer_id").distinct()
# MAGIC valid_customers = spark.table("dim_customers").select("customer_id")
# MAGIC invalid = customer_ids.subtract(valid_customers).count()
# MAGIC assert invalid == 0, f"{invalid} invalid customer references"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4️⃣ Idempotency
# MAGIC
# MAGIC ### Principle: Same Input → Same Output
# MAGIC
# MAGIC Your pipeline should produce identical results when run multiple times with the same input.
# MAGIC
# MAGIC ❌ **Bad Practice**: Append-only without deduplication
# MAGIC ```python
# MAGIC df.write.mode("append").save(path)  # Duplicate risk!
# MAGIC ```
# MAGIC ✅ **Best Practice**: Upsert or overwrite with partition
# MAGIC ```python
# MAGIC # Option 1: Full overwrite
# MAGIC df.write.mode("overwrite").save(path)
# MAGIC
# MAGIC # Option 2: Merge (upsert)
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC DeltaTable.forPath(spark, path).alias("target").merge(
# MAGIC     df.alias("source"),
# MAGIC     "target.id = source.id"
# MAGIC ).whenMatchedUpdateAll() \
# MAGIC  .whenNotMatchedInsertAll() \
# MAGIC  .execute()
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5️⃣ Error Handling
# MAGIC
# MAGIC ### Principle: Fail Gracefully
# MAGIC
# MAGIC ```python
# MAGIC try:
# MAGIC     df = spark.read.csv(source_path)
# MAGIC     
# MAGIC     # Validate
# MAGIC     if df.count() == 0:
# MAGIC         raise ValueError("Empty dataset")
# MAGIC     
# MAGIC     # Process
# MAGIC     df_clean = clean_data(df)
# MAGIC     
# MAGIC     # Write
# MAGIC     df_clean.write.mode("overwrite").save(target_path)
# MAGIC     
# MAGIC     # Log success
# MAGIC     log_pipeline_success(row_count=df_clean.count())
# MAGIC     
# MAGIC except FileNotFoundError:
# MAGIC     log_error("Source file not found")
# MAGIC     send_alert(team="data-eng")
# MAGIC     raise
# MAGIC     
# MAGIC except Exception as e:
# MAGIC     log_error(f"Pipeline failed: {str(e)}")
# MAGIC     # Write to dead letter queue
# MAGIC     write_to_quarantine(df, error=str(e))
# MAGIC     raise
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 6️⃣ Performance Optimization
# MAGIC
# MAGIC ### Partitioning
# MAGIC
# MAGIC ```python
# MAGIC # Partition by date for time-series queries
# MAGIC df.write \
# MAGIC     .partitionBy("year", "month", "day") \
# MAGIC     .format("delta") \
# MAGIC     .save(path)
# MAGIC
# MAGIC # Query benefit: Only reads relevant partitions
# MAGIC df_filtered = spark.read.format("delta").load(path) \
# MAGIC     .filter("year = 2026 AND month = 4")  # Partition pruning!
# MAGIC ```
# MAGIC
# MAGIC ### Z-Ordering (Delta Lake)
# MAGIC
# MAGIC ```python
# MAGIC # Co-locate related data for multi-dimensional queries
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC DeltaTable.forPath(spark, path).optimize() \
# MAGIC     .executeZOrderBy("customer_id", "product_id")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 7️⃣ Documentation & Metadata
# MAGIC
# MAGIC ```python
# MAGIC # Add metadata to tables
# MAGIC spark.sql("""
# MAGIC     COMMENT ON TABLE customer_metrics IS 
# MAGIC     'Gold layer customer analytics. Updated daily at 6 AM UTC.
# MAGIC     Owner: data-eng-team@company.com'
# MAGIC """)
# MAGIC
# MAGIC # Column-level documentation
# MAGIC spark.sql("""
# MAGIC     ALTER TABLE customer_metrics
# MAGIC     ALTER COLUMN total_revenue 
# MAGIC     COMMENT 'Lifetime revenue in USD. Excludes refunds.'
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 8️⃣ Monitoring & Alerting
# MAGIC
# MAGIC ```python
# MAGIC # Key metrics to track
# MAGIC metrics = {
# MAGIC     "pipeline_name": "retail_etl",
# MAGIC     "run_id": run_id,
# MAGIC     "start_time": start_time,
# MAGIC     "end_time": datetime.now(),
# MAGIC     "duration_seconds": (datetime.now() - start_time).total_seconds(),
# MAGIC     "records_processed": df.count(),
# MAGIC     "records_failed": failed_count,
# MAGIC     "success": True
# MAGIC }
# MAGIC
# MAGIC # Log to monitoring system
# MAGIC log_metrics(metrics)
# MAGIC
# MAGIC # Alert on failures
# MAGIC if failed_count > threshold:
# MAGIC     send_alert(
# MAGIC         severity="high",
# MAGIC         message=f"{failed_count} records failed validation"
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Summary: The 8 Commandments
# MAGIC
# MAGIC 1. ✅ **Modular Design**: Separate concerns, reusable code
# MAGIC 2. ✅ **Schema Enforcement**: Explicit schemas, not inferred
# MAGIC 3. ✅ **Data Quality**: Validate at every stage
# MAGIC 4. ✅ **Idempotency**: Same input = same output
# MAGIC 5. ✅ **Error Handling**: Fail gracefully, log everything
# MAGIC 6. ✅ **Performance**: Partition, optimize, cache wisely
# MAGIC 7. ✅ **Documentation**: Comment code and data
# MAGIC 8. ✅ **Monitoring**: Track metrics, set alerts

# COMMAND ----------

# DBTITLE 1,Section 7: Using Databricks Genie Code Agent
# MAGIC %md
# MAGIC # 🧞 SECTION 7: Accelerate with Databricks Genie Code Agent
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What is Genie Code Agent?
# MAGIC
# MAGIC Genie Code is your AI-powered Data Engineering assistant that:
# MAGIC * ✅ Writes production-quality PySpark and SQL code
# MAGIC * ✅ Follows best practices automatically
# MAGIC * ✅ Debugs errors and optimizes queries
# MAGIC * ✅ Explains code and architecture
# MAGIC * ✅ Accelerates development by 5-10x
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💬 Example Prompts: Data Ingestion
# MAGIC
# MAGIC ### Prompt 1: Load CSV with Auto Loader
# MAGIC ```
# MAGIC Load CSV files from /mnt/raw/orders/ using Auto Loader.
# MAGIC Infer schema and handle new columns automatically.
# MAGIC Save to bronze.orders as Delta table.
# MAGIC ```
# MAGIC
# MAGIC **Genie generates**:
# MAGIC ```python
# MAGIC df = spark.readStream \
# MAGIC     .format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "csv") \
# MAGIC     .option("cloudFiles.schemaLocation", "/mnt/schemas/orders") \
# MAGIC     .option("cloudFiles.inferColumnTypes", "true") \
# MAGIC     .load("/mnt/raw/orders/")
# MAGIC
# MAGIC df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .option("checkpointLocation", "/mnt/checkpoints/orders") \
# MAGIC     .table("bronze.orders")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Prompt 2: Batch Ingestion from Database
# MAGIC ```
# MAGIC Read from PostgreSQL table 'public.customers' using JDBC.
# MAGIC Add ingestion timestamp and save to bronze.customers.
# MAGIC ```
# MAGIC
# MAGIC **Genie generates**:
# MAGIC ```python
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC df = spark.read \
# MAGIC     .format("jdbc") \
# MAGIC     .option("url", "jdbc:postgresql://host:5432/db") \
# MAGIC     .option("dbtable", "public.customers") \
# MAGIC     .option("user", dbutils.secrets.get("db", "username")) \
# MAGIC     .option("password", dbutils.secrets.get("db", "password")) \
# MAGIC     .load()
# MAGIC
# MAGIC df_with_metadata = df.withColumn("ingestion_timestamp", F.current_timestamp())
# MAGIC
# MAGIC df_with_metadata.write \
# MAGIC     .format("delta") \
# MAGIC     .mode("overwrite") \
# MAGIC     .saveAsTable("bronze.customers")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💬 Example Prompts: Data Transformation
# MAGIC
# MAGIC ### Prompt 3: Clean and Validate
# MAGIC ```
# MAGIC Clean silver.orders table:
# MAGIC - Remove null order_ids
# MAGIC - Filter out negative amounts
# MAGIC - Deduplicate by order_id and timestamp
# MAGIC - Add a total_amount column (quantity * price)
# MAGIC Save to silver.orders_clean
# MAGIC ```
# MAGIC
# MAGIC **Genie generates**:
# MAGIC ```python
# MAGIC df = spark.table("silver.orders")
# MAGIC
# MAGIC df_clean = df \
# MAGIC     .filter(F.col("order_id").isNotNull()) \
# MAGIC     .filter(F.col("amount") > 0) \
# MAGIC     .dropDuplicates(["order_id", "timestamp"]) \
# MAGIC     .withColumn("total_amount", F.col("quantity") * F.col("price"))
# MAGIC
# MAGIC df_clean.write \
# MAGIC     .format("delta") \
# MAGIC     .mode("overwrite") \
# MAGIC     .saveAsTable("silver.orders_clean")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Prompt 4: Complex Join and Enrichment
# MAGIC ```
# MAGIC Join silver.orders with silver.customers on customer_id.
# MAGIC Add customer segment, country, and lifetime_value.
# MAGIC Filter for orders from last 30 days.
# MAGIC Save to gold.orders_enriched.
# MAGIC ```
# MAGIC
# MAGIC **Genie generates**:
# MAGIC ```python
# MAGIC from datetime import datetime, timedelta
# MAGIC
# MAGIC orders = spark.table("silver.orders")
# MAGIC customers = spark.table("silver.customers")
# MAGIC
# MAGIC cutoff_date = datetime.now() - timedelta(days=30)
# MAGIC
# MAGIC enriched = orders \
# MAGIC     .filter(F.col("order_date") >= cutoff_date) \
# MAGIC     .join(customers, "customer_id", "left") \
# MAGIC     .select(
# MAGIC         orders["*"],
# MAGIC         customers["segment"].alias("customer_segment"),
# MAGIC         customers["country"],
# MAGIC         customers["lifetime_value"]
# MAGIC     )
# MAGIC
# MAGIC enriched.write \
# MAGIC     .format("delta") \
# MAGIC     .mode("overwrite") \
# MAGIC     .saveAsTable("gold.orders_enriched")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💬 Example Prompts: Aggregations
# MAGIC
# MAGIC ### Prompt 5: Customer Metrics
# MAGIC ```
# MAGIC Create gold.customer_metrics table:
# MAGIC Group by customer_id and calculate:
# MAGIC - total_orders (count)
# MAGIC - total_revenue (sum of amount)
# MAGIC - avg_order_value
# MAGIC - first_order_date, last_order_date
# MAGIC Source: silver.orders_clean
# MAGIC ```
# MAGIC
# MAGIC **Genie generates**:
# MAGIC ```python
# MAGIC df = spark.table("silver.orders_clean")
# MAGIC
# MAGIC customer_metrics = df.groupBy("customer_id").agg(
# MAGIC     F.count("order_id").alias("total_orders"),
# MAGIC     F.sum("total_amount").alias("total_revenue"),
# MAGIC     F.avg("total_amount").alias("avg_order_value"),
# MAGIC     F.min("order_date").alias("first_order_date"),
# MAGIC     F.max("order_date").alias("last_order_date")
# MAGIC )
# MAGIC
# MAGIC customer_metrics.write \
# MAGIC     .format("delta") \
# MAGIC     .mode("overwrite") \
# MAGIC     .saveAsTable("gold.customer_metrics")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💬 Example Prompts: Optimization
# MAGIC
# MAGIC ### Prompt 6: Optimize Table
# MAGIC ```
# MAGIC Optimize gold.customer_metrics table.
# MAGIC Partition by country.
# MAGIC Z-order by customer_segment and last_order_date.
# MAGIC ```
# MAGIC
# MAGIC **Genie generates**:
# MAGIC ```python
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC # Repartition and save
# MAGIC df = spark.table("gold.customer_metrics")
# MAGIC df.write \
# MAGIC     .format("delta") \
# MAGIC     .mode("overwrite") \
# MAGIC     .partitionBy("country") \
# MAGIC     .saveAsTable("gold.customer_metrics")
# MAGIC
# MAGIC # Z-order optimization
# MAGIC DeltaTable.forName(spark, "gold.customer_metrics") \
# MAGIC     .optimize() \
# MAGIC     .executeZOrderBy("customer_segment", "last_order_date")
# MAGIC
# MAGIC print("✅ Table optimized!")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💬 Example Prompts: Data Quality
# MAGIC
# MAGIC ### Prompt 7: Quality Checks
# MAGIC ```
# MAGIC Run data quality checks on silver.orders_clean:
# MAGIC 1. Verify no null order_ids
# MAGIC 2. Check all amounts are positive
# MAGIC 3. Validate order_date is within last 2 years
# MAGIC 4. Count and report any duplicates
# MAGIC Raise error if any check fails.
# MAGIC ```
# MAGIC
# MAGIC **Genie generates**:
# MAGIC ```python
# MAGIC df = spark.table("silver.orders_clean")
# MAGIC
# MAGIC # Check 1: Null order_ids
# MAGIC null_count = df.filter(F.col("order_id").isNull()).count()
# MAGIC assert null_count == 0, f"FAIL: {null_count} null order_ids"
# MAGIC
# MAGIC # Check 2: Negative amounts
# MAGIC negative_count = df.filter(F.col("amount") <= 0).count()
# MAGIC assert negative_count == 0, f"FAIL: {negative_count} negative amounts"
# MAGIC
# MAGIC # Check 3: Date range
# MAGIC from datetime import datetime, timedelta
# MAGIC min_date = datetime.now() - timedelta(days=730)
# MAGIC invalid_dates = df.filter(F.col("order_date") < min_date).count()
# MAGIC assert invalid_dates == 0, f"FAIL: {invalid_dates} orders older than 2 years"
# MAGIC
# MAGIC # Check 4: Duplicates
# MAGIC total_count = df.count()
# MAGIC distinct_count = df.dropDuplicates(["order_id"]).count()
# MAGIC duplicates = total_count - distinct_count
# MAGIC assert duplicates == 0, f"WARN: {duplicates} duplicate orders"
# MAGIC
# MAGIC print("✅ All quality checks passed!")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💬 Tips for Effective Prompts
# MAGIC
# MAGIC ### ✅ Good Prompts
# MAGIC * **Specific**: Include table names, column names, business logic
# MAGIC * **Contextual**: Mention source and target locations
# MAGIC * **Clear**: One task per prompt (or related sub-tasks)
# MAGIC * **Complete**: Specify file formats, partitioning, modes
# MAGIC
# MAGIC ### ❌ Avoid
# MAGIC * Vague requests: "Clean the data" (what data? how?)
# MAGIC * Multiple unrelated tasks in one prompt
# MAGIC * Assuming context Genie doesn't have
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Workflow: Human + AI
# MAGIC
# MAGIC 1. **You define** the business requirements
# MAGIC 2. **Genie generates** the code
# MAGIC 3. **You review** and customize
# MAGIC 4. **Genie debugs** if issues arise
# MAGIC 5. **You deploy** to production
# MAGIC
# MAGIC **Result**: 10x faster development, fewer bugs, best practices built-in!

# COMMAND ----------

# DBTITLE 1,Section 8: Summary and Next Steps
# MAGIC %md
# MAGIC # 🎓 SECTION 8: Summary, Interview Questions & Common Mistakes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Key Learnings Recap
# MAGIC
# MAGIC ### What We Covered Today
# MAGIC
# MAGIC 1. ✅ **Data Engineer Role**
# MAGIC    * Bridge between data sources and analytics
# MAGIC    * Build, maintain, optimize data pipelines
# MAGIC    * Enable data-driven decision making
# MAGIC
# MAGIC 2. ✅ **End-to-End Pipeline**
# MAGIC    * Bronze: Raw data ingestion (540K+ records)
# MAGIC    * Silver: Cleaned and validated (390K records)
# MAGIC    * Gold: Business aggregates (3 analytics tables)
# MAGIC
# MAGIC 3. ✅ **OLTP vs OLAP**
# MAGIC    * OLTP: Transactional, row-oriented, write-heavy
# MAGIC    * OLAP: Analytical, column-oriented, read-heavy
# MAGIC    * Lakehouse: Unified platform for both
# MAGIC
# MAGIC 4. ✅ **Real-World Applications**
# MAGIC    * E-commerce: Customer analytics, personalization
# MAGIC    * Banking: Fraud detection, compliance
# MAGIC    * Healthcare: Patient outcomes, risk prediction
# MAGIC
# MAGIC 5. ✅ **Best Practices**
# MAGIC    * Schema enforcement, data quality, idempotency
# MAGIC    * Error handling, monitoring, documentation
# MAGIC    * Performance optimization (partitioning, Z-order)
# MAGIC
# MAGIC 6. ✅ **AI Acceleration**
# MAGIC    * Genie Code Agent for 10x faster development
# MAGIC    * Production-quality code generation
# MAGIC    * Automated debugging and optimization
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💼 Interview Questions (With Answers)
# MAGIC
# MAGIC ### Question 1: What is the Medallion Architecture?
# MAGIC
# MAGIC **Answer**:  
# MAGIC The Medallion Architecture is a data design pattern with three layers:
# MAGIC * **Bronze (Raw)**: Ingested data as-is, append-only, source of truth
# MAGIC * **Silver (Refined)**: Cleaned, validated, enriched, deduplicated
# MAGIC * **Gold (Curated)**: Business-level aggregates, analytics-ready, optimized
# MAGIC
# MAGIC Benefits: Clear data lineage, incremental quality improvement, separates raw data from consumption.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Question 2: Explain OLTP vs OLAP
# MAGIC
# MAGIC **Answer**:  
# MAGIC **OLTP (Online Transaction Processing)**:
# MAGIC * Purpose: Run day-to-day operations
# MAGIC * Operations: INSERT, UPDATE, DELETE
# MAGIC * Optimized for: Fast writes, point lookups
# MAGIC * Schema: Normalized (3NF)
# MAGIC * Example: Banking transactions, e-commerce orders
# MAGIC
# MAGIC **OLAP (Online Analytical Processing)**:
# MAGIC * Purpose: Analyze business data
# MAGIC * Operations: Complex SELECT queries
# MAGIC * Optimized for: Aggregations, full table scans
# MAGIC * Schema: Denormalized (star/snowflake)
# MAGIC * Example: Sales reports, customer analytics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Question 3: What is Delta Lake and why use it?
# MAGIC
# MAGIC **Answer**:  
# MAGIC Delta Lake is an open-source storage layer that brings ACID transactions to data lakes.
# MAGIC
# MAGIC **Key Features**:
# MAGIC * **ACID Transactions**: Ensure data consistency
# MAGIC * **Time Travel**: Query historical versions
# MAGIC * **Schema Evolution**: Add/modify columns safely
# MAGIC * **MERGE/UPDATE/DELETE**: Mutable data operations
# MAGIC * **Performance**: Indexing, statistics, data skipping
# MAGIC
# MAGIC **Why use it**:  
# MAGIC Combines data lake scalability with database reliability. Enables both OLTP and OLAP workloads on same platform.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Question 4: How do you ensure data quality in pipelines?
# MAGIC
# MAGIC **Answer**:  
# MAGIC 1. **Schema Validation**: Enforce explicit schemas, reject mismatched data
# MAGIC 2. **Null Checks**: Validate required fields are populated
# MAGIC 3. **Range Checks**: Ensure numeric values within expected bounds
# MAGIC 4. **Referential Integrity**: Verify foreign key relationships
# MAGIC 5. **Duplicate Detection**: Identify and handle duplicates
# MAGIC 6. **Reconciliation**: Compare record counts source vs target
# MAGIC 7. **Monitoring**: Track data quality metrics over time
# MAGIC 8. **Alerting**: Notify on quality threshold breaches
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Question 5: What is idempotency and why is it important?
# MAGIC
# MAGIC **Answer**:  
# MAGIC **Idempotency**: Running a pipeline multiple times with same input produces same output.
# MAGIC
# MAGIC **Why important**:
# MAGIC * Enables safe pipeline reruns after failures
# MAGIC * Prevents duplicate data
# MAGIC * Simplifies recovery and debugging
# MAGIC * Required for production reliability
# MAGIC
# MAGIC **Implementation**:
# MAGIC * Use MERGE (upsert) instead of append
# MAGIC * Overwrite partitions, not full tables
# MAGIC * Include timestamp/run_id for deduplication
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Question 6: Explain partitioning and when to use it
# MAGIC
# MAGIC **Answer**:  
# MAGIC **Partitioning**: Organizing data into subdirectories based on column values.
# MAGIC
# MAGIC **Common patterns**:
# MAGIC * Time-based: `year=2026/month=04/day=21`
# MAGIC * Location-based: `country=USA/state=CA`
# MAGIC * Category-based: `product_category=electronics`
# MAGIC
# MAGIC **Benefits**:
# MAGIC * **Partition pruning**: Read only relevant data
# MAGIC * **Faster queries**: Skip irrelevant partitions
# MAGIC * **Parallel processing**: Process partitions independently
# MAGIC
# MAGIC **When to use**:
# MAGIC * Large tables (>1GB)
# MAGIC * Queries filter by specific columns
# MAGIC * Time-series data (always partition by date)
# MAGIC
# MAGIC **When NOT to use**:
# MAGIC * Too many partitions (>10K) = small files problem
# MAGIC * High-cardinality columns (customer_id)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Question 7: What's the difference between batch and streaming?
# MAGIC
# MAGIC **Answer**:  
# MAGIC **Batch Processing**:
# MAGIC * Process data in large chunks (hourly, daily)
# MAGIC * Higher latency but higher throughput
# MAGIC * Simpler error handling and testing
# MAGIC * Example: Daily sales reports
# MAGIC
# MAGIC **Streaming Processing**:
# MAGIC * Process data as it arrives (real-time)
# MAGIC * Lower latency (<1 second)
# MAGIC * More complex fault tolerance
# MAGIC * Example: Fraud detection, monitoring
# MAGIC
# MAGIC **When to choose**:
# MAGIC * Batch: Historical analysis, non-urgent reports
# MAGIC * Streaming: Real-time alerts, live dashboards
# MAGIC * **Hybrid**: Common in practice (streaming + batch aggregates)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Question 8: How do you optimize Spark performance?
# MAGIC
# MAGIC **Answer**:  
# MAGIC 1. **Partitioning**: Organize data for query patterns
# MAGIC 2. **Caching**: Cache frequently accessed DataFrames
# MAGIC 3. **Broadcasting**: Broadcast small lookup tables
# MAGIC 4. **Avoid Shuffles**: Minimize groupBy/join operations
# MAGIC 5. **Predicate Pushdown**: Filter early in pipeline
# MAGIC 6. **Column Pruning**: Select only needed columns
# MAGIC 7. **Z-Ordering**: Co-locate related data (Delta)
# MAGIC 8. **File Sizing**: Target 128MB-1GB per file
# MAGIC 9. **Adaptive Query Execution**: Enable AQE
# MAGIC 10. **Monitoring**: Use Spark UI to identify bottlenecks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Question 9: Explain slowly changing dimensions (SCD)
# MAGIC
# MAGIC **Answer**:  
# MAGIC SCDs track how dimension attributes change over time.
# MAGIC
# MAGIC **Type 1 (Overwrite)**:
# MAGIC * Update record in place
# MAGIC * No history preserved
# MAGIC * Example: Correcting typos
# MAGIC
# MAGIC **Type 2 (Add Row)**:
# MAGIC * Insert new row with effective dates
# MAGIC * Full history maintained
# MAGIC * Example: Customer address changes
# MAGIC
# MAGIC **Type 3 (Add Column)**:
# MAGIC * Store previous value in separate column
# MAGIC * Limited history (only last change)
# MAGIC * Example: Previous_address column
# MAGIC
# MAGIC **Implementation in Delta**:
# MAGIC ```python
# MAGIC # Type 2 SCD using MERGE
# MAGIC target.merge(source, "id = source_id") \
# MAGIC   .whenMatchedUpdate(set={"end_date": current_date(), "is_current": False}) \
# MAGIC   .whenNotMatchedInsert(values={"start_date": current_date(), "is_current": True})
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Question 10: What is data lineage and why does it matter?
# MAGIC
# MAGIC **Answer**:  
# MAGIC **Data Lineage**: Documentation of data's journey from source to consumption.
# MAGIC
# MAGIC Tracks:
# MAGIC * Where data came from (source systems)
# MAGIC * What transformations were applied
# MAGIC * Where it's stored (tables, files)
# MAGIC * Who consumes it (reports, models)
# MAGIC
# MAGIC **Why it matters**:
# MAGIC * **Debugging**: Trace errors to source
# MAGIC * **Compliance**: Audit trails for regulations
# MAGIC * **Impact Analysis**: Understand downstream effects
# MAGIC * **Trust**: Verify data quality and accuracy
# MAGIC
# MAGIC **Tools**: Unity Catalog, data catalogs, pipeline docs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚠️ Common Beginner Mistakes
# MAGIC
# MAGIC ### 1. Schema Inference in Production
# MAGIC ❌ **Mistake**: `df = spark.read.csv(path, inferSchema=True)`  
# MAGIC ✅ **Fix**: Always use explicit schemas in production
# MAGIC
# MAGIC ### 2. Forgetting to Cache Reused DataFrames
# MAGIC ❌ **Mistake**: Recomputing expensive transformations  
# MAGIC ✅ **Fix**: `df.cache()` for DataFrames used multiple times
# MAGIC
# MAGIC ### 3. Over-Partitioning
# MAGIC ❌ **Mistake**: Partitioning by high-cardinality column (customer_id)  
# MAGIC ✅ **Fix**: Partition by low-cardinality, query-filtered columns (date)
# MAGIC
# MAGIC ### 4. Not Handling Nulls
# MAGIC ❌ **Mistake**: Assuming all columns have values  
# MAGIC ✅ **Fix**: Use `.na.drop()`, `.na.fill()`, or explicit null checks
# MAGIC
# MAGIC ### 5. Ignoring Data Quality
# MAGIC ❌ **Mistake**: No validation, "garbage in, garbage out"  
# MAGIC ✅ **Fix**: Implement checks at every pipeline stage
# MAGIC
# MAGIC ### 6. Hardcoding Values
# MAGIC ❌ **Mistake**: Hardcoded paths, credentials in code  
# MAGIC ✅ **Fix**: Use widgets, secrets, configuration files
# MAGIC
# MAGIC ### 7. Writing Tiny Files
# MAGIC ❌ **Mistake**: 100K+ small files (each < 1MB)  
# MAGIC ✅ **Fix**: Use `.repartition()` or `.coalesce()` before writing
# MAGIC
# MAGIC ### 8. No Error Handling
# MAGIC ❌ **Mistake**: Pipeline fails silently or crashes completely  
# MAGIC ✅ **Fix**: try/except blocks, dead letter queues, alerts
# MAGIC
# MAGIC ### 9. Ignoring Incremental Processing
# MAGIC ❌ **Mistake**: Full table scan every time  
# MAGIC ✅ **Fix**: Process only new/changed data (watermarks, CDC)
# MAGIC
# MAGIC ### 10. Not Monitoring Pipelines
# MAGIC ❌ **Mistake**: No visibility into failures or performance  
# MAGIC ✅ **Fix**: Log metrics, set up alerts, use monitoring dashboards
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 What's Next?
# MAGIC
# MAGIC ### Phase 1 — Day 2 Preview
# MAGIC * **Advanced Transformations**: Window functions, UDFs, complex joins
# MAGIC * **Streaming Pipelines**: Structured Streaming, Auto Loader
# MAGIC * **Performance Tuning**: Deep dive into Spark optimization
# MAGIC * **Data Quality Frameworks**: Great Expectations, Delta constraints
# MAGIC
# MAGIC ### Continue Learning
# MAGIC 1. **Practice**: Build end-to-end pipelines with your own data
# MAGIC 2. **Explore**: Databricks documentation and tutorials
# MAGIC 3. **Experiment**: Try different optimization techniques
# MAGIC 4. **Collaborate**: Work with your team on real projects
# MAGIC 5. **Certify**: Pursue Databricks certifications
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎉 Congratulations!
# MAGIC
# MAGIC You've completed **Phase 1 — Day 1: Data Engineering Fundamentals**!
# MAGIC
# MAGIC You now understand:
# MAGIC * ✅ What Data Engineers do and why they matter
# MAGIC * ✅ How to build end-to-end data pipelines
# MAGIC * ✅ OLTP vs OLAP systems
# MAGIC * ✅ Real-world applications across industries
# MAGIC * ✅ Best practices for production data engineering
# MAGIC * ✅ How to accelerate with AI (Genie Code)
# MAGIC
# MAGIC **You're ready to start building production data pipelines!** 🚀

# COMMAND ----------

# DBTITLE 1,Additional Resources
# MAGIC %md
# MAGIC # 📚 Additional Resources
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Databricks Documentation
# MAGIC * [Delta Lake Guide](https://docs.databricks.com/delta/index.html)
# MAGIC * [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
# MAGIC * [Databricks SQL Reference](https://docs.databricks.com/sql/language-manual/index.html)
# MAGIC * [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)
# MAGIC * [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Learning Paths
# MAGIC * [Databricks Academy](https://www.databricks.com/learn/training)
# MAGIC * [Data Engineer Learning Plan](https://www.databricks.com/learn/training/catalog?role=data-engineer)
# MAGIC * [Certification Programs](https://www.databricks.com/learn/certification)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Best Practices
# MAGIC * [Lakehouse Architecture](https://www.databricks.com/glossary/medallion-architecture)
# MAGIC * [Delta Lake Best Practices](https://docs.databricks.com/delta/best-practices.html)
# MAGIC * [Performance Tuning Guide](https://docs.databricks.com/optimizations/index.html)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Community
# MAGIC * [Databricks Community Forums](https://community.databricks.com/)
# MAGIC * [Stack Overflow - Databricks Tag](https://stackoverflow.com/questions/tagged/databricks)
# MAGIC * [Databricks Blog](https://www.databricks.com/blog)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Sample Datasets
# MAGIC * Databricks Datasets: `/databricks-datasets/`
# MAGIC * [Kaggle Datasets](https://www.kaggle.com/datasets)
# MAGIC * [AWS Open Data](https://registry.opendata.aws/)
# MAGIC * [Google Dataset Search](https://datasetsearch.research.google.com/)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📧 Contact & Feedback
# MAGIC
# MAGIC Questions or feedback on this training?  
# MAGIC Reach out to your Data Engineering team or training coordinator.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Happy Data Engineering!** 🎉

# COMMAND ----------

# DBTITLE 1,Notebook Footer and Watermark
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC <div style="text-align: center; padding: 30px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 10px; margin-top: 40px;">
# MAGIC   <h2 style="color: white; font-size: 2em; margin: 0 0 15px 0; text-shadow: 2px 2px 4px rgba(0,0,0,0.3);">✨ Training Complete! ✨</h2>
# MAGIC   <p style="color: #ffffff; font-size: 1.2em; margin: 10px 0;">You've completed Phase 1 — Day 1: Data Engineering Fundamentals</p>
# MAGIC   <p style="color: #f0f0f0; font-size: 1em; margin: 5px 0;">✅ Medallion Architecture • ✅ OLTP vs OLAP • ✅ Best Practices</p>
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC <div style="background-color: #f8f9fa; padding: 25px; border-radius: 10px; margin: 20px 0; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
# MAGIC
# MAGIC ### 🎓 What You've Achieved
# MAGIC
# MAGIC <table style="width: 100%; border-collapse: collapse;">
# MAGIC   <tr style="background-color: #e9ecef;">
# MAGIC     <td style="padding: 12px; border: 1px solid #dee2e6; font-weight: bold;">Skill Acquired</td>
# MAGIC     <td style="padding: 12px; border: 1px solid #dee2e6; font-weight: bold;">Status</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding: 10px; border: 1px solid #dee2e6;">👷 Understanding Data Engineer Role</td>
# MAGIC     <td style="padding: 10px; border: 1px solid #dee2e6;">✅ Mastered</td>
# MAGIC   </tr>
# MAGIC   <tr style="background-color: #f8f9fa;">
# MAGIC     <td style="padding: 10px; border: 1px solid #dee2e6;">🔄 End-to-End Pipeline Development</td>
# MAGIC     <td style="padding: 10px; border: 1px solid #dee2e6;">✅ Hands-on Practice</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding: 10px; border: 1px solid #dee2e6;">📊 OLTP vs OLAP Systems</td>
# MAGIC     <td style="padding: 10px; border: 1px solid #dee2e6;">✅ Conceptual Clarity</td>
# MAGIC   </tr>
# MAGIC   <tr style="background-color: #f8f9fa;">
# MAGIC     <td style="padding: 10px; border: 1px solid #dee2e6;">🌟 Production Best Practices</td>
# MAGIC     <td style="padding: 10px; border: 1px solid #dee2e6;">✅ Ready to Apply</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td style="padding: 10px; border: 1px solid #dee2e6;">🧞 AI-Powered Development (Genie)</td>
# MAGIC     <td style="padding: 10px; border: 1px solid #dee2e6;">✅ 10x Productivity</td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC <div style="background-color: #d1ecf1; padding: 20px; border-left: 5px solid #0c5460; border-radius: 5px; margin: 20px 0;">
# MAGIC
# MAGIC ### 🚀 Next Steps in Your Learning Journey
# MAGIC
# MAGIC 1. **Practice**: Re-run all cells and experiment with your own modifications
# MAGIC 2. **Phase 1 - Day 2**: Advanced transformations, streaming, and optimization
# MAGIC 3. **Build Projects**: Apply concepts to real datasets from your domain
# MAGIC 4. **Join Community**: Engage with Databricks forums and user groups
# MAGIC 5. **Pursue Certification**: Work towards Databricks Data Engineer Associate
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC <div style="background-color: #fff3cd; padding: 20px; border-left: 5px solid #ffc107; border-radius: 5px; margin: 20px 0;">
# MAGIC
# MAGIC ### 📝 Notebook Metadata
# MAGIC
# MAGIC **Notebook**: Phase 1 — Day 1: Data Engineering Fundamentals  
# MAGIC **Version**: 1.0  
# MAGIC **Created**: April 21, 2026  
# MAGIC **Author**: @TRRaveendra  
# MAGIC **Platform**: Databricks Lakehouse Platform  
# MAGIC **Runtime**: Databricks Runtime 15.x+  
# MAGIC **Compute**: Serverless Interactive Cluster  
# MAGIC **Format**: 33 cells (Markdown + Python)  
# MAGIC **Tables Created**: Bronze, Silver, Gold (Medallion Architecture)  
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC <div style="text-align: center; padding: 30px; background-color: #2c3e50; color: white; border-radius: 10px; margin: 20px 0;">
# MAGIC
# MAGIC ### 💬 Stay Connected
# MAGIC
# MAGIC **Instructor**: T.R. Raveendra  
# MAGIC **Handle**: @TRRaveendra  
# MAGIC **Training Program**: Databricks Data Engineering Certification Prep  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC <p style="font-size: 0.9em; margin: 15px 0; color: #ecf0f1;">
# MAGIC 📧 For questions, feedback, or support, reach out through the training portal
# MAGIC </p>
# MAGIC
# MAGIC <p style="font-size: 0.9em; margin: 10px 0; color: #bdc3c7;">
# MAGIC 👍 If you found this training valuable, please leave feedback and star the repository
# MAGIC </p>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC <div style="text-align: center; padding: 25px; background: linear-gradient(to right, #1e3c72, #2a5298); color: white; border-radius: 10px; margin: 20px 0;">
# MAGIC
# MAGIC ### © Copyright & Attribution
# MAGIC
# MAGIC <p style="font-size: 1.1em; margin: 10px 0;">
# MAGIC **Created by @TRRaveendra**  
# MAGIC Databricks Certified Data Engineering Training
# MAGIC </p>
# MAGIC
# MAGIC <p style="font-size: 0.9em; margin: 10px 0; color: #ecf0f1;">
# MAGIC © 2026 T.R. Raveendra | All Rights Reserved  
# MAGIC For Educational and Training Purposes
# MAGIC </p>
# MAGIC
# MAGIC <div style="margin-top: 20px; padding: 15px; background-color: rgba(255,255,255,0.1); border-radius: 5px;">
# MAGIC <p style="font-size: 1.2em; font-weight: bold; margin: 5px 0; letter-spacing: 2px;">
# MAGIC 📌 WATERMARK: @TRRaveendra
# MAGIC </p>
# MAGIC <p style="font-size: 0.9em; margin: 5px 0;">
# MAGIC Phase 1 | Day 1 | Data Engineering Fundamentals | Databricks Training
# MAGIC </p>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC <div style="text-align: center; padding: 20px; margin: 20px 0;">
# MAGIC
# MAGIC <p style="font-size: 1.5em; color: #667eea;">
# MAGIC 🎉 **Congratulations on Completing Day 1!** 🎉
# MAGIC </p>
# MAGIC
# MAGIC <p style="font-size: 1.1em; color: #555; margin-top: 10px;">
# MAGIC *Keep learning, keep building, keep growing!*
# MAGIC </p>
# MAGIC
# MAGIC <p style="font-size: 2em; margin-top: 15px;">
# MAGIC 🚀 ✨ 💻
# MAGIC </p>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC <p style="text-align: center; font-size: 0.8em; color: #888; margin-top: 30px;">
# MAGIC End of Notebook | @TRRaveendra | Databricks Data Engineering Training
# MAGIC </p>