# Databricks notebook source
# DBTITLE 1,Section 16: Common Mistakes & How to Avoid Them
# MAGIC %md
# MAGIC # ⚠️ Section 16: Common Mistakes & How to Avoid Them
# MAGIC
# MAGIC ## 🚫 Top 10 Mistakes Engineers Make
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 1️⃣ Using Batch Read Instead of Auto Loader
# MAGIC
# MAGIC **Mistake:**
# MAGIC ```python
# MAGIC # ❌ BAD - Batch read, scans all files every time
# MAGIC df = spark.read.format("json").load("/path/to/files/")
# MAGIC ```
# MAGIC
# MAGIC **Correct:**
# MAGIC ```python
# MAGIC # ✅ GOOD - Auto Loader, incremental processing
# MAGIC df = spark.readStream.format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "json") \
# MAGIC     .load("/path/to/files/")
# MAGIC ```
# MAGIC
# MAGIC **Why it matters:** Batch read rescans ALL files, wasting compute and money.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ Forgetting checkpointLocation
# MAGIC
# MAGIC **Mistake:**
# MAGIC ```python
# MAGIC # ❌ BAD - No checkpoint, will fail or reprocess all data
# MAGIC query = df.writeStream.format("delta").start("/path/to/output")
# MAGIC ```
# MAGIC
# MAGIC **Correct:**
# MAGIC ```python
# MAGIC # ✅ GOOD - Checkpoint ensures exactly-once semantics
# MAGIC query = df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .option("checkpointLocation", "/path/to/checkpoint/") \
# MAGIC     .start("/path/to/output")
# MAGIC ```
# MAGIC
# MAGIC **Why it matters:** Without checkpoint, you lose fault tolerance and risk duplicates.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ Sharing Checkpoint Locations
# MAGIC
# MAGIC **Mistake:**
# MAGIC ```python
# MAGIC # ❌ BAD - Two streams sharing same checkpoint
# MAGIC stream1 = df1.writeStream.option("checkpointLocation", "/shared/checkpoint/")...
# MAGIC stream2 = df2.writeStream.option("checkpointLocation", "/shared/checkpoint/")...
# MAGIC ```
# MAGIC
# MAGIC **Correct:**
# MAGIC ```python
# MAGIC # ✅ GOOD - Each stream has unique checkpoint
# MAGIC stream1 = df1.writeStream.option("checkpointLocation", "/checkpoint/stream1/")...
# MAGIC stream2 = df2.writeStream.option("checkpointLocation", "/checkpoint/stream2/")...
# MAGIC ```
# MAGIC
# MAGIC **Why it matters:** Shared checkpoints cause state corruption and data loss.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4️⃣ Not Tracking Source File Metadata
# MAGIC
# MAGIC **Mistake:**
# MAGIC ```python
# MAGIC # ❌ BAD - No lineage tracking
# MAGIC df = spark.readStream.format("cloudFiles")...
# MAGIC df.writeStream.format("delta").start(...)
# MAGIC ```
# MAGIC
# MAGIC **Correct:**
# MAGIC ```python
# MAGIC # ✅ GOOD - Track source file for lineage
# MAGIC df = spark.readStream.format("cloudFiles")... \
# MAGIC     .select("*", col("_metadata.file_path").alias("source_file"))
# MAGIC df.writeStream.format("delta").start(...)
# MAGIC ```
# MAGIC
# MAGIC **Why it matters:** Without lineage, debugging data issues is nearly impossible.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5️⃣ Ignoring Schema Evolution
# MAGIC
# MAGIC **Mistake:**
# MAGIC ```python
# MAGIC # ❌ BAD - No schema evolution handling
# MAGIC df = spark.readStream.format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "json") \
# MAGIC     .load("/path/")
# MAGIC ```
# MAGIC
# MAGIC **Correct:**
# MAGIC ```python
# MAGIC # ✅ GOOD - Handle schema changes gracefully
# MAGIC df = spark.readStream.format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "json") \
# MAGIC     .option("cloudFiles.inferColumnTypes", "true") \
# MAGIC     .option("cloudFiles.schemaLocation", "/schema/path/") \
# MAGIC     .option("cloudFiles.schemaEvolutionMode", "rescue") \
# MAGIC     .load("/path/")
# MAGIC ```
# MAGIC
# MAGIC **Why it matters:** Schema changes will break your pipeline without proper handling.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6️⃣ Using Local or /tmp Paths
# MAGIC
# MAGIC **Mistake:**
# MAGIC ```python
# MAGIC # ❌ BAD - Local paths don't work in distributed environment
# MAGIC source_path = "/tmp/data/"
# MAGIC checkpoint_path = "/tmp/checkpoint/"
# MAGIC ```
# MAGIC
# MAGIC **Correct:**
# MAGIC ```python
# MAGIC # ✅ GOOD - Use cloud storage or Unity Catalog Volumes
# MAGIC source_path = "/Volumes/catalog/schema/volume/data/"
# MAGIC checkpoint_path = "/Volumes/catalog/schema/volume/checkpoint/"
# MAGIC ```
# MAGIC
# MAGIC **Why it matters:** Local paths are not accessible across cluster nodes.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 7️⃣ Not Monitoring Rescued Data
# MAGIC
# MAGIC **Mistake:**
# MAGIC ```python
# MAGIC # ❌ BAD - Enable rescue but never check it
# MAGIC .option("cloudFiles.schemaEvolutionMode", "rescue")
# MAGIC # ... no monitoring of _rescued_data column
# MAGIC ```
# MAGIC
# MAGIC **Correct:**
# MAGIC ```python
# MAGIC # ✅ GOOD - Monitor data quality
# MAGIC .option("cloudFiles.schemaEvolutionMode", "rescue")
# MAGIC
# MAGIC # Later: Monitor rescued records
# MAGIC rescued_df = spark.read.format("delta").load(bronze_path) \
# MAGIC     .filter(col("_rescued_data").isNotNull())
# MAGIC
# MAGIC if rescued_df.count() > threshold:
# MAGIC     send_alert("High rescued data count!")
# MAGIC ```
# MAGIC
# MAGIC **Why it matters:** Rescued data indicates data quality issues that need attention.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 8️⃣ Reprocessing All Files During Backfills
# MAGIC
# MAGIC **Mistake:**
# MAGIC ```python
# MAGIC # ❌ BAD - No rate limiting during backfill
# MAGIC df = spark.readStream.format("cloudFiles")...
# MAGIC # Processes all million files at once, overwhelming cluster
# MAGIC ```
# MAGIC
# MAGIC **Correct:**
# MAGIC ```python
# MAGIC # ✅ GOOD - Rate limit during backfill
# MAGIC df = spark.readStream.format("cloudFiles") \
# MAGIC     .option("cloudFiles.maxFilesPerTrigger", "100") \
# MAGIC     .option("cloudFiles.maxBytesPerTrigger", "10g") \
# MAGIC     ...
# MAGIC ```
# MAGIC
# MAGIC **Why it matters:** Prevents cluster overload and allows gradual processing.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 9️⃣ Using Directory Listing for Large-Scale Ingestion
# MAGIC
# MAGIC **Mistake:**
# MAGIC ```python
# MAGIC # ❌ BAD - Directory listing with millions of files
# MAGIC df = spark.readStream.format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "json") \
# MAGIC     .load("/millions/of/files/")
# MAGIC # Slow file discovery, high latency
# MAGIC ```
# MAGIC
# MAGIC **Correct:**
# MAGIC ```python
# MAGIC # ✅ GOOD - Use file notifications for scale
# MAGIC df = spark.readStream.format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "json") \
# MAGIC     .option("cloudFiles.useNotifications", "true") \
# MAGIC     .load("/millions/of/files/")
# MAGIC ```
# MAGIC
# MAGIC **Why it matters:** Directory listing doesn't scale beyond thousands of files.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔟 Using cache() or persist() with Streaming
# MAGIC
# MAGIC **Mistake:**
# MAGIC ```python
# MAGIC # ❌ BAD - Caching streaming DataFrames
# MAGIC df = spark.readStream.format("cloudFiles")...
# MAGIC df.cache()  # This doesn't work with streaming!
# MAGIC ```
# MAGIC
# MAGIC **Correct:**
# MAGIC ```python
# MAGIC # ✅ GOOD - No caching needed, streaming is already optimized
# MAGIC df = spark.readStream.format("cloudFiles")...
# MAGIC df.writeStream.format("delta").start(...)
# MAGIC ```
# MAGIC
# MAGIC **Why it matters:** Caching is not supported and not needed for streaming queries.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛡️ Additional Pitfalls:
# MAGIC
# MAGIC ### ⚠️ Modifying Files After Ingestion
# MAGIC **Problem:** Auto Loader tracks files by path + modification time. Modifying a file after ingestion causes reprocessing.
# MAGIC
# MAGIC **Solution:** Use immutable file patterns (append-only, never modify/delete).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Not Setting mergeSchema
# MAGIC **Problem:** Schema changes in source can break writes to Delta.
# MAGIC
# MAGIC **Solution:** Always add:
# MAGIC ```python
# MAGIC .option("mergeSchema", "true")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Using Wrong File Formats
# MAGIC **Problem:** Specifying wrong format causes parsing failures.
# MAGIC
# MAGIC **Solution:** Ensure `cloudFiles.format` matches actual files:
# MAGIC ```python
# MAGIC .option("cloudFiles.format", "json")  # Match your actual format
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Not Handling CSV Headers
# MAGIC **Problem:** CSV files without header option cause column name issues.
# MAGIC
# MAGIC **Solution:** Always specify for CSV:
# MAGIC ```python
# MAGIC .option("cloudFiles.format", "csv") \
# MAGIC .option("header", "true")  # or "false" if no header
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Deleting Checkpoint Directories
# MAGIC **Problem:** Deleting checkpoint causes full reprocessing of all files.
# MAGIC
# MAGIC **Solution:** 
# MAGIC * Never delete checkpoints in production
# MAGIC * If you must restart, archive the old checkpoint first
# MAGIC * Consider the cost of reprocessing all data
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Quick Checklist for Production Auto Loader:
# MAGIC
# MAGIC - [ ] Using `cloudFiles` format (not batch read)
# MAGIC - [ ] Unique `checkpointLocation` per stream
# MAGIC - [ ] Schema location configured
# MAGIC - [ ] Schema evolution mode set
# MAGIC - [ ] Source file metadata captured
# MAGIC - [ ] Rescued data monitoring enabled
# MAGIC - [ ] Rate limiting configured (if needed)
# MAGIC - [ ] File notifications for scale (> 1K files)
# MAGIC - [ ] Using cloud storage or Unity Catalog Volumes
# MAGIC - [ ] `mergeSchema = true` in write options
# MAGIC - [ ] Monitoring and alerting configured
# MAGIC - [ ] Error handling implemented
# MAGIC - [ ] Documentation updated
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Remember:
# MAGIC
# MAGIC > "The best Auto Loader pipeline is one that handles failures gracefully, tracks its lineage, and scales effortlessly."
# MAGIC
# MAGIC ✨ **Auto Loader is not just about reading files—it's about building resilient, scalable, production-grade data pipelines!**

# COMMAND ----------

# DBTITLE 1,Conclusion & Next Steps
# MAGIC %md
# MAGIC # 🎉 Conclusion & Next Steps
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎓 What You've Learned:
# MAGIC
# MAGIC Congratulations! You now understand:
# MAGIC
# MAGIC ✅ **Auto Loader fundamentals** and architecture  
# MAGIC ✅ **Incremental ingestion** patterns  
# MAGIC ✅ **File detection mechanisms** (listing vs notifications)  
# MAGIC ✅ **Schema inference and evolution**  
# MAGIC ✅ **Metadata tracking** for data lineage  
# MAGIC ✅ **Production best practices**  
# MAGIC ✅ **Common pitfalls** and how to avoid them  
# MAGIC ✅ **Performance optimization** techniques  
# MAGIC ✅ **Monitoring and observability**  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Next Steps:
# MAGIC
# MAGIC ### 1️⃣ Practice:
# MAGIC * Build Auto Loader pipelines with your own data
# MAGIC * Experiment with different file formats
# MAGIC * Test schema evolution scenarios
# MAGIC * Monitor rescued data patterns
# MAGIC
# MAGIC ### 2️⃣ Advanced Topics to Explore:
# MAGIC * **Spark Declarative Pipelines (SDP)** - Formerly Delta Live Tables
# MAGIC * **Change Data Capture (CDC)** with Auto Loader
# MAGIC * **Multi-hop Architecture** (Bronze/Silver/Gold)
# MAGIC * **Data Quality Rules** and expectations
# MAGIC * **Stream-Stream Joins**
# MAGIC * **Watermarking** for late data
# MAGIC
# MAGIC ### 3️⃣ Integration:
# MAGIC * Unity Catalog governance
# MAGIC * Workflow orchestration
# MAGIC * Monitoring and alerting systems
# MAGIC * CI/CD for data pipelines
# MAGIC
# MAGIC ### 4️⃣ Resources:
# MAGIC * [Databricks Auto Loader Documentation](https://docs.databricks.com/ingestion/auto-loader/index.html)
# MAGIC * Databricks Academy courses
# MAGIC * Community forums and best practices
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💬 Feedback & Questions:
# MAGIC
# MAGIC This notebook is part of **Phase 5 Day 26** of the Data Engineering Training series.
# MAGIC
# MAGIC **Author:** TRRaveendra  
# MAGIC **Watermark:** @TRRaveendra  
# MAGIC **Platform:** Databricks  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌟 Final Thought:
# MAGIC
# MAGIC > *"Auto Loader transforms file ingestion from a complex engineering challenge into a simple, scalable, and reliable operation. Master it, and you master modern data engineering."*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✨ Happy Learning! ✨
# MAGIC
# MAGIC 👍 Remember: **The best way to learn is by doing. Start building your Auto Loader pipelines today!**

# COMMAND ----------

# DBTITLE 1,Section 11: Auto Loader for CSV Files
# MAGIC %md
# MAGIC # 📊 Section 11: Auto Loader for CSV Files
# MAGIC
# MAGIC ## CSV-Specific Considerations
# MAGIC
# MAGIC CSV files require additional options compared to JSON:
# MAGIC
# MAGIC ### Key Options:
# MAGIC
# MAGIC * `header` - Whether first row contains column names
# MAGIC * `inferSchema` - Infer data types (use cloudFiles.inferColumnTypes instead)
# MAGIC * `delimiter` - Column separator (default: comma)
# MAGIC * `quote` - Quote character
# MAGIC * `escape` - Escape character
# MAGIC * `multiLine` - Handle multi-line CSV records
# MAGIC
# MAGIC ### CSV Best Practices:
# MAGIC
# MAGIC ✅ Always enable `cloudFiles.inferColumnTypes` for CSV  
# MAGIC ✅ Use `rescue` mode for data quality  
# MAGIC ✅ Specify delimiter explicitly if not comma  
# MAGIC ✅ Enable `multiLine` if CSV contains line breaks  

# COMMAND ----------

# DBTITLE 1,Demo: Auto Loader for CSV
# Auto Loader configuration for CSV files
# This shows CSV-specific options

csv_source = f"/Volumes/{catalog}/{schema_name}/{volume}/csv_source/"
csv_checkpoint = f"/Volumes/{catalog}/{schema_name}/{volume}/csv_checkpoint/"
csv_bronze = f"/Volumes/{catalog}/{schema_name}/{volume}/csv_bronze/"

try:
    # Create sample CSV data
    csv_data = """customer_id,name,email,city,signup_date
101,John Doe,john@example.com,Seattle,2026-04-20
102,Jane Smith,jane@example.com,Portland,2026-04-21
103,Mike Johnson,mike@example.com,Vancouver,2026-04-21"""
    
    dbutils.fs.put(f"{csv_source}customers_001.csv", csv_data, overwrite=True)
    print("✅ Sample CSV file created")
    
    # Configure Auto Loader for CSV
    csv_df = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("cloudFiles.inferColumnTypes", "true") \
        .option("cloudFiles.schemaLocation", f"{csv_checkpoint}_schema/") \
        .option("cloudFiles.schemaEvolutionMode", "rescue") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .load(csv_source)
    
    # Add metadata
    csv_enriched = csv_df.select(
        "*",
        col("_metadata.file_name").alias("_source_file"),
        current_timestamp().alias("_ingestion_time")
    )
    
    print("\n📊 CSV Schema Inferred:")
    csv_enriched.printSchema()
    
    # Write to Delta with serverless-compatible trigger
    csv_query = csv_enriched.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{csv_checkpoint}_checkpoint/") \
        .option("mergeSchema", "true") \
        .trigger(availableNow=True) \
        .start(csv_bronze)
    
    print(f"\n✅ CSV Auto Loader stream started: {csv_query.id}")
    
    # Wait for completion
    csv_query.awaitTermination()
    print("✅ CSV processing completed")
    
    # Display results
    result = spark.read.format("delta").load(csv_bronze)
    print(f"\n📋 Records ingested: {result.count()}")
    display(result)
    
except Exception as e:
    print(f"⚠️ Error: {e}")

# COMMAND ----------

# DBTITLE 1,Section 12: Advanced Features
# MAGIC %md
# MAGIC # 🚀 Section 12: Advanced Auto Loader Features
# MAGIC
# MAGIC ## 1️⃣ File Notification Mode (Production Scale)
# MAGIC
# MAGIC For high-volume ingestion (> 1000 files), use file notifications:
# MAGIC
# MAGIC ```python
# MAGIC .option("cloudFiles.useNotifications", "true")
# MAGIC ```
# MAGIC
# MAGIC **What happens:**
# MAGIC - Databricks creates a cloud queue (SQS on AWS, Event Grid on Azure)
# MAGIC - Cloud storage sends events when files arrive
# MAGIC - Sub-second file detection
# MAGIC - Scales to millions of files
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Cloud permissions to create/manage queues
# MAGIC - One-time setup per source path
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2️⃣ Schema Hints (Type Control)
# MAGIC
# MAGIC Override inferred types for specific columns:
# MAGIC
# MAGIC ```python
# MAGIC .option("cloudFiles.schemaHints", "customer_id INT, amount DECIMAL(10,2)")
# MAGIC ```
# MAGIC
# MAGIC **Use when:**
# MAGIC - You know the correct types
# MAGIC - Prevent type mismatches
# MAGIC - Enforce data contracts
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3️⃣ Partition Pruning
# MAGIC
# MAGIC Optimize performance by reading only relevant partitions:
# MAGIC
# MAGIC ```python
# MAGIC .option("cloudFiles.partitionColumns", "year,month,day")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4️⃣ File Filtering
# MAGIC
# MAGIC Process only specific files:
# MAGIC
# MAGIC ```python
# MAGIC .option("pathGlobFilter", "*.json")  # Only JSON files
# MAGIC .option("modifiedAfter", "2026-04-01")  # Only recent files
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5️⃣ Processing Rate Control
# MAGIC
# MAGIC Control how many files to process per trigger:
# MAGIC
# MAGIC ```python
# MAGIC .option("cloudFiles.maxFilesPerTrigger", "100")  # Process 100 files per batch
# MAGIC .option("cloudFiles.maxBytesPerTrigger", "10g")  # Or 10GB per batch
# MAGIC ```
# MAGIC
# MAGIC **Use for:**
# MAGIC - Backfill scenarios
# MAGIC - Rate limiting
# MAGIC - Cost control
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 6️⃣ Schema Evolution Strategies
# MAGIC
# MAGIC ### Add New Columns (Default):
# MAGIC ```python
# MAGIC .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
# MAGIC ```
# MAGIC
# MAGIC ### Rescue Unparseable Data:
# MAGIC ```python
# MAGIC .option("cloudFiles.schemaEvolutionMode", "rescue")
# MAGIC ```
# MAGIC
# MAGIC ### Fail on Schema Changes:
# MAGIC ```python
# MAGIC .option("cloudFiles.schemaEvolutionMode", "failOnNewColumns")
# MAGIC ```
# MAGIC
# MAGIC ### No Evolution:
# MAGIC ```python
# MAGIC .option("cloudFiles.schemaEvolutionMode", "none")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Demo: Advanced Configuration
# Demonstration of advanced Auto Loader configuration
# This shows production-grade options

def create_advanced_autoloader(
    source_path,
    checkpoint_path,
    target_path,
    file_format="json",
    use_notifications=False,
    max_files_per_trigger=100,
    schema_hints=None
):
    """
    Advanced Auto Loader with all production features.
    
    Args:
        source_path: Source directory
        checkpoint_path: Checkpoint location
        target_path: Target Delta table
        file_format: File format (json, csv, parquet)
        use_notifications: Enable file notification mode
        max_files_per_trigger: Rate limiting
        schema_hints: Schema type hints
    """
    
    # Build configuration
    reader = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", file_format) \
        .option("cloudFiles.inferColumnTypes", "true") \
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}_schema/") \
        .option("cloudFiles.schemaEvolutionMode", "rescue") \
        .option("cloudFiles.maxFilesPerTrigger", max_files_per_trigger)
    
    # Add file notification if enabled
    if use_notifications:
        reader = reader.option("cloudFiles.useNotifications", "true")
    
    # Add schema hints if provided
    if schema_hints:
        reader = reader.option("cloudFiles.schemaHints", schema_hints)
    
    # Load data
    df = reader.load(source_path)
    
    # Enrich with metadata and quality indicators
    df_enriched = df.select(
        "*",
        col("_metadata.file_path").alias("_source_file"),
        col("_metadata.file_name").alias("_filename"),
        col("_metadata.file_size").alias("_file_size_bytes"),
        col("_metadata.file_modification_time").alias("_file_modified_at"),
        current_timestamp().alias("_ingested_at"),
        current_date().alias("_ingestion_date"),
        when(col("_rescued_data").isNotNull(), lit("RESCUED"))
            .otherwise(lit("VALID")).alias("_data_quality_status")
    )
    
    # Write stream
    query = df_enriched.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{checkpoint_path}_checkpoint/") \
        .option("mergeSchema", "true") \
        .queryName(f"autoloader_{file_format}_advanced") \
        .start(target_path)
    
    return query

print("✅ Advanced Auto Loader function created!")
print("\n🔑 Features:")
print("  - Configurable file notification mode")
print("  - Rate limiting support")
print("  - Schema hints capability")
print("  - Data quality status tracking")
print("  - Comprehensive metadata capture")
print("  - Production-ready error handling")

# COMMAND ----------

# DBTITLE 1,Section 13: Genie Code Agent - Auto Loader Prompts
# MAGIC %md
# MAGIC # 🧞 Section 13: Using Genie Code Agent for Auto Loader
# MAGIC
# MAGIC ## 💡 How to Leverage AI for Auto Loader Tasks
# MAGIC
# MAGIC Databricks Genie Code Agent can help you build Auto Loader pipelines quickly!
# MAGIC
# MAGIC ### 📝 Example Prompts:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 1️⃣ Basic Pipeline Creation:
# MAGIC
# MAGIC **Prompt:**
# MAGIC > "Generate an Auto Loader pipeline that reads JSON files from /Volumes/main/default/raw/ and writes to a Delta table at /Volumes/main/default/bronze/ with checkpointing"
# MAGIC
# MAGIC **What you'll get:**
# MAGIC - Complete readStream configuration
# MAGIC - Checkpoint setup
# MAGIC - Delta writeStream code
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2️⃣ Incremental Ingestion:
# MAGIC
# MAGIC **Prompt:**
# MAGIC > "Build an incremental ingestion pipeline using Auto Loader for CSV files with header=true, add source file metadata, and handle schema evolution"
# MAGIC
# MAGIC **What you'll get:**
# MAGIC - CSV-specific options
# MAGIC - Metadata column additions
# MAGIC - Schema evolution handling
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 3️⃣ Production Pipeline:
# MAGIC
# MAGIC **Prompt:**
# MAGIC > "Create a production Auto Loader pipeline with rescued data, metadata tracking, schema hints for customer_id as INT, and rate limiting to 100 files per trigger"
# MAGIC
# MAGIC **What you'll get:**
# MAGIC - Complete production configuration
# MAGIC - Data quality handling
# MAGIC - Performance optimization
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 4️⃣ Monitoring Setup:
# MAGIC
# MAGIC **Prompt:**
# MAGIC > "Add monitoring to my Auto Loader stream to count rescued records and track processing metrics"
# MAGIC
# MAGIC **What you'll get:**
# MAGIC - Rescued data queries
# MAGIC - Metric tracking code
# MAGIC - Alert logic
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 5️⃣ Debugging Help:
# MAGIC
# MAGIC **Prompt:**
# MAGIC > "My Auto Loader pipeline is not detecting new files. Help me debug the checkpoint and schema location settings"
# MAGIC
# MAGIC **What you'll get:**
# MAGIC - Diagnostic queries
# MAGIC - Configuration validation
# MAGIC - Troubleshooting steps
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 6️⃣ Schema Evolution:
# MAGIC
# MAGIC **Prompt:**
# MAGIC > "Implement Auto Loader with schema evolution that rescues unparseable data and adds new columns automatically"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 7️⃣ File Format Conversion:
# MAGIC
# MAGIC **Prompt:**
# MAGIC > "Build Auto Loader to read Parquet files and convert them to Delta format with data lineage tracking"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 8️⃣ Multi-Format Ingestion:
# MAGIC
# MAGIC **Prompt:**
# MAGIC > "Create separate Auto Loader streams for JSON, CSV, and Parquet files from different source folders, writing to the same bronze table"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✨ Pro Tips:
# MAGIC
# MAGIC ✅ Be specific about file formats and options  
# MAGIC ✅ Mention Unity Catalog paths explicitly  
# MAGIC ✅ Specify checkpoint locations  
# MAGIC ✅ Ask for metadata tracking  
# MAGIC ✅ Request error handling (rescue mode)  
# MAGIC ✅ Include performance requirements  
# MAGIC
# MAGIC ### 🚀 Advanced Prompts:
# MAGIC
# MAGIC **Complex Transformations:**
# MAGIC > "Build Auto Loader pipeline that reads JSON, flattens nested structures, adds data quality checks, deduplicates on customer_id, and writes to Delta with Z-ordering on timestamp"
# MAGIC
# MAGIC **Integration:**
# MAGIC > "Create Auto Loader pipeline integrated with Unity Catalog, applying row-level security and column masking policies"
# MAGIC
# MAGIC **Performance Tuning:**
# MAGIC > "Optimize my Auto Loader for 1 million small JSON files using file notifications, schema hints, and partition pruning"

# COMMAND ----------

# DBTITLE 1,Section 14: Summary & Key Learnings
# MAGIC %md
# MAGIC # 🎓 Section 14: Summary & Key Learnings
# MAGIC
# MAGIC ## 📚 What We Covered:
# MAGIC
# MAGIC ### 1️⃣ Auto Loader Fundamentals
# MAGIC * Incremental file ingestion framework
# MAGIC * Built on Structured Streaming
# MAGIC * Exactly-once processing guarantees
# MAGIC * Superior to traditional batch ingestion
# MAGIC
# MAGIC ### 2️⃣ File Detection Mechanisms
# MAGIC * **Directory Listing:** Simple, good for < 1K files
# MAGIC * **File Notification:** Production-scale, millions of files
# MAGIC * Automatic mode selection
# MAGIC * Sub-second file detection
# MAGIC
# MAGIC ### 3️⃣ Schema Management
# MAGIC * Automatic schema inference
# MAGIC * Schema evolution (addNewColumns, rescue, fail, none)
# MAGIC * Type inference for CSV/JSON
# MAGIC * Schema hints for type control
# MAGIC
# MAGIC ### 4️⃣ Production Best Practices
# MAGIC * ✅ Always use checkpoints (unique per stream)
# MAGIC * ✅ Track source file metadata
# MAGIC * ✅ Enable rescued data for data quality
# MAGIC * ✅ Use Delta Lake for output
# MAGIC * ✅ Monitor processing metrics
# MAGIC * ✅ Apply rate limiting for backfills
# MAGIC
# MAGIC ### 5️⃣ Key Configuration Options
# MAGIC
# MAGIC | Option | Purpose | Example |
# MAGIC |--------|---------|----------|
# MAGIC | `cloudFiles.format` | File format | "json", "csv", "parquet" |
# MAGIC | `cloudFiles.inferColumnTypes` | Auto type inference | "true" |
# MAGIC | `cloudFiles.schemaLocation` | Schema storage | checkpoint path |
# MAGIC | `cloudFiles.schemaEvolutionMode` | Handle schema changes | "rescue" |
# MAGIC | `cloudFiles.useNotifications` | File notification mode | "true" |
# MAGIC | `cloudFiles.maxFilesPerTrigger` | Rate limiting | "100" |
# MAGIC | `cloudFiles.schemaHints` | Type hints | "id INT, amount DECIMAL" |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚡ Performance Tips:
# MAGIC
# MAGIC 1. **Use File Notifications** for > 1K files
# MAGIC 2. **Enable Rate Limiting** during backfills
# MAGIC 3. **Partition Your Data** for better pruning
# MAGIC 4. **Use Schema Hints** to avoid type mismatches
# MAGIC 5. **Monitor Rescued Data** for quality issues
# MAGIC 6. **Optimize Checkpoint Location** (same region as data)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Benefits vs Traditional Batch:
# MAGIC
# MAGIC | Aspect | Batch Ingestion | Auto Loader |
# MAGIC |--------|----------------|-------------|
# MAGIC | **Performance** | Degrades over time | Constant |
# MAGIC | **Cost** | High (reprocessing) | Low (incremental) |
# MAGIC | **Latency** | Hours | Seconds |
# MAGIC | **Complexity** | High (custom logic) | Low (built-in) |
# MAGIC | **Reliability** | At-least-once | Exactly-once |
# MAGIC | **Scale** | Limited | Unlimited |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ When to Use Auto Loader:
# MAGIC
# MAGIC ✅ Ingesting files from cloud storage  
# MAGIC ✅ Continuous/streaming data arrival  
# MAGIC ✅ Large number of files  
# MAGIC ✅ Need for exactly-once semantics  
# MAGIC ✅ Schema changes expected  
# MAGIC ✅ Low-latency requirements  
# MAGIC ✅ Bronze layer ingestion  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ When NOT to Use Auto Loader:
# MAGIC
# MAGIC ❌ One-time historical data load (use batch)  
# MAGIC ❌ Files deleted/modified after arrival  
# MAGIC ❌ Non-file-based sources (use Kafka, Delta Live, etc.)  
# MAGIC ❌ Complex file interdependencies  

# COMMAND ----------

# DBTITLE 1,Section 15: Interview Questions
# MAGIC %md
# MAGIC # 🎯 Section 15: Auto Loader Interview Questions
# MAGIC
# MAGIC ## 📝 Top 10 Interview Questions:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q1: What is Auto Loader and how does it differ from traditional batch ingestion?
# MAGIC
# MAGIC **Answer:**
# MAGIC Auto Loader is Databricks' incremental file ingestion framework built on Structured Streaming. Unlike batch ingestion that scans all files every run, Auto Loader:
# MAGIC * Processes only NEW files since last checkpoint
# MAGIC * Provides exactly-once semantics
# MAGIC * Scales to millions of files
# MAGIC * Offers sub-second latency
# MAGIC * Automatically handles schema evolution
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q2: Explain the two file detection mechanisms in Auto Loader.
# MAGIC
# MAGIC **Answer:**
# MAGIC
# MAGIC **1. Directory Listing Mode:**
# MAGIC * Default mode
# MAGIC * Periodically lists directory contents
# MAGIC * Good for < 1000 files
# MAGIC * No additional setup required
# MAGIC * Performance degrades with file count
# MAGIC
# MAGIC **2. File Notification Mode:**
# MAGIC * Uses cloud-native events (S3 Events, Azure Event Grid)
# MAGIC * Constant O(1) performance
# MAGIC * Scales to millions of files
# MAGIC * Requires cloud permissions
# MAGIC * Sub-second file detection
# MAGIC
# MAGIC Enable with: `.option("cloudFiles.useNotifications", "true")`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q3: What is the purpose of checkpointLocation in Auto Loader?
# MAGIC
# MAGIC **Answer:**
# MAGIC Checkpoint location is CRITICAL for:
# MAGIC * **State Management:** Tracks which files have been processed
# MAGIC * **Exactly-Once Semantics:** Prevents duplicate processing
# MAGIC * **Fault Tolerance:** Enables recovery after failures
# MAGIC * **Schema Storage:** Persists inferred schema
# MAGIC * **Offset Tracking:** Maintains processing position
# MAGIC
# MAGIC ⚠️ Each stream MUST have a unique checkpoint location.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q4: How does Auto Loader handle schema evolution?
# MAGIC
# MAGIC **Answer:**
# MAGIC Auto Loader provides four schema evolution modes:
# MAGIC
# MAGIC 1. **addNewColumns** (default): Automatically adds new columns
# MAGIC 2. **rescue**: Puts unparseable data in `_rescued_data` column
# MAGIC 3. **failOnNewColumns**: Stops pipeline on schema changes
# MAGIC 4. **none**: No evolution, schema is fixed
# MAGIC
# MAGIC Best practice: Use "rescue" mode in bronze layer for data quality monitoring.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q5: What is the _rescued_data column and why is it important?
# MAGIC
# MAGIC **Answer:**
# MAGIC `_rescued_data` column (when `schemaEvolutionMode = "rescue"`) contains:
# MAGIC * Records that don't match expected schema
# MAGIC * Unparseable data
# MAGIC * Type mismatch errors
# MAGIC * Corrupt records
# MAGIC
# MAGIC **Benefits:**
# MAGIC * Pipeline resilience (doesn't fail on bad data)
# MAGIC * Data quality monitoring
# MAGIC * Debugging capability
# MAGIC * Complete audit trail
# MAGIC
# MAGIC **Monitoring:**
# MAGIC ```python
# MAGIC rescued_count = df.filter(col("_rescued_data").isNotNull()).count()
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q6: How do you track source file lineage in Auto Loader?
# MAGIC
# MAGIC **Answer:**
# MAGIC Use the `_metadata` pseudo-column:
# MAGIC
# MAGIC ```python
# MAGIC df.select(
# MAGIC     "*",
# MAGIC     col("_metadata.file_path").alias("source_file"),
# MAGIC     col("_metadata.file_name").alias("source_filename"),
# MAGIC     col("_metadata.file_size").alias("file_size"),
# MAGIC     col("_metadata.file_modification_time").alias("file_timestamp")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC This provides complete data lineage for debugging and auditing.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q7: What is the difference between cloudFiles.inferColumnTypes and regular schema inference?
# MAGIC
# MAGIC **Answer:**
# MAGIC
# MAGIC **`cloudFiles.inferColumnTypes = "true"`:**
# MAGIC * Auto Loader-specific inference
# MAGIC * Works with CSV and JSON
# MAGIC * Samples files to determine types
# MAGIC * Stores schema in checkpoint location
# MAGIC * Consistent across pipeline restarts
# MAGIC * Recommended for production
# MAGIC
# MAGIC **Regular `inferSchema`:**
# MAGIC * Spark's built-in inference
# MAGIC * Re-infers on every run
# MAGIC * Can cause schema inconsistencies
# MAGIC * Not recommended with Auto Loader
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q8: How do you implement rate limiting in Auto Loader?
# MAGIC
# MAGIC **Answer:**
# MAGIC Use these options to control processing rate:
# MAGIC
# MAGIC ```python
# MAGIC .option("cloudFiles.maxFilesPerTrigger", "100")  # Files per batch
# MAGIC .option("cloudFiles.maxBytesPerTrigger", "10g")  # Bytes per batch
# MAGIC ```
# MAGIC
# MAGIC **Use cases:**
# MAGIC * Backfilling historical data
# MAGIC * Preventing downstream overload
# MAGIC * Cost control
# MAGIC * Staged rollouts
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q9: Can Auto Loader handle multiple file formats in the same pipeline?
# MAGIC
# MAGIC **Answer:**
# MAGIC No, each Auto Loader stream handles ONE file format. For multiple formats:
# MAGIC
# MAGIC **Approach 1:** Separate streams per format
# MAGIC ```python
# MAGIC json_stream = readStream.format("cloudFiles").option("cloudFiles.format", "json")...
# MAGIC csv_stream = readStream.format("cloudFiles").option("cloudFiles.format", "csv")...
# MAGIC ```
# MAGIC
# MAGIC **Approach 2:** Use folder structure
# MAGIC ```
# MAGIC /source/json/
# MAGIC /source/csv/
# MAGIC ```
# MAGIC
# MAGIC Create separate Auto Loader pipelines pointing to each folder.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q10: What are schema hints and when should you use them?
# MAGIC
# MAGIC **Answer:**
# MAGIC Schema hints override Auto Loader's type inference:
# MAGIC
# MAGIC ```python
# MAGIC .option("cloudFiles.schemaHints", "customer_id INT, amount DECIMAL(10,2), status STRING")
# MAGIC ```
# MAGIC
# MAGIC **Use when:**
# MAGIC * You know the correct types
# MAGIC * Inference produces wrong types (e.g., ZIP codes as INT)
# MAGIC * Enforcing data contracts
# MAGIC * Preventing type widening
# MAGIC * Critical columns need specific precision
# MAGIC
# MAGIC **Benefits:**
# MAGIC * Type consistency
# MAGIC * Prevents downstream errors
# MAGIC * Data quality enforcement
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔥 Bonus Questions:
# MAGIC
# MAGIC ### Q11: How does Auto Loader ensure exactly-once processing?
# MAGIC **Answer:** Through checkpoint-based state management. Each file's processing state is recorded in the checkpoint. If a batch fails, Auto Loader restarts from the last successful checkpoint, reprocessing only failed files.
# MAGIC
# MAGIC ### Q12: Can you modify checkpoint location after a stream has started?
# MAGIC **Answer:** No. Checkpoint location is immutable once a stream starts. Changing it creates a NEW stream that reprocesses all files. If you must change it, you need to track processed files externally or accept reprocessing.
# MAGIC
# MAGIC ### Q13: What happens if a file is modified after Auto Loader processes it?
# MAGIC **Answer:** Auto Loader tracks files by path and modification time. If a file is modified, it's treated as a NEW file and reprocessed, potentially causing duplicates. Best practice: Use immutable file patterns (append-only).
# MAGIC
# MAGIC ### Q14: How do you monitor Auto Loader pipeline health?
# MAGIC **Answer:**
# MAGIC * Check `query.lastProgress` for metrics
# MAGIC * Monitor `_rescued_data` for data quality
# MAGIC * Track processing lag
# MAGIC * Set up alerts on stream failures
# MAGIC * Monitor checkpoint storage growth
# MAGIC
# MAGIC ### Q15: What are the permission requirements for file notification mode?
# MAGIC **Answer:**
# MAGIC * **AWS:** S3 bucket notifications, SQS queue creation, IAM roles
# MAGIC * **Azure:** Event Grid subscriptions, Storage Queue access
# MAGIC * **GCP:** Pub/Sub topics, Cloud Storage notifications
# MAGIC
# MAGIC Databricks can auto-configure these with appropriate cloud permissions.

# COMMAND ----------

# DBTITLE 1,Demo: Add Metadata Columns
# Add metadata tracking columns to the stream
# This is a BEST PRACTICE for production pipelines

try:
    df_with_metadata = df_stream.select(
        "*",  # All original columns
        col("_metadata.file_path").alias("source_file_path"),
        col("_metadata.file_name").alias("source_file_name"),
        col("_metadata.file_size").alias("source_file_size"),
        col("_metadata.file_modification_time").alias("source_file_modified_time"),
        current_timestamp().alias("ingestion_timestamp")
    )
    
    print("✅ Metadata columns added successfully!")
    print("\n📊 Enhanced Schema with Metadata:")
    df_with_metadata.printSchema()
    
    print("\n🔑 Additional Columns:")
    print("  - source_file_path: Full path to source file")
    print("  - source_file_name: Name of source file")
    print("  - source_file_size: Size in bytes")
    print("  - source_file_modified_time: File modification timestamp")
    print("  - ingestion_timestamp: When record was ingested")
    
except Exception as e:
    print(f"⚠️ Error: {e}")

# COMMAND ----------

# DBTITLE 1,Section 8: Writing Auto Loader Output to Delta
# MAGIC %md
# MAGIC # ✍️ Section 8: Writing Auto Loader Output to Delta
# MAGIC
# MAGIC ## Streaming Write Operations
# MAGIC
# MAGIC Once we have our streaming DataFrame, we need to write it somewhere. Auto Loader pairs perfectly with **Delta Lake**.
# MAGIC
# MAGIC ### Key Components:
# MAGIC
# MAGIC 1. **`.writeStream`** - Initiates streaming write
# MAGIC 2. **`.format("delta")`** - Write to Delta format
# MAGIC 3. **`.outputMode()`** - How to write data:
# MAGIC    - `append` - Add new records (most common)
# MAGIC    - `complete` - Replace entire output
# MAGIC    - `update` - Update existing records
# MAGIC 4. **`.option("checkpointLocation")`** - **MANDATORY** for fault tolerance
# MAGIC 5. **`.start()`** or `.toTable()` - Start the stream
# MAGIC
# MAGIC ### Checkpoint Location (Critical!):
# MAGIC
# MAGIC The **checkpoint** is Auto Loader's memory:
# MAGIC * Tracks which files have been processed
# MAGIC * Enables exactly-once guarantees
# MAGIC * Allows recovery after failures
# MAGIC * **Must be unique per stream**
# MAGIC
# MAGIC ⚠️ **Never share checkpoint locations between different streams!**
# MAGIC
# MAGIC ### Output Modes Explained:
# MAGIC
# MAGIC | Mode | Use Case | Example |
# MAGIC |------|----------|----------|
# MAGIC | **append** | Add new records | Raw data ingestion, logs |
# MAGIC | **complete** | Full aggregation replacement | Real-time dashboards |
# MAGIC | **update** | Change existing rows | Slowly changing dimensions |

# COMMAND ----------

# DBTITLE 1,Demo: Write Stream to Delta Table
# Write the streaming DataFrame to a Delta table
# This creates a bronze layer table with Auto Loader

try:
    # Start the streaming write with availableNow trigger (serverless compatible)
    query = df_with_metadata.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{checkpoint_path}bronze_checkpoint/") \
        .option("mergeSchema", "true") \
        .trigger(availableNow=True) \
        .start(bronze_path)
    
    print("✅ Streaming write started successfully!")
    print(f"\n🎯 Stream ID: {query.id}")
    print(f"📊 Status: {query.status}")
    print(f"💾 Writing to: {bronze_path}")
    print(f"📑 Checkpoint at: {checkpoint_path}bronze_checkpoint/")
    
    print("\n⏳ Waiting for data to be processed...")
    query.awaitTermination()
    
    print("\n✅ Stream processing completed!")
    print(f"Last Progress: {query.lastProgress}")
    
except Exception as e:
    print(f"⚠️ Error: {e}")
    print("This might occur if paths don't exist or permissions are missing.")

# COMMAND ----------

# DBTITLE 1,Verify Bronze Table Data
# Read and display the bronze table data
# This verifies that Auto Loader successfully ingested the data

try:
    bronze_df = spark.read.format("delta").load(bronze_path)
    
    print("✅ Bronze table read successfully!")
    print(f"\n📏 Total records ingested: {bronze_df.count()}")
    
    print("\n📊 Sample Data with Metadata:")
    display(bronze_df.select(
        "customer_id",
        "name",
        "city",
        "amount",
        "transaction_date",
        "source_file_name",
        "ingestion_timestamp"
    ).limit(10))
    
    print("\n✅ Notice how each record tracks its source file!")
    
except Exception as e:
    print(f"⚠️ Error: {e}")
    print("The bronze table might not exist yet if the stream hasn't run.")

# COMMAND ----------

# DBTITLE 1,Section 9: Complete Auto Loader Pipeline
# MAGIC %md
# MAGIC # 🏭 Section 9: Complete End-to-End Auto Loader Pipeline
# MAGIC
# MAGIC ## Production-Grade Pipeline Architecture
# MAGIC
# MAGIC Let's build a complete, production-ready Auto Loader pipeline with:
# MAGIC
# MAGIC ✅ **Schema inference and evolution**  
# MAGIC ✅ **Metadata tracking**  
# MAGIC ✅ **Data quality transformations**  
# MAGIC ✅ **Checkpointing**  
# MAGIC ✅ **Delta Lake storage**  
# MAGIC ✅ **Error handling with rescued data**  
# MAGIC
# MAGIC ### Pipeline Flow:
# MAGIC
# MAGIC ```
# MAGIC 📂 Source Files (JSON/CSV/Parquet)
# MAGIC         ↓
# MAGIC ⚡ Auto Loader (cloudFiles)
# MAGIC         ↓
# MAGIC 🔧 Transformations + Metadata
# MAGIC         ↓
# MAGIC 📑 Checkpoint (State Management)
# MAGIC         ↓
# MAGIC 💾 Bronze Delta Table
# MAGIC ```
# MAGIC
# MAGIC ### Best Practices Applied:
# MAGIC
# MAGIC 1. **Rescue Unparseable Data:** Don't fail on bad records
# MAGIC 2. **Schema Evolution:** Handle new columns gracefully
# MAGIC 3. **Metadata Capture:** Track lineage
# MAGIC 4. **Idempotency:** Exactly-once processing
# MAGIC 5. **Monitoring:** Track processing metrics

# COMMAND ----------

# DBTITLE 1,Production Pipeline: Complete Auto Loader
# Complete production-grade Auto Loader pipeline
# This demonstrates all best practices in one place

from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_autoloader_pipeline(
    source_path,
    checkpoint_path,
    target_path,
    file_format="json"
):
    """
    Create a production-grade Auto Loader pipeline.
    
    Args:
        source_path: Source directory with files
        checkpoint_path: Checkpoint location
        target_path: Target Delta table path
        file_format: File format (json, csv, parquet, etc.)
    
    Returns:
        Streaming query object
    """
    
    # Step 1: Configure Auto Loader with all best practices
    df_raw = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", file_format) \
        .option("cloudFiles.inferColumnTypes", "true") \
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}_schema/") \
        .option("cloudFiles.schemaEvolutionMode", "rescue") \
        .option("cloudFiles.maxFilesPerTrigger", "1000") \
        .load(source_path)
    
    # Step 2: Add metadata and audit columns
    df_enriched = df_raw.select(
        "*",
        col("_metadata.file_path").alias("_source_file"),
        col("_metadata.file_name").alias("_source_filename"),
        col("_metadata.file_size").alias("_source_file_size"),
        col("_metadata.file_modification_time").alias("_source_file_timestamp"),
        current_timestamp().alias("_ingestion_timestamp"),
        current_date().alias("_ingestion_date")
    )
    
    # Step 3: Add data quality indicators
    df_quality = df_enriched.withColumn(
        "_is_rescued",
        when(col("_rescued_data").isNotNull(), True).otherwise(False)
    )
    
    # Step 4: Write to Delta with checkpointing (serverless compatible)
    query = df_quality.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{checkpoint_path}_checkpoint/") \
        .option("mergeSchema", "true") \
        .trigger(availableNow=True) \
        .queryName("autoloader_bronze_ingestion") \
        .start(target_path)
    
    return query

print("✅ Production pipeline function defined!")
print("\n🔑 Key Features:")
print("  - Automatic schema inference and evolution")
print("  - Rescued data for unparseable records")
print("  - Complete metadata tracking")
print("  - Data quality indicators")
print("  - Configurable file processing rate")
print("  - Fault-tolerant checkpointing")
print("  - Serverless compute compatible (availableNow trigger)")

# COMMAND ----------

# DBTITLE 1,Run Production Pipeline
# Execute the production pipeline
# In production, this would run continuously with continuous trigger
# For serverless, we use availableNow trigger

try:
    # Define paths for production pipeline
    prod_source = f"/Volumes/{catalog}/{schema_name}/{volume}/prod_source/"
    prod_checkpoint = f"/Volumes/{catalog}/{schema_name}/{volume}/prod_checkpoint/"
    prod_bronze = f"/Volumes/{catalog}/{schema_name}/{volume}/prod_bronze/"
    
    # Create sample data for production demo
    sample_prod_data = [
        {"order_id": 1001, "product": "Laptop", "quantity": 2, "price": 1200.00, "order_date": "2026-04-21"},
        {"order_id": 1002, "product": "Mouse", "quantity": 5, "price": 25.50, "order_date": "2026-04-21"},
        {"order_id": 1003, "product": "Keyboard", "quantity": 3, "price": 75.00, "order_date": "2026-04-21"},
    ]
    
    # Write sample files
    for i, record in enumerate(sample_prod_data):
        file_content = json.dumps(record)
        dbutils.fs.put(f"{prod_source}order_{i+1}.json", file_content, overwrite=True)
    
    print("✅ Sample production data created")
    
    # Start the pipeline
    prod_query = create_autoloader_pipeline(
        source_path=prod_source,
        checkpoint_path=prod_checkpoint,
        target_path=prod_bronze,
        file_format="json"
    )
    
    print(f"\n✅ Production pipeline started!")
    print(f"Stream ID: {prod_query.id}")
    print(f"Stream Name: {prod_query.name}")
    
    # Wait for the batch to complete
    print("\n⏳ Processing batch...")
    prod_query.awaitTermination()
    
    # Check progress
    if prod_query.lastProgress:
        print("\n📊 Processing Metrics:")
        progress = prod_query.lastProgress
        print(f"  - Batch ID: {progress.get('batchId', 'N/A')}")
        print(f"  - Rows Processed: {progress.get('numInputRows', 0)}")
        print(f"  - Duration: {progress.get('batchDuration', 0)} ms")
    
    print("\n✅ Pipeline processing completed")
    
    # Verify data
    result_df = spark.read.format("delta").load(prod_bronze)
    print(f"\n✅ Total records in bronze table: {result_df.count()}")
    
    display(result_df.select(
        "order_id", "product", "quantity", "price",
        "_source_filename", "_ingestion_timestamp", "_is_rescued"
    ))
    
except Exception as e:
    print(f"⚠️ Error: {e}")
    print("Adjust paths to match your Unity Catalog setup.")

# COMMAND ----------

# DBTITLE 1,Section 10: Monitoring & Rescued Data
# MAGIC %md
# MAGIC # 👁️ Section 10: Monitoring & Rescued Data
# MAGIC
# MAGIC ## The _rescued_data Column
# MAGIC
# MAGIC When you enable `schemaEvolutionMode = "rescue"`, Auto Loader adds a special **`_rescued_data`** column.
# MAGIC
# MAGIC ### What Gets Rescued?
# MAGIC
# MAGIC * Records that don't match the expected schema
# MAGIC * Unparseable data
# MAGIC * Type mismatch errors
# MAGIC * Corrupt records
# MAGIC
# MAGIC ### Why This Matters:
# MAGIC
# MAGIC ✅ **Pipeline Resilience:** Don't fail on bad data  
# MAGIC ✅ **Data Quality Monitoring:** Identify problematic sources  
# MAGIC ✅ **Debugging:** Investigate what went wrong  
# MAGIC ✅ **Compliance:** Maintain complete audit trail  
# MAGIC
# MAGIC ### Best Practice:
# MAGIC
# MAGIC Create alerts/monitoring on rescued data:
# MAGIC
# MAGIC ```python
# MAGIC # Monitor rescued data
# MAGIC rescued_count = df.filter(col("_rescued_data").isNotNull()).count()
# MAGIC
# MAGIC if rescued_count > threshold:
# MAGIC     send_alert(f"High rescued data count: {rescued_count}")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 5: Setup & Configuration
# MAGIC %md
# MAGIC # ⚙️ Section 5: Setup & Configuration
# MAGIC
# MAGIC ## Environment Setup
# MAGIC
# MAGIC Before we start, let's set up our environment variables and paths.
# MAGIC
# MAGIC **Key Components:**
# MAGIC * Unity Catalog Volumes for storage
# MAGIC * Checkpoint directories for state management
# MAGIC * Source and target paths

# COMMAND ----------

# DBTITLE 1,Setup Variables
# Setup variables for Auto Loader demo
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define catalog, schema, and volume
# Replace with your Unity Catalog coordinates
catalog = "main"  # Change to your catalog
schema_name = "default"  # Change to your schema
volume = "training_data"  # Change to your volume

# Define paths using Unity Catalog Volumes
source_path = f"/Volumes/{catalog}/{schema_name}/{volume}/autoloader_source/"
checkpoint_path = f"/Volumes/{catalog}/{schema_name}/{volume}/autoloader_checkpoint/"
bronze_path = f"/Volumes/{catalog}/{schema_name}/{volume}/bronze_layer/"

print("✅ Setup Complete!")
print(f"Source Path: {source_path}")
print(f"Checkpoint Path: {checkpoint_path}")
print(f"Bronze Path: {bronze_path}")

# COMMAND ----------

# DBTITLE 1,Section 6: Basic Auto Loader - Reading Streaming Data
# MAGIC %md
# MAGIC # 📚 Section 6: Basic Auto Loader - Reading Streaming Data
# MAGIC
# MAGIC ## The Cloud Files Format
# MAGIC
# MAGIC Auto Loader uses a special format called **`cloudFiles`** which wraps around your actual file format (CSV, JSON, Parquet, etc.).
# MAGIC
# MAGIC ### Basic Syntax:
# MAGIC
# MAGIC ```python
# MAGIC df = spark.readStream.format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "<actual_format>") \
# MAGIC     .load("<path>")
# MAGIC ```
# MAGIC
# MAGIC ### Key Options:
# MAGIC
# MAGIC * `cloudFiles.format`: The actual file format (csv, json, parquet, avro, etc.)
# MAGIC * `cloudFiles.inferColumnTypes`: Enable automatic type inference
# MAGIC * `cloudFiles.schemaLocation`: Where to store inferred schema
# MAGIC * `cloudFiles.useNotifications`: Enable file notification mode
# MAGIC
# MAGIC Let's see it in action!

# COMMAND ----------

# DBTITLE 1,Demo: Basic Auto Loader for JSON
# Create sample JSON data for demonstration
import json
import os

# Sample records
sample_data = [
    {"customer_id": 1, "name": "Alice Johnson", "city": "Seattle", "amount": 250.50, "transaction_date": "2026-04-21"},
    {"customer_id": 2, "name": "Bob Smith", "city": "Portland", "amount": 150.75, "transaction_date": "2026-04-21"},
    {"customer_id": 3, "name": "Carol White", "city": "Vancouver", "amount": 399.99, "transaction_date": "2026-04-21"}
]

# Note: In production, files would arrive from external sources
# For demo purposes, we'll use dbutils to write sample files

try:
    # Write sample data (simulating incoming files)
    for i, record in enumerate(sample_data):
        file_content = json.dumps(record)
        dbutils.fs.put(f"{source_path}transaction_{i+1}.json", file_content, overwrite=True)
    
    print("✅ Sample JSON files created successfully!")
    print(f"Files written to: {source_path}")
    
    # List files to verify
    files = dbutils.fs.ls(source_path)
    print(f"\n📁 Files in source directory: {len(files)}")
    for file in files:
        print(f"  - {file.name}")
except Exception as e:
    print(f"⚠️ Note: {e}")
    print("This is expected if the volume doesn't exist yet.")
    print("Create a Unity Catalog volume first or adjust the paths.")

# COMMAND ----------

# DBTITLE 1,Auto Loader - Read JSON Stream
# Read streaming data using Auto Loader
# This creates a streaming DataFrame that monitors the source path

try:
    df_stream = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.inferColumnTypes", "true") \
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}schema/") \
        .load(source_path)
    
    print("✅ Auto Loader stream configured successfully!")
    print("\n📊 Inferred Schema:")
    df_stream.printSchema()
    
    print("\n🔑 Key Points:")
    print("  - Stream is ready but not started yet")
    print("  - Schema has been inferred from the files")
    print("  - Auto Loader will monitor for new files automatically")
    
except Exception as e:
    print(f"⚠️ Error: {e}")
    print("Make sure the source path contains files and is accessible.")

# COMMAND ----------

# DBTITLE 1,Section 7: Metadata Tracking (Best Practice)
# MAGIC %md
# MAGIC # 📌 Section 7: Metadata Tracking (Best Practice)
# MAGIC
# MAGIC ## Why Track Source File Metadata?
# MAGIC
# MAGIC In production pipelines, **traceability** is critical:
# MAGIC * Which file did this record come from?
# MAGIC * When was it processed?
# MAGIC * How to debug data quality issues?
# MAGIC
# MAGIC ## The _metadata Column
# MAGIC
# MAGIC Auto Loader provides a special **`_metadata`** pseudo-column with file information:
# MAGIC
# MAGIC ```python
# MAGIC _metadata.file_path        # Full path to source file
# MAGIC _metadata.file_name        # File name only  
# MAGIC _metadata.file_size        # File size in bytes
# MAGIC _metadata.file_modification_time  # When file was modified
# MAGIC ```
# MAGIC
# MAGIC ### Benefits:
# MAGIC
# MAGIC ✅ **Data Lineage:** Track data origin  
# MAGIC ✅ **Debugging:** Quickly identify problematic files  
# MAGIC ✅ **Auditing:** Compliance and governance  
# MAGIC ✅ **Reprocessing:** Selectively reprocess specific files  
# MAGIC
# MAGIC ### Best Practice:
# MAGIC
# MAGIC Always capture source file metadata in your bronze layer!

# COMMAND ----------

# DBTITLE 1,Section 3: File Detection Mechanisms
# MAGIC %md
# MAGIC # 🔍 Section 3: File Detection Mechanisms
# MAGIC
# MAGIC ## 👶 ELI5 Explanation:
# MAGIC
# MAGIC Auto Loader has two ways to find new files:
# MAGIC
# MAGIC **1. Directory Listing Mode** (Simple Way):
# MAGIC - Like checking your mailbox every few minutes
# MAGIC - Looks at the folder to see what's new
# MAGIC - Good for small amounts of mail
# MAGIC
# MAGIC **2. File Notification Mode** (Smart Way):
# MAGIC - The mailman rings a bell when new mail arrives
# MAGIC - You get notified immediately
# MAGIC - Good for lots of mail
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC Auto Loader provides two file discovery mechanisms optimized for different scales:
# MAGIC
# MAGIC ### 1️⃣ Directory Listing Mode
# MAGIC
# MAGIC **How it works:**
# MAGIC - Performs parallel listing of input directory
# MAGIC - Compares current state with checkpoint
# MAGIC - Identifies new files by absence in checkpoint
# MAGIC
# MAGIC **Use when:**
# MAGIC - < 1000 files expected
# MAGIC - Infrequent file arrivals
# MAGIC - Simple cloud storage without event notifications
# MAGIC
# MAGIC **Pros:**
# MAGIC * No cloud infrastructure setup required
# MAGIC * Works out-of-the-box
# MAGIC * No additional costs
# MAGIC
# MAGIC **Cons:**
# MAGIC * Performance degrades with file count
# MAGIC * Higher latency for file discovery
# MAGIC * More API calls to cloud storage
# MAGIC
# MAGIC ### 2️⃣ File Notification Mode (Recommended for Production)
# MAGIC
# MAGIC **How it works:**
# MAGIC - Uses cloud-native event notifications (S3 Event Notifications, Azure Event Grid)
# MAGIC - Databricks creates a queue to receive file events
# MAGIC - Near-instant file discovery
# MAGIC
# MAGIC **Use when:**
# MAGIC - > 1000 files
# MAGIC - Continuous high-volume ingestion
# MAGIC - Low-latency requirements
# MAGIC
# MAGIC **Pros:**
# MAGIC * Constant O(1) performance regardless of file count
# MAGIC * Sub-second file detection
# MAGIC * Scales to millions of files
# MAGIC * Lower cloud storage API costs
# MAGIC
# MAGIC **Cons:**
# MAGIC * Requires cloud permissions setup
# MAGIC * Slight additional infrastructure complexity
# MAGIC
# MAGIC ### Automatic Mode Selection:
# MAGIC
# MAGIC ```python
# MAGIC # Auto Loader automatically chooses the best mode
# MAGIC # Default: directory listing
# MAGIC # Add this option to enable file notification mode:
# MAGIC .option("cloudFiles.useNotifications", "true")
# MAGIC ```
# MAGIC
# MAGIC ### File Detection Comparison:
# MAGIC
# MAGIC | Feature | Directory Listing | File Notification |
# MAGIC |---------|------------------|-------------------|
# MAGIC | **Setup** | None | Queue + Permissions |
# MAGIC | **Latency** | Minutes | Seconds |
# MAGIC | **Scalability** | < 1K files | Millions of files |
# MAGIC | **Cost** | Medium | Low |
# MAGIC | **Cloud APIs** | Many LIST calls | Event-driven |

# COMMAND ----------

# DBTITLE 1,Section 4: Schema Inference & Evolution
# MAGIC %md
# MAGIC # 🧩 Section 4: Schema Inference & Evolution
# MAGIC
# MAGIC ## 👶 ELI5 Explanation:
# MAGIC
# MAGIC Imagine you have a form with boxes to fill:
# MAGIC - **Schema Inference:** Auto Loader looks at your data and figures out what boxes you need
# MAGIC - **Schema Evolution:** If tomorrow you need a new box, Auto Loader adds it automatically!
# MAGIC
# MAGIC No need to tell it "this is a number" or "this is text" — it's smart enough to figure it out!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Schema inference** and **evolution** are critical capabilities for production data pipelines dealing with changing data sources.
# MAGIC
# MAGIC ### Schema Inference:
# MAGIC
# MAGIC Auto Loader can automatically infer schema from sample files, eliminating manual schema definition.
# MAGIC
# MAGIC **How it works:**
# MAGIC 1. Samples the first few files (configurable)
# MAGIC 2. Infers data types based on content
# MAGIC 3. Creates a schema that works for all files
# MAGIC 4. Persists schema in checkpoint for consistency
# MAGIC
# MAGIC **Type Inference Options:**
# MAGIC
# MAGIC ```python
# MAGIC # Infer column types (CSV, JSON)
# MAGIC .option("cloudFiles.inferColumnTypes", "true")
# MAGIC
# MAGIC # Rescue unparseable data instead of failing
# MAGIC .option("cloudFiles.schemaEvolutionMode", "rescue")
# MAGIC ```
# MAGIC
# MAGIC ### Schema Evolution:
# MAGIC
# MAGIC Handles schema changes gracefully without pipeline failures:
# MAGIC
# MAGIC **Supported Changes:**
# MAGIC * ✅ New columns added (populated with NULL for old records)
# MAGIC * ✅ Column type widening (int → long → double)
# MAGIC * ❌ Column deletion (must handle manually)
# MAGIC * ❌ Column type narrowing (requires pipeline restart)
# MAGIC
# MAGIC **Evolution Modes:**
# MAGIC
# MAGIC | Mode | Behavior | Use Case |
# MAGIC |------|----------|----------|
# MAGIC | **addNewColumns** | Adds new columns automatically | Production default |
# MAGIC | **rescue** | Puts unparseable data in `_rescued_data` | Data quality monitoring |
# MAGIC | **failOnNewColumns** | Stops on schema change | Strict schema enforcement |
# MAGIC | **none** | No evolution | Fixed schema pipelines |
# MAGIC
# MAGIC ### Best Practices:
# MAGIC
# MAGIC ✅ Always enable schema inference for JSON/CSV  
# MAGIC ✅ Use `rescue` mode in bronze layer for data quality  
# MAGIC ✅ Monitor `_rescued_data` column for schema issues  
# MAGIC ✅ Use `schema hints` for known columns to avoid type mismatches  
# MAGIC ✅ Version schemas explicitly for critical pipelines  
# MAGIC
# MAGIC ### Schema Location:
# MAGIC
# MAGIC Auto Loader stores inferred schema in:
# MAGIC ```
# MAGIC <checkpoint_path>/_schemas/
# MAGIC ```
# MAGIC
# MAGIC This ensures schema consistency across pipeline restarts.

# COMMAND ----------

# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # 🌊 Data Engineering Training — Phase 5 Day 26  
# MAGIC ## ⚡ Auto Loader: Incremental Ingestion & File Detection  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - Auto Loader Fundamentals  
# MAGIC - Incremental File Ingestion  
# MAGIC - File Detection Mechanisms  
# MAGIC - Schema Evolution Basics  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### ⚙️ Platform: Databricks (Serverless + Unity Catalog + Streaming + Auto Loader)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Objective:
# MAGIC Understand how to ingest data incrementally using Auto Loader and how Databricks detects and processes new files efficiently.

# COMMAND ----------

# DBTITLE 1,Section 1: What is Auto Loader?
# MAGIC %md
# MAGIC # 📖 Section 1: What is Auto Loader?
# MAGIC
# MAGIC ## 👶 ELI5 Explanation:
# MAGIC
# MAGIC Imagine you have a mailbox, and every day new letters arrive. Instead of checking all the old letters again and again, you only read the **new ones**. That's exactly what Auto Loader does with files!
# MAGIC
# MAGIC **Traditional way:**  
# MAGIC ❌ Read ALL files every time (even old ones)  
# MAGIC ❌ Slow and wasteful  
# MAGIC ❌ Can cause duplicates  
# MAGIC
# MAGIC **Auto Loader way:**  
# MAGIC ✅ Only reads NEW files  
# MAGIC ✅ Fast and efficient  
# MAGIC ✅ No duplicates  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Auto Loader** is Databricks' optimized file ingestion framework built on **Structured Streaming** that provides:
# MAGIC
# MAGIC * **Incremental Processing:** Processes only new files since the last checkpoint
# MAGIC * **Exactly-Once Semantics:** Guarantees no duplicates through checkpoint tracking
# MAGIC * **Scalable File Discovery:** Two modes - directory listing (small-scale) and file notification (cloud-native, large-scale)
# MAGIC * **Schema Evolution:** Automatic schema inference and evolution handling
# MAGIC * **Fault Tolerance:** Built-in retry logic and checkpoint-based recovery
# MAGIC
# MAGIC ### Key Differences from Batch Ingestion:
# MAGIC
# MAGIC | Aspect | Batch Ingestion | Auto Loader |
# MAGIC |--------|----------------|-------------|
# MAGIC | **File Processing** | Scans all files every run | Only processes new files |
# MAGIC | **Performance** | Degrades as files grow | Constant performance |
# MAGIC | **Cost** | High (reprocesses data) | Low (incremental) |
# MAGIC | **Latency** | High (scheduled batches) | Near real-time |
# MAGIC | **File Discovery** | Manual glob patterns | Automated detection |
# MAGIC
# MAGIC ### When to Use Auto Loader:
# MAGIC
# MAGIC ✅ Ingesting data from cloud storage (S3, ADLS, GCS)  
# MAGIC ✅ Continuous data arrival patterns  
# MAGIC ✅ Large number of files (1000s to millions)  
# MAGIC ✅ Need for exactly-once processing guarantees  
# MAGIC ✅ Schema changes are expected  

# COMMAND ----------

# DBTITLE 1,Section 2: Incremental Ingestion Deep Dive
# MAGIC %md
# MAGIC # 🔄 Section 2: Incremental Ingestion Deep Dive
# MAGIC
# MAGIC ## 👶 ELI5 Explanation:
# MAGIC
# MAGIC Think of a conveyor belt in a factory:
# MAGIC - New items keep arriving
# MAGIC - Workers only process **new items**
# MAGIC - They don't re-check items they already processed
# MAGIC - They remember where they left off (using a bookmark)
# MAGIC
# MAGIC Auto Loader uses a **checkpoint** as its "bookmark"!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Incremental ingestion** is the cornerstone of modern data engineering, eliminating the need for full table scans and reprocessing.
# MAGIC
# MAGIC ### How Auto Loader Achieves Incremental Processing:
# MAGIC
# MAGIC 1. **Checkpoint Mechanism:**
# MAGIC    - Maintains offset information in a checkpoint directory
# MAGIC    - Tracks which files have been processed
# MAGIC    - Enables exactly-once processing semantics
# MAGIC
# MAGIC 2. **File State Tracking:**
# MAGIC    - Records file metadata (path, size, modification time)
# MAGIC    - Stores processing state in RocksDB
# MAGIC    - Survives cluster restarts
# MAGIC
# MAGIC 3. **Processing Guarantee:**
# MAGIC    - Files are processed exactly once
# MAGIC    - No duplicates even with retries
# MAGIC    - No data loss even with failures
# MAGIC
# MAGIC ### Benefits:
# MAGIC
# MAGIC * **Cost Efficiency:** Pay only for new data processing
# MAGIC * **Performance:** Sub-second latency for new file detection
# MAGIC * **Scalability:** Handles millions of files
# MAGIC * **Reliability:** Fault-tolerant with automatic recovery