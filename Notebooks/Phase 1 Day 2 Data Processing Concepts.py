# Databricks notebook source
# DBTITLE 1,Notebook Header and Documentation
# MAGIC %md
# MAGIC <div style="text-align: center; padding: 20px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; border-radius: 10px; margin-bottom: 20px;">
# MAGIC   <h1 style="font-size: 3em; margin: 0; text-shadow: 2px 2px 4px rgba(0,0,0,0.3);">⚡ TRRaveendra ⚡</h1>
# MAGIC   <h3 style="margin: 10px 0; font-weight: 300;">Data Engineering Training Series</h3>
# MAGIC   <p style="font-size: 0.9em; opacity: 0.9; margin: 5px 0;">Databricks Certified | AWS Solutions Architect | PySpark Expert</p>
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 📘 Notebook Documentation
# MAGIC
# MAGIC ## 📋 Metadata
# MAGIC
# MAGIC | Property | Details |
# MAGIC |----------|----------|
# MAGIC | **Course** | Databricks Data Engineering Fundamentals |
# MAGIC | **Phase** | Phase 1 - Foundations |
# MAGIC | **Day** | Day 2 - Data Processing Concepts |
# MAGIC | **Author** | TRRaveendra |
# MAGIC | **Version** | 1.0 |
# MAGIC | **Last Updated** | April 2026 |
# MAGIC | **Platform** | Databricks on AWS |
# MAGIC | **Runtime** | Python (PySpark) |
# MAGIC | **Difficulty** | Beginner to Intermediate |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Course Overview
# MAGIC
# MAGIC This notebook is part of a comprehensive **Data Engineering Training Program** designed to build production-ready skills on the Databricks platform.
# MAGIC
# MAGIC ### Training Series Structure:
# MAGIC * **Day 1**: Databricks Fundamentals & Workspace Navigation
# MAGIC * **Day 2**: Data Processing Concepts *(Current Notebook)*
# MAGIC * **Day 3**: Delta Lake & Advanced Transformations
# MAGIC * **Day 4**: Production Pipeline Development
# MAGIC * **Day 5**: Performance Tuning & Optimization
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 What You'll Learn Today
# MAGIC
# MAGIC This comprehensive notebook covers **5 core data engineering concepts**:
# MAGIC
# MAGIC ### 1️⃣ **ETL vs ELT**
# MAGIC * Traditional ETL (Extract, Transform, Load)
# MAGIC * Modern ELT (Extract, Load, Transform)
# MAGIC * When to use each approach
# MAGIC * Medallion Architecture (Bronze/Silver/Gold)
# MAGIC
# MAGIC ### 2️⃣ **Batch vs Streaming Processing**
# MAGIC * Batch processing patterns
# MAGIC * Streaming/real-time processing
# MAGIC * Use cases and trade-offs
# MAGIC * Micro-batch concepts
# MAGIC
# MAGIC ### 3️⃣ **Data Types**
# MAGIC * Structured data (CSV, Parquet)
# MAGIC * Semi-structured data (JSON, XML)
# MAGIC * Unstructured data (Text, Images)
# MAGIC * Processing strategies for each
# MAGIC
# MAGIC ### 4️⃣ **Distributed Computing**
# MAGIC * Apache Spark architecture
# MAGIC * Driver and Executor roles
# MAGIC * Partitioning strategies
# MAGIC * Parallel processing concepts
# MAGIC
# MAGIC ### 5️⃣ **Hands-on Pipeline**
# MAGIC * Complete end-to-end data pipeline
# MAGIC * Bronze → Silver → Gold layers
# MAGIC * Data quality validation
# MAGIC * Production best practices
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎓 Learning Outcomes
# MAGIC
# MAGIC By completing this notebook, you will be able to:
# MAGIC
# MAGIC ✅ Explain the difference between ETL and ELT patterns  
# MAGIC ✅ Choose appropriate processing paradigms (batch vs streaming)  
# MAGIC ✅ Handle all data types in a unified lakehouse  
# MAGIC ✅ Understand Spark's distributed execution model  
# MAGIC ✅ Build production-grade data pipelines  
# MAGIC ✅ Apply medallion architecture principles  
# MAGIC ✅ Optimize Spark jobs with partitioning  
# MAGIC ✅ Avoid common data engineering mistakes  
# MAGIC ✅ Answer technical interview questions confidently  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Notebook Contents
# MAGIC
# MAGIC | Section | Cells | Type | Description |
# MAGIC |---------|-------|------|-------------|
# MAGIC | **Header** | 1 | Documentation | This header with metadata |
# MAGIC | **Section 1** | 5 | ETL vs ELT | Concept + Demos |
# MAGIC | **Section 2** | 4 | Batch vs Streaming | Concept + Demos |
# MAGIC | **Section 3** | 5 | Data Types | Concept + Demos |
# MAGIC | **Section 4** | 6 | Distributed Computing | Concept + Demos |
# MAGIC | **Section 5** | 6 | Mini Pipeline | Hands-on Project |
# MAGIC | **Supplementary** | 7 | Reference Material | Summary, Q&A, Tips |
# MAGIC | **Total** | **34 cells** | **Mixed** | **~3 hour training** |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Technical Requirements
# MAGIC
# MAGIC ### Prerequisites:
# MAGIC * Basic Python programming knowledge
# MAGIC * Understanding of SQL fundamentals
# MAGIC * Familiarity with data formats (CSV, JSON)
# MAGIC * Basic understanding of databases
# MAGIC
# MAGIC ### Infrastructure:
# MAGIC * **Platform**: Databricks Workspace
# MAGIC * **Cloud**: AWS (S3, EC2)
# MAGIC * **Compute**: Serverless cluster (auto-provisioned)
# MAGIC * **Runtime**: Latest DBR with PySpark
# MAGIC * **Storage**: Delta Lake format
# MAGIC
# MAGIC ### Libraries Used:
# MAGIC ```python
# MAGIC - pyspark.sql (DataFrames API)
# MAGIC - pyspark.sql.functions (Transformations)
# MAGIC - pyspark.sql.types (Schema definitions)
# MAGIC - delta.tables (Delta Lake operations)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📖 How to Use This Notebook
# MAGIC
# MAGIC ### For Students:
# MAGIC 1. **Read sequentially** - Start from Section 1 and progress through
# MAGIC 2. **Run all cells** - Execute code to see live demonstrations
# MAGIC 3. **Take notes** - Add your observations in new markdown cells
# MAGIC 4. **Experiment** - Modify code and observe results
# MAGIC 5. **Test yourself** - Complete interview questions at the end
# MAGIC
# MAGIC ### For Instructors:
# MAGIC 1. Use as **teaching material** for live sessions
# MAGIC 2. **Customize examples** with organization-specific data
# MAGIC 3. **Extend sections** with additional use cases
# MAGIC 4. **Track progress** using completion checklist
# MAGIC
# MAGIC ### For Reference:
# MAGIC * Quick lookup for **ETL/ELT patterns**
# MAGIC * Code templates for **data pipelines**
# MAGIC * Interview preparation material
# MAGIC * Best practices documentation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏗️ Notebook Structure
# MAGIC
# MAGIC ```
# MAGIC 📘 Day 2: Data Processing Concepts
# MAGIC │
# MAGIC ├── 📍 Header (This Cell)
# MAGIC │   └── Metadata, Overview, Prerequisites
# MAGIC │
# MAGIC ├── 🟦 Section 1: ETL vs ELT
# MAGIC │   ├── Concept explanation
# MAGIC │   ├── Comparison tables
# MAGIC │   ├── ETL demo (transform before load)
# MAGIC │   ├── ELT demo (load then transform)
# MAGIC │   └── Key takeaways
# MAGIC │
# MAGIC ├── 🟩 Section 2: Batch vs Streaming
# MAGIC │   ├── Concept explanation
# MAGIC │   ├── Batch processing demo
# MAGIC │   ├── Streaming simulation demo
# MAGIC │   └── Real-world use cases
# MAGIC │
# MAGIC ├── 🟨 Section 3: Data Types
# MAGIC │   ├── Structured data demo (CSV)
# MAGIC │   ├── Semi-structured demo (JSON)
# MAGIC │   ├── Unstructured demo (Text)
# MAGIC │   └── Processing strategies
# MAGIC │
# MAGIC ├── 🟪 Section 4: Distributed Computing
# MAGIC │   ├── Spark architecture
# MAGIC │   ├── Partitioning demos
# MAGIC │   ├── Parallel processing
# MAGIC │   └── Execution plans
# MAGIC │
# MAGIC ├── 🟧 Section 5: Mini Pipeline
# MAGIC │   ├── Bronze layer (raw ingestion)
# MAGIC │   ├── Silver layer (transformation)
# MAGIC │   ├── Gold layer (aggregation)
# MAGIC │   └── Validation and testing
# MAGIC │
# MAGIC └── 📚 Supplementary Material
# MAGIC     ├── Genie Code usage
# MAGIC     ├── Summary & key learnings
# MAGIC     ├── 15 interview questions
# MAGIC     ├── 10 common mistakes
# MAGIC     ├── Auto Loader bonus
# MAGIC     └── Navigation guide
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⏱️ Estimated Time
# MAGIC
# MAGIC | Activity | Duration |
# MAGIC |----------|----------|
# MAGIC | Reading concepts | 45 minutes |
# MAGIC | Running demos | 60 minutes |
# MAGIC | Hands-on pipeline | 45 minutes |
# MAGIC | Review & practice | 30 minutes |
# MAGIC | **Total** | **3 hours** |
# MAGIC
# MAGIC *Add 2-3 hours for experimentation and interview question practice*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Version History
# MAGIC
# MAGIC | Version | Date | Changes | Author |
# MAGIC |---------|------|---------|--------|
# MAGIC | 1.0 | April 2026 | Initial release with 34 cells | TRRaveendra |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📧 Contact & Support
# MAGIC
# MAGIC **Instructor**: TRRaveendra  
# MAGIC **Specialization**: Databricks Data Engineering, AWS Cloud Architecture, Apache Spark  
# MAGIC **Certifications**: 
# MAGIC * Databricks Certified Data Engineer Associate
# MAGIC * AWS Solutions Architect
# MAGIC * Apache Spark Developer
# MAGIC
# MAGIC **For Questions**:
# MAGIC * Course-related: Use Databricks workspace chat
# MAGIC * Technical issues: Review documentation links in each section
# MAGIC * Feedback: Add comments in notebook cells
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚖️ License & Usage
# MAGIC
# MAGIC © 2026 TRRaveendra - Data Engineering Training Materials
# MAGIC
# MAGIC **Usage Rights**:
# MAGIC * ✅ Free to use for **personal learning**
# MAGIC * ✅ Free to use for **internal training** within organizations
# MAGIC * ✅ Can **modify and extend** for your needs
# MAGIC * ❌ **Do not redistribute** without attribution
# MAGIC * ❌ **Do not use for commercial training** without permission
# MAGIC
# MAGIC **Attribution**: When using this material, please credit:
# MAGIC > *"Based on training materials by TRRaveendra - Databricks Data Engineering Series"*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Success Tips
# MAGIC
# MAGIC 💡 **Take breaks** - This is intensive material, pace yourself  
# MAGIC 💡 **Practice actively** - Don't just read, run every cell  
# MAGIC 💡 **Ask questions** - Use Genie Code AI assistant for help  
# MAGIC 💡 **Build projects** - Apply concepts to your own datasets  
# MAGIC 💡 **Join community** - Engage with Databricks community forums  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Ready to Begin?
# MAGIC
# MAGIC Scroll down to start with **Section 1: ETL vs ELT**
# MAGIC
# MAGIC **Remember**: Data engineering is learned by doing. Run every cell, experiment fearlessly, and build something amazing!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC <div style="text-align: center; padding: 15px; background-color: #f0f0f0; border-left: 5px solid #667eea; margin-top: 20px;">
# MAGIC   <strong>🔥 Let's build production-grade data pipelines! 🔥</strong>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Title and Introduction
# MAGIC %md
# MAGIC # PHASE 1 — DAY 2: Data Processing Concepts
# MAGIC
# MAGIC ## 🎯 Learning Objectives
# MAGIC
# MAGIC By the end of this notebook, you will understand:
# MAGIC * **ETL vs ELT** — Two fundamental data integration patterns
# MAGIC * **Batch vs Streaming** — Processing paradigms for different use cases
# MAGIC * **Data Types** — Structured, Semi-structured, and Unstructured data
# MAGIC * **Distributed Computing** — Introduction to Apache Spark architecture
# MAGIC * **Hands-on Pipeline** — Building a mini data pipeline
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Prerequisites
# MAGIC * Basic understanding of databases
# MAGIC * Familiarity with Python
# MAGIC * Understanding of data formats (CSV, JSON)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏗️ Architecture Context
# MAGIC This training aligns with the **Lakehouse Architecture** — a unified platform that combines the best of data lakes and data warehouses.

# COMMAND ----------

# DBTITLE 1,Notebook Navigation Guide
# MAGIC %md
# MAGIC # 🗂️ Notebook Navigation Guide
# MAGIC
# MAGIC ## 📚 Table of Contents
# MAGIC
# MAGIC This notebook contains **30+ cells** organized into the following sections:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟦 **Section 1: ETL vs ELT**
# MAGIC * Concept explanation (ELI5 + Architect level)
# MAGIC * Comparison table
# MAGIC * Demo: ETL-style transformation
# MAGIC * Demo: ELT-style transformation
# MAGIC * Key takeaways
# MAGIC
# MAGIC **Key Learning**: Load raw data first (ELT), transform later in the lakehouse
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟩 **Section 2: Batch vs Streaming Systems**
# MAGIC * Concept explanation (ELI5 + Architect level)
# MAGIC * Comparison table
# MAGIC * Demo: Batch processing with CSV
# MAGIC * Demo: Simulated streaming (micro-batches)
# MAGIC * Real-world use cases
# MAGIC
# MAGIC **Key Learning**: Use batch by default; add streaming only for real-time requirements
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟨 **Section 3: Data Types**
# MAGIC * Structured, Semi-structured, Unstructured definitions
# MAGIC * Demo: Structured data (CSV)
# MAGIC * Demo: Semi-structured data (JSON with flattening)
# MAGIC * Demo: Unstructured data (text processing)
# MAGIC * Data types summary
# MAGIC
# MAGIC **Key Learning**: Lakehouse handles all three data types in one platform
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟪 **Section 4: Distributed Computing**
# MAGIC * Why distributed computing is needed
# MAGIC * Spark architecture (Driver, Executors)
# MAGIC * Demo: Understanding partitions
# MAGIC * Demo: Repartitioning for parallelism
# MAGIC * Demo: Parallel transformations
# MAGIC * Demo: Spark execution plan
# MAGIC * Key concepts summary
# MAGIC
# MAGIC **Key Learning**: Spark distributes work across executors for parallel processing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟧 **Section 5: Hands-on Mini Pipeline**
# MAGIC * Business scenario (E-commerce analytics)
# MAGIC * Bronze layer: Raw data ingestion (CSV + JSON)
# MAGIC * Silver layer: Cleaning and transformation
# MAGIC * Gold layer: Business aggregates
# MAGIC * Pipeline validation
# MAGIC * Architecture diagram
# MAGIC
# MAGIC **Key Learning**: Complete ELT pipeline with Medallion architecture
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧞 **Genie Code Agent Usage**
# MAGIC * What is Genie Code?
# MAGIC * Example prompts for data engineering tasks
# MAGIC * Best practices for AI-assisted development
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎓 **Final Summary**
# MAGIC * Core concepts recap
# MAGIC * Lakehouse advantages
# MAGIC * Production pipeline checklist
# MAGIC * Next steps for learning
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 **Interview Questions**
# MAGIC * **Beginner**: 5 questions (ETL/ELT, data types, batch/streaming, etc.)
# MAGIC * **Intermediate**: 5 questions (Spark architecture, schema, Delta Lake, etc.)
# MAGIC * **Advanced**: 5 questions (Pipeline design, optimization, SCD, etc.)
# MAGIC
# MAGIC **Total**: 15 interview questions with detailed answers
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ **Common Mistakes**
# MAGIC * 10 common pitfalls with examples
# MAGIC * Wrong vs Right code comparisons
# MAGIC * Best practices summary
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎁 **Bonus: Auto Loader**
# MAGIC * Light overview of Auto Loader
# MAGIC * When to use it
# MAGIC * Comparison with traditional batch
# MAGIC * Production patterns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Notebook Statistics
# MAGIC
# MAGIC | Metric | Count |
# MAGIC |--------|-------|
# MAGIC | **Total Cells** | 35+ |
# MAGIC | **Markdown Cells** | ~15 (concepts, explanations) |
# MAGIC | **Code Cells** | ~20 (hands-on demonstrations) |
# MAGIC | **Sections** | 5 major + 4 supplementary |
# MAGIC | **Demonstrations** | 15+ practical examples |
# MAGIC | **Interview Questions** | 15 (with answers) |
# MAGIC | **Common Mistakes** | 10 (with solutions) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Completion Checklist
# MAGIC
# MAGIC After working through this notebook, you should be able to:
# MAGIC
# MAGIC - [ ] Explain the difference between ETL and ELT
# MAGIC - [ ] Choose between batch and streaming processing
# MAGIC - [ ] Handle structured, semi-structured, and unstructured data
# MAGIC - [ ] Understand Spark's distributed architecture
# MAGIC - [ ] Build a complete Bronze/Silver/Gold pipeline
# MAGIC - [ ] Optimize Spark jobs with partitioning
# MAGIC - [ ] Avoid common data engineering mistakes
# MAGIC - [ ] Use Genie Code for AI-assisted development
# MAGIC - [ ] Answer interview questions confidently
# MAGIC - [ ] Understand when to use Auto Loader
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 How to Use This Notebook
# MAGIC
# MAGIC ### For Learning:
# MAGIC 1. **Read each section sequentially** (top to bottom)
# MAGIC 2. **Run all code cells** to see demonstrations
# MAGIC 3. **Experiment** by modifying code and re-running
# MAGIC 4. **Test yourself** with interview questions
# MAGIC 5. **Review common mistakes** to avoid them
# MAGIC
# MAGIC ### For Reference:
# MAGIC * Use this as a **quick reference guide**
# MAGIC * Jump to specific sections using the table of contents
# MAGIC * Copy code patterns for your own projects
# MAGIC * Review interview questions before technical discussions
# MAGIC
# MAGIC ### For Practice:
# MAGIC * **Modify demos** with your own data
# MAGIC * **Build similar pipelines** from scratch
# MAGIC * **Combine concepts** (e.g., streaming + medallion architecture)
# MAGIC * **Optimize code** and measure performance improvements
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💬 Feedback & Contributions
# MAGIC
# MAGIC This is a **living document**. As you learn:
# MAGIC * Add your own notes and examples
# MAGIC * Extend demos with additional features
# MAGIC * Document your learnings
# MAGIC * Share with your team
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌟 Final Thoughts
# MAGIC
# MAGIC > **"Understanding concepts is good. Implementing them is better. Teaching them is mastery."**
# MAGIC
# MAGIC Now go build something amazing with Databricks! 🚀
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **End of Day 2: Data Processing Concepts**  
# MAGIC **Ready for Day 3!** 👍

# COMMAND ----------

# DBTITLE 1,Bonus: Auto Loader Overview
# MAGIC %md
# MAGIC # 🎁 BONUS: Auto Loader — Light Overview
# MAGIC
# MAGIC ## What is Auto Loader?
# MAGIC
# MAGIC Auto Loader is a **Databricks feature** that simplifies incremental data ingestion from cloud storage.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 (Explain Like I'm 5)
# MAGIC
# MAGIC Imagine you have a mailbox that gets new letters every day:
# MAGIC * **Without Auto Loader**: You have to check which letters are new yourself, every single time
# MAGIC * **With Auto Loader**: The mailbox automatically tells you "Hey, you have 3 NEW letters!" and only gives you those
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Why Auto Loader?
# MAGIC
# MAGIC ### The Problem
# MAGIC ```python
# MAGIC # Traditional approach: List all files, track which ones processed
# MAGIC all_files = dbutils.fs.ls("/data/")
# MAGIC processed_files = get_from_checkpoint()  # ❌ Manual tracking
# MAGIC new_files = [f for f in all_files if f not in processed_files]  # ❌ Slow
# MAGIC for file in new_files:
# MAGIC     process(file)  # ❌ Complex logic
# MAGIC ```
# MAGIC
# MAGIC ### The Auto Loader Solution
# MAGIC ```python
# MAGIC # Auto Loader: Automatically detects and processes only new files
# MAGIC df = spark.readStream \
# MAGIC     .format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "json") \
# MAGIC     .load("/data/")
# MAGIC
# MAGIC df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .option("checkpointLocation", "/checkpoints/") \
# MAGIC     .start("/output/")
# MAGIC
# MAGIC # ✅ Auto Loader handles everything!
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Key Features
# MAGIC
# MAGIC | Feature | Benefit |
# MAGIC |---------|----------|
# MAGIC | **Incremental Processing** | Only processes new files (not entire directory) |
# MAGIC | **Schema Inference** | Automatically detects schema changes |
# MAGIC | **Schema Evolution** | Handles new columns gracefully |
# MAGIC | **Efficient File Discovery** | Uses cloud notifications (faster than listing) |
# MAGIC | **Exactly-Once Processing** | No duplicate processing |
# MAGIC | **Works with All Formats** | CSV, JSON, Parquet, Avro, etc. |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Traditional Batch vs Auto Loader
# MAGIC
# MAGIC ### Traditional Batch
# MAGIC ```python
# MAGIC # Read all files every time (inefficient for large directories)
# MAGIC df = spark.read.format("json").load("/data/*.json")
# MAGIC df.write.format("delta").mode("append").save("/output/")
# MAGIC # ❌ Problem: Re-reads old files, needs manual deduplication
# MAGIC ```
# MAGIC
# MAGIC ### Auto Loader (Incremental)
# MAGIC ```python
# MAGIC # Only reads NEW files automatically
# MAGIC df = spark.readStream \
# MAGIC     .format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "json") \
# MAGIC     .load("/data/")
# MAGIC
# MAGIC df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .option("checkpointLocation", "/checkpoints/") \
# MAGIC     .start("/output/")
# MAGIC # ✅ Automatically tracks and processes only new files
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚙️ How It Works
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │  Cloud Storage (S3, ADLS, GCS)         │
# MAGIC │  /data/                                 │
# MAGIC │    file1.json (old)                     │
# MAGIC │    file2.json (old)                     │
# MAGIC │    file3.json (NEW!)  ← Auto Loader    │
# MAGIC │    file4.json (NEW!)     detects these  │
# MAGIC └───────────────┬─────────────────────────┘
# MAGIC                │
# MAGIC                │ Processes only NEW files
# MAGIC                │
# MAGIC                ▼
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │  Delta Table (Output)                  │
# MAGIC │  /output/                              │
# MAGIC │  (Only file3 and file4 appended)      │
# MAGIC └────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC 1. **Monitor**: Auto Loader watches the directory
# MAGIC 2. **Detect**: New files trigger processing
# MAGIC 3. **Process**: Only new data is read and transformed
# MAGIC 4. **Checkpoint**: Tracks processed files (no reprocessing)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💻 Simple Example
# MAGIC
# MAGIC ```python
# MAGIC # Bronze Layer: Ingest raw files with Auto Loader
# MAGIC df = spark.readStream \
# MAGIC     .format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "csv") \
# MAGIC     .option("cloudFiles.schemaLocation", "/schemas/orders") \
# MAGIC     .load("/landing_zone/orders/")
# MAGIC
# MAGIC df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .option("checkpointLocation", "/checkpoints/bronze_orders") \
# MAGIC     .start("/bronze/orders/")
# MAGIC ```
# MAGIC
# MAGIC **What happens:**
# MAGIC * Auto Loader monitors `/landing_zone/orders/`
# MAGIC * When new CSV files arrive, they're automatically processed
# MAGIC * Data written to `/bronze/orders/` Delta table
# MAGIC * Checkpoint ensures exactly-once processing
# MAGIC * Schema stored in `/schemas/orders` for evolution
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 When to Use Auto Loader
# MAGIC
# MAGIC ✅ **Perfect for:**
# MAGIC * **Incremental data ingestion** from cloud storage
# MAGIC * **Landing zones** where files continuously arrive
# MAGIC * **Bronze layer ingestion** in medallion architecture
# MAGIC * **Large directories** with many files (more efficient than listing)
# MAGIC * **Schema evolution** scenarios (new columns added over time)
# MAGIC
# MAGIC ❌ **Not needed when:**
# MAGIC * Processing a static dataset (use batch read)
# MAGIC * Data arrives via streaming sources (Kafka, Event Hubs)
# MAGIC * Small directories with few files
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔑 Auto Loader vs Structured Streaming
# MAGIC
# MAGIC | Feature | Auto Loader | Structured Streaming |
# MAGIC |---------|-------------|----------------------|
# MAGIC | **Source** | Cloud files (S3, ADLS, GCS) | Kafka, Event Hubs, Delta |
# MAGIC | **Use Case** | File-based ingestion | Stream processing |
# MAGIC | **Schema** | Auto-inferred + evolution | Must define |
# MAGIC | **File Discovery** | Optimized (notifications) | Standard listing |
# MAGIC | **Best For** | Landing zone ingestion | Real-time streams |
# MAGIC
# MAGIC **Note**: Auto Loader IS a type of Structured Streaming — it uses `readStream` but optimized for files!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Production Pattern
# MAGIC
# MAGIC ```python
# MAGIC # Complete Auto Loader pipeline
# MAGIC
# MAGIC # 1. Bronze: Ingest raw files
# MAGIC bronze_df = spark.readStream \
# MAGIC     .format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "json") \
# MAGIC     .option("cloudFiles.schemaLocation", "/schemas/bronze") \
# MAGIC     .option("cloudFiles.inferColumnTypes", "true") \
# MAGIC     .load("/landing_zone/data/")
# MAGIC
# MAGIC bronze_df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .option("checkpointLocation", "/checkpoints/bronze") \
# MAGIC     .start("/bronze/data/")
# MAGIC
# MAGIC # 2. Silver: Transform from bronze
# MAGIC silver_df = spark.readStream \
# MAGIC     .format("delta") \
# MAGIC     .load("/bronze/data/") \
# MAGIC     .filter(col("status") == "active") \
# MAGIC     .withColumn("processed_at", current_timestamp())
# MAGIC
# MAGIC silver_df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .option("checkpointLocation", "/checkpoints/silver") \
# MAGIC     .start("/silver/data/")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Learn More
# MAGIC
# MAGIC * **Day 3+ Topics**: Deep dive into Auto Loader
# MAGIC * **Documentation**: [docs.databricks.com/ingestion/auto-loader](https://docs.databricks.com/ingestion/auto-loader)
# MAGIC * **Tutorials**: Databricks Academy courses
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Key Takeaway
# MAGIC
# MAGIC Auto Loader simplifies incremental data ingestion:
# MAGIC * **No manual file tracking**
# MAGIC * **Efficient for large directories**
# MAGIC * **Handles schema evolution**
# MAGIC * **Perfect for Bronze layer ingestion**
# MAGIC
# MAGIC **Next Steps**: In Day 3, you'll learn to build production pipelines with Auto Loader!

# COMMAND ----------

# DBTITLE 1,Genie Code Agent Usage
# MAGIC %md
# MAGIC # 🧞 Genie Code Agent: AI-Powered Development
# MAGIC
# MAGIC ## What is Genie Code?
# MAGIC
# MAGIC Genie Code is your AI assistant within Databricks that helps you:
# MAGIC * Write PySpark and SQL code
# MAGIC * Generate data pipelines
# MAGIC * Debug and optimize queries
# MAGIC * Explain complex code
# MAGIC * Follow best practices
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💬 Example Prompts for This Training
# MAGIC
# MAGIC ### ETL/ELT Pipelines
# MAGIC
# MAGIC ```
# MAGIC 💬 "Generate an ELT pipeline that loads raw CSV data to bronze, 
# MAGIC     cleans it in silver, and creates daily aggregates in gold"
# MAGIC
# MAGIC 💬 "Convert this ETL pipeline to use ELT approach with 
# MAGIC     bronze/silver/gold layers"
# MAGIC
# MAGIC 💬 "Create a medallion architecture pipeline for customer data"
# MAGIC ```
# MAGIC
# MAGIC ### Batch vs Streaming
# MAGIC
# MAGIC ```
# MAGIC 💬 "Create a batch job that processes daily sales data and 
# MAGIC     writes to Delta table"
# MAGIC
# MAGIC 💬 "Convert this batch processing code to streaming using 
# MAGIC     Auto Loader"
# MAGIC
# MAGIC 💬 "Generate a streaming pipeline that processes JSON events 
# MAGIC     in real-time"
# MAGIC ```
# MAGIC
# MAGIC ### Data Type Handling
# MAGIC
# MAGIC ```
# MAGIC 💬 "Read this JSON file, flatten the nested structures, and 
# MAGIC     write to Delta"
# MAGIC
# MAGIC 💬 "Infer schema from this CSV and create a structured table"
# MAGIC
# MAGIC 💬 "Parse this unstructured log file and extract key fields"
# MAGIC ```
# MAGIC
# MAGIC ### Distributed Computing
# MAGIC
# MAGIC ```
# MAGIC 💬 "Explain the Spark execution plan for this query"
# MAGIC
# MAGIC 💬 "Optimize this DataFrame by adjusting partitions"
# MAGIC
# MAGIC 💬 "Why is this query slow? Suggest performance improvements"
# MAGIC
# MAGIC 💬 "Show me how to repartition this data for better parallelism"
# MAGIC ```
# MAGIC
# MAGIC ### General Data Engineering
# MAGIC
# MAGIC ```
# MAGIC 💬 "Create a data quality check for null values and duplicates"
# MAGIC
# MAGIC 💬 "Generate a pipeline that joins orders with customer data"
# MAGIC
# MAGIC 💬 "Write a function to incrementally load only new records"
# MAGIC
# MAGIC 💬 "Add error handling and logging to this pipeline"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💎 Best Practices for Using Genie Code
# MAGIC
# MAGIC 1. **Be Specific**: Provide context about your data and requirements
# MAGIC 2. **Iterate**: Start with basic code, then ask for enhancements
# MAGIC 3. **Ask for Explanations**: Request comments and documentation
# MAGIC 4. **Request Best Practices**: Ask about performance and optimization
# MAGIC 5. **Use Examples**: Reference sample data or schemas
# MAGIC
# MAGIC ### Good Prompt Example:
# MAGIC ```
# MAGIC 💬 "Read the CSV file at /tmp/sales.csv with columns 
# MAGIC     (order_id, customer_id, amount, date). Filter for orders > $100, 
# MAGIC     group by customer, calculate total revenue, and write to 
# MAGIC     Delta table 'gold.customer_revenue' with overwrite mode. 
# MAGIC     Add error handling."
# MAGIC ```
# MAGIC
# MAGIC ### Vague Prompt (Avoid):
# MAGIC ```
# MAGIC ❌ "Process some data"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Try It Yourself!
# MAGIC
# MAGIC In the next cell, try asking Genie Code to help you with:
# MAGIC * Creating a new pipeline
# MAGIC * Optimizing existing code
# MAGIC * Explaining Spark concepts
# MAGIC * Debugging issues

# COMMAND ----------

# DBTITLE 1,Final Summary and Key Learnings
# MAGIC %md
# MAGIC # 🎓 Final Summary: Key Learnings
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📌 Core Concepts Mastered
# MAGIC
# MAGIC ### 1. ETL vs ELT
# MAGIC
# MAGIC | Concept | Key Takeaway |
# MAGIC |---------|-------------|
# MAGIC | **ETL** | Transform BEFORE loading (legacy approach) |
# MAGIC | **ELT** | Load raw FIRST, transform LATER (modern lakehouse) |
# MAGIC | **Databricks Way** | ELT with Medallion Architecture (Bronze → Silver → Gold) |
# MAGIC
# MAGIC **Remember**: Raw data in Bronze = flexibility to re-transform for new use cases
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. Batch vs Streaming
# MAGIC
# MAGIC | Processing Type | When to Use |
# MAGIC |----------------|-------------|
# MAGIC | **Batch** | Historical analysis, reports, ML training |
# MAGIC | **Streaming** | Real-time alerts, fraud detection, IoT |
# MAGIC
# MAGIC **Remember**: Most analytics start with batch; add streaming only when real-time is required
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. Data Types
# MAGIC
# MAGIC | Type | Structure | Processing |
# MAGIC |------|-----------|------------|
# MAGIC | **Structured** | Fixed schema | Easy (SQL) |
# MAGIC | **Semi-Structured** | Flexible, nested | Medium (flatten first) |
# MAGIC | **Unstructured** | No schema | Hard (ML/NLP) |
# MAGIC
# MAGIC **Remember**: Lakehouse handles all three types in one platform
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4. Distributed Computing
# MAGIC
# MAGIC **Spark Architecture**:
# MAGIC * **Driver** = Coordinator (your notebook)
# MAGIC * **Executors** = Workers (parallel processing)
# MAGIC * **Partitions** = Data chunks (enable parallelism)
# MAGIC
# MAGIC **Remember**: More partitions = more parallelism (but balance overhead)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 The Lakehouse Advantage
# MAGIC
# MAGIC ```
# MAGIC   Traditional Data Stack          Lakehouse (Databricks)
# MAGIC
# MAGIC ┌─────────────────┐           ┌─────────────────┐
# MAGIC │  Data Warehouse  │           │                 │
# MAGIC │  (Structured)    │           │                 │
# MAGIC └─────────────────┘           │                 │
# MAGIC                             │   Lakehouse       │
# MAGIC ┌─────────────────┐           │                 │
# MAGIC │   Data Lake      │           │   (All types)     │
# MAGIC │  (Unstructured)  │           │                 │
# MAGIC └─────────────────┘           │                 │
# MAGIC                             └─────────────────┘
# MAGIC    Two systems                  One platform
# MAGIC    Complex ETL                  Simple ELT
# MAGIC    Data silos                   Unified data
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Production Pipeline Checklist
# MAGIC
# MAGIC When building production pipelines, ensure:
# MAGIC
# MAGIC - ☑️ **Architecture**: Use Medallion (Bronze/Silver/Gold)
# MAGIC - ☑️ **Storage**: Use Delta Lake for ACID guarantees
# MAGIC - ☑️ **Processing**: Choose batch vs streaming appropriately
# MAGIC - ☑️ **Schema**: Define schema for Silver and Gold layers
# MAGIC - ☑️ **Partitioning**: Optimize for query patterns
# MAGIC - ☑️ **Data Quality**: Add validation and error handling
# MAGIC - ☑️ **Monitoring**: Log pipeline metrics and failures
# MAGIC - ☑️ **Idempotency**: Ensure pipelines can be re-run safely
# MAGIC - ☑️ **Documentation**: Comment code and document schemas
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Next Steps
# MAGIC
# MAGIC 1. **Practice**: Build pipelines with real datasets
# MAGIC 2. **Optimize**: Learn Spark tuning and optimization
# MAGIC 3. **Automate**: Schedule pipelines as jobs
# MAGIC 4. **Scale**: Handle larger datasets (TB+)
# MAGIC 5. **Advanced Topics**: 
# MAGIC    * Streaming with Auto Loader
# MAGIC    * Change Data Capture (CDC)
# MAGIC    * Data quality frameworks
# MAGIC    * Pipeline orchestration

# COMMAND ----------

# DBTITLE 1,Interview Questions
# MAGIC %md
# MAGIC # 🎯 Interview Questions: Test Your Knowledge
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🟢 Beginner Level
# MAGIC
# MAGIC **1. What is the difference between ETL and ELT?**
# MAGIC <details>
# MAGIC <summary>Click to reveal answer</summary>
# MAGIC
# MAGIC * **ETL**: Extract, Transform, Load — data is transformed BEFORE loading into the target system
# MAGIC * **ELT**: Extract, Load, Transform — raw data is loaded FIRST, then transformed inside the target system
# MAGIC * **Modern approach**: ELT is preferred in cloud/lakehouse environments
# MAGIC </details>
# MAGIC
# MAGIC **2. Explain structured, semi-structured, and unstructured data with examples.**
# MAGIC <details>
# MAGIC <summary>Click to reveal answer</summary>
# MAGIC
# MAGIC * **Structured**: Fixed schema with rows/columns (CSV, SQL tables, Parquet)
# MAGIC * **Semi-Structured**: Flexible schema with nested data (JSON, XML, Avro)
# MAGIC * **Unstructured**: No schema (images, videos, text documents, emails)
# MAGIC </details>
# MAGIC
# MAGIC **3. What is the Medallion Architecture?**
# MAGIC <details>
# MAGIC <summary>Click to reveal answer</summary>
# MAGIC
# MAGIC * **Bronze Layer**: Raw data as-is from source (no transformations)
# MAGIC * **Silver Layer**: Cleaned, validated, and conformed data
# MAGIC * **Gold Layer**: Business-level aggregates optimized for analytics
# MAGIC * **Purpose**: Organizes data pipeline with clear separation of concerns
# MAGIC </details>
# MAGIC
# MAGIC **4. When would you use batch processing vs streaming?**
# MAGIC <details>
# MAGIC <summary>Click to reveal answer</summary>
# MAGIC
# MAGIC * **Batch**: Historical analysis, daily reports, ML training, cost-sensitive workloads
# MAGIC * **Streaming**: Real-time alerts, fraud detection, IoT monitoring, clickstream analysis
# MAGIC * **Key difference**: Latency requirements (hours vs seconds)
# MAGIC </details>
# MAGIC
# MAGIC **5. What are Spark partitions and why do they matter?**
# MAGIC <details>
# MAGIC <summary>Click to reveal answer</summary>
# MAGIC
# MAGIC * **Partitions**: Chunks of data distributed across executors
# MAGIC * **Purpose**: Enable parallel processing across multiple machines
# MAGIC * **Why they matter**: More partitions = more parallelism = faster processing (but balance overhead)
# MAGIC * **Rule of thumb**: ~128MB per partition
# MAGIC </details>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🟡 Intermediate Level
# MAGIC
# MAGIC **6. Explain Spark's Driver and Executor roles.**
# MAGIC <details>
# MAGIC <summary>Click to reveal answer</summary>
# MAGIC
# MAGIC * **Driver**: 
# MAGIC   * Coordinates the entire Spark application
# MAGIC   * Converts user code into tasks
# MAGIC   * Schedules tasks to executors
# MAGIC   * Collects and aggregates results
# MAGIC   * Runs on the master node (your notebook)
# MAGIC
# MAGIC * **Executors**:
# MAGIC   * Perform actual data processing
# MAGIC   * Store data partitions in memory
# MAGIC   * Execute tasks assigned by driver
# MAGIC   * Run on worker nodes
# MAGIC   * Can scale to thousands of executors
# MAGIC </details>
# MAGIC
# MAGIC **7. What is schema-on-read vs schema-on-write?**
# MAGIC <details>
# MAGIC <summary>Click to reveal answer</summary>
# MAGIC
# MAGIC * **Schema-on-Write** (ETL, Data Warehouses):
# MAGIC   * Schema enforced when data is written
# MAGIC   * Data must conform to predefined structure
# MAGIC   * Less flexible but ensures data quality
# MAGIC
# MAGIC * **Schema-on-Read** (ELT, Data Lakes):
# MAGIC   * Schema inferred when data is read
# MAGIC   * Raw data stored without transformation
# MAGIC   * More flexible, can re-interpret data later
# MAGIC   * Lakehouse uses both: Bronze (schema-on-read), Silver/Gold (schema-on-write)
# MAGIC </details>
# MAGIC
# MAGIC **8. How does Delta Lake improve upon regular data lakes?**
# MAGIC <details>
# MAGIC <summary>Click to reveal answer</summary>
# MAGIC
# MAGIC * **ACID transactions**: Atomic operations, no partial writes
# MAGIC * **Time travel**: Query historical versions of data
# MAGIC * **Schema enforcement**: Prevent bad data from being written
# MAGIC * **Schema evolution**: Safely add/modify columns
# MAGIC * **Unified batch and streaming**: Single API for both
# MAGIC * **Performance**: Optimized file formats and indexing
# MAGIC </details>
# MAGIC
# MAGIC **9. What causes a shuffle in Spark and why is it expensive?**
# MAGIC <details>
# MAGIC <summary>Click to reveal answer</summary>
# MAGIC
# MAGIC * **Causes**: groupBy, join, repartition, distinct, orderBy
# MAGIC * **Why expensive**:
# MAGIC   * Data must be redistributed across executors
# MAGIC   * Requires network transfer (slow)
# MAGIC   * Requires disk I/O for spilling
# MAGIC   * Breaks pipeline parallelism
# MAGIC * **Optimization**: Filter early, broadcast small tables, avoid unnecessary shuffles
# MAGIC </details>
# MAGIC
# MAGIC **10. What is the difference between repartition() and coalesce()?**
# MAGIC <details>
# MAGIC <summary>Click to reveal answer</summary>
# MAGIC
# MAGIC * **repartition(n)**:
# MAGIC   * Can increase or decrease partitions
# MAGIC   * Performs full shuffle (expensive)
# MAGIC   * Evenly distributes data
# MAGIC   * Use when increasing parallelism
# MAGIC
# MAGIC * **coalesce(n)**:
# MAGIC   * Only decreases partitions
# MAGIC   * Minimizes data movement (no full shuffle)
# MAGIC   * More efficient for reducing partitions
# MAGIC   * Use after filtering to reduce overhead
# MAGIC </details>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔴 Advanced Level
# MAGIC
# MAGIC **11. Design a production-grade data pipeline for real-time fraud detection.**
# MAGIC <details>
# MAGIC <summary>Click to reveal answer</summary>
# MAGIC
# MAGIC * **Source**: Streaming transactions from Kafka/Event Hubs
# MAGIC * **Ingestion**: Auto Loader or Structured Streaming
# MAGIC * **Bronze**: Raw transactions (no transformation)
# MAGIC * **Silver**: 
# MAGIC   * Enriched with customer data (streaming join)
# MAGIC   * Feature engineering (rolling averages, velocity checks)
# MAGIC   * Data quality checks
# MAGIC * **Gold**: 
# MAGIC   * Real-time fraud scores
# MAGIC   * Aggregated metrics for dashboards
# MAGIC * **ML Model**: Serve predictions via Databricks Model Serving
# MAGIC * **Alerts**: Trigger notifications for high-risk transactions
# MAGIC * **Monitoring**: Track pipeline latency, data quality, model performance
# MAGIC </details>
# MAGIC
# MAGIC **12. How would you optimize a slow Spark job?**
# MAGIC <details>
# MAGIC <summary>Click to reveal answer</summary>
# MAGIC
# MAGIC 1. **Analyze execution plan**: Use explain() and Spark UI
# MAGIC 2. **Reduce data early**: Filter and select only needed columns first
# MAGIC 3. **Optimize partitions**: Balance between too few and too many
# MAGIC 4. **Cache wisely**: Cache frequently accessed DataFrames
# MAGIC 5. **Broadcast small tables**: For joins with small dimensions
# MAGIC 6. **Avoid shuffles**: Minimize groupBy/joins, use window functions
# MAGIC 7. **Use columnar formats**: Parquet, Delta for better compression
# MAGIC 8. **Predicate pushdown**: Ensure filters pushed to source
# MAGIC 9. **Increase parallelism**: Add more executors/cores
# MAGIC 10. **Z-order/optimize**: For Delta tables with specific query patterns
# MAGIC </details>
# MAGIC
# MAGIC **13. Explain the difference between narrow and wide transformations.**
# MAGIC <details>
# MAGIC <summary>Click to reveal answer</summary>
# MAGIC
# MAGIC * **Narrow Transformations**:
# MAGIC   * Each input partition contributes to only one output partition
# MAGIC   * No data shuffle required
# MAGIC   * Examples: map, filter, withColumn, select
# MAGIC   * Can be pipelined together
# MAGIC   * Fast and efficient
# MAGIC
# MAGIC * **Wide Transformations**:
# MAGIC   * Each input partition contributes to multiple output partitions
# MAGIC   * Requires shuffle (data redistribution)
# MAGIC   * Examples: groupBy, join, repartition, distinct
# MAGIC   * Breaks pipeline, creates stage boundaries
# MAGIC   * Expensive operations
# MAGIC </details>
# MAGIC
# MAGIC **14. How would you handle slowly changing dimensions (SCD) in a lakehouse?**
# MAGIC <details>
# MAGIC <summary>Click to reveal answer</summary>
# MAGIC
# MAGIC * **SCD Type 1** (Overwrite):
# MAGIC   * Use MERGE to update existing records
# MAGIC   * No history preserved
# MAGIC   
# MAGIC * **SCD Type 2** (History):
# MAGIC   * Add version columns (effective_date, end_date, is_current)
# MAGIC   * Use MERGE with conditional logic
# MAGIC   * Keep full history of changes
# MAGIC   * Example: Customer address changes over time
# MAGIC   
# MAGIC * **Implementation**:
# MAGIC   * Use Delta Lake MERGE operation
# MAGIC   * Add surrogate keys for Type 2
# MAGIC   * Implement in Silver or Gold layer
# MAGIC   * Consider time-travel for auditing
# MAGIC </details>
# MAGIC
# MAGIC **15. What are the trade-offs between ELT and ETL in modern data architectures?**
# MAGIC <details>
# MAGIC <summary>Click to reveal answer</summary>
# MAGIC
# MAGIC **ELT Advantages**:
# MAGIC * Preserves raw data for multiple use cases
# MAGIC * Leverages scalable cloud compute
# MAGIC * Faster time-to-insight (load first, transform later)
# MAGIC * Schema flexibility (schema-on-read)
# MAGIC * Cost-effective (pay for compute when needed)
# MAGIC
# MAGIC **ELT Challenges**:
# MAGIC * Requires robust data governance
# MAGIC * Storage costs for raw data
# MAGIC * Potential data quality issues if not validated
# MAGIC * Requires education on medallion architecture
# MAGIC
# MAGIC **When ETL still makes sense**:
# MAGIC * Legacy systems with limited target capacity
# MAGIC * Regulatory requirements for pre-processing
# MAGIC * Source systems that require specific transformation logic
# MAGIC * Small data volumes where ELT overhead isn't justified
# MAGIC
# MAGIC **Best Practice**: Use ELT as default in cloud/lakehouse, fall back to ETL only when specific constraints require it
# MAGIC </details>

# COMMAND ----------

# DBTITLE 1,Common Mistakes and Pitfalls
# MAGIC %md
# MAGIC # ⚠️ Common Mistakes and How to Avoid Them
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Mistake 1: Confusing ETL with ELT
# MAGIC
# MAGIC **Problem**:
# MAGIC ```python
# MAGIC # Transforming data BEFORE writing to bronze (wrong!)
# MAGIC df = spark.read.csv("source.csv")
# MAGIC transformed = df.filter(...).withColumn(...)  # ❌ Transforming
# MAGIC transformed.write.save("/bronze/data")  # ❌ Writing transformed to bronze
# MAGIC ```
# MAGIC
# MAGIC **Solution**:
# MAGIC ```python
# MAGIC # ELT: Load raw to bronze FIRST
# MAGIC raw_df = spark.read.csv("source.csv")
# MAGIC raw_df.write.save("/bronze/data")  # ✅ Raw data to bronze
# MAGIC
# MAGIC # THEN transform in silver
# MAGIC bronze = spark.read.load("/bronze/data")
# MAGIC silver = bronze.filter(...).withColumn(...)  # ✅ Transform from bronze
# MAGIC silver.write.save("/silver/data")
# MAGIC ```
# MAGIC
# MAGIC **Remember**: Bronze = raw data only, no transformations!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Mistake 2: Using Streaming for Batch Use Cases
# MAGIC
# MAGIC **Problem**:
# MAGIC ```python
# MAGIC # Using streaming for daily reports (unnecessary complexity)
# MAGIC stream_df = spark.readStream.format("delta").load("/data")  # ❌ Overkill
# MAGIC stream_df.writeStream.format("delta").start("/output")  # ❌ Not needed
# MAGIC ```
# MAGIC
# MAGIC **Solution**:
# MAGIC ```python
# MAGIC # Batch processing is simpler for scheduled jobs
# MAGIC batch_df = spark.read.format("delta").load("/data")  # ✅ Simple batch read
# MAGIC batch_df.write.format("delta").mode("overwrite").save("/output")  # ✅ Batch write
# MAGIC ```
# MAGIC
# MAGIC **Remember**: Use batch by default; add streaming only when real-time is required
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Mistake 3: Ignoring Schema Handling
# MAGIC
# MAGIC **Problem**:
# MAGIC ```python
# MAGIC # Relying on schema inference every time (slow and error-prone)
# MAGIC df = spark.read.option("inferSchema", "true").csv("/data")  # ❌ Slow
# MAGIC ```
# MAGIC
# MAGIC **Solution**:
# MAGIC ```python
# MAGIC # Define schema explicitly (faster and more reliable)
# MAGIC from pyspark.sql.types import *
# MAGIC
# MAGIC schema = StructType([
# MAGIC     StructField("id", IntegerType(), False),
# MAGIC     StructField("name", StringType(), True),
# MAGIC     StructField("amount", DoubleType(), True)
# MAGIC ])
# MAGIC
# MAGIC df = spark.read.schema(schema).csv("/data")  # ✅ Explicit schema
# MAGIC ```
# MAGIC
# MAGIC **Remember**: Define schemas for production pipelines (Silver/Gold layers)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Mistake 4: Not Understanding Partitions
# MAGIC
# MAGIC **Problem**:
# MAGIC ```python
# MAGIC # Reading data with default partitioning (may be inefficient)
# MAGIC df = spark.read.parquet("/large_dataset")  # ❌ Default partitions
# MAGIC df.count()  # Slow on large data
# MAGIC ```
# MAGIC
# MAGIC **Solution**:
# MAGIC ```python
# MAGIC # Optimize partitions for your cluster
# MAGIC df = spark.read.parquet("/large_dataset")
# MAGIC optimized_df = df.repartition(64)  # ✅ Match executor count
# MAGIC optimized_df.cache()  # Cache with optimized partitions
# MAGIC ```
# MAGIC
# MAGIC **Remember**: Balance partitions - not too few (underutilized), not too many (overhead)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Mistake 5: Using collect() on Large DataFrames
# MAGIC
# MAGIC **Problem**:
# MAGIC ```python
# MAGIC # Collecting millions of rows to driver (OUT OF MEMORY!)
# MAGIC large_df = spark.read.parquet("/huge_dataset")
# MAGIC data = large_df.collect()  # ❌ Driver crash!
# MAGIC for row in data:  # ❌ Won't reach here
# MAGIC     process(row)
# MAGIC ```
# MAGIC
# MAGIC **Solution**:
# MAGIC ```python
# MAGIC # Process data in distributed manner
# MAGIC large_df = spark.read.parquet("/huge_dataset")
# MAGIC processed = large_df.rdd.map(lambda row: process(row))  # ✅ Distributed
# MAGIC processed.toDF().write.save("/output")  # ✅ Stays distributed
# MAGIC
# MAGIC # Or use Spark transformations
# MAGIC result = large_df.withColumn("new_col", process_udf(col("col")))  # ✅ Spark way
# MAGIC ```
# MAGIC
# MAGIC **Remember**: Avoid collect(), use Spark transformations instead
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Mistake 6: Over-Caching DataFrames
# MAGIC
# MAGIC **Problem**:
# MAGIC ```python
# MAGIC # Caching everything (wastes memory)
# MAGIC df1 = spark.read.csv("/data1").cache()  # ❌ Not reused
# MAGIC df2 = spark.read.csv("/data2").cache()  # ❌ Not reused
# MAGIC df3 = spark.read.csv("/data3").cache()  # ❌ Not reused
# MAGIC result = df1.union(df2).union(df3).write.save("/output")  # Used once!
# MAGIC ```
# MAGIC
# MAGIC **Solution**:
# MAGIC ```python
# MAGIC # Only cache frequently reused DataFrames
# MAGIC df = spark.read.csv("/data")
# MAGIC
# MAGIC # Used multiple times? Cache it
# MAGIC filtered = df.filter(col("status") == "active").cache()  # ✅ Reused below
# MAGIC
# MAGIC result1 = filtered.groupBy("category").count()  # Uses cache
# MAGIC result2 = filtered.groupBy("region").sum("amount")  # Uses cache
# MAGIC result3 = filtered.filter(col("amount") > 100)  # Uses cache
# MAGIC ```
# MAGIC
# MAGIC **Remember**: Cache only when a DataFrame is used multiple times
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Mistake 7: Not Filtering Early
# MAGIC
# MAGIC **Problem**:
# MAGIC ```python
# MAGIC # Loading entire dataset, then filtering (slow)
# MAGIC df = spark.read.parquet("/huge_dataset")  # ❌ 10TB loaded
# MAGIC df = df.select("col1", "col2", "col3")  # ❌ All columns read
# MAGIC df = df.filter(col("date") == "2026-04-21")  # ❌ Filter at end
# MAGIC ```
# MAGIC
# MAGIC **Solution**:
# MAGIC ```python
# MAGIC # Predicate pushdown: filter early
# MAGIC df = spark.read.parquet("/huge_dataset") \
# MAGIC     .select("col1", "col2", "col3") \
# MAGIC     .filter(col("date") == "2026-04-21")  # ✅ Filter pushed to source
# MAGIC
# MAGIC # Even better: partition pruning
# MAGIC df = spark.read.parquet("/huge_dataset")  # ✅ Only reads date=2026-04-21 partition
# MAGIC ```
# MAGIC
# MAGIC **Remember**: Filter and select early to minimize data movement
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Mistake 8: Misunderstanding Lazy Evaluation
# MAGIC
# MAGIC **Problem**:
# MAGIC ```python
# MAGIC # Thinking transformations execute immediately
# MAGIC df = spark.read.csv("/data")
# MAGIC df = df.filter(col("amount") > 100)
# MAGIC df = df.withColumn("doubled", col("amount") * 2)
# MAGIC print("Done!")  # ❌ Nothing executed yet!
# MAGIC ```
# MAGIC
# MAGIC **Solution**:
# MAGIC ```python
# MAGIC # Understanding transformations vs actions
# MAGIC df = spark.read.csv("/data")  # Transformation (lazy)
# MAGIC df = df.filter(col("amount") > 100)  # Transformation (lazy)
# MAGIC df = df.withColumn("doubled", col("amount") * 2)  # Transformation (lazy)
# MAGIC
# MAGIC # Action triggers execution
# MAGIC df.write.save("/output")  # ✅ Action: NOW it executes
# MAGIC # OR
# MAGIC count = df.count()  # ✅ Action: NOW it executes
# MAGIC # OR
# MAGIC df.show()  # ✅ Action: NOW it executes
# MAGIC ```
# MAGIC
# MAGIC **Remember**: Transformations are lazy; actions trigger execution
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Mistake 9: Not Using Delta Lake Features
# MAGIC
# MAGIC **Problem**:
# MAGIC ```python
# MAGIC # Using Parquet without Delta Lake benefits
# MAGIC df.write.format("parquet").mode("overwrite").save("/data")  # ❌ No ACID
# MAGIC ```
# MAGIC
# MAGIC **Solution**:
# MAGIC ```python
# MAGIC # Use Delta Lake for production pipelines
# MAGIC df.write.format("delta").mode("overwrite").save("/data")  # ✅ ACID transactions
# MAGIC
# MAGIC # Leverage Delta features
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC # Time travel
# MAGIC old_version = spark.read.format("delta").option("versionAsOf", 0).load("/data")
# MAGIC
# MAGIC # Optimize
# MAGIC DeltaTable.forPath(spark, "/data").optimize().executeCompaction()
# MAGIC
# MAGIC # Vacuum old versions
# MAGIC DeltaTable.forPath(spark, "/data").vacuum(168)  # Keep 7 days
# MAGIC ```
# MAGIC
# MAGIC **Remember**: Always use Delta Lake for production data pipelines
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Mistake 10: Ignoring Data Quality
# MAGIC
# MAGIC **Problem**:
# MAGIC ```python
# MAGIC # No validation, bad data propagates
# MAGIC df = spark.read.csv("/data")
# MAGIC df.write.save("/silver/data")  # ❌ No quality checks!
# MAGIC ```
# MAGIC
# MAGIC **Solution**:
# MAGIC ```python
# MAGIC # Add data quality checks
# MAGIC df = spark.read.csv("/data")
# MAGIC
# MAGIC # Validate and filter
# MAGIC quality_df = df \
# MAGIC     .filter(col("id").isNotNull()) \
# MAGIC     .filter(col("amount") >= 0) \
# MAGIC     .filter(col("date").isNotNull()) \
# MAGIC     .dropDuplicates(["id"])
# MAGIC
# MAGIC # Log bad records
# MAGIC bad_records = df.exceptAll(quality_df)
# MAGIC bad_records.write.save("/logs/bad_records")  # ✅ Audit trail
# MAGIC
# MAGIC quality_df.write.save("/silver/data")  # ✅ Only clean data
# MAGIC ```
# MAGIC
# MAGIC **Remember**: Validate data quality between layers
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Summary: Best Practices
# MAGIC
# MAGIC ✅ **DO**:
# MAGIC * Use ELT with Medallion Architecture
# MAGIC * Choose batch by default, streaming when needed
# MAGIC * Define schemas for production pipelines
# MAGIC * Optimize partitions for your cluster
# MAGIC * Filter and select early
# MAGIC * Cache only frequently reused DataFrames
# MAGIC * Use Delta Lake for all production data
# MAGIC * Add data quality checks
# MAGIC * Monitor pipeline performance
# MAGIC
# MAGIC ❌ **DON'T**:
# MAGIC * Transform data before writing to bronze
# MAGIC * Use streaming for batch use cases
# MAGIC * Rely solely on schema inference in production
# MAGIC * Ignore partition sizing
# MAGIC * Use collect() on large datasets
# MAGIC * Over-cache everything
# MAGIC * Process entire datasets before filtering
# MAGIC * Forget Delta Lake features
# MAGIC * Skip data quality validation

# COMMAND ----------

# DBTITLE 1,Conclusion and Next Steps
# MAGIC %md
# MAGIC # 🎉 Congratulations!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏆 What You've Accomplished
# MAGIC
# MAGIC You've completed **Day 2: Data Processing Concepts** and now understand:
# MAGIC
# MAGIC ✅ **ETL vs ELT** — Modern data integration patterns  
# MAGIC ✅ **Batch vs Streaming** — Choosing the right processing paradigm  
# MAGIC ✅ **Data Types** — Handling structured, semi-structured, and unstructured data  
# MAGIC ✅ **Distributed Computing** — Spark architecture and parallel processing  
# MAGIC ✅ **Production Pipelines** — Building end-to-end data workflows  
# MAGIC ✅ **Best Practices** — Avoiding common mistakes  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Next Steps in Your Data Engineering Journey
# MAGIC
# MAGIC ### Immediate Practice
# MAGIC 1. **Build Your Own Pipeline**
# MAGIC    * Find a dataset (Kaggle, /databricks-datasets/)
# MAGIC    * Implement Bronze/Silver/Gold layers
# MAGIC    * Add data quality checks
# MAGIC    * Optimize for performance
# MAGIC
# MAGIC 2. **Experiment with Different Data Types**
# MAGIC    * Process CSV, JSON, and Parquet files
# MAGIC    * Practice flattening nested JSON
# MAGIC    * Work with real-world messy data
# MAGIC
# MAGIC 3. **Optimize Spark Jobs**
# MAGIC    * Use Spark UI to analyze execution
# MAGIC    * Experiment with partitioning
# MAGIC    * Profile slow queries
# MAGIC
# MAGIC ### Continue Learning
# MAGIC
# MAGIC **Day 3 Topics** (Coming Next):
# MAGIC * Delta Lake deep dive
# MAGIC * Advanced transformations
# MAGIC * Performance tuning
# MAGIC * Data quality frameworks
# MAGIC
# MAGIC **Advanced Topics**:
# MAGIC * Structured Streaming and Auto Loader
# MAGIC * Change Data Capture (CDC)
# MAGIC * Machine Learning pipelines
# MAGIC * Real-time analytics
# MAGIC * Unity Catalog and governance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Additional Resources
# MAGIC
# MAGIC * **Databricks Documentation**: [docs.databricks.com](https://docs.databricks.com)
# MAGIC * **Spark Documentation**: [spark.apache.org](https://spark.apache.org)
# MAGIC * **Delta Lake**: [delta.io](https://delta.io)
# MAGIC * **Databricks Academy**: Free training courses
# MAGIC * **Community Forums**: [community.databricks.com](https://community.databricks.com)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💬 Get Help
# MAGIC
# MAGIC * **Use Genie Code**: AI assistant for code generation and debugging
# MAGIC * **Databricks Community**: Ask questions in the forum
# MAGIC * **Documentation**: Search for specific features and examples
# MAGIC * **Tutorials**: Follow step-by-step guides
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⭐ Remember
# MAGIC
# MAGIC > "The best way to learn data engineering is to build data pipelines."
# MAGIC
# MAGIC Don't just read — **experiment, break things, fix them, and learn!**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📝 Feedback
# MAGIC
# MAGIC This notebook was designed to be:
# MAGIC * **Comprehensive** — Covering all core concepts
# MAGIC * **Hands-on** — Practical demonstrations
# MAGIC * **Production-oriented** — Real-world best practices
# MAGIC
# MAGIC Keep building, keep learning, and happy data engineering! 🚀
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Ready for Day 3?** Let's dive deeper into Delta Lake and advanced data transformations!

# COMMAND ----------

# DBTITLE 1,Section 5: Hands-on Mini Pipeline
# MAGIC %md
# MAGIC # Section 5: Hands-on Mini Pipeline
# MAGIC
# MAGIC ## 🎯 Objective
# MAGIC Build a **complete data pipeline** that demonstrates:
# MAGIC * **ELT approach** (load raw, transform in lakehouse)
# MAGIC * **Batch processing** pattern
# MAGIC * **Structured and semi-structured data**
# MAGIC * **Distributed processing** with Spark
# MAGIC * **Delta Lake** for reliable storage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💼 Business Scenario
# MAGIC **E-commerce Analytics Pipeline**
# MAGIC
# MAGIC We need to process:
# MAGIC 1. **Order data** (CSV - structured)
# MAGIC 2. **Customer events** (JSON - semi-structured)
# MAGIC 3. Combine and analyze for business insights
# MAGIC
# MAGIC **Pipeline Flow**:
# MAGIC ```
# MAGIC Raw Data (CSV + JSON)
# MAGIC     ↓
# MAGIC Bronze Layer (raw ingestion)
# MAGIC     ↓
# MAGIC Silver Layer (cleaned, joined)
# MAGIC     ↓
# MAGIC Gold Layer (business aggregates)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Pipeline Step 1: Ingest Raw Data (Bronze)
# BRONZE LAYER: Ingest raw data (ELT: Load first, transform later)
# This is the "E" and "L" in ELT

print("🥉 BRONZE LAYER: Raw Data Ingestion\n")

# Simulate order data (structured CSV)
orders_data = [
    (1001, "CUST001", "2026-04-20", 250.50, "completed"),
    (1002, "CUST002", "2026-04-20", 89.99, "completed"),
    (1003, "CUST001", "2026-04-21", 450.00, "pending"),
    (1004, "CUST003", "2026-04-21", 120.00, "completed"),
    (1005, "CUST002", "2026-04-21", 299.99, "completed"),
    (1006, "CUST004", "2026-04-21", 75.50, "cancelled"),
]

orders_df = spark.createDataFrame(
    orders_data,
    ["order_id", "customer_id", "order_date", "amount", "status"]
)

print("✅ Order data (structured CSV):")
display(orders_df)

# Write to Bronze layer (raw, no transformations)
orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/tmp/mini_pipeline/bronze/orders")

print("\n💾 Saved to Bronze: /tmp/mini_pipeline/bronze/orders")

# Simulate customer events (semi-structured JSON)
events_json = [
    '{"customer_id": "CUST001", "event_type": "page_view", "timestamp": "2026-04-20T10:30:00", "metadata": {"page": "/products", "duration_sec": 45}}',
    '{"customer_id": "CUST002", "event_type": "add_to_cart", "timestamp": "2026-04-20T11:00:00", "metadata": {"product_id": "P123"}}',
    '{"customer_id": "CUST001", "event_type": "purchase", "timestamp": "2026-04-20T11:15:00"}',
    '{"customer_id": "CUST003", "event_type": "page_view", "timestamp": "2026-04-21T09:00:00", "metadata": {"page": "/checkout", "duration_sec": 30}}',
    '{"customer_id": "CUST002", "event_type": "purchase", "timestamp": "2026-04-21T10:30:00"}',
]

events_rdd = spark.sparkContext.parallelize(events_json)
events_rdd.saveAsTextFile("/tmp/mini_pipeline/bronze/events_json")

events_df = spark.read.json("/tmp/mini_pipeline/bronze/events_json")
print("\n✅ Customer events (semi-structured JSON):")
display(events_df)

print("\n💾 Saved to Bronze: /tmp/mini_pipeline/bronze/events_json")

print("\n✅ BRONZE COMPLETE: Raw data ingested (ELT Load phase)")
print("   • No transformations applied")
print("   • Data preserved in original format")
print("   • Schema-on-read approach")

# COMMAND ----------

# DBTITLE 1,Pipeline Step 2: Clean and Transform (Silver)
# SILVER LAYER: Clean, transform, and join data
# This is the "T" in ELT (transform inside the lakehouse)

print("🥈 SILVER LAYER: Data Transformation\n")

# Read from Bronze (raw data)
print("📥 Reading from Bronze layer...")
bronze_orders = spark.read.format("delta").load("/tmp/mini_pipeline/bronze/orders")
bronze_events = spark.read.json("/tmp/mini_pipeline/bronze/events_json")

# Transform orders (structured data)
print("\n⚙️ Transforming orders...")
silver_orders = bronze_orders \
    .filter(col("status") == "completed") \
    .withColumn("order_date", to_date(col("order_date"))) \
    .withColumn("amount", round(col("amount"), 2)) \
    .withColumn("order_year", year(col("order_date"))) \
    .withColumn("order_month", month(col("order_date"))) \
    .withColumn("amount_bucket", 
                when(col("amount") >= 200, "High")
                .when(col("amount") >= 100, "Medium")
                .otherwise("Low"))

print("✅ Orders cleaned and enriched:")
display(silver_orders)

# Transform events (semi-structured data - flatten)
print("\n⚙️ Transforming events (flattening JSON)...")
silver_events = bronze_events \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("event_date", to_date(col("timestamp"))) \
    .withColumn("page_viewed", col("metadata.page")) \
    .withColumn("duration_sec", col("metadata.duration_sec")) \
    .select("customer_id", "event_type", "timestamp", "event_date", "page_viewed", "duration_sec")

print("✅ Events flattened and cleaned:")
display(silver_events)

# Save to Silver layer
silver_orders.write.format("delta").mode("overwrite").save("/tmp/mini_pipeline/silver/orders")
silver_events.write.format("delta").mode("overwrite").save("/tmp/mini_pipeline/silver/events")

print("\n💾 Saved to Silver layer")
print("✅ SILVER COMPLETE: Data cleaned and structured")
print("   • Filtered invalid records")
print("   • Standardized data types")
print("   • Enriched with derived columns")
print("   • Flattened nested JSON structures")

# COMMAND ----------

# DBTITLE 1,Pipeline Step 3: Business Aggregates (Gold)
# GOLD LAYER: Business-level aggregates and insights
# Optimized for analytics and reporting

print("🥇 GOLD LAYER: Business Analytics\n")

# Read from Silver
print("📥 Reading from Silver layer...")
silver_orders = spark.read.format("delta").load("/tmp/mini_pipeline/silver/orders")
silver_events = spark.read.format("delta").load("/tmp/mini_pipeline/silver/events")

# Gold Table 1: Daily sales summary
print("\n📈 Creating Gold Table 1: Daily Sales Summary")
daily_sales = silver_orders \
    .groupBy("order_date") \
    .agg(
        count("order_id").alias("total_orders"),
        sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_order_value"),
        countDistinct("customer_id").alias("unique_customers")
    ) \
    .withColumn("total_revenue", round(col("total_revenue"), 2)) \
    .withColumn("avg_order_value", round(col("avg_order_value"), 2)) \
    .orderBy("order_date")

print("✅ Daily sales summary:")
display(daily_sales)

daily_sales.write.format("delta").mode("overwrite").save("/tmp/mini_pipeline/gold/daily_sales")

# Gold Table 2: Customer analytics
print("\n📈 Creating Gold Table 2: Customer Analytics")
customer_analytics = silver_orders \
    .groupBy("customer_id") \
    .agg(
        count("order_id").alias("total_orders"),
        sum("amount").alias("lifetime_value"),
        avg("amount").alias("avg_order_value"),
        max("order_date").alias("last_order_date")
    ) \
    .withColumn("lifetime_value", round(col("lifetime_value"), 2)) \
    .withColumn("avg_order_value", round(col("avg_order_value"), 2)) \
    .withColumn("customer_segment",
                when(col("lifetime_value") >= 500, "VIP")
                .when(col("lifetime_value") >= 200, "Regular")
                .otherwise("New")) \
    .orderBy(desc("lifetime_value"))

print("✅ Customer analytics:")
display(customer_analytics)

customer_analytics.write.format("delta").mode("overwrite").save("/tmp/mini_pipeline/gold/customer_analytics")

# Gold Table 3: Customer engagement (join orders with events)
print("\n📈 Creating Gold Table 3: Customer Engagement")
event_summary = silver_events \
    .groupBy("customer_id") \
    .agg(
        count("*").alias("total_events"),
        countDistinct("event_type").alias("event_types"),
        sum(when(col("event_type") == "page_view", 1).otherwise(0)).alias("page_views"),
        sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchases")
    )

engagement = customer_analytics \
    .join(event_summary, "customer_id", "left") \
    .fillna(0, ["total_events", "event_types", "page_views", "purchases"]) \
    .withColumn("engagement_score", 
                (col("total_events") * 0.3 + col("total_orders") * 0.7)) \
    .select(
        "customer_id",
        "customer_segment",
        "lifetime_value",
        "total_orders",
        "total_events",
        "page_views",
        "engagement_score"
    ) \
    .orderBy(desc("engagement_score"))

print("✅ Customer engagement:")
display(engagement)

engagement.write.format("delta").mode("overwrite").save("/tmp/mini_pipeline/gold/customer_engagement")

print("\n💾 Saved to Gold layer")
print("✅ GOLD COMPLETE: Business-ready analytics tables")
print("   • Aggregated for fast queries")
print("   • Denormalized for reporting")
print("   • Optimized for BI tools")

# COMMAND ----------

# DBTITLE 1,Pipeline Step 4: Pipeline Validation
# PIPELINE VALIDATION
# Verify data quality and pipeline health

print("✅ PIPELINE VALIDATION\n")
print("="*60)

# Check Bronze layer
print("\n🥉 BRONZE LAYER:")
bronze_orders_count = spark.read.format("delta").load("/tmp/mini_pipeline/bronze/orders").count()
bronze_events_count = spark.read.json("/tmp/mini_pipeline/bronze/events_json").count()
print(f"   Orders (raw): {bronze_orders_count} records")
print(f"   Events (raw): {bronze_events_count} records")

# Check Silver layer
print("\n🥈 SILVER LAYER:")
silver_orders_count = spark.read.format("delta").load("/tmp/mini_pipeline/silver/orders").count()
silver_events_count = spark.read.format("delta").load("/tmp/mini_pipeline/silver/events").count()
print(f"   Orders (cleaned): {silver_orders_count} records")
print(f"   Events (cleaned): {silver_events_count} records")
print(f"   Data quality: {silver_orders_count}/{bronze_orders_count} orders passed validation")

# Check Gold layer
print("\n🥇 GOLD LAYER:")
gold_daily_sales = spark.read.format("delta").load("/tmp/mini_pipeline/gold/daily_sales").count()
gold_customers = spark.read.format("delta").load("/tmp/mini_pipeline/gold/customer_analytics").count()
gold_engagement = spark.read.format("delta").load("/tmp/mini_pipeline/gold/customer_engagement").count()
print(f"   Daily sales summary: {gold_daily_sales} days")
print(f"   Customer analytics: {gold_customers} customers")
print(f"   Customer engagement: {gold_engagement} customers")

# Pipeline metadata
print("\n\n📊 PIPELINE SUMMARY:")
print("="*60)
print(f"✅ Pipeline Status: SUCCESS")
print(f"\n📥 Data Processed:")
print(f"   Bronze → Silver: {bronze_orders_count + bronze_events_count} → {silver_orders_count + silver_events_count} records")
print(f"   Silver → Gold: {gold_daily_sales + gold_customers + gold_engagement} aggregate tables")
print(f"\n🎯 Pipeline Characteristics:")
print(f"   • Architecture: ELT (Extract, Load, Transform)")
print(f"   • Processing: Batch")
print(f"   • Data Types: Structured (CSV) + Semi-structured (JSON)")
print(f"   • Storage: Delta Lake (ACID transactions)")
print(f"   • Compute: Distributed (Apache Spark)")
print(f"   • Layers: Bronze → Silver → Gold (Medallion Architecture)")
print("\n🚀 Pipeline ready for production!")

# COMMAND ----------

# DBTITLE 1,Pipeline Architecture Diagram
# MAGIC %md
# MAGIC ## 🏛️ Pipeline Architecture
# MAGIC
# MAGIC ### Data Flow
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────┐
# MAGIC │          SOURCE DATA                       │
# MAGIC │  (CSV files + JSON events)                │
# MAGIC └────────────┬─────────────────────────────┘
# MAGIC              │
# MAGIC              │ EXTRACT + LOAD (no transformation)
# MAGIC              │
# MAGIC              ▼
# MAGIC ┌──────────────────────────────────────────┐
# MAGIC │      🥉 BRONZE LAYER                     │
# MAGIC │  (Raw data - no transformations)          │
# MAGIC │  - orders/ (Delta)                        │
# MAGIC │  - events/ (JSON)                         │
# MAGIC │  Schema: As-is from source                │
# MAGIC └────────────┬─────────────────────────────┘
# MAGIC              │
# MAGIC              │ TRANSFORM (inside lakehouse)
# MAGIC              │ - Filter, clean, standardize
# MAGIC              │ - Flatten JSON
# MAGIC              │ - Enrich with derived columns
# MAGIC              │
# MAGIC              ▼
# MAGIC ┌──────────────────────────────────────────┐
# MAGIC │      🥈 SILVER LAYER                     │
# MAGIC │  (Cleaned, validated, conformed)          │
# MAGIC │  - orders/ (Delta)                        │
# MAGIC │  - events/ (Delta)                        │
# MAGIC │  Schema: Standardized                     │
# MAGIC └────────────┬─────────────────────────────┘
# MAGIC              │
# MAGIC              │ AGGREGATE
# MAGIC              │ - Join dimensions
# MAGIC              │ - Calculate KPIs
# MAGIC              │ - Denormalize for reporting
# MAGIC              │
# MAGIC              ▼
# MAGIC ┌──────────────────────────────────────────┐
# MAGIC │      🥇 GOLD LAYER                       │
# MAGIC │  (Business-level aggregates)              │
# MAGIC │  - daily_sales/ (Delta)                   │
# MAGIC │  - customer_analytics/ (Delta)            │
# MAGIC │  - customer_engagement/ (Delta)           │
# MAGIC │  Schema: Denormalized for analytics       │
# MAGIC └────────────┬─────────────────────────────┘
# MAGIC              │
# MAGIC              ▼
# MAGIC ┌──────────────────────────────────────────┐
# MAGIC │   BI TOOLS & DASHBOARDS                   │
# MAGIC │  (Tableau, Power BI, Lakeview)            │
# MAGIC └──────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚙️ Pipeline Characteristics
# MAGIC
# MAGIC | Aspect | Implementation |
# MAGIC |--------|----------------|
# MAGIC | **Pattern** | ELT (Extract, Load, Transform) |
# MAGIC | **Processing** | Batch (scheduled runs) |
# MAGIC | **Architecture** | Medallion (Bronze → Silver → Gold) |
# MAGIC | **Storage** | Delta Lake (ACID transactions) |
# MAGIC | **Compute** | Apache Spark (distributed) |
# MAGIC | **Data Types** | Structured + Semi-structured |
# MAGIC | **Schema** | Schema-on-read (Bronze), Schema-on-write (Silver/Gold) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Key Design Decisions
# MAGIC
# MAGIC 1. **ELT over ETL**: Raw data preserved for reprocessing
# MAGIC 2. **Batch over Streaming**: Daily analytics, not real-time
# MAGIC 3. **Medallion Architecture**: Clear separation of concerns
# MAGIC 4. **Delta Lake**: ACID guarantees, time travel, versioning
# MAGIC 5. **Distributed Processing**: Scales to terabytes of data

# COMMAND ----------

# DBTITLE 1,Section 4: Distributed Computing - Concept
# MAGIC %md
# MAGIC # Section 4: Introduction to Distributed Computing
# MAGIC
# MAGIC ## 🧒 ELI5 (Explain Like I'm 5)
# MAGIC
# MAGIC **Single-Node (One Computer)**
# MAGIC > Imagine you have to color 100 pictures by yourself. You color them one by one. It takes a LONG time!
# MAGIC
# MAGIC **Distributed System (Many Computers)**
# MAGIC > Now imagine you have 10 friends helping you. You give 10 pictures to each friend. Everyone colors at the same time. You finish 10x faster!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation
# MAGIC
# MAGIC ### Why Distributed Computing?
# MAGIC
# MAGIC **The Problem**: Single machines have limits
# MAGIC * **Limited CPU** — Only so many cores
# MAGIC * **Limited RAM** — 100GB+ datasets won't fit in memory
# MAGIC * **Limited Storage** — Petabytes of data need distributed storage
# MAGIC * **Processing Time** — Hours/days for large computations
# MAGIC
# MAGIC **The Solution**: Distribute work across many machines
# MAGIC * **Horizontal Scaling** — Add more machines (not bigger machines)
# MAGIC * **Parallel Processing** — Process data simultaneously
# MAGIC * **Fault Tolerance** — If one machine fails, others continue
# MAGIC * **Cost Effective** — Use commodity hardware
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Apache Spark Architecture
# MAGIC
# MAGIC ### Components
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────┐
# MAGIC │                  DRIVER                         │
# MAGIC │  (Master node - coordinates everything)         │
# MAGIC │  - Creates SparkContext                         │
# MAGIC │  - Schedules tasks                              │
# MAGIC │  - Collects results                             │
# MAGIC └────────────┬────────────────────────────────────┘
# MAGIC              │
# MAGIC              │  Sends tasks
# MAGIC              │
# MAGIC      ┌───────┴──────────┬──────────────┐
# MAGIC      │                  │              │
# MAGIC ┌────▼─────┐      ┌────▼─────┐  ┌────▼─────┐
# MAGIC │ EXECUTOR │      │ EXECUTOR │  │ EXECUTOR │
# MAGIC │ (Worker) │      │ (Worker) │  │ (Worker) │
# MAGIC │  - Task 1│      │  - Task 3│  │  - Task 5│
# MAGIC │  - Task 2│      │  - Task 4│  │  - Task 6│
# MAGIC └──────────┘      └──────────┘  └──────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Roles
# MAGIC
# MAGIC **1. Driver (Master Node)**
# MAGIC * Your Databricks notebook runs here
# MAGIC * Converts your code into tasks
# MAGIC * Schedules tasks to executors
# MAGIC * Collects and aggregates results
# MAGIC * Single point of coordination
# MAGIC
# MAGIC **2. Executors (Worker Nodes)**
# MAGIC * Perform actual data processing
# MAGIC * Store data partitions in memory
# MAGIC * Execute tasks assigned by driver
# MAGIC * Multiple executors work in parallel
# MAGIC * Can scale to thousands of executors
# MAGIC
# MAGIC **3. Cluster Manager**
# MAGIC * Manages resources (CPU, memory)
# MAGIC * Allocates executors
# MAGIC * In Databricks: automatically managed
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚡ Parallel Processing
# MAGIC
# MAGIC ### How Spark Parallelizes Work
# MAGIC
# MAGIC 1. **Data Partitioning**
# MAGIC    * Data is split into chunks (partitions)
# MAGIC    * Each partition processed independently
# MAGIC    * Default: ~128MB per partition
# MAGIC
# MAGIC 2. **Task Distribution**
# MAGIC    * Driver creates tasks (one per partition)
# MAGIC    * Tasks sent to executors
# MAGIC    * Executors process in parallel
# MAGIC
# MAGIC 3. **Result Aggregation**
# MAGIC    * Executors send results back to driver
# MAGIC    * Driver combines results
# MAGIC    * Final result returned to user
# MAGIC
# MAGIC ### Key Benefits
# MAGIC * **Speed**: Process multiple partitions simultaneously
# MAGIC * **Scalability**: Add more executors for more data
# MAGIC * **Fault Tolerance**: Re-compute failed partitions
# MAGIC * **Memory Efficiency**: Process data in chunks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Single-Node vs Distributed
# MAGIC
# MAGIC | Aspect | Single-Node | Distributed (Spark) |
# MAGIC |--------|-------------|---------------------|
# MAGIC | **Data Size** | MB to low GB | TB to PB |
# MAGIC | **Processing** | Sequential | Parallel |
# MAGIC | **Scaling** | Vertical (bigger machine) | Horizontal (more machines) |
# MAGIC | **Fault Tolerance** | None (crash = restart) | High (re-compute failed tasks) |
# MAGIC | **Cost** | Expensive (high-end hardware) | Cost-effective (commodity) |
# MAGIC | **Memory** | Limited by single machine | Distributed across cluster |
# MAGIC | **Example** | Pandas, Excel | Spark, Hadoop |

# COMMAND ----------

# DBTITLE 1,Demo 4A: Understanding Partitions
# DISTRIBUTED COMPUTING DEMO
# Understanding how Spark distributes data across partitions

print("📦 PARTITIONS: How Spark distributes data\n")

# Create a sample dataset
data = [(i, f"user_{i}", i * 10) for i in range(1, 10001)]
df = spark.createDataFrame(data, ["id", "user_name", "value"])

print(f"✅ Created DataFrame with {df.count():,} records")

# Check number of partitions
num_partitions = df.rdd.getNumPartitions()
print(f"\n📏 Current number of partitions: {num_partitions}")
print("   (Spark automatically partitions data for parallel processing)")

# Show how data is distributed across partitions
print("\n📈 Records per partition:")
partition_sizes = df.rdd.glom().map(len).collect()
for i, size in enumerate(partition_sizes):
    print(f"   Partition {i}: {size:,} records")

print(f"\n🔑 Key Insight: Data is split into {num_partitions} partitions")
print(f"   Each partition can be processed by a different executor")
print(f"   Parallel processing = {num_partitions}x potential speedup!")

# COMMAND ----------

# DBTITLE 1,Demo 4B: Repartitioning for Parallelism
# REPARTITIONING DEMO
# Control how data is distributed for optimal parallel processing

print("🔄 REPARTITIONING: Controlling data distribution\n")

# Create a dataset
original_df = spark.range(0, 1000000)
original_partitions = original_df.rdd.getNumPartitions()

print(f"📊 Original DataFrame:")
print(f"   Records: {original_df.count():,}")
print(f"   Partitions: {original_partitions}")

# Increase partitions for more parallelism
more_parallel_df = original_df.repartition(16)
more_partitions = more_parallel_df.rdd.getNumPartitions()

print(f"\n⬆️ Increased parallelism:")
print(f"   Partitions: {more_partitions}")
print(f"   Effect: More executors can work simultaneously")
print(f"   Use case: Large cluster with many executors")

# Decrease partitions for less overhead
less_parallel_df = original_df.coalesce(2)
less_partitions = less_parallel_df.rdd.getNumPartitions()

print(f"\n⬇️ Reduced partitions (coalesce):")
print(f"   Partitions: {less_partitions}")
print(f"   Effect: Less overhead, but less parallelism")
print(f"   Use case: Small datasets, final aggregation")

print("\n💎 Best Practices:")
print("   • Default: ~128MB per partition")
print("   • Too few partitions: Underutilized cluster")
print("   • Too many partitions: High overhead")
print("   • Balance: Match partitions to available executors")

# COMMAND ----------

# DBTITLE 1,Demo 4C: Parallel Transformations
# PARALLEL PROCESSING DEMO
# How Spark executes transformations across partitions

from time import time

print("⚡ PARALLEL PROCESSING: Transformations across partitions\n")

# Create a larger dataset
large_df = spark.range(0, 5000000).toDF("id") \
    .withColumn("value", (col("id") * 2) + 10) \
    .withColumn("category", 
                when(col("id") % 3 == 0, "A")
                .when(col("id") % 3 == 1, "B")
                .otherwise("C"))

print(f"📊 Dataset: {large_df.count():,} records")
print(f"📏 Partitions: {large_df.rdd.getNumPartitions()}")

# Perform complex transformation (happens in parallel across partitions)
print("\n⚙️ Applying transformations (distributed across partitions)...")

start_time = time()
transformed_df = large_df \
    .filter(col("value") > 100) \
    .withColumn("value_squared", col("value") ** 2) \
    .withColumn("is_high_value", col("value") > 1000) \
    .groupBy("category") \
    .agg(
        count("*").alias("count"),
        avg("value").alias("avg_value"),
        max("value_squared").alias("max_squared")
    )

result = transformed_df.collect()  # Action: triggers computation
end_time = time()

print(f"✅ Processing complete in {end_time - start_time:.2f} seconds")
print("\n📈 Results:")
display(transformed_df)

print("\n🔑 What happened behind the scenes:")
print("   1. Driver divided data into partitions")
print("   2. Each executor processed its partition independently")
print("   3. Filter, withColumn operations done in parallel")
print("   4. GroupBy triggered shuffle (data redistribution)")
print("   5. Aggregations computed in parallel")
print("   6. Results collected back to driver")
print("\n   ⚡ This is the power of distributed computing!")

# COMMAND ----------

# DBTITLE 1,Demo 4D: Spark Execution Plan
# EXECUTION PLAN DEMO
# Understanding how Spark optimizes and executes queries

print("📝 SPARK EXECUTION PLAN: Behind the scenes\n")

# Create a sample query
df = spark.range(0, 1000000).toDF("id") \
    .withColumn("value", col("id") * 2) \
    .filter(col("value") > 1000) \
    .groupBy((col("id") % 10).alias("bucket")) \
    .agg(sum("value").alias("total_value"))

print("🔍 Logical Plan (what you asked for):")
print("="*60)
df.explain(mode="simple")

print("\n\n🔧 Physical Plan (how Spark will execute it):")
print("="*60)
df.explain(mode="formatted")

print("\n🔑 Key Concepts:")
print("   • Transformations are lazy (not executed immediately)")
print("   • Spark builds an execution plan")
print("   • Catalyst optimizer improves the plan")
print("   • Tungsten engine generates efficient code")
print("   • Actions trigger actual execution")
print("   • Work distributed across executors")

print("\n🚀 This optimization is why Spark is fast!")

# COMMAND ----------

# DBTITLE 1,Distributed Computing Summary
# MAGIC %md
# MAGIC ## 📊 Distributed Computing: Key Takeaways
# MAGIC
# MAGIC ### 🎯 Core Concepts
# MAGIC
# MAGIC 1. **Partitioning**
# MAGIC    * Data split into chunks (partitions)
# MAGIC    * Each partition processed independently
# MAGIC    * More partitions = more parallelism
# MAGIC
# MAGIC 2. **Driver vs Executors**
# MAGIC    * **Driver**: Coordinates work (your notebook)
# MAGIC    * **Executors**: Do the actual processing (workers)
# MAGIC    * Scale by adding more executors
# MAGIC
# MAGIC 3. **Lazy Evaluation**
# MAGIC    * Transformations build execution plan
# MAGIC    * Actions trigger actual computation
# MAGIC    * Spark optimizes entire pipeline
# MAGIC
# MAGIC 4. **Shuffle**
# MAGIC    * Redistribution of data across partitions
# MAGIC    * Needed for groupBy, join operations
# MAGIC    * Expensive operation (network transfer)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚡ Performance Best Practices
# MAGIC
# MAGIC * **Partition Size**: ~128MB per partition ideal
# MAGIC * **Avoid Shuffles**: Minimize groupBy, joins when possible
# MAGIC * **Cache Wisely**: Cache frequently used DataFrames
# MAGIC * **Broadcast Small Tables**: For joins with small tables
# MAGIC * **Predicate Pushdown**: Filter early in the pipeline
# MAGIC * **Columnar Formats**: Use Parquet, Delta for efficiency
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Why This Matters
# MAGIC
# MAGIC | Without Spark | With Spark |
# MAGIC |---------------|------------|
# MAGIC | Hours to days | Minutes to hours |
# MAGIC | Single machine limits | Scale to petabytes |
# MAGIC | Manual optimization | Automatic optimization |
# MAGIC | No fault tolerance | Automatic recovery |
# MAGIC | Expensive hardware | Commodity hardware |
# MAGIC
# MAGIC **Distributed computing enables processing at scale!**

# COMMAND ----------

# DBTITLE 1,Section 3: Data Types - Concept
# MAGIC %md
# MAGIC # Section 3: Data Types — Structured, Semi-Structured, Unstructured
# MAGIC
# MAGIC ## 🧒 ELI5 (Explain Like I'm 5)
# MAGIC
# MAGIC **Structured Data**
# MAGIC > Like a LEGO set with instructions. Every piece has a specific place, and everything is organized in neat rows and columns. It's very organized!
# MAGIC
# MAGIC **Semi-Structured Data**
# MAGIC > Like a backpack with different pockets. Some things are organized (books in the book pocket), but items can be in different places. It's flexible!
# MAGIC
# MAGIC **Unstructured Data**
# MAGIC > Like a toy box where you dump everything. Photos, drawings, random toys — no specific organization. It's messy but contains lots of stuff!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation
# MAGIC
# MAGIC ### 1. Structured Data
# MAGIC * **Definition**: Data organized in a **fixed schema** with rows and columns
# MAGIC * **Schema**: Pre-defined, rigid structure
# MAGIC * **Format**: Relational databases, CSV, Parquet, Excel
# MAGIC * **Query**: SQL-friendly
# MAGIC * **Examples**: 
# MAGIC   * Customer records (ID, Name, Email, Phone)
# MAGIC   * Financial transactions
# MAGIC   * Inventory systems
# MAGIC
# MAGIC ### 2. Semi-Structured Data
# MAGIC * **Definition**: Data with **some structure** but not rigidly organized
# MAGIC * **Schema**: Flexible, self-describing, can vary between records
# MAGIC * **Format**: JSON, XML, Avro, Parquet with nested structures
# MAGIC * **Query**: Requires schema inference or flattening
# MAGIC * **Examples**:
# MAGIC   * JSON API responses
# MAGIC   * Log files (with key-value pairs)
# MAGIC   * XML documents
# MAGIC   * NoSQL databases
# MAGIC
# MAGIC ### 3. Unstructured Data
# MAGIC * **Definition**: Data with **no predefined structure**
# MAGIC * **Schema**: None — free-form content
# MAGIC * **Format**: Images, videos, audio, PDFs, text documents
# MAGIC * **Query**: Requires specialized tools (NLP, computer vision)
# MAGIC * **Examples**:
# MAGIC   * Images and videos
# MAGIC   * Emails and documents
# MAGIC   * Social media posts
# MAGIC   * Audio recordings
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Data Type Distribution in Enterprises
# MAGIC
# MAGIC | Data Type | % of Enterprise Data | Processing Difficulty | Business Value |
# MAGIC |-----------|----------------------|-----------------------|----------------|
# MAGIC | **Structured** | ~20% | Low | High (easy to analyze) |
# MAGIC | **Semi-Structured** | ~10% | Medium | High (flexible) |
# MAGIC | **Unstructured** | ~70% | High | Very High (rich insights) |
# MAGIC
# MAGIC 🔑 **Key Insight**: Most data is unstructured, but structured data is easiest to process!

# COMMAND ----------

# DBTITLE 1,Demo 3A: Structured Data - CSV/Parquet
# STRUCTURED DATA DEMO
# Data with fixed schema: rows and columns

print("📋 STRUCTURED DATA: CSV Format\n")

# Read structured CSV data
structured_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/databricks-datasets/bikeSharing/data-001/day.csv")

print("✅ Loaded structured data from CSV")
print(f"📊 Total records: {structured_df.count():,}")
print("\n📋 Schema (fixed structure):")
structured_df.printSchema()

print("\n🔍 Sample data:")
display(structured_df.limit(5))

print("\n🔑 Key Characteristics:")
print("  • Every row has the same columns")
print("  • Schema is predefined and consistent")
print("  • Easy to query with SQL")
print("  • Efficient storage and processing")

# SQL queries work perfectly on structured data
structured_df.createOrReplaceTempView("bike_data")
result = spark.sql("""
    SELECT season, 
           ROUND(AVG(temp), 2) as avg_temp,
           ROUND(AVG(cnt), 0) as avg_riders
    FROM bike_data
    GROUP BY season
    ORDER BY season
""")

print("\n📈 Easy SQL aggregation on structured data:")
display(result)

# COMMAND ----------

# DBTITLE 1,Demo 3B: Semi-Structured Data - JSON
# SEMI-STRUCTURED DATA DEMO
# Data with flexible schema: nested fields, varying structures

print("📜 SEMI-STRUCTURED DATA: JSON Format\n")

# Create sample JSON data (semi-structured)
json_data = [
    '{"user_id": 1, "name": "Alice", "orders": [{"order_id": 101, "amount": 250.50}, {"order_id": 102, "amount": 89.99}], "preferences": {"newsletter": true, "theme": "dark"}}',
    '{"user_id": 2, "name": "Bob", "orders": [{"order_id": 103, "amount": 450.00}], "preferences": {"newsletter": false}}',
    '{"user_id": 3, "name": "Charlie", "orders": [], "preferences": {"newsletter": true, "theme": "light", "language": "en"}}',
    '{"user_id": 4, "name": "Diana", "orders": [{"order_id": 104, "amount": 120.00}, {"order_id": 105, "amount": 75.50}, {"order_id": 106, "amount": 200.00}]}'
]

# Write to temp file
json_rdd = spark.sparkContext.parallelize(json_data)
json_rdd.saveAsTextFile("/tmp/demo_json_data")

# Read JSON data
semi_structured_df = spark.read.json("/tmp/demo_json_data")

print("✅ Loaded semi-structured JSON data")
print("\n📋 Schema (notice nested structures):")
semi_structured_df.printSchema()

print("\n🔍 Sample data (with nested fields):")
display(semi_structured_df)

print("\n🔑 Key Characteristics:")
print("  • Schema varies between records (user 2 missing 'theme', user 4 missing 'preferences')")
print("  • Nested structures (orders array, preferences object)")
print("  • Self-describing (field names included in data)")
print("  • Flexible but requires flattening for analysis")

# Flatten nested JSON structure
flattened_df = semi_structured_df \
    .withColumn("order", explode_outer(col("orders"))) \
    .select(
        "user_id",
        "name",
        col("order.order_id").alias("order_id"),
        col("order.amount").alias("order_amount"),
        col("preferences.newsletter").alias("newsletter_opt_in"),
        col("preferences.theme").alias("theme_preference")
    )

print("\n🧹 Flattened structure for analysis:")
display(flattened_df)

print("\n💡 Semi-structured data requires transformation to make it analysis-ready!")

# COMMAND ----------

# DBTITLE 1,Demo 3C: Unstructured Data - Text/Images
# UNSTRUCTURED DATA DEMO
# Data with NO schema: text, images, documents

print("📄 UNSTRUCTURED DATA: Text and Images\n")

# Example 1: Text data (unstructured)
print("📝 Text Data (Unstructured):")
text_data = [
    (1, "The quick brown fox jumps over the lazy dog."),
    (2, "Databricks is a unified analytics platform built on Apache Spark."),
    (3, "Machine learning models require large amounts of training data."),
    (4, "Delta Lake provides ACID transactions on data lakes.")
]

text_df = spark.createDataFrame(text_data, ["id", "raw_text"])
display(text_df)

print("\n🔑 Key Characteristics:")
print("  • Free-form text with no structure")
print("  • Requires NLP/text processing to extract insights")
print("  • Cannot directly aggregate or query")

# Extract structure from unstructured text
print("\n⚙️ Extracting structure from unstructured text:")
processed_text = text_df \
    .withColumn("text_length", length(col("raw_text"))) \
    .withColumn("word_count", size(split(col("raw_text"), " "))) \
    .withColumn("contains_spark", col("raw_text").contains("Spark")) \
    .withColumn("contains_data", col("raw_text").contains("data"))

display(processed_text)

print("\n📊 Example: Word frequency analysis")
words_df = text_df \
    .select(explode(split(lower(col("raw_text")), "\\s+")).alias("word")) \
    .filter(length(col("word")) > 3) \
    .groupBy("word") \
    .count() \
    .orderBy(desc("count")) \
    .limit(10)

display(words_df)

print("\n🖼️ Image/Video/Audio Data:")
print("  • Stored as binary files (no schema)")
print("  • Requires computer vision or audio processing")
print("  • Example: /databricks-datasets/flowers/ (image data)")
print("  • Spark can process but needs specialized libraries (OpenCV, TensorFlow)")

# List image files
image_files = spark.read.format("binaryFile").load("/databricks-datasets/flowers/delta").limit(3)
print("\n🌸 Sample image metadata:")
display(image_files.select("path", "length", "modificationTime"))

print("\n🔑 Unstructured data requires preprocessing to extract insights!")

# COMMAND ----------

# DBTITLE 1,Data Types Summary
# MAGIC %md
# MAGIC ## 📊 Data Types: Quick Reference
# MAGIC
# MAGIC | Data Type | Structure | Schema | Examples | Processing |
# MAGIC |-----------|-----------|--------|----------|------------|
# MAGIC | **Structured** | Rigid rows/columns | Fixed, predefined | CSV, SQL tables, Parquet | Easy (SQL) |
# MAGIC | **Semi-Structured** | Flexible, nested | Self-describing, variable | JSON, XML, Avro | Medium (flatten first) |
# MAGIC | **Unstructured** | None | No schema | Text, images, videos | Hard (ML/NLP) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔧 Processing Approaches
# MAGIC
# MAGIC ### Structured Data
# MAGIC ```python
# MAGIC # Direct SQL queries work perfectly
# MAGIC spark.sql("SELECT * FROM table WHERE date > '2026-01-01'")
# MAGIC ```
# MAGIC
# MAGIC ### Semi-Structured Data
# MAGIC ```python
# MAGIC # Schema inference + flattening
# MAGIC df = spark.read.json("data.json")
# MAGIC df = df.select("field1", "nested.field2")
# MAGIC ```
# MAGIC
# MAGIC ### Unstructured Data
# MAGIC ```python
# MAGIC # Extract features using ML/NLP
# MAGIC from pyspark.ml.feature import Tokenizer
# MAGIC tokenizer = Tokenizer(inputCol="text", outputCol="words")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Databricks Approach
# MAGIC * **Lakehouse handles all three types** in one platform
# MAGIC * **Delta Lake** optimizes structured and semi-structured data
# MAGIC * **ML Runtime** provides tools for unstructured data
# MAGIC * **Unified processing** — no need for multiple systems!

# COMMAND ----------

# DBTITLE 1,Section 2: Batch vs Streaming - Concept
# MAGIC %md
# MAGIC # Section 2: Batch vs Streaming Systems
# MAGIC
# MAGIC ## 🧒 ELI5 (Explain Like I'm 5)
# MAGIC
# MAGIC **Batch Processing**
# MAGIC > Imagine doing your laundry. You collect dirty clothes all week, and on Sunday, you wash everything at once. You process a BIG pile of clothes in ONE go.
# MAGIC
# MAGIC **Streaming Processing**
# MAGIC > Now imagine you wash each piece of clothing immediately when it gets dirty. You process clothes continuously, one by one (or in small groups), as soon as they arrive.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation
# MAGIC
# MAGIC ### Batch Processing
# MAGIC * Processes **large volumes** of data at **scheduled intervals**
# MAGIC * Data is collected over time, then processed in one batch
# MAGIC * **High latency** (hours to days)
# MAGIC * **High throughput** (can process massive datasets)
# MAGIC * Ideal for historical analysis, reporting, ETL jobs
# MAGIC
# MAGIC **Example:** Daily sales reports, monthly financial statements, weekly data warehouse refreshes
# MAGIC
# MAGIC ### Streaming Processing
# MAGIC * Processes data **continuously** as it arrives
# MAGIC * **Low latency** (seconds to minutes)
# MAGIC * **Micro-batching** in Spark Streaming (small batches processed rapidly)
# MAGIC * Ideal for real-time analytics, alerting, monitoring
# MAGIC
# MAGIC **Example:** Fraud detection, IoT sensor monitoring, real-time dashboards, clickstream analysis
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Comparison Table
# MAGIC
# MAGIC | Feature | Batch Processing | Streaming Processing |
# MAGIC |---------|------------------|----------------------|
# MAGIC | **Data Volume** | Large volumes | Continuous small chunks |
# MAGIC | **Latency** | Hours to days | Seconds to minutes |
# MAGIC | **Processing** | Scheduled (hourly, daily) | Continuous (real-time) |
# MAGIC | **Use Case** | Historical analysis, reporting | Real-time alerts, monitoring |
# MAGIC | **Complexity** | Simpler | More complex |
# MAGIC | **Cost** | Lower (process once) | Higher (always running) |
# MAGIC | **Examples** | Monthly reports, ETL jobs | Fraud detection, IoT |
# MAGIC | **Spark API** | `spark.read` | `spark.readStream` |
# MAGIC | **Fault Tolerance** | Restart batch | Checkpointing required |
# MAGIC | **Data Arrival** | All at once | Incremental |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 When to Use Batch
# MAGIC * **Daily/weekly/monthly reports**
# MAGIC * **Historical data analysis**
# MAGIC * **Large-scale ETL jobs**
# MAGIC * **Machine learning model training** (on historical data)
# MAGIC * **Cost is a concern** (run only when needed)
# MAGIC
# MAGIC ## 🎯 When to Use Streaming
# MAGIC * **Real-time fraud detection**
# MAGIC * **IoT sensor monitoring**
# MAGIC * **Clickstream analysis**
# MAGIC * **Real-time dashboards**
# MAGIC * **Alert systems** (stock prices, anomaly detection)
# MAGIC * **User activity tracking**

# COMMAND ----------

# DBTITLE 1,Demo 2A: Batch Processing - CSV
# BATCH PROCESSING DEMO
# Processing a static dataset all at once

print("📂 BATCH PROCESSING: Reading entire dataset at once\n")

# Read sample CSV data from Databricks datasets
# This reads the ENTIRE file in one batch
batch_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/databricks-datasets/online_retail/data-001/data.csv")

print(f"✅ Loaded {batch_df.count():,} records in one batch")
print(f"📊 Schema:")
batch_df.printSchema()

print("\n🔍 Sample data:")
display(batch_df.limit(10))

# Perform batch aggregation
print("\n📈 Batch aggregation: Total sales by country")
sales_by_country = batch_df \
    .groupBy("Country") \
    .agg(
        count("*").alias("total_orders"),
        sum("Quantity").alias("total_quantity"),
        round(sum(col("Quantity") * col("UnitPrice")), 2).alias("total_revenue")
    ) \
    .orderBy(desc("total_revenue")) \
    .limit(10)

display(sales_by_country)

print("\n🔑 Key Point: All data processed at once - typical batch job pattern")

# COMMAND ----------

# DBTITLE 1,Demo 2B: Simulated Streaming - Incremental Processing
# STREAMING PROCESSING DEMO (Simulated)
# In real streaming, data arrives continuously
# We'll simulate this by processing data in micro-batches

from datetime import datetime
import time

print("🌊 STREAMING PROCESSING: Simulating continuous data arrival\n")

# Create sample streaming data (simulating continuous events)
events_data = [
    (1, "user_123", "login", "2026-04-21 10:00:00"),
    (2, "user_456", "purchase", "2026-04-21 10:00:05"),
    (3, "user_789", "login", "2026-04-21 10:00:10"),
    (4, "user_123", "view_product", "2026-04-21 10:00:15"),
    (5, "user_456", "logout", "2026-04-21 10:00:20"),
    (6, "user_999", "purchase", "2026-04-21 10:00:25"),
    (7, "user_789", "view_product", "2026-04-21 10:00:30"),
    (8, "user_123", "purchase", "2026-04-21 10:00:35"),
]

# Simulate micro-batch processing (process every few seconds)
print("🔄 Processing events in micro-batches (streaming simulation)...\n")

for i, event in enumerate(events_data, 1):
    # Create a micro-batch (single event or small batch)
    micro_batch = spark.createDataFrame(
        [event], 
        ["event_id", "user_id", "event_type", "timestamp"]
    )
    
    # Process the micro-batch
    processed = micro_batch \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("processing_time", current_timestamp()) \
        .withColumn("batch_id", lit(i))
    
    print(f"⏱️ Batch {i}: Processing event {event[0]} - {event[2]} by {event[1]}")
    
    # In real streaming, this would continuously append to a Delta table
    processed.write \
        .format("delta") \
        .mode("append") \
        .save("/tmp/streaming_demo/events")
    
    time.sleep(0.5)  # Simulate small delay between micro-batches

print("\n✅ Streaming simulation complete!\n")

# Read back the streaming results
streaming_results = spark.read.format("delta").load("/tmp/streaming_demo/events")
print(f"📊 Total events processed: {streaming_results.count()}")
display(streaming_results.orderBy("batch_id"))

print("\n🔑 Key Point: Data processed continuously as it arrives - streaming pattern")

# COMMAND ----------

# DBTITLE 1,Batch vs Streaming Real-World Examples
# MAGIC %md
# MAGIC ## 🌎 Real-World Use Cases
# MAGIC
# MAGIC ### Batch Processing Examples
# MAGIC
# MAGIC 1. **Daily Sales Reports**
# MAGIC    * Collect all transactions throughout the day
# MAGIC    * Process at midnight to generate daily summary
# MAGIC    * No need for real-time updates
# MAGIC
# MAGIC 2. **Monthly Financial Statements**
# MAGIC    * Aggregate all transactions for the month
# MAGIC    * Process at month-end
# MAGIC    * Historical accuracy is key
# MAGIC
# MAGIC 3. **Machine Learning Model Training**
# MAGIC    * Collect historical data over weeks/months
# MAGIC    * Train model in one batch job
# MAGIC    * Re-train periodically (weekly/monthly)
# MAGIC
# MAGIC 4. **Data Warehouse ETL**
# MAGIC    * Extract data from multiple sources daily
# MAGIC    * Transform and load overnight
# MAGIC    * Reports ready by morning
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Streaming Processing Examples
# MAGIC
# MAGIC 1. **Fraud Detection**
# MAGIC    * Monitor credit card transactions in real-time
# MAGIC    * Flag suspicious activity within seconds
# MAGIC    * Prevent fraudulent charges immediately
# MAGIC
# MAGIC 2. **IoT Sensor Monitoring**
# MAGIC    * Temperature, pressure, vibration sensors
# MAGIC    * Detect anomalies instantly
# MAGIC    * Alert maintenance teams in real-time
# MAGIC
# MAGIC 3. **Clickstream Analysis**
# MAGIC    * Track user behavior on website
# MAGIC    * Personalize content in real-time
# MAGIC    * Update recommendations instantly
# MAGIC
# MAGIC 4. **Stock Market Analysis**
# MAGIC    * Monitor stock prices continuously
# MAGIC    * Execute trades based on algorithms
# MAGIC    * React to market changes in milliseconds
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🤔 Hybrid Approach: Lambda Architecture
# MAGIC Many modern systems use **both**:
# MAGIC * **Streaming layer** for real-time insights (hot path)
# MAGIC * **Batch layer** for historical accuracy (cold path)
# MAGIC * Combine results for complete picture

# COMMAND ----------

# DBTITLE 1,Section 1: ETL vs ELT - Concept
# MAGIC %md
# MAGIC # Section 1: ETL vs ELT
# MAGIC
# MAGIC ## 🧒 ELI5 (Explain Like I'm 5)
# MAGIC
# MAGIC **ETL (Extract, Transform, Load)**
# MAGIC > Imagine you're making a smoothie. You wash the fruits, peel them, cut them, blend them, and THEN pour it into your cup. You do all the work BEFORE putting it in the cup.
# MAGIC
# MAGIC **ELT (Extract, Load, Transform)**
# MAGIC > Now imagine you put all the fruits directly into a magic cup, and the cup does the washing, peeling, and blending for you. You put the raw stuff in FIRST, and it gets processed inside.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation
# MAGIC
# MAGIC ### ETL (Extract, Transform, Load)
# MAGIC * Data is **extracted** from source systems
# MAGIC * **Transformed** in a staging area or ETL server
# MAGIC * Then **loaded** into the target system (data warehouse)
# MAGIC * **Processing happens outside the target system**
# MAGIC * Requires dedicated ETL tools/servers
# MAGIC
# MAGIC ### ELT (Extract, Load, Transform)
# MAGIC * Data is **extracted** from source systems
# MAGIC * **Loaded** directly into the target system (data lake/lakehouse) in raw format
# MAGIC * **Transformed** inside the target system using SQL or Spark
# MAGIC * **Leverages the compute power of modern data platforms**
# MAGIC * Aligns with Lakehouse architecture (Databricks, Delta Lake)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Comparison Table
# MAGIC
# MAGIC | Feature | ETL | ELT |
# MAGIC |---------|-----|-----|
# MAGIC | **Transform Location** | External staging area | Inside target system |
# MAGIC | **Data Storage** | Transformed before storage | Raw data stored first |
# MAGIC | **Compute** | Dedicated ETL servers | Leverages data platform compute |
# MAGIC | **Schema** | Schema-on-write | Schema-on-read |
# MAGIC | **Flexibility** | Less flexible (pre-defined transforms) | Highly flexible (re-transform anytime) |
# MAGIC | **Cost** | Higher infrastructure costs | Lower (uses existing platform) |
# MAGIC | **Use Case** | Legacy data warehouses | Modern Lakehouse, Big Data |
# MAGIC | **Speed to Insight** | Slower (transform first) | Faster (load raw, query immediately) |
# MAGIC | **Data Lake Friendly** | No | Yes |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ When to Use ETL
# MAGIC * Legacy data warehouse environments (on-premises)
# MAGIC * When transformations must happen before storage (compliance)
# MAGIC * Small to medium data volumes
# MAGIC * Strict schema enforcement required upfront
# MAGIC
# MAGIC ## ✅ When to Use ELT (Lakehouse Context)
# MAGIC * Modern cloud-based data platforms (Databricks, Snowflake)
# MAGIC * Large-scale data (Big Data)
# MAGIC * Need to store raw data for multiple use cases
# MAGIC * Agile analytics — schema can evolve
# MAGIC * Cost-effective compute (pay for what you use)
# MAGIC * **This is the preferred approach in Databricks!**

# COMMAND ----------

# DBTITLE 1,Setup: Import Libraries
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

print("✅ Libraries imported successfully")

# COMMAND ----------

# DBTITLE 1,Demo 1A: ETL Style - Transform Before Write
# ETL STYLE: Transform data BEFORE writing to target
# Scenario: Loading employee data and transforming it before storing

# Step 1: EXTRACT - Read raw data from source
raw_employees = spark.createDataFrame([
    (1, "John Doe", "engineering", 75000, "2020-01-15"),
    (2, "Jane Smith", "marketing", 68000, "2019-06-20"),
    (3, "Bob Johnson", "engineering", 82000, "2021-03-10"),
    (4, "Alice Brown", "sales", 71000, "2020-11-05"),
    (5, "Charlie Wilson", "engineering", 79000, "2022-02-14")
], ["id", "name", "department", "salary", "hire_date"])

print("\n📥 EXTRACT: Raw data from source")
display(raw_employees)

# Step 2: TRANSFORM - Apply business logic BEFORE loading
transformed_employees = raw_employees \
    .withColumn("hire_date", to_date(col("hire_date"))) \
    .withColumn("department", upper(col("department"))) \
    .withColumn("annual_bonus", col("salary") * 0.10) \
    .withColumn("salary_band", 
                when(col("salary") >= 80000, "High")
                .when(col("salary") >= 70000, "Medium")
                .otherwise("Low")) \
    .withColumn("ingestion_timestamp", current_timestamp())

print("\n⚙️ TRANSFORM: Data transformed BEFORE writing")
display(transformed_employees)

# Step 3: LOAD - Write transformed data to target
# In ETL, only clean, transformed data is stored
print("\n💾 LOAD: Writing transformed data to Delta table...")
transformed_employees.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/tmp/etl_demo/employees_transformed")

print("✅ ETL Complete: Data was transformed BEFORE loading into target")

# COMMAND ----------

# DBTITLE 1,Demo 1B: ELT Style - Load Raw Then Transform
# ELT STYLE: Load raw data FIRST, transform LATER
# This is the Lakehouse approach!

# Step 1: EXTRACT - Read raw data from source (same as ETL)
raw_employees_elt = spark.createDataFrame([
    (1, "John Doe", "engineering", 75000, "2020-01-15"),
    (2, "Jane Smith", "marketing", 68000, "2019-06-20"),
    (3, "Bob Johnson", "engineering", 82000, "2021-03-10"),
    (4, "Alice Brown", "sales", 71000, "2020-11-05"),
    (5, "Charlie Wilson", "engineering", 79000, "2022-02-14")
], ["id", "name", "department", "salary", "hire_date"])

print("\n📥 EXTRACT: Raw data from source")
display(raw_employees_elt)

# Step 2: LOAD - Write RAW data directly to bronze layer (no transformation!)
print("\n💾 LOAD: Writing RAW data to Delta bronze layer...")
raw_employees_elt.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/tmp/elt_demo/employees_bronze")

print("✅ Raw data loaded to Bronze layer")

# Step 3: TRANSFORM - Read from bronze and transform INSIDE the lakehouse
print("\n⚙️ TRANSFORM: Reading from bronze and transforming INSIDE the lakehouse")
bronze_df = spark.read.format("delta").load("/tmp/elt_demo/employees_bronze")

silver_df = bronze_df \
    .withColumn("hire_date", to_date(col("hire_date"))) \
    .withColumn("department", upper(col("department"))) \
    .withColumn("annual_bonus", col("salary") * 0.10) \
    .withColumn("salary_band", 
                when(col("salary") >= 80000, "High")
                .when(col("salary") >= 70000, "Medium")
                .otherwise("Low")) \
    .withColumn("processing_timestamp", current_timestamp())

print("\n🥈 Silver layer (transformed inside lakehouse)")
display(silver_df)

# Write to silver layer
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/tmp/elt_demo/employees_silver")

print("✅ ELT Complete: Raw data loaded FIRST, transformed LATER inside the lakehouse")

# COMMAND ----------

# DBTITLE 1,Key Takeaway: ETL vs ELT
# MAGIC %md
# MAGIC ## 💡 Key Takeaway: ETL vs ELT
# MAGIC
# MAGIC ### ETL Approach:
# MAGIC ```
# MAGIC Source → Transform (outside) → Load (clean data only)
# MAGIC ```
# MAGIC * Transformation happens **before** data reaches target
# MAGIC * Only processed data is stored
# MAGIC * Cannot revisit raw data
# MAGIC
# MAGIC ### ELT Approach (Lakehouse):
# MAGIC ```
# MAGIC Source → Load (raw data) → Transform (inside lakehouse)
# MAGIC ```
# MAGIC * Raw data preserved in **Bronze layer**
# MAGIC * Transformations happen **inside** the data platform
# MAGIC * Can re-transform raw data anytime for new use cases
# MAGIC * **This is the Databricks way!**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Databricks Medallion Architecture
# MAGIC * **Bronze** — Raw data (ELT Load step)
# MAGIC * **Silver** — Cleaned, conformed data (ELT Transform step)
# MAGIC * **Gold** — Business-level aggregates