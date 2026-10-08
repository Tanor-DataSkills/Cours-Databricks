# Databricks notebook source
# DBTITLE 1,Closing & Next Steps
# MAGIC %md
# MAGIC ## 🎉 Congratulations! You've Completed Phase 6 Day 32!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✅ What You've Learned:
# MAGIC
# MAGIC 1. ✅ **Data Quality Fundamentals** — Why quality matters for analytics and ML
# MAGIC 2. ✅ **Null Handling** — Detection, drop, replace, and flag strategies
# MAGIC 3. ✅ **Deduplication** — Simple and window-based approaches
# MAGIC 4. ✅ **Validation Rules** — Range, format, business logic, and referential checks
# MAGIC 5. ✅ **Medallion Architecture** — Bronze (raw) → Silver (clean) → Gold (business)
# MAGIC 6. ✅ **SDP Constraints** — EXPECT, EXPECT OR FAIL, EXPECT OR DROP
# MAGIC 7. ✅ **Quality Pipeline** — End-to-end implementation
# MAGIC 8. ✅ **Metrics & Monitoring** — Tracking and alerting on data quality
# MAGIC 9. ✅ **Production Patterns** — Quarantine, Unity Catalog, Delta Lake
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Next Steps:
# MAGIC
# MAGIC #### Immediate Actions:
# MAGIC 1. **Run all cells** in this notebook to see data quality in action
# MAGIC 2. **Experiment** with your own datasets
# MAGIC 3. **Implement** quality checks in your current projects
# MAGIC 4. **Review** the interview questions to test your knowledge
# MAGIC
# MAGIC #### Advanced Learning:
# MAGIC 1. **Great Expectations** — Python library for data validation
# MAGIC 2. **deequ** — AWS library for data quality (Spark-based)
# MAGIC 3. **Monte Carlo** — Data observability platform
# MAGIC 4. **Data Quality Dashboards** — Build with Lakeview
# MAGIC 5. **Advanced SDP** — Complex constraint patterns
# MAGIC
# MAGIC #### Practice Projects:
# MAGIC 1. Build a complete medallion pipeline with quality checks
# MAGIC 2. Create a data quality dashboard
# MAGIC 3. Implement quarantine review workflow
# MAGIC 4. Set up automated quality alerts
# MAGIC 5. Design a data quality framework for your organization
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📚 Resources:
# MAGIC
# MAGIC * [Databricks Delta Lake Guide](https://docs.databricks.com/delta/)
# MAGIC * [Lakeflow Spark Declarative Pipelines](https://docs.databricks.com/workflows/delta-live-tables/)
# MAGIC * [Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/)
# MAGIC * [PySpark DataFrame API](https://spark.apache.org/docs/latest/api/python/)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💬 Feedback & Questions:
# MAGIC
# MAGIC If you have questions or feedback on this training:
# MAGIC * Review the interview questions section
# MAGIC * Practice with your own datasets
# MAGIC * Connect with the Databricks community
# MAGIC * Explore additional Databricks learning paths
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏆 Remember:
# MAGIC
# MAGIC > **"Data quality is not a one-time project — it's a continuous practice."**
# MAGIC
# MAGIC * Monitor your data quality metrics daily
# MAGIC * Iterate on validation rules as business needs change
# MAGIC * Maintain quarantine review processes
# MAGIC * Document your quality standards
# MAGIC * Share learnings with your team
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Keep Building! Keep Learning!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏷️ **@TRRaveendra**
# MAGIC ### 👨‍🏫 Author: TRRaveendra
# MAGIC ### 📌 Phase 6 — Day 32: Data Quality Complete!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **✨ Happy Data Engineering! ✨**

# COMMAND ----------

# DBTITLE 1,Section 8: Data Quality Metrics
# MAGIC %md
# MAGIC ## 📊 Section 8: Data Quality Metrics & Monitoring
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC Imagine your teacher giving you a report card. It shows your grades in each subject. **Data Quality Metrics** are like report cards for your data!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Data Quality Metrics** provide quantitative measures of data health and enable proactive monitoring.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📍 Key Data Quality Metrics:
# MAGIC
# MAGIC ### 1. **Completeness Metrics**
# MAGIC * Null rate per column
# MAGIC * Missing record percentage
# MAGIC * Required field coverage
# MAGIC
# MAGIC ### 2. **Validity Metrics**
# MAGIC * Constraint violation rate
# MAGIC * Format compliance percentage
# MAGIC * Business rule pass rate
# MAGIC
# MAGIC ### 3. **Uniqueness Metrics**
# MAGIC * Duplicate record rate
# MAGIC * Primary key violations
# MAGIC * Deduplication impact
# MAGIC
# MAGIC ### 4. **Consistency Metrics**
# MAGIC * Cross-field validation failures
# MAGIC * Referential integrity violations
# MAGIC * Data standardization compliance
# MAGIC
# MAGIC ### 5. **Timeliness Metrics**
# MAGIC * Data freshness (age)
# MAGIC * SLA compliance
# MAGIC * Pipeline latency
# MAGIC
# MAGIC ### 6. **Accuracy Metrics**
# MAGIC * Record match rate (vs source of truth)
# MAGIC * Data correction frequency
# MAGIC * User-reported issues
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Metric Best Practices:
# MAGIC
# MAGIC 1. **Establish Baselines**: Know your normal ranges
# MAGIC 2. **Set Thresholds**: Define acceptable limits
# MAGIC 3. **Trend Over Time**: Track improvements/degradation
# MAGIC 4. **Alert on Anomalies**: Proactive issue detection
# MAGIC 5. **Dashboard Visibility**: Make metrics accessible
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚨 When to Alert:
# MAGIC
# MAGIC | Metric | Threshold Example | Action |
# MAGIC | --- | --- | --- |
# MAGIC | Null Rate | > 5% for critical fields | Alert + investigate |
# MAGIC | Validation Failure | > 10% of records | Pipeline review |
# MAGIC | Duplicate Rate | > 2% | Source investigation |
# MAGIC | Data Freshness | > 6 hours old | Check ingestion |
# MAGIC | Record Count Drop | > 20% vs average | Immediate escalation |

# COMMAND ----------

# DBTITLE 1,Calculate Data Quality Metrics
# Calculate comprehensive data quality metrics

print("\n📊 DATA QUALITY METRICS DASHBOARD")
print("=" * 60)

from pyspark.sql.functions import lit, round as _round

# Metric 1: Overall Data Quality Score
total_input = df_bronze_final.count()
total_valid = df_silver.count()
total_invalid = df_quarantine.count()

quality_score = round((total_valid / total_input) * 100, 2)

print(f"\n🎯 OVERALL DATA QUALITY SCORE: {quality_score}%")
print("=" * 60)

# Metric 2: Record Flow Metrics
record_metrics = spark.createDataFrame([
    ("Input (Bronze)", total_input, 100.0),
    ("Valid (Silver)", total_valid, round(total_valid/total_input*100, 2)),
    ("Invalid (Quarantine)", total_invalid, round(total_invalid/total_input*100, 2)),
], ["stage", "record_count", "percentage"])

print("\n📊 Record Flow Metrics:")
display(record_metrics)

# Metric 3: Validation Rule Performance
validation_metrics = spark.createDataFrame([
    ("Age Validation", df_step4.filter(col("age_valid") == 0).count()),
    ("Amount Validation", df_step4.filter(col("amount_valid") == 0).count()),
    ("Email Validation", df_step4.filter(col("email_valid") == 0).count()),
    ("Tier Validation", df_step4.filter(col("tier_valid") == 0).count()),
    ("Date Validation", df_step4.filter(col("date_valid") == 0).count()),
], ["validation_rule", "violations"])

validation_metrics = validation_metrics.withColumn(
    "violation_rate_pct",
    _round((col("violations") / total_input) * 100, 2)
).withColumn(
    "pass_rate_pct",
    _round(100 - ((col("violations") / total_input) * 100), 2)
)

print("\n🔍 Validation Rule Performance:")
display(validation_metrics.orderBy(col("violations").desc()))

# Metric 4: Null Analysis
null_metrics = spark.createDataFrame([
    ("Email Nulls", df_bronze_final.filter(col("email").isNull()).count()),
    ("Age Nulls", df_bronze_final.filter(col("age").isNull()).count()),
], ["null_type", "count"])

null_metrics = null_metrics.withColumn(
    "null_rate_pct",
    _round((col("count") / total_input) * 100, 2)
)

print("\n🔍 Null Analysis:")
display(null_metrics)

# Metric 5: Deduplication Impact
duplication_metrics = spark.createDataFrame([
    ("Before Deduplication", df_step2.count()),
    ("After Deduplication", df_step3.count()),
    ("Duplicates Removed", df_step2.count() - df_step3.count()),
], ["stage", "count"])

print("\n🧹 Deduplication Impact:")
display(duplication_metrics)

print(f"\n\n📊 QUALITY SUMMARY:")
print(f"  ✅ Quality Score: {quality_score}% (Target: >95%)")
print(f"  📊 Records Processed: {total_input}")
print(f"  ✅ Clean Records: {total_valid}")
print(f"  ❌ Quarantined Records: {total_invalid}")
print(f"  🧹 Duplicates Removed: {duplicates_removed}")

# COMMAND ----------

# DBTITLE 1,Data Quality Monitoring Dashboard
# Create a monitoring dashboard summary

print("\n📊 DATA QUALITY MONITORING DASHBOARD")
print("=" * 60)

# Create comprehensive quality report
quality_report = spark.createDataFrame([
    ("Data Quality Score", f"{quality_score}%", "✅" if quality_score >= 80 else "⚠️"),
    ("Valid Records", f"{total_valid}/{total_input}", "✅"),
    ("Quarantined Records", str(total_invalid), "⚠️" if total_invalid > 0 else "✅"),
    ("Duplicate Rate", f"{round(duplicates_removed/total_input*100, 1)}%", "✅"),
    ("Age Violations", str(df_step4.filter(col("age_valid") == 0).count()), "⚠️"),
    ("Email Violations", str(df_step4.filter(col("email_valid") == 0).count()), "⚠️"),
    ("Amount Violations", str(df_step4.filter(col("amount_valid") == 0).count()), "⚠️"),
], ["metric", "value", "status"])

print("\n📊 Quality Report Card:")
display(quality_report)

# Quarantine reasons breakdown
if total_invalid > 0:
    print("\n👀 Quarantine Reason Breakdown:")
    quarantine_summary = df_quarantine.groupBy("quarantine_reason").count() \
        .withColumnRenamed("count", "record_count") \
        .withColumn("percentage", _round((col("record_count") / total_invalid) * 100, 2)) \
        .orderBy(col("record_count").desc())
    display(quarantine_summary)

print("\n\n🚨 ALERTING RECOMMENDATIONS:")
print("=" * 60)

if quality_score < 80:
    print("❌ CRITICAL: Quality score below 80%")
    print("   Action: Investigate source data issues immediately")
elif quality_score < 95:
    print("⚠️  WARNING: Quality score below 95%")
    print("   Action: Review validation rules and data sources")
else:
    print("✅ HEALTHY: Quality score above 95%")
    print("   Action: Continue monitoring")

if total_invalid > (total_input * 0.1):
    print("\n❌ CRITICAL: More than 10% of records quarantined")
    print("   Action: Urgent investigation required")

print("\n\n📌 MONITORING BEST PRACTICES:")
print("  1. Track metrics over time (daily/hourly)")
print("  2. Set up automated alerts (PagerDuty, Slack)")
print("  3. Create dashboards (Lakeview, Power BI, Tableau)")
print("  4. Review quarantined records daily")
print("  5. Measure SLA compliance (data freshness)")
print("  6. Conduct weekly data quality reviews")

# COMMAND ----------

# DBTITLE 1,Section 9: End-to-End Data Quality Pipeline
# MAGIC %md
# MAGIC ## 🎯 Section 9: End-to-End Production Pipeline
# MAGIC
# MAGIC ### Complete Production Architecture:
# MAGIC
# MAGIC ```
# MAGIC 🔄 SOURCE SYSTEMS (CRM, ERP, APIs)
# MAGIC          │
# MAGIC          ↓
# MAGIC 🟤 BRONZE LAYER (Raw + Metadata)
# MAGIC     │
# MAGIC     ├─> Unity Catalog: main.bronze.transactions
# MAGIC     ├─> Delta Format (ACID)
# MAGIC     └─> Partitioned by ingestion_date
# MAGIC          │
# MAGIC          ↓
# MAGIC 🔒 DATA QUALITY LAYER
# MAGIC     ├─> Null Detection & Handling
# MAGIC     ├─> Deduplication (Window Functions)
# MAGIC     ├─> Validation Rules (Business Logic)
# MAGIC     ├─> Format Standardization
# MAGIC     └─> Quality Metrics Tracking
# MAGIC          │
# MAGIC          ↓
# MAGIC     🔀 SPLIT
# MAGIC     ├──────────────────────┬──────────────────────┐
# MAGIC     │                       │                       │
# MAGIC     ↓                       ↓                       ↓
# MAGIC 🥈 SILVER LAYER     🚨 QUARANTINE TABLE    📊 METRICS TABLE
# MAGIC (Clean Data)          (Invalid Data)        (Quality Stats)
# MAGIC     │                       │                       │
# MAGIC     └──────────────────────┴──────────────────────┘
# MAGIC                       │
# MAGIC                       ↓
# MAGIC               📊 MONITORING
# MAGIC                  ├─> Dashboards
# MAGIC                  ├─> Alerts
# MAGIC                  └─> Reports
# MAGIC                       │
# MAGIC                       ↓
# MAGIC 🥇 GOLD LAYER (Business Analytics)
# MAGIC     ├─> Aggregations
# MAGIC     ├─> KPIs
# MAGIC     └─> Reports
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Implementation Checklist:
# MAGIC
# MAGIC ### ☐ Phase 1: Foundation
# MAGIC * [ ] Set up Unity Catalog structure
# MAGIC * [ ] Create Bronze/Silver/Gold schemas
# MAGIC * [ ] Define table schemas
# MAGIC * [ ] Implement Delta format
# MAGIC
# MAGIC ### ☐ Phase 2: Data Quality
# MAGIC * [ ] Define validation rules (business + technical)
# MAGIC * [ ] Implement null handling strategy
# MAGIC * [ ] Build deduplication logic
# MAGIC * [ ] Create quarantine tables
# MAGIC
# MAGIC ### ☐ Phase 3: Automation
# MAGIC * [ ] Convert to SDP pipeline
# MAGIC * [ ] Add EXPECT constraints
# MAGIC * [ ] Schedule pipeline runs
# MAGIC * [ ] Implement error handling
# MAGIC
# MAGIC ### ☐ Phase 4: Monitoring
# MAGIC * [ ] Build quality dashboards
# MAGIC * [ ] Set up alerting (Slack, PagerDuty)
# MAGIC * [ ] Create SLA tracking
# MAGIC * [ ] Implement trend analysis
# MAGIC
# MAGIC ### ☐ Phase 5: Governance
# MAGIC * [ ] Document data quality rules
# MAGIC * [ ] Define ownership (Data Stewards)
# MAGIC * [ ] Establish review processes
# MAGIC * [ ] Implement data lineage tracking

# COMMAND ----------

# DBTITLE 1,Production Pipeline: Unity Catalog Integration
# Production Implementation: Write to Unity Catalog Delta Tables
# Note: This is a demonstration. In production, use your Unity Catalog.

print("\n🎯 END-TO-END PRODUCTION PIPELINE")
print("=" * 60)

# Define Unity Catalog table names (update for your environment)
catalog_name = "main"  # Your Unity Catalog
schema_name = "data_quality_demo"  # Your schema

# Table paths (would be used in production)
bronze_table = f"{catalog_name}.{schema_name}.bronze_transactions"
silver_table = f"{catalog_name}.{schema_name}.silver_transactions"
quarantine_table = f"{catalog_name}.{schema_name}.quarantine_records"
metrics_table = f"{catalog_name}.{schema_name}.data_quality_metrics"

print("\n💾 Production Table Structure:")
print(f"  🟤 Bronze: {bronze_table}")
print(f"  🥈 Silver: {silver_table}")
print(f"  🚨 Quarantine: {quarantine_table}")
print(f"  📊 Metrics: {metrics_table}")

# In production, you would write like this:
print("\n\n📝 Production Write Pattern:")
print("""
# Write Bronze (Append mode)
df_bronze_final.write \\
    .format("delta") \\
    .mode("append") \\
    .option("mergeSchema", "true") \\
    .partitionBy("ingestion_date") \\
    .saveAsTable(bronze_table)

# Write Silver (Merge/Upsert)
from delta.tables import DeltaTable

if DeltaTable.isDeltaTable(spark, silver_table):
    silver_delta = DeltaTable.forName(spark, silver_table)
    silver_delta.alias("target").merge(
        df_silver.alias("source"),
        "target.customer_id = source.customer_id"
    ).whenMatchedUpdateAll() \\
     .whenNotMatchedInsertAll() \\
     .execute()
else:
    df_silver.write \\
        .format("delta") \\
        .mode("overwrite") \\
        .saveAsTable(silver_table)

# Write Quarantine (Append)
df_quarantine.write \\
    .format("delta") \\
    .mode("append") \\
    .saveAsTable(quarantine_table)

# Write Metrics (Append)
df_metrics.write \\
    .format("delta") \\
    .mode("append") \\
    .partitionBy("metric_date") \\
    .saveAsTable(metrics_table)
""")

print("\n✅ Production pipeline would write to Unity Catalog Delta tables")
print("\n🔒 Key Features:")
print("  ✅ ACID transactions (Delta Lake)")
print("  ✅ Schema evolution (mergeSchema)")
print("  ✅ Upsert capability (MERGE)")
print("  ✅ Time travel (version history)")
print("  ✅ Optimized storage (partitioning)")

# Calculate metrics locally from existing dataframes
local_total_input = df_bronze_final.count()
local_total_valid = df_silver.count()
local_total_invalid = df_quarantine.count()
local_quality_score = round((local_total_valid / local_total_input) * 100, 2)

print("\n\n📊 Final Pipeline Summary:")
print(f"  Input: {local_total_input} records")
print(f"  → Bronze: {local_total_input} records (100%)")
print(f"  → Silver: {local_total_valid} records ({round(local_total_valid/local_total_input*100,1)}%)")
print(f"  → Quarantine: {local_total_invalid} records ({round(local_total_invalid/local_total_input*100,1)}%)")
print(f"  → Quality Score: {local_quality_score}%")

# COMMAND ----------

# DBTITLE 1,Genie Code Agent Prompts
# MAGIC %md
# MAGIC ## 🧞 Genie Code Agent Usage Examples
# MAGIC
# MAGIC ### Data Quality Prompts:
# MAGIC
# MAGIC Here are example prompts you can use with Genie Code Agent for data quality tasks:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔍 Detection Prompts:
# MAGIC ```
# MAGIC ➡️ "Check for null values in the customers table"
# MAGIC ➡️ "Find duplicate records in transactions by customer_id"
# MAGIC ➡️ "Analyze data quality issues in the sales dataset"
# MAGIC ➡️ "Show me all invalid email addresses"
# MAGIC ➡️ "Detect outliers in the amount column"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔧 Transformation Prompts:
# MAGIC ```
# MAGIC ➡️ "Remove duplicate records from df_transactions"
# MAGIC ➡️ "Fill null values in email column with 'unknown@example.com'"
# MAGIC ➡️ "Apply validation rules: age > 0 and amount >= 0"
# MAGIC ➡️ "Deduplicate keeping the latest record by date"
# MAGIC ➡️ "Standardize email addresses to lowercase"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✅ Validation Prompts:
# MAGIC ```
# MAGIC ➡️ "Validate that all customer_ids are not null"
# MAGIC ➡️ "Check if transaction amounts are positive"
# MAGIC ➡️ "Flag records where start_date > end_date"
# MAGIC ➡️ "Verify email format contains @ and ."
# MAGIC ➡️ "Apply business rule: Premium tier requires amount > 100"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Metrics Prompts:
# MAGIC ```
# MAGIC ➡️ "Calculate data quality score for my dataset"
# MAGIC ➡️ "Show null percentage by column"
# MAGIC ➡️ "Generate validation report"
# MAGIC ➡️ "Count records by validation status"
# MAGIC ➡️ "Create data quality dashboard"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔒 SDP Constraint Prompts:
# MAGIC ```
# MAGIC ➡️ "Add SDP constraint for non-null customer_id"
# MAGIC ➡️ "Create EXPECT OR DROP rule for invalid emails"
# MAGIC ➡️ "Implement data quality constraints in my pipeline"
# MAGIC ➡️ "Show me SDP pipeline with validation rules"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Complete Pipeline Prompts:
# MAGIC ```
# MAGIC ➡️ "Build end-to-end data quality pipeline from Bronze to Silver"
# MAGIC ➡️ "Create pipeline with null handling, deduplication, and validation"
# MAGIC ➡️ "Implement quarantine pattern for invalid records"
# MAGIC ➡️ "Set up medallion architecture with data quality checks"
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Final Summary & Key Learnings
# MAGIC %md
# MAGIC ## 🎓 Final Summary: Data Quality Mastery
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 KEY LEARNINGS:
# MAGIC
# MAGIC #### 1. **Data Quality Dimensions**
# MAGIC * Completeness (no nulls)
# MAGIC * Validity (business rules)
# MAGIC * Uniqueness (no duplicates)
# MAGIC * Consistency (standardized)
# MAGIC * Accuracy (correct values)
# MAGIC * Timeliness (fresh data)
# MAGIC
# MAGIC #### 2. **Null Handling Strategies**
# MAGIC * **Detect**: Count and analyze nulls
# MAGIC * **Drop**: Remove incomplete records (data loss)
# MAGIC * **Replace**: Fill with defaults (imputation)
# MAGIC * **Flag**: Mark nulls and keep records (best practice)
# MAGIC
# MAGIC #### 3. **Deduplication Approaches**
# MAGIC * **Simple**: dropDuplicates() for exact matches
# MAGIC * **Window-based**: Keep latest/best record with control
# MAGIC * **Composite keys**: Multiple column uniqueness
# MAGIC
# MAGIC #### 4. **Validation Types**
# MAGIC * **Range**: Numeric bounds (age 0-120)
# MAGIC * **Format**: Pattern matching (email, phone)
# MAGIC * **Business Rules**: Domain logic (Premium > $100)
# MAGIC * **Referential**: Foreign key constraints
# MAGIC
# MAGIC #### 5. **Medallion Quality Strategy**
# MAGIC * **Bronze**: Capture all (minimal validation)
# MAGIC * **Silver**: Clean & validate (80% of quality work)
# MAGIC * **Gold**: Business-ready (aggregate & serve)
# MAGIC
# MAGIC #### 6. **SDP Constraints**
# MAGIC * **EXPECT**: Log violations (monitoring)
# MAGIC * **EXPECT OR FAIL**: Stop pipeline (critical rules)
# MAGIC * **EXPECT OR DROP**: Filter invalid records
# MAGIC
# MAGIC #### 7. **Quality Metrics**
# MAGIC * Quality score (% valid records)
# MAGIC * Violation rates per rule
# MAGIC * Null percentages
# MAGIC * Duplicate rates
# MAGIC * Quarantine trends
# MAGIC
# MAGIC #### 8. **Production Patterns**
# MAGIC * Quarantine invalid records (don't delete)
# MAGIC * Track metrics over time
# MAGIC * Alert on anomalies
# MAGIC * Maintain lineage (Bronze ID)
# MAGIC * Use Delta for ACID properties
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👍 BEST PRACTICES:
# MAGIC
# MAGIC 1. ✅ **Bronze preserves everything** — never filter raw data
# MAGIC 2. ✅ **Silver validates aggressively** — most quality work here
# MAGIC 3. ✅ **Quarantine, don't delete** — maintain audit trail
# MAGIC 4. ✅ **Flag nulls before filling** — preserve data quality lineage
# MAGIC 5. ✅ **Use window functions for dedup** — control which record to keep
# MAGIC 6. ✅ **Document validation rules** — business logic as code
# MAGIC 7. ✅ **Monitor metrics continuously** — proactive issue detection
# MAGIC 8. ✅ **Implement SDP constraints** — declarative quality enforcement
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ COMMON MISTAKES TO AVOID:
# MAGIC
# MAGIC #### 1. **Ignoring Null Values**
# MAGIC * ❌ Not detecting nulls before processing
# MAGIC * ❌ Assuming nulls will "just work"
# MAGIC * ❌ Filling nulls without flagging them
# MAGIC * ✅ **FIX**: Detect, flag, then handle appropriately
# MAGIC
# MAGIC #### 2. **Not Removing Duplicates**
# MAGIC * ❌ Allowing duplicates to propagate
# MAGIC * ❌ Using dropDuplicates() without understanding which record is kept
# MAGIC * ❌ Not tracking deduplication metrics
# MAGIC * ✅ **FIX**: Use window functions with explicit ordering
# MAGIC
# MAGIC #### 3. **Weak Validation Rules**
# MAGIC * ❌ Only checking for nulls (insufficient)
# MAGIC * ❌ Not validating business rules
# MAGIC * ❌ No format standardization
# MAGIC * ✅ **FIX**: Implement comprehensive validation (range, format, business logic)
# MAGIC
# MAGIC #### 4. **Skipping Data Quality Layer**
# MAGIC * ❌ Going directly from Bronze to Gold
# MAGIC * ❌ No Silver layer for cleaning
# MAGIC * ❌ Quality checks scattered everywhere
# MAGIC * ✅ **FIX**: Implement Medallion with Silver as quality gate
# MAGIC
# MAGIC #### 5. **Deleting Invalid Records**
# MAGIC * ❌ Permanent data loss
# MAGIC * ❌ No audit trail
# MAGIC * ❌ Can't reprocess if rules change
# MAGIC * ✅ **FIX**: Quarantine invalid records for review
# MAGIC
# MAGIC #### 6. **No Monitoring**
# MAGIC * ❌ Not tracking quality metrics
# MAGIC * ❌ No alerting on degradation
# MAGIC * ❌ Reactive instead of proactive
# MAGIC * ✅ **FIX**: Build dashboards and alerts
# MAGIC
# MAGIC #### 7. **Hardcoded Values**
# MAGIC * ❌ Magic numbers in code
# MAGIC * ❌ No configuration
# MAGIC * ❌ Difficult to maintain
# MAGIC * ✅ **FIX**: Externalize validation rules
# MAGIC
# MAGIC #### 8. **Ignoring Data Lineage**
# MAGIC * ❌ Can't trace record origins
# MAGIC * ❌ No metadata tracking
# MAGIC * ❌ Troubleshooting is impossible
# MAGIC * ✅ **FIX**: Add timestamps, source system, record IDs
# MAGIC
# MAGIC #### 9. **Using RDDs for Data Quality**
# MAGIC * ❌ Outdated API
# MAGIC * ❌ No Catalyst optimization
# MAGIC * ❌ Harder to read and maintain
# MAGIC * ✅ **FIX**: Use DataFrame API exclusively
# MAGIC
# MAGIC #### 10. **Not Testing Quality Rules**
# MAGIC * ❌ Broken validation logic
# MAGIC * ❌ False positives/negatives
# MAGIC * ❌ No unit tests
# MAGIC * ✅ **FIX**: Test validation rules with known good/bad data
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📚 FURTHER READING:
# MAGIC
# MAGIC * Databricks Data Quality Guide
# MAGIC * Delta Lake Best Practices
# MAGIC * Lakeflow Spark Declarative Pipelines Documentation
# MAGIC * Unity Catalog Governance
# MAGIC * DAMA Data Quality Framework

# COMMAND ----------

# DBTITLE 1,Interview Questions
# MAGIC %md
# MAGIC ## 🎯 Interview Questions: Data Quality
# MAGIC
# MAGIC ### 🟢 Beginner Level:
# MAGIC
# MAGIC **Q1: What is data quality and why is it important?**
# MAGIC * **Answer**: Data quality measures how well data serves its intended purpose based on dimensions like accuracy, completeness, consistency, validity, uniqueness, and timeliness. It's critical because poor quality data leads to incorrect business decisions, flawed analytics, unreliable ML models, compliance violations, and operational inefficiencies. The principle "Garbage In, Garbage Out" (GIGO) emphasizes that outputs are only as good as inputs.
# MAGIC
# MAGIC **Q2: What's the difference between NULL and empty string?**
# MAGIC * **Answer**: NULL represents the absence of a value (unknown/missing), while an empty string `""` is an actual value that happens to contain no characters. NULL means "we don't know," whereas empty string means "we know it's empty." In validation, they require different handling strategies.
# MAGIC
# MAGIC **Q3: How do you detect duplicate records in PySpark?**
# MAGIC * **Answer**: Multiple approaches:
# MAGIC   1. Compare total vs distinct: `df.count()` vs `df.distinct().count()`
# MAGIC   2. Group by key: `df.groupBy("id").count().filter(col("count") > 1)`
# MAGIC   3. Use dropDuplicates: `df.dropDuplicates(["key_col"])`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟡 Intermediate Level:
# MAGIC
# MAGIC **Q4: Explain the Medallion Architecture's approach to data quality.**
# MAGIC * **Answer**: 
# MAGIC   * **Bronze Layer**: Captures raw data with minimal validation, preserves complete history, append-only. Quality focus: capture everything.
# MAGIC   * **Silver Layer**: Applies strong validation, null handling, deduplication, standardization. Quality focus: cleaned, validated, analytics-ready.
# MAGIC   * **Gold Layer**: Business-ready aggregations and transformations. Quality focus: trusted metrics.
# MAGIC   
# MAGIC   The pattern progressively improves quality: Bronze (100% captured) → Silver (95-99% valid) → Gold (business logic).
# MAGIC
# MAGIC **Q5: What's the difference between dropDuplicates() and window-based deduplication?**
# MAGIC * **Answer**: 
# MAGIC   * **dropDuplicates()**: Keeps an arbitrary record when duplicates found. Simple but no control.
# MAGIC   * **Window-based**: Uses window functions (row_number(), rank()) with explicit ordering to control which record to keep (e.g., latest by timestamp, highest quality score). Provides deterministic, business-rule-driven deduplication.
# MAGIC
# MAGIC **Q6: What are the three types of SDP constraints and when would you use each?**
# MAGIC * **Answer**:
# MAGIC   * **EXPECT**: Logs violations but allows records through. Use for monitoring and non-critical validation.
# MAGIC   * **EXPECT OR FAIL**: Stops pipeline if violated. Use for critical business rules (e.g., customer_id NOT NULL).
# MAGIC   * **EXPECT OR DROP**: Silently drops invalid records. Use for non-essential fields where you want to continue processing valid records.
# MAGIC
# MAGIC **Q7: Describe the quarantine pattern for handling invalid data.**
# MAGIC * **Answer**: Instead of dropping invalid records, write them to a separate quarantine/error table with:
# MAGIC   * Original record
# MAGIC   * Validation flags showing which rules failed
# MAGIC   * Rejection reason
# MAGIC   * Timestamp and metadata
# MAGIC   
# MAGIC   Benefits: Maintains audit trail, enables investigation, allows reprocessing if rules change, prevents data loss.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 Advanced Level:
# MAGIC
# MAGIC **Q8: How would you design a data quality monitoring system?**
# MAGIC * **Answer**: 
# MAGIC   1. **Metrics Collection**: Track quality score, validation violation rates, null percentages, duplicate rates, record counts
# MAGIC   2. **Storage**: Write metrics to dedicated Delta table with timestamps
# MAGIC   3. **Dashboards**: Visualize trends over time (Lakeview, Power BI)
# MAGIC   4. **Alerting**: Set thresholds and alert on anomalies (>10% failures, sudden drops)
# MAGIC   5. **SLA Tracking**: Monitor data freshness and pipeline latency
# MAGIC   6. **Reporting**: Daily/weekly quality reports to stakeholders
# MAGIC   7. **Lineage**: Track data quality through Bronze → Silver → Gold
# MAGIC
# MAGIC **Q9: What are the challenges of implementing data quality at scale?**
# MAGIC * **Answer**:
# MAGIC   * **Performance**: Validation adds latency; need optimization (predicate pushdown, partitioning)
# MAGIC   * **False Positives**: Overly strict rules may reject valid edge cases
# MAGIC   * **Rule Maintenance**: Business rules change; need versioning and testing
# MAGIC   * **Storage**: Quarantine tables can grow large
# MAGIC   * **Complexity**: Balancing thoroughness vs processing time
# MAGIC   * **Coordination**: Quality rules must align across teams
# MAGIC   * **Cost**: Quality checks consume compute resources
# MAGIC
# MAGIC **Q10: How do you handle data quality in streaming pipelines vs batch?**
# MAGIC * **Answer**:
# MAGIC   * **Streaming**:
# MAGIC     - Real-time validation (lightweight checks)
# MAGIC     - Quarantine to error stream/table
# MAGIC     - Stateful deduplication (watermarks, state stores)
# MAGIC     - Near real-time alerting
# MAGIC     - Checkpointing for fault tolerance
# MAGIC   * **Batch**:
# MAGIC     - Comprehensive validation (can be expensive)
# MAGIC     - Full deduplication with window functions
# MAGIC     - Historical trend analysis
# MAGIC     - Reprocessing easier
# MAGIC   
# MAGIC   Key difference: Streaming prioritizes latency; batch prioritizes thoroughness.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💎 BONUS QUESTION:
# MAGIC
# MAGIC **Q11: How would you migrate existing data to add new validation rules without breaking downstream consumers?**
# MAGIC * **Answer**:
# MAGIC   1. **Backward Compatible**: Add new validation flags without removing records initially
# MAGIC   2. **Phased Rollout**: 
# MAGIC      - Phase 1: EXPECT (warn only)
# MAGIC      - Phase 2: Monitor violations, adjust rules
# MAGIC      - Phase 3: Switch to EXPECT OR DROP
# MAGIC   3. **Soft Delete**: Flag invalid records instead of hard delete
# MAGIC   4. **Parallel Processing**: Run old and new pipelines side-by-side, compare results
# MAGIC   5. **Communication**: Alert downstream teams before enforcement
# MAGIC   6. **Backfill Strategy**: Reprocess historical data with new rules if needed
# MAGIC   7. **Schema Evolution**: Use Delta's mergeSchema for new columns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Quick Fire Questions:
# MAGIC
# MAGIC 1. **What's better for null handling: drop or fill?** → Depends on context; flagging is often best
# MAGIC 2. **Should you validate in Bronze?** → No, minimal validation only (schema conformance)
# MAGIC 3. **Where does most quality work happen?** → Silver layer
# MAGIC 4. **Can you modify Bronze data?** → No, it's immutable (except GDPR/retention)
# MAGIC 5. **What's the ideal Silver layer quality score?** → 95-99%
# MAGIC 6. **Should you use cache() for quality checks?** → No, avoid on Serverless
# MAGIC 7. **Best deduplication strategy?** → Window functions with business logic
# MAGIC 8. **How to handle PII in quarantine?** → Mask/encrypt sensitive fields
# MAGIC 9. **Validation before or after deduplication?** → After (cleaner logic)
# MAGIC 10. **Should validation rules be in code or config?** → Config for flexibility

# COMMAND ----------

# DBTITLE 1,Section 6: SDP Constraints Integration
# MAGIC %md
# MAGIC ## 🔒 Section 6: SDP Constraints Integration
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC Imagine a bouncer at a club checking IDs. If someone's too young, they can't enter. **SDP Constraints** are like automatic bouncers for your data!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Lakeflow Spark Declarative Pipelines (SDP)** supports declarative data quality constraints using **EXPECT** clauses.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Constraint Types:
# MAGIC
# MAGIC ### 1. **EXPECT** (Warn)
# MAGIC ```sql
# MAGIC CONSTRAINT valid_age EXPECT (age > 0 AND age <= 120)
# MAGIC ```
# MAGIC * Logs violations but **allows records through**
# MAGIC * Good for monitoring and alerting
# MAGIC * Non-blocking
# MAGIC
# MAGIC ### 2. **EXPECT OR FAIL** (Reject)
# MAGIC ```sql
# MAGIC CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE
# MAGIC ```
# MAGIC * **Stops the pipeline** if constraint violated
# MAGIC * Use for critical business rules
# MAGIC * Ensures zero bad data in downstream
# MAGIC
# MAGIC ### 3. **EXPECT OR DROP** (Filter)
# MAGIC ```sql
# MAGIC CONSTRAINT valid_amount EXPECT (transaction_amount >= 0) ON VIOLATION DROP ROW
# MAGIC ```
# MAGIC * **Silently drops** invalid records
# MAGIC * Continues processing valid records
# MAGIC * Good for non-critical fields
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 When to Use Each:
# MAGIC
# MAGIC | Constraint Type | Use Case | Impact |
# MAGIC | --- | --- | --- |
# MAGIC | **EXPECT** | Non-critical validation, monitoring | Logs only |
# MAGIC | **EXPECT OR FAIL** | Critical business rules (customer_id, order_id) | Pipeline fails |
# MAGIC | **EXPECT OR DROP** | Optional enrichment, non-essential data | Drops invalid rows |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔍 Monitoring Constraints:
# MAGIC
# MAGIC SDP automatically tracks:
# MAGIC * Number of violations per constraint
# MAGIC * Percentage of records failing
# MAGIC * Historical trend of data quality
# MAGIC
# MAGIC Available in:
# MAGIC * Pipeline Event Log
# MAGIC * Data Quality Dashboard
# MAGIC * System tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 SDP Pipeline Example:
# MAGIC
# MAGIC ```python
# MAGIC import dlt
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC @dlt.table(
# MAGIC   comment="Silver layer with data quality constraints",
# MAGIC   table_properties={
# MAGIC     "quality": "silver",
# MAGIC     "pipelines.autoOptimize.managed": "true"
# MAGIC   }
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_age", "age > 0 AND age <= 120")
# MAGIC @dlt.expect_or_drop("valid_amount", "transaction_amount >= 0")
# MAGIC @dlt.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
# MAGIC @dlt.expect("valid_email", "email LIKE '%@%'")
# MAGIC def silver_customers():
# MAGIC     return dlt.read("bronze_customers")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Benefits of SDP Constraints:
# MAGIC
# MAGIC 1. **Declarative**: Define rules, not implementation
# MAGIC 2. **Automated**: Applied automatically during ingestion
# MAGIC 3. **Monitored**: Built-in metrics and alerting
# MAGIC 4. **Versioned**: Constraints tracked in pipeline code
# MAGIC 5. **Auditable**: Complete lineage and violation history

# COMMAND ----------

# DBTITLE 1,Simulating SDP Constraints in Notebook
# Simulate SDP constraint behavior in standard notebook
# Note: Real SDP constraints are defined in pipeline code

print("\n🔒 SIMULATING SDP CONSTRAINT BEHAVIORS")
print("=" * 60)
print("\nNote: This simulates SDP. Real SDP uses @dlt.expect decorators.")
print("=" * 60)

from pyspark.sql.functions import col, when, lit

# Use our validation dataset
df_source = df_validation

# Define constraints (similar to SDP)
constraints = [
    {"name": "valid_customer_id", "condition": col("customer_id").isNotNull(), "action": "FAIL"},
    {"name": "valid_age", "condition": (col("age") > 0) & (col("age") <= 120), "action": "DROP"},
    {"name": "valid_amount", "condition": col("transaction_amount") >= 0, "action": "DROP"},
    {"name": "valid_email", "condition": col("email").contains("@"), "action": "WARN"},
    {"name": "valid_tier", "condition": col("tier").isin(["Premium", "Standard"]), "action": "DROP"},
]

print("\n📝 Defined Constraints:")
for idx, constraint in enumerate(constraints, 1):
    print(f"{idx}. {constraint['name']:25s} - Action: {constraint['action']}")

# Apply constraints
df_constrained = df_source
constraint_violations = {}

for constraint in constraints:
    constraint_name = constraint["name"]
    condition = constraint["condition"]
    action = constraint["action"]
    
    # Count violations
    violations = df_constrained.filter(~condition).count()
    constraint_violations[constraint_name] = violations
    
    if action == "FAIL" and violations > 0:
        print(f"\n❌ PIPELINE WOULD FAIL: {constraint_name}")
        print(f"   {violations} violations detected for critical constraint")
        print("   In real SDP: Pipeline execution stops here")
        # In real SDP, pipeline fails here
    
    elif action == "DROP":
        before_count = df_constrained.count()
        df_constrained = df_constrained.filter(condition)
        after_count = df_constrained.count()
        dropped = before_count - after_count
        if dropped > 0:
            print(f"\n🗑️  DROPPED ROWS: {constraint_name}")
            print(f"   {dropped} rows dropped ({round(dropped/before_count*100, 1)}% of data)")
    
    elif action == "WARN" and violations > 0:
        print(f"\n⚠️  WARNING: {constraint_name}")
        print(f"   {violations} violations detected (rows kept, logged)")

print("\n\n📊 Constraint Violation Summary:")
print("| Constraint | Violations | Action |")
print("| --- | --- | --- |")
for constraint in constraints:
    name = constraint["name"]
    action = constraint["action"]
    violations = constraint_violations.get(name, 0)
    print(f"| {name} | {violations} | {action} |")

print(f"\n\n✅ Final Result:")
print(f"   Input Records: {df_source.count()}")
print(f"   Output Records: {df_constrained.count()}")
print(f"   Dropped: {df_source.count() - df_constrained.count()}")

# COMMAND ----------

# DBTITLE 1,Section 7: Hands-on Data Quality Pipeline
# MAGIC %md
# MAGIC ## 🛠️ Section 7: Complete Data Quality Pipeline
# MAGIC
# MAGIC ### End-to-End Implementation:
# MAGIC
# MAGIC ```
# MAGIC 📥 Raw Data (Bronze)
# MAGIC    ↓
# MAGIC 🔍 Step 1: Detect Issues
# MAGIC    ↓
# MAGIC ❓ Step 2: Handle Nulls
# MAGIC    ↓
# MAGIC 🧹 Step 3: Deduplicate
# MAGIC    ↓
# MAGIC ✅ Step 4: Validate Rules
# MAGIC    ↓
# MAGIC 🔀 Step 5: Split Valid/Invalid
# MAGIC    ↓
# MAGIC 💾 Write to Delta Tables
# MAGIC    ├──> ✅ Silver (Clean)
# MAGIC    └──> ❌ Quarantine (Issues)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Let's build a complete pipeline!

# COMMAND ----------

# DBTITLE 1,Pipeline Step 1: Create Bronze Table
# Complete Data Quality Pipeline - Step 1: Bronze (Unity Catalog)

print("\n🔧 DATA QUALITY PIPELINE - Step 1: Bronze Layer")
print("=" * 60)

# For this demo, we'll use a local catalog. In production, use your Unity Catalog.
# Example: main.data_quality_demo.bronze_transactions

# Create sample data with all types of issues
data_complete = [
    (1, "Alice", "alice@email.com", 25, 100.0, "2026-04-01", "2026-04-10", "Premium"),
    (1, "Alice", "alice@email.com", 25, 100.0, "2026-04-01", "2026-04-10", "Premium"),  # Duplicate
    (2, "Bob", None, 30, 200.0, "2026-04-02", "2026-04-05", "Standard"),  # Null email
    (3, "Charlie", "charlie@email.com", -5, 150.0, "2026-04-03", "2026-04-08", "Premium"),  # Invalid age
    (4, "David", "david.email.com", 35, 300.0, "2026-04-04", "2026-04-12", "Premium"),  # Invalid email
    (5, "Eve", "eve@email.com", 28, -50.0, "2026-04-05", "2026-04-15", "Standard"),  # Negative amount
    (6, "Frank", "frank@email.com", 35, 250.0, "2026-04-20", "2026-04-10", "Premium"),  # Start > End
    (7, "Grace", "grace@email.com", 40, 175.0, "2026-04-01", "2026-04-05", "Gold"),  # Invalid tier
    (8, "Henry", "henry@email.com", 150, 225.0, "2026-04-02", "2026-04-06", "Premium"),  # Age > 120
    (9, "Iris", "iris@email.com", None, 400.0, "2026-04-03", "2026-04-10", "Premium"),  # Null age
    (10, "Jack", "jack@email.com", 33, 225.0, "2026-04-04", "2026-04-15", "Standard"),  # Valid
]

df_bronze_raw = spark.createDataFrame(data_complete, [
    "customer_id", "customer_name", "email", "age", "transaction_amount",
    "start_date", "end_date", "tier"
])

# Add Bronze metadata
df_bronze_final = df_bronze_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_system", lit("CRM")) \
    .withColumn("bronze_id", expr("uuid()"))

print(f"\n✅ Bronze layer created: {df_bronze_final.count()} records")
print("\n📊 Bronze Data (All records preserved):")
display(df_bronze_final)

print("\n👉 Next: Apply data quality transformations")

# COMMAND ----------

# DBTITLE 1,Pipeline Step 2-3: Null Handling & Deduplication
# Complete Data Quality Pipeline - Steps 2-3

print("\n🔧 DATA QUALITY PIPELINE - Steps 2-3: Null Handling & Deduplication")
print("=" * 60)

# Step 2: Handle Nulls (flag approach)
df_step2 = df_bronze_final \
    .withColumn("email_is_null", when(col("email").isNull(), 1).otherwise(0)) \
    .withColumn("age_is_null", when(col("age").isNull(), 1).otherwise(0)) \
    .fillna({
        "email": "unknown@example.com",
        "age": 0
    })

null_flags = df_step2.filter((col("email_is_null") == 1) | (col("age_is_null") == 1)).count()
print(f"\n✅ Step 2 Complete: Nulls flagged and filled")
print(f"   Records with nulls: {null_flags}")

# Step 3: Deduplicate (keep latest by ingestion_timestamp)
window_dedup = Window.partitionBy("customer_id", "email").orderBy(col("ingestion_timestamp").desc())

df_step3 = df_step2 \
    .withColumn("row_num", row_number().over(window_dedup)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

duplicates_removed = df_step2.count() - df_step3.count()
print(f"\n✅ Step 3 Complete: Deduplication applied")
print(f"   Records before: {df_step2.count()}")
print(f"   Records after: {df_step3.count()}")
print(f"   Duplicates removed: {duplicates_removed}")

print("\n📊 Data after Steps 2-3:")
display(df_step3.select(
    "customer_id", "customer_name", "email", "age", "transaction_amount",
    "email_is_null", "age_is_null"
))

# COMMAND ----------

# DBTITLE 1,Pipeline Step 4-5: Validation & Split
# Complete Data Quality Pipeline - Steps 4-5

print("\n🔧 DATA QUALITY PIPELINE - Steps 4-5: Validation & Split")
print("=" * 60)

# Step 4: Apply all validation rules
df_step4 = df_step3 \
    .withColumn("age_valid", 
                when((col("age") > 0) & (col("age") <= 120), 1).otherwise(0)) \
    .withColumn("amount_valid", 
                when(col("transaction_amount") >= 0, 1).otherwise(0)) \
    .withColumn("email_valid", 
                when((col("email").contains("@")) & (col("email").contains(".")), 1).otherwise(0)) \
    .withColumn("tier_valid", 
                when(col("tier").isin(["Premium", "Standard"]), 1).otherwise(0)) \
    .withColumn("date_valid", 
                when(col("start_date") < col("end_date"), 1).otherwise(0)) \
    .withColumn("is_valid",
                when(
                    (col("age_valid") == 1) &
                    (col("amount_valid") == 1) &
                    (col("email_valid") == 1) &
                    (col("tier_valid") == 1) &
                    (col("date_valid") == 1),
                    1
                ).otherwise(0)) \
    .withColumn("validation_timestamp", current_timestamp())

print(f"\n✅ Step 4 Complete: Validation rules applied")

# Step 5: Split into Silver (valid) and Quarantine (invalid)
df_silver = df_step4.filter(col("is_valid") == 1).select(
    "customer_id", "customer_name", "email", "age", "transaction_amount",
    "start_date", "end_date", "tier",
    "bronze_id", "ingestion_timestamp", "validation_timestamp", "source_system"
)

df_quarantine = df_step4.filter(col("is_valid") == 0).select(
    "customer_id", "customer_name", "email", "age", "transaction_amount",
    "start_date", "end_date", "tier",
    "age_valid", "amount_valid", "email_valid", "tier_valid", "date_valid",
    "bronze_id", "ingestion_timestamp", "validation_timestamp"
).withColumn(
    "quarantine_reason",
    expr(
        "CASE " +
        "WHEN age_valid = 0 THEN 'Invalid Age' " +
        "WHEN amount_valid = 0 THEN 'Negative Amount' " +
        "WHEN email_valid = 0 THEN 'Invalid Email' " +
        "WHEN tier_valid = 0 THEN 'Invalid Tier' " +
        "WHEN date_valid = 0 THEN 'Invalid Dates' " +
        "ELSE 'Multiple Issues' END"
    )
)

print(f"\n✅ Step 5 Complete: Data split into Silver and Quarantine")
print(f"\n📊 Pipeline Results:")
print(f"   🟤 Bronze (Input): {df_bronze_final.count()} records")
print(f"   → After deduplication: {df_step3.count()} records")
print(f"   → 🥈 Silver (Valid): {df_silver.count()} records ({round(df_silver.count()/df_bronze_final.count()*100, 1)}%)")
print(f"   → 🚨 Quarantine (Invalid): {df_quarantine.count()} records ({round(df_quarantine.count()/df_bronze_final.count()*100, 1)}%)")

print("\n👀 Silver Data (Clean):")
display(df_silver)

print("\n👀 Quarantine Data (Issues):")
display(df_quarantine.select(
    "customer_id", "customer_name", "age", "email", "tier", "quarantine_reason"
))

# COMMAND ----------

# DBTITLE 1,Section 5: Data Quality in Medallion Architecture
# MAGIC %md
# MAGIC ## 🏛️ Section 5: Data Quality in Medallion Architecture
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC Imagine making a smoothie: You start with whole fruits (Bronze), then wash and cut them (Silver), then blend into a perfect drink (Gold). Each step makes it better!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC The **Medallion Architecture** (Bronze-Silver-Gold) naturally implements progressive data quality.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🟤 Bronze Layer: Raw Data Ingestion
# MAGIC
# MAGIC ### Purpose:
# MAGIC * Capture data **exactly as received**
# MAGIC * Preserve complete history
# MAGIC * Enable replay and reprocessing
# MAGIC
# MAGIC ### Data Quality Strategy:
# MAGIC * ❌ **NO transformations**
# MAGIC * ❌ **NO filtering**
# MAGIC * ✅ **Minimal validation** (schema conformance only)
# MAGIC * ✅ **Append-only** (immutable)
# MAGIC * ✅ **Add metadata** (ingestion_time, source_system)
# MAGIC
# MAGIC ### Why?
# MAGIC * Don't lose data — you might need it later
# MAGIC * Troubleshooting requires raw data
# MAGIC * Regulatory requirements (audit trail)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🥈 Silver Layer: Cleaned & Validated Data
# MAGIC
# MAGIC ### Purpose:
# MAGIC * **Cleaned** data (nulls handled)
# MAGIC * **Validated** data (business rules applied)
# MAGIC * **Deduplicated** records
# MAGIC * **Standardized** formats
# MAGIC
# MAGIC ### Data Quality Strategy:
# MAGIC * ✅ **Strong validation**
# MAGIC * ✅ **Null handling**
# MAGIC * ✅ **Deduplication**
# MAGIC * ✅ **Format standardization**
# MAGIC * ✅ **Business rule enforcement**
# MAGIC * ✅ **Quarantine invalid records**
# MAGIC
# MAGIC ### This is where **80% of data quality work happens!**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🥇 Gold Layer: Business-Ready Analytics
# MAGIC
# MAGIC ### Purpose:
# MAGIC * **Aggregated** metrics
# MAGIC * **Business logic** applied
# MAGIC * **Denormalized** for performance
# MAGIC * **Trusted** data for reports/dashboards
# MAGIC
# MAGIC ### Data Quality Strategy:
# MAGIC * ✅ **Assume Silver is clean**
# MAGIC * ✅ **Apply business transformations**
# MAGIC * ✅ **Additional validation** if needed
# MAGIC * ✅ **SLA monitoring**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Quality Progression:
# MAGIC
# MAGIC | Layer | Quality Focus | Valid % | Use Case |
# MAGIC | --- | --- | --- | --- |
# MAGIC | **Bronze** | Capture All | ~100% | Raw archive, replay |
# MAGIC | **Silver** | Clean & Validate | 95-99% | Analytics-ready |
# MAGIC | **Gold** | Business Logic | ~100% | Reports, dashboards |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Error Handling Flow:
# MAGIC
# MAGIC ```
# MAGIC Bronze (Raw) 
# MAGIC   │
# MAGIC   └──> Silver (Validated)
# MAGIC          │
# MAGIC          ├──> Valid Records → Gold
# MAGIC          │
# MAGIC          └──> Invalid Records → Quarantine
# MAGIC                                     │
# MAGIC                                     └──> Manual Review / Reprocessing
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Medallion Architecture: Bronze Layer
# Demonstrate Bronze Layer: Capture raw data with minimal processing

print("\n🟤 BRONZE LAYER: RAW DATA INGESTION")
print("=" * 60)

# Simulate raw data ingestion
from pyspark.sql.functions import current_timestamp, lit

# Use our validation dataset as "raw" source
df_bronze = df_validation.withColumn(
    "ingestion_timestamp",
    current_timestamp()
).withColumn(
    "source_system",
    lit("CRM")
).withColumn(
    "bronze_record_id",
    expr("uuid()")  # Add unique bronze record ID
)

print("\n📊 Bronze Layer Characteristics:")
print(f"  ✅ All records preserved: {df_bronze.count()} records")
print(f"  ✅ No filtering applied")
print(f"  ✅ Metadata added: ingestion_timestamp, source_system, bronze_record_id")
print(f"  ✅ Append-only (immutable)")

print("\n👀 Bronze Layer Sample:")
display(df_bronze.select(
    "bronze_record_id", "customer_id", "customer_name", "email", 
    "transaction_amount", "ingestion_timestamp", "source_system"
).limit(5))

print("\n📌 Bronze Layer Best Practices:")
print("  1. Preserve ALL data (even invalid)")
print("  2. Add ingestion metadata")
print("  3. Use Delta format for ACID properties")
print("  4. Partition by ingestion date for performance")
print("  5. Never delete from Bronze (except for GDPR/retention)")

# COMMAND ----------

# DBTITLE 1,Medallion Architecture: Silver Layer
# Demonstrate Silver Layer: Apply data quality transformations

print("\n🥈 SILVER LAYER: CLEANED & VALIDATED DATA")
print("=" * 60)

# Apply ALL data quality transformations from previous sections
df_silver_processing = df_bronze

# Step 1: Standardize formats
df_silver_processing = df_silver_processing.withColumn(
    "email_clean",
    lower(trim(col("email")))
)

# Step 2: Handle nulls (fill with defaults where appropriate)
df_silver_processing = df_silver_processing.fillna({
    "discount_pct": 0
})

# Step 3: Apply validation rules
df_silver_processing = df_silver_processing.withColumn(
    "age_valid",
    when((col("age") > 0) & (col("age") <= 120), 1).otherwise(0)
).withColumn(
    "amount_valid",
    when(col("transaction_amount") >= 0, 1).otherwise(0)
).withColumn(
    "email_valid",
    when((col("email_clean").contains("@")) & (col("email_clean").contains(".")), 1).otherwise(0)
).withColumn(
    "tier_valid",
    when(col("tier").isin(["Premium", "Standard"]), 1).otherwise(0)
).withColumn(
    "date_valid",
    when(col("start_date") < col("end_date"), 1).otherwise(0)
)

# Step 4: Create overall validity flag
df_silver_processing = df_silver_processing.withColumn(
    "is_valid",
    when(
        (col("age_valid") == 1) &
        (col("amount_valid") == 1) &
        (col("email_valid") == 1) &
        (col("tier_valid") == 1) &
        (col("date_valid") == 1),
        1
    ).otherwise(0)
).withColumn(
    "silver_processing_timestamp",
    current_timestamp()
)

# Step 5: Separate valid and invalid (quarantine pattern)
df_silver = df_silver_processing.filter(col("is_valid") == 1).select(
    "customer_id",
    "customer_name",
    "email_clean",
    "age",
    "transaction_amount",
    "start_date",
    "end_date",
    "tier",
    "discount_pct",
    "bronze_record_id",
    "ingestion_timestamp",
    "silver_processing_timestamp",
    "source_system"
)

df_quarantine = df_silver_processing.filter(col("is_valid") == 0).select(
    "*"
).withColumn(
    "quarantine_reason",
    expr(
        "CASE " +
        "WHEN age_valid = 0 THEN 'Invalid Age' " +
        "WHEN amount_valid = 0 THEN 'Negative Amount' " +
        "WHEN email_valid = 0 THEN 'Invalid Email Format' " +
        "WHEN tier_valid = 0 THEN 'Invalid Tier' " +
        "WHEN date_valid = 0 THEN 'Invalid Date Sequence' " +
        "ELSE 'Multiple Violations' END"
    )
)

print("\n📊 Silver Layer Results:")
print(f"  ✅ Valid records (Silver): {df_silver.count()}")
print(f"  ⚠️ Invalid records (Quarantine): {df_quarantine.count()}")
print(f"  📊 Data Quality Score: {round(df_silver.count() / df_bronze.count() * 100, 2)}%")

print("\n👀 Silver Layer Sample (Clean Data):")
display(df_silver.limit(5))

print("\n👀 Quarantine Table Sample:")
display(df_quarantine.select(
    "customer_id", "customer_name", "age", "email", "tier", "quarantine_reason"
))

# COMMAND ----------

# DBTITLE 1,Medallion Architecture: Gold Layer
# Demonstrate Gold Layer: Business-ready aggregations

print("\n🥇 GOLD LAYER: BUSINESS-READY ANALYTICS")
print("=" * 60)

# Create business aggregations from clean Silver data
from pyspark.sql.functions import avg

# Gold Table 1: Customer Summary by Tier
df_gold_tier_summary = df_silver.groupBy("tier").agg(
    count("customer_id").alias("customer_count"),
    _sum("transaction_amount").alias("total_revenue"),
    _round(avg("transaction_amount"), 2).alias("avg_transaction"),
    _round(avg("age"), 2).alias("avg_age")
).withColumn(
    "gold_created_timestamp",
    current_timestamp()
)

print("\n📊 Gold Table 1: Tier Summary")
display(df_gold_tier_summary)

# Gold Table 2: Customer Lifetime Value
df_gold_customer_ltv = df_silver.groupBy("customer_id", "customer_name", "tier").agg(
    _sum("transaction_amount").alias("lifetime_value"),
    count("*").alias("transaction_count"),
    _round(avg("transaction_amount"), 2).alias("avg_transaction_value")
).withColumn(
    "customer_segment",
    when(col("lifetime_value") >= 200, "High Value")
    .when(col("lifetime_value") >= 100, "Medium Value")
    .otherwise("Low Value")
).withColumn(
    "gold_created_timestamp",
    current_timestamp()
)

print("\n📊 Gold Table 2: Customer Lifetime Value")
display(df_gold_customer_ltv.orderBy(col("lifetime_value").desc()))

print("\n\n✅ Medallion Architecture Complete!")
print("\n📊 Quality Flow Summary:")
print(f"  Bronze (Raw): {df_bronze.count()} records")
print(f"  → Silver (Valid): {df_silver.count()} records ({round(df_silver.count()/df_bronze.count()*100, 1)}%)")
print(f"  → Quarantine: {df_quarantine.count()} records ({round(df_quarantine.count()/df_bronze.count()*100, 1)}%)")
print(f"  → Gold (Aggregated): {df_gold_tier_summary.count()} tier summaries + {df_gold_customer_ltv.count()} customer profiles")

print("\n📌 Key Takeaway:")
print("  Each layer has a specific data quality role")
print("  Quality improves progressively: Bronze → Silver → Gold")
print("  Invalid records are quarantined, not deleted")

# COMMAND ----------

# DBTITLE 1,Section 4: Validation Rules
# MAGIC %md
# MAGIC ## ✅ Section 4: Validation Rules & Constraints
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC Imagine your teacher says "homework must be between 1 and 10 pages." If you submit 50 pages or 0 pages, that's wrong. **Validation rules** are like those homework rules — they check if data makes sense.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Validation Rules** are business logic constraints that ensure data conforms to expected patterns, ranges, formats, and relationships.
# MAGIC
# MAGIC #### Types of Validation:
# MAGIC
# MAGIC 1. **Range Validation**
# MAGIC    * Numeric ranges (age > 0, price >= 0)
# MAGIC    * Date ranges (hire_date <= current_date)
# MAGIC    * Logical bounds (discount <= 100%)
# MAGIC
# MAGIC 2. **Format Validation**
# MAGIC    * Email format (contains @)
# MAGIC    * Phone format (digits only, correct length)
# MAGIC    * ZIP code format
# MAGIC    * RegEx patterns
# MAGIC
# MAGIC 3. **Business Rule Validation**
# MAGIC    * Domain-specific constraints
# MAGIC    * Cross-column validation (start_date < end_date)
# MAGIC    * Conditional rules (if Premium, then amount > 100)
# MAGIC
# MAGIC 4. **Referential Integrity**
# MAGIC    * Foreign key constraints
# MAGIC    * Lookups in dimension tables
# MAGIC    * Cross-table validation
# MAGIC
# MAGIC 5. **Completeness Validation**
# MAGIC    * Required fields not null
# MAGIC    * Minimum data quality threshold
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Validation Approaches:
# MAGIC
# MAGIC | Approach | When to Use | Pros | Cons |
# MAGIC | --- | --- | --- | --- |
# MAGIC | **Filter** | Remove invalid records | Clean data | Data loss |
# MAGIC | **Flag** | Mark invalid records | Keep all data | Requires handling |
# MAGIC | **Reject** | Send to error table | Audit trail | Extra storage |
# MAGIC | **Fix** | Correct invalid data | No data loss | Risk of wrong fix |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚨 Impact of Invalid Data:
# MAGIC
# MAGIC * **Analytics**: Wrong insights (negative ages, future dates)
# MAGIC * **ML**: Poor model quality (outliers, impossible values)
# MAGIC * **Business**: Failed processes (invalid emails, wrong calculations)
# MAGIC * **Compliance**: Regulatory violations (data retention, privacy)

# COMMAND ----------

# DBTITLE 1,Create Sample Data with Validation Issues
# Create sample data with various validation issues

data_with_issues = [
    (1, "Alice", "alice@email.com", 25, 100.0, "2026-04-01", "2026-04-10", "Premium", 10),
    (2, "Bob", "bob.email.com", 30, 200.0, "2026-04-02", "2026-04-05", "Standard", 5),  # Invalid email
    (3, "Charlie", "charlie@email.com", -5, 150.0, "2026-04-03", "2026-04-08", "Premium", 0),  # Negative age
    (4, "David", "david@email.com", 150, 300.0, "2026-04-04", "2026-04-12", "Premium", 8),  # Age > 120
    (5, "Eve", "eve@email.com", 28, -50.0, "2026-04-05", "2026-04-15", "Standard", 15),  # Negative amount
    (6, "Frank", "frank@email.com", 35, 250.0, "2026-04-20", "2026-04-10", "Premium", 5),  # Start > End
    (7, "Grace", "grace@email.com", 40, 175.0, "2026-04-01", "2026-04-05", "Gold", 4),  # Invalid tier
    (8, "Henry", "henry@email.com", 22, 50.0, "2026-04-02", "2026-04-06", "Premium", 3),  # Premium < 100
    (9, "Iris", "IRIS@EMAIL.COM", 29, 400.0, "2026-04-03", "2026-04-10", "Premium", 7),  # Uppercase email
    (10, "Jack", "jack@email.com", 33, 225.0, "2026-04-04", "2026-04-15", "Standard", 11),  # Valid
]

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("transaction_amount", DoubleType(), True),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True),
    StructField("tier", StringType(), True),
    StructField("discount_pct", IntegerType(), True)
])

df_validation = spark.createDataFrame(data_with_issues, schema)

print("\n📊 Raw Data with Validation Issues:")
print(f"Total Records: {df_validation.count()}")
display(df_validation)

# COMMAND ----------

# DBTITLE 1,Range Validation
# Validation Type 1: RANGE validation

print("\n📊 RANGE VALIDATION")
print("=" * 60)

# Rule 1: Age must be between 0 and 120
df_age_flagged = df_validation.withColumn(
    "age_valid",
    when((col("age") > 0) & (col("age") <= 120), 1).otherwise(0)
)

age_violations = df_age_flagged.filter(col("age_valid") == 0).count()
print(f"\n⚠️ Age violations: {age_violations}")
print("\nRecords with invalid age:")
display(df_age_flagged.filter(col("age_valid") == 0).select("customer_id", "customer_name", "age", "age_valid"))

# Rule 2: Transaction amount must be >= 0
df_amount_flagged = df_age_flagged.withColumn(
    "amount_valid",
    when(col("transaction_amount") >= 0, 1).otherwise(0)
)

amount_violations = df_amount_flagged.filter(col("amount_valid") == 0).count()
print(f"\n⚠️ Amount violations: {amount_violations}")
print("\nRecords with invalid amount:")
display(df_amount_flagged.filter(col("amount_valid") == 0).select("customer_id", "customer_name", "transaction_amount", "amount_valid"))

# Rule 3: Discount must be between 0 and 100
df_discount_flagged = df_amount_flagged.withColumn(
    "discount_valid",
    when((col("discount_pct") >= 0) & (col("discount_pct") <= 100), 1).otherwise(0)
)

discount_violations = df_discount_flagged.filter(col("discount_valid") == 0).count()
print(f"\n⚠️ Discount violations: {discount_violations}")

# Store flagged data
df_range_validated = df_discount_flagged

print("\n✅ Range validation complete!")
print(f"\n📊 Validation Summary:")
print(f"   Age violations: {age_violations}")
print(f"   Amount violations: {amount_violations}")
print(f"   Discount violations: {discount_violations}")

# COMMAND ----------

# DBTITLE 1,Format Validation
# Validation Type 2: FORMAT validation

print("\n📑 FORMAT VALIDATION")
print("=" * 60)

from pyspark.sql.functions import regexp_extract, lower, upper

# Rule 1: Email must contain '@' and '.'
df_format = df_range_validated.withColumn(
    "email_valid",
    when(
        (col("email").contains("@")) & 
        (col("email").contains(".")),
        1
    ).otherwise(0)
)

email_violations = df_format.filter(col("email_valid") == 0).count()
print(f"\n⚠️ Email format violations: {email_violations}")
print("\nRecords with invalid email:")
display(df_format.filter(col("email_valid") == 0).select("customer_id", "customer_name", "email", "email_valid"))

# Rule 2: Email should be lowercase (standardization)
df_format = df_format.withColumn(
    "email_standardized",
    lower(col("email"))
).withColumn(
    "email_case_valid",
    when(col("email") == col("email_standardized"), 1).otherwise(0)
)

email_case_violations = df_format.filter(col("email_case_valid") == 0).count()
print(f"\n🔤 Email case violations (not lowercase): {email_case_violations}")

# Rule 3: Customer name should not be empty or just spaces
df_format = df_format.withColumn(
    "name_valid",
    when(length(trim(col("customer_name"))) > 0, 1).otherwise(0)
)

name_violations = df_format.filter(col("name_valid") == 0).count()
print(f"\n⚠️ Name violations (empty/spaces): {name_violations}")

df_format_validated = df_format

print("\n✅ Format validation complete!")

# COMMAND ----------

# DBTITLE 1,Business Rule Validation
# Validation Type 3: BUSINESS RULE validation

print("\n💼 BUSINESS RULE VALIDATION")
print("=" * 60)

# Rule 1: start_date must be before end_date
df_business = df_format_validated.withColumn(
    "date_sequence_valid",
    when(col("start_date") < col("end_date"), 1).otherwise(0)
)

date_violations = df_business.filter(col("date_sequence_valid") == 0).count()
print(f"\n⚠️ Date sequence violations (start >= end): {date_violations}")
print("\nRecords with invalid date sequence:")
display(df_business.filter(col("date_sequence_valid") == 0).select(
    "customer_id", "customer_name", "start_date", "end_date", "date_sequence_valid"
))

# Rule 2: Premium tier must have transaction_amount >= 100
df_business = df_business.withColumn(
    "tier_amount_valid",
    when(
        (col("tier") == "Premium") & (col("transaction_amount") < 100),
        0
    ).otherwise(1)
)

tier_violations = df_business.filter(col("tier_amount_valid") == 0).count()
print(f"\n⚠️ Tier-Amount rule violations: {tier_violations}")
print("   (Premium tier requires amount >= $100)")
print("\nRecords violating tier rules:")
display(df_business.filter(col("tier_amount_valid") == 0).select(
    "customer_id", "customer_name", "tier", "transaction_amount", "tier_amount_valid"
))

# Rule 3: Tier must be in allowed list
allowed_tiers = ["Premium", "Standard"]
df_business = df_business.withColumn(
    "tier_valid",
    when(col("tier").isin(allowed_tiers), 1).otherwise(0)
)

tier_list_violations = df_business.filter(col("tier_valid") == 0).count()
print(f"\n⚠️ Invalid tier values: {tier_list_violations}")
print(f"   Allowed tiers: {allowed_tiers}")

df_business_validated = df_business

print("\n✅ Business rule validation complete!")

# COMMAND ----------

# DBTITLE 1,Comprehensive Validation Report
# Create comprehensive validation report

print("\n📄 COMPREHENSIVE VALIDATION REPORT")
print("=" * 60)

# Create overall validity flag
df_validated = df_business_validated.withColumn(
    "is_valid",
    when(
        (col("age_valid") == 1) &
        (col("amount_valid") == 1) &
        (col("discount_valid") == 1) &
        (col("email_valid") == 1) &
        (col("name_valid") == 1) &
        (col("date_sequence_valid") == 1) &
        (col("tier_amount_valid") == 1) &
        (col("tier_valid") == 1),
        1
    ).otherwise(0)
)

# Summary statistics
total_records = df_validated.count()
valid_records = df_validated.filter(col("is_valid") == 1).count()
invalid_records = total_records - valid_records
validity_rate = round((valid_records / total_records) * 100, 2)

print(f"\n📊 Validation Summary:")
print(f"   Total Records: {total_records}")
print(f"   ✅ Valid Records: {valid_records}")
print(f"   ❌ Invalid Records: {invalid_records}")
print(f"   📊 Validity Rate: {validity_rate}%")

# Detailed validation breakdown
print("\n\n🔍 Validation Rule Breakdown:")
print("| Rule | Violations | Pass Rate |")
print("| --- | --- | --- |")

rules = [
    ("Age Range (0-120)", "age_valid"),
    ("Amount >= 0", "amount_valid"),
    ("Discount 0-100%", "discount_valid"),
    ("Email Format", "email_valid"),
    ("Name Not Empty", "name_valid"),
    ("Start < End Date", "date_sequence_valid"),
    ("Premium Tier Amount", "tier_amount_valid"),
    ("Valid Tier", "tier_valid")
]

for rule_name, col_name in rules:
    violations = df_validated.filter(col(col_name) == 0).count()
    pass_rate = round(((total_records - violations) / total_records) * 100, 2)
    print(f"| {rule_name} | {violations} | {pass_rate}% |")

print("\n\n👀 Invalid Records:")
display(df_validated.filter(col("is_valid") == 0).select(
    "customer_id", "customer_name", "age", "transaction_amount", "email", "tier",
    "is_valid"
))

print("\n\n✅ Valid Records:")
display(df_validated.filter(col("is_valid") == 1).select(
    "customer_id", "customer_name", "age", "transaction_amount", "email", "tier"
))

# COMMAND ----------

# DBTITLE 1,Validation Actions: Filter, Flag, Reject
# Different approaches to handling validation failures

print("\n🎯 VALIDATION HANDLING STRATEGIES")
print("=" * 60)

# Approach 1: FILTER - Remove invalid records (clean dataset)
df_filtered = df_validated.filter(col("is_valid") == 1).select(
    "customer_id", "customer_name", "email_standardized", "age", 
    "transaction_amount", "start_date", "end_date", "tier", "discount_pct"
)

print(f"\n1️⃣ FILTER Approach:")
print(f"   Valid records only: {df_filtered.count()} records")
print(f"   ❌ Data loss: {invalid_records} records discarded")

# Approach 2: FLAG - Keep all records, mark validity
df_flagged = df_validated.select(
    "customer_id", "customer_name", "email", "age", "transaction_amount",
    "start_date", "end_date", "tier", "discount_pct", "is_valid"
)

print(f"\n2️⃣ FLAG Approach:")
print(f"   All records kept: {df_flagged.count()} records")
print(f"   ✅ No data loss, downstream can filter")

# Approach 3: REJECT - Separate valid and invalid (quarantine pattern)
df_valid_records = df_validated.filter(col("is_valid") == 1).select(
    "customer_id", "customer_name", "email_standardized", "age",
    "transaction_amount", "start_date", "end_date", "tier", "discount_pct"
)

df_rejected_records = df_validated.filter(col("is_valid") == 0).select(
    "customer_id", "customer_name", "email", "age", "transaction_amount",
    "start_date", "end_date", "tier", "discount_pct",
    "age_valid", "amount_valid", "email_valid", "tier_valid"
).withColumn("rejection_reason", expr(
    "CASE " +
    "WHEN age_valid = 0 THEN 'Invalid Age' " +
    "WHEN amount_valid = 0 THEN 'Invalid Amount' " +
    "WHEN email_valid = 0 THEN 'Invalid Email' " +
    "WHEN tier_valid = 0 THEN 'Invalid Tier' " +
    "ELSE 'Multiple Violations' END"
))

print(f"\n3️⃣ REJECT Approach (Quarantine):")
print(f"   Valid records: {df_valid_records.count()}")
print(f"   Rejected records: {df_rejected_records.count()}")
print(f"   ✅ Audit trail maintained")

print("\n👀 Rejected Records with Reasons:")
display(df_rejected_records)

print("\n\n📌 RECOMMENDATION:")
print("  ✅ Use REJECT approach in production")
print("  ✅ Write valid records to Silver layer")
print("  ✅ Write rejected records to Quarantine/Error table")
print("  ✅ Monitor rejection rates as data quality KPI")
print("  ✅ Alert on high rejection rates")

# COMMAND ----------

# DBTITLE 1,Section 3: Deduplication Strategies
# MAGIC %md
# MAGIC ## 🧹 Section 3: Deduplication Strategies
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC Imagine you have a toy box with two identical red cars. You only need one! **Deduplication** is like finding the duplicate car and removing it so you have just one of each toy.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Deduplication** is the process of identifying and removing duplicate records from a dataset.
# MAGIC
# MAGIC #### Why Duplicates Occur:
# MAGIC
# MAGIC 1. **Data Collection Issues**
# MAGIC    * Multiple submissions (users clicking "submit" twice)
# MAGIC    * System retries on failures
# MAGIC    * Data ingestion from multiple sources
# MAGIC
# MAGIC 2. **Integration Problems**
# MAGIC    * Merging datasets without proper keys
# MAGIC    * CDC (Change Data Capture) sending multiple updates
# MAGIC    * Event streaming duplicates (at-least-once delivery)
# MAGIC
# MAGIC 3. **Human Error**
# MAGIC    * Manual data entry
# MAGIC    * Copy-paste mistakes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Impact of Duplicates:
# MAGIC
# MAGIC * **Analytics**: Inflated metrics (double-counting revenue, users)
# MAGIC * **ML**: Biased models (duplicates create artificial patterns)
# MAGIC * **Operations**: Sending duplicate emails, processing same transaction twice
# MAGIC * **Storage**: Wasted space and increased costs
# MAGIC * **Compliance**: GDPR violations (keeping unnecessary copies of PII)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Deduplication Strategies:
# MAGIC
# MAGIC | Strategy | When to Use | Complexity |
# MAGIC | --- | --- | --- |
# MAGIC | **Exact Match** | All columns identical | Low |
# MAGIC | **Subset Match** | Key columns identical | Low |
# MAGIC | **Window-based** | Keep latest/best record | Medium |
# MAGIC | **Fuzzy Match** | Similar but not exact | High |
# MAGIC | **Composite Key** | Multi-column uniqueness | Medium |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Critical Question:
# MAGIC **Which duplicate to keep?**
# MAGIC
# MAGIC * Latest by timestamp?
# MAGIC * Highest quality score?
# MAGIC * First occurrence?
# MAGIC * Most complete record?
# MAGIC
# MAGIC This decision depends on your business requirements!

# COMMAND ----------

# DBTITLE 1,Create Sample Data with Duplicates
# Create sample data with various types of duplicates

data_with_duplicates = [
    # Exact duplicates
    (1, "Alice", "alice@email.com", 100.0, "2026-04-01", "Premium"),
    (1, "Alice", "alice@email.com", 100.0, "2026-04-01", "Premium"),  # Exact duplicate
    
    # Same ID, different data (newer record)
    (2, "Bob", "bob@email.com", 200.0, "2026-04-02", "Standard"),
    (2, "Bob", "bob_new@email.com", 250.0, "2026-04-05", "Premium"),  # Updated record
    
    # Same email, different ID (potential same person)
    (3, "Charlie", "charlie@email.com", 150.0, "2026-04-03", "Premium"),
    (4, "Charlie Brown", "charlie@email.com", 150.0, "2026-04-03", "Premium"),  # Same email
    
    # Similar but not exact
    (5, "David", "david@email.com", 300.0, "2026-04-04", "Premium"),
    (5, "Dave", "david@email.com", 300.0, "2026-04-04", "Premium"),  # Same ID, name variation
    
    # Unique records
    (6, "Eve", "eve@email.com", 175.0, "2026-04-06", "Standard"),
    (7, "Frank", "frank@email.com", 225.0, "2026-04-07", "Premium"),
    
    # Time-based duplicates (CDC scenario)
    (8, "Grace", "grace@email.com", 100.0, "2026-04-08", "Standard"),
    (8, "Grace", "grace@email.com", 150.0, "2026-04-09", "Standard"),  # Same ID, later date
    (8, "Grace", "grace@email.com", 200.0, "2026-04-10", "Premium"),  # Same ID, latest
]

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("transaction_amount", DoubleType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("tier", StringType(), True)
])

df_with_dupes = spark.createDataFrame(data_with_duplicates, schema)

print("\n📊 Raw Data with Duplicates:")
print(f"Total Records: {df_with_dupes.count()}")
display(df_with_dupes.orderBy("customer_id", "transaction_date"))

# COMMAND ----------

# DBTITLE 1,Detect Duplicates
# Strategy 1: DETECT duplicates

print("\n🔍 DUPLICATE DETECTION")
print("=" * 60)

# Method 1: Count total vs distinct records
total_records = df_with_dupes.count()
distinct_records = df_with_dupes.distinct().count()
duplicates = total_records - distinct_records

print(f"\n📊 Total Records: {total_records}")
print(f"📊 Distinct Records: {distinct_records}")
print(f"⚠️ Duplicate Records: {duplicates}")
print(f"⚠️ Duplication Rate: {round(duplicates/total_records*100, 2)}%")

# Method 2: Find duplicates based on specific columns
print("\n🔑 Checking duplicates by customer_id:")

duplicates_by_id = df_with_dupes.groupBy("customer_id").count().filter(col("count") > 1)
print(f"⚠️ Customer IDs with duplicates: {duplicates_by_id.count()}")
display(duplicates_by_id.orderBy(col("count").desc()))

# Method 3: Show actual duplicate records
print("\n👀 Showing duplicate customer records:")

# Convert to list without using RDD (serverless compatible)
duplicate_ids = [row["customer_id"] for row in duplicates_by_id.select("customer_id").collect()]
df_duplicate_records = df_with_dupes.filter(col("customer_id").isin(duplicate_ids))

display(df_duplicate_records.orderBy("customer_id", "transaction_date"))

print("\n✅ Duplicate detection complete!")

# COMMAND ----------

# DBTITLE 1,Deduplicate: Simple Strategy
# Strategy 2: SIMPLE deduplication using dropDuplicates()

print("\n🧹 DEDUPLICATION: SIMPLE STRATEGY")
print("=" * 60)

# Method 1: Remove ALL exact duplicates
df_dedup_all = df_with_dupes.dropDuplicates()
print(f"\n1️⃣ Remove exact duplicates:")
print(f"   Before: {df_with_dupes.count()} → After: {df_dedup_all.count()} (removed {df_with_dupes.count() - df_dedup_all.count()})")

# Method 2: Remove duplicates based on SUBSET of columns (customer_id only)
df_dedup_id = df_with_dupes.dropDuplicates(["customer_id"])
print(f"\n2️⃣ Remove duplicates by customer_id:")
print(f"   Before: {df_with_dupes.count()} → After: {df_dedup_id.count()} (removed {df_with_dupes.count() - df_dedup_id.count()})")

print("\n📊 Deduplicated data (by customer_id):")
display(df_dedup_id.orderBy("customer_id"))

# Method 3: Remove duplicates based on MULTIPLE columns
df_dedup_composite = df_with_dupes.dropDuplicates(["customer_id", "email"])
print(f"\n3️⃣ Remove duplicates by composite key (customer_id + email):")
print(f"   Before: {df_with_dupes.count()} → After: {df_dedup_composite.count()}")

print("\n⚠️ WARNING: dropDuplicates() keeps arbitrary record when duplicates found!")
print("👉 For controlled deduplication, use window functions (next cell)")

# COMMAND ----------

# DBTITLE 1,Deduplicate: Window-Based Strategy (Keep Latest)
# Strategy 3: WINDOW-BASED deduplication (keep latest record)

print("\n🕹️ DEDUPLICATION: WINDOW-BASED STRATEGY")
print("=" * 60)

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank

# Window specification: Partition by customer_id, order by transaction_date descending
window_spec = Window.partitionBy("customer_id").orderBy(col("transaction_date").desc())

# Add row number to identify the latest record
df_with_row_num = df_with_dupes.withColumn(
    "row_num",
    row_number().over(window_spec)
)

print("\n📊 Data with row numbers (showing ranking):")
display(df_with_row_num.orderBy("customer_id", "row_num"))

# Keep only the latest record (row_num = 1)
df_dedup_latest = df_with_row_num.filter(col("row_num") == 1).drop("row_num")

print(f"\n✅ Deduplication complete!")
print(f"   Before: {df_with_dupes.count()} → After: {df_dedup_latest.count()}")
print("\n📊 Final deduplicated data (keeping latest by date):")
display(df_dedup_latest.orderBy("customer_id"))

print("\n💡 Key Insight: Window functions give you CONTROL over which record to keep!")

# COMMAND ----------

# DBTITLE 1,Deduplicate: Advanced Strategy (Keep Best Record)
# Strategy 4: ADVANCED deduplication (keep "best" record based on multiple criteria)

print("\n🎯 DEDUPLICATION: ADVANCED STRATEGY")
print("=" * 60)

# Scenario: Keep the record with:
# 1. Latest date (primary)
# 2. Highest transaction amount (secondary)
# 3. Premium tier preferred (tertiary)

# Create a quality score
df_scored = df_with_dupes.withColumn(
    "quality_score",
    # Higher score = better record
    (when(col("tier") == "Premium", 1000).otherwise(0) +  # Premium tier bonus
     col("transaction_amount"))  # Amount as tiebreaker
)

# Window: partition by customer_id, order by date DESC and quality_score DESC
window_best = Window.partitionBy("customer_id").orderBy(
    col("transaction_date").desc(),
    col("quality_score").desc()
)

df_with_rank = df_scored.withColumn(
    "rank",
    row_number().over(window_best)
)

print("\n📊 Data with quality scoring and ranking:")
display(df_with_rank.orderBy("customer_id", "rank"))

# Keep the best record
df_dedup_best = df_with_rank.filter(col("rank") == 1).drop("quality_score", "rank")

print(f"\n✅ Advanced deduplication complete!")
print(f"   Before: {df_with_dupes.count()} → After: {df_dedup_best.count()}")
print("\n📊 Final deduplicated data (keeping 'best' record):")
display(df_dedup_best.orderBy("customer_id"))

print("\n💡 Pro Tip: Define quality score based on your business rules!")
print("   Examples: completeness, recency, source reliability, data freshness")

# COMMAND ----------

# DBTITLE 1,Deduplication Summary
# Summary comparison of deduplication strategies

print("\n📊 DEDUPLICATION STRATEGIES COMPARISON")
print("=" * 60)

strategies = [
    ("Original Data", df_with_dupes.count()),
    ("dropDuplicates() - All columns", df_dedup_all.count()),
    ("dropDuplicates() - customer_id", df_dedup_id.count()),
    ("Window - Keep Latest", df_dedup_latest.count()),
    ("Window - Keep Best", df_dedup_best.count())
]

print("\n| Strategy | Record Count | Records Removed |")
print("| --- | --- | --- |")
for strategy, count in strategies:
    removed = df_with_dupes.count() - count
    print(f"| {strategy} | {count} | {removed} |")

print("\n\n📌 RECOMMENDATIONS:")
print("=" * 60)
print("✅ Use dropDuplicates() for simple exact match deduplication")
print("✅ Use Window functions when you need to control WHICH record to keep")
print("✅ Consider business rules: latest? most complete? highest value?")
print("✅ Document your deduplication logic for data lineage")
print("✅ Monitor duplicate rates as a data quality metric")

print("\n⚠️ Common Mistakes:")
print("  ❌ Not specifying which duplicate to keep")
print("  ❌ Deduplicating too early (before validation)")
print("  ❌ Not tracking removed duplicates for audit")
print("  ❌ Using wrong key columns for deduplication")

# COMMAND ----------

# DBTITLE 1,Section 2: Null Checks
# MAGIC %md
# MAGIC ## 🔍 Section 2: Null Checks & Missing Data
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC Imagine your teacher asks everyone's favorite color. Some kids don't answer. Those empty answers are like **NULL values** — missing information that we need to handle.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **NULL** represents the absence of a value. In data engineering, nulls can occur due to:
# MAGIC
# MAGIC * Data collection failures
# MAGIC * System errors
# MAGIC * User opt-outs (privacy)
# MAGIC * Data integration mismatches
# MAGIC * ETL pipeline bugs
# MAGIC
# MAGIC #### Null Handling Strategies:
# MAGIC
# MAGIC 1. **Detection**: Identify null values and their distribution
# MAGIC 2. **Drop**: Remove rows/columns with nulls (use cautiously)
# MAGIC 3. **Imputation**: Replace nulls with defaults (mean, median, mode, forward-fill)
# MAGIC 4. **Flagging**: Keep nulls but add indicator columns
# MAGIC 5. **Validation**: Enforce NOT NULL constraints in schema
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Important Distinction:
# MAGIC
# MAGIC * **NULL**: Absence of value (unknown)
# MAGIC * **Empty String**: `""` is a value (known to be empty)
# MAGIC * **NaN**: Not a Number (mathematical undefined)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Impact of Nulls:
# MAGIC
# MAGIC * Aggregations can produce incorrect results
# MAGIC * Joins may fail or produce unexpected results
# MAGIC * ML models may crash or produce biased predictions
# MAGIC * Business logic may break

# COMMAND ----------

# DBTITLE 1,Create Sample Data with Nulls
# Create sample customer transaction data with quality issues
from datetime import datetime, timedelta

data_with_nulls = [
    (1, "Alice", "alice@email.com", 100.0, "2026-04-01", "Premium"),
    (2, "Bob", None, 200.0, "2026-04-02", "Standard"),  # Missing email
    (3, "Charlie", "charlie@email.com", None, "2026-04-03", "Premium"),  # Missing amount
    (4, None, "dave@email.com", 150.0, None, "Standard"),  # Missing name and date
    (5, "Eve", "eve@email.com", 300.0, "2026-04-05", None),  # Missing tier
    (6, "Frank", "frank@email.com", 0.0, "2026-04-06", "Premium"),
    (7, "Grace", "", 250.0, "2026-04-07", "Standard"),  # Empty string email
    (8, "Henry", "henry@email.com", -50.0, "2026-04-08", "Premium"),  # Negative amount
    (9, "Iris", "iris@email.com", 400.0, "2026-04-09", "Premium"),
    (10, "Jack", "jack@email.com", 175.0, "2026-04-10", "Standard")
]

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("transaction_amount", DoubleType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("tier", StringType(), True)
])

df_raw = spark.createDataFrame(data_with_nulls, schema)

print("\n📊 Raw Data with Quality Issues:")
display(df_raw)

# COMMAND ----------

# DBTITLE 1,Detect Nulls: Count Missing Values
# Strategy 1: Detect and count nulls per column
from pyspark.sql.functions import col, count, when, isnan, isnull

print("\n🔍 NULL DETECTION REPORT")
print("=" * 60)

# Method 1: Count nulls per column
null_counts = df_raw.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in df_raw.columns
])

print("\n📊 Null Counts by Column:")
display(null_counts)

# Method 2: Calculate percentage of nulls
total_rows = df_raw.count()

null_percentages = df_raw.select([
    (_round((count(when(col(c).isNull(), c)) / total_rows * 100), 2)).alias(f"{c}_null_pct")
    for c in df_raw.columns
])

print(f"\n📊 Null Percentages (Total Rows: {total_rows}):")
display(null_percentages)

# Method 3: Rows with ANY null
rows_with_nulls = df_raw.filter(
    col("customer_id").isNull() | 
    col("customer_name").isNull() | 
    col("email").isNull() | 
    col("transaction_amount").isNull() | 
    col("transaction_date").isNull() | 
    col("tier").isNull()
).count()

print(f"\n⚠️ Rows with at least one NULL: {rows_with_nulls}/{total_rows}")
print(f"⚠️ Percentage of impacted rows: {round(rows_with_nulls/total_rows*100, 2)}%")

# COMMAND ----------

# DBTITLE 1,Handle Nulls: Drop Strategy
# Strategy 2: DROP rows with nulls

print("\n🗑️ NULL HANDLING: DROP STRATEGY")
print("=" * 60)

# Drop rows where ANY column is null
df_drop_any = df_raw.dropna(how='any')
print(f"\n1️⃣ Drop ANY nulls: {df_raw.count()} → {df_drop_any.count()} rows (dropped {df_raw.count() - df_drop_any.count()})")

# Drop rows where ALL columns are null (less aggressive)
df_drop_all = df_raw.dropna(how='all')
print(f"2️⃣ Drop ALL nulls: {df_raw.count()} → {df_drop_all.count()} rows (dropped {df_raw.count() - df_drop_all.count()})")

# Drop rows where SPECIFIC columns are null (best practice)
df_drop_critical = df_raw.dropna(subset=['customer_id', 'transaction_amount'])
print(f"3️⃣ Drop critical column nulls: {df_raw.count()} → {df_drop_critical.count()} rows (dropped {df_raw.count() - df_drop_critical.count()})")

# Drop with threshold: keep rows with at least N non-null values
df_drop_threshold = df_raw.dropna(thresh=5)  # Keep rows with at least 5 non-null values
print(f"4️⃣ Drop with threshold (5+ non-nulls): {df_raw.count()} → {df_drop_threshold.count()} rows")

print("\n📊 Data after dropping critical nulls:")
display(df_drop_critical)

# COMMAND ----------

# DBTITLE 1,Handle Nulls: Replace Strategy
# Strategy 3: REPLACE nulls with default values

print("\n🔄 NULL HANDLING: REPLACE STRATEGY")
print("=" * 60)

# Method 1: fillna() - simple replacement
df_filled_simple = df_raw.fillna({
    'customer_name': 'Unknown',
    'email': 'noemail@example.com',
    'transaction_amount': 0.0,
    'transaction_date': '1900-01-01',
    'tier': 'Standard'
})

print("\n📊 Data after filling nulls with defaults:")
display(df_filled_simple)

# Method 2: Using coalesce() - more flexible
df_filled_coalesce = df_raw.select(
    col("customer_id"),
    coalesce(col("customer_name"), lit("Unknown")).alias("customer_name"),
    coalesce(col("email"), lit("noemail@example.com")).alias("email"),
    coalesce(col("transaction_amount"), lit(0.0)).alias("transaction_amount"),
    coalesce(col("transaction_date"), lit("1900-01-01")).alias("transaction_date"),
    coalesce(col("tier"), lit("Standard")).alias("tier")
)

print("\n✅ Nulls replaced successfully using coalesce()")

# Method 3: Statistical imputation (for numerical columns)
from pyspark.sql.functions import mean, median, stddev

# Calculate mean for imputation
mean_amount = df_raw.select(mean("transaction_amount")).collect()[0][0]

df_filled_mean = df_raw.fillna({'transaction_amount': mean_amount})

print(f"\n📊 Mean transaction_amount: ${mean_amount:.2f}")
print("\n✅ Nulls replaced with statistical mean")

# COMMAND ----------

# DBTITLE 1,Handle Nulls: Flag Strategy
# Strategy 4: FLAG nulls with indicator columns

print("\n🏴 NULL HANDLING: FLAG STRATEGY")
print("=" * 60)

df_flagged = df_raw.select(
    "*",
    when(col("customer_name").isNull(), 1).otherwise(0).alias("name_is_missing"),
    when(col("email").isNull(), 1).otherwise(0).alias("email_is_missing"),
    when(col("transaction_amount").isNull(), 1).otherwise(0).alias("amount_is_missing"),
    when(col("transaction_date").isNull(), 1).otherwise(0).alias("date_is_missing"),
    when(col("tier").isNull(), 1).otherwise(0).alias("tier_is_missing")
)

print("\n📊 Data with null indicator flags:")
display(df_flagged)

# Now fill the nulls while keeping the flags
df_flagged_filled = df_flagged.fillna({
    'customer_name': 'Unknown',
    'email': 'noemail@example.com',
    'transaction_amount': 0.0,
    'transaction_date': '1900-01-01',
    'tier': 'Standard'
})

print("\n✅ Best Practice: Flags preserved, nulls filled")
print("\n👉 This allows downstream analytics to track data quality issues")
display(df_flagged_filled)

# COMMAND ----------

# DBTITLE 1,Null Check Summary Statistics
# Summary: Null analysis statistics

print("\n📊 NULL ANALYSIS SUMMARY")
print("=" * 60)

# Create comprehensive null report
from functools import reduce
from pyspark.sql import DataFrame

null_report_data = []
for c in df_raw.columns:
    null_report_data.append((
        c,  # column_name
        str(df_raw.select(c).dtypes[0][1]),  # data_type
        df_raw.select(col(c)).filter(col(c).isNotNull()).count(),  # non_null_count
        df_raw.select(col(c)).filter(col(c).isNull()).count(),  # null_count
        round((df_raw.select(col(c)).filter(col(c).isNull()).count() / df_raw.count() * 100), 2)  # null_percentage
    ))

null_summary = spark.createDataFrame(null_report_data, 
    ["column_name", "data_type", "non_null_count", "null_count", "null_percentage"])

print("\n📄 Comprehensive Null Report:")
display(null_summary.orderBy(col("null_percentage").desc()))

print("\n✅ Null analysis complete!")
print("\n💡 Key Takeaways:")
print("  1. Always detect before handling")
print("  2. Document your null handling strategy")
print("  3. Consider business context when choosing strategy")
print("  4. Flag approach preserves data quality lineage")

# COMMAND ----------

# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # 🏗 Data Engineering Training — Phase 6 Day 32  
# MAGIC ## 🛡️ Data Quality: Validation, Deduplication & Rules  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC * Data Quality Fundamentals  
# MAGIC * Null Checks & Data Validation  
# MAGIC * Deduplication Strategies  
# MAGIC * Validation Rules & Constraints (SDP Integration)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### ⚙️ Platform: Databricks (Serverless + Unity Catalog + Delta Lake + SDP)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Objective:
# MAGIC Learn how to implement enterprise-grade data quality checks including null validation, deduplication, and rule-based validation within Delta pipelines and SDP.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚨 Engineering Constraints:
# MAGIC * ✅ Databricks Serverless Compute
# MAGIC * ✅ DataFrame API only (NO RDDs)
# MAGIC * ✅ NO cache() / persist()
# MAGIC * ✅ NO /tmp or local storage
# MAGIC * ✅ Unity Catalog managed tables
# MAGIC * ✅ Medallion + Data Quality-first design

# COMMAND ----------

# DBTITLE 1,Section 1: Data Quality Fundamentals
# MAGIC %md
# MAGIC ## 📖 Section 1: Data Quality Fundamentals
# MAGIC
# MAGIC ### 🧒 ELI5 (Explain Like I'm 5):
# MAGIC Imagine you're building a LEGO castle. If some pieces are broken, missing, or duplicates, your castle won't look right. **Data Quality** is like making sure all your LEGO pieces are clean, complete, and the right ones before you build.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Data Quality** refers to the condition of data based on factors like accuracy, completeness, consistency, reliability, and timeliness.
# MAGIC
# MAGIC #### Why Data Quality is Critical:
# MAGIC
# MAGIC 1. **Analytics Impact**
# MAGIC    * Poor quality data leads to incorrect insights
# MAGIC    * "Garbage In, Garbage Out" (GIGO)
# MAGIC    * Business decisions based on flawed data can be catastrophic
# MAGIC
# MAGIC 2. **ML/AI Impact**
# MAGIC    * Models trained on bad data produce unreliable predictions
# MAGIC    * Biased or incomplete data leads to biased models
# MAGIC    * Data quality directly affects model accuracy
# MAGIC
# MAGIC 3. **Business Impact**
# MAGIC    * Lost revenue from incorrect insights
# MAGIC    * Compliance violations (GDPR, SOX, HIPAA)
# MAGIC    * Reputation damage
# MAGIC    * Operational inefficiencies
# MAGIC
# MAGIC #### Key Data Quality Dimensions:
# MAGIC
# MAGIC | Dimension | Description | Example |
# MAGIC | --- | --- | --- |
# MAGIC | **Accuracy** | Data correctly represents reality | Email format is valid |
# MAGIC | **Completeness** | No missing values | All required fields populated |
# MAGIC | **Consistency** | Data is uniform across systems | Same customer_id format everywhere |
# MAGIC | **Validity** | Data conforms to business rules | Age > 0, Amount >= 0 |
# MAGIC | **Uniqueness** | No duplicate records | One record per customer |
# MAGIC | **Timeliness** | Data is current and available when needed | Real-time fraud detection |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Data Quality in Modern Data Platforms:
# MAGIC
# MAGIC * **Bronze Layer**: Minimal validation (capture raw data)
# MAGIC * **Silver Layer**: Strong validation (cleaned, deduplicated)
# MAGIC * **Gold Layer**: Business-ready (aggregated, trusted)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚨 Cost of Poor Data Quality:
# MAGIC
# MAGIC * IBM estimates poor data quality costs US businesses ~$3.1 trillion annually
# MAGIC * Gartner reports organizations believe poor data quality impacts 40% of business initiatives

# COMMAND ----------

# DBTITLE 1,Setup: Import Libraries
# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, when, isnan, isnull, lit, sum as _sum,
    row_number, rank, dense_rank, countDistinct,
    datediff, current_date, year, month, dayofmonth,
    regexp_extract, length, trim, upper, lower,
    coalesce, expr, round as _round
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
from delta.tables import DeltaTable

print("✅ Libraries imported successfully")
print(f"✅ Spark Version: {spark.version}")
print("✅ Ready for Data Quality operations")