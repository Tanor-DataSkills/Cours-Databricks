# Databricks notebook source
# DBTITLE 1,Notebook Complete
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC # ✅ Notebook Complete!
# MAGIC
# MAGIC ## 🎓 Congratulations!
# MAGIC
# MAGIC You've completed **Phase 4 Day 21: Delta Lake Versioning, Time Travel & Data Recovery**
# MAGIC
# MAGIC ### 📦 What You've Learned:
# MAGIC
# MAGIC * ✅ Delta Lake versioning fundamentals
# MAGIC * ✅ Time travel (version and timestamp-based)
# MAGIC * ✅ Data recovery techniques
# MAGIC * ✅ Transaction history analysis
# MAGIC * ✅ End-to-end recovery scenarios
# MAGIC * ✅ Best practices for production
# MAGIC * ✅ VACUUM impact on versioning
# MAGIC * ✅ Audit pipeline concepts
# MAGIC
# MAGIC ### 🚀 Next Steps:
# MAGIC
# MAGIC 1. Practice time travel queries on your own data
# MAGIC 2. Implement recovery procedures for critical tables
# MAGIC 3. Set up appropriate retention policies
# MAGIC 4. Build audit trails for compliance
# MAGIC 5. Test recovery processes regularly
# MAGIC
# MAGIC ### 📚 Resources:
# MAGIC
# MAGIC * [Delta Lake Documentation](https://docs.delta.io/)
# MAGIC * [Databricks Delta Lake Guide](https://docs.databricks.com/delta/index.html)
# MAGIC * [Time Travel Best Practices](https://docs.databricks.com/delta/history.html)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏷️ **@TRRaveendra**
# MAGIC ### 👨‍💻 Author: TRRaveendra
# MAGIC ### 📌 Platform: Databricks | Delta Lake | Unity Catalog
# MAGIC ### 📅 Created: April 21, 2026
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Happy Learning! 🚀**

# COMMAND ----------

# DBTITLE 1,Optional: VACUUM Impact
# MAGIC %md
# MAGIC ## 🗑️ OPTIONAL ADD-ON: VACUUM Impact on Versioning
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC VACUUM is like cleaning out old photos from your storage. You keep the list of what photos existed (the journal), but the actual old photos get deleted to save space. You can still see what happened, but you can't look at the old photos anymore.
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **What is VACUUM?**
# MAGIC
# MAGIC VACUUM removes data files that are no longer referenced by the current table state and are older than the retention period.
# MAGIC
# MAGIC **Impact on Time Travel:**
# MAGIC
# MAGIC * **Transaction Log Preserved**: Commit files remain, metadata is intact
# MAGIC * **Data Files Removed**: Actual data files (Parquet) are deleted
# MAGIC * **Time Travel Limited**: Cannot query versions older than retention period
# MAGIC * **History Available**: `DESCRIBE HISTORY` still shows all versions
# MAGIC
# MAGIC **Default Behavior:**
# MAGIC
# MAGIC * Default retention: **7 days** (168 hours)
# MAGIC * Safety check: Prevents deletion if retention < 7 days
# MAGIC * Can override with: `SET spark.databricks.delta.retentionDurationCheck.enabled = false`
# MAGIC
# MAGIC **Syntax:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Standard VACUUM (7 day retention)
# MAGIC VACUUM table_name
# MAGIC
# MAGIC -- Custom retention
# MAGIC VACUUM table_name RETAIN 240 HOURS  -- 10 days
# MAGIC
# MAGIC -- Dry run (see what would be deleted)
# MAGIC VACUUM table_name DRY RUN
# MAGIC ```
# MAGIC
# MAGIC **Best Practices:**
# MAGIC
# MAGIC 1. **Plan Retention Based on Requirements:**
# MAGIC    * Audit/Compliance needs
# MAGIC    * Recovery time objectives (RTO)
# MAGIC    * Storage budget
# MAGIC
# MAGIC 2. **Set Table-Level Retention:**
# MAGIC ```sql
# MAGIC ALTER TABLE critical_table
# MAGIC SET TBLPROPERTIES (
# MAGIC   delta.deletedFileRetentionDuration = 'interval 30 days',
# MAGIC   delta.logRetentionDuration = 'interval 90 days'
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC 3. **Regular VACUUM Schedule:**
# MAGIC    * Weekly for active tables
# MAGIC    * Monthly for archived tables
# MAGIC    * After major data changes
# MAGIC
# MAGIC 4. **Monitor Storage:**
# MAGIC    * Track storage growth
# MAGIC    * Balance costs vs recovery needs
# MAGIC    * Adjust retention policies as needed

# COMMAND ----------

# DBTITLE 1,VACUUM Impact Demo
# Demonstrate VACUUM impact (conceptual - not executing actual VACUUM)
print("📊 VACUUM Impact Demonstration (Conceptual)")
print("="*80)

# Show current history
history_df = spark.sql(f"DESCRIBE HISTORY {catalog_name}.{schema_name}.{feedback_table}")

print("\nCurrent version history:")
display(history_df.select("version", "timestamp", "operation"))

print("\n💡 Important Points:")
print("  1. VACUUM removes old DATA files (Parquet files)")
print("  2. Transaction LOG files (_delta_log/*.json) remain")
print("  3. DESCRIBE HISTORY still shows all versions")
print("  4. Time travel queries fail if data files are removed")
print("\n⚠️ After VACUUM with 7-day retention:")
print("  - Versions older than 7 days: Metadata available, data unavailable")
print("  - Versions within 7 days: Fully accessible")
print("\n✅ Recommendation: Set retention based on recovery requirements")

# COMMAND ----------

# DBTITLE 1,Optional: Audit Pipeline
# MAGIC %md
# MAGIC ## 📊 OPTIONAL ADD-ON: Audit Pipeline Example (Conceptual)
# MAGIC
# MAGIC ### 🎯 Objective:
# MAGIC Build an automated audit pipeline that tracks and reports on data changes.
# MAGIC
# MAGIC ### 📝 Audit Pipeline Architecture:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────┐
# MAGIC │  Source Tables  │
# MAGIC │  (Delta Lake)   │
# MAGIC └────────┬────────┘
# MAGIC          │
# MAGIC          │ DESCRIBE HISTORY
# MAGIC          │
# MAGIC          ↓
# MAGIC ┌────────┴────────┐
# MAGIC │  Audit Capture  │
# MAGIC │  (Extract logs) │
# MAGIC └────────┬────────┘
# MAGIC          │
# MAGIC          ↓
# MAGIC ┌────────┴────────┐
# MAGIC │ Audit Storage  │
# MAGIC │ (Audit tables) │
# MAGIC └────────┬────────┘
# MAGIC          │
# MAGIC          ↓
# MAGIC ┌────────┴────────┐
# MAGIC │  Reporting &    │
# MAGIC │  Alerting       │
# MAGIC └─────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### 🛠️ Implementation Components:
# MAGIC
# MAGIC **1. Audit Capture Job (Scheduled):**
# MAGIC ```python
# MAGIC # Run daily to capture history
# MAGIC for table in critical_tables:
# MAGIC     history_df = spark.sql(f"DESCRIBE HISTORY {table}")
# MAGIC     history_df.write.format("delta").mode("append").saveAsTable("audit.table_history")
# MAGIC ```
# MAGIC
# MAGIC **2. Change Detection:**
# MAGIC ```sql
# MAGIC -- Identify unusual operations
# MAGIC SELECT 
# MAGIC     table_name,
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     userName,
# MAGIC     operationMetrics.numRemovedRows as rows_deleted
# MAGIC FROM audit.table_history
# MAGIC WHERE operation = 'DELETE'
# MAGIC   AND operationMetrics.numRemovedRows > 1000
# MAGIC   AND timestamp >= current_date() - 1
# MAGIC ```
# MAGIC
# MAGIC **3. Compliance Reports:**
# MAGIC ```sql
# MAGIC -- Weekly data change summary
# MAGIC SELECT 
# MAGIC     table_name,
# MAGIC     COUNT(DISTINCT version) as total_versions,
# MAGIC     COUNT(DISTINCT userName) as unique_users,
# MAGIC     SUM(CASE WHEN operation = 'DELETE' THEN 1 ELSE 0 END) as delete_ops,
# MAGIC     MIN(timestamp) as first_change,
# MAGIC     MAX(timestamp) as last_change
# MAGIC FROM audit.table_history
# MAGIC WHERE timestamp >= current_date() - 7
# MAGIC GROUP BY table_name
# MAGIC ```
# MAGIC
# MAGIC **4. Alerts:**
# MAGIC * Large-scale deletions
# MAGIC * Unauthorized users
# MAGIC * Schema changes
# MAGIC * After-hours modifications
# MAGIC
# MAGIC ### 📊 Audit Metrics to Track:
# MAGIC
# MAGIC * **Volume Metrics**: Rows added/removed/updated
# MAGIC * **Operation Metrics**: Types of operations
# MAGIC * **User Metrics**: Who is changing data
# MAGIC * **Time Metrics**: When changes occur
# MAGIC * **Performance Metrics**: Operation duration
# MAGIC
# MAGIC ### ✅ Benefits:
# MAGIC
# MAGIC * **Compliance**: Meet regulatory requirements
# MAGIC * **Security**: Detect unauthorized access
# MAGIC * **Quality**: Identify data quality issues
# MAGIC * **Debugging**: Trace issues to specific changes
# MAGIC * **Accountability**: Track user actions

# COMMAND ----------

# DBTITLE 1,Final Summary
# MAGIC %md
# MAGIC ## 🎯 FINAL SUMMARY
# MAGIC
# MAGIC ### 📚 Key Learnings:
# MAGIC
# MAGIC 1. **Delta Lake Versioning**
# MAGIC    * Every write creates a new immutable version
# MAGIC    * Transaction log tracks all changes in `_delta_log/`
# MAGIC    * Versions are sequential integers starting from 0
# MAGIC
# MAGIC 2. **Time Travel Capabilities**
# MAGIC    * Version-based: `VERSION AS OF <number>`
# MAGIC    * Timestamp-based: `TIMESTAMP AS OF '<timestamp>'`
# MAGIC    * Works for SELECT queries and table creation
# MAGIC
# MAGIC 3. **Data Recovery**
# MAGIC    * Restore from any historical version
# MAGIC    * Multiple strategies: create new table, overwrite, or selective merge
# MAGIC    * Critical for disaster recovery and rollback scenarios
# MAGIC
# MAGIC 4. **Transaction History**
# MAGIC    * `DESCRIBE HISTORY` provides complete audit trail
# MAGIC    * Tracks operation type, user, timestamp, and metrics
# MAGIC    * Essential for compliance and debugging
# MAGIC
# MAGIC 5. **Best Practices**
# MAGIC    * Always use Delta format for production tables
# MAGIC    * Plan retention periods based on recovery needs
# MAGIC    * Monitor storage costs vs recovery requirements
# MAGIC    * Document recovery procedures
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏆 Production Readiness:
# MAGIC
# MAGIC ✅ **You can now:**
# MAGIC * Query historical data states
# MAGIC * Implement reliable data recovery
# MAGIC * Audit data changes
# MAGIC * Debug data quality issues
# MAGIC * Build versioned data pipelines
# MAGIC * Meet compliance requirements

# COMMAND ----------

# DBTITLE 1,Interview Questions
# MAGIC %md
# MAGIC ## 📝 Interview Questions (10)
# MAGIC
# MAGIC ### Basic Level:
# MAGIC
# MAGIC **Q1:** What is Delta Lake versioning?
# MAGIC
# MAGIC **A:** Delta Lake versioning is a feature where every write operation (INSERT, UPDATE, DELETE, MERGE) creates a new version of the table. Each version is tracked through the Delta transaction log, enabling time travel and data recovery.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q2:** How do you query a specific version of a Delta table?
# MAGIC
# MAGIC **A:** Using `VERSION AS OF` syntax:
# MAGIC ```sql
# MAGIC SELECT * FROM table_name VERSION AS OF 5
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q3:** What is the difference between version-based and timestamp-based time travel?
# MAGIC
# MAGIC **A:** 
# MAGIC * Version-based: Query specific version number (`VERSION AS OF 5`)
# MAGIC * Timestamp-based: Query data as it existed at a specific time (`TIMESTAMP AS OF '2024-01-15'`)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Intermediate Level:
# MAGIC
# MAGIC **Q4:** How does Delta Lake store version information?
# MAGIC
# MAGIC **A:** Delta stores version information in the `_delta_log/` directory. Each version is represented by a JSON commit file (e.g., `00000000000000000005.json`). These files contain metadata about transactions including added/removed files, operation details, and schema changes.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q5:** How would you recover data from an accidental deletion?
# MAGIC
# MAGIC **A:** 
# MAGIC 1. Use `DESCRIBE HISTORY` to identify the last good version
# MAGIC 2. Query that version using time travel
# MAGIC 3. Restore using:
# MAGIC ```sql
# MAGIC INSERT OVERWRITE TABLE current_table
# MAGIC SELECT * FROM current_table VERSION AS OF <last_good_version>
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q6:** What is the impact of VACUUM on time travel?
# MAGIC
# MAGIC **A:** VACUUM removes old data files that are no longer referenced. After VACUUM, you cannot time travel to versions older than the retention period (default 7 days). The commit files remain, but the actual data is gone.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Advanced Level:
# MAGIC
# MAGIC **Q7:** How would you implement an audit trail using Delta versioning?
# MAGIC
# MAGIC **A:** Use `DESCRIBE HISTORY` to track:
# MAGIC * All operations and their timestamps
# MAGIC * Users who made changes (userName column)
# MAGIC * Operation metrics (rows affected, files changed)
# MAGIC * Create scheduled jobs to export history to audit tables
# MAGIC * Set appropriate retention periods for compliance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q8:** Explain the relationship between Delta transaction log and ACID properties.
# MAGIC
# MAGIC **A:** The transaction log ensures ACID:
# MAGIC * **Atomicity**: Commit file is written atomically
# MAGIC * **Consistency**: Each version represents valid state
# MAGIC * **Isolation**: Optimistic concurrency control via log
# MAGIC * **Durability**: Commit files are immutable and persisted
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q9:** How would you optimize storage costs while maintaining recovery capabilities?
# MAGIC
# MAGIC **A:** 
# MAGIC * Set appropriate retention periods per table criticality
# MAGIC * Critical tables: 30+ days
# MAGIC * Standard tables: 7 days (default)
# MAGIC * Run VACUUM regularly on non-critical tables
# MAGIC * Use table properties:
# MAGIC ```sql
# MAGIC ALTER TABLE table_name 
# MAGIC SET TBLPROPERTIES (delta.deletedFileRetentionDuration = 'interval 14 days')
# MAGIC ```
# MAGIC * Monitor storage usage and adjust policies
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q10:** Design a disaster recovery strategy using Delta versioning.
# MAGIC
# MAGIC **A:** 
# MAGIC 1. **Prevention**:
# MAGIC    * Use Delta format for all production tables
# MAGIC    * Set retention based on RTO/RPO requirements
# MAGIC    * Implement access controls
# MAGIC
# MAGIC 2. **Detection**:
# MAGIC    * Monitor for unexpected operations
# MAGIC    * Alert on large-scale deletions/updates
# MAGIC    * Regular data quality checks
# MAGIC
# MAGIC 3. **Recovery**:
# MAGIC    * Document recovery procedures
# MAGIC    * Test recovery process regularly
# MAGIC    * Use time travel for point-in-time recovery
# MAGIC    * Validate data post-recovery
# MAGIC
# MAGIC 4. **Documentation**:
# MAGIC    * Maintain version history logs
# MAGIC    * Document critical version milestones
# MAGIC    * Track recovery incidents

# COMMAND ----------

# DBTITLE 1,Common Mistakes
# MAGIC %md
# MAGIC ## ⚠️ Common Mistakes
# MAGIC
# MAGIC ### 1️⃣ **Not Using Time Travel**
# MAGIC
# MAGIC **Mistake:**
# MAGIC ```python
# MAGIC # Creating manual backup tables
# MAGIC df.write.saveAsTable("table_backup_20240115")
# MAGIC df.write.saveAsTable("table_backup_20240116")
# MAGIC # ... managing many backup tables
# MAGIC ```
# MAGIC
# MAGIC **Correct:**
# MAGIC ```python
# MAGIC # Let Delta handle versioning
# MAGIC df.write.format("delta").mode("overwrite").saveAsTable("table_name")
# MAGIC # Use VERSION AS OF for any historical state
# MAGIC ```
# MAGIC
# MAGIC **Why it matters:** Eliminates backup table sprawl, reduces storage, simplifies recovery.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ **Overwriting Data Without Backup**
# MAGIC
# MAGIC **Mistake:**
# MAGIC ```python
# MAGIC # Using non-Delta format
# MAGIC df.write.format("parquet").mode("overwrite").save("/path/to/data")
# MAGIC # Previous data is gone forever!
# MAGIC ```
# MAGIC
# MAGIC **Correct:**
# MAGIC ```python
# MAGIC # Always use Delta for important data
# MAGIC df.write.format("delta").mode("overwrite").save("/path/to/data")
# MAGIC # Previous versions remain accessible
# MAGIC ```
# MAGIC
# MAGIC **Why it matters:** Delta's versioning provides automatic insurance against data loss.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ **Ignoring Version History**
# MAGIC
# MAGIC **Mistake:**
# MAGIC * Not checking history when debugging
# MAGIC * Assuming current state is always correct
# MAGIC * No audit trail review
# MAGIC
# MAGIC **Correct:**
# MAGIC ```sql
# MAGIC -- Always check history first
# MAGIC DESCRIBE HISTORY table_name
# MAGIC
# MAGIC -- Investigate suspicious changes
# MAGIC SELECT version, timestamp, operation, userName
# MAGIC FROM (DESCRIBE HISTORY table_name)
# MAGIC WHERE operation = 'DELETE'
# MAGIC ```
# MAGIC
# MAGIC **Why it matters:** History provides valuable debugging information and audit trails.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4️⃣ **Misunderstanding Delta Logs**
# MAGIC
# MAGIC **Mistake:**
# MAGIC * Manually modifying `_delta_log/` files
# MAGIC * Deleting transaction log files
# MAGIC * Not understanding checkpoint mechanism
# MAGIC
# MAGIC **Correct:**
# MAGIC * Never manually modify `_delta_log/`
# MAGIC * Let Delta manage transaction log automatically
# MAGIC * Understand checkpoints optimize reads, don't delete them
# MAGIC
# MAGIC **Why it matters:** Corrupting the transaction log can break time travel and table access.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5️⃣ **Aggressive VACUUM Without Planning**
# MAGIC
# MAGIC **Mistake:**
# MAGIC ```sql
# MAGIC -- Too aggressive - loses time travel capability
# MAGIC VACUUM table_name RETAIN 0 HOURS
# MAGIC ```
# MAGIC
# MAGIC **Correct:**
# MAGIC ```sql
# MAGIC -- Plan retention based on recovery needs
# MAGIC VACUUM table_name RETAIN 168 HOURS  -- 7 days
# MAGIC
# MAGIC -- For critical tables, longer retention
# MAGIC ALTER TABLE critical_table
# MAGIC SET TBLPROPERTIES (delta.deletedFileRetentionDuration = 'interval 30 days')
# MAGIC ```
# MAGIC
# MAGIC **Why it matters:** Balances storage costs with recovery capabilities.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6️⃣ **Not Testing Recovery Procedures**
# MAGIC
# MAGIC **Mistake:**
# MAGIC * Assuming recovery will work
# MAGIC * No documented recovery process
# MAGIC * First test during actual incident
# MAGIC
# MAGIC **Correct:**
# MAGIC * Regular recovery drills
# MAGIC * Documented step-by-step procedures
# MAGIC * Test on non-production data first
# MAGIC * Validate data integrity post-recovery
# MAGIC
# MAGIC **Why it matters:** Reduces recovery time and prevents errors during incidents.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 7️⃣ **Ignoring Storage Costs**
# MAGIC
# MAGIC **Mistake:**
# MAGIC * Setting infinite retention periods
# MAGIC * Never running VACUUM
# MAGIC * Not monitoring storage growth
# MAGIC
# MAGIC **Correct:**
# MAGIC * Balance retention vs cost
# MAGIC * Regular VACUUM on appropriate schedule
# MAGIC * Monitor storage metrics
# MAGIC * Different policies for different table criticalities
# MAGIC
# MAGIC **Why it matters:** Uncontrolled storage growth leads to unnecessary costs.

# COMMAND ----------

# DBTITLE 1,Best Practices
# MAGIC %md
# MAGIC ## ✅ Data Engineering Best Practices
# MAGIC
# MAGIC ### 1️⃣ Use Time Travel for Debugging
# MAGIC
# MAGIC **Why:**
# MAGIC * Quickly identify when issues were introduced
# MAGIC * Compare data states before and after changes
# MAGIC * No need for separate backup tables
# MAGIC
# MAGIC **How:**
# MAGIC ```sql
# MAGIC -- Compare current vs previous version
# MAGIC SELECT * FROM table_name VERSION AS OF 5
# MAGIC EXCEPT
# MAGIC SELECT * FROM table_name VERSION AS OF 6
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ Avoid Accidental Overwrites
# MAGIC
# MAGIC **Anti-Pattern:**
# MAGIC ```python
# MAGIC # Dangerous: overwrites without backup
# MAGIC df.write.mode("overwrite").saveAsTable("important_table")
# MAGIC ```
# MAGIC
# MAGIC **Best Practice:**
# MAGIC ```python
# MAGIC # Safer: Delta handles versioning automatically
# MAGIC df.write.format("delta").mode("overwrite").saveAsTable("important_table")
# MAGIC # Can always recover with VERSION AS OF
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ Track History for Audit
# MAGIC
# MAGIC **Audit Requirements:**
# MAGIC * Who made changes (userName in history)
# MAGIC * When changes occurred (timestamp)
# MAGIC * What changed (operationMetrics)
# MAGIC * Why changes were made (use table comments/tags)
# MAGIC
# MAGIC **Implementation:**
# MAGIC ```sql
# MAGIC DESCRIBE HISTORY table_name
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4️⃣ Use Versioning for Rollback
# MAGIC
# MAGIC **Recovery Strategy:**
# MAGIC 1. Identify last known good version
# MAGIC 2. Test recovery on copy first
# MAGIC 3. Validate data integrity
# MAGIC 4. Execute rollback
# MAGIC 5. Document incident
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5️⃣ Understand VACUUM Impact
# MAGIC
# MAGIC **Important:**
# MAGIC * VACUUM removes old data files (not immediately recoverable)
# MAGIC * Default retention: 7 days
# MAGIC * Time travel limited by retention period
# MAGIC * Plan retention based on audit/compliance needs
# MAGIC
# MAGIC ```sql
# MAGIC -- Set longer retention for critical tables
# MAGIC ALTER TABLE important_table 
# MAGIC SET TBLPROPERTIES (delta.deletedFileRetentionDuration = 'interval 30 days')
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6️⃣ Monitor Storage Costs
# MAGIC
# MAGIC **Balance:**
# MAGIC * Longer retention = better recovery options
# MAGIC * Longer retention = higher storage costs
# MAGIC * Regular VACUUM = cost optimization
# MAGIC
# MAGIC **Strategy:**
# MAGIC * Critical tables: 30+ days retention
# MAGIC * Standard tables: 7 days (default)
# MAGIC * Temp/staging tables: 0-1 days

# COMMAND ----------

# DBTITLE 1,Genie Code Agent Examples
# MAGIC %md
# MAGIC ## 🤖 Genie Code Agent Usage Examples
# MAGIC
# MAGIC ### Prompt Examples for This Topic:
# MAGIC
# MAGIC 1️⃣ **Query History:**
# MAGIC ```
# MAGIC Show me the version history of customer_orders table
# MAGIC ```
# MAGIC
# MAGIC 2️⃣ **Time Travel:**
# MAGIC ```
# MAGIC Query customer_orders table as it was in version 5
# MAGIC ```
# MAGIC
# MAGIC 3️⃣ **Data Recovery:**
# MAGIC ```
# MAGIC Recover data from customer_orders version 3 and save as recovered_orders
# MAGIC ```
# MAGIC
# MAGIC 4️⃣ **Timestamp Query:**
# MAGIC ```
# MAGIC Show me product_inventory as it was on 2024-01-15
# MAGIC ```
# MAGIC
# MAGIC 5️⃣ **Debugging:**
# MAGIC ```
# MAGIC Help me identify when sales_data was corrupted by comparing recent versions
# MAGIC ```
# MAGIC
# MAGIC 6️⃣ **Audit:**
# MAGIC ```
# MAGIC Show me all changes made to employee_table in the last 7 days
# MAGIC ```
# MAGIC
# MAGIC 7️⃣ **Rollback:**
# MAGIC ```
# MAGIC Rollback transactions_table to version 10
# MAGIC ```
# MAGIC
# MAGIC 8️⃣ **Version Comparison:**
# MAGIC ```
# MAGIC Compare version 5 and version 8 of inventory_table to see what changed
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Tips:
# MAGIC * Be specific about version numbers or timestamps
# MAGIC * Always verify before rolling back
# MAGIC * Use natural language - Genie understands context
# MAGIC * Ask for explanations of version history

# COMMAND ----------

# DBTITLE 1,Analysis: Time Travel Investigation
# 🔍 ANALYSIS: Use time travel to investigate
print("🔙 ANALYSIS PHASE: Time Travel Investigation")
print("="*80)

# Query Version 1 (before deletion)
print("\nVersion 1 (Before Incident):")
df_v1 = spark.sql(f"""
    SELECT * FROM {catalog_name}.{schema_name}.{feedback_table} VERSION AS OF 1
""")
print(f"Row count: {df_v1.count()}")
display(df_v1)

# Identify what was deleted
print("\n💡 Analysis: Version 1 had all feedback including 5-star ratings")
print("Strategy: Restore from Version 1 (last known good state)")

# COMMAND ----------

# DBTITLE 1,Recovery: Restore Data
# 🔧 RECOVERY: Restore data from Version 1
print("⚙️ RECOVERY PHASE")
print("="*80)

# Method: Overwrite current table with Version 1 data
spark.sql(f"""
    INSERT OVERWRITE TABLE {catalog_name}.{schema_name}.{feedback_table}
    SELECT * FROM {catalog_name}.{schema_name}.{feedback_table} VERSION AS OF 1
""")

print("✅ Recovery completed: Data restored from Version 1")

# COMMAND ----------

# DBTITLE 1,Validation: Verify Success
# ✅ VALIDATION: Verify recovery success
print("✅ VALIDATION PHASE")
print("="*80)

restored_df = spark.table(f"{catalog_name}.{schema_name}.{feedback_table}")

print(f"\nTotal feedback count: {restored_df.count()}")
print("\n5-star ratings:")
five_star_df = restored_df.filter("rating = 5")
print(f"Count: {five_star_df.count()}")

display(restored_df)

print("\n✅ SUCCESS: All data recovered successfully!")
print("📊 Recovery Summary:")
print(f"  - Incident: Accidental deletion (Version 2)")
print(f"  - Recovery point: Version 1")
print(f"  - Records recovered: {five_star_df.count()} five-star ratings")
print(f"  - Total records: {restored_df.count()}")

# COMMAND ----------

# DBTITLE 1,Version 1: Add New Feedback
# Version 1: Add more feedback
new_feedback = [
    (6, "C006", "Fast delivery", 4, "2024-01-06"),
    (7, "C007", "Quality product", 5, "2024-01-07"),
    (8, "C008", "Will buy again", 5, "2024-01-08")
]

df_new = spark.createDataFrame(
    new_feedback,
    ["feedback_id", "customer_id", "comment", "rating", "created_date"]
)

df_new.write.format("delta").mode("append").saveAsTable(
    f"{catalog_name}.{schema_name}.{feedback_table}"
)

print("✅ Version 1: New feedback added")
print(f"Total feedback: {spark.table(f'{catalog_name}.{schema_name}.{feedback_table}').count()}")
display(spark.table(f"{catalog_name}.{schema_name}.{feedback_table}"))

# COMMAND ----------

# DBTITLE 1,Incident: Accidental Deletion
# 🚨 INCIDENT: Accidental deletion (Version 2)
# Someone accidentally deletes all 5-star ratings!
spark.sql(f"""
    DELETE FROM {catalog_name}.{schema_name}.{feedback_table}
    WHERE rating = 5
""")

print("❌ INCIDENT: Accidental deletion of 5-star ratings!")
print(f"Remaining feedback: {spark.table(f'{catalog_name}.{schema_name}.{feedback_table}').count()}")
display(spark.table(f"{catalog_name}.{schema_name}.{feedback_table}"))

# COMMAND ----------

# DBTITLE 1,Detection: Investigate History
# 🔍 DETECTION: Investigate what happened
print("🔍 DETECTION PHASE")
print("="*80)

# Check table history
history_df = spark.sql(f"DESCRIBE HISTORY {catalog_name}.{schema_name}.{feedback_table}")

print("\nRecent operations:")
display(history_df.select("version", "timestamp", "operation", "operationMetrics").limit(5))

# Identify the problem
print("\n⚠️ Issue detected: DELETE operation in Version 2 removed 5-star ratings!")

# COMMAND ----------

# DBTITLE 1,Section 6: Delta Transaction Log
# MAGIC %md
# MAGIC ## 📜 SECTION 6 — Delta Transaction Log (Conceptual)
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC The transaction log is like a journal that writes down everything that happens to your data. Every change gets a new page in the journal, and the pages are numbered in order.
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Delta Transaction Log Structure:**
# MAGIC
# MAGIC ```
# MAGIC table_location/
# MAGIC   _delta_log/
# MAGIC     00000000000000000000.json  <- Version 0
# MAGIC     00000000000000000001.json  <- Version 1
# MAGIC     00000000000000000002.json  <- Version 2
# MAGIC     00000000000000000010.checkpoint.parquet  <- Checkpoint
# MAGIC ```
# MAGIC
# MAGIC **Key Components:**
# MAGIC
# MAGIC 1. **Commit Files**: JSON files containing transaction metadata
# MAGIC 2. **Checkpoints**: Parquet files for faster state reconstruction
# MAGIC 3. **Last Checkpoint**: File indicating latest checkpoint
# MAGIC
# MAGIC **Each Commit File Contains:**
# MAGIC
# MAGIC * **add**: Files added to the table
# MAGIC * **remove**: Files removed from the table
# MAGIC * **metadata**: Table schema and properties
# MAGIC * **protocol**: Delta protocol version
# MAGIC * **commitInfo**: Operation details (user, timestamp, metrics)
# MAGIC
# MAGIC **How Versioning Works:**
# MAGIC
# MAGIC 1. Write operation creates new commit file
# MAGIC 2. File is atomically written to `_delta_log/`
# MAGIC 3. Version number = filename = sequential integer
# MAGIC 4. Delta reads all commit files to reconstruct table state
# MAGIC 5. Checkpoints optimize reads by consolidating history
# MAGIC
# MAGIC **Foundation of Time Travel:**
# MAGIC
# MAGIC * Each version's commit file is immutable
# MAGIC * Reading a version = replaying commits up to that version
# MAGIC * VACUUM removes old data files (not commit files immediately)
# MAGIC * Commit files enable time travel even after data file removal

# COMMAND ----------

# DBTITLE 1,Section 7: End-to-End Recovery Scenario
# MAGIC %md
# MAGIC ## 🚨 SECTION 7 — End-to-End Recovery Scenario
# MAGIC
# MAGIC ### 🎯 Scenario:
# MAGIC Simulate a real-world data incident and complete recovery process:
# MAGIC
# MAGIC 1. **Normal Operations**: Data pipeline running smoothly
# MAGIC 2. **Incident**: Accidental data corruption or deletion
# MAGIC 3. **Detection**: Identify the issue and affected version
# MAGIC 4. **Analysis**: Use time travel to investigate
# MAGIC 5. **Recovery**: Restore data to known good state
# MAGIC 6. **Validation**: Verify recovery success

# COMMAND ----------

# DBTITLE 1,Initial State: Normal Operations
# Scenario: Customer feedback system
feedback_table = "customer_feedback"

# Version 0: Initial feedback data
initial_feedback = [
    (1, "C001", "Great product!", 5, "2024-01-01"),
    (2, "C002", "Good value", 4, "2024-01-02"),
    (3, "C003", "Excellent service", 5, "2024-01-03"),
    (4, "C004", "Average experience", 3, "2024-01-04"),
    (5, "C005", "Highly recommended", 5, "2024-01-05")
]

df_feedback = spark.createDataFrame(
    initial_feedback,
    ["feedback_id", "customer_id", "comment", "rating", "created_date"]
)

df_feedback.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_name}.{feedback_table}"
)

print("✅ Version 0: Initial feedback system created")
print(f"Total feedback: {df_feedback.count()}")
display(spark.table(f"{catalog_name}.{schema_name}.{feedback_table}"))

# COMMAND ----------

# DBTITLE 1,Step 3: Restock Inventory
# Step 3: Restock inventory (Version 2)
new_stock = [
    ("P001", "Laptop", 30, 999.99, "2024-01-10"),
    ("P003", "Keyboard", 50, 79.99, "2024-01-10")
]

df_restock = spark.createDataFrame(
    new_stock,
    ["product_id", "product_name", "quantity_add", "unit_price", "last_updated"]
)

# Add to existing quantity
for row in new_stock:
    spark.sql(f"""
        UPDATE {catalog_name}.{schema_name}.{inventory_table}
        SET quantity = quantity + {row[2]},
            last_updated = '{row[4]}'
        WHERE product_id = '{row[0]}'
    """)

print("✅ Version 2: Inventory restocked")
display(spark.table(f"{catalog_name}.{schema_name}.{inventory_table}"))

# COMMAND ----------

# DBTITLE 1,Step 4: Price Adjustment
# Step 4: Price adjustment (Version 3)
spark.sql(f"""
    UPDATE {catalog_name}.{schema_name}.{inventory_table}
    SET unit_price = unit_price * 0.9,
        last_updated = '2024-01-15'
    WHERE product_id IN ('P002', 'P004')
""")

print("✅ Version 3: Price adjustment (10% discount on Mouse and Monitor)")
display(spark.table(f"{catalog_name}.{schema_name}.{inventory_table}"))

# COMMAND ----------

# DBTITLE 1,Step 5: Historical Analysis
# Step 5: Analyze inventory changes across versions
print("📊 Historical Inventory Analysis")
print("="*80)

# Compare quantity changes across versions
for version in [0, 1, 2, 3]:
    df_version = spark.sql(f"""
        SELECT product_id, product_name, quantity, unit_price
        FROM {catalog_name}.{schema_name}.{inventory_table} 
        VERSION AS OF {version}
        ORDER BY product_id
    """)
    
    print(f"\n📌 Version {version}:")
    display(df_version)

# COMMAND ----------

# DBTITLE 1,Section 5: Hands-on Versioning Pipeline
# MAGIC %md
# MAGIC ## 🛠️ SECTION 5 — Hands-on Versioning Pipeline
# MAGIC
# MAGIC ### 🎯 Objective:
# MAGIC Build a complete versioning pipeline demonstrating:
# MAGIC 1. Initial data creation
# MAGIC 2. Incremental updates
# MAGIC 3. Data corrections
# MAGIC 4. Version tracking
# MAGIC 5. Historical analysis
# MAGIC
# MAGIC ### 📝 Pipeline Steps:
# MAGIC 1. Create product inventory table
# MAGIC 2. Track stock changes across versions
# MAGIC 3. Query historical inventory levels
# MAGIC 4. Audit inventory changes

# COMMAND ----------

# DBTITLE 1,Step 1: Create Initial Inventory
# Step 1: Create initial product inventory (Version 0)
inventory_table = "product_inventory"

initial_inventory = [
    ("P001", "Laptop", 50, 999.99, "2024-01-01"),
    ("P002", "Mouse", 200, 29.99, "2024-01-01"),
    ("P003", "Keyboard", 150, 79.99, "2024-01-01"),
    ("P004", "Monitor", 75, 299.99, "2024-01-01")
]

df_inventory = spark.createDataFrame(
    initial_inventory, 
    ["product_id", "product_name", "quantity", "unit_price", "last_updated"]
)

df_inventory.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_name}.{inventory_table}"
)

print("✅ Version 0: Initial inventory created")
display(spark.table(f"{catalog_name}.{schema_name}.{inventory_table}"))

# COMMAND ----------

# DBTITLE 1,Step 2: Process Sales
# Step 2: Sales reduce inventory (Version 1)
spark.sql(f"""
    UPDATE {catalog_name}.{schema_name}.{inventory_table}
    SET quantity = quantity - 10,
        last_updated = '2024-01-05'
    WHERE product_id IN ('P001', 'P002')
""")

print("✅ Version 1: Sales processed (reduced inventory)")
display(spark.table(f"{catalog_name}.{schema_name}.{inventory_table}"))

# COMMAND ----------

# DBTITLE 1,View Current State (Deleted Data)
# Current state (Version 3 - after deletion)
print("❌ Current State (Data Deleted - Version 3):")
print("="*80)
current_df = spark.table(f"{catalog_name}.{schema_name}.{table_name}")
print(f"Row count: {current_df.count()}")
display(current_df)

# COMMAND ----------

# DBTITLE 1,Recover Data from Version 2
# Recover data by creating new table from Version 2 (before deletion)
recovered_table = "customer_orders_recovered"

spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.{recovered_table} AS
    SELECT * FROM {catalog_name}.{schema_name}.{table_name} VERSION AS OF 2
""")

print(f"✅ Data Recovered from Version 2")
print(f"💾 Recovered table: {recovered_table}")
print("="*80)

recovered_df = spark.table(f"{catalog_name}.{schema_name}.{recovered_table}")
print(f"Row count: {recovered_df.count()}")
display(recovered_df)

# COMMAND ----------

# DBTITLE 1,Restore Original Table
# Alternative: Restore original table by overwriting with historical version
spark.sql(f"""
    INSERT OVERWRITE TABLE {catalog_name}.{schema_name}.{table_name}
    SELECT * FROM {catalog_name}.{schema_name}.{table_name} VERSION AS OF 2
""")

print(f"✅ Original table restored to Version 2 state")
print("="*80)

restored_df = spark.table(f"{catalog_name}.{schema_name}.{table_name}")
print(f"Row count: {restored_df.count()}")
display(restored_df)

# COMMAND ----------

# DBTITLE 1,Timestamp-Based Query
# Get timestamp from version 1 for demonstration
history_df = spark.sql(f"DESCRIBE HISTORY {catalog_name}.{schema_name}.{table_name}")
timestamp_v1 = history_df.filter("version = 1").select("timestamp").collect()[0][0]

print(f"🕒 Timestamp for Version 1: {timestamp_v1}")
print("="*80)

# Query using timestamp
df_timestamp = spark.sql(f"""
    SELECT * FROM {catalog_name}.{schema_name}.{table_name} 
    TIMESTAMP AS OF '{timestamp_v1}'
""")

print("\nData as of timestamp:")
display(df_timestamp)

# COMMAND ----------

# DBTITLE 1,Section 4: Data Recovery
# MAGIC %md
# MAGIC ## 🔄 SECTION 4 — Data Recovery
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC Imagine you accidentally erased something important. With Delta Lake, you can simply go back in time, grab the old version, and bring it back. It's like having an "undo" button!
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Data Recovery Strategies:**
# MAGIC
# MAGIC 1. **Restore from Version**: Create new table from historical version
# MAGIC 2. **Overwrite Current**: Replace current data with historical data
# MAGIC 3. **Selective Recovery**: Merge specific records from historical version
# MAGIC 4. **Clone Historical Version**: Create independent copy
# MAGIC
# MAGIC **Recovery Syntax:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Full table recovery
# MAGIC CREATE OR REPLACE TABLE recovered_table AS
# MAGIC SELECT * FROM original_table VERSION AS OF <version>
# MAGIC
# MAGIC -- Overwrite current table
# MAGIC INSERT OVERWRITE TABLE current_table
# MAGIC SELECT * FROM current_table VERSION AS OF <version>
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Recovery Scenario
# MAGIC %md
# MAGIC ### 🔧 Scenario: Recover from Accidental Deletion

# COMMAND ----------

# DBTITLE 1,Query Version 1
# Query Version 1 (After first insert)
print("🔙 Time Travel to Version 1 (After INSERT):")
print("="*80)

df_v1 = spark.sql(f"""
    SELECT * FROM {catalog_name}.{schema_name}.{table_name} VERSION AS OF 1
""")

display(df_v1)

# COMMAND ----------

# DBTITLE 1,Query Version 2
# Query Version 2 (After update)
print("🔙 Time Travel to Version 2 (After UPDATE):")
print("="*80)

df_v2 = spark.sql(f"""
    SELECT * FROM {catalog_name}.{schema_name}.{table_name} VERSION AS OF 2
""")

display(df_v2)

# COMMAND ----------

# DBTITLE 1,Part B: Timestamp-Based Time Travel
# MAGIC %md
# MAGIC ### 📅 PART B — Timestamp-Based Time Travel
# MAGIC
# MAGIC **Timestamp-based queries** allow you to query data as it existed at a specific date/time.
# MAGIC
# MAGIC **Formats Supported:**
# MAGIC * `'2024-01-01'`
# MAGIC * `'2024-01-01 10:30:00'`
# MAGIC * `'2024-01-01T10:30:00.000Z'`

# COMMAND ----------

# DBTITLE 1,Version 3: Delete Data
# Version 3: DELETE data
spark.sql(f"""
    DELETE FROM {catalog_name}.{schema_name}.{table_name}
    WHERE amount < 150
""")

print(f"📊 Version 3: Deleted orders with amount < 150")

# View current state
print("\nCurrent State (Version 3):")
display(spark.table(f"{catalog_name}.{schema_name}.{table_name}"))

# COMMAND ----------

# DBTITLE 1,Part A: Version-Based Time Travel
# MAGIC %md
# MAGIC ### 🔍 PART A — Version-Based Time Travel

# COMMAND ----------

# DBTITLE 1,Query Version 0
# Query Version 0 (Original data)
print("🔙 Time Travel to Version 0 (Original Data):")
print("="*80)

df_v0 = spark.sql(f"""
    SELECT * FROM {catalog_name}.{schema_name}.{table_name} VERSION AS OF 0
""")

display(df_v0)

# COMMAND ----------

# DBTITLE 1,Section 3: Time Travel
# MAGIC %md
# MAGIC ## ⏱️ SECTION 3 — Time Travel (IMPORTANT)
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC Time Travel is like having a time machine for your data! You can go back to see what your data looked like yesterday, last week, or at any specific version.
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **What is Time Travel?**
# MAGIC
# MAGIC Time Travel allows you to query historical versions of Delta tables using:
# MAGIC 1. **Version Number**: Query a specific version
# MAGIC 2. **Timestamp**: Query data as it existed at a specific point in time
# MAGIC
# MAGIC **Use Cases:**
# MAGIC * Audit & Compliance
# MAGIC * Debugging data issues
# MAGIC * Reproducibility
# MAGIC * A/B Testing
# MAGIC * Rollback operations

# COMMAND ----------

# DBTITLE 1,Version 1: Insert Data
# Version 1: INSERT new data
data_v1 = [
    (4, "Diana", 250.0, "2024-01-18"),
    (5, "Eve", 300.0, "2024-01-19")
]

df_v1 = spark.createDataFrame(data_v1, ["order_id", "customer_name", "amount", "order_date"])
df_v1.write.format("delta").mode("append").saveAsTable(f"{catalog_name}.{schema_name}.{table_name}")

print(f"📊 Version 1: Inserted 2 new orders")

# COMMAND ----------

# DBTITLE 1,Version 2: Update Data
# Version 2: UPDATE existing data
spark.sql(f"""
    UPDATE {catalog_name}.{schema_name}.{table_name}
    SET amount = amount * 1.1
    WHERE customer_name IN ('Alice', 'Bob')
""")

print(f"📊 Version 2: Updated amounts for Alice and Bob")

# COMMAND ----------

# DBTITLE 1,Section 2: Viewing Table History
# MAGIC %md
# MAGIC ## 📖 SECTION 2 — Viewing Table History
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC Every time you make a change to your Delta table, it's like adding a new entry to a diary. You can read this diary anytime to see what happened, when it happened, and who did it!
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **DESCRIBE HISTORY Command:**
# MAGIC
# MAGIC The `DESCRIBE HISTORY` command provides access to the Delta transaction log in a human-readable format.
# MAGIC
# MAGIC **Key Columns:**
# MAGIC
# MAGIC * **version**: Integer version number (starts at 0)
# MAGIC * **timestamp**: When the operation occurred
# MAGIC * **operation**: Type of operation (CREATE, WRITE, DELETE, UPDATE, MERGE, etc.)
# MAGIC * **operationMetrics**: Statistics about the operation
# MAGIC * **userName**: User who executed the operation
# MAGIC * **readVersion**: Version read during the operation
# MAGIC * **isolationLevel**: Transaction isolation level
# MAGIC
# MAGIC **Use Cases:**
# MAGIC
# MAGIC * **Audit**: Track who changed what and when
# MAGIC * **Debugging**: Identify when data issues were introduced
# MAGIC * **Compliance**: Maintain change history for regulations
# MAGIC * **Performance Analysis**: Review operation metrics

# COMMAND ----------

# DBTITLE 1,Create Sample Table (Version 0)
# Create a sample table to demonstrate versioning
table_name = "customer_orders"

# Version 0: CREATE TABLE with initial data
data_v0 = [
    (1, "Alice", 100.0, "2024-01-15"),
    (2, "Bob", 150.0, "2024-01-16"),
    (3, "Charlie", 200.0, "2024-01-17")
]

df_v0 = spark.createDataFrame(data_v0, ["order_id", "customer_name", "amount", "order_date"])

df_v0.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.{table_name}")

print(f"✅ Table created: {catalog_name}.{schema_name}.{table_name}")
print(f"📊 Version 0: Initial data loaded")

# Display current data
display(spark.table(f"{catalog_name}.{schema_name}.{table_name}"))

# COMMAND ----------

# DBTITLE 1,View History Header
# MAGIC %md
# MAGIC ### 🔍 View Table History

# COMMAND ----------

# DBTITLE 1,Describe History
# Use DESCRIBE HISTORY to view version history
history_df = spark.sql(f"DESCRIBE HISTORY {catalog_name}.{schema_name}.{table_name}")

print(f"📜 Transaction History for {table_name}:")
print("="*80)

# Display the history
display(history_df.select("version", "timestamp", "operation", "operationMetrics", "userName"))

# COMMAND ----------

# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # 🧊 Data Engineering Training — Phase 4 Day 21  
# MAGIC ## ⏳ Delta Lake Versioning: Time Travel & Data Recovery  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - Delta Table Versioning  
# MAGIC - Time Travel (Version & Timestamp)  
# MAGIC - Data Recovery Techniques  
# MAGIC - Delta Transaction History  
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
# MAGIC Understand how Delta Lake enables versioning, time travel, and reliable data recovery using transaction logs.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚙️ Engineering Constraints:
# MAGIC * ✅ Databricks Serverless Compute
# MAGIC * ✅ DataFrame API only (no RDDs)
# MAGIC * ✅ No cache() / persist()
# MAGIC * ✅ Unity Catalog managed tables
# MAGIC * ✅ Delta Lake format (versioning enabled by default)

# COMMAND ----------

# DBTITLE 1,Section 1: Delta Versioning Overview
# MAGIC %md
# MAGIC ## 📚 SECTION 1 — Delta Versioning Overview
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC Imagine you're writing in a notebook and every time you make a change, someone takes a photo of that page. You can always go back and look at any previous photo to see what it looked like before. That's versioning!
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **What is Delta Versioning?**
# MAGIC
# MAGIC Delta Lake maintains a complete **transaction log** that tracks every change made to a table. Each write operation (INSERT, UPDATE, DELETE, MERGE) creates a new **version** of the table.
# MAGIC
# MAGIC **Key Concepts:**
# MAGIC
# MAGIC * **Version Number**: Sequential integer starting from 0
# MAGIC * **Transaction Log**: JSON files stored in `_delta_log/` directory
# MAGIC * **Immutability**: Previous versions remain accessible (until VACUUM)
# MAGIC * **ACID Guarantees**: Each version represents a consistent state
# MAGIC
# MAGIC **How Delta Tracks Versions:**
# MAGIC
# MAGIC 1. Every write operation commits a new JSON file to `_delta_log/`
# MAGIC 2. File naming: `00000000000000000000.json`, `00000000000000000001.json`, etc.
# MAGIC 3. Each file contains metadata about the transaction
# MAGIC 4. Delta automatically manages version increments
# MAGIC
# MAGIC **Benefits:**
# MAGIC
# MAGIC * **Audit Trail**: Complete history of all changes
# MAGIC * **Time Travel**: Query historical data states
# MAGIC * **Data Recovery**: Rollback to previous versions
# MAGIC * **Debugging**: Trace data issues to specific operations

# COMMAND ----------

# DBTITLE 1,Setup Instructions
# MAGIC %md
# MAGIC ### 🛠️ Setup: Create Catalog and Schema

# COMMAND ----------

# DBTITLE 1,Setup Catalog and Schema
# Create catalog and schema for this tutorial
catalog_name = "day21_versioning_demo"
schema_name = "time_travel"

# Create catalog
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"USE CATALOG {catalog_name}")

# Create schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
spark.sql(f"USE SCHEMA {schema_name}")

print(f"✅ Catalog: {catalog_name}")
print(f"✅ Schema: {schema_name}")
print(f"✅ Current namespace: {catalog_name}.{schema_name}")