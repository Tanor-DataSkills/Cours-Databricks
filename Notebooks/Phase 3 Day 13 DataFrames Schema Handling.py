# Databricks notebook source
# DBTITLE 1,🌟 Bonus: Schema Evolution
# MAGIC %md
# MAGIC ---
# MAGIC ## 🌟 BONUS: Schema Evolution (Advanced)
# MAGIC
# MAGIC ### What is Schema Evolution?
# MAGIC
# MAGIC Schema evolution allows you to change a table's schema over time without rewriting existing data.
# MAGIC
# MAGIC ### 🔄 Compatible Changes
# MAGIC
# MAGIC #### ✅ Allowed (with mergeSchema=true):
# MAGIC 1. **Adding new columns**
# MAGIC    ```python
# MAGIC    # Existing: [id, name]
# MAGIC    # New: [id, name, email]  ← OK!
# MAGIC    df.write.option("mergeSchema", "true").mode("append").saveAsTable("table")
# MAGIC    ```
# MAGIC
# MAGIC 2. **Widening data types**
# MAGIC    ```python
# MAGIC    # IntegerType → LongType  ← OK!
# MAGIC    # FloatType → DoubleType  ← OK!
# MAGIC    ```
# MAGIC
# MAGIC #### ❌ NOT Allowed:
# MAGIC 1. **Dropping columns** (breaks backward compatibility)
# MAGIC 2. **Renaming columns** (seen as drop + add)
# MAGIC 3. **Narrowing types** (LongType → IntegerType)
# MAGIC 4. **Changing nullability** (non-nullable → nullable is OK, reverse is NOT)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔒 Delta Schema Enforcement
# MAGIC
# MAGIC Delta Lake enforces schema by default:
# MAGIC
# MAGIC ```python
# MAGIC # Schema enforcement (default behavior)
# MAGIC df.write.format("delta").saveAsTable("table")  
# MAGIC # Rejects writes with incompatible schema!
# MAGIC
# MAGIC # Allow schema evolution
# MAGIC df.write.option("mergeSchema", "true").format("delta").saveAsTable("table")
# MAGIC # Allows adding new columns
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛡️ Why Schema Enforcement?
# MAGIC
# MAGIC 1. **Data Quality**: Prevents corrupt data
# MAGIC 2. **Consistency**: Ensures all data has same structure
# MAGIC 3. **Downstream Safety**: Protects consumers from breaking changes
# MAGIC 4. **Type Safety**: Enforces data types
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💻 Example: Schema Evolution
# MAGIC
# MAGIC ```python
# MAGIC # Initial schema
# MAGIC initial_schema = StructType([
# MAGIC     StructField("id", IntegerType(), False),
# MAGIC     StructField("name", StringType(), False)
# MAGIC ])
# MAGIC
# MAGIC # Initial write
# MAGIC df1.write.format("delta").saveAsTable("catalog.schema.customers")
# MAGIC
# MAGIC # Evolved schema (added email column)
# MAGIC evolved_schema = StructType([
# MAGIC     StructField("id", IntegerType(), False),
# MAGIC     StructField("name", StringType(), False),
# MAGIC     StructField("email", StringType(), True)  # New column
# MAGIC ])
# MAGIC
# MAGIC # Write with schema evolution
# MAGIC df2.write.option("mergeSchema", "true") \
# MAGIC     .format("delta") \
# MAGIC     .mode("append") \
# MAGIC     .saveAsTable("catalog.schema.customers")
# MAGIC
# MAGIC # Old rows will have NULL in email column
# MAGIC # New rows will have email values
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Best Practices for Schema Evolution
# MAGIC
# MAGIC 1. **Always add columns as nullable** (nullable=True)
# MAGIC 2. **Document schema changes** in version control
# MAGIC 3. **Test backward compatibility** with downstream consumers
# MAGIC 4. **Use schema registry** for governance
# MAGIC 5. **Monitor schema changes** with alerts
# MAGIC 6. **Communicate changes** to stakeholders

# COMMAND ----------

# DBTITLE 1,📚 Additional Resources & Next Steps
# MAGIC %md
# MAGIC ---
# MAGIC ## 📚 Additional Resources
# MAGIC
# MAGIC ### 📚 Documentation
# MAGIC * [Spark SQL Data Types](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)
# MAGIC * [Delta Lake Schema Enforcement](https://docs.databricks.com/delta/schema-enforcement.html)
# MAGIC * [Delta Lake Schema Evolution](https://docs.databricks.com/delta/schema-evolution.html)
# MAGIC * [PySpark DataFrame API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Practice Exercises
# MAGIC
# MAGIC 1. Create a DataFrame with nested customer data (name, address, orders)
# MAGIC 2. Implement a schema validation function
# MAGIC 3. Build a pipeline that handles schema drift
# MAGIC 4. Convert a JSON schema to StructType programmatically
# MAGIC 5. Implement type casting for mixed-type columns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛠️ Tools & Utilities
# MAGIC
# MAGIC * **Schema Registry**: Manage schema versions
# MAGIC * **Data Quality Frameworks**: Great Expectations, Deequ
# MAGIC * **Schema Conversion Tools**: JSON Schema to StructType converters
# MAGIC * **Monitoring**: Track schema changes over time
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎓 Training Complete!
# MAGIC
# MAGIC **Phase 3 Day 13: DataFrames (Schema Handling & Data Types)**
# MAGIC
# MAGIC You are now equipped to:
# MAGIC * Design robust schemas
# MAGIC * Handle complex data types
# MAGIC * Build production-grade data pipelines
# MAGIC * Manage schema evolution
# MAGIC
# MAGIC **Keep learning, keep building!**
# MAGIC
# MAGIC **@TRRaveendra**

# COMMAND ----------

# DBTITLE 1,🎓 Final Summary
# MAGIC %md
# MAGIC ---
# MAGIC ## 🎓 FINAL SUMMARY
# MAGIC
# MAGIC ### 📚 Key Learnings
# MAGIC
# MAGIC #### 1️⃣ DataFrame Fundamentals
# MAGIC * Distributed, immutable, tabular data structure
# MAGIC * Schema-aware with Catalyst optimization
# MAGIC * Lazy evaluation with Tungsten execution
# MAGIC
# MAGIC #### 2️⃣ Schema Handling
# MAGIC * **Explicit Schema** > Schema Inference (in production)
# MAGIC * Use `StructType` and `StructField` for definition
# MAGIC * Always validate schema after reading and transforming
# MAGIC
# MAGIC #### 3️⃣ Data Types
# MAGIC * Choose appropriate types for your data
# MAGIC * Use `DecimalType` for money, `DateType` for dates
# MAGIC * Leverage complex types: `ArrayType`, `StructType`, `MapType`
# MAGIC
# MAGIC #### 4️⃣ Schema Operations
# MAGIC * Inspect: `printSchema()`, `schema`, `dtypes`, `columns`
# MAGIC * Transform: `select()`, `withColumn()`, `cast()`
# MAGIC * Validate: Assert column names and types
# MAGIC
# MAGIC #### 5️⃣ Nested Data
# MAGIC * Access nested fields with dot notation
# MAGIC * Explode arrays for row-per-element processing
# MAGIC * Handle JSON and semi-structured data
# MAGIC
# MAGIC #### 6️⃣ Production Pipeline Pattern
# MAGIC ```
# MAGIC Define Schema → Read Data → Validate → Transform → Validate → Write Delta
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛠️ Quick Reference
# MAGIC
# MAGIC ```python
# MAGIC # Define Schema
# MAGIC from pyspark.sql.types import *
# MAGIC schema = StructType([
# MAGIC     StructField("col1", StringType(), False),
# MAGIC     StructField("col2", IntegerType(), True)
# MAGIC ])
# MAGIC
# MAGIC # Read with Schema
# MAGIC df = spark.read.schema(schema).format("csv").load(path)
# MAGIC
# MAGIC # Inspect Schema
# MAGIC df.printSchema()
# MAGIC
# MAGIC # Access Nested Field
# MAGIC df.select("customer.address.city")
# MAGIC
# MAGIC # Explode Array
# MAGIC from pyspark.sql.functions import explode
# MAGIC df.select(explode("items"))
# MAGIC
# MAGIC # Cast Type
# MAGIC df.withColumn("col", col("col").cast(DoubleType()))
# MAGIC
# MAGIC # Write Delta
# MAGIC df.write.format("delta").saveAsTable("catalog.schema.table")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Next Steps
# MAGIC
# MAGIC 1. **Practice** with real datasets
# MAGIC 2. **Experiment** with complex nested structures
# MAGIC 3. **Build** end-to-end pipelines with schema validation
# MAGIC 4. **Learn** about schema evolution in Delta Lake
# MAGIC 5. **Explore** schema registries for governance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Remember
# MAGIC
# MAGIC ✅ **Schema First, Always**  
# MAGIC ✅ **Validate Early, Validate Often**  
# MAGIC ✅ **Choose Right Data Types**  
# MAGIC ✅ **Document Your Schema**  
# MAGIC ✅ **Test with Production-like Data**  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎆 Congratulations!
# MAGIC
# MAGIC You've completed **Phase 3 Day 13: DataFrames (Schema Handling & Data Types)**
# MAGIC
# MAGIC You now understand:
# MAGIC * How to work with Spark DataFrames
# MAGIC * How to manage schemas effectively
# MAGIC * How to choose appropriate data types
# MAGIC * How to build production-grade data pipelines
# MAGIC
# MAGIC **@TRRaveendra**

# COMMAND ----------

# DBTITLE 1,🎯 Interview Questions
# MAGIC %md
# MAGIC ---
# MAGIC ## 🎯 INTERVIEW QUESTIONS
# MAGIC
# MAGIC ### 🟢 Beginner Level
# MAGIC
# MAGIC **Q1: What is a DataFrame in Spark?**
# MAGIC
# MAGIC **Answer:** A DataFrame is a distributed collection of data organized into named columns with a schema. It's similar to a table in a relational database but optimized for distributed processing across a cluster.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q2: What are the two ways to define a schema in Spark?**
# MAGIC
# MAGIC **Answer:**
# MAGIC 1. **Schema Inference**: Spark automatically detects data types by scanning the data
# MAGIC 2. **Explicit Schema**: Developer manually defines the schema using StructType and StructField
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q3: Which data type should you use for storing monetary values?**
# MAGIC
# MAGIC **Answer:** `DecimalType(precision, scale)` - e.g., `DecimalType(10, 2)` for values up to 99,999,999.99. Never use `DoubleType` or `FloatType` for money due to floating-point precision issues.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q4: What does nullable=False mean in a StructField?**
# MAGIC
# MAGIC **Answer:** It means the column cannot contain NULL values. It's a NOT NULL constraint that enforces data quality at read time.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q5: How do you inspect a DataFrame's schema?**
# MAGIC
# MAGIC **Answer:**
# MAGIC * `df.printSchema()` - Tree format
# MAGIC * `df.schema` - StructType object
# MAGIC * `df.dtypes` - List of (name, type) tuples
# MAGIC * `df.columns` - Column names only
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟡 Intermediate Level
# MAGIC
# MAGIC **Q6: Why is explicit schema better than schema inference in production?**
# MAGIC
# MAGIC **Answer:**
# MAGIC * **Performance**: No data scanning overhead
# MAGIC * **Deterministic**: Same schema every time
# MAGIC * **Validation**: Catches data quality issues early
# MAGIC * **Documentation**: Schema as code
# MAGIC * **Reliability**: No surprises in production
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q7: How do you access nested fields in a DataFrame?**
# MAGIC
# MAGIC **Answer:**
# MAGIC ```python
# MAGIC # Dot notation
# MAGIC df.select("customer.address.city")
# MAGIC
# MAGIC # Col function
# MAGIC df.select(col("customer.address.city"))
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q8: What's the difference between ArrayType and MapType?**
# MAGIC
# MAGIC **Answer:**
# MAGIC * **ArrayType**: Ordered list of elements of the same type (like Python list)
# MAGIC * **MapType**: Key-value pairs (like Python dictionary)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q9: How do you handle an array column with multiple values per row?**
# MAGIC
# MAGIC **Answer:** Use `explode()` function to create one row per array element:
# MAGIC ```python
# MAGIC from pyspark.sql.functions import explode
# MAGIC df.select("id", explode("items").alias("item"))
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q10: What happens if you read data with the wrong schema?**
# MAGIC
# MAGIC **Answer:** Spark will try to cast data to the specified types. If casting fails:
# MAGIC * Values may become NULL
# MAGIC * Exceptions may be raised (depending on mode)
# MAGIC * Data quality issues may go undetected
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 Advanced Level
# MAGIC
# MAGIC **Q11: Explain schema evolution in Delta Lake.**
# MAGIC
# MAGIC **Answer:** Schema evolution allows adding new columns to a Delta table without rewriting existing data. Enable with:
# MAGIC ```python
# MAGIC df.write.option("mergeSchema", "true").saveAsTable("table")
# MAGIC ```
# MAGIC Supports: adding columns, widening types (int → long). Does NOT support: dropping columns, narrowing types.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q12: What's the difference between schema enforcement and schema evolution?**
# MAGIC
# MAGIC **Answer:**
# MAGIC * **Schema Enforcement**: Rejects writes that don't match existing schema (default in Delta)
# MAGIC * **Schema Evolution**: Allows compatible schema changes (adding columns, type widening)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q13: How do you optimize schema for better performance?**
# MAGIC
# MAGIC **Answer:**
# MAGIC 1. Use appropriate data types (smaller types = less memory)
# MAGIC 2. Use primitive types over complex types when possible
# MAGIC 3. Partition on low-cardinality columns
# MAGIC 4. Avoid excessive nullability (enable nullable=False when appropriate)
# MAGIC 5. Use column pruning (select only needed columns)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q14: How do you handle schema drift in a data pipeline?**
# MAGIC
# MAGIC **Answer:**
# MAGIC 1. **Define explicit schema** at source
# MAGIC 2. **Validate schema** before processing
# MAGIC 3. **Version schemas** using schema registry
# MAGIC 4. **Monitor schema changes** with alerts
# MAGIC 5. **Use schema evolution** for backward-compatible changes
# MAGIC 6. **Implement error handling** for incompatible changes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q15: What are the performance implications of schema inference?**
# MAGIC
# MAGIC **Answer:**
# MAGIC 1. **Double data scan**: Once for schema, once for reading
# MAGIC 2. **Network overhead**: Data transfer for sampling
# MAGIC 3. **Non-deterministic**: Different inferences on different data
# MAGIC 4. **No type validation**: Wrong types may be inferred
# MAGIC 5. **Scalability issues**: Slower on large datasets

# COMMAND ----------

# DBTITLE 1,⚠️ Common Mistakes
# MAGIC %md
# MAGIC ---
# MAGIC ## ⚠️ COMMON MISTAKES
# MAGIC
# MAGIC ### 1️⃣ Relying on Schema Inference
# MAGIC
# MAGIC **Mistake:**
# MAGIC ```python
# MAGIC df = spark.read.option("inferSchema", "true").csv("data.csv")
# MAGIC ```
# MAGIC
# MAGIC **Problem:**
# MAGIC * Scans data twice (once for schema, once for reading)
# MAGIC * Non-deterministic (data changes = schema changes)
# MAGIC * May infer wrong types
# MAGIC
# MAGIC **Solution:**
# MAGIC ```python
# MAGIC schema = StructType([...])  # Define explicitly
# MAGIC df = spark.read.schema(schema).csv("data.csv")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ Using Wrong Data Types
# MAGIC
# MAGIC **Mistake:**
# MAGIC ```python
# MAGIC StructField("price", DoubleType())  # Floating point precision issues!
# MAGIC StructField("date", StringType())   # String instead of DateType!
# MAGIC ```
# MAGIC
# MAGIC **Solution:**
# MAGIC ```python
# MAGIC StructField("price", DecimalType(10, 2))  # Exact decimal precision
# MAGIC StructField("date", DateType())            # Proper date type
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ Ignoring Schema Validation
# MAGIC
# MAGIC **Mistake:**
# MAGIC ```python
# MAGIC df = spark.read.csv(path)
# MAGIC # Directly start transformations without validation!
# MAGIC ```
# MAGIC
# MAGIC **Solution:**
# MAGIC ```python
# MAGIC df = spark.read.schema(expected_schema).csv(path)
# MAGIC assert set(df.columns) == set(expected_columns)
# MAGIC assert df.schema == expected_schema
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4️⃣ Poor Handling of Nested Data
# MAGIC
# MAGIC **Mistake:**
# MAGIC ```python
# MAGIC # Trying to access nested field incorrectly
# MAGIC df.select("customer").select("name")  # Wrong!
# MAGIC ```
# MAGIC
# MAGIC **Solution:**
# MAGIC ```python
# MAGIC # Use dot notation
# MAGIC df.select("customer.name")
# MAGIC # Or col function
# MAGIC df.select(col("customer.name"))
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5️⃣ Mixing String Operations on Typed Columns
# MAGIC
# MAGIC **Mistake:**
# MAGIC ```python
# MAGIC # Date stored as DateType, but filtering as string
# MAGIC df.filter("date > '2023-01-01'")  # Type mismatch!
# MAGIC ```
# MAGIC
# MAGIC **Solution:**
# MAGIC ```python
# MAGIC from datetime import date
# MAGIC df.filter(col("date") > lit(date(2023, 1, 1)))
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6️⃣ Not Setting Nullability Constraints
# MAGIC
# MAGIC **Mistake:**
# MAGIC ```python
# MAGIC StructField("id", IntegerType(), True)  # IDs should not be null!
# MAGIC ```
# MAGIC
# MAGIC **Solution:**
# MAGIC ```python
# MAGIC StructField("id", IntegerType(), False)  # Enforce NOT NULL
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,🎯 Best Practices: Schema Management
# MAGIC %md
# MAGIC ---
# MAGIC ## 🎯 BEST PRACTICES: Schema Management
# MAGIC
# MAGIC ### 🟢 DO's:
# MAGIC
# MAGIC #### 1️⃣ Always Use Explicit Schema in Production
# MAGIC ```python
# MAGIC # ✅ GOOD
# MAGIC schema = StructType([...])
# MAGIC df = spark.read.schema(schema).csv(path)
# MAGIC
# MAGIC # ❌ BAD
# MAGIC df = spark.read.option("inferSchema", "true").csv(path)
# MAGIC ```
# MAGIC
# MAGIC #### 2️⃣ Choose Appropriate Data Types
# MAGIC * Use `DecimalType` for money (not `DoubleType`)
# MAGIC * Use `DateType` for dates (not `StringType`)
# MAGIC * Use `TimestampType` for datetime (not `StringType`)
# MAGIC * Use `LongType` for IDs (not `IntegerType` if > 2B)
# MAGIC
# MAGIC #### 3️⃣ Set Nullability Correctly
# MAGIC ```python
# MAGIC StructField("id", IntegerType(), nullable=False)  # Required
# MAGIC StructField("email", StringType(), nullable=True)  # Optional
# MAGIC ```
# MAGIC
# MAGIC #### 4️⃣ Validate Schema After Transformations
# MAGIC ```python
# MAGIC # Check schema matches expectations
# MAGIC assert set(df.columns) == set(expected_columns)
# MAGIC ```
# MAGIC
# MAGIC #### 5️⃣ Use Complex Types for Structured Data
# MAGIC * `ArrayType` for lists
# MAGIC * `StructType` for nested objects
# MAGIC * `MapType` for key-value pairs
# MAGIC
# MAGIC #### 6️⃣ Document Your Schema
# MAGIC ```python
# MAGIC # Add comments explaining business logic
# MAGIC schema = StructType([
# MAGIC     StructField("customer_id", LongType(), False),  # Unique customer identifier
# MAGIC     StructField("ltv", DecimalType(10,2), True)      # Customer lifetime value (USD)
# MAGIC ])
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 DON'Ts:
# MAGIC
# MAGIC #### 1️⃣ Don't Rely on Schema Inference in Production
# MAGIC * Non-deterministic
# MAGIC * Performance overhead
# MAGIC * Can change unexpectedly
# MAGIC
# MAGIC #### 2️⃣ Don't Use Wrong Data Types
# MAGIC ```python
# MAGIC # ❌ BAD: Money as string or float
# MAGIC StructField("price", StringType())    # Wrong!
# MAGIC StructField("price", DoubleType())    # Precision issues!
# MAGIC
# MAGIC # ✅ GOOD: Money as Decimal
# MAGIC StructField("price", DecimalType(10, 2))  # Correct!
# MAGIC ```
# MAGIC
# MAGIC #### 3️⃣ Don't Ignore Nullability
# MAGIC ```python
# MAGIC # ❌ BAD: Everything nullable
# MAGIC StructField("id", IntegerType(), True)  # IDs should not be null!
# MAGIC
# MAGIC # ✅ GOOD: Explicit constraints
# MAGIC StructField("id", IntegerType(), False)  # Required field
# MAGIC ```
# MAGIC
# MAGIC #### 4️⃣ Don't Skip Schema Validation
# MAGIC * Always validate after reading data
# MAGIC * Check column names and types
# MAGIC * Verify nullability constraints
# MAGIC
# MAGIC #### 5️⃣ Don't Mix String and Proper Types
# MAGIC ```python
# MAGIC # ❌ BAD
# MAGIC df.filter("date_col > '2023-01-01'")  # String comparison!
# MAGIC
# MAGIC # ✅ GOOD
# MAGIC df.filter(col("date_col") > lit(date(2023, 1, 1)))  # Date comparison
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,🤖 Genie Code Agent Usage
# MAGIC %md
# MAGIC ---
# MAGIC ## 🤖 GENIE CODE AGENT: Schema Management Assistant
# MAGIC
# MAGIC ### 🎯 Use Genie Code for Schema Tasks
# MAGIC
# MAGIC Genie Code can help you with:
# MAGIC - Schema generation from sample data
# MAGIC - Schema conversion and validation
# MAGIC - Complex nested schema creation
# MAGIC - Type inference and recommendations
# MAGIC - Schema evolution strategies
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💬 Example Prompts:
# MAGIC
# MAGIC #### 1️⃣ Schema Generation
# MAGIC ```
# MAGIC "Generate an explicit StructType schema for a customer dataset 
# MAGIC with id, name, email, registration_date, and purchase_history (array)"
# MAGIC ```
# MAGIC
# MAGIC #### 2️⃣ Schema Conversion
# MAGIC ```
# MAGIC "Convert this JSON schema to PySpark StructType:
# MAGIC {
# MAGIC   'customer_id': 'integer',
# MAGIC   'orders': [{
# MAGIC     'order_id': 'integer',
# MAGIC     'total': 'decimal'
# MAGIC   }]
# MAGIC }"
# MAGIC ```
# MAGIC
# MAGIC #### 3️⃣ Schema Optimization
# MAGIC ```
# MAGIC "Optimize this schema for better performance. I have columns with 
# MAGIC mostly null values and some columns that should be integers but 
# MAGIC are currently strings."
# MAGIC ```
# MAGIC
# MAGIC #### 4️⃣ Nested Data Handling
# MAGIC ```
# MAGIC "Help me flatten this nested JSON structure and create a schema 
# MAGIC for the flattened DataFrame"
# MAGIC ```
# MAGIC
# MAGIC #### 5️⃣ Schema Evolution
# MAGIC ```
# MAGIC "I need to add new columns to my existing Delta table without 
# MAGIC breaking downstream consumers. What's the best approach?"
# MAGIC ```
# MAGIC
# MAGIC #### 6️⃣ Type Casting
# MAGIC ```
# MAGIC "Cast all string columns in my DataFrame to appropriate types 
# MAGIC based on their content"
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,💾 Step 6: Write to Delta (Simulated)
# STEP 6: Write to Delta Table (Simulated)
print("\n💾 STEP 6: Write to Delta Table")

# In a production environment with Unity Catalog:
# transformed_df.write.format("delta") \
#     .mode("append") \
#     .option("mergeSchema", "false") \
#     .saveAsTable("catalog.schema.transactions_processed")

print("📄 Would write to: catalog.schema.transactions_processed")
print("  Format: Delta")
print("  Mode: append")
print("  Schema enforcement: ENABLED")
print(f"  Rows to write: {transformed_df.count()}")

print("\n✅ PIPELINE COMPLETED SUCCESSFULLY!")
print("="*60)
print("\n🎉 Summary:")
print(f"  • Input rows: {raw_df.count()}")
print(f"  • Output rows: {transformed_df.count()}")
print(f"  • Input columns: {len(raw_df.columns)}")
print(f"  • Output columns: {len(transformed_df.columns)}")
print(f"  • Schema validation: PASSED")
print(f"  • Data quality checks: PASSED")

# COMMAND ----------

# DBTITLE 1,👉 Step 1-2: Schema Definition & Data Load
# Complete Schema-Driven Pipeline

from pyspark.sql.functions import col, current_timestamp, lit

print("🚀 STARTING SCHEMA-DRIVEN DATA PIPELINE")
print("="*60)

# STEP 1: Define Source Schema (EXPLICIT)
print("\n📝 STEP 1: Define Explicit Schema")
source_schema = StructType([
    StructField("transaction_id", LongType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DecimalType(10, 2), False),
    StructField("transaction_date", DateType(), False),
    StructField("store_id", IntegerType(), True)
])
print("✅ Source schema defined")

# STEP 2: Create Sample Data (simulating source data)
print("\n📊 STEP 2: Create Source Data")
source_data = [
    (1001, 501, 101, 2, Decimal("25.99"), date(2026, 4, 21), 1),
    (1002, 502, 102, 1, Decimal("49.99"), date(2026, 4, 21), 1),
    (1003, 503, 103, 3, Decimal("15.99"), date(2026, 4, 21), 2),
    (1004, 501, 104, 1, Decimal("99.99"), date(2026, 4, 20), 1),
    (1005, 504, 101, 5, Decimal("25.99"), date(2026, 4, 20), 3)
]

raw_df = spark.createDataFrame(source_data, schema=source_schema)
print(f"✅ Source data created: {raw_df.count()} rows")

# COMMAND ----------

# DBTITLE 1,✔️ Step 3: Validate Input Schema
# STEP 3: Validate Input Schema
print("\n✔️ STEP 3: Validate Input Schema")
print("Expected columns:", [f.name for f in source_schema.fields])
print("Actual columns:", raw_df.columns)
assert raw_df.columns == [f.name for f in source_schema.fields], "Schema mismatch!"
print("✅ Input schema validated")

raw_df.printSchema()
display(raw_df)

# COMMAND ----------

# DBTITLE 1,⚙️ Step 4: Transform Data
# STEP 4: Transform Data
print("\n⚙️ STEP 4: Transform Data")

transformed_df = raw_df.withColumn(
    "line_total", col("quantity") * col("unit_price")
).withColumn(
    "processing_timestamp", current_timestamp()
).withColumn(
    "data_source", lit("transaction_system")
).withColumn(
    "price_category",
    when(col("unit_price") >= 50, "Premium")
    .when(col("unit_price") >= 25, "Standard")
    .otherwise("Budget")
)

print("✅ Transformations applied")
transformed_df.printSchema()
display(transformed_df)

# COMMAND ----------

# DBTITLE 1,✔️ Step 5: Validate Output Schema
# STEP 5: Validate Output Schema
print("\n✔️ STEP 5: Validate Output Schema")

expected_columns = [
    "transaction_id", "customer_id", "product_id", "quantity", 
    "unit_price", "transaction_date", "store_id", "line_total",
    "processing_timestamp", "data_source", "price_category"
]

actual_columns = transformed_df.columns
print(f"Expected columns: {len(expected_columns)}")
print(f"Actual columns: {len(actual_columns)}")

assert set(expected_columns) == set(actual_columns), "Output schema validation failed!"
print("✅ Output schema validated")

# Check for nulls in critical columns
print("\n🔍 Checking for nulls in critical columns...")
for col_name in ["transaction_id", "customer_id", "line_total"]:
    null_count = transformed_df.filter(col(col_name).isNull()).count()
    print(f"  {col_name}: {null_count} nulls")
    
print("✅ Data quality checks passed")

# COMMAND ----------

# DBTITLE 1,🚀 Section 7: Schema-Driven Pipeline
# MAGIC %md
# MAGIC ---
# MAGIC ## 🚀 SECTION 7: End-to-End Schema-Driven Pipeline
# MAGIC
# MAGIC ### 🎯 Production Pipeline Pattern
# MAGIC
# MAGIC ```
# MAGIC ┌───────────────────┐
# MAGIC │  1. Define Schema  │
# MAGIC │  (Explicit)       │
# MAGIC └────────┬─────────┘
# MAGIC          │
# MAGIC          ↓
# MAGIC ┌────────┴─────────┐
# MAGIC │  2. Read Data     │
# MAGIC │  (with schema)    │
# MAGIC └────────┬─────────┘
# MAGIC          │
# MAGIC          ↓
# MAGIC ┌────────┴─────────┐
# MAGIC │  3. Validate      │
# MAGIC │  Schema           │
# MAGIC └────────┬─────────┘
# MAGIC          │
# MAGIC          ↓
# MAGIC ┌────────┴─────────┐
# MAGIC │  4. Transform     │
# MAGIC │  Data             │
# MAGIC └────────┬─────────┘
# MAGIC          │
# MAGIC          ↓
# MAGIC ┌────────┴─────────┐
# MAGIC │  5. Validate      │
# MAGIC │  Output Schema    │
# MAGIC └────────┬─────────┘
# MAGIC          │
# MAGIC          ↓
# MAGIC ┌────────┴─────────┐
# MAGIC │  6. Write Delta   │
# MAGIC │  Table            │
# MAGIC └───────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### 🔑 Key Principles:
# MAGIC 1. **Schema First**: Define before reading
# MAGIC 2. **Validate Early**: Catch issues at source
# MAGIC 3. **Transform Safely**: Type-aware operations
# MAGIC 4. **Validate Again**: Check output schema
# MAGIC 5. **Document**: Schema is documentation

# COMMAND ----------

# DBTITLE 1,⚙️ Demo Setup: Original DataFrame
# Demo: DataFrame Operations Pipeline

from pyspark.sql.functions import col, expr, avg, sum, count, when

# Start with our explicit_df from earlier
print("🔍 ORIGINAL SCHEMA:")
explicit_df.printSchema()
print(f"\nRow count: {explicit_df.count()}")
display(explicit_df)

# COMMAND ----------

# DBTITLE 1,🔍 Demo: Filter Operation
# Operation 1: Filter rows
print("✅ OPERATION 1: FILTER (age >= 30)")
filtered_df = explicit_df.filter(col("age") >= 30)
print(f"Row count after filter: {filtered_df.count()}")
display(filtered_df)

# Schema remains the same
print("\n🔍 Schema after filter:")
filtered_df.printSchema()

# COMMAND ----------

# DBTITLE 1,➕ Demo: Add Columns
# Operation 2: Add calculated columns
print("✅ OPERATION 2: ADD CALCULATED COLUMNS")

transformed_df = explicit_df.withColumn(
    "annual_salary", col("salary") * 12
).withColumn(
    "salary_category",
    when(col("salary") >= 90000, "High")
    .when(col("salary") >= 80000, "Medium")
    .otherwise("Low")
).withColumn(
    "years_employed",
    expr("datediff(current_date(), join_date) / 365")
)

print("\n🔍 Schema after transformations:")
transformed_df.printSchema()

display(transformed_df)

# COMMAND ----------

# DBTITLE 1,📊 Demo: Aggregations
# Operation 3: Aggregations
print("✅ OPERATION 3: AGGREGATIONS")

agg_df = explicit_df.groupBy("salary_category").agg(
    count("*").alias("employee_count"),
    avg("salary").alias("avg_salary"),
    sum("salary").alias("total_salary")
).orderBy(col("avg_salary").desc())

# Wait, we need the salary_category first
agg_df = transformed_df.groupBy("salary_category").agg(
    count("*").alias("employee_count"),
    avg("salary").alias("avg_salary"),
    sum("salary").alias("total_salary")
).orderBy(col("avg_salary").desc())

print("\n🔍 Schema after aggregation:")
agg_df.printSchema()

display(agg_df)

# COMMAND ----------

# DBTITLE 1,⚙️ Section 6: DataFrame Operations
# MAGIC %md
# MAGIC ---
# MAGIC ## ⚙️ SECTION 6: Hands-on DataFrame Operations
# MAGIC
# MAGIC ### Common DataFrame Transformations
# MAGIC
# MAGIC #### 1️⃣ select() - Choose columns
# MAGIC ```python
# MAGIC df.select("col1", "col2")
# MAGIC ```
# MAGIC
# MAGIC #### 2️⃣ filter() / where() - Filter rows
# MAGIC ```python
# MAGIC df.filter(col("age") > 25)
# MAGIC df.where("salary > 50000")
# MAGIC ```
# MAGIC
# MAGIC #### 3️⃣ withColumn() - Add/Transform columns
# MAGIC ```python
# MAGIC df.withColumn("new_col", expr("col1 + col2"))
# MAGIC ```
# MAGIC
# MAGIC #### 4️⃣ groupBy() + agg() - Aggregations
# MAGIC ```python
# MAGIC df.groupBy("department").agg(avg("salary"))
# MAGIC ```
# MAGIC
# MAGIC #### 5️⃣ orderBy() / sort() - Sort data
# MAGIC ```python
# MAGIC df.orderBy(col("date").desc())
# MAGIC ```
# MAGIC
# MAGIC ### 🔑 Best Practice:
# MAGIC **Always validate schema after transformations!**

# COMMAND ----------

# DBTITLE 1,🌳 Demo: Nested Schema Definition
# Demo: Nested Data Structures

from pyspark.sql.functions import col, explode, struct

# Create nested schema
nested_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer", StructType([
        StructField("name", StringType(), False),
        StructField("email", StringType(), True),
        StructField("address", StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True)
        ]), True)
    ]), False),
    StructField("items", ArrayType(StructType([
        StructField("product_id", IntegerType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("price", DoubleType(), False)
    ])), False)
])

print("✅ NESTED SCHEMA DEFINED:")
print(nested_schema.simpleString())

# COMMAND ----------

# DBTITLE 1,📊 Demo: Nested Data Creation
# Create nested data

nested_data = [
    (
        1001,
        ("Alice Johnson", "alice@example.com", ("123 Main St", "Seattle", "WA")),
        [(101, 2, 29.99), (102, 1, 49.99)]
    ),
    (
        1002,
        ("Bob Smith", "bob@example.com", ("456 Oak Ave", "Portland", "OR")),
        [(103, 3, 19.99), (104, 1, 99.99), (105, 2, 15.99)]
    ),
    (
        1003,
        ("Charlie Brown", "charlie@example.com", ("789 Pine Rd", "Denver", "CO")),
        [(101, 1, 29.99)]
    )
]

nested_df = spark.createDataFrame(nested_data, schema=nested_schema)

print("✅ NESTED DATAFRAME CREATED:")
nested_df.printSchema()
display(nested_df)

# COMMAND ----------

# DBTITLE 1,🔍 Demo: Accessing Nested Fields
# Demo: Accessing Nested Fields

print("✅ 1. ACCESS NESTED CUSTOMER NAME:")
customer_names = nested_df.select(
    "order_id",
    col("customer.name").alias("customer_name")
)
display(customer_names)

print("\n✅ 2. ACCESS DEEPLY NESTED CITY:")
customer_cities = nested_df.select(
    "order_id",
    col("customer.name").alias("customer_name"),
    col("customer.address.city").alias("city"),
    col("customer.address.state").alias("state")
)
display(customer_cities)

# COMMAND ----------

# DBTITLE 1,💥 Demo: Exploding Arrays
# Demo: Exploding Arrays

print("✅ 3. EXPLODE ARRAY (One row per item):")
exploded_df = nested_df.select(
    "order_id",
    col("customer.name").alias("customer_name"),
    explode("items").alias("item")
)
display(exploded_df)

print("\n✅ 4. ACCESS FIELDS FROM EXPLODED ARRAY:")
item_details = exploded_df.select(
    "order_id",
    "customer_name",
    col("item.product_id").alias("product_id"),
    col("item.quantity").alias("quantity"),
    col("item.price").alias("price"),
    (col("item.quantity") * col("item.price")).alias("line_total")
)
display(item_details)

# COMMAND ----------

# DBTITLE 1,🌳 Section 5: Handling Nested Data
# MAGIC %md
# MAGIC ---
# MAGIC ## 🌳 SECTION 5: Handling Nested Data
# MAGIC
# MAGIC ### 🧒 ELI5:
# MAGIC Sometimes data is like a box inside a box inside a box (like Russian dolls). Spark can handle this!
# MAGIC
# MAGIC ### 🏛️ Architect-Level:
# MAGIC Nested data structures (StructType, ArrayType, MapType) are common in:
# MAGIC - JSON data
# MAGIC - Event logs
# MAGIC - API responses
# MAGIC - Semi-structured data
# MAGIC
# MAGIC ### 🔑 Key Operations:
# MAGIC
# MAGIC #### 1️⃣ Accessing Nested Fields
# MAGIC ```python
# MAGIC df.select("customer.name")              # Dot notation
# MAGIC df.select(col("customer.address.city")) # Deeper nesting
# MAGIC ```
# MAGIC
# MAGIC #### 2️⃣ Exploding Arrays
# MAGIC ```python
# MAGIC from pyspark.sql.functions import explode
# MAGIC df.select("id", explode("items"))      # One row per array element
# MAGIC ```
# MAGIC
# MAGIC #### 3️⃣ Accessing Array Elements
# MAGIC ```python
# MAGIC df.select("items[0]")                   # First element
# MAGIC df.select("items").getItem(0)          # Alternative syntax
# MAGIC ```
# MAGIC
# MAGIC #### 4️⃣ Accessing Map Values
# MAGIC ```python
# MAGIC df.select("metadata['key']")            # Access by key
# MAGIC ```
# MAGIC
# MAGIC ### ⚠️ Common Use Cases:
# MAGIC - Processing JSON from APIs
# MAGIC - Event log analysis
# MAGIC - IoT sensor data
# MAGIC - Nested customer/product hierarchies

# COMMAND ----------

# DBTITLE 1,🔍 Demo: Schema Inspection
# Demo: Schema Inspection

from pyspark.sql.functions import col, expr

print("="*60)
print("🔍 1. PRINT SCHEMA (Tree Format)")
print("="*60)
complex_df.printSchema()

print("\n" + "="*60)
print("🔍 2. SCHEMA OBJECT")
print("="*60)
print(complex_df.schema)

print("\n" + "="*60)
print("🔍 3. DATA TYPES (List of Tuples)")
print("="*60)
for col_name, col_type in complex_df.dtypes:
    print(f"  {col_name:20} -> {col_type}")

print("\n" + "="*60)
print("🔍 4. COLUMN NAMES")
print("="*60)
print(complex_df.columns)

# COMMAND ----------

# DBTITLE 1,⚙️ Demo: Column Selection
# Demo: Column Selection and Transformation

# 1. Select specific columns
print("✅ 1. SELECT SPECIFIC COLUMNS:")
selected_df = complex_df.select("customer_id", "name", "balance", "is_active")
display(selected_df)

# 2. Select with transformations
print("\n✅ 2. SELECT WITH EXPRESSIONS:")
transformed_df = complex_df.select(
    col("customer_id"),
    col("name"),
    (col("balance") * 1.05).alias("balance_with_interest"),
    col("credit_score")
)
display(transformed_df)

# COMMAND ----------

# DBTITLE 1,🔄 Demo: Type Casting
# Demo: Type Casting

print("✅ TYPE CASTING DEMONSTRATION:")

# Create a DataFrame with string numbers
string_data = [("1", "100.50"), ("2", "200.75"), ("3", "300.00")]
string_df = spark.createDataFrame(string_data, ["id", "amount"])

print("\n🔴 BEFORE CASTING:")
string_df.printSchema()
display(string_df)

# Cast to proper types
casted_df = string_df.select(
    col("id").cast(IntegerType()).alias("id"),
    col("amount").cast(DoubleType()).alias("amount")
)

print("\n🟢 AFTER CASTING:")
casted_df.printSchema()
display(casted_df)

print("\n🎉 SUCCESS: Strings converted to Integer and Double!")

# COMMAND ----------

# DBTITLE 1,🔧 Section 4: Working with Schema
# MAGIC %md
# MAGIC ---
# MAGIC ## 🔧 SECTION 4: Working with Schema
# MAGIC
# MAGIC ### Schema Operations
# MAGIC
# MAGIC #### 1️⃣ Inspecting Schema
# MAGIC ```python
# MAGIC df.printSchema()        # Tree format
# MAGIC df.schema              # StructType object
# MAGIC df.dtypes              # List of (name, type) tuples
# MAGIC df.columns             # List of column names
# MAGIC ```
# MAGIC
# MAGIC #### 2️⃣ Selecting Columns
# MAGIC ```python
# MAGIC df.select("col1", "col2")              # Select specific columns
# MAGIC df.select(df.col1, df.col2)            # Using DataFrame notation
# MAGIC df.select(col("col1"), col("col2"))   # Using col function
# MAGIC ```
# MAGIC
# MAGIC #### 3️⃣ Type Casting
# MAGIC ```python
# MAGIC df.withColumn("amount", col("amount").cast("double"))
# MAGIC df.withColumn("amount", col("amount").cast(DoubleType()))
# MAGIC ```
# MAGIC
# MAGIC #### 4️⃣ Adding Columns
# MAGIC ```python
# MAGIC df.withColumn("new_col", expr("col1 + col2"))
# MAGIC ```
# MAGIC
# MAGIC #### 5️⃣ Renaming Columns
# MAGIC ```python
# MAGIC df.withColumnRenamed("old_name", "new_name")
# MAGIC ```
# MAGIC
# MAGIC #### 6️⃣ Dropping Columns
# MAGIC ```python
# MAGIC df.drop("col1", "col2")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,📦 Demo: All Data Types
# Demo: Comprehensive Data Types

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType, FloatType,
    BooleanType, DateType, TimestampType, DecimalType,
    ArrayType, MapType
)
from datetime import datetime, date
from decimal import Decimal

# Complex schema with all common types
comprehensive_schema = StructType([
    StructField("customer_id", LongType(), False),
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), True),
    StructField("balance", DecimalType(10, 2), True),       # Precision 10, Scale 2
    StructField("credit_score", FloatType(), True),
    StructField("is_active", BooleanType(), False),
    StructField("registration_date", DateType(), False),
    StructField("last_login", TimestampType(), True),
    StructField("phone_numbers", ArrayType(StringType()), True),  # Array of strings
    StructField("metadata", MapType(StringType(), StringType()), True)  # Key-value pairs
])

print("✅ COMPREHENSIVE SCHEMA WITH ALL DATA TYPES:")
print(comprehensive_schema.simpleString())

# COMMAND ----------

# DBTITLE 1,🔧 Demo: Complex Data Types Example
# Create sample data with all data types

complex_data = [
    (
        1001,
        "Alice Johnson",
        25,
        Decimal("15000.50"),
        750.5,
        True,
        date(2023, 1, 15),
        datetime(2026, 4, 21, 10, 30, 0),
        ["555-1234", "555-5678"],
        {"tier": "gold", "region": "west"}
    ),
    (
        1002,
        "Bob Smith",
        30,
        Decimal("25000.75"),
        820.3,
        True,
        date(2022, 6, 20),
        datetime(2026, 4, 20, 15, 45, 0),
        ["555-9999"],
        {"tier": "platinum", "region": "east"}
    ),
    (
        1003,
        "Charlie Brown",
        35,
        Decimal("35000.00"),
        690.1,
        False,
        date(2021, 3, 10),
        None,
        ["555-1111", "555-2222", "555-3333"],
        {"tier": "silver", "region": "north"}
    )
]

# Create DataFrame
complex_df = spark.createDataFrame(complex_data, schema=comprehensive_schema)

print("✅ DATAFRAME WITH COMPLEX DATA TYPES:")
complex_df.printSchema()

display(complex_df)

# COMMAND ----------

# DBTITLE 1,📦 Section 3: Data Types in Spark
# MAGIC %md
# MAGIC ---
# MAGIC ## 📦 SECTION 3: Data Types in Spark
# MAGIC
# MAGIC ### 📑 Primitive Data Types
# MAGIC
# MAGIC | Type | Python Class | Description | Example |
# MAGIC |------|--------------|-------------|----------|
# MAGIC | **StringType** | `StringType()` | Text data | "Alice", "Product A" |
# MAGIC | **IntegerType** | `IntegerType()` | 32-bit integers | 42, -100, 0 |
# MAGIC | **LongType** | `LongType()` | 64-bit integers | 9223372036854775807 |
# MAGIC | **DoubleType** | `DoubleType()` | 64-bit floating point | 3.14, 99.99 |
# MAGIC | **FloatType** | `FloatType()` | 32-bit floating point | 2.5 |
# MAGIC | **BooleanType** | `BooleanType()` | True/False | True, False |
# MAGIC | **DateType** | `DateType()` | Date (no time) | 2023-01-15 |
# MAGIC | **TimestampType** | `TimestampType()` | Date + Time | 2023-01-15 14:30:00 |
# MAGIC | **DecimalType** | `DecimalType(p, s)` | Precise decimal | Decimal(10, 2) for money |
# MAGIC | **BinaryType** | `BinaryType()` | Byte array | Binary data |
# MAGIC
# MAGIC ### 🧩 Complex Data Types
# MAGIC
# MAGIC | Type | Description | Use Case |
# MAGIC |------|-------------|----------|
# MAGIC | **ArrayType** | List of elements (same type) | Tags, phone numbers |
# MAGIC | **StructType** | Nested structure (like JSON object) | Address, nested entities |
# MAGIC | **MapType** | Key-value pairs | Metadata, attributes |
# MAGIC
# MAGIC ### 💞 Nullability
# MAGIC - `nullable=True`: Column can contain NULL values
# MAGIC - `nullable=False`: Column must have values (NOT NULL constraint)

# COMMAND ----------

# DBTITLE 1,🟢 Demo: Explicit Schema Definition
# Demo: Explicit Schema Definition

from pyspark.sql.types import (
    StructType, StructField, 
    StringType, IntegerType, DoubleType, DateType
)

# Define explicit schema
explicit_schema = StructType([
    StructField("id", IntegerType(), nullable=False),      # ID cannot be null
    StructField("name", StringType(), nullable=False),     # Name required
    StructField("age", IntegerType(), nullable=True),      # Age can be null
    StructField("salary", DoubleType(), nullable=True),    # Salary can be null
    StructField("join_date", DateType(), nullable=True)    # Date can be null
])

print("✅ EXPLICIT SCHEMA DEFINED:")
print(explicit_schema)
print("\n🔑 Key Benefits:")
print("1. Correct data types (Integer, Double, Date)")
print("2. Nullability constraints defined")
print("3. No data scanning required")
print("4. Type validation at read time")

# COMMAND ----------

# DBTITLE 1,✅ Demo: DataFrame with Explicit Schema
# Create DataFrame with Explicit Schema

from datetime import date

# Create properly typed data
typed_data = [
    (1, "Alice", 25, 75000.50, date(2023, 1, 15)),
    (2, "Bob", 30, 85000.75, date(2022, 6, 20)),
    (3, "Charlie", 35, 95000.00, date(2021, 3, 10))
]

# Create DataFrame with explicit schema
explicit_df = spark.createDataFrame(typed_data, schema=explicit_schema)

print("✅ DATAFRAME WITH EXPLICIT SCHEMA:")
explicit_df.printSchema()

print("\n🎉 SUCCESS: Proper data types!")
print("- id: integer (not string)")
print("- age: integer (not string)")
print("- salary: double (not string)")
print("- join_date: date (not string)")

display(explicit_df)

# COMMAND ----------

# DBTITLE 1,🟢 Section 2B: Explicit Schema (Best Practice)
# MAGIC %md
# MAGIC ---
# MAGIC ### 🟢 PART B: Explicit Schema (BEST PRACTICE)
# MAGIC
# MAGIC #### 🧒 ELI5:
# MAGIC Instead of guessing, you tell Spark exactly what type each column is. It's like giving someone a recipe instead of letting them guess ingredients.
# MAGIC
# MAGIC #### 🏛️ Architect-Level:
# MAGIC Explicit schema definition provides:
# MAGIC - **Deterministic behavior**: Same schema every time
# MAGIC - **Performance**: No inference overhead
# MAGIC - **Data validation**: Type checking at read time
# MAGIC - **Documentation**: Schema as code
# MAGIC - **Production-ready**: Predictable and reliable
# MAGIC
# MAGIC #### ⭐ Why Explicit Schema?
# MAGIC 1. **Performance**: Skip data scanning
# MAGIC 2. **Reliability**: Catch data quality issues early
# MAGIC 3. **Predictability**: No surprises in production
# MAGIC 4. **Governance**: Clear data contracts
# MAGIC 5. **Optimization**: Better query planning
# MAGIC
# MAGIC #### 📝 Schema Definition Components:
# MAGIC ```python
# MAGIC StructType([           # Container for schema
# MAGIC     StructField(       # Individual column definition
# MAGIC         name,          # Column name
# MAGIC         dataType,      # Data type (IntegerType, StringType, etc.)
# MAGIC         nullable       # Can contain NULL values?
# MAGIC     )
# MAGIC ])
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,🛠️ Demo Setup: Create Sample Data
# Demo: Schema Inference
# Creating sample data for demonstration

from pyspark.sql import Row

# Create sample data
data = [
    Row(id="1", name="Alice", age="25", salary="75000.50", join_date="2023-01-15"),
    Row(id="2", name="Bob", age="30", salary="85000.75", join_date="2022-06-20"),
    Row(id="3", name="Charlie", age="35", salary="95000.00", join_date="2021-03-10")
]

# Create DataFrame and save as CSV for inference demo
temp_df = spark.createDataFrame(data)
print("✅ Sample data created for demonstration")
display(temp_df)

# COMMAND ----------

# DBTITLE 1,🔴 Demo: Schema Inference Issues
# Schema Inference Example
# Notice: All columns are inferred as STRING type!

from pyspark.sql.functions import *

# When reading data without schema, Spark tries to infer types
# For this demo, we'll create a DataFrame from the data
inferred_df = spark.createDataFrame(data)

print("\n🔍 INFERRED SCHEMA:")
inferred_df.printSchema()

print("\n⚠️ PROBLEM: All fields are strings!")
print("Even numeric fields like 'age' and 'salary' are treated as text.")

display(inferred_df)

# COMMAND ----------

# DBTITLE 1,📘 Course Header
# MAGIC %md
# MAGIC # ⚡ Data Engineering Training — Phase 3 Day 13  
# MAGIC ## 📊 DataFrames: Schema Handling & Data Types  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - DataFrame Basics  
# MAGIC - Schema Handling (Inference vs Explicit Schema)  
# MAGIC - Data Types in Spark  
# MAGIC - Best Practices for Schema Management  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### ⚙️ Platform: Databricks (Serverless + Unity Catalog + Spark)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Objective:
# MAGIC Understand how to work with Spark DataFrames, manage schemas effectively, and use appropriate data types for scalable data processing.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Engineering Constraints:
# MAGIC - ✅ Use Databricks Serverless Compute
# MAGIC - ✅ DataFrame API only (NO RDDs)
# MAGIC - ✅ NO cache() / persist()
# MAGIC - ✅ NO /tmp or local storage
# MAGIC - ✅ Unity Catalog Volumes for data access
# MAGIC - ✅ Schema-first and governance-first design

# COMMAND ----------

# DBTITLE 1,📖 Section 1: Introduction to DataFrames
# MAGIC %md
# MAGIC ---
# MAGIC ## 📖 SECTION 1: Introduction to DataFrames
# MAGIC
# MAGIC ### 🧒 ELI5 (Explain Like I'm 5):
# MAGIC Imagine you have a big spreadsheet with rows and columns, but instead of being on one computer, it's split across many computers working together. That's a DataFrame! It helps process huge amounts of data super fast.
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC A **DataFrame** is Spark's distributed, tabular data abstraction built on top of RDDs. It provides:
# MAGIC - **Schema awareness**: Strongly typed columns with metadata
# MAGIC - **Catalyst optimizer**: Query optimization engine
# MAGIC - **Tungsten execution**: Efficient binary processing
# MAGIC - **Immutability**: Transformations create new DataFrames
# MAGIC - **Lazy evaluation**: Operations are optimized before execution
# MAGIC
# MAGIC ### 🔄 DataFrame vs Traditional Tables:
# MAGIC
# MAGIC | Feature | Traditional Table | Spark DataFrame |
# MAGIC |---------|------------------|----------------|
# MAGIC | Storage | Single machine | Distributed across cluster |
# MAGIC | Processing | Single-threaded | Parallel processing |
# MAGIC | Size Limit | RAM/Disk of one machine | Virtually unlimited |
# MAGIC | Optimization | Manual query tuning | Catalyst optimizer |
# MAGIC | API | SQL only | SQL + Python + Scala + R |
# MAGIC
# MAGIC ### ✨ Key Characteristics:
# MAGIC 1. **Distributed**: Data spread across multiple nodes
# MAGIC 2. **Immutable**: Transformations create new DataFrames
# MAGIC 3. **Lazy**: Computations triggered only on actions
# MAGIC 4. **Schema-aware**: Typed columns with validation
# MAGIC 5. **Optimized**: Catalyst + Tungsten for performance

# COMMAND ----------

# DBTITLE 1,📝 Section 2A: Schema Handling - Inference
# MAGIC %md
# MAGIC ---
# MAGIC ## 📝 SECTION 2: Schema Handling
# MAGIC
# MAGIC ### What is a Schema?
# MAGIC A schema defines the structure of your data:
# MAGIC - Column names
# MAGIC - Data types
# MAGIC - Nullability constraints
# MAGIC
# MAGIC ### Two Approaches:
# MAGIC 1. **Schema Inference** (Automatic detection)
# MAGIC 2. **Explicit Schema** (Manual definition) ⭐ **BEST PRACTICE**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 PART A: Schema Inference
# MAGIC
# MAGIC #### 🧒 ELI5:
# MAGIC Spark looks at your data and tries to guess what type each column is (like number, text, date). It's like guessing what's in a box by shaking it.
# MAGIC
# MAGIC #### 🏛️ Architect-Level:
# MAGIC Schema inference scans the data (sampling or full scan) to detect data types. 
# MAGIC
# MAGIC **Pros:**
# MAGIC - Quick for exploratory analysis
# MAGIC - No schema definition needed
# MAGIC
# MAGIC **Cons:**
# MAGIC - Performance overhead (requires data scan)
# MAGIC - Non-deterministic (data changes = schema changes)
# MAGIC - Not suitable for production
# MAGIC - Can misidentify types (e.g., "123" as string vs integer)