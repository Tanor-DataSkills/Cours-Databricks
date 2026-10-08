# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ⚡ Data Engineering Training — Phase 3 Day 15  
# MAGIC ## 🔄 Spark Transformations: Select, Filter, WithColumn & Aggregations  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - Column Selection (select)  
# MAGIC - Row Filtering (filter / where)  
# MAGIC - Column Creation (withColumn)  
# MAGIC - groupBy & Aggregations  
# MAGIC - Transformation Optimization Basics  
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
# MAGIC Learn how to transform data using Spark DataFrame APIs and build efficient transformation pipelines using best practices.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Engineering Constraints:
# MAGIC - ✅ Use Databricks Serverless Compute
# MAGIC - ✅ DataFrame API only (NO RDDs)
# MAGIC - ✅ NO cache() / persist()
# MAGIC - ✅ NO /tmp or local storage
# MAGIC - ✅ Unity Catalog Volumes for all data access
# MAGIC - ✅ Transformation-efficient design

# COMMAND ----------

# DBTITLE 1,Section 1: Introduction to Transformations
# MAGIC %md
# MAGIC # 📚 Section 1: Introduction to Transformations
# MAGIC
# MAGIC ## 🧠 What are Transformations?
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC Transformations are like **instructions** you give to Spark to change your data. Think of it like giving directions: "take this table, pick only these columns, filter out bad rows, and add a new column." The cool thing? Spark doesn't actually **do** the work until you ask for results!
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC Transformations are **lazy operations** that build a **Directed Acyclic Graph (DAG)** of execution plans. They return new DataFrames without modifying the original. Transformations are only executed when an **action** (like `show()`, `count()`, `write()`) is called.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⏱️ Lazy Evaluation (Recap)
# MAGIC
# MAGIC **Key Concept:** Spark doesn't execute transformations immediately. It builds an execution plan and optimizes it before running.
# MAGIC
# MAGIC **Benefits:**
# MAGIC - ✅ Query optimization (Catalyst Optimizer)
# MAGIC - ✅ Predicate pushdown
# MAGIC - ✅ Column pruning
# MAGIC - ✅ Reduced shuffles
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔀 Narrow vs Wide Transformations
# MAGIC
# MAGIC ### 🔹 Narrow Transformations:
# MAGIC - Each input partition contributes to **at most one output partition**
# MAGIC - **No data shuffle** across the cluster
# MAGIC - Examples: `select()`, `filter()`, `withColumn()`
# MAGIC - ⚡ **Fast and efficient**
# MAGIC
# MAGIC ### 🔶 Wide Transformations:
# MAGIC - Input partitions contribute to **multiple output partitions**
# MAGIC - **Requires data shuffle** across the network
# MAGIC - Examples: `groupBy()`, `join()`, `orderBy()`, `repartition()`
# MAGIC - ⏱️ **Slower, requires network I/O**
# MAGIC
# MAGIC **Optimization Tip:** Minimize wide transformations and apply narrow transformations (filters) early!

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_date()

# COMMAND ----------

# how to import functions into current session
from pyspark.sql.functions import current_date # importing individual functions 
from pyspark.sql.functions import * #importing all functions from pyspark.sql.functions class
# pyspark is library
# sql is module 
# functions is class 
# PL hierarchy
# library->modules->classes->methods(functions)
# pyspark.sql.functions or pyspark.sql.types  (pyspark is library and sql is module and types is class)
# pyspark.ml (pyspark is library and ml is module)

# how to import data types in pyspark 
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType
# how to import all data types in current session
from pyspark.sql.types import *
# for window functions we have separate class 
from pyspark.sql.window import * # importing window functions class (ranking ,analytical,value functions)
# datetime is python library for date and time or timestamp related functions 
from datetime import * # this is python library not spark library


# COMMAND ----------

# DBTITLE 1,Setup: Import Libraries
# Import necessary libraries
from pyspark.sql.functions import col, lit, when, sum, avg, count, max, min, round as spark_round
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from datetime import date

print("✅ Libraries imported successfully!")

# COMMAND ----------

# DBTITLE 1,Create Sample Dataset
# Create sample sales dataset
data = [
    (1, "Laptop", "Electronics", 1200.50, "2026-01-15", "Store_A"),
    (2, "Mouse", "Electronics", 25.99, "2026-01-16", "Store_B"),
    (3, "Desk", "Furniture", 350.00, "2026-01-17", "Store_A"),
    (4, "Chair", "Furniture", 150.00, "2026-01-18", "Store_C"),
    (5, "Keyboard", "Electronics", 75.50, "2026-01-19", "Store_B"),
    (6, "Monitor", "Electronics", 300.00, "2026-01-20", "Store_A"),
    (7, "Lamp", "Furniture", 45.00, "2026-01-21", "Store_C"),
    (8, "Notebook", "Stationery", 5.99, "2026-01-22", "Store_B"),
    (9, "Pen Set", "Stationery", 12.50, "2026-01-23", "Store_A"),
    (10, "Bookshelf", "Furniture", 200.00, "2026-01-24", "Store_C")
]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("sale_date", StringType(), True),
    StructField("store", StringType(), True)
])

df = spark.createDataFrame(data, schema)

print("\u2705 Sample dataset created successfully!")
print(f"Total records: {df.count()}")
print("\n👇 Sample data:")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dataframe Actions
# MAGIC * Displying data
# MAGIC - df.show() # dataframe function to display data in text format 
# MAGIC - df.collect() # dataframe function to display data in array
# MAGIC - df.display() #dataframe function to diaplay data in html table
# MAGIC - display(df) # notebook function
# MAGIC #### Save the data 
# MAGIC - df.write.mode(append/overwrite).save(path) # saving in location 
# MAGIC - df.write.mode(append/overwrite).saveAsTable(catalog.schema.table) # saving in unity catalog table inside schema.

# COMMAND ----------

df.head()

# COMMAND ----------

# saving data in delta format in volume location using save option
df.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/processed_data/products")
# saving data in delta format in uc managed table using saveAsTable option 
df.write.format("delta").mode("overwrite").saveAsTable("workspace.default.products")

# COMMAND ----------

# query by path 
df1 = spark.read.format("delta").load("/Volumes/workspace/default/processed_data/products")
df2 = spark.table("workspace.default.products")
display(df1)
# query by metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC -- query by path 
# MAGIC
# MAGIC --select * from delta.`/Volumes/workspace/default/processed_data/products`
# MAGIC -- query by metastore 
# MAGIC select * from workspace.default.products

# COMMAND ----------

df.columns

# COMMAND ----------

# on top of dataframe we add a column,we can select column,we can display data,we can filter data ,we can get columns,we can get columns with datatypes, these functions are divided into multiple based on functionality
# narrow transformations
# wide transformations 
# actions (displaying data(it will run job) and displaying metadata (it wont run any job))
# dataframe default stores metadata only in-memory. Data stores while doing an action

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dataframe Trasformations
# MAGIC - df.withColumn -- for adding a new column
# MAGIC - df.drop() -- for remving column/columns
# MAGIC - df.withColumnRenamed -- for renaming column
# MAGIC - df.select() -- for selecting required columns
# MAGIC - df.selectExpr() -- for selecting existing columns or adding new column with expressions
# MAGIC - df.fillna() -- converting null values to actual values
# MAGIC - df.dropna() -- removing null rows 
# MAGIC - df.distinct() -- removing duplciate rows for all columns combination
# MAGIC - df.dropDuplicates([cols]) -- removing duplicate rows for specific column combination
# MAGIC - df.filter()/df.where() -- for filtering rows based on condition
# MAGIC - df.orderBy() -- sorting data in ascending or descending order 
# MAGIC - df.joins() --- joining dataframes
# MAGIC - df.union()/df.intersect()/df.minus() -- set operators for merge same structure dataframes 
# MAGIC - df.groupBy().agg(sum,min,max,avg,count,stdev,sum)
# MAGIC - df.repartition(n) --for increasing or descreasing no of partitions 
# MAGIC - df.coalesce(n) -- for descreasing no of partitions

# COMMAND ----------

help(df.groupBy)

# COMMAND ----------

df_airlines = spark.read.format("csv").load("/databricks-datasets/airlines/part-00000",header=True,nullValue="NA",inferSchema=True)
df_airlines.display()

# COMMAND ----------

df_red_wine = spark.read.format("csv").load("dbfs:/databricks-datasets/wine-quality/winequality-red.csv",sep=";",header=True,inferSchema=True)
df_red_wine.display()

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/

# COMMAND ----------

df3 = df.selectExpr("id","product_name","amount","current_date() as CREATED_DATE","amount+1000 as New_Amount")
df3.display()

# COMMAND ----------

df.where("category = 'Electronics'").display()

# COMMAND ----------

from pyspark.sql.functions import current_date
df1 = (df.withColumn("CREATED_DATE",current_date())
       .withColumnRenamed("id","PRODUCT_ID")
       .drop("product_name")
       .orderBy("amount")
       .groupBy("store").agg(sum("amount").alias("TOTAL_SALES"),min("amount").alias("MIN_SALES"),max("amount").alias("MAX_SALES"),avg("amount").alias("AVG_SALES"),count("amount").alias("COUNT_SALES"))
)
df1.display()

# COMMAND ----------

# DBTITLE 1,Section 2: select() - Column Selection
# MAGIC %md
# MAGIC # 🎯 Section 2: select() - Column Selection
# MAGIC
# MAGIC ## 🧠 What is select()?
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC Imagine you have a huge table with 50 columns, but you only need 3 of them. The `select()` function lets you **pick just the columns you want**, like choosing specific ingredients from a recipe.
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC `select()` is a **narrow transformation** that performs **column pruning** at the Catalyst optimizer level. It reduces memory footprint, network I/O, and improves query performance by eliminating unnecessary data early in the execution plan.
# MAGIC
# MAGIC **Best Practice:** Always select only the columns you need — this is called **projection pushdown**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Usage Patterns:
# MAGIC
# MAGIC 1. **Select specific columns by name**
# MAGIC 2. **Select using column expressions**
# MAGIC 3. **Select with transformations**
# MAGIC 4. **Select with aliases**

# COMMAND ----------

# DBTITLE 1,Demo: Basic select()
# Pattern 1: Select specific columns by name
print("🔹 Pattern 1: Select specific columns")
df_selected = df.select("id", "product_name", "amount")
display(df_selected)

# COMMAND ----------

# DBTITLE 1,Demo: select() with Column Expressions
# Pattern 2: Select using col() function
print("🔹 Pattern 2: Using col() for column expressions")
df_col_expr = df.select(
    col("product_name"),
    col("amount"),
    col("category")
)
display(df_col_expr)

# COMMAND ----------

# DBTITLE 1,Demo: select() with Transformations
# Pattern 3: Select with transformations
print("🔹 Pattern 3: Select with inline transformations")
df_transformed = df.select(
    col("product_name"),
    col("amount"),
    (col("amount") * 2).alias("amount_doubled"),
    (col("amount") * 0.1).alias("tax_amount")
)
display(df_transformed)

# COMMAND ----------

# DBTITLE 1,Demo: select() with Aliases
# Pattern 4: Select with aliases for cleaner column names
print("🔹 Pattern 4: Using aliases for better readability")
df_aliased = df.select(
    col("id").alias("product_id"),
    col("product_name").alias("name"),
    col("amount").alias("price_usd"),
    col("category").alias("product_category")
)
display(df_aliased)

# COMMAND ----------

# DBTITLE 1,Section 3: filter() / where() - Row Filtering
# MAGIC %md
# MAGIC # 🔍 Section 3: filter() / where() - Row Filtering
# MAGIC
# MAGIC ## 🧠 What is filter() / where()?
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC Filtering is like using a **strainer**. You pour data through it, and only the rows that match your condition pass through. For example: "Show me only products that cost more than $100."
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC `filter()` and `where()` are **narrow transformations** that apply **predicate pushdown** optimization. The Catalyst optimizer pushes filter conditions as close to the data source as possible, minimizing data read and processed.
# MAGIC
# MAGIC **Performance Note:** Filters are executed **before** shuffles in wide transformations, significantly reducing data movement.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Usage Patterns:
# MAGIC
# MAGIC 1. **Simple condition filtering**
# MAGIC 2. **Multiple conditions (AND/OR)**
# MAGIC 3. **SQL-style string expressions**
# MAGIC 4. **Complex boolean logic**

# COMMAND ----------

# DBTITLE 1,Demo: Basic filter()
# Pattern 1: Simple condition - Products with amount > 100
print("🔹 Pattern 1: Filter products with amount > 100")
df_filtered = df.filter(col("amount") > 100)
display(df_filtered)

# COMMAND ----------

# DBTITLE 1,Demo: filter() with Multiple Conditions (AND)
# Pattern 2: Multiple conditions using AND (&)
print("🔹 Pattern 2: Electronics products with amount > 50")
df_filtered_and = df.filter(
    (col("category") == "Electronics") & (col("amount") > 50)
)
display(df_filtered_and)

# COMMAND ----------

# DBTITLE 1,Demo: filter() with Multiple Conditions (OR)
# Pattern 3: Multiple conditions using OR (|)
print("🔹 Pattern 3: Electronics OR Furniture products")
df_filtered_or = df.filter(
    (col("category") == "Electronics") | (col("category") == "Furniture")
)
display(df_filtered_or)

# COMMAND ----------

# DBTITLE 1,Demo: where() with SQL String Expression
# Pattern 4: Using where() with SQL-style string
print("🔹 Pattern 4: SQL-style where clause")
df_where = df.where("amount > 100 AND category = 'Electronics'")
display(df_where)

# COMMAND ----------

# DBTITLE 1,Demo: Complex Boolean Logic
# Pattern 5: Complex filtering with NOT (~)
print("🔹 Pattern 5: Products NOT in Store_A with amount between 50 and 500")
df_complex = df.filter(
    (~(col("store") == "Store_A")) & 
    (col("amount") >= 50) & 
    (col("amount") <= 500)
)
display(df_complex)

# COMMAND ----------

# DBTITLE 1,Section 4: withColumn() - Column Transformation
# MAGIC %md
# MAGIC # ➕ Section 4: withColumn() - Column Transformation
# MAGIC
# MAGIC ## 🧠 What is withColumn()?
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC `withColumn()` is like adding a **new ingredient** to your recipe or **replacing** an existing one. You can create new columns based on calculations or modify existing columns.
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC `withColumn()` is a **narrow transformation** that adds or replaces a single column. While convenient, **chaining multiple `withColumn()` calls can be inefficient** as each call creates a new DataFrame object. 
# MAGIC
# MAGIC **Best Practice:** Use `select()` with multiple expressions or `withColumns()` (plural) for adding multiple columns at once.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Usage Patterns:
# MAGIC
# MAGIC 1. **Add new column with calculation**
# MAGIC 2. **Modify existing column**
# MAGIC 3. **Conditional column creation (when/otherwise)**
# MAGIC 4. **Multiple columns efficiently**

# COMMAND ----------

# DBTITLE 1,Demo: Add New Column
# Pattern 1: Add new column with calculation
print("🔹 Pattern 1: Add tax_amount column (10% of amount)")
df_with_tax = df.withColumn("tax_amount", col("amount") * 0.1)
df_with_tax = df_with_tax.withColumn("total_with_tax", col("amount") + col("tax_amount"))

print("Columns:", df_with_tax.columns)
display(df_with_tax.select("product_name", "amount", "tax_amount", "total_with_tax"))

# COMMAND ----------

# DBTITLE 1,Demo: Modify Existing Column
# Pattern 2: Modify existing column - round amount to nearest integer
print("🔹 Pattern 2: Round amount to nearest dollar")
df_rounded = df.withColumn("amount", spark_round(col("amount"), 0))
display(df_rounded.select("product_name", "amount"))

# COMMAND ----------

# DBTITLE 1,Demo: Conditional Column with when()
# Pattern 3: Conditional column creation using when/otherwise
print("🔹 Pattern 3: Create price_category based on amount")
df_categorized = df.withColumn(
    "price_category",
    when(col("amount") < 50, "Low")
    .when((col("amount") >= 50) & (col("amount") < 200), "Medium")
    .otherwise("High")
)
display(df_categorized.select("product_name", "amount", "price_category"))

# COMMAND ----------

# DBTITLE 1,Demo: Efficient Multiple Column Addition
# Pattern 4: Add multiple columns efficiently using select()
print("🔹 Pattern 4: Add multiple columns in one operation")

# ❌ BAD: Chaining multiple withColumn calls
# df_bad = df.withColumn("col1", ...).withColumn("col2", ...).withColumn("col3", ...)

# ✅ GOOD: Using select() with all transformations
df_efficient = df.select(
    "*",  # Keep all existing columns
    (col("amount") * 0.1).alias("tax_amount"),
    (col("amount") * 1.1).alias("total_with_tax"),
    (col("amount") * 0.2).alias("discount_20_percent")
)

print("All columns added in single operation!")
display(df_efficient)

# COMMAND ----------

# DBTITLE 1,Section 5: groupBy() & Aggregations
# MAGIC %md
# MAGIC # 📦 Section 5: groupBy() & Aggregations
# MAGIC
# MAGIC ## 🧠 What is groupBy()?
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC Imagine sorting your toys into boxes by type: all cars in one box, all dolls in another. `groupBy()` does the same with data — it **groups rows** that have the same value, then lets you calculate things like "how many cars?" or "average price of dolls?"
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC `groupBy()` is a **wide transformation** that triggers a **shuffle operation**. Data with the same key is redistributed across partitions. The Catalyst optimizer can apply **partial aggregations** before the shuffle to reduce data movement.
# MAGIC
# MAGIC **Performance Impact:** Shuffles are expensive. Minimize groupBy keys and apply filters **before** grouping.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Common Aggregation Functions:
# MAGIC
# MAGIC * `count()` - Count rows
# MAGIC * `sum()` - Sum of values
# MAGIC * `avg()` - Average of values
# MAGIC * `min()` / `max()` - Minimum/Maximum
# MAGIC * `countDistinct()` - Count unique values

# COMMAND ----------

# DBTITLE 1,Demo: Basic groupBy with count()
# Pattern 1: Count products by category
print("🔹 Pattern 1: Count products by category")
df_count = df.groupBy("category").count()
display(df_count.orderBy(col("count").desc()))

# COMMAND ----------

# DBTITLE 1,Demo: groupBy with sum() and avg()
# Pattern 2: Sum and average amount by category
print("🔹 Pattern 2: Total sales and average price by category")
df_agg = df.groupBy("category").agg(
    sum("amount").alias("total_sales"),
    avg("amount").alias("avg_price"),
    count("*").alias("product_count")
)
display(df_agg.orderBy(col("total_sales").desc()))

# COMMAND ----------

# DBTITLE 1,Demo: groupBy with Multiple Aggregations
# Pattern 3: Multiple aggregations on the same column
print("🔹 Pattern 3: Min, Max, Avg by category")
df_stats = df.groupBy("category").agg(
    min("amount").alias("min_price"),
    max("amount").alias("max_price"),
    avg("amount").alias("avg_price"),
    spark_round(avg("amount"), 2).alias("avg_price_rounded")
)
display(df_stats)

# COMMAND ----------

# DBTITLE 1,Demo: groupBy with Multiple Keys
# Pattern 4: Group by multiple columns
print("🔹 Pattern 4: Sales by category and store")
df_multi_group = df.groupBy("category", "store").agg(
    sum("amount").alias("total_sales"),
    count("*").alias("num_products")
)
display(df_multi_group.orderBy("category", "store"))

# COMMAND ----------

# DBTITLE 1,Demo: Aggregation Without groupBy
# Pattern 5: Global aggregations (no grouping)
print("🔹 Pattern 5: Overall statistics across all products")
df_global = df.agg(
    count("*").alias("total_products"),
    sum("amount").alias("total_revenue"),
    avg("amount").alias("avg_price"),
    min("amount").alias("min_price"),
    max("amount").alias("max_price")
)
display(df_global)

# COMMAND ----------

# DBTITLE 1,Section 6: Combining Transformations
# MAGIC %md
# MAGIC # 🔗 Section 6: Combining Transformations
# MAGIC
# MAGIC ## 🧠 Transformation Chaining
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC You can **chain** transformations together like building blocks. First select, then filter, then add columns, then group — all in one smooth pipeline!
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC Spark's **fluent API** allows method chaining. The Catalyst optimizer analyzes the entire chain and creates an optimized physical plan. The key is to order transformations strategically:
# MAGIC
# MAGIC 1. **Filter early** (reduce data volume)
# MAGIC 2. **Project early** (select only needed columns)
# MAGIC 3. **Aggregate last** (after data reduction)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Best Practice Transformation Order:
# MAGIC
# MAGIC ```
# MAGIC read → filter → select → withColumn → groupBy/agg → write
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Demo: Chained Transformation Pipeline
# Complete transformation pipeline
print("🔹 Chained Transformation Pipeline")
print("Steps: filter → select → withColumn → groupBy → agg")
print()

result = df \
    .filter(col("amount") > 50) \
    .select("category", "amount", "store") \
    .withColumn("amount_with_tax", col("amount") * 1.1) \
    .groupBy("category") \
    .agg(
        count("*").alias("product_count"),
        sum("amount").alias("total_sales"),
        sum("amount_with_tax").alias("total_with_tax"),
        spark_round(avg("amount"), 2).alias("avg_price")
    ) \
    .orderBy(col("total_sales").desc())

print("✅ Pipeline executed successfully!")
display(result)

# COMMAND ----------

# DBTITLE 1,Section 7: Hands-on Transformation Pipeline
# MAGIC %md
# MAGIC # 🛠️ Section 7: Hands-on Transformation Pipeline
# MAGIC
# MAGIC ## 🎯 Objective:
# MAGIC Build a complete ETL pipeline using Unity Catalog Volumes:
# MAGIC
# MAGIC 1. Read data from Unity Catalog Volume
# MAGIC 2. Apply select
# MAGIC 3. Apply filter
# MAGIC 4. Add column using withColumn
# MAGIC 5. Aggregate using groupBy
# MAGIC 6. Write as Delta table
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💾 Unity Catalog Volumes Setup:
# MAGIC
# MAGIC For this demo, we'll:
# MAGIC * Write sample data to a Volume path
# MAGIC * Read it back
# MAGIC * Apply transformations
# MAGIC * Write output as Delta

# COMMAND ----------

# DBTITLE 1,Setup: Define Unity Catalog Volume Paths
# Define Unity Catalog Volume paths
# Note: Replace with your actual catalog/schema/volume names

catalog_name = "main"  # Your catalog name
schema_name = "default"  # Your schema name
volume_name = "training_data"  # Your volume name

# Construct volume paths
input_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/sales_data"
output_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/processed_sales"

print(f"✅ Volume paths configured:")
print(f"Input: {input_path}")
print(f"Output: {output_path}")

# COMMAND ----------

# DBTITLE 1,Step 1: Write Sample Data to Volume
# Write the sample DataFrame to Unity Catalog Volume as Parquet
print("💾 Step 1: Writing sample data to Unity Catalog Volume...")

try:
    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .save(input_path)
    
    print(f"✅ Data written successfully to {input_path}")
except Exception as e:
    print(f"⚠️ Note: If volume doesn't exist, you may need to create it first.")
    print(f"Error: {str(e)}")
    print("\nContinuing with in-memory DataFrame for demonstration...")

# COMMAND ----------

# DBTITLE 1,Step 2-6: Complete Transformation Pipeline
# Complete ETL Pipeline
print("🔄 Complete Transformation Pipeline")
print("="*60)

try:
    # Step 2: Read from Volume
    print("\n📝 Step 2: Reading data from Volume...")
    df_source = spark.read.format("parquet").load(input_path)
    print(f"   Records read: {df_source.count()}")
    
except:
    # Fallback to in-memory DataFrame if volume read fails
    print("   Using in-memory DataFrame for demo...")
    df_source = df

# Step 3: Select required columns
print("\n🎯 Step 3: Selecting required columns...")
df_selected = df_source.select("product_name", "category", "amount", "store")

# Step 4: Filter high-value products (amount > 50)
print("\n🔍 Step 4: Filtering products with amount > 50...")
df_filtered = df_selected.filter(col("amount") > 50)
print(f"   Records after filter: {df_filtered.count()}")

# Step 5: Add calculated columns
print("\n➕ Step 5: Adding calculated columns...")
df_enriched = df_filtered.select(
    "*",
    (col("amount") * 0.1).alias("tax_amount"),
    (col("amount") * 1.1).alias("total_with_tax")
)

# Step 6: Aggregate by category and store
print("\n📦 Step 6: Aggregating by category and store...")
df_aggregated = df_enriched.groupBy("category", "store").agg(
    count("*").alias("product_count"),
    spark_round(sum("amount"), 2).alias("total_sales"),
    spark_round(sum("total_with_tax"), 2).alias("total_with_tax"),
    spark_round(avg("amount"), 2).alias("avg_price")
).orderBy("category", col("total_sales").desc())

print("\n✅ Transformation pipeline completed!")
print("\n👇 Final aggregated results:")
display(df_aggregated)

# COMMAND ----------

# DBTITLE 1,Section 8: Optimization Best Practices
# MAGIC %md
# MAGIC # ⚡ Section 8: Optimization Best Practices
# MAGIC
# MAGIC ## 🎯 Key Optimization Principles:
# MAGIC
# MAGIC ### 1️⃣ **Push Filters Early**
# MAGIC Apply filters as early as possible to reduce data volume before expensive operations.
# MAGIC
# MAGIC ### 2️⃣ **Avoid Unnecessary Columns**
# MAGIC Select only columns you need. Reduces memory and I/O.
# MAGIC
# MAGIC ### 3️⃣ **Reduce Data Before Aggregation**
# MAGIC Filter and project before groupBy to minimize shuffle data.
# MAGIC
# MAGIC ### 4️⃣ **Avoid Excessive withColumn Chaining**
# MAGIC Use `select()` with multiple expressions instead.
# MAGIC
# MAGIC ### 5️⃣ **Leverage Predicate Pushdown**
# MAGIC Filters on partitioned columns are pushed to storage layer.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Bad vs ✅ Good Patterns

# COMMAND ----------

# DBTITLE 1,Anti-Pattern: Inefficient Transformation
# ❌ BAD: Inefficient transformation flow
print("❌ ANTI-PATTERN: Inefficient approach")
print("Issues: ")
print("  - groupBy before filter (processes more data)")
print("  - Selects all columns unnecessarily")
print("  - Applies filter after expensive aggregation")
print()

# This is what NOT to do (for demonstration only)
bad_result = df \
    .groupBy("category", "store") \
    .agg(sum("amount").alias("total_sales")) \
    .filter(col("total_sales") > 100)

print("Result (but inefficient):")
display(bad_result)

# COMMAND ----------

# DBTITLE 1,Optimized Pattern: Efficient Transformation
# ✅ GOOD: Optimized transformation flow
print("✅ OPTIMIZED PATTERN: Efficient approach")
print("Improvements: ")
print("  - Filter BEFORE groupBy (reduces data to aggregate)")
print("  - Select only required columns early")
print("  - Minimizes shuffle data volume")
print()

# Optimized version
good_result = df \
    .filter(col("amount") > 50) \
    .select("category", "store", "amount") \
    .groupBy("category", "store") \
    .agg(sum("amount").alias("total_sales")) \
    .filter(col("total_sales") > 100) \
    .orderBy(col("total_sales").desc())

print("Result (optimized):")
display(good_result)

# COMMAND ----------

# DBTITLE 1,Comparison: withColumn Chaining
# Comparison: Multiple withColumn vs select

print("❌ BAD: Chaining multiple withColumn calls")
print("   Creates intermediate DataFrame objects")
print()

# Anti-pattern (avoid this)
# df_bad = df.withColumn("col1", expr1) \
#            .withColumn("col2", expr2) \
#            .withColumn("col3", expr3)

print("✅ GOOD: Single select with all transformations")
print("   Single DataFrame transformation")
print()

df_optimized = df.select(
    "*",
    (col("amount") * 0.1).alias("tax"),
    (col("amount") * 1.1).alias("with_tax"),
    when(col("amount") > 100, "High").otherwise("Low").alias("price_tier")
)

print("All columns added efficiently:")
display(df_optimized.limit(5))

# COMMAND ----------

# DBTITLE 1,Section 9: End-to-End Transformation Pipeline
# MAGIC %md
# MAGIC # 🏁 Section 9: End-to-End Transformation Pipeline
# MAGIC
# MAGIC ## 🎯 Complete Production Pipeline
# MAGIC
# MAGIC Let's build a production-ready transformation pipeline that:
# MAGIC
# MAGIC 1. 📝 Reads from source
# MAGIC 2. 🔍 Applies filters early
# MAGIC 3. 🎯 Selects required columns
# MAGIC 4. ➕ Enriches with calculated fields
# MAGIC 5. 📦 Aggregates data
# MAGIC 6. 💾 Writes to Delta table
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Design Principle:
# MAGIC ```
# MAGIC Source Data → Early Filtering → Projection → Transformation → Aggregation → Delta Output
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Production Pipeline: Complete Implementation
# 🎯 Production-Grade Transformation Pipeline
# Watermark: @TRRaveendra

print("🚀 Starting Production Transformation Pipeline")
print("="*70)

# Configuration
catalog = "main"
schema = "default"
volume = "training_data"
output_table = f"{catalog}.{schema}.sales_summary"

print(f"\n📊 Pipeline Configuration:")
print(f"  Catalog: {catalog}")
print(f"  Schema: {schema}")
print(f"  Output Table: {output_table}")

# Step 1: Source Data (using in-memory for demo)
print(f"\n📝 Step 1: Loading source data...")
df_source = df
print(f"   Source records: {df_source.count()}")

# Step 2: Early Filtering (narrow transformation)
print(f"\n🔍 Step 2: Applying early filters...")
df_filtered = df_source.filter(
    (col("amount") > 10) &  # Remove low-value items
    (col("category").isNotNull())  # Remove null categories
)
print(f"   Records after filter: {df_filtered.count()}")

# Step 3: Column Projection (select only needed columns)
print(f"\n🎯 Step 3: Selecting required columns...")
df_projected = df_filtered.select(
    "category",
    "store",
    "amount",
    "sale_date"
)

# Step 4: Enrichment (add calculated columns efficiently)
print(f"\n➕ Step 4: Adding calculated fields...")
df_enriched = df_projected.select(
    "*",
    (col("amount") * 0.08).alias("sales_tax"),
    (col("amount") * 1.08).alias("total_amount"),
    when(col("amount") < 50, "Budget")
        .when((col("amount") >= 50) & (col("amount") < 200), "Standard")
        .otherwise("Premium").alias("product_tier")
)

# Step 5: Aggregation (wide transformation)
print(f"\n📦 Step 5: Aggregating by category and store...")
df_aggregated = df_enriched.groupBy("category", "store").agg(
    count("*").alias("transaction_count"),
    spark_round(sum("amount"), 2).alias("gross_sales"),
    spark_round(sum("total_amount"), 2).alias("revenue_with_tax"),
    spark_round(avg("amount"), 2).alias("avg_transaction_value"),
    min("amount").alias("min_amount"),
    max("amount").alias("max_amount")
).orderBy(col("revenue_with_tax").desc())

print(f"   Aggregated groups: {df_aggregated.count()}")

# Display final results
print(f"\n✅ Pipeline completed successfully!")
print(f"\n👇 Final aggregated results:")
display(df_aggregated)

# Step 6: Write to Delta (commented for demo - uncomment in production)
print(f"\n💾 Step 6: Writing to Delta table (demo - not executed)...")
print(f"   Target: {output_table}")
# Uncomment in production:
# df_aggregated.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .saveAsTable(output_table)
# print(f"\u2705 Data written to {output_table}")

print(f"\n" + "="*70)
print("🏆 Production Pipeline Complete!")

# COMMAND ----------

# DBTITLE 1,Genie Code Agent Usage Examples
# MAGIC %md
# MAGIC # 🧞 Genie Code Agent Usage Examples
# MAGIC
# MAGIC ## 💡 How to Use Genie Code Agent for Transformations
# MAGIC
# MAGIC Genie Code is your AI assistant that can help generate, optimize, and debug Spark transformation code.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔥 Example Prompts:
# MAGIC
# MAGIC ### 📝 Generation Prompts:
# MAGIC
# MAGIC 1. **"Generate a transformation pipeline to read sales data, filter amounts > 100, group by category, and calculate total revenue"**
# MAGIC
# MAGIC 2. **"Create a DataFrame transformation that adds a tax column (10% of amount) and a total_with_tax column"**
# MAGIC
# MAGIC 3. **"Write PySpark code to aggregate customer data by region with count, sum, and average calculations"**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚡ Optimization Prompts:
# MAGIC
# MAGIC 4. **"Optimize this transformation pipeline to reduce shuffle operations"**
# MAGIC
# MAGIC 5. **"Refactor this code to avoid multiple withColumn calls"**
# MAGIC
# MAGIC 6. **"Show me the best practice way to filter and aggregate this DataFrame"**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🐞 Debugging Prompts:
# MAGIC
# MAGIC 7. **"Why is my groupBy operation taking so long?"**
# MAGIC
# MAGIC 8. **"Fix this error: Column 'amount' cannot be resolved"**
# MAGIC
# MAGIC 9. **"Explain why I'm getting null values after this join"**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📚 Learning Prompts:
# MAGIC
# MAGIC 10. **"Explain the difference between narrow and wide transformations with examples"**
# MAGIC
# MAGIC 11. **"What's the best way to handle multiple aggregations on the same column?"**
# MAGIC
# MAGIC 12. **"Show me examples of using when/otherwise for conditional logic"**

# COMMAND ----------

# DBTITLE 1,Final Summary & Key Learnings
# MAGIC %md
# MAGIC # 🎓 Final Summary & Key Learnings
# MAGIC
# MAGIC ## 📌 Key Concepts Covered:
# MAGIC
# MAGIC ### 1️⃣ **Core Transformations**
# MAGIC * `select()` - Column selection and projection
# MAGIC * `filter()` / `where()` - Row filtering with predicates
# MAGIC * `withColumn()` - Column creation and modification
# MAGIC * `groupBy()` - Data aggregation
# MAGIC
# MAGIC ### 2️⃣ **Transformation Types**
# MAGIC * **Narrow Transformations**: No shuffle (select, filter, withColumn)
# MAGIC * **Wide Transformations**: Require shuffle (groupBy, join, orderBy)
# MAGIC
# MAGIC ### 3️⃣ **Optimization Principles**
# MAGIC * ✅ Push filters early
# MAGIC * ✅ Select only needed columns
# MAGIC * ✅ Reduce data before aggregation
# MAGIC * ✅ Avoid excessive withColumn chaining
# MAGIC * ✅ Leverage lazy evaluation
# MAGIC
# MAGIC ### 4️⃣ **Best Practices**
# MAGIC * Chain transformations fluently
# MAGIC * Use `select()` for multiple column operations
# MAGIC * Apply filters before wide transformations
# MAGIC * Use meaningful aliases for readability
# MAGIC * Consider partition-aware operations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Transformation Pipeline Pattern:
# MAGIC
# MAGIC ```python
# MAGIC result = df \
# MAGIC     .filter(...)        # Early filtering (narrow)
# MAGIC     .select(...)        # Column projection (narrow)
# MAGIC     .withColumn(...)    # Enrichment (narrow)
# MAGIC     .groupBy(...).agg(...)  # Aggregation (wide)
# MAGIC     .orderBy(...)       # Sorting (wide)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Production Checklist:
# MAGIC
# MAGIC * ☑️ Use DataFrame API (not RDDs)
# MAGIC * ☑️ Avoid cache/persist unless necessary
# MAGIC * ☑️ Use Unity Catalog for data access
# MAGIC * ☑️ Apply filters early
# MAGIC * ☑️ Select only required columns
# MAGIC * ☑️ Use meaningful column names
# MAGIC * ☑️ Test with sample data first
# MAGIC * ☑️ Monitor query execution plans

# COMMAND ----------

# DBTITLE 1,Interview Questions
# MAGIC %md
# MAGIC # 🎯 Interview Questions
# MAGIC
# MAGIC ## 📝 Spark Transformations - Interview Prep
# MAGIC
# MAGIC ### 🔴 Question 1: Narrow vs Wide Transformations
# MAGIC **Q:** What is the difference between narrow and wide transformations? Give examples.
# MAGIC
# MAGIC **A:** 
# MAGIC * **Narrow**: Each input partition contributes to at most one output partition. No shuffle required. Examples: `select()`, `filter()`, `withColumn()`, `map()`
# MAGIC * **Wide**: Input partitions contribute to multiple output partitions. Requires shuffle. Examples: `groupBy()`, `join()`, `orderBy()`, `repartition()`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟠 Question 2: Optimization Strategy
# MAGIC **Q:** How would you optimize a DataFrame pipeline that filters after groupBy?
# MAGIC
# MAGIC **A:** Move the filter **before** the groupBy to reduce the data volume being aggregated. This minimizes shuffle data and improves performance.
# MAGIC
# MAGIC ```python
# MAGIC # Bad: Filter after groupBy
# MAGIC df.groupBy("category").agg(sum("amount")).filter(col("sum") > 1000)
# MAGIC
# MAGIC # Good: Filter before groupBy
# MAGIC df.filter(col("amount") > 100).groupBy("category").agg(sum("amount"))
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟡 Question 3: withColumn Performance
# MAGIC **Q:** Why is chaining multiple withColumn() calls inefficient?
# MAGIC
# MAGIC **A:** Each `withColumn()` creates a new DataFrame object. Chaining multiple calls creates intermediate DataFrames. Instead, use `select()` with all transformations in one operation.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔵 Question 4: Lazy Evaluation
# MAGIC **Q:** What is lazy evaluation and how does it benefit Spark?
# MAGIC
# MAGIC **A:** Lazy evaluation means transformations are not executed until an action is called. Benefits:
# MAGIC * Catalyst optimizer can analyze the entire query plan
# MAGIC * Predicate pushdown and column pruning
# MAGIC * Eliminates unnecessary computations
# MAGIC * Reduces shuffle operations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟣 Question 5: Predicate Pushdown
# MAGIC **Q:** What is predicate pushdown and when does it occur?
# MAGIC
# MAGIC **A:** Predicate pushdown is an optimization where filter conditions are pushed down to the data source level, reducing data read. Occurs when:
# MAGIC * Reading from partitioned tables
# MAGIC * Using columnar formats (Parquet, Delta)
# MAGIC * Filters on partition columns
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟢 Question 6: Aggregation Functions
# MAGIC **Q:** Name 5 common aggregation functions and their use cases.
# MAGIC
# MAGIC **A:**
# MAGIC 1. `count()` - Count rows or non-null values
# MAGIC 2. `sum()` - Calculate total revenue/amounts
# MAGIC 3. `avg()` - Calculate average price/score
# MAGIC 4. `min()`/`max()` - Find boundaries
# MAGIC 5. `countDistinct()` - Count unique customers/products
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟡 Question 7: When/Otherwise
# MAGIC **Q:** How do you create conditional columns in Spark?
# MAGIC
# MAGIC **A:** Use `when().otherwise()` pattern:
# MAGIC ```python
# MAGIC df.withColumn("category",
# MAGIC     when(col("amount") < 50, "Low")
# MAGIC     .when(col("amount") < 200, "Medium")
# MAGIC     .otherwise("High")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 Question 8: Select vs SelectExpr
# MAGIC **Q:** What's the difference between select() and selectExpr()?
# MAGIC
# MAGIC **A:**
# MAGIC * `select()` - Uses Column objects: `df.select(col("amount") * 2)`
# MAGIC * `selectExpr()` - Uses SQL expressions as strings: `df.selectExpr("amount * 2 as doubled")`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟠 Question 9: GroupBy Memory
# MAGIC **Q:** What happens in memory during a groupBy operation?
# MAGIC
# MAGIC **A:** 
# MAGIC 1. Data is **shuffled** across partitions by grouping key
# MAGIC 2. All rows with same key move to same partition
# MAGIC 3. Aggregation happens within each partition
# MAGIC 4. Results are combined
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟢 Question 10: Transformation Ordering
# MAGIC **Q:** What is the optimal order for chaining transformations?
# MAGIC
# MAGIC **A:** 
# MAGIC 1. **Filter** - Reduce data early
# MAGIC 2. **Select** - Remove unnecessary columns
# MAGIC 3. **WithColumn** - Add calculated fields
# MAGIC 4. **GroupBy/Agg** - Aggregate reduced dataset
# MAGIC 5. **OrderBy** - Sort final results

# COMMAND ----------

# DBTITLE 1,Common Mistakes & Best Practices
# MAGIC %md
# MAGIC # ⚠️ Common Mistakes & Best Practices
# MAGIC
# MAGIC ## ❌ Common Mistakes to Avoid:
# MAGIC
# MAGIC ### 1️⃣ **Applying Filters Late**
# MAGIC ```python
# MAGIC # ❌ BAD
# MAGIC df.groupBy("category").agg(sum("amount")).filter(col("amount") > 100)
# MAGIC
# MAGIC # ✅ GOOD
# MAGIC df.filter(col("amount") > 100).groupBy("category").agg(sum("amount"))
# MAGIC ```
# MAGIC **Impact:** Processes unnecessary data during aggregation.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ **Selecting Unnecessary Columns**
# MAGIC ```python
# MAGIC # ❌ BAD - Selecting all columns when only need 2
# MAGIC df.select("*").groupBy("id").agg(sum("amount"))
# MAGIC
# MAGIC # ✅ GOOD
# MAGIC df.select("id", "amount").groupBy("id").agg(sum("amount"))
# MAGIC ```
# MAGIC **Impact:** Increased memory usage and network I/O.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ **Overusing withColumn**
# MAGIC ```python
# MAGIC # ❌ BAD - Chaining multiple withColumn calls
# MAGIC df.withColumn("col1", expr1) \
# MAGIC   .withColumn("col2", expr2) \
# MAGIC   .withColumn("col3", expr3)
# MAGIC
# MAGIC # ✅ GOOD - Single select with all expressions
# MAGIC df.select("*", expr1.alias("col1"), expr2.alias("col2"), expr3.alias("col3"))
# MAGIC ```
# MAGIC **Impact:** Creates intermediate DataFrame objects.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4️⃣ **Inefficient Aggregations**
# MAGIC ```python
# MAGIC # ❌ BAD - Multiple groupBy for same key
# MAGIC total = df.groupBy("id").agg(sum("amount"))
# MAGIC avg_val = df.groupBy("id").agg(avg("amount"))
# MAGIC
# MAGIC # ✅ GOOD - Single groupBy with multiple aggs
# MAGIC result = df.groupBy("id").agg(
# MAGIC     sum("amount").alias("total"),
# MAGIC     avg("amount").alias("average")
# MAGIC )
# MAGIC ```
# MAGIC **Impact:** Performs multiple shuffles unnecessarily.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5️⃣ **Forgetting to Use Aliases**
# MAGIC ```python
# MAGIC # ❌ BAD - Unclear column names
# MAGIC df.select((col("amount") * 2))
# MAGIC
# MAGIC # ✅ GOOD - Clear aliases
# MAGIC df.select((col("amount") * 2).alias("amount_doubled"))
# MAGIC ```
# MAGIC **Impact:** Auto-generated column names are hard to read.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6️⃣ **Using RDDs Instead of DataFrames**
# MAGIC ```python
# MAGIC # ❌ BAD - Using RDD API
# MAGIC rdd.map(lambda x: x[0]).filter(lambda x: x > 100)
# MAGIC
# MAGIC # ✅ GOOD - Using DataFrame API
# MAGIC df.select("amount").filter(col("amount") > 100)
# MAGIC ```
# MAGIC **Impact:** Loses Catalyst optimizer benefits.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 7️⃣ **Ignoring Null Values**
# MAGIC ```python
# MAGIC # ❌ BAD - Not handling nulls
# MAGIC df.filter(col("category") == "Electronics")
# MAGIC
# MAGIC # ✅ GOOD - Explicitly handle nulls
# MAGIC df.filter((col("category") == "Electronics") & col("category").isNotNull())
# MAGIC ```
# MAGIC **Impact:** Unexpected null-related issues.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 8️⃣ **Not Using Appropriate Data Types**
# MAGIC ```python
# MAGIC # ❌ BAD - String for numeric operations
# MAGIC df.withColumn("amount", col("amount_str").cast("string"))
# MAGIC
# MAGIC # ✅ GOOD - Use proper numeric types
# MAGIC df.withColumn("amount", col("amount_str").cast("double"))
# MAGIC ```
# MAGIC **Impact:** Incorrect calculations or errors.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Best Practices Checklist:
# MAGIC
# MAGIC * ☑️ Apply filters early in the pipeline
# MAGIC * ☑️ Select only required columns
# MAGIC * ☑️ Use `select()` for multiple column operations
# MAGIC * ☑️ Combine multiple aggregations in single groupBy
# MAGIC * ☑️ Always use meaningful aliases
# MAGIC * ☑️ Handle null values explicitly
# MAGIC * ☑️ Use appropriate data types
# MAGIC * ☑️ Leverage DataFrame API over RDDs
# MAGIC * ☑️ Chain transformations fluently
# MAGIC * ☑️ Test with explain() to check query plans
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Additional Resources:
# MAGIC
# MAGIC * Spark SQL Guide: https://spark.apache.org/docs/latest/sql-programming-guide.html
# MAGIC * Databricks Optimization Guide
# MAGIC * PySpark API Documentation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏆 Congratulations!
# MAGIC
# MAGIC You've completed **Phase 3 Day 15: Spark Transformations**!
# MAGIC
# MAGIC **Next Steps:**
# MAGIC * Practice with real datasets
# MAGIC * Experiment with complex pipelines
# MAGIC * Profile your transformations with explain()
# MAGIC * Explore window functions (advanced)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📝 Author: **TRRaveendra** | Watermark: **@TRRaveendra**