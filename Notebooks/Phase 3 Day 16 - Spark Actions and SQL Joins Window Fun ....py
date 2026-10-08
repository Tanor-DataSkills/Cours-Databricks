# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # ⚡ Data Engineering Training — Phase 3 Day 16  
# MAGIC ## 🔍 Spark Actions & SQL: Joins, Window Functions  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - Spark Actions (show, collect, count)  
# MAGIC - Spark SQL Basics  
# MAGIC - Joins (inner, left, right, full outer)  
# MAGIC - Window Functions (rank, row_number, lag, lead, etc.)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### ⚙️ Platform: Databricks (Serverless + Unity Catalog + Spark SQL)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Objective:
# MAGIC Understand how Spark actions trigger execution and how to use Spark SQL for joins and advanced analytics using window functions.
# MAGIC
# MAGIC ### 🎓 Learning Outcomes:
# MAGIC - Master Spark action operations and understand lazy evaluation
# MAGIC - Write efficient SQL joins in PySpark and Spark SQL
# MAGIC - Apply window functions for ranking and analytical queries
# MAGIC - Build hybrid DataFrame + SQL analytics pipelines
# MAGIC - Optimize join strategies for production workloads
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **IMPORTANT ENGINEERING CONSTRAINTS:**
# MAGIC - ✅ Databricks Serverless Compute
# MAGIC - ✅ DataFrame API only (no RDDs)
# MAGIC - ✅ No cache() / persist()
# MAGIC - ✅ No /tmp or local storage
# MAGIC - ✅ Unity Catalog Volumes for data access
# MAGIC - ✅ SQL + DataFrame hybrid approach

# COMMAND ----------

# DBTITLE 1,Section 1 - Actions in Spark Concept
# MAGIC %md
# MAGIC # 🔥 SECTION 1: Actions in Spark
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 Explanation:
# MAGIC
# MAGIC Think of Spark like a **recipe book**. When you write transformations (like `filter()`, `select()`, `map()`), you're just writing down the recipe steps. **Nothing actually gets cooked yet!**
# MAGIC
# MAGIC **Actions** are like saying **"Now cook it!"** — that's when Spark actually executes all the steps and gives you results.
# MAGIC
# MAGIC Examples:
# MAGIC - `show()` → "Show me the first few rows"
# MAGIC - `count()` → "How many rows are there?"
# MAGIC - `collect()` → "Bring all the data to me"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Spark uses lazy evaluation** — transformations build a **Directed Acyclic Graph (DAG)** of logical operations but don't execute immediately.
# MAGIC
# MAGIC **Actions trigger execution** by:
# MAGIC 1. Optimizing the DAG (Catalyst Optimizer)
# MAGIC 2. Generating physical execution plan
# MAGIC 3. Distributing tasks across executors
# MAGIC 4. Returning results to the driver
# MAGIC
# MAGIC ### Common Actions:
# MAGIC | Action | Purpose | Returns |
# MAGIC |--------|---------|----------|
# MAGIC | `show()` | Display rows | None (prints to console) |
# MAGIC | `count()` | Count rows | Long |
# MAGIC | `collect()` | Gather all rows to driver | Array/List |
# MAGIC | `take(n)` | Fetch first n rows | Array/List |
# MAGIC | `first()` | Get first row | Row |
# MAGIC | `write()` | Save to storage | None |
# MAGIC
# MAGIC ### Key Concepts:
# MAGIC - **Transformations = Lazy** (filter, select, groupBy)
# MAGIC - **Actions = Eager** (show, count, collect)
# MAGIC - **Actions trigger job execution**
# MAGIC - **Each action creates a new Spark job**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Critical Warning:
# MAGIC **`collect()` brings ALL data to the driver node** → Can cause **OutOfMemoryError** on large datasets!
# MAGIC
# MAGIC **Best Practice:** Use `show()` or `take()` for inspection, avoid `collect()` in production.

# COMMAND ----------

# DBTITLE 1,Section 1 - Create Sample Data
# Create sample data for demonstration
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Sample employee data
employee_data = [
    (1, "Alice", "Engineering", 95000, "2020-01-15"),
    (2, "Bob", "Sales", 75000, "2019-03-20"),
    (3, "Charlie", "Engineering", 105000, "2018-07-10"),
    (4, "Diana", "HR", 65000, "2021-05-12"),
    (5, "Eve", "Sales", 82000, "2020-11-08"),
    (6, "Frank", "Engineering", 98000, "2019-09-25"),
    (7, "Grace", "HR", 70000, "2021-01-30"),
    (8, "Henry", "Sales", 78000, "2020-04-17"),
    (9, "Ivy", "Engineering", 110000, "2017-12-05"),
    (10, "Jack", "Sales", 88000, "2019-08-22")
]

employee_schema = StructType([
    StructField("emp_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("department", StringType(), False),
    StructField("salary", IntegerType(), False),
    StructField("hire_date", StringType(), False)
])

df_employees = spark.createDataFrame(employee_data, employee_schema)

print("✅ Sample employee data created successfully!")
print(f"Total records: {len(employee_data)}")
print(f"Columns: {df_employees.columns}")

# COMMAND ----------

# DBTITLE 1,Section 1 - Demonstrate Actions
# ========================================
# DEMONSTRATION: Spark Actions
# ========================================

print("\n" + "="*60)
print("ACTION 1: show() - Display DataFrame")
print("="*60)

# show() displays first 20 rows by default
df_employees.show()

# Show only 5 rows
print("\n🔹 Showing first 5 rows:")
df_employees.show(5)

# Show with truncate=False to see full content
print("\n🔹 Showing without truncation:")
df_employees.show(5, truncate=False)


print("\n" + "="*60)
print("ACTION 2: count() - Count Rows")
print("="*60)

# count() returns the number of rows
total_employees = df_employees.count()
print(f"📊 Total employees: {total_employees}")

# Count by department
print("\n🔹 Count by department:")
department_counts = df_employees.groupBy("department").count()
department_counts.show()


print("\n" + "="*60)
print("ACTION 3: collect() - Gather All Data")
print("="*60)
print("⚠️ WARNING: collect() brings ALL data to driver memory!")
print("Only use on small datasets or after filtering.\n")

# Collect all rows (DANGEROUS on large data!)
all_rows = df_employees.collect()
print(f"Type of collected data: {type(all_rows)}")
print(f"Number of rows collected: {len(all_rows)}")
print(f"\nFirst row: {all_rows[0]}")
print(f"Accessing columns: Name = {all_rows[0]['name']}, Salary = {all_rows[0]['salary']}")


print("\n" + "="*60)
print("ACTION 4: take() - Fetch First N Rows")
print("="*60)

# take() is safer than collect()
first_3 = df_employees.take(3)
print(f"🔹 First 3 employees:")
for row in first_3:
    print(f"  - {row['name']}: {row['department']} - ${row['salary']:,}")


print("\n" + "="*60)
print("ACTION 5: first() - Get First Row")
print("="*60)

first_employee = df_employees.first()
print(f"🔹 First employee: {first_employee['name']}")


print("\n" + "="*60)
print("SUMMARY: Actions vs Transformations")
print("="*60)
print("""
✅ Transformations (Lazy):
   - filter(), select(), groupBy(), join()
   - Build execution plan, don't execute
   - Return DataFrame

✅ Actions (Eager):
   - show(), count(), collect(), take()
   - Trigger execution
   - Return results to driver

👍 Best Practice: Use display() in Databricks for better visualization
""")

# COMMAND ----------

# DBTITLE 1,Section 1 - Using display() in Databricks
# ========================================
# DATABRICKS BEST PRACTICE: display()
# ========================================

print("🔹 Using display() - Databricks native visualization\n")

# display() is better than show() in Databricks
# - Interactive table
# - Automatic visualizations
# - Export capabilities
# - No truncation issues

display(df_employees)

# COMMAND ----------

# DBTITLE 1,Section 2 - Spark SQL Overview Concept
# MAGIC %md
# MAGIC # 📊 SECTION 2: Spark SQL Overview
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 Explanation:
# MAGIC
# MAGIC Imagine you have a toy box (DataFrame) full of toys. Sometimes you want to:
# MAGIC - Play with the toys using your hands (DataFrame API)
# MAGIC - Tell someone else what toys you want using words (SQL)
# MAGIC
# MAGIC **Spark SQL lets you use regular SQL language** instead of Python/Scala code!
# MAGIC
# MAGIC Instead of writing:
# MAGIC ```python
# MAGIC df.filter(df.salary > 80000).select("name", "salary")
# MAGIC ```
# MAGIC
# MAGIC You can write:
# MAGIC ```sql
# MAGIC SELECT name, salary FROM employees WHERE salary > 80000
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Spark SQL** is a module for structured data processing that provides:
# MAGIC 1. **SQL interface** to Spark's distributed computing engine
# MAGIC 2. **Unified API** across DataFrame, Dataset, and SQL
# MAGIC 3. **Catalyst Optimizer** for query optimization
# MAGIC 4. **Tungsten execution engine** for performance
# MAGIC
# MAGIC ### Key Concepts:
# MAGIC
# MAGIC | Feature | Description |
# MAGIC |---------|-------------|
# MAGIC | **Temporary Views** | In-memory table reference for SQL queries |
# MAGIC | **Global Temp Views** | Views accessible across SparkSessions |
# MAGIC | **Catalog** | Metadata store for tables and views |
# MAGIC | **SQL Context** | Unified entry point for SQL operations |
# MAGIC
# MAGIC ### Architecture Flow:
# MAGIC ```
# MAGIC SQL Query / DataFrame API
# MAGIC          ↓
# MAGIC Unresolved Logical Plan
# MAGIC          ↓
# MAGIC Analyzer (validates against catalog)
# MAGIC          ↓
# MAGIC Resolved Logical Plan
# MAGIC          ↓
# MAGIC Catalyst Optimizer (applies optimization rules)
# MAGIC          ↓
# MAGIC Optimized Logical Plan
# MAGIC          ↓
# MAGIC Spark Planner (generates physical plans)
# MAGIC          ↓
# MAGIC Cost Model (selects best physical plan)
# MAGIC          ↓
# MAGIC Physical Plan
# MAGIC          ↓
# MAGIC Code Generation (Tungsten)
# MAGIC          ↓
# MAGIC Execution
# MAGIC ```
# MAGIC
# MAGIC ### Benefits:
# MAGIC - **Familiar SQL syntax** for analysts
# MAGIC - **Same optimization** as DataFrame API
# MAGIC - **Interoperability** between SQL and DataFrames
# MAGIC - **Performance parity** — no overhead
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Best Practice:
# MAGIC Use **SQL for complex analytics** and **DataFrame API for ETL pipelines** — mix them as needed!

# COMMAND ----------

# DBTITLE 1,Section 2 - Create Temporary Views
# ========================================
# DEMONSTRATION: Creating Temporary Views
# ========================================

print("\n" + "="*60)
print("Creating Temporary Views for SQL")
print("="*60)

# Create temporary view from DataFrame
df_employees.createOrReplaceTempView("employees")

print("✅ Temporary view 'employees' created!")
print("\n🔹 Now you can query it using SQL\n")

# Verify the view exists
print("Current tables/views in catalog:")
spark.catalog.listTables()

# COMMAND ----------

# DBTITLE 1,Section 2 - Query with SQL
# MAGIC %sql
# MAGIC -- ========================================
# MAGIC -- QUERYING WITH SQL
# MAGIC -- ========================================
# MAGIC
# MAGIC -- Simple SELECT
# MAGIC SELECT * FROM employees
# MAGIC LIMIT 5

# COMMAND ----------

# DBTITLE 1,Section 2 - SQL Filtering and Aggregation
# MAGIC %sql
# MAGIC -- Filter employees by salary
# MAGIC SELECT 
# MAGIC     name,
# MAGIC     department,
# MAGIC     salary,
# MAGIC     hire_date
# MAGIC FROM employees
# MAGIC WHERE salary > 80000
# MAGIC ORDER BY salary DESC

# COMMAND ----------

# DBTITLE 1,Section 2 - SQL Aggregations
# MAGIC %sql
# MAGIC -- Aggregation: Average salary by department
# MAGIC SELECT 
# MAGIC     department,
# MAGIC     COUNT(*) as employee_count,
# MAGIC     AVG(salary) as avg_salary,
# MAGIC     MIN(salary) as min_salary,
# MAGIC     MAX(salary) as max_salary
# MAGIC FROM employees
# MAGIC GROUP BY department
# MAGIC ORDER BY avg_salary DESC

# COMMAND ----------

# DBTITLE 1,Section 2 - SQL Results to DataFrame
# ========================================
# SQL → DataFrame Conversion
# ========================================

print("\n" + "="*60)
print("Converting SQL Results to DataFrame")
print("="*60)

# Execute SQL and store result as DataFrame
df_high_earners = spark.sql("""
    SELECT 
        name,
        department,
        salary,
        CASE 
            WHEN salary >= 100000 THEN 'High'
            WHEN salary >= 80000 THEN 'Medium'
            ELSE 'Standard'
        END as salary_band
    FROM employees
    WHERE salary > 75000
    ORDER BY salary DESC
""")

print("✅ SQL query executed and stored as DataFrame\n")

# Now you can use DataFrame operations on SQL results
print("🔹 Result:")
display(df_high_earners)

print(f"\n📊 Total high earners: {df_high_earners.count()}")

# COMMAND ----------

# DBTITLE 1,Section 2 - Hybrid Approach Demo
# ========================================
# HYBRID APPROACH: DataFrame + SQL
# ========================================

print("\n" + "="*60)
print("Hybrid Approach: Mix DataFrame API with SQL")
print("="*60)

# Step 1: Use DataFrame API for initial filtering
df_filtered = df_employees.filter(col("department") == "Engineering")

print("🔹 Step 1: Filtered using DataFrame API")
print(f"   Engineers count: {df_filtered.count()}\n")

# Step 2: Create temp view from filtered DataFrame
df_filtered.createOrReplaceTempView("engineers")

print("✅ Temporary view 'engineers' created from filtered DataFrame\n")

# Step 3: Use SQL for complex analytics
result = spark.sql("""
    SELECT 
        name,
        salary,
        ROUND(salary * 0.15, 2) as annual_bonus,
        ROUND(salary * 1.15, 2) as total_comp
    FROM engineers
    ORDER BY salary DESC
""")

print("🔹 Step 2: Applied complex SQL analytics")
display(result)

print("""
\n👍 Why Hybrid Approach?
- Use DataFrame API for ETL and transformations
- Use SQL for complex analytics and reporting
- Both compile to same execution plan
- Choose based on readability and team skills
""")

# COMMAND ----------

# DBTITLE 1,Section 2 - Global Temp Views
# ========================================
# GLOBAL TEMPORARY VIEWS (Serverless Limitation)
# ========================================

print("\n" + "="*60)
print("Global Temporary Views - Serverless Note")
print("="*60)

print("""
⚠️  SERVERLESS LIMITATION:
Global temporary views are NOT supported on Databricks Serverless compute.

This is a platform constraint:
- Serverless doesn't support cross-session global temp views
- Use regular temporary views instead
- For shared data, use Delta tables in Unity Catalog

""")

# Alternative: Use regular temp view (already demonstrated)
print("✅ Alternative: Using regular temporary views (already created)\n")

# Query the regular temp view
result = spark.sql("""
    SELECT department, COUNT(*) as count
    FROM employees
    GROUP BY department
""")

print("🔹 Querying regular temp view 'employees':")
display(result)

print("""
\n💡 Temp Views on Serverless:
- Temporary View: Scoped to current SparkSession ✅ SUPPORTED
- Global Temp View: Accessible across SparkSessions ❌ NOT SUPPORTED on serverless

📌 Best Practice on Serverless:
   Use Unity Catalog tables for sharing data across sessions
   Use temporary views for single-session analytics
""")

# COMMAND ----------

# DBTITLE 1,Section 3 - Joins in Spark Concept
# MAGIC %md
# MAGIC # 🔗 SECTION 3: Joins in Spark
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 Explanation:
# MAGIC
# MAGIC Imagine you have two lists:
# MAGIC - **List A**: Student names and their student IDs
# MAGIC - **List B**: Student IDs and their grades
# MAGIC
# MAGIC A **join** is like matching the lists together:
# MAGIC - **Inner Join**: Only show students who have both a name AND a grade
# MAGIC - **Left Join**: Show all students, even if some don't have grades yet
# MAGIC - **Right Join**: Show all grades, even if some don't have student names
# MAGIC - **Full Outer Join**: Show everything, even if some don't match
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Joins** combine two DataFrames based on a common key (or condition).
# MAGIC
# MAGIC ### Join Types:
# MAGIC
# MAGIC | Join Type | SQL Equivalent | Description | Use Case |
# MAGIC |-----------|----------------|-------------|----------|
# MAGIC | **Inner** | `INNER JOIN` | Only matching rows from both sides | Find customers with orders |
# MAGIC | **Left (Outer)** | `LEFT JOIN` | All rows from left + matches from right | All customers, with or without orders |
# MAGIC | **Right (Outer)** | `RIGHT JOIN` | All rows from right + matches from left | All orders, with or without customer data |
# MAGIC | **Full Outer** | `FULL OUTER JOIN` | All rows from both sides | Union of left and right joins |
# MAGIC | **Cross** | `CROSS JOIN` | Cartesian product | All possible combinations |
# MAGIC | **Left Semi** | `WHERE EXISTS` | Rows from left with matches in right | Filter based on existence |
# MAGIC | **Left Anti** | `WHERE NOT EXISTS` | Rows from left without matches in right | Find orphaned records |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Join Execution Strategies:
# MAGIC
# MAGIC **1. Broadcast Hash Join (BHJ)**
# MAGIC - Small table broadcast to all executors
# MAGIC - Fast for small dimension tables
# MAGIC - Threshold: `spark.sql.autoBroadcastJoinThreshold` (default 10MB)
# MAGIC
# MAGIC **2. Sort-Merge Join (SMJ)**
# MAGIC - Both datasets sorted and merged
# MAGIC - Used for large-large joins
# MAGIC - Requires shuffling both sides
# MAGIC
# MAGIC **3. Shuffle Hash Join (SHJ)**
# MAGIC - Build hash table after shuffle
# MAGIC - Used when one side is smaller but not broadcastable
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Performance Considerations:
# MAGIC
# MAGIC ✅ **Best Practices:**
# MAGIC - Filter before joining to reduce data size
# MAGIC - Use broadcast joins for dimension tables (<10MB)
# MAGIC - Partition on join keys to minimize shuffle
# MAGIC - Avoid data skew (uneven key distribution)
# MAGIC
# MAGIC ⚠️ **Anti-Patterns:**
# MAGIC - Joining large tables without filters
# MAGIC - Cross joins without conditions
# MAGIC - Multiple sequential joins without optimization

# COMMAND ----------

# DBTITLE 1,Section 3 - Create Second Dataset for Joins
# ========================================
# CREATE DEPARTMENT DATA FOR JOINS
# ========================================

print("\n" + "="*60)
print("Creating Department Data")
print("="*60)

# Department budget data
department_data = [
    ("Engineering", 500000, "Building products", "John Smith"),
    ("Sales", 300000, "Revenue generation", "Sarah Johnson"),
    ("HR", 150000, "People management", "Mike Davis"),
    ("Marketing", 250000, "Brand awareness", "Lisa Wong")  # Not in employees!
]

department_schema = StructType([
    StructField("department", StringType(), False),
    StructField("budget", IntegerType(), False),
    StructField("description", StringType(), False),
    StructField("manager", StringType(), False)
])

df_departments = spark.createDataFrame(department_data, department_schema)

print("✅ Department data created!\n")
print("🔹 Employees DataFrame:")
display(df_employees)

print("\n🔹 Departments DataFrame:")
display(df_departments)

print("""
\n💡 Notice:
- Employees has Engineering, Sales, HR
- Departments has Engineering, Sales, HR, Marketing
- Marketing department has no employees (yet)
- This will demonstrate different join behaviors
""")

# COMMAND ----------

# DBTITLE 1,Section 3 - Inner Join DataFrame API
# ========================================
# INNER JOIN - DataFrame API
# ========================================

print("\n" + "="*60)
print("INNER JOIN - DataFrame API")
print("="*60)

# Inner join: Only matching rows
df_inner = df_employees.join(
    df_departments,
    on="department",
    how="inner"
)

print("✅ Inner join completed")
print("   Returns: Only departments that exist in BOTH tables\n")

print("🔹 Result:")
display(df_inner.select(
    "emp_id",
    "name",
    "department",
    "salary",
    "budget",
    "manager"
))

print(f"\n📊 Row count after inner join: {df_inner.count()}")
print(f"   Original employees: {df_employees.count()}")
print(f"   Original departments: {df_departments.count()}")
print("   Marketing department excluded (no employees)")

# COMMAND ----------

# DBTITLE 1,Section 3 - Inner Join SQL
# MAGIC %sql
# MAGIC -- ========================================
# MAGIC -- INNER JOIN - SQL
# MAGIC -- ========================================
# MAGIC
# MAGIC -- Create temp views
# MAGIC CREATE OR REPLACE TEMPORARY VIEW employees_view AS 
# MAGIC SELECT * FROM (VALUES
# MAGIC     (1, 'Alice', 'Engineering', 95000, '2020-01-15'),
# MAGIC     (2, 'Bob', 'Sales', 75000, '2019-03-20'),
# MAGIC     (3, 'Charlie', 'Engineering', 105000, '2018-07-10'),
# MAGIC     (4, 'Diana', 'HR', 65000, '2021-05-12'),
# MAGIC     (5, 'Eve', 'Sales', 82000, '2020-11-08'),
# MAGIC     (6, 'Frank', 'Engineering', 98000, '2019-09-25'),
# MAGIC     (7, 'Grace', 'HR', 70000, '2021-01-30'),
# MAGIC     (8, 'Henry', 'Sales', 78000, '2020-04-17'),
# MAGIC     (9, 'Ivy', 'Engineering', 110000, '2017-12-05'),
# MAGIC     (10, 'Jack', 'Sales', 88000, '2019-08-22')
# MAGIC ) AS employees(emp_id, name, department, salary, hire_date);
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW departments_view AS
# MAGIC SELECT * FROM (VALUES
# MAGIC     ('Engineering', 500000, 'Building products', 'John Smith'),
# MAGIC     ('Sales', 300000, 'Revenue generation', 'Sarah Johnson'),
# MAGIC     ('HR', 150000, 'People management', 'Mike Davis'),
# MAGIC     ('Marketing', 250000, 'Brand awareness', 'Lisa Wong')
# MAGIC ) AS departments(department, budget, description, manager);
# MAGIC
# MAGIC -- Now perform INNER JOIN
# MAGIC SELECT 
# MAGIC     e.emp_id,
# MAGIC     e.name,
# MAGIC     e.department,
# MAGIC     e.salary,
# MAGIC     d.budget,
# MAGIC     d.manager,
# MAGIC     ROUND((e.salary / d.budget) * 100, 2) as pct_of_budget
# MAGIC FROM employees_view e
# MAGIC INNER JOIN departments_view d
# MAGIC     ON e.department = d.department
# MAGIC ORDER BY e.department, e.salary DESC

# COMMAND ----------

# DBTITLE 1,Section 3 - Left Join DataFrame API
# ========================================
# LEFT JOIN - DataFrame API
# ========================================

print("\n" + "="*60)
print("LEFT JOIN (LEFT OUTER JOIN) - DataFrame API")
print("="*60)

# Left join: All rows from left table, matching rows from right
df_left = df_employees.join(
    df_departments,
    on="department",
    how="left"  # or "left_outer"
)

print("✅ Left join completed")
print("   Returns: ALL employees + matching department info")
print("   If no department match, department columns are NULL\n")

print("🔹 Result:")
display(df_left.select(
    "emp_id",
    "name",
    "department",
    "salary",
    "budget",
    "manager"
).orderBy("department", "salary", ascending=[True, False]))

print(f"\n📊 Row count after left join: {df_left.count()}")
print(f"   Same as employees count: {df_employees.count()}")
print("   All employees preserved, department info added where available")

# COMMAND ----------

# DBTITLE 1,Section 3 - Left Join SQL
# MAGIC %sql
# MAGIC -- ========================================
# MAGIC -- LEFT JOIN - SQL
# MAGIC -- ========================================
# MAGIC
# MAGIC SELECT 
# MAGIC     e.emp_id,
# MAGIC     e.name,
# MAGIC     e.department,
# MAGIC     e.salary,
# MAGIC     d.budget,
# MAGIC     d.manager,
# MAGIC     d.description,
# MAGIC     CASE 
# MAGIC         WHEN d.budget IS NULL THEN 'No department data'
# MAGIC         ELSE 'Department exists'
# MAGIC     END as data_status
# MAGIC FROM employees_view e
# MAGIC LEFT JOIN departments_view d
# MAGIC     ON e.department = d.department
# MAGIC ORDER BY e.department, e.salary DESC

# COMMAND ----------

# DBTITLE 1,Section 3 - Right Join DataFrame API
# ========================================
# RIGHT JOIN - DataFrame API
# ========================================

print("\n" + "="*60)
print("RIGHT JOIN (RIGHT OUTER JOIN) - DataFrame API")
print("="*60)

# Right join: All rows from right table, matching rows from left
df_right = df_employees.join(
    df_departments,
    on="department",
    how="right"  # or "right_outer"
)

print("✅ Right join completed")
print("   Returns: ALL departments + matching employees")
print("   If no employees, employee columns are NULL\n")

print("🔹 Result:")
display(df_right.select(
    "department",
    "budget",
    "manager",
    "emp_id",
    "name",
    "salary"
).orderBy("department"))

print(f"\n📊 Row count after right join: {df_right.count()}")
print("   ❗ Notice Marketing department appears with NULL employees")

# Show departments without employees
print("\n🔹 Departments without employees:")
df_no_employees = df_right.filter(col("emp_id").isNull())
display(df_no_employees.select("department", "budget", "manager"))

# COMMAND ----------

# DBTITLE 1,Section 3 - Right Join SQL
# MAGIC %sql
# MAGIC -- ========================================
# MAGIC -- RIGHT JOIN - SQL
# MAGIC -- ========================================
# MAGIC
# MAGIC SELECT 
# MAGIC     d.department,
# MAGIC     d.budget,
# MAGIC     d.manager,
# MAGIC     d.description,
# MAGIC     COUNT(e.emp_id) as employee_count,
# MAGIC     COALESCE(SUM(e.salary), 0) as total_salary_cost,
# MAGIC     d.budget - COALESCE(SUM(e.salary), 0) as remaining_budget
# MAGIC FROM employees_view e
# MAGIC RIGHT JOIN departments_view d
# MAGIC     ON e.department = d.department
# MAGIC GROUP BY d.department, d.budget, d.manager, d.description
# MAGIC ORDER BY d.department

# COMMAND ----------

# DBTITLE 1,Section 3 - Full Outer Join
# ========================================
# FULL OUTER JOIN - DataFrame API
# ========================================

print("\n" + "="*60)
print("FULL OUTER JOIN - DataFrame API")
print("="*60)

# Full outer join: All rows from both tables
df_full = df_employees.join(
    df_departments,
    on="department",
    how="full"  # or "full_outer" or "outer"
)

print("✅ Full outer join completed")
print("   Returns: ALL rows from BOTH tables")
print("   NULLs appear where no match exists\n")

print("🔹 Result:")
display(df_full.select(
    "department",
    "emp_id",
    "name",
    "salary",
    "budget",
    "manager"
).orderBy("department"))

print(f"\n📊 Analysis:")
print(f"   Total rows: {df_full.count()}")
print(f"   Employees without dept info: {df_full.filter(col('budget').isNull()).count()}")
print(f"   Departments without employees: {df_full.filter(col('emp_id').isNull()).count()}")

# COMMAND ----------

# DBTITLE 1,Section 3 - Full Outer Join SQL
# MAGIC %sql
# MAGIC -- ========================================
# MAGIC -- FULL OUTER JOIN - SQL
# MAGIC -- ========================================
# MAGIC
# MAGIC SELECT 
# MAGIC     COALESCE(e.department, d.department) as department,
# MAGIC     e.emp_id,
# MAGIC     e.name,
# MAGIC     e.salary,
# MAGIC     d.budget,
# MAGIC     d.manager,
# MAGIC     CASE 
# MAGIC         WHEN e.emp_id IS NULL THEN 'Department without employees'
# MAGIC         WHEN d.budget IS NULL THEN 'Employee without department'
# MAGIC         ELSE 'Matched'
# MAGIC     END as join_status
# MAGIC FROM employees_view e
# MAGIC FULL OUTER JOIN departments_view d
# MAGIC     ON e.department = d.department
# MAGIC ORDER BY department, e.salary DESC

# COMMAND ----------

# DBTITLE 1,Section 3 - Semi and Anti Joins
# ========================================
# LEFT SEMI & LEFT ANTI JOINS
# ========================================

print("\n" + "="*60)
print("LEFT SEMI JOIN - DataFrame API")
print("="*60)

# Left semi join: Returns rows from left where match exists in right
# Does NOT include columns from right table
df_semi = df_employees.join(
    df_departments,
    on="department",
    how="left_semi"
)

print("✅ Left semi join completed")
print("   Returns: Employees whose department exists in departments table")
print("   Similar to WHERE EXISTS in SQL")
print("   Note: Does NOT include department columns!\n")

print("🔹 Result (only employee columns):")
display(df_semi)

print(f"\n📊 Employees with valid departments: {df_semi.count()}")


print("\n" + "="*60)
print("LEFT ANTI JOIN - DataFrame API")
print("="*60)

# Left anti join: Returns rows from left where NO match in right
df_anti = df_employees.join(
    df_departments,
    on="department",
    how="left_anti"
)

print("✅ Left anti join completed")
print("   Returns: Employees whose department does NOT exist in departments table")
print("   Similar to WHERE NOT EXISTS in SQL\n")

print("🔹 Result (employees without matching department):")
if df_anti.count() == 0:
    print("   ✅ No orphaned employees found!")
    print("   All employees have matching departments.")
else:
    display(df_anti)

print("""
\n💡 Use Cases:
- Semi Join: Filter based on existence (efficient alternative to IN clause)
- Anti Join: Find orphaned records, data quality checks
""")

# COMMAND ----------

# DBTITLE 1,Section 3 - Join Comparison Summary
# ========================================
# JOIN TYPES SUMMARY
# ========================================

print("\n" + "="*80)
print("JOIN TYPES COMPARISON SUMMARY")
print("="*80)

# Create comparison DataFrame
from pyspark.sql.types import *

join_types = [
    ("inner", df_employees.join(df_departments, "department", "inner").count()),
    ("left", df_employees.join(df_departments, "department", "left").count()),
    ("right", df_employees.join(df_departments, "department", "right").count()),
    ("full", df_employees.join(df_departments, "department", "full").count()),
    ("left_semi", df_employees.join(df_departments, "department", "left_semi").count()),
    ("left_anti", df_employees.join(df_departments, "department", "left_anti").count())
]

df_join_summary = spark.createDataFrame(join_types, ["join_type", "row_count"])

print("\n📊 Row Counts by Join Type:")
display(df_join_summary)

print(f"""
\n💡 Analysis:
- Employees: {df_employees.count()} rows
- Departments: {df_departments.count()} rows

Join Results:
- Inner: {df_employees.join(df_departments, 'department', 'inner').count()} (only matches)
- Left: {df_employees.join(df_departments, 'department', 'left').count()} (all employees)
- Right: {df_employees.join(df_departments, 'department', 'right').count()} (all departments + employees)
- Full: {df_employees.join(df_departments, 'department', 'full').count()} (union of all)
- Left Semi: {df_employees.join(df_departments, 'department', 'left_semi').count()} (employees with dept)
- Left Anti: {df_employees.join(df_departments, 'department', 'left_anti').count()} (employees without dept)

✅ Key Takeaway:
   Choose join type based on business logic:
   - Use INNER when you only want matching records
   - Use LEFT when you want to keep all left-side records
   - Use RIGHT when you want to keep all right-side records
   - Use FULL when you need everything from both sides
""")

# COMMAND ----------

# DBTITLE 1,Section 4 - Join Optimization Concept
# MAGIC %md
# MAGIC # ⚡ SECTION 4: Join Optimization
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 Explanation:
# MAGIC
# MAGIC Imagine you're organizing a party and matching name tags to guests:
# MAGIC
# MAGIC **Bad Way (Slow)**:
# MAGIC - You have 1000 guests and 1000 name tags in different rooms
# MAGIC - You walk back and forth between rooms for each guest (slow!)
# MAGIC
# MAGIC **Good Way (Fast)**:
# MAGIC - You carry all 1000 name tags in your pocket (broadcast)
# MAGIC - You match each guest instantly without walking
# MAGIC
# MAGIC That's **broadcast join** — send small data everywhere so matching is fast!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### Join Strategies in Spark:
# MAGIC
# MAGIC #### 1. 📡 **Broadcast Hash Join (BHJ)**
# MAGIC
# MAGIC **When:** One side is small (≤ 10MB by default)
# MAGIC
# MAGIC **How:**
# MAGIC ```
# MAGIC 1. Small table is broadcast to all executors
# MAGIC 2. Each executor builds a hash table
# MAGIC 3. Large table is streamed through hash table
# MAGIC 4. No shuffle required!
# MAGIC ```
# MAGIC
# MAGIC **Configuration:**
# MAGIC ```python
# MAGIC spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")
# MAGIC ```
# MAGIC
# MAGIC **Performance:** 🚀 **Fastest** (no shuffle)
# MAGIC
# MAGIC **Use Case:** Dimension table joins (e.g., customers, products)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2. 🔀 **Sort-Merge Join (SMJ)**
# MAGIC
# MAGIC **When:** Both sides are large
# MAGIC
# MAGIC **How:**
# MAGIC ```
# MAGIC 1. Shuffle both DataFrames by join key
# MAGIC 2. Sort each partition
# MAGIC 3. Merge sorted partitions
# MAGIC ```
# MAGIC
# MAGIC **Performance:** 🐢 Slower (requires shuffle + sort)
# MAGIC
# MAGIC **Use Case:** Large-large table joins
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 3. #️⃣ **Shuffle Hash Join (SHJ)**
# MAGIC
# MAGIC **When:** One side is smaller but not broadcastable
# MAGIC
# MAGIC **How:**
# MAGIC ```
# MAGIC 1. Shuffle both sides by join key
# MAGIC 2. Build hash table from smaller side
# MAGIC 3. Probe with larger side
# MAGIC ```
# MAGIC
# MAGIC **Performance:** 🐌 Medium
# MAGIC
# MAGIC **Use Case:** When broadcast is disabled or data exceeds threshold
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Performance Optimization Techniques:
# MAGIC
# MAGIC #### ✅ **Best Practices:**
# MAGIC
# MAGIC **1. Filter Before Join**
# MAGIC ```python
# MAGIC # BAD
# MAGIC df1.join(df2, "key").filter(col("date") > "2024-01-01")
# MAGIC
# MAGIC # GOOD
# MAGIC df1_filtered = df1.filter(col("date") > "2024-01-01")
# MAGIC df1_filtered.join(df2, "key")
# MAGIC ```
# MAGIC
# MAGIC **2. Broadcast Small Tables**
# MAGIC ```python
# MAGIC from pyspark.sql.functions import broadcast
# MAGIC
# MAGIC # Force broadcast
# MAGIC df_large.join(broadcast(df_small), "key")
# MAGIC ```
# MAGIC
# MAGIC **3. Partition on Join Keys**
# MAGIC ```python
# MAGIC # Write with partitioning
# MAGIC df.write.partitionBy("join_key").parquet("/path")
# MAGIC ```
# MAGIC
# MAGIC **4. Avoid Data Skew**
# MAGIC ```python
# MAGIC # Salting technique for skewed keys
# MAGIC df = df.withColumn("salted_key", concat(col("key"), lit("_"), (rand() * 10).cast("int")))
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### ⚠️ **Anti-Patterns:**
# MAGIC
# MAGIC ❌ **Cross Join without condition**
# MAGIC ```python
# MAGIC df1.crossJoin(df2)  # Cartesian product - DANGER!
# MAGIC ```
# MAGIC
# MAGIC ❌ **Multiple sequential joins without caching**
# MAGIC ```python
# MAGIC # Repeated computation
# MAGIC df1.join(df2, "k").join(df3, "k").join(df4, "k")
# MAGIC ```
# MAGIC
# MAGIC ❌ **Joining on non-partitioned keys**
# MAGIC ```python
# MAGIC # Forces full shuffle
# MAGIC large_df.join(another_large_df, col("a") == col("b"))
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Monitoring Join Performance:
# MAGIC
# MAGIC **Check Spark UI:**
# MAGIC 1. **SQL Tab** → View query plan
# MAGIC 2. Look for:
# MAGIC    - `BroadcastHashJoin` 🚀 (good)
# MAGIC    - `SortMergeJoin` 🐢 (acceptable)
# MAGIC    - `CartesianProduct` 🔥 (danger!)
# MAGIC
# MAGIC **Explain Plan:**
# MAGIC ```python
# MAGIC df_joined.explain("formatted")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 4 - Broadcast Join Example
# ========================================
# BROADCAST JOIN OPTIMIZATION
# ========================================

from pyspark.sql.functions import broadcast

print("\n" + "="*60)
print("Broadcast Join Demonstration")
print("="*60)

# Regular join (Spark decides strategy)
print("\n🔹 Regular Join:")
df_regular = df_employees.join(df_departments, "department")
print(f"   Row count: {df_regular.count()}")

# Force broadcast join
print("\n🔹 Broadcast Join (forced):")
df_broadcast = df_employees.join(
    broadcast(df_departments),  # Force broadcast
    "department"
)
print(f"   Row count: {df_broadcast.count()}")

print("""
\n💡 When to Use Broadcast:
- Small dimension tables (<10MB)
- Lookup tables, reference data
- Frequently joined small tables

⚡ Benefits:
- No shuffle required
- Much faster execution
- Lower memory overhead

⚠️ Warning:
- Don't broadcast large tables (>200MB)
- Can cause driver OOM errors
""")

# View execution plan
print("\n🔹 Execution Plan (Regular Join):")
df_regular.explain(mode="simple")

print("\n🔹 Execution Plan (Broadcast Join):")
df_broadcast.explain(mode="simple")

# COMMAND ----------

# DBTITLE 1,Section 4 - Filter Before Join
# ========================================
# OPTIMIZATION: Filter Before Join
# ========================================

print("\n" + "="*60)
print("Filter Before Join - Performance Optimization")
print("="*60)

# BAD: Join first, filter later
print("\n❌ BAD PRACTICE: Join then filter")
df_bad = df_employees.join(df_departments, "department") \
    .filter(col("salary") > 90000)

print(f"   Result count: {df_bad.count()}")
print("   ❗ Performs join on ALL rows, then filters")

# GOOD: Filter first, then join
print("\n✅ GOOD PRACTICE: Filter then join")
df_filtered_employees = df_employees.filter(col("salary") > 90000)
df_good = df_filtered_employees.join(df_departments, "department")

print(f"   Filtered employees: {df_filtered_employees.count()}")
print(f"   Result count: {df_good.count()}")
print("   ✅ Joins only filtered rows (fewer records = faster)")

print("""
\n🚀 Performance Impact:
- Fewer rows to shuffle
- Less memory consumption
- Faster join execution
- Lower network overhead

💡 Best Practice:
   ALWAYS filter DataFrames BEFORE joining when possible
""")

# COMMAND ----------

# DBTITLE 1,Section 4 - Join Configuration Settings
# ========================================
# JOIN CONFIGURATION SETTINGS (Serverless Note)
# ========================================

print("\n" + "="*60)
print("Spark Join Configuration on Serverless")
print("="*60)

print("""
⚠️  SERVERLESS NOTE:
Many Spark configurations are managed automatically on serverless compute
and cannot be accessed or modified directly.

📊 Default Join Optimizations (Serverless):

1. spark.sql.autoBroadcastJoinThreshold
   - Default: ~10MB (managed by serverless)
   - Tables smaller than threshold are automatically broadcast
   - Serverless optimizes this automatically
   - You can still use broadcast() function to force it

2. spark.sql.adaptive.enabled
   - Default: true (on Spark 3+)
   - Automatically enabled on serverless
   - Dynamically optimizes join strategy during execution
   - Can switch from SMJ to BHJ if data is smaller than expected

3. spark.sql.adaptive.autoBroadcastJoinThreshold
   - Used during runtime optimization
   - Automatically managed on serverless

4. spark.sql.shuffle.partitions
   - Default: 200 (or auto-optimized)
   - Serverless auto-tunes based on data size

""")

print("✅ Serverless Advantages:")
print("   - Automatic optimization (no manual tuning needed)")
print("   - Adaptive query execution enabled by default")
print("   - Dynamic resource allocation")
print("   - No configuration management overhead")

print("""
\n💡 Best Practices on Serverless:
   1. Use broadcast() function explicitly for small tables
   2. Filter before joining to reduce data size
   3. Partition on join keys when writing data
   4. Trust serverless to auto-optimize
   5. Monitor Spark UI for actual execution strategy

🔧 Example - Explicit Broadcast:
```python
from pyspark.sql.functions import broadcast

# Force broadcast for small dimension table
df_large.join(broadcast(df_small), "key")
```
""")

# COMMAND ----------

# DBTITLE 1,Section 5 - Window Functions Concept
# MAGIC %md
# MAGIC # 📊 SECTION 5: Window Functions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧒 ELI5 Explanation:
# MAGIC
# MAGIC Imagine you're in a classroom with students from different grades:
# MAGIC
# MAGIC **Regular Aggregation (GROUP BY):**
# MAGIC - Count students per grade
# MAGIC - Result: One row per grade (loses individual student info)
# MAGIC
# MAGIC **Window Function:**
# MAGIC - Rank students by score WITHIN each grade
# MAGIC - Result: Keep ALL students, add their rank within their grade
# MAGIC - You can see each student AND their relative position!
# MAGIC
# MAGIC Window functions let you **calculate across groups WITHOUT collapsing rows**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC **Window Functions** perform calculations across a set of rows (window) related to the current row, WITHOUT reducing the number of rows.
# MAGIC
# MAGIC ### Key Concepts:
# MAGIC
# MAGIC #### 1. **Window Specification**
# MAGIC ```python
# MAGIC Window.partitionBy("column")  # Define groups
# MAGIC       .orderBy("column")       # Define order within groups
# MAGIC       .rowsBetween(start, end) # Define frame (optional)
# MAGIC ```
# MAGIC
# MAGIC #### 2. **Window Frame Types**
# MAGIC
# MAGIC | Frame Type | Description | Example |
# MAGIC |------------|-------------|----------|
# MAGIC | **ROWS** | Physical offset | `ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING` |
# MAGIC | **RANGE** | Logical offset | `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` |
# MAGIC
# MAGIC #### 3. **Window Function Categories**
# MAGIC
# MAGIC **A. Ranking Functions:**
# MAGIC - `ROW_NUMBER()` → Sequential number (1, 2, 3...)
# MAGIC - `RANK()` → Rank with gaps (1, 2, 2, 4...)
# MAGIC - `DENSE_RANK()` → Rank without gaps (1, 2, 2, 3...)
# MAGIC - `NTILE(n)` → Divide into n buckets
# MAGIC - `PERCENT_RANK()` → Relative rank (0 to 1)
# MAGIC
# MAGIC **B. Analytic Functions:**
# MAGIC - `LEAD(col, n)` → Value n rows ahead
# MAGIC - `LAG(col, n)` → Value n rows behind
# MAGIC - `FIRST_VALUE(col)` → First value in window
# MAGIC - `LAST_VALUE(col)` → Last value in window
# MAGIC
# MAGIC **C. Aggregate Functions:**
# MAGIC - `SUM()`, `AVG()`, `MIN()`, `MAX()`, `COUNT()`
# MAGIC - When used with OVER(), they become window functions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Common Use Cases:
# MAGIC
# MAGIC 🏆 **Ranking & Top-N**
# MAGIC ```sql
# MAGIC -- Find top 3 earners per department
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC     SELECT *, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rank
# MAGIC     FROM employees
# MAGIC ) WHERE rank <= 3
# MAGIC ```
# MAGIC
# MAGIC 📈 **Running Totals**
# MAGIC ```sql
# MAGIC -- Calculate cumulative sales
# MAGIC SELECT date, sales,
# MAGIC        SUM(sales) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total
# MAGIC FROM daily_sales
# MAGIC ```
# MAGIC
# MAGIC 🔀 **Period-over-Period Comparison**
# MAGIC ```sql
# MAGIC -- Compare with previous month
# MAGIC SELECT month, revenue,
# MAGIC        LAG(revenue, 1) OVER (ORDER BY month) as prev_month_revenue,
# MAGIC        revenue - LAG(revenue, 1) OVER (ORDER BY month) as growth
# MAGIC FROM monthly_revenue
# MAGIC ```
# MAGIC
# MAGIC 🎯 **Moving Averages**
# MAGIC ```sql
# MAGIC -- 7-day moving average
# MAGIC SELECT date, value,
# MAGIC        AVG(value) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as ma_7
# MAGIC FROM time_series
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Performance Considerations:
# MAGIC
# MAGIC ✅ **Best Practices:**
# MAGIC - Partition by high-cardinality columns when possible
# MAGIC - Order by indexed/sorted columns
# MAGIC - Use appropriate frame specifications
# MAGIC - Combine multiple window functions with same spec
# MAGIC
# MAGIC ⚠️ **Warnings:**
# MAGIC - Window functions require sorting (can be expensive)
# MAGIC - Large windows can consume significant memory
# MAGIC - Avoid UNBOUNDED frames on huge datasets

# COMMAND ----------

# DBTITLE 1,Section 5 - Window Functions Basic Example
# ========================================
# WINDOW FUNCTIONS - Basic Example
# ========================================

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, ntile

print("\n" + "="*60)
print("Window Functions - Ranking Example")
print("="*60)

# Define window specification
window_spec = Window.partitionBy("department").orderBy(col("salary").desc())

print("\n🔹 Original Data:")
display(df_employees.orderBy("department", col("salary").desc()))

# Apply ranking functions
df_ranked = df_employees.withColumn(
    "row_number", row_number().over(window_spec)
).withColumn(
    "rank", rank().over(window_spec)
).withColumn(
    "dense_rank", dense_rank().over(window_spec)
).withColumn(
    "quartile", ntile(4).over(window_spec)
)

print("\n🔹 With Window Functions:")
display(df_ranked.select(
    "department",
    "name",
    "salary",
    "row_number",
    "rank",
    "dense_rank",
    "quartile"
).orderBy("department", "row_number"))

print("""
\n💡 Understanding the Differences:

- ROW_NUMBER: Sequential numbers (1, 2, 3, 4...)
  → Always unique, even for ties

- RANK: Ranking with gaps (1, 2, 2, 4...)
  → Same values get same rank, then skips

- DENSE_RANK: Ranking without gaps (1, 2, 2, 3...)
  → Same values get same rank, no skips

- NTILE(n): Divide into n buckets
  → Distributes rows into equal groups
""")

# COMMAND ----------

# DBTITLE 1,Section 5 - Top N per Group
# ========================================
# USE CASE: Top N per Group
# ========================================

print("\n" + "="*60)
print("Window Function Use Case: Top N per Group")
print("="*60)

# Find top 2 earners per department
window_top_n = Window.partitionBy("department").orderBy(col("salary").desc())

df_with_rank = df_employees.withColumn(
    "salary_rank",
    row_number().over(window_top_n)
)

# Filter to get top 2
df_top_2 = df_with_rank.filter(col("salary_rank") <= 2)

print("\n🏆 Top 2 Earners per Department:")
display(df_top_2.select(
    "department",
    "name",
    "salary",
    "salary_rank"
).orderBy("department", "salary_rank"))

print("""
\n🚀 This Pattern is Very Common:
- Top N products by sales per category
- Top N customers by revenue per region
- Top N students by score per class
- Latest N transactions per account
""")

# COMMAND ----------

# DBTITLE 1,Section 5 - LAG and LEAD Functions
# ========================================
# LAG and LEAD Functions
# ========================================

from pyspark.sql.functions import lag, lead

print("\n" + "="*60)
print("LAG and LEAD - Time Series Analysis")
print("="*60)

# Sort by hire date and use LAG/LEAD
window_time = Window.orderBy("hire_date")

df_with_lag_lead = df_employees.withColumn(
    "prev_hire_name",
    lag("name", 1).over(window_time)
).withColumn(
    "prev_hire_date",
    lag("hire_date", 1).over(window_time)
).withColumn(
    "next_hire_name",
    lead("name", 1).over(window_time)
).withColumn(
    "next_hire_date",
    lead("hire_date", 1).over(window_time)
)

print("\n🔹 Employee Hire Timeline with LAG/LEAD:")
display(df_with_lag_lead.select(
    "name",
    "hire_date",
    "prev_hire_name",
    "prev_hire_date",
    "next_hire_name",
    "next_hire_date"
).orderBy("hire_date"))

print("""
\n💡 LAG vs LEAD:

- LAG(col, n): Get value from n rows BEFORE current row
  → Look backward in time
  → Compare with previous period

- LEAD(col, n): Get value from n rows AFTER current row
  → Look forward in time
  → Predict or compare with next period

🎯 Common Use Cases:
- Month-over-month growth
- Day-over-day changes
- Detect anomalies (sudden jumps)
- Calculate gaps between events
""")

# COMMAND ----------

# DBTITLE 1,Section 5 - Running Totals and Aggregations
# ========================================
# RUNNING TOTALS - Cumulative Aggregations
# ========================================

from pyspark.sql.functions import sum as _sum, avg as _avg

print("\n" + "="*60)
print("Running Totals - Cumulative Aggregations")
print("="*60)

# Window for running total (unbounded preceding to current row)
window_running = Window.partitionBy("department") \
                      .orderBy("hire_date") \
                      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_running = df_employees.withColumn(
    "cumulative_salary",
    _sum("salary").over(window_running)
).withColumn(
    "avg_salary_so_far",
    _avg("salary").over(window_running)
).withColumn(
    "employee_count_so_far",
    row_number().over(Window.partitionBy("department").orderBy("hire_date"))
)

print("\n📈 Running Totals by Department:")
display(df_running.select(
    "department",
    "name",
    "hire_date",
    "salary",
    "employee_count_so_far",
    "cumulative_salary",
    "avg_salary_so_far"
).orderBy("department", "hire_date"))

print("""
\n💡 Frame Specifications:

rowsBetween(start, end):
- Window.unboundedPreceding: From beginning
- Window.currentRow: Current row
- Window.unboundedFollowing: To end
- n (integer): Specific offset

Examples:
- rowsBetween(Window.unboundedPreceding, Window.currentRow)
  → Running total from start to current

- rowsBetween(-3, 0)
  → Current row and 3 rows before

- rowsBetween(-1, 1)
  → Previous row, current row, next row
""")

# COMMAND ----------

# DBTITLE 1,Section 5 - Moving Average
# ========================================
# MOVING AVERAGE - Sliding Window
# ========================================

print("\n" + "="*60)
print("Moving Average - Sliding Window")
print("="*60)

# Create a simple time series for demo
from pyspark.sql.functions import lit, expr

# Add row numbers to simulate time series
window_order = Window.orderBy("hire_date")

df_timeseries = df_employees.withColumn(
    "day_number",
    row_number().over(window_order)
)

# 3-period moving average of salary
window_ma = Window.orderBy("hire_date") \
                 .rowsBetween(-2, 0)  # Current + 2 previous

df_with_ma = df_timeseries.withColumn(
    "salary_3ma",
    _avg("salary").over(window_ma)
).withColumn(
    "salary_3ma_count",
    count("salary").over(window_ma)
)

print("\n📉 3-Period Moving Average:")
display(df_with_ma.select(
    "day_number",
    "name",
    "hire_date",
    "salary",
    "salary_3ma",
    "salary_3ma_count"
).orderBy("hire_date"))

print("""
\n💡 Moving Average Use Cases:

- Smooth out noise in time series data
- Identify trends (7-day MA, 30-day MA)
- Stock price analysis (50-day, 200-day MA)
- Seasonality detection
- Anomaly detection

🔧 Frame Definition:
- rowsBetween(-2, 0): 3-period MA (current + 2 before)
- rowsBetween(-6, 0): 7-period MA
- rowsBetween(-29, 0): 30-period MA
""")

# COMMAND ----------

# DBTITLE 1,Section 6 - SQL Window Functions
# MAGIC %sql
# MAGIC -- ========================================
# MAGIC -- SECTION 6: SQL Window Functions
# MAGIC -- ========================================
# MAGIC
# MAGIC -- Ranking employees by salary within department
# MAGIC SELECT 
# MAGIC     department,
# MAGIC     name,
# MAGIC     salary,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as row_num,
# MAGIC     RANK() OVER (PARTITION BY department ORDER BY salary DESC) as rank,
# MAGIC     DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dense_rank,
# MAGIC     ROUND(PERCENT_RANK() OVER (PARTITION BY department ORDER BY salary DESC), 2) as percent_rank
# MAGIC FROM employees
# MAGIC ORDER BY department, salary DESC

# COMMAND ----------

# DBTITLE 1,Section 6 - SQL Running Totals
# MAGIC %sql
# MAGIC -- ========================================
# MAGIC -- SQL: Running Totals
# MAGIC -- ========================================
# MAGIC
# MAGIC SELECT 
# MAGIC     department,
# MAGIC     name,
# MAGIC     hire_date,
# MAGIC     salary,
# MAGIC     SUM(salary) OVER (
# MAGIC         PARTITION BY department 
# MAGIC         ORDER BY hire_date
# MAGIC         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) as cumulative_salary,
# MAGIC     AVG(salary) OVER (
# MAGIC         PARTITION BY department 
# MAGIC         ORDER BY hire_date
# MAGIC         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) as avg_salary_to_date,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC         PARTITION BY department 
# MAGIC         ORDER BY hire_date
# MAGIC     ) as hire_sequence
# MAGIC FROM employees
# MAGIC ORDER BY department, hire_date

# COMMAND ----------

# DBTITLE 1,Section 6 - SQL LAG and LEAD
# MAGIC %sql
# MAGIC -- ========================================
# MAGIC -- SQL: LAG and LEAD Functions
# MAGIC -- ========================================
# MAGIC
# MAGIC SELECT 
# MAGIC     name,
# MAGIC     department,
# MAGIC     hire_date,
# MAGIC     salary,
# MAGIC     LAG(salary, 1) OVER (PARTITION BY department ORDER BY hire_date) as prev_salary,
# MAGIC     LEAD(salary, 1) OVER (PARTITION BY department ORDER BY hire_date) as next_salary,
# MAGIC     salary - LAG(salary, 1) OVER (PARTITION BY department ORDER BY hire_date) as salary_diff_from_prev,
# MAGIC     LAG(name, 1) OVER (PARTITION BY department ORDER BY hire_date) as prev_hire,
# MAGIC     LEAD(name, 1) OVER (PARTITION BY department ORDER BY hire_date) as next_hire
# MAGIC FROM employees
# MAGIC ORDER BY department, hire_date

# COMMAND ----------

# DBTITLE 1,Section 6 - SQL Top N per Group
# MAGIC %sql
# MAGIC -- ========================================
# MAGIC -- SQL: Top N per Group (Top 2 Earners)
# MAGIC -- ========================================
# MAGIC
# MAGIC WITH ranked_employees AS (
# MAGIC     SELECT 
# MAGIC         department,
# MAGIC         name,
# MAGIC         salary,
# MAGIC         hire_date,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as salary_rank
# MAGIC     FROM employees
# MAGIC )
# MAGIC SELECT 
# MAGIC     department,
# MAGIC     name,
# MAGIC     salary,
# MAGIC     hire_date,
# MAGIC     salary_rank
# MAGIC FROM ranked_employees
# MAGIC WHERE salary_rank <= 2
# MAGIC ORDER BY department, salary_rank

# COMMAND ----------

# DBTITLE 1,Section 7 - Hands-on Analytics Pipeline
# MAGIC %md
# MAGIC # 🚀 SECTION 7: Hands-on Analytics Pipeline
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Objective:
# MAGIC Build a complete analytics pipeline that combines:
# MAGIC 1. **Data ingestion** (Unity Catalog Volumes)
# MAGIC 2. **Transformations** (DataFrame API)
# MAGIC 3. **Joins** (enrichment)
# MAGIC 4. **Window functions** (analytics)
# MAGIC 5. **Results** (insights)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Business Scenario:
# MAGIC
# MAGIC **Acme Corp** wants to analyze:
# MAGIC - Employee performance by department
# MAGIC - Salary distribution and rankings
# MAGIC - Departmental budget utilization
# MAGIC - Growth trends over time
# MAGIC
# MAGIC We'll build an analytics pipeline to answer these questions.

# COMMAND ----------

# DBTITLE 1,Section 7 - Step 1 - Prepare Enhanced Data
# ========================================
# STEP 1: Prepare Enhanced Employee Data
# ========================================

from pyspark.sql.functions import *
from datetime import datetime, timedelta
import random

print("\n" + "="*60)
print("Building Analytics Pipeline - Step 1: Data Preparation")
print("="*60)

# Create more realistic employee data with performance metrics
enhanced_employee_data = [
    (1, "Alice", "Engineering", 95000, "2020-01-15", 4.5, 12),
    (2, "Bob", "Sales", 75000, "2019-03-20", 4.2, 8),
    (3, "Charlie", "Engineering", 105000, "2018-07-10", 4.8, 15),
    (4, "Diana", "HR", 65000, "2021-05-12", 4.0, 5),
    (5, "Eve", "Sales", 82000, "2020-11-08", 4.6, 10),
    (6, "Frank", "Engineering", 98000, "2019-09-25", 4.4, 11),
    (7, "Grace", "HR", 70000, "2021-01-30", 4.3, 6),
    (8, "Henry", "Sales", 78000, "2020-04-17", 4.1, 9),
    (9, "Ivy", "Engineering", 110000, "2017-12-05", 4.9, 18),
    (10, "Jack", "Sales", 88000, "2019-08-22", 4.7, 13),
    (11, "Karen", "Engineering", 92000, "2020-06-15", 4.3, 10),
    (12, "Leo", "Sales", 85000, "2019-11-10", 4.5, 12),
    (13, "Mia", "HR", 68000, "2021-03-25", 4.2, 6),
    (14, "Nathan", "Engineering", 102000, "2018-10-30", 4.7, 16),
    (15, "Olivia", "Sales", 79000, "2020-09-14", 4.4, 9)
]

enhanced_schema = StructType([
    StructField("emp_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("department", StringType(), False),
    StructField("salary", IntegerType(), False),
    StructField("hire_date", StringType(), False),
    StructField("performance_rating", DoubleType(), False),
    StructField("projects_completed", IntegerType(), False)
])

df_emp_enhanced = spark.createDataFrame(enhanced_employee_data, enhanced_schema)

print("✅ Enhanced employee data created")
print(f"   Total employees: {df_emp_enhanced.count()}")
print(f"   Departments: {df_emp_enhanced.select('department').distinct().count()}")

display(df_emp_enhanced)

# COMMAND ----------

# DBTITLE 1,Section 7 - Step 2 - Enrich with Joins
# ========================================
# STEP 2: Enrich with Department Data (Join)
# ========================================

print("\n" + "="*60)
print("Step 2: Enrichment via Joins")
print("="*60)

# Create temp views
df_emp_enhanced.createOrReplaceTempView("emp_enhanced")
df_departments.createOrReplaceTempView("dept_info")

# Perform enrichment join
df_enriched = spark.sql("""
    SELECT 
        e.*,
        d.budget as dept_budget,
        d.manager as dept_manager,
        d.description as dept_description,
        ROUND((e.salary / d.budget) * 100, 2) as salary_pct_of_budget
    FROM emp_enhanced e
    INNER JOIN dept_info d
        ON e.department = d.department
""")

print("✅ Data enriched with department information")
print(f"   Enriched records: {df_enriched.count()}")

display(df_enriched.select(
    "emp_id", "name", "department", "salary",
    "dept_budget", "salary_pct_of_budget", "dept_manager"
))

# COMMAND ----------

# DBTITLE 1,Section 7 - Step 3 - Apply Window Functions
# ========================================
# STEP 3: Apply Window Functions for Analytics
# ========================================

print("\n" + "="*60)
print("Step 3: Advanced Analytics with Window Functions")
print("="*60)

# Create comprehensive analytics view
df_enriched.createOrReplaceTempView("enriched_data")

df_analytics = spark.sql("""
    SELECT 
        emp_id,
        name,
        department,
        salary,
        performance_rating,
        projects_completed,
        hire_date,
        dept_budget,
        dept_manager,
        
        -- Salary Rankings
        ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as salary_rank_in_dept,
        ROW_NUMBER() OVER (ORDER BY salary DESC) as salary_rank_company,
        
        -- Performance Rankings
        ROW_NUMBER() OVER (PARTITION BY department ORDER BY performance_rating DESC) as perf_rank_in_dept,
        
        -- Salary Percentiles
        NTILE(4) OVER (PARTITION BY department ORDER BY salary) as salary_quartile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY department ORDER BY salary), 2) as salary_percentile,
        
        -- Department Aggregates (for comparison)
        AVG(salary) OVER (PARTITION BY department) as dept_avg_salary,
        MAX(salary) OVER (PARTITION BY department) as dept_max_salary,
        MIN(salary) OVER (PARTITION BY department) as dept_min_salary,
        
        -- Running Totals
        SUM(salary) OVER (PARTITION BY department ORDER BY hire_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as dept_cumulative_salary,
        
        -- Tenure-based Analysis
        LAG(salary, 1) OVER (PARTITION BY department ORDER BY hire_date) as prev_hire_salary,
        LEAD(salary, 1) OVER (PARTITION BY department ORDER BY hire_date) as next_hire_salary,
        
        -- Comparative Metrics
        ROUND(salary - AVG(salary) OVER (PARTITION BY department), 2) as salary_vs_dept_avg,
        ROUND((salary / AVG(salary) OVER (PARTITION BY department) - 1) * 100, 2) as salary_pct_vs_dept_avg
        
    FROM enriched_data
""")

print("✅ Analytics transformations completed")
print(f"   Total records with analytics: {df_analytics.count()}")

# Store for further analysis
df_analytics.createOrReplaceTempView("employee_analytics")

print("\n🔹 Sample Analytics Output:")
display(df_analytics.select(
    "name", "department", "salary",
    "salary_rank_in_dept", "salary_quartile",
    "dept_avg_salary", "salary_vs_dept_avg"
).orderBy("department", "salary_rank_in_dept"))

# COMMAND ----------

# DBTITLE 1,Section 7 - Step 4 - Top Performers Analysis
# MAGIC %sql
# MAGIC -- ========================================
# MAGIC -- STEP 4: Top Performers Analysis
# MAGIC -- ========================================
# MAGIC
# MAGIC -- Find top performers: High performance + salary in top quartile
# MAGIC SELECT 
# MAGIC     department,
# MAGIC     name,
# MAGIC     salary,
# MAGIC     performance_rating,
# MAGIC     projects_completed,
# MAGIC     salary_rank_in_dept,
# MAGIC     perf_rank_in_dept,
# MAGIC     salary_quartile,
# MAGIC     CASE 
# MAGIC         WHEN performance_rating >= 4.5 AND salary_quartile = 4 THEN 'Star Performer'
# MAGIC         WHEN performance_rating >= 4.5 THEN 'High Performer'
# MAGIC         WHEN salary_quartile = 4 THEN 'High Earner'
# MAGIC         ELSE 'Standard'
# MAGIC     END as employee_category
# MAGIC FROM employee_analytics
# MAGIC WHERE salary_quartile >= 3 OR performance_rating >= 4.4
# MAGIC ORDER BY department, performance_rating DESC, salary DESC

# COMMAND ----------

# DBTITLE 1,Section 7 - Step 5 - Departmental Dashboard
# MAGIC %sql
# MAGIC -- ========================================
# MAGIC -- STEP 5: Departmental Executive Dashboard
# MAGIC -- ========================================
# MAGIC
# MAGIC SELECT 
# MAGIC     department,
# MAGIC     dept_manager,
# MAGIC     dept_budget,
# MAGIC     COUNT(*) as total_employees,
# MAGIC     ROUND(AVG(salary), 2) as avg_salary,
# MAGIC     ROUND(MIN(salary), 2) as min_salary,
# MAGIC     ROUND(MAX(salary), 2) as max_salary,
# MAGIC     ROUND(SUM(salary), 2) as total_salary_cost,
# MAGIC     ROUND(dept_budget - SUM(salary), 2) as remaining_budget,
# MAGIC     ROUND((SUM(salary) / dept_budget) * 100, 2) as budget_utilization_pct,
# MAGIC     ROUND(AVG(performance_rating), 2) as avg_performance,
# MAGIC     SUM(projects_completed) as total_projects,
# MAGIC     ROUND(AVG(projects_completed), 1) as avg_projects_per_employee
# MAGIC FROM employee_analytics
# MAGIC GROUP BY department, dept_manager, dept_budget
# MAGIC ORDER BY budget_utilization_pct DESC

# COMMAND ----------

# DBTITLE 1,Section 7 - Step 6 - Insights and Recommendations
# MAGIC %sql
# MAGIC -- ========================================
# MAGIC -- STEP 6: Insights - Underutilized Budget
# MAGIC -- ========================================
# MAGIC
# MAGIC -- Find departments with room to hire or give raises
# MAGIC WITH dept_summary AS (
# MAGIC     SELECT 
# MAGIC         department,
# MAGIC         dept_budget,
# MAGIC         SUM(salary) as total_salary_cost,
# MAGIC         dept_budget - SUM(salary) as available_budget,
# MAGIC         COUNT(*) as employee_count,
# MAGIC         ROUND(AVG(salary), 2) as avg_salary,
# MAGIC         ROUND(AVG(performance_rating), 2) as avg_performance
# MAGIC     FROM employee_analytics
# MAGIC     GROUP BY department, dept_budget
# MAGIC )
# MAGIC SELECT 
# MAGIC     department,
# MAGIC     employee_count,
# MAGIC     total_salary_cost,
# MAGIC     available_budget,
# MAGIC     avg_salary,
# MAGIC     avg_performance,
# MAGIC     ROUND((available_budget / avg_salary), 0) as potential_new_hires,
# MAGIC     ROUND((available_budget / employee_count), 2) as potential_raise_per_employee,
# MAGIC     CASE 
# MAGIC         WHEN available_budget > avg_salary * 2 THEN 'High capacity - consider hiring'
# MAGIC         WHEN available_budget > avg_salary THEN 'Moderate capacity'
# MAGIC         ELSE 'Limited capacity'
# MAGIC     END as recommendation
# MAGIC FROM dept_summary
# MAGIC ORDER BY available_budget DESC

# COMMAND ----------

# DBTITLE 1,Section 8 - End-to-End Pipeline
# MAGIC %md
# MAGIC # 🎯 SECTION 8: End-to-End SQL + DataFrame Pipeline
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Hybrid Architecture Pattern
# MAGIC
# MAGIC **Best Practice:** Combine DataFrame API (for ETL) with SQL (for analytics)
# MAGIC
# MAGIC ```
# MAGIC 📊 Data Flow:
# MAGIC
# MAGIC 1. INGESTION (DataFrame API)
# MAGIC    ↓
# MAGIC 2. CLEANSING (DataFrame API)
# MAGIC    ↓
# MAGIC 3. TRANSFORMATION (DataFrame API)
# MAGIC    ↓
# MAGIC 4. CREATE TEMP VIEW
# MAGIC    ↓
# MAGIC 5. ANALYTICS (SQL)
# MAGIC    ↓
# MAGIC 6. VISUALIZATION
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Why Hybrid Approach?
# MAGIC
# MAGIC ✅ **DataFrame API Benefits:**
# MAGIC - Type safety
# MAGIC - Better for complex ETL logic
# MAGIC - Programmatic transformations
# MAGIC - Easier to test and debug
# MAGIC
# MAGIC ✅ **SQL Benefits:**
# MAGIC - Familiar to analysts
# MAGIC - Readable analytics queries
# MAGIC - Great for reporting
# MAGIC - Standard business logic
# MAGIC
# MAGIC ✅ **Combined:**
# MAGIC - Best of both worlds
# MAGIC - **Same execution plan** (Catalyst optimizes both)
# MAGIC - Team flexibility
# MAGIC - Maintainable code

# COMMAND ----------

# DBTITLE 1,Section 8 - Complete Hybrid Pipeline Example
# ========================================
# COMPLETE HYBRID PIPELINE EXAMPLE
# ========================================

print("\n" + "="*80)
print("END-TO-END HYBRID PIPELINE: DataFrame API + SQL")
print("="*80)

print("""
\n📍 Pipeline Architecture:

Stage 1: Data Ingestion (DataFrame API)
   ↓
Stage 2: Data Quality & Cleansing (DataFrame API)
   ↓
Stage 3: Business Transformations (DataFrame API)
   ↓
Stage 4: Create SQL Views
   ↓
Stage 5: Analytics & Reporting (SQL)
   ↓
Stage 6: Results & Visualization

""")

# ==========================================
# STAGE 1: INGESTION (DataFrame API)
# ==========================================
print("\n" + "="*60)
print("Stage 1: Data Ingestion")
print("="*60)

# Simulate reading from Unity Catalog Volume
# In production: spark.read.parquet("/Volumes/catalog/schema/volume/path")
df_raw_employees = df_emp_enhanced
df_raw_departments = df_departments

print(f"✅ Loaded {df_raw_employees.count()} employees")
print(f"✅ Loaded {df_raw_departments.count()} departments")


# ==========================================
# STAGE 2: DATA QUALITY (DataFrame API)
# ==========================================
print("\n" + "="*60)
print("Stage 2: Data Quality Checks")
print("="*60)

# Quality checks
df_employees_clean = df_raw_employees \
    .filter(col("salary").isNotNull()) \
    .filter(col("salary") > 0) \
    .filter(col("performance_rating") >= 1.0) \
    .filter(col("performance_rating") <= 5.0)

print(f"✅ Quality checks passed: {df_employees_clean.count()} records")


# ==========================================
# STAGE 3: TRANSFORMATIONS (DataFrame API)
# ==========================================
print("\n" + "="*60)
print("Stage 3: Business Transformations")
print("="*60)

# Add calculated fields
df_transformed = df_employees_clean \
    .withColumn("annual_bonus", col("salary") * 0.15) \
    .withColumn("total_comp", col("salary") * 1.15) \
    .withColumn("tenure_days", datediff(current_date(), col("hire_date"))) \
    .withColumn("tenure_years", round(col("tenure_days") / 365, 1)) \
    .withColumn("salary_band",
        when(col("salary") >= 100000, "Senior")
        .when(col("salary") >= 80000, "Mid")
        .otherwise("Junior")
    )

print("✅ Business transformations applied")
print("   Added: annual_bonus, total_comp, tenure_years, salary_band")


# ==========================================
# STAGE 4: CREATE SQL VIEWS
# ==========================================
print("\n" + "="*60)
print("Stage 4: Creating SQL Views")
print("="*60)

df_transformed.createOrReplaceTempView("employees_transformed")
df_raw_departments.createOrReplaceTempView("departments_dim")

print("✅ SQL views created:")
print("   - employees_transformed")
print("   - departments_dim")


# ==========================================
# STAGE 5: ANALYTICS (SQL)
# ==========================================
print("\n" + "="*60)
print("Stage 5: SQL Analytics")
print("="*60)
print("   See next SQL cells for analytics queries...")

print("""
\n✅ Pipeline Complete!

💡 Key Benefits:
- DataFrame API for ETL (type-safe, testable)
- SQL for analytics (readable, standard)
- Same optimization (Catalyst)
- Team flexibility (engineers use DF, analysts use SQL)
- Maintainable and scalable
""")

# COMMAND ----------

# DBTITLE 1,Section 8 - Final Analytics Query
# MAGIC %sql
# MAGIC -- ========================================
# MAGIC -- STAGE 5: FINAL ANALYTICS (SQL)
# MAGIC -- ========================================
# MAGIC
# MAGIC -- Comprehensive Employee Analytics Report
# MAGIC WITH employee_metrics AS (
# MAGIC     SELECT 
# MAGIC         e.*,
# MAGIC         d.budget as dept_budget,
# MAGIC         d.manager as dept_manager,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY e.department ORDER BY e.salary DESC) as dept_rank,
# MAGIC         ROUND(AVG(e.salary) OVER (PARTITION BY e.department), 2) as dept_avg_salary,
# MAGIC         ROUND(e.salary / d.budget * 100, 2) as salary_pct_of_budget
# MAGIC     FROM employees_transformed e
# MAGIC     INNER JOIN departments_dim d ON e.department = d.department
# MAGIC )
# MAGIC SELECT 
# MAGIC     name,
# MAGIC     department,
# MAGIC     salary_band,
# MAGIC     salary,
# MAGIC     total_comp,
# MAGIC     performance_rating,
# MAGIC     tenure_years,
# MAGIC     dept_rank,
# MAGIC     dept_avg_salary,
# MAGIC     ROUND(salary - dept_avg_salary, 2) as salary_vs_avg,
# MAGIC     salary_pct_of_budget,
# MAGIC     CASE 
# MAGIC         WHEN performance_rating >= 4.5 AND dept_rank <= 3 THEN 'Top Talent'
# MAGIC         WHEN performance_rating >= 4.0 THEN 'Strong Performer'
# MAGIC         ELSE 'Developing'
# MAGIC     END as talent_category
# MAGIC FROM employee_metrics
# MAGIC ORDER BY department, dept_rank

# COMMAND ----------

# DBTITLE 1,Genie Code Examples
# MAGIC %md
# MAGIC # 🪄 Genie Code Agent - Example Prompts
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## How to Use Genie Code for This Notebook:
# MAGIC
# MAGIC ### 1. **Generate Joins:**
# MAGIC ```
# MAGIC Prompt: "Join employees and departments tables on department field using inner join"
# MAGIC ```
# MAGIC
# MAGIC ### 2. **Optimize Join Strategy:**
# MAGIC ```
# MAGIC Prompt: "Add broadcast hint to the departments join to optimize performance"
# MAGIC ```
# MAGIC
# MAGIC ### 3. **Create Window Function:**
# MAGIC ```
# MAGIC Prompt: "Rank employees by salary within each department using window function"
# MAGIC ```
# MAGIC
# MAGIC ### 4. **Convert PySpark to SQL:**
# MAGIC ```
# MAGIC Prompt: "Convert this DataFrame operation to equivalent SQL query"
# MAGIC ```
# MAGIC
# MAGIC ### 5. **Generate Analytics:**
# MAGIC ```
# MAGIC Prompt: "Calculate running total of salaries by department ordered by hire date"
# MAGIC ```
# MAGIC
# MAGIC ### 6. **Top-N Query:**
# MAGIC ```
# MAGIC Prompt: "Find top 3 employees by performance rating in each department"
# MAGIC ```
# MAGIC
# MAGIC ### 7. **LAG/LEAD Analysis:**
# MAGIC ```
# MAGIC Prompt: "Add LAG function to compare each employee salary with previous hire"
# MAGIC ```
# MAGIC
# MAGIC ### 8. **Moving Average:**
# MAGIC ```
# MAGIC Prompt: "Calculate 3-period moving average of salaries ordered by hire date"
# MAGIC ```
# MAGIC
# MAGIC ### 9. **Complex Analytics:**
# MAGIC ```
# MAGIC Prompt: "Create a comprehensive dashboard showing department budgets, employee counts, average salaries, and top performers"
# MAGIC ```
# MAGIC
# MAGIC ### 10. **Debug Query:**
# MAGIC ```
# MAGIC Prompt: "Why is my window function returning NULL values?"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Tips for Effective Prompts:
# MAGIC
# MAGIC 1. **Be specific** about table/column names
# MAGIC 2. **Mention the operation** explicitly (join type, window function)
# MAGIC 3. **Specify ordering** for window functions
# MAGIC 4. **Ask for optimization** when needed
# MAGIC 5. **Request explanations** for complex queries
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Example Workflow:
# MAGIC
# MAGIC ```
# MAGIC You: "Create a window function to rank employees by salary in each department"
# MAGIC
# MAGIC Genie: [Generates code with ROW_NUMBER()]
# MAGIC
# MAGIC You: "Now filter to show only top 3 per department"
# MAGIC
# MAGIC Genie: [Adds filter condition]
# MAGIC
# MAGIC You: "Convert this to SQL"
# MAGIC
# MAGIC Genie: [Provides SQL equivalent]
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Final Summary and Interview Questions
# MAGIC %md
# MAGIC # 🎓 FINAL SUMMARY
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Key Learnings:
# MAGIC
# MAGIC ### 1. **Spark Actions:**
# MAGIC - Actions trigger job execution (show, count, collect)
# MAGIC - Transformations are lazy (filter, select, join)
# MAGIC - Avoid `collect()` on large datasets
# MAGIC - Use `display()` in Databricks for better visualization
# MAGIC
# MAGIC ### 2. **Spark SQL:**
# MAGIC - SQL interface to Spark DataFrames
# MAGIC - Create temporary views for SQL queries
# MAGIC - Same optimization as DataFrame API (Catalyst)
# MAGIC - Hybrid approach: DataFrame for ETL, SQL for analytics
# MAGIC
# MAGIC ### 3. **Joins:**
# MAGIC - **Inner**: Only matching rows
# MAGIC - **Left**: All left + matching right
# MAGIC - **Right**: All right + matching left
# MAGIC - **Full Outer**: All rows from both sides
# MAGIC - **Semi/Anti**: Filtering joins
# MAGIC
# MAGIC ### 4. **Join Optimization:**
# MAGIC - **Broadcast joins** for small tables (<10MB)
# MAGIC - **Filter before joining** to reduce data size
# MAGIC - **Partition on join keys** for better performance
# MAGIC - Monitor execution plans in Spark UI
# MAGIC
# MAGIC ### 5. **Window Functions:**
# MAGIC - Calculate across groups without collapsing rows
# MAGIC - **Ranking**: ROW_NUMBER, RANK, DENSE_RANK
# MAGIC - **Analytics**: LAG, LEAD, FIRST_VALUE, LAST_VALUE
# MAGIC - **Aggregations**: SUM, AVG with OVER clause
# MAGIC - Define window: PARTITION BY + ORDER BY + ROWS/RANGE
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❓ Interview Questions:
# MAGIC
# MAGIC ### Basic:
# MAGIC 1. **What's the difference between a transformation and an action in Spark?**
# MAGIC    - Transformations are lazy (build DAG), actions trigger execution
# MAGIC
# MAGIC 2. **Why is `collect()` dangerous on large datasets?**
# MAGIC    - Brings all data to driver memory, can cause OOM
# MAGIC
# MAGIC 3. **What are the main types of joins in Spark?**
# MAGIC    - Inner, Left, Right, Full Outer, Cross, Semi, Anti
# MAGIC
# MAGIC 4. **Explain the difference between ROW_NUMBER, RANK, and DENSE_RANK.**
# MAGIC    - ROW_NUMBER: sequential (1,2,3,4)
# MAGIC    - RANK: gaps for ties (1,2,2,4)
# MAGIC    - DENSE_RANK: no gaps (1,2,2,3)
# MAGIC
# MAGIC ### Intermediate:
# MAGIC 5. **How does Spark decide between Broadcast Join and Sort-Merge Join?**
# MAGIC    - Checks table size against `spark.sql.autoBroadcastJoinThreshold` (default 10MB)
# MAGIC    - Broadcasts small tables, uses SMJ for large-large joins
# MAGIC
# MAGIC 6. **What's the difference between LEFT SEMI JOIN and INNER JOIN?**
# MAGIC    - Semi join returns only left table columns where match exists
# MAGIC    - Inner join returns columns from both tables
# MAGIC
# MAGIC 7. **How do you create a running total in Spark SQL?**
# MAGIC    - `SUM(col) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`
# MAGIC
# MAGIC 8. **What's the difference between ROWS and RANGE in window frames?**
# MAGIC    - ROWS: physical row offset
# MAGIC    - RANGE: logical value-based offset
# MAGIC
# MAGIC ### Advanced:
# MAGIC 9. **How would you handle data skew in joins?**
# MAGIC    - Salting: add random suffix to skewed keys
# MAGIC    - Broadcast: if one side is small
# MAGIC    - Repartition: balance data distribution
# MAGIC    - Adaptive execution: let Spark optimize at runtime
# MAGIC
# MAGIC 10. **Explain the execution plan for a window function query.**
# MAGIC     - Parse SQL → Logical plan → Catalyst optimization
# MAGIC     - Physical plan: Exchange (shuffle) + Sort + Window
# MAGIC     - Window operator processes ordered partitions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚠️ Common Mistakes:
# MAGIC
# MAGIC ### 1. **Using `collect()` on Large Data**
# MAGIC ```python
# MAGIC # BAD
# MAGIC all_data = large_df.collect()  # OOM error!
# MAGIC
# MAGIC # GOOD
# MAGIC large_df.show(10)  # Just preview
# MAGIC large_df.take(10)  # Fetch limited rows
# MAGIC ```
# MAGIC
# MAGIC ### 2. **Incorrect Join Logic**
# MAGIC ```python
# MAGIC # BAD: Wrong join type
# MAGIC df1.join(df2, "key", "inner")  # Excludes non-matching
# MAGIC
# MAGIC # GOOD: Use left join to keep all df1 records
# MAGIC df1.join(df2, "key", "left")
# MAGIC ```
# MAGIC
# MAGIC ### 3. **Ignoring Join Performance**
# MAGIC ```python
# MAGIC # BAD: Join then filter
# MAGIC large_df.join(another_large_df, "key").filter(col > 100)
# MAGIC
# MAGIC # GOOD: Filter then join
# MAGIC filtered = large_df.filter(col > 100)
# MAGIC filtered.join(another_large_df, "key")
# MAGIC ```
# MAGIC
# MAGIC ### 4. **Misusing Window Functions**
# MAGIC ```sql
# MAGIC -- BAD: Missing ORDER BY for ranking
# MAGIC ROW_NUMBER() OVER (PARTITION BY dept)  -- ERROR!
# MAGIC
# MAGIC -- GOOD: Include ORDER BY
# MAGIC ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)
# MAGIC ```
# MAGIC
# MAGIC ### 5. **Not Using Broadcast for Small Tables**
# MAGIC ```python
# MAGIC # BAD: Let Spark decide (might shuffle small table)
# MAGIC large_df.join(small_df, "key")
# MAGIC
# MAGIC # GOOD: Force broadcast
# MAGIC large_df.join(broadcast(small_df), "key")
# MAGIC ```
# MAGIC
# MAGIC ### 6. **Forgetting Window Frame**
# MAGIC ```sql
# MAGIC -- BAD: Unbounded frame (entire partition)
# MAGIC SUM(amount) OVER (PARTITION BY customer ORDER BY date)
# MAGIC
# MAGIC -- GOOD: Explicit frame for running total
# MAGIC SUM(amount) OVER (
# MAGIC     PARTITION BY customer 
# MAGIC     ORDER BY date 
# MAGIC     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### 7. **Cross Join Without Condition**
# MAGIC ```python
# MAGIC # DANGER: Cartesian product!
# MAGIC df1.crossJoin(df2)  # n * m rows
# MAGIC
# MAGIC # GOOD: Use proper join condition
# MAGIC df1.join(df2, df1.id == df2.id)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Next Steps:
# MAGIC
# MAGIC 1. **Practice** window functions on real datasets
# MAGIC 2. **Monitor** Spark UI for join strategies
# MAGIC 3. **Experiment** with different join types
# MAGIC 4. **Read** execution plans to understand performance
# MAGIC 5. **Optimize** queries based on data characteristics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Additional Resources:
# MAGIC
# MAGIC - Databricks SQL Reference: [docs.databricks.com/sql](https://docs.databricks.com/sql)
# MAGIC - Spark SQL Programming Guide: [spark.apache.org/docs](https://spark.apache.org/docs/latest/sql-programming-guide.html)
# MAGIC - Window Functions Deep Dive: Databricks Blog
# MAGIC - Join Optimization Strategies: Spark Performance Tuning Guide
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👏 Congratulations!
# MAGIC
# MAGIC You've completed **Phase 3 Day 16: Spark Actions & SQL**
# MAGIC
# MAGIC You now understand:
# MAGIC - ✅ How actions trigger execution
# MAGIC - ✅ How to write efficient joins
# MAGIC - ✅ How to use window functions for advanced analytics
# MAGIC - ✅ How to build hybrid DataFrame + SQL pipelines
# MAGIC
# MAGIC **Keep practicing and building! 🚀**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**

# COMMAND ----------

# DBTITLE 1,Advanced Window Functions - LEAD LAG FIRST LAST
# MAGIC %md
# MAGIC # 🔥 BONUS: Advanced Window Functions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Additional Window Functions:
# MAGIC
# MAGIC ### 1. **FIRST_VALUE & LAST_VALUE**
# MAGIC
# MAGIC Get the first or last value within a window.
# MAGIC
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     name,
# MAGIC     department,
# MAGIC     salary,
# MAGIC     FIRST_VALUE(salary) OVER (PARTITION BY department ORDER BY salary DESC) as highest_salary_in_dept,
# MAGIC     LAST_VALUE(salary) OVER (PARTITION BY department ORDER BY salary DESC
# MAGIC         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as lowest_salary_in_dept
# MAGIC FROM employees
# MAGIC ```
# MAGIC
# MAGIC **Note:** LAST_VALUE requires explicit frame to UNBOUNDED FOLLOWING
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. **CUME_DIST - Cumulative Distribution**
# MAGIC
# MAGIC Calculates the relative position of a value in a group.
# MAGIC
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     name,
# MAGIC     salary,
# MAGIC     CUME_DIST() OVER (ORDER BY salary) as cumulative_dist
# MAGIC FROM employees
# MAGIC -- Returns: What percentage of values are <= current value?
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. **NTH_VALUE - Get Nth Value**
# MAGIC
# MAGIC Retrieve the Nth value in a window.
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import nth_value
# MAGIC
# MAGIC window_spec = Window.partitionBy("department").orderBy(col("salary").desc())
# MAGIC
# MAGIC df.withColumn(
# MAGIC     "second_highest_salary",
# MAGIC     nth_value("salary", 2).over(window_spec)
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4. **Complex Frame Specifications**
# MAGIC
# MAGIC **Moving Average (3 periods):**
# MAGIC ```sql
# MAGIC AVG(value) OVER (
# MAGIC     ORDER BY date
# MAGIC     ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Centered Moving Average:**
# MAGIC ```sql
# MAGIC AVG(value) OVER (
# MAGIC     ORDER BY date
# MAGIC     ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Year-to-Date Total:**
# MAGIC ```sql
# MAGIC SUM(amount) OVER (
# MAGIC     PARTITION BY YEAR(date)
# MAGIC     ORDER BY date
# MAGIC     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Performance Tips:
# MAGIC
# MAGIC 1. **Reuse Window Specifications:**
# MAGIC ```python
# MAGIC # GOOD: Define once, use multiple times
# MAGIC window_spec = Window.partitionBy("dept").orderBy("salary")
# MAGIC
# MAGIC df.withColumn("rank", rank().over(window_spec)) \
# MAGIC   .withColumn("row_num", row_number().over(window_spec))
# MAGIC ```
# MAGIC
# MAGIC 2. **Minimize Window Operations:**
# MAGIC ```python
# MAGIC # BAD: Multiple passes
# MAGIC df.withColumn("rank1", rank().over(w1)) \
# MAGIC   .withColumn("rank2", rank().over(w2))
# MAGIC
# MAGIC # GOOD: Same window spec = single pass
# MAGIC ```
# MAGIC
# MAGIC 3. **Use Appropriate Frame:**
# MAGIC ```sql
# MAGIC -- Unnecessary frame:
# MAGIC ROW_NUMBER() OVER (ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
# MAGIC
# MAGIC -- Better: ranking doesn't need explicit frame
# MAGIC ROW_NUMBER() OVER (ORDER BY salary)
# MAGIC ```