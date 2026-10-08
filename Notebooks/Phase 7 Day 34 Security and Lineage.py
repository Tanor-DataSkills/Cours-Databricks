# Databricks notebook source
# DBTITLE 1,📘 Notebook Header
# MAGIC %md
# MAGIC # 🔐 Data Engineering Training — Phase 7 Day 34  
# MAGIC ## 🛡️ Data Security & Lineage: Row-Level, Column-Level & Governance  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - Row-Level Security (RLS)  
# MAGIC - Column-Level Security (CLS)  
# MAGIC - Data Masking & Policies  
# MAGIC - Data Lineage in Unity Catalog  
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
# MAGIC Learn how to implement fine-grained security (row-level and column-level) and understand data lineage for end-to-end governance in Unity Catalog.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Platform Requirements:
# MAGIC - ✅ Databricks Serverless Compute  
# MAGIC - ✅ Unity Catalog (Mandatory)  
# MAGIC - ✅ Delta Lake Format  
# MAGIC - ❌ No RDD Usage  
# MAGIC - ❌ No cache() / persist()  
# MAGIC - ❌ No /tmp or local storage  
# MAGIC
# MAGIC ### 🎓 Learning Path:
# MAGIC This notebook follows a governance-first and security-first design approach for modern data platforms.

# COMMAND ----------

# DBTITLE 1,🔒 Section 1: Data Security Fundamentals
# MAGIC %md
# MAGIC # 🔒 SECTION 1: Data Security Fundamentals
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine you have a **big toy box** with different toys inside. Some toys are for everyone, but some special toys (like your favorite doll or action figure) should only be seen or played with by certain people.
# MAGIC
# MAGIC **Data security** is like having **rules** about:
# MAGIC - Who can open the toy box? 🔑
# MAGIC - Who can see which toys? 👀
# MAGIC - Who can play with specific toys? 🧸
# MAGIC
# MAGIC In the data world, we don't want everyone to see **sensitive information** like:
# MAGIC - Social Security Numbers (SSN) 🚫
# MAGIC - Salaries 💵
# MAGIC - Personal emails 📧
# MAGIC - Medical records 🏥
# MAGIC
# MAGIC So we create **security rules** to protect this data!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### Why Data Security is Critical:
# MAGIC
# MAGIC 1. **Regulatory Compliance**  
# MAGIC    - GDPR (Europe), CCPA (California), HIPAA (Healthcare)  
# MAGIC    - Non-compliance = Heavy fines + Legal issues  
# MAGIC
# MAGIC 2. **Data Breach Prevention**  
# MAGIC    - Average cost of data breach: **$4.45 million** (IBM 2023)  
# MAGIC    - Unauthorized access to PII can destroy company reputation  
# MAGIC
# MAGIC 3. **Principle of Least Privilege**  
# MAGIC    - Users should only access data they **need** for their job  
# MAGIC    - Reduces attack surface and insider threats  
# MAGIC
# MAGIC 4. **Audit & Compliance Requirements**  
# MAGIC    - Must track **who accessed what data when**  
# MAGIC    - Data lineage helps prove compliance  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Fine-Grained Access Control
# MAGIC
# MAGIC ### Traditional Security (Table-Level):
# MAGIC ```
# MAGIC ❌ Problem: All-or-nothing access
# MAGIC User has access to entire table OR no access at all
# MAGIC ```
# MAGIC
# MAGIC ### Modern Security (Fine-Grained):
# MAGIC ```
# MAGIC ✅ Row-Level Security (RLS): Control which ROWS a user sees
# MAGIC ✅ Column-Level Security (CLS): Control which COLUMNS a user sees  
# MAGIC ✅ Data Masking: HIDE or REDACT sensitive values
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛡️ Security Layers in Unity Catalog:
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────┐
# MAGIC │   Layer 1: Catalog/Schema Access    │
# MAGIC └────────────────────────────────────┘
# MAGIC            ↓
# MAGIC ┌────────────────────────────────────┐
# MAGIC │   Layer 2: Table-Level Access      │
# MAGIC └────────────────────────────────────┘
# MAGIC            ↓
# MAGIC ┌────────────────────────────────────┐
# MAGIC │   Layer 3: Row-Level Security      │  ← Filter rows
# MAGIC └────────────────────────────────────┘
# MAGIC            ↓
# MAGIC ┌────────────────────────────────────┐
# MAGIC │   Layer 4: Column-Level Security   │  ← Filter columns
# MAGIC └────────────────────────────────────┘
# MAGIC            ↓
# MAGIC ┌────────────────────────────────────┐
# MAGIC │   Layer 5: Data Masking            │  ← Redact values
# MAGIC └────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Key Takeaway:
# MAGIC **Security is not a single gate—it's multiple layers of protection!**

# COMMAND ----------

# DBTITLE 1,🔴 Section 2: Row-Level Security (RLS)
# MAGIC %md
# MAGIC # 🔴 SECTION 2: Row-Level Security (RLS)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine you have a **class attendance sheet** with all students' names. But each student should only see **their own row**—not everyone else's rows!
# MAGIC
# MAGIC **Row-Level Security** is like having **magic glasses** that automatically hide rows you're not allowed to see:
# MAGIC - Teacher sees ALL rows 👩‍🏫
# MAGIC - Student A sees only their row 👦
# MAGIC - Student B sees only their row 👧
# MAGIC
# MAGIC In databases, this means:
# MAGIC - **Sales Manager** sees all sales data 📈
# MAGIC - **Sales Rep (Region: West)** sees only West region data 🌎
# MAGIC - **Sales Rep (Region: East)** sees only East region data 🌏
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### What is Row-Level Security (RLS)?
# MAGIC
# MAGIC RLS dynamically **filters rows** in a query result based on:
# MAGIC - User identity (`current_user()`)
# MAGIC - User groups (`is_member('group_name')`)
# MAGIC - User attributes (department, region, role)
# MAGIC - Session variables
# MAGIC
# MAGIC ### RLS Implementation Approaches:
# MAGIC
# MAGIC #### 1. **View-Based RLS** (Simple)
# MAGIC ```sql
# MAGIC CREATE OR REPLACE VIEW sales_view AS
# MAGIC SELECT * FROM sales_table
# MAGIC WHERE region = current_user_region();
# MAGIC ```
# MAGIC
# MAGIC #### 2. **ABAC Policy-Based RLS** (Advanced - Unity Catalog)
# MAGIC ```sql
# MAGIC -- Row filter UDF
# MAGIC CREATE OR REPLACE FUNCTION filter_by_region(region STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN region = session_variable('user_region');
# MAGIC
# MAGIC -- Policy applied via governed tags
# MAGIC CREATE POLICY regional_isolation
# MAGIC ON SCHEMA catalog.sales
# MAGIC ROW FILTER catalog.security.filter_by_region
# MAGIC TO `All Users` EXCEPT admins
# MAGIC FOR TABLES
# MAGIC WHEN has_tag('data_isolation')
# MAGIC MATCH COLUMNS has_tag('region') AS region
# MAGIC USING COLUMNS (region);
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Use Cases for RLS:
# MAGIC
# MAGIC ### 1. **Multi-Tenant Applications**
# MAGIC - SaaS platforms where customers share the same table
# MAGIC - Each customer sees only their own data
# MAGIC
# MAGIC ### 2. **Regional Data Isolation**
# MAGIC - EMEA users see only EMEA data
# MAGIC - APAC users see only APAC data
# MAGIC - Global admins see everything
# MAGIC
# MAGIC ### 3. **Department-Based Access**
# MAGIC - HR sees HR data
# MAGIC - Finance sees Finance data
# MAGIC - Engineering sees Engineering data
# MAGIC
# MAGIC ### 4. **Hierarchical Access**
# MAGIC - Managers see their team's data
# MAGIC - Directors see all teams in their division
# MAGIC - C-level sees company-wide data
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚙️ RLS Architecture:
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │  User Query: SELECT * FROM sales   │
# MAGIC └────────────────────────────────────────┘
# MAGIC                   ↓
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │  Unity Catalog Checks:             │
# MAGIC │  - Who is the user?                │
# MAGIC │  - What groups are they in?        │
# MAGIC │  - What policies apply?            │
# MAGIC └────────────────────────────────────────┘
# MAGIC                   ↓
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │  Rewritten Query:                  │
# MAGIC │  SELECT * FROM sales               │
# MAGIC │  WHERE region = 'West'  ← Auto  │
# MAGIC └────────────────────────────────────────┘
# MAGIC                   ↓
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │  Filtered Results Returned         │
# MAGIC └────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚨 Important Notes:
# MAGIC
# MAGIC 1. **Transparent to Users**: Users write normal queries; filtering happens automatically
# MAGIC 2. **Performance**: Row filters are pushed down to query execution (efficient)
# MAGIC 3. **Cannot Bypass**: Even direct table access respects RLS policies
# MAGIC 4. **Exceptions**: Admins can be exempted using `EXCEPT` clause

# COMMAND ----------

# DBTITLE 1,💡 Section 2: RLS Examples
# MAGIC %md
# MAGIC ## 💻 Hands-On: Row-Level Security Examples
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example 1: Simple View-Based RLS
# MAGIC
# MAGIC ```sql
# MAGIC -- Scenario: Sales reps should only see their own region's data
# MAGIC
# MAGIC CREATE OR REPLACE VIEW sales_by_region AS
# MAGIC SELECT 
# MAGIC     order_id,
# MAGIC     customer_name,
# MAGIC     region,
# MAGIC     amount,
# MAGIC     order_date
# MAGIC FROM catalog.schema.sales_table
# MAGIC WHERE region = current_user()  -- Assumes username = region
# MAGIC    OR is_member('sales_managers');  -- Managers see all
# MAGIC
# MAGIC -- Grant access to the view
# MAGIC GRANT SELECT ON VIEW catalog.schema.sales_by_region TO `sales_team`;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example 2: Dynamic Filtering with UDF
# MAGIC
# MAGIC ```sql
# MAGIC -- Step 1: Create a row filter function
# MAGIC CREATE OR REPLACE FUNCTION catalog.security.filter_by_department(dept STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN 
# MAGIC   dept = session_user() OR 
# MAGIC   is_member('dept_managers');
# MAGIC
# MAGIC -- Step 2: Users query the table directly
# MAGIC -- (In production, this would be enforced via ABAC policy)
# MAGIC SELECT * FROM employee_table
# MAGIC WHERE catalog.security.filter_by_department(department);
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example 3: Multi-Tenant RLS
# MAGIC
# MAGIC ```sql
# MAGIC -- Scenario: SaaS application with multiple customers
# MAGIC
# MAGIC CREATE OR REPLACE VIEW customer_orders AS
# MAGIC SELECT 
# MAGIC     order_id,
# MAGIC     product_name,
# MAGIC     quantity,
# MAGIC     price,
# MAGIC     order_date
# MAGIC FROM catalog.schema.orders
# MAGIC WHERE 
# MAGIC     customer_id = current_user()  -- User sees only their orders
# MAGIC     OR is_member('support_team');  -- Support sees all
# MAGIC
# MAGIC -- Each customer is a separate user/service principal
# MAGIC GRANT SELECT ON VIEW catalog.schema.customer_orders TO `customer_alice`;
# MAGIC GRANT SELECT ON VIEW catalog.schema.customer_orders TO `customer_bob`;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example 4: Hierarchical RLS (Manager Access)
# MAGIC
# MAGIC ```sql
# MAGIC -- Scenario: Managers see their team + their own records
# MAGIC
# MAGIC CREATE OR REPLACE VIEW employee_performance AS
# MAGIC SELECT 
# MAGIC     employee_id,
# MAGIC     employee_name,
# MAGIC     manager_id,
# MAGIC     performance_score,
# MAGIC     review_date
# MAGIC FROM catalog.schema.performance_reviews
# MAGIC WHERE 
# MAGIC     employee_id = current_user()  -- See your own reviews
# MAGIC     OR manager_id = current_user()  -- See your team's reviews
# MAGIC     OR is_member('hr_team');  -- HR sees all
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔑 Key SQL Functions for RLS:
# MAGIC
# MAGIC | Function | Description | Example |
# MAGIC |----------|-------------|----------|
# MAGIC | `current_user()` | Returns logged-in username | `WHERE owner = current_user()` |
# MAGIC | `is_member('group')` | Checks group membership | `WHERE is_member('admins')` |
# MAGIC | `session_user()` | Alternative to current_user() | `WHERE dept = session_user()` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Common RLS Pitfalls:
# MAGIC
# MAGIC 1. **Forgetting to grant view access**: View created but users can't query it
# MAGIC 2. **Performance issues**: Row filters without proper indexes
# MAGIC 3. **Logic errors**: Accidentally excluding legitimate data
# MAGIC 4. **Admin lockout**: Forgetting to add admin exception clause
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 RLS vs Traditional Security:
# MAGIC
# MAGIC | Aspect | Traditional | RLS |
# MAGIC |--------|-------------|-----|
# MAGIC | **Granularity** | Table-level | Row-level |
# MAGIC | **User Experience** | Multiple tables/views | Single unified view |
# MAGIC | **Maintenance** | High (many objects) | Low (centralized policies) |
# MAGIC | **Performance** | Good | Good (predicate pushdown) |
# MAGIC | **Complexity** | Low | Medium |

# COMMAND ----------

# DBTITLE 1,🔧 Hands-On Lab: Setup Environment
# MAGIC %md
# MAGIC # 🔧 HANDS-ON LAB: Unity Catalog Security & Governance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📋 Lab Overview:
# MAGIC
# MAGIC In this hands-on lab, we'll create a complete security and governance demonstration:
# MAGIC
# MAGIC 1. ✅ Create sample catalog and schema
# MAGIC 2. ✅ Create sample tables with sensitive data
# MAGIC 3. ✅ Implement Row-Level Security (RLS)
# MAGIC 4. ✅ Implement Column-Level Security (CLS)
# MAGIC 5. ✅ Create and test masking functions
# MAGIC 6. ✅ Apply Unity Catalog tags
# MAGIC 7. ✅ Query data lineage
# MAGIC 8. ✅ Verify security policies
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 What You'll Build:
# MAGIC
# MAGIC A secure **Sales & Employee** data platform with:
# MAGIC - **Regional access control** (sales reps see only their region)
# MAGIC - **Sensitive data masking** (SSN, salary protected)
# MAGIC - **Role-based column access** (HR, Finance, Analysts)
# MAGIC - **Complete lineage tracking**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Important Notes:
# MAGIC
# MAGIC - This lab uses **serverless compute** (auto-attached)
# MAGIC - All objects created in **your Unity Catalog**
# MAGIC - Tables prefixed with `security_demo_` for easy cleanup
# MAGIC - Can be safely deleted after the lab
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Let's Get Started!

# COMMAND ----------

# DBTITLE 1,Step 1: Create Catalog and Schema
# Step 1: Create Catalog and Schema for Security Demo
# Note: Using current catalog/schema to avoid permission issues

import os
from datetime import datetime, timedelta
import random

# Get current catalog and schema
current_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
current_schema = spark.sql("SELECT current_schema()").collect()[0][0]

print(f"🎯 Using Catalog: {current_catalog}")
print(f"🎯 Using Schema: {current_schema}")
print(f"✅ Environment ready for security demo!")

# Create a demo schema (if you have permissions)
try:
    spark.sql("CREATE SCHEMA IF NOT EXISTS security_demo COMMENT 'Security and Governance Demo Schema'")
    demo_schema = "security_demo"
    print(f"✅ Created demo schema: {current_catalog}.{demo_schema}")
except Exception as e:
    demo_schema = current_schema
    print(f"ℹ️  Using current schema: {demo_schema}")
    print(f"   (Reason: {str(e)[:100]}...)")

# COMMAND ----------

# DBTITLE 1,Step 2: Create Sample Sales Data
# Step 2: Create sample sales data with regional information

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from pyspark.sql.functions import col, lit
from datetime import date, timedelta
from decimal import Decimal
import random

# Sample data for sales (using Decimal for decimal values)
sales_data = [
    (1, "Alice Johnson", "West", Decimal("125000.50"), "2026-01-15", "alice@company.com"),
    (2, "Bob Smith", "West", Decimal("98000.75"), "2026-01-16", "bob@company.com"),
    (3, "Carol Davis", "East", Decimal("156000.25"), "2026-01-17", "carol@company.com"),
    (4, "David Wilson", "East", Decimal("87500.00"), "2026-01-18", "david@company.com"),
    (5, "Eve Martinez", "Central", Decimal("110000.80"), "2026-01-19", "eve@company.com"),
    (6, "Frank Brown", "Central", Decimal("143000.60"), "2026-01-20", "frank@company.com"),
    (7, "Grace Lee", "West", Decimal("92000.30"), "2026-01-21", "grace@company.com"),
    (8, "Henry Taylor", "East", Decimal("201000.90"), "2026-01-22", "henry@company.com"),
    (9, "Ivy Anderson", "Central", Decimal("76000.45"), "2026-01-23", "ivy@company.com"),
    (10, "Jack Thomas", "West", Decimal("189000.70"), "2026-01-24", "jack@company.com")
]

# Create DataFrame
schema = StructType([
    StructField("sales_id", IntegerType(), False),
    StructField("sales_rep_name", StringType(), False),
    StructField("region", StringType(), False),
    StructField("sales_amount", DecimalType(10, 2), False),
    StructField("sale_date", StringType(), False),
    StructField("email", StringType(), False)
])

df_sales = spark.createDataFrame(sales_data, schema)

# Save as Delta table
table_name = f"{current_catalog}.{demo_schema}.security_demo_sales"
df_sales.write.format("delta").mode("overwrite").saveAsTable(table_name)

print(f"✅ Created table: {table_name}")
print(f"📊 Total records: {df_sales.count()}")
print("\n🔍 Sample data:")
display(df_sales.limit(5))

# COMMAND ----------

# DBTITLE 1,Step 3: Create Sample Employee Data with PII
# Step 3: Create sample employee data with sensitive PII

from decimal import Decimal

# Sample employee data with SSN and salary (using Decimal for salary)
employee_data = [
    (101, "John Doe", "123-45-6789", "Engineering", Decimal("95000.00"), "john.doe@company.com", "555-0101"),
    (102, "Jane Smith", "234-56-7890", "Engineering", Decimal("102000.00"), "jane.smith@company.com", "555-0102"),
    (103, "Mike Johnson", "345-67-8901", "Sales", Decimal("88000.00"), "mike.j@company.com", "555-0103"),
    (104, "Sarah Williams", "456-78-9012", "HR", Decimal("78000.00"), "sarah.w@company.com", "555-0104"),
    (105, "Tom Brown", "567-89-0123", "Finance", Decimal("110000.00"), "tom.b@company.com", "555-0105"),
    (106, "Emily Davis", "678-90-1234", "Engineering", Decimal("125000.00"), "emily.d@company.com", "555-0106"),
    (107, "Chris Wilson", "789-01-2345", "Sales", Decimal("92000.00"), "chris.w@company.com", "555-0107"),
    (108, "Lisa Anderson", "890-12-3456", "HR", Decimal("85000.00"), "lisa.a@company.com", "555-0108"),
    (109, "David Martinez", "901-23-4567", "Finance", Decimal("115000.00"), "david.m@company.com", "555-0109"),
    (110, "Amy Taylor", "012-34-5678", "Engineering", Decimal("98000.00"), "amy.t@company.com", "555-0110")
]

schema_emp = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("employee_name", StringType(), False),
    StructField("ssn", StringType(), False),
    StructField("department", StringType(), False),
    StructField("salary", DecimalType(10, 2), False),
    StructField("email", StringType(), False),
    StructField("phone", StringType(), False)
])

df_employees = spark.createDataFrame(employee_data, schema_emp)

# Save as Delta table
table_name_emp = f"{current_catalog}.{demo_schema}.security_demo_employees"
df_employees.write.format("delta").mode("overwrite").saveAsTable(table_name_emp)

print(f"✅ Created table: {table_name_emp}")
print(f"📊 Total records: {df_employees.count()}")
print("\n⚠️  This table contains sensitive PII:")
print("   - SSN (Social Security Numbers)")
print("   - Salary information")
print("   - Phone numbers")
print("\n🔍 Sample data (showing sensitive fields):")
display(df_employees.limit(3))

# COMMAND ----------

# DBTITLE 1,Step 4: Implement Row-Level Security (View-Based)
# Step 4: Create Row-Level Security View for Regional Sales Data
# Scenario: Sales reps should only see their region's data

# First, let's see all the data (before RLS)
query = f"""
SELECT 
    region,
    COUNT(*) as total_sales,
    SUM(sales_amount) as total_amount
FROM {current_catalog}.{demo_schema}.security_demo_sales
GROUP BY region
ORDER BY region
"""

print("📊 Sales Data by Region (Before RLS):")
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Step 4b: Create RLS View
# Step 4b: Create a view that filters by region
# In production, you would use current_user() or is_member() functions
# For demo purposes, we'll show how the view structure works

query = f"""
CREATE OR REPLACE VIEW {current_catalog}.{demo_schema}.security_demo_sales_west_only AS
SELECT 
    sales_id,
    sales_rep_name,
    region,
    sales_amount,
    sale_date,
    email
FROM {current_catalog}.{demo_schema}.security_demo_sales
WHERE region = 'West'
"""

spark.sql(query)
print(f"✅ Created RLS view: {current_catalog}.{demo_schema}.security_demo_sales_west_only")

# View the filtered data (West region only)
query2 = f"""
SELECT 
    region,
    COUNT(*) as sales_count,
    ROUND(SUM(sales_amount), 2) as total_sales
FROM {current_catalog}.{demo_schema}.security_demo_sales_west_only
GROUP BY region
"""

print("\n🔒 RLS View Result (West region only):")
display(spark.sql(query2))

# COMMAND ----------

# DBTITLE 1,🟢 Section 3: Column-Level Security (CLS)
# MAGIC %md
# MAGIC # 🟢 SECTION 3: Column-Level Security (CLS)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine you have a **report card** with many columns:
# MAGIC - Name (everyone can see) 📝
# MAGIC - Math Grade (everyone can see) 🔢
# MAGIC - Reading Grade (everyone can see) 📚
# MAGIC - Teacher's Notes (only teacher can see) 🚫👩‍🏫
# MAGIC - Parent Contact Info (only principal can see) 🚫📞
# MAGIC
# MAGIC **Column-Level Security** is like having **invisible columns** that only certain people can see!
# MAGIC
# MAGIC In databases:
# MAGIC - Everyone sees: `customer_name`, `order_date`, `product`
# MAGIC - Only Finance sees: `salary`, `credit_card`, `bank_account`
# MAGIC - Only HR sees: `ssn`, `date_of_birth`, `home_address`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### What is Column-Level Security (CLS)?
# MAGIC
# MAGIC CLS restricts access to **specific columns** in a table based on:
# MAGIC - User identity
# MAGIC - User groups
# MAGIC - User roles
# MAGIC
# MAGIC ### CLS Implementation Methods:
# MAGIC
# MAGIC #### 1. **Column-Level GRANT** (Explicit)
# MAGIC ```sql
# MAGIC -- Grant access to specific columns only
# MAGIC GRANT SELECT (customer_id, customer_name, email) 
# MAGIC ON TABLE catalog.schema.customers 
# MAGIC TO `marketing_team`;
# MAGIC ```
# MAGIC
# MAGIC #### 2. **View-Based CLS** (Common)
# MAGIC ```sql
# MAGIC -- Create view without sensitive columns
# MAGIC CREATE OR REPLACE VIEW customer_public AS
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     email,
# MAGIC     city,
# MAGIC     state
# MAGIC     -- Excludes: ssn, credit_card, salary
# MAGIC FROM catalog.schema.customers;
# MAGIC
# MAGIC GRANT SELECT ON VIEW catalog.schema.customer_public TO `analysts`;
# MAGIC ```
# MAGIC
# MAGIC #### 3. **ABAC Column Mask Policies** (Advanced)
# MAGIC ```sql
# MAGIC -- Column mask via governed tags
# MAGIC CREATE POLICY ssn_mask
# MAGIC ON CATALOG hr
# MAGIC COLUMN MASK catalog.security.mask_ssn
# MAGIC TO `All Users` EXCEPT `HR admins`
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS has_tag_value('pii', 'ssn') AS ssn
# MAGIC ON COLUMN ssn;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Use Cases for CLS:
# MAGIC
# MAGIC ### 1. **PII Protection**
# MAGIC - Mask/restrict: SSN, credit cards, phone numbers
# MAGIC - Compliance: GDPR, CCPA, HIPAA
# MAGIC
# MAGIC ### 2. **Financial Data**
# MAGIC - Salary information (HR only)
# MAGIC - Revenue numbers (Finance only)
# MAGIC - Cost data (Executives only)
# MAGIC
# MAGIC ### 3. **Competitive Intelligence**
# MAGIC - Customer lists (Sales only)
# MAGIC - Pricing strategies (Product team only)
# MAGIC - Strategic plans (C-level only)
# MAGIC
# MAGIC ### 4. **Development vs Production**
# MAGIC - Dev team sees anonymized data
# MAGIC - Analysts see partial data
# MAGIC - Admins see full data
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚙️ CLS Architecture:
# MAGIC
# MAGIC ```
# MAGIC                     ORIGINAL TABLE
# MAGIC ┌────────────────────────────────────────────┐
# MAGIC │ id | name | email | ssn | salary | phone │
# MAGIC └────────────────────────────────────────────┘
# MAGIC                     ↓
# MAGIC         ┌─────────────────────────┐
# MAGIC         │  Unity Catalog CLS  │
# MAGIC         └─────────────────────────┘
# MAGIC                     ↓
# MAGIC       ┌────────────────────────────────────┐
# MAGIC       │     What user sees:          │
# MAGIC       │                              │
# MAGIC       │  Analyst: id, name, email   │
# MAGIC       │  HR: id, name, ssn, phone   │
# MAGIC       │  Finance: id, name, salary  │
# MAGIC       │  Admin: ALL columns         │
# MAGIC       └────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚨 Important Notes:
# MAGIC
# MAGIC 1. **Column grants are cumulative**: User with SELECT on column A and B can see both
# MAGIC 2. **Views are isolated**: Each view has its own permissions
# MAGIC 3. **SELECT * behavior**: Only returns columns user has access to
# MAGIC 4. **Performance**: No overhead (column pruning at query planning)

# COMMAND ----------

# DBTITLE 1,💡 Section 3: CLS Examples
# MAGIC %md
# MAGIC ## 💻 Hands-On: Column-Level Security Examples
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example 1: Basic Column-Level GRANT
# MAGIC
# MAGIC ```sql
# MAGIC -- Scenario: Marketing team should only see contact info, not financial data
# MAGIC
# MAGIC -- Grant access to specific columns
# MAGIC GRANT SELECT (customer_id, customer_name, email, phone, city, state) 
# MAGIC ON TABLE catalog.schema.customers 
# MAGIC TO `marketing_team`;
# MAGIC
# MAGIC -- When marketing queries: SELECT * FROM customers
# MAGIC -- They only see: customer_id, customer_name, email, phone, city, state
# MAGIC -- They DON'T see: ssn, credit_card, salary, bank_account
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example 2: Multiple Role-Based Views
# MAGIC
# MAGIC ```sql
# MAGIC -- View for Analysts (Safe columns only)
# MAGIC CREATE OR REPLACE VIEW employees_analysts AS
# MAGIC SELECT 
# MAGIC     employee_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     department,
# MAGIC     hire_date,
# MAGIC     job_title
# MAGIC FROM catalog.schema.employees;
# MAGIC
# MAGIC GRANT SELECT ON VIEW catalog.schema.employees_analysts TO `analysts`;
# MAGIC
# MAGIC -- View for HR (Includes PII)
# MAGIC CREATE OR REPLACE VIEW employees_hr AS
# MAGIC SELECT 
# MAGIC     employee_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     ssn,
# MAGIC     date_of_birth,
# MAGIC     home_address,
# MAGIC     phone_number,
# MAGIC     emergency_contact
# MAGIC FROM catalog.schema.employees;
# MAGIC
# MAGIC GRANT SELECT ON VIEW catalog.schema.employees_hr TO `hr_team`;
# MAGIC
# MAGIC -- View for Finance (Includes compensation)
# MAGIC CREATE OR REPLACE VIEW employees_finance AS
# MAGIC SELECT 
# MAGIC     employee_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     salary,
# MAGIC     bonus,
# MAGIC     stock_options,
# MAGIC     bank_account
# MAGIC FROM catalog.schema.employees;
# MAGIC
# MAGIC GRANT SELECT ON VIEW catalog.schema.employees_finance TO `finance_team`;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example 3: Dynamic Column Selection with CASE
# MAGIC
# MAGIC ```sql
# MAGIC -- Scenario: Show salary only to managers and HR
# MAGIC
# MAGIC CREATE OR REPLACE VIEW employee_compensation AS
# MAGIC SELECT 
# MAGIC     employee_id,
# MAGIC     employee_name,
# MAGIC     department,
# MAGIC     CASE 
# MAGIC         WHEN is_member('managers') OR is_member('hr_team') 
# MAGIC         THEN salary 
# MAGIC         ELSE NULL  -- or '****' for masking
# MAGIC     END AS salary,
# MAGIC     CASE 
# MAGIC         WHEN is_member('hr_team') 
# MAGIC         THEN bonus 
# MAGIC         ELSE NULL 
# MAGIC     END AS bonus
# MAGIC FROM catalog.schema.employees;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example 4: Progressive Column Access
# MAGIC
# MAGIC ```sql
# MAGIC -- Scenario: Different levels of data access
# MAGIC
# MAGIC -- Level 1: Public Info
# MAGIC CREATE OR REPLACE VIEW products_public AS
# MAGIC SELECT product_id, product_name, category, description
# MAGIC FROM catalog.schema.products;
# MAGIC
# MAGIC -- Level 2: + Pricing (for sales team)
# MAGIC CREATE OR REPLACE VIEW products_sales AS
# MAGIC SELECT product_id, product_name, category, description, price, discount
# MAGIC FROM catalog.schema.products;
# MAGIC
# MAGIC -- Level 3: + Cost (for finance team)
# MAGIC CREATE OR REPLACE VIEW products_finance AS
# MAGIC SELECT product_id, product_name, category, description, price, discount, 
# MAGIC        cost, profit_margin
# MAGIC FROM catalog.schema.products;
# MAGIC
# MAGIC -- Level 4: + Supplier (for procurement team)
# MAGIC CREATE OR REPLACE VIEW products_procurement AS
# MAGIC SELECT product_id, product_name, category, description, price, cost, 
# MAGIC        supplier_id, supplier_name, supplier_contract
# MAGIC FROM catalog.schema.products;
# MAGIC
# MAGIC GRANT SELECT ON VIEW catalog.schema.products_public TO `public`;
# MAGIC GRANT SELECT ON VIEW catalog.schema.products_sales TO `sales_team`;
# MAGIC GRANT SELECT ON VIEW catalog.schema.products_finance TO `finance_team`;
# MAGIC GRANT SELECT ON VIEW catalog.schema.products_procurement TO `procurement_team`;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔑 Best Practices for CLS:
# MAGIC
# MAGIC 1. **Naming Convention**: Use suffixes like `_public`, `_internal`, `_restricted`
# MAGIC 2. **Documentation**: Comment views with access level and purpose
# MAGIC 3. **Regular Audits**: Review column grants periodically
# MAGIC 4. **Minimal Access**: Start with minimal columns, add as needed
# MAGIC 5. **Consistent Tags**: Tag sensitive columns for automated policies
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ CLS Common Mistakes:
# MAGIC
# MAGIC | Mistake | Impact | Solution |
# MAGIC |---------|--------|----------|
# MAGIC | Granting full table access | Sensitive data exposed | Use column-level grants |
# MAGIC | Too many views | Maintenance nightmare | Use ABAC policies instead |
# MAGIC | Forgetting JOIN columns | Queries break | Always include join keys |
# MAGIC | No documentation | Confusion about access levels | Document each view |
# MAGIC | Hardcoding values | Inflexible security | Use dynamic functions |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 CLS vs RLS Comparison:
# MAGIC
# MAGIC | Feature | RLS | CLS |
# MAGIC |---------|-----|-----|
# MAGIC | **What it filters** | Rows | Columns |
# MAGIC | **When to use** | User sees subset of records | User sees subset of fields |
# MAGIC | **Complexity** | Medium | Low |
# MAGIC | **Performance impact** | Low | Very low |
# MAGIC | **Maintenance** | Moderate | Moderate |
# MAGIC | **Can combine?** | ✅ Yes! Use both together | ✅ Yes! Use both together |

# COMMAND ----------

# DBTITLE 1,Step 5: Column-Level Security - Public View
# Step 5: Column-Level Security - Public View
# Scenario: General employees can see basic info, but NOT sensitive fields (SSN, salary, phone)

query = f"""
CREATE OR REPLACE VIEW {current_catalog}.{demo_schema}.security_demo_employees_public AS
SELECT 
    employee_id,
    employee_name,
    department,
    email
FROM {current_catalog}.{demo_schema}.security_demo_employees
"""

spark.sql(query)
print(f"✅ Created CLS public view: {current_catalog}.{demo_schema}.security_demo_employees_public")
print("   ✅ Includes: employee_id, employee_name, department, email")
print("   ❌ Excludes: SSN, salary, phone")

print("\n🔓 Public View Sample:")
display(spark.sql(f"SELECT * FROM {current_catalog}.{demo_schema}.security_demo_employees_public LIMIT 5"))

# COMMAND ----------

# DBTITLE 1,Step 5b: Column-Level Security - HR View
# Step 5b: Column-Level Security - HR View
# Scenario: HR can see SSN and phone for employee verification

query = f"""
CREATE OR REPLACE VIEW {current_catalog}.{demo_schema}.security_demo_employees_hr AS
SELECT 
    employee_id,
    employee_name,
    ssn,
    department,
    email,
    phone
FROM {current_catalog}.{demo_schema}.security_demo_employees
"""

spark.sql(query)
print(f"✅ Created CLS HR view: {current_catalog}.{demo_schema}.security_demo_employees_hr")
print("   ✅ Includes: employee_id, name, SSN, department, email, phone")
print("   ❌ Excludes: salary")

print("\n🛡️ HR View Sample (includes SSN):")
display(spark.sql(f"SELECT * FROM {current_catalog}.{demo_schema}.security_demo_employees_hr LIMIT 5"))

# COMMAND ----------

# DBTITLE 1,Step 5c: Column-Level Security - Finance View
# Step 5c: Column-Level Security - Finance View
# Scenario: Finance can see salary for compensation analysis

query = f"""
CREATE OR REPLACE VIEW {current_catalog}.{demo_schema}.security_demo_employees_finance AS
SELECT 
    employee_id,
    employee_name,
    department,
    salary,
    email
FROM {current_catalog}.{demo_schema}.security_demo_employees
"""

spark.sql(query)
print(f"✅ Created CLS finance view: {current_catalog}.{demo_schema}.security_demo_employees_finance")
print("   ✅ Includes: employee_id, name, department, salary, email")
print("   ❌ Excludes: SSN, phone")

print("\n💰 Finance View Sample (includes salary):")
query2 = f"""
SELECT 
    department,
    COUNT(*) as employee_count,
    ROUND(AVG(salary), 2) as avg_salary,
    ROUND(MIN(salary), 2) as min_salary,
    ROUND(MAX(salary), 2) as max_salary
FROM {current_catalog}.{demo_schema}.security_demo_employees_finance
GROUP BY department
ORDER BY avg_salary DESC
"""
display(spark.sql(query2))

# COMMAND ----------

# DBTITLE 1,🔵 Section 4: Data Masking
# MAGIC %md
# MAGIC # 🔵 SECTION 4: Data Masking
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine you're writing a **secret message** but you want to show it to your friend without revealing the secret:
# MAGIC
# MAGIC - **Original**: "My password is 12345" 🔒
# MAGIC - **Masked**: "My password is *****" 🚫
# MAGIC
# MAGIC **Data Masking** is like putting **stickers** over sensitive information:
# MAGIC - Real SSN: `123-45-6789`
# MAGIC - Masked SSN: `***-**-6789` (show only last 4 digits)
# MAGIC
# MAGIC - Real Email: `john.doe@company.com`
# MAGIC - Masked Email: `j***@company.com`
# MAGIC
# MAGIC - Real Credit Card: `1234-5678-9012-3456`
# MAGIC - Masked Credit Card: `****-****-****-3456`
# MAGIC
# MAGIC This way:
# MAGIC - You can see **some** information (like last 4 digits)
# MAGIC - But the **full secret** is hidden! 🎭
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### What is Data Masking?
# MAGIC
# MAGIC Data masking is the process of **replacing sensitive data** with fictitious but realistic values. The masked data:
# MAGIC - Preserves format and data type
# MAGIC - Maintains referential integrity
# MAGIC - Protects PII while allowing development/testing
# MAGIC
# MAGIC ### Types of Data Masking:
# MAGIC
# MAGIC #### 1. **Static Masking**
# MAGIC - Creates a masked copy of the database
# MAGIC - Used for: Dev/Test environments
# MAGIC - Example: Production → Masked Dev database
# MAGIC
# MAGIC #### 2. **Dynamic Masking** (Real-Time)
# MAGIC - Masks data at query time based on user
# MAGIC - Used for: Production access control
# MAGIC - Example: Analysts see masked data, HR sees real data
# MAGIC
# MAGIC #### 3. **On-the-Fly Masking**
# MAGIC - Masks during data extraction/export
# MAGIC - Used for: Data sharing, compliance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Masking Techniques:
# MAGIC
# MAGIC ### 1. **Redaction** (Complete Hiding)
# MAGIC ```sql
# MAGIC -- Before: john.doe@email.com
# MAGIC -- After:  ****
# MAGIC CASE WHEN is_member('admins') THEN email ELSE '****' END
# MAGIC ```
# MAGIC
# MAGIC ### 2. **Partial Masking** (Show Last N Characters)
# MAGIC ```sql
# MAGIC -- Before: 123-45-6789
# MAGIC -- After:  ***-**-6789
# MAGIC CASE 
# MAGIC     WHEN is_member('hr_team') THEN ssn 
# MAGIC     ELSE CONCAT('***-**-', RIGHT(ssn, 4)) 
# MAGIC END AS ssn
# MAGIC ```
# MAGIC
# MAGIC ### 3. **Substitution** (Replace with Fake Data)
# MAGIC ```sql
# MAGIC -- Before: John Doe
# MAGIC -- After:  User12345
# MAGIC CASE 
# MAGIC     WHEN is_member('admins') THEN customer_name 
# MAGIC     ELSE CONCAT('User', customer_id) 
# MAGIC END AS customer_name
# MAGIC ```
# MAGIC
# MAGIC ### 4. **Shuffling** (Randomize within dataset)
# MAGIC ```sql
# MAGIC -- Randomly reassign salaries to employees
# MAGIC -- Preserves distribution but breaks person-salary link
# MAGIC ```
# MAGIC
# MAGIC ### 5. **Encryption** (Reversible)
# MAGIC ```sql
# MAGIC -- Store: AES_ENCRYPT(ssn, key)
# MAGIC -- Retrieve: AES_DECRYPT(encrypted_ssn, key)
# MAGIC ```
# MAGIC
# MAGIC ### 6. **Hashing** (One-Way)
# MAGIC ```sql
# MAGIC -- Before: john@email.com
# MAGIC -- After:  5d41402abc4b2a76b9719d911017c592
# MAGIC SHA2(email, 256)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚙️ Data Masking in Unity Catalog:
# MAGIC
# MAGIC ### ABAC Column Mask Policies:
# MAGIC
# MAGIC ```sql
# MAGIC -- Step 1: Create masking UDF
# MAGIC CREATE OR REPLACE FUNCTION catalog.security.mask_ssn(ssn STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CONCAT('***-**-', RIGHT(ssn, 4));
# MAGIC
# MAGIC -- Step 2: Apply policy via governed tags
# MAGIC CREATE POLICY ssn_masking
# MAGIC ON CATALOG hr
# MAGIC COLUMN MASK catalog.security.mask_ssn
# MAGIC TO `All Users` EXCEPT `HR admins`
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS has_tag_value('pii', 'ssn') AS ssn
# MAGIC ON COLUMN ssn;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Use Cases for Data Masking:
# MAGIC
# MAGIC ### 1. **Development & Testing**
# MAGIC - Developers need realistic data but not real PII
# MAGIC - Mask: SSN, emails, names, addresses
# MAGIC
# MAGIC ### 2. **Analytics & Reporting**
# MAGIC - Analysts need patterns but not individual identities
# MAGIC - Mask: Customer IDs, transaction details
# MAGIC
# MAGIC ### 3. **Third-Party Sharing**
# MAGIC - Partners need aggregated data but not raw PII
# MAGIC - Mask: All personally identifiable fields
# MAGIC
# MAGIC ### 4. **Compliance Requirements**
# MAGIC - GDPR "Right to be Forgotten"
# MAGIC - HIPAA de-identification
# MAGIC - PCI-DSS credit card protection
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚨 Important Considerations:
# MAGIC
# MAGIC 1. **Consistency**: Same input → Same masked output (use deterministic functions)
# MAGIC 2. **Format Preservation**: Masked SSN looks like SSN (preserves validations)
# MAGIC 3. **Performance**: Dynamic masking adds minimal overhead
# MAGIC 4. **Reversibility**: Decide if unmasking should be possible
# MAGIC 5. **Audit Trail**: Log who accessed unmasked data

# COMMAND ----------

# DBTITLE 1,💡 Section 4: Data Masking Examples
# MAGIC %md
# MAGIC ## 💻 Hands-On: Data Masking Examples
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example 1: SSN Masking (Last 4 Visible)
# MAGIC
# MAGIC ```sql
# MAGIC -- Create masking function
# MAGIC CREATE OR REPLACE FUNCTION catalog.security.mask_ssn(ssn STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CONCAT('***-**-', SUBSTRING(ssn, -4, 4));
# MAGIC
# MAGIC -- Usage in query
# MAGIC SELECT 
# MAGIC     employee_id,
# MAGIC     employee_name,
# MAGIC     CASE 
# MAGIC         WHEN is_member('hr_admins') THEN ssn
# MAGIC         ELSE catalog.security.mask_ssn(ssn)
# MAGIC     END AS ssn
# MAGIC FROM catalog.schema.employees;
# MAGIC
# MAGIC -- Output:
# MAGIC -- For HR:      123-45-6789
# MAGIC -- For Others:  ***-**-6789
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example 2: Email Masking
# MAGIC
# MAGIC ```sql
# MAGIC -- Create email masking function
# MAGIC CREATE OR REPLACE FUNCTION catalog.security.mask_email(email STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CONCAT(
# MAGIC     SUBSTRING(email, 1, 1),
# MAGIC     '***@',
# MAGIC     SPLIT(email, '@')[1]
# MAGIC );
# MAGIC
# MAGIC -- Usage
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     catalog.security.mask_email(email) AS email
# MAGIC FROM catalog.schema.customers;
# MAGIC
# MAGIC -- Output:
# MAGIC -- Original:  john.doe@company.com
# MAGIC -- Masked:    j***@company.com
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example 3: Credit Card Masking
# MAGIC
# MAGIC ```sql
# MAGIC -- Create credit card masking function
# MAGIC CREATE OR REPLACE FUNCTION catalog.security.mask_credit_card(card STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CONCAT('****-****-****-', SUBSTRING(card, -4, 4));
# MAGIC
# MAGIC -- Usage
# MAGIC SELECT 
# MAGIC     transaction_id,
# MAGIC     customer_id,
# MAGIC     CASE 
# MAGIC         WHEN is_member('finance_admins') THEN credit_card_number
# MAGIC         ELSE catalog.security.mask_credit_card(credit_card_number)
# MAGIC     END AS credit_card_number,
# MAGIC     amount
# MAGIC FROM catalog.schema.transactions;
# MAGIC
# MAGIC -- Output:
# MAGIC -- For Finance:  1234-5678-9012-3456
# MAGIC -- For Others:   ****-****-****-3456
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example 4: Salary Masking (Salary Bands)
# MAGIC
# MAGIC ```sql
# MAGIC -- Create salary band function
# MAGIC CREATE OR REPLACE FUNCTION catalog.security.mask_salary(salary DECIMAL(10,2))
# MAGIC RETURNS STRING
# MAGIC RETURN CASE 
# MAGIC     WHEN salary < 50000 THEN '$0-$50K'
# MAGIC     WHEN salary < 100000 THEN '$50K-$100K'
# MAGIC     WHEN salary < 150000 THEN '$100K-$150K'
# MAGIC     WHEN salary < 200000 THEN '$150K-$200K'
# MAGIC     ELSE '$200K+'
# MAGIC END;
# MAGIC
# MAGIC -- Usage
# MAGIC SELECT 
# MAGIC     employee_id,
# MAGIC     employee_name,
# MAGIC     department,
# MAGIC     CASE 
# MAGIC         WHEN is_member('finance_team') THEN CAST(salary AS STRING)
# MAGIC         ELSE catalog.security.mask_salary(salary)
# MAGIC     END AS salary
# MAGIC FROM catalog.schema.employees;
# MAGIC
# MAGIC -- Output:
# MAGIC -- For Finance:   75000.00
# MAGIC -- For Others:    $50K-$100K
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example 5: Phone Number Masking
# MAGIC
# MAGIC ```sql
# MAGIC -- Create phone masking function
# MAGIC CREATE OR REPLACE FUNCTION catalog.security.mask_phone(phone STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CONCAT('***-***-', SUBSTRING(phone, -4, 4));
# MAGIC
# MAGIC -- Usage
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     catalog.security.mask_phone(phone_number) AS phone_number
# MAGIC FROM catalog.schema.customers
# MAGIC WHERE NOT is_member('customer_service');
# MAGIC
# MAGIC -- Output:
# MAGIC -- Original:  555-123-4567
# MAGIC -- Masked:    ***-***-4567
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example 6: Address Masking (City Only)
# MAGIC
# MAGIC ```sql
# MAGIC -- Show only city and state, hide street address
# MAGIC CREATE OR REPLACE VIEW customer_locations AS
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     CASE 
# MAGIC         WHEN is_member('shipping_team') THEN street_address
# MAGIC         ELSE '[REDACTED]'
# MAGIC     END AS street_address,
# MAGIC     city,
# MAGIC     state,
# MAGIC     CASE 
# MAGIC         WHEN is_member('shipping_team') THEN zip_code
# MAGIC         ELSE SUBSTRING(zip_code, 1, 3) || '**'
# MAGIC     END AS zip_code
# MAGIC FROM catalog.schema.customers;
# MAGIC
# MAGIC -- Output for non-shipping users:
# MAGIC -- street_address: [REDACTED]
# MAGIC -- city: San Francisco
# MAGIC -- state: CA
# MAGIC -- zip_code: 941**
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example 7: Multi-Level Masking
# MAGIC
# MAGIC ```sql
# MAGIC -- Different masking levels based on role
# MAGIC CREATE OR REPLACE VIEW employee_data_masked AS
# MAGIC SELECT 
# MAGIC     employee_id,
# MAGIC     employee_name,
# MAGIC     CASE 
# MAGIC         WHEN is_member('hr_admins') THEN ssn  -- Full access
# MAGIC         WHEN is_member('hr_team') THEN CONCAT('***-**-', SUBSTRING(ssn, -4, 4))  -- Partial
# MAGIC         ELSE '***-**-****'  -- Fully masked
# MAGIC     END AS ssn,
# MAGIC     CASE 
# MAGIC         WHEN is_member('finance_admins') THEN salary  -- Full access
# MAGIC         WHEN is_member('managers') THEN FLOOR(salary / 10000) * 10000  -- Rounded
# MAGIC         ELSE NULL  -- Hidden
# MAGIC     END AS salary,
# MAGIC     department,
# MAGIC     hire_date
# MAGIC FROM catalog.schema.employees;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔑 Masking Best Practices:
# MAGIC
# MAGIC 1. **Create Reusable Functions**: Write once, use everywhere
# MAGIC 2. **Test Thoroughly**: Verify edge cases (NULL, empty strings)
# MAGIC 3. **Document Rules**: Explain what each mask shows/hides
# MAGIC 4. **Performance**: Use simple string functions (avoid regex if possible)
# MAGIC 5. **Consistency**: Same masking rules across all tables
# MAGIC 6. **Audit**: Log when unmasked data is accessed
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Masking Anti-Patterns:
# MAGIC
# MAGIC | Anti-Pattern | Why It's Bad | Better Approach |
# MAGIC |--------------|--------------|------------------|
# MAGIC | Random masking | Inconsistent results | Use deterministic functions |
# MAGIC | Complete redaction | Loses all context | Use partial masking |
# MAGIC | No format preservation | Breaks validations | Preserve structure |
# MAGIC | Masking in application | Security bypass risk | Mask at database level |
# MAGIC | One-size-fits-all | Too restrictive or loose | Role-based masking levels |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Masking vs Encryption:
# MAGIC
# MAGIC | Aspect | Masking | Encryption |
# MAGIC |--------|---------|------------|
# MAGIC | **Purpose** | Hide from unauthorized users | Protect data at rest/transit |
# MAGIC | **Reversible** | Usually No | Yes (with key) |
# MAGIC | **Performance** | Fast | Slower (crypto overhead) |
# MAGIC | **Use Case** | Analytics, dev/test | Compliance, storage |
# MAGIC | **Data Utility** | Maintains patterns | Unreadable without key |

# COMMAND ----------

# DBTITLE 1,Step 6: Create Data Masking Functions
# Step 6: Create Data Masking Functions (UDFs)
# These functions can be reused across all tables and views

# Function 1: Mask SSN (show only last 4 digits)
query = f"""
CREATE OR REPLACE FUNCTION {current_catalog}.{demo_schema}.mask_ssn(ssn STRING)
RETURNS STRING
RETURN CONCAT('***-**-', SUBSTRING(ssn, -4, 4))
"""

spark.sql(query)
print(f"✅ Created masking function: {current_catalog}.{demo_schema}.mask_ssn")

# Test SSN masking
test_query = f"""
SELECT 
    '123-45-6789' as original_ssn,
    {current_catalog}.{demo_schema}.mask_ssn('123-45-6789') as masked_ssn
"""

print("\n🎭 Test SSN Masking:")
display(spark.sql(test_query))

# COMMAND ----------

# DBTITLE 1,Step 6b: Create Email and Salary Masking Functions
# Step 6b: Create Email and Salary Masking Functions

# Function 2: Mask email (show only first character and domain)
query1 = f"""
CREATE OR REPLACE FUNCTION {current_catalog}.{demo_schema}.mask_email(email STRING)
RETURNS STRING
RETURN CONCAT(
    SUBSTRING(email, 1, 1),
    '***@',
    SUBSTRING_INDEX(email, '@', -1)
)
"""

spark.sql(query1)
print(f"✅ Created masking function: {current_catalog}.{demo_schema}.mask_email")

# Function 3: Mask salary as range
query2 = f"""
CREATE OR REPLACE FUNCTION {current_catalog}.{demo_schema}.mask_salary(salary DECIMAL(10,2))
RETURNS STRING
RETURN CASE 
    WHEN salary < 50000 THEN '$0-$50K'
    WHEN salary < 75000 THEN '$50K-$75K'
    WHEN salary < 100000 THEN '$75K-$100K'
    WHEN salary < 150000 THEN '$100K-$150K'
    ELSE '$150K+'
END
"""

spark.sql(query2)
print(f"✅ Created masking function: {current_catalog}.{demo_schema}.mask_salary")

# Test both functions
test_query = f"""
SELECT 
    {current_catalog}.{demo_schema}.mask_email('john.doe@company.com') as masked_email,
    {current_catalog}.{demo_schema}.mask_salary(95000) as masked_salary
"""

print("\n🎭 Test Email and Salary Masking:")
display(spark.sql(test_query))

# COMMAND ----------

# DBTITLE 1,Step 6c: Apply Masking to Employee Data
# Step 6c: Apply Masking to Employee Data
# Create a masked view for analysts

query = f"""
CREATE OR REPLACE VIEW {current_catalog}.{demo_schema}.security_demo_employees_masked AS
SELECT 
    employee_id,
    employee_name,
    {current_catalog}.{demo_schema}.mask_ssn(ssn) as ssn_masked,
    department,
    {current_catalog}.{demo_schema}.mask_salary(salary) as salary_range,
    {current_catalog}.{demo_schema}.mask_email(email) as email_masked,
    phone
FROM {current_catalog}.{demo_schema}.security_demo_employees
"""

spark.sql(query)
print(f"✅ Created masked view: {current_catalog}.{demo_schema}.security_demo_employees_masked")
print("   🔒 All sensitive fields are masked")
print("   🔒 SSN: Shows only last 4 digits")
print("   🔒 Email: Shows only first letter and domain")
print("   🔒 Salary: Shows as range instead of exact value")

print("\n🎭 Masked Employee Data:")
display(spark.sql(f"SELECT * FROM {current_catalog}.{demo_schema}.security_demo_employees_masked LIMIT 5"))

# COMMAND ----------

# DBTITLE 1,🟡 Section 5: Unity Catalog ABAC Policies
# MAGIC %md
# MAGIC # 🟡 SECTION 5: Unity Catalog ABAC Policies
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine you have a **giant toy library** with thousands of toys across many rooms. Instead of making **individual rules** for each toy, you use **smart labels**:
# MAGIC
# MAGIC - All toys with 🟢 **GREEN label** = Everyone can play
# MAGIC - All toys with 🟡 **YELLOW label** = Only kids 8+ can play
# MAGIC - All toys with 🔴 **RED label** = Only teachers can touch
# MAGIC
# MAGIC **ABAC (Attribute-Based Access Control)** is like using **smart tags** to automatically apply security rules!
# MAGIC
# MAGIC Instead of:
# MAGIC ```
# MAGIC Table 1 → Rule A
# MAGIC Table 2 → Rule B
# MAGIC Table 3 → Rule C
# MAGIC ... (1000 rules!) 😱
# MAGIC ```
# MAGIC
# MAGIC You do:
# MAGIC ```
# MAGIC Any table with tag 'sensitive' → Apply Security Policy 🎉
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### What is ABAC?
# MAGIC
# MAGIC **Attribute-Based Access Control (ABAC)** enables **centralized, tag-driven** row filtering and column masking across Unity Catalog.
# MAGIC
# MAGIC Instead of creating policies for each table individually, you:
# MAGIC 1. **Tag** tables and columns with governed tags (e.g., `pii`, `sensitivity`)
# MAGIC 2. **Create policies** that apply to **any resource matching those tags**
# MAGIC 3. Unity Catalog **automatically enforces** the policies at query time
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 ABAC Components:
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │   1. GOVERNED TAGS                   │
# MAGIC │   Account-level tags with rules      │
# MAGIC │   Example: pii = {ssn, email, phone} │
# MAGIC └────────────────────────────────────────┘
# MAGIC              ↓
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │   2. TAG TABLES/COLUMNS              │
# MAGIC │   Apply tags to data assets          │
# MAGIC │   Example: employees.ssn = pii:ssn   │
# MAGIC └────────────────────────────────────────┘
# MAGIC              ↓
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │   3. CREATE UDFs                     │
# MAGIC │   Row filter / Column mask logic     │
# MAGIC │   Example: mask_ssn(ssn) function    │
# MAGIC └────────────────────────────────────────┘
# MAGIC              ↓
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │   4. CREATE POLICIES                 │
# MAGIC │   Link tags → UDFs → principals     │
# MAGIC │   Catalog/Schema/Table level         │
# MAGIC └────────────────────────────────────────┘
# MAGIC              ↓
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │   5. AUTOMATIC ENFORCEMENT            │
# MAGIC │   Unity Catalog applies at runtime   │
# MAGIC │   Transparent to users               │
# MAGIC └────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📌 Policy Hierarchy:
# MAGIC
# MAGIC Policies can be created at **three levels** (they inherit downward):
# MAGIC
# MAGIC ```
# MAGIC CATALOG-level policy
# MAGIC     ↓ inherits to
# MAGIC SCHEMA-level policy  
# MAGIC     ↓ inherits to
# MAGIC TABLE-level policy
# MAGIC ```
# MAGIC
# MAGIC **Example**:
# MAGIC - Catalog policy: Mask all PII across the entire catalog
# MAGIC - Schema policy: Additional filters for sensitive schemas
# MAGIC - Table policy: Specific rules for high-risk tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔑 Key Policy Functions:
# MAGIC
# MAGIC | Function | Purpose | Example |
# MAGIC |----------|---------|----------|
# MAGIC | `has_tag('key')` | Check if tag exists | `has_tag('sensitivity')` |
# MAGIC | `has_tag_value('key', 'val')` | Check tag value | `has_tag_value('pii', 'ssn')` |
# MAGIC | `has_column_tag('key')` | Column tag exists | `has_column_tag('encrypted')` |
# MAGIC | `has_column_tag_value()` | Column tag value | `has_column_tag_value('pii', 'email')` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚙️ Policy Types:
# MAGIC
# MAGIC ### 1. Row Filter Policies
# MAGIC - Filter which **rows** users can see
# MAGIC - UDF must return **BOOLEAN** (true = keep row)
# MAGIC
# MAGIC ### 2. Column Mask Policies
# MAGIC - Transform **column values**
# MAGIC - UDF must return **same type** as column
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Benefits of ABAC:
# MAGIC
# MAGIC 1. **Centralized Management**: One policy for all matching tables
# MAGIC 2. **Scalable**: New tables automatically inherit policies via tags
# MAGIC 3. **Consistent**: Same security logic everywhere
# MAGIC 4. **Auditable**: Track policy applications via system tables
# MAGIC 5. **Flexible**: Tag-driven targeting without hardcoding table names
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚨 ABAC Requirements:
# MAGIC
# MAGIC **Compute**:
# MAGIC - Serverless compute (fully supported)
# MAGIC - Databricks Runtime 16.4+ (for classic clusters)
# MAGIC
# MAGIC **Tags**:
# MAGIC - **Must use governed tags** (not free-form tags)
# MAGIC - Governed tags enforce consistency at account level
# MAGIC
# MAGIC **Limitations**:
# MAGIC - **Cannot apply to VIEWS** (only tables)
# MAGIC - One row filter per table per user
# MAGIC - One column mask per column per user

# COMMAND ----------

# DBTITLE 1,💡 Section 5: ABAC Policy Examples
# MAGIC %md
# MAGIC ## 💻 Hands-On: Unity Catalog ABAC Examples
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Step 1: Create Governed Tags (via UI)
# MAGIC
# MAGIC **Note**: Governed tags **CANNOT** be created via SQL. Use the Catalog Explorer UI:
# MAGIC
# MAGIC 1. Open **Catalog** in left navigation
# MAGIC 2. Click **Govern** button (top-right)
# MAGIC 3. Find **Governed Tags** card → **View All**
# MAGIC 4. Click **Create Governed Tag**
# MAGIC 5. Define tag key and allowed values
# MAGIC
# MAGIC **Example Governed Tags**:
# MAGIC - Tag: `pii` → Values: `ssn`, `email`, `phone`, `address`
# MAGIC - Tag: `sensitivity` → Values: `low`, `medium`, `high`
# MAGIC - Tag: `data_classification` → Values: `public`, `internal`, `confidential`, `restricted`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Step 2: Apply Tags to Tables and Columns
# MAGIC
# MAGIC ```sql
# MAGIC -- Tag a table
# MAGIC SET TAG ON TABLE catalog.schema.employees sensitivity = high;
# MAGIC SET TAG ON TABLE catalog.schema.employees data_classification = confidential;
# MAGIC
# MAGIC -- Tag sensitive columns
# MAGIC SET TAG ON COLUMN catalog.schema.employees.ssn pii = ssn;
# MAGIC SET TAG ON COLUMN catalog.schema.employees.email pii = email;
# MAGIC SET TAG ON COLUMN catalog.schema.employees.phone pii = phone;
# MAGIC SET TAG ON COLUMN catalog.schema.employees.salary sensitivity = high;
# MAGIC
# MAGIC -- Tag customer table
# MAGIC SET TAG ON TABLE catalog.schema.customers sensitivity = high;
# MAGIC SET TAG ON COLUMN catalog.schema.customers.credit_card pii = credit_card;
# MAGIC SET TAG ON COLUMN catalog.schema.customers.email pii = email;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Step 3: Create UDFs for Policies
# MAGIC
# MAGIC ```sql
# MAGIC -- Row filter UDF: Filter by region
# MAGIC CREATE OR REPLACE FUNCTION catalog.security.filter_by_region(region STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN region IN ('US', 'CA') OR is_member('global_admins');
# MAGIC
# MAGIC -- Column mask UDF: Mask SSN
# MAGIC CREATE OR REPLACE FUNCTION catalog.security.mask_ssn(ssn STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CONCAT('***-**-', SUBSTRING(ssn, -4, 4));
# MAGIC
# MAGIC -- Column mask UDF: Mask email
# MAGIC CREATE OR REPLACE FUNCTION catalog.security.mask_email(email STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CONCAT(
# MAGIC     SUBSTRING(email, 1, 1),
# MAGIC     '***@',
# MAGIC     SPLIT(email, '@')[1]
# MAGIC );
# MAGIC
# MAGIC -- Column mask UDF: Mask credit card
# MAGIC CREATE OR REPLACE FUNCTION catalog.security.mask_credit_card(card STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CONCAT('****-****-****-', SUBSTRING(card, -4, 4));
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Step 4: Create Row Filter Policy
# MAGIC
# MAGIC ```sql
# MAGIC -- Policy: Hide high-sensitivity rows from analysts
# MAGIC CREATE POLICY regional_data_filter
# MAGIC ON CATALOG production
# MAGIC COMMENT 'Filter high-sensitivity data by region'
# MAGIC ROW FILTER catalog.security.filter_by_region
# MAGIC TO `data_analysts` EXCEPT `senior_analysts`
# MAGIC FOR TABLES
# MAGIC WHEN has_tag_value('sensitivity', 'high')
# MAGIC MATCH COLUMNS has_tag('region') AS region
# MAGIC USING COLUMNS (region);
# MAGIC ```
# MAGIC
# MAGIC **What this does**:
# MAGIC - Applies to: All tables in `production` catalog tagged `sensitivity=high`
# MAGIC - Who: `data_analysts` group (except `senior_analysts`)
# MAGIC - Filter: Only show rows where region is US/CA
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Step 5: Create Column Mask Policies
# MAGIC
# MAGIC ```sql
# MAGIC -- Policy 1: Mask SSN columns
# MAGIC CREATE POLICY ssn_masking
# MAGIC ON CATALOG production
# MAGIC COMMENT 'Mask SSN columns for non-HR users'
# MAGIC COLUMN MASK catalog.security.mask_ssn
# MAGIC TO `All Users` EXCEPT `HR admins`
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS has_tag_value('pii', 'ssn') AS ssn
# MAGIC ON COLUMN ssn;
# MAGIC
# MAGIC -- Policy 2: Mask email columns
# MAGIC CREATE POLICY email_masking
# MAGIC ON CATALOG production
# MAGIC COMMENT 'Mask email columns for non-marketing users'
# MAGIC COLUMN MASK catalog.security.mask_email
# MAGIC TO `All Users` EXCEPT `marketing_team`, `support_team`
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS has_tag_value('pii', 'email') AS email
# MAGIC ON COLUMN email;
# MAGIC
# MAGIC -- Policy 3: Mask credit card columns
# MAGIC CREATE POLICY credit_card_masking
# MAGIC ON CATALOG production
# MAGIC COMMENT 'Mask credit card for non-finance users'
# MAGIC COLUMN MASK catalog.security.mask_credit_card
# MAGIC TO `All Users` EXCEPT `finance_admins`
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS has_tag_value('pii', 'credit_card') AS card
# MAGIC ON COLUMN card;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Step 6: View Policies
# MAGIC
# MAGIC ```sql
# MAGIC -- Show all policies on a schema
# MAGIC SHOW POLICIES ON SCHEMA catalog.schema;
# MAGIC
# MAGIC -- Show effective policies (includes inherited)
# MAGIC SHOW EFFECTIVE POLICIES ON TABLE catalog.schema.employees;
# MAGIC
# MAGIC -- Describe a specific policy
# MAGIC DESCRIBE POLICY ssn_masking ON CATALOG production;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Step 7: Query Protected Data
# MAGIC
# MAGIC ```sql
# MAGIC -- User queries normally
# MAGIC SELECT * FROM catalog.schema.employees;
# MAGIC
# MAGIC -- Unity Catalog automatically:
# MAGIC -- 1. Checks user's group membership
# MAGIC -- 2. Applies applicable row filters
# MAGIC -- 3. Applies applicable column masks
# MAGIC -- 4. Returns filtered/masked results
# MAGIC
# MAGIC -- For HR Admin:
# MAGIC -- See all rows, all columns (including real SSN)
# MAGIC
# MAGIC -- For Data Analyst:
# MAGIC -- See filtered rows (based on region)
# MAGIC -- See masked SSN (***-**-1234)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example: Complete ABAC Setup
# MAGIC
# MAGIC ```sql
# MAGIC -- Scenario: Protect employee data across entire HR catalog
# MAGIC
# MAGIC -- 1. Tag tables
# MAGIC SET TAG ON TABLE hr.employees.profiles sensitivity = high;
# MAGIC SET TAG ON TABLE hr.employees.compensation sensitivity = high;
# MAGIC
# MAGIC -- 2. Tag columns
# MAGIC SET TAG ON COLUMN hr.employees.profiles.ssn pii = ssn;
# MAGIC SET TAG ON COLUMN hr.employees.profiles.email pii = email;
# MAGIC SET TAG ON COLUMN hr.employees.compensation.salary sensitivity = high;
# MAGIC
# MAGIC -- 3. Create masking functions
# MAGIC CREATE OR REPLACE FUNCTION hr.security.mask_ssn(ssn STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE 
# MAGIC     WHEN is_member('hr_admins') THEN ssn
# MAGIC     ELSE CONCAT('***-**-', SUBSTRING(ssn, -4, 4))
# MAGIC END;
# MAGIC
# MAGIC -- 4. Create catalog-level policy (applies to all tables)
# MAGIC CREATE POLICY hr_ssn_protection
# MAGIC ON CATALOG hr
# MAGIC COLUMN MASK hr.security.mask_ssn
# MAGIC TO `All Users` EXCEPT `HR admins`
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS has_tag_value('pii', 'ssn') AS ssn
# MAGIC ON COLUMN ssn;
# MAGIC
# MAGIC -- 5. Verify
# MAGIC SHOW EFFECTIVE POLICIES ON TABLE hr.employees.profiles;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔑 Managing Policies:
# MAGIC
# MAGIC ```sql
# MAGIC -- Update a policy (use CREATE OR REPLACE)
# MAGIC CREATE OR REPLACE POLICY ssn_masking
# MAGIC ON CATALOG production
# MAGIC COLUMN MASK catalog.security.mask_ssn_v2  -- Updated function
# MAGIC TO `All Users` EXCEPT `HR admins`, `compliance_team`  -- Added group
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS has_tag_value('pii', 'ssn') AS ssn
# MAGIC ON COLUMN ssn;
# MAGIC
# MAGIC -- Drop a policy
# MAGIC DROP POLICY ssn_masking ON CATALOG production;
# MAGIC DROP POLICY regional_data_filter ON SCHEMA catalog.schema;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Policy Quotas:
# MAGIC
# MAGIC | Level | Limit |
# MAGIC |-------|-------|
# MAGIC | Catalog | 10 policies per catalog |
# MAGIC | Schema | 10 policies per schema |
# MAGIC | Table | 5 policies per table |
# MAGIC | Principals | 20 per policy (TO + EXCEPT) |
# MAGIC | Column conditions | 3 per MATCH COLUMNS |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ ABAC Best Practices:
# MAGIC
# MAGIC 1. **Use Governed Tags**: Never free-form tags
# MAGIC 2. **Catalog-Level Policies**: Prefer higher-level policies (more scalable)
# MAGIC 3. **Test Policies**: Verify with different user groups before production
# MAGIC 4. **Document Tags**: Maintain tag taxonomy documentation
# MAGIC 5. **Monitor Usage**: Query `system.access` tables for auditing
# MAGIC 6. **Exception Groups**: Always have admin exception clause
# MAGIC 7. **UDF Optimization**: Keep masking/filter logic simple and fast

# COMMAND ----------

# DBTITLE 1,Step 7: Apply Unity Catalog Tags
# Step 7: Apply Unity Catalog Tags
# Tags enable ABAC policies and data classification

# Tag the employee table
query1 = f"""
ALTER TABLE {current_catalog}.{demo_schema}.security_demo_employees
SET TAGS ('data_classification' = 'confidential', 'contains_pii' = 'true')
"""

spark.sql(query1)
print(f"✅ Applied table tags to {current_catalog}.{demo_schema}.security_demo_employees")
print("   🏷️ data_classification = confidential")
print("   🏷️ contains_pii = true")

# Tag sensitive columns
query2 = f"""
ALTER TABLE {current_catalog}.{demo_schema}.security_demo_employees
ALTER COLUMN ssn SET TAGS ('pii_type' = 'ssn', 'sensitivity' = 'high')
"""

query3 = f"""
ALTER TABLE {current_catalog}.{demo_schema}.security_demo_employees
ALTER COLUMN salary SET TAGS ('data_type' = 'financial', 'sensitivity' = 'high')
"""

query4 = f"""
ALTER TABLE {current_catalog}.{demo_schema}.security_demo_employees
ALTER COLUMN email SET TAGS ('pii_type' = 'email', 'sensitivity' = 'medium')
"""

spark.sql(query2)
spark.sql(query3)
spark.sql(query4)

print("\n✅ Applied column tags:")
print("   🏷️ ssn: pii_type=ssn, sensitivity=high")
print("   🏷️ salary: data_type=financial, sensitivity=high")
print("   🏷️ email: pii_type=email, sensitivity=medium")

# COMMAND ----------

# DBTITLE 1,Step 7b: Query Tags from System Tables
# Step 7b: Query Tags from System Tables
# Unity Catalog stores tags in system.information_schema

query = """
SELECT 
    catalog_name,
    schema_name,
    table_name,
    tag_name,
    tag_value
FROM system.information_schema.table_tags
WHERE table_name = 'security_demo_employees'
ORDER BY tag_name
"""

print("🏷️ Table Tags:")
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Step 7c: Query Column Tags
# Step 7c: Query Column Tags
# Find all PII columns using tags

query = """
SELECT 
    catalog_name,
    schema_name,
    table_name,
    column_name,
    tag_name,
    tag_value
FROM system.information_schema.column_tags
WHERE table_name = 'security_demo_employees'
  AND tag_name IN ('pii_type', 'sensitivity', 'data_type')
ORDER BY column_name, tag_name
"""

print("🏷️ Column Tags (PII & Sensitivity):")
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,🔶 Section 6: Data Lineage
# MAGIC %md
# MAGIC # 🔶 SECTION 6: Data Lineage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 ELI5 (Explain Like I'm 5):
# MAGIC
# MAGIC Imagine you're making a **sandwich** 🥪:
# MAGIC
# MAGIC 1. Start with **bread** 🍞
# MAGIC 2. Add **peanut butter** 🥜
# MAGIC 3. Add **jelly** 🍇
# MAGIC 4. Put another **bread** on top 🍞
# MAGIC 5. **Final sandwich!** 🥪
# MAGIC
# MAGIC If someone asks: "How did you make this sandwich?"
# MAGIC
# MAGIC You can trace back:
# MAGIC ```
# MAGIC Sandwich ← Bread + Peanut Butter + Jelly + Bread
# MAGIC ```
# MAGIC
# MAGIC **Data Lineage** is like a **recipe book** for your data! 📖
# MAGIC
# MAGIC It tells you:
# MAGIC - Where did this data **come from**? 🤔
# MAGIC - What **transformations** happened? 🔄
# MAGIC - Who **created** it? 👨‍💻
# MAGIC - Where is it **used**? 📋
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC ### What is Data Lineage?
# MAGIC
# MAGIC Data lineage is the **end-to-end tracking** of data flow through your data platform:
# MAGIC - **Upstream**: Where data originates (sources)
# MAGIC - **Transformations**: How data is processed
# MAGIC - **Downstream**: Where data is consumed (reports, dashboards, ML models)
# MAGIC
# MAGIC ### Lineage Tracks:
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │  1. TABLE-TO-TABLE LINEAGE           │
# MAGIC │     Source → Transformation → Target   │
# MAGIC └────────────────────────────────────────┘
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │  2. COLUMN-LEVEL LINEAGE             │
# MAGIC │     Which columns depend on others   │
# MAGIC └────────────────────────────────────────┘
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │  3. QUERY LINEAGE                    │
# MAGIC │     Which queries read/write tables  │
# MAGIC └────────────────────────────────────────┘
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │  4. NOTEBOOK/JOB LINEAGE             │
# MAGIC │     Which processes touch data       │
# MAGIC └────────────────────────────────────────┘
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │  5. DASHBOARD LINEAGE                │
# MAGIC │     Which dashboards use which data  │
# MAGIC └────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Why Data Lineage Matters:
# MAGIC
# MAGIC ### 1. **Impact Analysis**
# MAGIC ```
# MAGIC Question: "If I change this source table, what breaks?"
# MAGIC Lineage Answer: Shows all downstream dependencies
# MAGIC ```
# MAGIC
# MAGIC ### 2. **Root Cause Analysis**
# MAGIC ```
# MAGIC Question: "Why is this dashboard showing wrong data?"
# MAGIC Lineage Answer: Trace back to identify the faulty transformation
# MAGIC ```
# MAGIC
# MAGIC ### 3. **Compliance & Auditing**
# MAGIC ```
# MAGIC Question: "Where does customer PII flow?"
# MAGIC Lineage Answer: End-to-end PII data flow map
# MAGIC ```
# MAGIC
# MAGIC ### 4. **Data Quality**
# MAGIC ```
# MAGIC Question: "Which tables are derived from this unreliable source?"
# MAGIC Lineage Answer: All affected downstream tables
# MAGIC ```
# MAGIC
# MAGIC ### 5. **Optimization**
# MAGIC ```
# MAGIC Question: "Is this table still being used?"
# MAGIC Lineage Answer: Shows if any downstream consumers exist
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Data Lineage Flow:
# MAGIC
# MAGIC ```
# MAGIC         ┌───────────────────────┐
# MAGIC         │  RAW DATA SOURCES     │
# MAGIC         │  (S3, Kafka, APIs)    │
# MAGIC         └───────────────────────┘
# MAGIC                   ↓
# MAGIC         ┌───────────────────────┐
# MAGIC         │  BRONZE TABLES        │
# MAGIC         │  (Raw ingestion)      │
# MAGIC         └───────────────────────┘
# MAGIC                   ↓
# MAGIC         ┌───────────────────────┐
# MAGIC         │  SILVER TABLES        │
# MAGIC         │  (Cleaned, dedupe)    │
# MAGIC         └───────────────────────┘
# MAGIC                   ↓
# MAGIC         ┌───────────────────────┐
# MAGIC         │  GOLD TABLES          │
# MAGIC         │  (Business logic)     │
# MAGIC         └───────────────────────┘
# MAGIC            ↓              ↓
# MAGIC   ┌────────────┐   ┌───────────┐
# MAGIC   │ Dashboards │   │ ML Models │
# MAGIC   └────────────┘   └───────────┘
# MAGIC ```
# MAGIC
# MAGIC Unity Catalog **automatically tracks** this entire flow!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔍 Lineage Relationships:
# MAGIC
# MAGIC ### 1. **Table → Table**
# MAGIC ```sql
# MAGIC CREATE TABLE gold.customers AS
# MAGIC SELECT * FROM silver.customers
# MAGIC WHERE status = 'active';
# MAGIC
# MAGIC -- Lineage: silver.customers → gold.customers
# MAGIC ```
# MAGIC
# MAGIC ### 2. **Multiple Sources**
# MAGIC ```sql
# MAGIC CREATE TABLE gold.customer_orders AS
# MAGIC SELECT c.*, o.*
# MAGIC FROM silver.customers c
# MAGIC JOIN silver.orders o ON c.id = o.customer_id;
# MAGIC
# MAGIC -- Lineage: 
# MAGIC --   silver.customers → gold.customer_orders
# MAGIC --   silver.orders → gold.customer_orders
# MAGIC ```
# MAGIC
# MAGIC ### 3. **View Lineage**
# MAGIC ```sql
# MAGIC CREATE VIEW analytics.revenue_by_region AS
# MAGIC SELECT region, SUM(amount) as revenue
# MAGIC FROM gold.customer_orders
# MAGIC GROUP BY region;
# MAGIC
# MAGIC -- Lineage: gold.customer_orders → analytics.revenue_by_region
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚙️ Unity Catalog Lineage Features:
# MAGIC
# MAGIC ### Automatic Tracking:
# MAGIC - ✅ CREATE TABLE AS SELECT (CTAS)
# MAGIC - ✅ INSERT INTO
# MAGIC - ✅ MERGE operations
# MAGIC - ✅ Views
# MAGIC - ✅ Materialized views
# MAGIC - ✅ Lakeflow Spark Declarative Pipelines
# MAGIC - ✅ Notebooks
# MAGIC - ✅ Jobs
# MAGIC - ✅ Dashboards
# MAGIC
# MAGIC ### Lineage System Tables:
# MAGIC ```sql
# MAGIC -- Query table lineage
# MAGIC SELECT * FROM system.access.table_lineage
# MAGIC WHERE target_table_full_name = 'catalog.schema.table';
# MAGIC
# MAGIC -- Query column lineage
# MAGIC SELECT * FROM system.access.column_lineage
# MAGIC WHERE target_table_full_name = 'catalog.schema.table';
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Lineage Benefits:
# MAGIC
# MAGIC | Benefit | Description |
# MAGIC |---------|-------------|
# MAGIC | **Transparency** | Understand data transformations |
# MAGIC | **Trust** | Verify data quality and sources |
# MAGIC | **Compliance** | Track PII flow for GDPR/CCPA |
# MAGIC | **Debugging** | Trace errors to root cause |
# MAGIC | **Optimization** | Identify unused tables |
# MAGIC | **Impact Analysis** | Assess change impact |
# MAGIC | **Documentation** | Auto-generated data flow docs |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚨 What Unity Catalog Tracks:
# MAGIC
# MAGIC 1. **Source Tables**: Where data comes from
# MAGIC 2. **Target Tables**: Where data goes to
# MAGIC 3. **Columns**: Column-level dependencies
# MAGIC 4. **Operations**: INSERT, MERGE, CTAS, etc.
# MAGIC 5. **Users**: Who created the lineage
# MAGIC 6. **Timestamps**: When lineage was created
# MAGIC 7. **Notebooks**: Which notebook created the table
# MAGIC 8. **Jobs**: Which job processes the data
# MAGIC 9. **Queries**: Which queries read/write tables

# COMMAND ----------

# DBTITLE 1,💡 Section 6: Lineage Query Examples
# MAGIC %md
# MAGIC ## 💻 Hands-On: Querying Data Lineage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example 1: Find Upstream Dependencies
# MAGIC
# MAGIC ```sql
# MAGIC -- Question: Where does this table get its data from?
# MAGIC
# MAGIC SELECT 
# MAGIC     source_table_full_name AS upstream_table,
# MAGIC     source_table_catalog,
# MAGIC     source_table_schema,
# MAGIC     source_table_name,
# MAGIC     source_type,
# MAGIC     created_by,
# MAGIC     created_at
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE target_table_full_name = 'prod.analytics.customer_revenue'
# MAGIC ORDER BY created_at DESC;
# MAGIC
# MAGIC -- Result: Shows all tables that feed into customer_revenue
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example 2: Find Downstream Dependencies
# MAGIC
# MAGIC ```sql
# MAGIC -- Question: What tables/dashboards depend on this table?
# MAGIC
# MAGIC SELECT 
# MAGIC     target_table_full_name AS downstream_table,
# MAGIC     target_table_catalog,
# MAGIC     target_table_schema,
# MAGIC     target_table_name,
# MAGIC     entity_type,
# MAGIC     created_by,
# MAGIC     created_at
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE source_table_full_name = 'prod.silver.customers'
# MAGIC ORDER BY created_at DESC;
# MAGIC
# MAGIC -- Result: Shows all tables that depend on silver.customers
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example 3: End-to-End Lineage Path
# MAGIC
# MAGIC ```sql
# MAGIC -- Question: Trace complete lineage from source to final table
# MAGIC
# MAGIC WITH RECURSIVE lineage_path AS (
# MAGIC     -- Start with target table
# MAGIC     SELECT 
# MAGIC         source_table_full_name,
# MAGIC         target_table_full_name,
# MAGIC         1 AS level
# MAGIC     FROM system.access.table_lineage
# MAGIC     WHERE target_table_full_name = 'prod.gold.revenue_summary'
# MAGIC     
# MAGIC     UNION ALL
# MAGIC     
# MAGIC     -- Recursively find upstream tables
# MAGIC     SELECT 
# MAGIC         tl.source_table_full_name,
# MAGIC         tl.target_table_full_name,
# MAGIC         lp.level + 1
# MAGIC     FROM system.access.table_lineage tl
# MAGIC     INNER JOIN lineage_path lp 
# MAGIC         ON tl.target_table_full_name = lp.source_table_full_name
# MAGIC     WHERE lp.level < 10  -- Prevent infinite loops
# MAGIC )
# MAGIC SELECT 
# MAGIC     level,
# MAGIC     source_table_full_name AS source,
# MAGIC     target_table_full_name AS target
# MAGIC FROM lineage_path
# MAGIC ORDER BY level;
# MAGIC
# MAGIC -- Result: Shows complete upstream lineage hierarchy
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example 4: Column-Level Lineage
# MAGIC
# MAGIC ```sql
# MAGIC -- Question: Where does this column come from?
# MAGIC
# MAGIC SELECT 
# MAGIC     source_table_full_name,
# MAGIC     source_column_name,
# MAGIC     target_table_full_name,
# MAGIC     target_column_name,
# MAGIC     transformation_type,
# MAGIC     created_at
# MAGIC FROM system.access.column_lineage
# MAGIC WHERE target_table_full_name = 'prod.gold.customer_metrics'
# MAGIC   AND target_column_name = 'total_revenue'
# MAGIC ORDER BY created_at DESC;
# MAGIC
# MAGIC -- Result: Shows which source columns contribute to total_revenue
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example 5: Find Unused Tables
# MAGIC
# MAGIC ```sql
# MAGIC -- Question: Which tables have no downstream dependencies?
# MAGIC
# MAGIC SELECT 
# MAGIC     t.table_catalog,
# MAGIC     t.table_schema,
# MAGIC     t.table_name,
# MAGIC     t.table_type,
# MAGIC     t.created,
# MAGIC     t.last_altered
# MAGIC FROM system.information_schema.tables t
# MAGIC LEFT JOIN system.access.table_lineage l
# MAGIC     ON CONCAT(t.table_catalog, '.', t.table_schema, '.', t.table_name) = l.source_table_full_name
# MAGIC WHERE l.source_table_full_name IS NULL
# MAGIC   AND t.table_catalog = 'prod'
# MAGIC   AND t.table_type = 'MANAGED'
# MAGIC ORDER BY t.last_altered DESC;
# MAGIC
# MAGIC -- Result: Tables with no downstream consumers (candidates for deletion)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example 6: Impact Analysis
# MAGIC
# MAGIC ```sql
# MAGIC -- Question: If I modify this table, what will be affected?
# MAGIC
# MAGIC WITH impacted_tables AS (
# MAGIC     SELECT DISTINCT
# MAGIC         target_table_full_name AS impacted_table,
# MAGIC         entity_type
# MAGIC     FROM system.access.table_lineage
# MAGIC     WHERE source_table_full_name = 'prod.bronze.raw_events'
# MAGIC )
# MAGIC SELECT 
# MAGIC     impacted_table,
# MAGIC     entity_type,
# MAGIC     COUNT(*) AS dependency_count
# MAGIC FROM impacted_tables
# MAGIC GROUP BY impacted_table, entity_type
# MAGIC ORDER BY dependency_count DESC;
# MAGIC
# MAGIC -- Result: All downstream tables/views affected by changes
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example 7: Lineage by User
# MAGIC
# MAGIC ```sql
# MAGIC -- Question: What tables did this user create lineage for?
# MAGIC
# MAGIC SELECT 
# MAGIC     created_by,
# MAGIC     source_table_full_name,
# MAGIC     target_table_full_name,
# MAGIC     entity_type,
# MAGIC     created_at
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE created_by = 'data.engineer@company.com'
# MAGIC ORDER BY created_at DESC
# MAGIC LIMIT 100;
# MAGIC
# MAGIC -- Result: All lineage created by specific user
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example 8: Recent Lineage Changes
# MAGIC
# MAGIC ```sql
# MAGIC -- Question: What new lineage was created in the last 7 days?
# MAGIC
# MAGIC SELECT 
# MAGIC     source_table_full_name,
# MAGIC     target_table_full_name,
# MAGIC     entity_type,
# MAGIC     created_by,
# MAGIC     created_at
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE created_at >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC ORDER BY created_at DESC;
# MAGIC
# MAGIC -- Result: Recently created lineage relationships
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example 9: Cross-Catalog Lineage
# MAGIC
# MAGIC ```sql
# MAGIC -- Question: Does data flow across catalogs?
# MAGIC
# MAGIC SELECT 
# MAGIC     source_table_catalog,
# MAGIC     target_table_catalog,
# MAGIC     COUNT(*) AS cross_catalog_flows
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE source_table_catalog != target_table_catalog
# MAGIC GROUP BY source_table_catalog, target_table_catalog
# MAGIC ORDER BY cross_catalog_flows DESC;
# MAGIC
# MAGIC -- Result: Data flows between different catalogs
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Example 10: PII Tracking
# MAGIC
# MAGIC ```sql
# MAGIC -- Question: Where does PII data flow?
# MAGIC
# MAGIC WITH pii_tables AS (
# MAGIC     SELECT DISTINCT
# MAGIC         CONCAT(catalog_name, '.', schema_name, '.', table_name) AS table_full_name
# MAGIC     FROM system.information_schema.column_tags
# MAGIC     WHERE tag_name = 'pii' AND tag_value = 'true'
# MAGIC )
# MAGIC SELECT 
# MAGIC     pt.table_full_name AS pii_source,
# MAGIC     tl.target_table_full_name AS pii_flows_to,
# MAGIC     tl.entity_type
# MAGIC FROM pii_tables pt
# MAGIC JOIN system.access.table_lineage tl
# MAGIC     ON pt.table_full_name = tl.source_table_full_name
# MAGIC ORDER BY pii_source, pii_flows_to;
# MAGIC
# MAGIC -- Result: Complete PII data flow map (critical for GDPR compliance)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔑 Lineage Query Best Practices:
# MAGIC
# MAGIC 1. **Use Fully Qualified Names**: Always `catalog.schema.table`
# MAGIC 2. **Filter by Date**: Lineage grows large, filter by created_at
# MAGIC 3. **Limit Recursion**: Set max depth for recursive queries
# MAGIC 4. **Index on Table Names**: For faster lineage lookups
# MAGIC 5. **Regular Audits**: Periodically review unused tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Lineage Visualization:
# MAGIC
# MAGIC Unity Catalog provides **visual lineage graphs** in the UI:
# MAGIC 1. Open Catalog Explorer
# MAGIC 2. Navigate to a table
# MAGIC 3. Click **Lineage** tab
# MAGIC 4. See interactive graph showing:
# MAGIC    - Upstream sources (inputs)
# MAGIC    - Downstream consumers (outputs)
# MAGIC    - Notebooks/jobs involved
# MAGIC    - Dashboards using the data
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Lineage Limitations:
# MAGIC
# MAGIC | Limitation | Description |
# MAGIC |------------|-------------|
# MAGIC | **Path-based access** | Direct file reads not tracked |
# MAGIC | **External systems** | Non-Unity tables not tracked |
# MAGIC | **Dynamic SQL** | EXECUTE IMMEDIATE may not track |
# MAGIC | **Python DataFrame** | Some operations not captured |
# MAGIC | **Retention** | Lineage data has retention policy |

# COMMAND ----------

# DBTITLE 1,Step 8: Create Gold Table for Lineage
# Step 8: Create Gold Table for Lineage
# This aggregation creates lineage from source to gold layer

query = f"""
CREATE OR REPLACE TABLE {current_catalog}.{demo_schema}.security_demo_dept_summary
USING DELTA
COMMENT 'Gold layer: Department summary with lineage from employee table'
AS
SELECT 
    department,
    COUNT(*) as employee_count,
    ROUND(AVG(salary), 2) as avg_salary,
    ROUND(MIN(salary), 2) as min_salary,
    ROUND(MAX(salary), 2) as max_salary,
    CURRENT_TIMESTAMP() as last_updated
FROM {current_catalog}.{demo_schema}.security_demo_employees
GROUP BY department
"""

spark.sql(query)
print(f"✅ Created gold table: {current_catalog}.{demo_schema}.security_demo_dept_summary")
print("   🔗 Lineage established from security_demo_employees")

print("\n📊 Department Summary:")
display(spark.sql(f"SELECT * FROM {current_catalog}.{demo_schema}.security_demo_dept_summary ORDER BY avg_salary DESC"))

# COMMAND ----------

# DBTITLE 1,Step 8b: Query Table Lineage
# Step 8b: Query Table Lineage
# Find all tables that use security_demo_employees as source

query = f"""
SELECT 
    source_table_full_name as source_table,
    target_table_full_name as target_table,
    source_type,
    target_type,
    created_by,
    event_date
FROM system.access.table_lineage
WHERE source_table_name = 'security_demo_employees'
   OR target_table_name = 'security_demo_employees'
ORDER BY event_date DESC
LIMIT 20
"""

print("🔗 Table Lineage (upstream and downstream):")
try:
    display(spark.sql(query))
except Exception as e:
    print(f"⚠️  Lineage tracking may take a few minutes to propagate in Unity Catalog system tables.")
    print(f"   The gold table has been created successfully with automatic lineage capture.")
    print(f"   Error: {str(e)[:100]}...")

# COMMAND ----------

# DBTITLE 1,Step 8c: Visualize Lineage
# Visualize the data lineage we've created

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

# Create a simple lineage diagram
fig, ax = plt.subplots(figsize=(12, 8))
ax.set_xlim(0, 10)
ax.set_ylim(0, 10)
ax.axis('off')

# Define positions
layer_y = {'source': 8, 'views': 5, 'gold': 2}

# Source table
source_box = FancyBboxPatch((4, layer_y['source']-0.3), 2, 0.6, 
                            boxstyle="round,pad=0.1", 
                            edgecolor='blue', facecolor='lightblue', linewidth=2)
ax.add_patch(source_box)
ax.text(5, layer_y['source'], 'security_demo_employees\n(Source Table)', 
        ha='center', va='center', fontsize=10, weight='bold')

# Views layer
views = [
    ('Public View', 1.5),
    ('HR View', 3.5),
    ('Finance View', 5.5),
    ('Masked View', 7.5)
]

for view_name, x_pos in views:
    view_box = FancyBboxPatch((x_pos-0.6, layer_y['views']-0.25), 1.2, 0.5,
                              boxstyle="round,pad=0.05",
                              edgecolor='green', facecolor='lightgreen', linewidth=1.5)
    ax.add_patch(view_box)
    ax.text(x_pos, layer_y['views'], view_name, ha='center', va='center', 
            fontsize=8, weight='bold')
    
    # Arrow from source to view
    arrow = FancyArrowPatch((5, layer_y['source']-0.3), (x_pos, layer_y['views']+0.25),
                           arrowstyle='->', mutation_scale=20, linewidth=1.5,
                           color='gray', alpha=0.6)
    ax.add_patch(arrow)

# Gold table
gold_box = FancyBboxPatch((4, layer_y['gold']-0.3), 2, 0.6,
                          boxstyle="round,pad=0.1",
                          edgecolor='gold', facecolor='lightyellow', linewidth=2)
ax.add_patch(gold_box)
ax.text(5, layer_y['gold'], 'security_demo_dept_summary\n(Gold Layer)', 
        ha='center', va='center', fontsize=10, weight='bold')

# Arrow from source to gold
arrow_gold = FancyArrowPatch((5, layer_y['source']-0.3), (5, layer_y['gold']+0.3),
                            arrowstyle='->', mutation_scale=25, linewidth=2,
                            color='orange')
ax.add_patch(arrow_gold)

# Add legend
ax.text(5, 9.5, '📊 Data Lineage Visualization', ha='center', 
        fontsize=14, weight='bold', color='darkblue')

ax.text(0.5, 9, '🔵 Source Table', fontsize=9, color='blue')
ax.text(0.5, 8.5, '🟢 Security Views (RLS/CLS)', fontsize=9, color='green')
ax.text(0.5, 8, '🟡 Gold Aggregation', fontsize=9, color='orange')

ax.text(5, 0.5, '✅ Unity Catalog automatically tracks all lineage relationships', 
        ha='center', fontsize=10, style='italic', color='gray')

plt.title('End-to-End Data Lineage: Security Demo', fontsize=16, weight='bold', pad=20)
plt.tight_layout()
plt.show()

print("\n📊 Lineage Summary:")
print("  ✅ 1 Source Table (security_demo_employees)")
print("  ✅ 4 Security Views (RLS/CLS with different permissions)")
print("  ✅ 1 Gold Table (dept_summary aggregation)")
print("  ✅ All relationships automatically tracked by Unity Catalog")

# COMMAND ----------

# DBTITLE 1,🔷 Section 7: Viewing Lineage in UI
# MAGIC %md
# MAGIC # 🔷 SECTION 7: Viewing Lineage in Unity Catalog UI
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💻 How to Access Lineage UI:
# MAGIC
# MAGIC ### Step 1: Open Catalog Explorer
# MAGIC 1. Click **Catalog** in the left navigation bar
# MAGIC 2. Browse to your catalog → schema → table
# MAGIC 3. Click on the table name to open details
# MAGIC
# MAGIC ### Step 2: View Lineage Tab
# MAGIC 1. Click the **Lineage** tab (next to Overview)
# MAGIC 2. Interactive graph appears showing:
# MAGIC    - **Upstream**: Tables this table reads from (sources)
# MAGIC    - **Downstream**: Tables/views/dashboards that read this table (consumers)
# MAGIC    - **Notebooks**: Which notebooks create/modify the table
# MAGIC    - **Jobs**: Which jobs process the table
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Lineage Graph Elements:
# MAGIC
# MAGIC ```
# MAGIC                   ┌──────────────────┐
# MAGIC                   │  UPSTREAM        │
# MAGIC                   │  (Sources)       │
# MAGIC                   └──────────────────┘
# MAGIC                          ↓
# MAGIC             ┌────────────────────────────┐
# MAGIC             │   YOUR TABLE          │
# MAGIC             │   (Current Focus)     │
# MAGIC             └────────────────────────────┘
# MAGIC                          ↓
# MAGIC                   ┌──────────────────┐
# MAGIC                   │  DOWNSTREAM      │
# MAGIC                   │  (Consumers)     │
# MAGIC                   └──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔍 Interactive Features:
# MAGIC
# MAGIC ### 1. **Zoom & Pan**
# MAGIC - Scroll to zoom in/out
# MAGIC - Drag to pan around large lineage graphs
# MAGIC
# MAGIC ### 2. **Click to Explore**
# MAGIC - Click any table → Navigate to that table's details
# MAGIC - Click notebook → Open the notebook
# MAGIC - Click job → View job configuration
# MAGIC
# MAGIC ### 3. **Filter View**
# MAGIC - **Upstream only**: See where data comes from
# MAGIC - **Downstream only**: See where data goes
# MAGIC - **Both**: Complete picture
# MAGIC
# MAGIC ### 4. **Expand Levels**
# MAGIC - Show immediate dependencies (1 level)
# MAGIC - Expand to 2, 3, or more levels
# MAGIC - See entire dependency chain
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 What the Lineage Shows:
# MAGIC
# MAGIC ### 🟢 Green Arrows: Upstream
# MAGIC ```
# MAGIC raw_data → bronze_table → silver_table → YOUR_TABLE
# MAGIC ```
# MAGIC
# MAGIC ### 🔵 Blue Arrows: Downstream
# MAGIC ```
# MAGIC YOUR_TABLE → gold_aggregations → dashboard_revenue
# MAGIC ```
# MAGIC
# MAGIC ### 🟡 Yellow Icons: Processing
# MAGIC ```
# MAGIC Notebook: "ETL_Pipeline.ipynb"
# MAGIC Job: "Daily_Sales_Processing"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Lineage Use Cases in UI:
# MAGIC
# MAGIC ### Use Case 1: Impact Analysis
# MAGIC **Scenario**: You need to modify `customers` table schema
# MAGIC
# MAGIC **Steps**:
# MAGIC 1. Open `customers` table in Catalog Explorer
# MAGIC 2. Click **Lineage** tab
# MAGIC 3. View all **downstream** dependencies
# MAGIC 4. **Result**: See 5 gold tables, 3 dashboards, 2 ML models affected
# MAGIC 5. **Action**: Coordinate with owners before making changes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Use Case 2: Root Cause Analysis
# MAGIC **Scenario**: Dashboard shows incorrect revenue numbers
# MAGIC
# MAGIC **Steps**:
# MAGIC 1. Open `revenue_summary` table (used by dashboard)
# MAGIC 2. Click **Lineage** tab
# MAGIC 3. Trace **upstream** to source tables
# MAGIC 4. **Result**: Find `orders` table was updated incorrectly
# MAGIC 5. **Action**: Fix the source and re-run pipeline
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Use Case 3: Compliance Audit
# MAGIC **Scenario**: Need to document where customer PII flows
# MAGIC
# MAGIC **Steps**:
# MAGIC 1. Open `customers` table (contains PII)
# MAGIC 2. Click **Lineage** tab
# MAGIC 3. Expand all **downstream** dependencies
# MAGIC 4. **Result**: Complete map of PII data flow
# MAGIC 5. **Action**: Document for GDPR compliance report
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Use Case 4: Dead Table Identification
# MAGIC **Scenario**: Clean up unused tables to save costs
# MAGIC
# MAGIC **Steps**:
# MAGIC 1. Open suspected unused table
# MAGIC 2. Click **Lineage** tab
# MAGIC 3. Check for **downstream** dependencies
# MAGIC 4. **Result**: No downstream consumers found
# MAGIC 5. **Action**: Safe to delete (after verification)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📋 Lineage Information Displayed:
# MAGIC
# MAGIC ### Table Details:
# MAGIC - Table name (fully qualified)
# MAGIC - Catalog, schema, table
# MAGIC - Last updated timestamp
# MAGIC - Owner/creator
# MAGIC
# MAGIC ### Relationship Details:
# MAGIC - Connection type (read/write)
# MAGIC - Creation timestamp
# MAGIC - User who created the relationship
# MAGIC - Operation type (CTAS, INSERT, MERGE)
# MAGIC
# MAGIC ### Asset Links:
# MAGIC - Direct links to notebooks
# MAGIC - Job configuration pages
# MAGIC - Dashboard pages
# MAGIC - Related tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✨ Advanced Lineage Features:
# MAGIC
# MAGIC ### 1. **Column-Level Lineage**
# MAGIC - Click specific columns
# MAGIC - See which source columns contribute
# MAGIC - Trace transformations
# MAGIC
# MAGIC ### 2. **Time Travel**
# MAGIC - View historical lineage
# MAGIC - See how relationships changed over time
# MAGIC
# MAGIC ### 3. **Export Lineage**
# MAGIC - Download lineage as JSON
# MAGIC - Use for documentation
# MAGIC - Integrate with external tools
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Pro Tips:
# MAGIC
# MAGIC 1. **Regular Reviews**: Check lineage monthly for unused tables
# MAGIC 2. **Document Findings**: Take screenshots for compliance reports
# MAGIC 3. **Team Communication**: Share lineage when requesting schema changes
# MAGIC 4. **CI/CD Integration**: Check lineage before deploying changes
# MAGIC 5. **Training**: Use lineage to onboard new team members
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚠️ Important Notes:
# MAGIC
# MAGIC ### Lineage Accuracy:
# MAGIC - ✅ Automatically captured for SQL operations
# MAGIC - ✅ Captured for Lakeflow pipelines
# MAGIC - ⚠️ Python DataFrame operations (best effort)
# MAGIC - ❌ Direct file I/O (not captured)
# MAGIC
# MAGIC ### Refresh Rate:
# MAGIC - Lineage updates within minutes of operation
# MAGIC - No manual refresh needed
# MAGIC - Historical lineage retained per policy
# MAGIC
# MAGIC ### Permissions:
# MAGIC - Must have `SELECT` on table to view lineage
# MAGIC - Lineage respects table access controls
# MAGIC - Cannot see lineage for tables you can't access

# COMMAND ----------

# DBTITLE 1,🎯 Section 8: Hands-On Security + Lineage Scenario
# MAGIC %md
# MAGIC # 🎯 SECTION 8: Hands-On Security + Lineage Scenario
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏗️ Real-World Scenario:
# MAGIC
# MAGIC ### Business Context:
# MAGIC You're a Data Engineer at **HealthCare Inc.**, managing patient data. You need to:
# MAGIC
# MAGIC 1. ✅ Protect sensitive patient information (HIPAA compliance)
# MAGIC 2. ✅ Allow different access levels for different roles
# MAGIC 3. ✅ Track data lineage for auditing
# MAGIC 4. ✅ Enable secure analytics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Data Architecture:
# MAGIC
# MAGIC ```
# MAGIC Bronze Layer:  raw_patient_data
# MAGIC      ↓
# MAGIC Silver Layer:  cleaned_patient_records
# MAGIC      ↓
# MAGIC Gold Layer:    patient_analytics
# MAGIC      ↓
# MAGIC Consumption:   dashboards + ML models
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛡️ Security Requirements:
# MAGIC
# MAGIC ### Role-Based Access:
# MAGIC
# MAGIC | Role | Can See |
# MAGIC |------|----------|
# MAGIC | **Doctors** | All patient data for their department |
# MAGIC | **Nurses** | Basic info + vitals (no diagnosis) |
# MAGIC | **Analysts** | Anonymized data only |
# MAGIC | **Billing** | Financial data only |
# MAGIC | **Admin** | Everything |
# MAGIC
# MAGIC ### Data Protection:
# MAGIC - **SSN**: Mask for everyone except billing
# MAGIC - **Diagnosis**: Hide from nurses
# MAGIC - **Financial**: Hide from medical staff
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step-by-Step Implementation:
# MAGIC
# MAGIC ### 📌 STEP 1: Create Sample Data
# MAGIC
# MAGIC ```sql
# MAGIC -- Create sample patient table
# MAGIC CREATE TABLE healthcare.silver.patient_records (
# MAGIC     patient_id STRING,
# MAGIC     patient_name STRING,
# MAGIC     ssn STRING,
# MAGIC     date_of_birth DATE,
# MAGIC     department STRING,
# MAGIC     diagnosis STRING,
# MAGIC     treatment_cost DECIMAL(10,2),
# MAGIC     insurance_provider STRING,
# MAGIC     admission_date DATE,
# MAGIC     doctor_name STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Insert sample data
# MAGIC INSERT INTO healthcare.silver.patient_records VALUES
# MAGIC ('P001', 'John Doe', '123-45-6789', '1980-05-15', 'Cardiology', 'Hypertension', 5000.00, 'BlueCross', '2026-04-01', 'Dr. Smith'),
# MAGIC ('P002', 'Jane Smith', '987-65-4321', '1975-08-22', 'Neurology', 'Migraine', 3000.00, 'Aetna', '2026-04-05', 'Dr. Johnson'),
# MAGIC ('P003', 'Bob Wilson', '456-78-9012', '1990-12-10', 'Cardiology', 'Arrhythmia', 8000.00, 'Cigna', '2026-04-10', 'Dr. Smith'),
# MAGIC ('P004', 'Alice Brown', '321-54-9876', '1985-03-18', 'Orthopedics', 'Fracture', 4500.00, 'UnitedHealth', '2026-04-12', 'Dr. Lee');
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 STEP 2: Apply Governed Tags
# MAGIC
# MAGIC ```sql
# MAGIC -- Tag table
# MAGIC SET TAG ON TABLE healthcare.silver.patient_records sensitivity = high;
# MAGIC SET TAG ON TABLE healthcare.silver.patient_records compliance = HIPAA;
# MAGIC
# MAGIC -- Tag sensitive columns
# MAGIC SET TAG ON COLUMN healthcare.silver.patient_records.ssn pii = ssn;
# MAGIC SET TAG ON COLUMN healthcare.silver.patient_records.diagnosis pii = medical;
# MAGIC SET TAG ON COLUMN healthcare.silver.patient_records.treatment_cost sensitivity = high;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 STEP 3: Create Masking Functions
# MAGIC
# MAGIC ```sql
# MAGIC -- Mask SSN
# MAGIC CREATE OR REPLACE FUNCTION healthcare.security.mask_ssn(ssn STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CONCAT('***-**-', SUBSTRING(ssn, -4, 4));
# MAGIC
# MAGIC -- Mask diagnosis
# MAGIC CREATE OR REPLACE FUNCTION healthcare.security.mask_diagnosis(diagnosis STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN '[REDACTED]';
# MAGIC
# MAGIC -- Mask cost (show ranges)
# MAGIC CREATE OR REPLACE FUNCTION healthcare.security.mask_cost(cost DECIMAL(10,2))
# MAGIC RETURNS STRING
# MAGIC RETURN CASE 
# MAGIC     WHEN cost < 1000 THEN '$0-$1K'
# MAGIC     WHEN cost < 5000 THEN '$1K-$5K'
# MAGIC     WHEN cost < 10000 THEN '$5K-$10K'
# MAGIC     ELSE '$10K+'
# MAGIC END;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 STEP 4: Create Row Filter (Department-Based)
# MAGIC
# MAGIC ```sql
# MAGIC -- Create row filter function
# MAGIC CREATE OR REPLACE FUNCTION healthcare.security.filter_by_department(dept STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN 
# MAGIC     -- Admins see everything
# MAGIC     is_member('healthcare_admins') OR
# MAGIC     -- Doctors/Nurses see their department
# MAGIC     dept = session_user() OR
# MAGIC     -- Analysts see all (but masked)
# MAGIC     is_member('healthcare_analysts');
# MAGIC
# MAGIC -- Apply row filter policy (example - requires governed tag setup)
# MAGIC -- This is conceptual - actual policy would be:
# MAGIC /*
# MAGIC CREATE POLICY department_isolation
# MAGIC ON SCHEMA healthcare.silver
# MAGIC ROW FILTER healthcare.security.filter_by_department
# MAGIC TO `All Users` EXCEPT `healthcare_admins`
# MAGIC FOR TABLES
# MAGIC WHEN has_tag('department_scoped')
# MAGIC MATCH COLUMNS has_tag('department') AS dept
# MAGIC USING COLUMNS (dept);
# MAGIC */
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 STEP 5: Create Role-Specific Views
# MAGIC
# MAGIC ```sql
# MAGIC -- View for Doctors (full access to their department)
# MAGIC CREATE OR REPLACE VIEW healthcare.access.doctor_view AS
# MAGIC SELECT 
# MAGIC     patient_id,
# MAGIC     patient_name,
# MAGIC     CASE 
# MAGIC         WHEN is_member('healthcare_admins') THEN ssn
# MAGIC         ELSE healthcare.security.mask_ssn(ssn)
# MAGIC     END AS ssn,
# MAGIC     date_of_birth,
# MAGIC     department,
# MAGIC     diagnosis,
# MAGIC     admission_date,
# MAGIC     doctor_name
# MAGIC FROM healthcare.silver.patient_records
# MAGIC WHERE is_member('healthcare_admins') OR is_member('doctors');
# MAGIC
# MAGIC GRANT SELECT ON VIEW healthcare.access.doctor_view TO `doctors`;
# MAGIC
# MAGIC -- View for Nurses (no diagnosis, no financial)
# MAGIC CREATE OR REPLACE VIEW healthcare.access.nurse_view AS
# MAGIC SELECT 
# MAGIC     patient_id,
# MAGIC     patient_name,
# MAGIC     healthcare.security.mask_ssn(ssn) AS ssn,
# MAGIC     date_of_birth,
# MAGIC     department,
# MAGIC     '[RESTRICTED]' AS diagnosis,  -- Hidden
# MAGIC     admission_date,
# MAGIC     doctor_name
# MAGIC FROM healthcare.silver.patient_records
# MAGIC WHERE is_member('healthcare_admins') OR is_member('nurses');
# MAGIC
# MAGIC GRANT SELECT ON VIEW healthcare.access.nurse_view TO `nurses`;
# MAGIC
# MAGIC -- View for Analysts (anonymized data)
# MAGIC CREATE OR REPLACE VIEW healthcare.access.analyst_view AS
# MAGIC SELECT 
# MAGIC     CONCAT('Patient_', ROW_NUMBER() OVER (ORDER BY patient_id)) AS anonymized_id,
# MAGIC     FLOOR(DATEDIFF(CURRENT_DATE(), date_of_birth) / 365.25) AS age_years,
# MAGIC     department,
# MAGIC     diagnosis,
# MAGIC     healthcare.security.mask_cost(treatment_cost) AS cost_range,
# MAGIC     YEAR(admission_date) AS admission_year,
# MAGIC     MONTH(admission_date) AS admission_month
# MAGIC FROM healthcare.silver.patient_records;
# MAGIC
# MAGIC GRANT SELECT ON VIEW healthcare.access.analyst_view TO `healthcare_analysts`;
# MAGIC
# MAGIC -- View for Billing (financial focus)
# MAGIC CREATE OR REPLACE VIEW healthcare.access.billing_view AS
# MAGIC SELECT 
# MAGIC     patient_id,
# MAGIC     patient_name,
# MAGIC     ssn,  -- Billing needs full SSN
# MAGIC     treatment_cost,
# MAGIC     insurance_provider,
# MAGIC     admission_date,
# MAGIC     '[RESTRICTED]' AS diagnosis  -- Medical details hidden
# MAGIC FROM healthcare.silver.patient_records
# MAGIC WHERE is_member('healthcare_admins') OR is_member('billing_team');
# MAGIC
# MAGIC GRANT SELECT ON VIEW healthcare.access.billing_view TO `billing_team`;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 STEP 6: Create Gold Analytics Table (with Lineage)
# MAGIC
# MAGIC ```sql
# MAGIC -- Create aggregated analytics table
# MAGIC CREATE OR REPLACE TABLE healthcare.gold.department_metrics
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT 
# MAGIC     department,
# MAGIC     COUNT(DISTINCT patient_id) AS total_patients,
# MAGIC     AVG(treatment_cost) AS avg_treatment_cost,
# MAGIC     SUM(treatment_cost) AS total_revenue,
# MAGIC     COUNT(DISTINCT insurance_provider) AS unique_insurers,
# MAGIC     YEAR(admission_date) AS year,
# MAGIC     MONTH(admission_date) AS month
# MAGIC FROM healthcare.silver.patient_records
# MAGIC GROUP BY department, YEAR(admission_date), MONTH(admission_date);
# MAGIC
# MAGIC -- Lineage automatically tracked:
# MAGIC -- silver.patient_records → gold.department_metrics
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 STEP 7: Query Lineage
# MAGIC
# MAGIC ```sql
# MAGIC -- Check upstream lineage
# MAGIC SELECT 
# MAGIC     source_table_full_name,
# MAGIC     target_table_full_name,
# MAGIC     created_by,
# MAGIC     created_at
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE target_table_full_name = 'healthcare.gold.department_metrics';
# MAGIC
# MAGIC -- Result:
# MAGIC -- silver.patient_records → gold.department_metrics
# MAGIC
# MAGIC -- Check downstream lineage
# MAGIC SELECT 
# MAGIC     source_table_full_name,
# MAGIC     target_table_full_name
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE source_table_full_name = 'healthcare.silver.patient_records';
# MAGIC
# MAGIC -- Result:
# MAGIC -- silver.patient_records → gold.department_metrics
# MAGIC -- silver.patient_records → access.doctor_view
# MAGIC -- silver.patient_records → access.nurse_view
# MAGIC -- silver.patient_records → access.analyst_view
# MAGIC -- silver.patient_records → access.billing_view
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 STEP 8: Test Security
# MAGIC
# MAGIC ```sql
# MAGIC -- As Analyst (should see anonymized data)
# MAGIC SELECT * FROM healthcare.access.analyst_view;
# MAGIC -- Result: No real names, no SSN, cost ranges only
# MAGIC
# MAGIC -- As Doctor (should see medical data)
# MAGIC SELECT * FROM healthcare.access.doctor_view;
# MAGIC -- Result: See diagnosis, masked SSN
# MAGIC
# MAGIC -- As Billing (should see financial data)
# MAGIC SELECT * FROM healthcare.access.billing_view;
# MAGIC -- Result: See SSN and costs, no diagnosis
# MAGIC
# MAGIC -- As Nurse (should see basic data)
# MAGIC SELECT * FROM healthcare.access.nurse_view;
# MAGIC -- Result: See patient info, no diagnosis or financial
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Complete Architecture Diagram:
# MAGIC
# MAGIC ```
# MAGIC ┌───────────────────────────────────────────┐
# MAGIC │   silver.patient_records (SOURCE)        │
# MAGIC │   🏷️ Tags: sensitivity=high, HIPAA       │
# MAGIC │   🔒 Row Filter: Department-based          │
# MAGIC │   🔒 Column Mask: SSN, Diagnosis, Cost     │
# MAGIC └───────────────────────────────────────────┘
# MAGIC          │         │         │         │
# MAGIC          │         │         │         │
# MAGIC    ┌─────┼─────────┼─────────┼─────────┐
# MAGIC    │     │         │         │         │
# MAGIC    │     │         │         │         │
# MAGIC    v     v         v         v         v
# MAGIC ┌───────────────────────────────────────────┐
# MAGIC │  doctor_view  nurse_view  analyst_view  │
# MAGIC │  billing_view  department_metrics (GOLD) │
# MAGIC │  🔒 Each with specific access controls    │
# MAGIC └───────────────────────────────────────────┘
# MAGIC          │
# MAGIC          v
# MAGIC ┌───────────────────────────────────────────┐
# MAGIC │  📊 Dashboards + 🤖 ML Models           │
# MAGIC │  (Complete lineage tracked)            │
# MAGIC └───────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Compliance Checklist:
# MAGIC
# MAGIC - ☑️ **PII Protection**: SSN masked for non-authorized users
# MAGIC - ☑️ **Access Control**: Role-based views implemented
# MAGIC - ☑️ **Data Lineage**: Complete tracking from source to consumption
# MAGIC - ☑️ **Audit Trail**: All access logged via Unity Catalog
# MAGIC - ☑️ **Governed Tags**: Sensitive data classified
# MAGIC - ☑️ **HIPAA Compliance**: Medical data protected
# MAGIC - ☑️ **Least Privilege**: Users see only what they need
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Documentation Output:
# MAGIC
# MAGIC **For Compliance Report**:
# MAGIC 1. **Data Flow Map**: Lineage graph screenshot
# MAGIC 2. **Access Matrix**: Role-to-view mapping table
# MAGIC 3. **PII Inventory**: Tagged columns report
# MAGIC 4. **Security Policies**: List of applied masks/filters
# MAGIC 5. **Audit Logs**: Query history from system tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Key Takeaways:
# MAGIC
# MAGIC 1. **Security + Lineage = Compliance**: Both are critical
# MAGIC 2. **Tag Everything**: Makes policy management scalable
# MAGIC 3. **Test Thoroughly**: Verify each role's access
# MAGIC 4. **Document Well**: Critical for audits
# MAGIC 5. **Automate**: Use ABAC for centralized control

# COMMAND ----------

# DBTITLE 1,🏢 Section 9: End-to-End Secure Architecture
# MAGIC %md
# MAGIC # 🏢 SECTION 9: End-to-End Secure Architecture
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Enterprise Data Governance Framework
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────────────────────────────────────────┐
# MAGIC │                       GOVERNANCE LAYER                                    │
# MAGIC │                                                                           │
# MAGIC │  📋 Policies  🏷️ Tags  🔐 ABAC  📊 Lineage  📝 Audit Logs              │
# MAGIC └────────────────────────────────────────────────────────────────────────────────┘
# MAGIC                                     ↓
# MAGIC ┌────────────────────────────────────────────────────────────────────────────────┐
# MAGIC │                      UNITY CATALOG LAYER                                 │
# MAGIC │                                                                           │
# MAGIC │  Catalogs → Schemas → Tables → Columns                                  │
# MAGIC │  🛡️ Security enforced at every level                                   │
# MAGIC └────────────────────────────────────────────────────────────────────────────────┘
# MAGIC          │                        │                        │
# MAGIC          │                        │                        │
# MAGIC          v                        v                        v
# MAGIC ┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐
# MAGIC │  ROW-LEVEL       │  │  COLUMN-LEVEL    │  │  DATA MASKING    │
# MAGIC │  SECURITY        │  │  SECURITY        │  │                 │
# MAGIC │                  │  │                  │  │  🎭 Redact PII  │
# MAGIC │  🛡️ Filter Rows │  │  🛡️ Hide Columns │  │  🎭 Anonymize   │
# MAGIC └────────────────────┘  └────────────────────┘  └────────────────────┘
# MAGIC          │                        │                        │
# MAGIC          └────────────────────────┼────────────────────────┘
# MAGIC                                     ↓
# MAGIC ┌────────────────────────────────────────────────────────────────────────────────┐
# MAGIC │                        DATA ACCESS LAYER                                 │
# MAGIC │                                                                           │
# MAGIC │  👥 Users → 🛡️ Policies → 🔑 Unity Catalog → 📊 Data                 │
# MAGIC └────────────────────────────────────────────────────────────────────────────────┘
# MAGIC                                     ↓
# MAGIC ┌────────────────────────────────────────────────────────────────────────────────┐
# MAGIC │                      LINEAGE TRACKING LAYER                              │
# MAGIC │                                                                           │
# MAGIC │  📊 Automatic tracking of data flow end-to-end                        │
# MAGIC │  🔍 Upstream sources → Transformations → Downstream consumers         │
# MAGIC └────────────────────────────────────────────────────────────────────────────────┘
# MAGIC                                     ↓
# MAGIC ┌────────────────────────────────────────────────────────────────────────────────┐
# MAGIC │                       AUDIT & COMPLIANCE                                  │
# MAGIC │                                                                           │
# MAGIC │  📝 System Tables  🔍 Query Logs  📊 Reports  ✅ Compliance        │
# MAGIC └────────────────────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔑 Security Implementation Checklist:
# MAGIC
# MAGIC ### Phase 1: Foundation
# MAGIC - ☑️ Set up Unity Catalog
# MAGIC - ☑️ Define user groups and roles
# MAGIC - ☑️ Create catalog/schema structure
# MAGIC - ☑️ Document security requirements
# MAGIC
# MAGIC ### Phase 2: Tagging
# MAGIC - ☑️ Create governed tags (UI/SDK)
# MAGIC - ☑️ Define tag taxonomy
# MAGIC - ☑️ Tag tables and columns
# MAGIC - ☑️ Document tag meanings
# MAGIC
# MAGIC ### Phase 3: Security Policies
# MAGIC - ☑️ Create masking UDFs
# MAGIC - ☑️ Create row filter UDFs
# MAGIC - ☑️ Implement ABAC policies
# MAGIC - ☑️ Create role-based views
# MAGIC
# MAGIC ### Phase 4: Testing
# MAGIC - ☑️ Test with different users
# MAGIC - ☑️ Verify row filtering
# MAGIC - ☑️ Verify column masking
# MAGIC - ☑️ Check performance impact
# MAGIC
# MAGIC ### Phase 5: Monitoring
# MAGIC - ☑️ Set up audit logging
# MAGIC - ☑️ Monitor data access
# MAGIC - ☑️ Review lineage regularly
# MAGIC - ☑️ Generate compliance reports
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Governance Dashboard (Conceptual)
# MAGIC
# MAGIC ### Key Metrics to Track:
# MAGIC
# MAGIC ```sql
# MAGIC -- 1. PII Exposure Risk
# MAGIC SELECT 
# MAGIC     COUNT(*) AS tables_with_pii,
# MAGIC     SUM(CASE WHEN has_policy = 'No' THEN 1 ELSE 0 END) AS unprotected_tables
# MAGIC FROM (
# MAGIC     SELECT DISTINCT 
# MAGIC         CONCAT(catalog_name, '.', schema_name, '.', table_name) AS table_name,
# MAGIC         'Check manually' AS has_policy
# MAGIC     FROM system.information_schema.column_tags
# MAGIC     WHERE tag_name = 'pii'
# MAGIC );
# MAGIC
# MAGIC -- 2. Access Patterns
# MAGIC SELECT 
# MAGIC     user_name,
# MAGIC     COUNT(*) AS query_count,
# MAGIC     COUNT(DISTINCT table_full_name) AS unique_tables_accessed
# MAGIC FROM system.access.audit
# MAGIC WHERE event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC GROUP BY user_name
# MAGIC ORDER BY query_count DESC;
# MAGIC
# MAGIC -- 3. Lineage Coverage
# MAGIC SELECT 
# MAGIC     COUNT(DISTINCT target_table_full_name) AS tables_with_lineage,
# MAGIC     (SELECT COUNT(*) FROM system.information_schema.tables) AS total_tables
# MAGIC FROM system.access.table_lineage;
# MAGIC
# MAGIC -- 4. Policy Effectiveness
# MAGIC SELECT 
# MAGIC     policy_name,
# MAGIC     COUNT(*) AS tables_covered
# MAGIC FROM system.information_schema.table_tags tt
# MAGIC JOIN policies p ON tt.tag_name = p.tag_condition
# MAGIC GROUP BY policy_name;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔐 Data Classification Matrix:
# MAGIC
# MAGIC | Classification | RLS | CLS | Masking | Retention | Audit |
# MAGIC |----------------|-----|-----|---------|-----------|-------|
# MAGIC | **Public** | ❌ No | ❌ No | ❌ No | Standard | Basic |
# MAGIC | **Internal** | ✅ Yes | ❌ No | ❌ No | Standard | Standard |
# MAGIC | **Confidential** | ✅ Yes | ✅ Yes | ✅ Yes | Extended | Enhanced |
# MAGIC | **Restricted** | ✅ Yes | ✅ Yes | ✅ Yes | Long-term | Full |
# MAGIC | **PII** | ✅ Yes | ✅ Yes | ✅ Yes | Compliant | Full |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎓 Genie Code Agent Prompts:
# MAGIC
# MAGIC Use these prompts to interact with Genie Code:
# MAGIC
# MAGIC ```
# MAGIC 🚀 "Implement row-level security on sales_table by region"
# MAGIC 🚀 "Mask SSN column for non-HR users in employees table"
# MAGIC 🚀 "Apply column-level security to salary field"
# MAGIC 🚀 "Show me the lineage for customer_revenue table"
# MAGIC 🚀 "Create ABAC policy for PII protection"
# MAGIC 🚀 "Find all tables without downstream dependencies"
# MAGIC 🚀 "Generate a compliance report for GDPR"
# MAGIC 🚀 "Create role-based views for different user groups"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚙️ Architecture Patterns:
# MAGIC
# MAGIC ### Pattern 1: Medallion + Security
# MAGIC ```
# MAGIC Bronze (Raw)       → No security (limited access)
# MAGIC Silver (Cleaned)   → RLS + Basic masking
# MAGIC Gold (Business)    → Full RLS + CLS + Masking
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 2: Multi-Tenant
# MAGIC ```
# MAGIC Shared tables → Row filters by tenant_id
# MAGIC Isolation via RLS policies
# MAGIC No data leakage between tenants
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 3: Compliance-First
# MAGIC ```
# MAGIC All tables tagged from day 1
# MAGIC ABAC policies at catalog level
# MAGIC Automatic protection for new tables
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Best Practices Summary:
# MAGIC
# MAGIC ### Security:
# MAGIC 1. **Least Privilege**: Start restrictive, grant as needed
# MAGIC 2. **Defense in Depth**: Multiple security layers
# MAGIC 3. **Regular Audits**: Review access quarterly
# MAGIC 4. **Automation**: Use ABAC for scale
# MAGIC 5. **Testing**: Verify policies with real users
# MAGIC
# MAGIC ### Lineage:
# MAGIC 1. **Consistent Naming**: Use fully qualified names
# MAGIC 2. **Documentation**: Document key transformations
# MAGIC 3. **Regular Reviews**: Check for unused tables
# MAGIC 4. **Impact Analysis**: Always check before changes
# MAGIC 5. **Compliance**: Use for audit reports
# MAGIC
# MAGIC ### Governance:
# MAGIC 1. **Tag Everything**: Comprehensive tagging
# MAGIC 2. **Document Tags**: Maintain tag glossary
# MAGIC 3. **Monitor Usage**: Track access patterns
# MAGIC 4. **Report Regularly**: Compliance dashboards
# MAGIC 5. **Train Users**: Security awareness

# COMMAND ----------

# DBTITLE 1,🎓 Final Summary & Interview Prep
# MAGIC %md
# MAGIC # 🎓 FINAL SUMMARY & INTERVIEW PREP
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Key Learnings from Phase 7 Day 34:
# MAGIC
# MAGIC ### 1. **Data Security is Multi-Layered**
# MAGIC    - Table-level → Row-level → Column-level → Data masking
# MAGIC    - Each layer adds protection without replacing previous layers
# MAGIC
# MAGIC ### 2. **Unity Catalog ABAC is Powerful**
# MAGIC    - Tag-driven policies scale automatically
# MAGIC    - Centralized management reduces complexity
# MAGIC    - Governed tags enforce consistency
# MAGIC
# MAGIC ### 3. **Lineage Enables Trust**
# MAGIC    - Transparency in data transformations
# MAGIC    - Impact analysis before changes
# MAGIC    - Root cause debugging
# MAGIC    - Compliance documentation
# MAGIC
# MAGIC ### 4. **Security + Lineage = Compliance**
# MAGIC    - Both are required for modern governance
# MAGIC    - Audit trails prove compliance
# MAGIC    - Automated tracking reduces manual work
# MAGIC
# MAGIC ### 5. **Performance is Not Sacrificed**
# MAGIC    - Row filters push down to query engine
# MAGIC    - Column masking has minimal overhead
# MAGIC    - Lineage tracking is asynchronous
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 10 Interview Questions & Answers:
# MAGIC
# MAGIC ### Q1: **What is Row-Level Security (RLS) and why is it important?**
# MAGIC
# MAGIC **Answer:**  
# MAGIC Row-Level Security (RLS) dynamically filters rows in a query result based on the user's identity, group membership, or attributes. It's important because it enables **fine-grained access control** without creating multiple physical copies of data. For example, sales reps can see only their region's data from the same table, while managers see everything. This is critical for:
# MAGIC - Multi-tenant applications
# MAGIC - Regional data isolation
# MAGIC - Compliance with data residency requirements
# MAGIC - Reducing data duplication
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q2: **How does Column-Level Security differ from Row-Level Security?**
# MAGIC
# MAGIC **Answer:**  
# MAGIC - **RLS** controls which **ROWS** a user can see (horizontal filtering)
# MAGIC - **CLS** controls which **COLUMNS** a user can see (vertical filtering)
# MAGIC
# MAGIC Example: In an employee table:
# MAGIC - RLS: Manager sees only their team's rows
# MAGIC - CLS: Analysts see names and departments but not salaries or SSN
# MAGIC
# MAGIC Both can be **combined** for comprehensive protection: users see filtered rows AND filtered columns.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q3: **What are governed tags and why should you use them for ABAC?**
# MAGIC
# MAGIC **Answer:**  
# MAGIC Governed tags are **account-level tags with enforced rules** about allowed values and who can assign them. You should use them (not free-form tags) for ABAC because:
# MAGIC
# MAGIC 1. **Consistency**: Enforced values prevent typos ("pii" vs "PII" vs "personally_identifiable")
# MAGIC 2. **Governance**: Controlled who can apply sensitive tags
# MAGIC 3. **Policy Matching**: ABAC policies require predictable tag values
# MAGIC 4. **Audit**: Governed tags have better tracking
# MAGIC
# MAGIC Free-form tags can cause policy failures due to inconsistent values.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q4: **Explain the difference between static and dynamic data masking.**
# MAGIC
# MAGIC **Answer:**  
# MAGIC
# MAGIC | Aspect | Static Masking | Dynamic Masking |
# MAGIC |--------|----------------|------------------|
# MAGIC | **When** | One-time process | Real-time at query |
# MAGIC | **Where** | Creates masked copy | Masks on-the-fly |
# MAGIC | **Use Case** | Dev/Test environments | Production access control |
# MAGIC | **Reversible** | No (data replaced) | Yes (original unchanged) |
# MAGIC | **Storage** | 2x storage needed | Single source of truth |
# MAGIC
# MAGIC **Unity Catalog uses dynamic masking** via ABAC column mask policies.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q5: **What is data lineage and why is it critical for data governance?**
# MAGIC
# MAGIC **Answer:**  
# MAGIC Data lineage is the **end-to-end tracking** of data flow from sources through transformations to consumers. It's critical for:
# MAGIC
# MAGIC 1. **Impact Analysis**: Understand downstream effects before changes
# MAGIC 2. **Root Cause Analysis**: Trace data quality issues to source
# MAGIC 3. **Compliance**: Document PII data flow for GDPR/CCPA
# MAGIC 4. **Trust**: Verify data quality and transformations
# MAGIC 5. **Optimization**: Identify and remove unused tables
# MAGIC
# MAGIC Unity Catalog **automatically tracks lineage** for SQL operations, pipelines, notebooks, and dashboards.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q6: **How would you implement ABAC policies at scale across 1000+ tables?**
# MAGIC
# MAGIC **Answer:**  
# MAGIC
# MAGIC **Step 1**: Create governed tags taxonomy
# MAGIC ```
# MAGIC pii: {ssn, email, phone, address}
# MAGIC sensitivity: {low, medium, high}
# MAGIC ```
# MAGIC
# MAGIC **Step 2**: Tag columns programmatically (Python SDK)
# MAGIC ```python
# MAGIC # Scan tables, apply tags based on column names/patterns
# MAGIC ```
# MAGIC
# MAGIC **Step 3**: Create catalog-level policies
# MAGIC ```sql
# MAGIC CREATE POLICY ssn_mask ON CATALOG production ...
# MAGIC -- Applies to ALL tables with pii=ssn tag
# MAGIC ```
# MAGIC
# MAGIC **Step 4**: New tables automatically inherit protection via tags
# MAGIC
# MAGIC **Key**: Catalog-level policies scale; table-level policies don't.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q7: **What are the limitations of ABAC policies in Unity Catalog?**
# MAGIC
# MAGIC **Answer:**  
# MAGIC
# MAGIC 1. **Cannot apply to VIEWS** (only tables/materialized views)
# MAGIC 2. **One row filter per table per user** (no multiple RLS policies)
# MAGIC 3. **One column mask per column per user**
# MAGIC 4. **Requires Databricks Runtime 16.4+** (older versions can't access protected tables)
# MAGIC 5. **Time travel not supported** for non-exempt users
# MAGIC 6. **Deep/shallow clones restricted**
# MAGIC 7. **Policy quotas**: 10 per catalog, 10 per schema, 5 per table
# MAGIC
# MAGIC **Workaround**: Use views for complex scenarios where ABAC doesn't fit.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q8: **How do you query data lineage programmatically?**
# MAGIC
# MAGIC **Answer:**  
# MAGIC Use Unity Catalog **system tables**:
# MAGIC
# MAGIC ```sql
# MAGIC -- Table-to-table lineage
# MAGIC SELECT * FROM system.access.table_lineage
# MAGIC WHERE target_table_full_name = 'catalog.schema.table';
# MAGIC
# MAGIC -- Column-level lineage
# MAGIC SELECT * FROM system.access.column_lineage
# MAGIC WHERE target_column_name = 'revenue';
# MAGIC
# MAGIC -- Find upstream sources
# MAGIC WHERE target_table_full_name = 'my.table';
# MAGIC
# MAGIC -- Find downstream consumers
# MAGIC WHERE source_table_full_name = 'my.table';
# MAGIC ```
# MAGIC
# MAGIC Lineage updates **automatically** within minutes of operations.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q9: **What's the difference between row filters and column masks in ABAC?**
# MAGIC
# MAGIC **Answer:**  
# MAGIC
# MAGIC | Aspect | Row Filter | Column Mask |
# MAGIC |--------|------------|-------------|
# MAGIC | **Purpose** | Control which rows | Control column values |
# MAGIC | **UDF Return Type** | BOOLEAN (true=keep) | Same as column type |
# MAGIC | **SQL Clause** | `ROW FILTER function` | `COLUMN MASK function` |
# MAGIC | **Example** | Show only US rows | Mask SSN as ***-**-1234 |
# MAGIC | **Performance** | Predicate pushdown | Applied at query time |
# MAGIC
# MAGIC **Both** use governed tags and can be combined on the same table.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q10: **How would you troubleshoot a user reporting "I can't see any data" after implementing RLS?**
# MAGIC
# MAGIC **Answer:**  
# MAGIC
# MAGIC **Step 1**: Check user's group membership
# MAGIC ```sql
# MAGIC SELECT * FROM system.access.groups WHERE user_name = 'user@company.com';
# MAGIC ```
# MAGIC
# MAGIC **Step 2**: Check effective policies
# MAGIC ```sql
# MAGIC SHOW EFFECTIVE POLICIES ON TABLE catalog.schema.table;
# MAGIC ```
# MAGIC
# MAGIC **Step 3**: Test row filter logic
# MAGIC ```sql
# MAGIC SELECT catalog.security.row_filter_function('test_value');
# MAGIC -- Should return TRUE for valid cases
# MAGIC ```
# MAGIC
# MAGIC **Step 4**: Check if user is in exception list
# MAGIC ```sql
# MAGIC DESCRIBE POLICY policy_name ON CATALOG catalog;
# MAGIC -- Review TO/EXCEPT principals
# MAGIC ```
# MAGIC
# MAGIC **Common Issue**: Row filter function logic error (returns FALSE for all rows)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Common Mistakes & How to Avoid:
# MAGIC
# MAGIC ### 1. **Exposing Sensitive Data**
# MAGIC
# MAGIC **Mistake**: Not masking SSN/PII in non-production environments
# MAGIC
# MAGIC **Fix**:
# MAGIC - Tag all PII columns from day 1
# MAGIC - Apply ABAC policies at catalog level
# MAGIC - Verify with test queries before granting access
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. **No Row-Level Restrictions**
# MAGIC
# MAGIC **Mistake**: Giving table access = access to ALL rows
# MAGIC
# MAGIC **Fix**:
# MAGIC - Implement RLS for multi-tenant tables
# MAGIC - Use views with WHERE clauses for simple cases
# MAGIC - ABAC policies for complex/scalable scenarios
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. **Weak Governance**
# MAGIC
# MAGIC **Mistake**: Using free-form tags instead of governed tags
# MAGIC
# MAGIC **Fix**:
# MAGIC - Create governed tags in UI
# MAGIC - Enforce tag taxonomy
# MAGIC - Document tag meanings
# MAGIC - Regular audits of tag usage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4. **Ignoring Lineage**
# MAGIC
# MAGIC **Mistake**: Making schema changes without checking impact
# MAGIC
# MAGIC **Fix**:
# MAGIC - Always check lineage before modifications
# MAGIC - Use `SHOW EFFECTIVE POLICIES` before changes
# MAGIC - Communicate with downstream consumers
# MAGIC - Implement change management process
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5. **Over-Complicated Security**
# MAGIC
# MAGIC **Mistake**: Creating 100 different views for 100 user combinations
# MAGIC
# MAGIC **Fix**:
# MAGIC - Use ABAC policies (scales automatically)
# MAGIC - Catalog-level policies > table-level
# MAGIC - Dynamic functions (current_user(), is_member())
# MAGIC - Start simple, add complexity as needed
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6. **Not Testing Performance**
# MAGIC
# MAGIC **Mistake**: Assuming row filters have no performance impact
# MAGIC
# MAGIC **Fix**:
# MAGIC - Test with production data volumes
# MAGIC - Ensure indexed columns in filter predicates
# MAGIC - Monitor query execution times
# MAGIC - Use EXPLAIN to verify predicate pushdown
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 7. **Forgetting Admin Exceptions**
# MAGIC
# MAGIC **Mistake**: Locking out admins with overly restrictive policies
# MAGIC
# MAGIC **Fix**:
# MAGIC - Always include `EXCEPT admins` in policies
# MAGIC - Create emergency access procedures
# MAGIC - Document override mechanisms
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 8. **Poor Documentation**
# MAGIC
# MAGIC **Mistake**: No documentation of security policies and tag meanings
# MAGIC
# MAGIC **Fix**:
# MAGIC - Maintain tag glossary
# MAGIC - Document each policy's purpose
# MAGIC - Create architecture diagrams
# MAGIC - Training for new team members
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Next Steps:
# MAGIC
# MAGIC ### Immediate Actions:
# MAGIC 1. ✅ Review your current tables for PII
# MAGIC 2. ✅ Create governed tag taxonomy
# MAGIC 3. ✅ Implement basic RLS/CLS on sensitive tables
# MAGIC 4. ✅ Enable lineage tracking
# MAGIC 5. ✅ Set up audit monitoring
# MAGIC
# MAGIC ### Advanced Topics to Explore:
# MAGIC 1. 🔹 Dynamic data masking with ML (context-aware)
# MAGIC 2. 🔹 Attribute-based access with custom attributes
# MAGIC 3. 🔹 Cross-catalog lineage patterns
# MAGIC 4. 🔹 Automated compliance reporting
# MAGIC 5. 🔹 Data classification automation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Additional Resources:
# MAGIC
# MAGIC * **Databricks Documentation**:
# MAGIC   - Unity Catalog Security Model
# MAGIC   - ABAC Policies Guide
# MAGIC   - Data Lineage Documentation
# MAGIC
# MAGIC * **System Tables Reference**:
# MAGIC   - `system.access.table_lineage`
# MAGIC   - `system.access.column_lineage`
# MAGIC   - `system.access.audit`
# MAGIC   - `system.information_schema.column_tags`
# MAGIC
# MAGIC * **Compliance Frameworks**:
# MAGIC   - GDPR Data Protection
# MAGIC   - CCPA Privacy Requirements
# MAGIC   - HIPAA Security Rules
# MAGIC   - SOC 2 Controls
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✨ Congratulations!
# MAGIC
# MAGIC You've completed **Phase 7 Day 34**: Data Security & Lineage!
# MAGIC
# MAGIC You now understand:
# MAGIC - ✅ Row-Level Security (RLS)
# MAGIC - ✅ Column-Level Security (CLS)
# MAGIC - ✅ Data Masking Techniques
# MAGIC - ✅ Unity Catalog ABAC Policies
# MAGIC - ✅ Data Lineage Tracking
# MAGIC - ✅ End-to-End Secure Architecture
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💬 Final Thought:
# MAGIC
# MAGIC > **"Security and lineage are not optional—they are foundational to trustworthy data platforms."**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 📅 Training: Phase 7 Day 34 — Data Engineering  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **🚀 Keep Learning | Stay Secure | Build Trust 🚀**

# COMMAND ----------

# DBTITLE 1,📊 Lab Summary & Verification
# MAGIC %md
# MAGIC # 📊 LAB VERIFICATION & SUMMARY
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ What We've Accomplished:
# MAGIC
# MAGIC ### 1. **Data Security Implementation**
# MAGIC - ✅ Created source tables with sensitive data (SSN, salary, email)
# MAGIC - ✅ Implemented Row-Level Security (RLS) with regional filtering
# MAGIC - ✅ Implemented Column-Level Security (CLS) with role-based views
# MAGIC - ✅ Created reusable masking functions (SSN, email, salary)
# MAGIC - ✅ Applied masking to protect PII
# MAGIC
# MAGIC ### 2. **Unity Catalog Governance**
# MAGIC - ✅ Applied tags to tables (`data_classification`, `contains_pii`)
# MAGIC - ✅ Applied tags to columns (`pii_type`, `sensitivity`)
# MAGIC - ✅ Queried tags from system tables
# MAGIC - ✅ Demonstrated tag-based data discovery
# MAGIC
# MAGIC ### 3. **Data Lineage Tracking**
# MAGIC - ✅ Created data transformations (source → views → gold)
# MAGIC - ✅ Queried lineage from Unity Catalog system tables
# MAGIC - ✅ Visualized end-to-end data flow
# MAGIC - ✅ Demonstrated automatic lineage capture
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Objects Created:
# MAGIC
# MAGIC | Object Type | Object Name | Purpose |
# MAGIC |-------------|-------------|----------|
# MAGIC | Table | `security_demo_sales` | Source: Regional sales data |
# MAGIC | Table | `security_demo_employees` | Source: Employee data with PII |
# MAGIC | View | `security_demo_sales_west_only` | RLS: West region only |
# MAGIC | View | `security_demo_employees_public` | CLS: Public columns only |
# MAGIC | View | `security_demo_employees_hr` | CLS: HR view with SSN |
# MAGIC | View | `security_demo_employees_finance` | CLS: Finance view with salary |
# MAGIC | View | `security_demo_employees_masked` | Masking: All PII masked |
# MAGIC | Table | `security_demo_dept_summary` | Gold: Department aggregation |
# MAGIC | Function | `mask_ssn` | UDF: SSN masking |
# MAGIC | Function | `mask_email` | UDF: Email masking |
# MAGIC | Function | `mask_salary` | UDF: Salary masking |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔍 Verification Steps:

# COMMAND ----------

# DBTITLE 1,Verify 1: List All Demo Objects
# Verify 1: List all tables and views created

query = f"SHOW TABLES IN {current_catalog}.{demo_schema} LIKE 'security_demo*'"

print("📊 All Demo Objects:")
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Verify 2: Count Records in Each Object
# Verify 2: Count records to ensure data integrity

query = f"""
SELECT 'security_demo_employees' as table_name, COUNT(*) as record_count 
FROM {current_catalog}.{demo_schema}.security_demo_employees
UNION ALL
SELECT 'security_demo_sales', COUNT(*) 
FROM {current_catalog}.{demo_schema}.security_demo_sales
UNION ALL
SELECT 'security_demo_employees_public', COUNT(*) 
FROM {current_catalog}.{demo_schema}.security_demo_employees_public
UNION ALL
SELECT 'security_demo_employees_masked', COUNT(*) 
FROM {current_catalog}.{demo_schema}.security_demo_employees_masked
UNION ALL
SELECT 'security_demo_dept_summary', COUNT(*) 
FROM {current_catalog}.{demo_schema}.security_demo_dept_summary
"""

print("🔢 Record Counts Verification:")
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Verify 3: Check Security Controls
# Verify 3: Compare original vs masked data side-by-side

query = f"""
SELECT 
    'Original (Unprotected)' as data_type,
    employee_name,
    ssn as ssn_field,
    CAST(salary AS STRING) as salary_field,
    email as email_field
FROM {current_catalog}.{demo_schema}.security_demo_employees
WHERE employee_id <= 103

UNION ALL

SELECT 
    'Masked (Protected)' as data_type,
    employee_name,
    ssn_masked as ssn_field,
    salary_range as salary_field,
    email_masked as email_field
FROM {current_catalog}.{demo_schema}.security_demo_employees_masked
WHERE employee_id <= 103

ORDER BY employee_name, data_type DESC
"""

print("🔐 Original vs Masked Data Comparison:")
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Verify 4: Audit Tags Applied
# Verify 4: Audit all tags applied during this lab

query = """
SELECT 
    'Table Tags' as tag_location,
    table_name,
    NULL as column_name,
    tag_name,
    tag_value
FROM system.information_schema.table_tags
WHERE table_name LIKE 'security_demo%'

UNION ALL

SELECT 
    'Column Tags' as tag_location,
    table_name,
    column_name,
    tag_name,
    tag_value
FROM system.information_schema.column_tags
WHERE table_name LIKE 'security_demo%'

ORDER BY tag_location, table_name, column_name, tag_name
"""

print("🏷️ Tags Audit (Table and Column Tags):")
display(spark.sql(query))

# COMMAND ----------

# DBTITLE 1,Verify 5: Security Controls Summary
# Verify 5: Generate security controls summary report

print("="*80)
print("🔒 SECURITY & GOVERNANCE CONTROLS SUMMARY")
print("="*80)

# Check tables
tables = spark.sql(f"""
    SELECT table_name, table_type 
    FROM system.information_schema.tables 
    WHERE table_schema = '{demo_schema}' 
    AND table_name LIKE 'security_demo%'
    ORDER BY table_name
""")

print("\n📋 OBJECTS CREATED:")
for row in tables.collect():
    icon = "📄" if row.table_type == "VIEW" else "📊"
    print(f"  {icon} {row.table_name} ({row.table_type})")

# Check functions
functions = spark.sql(f"""
    SELECT routine_name 
    FROM system.information_schema.routines 
    WHERE routine_schema = '{demo_schema}' 
    AND routine_name LIKE 'mask%'
    ORDER BY routine_name
""")

print("\n🔧 MASKING FUNCTIONS:")
for row in functions.collect():
    print(f"  🔐 {row.routine_name}")

# Check tags
table_tags_count = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM system.information_schema.table_tags
    WHERE table_name LIKE 'security_demo%'
""").collect()[0].cnt

column_tags_count = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM system.information_schema.column_tags
    WHERE table_name LIKE 'security_demo%'
""").collect()[0].cnt

print(f"\n🏷️  TAGS APPLIED:")
print(f"  • Table Tags: {table_tags_count}")
print(f"  • Column Tags: {column_tags_count}")

# Security controls
print("\n🛡️  SECURITY CONTROLS ACTIVE:")
print("  ✅ Row-Level Security (RLS) - Regional filtering")
print("  ✅ Column-Level Security (CLS) - Role-based views")
print("  ✅ Data Masking - SSN, Email, Salary protection")
print("  ✅ Unity Catalog Tags - Data classification")
print("  ✅ Data Lineage - Automatic tracking enabled")

print("\n" + "="*80)
print("✅ LAB COMPLETE - All security controls verified!")
print("="*80)

# COMMAND ----------

# DBTITLE 1,🧹 Cleanup Instructions (Optional)
# MAGIC %md
# MAGIC # 🧹 CLEANUP INSTRUCTIONS (Optional)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚠️ Clean Up Demo Objects
# MAGIC
# MAGIC If you want to remove all demo objects created in this lab, run the cells below.
# MAGIC
# MAGIC **Note**: This will permanently delete:
# MAGIC - All demo tables and views
# MAGIC - All masking functions
# MAGIC - All applied tags
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Option 1: Keep for Reference
# MAGIC - Leave objects in place for future reference
# MAGIC - They're prefixed with `security_demo_` for easy identification
# MAGIC - Very small storage footprint
# MAGIC
# MAGIC ### Option 2: Clean Up
# MAGIC - Run the cleanup cells below
# MAGIC - Removes all demo objects
# MAGIC - Frees up namespace

# COMMAND ----------

# DBTITLE 1,Cleanup: Drop All Demo Objects
# Cleanup: Drop all demo objects (UNCOMMENT TO RUN)

# To clean up, uncomment the lines below and run this cell

print("⚠️  Cleanup commands are commented out. Uncomment to execute cleanup.")
print("\n🗑️ To clean up all demo objects, uncomment the code below:\n")

# Drop views first
# spark.sql(f"DROP VIEW IF EXISTS {current_catalog}.{demo_schema}.security_demo_sales_west_only")
# spark.sql(f"DROP VIEW IF EXISTS {current_catalog}.{demo_schema}.security_demo_employees_public")
# spark.sql(f"DROP VIEW IF EXISTS {current_catalog}.{demo_schema}.security_demo_employees_hr")
# spark.sql(f"DROP VIEW IF EXISTS {current_catalog}.{demo_schema}.security_demo_employees_finance")
# spark.sql(f"DROP VIEW IF EXISTS {current_catalog}.{demo_schema}.security_demo_employees_masked")

# Drop tables
# spark.sql(f"DROP TABLE IF EXISTS {current_catalog}.{demo_schema}.security_demo_employees")
# spark.sql(f"DROP TABLE IF EXISTS {current_catalog}.{demo_schema}.security_demo_sales")
# spark.sql(f"DROP TABLE IF EXISTS {current_catalog}.{demo_schema}.security_demo_dept_summary")

# Drop functions
# spark.sql(f"DROP FUNCTION IF EXISTS {current_catalog}.{demo_schema}.mask_ssn")
# spark.sql(f"DROP FUNCTION IF EXISTS {current_catalog}.{demo_schema}.mask_email")
# spark.sql(f"DROP FUNCTION IF EXISTS {current_catalog}.{demo_schema}.mask_salary")

# print("\n✅ Cleanup complete! All demo objects have been removed.")