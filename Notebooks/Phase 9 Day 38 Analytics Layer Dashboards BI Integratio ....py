# Databricks notebook source
# DBTITLE 1,Genie Code Agent for Analytics
# MAGIC %md
# MAGIC # 🤖 GENIE CODE AGENT: Analytics & BI Use Cases
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌟 What is Genie Code Agent?
# MAGIC
# MAGIC Genie Code Agent is Databricks' AI assistant that helps you write SQL, build dashboards, create analytics pipelines, and integrate BI tools through natural language prompts.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💬 Example Prompts for Analytics Layer
# MAGIC
# MAGIC ### 📊 Dashboard Creation
# MAGIC
# MAGIC ```
# MAGIC 🗨️ Prompt:
# MAGIC "Build a dashboard query showing monthly revenue by product category 
# MAGIC with year-over-year comparison"
# MAGIC
# MAGIC ✅ Genie Response:
# MAGIC - Generates SQL query with proper aggregations
# MAGIC - Includes time-based calculations
# MAGIC - Adds formatting for dashboard consumption
# MAGIC - Suggests appropriate visualizations
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 🗨️ Prompt:
# MAGIC "Create KPI cards showing total orders, revenue, and average order value 
# MAGIC for the current month with MoM growth percentage"
# MAGIC
# MAGIC ✅ Genie Response:
# MAGIC - Writes SQL with current and previous month logic
# MAGIC - Calculates growth percentages
# MAGIC - Formats currency and percentages
# MAGIC - Provides dashboard-ready output
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Semantic Layer Design
# MAGIC
# MAGIC ```
# MAGIC 🗨️ Prompt:
# MAGIC "Create a semantic view called customer_360 that combines customer 
# MAGIC demographics, order history, and lifetime value"
# MAGIC
# MAGIC ✅ Genie Response:
# MAGIC - Generates CREATE VIEW statement
# MAGIC - Joins relevant Gold tables
# MAGIC - Uses business-friendly column names
# MAGIC - Adds documentation comments
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 🗨️ Prompt:
# MAGIC "Build a monthly KPI table with pre-aggregated metrics: active customers, 
# MAGIC total orders, revenue, and avg order value by segment"
# MAGIC
# MAGIC ✅ Genie Response:
# MAGIC - Creates materialized table structure
# MAGIC - Includes proper GROUP BY logic
# MAGIC - Adds time dimensions
# MAGIC - Optimizes for dashboard queries
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔌 BI Integration
# MAGIC
# MAGIC ```
# MAGIC 🗨️ Prompt:
# MAGIC "Show me how to connect Power BI to this Databricks SQL Warehouse"
# MAGIC
# MAGIC ✅ Genie Response:
# MAGIC - Provides connection string details
# MAGIC - Explains authentication options
# MAGIC - Lists required connection parameters
# MAGIC - Suggests best practices
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 🗨️ Prompt:
# MAGIC "Create a view optimized for Tableau that shows sales performance 
# MAGIC with drill-down capabilities by region, category, and product"
# MAGIC
# MAGIC ✅ Genie Response:
# MAGIC - Builds hierarchical view structure
# MAGIC - Includes all drill-down dimensions
# MAGIC - Optimizes join performance
# MAGIC - Adds Tableau-friendly formatting
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚡ Query Optimization
# MAGIC
# MAGIC ```
# MAGIC 🗨️ Prompt:
# MAGIC "Optimize this dashboard query - it's taking too long to run"
# MAGIC [paste slow query]
# MAGIC
# MAGIC ✅ Genie Response:
# MAGIC - Analyzes query structure
# MAGIC - Identifies performance bottlenecks
# MAGIC - Suggests indexing or partitioning
# MAGIC - Recommends pre-aggregation strategies
# MAGIC - Provides optimized query
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 🗨️ Prompt:
# MAGIC "Create a materialized view to speed up this frequently-run dashboard query"
# MAGIC
# MAGIC ✅ Genie Response:
# MAGIC - Generates CREATE TABLE statement
# MAGIC - Includes proper partitioning
# MAGIC - Suggests refresh schedule
# MAGIC - Provides maintenance strategy
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Data Analysis
# MAGIC
# MAGIC ```
# MAGIC 🗨️ Prompt:
# MAGIC "Analyze customer purchase patterns - show repeat purchase rate, 
# MAGIC average time between orders, and customer segments"
# MAGIC
# MAGIC ✅ Genie Response:
# MAGIC - Writes cohort analysis SQL
# MAGIC - Calculates retention metrics
# MAGIC - Performs segmentation logic
# MAGIC - Visualizes results
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 🗨️ Prompt:
# MAGIC "Find anomalies in daily revenue - highlight days with unusual patterns"
# MAGIC
# MAGIC ✅ Genie Response:
# MAGIC - Implements statistical anomaly detection
# MAGIC - Calculates moving averages
# MAGIC - Identifies outliers
# MAGIC - Provides visualization-ready output
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛠️ Troubleshooting
# MAGIC
# MAGIC ```
# MAGIC 🗨️ Prompt:
# MAGIC "My dashboard shows no data - help me debug"
# MAGIC
# MAGIC ✅ Genie Response:
# MAGIC - Checks table existence
# MAGIC - Validates date filters
# MAGIC - Verifies Unity Catalog permissions
# MAGIC - Suggests debugging steps
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC 🗨️ Prompt:
# MAGIC "Diagnose why my SQL Warehouse queries are slow"
# MAGIC
# MAGIC ✅ Genie Response:
# MAGIC - Reviews query execution plan
# MAGIC - Analyzes warehouse configuration
# MAGIC - Checks for missing statistics
# MAGIC - Recommends optimization strategies
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Best Practices for Genie Prompts
# MAGIC
# MAGIC ### ✅ DO:
# MAGIC
# MAGIC 1. **Be Specific**: Include table names, metrics, and time periods
# MAGIC 2. **Provide Context**: Mention dashboard requirements or business goals
# MAGIC 3. **Request Optimization**: Ask for performance considerations
# MAGIC 4. **Include Examples**: Show sample data or desired output format
# MAGIC
# MAGIC ### ❌ DON'T:
# MAGIC
# MAGIC 1. **Be Vague**: "Build a dashboard" → Too broad
# MAGIC 2. **Skip Requirements**: Missing aggregation logic or filters
# MAGIC 3. **Ignore Performance**: Not mentioning data volumes
# MAGIC 4. **Forget Governance**: Skipping Unity Catalog paths
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Advanced Genie Use Cases
# MAGIC
# MAGIC ```
# MAGIC 🗨️ Complex Prompt:
# MAGIC "Create a complete analytics layer for e-commerce:
# MAGIC 1. Build semantic views for customers, products, and orders
# MAGIC 2. Create monthly KPI tables with pre-aggregations
# MAGIC 3. Generate dashboard queries for executive reporting
# MAGIC 4. Include revenue, customer, and operational metrics
# MAGIC 5. Optimize for sub-second query response times"
# MAGIC
# MAGIC ✅ Genie Response:
# MAGIC - Generates complete DDL for views and tables
# MAGIC - Includes proper naming conventions
# MAGIC - Adds documentation and comments
# MAGIC - Provides performance optimization suggestions
# MAGIC - Creates sample dashboard queries
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Learning Resources
# MAGIC
# MAGIC Use Genie to:
# MAGIC - Learn SQL best practices
# MAGIC - Understand Delta Lake optimization
# MAGIC - Explore Unity Catalog features
# MAGIC - Get BI integration guidance
# MAGIC - Debug complex queries
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Genie Code Agent accelerates your analytics development by 10x!** 🚀

# COMMAND ----------

# DBTITLE 1,Final Summary & Interview Questions
# MAGIC %md
# MAGIC # 🎯 FINAL SUMMARY: Analytics Layer Mastery
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Key Learnings Recap
# MAGIC
# MAGIC ### 1. **Analytics Layer Fundamentals**
# MAGIC - Analytics layer sits on top of Gold layer
# MAGIC - Provides business-friendly data abstraction
# MAGIC - Enables self-service analytics for business users
# MAGIC - Uses SQL Warehouses as compute engine
# MAGIC
# MAGIC ### 2. **Databricks Dashboards**
# MAGIC - Native visualization tool within Databricks
# MAGIC - Direct connection to Delta tables
# MAGIC - Supports interactive filters and parameters
# MAGIC - Scheduled refresh and sharing capabilities
# MAGIC
# MAGIC ### 3. **BI Tool Integration**
# MAGIC - Connect via JDBC/ODBC to SQL Warehouses
# MAGIC - Supports Power BI, Tableau, Qlik, Looker
# MAGIC - Uses Unity Catalog for governance
# MAGIC - Real-time query execution on Delta Lake
# MAGIC
# MAGIC ### 4. **SQL Warehouses**
# MAGIC - Serverless compute for analytics workloads
# MAGIC - Auto-scaling and auto-suspend for cost optimization
# MAGIC - Photon engine for 2-10x performance boost
# MAGIC - Query result caching and optimization
# MAGIC
# MAGIC ### 5. **Semantic Layer Design**
# MAGIC - Business-friendly views and metric tables
# MAGIC - Pre-aggregated KPIs for performance
# MAGIC - Consistent metric definitions
# MAGIC - Abstraction from technical complexity
# MAGIC
# MAGIC ### 6. **End-to-End Architecture**
# MAGIC - Bronze → Silver → Gold → Semantic → Consumption
# MAGIC - Unity Catalog governance throughout
# MAGIC - SQL Warehouse for query execution
# MAGIC - Multiple consumption patterns (dashboards, BI tools)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❗ Key Takeaways
# MAGIC
# MAGIC ✅ **Always consume from Gold layer or semantic layer**  
# MAGIC ✅ **Pre-aggregate data for dashboard performance**  
# MAGIC ✅ **Use business-friendly naming in semantic layer**  
# MAGIC ✅ **Leverage serverless SQL Warehouses for cost efficiency**  
# MAGIC ✅ **Design dashboards for clarity and actionability**  
# MAGIC ✅ **Apply Unity Catalog governance for security**  
# MAGIC ✅ **Optimize queries with materialized views and caching**  
# MAGIC ✅ **Choose the right chart type for each metric**  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 INTERVIEW QUESTIONS (Must Know)
# MAGIC
# MAGIC ### Question 1: Analytics Layer Design
# MAGIC **Q: How would you design an analytics layer for a large e-commerce platform with millions of orders?**
# MAGIC
# MAGIC **Expected Answer:**
# MAGIC - Start with Gold layer as the foundation (curated, governed data)
# MAGIC - Create semantic layer with business views:
# MAGIC   * customer_360 (combined customer metrics)
# MAGIC   * product_performance (product analytics)
# MAGIC   * order_metrics (transaction analytics)
# MAGIC - Build pre-aggregated KPI tables:
# MAGIC   * daily_kpis (most recent data)
# MAGIC   * monthly_kpis (trend analysis)
# MAGIC   * customer_segments (cohort analysis)
# MAGIC - Use materialized tables for frequent queries
# MAGIC - Partition by date for performance
# MAGIC - Apply Unity Catalog governance
# MAGIC - Use Serverless SQL Warehouse for queries
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Question 2: Dashboard Performance
# MAGIC **Q: A dashboard is taking 30 seconds to load. How do you optimize it?**
# MAGIC
# MAGIC **Expected Answer:**
# MAGIC 1. **Analyze Query**:
# MAGIC    - Check execution plan
# MAGIC    - Identify expensive operations (joins, aggregations)
# MAGIC    - Look for full table scans
# MAGIC
# MAGIC 2. **Pre-Aggregate Data**:
# MAGIC    - Create materialized view with pre-calculated metrics
# MAGIC    - Partition by relevant dimensions (date, category)
# MAGIC    - Schedule refresh (hourly/daily)
# MAGIC
# MAGIC 3. **Optimize Warehouse**:
# MAGIC    - Ensure Photon is enabled
# MAGIC    - Check warehouse size
# MAGIC    - Enable query result caching
# MAGIC
# MAGIC 4. **Reduce Data Scanned**:
# MAGIC    - Apply proper date filters
# MAGIC    - Use Z-ordering on filter columns
# MAGIC    - OPTIMIZE and VACUUM tables regularly
# MAGIC
# MAGIC 5. **Dashboard Design**:
# MAGIC    - Limit number of visualizations
# MAGIC    - Use appropriate time ranges
# MAGIC    - Implement incremental refresh
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Question 3: BI Tool Integration
# MAGIC **Q: Explain how to connect Tableau to Databricks and best practices.**
# MAGIC
# MAGIC **Expected Answer:**
# MAGIC
# MAGIC **Connection Steps:**
# MAGIC 1. Install Databricks JDBC driver in Tableau
# MAGIC 2. Get SQL Warehouse connection details:
# MAGIC    - Server hostname: `<workspace>.cloud.databricks.com`
# MAGIC    - HTTP Path: `/sql/1.0/warehouses/<warehouse-id>`
# MAGIC    - Personal Access Token for authentication
# MAGIC 3. Configure Tableau data source
# MAGIC 4. Select Unity Catalog, schema, and tables
# MAGIC
# MAGIC **Best Practices:**
# MAGIC - Use dedicated SQL Warehouse for BI workloads
# MAGIC - Consume from semantic layer (not Gold directly)
# MAGIC - Use DirectQuery for real-time data
# MAGIC - Use Extract for large historical analysis
# MAGIC - Apply row-level security in Unity Catalog
# MAGIC - Create optimized views for Tableau
# MAGIC - Monitor query performance
# MAGIC - Document data sources
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Question 4: Semantic Layer vs Gold Layer
# MAGIC **Q: What's the difference between Gold layer and Semantic layer?**
# MAGIC
# MAGIC **Expected Answer:**
# MAGIC
# MAGIC | **Aspect** | **Gold Layer** | **Semantic Layer** |
# MAGIC |------------|---------------|-------------------|
# MAGIC | **Purpose** | Curated, business-ready data | Business-friendly abstraction |
# MAGIC | **Schema** | Dimensional models (facts, dims) | Business views and metrics |
# MAGIC | **Naming** | Technical (fact_sales, dim_customer) | Business (customer_metrics, sales_dashboard) |
# MAGIC | **Aggregation** | Detailed transactions | Pre-aggregated KPIs |
# MAGIC | **Users** | Data analysts, data scientists | Business users, executives |
# MAGIC | **Complexity** | May require SQL expertise | Simple, intuitive queries |
# MAGIC | **Example** | `gold.fact_orders` | `analytics.monthly_revenue_kpis` |
# MAGIC
# MAGIC **Key Point**: Semantic layer sits on top of Gold layer and provides simplified, business-oriented access.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Question 5: SQL Warehouse Sizing
# MAGIC **Q: How do you choose the right SQL Warehouse size?**
# MAGIC
# MAGIC **Expected Answer:**
# MAGIC
# MAGIC **Factors to Consider:**
# MAGIC 1. **Concurrency**: Number of simultaneous users
# MAGIC 2. **Query Complexity**: Aggregations, joins, data volume
# MAGIC 3. **Response Time SLA**: Sub-second vs minutes
# MAGIC 4. **Budget**: DBU costs
# MAGIC
# MAGIC **Sizing Guidelines:**
# MAGIC - **X-Small**: Development, testing, < 5 users
# MAGIC - **Small**: Small team (5-10 users), simple queries
# MAGIC - **Medium**: Production (10-50 users), moderate complexity
# MAGIC - **Large**: Enterprise (50-200 users), complex queries
# MAGIC - **X-Large**: Very large enterprise (200+ users)
# MAGIC
# MAGIC **Recommendations:**
# MAGIC - Start small and monitor performance
# MAGIC - Enable auto-scaling (min/max clusters)
# MAGIC - Use query history to analyze patterns
# MAGIC - Set appropriate auto-stop time (10-15 min)
# MAGIC - Monitor DBU consumption
# MAGIC - Use Serverless for variable workloads
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Question 6: Dashboard Design Principles
# MAGIC **Q: What are the top 5 dashboard design principles?**
# MAGIC
# MAGIC **Expected Answer:**
# MAGIC
# MAGIC 1. **Simplicity**: 5-7 key metrics, avoid clutter
# MAGIC 2. **Visual Hierarchy**: Most important metrics at top-left
# MAGIC 3. **Right Chart Types**: 
# MAGIC    - Trends → Line charts
# MAGIC    - Comparisons → Bar charts
# MAGIC    - Proportions → Pie charts
# MAGIC    - KPIs → Number cards
# MAGIC 4. **Actionable Insights**: Focus on metrics that drive decisions
# MAGIC 5. **Performance**: Sub-3-second load time, pre-aggregated data
# MAGIC
# MAGIC **Additional:**
# MAGIC - Consistent color scheme
# MAGIC - Clear labels and tooltips
# MAGIC - Date range visibility
# MAGIC - Mobile-responsive design
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Question 7: Unity Catalog in Analytics
# MAGIC **Q: How does Unity Catalog enhance analytics security and governance?**
# MAGIC
# MAGIC **Expected Answer:**
# MAGIC
# MAGIC **Security Features:**
# MAGIC 1. **Fine-Grained Access Control**:
# MAGIC    - Table-level: GRANT SELECT ON TABLE
# MAGIC    - Column-level: Specific columns only
# MAGIC    - Row-level: Dynamic filtering based on user
# MAGIC
# MAGIC 2. **Data Masking**:
# MAGIC    - PII protection (email, SSN)
# MAGIC    - Dynamic masking based on user role
# MAGIC
# MAGIC 3. **Audit Logging**:
# MAGIC    - Track all data access
# MAGIC    - Query history by user
# MAGIC    - Compliance reporting
# MAGIC
# MAGIC 4. **Data Lineage**:
# MAGIC    - Track data flow from source to dashboard
# MAGIC    - Impact analysis for changes
# MAGIC
# MAGIC **Implementation:**
# MAGIC ```sql
# MAGIC -- Grant dashboard access
# MAGIC GRANT SELECT ON TABLE analytics.business.sales_dashboard 
# MAGIC TO group_business_users;
# MAGIC
# MAGIC -- Row-level security
# MAGIC CREATE ROW FILTER regional_filter 
# MAGIC ON analytics.business.sales_dashboard
# MAGIC FOR SELECT 
# MAGIC RETURN region = current_user_region();
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Question 8: Real-Time Dashboards
# MAGIC **Q: How would you build a real-time operational dashboard?**
# MAGIC
# MAGIC **Expected Answer:**
# MAGIC
# MAGIC **Architecture:**
# MAGIC 1. **Streaming Ingestion**:
# MAGIC    - Kafka/Kinesis → Auto Loader → Bronze
# MAGIC    - Structured Streaming pipeline
# MAGIC
# MAGIC 2. **Incremental Processing**:
# MAGIC    - Bronze → Silver → Gold (streaming)
# MAGIC    - Delta Live Tables for continuous updates
# MAGIC
# MAGIC 3. **Materialized Views**:
# MAGIC    - Aggregate to 1-minute windows
# MAGIC    - Store in Gold layer
# MAGIC
# MAGIC 4. **SQL Warehouse**:
# MAGIC    - Serverless for elastic scaling
# MAGIC    - Short auto-suspend time (5 min)
# MAGIC
# MAGIC 5. **Dashboard**:
# MAGIC    - Auto-refresh every 30-60 seconds
# MAGIC    - Show latest 1-hour window
# MAGIC    - Alert on anomalies
# MAGIC
# MAGIC **Example Use Case**: Real-time order monitoring dashboard for operations team
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Question 9: Cost Optimization
# MAGIC **Q: Strategies to reduce analytics layer costs?**
# MAGIC
# MAGIC **Expected Answer:**
# MAGIC
# MAGIC 1. **Compute Optimization**:
# MAGIC    - Use Serverless SQL Warehouses
# MAGIC    - Enable auto-stop (10-15 min)
# MAGIC    - Right-size warehouse
# MAGIC    - Leverage query result caching
# MAGIC
# MAGIC 2. **Storage Optimization**:
# MAGIC    - Run OPTIMIZE on Delta tables
# MAGIC    - VACUUM old versions
# MAGIC    - Use appropriate partitioning
# MAGIC    - Archive old data to cold storage
# MAGIC
# MAGIC 3. **Query Optimization**:
# MAGIC    - Pre-aggregate common queries
# MAGIC    - Create materialized views
# MAGIC    - Use Z-ordering for filters
# MAGIC    - Optimize join strategies
# MAGIC
# MAGIC 4. **Dashboard Optimization**:
# MAGIC    - Scheduled refresh vs on-demand
# MAGIC    - Limit historical data range
# MAGIC    - Reduce visualization count
# MAGIC
# MAGIC 5. **Monitoring**:
# MAGIC    - Track DBU consumption by query/user
# MAGIC    - Identify expensive queries
# MAGIC    - Set query timeout limits
# MAGIC
# MAGIC **Expected Savings**: 40-60% cost reduction
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Question 10: Handling Large Datasets
# MAGIC **Q: How do you design dashboards for datasets with billions of rows?**
# MAGIC
# MAGIC **Expected Answer:**
# MAGIC
# MAGIC **Strategies:**
# MAGIC
# MAGIC 1. **Pre-Aggregation**:
# MAGIC    - Create summary tables (daily, hourly)
# MAGIC    - Aggregate by key dimensions
# MAGIC    - Materialized views with scheduled refresh
# MAGIC
# MAGIC 2. **Partitioning**:
# MAGIC    - Partition by date (most common filter)
# MAGIC    - Limit dashboard to recent data (30-90 days)
# MAGIC    - Provide archive query for historical analysis
# MAGIC
# MAGIC 3. **Incremental Processing**:
# MAGIC    - Only process new/changed data
# MAGIC    - Use Delta's MERGE for upserts
# MAGIC    - Streaming aggregations
# MAGIC
# MAGIC 4. **Query Pushdown**:
# MAGIC    - Apply filters early (date, category)
# MAGIC    - Use partition pruning
# MAGIC    - Limit result set size
# MAGIC
# MAGIC 5. **Caching Strategy**:
# MAGIC    - Enable query result caching
# MAGIC    - Pre-warm cache for common queries
# MAGIC    - Separate cache for different user groups
# MAGIC
# MAGIC **Example**:
# MAGIC ```sql
# MAGIC -- Instead of querying billion-row table:
# MAGIC SELECT * FROM fact_clicks WHERE date >= '2026-01-01'
# MAGIC
# MAGIC -- Query pre-aggregated table:
# MAGIC SELECT * FROM daily_click_summary WHERE date >= '2026-01-01'
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ COMMON MISTAKES (Avoid These!)
# MAGIC
# MAGIC ### 1. **Using Raw Data for Dashboards**
# MAGIC ❌ **Wrong**: Query Bronze or Silver layers directly  
# MAGIC ✅ **Right**: Consume from Gold layer or semantic layer
# MAGIC
# MAGIC ### 2. **Poor KPI Design**
# MAGIC ❌ **Wrong**: Vague metrics like "total count"  
# MAGIC ✅ **Right**: Specific, actionable KPIs like "Active Customers (MoM Growth)"
# MAGIC
# MAGIC ### 3. **Unoptimized Queries**
# MAGIC ❌ **Wrong**: Full table scans, no filters  
# MAGIC ✅ **Right**: Proper filters, partitioning, pre-aggregation
# MAGIC
# MAGIC ### 4. **No Semantic Layer**
# MAGIC ❌ **Wrong**: Business users write complex SQL every time  
# MAGIC ✅ **Right**: Create business views with simple names
# MAGIC
# MAGIC ### 5. **Ignoring Performance**
# MAGIC ❌ **Wrong**: 30-second dashboard load times  
# MAGIC ✅ **Right**: Sub-3-second response with materialized views
# MAGIC
# MAGIC ### 6. **Wrong Chart Types**
# MAGIC ❌ **Wrong**: Pie chart with 20 slices  
# MAGIC ✅ **Right**: Bar chart showing top 10
# MAGIC
# MAGIC ### 7. **Dashboard Clutter**
# MAGIC ❌ **Wrong**: 25 charts on one page  
# MAGIC ✅ **Right**: 5-7 focused visualizations
# MAGIC
# MAGIC ### 8. **No Governance**
# MAGIC ❌ **Wrong**: Open access to all data  
# MAGIC ✅ **Right**: Unity Catalog with row-level security
# MAGIC
# MAGIC ### 9. **Wrong Compute**
# MAGIC ❌ **Wrong**: Using clusters for BI workloads  
# MAGIC ✅ **Right**: SQL Warehouses for analytics
# MAGIC
# MAGIC ### 10. **Lack of Documentation**
# MAGIC ❌ **Wrong**: Metrics with unclear definitions  
# MAGIC ✅ **Right**: Documented metric definitions and business logic
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎓 Certification Readiness
# MAGIC
# MAGIC You're now ready to:
# MAGIC - Design scalable analytics layers
# MAGIC - Build production dashboards
# MAGIC - Integrate BI tools with Databricks
# MAGIC - Optimize query performance
# MAGIC - Apply governance and security
# MAGIC - Lead analytics engineering projects
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Next Steps
# MAGIC
# MAGIC 1. **Practice**: Build dashboards on sample datasets
# MAGIC 2. **Explore**: Connect BI tools (Power BI, Tableau)
# MAGIC 3. **Optimize**: Tune query performance
# MAGIC 4. **Advanced**: Real-time streaming dashboards
# MAGIC 5. **Certify**: Databricks Data Analyst certification
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔗 Additional Resources
# MAGIC
# MAGIC - Databricks SQL Warehouses Documentation
# MAGIC - Lakeview Dashboards Guide
# MAGIC - Unity Catalog Security Best Practices
# MAGIC - BI Tool Integration Guides
# MAGIC - Query Performance Tuning
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏆 Congratulations!
# MAGIC
# MAGIC **You've completed Phase 9 Day 38: Analytics Layer & BI Integration**
# MAGIC
# MAGIC You now have the expertise to build enterprise-grade analytics solutions on Databricks!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏷️ Created by: **TRRaveendra** | **@TRRaveendra**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Keep learning, keep building!** 🚀📊🏛️

# COMMAND ----------

# DBTITLE 1,Section 9 - Real-World BI Scenario
# MAGIC %md
# MAGIC # 🌟 SECTION 9: Real-World BI Scenario
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Use Case: E-Commerce Executive Dashboard
# MAGIC
# MAGIC ### 💼 Business Context
# MAGIC
# MAGIC **Stakeholder**: Chief Revenue Officer (CRO)  
# MAGIC **Requirement**: "I need a dashboard that shows me our business health at a glance"
# MAGIC
# MAGIC ### 📝 Specific Requirements:
# MAGIC
# MAGIC 1. **Revenue Metrics**
# MAGIC    - Total revenue (current month)
# MAGIC    - Revenue growth (MoM, YoY)
# MAGIC    - Revenue by product category
# MAGIC
# MAGIC 2. **Customer Metrics**
# MAGIC    - Active customers
# MAGIC    - New vs returning customers
# MAGIC    - Customer lifetime value
# MAGIC
# MAGIC 3. **Operational Metrics**
# MAGIC    - Average order value
# MAGIC    - Order volume
# MAGIC    - Conversion rate
# MAGIC
# MAGIC 4. **Trends**
# MAGIC    - 12-month revenue trend
# MAGIC    - Category performance comparison
# MAGIC    - Geographic distribution
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Solution Architecture
# MAGIC
# MAGIC ```
# MAGIC Step 1: Create Gold Layer Tables
# MAGIC   • gold_orders (fact table)
# MAGIC   • gold_customers (dimension table)
# MAGIC   • gold_products (dimension table)
# MAGIC
# MAGIC Step 2: Build Semantic Layer
# MAGIC   • exec_dashboard_metrics (aggregated KPIs)
# MAGIC   • revenue_trends (time-series data)
# MAGIC   • customer_segments (cohort analysis)
# MAGIC
# MAGIC Step 3: Create Dashboard
# MAGIC   • KPI cards at top
# MAGIC   • Revenue trend line chart
# MAGIC   • Category breakdown bar chart
# MAGIC   • Regional map visualization
# MAGIC   • Detailed metrics table
# MAGIC
# MAGIC Step 4: Configure Refresh
# MAGIC   • Scheduled: Every 6 hours
# MAGIC   • Email alerts on significant changes
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Dashboard Design
# MAGIC
# MAGIC ### Layout:
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────────────────┐
# MAGIC │  📊 EXECUTIVE DASHBOARD - April 2026                │
# MAGIC │  Last Updated: 2026-04-21 23:15                       │
# MAGIC ├────────────────────────────────────────────────────────┤
# MAGIC │                                                        │
# MAGIC │  ┌──────────────┐  ┌──────────────┐  ┌─────────────┐  │
# MAGIC │  │ Total Revenue  │  │ Active Customers│  │ Avg Order    │  │
# MAGIC │  │  $2.5M         │  │    12,450       │  │  $201.00     │  │
# MAGIC │  │  ↑ +15.2%      │  │    ↑ +8.1%      │  │  ↑ +3.5%     │  │
# MAGIC │  └──────────────┘  └──────────────┘  └─────────────┘  │
# MAGIC │                                                        │
# MAGIC │  ┌────────────────────────────────────────────────┐  │
# MAGIC │  │  📈 REVENUE TREND (12 Months)                   │  │
# MAGIC │  │  [Line Chart: Monthly Revenue Growth]          │  │
# MAGIC │  └────────────────────────────────────────────────┘  │
# MAGIC │                                                        │
# MAGIC │  ┌───────────────────────┐  ┌───────────────────────┐  │
# MAGIC │  │  📊 Revenue by Category  │  │  🌎 Regional Split   │  │
# MAGIC │  │  [Bar Chart]            │  │  [Pie Chart]          │  │
# MAGIC │  └───────────────────────┘  └───────────────────────┘  │
# MAGIC │                                                        │
# MAGIC │  ┌────────────────────────────────────────────────┐  │
# MAGIC │  │  📊 TOP PERFORMING PRODUCTS                    │  │
# MAGIC │  │  [Table with Drill-Down]                       │  │
# MAGIC │  └────────────────────────────────────────────────┘  │
# MAGIC └────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Real-World Scenario: Sales Dashboard
# MAGIC
# MAGIC ### Business Questions Answered:
# MAGIC
# MAGIC 1. **"How are we performing this month?"**
# MAGIC    → KPI cards show current metrics with MoM comparison
# MAGIC
# MAGIC 2. **"Which categories drive revenue?"**
# MAGIC    → Bar chart shows revenue by category
# MAGIC
# MAGIC 3. **"Are we growing?"**
# MAGIC    → Line chart shows 12-month trend
# MAGIC
# MAGIC 4. **"Where are our customers?"**
# MAGIC    → Regional breakdown pie/map chart
# MAGIC
# MAGIC 5. **"What are our top products?"**
# MAGIC    → Table shows detailed product performance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Implementation Steps
# MAGIC
# MAGIC ### Phase 1: Data Preparation (Week 1)
# MAGIC - Create Gold layer tables
# MAGIC - Validate data quality
# MAGIC - Set up Unity Catalog governance
# MAGIC
# MAGIC ### Phase 2: Semantic Layer (Week 2)
# MAGIC - Build aggregated KPI tables
# MAGIC - Create business views
# MAGIC - Document metric definitions
# MAGIC
# MAGIC ### Phase 3: Dashboard Build (Week 3)
# MAGIC - Design dashboard layout
# MAGIC - Create SQL queries
# MAGIC - Build visualizations
# MAGIC - Configure filters
# MAGIC
# MAGIC ### Phase 4: Testing & Launch (Week 4)
# MAGIC - User acceptance testing
# MAGIC - Performance optimization
# MAGIC - Training stakeholders
# MAGIC - Production deployment
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Let's build the actual query!** 🚀

# COMMAND ----------

# DBTITLE 1,Section 9 - Real-World Dashboard Query
# MAGIC %sql
# MAGIC -- 🌟 REAL-WORLD SCENARIO: Executive Sales Dashboard Query
# MAGIC -- This query powers a production executive dashboard
# MAGIC
# MAGIC -- Sample data representing a real e-commerce business
# MAGIC WITH sales_data AS (
# MAGIC     -- April 2026 data
# MAGIC     SELECT 'Electronics' AS category, 'North America' AS region, DATE '2026-04-01' AS sale_date, 1500.00 AS amount, 'CUST001' AS customer_id
# MAGIC     UNION ALL SELECT 'Electronics', 'North America', DATE '2026-04-02', 2300.00, 'CUST002'
# MAGIC     UNION ALL SELECT 'Clothing', 'Europe', DATE '2026-04-03', 450.00, 'CUST003'
# MAGIC     UNION ALL SELECT 'Home & Garden', 'Asia', DATE '2026-04-04', 890.00, 'CUST004'
# MAGIC     UNION ALL SELECT 'Electronics', 'North America', DATE '2026-04-05', 3200.00, 'CUST005'
# MAGIC     UNION ALL SELECT 'Sports', 'Europe', DATE '2026-04-06', 560.00, 'CUST001'
# MAGIC     UNION ALL SELECT 'Clothing', 'North America', DATE '2026-04-07', 280.00, 'CUST006'
# MAGIC     UNION ALL SELECT 'Electronics', 'Asia', DATE '2026-04-08', 1800.00, 'CUST007'
# MAGIC     UNION ALL SELECT 'Home & Garden', 'Europe', DATE '2026-04-09', 670.00, 'CUST003'
# MAGIC     UNION ALL SELECT 'Sports', 'North America', DATE '2026-04-10', 420.00, 'CUST008'
# MAGIC     
# MAGIC     -- March 2026 data (for comparison)
# MAGIC     UNION ALL SELECT 'Electronics', 'North America', DATE '2026-03-15', 1200.00, 'CUST001'
# MAGIC     UNION ALL SELECT 'Clothing', 'Europe', DATE '2026-03-16', 380.00, 'CUST002'
# MAGIC     UNION ALL SELECT 'Electronics', 'Asia', DATE '2026-03-17', 2100.00, 'CUST003'
# MAGIC     UNION ALL SELECT 'Home & Garden', 'North America', DATE '2026-03-18', 750.00, 'CUST004'
# MAGIC     UNION ALL SELECT 'Sports', 'Europe', DATE '2026-03-19', 490.00, 'CUST005'
# MAGIC ),
# MAGIC
# MAGIC -- Calculate current month metrics
# MAGIC current_month_metrics AS (
# MAGIC     SELECT 
# MAGIC         DATE_TRUNC('month', sale_date) AS month,
# MAGIC         COUNT(DISTINCT customer_id) AS active_customers,
# MAGIC         COUNT(*) AS total_orders,
# MAGIC         SUM(amount) AS total_revenue,
# MAGIC         AVG(amount) AS avg_order_value
# MAGIC     FROM sales_data
# MAGIC     WHERE sale_date >= DATE_TRUNC('month', CURRENT_DATE)
# MAGIC     GROUP BY DATE_TRUNC('month', sale_date)
# MAGIC ),
# MAGIC
# MAGIC -- Calculate previous month metrics
# MAGIC previous_month_metrics AS (
# MAGIC     SELECT 
# MAGIC         DATE_TRUNC('month', sale_date) AS month,
# MAGIC         COUNT(DISTINCT customer_id) AS active_customers,
# MAGIC         COUNT(*) AS total_orders,
# MAGIC         SUM(amount) AS total_revenue,
# MAGIC         AVG(amount) AS avg_order_value
# MAGIC     FROM sales_data
# MAGIC     WHERE sale_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL 1 MONTH
# MAGIC       AND sale_date < DATE_TRUNC('month', CURRENT_DATE)
# MAGIC     GROUP BY DATE_TRUNC('month', sale_date)
# MAGIC ),
# MAGIC
# MAGIC -- Category performance
# MAGIC category_performance AS (
# MAGIC     SELECT 
# MAGIC         category,
# MAGIC         COUNT(*) AS orders,
# MAGIC         SUM(amount) AS revenue,
# MAGIC         ROUND(AVG(amount), 2) AS avg_order_value
# MAGIC     FROM sales_data
# MAGIC     WHERE sale_date >= DATE_TRUNC('month', CURRENT_DATE)
# MAGIC     GROUP BY category
# MAGIC ),
# MAGIC
# MAGIC -- Regional breakdown
# MAGIC regional_breakdown AS (
# MAGIC     SELECT 
# MAGIC         region,
# MAGIC         COUNT(*) AS orders,
# MAGIC         SUM(amount) AS revenue,
# MAGIC         ROUND(SUM(amount) * 100.0 / SUM(SUM(amount)) OVER (), 2) AS revenue_percentage
# MAGIC     FROM sales_data
# MAGIC     WHERE sale_date >= DATE_TRUNC('month', CURRENT_DATE)
# MAGIC     GROUP BY region
# MAGIC )
# MAGIC
# MAGIC -- EXECUTIVE DASHBOARD OUTPUT
# MAGIC SELECT 
# MAGIC     '=== KEY PERFORMANCE INDICATORS ===' AS section,
# MAGIC     NULL AS metric,
# MAGIC     NULL AS current_value,
# MAGIC     NULL AS previous_value,
# MAGIC     NULL AS growth_rate
# MAGIC     
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'KPI',
# MAGIC     'Total Revenue',
# MAGIC     CONCAT('$', FORMAT_NUMBER(curr.total_revenue, 2)),
# MAGIC     CONCAT('$', FORMAT_NUMBER(prev.total_revenue, 2)),
# MAGIC     CONCAT(ROUND((curr.total_revenue - prev.total_revenue) * 100.0 / prev.total_revenue, 1), '%')
# MAGIC FROM current_month_metrics curr, previous_month_metrics prev
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'KPI',
# MAGIC     'Active Customers',
# MAGIC     CAST(curr.active_customers AS STRING),
# MAGIC     CAST(prev.active_customers AS STRING),
# MAGIC     CONCAT(ROUND((curr.active_customers - prev.active_customers) * 100.0 / prev.active_customers, 1), '%')
# MAGIC FROM current_month_metrics curr, previous_month_metrics prev
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'KPI',
# MAGIC     'Average Order Value',
# MAGIC     CONCAT('$', FORMAT_NUMBER(curr.avg_order_value, 2)),
# MAGIC     CONCAT('$', FORMAT_NUMBER(prev.avg_order_value, 2)),
# MAGIC     CONCAT(ROUND((curr.avg_order_value - prev.avg_order_value) * 100.0 / prev.avg_order_value, 1), '%')
# MAGIC FROM current_month_metrics curr, previous_month_metrics prev
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     '=== CATEGORY PERFORMANCE ===' AS section,
# MAGIC     NULL, NULL, NULL, NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Category',
# MAGIC     category AS metric,
# MAGIC     CONCAT('$', FORMAT_NUMBER(revenue, 2)) AS current_value,
# MAGIC     CAST(orders AS STRING) AS previous_value,
# MAGIC     CONCAT('$', FORMAT_NUMBER(avg_order_value, 2)) AS growth_rate
# MAGIC FROM category_performance
# MAGIC ORDER BY 
# MAGIC     CASE 
# MAGIC         WHEN section LIKE '===%' THEN 0
# MAGIC         WHEN section = 'KPI' THEN 1
# MAGIC         ELSE 2
# MAGIC     END,
# MAGIC     CAST(REGEXP_REPLACE(COALESCE(current_value, '0'), '[^0-9.]', '') AS DECIMAL(18,2)) DESC;
# MAGIC
# MAGIC -- 🎯 This query provides:
# MAGIC -- 1. Key KPIs with month-over-month comparison
# MAGIC -- 2. Category breakdown for bar charts
# MAGIC -- 3. Growth rates for trend analysis
# MAGIC
# MAGIC -- 📊 Dashboard Visualizations:
# MAGIC -- • KPI Cards: Total Revenue, Active Customers, Avg Order Value
# MAGIC -- • Bar Chart: Revenue by Category
# MAGIC -- • Line Chart: Monthly trends (add time dimension)
# MAGIC -- • Pie Chart: Regional distribution
# MAGIC
# MAGIC -- 🚀 Real Implementation:
# MAGIC -- CREATE OR REPLACE TABLE analytics.dashboards.exec_sales_dashboard AS
# MAGIC -- SELECT ... FROM main.gold.sales ...

# COMMAND ----------

# DBTITLE 1,Section 7 - Dashboard Design Best Practices
# MAGIC %md
# MAGIC # 🎨 SECTION 7: Dashboard Design Best Practices
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Golden Rules of Dashboard Design
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC Imagine your dashboard is like a movie poster. It should show the most important things right away, be easy to read, and make people want to know more. Too much stuff makes it confusing. Keep it simple and clear!
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC Effective dashboard design balances **information density** with **cognitive load**. The goal is to surface actionable insights quickly while maintaining visual hierarchy and minimizing decision fatigue.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ DO: Best Practices
# MAGIC
# MAGIC ### 1. **Keep It Simple**
# MAGIC - Limit to 5-7 key metrics per dashboard
# MAGIC - Use white space effectively
# MAGIC - Avoid clutter and unnecessary decorations
# MAGIC
# MAGIC ### 2. **Use Meaningful KPIs**
# MAGIC - Focus on actionable metrics
# MAGIC - Align with business objectives
# MAGIC - Provide context (targets, benchmarks, trends)
# MAGIC
# MAGIC ### 3. **Visual Hierarchy**
# MAGIC - Most important metrics at top-left
# MAGIC - Use size to indicate importance
# MAGIC - Group related metrics together
# MAGIC
# MAGIC ### 4. **Choose Right Chart Types**
# MAGIC
# MAGIC | **Data Type** | **Best Chart** | **Use Case** |
# MAGIC |---------------|---------------|-------------|
# MAGIC | Trends over time | Line chart | Revenue growth |
# MAGIC | Comparisons | Bar chart | Sales by region |
# MAGIC | Proportions | Pie chart | Market share |
# MAGIC | Relationships | Scatter plot | Price vs demand |
# MAGIC | Distributions | Histogram | Customer age |
# MAGIC | KPIs | Number cards | Current metrics |
# MAGIC
# MAGIC ### 5. **Consistent Color Scheme**
# MAGIC - Use brand colors
# MAGIC - Red = negative, Green = positive
# MAGIC - Consistent across all dashboards
# MAGIC
# MAGIC ### 6. **Add Context**
# MAGIC - Include time period labels
# MAGIC - Show targets and goals
# MAGIC - Add comparative metrics (YoY, MoM)
# MAGIC
# MAGIC ### 7. **Enable Interactivity**
# MAGIC - Add filters (date range, category)
# MAGIC - Drill-down capabilities
# MAGIC - Hover tooltips for details
# MAGIC
# MAGIC ### 8. **Optimize Performance**
# MAGIC - Pre-aggregate data
# MAGIC - Use materialized views
# MAGIC - Set appropriate refresh intervals
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ DON'T: Common Mistakes
# MAGIC
# MAGIC ### 1. **Too Much Information**
# MAGIC ```
# MAGIC ❌ Bad: 20 charts on one dashboard
# MAGIC ✅ Good: 5-7 focused charts
# MAGIC ```
# MAGIC
# MAGIC ### 2. **Poor Chart Selection**
# MAGIC ```
# MAGIC ❌ Bad: Pie chart with 15 slices
# MAGIC ✅ Good: Bar chart showing top 10
# MAGIC ```
# MAGIC
# MAGIC ### 3. **No Clear Story**
# MAGIC ```
# MAGIC ❌ Bad: Random metrics with no connection
# MAGIC ✅ Good: Metrics that tell a business story
# MAGIC ```
# MAGIC
# MAGIC ### 4. **Ignoring Mobile Users**
# MAGIC ```
# MAGIC ❌ Bad: Dashboard only works on large screens
# MAGIC ✅ Good: Responsive design for all devices
# MAGIC ```
# MAGIC
# MAGIC ### 5. **Stale Data**
# MAGIC ```
# MAGIC ❌ Bad: Last updated 3 weeks ago
# MAGIC ✅ Good: Real-time or clearly labeled refresh time
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Dashboard Layout Template
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────────┐
# MAGIC │  📊 DASHBOARD TITLE                        │
# MAGIC │  Last Updated: 2026-04-21 23:15          │
# MAGIC ├────────────────────────────────────────────────┤
# MAGIC │  🎯 Filters: [Date] [Region] [Segment]   │
# MAGIC ├────────────────────────────────────────────────┤
# MAGIC │                                                │
# MAGIC │  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
# MAGIC │  │   KPI 1   │  │   KPI 2   │  │   KPI 3   │  │
# MAGIC │  │  $125K   │  │   1,234   │  │   +15%   │  │
# MAGIC │  └──────────┘  └──────────┘  └──────────┘  │
# MAGIC │                                                │
# MAGIC │  ┌──────────────────────────────────────────┐  │
# MAGIC │  │  📈 MAIN TREND CHART                    │  │
# MAGIC │  │  (Line Chart - Revenue Over Time)      │  │
# MAGIC │  └──────────────────────────────────────────┘  │
# MAGIC │                                                │
# MAGIC │  ┌────────────────────┐  ┌────────────────────┐  │
# MAGIC │  │  📊 BREAKDOWN      │  │  📊 COMPARISON   │  │
# MAGIC │  │  (Bar Chart)      │  │  (Bar Chart)      │  │
# MAGIC │  └────────────────────┘  └────────────────────┘  │
# MAGIC │                                                │
# MAGIC │  ┌──────────────────────────────────────────┐  │
# MAGIC │  │  📊 DETAILED TABLE                    │  │
# MAGIC │  │  (Top 10 Items with Drill-Down)        │  │
# MAGIC │  └──────────────────────────────────────────┘  │
# MAGIC └────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Dashboard Types
# MAGIC
# MAGIC ### 1. **Strategic Dashboard**
# MAGIC - Audience: Executives
# MAGIC - Refresh: Daily/Weekly
# MAGIC - Focus: High-level KPIs, trends
# MAGIC - Example: Company performance scorecard
# MAGIC
# MAGIC ### 2. **Operational Dashboard**
# MAGIC - Audience: Operations teams
# MAGIC - Refresh: Real-time/Hourly
# MAGIC - Focus: Current status, alerts
# MAGIC - Example: Order processing monitor
# MAGIC
# MAGIC ### 3. **Analytical Dashboard**
# MAGIC - Audience: Analysts
# MAGIC - Refresh: On-demand
# MAGIC - Focus: Deep-dive analysis, drill-down
# MAGIC - Example: Customer behavior analysis
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Testing Your Dashboard
# MAGIC
# MAGIC ### Questions to Ask:
# MAGIC
# MAGIC 1. Can users answer their key questions in < 5 seconds?
# MAGIC 2. Is the most important information immediately visible?
# MAGIC 3. Are the visualizations easy to interpret?
# MAGIC 4. Does it load quickly (< 3 seconds)?
# MAGIC 5. Is it useful on mobile devices?
# MAGIC 6. Can users filter and drill down easily?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Next: Let's see the complete architecture!** 🚀

# COMMAND ----------

# DBTITLE 1,Section 8 - End-to-End Analytics Architecture
# MAGIC %md
# MAGIC # 🏛️ SECTION 8: End-to-End Analytics Architecture
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌟 Complete Analytics Data Flow
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC Think of it like a factory assembly line. Raw materials (data) come in one end, go through different stations (Bronze → Silver → Gold), get packaged nicely (Semantic Layer), and then delivered to customers (Business Users) through different delivery methods (Dashboards, BI Tools).
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC The modern analytics architecture follows the **Medallion Architecture** pattern combined with a **Semantic Layer** for consumption. This provides a structured, governed, and performant path from raw data to business insights.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛤️ Complete Architecture Diagram
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────────────────┐
# MAGIC │                     DATA SOURCES                            │
# MAGIC │   📦 Apps    📊 APIs    💾 Databases    ☁️ Cloud Storage   │
# MAGIC └─────────────────────────┬───────────────────────────────┘
# MAGIC                           │
# MAGIC                           │ Ingestion (Batch / Streaming)
# MAGIC                           │
# MAGIC        ┌──────────────────┴──────────────────┐
# MAGIC        │         BRONZE LAYER                │
# MAGIC        │   (Raw, Immutable Data)           │
# MAGIC        │   • Delta Tables                    │
# MAGIC        │   • Unity Catalog                  │
# MAGIC        │   • No Transformations             │
# MAGIC        └─────────────┬───────────────────┘
# MAGIC                      │
# MAGIC                      │ Cleansing & Validation
# MAGIC                      │
# MAGIC        ┌─────────────┴───────────────────┐
# MAGIC        │         SILVER LAYER                │
# MAGIC        │   (Cleaned, Validated Data)       │
# MAGIC        │   • Data Quality Rules             │
# MAGIC        │   • Deduplication                  │
# MAGIC        │   • Schema Evolution               │
# MAGIC        └─────────────┬───────────────────┘
# MAGIC                      │
# MAGIC                      │ Business Logic & Enrichment
# MAGIC                      │
# MAGIC        ┌─────────────┴───────────────────┐
# MAGIC        │         GOLD LAYER                  │
# MAGIC        │   (Business-Ready Data)           │
# MAGIC        │   • Dimensional Models             │
# MAGIC        │   • Aggregations                   │
# MAGIC        │   • Governed & Secure              │
# MAGIC        └─────────────┬───────────────────┘
# MAGIC                      │
# MAGIC                      │ Semantic Abstraction
# MAGIC                      │
# MAGIC        ┌─────────────┴───────────────────┐
# MAGIC        │      SEMANTIC LAYER               │
# MAGIC        │   (Business Metrics)              │
# MAGIC        │   • Business Views                 │
# MAGIC        │   • KPI Tables                     │
# MAGIC        │   • Metric Definitions             │
# MAGIC        └─────────────┬───────────────────┘
# MAGIC                      │
# MAGIC                      │ SQL Warehouse (Compute)
# MAGIC                      │
# MAGIC        ┌─────────────┴───────────────────┐
# MAGIC        │    CONSUMPTION LAYER             │
# MAGIC        │   • Databricks Dashboards          │
# MAGIC        │   • Power BI / Tableau             │
# MAGIC        │   • Looker / Qlik                  │
# MAGIC        │   • Custom Apps                    │
# MAGIC        └─────────────┬───────────────────┘
# MAGIC                      │
# MAGIC                      │
# MAGIC        ┌─────────────┴───────────────────┐
# MAGIC        │      BUSINESS USERS               │
# MAGIC        │   👥 Analysts  👥 Executives        │
# MAGIC        │   👥 Operations  👥 Stakeholders    │
# MAGIC        └─────────────────────────────────┘
# MAGIC
# MAGIC   🔒 Unity Catalog Governance Applied Throughout
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Layer Responsibilities
# MAGIC
# MAGIC | **Layer** | **Purpose** | **Data Quality** | **Users** |
# MAGIC |-----------|------------|-----------------|----------|
# MAGIC | **Bronze** | Raw data ingestion | None | Data Engineers |
# MAGIC | **Silver** | Cleaned & validated | High | Data Engineers |
# MAGIC | **Gold** | Business-ready | Very High | Analysts, BI |
# MAGIC | **Semantic** | Business abstraction | Very High | Business Users |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚙️ Key Components
# MAGIC
# MAGIC ### 1. **Data Ingestion**
# MAGIC - Auto Loader for files
# MAGIC - Delta Live Tables for pipelines
# MAGIC - Kafka/Kinesis for streaming
# MAGIC - JDBC for databases
# MAGIC
# MAGIC ### 2. **Storage**
# MAGIC - Delta Lake format
# MAGIC - Unity Catalog governance
# MAGIC - Time travel & versioning
# MAGIC - ACID transactions
# MAGIC
# MAGIC ### 3. **Compute**
# MAGIC - SQL Warehouses for analytics
# MAGIC - Clusters for engineering
# MAGIC - Serverless for cost optimization
# MAGIC
# MAGIC ### 4. **Orchestration**
# MAGIC - Databricks Workflows (Jobs)
# MAGIC - Scheduled pipelines
# MAGIC - Event-driven triggers
# MAGIC
# MAGIC ### 5. **Consumption**
# MAGIC - Native dashboards
# MAGIC - BI tool connectors
# MAGIC - REST APIs
# MAGIC - Embedded analytics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔒 Security & Governance
# MAGIC
# MAGIC ### Unity Catalog Features:
# MAGIC
# MAGIC 1. **Authentication**: SSO, OAuth, Tokens
# MAGIC 2. **Authorization**: Table/Column-level grants
# MAGIC 3. **Row-Level Security**: Dynamic filters
# MAGIC 4. **Column Masking**: PII protection
# MAGIC 5. **Audit Logging**: Track all access
# MAGIC 6. **Data Lineage**: End-to-end traceability
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💰 Cost Optimization Strategy
# MAGIC
# MAGIC 1. **Storage**: Use Delta optimization (VACUUM, OPTIMIZE)
# MAGIC 2. **Compute**: Serverless SQL Warehouses with auto-suspend
# MAGIC 3. **Caching**: Query result caching
# MAGIC 4. **Aggregation**: Pre-compute common queries
# MAGIC 5. **Monitoring**: Track DBU usage and optimize
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Data Flow Example
# MAGIC
# MAGIC ### Use Case: E-Commerce Analytics
# MAGIC
# MAGIC ```
# MAGIC 1. BRONZE: Raw order events from website
# MAGIC    ↓
# MAGIC 2. SILVER: Cleaned orders, validated customers
# MAGIC    ↓
# MAGIC 3. GOLD: Order facts, customer dimensions
# MAGIC    ↓
# MAGIC 4. SEMANTIC: monthly_sales_kpis, customer_360_view
# MAGIC    ↓
# MAGIC 5. SQL WAREHOUSE: Query execution engine
# MAGIC    ↓
# MAGIC 6. DASHBOARD: Executive sales dashboard
# MAGIC    ↓
# MAGIC 7. BUSINESS USER: CEO views revenue trends
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Next: Real-world BI scenario!** 🚀

# COMMAND ----------

# DBTITLE 1,Step 1 - Query Gold Layer
# MAGIC %sql
# MAGIC -- 📊 STEP 1: Query Gold Layer
# MAGIC -- Access curated customer and order data
# MAGIC
# MAGIC -- Sample Gold layer data
# MAGIC WITH gold_customers AS (
# MAGIC     SELECT 'CUST001' AS customer_id, 'John Smith' AS name, 'Premium' AS segment, DATE '2025-01-15' AS registration_date
# MAGIC     UNION ALL SELECT 'CUST002', 'Jane Doe', 'Standard', DATE '2025-03-20'
# MAGIC     UNION ALL SELECT 'CUST003', 'Bob Wilson', 'Premium', DATE '2025-02-10'
# MAGIC     UNION ALL SELECT 'CUST004', 'Alice Brown', 'Standard', DATE '2025-04-05'
# MAGIC     UNION ALL SELECT 'CUST005', 'Charlie Davis', 'Premium', DATE '2025-01-25'
# MAGIC ),
# MAGIC
# MAGIC gold_orders AS (
# MAGIC     SELECT 'ORD001' AS order_id, 'CUST001' AS customer_id, 1500.00 AS amount, DATE '2026-04-01' AS order_date
# MAGIC     UNION ALL SELECT 'ORD002', 'CUST001', 25.00, DATE '2026-04-05'
# MAGIC     UNION ALL SELECT 'ORD003', 'CUST002', 450.00, DATE '2026-04-10'
# MAGIC     UNION ALL SELECT 'ORD004', 'CUST003', 750.00, DATE '2026-04-12'
# MAGIC     UNION ALL SELECT 'ORD005', 'CUST002', 200.00, DATE '2026-04-15'
# MAGIC     UNION ALL SELECT 'ORD006', 'CUST001', 350.00, DATE '2026-04-18'
# MAGIC     UNION ALL SELECT 'ORD007', 'CUST004', 120.00, DATE '2026-03-20'
# MAGIC     UNION ALL SELECT 'ORD008', 'CUST003', 600.00, DATE '2026-03-25'
# MAGIC     UNION ALL SELECT 'ORD009', 'CUST005', 890.00, DATE '2026-04-20'
# MAGIC     UNION ALL SELECT 'ORD010', 'CUST005', 1200.00, DATE '2026-03-15'
# MAGIC )
# MAGIC
# MAGIC -- Query: Join customer and order data
# MAGIC SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.name AS customer_name,
# MAGIC     c.segment AS customer_segment,
# MAGIC     c.registration_date,
# MAGIC     o.order_id,
# MAGIC     o.order_date,
# MAGIC     o.amount AS order_amount,
# MAGIC     DATE_TRUNC('month', o.order_date) AS order_month
# MAGIC FROM gold_customers c
# MAGIC INNER JOIN gold_orders o
# MAGIC     ON c.customer_id = o.customer_id
# MAGIC WHERE o.order_date >= DATE '2026-03-01'
# MAGIC ORDER BY c.customer_id, o.order_date;
# MAGIC
# MAGIC -- ✅ This gives us raw transaction-level data from Gold layer
# MAGIC -- Next: Aggregate to create business metrics

# COMMAND ----------

# DBTITLE 1,Step 2 - Create Aggregated Dataset
# MAGIC %sql
# MAGIC -- 📊 STEP 2: Create Aggregated Dataset
# MAGIC -- Calculate KPIs and business metrics
# MAGIC
# MAGIC WITH gold_customers AS (
# MAGIC     SELECT 'CUST001' AS customer_id, 'John Smith' AS name, 'Premium' AS segment, DATE '2025-01-15' AS registration_date
# MAGIC     UNION ALL SELECT 'CUST002', 'Jane Doe', 'Standard', DATE '2025-03-20'
# MAGIC     UNION ALL SELECT 'CUST003', 'Bob Wilson', 'Premium', DATE '2025-02-10'
# MAGIC     UNION ALL SELECT 'CUST004', 'Alice Brown', 'Standard', DATE '2025-04-05'
# MAGIC     UNION ALL SELECT 'CUST005', 'Charlie Davis', 'Premium', DATE '2025-01-25'
# MAGIC ),
# MAGIC
# MAGIC gold_orders AS (
# MAGIC     SELECT 'ORD001' AS order_id, 'CUST001' AS customer_id, 1500.00 AS amount, DATE '2026-04-01' AS order_date
# MAGIC     UNION ALL SELECT 'ORD002', 'CUST001', 25.00, DATE '2026-04-05'
# MAGIC     UNION ALL SELECT 'ORD003', 'CUST002', 450.00, DATE '2026-04-10'
# MAGIC     UNION ALL SELECT 'ORD004', 'CUST003', 750.00, DATE '2026-04-12'
# MAGIC     UNION ALL SELECT 'ORD005', 'CUST002', 200.00, DATE '2026-04-15'
# MAGIC     UNION ALL SELECT 'ORD006', 'CUST001', 350.00, DATE '2026-04-18'
# MAGIC     UNION ALL SELECT 'ORD007', 'CUST004', 120.00, DATE '2026-03-20'
# MAGIC     UNION ALL SELECT 'ORD008', 'CUST003', 600.00, DATE '2026-03-25'
# MAGIC     UNION ALL SELECT 'ORD009', 'CUST005', 890.00, DATE '2026-04-20'
# MAGIC     UNION ALL SELECT 'ORD010', 'CUST005', 1200.00, DATE '2026-03-15'
# MAGIC ),
# MAGIC
# MAGIC customer_orders AS (
# MAGIC     SELECT 
# MAGIC         c.customer_id,
# MAGIC         c.segment AS customer_segment,
# MAGIC         o.order_date,
# MAGIC         o.amount AS order_amount,
# MAGIC         DATE_TRUNC('month', o.order_date) AS order_month
# MAGIC     FROM gold_customers c
# MAGIC     INNER JOIN gold_orders o ON c.customer_id = o.customer_id
# MAGIC )
# MAGIC
# MAGIC -- Aggregate to monthly metrics by customer segment
# MAGIC SELECT 
# MAGIC     DATE_FORMAT(order_month, 'yyyy-MM') AS month,
# MAGIC     customer_segment,
# MAGIC     
# MAGIC     -- Business Metrics
# MAGIC     COUNT(DISTINCT customer_id) AS active_customers,
# MAGIC     COUNT(*) AS total_orders,
# MAGIC     ROUND(SUM(order_amount), 2) AS total_revenue,
# MAGIC     ROUND(AVG(order_amount), 2) AS avg_order_value,
# MAGIC     ROUND(SUM(order_amount) / COUNT(DISTINCT customer_id), 2) AS revenue_per_customer,
# MAGIC     
# MAGIC     -- Additional KPIs
# MAGIC     ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT customer_id), 2) AS orders_per_customer
# MAGIC     
# MAGIC FROM customer_orders
# MAGIC GROUP BY DATE_FORMAT(order_month, 'yyyy-MM'), customer_segment
# MAGIC ORDER BY month DESC, customer_segment;
# MAGIC
# MAGIC -- ✅ This creates aggregated business metrics
# MAGIC -- Next: Format for dashboard consumption

# COMMAND ----------

# DBTITLE 1,Step 3 - Dashboard-Ready Visualization
# MAGIC %sql
# MAGIC -- 📊 STEP 3: Dashboard-Ready Visualization
# MAGIC -- Format data for business consumption and charts
# MAGIC
# MAGIC WITH gold_customers AS (
# MAGIC     SELECT 'CUST001' AS customer_id, 'John Smith' AS name, 'Premium' AS segment, DATE '2025-01-15' AS registration_date
# MAGIC     UNION ALL SELECT 'CUST002', 'Jane Doe', 'Standard', DATE '2025-03-20'
# MAGIC     UNION ALL SELECT 'CUST003', 'Bob Wilson', 'Premium', DATE '2025-02-10'
# MAGIC     UNION ALL SELECT 'CUST004', 'Alice Brown', 'Standard', DATE '2025-04-05'
# MAGIC     UNION ALL SELECT 'CUST005', 'Charlie Davis', 'Premium', DATE '2025-01-25'
# MAGIC ),
# MAGIC
# MAGIC gold_orders AS (
# MAGIC     SELECT 'ORD001' AS order_id, 'CUST001' AS customer_id, 1500.00 AS amount, DATE '2026-04-01' AS order_date
# MAGIC     UNION ALL SELECT 'ORD002', 'CUST001', 25.00, DATE '2026-04-05'
# MAGIC     UNION ALL SELECT 'ORD003', 'CUST002', 450.00, DATE '2026-04-10'
# MAGIC     UNION ALL SELECT 'ORD004', 'CUST003', 750.00, DATE '2026-04-12'
# MAGIC     UNION ALL SELECT 'ORD005', 'CUST002', 200.00, DATE '2026-04-15'
# MAGIC     UNION ALL SELECT 'ORD006', 'CUST001', 350.00, DATE '2026-04-18'
# MAGIC     UNION ALL SELECT 'ORD007', 'CUST004', 120.00, DATE '2026-03-20'
# MAGIC     UNION ALL SELECT 'ORD008', 'CUST003', 600.00, DATE '2026-03-25'
# MAGIC     UNION ALL SELECT 'ORD009', 'CUST005', 890.00, DATE '2026-04-20'
# MAGIC     UNION ALL SELECT 'ORD010', 'CUST005', 1200.00, DATE '2026-03-15'
# MAGIC ),
# MAGIC
# MAGIC customer_orders AS (
# MAGIC     SELECT 
# MAGIC         c.customer_id,
# MAGIC         c.segment AS customer_segment,
# MAGIC         o.order_date,
# MAGIC         o.amount AS order_amount,
# MAGIC         DATE_TRUNC('month', o.order_date) AS order_month
# MAGIC     FROM gold_customers c
# MAGIC     INNER JOIN gold_orders o ON c.customer_id = o.customer_id
# MAGIC ),
# MAGIC
# MAGIC monthly_metrics AS (
# MAGIC     SELECT 
# MAGIC         DATE_FORMAT(order_month, 'yyyy-MM') AS month,
# MAGIC         customer_segment,
# MAGIC         COUNT(DISTINCT customer_id) AS active_customers,
# MAGIC         COUNT(*) AS total_orders,
# MAGIC         SUM(order_amount) AS total_revenue,
# MAGIC         AVG(order_amount) AS avg_order_value,
# MAGIC         SUM(order_amount) / COUNT(DISTINCT customer_id) AS revenue_per_customer
# MAGIC     FROM customer_orders
# MAGIC     GROUP BY DATE_FORMAT(order_month, 'yyyy-MM'), customer_segment
# MAGIC )
# MAGIC
# MAGIC -- Dashboard-Ready Output with Business Formatting
# MAGIC SELECT 
# MAGIC     month AS reporting_month,
# MAGIC     customer_segment,
# MAGIC     
# MAGIC     -- Formatted Metrics (Dashboard-friendly)
# MAGIC     active_customers,
# MAGIC     total_orders,
# MAGIC     CONCAT('$', FORMAT_NUMBER(total_revenue, 2)) AS total_revenue_formatted,
# MAGIC     CONCAT('$', FORMAT_NUMBER(avg_order_value, 2)) AS avg_order_value_formatted,
# MAGIC     CONCAT('$', FORMAT_NUMBER(revenue_per_customer, 2)) AS revenue_per_customer_formatted,
# MAGIC     
# MAGIC     -- Calculated Fields for Charts
# MAGIC     ROUND((revenue_per_customer / avg_order_value), 2) AS purchase_frequency,
# MAGIC     
# MAGIC     -- Performance Indicator
# MAGIC     CASE 
# MAGIC         WHEN total_revenue > 1500 THEN '✅ Excellent'
# MAGIC         WHEN total_revenue > 800 THEN '⚠️ Good'
# MAGIC         ELSE '🔴 Needs Attention'
# MAGIC     END AS performance_status
# MAGIC     
# MAGIC FROM monthly_metrics
# MAGIC ORDER BY month DESC, total_revenue DESC;
# MAGIC
# MAGIC -- 🎯 This query is ready for:
# MAGIC -- 1. Databricks Dashboard visualization
# MAGIC -- 2. BI tool consumption (Power BI, Tableau)
# MAGIC -- 3. Scheduled reports
# MAGIC -- 4. Executive presentations
# MAGIC
# MAGIC -- 📊 Recommended Charts:
# MAGIC -- • Bar Chart: Revenue by Segment
# MAGIC -- • Line Chart: Monthly Revenue Trend
# MAGIC -- • KPI Cards: Total Revenue, Active Customers
# MAGIC -- • Table: Detailed Metrics

# COMMAND ----------

# DBTITLE 1,Section 5 - Demo Semantic Layer
# MAGIC %sql
# MAGIC -- 🏛️ DEMO: Creating a Semantic Layer
# MAGIC -- This demonstrates building business-friendly analytics tables
# MAGIC
# MAGIC -- Note: Replace with your actual Unity Catalog path
# MAGIC -- Format: catalog_name.schema_name.table_name
# MAGIC
# MAGIC -- Step 1: Create sample Gold layer data
# MAGIC WITH gold_sales AS (
# MAGIC     SELECT 'ORD001' AS order_id, 'CUST001' AS customer_id, 'Electronics' AS category, 'Laptop' AS product, 1500.00 AS amount, DATE '2026-04-01' AS order_date
# MAGIC     UNION ALL SELECT 'ORD002', 'CUST001', 'Electronics', 'Mouse', 25.00, DATE '2026-04-05'
# MAGIC     UNION ALL SELECT 'ORD003', 'CUST002', 'Clothing', 'Shirt', 45.00, DATE '2026-04-10'
# MAGIC     UNION ALL SELECT 'ORD004', 'CUST003', 'Electronics', 'Keyboard', 75.00, DATE '2026-04-12'
# MAGIC     UNION ALL SELECT 'ORD005', 'CUST002', 'Home & Garden', 'Chair', 200.00, DATE '2026-04-15'
# MAGIC     UNION ALL SELECT 'ORD006', 'CUST001', 'Electronics', 'Monitor', 350.00, DATE '2026-04-18'
# MAGIC     UNION ALL SELECT 'ORD007', 'CUST004', 'Sports', 'Tennis Racket', 120.00, DATE '2026-04-20'
# MAGIC     UNION ALL SELECT 'ORD008', 'CUST003', 'Clothing', 'Jeans', 60.00, DATE '2026-04-21'
# MAGIC ),
# MAGIC
# MAGIC -- Step 2: Create Business-Friendly Semantic View
# MAGIC semantic_sales_metrics AS (
# MAGIC     SELECT 
# MAGIC         -- Time dimensions (business-friendly)
# MAGIC         DATE_TRUNC('month', order_date) AS month,
# MAGIC         DATE_TRUNC('week', order_date) AS week,
# MAGIC         order_date AS date,
# MAGIC         
# MAGIC         -- Product dimensions
# MAGIC         category AS product_category,
# MAGIC         product AS product_name,
# MAGIC         
# MAGIC         -- Customer dimension
# MAGIC         customer_id,
# MAGIC         
# MAGIC         -- Metrics (clear business names)
# MAGIC         amount AS order_amount,
# MAGIC         1 AS order_count
# MAGIC     FROM gold_sales
# MAGIC ),
# MAGIC
# MAGIC -- Step 3: Create Pre-Aggregated KPI Table
# MAGIC monthly_kpis AS (
# MAGIC     SELECT 
# MAGIC         month,
# MAGIC         product_category,
# MAGIC         
# MAGIC         -- Key Performance Indicators
# MAGIC         COUNT(DISTINCT customer_id) AS unique_customers,
# MAGIC         SUM(order_count) AS total_orders,
# MAGIC         SUM(order_amount) AS total_revenue,
# MAGIC         ROUND(AVG(order_amount), 2) AS avg_order_value,
# MAGIC         ROUND(SUM(order_amount) / COUNT(DISTINCT customer_id), 2) AS revenue_per_customer
# MAGIC         
# MAGIC     FROM semantic_sales_metrics
# MAGIC     GROUP BY month, product_category
# MAGIC )
# MAGIC
# MAGIC -- Final Output: Business-Ready Analytics
# MAGIC SELECT 
# MAGIC     DATE_FORMAT(month, 'yyyy-MM') AS reporting_month,
# MAGIC     product_category,
# MAGIC     unique_customers,
# MAGIC     total_orders,
# MAGIC     CONCAT('$', FORMAT_NUMBER(total_revenue, 2)) AS total_revenue,
# MAGIC     CONCAT('$', FORMAT_NUMBER(avg_order_value, 2)) AS avg_order_value,
# MAGIC     CONCAT('$', FORMAT_NUMBER(revenue_per_customer, 2)) AS revenue_per_customer
# MAGIC FROM monthly_kpis
# MAGIC ORDER BY reporting_month, total_revenue DESC;
# MAGIC
# MAGIC -- 🎯 This semantic layer provides:
# MAGIC -- 1. Business-friendly column names
# MAGIC -- 2. Pre-calculated KPIs
# MAGIC -- 3. Formatted currency values
# MAGIC -- 4. Clear aggregation logic
# MAGIC
# MAGIC -- 📊 Real-world implementation:
# MAGIC -- CREATE OR REPLACE TABLE analytics.business.monthly_kpis AS
# MAGIC -- SELECT ... FROM main.gold.sales ...
# MAGIC
# MAGIC -- CREATE OR REPLACE VIEW analytics.business.sales_dashboard_view AS
# MAGIC -- SELECT ... FROM analytics.business.monthly_kpis ...

# COMMAND ----------

# DBTITLE 1,Section 6 - Hands-on Analytics Pipeline Concept
# MAGIC %md
# MAGIC # 🛠️ SECTION 6: Hands-on Analytics Pipeline
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Building an End-to-End Analytics Flow
# MAGIC
# MAGIC ### Pipeline Overview:
# MAGIC
# MAGIC ```
# MAGIC Gold Table → Semantic Layer → Dashboard Query → Visualization
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Analytics Pipeline Steps
# MAGIC
# MAGIC ### **Step 1: Query Gold Layer**
# MAGIC - Access curated, governed data
# MAGIC - Apply business filters
# MAGIC - Select relevant columns
# MAGIC
# MAGIC ### **Step 2: Create Aggregated Dataset**
# MAGIC - Group by business dimensions
# MAGIC - Calculate KPIs and metrics
# MAGIC - Apply business logic
# MAGIC
# MAGIC ### **Step 3: Visualize Results**
# MAGIC - Format for business consumption
# MAGIC - Add calculated fields
# MAGIC - Prepare for dashboard
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Use Case: Customer Analytics Dashboard
# MAGIC
# MAGIC ### Business Requirement:
# MAGIC **"Show me monthly customer engagement metrics by segment"**
# MAGIC
# MAGIC ### What We'll Build:
# MAGIC
# MAGIC | **Metric** | **Definition** |
# MAGIC |-----------|---------------|
# MAGIC | Active Customers | Customers with orders in the month |
# MAGIC | Total Orders | Count of all orders |
# MAGIC | Revenue | Sum of order amounts |
# MAGIC | Avg Order Value | Revenue / Total Orders |
# MAGIC | Customer Lifetime Value | Total revenue per customer |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Let's Build It!
# MAGIC
# MAGIC Next cells will demonstrate each step of the analytics pipeline.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 3 - BI Tool Integration
# MAGIC %md
# MAGIC # 🔌 SECTION 3: BI Tool Integration
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌟 Connecting BI Tools to Databricks
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC Imagine you have a special remote control (BI tool like Power BI) that can talk to your TV (Databricks). You don't need to be next to the TV to see your favorite shows — the remote connects wirelessly and shows you everything!
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC BI Tool Integration enables external business intelligence platforms to connect to Databricks SQL Warehouses via **JDBC/ODBC** connectors. This allows organizations to leverage existing BI investments while querying Delta Lake data directly.
# MAGIC
# MAGIC **Key Benefits:**
# MAGIC - Unified data source (single source of truth)
# MAGIC - Real-time query execution
# MAGIC - Familiar BI interfaces for business users
# MAGIC - Enterprise security and governance
# MAGIC - Cost-effective (no data duplication)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Supported BI Tools
# MAGIC
# MAGIC | **BI Tool** | **Connector Type** | **Use Case** |
# MAGIC |-------------|-------------------|-------------|
# MAGIC | **Power BI** | ODBC / Partner Connect | Microsoft ecosystem integration |
# MAGIC | **Tableau** | JDBC / Native Connector | Advanced visualizations |
# MAGIC | **Qlik Sense** | ODBC | Associative analytics |
# MAGIC | **Looker** | SQL Database | Semantic modeling |
# MAGIC | **ThoughtSpot** | Direct Integration | AI-powered search analytics |
# MAGIC | **Excel** | ODBC | Ad-hoc analysis |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔗 Connection Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌───────────────────────┐
# MAGIC │   BI Tool              │
# MAGIC │   (Power BI/Tableau)   │
# MAGIC └──────────┬────────────┘
# MAGIC            │
# MAGIC            │ JDBC/ODBC
# MAGIC            │
# MAGIC ┌──────────┴────────────┐
# MAGIC │   SQL Warehouse        │
# MAGIC │   (Serverless)         │
# MAGIC └──────────┬────────────┘
# MAGIC            │
# MAGIC            │ Query Execution
# MAGIC            │
# MAGIC ┌──────────┴────────────┐
# MAGIC │   Delta Lake           │
# MAGIC │   (Unity Catalog)      │
# MAGIC └───────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔑 Connection Parameters
# MAGIC
# MAGIC To connect a BI tool to Databricks, you need:
# MAGIC
# MAGIC 1. **Server Hostname**: `<workspace-url>.cloud.databricks.com`
# MAGIC 2. **HTTP Path**: `/sql/1.0/warehouses/<warehouse-id>`
# MAGIC 3. **Personal Access Token**: Authentication credential
# MAGIC 4. **Port**: 443 (HTTPS)
# MAGIC 5. **Catalog**: Unity Catalog name
# MAGIC 6. **Schema**: Target schema name
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Power BI Integration Example
# MAGIC
# MAGIC ### Steps:
# MAGIC
# MAGIC 1. **Open Power BI Desktop**
# MAGIC 2. **Get Data** → **More** → **Databricks**
# MAGIC 3. **Enter connection details**:
# MAGIC    - Server: `<workspace>.cloud.databricks.com`
# MAGIC    - HTTP Path: `/sql/1.0/warehouses/<id>`
# MAGIC    - Authentication: Personal Access Token
# MAGIC 4. **Select tables** from Unity Catalog
# MAGIC 5. **Load data** and create visualizations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Best Practices
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC - Use dedicated SQL Warehouse for BI workloads
# MAGIC - Leverage Import mode for small datasets
# MAGIC - Use DirectQuery for real-time data
# MAGIC - Create materialized views for complex queries
# MAGIC - Apply row-level security in Unity Catalog
# MAGIC
# MAGIC ❌ **DON'T:**
# MAGIC - Query Bronze/Silver layers directly
# MAGIC - Use Classic Clusters for BI
# MAGIC - Ignore query performance optimization
# MAGIC - Expose raw technical table names
# MAGIC - Skip data quality validation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔐 Security Considerations
# MAGIC
# MAGIC - **Authentication**: Personal Access Token or OAuth
# MAGIC - **Authorization**: Unity Catalog grants (SELECT privileges)
# MAGIC - **Row-Level Security**: Applied at Unity Catalog level
# MAGIC - **Network Security**: Private Link or IP Access Lists
# MAGIC - **Audit Logging**: Track all BI tool queries
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Next: Let's explore SQL Warehouses!** 🚀

# COMMAND ----------

# DBTITLE 1,Section 4 - SQL Warehouse for BI
# MAGIC %md
# MAGIC # ⚡ SECTION 4: SQL Warehouse for BI
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌟 What is a SQL Warehouse?
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC Think of a SQL Warehouse like a super-fast calculator that many people can use at the same time. When someone asks a question (query), it quickly finds the answer and shows it to them. If lots of people ask questions, it gets bigger automatically to handle everyone!
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC **Databricks SQL Warehouse** is a managed compute resource optimized for SQL analytics workloads. It provides:
# MAGIC
# MAGIC - **Serverless Architecture**: Auto-provisioning and scaling
# MAGIC - **Query Optimization**: Photon engine, query compilation
# MAGIC - **Concurrency**: Multi-user support with query queuing
# MAGIC - **Caching**: Result caching, disk caching
# MAGIC - **Cost Management**: Auto-suspend, per-second billing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ SQL Warehouse Types
# MAGIC
# MAGIC | **Type** | **Use Case** | **Characteristics** |
# MAGIC |----------|-------------|--------------------|
# MAGIC | **Serverless** | BI, dashboards, ad-hoc queries | Instant start, auto-scale, minimal config |
# MAGIC | **Pro** | Production workloads | High concurrency, auto-scaling |
# MAGIC | **Classic** | Legacy (deprecated) | Fixed size, manual scaling |
# MAGIC
# MAGIC **Recommendation**: Always use **Serverless SQL Warehouse** for modern workloads.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏃 Performance Features
# MAGIC
# MAGIC ### 1. **Photon Engine**
# MAGIC - Vectorized query execution
# MAGIC - Native C++ implementation
# MAGIC - 2-10x faster than standard Spark SQL
# MAGIC
# MAGIC ### 2. **Query Result Caching**
# MAGIC - Cache identical queries
# MAGIC - Instant response for repeated queries
# MAGIC - Automatic cache invalidation
# MAGIC
# MAGIC ### 3. **Auto-Scaling**
# MAGIC - Scale clusters up/down based on demand
# MAGIC - Handle burst workloads
# MAGIC - Optimize cost and performance
# MAGIC
# MAGIC ### 4. **Query Compilation**
# MAGIC - Pre-compiled query plans
# MAGIC - Faster execution
# MAGIC - Reduced CPU overhead
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Warehouse Sizing
# MAGIC
# MAGIC | **Size** | **Clusters** | **Use Case** |
# MAGIC |----------|-------------|-------------|
# MAGIC | **X-Small** | 1 cluster | Development, testing |
# MAGIC | **Small** | 1-2 clusters | Small team, low concurrency |
# MAGIC | **Medium** | 2-4 clusters | Production, moderate concurrency |
# MAGIC | **Large** | 4-8 clusters | Enterprise, high concurrency |
# MAGIC | **X-Large** | 8-16 clusters | Very large enterprise |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚙️ Configuration Best Practices
# MAGIC
# MAGIC ### Auto-Stop Settings:
# MAGIC ```
# MAGIC Recommended: 10-15 minutes
# MAGIC - Balances cost and user experience
# MAGIC - Prevents cold starts for active users
# MAGIC ```
# MAGIC
# MAGIC ### Scaling Policy:
# MAGIC ```
# MAGIC Min Clusters: 1
# MAGIC Max Clusters: 4-8 (based on concurrency needs)
# MAGIC - Allows burst capacity
# MAGIC - Controls maximum cost
# MAGIC ```
# MAGIC
# MAGIC ### Query Timeout:
# MAGIC ```
# MAGIC Recommended: 5-10 minutes
# MAGIC - Prevents runaway queries
# MAGIC - Protects warehouse resources
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💰 Cost Optimization
# MAGIC
# MAGIC ✅ **Strategies:**
# MAGIC
# MAGIC 1. **Right-Size Warehouse**: Start small, scale as needed
# MAGIC 2. **Enable Auto-Stop**: Avoid idle compute costs
# MAGIC 3. **Use Result Cache**: Reduce redundant query execution
# MAGIC 4. **Optimize Queries**: Reduce data scanned
# MAGIC 5. **Schedule Reports**: Batch during off-peak hours
# MAGIC 6. **Monitor Usage**: Track DBU consumption
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 SQL Warehouse vs Cluster
# MAGIC
# MAGIC | **Aspect** | **SQL Warehouse** | **Cluster** |
# MAGIC |------------|------------------|------------|
# MAGIC | **Purpose** | Analytics, BI | Data engineering, ML |
# MAGIC | **Language** | SQL only | Python, Scala, R, SQL |
# MAGIC | **Optimization** | Query performance | General-purpose |
# MAGIC | **Concurrency** | High (multi-user) | Low (notebook-based) |
# MAGIC | **Cost** | Per-query billing | Always-on compute |
# MAGIC | **Use Case** | Dashboards, reports | ETL pipelines, training |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 When to Use SQL Warehouse
# MAGIC
# MAGIC ✅ **PERFECT FOR:**
# MAGIC - Databricks Dashboards
# MAGIC - BI tool connections (Power BI, Tableau)
# MAGIC - Ad-hoc SQL queries
# MAGIC - Scheduled reports
# MAGIC - Self-service analytics
# MAGIC
# MAGIC ❌ **NOT IDEAL FOR:**
# MAGIC - Data engineering pipelines (use workflows)
# MAGIC - Machine learning training (use ML clusters)
# MAGIC - Python/Scala notebooks (use compute clusters)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Next: Let's design a semantic layer!** 🚀

# COMMAND ----------

# DBTITLE 1,Section 5 - Semantic Layer Design
# MAGIC %md
# MAGIC # 🏛️ SECTION 5: Semantic Layer Design
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌟 What is a Semantic Layer?
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC Imagine you have a secret language book that translates computer words into words you actually understand. Instead of "customer_id", it says "Customer Number". Instead of weird table names, it says "Sales Report". The semantic layer is that translation book!
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC A **Semantic Layer** is an abstraction layer between raw data tables and business users that provides:
# MAGIC
# MAGIC - **Business-Friendly Naming**: Translate technical names to business terminology
# MAGIC - **Pre-Aggregated Metrics**: Define KPIs once, use everywhere
# MAGIC - **Consistent Logic**: Single source of truth for calculations
# MAGIC - **Access Control**: Hide complexity, expose only relevant data
# MAGIC - **Performance**: Optimized views and materialized tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Why Semantic Layer Matters
# MAGIC
# MAGIC ### Without Semantic Layer:
# MAGIC ```sql
# MAGIC -- Users write complex SQL every time
# MAGIC SELECT 
# MAGIC     c.cust_id,
# MAGIC     c.cust_nm,
# MAGIC     SUM(o.ord_amt) as tot_rev,
# MAGIC     COUNT(DISTINCT o.ord_id) as tot_ords
# MAGIC FROM prod_db.raw_schema.customer_dim c
# MAGIC JOIN prod_db.raw_schema.order_fact o
# MAGIC   ON c.cust_key = o.cust_fk
# MAGIC WHERE o.ord_dt >= '2026-01-01'
# MAGIC GROUP BY c.cust_id, c.cust_nm;
# MAGIC ```
# MAGIC
# MAGIC ### With Semantic Layer:
# MAGIC ```sql
# MAGIC -- Simple, business-friendly query
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     total_revenue,
# MAGIC     total_orders
# MAGIC FROM analytics.business.customer_metrics
# MAGIC WHERE year = 2026;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Semantic Layer Components
# MAGIC
# MAGIC | **Component** | **Purpose** | **Example** |
# MAGIC |---------------|------------|------------|
# MAGIC | **Business Views** | Simplified table structures | `customer_360_view` |
# MAGIC | **Metric Tables** | Pre-calculated KPIs | `monthly_sales_kpis` |
# MAGIC | **Dimension Tables** | Master data | `dim_customer`, `dim_product` |
# MAGIC | **Fact Tables** | Transaction data | `fact_sales`, `fact_orders` |
# MAGIC | **Aggregate Tables** | Rolled-up summaries | `sales_by_month_category` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Semantic Layer Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────┐
# MAGIC │   BUSINESS USERS           │
# MAGIC │   (Self-Service Analytics) │
# MAGIC └───────────┬────────────────┘
# MAGIC             │
# MAGIC             │ Query
# MAGIC             │
# MAGIC ┌───────────┴────────────────┐
# MAGIC │   SEMANTIC LAYER           │
# MAGIC │   • Business Views          │
# MAGIC │   • Metric Tables           │
# MAGIC │   • KPI Definitions        │
# MAGIC └───────────┬────────────────┘
# MAGIC             │
# MAGIC             │ Transform
# MAGIC             │
# MAGIC ┌───────────┴────────────────┐
# MAGIC │   GOLD LAYER               │
# MAGIC │   (Curated Delta Tables)   │
# MAGIC └────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Design Patterns
# MAGIC
# MAGIC ### 1. **Business View Pattern**
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REPLACE VIEW analytics.business.customer_overview AS
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     email,
# MAGIC     registration_date,
# MAGIC     customer_segment,
# MAGIC     lifetime_value,
# MAGIC     total_orders,
# MAGIC     last_order_date
# MAGIC FROM main.gold.customer_master
# MAGIC WHERE is_active = true;
# MAGIC ```
# MAGIC
# MAGIC ### 2. **KPI Metric Pattern**
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REPLACE TABLE analytics.metrics.monthly_kpis AS
# MAGIC SELECT 
# MAGIC     DATE_TRUNC('month', order_date) as month,
# MAGIC     COUNT(DISTINCT customer_id) as active_customers,
# MAGIC     COUNT(*) as total_orders,
# MAGIC     SUM(order_amount) as total_revenue,
# MAGIC     AVG(order_amount) as avg_order_value,
# MAGIC     SUM(order_amount) / COUNT(DISTINCT customer_id) as revenue_per_customer
# MAGIC FROM main.gold.orders
# MAGIC GROUP BY DATE_TRUNC('month', order_date);
# MAGIC ```
# MAGIC
# MAGIC ### 3. **Aggregate Table Pattern**
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REPLACE TABLE analytics.aggregates.sales_by_category AS
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     sub_category,
# MAGIC     DATE_TRUNC('day', order_date) as date,
# MAGIC     COUNT(*) as order_count,
# MAGIC     SUM(quantity) as units_sold,
# MAGIC     SUM(revenue) as total_revenue
# MAGIC FROM main.gold.sales
# MAGIC GROUP BY category, sub_category, DATE_TRUNC('day', order_date);
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Best Practices
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC
# MAGIC 1. **Use Business Names**: `customer_name` not `cust_nm`
# MAGIC 2. **Document Metrics**: Add comments to views/tables
# MAGIC 3. **Standardize Calculations**: Define KPIs once
# MAGIC 4. **Optimize Performance**: Create materialized tables
# MAGIC 5. **Version Control**: Track schema changes
# MAGIC 6. **Apply Governance**: Row-level security, column masking
# MAGIC
# MAGIC ❌ **DON'T:**
# MAGIC
# MAGIC 1. **Expose Raw Tables**: Users shouldn't query Bronze/Silver
# MAGIC 2. **Use Technical Jargon**: Avoid `fct_`, `dim_`, `stg_` prefixes
# MAGIC 3. **Skip Documentation**: Always explain business logic
# MAGIC 4. **Ignore Performance**: Monitor query execution times
# MAGIC 5. **Duplicate Logic**: One metric, one definition
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Let's build a semantic layer!** 🚀

# COMMAND ----------

# DBTITLE 1,Phase 9 Day 38 - Analytics Layer Header
# MAGIC %md
# MAGIC # 🧮 Data Engineering Training — Phase 9 Day 38  
# MAGIC ## 📊 Analytics Layer: Dashboards & BI Integration  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - Databricks Dashboards  
# MAGIC - BI Integration (Power BI, Tableau, Qlik)  
# MAGIC - SQL Warehouses for Analytics  
# MAGIC - Semantic Layer & Reporting  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### ⚙️ Platform: Databricks (Serverless SQL + Unity Catalog + Delta Lake + BI Tools)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Objective:
# MAGIC Understand how to build dashboards, integrate with BI tools, and design a scalable analytics layer on top of Delta Lake.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ IMPORTANT ENGINEERING CONSTRAINTS:
# MAGIC
# MAGIC ✅ **USE:**
# MAGIC - Databricks Serverless SQL Warehouses
# MAGIC - Unity Catalog for all tables
# MAGIC - Delta Lake format
# MAGIC - Analytics-first design
# MAGIC - Governed data architecture
# MAGIC
# MAGIC ❌ **DO NOT USE:**
# MAGIC - RDDs (Resilient Distributed Datasets)
# MAGIC - cache() / persist() operations
# MAGIC - Local storage paths
# MAGIC - Unoptimized queries for dashboards
# MAGIC - Direct raw data consumption
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📚 Training Flow:
# MAGIC 1. Analytics Layer Fundamentals
# MAGIC 2. Databricks Dashboard Creation
# MAGIC 3. BI Tool Integration Patterns
# MAGIC 4. SQL Warehouse Architecture
# MAGIC 5. Semantic Layer Design
# MAGIC 6. Hands-on Analytics Pipeline
# MAGIC 7. Dashboard Design Best Practices
# MAGIC 8. End-to-End Architecture
# MAGIC 9. Real-World Scenarios
# MAGIC 10. Genie Code Agent for Analytics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Let's begin building production-grade analytics solutions!** 🚀

# COMMAND ----------

# DBTITLE 1,Section 1 - Analytics Layer Overview
# MAGIC %md
# MAGIC # 📊 SECTION 1: Analytics Layer Overview
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌟 What is the Analytics Layer?
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC Imagine you have a giant toy box (data warehouse) full of different toys (data). The **Analytics Layer** is like organizing those toys into special display cases where everyone can easily see and play with their favorite toys. Instead of digging through the whole box, you can just look at the organized display!
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC The **Analytics Layer** is the consumption layer of the data architecture that sits on top of the curated data warehouse (Gold layer). It provides:
# MAGIC
# MAGIC 1. **Data Abstraction**: Business-friendly views and semantic models
# MAGIC 2. **Performance Optimization**: Pre-aggregated datasets, materialized views
# MAGIC 3. **Consumption Interfaces**: Dashboards, BI tools, reports, APIs
# MAGIC 4. **Self-Service Analytics**: Empowers business users without SQL expertise
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Role of the Analytics Layer
# MAGIC
# MAGIC | **Aspect** | **Description** |
# MAGIC |------------|----------------|
# MAGIC | **Purpose** | Transform technical data into business insights |
# MAGIC | **Consumers** | Business analysts, executives, operations teams |
# MAGIC | **Technologies** | SQL Warehouses, Dashboards, BI Tools |
# MAGIC | **Data Source** | Gold Layer (curated, governed data) |
# MAGIC | **Query Pattern** | Read-heavy, aggregation-focused |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛤️ Modern Analytics Stack
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────┐
# MAGIC │   BUSINESS USERS          │
# MAGIC │   (Self-Service)          │
# MAGIC └───────┬──────────────────┘
# MAGIC         │
# MAGIC         │ Dashboards & BI Tools
# MAGIC         │
# MAGIC ┌───────┴──────────────────┐
# MAGIC │   ANALYTICS LAYER         │
# MAGIC │   (Semantic Models)       │
# MAGIC └───────┬──────────────────┘
# MAGIC         │
# MAGIC         │ SQL Warehouse
# MAGIC         │
# MAGIC ┌───────┴──────────────────┐
# MAGIC │   GOLD LAYER              │
# MAGIC │   (Curated Data)          │
# MAGIC └──────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔑 Key Principles
# MAGIC
# MAGIC 1. **Gold Layer First**: Always consume from curated, governed data
# MAGIC 2. **Pre-Aggregate**: Create summary tables for common queries
# MAGIC 3. **Semantic Clarity**: Use business terminology, not technical jargon
# MAGIC 4. **Performance Optimization**: Design for fast query response times
# MAGIC 5. **Governance**: Apply access controls and data quality checks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Next: Let's build our first dashboard!** 🚀

# COMMAND ----------

# DBTITLE 1,Section 2 - Databricks Dashboards Concept
# MAGIC %md
# MAGIC # 📊 SECTION 2: Databricks Dashboards
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌟 What are Databricks Dashboards?
# MAGIC
# MAGIC ### 👶 ELI5 Explanation:
# MAGIC Think of a dashboard like the dashboard in a car. Instead of showing speed and fuel, it shows business numbers like "How many customers?" or "How much money did we make?" You can see everything important at a glance!
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC **Databricks Dashboards** (Lakeview Dashboards) are native, interactive visualization tools built directly into the Databricks platform. They provide:
# MAGIC
# MAGIC - **Native Integration**: Direct connection to Delta tables and SQL Warehouses
# MAGIC - **Real-Time Data**: Query live data without ETL to external BI tools
# MAGIC - **Collaboration**: Share insights with team members
# MAGIC - **Parameterization**: Dynamic filtering and drill-down capabilities
# MAGIC - **Scheduled Refresh**: Automated updates
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠️ Dashboard Capabilities
# MAGIC
# MAGIC | **Feature** | **Description** |
# MAGIC |-------------|----------------|
# MAGIC | **Visualizations** | Bar charts, line charts, pie charts, tables, maps |
# MAGIC | **Interactivity** | Filters, parameters, drill-down |
# MAGIC | **Data Source** | SQL queries against Delta tables |
# MAGIC | **Sharing** | Public links, embedded dashboards |
# MAGIC | **Scheduling** | Email delivery, Slack notifications |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Dashboard Creation Workflow
# MAGIC
# MAGIC ```
# MAGIC Step 1: Write SQL Query
# MAGIC    ↓
# MAGIC Step 2: Create Visualization
# MAGIC    ↓
# MAGIC Step 3: Add to Dashboard
# MAGIC    ↓
# MAGIC Step 4: Configure Parameters
# MAGIC    ↓
# MAGIC Step 5: Share with Stakeholders
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 From Query to Dashboard
# MAGIC
# MAGIC ### Process:
# MAGIC
# MAGIC 1. **Write a SQL query** in Databricks SQL editor
# MAGIC 2. **Run the query** to verify results
# MAGIC 3. **Add visualization** (chart type, axes, colors)
# MAGIC 4. **Create dashboard** and add the visualization
# MAGIC 5. **Configure refresh schedule**
# MAGIC 6. **Share with team** via link or embed
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Use Cases
# MAGIC
# MAGIC - **Executive Dashboards**: High-level KPIs and trends
# MAGIC - **Operational Dashboards**: Real-time monitoring
# MAGIC - **Analytical Dashboards**: Deep-dive analysis
# MAGIC - **Report Dashboards**: Scheduled business reports
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Let's create our first analytics query!** 📊

# COMMAND ----------

# DBTITLE 1,Section 2 - Demo Dashboard Query
# MAGIC %sql
# MAGIC -- 📊 DEMO: Sample Analytics Query for Dashboard
# MAGIC -- This query demonstrates a typical analytics pattern for dashboards
# MAGIC
# MAGIC -- Note: Replace with your actual Unity Catalog path
# MAGIC -- Format: catalog_name.schema_name.table_name
# MAGIC
# MAGIC -- Example: Sales Performance by Category
# MAGIC SELECT 
# MAGIC     'Electronics' AS category,
# MAGIC     250 AS total_orders,
# MAGIC     125000.50 AS total_revenue,
# MAGIC     500.00 AS avg_order_value,
# MAGIC     '2026-04' AS month
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Clothing' AS category,
# MAGIC     180 AS total_orders,
# MAGIC     45000.75 AS total_revenue,
# MAGIC     250.00 AS avg_order_value,
# MAGIC     '2026-04' AS month
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Home & Garden' AS category,
# MAGIC     150 AS total_orders,
# MAGIC     67500.00 AS total_revenue,
# MAGIC     450.00 AS avg_order_value,
# MAGIC     '2026-04' AS month
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Sports' AS category,
# MAGIC     200 AS total_orders,
# MAGIC     80000.00 AS total_revenue,
# MAGIC     400.00 AS avg_order_value,
# MAGIC     '2026-04' AS month
# MAGIC ORDER BY total_revenue DESC;
# MAGIC
# MAGIC -- 📌 This query can be converted to a dashboard with:
# MAGIC -- 1. Bar chart: category vs total_revenue
# MAGIC -- 2. Table: showing all metrics
# MAGIC -- 3. KPI cards: total orders, total revenue
# MAGIC
# MAGIC -- 🎯 Real-world usage:
# MAGIC -- SELECT 
# MAGIC --     category,
# MAGIC --     COUNT(*) AS total_orders,
# MAGIC --     SUM(order_amount) AS total_revenue,
# MAGIC --     AVG(order_amount) AS avg_order_value,
# MAGIC --     DATE_FORMAT(order_date, 'yyyy-MM') AS month
# MAGIC -- FROM catalog.schema.gold_sales
# MAGIC -- WHERE order_date >= CURRENT_DATE - INTERVAL 30 DAYS
# MAGIC -- GROUP BY category, DATE_FORMAT(order_date, 'yyyy-MM')
# MAGIC -- ORDER BY total_revenue DESC;