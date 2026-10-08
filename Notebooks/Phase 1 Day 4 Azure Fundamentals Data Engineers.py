# Databricks notebook source
# DBTITLE 1,📘 Notebook Header
# MAGIC %md
# MAGIC # ☁️ Data Engineering Training — Phase 1 Day 4  
# MAGIC ## 🔐 Azure Fundamentals for Data Engineers  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - Azure Architecture & Core Services  
# MAGIC - Compute, Storage, Networking  
# MAGIC - Resource Groups & IAM (RBAC)  
# MAGIC - Security Fundamentals  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### ⚙️ Platform: Azure + Databricks (Serverless + Unity Catalog)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Objective:
# MAGIC Understand Azure core services and how they integrate with Databricks Lakehouse for building secure, scalable data platforms.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚠️ Engineering Constraints:
# MAGIC
# MAGIC ✅ **MUST USE:**
# MAGIC - Databricks Serverless Compute
# MAGIC - PySpark DataFrame API
# MAGIC - Unity Catalog Volumes for storage
# MAGIC - Delta Lake format
# MAGIC - Governance-first design
# MAGIC
# MAGIC ❌ **DO NOT USE:**
# MAGIC - RDDs
# MAGIC - cache() / persist()
# MAGIC - /tmp or local storage paths
# MAGIC - Hardcoded credentials
# MAGIC - Unmanaged data access

# COMMAND ----------

# DBTITLE 1,Section 1: Azure Architecture Overview
# MAGIC %md
# MAGIC # 🏛️ Section 1: Azure Architecture Overview
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌟 What is Azure?
# MAGIC
# MAGIC ### 👶 ELI5 (Explain Like I'm 5):
# MAGIC Imagine Azure is like a massive **digital warehouse** with rooms all over the world. Each room has:
# MAGIC - **Computers** to do work (Compute)
# MAGIC - **Storage boxes** to keep files (Storage)
# MAGIC - **Hallways** to connect rooms (Networking)
# MAGIC - **Security guards** to control who enters (Security)
# MAGIC
# MAGIC You don't buy the warehouse — you just rent the rooms you need!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC
# MAGIC Azure is Microsoft's **global cloud computing platform** providing:
# MAGIC - **200+ services** across compute, storage, networking, AI, analytics, and more
# MAGIC - **60+ regions** worldwide with multiple availability zones
# MAGIC - **99.99% SLA** for most services
# MAGIC - **Hybrid cloud** capabilities (on-premises + cloud)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌍 Azure Global Infrastructure
# MAGIC
# MAGIC ### Key Concepts:
# MAGIC
# MAGIC | Concept | Definition | Example |
# MAGIC |---------|------------|----------|
# MAGIC | **Region** | Physical location with multiple data centers | East US, West Europe, Southeast Asia |
# MAGIC | **Availability Zone** | Isolated data centers within a region | Zone 1, Zone 2, Zone 3 |
# MAGIC | **Geography** | Data residency boundary (compliance) | United States, European Union |
# MAGIC | **Region Pair** | Secondary region for disaster recovery | East US ↔ West US |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛡️ High Availability & Scalability:
# MAGIC
# MAGIC **High Availability:**
# MAGIC - Deploy across multiple availability zones
# MAGIC - Automatic failover and redundancy
# MAGIC - 99.99% uptime SLA
# MAGIC
# MAGIC **Scalability:**
# MAGIC - **Vertical scaling:** Increase VM size (scale up)
# MAGIC - **Horizontal scaling:** Add more VMs (scale out)
# MAGIC - **Elastic scaling:** Auto-scale based on demand
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📦 Azure Service Categories
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │         AZURE CLOUD PLATFORM              │
# MAGIC └────────────────────────────────────────┘
# MAGIC            │
# MAGIC    ┌───────┼───────┐
# MAGIC    │               │
# MAGIC ┌──┴──┐   ┌────┴────┐   ┌──────────┐
# MAGIC │ 🖥 │   │ 💾     │   │ 🌐      │
# MAGIC │Comp│   │Storage │   │Networking│
# MAGIC └─────┘   └─────────┘   └──────────┘
# MAGIC    │               │
# MAGIC ┌──┴──┐   ┌────┴────┐
# MAGIC │ 🔐 │   │ 📊     │
# MAGIC │ IAM │   │Analytics│
# MAGIC └─────┘   └─────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👉 How Databricks Fits into Azure Ecosystem:
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────┐
# MAGIC │  AZURE SUBSCRIPTION                                   │
# MAGIC │                                                      │
# MAGIC │  ┌────────────────────────────────────────┐  │
# MAGIC │  │  RESOURCE GROUP (e.g., rg-dataplatform)      │  │
# MAGIC │  │                                              │  │
# MAGIC │  │  ┌────────────────────────────────┐  │  │
# MAGIC │  │  │  ADLS Gen2 (Storage Account)      │  │  │
# MAGIC │  │  │  - Raw Data                       │  │  │
# MAGIC │  │  │  - Bronze/Silver/Gold             │  │  │
# MAGIC │  │  └────────────────────────────────┘  │  │
# MAGIC │  │           ↓                              │  │
# MAGIC │  │  ┌────────────────────────────────┐  │  │
# MAGIC │  │  │  Azure Databricks Workspace       │  │  │
# MAGIC │  │  │  - Serverless Compute             │  │  │
# MAGIC │  │  │  - Unity Catalog                  │  │  │
# MAGIC │  │  │  - Delta Lake                     │  │  │
# MAGIC │  │  └────────────────────────────────┘  │  │
# MAGIC │  │                                              │  │
# MAGIC │  │  ┌────────────────────────────────┐  │  │
# MAGIC │  │  │  Azure Key Vault (Secrets)        │  │  │
# MAGIC │  │  └────────────────────────────────┘  │  │
# MAGIC │  │                                              │  │
# MAGIC │  │  ┌────────────────────────────────┐  │  │
# MAGIC │  │  │  Virtual Network (VNet)           │  │  │
# MAGIC │  │  └────────────────────────────────┘  │  │
# MAGIC │  └────────────────────────────────────────┘  │
# MAGIC └──────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 2: Compute, Storage, Networking
# MAGIC %md
# MAGIC # 🖥️ Section 2: Compute, Storage, Networking
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 👉 PART A: Compute Services
# MAGIC
# MAGIC ### 👶 ELI5:
# MAGIC Compute = **The workers who do the job**  
# MAGIC Imagine you need to build a LEGO castle. You need:
# MAGIC - **Workers** (computers) to put blocks together
# MAGIC - **Different workers** for different jobs (some fast, some strong)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Azure Compute Options:
# MAGIC
# MAGIC | Service | Purpose | Data Engineering Use Case |
# MAGIC |---------|---------|---------------------------|
# MAGIC | **Azure VMs** | Virtual machines in the cloud | Run custom ETL tools, legacy apps |
# MAGIC | **Azure Databricks** | Unified analytics platform | **PRIMARY: Data processing, ML, analytics** |
# MAGIC | **Azure Functions** | Serverless event-driven compute | Trigger pipelines, lightweight transforms |
# MAGIC | **Azure Container Instances** | Run containers without orchestration | Microservices, batch jobs |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⭐ Azure Databricks Compute Models:
# MAGIC
# MAGIC #### 1️⃣ **Serverless Compute** (✅ RECOMMENDED)
# MAGIC - **What:** Instant, auto-managed compute
# MAGIC - **When:** Default choice for notebooks, queries, pipelines
# MAGIC - **Benefits:**
# MAGIC   - No cluster management
# MAGIC   - Auto-scaling
# MAGIC   - Pay-per-second billing
# MAGIC   - Latest runtime versions
# MAGIC
# MAGIC #### 2️⃣ **Classic Clusters** (Legacy)
# MAGIC - **What:** User-managed clusters
# MAGIC - **When:** Special configs, custom libraries
# MAGIC - **Drawbacks:** Manual management, fixed capacity
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💾 PART B: Storage Services
# MAGIC
# MAGIC ### 👶 ELI5:
# MAGIC Storage = **The boxes where you keep your LEGO blocks**  
# MAGIC - Some boxes for **raw pieces** (data lake)
# MAGIC - Some boxes for **sorted pieces** (organized data)
# MAGIC - Some boxes for **completed models** (processed data)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Azure Storage for Data Engineering:
# MAGIC
# MAGIC | Service | Purpose | Data Pattern | Unity Catalog Mapping |
# MAGIC |---------|---------|--------------|------------------------|
# MAGIC | **ADLS Gen2** | Hierarchical data lake | Large-scale analytics | **External Locations** |
# MAGIC | **Blob Storage** | Object storage | Unstructured files | Legacy, less common |
# MAGIC | **Azure Files** | Managed file shares | Shared config files | Not typically used |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⭐ ADLS Gen2 Architecture:
# MAGIC
# MAGIC ```
# MAGIC Storage Account: mystorageaccount.dfs.core.windows.net
# MAGIC │
# MAGIC ├── Container: bronze (raw data)
# MAGIC │   ├── source_system_1/
# MAGIC │   └── source_system_2/
# MAGIC │
# MAGIC ├── Container: silver (cleaned)
# MAGIC │   ├── customers/
# MAGIC │   └── transactions/
# MAGIC │
# MAGIC └── Container: gold (aggregated)
# MAGIC     ├── daily_metrics/
# MAGIC     └── customer_360/
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔗 Unity Catalog Integration:
# MAGIC
# MAGIC **ADLS Gen2** → **External Location** → **Unity Catalog Volume/Table**
# MAGIC
# MAGIC ```python
# MAGIC # Traditional ADLS path (OLD - avoid)
# MAGIC abfss://container@storage.dfs.core.windows.net/path
# MAGIC
# MAGIC # Unity Catalog Volume (NEW - preferred)
# MAGIC /Volumes/catalog_name/schema_name/volume_name/file.parquet
# MAGIC ```
# MAGIC
# MAGIC **⚠️ Key Principle:** Use Unity Catalog Volumes to abstract storage paths!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌐 PART C: Networking
# MAGIC
# MAGIC ### 👶 ELI5:
# MAGIC Networking = **The roads connecting buildings**  
# MAGIC - **Public roads:** Anyone can use (public internet)
# MAGIC - **Private roads:** Only special cars allowed (VNet)
# MAGIC - **Tunnels:** Secret underground paths (Private endpoints)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Azure Networking Fundamentals:
# MAGIC
# MAGIC | Component | Purpose | Example |
# MAGIC |-----------|---------|----------|
# MAGIC | **Virtual Network (VNet)** | Isolated network in Azure | 10.0.0.0/16 |
# MAGIC | **Subnet** | Segment within VNet | 10.0.1.0/24 (Databricks control plane) |
# MAGIC | **Private Endpoint** | Private connection to PaaS services | ADLS access without public internet |
# MAGIC | **Network Security Group (NSG)** | Firewall rules | Allow port 443 from specific IPs |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔒 Databricks + ADLS Connectivity:
# MAGIC
# MAGIC #### 🔴 Public Access (Default):
# MAGIC ```
# MAGIC Databricks → Public Internet → ADLS (public endpoint)
# MAGIC ```
# MAGIC
# MAGIC #### 🟢 Private Access (Enterprise):
# MAGIC ```
# MAGIC Databricks → VNet → Private Endpoint → ADLS (no public internet)
# MAGIC ```
# MAGIC
# MAGIC **⚠️ Production Best Practice:** Use private endpoints for all storage accounts!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Key Networking Principles for Data Engineers:
# MAGIC
# MAGIC 1. **Separation of Concerns:**
# MAGIC    - Databricks workspace VNet
# MAGIC    - Storage account private endpoints
# MAGIC    - Separate subnets for different services
# MAGIC
# MAGIC 2. **Security:**
# MAGIC    - Disable public access to storage accounts
# MAGIC    - Use private endpoints
# MAGIC    - Implement network security groups
# MAGIC
# MAGIC 3. **Performance:**
# MAGIC    - Keep compute and storage in same region
# MAGIC    - Use Azure Backbone (private network) when possible

# COMMAND ----------

# DBTITLE 1,Section 3: Resource Groups & IAM (RBAC)
# MAGIC %md
# MAGIC # 📁 Section 3: Resource Groups & IAM (RBAC)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏷️ Resource Groups
# MAGIC
# MAGIC ### 👶 ELI5:
# MAGIC Resource Group = **A labeled box for your toys**  
# MAGIC Instead of toys scattered everywhere, you put:
# MAGIC - All LEGO blocks in one box
# MAGIC - All action figures in another box
# MAGIC - Each box has a label
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect Explanation:
# MAGIC
# MAGIC **Resource Group** = Logical container for Azure resources that share:
# MAGIC - **Lifecycle:** Deployed, managed, deleted together
# MAGIC - **Permissions:** Common access control
# MAGIC - **Location:** Metadata stored in specific region (resources can be anywhere)
# MAGIC - **Billing:** Cost tracking and tagging
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Azure Hierarchy:
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │  Management Group (Optional)             │
# MAGIC │  (Organizational hierarchy)              │
# MAGIC └────────────────────────────────────────┘
# MAGIC                   │
# MAGIC                   ↓
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │  Subscription                            │
# MAGIC │  (Billing boundary)                      │
# MAGIC └────────────────────────────────────────┘
# MAGIC                   │
# MAGIC        ┌──────────┼──────────┐
# MAGIC        │                │
# MAGIC        ↓                ↓
# MAGIC ┌─────────────┐  ┌─────────────┐
# MAGIC │ Resource     │  │ Resource     │
# MAGIC │ Group 1      │  │ Group 2      │
# MAGIC │ (rg-prod)    │  │ (rg-dev)     │
# MAGIC └─────────────┘  └─────────────┘
# MAGIC        │                │
# MAGIC        ↓                ↓
# MAGIC   Resources          Resources
# MAGIC   (Storage,          (Databricks,
# MAGIC    Databricks)        VNet)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📝 Naming Convention Example:
# MAGIC
# MAGIC ```
# MAGIC rg-<workload>-<environment>-<region>-<instance>
# MAGIC
# MAGIC Examples:
# MAGIC - rg-dataplatform-prod-eastus-001
# MAGIC - rg-analytics-dev-westeurope-001
# MAGIC - rg-lakehouse-staging-southeastasia-001
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔐 Identity & Access Management (IAM / RBAC)
# MAGIC
# MAGIC ### 👶 ELI5:
# MAGIC RBAC = **Permission slips for different people**  
# MAGIC - **Principal:** Who (person, app, service)
# MAGIC - **Role:** What they can do (read, write, delete)
# MAGIC - **Scope:** Where they can do it (which box/resource)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Role-Based Access Control (RBAC):
# MAGIC
# MAGIC **Formula:**  
# MAGIC `Security Principal + Role + Scope = Permission`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📄 Built-in Azure Roles (Common):
# MAGIC
# MAGIC | Role | Permissions | Use Case |
# MAGIC |------|-------------|----------|
# MAGIC | **Owner** | Full control (incl. access mgmt) | Subscription admin |
# MAGIC | **Contributor** | Create/manage resources (no access mgmt) | Developers, data engineers |
# MAGIC | **Reader** | View resources only | Auditors, monitoring |
# MAGIC | **Storage Blob Data Contributor** | Read/write blobs | Databricks service principal |
# MAGIC | **Storage Blob Data Reader** | Read blobs only | Read-only analytics |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Principle of Least Privilege:
# MAGIC
# MAGIC **❌ Bad:**
# MAGIC ```
# MAGIC Grant "Owner" to everyone on the subscription
# MAGIC ```
# MAGIC
# MAGIC **✅ Good:**
# MAGIC ```
# MAGIC Data Engineer → Contributor on rg-dataplatform
# MAGIC Analyst → Reader on rg-dataplatform
# MAGIC Databricks Service Principal → Storage Blob Data Contributor on storage account
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Azure IAM vs Unity Catalog Permissions
# MAGIC
# MAGIC ### Two Layers of Security:
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────┐
# MAGIC │  LAYER 1: Azure RBAC (Infrastructure)         │
# MAGIC │  - Controls access to Azure resources         │
# MAGIC │  - Storage accounts, Databricks workspace     │
# MAGIC │  - Managed at Azure Portal / ARM level        │
# MAGIC └────────────────────────────────────────────┘
# MAGIC                        ↓
# MAGIC ┌────────────────────────────────────────────┐
# MAGIC │  LAYER 2: Unity Catalog (Data Governance)     │
# MAGIC │  - Controls access to data objects            │
# MAGIC │  - Catalogs, schemas, tables, columns         │
# MAGIC │  - Managed within Databricks                  │
# MAGIC └────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Comparison Table:
# MAGIC
# MAGIC | Aspect | Azure RBAC | Unity Catalog |
# MAGIC |--------|------------|---------------|
# MAGIC | **Scope** | Azure resources | Data objects |
# MAGIC | **Granularity** | Resource/Resource Group | Catalog/Schema/Table/Column |
# MAGIC | **Example** | "Contributor on Storage Account" | "SELECT on schema.table" |
# MAGIC | **Managed In** | Azure Portal | Databricks |
# MAGIC | **Purpose** | Infrastructure access | Data access |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✅ Best Practice:
# MAGIC
# MAGIC 1. **Azure RBAC:** Grant Databricks service principal access to ADLS
# MAGIC 2. **Unity Catalog:** Grant users access to specific tables/schemas
# MAGIC 3. **Result:** Secure, governed data access without direct storage credentials

# COMMAND ----------

# DBTITLE 1,Section 4: Security Fundamentals
# MAGIC %md
# MAGIC # 🔒 Section 4: Security Fundamentals
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ The 4 Pillars of Azure Security
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────┐
# MAGIC │         AZURE SECURITY ARCHITECTURE          │
# MAGIC └────────────────────────────────────────────┘
# MAGIC                     │
# MAGIC         ┌───────────┼───────────┐
# MAGIC         │                │
# MAGIC    ┌────┴────┐      ┌────┴────┐
# MAGIC    │           │      │           │
# MAGIC ┌──┴──┐   ┌──┴──┐   ┌──┴──┐   ┌──┴──┐
# MAGIC │     │   │     │   │     │   │     │
# MAGIC │  1  │   │  2  │   │  3  │   │  4  │
# MAGIC │ ID  │   │ IAM │   │ ENC │   │ SEC │
# MAGIC │     │   │     │   │     │   │     │
# MAGIC └─────┘   └─────┘   └─────┘   └─────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1️⃣ Identity (Azure AD / Entra ID)
# MAGIC
# MAGIC ### 👶 ELI5:
# MAGIC Identity = **Your ID card that proves who you are**  
# MAGIC - Name badge at school
# MAGIC - Password to login
# MAGIC - Fingerprint scanner
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Azure Entra ID (formerly Azure AD):
# MAGIC
# MAGIC **What:** Cloud-based identity and access management service
# MAGIC
# MAGIC **Key Concepts:**
# MAGIC
# MAGIC | Concept | Description | Example |
# MAGIC |---------|-------------|----------|
# MAGIC | **User** | Human identity | john.doe@company.com |
# MAGIC | **Service Principal** | Application identity | Databricks workspace identity |
# MAGIC | **Managed Identity** | Auto-managed identity for Azure resources | VM accessing Key Vault |
# MAGIC | **Group** | Collection of users | data-engineering-team |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔐 Authentication Methods:
# MAGIC
# MAGIC ```
# MAGIC ✅ Multi-Factor Authentication (MFA)
# MAGIC ✅ Conditional Access Policies
# MAGIC ✅ Single Sign-On (SSO)
# MAGIC ✅ OAuth 2.0 / OpenID Connect
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2️⃣ Access Control (IAM / RBAC)
# MAGIC
# MAGIC ### Already covered in Section 3!
# MAGIC
# MAGIC **Key Reminder:**
# MAGIC - Use **Principle of Least Privilege**
# MAGIC - Separate **Azure RBAC** (infrastructure) from **Unity Catalog** (data)
# MAGIC - Use **Service Principals** for application access
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3️⃣ Encryption
# MAGIC
# MAGIC ### 👶 ELI5:
# MAGIC Encryption = **Secret code for your messages**  
# MAGIC - You write: "Meet at park"
# MAGIC - Encrypted: "Xjju bu qbsl"
# MAGIC - Only your friend with the key can decode it
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Two Types of Encryption:
# MAGIC
# MAGIC #### 🔒 Encryption at Rest
# MAGIC **What:** Data stored on disk is encrypted
# MAGIC
# MAGIC **Azure Implementation:**
# MAGIC - **ADLS Gen2:** Enabled by default (256-bit AES)
# MAGIC - **Databricks:** DBFS encrypted
# MAGIC - **Key Management:** Azure Key Vault or customer-managed keys
# MAGIC
# MAGIC ```
# MAGIC Data on Disk → [Encrypted] → Unreadable without key
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 🔒 Encryption in Transit
# MAGIC **What:** Data moving over network is encrypted
# MAGIC
# MAGIC **Azure Implementation:**
# MAGIC - **TLS/SSL:** All Azure services use HTTPS
# MAGIC - **Private Endpoints:** Data never leaves Azure backbone
# MAGIC
# MAGIC ```
# MAGIC Client → [HTTPS/TLS] → Azure Service
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Encryption Summary:
# MAGIC
# MAGIC | Layer | Method | Azure Service |
# MAGIC |-------|--------|---------------|
# MAGIC | **Storage** | AES-256 | ADLS Gen2, Managed Disks |
# MAGIC | **Network** | TLS 1.2+ | All Azure traffic |
# MAGIC | **Application** | End-to-end | Databricks notebooks |
# MAGIC | **Key Management** | Centralized | Azure Key Vault |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4️⃣ Secrets Management
# MAGIC
# MAGIC ### 👶 ELI5:
# MAGIC Secrets = **Passwords you should never write down**  
# MAGIC Key Vault = **A super secure safe that remembers passwords for you**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Azure Key Vault:
# MAGIC
# MAGIC **Purpose:** Centralized secrets, keys, and certificate management
# MAGIC
# MAGIC **What You Store:**
# MAGIC - Database connection strings
# MAGIC - API keys
# MAGIC - Storage account keys
# MAGIC - Certificates
# MAGIC - Encryption keys
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Never Do This:
# MAGIC
# MAGIC ```python
# MAGIC # WRONG - Hardcoded credentials
# MAGIC storage_account_key = "abc123secretkey456"
# MAGIC spark.conf.set("fs.azure.account.key.mystorageaccount.dfs.core.windows.net", 
# MAGIC                storage_account_key)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✅ Correct Approach:
# MAGIC
# MAGIC ```python
# MAGIC # RIGHT - Use Databricks secrets (backed by Key Vault)
# MAGIC storage_account_key = dbutils.secrets.get(scope="azure-key-vault", key="storage-key")
# MAGIC spark.conf.set("fs.azure.account.key.mystorageaccount.dfs.core.windows.net", 
# MAGIC                storage_account_key)
# MAGIC ```
# MAGIC
# MAGIC **⭐ Even Better:** Use Unity Catalog (no credentials needed!)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Azure Security → Databricks Mapping
# MAGIC
# MAGIC | Azure Security | Databricks Implementation |
# MAGIC |----------------|---------------------------|
# MAGIC | **Entra ID** | Workspace authentication, SCIM sync |
# MAGIC | **RBAC** | Workspace admin, Unity Catalog admin |
# MAGIC | **Key Vault** | Databricks secrets (backed by Key Vault) |
# MAGIC | **Private Endpoints** | Secure connectivity to ADLS |
# MAGIC | **Encryption** | DBFS encryption, Unity Catalog encryption |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚪 Secure Data Access Pattern
# MAGIC
# MAGIC ### 🔴 Traditional (Complex, Insecure):
# MAGIC
# MAGIC ```
# MAGIC User → Storage Key → Direct ADLS Access
# MAGIC - Keys exposed in notebooks
# MAGIC - No audit trail
# MAGIC - No fine-grained permissions
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟢 Unity Catalog (Modern, Secure):
# MAGIC
# MAGIC ```
# MAGIC User → Unity Catalog → ADLS (via managed identity)
# MAGIC - No keys in notebooks
# MAGIC - Full audit trail
# MAGIC - Table/column-level permissions
# MAGIC - Automatic encryption
# MAGIC ```
# MAGIC
# MAGIC **⭐ This is the pattern we'll use in hands-on demo!**

# COMMAND ----------

# DBTITLE 1,Section 5: Hands-on Simulation Setup
# MAGIC %md
# MAGIC # 🛠️ Section 5: Hands-on Simulation (Databricks + Azure)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Objective:
# MAGIC Simulate Azure storage access using Unity Catalog Volumes to demonstrate:
# MAGIC 1. Secure data access (no credentials)
# MAGIC 2. Reading data from volumes
# MAGIC 3. Writing Delta tables
# MAGIC 4. Governance-first approach
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📌 Prerequisites:
# MAGIC - Databricks workspace (serverless compute)
# MAGIC - Unity Catalog enabled
# MAGIC - Access to a catalog and schema
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 What We'll Build:
# MAGIC
# MAGIC ```
# MAGIC Azure ADLS Gen2 (External Storage)
# MAGIC         ↓
# MAGIC Unity Catalog Volume
# MAGIC         ↓
# MAGIC Read Data (PySpark)
# MAGIC         ↓
# MAGIC Transform Data
# MAGIC         ↓
# MAGIC Write Delta Table (Managed)
# MAGIC         ↓
# MAGIC Query with Governance
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Let's Start!
# MAGIC
# MAGIC In the next cells, we'll:
# MAGIC 1. Create sample data in memory
# MAGIC 2. Demonstrate secure patterns
# MAGIC 3. Write to Delta Lake
# MAGIC 4. Query the results

# COMMAND ----------

# DBTITLE 1,5.1: Setup - Create Sample Azure-style Data
# 📊 Create sample data simulating Azure data sources
# In production, this would come from ADLS Gen2 via Unity Catalog Volumes

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, expr, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from datetime import datetime, timedelta
import random

print("✅ Simulating Azure Data Platform Environment")
print("="*60)
print("📌 Context: Azure ADLS Gen2 → Unity Catalog → Delta Lake")
print("="*60)

# Simulate customer data from Azure source system
customer_data = [
    (1, "Alice Johnson", "alice@company.com", "Premium", "East US"),
    (2, "Bob Smith", "bob@company.com", "Standard", "West US"),
    (3, "Charlie Brown", "charlie@company.com", "Premium", "East US"),
    (4, "Diana Prince", "diana@company.com", "Enterprise", "West Europe"),
    (5, "Ethan Hunt", "ethan@company.com", "Standard", "Southeast Asia"),
    (6, "Fiona Apple", "fiona@company.com", "Premium", "East US"),
    (7, "George Martin", "george@company.com", "Enterprise", "West Europe"),
    (8, "Hannah Montana", "hannah@company.com", "Standard", "West US")
]

customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("subscription_tier", StringType(), True),
    StructField("azure_region", StringType(), True)
])

df_customers = spark.createDataFrame(customer_data, customer_schema)

# Add metadata columns (common pattern in Azure data pipelines)
df_customers = df_customers \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_system", lit("azure_crm")) \
    .withColumn("data_classification", lit("PII"))

print("\n📋 Sample Customer Data (simulating Azure ADLS source):")
display(df_customers)

print(f"\n✅ Created {df_customers.count()} customer records")
print("🔒 Note: In production, this data would be read from Unity Catalog Volumes")
print("🔒 Pattern: /Volumes/catalog_name/schema_name/volume_name/customers.parquet")

# COMMAND ----------

# DBTITLE 1,5.2: Demonstrate Secure Data Access Pattern
# 🔒 Demonstrate Secure Data Access Pattern
# Compare traditional vs Unity Catalog approach

print("🔒 AZURE + DATABRICKS SECURITY PATTERNS")
print("="*70)

# ❌ BAD PATTERN (Traditional - DO NOT USE)
print("\n❌ ANTI-PATTERN (Traditional ADLS Access):")
print("-" * 70)
print("# Hardcoded credentials - SECURITY RISK!")
print('storage_key = "abc123secretkey"  # NEVER DO THIS')
print('spark.conf.set("fs.azure.account.key.mystorageaccount.dfs.core.windows.net", storage_key)')
print('df = spark.read.parquet("abfss://container@storage.dfs.core.windows.net/data")')
print("\n⚠️  Problems:")
print("   - Credentials exposed in notebook")
print("   - No audit trail")
print("   - No fine-grained permissions")
print("   - Key rotation is difficult")

# ✅ GOOD PATTERN (Unity Catalog)
print("\n" + "="*70)
print("✅ RECOMMENDED PATTERN (Unity Catalog):")
print("-" * 70)
print("# No credentials needed - fully managed!")
print('df = spark.read.parquet("/Volumes/main/bronze/raw_data/customers.parquet")')
print("\n✅ Benefits:")
print("   ✓ No credentials in notebook")
print("   ✓ Full audit trail (who accessed what, when)")
print("   ✓ Table/column-level permissions")
print("   ✓ Automatic encryption")
print("   ✓ Centralized governance")

print("\n" + "="*70)
print("🎯 KEY TAKEAWAY:")
print("   Use Unity Catalog Volumes/Tables for ALL data access")
print("   Azure RBAC (infrastructure) + Unity Catalog (data) = Complete security")
print("="*70)

# Show Unity Catalog hierarchy
print("\n📊 Unity Catalog Hierarchy (Maps to Azure Resources):")
print("""
┌────────────────────────────────────────┐
│  METASTORE (Regional)                  │  ← Azure Region
│    │                                      │
│    ├── CATALOG (e.g., main)               │  ← Azure Storage Account
│    │     │                                │
│    │     ├── SCHEMA (e.g., bronze)       │  ← Container/Folder
│    │     │     │                        │
│    │     │     ├── TABLE              │  ← Delta Table
│    │     │     ├── VOLUME             │  ← ADLS Path
│    │     │     └── VIEW               │  ← Virtual Table
└────────────────────────────────────────┘

Permissions at each level:
- METASTORE: Admin access
- CATALOG: USE, CREATE SCHEMA
- SCHEMA: USE, CREATE TABLE
- TABLE/VOLUME: SELECT, INSERT, UPDATE, DELETE
""")

# COMMAND ----------

# DBTITLE 1,5.3: Transform Data (Business Logic)
# 🔧 Transform data - Apply business logic
# This represents the processing layer in Azure + Databricks architecture

from pyspark.sql.functions import when, length, upper, concat_ws

print("🔧 TRANSFORMATION LAYER (Silver)")
print("="*60)

# Apply business rules
df_transformed = df_customers \
    .withColumn(
        "tier_priority",
        when(col("subscription_tier") == "Enterprise", 1)
        .when(col("subscription_tier") == "Premium", 2)
        .when(col("subscription_tier") == "Standard", 3)
        .otherwise(4)
    ) \
    .withColumn(
        "region_code",
        when(col("azure_region").contains("East US"), "USE")
        .when(col("azure_region").contains("West US"), "USW")
        .when(col("azure_region").contains("West Europe"), "EUW")
        .when(col("azure_region").contains("Southeast Asia"), "ASE")
        .otherwise("UNK")
    ) \
    .withColumn(
        "customer_key",
        concat_ws("-", col("region_code"), col("customer_id"))
    ) \
    .withColumn("email_domain", expr("split(email, '@')[1]")) \
    .withColumn("is_enterprise", col("subscription_tier") == "Enterprise")

print("✅ Applied Transformations:")
print("   1. Tier priority ranking (Enterprise=1, Standard=3)")
print("   2. Region codes (USE, USW, EUW, ASE)")
print("   3. Customer key generation")
print("   4. Email domain extraction")
print("   5. Enterprise flag")

print("\n📋 Transformed Data (Silver Layer):")
display(df_transformed.select(
    "customer_id", "customer_name", "subscription_tier", 
    "tier_priority", "azure_region", "region_code", 
    "customer_key", "is_enterprise"
))

print(f"\n📊 Transformation Summary:")
print(f"   Total Records: {df_transformed.count()}")
print(f"   Enterprise Customers: {df_transformed.filter(col('is_enterprise')).count()}")
print(f"   Regions Covered: {df_transformed.select('azure_region').distinct().count()}")

# COMMAND ----------

# DBTITLE 1,Section 6: End-to-End Azure + Databricks Flow
# MAGIC %md
# MAGIC # 🔄 Section 6: End-to-End Azure + Databricks Flow
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏛️ Enterprise Data Pipeline Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────────────────┐
# MAGIC │                    DATA PIPELINE FLOW                         │
# MAGIC └────────────────────────────────────────────────────────┘
# MAGIC
# MAGIC ┌────────────────────────────────────────────────────────┐
# MAGIC │  📊 SOURCE: AZURE ADLS GEN2                                │
# MAGIC │  - Raw files (CSV, JSON, Parquet)                          │
# MAGIC │  - Streaming data                                          │
# MAGIC │  - External systems                                        │
# MAGIC └────────────────────────────────────────────────────────┘
# MAGIC                             │
# MAGIC                             ↓ (Secure access via Unity Catalog)
# MAGIC                             │
# MAGIC ┌────────────────────────────────────────────────────────┐
# MAGIC │  🟡 BRONZE LAYER (Raw)                                   │
# MAGIC │  - Exact copy of source                                    │
# MAGIC │  - Minimal transformation                                  │
# MAGIC │  - Delta format                                            │
# MAGIC │  Location: catalog.bronze.table_name                       │
# MAGIC └────────────────────────────────────────────────────────┘
# MAGIC                             │
# MAGIC                             ↓ (Databricks Serverless Processing)
# MAGIC                             │
# MAGIC ┌────────────────────────────────────────────────────────┐
# MAGIC │  🥈 SILVER LAYER (Cleaned & Validated)                 │
# MAGIC │  - Data quality checks                                     │
# MAGIC │  - Business rules applied                                  │
# MAGIC │  - Standardized schema                                     │
# MAGIC │  - Deduplication                                           │
# MAGIC │  Location: catalog.silver.table_name                       │
# MAGIC └────────────────────────────────────────────────────────┘
# MAGIC                             │
# MAGIC                             ↓ (Business Logic & Aggregations)
# MAGIC                             │
# MAGIC ┌────────────────────────────────────────────────────────┐
# MAGIC │  🥇 GOLD LAYER (Analytics-Ready)                       │
# MAGIC │  - Aggregated metrics                                      │
# MAGIC │  - Dimension tables                                        │
# MAGIC │  - Business KPIs                                           │
# MAGIC │  - Optimized for BI tools                                  │
# MAGIC │  Location: catalog.gold.table_name                         │
# MAGIC └────────────────────────────────────────────────────────┘
# MAGIC                             │
# MAGIC                             ↓
# MAGIC                             │
# MAGIC ┌────────────────────────────────────────────────────────┐
# MAGIC │  📊 ANALYTICS LAYER                                     │
# MAGIC │  - Power BI / Tableau                                      │
# MAGIC │  - Databricks SQL Dashboards                               │
# MAGIC │  - ML Models                                               │
# MAGIC │  - Ad-hoc queries                                          │
# MAGIC └────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔑 Key Principles:
# MAGIC
# MAGIC ### 1️⃣ Separation of Compute & Storage
# MAGIC ```
# MAGIC ✅ Storage: ADLS Gen2 (persistent, scalable)
# MAGIC ✅ Compute: Databricks Serverless (elastic, on-demand)
# MAGIC ✅ Benefit: Scale independently, cost-efficient
# MAGIC ```
# MAGIC
# MAGIC ### 2️⃣ Layered Architecture (Medallion)
# MAGIC ```
# MAGIC Bronze → Silver → Gold
# MAGIC  Raw  → Cleaned → Analytics
# MAGIC ```
# MAGIC
# MAGIC ### 3️⃣ Governance-First
# MAGIC ```
# MAGIC Unity Catalog controls:
# MAGIC - WHO can access (RBAC)
# MAGIC - WHAT they can see (row/column filters)
# MAGIC - WHEN they accessed (audit logs)
# MAGIC ```
# MAGIC
# MAGIC ### 4️⃣ Security by Default
# MAGIC ```
# MAGIC - No credentials in notebooks
# MAGIC - Encrypted at rest & in transit
# MAGIC - Private networking
# MAGIC - Centralized secret management
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,6.1: End-to-End Demo - Create Analytics Table
# 🚀 End-to-End Demo: Write to Delta Lake
# Simulating complete Azure → Databricks → Delta pipeline

from pyspark.sql.functions import count, sum as spark_sum, avg, max as spark_max

print("🚀 COMPLETE AZURE + DATABRICKS PIPELINE DEMO")
print("="*70)

# Step 1: Bronze Layer (we already have df_customers from earlier)
print("\n🟡 STEP 1: Bronze Layer (Raw Data)")
print("-" * 70)
print(f"Source: Simulated Azure ADLS Gen2 data")
print(f"Records: {df_customers.count()}")
print("📋 Schema:")
df_customers.printSchema()

# Step 2: Silver Layer (we already have df_transformed)
print("\n🥈 STEP 2: Silver Layer (Cleaned & Validated)")
print("-" * 70)
print("Transformations applied:")
print("  ✓ Tier priority ranking")
print("  ✓ Region standardization")
print("  ✓ Business key generation")
print("  ✓ Email domain extraction")
print(f"Records: {df_transformed.count()}")

# Step 3: Gold Layer - Create aggregated metrics
print("\n🥇 STEP 3: Gold Layer (Analytics-Ready)")
print("-" * 70)

# Aggregate by subscription tier and region
df_gold_summary = df_transformed.groupBy(
    "subscription_tier", 
    "azure_region",
    "region_code"
).agg(
    count("customer_id").alias("customer_count"),
    avg("tier_priority").alias("avg_priority")
).orderBy("subscription_tier", "azure_region")

print("📊 Customer Summary by Tier and Region:")
display(df_gold_summary)

# Create a dimension table for regions
df_region_dim = df_transformed.select(
    "azure_region",
    "region_code"
).distinct().orderBy("azure_region")

print("\n🌍 Region Dimension Table:")
display(df_region_dim)

# Final metrics
print("\n📊 GOLD LAYER METRICS:")
print("="*70)
print(f"Total Customers: {df_transformed.count()}")
print(f"Unique Regions: {df_transformed.select('azure_region').distinct().count()}")
print(f"Enterprise Customers: {df_transformed.filter(col('subscription_tier') == 'Enterprise').count()}")
print(f"Premium Customers: {df_transformed.filter(col('subscription_tier') == 'Premium').count()}")
print(f"Standard Customers: {df_transformed.filter(col('subscription_tier') == 'Standard').count()}")

print("\n✅ Pipeline Complete!")
print("💾 In production, these would be written to Unity Catalog managed tables")
print("🔒 Example: spark.write.format('delta').mode('overwrite').saveAsTable('catalog.gold.customer_summary')")

# COMMAND ----------

# DBTITLE 1,6.2: Demonstrate Unity Catalog Governance
# 🔐 Demonstrate Unity Catalog Governance Features
# Show how permissions would work in production

print("🔐 UNITY CATALOG GOVERNANCE FEATURES")
print("="*70)

print("\n1️⃣ TABLE-LEVEL PERMISSIONS:")
print("-" * 70)
print("""
-- Grant SELECT on specific tables
GRANT SELECT ON TABLE catalog.gold.customer_summary TO `analysts@company.com`;

-- Grant MODIFY on silver tables to data engineers
GRANT SELECT, MODIFY ON SCHEMA catalog.silver TO `data-engineers`;

-- Revoke access
REVOKE SELECT ON TABLE catalog.gold.customer_summary FROM `analysts@company.com`;
""")

print("\n2️⃣ COLUMN-LEVEL PERMISSIONS:")
print("-" * 70)
print("""
-- Hide PII columns from certain users
CREATE VIEW catalog.gold.customer_summary_public AS
SELECT 
  customer_id,
  subscription_tier,
  azure_region,
  tier_priority
  -- email excluded (PII)
FROM catalog.silver.customers;

GRANT SELECT ON VIEW catalog.gold.customer_summary_public TO `public-analysts`;
""")

print("\n3️⃣ ROW-LEVEL SECURITY (RLS):")
print("-" * 70)
print("""
-- Users only see data from their region
CREATE FUNCTION catalog.default.current_user_region()
RETURNS STRING
RETURN 
  CASE 
    WHEN current_user() LIKE '%eastus%' THEN 'East US'
    WHEN current_user() LIKE '%westus%' THEN 'West US'
    ELSE 'ALL'
  END;

-- Apply row filter
ALTER TABLE catalog.silver.customers 
SET ROW FILTER current_user_region() ON azure_region;
""")

print("\n4️⃣ AUDIT LOGGING:")
print("-" * 70)
print("""
-- Audit logs automatically capture:
✓ Who accessed the data (user/service principal)
✓ What table/column they accessed
✓ When they accessed it (timestamp)
✓ What operation (SELECT, INSERT, UPDATE, DELETE)
✓ From where (IP address, notebook)

-- Query audit logs
SELECT 
  event_time,
  user_identity.email,
  request_params.full_name_arg AS table_name,
  action_name
FROM system.access.audit
WHERE action_name IN ('getTable', 'readTable')
  AND event_date >= current_date() - 7
ORDER BY event_time DESC;
""")

print("\n5️⃣ DATA LINEAGE:")
print("-" * 70)
print("""
Unity Catalog automatically tracks:
✓ Where data came from (source tables/files)
✓ What transformations were applied
✓ Where data flows to (downstream tables/dashboards)
✓ Which notebooks/jobs access the data

-- Visual lineage available in Databricks UI:
Catalog Explorer → Select Table → Lineage Tab
""")

print("\n" + "="*70)
print("🎯 KEY GOVERNANCE BENEFITS:")
print("="*70)
print("""
✅ Fine-grained access control (table/column/row level)
✅ Complete audit trail (compliance ready)
✅ Automatic lineage tracking
✅ Centralized metadata management
✅ No credentials in notebooks
✅ Works across clouds (Azure, AWS, GCP)
""")

# COMMAND ----------

# DBTITLE 1,Section 7: Genie Code Agent Usage
# MAGIC %md
# MAGIC # 🧞 Databricks Genie Code Agent - Your AI Assistant
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 What is Genie Code?
# MAGIC
# MAGIC Databricks Genie Code is an AI-powered coding assistant that helps you:
# MAGIC - Write PySpark and SQL code
# MAGIC - Debug errors
# MAGIC - Optimize queries
# MAGIC - Generate documentation
# MAGIC - Learn best practices
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💬 Example Prompts for Azure + Databricks:
# MAGIC
# MAGIC ### 🟢 Architecture & Design:
# MAGIC
# MAGIC ```
# MAGIC "Explain Azure architecture for data engineers"
# MAGIC
# MAGIC "How does Databricks integrate with ADLS Gen2?"
# MAGIC
# MAGIC "Design a medallion architecture for customer data in Azure"
# MAGIC
# MAGIC "What's the difference between Azure RBAC and Unity Catalog permissions?"
# MAGIC
# MAGIC "Recommend Azure services for a real-time streaming pipeline"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔵 Security & Governance:
# MAGIC
# MAGIC ```
# MAGIC "Generate secure data pipeline using Azure + Databricks"
# MAGIC
# MAGIC "How do I access ADLS without hardcoding credentials?"
# MAGIC
# MAGIC "Map Azure RBAC to Unity Catalog permissions for my team"
# MAGIC
# MAGIC "Show me row-level security examples in Unity Catalog"
# MAGIC
# MAGIC "Create audit query to track who accessed PII tables"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟡 Code Generation:
# MAGIC
# MAGIC ```
# MAGIC "Read parquet files from Unity Catalog Volume and write to Delta table"
# MAGIC
# MAGIC "Create bronze/silver/gold pipeline for customer data"
# MAGIC
# MAGIC "Generate PySpark code to deduplicate data based on customer_id"
# MAGIC
# MAGIC "Write SQL to aggregate sales by region and tier"
# MAGIC
# MAGIC "Create a slowly changing dimension (SCD Type 2) implementation"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟠 Debugging & Optimization:
# MAGIC
# MAGIC ```
# MAGIC "Why is my Spark job slow? Analyze the query plan"
# MAGIC
# MAGIC "Fix this error: AnalysisException: Table or view not found"
# MAGIC
# MAGIC "Optimize this query for better performance"
# MAGIC
# MAGIC "How can I reduce data skew in my join operation?"
# MAGIC
# MAGIC "Explain this Spark execution plan"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟣 Learning & Documentation:
# MAGIC
# MAGIC ```
# MAGIC "What are Azure availability zones?"
# MAGIC
# MAGIC "Explain serverless compute vs classic clusters"
# MAGIC
# MAGIC "Show me Unity Catalog best practices"
# MAGIC
# MAGIC "What's the difference between ADLS Gen1 and Gen2?"
# MAGIC
# MAGIC "How does Delta Lake handle concurrent writes?"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⭐ Best Practices for Using Genie Code:
# MAGIC
# MAGIC ### ✅ DO:
# MAGIC * **Be specific:** "Create a PySpark DataFrame with customer data and aggregate by region" is better than "make a dataframe"
# MAGIC * **Provide context:** Mention if you're using Unity Catalog, serverless compute, etc.
# MAGIC * **Ask follow-ups:** "Now add error handling" or "Optimize this for large datasets"
# MAGIC * **Request explanations:** "Explain how this code works" to learn
# MAGIC
# MAGIC ### ❌ DON'T:
# MAGIC * **Be vague:** "Write some code" doesn't give enough context
# MAGIC * **Ignore constraints:** Always mention serverless, Unity Catalog requirements
# MAGIC * **Skip validation:** Always review and test generated code
# MAGIC * **Assume it knows your data:** Describe your schema and requirements
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Try It Now!
# MAGIC
# MAGIC Type any of the example prompts above in the chat, or ask your own questions about:
# MAGIC - Azure services
# MAGIC - Databricks features  
# MAGIC - PySpark/SQL code
# MAGIC - Data engineering patterns
# MAGIC - Security and governance
# MAGIC
# MAGIC **Genie Code learns from your context and helps you build production-quality data pipelines!**

# COMMAND ----------

# DBTITLE 1,Summary & Key Takeaways
# MAGIC %md
# MAGIC # 🎓 Summary & Key Takeaways
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Key Learnings from Phase 1 - Day 4:
# MAGIC
# MAGIC ### 1️⃣ Azure Architecture
# MAGIC * Azure is a global cloud platform with **60+ regions** and **200+ services**
# MAGIC * Key concepts: Regions, Availability Zones, Resource Groups, Subscriptions
# MAGIC * High availability through redundancy and multi-zone deployment
# MAGIC * Databricks is a **native Azure service** integrated into the ecosystem
# MAGIC
# MAGIC ### 2️⃣ Core Services for Data Engineering
# MAGIC * **Compute:** Azure Databricks (Serverless preferred), VMs, Functions
# MAGIC * **Storage:** ADLS Gen2 (primary for data lakes), Blob Storage
# MAGIC * **Networking:** VNets, Private Endpoints, NSGs for secure connectivity
# MAGIC * **Integration:** Unity Catalog abstracts Azure storage for governance
# MAGIC
# MAGIC ### 3️⃣ Resource Management
# MAGIC * **Resource Groups** organize related resources with shared lifecycle
# MAGIC * **Naming conventions** are critical for maintainability
# MAGIC * **Azure hierarchy:** Management Group → Subscription → Resource Group → Resources
# MAGIC
# MAGIC ### 4️⃣ Security & Governance
# MAGIC * **Identity:** Azure Entra ID (formerly Azure AD) for authentication
# MAGIC * **Access Control:** Azure RBAC for infrastructure, Unity Catalog for data
# MAGIC * **Encryption:** At rest (AES-256) and in transit (TLS) by default
# MAGIC * **Secrets:** Azure Key Vault integrated with Databricks secrets
# MAGIC * **Two-layer security:** Azure RBAC + Unity Catalog = complete governance
# MAGIC
# MAGIC ### 5️⃣ Data Pipeline Architecture
# MAGIC * **Medallion Architecture:** Bronze (raw) → Silver (cleaned) → Gold (analytics)
# MAGIC * **Separation of concerns:** Compute (Databricks) independent from Storage (ADLS)
# MAGIC * **Unity Catalog:** Centralized metadata, permissions, audit, lineage
# MAGIC * **Security pattern:** No credentials in notebooks, everything via Unity Catalog
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Architecture Summary:
# MAGIC
# MAGIC ```
# MAGIC   AZURE CLOUD PLATFORM
# MAGIC          │
# MAGIC     ┌────┼────┐
# MAGIC     │         │
# MAGIC ┌───┴───┐ ┌───┴────────────┐
# MAGIC │ ADLS  │ │  Databricks     │
# MAGIC │ Gen2  │ │  Serverless     │
# MAGIC │       │ │  Unity Catalog  │
# MAGIC └───┬───┘ └───┬────────────┘
# MAGIC     │           │
# MAGIC     └─────┬─────┘
# MAGIC           │
# MAGIC      ┌────┴────┐
# MAGIC      │  Secure  │
# MAGIC      │  Access  │
# MAGIC      │  via UC  │
# MAGIC      └────┬────┘
# MAGIC           │
# MAGIC      ┌────┴────┐
# MAGIC      │  Delta   │
# MAGIC      │  Tables  │
# MAGIC      └─────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Best Practices Checklist:
# MAGIC
# MAGIC ```
# MAGIC ☑ Use Databricks Serverless (no cluster management)
# MAGIC ☑ Access storage via Unity Catalog (no credentials)
# MAGIC ☑ Implement medallion architecture (Bronze/Silver/Gold)
# MAGIC ☑ Apply principle of least privilege (RBAC)
# MAGIC ☑ Use private endpoints for production
# MAGIC ☑ Enable encryption at rest and in transit
# MAGIC ☑ Store secrets in Azure Key Vault
# MAGIC ☑ Use consistent naming conventions
# MAGIC ☑ Separate compute from storage
# MAGIC ☑ Implement comprehensive audit logging
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 Phase 1 Day 4 - Complete! ✅
# MAGIC
# MAGIC **Next Steps:**
# MAGIC * **Day 5:** Advanced Delta Lake and Data Governance
# MAGIC * **Day 6:** Streaming and Real-time Processing
# MAGIC * **Day 7:** Performance Optimization and Best Practices
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏆 You now understand:
# MAGIC ✓ Azure cloud fundamentals  
# MAGIC ✓ How Databricks integrates with Azure  
# MAGIC ✓ Security and governance best practices  
# MAGIC ✓ Production-ready data pipeline architecture  
# MAGIC
# MAGIC **Keep practicing and building! 🚀**

# COMMAND ----------

# DBTITLE 1,Interview Questions & Common Mistakes
# MAGIC %md
# MAGIC # 📝 Interview Questions & Common Mistakes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Interview Questions (Azure + Databricks):
# MAGIC
# MAGIC ### 🟢 Fundamentals (Junior Level):
# MAGIC
# MAGIC **Q1: What is the difference between Azure Blob Storage and ADLS Gen2?**
# MAGIC <details>
# MAGIC <summary>Click for Answer</summary>
# MAGIC
# MAGIC **Answer:**
# MAGIC * **Blob Storage:** Object storage, flat namespace, good for unstructured data
# MAGIC * **ADLS Gen2:** Hierarchical namespace (folder structure), optimized for big data analytics, better performance for Spark workloads, supports POSIX permissions
# MAGIC * **Key:** ADLS Gen2 = Blob Storage + Hierarchical Namespace + Analytics optimizations
# MAGIC </details>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q2: Explain Azure Resource Groups and their purpose.**
# MAGIC <details>
# MAGIC <summary>Click for Answer</summary>
# MAGIC
# MAGIC **Answer:**
# MAGIC * Logical container for grouping related Azure resources
# MAGIC * Resources share: lifecycle, permissions, billing tags, location (metadata)
# MAGIC * Best practice: Group resources by application, environment, or team
# MAGIC * Deleting a resource group deletes all contained resources
# MAGIC * RBAC can be applied at resource group level
# MAGIC </details>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q3: What are Azure Availability Zones?**
# MAGIC <details>
# MAGIC <summary>Click for Answer</summary>
# MAGIC
# MAGIC **Answer:**
# MAGIC * Physically separate data centers within an Azure region
# MAGIC * Each zone has independent power, cooling, networking
# MAGIC * Minimum 3 zones per supported region
# MAGIC * Purpose: Protect against data center failures
# MAGIC * Use case: Deploy VMs across zones for 99.99% SLA
# MAGIC </details>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟡 Intermediate (Mid-Level):
# MAGIC
# MAGIC **Q4: How does Databricks securely access ADLS Gen2 without credentials in notebooks?**
# MAGIC <details>
# MAGIC <summary>Click for Answer</summary>
# MAGIC
# MAGIC **Answer:**
# MAGIC 1. **Unity Catalog** uses a **Service Principal** or **Managed Identity**
# MAGIC 2. Service Principal is granted **Storage Blob Data Contributor** role on ADLS
# MAGIC 3. Unity Catalog creates **External Location** pointing to ADLS path
# MAGIC 4. Users access data via Unity Catalog tables/volumes
# MAGIC 5. **Result:** No credentials in notebooks, centralized access control
# MAGIC </details>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q5: What's the difference between Azure RBAC and Unity Catalog permissions?**
# MAGIC <details>
# MAGIC <summary>Click for Answer</summary>
# MAGIC
# MAGIC **Answer:**
# MAGIC * **Azure RBAC:**
# MAGIC   - Controls access to Azure resources (storage accounts, VMs)
# MAGIC   - Managed in Azure Portal
# MAGIC   - Roles: Owner, Contributor, Reader, Storage Blob Data Contributor
# MAGIC   
# MAGIC * **Unity Catalog:**
# MAGIC   - Controls access to data objects (catalogs, schemas, tables, columns)
# MAGIC   - Managed in Databricks
# MAGIC   - Permissions: SELECT, INSERT, MODIFY, CREATE, USE
# MAGIC   - Supports row/column-level security
# MAGIC
# MAGIC * **Relationship:** Azure RBAC grants Databricks access to storage; Unity Catalog governs user access to data
# MAGIC </details>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q6: Explain the Medallion Architecture (Bronze/Silver/Gold).**
# MAGIC <details>
# MAGIC <summary>Click for Answer</summary>
# MAGIC
# MAGIC **Answer:**
# MAGIC * **Bronze (Raw):**
# MAGIC   - Exact copy of source data
# MAGIC   - Minimal transformation
# MAGIC   - Preserves history
# MAGIC   - Delta format for time travel
# MAGIC
# MAGIC * **Silver (Cleaned):**
# MAGIC   - Data quality validation
# MAGIC   - Deduplication
# MAGIC   - Schema standardization
# MAGIC   - Business rules applied
# MAGIC   - Filtered/cleaned data
# MAGIC
# MAGIC * **Gold (Analytics-Ready):**
# MAGIC   - Aggregated metrics
# MAGIC   - Business KPIs
# MAGIC   - Dimension tables
# MAGIC   - Optimized for BI tools
# MAGIC   - Often denormalized
# MAGIC
# MAGIC **Benefits:** Clear separation of concerns, data quality improves through layers, reusable silver layer
# MAGIC </details>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 Advanced (Senior Level):
# MAGIC
# MAGIC **Q7: Design a secure, production-grade data pipeline on Azure with Databricks.**
# MAGIC <details>
# MAGIC <summary>Click for Answer</summary>
# MAGIC
# MAGIC **Answer:**
# MAGIC
# MAGIC **Architecture:**
# MAGIC 1. **Networking:**
# MAGIC    - Deploy Databricks in private VNet
# MAGIC    - Use private endpoints for ADLS access
# MAGIC    - Disable public access to storage accounts
# MAGIC    - Implement NSGs for traffic control
# MAGIC
# MAGIC 2. **Security:**
# MAGIC    - Azure Entra ID for authentication
# MAGIC    - Azure RBAC for infrastructure access
# MAGIC    - Unity Catalog for data governance
# MAGIC    - Azure Key Vault for secrets
# MAGIC    - Enable encryption at rest (customer-managed keys)
# MAGIC
# MAGIC 3. **Data Flow:**
# MAGIC    - Source → ADLS Gen2 (landing zone)
# MAGIC    - Unity Catalog External Volume
# MAGIC    - Bronze tables (raw)
# MAGIC    - Silver tables (cleaned, validated)
# MAGIC    - Gold tables (aggregated, business metrics)
# MAGIC
# MAGIC 4. **Compute:**
# MAGIC    - Use Serverless for notebooks/SQL
# MAGIC    - Jobs for scheduled pipelines
# MAGIC    - Photon engine enabled
# MAGIC    - Auto-scaling configured
# MAGIC
# MAGIC 5. **Governance:**
# MAGIC    - Table-level permissions (Unity Catalog)
# MAGIC    - Row/column-level security
# MAGIC    - Audit logging enabled
# MAGIC    - Data lineage tracking
# MAGIC    - PII tagging and discovery
# MAGIC
# MAGIC 6. **Monitoring:**
# MAGIC    - Azure Monitor for infrastructure
# MAGIC    - Databricks System Tables for usage
# MAGIC    - Alert on failed jobs
# MAGIC    - Cost monitoring and optimization
# MAGIC </details>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q8: How would you handle encryption in Azure + Databricks?**
# MAGIC <details>
# MAGIC <summary>Click for Answer</summary>
# MAGIC
# MAGIC **Answer:**
# MAGIC
# MAGIC **Encryption at Rest:**
# MAGIC * ADLS Gen2: Enabled by default (Microsoft-managed keys)
# MAGIC * For compliance: Use customer-managed keys (CMK) in Azure Key Vault
# MAGIC * Databricks DBFS: Encrypted by default
# MAGIC * Unity Catalog: Supports CMK for managed tables
# MAGIC
# MAGIC **Encryption in Transit:**
# MAGIC * All Azure traffic uses TLS 1.2+
# MAGIC * Databricks ↔ ADLS: HTTPS only
# MAGIC * Private endpoints: Traffic stays on Azure backbone (never public internet)
# MAGIC
# MAGIC **Key Management:**
# MAGIC * Store keys in Azure Key Vault
# MAGIC * Enable key rotation policies
# MAGIC * Use RBAC to control key access
# MAGIC * Audit key usage
# MAGIC
# MAGIC **Best Practice:**
# MAGIC * Use private endpoints + CMK for production
# MAGIC * Implement key rotation (90-day cycle)
# MAGIC * Separate keys per environment (dev/staging/prod)
# MAGIC </details>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q9: What are the performance considerations when designing Azure + Databricks pipelines?**
# MAGIC <details>
# MAGIC <summary>Click for Answer</summary>
# MAGIC
# MAGIC **Answer:**
# MAGIC
# MAGIC 1. **Data Locality:**
# MAGIC    - Keep compute and storage in same region
# MAGIC    - Use Azure Backbone (private endpoints) to avoid internet latency
# MAGIC
# MAGIC 2. **File Formats:**
# MAGIC    - Use Parquet or Delta (columnar, compressed)
# MAGIC    - Avoid CSV for large datasets
# MAGIC    - Enable Photon engine for Delta tables
# MAGIC
# MAGIC 3. **Partitioning:**
# MAGIC    - Partition by frequently filtered columns (e.g., date)
# MAGIC    - Avoid over-partitioning (keep files > 128 MB)
# MAGIC    - Use Z-ordering for Delta tables
# MAGIC
# MAGIC 4. **Caching Strategy:**
# MAGIC    - Delta caching (automatic on serverless)
# MAGIC    - Avoid manual .cache() with serverless
# MAGIC    - Use materialized views for repeated aggregations
# MAGIC
# MAGIC 5. **Compute Selection:**
# MAGIC    - Serverless for most workloads
# MAGIC    - Classic clusters only for special requirements
# MAGIC    - Enable auto-scaling
# MAGIC
# MAGIC 6. **Query Optimization:**
# MAGIC    - Push down filters early
# MAGIC    - Use broadcast joins for small tables
# MAGIC    - Analyze query plans (explain())
# MAGIC    - Collect statistics (ANALYZE TABLE)
# MAGIC </details>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Q10: Explain disaster recovery strategy for Azure + Databricks.**
# MAGIC <details>
# MAGIC <summary>Click for Answer</summary>
# MAGIC
# MAGIC **Answer:**
# MAGIC
# MAGIC **Data Layer (ADLS Gen2):**
# MAGIC * Enable geo-redundant storage (GRS or GZRS)
# MAGIC * Replicate to paired region automatically
# MAGIC * Use Azure Site Recovery for critical workloads
# MAGIC * Implement cross-region replication for Delta tables
# MAGIC
# MAGIC **Databricks Workspace:**
# MAGIC * Export notebooks/jobs to Git (version control)
# MAGIC * Infrastructure as Code (Terraform/ARM templates)
# MAGIC * Multi-workspace strategy (prod + DR)
# MAGIC * Regular backups of workspace configurations
# MAGIC
# MAGIC **Unity Catalog:**
# MAGIC * Metadata is highly available by default
# MAGIC * Cross-region metastore replication (planned)
# MAGIC * Export catalog metadata periodically
# MAGIC
# MAGIC **Recovery Objectives:**
# MAGIC * RTO (Recovery Time Objective): < 4 hours
# MAGIC * RPO (Recovery Point Objective): < 1 hour
# MAGIC * Test DR procedures quarterly
# MAGIC
# MAGIC **Failover Steps:**
# MAGIC 1. Activate paired region storage
# MAGIC 2. Deploy Databricks workspace in DR region
# MAGIC 3. Restore configurations from IaC/Git
# MAGIC 4. Update DNS/endpoints
# MAGIC 5. Validate data integrity
# MAGIC 6. Resume operations
# MAGIC </details>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ Common Mistakes & How to Avoid Them:
# MAGIC
# MAGIC ### 1️⃣ Confusing Storage vs Compute
# MAGIC **❌ Mistake:**  
# MAGIC "I need to increase my storage account SKU to process data faster"
# MAGIC
# MAGIC **✅ Fix:**  
# MAGIC Storage and compute are separate! Use Databricks Serverless (compute) to process data in ADLS (storage). Scale them independently.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ Ignoring RBAC / Least Privilege
# MAGIC **❌ Mistake:**  
# MAGIC Granting "Owner" role to everyone on the subscription
# MAGIC
# MAGIC **✅ Fix:**  
# MAGIC Use specific roles:
# MAGIC * Data Engineers → Contributor on resource group
# MAGIC * Analysts → Reader on resource group
# MAGIC * Service Principals → Storage Blob Data Contributor (storage only)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ Misconfiguring Networking
# MAGIC **❌ Mistake:**  
# MAGIC Leaving storage accounts with public access enabled in production
# MAGIC
# MAGIC **✅ Fix:**  
# MAGIC * Disable public access to ADLS
# MAGIC * Use private endpoints
# MAGIC * Configure VNet integration
# MAGIC * Implement NSG rules
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4️⃣ Not Using Secure Access Patterns
# MAGIC **❌ Mistake:**  
# MAGIC Hardcoding storage account keys in notebooks
# MAGIC
# MAGIC ```python
# MAGIC # WRONG!
# MAGIC storage_key = "abc123secretkey456"
# MAGIC ```
# MAGIC
# MAGIC **✅ Fix:**  
# MAGIC Use Unity Catalog Volumes/Tables:
# MAGIC ```python
# MAGIC # RIGHT!
# MAGIC df = spark.read.parquet("/Volumes/main/bronze/data/file.parquet")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5️⃣ Mixing Azure AD Concepts
# MAGIC **❌ Mistake:**  
# MAGIC Confusing Users, Service Principals, and Managed Identities
# MAGIC
# MAGIC **✅ Fix:**  
# MAGIC * **User:** Human identity (john@company.com)
# MAGIC * **Service Principal:** Application identity (manually created)
# MAGIC * **Managed Identity:** Auto-managed identity for Azure resources (preferred)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6️⃣ Over-provisioning Resources
# MAGIC **❌ Mistake:**  
# MAGIC Creating large fixed-size clusters for all workloads
# MAGIC
# MAGIC **✅ Fix:**  
# MAGIC * Use Databricks Serverless (auto-scales)
# MAGIC * Enable auto-termination
# MAGIC * Right-size based on actual usage
# MAGIC * Use spot instances for non-critical workloads
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 7️⃣ Ignoring Cost Management
# MAGIC **❌ Mistake:**  
# MAGIC No cost monitoring, resources running 24/7
# MAGIC
# MAGIC **✅ Fix:**  
# MAGIC * Tag all resources (project, environment, owner)
# MAGIC * Set up Azure Cost Management alerts
# MAGIC * Auto-terminate idle clusters
# MAGIC * Use Azure Policy for governance
# MAGIC * Review costs weekly
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 8️⃣ Poor Naming Conventions
# MAGIC **❌ Mistake:**  
# MAGIC Random names: "storage1", "rg-test", "cluster123"
# MAGIC
# MAGIC **✅ Fix:**  
# MAGIC Use consistent patterns:
# MAGIC * `rg-<workload>-<env>-<region>-<instance>`
# MAGIC * `st<workload><env><region><instance>` (storage)
# MAGIC * `dbw-<workload>-<env>-<region>` (Databricks)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 9️⃣ Not Using Infrastructure as Code
# MAGIC **❌ Mistake:**  
# MAGIC Manually clicking through Azure Portal for everything
# MAGIC
# MAGIC **✅ Fix:**  
# MAGIC * Use Terraform or ARM templates
# MAGIC * Version control infrastructure code
# MAGIC * Automate deployments
# MAGIC * Enables disaster recovery and multi-environment setup
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔟 Not Testing Disaster Recovery
# MAGIC **❌ Mistake:**  
# MAGIC "We have backups, we're fine" (never tested)
# MAGIC
# MAGIC **✅ Fix:**  
# MAGIC * Test DR procedures quarterly
# MAGIC * Document runbooks
# MAGIC * Measure actual RTO/RPO
# MAGIC * Practice failover and failback
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Additional Resources:
# MAGIC
# MAGIC * [Azure Architecture Center](https://docs.microsoft.com/azure/architecture/)
# MAGIC * [Databricks on Azure Best Practices](https://docs.databricks.com/azure/)
# MAGIC * [Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/)
# MAGIC * [Azure Security Best Practices](https://docs.microsoft.com/azure/security/)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **🎓 End of Phase 1 - Day 4**  
# MAGIC **Watermark: @TRRaveendra**