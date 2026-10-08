# 🚀PySpark Transformations, Narrow Transformations & Wide Transformations

Dans cette session nous aborderons l'un des sujets les plus important en data engineering, les transformations. Nous montrerons:

## Ce qui se passe lorsqu'on utilise certaines opérations comme:select(), filter(), groupBy(), join(), orderBy(),... 

## Pourquoi certaines transformations sont rapide à exécuter tandisque d'autres causent expensive data shuffles.


## 🔥Que va t-on apprendre


## 1️⃣ PySpark Transformations

✅ What is a Transformation?

✅ Transformations vs Actions

✅ Lazy Evaluation

✅ How Spark builds a logical execution plan

✅ Common DataFrame transformations

✅ select()

✅ filter()

✅ withColumn()

✅ union()

✅ drop()

✅ distinct()

✅ groupBy()

✅ join()

✅ orderBy()

✅ repartition()


## 2️⃣ Narrow Transformations
  
✅ select()

✅ filter()

✅ map()

✅ withColumn()

✅ union()

✅ coalesce()
 
✅ groupBy()

✅ join()

✅ distinct()

✅ orderBy()

✅ sort()

✅ repartition()

Key idea:

Data movement → Shuffle → More network and disk activity → Potential performance impact

## 🧠 Narrow vs Wide — Simple Explanation

Think of a company with three employees processing three piles of documents.
 
 
Comprendre Narrow vs Wide Transformations permet de mieux comprendre:

🔥 Why a Spark job is slow
🔥 Why shuffle happens
🔥 Why some joins are expensive
🔥 Why groupBy() can be costly
🔥 How partitions affect performance
🔥 How Spark creates stages
🔥 How to optimize Spark workloads
🔥 How to troubleshoot Databricks jobs







# 🗺️ Project Phases & Guide


## 🏗️ Phase1 - Project Usecase definition

<aside>

> [!TIP]
> ***Goal***: *Document à rédiger avec collaboration des métiers pour bien cadrer le besoin.Ce sera un résumé de l'existant, du problème et les solutions adaptées.Il sera lieu de dessiner une architecture technique*

## Background

- Les entreprises de retail génère une grosse volumétrie de données de CRM, d'inventaire et de transaction.
- Nous disposons des données issues de sources et de formats variés; ce qui rend compliqué l'analyse unifiée.
- Les équipes métier ont besoin de kpi(quick insight) dans les ventes(sales), les customers, products et inventory.

## Problem

- Les données sont réparties dans plusieurs sources déconnectées entre elles; ceci crée data silos.
- Les anciennes pipelines existantes sont devenues lentes avec l'augmentation des données et la mise en échelle est difficile(not scalable).
- Les métiers ont besoin d'instaurer une politique de self service BI analytic pour les opération de retail et des indicateurs de vente à travers des dashboards et Genie AI interface.

## Solution approach

- Créer une plateforme d'analyse de retail(**retail analytic plateform**) dans Databricks en utilisant une architecture de medaille(Bronze-Silver-Gold) qui est scalable et répond aux standards modernes.
- Intégrer dans le **retail analytic plateform** des données de Salesforce, PostgreSQL et Azure blob storage réglant le problème de silos
- Créer un retail Dashboard et Databricks genie AI interface permettant aux end users de faire leurs dashboards et d'interagir direcetement avec les données en utilisant un langage naturel.
  
## Design the architecture

L'image ci-dessous montre ce que nous allons techniquement faire dans ce projet:

<img width="925" height="454" alt="image" src="https://github.com/user-attachments/assets/5cbc02d9-6512-448d-8a38-61d00d5b8caa" />

---

- **Sources de données:** Nous utiliserons **3 sources de données**: Salesforce, PostgreSQL et Azure blob storage(on utilisera les free version)
- **Ingestion:** Ensuite on va ingérer les données à l'aide des fonctionnalités de Databricks **Lakeflow connect** pour Salesforce, PostgreSQL; **Auto loader** pour Azure blob storage
- **Transformations:** Pour la transformation des données dans une medaillon architecture on utilisera le composant **Lakeflow Déclarative Pipelines** de Databricks.
- **Semantic layer:** Nous mettrons en place un **Metric view** comme **semantic layer**
- **Data consum:** Pour la consommation on produira des **Databricks dashboards** et **Genie workspace**.
- **Orchestration:** Après avoir conçu la solution on utilisera des **Databricks job** pour orchestrer le tout.
- **Gouvernance: Unity Catalog de Databricks** sera utilisé comme outil de gouvernance.
- **Databricks Genie code**: va etre utilisé comme base de développement durant tout au long de ce projet dans les notebooks

**Databricks Genie code:** *Est récemment conçu par Databricks; il est très pratique en permettant de gagner en productivité et ne pas perdre trop de temps dans la conception des code manuellement*.

- **Gold layer - Modèlisation:**
Une modélisation en étoile sera conçue.dans Gold layer on aura une Fact_table et 3 Dim_tables(Dim_customer, dim_product et dim_calendar)

<img width="893" height="465" alt="image" src="https://github.com/user-attachments/assets/a656c63f-5816-496f-b33c-6e50adab6ae4" />

---

## 🏗️ Phase 2 - Project Initialization

<aside>

**Goal**: Préparation des étapes de développement à travers des **Backlogs**, **Users stories** et **Sprints** en utilisant **notion** ou **jira**.

- [ ]  **Environment setup**
      
    - [ ]  **Setup PostgreSQL**
          
        - [ ]  Use Neon serveless postgres(https://neon.com) et utiliser un google account
        - [ ]  Donner un project name ( retail_project) et choisir une région proche de France
            - [ ]  Copier et coller quelque part le lien d'accès généré(qui sera ultérieurement utilisé)
        - [ ]  Créons les tables sources **product_catalog** et **inventory** dans Postgres en y insérant des données(avec copier coller sur sql editor du code sql disponible dans les fichiers ***01_postgres_product_history.sql*** et ***02_postgres_inventory_history.sql*** disponible dans le dossier ***00_Source_Data*** de ce repos)
    
    - [ ]  **Setup Salesforce**
          
        - [ ]  Use salesforce trial account (30 days free trial)
        - [ ]  Conserver le username reçu par mail and reset the password
        - [ ]  Créons les tables sources **Account** & **Opportunity** en important dans salesforce Accounts et Opportunités(fichiers client & Opportunités) les fichiers ***03_salesforce_accounts_history.csv*** et ***04_salesforce_opportunities_history*** disponible dans le dossier ***00_Source_Data*** de ce repos.
                  
    - [ ]  **Setup Azure blob storage as volume**
          
  Azure blob storage, S3, gcp sont des stockages cloude qui peuvent stocker n'importe quel type de données(structured, semi structured, not structured).Nous pouvons créer un volume dans Databricks qui aura les memes caractéristiques de stockage que les clouds.
     - [ ]  Créer un nouveau schéma(***Volumes*** et y créer un volume ***blob_source*** dans le catalog ***retail_q***
     - [ ]  On a 2 choix de types de volume(external et managed); on choisit managed pour un volume entierement managé par Unity catalog. On aurait choisi external si on avait un adsl ou s3.
     - [ ]  Créer un répertoire dans volume nommé ***transactions_source** pour y stocker les fichiers source transactions (***05_blob_transactions_history.csv***) disponibles dans le dossier ***00_Source_Data*** de ce repos)
     - [ ]  On importe le fichier ***05_blob_transactions_history.csv*** dans le volume
     - [ ]  Copier le ***path:*** "/Volumes/nom catalog/nom schema/nom volume/nom répertoire correspondant dans notre cas à ***"/Volumes/retail_q/Volumes/blob_source/transactions_source"*** (qui sera ultérieurement utilisé)
     - [ ]  ***blob_source*** etant disponible dans un volume nous devons l'ingérer dans un nouveau schéma ***blob_bronze** du catalog ***retail_q***.

> [!NOTE]
> 
> **Lakeflow Connect** *est une collection de connecteurs managés dans Azure Databricks qui simplifient l’ingestion des données à partir de sources externes. Au lieu d’écrire du code d’extraction personnalisé, vous configurez des pipelines via une interface graphique ou des définitions déclaratives.*
> 
> *Dans le lakeflow connect des connecteurs managés sont disponibles pour salesforces, postgres et tant d'autres rendant l'option ***ingestion pipeline (lakeflow connect)*** possible mais un transfert de données d'un volume databricks à un schema (ou élémens internes à Unity catalog) n'est pas disponible car on a affaire à un transfert entre repertoires. On ne peut donc utiliser l'option ingestion pipeline (lakeflow connect) comme on l'a fait tantôt avec salesforce et postgres.Peut dans l'avenir Databricks intégrera un connecteur adapté dans le lakeflow connect.*
>
> *Lorsque vous créez un pipeline d’ingestion pour un connecteur de base de données tel que SQL Server, Lakeflow Connect crée également une passerelle d’ingestion. Cette passerelle extrait en continu les données modifiées de la base de données source et les met en phase pour le traitement. Pour les connecteurs SaaS comme Salesforce, le connecteur gère l’extraction directement sans passerelle distincte.
Pour pallier à ce problème Databricks dispose d'un outil appelé ***Auto loader*** faisant référence à une ingestion interne à Databricks disponible via notebook.
Nous utiliserons les connecteurs managés de lakeflow connect pour créer 2 pipelines d'ingestion pour les données sources de Salesforces et Neon postgreSQL; pour ce qui est des données qui sont le volume de Databricks on utilisera Auto loader à travers un code personnalisé sur notebook python.*

**Result:** Project is ready to start building Bronze, Silver, and Gold layers.

</aside>

---
> [!NOTE]
> ## 🥉 Phase 3 - Building Bronze Layers(postgres_bronze, salesforce_bronze, blob_bronze)

<aside>

**Goal**: Build the Bronze layers by ingesting all raw CSV files into Delta tables without any kind of transformations. Nous avons un catalog (***retail_q***) dans lequel des schemas bronze sont dédiés à chacune de nos sources:  ***postgres_bronze, salesforce_bronze, blob_bronze*** pour y stocker nos données sources à l'etat brut.

</aside>

- [ ]   **Data Ingestion pipeline from Postgres with Lakeflow connect(phase 1)**
    - [ ]  Launch Databricks workspace
    - [ ]  Create catalog ***retail_q*** et schémas ***postgres_bronze*** for source coming from postgresSQL
    - [ ]  Go to Jobs & Pipelines -> Ingestion pipeline(correspondant au **Lakeflow connect** dans le repos)
          
    - [ ]  **Step 1/5 : Connexion**
          
        - [ ]  Parmi toutes les sources disponibles on clique sur postgreSQL
        - [ ]  Create connexion between Postgres & Databricks(*connex_name = neon_postgres_project*), pour avoir user, passeword et le port on utilise chat gpt en collant le lien qu'on avait récupéré(*based on this url give me the username, passeword and hostname*)
              
    - [ ]  **Step 2/5 : Ingestion setup**
          
        - [ ]  Donner un nom au pipeline(***postgres_to_bronze*** et sélectionne le catalog et schema cibles(créons les respectivement ***retail_q*** et ***postgres_bronze***)
        - [ ]  Cliquez sur *create pipeline and continu*
              
    - [ ]  **Step 3/5 : Validating pipeline configuration**
          
        - [ ]  Spécifier la database, le schema et les 2 tables source à ingérer
        - [ ]  Choisir ***batch ou incrementielle ingestion:***
        - [ ]  Choisir dans ***cursor column*** la colonne ***updated_at*** pour définir la colonne ***incrémentielle*** de la table ***product_catalog***
        - [ ]  Pour la table ***product_catalog*** mettre ***history tracking*** en **ON** pour rendre la table de destination une structure **SCD2** en créant 2 colonnes supplémentaires ***start_at(même valeur que update_at par défaut) & end_at(null par défaut)***.
        - [ ]  Choisir dans ***cursor column*** la colonne ***last_stock_update_at*** pour définir la colonne ***incrémentielle*** de la table ***inventory***
        - [ ]  Pour la table ***inventory*** mettre ***history tracking*** en **OFF** pour rendre la table de destination une structure **SCD1**
              
> [!NOTE]
> *Le paramètre de type SCD détermine la façon dont la table de destination gère les modifications.*
> 
> **SCD 2:** *Si un enregistrement est mise à jour ou supprimé depuis la source; alors toute la ligne sera historisée avec une valeur **end_at not null**; une nouvelle ligne avec les nouvelles valeurs vont être créées avec **end_at null** et start_at prend la valeur de la dernière end_at.2 lignes vont être updté.On créera une colonne conditionnelle nommée active basée sur la end_at is null*.Cette approche suit la façon dont les données évoluent au fil du temps.
> 
> **SCD 1:** *Il n'ya pas d'historisation les enregistrements modifiées ou supprimées disparaissent directement; seules les lignes activent seront présente: c'est le cas pour la table inventory.Une ligne sera updatée par le pipeline*.

  - [ ]  **Step 4/5 : Spécifier ou stocker les données ingérées dans Databricks**
        
     - [ ]  Choisir le catalog ***retail_q*** et le schémas ***postgres_bronze***
           
  - [ ]  **Step 5/5 : Schedules(run planification) & Notifications**
        
    - [ ]  Add a trigger to run the job on a schedule (for example daily)
    - [ ]  Pour l'instant nous supprimons le schedule de défaut qu'on configurera plus tard en fonction des besoins d'execution
    - [ ]  Les notifications ne sont configurées qu'avec **failure** afin de ce recevoir un email en cas d'echec de pipeline execution.
    - [ ]  On clique sur **Save & Run** pour terminer la création du **ingestion pipeline** nommé ***postgres_to_bronze*** qui ingère dans le catalogue ***retail_q*** et le schema ***postgres_bronze*** de Unity catalog de databricks les tables ***product_catalog*** et ***inventory***.

- [ ]  **Data Ingestion pipeline from Salesforce with Lakeflow connect(phase 2)**
    - [ ]  Create schémas ***salesforce_bronze*** for source coming from salesforce
    - [ ]  Go to Jobs & Pipelines -> Ingestion pipeline(correspondant au *Lakeflow connect* dans le repos)
          
    - [ ]  **Step 1/5 : Connexion**
          
        - [ ]  Parmi toutes les sources disponibles on clique sur Salesforce
        - [ ]  Create connexion between Salesforce & Databricks(*connex_name = Salesforce_retail_project*),
        - [ ]  On utilise les username, passeword obtenus lors de la création du salesforce account
              
    - [ ]  **Step 2/5 : Ingestion setup**
          
        - [ ]  Donner un nom au pipeline (***salesforce_to_bronze***) et on sélectionne le catalog ***retail_q*** et on crée le schéma cibles ***salesforce_bronze***
        - [ ]  Cliquez sur *create pipeline and continu
              
    - [ ]  **Step 3/5 : Validating pipeline configuration**
          
        - [ ]  Spécifier la database, le schema et les 2 tables source à ingérer
        - [ ]  Cocher ***history tracking*** en **ON ou OFF**définit le choix entre  ***batch et incrementielle ingestion***
        - [ ]  Pas besoin de préciser un ***cursor column*** sur Salesforce car c'est déja intégré la colonne pour définir la colonne ***incrémentielle*** de la table ***account***
        - [ ]  Pour la table ***opportunity*** mettre ***history tracking*** en **OFF**.
              
   - [ ]  **Step 4/5 : Spécifier où stocker les données ingérées dans Databricks**
         
        - [ ]  Choisir le catalog ***retail_q*** et le schémas ***sales_bronze***
              
   - [ ]  **Step 5/5 : Schedules(run planification) & Notifications**
         
        - [ ]  Add a trigger to run the job on a schedule (for example daily)
        - [ ]  Pour l'instant nous supprimons le schedule de défaut qu'on configurera plus tard en fonction des besoins d'execution
        - [ ]  Les notifications ne sont configurées qu'avec **failure** afin de ce recevoir un email en cas d'echec de pipeline execution.
        - [ ]  On clique sur **Save & Run** pour terminer la création du **ingestion pipeline** nommé ***salesforce_to_bronze*** qui ingère dans le catalogue ***retail_q*** et le schema ***salesforce_bronze*** de Unity catalog de databricks les tables ***account** et ***opportunity***.

- [ ]  **Data Ingestion pipeline from Volume with Autoloader(phase 3)**
    - [ ]  Create a folder in the repository called ***01_Notebook*** to store all scripts inside it
          
    - [ ]  **Step 1/3 : Create a folder & notebooks**
          
        - [ ]  Go to Databricks workspace -> Create new folder nommé ***retail_q*** dans le workspace où l'on stokera tout ce qui est important(notebooks,...)
        - [ ]  Create notebook nommé ***blob_to_bronze***
              
    - [ ]  **Step 2/3 : Read & Write with Autoloader (code genie)**
          
        - [ ]   Read the ***05_blob_transactions_history.csv*** file se trouvant dans le volume ***transactions_source** into a DataFrame
        - [ ]   Write the DataFrame to a table in the Bronze schema ***blob_bronze*** using overwrite mode.
        - [ ]  Il est conseillé d'utiliser Genie code pour gagner du temps(Genie dispose 2 variants: **Agent**(run multi step data and AI tasks- ***changer le code du notebook***) & **Chat**(Asking question about your code).Pensez à bien vérifier le resultat de genie 
        - [ ]  Choisir Agent et saisir les questions(ce qu'on souhaite faire) dans le prompt.
        - [ ]  Run the script and query the bronze table to verify it is loaded correctly
        - [ ]  Run the whole notebook to see if everything works successfully.
              
   - [ ]  **Step 3/3 : Push & commit github**
         
        - [ ]  Commit & Push your changes to the GitHub repository

> [!IMPORTANT]
> **Goal of the question in genie:** For reading csv files from source and write them into target schema using Auto loader
> 
> **Question 1:** We are getting csv files at ***"/Volumes/retail_q/Volumes/blob_source/transactions_source"***
> 
> read the csv files with Auto loader and then write them into the target table ***"retail_q.blob_bronze.transactions"***
> 
>  **Answer 1:**


      >  genie output ( cf 01_blob_to_bronze.py file)
      
        # Databricks notebook source
        
        # Read CSV files using Auto Loader
        df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/retail_q/volumes/blob_source/_schema")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Volumes/retail_q/volumes/blob_source/transactions_source/")
       )

        # Write to bronze table - process available data and stop
         query = (df.writeStream
        .option("checkpointLocation", "/Volumes/retail_q/volumes/blob_source/_checkpoint")
        .trigger(availableNow=True)
        .toTable("retail_q.blob_bronze.transactions")
       )

        # Wait for the batch to complete
        query.awaitTermination()

        # COMMAND ----------

        # MAGIC %sql
        # MAGIC select count(*) from retail_q.blob_bronze.transactions
        

> [!IMPORTANT]
> **Interprétation**
> 
> **Auto Loader:** *Auto Loader (via format("cloudFiles")) est l'outil recommandé par Databricks pour ingérer en continu ou par lots des fichiers volumineux stockés dans un Cloud Object Storage (Databrick's Volumes, S3, ADLS, GCS). Nos sources sont dans un volume de Databricks et notre cible dans un schemas de Unity Catalog de Databricks. Sa force se trouve dans sa capacité à détecter et traiter automatiquement les nouveaux fichiers avec l’évolution du schéma*.


> [!NOTE]
> **Rappel sur Ingesting data into the Unity catalog:**
> 
> Les notebooks dans Azure Databricks fournissent une approche flexible, pilotée par le code, pour ingérer des données provenant de diverses sources. Lorsque les outils graphiques comme lakeflow connect ne répondent pas à vos besoins ou que vous avez besoin d’une logique personnalisée pour la transformation des données, les notebooks vous permettent de contrôler complètement le processus d’ingestion(**Ingesting batch and streaming data using notebooks with DataFrames and Structured Streaming**).
> Les codes python (read & write) pour ingérer des données à l’aide de notebooks varient selon les scénarios de traitement (par lots, de streaming) et l'emplacement de stockage(hors Cloud/Cloud Object Storage).


**Result**: All 6 raw source files(***product_catalog, inventory,account, opportunity et transaction*** are ingested into there dedicated Bronze tables with no transformations applied.

</aside>

---
> [!NOTE]
> ## 🥈 Phase 4 - Building Silver Layer (Data Cleaning & Transformation)

<aside>

**Goal**: It is time to clean and transform our bronze data and load the clean results into silver layer. This is usually the most time consuming phase of the project and the fun part!

Nous allons utiliser la composante ***Lakeflow spark declarative pipeline*** pour faire les transformations afin d'avoir Silver & gold layers. Nous créerons un ETL pipeline pour chacune de nos 4 tables sources.

</aside>

- [ ]  **ETL pipeline for cleaning & transforming using Lakeflow spark declarative pipeline**
    - [ ]  **Step 1/5: Github setup**
          
        - [ ]  Create repository structure
        - [ ]  Create a folder called ***00_Bronze_to_silver*** pour y stocker les notebooks de lakeflow spark declarative pipeline pour chaque table.
              
    - [ ]  **Step 2/5: Etl pipeline setup**
          
        - [ ]  For each Bronze table (4 tables)
            - [ ]   Create dans le catalog ***retail_q*** le schémas ***retail_silver*** pour les tables transformées venant de toutes les bronze layers
            - [ ]   Go to Jobs & Pipelines -> Etl pipeline -> configure the blank pipeline :
            - [ ]   Attribuer un nom au pipeline nommé ***retail_transformation***
            - [ ]   Un folder est créé par defaut nommé transformations qu'on renomme ***bronze_to_silver***
            - [ ]   On va créer dans le folder **bronze_to_silver*** des ***fichiers.py*** devant servir de code pour chacune des tables à transformer.i.e: ***Product_catalog.py,inventory,account.py,opportunity.py,transactions.py***
    - [ ]   **Step 3/5: Transformation using genie**
          
        - [ ]  ***Data Quality check using @dp.expect_all_or_drop & @dp.expect***
             
      -  ***@dp.expect_all_or_drop("valid_column", "rules")*** est pour appliquer des règles data quality sur les colonnes et supprimer les lignes ne respectant pas les conditions i.e: @dp.expect_all_or_drop("valid_price", "unit_price > 0")
        
      -  ***@dp.expect("colonne", "rules")*** est pour appliquer des règles data quality sur les colonnes sans supprimer les lignes ne respectant les règles.

           >
          
               from pyspark import pipelines as dp
               from pyspark.sql import functions as F
 
               @dp.table(
               name="retail_q.retail_silver.product_catalog",
               comment="Silver layer product catalog with standardized data and data quality rules"
               )
               @dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL AND LENGTH(TRIM(product_id)) > 0")
               @dp.expect_or_drop("valid_product_name", "product_name IS NOT NULL AND LENGTH(TRIM(product_name)) > 0")
               @dp.expect("valid_category", "category IS NOT NULL")
               @dp.expect("valid_price", "unit_price > 0")
               @dp.expect_or_drop("valid_launch_date", "launch_date IS NOT NULL")
               @dp.expect("valid_supplier", "supplier_name IS NOT NULL")
          >
          
          - [ ]  Find duplicates
          - [ ]  Validate string values: Check extra spaces, Identify abbreviations to normalize
          - [ ]  Validate dates values: Check Data Type, check the format, handle missing values
          - [ ]  Validate numeric values
          - [ ]  Standardize business key IDs to ensure tables can be joined correctly.
          - [ ]  Check the name of columns and table and make a plan how to rename them to something friendly.
              
    - [ ]  **Section 1: Read data Bronze Table and Load it into a DataFrame**
          
         - [ ]  Create df function nommée ***def table_clean()***
         - [ ]  Create source_df qui lit la table avec ***source_df** = spark.readStream.table("catalog.schema_bronze.table")
        

        >
             def account_clean():
                   # Read source streaming table
                   source_df = spark.readStream.table("retail_q.salesforce_bronze.account")
    
       >
   
    - [ ]  **Section 2: Standardize operations - Transform data**
          
- Fix issues one by one
- Garder les transformations petites et claires
- Eviter une large bloque de transformation
- Use Spark SQL or PySpark (Python)
- Usage des def function est très pratique:
- Appliquer le ***Return au selected_df ou directement au source_df.select()*** pour récupérer les colonnes pertinentes avec des noms en minuscules
         
         >
               def account_clean():
                   # Read source streaming table
                   source_df = spark.readStream.table("retail_q.salesforce_bronze.account")
    
                  # Sélectionner les colonnes pertinentes avec des noms en minuscules.
                    return source_df.select(
                    F.col("Id").alias("id"),
                    F.col("IsDeleted").alias("is_deleted"),
                    F.upper(F.trim(F.col("Name"))).alias("customer_name"),
                    F.col("Type").alias("type")
                  )
        >
        
    - [ ]  Effectue des vérifications de cohérence sur le DataFrame final avant l'écriture.
         
    - [ ]  **Step 4/5: Run the pipeline**
  
       - [ ]  Finalize notebook
          - [ ]  Review structure and readability
          - [ ]  Add comments and documentation
          - [ ]  Run the full notebook end to end
          - [ ]  Choisi le catalog et schema de destination (***retail_q et retail_silver***)
          - [ ]  Vérification de cohérence de la table « silver » après écriture
          - [ ]  Notre pipeline ***retail_transformation*** et le fichier ***product_catalog.py*** sont créés
       
  - [ ]  **Step 5/5:Commit & Push your changes to the GitHub repository**
      - [ ]  Clone the notebook as a template for the next table

> [!IMPORTANT]
> **Goal of the question in genie:** Create silver layer table in spark lakeflow declarative pipeline
> 
> **Question 2:** read the table ***retail_q.postgres_bronze.product_catalog***, apply generic standardization operations, apply some ***data quality rules*** then write the output ***"retail_q.retail_silver.product_catalog"***
> 
>  **Answer 2:**


      >  genie output ( cf product_catalog.py)
      
           from pyspark import pipelines as dp
           from pyspark.sql import functions as F
     
          @dp.table(
              name="retail_q.retail_silver.product_catalog",
              comment="Silver layer product catalog with standardized data and data quality rules"
         )
        @dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL AND LENGTH(TRIM(product_id)) > 0")
        @dp.expect_or_drop("valid_product_name", "product_name IS NOT NULL AND LENGTH(TRIM(product_name)) > 0")
        @dp.expect("valid_category", "category IS NOT NULL")
        @dp.expect("valid_price", "unit_price > 0")
        @dp.expect_or_drop("valid_launch_date", "launch_date IS NOT NULL")
        @dp.expect("valid_supplier", "supplier_name IS NOT NULL")
        def product_catalog():
        return (
        spark.readStream.table("retail_q.postgres_bronze.product_catalog")
        .select(
            # Standardize product_id: trim and uppercase
            F.upper(F.trim(F.col("product_id"))).alias("product_id"),
            
            # Standardize product_name: trim and title case
            F.initcap(F.trim(F.col("product_name"))).alias("product_name"),
            
            # Standardize category: trim and title case
            F.initcap(F.trim(F.col("category"))).alias("category"),
            
            # Standardize subcategory: trim and title case, handle nulls
            F.when(F.col("subcategory").isNotNull(), 
                   F.initcap(F.trim(F.col("subcategory"))))
             .otherwise(F.lit("Unknown")).alias("subcategory"),
            
            # Standardize brand: trim and title case, handle nulls
            F.when(F.col("brand").isNotNull(), 
                   F.initcap(F.trim(F.col("brand"))))
             .otherwise(F.lit("Unknown")).alias("brand"),
            
            # Standardize price: round to 2 decimal places
            F.round(F.col("unit_price"), 2).alias("unit_price"),
            
            # Standardize supplier_name: trim and title case
            F.initcap(F.trim(F.col("supplier_name"))).alias("supplier_name"),
            
            # Keep dates and timestamps as-is
            F.col("launch_date"),
            F.when(F.col("unit_price") > 50000, "PREMIUM")
             .when(F.col("unit_price") > 10000, "MID_RANGE")
             .otherwise("BUDGET")
             .alias("product_segment"),
            
            # Keep CDC tracking columns with correct names
            F.col("__START_AT").alias("start_at"),
            F.col("__END_AT").alias("end_at"),
            
            # Derive is_active from end_at: null means current/active record
            F.when(F.col("__END_AT").isNull(), F.lit(True)).otherwise(F.lit(False)).alias("is_active"),
            
            # Keep updated_at timestamp
            F.col("updated_at"),
            
            # Add processing timestamp for audit trail
            F.current_timestamp().alias("processed_at")
          )
       )

> [!IMPORTANT]
> Pour le traitement des autres tables ***inventory, account, opportunity,transactions*** on revient sur notre pipeline ***retail_transformation*** pour ***l'éditer*** afin de reprendre le même processus pour générer ***inventory.py, account.py, opportunity.py,transactions.py*** dans le meme folder ***bronze_to_silver***. On peut copier le code utlisé ou genie pour product_catalog.py pour les autres fichiers.

<img width="996" height="618" alt="image" src="https://github.com/user-attachments/assets/24c6e51d-bd83-4824-8f4a-95974ad71da5" />


<aside>

**Result:** All Bronze tables are transformed into analytics-ready Silver tables with validated data quality and standardized structure using Lakeflow spark declarative pipeline.

</aside>

---
> [!NOTE]
> ## 🥇 Phase 5 - Building Gold Layer

<aside>

**Goal**: Dissocier le modèle de données des systèmes sources et mettre en place un nouveau modèle adapté à la Business Intelligence et à l'analyse de données.

Nous utiliserons la modélisation dimensionnelle pour transformer les tables « Silver » ***inventory, account, opportunity,transactions*** en un schéma en étoile composé de tables de ***fact*** et de tables de ***dimensions***.
          <img width="692" height="524" alt="image" src="https://github.com/user-attachments/assets/a704457f-6a67-4467-b0cb-3c7a1de84e56" />
- On va joindre les tables Transactions et opportunity pour avoir la ***fact table***.
- Pour les tables de dimensions en pratique on les transforme directement en vues via SQL mais pour notre cas comme elles deja propres on les laisse dans le silver en tant que tables ***dim_customer, dim_product, dim_calendar****.
  
</aside>
 
- [ ]  **Data Modeling using Lakeflow spark declarative pipeline & SQL Views**
    - [ ]   **Step 1/6 : Data model preparation**
          
        - [ ]  Comprendre le contenu de chaque Silver table
        - [ ]  Map each table to a business object such as customers, products, or sales
        - [ ]  Utiliser draw.io pour dessiner le data model cible. Example: `fact_sales`, `dim_customers`, `dim_products`, `dim_calendar
              
    - [ ]  ***Step 2/6: Github setup***
          
        - [ ]  Create repository structure
        - [ ]  Create a folders called ***01_Silver_to_gold & 01_Notebook*** pour y stocker les notebooks de lakeflow spark declarative pipeline pour chaque table.
              
   - [ ]   **Step 3/6: Build Gold tables - For each table in the new model**
         
       - [ ]  For each Bronze table (4 tables)
           - [ ]  Go to Jobs & Pipelines -> Etl pipeline ->***retail_transformation -> edit pipeline*** :
           - [ ]  Créer un nouveau folder nommé ***Silver_to_gold***
           - [ ]  On va créer dans le folder **silver_to_gold*** des ***fichiers.py*** devant servir de code pour chacune des tables de dimension et de fact.i.e: ***dim_product.py,dim_product.py,fact_sales.py***
                 
      - [ ]  **Section 1: Join preparation**
          
           - [ ]  Create def function nommée ***def fact_sales()***
           - [ ]  Créer 2 dataframes: transaction_df & opportunity_df qui lisent les tables delta dans silver:
               - [ ]    ***transactions_df*** = spark.read.table("retail_q.retail_silver.transactions")
               - [ ]    ***opportunity_df*** = spark.read.table("retail_q.retail_silver.opportunity")
                  
      - [ ]  **Section 2: Join & select useful columns**
                 
           - [ ]  Join all relevant Silver tables for the dimension or fact( Transaction & opportunity
           - [ ]  Créer un dataframe ***join_df(issu des 2 premiers)***
           - [ ]  Créer un autre dataframe ***selected_df*** auquel on applique le ***Return*** pour récupérer les colonnes pertinentes avec des noms en minuscules
           - [ ]  S'assurer qu'il n'y ait pas de doublons after joins
         
        >
              from pyspark.sql.functions import upper, trim, sum as _sum, countDistinct, col
              from pyspark import pipelines as dp

             @dp.table(name="retail_q.retail_gold.fact_sales")
             def fact_sales():
             transactions_df = spark.read.table("retail_q.retail_silver.transactions")
             opportunity_df = spark.read.table("retail_q.retail_silver.opportunity")
    
             joined_df = transactions_df.alias("t").join(
             opportunity_df.alias("o"),
             upper(trim(transactions_df.opportunity_name)) == upper(trim(opportunity_df.name)),
             how="left"
            )
       
             # Select important columns (customize as needed)
             selected_df = joined_df.select(
            "t.transaction_id",
            "t.opportunity_name",
            "t.product_id",
            "t.store_id",
            "t.quantity",
            "t.selling_price",
            "t.discount_amount",
            "t.transaction_timestamp",
            col("t.transaction_timestamp").cast("date").alias("transaction_date"),
            "t.payment_mode",
            "t.sales_channel",
            "o.name",
            "o.stage_name",
            "o.owner_id",
            "o.amount",
             col("o.account_id").alias("customer_id")
            )
            return selected_df
     >
  - [ ]  Effectue des vérifications de cohérence sur le DataFrame final avant le run.
         
    - [ ]  **Step 4/6: Run the pipeline**
  
       - [ ]  Finalize notebook
          - [ ]  Review structure and readability
          - [ ]  Add comments and documentation
          - [ ]  Run the full notebook end to end
          - [ ]  Choisi le catalog et schema de destination (***retail_q et retail_gold***)
          - [ ]  Vérification de cohérence de la table **fact_sales** dans unity catalog
          - [ ]  On peut créer des vues ou les laisser en silver pour les dimensions
            
    - [ ]  **Step 4/6: Create SQL Views & calendar table for dim tables**
  
       - [ ]  Create views ***dim_customer, dim_product & fact_inventory*** dans ***retail_q.retail_gold*** via le fichier ***02_Gold_Views.sql***
       - [ ]  Create **dim_calendar** with notebook via le fichier ***03_calendar.py***
           - [ ]  On peut générer la table calendar via genie avec la syntaxe suivante:
                 
> [!IMPORTANT]
> **Goal of the question in genie:** Create dim_calendar
> 
> **Question 3:** "I want to create a calendar table in retail_q.retail_gold schema.It should have a date column which will be key column.Add other columns based on date like year, month, week which are standard columns used in generic calendar tables.Provide start and end date so that i can decide how mach data to be populated"
> 
>  **Answer 3:**             
         
   - [ ]  **Step 6/6: Commit & Push your changes to the GitHub repository**
         

<aside>
🔥

**Bonus Task - Data Product Ownership**

At this point, the data is ready for analytics and your tables represents a **data product**

You are now responsible for making it reliable, clear, and easy to use

**Enhance metadata in Unity Catalog**

- Add meaningful descriptions to Gold tables
- Add clear descriptions to all important columns
- Ensure column names and meanings are easy to understand for analysts

**Define data relationships**

- Define primary keys for dimension tables
- Define foreign keys between fact and dimension tables
</aside>

<aside>

**Result**: All Silver tables are transformed into business-ready Gold tables designed for analytics and reporting.

</aside>

---
> [!NOTE]
> ## 🔥 Phase 6 - Building the Semantic layer using the Metric view 

<aside>

**Goal:** Databricks a récemment créé une nouvelle fonctionalité appelée ***Metric View*** qui fait à peu prés la même chose que les cubes tabulaires de SSAS facilitant une analyse multidimensionnelle associant dimensions et measures



- [ ]  **Implementing Metric Views**
    - [ ]   **Step 1/4 : Define Metric View using YAML & SQL(generate code by genie)**
          
        - [ ]  Create un schema nommé **retail_semantic**
        - [ ]  Nous pouvons créer sous ce schema un **Metric View** mais nous passerons par genie
              
> [!NOTE]
> **Rappel sur les objets de Unity catalog :**
> 
> L'architecture de stockage de Unity catalog est hiérarchisé comme suit: ***Metadata -> Catalog -> Schemas ->(Table,Views, Volume & Metric View)***


> [!IMPORTANT]
> **Goal of the question in genie:** Create a Metric View sans passer par SQL et YAML
> 
> **Question 4:** We want to create a metric view with name as ***retail_metrics*** in ***"reatail_q.retail_semantic"*** schema
> 
> Read the following table schema and sample data:
> 
> retail_d.retail_gold.fact_sales
> 
> retail_d.retail_gold.dim_product
> 
> retail_d.retail_gold.dim_customer
> 
> retail_d.retail_gold.dim_calendar
> 
> Decide the dimensions and measures based on the data and then create it as a metric view
> 
>  **Answer 4:** coller le result de genie dans un notebook metric_view.py et l'executer
              
   - [ ]  **Step 2/4: Github setup**
          
        - [ ]  Create repository structure
        - [ ]  Le folder 01_Notebook existe déja
              
   - [ ]  **Step 3/4: Modeling the Metric View**
          
        - [ ]  Spécifier les axes d'analyses(dimensions) et measures
              
              
   - [ ]  **Step 4/4: Consumption Metric View(Query it)**
          
        - [ ]  Use multiple language Sql, Python, Scala to query our Metric View
        - [ ]  Generate AI/BI Dashboards with Metric View
        - [ ]  Use AI/BI Genie to interacte with Metric View
             - [ ] Go to Genie spaces -> New space -> All -> retail_gold ->select the gold tables -> create
             - [ ] On peut maintenant poser des question via natural langage pour obtenir des réponses sur nos données
             - [ ] i.e: which customer is doing max transactions
        - [ ]  Plan a alert with Metric View
        - [ ]  Assistant with Metric View

> [!IMPORTANT]
> **Goal of the question in genie:** Consume Metric View using a Dashboard
> 
> **Question A:** Create a detailed analysis dashboard based on the metric view ***reatail_q.retail_semantic.retail_metrics***.Understand each on dimensions and measures and prepare the dashboard
>
> **Question A:** Update this dashboard based on the metric view ***reatail_q.retail_semantic.retail_metrics***
>
> **Question A:** All visualizations are empty; no fields are selected
> 
>  **Answers :** Une nouvelle page s'ouvre pour éditer le dashboard qu'on peut exporter, partager et publier dans Dashboards pour l'accès aux end users

**Result**: Nous disposons d'un outil complet d'analyse de données avec plusieurs possibilités d'utilisation (Dashboards, genie, SQL) and reporting.

</aside>
              


</aside>

---
> [!NOTE]
> ## Phase 7 - Building the Pipeline

<aside>

**Goal:** Automatiser le flux de lakehouse du debut à la fin de telle sorte que les données sont traitées de manière fiable de Bronze à Gold en passant par silver.

</aside>

### Current Setup (Configuration actuelle)

- 2 Ingestion pipelines  (Lakeflow connect): postgres_to_bronze & salesforce_to_bronze
- 1 notebook : blob_to_bronze (01_blob_to_bronze.py)
- 1 ETL pipeline nommé **retail_transaction** contenant 2 folders: bronze_to_silver et silver_to_gold
    - Dans ***bronze_to_silver*** il y'a les notebooks ***Product_catalog.py, inventory, account.py, opportunity.py, transactions.py***
    - Dans ***silver_to_gold*** on a les notebooks ***dim_product.py, dim_product.py, fact_sales.py***. Par simplicité on a juste créé le ***fact_sales.py***.
- 1 un dashboard retail_gold_analytics_dashboard à raffraichir
- 1 notebook pour la table dim_calendar (03_calendar.py)
- 1 notebook pour les Views (02_Gold_Views.sql)
- 1 notebook pour Metric View (04_Metric View.py)


To run each layer cleanly, we introduce **orchestration notebooks** that act as single entry points.

---

- [ ]  **Step 1/4: Create Orchestration Notebooks**
      
    - [ ]  Silver orchestration: Create one Silver orchestration notebook that triggers all 6 Silver notebooks in sequence. Use **`dbutils.notebook.run`** to run notebookes.
    - [ ]  Silver orchestration: Create one Gold orchestration notebook that triggers all 6 Silver notebooks in sequence. Use **`dbutils.notebook.run`** to run notebookes.
          
- [ ]  **Step 2/4: Create a Databricks Job**
      
    - [ ]  Go to **Databricks → Jobs & Pipelines** then create Create a new Job
    - [ ]  Create a new Job and Give it a clear name, for example: ***RetaiQ_end_to_end_job***
    - [ ]  Add Tasks: Une tache d'exécution sera associée à chacun des pipelines(ingestion & ETL) et des notebooks
    - [ ]  **Section 1: Add Ingestion Tasks**
        - [ ]  **Postgres to postgres_bronze schema of Unity catalog ( Ingestion pipeline : postgres_to_bronze)**
            - [ ]  Cliquez sur pipeline task
            - [ ]  Nommer le task: ***postgres_to_bronze***
            - [ ]  Definir le type de task : ***pipeline***
            - [ ]  Choisir le ingestion pipeline en question par ceux qui s'affichent: ***postgres_to_bronze***
            - [ ]  Cliquer sur ***Create task***
            - [ ]  Notre premiere task est créé on clique ***Add task***
        - [ ]  **Salesforce to salesforce_bronze schema of Unity catalog**
            - [ ]  Clique Ingestion pipeline pour un choisir une task  qu'on nomme ***salesforce_to_bronze***
            - [ ]  Delete the dependances entre les 2 tasks d'ingestion pour avoir une execution parallele(mais en mode gratuite on a la possibilité d'exécuter qu'une seule ingestion pipeline on garde alors la dependance)
        - [ ]  **Volume to blob_bronze schema of Unity catalog (notebook : 01_blob_to_bronze.py)**
            - [ ]  Add task pour le notebook ***blob_to_bronze***
            - [ ]  Clique Notebook pour choisir une task  qu'on nomme ***blob_to_bronze***
            - [ ]  Selectionner le notebook en question ***01_blob_to_bronze.py***
    - [ ]  **Section 2 : Add ETL Tasks**
        - [ ]  **postgres_bronze, salesforce_bronze, blob_bronze to retail_silver & retail_gold schemas of Unity catalog ( ETL pipeline : retail_transaction contenant 2 folders: bronze_to_silver et silver_to_gold)**
            - [ ]  Add task -> ETL pipeline
            - [ ]  Nommer le task: ***silver and gold***
            - [ ]  Choisir le ETL pipeline en question par ceux qui s'affichent: ***retail_transaction***
            - [ ]  Mettre une dépendance avec le blob_to_bronze
            - [ ]  Cliquer sur ***Create task*** 
  - [ ]  **Section 3 : Add Dashboard refresh Tasks**
        - [ ]  **postgres_bronze, salesforce_bronze, blob_bronze to retail_silver & retail_gold schemas of Unity catalog ( ETL pipeline : retail_transaction contenant 2 folders: bronze_to_silver et silver_to_gold)**
            - [ ]  Add task -> Dashboard
            - [ ]  Nommer le task: ***Dashboard_refresh***
            - [ ]  Choisir le dashboard en question par ceux qui s'affichent: ***retail_gold_analytics_dashboard***
> [!NOTE]
> On aura affaire à des pipelines tasks et notebook tasks selon le cas.
> Nous avons 2 types de pipelines:
> Pipeline type: ***Ingestion Pipeline*** & ***ETL Pipeline***
> - Les **Les notebook Views** dans gold n'ont pas besoin d'être exécuter (02_Gold_Views.sql & 04_Metric View.py); leurs maj est automatique après la création des tables dans gold.
> - Le **genie space** n'a pas aussi besoin d'execution; elle s'en occupe tout seul
      
      
- [ ]  **Step 3/4: Run and Validate**
      
    - [ ]  Click **Run All,**
    - [ ]  Monitor the job execution
    - [ ]  Ensure all tasks complete successfully
    - [ ]  Verify Bronze, Silver, and Gold tables are created correctly
          
- [ ]  **Step 4/4: Schedule, Trigger & some configs**
      
    - [ ]  Add a trigger to run the job on a schedule trigger type(for example daily or file arrival)
    - [ ]  For the first few days: (Mointor the runes and check logs)
    - [ ]  After three days, pause or adjust the trigger as needed
    - [ ]  Edit notification when job is failure
    - [ ]  Integrer git

> [!IMPORTANT]
> Maintenant que nous avons configuré et run notre job tout est automatisé: des qu'un nouveau fichier source est disponible le job va tout executer maintenant ainsi notre système automatique.
> 
---

<aside>

### 🎉 Congratulations

You’ve just built a complete **Data Lakehouse**.

This is **Lakehouse 1.0** and it represents the core foundation of real data engineering work.

</aside>


If you are **preparing for job interviews**, make sure you practice explaining this project:

- Why you designed the Lakehouse this way
- How data flows from Bronze to Silver to Gold
- How you ensured data quality and scalability
- How you automated everything with pipelines

Being able to clearly explain this project can be a **strong differentiator** and may be one of the reasons a company decides to hire you.

This is real, practical data engineering work.

</aside>

<aside>

### 🚀 Next Steps!

From here, you can take your Lakehouse to the next level by adding more advanced capabilities, such as:

- **Data quality checks**
    - Row counts, null checks, duplicates, and business rules
- **Reusable code and functions**
    - Shared transformation logic, configs, and utilities
- **New data sources**
    - APIs, Kafka, streaming data, and operational databases
- **CI/CD pipelines**
    - Automated testing, deployment, and environment promotion
- **Security and governance**
    - Access control, row-level security, and data masking
- **Monitoring and observability**
    - Pipeline health, alerts, and performance tracking
- **Incremental and streaming pipelines**
    - CDC, MERGE patterns, and real-time data processing

This is exactly how real-world data platforms evolve.

Strong foundations first, then continuous improvement.

</aside>


## Generate code to read a CSV file into a DataFrame 


### ###

