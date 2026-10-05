
Ressources: Perplexe, doc.databricks

# Spark Structured Streaming vs Auto Loader

La différence principale est que **Spark Structured Streaming est le moteur de traitement**, tandis qu’**Auto Loader est une source d’ingestion spécialisée de Databricks**, conçue pour détecter et charger efficacement les nouveaux fichiers depuis un stockage cloud. Auto Loader fonctionne donc généralement **au-dessus de Spark Structured Streaming**. citeturn0search0

## Comparaison

| Aspect | Spark Structured Streaming | Auto Loader |
|---|---|---|
| Rôle | Traiter des données en continu ou par micro-lots | Ingérer les nouveaux fichiers déposés dans un stockage cloud |
| Sources | Kafka, fichiers, Delta Lake, rate, sockets, etc. | Principalement fichiers dans S3, ADLS, GCS ou stockage cloud compatible |
| Détection des fichiers | Utilise la source fichier classique de Spark | Utilise la source `cloudFiles`, optimisée pour la découverte des fichiers |
| Passage à l’échelle | Correct pour un volume modéré de fichiers | Adapté à des millions ou milliards de fichiers |
| Évolution du schéma | À gérer davantage manuellement | Inférence, évolution et récupération du schéma intégrées |
| Suivi des fichiers traités | Via les mécanismes de checkpoint de Structured Streaming | Checkpoint et métadonnées spécifiques pour éviter de retraiter les fichiers |
| Produit | Fonctionnalité Apache Spark | Fonctionnalité Databricks |

## Spark Structured Streaming

Avec Spark, on peut lire progressivement les nouveaux fichiers ainsi :

```python
df = (
    spark.readStream
         .format("json")
         .schema(schema)
         .load("/mnt/landing")
)
```

Spark surveille le répertoire et traite les nouveaux fichiers. Cette approche est suffisante lorsque le volume de fichiers et la complexité de l’arborescence restent raisonnables.

Spark Structured Streaming permet également de consommer d’autres sources, par exemple Kafka :

```python
df = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "...")
         .option("subscribe", "events")
         .load()
)
```

## Auto Loader

Auto Loader utilise la source `cloudFiles` :

```python
df = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "json")
         .option("cloudFiles.schemaLocation", "/mnt/schema")
         .load("/mnt/landing")
)
```

Il est conçu pour les zones d’atterrissage cloud où de nouveaux fichiers arrivent régulièrement. Il conserve les métadonnées des fichiers découverts dans le checkpoint et peut utiliser soit le listing optimisé, soit des notifications ou événements du stockage cloud. citeturn0search1

Auto Loader apporte notamment :

- Une découverte plus efficace des fichiers à grande échelle.
- La gestion de l’inférence et de l’évolution du schéma.
- La possibilité de traiter uniquement les fichiers jamais vus.
- La prise en charge de formats comme JSON, CSV, XML, Parquet, Avro, ORC et texte.
- La possibilité d’effectuer une ingestion continue ou planifiée avec `availableNow`. citeturn0search1

## Exemple pratique

Supposons que des fichiers JSON arrivent dans ADLS :

```text
/raw/events/2026/10/05/file-001.json
/raw/events/2026/10/05/file-002.json
```

Avec la source fichier Spark :

```python
spark.readStream \
    .format("json") \
    .schema(schema) \
    .load("/raw/events")
```

Avec Auto Loader :

```python
spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/checkpoints/events_schema") \
    .load("/raw/events")
```

Les deux solutions traitent les nouveaux fichiers, mais Auto Loader est généralement préférable lorsque le répertoire contient beaucoup de fichiers ou lorsque le schéma peut évoluer.

## Quand utiliser lequel ?

Utilisez **Spark Structured Streaming seul** lorsque :

- Vous consommez Kafka ou une autre source non basée sur des fichiers.
- Vous avez peu de fichiers.
- Vous contrôlez précisément le schéma.
- Vous avez besoin d’un traitement streaming général.

Utilisez **Auto Loader** lorsque :

- Des fichiers arrivent continuellement dans S3, ADLS ou GCS.
- Vous avez un grand nombre de fichiers.
- Vous voulez gérer automatiquement les nouveaux fichiers.
- Le schéma peut changer au fil du temps.
- Vous construisez une ingestion Databricks vers Delta Lake.

En résumé : **Spark Structured Streaming traite le flux ; Auto Loader facilite et optimise la découverte et l’ingestion des fichiers cloud**. Auto Loader n’est donc pas vraiment une alternative à Spark Streaming, mais plutôt une fonctionnalité qui l’utilise.



Dans le cas des streaming sur un cloud storage(Lorsque de nouveaux fichiers de données arrivent en continu dans le stockage cloud s3, adsl, volumes) vous avez besoin d’un moyen efficace de les traiter sans suivre manuellement les fichiers qui ont été ingérés. Auto Loader résout ce problème.

Auto loader surveille un emplacement de stockage cloud et traite de manière incrémentielle les nouveaux fichiers. 
Auto loader traite d'abord les fichiers existants dans le répertoire, puis **surveille en permanence les nouveaux arrivées**. Il stocke les informations de progression et l'etat des fichiers dans un **CheckpointLocation**( emplacement de point de contrôle); ce qui lui permet de reprendre exactement à partir de l’endroit où il s’est arrêté s’il est interrompu.

Auto loader dispose 2 modes pour se tenir informer de l'arrivée d'un ou des nouveaux fichiers dans le stockage cloud afin de traiter de manière incrémentielle tout en continu sans intervention manuelle:
- **Directory listing mode :** Autoloader fait des scans réguliers dans les répertoires du cloud storage pour s'informer de l'arriver de nouveaux fichiers
- **File notification mode :** Autoloader utilise les notifications cloud qui annoncent l'arrivée de nouveaux fichiers.Ce mode est plus efficace pour les charges de travail à grande échelle.

Databricks recommande le mode de **file notification mode with file events enabled** sur votre **external location** dans le catalogue Unity. Avec les events de fichier, le Autoloader reçoit des notifications directement lorsque les fichiers arrivent, ce qui réduit la latence et les coûts d’API cloud.

*Pour ingérer des données avec le Auto loader, vous utilisez le **cloudFiles format** avec **spark.readStream.** L’exemple suivant lit les fichiers JSON à partir d’Azure Data Lake Storage et les écrit dans une table de catalogue Unity :*


**Ingesting data with the autoloader**

Dans le cas des streaming sur un cloud storage(Lorsque de nouveaux fichiers de données arrivent en continu dans le stockage cloud s3, adsl, volumes) vous avez besoin d’un moyen efficace de les traiter sans suivre manuellement les fichiers qui ont été ingérés. Auto Loader résout ce problème.

Auto loader surveille un emplacement de stockage cloud et traite de manière incrémentielle les nouveaux fichiers. 
Auto loader traite d'abord les fichiers existants dans le répertoire, puis **surveille en permanence les nouveaux arrivées**. Il stocke les informations de progression et l'etat des fichiers dans un **CheckpointLocation**( emplacement de point de contrôle); ce qui lui permet de reprendre exactement à partir de l’endroit où il s’est arrêté s’il est interrompu.

Auto loader dispose 2 modes pour se tenir informer de l'arrivée d'un ou des nouveaux fichiers dans le stockage cloud afin de traiter de manière incrémentielle tout en continu sans intervention manuelle:
- **Directory listing mode :** Autoloader fait des scans réguliers dans les répertoires du cloud storage pour s'informer de l'arriver de nouveaux fichiers
- **File notification mode :** Autoloader utilise les notifications cloud qui annoncent l'arrivée de nouveaux fichiers.Ce mode est plus efficace pour les charges de travail à grande échelle.





Databricks recommande le mode de **file notification mode with file events enabled** sur votre **external location** dans le catalogue Unity. Avec les events de fichier, le Autoloader reçoit des notifications directement lorsque les fichiers arrivent, ce qui réduit la latence et les coûts d’API cloud.

*Pour ingérer des données avec le Auto loader, vous utilisez le **cloudFiles format** avec **spark.readStream.** L’exemple suivant lit les fichiers JSON à partir d’Azure Data Lake Storage et les écrit dans une table de catalogue Unity :*

>
         base_path = "abfss://container@storage.dfs.core.windows.net/autoloader/orders"
         schema_path = f"{base_path}/schema"
         checkpoint_path = f"{base_path}/checkpoint"

         (spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "json")
         .option("cloudFiles.schemaLocation", schema_path)
         .load("abfss://container@storage.dfs.core.windows.net/incoming/orders/")
         .writeStream
         .option("checkpointLocation", checkpoint_path)
         .trigger(availableNow=True)
         .toTable("sales.bronze.orders"))

> [!WARNING]
>
- **spark.readStream:** Pour la lecture en streaming ou par lot des fichiers stockés dans un Cloud Object Storage

- **.format("cloudFiles"):** La format("cloudFiles") méthode spécifie la nature du stockage(Cloud Object Storage); on a affaire à des cloudFiles.

- **.option("cloudFiles.format", "csv"):** spécifie le type de fichier;

- **.option("cloudFiles.schemaLocation", "/Volumes/retail_q/volumes/blob_source/_schema"):**

- **.option("header", "true"):**

- **.option("inferSchema", "true"):** n’est pas prise en charge pour les sources de streaming, car elle nécessite la lecture de l’ensemble du jeu de données.

- **.load("/Volumes/retail_q/volumes/blob_source/transactions_source/"):**

- **df.writeStream:**
- 
- **.outputMode("append"):** Le mode de sortie détermine quels enregistrements sont écrits :

> [!NOTE]
*append :* seules les nouvelles lignes sont écrites (par défaut pour la plupart des opérations)
*Update :* les lignes modifiées sont enregistrées
*complet :* toutes les lignes sont réécrites (utilisées avec des agrégations)

- **.option("checkpointLocation", "/Volumes/retail_q/volumes/blob_source/_checkpoint"):** Le checkpointLocation ou l’emplacement du point de contrôle stocke les informations de progression, ce qui permet au flux de reprendre à partir de l’endroit où il s’est arrêté en cas d’interruption.

- **.trigger(availableNow=True):**

- **.toTable("retail_q.blob_bronze.transactions"):**

> [!WARNING]
> *Le code généré par genie ne doit pas être directement utilisé; il doit être réadapté car il peut contenir des imperfections:*
>
> On peut :

 *1- Faire des **commentaires** explicites pour chaque étapes*

*2- Configurer des **paths et f string function** pour éviter de taper des noms de repertoires longs passible à des erreurs*

Ci-dessous un exemple de reading csv files avec Autoloader reformatté:

<img width="983" height="573" alt="image" src="https://github.com/user-attachments/assets/07d19f59-5de2-489b-9bc7-e5fc00465407" />

un exemple de writting csv files avec Autoloader reformatté:

<img width="973" height="294" alt="image" src="https://github.com/user-attachments/assets/4bbbe3cb-efff-4c0e-976b-be3e9a7a91dc" />
