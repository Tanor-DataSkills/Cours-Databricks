
Ressources: Perplexe, doc.databricks
**Ingesting data with the Auto Loader**

Dans le cas des streaming sur un cloud storage(Lorsque de nouveaux fichiers de données arrivent en continu dans le stockage cloud s3, adsl, volumes) vous avez besoin d’un moyen efficace de les traiter sans suivre manuellement les fichiers qui ont été ingérés. Auto Loader résout ce problème.

Auto loader surveille un emplacement de stockage cloud et traite de manière incrémentielle les nouveaux fichiers. 
Auto loader traite d'abord les fichiers existants dans le répertoire, puis **surveille en permanence les nouveaux arrivées**. Il stocke les informations de progression et l'etat des fichiers dans un **CheckpointLocation**( emplacement de point de contrôle); ce qui lui permet de reprendre exactement à partir de l’endroit où il s’est arrêté s’il est interrompu.

Auto loader dispose 2 modes pour se tenir informer de l'arrivée d'un ou des nouveaux fichiers dans le stockage cloud afin de traiter de manière incrémentielle tout en continu sans intervention manuelle:
- **Directory listing mode :** Autoloader fait des scans réguliers dans les répertoires du cloud storage pour s'informer de l'arriver de nouveaux fichiers
- **File notification mode :** Autoloader utilise les notifications cloud qui annoncent l'arrivée de nouveaux fichiers.Ce mode est plus efficace pour les charges de travail à grande échelle.

Databricks recommande le mode de **file notification mode with file events enabled** sur votre **external location** dans le catalogue Unity. Avec les events de fichier, le Autoloader reçoit des notifications directement lorsque les fichiers arrivent, ce qui réduit la latence et les coûts d’API cloud.

*Pour ingérer des données avec le Auto loader, vous utilisez le **cloudFiles format** avec **spark.readStream.** L’exemple suivant lit les fichiers JSON à partir d’Azure Data Lake Storage et les écrit dans une table de catalogue Unity :*
