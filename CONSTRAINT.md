# Constraint

- Format des fichiers supporté : csv

- Delimiter par defaut : ","

- Prise en compte de la ligne header ignorée par defaut

- Repertoire de travail pour importation "./dm_bigdata_worker_folder"

- Compatibilité et test : Spark_3.2.1 | spark-3.2.1-bin-hadoop3.2

- Status file : 
    * PREPARING : Preparation pour l'importation
    * IMPORTING : En cours d'importation
    * APPEND : Ajout sur un fichier existant
    * ERROR : Erreur produite pendant l'importation
    * DONE : Fichier correctement importé