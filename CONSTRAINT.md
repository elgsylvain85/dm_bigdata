# Constraint

- Format des fichiers supporté : csv

- Delimiter par defaut : ","

- Prise en compte de la ligne header ignorée par defaut

- application configurable depuis : ./config/application.properties

- Compatibilité et test : Spark_3.2.1 | spark-3.2.1-bin-hadoop3.2.4

- Status file : 
    * PREPARING : Preparation pour l'importation
    * IMPORTING : En cours d'importation
    * IMPORT ERROR : Erreur produite pendant l'importation
    * DONE : Fichier correctement importé
    * EXPORTING : En cours d'exportation
    * EXPORTED : Fichier exporté
    * EXPORT ERROR : Erreur produite pendant l'exportation
    * APPENDING : Les données importees s'ajoutent à l'existant sans joindre
    * OVERWRITING : Les données importees creent la base pour la premiere fois
    * JOINING : Les données importees se joient à l'existant
    * SAVING : Les donnees importées se preparent à etre persister
    * QUEUE : En attente d'importation

- Donnees de fichier sont regroupés à l'importation pour suprimer les doublons

- Contraite importation : 
    * Nouvelles données creees si base vide
    * Sinon Jointure si colonnes definies
    * Sinon ajouts
