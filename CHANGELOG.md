# CHANGELOG

## 0.0.1-SNAPSHOT : 8 juillet 2022

- Release alpha

## 0.0.2-SNAPSHOT : 11 juillet 2022

- Re-run import files after import work folder, import delimiter, or import header setting has been updated
- Re-run import files after Columns or Join has been updated
- Remove duplicate value from 2e join files
- Correction datatable on data view

## 0.1.0-SNAPSHOT : 15 Juillet 2022

- revision du code

## 0.1.1 : 15 Juillet 2022

- reajustement datatable 

## 0.2.0 : 18 Juillet 2022

- Revision recherche maintenant insensible à la case
- Affichage nombre des lignes
- Amelioration du filtre et rendu
- Amélioration clair et nette du design general
- Filtre, choix sur plusieurs fichiers à la fois
- Aperçu des données avant Importation

## 0.3.0 : 19 Juillet 2022

- 1.0.0 release

## 0.3.1 : 

- Correction bug à l'importation

## 0.4.0 : 21 Juillet 2022

- ajout option "Exclude header" à l'importation
- ouverture d'un port d'ecoute

## 0.5.0 : 25 Juillet 2022

- Mise en place du cache des donnees (jointures) au demarage, importation et mise à jour de la structure
- Fonction export

## 0.5.1 : 27 Juillet 2022

- Persister données jointures directement apres importation
- Travailler sur une table unique comprennant les donnees des jointures
- Option delimiter avant importation
- Correction bug saut champs vide
- Revision & correction du code

## 0.5.2 : 

- Contraite importation : 
    * Nouvelles données creees si base vide
    * Sinon Jointure si colonnes definies
    * Sinon ajouts
- Correction bug SparkException: Task failed while writing rows => Causé par certains champs null pendant l'ecriture (non supporté par DElta lake) => Solution remplacer les champs null par ""

## 0.5.3 : 29 Juillet 2022

- Rendre la jointure sequentielle lorsqu'il exist plusieurs colonnes de jointures

## 0.6.0 : 01 Aout 2022

- Integration Hadoop
- Pick file to import directly from form

## 0.6.1 : 

- Correction bug "Out of memory" while large file importing => Send by stream

