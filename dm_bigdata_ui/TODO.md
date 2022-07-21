# TODO

## Backlog

- Status Server View
- Authentification View

## In progress

-

## Done

- Filtre View
- Status View
- Import View
- Setting View
- Data View

## Bug

- le filtrage est fait en generant du code SQL avec like %%, ce qui pose probleme lors du parse URL avec certains caracteres speciaux. ex : +243

## Correction bug

- le filtrage commencant par des digits pose probleme => Cela etait dû au caractere '%' au debut de l'operateur 'like' qui une fois collé aux digits le reconvertisait au parse URL => La solution a ete d'echaper le caractere '%' par '%25' au debut et la fin de 'like'

