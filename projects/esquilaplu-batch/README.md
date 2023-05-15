# Batch

## Introduction

* Scrapping des données météo
* S'assure de ne scrapper que les données des dates manquantes
* Envoie les données sur un S3

## Lancement

```bash
docker build -t test .
docker run --rm -it -v "`pwd`/secrets:/app/secrets" --name test test
```

## Ubiquitous language

* station météo
* donnée météo horraire pour une station
* données météo horraires pour toutes les stations
* durée maximale d'hitorique de données météo requêtable
* fournisseur de données météo
* pluviométrie
* intervalle de temps
