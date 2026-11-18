# Analyse de tweets — pipeline Kafka

> Pipeline Data Engineering : **Kafka** comme bus unique, producteur → topic → consommateur → analyse de sentiment → SQLite.

**Statut** : 🟢 Réalisé  
**Catégorie** : ⚡ Traitement en temps réel  
**Portfolio** : [Profil GitHub](https://github.com/ousmajalloh405-eng)

---

## 1. Problème métier

Collecter un flux de messages (tweets simulés), les faire passer par un **bus d’événements Kafka**, analyser le sentiment, et stocker le résultat pour suivi d’opinion.

## 2. Architecture (Kafka uniquement pour le transport)

```
[Producteur Python]
        │
        │  publish
        ▼
  Topic Kafka : tweets-bruts
        │
        │  consume
        ▼
[Consommateur Python]
   analyse sentiment
        │
        ▼
     SQLite (tweets.db)
```

**Un seul outil de streaming : Kafka** (pas de Redis, pas d’ELK dans ce projet).

## 3. Technologies

| Composant | Technologie |
|-----------|-------------|
| Bus de données | **Apache Kafka** (Docker) |
| Producteur / consommateur | Python + `kafka-python` |
| Sentiment | Lexique simple |
| Stockage | SQLite |

## 4. Installation

```bash
git clone https://github.com/ousmajalloh405-eng/analyse-tweets.git
cd analyse-tweets
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
docker compose up -d
python3 main.py
```

Sans Kafka, le script bascule en **mode demo** (même logique).

## 5. Compétences pour le CV

Mise en place d’un pipeline de données en streaming avec **Apache Kafka** (producteur, topic, consommateur), traitement Python et stockage analytique.

---

*Projet réalisé dans le cadre de la formation Data — Sonatel Academy.*
