"""Producteur Kafka : envoie des tweets (simulés) vers le topic."""

import json
import random
import time
from datetime import datetime

TOPIC = "tweets-bruts"

EXEMPLES = [
    "Super service client, vraiment top !",
    "Produit excellent, je recommande",
    "Livraison trop lente, déçu",
    "Prix trop cher pour la qualité",
    "Bonne expérience globale",
    "Bug sur l'application mobile",
    "Love this product, amazing quality",
    "Worst purchase ever, terrible",
    "Merci pour le suivi rapide",
    "Arnaque totale, à éviter",
    "Parfait, content de mon achat",
    "Problème de connexion aujourd'hui",
]


def generer_tweet(i: int) -> dict:
    return {
        "id": i,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "user": f"user_{random.randint(1, 50)}",
        "text": random.choice(EXEMPLES),
        "hashtag": random.choice(["#produit", "#service", "#app", "#livraison"]),
    }


def publier_vers_kafka(n: int = 30, bootstrap: str = "localhost:9092") -> int:
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
    )
    for i in range(1, n + 1):
        tweet = generer_tweet(i)
        producer.send(TOPIC, key=tweet["id"], value=tweet)
        print(f"  [PRODUCER] → tweet {i}: {tweet['text'][:40]}...")
        time.sleep(0.15)
    producer.flush()
    producer.close()
    return n


def publier_mode_demo(n: int = 30) -> list:
    tweets = []
    for i in range(1, n + 1):
        t = generer_tweet(i)
        tweets.append(t)
        print(f"  [DEMO] tweet {i}: {t['text'][:40]}...")
    return tweets
