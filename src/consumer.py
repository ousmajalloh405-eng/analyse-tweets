"""Consommateur Kafka : lit les tweets, analyse le sentiment, charge SQLite."""

import json
import sqlite3
from pathlib import Path
from typing import List, Dict

from sentiment import analyser_sentiment

TOPIC = "tweets-bruts"
GROUP = "analyse-tweets-group"


def initialiser_base(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tweets (
            id INTEGER PRIMARY KEY,
            timestamp TEXT,
            user TEXT,
            text TEXT,
            hashtag TEXT,
            sentiment TEXT
        )
    """)
    conn.commit()
    return conn


def inserer(conn: sqlite3.Connection, tweet: dict, sentiment: str, commit: bool = True) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO tweets
           (id, timestamp, user, text, hashtag, sentiment)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            tweet["id"], tweet["timestamp"], tweet["user"], tweet["text"],
            tweet.get("hashtag", ""), sentiment,
        ),
    )
    if commit:
        conn.commit()


def consommer_kafka(db_path: Path, bootstrap: str = "localhost:9092",
                    max_messages: int = 30, timeout_sec: float = 25.0) -> int:
    from kafka import KafkaConsumer
    conn = initialiser_base(db_path)
    consumer = KafkaConsumer(
        TOPIC, bootstrap_servers=bootstrap, group_id=GROUP,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=int(timeout_sec * 1000),
    )
    count = 0
    for msg in consumer:
        tweet = msg.value
        sentiment = analyser_sentiment(tweet.get("text", ""))
        inserer(conn, tweet, sentiment)
        count += 1
        print(f"  [CONSUMER] id={tweet.get('id')} → {sentiment}")
        if count >= max_messages:
            break
    consumer.close()
    conn.close()
    return count


def consommer_liste(tweets: List[Dict], db_path: Path) -> int:
    conn = initialiser_base(db_path)
    for tweet in tweets:
        sentiment = analyser_sentiment(tweet.get("text", ""))
        inserer(conn, tweet, sentiment, commit=False)
        print(f"  [DEMO] id={tweet.get('id')} → {sentiment}")
    conn.commit()
    conn.close()
    return len(tweets)
