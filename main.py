"""Pipeline d'analyse de tweets avec Kafka comme bus de données."""

import sys
import sqlite3
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from producer import publier_vers_kafka, publier_mode_demo
from consumer import consommer_kafka, consommer_liste

DB_PATH = Path(__file__).parent / "data" / "tweets.db"
BOOTSTRAP = "localhost:9092"
N_TWEETS = 25


def kafka_disponible(bootstrap: str = BOOTSTRAP) -> bool:
    import socket
    host, _, port = bootstrap.partition(":")
    port = int(port or 9092)
    try:
        with socket.create_connection((host, port), timeout=1.5):
            return True
    except OSError:
        return False


def afficher_resume(db_path: Path) -> None:
    if not db_path.exists():
        print("Aucune base trouvée.")
        return
    conn = sqlite3.connect(str(db_path))
    print("\n=== Résumé sentiments ===")
    for row in conn.execute(
        "SELECT sentiment, COUNT(*) FROM tweets GROUP BY sentiment ORDER BY COUNT(*) DESC"
    ):
        print(f"  {row[0]:10} : {row[1]}")
    print("\nExemples :")
    for row in conn.execute(
        "SELECT id, sentiment, substr(text, 1, 50) FROM tweets LIMIT 5"
    ):
        print(f"  [{row[0]}] {row[1]:8} | {row[2]}")
    conn.close()


def main():
    print("=== Pipeline tweets (Kafka) ===\n")

    if kafka_disponible():
        print("Kafka détecté → mode pipeline réel\n")
        print("--- Production ---")
        n = publier_vers_kafka(N_TWEETS, BOOTSTRAP)
        print(f"{n} messages publiés sur le topic tweets-bruts\n")
        print("--- Consommation + sentiment ---")
        import time
        time.sleep(1)
        traites = consommer_kafka(DB_PATH, BOOTSTRAP, max_messages=N_TWEETS)
        print(f"{traites} tweets traités et stockés")
    else:
        print("Kafka non disponible sur localhost:9092")
        print("→ Mode DEMO (même pipeline logique, sans broker)\n")
        print("Pour le mode Kafka :")
        print("  docker compose up -d")
        print("  puis relancer: python3 main.py\n")
        print("--- Production (demo) ---")
        tweets = publier_mode_demo(N_TWEETS)
        print("--- Consommation + sentiment (demo) ---")
        traites = consommer_liste(tweets, DB_PATH)
        print(f"{traites} tweets traités et stockés")

    afficher_resume(DB_PATH)
    print(f"\nBase : {DB_PATH}")
    print("Pipeline terminé.")


if __name__ == "__main__":
    main()
