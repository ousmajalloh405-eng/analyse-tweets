"""Analyse de sentiment simple (lexique FR/EN)."""

POSITIFS = {
    "bon", "bonne", "super", "génial", "excellent", "top", "love", "aime",
    "merci", "bravo", "parfait", "content", "heureuse", "heureux", "good",
    "great", "awesome", "amazing", "best", "happy", "nice", "cool",
}

NEGATIFS = {
    "mauvais", "nul", "horrible", "déçu", "decu", "pire", "hate", "déteste",
    "arnaque", "scandale", "problem", "problème", "bug", "lent", "cher",
    "bad", "worst", "terrible", "awful", "poor", "angry", "sad",
}


def analyser_sentiment(texte: str) -> str:
    if not texte:
        return "neutre"
    mots = set("".join(c.lower() if c.isalnum() or c.isspace() else " " for c in texte).split())
    score = sum(1 for m in mots if m in POSITIFS) - sum(1 for m in mots if m in NEGATIFS)
    if score > 0:
        return "positif"
    if score < 0:
        return "negatif"
    return "neutre"
