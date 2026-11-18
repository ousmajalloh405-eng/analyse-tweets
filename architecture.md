# Architecture — Pipeline tweets (Kafka)

## Principe

**Kafka est le seul bus de données** entre la production et la consommation.

```
Producteur Python → topic tweets-bruts (Kafka) → Consommateur Python → SQLite
```

## Pourquoi Kafka ?

- Découplage producteur / consommateur
- Buffer en cas de pic de messages
- Standard des architectures streaming Data Engineer

## Hors périmètre

- Redis, ELK : non utilisés
- API X/Twitter : données simulées (pas de clé API)
