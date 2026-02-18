# de-ecommerce-pipeline

Pipeline Data Engineering (projet académique) : ingestion, transformation et mise à disposition de données e-commerce pour analyse BI.

## Objectif
- Ingerer des données e-commerce (Kaggle) dans un data lake (S3)
- Nettoyer / transformer les données (Python/SQL)
- Charger des tables prêtes BI dans PostgreSQL (modèle en étoile)
- Orchestrer le pipeline avec Apache Airflow
- Construire un dashboard Power BI à partir des données curées

## Stack
- Python (pandas), SQL
- AWS S3 (raw / staging)
- PostgreSQL (curated / data warehouse)
- Apache Airflow (orchestration)
- Power BI (visualisation)

## Architecture (high-level)
Kaggle CSV → S3 (raw) → Python cleaning (staging) → PostgreSQL (curated) → Power BI  
Orchestration : Airflow (DAG batch)

## Structure du repo
- `scripts/` : scripts ingestion / cleaning / load
- `dags/` : DAGs Airflow
- `sql/` : création tables / requêtes utiles

## Avancement
- [ ] Setup repo + environnement
- [ ] Ingestion RAW vers S3
- [ ] Cleaning + staging (parquet)
- [ ] Chargement PostgreSQL (curated)
- [ ] DAG Airflow
- [ ] Dashboard Power BI
