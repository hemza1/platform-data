# platform-data

Pipeline de données end-to-end combinant **Airflow** (orchestration + EL) et **dbt** (transformations) sur un entrepôt **PostgreSQL**.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Airflow                                                     │
│                                                             │
│  extract_dvf_2025_s1      extract_open_meteo_marseille      │
│  ├── download_dvf         ├── fetch_and_save                │
│  ├── load_to_bronze       ├── load_to_bronze                │
│  └── transform_to_silver  └── transform_to_silver           │
│           │                           │                     │
│           └──────────┬────────────────┘                     │
│                      ▼                                      │
│              dbt_platform (triggered)                       │
│              ├── dbt_deps                                   │
│              ├── dbt_run_silver                             │
│              ├── dbt_run_gold                               │
│              ├── dbt_test                                   │
│              ├── mark_silver_ready  ──► Dataset consumers   │
│              └── mark_gold_ready    ──► Dataset consumers   │
└─────────────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  PostgreSQL (warehouse)                                     │
│  ├── bronze.dvf_2025_s1          (raw DVF)                 │
│  ├── bronze.meteo_quotidien      (raw météo)               │
│  ├── silver.dvf_mutations_gold   (dbt)                     │
│  ├── silver.meteo_quotidien_gold (dbt)                     │
│  └── gold.*                      (dims + facts, dbt)       │
└─────────────────────────────────────────────────────────────┘
```

## Stack

| Outil | Rôle |
|-------|------|
| **Airflow 3.1** | Orchestration, Extract, Load vers bronze |
| **dbt 1.9** | Transformations silver + gold + tests |
| **PostgreSQL 17** | Entrepôt de données |
| **Redis** | Broker Celery pour Airflow |
| **pgAdmin** | Interface admin PostgreSQL |

## Structure

```
platform-data/
├── airflow/
│   ├── Dockerfile              # Image Airflow + dbt-postgres
│   ├── docker-compose.yaml     # Services Airflow
│   ├── dbt_profiles/
│   │   └── profiles.yml        # Connexion dbt → PostgreSQL
│   └── dags/
│       ├── dvf_2025.py         # Pipeline DVF (extract → bronze → silver)
│       ├── open_meteo.py       # Pipeline météo (extract → bronze → silver)
│       └── dbt_platform_dag.py # DAG dbt (silver → gold → test)
├── dbt_platform/               # Projet dbt
│   ├── models/
│   │   ├── bronze/sources.yaml # Sources bronze déclarées
│   │   ├── silver/             # Modèles silver
│   │   └── gold/               # Dims + facts
│   └── dbt_project.yml
└── docker-compose.yml          # PostgreSQL + Redis + pgAdmin
```

## Démarrage

### 1. Infrastructure

```bash
# Depuis la racine
docker compose up -d
```

### 2. Airflow

```bash
cd airflow
docker compose build
docker compose up -d
```

### 3. Connexion PostgreSQL dans Airflow

Admin → Connections → `+`

| Champ | Valeur |
|-------|--------|
| Conn Id | `postgres_warehouse` |
| Conn Type | `Postgres` |
| Host | `postgres-warehouse` |
| Schema | `warehouse` |
| Login | `platform` |
| Password | `platform` |
| Port | `5432` |

### 4. Lancer le pipeline

Activer dans l'UI (`http://10.1.1.7:8080`) puis trigger manuel :
1. `dbt_platform` (si bronze déjà rempli)
2. ou `extract_dvf_2025_s1` + `extract_open_meteo_marseille` pour un cycle complet

## Modèles dbt

### Silver
| Modèle | Source | Description |
|--------|--------|-------------|
| `dvf_mutations_gold` | `bronze.dvf_2025_s1` | Mutations DVF nettoyées et dédoublonnées |
| `meteo_quotidien_gold` | `bronze.meteo_quotidien` | Météo quotidienne normalisée |

### Gold
| Modèle | Type | Description |
|--------|------|-------------|
| `dim_commune` | Dimension | Communes uniques |
| `dim_local` | Dimension | Types de locaux |
| `dim_nature_mutation` | Dimension | Natures de mutation |
| `dim_temps` | Dimension | Calendrier |
| `fact_mutations` | Fait | Mutations foncières avec clés de dims |
| `meteo_quotidien` | Fait | Météo quotidienne |

## Datasets Airflow

Après chaque run réussi de `dbt_platform`, deux datasets sont émis :

- `dbt://platform/silver/ready`
- `dbt://platform/gold/ready`

Tout DAG downstream (refresh BI, export, alertes) peut s'y abonner :

```python
from airflow.datasets import Dataset

@dag(schedule=[Dataset("dbt://platform/gold/ready")], ...)
def mon_dag_bi():
    ...
```
