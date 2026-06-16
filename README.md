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
| Login | utilisateur DWH (non versionne) |
| Password | secret saisi dans Airflow |
| Port | `5432` |

### 4. Lancer le pipeline

UI Airflow: `http://10.1.1.7:8080`

1. Activer le DAG `elt_e2e`
2. Trigger manuel du DAG `elt_e2e`
3. Verifier les taches dans l'ordre:
     - `extract_meteo`, `extract_dvf`
     - `load_meteo_bronze`, `load_dvf_bronze`
     - `run_dbt`, puis `dbt_test`

## Flux ELT

Le flux principal est orchestre par le DAG `elt_e2e` dans `airflow/dags/elt_e2e.py`.

- E (Extract): API Open-Meteo + source DVF
- L (Load): chargement des fichiers en tables `bronze.*`
- T (Transform): `dbt run` puis `dbt test` (silver -> gold)

Verification des donnees apres un run complet:
- Bronze: `bronze.meteo_quotidien`, `bronze.dvf_2025_s1`
- Silver: `silver.dvf_mutations_gold`, `silver.meteo_quotidien_gold`
- Gold: `gold.fact_mutations`, `gold.dim_*`, `gold.meteo_quotidien`

## Lac de données Snowflake

Le flux Snowflake de la séance 7 est orchestré par le DAG
`elt_snowflake` dans `airflow/dags/elt_snowflake.py`.

Deux DAGs Airflow sont reliés pour séparer l'orchestration ELT et les
traitements dbt :

```text
elt_snowflake
├── extract_dvf
├── extract_meteo
├── prepare_dvf_for_stage
├── put_dvf_to_raw_stage
├── put_meteo_to_raw_stage
├── copy_dvf_to_bronze
├── copy_meteo_to_bronze
└── run_dbt_snowflake ──► déclenche dbt_platform_snowflake

dbt_platform_snowflake
├── dbt_deps
├── dbt_run_silver
├── dbt_run_gold
├── dbt_test
├── mark_silver_ready
└── mark_gold_ready
```

La tâche `run_dbt_snowflake` est un `TriggerDagRunOperator` : elle déclenche le
DAG `dbt_platform_snowflake`, attend sa fin, puis ne passe en succès que si le
DAG dbt se termine correctement.

Le lac utilise les stages internes du schéma `PLATFORM_DB.BRONZE` :

- `RAW_STAGE` reçoit les fichiers bruts extraits par Airflow.
- `REFINED_STAGE` est réservé aux exports transformés, notamment Parquet.

Convention de partitionnement du raw :

```text
@RAW_STAGE/dvf/annee=2025/dvf_2025.txt
@RAW_STAGE/meteo/date=YYYY-MM-DD/marseille_forecast.json
```

Déroulement du pipeline :

1. Airflow extrait les sources DVF et Open-Meteo.
2. Le connecteur Snowflake exécute `PUT` vers les partitions de `RAW_STAGE`.
3. `COPY INTO` charge les fichiers dans `BRONZE.DVF_2025_S1` et
   `BRONZE.METEO_RAW`.
4. Les données météo imbriquées sont exposées dans
   `BRONZE.METEO_QUOTIDIEN`.
5. `run_dbt_snowflake` déclenche `dbt_platform_snowflake`, qui exécute les
   traitements dbt côté Snowflake : `dbt_run_silver`, `dbt_run_gold`, puis
   `dbt_test`.

Tables produites par dbt sur Snowflake :

- `PLATFORM_DB.SILVER.DVF_MUTATIONS_GOLD`
- `PLATFORM_DB.SILVER.METEO_QUOTIDIEN_GOLD`
- `PLATFORM_DB.GOLD.DIM_COMMUNE`
- `PLATFORM_DB.GOLD.DIM_LOCAL`
- `PLATFORM_DB.GOLD.DIM_NATURE_MUTATION`
- `PLATFORM_DB.GOLD.DIM_TEMPS`
- `PLATFORM_DB.GOLD.FACT_MUTATIONS`
- `PLATFORM_DB.GOLD.METEO_QUOTIDIEN`

Contrôles Snowflake après un run :

```sql
LIST @PLATFORM_DB.BRONZE.RAW_STAGE;
SELECT COUNT(*) FROM PLATFORM_DB.BRONZE.DVF_2025_S1;
SELECT COUNT(*) FROM PLATFORM_DB.BRONZE.METEO_QUOTIDIEN;
SHOW TABLES IN SCHEMA PLATFORM_DB.SILVER;
SHOW TABLES IN SCHEMA PLATFORM_DB.GOLD;
```

Le target dbt Snowflake est défini dans `airflow/dbt_profiles/profiles.yml` et
utilise les variables d'environnement `DBT_SNOWFLAKE_*` pour éviter de
versionner des identifiants en clair.

## Gouvernance / Acces

### Roles Airflow

| Role | Capacites attendues |
|------|----------------------|
| `Dev` | Lire/declencher les DAGs, modifier Variables et Connections |
| `Lecture` | Lire DAGs, DAG Runs, Task Instances, logs; pas de modification |

Configuration UI:
1. `Admin -> Roles`: creer `Dev` et `Lecture`
2. `Admin -> Users`: creer un utilisateur par role

Test RBAC a realiser:
1. Se connecter avec l'utilisateur `Lecture`
2. Ouvrir `Admin -> Variables` puis tenter `Edit` ou `+`
3. Ouvrir `Admin -> Connections` puis tenter `Edit` ou `+`
4. Le resultat attendu est un refus (bouton absent/desactive ou erreur `403`)

### Gestion des secrets

Les DAGs lisent les secrets depuis Airflow, pas depuis le code versionne:

- Connection: `postgres_warehouse` (host/login/password/schema/port)
- Connection: `snowflake_platform` (account/user/password/warehouse/database/role)
- Variables:
    - `DVF_URL`
    - `OPEN_METEO_API_URL`

Rappel: ne jamais committer de mot de passe, token ou cle API en clair dans le depot.

### Roles Snowflake

La gouvernance Snowflake est portée par Terraform avec deux rôles principaux :

| Role Snowflake | Profil | Droits principaux |
|----------------|--------|-------------------|
| `ROLE_ENGINEER` | Data Engineer | Usage de `PLATFORM_WH`, usage de `PLATFORM_DB`, droits étendus sur `BRONZE`, `SILVER`, `GOLD` et sur le stage `RAW_STAGE` |
| `ROLE_ANALYST` | Data Analyst | Usage de `PLATFORM_WH`, usage de `PLATFORM_DB`, accès au schéma `GOLD` et lecture des tables analytiques |

Vérifications utiles :

```sql
SHOW ROLES LIKE 'ROLE_%';
SHOW GRANTS TO ROLE ROLE_ENGINEER;
SHOW GRANTS TO ROLE ROLE_ANALYST;
```

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
