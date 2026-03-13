# Warehouse compute
resource "snowflake_warehouse" "platform_wh" {
  name           = "PLATFORM_WH"
  warehouse_size = "X-SMALL"
  auto_suspend   = 60
  auto_resume    = true
  comment        = "Warehouse principal de la plateforme"
}

# Database
resource "snowflake_database" "platform_db" {
  name    = "PLATFORM_DB"
  comment = "Base de données principale"
}

# Schémas médaillon
resource "snowflake_schema" "bronze" {
  database = snowflake_database.platform_db.name
  name     = "BRONZE"
  comment  = "Données brutes"
}

resource "snowflake_schema" "silver" {
  database = snowflake_database.platform_db.name
  name     = "SILVER"
  comment  = "Données nettoyées"
}

resource "snowflake_schema" "gold" {
  database = snowflake_database.platform_db.name
  name     = "GOLD"
  comment  = "Marts analytiques"
}

# Rôle Engineer : droits complets bronze/silver/gold
resource "snowflake_account_role" "engineer" {
  name    = "ROLE_ENGINEER"
  comment = "Data engineer : écriture et lecture sur tous les schémas"
}

# Rôle Analyst : lecture sur gold uniquement
resource "snowflake_account_role" "analyst" {
  name    = "ROLE_ANALYST"
  comment = "Analyste : lecture seule sur gold"
}

# Grants warehouse
resource "snowflake_grant_privileges_to_account_role" "engineer_wh" {
  account_role_name = snowflake_account_role.engineer.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.platform_wh.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "analyst_wh" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.platform_wh.name
  }
}

# Grants database
resource "snowflake_grant_privileges_to_account_role" "engineer_db" {
  account_role_name = snowflake_account_role.engineer.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.platform_db.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "analyst_db" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.platform_db.name
  }
}

# Engineer : ALL sur bronze, silver, gold
resource "snowflake_grant_privileges_to_account_role" "engineer_bronze" {
  account_role_name = snowflake_account_role.engineer.name
  privileges        = ["ALL PRIVILEGES"]
  on_schema {
    schema_name = "\"${snowflake_database.platform_db.name}\".\"${snowflake_schema.bronze.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "engineer_silver" {
  account_role_name = snowflake_account_role.engineer.name
  privileges        = ["ALL PRIVILEGES"]
  on_schema {
    schema_name = "\"${snowflake_database.platform_db.name}\".\"${snowflake_schema.silver.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "engineer_gold" {
  account_role_name = snowflake_account_role.engineer.name
  privileges        = ["ALL PRIVILEGES"]
  on_schema {
    schema_name = "\"${snowflake_database.platform_db.name}\".\"${snowflake_schema.gold.name}\""
  }
}

# Analyst : USAGE + SELECT sur gold uniquement
resource "snowflake_grant_privileges_to_account_role" "analyst_gold_usage" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = "\"${snowflake_database.platform_db.name}\".\"${snowflake_schema.gold.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "analyst_gold_select" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.platform_db.name}\".\"${snowflake_schema.gold.name}\""
    }
  }
}


# Stage raw : fichiers bruts partitionnés
resource "snowflake_stage" "raw_stage" {
  database = snowflake_database.platform_db.name
  schema   = snowflake_schema.bronze.name
  name     = "RAW_STAGE"
  comment  = "Lac de données - zone raw (fichiers bruts partitionnés)"
}

# Stage refined : fichiers Parquet transformés
resource "snowflake_stage" "refined_stage" {
  database = snowflake_database.platform_db.name
  schema   = snowflake_schema.bronze.name
  name     = "REFINED_STAGE"
  comment  = "Lac de données - zone refined (Parquet partitionnés)"
}

# Grant : engineer peut lire/écrire dans les stages
resource "snowflake_grant_privileges_to_account_role" "engineer_raw_stage" {
  account_role_name = snowflake_account_role.engineer.name
  privileges        = ["ALL PRIVILEGES"]
  on_schema_object {
    object_type = "STAGE"
    object_name = "\"${snowflake_database.platform_db.name}\".\"${snowflake_schema.bronze.name}\".\"${snowflake_stage.raw_stage.name}\""
  }
}