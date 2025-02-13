import yaml
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Charger la configuration
with open("config/config.yaml", "r") as file:
    config = yaml.safe_load(file)

SNOWFLAKE_CONN_ID = "snowflake_default"

def load_to_snowflake():
    """Charge les nouvelles données extraites dans Snowflake"""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    # Lire les données depuis le fichier CSV
    df = pd.read_csv('/tmp/odoo_production.csv')

    # Conversion des types de données
    df['date_start'] = pd.to_datetime(df['date_start']).astype(str)
    df['date_finished'] = pd.to_datetime(df['date_finished']).astype(str)

    # Définir le schéma de la table
    table_name = 'MRP_DATA_PRODUCTION'
    schema = 'PUBLIC'

    # Créer la table si elle n'existe pas
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS PUBLIC.MRP_DATA_PRODUCTION (
        id STRING PRIMARY KEY,
        name STRING,
        product_id STRING,
        product_qty FLOAT,
        state STRING,
        date_start TIMESTAMP,
        date_finished TIMESTAMP,
        qty_producing FLOAT,
        qty_produced FLOAT,
        duration_expected TIMESTAMP,
        duration TIMESTAMP,
        workcenter_id STRING,
        warehouse_id STRING,
        components_availability STRING,
        quality_check_fail INT,
        maintenance_count INT,
        extra_cost FLOAT,
        is_delayed BOOLEAN
    );
    """
    cur.execute(create_table_query)

    # Récupérer les identifiants déjà présents dans la table
    cur.execute(f"SELECT id FROM PUBLIC.MRP_DATA_PRODUCTION")
    existing_ids = {row[0] for row in cur.fetchall()}

    # Filtrer les nouvelles données pour exclure les enregistrements déjà présents
    new_data = df[~df['id'].isin(existing_ids)]

    if not new_data.empty:
        # Charger les nouvelles données dans Snowflake
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=new_data,
            table_name=table_name,
            schema=schema,
            quote_identifiers=False
        )

        if success:
            print(f"✅ {nrows} nouvelles lignes ont été chargées avec succès dans {schema}.{table_name}")
        else:
            print("⚠️ Échec du chargement des nouvelles données dans Snowflake")
    else:
        print("ℹ️ Aucune nouvelle donnée à charger.")

    cur.close()
    conn.close()
