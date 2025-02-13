import yaml
import xmlrpc.client
import pandas as pd
import os

def extract_odoo_data():
    # Charger la configuration
    with open("config/config.yaml", "r") as file:
        config = yaml.safe_load(file)

    odoo_cfg = config["odoo"]
    last_run_file = "/tmp/last_extraction_date.txt"

    # Connexion à Odoo
    common = xmlrpc.client.ServerProxy(f"{odoo_cfg['url']}/xmlrpc/2/common")
    uid = common.authenticate(odoo_cfg["db"], odoo_cfg["username"], odoo_cfg["password"], {})

    models = xmlrpc.client.ServerProxy(f"{odoo_cfg['url']}/xmlrpc/2/object")

    # Vérifier les champs disponibles
    available_fields = models.execute_kw(
        odoo_cfg["db"], uid, odoo_cfg["password"],
        "mrp.production", "fields_get", [], {"attributes": ["string", "type"]}
    )

    requested_fields = ['name', 'product_id', 'product_qty', 'state',
    'date_start', 'date_finished',
    'qty_producing', 'qty_produced', 'duration_expected', 'duration',
    'workcenter_id', 'warehouse_id', 'components_availability',
    'quality_check_fail', 'maintenance_count', 'extra_cost', 'is_delayed']
    valid_fields = [field for field in requested_fields if field in available_fields]

    if 'date_start' not in available_fields:
        print("⚠️ 'date_start' est absent, filtrage par date non possible.")
        date_filter = []
    else:
        last_extraction_date = None
        if os.path.exists(last_run_file):
            with open(last_run_file, "r") as f:
                last_extraction_date = f.read().strip()

        date_filter = [[('date_start', '>', last_extraction_date)]] if last_extraction_date else []

    has_access = models.execute_kw(
        odoo_cfg["db"], uid, odoo_cfg["password"],
        "mrp.production", "check_access_rights", ['read'], {"raise_exception": False}
    )

    if not has_access:
        raise PermissionError("🚫 L'utilisateur ne possède pas les droits de lecture sur 'mrp.production'.")

    records = models.execute_kw(
        odoo_cfg["db"], uid, odoo_cfg["password"],
        "mrp.production", "search_read", date_filter, {"fields": valid_fields}
    )

    if not records:
        print("⚠️ Aucun nouvel enregistrement trouvé.")
    else:
        df = pd.DataFrame(records)
        df.to_csv("/tmp/odoo_production.csv", index=False)

        if 'date_start' in valid_fields:
            latest_date = df['date_start'].max()
            with open(last_run_file, "w") as f:
                f.write(str(latest_date))

        print("✅ Extraction Odoo terminée !")

