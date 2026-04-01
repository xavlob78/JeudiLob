import duckdb
import yaml
import sys
import os

def generate_yaml(db_path, target_database, target_schema):
    """
    db_path: Chemin vers le fichier .duckdb principal
    target_database: Nom de la base (ex: 'raw_data' ou 'main')
    target_schema: Nom du schéma (ex: 'staging' ou 'main')
    """
    # Connexion à la base
    con = duckdb.connect(db_path)
    
    # Requête pour extraire les tables et colonnes du catalogue et schéma spécifiés
    query = f"""
        SELECT 
            table_name, 
            column_name, 
            data_type
        FROM information_schema.columns 
        WHERE table_catalog = '{target_database}' 
        AND table_schema = '{target_schema}'
        ORDER BY table_name, ordinal_position
    """
    
    try:
        results = con.execute(query).fetchall()
    except Exception as e:
        print(f"❌ Erreur lors de l'interrogation de DuckDB : {e}")
        return

    if not results:
        print(f"⚠️ Aucune table trouvée pour {target_database}.{target_schema}")
        return

    # Structuration des données pour le YAML
    tables_dict = {}
    for table_name, col_name, dtype in results:
        if table_name not in tables_dict:
            tables_dict[table_name] = []
        
        tables_dict[table_name].append({
            "name": col_name
        })

    # Conversion au format dbt
    dbt_models = []
    for table_name, columns in tables_dict.items():
        dbt_models.append({
            "name": table_name,
            "columns": columns
        })

    output_data = {
        "version": 2,
        "models": dbt_models
    }

    # Création du fichier de sortie
    os.makedirs("models", exist_ok=True)
    filename = f"models/schema_{target_schema}.yml"
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(f"# Généré automatiquement - Base: {target_database}, Schéma: {target_schema}\n")
        yaml.dump(output_data, f, default_flow_style=False, sort_keys=False)
    
    print(f"✅ Succès ! {len(dbt_models)} modèles écrits dans : {filename}")

if __name__ == "__main__":
    # Vérification des arguments
    if len(sys.argv) < 4:
        print("Usage: python generate_dbt_assets.py <chemin_db_principal> <nom_base> <nom_schema>")
        print("Exemple: python generate_dbt_assets.py dev.duckdb raw_data public")
    else:
        db_path = sys.argv[1]
        db_name = sys.argv[2]
        schema_name = sys.argv[3]
        generate_yaml(db_path, db_name, schema_name)