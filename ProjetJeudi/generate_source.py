import duckdb
import yaml
import sys
import os

def generate_dbt_source_with_columns(db_path, target_database):
    con = duckdb.connect(db_path)
    
    # Requête pour récupérer les tables ET leurs colonnes
    query = f"""
        SELECT 
            table_name, 
            column_name, 
            data_type
        FROM information_schema.columns 
        WHERE table_catalog = '{target_database}' 
        AND table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_name, ordinal_position
    """
    
    try:
        results = con.execute(query).fetchall()
    except Exception as e:
        print(f"Erreur : {e}")
        return

    if not results:
        print(f"Aucune donnée trouvée pour {target_database}")
        return

    # Structuration des données : on groupe les colonnes par table
    tables_dict = {}
    for table_name, col_name, dtype in results:
        if table_name not in tables_dict:
            tables_dict[table_name] = []
        
        tables_dict[table_name].append({
            "name": col_name
        })

    # Conversion en format liste pour dbt
    dbt_tables = []
    for table_name, columns in tables_dict.items():
        dbt_tables.append({
            "name": table_name,
            "columns": columns
        })

    source_config = {
        "version": 2,
        "sources": [
            {
                "name": target_database,
                "database": target_database,
                "schema": "main",
                "tables": dbt_tables
            }
        ]
    }

    # Écriture du fichier
    os.makedirs("models", exist_ok=True)
    filename = f"models/source_{target_database}.yml"
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(f"# Généré automatiquement pour la base : {target_database}\n")
        yaml.dump(source_config, f, default_flow_style=False, sort_keys=False)
    
    print(f"✅ Source générée avec colonnes : {filename}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python script.py <db_principal> <nom_base_attachee>")
    else:
        generate_dbt_source_with_columns(sys.argv[1], sys.argv[2])