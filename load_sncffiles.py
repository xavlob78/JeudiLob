import dlt
import os
from dlt.sources.filesystem import filesystem, read_csv

# Liste tous les fichiers CSV dans le dossier Data/Sncf
files = [f for f in os.listdir("./Data/Sncf/") if f.endswith('.csv')]

# Initialise le pipeline dlt
pipeline = dlt.pipeline(
    pipeline_name="sncf", 
    destination=dlt.destinations.duckdb("Db/sncf.db"),
    dataset_name="csv",
)

# Pour chaque fichier CSV, crée une ressource et charge-la dans la base DuckDB
for file in files:
    # Nom de la table basé sur le nom du fichier (sans extension)
    table_name = os.path.splitext(file)[0]
    filesystem_pipe = filesystem(
        bucket_url="file:///C:/Dev/Lobellia/Data/Sncf/", 
        file_glob=file
    ) | read_csv(delimiter=';')
    
    print("............." + table_name + "..........")

    # Exécute le pipeline pour ce fichier/table
    info = pipeline.run(filesystem_pipe, table_name=table_name, write_disposition="replace")
    print(info)
