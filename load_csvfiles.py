import dlt
import os
from dlt.sources.filesystem import filesystem, read_csv

# Liste tous les fichiers CSV dans le dossier cible
files = [f for f in os.listdir("./Data/Sncf/") if f.endswith('.csv')]

# Initialise le pipeline dlt
pipeline = dlt.pipeline(
    pipeline_name="sncf", 
    destination=dlt.destinations.duckdb("Db/sncf.db"),
    dataset_name="csv",
    progress="log"
)

for file in files:
    # Crée une source pour chaque fichier
    table_name = os.path.splitext(file)[0]
    filesystem_pipe = filesystem(
        bucket_url="file:///C:/Dev/Lobellia/Data/Sncf/", 
        file_glob=file
    ) | read_csv(delimiter=';')

    print(f".............{table_name}..........")

    # Spécifie le nom de la table avec with_name()
    info = pipeline.run(filesystem_pipe, table_name=table_name, write_disposition="replace")
    print(info)
