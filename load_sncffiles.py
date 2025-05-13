import dlt
import os
from pathlib import Path
from dlt.sources.filesystem import filesystem, read_csv

DATA_DIR = Path("./Data/Sncf/")
BUCKET_URL = f"file:///{DATA_DIR.resolve()}".replace("\\", "/")
DB_PATH = "Db/sncf.db"


def main():
    files = [f.name for f in DATA_DIR.glob("*.csv")]
    pipeline = dlt.pipeline(
        pipeline_name="sncf",
        destination=dlt.destinations.duckdb(DB_PATH),
        dataset_name="csv",
        progress="log"
    )
    for file in files:
        table_name = os.path.splitext(file)[0]
        fs_pipe = filesystem(bucket_url=BUCKET_URL, file_glob=file) | read_csv(delimiter=';')
        print(f"--- {table_name} ---")
        info = pipeline.run(fs_pipe, table_name=table_name, write_disposition="replace")
        print(info)


if __name__ == "__main__":
    main()
