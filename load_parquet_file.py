import dlt
from dlt.sources.filesystem import filesystem, read_parquet

@dlt.resource(name="users")
def users():
    return filesystem(
        bucket_url="file:///C:/Dev/Lobellia/Data/PBI/",
        file_glob="users.parquet") | read_parquet()
                               
pipeline = dlt.pipeline(
 pipeline_name="load_p", 
 destination="duckdb",
 dataset_name="parquet",
 progress="log"
)


info = pipeline.run(users(), table_name="users", write_disposition="replace")
print(info)
