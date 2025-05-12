import dlt
import snowflake.connector
import os
from itertools import islice

os.environ['LOAD__WORKERS'] = '2'
os.environ['NORMALIZE__WORKERS'] = '2'
os.environ['DATA_WRITER__BUFFER_MAX_ITEMS'] = '10000'
os.environ["EXTRACT__DATA_WRITER__FILE_MAX_ITEMS"] = "100000"
os.environ['NORMALIZE_DATA_WRITER__DISABLE_COMPRESSION'] = 'true'
os.environ['NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS'] = '50000'

def snowflake_table_resource(table_name: str):
    def get_rows(chunk_size=10000):
        conn = snowflake.connector.connect(
            user=dlt.secrets["snowflake.user"],
            password=dlt.secrets["snowflake.password"],
            account=dlt.secrets["snowflake.account"],
            warehouse=dlt.secrets["snowflake.warehouse"],
            database=dlt.secrets["snowflake.database"],
            schema=dlt.secrets["snowflake.schema"],
        )
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table_name} limit 50000")
        columns = [desc[0] for desc in cur.description]
        rows = (dict(zip(columns, row)) for row in cur)
        while True:
            chunk = list(islice(rows, chunk_size))
            if not chunk:
                break
            yield chunk
        cur.close()
        conn.close()
    return dlt.resource(get_rows, name=table_name, write_disposition="replace", parallelized=True)

@dlt.source
def snowflake_source():
    tables = ["CLIMATJOUR", "HISTORY", "FORCASTJOUR"]
    for table in tables:
        yield snowflake_table_resource(table)


pipeline = dlt.pipeline(
        pipeline_name="snow",
        destination=dlt.destinations.duckdb("Db/snow.db"),
        dataset_name="ods",
        progress="log"
    )

if __name__ == "__main__":
    load =pipeline.run(snowflake_source())
    print(load)
