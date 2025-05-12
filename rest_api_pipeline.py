import dlt

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="bfrb_store_rest",
        destination="duckdb",
        dataset_name="bfrb_store_data",
        progress="log",
        dev_mode=True,
        credentials={"database": "./data/bfrb_store.duckdb"}
    )
    pipeline.run(source) 