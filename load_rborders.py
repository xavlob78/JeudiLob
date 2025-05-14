import dlt
from datetime import datetime
from dlt.sources.rest_api import (
    RESTAPIConfig,
    check_connection,
    rest_api_resources,
    rest_api_source,
)
from dlt.sources.helpers.rest_client.auth import APIKeyAuth


acrelec_auth = APIKeyAuth(
    name="X-API-Key",
    api_key=dlt.secrets["sources.bfrb.api_key"],
    location="header"
)
acrelec_url = dlt.config["sources.bfrb.url"]

@dlt.source(name="acrelec")
def acrelec_source(last_date :str):

    config: RESTAPIConfig = {
        "client": {
            "base_url": acrelec_url,
            "auth": acrelec_auth,
        },
        "resource_defaults": {
            "write_disposition": "replace",
        },
        "resources": [
            {
                "name": "store",
                "endpoint": {
                    "path": "/global/store",
                },
            },
            {
                "name": "order_picture",
                "write_disposition": "append",
                "primary_key": "order-picture-id",
                "endpoint": {
                    "path": "/sale/reports/order-picture?store-id={store-id}",
                    "params": {
                        "timestamp-from": last_date,
                        "store-id": {
                            "type": "resolve",
                            "resource": "store",
                            "field": "id",
                        },
                    },
                    "data_selector": "order-pictures[*]",
                },
            },
        ],
    }
    yield from rest_api_resources(config)

def load_acrelec(last_date :str, default="20240101000000") -> None:
    pipeline = dlt.pipeline(
        pipeline_name="rb_orders",
        destination=dlt.destinations.duckdb("Db/rb.db"),
        dataset_name="rb_orders",
        progress="log",
    )
    load_info = pipeline.run(acrelec_source(last_date))
    # Suppose you get the latest timestamp from load_info or your data
    print(load_info)

if __name__ == "__main__":
    load_acrelec("20240101000000")
