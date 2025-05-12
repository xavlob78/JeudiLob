import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import OAuth2ClientCredentials
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator
from dlt.sources.rest_api import rest_api_source

# Authentication setup
auth = OAuth2ClientCredentials(
    access_token_url=dlt.config["sources.pbi.access_token_url"],
    client_id=dlt.secrets["sources.pbi.client_id"],
    client_secret=dlt.secrets["sources.pbi.client_secret"],
    access_token_request_data={
        "scope": dlt.config["sources.pbi.scope"],
        "grant_type": "client_credentials"
    }
)

@dlt.source
def all_data():
    return [get_ws]

@dlt.resource(table_name="workspaces", write_disposition="replace")
def get_ws():
    source_config = {
        "client": {
            "base_url": dlt.config["sources.pbi.base_url"],
            "auth": auth
        },
        "resources": [
            {
                "name": "workspaces",
                "primary_key": "id",
                "endpoint": {
                    "path": "admin/groups",
                    "params": {
                        "$top": 5000,
                        "$expand": "users,reports,datasets"
                    },
                },
                "max_table_nesting": 5
            },
        ],
    }

    source = rest_api_source(source_config)
    for resource in source:
        yield resource

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="pbijeudi",
        destination=dlt.destinations.duckdb("Db/pbi.db"),
        dataset_name="raw"
    )
    load_info = pipeline.run(all_data())
    print(load_info) 