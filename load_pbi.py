import dlt
from datetime import datetime, timezone, timedelta
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
def all_data(start_date=None):
    return [get_ws(), get_activity(start_date)]

@dlt.resource(table_name="workspaces", write_disposition="replace")
def get_ws():
    source_config = {
        "client": {
            "base_url": dlt.config["sources.pbi.base_url"],
            "auth": auth,
            "paginator": JSONLinkPaginator(next_url_path ="continuationUri")
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

@dlt.resource(table_name="activity",write_disposition="merge", primary_key="id")
def get_activity(start_date=None):
    # If no date is provided, use yesterday's date
    if not start_date:
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        start_date = yesterday.strftime('%Y-%m-%d')
        print(start_date)
    

    source_config= {
        "client" : {
            "base_url" : dlt.config["sources.pbi.base_url"],
            "auth" : auth,
            "paginator": JSONLinkPaginator(next_url_path ="continuationUri"),
        },
        "resources": [
            {
                "name" : "activity",
                "primary_key" : "Id",
                "endpoint" : {
                    "path" : "admin/activityevents",
                    "params" : {
                        "startDateTime" : f"'{start_date}T00:00:00Z'",
                        "endDateTime" : f"'{start_date}T23:59:59Z'",
                    },
                    "data_selector" :"$.activityEventEntities[*]"
                },
                "max_table_nesting" : 0,
            }
        ],
    }
    source = rest_api_source(source_config)
    for resource in source: 
        yield resource


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="pbijeudi",
        destination=dlt.destinations.duckdb("Db/pbi.db"),
        dataset_name="raw",
        progress="log"
    )
    load_info = pipeline.run(all_data())
    print(load_info) 