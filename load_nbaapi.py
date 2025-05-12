import dlt
from dlt.sources.rest_api import rest_api_source
import os

from itertools import islice

def chunked_source(source, chunk_size):
    iterator = iter(source)
    while chunk := list(islice(iterator, chunk_size)):
        yield chunk

os.environ["EXTRACT__DATA_WRITER__FILE_MAX_ITEMS"] = "10000"
os.environ["EXTRACT__WORKERS"] = "2"
os.environ["NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS"] = "10000"


@dlt.source
def nba_source():
    return nba_player, player_shot, player_details, team_details

@dlt.resource(table_name="Players", write_disposition="replace")
def nba_player():
    source_config= {
        "client": {
            "base_url":"http://rest.nbaapi.com/api/",
            "paginator": {
                    "type": "page_number",
                    "base_page": 1,
                    "page_param": "pageNumber",
                    "total_path": None
                },
        },
        "resource_defaults": {
            "endpoint": {
                "response_actions": [
                    {"status_code": 404, "action": "ignore"},
                  ]
            },
        },
        "resources": [
            {
                "name":"players",
                "table_name":"players",
                "write_disposition":"replace",
                "endpoint": {
                    "path":"PlayerDataTotals/query",
                    "params": {
                        "pageSize": 10000
                    }
                },
            },
        ],
    }
    
    
    source = rest_api_source(source_config)
    
    source = chunked_source(source, 10000)  # Here, 1000 is the chunk size
    for chunk in source:
        yield chunk

@dlt.resource(table_name="Shots", write_disposition="replace")
def player_shot():
    source_config= {
        "client": {
            "base_url":"http://rest.nbaapi.com/api/",
        },
        "resource_defaults": {
            "endpoint": {
                "response_actions": [
                    {"status_code": 404, "action": "ignore"},
                  ]
            },
        },
        "resources": [
            {
                "name":"shots",
                "endpoint": {
                    "path":"ShotChartData",
                },
            },
        ],
    }
    
    
    source = rest_api_source(source_config)
    
    source = chunked_source(source, 10000)  # Here, 1000 is the chunk size
    for chunk in source:
        yield chunk

@dlt.resource(table_name="players_details", write_disposition="replace")
def player_details():
    source_config= {
        "client": {
            "base_url":"https://api.sportsdata.io/v3/nba/scores/json/",
        },
        "resources": [
            {
                "name":"players_details",
                "endpoint": {
                    "path":"Players",
                    "params": {
                        "key": dlt.secrets["nba.api_key"]
                    },
                },
            },
        ],
    }
    
    
    source = rest_api_source(source_config)
    
    source = chunked_source(source, 10000)  # Here, 1000 is the chunk size
    for chunk in source:
        yield chunk

@dlt.resource(table_name="teams_details", write_disposition="replace")
def team_details():
    source_config= {
        "client": {
            "base_url":"https://api.sportsdata.io/v3/nba/scores/json/",
        },
        "resources": [
            {
                "name":"players_details",
                "endpoint": {
                    "path":"AllTeams",
                    "params": {
                        "key": dlt.secrets["nba.api_key"]
                    },
                },
            },
        ],
    }
    
    
    source = rest_api_source(source_config)
    
    source = chunked_source(source, 10000)  # Here, 1000 is the chunk size
    for chunk in source:
        yield chunk

pipeline = dlt.pipeline(
    pipeline_name="nbadata",
    destination=dlt.destinations.duckdb("Db/nba.db"),
    dataset_name="nbaapi",
    progress=dlt.progress.tqdm(colour="yellow")
)


if __name__ == "__main__":
    load_info= pipeline.run(nba_source())
    print(load_info)