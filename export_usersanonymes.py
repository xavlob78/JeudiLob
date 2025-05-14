import dlt
from dlt.sources.rest_api import rest_api_source
from dlt.sources.helpers.rest_client.auth import OAuth2ClientCredentials
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator
import hashlib


auth = OAuth2ClientCredentials(
    access_token_url=dlt.secrets["microsoft_graph.token_url"],
    client_id=dlt.secrets["microsoft_graph.client_id"],
    client_secret=dlt.secrets["microsoft_graph.client_secret"],
    access_token_request_data={
        "grant_type": "client_credentials",
        "scope": dlt.secrets["microsoft_graph.scopes"]
    } 
)


SALT = dlt.secrets["microsoft_graph.salt"]

def anonymize_user(col: str, salt: str):
    return hashlib.sha256((salt + col).encode("utf-8")).hexdigest()

@dlt.source(name="ad")
def all_data():
    return get_users()

@dlt.resource(name="users", write_disposition="replace")
def get_users():
    config = {
        "client": {
            "base_url": "https://graph.microsoft.com/v1.0/",
            "auth": auth,
            "paginator": JSONLinkPaginator(next_url_path ='"@odata.nextLink"')
        },
        "resources": [
            {
                "name": "users",
                "primary_key": "id",
                "endpoint": {
                    "path": "users",
                    "params": {
                        "$select": "displayName,givenName,mail,mobilePhone, department, employeeId, jobTitle, officeLocation"
                    },
                    "data_selector": "value[*]",
                }
            }
    ]
}


    client = rest_api_source(config)
    for user in client:
        display_name = user.get("displayName")
        given_name = user.get("givenName")
        mail = user.get("mail")
        mobile_phone = user.get("mobilePhone")
        if display_name:
            pseudo = anonymize_user(display_name, SALT)
            user["displayName"] = pseudo
        if given_name:
            pseudo = anonymize_user(display_name, SALT)
            user["givenName"] = pseudo
        if mail:
            pseudo = anonymize_user(display_name, SALT)
            user["mail"] = pseudo
        if mobile_phone:
            pseudo = anonymize_user(display_name, SALT)
            user["mobilePhone"] = pseudo

        yield user


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="ad",
        destination="filesystem",
        dataset_name="files",
        progress="log"
    )

    # Exécuter uniquement la ressource `users`
    pipeline.run(all_data(), loader_file_format="parquet")
