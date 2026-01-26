from typing import Any, Optional

import dlt
from dlt.common.pendulum import pendulum
from dlt.sources.rest_api import (
    RESTAPIConfig,
    rest_api_resources,
)


@dlt.source(name="tripletex")
def tripletex_source(access_token: Optional[str] = dlt.secrets.value) -> Any:
    # Create a REST API configuration for the Tripletex API
    # Use RESTAPIConfig to get autocompletion and type checking
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api-test.tripletex.tech/v2",
            # default headers used for all requests
            "headers": {
                "Accept": "application/json"
            },
            # we add an auth config if the auth token is present
            "auth": (
                {
                    # dlt expects the HTTP Basic auth type to be named 'http_basic'
                    "type": "http_basic",
                    "username": "0",
                    "password": access_token
                }
                if access_token
                else None
            ),
        },
        # The default configuration for all resources and their endpoints
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
            "endpoint": {
                "params": {
                    "per_page": 100,
                },
            },
        },
        # resources can be specified as simple endpoint paths
        "resources": [
            {
                "name":"customers",
                "endpoint":"customer",
            },
            {
                "name":"contacts",
                "endpoint":"contact"
            },
        ],
    }

    yield from rest_api_resources(config)


def load_tripletex() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_tripletex",
        destination='snowflake',
        dataset_name="tripletex_data",
    )

    load_info = pipeline.run(tripletex_source())
    print(load_info)  # noqa: T201



if __name__ == "__main__":
    load_tripletex()
