from typing import Any, List, Optional

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import (
    ClientConfig,
    EndpointResource,
    RESTAPIConfig,
    OffsetPaginatorConfig,
)


@dlt.source(name="tripletex")
def tripletex_source(access_token: Optional[str] = dlt.secrets.value) -> Any:
    paginator_cfg: OffsetPaginatorConfig = {
        "type": "offset",
        "offset": 0,
        "offset_param": "from",
        "limit_param": "count",
        "limit": 100,
        "total_path": "fullResultSize",
    }

    client_cfg: ClientConfig = {
        "base_url": "https://api-test.tripletex.tech/v2",
        "headers": {"Accept": "application/json"},
        "auth": (
            {
                "type": "http_basic",
                "username": "0",
                "password": access_token,
            }
            if access_token
            else None
        ),
        "paginator": paginator_cfg,
    }

    resources: List[EndpointResource] = [
        {
            "name": "customers",
            "endpoint": "customer",
            "columns": {
                "account_manager": {"data_type": "text"},
                "department": {"data_type": "text"},
                "delivery_address": {"data_type": "text"},
                "category1": {"data_type": "text"},
                "category2": {"data_type": "text"},
                "category3": {"data_type": "text"},
            },
        },
        {
            "name": "contacts",
            "endpoint": "contact",
            "columns": {"department": {"data_type": "text"}},
        },
    ]

    config: RESTAPIConfig = {
        "client": client_cfg,
        "resources": resources,
    }

    yield from rest_api_resources(config)


def load_tripletex() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_tripletex",
        destination="snowflake",
        dataset_name="tripletex_data",
    )

    load_info = pipeline.run(tripletex_source())
    print(load_info)  # noqa: T201



if __name__ == "__main__":
    load_tripletex()
