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

    """
    set "write_disposition": "merge" if:
    - entities are mutable
    - you don't care about historical changes
    - you want idempotency
    - you want safe re-runs
    - does stage updates
    - must have primary_key set: "primary_key": "id",
    set "write_disposition": "append" if:
    - entities are immutable
    - you only fetch new data
    - you dont care about duplicates
    - you don't care about idempotency
    - does not stage updates
    """
    def _tripletex_datetime(value: Any) -> Any:
        """Format dlt incremental state as Tripletex-compatible ISO-8601 datetime."""
        if value is None:
            return value
        return str(value).replace("+00:00", "Z")

    resources: List[EndpointResource] = [
        {
            "name": "customers",
            "endpoint": {
                "path": "customer",
                # Expose change history so the cursor can track the latest update timestamp.
                "params": {"fields": "*,changes"},
                "incremental": {
                    "start_param": "changedSince",
                    "cursor_path": "changes[-1].date",
                    "initial_value": "1970-01-01T00:00:00Z",
                    "on_cursor_value_missing": "include",
                    "convert": _tripletex_datetime,
                },
            },
            "columns": {
                "account_manager": {"data_type": "text"},
                "department": {"data_type": "text"},
                "delivery_address": {"data_type": "text"},
                "category1": {"data_type": "text"},
                "category2": {"data_type": "text"},
                "category3": {"data_type": "text"},
            },
            "primary_key": "id",
            "write_disposition": "merge",
        },
        {
            "name": "contacts",
            "endpoint": "contact",
            "columns": {
                "department": {"data_type": "text"},
                "customer": {"data_type": "text"},
            },
            "primary_key": "id",
            "write_disposition": "merge",
        },
    ]

    config: RESTAPIConfig = {
        "client": client_cfg,
        "resources": resources,
    }

    yield from rest_api_resources(config)


def load_tripletex() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="tripletex",
        destination="snowflake",
        dataset_name="tripletex_dataset",
        # dev_mode=True,
    )

    load_info = pipeline.run(tripletex_source())
    print(load_info)  # noqa: T201



if __name__ == "__main__":
    load_tripletex()
