from datetime import datetime, timezone
from typing import Any, Dict, Generator, Optional

import dlt
import requests


BASE_URL = "https://api-test.tripletex.tech/v2"
DEFAULT_CHANGED_SINCE = "1970-01-01T00:00:00Z"
PAGE_SIZE = 100
REQUEST_TIMEOUT_SECONDS = 30


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _tripletex_get(
    endpoint: str,
    access_token: str,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    response = requests.get(
        f"{BASE_URL}/{endpoint}",
        auth=("0", access_token),
        headers={"Accept": "application/json"},
        params=params,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def _paged_values(
    endpoint: str,
    access_token: str,
    params: Optional[Dict[str, Any]] = None,
) -> Generator[Dict[str, Any], None, None]:
    offset = 0
    static_params = dict(params or {})

    while True:
        page_params = {**static_params, "from": offset, "count": PAGE_SIZE}
        payload = _tripletex_get(endpoint=endpoint, access_token=access_token, params=page_params)
        values = payload.get("values", [])
        if not values:
            break

        for value in values:
            yield value

        offset += len(values)
        if len(values) < PAGE_SIZE:
            break


@dlt.source(name="tripletex")
def tripletex_source(access_token: Optional[str] = dlt.secrets.value) -> Any:
    if not access_token:
        raise ValueError("Missing Tripletex access token")

    @dlt.resource(
        name="customers",
        primary_key="id",
        write_disposition="merge",
        columns={
            "account_manager": {"data_type": "text"},
            "department": {"data_type": "text"},
            "delivery_address": {"data_type": "text"},
            "category1": {"data_type": "text"},
            "category2": {"data_type": "text"},
            "category3": {"data_type": "text"},
        },
    )
    def customers() -> Generator[Dict[str, Any], None, None]:
        state = dlt.current.resource_state()
        changed_since = state.get("changed_since", DEFAULT_CHANGED_SINCE)
        next_changed_since = _utc_now_iso()

        for item in _paged_values(
            endpoint="customer",
            access_token=access_token,
            params={"changedSince": changed_since},
        ):
            yield item

        state["changed_since"] = next_changed_since

    @dlt.resource(
        name="contacts",
        primary_key="id",
        write_disposition="merge",
        columns={
            "department": {"data_type": "text"},
            "customer": {"data_type": "text"},
        },
    )
    def contacts() -> Generator[Dict[str, Any], None, None]:
        # Tripletex /contact does not support changedSince, so full extraction is required.
        yield from _paged_values(endpoint="contact", access_token=access_token)

    yield customers
    yield contacts


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
