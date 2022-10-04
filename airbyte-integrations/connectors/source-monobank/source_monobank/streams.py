from abc import ABC
import time
from datetime import datetime
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator


class MonoTokenAuthenticator(TokenAuthenticator):
    def __init__(self, token: str):
        super().__init__(token=token, auth_method="X-Token", auth_header="Authorization")

    def get_auth_header(self) -> Mapping[str, Any]:
        return {self.auth_header: f"self._token"}


class MonoStream(HttpStream, ABC):
    url_base = "https://api.monobank.ua/"
    default_date_format = "%Y-%m-%d"

    def __init__(self, api_key, **kwargs):
        self.api_key = api_key
        super().__init__(**kwargs)

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {"X-Token": self.api_key}


class ClientInfo(MonoStream):
    url = "personal/client-info"
    primary_key = ["clientId"]
    http_method = "GET"

    def path(
        self, *, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.url

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield response.json()


class Currency(MonoStream):
    url = "bank/currency"
    primary_key = ["currencyCodeA"]

    def path(
        self, *, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.url

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield from response.json()


class Statement(MonoStream):
    url = "personal/statement/{account}/{start}/{end}"
    primary_key = ["id"]

    def __init__(self, start_date, **kwargs):
        self.start_date = start_date
        super().__init__(**kwargs)

    def path(
        self, *, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.url.format(
            account=stream_slice.get("account"),
            start=stream_slice.get("start"),
            end=stream_slice.get("end"),
        )

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        client = ClientInfo(**{"authenticator": self.authenticator, "api_key": self.api_key})
        for accounts in client.read_records(sync_mode=SyncMode.full_refresh):
            for account in accounts.get("accounts"):
                for dates in self._chunk_date_range(self.start_date):
                    yield from super().read_records(
                        stream_slice={"account": account["id"], "start": int(dates["start_date"]), "end": int(dates["end_date"])}, **kwargs
                    )

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield from response.json()

    def _chunk_date_range(self, start_date: datetime) -> List[str]:
        dates = []
        today = time.mktime(datetime.strptime(datetime.now().strftime("%Y-%m-%d"), "%Y-%m-%d").timetuple())
        while start_date < today:
            dates.append({"start_date": start_date, "end_date": start_date + 2682000})
            start_date += 2682000
        return dates
