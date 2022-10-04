import json
import time
from datetime import datetime
from typing import Any, List, Mapping

import requests
from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import AirbyteConnectionStatus, Status
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from source_monobank.streams import ClientInfo, Currency, Statement, MonoTokenAuthenticator

class SourceMonobank(AbstractSource):
    def check_connection(self, logger: AirbyteLogger, config: json) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the integration
            e.g: if a provided Stripe API token can be used to connect to the Stripe API.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.yaml file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        try:
            headers = {
                "X-Token": config.get("api_key"),
            }
            request = requests.post("https://api.monobank.ua/personal/webhook", headers=headers)
            if request.status_code == 200:
                if request.json().get("status") == "ok":
                    return AirbyteConnectionStatus(status=Status.SUCCEEDED)
                else:
                    return AirbyteConnectionStatus(status=Status.FAILED, message=request.json())

            return AirbyteConnectionStatus(status=Status.FAILED, message=f"Request get status {request.status_code}")
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {str(e)}")

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """

        start_date = time.mktime(datetime.strptime(config.get("start_date", "2022-01-01"), "%Y-%m-%d").timetuple())
        auth = MonoTokenAuthenticator(token=config.get("api_key"))
        args = {"authenticator": auth, 'api_key': config.get("api_key")}
        incremental_args = {**args, "start_date": start_date}
        return [
            ClientInfo(**args),
            Currency(**args), 
            Statement(**incremental_args)
        ]
