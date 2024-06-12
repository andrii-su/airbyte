#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
import logging
from typing import Dict, Generator

from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    Status,
    SyncMode,
)
from airbyte_cdk.sources import Source

from .database import establish_connection, get_table_structure
from .utils import airbyte_message_from_data, convert_type

SUPPORTED_SYNC_MODES = [SyncMode.full_refresh]


class SourceFirebolt(Source):
    def check(self, logger: logging.Logger, config: json) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the integration
            e.g: if a provided Stripe API token can be used to connect to the Stripe API.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.json file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        try:
            with establish_connection(config, logger) as connection:
                # We can only verify correctness of connection parameters on execution
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {str(e)}")

    def discover(self, logger: logging.Logger, config: json) -> AirbyteCatalog:
        """
        Returns an AirbyteCatalog representing the available streams and fields in this integration.
        For example, given valid credentials to a Postgres database,
        returns an Airbyte catalog where each postgres table is a stream, and each table column is a field.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.json file

        :return: AirbyteCatalog is an object describing a list of all available streams in this source.
            A stream is an AirbyteStream object that includes:
            - its stream name (or table name in the case of Postgres)
            - json_schema providing the specifications of expected schema for this stream (a list of columns described
            by their names and types)
        """

        with establish_connection(config, logger) as connection:
            structure = get_table_structure(connection)

        streams = []
        for table, columns in structure.items():
            column_mapping = {c_name: convert_type(c_type, nullable) for c_name, c_type, nullable in columns}
            json_schema = {
                "type": "object",
                "properties": column_mapping,
            }
            streams.append(AirbyteStream(name=table, json_schema=json_schema, supported_sync_modes=SUPPORTED_SYNC_MODES))
        logger.info(f"Provided {len(streams)} streams to the Aribyte Catalog.")
        return AirbyteCatalog(streams=streams)

    def read(
        self,
        logger: logging.Logger,
        config: json,
        catalog: ConfiguredAirbyteCatalog,
        state: Dict[str, any],
    ) -> Generator[AirbyteMessage, None, None]:
        """
        Returns a generator of the AirbyteMessages generated by reading the source with the given configuration,
        catalog, and state.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
            the properties of the spec.json file
        :param catalog: The input catalog is a ConfiguredAirbyteCatalog which is almost the same as AirbyteCatalog
            returned by discover(), but
        in addition, it's been configured in the UI! For each particular stream and field, there may have been provided
        with extra modifications such as: filtering streams and/or columns out, renaming some entities, etc
        :param state: When a Airbyte reads data from a source, it might need to keep a checkpoint cursor to resume
            replication in the future from that saved checkpoint.
            This is the object that is provided with state from previous runs and avoid replicating the entire set of
            data everytime.

        :return: A generator that produces a stream of AirbyteRecordMessage contained in AirbyteMessage object.
        """

        logger.info(f"Reading data from {len(catalog.streams)} Firebolt tables.")
        with establish_connection(config, logger) as connection:
            with connection.cursor() as cursor:
                for c_stream in catalog.streams:
                    table_name = c_stream.stream.name
                    table_properties = c_stream.stream.json_schema.get("properties", {})
                    columns = list(table_properties.keys())
                    if columns:
                        # Escape columns with " to avoid reserved keywords e.g. id
                        escaped_columns = ['"{}"'.format(col) for col in columns]

                        query = "SELECT {columns} FROM {table}".format(columns=",".join(escaped_columns), table=table_name)
                        print(query)
                        cursor.execute(query)

                        logger.info(f"Fetched {cursor.rowcount} rows from table {table_name}.")
                        for result in cursor.fetchall():
                            message = airbyte_message_from_data(result, columns, table_name)
                            if message:
                                yield message
        logger.info("Data read complete.")
