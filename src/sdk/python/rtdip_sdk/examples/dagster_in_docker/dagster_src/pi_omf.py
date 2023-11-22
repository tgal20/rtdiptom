#  Copyright 2022 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import time
import requests
import math
from typing import Literal
from requests.exceptions import HTTPError
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    to_json,
    struct,
    col,
    row_number,
    collect_list,
    floor,
    when,
    coalesce,
    lit,
    size,
)
from pyspark.sql.window import Window
from py4j.protocol import Py4JJavaError
import gzip
import json

from rest_api import SparkRestAPIDestination
#from ..interfaces import DestinationInterface
#from ..._pipeline_utils.models import Libraries, SystemType
#from ..._pipeline_utils.constants import get_default_package


class SparkPIOMFDestination():
    """
    The Spark PI OMF Destination is used to write data to a Rest API PI endpoint.
    !!! Note
        While it is possible to use the `write_batch` method, it is easy to overwhlem a Rest API with large volumes of data.
        Consider reducing data volumes when writing to a Rest API in Batch mode to prevent API errors including throtting.

    Parameters:
        data (DataFrame): Dataframe to be merged into a Delta Table
        options (dict): A dictionary of options for streaming writes
        url (str): The Rest API Url
        username (str): Username for your PI destination
        password (str): Password for your PI destination
        message_length (int): The number of {time,value} messages to be packed into each row. Cannot exceed 70,000
        batch_size (int): The number of DataFrame rows to be used in each Rest API call
        parallelism (optional int): The number of concurrent calls to be made to the Rest API
        omf_version (optional str): The OMF version to use
        trigger (optional str): Frequency of the write operation. Specify "availableNow" to execute a trigger once, otherwise specify a time period such as "30 seconds", "5 minutes". Set to "0 seconds" if you do not want to use a trigger. (stream) Default is 10 seconds
        query_name (optional str): Unique name for the query in associated SparkSession
        query_wait_interval (optional int): If set, waits for the streaming query to complete before returning. (stream) Default is None
        compression (optional bool): If True, sends gzip compressed data in the API call
        timeout: (optional int): Time in seconds to wait for the type and container messages to be sent
        create_type_message (opitional bool): Can set to False if the type messages have already been created
        max_payload_length (optional int): Maximum allowed length of request content

    Attributes:
        checkpointLocation (str): Path to checkpoint files. (Streaming)
    """

    data: DataFrame
    options: dict
    url: str
    username: str
    password: str
    message_length: int
    batch_size: int
    parallelism: int
    omf_version: str
    trigger: str
    query_name: str
    query_wait_interval: int
    compression: bool
    timeout: int
    create_type_message: bool
    max_payload_length: int

    def __init__(
        self,
        data: DataFrame,
        options: dict,
        url: str,
        username: str,
        password: str,
        message_length: int,
        batch_size: int,
        parallelism: int = 8,
        omf_version: str = "1.1",
        trigger="1 minutes",
        query_name: str = "PIOMFRestAPIDestination",
        query_wait_interval: int = None,
        compression: bool = False,
        timeout: int = 30,
        create_type_message: bool = True,
        max_payload_length: int = 4194304,
    ) -> None:  # NOSONAR
        self.data = data
        self.options = options
        self.url = f"{url}/omf"
        self.username = username
        self.password = password
        self.message_length = message_length if message_length <= 70000 else 70000
        self.batch_size = batch_size
        self.parallelism = parallelism
        self.omf_version = omf_version
        self.trigger = trigger
        self.query_name = query_name
        self.query_wait_interval = query_wait_interval
        self.compression = compression
        self.timeout = timeout
        self.create_type_message = (
            self._send_type_message() if create_type_message == True else None
        )
        self.max_payload_length = max_payload_length
        self.container_ids = []
        self.data_headers = {
            "messagetype": "data",
            "action": "create",
            "messageformat": "JSON",
            "omfversion": self.omf_version,
            "x-requested-with": "xmlhttprequest",
        }

    def _send_type_message(self):
        json_message = [
            {
                "id": "RTDIPstring",
                "version": "1.0.0.0",
                "type": "object",
                "classification": "dynamic",
                "properties": {
                    "time": {"type": "string", "format": "date-time", "isindex": True},
                    "Value": {"type": "string"},
                },
            },
            {
                "id": "RTDIPnumber",
                "version": "1.0.0.0",
                "type": "object",
                "classification": "dynamic",
                "properties": {
                    "time": {"type": "string", "format": "date-time", "isindex": True},
                    "Value": {"type": "number"},
                },
            },
        ]
        self._setup_omf_execute(json.dumps(json_message), "type")

    def _send_container_message(self, micro_batch_df: DataFrame):
        distinct_values_df = micro_batch_df.select("TagName", "ValueType").distinct()
        json_message = []
        for row in distinct_values_df.collect():
            if row["TagName"] not in self.container_ids:
                json_message.append(
                    {
                        "id": row["TagName"],
                        "typeid": "RTDIPnumber"
                        if row["ValueType"].lower() in ["int", "integer", "float"]
                        else "RTDIPstring",
                    }
                )
                self.container_ids.append(row["TagName"])
        if len(json_message) > 0:
            self._setup_omf_execute(json.dumps(json_message), "container")

    def _setup_omf_execute(
        self, data: str, message_type: Literal["type", "container", "data"]
    ):
        headers = {
            "messagetype": message_type,
            "action": "create",
            "messageformat": "JSON",
            "omfversion": self.omf_version,
            "x-requested-with": "xmlhttprequest",
        }
        if self.compression:
            data = gzip.compress(bytes(data, "utf-8"))
            headers["compression"] = "gzip"
        response = requests.post(
            url=self.url,
            headers=headers,
            data=data,
            verify=False,
            timeout=self.timeout,
            auth=(self.username, self.password),
        )  # NOSONAR
        time.sleep(0.5)
        if response.status_code not in [200, 201, 202]:
            raise HTTPError(
                "Response status : {} .Response message : {}".format(
                    str(response.status_code), response.text
                )
            )  # NOSONAR

    def _pre_batch_records_for_api_call(self, micro_batch_df: DataFrame):
        micro_batch_df = (
            micro_batch_df.withColumn(
                "int_values",
                when(
                    col("ValueType") == "integer",
                    struct(
                        col("EventTime").alias("time"),
                        col("Value").cast("integer").alias("Value"),
                    ),
                ).otherwise(None),
            )
            .withColumn(
                "float_values",
                when(
                    col("ValueType") == "float",
                    struct(
                        col("EventTime").alias("time"),
                        col("Value").cast("float").alias("Value"),
                    ),
                ).otherwise(None),
            )
            .withColumn(
                "string_values",
                when(
                    col("ValueType") == "string",
                    struct(
                        col("EventTime").alias("time"),
                        col("Value").cast("string").alias("Value"),
                    ),
                ).otherwise(None),
            )
            .withColumnRenamed("TagName", "containerid")
            .withColumn(
                "row_number",
                row_number().over(Window().orderBy(col("containerid"))),
            )
            .withColumn(
                "batch_id", floor((col("row_number") / self.message_length) - 0.01)
            )
        )
        micro_batch_df = micro_batch_df.groupBy("batch_id", "containerid").agg(
            collect_list(col("int_values")).alias("int_values"),
            collect_list(col("float_values")).alias("float_values"),
            collect_list(col("string_values")).alias("string_values"),
        )

        micro_batch_df = (
            micro_batch_df.withColumn(
                "int_payload",
                when(size(col("int_values")) == 0, None).otherwise(
                    to_json(
                        struct(
                            col("containerid"),
                            col("int_values").alias("values"),
                        )
                    ),
                ),
            )
            .withColumn(
                "float_payload",
                when(size(col("float_values")) == 0, None).otherwise(
                    to_json(
                        struct(
                            col("containerid"),
                            col("float_values").alias("values"),
                        )
                    ),
                ),
            )
            .withColumn(
                "string_payload",
                when(size(col("string_values")) == 0, None).otherwise(
                    to_json(
                        struct(
                            col("containerid"),
                            col("string_values").alias("values"),
                        )
                    ),
                ),
            )
        )
        micro_batch_df = micro_batch_df.withColumn(
            "payload",
            coalesce("int_payload", "float_payload", "string_payload"),
        )
        return micro_batch_df.select("payload")

    def _group_rows(self, micro_batch_df: DataFrame):
        batch_count = math.ceil(micro_batch_df.count() / self.batch_size)
        micro_batch_df = micro_batch_df.withColumn(
            "row_number", row_number().over(Window().orderBy(lit("A")))
        ).withColumn("batch_id", col("row_number") % batch_count)
        return micro_batch_df.groupBy("batch_id").agg(
            collect_list("payload").cast("string").alias("payload")
        )

    def _api_micro_batch(self):
        self._send_container_message(self.data)
        pi_omf_df = self._pre_batch_records_for_api_call(self.data)
        longest_payload = pi_omf_df.selectExpr(
            "max(length(payload)) as max_length"
        ).collect()[0][0]

        if longest_payload * self.batch_size * 1.1 > self.max_payload_length:
            original_batch_size = self.batch_size
            self.batch_size = int(self.max_payload_length // (longest_payload * 1.1))
            print(
                f"Request content exceeds maximum allowed length; reducing batch size from {original_batch_size} to {self.batch_size}"
            )
        pi_omf_df = self._group_rows(pi_omf_df)
        return SparkRestAPIDestination(
            data=pi_omf_df,
            options=self.options,
            url=self.url,
            headers=self.data_headers,
            batch_size=1,
            method="POST",
            auth=(self.username, self.password),
            compression=self.compression,
            max_payload_length=4194304,
        )._api_micro_batch(micro_batch_df=pi_omf_df, _execute_transformation=False)

    def write_batch(self):
        """
        Writes batch data to a PI Rest API
        """
        try:
            self._api_micro_batch()
        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e

    def write_stream(self):
        """
        Writes streaming data to a PI Rest API
        """
        try:
            TRIGGER_OPTION = (
                {"availableNow": True}
                if self.trigger == "availableNow"
                else {"processingTime": self.trigger}
            )
            query = (
                self.data.writeStream.trigger(**TRIGGER_OPTION)
                .foreachBatch(self._api_micro_batch)
                .queryName(self.query_name)
                .outputMode("update")
                .options(**self.options)
                .start()
            )

            if self.query_wait_interval:
                while query.isActive:
                    if query.lastProgress:
                        logging.info(query.lastProgress)
                    time.sleep(self.query_wait_interval)

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e
