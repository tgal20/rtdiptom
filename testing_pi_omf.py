from src.sdk.python.rtdip_sdk.pipelines.sources.spark.eventhub import (
    SparkEventhubSource,
)
from src.sdk.python.rtdip_sdk.examples.dagster_in_docker.dagster_src.pi_omf import (
    SparkPIOMFDestination,
)
from src.sdk.python.rtdip_sdk.examples.dagster_in_docker.dagster_src.rest_api import (
    SparkRestAPIDestination,
)
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.binary_to_string import (
    BinaryToStringTransformer,
)
from src.sdk.python.rtdip_sdk.pipelines.utilities import SparkSessionUtility
import json
import pandas as pd
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
    FloatType,
    ArrayType,
    DoubleType,
)
from pyspark.sql.functions import explode, expr, col, from_json

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
piomf_password = os.getenv('PIOMF_PASSWORD')
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("MySparkSession")
    .config(
        "spark.jars.packages",
        "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22,io.delta:delta-core_2.12:2.4.0",
    )
    .config("spark.jars.excludes", "net.minidev:json-smart,com.nimbusds:lang-tag")
    .getOrCreate()
)


# ,net.minidev:json-smart:2.3,com.nimbus:lang-tag:1.4.3
def pipeline():
    spark = SparkSessionUtility().execute()
    eventhub_source_configuration = {
        "eventhubs.connectionString": "Endpoint=sb://az-as-ehns-ex-n-seq00039-ew-dev-gen-02.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=qwQX5Kqv2NSxkQ+gt053u1ztv2nDpIBkq+AEhD088Qo=;EntityPath=az-as-eh-ex-n-seq00039-ew-dev-gen-09",
        "eventhubs.startingPosition": json.dumps(
            {"offset": "0", "seqNo": -1, "enqueuedTime": None, "isInclusive": True}
        ),
    }
    df = SparkEventhubSource(spark, eventhub_source_configuration).read_batch()
    df1 = BinaryToStringTransformer(df, "body", "body").transform()
    json_schema = StructType(
        [
            StructField("TagName", StringType(), True),
            StructField("EventTime", TimestampType(), True),
            StructField("Status", StringType(), True),
            StructField("Value", DoubleType(), True),
            StructField("ValueType", StringType(), True),
            StructField("ChangeType", StringType(), True),
        ]
    )
    df = df1.withColumn("body", from_json("body", json_schema))
    df = df.select("body.*")
    #     df = spark.read.csv(
    #     "/Users/James.Broady/Documents/RTDIP_work/pcdm_sample_data.csv",
    #     header=True,
    #     inferSchema=True,
    # )
    # print(df.count())
    # top100kRows = df.limit(200000)
    return SparkPIOMFDestination(
        data=df,
        options={},
        url="https://aewnw01528piwa1.europe.shell.com/piwebapi",
        username="piwa-euaccpicoll-s",
        password=piomf_password,
        message_length=1,
        batch_size=100000,
        compression=False,
        create_type_message=True,
    ).write_batch()


if __name__ == "__main__":
    x = pipeline()
    # print(x)

# {"containerid":"US:OL:TT_1107A1_HAL_1107_9","values":[{"time":"2023-11-01T19:06:00.000Z","Value":"True"}]}, {"containerid":"US:OL:TT_1107A1_HAL_1107_9","values":[{"time":"2023-11-01T00:13:00.000Z","Value":"True"}]}]
