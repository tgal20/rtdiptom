from datetime import datetime
import json
from dagster import (
    Definitions, 
    graph, 
    op, 
    schedule, 
    ScheduleEvaluationContext, 
    ConfigurableResource, 
    RunRequest, 
    DefaultScheduleStatus
)
from dagster_pyspark.resources import pyspark_resource

from rtdip_sdk.pipelines.sources.spark.eventhub import SparkEventhubSource
from rtdip_sdk.pipelines.transformers.spark.binary_to_string import BinaryToStringTransformer
from rtdip_sdk.pipelines.transformers.spark.fledge_opcua_json_to_pcdm import FledgeOPCUAJsonToPCDMTransformer
from rtdip_sdk.pipelines.destinations.spark.delta import SparkDeltaDestination

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

from pyspark.sql.functions import from_json

from pi_omf import SparkPIOMFDestination

import os
piomf_password = os.getenv('PIOMF_PASSWORD')
class DateFormatter(ConfigurableResource):
    format: str

    def strftime(self, dt: datetime) -> str:
        return dt.strftime(self.format)

packages = "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22,io.delta:delta-core_2.12:2.4.0"
my_pyspark_resource = pyspark_resource.configured(
    {"spark_conf": {"spark.default.parallelism": 1,
                    "spark.jars.packages": packages,
                    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension", 
                    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                    "spark.sql.warehouse.dir": "/home/rtdip/apps/local_scheduled/edgex_test_warehouse",
                    "spark.jars.excludes": "net.minidev:json-smart,com.nimbusds:lang-tag"
                    }
    }
)

startOffset = "-1"
endTime = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

startingEventPosition = {
  "offset": startOffset,  
  "seqNo": -1,            
  "enqueuedTime": None,   
  "isInclusive": True
}

endingEventPosition = {
  "offset": None,           
  "seqNo": -1,              
  "enqueuedTime": endTime,
  "isInclusive": True
}

ehConf  = {
    "eventhubs.connectionString": "Endpoint=sb://az-as-ehns-ex-n-seq00039-ew-dev-gen-02.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=qwQX5Kqv2NSxkQ+gt053u1ztv2nDpIBkq+AEhD088Qo=;EntityPath=az-as-eh-ex-n-seq00039-ew-dev-gen-09",
    "eventhubs.startingPosition": json.dumps({"offset": "0", "seqNo": -1, "enqueuedTime": None, "isInclusive": True}),
    }

json_schema = StructType([
    StructField("TagName", StringType(), True),
    StructField("EventTime", TimestampType(), True),
    StructField("Status", StringType(), True),
    StructField("Value", DoubleType(), True),
    StructField("ValueType", StringType(), True),
    StructField("ChangeType", StringType(), True),
])

# Pipeline op
@op(required_resource_keys={"pyspark"})
def pipeline(context):
    spark = context.resources.pyspark.spark_session
    source = SparkEventhubSource(spark, ehConf).read_batch() # change to read/write stream. Research streaming in dagster
    transformer = BinaryToStringTransformer(source, "body", "body").transform()
    transformer = transformer.withColumn("body", from_json("body", json_schema))
    transformer = transformer.select("body.*")
    SparkPIOMFDestination(
        data=transformer,
        options={},
        url="https://aewnw01528piwa1.europe.shell.com/piwebapi",
        username="piwa-euaccpicoll-s",
        password=piomf_password,
        message_length=1,
        batch_size=100000,
        compression=False,
        create_type_message=True).write_batch()


@graph
def fledge_pipeline():
    pipeline()

# Job Set Up
fledge_pipeline_job = fledge_pipeline.to_job(
    resource_defs={
                   "pyspark": my_pyspark_resource
                   }
)

# Define Schedule
@schedule(job=fledge_pipeline_job, cron_schedule="* * * * *", execution_timezone="Europe/London", default_status=DefaultScheduleStatus.RUNNING)
def process_data_schedule(
    context: ScheduleEvaluationContext,
    date_formatter: DateFormatter,
):
    formatted_date = date_formatter.strftime(context.scheduled_execution_time)

    return RunRequest(
        run_key=None,
        tags={"date": formatted_date},
    )

# Definitions/Resources for job to use
defs = Definitions(
    jobs=[fledge_pipeline_job],
    schedules=[process_data_schedule],
    resources={"date_formatter": DateFormatter(format="%Y-%m-%d")}
)