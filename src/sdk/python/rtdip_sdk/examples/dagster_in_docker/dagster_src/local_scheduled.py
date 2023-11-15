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
                    "spark.sql.warehouse.dir": "/home/rtdip/apps/local_scheduled/edgex_test_warehouse"
                    }
    }
)

eventhub_connection_string = "Endpoint=sb://az-as-ehns-ex-n-seq00039-ew-dev-gen-01.servicebus.windows.net/;SharedAccessKeyName=az-as-ehap-ex-n-seq00039-ew-dev-gen-03;SharedAccessKey=tW2k0JGqgjr577/4l/8bSz8S7V+bkSYA0jFgmjwKMUc=;EntityPath=az-as-eh-ex-n-seq00039-ew-dev-gen-03"
eventhub_consumer_group = "$Default"

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

ehConf = {
'eventhubs.connectionString' : eventhub_connection_string,
'eventhubs.consumerGroup': eventhub_consumer_group,
'eventhubs.startingPosition' : json.dumps(startingEventPosition),
'eventhubs.endingPosition' : json.dumps(endingEventPosition),
'maxEventsPerTrigger': 1000
}

# Pipeline op
@op(required_resource_keys={"pyspark"})
def pipeline(context):
    spark = context.resources.pyspark.spark_session
    source = SparkEventhubSource(spark, ehConf).read_batch() # change to read/write stream
    transformer = BinaryToStringTransformer(source, "body", "body").transform()
    SparkDeltaDestination(transformer, {}, "edgex_test", "overwrite").write_batch()

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