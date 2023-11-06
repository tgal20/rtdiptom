from rtdip_sdk.pipelines.destinations import SparkDeltaDestination
from rtdip_sdk.pipelines.transformers import MISOToMDMTransformer
from rtdip_sdk.pipelines.sources import MISODailyLoadISOSource

from pyspark.sql import SparkSession

from dagster import asset
import shutil


@asset  # add the asset decorator to tell Dagster this is an asset
def run_miso_ingest():
    # First Clear local files
    shutil.rmtree("spark-warehouse")

    spark = (
        SparkSession.builder.config(
            "spark.jars.packages", "io.delta:delta-core_2.12:2.4.0"
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    # Build the query
    source_df = MISODailyLoadISOSource(
        spark=spark,
        options={
            "load_type": "actual",
            "date": "20230520",
        },
    ).read_batch()

    transform_value_df = MISOToMDMTransformer(
        spark=spark, data=source_df, output_type="usage"
    ).transform()

    transform_meta_df = MISOToMDMTransformer(
        spark=spark, data=source_df, output_type="meta"
    ).transform()

    SparkDeltaDestination(
        data=transform_value_df,
        options={"partitionBy": "timestamp"},
        destination="miso_usage_data",
    ).write_batch()

    SparkDeltaDestination(
        data=transform_meta_df,
        options={"partitionBy": "timestamp"},
        destination="miso_meta_data",
    ).write_batch()

    spark.stop()
