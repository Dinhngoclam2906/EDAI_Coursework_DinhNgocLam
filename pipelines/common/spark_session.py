"""
SparkSession factory for FinGuard pipelines.
Configures Delta Lake support and local MinIO (optional).
"""

import glob
import os
from pyspark.sql import SparkSession

_JARS_DIR    = os.environ.get("SPARK_DELTA_JARS_DIR", "D:/spark-delta-jars")
_HADOOP_BIN  = os.environ.get("HADOOP_HOME", "C:/hadoop") + "/bin"


def get_spark(app_name: str, use_minio: bool = False) -> SparkSession:
    jars = ",".join(glob.glob(f"{_JARS_DIR}/*.jar"))

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars", jars)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.extraLibraryPath", _HADOOP_BIN)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    )

    if use_minio:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )

    return builder.getOrCreate()
