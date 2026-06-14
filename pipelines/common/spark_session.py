"""
SparkSession factory for FinGuard pipelines.
Configures Delta Lake support and local MinIO (optional).
"""

import glob
import os
from pyspark.sql import SparkSession

_JARS_DIR    = os.environ.get("SPARK_DELTA_JARS_DIR", "")
_HADOOP_HOME = os.environ.get("HADOOP_HOME", "")

_DELTA_MAVEN = "io.delta:delta-spark_2.12:3.2.0"


def get_spark(app_name: str, use_minio: bool = False) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    )

    if _JARS_DIR:
        # Local Windows dev: JARs pre-downloaded to SPARK_DELTA_JARS_DIR
        jars = ",".join(glob.glob(f"{_JARS_DIR}/*.jar"))
        builder = builder.config("spark.jars", jars)
        if _HADOOP_HOME:
            builder = builder.config("spark.driver.extraLibraryPath", f"{_HADOOP_HOME}/bin")
    else:
        # Docker / Linux: delta-spark 3.x ships no bundled JAR — load via Ivy.
        # Extensions + catalog are set above; spark.jars.packages pulls the JAR
        # (uses ~/.ivy2 cache so no network hit after the first run).
        builder = builder.config("spark.jars.packages", _DELTA_MAVEN)

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
