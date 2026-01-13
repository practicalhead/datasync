"""
Source Cluster Data Extractor

PySpark job for extracting filtered data from Hive tables on the source
Cloudera cluster. Applies row-level filtering and column selection before
writing to intermediate Parquet format for transfer.

This job runs on the SOURCE cluster where the SparkContext is bound to
the source Hive metastore.
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime
from typing import Dict, Any, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config import load_config, TableConfig, JobConfig
from common.logging_utils import (
    setup_logging,
    AuditLogger,
    compute_dataframe_checksum
)

logger = logging.getLogger("datasync.extraction")


class HiveDataExtractor:
    """
    Extracts filtered data from Hive tables and writes to staging location.

    Designed to run on the source Cloudera cluster with SparkContext bound
    to the local Hive metastore.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: JobConfig,
        audit_logger: AuditLogger
    ):
        self.spark = spark
        self.config = config
        self.audit = audit_logger
        self.extraction_metadata: Dict[str, Dict[str, Any]] = {}

    def _build_query(self, table_config: TableConfig) -> str:
        """
        Build the extraction SQL query with column selection and filters.

        Args:
            table_config: Table configuration with columns and filters

        Returns:
            SQL query string
        """
        columns = table_config.get_select_columns()
        source_table = table_config.get_full_source_name()
        where_clause = table_config.get_where_clause()

        query = f"SELECT {columns} FROM {source_table}"
        if where_clause:
            query += f" WHERE {where_clause}"

        logger.info(f"Built extraction query: {query}")
        return query

    def _get_staging_path(self, table_config: TableConfig) -> str:
        """
        Generate staging path for extracted data.

        Args:
            table_config: Table configuration

        Returns:
            HDFS path for staging the extracted data
        """
        base_path = self.config.source_cluster.staging_path
        job_id = self.config.job_id
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        table_name = f"{table_config.source_database}_{table_config.source_table}"

        return f"{base_path}/{job_id}/{table_name}/{timestamp}"

    def _add_metadata_columns(self, df: DataFrame) -> DataFrame:
        """
        Add extraction metadata columns to the DataFrame.

        Args:
            df: Source DataFrame

        Returns:
            DataFrame with metadata columns added
        """
        return df.withColumn(
            "_extraction_timestamp", current_timestamp()
        ).withColumn(
            "_source_job_id", lit(self.config.job_id)
        )

    def extract_table(self, table_config: TableConfig) -> Tuple[str, Dict[str, Any]]:
        """
        Extract a single table with filtering and column selection.

        Args:
            table_config: Configuration for the table to extract

        Returns:
            Tuple of (staging_path, metadata_dict)
        """
        table_name = f"{table_config.source_database}.{table_config.source_table}"
        logger.info(f"Starting extraction for table: {table_name}")

        start_time = time.time()
        filter_conditions = table_config.get_where_clause() or "None"

        self.audit.log_extraction_start(table_name, filter_conditions)

        try:
            # Build and execute query
            query = self._build_query(table_config)
            df = self.spark.sql(query)

            # Optionally coalesce partitions for transfer efficiency
            if table_config.coalesce_partitions:
                df = df.coalesce(table_config.coalesce_partitions)

            # Get row count before writing (cache if needed)
            row_count = df.count()
            logger.info(f"Extracted {row_count} rows from {table_name}")

            # Compute checksum for audit
            checksum = compute_dataframe_checksum(df)

            # Add metadata columns
            df = self._add_metadata_columns(df)

            # Get staging path
            staging_path = self._get_staging_path(table_config)

            # Write to staging location
            writer = df.write.mode("overwrite")

            # Set output format and compression
            if table_config.output_format == "parquet":
                writer = writer.option("compression", table_config.compression)
                writer.parquet(staging_path)
            elif table_config.output_format == "orc":
                writer = writer.option("compression", table_config.compression)
                writer.orc(staging_path)
            else:
                raise ValueError(f"Unsupported output format: {table_config.output_format}")

            # Get size of written data
            hadoop_conf = self.spark._jsc.hadoopConfiguration()
            fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                self.spark._jvm.java.net.URI.create(staging_path),
                hadoop_conf
            )
            path = self.spark._jvm.org.apache.hadoop.fs.Path(staging_path)
            bytes_written = fs.getContentSummary(path).getLength()

            duration = time.time() - start_time

            # Log audit event
            self.audit.log_extraction_complete(
                table_name=table_name,
                row_count=row_count,
                bytes_written=bytes_written,
                checksum=checksum,
                duration_seconds=duration
            )

            # Store metadata for later use
            metadata = {
                "source_table": table_name,
                "target_database": table_config.target_database,
                "target_table": table_config.target_table,
                "staging_path": staging_path,
                "row_count": row_count,
                "bytes_written": bytes_written,
                "checksum": checksum,
                "output_format": table_config.output_format,
                "partition_columns": table_config.partition_columns,
                "extraction_timestamp": datetime.now().isoformat(),
                "duration_seconds": duration
            }

            self.extraction_metadata[table_name] = metadata
            logger.info(f"Extraction complete for {table_name}: {row_count} rows, {bytes_written} bytes")

            return staging_path, metadata

        except Exception as e:
            duration = time.time() - start_time
            error_msg = str(e)
            logger.error(f"Extraction failed for {table_name}: {error_msg}")
            self.audit.log_error("EXTRACTION_FAILED", table_name, error_msg)
            raise

    def extract_all_tables(self) -> Dict[str, Dict[str, Any]]:
        """
        Extract all configured tables.

        Returns:
            Dictionary mapping table names to their extraction metadata
        """
        logger.info(f"Starting extraction for {len(self.config.tables)} tables")

        for table_config in self.config.tables:
            self.extract_table(table_config)

        return self.extraction_metadata

    def write_manifest(self, output_path: str):
        """
        Write extraction manifest file for use by transfer and loading steps.

        Args:
            output_path: Path to write the manifest JSON
        """
        import json

        manifest = {
            "job_id": self.config.job_id,
            "job_name": self.config.job_name,
            "source_cluster": self.config.source_cluster.name,
            "target_cluster": self.config.target_cluster.name,
            "extraction_timestamp": datetime.now().isoformat(),
            "tables": self.extraction_metadata
        }

        # Write to HDFS
        manifest_json = json.dumps(manifest, indent=2)

        # Use Spark to write the manifest
        manifest_df = self.spark.createDataFrame(
            [(manifest_json,)],
            ["manifest"]
        )
        manifest_df.coalesce(1).write.mode("overwrite").text(output_path)

        logger.info(f"Extraction manifest written to {output_path}")

        return manifest


def create_spark_session(config: JobConfig, app_name: str = "HiveDataExtractor") -> SparkSession:
    """
    Create SparkSession configured for Hive access on source cluster.

    Args:
        config: Job configuration
        app_name: Spark application name

    Returns:
        Configured SparkSession
    """
    builder = SparkSession.builder \
        .appName(f"{app_name}_{config.job_id}") \
        .enableHiveSupport()

    # Apply custom Spark configurations
    for key, value in config.spark_config.items():
        builder = builder.config(key, value)

    # Configure Hive metastore
    if config.source_cluster.hive_metastore_uri:
        builder = builder.config(
            "hive.metastore.uris",
            config.source_cluster.hive_metastore_uri
        )

    # Enable Kerberos if configured
    if config.source_cluster.kerberos_principal:
        builder = builder.config(
            "spark.yarn.principal",
            config.source_cluster.kerberos_principal
        )
    if config.source_cluster.kerberos_keytab:
        builder = builder.config(
            "spark.yarn.keytab",
            config.source_cluster.kerberos_keytab
        )

    return builder.getOrCreate()


def main():
    """Main entry point for extraction job."""
    parser = argparse.ArgumentParser(description="Extract filtered data from Hive tables")
    parser.add_argument(
        "--config",
        required=True,
        help="Path to job configuration YAML file"
    )
    parser.add_argument(
        "--table",
        help="Specific table to extract (optional, extracts all if not specified)"
    )
    parser.add_argument(
        "--manifest-path",
        help="Path to write extraction manifest"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )

    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)

    # Setup logging
    setup_logging(
        log_level=args.log_level,
        log_path=config.audit.log_path,
        job_id=config.job_id
    )

    logger.info(f"Starting extraction job: {config.job_id}")

    # Initialize audit logger
    audit_logger = AuditLogger(
        job_id=config.job_id,
        source_cluster=config.source_cluster.name,
        target_cluster=config.target_cluster.name,
        audit_path=config.audit.log_path,
        enabled=config.audit.enabled
    )

    # Create Spark session
    spark = create_spark_session(config)

    try:
        # Create extractor
        extractor = HiveDataExtractor(spark, config, audit_logger)

        # Start audit trail
        audit_logger.log_job_start(len(config.tables))

        start_time = time.time()

        # Extract tables
        if args.table:
            # Extract specific table
            table_config = next(
                (t for t in config.tables
                 if f"{t.source_database}.{t.source_table}" == args.table),
                None
            )
            if not table_config:
                raise ValueError(f"Table not found in config: {args.table}")
            extractor.extract_table(table_config)
        else:
            # Extract all tables
            extractor.extract_all_tables()

        # Write manifest
        if args.manifest_path:
            extractor.write_manifest(args.manifest_path)
        else:
            default_manifest_path = f"{config.source_cluster.staging_path}/{config.job_id}/manifest"
            extractor.write_manifest(default_manifest_path)

        duration = time.time() - start_time
        total_rows = sum(m["row_count"] for m in extractor.extraction_metadata.values())
        total_bytes = sum(m["bytes_written"] for m in extractor.extraction_metadata.values())

        audit_logger.log_job_complete(duration, total_rows, total_bytes)
        logger.info(f"Extraction job completed: {total_rows} rows, {total_bytes} bytes in {duration:.2f}s")

    except Exception as e:
        duration = time.time() - start_time
        audit_logger.log_job_failed(str(e), duration)
        logger.error(f"Extraction job failed: {e}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
