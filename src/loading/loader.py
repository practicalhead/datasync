"""
Target Cluster Data Loader

PySpark job for loading transferred data into Hive tables on the target
Cloudera cluster. Reads intermediate Parquet files and creates/updates
Hive tables with proper metadata.

This job runs on the TARGET cluster where the SparkContext is bound to
the target Hive metastore.
"""

import os
import sys
import time
import json
import logging
import argparse
from datetime import datetime
from typing import Dict, Any, Optional, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config import load_config, TableConfig, JobConfig
from common.logging_utils import (
    setup_logging,
    AuditLogger,
    compute_dataframe_checksum
)

logger = logging.getLogger("datasync.loading")


class HiveDataLoader:
    """
    Loads transferred data into Hive tables on target cluster.

    Designed to run on the target Cloudera cluster with SparkContext bound
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
        self.loading_metadata: Dict[str, Dict[str, Any]] = {}

    def read_manifest(self, manifest_path: str) -> Dict[str, Any]:
        """
        Read the extraction manifest from the transfer staging area.

        Args:
            manifest_path: HDFS path to the manifest directory

        Returns:
            Manifest dictionary with extraction metadata
        """
        logger.info(f"Reading manifest from {manifest_path}")

        # Read manifest file (text format, single partition)
        manifest_df = self.spark.read.text(manifest_path)
        manifest_lines = manifest_df.collect()
        manifest_json = "".join([row.value for row in manifest_lines])

        return json.loads(manifest_json)

    def _ensure_database_exists(self, database: str):
        """
        Ensure the target database exists, create if necessary.

        Args:
            database: Database name to check/create
        """
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS `{database}`")
        logger.info(f"Ensured database exists: {database}")

    def _get_table_location(self, database: str, table: str) -> str:
        """
        Get the HDFS location for a Hive table.

        Args:
            database: Target database name
            table: Target table name

        Returns:
            HDFS path for the table data
        """
        warehouse_path = self.config.target_cluster.staging_path.replace("/staging", "/warehouse")
        return f"{warehouse_path}/{database}.db/{table}"

    def _drop_metadata_columns(self, df: DataFrame) -> DataFrame:
        """
        Remove extraction metadata columns if they exist.

        Args:
            df: DataFrame with potential metadata columns

        Returns:
            DataFrame without metadata columns
        """
        metadata_cols = ["_extraction_timestamp", "_source_job_id"]
        cols_to_drop = [c for c in metadata_cols if c in df.columns]
        if cols_to_drop:
            df = df.drop(*cols_to_drop)
        return df

    def load_table(
        self,
        staging_path: str,
        target_database: str,
        target_table: str,
        partition_columns: List[str],
        output_format: str = "parquet",
        mode: str = "overwrite"
    ) -> Dict[str, Any]:
        """
        Load a single table from staging into Hive.

        Args:
            staging_path: HDFS path to staged data
            target_database: Target Hive database
            target_table: Target Hive table name
            partition_columns: Columns to partition by
            output_format: Storage format (parquet, orc)
            mode: Write mode (overwrite, append)

        Returns:
            Metadata about the loaded table
        """
        full_table_name = f"{target_database}.{target_table}"
        logger.info(f"Loading table: {full_table_name} from {staging_path}")

        start_time = time.time()
        self.audit.log_loading_start(staging_path, full_table_name)

        try:
            # Ensure database exists
            self._ensure_database_exists(target_database)

            # Read staged data
            if output_format == "parquet":
                df = self.spark.read.parquet(staging_path)
            elif output_format == "orc":
                df = self.spark.read.orc(staging_path)
            else:
                raise ValueError(f"Unsupported format: {output_format}")

            # Remove metadata columns
            df = self._drop_metadata_columns(df)

            # Get row count
            row_count = df.count()

            # Verify checksum if available
            checksum = compute_dataframe_checksum(df)

            # Get table location
            table_location = self._get_table_location(target_database, target_table)

            # Write as Hive table
            writer = df.write.mode(mode).format(output_format)

            if partition_columns:
                writer = writer.partitionBy(*partition_columns)

            # Save as table
            writer.option("path", table_location).saveAsTable(full_table_name)

            # Refresh table metadata
            self.spark.sql(f"REFRESH TABLE `{target_database}`.`{target_table}`")

            # Verify the table was created
            verify_count = self.spark.sql(
                f"SELECT COUNT(*) as cnt FROM `{target_database}`.`{target_table}`"
            ).collect()[0].cnt

            if verify_count != row_count:
                logger.warning(
                    f"Row count mismatch: expected {row_count}, got {verify_count}"
                )

            duration = time.time() - start_time

            self.audit.log_loading_complete(
                table_name=full_table_name,
                row_count=row_count,
                duration_seconds=duration
            )

            metadata = {
                "target_table": full_table_name,
                "staging_path": staging_path,
                "table_location": table_location,
                "row_count": row_count,
                "verified_count": verify_count,
                "checksum": checksum,
                "partition_columns": partition_columns,
                "loading_timestamp": datetime.now().isoformat(),
                "duration_seconds": duration
            }

            self.loading_metadata[full_table_name] = metadata
            logger.info(f"Loading complete for {full_table_name}: {row_count} rows in {duration:.2f}s")

            return metadata

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Loading failed for {full_table_name}: {error_msg}")
            self.audit.log_error("LOADING_FAILED", full_table_name, error_msg)
            raise

    def load_from_manifest(self, manifest_path: str, mode: str = "overwrite") -> Dict[str, Dict[str, Any]]:
        """
        Load all tables specified in the extraction manifest.

        Args:
            manifest_path: Path to the extraction manifest
            mode: Write mode for all tables

        Returns:
            Dictionary mapping table names to loading metadata
        """
        manifest = self.read_manifest(manifest_path)

        logger.info(
            f"Loading {len(manifest['tables'])} tables from manifest"
        )

        for source_table, table_meta in manifest["tables"].items():
            # Adjust staging path to target cluster location
            source_staging = table_meta["staging_path"]
            # Replace source HDFS prefix with target prefix if needed
            target_staging = source_staging.replace(
                self.config.source_cluster.hdfs_namenode,
                self.config.target_cluster.hdfs_namenode
            ) if self.config.source_cluster.hdfs_namenode else source_staging

            self.load_table(
                staging_path=target_staging,
                target_database=table_meta["target_database"],
                target_table=table_meta["target_table"],
                partition_columns=table_meta.get("partition_columns", []),
                output_format=table_meta.get("output_format", "parquet"),
                mode=mode
            )

        return self.loading_metadata

    def load_all_tables(self, mode: str = "overwrite") -> Dict[str, Dict[str, Any]]:
        """
        Load all configured tables from their staging locations.

        Uses paths from configuration rather than manifest.

        Args:
            mode: Write mode for all tables

        Returns:
            Dictionary mapping table names to loading metadata
        """
        logger.info(f"Loading {len(self.config.tables)} tables from configuration")

        for table_config in self.config.tables:
            # Build staging path
            staging_path = (
                f"{self.config.target_cluster.staging_path}/"
                f"{self.config.job_id}/"
                f"{table_config.source_database}_{table_config.source_table}"
            )

            self.load_table(
                staging_path=staging_path,
                target_database=table_config.target_database,
                target_table=table_config.target_table,
                partition_columns=table_config.partition_columns,
                output_format=table_config.output_format,
                mode=mode
            )

        return self.loading_metadata

    def write_loading_report(self, output_path: str):
        """
        Write loading completion report.

        Args:
            output_path: Path to write the report
        """
        report = {
            "job_id": self.config.job_id,
            "job_name": self.config.job_name,
            "target_cluster": self.config.target_cluster.name,
            "loading_timestamp": datetime.now().isoformat(),
            "tables": self.loading_metadata,
            "summary": {
                "total_tables": len(self.loading_metadata),
                "total_rows": sum(m["row_count"] for m in self.loading_metadata.values())
            }
        }

        report_json = json.dumps(report, indent=2)
        report_df = self.spark.createDataFrame([(report_json,)], ["report"])
        report_df.coalesce(1).write.mode("overwrite").text(output_path)

        logger.info(f"Loading report written to {output_path}")

        return report


def create_spark_session(config: JobConfig, app_name: str = "HiveDataLoader") -> SparkSession:
    """
    Create SparkSession configured for Hive access on target cluster.

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

    # Configure Hive metastore for TARGET cluster
    if config.target_cluster.hive_metastore_uri:
        builder = builder.config(
            "hive.metastore.uris",
            config.target_cluster.hive_metastore_uri
        )

    # Enable Kerberos if configured
    if config.target_cluster.kerberos_principal:
        builder = builder.config(
            "spark.yarn.principal",
            config.target_cluster.kerberos_principal
        )
    if config.target_cluster.kerberos_keytab:
        builder = builder.config(
            "spark.yarn.keytab",
            config.target_cluster.kerberos_keytab
        )

    return builder.getOrCreate()


def main():
    """Main entry point for loading job."""
    parser = argparse.ArgumentParser(description="Load transferred data into Hive tables")
    parser.add_argument(
        "--config",
        required=True,
        help="Path to job configuration YAML file"
    )
    parser.add_argument(
        "--manifest-path",
        help="Path to extraction manifest (uses config staging paths if not specified)"
    )
    parser.add_argument(
        "--mode",
        default="overwrite",
        choices=["overwrite", "append"],
        help="Write mode for target tables"
    )
    parser.add_argument(
        "--report-path",
        help="Path to write loading report"
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

    logger.info(f"Starting loading job: {config.job_id}")

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
        # Create loader
        loader = HiveDataLoader(spark, config, audit_logger)

        # Start audit trail
        audit_logger.log_job_start(len(config.tables))

        start_time = time.time()

        # Load tables
        if args.manifest_path:
            loader.load_from_manifest(args.manifest_path, mode=args.mode)
        else:
            loader.load_all_tables(mode=args.mode)

        # Write report
        if args.report_path:
            loader.write_loading_report(args.report_path)
        else:
            default_report_path = f"{config.target_cluster.staging_path}/{config.job_id}/loading_report"
            loader.write_loading_report(default_report_path)

        duration = time.time() - start_time
        total_rows = sum(m["row_count"] for m in loader.loading_metadata.values())

        audit_logger.log_job_complete(duration, total_rows, 0)
        logger.info(f"Loading job completed: {total_rows} rows in {duration:.2f}s")

    except Exception as e:
        duration = time.time() - start_time
        audit_logger.log_job_failed(str(e), duration)
        logger.error(f"Loading job failed: {e}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
