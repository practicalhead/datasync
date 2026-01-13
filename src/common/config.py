"""
Configuration management for Hive data transfer jobs.

Handles loading, validation, and access to transfer job configurations
defined in YAML format.
"""

import os
import yaml
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class ColumnConfig:
    """Configuration for column selection."""
    name: str
    alias: Optional[str] = None

    def to_select_expr(self) -> str:
        """Generate SQL select expression for this column."""
        if self.alias:
            return f"`{self.name}` AS `{self.alias}`"
        return f"`{self.name}`"


@dataclass
class FilterConfig:
    """Configuration for row-level filtering."""
    condition: str
    description: Optional[str] = None

    def validate(self) -> bool:
        """Basic validation of filter condition."""
        if not self.condition or not self.condition.strip():
            raise ValueError("Filter condition cannot be empty")
        # Check for dangerous patterns
        dangerous_patterns = ['drop ', 'delete ', 'truncate ', 'insert ', 'update ']
        condition_lower = self.condition.lower()
        for pattern in dangerous_patterns:
            if pattern in condition_lower:
                raise ValueError(f"Filter condition contains forbidden pattern: {pattern}")
        return True


@dataclass
class TableConfig:
    """Configuration for a single table transfer."""
    source_database: str
    source_table: str
    target_database: str
    target_table: str
    columns: List[ColumnConfig]
    filters: List[FilterConfig] = field(default_factory=list)
    partition_columns: List[str] = field(default_factory=list)
    output_format: str = "parquet"
    compression: str = "snappy"
    coalesce_partitions: Optional[int] = None

    def get_select_columns(self) -> str:
        """Generate comma-separated column list for SELECT."""
        if not self.columns:
            return "*"
        return ", ".join(col.to_select_expr() for col in self.columns)

    def get_where_clause(self) -> str:
        """Generate WHERE clause from filters."""
        if not self.filters:
            return ""
        conditions = [f.condition for f in self.filters]
        return " AND ".join(f"({c})" for c in conditions)

    def get_full_source_name(self) -> str:
        """Get fully qualified source table name."""
        return f"`{self.source_database}`.`{self.source_table}`"

    def get_full_target_name(self) -> str:
        """Get fully qualified target table name."""
        return f"`{self.target_database}`.`{self.target_table}`"


@dataclass
class ClusterConfig:
    """Configuration for a Cloudera cluster."""
    name: str
    hive_metastore_uri: str
    hdfs_namenode: str
    kerberos_principal: Optional[str] = None
    kerberos_keytab: Optional[str] = None
    ssl_enabled: bool = True
    staging_path: str = "/tmp/datasync/staging"

    def get_hdfs_staging_path(self) -> str:
        """Get full HDFS path for staging data."""
        return f"{self.hdfs_namenode}{self.staging_path}"


@dataclass
class TransferConfig:
    """Configuration for secure data transfer between clusters."""
    method: str = "distcp"  # distcp, webhdfs, or scp
    bandwidth_limit_mb: int = 100
    parallel_maps: int = 10
    skip_checksum: bool = False
    preserve_permissions: bool = True
    encryption_enabled: bool = True
    kerberos_enabled: bool = True
    retry_count: int = 3
    retry_delay_seconds: int = 30


@dataclass
class AuditConfig:
    """Configuration for audit logging."""
    enabled: bool = True
    log_path: str = "/var/log/datasync"
    include_row_counts: bool = True
    include_checksums: bool = True
    notify_on_completion: bool = False
    notification_email: Optional[str] = None


@dataclass
class JobConfig:
    """Complete configuration for a data transfer job."""
    job_id: str
    job_name: str
    source_cluster: ClusterConfig
    target_cluster: ClusterConfig
    tables: List[TableConfig]
    transfer: TransferConfig
    audit: AuditConfig
    spark_config: Dict[str, str] = field(default_factory=dict)
    schedule: Optional[str] = None  # Cron expression

    def validate(self) -> bool:
        """Validate the complete job configuration."""
        if not self.job_id:
            raise ValueError("job_id is required")
        if not self.tables:
            raise ValueError("At least one table must be configured")

        # Validate all filters
        for table in self.tables:
            for f in table.filters:
                f.validate()

        return True


class ConfigLoader:
    """Loads and parses job configuration from YAML files."""

    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

    def load(self) -> JobConfig:
        """Load and parse configuration file."""
        logger.info(f"Loading configuration from {self.config_path}")

        with open(self.config_path, 'r') as f:
            raw_config = yaml.safe_load(f)

        return self._parse_config(raw_config)

    def _parse_config(self, raw: Dict[str, Any]) -> JobConfig:
        """Parse raw YAML dict into JobConfig dataclass."""
        # Parse source cluster
        source_raw = raw.get('source_cluster', {})
        source_cluster = ClusterConfig(
            name=source_raw.get('name', 'source'),
            hive_metastore_uri=source_raw.get('hive_metastore_uri', ''),
            hdfs_namenode=source_raw.get('hdfs_namenode', ''),
            kerberos_principal=source_raw.get('kerberos_principal'),
            kerberos_keytab=source_raw.get('kerberos_keytab'),
            ssl_enabled=source_raw.get('ssl_enabled', True),
            staging_path=source_raw.get('staging_path', '/tmp/datasync/staging')
        )

        # Parse target cluster
        target_raw = raw.get('target_cluster', {})
        target_cluster = ClusterConfig(
            name=target_raw.get('name', 'target'),
            hive_metastore_uri=target_raw.get('hive_metastore_uri', ''),
            hdfs_namenode=target_raw.get('hdfs_namenode', ''),
            kerberos_principal=target_raw.get('kerberos_principal'),
            kerberos_keytab=target_raw.get('kerberos_keytab'),
            ssl_enabled=target_raw.get('ssl_enabled', True),
            staging_path=target_raw.get('staging_path', '/tmp/datasync/staging')
        )

        # Parse tables
        tables = []
        for table_raw in raw.get('tables', []):
            columns = [
                ColumnConfig(
                    name=c.get('name') if isinstance(c, dict) else c,
                    alias=c.get('alias') if isinstance(c, dict) else None
                )
                for c in table_raw.get('columns', [])
            ]

            filters = [
                FilterConfig(
                    condition=f.get('condition') if isinstance(f, dict) else f,
                    description=f.get('description') if isinstance(f, dict) else None
                )
                for f in table_raw.get('filters', [])
            ]

            tables.append(TableConfig(
                source_database=table_raw.get('source_database', 'default'),
                source_table=table_raw.get('source_table'),
                target_database=table_raw.get('target_database', 'default'),
                target_table=table_raw.get('target_table'),
                columns=columns,
                filters=filters,
                partition_columns=table_raw.get('partition_columns', []),
                output_format=table_raw.get('output_format', 'parquet'),
                compression=table_raw.get('compression', 'snappy'),
                coalesce_partitions=table_raw.get('coalesce_partitions')
            ))

        # Parse transfer config
        transfer_raw = raw.get('transfer', {})
        transfer = TransferConfig(
            method=transfer_raw.get('method', 'distcp'),
            bandwidth_limit_mb=transfer_raw.get('bandwidth_limit_mb', 100),
            parallel_maps=transfer_raw.get('parallel_maps', 10),
            skip_checksum=transfer_raw.get('skip_checksum', False),
            preserve_permissions=transfer_raw.get('preserve_permissions', True),
            encryption_enabled=transfer_raw.get('encryption_enabled', True),
            kerberos_enabled=transfer_raw.get('kerberos_enabled', True),
            retry_count=transfer_raw.get('retry_count', 3),
            retry_delay_seconds=transfer_raw.get('retry_delay_seconds', 30)
        )

        # Parse audit config
        audit_raw = raw.get('audit', {})
        audit = AuditConfig(
            enabled=audit_raw.get('enabled', True),
            log_path=audit_raw.get('log_path', '/var/log/datasync'),
            include_row_counts=audit_raw.get('include_row_counts', True),
            include_checksums=audit_raw.get('include_checksums', True),
            notify_on_completion=audit_raw.get('notify_on_completion', False),
            notification_email=audit_raw.get('notification_email')
        )

        return JobConfig(
            job_id=raw.get('job_id', ''),
            job_name=raw.get('job_name', ''),
            source_cluster=source_cluster,
            target_cluster=target_cluster,
            tables=tables,
            transfer=transfer,
            audit=audit,
            spark_config=raw.get('spark_config', {}),
            schedule=raw.get('schedule')
        )


def load_config(config_path: str) -> JobConfig:
    """Convenience function to load configuration."""
    loader = ConfigLoader(config_path)
    config = loader.load()
    config.validate()
    return config
