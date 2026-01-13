"""
Tests for configuration management module.
"""

import os
import sys
import tempfile
import pytest

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from common.config import (
    ColumnConfig,
    FilterConfig,
    TableConfig,
    ClusterConfig,
    TransferConfig,
    AuditConfig,
    JobConfig,
    ConfigLoader,
    load_config
)


class TestColumnConfig:
    """Tests for ColumnConfig."""

    def test_simple_column(self):
        col = ColumnConfig(name="customer_id")
        assert col.to_select_expr() == "`customer_id`"

    def test_column_with_alias(self):
        col = ColumnConfig(name="full_name", alias="name")
        assert col.to_select_expr() == "`full_name` AS `name`"


class TestFilterConfig:
    """Tests for FilterConfig."""

    def test_valid_filter(self):
        f = FilterConfig(condition="status = 'ACTIVE'")
        assert f.validate() is True

    def test_empty_filter_raises(self):
        f = FilterConfig(condition="")
        with pytest.raises(ValueError):
            f.validate()

    def test_dangerous_pattern_raises(self):
        f = FilterConfig(condition="1=1; DROP TABLE users")
        with pytest.raises(ValueError):
            f.validate()


class TestTableConfig:
    """Tests for TableConfig."""

    def test_get_select_columns(self):
        table = TableConfig(
            source_database="db",
            source_table="tbl",
            target_database="db",
            target_table="tbl",
            columns=[
                ColumnConfig(name="col1"),
                ColumnConfig(name="col2", alias="column2")
            ]
        )
        result = table.get_select_columns()
        assert "`col1`" in result
        assert "`col2` AS `column2`" in result

    def test_get_where_clause(self):
        table = TableConfig(
            source_database="db",
            source_table="tbl",
            target_database="db",
            target_table="tbl",
            columns=[],
            filters=[
                FilterConfig(condition="status = 'ACTIVE'"),
                FilterConfig(condition="region = 'US'")
            ]
        )
        where = table.get_where_clause()
        assert "(status = 'ACTIVE')" in where
        assert "(region = 'US')" in where
        assert " AND " in where

    def test_empty_columns_returns_star(self):
        table = TableConfig(
            source_database="db",
            source_table="tbl",
            target_database="db",
            target_table="tbl",
            columns=[]
        )
        assert table.get_select_columns() == "*"


class TestConfigLoader:
    """Tests for ConfigLoader."""

    def test_load_valid_config(self):
        config_content = """
job_id: "test_job"
job_name: "Test Job"

source_cluster:
  name: "source"
  hive_metastore_uri: "thrift://localhost:9083"
  hdfs_namenode: "hdfs://localhost:8020"
  staging_path: "/tmp/staging"

target_cluster:
  name: "target"
  hive_metastore_uri: "thrift://localhost:9083"
  hdfs_namenode: "hdfs://localhost:8020"
  staging_path: "/tmp/staging"

tables:
  - source_database: "src_db"
    source_table: "src_table"
    target_database: "tgt_db"
    target_table: "tgt_table"
    columns:
      - name: "col1"
    filters:
      - condition: "active = true"

transfer:
  method: "distcp"

audit:
  enabled: true
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(config_content)
            f.flush()
            config_path = f.name

        try:
            config = load_config(config_path)
            assert config.job_id == "test_job"
            assert config.source_cluster.name == "source"
            assert len(config.tables) == 1
            assert config.tables[0].source_table == "src_table"
        finally:
            os.unlink(config_path)

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            ConfigLoader("/nonexistent/path.yaml")


class TestJobConfigValidation:
    """Tests for JobConfig validation."""

    def test_empty_job_id_raises(self):
        config = JobConfig(
            job_id="",
            job_name="Test",
            source_cluster=ClusterConfig(
                name="src",
                hive_metastore_uri="",
                hdfs_namenode=""
            ),
            target_cluster=ClusterConfig(
                name="tgt",
                hive_metastore_uri="",
                hdfs_namenode=""
            ),
            tables=[],
            transfer=TransferConfig(),
            audit=AuditConfig()
        )
        with pytest.raises(ValueError):
            config.validate()

    def test_empty_tables_raises(self):
        config = JobConfig(
            job_id="test",
            job_name="Test",
            source_cluster=ClusterConfig(
                name="src",
                hive_metastore_uri="",
                hdfs_namenode=""
            ),
            target_cluster=ClusterConfig(
                name="tgt",
                hive_metastore_uri="",
                hdfs_namenode=""
            ),
            tables=[],
            transfer=TransferConfig(),
            audit=AuditConfig()
        )
        with pytest.raises(ValueError):
            config.validate()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
