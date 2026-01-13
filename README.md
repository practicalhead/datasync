# DataSync - Hive Data Transfer for Cloudera Clusters

A production-ready framework for transferring filtered Hive data between isolated Cloudera clusters with row-level filtering and column selection.

## Overview

DataSync enables secure, auditable data transfer between Cloudera clusters that do not share storage. It addresses the challenge of moving selected data between isolated Hive environments while:

- Applying row-level filtering (SQL WHERE conditions)
- Selecting specific columns for transfer
- Preserving Hive metadata on the target cluster
- Maintaining audit trails for compliance
- Supporting Kerberos authentication

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DataSync Workflow                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐  │
│  │  SOURCE CLUSTER  │    │     TRANSFER     │    │  TARGET CLUSTER  │  │
│  │    (Region A)    │    │                  │    │    (Region B)    │  │
│  │                  │    │                  │    │                  │  │
│  │  ┌────────────┐  │    │                  │    │  ┌────────────┐  │  │
│  │  │ Hive       │  │    │                  │    │  │ Hive       │  │  │
│  │  │ Metastore  │  │    │                  │    │  │ Metastore  │  │  │
│  │  └─────┬──────┘  │    │                  │    │  └─────┬──────┘  │  │
│  │        │         │    │                  │    │        │         │  │
│  │  ┌─────┴──────┐  │    │  ┌────────────┐  │    │  ┌─────┴──────┐  │  │
│  │  │ Spark      │──┼────┼──│  DistCp    │──┼────┼──│ Spark      │  │  │
│  │  │ Extraction │  │    │  │  Transfer  │  │    │  │ Loading    │  │  │
│  │  └─────┬──────┘  │    │  └────────────┘  │    │  └─────┬──────┘  │  │
│  │        │         │    │                  │    │        │         │  │
│  │  ┌─────┴──────┐  │    │                  │    │  ┌─────┴──────┐  │  │
│  │  │ HDFS       │  │    │                  │    │  │ HDFS       │  │  │
│  │  │ Staging    │  │    │                  │    │  │ Staging    │  │  │
│  │  └────────────┘  │    │                  │    │  └────────────┘  │  │
│  │                  │    │                  │    │                  │  │
│  └──────────────────┘    └──────────────────┘    └──────────────────┘  │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Three-Phase Process

1. **Extraction (Source Cluster)**
   - Spark job reads from source Hive tables
   - Applies row-level filtering (WHERE conditions)
   - Selects specified columns
   - Writes filtered data to HDFS staging in Parquet format

2. **Transfer (Cross-Cluster)**
   - DistCp copies data securely between HDFS clusters
   - Supports Kerberos authentication
   - Bandwidth throttling to prevent network saturation
   - Checksum verification for data integrity

3. **Loading (Target Cluster)**
   - Spark job reads from staging area
   - Creates/updates Hive tables with proper metadata
   - Verifies row counts match extraction

## Requirements

- Apache Spark 2.4+ (Cloudera distribution)
- Hadoop 3.x with DistCp
- Python 3.7+
- PyYAML (`pip install pyyaml`)
- Kerberos client (for secure clusters)

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd datasync

# Install Python dependencies
pip install pyyaml

# Verify Spark is available
spark-submit --version
```

## Quick Start

### 1. Create a Configuration File

Copy the template and customize:

```bash
cp config/templates/job_template.yaml config/my_job.yaml
```

Edit the configuration with your cluster details and table definitions.

### 2. Run the Complete Workflow

```bash
# Full workflow (extraction → transfer → loading)
./scripts/run_workflow.sh --config config/my_job.yaml
```

### 3. Or Run Individual Phases

```bash
# On SOURCE cluster: Extract data
./scripts/run_extraction.sh --config config/my_job.yaml

# Transfer between clusters
./scripts/run_transfer.sh --config config/my_job.yaml

# On TARGET cluster: Load data
./scripts/run_loading.sh --config config/my_job.yaml
```

## Configuration

### Job Configuration Structure

```yaml
job_id: "unique_job_identifier"
job_name: "Human readable name"

source_cluster:
  name: "source-cluster"
  hive_metastore_uri: "thrift://metastore:9083"
  hdfs_namenode: "hdfs://namenode:8020"
  kerberos_principal: "user@REALM.COM"
  kerberos_keytab: "/path/to/keytab"
  staging_path: "/data/staging"

target_cluster:
  name: "target-cluster"
  hive_metastore_uri: "thrift://metastore:9083"
  hdfs_namenode: "hdfs://namenode:8020"
  kerberos_principal: "user@REALM.COM"
  kerberos_keytab: "/path/to/keytab"
  staging_path: "/data/staging"

tables:
  - source_database: "db"
    source_table: "table"
    target_database: "db"
    target_table: "table"
    columns:
      - name: "col1"
      - name: "col2"
    filters:
      - condition: "region = 'US'"
      - condition: "date >= '2024-01-01'"
    output_format: "parquet"
    compression: "snappy"

transfer:
  method: "distcp"
  bandwidth_limit_mb: 100
  parallel_maps: 10

audit:
  enabled: true
  log_path: "/var/log/datasync"
```

### Column Selection

Specify columns to include in the transfer:

```yaml
columns:
  - name: "customer_id"
  - name: "email"
  - name: "full_name"
    alias: "name"  # Optional: rename column in target
```

Leave `columns: []` to transfer all columns.

### Row Filtering

Apply SQL WHERE conditions:

```yaml
filters:
  - condition: "region = 'EMEA'"
    description: "Only EMEA region data"
  - condition: "status = 'ACTIVE'"
    description: "Only active records"
  - condition: "created_at >= '2024-01-01'"
    description: "Data from 2024 onwards"
```

Multiple filters are combined with AND logic.

## Security

### Kerberos Authentication

For secure Cloudera clusters, configure Kerberos:

```yaml
source_cluster:
  kerberos_principal: "datasync@REALM.COM"
  kerberos_keytab: "/etc/security/keytabs/datasync.keytab"
```

### Data Protection

- Filter conditions can exclude sensitive columns (PII, etc.)
- Audit logs track all data movement
- Wire encryption via Hadoop's encryption settings
- No data written to local disk during transfer

## Audit Logging

All operations are logged for compliance:

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "job_id": "customer_sync_001",
  "event_type": "EXTRACTION_COMPLETE",
  "status": "SUCCESS",
  "table_name": "sales_db.customers",
  "row_count": 1500000,
  "bytes_transferred": 234567890,
  "checksum": "a1b2c3d4...",
  "user": "datasync"
}
```

Audit logs are written to the configured `audit.log_path`.

## Project Structure

```
datasync/
├── README.md
├── config/
│   ├── samples/
│   │   └── sample_transfer_job.yaml
│   └── templates/
│       └── job_template.yaml
├── scripts/
│   ├── run_extraction.sh
│   ├── run_transfer.sh
│   ├── run_loading.sh
│   └── run_workflow.sh
├── src/
│   ├── __init__.py
│   ├── common/
│   │   ├── __init__.py
│   │   ├── config.py
│   │   └── logging_utils.py
│   ├── extraction/
│   │   ├── __init__.py
│   │   └── extractor.py
│   ├── loading/
│   │   ├── __init__.py
│   │   └── loader.py
│   ├── transfer/
│   │   ├── __init__.py
│   │   └── transfer_manager.py
│   └── orchestration/
│       ├── __init__.py
│       └── workflow.py
└── tests/
```

## Advanced Usage

### Resume from Checkpoint

If a job fails, resume from the last successful phase:

```bash
./scripts/run_workflow.sh --config config/my_job.yaml --resume
```

### Run Specific Phase

```bash
# Start from transfer phase (extraction already done)
./scripts/run_workflow.sh --config config/my_job.yaml --start-from transfer

# Start from loading phase (transfer already done)
./scripts/run_workflow.sh --config config/my_job.yaml --start-from loading
```

### Custom Spark Configuration

Add Spark settings in the configuration:

```yaml
spark_config:
  spark.executor.memory: "16g"
  spark.executor.cores: "8"
  spark.dynamicAllocation.maxExecutors: "50"
```

## Troubleshooting

### Common Issues

**Kerberos Authentication Failed**
- Verify keytab file exists and is readable
- Check principal name matches keytab
- Ensure KDC is reachable

**DistCp Transfer Failed**
- Verify network connectivity between clusters
- Check HDFS permissions on staging directories
- Ensure sufficient disk space

**Hive Table Not Found**
- Verify database and table names
- Check Hive Metastore connectivity
- Ensure user has SELECT permissions

### Logs

Check logs in the configured audit path:
```bash
ls -la /var/log/datasync/
cat /var/log/datasync/datasync_<job_id>_*.log
```

## Contributing

1. Create a feature branch
2. Make changes with tests
3. Submit a pull request

## License

Copyright 2024. All rights reserved.
