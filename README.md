# Hive Cross-Cluster Data Transfer

Transfer Hive table data between Cloudera clusters with row-level filtering and column selection, without requiring shared storage or distcp.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           TARGET CLUSTER (Region B)                          │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │                         SPARK APPLICATION                                ││
│  │  ┌─────────────────┐    ┌──────────────────┐    ┌───────────────────┐  ││
│  │  │  HiveJdbcReader │───►│ CrossCluster     │───►│ HiveTargetWriter  │  ││
│  │  │  (JDBC Source)  │    │ TransferExecutor │    │ (Native Hive)     │  ││
│  │  └────────┬────────┘    └──────────────────┘    └─────────┬─────────┘  ││
│  │           │                                                │            ││
│  └───────────┼────────────────────────────────────────────────┼────────────┘│
│              │                                                │             │
│              │ JDBC                                           │ Native      │
│              │                                                ▼             │
│              │                                     ┌───────────────────┐    │
│              │                                     │   Target HDFS     │    │
│              │                                     │   + Hive Metastore│    │
│              │                                     └───────────────────┘    │
└──────────────┼──────────────────────────────────────────────────────────────┘
               │
               │ Network (Secure)
               │
┌──────────────┼──────────────────────────────────────────────────────────────┐
│              ▼                              SOURCE CLUSTER (Region A)        │
│  ┌───────────────────┐                                                       │
│  │   HiveServer2     │◄──── Source Hive Tables                              │
│  │   (JDBC Endpoint) │                                                       │
│  └───────────────────┘                                                       │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Key Design Decisions

### Why JDBC Instead of distcp?

| Aspect | JDBC Approach | distcp Approach |
|--------|---------------|-----------------|
| Filter pushdown | Yes - filtering at source | No - transfer all, filter later |
| Column selection | Yes - only selected columns | No - all columns transferred |
| Shared storage | Not required | Required or staging area needed |
| Multiple SparkContexts | Single context on target | Complex multi-context setup |
| Data landing | Direct to target Hive | Files first, then Hive |

### How It Works

1. **Spark runs on TARGET cluster** - Single SparkContext bound to target
2. **Read from SOURCE via JDBC** - Connect to HiveServer2 on source cluster
3. **Filter and select at source** - SQL pushdown minimizes data transfer
4. **Write to TARGET natively** - Direct Spark-Hive integration

## Quick Start

### 1. Build the Project

```bash
cd /home/user/datasync
sbt assembly
```

### 2. Simple Single-Table Transfer

```bash
./scripts/simple-transfer.sh \
  --source-jdbc "jdbc:hive2://source-hiveserver2:10000/default" \
  --source-table "source_db.customer_data" \
  --target-table "target_db.customer_data" \
  --columns "customer_id,name,email,region" \
  --filter "region = 'REGION_B'"
```

### 3. Multi-Table Transfer with Configuration

```bash
# Edit configuration
vim conf/transfer-config.conf

# Submit job
./scripts/submit-transfer.sh --config conf/transfer-config.conf
```

## Configuration

### transfer-config.conf

```hocon
job {
  id = "transfer-job-001"
}

source {
  hive {
    jdbcUrl = "jdbc:hive2://source-hs2.region-a.example.com:10000/default"
    database = "source_db"
  }
  kerberos {
    enabled = true
    principal = "hive/_HOST@REGION-A.REALM"
    keytab = "/path/to/keytab"
  }
}

target {
  hive {
    database = "target_db"
    jdbcUrl = ""  # Not needed - native Hive
  }
  kerberos {
    enabled = true
    principal = "spark/_HOST@REGION-B.REALM"
    keytab = "/path/to/keytab"
  }
}

tables = [
  {
    source = "transactions"
    target = "transactions"
    columns = ["txn_id", "customer_id", "amount", "txn_date"]
    filter = "txn_date >= '2024-01-01' AND region = 'B'"
    partitionBy = ["txn_date"]
    writeMode = "overwrite"
  }
]

transfer {
  batchSize = 10000
  parallelism = 20
  enableAudit = true
}
```

## Features

### Row-Level Filtering
```hocon
filter = "region = 'REGION_B' AND status = 'ACTIVE' AND created_date >= '2024-01-01'"
```

### Column Selection
```hocon
# Specific columns
columns = ["id", "name", "email", "created_date"]

# All columns
columns = ["*"]
```

### Partitioned Tables
```hocon
partitionBy = ["year", "month", "day"]
```

### Write Modes
- `overwrite` - Replace existing data
- `append` - Add to existing data
- `ignore` - Skip if table exists
- `error` - Fail if table exists

## Kerberos Authentication

For secured clusters with cross-realm trust:

```bash
./scripts/submit-transfer-kerberos.sh \
  --config conf/transfer-config.conf \
  --principal spark/node@REGION-B.REALM \
  --keytab /etc/security/keytabs/spark.keytab \
  --source-hs2-principal hive/hs2-node@REGION-A.REALM
```

## Performance Tuning

### Parallel JDBC Reads

For large tables with numeric ID columns:

```hocon
transfer {
  parallelism = 20  # Number of parallel JDBC connections
}
```

### Resource Allocation

```bash
./scripts/submit-transfer.sh \
  --config conf/transfer-config.conf \
  --num-executors 20 \
  --executor-memory 16g \
  --executor-cores 4
```

## Audit Logging

When `enableAudit = true`, transfer events are logged to:
- Table: `datasync_audit.transfer_audit_log`

Query audit history:
```sql
SELECT * FROM datasync_audit.transfer_audit_log
WHERE table_name = 'transactions'
ORDER BY transfer_timestamp DESC;
```

## Project Structure

```
datasync/
├── build.sbt                    # SBT build configuration
├── conf/
│   └── transfer-config.conf     # Sample configuration
├── scripts/
│   ├── submit-transfer.sh       # Standard job submission
│   ├── submit-transfer-kerberos.sh  # Kerberos-enabled submission
│   └── simple-transfer.sh       # Quick single-table transfer
└── src/main/scala/com/datasync/
    ├── HiveCrossClusterTransfer.scala  # Main entry point
    ├── SimpleTransfer.scala            # Simple CLI transfer
    ├── config/
    │   └── TransferConfig.scala        # Configuration parsing
    ├── transfer/
    │   ├── HiveJdbcReader.scala        # JDBC source reader
    │   ├── HiveTargetWriter.scala      # Native Hive writer
    │   └── CrossClusterTransferExecutor.scala  # Orchestrator
    └── utils/
        ├── AuditLogger.scala           # Audit trail
        ├── KerberosAuth.scala          # Kerberos utilities
        └── TransferMetrics.scala       # Transfer metrics
```

## Requirements

- Cloudera CDP/CDH with Spark 3.x
- Network connectivity between clusters
- HiveServer2 accessible from target cluster
- For Kerberos: Cross-realm trust or shared realm

## Troubleshooting

### Connection Issues
```bash
# Test JDBC connectivity from target cluster
beeline -u "jdbc:hive2://source-hs2:10000/default"
```

### Kerberos Issues
```bash
# Verify ticket
klist

# Test cross-realm access
kinit -kt /path/to/keytab principal@REALM
```

### Performance Issues
- Increase `parallelism` for large tables
- Add partition column for parallel reads
- Tune executor memory for wide tables
