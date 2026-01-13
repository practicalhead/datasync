# Hive Cross-Cluster Transfer - Architecture Documentation

## Table of Contents
1. [Overview](#overview)
2. [Execution Flow](#execution-flow)
3. [Key Objects and Classes](#key-objects-and-classes)
4. [Kerberos Authentication Flow](#kerberos-authentication-flow)
5. [Source Hive Connection with Kerberos](#source-hive-connection-with-kerberos)
6. [Method Reference](#method-reference)

---

## Overview

This application transfers Hive table data between two Cloudera clusters without requiring shared storage or distcp. It uses a JDBC-based approach where:

- **Spark runs on the TARGET cluster** (single SparkContext)
- **Reads from SOURCE cluster via JDBC** (connects to HiveServer2)
- **Writes to TARGET cluster natively** (Spark-Hive integration)

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
│              │ JDBC                                           │ Native      │
│              │                                                ▼             │
│              │                                     ┌───────────────────┐    │
│              │                                     │   Target HDFS     │    │
│              │                                     │   + Hive Metastore│    │
│              │                                     └───────────────────┘    │
└──────────────┼──────────────────────────────────────────────────────────────┘
               │ Network (Kerberos Secured)
┌──────────────┼──────────────────────────────────────────────────────────────┐
│              ▼                              SOURCE CLUSTER (Region A)        │
│  ┌───────────────────┐                                                       │
│  │   HiveServer2     │◄──── Source Hive Tables                              │
│  │   (JDBC Endpoint) │                                                       │
│  └───────────────────┘                                                       │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Execution Flow

### High-Level Flow

```
1. HiveCrossClusterTransfer.main(args)
       │
       ├──► TransferConfig.fromFile(configPath)      # Load HOCON config
       │
       ├──► KerberosAuth.authenticateWithKeytab()    # Get TGT from KDC
       │
       ├──► createSparkSession()                     # Spark with Hive support
       │
       └──► CrossClusterTransferExecutor.executeAll()
                   │
                   └──► For each table:
                            │
                            ├──► HiveJdbcReader.readTable()
                            │        └──► spark.read.jdbc(url, query)
                            │
                            └──► HiveTargetWriter.writeTable()
                                     └──► df.write.saveAsTable()
```

### Detailed Execution Sequence

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           EXECUTION SEQUENCE                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  PHASE 1: INITIALIZATION                                                     │
│  ════════════════════════                                                    │
│                                                                              │
│  HiveCrossClusterTransfer.main(args)                                        │
│      │                                                                       │
│      ├─► parseArgs(args)                                                    │
│      │       Returns: Map("config" -> "/path/to/config.conf")               │
│      │                                                                       │
│      ├─► TransferConfig.fromFile(configPath)                                │
│      │       │                                                               │
│      │       ├─► ConfigFactory.parseFile()     # Parse HOCON                │
│      │       ├─► parseClusterConfig(source)    # Source cluster settings    │
│      │       ├─► parseClusterConfig(target)    # Target cluster settings    │
│      │       └─► parseTableConfig()            # Per-table settings         │
│      │                                                                       │
│      │       Returns: TransferJobConfig                                      │
│      │                                                                       │
│  PHASE 2: KERBEROS AUTHENTICATION                                           │
│  ════════════════════════════════                                            │
│                                                                              │
│      ├─► authenticateKerberos(config)                                       │
│      │       │                                                               │
│      │       └─► KerberosAuth.authenticateWithKeytab(principal, keytab)     │
│      │               │                                                       │
│      │               ├─► Configuration.set("hadoop.security.authentication",│
│      │               │                      "kerberos")                      │
│      │               ├─► UserGroupInformation.setConfiguration(conf)        │
│      │               └─► UserGroupInformation.loginUserFromKeytab()         │
│      │                       │                                               │
│      │                       └─► Contacts KDC, gets TGT, stores in Subject  │
│      │                                                                       │
│  PHASE 3: SPARK SESSION CREATION                                            │
│  ═══════════════════════════════                                             │
│                                                                              │
│      ├─► createSparkSession(config)                                         │
│      │       │                                                               │
│      │       └─► SparkSession.builder()                                     │
│      │               .appName("HiveCrossClusterTransfer-{jobId}")           │
│      │               .enableHiveSupport()        # Native Hive access       │
│      │               .config("spark.sql.adaptive.enabled", "true")          │
│      │               .getOrCreate()                                          │
│      │                                                                       │
│  PHASE 4: TRANSFER EXECUTION                                                │
│  ═══════════════════════════                                                 │
│                                                                              │
│      └─► CrossClusterTransferExecutor(spark, config)                        │
│              │                                                               │
│              ├─► new HiveJdbcReader(spark, sourceCluster)                   │
│              ├─► new HiveTargetWriter(spark, targetCluster)                 │
│              └─► new AuditLogger(spark, jobId)  [if enabled]                │
│              │                                                               │
│              └─► executeAll()                                                │
│                      │                                                       │
│                      └─► For each TableTransferConfig:                      │
│                              │                                               │
│                              └─► executeTableTransfer(tableConfig)          │
│                                      │                                       │
│                                      │  [See Table Transfer Flow below]     │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Single Table Transfer Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         TABLE TRANSFER FLOW                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  executeTableTransfer(tableConfig)                                          │
│      │                                                                       │
│      ├─► readSourceData(tableConfig, metrics)                               │
│      │       │                                                               │
│      │       ├─► Check: parallelism > 1 AND hasNumericPartitionColumn?      │
│      │       │                                                               │
│      │       │   YES ──► Parallel Read Path:                                │
│      │       │           │                                                   │
│      │       │           ├─► detectPartitionColumn()                        │
│      │       │           │       Find: "id", "*_id", or "*key*" column      │
│      │       │           │                                                   │
│      │       │           ├─► getPartitionBounds()                           │
│      │       │           │       Query: SELECT MIN(col), MAX(col)           │
│      │       │           │       Returns: (lowerBound, upperBound)          │
│      │       │           │                                                   │
│      │       │           └─► sourceReader.readTablePartitioned()            │
│      │       │                   spark.read.jdbc(url, query,                │
│      │       │                       partitionColumn, min, max, numParts)   │
│      │       │                                                               │
│      │       │   NO ───► Simple Read Path:                                  │
│      │       │           │                                                   │
│      │       │           └─► sourceReader.readTable()                       │
│      │       │                   spark.read.jdbc(url, query, props)         │
│      │       │                                                               │
│      │       └─► Returns: DataFrame with filtered, selected columns         │
│      │                                                                       │
│      ├─► sourceData.cache()                                                 │
│      │       Cache DataFrame for count + write operations                   │
│      │                                                                       │
│      ├─► targetWriter.validateSchema(sourceData, tableConfig)               │
│      │       Check source columns exist in target (if table exists)         │
│      │                                                                       │
│      ├─► writeToTarget(sourceData, tableConfig, metrics)                    │
│      │       │                                                               │
│      │       └─► targetWriter.writeTable(df, tableConfig)                   │
│      │               │                                                       │
│      │               ├─► CREATE DATABASE IF NOT EXISTS target_db            │
│      │               │                                                       │
│      │               ├─► If partitionColumns.nonEmpty:                      │
│      │               │       writePartitionedTable()                        │
│      │               │           df.write.partitionBy(...).saveAsTable()    │
│      │               │           MSCK REPAIR TABLE                          │
│      │               │                                                       │
│      │               └─► Else:                                              │
│      │                       writeNonPartitionedTable()                     │
│      │                           df.write.saveAsTable()                     │
│      │                                                                       │
│      ├─► sourceData.unpersist()                                             │
│      │       Free cached memory                                             │
│      │                                                                       │
│      ├─► auditLogger.logTransfer(result)                                    │
│      │       Write to datasync_audit.transfer_audit_log                     │
│      │                                                                       │
│      └─► Return: TransferResult(tableName, success, rows, duration)         │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Key Objects and Classes

### 1. TransferConfig.scala - Configuration Objects

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CONFIGURATION HIERARCHY                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  TransferJobConfig                    # Root configuration object            │
│  ├── jobId: String                    # Unique job identifier               │
│  ├── sourceCluster: ClusterConfig     # Source cluster settings             │
│  ├── targetCluster: ClusterConfig     # Target cluster settings             │
│  ├── tables: Seq[TableTransferConfig] # List of tables to transfer          │
│  ├── batchSize: Int                   # JDBC batch size                     │
│  ├── parallelism: Int                 # Number of parallel readers          │
│  └── enableAudit: Boolean             # Enable audit logging                │
│                                                                              │
│  ClusterConfig                        # Per-cluster settings                 │
│  ├── name: String                     # Cluster identifier                  │
│  ├── hiveJdbcUrl: String              # jdbc:hive2://host:port/db           │
│  ├── hiveDatabase: String             # Database name                       │
│  ├── kerberosEnabled: Boolean         # Is Kerberos auth required?          │
│  ├── kerberosPrincipal: Option[String]# e.g., hive/host@REALM               │
│  └── kerberosKeytab: Option[String]   # Path to keytab file                 │
│                                                                              │
│  TableTransferConfig                  # Per-table settings                   │
│  ├── sourceTable: String              # Source table name                   │
│  ├── targetTable: String              # Target table name                   │
│  ├── columns: Seq[String]             # Columns to transfer                 │
│  ├── filterCondition: Option[String]  # WHERE clause                        │
│  ├── partitionColumns: Seq[String]    # Partition columns for target        │
│  └── writeMode: String                # overwrite/append/ignore/error       │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2. HiveJdbcReader.scala - Source Data Reader

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  CLASS: HiveJdbcReader                                                       │
│  FILE:  src/main/scala/com/datasync/transfer/HiveJdbcReader.scala           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  PURPOSE: Reads data from source Hive cluster via JDBC to HiveServer2       │
│                                                                              │
│  CONSTRUCTOR:                                                                │
│    HiveJdbcReader(spark: SparkSession, clusterConfig: ClusterConfig)        │
│                                                                              │
│  KEY FIELDS:                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │  effectiveJdbcUrl: String                                               ││
│  │      JDBC URL with Kerberos params appended if enabled                  ││
│  │      Example: jdbc:hive2://host:10000/db;principal=hive/host@REALM;     ││
│  │               auth=kerberos                                              ││
│  │                                                                          ││
│  │  connectionProperties: Properties                                        ││
│  │      JDBC driver properties including Kerberos settings                 ││
│  │      - driver: org.apache.hive.jdbc.HiveDriver                          ││
│  │      - AuthMech: 1 (Kerberos)                                           ││
│  │      - KrbRealm, KrbHostFQDN, KrbServiceName                            ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                                                              │
│  METHODS:                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │  readTable(tableConfig): DataFrame                                      ││
│  │      Basic JDBC read with column selection and filter pushdown          ││
│  │      Generates: (SELECT cols FROM table WHERE filter) AS source_data    ││
│  │                                                                          ││
│  │  readTablePartitioned(tableConfig, partCol, min, max, numParts): DF     ││
│  │      Parallel read using numeric column range partitioning              ││
│  │      Creates N parallel JDBC connections with range predicates          ││
│  │                                                                          ││
│  │  readTableWithPredicates(tableConfig, predicates: Array[String]): DF    ││
│  │      Parallel read using custom WHERE predicates per partition          ││
│  │                                                                          ││
│  │  getRowCount(tableConfig): Long                                         ││
│  │      Get filtered row count before transfer (for planning)              ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3. HiveTargetWriter.scala - Target Data Writer

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  CLASS: HiveTargetWriter                                                     │
│  FILE:  src/main/scala/com/datasync/transfer/HiveTargetWriter.scala         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  PURPOSE: Writes data to target Hive using native Spark-Hive integration    │
│                                                                              │
│  CONSTRUCTOR:                                                                │
│    HiveTargetWriter(spark: SparkSession, clusterConfig: ClusterConfig)      │
│                                                                              │
│  METHODS:                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │  writeTable(df, tableConfig, format="parquet"): Unit                    ││
│  │      Main write method - routes to partitioned or non-partitioned       ││
│  │      1. CREATE DATABASE IF NOT EXISTS                                   ││
│  │      2. df.write.mode(...).saveAsTable(...)                             ││
│  │      3. MSCK REPAIR TABLE (for partitioned)                             ││
│  │                                                                          ││
│  │  writePartitionedTable(df, tableName, partCols, mode, format): Unit     ││
│  │      df.write.partitionBy(...).saveAsTable(...)                         ││
│  │                                                                          ││
│  │  writeNonPartitionedTable(df, tableName, mode, format): Unit            ││
│  │      df.write.saveAsTable(...)                                          ││
│  │                                                                          ││
│  │  insertIntoTable(df, tableConfig, overwrite): Unit                      ││
│  │      Insert into existing table (schema must match)                     ││
│  │                                                                          ││
│  │  validateSchema(df, tableConfig): Boolean                               ││
│  │      Check source columns exist in target table                         ││
│  │                                                                          ││
│  │  tableExists(tableName): Boolean                                        ││
│  │      Check if table exists in target metastore                          ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4. CrossClusterTransferExecutor.scala - Orchestrator

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  CLASS: CrossClusterTransferExecutor                                         │
│  FILE:  src/main/scala/com/datasync/transfer/CrossClusterTransferExecutor.scala│
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  PURPOSE: Orchestrates the entire transfer process                          │
│                                                                              │
│  CONSTRUCTOR:                                                                │
│    CrossClusterTransferExecutor(spark: SparkSession, config: TransferJobConfig)│
│                                                                              │
│  COMPOSITION:                                                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │  sourceReader: HiveJdbcReader      # Reads from source via JDBC         ││
│  │  targetWriter: HiveTargetWriter    # Writes to target natively          ││
│  │  auditLogger: Option[AuditLogger]  # Audit trail (if enabled)           ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                                                              │
│  METHODS:                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │  executeAll(): Map[String, TransferResult]                              ││
│  │      Iterates over all tables, calls executeTableTransfer for each      ││
│  │                                                                          ││
│  │  executeTableTransfer(tableConfig): TransferResult                      ││
│  │      Core transfer logic: read → validate → write → audit               ││
│  │                                                                          ││
│  │  readSourceData(tableConfig, metrics): DataFrame                        ││
│  │      Decides between simple or partitioned JDBC read                    ││
│  │                                                                          ││
│  │  writeToTarget(df, tableConfig, metrics): Unit                          ││
│  │      Delegates to HiveTargetWriter                                      ││
│  │                                                                          ││
│  │  hasNumericPartitionColumn(tableConfig): Boolean                        ││
│  │      Checks if parallel reads are possible                              ││
│  │                                                                          ││
│  │  detectPartitionColumn(tableConfig): String                             ││
│  │      Finds best column for partitioning (id, *_id, *key*)               ││
│  │                                                                          ││
│  │  getPartitionBounds(tableConfig, partCol): (Long, Long)                 ││
│  │      Queries MIN/MAX via JDBC for range splitting                       ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 5. KerberosAuth.scala - Security Utilities

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  OBJECT: KerberosAuth                                                        │
│  FILE:   src/main/scala/com/datasync/utils/KerberosAuth.scala               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  PURPOSE: Handles Kerberos authentication for secure cluster access         │
│                                                                              │
│  METHODS:                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │  authenticateWithKeytab(principal, keytabPath): Unit                    ││
│  │      Logs into Kerberos using keytab file                               ││
│  │      Stores TGT in JVM's Subject credentials                            ││
│  │                                                                          ││
│  │  isKerberosEnabled: Boolean                                             ││
│  │      Check if Kerberos is enabled in Hadoop config                      ││
│  │                                                                          ││
│  │  getCurrentUser: String                                                 ││
│  │      Get current authenticated user from UGI                            ││
│  │                                                                          ││
│  │  checkAndRefreshCredentials(): Unit                                     ││
│  │      Refresh TGT if needed (for long-running jobs)                      ││
│  │                                                                          ││
│  │  configureCrossRealmTrust(sourceRealm, targetRealm): Unit               ││
│  │      Configure auth-to-local rules for cross-realm auth                 ││
│  │                                                                          ││
│  │  buildKerberosJdbcUrl(baseUrl, principal): String                       ││
│  │      Append Kerberos params to JDBC URL                                 ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Kerberos Authentication Flow

### Complete Authentication Sequence

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    KERBEROS AUTHENTICATION SEQUENCE                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────┐        ┌─────────────┐        ┌─────────────┐              │
│  │   KEYTAB    │        │     KDC     │        │ HIVESERVER2 │              │
│  │   FILE      │        │  (Kerberos) │        │   (Source)  │              │
│  └──────┬──────┘        └──────┬──────┘        └──────┬──────┘              │
│         │                      │                      │                      │
│  ═══════════════════════════════════════════════════════════════════════    │
│  STEP 1: Application Startup - Get TGT                                      │
│  ═══════════════════════════════════════════════════════════════════════    │
│         │                      │                      │                      │
│         │  KerberosAuth.authenticateWithKeytab()      │                      │
│         │                      │                      │                      │
│         │──── AS-REQ ─────────►│                      │                      │
│         │    (principal +      │                      │                      │
│         │     encrypted key    │                      │                      │
│         │     from keytab)     │                      │                      │
│         │                      │                      │                      │
│         │◄─── AS-REP ──────────│                      │                      │
│         │    (TGT - Ticket     │                      │                      │
│         │     Granting Ticket) │                      │                      │
│         │                      │                      │                      │
│         ▼                      │                      │                      │
│  ┌──────────────┐              │                      │                      │
│  │ JVM Subject  │              │                      │                      │
│  │ credentials  │              │                      │                      │
│  │ stores TGT   │              │                      │                      │
│  └──────┬───────┘              │                      │                      │
│         │                      │                      │                      │
│  ═══════════════════════════════════════════════════════════════════════    │
│  STEP 2: JDBC Connection - Get Service Ticket                               │
│  ═══════════════════════════════════════════════════════════════════════    │
│         │                      │                      │                      │
│         │  spark.read.jdbc() called                   │                      │
│         │  URL contains: ;principal=hive/host@REALM;auth=kerberos           │
│         │                      │                      │                      │
│         │  Hive JDBC Driver:   │                      │                      │
│         │  1. Sees auth=kerberos                      │                      │
│         │  2. Calls UGI.getCurrentUser()              │                      │
│         │  3. Gets TGT from Subject                   │                      │
│         │                      │                      │                      │
│         │──── TGS-REQ ────────►│                      │                      │
│         │   (TGT + request     │                      │                      │
│         │    service ticket    │                      │                      │
│         │    for hive/host)    │                      │                      │
│         │                      │                      │                      │
│         │◄─── TGS-REP ─────────│                      │                      │
│         │   (Service Ticket    │                      │                      │
│         │    for HiveServer2)  │                      │                      │
│         │                      │                      │                      │
│  ═══════════════════════════════════════════════════════════════════════    │
│  STEP 3: SASL/GSSAPI Authentication                                         │
│  ═══════════════════════════════════════════════════════════════════════    │
│         │                      │                      │                      │
│         │───────── SASL/GSSAPI Handshake ────────────►│                      │
│         │          (Service Ticket)                   │                      │
│         │                                             │                      │
│         │◄──────── Authentication Success ────────────│                      │
│         │          (Mutual auth complete)             │                      │
│         │                                             │                      │
│  ═══════════════════════════════════════════════════════════════════════    │
│  STEP 4: Execute SQL Query                                                   │
│  ═══════════════════════════════════════════════════════════════════════    │
│         │                                             │                      │
│         │───────── SQL Query ────────────────────────►│                      │
│         │  SELECT col1, col2 FROM table WHERE ...     │                      │
│         │                                             │                      │
│         │◄──────── Result Rows ───────────────────────│                      │
│         │                                             │                      │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Source Hive Connection with Kerberos

### Where is the Connection Created?

The source Hive connection is created in `HiveJdbcReader.scala` when `spark.read.jdbc()` is called.

### Step-by-Step Connection Process

#### Step 1: UGI Authentication (Application Startup)

**File:** `HiveCrossClusterTransfer.scala` (lines 61-63, 109-130)
**File:** `KerberosAuth.scala` (lines 25-35)

```scala
// HiveCrossClusterTransfer.scala:61-63
if (config.sourceCluster.kerberosEnabled || config.targetCluster.kerberosEnabled) {
  authenticateKerberos(config)
}

// This calls KerberosAuth.scala:25-35
def authenticateWithKeytab(principal: String, keytabPath: String): Unit = {
  val conf = new Configuration()
  conf.set("hadoop.security.authentication", "kerberos")

  UserGroupInformation.setConfiguration(conf)
  UserGroupInformation.loginUserFromKeytab(principal, keytabPath)
  // ↑ This contacts KDC, authenticates, and stores TGT in Subject
}
```

**What happens internally:**
1. Reads encrypted keys from keytab file
2. Sends AS-REQ to KDC (Key Distribution Center)
3. KDC validates credentials, returns TGT
4. TGT is stored in JVM's `Subject.getSubject().getPrivateCredentials()`
5. `UserGroupInformation` singleton now returns authenticated user

#### Step 2: Build Kerberos-Enabled JDBC URL

**File:** `HiveJdbcReader.scala` (lines 38-52)

```scala
private val effectiveJdbcUrl: String = {
  if (clusterConfig.kerberosEnabled) {
    val principal = clusterConfig.kerberosPrincipal.getOrElse(
      throw new IllegalArgumentException("Kerberos principal required"))

    // Append Kerberos params to JDBC URL
    if (clusterConfig.hiveJdbcUrl.contains("principal=")) {
      clusterConfig.hiveJdbcUrl
    } else {
      s"${clusterConfig.hiveJdbcUrl};principal=$principal;auth=kerberos"
    }
  } else {
    clusterConfig.hiveJdbcUrl
  }
}

// Example result:
// jdbc:hive2://source-hs2.region-a.com:10000/default;principal=hive/source-hs2.region-a.com@REGION-A.REALM;auth=kerberos
```

#### Step 3: Set Connection Properties

**File:** `HiveJdbcReader.scala` (lines 54-66)

```scala
private val connectionProperties: Properties = {
  val props = new Properties()
  props.setProperty("driver", "org.apache.hive.jdbc.HiveDriver")

  if (clusterConfig.kerberosEnabled) {
    props.setProperty("AuthMech", "1")  // 1 = Kerberos authentication
    props.setProperty("KrbRealm", extractKerberosRealm(principal))
    props.setProperty("KrbHostFQDN", extractKerberosHost(jdbcUrl))
    props.setProperty("KrbServiceName", "hive")
  }
  props
}
```

#### Step 4: Create JDBC Connection (via spark.read.jdbc)

**File:** `HiveJdbcReader.scala` (lines 67-69, 92-96)

```scala
def readTable(tableConfig: TableTransferConfig): DataFrame = {
  // Build query with filter pushdown
  val query = s"(SELECT $columnList FROM $fullTableName WHERE $filter) AS source_data"

  // This is where the connection is created
  spark.read.jdbc(effectiveJdbcUrl, query, connectionProperties)
  //           ↑                    ↑      ↑
  //           │                    │      └── Kerberos properties
  //           │                    └── SQL query with filter
  //           └── URL with ;principal=...;auth=kerberos
}
```

#### What Happens Inside spark.read.jdbc()

```
spark.read.jdbc(url, query, properties)
    │
    ├─► Spark creates JdbcRDD with the URL and properties
    │
    ├─► When DataFrame action is triggered (count, collect, write):
    │       │
    │       ├─► Each executor opens a JDBC connection
    │       │
    │       └─► Hive JDBC Driver.connect(url, properties):
    │               │
    │               ├─► Parse URL, extract principal and auth=kerberos
    │               │
    │               ├─► UserGroupInformation.getCurrentUser()
    │               │       Returns the user authenticated in Step 1
    │               │
    │               ├─► ugi.doAs(new PrivilegedAction<Connection>() {
    │               │       // Execute connection in authenticated context
    │               │
    │               │       ├─► Get TGT from Subject credentials
    │               │       │
    │               │       ├─► Request service ticket for hive/host@REALM
    │               │       │   (TGS-REQ to KDC)
    │               │       │
    │               │       ├─► Receive service ticket (TGS-REP from KDC)
    │               │       │
    │               │       └─► SASL/GSSAPI handshake with HiveServer2
    │               │           using the service ticket
    │               │   })
    │               │
    │               └─► Return authenticated JDBC Connection
    │
    └─► Execute SQL query over authenticated connection
```

### Code Location Summary

| Step | File | Lines | Method/Code |
|------|------|-------|-------------|
| 1. UGI Login | `KerberosAuth.scala` | 25-35 | `authenticateWithKeytab()` |
| 2. Build URL | `HiveJdbcReader.scala` | 38-52 | `effectiveJdbcUrl` |
| 3. Set Props | `HiveJdbcReader.scala` | 54-66 | `connectionProperties` |
| 4. Create Conn | `HiveJdbcReader.scala` | 67-69 | `spark.read.jdbc()` |
| 5. Trigger | `CrossClusterTransferExecutor.scala` | 138 | `df.count()` |

### Why No Explicit Auth in JDBC Call?

The JDBC driver automatically uses UGI because:

1. **UGI is a JVM singleton** - `loginUserFromKeytab()` sets the current user for the entire JVM
2. **Subject stores credentials** - TGT is in `Subject.getPrivateCredentials()`
3. **Driver checks UGI** - When `auth=kerberos` in URL, driver calls `UGI.getCurrentUser()`
4. **doAs() provides context** - Driver executes connection in authenticated context

---

## Method Reference

### Entry Point Methods

| Method | File | Purpose |
|--------|------|---------|
| `main(args)` | HiveCrossClusterTransfer.scala:43 | Application entry point |
| `parseArgs(args)` | HiveCrossClusterTransfer.scala:135 | Parse CLI arguments |
| `createSparkSession(config)` | HiveCrossClusterTransfer.scala:90 | Create Spark with Hive |
| `authenticateKerberos(config)` | HiveCrossClusterTransfer.scala:109 | Kerberos login |

### Configuration Methods

| Method | File | Purpose |
|--------|------|---------|
| `fromFile(path)` | TransferConfig.scala:59 | Load config from file |
| `fromConfig(config)` | TransferConfig.scala:43 | Parse Config object |
| `parseClusterConfig(config)` | TransferConfig.scala:64 | Extract cluster settings |
| `parseTableConfig(config)` | TransferConfig.scala:77 | Extract table settings |

### Transfer Execution Methods

| Method | File | Purpose |
|--------|------|---------|
| `executeAll()` | CrossClusterTransferExecutor.scala:41 | Run all table transfers |
| `executeTableTransfer(tableConfig)` | CrossClusterTransferExecutor.scala:60 | Transfer single table |
| `readSourceData(tableConfig, metrics)` | CrossClusterTransferExecutor.scala:119 | Read from source |
| `writeToTarget(df, tableConfig, metrics)` | CrossClusterTransferExecutor.scala:146 | Write to target |

### JDBC Reader Methods

| Method | File | Purpose |
|--------|------|---------|
| `readTable(tableConfig)` | HiveJdbcReader.scala:74 | Basic JDBC read |
| `readTablePartitioned(...)` | HiveJdbcReader.scala:99 | Parallel partitioned read |
| `readTableWithPredicates(...)` | HiveJdbcReader.scala:145 | Custom predicate read |
| `getRowCount(tableConfig)` | HiveJdbcReader.scala:155 | Get filtered count |

### Target Writer Methods

| Method | File | Purpose |
|--------|------|---------|
| `writeTable(df, tableConfig)` | HiveTargetWriter.scala:31 | Main write method |
| `writePartitionedTable(...)` | HiveTargetWriter.scala:58 | Write with partitions |
| `writeNonPartitionedTable(...)` | HiveTargetWriter.scala:81 | Simple write |
| `validateSchema(df, tableConfig)` | HiveTargetWriter.scala:159 | Check schema compat |
| `tableExists(tableName)` | HiveTargetWriter.scala:145 | Check table exists |

### Kerberos Methods

| Method | File | Purpose |
|--------|------|---------|
| `authenticateWithKeytab(principal, keytab)` | KerberosAuth.scala:25 | Login with keytab |
| `checkAndRefreshCredentials()` | KerberosAuth.scala:55 | Refresh TGT |
| `configureCrossRealmTrust(...)` | KerberosAuth.scala:69 | Cross-realm setup |
| `buildKerberosJdbcUrl(url, principal)` | KerberosAuth.scala:92 | Build Kerberos URL |

### Audit Methods

| Method | File | Purpose |
|--------|------|---------|
| `logTransfer(result)` | AuditLogger.scala:47 | Log transfer result |
| `getTableHistory(tableName)` | AuditLogger.scala:76 | Query table history |
| `getTransferStats(days)` | AuditLogger.scala:90 | Get transfer stats |
