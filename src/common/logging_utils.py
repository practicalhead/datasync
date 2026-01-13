"""
Logging and audit utilities for data transfer operations.

Provides structured logging with audit trail capabilities for
compliance and monitoring in regulated environments.
"""

import os
import json
import logging
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from pathlib import Path
from dataclasses import dataclass, asdict


def setup_logging(
    log_level: str = "INFO",
    log_path: Optional[str] = None,
    job_id: Optional[str] = None
) -> logging.Logger:
    """
    Configure logging for the data transfer job.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_path: Directory for log files
        job_id: Job identifier for log file naming

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger("datasync")
    logger.setLevel(getattr(logging, log_level.upper()))

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    # File handler if path provided
    if log_path:
        log_dir = Path(log_path)
        log_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"datasync_{job_id}_{timestamp}.log" if job_id else f"datasync_{timestamp}.log"
        file_handler = logging.FileHandler(log_dir / log_filename)
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
        )
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)

    return logger


@dataclass
class AuditEvent:
    """Represents a single auditable event in the data transfer process."""
    timestamp: str
    job_id: str
    event_type: str
    status: str
    source_cluster: str
    target_cluster: str
    table_name: Optional[str] = None
    row_count: Optional[int] = None
    bytes_transferred: Optional[int] = None
    checksum: Optional[str] = None
    duration_seconds: Optional[float] = None
    user: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, excluding None values."""
        return {k: v for k, v in asdict(self).items() if v is not None}

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), indent=2)


class AuditLogger:
    """
    Audit logger for compliance and monitoring.

    Writes structured audit events to a separate audit log file
    in JSON format for easy parsing and analysis.
    """

    def __init__(
        self,
        job_id: str,
        source_cluster: str,
        target_cluster: str,
        audit_path: str = "/var/log/datasync/audit",
        enabled: bool = True
    ):
        self.job_id = job_id
        self.source_cluster = source_cluster
        self.target_cluster = target_cluster
        self.enabled = enabled
        self.events: list = []

        if enabled:
            audit_dir = Path(audit_path)
            audit_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.audit_file = audit_dir / f"audit_{job_id}_{timestamp}.json"
        else:
            self.audit_file = None

        self.logger = logging.getLogger("datasync.audit")

    def _get_current_user(self) -> str:
        """Get the current user running the job."""
        return os.environ.get("USER", os.environ.get("HADOOP_USER_NAME", "unknown"))

    def _create_event(
        self,
        event_type: str,
        status: str,
        **kwargs
    ) -> AuditEvent:
        """Create an audit event with common fields populated."""
        return AuditEvent(
            timestamp=datetime.now(timezone.utc).isoformat(),
            job_id=self.job_id,
            event_type=event_type,
            status=status,
            source_cluster=self.source_cluster,
            target_cluster=self.target_cluster,
            user=self._get_current_user(),
            **kwargs
        )

    def _write_event(self, event: AuditEvent):
        """Write event to audit log file."""
        self.events.append(event)
        self.logger.info(f"Audit: {event.event_type} - {event.status}")

        if self.audit_file:
            with open(self.audit_file, 'w') as f:
                json.dump([e.to_dict() for e in self.events], f, indent=2)

    def log_job_start(self, table_count: int):
        """Log job start event."""
        event = self._create_event(
            event_type="JOB_START",
            status="STARTED",
            details={"table_count": table_count}
        )
        self._write_event(event)

    def log_job_complete(self, duration_seconds: float, total_rows: int, total_bytes: int):
        """Log job completion event."""
        event = self._create_event(
            event_type="JOB_COMPLETE",
            status="SUCCESS",
            duration_seconds=duration_seconds,
            row_count=total_rows,
            bytes_transferred=total_bytes
        )
        self._write_event(event)

    def log_job_failed(self, error_message: str, duration_seconds: float):
        """Log job failure event."""
        event = self._create_event(
            event_type="JOB_FAILED",
            status="FAILED",
            duration_seconds=duration_seconds,
            error_message=error_message
        )
        self._write_event(event)

    def log_extraction_start(self, table_name: str, filter_conditions: str):
        """Log table extraction start."""
        event = self._create_event(
            event_type="EXTRACTION_START",
            status="STARTED",
            table_name=table_name,
            details={"filter_conditions": filter_conditions}
        )
        self._write_event(event)

    def log_extraction_complete(
        self,
        table_name: str,
        row_count: int,
        bytes_written: int,
        checksum: str,
        duration_seconds: float
    ):
        """Log table extraction completion."""
        event = self._create_event(
            event_type="EXTRACTION_COMPLETE",
            status="SUCCESS",
            table_name=table_name,
            row_count=row_count,
            bytes_transferred=bytes_written,
            checksum=checksum,
            duration_seconds=duration_seconds
        )
        self._write_event(event)

    def log_transfer_start(self, table_name: str, source_path: str, target_path: str):
        """Log data transfer start."""
        event = self._create_event(
            event_type="TRANSFER_START",
            status="STARTED",
            table_name=table_name,
            details={"source_path": source_path, "target_path": target_path}
        )
        self._write_event(event)

    def log_transfer_complete(
        self,
        table_name: str,
        bytes_transferred: int,
        duration_seconds: float
    ):
        """Log data transfer completion."""
        event = self._create_event(
            event_type="TRANSFER_COMPLETE",
            status="SUCCESS",
            table_name=table_name,
            bytes_transferred=bytes_transferred,
            duration_seconds=duration_seconds
        )
        self._write_event(event)

    def log_loading_start(self, table_name: str, target_table: str):
        """Log target table loading start."""
        event = self._create_event(
            event_type="LOADING_START",
            status="STARTED",
            table_name=table_name,
            details={"target_table": target_table}
        )
        self._write_event(event)

    def log_loading_complete(
        self,
        table_name: str,
        row_count: int,
        duration_seconds: float
    ):
        """Log target table loading completion."""
        event = self._create_event(
            event_type="LOADING_COMPLETE",
            status="SUCCESS",
            table_name=table_name,
            row_count=row_count,
            duration_seconds=duration_seconds
        )
        self._write_event(event)

    def log_error(self, event_type: str, table_name: str, error_message: str):
        """Log an error event."""
        event = self._create_event(
            event_type=event_type,
            status="FAILED",
            table_name=table_name,
            error_message=error_message
        )
        self._write_event(event)

    def get_audit_summary(self) -> Dict[str, Any]:
        """Get summary of all audit events."""
        return {
            "job_id": self.job_id,
            "total_events": len(self.events),
            "successful_events": sum(1 for e in self.events if e.status == "SUCCESS"),
            "failed_events": sum(1 for e in self.events if e.status == "FAILED"),
            "events": [e.to_dict() for e in self.events]
        }


def compute_checksum(file_path: str, algorithm: str = "md5") -> str:
    """
    Compute checksum of a file for data integrity verification.

    Args:
        file_path: Path to the file
        algorithm: Hash algorithm (md5, sha256)

    Returns:
        Hex digest of the file contents
    """
    hash_func = hashlib.new(algorithm)
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_func.update(chunk)
    return hash_func.hexdigest()


def compute_dataframe_checksum(spark_df, sample_size: int = 10000) -> str:
    """
    Compute a checksum for a Spark DataFrame.

    Uses sampling for large datasets to balance accuracy with performance.

    Args:
        spark_df: Spark DataFrame to checksum
        sample_size: Number of rows to sample

    Returns:
        MD5 hash of sampled data
    """
    # Sample and collect data for checksum
    sample_data = spark_df.limit(sample_size).collect()
    data_str = str(sorted([str(row.asDict()) for row in sample_data]))
    return hashlib.md5(data_str.encode()).hexdigest()
