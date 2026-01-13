"""
Secure Data Transfer Manager

Handles secure data movement between Cloudera clusters using native tools
such as DistCp for HDFS-to-HDFS transfers with Kerberos authentication.

Supports multiple transfer methods while ensuring data integrity and
security compliance.
"""

import os
import sys
import time
import json
import logging
import subprocess
import hashlib
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
from dataclasses import dataclass
from enum import Enum

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config import JobConfig, TransferConfig, ClusterConfig
from common.logging_utils import AuditLogger

logger = logging.getLogger("datasync.transfer")


class TransferMethod(Enum):
    """Supported transfer methods."""
    DISTCP = "distcp"
    WEBHDFS = "webhdfs"
    SCP = "scp"


class TransferStatus(Enum):
    """Transfer operation status."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


@dataclass
class TransferResult:
    """Result of a data transfer operation."""
    source_path: str
    target_path: str
    status: TransferStatus
    bytes_transferred: int = 0
    duration_seconds: float = 0.0
    checksum_verified: bool = False
    error_message: Optional[str] = None
    retry_count: int = 0


class DistCpTransfer:
    """
    DistCp-based transfer between Cloudera clusters.

    Uses Hadoop DistCp with Kerberos authentication for secure,
    parallel data transfer between separate HDFS clusters.
    """

    def __init__(
        self,
        source_cluster: ClusterConfig,
        target_cluster: ClusterConfig,
        transfer_config: TransferConfig
    ):
        self.source = source_cluster
        self.target = target_cluster
        self.config = transfer_config

    def _build_distcp_command(
        self,
        source_path: str,
        target_path: str
    ) -> List[str]:
        """
        Build DistCp command with all required options.

        Args:
            source_path: Source HDFS path
            target_path: Target HDFS path

        Returns:
            List of command arguments
        """
        cmd = ["hadoop", "distcp"]

        # Parallel copy with mappers
        cmd.extend(["-m", str(self.config.parallel_maps)])

        # Bandwidth limiting
        if self.config.bandwidth_limit_mb > 0:
            cmd.extend(["-bandwidth", str(self.config.bandwidth_limit_mb)])

        # Preserve metadata
        if self.config.preserve_permissions:
            cmd.append("-p")

        # Skip checksum verification if configured (faster but less safe)
        if self.config.skip_checksum:
            cmd.append("-skipcrccheck")

        # Update mode - only copy files that have changed
        cmd.append("-update")

        # Delete files in target that don't exist in source
        # cmd.append("-delete")  # Uncomment if sync behavior desired

        # Overwrite if files differ
        cmd.append("-overwrite")

        # Log path for debugging
        log_path = f"/tmp/distcp_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        cmd.extend(["-log", log_path])

        # Full source and target paths with HDFS URI
        full_source = f"{self.source.hdfs_namenode}{source_path}"
        full_target = f"{self.target.hdfs_namenode}{target_path}"

        cmd.extend([full_source, full_target])

        return cmd

    def _kinit(self, cluster: ClusterConfig) -> bool:
        """
        Initialize Kerberos authentication.

        Args:
            cluster: Cluster configuration with Kerberos details

        Returns:
            True if authentication successful
        """
        if not cluster.kerberos_principal or not cluster.kerberos_keytab:
            logger.info("Kerberos not configured, skipping kinit")
            return True

        logger.info(f"Authenticating with Kerberos principal: {cluster.kerberos_principal}")

        try:
            cmd = [
                "kinit",
                "-kt", cluster.kerberos_keytab,
                cluster.kerberos_principal
            ]
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60
            )

            if result.returncode != 0:
                logger.error(f"kinit failed: {result.stderr}")
                return False

            logger.info("Kerberos authentication successful")
            return True

        except subprocess.TimeoutExpired:
            logger.error("kinit timed out")
            return False
        except Exception as e:
            logger.error(f"kinit error: {e}")
            return False

    def transfer(
        self,
        source_path: str,
        target_path: str
    ) -> TransferResult:
        """
        Execute DistCp transfer with retries.

        Args:
            source_path: Source HDFS path (relative to namenode)
            target_path: Target HDFS path (relative to namenode)

        Returns:
            TransferResult with operation details
        """
        logger.info(f"Starting DistCp transfer: {source_path} -> {target_path}")

        start_time = time.time()
        result = TransferResult(
            source_path=source_path,
            target_path=target_path,
            status=TransferStatus.IN_PROGRESS
        )

        # Authenticate if Kerberos enabled
        if self.config.kerberos_enabled:
            if not self._kinit(self.source):
                result.status = TransferStatus.FAILED
                result.error_message = "Source cluster Kerberos authentication failed"
                return result

        cmd = self._build_distcp_command(source_path, target_path)
        logger.debug(f"DistCp command: {' '.join(cmd)}")

        retry_count = 0
        last_error = None

        while retry_count <= self.config.retry_count:
            try:
                if retry_count > 0:
                    logger.info(f"Retry attempt {retry_count}/{self.config.retry_count}")
                    result.status = TransferStatus.RETRYING
                    time.sleep(self.config.retry_delay_seconds)

                proc = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=3600 * 4  # 4 hour timeout for large transfers
                )

                if proc.returncode == 0:
                    result.status = TransferStatus.COMPLETED
                    result.duration_seconds = time.time() - start_time
                    result.retry_count = retry_count

                    # Get bytes transferred from output if available
                    bytes_transferred = self._parse_bytes_from_output(proc.stdout)
                    result.bytes_transferred = bytes_transferred

                    logger.info(
                        f"DistCp completed: {bytes_transferred} bytes in "
                        f"{result.duration_seconds:.2f}s"
                    )
                    return result
                else:
                    last_error = proc.stderr
                    logger.warning(f"DistCp attempt failed: {last_error}")
                    retry_count += 1

            except subprocess.TimeoutExpired:
                last_error = "Transfer timed out after 4 hours"
                logger.warning(last_error)
                retry_count += 1

            except Exception as e:
                last_error = str(e)
                logger.warning(f"Transfer error: {last_error}")
                retry_count += 1

        # All retries exhausted
        result.status = TransferStatus.FAILED
        result.error_message = f"Transfer failed after {retry_count} attempts: {last_error}"
        result.duration_seconds = time.time() - start_time
        result.retry_count = retry_count

        logger.error(result.error_message)
        return result

    def _parse_bytes_from_output(self, output: str) -> int:
        """
        Parse bytes transferred from DistCp output.

        Args:
            output: DistCp stdout

        Returns:
            Bytes transferred or 0 if not found
        """
        # DistCp output includes counters like:
        # Bytes Copied=123456789
        import re
        match = re.search(r'Bytes[^\d]*(\d+)', output)
        if match:
            return int(match.group(1))
        return 0


class WebHDFSTransfer:
    """
    WebHDFS-based transfer between clusters.

    Uses WebHDFS REST API for transferring data when DistCp is not
    available or for smaller datasets.
    """

    def __init__(
        self,
        source_cluster: ClusterConfig,
        target_cluster: ClusterConfig,
        transfer_config: TransferConfig
    ):
        self.source = source_cluster
        self.target = target_cluster
        self.config = transfer_config

    def _get_webhdfs_url(self, cluster: ClusterConfig, path: str, operation: str) -> str:
        """Build WebHDFS URL for operation."""
        # Extract host and port from hdfs URI
        # hdfs://namenode:8020 -> namenode:50070 (WebHDFS port)
        host = cluster.hdfs_namenode.replace("hdfs://", "").split(":")[0]
        webhdfs_port = "50070"  # Default WebHDFS port
        protocol = "https" if cluster.ssl_enabled else "http"

        return f"{protocol}://{host}:{webhdfs_port}/webhdfs/v1{path}?op={operation}"

    def transfer(
        self,
        source_path: str,
        target_path: str
    ) -> TransferResult:
        """
        Execute WebHDFS transfer.

        Note: WebHDFS transfer is implemented via curl for simplicity
        and compatibility with Kerberos authentication.

        Args:
            source_path: Source HDFS path
            target_path: Target HDFS path

        Returns:
            TransferResult with operation details
        """
        import requests
        from requests_kerberos import HTTPKerberosAuth

        logger.info(f"Starting WebHDFS transfer: {source_path} -> {target_path}")

        start_time = time.time()
        result = TransferResult(
            source_path=source_path,
            target_path=target_path,
            status=TransferStatus.IN_PROGRESS
        )

        try:
            # Use curl with Kerberos for the actual transfer
            # This is more reliable than Python requests in Hadoop environments

            # First, list files in source
            list_cmd = [
                "curl", "-s", "--negotiate", "-u", ":",
                self._get_webhdfs_url(self.source, source_path, "LISTSTATUS")
            ]

            list_result = subprocess.run(list_cmd, capture_output=True, text=True)
            if list_result.returncode != 0:
                raise Exception(f"Failed to list source: {list_result.stderr}")

            # For each file, download and upload
            # (This is simplified - production would use streaming)
            file_list = json.loads(list_result.stdout)

            total_bytes = 0
            for file_status in file_list.get("FileStatuses", {}).get("FileStatus", []):
                file_path = f"{source_path}/{file_status['pathSuffix']}"
                target_file = f"{target_path}/{file_status['pathSuffix']}"

                # Download
                download_cmd = [
                    "curl", "-s", "--negotiate", "-u", ":",
                    "-o", f"/tmp/transfer_{file_status['pathSuffix']}",
                    self._get_webhdfs_url(self.source, file_path, "OPEN")
                ]
                subprocess.run(download_cmd, check=True)

                # Upload
                upload_cmd = [
                    "curl", "-s", "--negotiate", "-u", ":",
                    "-X", "PUT",
                    "-T", f"/tmp/transfer_{file_status['pathSuffix']}",
                    self._get_webhdfs_url(self.target, target_file, "CREATE&overwrite=true")
                ]
                subprocess.run(upload_cmd, check=True)

                total_bytes += file_status.get("length", 0)

                # Clean up temp file
                os.remove(f"/tmp/transfer_{file_status['pathSuffix']}")

            result.status = TransferStatus.COMPLETED
            result.bytes_transferred = total_bytes
            result.duration_seconds = time.time() - start_time

            logger.info(f"WebHDFS transfer completed: {total_bytes} bytes")

        except Exception as e:
            result.status = TransferStatus.FAILED
            result.error_message = str(e)
            result.duration_seconds = time.time() - start_time
            logger.error(f"WebHDFS transfer failed: {e}")

        return result


class TransferManager:
    """
    Orchestrates data transfer between Cloudera clusters.

    Selects appropriate transfer method and manages the complete
    transfer workflow including verification and retry logic.
    """

    def __init__(
        self,
        config: JobConfig,
        audit_logger: AuditLogger
    ):
        self.config = config
        self.audit = audit_logger
        self.transfer_results: Dict[str, TransferResult] = {}

        # Initialize transfer handler based on method
        if config.transfer.method == "distcp":
            self.handler = DistCpTransfer(
                config.source_cluster,
                config.target_cluster,
                config.transfer
            )
        elif config.transfer.method == "webhdfs":
            self.handler = WebHDFSTransfer(
                config.source_cluster,
                config.target_cluster,
                config.transfer
            )
        else:
            raise ValueError(f"Unsupported transfer method: {config.transfer.method}")

    def transfer_path(
        self,
        source_path: str,
        target_path: str,
        table_name: Optional[str] = None
    ) -> TransferResult:
        """
        Transfer data from source to target path.

        Args:
            source_path: Source HDFS path
            target_path: Target HDFS path
            table_name: Optional table name for logging

        Returns:
            TransferResult with operation details
        """
        table_id = table_name or source_path

        self.audit.log_transfer_start(table_id, source_path, target_path)

        result = self.handler.transfer(source_path, target_path)

        if result.status == TransferStatus.COMPLETED:
            self.audit.log_transfer_complete(
                table_id,
                result.bytes_transferred,
                result.duration_seconds
            )
        else:
            self.audit.log_error(
                "TRANSFER_FAILED",
                table_id,
                result.error_message or "Unknown error"
            )

        self.transfer_results[table_id] = result
        return result

    def transfer_from_manifest(self, manifest: Dict[str, Any]) -> Dict[str, TransferResult]:
        """
        Transfer all tables specified in extraction manifest.

        Args:
            manifest: Extraction manifest with table metadata

        Returns:
            Dictionary mapping table names to transfer results
        """
        logger.info(f"Starting transfer for {len(manifest['tables'])} tables")

        for source_table, metadata in manifest["tables"].items():
            source_path = metadata["staging_path"]

            # Build target path by replacing source staging with target staging
            target_path = source_path.replace(
                self.config.source_cluster.staging_path,
                self.config.target_cluster.staging_path
            )

            self.transfer_path(source_path, target_path, source_table)

        return self.transfer_results

    def verify_transfer(self, source_path: str, target_path: str) -> bool:
        """
        Verify transfer by comparing checksums.

        Args:
            source_path: Source HDFS path
            target_path: Target HDFS path

        Returns:
            True if verification passes
        """
        logger.info(f"Verifying transfer: {source_path} -> {target_path}")

        try:
            # Get source checksum
            source_cmd = [
                "hdfs", "dfs", "-checksum",
                f"{self.config.source_cluster.hdfs_namenode}{source_path}"
            ]
            source_result = subprocess.run(source_cmd, capture_output=True, text=True)

            # Get target checksum
            target_cmd = [
                "hdfs", "dfs", "-checksum",
                f"{self.config.target_cluster.hdfs_namenode}{target_path}"
            ]
            target_result = subprocess.run(target_cmd, capture_output=True, text=True)

            # Compare (simple string comparison of checksum output)
            source_checksum = source_result.stdout.strip().split()[-1]
            target_checksum = target_result.stdout.strip().split()[-1]

            if source_checksum == target_checksum:
                logger.info("Transfer verification passed")
                return True
            else:
                logger.warning(
                    f"Checksum mismatch: source={source_checksum}, target={target_checksum}"
                )
                return False

        except Exception as e:
            logger.error(f"Verification failed: {e}")
            return False

    def get_transfer_summary(self) -> Dict[str, Any]:
        """Get summary of all transfer operations."""
        completed = sum(1 for r in self.transfer_results.values()
                       if r.status == TransferStatus.COMPLETED)
        failed = sum(1 for r in self.transfer_results.values()
                    if r.status == TransferStatus.FAILED)
        total_bytes = sum(r.bytes_transferred for r in self.transfer_results.values())

        return {
            "total_transfers": len(self.transfer_results),
            "completed": completed,
            "failed": failed,
            "total_bytes_transferred": total_bytes,
            "results": {
                name: {
                    "status": result.status.value,
                    "bytes": result.bytes_transferred,
                    "duration": result.duration_seconds,
                    "error": result.error_message
                }
                for name, result in self.transfer_results.items()
            }
        }
