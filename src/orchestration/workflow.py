"""
Workflow Orchestrator for Hive Data Transfer

Coordinates the complete data transfer workflow across source and target
Cloudera clusters. Manages the three-phase process:
1. Extraction on source cluster
2. Secure transfer between clusters
3. Loading on target cluster

Designed for production use with proper error handling, checkpointing,
and recovery capabilities.
"""

import os
import sys
import time
import json
import logging
import argparse
import subprocess
from datetime import datetime
from typing import Dict, Any, Optional, List
from enum import Enum
from dataclasses import dataclass, field
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config import load_config, JobConfig
from common.logging_utils import setup_logging, AuditLogger

logger = logging.getLogger("datasync.orchestration")


class WorkflowPhase(Enum):
    """Workflow execution phases."""
    INITIALIZED = "initialized"
    EXTRACTING = "extracting"
    TRANSFERRING = "transferring"
    LOADING = "loading"
    COMPLETED = "completed"
    FAILED = "failed"


class ExecutionMode(Enum):
    """How to execute Spark jobs."""
    LOCAL = "local"  # Local Spark for testing
    YARN = "yarn"    # YARN cluster mode
    SPARK_SUBMIT = "spark-submit"  # Use spark-submit command


@dataclass
class WorkflowState:
    """Tracks workflow execution state for checkpointing and recovery."""
    job_id: str
    phase: WorkflowPhase
    start_time: str
    extraction_complete: bool = False
    extraction_manifest_path: Optional[str] = None
    transfer_complete: bool = False
    loading_complete: bool = False
    error_message: Optional[str] = None
    checkpoint_path: Optional[str] = None
    tables_processed: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize state to dictionary."""
        return {
            "job_id": self.job_id,
            "phase": self.phase.value,
            "start_time": self.start_time,
            "extraction_complete": self.extraction_complete,
            "extraction_manifest_path": self.extraction_manifest_path,
            "transfer_complete": self.transfer_complete,
            "loading_complete": self.loading_complete,
            "error_message": self.error_message,
            "tables_processed": self.tables_processed
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WorkflowState":
        """Deserialize state from dictionary."""
        return cls(
            job_id=data["job_id"],
            phase=WorkflowPhase(data["phase"]),
            start_time=data["start_time"],
            extraction_complete=data.get("extraction_complete", False),
            extraction_manifest_path=data.get("extraction_manifest_path"),
            transfer_complete=data.get("transfer_complete", False),
            loading_complete=data.get("loading_complete", False),
            error_message=data.get("error_message"),
            tables_processed=data.get("tables_processed", [])
        )


class WorkflowOrchestrator:
    """
    Orchestrates the complete data transfer workflow.

    Coordinates extraction, transfer, and loading phases while maintaining
    state for recovery and providing audit trail.
    """

    def __init__(
        self,
        config: JobConfig,
        execution_mode: ExecutionMode = ExecutionMode.SPARK_SUBMIT
    ):
        self.config = config
        self.execution_mode = execution_mode
        self.state = WorkflowState(
            job_id=config.job_id,
            phase=WorkflowPhase.INITIALIZED,
            start_time=datetime.now().isoformat()
        )

        # Initialize audit logger
        self.audit = AuditLogger(
            job_id=config.job_id,
            source_cluster=config.source_cluster.name,
            target_cluster=config.target_cluster.name,
            audit_path=config.audit.log_path,
            enabled=config.audit.enabled
        )

        # Set checkpoint path
        self.state.checkpoint_path = f"{config.source_cluster.staging_path}/{config.job_id}/checkpoint.json"

    def _save_checkpoint(self):
        """Save current workflow state to checkpoint file."""
        if self.state.checkpoint_path:
            checkpoint_data = json.dumps(self.state.to_dict(), indent=2)
            local_path = f"/tmp/checkpoint_{self.config.job_id}.json"
            with open(local_path, 'w') as f:
                f.write(checkpoint_data)
            logger.info(f"Checkpoint saved: phase={self.state.phase.value}")

    def _load_checkpoint(self) -> bool:
        """
        Load workflow state from checkpoint if exists.

        Returns:
            True if checkpoint was loaded
        """
        local_path = f"/tmp/checkpoint_{self.config.job_id}.json"
        if os.path.exists(local_path):
            with open(local_path, 'r') as f:
                data = json.load(f)
            self.state = WorkflowState.from_dict(data)
            logger.info(f"Checkpoint loaded: phase={self.state.phase.value}")
            return True
        return False

    def _build_spark_submit_cmd(
        self,
        script_path: str,
        cluster_type: str,
        extra_args: List[str] = None
    ) -> List[str]:
        """
        Build spark-submit command for running extraction or loading job.

        Args:
            script_path: Path to the PySpark script
            cluster_type: 'source' or 'target' cluster
            extra_args: Additional arguments to pass to the script

        Returns:
            Command as list of strings
        """
        cluster = (self.config.source_cluster if cluster_type == "source"
                  else self.config.target_cluster)

        cmd = [
            "spark-submit",
            "--master", "yarn",
            "--deploy-mode", "cluster",
            "--name", f"{self.config.job_id}_{cluster_type}",
        ]

        # Add Spark configurations
        for key, value in self.config.spark_config.items():
            cmd.extend(["--conf", f"{key}={value}"])

        # Kerberos configuration
        if cluster.kerberos_principal:
            cmd.extend(["--principal", cluster.kerberos_principal])
        if cluster.kerberos_keytab:
            cmd.extend(["--keytab", cluster.kerberos_keytab])

        # Hive configuration
        cmd.extend([
            "--conf", f"hive.metastore.uris={cluster.hive_metastore_uri}",
            "--conf", "spark.sql.hive.metastore.jars=builtin"
        ])

        # Add the script
        cmd.append(script_path)

        # Add script arguments
        if extra_args:
            cmd.extend(extra_args)

        return cmd

    def _run_spark_job(
        self,
        script_path: str,
        cluster_type: str,
        extra_args: List[str] = None,
        timeout: int = 7200
    ) -> bool:
        """
        Execute a Spark job on the specified cluster.

        Args:
            script_path: Path to PySpark script
            cluster_type: 'source' or 'target'
            extra_args: Additional script arguments
            timeout: Job timeout in seconds

        Returns:
            True if job succeeded
        """
        if self.execution_mode == ExecutionMode.LOCAL:
            # For testing - run locally with Python
            cmd = ["python", script_path] + (extra_args or [])
        else:
            cmd = self._build_spark_submit_cmd(script_path, cluster_type, extra_args)

        logger.info(f"Running Spark job: {' '.join(cmd[:5])}...")

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout
            )

            if result.returncode == 0:
                logger.info("Spark job completed successfully")
                return True
            else:
                logger.error(f"Spark job failed: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            logger.error(f"Spark job timed out after {timeout}s")
            return False
        except Exception as e:
            logger.error(f"Spark job error: {e}")
            return False

    def run_extraction(self, config_path: str) -> bool:
        """
        Run extraction phase on source cluster.

        Args:
            config_path: Path to job configuration file

        Returns:
            True if extraction succeeded
        """
        logger.info("Starting extraction phase")
        self.state.phase = WorkflowPhase.EXTRACTING
        self._save_checkpoint()

        script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        extractor_script = os.path.join(script_dir, "extraction", "extractor.py")

        manifest_path = f"{self.config.source_cluster.staging_path}/{self.config.job_id}/manifest"

        extra_args = [
            "--config", config_path,
            "--manifest-path", manifest_path,
            "--log-level", "INFO"
        ]

        success = self._run_spark_job(
            extractor_script,
            "source",
            extra_args
        )

        if success:
            self.state.extraction_complete = True
            self.state.extraction_manifest_path = manifest_path
            self._save_checkpoint()

        return success

    def run_transfer(self) -> bool:
        """
        Run transfer phase between clusters.

        Returns:
            True if transfer succeeded
        """
        logger.info("Starting transfer phase")
        self.state.phase = WorkflowPhase.TRANSFERRING
        self._save_checkpoint()

        if not self.state.extraction_manifest_path:
            logger.error("No extraction manifest found - run extraction first")
            return False

        # Import transfer manager
        from transfer.transfer_manager import TransferManager

        try:
            manager = TransferManager(self.config, self.audit)

            # Read manifest from source cluster
            # In real implementation, this would read from HDFS
            manifest_path = self.state.extraction_manifest_path
            logger.info(f"Reading manifest from {manifest_path}")

            # For the transfer, we need to construct paths based on config
            # since we can't directly read the manifest without Spark

            for table in self.config.tables:
                source_table = f"{table.source_database}.{table.source_table}"
                source_path = (
                    f"{self.config.source_cluster.staging_path}/"
                    f"{self.config.job_id}/"
                    f"{table.source_database}_{table.source_table}"
                )
                target_path = (
                    f"{self.config.target_cluster.staging_path}/"
                    f"{self.config.job_id}/"
                    f"{table.source_database}_{table.source_table}"
                )

                result = manager.transfer_path(source_path, target_path, source_table)

                if result.status.value != "completed":
                    logger.error(f"Transfer failed for {source_table}")
                    return False

                self.state.tables_processed.append(source_table)
                self._save_checkpoint()

            self.state.transfer_complete = True
            self._save_checkpoint()

            summary = manager.get_transfer_summary()
            logger.info(f"Transfer complete: {summary}")

            return True

        except Exception as e:
            logger.error(f"Transfer phase failed: {e}")
            return False

    def run_loading(self, config_path: str) -> bool:
        """
        Run loading phase on target cluster.

        Args:
            config_path: Path to job configuration file

        Returns:
            True if loading succeeded
        """
        logger.info("Starting loading phase")
        self.state.phase = WorkflowPhase.LOADING
        self._save_checkpoint()

        script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        loader_script = os.path.join(script_dir, "loading", "loader.py")

        # Adjust manifest path for target cluster
        manifest_path = self.state.extraction_manifest_path
        if manifest_path:
            manifest_path = manifest_path.replace(
                self.config.source_cluster.staging_path,
                self.config.target_cluster.staging_path
            )

        extra_args = [
            "--config", config_path,
            "--mode", "overwrite",
            "--log-level", "INFO"
        ]

        if manifest_path:
            extra_args.extend(["--manifest-path", manifest_path])

        success = self._run_spark_job(
            loader_script,
            "target",
            extra_args
        )

        if success:
            self.state.loading_complete = True
            self.state.phase = WorkflowPhase.COMPLETED
            self._save_checkpoint()

        return success

    def run_full_workflow(self, config_path: str, resume: bool = False) -> bool:
        """
        Run the complete data transfer workflow.

        Args:
            config_path: Path to job configuration file
            resume: Whether to resume from checkpoint

        Returns:
            True if workflow completed successfully
        """
        logger.info(f"Starting workflow: job_id={self.config.job_id}")

        start_time = time.time()

        # Check for existing checkpoint
        if resume and self._load_checkpoint():
            logger.info(f"Resuming from checkpoint: phase={self.state.phase.value}")
        else:
            self.audit.log_job_start(len(self.config.tables))

        try:
            # Phase 1: Extraction (if not already complete)
            if not self.state.extraction_complete:
                if not self.run_extraction(config_path):
                    raise Exception("Extraction phase failed")

            # Phase 2: Transfer (if not already complete)
            if not self.state.transfer_complete:
                if not self.run_transfer():
                    raise Exception("Transfer phase failed")

            # Phase 3: Loading (if not already complete)
            if not self.state.loading_complete:
                if not self.run_loading(config_path):
                    raise Exception("Loading phase failed")

            # Workflow complete
            duration = time.time() - start_time
            self.audit.log_job_complete(duration, 0, 0)

            logger.info(f"Workflow completed successfully in {duration:.2f}s")
            return True

        except Exception as e:
            self.state.phase = WorkflowPhase.FAILED
            self.state.error_message = str(e)
            self._save_checkpoint()

            duration = time.time() - start_time
            self.audit.log_job_failed(str(e), duration)

            logger.error(f"Workflow failed: {e}")
            return False

    def get_status(self) -> Dict[str, Any]:
        """Get current workflow status."""
        return {
            "job_id": self.config.job_id,
            "phase": self.state.phase.value,
            "extraction_complete": self.state.extraction_complete,
            "transfer_complete": self.state.transfer_complete,
            "loading_complete": self.state.loading_complete,
            "tables_processed": self.state.tables_processed,
            "error": self.state.error_message
        }


def main():
    """Main entry point for workflow orchestration."""
    parser = argparse.ArgumentParser(
        description="Orchestrate Hive data transfer between Cloudera clusters"
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to job configuration YAML file"
    )
    parser.add_argument(
        "--phase",
        choices=["extraction", "transfer", "loading", "full"],
        default="full",
        help="Which phase to run (default: full workflow)"
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint if available"
    )
    parser.add_argument(
        "--execution-mode",
        choices=["local", "yarn", "spark-submit"],
        default="spark-submit",
        help="How to execute Spark jobs"
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

    # Create orchestrator
    execution_mode = ExecutionMode(args.execution_mode)
    orchestrator = WorkflowOrchestrator(config, execution_mode)

    # Run specified phase
    success = False

    if args.phase == "extraction":
        success = orchestrator.run_extraction(args.config)
    elif args.phase == "transfer":
        success = orchestrator.run_transfer()
    elif args.phase == "loading":
        success = orchestrator.run_loading(args.config)
    else:  # full
        success = orchestrator.run_full_workflow(args.config, resume=args.resume)

    # Print final status
    status = orchestrator.get_status()
    print(json.dumps(status, indent=2))

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
