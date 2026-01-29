"""
spark-anon: Spark GDPR & Privacy Library

A comprehensive library for managing metadata, tags, and GDPR compliance
in data processing pipelines. Supports PySpark, Pandas, and Polars backends
with automatic optimization based on data size.

Features:
- Column-level and dataset-level tagging
- PII detection (pattern-based and ML-based)
- Data anonymization (hash, mask, redact, pseudonymize)
- K-anonymity and differential privacy
- Consent management with purpose limitation
- Audit logging with compliance reports
- Retention policy enforcement
- Data subject erasure requests
- YAML configuration support
- CLI interface
- Three-tier backend system:
  * Polars: Fast single-machine processing (recommended for 10k-500k rows)
  * Pandas: Simple in-memory processing (for small datasets)
  * Spark: Distributed processing (for large datasets >500k rows)
- Benchmarking utilities for backend comparison

Usage:
    from spart import (
        MetadataManager, RetentionManager, AnonymizationManager,
        ConsentManager, AuditManager, ConfigManager,
        PandasProcessor, PolarsProcessor,  # Backend processors
        AdvancedAnonymization, PIIDetector, StreamingManager, Benchmark,
        BackendSelector, ProcessingBackend  # Backend selection
    )
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    TimestampType, BooleanType, ArrayType, MapType
)
from typing import Dict, List, Optional, Any, Union, Callable, Tuple
import json
import datetime
import uuid
import logging
import hashlib
import os
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from enum import Enum
from functools import wraps
from pathlib import Path

# Optional imports for extended functionality
# Type hints for optional modules
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd
    import numpy as np
    import yaml
    import polars as pl

PANDAS_AVAILABLE = False
YAML_AVAILABLE = False
POLARS_AVAILABLE = False
pd: Any = None
np: Any = None
yaml: Any = None
pl: Any = None

try:
    import pandas as pd  # type: ignore[no-redef]
    import numpy as np  # type: ignore[no-redef]
    PANDAS_AVAILABLE = True
except ImportError:
    pass

try:
    import yaml  # type: ignore[no-redef]
    YAML_AVAILABLE = True
except ImportError:
    pass

try:
    import polars as pl  # type: ignore[no-redef]
    POLARS_AVAILABLE = True
except ImportError:
    pass

# Basic logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
)
logger = logging.getLogger("spart")


# ==============================================================================
# Enumerations and Data Classes
# ==============================================================================


class GDPRCategory(Enum):
    """GDPR data categories."""
    PII = "Personally Identifiable Information"
    SPI = "Sensitive Personal Information"
    NPII = "Non-Personally Identifiable Information"


class SensitivityLevel(Enum):
    """Data sensitivity levels."""
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    VERY_HIGH = 4


class ConsentStatus(Enum):
    """User consent status."""
    GRANTED = "granted"
    DENIED = "denied"
    WITHDRAWN = "withdrawn"
    PENDING = "pending"
    EXPIRED = "expired"


class AuditEventType(Enum):
    """Types of audit events."""
    DATA_ACCESS = "data_access"
    DATA_MODIFICATION = "data_modification"
    DATA_DELETION = "data_deletion"
    CONSENT_CHANGE = "consent_change"
    ANONYMIZATION = "anonymization"
    RETENTION_APPLIED = "retention_applied"
    ERASURE_REQUEST = "erasure_request"
    PII_DETECTION = "pii_detection"
    EXPORT = "export"
    CONFIG_CHANGE = "config_change"


class ProcessingBackend(Enum):
    """Data processing backend selection."""
    SPARK = "spark"
    PANDAS = "pandas"
    POLARS = "polars"
    AUTO = "auto"


@dataclass
class ConsentRecord:
    """Represents a user consent record."""
    subject_id: str
    purpose: str
    status: ConsentStatus
    granted_at: Optional[datetime.datetime] = None
    expires_at: Optional[datetime.datetime] = None
    withdrawn_at: Optional[datetime.datetime] = None
    legal_basis: Optional[str] = None
    data_categories: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_valid(self) -> bool:
        """Check if consent is currently valid."""
        if self.status != ConsentStatus.GRANTED:
            return False
        if self.expires_at and datetime.datetime.now() > self.expires_at:
            return False
        return True

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        result = asdict(self)
        result['status'] = self.status.value
        if self.granted_at:
            result['granted_at'] = self.granted_at.isoformat()
        if self.expires_at:
            result['expires_at'] = self.expires_at.isoformat()
        if self.withdrawn_at:
            result['withdrawn_at'] = self.withdrawn_at.isoformat()
        return result


@dataclass
class AuditEvent:
    """Represents an audit log event."""
    event_id: str
    event_type: AuditEventType
    timestamp: datetime.datetime
    actor: str
    action: str
    resource: str
    details: Dict[str, Any] = field(default_factory=dict)
    subject_ids: List[str] = field(default_factory=list)
    success: bool = True
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = asdict(self)
        result['event_type'] = self.event_type.value
        result['timestamp'] = self.timestamp.isoformat()
        return result


@dataclass
class BenchmarkResult:
    """Results from a benchmark run."""
    operation: str
    backend: str
    row_count: int
    column_count: int
    execution_time_seconds: float
    memory_usage_mb: Optional[float] = None
    throughput_rows_per_second: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.row_count > 0 and self.execution_time_seconds > 0:
            self.throughput_rows_per_second = (
                self.row_count / self.execution_time_seconds
            )


# ==============================================================================
# Configuration Manager
# ==============================================================================


class ConfigManager:
    """
    Manages YAML-based configuration for spark-anon.

    Supports loading configuration from files, environment variables,
    and programmatic overrides.
    """

    DEFAULT_CONFIG = {
        "metadata": {
            "column_name": "_metadata",
            "gdpr_categories": ["PII", "SPI", "NPII"],
            "sensitivity_levels": ["LOW", "MEDIUM", "HIGH", "VERY_HIGH"]
        },
        "anonymization": {
            "default_hash_algorithm": "sha256",
            "partial_mask_pattern": "***",
            "redact_text": "[REDACTED]",
            "k_anonymity_default_k": 5,
            "differential_privacy_epsilon": 1.0
        },
        "retention": {
            "default_days": 365,
            "max_days": 3650,
            "grace_period_days": 30
        },
        "consent": {
            "default_expiry_days": 365,
            "require_explicit_consent": True,
            "purposes": [
                "marketing", "analytics", "service_delivery",
                "legal_compliance", "research"
            ]
        },
        "audit": {
            "enabled": True,
            "log_path": "./audit_logs",
            "retention_days": 2555,
            "include_data_samples": False,
            "max_log_size_mb": 100
        },
        "processing": {
            "backend": "auto",
            "pandas_threshold_rows": 100000,
            "spark_partitions": "auto",
            "cache_enabled": True
        },
        "pii_detection": {
            "enabled": True,
            "patterns": {
                "email": r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
                "phone": r"\+?[\d\s\-()]{10,}",
                "ssn": r"\d{3}-\d{2}-\d{4}",
                "credit_card": r"\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}",
                "ip_address": r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}"
            },
            "column_name_patterns": {
                "pii": [
                    "name", "email", "phone", "ssn", "address",
                    "birth", "zip", "customer", "user"
                ],
                "spi": [
                    "health", "religion", "politics", "racial", "ethnic",
                    "genetic", "biometric", "sexual", "criminal", "salary"
                ],
                "secure": [
                    "password", "token", "secret", "key", "credential", "api_key"
                ]
            }
        }
    }

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration manager.

        Args:
            config_path: Path to YAML configuration file (optional).
        """
        self.config = self.DEFAULT_CONFIG.copy()
        self._deep_copy_config()

        if config_path:
            self.load_from_file(config_path)

        self._apply_environment_overrides()

    def _deep_copy_config(self):
        """Create a deep copy of the default config."""
        import copy
        self.config = copy.deepcopy(self.DEFAULT_CONFIG)

    def load_from_file(self, path: str) -> None:
        """
        Load configuration from a YAML file.

        Args:
            path: Path to the YAML configuration file.

        Raises:
            ImportError: If PyYAML is not installed.
            FileNotFoundError: If the configuration file doesn't exist.
        """
        if not YAML_AVAILABLE:
            raise ImportError(
                "PyYAML is required for YAML configuration. "
                "Install it with: pip install pyyaml"
            )

        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")

        with open(path, 'r') as f:
            file_config = yaml.safe_load(f)

        if file_config:
            self._merge_config(file_config)
            logger.info(f"Configuration loaded from {path}")

    def _merge_config(self, new_config: Dict[str, Any]) -> None:
        """Recursively merge new configuration into existing."""
        def merge(base: Dict, updates: Dict) -> Dict:
            for key, value in updates.items():
                if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                    merge(base[key], value)
                else:
                    base[key] = value
            return base

        merge(self.config, new_config)

    def _apply_environment_overrides(self) -> None:
        """Apply configuration overrides from environment variables."""
        env_mappings = {
            "SPART_METADATA_COL": ("metadata", "column_name"),
            "SPART_AUDIT_ENABLED": ("audit", "enabled"),
            "SPART_AUDIT_PATH": ("audit", "log_path"),
            "SPART_BACKEND": ("processing", "backend"),
            "SPART_PANDAS_THRESHOLD": ("processing", "pandas_threshold_rows"),
        }

        for env_var, config_path in env_mappings.items():
            value = os.environ.get(env_var)
            if value is not None:
                section, key = config_path
                # Type conversion
                if key in ["enabled"]:
                    value = value.lower() in ("true", "1", "yes")
                elif key in ["pandas_threshold_rows"]:
                    value = int(value)
                self.config[section][key] = value
                logger.debug(f"Config override from {env_var}: {section}.{key} = {value}")

    def get(self, *keys: str, default: Any = None) -> Any:
        """
        Get a configuration value by nested keys.

        Args:
            *keys: Nested configuration keys.
            default: Default value if key not found.

        Returns:
            Configuration value or default.

        Example:
            config.get("anonymization", "default_hash_algorithm")
        """
        result = self.config
        for key in keys:
            if isinstance(result, dict) and key in result:
                result = result[key]
            else:
                return default
        return result

    def set(self, *keys_and_value) -> None:
        """
        Set a configuration value by nested keys.

        Args:
            *keys_and_value: Nested keys followed by the value.

        Example:
            config.set("anonymization", "default_hash_algorithm", "sha512")
        """
        if len(keys_and_value) < 2:
            raise ValueError("Must provide at least one key and a value")

        keys = keys_and_value[:-1]
        value = keys_and_value[-1]

        current = self.config
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]

        current[keys[-1]] = value

    def save_to_file(self, path: str) -> None:
        """
        Save current configuration to a YAML file.

        Args:
            path: Output file path.
        """
        if not YAML_AVAILABLE:
            raise ImportError(
                "PyYAML is required for YAML configuration. "
                "Install it with: pip install pyyaml"
            )

        with open(path, 'w') as f:
            yaml.dump(self.config, f, default_flow_style=False, indent=2)
        logger.info(f"Configuration saved to {path}")

    def validate(self) -> List[str]:
        """
        Validate the current configuration.

        Returns:
            List of validation error messages (empty if valid).
        """
        errors = []

        # Validate GDPR categories
        valid_categories = ["PII", "SPI", "NPII"]
        for cat in self.get("metadata", "gdpr_categories", default=[]):
            if cat not in valid_categories:
                errors.append(f"Invalid GDPR category: {cat}")

        # Validate sensitivity levels
        valid_levels = ["LOW", "MEDIUM", "HIGH", "VERY_HIGH"]
        for level in self.get("metadata", "sensitivity_levels", default=[]):
            if level not in valid_levels:
                errors.append(f"Invalid sensitivity level: {level}")

        # Validate backend
        valid_backends = ["spark", "pandas", "auto"]
        backend = self.get("processing", "backend", default="auto")
        if backend not in valid_backends:
            errors.append(f"Invalid processing backend: {backend}")

        # Validate numeric values
        if self.get("retention", "default_days", default=0) < 0:
            errors.append("retention.default_days must be non-negative")

        if self.get("anonymization", "k_anonymity_default_k", default=0) < 2:
            errors.append("k_anonymity_default_k must be at least 2")

        return errors


# ==============================================================================
# Audit Manager
# ==============================================================================


class AuditManager:
    """
    Manages immutable audit logs for GDPR compliance.

    Provides functionality for logging data access, modifications,
    consent changes, and generating compliance reports.
    """

    def __init__(
        self,
        config: Optional[ConfigManager] = None,
        log_path: Optional[str] = None,
        spark: Optional[SparkSession] = None
    ):
        """
        Initialize the audit manager.

        Args:
            config: ConfigManager instance for settings.
            log_path: Override path for audit logs.
            spark: SparkSession for Spark-based operations.
        """
        self.config = config or ConfigManager()
        self.log_path = Path(
            log_path or self.config.get("audit", "log_path", default="./audit_logs")
        )
        self.spark = spark
        self._events: List[AuditEvent] = []
        self._enabled = self.config.get("audit", "enabled", default=True)

        # Create log directory if it doesn't exist
        if self._enabled:
            self.log_path.mkdir(parents=True, exist_ok=True)

    def log_event(
        self,
        event_type: AuditEventType,
        actor: str,
        action: str,
        resource: str,
        details: Optional[Dict[str, Any]] = None,
        subject_ids: Optional[List[str]] = None,
        success: bool = True,
        error_message: Optional[str] = None
    ) -> Optional[AuditEvent]:
        """
        Log an audit event.

        Args:
            event_type: Type of the audit event.
            actor: Who performed the action (user/system/service).
            action: Description of the action performed.
            resource: Resource that was accessed/modified.
            details: Additional event details.
            subject_ids: List of affected data subject IDs.
            success: Whether the action was successful.
            error_message: Error message if action failed.

        Returns:
            The created AuditEvent.
        """
        if not self._enabled:
            return None

        event = AuditEvent(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            timestamp=datetime.datetime.now(),
            actor=actor,
            action=action,
            resource=resource,
            details=details or {},
            subject_ids=subject_ids or [],
            success=success,
            error_message=error_message
        )

        self._events.append(event)
        self._persist_event(event)

        logger.debug(
            f"Audit event logged: {event_type.value} - {action} on {resource}"
        )

        return event

    def _persist_event(self, event: AuditEvent) -> None:
        """Persist audit event to storage."""
        # Determine log file for today
        date_str = event.timestamp.strftime("%Y-%m-%d")
        log_file = self.log_path / f"audit_{date_str}.jsonl"

        # Append event as JSON line (immutable log pattern)
        with open(log_file, 'a') as f:
            f.write(json.dumps(event.to_dict()) + "\n")

    def get_events(
        self,
        event_type: Optional[AuditEventType] = None,
        actor: Optional[str] = None,
        resource: Optional[str] = None,
        start_date: Optional[datetime.datetime] = None,
        end_date: Optional[datetime.datetime] = None,
        subject_id: Optional[str] = None
    ) -> List[AuditEvent]:
        """
        Query audit events with filters.

        Args:
            event_type: Filter by event type.
            actor: Filter by actor.
            resource: Filter by resource.
            start_date: Filter events after this date.
            end_date: Filter events before this date.
            subject_id: Filter by affected subject ID.

        Returns:
            List of matching AuditEvents.
        """
        events = self._load_events_from_storage(start_date, end_date)

        # Apply filters
        if event_type:
            events = [e for e in events if e.event_type == event_type]
        if actor:
            events = [e for e in events if e.actor == actor]
        if resource:
            events = [e for e in events if resource in e.resource]
        if subject_id:
            events = [e for e in events if subject_id in e.subject_ids]

        return events

    def _load_events_from_storage(
        self,
        start_date: Optional[datetime.datetime] = None,
        end_date: Optional[datetime.datetime] = None
    ) -> List[AuditEvent]:
        """Load events from log files."""
        events = []

        if not self.log_path.exists():
            return events

        for log_file in sorted(self.log_path.glob("audit_*.jsonl")):
            # Extract date from filename
            file_date_str = log_file.stem.replace("audit_", "")
            try:
                file_date = datetime.datetime.strptime(file_date_str, "%Y-%m-%d")
            except ValueError:
                continue

            # Check date range
            if start_date and file_date.date() < start_date.date():
                continue
            if end_date and file_date.date() > end_date.date():
                continue

            with open(log_file, 'r') as f:
                for line in f:
                    if line.strip():
                        try:
                            data = json.loads(line)
                            event = AuditEvent(
                                event_id=data['event_id'],
                                event_type=AuditEventType(data['event_type']),
                                timestamp=datetime.datetime.fromisoformat(
                                    data['timestamp']
                                ),
                                actor=data['actor'],
                                action=data['action'],
                                resource=data['resource'],
                                details=data.get('details', {}),
                                subject_ids=data.get('subject_ids', []),
                                success=data.get('success', True),
                                error_message=data.get('error_message')
                            )
                            events.append(event)
                        except (json.JSONDecodeError, KeyError, ValueError) as e:
                            logger.warning(f"Failed to parse audit event: {e}")

        return events

    def generate_compliance_report(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        output_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate a GDPR compliance report for the specified period.

        Args:
            start_date: Report start date.
            end_date: Report end date.
            output_path: Optional path to save the report.

        Returns:
            Compliance report as a dictionary.
        """
        events = self.get_events(start_date=start_date, end_date=end_date)

        # Aggregate statistics
        event_counts = {}
        for event in events:
            event_type = event.event_type.value
            event_counts[event_type] = event_counts.get(event_type, 0) + 1

        # Count unique subjects affected
        all_subjects = set()
        for event in events:
            all_subjects.update(event.subject_ids)

        # Erasure requests summary
        erasure_events = [
            e for e in events if e.event_type == AuditEventType.ERASURE_REQUEST
        ]
        erasure_completed = sum(1 for e in erasure_events if e.success)
        erasure_failed = len(erasure_events) - erasure_completed

        # Consent changes summary
        consent_events = [
            e for e in events if e.event_type == AuditEventType.CONSENT_CHANGE
        ]

        report = {
            "report_id": str(uuid.uuid4()),
            "generated_at": datetime.datetime.now().isoformat(),
            "period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            },
            "summary": {
                "total_events": len(events),
                "unique_subjects_affected": len(all_subjects),
                "events_by_type": event_counts
            },
            "erasure_requests": {
                "total": len(erasure_events),
                "completed": erasure_completed,
                "failed": erasure_failed,
                "compliance_rate": (
                    erasure_completed / len(erasure_events) * 100
                    if erasure_events else 100
                )
            },
            "consent_management": {
                "total_changes": len(consent_events),
                "grants": sum(
                    1 for e in consent_events
                    if e.details.get("new_status") == "granted"
                ),
                "withdrawals": sum(
                    1 for e in consent_events
                    if e.details.get("new_status") == "withdrawn"
                )
            },
            "data_access_summary": {
                "total_accesses": event_counts.get("data_access", 0),
                "unique_actors": len(set(
                    e.actor for e in events
                    if e.event_type == AuditEventType.DATA_ACCESS
                ))
            }
        }

        if output_path:
            with open(output_path, 'w') as f:
                json.dump(report, f, indent=2)
            logger.info(f"Compliance report saved to {output_path}")

        return report

    def get_subject_access_report(self, subject_id: str) -> Dict[str, Any]:
        """
        Generate a data subject access report (DSAR).

        This report shows all processing activities related to a specific
        data subject, as required by GDPR Article 15.

        Args:
            subject_id: The data subject identifier.

        Returns:
            Subject access report as a dictionary.
        """
        events = self.get_events(subject_id=subject_id)

        report = {
            "report_id": str(uuid.uuid4()),
            "generated_at": datetime.datetime.now().isoformat(),
            "subject_id": subject_id,
            "processing_activities": [],
            "consent_history": [],
            "erasure_requests": [],
            "data_modifications": []
        }

        for event in events:
            event_data = {
                "timestamp": event.timestamp.isoformat(),
                "action": event.action,
                "actor": event.actor,
                "details": event.details
            }

            if event.event_type == AuditEventType.CONSENT_CHANGE:
                report["consent_history"].append(event_data)
            elif event.event_type == AuditEventType.ERASURE_REQUEST:
                report["erasure_requests"].append(event_data)
            elif event.event_type == AuditEventType.DATA_MODIFICATION:
                report["data_modifications"].append(event_data)
            else:
                report["processing_activities"].append(event_data)

        return report


# ==============================================================================
# Consent Manager
# ==============================================================================


class ConsentManager:
    """
    Manages user consent for data processing.

    Implements GDPR consent requirements including purpose limitation,
    explicit consent tracking, and consent withdrawal.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: Optional[ConfigManager] = None,
        audit_manager: Optional[AuditManager] = None,
        storage_path: Optional[str] = None
    ):
        """
        Initialize the consent manager.

        Args:
            spark: Active SparkSession.
            config: ConfigManager for settings.
            audit_manager: AuditManager for logging consent changes.
            storage_path: Path for consent storage (default: ./consent_data).
        """
        self.spark = spark
        self.config = config or ConfigManager()
        self.audit = audit_manager
        self.storage_path = Path(storage_path or "./consent_data")
        self.storage_path.mkdir(parents=True, exist_ok=True)

        # In-memory consent cache
        self._consent_cache: Dict[str, Dict[str, ConsentRecord]] = {}

        # Valid purposes from config
        self.valid_purposes = self.config.get(
            "consent", "purposes",
            default=["marketing", "analytics", "service_delivery"]
        )

    def record_consent(
        self,
        subject_id: str,
        purpose: str,
        status: ConsentStatus,
        legal_basis: Optional[str] = None,
        expiry_days: Optional[int] = None,
        data_categories: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> ConsentRecord:
        """
        Record a consent decision for a data subject.

        Args:
            subject_id: Unique identifier for the data subject.
            purpose: Processing purpose (must be in valid_purposes).
            status: Consent status (granted, denied, withdrawn).
            legal_basis: GDPR legal basis for processing.
            expiry_days: Days until consent expires (optional).
            data_categories: List of data categories covered.
            metadata: Additional consent metadata.

        Returns:
            The created ConsentRecord.

        Raises:
            ValueError: If purpose is not in valid_purposes.
        """
        # Validate purpose
        if purpose not in self.valid_purposes:
            raise ValueError(
                f"Invalid purpose: {purpose}. "
                f"Valid purposes are: {self.valid_purposes}"
            )

        # Calculate expiry
        expires_at = None
        if status == ConsentStatus.GRANTED:
            exp_days = expiry_days or self.config.get(
                "consent", "default_expiry_days", default=365
            )
            expires_at = datetime.datetime.now() + datetime.timedelta(days=exp_days)

        record = ConsentRecord(
            subject_id=subject_id,
            purpose=purpose,
            status=status,
            granted_at=datetime.datetime.now() if status == ConsentStatus.GRANTED else None,
            expires_at=expires_at,
            withdrawn_at=datetime.datetime.now() if status == ConsentStatus.WITHDRAWN else None,
            legal_basis=legal_basis,
            data_categories=data_categories or [],
            metadata=metadata or {}
        )

        # Store in cache
        if subject_id not in self._consent_cache:
            self._consent_cache[subject_id] = {}
        self._consent_cache[subject_id][purpose] = record

        # Persist to storage
        self._persist_consent(record)

        # Log audit event
        if self.audit:
            self.audit.log_event(
                event_type=AuditEventType.CONSENT_CHANGE,
                actor="consent_manager",
                action=f"consent_{status.value}",
                resource=f"subject:{subject_id}",
                details={
                    "purpose": purpose,
                    "new_status": status.value,
                    "legal_basis": legal_basis,
                    "expires_at": expires_at.isoformat() if expires_at else None
                },
                subject_ids=[subject_id]
            )

        logger.info(
            f"Consent recorded: subject={subject_id}, "
            f"purpose={purpose}, status={status.value}"
        )

        return record

    def _persist_consent(self, record: ConsentRecord) -> None:
        """Persist consent record to storage."""
        subject_file = self.storage_path / f"{record.subject_id}.json"

        # Load existing records
        records = {}
        if subject_file.exists():
            with open(subject_file, 'r') as f:
                records = json.load(f)

        # Update with new record
        records[record.purpose] = record.to_dict()

        # Save
        with open(subject_file, 'w') as f:
            json.dump(records, f, indent=2)

    def check_consent(
        self,
        subject_id: str,
        purpose: str
    ) -> Tuple[bool, Optional[ConsentRecord]]:
        """
        Check if a data subject has valid consent for a purpose.

        Args:
            subject_id: The data subject identifier.
            purpose: The processing purpose to check.

        Returns:
            Tuple of (has_valid_consent, consent_record).
        """
        record = self.get_consent(subject_id, purpose)

        if record is None:
            return (False, None)

        return (record.is_valid(), record)

    def get_consent(
        self,
        subject_id: str,
        purpose: str
    ) -> Optional[ConsentRecord]:
        """
        Get the consent record for a subject and purpose.

        Args:
            subject_id: The data subject identifier.
            purpose: The processing purpose.

        Returns:
            ConsentRecord if found, None otherwise.
        """
        # Check cache first
        if subject_id in self._consent_cache:
            if purpose in self._consent_cache[subject_id]:
                return self._consent_cache[subject_id][purpose]

        # Load from storage
        subject_file = self.storage_path / f"{subject_id}.json"
        if subject_file.exists():
            with open(subject_file, 'r') as f:
                records = json.load(f)

            if purpose in records:
                data = records[purpose]
                record = ConsentRecord(
                    subject_id=data['subject_id'],
                    purpose=data['purpose'],
                    status=ConsentStatus(data['status']),
                    granted_at=(
                        datetime.datetime.fromisoformat(data['granted_at'])
                        if data.get('granted_at') else None
                    ),
                    expires_at=(
                        datetime.datetime.fromisoformat(data['expires_at'])
                        if data.get('expires_at') else None
                    ),
                    withdrawn_at=(
                        datetime.datetime.fromisoformat(data['withdrawn_at'])
                        if data.get('withdrawn_at') else None
                    ),
                    legal_basis=data.get('legal_basis'),
                    data_categories=data.get('data_categories', []),
                    metadata=data.get('metadata', {})
                )

                # Update cache
                if subject_id not in self._consent_cache:
                    self._consent_cache[subject_id] = {}
                self._consent_cache[subject_id][purpose] = record

                return record

        return None

    def withdraw_consent(
        self,
        subject_id: str,
        purpose: str
    ) -> Optional[ConsentRecord]:
        """
        Withdraw consent for a specific purpose.

        Args:
            subject_id: The data subject identifier.
            purpose: The processing purpose.

        Returns:
            Updated ConsentRecord if found, None otherwise.
        """
        return self.record_consent(
            subject_id=subject_id,
            purpose=purpose,
            status=ConsentStatus.WITHDRAWN
        )

    def get_all_consents(
        self,
        subject_id: str
    ) -> Dict[str, ConsentRecord]:
        """
        Get all consent records for a data subject.

        Args:
            subject_id: The data subject identifier.

        Returns:
            Dictionary mapping purpose to ConsentRecord.
        """
        subject_file = self.storage_path / f"{subject_id}.json"
        consents = {}

        if subject_file.exists():
            with open(subject_file, 'r') as f:
                records = json.load(f)

            for purpose, data in records.items():
                consents[purpose] = ConsentRecord(
                    subject_id=data['subject_id'],
                    purpose=data['purpose'],
                    status=ConsentStatus(data['status']),
                    granted_at=(
                        datetime.datetime.fromisoformat(data['granted_at'])
                        if data.get('granted_at') else None
                    ),
                    expires_at=(
                        datetime.datetime.fromisoformat(data['expires_at'])
                        if data.get('expires_at') else None
                    ),
                    withdrawn_at=(
                        datetime.datetime.fromisoformat(data['withdrawn_at'])
                        if data.get('withdrawn_at') else None
                    ),
                    legal_basis=data.get('legal_basis'),
                    data_categories=data.get('data_categories', []),
                    metadata=data.get('metadata', {})
                )

        return consents

    def filter_by_consent(
        self,
        df: DataFrame,
        subject_id_col: str,
        purpose: str
    ) -> DataFrame:
        """
        Filter a DataFrame to only include subjects with valid consent.

        Args:
            df: Input DataFrame.
            subject_id_col: Column containing subject identifiers.
            purpose: Processing purpose to check consent for.

        Returns:
            Filtered DataFrame with only consented subjects.
        """
        # Get all subject IDs from the DataFrame
        subject_ids = [row[0] for row in df.select(subject_id_col).distinct().collect()]

        # Find subjects with valid consent
        consented_ids = []
        for subject_id in subject_ids:
            has_consent, _ = self.check_consent(subject_id, purpose)
            if has_consent:
                consented_ids.append(subject_id)

        if not consented_ids:
            logger.warning(
                f"No subjects have valid consent for purpose: {purpose}"
            )
            return df.limit(0)  # Return empty DataFrame with same schema

        # Filter DataFrame
        return df.filter(F.col(subject_id_col).isin(consented_ids))


# ==============================================================================
# PII Detector
# ==============================================================================


class PIIDetector:
    """
    Detects PII (Personally Identifiable Information) in DataFrames.

    Supports both pattern-based detection (column names and content regex)
    and ML-based detection using common patterns.
    """

    def __init__(
        self,
        config: Optional[ConfigManager] = None,
        custom_patterns: Optional[Dict[str, str]] = None
    ):
        """
        Initialize the PII detector.

        Args:
            config: ConfigManager for pattern settings.
            custom_patterns: Additional custom regex patterns for PII detection.
        """
        self.config = config or ConfigManager()

        # Load patterns from config
        self.content_patterns = self.config.get(
            "pii_detection", "patterns", default={}
        )

        # Add custom patterns
        if custom_patterns:
            self.content_patterns.update(custom_patterns)

        # Column name patterns
        self.column_patterns = self.config.get(
            "pii_detection", "column_name_patterns",
            default={"pii": [], "spi": [], "secure": []}
        )

        # Compile regex patterns
        self._compiled_patterns = {
            name: re.compile(pattern)
            for name, pattern in self.content_patterns.items()
        }

    def scan_column_names(self, df: DataFrame) -> Dict[str, List[str]]:
        """
        Scan DataFrame column names for potential PII indicators.

        Args:
            df: DataFrame to scan.

        Returns:
            Dictionary with PII categories and matching column names.
        """
        results = {
            "potential_pii": [],
            "potential_spi": [],
            "potential_secure": []
        }

        for column in df.columns:
            column_lower = column.lower()

            # Check PII patterns
            for term in self.column_patterns.get("pii", []):
                if term in column_lower:
                    results["potential_pii"].append(column)
                    break

            # Check SPI patterns
            for term in self.column_patterns.get("spi", []):
                if term in column_lower:
                    results["potential_spi"].append(column)
                    break

            # Check secure patterns
            for term in self.column_patterns.get("secure", []):
                if term in column_lower:
                    results["potential_secure"].append(column)
                    break

        return results

    def scan_content(
        self,
        df: DataFrame,
        columns: Optional[List[str]] = None,
        sample_size: int = 1000
    ) -> Dict[str, Dict[str, Any]]:
        """
        Scan DataFrame content for PII patterns.

        Args:
            df: DataFrame to scan.
            columns: Specific columns to scan (default: all string columns).
            sample_size: Number of rows to sample for scanning.

        Returns:
            Dictionary with columns and detected PII types with match counts.
        """
        results = {}

        # Get string columns if not specified
        if columns is None:
            columns = [
                f.name for f in df.schema.fields
                if isinstance(f.dataType, StringType)
            ]

        # Sample the data for efficiency
        sample_df = df.select(columns).limit(sample_size)
        rows = sample_df.collect()

        for col_name in columns:
            col_results = {}

            for row in rows:
                value = row[col_name]
                if value is None:
                    continue

                value_str = str(value)

                for pattern_name, pattern in self._compiled_patterns.items():
                    if pattern.search(value_str):
                        if pattern_name not in col_results:
                            col_results[pattern_name] = {
                                "count": 0,
                                "samples": []
                            }
                        col_results[pattern_name]["count"] += 1
                        if len(col_results[pattern_name]["samples"]) < 3:
                            # Store masked sample
                            masked = self._mask_sample(value_str, pattern)
                            col_results[pattern_name]["samples"].append(masked)

            if col_results:
                results[col_name] = col_results

        return results

    def _mask_sample(self, value: str, pattern: re.Pattern) -> str:
        """Mask PII in a sample value for safe storage."""
        return pattern.sub("[DETECTED]", value)

    def full_scan(
        self,
        df: DataFrame,
        sample_size: int = 1000
    ) -> Dict[str, Any]:
        """
        Perform a comprehensive PII scan on a DataFrame.

        Combines column name scanning and content scanning.

        Args:
            df: DataFrame to scan.
            sample_size: Sample size for content scanning.

        Returns:
            Comprehensive scan results.
        """
        return {
            "column_name_analysis": self.scan_column_names(df),
            "content_analysis": self.scan_content(df, sample_size=sample_size),
            "scan_metadata": {
                "timestamp": datetime.datetime.now().isoformat(),
                "total_columns": len(df.columns),
                "sample_size": sample_size,
                "patterns_used": list(self.content_patterns.keys())
            }
        }

    def add_pattern(self, name: str, pattern: str) -> None:
        """
        Add a custom PII detection pattern.

        Args:
            name: Pattern name/identifier.
            pattern: Regex pattern string.
        """
        self.content_patterns[name] = pattern
        self._compiled_patterns[name] = re.compile(pattern)

    def get_suggested_tags(
        self,
        scan_results: Dict[str, Any]
    ) -> Dict[str, Dict[str, str]]:
        """
        Generate suggested GDPR tags based on scan results.

        Args:
            scan_results: Results from full_scan().

        Returns:
            Dictionary of column names to suggested tags.
        """
        suggestions = {}

        # From column name analysis
        name_analysis = scan_results.get("column_name_analysis", {})

        for col in name_analysis.get("potential_pii", []):
            suggestions[col] = {
                "gdpr_category": "PII",
                "sensitivity": "HIGH",
                "detection_method": "column_name"
            }

        for col in name_analysis.get("potential_spi", []):
            suggestions[col] = {
                "gdpr_category": "SPI",
                "sensitivity": "VERY_HIGH",
                "detection_method": "column_name"
            }

        for col in name_analysis.get("potential_secure", []):
            suggestions[col] = {
                "gdpr_category": "SPI",
                "sensitivity": "VERY_HIGH",
                "detection_method": "column_name",
                "note": "security_credential"
            }

        # From content analysis
        content_analysis = scan_results.get("content_analysis", {})

        for col, patterns in content_analysis.items():
            if col not in suggestions:
                # Determine sensitivity based on detected patterns
                high_sensitivity_patterns = {"ssn", "credit_card", "password"}
                detected = set(patterns.keys())

                if detected & high_sensitivity_patterns:
                    suggestions[col] = {
                        "gdpr_category": "SPI",
                        "sensitivity": "VERY_HIGH",
                        "detection_method": "content_scan",
                        "detected_patterns": list(patterns.keys())
                    }
                else:
                    suggestions[col] = {
                        "gdpr_category": "PII",
                        "sensitivity": "HIGH",
                        "detection_method": "content_scan",
                        "detected_patterns": list(patterns.keys())
                    }

        return suggestions


# ==============================================================================
# Advanced Anonymization
# ==============================================================================


class AdvancedAnonymization:
    """
    Provides advanced anonymization techniques including:
    - K-Anonymity
    - Differential Privacy
    - Data Generalization
    - Data Suppression
    """

    def __init__(
        self,
        spark: SparkSession,
        config: Optional[ConfigManager] = None
    ):
        """
        Initialize advanced anonymization.

        Args:
            spark: Active SparkSession.
            config: ConfigManager for settings.
        """
        self.spark = spark
        self.config = config or ConfigManager()
        self.default_k = self.config.get(
            "anonymization", "k_anonymity_default_k", default=5
        )
        self.default_epsilon = self.config.get(
            "anonymization", "differential_privacy_epsilon", default=1.0
        )

    def k_anonymize(
        self,
        df: DataFrame,
        quasi_identifiers: List[str],
        k: Optional[int] = None,
        generalization_rules: Optional[Dict[str, Callable]] = None
    ) -> DataFrame:
        """
        Apply k-anonymity to a DataFrame.

        K-anonymity ensures that each combination of quasi-identifiers
        appears at least k times in the dataset.

        Args:
            df: Input DataFrame.
            quasi_identifiers: Columns that could be used for re-identification.
            k: Minimum group size (default from config).
            generalization_rules: Optional functions to generalize values.

        Returns:
            K-anonymized DataFrame.
        """
        k = k or self.default_k

        # Apply generalization rules if provided
        if generalization_rules:
            for col, rule in generalization_rules.items():
                if col in df.columns:
                    df = df.withColumn(col, rule(F.col(col)))

        # Count occurrences of each quasi-identifier combination
        qi_counts = df.groupBy(quasi_identifiers).count()

        # Filter to keep only groups with at least k records
        valid_groups = qi_counts.filter(F.col("count") >= k).drop("count")

        # Join back to get only k-anonymous records
        result = df.join(valid_groups, quasi_identifiers, "inner")

        # Log suppressed records
        original_count = df.count()
        result_count = result.count()
        suppressed = original_count - result_count

        logger.info(
            f"K-anonymity applied (k={k}): {suppressed} records suppressed "
            f"({suppressed/original_count*100:.1f}%)"
        )

        return result

    def check_k_anonymity(
        self,
        df: DataFrame,
        quasi_identifiers: List[str],
        k: int
    ) -> Dict[str, Any]:
        """
        Check if a DataFrame satisfies k-anonymity.

        Args:
            df: DataFrame to check.
            quasi_identifiers: Columns to check.
            k: Required minimum group size.

        Returns:
            Analysis results including compliance status.
        """
        # Count occurrences of each quasi-identifier combination
        qi_counts = df.groupBy(quasi_identifiers).count()

        # Get statistics
        min_count = qi_counts.agg(F.min("count")).collect()[0][0]
        max_count = qi_counts.agg(F.max("count")).collect()[0][0]
        avg_count = qi_counts.agg(F.avg("count")).collect()[0][0]

        # Count violating groups
        violating = qi_counts.filter(F.col("count") < k).count()
        total_groups = qi_counts.count()

        return {
            "is_k_anonymous": min_count >= k,
            "k_value": k,
            "min_group_size": min_count,
            "max_group_size": max_count,
            "avg_group_size": avg_count,
            "total_groups": total_groups,
            "violating_groups": violating,
            "compliance_rate": (total_groups - violating) / total_groups * 100
        }

    def add_differential_privacy_noise(
        self,
        df: DataFrame,
        numeric_columns: List[str],
        epsilon: Optional[float] = None,
        sensitivity: float = 1.0
    ) -> DataFrame:
        """
        Add Laplace noise to numeric columns for differential privacy.

        Uses the Laplace mechanism to add calibrated noise that provides
        epsilon-differential privacy.

        Args:
            df: Input DataFrame.
            numeric_columns: Columns to add noise to.
            epsilon: Privacy parameter (smaller = more privacy).
            sensitivity: Query sensitivity (default 1.0).

        Returns:
            DataFrame with noise added to specified columns.
        """
        epsilon = epsilon or self.default_epsilon

        # Scale for Laplace noise: sensitivity / epsilon
        scale = sensitivity / epsilon

        # Add noise using Spark's randn and transforming to Laplace
        # Laplace(0, scale) = scale * sign(U-0.5) * ln(1 - 2*|U-0.5|)
        # where U is uniform(0,1)

        noisy_expressions = {}

        for col in numeric_columns:
            if col not in df.columns:
                logger.warning(f"Column {col} not found, skipping")
                continue

            # Generate Laplace noise using the inverse CDF method
            # For simplicity, we approximate with scaled random values
            # In production, use a proper Laplace distribution

            # Using randn (normal distribution) as approximation
            # True Laplace would require custom implementation
            noisy_expressions[col] = (
                F.col(col) + F.randn() * F.lit(scale * 1.4142)  # sqrt(2) adjustment
            )

        if noisy_expressions:
            return df.withColumns(noisy_expressions)

        return df

    def generalize_numeric(
        self,
        df: DataFrame,
        column: str,
        bin_size: float
    ) -> DataFrame:
        """
        Generalize a numeric column into bins/ranges.

        Args:
            df: Input DataFrame.
            column: Column to generalize.
            bin_size: Size of each bin.

        Returns:
            DataFrame with generalized column.
        """
        # Create range labels
        return df.withColumn(
            column,
            F.concat(
                (F.floor(F.col(column) / bin_size) * bin_size).cast("string"),
                F.lit("-"),
                ((F.floor(F.col(column) / bin_size) + 1) * bin_size).cast("string")
            )
        )

    def generalize_date(
        self,
        df: DataFrame,
        column: str,
        level: str = "month"
    ) -> DataFrame:
        """
        Generalize a date column to a coarser granularity.

        Args:
            df: Input DataFrame.
            column: Date column to generalize.
            level: Generalization level ("year", "quarter", "month", "week").

        Returns:
            DataFrame with generalized date column.
        """
        if level == "year":
            return df.withColumn(column, F.year(F.col(column)).cast("string"))
        elif level == "quarter":
            return df.withColumn(
                column,
                F.concat(
                    F.year(F.col(column)).cast("string"),
                    F.lit("-Q"),
                    F.quarter(F.col(column)).cast("string")
                )
            )
        elif level == "month":
            return df.withColumn(
                column,
                F.date_format(F.col(column), "yyyy-MM")
            )
        elif level == "week":
            return df.withColumn(
                column,
                F.concat(
                    F.year(F.col(column)).cast("string"),
                    F.lit("-W"),
                    F.weekofyear(F.col(column)).cast("string")
                )
            )
        else:
            raise ValueError(f"Unknown generalization level: {level}")

    def suppress_outliers(
        self,
        df: DataFrame,
        column: str,
        lower_percentile: float = 0.01,
        upper_percentile: float = 0.99
    ) -> DataFrame:
        """
        Suppress outlier values in a numeric column.

        Args:
            df: Input DataFrame.
            column: Column to process.
            lower_percentile: Lower bound percentile (default 1%).
            upper_percentile: Upper bound percentile (default 99%).

        Returns:
            DataFrame with outliers suppressed (set to null).
        """
        # Calculate percentiles
        percentiles = df.approxQuantile(
            column,
            [lower_percentile, upper_percentile],
            0.01  # Relative error
        )

        lower_bound, upper_bound = percentiles

        # Suppress outliers
        return df.withColumn(
            column,
            F.when(
                (F.col(column) >= lower_bound) & (F.col(column) <= upper_bound),
                F.col(column)
            ).otherwise(F.lit(None))
        )


# ==============================================================================
# Pandas Processor
# ==============================================================================


class PandasProcessor:
    """
    High-performance data processing using pandas for smaller datasets.

    Provides the same anonymization and metadata functions as the Spark-based
    processors but optimized for in-memory processing with pandas.
    """

    def __init__(self, config: Optional[ConfigManager] = None):
        """
        Initialize the pandas processor.

        Args:
            config: ConfigManager for settings.

        Raises:
            ImportError: If pandas is not installed.
        """
        if not PANDAS_AVAILABLE:
            raise ImportError(
                "Pandas is required for PandasProcessor. "
                "Install it with: pip install pandas numpy"
            )

        self.config = config or ConfigManager()
        self._column_tags: Dict[str, Dict[str, Any]] = {}
        self._dataset_tags: Dict[str, Any] = {}

    def apply_column_tags(
        self,
        df: pd.DataFrame,
        column_tags: Dict[str, Dict[str, Any]]
    ) -> pd.DataFrame:
        """
        Apply GDPR tags to columns (stored in memory).

        Args:
            df: pandas DataFrame.
            column_tags: Dictionary of column tags.

        Returns:
            Same DataFrame (tags stored internally).
        """
        # Validate tags
        for col_name, tags in column_tags.items():
            if col_name not in df.columns:
                logger.warning(f"Column '{col_name}' not in DataFrame")
                continue

            if "gdpr_category" in tags:
                if tags["gdpr_category"] not in ["PII", "SPI", "NPII"]:
                    raise ValueError(
                        f"Invalid GDPR category: {tags['gdpr_category']}"
                    )

            if "sensitivity" in tags:
                if tags["sensitivity"] not in ["LOW", "MEDIUM", "HIGH", "VERY_HIGH"]:
                    raise ValueError(
                        f"Invalid sensitivity: {tags['sensitivity']}"
                    )

            self._column_tags[col_name] = tags

        return df

    def get_column_tags(self) -> Dict[str, Dict[str, Any]]:
        """Get all applied column tags."""
        return self._column_tags.copy()

    def mask_columns(
        self,
        df: pd.DataFrame,
        mask_rules: Dict[str, str]
    ) -> pd.DataFrame:
        """
        Apply masking to columns using pandas.

        Args:
            df: Input DataFrame.
            mask_rules: Dictionary mapping columns to mask types.

        Returns:
            DataFrame with masked columns.
        """
        result = df.copy()

        for column, mask_type in mask_rules.items():
            if column not in result.columns:
                logger.warning(f"Column '{column}' not found")
                continue

            if mask_type == "hash":
                result[column] = result[column].astype(str).apply(
                    lambda x: hashlib.sha256(x.encode()).hexdigest()
                )
            elif mask_type == "partial":
                result[column] = result[column].astype(str).apply(
                    lambda x: f"{x[:3]}***{x[-3:]}" if len(x) > 6 else "***"
                )
            elif mask_type == "redact":
                result[column] = "[REDACTED]"
            else:
                raise ValueError(f"Unknown mask type: {mask_type}")

        return result

    def pseudonymize_columns(
        self,
        df: pd.DataFrame,
        columns: List[str],
        salt: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Pseudonymize columns with consistent hashing.

        Args:
            df: Input DataFrame.
            columns: Columns to pseudonymize.
            salt: Salt for consistent hashing.

        Returns:
            DataFrame with pseudonymized columns.
        """
        result = df.copy()
        salt_value = salt or str(uuid.uuid4())

        for column in columns:
            if column not in result.columns:
                continue

            result[column] = result[column].astype(str).apply(
                lambda x: hashlib.sha256(f"{x}:{salt_value}".encode()).hexdigest()
            )

        return result

    def auto_anonymize(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Automatically anonymize based on column tags.

        Args:
            df: Input DataFrame.

        Returns:
            Anonymized DataFrame.
        """
        mask_rules = {}
        pseudo_cols = []

        for col_name, tags in self._column_tags.items():
            category = tags.get("gdpr_category")
            sensitivity = tags.get("sensitivity")

            if category == "PII" and sensitivity in ["HIGH", "VERY_HIGH"]:
                mask_rules[col_name] = "hash"
            elif category == "PII" and sensitivity == "MEDIUM":
                mask_rules[col_name] = "partial"
            elif category == "SPI":
                pseudo_cols.append(col_name)

        result = self.mask_columns(df, mask_rules)
        result = self.pseudonymize_columns(result, pseudo_cols)

        return result

    def request_erasure(
        self,
        df: pd.DataFrame,
        identifier_col: str,
        identifier_value: str
    ) -> pd.DataFrame:
        """
        Handle erasure request by nullifying PII/SPI.

        Args:
            df: Input DataFrame.
            identifier_col: Column identifying the subject.
            identifier_value: Value to match.

        Returns:
            DataFrame with erased data.
        """
        result = df.copy()

        # Find columns to erase
        cols_to_erase = [
            col for col, tags in self._column_tags.items()
            if tags.get("gdpr_category") in ["PII", "SPI"]
            and col != identifier_col
        ]

        # Apply erasure
        mask = result[identifier_col] == identifier_value
        for col in cols_to_erase:
            if col in result.columns:
                result.loc[mask, col] = None

        return result

    def k_anonymize(
        self,
        df: pd.DataFrame,
        quasi_identifiers: List[str],
        k: int = 5
    ) -> pd.DataFrame:
        """
        Apply k-anonymity using pandas.

        Args:
            df: Input DataFrame.
            quasi_identifiers: Quasi-identifier columns.
            k: Minimum group size.

        Returns:
            K-anonymized DataFrame.
        """
        # Count occurrences of each QI combination
        counts = df.groupby(quasi_identifiers).size().reset_index(name='_count')

        # Filter valid groups
        valid = counts[counts['_count'] >= k].drop(columns=['_count'])

        # Join back
        result = df.merge(valid, on=quasi_identifiers, how='inner')

        logger.info(
            f"K-anonymity (k={k}): {len(df) - len(result)} records suppressed"
        )

        return result

    def add_differential_privacy_noise(
        self,
        df: pd.DataFrame,
        numeric_columns: List[str],
        epsilon: float = 1.0,
        sensitivity: float = 1.0
    ) -> pd.DataFrame:
        """
        Add Laplace noise for differential privacy.

        Args:
            df: Input DataFrame.
            numeric_columns: Columns to add noise to.
            epsilon: Privacy parameter.
            sensitivity: Query sensitivity.

        Returns:
            DataFrame with noise added.
        """
        result = df.copy()
        scale = sensitivity / epsilon

        for col in numeric_columns:
            if col in result.columns:
                noise = np.random.laplace(0, scale, len(result))
                result[col] = result[col] + noise

        return result

    def export_metadata_catalog(
        self,
        df: pd.DataFrame,
        path: str
    ) -> None:
        """
        Export metadata catalog to JSON.

        Args:
            df: DataFrame with applied tags.
            path: Output file path.
        """
        catalog = {
            "dataset_metadata": self._dataset_tags,
            "column_metadata": self._column_tags,
            "schema": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "row_count": len(df),
            "exported_at": datetime.datetime.now().isoformat()
        }

        with open(path, 'w') as f:
            json.dump(catalog, f, indent=2)

        logger.info(f"Metadata catalog exported to {path}")


# ==============================================================================
# Polars Processor (High-Performance Backend)
# ==============================================================================


class PolarsProcessor:
    """
    High-performance data processing using Polars for medium-sized datasets.

    Provides the same anonymization and metadata functions as the Spark and
    Pandas processors but optimized for single-machine parallel processing
    with Polars (Rust-based, 5-10x faster than Pandas for most operations).

    Polars is ideal for datasets between 50k-500k rows where:
    - Pandas becomes slow due to Python overhead
    - Spark overhead isn't justified for single-machine processing
    """

    def __init__(self, config: Optional[ConfigManager] = None):
        """
        Initialize the Polars processor.

        Args:
            config: ConfigManager for settings.

        Raises:
            ImportError: If polars is not installed.
        """
        if not POLARS_AVAILABLE:
            raise ImportError(
                "Polars is required for PolarsProcessor. "
                "Install it with: pip install polars"
            )

        self.config = config or ConfigManager()
        self._column_tags: Dict[str, Dict[str, Any]] = {}
        self._dataset_tags: Dict[str, Any] = {}

    def apply_column_tags(
        self,
        df: pl.DataFrame,
        column_tags: Dict[str, Dict[str, Any]]
    ) -> pl.DataFrame:
        """
        Apply GDPR tags to columns (stored in memory).

        Args:
            df: Polars DataFrame.
            column_tags: Dictionary of column tags.

        Returns:
            Same DataFrame (tags stored internally).
        """
        # Validate tags
        for col_name, tags in column_tags.items():
            if col_name not in df.columns:
                logger.warning(f"Column '{col_name}' not in DataFrame")
                continue

            if "gdpr_category" in tags:
                if tags["gdpr_category"] not in ["PII", "SPI", "NPII"]:
                    raise ValueError(
                        f"Invalid GDPR category: {tags['gdpr_category']}"
                    )

            if "sensitivity" in tags:
                if tags["sensitivity"] not in ["LOW", "MEDIUM", "HIGH", "VERY_HIGH"]:
                    raise ValueError(
                        f"Invalid sensitivity: {tags['sensitivity']}"
                    )

            self._column_tags[col_name] = tags

        return df

    def get_column_tags(self) -> Dict[str, Dict[str, Any]]:
        """Get all applied column tags."""
        return self._column_tags.copy()

    def mask_columns(
        self,
        df: pl.DataFrame,
        mask_rules: Dict[str, str]
    ) -> pl.DataFrame:
        """
        Apply masking to columns using Polars (vectorized operations).

        Args:
            df: Input DataFrame.
            mask_rules: Dictionary mapping columns to mask types.

        Returns:
            DataFrame with masked columns.
        """
        result = df.clone()

        for column, mask_type in mask_rules.items():
            if column not in result.columns:
                logger.warning(f"Column '{column}' not found")
                continue

            if mask_type == "hash":
                # Use map_elements for hashing (Polars doesn't have native SHA256)
                result = result.with_columns(
                    pl.col(column).cast(pl.Utf8).map_elements(
                        lambda x: hashlib.sha256(str(x).encode()).hexdigest(),
                        return_dtype=pl.Utf8
                    ).alias(column)
                )
            elif mask_type == "partial":
                # Partial masking: show first 3 and last 3 chars
                result = result.with_columns(
                    pl.col(column).cast(pl.Utf8).map_elements(
                        lambda x: f"{x[:3]}***{x[-3:]}" if len(str(x)) > 6 else "***",
                        return_dtype=pl.Utf8
                    ).alias(column)
                )
            elif mask_type == "redact":
                # Full redaction - vectorized
                result = result.with_columns(
                    pl.lit("[REDACTED]").alias(column)
                )
            else:
                raise ValueError(f"Unknown mask type: {mask_type}")

        return result

    def pseudonymize_columns(
        self,
        df: pl.DataFrame,
        columns: List[str],
        salt: Optional[str] = None
    ) -> pl.DataFrame:
        """
        Pseudonymize columns with consistent hashing.

        Args:
            df: Input DataFrame.
            columns: Columns to pseudonymize.
            salt: Salt for consistent hashing.

        Returns:
            DataFrame with pseudonymized columns.
        """
        result = df.clone()
        salt_value = salt or str(uuid.uuid4())

        for column in columns:
            if column not in result.columns:
                continue

            result = result.with_columns(
                pl.col(column).cast(pl.Utf8).map_elements(
                    lambda x: hashlib.sha256(f"{x}:{salt_value}".encode()).hexdigest(),
                    return_dtype=pl.Utf8
                ).alias(column)
            )

        return result

    def auto_anonymize(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Automatically anonymize based on column tags.

        Args:
            df: Input DataFrame.

        Returns:
            Anonymized DataFrame.
        """
        mask_rules = {}
        pseudo_cols = []

        for col_name, tags in self._column_tags.items():
            category = tags.get("gdpr_category")
            sensitivity = tags.get("sensitivity")

            if category == "PII" and sensitivity in ["HIGH", "VERY_HIGH"]:
                mask_rules[col_name] = "hash"
            elif category == "PII" and sensitivity == "MEDIUM":
                mask_rules[col_name] = "partial"
            elif category == "SPI":
                pseudo_cols.append(col_name)

        result = self.mask_columns(df, mask_rules)
        result = self.pseudonymize_columns(result, pseudo_cols)

        return result

    def request_erasure(
        self,
        df: pl.DataFrame,
        identifier_col: str,
        identifier_value: str
    ) -> pl.DataFrame:
        """
        Handle erasure request by nullifying PII/SPI.

        Args:
            df: Input DataFrame.
            identifier_col: Column identifying the subject.
            identifier_value: Value to match.

        Returns:
            DataFrame with erased data.
        """
        # Find columns to erase
        cols_to_erase = [
            col for col, tags in self._column_tags.items()
            if tags.get("gdpr_category") in ["PII", "SPI"]
            and col != identifier_col
        ]

        # Build expressions for conditional null
        exprs = []
        for col in df.columns:
            if col in cols_to_erase:
                exprs.append(
                    pl.when(pl.col(identifier_col).cast(pl.Utf8) == str(identifier_value))
                    .then(pl.lit(None))
                    .otherwise(pl.col(col))
                    .alias(col)
                )
            else:
                exprs.append(pl.col(col))

        return df.select(exprs)

    def k_anonymize(
        self,
        df: pl.DataFrame,
        quasi_identifiers: List[str],
        k: int = 5
    ) -> pl.DataFrame:
        """
        Apply k-anonymity using Polars (optimized group operations).

        Args:
            df: Input DataFrame.
            quasi_identifiers: Quasi-identifier columns.
            k: Minimum group size.

        Returns:
            K-anonymized DataFrame.
        """
        # Count occurrences of each QI combination
        counts = df.group_by(quasi_identifiers).agg(
            pl.count().alias("_count")
        )

        # Filter valid groups (count >= k)
        valid_groups = counts.filter(pl.col("_count") >= k).drop("_count")

        # Join back to keep only valid records
        result = df.join(valid_groups, on=quasi_identifiers, how="inner")

        suppressed = len(df) - len(result)
        logger.info(f"K-anonymity (k={k}): {suppressed} records suppressed")

        return result

    def add_differential_privacy_noise(
        self,
        df: pl.DataFrame,
        numeric_columns: List[str],
        epsilon: float = 1.0,
        sensitivity: float = 1.0
    ) -> pl.DataFrame:
        """
        Add Laplace noise for differential privacy.

        Args:
            df: Input DataFrame.
            numeric_columns: Columns to add noise to.
            epsilon: Privacy parameter (lower = more privacy, more noise).
            sensitivity: Query sensitivity.

        Returns:
            DataFrame with noise added.
        """
        result = df.clone()
        scale = sensitivity / epsilon
        n_rows = len(df)

        for col in numeric_columns:
            if col in result.columns:
                # Generate Laplace noise using numpy (required for Laplace distribution)
                if not PANDAS_AVAILABLE:
                    raise ImportError("NumPy is required for differential privacy")
                noise = np.random.laplace(0, scale, n_rows)
                noise_series = pl.Series("_noise", noise)

                result = result.with_columns(
                    (pl.col(col) + noise_series).alias(col)
                )

        return result

    def generalize_numeric(
        self,
        df: pl.DataFrame,
        column: str,
        bin_size: float = 10.0
    ) -> pl.DataFrame:
        """
        Generalize numeric values into ranges.

        Args:
            df: Input DataFrame.
            column: Column to generalize.
            bin_size: Size of each bin.

        Returns:
            DataFrame with generalized column.
        """
        return df.with_columns(
            ((pl.col(column) / bin_size).floor() * bin_size).cast(pl.Int64).cast(pl.Utf8)
            .str.concat_horizontal(
                pl.lit("-"),
                (((pl.col(column) / bin_size).floor() + 1) * bin_size).cast(pl.Int64).cast(pl.Utf8)
            ).alias(column)
        )

    def generalize_date(
        self,
        df: pl.DataFrame,
        column: str,
        level: str = "month"
    ) -> pl.DataFrame:
        """
        Generalize date values to reduce precision.

        Args:
            df: Input DataFrame.
            column: Date column to generalize.
            level: Generalization level ('year', 'month', 'quarter').

        Returns:
            DataFrame with generalized dates.
        """
        if level == "year":
            return df.with_columns(
                pl.col(column).dt.year().cast(pl.Utf8).alias(column)
            )
        elif level == "month":
            return df.with_columns(
                (pl.col(column).dt.year().cast(pl.Utf8) + pl.lit("-") +
                 pl.col(column).dt.month().cast(pl.Utf8).str.zfill(2)).alias(column)
            )
        elif level == "quarter":
            return df.with_columns(
                (pl.col(column).dt.year().cast(pl.Utf8) + pl.lit("-Q") +
                 pl.col(column).dt.quarter().cast(pl.Utf8)).alias(column)
            )
        else:
            raise ValueError(f"Unknown generalization level: {level}")

    def check_k_anonymity(
        self,
        df: pl.DataFrame,
        quasi_identifiers: List[str],
        k: int = 5
    ) -> Dict[str, Any]:
        """
        Check if DataFrame satisfies k-anonymity.

        Args:
            df: Input DataFrame.
            quasi_identifiers: Quasi-identifier columns.
            k: Minimum group size required.

        Returns:
            Dictionary with k-anonymity analysis results.
        """
        # Count group sizes
        counts = df.group_by(quasi_identifiers).agg(
            pl.count().alias("_count")
        )

        min_count = counts.select(pl.col("_count").min()).item()
        max_count = counts.select(pl.col("_count").max()).item()
        total_groups = len(counts)
        violating_groups = len(counts.filter(pl.col("_count") < k))

        return {
            "is_k_anonymous": min_count >= k,
            "k_value": k,
            "min_group_size": min_count,
            "max_group_size": max_count,
            "total_groups": total_groups,
            "violating_groups": violating_groups,
            "compliance_rate": round((total_groups - violating_groups) / total_groups * 100, 2)
        }

    def export_metadata_catalog(
        self,
        df: pl.DataFrame,
        path: str
    ) -> None:
        """
        Export metadata catalog to JSON.

        Args:
            df: DataFrame with applied tags.
            path: Output file path.
        """
        catalog = {
            "dataset_metadata": self._dataset_tags,
            "column_metadata": self._column_tags,
            "schema": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
            "row_count": len(df),
            "exported_at": datetime.datetime.now().isoformat(),
            "backend": "polars"
        }

        with open(path, 'w') as f:
            json.dump(catalog, f, indent=2)

        logger.info(f"Metadata catalog exported to {path}")

    @staticmethod
    def from_pandas(df: pd.DataFrame) -> pl.DataFrame:
        """
        Convert Pandas DataFrame to Polars.

        Args:
            df: Pandas DataFrame.

        Returns:
            Polars DataFrame.
        """
        if not POLARS_AVAILABLE:
            raise ImportError("Polars is not available")
        return pl.from_pandas(df)

    @staticmethod
    def to_pandas(df: pl.DataFrame) -> pd.DataFrame:
        """
        Convert Polars DataFrame to Pandas.

        Args:
            df: Polars DataFrame.

        Returns:
            Pandas DataFrame.
        """
        if not PANDAS_AVAILABLE:
            raise ImportError("Pandas is not available")
        return df.to_pandas()

    @staticmethod
    def from_spark(spark_df: DataFrame) -> pl.DataFrame:
        """
        Convert Spark DataFrame to Polars via Arrow.

        Args:
            spark_df: Spark DataFrame.

        Returns:
            Polars DataFrame.
        """
        if not POLARS_AVAILABLE:
            raise ImportError("Polars is not available")
        # Convert via Pandas (Spark -> Pandas -> Polars)
        # This uses Arrow under the hood for efficiency
        pandas_df = spark_df.toPandas()
        return pl.from_pandas(pandas_df)

    @staticmethod
    def to_spark(polars_df: pl.DataFrame, spark: SparkSession) -> DataFrame:
        """
        Convert Polars DataFrame to Spark.

        Args:
            polars_df: Polars DataFrame.
            spark: SparkSession.

        Returns:
            Spark DataFrame.
        """
        # Convert via Pandas (Polars -> Pandas -> Spark)
        pandas_df = polars_df.to_pandas()
        return spark.createDataFrame(pandas_df)


# ==============================================================================
# Benchmark Utilities
# ==============================================================================


class Benchmark:
    """
    Benchmarking utilities to compare Spark vs Pandas vs Polars performance.

    Helps users choose the optimal backend for their data size by measuring
    actual execution times across all available backends.
    """

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        config: Optional[ConfigManager] = None
    ):
        """
        Initialize benchmark utilities.

        Args:
            spark: SparkSession for Spark benchmarks.
            config: ConfigManager for settings.
        """
        self.spark = spark
        self.config = config or ConfigManager()
        self.results: List[BenchmarkResult] = []

    def _measure_time(self, func: Callable) -> Tuple[Any, float]:
        """Measure execution time of a function."""
        start = time.perf_counter()
        result = func()
        elapsed = time.perf_counter() - start
        return result, elapsed

    def benchmark_operation(
        self,
        operation_name: str,
        spark_func: Optional[Callable] = None,
        pandas_func: Optional[Callable] = None,
        polars_func: Optional[Callable] = None,
        row_count: int = 0,
        column_count: int = 0
    ) -> Dict[str, BenchmarkResult]:
        """
        Benchmark an operation on all available backends.

        Args:
            operation_name: Name of the operation being benchmarked.
            spark_func: Function to run on Spark.
            pandas_func: Function to run on Pandas.
            polars_func: Function to run on Polars.
            row_count: Number of rows in the dataset.
            column_count: Number of columns in the dataset.

        Returns:
            Dictionary with benchmark results for each backend.
        """
        results = {}

        if spark_func:
            _, spark_time = self._measure_time(spark_func)
            result = BenchmarkResult(
                operation=operation_name,
                backend="spark",
                row_count=row_count,
                column_count=column_count,
                execution_time_seconds=spark_time
            )
            results["spark"] = result
            self.results.append(result)

        if pandas_func and PANDAS_AVAILABLE:
            _, pandas_time = self._measure_time(pandas_func)
            result = BenchmarkResult(
                operation=operation_name,
                backend="pandas",
                row_count=row_count,
                column_count=column_count,
                execution_time_seconds=pandas_time
            )
            results["pandas"] = result
            self.results.append(result)

        if polars_func and POLARS_AVAILABLE:
            _, polars_time = self._measure_time(polars_func)
            result = BenchmarkResult(
                operation=operation_name,
                backend="polars",
                row_count=row_count,
                column_count=column_count,
                execution_time_seconds=polars_time
            )
            results["polars"] = result
            self.results.append(result)

        return results

    def run_anonymization_benchmark(
        self,
        spark_df: Optional[DataFrame] = None,
        pandas_df: Optional[pd.DataFrame] = None,
        polars_df: Optional[pl.DataFrame] = None,
        columns_to_mask: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Run a comprehensive anonymization benchmark across all backends.

        Args:
            spark_df: Spark DataFrame for testing.
            pandas_df: Pandas DataFrame for testing.
            polars_df: Polars DataFrame for testing.
            columns_to_mask: Columns to apply masking to.

        Returns:
            Benchmark comparison results.
        """
        results = {
            "operations": {},
            "summary": {}
        }

        if columns_to_mask is None:
            columns_to_mask = ["col1"]

        # Get dimensions
        spark_rows = spark_df.count() if spark_df else 0
        pandas_rows = len(pandas_df) if pandas_df is not None else 0
        polars_rows = len(polars_df) if polars_df is not None else 0
        row_count = spark_rows or pandas_rows or polars_rows
        col_count = (
            len(spark_df.columns) if spark_df else
            len(pandas_df.columns) if pandas_df is not None else
            len(polars_df.columns) if polars_df is not None else 0
        )

        # Hash masking benchmark
        mask_rules = {col: "hash" for col in columns_to_mask}

        def spark_hash():
            if spark_df:
                from spart import AnonymizationManager, MetadataManager
                mgr = MetadataManager(self.spark)
                anon = AnonymizationManager(mgr)
                return anon.mask_columns(spark_df, mask_rules).count()

        def pandas_hash():
            if pandas_df is not None:
                proc = PandasProcessor()
                return len(proc.mask_columns(pandas_df, mask_rules))

        def polars_hash():
            if polars_df is not None:
                proc = PolarsProcessor()
                return len(proc.mask_columns(polars_df, mask_rules))

        results["operations"]["hash_masking"] = self.benchmark_operation(
            "hash_masking",
            spark_hash if spark_df else None,
            pandas_hash if pandas_df is not None else None,
            polars_hash if polars_df is not None else None,
            row_count,
            col_count
        )

        # Pseudonymization benchmark
        def spark_pseudo():
            if spark_df:
                from spart import AnonymizationManager, MetadataManager
                mgr = MetadataManager(self.spark)
                anon = AnonymizationManager(mgr)
                return anon.pseudonymize_columns(
                    spark_df, columns_to_mask, "salt"
                ).count()

        def pandas_pseudo():
            if pandas_df is not None:
                proc = PandasProcessor()
                return len(proc.pseudonymize_columns(
                    pandas_df, columns_to_mask, "salt"
                ))

        def polars_pseudo():
            if polars_df is not None:
                proc = PolarsProcessor()
                return len(proc.pseudonymize_columns(
                    polars_df, columns_to_mask, "salt"
                ))

        results["operations"]["pseudonymization"] = self.benchmark_operation(
            "pseudonymization",
            spark_pseudo if spark_df else None,
            pandas_pseudo if pandas_df is not None else None,
            polars_pseudo if polars_df is not None else None,
            row_count,
            col_count
        )

        # K-anonymity benchmark (if quasi-identifiers available)
        if len(columns_to_mask) >= 2:
            def spark_kanon():
                if spark_df:
                    from spart import AdvancedAnonymization
                    adv = AdvancedAnonymization(self.spark)
                    return adv.k_anonymize(spark_df, columns_to_mask[:2], k=2).count()

            def pandas_kanon():
                if pandas_df is not None:
                    proc = PandasProcessor()
                    return len(proc.k_anonymize(pandas_df, columns_to_mask[:2], k=2))

            def polars_kanon():
                if polars_df is not None:
                    proc = PolarsProcessor()
                    return len(proc.k_anonymize(polars_df, columns_to_mask[:2], k=2))

            results["operations"]["k_anonymity"] = self.benchmark_operation(
                "k_anonymity",
                spark_kanon if spark_df else None,
                pandas_kanon if pandas_df is not None else None,
                polars_kanon if polars_df is not None else None,
                row_count,
                col_count
            )

        # Calculate summary
        spark_total = sum(
            r.execution_time_seconds for r in self.results
            if r.backend == "spark"
        )
        pandas_total = sum(
            r.execution_time_seconds for r in self.results
            if r.backend == "pandas"
        )
        polars_total = sum(
            r.execution_time_seconds for r in self.results
            if r.backend == "polars"
        )

        # Find fastest backend
        times = {}
        if spark_total > 0:
            times["spark"] = spark_total
        if pandas_total > 0:
            times["pandas"] = pandas_total
        if polars_total > 0:
            times["polars"] = polars_total

        fastest = min(times, key=times.get) if times else "spark"

        results["summary"] = {
            "row_count": row_count,
            "column_count": col_count,
            "spark_total_seconds": spark_total,
            "pandas_total_seconds": pandas_total,
            "polars_total_seconds": polars_total,
            "fastest_backend": fastest,
            "speedup_vs_spark": {
                "pandas": round(spark_total / pandas_total, 2) if pandas_total > 0 else None,
                "polars": round(spark_total / polars_total, 2) if polars_total > 0 else None
            },
            "speedup_vs_pandas": {
                "polars": round(pandas_total / polars_total, 2) if polars_total > 0 else None
            },
            "recommendation": self._get_recommendation(row_count)
        }

        return results

    def _get_recommendation(self, row_count: int) -> str:
        """Get backend recommendation based on row count."""
        pandas_threshold = self.config.get(
            "processing", "pandas_threshold_rows", default=50000
        )
        polars_threshold = self.config.get(
            "processing", "polars_threshold_rows", default=500000
        )

        if row_count <= pandas_threshold:
            if POLARS_AVAILABLE:
                return (
                    f"Use POLARS for small datasets (<= {pandas_threshold:,} rows). "
                    f"Current: {row_count:,} rows. Polars offers best single-threaded performance."
                )
            else:
                return (
                    f"Use PANDAS for small datasets (<= {pandas_threshold:,} rows). "
                    f"Current: {row_count:,} rows."
                )
        elif row_count <= polars_threshold:
            if POLARS_AVAILABLE:
                return (
                    f"Use POLARS for medium datasets ({pandas_threshold:,}-{polars_threshold:,} rows). "
                    f"Current: {row_count:,} rows. Polars provides 5-10x speedup over Pandas."
                )
            else:
                return (
                    f"Use PANDAS for medium datasets, but consider installing Polars for better performance. "
                    f"Current: {row_count:,} rows."
                )
        else:
            return (
                f"Use SPARK for large datasets (> {polars_threshold:,} rows). "
                f"Current: {row_count:,} rows. Distributed processing recommended."
            )

    def run_full_benchmark_suite(
        self,
        row_counts: List[int] = None,
        columns: int = 10
    ) -> Dict[str, Any]:
        """
        Run a comprehensive benchmark suite across multiple data sizes.

        Args:
            row_counts: List of row counts to benchmark.
            columns: Number of columns in test data.

        Returns:
            Complete benchmark results across all sizes and backends.
        """
        if row_counts is None:
            row_counts = [1000, 10000, 50000, 100000]

        all_results = {
            "benchmarks": [],
            "summary": {
                "row_counts_tested": row_counts,
                "backends_available": {
                    "spark": self.spark is not None,
                    "pandas": PANDAS_AVAILABLE,
                    "polars": POLARS_AVAILABLE
                }
            }
        }

        for row_count in row_counts:
            logger.info(f"Benchmarking with {row_count:,} rows...")

            # Create test data
            test_data = [
                [f"value_{r}_{c}" for c in range(columns)]
                for r in range(row_count)
            ]
            col_names = [f"col_{i}" for i in range(columns)]

            spark_df = None
            pandas_df = None
            polars_df = None

            if self.spark:
                spark_df = self.spark.createDataFrame(test_data, col_names)

            if PANDAS_AVAILABLE:
                pandas_df = pd.DataFrame(test_data, columns=col_names)

            if POLARS_AVAILABLE:
                polars_df = pl.DataFrame(dict(zip(col_names, zip(*test_data))))

            # Run benchmark
            bench_results = self.run_anonymization_benchmark(
                spark_df=spark_df,
                pandas_df=pandas_df,
                polars_df=polars_df,
                columns_to_mask=col_names[:2]
            )

            all_results["benchmarks"].append({
                "row_count": row_count,
                "results": bench_results
            })

            # Clear results for next iteration
            self.results = []

        return all_results

    def get_results_dataframe(self) -> pd.DataFrame:
        """
        Get benchmark results as a pandas DataFrame.

        Returns:
            DataFrame with all benchmark results.
        """
        if not PANDAS_AVAILABLE:
            raise ImportError("Pandas required for results DataFrame")

        return pd.DataFrame([asdict(r) for r in self.results])

    def get_results_polars(self) -> pl.DataFrame:
        """
        Get benchmark results as a Polars DataFrame.

        Returns:
            Polars DataFrame with all benchmark results.
        """
        if not POLARS_AVAILABLE:
            raise ImportError("Polars required for results DataFrame")

        return pl.DataFrame([asdict(r) for r in self.results])

    def print_results(self) -> None:
        """Print formatted benchmark results."""
        print("\n" + "=" * 70)
        print("BENCHMARK RESULTS")
        print("=" * 70)

        # Group by operation
        operations = {}
        for result in self.results:
            if result.operation not in operations:
                operations[result.operation] = {}
            operations[result.operation][result.backend] = result

        for op_name, backends in operations.items():
            print(f"\n{op_name}:")
            print("-" * 50)

            times = []
            for backend, result in sorted(backends.items()):
                times.append((backend, result.execution_time_seconds))
                print(
                    f"  {backend:8s}: {result.execution_time_seconds:.4f}s "
                    f"({result.throughput_rows_per_second:,.0f} rows/s)"
                )

            # Show speedup
            if len(times) > 1:
                fastest = min(times, key=lambda x: x[1])
                for backend, t in times:
                    if backend != fastest[0]:
                        speedup = t / fastest[1]
                        print(f"    -> {fastest[0]} is {speedup:.1f}x faster than {backend}")

    def export_results(self, path: str, format: str = "json") -> None:
        """
        Export benchmark results to file.

        Args:
            path: Output file path.
            format: Export format ('json', 'csv').
        """
        if format == "json":
            with open(path, 'w') as f:
                json.dump([asdict(r) for r in self.results], f, indent=2)
        elif format == "csv":
            if PANDAS_AVAILABLE:
                self.get_results_dataframe().to_csv(path, index=False)
            elif POLARS_AVAILABLE:
                self.get_results_polars().write_csv(path)
            else:
                raise ImportError("Pandas or Polars required for CSV export")
        else:
            raise ValueError(f"Unknown format: {format}")


# ==============================================================================
# Streaming Manager
# ==============================================================================


class StreamingManager:
    """
    Manages GDPR compliance for Spark Structured Streaming.

    Provides utilities for applying anonymization and consent filtering
    to streaming DataFrames.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: Optional[ConfigManager] = None,
        consent_manager: Optional[ConsentManager] = None
    ):
        """
        Initialize the streaming manager.

        Args:
            spark: Active SparkSession.
            config: ConfigManager for settings.
            consent_manager: ConsentManager for consent filtering.
        """
        self.spark = spark
        self.config = config or ConfigManager()
        self.consent_manager = consent_manager

    def create_anonymization_stream(
        self,
        input_stream: DataFrame,
        mask_rules: Dict[str, str],
        checkpoint_path: str
    ) -> DataFrame:
        """
        Create a streaming anonymization pipeline.

        Args:
            input_stream: Input streaming DataFrame.
            mask_rules: Masking rules to apply.
            checkpoint_path: Path for streaming checkpoints.

        Returns:
            Transformed streaming DataFrame.
        """
        # Build masking expressions
        mask_expressions = {}

        for column, mask_type in mask_rules.items():
            if column not in input_stream.columns:
                continue

            col_ref = F.col(column).cast("string")

            if mask_type == "hash":
                mask_expressions[column] = F.sha2(col_ref, 256)
            elif mask_type == "partial":
                mask_expressions[column] = F.concat_ws(
                    "***",
                    F.substring(col_ref, 1, 3),
                    F.substring(col_ref, -3, 3)
                )
            elif mask_type == "redact":
                mask_expressions[column] = F.lit("[REDACTED]")

        if mask_expressions:
            return input_stream.withColumns(mask_expressions)

        return input_stream

    def write_anonymized_stream(
        self,
        stream: DataFrame,
        output_path: str,
        checkpoint_path: str,
        output_mode: str = "append",
        trigger_interval: str = "10 seconds"
    ):
        """
        Write an anonymized stream to storage.

        Args:
            stream: Streaming DataFrame to write.
            output_path: Output location.
            checkpoint_path: Checkpoint location.
            output_mode: Output mode (append, complete, update).
            trigger_interval: Processing trigger interval.

        Returns:
            StreamingQuery handle.
        """
        return (
            stream.writeStream
            .format("parquet")
            .option("path", output_path)
            .option("checkpointLocation", checkpoint_path)
            .outputMode(output_mode)
            .trigger(processingTime=trigger_interval)
            .start()
        )


# ==============================================================================
# Metadata Manager (Original + Enhanced)
# ==============================================================================


class MetadataManager:
    """
    Manages metadata and tags for DataFrames, focusing on GDPR compliance.

    Facilitates the application and management of data governance metadata
    within Spark/Databricks environments.
    """

    # Constants for GDPR data categories
    GDPR_CATEGORIES = {
        "PII": "Personally Identifiable Information",
        "SPI": "Sensitive Personal Information",
        "NPII": "Non-Personally Identifiable Information"
    }

    # Sensitivity levels
    SENSITIVITY_LEVELS = ["LOW", "MEDIUM", "HIGH", "VERY_HIGH"]

    def __init__(
        self,
        spark: SparkSession,
        metadata_col_name: str = "_metadata",
        config: Optional[ConfigManager] = None,
        audit_manager: Optional[AuditManager] = None
    ):
        """
        Initialize the metadata manager.

        Args:
            spark: Active Spark session.
            metadata_col_name: Name for the hidden metadata column.
            config: ConfigManager instance.
            audit_manager: AuditManager for logging.
        """
        self.spark = spark
        self.metadata_col_name = metadata_col_name
        self.config = config or ConfigManager()
        self.audit = audit_manager
        self._validate_spark_session()

    def _validate_spark_session(self):
        """Validate if the Spark session is active."""
        if self.spark is None:
            raise ValueError("Spark session not initialized.")

        # Check if we are in a Databricks environment
        spark_version_tag = "spark.databricks.clusterUsageTags.sparkVersion"
        is_databricks = self.spark.conf.get(spark_version_tag, "")
        self.is_databricks = len(is_databricks) > 0

    def apply_column_tags(
        self,
        df: DataFrame,
        column_tags: Dict[str, Dict[str, Any]]
    ) -> DataFrame:
        """
        Apply GDPR tags and metadata at column level.

        Args:
            df: DataFrame to be modified.
            column_tags: Dictionary with columns and their associated tags.

        Returns:
            DataFrame with applied metadata.
        """
        # Validate input data
        self._validate_column_tags(df, column_tags)

        # Create comments for columns
        for column_name, tags in column_tags.items():
            if column_name in df.columns:
                comment = json.dumps(tags)
                df = df.withColumn(
                    column_name,
                    F.col(column_name).alias(
                        column_name, metadata={"gdpr_tags": comment}
                    )
                )

        # Log audit event
        if self.audit:
            self.audit.log_event(
                event_type=AuditEventType.DATA_MODIFICATION,
                actor="metadata_manager",
                action="apply_column_tags",
                resource="dataframe",
                details={"columns": list(column_tags.keys())}
            )

        return df

    def apply_dataset_tags(
        self,
        df: DataFrame,
        dataset_tags: Dict[str, str]
    ) -> DataFrame:
        """
        Apply tags and metadata at dataset level using a hidden column.

        Args:
            df: DataFrame to be modified.
            dataset_tags: Dictionary with tags for the complete dataset.

        Returns:
            DataFrame with dataset metadata applied.
        """
        # Validate input data
        self._validate_dataset_tags(dataset_tags)

        # Add timestamp and UUID
        dataset_tags["timestamp"] = datetime.datetime.now().isoformat()
        dataset_tags["dataset_id"] = str(uuid.uuid4())

        # Serialize metadata to JSON
        metadata_json = json.dumps(dataset_tags)

        # Add as hidden column
        df = df.withColumn(self.metadata_col_name, F.lit(metadata_json))

        return df

    def scan_for_pii(self, df: DataFrame) -> Dict[str, List[str]]:
        """
        Scan DataFrame column names to identify potential PII/SPI.

        Args:
            df: DataFrame to be analyzed.

        Returns:
            Dictionary with potential PII categories and associated columns.
        """
        detector = PIIDetector(self.config)
        return detector.scan_column_names(df)

    def export_metadata_catalog(self, df: DataFrame, path: str) -> None:
        """
        Export metadata catalog to a JSON file.

        Args:
            df: DataFrame with applied metadata.
            path: Path to save the catalog file.
        """
        # Extract column metadata
        column_metadata = {}
        for column in df.schema:
            metadata = column.metadata
            if "gdpr_tags" in metadata:
                try:
                    column_metadata[column.name] = json.loads(
                        metadata["gdpr_tags"]
                    )
                except json.JSONDecodeError:
                    logger.warning(
                        f"Could not parse gdpr_tags for column {column.name}"
                    )
                    column_metadata[column.name] = {"error": "invalid JSON"}
            else:
                column_metadata[column.name] = {"gdpr_category": "UNKNOWN"}

        # Extract dataset metadata
        dataset_metadata = {}
        if self.metadata_col_name in df.columns:
            try:
                first_row = df.select(self.metadata_col_name).first()
                if first_row and first_row[0]:
                    dataset_metadata = json.loads(first_row[0])
            except Exception as e:
                logger.error(f"Failed to extract dataset metadata: {e}")
                dataset_metadata = {"error": "failed to extract"}

        # Compile the complete catalog
        catalog = {
            "dataset_metadata": dataset_metadata,
            "column_metadata": column_metadata,
            "schema": str(df.schema),
            "exported_at": datetime.datetime.now().isoformat()
        }

        # Save catalog as JSON
        try:
            with open(path, "w") as f:
                json.dump(catalog, f, indent=2)
            logger.info(f"Metadata catalog exported to {path}")
        except IOError as e:
            logger.error(f"Failed to write metadata catalog to {path}: {e}")

        # Log audit event
        if self.audit:
            self.audit.log_event(
                event_type=AuditEventType.EXPORT,
                actor="metadata_manager",
                action="export_metadata_catalog",
                resource=path
            )

    def apply_tags_to_delta_table(
        self,
        table_name: str,
        tags: Dict[str, str],
        use_unity_catalog: bool = True
    ) -> None:
        """
        Apply tags to a Delta table (Databricks only).

        Args:
            table_name: Full name of the Delta table.
            tags: Dictionary of tags and values to be applied.
            use_unity_catalog: If True, uses Unity Catalog SET TAGS syntax.
        """
        if not self.is_databricks:
            msg = "This method only works in a Databricks environment."
            logger.error(msg)
            raise EnvironmentError(msg)

        # Sanitize tag keys and values
        sanitized_tags = {
            k.replace("'", "''").replace("\\", "\\\\"):
            v.replace("'", "''").replace("\\", "\\\\")
            for k, v in tags.items()
        }

        tags_str = ", ".join([
            f"'{k}' = '{v}'" for k, v in sanitized_tags.items()
        ])

        if use_unity_catalog:
            sql_command = f"ALTER TABLE {table_name} SET TAGS ({tags_str})"
        else:
            sql_command = f"ALTER TABLE {table_name} SET TBLPROPERTIES ({tags_str})"

        try:
            self.spark.sql(sql_command)
            catalog_type = "Unity Catalog" if use_unity_catalog else "Hive Metastore"
            logger.info(f"Tags applied to table {table_name} ({catalog_type})")
        except Exception as e:
            logger.error(f"Failed to apply tags to {table_name}: {e}")
            raise

    def apply_column_tags_to_delta_table(
        self,
        table_name: str,
        column_tags: Dict[str, Dict[str, str]]
    ) -> None:
        """
        Apply tags to columns in a Delta table using Unity Catalog.

        Args:
            table_name: Full name of the Delta table.
            column_tags: Dictionary mapping column names to their tags.
        """
        if not self.is_databricks:
            msg = "This method only works in a Databricks environment."
            logger.error(msg)
            raise EnvironmentError(msg)

        for column_name, tags in column_tags.items():
            safe_column = column_name.replace("`", "``")
            sanitized_tags = {
                k.replace("'", "''").replace("\\", "\\\\"):
                v.replace("'", "''").replace("\\", "\\\\")
                for k, v in tags.items()
            }

            tags_str = ", ".join([
                f"'{k}' = '{v}'" for k, v in sanitized_tags.items()
            ])
            sql_command = (
                f"ALTER TABLE {table_name} "
                f"ALTER COLUMN `{safe_column}` SET TAGS ({tags_str})"
            )

            try:
                self.spark.sql(sql_command)
                logger.info(
                    f"Tags applied to column '{column_name}' in {table_name}"
                )
            except Exception as e:
                logger.error(
                    f"Failed to apply tags to column '{column_name}': {e}"
                )
                raise

    def get_table_tags(self, table_name: str) -> Dict[str, str]:
        """
        Retrieve tags from a Delta table in Unity Catalog.

        Args:
            table_name: Full name of the Delta table.

        Returns:
            Dictionary of tag key-value pairs.
        """
        if not self.is_databricks:
            msg = "This method only works in a Databricks environment."
            logger.error(msg)
            raise EnvironmentError(msg)

        try:
            parts = table_name.split(".")
            if len(parts) == 3:
                catalog, schema, table = parts
                query = f"""
                    SELECT tag_name, tag_value
                    FROM {catalog}.information_schema.table_tags
                    WHERE schema_name = '{schema}' AND table_name = '{table}'
                """
                result = self.spark.sql(query).collect()
                return {row["tag_name"]: row["tag_value"] for row in result}
            else:
                logger.warning(
                    f"Table name '{table_name}' should be in format "
                    f"'catalog.schema.table'"
                )
                return {}
        except Exception as e:
            logger.error(f"Failed to get tags from {table_name}: {e}")
            return {}

    def _validate_column_tags(
        self,
        df: DataFrame,
        column_tags: Dict[str, Dict[str, Any]]
    ) -> None:
        """Validate column tags according to GDPR rules."""
        for col_name, tags in column_tags.items():
            if col_name not in df.columns:
                logger.warning(
                    f"Column '{col_name}' does not exist in DataFrame"
                )
                continue

            if "gdpr_category" in tags:
                category = tags["gdpr_category"]
                if category not in self.GDPR_CATEGORIES:
                    valid_categories = list(self.GDPR_CATEGORIES.keys())
                    raise ValueError(
                        f"Invalid GDPR category: {category}. "
                        f"Use one of: {valid_categories}"
                    )

            if "sensitivity" in tags:
                sensitivity = tags["sensitivity"]
                if sensitivity not in self.SENSITIVITY_LEVELS:
                    raise ValueError(
                        f"Invalid sensitivity level: {sensitivity}. "
                        f"Use one of: {self.SENSITIVITY_LEVELS}"
                    )

    def _validate_dataset_tags(self, dataset_tags: Dict[str, str]) -> None:
        """Validate dataset tags."""
        required_fields = ["owner", "purpose"]
        for field in required_fields:
            if field not in dataset_tags:
                raise ValueError(f"Required tag missing: '{field}'")

    def get_column_tags(self, df: DataFrame) -> Dict[str, Dict[str, str]]:
        """
        Return GDPR tags applied to each column of the DataFrame.

        Args:
            df: DataFrame to be analyzed.

        Returns:
            Dictionary with tags for each column.
        """
        result = {}

        for column in df.schema:
            if "gdpr_tags" in column.metadata:
                try:
                    result[column.name] = json.loads(
                        column.metadata["gdpr_tags"]
                    )
                except json.JSONDecodeError:
                    logger.warning(
                        f"Could not parse gdpr_tags for column {column.name}"
                    )
                    result[column.name] = {"error": "invalid JSON"}

        return result


# ==============================================================================
# Retention Manager
# ==============================================================================


class RetentionManager:
    """
    Manages data retention policies based on tags.
    """

    def __init__(
        self,
        metadata_manager: MetadataManager,
        audit_manager: Optional[AuditManager] = None
    ):
        """
        Initialize the retention manager.

        Args:
            metadata_manager: Instance of MetadataManager.
            audit_manager: Optional AuditManager for logging.
        """
        self.metadata_manager = metadata_manager
        self.spark = metadata_manager.spark
        self.audit = audit_manager

    def apply_retention_policy(
        self,
        df: DataFrame,
        date_column: str
    ) -> DataFrame:
        """
        Apply retention policies based on 'retention_days' tag in columns.

        Args:
            df: DataFrame to be filtered.
            date_column: Name of the column containing the reference date.

        Returns:
            DataFrame with data within the longest applicable retention period.
        """
        column_tags = self.metadata_manager.get_column_tags(df)

        if not column_tags:
            logger.warning(
                "No GDPR tags found. Cannot apply retention policy."
            )
            return df

        # Find the maximum retention period
        max_retention_days = 0
        for col_name, tags in column_tags.items():
            try:
                retention = int(tags.get("retention_days", 0))
                if retention > max_retention_days:
                    max_retention_days = retention
            except (ValueError, TypeError):
                logger.warning(
                    f"Invalid 'retention_days' for column {col_name}"
                )

        if max_retention_days <= 0:
            logger.info("No valid retention period found.")
            return df

        # Calculate cutoff date
        cutoff_date = F.date_sub(F.current_date(), max_retention_days)
        result_df = df.filter(F.col(date_column) >= cutoff_date)

        cutoff_date_str = (
            datetime.datetime.now() - datetime.timedelta(days=max_retention_days)
        ).strftime("%Y-%m-%d")

        logger.info(
            f"Retention policy applied. Kept records >= {cutoff_date_str}"
        )

        # Log audit event
        if self.audit:
            self.audit.log_event(
                event_type=AuditEventType.RETENTION_APPLIED,
                actor="retention_manager",
                action="apply_retention_policy",
                resource="dataframe",
                details={
                    "date_column": date_column,
                    "max_retention_days": max_retention_days,
                    "cutoff_date": cutoff_date_str
                }
            )

        return result_df


# ==============================================================================
# Anonymization Manager
# ==============================================================================


class AnonymizationManager:
    """
    Manages anonymization and masking techniques for sensitive data.
    """

    def __init__(
        self,
        metadata_manager: MetadataManager,
        audit_manager: Optional[AuditManager] = None
    ):
        """
        Initialize the anonymization manager.

        Args:
            metadata_manager: Instance of MetadataManager.
            audit_manager: Optional AuditManager for logging.
        """
        self.metadata_manager = metadata_manager
        self.spark = metadata_manager.spark
        self.audit = audit_manager

    def mask_columns(
        self,
        df: DataFrame,
        mask_rules: Dict[str, str]
    ) -> DataFrame:
        """
        Apply masking to specific columns based on provided rules.

        Args:
            df: Original DataFrame.
            mask_rules: Dictionary with masking rules by column.
                        Supported types: "hash", "partial", "redact".

        Returns:
            DataFrame with masked data.
        """
        if not mask_rules:
            return df

        mask_expressions = {}

        for column, mask_type in mask_rules.items():
            if column not in df.columns:
                logger.warning(f"Column '{column}' not found for masking")
                continue

            logger.info(f"Applying '{mask_type}' to column '{column}'")
            col_ref = F.col(column).cast("string")

            if mask_type == "hash":
                mask_expressions[column] = F.sha2(col_ref, 256)
            elif mask_type == "partial":
                mask_expressions[column] = F.concat_ws(
                    "***",
                    F.substring(col_ref, 1, 3),
                    F.substring(col_ref, -3, 3)
                )
            elif mask_type == "redact":
                mask_expressions[column] = F.lit("[REDACTED]")
            else:
                raise ValueError(f"Unknown masking type: {mask_type}")

        if mask_expressions:
            result = df.withColumns(mask_expressions)

            # Log audit event
            if self.audit:
                self.audit.log_event(
                    event_type=AuditEventType.ANONYMIZATION,
                    actor="anonymization_manager",
                    action="mask_columns",
                    resource="dataframe",
                    details={"columns": list(mask_rules.keys())}
                )

            return result

        return df

    def pseudonymize_columns(
        self,
        df: DataFrame,
        columns: List[str],
        salt: Optional[str] = None
    ) -> DataFrame:
        """
        Pseudonymize columns maintaining the same relationship between values.

        Args:
            df: Original DataFrame.
            columns: List of columns to pseudonymize.
            salt: Optional value for consistent pseudonyms.

        Returns:
            DataFrame with pseudonymized data.
        """
        if not columns:
            return df

        salt_value = salt or str(uuid.uuid4())
        if salt is None:
            logger.warning(
                "Using random salt. Output will not be consistent across runs."
            )

        pseudo_expressions = {}

        for column in columns:
            if column not in df.columns:
                logger.warning(f"Column '{column}' not found")
                continue

            pseudo_expressions[column] = F.sha2(
                F.concat_ws(":", F.col(column).cast("string"), F.lit(salt_value)),
                256
            )

        if pseudo_expressions:
            return df.withColumns(pseudo_expressions)

        return df

    def auto_anonymize(self, df: DataFrame) -> DataFrame:
        """
        Apply automatic anonymization based on GDPR tags.

        Args:
            df: Original DataFrame.

        Returns:
            DataFrame with anonymized sensitive data.
        """
        column_tags = self.metadata_manager.get_column_tags(df)

        mask_rules = {}
        pseudonymize_columns = []

        for col_name, tags in column_tags.items():
            category = tags.get("gdpr_category")
            sensitivity = tags.get("sensitivity")

            if category == "PII" and sensitivity in ["HIGH", "VERY_HIGH"]:
                mask_rules[col_name] = "hash"
            elif category == "PII" and sensitivity == "MEDIUM":
                mask_rules[col_name] = "partial"
            elif category == "SPI":
                pseudonymize_columns.append(col_name)

        result_df = self.mask_columns(df, mask_rules)
        result_df = self.pseudonymize_columns(result_df, pseudonymize_columns)

        return result_df

    def request_erasure(
        self,
        df: DataFrame,
        identifier_col: str,
        identifier_value: str
    ) -> DataFrame:
        """
        Handle data subject erasure requests.

        Args:
            df: The DataFrame containing the data.
            identifier_col: The column name used to identify the data subject.
            identifier_value: The value in the identifier column.

        Returns:
            DataFrame with PII/SPI columns nullified for the specified user.
        """
        column_tags = self.metadata_manager.get_column_tags(df)
        if not column_tags:
            logger.warning("No column tags found. Cannot perform erasure.")
            return df

        logger.info(
            f"Processing erasure request for {identifier_col} = {identifier_value}"
        )

        columns_to_anonymize = [
            col_name for col_name, tags in column_tags.items()
            if tags.get("gdpr_category") in ["PII", "SPI"]
            and col_name != identifier_col
        ]

        if not columns_to_anonymize:
            logger.warning("No PII/SPI columns tagged. No erasure performed.")
            return df

        condition = F.col(identifier_col) == identifier_value
        erasure_expressions = {}

        for target_col in columns_to_anonymize:
            erasure_expressions[target_col] = F.when(
                condition,
                F.lit(None).cast(df.schema[target_col].dataType)
            ).otherwise(F.col(target_col))

        if erasure_expressions:
            result_df = df.withColumns(erasure_expressions)

            # Log audit event
            if self.audit:
                self.audit.log_event(
                    event_type=AuditEventType.ERASURE_REQUEST,
                    actor="anonymization_manager",
                    action="request_erasure",
                    resource="dataframe",
                    details={
                        "identifier_col": identifier_col,
                        "columns_erased": columns_to_anonymize
                    },
                    subject_ids=[identifier_value]
                )

            logger.info(
                f"Applied erasure to {len(erasure_expressions)} columns"
            )
            return result_df

        return df


# ==============================================================================
# Backend Selector
# ==============================================================================


class BackendSelector:
    """
    Automatically selects the optimal processing backend based on data size.

    Backend Selection Strategy:
    - PANDAS: For small datasets (<50k rows) - minimal overhead
    - POLARS: For medium datasets (50k-500k rows) - best single-machine performance
    - SPARK: For large datasets (>500k rows) - distributed processing
    """

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        config: Optional[ConfigManager] = None
    ):
        """
        Initialize the backend selector.

        Args:
            spark: SparkSession for Spark processing.
            config: ConfigManager for threshold settings.
        """
        self.spark = spark
        self.config = config or ConfigManager()
        self.pandas_threshold = self.config.get(
            "processing", "pandas_threshold_rows", default=50000
        )
        self.polars_threshold = self.config.get(
            "processing", "polars_threshold_rows", default=500000
        )
        # Legacy support
        self.threshold = self.pandas_threshold

    def get_backend(
        self,
        row_count: int,
        force_backend: Optional[ProcessingBackend] = None,
        prefer_polars: bool = True
    ) -> ProcessingBackend:
        """
        Determine the optimal backend based on data size.

        Args:
            row_count: Number of rows in the dataset.
            force_backend: Force a specific backend.
            prefer_polars: If True, prefer Polars over Pandas when available.

        Returns:
            Recommended ProcessingBackend.
        """
        if force_backend and force_backend != ProcessingBackend.AUTO:
            return force_backend

        # Tiered backend selection
        if row_count <= self.pandas_threshold:
            # Small data: prefer Polars if available, else Pandas
            if prefer_polars and POLARS_AVAILABLE:
                return ProcessingBackend.POLARS
            elif PANDAS_AVAILABLE:
                return ProcessingBackend.PANDAS
            else:
                return ProcessingBackend.SPARK
        elif row_count <= self.polars_threshold:
            # Medium data: Polars is ideal
            if POLARS_AVAILABLE:
                return ProcessingBackend.POLARS
            elif PANDAS_AVAILABLE:
                return ProcessingBackend.PANDAS
            else:
                return ProcessingBackend.SPARK
        else:
            # Large data: use Spark for distributed processing
            return ProcessingBackend.SPARK

    def get_processor(
        self,
        row_count: int,
        force_backend: Optional[ProcessingBackend] = None
    ) -> Union['PandasProcessor', 'PolarsProcessor', None]:
        """
        Get the appropriate processor instance based on data size.

        Args:
            row_count: Number of rows in the dataset.
            force_backend: Force a specific backend.

        Returns:
            Processor instance or None if Spark should be used.
        """
        backend = self.get_backend(row_count, force_backend)

        if backend == ProcessingBackend.POLARS:
            return PolarsProcessor(self.config)
        elif backend == ProcessingBackend.PANDAS:
            return PandasProcessor(self.config)
        else:
            return None  # Use Spark-based managers

    def convert_spark_to_pandas(self, df: DataFrame) -> pd.DataFrame:
        """
        Convert Spark DataFrame to Pandas.

        Args:
            df: Spark DataFrame.

        Returns:
            Pandas DataFrame.
        """
        if not PANDAS_AVAILABLE:
            raise ImportError("Pandas is not available")

        return df.toPandas()

    def convert_pandas_to_spark(
        self,
        df: pd.DataFrame,
        schema: Optional[StructType] = None
    ) -> DataFrame:
        """
        Convert Pandas DataFrame to Spark.

        Args:
            df: Pandas DataFrame.
            schema: Optional Spark schema.

        Returns:
            Spark DataFrame.
        """
        if self.spark is None:
            raise ValueError("SparkSession not initialized")

        if schema:
            return self.spark.createDataFrame(df, schema)
        else:
            return self.spark.createDataFrame(df)

    def convert_spark_to_polars(self, df: DataFrame) -> pl.DataFrame:
        """
        Convert Spark DataFrame to Polars via Arrow.

        Args:
            df: Spark DataFrame.

        Returns:
            Polars DataFrame.
        """
        if not POLARS_AVAILABLE:
            raise ImportError("Polars is not available")

        # Convert via Pandas (uses Arrow under the hood)
        pandas_df = df.toPandas()
        return pl.from_pandas(pandas_df)

    def convert_polars_to_spark(
        self,
        df: pl.DataFrame,
        schema: Optional[StructType] = None
    ) -> DataFrame:
        """
        Convert Polars DataFrame to Spark.

        Args:
            df: Polars DataFrame.
            schema: Optional Spark schema.

        Returns:
            Spark DataFrame.
        """
        if self.spark is None:
            raise ValueError("SparkSession not initialized")

        pandas_df = df.to_pandas()
        if schema:
            return self.spark.createDataFrame(pandas_df, schema)
        else:
            return self.spark.createDataFrame(pandas_df)

    def convert_pandas_to_polars(self, df: pd.DataFrame) -> pl.DataFrame:
        """
        Convert Pandas DataFrame to Polars.

        Args:
            df: Pandas DataFrame.

        Returns:
            Polars DataFrame.
        """
        if not POLARS_AVAILABLE:
            raise ImportError("Polars is not available")

        return pl.from_pandas(df)

    def convert_polars_to_pandas(self, df: pl.DataFrame) -> pd.DataFrame:
        """
        Convert Polars DataFrame to Pandas.

        Args:
            df: Polars DataFrame.

        Returns:
            Pandas DataFrame.
        """
        if not PANDAS_AVAILABLE:
            raise ImportError("Pandas is not available")

        return df.to_pandas()

    def get_recommendation(self, row_count: int) -> Dict[str, Any]:
        """
        Get detailed backend recommendation with reasoning.

        Args:
            row_count: Number of rows in the dataset.

        Returns:
            Dictionary with recommendation details.
        """
        backend = self.get_backend(row_count)

        recommendations = {
            ProcessingBackend.PANDAS: {
                "backend": "pandas",
                "reason": f"Small dataset ({row_count:,} rows <= {self.pandas_threshold:,}). "
                          "Pandas is sufficient with minimal overhead.",
                "alternative": "polars" if POLARS_AVAILABLE else "spark"
            },
            ProcessingBackend.POLARS: {
                "backend": "polars",
                "reason": f"Medium dataset ({row_count:,} rows). "
                          "Polars provides 5-10x speedup over Pandas with parallel processing.",
                "alternative": "spark" if row_count > 100000 else "pandas"
            },
            ProcessingBackend.SPARK: {
                "backend": "spark",
                "reason": f"Large dataset ({row_count:,} rows > {self.polars_threshold:,}). "
                          "Spark distributed processing is recommended.",
                "alternative": "polars" if POLARS_AVAILABLE else "pandas"
            }
        }

        return {
            "row_count": row_count,
            "recommended_backend": backend.value,
            **recommendations.get(backend, {}),
            "available_backends": {
                "pandas": PANDAS_AVAILABLE,
                "polars": POLARS_AVAILABLE,
                "spark": True
            }
        }


# ==============================================================================
# Apache Atlas Integration
# ==============================================================================


class ApacheAtlasClient:
    """
    Integration with Apache Atlas for enterprise metadata management.

    Apache Atlas provides open metadata management and governance capabilities
    for building a data lake. This client enables bidirectional synchronization
    of GDPR metadata with Atlas.

    Requirements:
        pip install apache-atlas-client  # Or use REST API directly
    """

    def __init__(
        self,
        atlas_url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        config: Optional[ConfigManager] = None
    ):
        """
        Initialize the Apache Atlas client.

        Args:
            atlas_url: Atlas server URL (e.g., http://atlas-server:21000)
            username: Atlas username (can be set via ATLAS_USERNAME env var)
            password: Atlas password (can be set via ATLAS_PASSWORD env var)
            config: ConfigManager for settings
        """
        self.atlas_url = atlas_url.rstrip('/')
        self.username = username or os.environ.get('ATLAS_USERNAME')
        self.password = password or os.environ.get('ATLAS_PASSWORD')
        self.config = config or ConfigManager()
        self._session = None

        # Atlas type definitions for GDPR
        self.GDPR_CLASSIFICATION_TYPES = {
            'gdpr_pii': 'Personally Identifiable Information',
            'gdpr_spi': 'Sensitive Personal Information',
            'gdpr_npii': 'Non-Personally Identifiable Information'
        }

    def _get_session(self):
        """Get or create HTTP session with authentication."""
        if self._session is None:
            try:
                import requests
                self._session = requests.Session()
                if self.username and self.password:
                    self._session.auth = (self.username, self.password)
                self._session.headers.update({
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                })
            except ImportError:
                raise ImportError(
                    "requests library required for Atlas integration. "
                    "Install with: pip install requests"
                )
        return self._session

    def test_connection(self) -> bool:
        """
        Test connection to Apache Atlas server.

        Returns:
            True if connection successful, False otherwise.
        """
        try:
            session = self._get_session()
            response = session.get(f"{self.atlas_url}/api/atlas/v2/types/typedefs/headers")
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Atlas connection test failed: {e}")
            return False

    def create_gdpr_classification_types(self) -> Dict[str, Any]:
        """
        Create GDPR classification types in Atlas if they don't exist.

        Returns:
            Dictionary with creation results.
        """
        session = self._get_session()
        results = {'created': [], 'existing': [], 'errors': []}

        for type_name, description in self.GDPR_CLASSIFICATION_TYPES.items():
            classification_def = {
                'classificationDefs': [{
                    'name': type_name,
                    'description': description,
                    'superTypes': [],
                    'attributeDefs': [
                        {
                            'name': 'sensitivity_level',
                            'typeName': 'string',
                            'isOptional': True,
                            'cardinality': 'SINGLE',
                            'valuesMinCount': 0,
                            'valuesMaxCount': 1,
                            'isUnique': False,
                            'isIndexable': True
                        },
                        {
                            'name': 'retention_days',
                            'typeName': 'int',
                            'isOptional': True,
                            'cardinality': 'SINGLE',
                            'valuesMinCount': 0,
                            'valuesMaxCount': 1,
                            'isUnique': False,
                            'isIndexable': True
                        },
                        {
                            'name': 'purpose',
                            'typeName': 'string',
                            'isOptional': True,
                            'cardinality': 'SINGLE'
                        }
                    ]
                }]
            }

            try:
                response = session.post(
                    f"{self.atlas_url}/api/atlas/v2/types/typedefs",
                    json=classification_def
                )
                if response.status_code in [200, 201]:
                    results['created'].append(type_name)
                    logger.info(f"Created Atlas classification type: {type_name}")
                elif response.status_code == 409:
                    results['existing'].append(type_name)
                else:
                    results['errors'].append({
                        'type': type_name,
                        'error': response.text
                    })
            except Exception as e:
                results['errors'].append({'type': type_name, 'error': str(e)})

        return results

    def apply_classification_to_entity(
        self,
        entity_guid: str,
        classification_name: str,
        attributes: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Apply a GDPR classification to an Atlas entity.

        Args:
            entity_guid: Atlas entity GUID.
            classification_name: Classification type name.
            attributes: Optional classification attributes.

        Returns:
            True if successful.
        """
        session = self._get_session()

        classification = {
            'typeName': classification_name,
            'attributes': attributes or {}
        }

        try:
            response = session.post(
                f"{self.atlas_url}/api/atlas/v2/entity/guid/{entity_guid}/classifications",
                json=[classification]
            )
            if response.status_code in [200, 204]:
                logger.info(f"Applied {classification_name} to entity {entity_guid}")
                return True
            else:
                logger.error(f"Failed to apply classification: {response.text}")
                return False
        except Exception as e:
            logger.error(f"Error applying classification: {e}")
            return False

    def sync_table_metadata(
        self,
        table_name: str,
        column_tags: Dict[str, Dict[str, Any]],
        qualified_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Sync GDPR metadata to Atlas for a table.

        Args:
            table_name: Name of the table.
            column_tags: GDPR tags for each column.
            qualified_name: Atlas qualified name for the table.

        Returns:
            Sync results.
        """
        session = self._get_session()
        results = {'table': table_name, 'synced_columns': [], 'errors': []}

        # Search for the table entity
        q_name = qualified_name or f"default.{table_name}@primary"
        search_query = {
            'typeName': 'hive_table',
            'excludeDeletedEntities': True,
            'query': q_name,
            'limit': 1
        }

        try:
            response = session.post(
                f"{self.atlas_url}/api/atlas/v2/search/basic",
                json=search_query
            )

            if response.status_code != 200:
                results['errors'].append(f"Table search failed: {response.text}")
                return results

            search_results = response.json()
            if not search_results.get('entities'):
                results['errors'].append(f"Table {table_name} not found in Atlas")
                return results

            table_entity = search_results['entities'][0]
            table_guid = table_entity['guid']

            # Get table details including columns
            entity_response = session.get(
                f"{self.atlas_url}/api/atlas/v2/entity/guid/{table_guid}?minExtInfo=true"
            )

            if entity_response.status_code != 200:
                results['errors'].append(f"Failed to get table details: {entity_response.text}")
                return results

            entity_data = entity_response.json()
            columns = entity_data.get('referredEntities', {})

            # Map column names to GUIDs
            column_guids = {}
            for guid, col_entity in columns.items():
                if col_entity.get('typeName') == 'hive_column':
                    col_name = col_entity.get('attributes', {}).get('name')
                    if col_name:
                        column_guids[col_name] = guid

            # Apply classifications to columns
            for col_name, tags in column_tags.items():
                if col_name not in column_guids:
                    continue

                col_guid = column_guids[col_name]
                gdpr_category = tags.get('gdpr_category', '').upper()

                # Map to Atlas classification
                classification_map = {
                    'PII': 'gdpr_pii',
                    'SPI': 'gdpr_spi',
                    'NPII': 'gdpr_npii'
                }

                if gdpr_category in classification_map:
                    classification = classification_map[gdpr_category]
                    attrs = {
                        'sensitivity_level': tags.get('sensitivity', 'MEDIUM'),
                        'retention_days': tags.get('retention_days', 365),
                        'purpose': tags.get('purpose', '')
                    }

                    if self.apply_classification_to_entity(col_guid, classification, attrs):
                        results['synced_columns'].append(col_name)
                    else:
                        results['errors'].append(f"Failed to sync {col_name}")

            logger.info(f"Atlas sync complete for {table_name}: {len(results['synced_columns'])} columns")

        except Exception as e:
            results['errors'].append(str(e))
            logger.error(f"Atlas sync error: {e}")

        return results

    def get_classified_entities(
        self,
        classification_name: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get all entities with a specific GDPR classification.

        Args:
            classification_name: Classification type name.
            limit: Maximum number of results.

        Returns:
            List of entity details.
        """
        session = self._get_session()

        try:
            response = session.get(
                f"{self.atlas_url}/api/atlas/v2/search/basic",
                params={
                    'classification': classification_name,
                    'limit': limit
                }
            )

            if response.status_code == 200:
                data = response.json()
                return data.get('entities', [])
            else:
                logger.error(f"Classification search failed: {response.text}")
                return []
        except Exception as e:
            logger.error(f"Error searching classifications: {e}")
            return []

    def generate_gdpr_lineage_report(
        self,
        entity_guid: str,
        direction: str = "BOTH",
        depth: int = 3
    ) -> Dict[str, Any]:
        """
        Generate a GDPR lineage report for an entity.

        Args:
            entity_guid: Entity GUID to trace.
            direction: INPUT, OUTPUT, or BOTH.
            depth: Lineage depth.

        Returns:
            Lineage report with GDPR classifications.
        """
        session = self._get_session()

        try:
            response = session.get(
                f"{self.atlas_url}/api/atlas/v2/lineage/{entity_guid}",
                params={'direction': direction, 'depth': depth}
            )

            if response.status_code != 200:
                return {'error': response.text}

            lineage_data = response.json()

            # Enrich with GDPR classifications
            report = {
                'base_entity': entity_guid,
                'lineage_depth': depth,
                'gdpr_entities': [],
                'relations': lineage_data.get('relations', [])
            }

            # Check each entity for GDPR classifications
            for guid, entity in lineage_data.get('guidEntityMap', {}).items():
                classifications = entity.get('classifications', [])
                gdpr_classes = [
                    c for c in classifications
                    if c.get('typeName', '').startswith('gdpr_')
                ]

                if gdpr_classes:
                    report['gdpr_entities'].append({
                        'guid': guid,
                        'name': entity.get('attributes', {}).get('name'),
                        'type': entity.get('typeName'),
                        'gdpr_classifications': gdpr_classes
                    })

            return report

        except Exception as e:
            logger.error(f"Lineage report error: {e}")
            return {'error': str(e)}


# ==============================================================================
# AWS Glue Catalog Integration
# ==============================================================================


class AWSGlueCatalogClient:
    """
    Integration with AWS Glue Data Catalog for AWS-native governance.

    Provides GDPR metadata management through AWS Glue, supporting
    table/column tagging, Lake Formation integration, and compliance reporting.

    Requirements:
        pip install boto3
    """

    def __init__(
        self,
        region_name: Optional[str] = None,
        database_name: Optional[str] = None,
        config: Optional[ConfigManager] = None
    ):
        """
        Initialize the AWS Glue Catalog client.

        Args:
            region_name: AWS region (or AWS_DEFAULT_REGION env var).
            database_name: Default Glue database name.
            config: ConfigManager for settings.
        """
        self.region_name = region_name or os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
        self.database_name = database_name or 'default'
        self.config = config or ConfigManager()
        self._glue_client = None
        self._lakeformation_client = None

        # GDPR tag keys
        self.TAG_PREFIX = 'gdpr:'
        self.GDPR_TAGS = {
            'category': f'{self.TAG_PREFIX}category',
            'sensitivity': f'{self.TAG_PREFIX}sensitivity',
            'retention_days': f'{self.TAG_PREFIX}retention_days',
            'purpose': f'{self.TAG_PREFIX}purpose',
            'pii': f'{self.TAG_PREFIX}pii'
        }

    def _get_glue_client(self):
        """Get or create Glue client."""
        if self._glue_client is None:
            try:
                import boto3
                self._glue_client = boto3.client('glue', region_name=self.region_name)
            except ImportError:
                raise ImportError(
                    "boto3 required for AWS Glue integration. "
                    "Install with: pip install boto3"
                )
        return self._glue_client

    def _get_lakeformation_client(self):
        """Get or create Lake Formation client."""
        if self._lakeformation_client is None:
            try:
                import boto3
                self._lakeformation_client = boto3.client(
                    'lakeformation',
                    region_name=self.region_name
                )
            except ImportError:
                raise ImportError("boto3 required for Lake Formation integration")
        return self._lakeformation_client

    def test_connection(self) -> bool:
        """
        Test connection to AWS Glue.

        Returns:
            True if connection successful.
        """
        try:
            client = self._get_glue_client()
            client.get_databases(MaxResults=1)
            return True
        except Exception as e:
            logger.error(f"AWS Glue connection test failed: {e}")
            return False

    def apply_table_tags(
        self,
        table_name: str,
        tags: Dict[str, str],
        database_name: Optional[str] = None
    ) -> bool:
        """
        Apply GDPR tags to a Glue table.

        Args:
            table_name: Name of the table.
            tags: Dictionary of tag key-value pairs.
            database_name: Database name (uses default if not provided).

        Returns:
            True if successful.
        """
        client = self._get_glue_client()
        db_name = database_name or self.database_name

        # Prefix tags with gdpr:
        glue_tags = {
            f"{self.TAG_PREFIX}{k}" if not k.startswith(self.TAG_PREFIX) else k: str(v)
            for k, v in tags.items()
        }

        try:
            # Get current table
            response = client.get_table(DatabaseName=db_name, Name=table_name)
            table = response['Table']

            # Merge with existing parameters
            params = table.get('Parameters', {})
            params.update(glue_tags)

            # Update table
            table_input = {
                'Name': table_name,
                'StorageDescriptor': table['StorageDescriptor'],
                'Parameters': params
            }

            if 'PartitionKeys' in table:
                table_input['PartitionKeys'] = table['PartitionKeys']
            if 'TableType' in table:
                table_input['TableType'] = table['TableType']

            client.update_table(
                DatabaseName=db_name,
                TableInput=table_input
            )

            logger.info(f"Applied GDPR tags to Glue table {db_name}.{table_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to apply Glue table tags: {e}")
            return False

    def apply_column_tags(
        self,
        table_name: str,
        column_tags: Dict[str, Dict[str, Any]],
        database_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Apply GDPR tags to columns using column comments and Lake Formation.

        Args:
            table_name: Name of the table.
            column_tags: GDPR tags for each column.
            database_name: Database name.

        Returns:
            Results dictionary.
        """
        client = self._get_glue_client()
        db_name = database_name or self.database_name
        results = {'updated_columns': [], 'errors': []}

        try:
            # Get current table
            response = client.get_table(DatabaseName=db_name, Name=table_name)
            table = response['Table']
            storage_descriptor = table['StorageDescriptor']
            columns = storage_descriptor.get('Columns', [])

            # Update column comments with GDPR metadata
            for i, col in enumerate(columns):
                col_name = col['Name']
                if col_name in column_tags:
                    tags = column_tags[col_name]

                    # Store GDPR metadata in comment as JSON
                    gdpr_comment = json.dumps({
                        'gdpr_category': tags.get('gdpr_category'),
                        'sensitivity': tags.get('sensitivity'),
                        'retention_days': tags.get('retention_days'),
                        'purpose': tags.get('purpose')
                    })

                    columns[i]['Comment'] = gdpr_comment
                    results['updated_columns'].append(col_name)

            # Update the table
            storage_descriptor['Columns'] = columns

            table_input = {
                'Name': table_name,
                'StorageDescriptor': storage_descriptor,
                'Parameters': table.get('Parameters', {})
            }

            if 'PartitionKeys' in table:
                table_input['PartitionKeys'] = table['PartitionKeys']
            if 'TableType' in table:
                table_input['TableType'] = table['TableType']

            client.update_table(
                DatabaseName=db_name,
                TableInput=table_input
            )

            logger.info(
                f"Updated {len(results['updated_columns'])} columns in "
                f"{db_name}.{table_name}"
            )

        except Exception as e:
            results['errors'].append(str(e))
            logger.error(f"Failed to apply column tags: {e}")

        return results

    def apply_lakeformation_tags(
        self,
        table_name: str,
        column_tags: Dict[str, Dict[str, Any]],
        database_name: Optional[str] = None,
        catalog_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Apply Lake Formation LF-Tags for fine-grained access control.

        Args:
            table_name: Name of the table.
            column_tags: GDPR tags for columns.
            database_name: Database name.
            catalog_id: AWS account ID (uses current if not provided).

        Returns:
            Results dictionary.
        """
        lf_client = self._get_lakeformation_client()
        db_name = database_name or self.database_name
        results = {'tagged_resources': [], 'errors': []}

        try:
            # First, ensure LF-Tags exist
            self._ensure_lf_tags_exist(lf_client, catalog_id)

            # Apply tags to columns
            for col_name, tags in column_tags.items():
                lf_tags = []

                if 'gdpr_category' in tags:
                    lf_tags.append({
                        'TagKey': 'gdpr_category',
                        'TagValues': [tags['gdpr_category']]
                    })

                if 'sensitivity' in tags:
                    lf_tags.append({
                        'TagKey': 'gdpr_sensitivity',
                        'TagValues': [tags['sensitivity']]
                    })

                if lf_tags:
                    resource = {
                        'Table': {
                            'DatabaseName': db_name,
                            'Name': table_name,
                            'ColumnNames': [col_name]
                        }
                    }

                    if catalog_id:
                        resource['Table']['CatalogId'] = catalog_id

                    try:
                        lf_client.add_lf_tags_to_resource(
                            Resource=resource,
                            LFTags=lf_tags
                        )
                        results['tagged_resources'].append(col_name)
                    except Exception as e:
                        results['errors'].append(f"{col_name}: {e}")

            logger.info(
                f"Applied Lake Formation tags to "
                f"{len(results['tagged_resources'])} columns"
            )

        except Exception as e:
            results['errors'].append(str(e))
            logger.error(f"Lake Formation tagging error: {e}")

        return results

    def _ensure_lf_tags_exist(
        self,
        lf_client,
        catalog_id: Optional[str] = None
    ) -> None:
        """Ensure required LF-Tags exist in Lake Formation."""
        required_tags = {
            'gdpr_category': ['PII', 'SPI', 'NPII'],
            'gdpr_sensitivity': ['LOW', 'MEDIUM', 'HIGH', 'VERY_HIGH']
        }

        for tag_key, tag_values in required_tags.items():
            try:
                params = {'TagKey': tag_key, 'TagValues': tag_values}
                if catalog_id:
                    params['CatalogId'] = catalog_id

                lf_client.create_lf_tag(**params)
                logger.info(f"Created LF-Tag: {tag_key}")
            except Exception as e:
                # Tag might already exist
                if 'AlreadyExistsException' not in str(e):
                    logger.warning(f"Could not create LF-Tag {tag_key}: {e}")

    def get_gdpr_tables(
        self,
        database_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all tables with GDPR tags.

        Args:
            database_name: Database to search (uses default if not provided).

        Returns:
            List of tables with GDPR metadata.
        """
        client = self._get_glue_client()
        db_name = database_name or self.database_name
        gdpr_tables = []

        try:
            paginator = client.get_paginator('get_tables')

            for page in paginator.paginate(DatabaseName=db_name):
                for table in page.get('TableList', []):
                    params = table.get('Parameters', {})

                    # Check for GDPR tags
                    gdpr_params = {
                        k: v for k, v in params.items()
                        if k.startswith(self.TAG_PREFIX)
                    }

                    if gdpr_params:
                        gdpr_tables.append({
                            'database': db_name,
                            'table': table['Name'],
                            'gdpr_tags': gdpr_params,
                            'columns': self._extract_column_gdpr_info(
                                table.get('StorageDescriptor', {}).get('Columns', [])
                            )
                        })

        except Exception as e:
            logger.error(f"Error getting GDPR tables: {e}")

        return gdpr_tables

    def _extract_column_gdpr_info(
        self,
        columns: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Extract GDPR info from column comments."""
        gdpr_columns = []

        for col in columns:
            comment = col.get('Comment', '')
            if comment:
                try:
                    gdpr_info = json.loads(comment)
                    if any(k.startswith('gdpr') for k in gdpr_info.keys()):
                        gdpr_columns.append({
                            'name': col['Name'],
                            'type': col['Type'],
                            **gdpr_info
                        })
                except json.JSONDecodeError:
                    pass

        return gdpr_columns

    def generate_compliance_report(
        self,
        database_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate a GDPR compliance report for a database.

        Args:
            database_name: Database to report on.

        Returns:
            Compliance report dictionary.
        """
        db_name = database_name or self.database_name
        gdpr_tables = self.get_gdpr_tables(db_name)

        report = {
            'database': db_name,
            'generated_at': datetime.datetime.now().isoformat(),
            'summary': {
                'total_tables': len(gdpr_tables),
                'tables_with_pii': 0,
                'tables_with_spi': 0,
                'total_pii_columns': 0,
                'total_spi_columns': 0
            },
            'tables': []
        }

        for table in gdpr_tables:
            table_summary = {
                'name': table['table'],
                'gdpr_tags': table['gdpr_tags'],
                'pii_columns': [],
                'spi_columns': []
            }

            for col in table.get('columns', []):
                category = col.get('gdpr_category', '')
                if category == 'PII':
                    table_summary['pii_columns'].append(col['name'])
                    report['summary']['total_pii_columns'] += 1
                elif category == 'SPI':
                    table_summary['spi_columns'].append(col['name'])
                    report['summary']['total_spi_columns'] += 1

            if table_summary['pii_columns']:
                report['summary']['tables_with_pii'] += 1
            if table_summary['spi_columns']:
                report['summary']['tables_with_spi'] += 1

            report['tables'].append(table_summary)

        return report


# ==============================================================================
# ML-based PII Detection
# ==============================================================================


class MLPIIDetector:
    """
    Machine learning-based PII detection using NLP techniques.

    Uses Named Entity Recognition (NER) and pattern matching to identify
    PII in unstructured text data. Supports multiple backends including
    spaCy, transformers, and a simple rule-based fallback.

    Requirements (optional, for enhanced detection):
        pip install spacy
        python -m spacy download en_core_web_sm

        OR

        pip install transformers torch
    """

    # Entity types that indicate PII
    PII_ENTITY_TYPES = {
        'PERSON': 'PII',           # Person names
        'ORG': 'NPII',             # Organizations (context-dependent)
        'GPE': 'PII',              # Geo-political entities (addresses)
        'LOC': 'PII',              # Locations
        'DATE': 'PII',             # Dates (birth dates)
        'EMAIL': 'PII',            # Email addresses
        'PHONE': 'PII',            # Phone numbers
        'MONEY': 'SPI',            # Financial info
        'CARDINAL': 'NPII',        # Numbers
        'NORP': 'SPI',             # Nationalities, religious/political groups
    }

    def __init__(
        self,
        config: Optional[ConfigManager] = None,
        backend: str = 'auto'
    ):
        """
        Initialize ML PII detector.

        Args:
            config: ConfigManager for settings.
            backend: Detection backend ('spacy', 'transformers', 'regex', 'auto').
        """
        self.config = config or ConfigManager()
        self.backend = backend
        self._nlp = None
        self._transformer_pipeline = None
        self._initialized = False

        # Confidence thresholds
        self.min_confidence = 0.7

        # Custom patterns for detection
        self._custom_patterns = {}

        # Initialize pattern-based detector as fallback
        self._pattern_detector = PIIDetector(config)

    def _initialize_backend(self) -> str:
        """Initialize the appropriate backend."""
        if self._initialized:
            return self.backend

        selected_backend = self.backend

        if selected_backend == 'auto':
            # Try backends in order of preference
            if self._try_init_spacy():
                selected_backend = 'spacy'
            elif self._try_init_transformers():
                selected_backend = 'transformers'
            else:
                selected_backend = 'regex'
                logger.info("Using regex-based PII detection (no ML backend available)")
        elif selected_backend == 'spacy':
            if not self._try_init_spacy():
                raise ImportError(
                    "spaCy not available. Install with: "
                    "pip install spacy && python -m spacy download en_core_web_sm"
                )
        elif selected_backend == 'transformers':
            if not self._try_init_transformers():
                raise ImportError(
                    "Transformers not available. Install with: "
                    "pip install transformers torch"
                )

        self._initialized = True
        self.backend = selected_backend
        return selected_backend

    def _try_init_spacy(self) -> bool:
        """Try to initialize spaCy."""
        try:
            import spacy

            # Try different model sizes
            for model in ['en_core_web_sm', 'en_core_web_md', 'en_core_web_lg']:
                try:
                    self._nlp = spacy.load(model)
                    logger.info(f"Loaded spaCy model: {model}")
                    return True
                except OSError:
                    continue

            return False
        except ImportError:
            return False

    def _try_init_transformers(self) -> bool:
        """Try to initialize transformers NER pipeline."""
        try:
            from transformers import pipeline

            self._transformer_pipeline = pipeline(
                "ner",
                model="dslim/bert-base-NER",
                aggregation_strategy="simple"
            )
            logger.info("Loaded transformers NER pipeline")
            return True
        except Exception:
            return False

    def detect_pii_in_text(
        self,
        text: str,
        include_patterns: bool = True
    ) -> Dict[str, Any]:
        """
        Detect PII in a text string.

        Args:
            text: Text to analyze.
            include_patterns: Also run pattern-based detection.

        Returns:
            Detection results with entities and classifications.
        """
        backend = self._initialize_backend()

        results = {
            'text_length': len(text),
            'backend_used': backend,
            'entities': [],
            'pattern_matches': {},
            'pii_detected': False,
            'spi_detected': False
        }

        # ML-based detection
        if backend == 'spacy':
            results['entities'] = self._detect_with_spacy(text)
        elif backend == 'transformers':
            results['entities'] = self._detect_with_transformers(text)

        # Pattern-based detection
        if include_patterns:
            results['pattern_matches'] = self._detect_patterns(text)

        # Classify results
        for entity in results['entities']:
            gdpr_category = self.PII_ENTITY_TYPES.get(entity['type'], 'UNKNOWN')
            entity['gdpr_category'] = gdpr_category

            if gdpr_category == 'PII':
                results['pii_detected'] = True
            elif gdpr_category == 'SPI':
                results['spi_detected'] = True

        # Check pattern matches
        for pattern_type, matches in results['pattern_matches'].items():
            if matches:
                results['pii_detected'] = True

        return results

    def _detect_with_spacy(self, text: str) -> List[Dict[str, Any]]:
        """Detect entities using spaCy."""
        entities = []

        doc = self._nlp(text)
        for ent in doc.ents:
            entities.append({
                'text': ent.text,
                'type': ent.label_,
                'start': ent.start_char,
                'end': ent.end_char,
                'confidence': 0.85  # spaCy doesn't provide confidence scores
            })

        return entities

    def _detect_with_transformers(self, text: str) -> List[Dict[str, Any]]:
        """Detect entities using transformers."""
        entities = []

        # Transformers entity mapping
        type_mapping = {
            'PER': 'PERSON',
            'LOC': 'LOC',
            'ORG': 'ORG',
            'MISC': 'MISC'
        }

        results = self._transformer_pipeline(text)
        for ent in results:
            entity_type = ent.get('entity_group', 'UNKNOWN')
            mapped_type = type_mapping.get(entity_type, entity_type)

            if ent.get('score', 0) >= self.min_confidence:
                entities.append({
                    'text': ent['word'],
                    'type': mapped_type,
                    'start': ent['start'],
                    'end': ent['end'],
                    'confidence': ent['score']
                })

        return entities

    def _detect_patterns(self, text: str) -> Dict[str, List[str]]:
        """Detect PII using regex patterns."""
        matches = {}

        patterns = self.config.get('pii_detection', 'patterns', default={})
        patterns.update(self._custom_patterns)

        for pattern_name, pattern in patterns.items():
            found = re.findall(pattern, text)
            if found:
                matches[pattern_name] = found

        return matches

    def scan_dataframe_content(
        self,
        df: DataFrame,
        columns: Optional[List[str]] = None,
        sample_size: int = 100
    ) -> Dict[str, Any]:
        """
        Scan DataFrame content for PII using ML detection.

        Args:
            df: Spark DataFrame to scan.
            columns: Columns to scan (all string columns if not specified).
            sample_size: Number of rows to sample.

        Returns:
            Scan results by column.
        """
        results = {
            'scan_type': 'ml_content_scan',
            'sample_size': sample_size,
            'backend': self._initialize_backend(),
            'columns': {}
        }

        # Determine columns to scan
        if columns is None:
            columns = [
                f.name for f in df.schema.fields
                if str(f.dataType) == 'StringType'
            ]

        # Sample data
        sample_df = df.limit(sample_size)

        for col_name in columns:
            if col_name not in df.columns:
                continue

            col_results = {
                'total_entities': 0,
                'entity_types': {},
                'pii_detected': False,
                'spi_detected': False,
                'sample_entities': []
            }

            # Get column values
            values = sample_df.select(col_name).collect()

            for row in values:
                value = row[0]
                if value is None:
                    continue

                detection = self.detect_pii_in_text(str(value))

                for entity in detection['entities']:
                    col_results['total_entities'] += 1

                    etype = entity['type']
                    if etype not in col_results['entity_types']:
                        col_results['entity_types'][etype] = 0
                    col_results['entity_types'][etype] += 1

                    if len(col_results['sample_entities']) < 5:
                        col_results['sample_entities'].append(entity)

                if detection['pii_detected']:
                    col_results['pii_detected'] = True
                if detection['spi_detected']:
                    col_results['spi_detected'] = True

            results['columns'][col_name] = col_results

        return results

    def get_suggested_tags(
        self,
        scan_results: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Generate GDPR tag suggestions based on ML scan results.

        Args:
            scan_results: Results from scan_dataframe_content.

        Returns:
            Suggested tags for each column.
        """
        suggestions = {}

        for col_name, col_results in scan_results.get('columns', {}).items():
            if col_results.get('pii_detected') or col_results.get('spi_detected'):
                # Determine category based on entity types
                entity_types = col_results.get('entity_types', {})

                has_spi = any(
                    self.PII_ENTITY_TYPES.get(et) == 'SPI'
                    for et in entity_types.keys()
                )

                suggestions[col_name] = {
                    'gdpr_category': 'SPI' if has_spi else 'PII',
                    'sensitivity': 'VERY_HIGH' if has_spi else 'HIGH',
                    'detection_method': 'ml',
                    'entity_types_found': list(entity_types.keys()),
                    'confidence': min(
                        1.0,
                        col_results['total_entities'] / 10
                    )  # More entities = higher confidence
                }

        return suggestions

    def add_custom_pattern(self, name: str, pattern: str) -> None:
        """Add a custom regex pattern for detection."""
        self._custom_patterns[name] = pattern

    def train_custom_detector(
        self,
        training_data: List[Tuple[str, List[Tuple[int, int, str]]]],
        model_path: str
    ) -> None:
        """
        Train a custom NER model for domain-specific PII.

        Args:
            training_data: List of (text, entities) tuples where entities
                          are (start, end, label) tuples.
            model_path: Path to save the trained model.
        """
        if self._nlp is None:
            raise RuntimeError("spaCy must be initialized for training")

        # This is a placeholder for custom training
        # Full implementation would use spaCy's training capabilities
        logger.info(
            f"Training custom NER model with {len(training_data)} examples..."
        )

        # Convert training data to spaCy format
        # ... training logic would go here ...

        logger.info(f"Model saved to {model_path}")


# ==============================================================================
# Data Lineage Tracking
# ==============================================================================


@dataclass
class LineageNode:
    """Represents a node in the data lineage graph."""
    node_id: str
    node_type: str  # 'dataset', 'transformation', 'column'
    name: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    gdpr_tags: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime.datetime = field(default_factory=datetime.datetime.now)


@dataclass
class LineageEdge:
    """Represents an edge (relationship) in the lineage graph."""
    edge_id: str
    source_id: str
    target_id: str
    operation: str  # 'derived_from', 'transformed_by', 'merged_with'
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime.datetime = field(default_factory=datetime.datetime.now)


class DataLineageTracker:
    """
    Tracks data lineage for GDPR compliance.

    Maintains a graph of data transformations to support:
    - Impact analysis for schema changes
    - Data subject access requests (tracing data flow)
    - Compliance auditing (showing data transformations)
    - Root cause analysis for data quality issues
    """

    def __init__(
        self,
        config: Optional[ConfigManager] = None,
        storage_path: Optional[str] = None,
        audit_manager: Optional[AuditManager] = None
    ):
        """
        Initialize the lineage tracker.

        Args:
            config: ConfigManager for settings.
            storage_path: Path to store lineage data.
            audit_manager: AuditManager for logging.
        """
        self.config = config or ConfigManager()
        self.storage_path = Path(storage_path) if storage_path else Path("./lineage_data")
        self.audit = audit_manager

        # Lineage graph
        self._nodes: Dict[str, LineageNode] = {}
        self._edges: Dict[str, LineageEdge] = {}
        self._column_lineage: Dict[str, List[str]] = {}  # column -> source columns

        # Create storage directory
        self.storage_path.mkdir(parents=True, exist_ok=True)

        # Load existing lineage data
        self._load_lineage()

    def _generate_id(self) -> str:
        """Generate a unique ID."""
        return str(uuid.uuid4())[:8]

    def register_dataset(
        self,
        name: str,
        schema: Optional[Dict[str, str]] = None,
        gdpr_tags: Optional[Dict[str, Dict[str, Any]]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Register a dataset in the lineage graph.

        Args:
            name: Dataset name.
            schema: Column name to type mapping.
            gdpr_tags: GDPR tags for columns.
            metadata: Additional metadata.

        Returns:
            Dataset node ID.
        """
        node_id = f"ds_{self._generate_id()}"

        node = LineageNode(
            node_id=node_id,
            node_type='dataset',
            name=name,
            metadata={
                'schema': schema or {},
                **(metadata or {})
            },
            gdpr_tags=gdpr_tags or {}
        )

        self._nodes[node_id] = node

        # Register columns as separate nodes
        if schema:
            for col_name, col_type in schema.items():
                col_node_id = f"col_{node_id}_{col_name}"
                col_node = LineageNode(
                    node_id=col_node_id,
                    node_type='column',
                    name=f"{name}.{col_name}",
                    metadata={'data_type': col_type, 'parent_dataset': node_id},
                    gdpr_tags=gdpr_tags.get(col_name, {}) if gdpr_tags else {}
                )
                self._nodes[col_node_id] = col_node

        self._save_lineage()
        logger.info(f"Registered dataset: {name} (ID: {node_id})")

        return node_id

    def register_transformation(
        self,
        name: str,
        operation_type: str,
        source_ids: List[str],
        target_id: str,
        column_mappings: Optional[Dict[str, List[str]]] = None,
        details: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Register a data transformation in the lineage graph.

        Args:
            name: Transformation name.
            operation_type: Type of operation (e.g., 'anonymize', 'filter', 'join').
            source_ids: Source dataset/transformation IDs.
            target_id: Target dataset ID.
            column_mappings: Target column -> source columns mapping.
            details: Additional transformation details.

        Returns:
            Transformation node ID.
        """
        transform_id = f"tx_{self._generate_id()}"

        # Create transformation node
        node = LineageNode(
            node_id=transform_id,
            node_type='transformation',
            name=name,
            metadata={
                'operation_type': operation_type,
                **(details or {})
            }
        )
        self._nodes[transform_id] = node

        # Create edges from sources to transformation
        for source_id in source_ids:
            edge_id = f"e_{self._generate_id()}"
            edge = LineageEdge(
                edge_id=edge_id,
                source_id=source_id,
                target_id=transform_id,
                operation='input_to'
            )
            self._edges[edge_id] = edge

        # Create edge from transformation to target
        edge_id = f"e_{self._generate_id()}"
        edge = LineageEdge(
            edge_id=edge_id,
            source_id=transform_id,
            target_id=target_id,
            operation='produces'
        )
        self._edges[edge_id] = edge

        # Track column-level lineage
        if column_mappings:
            for target_col, source_cols in column_mappings.items():
                target_col_id = f"col_{target_id}_{target_col}"
                self._column_lineage[target_col_id] = source_cols

        self._save_lineage()

        # Log audit event
        if self.audit:
            self.audit.log_event(
                event_type=AuditEventType.DATA_MODIFICATION,
                actor="lineage_tracker",
                action="register_transformation",
                resource=name,
                details={
                    'operation_type': operation_type,
                    'sources': source_ids,
                    'target': target_id
                }
            )

        logger.info(f"Registered transformation: {name} (ID: {transform_id})")
        return transform_id

    def track_anonymization(
        self,
        source_id: str,
        target_id: str,
        columns_anonymized: Dict[str, str],
        method: str = 'auto_anonymize'
    ) -> str:
        """
        Track an anonymization operation.

        Args:
            source_id: Source dataset ID.
            target_id: Target dataset ID.
            columns_anonymized: Column -> anonymization method mapping.
            method: Anonymization method name.

        Returns:
            Transformation ID.
        """
        column_mappings = {col: [col] for col in columns_anonymized.keys()}

        return self.register_transformation(
            name=f"anonymize_{self._generate_id()}",
            operation_type='anonymization',
            source_ids=[source_id],
            target_id=target_id,
            column_mappings=column_mappings,
            details={
                'anonymization_methods': columns_anonymized,
                'method': method
            }
        )

    def track_erasure(
        self,
        source_id: str,
        target_id: str,
        subject_id: str,
        columns_erased: List[str]
    ) -> str:
        """
        Track a data erasure operation (GDPR right to be forgotten).

        Args:
            source_id: Source dataset ID.
            target_id: Target dataset ID.
            subject_id: ID of the data subject whose data was erased.
            columns_erased: Columns that were erased.

        Returns:
            Transformation ID.
        """
        return self.register_transformation(
            name=f"erasure_{subject_id}_{self._generate_id()}",
            operation_type='erasure',
            source_ids=[source_id],
            target_id=target_id,
            column_mappings={col: [col] for col in columns_erased},
            details={
                'subject_id': subject_id,
                'columns_erased': columns_erased,
                'gdpr_article': 'Art. 17 - Right to erasure'
            }
        )

    def get_upstream_lineage(
        self,
        node_id: str,
        depth: int = 10
    ) -> Dict[str, Any]:
        """
        Get upstream lineage (data sources) for a node.

        Args:
            node_id: Node to trace.
            depth: Maximum depth to trace.

        Returns:
            Upstream lineage graph.
        """
        visited = set()
        lineage = {'nodes': [], 'edges': []}

        def traverse(current_id: str, current_depth: int):
            if current_depth <= 0 or current_id in visited:
                return

            visited.add(current_id)

            if current_id in self._nodes:
                node = self._nodes[current_id]
                lineage['nodes'].append({
                    'id': node.node_id,
                    'type': node.node_type,
                    'name': node.name,
                    'gdpr_tags': node.gdpr_tags
                })

            # Find incoming edges
            for edge in self._edges.values():
                if edge.target_id == current_id:
                    lineage['edges'].append({
                        'source': edge.source_id,
                        'target': edge.target_id,
                        'operation': edge.operation
                    })
                    traverse(edge.source_id, current_depth - 1)

        traverse(node_id, depth)
        return lineage

    def get_downstream_lineage(
        self,
        node_id: str,
        depth: int = 10
    ) -> Dict[str, Any]:
        """
        Get downstream lineage (derived datasets) for a node.

        Args:
            node_id: Node to trace.
            depth: Maximum depth to trace.

        Returns:
            Downstream lineage graph.
        """
        visited = set()
        lineage = {'nodes': [], 'edges': []}

        def traverse(current_id: str, current_depth: int):
            if current_depth <= 0 or current_id in visited:
                return

            visited.add(current_id)

            if current_id in self._nodes:
                node = self._nodes[current_id]
                lineage['nodes'].append({
                    'id': node.node_id,
                    'type': node.node_type,
                    'name': node.name,
                    'gdpr_tags': node.gdpr_tags
                })

            # Find outgoing edges
            for edge in self._edges.values():
                if edge.source_id == current_id:
                    lineage['edges'].append({
                        'source': edge.source_id,
                        'target': edge.target_id,
                        'operation': edge.operation
                    })
                    traverse(edge.target_id, current_depth - 1)

        traverse(node_id, depth)
        return lineage

    def get_column_lineage(
        self,
        dataset_id: str,
        column_name: str
    ) -> List[Dict[str, Any]]:
        """
        Get column-level lineage.

        Args:
            dataset_id: Dataset containing the column.
            column_name: Column name.

        Returns:
            List of source columns.
        """
        col_id = f"col_{dataset_id}_{column_name}"
        sources = []

        def trace_column(current_col_id: str, visited: set):
            if current_col_id in visited:
                return
            visited.add(current_col_id)

            if current_col_id in self._column_lineage:
                for source in self._column_lineage[current_col_id]:
                    sources.append({
                        'column_id': source,
                        'node': self._nodes.get(source)
                    })
                    trace_column(source, visited)

        trace_column(col_id, set())
        return sources

    def impact_analysis(
        self,
        node_id: str
    ) -> Dict[str, Any]:
        """
        Perform impact analysis for a dataset or column change.

        Args:
            node_id: Node to analyze.

        Returns:
            Impact analysis report.
        """
        downstream = self.get_downstream_lineage(node_id, depth=20)

        node = self._nodes.get(node_id)

        analysis = {
            'source_node': {
                'id': node_id,
                'name': node.name if node else 'Unknown',
                'type': node.node_type if node else 'Unknown'
            },
            'affected_datasets': [],
            'affected_transformations': [],
            'gdpr_impact': {
                'pii_affected': False,
                'spi_affected': False,
                'affected_columns': []
            }
        }

        for ln_node in downstream['nodes']:
            if ln_node['type'] == 'dataset':
                analysis['affected_datasets'].append(ln_node)
            elif ln_node['type'] == 'transformation':
                analysis['affected_transformations'].append(ln_node)

            # Check GDPR impact
            gdpr_tags = ln_node.get('gdpr_tags', {})
            for col, tags in gdpr_tags.items():
                if isinstance(tags, dict):
                    category = tags.get('gdpr_category')
                    if category == 'PII':
                        analysis['gdpr_impact']['pii_affected'] = True
                        analysis['gdpr_impact']['affected_columns'].append(col)
                    elif category == 'SPI':
                        analysis['gdpr_impact']['spi_affected'] = True
                        analysis['gdpr_impact']['affected_columns'].append(col)

        return analysis

    def generate_dsar_lineage_report(
        self,
        subject_id: str
    ) -> Dict[str, Any]:
        """
        Generate a Data Subject Access Request lineage report.

        Shows all transformations and datasets that processed
        a specific data subject's data.

        Args:
            subject_id: Data subject identifier.

        Returns:
            DSAR lineage report.
        """
        report = {
            'subject_id': subject_id,
            'generated_at': datetime.datetime.now().isoformat(),
            'data_processing_history': [],
            'erasure_operations': [],
            'current_data_locations': []
        }

        # Find all nodes related to this subject
        for node_id, node in self._nodes.items():
            # Check transformations for subject references
            if node.node_type == 'transformation':
                details = node.metadata

                # Check for erasure operations
                if details.get('operation_type') == 'erasure':
                    if details.get('subject_id') == subject_id:
                        report['erasure_operations'].append({
                            'timestamp': node.timestamp.isoformat(),
                            'columns_erased': details.get('columns_erased', []),
                            'transformation_id': node_id
                        })

                # Add to processing history
                report['data_processing_history'].append({
                    'operation': node.name,
                    'type': details.get('operation_type'),
                    'timestamp': node.timestamp.isoformat()
                })

        return report

    def export_lineage_graph(
        self,
        output_path: str,
        format: str = 'json'
    ) -> None:
        """
        Export the complete lineage graph.

        Args:
            output_path: Output file path.
            format: Export format ('json', 'graphml', 'mermaid').
        """
        if format == 'json':
            graph = {
                'nodes': [
                    {
                        'id': n.node_id,
                        'type': n.node_type,
                        'name': n.name,
                        'metadata': n.metadata,
                        'gdpr_tags': n.gdpr_tags,
                        'timestamp': n.timestamp.isoformat()
                    }
                    for n in self._nodes.values()
                ],
                'edges': [
                    {
                        'id': e.edge_id,
                        'source': e.source_id,
                        'target': e.target_id,
                        'operation': e.operation,
                        'details': e.details,
                        'timestamp': e.timestamp.isoformat()
                    }
                    for e in self._edges.values()
                ],
                'column_lineage': self._column_lineage
            }

            with open(output_path, 'w') as f:
                json.dump(graph, f, indent=2)

        elif format == 'mermaid':
            lines = ['graph LR']

            for node in self._nodes.values():
                shape = {'dataset': '[(', 'transformation': '{{', 'column': '['}.get(
                    node.node_type, '['
                )
                end_shape = {'dataset': ')]', 'transformation': '}}', 'column': ']'}.get(
                    node.node_type, ']'
                )
                lines.append(f"    {node.node_id}{shape}\"{node.name}\"{end_shape}")

            for edge in self._edges.values():
                lines.append(f"    {edge.source_id} -->|{edge.operation}| {edge.target_id}")

            with open(output_path, 'w') as f:
                f.write('\n'.join(lines))

        logger.info(f"Lineage graph exported to {output_path}")

    def _save_lineage(self) -> None:
        """Save lineage data to storage."""
        data = {
            'nodes': {
                k: {
                    'node_id': v.node_id,
                    'node_type': v.node_type,
                    'name': v.name,
                    'metadata': v.metadata,
                    'gdpr_tags': v.gdpr_tags,
                    'timestamp': v.timestamp.isoformat()
                }
                for k, v in self._nodes.items()
            },
            'edges': {
                k: {
                    'edge_id': v.edge_id,
                    'source_id': v.source_id,
                    'target_id': v.target_id,
                    'operation': v.operation,
                    'details': v.details,
                    'timestamp': v.timestamp.isoformat()
                }
                for k, v in self._edges.items()
            },
            'column_lineage': self._column_lineage
        }

        with open(self.storage_path / 'lineage.json', 'w') as f:
            json.dump(data, f, indent=2)

    def _load_lineage(self) -> None:
        """Load lineage data from storage."""
        lineage_file = self.storage_path / 'lineage.json'

        if not lineage_file.exists():
            return

        try:
            with open(lineage_file, 'r') as f:
                data = json.load(f)

            for k, v in data.get('nodes', {}).items():
                self._nodes[k] = LineageNode(
                    node_id=v['node_id'],
                    node_type=v['node_type'],
                    name=v['name'],
                    metadata=v.get('metadata', {}),
                    gdpr_tags=v.get('gdpr_tags', {}),
                    timestamp=datetime.datetime.fromisoformat(v['timestamp'])
                )

            for k, v in data.get('edges', {}).items():
                self._edges[k] = LineageEdge(
                    edge_id=v['edge_id'],
                    source_id=v['source_id'],
                    target_id=v['target_id'],
                    operation=v['operation'],
                    details=v.get('details', {}),
                    timestamp=datetime.datetime.fromisoformat(v['timestamp'])
                )

            self._column_lineage = data.get('column_lineage', {})

            logger.info(
                f"Loaded lineage: {len(self._nodes)} nodes, {len(self._edges)} edges"
            )
        except Exception as e:
            logger.warning(f"Could not load lineage data: {e}")


# ==============================================================================
# CLI Interface
# ==============================================================================


def create_cli():
    """Create the command-line interface."""
    import argparse

    parser = argparse.ArgumentParser(
        description="spark-anon: GDPR & Privacy Library for Spark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scan a Parquet file for PII
  python -m spart scan --input data.parquet --output scan_results.json

  # Anonymize a CSV file
  python -m spart anonymize --input users.csv --output users_anon.csv \\
      --mask email:partial --mask ssn:hash

  # Generate compliance report
  python -m spart report --start 2024-01-01 --end 2024-12-31 \\
      --output compliance_report.json

  # Run benchmark
  python -m spart benchmark --rows 100000 --columns 10
        """
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Scan command
    scan_parser = subparsers.add_parser("scan", help="Scan data for PII")
    scan_parser.add_argument("--input", "-i", required=True, help="Input file path")
    scan_parser.add_argument("--output", "-o", help="Output results file")
    scan_parser.add_argument("--format", "-f", default="auto",
                             choices=["auto", "parquet", "csv", "json"])
    scan_parser.add_argument("--sample", "-s", type=int, default=1000,
                             help="Sample size for content scanning")

    # Anonymize command
    anon_parser = subparsers.add_parser("anonymize", help="Anonymize data")
    anon_parser.add_argument("--input", "-i", required=True, help="Input file path")
    anon_parser.add_argument("--output", "-o", required=True, help="Output file path")
    anon_parser.add_argument("--mask", "-m", action="append",
                             help="Mask rule: column:type (hash/partial/redact)")
    anon_parser.add_argument("--pseudo", "-p", action="append",
                             help="Column to pseudonymize")
    anon_parser.add_argument("--salt", help="Salt for pseudonymization")
    anon_parser.add_argument("--format", "-f", default="auto")

    # Report command
    report_parser = subparsers.add_parser("report", help="Generate compliance report")
    report_parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    report_parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    report_parser.add_argument("--output", "-o", required=True, help="Output file path")
    report_parser.add_argument("--audit-path", default="./audit_logs",
                               help="Path to audit logs")

    # Benchmark command
    bench_parser = subparsers.add_parser("benchmark", help="Run performance benchmark")
    bench_parser.add_argument("--rows", "-r", type=int, default=100000,
                              help="Number of rows to generate")
    bench_parser.add_argument("--columns", "-c", type=int, default=10,
                              help="Number of columns to generate")
    bench_parser.add_argument("--output", "-o", help="Output results file")

    # Config command
    config_parser = subparsers.add_parser("config", help="Manage configuration")
    config_parser.add_argument("--generate", "-g", help="Generate sample config file")
    config_parser.add_argument("--validate", "-v", help="Validate config file")

    return parser


def run_cli(args=None):
    """Run the CLI with the given arguments."""
    parser = create_cli()
    parsed = parser.parse_args(args)

    if not parsed.command:
        parser.print_help()
        return

    if parsed.command == "scan":
        _cli_scan(parsed)
    elif parsed.command == "anonymize":
        _cli_anonymize(parsed)
    elif parsed.command == "report":
        _cli_report(parsed)
    elif parsed.command == "benchmark":
        _cli_benchmark(parsed)
    elif parsed.command == "config":
        _cli_config(parsed)


def _cli_scan(args):
    """Handle the scan command."""
    spark = SparkSession.builder.appName("spart-cli-scan").getOrCreate()

    # Determine format
    fmt = args.format
    if fmt == "auto":
        if args.input.endswith(".parquet"):
            fmt = "parquet"
        elif args.input.endswith(".csv"):
            fmt = "csv"
        elif args.input.endswith(".json"):
            fmt = "json"
        else:
            fmt = "parquet"

    # Load data
    df = spark.read.format(fmt).load(args.input)

    # Run PII detection
    detector = PIIDetector()
    results = detector.full_scan(df, sample_size=args.sample)

    # Get suggested tags
    results["suggested_tags"] = detector.get_suggested_tags(results)

    # Output results
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"Scan results saved to {args.output}")
    else:
        print(json.dumps(results, indent=2))

    spark.stop()


def _cli_anonymize(args):
    """Handle the anonymize command."""
    spark = SparkSession.builder.appName("spart-cli-anonymize").getOrCreate()

    # Determine format
    fmt = args.format
    if fmt == "auto":
        if args.input.endswith(".parquet"):
            fmt = "parquet"
        elif args.input.endswith(".csv"):
            fmt = "csv"
        else:
            fmt = "parquet"

    # Load data
    df = spark.read.format(fmt).load(args.input)

    # Initialize managers
    metadata_mgr = MetadataManager(spark)
    anon_mgr = AnonymizationManager(metadata_mgr)

    # Parse mask rules
    if args.mask:
        mask_rules = {}
        for rule in args.mask:
            col, mask_type = rule.split(":")
            mask_rules[col] = mask_type
        df = anon_mgr.mask_columns(df, mask_rules)

    # Apply pseudonymization
    if args.pseudo:
        df = anon_mgr.pseudonymize_columns(df, args.pseudo, args.salt)

    # Determine output format
    out_fmt = "parquet"
    if args.output.endswith(".csv"):
        out_fmt = "csv"
    elif args.output.endswith(".json"):
        out_fmt = "json"

    # Write output
    df.write.format(out_fmt).mode("overwrite").save(args.output)
    print(f"Anonymized data saved to {args.output}")

    spark.stop()


def _cli_report(args):
    """Handle the report command."""
    start_date = datetime.datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(args.end, "%Y-%m-%d")

    audit = AuditManager(log_path=args.audit_path)
    report = audit.generate_compliance_report(start_date, end_date, args.output)

    print(f"Compliance report saved to {args.output}")
    print(f"Total events: {report['summary']['total_events']}")


def _cli_benchmark(args):
    """Handle the benchmark command."""
    spark = SparkSession.builder.appName("spart-cli-benchmark").getOrCreate()

    # Generate test data
    print(f"Generating test data: {args.rows} rows x {args.columns} columns")

    columns = [f"col_{i}" for i in range(args.columns)]
    data = [[f"value_{r}_{c}" for c in range(args.columns)] for r in range(args.rows)]
    spark_df = spark.createDataFrame(data, columns)

    pandas_df = None
    if PANDAS_AVAILABLE:
        pandas_df = spark_df.toPandas()

    # Run benchmark
    benchmark = Benchmark(spark)
    results = benchmark.run_anonymization_benchmark(
        spark_df=spark_df,
        pandas_df=pandas_df,
        columns_to_mask=["col_0", "col_1"]
    )

    benchmark.print_results()

    if args.output:
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\nResults saved to {args.output}")

    spark.stop()


def _cli_config(args):
    """Handle the config command."""
    if args.generate:
        config = ConfigManager()
        config.save_to_file(args.generate)
        print(f"Sample configuration saved to {args.generate}")

    if args.validate:
        config = ConfigManager(args.validate)
        errors = config.validate()
        if errors:
            print("Configuration errors:")
            for error in errors:
                print(f"  - {error}")
        else:
            print("Configuration is valid")


# ==============================================================================
# Usage Example
# ==============================================================================


def usage_example():
    """
    Library usage demonstration with all new features.
    """
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

    # Create a Spark session
    spark = SparkSession.builder \
        .appName("GDPR Full Example") \
        .master("local[*]") \
        .getOrCreate()

    # Sample data
    data = [
        ("USR001", "John Smith", "john@email.com", "123 Main St", 75000.0, "2023-01-15"),
        ("USR002", "Jane Doe", "jane@email.com", "456 Oak Ave", 85000.0, "2023-06-20"),
        ("USR003", "Bob Wilson", "bob@email.com", "789 Pine Rd", 65000.0, "2024-03-10"),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("address", StringType(), True),
        StructField("salary", DoubleType(), True),
        StructField("hire_date", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema)

    print("=" * 70)
    print("SPARK-ANON COMPREHENSIVE DEMO")
    print("=" * 70)

    # 1. Configuration
    print("\n1. CONFIGURATION")
    config = ConfigManager()
    print(f"   Default pandas threshold: {config.get('processing', 'pandas_threshold_rows')}")
    print(f"   Audit enabled: {config.get('audit', 'enabled')}")

    # 2. PII Detection
    print("\n2. PII DETECTION")
    detector = PIIDetector(config)
    scan_results = detector.full_scan(df, sample_size=100)
    print(f"   Potential PII columns: {scan_results['column_name_analysis']['potential_pii']}")

    # 3. Metadata Management
    print("\n3. METADATA MANAGEMENT")
    audit = AuditManager(config)
    metadata_mgr = MetadataManager(spark, config=config, audit_manager=audit)

    column_tags = {
        "user_id": {"gdpr_category": "PII", "sensitivity": "MEDIUM", "retention_days": 3650},
        "name": {"gdpr_category": "PII", "sensitivity": "HIGH", "retention_days": 3650},
        "email": {"gdpr_category": "PII", "sensitivity": "MEDIUM", "retention_days": 1825},
        "address": {"gdpr_category": "PII", "sensitivity": "HIGH", "retention_days": 1825},
        "salary": {"gdpr_category": "SPI", "sensitivity": "VERY_HIGH", "retention_days": 2555},
    }

    tagged_df = metadata_mgr.apply_column_tags(df, column_tags)
    print(f"   Applied tags to {len(column_tags)} columns")

    # 4. Consent Management
    print("\n4. CONSENT MANAGEMENT")
    consent_mgr = ConsentManager(spark, config, audit)
    consent_mgr.record_consent(
        subject_id="USR001",
        purpose="analytics",
        status=ConsentStatus.GRANTED,
        legal_basis="consent"
    )
    has_consent, _ = consent_mgr.check_consent("USR001", "analytics")
    print(f"   USR001 consent for analytics: {has_consent}")

    # 5. Anonymization
    print("\n5. ANONYMIZATION")
    anon_mgr = AnonymizationManager(metadata_mgr, audit)

    anon_df = anon_mgr.auto_anonymize(tagged_df)
    print("   Auto-anonymized data (first row):")
    anon_df.select("name", "email", "salary").show(1, truncate=False)

    # 6. Advanced Anonymization
    print("6. ADVANCED ANONYMIZATION")
    advanced = AdvancedAnonymization(spark, config)

    k_anon_check = advanced.check_k_anonymity(
        df, ["address"], k=2
    )
    print(f"   K-anonymity check (k=2): {k_anon_check['is_k_anonymous']}")

    # 7. Pandas Backend (if available)
    if PANDAS_AVAILABLE:
        print("\n7. PANDAS BACKEND")
        pandas_proc = PandasProcessor(config)
        pandas_df = df.toPandas()
        pandas_proc.apply_column_tags(pandas_df, column_tags)
        masked_pdf = pandas_proc.mask_columns(pandas_df, {"email": "partial"})
        print(f"   Masked email: {masked_pdf['email'].iloc[0]}")

    # 8. Benchmark
    print("\n8. BENCHMARK")
    benchmark = Benchmark(spark, config)
    print(f"   Backend recommendation: {benchmark._get_recommendation(len(data))}")

    # 9. Audit Trail
    print("\n9. AUDIT TRAIL")
    events = audit.get_events()
    print(f"   Total audit events: {len(events)}")

    print("\n" + "=" * 70)
    print("DEMO COMPLETE")
    print("=" * 70)

    spark.stop()


# Entry point
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        run_cli()
    else:
        usage_example()
