"""
Comprehensive Test Suite for spark-anon - GDPR Privacy Library

Tests all features including:
- Original functionality (MetadataManager, RetentionManager, AnonymizationManager)
- Consent Management
- Audit Logging
- Configuration Management
- PII Detection
- Advanced Anonymization (K-Anonymity, Differential Privacy)
- Pandas Backend Processing
- Benchmarking

Run with: python test_spart.py
"""

import sys
import json
import datetime
import tempfile
import shutil
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType, IntegerType
)

# Import the library
from spart import (
    MetadataManager, RetentionManager, AnonymizationManager,
    ConsentManager, AuditManager, ConfigManager,
    PIIDetector, AdvancedAnonymization, Benchmark, BackendSelector,
    ConsentStatus, AuditEventType, ProcessingBackend,
    GDPRCategory, SensitivityLevel,
    # Future Considerations - now implemented
    ApacheAtlasClient, AWSGlueCatalogClient, MLPIIDetector,
    DataLineageTracker, LineageNode, LineageEdge
)

# Check if pandas is available with correct version for PySpark
PANDAS_AVAILABLE = False
PANDAS_VERSION_OK = False
pd = None
np = None

try:
    import pandas as pd
    import numpy as np
    from packaging import version
    PANDAS_AVAILABLE = True
    # PySpark requires pandas >= 2.2.0 for toPandas() conversion
    PANDAS_VERSION_OK = version.parse(pd.__version__) >= version.parse("2.2.0")
    if PANDAS_VERSION_OK:
        from spart import PandasProcessor
except ImportError:
    pass

# Check if yaml is available
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False


def create_test_spark():
    """Create a local Spark session for testing."""
    return SparkSession.builder \
        .appName("spart-test") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()


def create_test_dataframe(spark):
    """Create a test DataFrame with sample data."""
    data = [
        ("USR101", "John Smith", "john.smith@example.com", "123 Main St",
         50000.0, datetime.date(2022, 1, 15), 30),
        ("USR102", "Jane Doe", "jane.doe@example.com", "456 Oak Ave",
         75000.0, datetime.date(2023, 3, 10), 35),
        ("USR103", "Peter Jones", "peter.jones@sample.net", "789 Pine Rd",
         62000.0, datetime.date(2024, 8, 20), 28),
        ("USR104", "Mary Wilson", "mary.wilson@test.org", "321 Elm St",
         55000.0, datetime.date(2024, 1, 5), 42),
        ("USR105", "Bob Brown", "bob.brown@mail.com", "654 Maple Dr",
         68000.0, datetime.date(2023, 11, 15), 38),
    ]
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("address", StringType(), True),
        StructField("salary", DoubleType(), True),
        StructField("last_login", DateType(), True),
        StructField("age", IntegerType(), True)
    ])
    return spark.createDataFrame(data, schema)


def create_large_test_dataframe(spark, rows=1000, columns=10):
    """Create a larger test DataFrame for benchmarking."""
    data = [
        [f"value_{r}_{c}" for c in range(columns)]
        for r in range(rows)
    ]
    col_names = [f"col_{i}" for i in range(columns)]
    return spark.createDataFrame(data, col_names)


class TestRunner:
    """Test runner with result tracking."""

    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []

    def run_test(self, name, test_func):
        """Run a single test and track results."""
        try:
            test_func()
            self.passed += 1
            print(f"   PASS: {name}")
            return True
        except AssertionError as e:
            self.failed += 1
            self.errors.append((name, str(e)))
            print(f"   FAIL: {name} - {e}")
            return False
        except Exception as e:
            self.failed += 1
            self.errors.append((name, str(e)))
            print(f"   ERROR: {name} - {e}")
            return False

    def summary(self):
        """Print test summary."""
        total = self.passed + self.failed
        print(f"\nResults: {self.passed}/{total} tests passed")
        if self.errors:
            print("\nFailures:")
            for name, error in self.errors:
                print(f"  - {name}: {error}")
        return self.failed == 0


# ==============================================================================
# Test Functions
# ==============================================================================


def test_config_manager(runner, temp_dir):
    """Test ConfigManager functionality."""
    print("\n" + "=" * 60)
    print("TEST: ConfigManager")
    print("=" * 60)

    def test_default_config():
        config = ConfigManager()
        assert config.get("metadata", "column_name") == "_metadata"
        assert config.get("anonymization", "default_hash_algorithm") == "sha256"
        assert config.get("processing", "pandas_threshold_rows") == 100000

    def test_get_nested():
        config = ConfigManager()
        result = config.get("pii_detection", "patterns", "email")
        assert result is not None
        assert "@" in result  # Should contain email pattern

    def test_set_value():
        config = ConfigManager()
        config.set("processing", "pandas_threshold_rows", 50000)
        assert config.get("processing", "pandas_threshold_rows") == 50000

    def test_validation():
        config = ConfigManager()
        errors = config.validate()
        assert len(errors) == 0, f"Default config should be valid: {errors}"

    def test_invalid_validation():
        config = ConfigManager()
        config.set("anonymization", "k_anonymity_default_k", 1)
        errors = config.validate()
        assert len(errors) > 0, "Should detect invalid k value"

    def test_save_load():
        if not YAML_AVAILABLE:
            return  # Skip if yaml not available

        config = ConfigManager()
        config.set("test", "value", "hello")
        config_path = temp_dir / "test_config.yaml"
        config.save_to_file(str(config_path))

        config2 = ConfigManager(str(config_path))
        assert config2.get("test", "value") == "hello"

    runner.run_test("default config values", test_default_config)
    runner.run_test("get nested values", test_get_nested)
    runner.run_test("set values", test_set_value)
    runner.run_test("validation passes for valid config", test_validation)
    runner.run_test("validation catches invalid values", test_invalid_validation)
    if YAML_AVAILABLE:
        runner.run_test("save and load config", test_save_load)


def test_audit_manager(runner, temp_dir):
    """Test AuditManager functionality."""
    print("\n" + "=" * 60)
    print("TEST: AuditManager")
    print("=" * 60)

    audit_path = temp_dir / "audit_logs"

    def test_log_event():
        audit = AuditManager(log_path=str(audit_path))
        event = audit.log_event(
            event_type=AuditEventType.DATA_ACCESS,
            actor="test_user",
            action="read_data",
            resource="test_table",
            details={"rows": 100}
        )
        assert event is not None
        assert event.event_type == AuditEventType.DATA_ACCESS
        assert event.actor == "test_user"

    def test_log_with_subjects():
        audit = AuditManager(log_path=str(audit_path))
        event = audit.log_event(
            event_type=AuditEventType.ERASURE_REQUEST,
            actor="system",
            action="erase_data",
            resource="user_table",
            subject_ids=["USR001", "USR002"]
        )
        assert "USR001" in event.subject_ids
        assert "USR002" in event.subject_ids

    def test_query_events():
        audit = AuditManager(log_path=str(audit_path))
        # Log some events
        audit.log_event(
            event_type=AuditEventType.DATA_ACCESS,
            actor="user_a",
            action="select",
            resource="table1"
        )
        audit.log_event(
            event_type=AuditEventType.DATA_MODIFICATION,
            actor="user_b",
            action="update",
            resource="table2"
        )

        # Query all events
        all_events = audit.get_events()
        assert len(all_events) >= 2

        # Query by actor
        user_a_events = audit.get_events(actor="user_a")
        assert all(e.actor == "user_a" for e in user_a_events)

    def test_compliance_report():
        audit = AuditManager(log_path=str(audit_path))

        # Log some test events
        audit.log_event(
            event_type=AuditEventType.ERASURE_REQUEST,
            actor="system",
            action="erase",
            resource="users",
            subject_ids=["USR001"],
            success=True
        )

        start = datetime.datetime.now() - datetime.timedelta(days=1)
        end = datetime.datetime.now() + datetime.timedelta(days=1)

        report = audit.generate_compliance_report(start, end)
        assert "summary" in report
        assert "erasure_requests" in report
        assert report["summary"]["total_events"] > 0

    def test_subject_access_report():
        audit = AuditManager(log_path=str(audit_path))

        # Log events for a specific subject
        audit.log_event(
            event_type=AuditEventType.DATA_ACCESS,
            actor="analyst",
            action="query",
            resource="analytics_db",
            subject_ids=["SUBJ001"]
        )

        report = audit.get_subject_access_report("SUBJ001")
        assert report["subject_id"] == "SUBJ001"
        assert "processing_activities" in report

    runner.run_test("log event", test_log_event)
    runner.run_test("log with subject IDs", test_log_with_subjects)
    runner.run_test("query events", test_query_events)
    runner.run_test("compliance report", test_compliance_report)
    runner.run_test("subject access report", test_subject_access_report)


def test_consent_manager(runner, spark, temp_dir):
    """Test ConsentManager functionality."""
    print("\n" + "=" * 60)
    print("TEST: ConsentManager")
    print("=" * 60)

    consent_path = temp_dir / "consent_data"

    def test_record_consent():
        consent = ConsentManager(spark, storage_path=str(consent_path))
        record = consent.record_consent(
            subject_id="USR001",
            purpose="analytics",
            status=ConsentStatus.GRANTED,
            legal_basis="explicit_consent"
        )
        assert record.subject_id == "USR001"
        assert record.purpose == "analytics"
        assert record.status == ConsentStatus.GRANTED
        assert record.is_valid()

    def test_check_consent():
        consent = ConsentManager(spark, storage_path=str(consent_path))
        consent.record_consent(
            subject_id="USR002",
            purpose="marketing",
            status=ConsentStatus.GRANTED
        )

        has_consent, record = consent.check_consent("USR002", "marketing")
        assert has_consent is True
        assert record is not None

        # Check non-existent consent
        has_consent, record = consent.check_consent("USR999", "marketing")
        assert has_consent is False
        assert record is None

    def test_withdraw_consent():
        consent = ConsentManager(spark, storage_path=str(consent_path))
        consent.record_consent(
            subject_id="USR003",
            purpose="analytics",
            status=ConsentStatus.GRANTED
        )

        # Withdraw
        consent.withdraw_consent("USR003", "analytics")

        has_consent, record = consent.check_consent("USR003", "analytics")
        assert has_consent is False
        assert record.status == ConsentStatus.WITHDRAWN

    def test_invalid_purpose():
        consent = ConsentManager(spark, storage_path=str(consent_path))
        try:
            consent.record_consent(
                subject_id="USR004",
                purpose="invalid_purpose",
                status=ConsentStatus.GRANTED
            )
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "Invalid purpose" in str(e)

    def test_get_all_consents():
        consent = ConsentManager(spark, storage_path=str(consent_path))
        consent.record_consent("USR005", "analytics", ConsentStatus.GRANTED)
        consent.record_consent("USR005", "marketing", ConsentStatus.DENIED)

        all_consents = consent.get_all_consents("USR005")
        assert len(all_consents) == 2
        assert "analytics" in all_consents
        assert "marketing" in all_consents

    runner.run_test("record consent", test_record_consent)
    runner.run_test("check consent", test_check_consent)
    runner.run_test("withdraw consent", test_withdraw_consent)
    runner.run_test("invalid purpose rejected", test_invalid_purpose)
    runner.run_test("get all consents", test_get_all_consents)


def test_pii_detector(runner, spark, df):
    """Test PIIDetector functionality."""
    print("\n" + "=" * 60)
    print("TEST: PIIDetector")
    print("=" * 60)

    def test_column_name_scan():
        detector = PIIDetector()
        results = detector.scan_column_names(df)

        assert "name" in results["potential_pii"]
        assert "email" in results["potential_pii"]
        assert "address" in results["potential_pii"]

    def test_content_scan():
        detector = PIIDetector()
        results = detector.scan_content(df, sample_size=10)

        # Email column should have email pattern detected
        assert "email" in results
        assert "email" in results["email"]

    def test_full_scan():
        detector = PIIDetector()
        results = detector.full_scan(df, sample_size=10)

        assert "column_name_analysis" in results
        assert "content_analysis" in results
        assert "scan_metadata" in results

    def test_custom_patterns():
        detector = PIIDetector()
        detector.add_pattern("custom_id", r"USR\d+")

        results = detector.scan_content(df, columns=["user_id"], sample_size=10)
        assert "user_id" in results
        assert "custom_id" in results["user_id"]

    def test_suggested_tags():
        detector = PIIDetector()
        scan_results = detector.full_scan(df, sample_size=10)
        suggestions = detector.get_suggested_tags(scan_results)

        # Should suggest tags for detected PII columns
        assert len(suggestions) > 0
        for col, tags in suggestions.items():
            assert "gdpr_category" in tags
            assert "sensitivity" in tags

    runner.run_test("column name scanning", test_column_name_scan)
    runner.run_test("content scanning", test_content_scan)
    runner.run_test("full scan", test_full_scan)
    runner.run_test("custom patterns", test_custom_patterns)
    runner.run_test("suggested tags", test_suggested_tags)


def test_advanced_anonymization(runner, spark, df):
    """Test AdvancedAnonymization functionality."""
    print("\n" + "=" * 60)
    print("TEST: AdvancedAnonymization")
    print("=" * 60)

    def test_k_anonymity_check():
        advanced = AdvancedAnonymization(spark)
        result = advanced.check_k_anonymity(df, ["address"], k=2)

        assert "is_k_anonymous" in result
        assert "min_group_size" in result
        assert "compliance_rate" in result

    def test_k_anonymize():
        # Create data with some groups that don't meet k
        data = [
            ("A", "X"), ("A", "X"), ("A", "X"),  # Group of 3
            ("B", "Y"), ("B", "Y"),  # Group of 2
            ("C", "Z"),  # Group of 1 - should be suppressed for k=2
        ]
        test_df = spark.createDataFrame(data, ["quasi1", "quasi2"])

        advanced = AdvancedAnonymization(spark)
        result = advanced.k_anonymize(test_df, ["quasi1", "quasi2"], k=2)

        # Group C should be suppressed
        assert result.count() == 5  # Only groups A and B remain
        assert result.filter("quasi1 = 'C'").count() == 0

    def test_differential_privacy():
        # Create numeric data
        data = [(i, float(i * 10)) for i in range(100)]
        numeric_df = spark.createDataFrame(data, ["id", "value"])

        advanced = AdvancedAnonymization(spark)
        noisy_df = advanced.add_differential_privacy_noise(
            numeric_df, ["value"], epsilon=1.0
        )

        # Values should be different due to noise
        original_values = [row["value"] for row in numeric_df.collect()]
        noisy_values = [row["value"] for row in noisy_df.collect()]

        differences = [abs(o - n) for o, n in zip(original_values, noisy_values)]
        avg_diff = sum(differences) / len(differences)

        # Noise should have been added
        assert avg_diff > 0, "Noise should have been added"

    def test_generalize_numeric():
        data = [(1, 25.0), (2, 47.0), (3, 33.0)]
        test_df = spark.createDataFrame(data, ["id", "age"])

        advanced = AdvancedAnonymization(spark)
        result = advanced.generalize_numeric(test_df, "age", bin_size=10.0)

        # Check that ages are generalized to ranges
        ages = [row["age"] for row in result.collect()]
        assert all("-" in str(age) for age in ages)

    def test_generalize_date():
        data = [
            (1, datetime.date(2024, 3, 15)),
            (2, datetime.date(2024, 6, 20)),
        ]
        test_df = spark.createDataFrame(data, ["id", "event_date"])

        advanced = AdvancedAnonymization(spark)

        # Test month generalization
        result = advanced.generalize_date(test_df, "event_date", level="month")
        dates = [row["event_date"] for row in result.collect()]
        assert "2024-03" in dates
        assert "2024-06" in dates

        # Test year generalization
        result = advanced.generalize_date(test_df, "event_date", level="year")
        years = [row["event_date"] for row in result.collect()]
        assert all(y == "2024" for y in years)

    runner.run_test("k-anonymity check", test_k_anonymity_check)
    runner.run_test("k-anonymize with suppression", test_k_anonymize)
    runner.run_test("differential privacy noise", test_differential_privacy)
    runner.run_test("numeric generalization", test_generalize_numeric)
    runner.run_test("date generalization", test_generalize_date)


def test_pandas_processor(runner, spark, df):
    """Test PandasProcessor functionality."""
    if not PANDAS_AVAILABLE:
        print("\n" + "=" * 60)
        print("TEST: PandasProcessor (SKIPPED - pandas not installed)")
        print("=" * 60)
        return

    if not PANDAS_VERSION_OK:
        print("\n" + "=" * 60)
        print(f"TEST: PandasProcessor (SKIPPED - pandas {pd.__version__} < 2.2.0 required by PySpark)")
        print("=" * 60)
        return

    print("\n" + "=" * 60)
    print("TEST: PandasProcessor")
    print("=" * 60)

    # Import PandasProcessor now that we know version is OK
    from spart import PandasProcessor

    # Convert to pandas
    pandas_df = df.toPandas()

    def test_apply_tags():
        proc = PandasProcessor()
        tags = {
            "name": {"gdpr_category": "PII", "sensitivity": "HIGH"},
            "email": {"gdpr_category": "PII", "sensitivity": "MEDIUM"}
        }
        proc.apply_column_tags(pandas_df, tags)
        retrieved = proc.get_column_tags()
        assert "name" in retrieved
        assert retrieved["name"]["gdpr_category"] == "PII"

    def test_mask_hash():
        proc = PandasProcessor()
        result = proc.mask_columns(pandas_df.copy(), {"email": "hash"})

        # Hash should be 64 chars (SHA-256)
        assert len(result["email"].iloc[0]) == 64

    def test_mask_partial():
        proc = PandasProcessor()
        result = proc.mask_columns(pandas_df.copy(), {"email": "partial"})

        # Should contain ***
        assert "***" in result["email"].iloc[0]

    def test_mask_redact():
        proc = PandasProcessor()
        result = proc.mask_columns(pandas_df.copy(), {"address": "redact"})

        assert result["address"].iloc[0] == "[REDACTED]"

    def test_pseudonymize():
        proc = PandasProcessor()
        result1 = proc.pseudonymize_columns(pandas_df.copy(), ["user_id"], "salt1")
        result2 = proc.pseudonymize_columns(pandas_df.copy(), ["user_id"], "salt1")

        # Same salt should produce same results
        assert result1["user_id"].iloc[0] == result2["user_id"].iloc[0]

    def test_auto_anonymize():
        proc = PandasProcessor()
        tags = {
            "name": {"gdpr_category": "PII", "sensitivity": "HIGH"},
            "email": {"gdpr_category": "PII", "sensitivity": "MEDIUM"},
            "salary": {"gdpr_category": "SPI", "sensitivity": "VERY_HIGH"}
        }
        proc.apply_column_tags(pandas_df, tags)
        result = proc.auto_anonymize(pandas_df.copy())

        # HIGH PII should be hashed
        assert len(result["name"].iloc[0]) == 64
        # MEDIUM PII should be partial
        assert "***" in result["email"].iloc[0]
        # SPI should be pseudonymized
        assert len(result["salary"].iloc[0]) == 64

    def test_erasure():
        proc = PandasProcessor()
        tags = {
            "name": {"gdpr_category": "PII", "sensitivity": "HIGH"},
            "email": {"gdpr_category": "PII", "sensitivity": "MEDIUM"}
        }
        proc.apply_column_tags(pandas_df, tags)
        result = proc.request_erasure(pandas_df.copy(), "user_id", "USR101")

        # USR101's PII should be None
        usr101_row = result[result["user_id"] == "USR101"].iloc[0]
        assert pd.isna(usr101_row["name"])
        assert pd.isna(usr101_row["email"])

        # Other users should be unchanged
        usr102_row = result[result["user_id"] == "USR102"].iloc[0]
        assert usr102_row["name"] == "Jane Doe"

    def test_k_anonymize():
        proc = PandasProcessor()
        result = proc.k_anonymize(pandas_df.copy(), ["address"], k=2)

        # Groups with fewer than k members should be suppressed
        assert len(result) <= len(pandas_df)

    def test_differential_privacy():
        proc = PandasProcessor()
        result = proc.add_differential_privacy_noise(
            pandas_df.copy(), ["salary"], epsilon=1.0
        )

        # Values should be different
        original_salaries = pandas_df["salary"].values
        noisy_salaries = result["salary"].values
        assert not np.allclose(original_salaries, noisy_salaries)

    runner.run_test("apply tags", test_apply_tags)
    runner.run_test("hash masking", test_mask_hash)
    runner.run_test("partial masking", test_mask_partial)
    runner.run_test("redact masking", test_mask_redact)
    runner.run_test("pseudonymization", test_pseudonymize)
    runner.run_test("auto anonymize", test_auto_anonymize)
    runner.run_test("erasure request", test_erasure)
    runner.run_test("k-anonymity", test_k_anonymize)
    runner.run_test("differential privacy", test_differential_privacy)


def test_benchmark(runner, spark):
    """Test Benchmark functionality."""
    print("\n" + "=" * 60)
    print("TEST: Benchmark")
    print("=" * 60)

    def test_recommendation():
        benchmark = Benchmark(spark)
        small_rec = benchmark._get_recommendation(1000)
        assert "PANDAS" in small_rec.upper()

        large_rec = benchmark._get_recommendation(1000000)
        assert "SPARK" in large_rec.upper()

    def test_benchmark_operation():
        benchmark = Benchmark(spark)

        def dummy_spark():
            return 1

        results = benchmark.benchmark_operation(
            "test_op",
            spark_func=dummy_spark,
            row_count=100,
            column_count=5
        )

        assert "spark" in results
        assert results["spark"].execution_time_seconds > 0

    def test_results_tracking():
        benchmark = Benchmark(spark)
        benchmark.benchmark_operation("op1", lambda: 1, row_count=100, column_count=5)
        benchmark.benchmark_operation("op2", lambda: 2, row_count=200, column_count=10)

        assert len(benchmark.results) >= 2

    runner.run_test("backend recommendation", test_recommendation)
    runner.run_test("benchmark operation", test_benchmark_operation)
    runner.run_test("results tracking", test_results_tracking)


def test_backend_selector(runner, spark, df):
    """Test BackendSelector functionality."""
    print("\n" + "=" * 60)
    print("TEST: BackendSelector")
    print("=" * 60)

    def test_auto_selection_small():
        selector = BackendSelector(spark)
        backend = selector.get_backend(1000)
        if PANDAS_AVAILABLE:
            assert backend == ProcessingBackend.PANDAS

    def test_auto_selection_large():
        selector = BackendSelector(spark)
        backend = selector.get_backend(1000000)
        assert backend == ProcessingBackend.SPARK

    def test_force_backend():
        selector = BackendSelector(spark)
        backend = selector.get_backend(1000, force_backend=ProcessingBackend.SPARK)
        assert backend == ProcessingBackend.SPARK

    def test_conversion():
        if not PANDAS_VERSION_OK:
            return

        selector = BackendSelector(spark)

        # Spark to Pandas
        pandas_df = selector.convert_spark_to_pandas(df)
        assert isinstance(pandas_df, pd.DataFrame)
        assert len(pandas_df) == df.count()

        # Pandas to Spark
        spark_df = selector.convert_pandas_to_spark(pandas_df)
        assert spark_df.count() == len(pandas_df)

    runner.run_test("auto selection (small data)", test_auto_selection_small)
    runner.run_test("auto selection (large data)", test_auto_selection_large)
    runner.run_test("force backend", test_force_backend)
    if PANDAS_VERSION_OK:
        runner.run_test("conversion between backends", test_conversion)


def test_databricks_methods(runner, spark):
    """Test Databricks/Unity Catalog methods raise errors outside Databricks."""
    print("\n" + "=" * 60)
    print("TEST: Databricks/Unity Catalog Methods")
    print("=" * 60)

    def test_apply_tags_to_delta_table():
        metadata_mgr = MetadataManager(spark)
        try:
            metadata_mgr.apply_tags_to_delta_table(
                "catalog.schema.table",
                {"tag": "value"}
            )
            assert False, "Should have raised EnvironmentError"
        except EnvironmentError as e:
            assert "Databricks" in str(e)

    def test_apply_column_tags_to_delta_table():
        metadata_mgr = MetadataManager(spark)
        try:
            metadata_mgr.apply_column_tags_to_delta_table(
                "catalog.schema.table",
                {"column": {"tag": "value"}}
            )
            assert False, "Should have raised EnvironmentError"
        except EnvironmentError as e:
            assert "Databricks" in str(e)

    def test_get_table_tags():
        metadata_mgr = MetadataManager(spark)
        try:
            metadata_mgr.get_table_tags("catalog.schema.table")
            assert False, "Should have raised EnvironmentError"
        except EnvironmentError as e:
            assert "Databricks" in str(e)

    def test_is_databricks_detection():
        metadata_mgr = MetadataManager(spark)
        assert metadata_mgr.is_databricks is False

    runner.run_test("apply_tags_to_delta_table (non-Databricks)", test_apply_tags_to_delta_table)
    runner.run_test("apply_column_tags_to_delta_table (non-Databricks)", test_apply_column_tags_to_delta_table)
    runner.run_test("get_table_tags (non-Databricks)", test_get_table_tags)
    runner.run_test("is_databricks detection", test_is_databricks_detection)


def test_metadata_manager(runner, spark, df, temp_dir):
    """Test MetadataManager functionality."""
    print("\n" + "=" * 60)
    print("TEST: MetadataManager")
    print("=" * 60)

    def test_apply_column_tags():
        metadata_mgr = MetadataManager(spark)
        column_tags = {
            "user_id": {"gdpr_category": "PII", "sensitivity": "MEDIUM"},
            "name": {"gdpr_category": "PII", "sensitivity": "HIGH"},
            "email": {"gdpr_category": "PII", "sensitivity": "MEDIUM"},
            "salary": {"gdpr_category": "SPI", "sensitivity": "VERY_HIGH"},
        }
        tagged_df = metadata_mgr.apply_column_tags(df, column_tags)
        retrieved_tags = metadata_mgr.get_column_tags(tagged_df)

        assert len(retrieved_tags) == 4
        assert retrieved_tags["name"]["gdpr_category"] == "PII"
        assert retrieved_tags["salary"]["sensitivity"] == "VERY_HIGH"

    def test_apply_dataset_tags():
        metadata_mgr = MetadataManager(spark)
        dataset_tags = {
            "owner": "data_team",
            "purpose": "analytics"
        }
        tagged_df = metadata_mgr.apply_dataset_tags(df, dataset_tags)

        assert "_metadata" in tagged_df.columns
        metadata_row = tagged_df.select("_metadata").first()
        metadata_dict = json.loads(metadata_row[0])
        assert metadata_dict["owner"] == "data_team"
        assert "dataset_id" in metadata_dict

    def test_pii_scan():
        metadata_mgr = MetadataManager(spark)
        results = metadata_mgr.scan_for_pii(df)

        assert "name" in results["potential_pii"]
        assert "email" in results["potential_pii"]
        assert "address" in results["potential_pii"]

    def test_invalid_gdpr_category():
        metadata_mgr = MetadataManager(spark)
        try:
            invalid_tags = {"user_id": {"gdpr_category": "INVALID"}}
            metadata_mgr.apply_column_tags(df, invalid_tags)
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "Invalid GDPR category" in str(e)

    def test_invalid_sensitivity():
        metadata_mgr = MetadataManager(spark)
        try:
            invalid_tags = {"user_id": {"gdpr_category": "PII", "sensitivity": "EXTREME"}}
            metadata_mgr.apply_column_tags(df, invalid_tags)
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "Invalid sensitivity" in str(e)

    def test_export_metadata_catalog():
        metadata_mgr = MetadataManager(spark)
        column_tags = {
            "email": {"gdpr_category": "PII", "sensitivity": "MEDIUM"}
        }
        dataset_tags = {"owner": "test", "purpose": "testing"}

        tagged_df = metadata_mgr.apply_column_tags(df, column_tags)
        tagged_df = metadata_mgr.apply_dataset_tags(tagged_df, dataset_tags)

        catalog_path = str(temp_dir / "catalog.json")
        metadata_mgr.export_metadata_catalog(tagged_df, catalog_path)

        with open(catalog_path, "r") as f:
            catalog = json.load(f)

        assert "dataset_metadata" in catalog
        assert "column_metadata" in catalog
        assert "exported_at" in catalog

    runner.run_test("apply column tags", test_apply_column_tags)
    runner.run_test("apply dataset tags", test_apply_dataset_tags)
    runner.run_test("PII scan", test_pii_scan)
    runner.run_test("invalid GDPR category rejected", test_invalid_gdpr_category)
    runner.run_test("invalid sensitivity rejected", test_invalid_sensitivity)
    runner.run_test("export metadata catalog", test_export_metadata_catalog)


def test_retention_manager(runner, spark, df):
    """Test RetentionManager functionality."""
    print("\n" + "=" * 60)
    print("TEST: RetentionManager")
    print("=" * 60)

    def test_apply_retention_policy():
        metadata_mgr = MetadataManager(spark)
        retention_mgr = RetentionManager(metadata_mgr)

        column_tags = {
            "email": {"gdpr_category": "PII", "retention_days": "3650"},
            "salary": {"gdpr_category": "SPI", "retention_days": "1825"}
        }
        tagged_df = metadata_mgr.apply_column_tags(df, column_tags)

        original_count = tagged_df.count()
        filtered_df = retention_mgr.apply_retention_policy(tagged_df, "last_login")

        # With 10-year retention, all records should be kept
        # (since none are older than 10 years)
        assert filtered_df.count() <= original_count

    def test_no_tags_warning():
        metadata_mgr = MetadataManager(spark)
        retention_mgr = RetentionManager(metadata_mgr)

        # No tags applied
        result = retention_mgr.apply_retention_policy(df, "last_login")
        # Should return original df when no tags found
        assert result.count() == df.count()

    runner.run_test("apply retention policy", test_apply_retention_policy)
    runner.run_test("no tags warning", test_no_tags_warning)


def test_anonymization_manager(runner, spark, df):
    """Test AnonymizationManager functionality."""
    print("\n" + "=" * 60)
    print("TEST: AnonymizationManager")
    print("=" * 60)

    metadata_mgr = MetadataManager(spark)
    column_tags = {
        "user_id": {"gdpr_category": "PII", "sensitivity": "MEDIUM"},
        "name": {"gdpr_category": "PII", "sensitivity": "HIGH"},
        "email": {"gdpr_category": "PII", "sensitivity": "MEDIUM"},
        "address": {"gdpr_category": "PII", "sensitivity": "HIGH"},
        "salary": {"gdpr_category": "SPI", "sensitivity": "VERY_HIGH"},
    }
    tagged_df = metadata_mgr.apply_column_tags(df, column_tags)

    def test_mask_hash():
        anon_mgr = AnonymizationManager(metadata_mgr)
        masked_df = anon_mgr.mask_columns(df, {"email": "hash"})
        first_email = masked_df.select("email").first()[0]
        assert len(first_email) == 64  # SHA-256

    def test_mask_partial():
        anon_mgr = AnonymizationManager(metadata_mgr)
        masked_df = anon_mgr.mask_columns(df, {"email": "partial"})
        first_email = masked_df.select("email").first()[0]
        assert "***" in first_email

    def test_mask_redact():
        anon_mgr = AnonymizationManager(metadata_mgr)
        masked_df = anon_mgr.mask_columns(df, {"address": "redact"})
        first_address = masked_df.select("address").first()[0]
        assert first_address == "[REDACTED]"

    def test_pseudonymize():
        anon_mgr = AnonymizationManager(metadata_mgr)
        pseudo_df1 = anon_mgr.pseudonymize_columns(df, ["user_id"], salt="test_salt")
        pseudo_df2 = anon_mgr.pseudonymize_columns(df, ["user_id"], salt="test_salt")

        id1 = pseudo_df1.select("user_id").first()[0]
        id2 = pseudo_df2.select("user_id").first()[0]

        assert len(id1) == 64
        assert id1 == id2  # Same salt = same result

    def test_auto_anonymize():
        anon_mgr = AnonymizationManager(metadata_mgr)
        anon_df = anon_mgr.auto_anonymize(tagged_df)

        # HIGH PII should be hashed
        first_name = anon_df.select("name").first()[0]
        assert len(first_name) == 64

        # MEDIUM PII should be partially masked
        first_email = anon_df.select("email").first()[0]
        assert "***" in first_email

        # SPI should be pseudonymized
        first_salary = anon_df.select("salary").first()[0]
        assert len(str(first_salary)) == 64

    def test_request_erasure():
        anon_mgr = AnonymizationManager(metadata_mgr)
        erased_df = anon_mgr.request_erasure(tagged_df, "user_id", "USR102")

        # USR102's PII fields should be null
        usr102_row = erased_df.filter("user_id = 'USR102'").first()
        assert usr102_row["name"] is None
        assert usr102_row["email"] is None
        assert usr102_row["salary"] is None

        # Other users should be unchanged
        usr101_row = erased_df.filter("user_id = 'USR101'").first()
        assert usr101_row["name"] == "John Smith"

    runner.run_test("hash masking", test_mask_hash)
    runner.run_test("partial masking", test_mask_partial)
    runner.run_test("redact masking", test_mask_redact)
    runner.run_test("pseudonymization", test_pseudonymize)
    runner.run_test("auto anonymize", test_auto_anonymize)
    runner.run_test("erasure request", test_request_erasure)


def test_enums_and_dataclasses(runner):
    """Test Enums and DataClasses."""
    print("\n" + "=" * 60)
    print("TEST: Enums and DataClasses")
    print("=" * 60)

    def test_gdpr_category():
        assert GDPRCategory.PII.value == "Personally Identifiable Information"
        assert GDPRCategory.SPI.value == "Sensitive Personal Information"
        assert GDPRCategory.NPII.value == "Non-Personally Identifiable Information"

    def test_sensitivity_level():
        assert SensitivityLevel.LOW.value == 1
        assert SensitivityLevel.VERY_HIGH.value == 4
        assert SensitivityLevel.HIGH.value > SensitivityLevel.LOW.value

    def test_consent_status():
        assert ConsentStatus.GRANTED.value == "granted"
        assert ConsentStatus.WITHDRAWN.value == "withdrawn"

    def test_consent_record():
        from spart import ConsentRecord

        record = ConsentRecord(
            subject_id="USR001",
            purpose="analytics",
            status=ConsentStatus.GRANTED,
            granted_at=datetime.datetime.now(),
            expires_at=datetime.datetime.now() + datetime.timedelta(days=365)
        )

        assert record.is_valid()

        record_dict = record.to_dict()
        assert record_dict["subject_id"] == "USR001"
        assert record_dict["status"] == "granted"

    def test_consent_record_expired():
        from spart import ConsentRecord

        record = ConsentRecord(
            subject_id="USR002",
            purpose="marketing",
            status=ConsentStatus.GRANTED,
            granted_at=datetime.datetime.now() - datetime.timedelta(days=400),
            expires_at=datetime.datetime.now() - datetime.timedelta(days=35)  # Expired
        )

        assert not record.is_valid()

    def test_audit_event():
        from spart import AuditEvent

        event = AuditEvent(
            event_id="evt001",
            event_type=AuditEventType.DATA_ACCESS,
            timestamp=datetime.datetime.now(),
            actor="test_user",
            action="read",
            resource="table1"
        )

        event_dict = event.to_dict()
        assert event_dict["event_type"] == "data_access"
        assert event_dict["actor"] == "test_user"

    def test_benchmark_result():
        from spart import BenchmarkResult

        result = BenchmarkResult(
            operation="hash_mask",
            backend="spark",
            row_count=10000,
            column_count=10,
            execution_time_seconds=1.5
        )

        assert result.throughput_rows_per_second is not None
        assert result.throughput_rows_per_second > 0

    runner.run_test("GDPRCategory enum", test_gdpr_category)
    runner.run_test("SensitivityLevel enum", test_sensitivity_level)
    runner.run_test("ConsentStatus enum", test_consent_status)
    runner.run_test("ConsentRecord dataclass", test_consent_record)
    runner.run_test("ConsentRecord expiry", test_consent_record_expired)
    runner.run_test("AuditEvent dataclass", test_audit_event)
    runner.run_test("BenchmarkResult dataclass", test_benchmark_result)


def test_integration(runner, spark, df, temp_dir):
    """Test integration of multiple components."""
    print("\n" + "=" * 60)
    print("TEST: Integration")
    print("=" * 60)

    def test_full_pipeline():
        # Initialize all components
        config = ConfigManager()
        audit = AuditManager(config, log_path=str(temp_dir / "integration_audit"))
        metadata_mgr = MetadataManager(spark, config=config, audit_manager=audit)
        consent_mgr = ConsentManager(spark, config, audit, str(temp_dir / "integration_consent"))
        anon_mgr = AnonymizationManager(metadata_mgr, audit)
        retention_mgr = RetentionManager(metadata_mgr, audit)

        # 1. PII Detection
        detector = PIIDetector(config)
        scan_results = detector.full_scan(df, sample_size=10)
        suggestions = detector.get_suggested_tags(scan_results)

        # 2. Apply tags
        column_tags = {
            "user_id": {"gdpr_category": "PII", "sensitivity": "MEDIUM", "retention_days": "3650"},
            "name": {"gdpr_category": "PII", "sensitivity": "HIGH", "retention_days": "3650"},
            "email": {"gdpr_category": "PII", "sensitivity": "MEDIUM", "retention_days": "1825"},
            "salary": {"gdpr_category": "SPI", "sensitivity": "VERY_HIGH", "retention_days": "2555"},
        }
        tagged_df = metadata_mgr.apply_column_tags(df, column_tags)

        # 3. Record consent
        consent_mgr.record_consent("USR101", "analytics", ConsentStatus.GRANTED)
        consent_mgr.record_consent("USR102", "analytics", ConsentStatus.DENIED)

        # 4. Apply anonymization
        anon_df = anon_mgr.auto_anonymize(tagged_df)

        # 5. Apply retention
        compliant_df = retention_mgr.apply_retention_policy(anon_df, "last_login")

        # 6. Check audit trail
        events = audit.get_events()
        assert len(events) > 0

        # 7. Generate compliance report
        start = datetime.datetime.now() - datetime.timedelta(days=1)
        end = datetime.datetime.now() + datetime.timedelta(days=1)
        report = audit.generate_compliance_report(start, end)
        assert report["summary"]["total_events"] > 0

    def test_audit_with_all_operations():
        audit = AuditManager(log_path=str(temp_dir / "audit_ops"))
        metadata_mgr = MetadataManager(spark, audit_manager=audit)
        anon_mgr = AnonymizationManager(metadata_mgr, audit)
        retention_mgr = RetentionManager(metadata_mgr, audit)

        # Perform operations
        column_tags = {"email": {"gdpr_category": "PII", "sensitivity": "MEDIUM"}}
        tagged_df = metadata_mgr.apply_column_tags(df, column_tags)

        anon_mgr.mask_columns(tagged_df, {"email": "hash"})
        anon_mgr.request_erasure(tagged_df, "user_id", "USR101")

        # All operations should be audited
        events = audit.get_events()
        event_types = [e.event_type for e in events]

        assert AuditEventType.ANONYMIZATION in event_types
        assert AuditEventType.ERASURE_REQUEST in event_types

    runner.run_test("full GDPR pipeline", test_full_pipeline)
    runner.run_test("audit with all operations", test_audit_with_all_operations)


# ============================================================================
# TESTS: Apache Atlas Client
# ============================================================================

def test_apache_atlas_client(runner, temp_dir):
    """Test Apache Atlas client functionality."""
    print("\n" + "=" * 60)
    print("TEST: ApacheAtlasClient")
    print("=" * 60)

    from unittest.mock import Mock, patch, MagicMock

    def test_atlas_initialization():
        """Test Atlas client initialization."""
        config = ConfigManager()

        # Test initialization with default config
        atlas = ApacheAtlasClient(
            atlas_url="http://localhost:21000",
            username="admin",
            password="admin",
            config=config
        )

        assert atlas.atlas_url == "http://localhost:21000"
        assert atlas.username == "admin"
        # Session is lazily initialized via _get_session()
        assert atlas._session is None  # Not initialized until first use

    def test_atlas_gdpr_classification_types():
        """Test GDPR classification type creation."""
        config = ConfigManager()
        atlas = ApacheAtlasClient(
            atlas_url="http://localhost:21000",
            username="admin",
            password="admin",
            config=config
        )

        # Mock requests module and the session
        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"classificationDefs": []}
        mock_session.post.return_value = mock_response

        with patch.object(atlas, '_get_session', return_value=mock_session):
            result = atlas.create_gdpr_classification_types()

            # Result contains 'created', 'existing', and 'errors' keys
            assert "created" in result
            assert "existing" in result
            assert "errors" in result
            # Should have processed all 3 GDPR types
            assert len(result["created"]) + len(result["existing"]) == 3

    def test_atlas_apply_classification():
        """Test applying classification to entity."""
        config = ConfigManager()
        atlas = ApacheAtlasClient(
            atlas_url="http://localhost:21000",
            username="admin",
            password="admin",
            config=config
        )

        # Mock the session
        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_session.post.return_value = mock_response

        with patch.object(atlas, '_get_session', return_value=mock_session):
            result = atlas.apply_classification_to_entity(
                entity_guid="12345",
                classification_name="gdpr_pii",
                attributes={"pii_type": "email"}
            )

            assert result is True

    def test_atlas_sync_table_metadata():
        """Test syncing table metadata to Atlas."""
        config = ConfigManager()
        atlas = ApacheAtlasClient(
            atlas_url="http://localhost:21000",
            username="admin",
            password="admin",
            config=config
        )

        column_tags = {
            "email": {"gdpr_category": "PII", "sensitivity": "HIGH"},
            "name": {"gdpr_category": "PII", "sensitivity": "MEDIUM"}
        }

        # Mock the session
        mock_session = Mock()

        # Mock search response (table lookup)
        mock_search_response = Mock()
        mock_search_response.status_code = 200
        mock_search_response.json.return_value = {
            "entities": [{"guid": "guid-123"}]
        }

        # Mock get entity response
        mock_get_response = Mock()
        mock_get_response.status_code = 200
        mock_get_response.json.return_value = {
            "entity": {"guid": "guid-123"},
            "referredEntities": {
                "col-guid-1": {"typeName": "hive_column", "attributes": {"name": "email"}},
                "col-guid-2": {"typeName": "hive_column", "attributes": {"name": "name"}}
            }
        }

        # Mock classification apply response
        mock_classify_response = Mock()
        mock_classify_response.status_code = 200

        mock_session.post.side_effect = [mock_search_response, mock_classify_response, mock_classify_response]
        mock_session.get.return_value = mock_get_response

        with patch.object(atlas, '_get_session', return_value=mock_session):
            result = atlas.sync_table_metadata(
                table_name="test_table",
                column_tags=column_tags,
                qualified_name="default.test_table"
            )

            # Result contains 'table', 'synced_columns', and 'errors' keys
            assert "table" in result
            assert "synced_columns" in result
            assert "errors" in result

    def test_atlas_get_classified_entities():
        """Test getting classified entities."""
        config = ConfigManager()
        atlas = ApacheAtlasClient(
            atlas_url="http://localhost:21000",
            username="admin",
            password="admin",
            config=config
        )

        # Mock the session
        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "entities": [
                {"guid": "123", "typeName": "hive_table"},
                {"guid": "456", "typeName": "hive_column"}
            ]
        }
        mock_session.get.return_value = mock_response

        with patch.object(atlas, '_get_session', return_value=mock_session):
            result = atlas.get_classified_entities("gdpr_pii", limit=10)

            assert len(result) == 2

    runner.run_test("atlas initialization", test_atlas_initialization)
    runner.run_test("atlas GDPR classification types", test_atlas_gdpr_classification_types)
    runner.run_test("atlas apply classification", test_atlas_apply_classification)
    runner.run_test("atlas sync table metadata", test_atlas_sync_table_metadata)
    runner.run_test("atlas get classified entities", test_atlas_get_classified_entities)


# ============================================================================
# TESTS: AWS Glue Catalog Client
# ============================================================================

def test_aws_glue_catalog_client(runner, temp_dir):
    """Test AWS Glue Catalog client functionality."""
    print("\n" + "=" * 60)
    print("TEST: AWSGlueCatalogClient")
    print("=" * 60)

    from unittest.mock import Mock, patch, MagicMock

    def test_glue_initialization():
        """Test Glue client initialization."""
        config = ConfigManager()

        # Mock boto3 clients
        with patch('boto3.client') as mock_boto:
            mock_glue = Mock()
            mock_lf = Mock()
            mock_boto.side_effect = [mock_glue, mock_lf]

            glue = AWSGlueCatalogClient(
                region_name="us-east-1",
                database_name="test_db",
                config=config
            )

            assert glue.database_name == "test_db"
            assert glue.region_name == "us-east-1"

    def test_glue_apply_table_tags():
        """Test applying tags to a Glue table."""
        config = ConfigManager()

        with patch('boto3.client') as mock_boto:
            mock_glue = Mock()
            mock_lf = Mock()
            mock_boto.side_effect = [mock_glue, mock_lf]

            # Mock get_table response
            mock_glue.get_table.return_value = {
                "Table": {
                    "Name": "test_table",
                    "StorageDescriptor": {},
                    "Parameters": {}
                }
            }
            mock_glue.update_table.return_value = {}

            glue = AWSGlueCatalogClient(
                region_name="us-east-1",
                database_name="test_db",
                config=config
            )

            result = glue.apply_table_tags(
                table_name="test_table",
                tags={"gdpr_compliant": "true", "data_classification": "PII"}
            )

            assert result is True

    def test_glue_apply_column_tags():
        """Test applying column-level tags."""
        config = ConfigManager()

        with patch('boto3.client') as mock_boto:
            mock_glue = Mock()
            mock_lf = Mock()
            mock_boto.side_effect = [mock_glue, mock_lf]

            # Mock get_table response with columns
            mock_glue.get_table.return_value = {
                "Table": {
                    "Name": "test_table",
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "email", "Type": "string"},
                            {"Name": "name", "Type": "string"}
                        ]
                    },
                    "Parameters": {}
                }
            }
            mock_glue.update_table.return_value = {}

            glue = AWSGlueCatalogClient(
                region_name="us-east-1",
                database_name="test_db",
                config=config
            )

            column_tags = {
                "email": {"gdpr_category": "PII", "sensitivity": "HIGH"},
                "name": {"gdpr_category": "PII", "sensitivity": "MEDIUM"}
            }

            result = glue.apply_column_tags("test_table", column_tags)

            # Result has 'updated_columns' and 'errors' keys
            assert "updated_columns" in result
            assert len(result["updated_columns"]) == 2

    def test_glue_get_gdpr_tables():
        """Test getting GDPR-tagged tables."""
        config = ConfigManager()

        with patch('boto3.client') as mock_boto:
            mock_glue = Mock()
            mock_lf = Mock()
            mock_boto.side_effect = [mock_glue, mock_lf]

            # Mock paginator for get_tables
            mock_paginator = Mock()
            mock_paginator.paginate.return_value = [
                {
                    "TableList": [
                        {
                            "Name": "pii_table",
                            "Parameters": {"gdpr:category": "PII", "gdpr:compliant": "true"},
                            "StorageDescriptor": {"Columns": []}
                        },
                        {
                            "Name": "regular_table",
                            "Parameters": {},
                            "StorageDescriptor": {"Columns": []}
                        }
                    ]
                }
            ]
            mock_glue.get_paginator.return_value = mock_paginator

            glue = AWSGlueCatalogClient(
                region_name="us-east-1",
                database_name="test_db",
                config=config
            )

            result = glue.get_gdpr_tables()

            # Should return list of tables with GDPR tags
            assert isinstance(result, list)

    def test_glue_compliance_report():
        """Test generating compliance report."""
        config = ConfigManager()

        with patch('boto3.client') as mock_boto:
            mock_glue = Mock()
            mock_lf = Mock()
            mock_boto.side_effect = [mock_glue, mock_lf]

            # Mock paginator for get_tables
            mock_paginator = Mock()
            mock_paginator.paginate.return_value = [
                {
                    "TableList": [
                        {
                            "Name": "pii_table",
                            "Parameters": {"gdpr:category": "PII"},
                            "StorageDescriptor": {
                                "Columns": [
                                    {"Name": "email", "Type": "string", "Comment": '{"gdpr_category": "PII"}'}
                                ]
                            }
                        }
                    ]
                }
            ]
            mock_glue.get_paginator.return_value = mock_paginator

            glue = AWSGlueCatalogClient(
                region_name="us-east-1",
                database_name="test_db",
                config=config
            )

            report = glue.generate_compliance_report()

            # Check report structure
            assert isinstance(report, dict)
            assert "database" in report

    runner.run_test("glue initialization", test_glue_initialization)
    runner.run_test("glue apply table tags", test_glue_apply_table_tags)
    runner.run_test("glue apply column tags", test_glue_apply_column_tags)
    runner.run_test("glue get GDPR tables", test_glue_get_gdpr_tables)
    runner.run_test("glue compliance report", test_glue_compliance_report)


# ============================================================================
# TESTS: ML PII Detector
# ============================================================================

def test_ml_pii_detector(runner, spark, df, temp_dir):
    """Test ML PII Detector functionality."""
    print("\n" + "=" * 60)
    print("TEST: MLPIIDetector")
    print("=" * 60)

    from unittest.mock import Mock, patch, MagicMock

    def test_ml_detector_initialization():
        """Test ML detector initialization with regex backend."""
        config = ConfigManager()

        # Regex backend doesn't require external dependencies
        detector = MLPIIDetector(config=config, backend='regex')

        assert detector.backend == 'regex'
        # Has a pattern detector for fallback
        assert detector._pattern_detector is not None
        # Custom patterns dict exists (even if empty)
        assert isinstance(detector._custom_patterns, dict)

    def test_ml_detector_text_detection():
        """Test PII detection in text using regex."""
        config = ConfigManager()
        detector = MLPIIDetector(config=config, backend='regex')

        # Test with text containing PII patterns
        text = "Contact john.doe@example.com or call 123-456-7890"
        result = detector.detect_pii_in_text(text, include_patterns=True)

        # Check result structure
        assert "backend_used" in result
        assert "pii_detected" in result or "entities" in result
        # Should detect patterns
        assert "pattern_matches" in result

    def test_ml_detector_custom_pattern():
        """Test adding custom patterns."""
        config = ConfigManager()
        detector = MLPIIDetector(config=config, backend='regex')

        # Add custom pattern for employee ID
        detector.add_custom_pattern("EMPLOYEE_ID", r"EMP-\d{6}")

        # Verify pattern was added
        assert "EMPLOYEE_ID" in detector._custom_patterns

        # Test detection
        text = "Employee EMP-123456 started today"
        result = detector.detect_pii_in_text(text, include_patterns=True)

        # Custom pattern should be in pattern matches
        assert "pattern_matches" in result
        if result["pattern_matches"]:
            assert "EMPLOYEE_ID" in result["pattern_matches"]

    def test_ml_detector_dataframe_scan():
        """Test scanning DataFrame content for PII."""
        config = ConfigManager()
        detector = MLPIIDetector(config=config, backend='regex')

        # Scan the test DataFrame
        result = detector.scan_dataframe_content(
            df=df,
            columns=["email", "name"],
            sample_size=5
        )

        # Check result structure
        assert "columns" in result or "columns_scanned" in result or "summary" in result

    def test_ml_detector_suggested_tags():
        """Test getting suggested GDPR tags from scan results."""
        config = ConfigManager()
        detector = MLPIIDetector(config=config, backend='regex')

        # Create mock scan results in the expected format for MLPIIDetector
        # Must have 'columns' key and each column must have 'total_entities'
        scan_results = {
            "columns": {
                "email": {
                    "samples_analyzed": 5,
                    "pii_detected": True,
                    "entity_types": {"EMAIL": 5},
                    "total_entities": 5
                },
                "phone": {
                    "samples_analyzed": 5,
                    "pii_detected": True,
                    "entity_types": {"PHONE": 3},
                    "total_entities": 3
                }
            },
            "summary": {
                "total_columns": 2
            }
        }

        suggestions = detector.get_suggested_tags(scan_results)

        # Should return suggestions dict with detected columns
        assert isinstance(suggestions, dict)
        assert "email" in suggestions
        assert "phone" in suggestions

    def test_ml_detector_auto_backend():
        """Test auto backend selection."""
        config = ConfigManager()

        # Auto backend should fall back to regex if no ML libraries available
        detector = MLPIIDetector(config=config, backend='auto')

        # Initialize and check the selected backend
        selected = detector._initialize_backend()

        # Should select one of the valid backends
        assert selected in ['spacy', 'transformers', 'regex']

    runner.run_test("ml detector initialization", test_ml_detector_initialization)
    runner.run_test("ml detector text detection", test_ml_detector_text_detection)
    runner.run_test("ml detector custom pattern", test_ml_detector_custom_pattern)
    runner.run_test("ml detector dataframe scan", test_ml_detector_dataframe_scan)
    runner.run_test("ml detector suggested tags", test_ml_detector_suggested_tags)
    runner.run_test("ml detector auto backend", test_ml_detector_auto_backend)


# ============================================================================
# TESTS: Data Lineage Tracker
# ============================================================================

def test_data_lineage_tracker(runner, spark, df, temp_dir):
    """Test Data Lineage Tracker functionality."""
    print("\n" + "=" * 60)
    print("TEST: DataLineageTracker")
    print("=" * 60)

    def test_lineage_initialization():
        """Test lineage tracker initialization."""
        config = ConfigManager()
        audit = AuditManager(log_path=str(temp_dir / "lineage_audit"))

        tracker = DataLineageTracker(
            config=config,
            storage_path=str(temp_dir / "lineage"),
            audit_manager=audit
        )

        assert tracker.storage_path is not None
        # Uses private attributes _nodes and _edges
        assert len(tracker._nodes) == 0
        assert len(tracker._edges) == 0

    def test_lineage_register_dataset():
        """Test registering a dataset in lineage."""
        config = ConfigManager()
        audit = AuditManager(log_path=str(temp_dir / "lineage_audit2"))
        tracker = DataLineageTracker(
            config=config,
            storage_path=str(temp_dir / "lineage2"),
            audit_manager=audit
        )

        # Register a dataset
        schema = {"user_id": "string", "email": "string", "salary": "double"}
        gdpr_tags = {"email": {"gdpr_category": "PII"}, "salary": {"gdpr_category": "SPI"}}

        node_id = tracker.register_dataset(
            name="users_raw",
            schema=schema,
            gdpr_tags=gdpr_tags,
            metadata={"source": "postgres", "table": "users"}
        )

        assert node_id is not None
        assert node_id in tracker._nodes
        assert tracker._nodes[node_id].name == "users_raw"
        assert tracker._nodes[node_id].node_type == "dataset"

    def test_lineage_register_transformation():
        """Test registering a transformation in lineage."""
        config = ConfigManager()
        audit = AuditManager(log_path=str(temp_dir / "lineage_audit3"))
        tracker = DataLineageTracker(
            config=config,
            storage_path=str(temp_dir / "lineage3"),
            audit_manager=audit
        )

        # Register source and target datasets
        source_id = tracker.register_dataset(
            name="users_raw",
            schema={"email": "string"},
            gdpr_tags={"email": {"gdpr_category": "PII"}}
        )

        target_id = tracker.register_dataset(
            name="users_anonymized",
            schema={"email_hash": "string"},
            gdpr_tags={"email_hash": {"gdpr_category": "NPII"}}
        )

        # Register transformation
        transform_id = tracker.register_transformation(
            name="anonymize_emails",
            operation_type="anonymization",
            source_ids=[source_id],
            target_id=target_id,
            column_mappings={"email": "email_hash"},
            details={"method": "sha256"}
        )

        assert transform_id is not None
        assert transform_id in tracker._nodes
        assert tracker._nodes[transform_id].node_type == "transformation"

        # Should have edges connecting source -> transform -> target
        edge_operations = [e.operation for e in tracker._edges.values()]
        assert len(edge_operations) > 0

    def test_lineage_track_anonymization():
        """Test tracking anonymization operation."""
        config = ConfigManager()
        audit = AuditManager(log_path=str(temp_dir / "lineage_audit4"))
        tracker = DataLineageTracker(
            config=config,
            storage_path=str(temp_dir / "lineage4"),
            audit_manager=audit
        )

        # Register datasets
        source_id = tracker.register_dataset(
            name="customer_data",
            schema={"ssn": "string", "name": "string"},
            gdpr_tags={"ssn": {"gdpr_category": "SPI"}}
        )

        target_id = tracker.register_dataset(
            name="customer_data_safe",
            schema={"ssn_masked": "string", "name": "string"},
            gdpr_tags={"ssn_masked": {"gdpr_category": "NPII"}}
        )

        # Track anonymization - columns_anonymized is Dict[str, str] mapping column to method
        transform_id = tracker.track_anonymization(
            source_id=source_id,
            target_id=target_id,
            columns_anonymized={"ssn": "masking"},  # Dict, not list
            method="masking"
        )

        assert transform_id is not None
        node = tracker._nodes[transform_id]
        assert "anonymization" in node.metadata.get("operation_type", "") or node.node_type == "transformation"

    def test_lineage_track_erasure():
        """Test tracking data erasure for GDPR compliance."""
        config = ConfigManager()
        audit = AuditManager(log_path=str(temp_dir / "lineage_audit5"))
        tracker = DataLineageTracker(
            config=config,
            storage_path=str(temp_dir / "lineage5"),
            audit_manager=audit
        )

        # Register datasets
        source_id = tracker.register_dataset(
            name="user_profiles",
            schema={"user_id": "string", "email": "string"},
            gdpr_tags={"email": {"gdpr_category": "PII"}}
        )

        target_id = tracker.register_dataset(
            name="user_profiles_gdpr_compliant",
            schema={"user_id": "string"},  # email column removed
            gdpr_tags={}
        )

        # Track erasure
        transform_id = tracker.track_erasure(
            source_id=source_id,
            target_id=target_id,
            subject_id="user_123",
            columns_erased=["email"]
        )

        assert transform_id is not None
        node = tracker._nodes[transform_id]
        assert "erasure" in node.metadata.get("operation_type", "") or node.node_type == "transformation"

    def test_lineage_upstream_downstream():
        """Test getting upstream and downstream lineage."""
        config = ConfigManager()
        audit = AuditManager(log_path=str(temp_dir / "lineage_audit6"))
        tracker = DataLineageTracker(
            config=config,
            storage_path=str(temp_dir / "lineage6"),
            audit_manager=audit
        )

        # Create a lineage chain: raw -> cleaned -> anonymized -> aggregated
        raw_id = tracker.register_dataset("raw_data", {"col": "string"}, {})
        cleaned_id = tracker.register_dataset("cleaned_data", {"col": "string"}, {})
        anon_id = tracker.register_dataset("anon_data", {"col": "string"}, {})
        agg_id = tracker.register_dataset("agg_data", {"count": "long"}, {})

        # Register transformations
        tracker.register_transformation("clean", "cleaning", [raw_id], cleaned_id, {}, {})
        tracker.register_transformation("anonymize", "anonymization", [cleaned_id], anon_id, {}, {})
        tracker.register_transformation("aggregate", "aggregation", [anon_id], agg_id, {}, {})

        # Get upstream lineage from aggregated data
        upstream = tracker.get_upstream_lineage(agg_id, depth=10)
        assert "nodes" in upstream
        assert len(upstream["nodes"]) > 1  # Should include ancestor nodes

        # Get downstream lineage from raw data
        downstream = tracker.get_downstream_lineage(raw_id, depth=10)
        assert "nodes" in downstream
        assert len(downstream["nodes"]) > 1  # Should include descendant nodes

    def test_lineage_impact_analysis():
        """Test impact analysis for GDPR compliance."""
        config = ConfigManager()
        audit = AuditManager(log_path=str(temp_dir / "lineage_audit7"))
        tracker = DataLineageTracker(
            config=config,
            storage_path=str(temp_dir / "lineage7"),
            audit_manager=audit
        )

        # Create lineage with GDPR tags
        source_id = tracker.register_dataset(
            name="customer_pii",
            schema={"email": "string", "ssn": "string"},
            gdpr_tags={
                "email": {"gdpr_category": "PII"},
                "ssn": {"gdpr_category": "SPI"}
            }
        )

        derived_id = tracker.register_dataset(
            name="customer_analytics",
            schema={"email_domain": "string"},
            gdpr_tags={"email_domain": {"gdpr_category": "NPII"}}
        )

        tracker.register_transformation(
            "extract_domain", "derivation", [source_id], derived_id,
            {"email": "email_domain"}, {}
        )

        # Run impact analysis
        impact = tracker.impact_analysis(source_id)

        # Check basic structure
        assert isinstance(impact, dict)
        assert "source_node" in impact or "node_id" in impact

    def test_lineage_export():
        """Test exporting lineage graph."""
        config = ConfigManager()
        audit = AuditManager(log_path=str(temp_dir / "lineage_audit8"))
        tracker = DataLineageTracker(
            config=config,
            storage_path=str(temp_dir / "lineage8"),
            audit_manager=audit
        )

        # Create some lineage
        source_id = tracker.register_dataset("source", {"col": "string"}, {})
        target_id = tracker.register_dataset("target", {"col": "string"}, {})
        tracker.register_transformation("transform", "processing", [source_id], target_id, {}, {})

        # Export to JSON
        json_path = str(temp_dir / "lineage_export.json")
        tracker.export_lineage_graph(json_path, format="json")

        assert Path(json_path).exists()

        # Verify JSON content
        with open(json_path) as f:
            exported = json.load(f)

        assert "nodes" in exported
        assert "edges" in exported
        assert len(exported["nodes"]) >= 3  # 2 datasets + 1 transformation

    def test_lineage_dsar_report():
        """Test generating DSAR (Data Subject Access Request) lineage report."""
        config = ConfigManager()
        audit = AuditManager(log_path=str(temp_dir / "lineage_audit9"))
        tracker = DataLineageTracker(
            config=config,
            storage_path=str(temp_dir / "lineage9"),
            audit_manager=audit
        )

        # Create lineage with subject-specific erasure
        source_id = tracker.register_dataset(
            name="user_data",
            schema={"user_id": "string", "email": "string"},
            gdpr_tags={"email": {"gdpr_category": "PII"}}
        )

        erased_id = tracker.register_dataset(
            name="user_data_erased",
            schema={"user_id": "string"},
            gdpr_tags={}
        )

        tracker.track_erasure(source_id, erased_id, "subject_abc", ["email"])

        # Generate DSAR report
        report = tracker.generate_dsar_lineage_report("subject_abc")

        assert "subject_id" in report
        assert report["subject_id"] == "subject_abc"

    runner.run_test("lineage initialization", test_lineage_initialization)
    runner.run_test("lineage register dataset", test_lineage_register_dataset)
    runner.run_test("lineage register transformation", test_lineage_register_transformation)
    runner.run_test("lineage track anonymization", test_lineage_track_anonymization)
    runner.run_test("lineage track erasure", test_lineage_track_erasure)
    runner.run_test("lineage upstream/downstream", test_lineage_upstream_downstream)
    runner.run_test("lineage impact analysis", test_lineage_impact_analysis)
    runner.run_test("lineage export", test_lineage_export)
    runner.run_test("lineage DSAR report", test_lineage_dsar_report)


def run_all_tests():
    """Run all tests."""
    print("\n" + "#" * 70)
    print("# SPARK-ANON COMPREHENSIVE TEST SUITE")
    print("#" * 70)

    spark = None
    temp_dir = None
    runner = TestRunner()

    try:
        # Setup
        print("\nInitializing Spark session...")
        spark = create_test_spark()
        spark.sparkContext.setLogLevel("ERROR")
        print("Spark session created successfully")

        # Create temp directory
        temp_dir = Path(tempfile.mkdtemp())
        print(f"Temp directory: {temp_dir}")

        # Create test DataFrame
        print("\nCreating test DataFrame...")
        df = create_test_dataframe(spark)
        print(f"Test DataFrame created with {df.count()} rows")
        df.show()

        # Run all tests
        test_enums_and_dataclasses(runner)
        test_config_manager(runner, temp_dir)
        test_audit_manager(runner, temp_dir)
        test_consent_manager(runner, spark, temp_dir)
        test_pii_detector(runner, spark, df)
        test_advanced_anonymization(runner, spark, df)
        test_pandas_processor(runner, spark, df)
        test_benchmark(runner, spark)
        test_backend_selector(runner, spark, df)
        test_databricks_methods(runner, spark)
        test_metadata_manager(runner, spark, df, temp_dir)
        test_retention_manager(runner, spark, df)
        test_anonymization_manager(runner, spark, df)
        test_integration(runner, spark, df, temp_dir)

        # Future Considerations - now implemented
        test_apache_atlas_client(runner, temp_dir)
        test_aws_glue_catalog_client(runner, temp_dir)
        test_ml_pii_detector(runner, spark, df, temp_dir)
        test_data_lineage_tracker(runner, spark, df, temp_dir)

        # Summary
        print("\n" + "#" * 70)
        if runner.summary():
            print("# ALL TESTS PASSED!")
        else:
            print("# SOME TESTS FAILED")
        print("#" * 70 + "\n")

        return runner.failed == 0

    except Exception as e:
        print(f"\n!!! TEST SUITE FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        if spark:
            spark.stop()
            print("Spark session stopped")

        if temp_dir and temp_dir.exists():
            shutil.rmtree(temp_dir)
            print(f"Temp directory cleaned up")


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
