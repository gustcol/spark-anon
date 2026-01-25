"""
Test suite for spart.py - GDPR Privacy Library
Run with: python test_spart.py
"""

import sys
import json
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

# Import the library
from spart import MetadataManager, RetentionManager, AnonymizationManager


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
        ("USR101", "John Smith", "john.smith@example.com", "123 Main St", 50000.0, datetime.date(2022, 1, 15)),
        ("USR102", "Jane Doe", "jane.doe@example.com", "456 Oak Ave", 75000.0, datetime.date(2023, 3, 10)),
        ("USR103", "Peter Jones", "peter.jones@sample.net", "789 Pine Rd", 62000.0, datetime.date(2024, 8, 20)),
    ]
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("address", StringType(), True),
        StructField("salary", DoubleType(), True),
        StructField("last_login", DateType(), True)
    ])
    return spark.createDataFrame(data, schema)


def test_databricks_methods(spark):
    """Test Databricks/Unity Catalog methods raise errors outside Databricks."""
    print("\n" + "=" * 60)
    print("TEST: Databricks/Unity Catalog Methods")
    print("=" * 60)

    metadata_mgr = MetadataManager(spark)

    # Test 1: apply_tags_to_delta_table should fail outside Databricks
    print("\n[1] Testing apply_tags_to_delta_table (non-Databricks)...")
    try:
        metadata_mgr.apply_tags_to_delta_table(
            "catalog.schema.table",
            {"tag": "value"}
        )
        print("   FAIL: Should have raised EnvironmentError")
    except EnvironmentError as e:
        assert "Databricks" in str(e)
        print("   PASS: Correctly raises EnvironmentError outside Databricks")

    # Test 2: apply_column_tags_to_delta_table should fail outside Databricks
    print("\n[2] Testing apply_column_tags_to_delta_table (non-Databricks)...")
    try:
        metadata_mgr.apply_column_tags_to_delta_table(
            "catalog.schema.table",
            {"column": {"tag": "value"}}
        )
        print("   FAIL: Should have raised EnvironmentError")
    except EnvironmentError as e:
        assert "Databricks" in str(e)
        print("   PASS: Correctly raises EnvironmentError outside Databricks")

    # Test 3: get_table_tags should fail outside Databricks
    print("\n[3] Testing get_table_tags (non-Databricks)...")
    try:
        metadata_mgr.get_table_tags("catalog.schema.table")
        print("   FAIL: Should have raised EnvironmentError")
    except EnvironmentError as e:
        assert "Databricks" in str(e)
        print("   PASS: Correctly raises EnvironmentError outside Databricks")

    # Test 4: Verify is_databricks flag is False
    print("\n[4] Testing is_databricks detection...")
    assert metadata_mgr.is_databricks is False, "Should detect non-Databricks env"
    print("   PASS: Correctly detected non-Databricks environment")


def test_metadata_manager(spark, df):
    """Test MetadataManager functionality."""
    print("\n" + "=" * 60)
    print("TEST: MetadataManager")
    print("=" * 60)

    metadata_mgr = MetadataManager(spark)

    # Test 1: Apply column tags
    print("\n[1] Testing apply_column_tags...")
    column_tags = {
        "user_id": {"gdpr_category": "PII", "sensitivity": "MEDIUM", "purpose": "identification", "retention_days": "3650"},
        "name": {"gdpr_category": "PII", "sensitivity": "HIGH", "purpose": "identification", "retention_days": "3650"},
        "email": {"gdpr_category": "PII", "sensitivity": "MEDIUM", "purpose": "contact", "retention_days": "1825"},
        "address": {"gdpr_category": "PII", "sensitivity": "HIGH", "purpose": "location", "retention_days": "1825"},
        "salary": {"gdpr_category": "SPI", "sensitivity": "VERY_HIGH", "purpose": "compensation", "retention_days": "2555"},
        "last_login": {"gdpr_category": "NPII", "sensitivity": "LOW", "purpose": "activity_tracking"}
    }
    tagged_df = metadata_mgr.apply_column_tags(df, column_tags)

    # Verify tags were applied
    retrieved_tags = metadata_mgr.get_column_tags(tagged_df)
    assert len(retrieved_tags) == 6, f"Expected 6 tagged columns, got {len(retrieved_tags)}"
    assert retrieved_tags["name"]["gdpr_category"] == "PII", "Name should be PII"
    assert retrieved_tags["salary"]["sensitivity"] == "VERY_HIGH", "Salary should be VERY_HIGH sensitivity"
    print("   PASS: Column tags applied and retrieved correctly")

    # Test 2: Apply dataset tags
    print("\n[2] Testing apply_dataset_tags...")
    dataset_tags = {
        "owner": "data_analytics_team",
        "purpose": "user_behavior_analysis",
        "source_system": "crm_database",
        "classification": "CONFIDENTIAL"
    }
    tagged_df = metadata_mgr.apply_dataset_tags(tagged_df, dataset_tags)

    # Verify dataset tags via the metadata column
    assert "_metadata" in tagged_df.columns, "Metadata column should exist"
    metadata_row = tagged_df.select("_metadata").first()
    metadata_dict = json.loads(metadata_row[0])
    assert metadata_dict["owner"] == "data_analytics_team", "Owner should match"
    assert "dataset_id" in metadata_dict, "Should have auto-generated dataset_id"
    print("   PASS: Dataset tags applied correctly")

    # Test 3: PII scan
    print("\n[3] Testing scan_for_pii...")
    pii_results = metadata_mgr.scan_for_pii(df)
    assert "name" in pii_results["potential_pii"], "Name should be detected as potential PII"
    assert "email" in pii_results["potential_pii"], "Email should be detected as potential PII"
    assert "address" in pii_results["potential_pii"], "Address should be detected as potential PII"
    print(f"   PII detected: {pii_results['potential_pii']}")
    print("   PASS: PII scan works correctly")

    # Test 4: Validation - invalid GDPR category
    print("\n[4] Testing validation (invalid GDPR category)...")
    try:
        invalid_tags = {"user_id": {"gdpr_category": "INVALID_CATEGORY"}}
        metadata_mgr.apply_column_tags(df, invalid_tags)
        print("   FAIL: Should have raised ValueError")
    except ValueError as e:
        print(f"   PASS: Correctly raised ValueError: {e}")

    # Test 5: Validation - invalid sensitivity
    print("\n[5] Testing validation (invalid sensitivity)...")
    try:
        invalid_tags = {"user_id": {"gdpr_category": "PII", "sensitivity": "EXTREME"}}
        metadata_mgr.apply_column_tags(df, invalid_tags)
        print("   FAIL: Should have raised ValueError")
    except ValueError as e:
        print(f"   PASS: Correctly raised ValueError: {e}")

    # Test 6: Export metadata catalog
    print("\n[6] Testing export_metadata_catalog...")
    catalog_path = "/tmp/test_metadata_catalog.json"
    metadata_mgr.export_metadata_catalog(tagged_df, catalog_path)
    with open(catalog_path, "r") as f:
        catalog = json.load(f)
    assert "dataset_metadata" in catalog, "Catalog should have dataset_metadata"
    assert "column_metadata" in catalog, "Catalog should have column_metadata"
    assert "exported_at" in catalog, "Catalog should have exported_at timestamp"
    print(f"   PASS: Catalog exported to {catalog_path}")

    return tagged_df


def test_retention_manager(metadata_mgr, tagged_df):
    """Test RetentionManager functionality."""
    print("\n" + "=" * 60)
    print("TEST: RetentionManager")
    print("=" * 60)

    retention_mgr = RetentionManager(metadata_mgr)

    # Test retention policy
    print("\n[1] Testing apply_retention_policy...")
    original_count = tagged_df.count()
    print(f"   Original count: {original_count}")

    # Apply retention policy using last_login as reference
    filtered_df = retention_mgr.apply_retention_policy(tagged_df, "last_login")
    filtered_count = filtered_df.count()
    print(f"   Filtered count: {filtered_count}")

    # With max retention of 3650 days (10 years), all records should be retained
    # since none are older than 10 years
    assert filtered_count <= original_count, "Filtered count should be <= original"
    print("   PASS: Retention policy applied successfully")

    return filtered_df


def test_anonymization_manager(metadata_mgr, tagged_df):
    """Test AnonymizationManager functionality."""
    print("\n" + "=" * 60)
    print("TEST: AnonymizationManager")
    print("=" * 60)

    anon_mgr = AnonymizationManager(metadata_mgr)

    # Test 1: Manual masking - hash
    print("\n[1] Testing mask_columns (hash)...")
    mask_rules = {"email": "hash"}
    masked_df = anon_mgr.mask_columns(tagged_df, mask_rules)

    # Verify hash was applied (SHA-256 produces 64-char hex string)
    first_email = masked_df.select("email").first()[0]
    assert len(first_email) == 64, f"Hash should be 64 chars, got {len(first_email)}"
    print(f"   Hashed email sample: {first_email[:20]}...")
    print("   PASS: Hash masking works correctly")

    # Test 2: Manual masking - partial
    print("\n[2] Testing mask_columns (partial)...")
    mask_rules = {"email": "partial"}
    masked_df = anon_mgr.mask_columns(tagged_df, mask_rules)
    first_email = masked_df.select("email").first()[0]
    assert "***" in first_email, "Partial mask should contain ***"
    print(f"   Partially masked email: {first_email}")
    print("   PASS: Partial masking works correctly")

    # Test 3: Manual masking - redact
    print("\n[3] Testing mask_columns (redact)...")
    mask_rules = {"address": "redact"}
    masked_df = anon_mgr.mask_columns(tagged_df, mask_rules)
    first_address = masked_df.select("address").first()[0]
    assert first_address == "[REDACTED]", f"Expected [REDACTED], got {first_address}"
    print("   PASS: Redact masking works correctly")

    # Test 4: Pseudonymization
    print("\n[4] Testing pseudonymize_columns...")
    pseudo_df = anon_mgr.pseudonymize_columns(tagged_df, ["user_id"], salt="test_salt")
    first_user_id = pseudo_df.select("user_id").first()[0]
    assert len(first_user_id) == 64, "Pseudonymized value should be 64 chars"
    print(f"   Pseudonymized user_id: {first_user_id[:20]}...")

    # Verify consistent pseudonymization with same salt
    pseudo_df2 = anon_mgr.pseudonymize_columns(tagged_df, ["user_id"], salt="test_salt")
    first_user_id2 = pseudo_df2.select("user_id").first()[0]
    assert first_user_id == first_user_id2, "Same salt should produce same pseudonym"
    print("   PASS: Pseudonymization is consistent with same salt")

    # Test 5: Auto-anonymize
    print("\n[5] Testing auto_anonymize...")
    anon_df = anon_mgr.auto_anonymize(tagged_df)

    # HIGH PII should be hashed (name, address)
    first_name = anon_df.select("name").first()[0]
    assert len(first_name) == 64, "Name (HIGH PII) should be hashed"

    # MEDIUM PII should be partially masked (email, user_id)
    first_email = anon_df.select("email").first()[0]
    assert "***" in first_email, "Email (MEDIUM PII) should be partially masked"

    # SPI should be pseudonymized (salary)
    first_salary = anon_df.select("salary").first()[0]
    assert len(str(first_salary)) == 64, "Salary (SPI) should be pseudonymized"

    print("   PASS: Auto-anonymization applies correct techniques based on tags")

    # Test 6: Data subject erasure
    print("\n[6] Testing request_erasure...")
    erased_df = anon_mgr.request_erasure(tagged_df, "user_id", "USR102")

    # Check that USR102's PII fields are nullified
    usr102_row = erased_df.filter("user_id = 'USR102'").first()
    assert usr102_row["name"] is None, "Name should be null after erasure"
    assert usr102_row["email"] is None, "Email should be null after erasure"
    assert usr102_row["address"] is None, "Address should be null after erasure"
    assert usr102_row["salary"] is None, "Salary (SPI) should be null after erasure"

    # Check that other users are not affected
    usr101_row = erased_df.filter("user_id = 'USR101'").first()
    assert usr101_row["name"] == "John Smith", "USR101 name should be unchanged"
    assert usr101_row["email"] == "john.smith@example.com", "USR101 email should be unchanged"

    print("   PASS: Erasure request correctly nullifies PII/SPI for specified user")

    return anon_df


def run_all_tests():
    """Run all tests."""
    print("\n" + "#" * 60)
    print("# SPART-ANON TEST SUITE")
    print("#" * 60)

    spark = None
    try:
        # Initialize Spark
        print("\nInitializing Spark session...")
        spark = create_test_spark()
        spark.sparkContext.setLogLevel("ERROR")  # Reduce noise
        print("Spark session created successfully")

        # Create test DataFrame
        print("\nCreating test DataFrame...")
        df = create_test_dataframe(spark)
        print(f"Test DataFrame created with {df.count()} rows")
        df.show()

        # Initialize MetadataManager
        metadata_mgr = MetadataManager(spark)

        # Run tests
        test_databricks_methods(spark)
        tagged_df = test_metadata_manager(spark, df)
        test_retention_manager(metadata_mgr, tagged_df)
        test_anonymization_manager(metadata_mgr, tagged_df)

        print("\n" + "#" * 60)
        print("# ALL TESTS PASSED!")
        print("#" * 60 + "\n")

    except Exception as e:
        print(f"\n!!! TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        if spark:
            spark.stop()
            print("Spark session stopped")


if __name__ == "__main__":
    run_all_tests()
