from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict, List, Optional, Any
import json
import datetime
import uuid
import logging

# Basic logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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

    def __init__(self,
                 spark: SparkSession,
                 metadata_col_name: str = "_metadata"):
        """
        Initializes the metadata manager.

        Args:
            spark: Active Spark session.
            metadata_col_name: Name for the hidden metadata column
                             (default: "_metadata").
        """
        self.spark = spark
        self.metadata_col_name = metadata_col_name
        self._validate_spark_session()

    def _validate_spark_session(self):
        """Validates if the Spark session is active."""
        if self.spark is None:
            raise ValueError("Spark session not initialized.")

        # Check if we are in a Databricks environment
        spark_version_tag = "spark.databricks.clusterUsageTags.sparkVersion"
        is_databricks = self.spark.conf.get(spark_version_tag, "")
        self.is_databricks = len(is_databricks) > 0

    def apply_column_tags(
        self, df: DataFrame, column_tags: Dict[str, Dict[str, Any]]
    ) -> DataFrame:
        """
        Applies GDPR tags and metadata at column level.

        Args:
            df: DataFrame to be modified.
            column_tags: Dictionary with columns and their associated tags.
                         Ex: {"name": {"gdpr_category": "PII",
                                      "sensitivity": "HIGH",
                                      "retention_days": 1825}} # 5 years

        Returns:
            DataFrame with applied metadata.
        """
        # Validate input data
        self._validate_column_tags(df, column_tags)

        # Create comments for columns
        for column_name, tags in column_tags.items():
            if column_name in df.columns:
                comment = json.dumps(tags)
                # Apply metadata using alias to preserve column comment
                df = df.withColumn(
                    column_name,
                    F.col(column_name).alias(
                        column_name, metadata={"gdpr_tags": comment}
                    )
                )

        return df

    def apply_dataset_tags(
        self, df: DataFrame, dataset_tags: Dict[str, str]
    ) -> DataFrame:
        """
        Applies tags and metadata at dataset level using a hidden column.

        Args:
            df: DataFrame to be modified.
            dataset_tags: Dictionary with tags for the complete dataset.
                          Ex: {"owner": "data_team",
                               "purpose": "customer_analysis",
                               "retention": "7_YEARS"}

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

        # Add as hidden column using the configured name
        df = df.withColumn(self.metadata_col_name, F.lit(metadata_json))

        return df

    def scan_for_pii(self, df: DataFrame) -> Dict[str, List[str]]:
        """
        Scans DataFrame column names to identify potential PII/SPI.

        NOTE: This method uses simple keyword matching on column names and is
        not a substitute for thorough data content analysis. It may produce
        false positives or negatives.

        Args:
            df: DataFrame to be analyzed.

        Returns:
            Dictionary with potential PII categories and associated columns.
        """
        # Patterns for potentially sensitive data types based on column names
        patterns = {
            "potential_pii": [
                "name", "customer", "email", "phone", "ssn", "address",
                "birth", "zip"
            ],
            "potential_spi": [
                "health", "religion", "politics", "racial", "ethnic",
                "genetic", "biometric", "sexual", "criminal"
            ],
            "potential_secure": [
                "password", "token", "secret", "key", "credential"
            ]
        }

        result = {k: [] for k in patterns.keys()}

        # Check columns based on their names
        for column in df.columns:
            column_lower = column.lower()
            for category, terms in patterns.items():
                for term in terms:
                    if term in column_lower:
                        result[category].append(column)
                        break  # Move to next column once a match is found

        return result

    def export_metadata_catalog(self, df: DataFrame, path: str) -> None:
        """
        Exports metadata catalog to a JSON file.

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
                # Use the configured metadata column name
                first_row = df.select(self.metadata_col_name).first()
                if first_row and first_row[0]:
                    dataset_metadata = json.loads(first_row[0])
            except Exception as e:  # Catch specific exceptions if possible
                logger.error(f"Failed to extract dataset metadata: {e}")
                dataset_metadata = {"error": "failed to extract"}
        else:
            logger.info(
                f"Dataset metadata column '{self.metadata_col_name}' "
                f"not found."
            )

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

    def apply_tags_to_delta_table(
        self, table_name: str, tags: Dict[str, str],
        use_unity_catalog: bool = True
    ) -> None:
        """
        Applies tags to a Delta table (Databricks only).

        Supports both Unity Catalog (SET TAGS) and legacy Hive Metastore
        (SET TBLPROPERTIES) syntax.

        Args:
            table_name: Full name of the Delta table.
                        For Unity Catalog: 'catalog.schema.table'
                        For Hive Metastore: 'schema.table'
            tags: Dictionary of tags and values to be applied.
            use_unity_catalog: If True, uses Unity Catalog SET TAGS syntax.
                               If False, uses legacy TBLPROPERTIES syntax.
                               Default: True
        """
        if not self.is_databricks:
            msg = "This method only works in a Databricks environment."
            logger.error(msg)
            raise EnvironmentError(msg)

        # Sanitize tag keys and values to prevent SQL injection
        sanitized_tags = {
            k.replace("'", "''").replace("\\", "\\\\"):
            v.replace("'", "''").replace("\\", "\\\\")
            for k, v in tags.items()
        }

        # Create tags/properties string for SQL command
        tags_str = ", ".join([f"'{k}' = '{v}'" for k, v in sanitized_tags.items()])

        # Use appropriate syntax based on catalog type
        if use_unity_catalog:
            # Unity Catalog syntax
            sql_command = f"ALTER TABLE {table_name} SET TAGS ({tags_str})"
        else:
            # Legacy Hive Metastore syntax
            sql_command = f"ALTER TABLE {table_name} SET TBLPROPERTIES ({tags_str})"

        try:
            self.spark.sql(sql_command)
            catalog_type = "Unity Catalog" if use_unity_catalog else "Hive Metastore"
            logger.info(f"Tags applied to table {table_name} ({catalog_type})")
        except Exception as e:
            logger.error(f"Failed to apply tags to {table_name}: {e}")
            raise

    def apply_column_tags_to_delta_table(
        self, table_name: str, column_tags: Dict[str, Dict[str, str]]
    ) -> None:
        """
        Applies tags to columns in a Delta table using Unity Catalog.

        This method uses Unity Catalog's column-level tagging feature.
        Requires Databricks with Unity Catalog enabled.

        Args:
            table_name: Full name of the Delta table ('catalog.schema.table').
            column_tags: Dictionary mapping column names to their tags.
                         Ex: {"email": {"pii": "true", "sensitivity": "high"}}
        """
        if not self.is_databricks:
            msg = "This method only works in a Databricks environment."
            logger.error(msg)
            raise EnvironmentError(msg)

        for column_name, tags in column_tags.items():
            # Sanitize to prevent SQL injection
            safe_column = column_name.replace("`", "``")
            sanitized_tags = {
                k.replace("'", "''").replace("\\", "\\\\"):
                v.replace("'", "''").replace("\\", "\\\\")
                for k, v in tags.items()
            }

            tags_str = ", ".join([f"'{k}' = '{v}'" for k, v in sanitized_tags.items()])
            sql_command = (
                f"ALTER TABLE {table_name} "
                f"ALTER COLUMN `{safe_column}` SET TAGS ({tags_str})"
            )

            try:
                self.spark.sql(sql_command)
                logger.info(
                    f"Tags applied to column '{column_name}' in table {table_name}"
                )
            except Exception as e:
                logger.error(
                    f"Failed to apply tags to column '{column_name}' "
                    f"in table {table_name}: {e}"
                )
                raise

    def get_table_tags(self, table_name: str) -> Dict[str, str]:
        """
        Retrieves tags from a Delta table in Unity Catalog.

        Args:
            table_name: Full name of the Delta table ('catalog.schema.table').

        Returns:
            Dictionary of tag key-value pairs.
        """
        if not self.is_databricks:
            msg = "This method only works in a Databricks environment."
            logger.error(msg)
            raise EnvironmentError(msg)

        try:
            # Query information_schema for table tags
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
                    f"'catalog.schema.table' for Unity Catalog"
                )
                return {}
        except Exception as e:
            logger.error(f"Failed to get tags from {table_name}: {e}")
            return {}

    def _validate_column_tags(
        self, df: DataFrame, column_tags: Dict[str, Dict[str, Any]]
    ) -> None:
        """Validates column tags according to GDPR rules."""
        for col_name, tags in column_tags.items():
            # Check if column exists
            if col_name not in df.columns:
                logger.warning(
                    f"Column '{col_name}' does not exist in DataFrame "
                    f"and will be ignored."
                )
                continue

            # Check GDPR category
            if "gdpr_category" in tags:
                category = tags["gdpr_category"]
                if category not in self.GDPR_CATEGORIES:
                    valid_categories = list(self.GDPR_CATEGORIES.keys())
                    raise ValueError(
                        f"Invalid GDPR category: {category}. "
                        f"Use one of: {valid_categories}"
                    )

            # Check sensitivity level
            if "sensitivity" in tags:
                sensitivity = tags["sensitivity"]
                if sensitivity not in self.SENSITIVITY_LEVELS:
                    raise ValueError(
                        f"Invalid sensitivity level: {sensitivity}. "
                        f"Use one of: {self.SENSITIVITY_LEVELS}"
                    )

    def _validate_dataset_tags(self, dataset_tags: Dict[str, str]) -> None:
        """Validates dataset tags."""
        required_fields = ["owner", "purpose"]
        for field in required_fields:
            if field not in dataset_tags:
                raise ValueError(f"Required tag missing: '{field}'")

    def get_column_tags(self, df: DataFrame) -> Dict[str, Dict[str, str]]:
        """
        Returns GDPR tags applied to each column of the DataFrame.

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

    TODO: Refine logic for more granular retention (e.g., nullifying fields).
    """

    def __init__(self, metadata_manager: MetadataManager):
        """
        Initializes the retention manager.

        Args:
            metadata_manager: Instance of MetadataManager.
        """
        self.metadata_manager = metadata_manager
        self.spark = metadata_manager.spark

    def apply_retention_policy(
        self, df: DataFrame, date_column: str
    ) -> DataFrame:
        """
        Applies retention policies based on 'retention_days' tag in columns.

        Filters rows where the `date_column` is older than the *longest*
        retention period specified in any of its tagged columns.

        Args:
            df: DataFrame to be filtered.
            date_column: Name of the column containing the reference date
                         (must be DateType or TimestampType).

        Returns:
            DataFrame with data within the longest applicable retention period.
        """
        # Get all column tags
        column_tags = self.metadata_manager.get_column_tags(df)

        if not column_tags:
            logger.warning(
                "No GDPR tags found. Cannot apply retention policy."
            )
            return df

        # Find the maximum retention period (in days) across all tagged columns
        max_retention_days = 0
        for col_name, tags in column_tags.items():
            try:
                retention = int(tags.get("retention_days", 0))
                if retention > max_retention_days:
                    max_retention_days = retention
            except (ValueError, TypeError):
                logger.warning(
                    f"Invalid 'retention_days' value for column {col_name}. "
                    f"Ignoring for retention calculation."
                )

        if max_retention_days <= 0:
            logger.info(
                "No valid retention period found in tags. No rows filtered."
            )
            return df

        # Calculate cutoff date
        cutoff_date = F.date_sub(F.current_date(), max_retention_days)

        # Filter DataFrame
        result_df = df.filter(F.col(date_column) >= cutoff_date)

        # Calculate actual cutoff date for logging
        cutoff_date_str = (
            datetime.datetime.now() - datetime.timedelta(days=max_retention_days)
        ).strftime("%Y-%m-%d")

        logger.info(
            f"Applied retention policy. Kept records where '{date_column}' "
            f">= {cutoff_date_str} (max retention: {max_retention_days} days)."
        )

        return result_df


# ==============================================================================
# Anonymization Manager
# ==============================================================================


class AnonymizationManager:
    """
    Manages anonymization and masking techniques for sensitive data.
    """

    def __init__(self, metadata_manager: MetadataManager):
        """
        Initializes the anonymization manager.

        Args:
            metadata_manager: Instance of MetadataManager.
        """
        self.metadata_manager = metadata_manager
        self.spark = metadata_manager.spark

    def mask_columns(
        self, df: DataFrame, mask_rules: Dict[str, str]
    ) -> DataFrame:
        """
        Applies masking to specific columns based on provided rules.

        Args:
            df: Original DataFrame.
            mask_rules: Dictionary with masking rules by column.
                        Supported types: "hash", "partial", "redact".
                        Ex: {"ssn": "hash", "email": "partial"}

        Returns:
            DataFrame with masked data.
        """
        if not mask_rules:
            return df

        # Build all masking expressions at once for better performance
        mask_expressions = {}

        for column, mask_type in mask_rules.items():
            if column not in df.columns:
                logger.warning(
                    f"Masking rule specified for non-existent column "
                    f"'{column}'. Ignoring."
                )
                continue

            logger.info(
                f"Applying masking type '{mask_type}' to column '{column}'"
            )
            # Ensure string type for operations
            col_ref = F.col(column).cast("string")

            if mask_type == "hash":
                # Apply SHA-256 hash
                mask_expressions[column] = F.sha2(col_ref, 256)

            elif mask_type == "partial":
                # Show only initial and final parts (e.g., joh***com)
                mask_expressions[column] = F.concat_ws(
                    "***",
                    F.substring(col_ref, 1, 3),
                    F.substring(col_ref, -3, 3)
                )

            elif mask_type == "redact":
                # Replace with fixed text
                mask_expressions[column] = F.lit("[REDACTED]")

            else:
                raise ValueError(f"Unknown masking type: {mask_type}")

        # Apply all masking in one operation for better performance
        if mask_expressions:
            return df.withColumns(mask_expressions)
        return df

    def pseudonymize_columns(
        self, df: DataFrame, columns: List[str], salt: Optional[str] = None
    ) -> DataFrame:
        """
        Pseudonymizes columns maintaining the same relationship between values.

        Args:
            df: Original DataFrame.
            columns: List of columns to pseudonymize.
            salt: Optional value for consistent pseudonyms across
                  runs/datasets. If None, a random UUID is generated
                  per call (not consistent). Manage and provide a
                  consistent salt securely if needed.

        Returns:
            DataFrame with pseudonymized data.
        """
        if not columns:
            return df

        salt_value = salt or str(uuid.uuid4())
        if salt is None:
            logger.warning(
                "Using random salt for pseudonymization. "
                "Output will not be consistent across runs."
            )

        # Build all pseudonymization expressions at once for better performance
        pseudo_expressions = {}

        for column in columns:
            if column not in df.columns:
                logger.warning(
                    f"Pseudonymization requested for non-existent column "
                    f"'{column}'. Ignoring."
                )
                continue

            # Create a consistent pseudonym using SHA256 with salt
            pseudo_expressions[column] = F.sha2(
                F.concat_ws(":", F.col(column).cast("string"), F.lit(salt_value)),
                256
            )

        # Apply all pseudonymization in one operation for better performance
        if pseudo_expressions:
            return df.withColumns(pseudo_expressions)
        return df

    def auto_anonymize(self, df: DataFrame) -> DataFrame:
        """
        Applies automatic anonymization based on GDPR tags.

        - PII (HIGH/VERY_HIGH): Hashed
        - PII (MEDIUM): Partially masked
        - SPI: Pseudonymized (using a random salt per call)

        Args:
            df: Original DataFrame.

        Returns:
            DataFrame with anonymized sensitive data.
        """
        # Get all column tags
        column_tags = self.metadata_manager.get_column_tags(df)

        # Define anonymization rules by category and sensitivity
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
                # Note: Using default random salt for auto-pseudonymization
                pseudonymize_columns.append(col_name)

        # Apply masking
        result_df = self.mask_columns(df, mask_rules)

        # Apply pseudonymization
        # Using default salt (random UUID per call) for auto-anonymize
        result_df = self.pseudonymize_columns(
            result_df, pseudonymize_columns
        )

        return result_df

    def request_erasure(
        self, df: DataFrame, identifier_col: str, identifier_value: str
    ) -> DataFrame:
        """
        Handles data subject erasure requests by anonymizing PII/SPI fields.

        Identifies the row(s) matching the identifier and applies nullification
        or redaction to columns tagged as PII or SPI.

        Args:
            df: The DataFrame containing the data.
            identifier_col: The column name used to identify the data subject.
            identifier_value: The value in the identifier column for the
                              subject.

        Returns:
            DataFrame with PII/SPI columns nullified for the specified user.
        """
        column_tags = self.metadata_manager.get_column_tags(df)
        if not column_tags:
            logger.warning("No column tags found. Cannot perform erasure.")
            return df

        logger.info(
            f"Processing erasure request for {identifier_col} = "
            f"{identifier_value}"
        )

        # Identify columns to anonymize (PII/SPI)
        columns_to_anonymize = [
            col_name for col_name, tags in column_tags.items()
            if tags.get("gdpr_category") in ["PII", "SPI"]
            and col_name != identifier_col  # Exclude identifier column
        ]

        if not columns_to_anonymize:
            logger.warning("No PII/SPI columns tagged. No erasure performed.")
            return df

        # Build all erasure expressions at once for better performance
        condition = F.col(identifier_col) == identifier_value
        erasure_expressions = {}

        for target_col in columns_to_anonymize:
            logger.debug(f"Applying erasure (null) to column: {target_col}")
            erasure_expressions[target_col] = F.when(
                condition,
                F.lit(None).cast(df.schema[target_col].dataType)
            ).otherwise(F.col(target_col))

        # Apply all erasures in one operation for better performance
        if erasure_expressions:
            result_df = df.withColumns(erasure_expressions)
            logger.info(
                f"Applied erasure to {len(erasure_expressions)} columns for "
                f"{identifier_col} = {identifier_value}"
            )
            return result_df

        logger.info(
            f"No applicable columns found to anonymize for "
            f"{identifier_col} = {identifier_value}"
        )
        return df


# ==============================================================================
# Usage Example
# ==============================================================================


def usage_example():
    """
    Library usage demonstration.
    """
    from pyspark.sql.types import StructType, StructField, StringType

    # Create a Spark session
    spark = SparkSession.builder \
        .appName("GDPR Metadata Example") \
        .getOrCreate()

    # Create sample data
    data = [
        ("John Smith", "john@email.com", "11999887766", "Analyst",
         5000.00, "2023-01-15"),
        ("Mary Johnson", "mary@email.com", "11988776655", "Manager",
         8000.00, "2023-02-20"),
        ("Charles Brown", "charles@email.com", "11977665544", "Developer",
         6000.00, "2023-03-10"),
    ]

    schema = StructType([
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("position", StringType(), True),
        StructField("salary", StringType(), True),
        StructField("hire_date", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema)

    # Initialize metadata manager
    metadata_mgr = MetadataManager(spark)

    # Define tags for columns (including retention in days)
    column_tags = {
        "name": {"gdpr_category": "PII", "sensitivity": "HIGH",
                 "purpose": "identification", "retention_days": 3650},  # 10y
        "email": {"gdpr_category": "PII", "sensitivity": "MEDIUM",
                  "purpose": "contact", "retention_days": 1825},       # 5y
        "phone": {"gdpr_category": "PII", "sensitivity": "MEDIUM",
                  "purpose": "contact", "retention_days": 1825},       # 5y
        "position": {"gdpr_category": "NPII", "sensitivity": "LOW",
                     "purpose": "organization"},  # No specific retention
        "salary": {"gdpr_category": "SPI", "sensitivity": "VERY_HIGH",
                   "purpose": "payment", "retention_days": 2555},      # 7y
        "hire_date": {"gdpr_category": "NPII", "sensitivity": "LOW",
                      "purpose": "history"}  # No specific retention
    }

    # Apply tags to columns
    tagged_df = metadata_mgr.apply_column_tags(df, column_tags)

    # Apply tags to dataset
    dataset_tags = {
        "owner": "hr_department",
        "purpose": "employee_management",
        "classification": "CONFIDENTIAL"
    }
    tagged_df = metadata_mgr.apply_dataset_tags(tagged_df, dataset_tags)

    # Show column metadata
    print("\n--- Tags applied to columns ---")
    print(json.dumps(metadata_mgr.get_column_tags(tagged_df), indent=2))

    # Initialize anonymization manager
    anon_mgr = AnonymizationManager(metadata_mgr)

    # Apply automatic anonymization
    anon_df = anon_mgr.auto_anonymize(tagged_df)

    print("\n--- Original data ---")
    df.show(truncate=False)

    print("\n--- Data after anonymization ---")
    # Select relevant columns to show anonymization effect clearly
    anon_df.select(
        "name", "email", "phone", "salary", "position"
    ).show(truncate=False)

    # Export metadata catalog
    catalog_path = "metadata_catalog.json"
    metadata_mgr.export_metadata_catalog(tagged_df, catalog_path)
    print(f"\nMetadata catalog exported to '{catalog_path}'")

    spark.stop()


if __name__ == "__main__":
    usage_example()