#!/usr/bin/env python3
"""
Alma Digital Upload - Unified Tool

A unified command-line tool for uploading digital files to Alma ILS
with support for multiple matching strategies.

Supported strategies:
- marc-907e: Match files using MARC 907$e field values (folder-path workflow)
- mms-id-filename: Match files by MMS ID in filename (filename-based workflow)

Usage:
    # Folder-path workflow (MARC 907$e matching)
    poetry run python alma_digital_upload.py --config config.json --match-strategy marc-907e

    # Filename-based workflow (MMS ID filename matching)
    poetry run python alma_digital_upload.py --config config.json --match-strategy mms-id-filename

    # Run specific step only
    poetry run python alma_digital_upload.py --config config.json --step 3

    # Dry-run mode (default for safety)
    poetry run python alma_digital_upload.py --config config.json --dry-run
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3

from almaapitk import AlmaAPIClient, Admin, BibliographicRecords

from strategies import Marc907eStrategy, MmsIdFilenameStrategy, MatchStrategy
from strategies.base import MatchResult, UploadRecord


def setup_logging(output_dir: str, prefix: str = "alma_upload") -> logging.Logger:
    """
    Setup logging to both file and console.

    Args:
        output_dir: Directory for log file
        prefix: Prefix for log file name

    Returns:
        Configured logger
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(output_dir, f"{prefix}_{timestamp}.log")

    logger = logging.getLogger("AlmaDigitalUpload")
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        logger.handlers.clear()

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(file_formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter("%(levelname)s: %(message)s")
    console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(f"Logging initialized - Log file: {log_file}")
    return logger


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from JSON file.

    Args:
        config_path: Path to configuration file

    Returns:
        Configuration dictionary
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)

        return config

    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in configuration file: {e}")


def get_strategy(strategy_name: str, config: Dict[str, Any]) -> MatchStrategy:
    """
    Get the matching strategy instance.

    Args:
        strategy_name: Name of the strategy (marc-907e or mms-id-filename)
        config: Configuration dictionary

    Returns:
        MatchStrategy instance
    """
    strategies = {
        "marc-907e": Marc907eStrategy,
        "mms-id-filename": MmsIdFilenameStrategy,
    }

    if strategy_name not in strategies:
        raise ValueError(
            f"Unknown strategy: {strategy_name}. "
            f"Available strategies: {', '.join(strategies.keys())}"
        )

    return strategies[strategy_name](config)


class AlmaDigitalUploader:
    """
    Unified digital file uploader for Alma.

    Orchestrates the complete upload workflow:
    1. Get MMS IDs from Alma set
    2. Match files using selected strategy
    3. Create representations in Alma
    4. Upload files to AWS S3
    5. Link files to representations
    """

    def __init__(
        self,
        config: Dict[str, Any],
        strategy: MatchStrategy,
        logger: logging.Logger,
    ):
        """
        Initialize the uploader.

        Args:
            config: Configuration dictionary
            strategy: Matching strategy instance
            logger: Logger instance
        """
        self.config = config
        self.strategy = strategy
        self.logger = logger

        # Extract common config
        alma_config = config.get("alma", {})
        self.environment = alma_config.get("environment", "SANDBOX")
        self.set_id = alma_config.get("set_id", "")
        self.library_code = alma_config.get("library_code", "")
        self.access_rights_code = alma_config.get("access_rights_code", "")
        self.access_rights_desc = alma_config.get("access_rights_desc", "")

        aws_config = config.get("aws", {})
        self.institution_code = aws_config.get("institution_code", "972TAU_INST")

        options = config.get("options", {})
        self.dry_run = options.get("dry_run", True)

        # Initialize clients
        self.alma_client: Optional[AlmaAPIClient] = None
        self.admin: Optional[Admin] = None
        self.bibs: Optional[BibliographicRecords] = None

    def initialize_clients(self) -> None:
        """Initialize Alma API clients."""
        self.logger.info(f"Initializing Alma API client for {self.environment}")

        self.alma_client = AlmaAPIClient(self.environment)

        if not self.alma_client.test_connection():
            raise RuntimeError("Failed to connect to Alma API")

        self.admin = Admin(self.alma_client)
        self.bibs = BibliographicRecords(self.alma_client)

        self.logger.info(f"Connected to Alma API ({self.environment})")

    def step1_get_set_members(self) -> List[str]:
        """
        Step 1: Get MMS IDs from Alma set.

        Returns:
            List of MMS IDs
        """
        self.logger.info("=== Step 1: Retrieving Set Members ===")
        self.logger.info(f"Set ID: {self.set_id}")

        # Get set info first
        set_info = self.admin.get_set_info(self.set_id)
        set_name = set_info.get("name", "Unknown")
        content_type = set_info.get("content_type", "Unknown")
        total_members = set_info.get("total_members", 0)

        self.logger.info(f"Set: {set_name}")
        self.logger.info(f"Content type: {content_type}")
        self.logger.info(f"Total members: {total_members}")

        if content_type != "BIB_MMS":
            raise ValueError(f"Set must be BIB_MMS type, got: {content_type}")

        # Get members
        mms_ids = self.admin.get_bib_set_members(self.set_id)

        if not mms_ids:
            raise ValueError(f"Set {self.set_id} is empty")

        self.logger.info(f"Retrieved {len(mms_ids)} MMS IDs from set")
        return mms_ids

    def step2_match_files(self, mms_ids: List[str]) -> List[MatchResult]:
        """
        Step 2: Match files using the selected strategy.

        Args:
            mms_ids: List of MMS IDs

        Returns:
            List of MatchResult objects
        """
        self.logger.info("=== Step 2: Matching Files ===")
        self.logger.info(f"Strategy: {self.strategy.name}")
        self.logger.info(f"Description: {self.strategy.description}")

        match_results = self.strategy.match(mms_ids, self.bibs)

        # Summary
        matched = sum(1 for r in match_results if r.matched)
        self.logger.info(f"\n=== Matching Summary ===")
        self.logger.info(f"Total records: {len(match_results)}")
        self.logger.info(f"Matched: {matched}")
        self.logger.info(f"Not matched: {len(match_results) - matched}")

        return match_results

    def step3_create_representations(
        self,
        match_results: List[MatchResult],
    ) -> List[MatchResult]:
        """
        Step 3: Create representations for matched records.

        Args:
            match_results: List of MatchResult objects

        Returns:
            Updated match results with representation info
        """
        self.logger.info("=== Step 3: Representation Management ===")

        matched_records = [r for r in match_results if r.matched]
        self.logger.info(f"Processing {len(matched_records)} matched records")

        created = 0
        existing = 0
        failed = 0
        skipped = 0

        for i, result in enumerate(matched_records, 1):
            mms_id = result.mms_id

            if i % 10 == 0 or i == len(matched_records):
                self.logger.info(
                    f"Processing representation {i}/{len(matched_records)}: {mms_id}"
                )

            try:
                # Check existing representations
                existing_reps = self.bibs.get_representations(mms_id)

                if existing_reps.success:
                    reps_data = existing_reps.json()
                    existing_count = reps_data.get("total_record_count", 0)

                    if existing_count > 0:
                        # Use existing representation
                        reps_list = reps_data.get("representation", [])
                        if isinstance(reps_list, list) and reps_list:
                            result.metadata["representation_id"] = reps_list[0].get(
                                "id"
                            )
                        elif isinstance(reps_list, dict):
                            result.metadata["representation_id"] = reps_list.get("id")

                        result.metadata["representation_action"] = "exists"
                        existing += 1
                        continue

                # Create new representation
                if self.dry_run:
                    self.logger.debug(
                        f"[DRY RUN] Would create representation for {mms_id}"
                    )
                    result.metadata["representation_action"] = "dry_run"
                    result.metadata["representation_id"] = "DRY_RUN_ID"
                    skipped += 1
                else:
                    create_response = self.bibs.create_representation(
                        mms_id=mms_id,
                        access_rights_value=self.access_rights_code,
                        access_rights_desc=self.access_rights_desc,
                        lib_code=self.library_code,
                        usage_type="PRESERVATION_MASTER",
                    )

                    if create_response.success:
                        rep_data = create_response.json()
                        result.metadata["representation_id"] = rep_data.get("id")
                        result.metadata["representation_action"] = "created"
                        created += 1
                        self.logger.info(
                            f"Created representation for {mms_id}: {rep_data.get('id')}"
                        )
                    else:
                        result.metadata["representation_action"] = "failed"
                        result.metadata["representation_error"] = (
                            f"HTTP {create_response.status_code}"
                        )
                        failed += 1
                        self.logger.error(
                            f"Failed to create representation for {mms_id}"
                        )

            except Exception as e:
                result.metadata["representation_action"] = "error"
                result.metadata["representation_error"] = str(e)
                failed += 1
                self.logger.error(f"Error processing {mms_id}: {e}")

        self.logger.info(f"\n=== Representation Summary ===")
        self.logger.info(f"Created: {created}")
        self.logger.info(f"Already existing: {existing}")
        self.logger.info(f"Failed: {failed}")
        self.logger.info(f"Skipped (dry-run): {skipped}")

        return match_results

    def _get_aws_credentials(self) -> Tuple[str, str, str]:
        """
        Get AWS credentials from environment.

        Returns:
            Tuple of (access_key, secret_key, bucket_name)
        """
        access_key = os.getenv("AWS_ACCESS_KEY")
        secret_key = os.getenv("AWS_SECRET")

        if self.environment == "SANDBOX":
            bucket_name = os.getenv("ALMA_SB_BUCKET_NAME")
            bucket_var = "ALMA_SB_BUCKET_NAME"
        else:
            bucket_name = os.getenv("ALMA_PROD_BUCKET_NAME")
            bucket_var = "ALMA_PROD_BUCKET_NAME"

        if not access_key:
            raise ValueError("AWS_ACCESS_KEY environment variable not set")
        if not secret_key:
            raise ValueError("AWS_SECRET environment variable not set")
        if not bucket_name:
            raise ValueError(f"{bucket_var} environment variable not set")

        return access_key, secret_key, bucket_name

    def _upload_file_to_s3(
        self,
        file_path: str,
        s3_path_key: str,
        aws_credentials: Tuple[str, str, str],
    ) -> str:
        """
        Upload a file to S3.

        Args:
            file_path: Local file path
            s3_path_key: Key to use in S3 path
            aws_credentials: AWS credentials tuple

        Returns:
            S3 key of uploaded file
        """
        access_key, secret_key, bucket_name = aws_credentials

        s3_resource = boto3.resource(
            service_name="s3",
            region_name="eu-central-1",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

        filename = os.path.basename(file_path)
        s3_key = f"{self.institution_code}/upload/{s3_path_key}/{filename}"

        bucket = s3_resource.Bucket(bucket_name)
        with open(file_path, "rb") as file_data:
            bucket.put_object(Key=s3_key, Body=file_data)

        return s3_key

    def step4_upload_and_link(
        self,
        match_results: List[MatchResult],
    ) -> List[MatchResult]:
        """
        Step 4: Upload files to AWS and link to representations.

        Args:
            match_results: Match results with representation info

        Returns:
            Updated match results with upload info
        """
        self.logger.info("=== Step 4: Upload and Link Files ===")

        aws_credentials = self._get_aws_credentials()
        self.logger.info(f"AWS credentials loaded for {self.environment}")

        matched_with_rep = [
            r
            for r in match_results
            if r.matched and r.metadata.get("representation_id")
        ]

        self.logger.info(
            f"Processing {len(matched_with_rep)} records with representations"
        )

        files_uploaded = 0
        files_linked = 0
        failed_uploads = 0
        failed_links = 0

        for i, result in enumerate(matched_with_rep, 1):
            mms_id = result.mms_id
            representation_id = result.metadata.get("representation_id")
            s3_path_key = self.strategy.get_s3_path_key(result)

            if i % 10 == 0 or i == len(matched_with_rep):
                self.logger.info(
                    f"Processing upload {i}/{len(matched_with_rep)}: {mms_id}"
                )

            result.metadata["uploaded_files"] = []
            result.metadata["linked_files"] = []

            for file_path in result.file_paths:
                filename = os.path.basename(file_path)

                try:
                    if self.dry_run:
                        self.logger.debug(
                            f"[DRY RUN] Would upload: {filename} -> {s3_path_key}"
                        )
                        files_uploaded += 1
                        files_linked += 1
                        continue

                    # Upload to S3
                    s3_key = self._upload_file_to_s3(
                        file_path, s3_path_key, aws_credentials
                    )
                    files_uploaded += 1
                    result.metadata["uploaded_files"].append(s3_key)

                    # Link to representation
                    link_response = self.bibs.link_file_to_representation(
                        mms_id=mms_id,
                        representation_id=representation_id,
                        file_path=s3_key,
                    )

                    if link_response.success:
                        files_linked += 1
                        result.metadata["linked_files"].append(s3_key)
                        self.logger.debug(f"Uploaded and linked: {filename}")
                    else:
                        failed_links += 1
                        self.logger.warning(
                            f"Uploaded but link failed: {filename}"
                        )

                except Exception as e:
                    failed_uploads += 1
                    self.logger.error(f"Upload failed for {filename}: {e}")

        self.logger.info(f"\n=== Upload Summary ===")
        self.logger.info(f"Files uploaded: {files_uploaded}")
        self.logger.info(f"Files linked: {files_linked}")
        self.logger.info(f"Upload failures: {failed_uploads}")
        self.logger.info(f"Link failures: {failed_links}")

        return match_results

    def run(self, step: str = "all") -> None:
        """
        Run the upload workflow.

        Args:
            step: Step to run ("1", "2", "3", "4", "all")
        """
        self.logger.info("=== Alma Digital Upload Started ===")
        self.logger.info(f"Strategy: {self.strategy.name}")
        self.logger.info(f"Environment: {self.environment}")
        self.logger.info(f"Set ID: {self.set_id}")
        self.logger.info(f"Dry-run: {self.dry_run}")
        self.logger.info(f"Step: {step}")

        # Initialize clients
        self.initialize_clients()

        # Validate strategy config
        validation_errors = self.strategy.validate_config()
        if validation_errors:
            for error in validation_errors:
                self.logger.error(f"Config error: {error}")
            raise ValueError("Configuration validation failed")

        # Step 1: Get set members
        mms_ids = self.step1_get_set_members()

        if step == "1":
            self.logger.info("\n=== Step 1 Complete ===")
            return

        # Step 2: Match files
        match_results = self.step2_match_files(mms_ids)

        if step == "2":
            self.logger.info("\n=== Step 2 Complete ===")
            return

        # Step 3: Create representations
        match_results = self.step3_create_representations(match_results)

        if step == "3":
            self.logger.info("\n=== Step 3 Complete ===")
            return

        # Step 4: Upload and link
        match_results = self.step4_upload_and_link(match_results)

        if step == "4":
            self.logger.info("\n=== Step 4 Complete ===")
            return

        self.logger.info("\n=== All Steps Complete! ===")
        self.logger.info("Workflow completed successfully")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Alma Digital Upload - Upload digital files to Alma ILS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Folder-path workflow (MARC 907$e matching)
  python alma_digital_upload.py --config config.json --match-strategy marc-907e

  # Filename-based workflow (MMS ID filename matching)
  python alma_digital_upload.py --config config.json --match-strategy mms-id-filename

  # Dry-run mode (default, no actual changes)
  python alma_digital_upload.py --config config.json --dry-run

  # Live mode (actually uploads and links)
  python alma_digital_upload.py --config config.json --live

  # Run specific step only
  python alma_digital_upload.py --config config.json --step 2
        """,
    )

    parser.add_argument(
        "--config",
        "-c",
        required=True,
        help="Path to JSON configuration file",
    )
    parser.add_argument(
        "--match-strategy",
        "-m",
        choices=["marc-907e", "mms-id-filename"],
        help="File matching strategy (overrides config)",
    )
    parser.add_argument(
        "--step",
        choices=["1", "2", "3", "4", "all"],
        default="all",
        help="Run specific step only or all steps",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Dry-run mode (default, no actual changes)",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Live mode (perform actual uploads and changes)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    try:
        # Load configuration
        config = load_config(args.config)

        # Override strategy from command line if provided
        strategy_name = args.match_strategy or config.get("matching", {}).get(
            "strategy", "marc-907e"
        )

        # Override dry-run from command line
        if args.live:
            if "options" not in config:
                config["options"] = {}
            config["options"]["dry_run"] = False
        elif args.dry_run:
            if "options" not in config:
                config["options"] = {}
            config["options"]["dry_run"] = True

        # Setup logging
        output_dir = config.get("output_settings", {}).get(
            "output_directory", "./output"
        )
        logger = setup_logging(output_dir)

        # Get strategy
        strategy = get_strategy(strategy_name, config)

        # Create uploader and run
        uploader = AlmaDigitalUploader(config, strategy, logger)
        uploader.run(args.step)

        print("\nUpload completed successfully!")

    except Exception as e:
        print(f"\nERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
