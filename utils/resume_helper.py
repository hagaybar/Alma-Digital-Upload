#!/usr/bin/env python3
"""
Resume Helper Utility.

Helps resume upload operations from where they stopped by analyzing
log files and creating resume TSV files with only unprocessed records.
"""

import csv
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


@dataclass
class ProcessingResults:
    """Results from analyzing a log file."""

    completed_successfully: Set[str] = field(default_factory=set)
    processed_with_errors: Set[str] = field(default_factory=set)
    skipped_no_marc: Set[str] = field(default_factory=set)
    skipped_no_path: Set[str] = field(default_factory=set)

    @property
    def exclude_from_resume(self) -> Set[str]:
        """Records to exclude from resume file."""
        return (
            self.completed_successfully | self.skipped_no_marc | self.skipped_no_path
        )

    @property
    def total_processed(self) -> int:
        """Total records that were processed."""
        return (
            len(self.completed_successfully)
            + len(self.processed_with_errors)
            + len(self.skipped_no_marc)
            + len(self.skipped_no_path)
        )


@dataclass
class LogFileInfo:
    """Information about a log file."""

    path: str
    size_bytes: int
    size_kb: float
    line_count: int
    created: str
    modified: str
    first_timestamp: str
    last_timestamp: str


class ResumeHelper:
    """
    Helper for resuming upload operations.

    Features:
    - Analyzes log files to find processed records
    - Creates resume TSV with only unprocessed records
    - Creates resume config for continuing operation
    - Categorizes records by processing status
    """

    def __init__(self, output_directory: str = "./output"):
        """
        Initialize resume helper.

        Args:
            output_directory: Directory for output files
        """
        self.output_directory = output_directory

    def find_log_files(
        self,
        search_dirs: Optional[List[str]] = None,
        pattern: str = "alma_loader_*.log",
    ) -> List[Path]:
        """
        Find log files in search directories.

        Args:
            search_dirs: Directories to search (default: common locations)
            pattern: Glob pattern for log files

        Returns:
            List of log file paths, sorted by modification time (newest first)
        """
        if search_dirs is None:
            search_dirs = ["./output", ".", "../output", "./logs"]

        all_log_files = []

        for search_dir in search_dirs:
            if os.path.exists(search_dir):
                log_files = list(Path(search_dir).glob(pattern))
                for log_file in log_files:
                    mtime = os.path.getmtime(log_file)
                    all_log_files.append((log_file, mtime))

        # Sort by modification time (newest first)
        all_log_files.sort(key=lambda x: x[1], reverse=True)

        return [f[0] for f in all_log_files]

    def get_log_file_info(self, log_file_path: str) -> LogFileInfo:
        """
        Get detailed information about a log file.

        Args:
            log_file_path: Path to log file

        Returns:
            LogFileInfo object
        """
        try:
            stat = os.stat(log_file_path)

            with open(log_file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()

            first_line = lines[0].strip() if lines else ""
            last_line = lines[-1].strip() if lines else ""

            first_timestamp = first_line[:19] if len(first_line) >= 19 else "Unknown"
            last_timestamp = last_line[:19] if len(last_line) >= 19 else "Unknown"

            return LogFileInfo(
                path=log_file_path,
                size_bytes=stat.st_size,
                size_kb=round(stat.st_size / 1024, 1),
                line_count=len(lines),
                created=datetime.fromtimestamp(stat.st_ctime).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                modified=datetime.fromtimestamp(stat.st_mtime).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                first_timestamp=first_timestamp,
                last_timestamp=last_timestamp,
            )

        except Exception as e:
            logger.error(f"Error reading log info: {e}")
            raise

    def extract_processed_mms_ids(self, log_file_path: str) -> ProcessingResults:
        """
        Extract MMS IDs by processing status from log file.

        Args:
            log_file_path: Path to log file

        Returns:
            ProcessingResults with categorized MMS IDs
        """
        logger.info("Extracting processed MMS IDs by category...")

        results = ProcessingResults()

        try:
            with open(log_file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()

            current_processing_mms = None

            for line in lines:
                # Track which MMS ID is being processed
                if "Processing path construction" in line and ":" in line:
                    parts = line.split(":")
                    if len(parts) > 1:
                        mms_part = parts[-1].strip()
                        if mms_part.isdigit() and len(mms_part) >= 13:
                            current_processing_mms = mms_part

                # Successful completion
                if "files uploaded and linked successfully" in line and "✓" in line:
                    checkmark_pos = line.find("✓")
                    if checkmark_pos != -1:
                        after_checkmark = line[checkmark_pos + 1 :].strip()
                        if ":" in after_checkmark:
                            mms_id_part = after_checkmark.split(":")[0].strip()
                            if mms_id_part.isdigit() and len(mms_id_part) >= 13:
                                results.completed_successfully.add(mms_id_part)

                # Processed with errors
                elif "⚠️" in line and "upload errors," in line and "link errors" in line:
                    warning_pos = line.find("⚠️")
                    if warning_pos != -1:
                        after_warning = line[warning_pos + 1 :].strip()
                        if ":" in after_warning:
                            mms_id_part = after_warning.split(":")[0].strip()
                            if mms_id_part.isdigit() and len(mms_id_part) >= 13:
                                results.processed_with_errors.add(mms_id_part)

                # No MARC 907$e
                elif "No MARC 907$e found for MMS ID:" in line and "⚠️" in line:
                    parts = line.split("No MARC 907$e found for MMS ID:")
                    if len(parts) > 1:
                        mms_id_part = parts[1].strip()
                        if mms_id_part.isdigit() and len(mms_id_part) >= 13:
                            results.skipped_no_marc.add(mms_id_part)

                # Path does not exist
                elif "Path does not exist:" in line and "⚠️" in line:
                    if current_processing_mms:
                        results.skipped_no_path.add(current_processing_mms)
                        current_processing_mms = None

                # Reset on successful path
                elif "Path exists:" in line and "✓" in line:
                    current_processing_mms = None

            logger.info(f"\n=== Processing Status Summary ===")
            logger.info(
                f"Completed successfully: {len(results.completed_successfully)} (exclude from resume)"
            )
            logger.info(
                f"Processed with errors: {len(results.processed_with_errors)} (INCLUDE in resume)"
            )
            logger.info(
                f"Skipped (no MARC): {len(results.skipped_no_marc)} (exclude from resume)"
            )
            logger.info(
                f"Skipped (no path): {len(results.skipped_no_path)} (exclude from resume)"
            )
            logger.info(
                f"Total to exclude from resume: {len(results.exclude_from_resume)}"
            )

            return results

        except Exception as e:
            logger.error(f"Error extracting processed IDs: {e}")
            raise

    def create_resume_tsv(
        self,
        original_tsv_path: str,
        processed_ids: Set[str],
        output_path: Optional[str] = None,
    ) -> str:
        """
        Create a new TSV file with only unprocessed records.

        Args:
            original_tsv_path: Path to original TSV file
            processed_ids: Set of MMS IDs already processed (to exclude)
            output_path: Output file path (auto-generated if None)

        Returns:
            Path to the new resume TSV file
        """
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(
                self.output_directory, f"resume_tsv_{timestamp}.tsv"
            )

        logger.info(f"Creating resume TSV file: {output_path}")
        logger.info(f"Original TSV: {original_tsv_path}")
        logger.info(f"Excluding {len(processed_ids)} already processed records")

        try:
            unprocessed_records = []
            total_records = 0

            with open(original_tsv_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f, delimiter="\t")

                for row in reader:
                    total_records += 1

                    if len(row) < 1:
                        continue

                    mms_id = row[0].strip()

                    if mms_id not in processed_ids:
                        unprocessed_records.append(row)

            # Write resume TSV
            output_dir = Path(output_path).parent
            output_dir.mkdir(parents=True, exist_ok=True)

            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter="\t")
                writer.writerows(unprocessed_records)

            logger.info(f"\nResume TSV Summary:")
            logger.info(f"  Original records: {total_records}")
            logger.info(f"  Already processed: {len(processed_ids)}")
            logger.info(f"  Remaining to process: {len(unprocessed_records)}")
            logger.info(f"Resume TSV created: {output_path}")

            return output_path

        except Exception as e:
            logger.error(f"Error creating resume TSV: {e}")
            raise

    def create_resume_config(
        self,
        original_config_path: str,
        resume_tsv_path: str,
        output_config_path: Optional[str] = None,
    ) -> str:
        """
        Create a new config file that uses the resume TSV.

        Args:
            original_config_path: Path to original config
            resume_tsv_path: Path to resume TSV file
            output_config_path: Output config path (auto-generated if None)

        Returns:
            Path to new config file
        """
        if output_config_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_config_path = os.path.join(
                self.output_directory, f"resume_config_{timestamp}.json"
            )

        logger.info(f"Creating resume config: {output_config_path}")

        try:
            with open(original_config_path, "r") as f:
                config = json.load(f)

            # Modify config for direct TSV input
            if "input" not in config:
                config["input"] = {}

            config["input"]["use_direct_tsv"] = True
            config["input"]["direct_tsv_path"] = resume_tsv_path
            config["input"]["skip_tsv_generation"] = True

            # Update output settings
            if "output_settings" not in config:
                config["output_settings"] = {}
            config["output_settings"]["file_prefix"] = "alma_resume"

            # Write new config
            with open(output_config_path, "w") as f:
                json.dump(config, f, indent=2)

            logger.info(f"Resume config created: {output_config_path}")
            return output_config_path

        except Exception as e:
            logger.error(f"Error creating resume config: {e}")
            raise

    def display_analysis(self, results: ProcessingResults) -> None:
        """
        Display detailed analysis of processing results.

        Args:
            results: ProcessingResults object
        """
        print("\n" + "=" * 60)
        print("RESUME ANALYSIS")
        print("=" * 60)
        print(
            f"Completed successfully: {len(results.completed_successfully)} (exclude)"
        )
        print(f"Processed with errors: {len(results.processed_with_errors)} (RETRY)")
        print(f"Skipped (no MARC): {len(results.skipped_no_marc)} (exclude)")
        print(f"Skipped (no path): {len(results.skipped_no_path)} (exclude)")
        print(f"\nTotal to exclude from resume: {len(results.exclude_from_resume)}")
        print(
            f"Records that will be retried: {len(results.processed_with_errors)}"
        )

        if results.skipped_no_marc:
            print(
                f"\nMMS IDs skipped (no MARC 907$e): {len(results.skipped_no_marc)}"
            )
            for mms_id in sorted(list(results.skipped_no_marc))[:10]:
                print(f"  - {mms_id}")
            if len(results.skipped_no_marc) > 10:
                print(f"  ... and {len(results.skipped_no_marc) - 10} more")

        if results.skipped_no_path:
            print(
                f"\nMMS IDs with path not found: {len(results.skipped_no_path)}"
            )
            for mms_id in sorted(list(results.skipped_no_path))[:10]:
                print(f"  - {mms_id}")
            if len(results.skipped_no_path) > 10:
                print(f"  ... and {len(results.skipped_no_path) - 10} more")

        if results.processed_with_errors:
            print(
                f"\nMMS IDs for retry (errors): {len(results.processed_with_errors)}"
            )
            for mms_id in sorted(list(results.processed_with_errors))[:10]:
                print(f"  - {mms_id}")
            if len(results.processed_with_errors) > 10:
                print(f"  ... and {len(results.processed_with_errors) - 10} more")

        print("=" * 60)
