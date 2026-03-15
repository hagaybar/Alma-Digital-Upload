#!/usr/bin/env python3
"""
Folder Matching Utility.

Matches values from TSV files (like 907$l_cleaned) to folder names
in a local directory and generates matching reports.
"""

import csv
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


@dataclass
class MatchReport:
    """Result of matching a TSV record to a folder."""

    row_number: int
    mms_id: str
    value_907e: str = ""
    value_907l_cleaned: str = ""
    match_status: str = "pending"  # MATCHED, NOT_FOUND, EMPTY_VALUE
    folder_exists: bool = False


class FolderMatcher:
    """
    Matches TSV values to folder names.

    Features:
    - Reads TSV files with MARC 907 data
    - Matches 907$l_cleaned values to folder names
    - Generates detailed matching reports
    - Identifies unmatched folders
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize folder matcher.

        Args:
            config: Optional configuration dictionary
        """
        self.config = config or {}
        output = self.config.get("output_settings", {})
        self.output_directory = output.get("output_directory", "./output")

    @staticmethod
    def convert_windows_path(path: str) -> str:
        """
        Convert Windows path to WSL path if needed.

        Args:
            path: Windows or WSL path

        Returns:
            WSL-compatible path

        Examples:
            "C:\\Users\\name\\folder" -> "/mnt/c/Users/name/folder"
            "/mnt/c/Users/name" -> "/mnt/c/Users/name"
        """
        if len(path) > 1 and path[1] == ":":
            drive_letter = path[0].lower()
            rest_of_path = path[2:].replace("\\", "/")
            return f"/mnt/{drive_letter}{rest_of_path}"
        return path

    def read_tsv_file(self, tsv_path: str) -> List[Dict[str, str]]:
        """
        Read TSV file and return list of records.

        Args:
            tsv_path: Path to TSV file

        Returns:
            List of dictionaries, one per row
        """
        records = []

        try:
            with open(tsv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f, delimiter="\t")
                for row in reader:
                    records.append(row)

            logger.info(f"Read {len(records)} records from TSV file")
            return records

        except FileNotFoundError:
            logger.error(f"TSV file not found: {tsv_path}")
            raise
        except Exception as e:
            logger.error(f"Error reading TSV file: {e}")
            raise

    def list_folders(self, folder_path: str) -> Set[str]:
        """
        List all folder names in the specified directory.

        Args:
            folder_path: Path to directory (Windows or WSL format)

        Returns:
            Set of folder names
        """
        wsl_path = self.convert_windows_path(folder_path)

        try:
            path = Path(wsl_path)

            if not path.exists():
                raise FileNotFoundError(
                    f"Directory does not exist: {folder_path} (checked: {wsl_path})"
                )

            if not path.is_dir():
                raise NotADirectoryError(f"Path is not a directory: {folder_path}")

            folders = {item.name for item in path.iterdir() if item.is_dir()}

            logger.info(f"Found {len(folders)} folders in directory")
            return folders

        except Exception as e:
            logger.error(f"Error listing folders: {e}")
            raise

    def match_records_to_folders(
        self,
        records: List[Dict[str, str]],
        folders: Set[str],
        value_column: str = "907$l_cleaned",
    ) -> Tuple[List[MatchReport], Set[str]]:
        """
        Match TSV records to folder names.

        Args:
            records: List of TSV records
            folders: Set of folder names
            value_column: Column name to match against folder names

        Returns:
            Tuple of (match reports, unmatched folders)
        """
        results = []
        matched_folders: Set[str] = set()

        for idx, record in enumerate(records, 1):
            mms_id = record.get("MMS_ID", "")
            value = record.get(value_column, "").strip()
            field_907e = record.get("907$e", "")
            field_907l = record.get("907$l_cleaned", "")

            if value and value in folders:
                match_status = "MATCHED"
                matched_folders.add(value)
                folder_exists = True
            elif not value:
                match_status = "EMPTY_VALUE"
                folder_exists = False
            else:
                match_status = "NOT_FOUND"
                folder_exists = False

            results.append(
                MatchReport(
                    row_number=idx,
                    mms_id=mms_id,
                    value_907e=field_907e,
                    value_907l_cleaned=field_907l,
                    match_status=match_status,
                    folder_exists=folder_exists,
                )
            )

        unmatched_folders = folders - matched_folders

        logger.info(f"Matching complete:")
        logger.info(f"  - Total records: {len(results)}")
        logger.info(
            f"  - Matched: {sum(1 for r in results if r.match_status == 'MATCHED')}"
        )
        logger.info(
            f"  - Not found: {sum(1 for r in results if r.match_status == 'NOT_FOUND')}"
        )
        logger.info(
            f"  - Empty values: {sum(1 for r in results if r.match_status == 'EMPTY_VALUE')}"
        )
        logger.info(f"  - Folders matched: {len(matched_folders)}")
        logger.info(f"  - Folders not in TSV: {len(unmatched_folders)}")

        return results, unmatched_folders

    def write_report(
        self,
        results: List[MatchReport],
        unmatched_folders: Set[str],
        output_path: Optional[str] = None,
    ) -> str:
        """
        Write matching report to TSV file.

        Args:
            results: List of MatchReport objects
            unmatched_folders: Set of folders not matched
            output_path: Output file path (auto-generated if None)

        Returns:
            Path to created report file
        """
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_dir = Path(self.output_directory)
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = str(output_dir / f"folder_match_report_{timestamp}.tsv")

        try:
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter="\t")

                # Write header
                writer.writerow(
                    [
                        "Row_Number",
                        "MMS_ID",
                        "907$e",
                        "907$l_cleaned",
                        "Match_Status",
                        "Folder_Exists",
                    ]
                )

                # Write data rows
                for result in results:
                    writer.writerow(
                        [
                            result.row_number,
                            result.mms_id,
                            result.value_907e,
                            result.value_907l_cleaned,
                            result.match_status,
                            "YES" if result.folder_exists else "NO",
                        ]
                    )

                # Add separator and unmatched folders section
                if unmatched_folders:
                    writer.writerow([])
                    writer.writerow(["FOLDERS NOT IN TSV:", "", "", "", "", ""])
                    writer.writerow(["Folder_Name", "", "", "", "", ""])

                    for folder in sorted(unmatched_folders):
                        writer.writerow([folder, "", "", "", "", ""])

            logger.info(f"Report written to: {output_path}")
            return output_path

        except Exception as e:
            logger.error(f"Error writing report: {e}")
            raise

    def get_statistics(
        self,
        results: List[MatchReport],
        unmatched_folders: Set[str],
    ) -> Dict[str, int]:
        """
        Calculate matching statistics.

        Args:
            results: List of MatchReport objects
            unmatched_folders: Set of unmatched folders

        Returns:
            Dictionary with statistics
        """
        return {
            "total_records": len(results),
            "matched": sum(1 for r in results if r.match_status == "MATCHED"),
            "not_found": sum(1 for r in results if r.match_status == "NOT_FOUND"),
            "empty_values": sum(1 for r in results if r.match_status == "EMPTY_VALUE"),
            "unmatched_folders": len(unmatched_folders),
        }

    def display_summary(
        self,
        results: List[MatchReport],
        unmatched_folders: Set[str],
    ) -> None:
        """
        Display summary statistics.

        Args:
            results: List of MatchReport objects
            unmatched_folders: Set of unmatched folders
        """
        stats = self.get_statistics(results, unmatched_folders)

        matched = [r for r in results if r.match_status == "MATCHED"]
        not_found = [r for r in results if r.match_status == "NOT_FOUND"]

        print("\n" + "=" * 60)
        print("FOLDER MATCHING SUMMARY")
        print("=" * 60)
        print(f"Total TSV records: {stats['total_records']}")
        print(f"  - Matched to folders: {stats['matched']}")
        print(f"  - Not found in folders: {stats['not_found']}")
        print(f"  - Empty values: {stats['empty_values']}")
        print(f"Folders not referenced in TSV: {stats['unmatched_folders']}")

        if not_found:
            print(f"\nFirst 10 values not found in folders:")
            for result in not_found[:10]:
                print(
                    f"  - Row {result.row_number}: {result.value_907l_cleaned} (MMS: {result.mms_id})"
                )

        if unmatched_folders and len(unmatched_folders) <= 20:
            print(f"\nFolders not in TSV:")
            for folder in sorted(unmatched_folders):
                print(f"  - {folder}")
        elif unmatched_folders:
            print(f"\nFolders not in TSV (first 20):")
            for folder in sorted(list(unmatched_folders))[:20]:
                print(f"  - {folder}")
            print(f"  ... and {len(unmatched_folders) - 20} more")

        print("=" * 60)
