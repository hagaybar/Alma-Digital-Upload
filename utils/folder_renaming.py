#!/usr/bin/env python3
"""
Folder Renaming Utility.

Renames folders based on MARC 907 field mapping data, converting
from 907$l_cleaned values to 907$e values.
"""

import csv
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


@dataclass
class RenameCandidate:
    """Candidate for folder renaming."""

    mms_id: str
    old_name: str
    new_name: str
    status: str = "candidate"  # candidate, ready, renamed, dry_run_ok, skipped_conflict, folder_not_found, error
    error: Optional[str] = None


class FolderRenamer:
    """
    Renames folders based on MARC 907 field mapping.

    Features:
    - Dry-run mode as default (safe by default)
    - Only processes folders where Match_907l=TRUE
    - Skips if target folder already exists
    - Windows/WSL path conversion
    - Detailed TSV report with rename status
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize folder renamer.

        Args:
            config: Configuration dictionary
        """
        self.config = config

        input_config = config.get("input", {})
        self.tsv_file = input_config.get("tsv_file", "")
        self.folder_path = input_config.get("folder_path", "")

        processing = config.get("processing", {})
        self.source_column = processing.get("source_column", "907$l_cleaned")
        self.target_column = processing.get("target_column", "907$e")
        self.match_filter = processing.get("match_filter", "Match_907l")
        self.require_match = processing.get("require_match", True)

        output = config.get("output_settings", {})
        self.output_directory = output.get("output_directory", "./output")
        self.report_prefix = output.get("report_prefix", "rename_report")

        self.dry_run = config.get("dry_run", True)

    @staticmethod
    def convert_windows_path(path: str) -> str:
        """
        Convert Windows path to WSL path if needed.

        Args:
            path: Windows or WSL path

        Returns:
            WSL-compatible path
        """
        if len(path) > 1 and path[1] == ":":
            drive_letter = path[0].lower()
            rest_of_path = path[2:].replace("\\", "/")
            return f"/mnt/{drive_letter}{rest_of_path}"
        return path

    def validate_folder_path(self) -> Path:
        """
        Validate and convert folder path.

        Returns:
            Path object for validated folder

        Raises:
            FileNotFoundError: If folder doesn't exist
            NotADirectoryError: If path is not a directory
        """
        wsl_path = self.convert_windows_path(self.folder_path)
        path = Path(wsl_path)

        if not path.exists():
            raise FileNotFoundError(
                f"Folder path does not exist: {self.folder_path} (checked: {wsl_path})"
            )
        if not path.is_dir():
            raise NotADirectoryError(f"Path is not a directory: {self.folder_path}")

        logger.info(f"Validated folder path: {path}")
        return path

    def load_rename_mapping(self) -> List[Dict[str, str]]:
        """
        Load rename mapping from TSV file.

        Returns:
            List of dictionaries, one per row
        """
        records = []

        try:
            with open(self.tsv_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f, delimiter="\t")
                for row in reader:
                    records.append(row)

            logger.info(f"Loaded {len(records)} records from {self.tsv_file}")
            return records

        except FileNotFoundError:
            logger.error(f"TSV file not found: {self.tsv_file}")
            raise

    def filter_rename_candidates(
        self,
        records: List[Dict[str, str]],
    ) -> List[RenameCandidate]:
        """
        Filter records to only those eligible for renaming.

        Rules:
        - Only include if Match_907l == 'TRUE' (when require_match=True)
        - Skip if source value is empty
        - Skip if target value is empty

        Args:
            records: List of TSV records

        Returns:
            List of RenameCandidate objects
        """
        candidates = []

        for record in records:
            # Check match filter
            if self.require_match:
                match_value = record.get(self.match_filter, "").strip()
                if match_value != "TRUE":
                    continue

            # Get source (old name)
            source_name = record.get(self.source_column, "").strip()
            if not source_name:
                continue

            # Get target (new name)
            target_name = record.get(self.target_column, "").strip()
            if not target_name:
                logger.warning(
                    f"Skipping MMS {record.get('MMS_ID', 'Unknown')}: empty target value"
                )
                continue

            candidates.append(
                RenameCandidate(
                    mms_id=record.get("MMS_ID", ""),
                    old_name=source_name,
                    new_name=target_name,
                )
            )

        logger.info(
            f"Filtered to {len(candidates)} rename candidates ({self.match_filter}=TRUE)"
        )
        return candidates

    def scan_folders(self, folder_path: Path) -> Set[str]:
        """
        Scan directory and return set of folder names.

        Args:
            folder_path: Path to directory

        Returns:
            Set of folder names
        """
        folders = {item.name for item in folder_path.iterdir() if item.is_dir()}
        logger.info(f"Found {len(folders)} folders in directory")
        return folders

    def match_folders_to_mapping(
        self,
        folders: Set[str],
        candidates: List[RenameCandidate],
    ) -> tuple[List[RenameCandidate], List[RenameCandidate]]:
        """
        Match actual folders to rename candidates.

        Args:
            folders: Set of folder names
            candidates: List of RenameCandidate objects

        Returns:
            Tuple of (matched candidates, unmatched candidates)
        """
        matched = []
        unmatched = []

        for candidate in candidates:
            if candidate.old_name in folders:
                candidate.status = "ready"
                matched.append(candidate)
            else:
                candidate.status = "folder_not_found"
                candidate.error = f"Folder '{candidate.old_name}' not found"
                unmatched.append(candidate)

        logger.info(f"Matched {len(matched)} folders to rename")
        if unmatched:
            logger.warning(f"{len(unmatched)} candidates have no matching folder")

        return matched, unmatched

    def check_for_conflicts(
        self,
        rename_plan: List[RenameCandidate],
        folder_path: Path,
    ) -> List[RenameCandidate]:
        """
        Check for naming conflicts where target folder already exists.

        Args:
            rename_plan: List of rename candidates
            folder_path: Path to folder directory

        Returns:
            Updated rename plan with conflict status
        """
        updated_plan = []
        conflicts = 0

        for item in rename_plan:
            if item.status != "ready":
                updated_plan.append(item)
                continue

            target_path = folder_path / item.new_name

            if target_path.exists():
                item.status = "skipped_conflict"
                item.error = f"Target folder '{item.new_name}' already exists"
                conflicts += 1

            updated_plan.append(item)

        if conflicts > 0:
            logger.warning(f"{conflicts} rename(s) skipped due to existing targets")

        return updated_plan

    def rename_single_folder(
        self,
        candidate: RenameCandidate,
        folder_path: Path,
    ) -> RenameCandidate:
        """
        Rename a single folder.

        Args:
            candidate: RenameCandidate to process
            folder_path: Parent directory path

        Returns:
            Updated RenameCandidate with result
        """
        old_path = folder_path / candidate.old_name
        new_path = folder_path / candidate.new_name

        try:
            if self.dry_run:
                if not old_path.exists():
                    candidate.status = "error"
                    candidate.error = "Source folder not found"
                else:
                    logger.info(
                        f"[DRY RUN] Would rename: {candidate.old_name} -> {candidate.new_name}"
                    )
                    candidate.status = "dry_run_ok"
            else:
                old_path.rename(new_path)
                logger.info(f"Renamed: {candidate.old_name} -> {candidate.new_name}")
                candidate.status = "renamed"

        except FileNotFoundError:
            candidate.status = "error"
            candidate.error = f"Source folder not found: {candidate.old_name}"
            logger.error(f"Error: {candidate.error}")
        except PermissionError:
            candidate.status = "error"
            candidate.error = f"Permission denied for: {candidate.old_name}"
            logger.error(f"Error: {candidate.error}")
        except Exception as e:
            candidate.status = "error"
            candidate.error = str(e)
            logger.error(f"Error renaming {candidate.old_name}: {e}")

        return candidate

    def execute_renames(
        self,
        rename_plan: List[RenameCandidate],
        folder_path: Path,
    ) -> List[RenameCandidate]:
        """
        Execute folder renames.

        Args:
            rename_plan: List of RenameCandidate objects
            folder_path: Parent directory path

        Returns:
            List of processed RenameCandidate objects
        """
        results = []
        ready_count = sum(1 for c in rename_plan if c.status == "ready")

        mode = "DRY RUN" if self.dry_run else "LIVE RENAME"
        logger.info(f"\n{'=' * 60}")
        logger.info(f"{mode}: {ready_count} folders to rename")
        logger.info(f"{'=' * 60}\n")

        renamed_count = 0
        skipped_count = 0
        error_count = 0

        for idx, candidate in enumerate(rename_plan, 1):
            if candidate.status != "ready":
                skipped_count += 1
                results.append(candidate)
                continue

            result = self.rename_single_folder(candidate, folder_path)

            if result.status in ("renamed", "dry_run_ok"):
                renamed_count += 1
            else:
                error_count += 1

            results.append(result)

            if idx % 10 == 0:
                logger.info(f"Progress: {idx}/{len(rename_plan)} processed")

        logger.info(f"\nRename operation complete:")
        logger.info(f"  - Renamed: {renamed_count}")
        logger.info(f"  - Skipped: {skipped_count}")
        logger.info(f"  - Errors: {error_count}")

        return results

    def write_rename_report(
        self,
        results: List[RenameCandidate],
        output_path: Optional[str] = None,
    ) -> str:
        """
        Write rename results to TSV report.

        Args:
            results: List of RenameCandidate objects
            output_path: Optional output file path

        Returns:
            Path to generated report file
        """
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_dir = Path(self.output_directory)
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = str(output_dir / f"{self.report_prefix}_{timestamp}.tsv")

        try:
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter="\t")

                writer.writerow(["MMS_ID", "Old_Name", "New_Name", "Status", "Error"])

                for result in results:
                    writer.writerow(
                        [
                            result.mms_id,
                            result.old_name,
                            result.new_name,
                            result.status,
                            result.error or "",
                        ]
                    )

            logger.info(f"Report written to: {output_path}")
            return output_path

        except Exception as e:
            logger.error(f"Failed to write report: {e}")
            raise

    def get_statistics(self, results: List[RenameCandidate]) -> Dict[str, int]:
        """
        Calculate statistics from results.

        Args:
            results: List of RenameCandidate objects

        Returns:
            Dictionary with statistics
        """
        return {
            "total": len(results),
            "renamed": sum(1 for r in results if r.status == "renamed"),
            "dry_run_ok": sum(1 for r in results if r.status == "dry_run_ok"),
            "skipped_conflict": sum(
                1 for r in results if r.status == "skipped_conflict"
            ),
            "folder_not_found": sum(
                1 for r in results if r.status == "folder_not_found"
            ),
            "errors": sum(1 for r in results if r.status == "error"),
        }

    def display_summary(self, results: List[RenameCandidate]) -> None:
        """
        Display summary statistics.

        Args:
            results: List of RenameCandidate objects
        """
        stats = self.get_statistics(results)

        print("\n" + "=" * 60)
        print("FOLDER RENAME SUMMARY")
        print("=" * 60)
        print(f"Total candidates processed: {stats['total']}")
        print(f"  - Successfully renamed: {stats['renamed'] + stats['dry_run_ok']}")
        print(f"  - Skipped (conflict): {stats['skipped_conflict']}")
        print(f"  - Folder not found: {stats['folder_not_found']}")
        print(f"  - Errors: {stats['errors']}")
        print("=" * 60 + "\n")
