#!/usr/bin/env python3
"""
MMS ID Filename Matching Strategy.

This strategy matches files by looking for files named with the MMS ID
(e.g., "990012345678904146.pdf") in a specified input folder.

Workflow:
1. Scan input folder for files matching pattern: {mms_id}.{extension}
2. For each MMS ID, check if matching file exists
3. Return match results with file paths

Used by: Rare Books Upload workflow
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Set

from strategies.base import MatchResult, MatchStrategy

logger = logging.getLogger(__name__)


class MmsIdFilenameStrategy(MatchStrategy):
    """
    Strategy that matches files by MMS ID in the filename.

    Files must be named exactly: {mms_id}.{extension}
    For example: 990012345678904146.pdf
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the MMS ID filename strategy.

        Args:
            config: Configuration with matching settings
        """
        super().__init__(config)

        matching_config = config.get("matching", {})
        self.file_extension = matching_config.get("file_extension", "pdf")
        self._file_map: Dict[str, str] = {}

    @property
    def name(self) -> str:
        """Return strategy name."""
        return "mms-id-filename"

    @property
    def description(self) -> str:
        """Return strategy description."""
        return (
            f"Matches files by MMS ID in filename. "
            f"Looks for files named {{mms_id}}.{self.file_extension} in the input folder."
        )

    def match(
        self,
        mms_ids: List[str],
        bibs_client: Any,
    ) -> List[MatchResult]:
        """
        Match files for each MMS ID by looking for matching filenames.

        Note: bibs_client is not used by this strategy but is required
        by the interface for consistency with other strategies.

        Args:
            mms_ids: List of MMS IDs to process
            bibs_client: BibliographicRecords client (not used)

        Returns:
            List of MatchResult objects
        """
        results = []
        total = len(mms_ids)

        logger.info(f"Starting MMS ID filename matching for {total} records")
        logger.info(f"Scanning folder: {self.files_root}")

        # First, scan the folder and build a mapping
        self._file_map = self._scan_folder()

        logger.info(f"Found {len(self._file_map)} valid files in folder")

        # Match each MMS ID
        for idx, mms_id in enumerate(mms_ids, 1):
            if idx % 10 == 0 or idx == total:
                logger.info(f"Processing {idx}/{total}: {mms_id}")
            else:
                logger.debug(f"Processing {idx}/{total}: {mms_id}")

            result = self._match_single_record(mms_id)
            results.append(result)

        # Log summary
        matched = sum(1 for r in results if r.matched)
        logger.info(f"Matching complete: {matched}/{total} records matched")

        return results

    def _scan_folder(self) -> Dict[str, str]:
        """
        Scan the input folder and build MMS ID to file path mapping.

        Returns:
            Dictionary mapping MMS ID to file path
        """
        file_map = {}
        invalid_files = []

        if not os.path.exists(self.files_root):
            logger.error(f"Input folder does not exist: {self.files_root}")
            return file_map

        if not os.path.isdir(self.files_root):
            logger.error(f"Input path is not a directory: {self.files_root}")
            return file_map

        # Scan for files with the expected extension
        pattern = f"*.{self.file_extension}"

        for item in os.listdir(self.files_root):
            item_path = os.path.join(self.files_root, item)

            if not os.path.isfile(item_path):
                continue

            # Check if file has the expected extension
            if not item.lower().endswith(f".{self.file_extension}"):
                continue

            # Extract MMS ID from filename
            filename_without_ext = item[: -(len(self.file_extension) + 1)]

            # Validate MMS ID format (should be numeric)
            if filename_without_ext.isdigit():
                file_map[filename_without_ext] = item_path
                logger.debug(f"Mapped: {filename_without_ext} -> {item}")
            else:
                invalid_files.append(item)
                logger.debug(f"Invalid MMS ID format: {item}")

        if invalid_files:
            logger.warning(
                f"Found {len(invalid_files)} files with invalid MMS ID format"
            )
            if len(invalid_files) <= 5:
                logger.warning(f"Invalid files: {invalid_files}")
            else:
                logger.warning(f"First 5 invalid files: {invalid_files[:5]}")

        return file_map

    def _match_single_record(self, mms_id: str) -> MatchResult:
        """
        Match a single MMS ID to a file.

        Args:
            mms_id: The MMS ID to match

        Returns:
            MatchResult for this record
        """
        if mms_id in self._file_map:
            file_path = self._file_map[mms_id]
            filename = os.path.basename(file_path)

            logger.debug(f"Match found: {mms_id} -> {filename}")

            return MatchResult(
                mms_id=mms_id,
                matched=True,
                file_paths=[file_path],
                match_key=mms_id,
                status="matched",
                metadata={"filename": filename},
            )
        else:
            logger.debug(f"No file found for: {mms_id}")
            return MatchResult(
                mms_id=mms_id,
                matched=False,
                match_key=mms_id,
                status="no_file",
                error=f"No file found matching {mms_id}.{self.file_extension}",
            )

    def get_s3_path_key(self, match_result: MatchResult) -> str:
        """
        Return the MMS ID for S3 path structure.

        S3 path pattern: {inst_code}/upload/{mms_id}/{filename}

        Args:
            match_result: Match result with MMS ID

        Returns:
            The MMS ID to use in S3 path
        """
        return match_result.mms_id

    def validate_config(self) -> List[str]:
        """
        Validate configuration for this strategy.

        Returns:
            List of validation errors
        """
        errors = super().validate_config()

        if not os.path.exists(self.files_root):
            errors.append(f"Input folder does not exist: {self.files_root}")

        return errors

    def get_unmatched_files(self, matched_mms_ids: Set[str]) -> List[str]:
        """
        Get list of files in folder that weren't matched to any MMS ID.

        Useful for reporting files that exist but weren't in the set.

        Args:
            matched_mms_ids: Set of MMS IDs that were matched

        Returns:
            List of unmatched file paths
        """
        unmatched = []
        for mms_id, file_path in self._file_map.items():
            if mms_id not in matched_mms_ids:
                unmatched.append(file_path)

        return unmatched
