#!/usr/bin/env python3
"""
MARC 907$e Matching Strategy.

This strategy matches files by looking up MARC 907$e values from bibliographic
records and using them to locate corresponding folders on the filesystem.

Workflow:
1. For each MMS ID, extract MARC 907$e value(s)
2. Construct file path: files_root + 907$e_value
3. Discover files in the matched folder
4. Return match results with file paths

Used by: LGBTQ Upload, NIMTZOV Upload workflows
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, List

from strategies.base import MatchResult, MatchStrategy

logger = logging.getLogger(__name__)


class Marc907eStrategy(MatchStrategy):
    """
    Strategy that matches files using MARC 907$e field values.

    The 907$e subfield typically contains a path identifier that maps
    to a folder structure on the local filesystem.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the MARC 907$e strategy.

        Args:
            config: Configuration with matching settings
        """
        super().__init__(config)

        matching_config = config.get("matching", {})
        self.marc_field = matching_config.get("marc_field", "907")
        self.marc_subfield = matching_config.get("marc_subfield", "e")

    @property
    def name(self) -> str:
        """Return strategy name."""
        return "marc-907e"

    @property
    def description(self) -> str:
        """Return strategy description."""
        return (
            f"Matches files using MARC {self.marc_field}${self.marc_subfield} values. "
            "Extracts path identifiers from bibliographic records and locates "
            "corresponding folders on the filesystem."
        )

    def match(
        self,
        mms_ids: List[str],
        bibs_client: Any,
    ) -> List[MatchResult]:
        """
        Match files for each MMS ID using MARC 907$e extraction.

        Args:
            mms_ids: List of MMS IDs to process
            bibs_client: BibliographicRecords client for MARC extraction

        Returns:
            List of MatchResult objects
        """
        results = []
        total = len(mms_ids)

        logger.info(f"Starting MARC {self.marc_field}${self.marc_subfield} matching for {total} records")

        for idx, mms_id in enumerate(mms_ids, 1):
            if idx % 10 == 0 or idx == total:
                logger.info(f"Processing {idx}/{total}: {mms_id}")
            else:
                logger.debug(f"Processing {idx}/{total}: {mms_id}")

            result = self._match_single_record(mms_id, bibs_client)
            results.append(result)

        # Log summary
        matched = sum(1 for r in results if r.matched)
        logger.info(f"Matching complete: {matched}/{total} records matched")

        return results

    def _match_single_record(
        self,
        mms_id: str,
        bibs_client: Any,
    ) -> MatchResult:
        """
        Match files for a single MMS ID.

        Args:
            mms_id: The MMS ID to process
            bibs_client: BibliographicRecords client

        Returns:
            MatchResult for this record
        """
        try:
            # Extract MARC 907$e values
            marc_values = bibs_client.get_marc_subfield(
                mms_id, self.marc_field, self.marc_subfield
            )

            if not marc_values:
                logger.debug(f"No MARC {self.marc_field}${self.marc_subfield} found for {mms_id}")
                return MatchResult(
                    mms_id=mms_id,
                    matched=False,
                    status="no_marc",
                    error=f"No MARC {self.marc_field}${self.marc_subfield} found",
                )

            # Use primary (first) value for matching
            primary_value = marc_values[0].strip()

            if not primary_value:
                return MatchResult(
                    mms_id=mms_id,
                    matched=False,
                    status="empty_marc",
                    error=f"Empty MARC {self.marc_field}${self.marc_subfield} value",
                )

            # Construct file path
            folder_path = os.path.join(self.files_root, primary_value)

            # Check if path exists
            if not os.path.exists(folder_path):
                logger.debug(f"Path does not exist: {folder_path}")
                return MatchResult(
                    mms_id=mms_id,
                    matched=False,
                    match_key=primary_value,
                    status="path_not_found",
                    error=f"Path does not exist: {folder_path}",
                    metadata={"marc_values": marc_values},
                )

            if not os.path.isdir(folder_path):
                logger.debug(f"Path is not a directory: {folder_path}")
                return MatchResult(
                    mms_id=mms_id,
                    matched=False,
                    match_key=primary_value,
                    status="not_directory",
                    error=f"Path is not a directory: {folder_path}",
                    metadata={"marc_values": marc_values},
                )

            # Discover files in the folder
            file_paths = self._discover_files(folder_path)

            if not file_paths:
                logger.debug(f"No files found in: {folder_path}")
                return MatchResult(
                    mms_id=mms_id,
                    matched=False,
                    match_key=primary_value,
                    status="no_files",
                    error=f"No files found in: {folder_path}",
                    metadata={"marc_values": marc_values},
                )

            logger.debug(f"Found {len(file_paths)} files for {mms_id}")
            return MatchResult(
                mms_id=mms_id,
                matched=True,
                file_paths=file_paths,
                match_key=primary_value,
                status="matched",
                metadata={
                    "marc_values": marc_values,
                    "folder_path": folder_path,
                },
            )

        except Exception as e:
            logger.error(f"Error matching {mms_id}: {e}")
            return MatchResult(
                mms_id=mms_id,
                matched=False,
                status="error",
                error=str(e),
            )

    def _discover_files(self, folder_path: str) -> List[str]:
        """
        Discover all files in a folder (non-recursive).

        Args:
            folder_path: Path to the folder

        Returns:
            List of file paths
        """
        files = []
        try:
            for item in os.listdir(folder_path):
                item_path = os.path.join(folder_path, item)
                if os.path.isfile(item_path):
                    files.append(item_path)
        except Exception as e:
            logger.error(f"Error discovering files in {folder_path}: {e}")

        return sorted(files)

    def get_s3_path_key(self, match_result: MatchResult) -> str:
        """
        Return the MARC 907$e value for S3 path structure.

        S3 path pattern: {inst_code}/upload/{marc_907e_value}/{filename}

        Args:
            match_result: Match result with MARC value

        Returns:
            The MARC 907$e value to use in S3 path
        """
        return match_result.match_key

    def validate_config(self) -> List[str]:
        """
        Validate configuration for this strategy.

        Returns:
            List of validation errors
        """
        errors = super().validate_config()

        if not os.path.exists(self.files_root):
            errors.append(f"Files root directory does not exist: {self.files_root}")

        return errors
