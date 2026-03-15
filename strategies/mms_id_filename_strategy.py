#!/usr/bin/env python3
"""
MMS ID Filename Matching Strategy.

This strategy matches files by looking for files named with the MMS ID
(e.g., "990012345678904146.pdf") in a specified input folder.

Workflow:
1. Scan input folder for files matching pattern: {mms_id}.{extension}
2. For each MMS ID, check if matching file exists
3. Return match results with file paths

Use case: Simple PDF collections where files are named with their MMS ID.

Configuration:
    file_extensions: Optional list of extensions to match.
        - List: ["pdf", "tif", "jpg"] - matches only these extensions
        - String: "pdf" - matches single extension (backwards compatible)
        - Empty/null/omitted: matches ALL file extensions
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union

from strategies.base import MatchResult, MatchStrategy

logger = logging.getLogger(__name__)


class MmsIdFilenameStrategy(MatchStrategy):
    """
    Strategy that matches files by MMS ID in the filename.

    Files must be named exactly: {mms_id}.{extension}
    For example: 990012345678904146.pdf

    Supports multiple extensions or all extensions when none specified.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the MMS ID filename strategy.

        Args:
            config: Configuration with matching settings.
                file_extensions: List of extensions, single extension string,
                    or empty/null for all extensions.
        """
        super().__init__(config)

        matching_config = config.get("matching", {})
        self.file_extensions = self._parse_extensions(matching_config)
        # Maps MMS ID to file path (str) or list of paths (List[str]) for multiple files
        self._file_map: Dict[str, Union[str, List[str]]] = {}

    def _parse_extensions(
        self, matching_config: Dict[str, Any]
    ) -> Optional[List[str]]:
        """
        Parse file extensions from config.

        Supports:
            - List: ["pdf", "tif"] -> ["pdf", "tif"]
            - String: "pdf" -> ["pdf"]
            - Empty list/string/null: None (accept all)

        Args:
            matching_config: The matching section of config

        Returns:
            List of lowercase extensions without dots, or None for all extensions
        """
        # Check for new plural key first, fall back to singular for backwards compat
        raw_value = matching_config.get(
            "file_extensions", matching_config.get("file_extension")
        )

        # Empty, null, or missing -> accept all
        if raw_value is None or raw_value == "" or raw_value == []:
            return None

        # String -> single-item list
        if isinstance(raw_value, str):
            ext = raw_value.strip().lower().lstrip(".")
            return [ext] if ext else None

        # List -> normalize each extension
        if isinstance(raw_value, list):
            extensions = []
            for ext in raw_value:
                if isinstance(ext, str):
                    normalized = ext.strip().lower().lstrip(".")
                    if normalized:
                        extensions.append(normalized)
            return extensions if extensions else None

        # Unknown type -> accept all
        logger.warning(f"Unknown file_extensions type: {type(raw_value)}, accepting all")
        return None

    @property
    def name(self) -> str:
        """Return strategy name."""
        return "mms-id-filename"

    @property
    def description(self) -> str:
        """Return strategy description."""
        if self.file_extensions is None:
            ext_desc = "any extension"
        elif len(self.file_extensions) == 1:
            ext_desc = f".{self.file_extensions[0]}"
        else:
            ext_desc = f".{{{', '.join(self.file_extensions)}}}"

        return (
            f"Matches files by MMS ID in filename. "
            f"Looks for files named {{mms_id}}.ext ({ext_desc}) in the input folder."
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

        # Log what extensions we're looking for
        if self.file_extensions is None:
            logger.info("Scanning for files with any extension")
        else:
            logger.info(f"Scanning for files with extensions: {self.file_extensions}")

        for item in os.listdir(self.files_root):
            item_path = os.path.join(self.files_root, item)

            if not os.path.isfile(item_path):
                continue

            # Check if file has an extension
            if "." not in item:
                continue

            # Get extension (lowercase, without dot)
            file_ext = item.rsplit(".", 1)[-1].lower()

            # Check if extension matches (if filtering)
            if self.file_extensions is not None:
                if file_ext not in self.file_extensions:
                    continue

            # Extract MMS ID from filename (everything before the last dot)
            filename_without_ext = item.rsplit(".", 1)[0]

            # Validate MMS ID format (should be numeric)
            if filename_without_ext.isdigit():
                # Handle multiple files for same MMS ID (different extensions)
                if filename_without_ext in file_map:
                    # Already have a file for this MMS ID
                    existing = file_map[filename_without_ext]
                    if isinstance(existing, str):
                        # Convert to list
                        file_map[filename_without_ext] = [existing, item_path]
                    else:
                        # Append to existing list
                        file_map[filename_without_ext].append(item_path)
                    logger.debug(f"Additional file for {filename_without_ext}: {item}")
                else:
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
            file_entry = self._file_map[mms_id]

            # Handle both single file (string) and multiple files (list)
            if isinstance(file_entry, str):
                file_paths = [file_entry]
            else:
                file_paths = file_entry

            filenames = [os.path.basename(fp) for fp in file_paths]

            if len(file_paths) == 1:
                logger.debug(f"Match found: {mms_id} -> {filenames[0]}")
            else:
                logger.debug(f"Match found: {mms_id} -> {len(file_paths)} files: {filenames}")

            return MatchResult(
                mms_id=mms_id,
                matched=True,
                file_paths=file_paths,
                match_key=mms_id,
                status="matched",
                metadata={"filenames": filenames},
            )
        else:
            logger.debug(f"No file found for: {mms_id}")

            # Build descriptive error message
            if self.file_extensions is None:
                ext_desc = "any extension"
            elif len(self.file_extensions) == 1:
                ext_desc = f".{self.file_extensions[0]}"
            else:
                ext_desc = f".{{{', '.join(self.file_extensions)}}}"

            return MatchResult(
                mms_id=mms_id,
                matched=False,
                match_key=mms_id,
                status="no_file",
                error=f"No file found matching {mms_id} with {ext_desc}",
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
