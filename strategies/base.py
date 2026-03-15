#!/usr/bin/env python3
"""
Abstract base class for file matching strategies.

This module defines the interface that all matching strategies must implement.
Each strategy provides a different way to match local files with Alma
bibliographic records.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class MatchResult:
    """Result of matching a bibliographic record to local files."""

    mms_id: str
    matched: bool
    file_paths: List[str] = field(default_factory=list)
    match_key: str = ""  # The value used for matching (e.g., 907$e or filename)
    status: str = "pending"  # pending, matched, not_found, error
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def file_count(self) -> int:
        """Return number of matched files."""
        return len(self.file_paths)


@dataclass
class UploadRecord:
    """Record representing a file to be uploaded and linked."""

    mms_id: str
    library_code: str
    file_path: str
    match_key: str  # The value used for S3 path (e.g., 907$e value or mms_id)
    access_rights_code: str = ""
    access_rights_desc: str = ""
    representation_id: Optional[str] = None
    s3_key: Optional[str] = None
    upload_status: str = "pending"  # pending, uploaded, linked, error
    error: Optional[str] = None


class MatchStrategy(ABC):
    """
    Abstract base class for file matching strategies.

    Each strategy implements a different way to match local files
    with Alma bibliographic records.

    Subclasses must implement:
    - match(): Match files for a list of MMS IDs
    - get_s3_path_key(): Return the key to use in S3 path structure
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the strategy with configuration.

        Args:
            config: Configuration dictionary with strategy-specific settings
        """
        self.config = config
        self.files_root = config.get("matching", {}).get("files_root", "")
        self.library_code = config.get("alma", {}).get("library_code", "")
        self.access_rights_code = config.get("alma", {}).get("access_rights_code", "")
        self.access_rights_desc = config.get("alma", {}).get("access_rights_desc", "")

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name of this strategy."""
        pass

    @property
    @abstractmethod
    def description(self) -> str:
        """Return a description of what this strategy does."""
        pass

    @abstractmethod
    def match(
        self,
        mms_ids: List[str],
        bibs_client: Any,
    ) -> List[MatchResult]:
        """
        Match local files to the given MMS IDs.

        Args:
            mms_ids: List of MMS IDs to match
            bibs_client: BibliographicRecords client for MARC extraction

        Returns:
            List of MatchResult objects, one per MMS ID
        """
        pass

    @abstractmethod
    def get_s3_path_key(self, match_result: MatchResult) -> str:
        """
        Return the key to use in S3 path structure.

        For Marc907eStrategy: returns the 907$e value
        For MmsIdFilenameStrategy: returns the MMS ID

        Args:
            match_result: The match result containing match information

        Returns:
            String to use as the path key in S3
        """
        pass

    def prepare_upload_records(
        self,
        match_results: List[MatchResult],
    ) -> List[UploadRecord]:
        """
        Convert match results to upload records.

        Args:
            match_results: List of successful match results

        Returns:
            List of UploadRecord objects ready for upload processing
        """
        upload_records = []

        for result in match_results:
            if not result.matched:
                continue

            for file_path in result.file_paths:
                record = UploadRecord(
                    mms_id=result.mms_id,
                    library_code=self.library_code,
                    file_path=file_path,
                    match_key=self.get_s3_path_key(result),
                    access_rights_code=self.access_rights_code,
                    access_rights_desc=self.access_rights_desc,
                )
                upload_records.append(record)

        return upload_records

    def validate_config(self) -> List[str]:
        """
        Validate the configuration for this strategy.

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []

        if not self.files_root:
            errors.append("Missing 'matching.files_root' in configuration")
        if not self.library_code:
            errors.append("Missing 'alma.library_code' in configuration")

        return errors
