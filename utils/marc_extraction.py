#!/usr/bin/env python3
"""
MARC 907 Field Extraction Utility.

Extracts MARC 907 field data from Alma bibliographic records and generates
TSV mapping files with MMS ID, 907$e, and cleaned 907$l values.
"""

import csv
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class ExtractionResult:
    """Result of extracting MARC 907 data from a record."""

    mms_id: str
    marc_907e: str = ""
    marc_907l: str = ""
    marc_907l_cleaned: str = ""
    status: str = "pending"  # success, no_907_field, error
    error: Optional[str] = None


class Marc907Extractor:
    """
    Extracts MARC 907 field data from Alma bibliographic records.

    Features:
    - Extracts all 907 field occurrences from each record
    - Creates multiple rows for records with multiple 907 fields
    - Removes prefix (ending with underscore) from 907$l subfield
    - Generates TSV output with results
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize extractor with configuration.

        Args:
            config: Configuration dictionary with processing settings
        """
        self.config = config

        processing = config.get("processing", {})
        self.field = processing.get("field", "907")
        self.subfield_e = processing.get("subfield_e", "e")
        self.subfield_l = processing.get("subfield_l", "l")

        prefix_config = processing.get("prefix_removal", {})
        self.prefix_removal_enabled = prefix_config.get("enabled", True)
        self.prefix_delimiter = prefix_config.get("delimiter", "_")

        output = config.get("output_settings", {})
        self.output_directory = output.get("output_directory", "./output")
        self.file_prefix = output.get("file_prefix", "marc_907_mapping")
        self.include_headers = output.get("include_headers", True)

    def extract_all_907_occurrences(
        self,
        mms_id: str,
        bibs_client: Any,
    ) -> List[Tuple[str, str]]:
        """
        Extract all 907 field occurrences from a record.

        Args:
            mms_id: MMS ID of the bibliographic record
            bibs_client: BibliographicRecords client

        Returns:
            List of tuples: [(907$e, 907$l), ...]
        """
        values_e = bibs_client.get_marc_subfield(mms_id, self.field, self.subfield_e)
        values_l = bibs_client.get_marc_subfield(mms_id, self.field, self.subfield_l)

        if not values_e and not values_l:
            return []

        # Pair up e and l values by index
        max_count = max(len(values_e), len(values_l))
        pairs = []

        for i in range(max_count):
            e_val = values_e[i] if i < len(values_e) else ""
            l_val = values_l[i] if i < len(values_l) else ""
            pairs.append((e_val, l_val))

        return pairs

    def remove_prefix_from_subfield_l(self, value: str) -> str:
        """
        Remove prefix ending with delimiter from subfield l value.

        Args:
            value: Original subfield l value

        Returns:
            Cleaned value with prefix removed

        Examples:
            "prefix_value" -> "value"
            "multi_part_value" -> "part_value" (only first prefix removed)
            "no_delimiter" -> "No underscore found: no_delimiter"
        """
        if not value:
            return ""

        if not self.prefix_removal_enabled:
            return value

        delimiter_index = value.find(self.prefix_delimiter)

        if delimiter_index == -1:
            return f"No {self.prefix_delimiter} found: {value}"

        return value[delimiter_index + 1 :]

    def process_single_record(
        self,
        mms_id: str,
        bibs_client: Any,
    ) -> List[ExtractionResult]:
        """
        Process single record and return extraction results.

        Args:
            mms_id: MMS ID of the bibliographic record
            bibs_client: BibliographicRecords client

        Returns:
            List of ExtractionResult objects (one per 907 occurrence)
        """
        try:
            pairs = self.extract_all_907_occurrences(mms_id, bibs_client)

            if not pairs:
                logger.debug(f"No 907 fields found for MMS ID: {mms_id}")
                return [
                    ExtractionResult(
                        mms_id=mms_id,
                        status="no_907_field",
                    )
                ]

            results = []
            for e_val, l_val in pairs:
                l_cleaned = self.remove_prefix_from_subfield_l(l_val)
                results.append(
                    ExtractionResult(
                        mms_id=mms_id,
                        marc_907e=e_val,
                        marc_907l=l_val,
                        marc_907l_cleaned=l_cleaned,
                        status="success",
                    )
                )

            if len(pairs) > 1:
                logger.debug(f"Found {len(pairs)} 907 fields for MMS ID: {mms_id}")

            return results

        except Exception as e:
            logger.error(f"Error processing MMS ID {mms_id}: {e}")
            return [
                ExtractionResult(
                    mms_id=mms_id,
                    status="error",
                    error=str(e),
                )
            ]

    def extract_from_mms_ids(
        self,
        mms_ids: List[str],
        bibs_client: Any,
    ) -> List[ExtractionResult]:
        """
        Extract 907 data from all provided MMS IDs.

        Args:
            mms_ids: List of MMS IDs to process
            bibs_client: BibliographicRecords client

        Returns:
            List of all ExtractionResult objects
        """
        if not mms_ids:
            logger.warning("No MMS IDs to process")
            return []

        all_results = []
        total = len(mms_ids)

        logger.info(f"Starting extraction from {total} records...")

        for idx, mms_id in enumerate(mms_ids, 1):
            results = self.process_single_record(mms_id, bibs_client)
            all_results.extend(results)

            if idx % 10 == 0 or idx == total:
                logger.info(
                    f"Processed {idx}/{total} records ({len(all_results)} total rows)"
                )

        logger.info(
            f"Extraction complete: {len(all_results)} total rows from {total} records"
        )
        return all_results

    def generate_output_filename(self, set_id: str = "") -> str:
        """
        Generate timestamped output filename.

        Args:
            set_id: Optional set ID to include in filename

        Returns:
            Full path to output file
        """
        output_dir = Path(self.output_directory)
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if set_id:
            filename = f"{self.file_prefix}_{set_id}_{timestamp}.tsv"
        else:
            filename = f"{self.file_prefix}_{timestamp}.tsv"

        return str(output_dir / filename)

    def write_tsv_output(
        self,
        results: List[ExtractionResult],
        output_file: Optional[str] = None,
    ) -> str:
        """
        Write extraction results to TSV file.

        Args:
            results: List of ExtractionResult objects
            output_file: Optional output file path

        Returns:
            Path to created TSV file
        """
        if not results:
            logger.warning("No results to write")
            return ""

        if output_file is None:
            output_file = self.generate_output_filename()

        try:
            with open(output_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter="\t")

                if self.include_headers:
                    writer.writerow(["MMS_ID", "907$e", "907$l_cleaned"])

                for result in results:
                    writer.writerow(
                        [
                            result.mms_id,
                            result.marc_907e,
                            result.marc_907l_cleaned,
                        ]
                    )

            logger.info(f"TSV file created: {output_file}")
            logger.info(f"Total rows: {len(results)}")

            return output_file

        except IOError as e:
            logger.error(f"Failed to write TSV file: {e}")
            raise

    def get_statistics(self, results: List[ExtractionResult]) -> Dict[str, int]:
        """
        Calculate statistics from extraction results.

        Args:
            results: List of ExtractionResult objects

        Returns:
            Dictionary with statistics
        """
        stats = {
            "total_rows": len(results),
            "unique_mms_ids": len(set(r.mms_id for r in results)),
            "success": sum(1 for r in results if r.status == "success"),
            "no_907_field": sum(1 for r in results if r.status == "no_907_field"),
            "errors": sum(1 for r in results if r.status == "error"),
            "no_delimiter": sum(
                1
                for r in results
                if f"No {self.prefix_delimiter} found" in r.marc_907l_cleaned
            ),
        }

        # Records with multiple 907 fields
        mms_counts = {}
        for r in results:
            mms_counts[r.mms_id] = mms_counts.get(r.mms_id, 0) + 1

        stats["multiple_907_records"] = sum(1 for c in mms_counts.values() if c > 1)

        return stats

    def display_summary(self, results: List[ExtractionResult]) -> None:
        """
        Display summary statistics.

        Args:
            results: List of ExtractionResult objects
        """
        stats = self.get_statistics(results)

        print("\n" + "=" * 60)
        print("EXTRACTION SUMMARY")
        print("=" * 60)
        print(f"Unique MMS IDs processed: {stats['unique_mms_ids']}")
        print(f"Total TSV rows created: {stats['total_rows']}")
        print(f"  - Successful extractions: {stats['success']}")
        print(f"  - Records without 907 field: {stats['no_907_field']}")
        print(f"  - Errors: {stats['errors']}")
        print(f"Records with multiple 907 fields: {stats['multiple_907_records']}")
        print(f"Values without delimiter: {stats['no_delimiter']}")
        print("=" * 60 + "\n")
