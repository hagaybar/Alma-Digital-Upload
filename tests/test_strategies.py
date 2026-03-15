#!/usr/bin/env python3
"""
Tests for matching strategies.
"""

import pytest

from strategies import MatchStrategy, Marc907eStrategy, MmsIdFilenameStrategy
from strategies.base import MatchResult, UploadRecord


class TestMatchResult:
    """Tests for MatchResult dataclass."""

    def test_create_matched_result(self):
        """Test creating a matched result."""
        result = MatchResult(
            mms_id="9900001234567890",
            matched=True,
            file_paths=["/path/to/file1.pdf", "/path/to/file2.pdf"],
            match_key="test-key",
            status="matched",
        )

        assert result.mms_id == "9900001234567890"
        assert result.matched is True
        assert result.file_count == 2
        assert result.status == "matched"

    def test_create_unmatched_result(self):
        """Test creating an unmatched result."""
        result = MatchResult(
            mms_id="9900001234567890",
            matched=False,
            status="no_file",
            error="File not found",
        )

        assert result.matched is False
        assert result.file_count == 0
        assert result.error == "File not found"


class TestUploadRecord:
    """Tests for UploadRecord dataclass."""

    def test_create_upload_record(self):
        """Test creating an upload record."""
        record = UploadRecord(
            mms_id="9900001234567890",
            library_code="MAIN_LIB",
            file_path="/path/to/file.pdf",
            match_key="test-key",
        )

        assert record.mms_id == "9900001234567890"
        assert record.library_code == "MAIN_LIB"
        assert record.upload_status == "pending"


class TestMarc907eStrategy:
    """Tests for Marc907eStrategy."""

    @pytest.fixture
    def strategy(self):
        """Create a strategy instance for testing."""
        config = {
            "alma": {"library_code": "TEST"},
            "matching": {
                "files_root": "/tmp",
                "marc_field": "907",
                "marc_subfield": "e",
            },
        }
        return Marc907eStrategy(config)

    def test_strategy_name(self, strategy):
        """Test strategy name."""
        assert strategy.name == "marc-907e"

    def test_strategy_description(self, strategy):
        """Test strategy description."""
        assert "MARC 907$e" in strategy.description

    def test_get_s3_path_key(self, strategy):
        """Test S3 path key extraction."""
        result = MatchResult(
            mms_id="9900001234567890",
            matched=True,
            match_key="test-path-value",
            status="matched",
        )

        s3_key = strategy.get_s3_path_key(result)
        assert s3_key == "test-path-value"

    def test_validate_config_missing_files_root(self):
        """Test config validation with missing files_root."""
        config = {
            "alma": {"library_code": "TEST"},
            "matching": {},
        }
        strategy = Marc907eStrategy(config)
        errors = strategy.validate_config()

        assert len(errors) > 0
        assert any("files_root" in e for e in errors)


class TestMmsIdFilenameStrategy:
    """Tests for MmsIdFilenameStrategy."""

    @pytest.fixture
    def strategy(self):
        """Create a strategy instance for testing."""
        config = {
            "alma": {"library_code": "TEST"},
            "matching": {
                "files_root": "/tmp",
                "file_extension": "pdf",
            },
        }
        return MmsIdFilenameStrategy(config)

    def test_strategy_name(self, strategy):
        """Test strategy name."""
        assert strategy.name == "mms-id-filename"

    def test_strategy_description(self, strategy):
        """Test strategy description."""
        assert "MMS ID" in strategy.description

    def test_get_s3_path_key(self, strategy):
        """Test S3 path key extraction."""
        result = MatchResult(
            mms_id="9900001234567890",
            matched=True,
            match_key="9900001234567890",
            status="matched",
        )

        s3_key = strategy.get_s3_path_key(result)
        assert s3_key == "9900001234567890"


class TestStrategySelection:
    """Tests for strategy selection."""

    def test_get_marc_907e_strategy(self):
        """Test selecting Marc907e strategy."""
        from alma_digital_upload import get_strategy

        config = {
            "alma": {"library_code": "TEST"},
            "matching": {"files_root": "/tmp"},
        }

        strategy = get_strategy("marc-907e", config)
        assert isinstance(strategy, Marc907eStrategy)

    def test_get_mms_id_filename_strategy(self):
        """Test selecting MmsIdFilename strategy."""
        from alma_digital_upload import get_strategy

        config = {
            "alma": {"library_code": "TEST"},
            "matching": {"files_root": "/tmp"},
        }

        strategy = get_strategy("mms-id-filename", config)
        assert isinstance(strategy, MmsIdFilenameStrategy)

    def test_unknown_strategy_raises(self):
        """Test that unknown strategy raises error."""
        from alma_digital_upload import get_strategy

        config = {}

        with pytest.raises(ValueError, match="Unknown strategy"):
            get_strategy("unknown-strategy", config)
