#!/usr/bin/env python3
"""
Import hygiene tests for Alma Digital Upload.

Ensures no legacy imports and all modules are importable.
"""

import ast
from pathlib import Path

import pytest


def get_project_root() -> Path:
    """Get the project root directory."""
    return Path(__file__).parent.parent


def get_all_python_files() -> list[Path]:
    """Get all Python files in the project."""
    root = get_project_root()
    files = list(root.glob("**/*.py"))
    # Exclude test files themselves
    return [f for f in files if "tests/" not in str(f)]


class TestImportHygiene:
    """Tests for import hygiene."""

    def test_no_legacy_src_imports(self):
        """Ensure no 'from src.*' imports exist."""
        legacy_imports = []

        for py_file in get_all_python_files():
            try:
                with open(py_file, "r", encoding="utf-8") as f:
                    content = f.read()

                tree = ast.parse(content)

                for node in ast.walk(tree):
                    if isinstance(node, ast.ImportFrom):
                        if node.module and node.module.startswith("src."):
                            rel_path = py_file.relative_to(get_project_root())
                            legacy_imports.append(
                                f"{rel_path}: from {node.module}"
                            )

            except SyntaxError:
                pytest.fail(f"Syntax error in {py_file}")

        if legacy_imports:
            pytest.fail(
                f"Found legacy 'from src.*' imports:\n"
                + "\n".join(f"  - {imp}" for imp in legacy_imports)
            )

    def test_no_client_imports(self):
        """Ensure no direct 'from client.*' imports exist."""
        legacy_imports = []

        for py_file in get_all_python_files():
            try:
                with open(py_file, "r", encoding="utf-8") as f:
                    content = f.read()

                tree = ast.parse(content)

                for node in ast.walk(tree):
                    if isinstance(node, ast.ImportFrom):
                        if node.module and node.module.startswith("client."):
                            rel_path = py_file.relative_to(get_project_root())
                            legacy_imports.append(
                                f"{rel_path}: from {node.module}"
                            )

            except SyntaxError:
                pytest.fail(f"Syntax error in {py_file}")

        if legacy_imports:
            pytest.fail(
                f"Found legacy 'from client.*' imports:\n"
                + "\n".join(f"  - {imp}" for imp in legacy_imports)
            )


class TestModuleImports:
    """Tests for module importability."""

    def test_strategies_package_imports(self):
        """Test that strategies package imports work."""
        from strategies import MatchStrategy, Marc907eStrategy, MmsIdFilenameStrategy

        assert MatchStrategy is not None
        assert Marc907eStrategy is not None
        assert MmsIdFilenameStrategy is not None

    def test_utils_package_imports(self):
        """Test that utils package imports work."""
        from utils import (
            Marc907Extractor,
            FolderMatcher,
            FolderRenamer,
            ResumeHelper,
        )

        assert Marc907Extractor is not None
        assert FolderMatcher is not None
        assert FolderRenamer is not None
        assert ResumeHelper is not None

    def test_strategies_base_imports(self):
        """Test that base strategy classes import correctly."""
        from strategies.base import MatchResult, UploadRecord, MatchStrategy

        assert MatchResult is not None
        assert UploadRecord is not None
        assert MatchStrategy is not None

    def test_main_module_imports(self):
        """Test that main module imports work."""
        import alma_digital_upload

        assert hasattr(alma_digital_upload, "main")
        assert hasattr(alma_digital_upload, "AlmaDigitalUploader")
        assert hasattr(alma_digital_upload, "get_strategy")
