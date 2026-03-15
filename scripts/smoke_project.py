#!/usr/bin/env python3
"""
Smoke Test for Alma Digital Upload Project.

Verifies that:
1. All modules can be imported
2. No legacy 'from src.*' imports are present
3. All strategy classes are properly defined
4. Configuration validation works
"""

import ast
import sys
from pathlib import Path


def check_imports() -> tuple[bool, list[str]]:
    """
    Check that all modules can be imported.

    Returns:
        Tuple of (success, list of error messages)
    """
    errors = []

    modules_to_import = [
        "strategies",
        "strategies.base",
        "strategies.marc_907e_strategy",
        "strategies.mms_id_filename_strategy",
        "utils",
        "utils.marc_extraction",
        "utils.folder_matching",
        "utils.folder_renaming",
        "utils.resume_helper",
    ]

    for module in modules_to_import:
        try:
            __import__(module)
            print(f"  ✓ {module}")
        except ImportError as e:
            errors.append(f"Failed to import {module}: {e}")
            print(f"  ✗ {module}: {e}")

    return len(errors) == 0, errors


def check_no_legacy_imports() -> tuple[bool, list[str]]:
    """
    Check that no legacy 'from src.*' imports exist.

    Returns:
        Tuple of (success, list of files with legacy imports)
    """
    errors = []

    project_root = Path(__file__).parent.parent
    python_files = list(project_root.glob("**/*.py"))

    for py_file in python_files:
        try:
            with open(py_file, "r", encoding="utf-8") as f:
                content = f.read()

            tree = ast.parse(content)

            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    if node.module and node.module.startswith("src."):
                        rel_path = py_file.relative_to(project_root)
                        errors.append(
                            f"{rel_path}: 'from {node.module} import ...'"
                        )
                        print(f"  ✗ {rel_path}: legacy import from {node.module}")

        except SyntaxError as e:
            rel_path = py_file.relative_to(project_root)
            errors.append(f"{rel_path}: Syntax error - {e}")
            print(f"  ✗ {rel_path}: Syntax error")

    if not errors:
        print("  ✓ No legacy 'from src.*' imports found")

    return len(errors) == 0, errors


def check_strategy_classes() -> tuple[bool, list[str]]:
    """
    Check that strategy classes are properly defined.

    Returns:
        Tuple of (success, list of error messages)
    """
    errors = []

    try:
        from strategies import MatchStrategy, Marc907eStrategy, MmsIdFilenameStrategy

        # Check base class has required abstract methods
        required_methods = ["match", "get_s3_path_key", "name", "description"]

        for method in required_methods:
            if not hasattr(MatchStrategy, method):
                errors.append(f"MatchStrategy missing method: {method}")
                print(f"  ✗ MatchStrategy missing: {method}")

        # Check concrete classes implement interface
        test_config = {
            "alma": {"library_code": "TEST"},
            "matching": {"files_root": "/tmp"},
        }

        for cls in [Marc907eStrategy, MmsIdFilenameStrategy]:
            try:
                instance = cls(test_config)
                print(f"  ✓ {cls.__name__} instantiated")

                # Check properties
                _ = instance.name
                _ = instance.description
                print(f"    ✓ {cls.__name__} properties accessible")

            except Exception as e:
                errors.append(f"{cls.__name__} instantiation failed: {e}")
                print(f"  ✗ {cls.__name__}: {e}")

    except ImportError as e:
        errors.append(f"Failed to import strategies: {e}")
        print(f"  ✗ Import error: {e}")

    return len(errors) == 0, errors


def check_utility_classes() -> tuple[bool, list[str]]:
    """
    Check that utility classes are properly defined.

    Returns:
        Tuple of (success, list of error messages)
    """
    errors = []

    try:
        from utils import (
            Marc907Extractor,
            FolderMatcher,
            FolderRenamer,
            ResumeHelper,
        )

        test_config = {
            "processing": {},
            "output_settings": {},
            "input": {"tsv_file": "", "folder_path": ""},
        }

        for cls in [Marc907Extractor, FolderMatcher]:
            try:
                instance = cls(test_config)
                print(f"  ✓ {cls.__name__} instantiated")
            except Exception as e:
                errors.append(f"{cls.__name__} failed: {e}")
                print(f"  ✗ {cls.__name__}: {e}")

        # FolderRenamer and ResumeHelper need different configs
        try:
            renamer = FolderRenamer(test_config)
            print(f"  ✓ FolderRenamer instantiated")
        except Exception as e:
            errors.append(f"FolderRenamer failed: {e}")
            print(f"  ✗ FolderRenamer: {e}")

        try:
            helper = ResumeHelper()
            print(f"  ✓ ResumeHelper instantiated")
        except Exception as e:
            errors.append(f"ResumeHelper failed: {e}")
            print(f"  ✗ ResumeHelper: {e}")

    except ImportError as e:
        errors.append(f"Failed to import utils: {e}")
        print(f"  ✗ Import error: {e}")

    return len(errors) == 0, errors


def main():
    """Run all smoke tests."""
    print("=" * 60)
    print("ALMA DIGITAL UPLOAD - SMOKE TEST")
    print("=" * 60)

    all_passed = True
    all_errors = []

    # Test 1: Module imports
    print("\n1. Checking module imports...")
    passed, errors = check_imports()
    all_passed = all_passed and passed
    all_errors.extend(errors)

    # Test 2: No legacy imports
    print("\n2. Checking for legacy imports...")
    passed, errors = check_no_legacy_imports()
    all_passed = all_passed and passed
    all_errors.extend(errors)

    # Test 3: Strategy classes
    print("\n3. Checking strategy classes...")
    passed, errors = check_strategy_classes()
    all_passed = all_passed and passed
    all_errors.extend(errors)

    # Test 4: Utility classes
    print("\n4. Checking utility classes...")
    passed, errors = check_utility_classes()
    all_passed = all_passed and passed
    all_errors.extend(errors)

    # Summary
    print("\n" + "=" * 60)
    if all_passed:
        print("SMOKE TEST PASSED")
        print("=" * 60)
        sys.exit(0)
    else:
        print("SMOKE TEST FAILED")
        print("=" * 60)
        print("\nErrors:")
        for error in all_errors:
            print(f"  - {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
