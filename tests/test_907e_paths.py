"""A 907$e value must not lead outside files_root (#23)."""

from unittest.mock import MagicMock

import pytest

from strategies import Marc907eStrategy


@pytest.fixture
def tree(tmp_path):
    root = tmp_path / "root"
    (root / "good").mkdir(parents=True)
    (root / "good" / "a.pdf").write_bytes(b"x")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "private.pdf").write_bytes(b"x")
    return root, outside


def match(root, value):
    strategy = Marc907eStrategy({"matching": {"files_root": str(root)}})
    bibs = MagicMock()
    bibs.get_marc_subfield.return_value = [value]
    return strategy._match_single_record("99", bibs)


def test_folder_inside_root_matches(tree):
    root, _ = tree
    result = match(root, "good")
    assert result.matched and result.file_count == 1


@pytest.mark.parametrize("value_for", [
    lambda root, outside: "../outside",
    lambda root, outside: "good/../../outside",
    lambda root, outside: str(outside),  # absolute path
    lambda root, outside: ".",  # files_root itself
])
def test_value_leading_outside_root_is_refused(tree, value_for):
    root, outside = tree
    result = match(root, value_for(root, outside))
    assert not result.matched
    assert result.status == "outside_files_root"
    assert result.file_paths == []


def test_link_placed_inside_root_is_followed(tree):
    # Links inside files_root are set up by whoever manages that folder (e.g. a junction
    # to another drive on masedet), not by catalogue data, so they stay allowed.
    root, outside = tree
    (root / "link").symlink_to(outside, target_is_directory=True)
    assert match(root, "link").matched
