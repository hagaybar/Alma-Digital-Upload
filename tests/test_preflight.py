"""A live run must check what step 4 needs before step 1 starts writing (#21)."""

import logging
from unittest.mock import MagicMock

import pytest

from alma_digital_upload import AlmaDigitalUploader


@pytest.fixture
def aws_env(monkeypatch):
    for name, value in {
        "AWS_ACCESS_KEY": "a", "AWS_SECRET": "s",
        "ALMA_SB_BUCKET_NAME": "sb", "ALMA_PROD_BUCKET_NAME": "prod",
    }.items():
        monkeypatch.setenv(name, value)


def make_uploader(tmp_path, dry_run=False, library_code="LIB"):
    config = {
        "alma": {"environment": "SANDBOX", "set_id": "1", "library_code": library_code},
        "matching": {"files_root": str(tmp_path)},
        "options": {"dry_run": dry_run},
    }
    strategy = MagicMock()
    strategy.validate_config.return_value = []
    up = AlmaDigitalUploader(config, strategy, logging.getLogger("test"))
    up.initialize_clients = MagicMock()
    up.step1_get_set_members = MagicMock(return_value=["99"])
    up.step2_match_files = MagicMock(return_value=[])
    up.step3_create_representations = MagicMock(return_value=[])
    up.step4_upload_and_link = MagicMock(return_value=[])
    return up


@pytest.mark.parametrize("missing", ["AWS_ACCESS_KEY", "AWS_SECRET", "ALMA_SB_BUCKET_NAME"])
def test_live_run_without_aws_stops_before_any_alma_call(tmp_path, aws_env, monkeypatch, missing):
    monkeypatch.delenv(missing)
    up = make_uploader(tmp_path)
    with pytest.raises(ValueError):
        up.run("all")
    up.initialize_clients.assert_not_called()
    up.step3_create_representations.assert_not_called()


def test_live_run_without_library_code_stops_early(tmp_path, aws_env):
    up = make_uploader(tmp_path, library_code="")
    with pytest.raises(ValueError):
        up.run("all")
    up.initialize_clients.assert_not_called()


def test_live_run_with_missing_files_root_stops_early(tmp_path, aws_env):
    up = make_uploader(tmp_path / "nope")
    with pytest.raises(ValueError):
        up.run("all")
    up.initialize_clients.assert_not_called()


def test_live_run_up_to_step_2_needs_no_aws(tmp_path, aws_env, monkeypatch):
    monkeypatch.delenv("AWS_SECRET")
    up = make_uploader(tmp_path)
    up.run("2")
    up.step2_match_files.assert_called_once()


def test_complete_live_config_runs_all_steps(tmp_path, aws_env):
    up = make_uploader(tmp_path)
    up.run("all")
    up.step4_upload_and_link.assert_called_once()
