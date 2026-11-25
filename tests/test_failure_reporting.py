"""Failures must change the exit code and the log must say why (#22)."""

import logging
import sys
from unittest.mock import MagicMock

import pytest

import alma_digital_upload as adu
from alma_digital_upload import AlmaDigitalUploader, describe_error
from strategies.base import MatchResult


class FakeAlmaError(Exception):
    def __init__(self):
        super().__init__("secret request text")
        self.status_code = "400"
        self.alma_code = "702250"
        self.tracking_id = "E01-TEST"


def test_describe_error_gives_codes_not_text():
    text = describe_error(FakeAlmaError())
    assert "702250" in text and "E01-TEST" in text and "HTTP 400" in text
    assert "secret request text" not in text


def test_describe_error_plain_exception_is_type_only():
    assert describe_error(ValueError("x")) == "ValueError"


def make_uploader():
    config = {"alma": {"environment": "SANDBOX"}, "options": {"dry_run": False}}
    strategy = MagicMock()
    strategy.get_s3_path_key.return_value = "99"
    up = AlmaDigitalUploader(config, strategy, logging.getLogger("test"))
    up._get_aws_credentials = MagicMock(return_value=("a", "s", "b"))
    up._upload_file_to_s3 = MagicMock(return_value="INST/upload/99/a.txt")
    files = MagicMock(success=True)
    files.json.return_value = {"representation_file": [], "total_record_count": 0}
    up.bibs = MagicMock()
    up.bibs.get_representation_files.return_value = files
    return up


def match():
    return MatchResult(mms_id="99", matched=True, file_paths=["/d/a.txt"],
                       metadata={"representation_id": "12", "representation_action": "exists"})


def test_link_exception_counts_as_link_failure_and_logs_code(caplog):
    up = make_uploader()
    up.bibs.link_file_to_representation.side_effect = FakeAlmaError()
    with caplog.at_level(logging.ERROR):
        [r] = up.step4_upload_and_link([match()])
    assert up.failures == 1
    assert r.metadata["uploaded_files"] == ["INST/upload/99/a.txt"]
    assert "link failed" in caplog.text and "702250" in caplog.text


def test_clean_step4_counts_no_failures():
    up = make_uploader()
    up.bibs.link_file_to_representation.return_value = MagicMock(success=True)
    up.step4_upload_and_link([match()])
    assert up.failures == 0


@pytest.mark.parametrize("failures,code", [(0, None), (3, 2)])
def test_cli_exit_code(monkeypatch, tmp_path, failures, code):
    cfg = tmp_path / "c.json"
    cfg.write_text('{"alma": {"environment": "SANDBOX"}, "matching": {"strategy": "mms-id-filename"},'
                   ' "output_settings": {"output_directory": "%s"}}' % tmp_path.as_posix())
    monkeypatch.setattr(adu.AlmaDigitalUploader, "run", lambda self, step: failures)
    monkeypatch.setattr(sys, "argv", ["x", "-c", str(cfg)])
    if code is None:
        adu.main()
    else:
        with pytest.raises(SystemExit) as exc:
            adu.main()
        assert exc.value.code == code
