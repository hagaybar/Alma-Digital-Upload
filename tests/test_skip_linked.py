"""Step 4 must not re-upload or re-link a file the representation already has (#13)."""

import logging
from unittest.mock import MagicMock

from alma_digital_upload import AlmaDigitalUploader
from strategies.base import MatchResult

MMS = "990000000000000001"
REP = "120000000000000001"


def make_uploader(existing_paths=None, read_ok=True, dry_run=False):
    config = {"alma": {"environment": "SANDBOX"}, "options": {"dry_run": dry_run}}
    strategy = MagicMock()
    strategy.get_s3_path_key.return_value = MMS
    up = AlmaDigitalUploader(config, strategy, logging.getLogger("test"))
    up._get_aws_credentials = MagicMock(return_value=("a", "s", "bucket"))
    up._upload_file_to_s3 = MagicMock(
        side_effect=lambda path, key, creds: f"INST/upload/{key}/{path.split('/')[-1]}"
    )
    files = [{"path": p} for p in (existing_paths or [])]
    response = MagicMock(success=read_ok)
    response.json.return_value = {"representation_file": files, "total_record_count": len(files)}
    up.bibs = MagicMock()
    up.bibs.get_representation_files.return_value = response
    up.bibs.link_file_to_representation.return_value = MagicMock(success=True)
    return up


def match(*names):
    return MatchResult(
        mms_id=MMS,
        matched=True,
        file_paths=[f"/data/{n}" for n in names],
        metadata={"representation_id": REP},
    )


def uploaded_names(up):
    return [c.args[0].split("/")[-1] for c in up._upload_file_to_s3.call_args_list]


def test_file_already_linked_is_skipped():
    up = make_uploader(existing_paths=["INST/storage/alma/AB/CD/a.txt"])
    [result] = up.step4_upload_and_link([match("a.txt", "b.txt")])
    assert uploaded_names(up) == ["b.txt"]
    assert up.bibs.link_file_to_representation.call_count == 1
    assert result.metadata["skipped_files"] == ["a.txt"]


def test_nothing_linked_uploads_all():
    up = make_uploader(existing_paths=[])
    up.step4_upload_and_link([match("a.txt", "b.txt")])
    assert uploaded_names(up) == ["a.txt", "b.txt"]


def test_unreadable_representation_uploads_nothing():
    up = make_uploader(read_ok=False)
    [result] = up.step4_upload_and_link([match("a.txt")])
    assert up._upload_file_to_s3.call_count == 0
    assert up.bibs.link_file_to_representation.call_count == 0
    assert result.metadata["uploaded_files"] == []


def test_read_exception_uploads_nothing():
    up = make_uploader()
    up.bibs.get_representation_files.side_effect = RuntimeError("boom")
    up.step4_upload_and_link([match("a.txt")])
    assert up._upload_file_to_s3.call_count == 0


def test_dry_run_reports_skip_without_writing():
    up = make_uploader(existing_paths=["INST/storage/x/a.txt"], dry_run=True)
    [result] = up.step4_upload_and_link([match("a.txt", "b.txt")])
    assert up._upload_file_to_s3.call_count == 0
    assert result.metadata["skipped_files"] == ["a.txt"]


def test_new_representation_needs_no_files_read():
    # A representation created in this run (or a dry-run placeholder) has no files yet:
    # don't ask Alma, which can't know a placeholder id and would fail the record.
    for action, rep_id in (("dry_run", "DRY_RUN_ID"), ("created", REP)):
        up = make_uploader(dry_run=(action == "dry_run"))
        up.bibs.get_representation_files.side_effect = RuntimeError("must not be called")
        result = match("a.txt")
        result.metadata.update(representation_action=action, representation_id=rep_id)
        [out] = up.step4_upload_and_link([result])
        assert up.bibs.get_representation_files.call_count == 0
        assert out.metadata["skipped_files"] == []
        if action == "created":
            assert uploaded_names(up) == ["a.txt"]
