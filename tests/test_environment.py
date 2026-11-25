"""alma.environment must pick the matching Alma env and S3 bucket in any letter case (#12)."""

import logging
from unittest.mock import MagicMock

import pytest

from alma_digital_upload import AlmaDigitalUploader


def make_uploader(environment):
    config = {"alma": {"environment": environment}}
    return AlmaDigitalUploader(config, MagicMock(), logging.getLogger("test"))


@pytest.fixture
def aws_env(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY", "test-access")
    monkeypatch.setenv("AWS_SECRET", "test-secret")
    monkeypatch.setenv("ALMA_SB_BUCKET_NAME", "sandbox-bucket")
    monkeypatch.setenv("ALMA_PROD_BUCKET_NAME", "production-bucket")


@pytest.mark.parametrize("value", ["SANDBOX", "sandbox", "Sandbox", " sandbox "])
def test_sandbox_any_case_uses_sandbox_bucket(aws_env, value):
    uploader = make_uploader(value)
    assert uploader.environment == "SANDBOX"
    assert uploader._get_aws_credentials()[2] == "sandbox-bucket"


@pytest.mark.parametrize("value", ["PRODUCTION", "production"])
def test_production_any_case_uses_production_bucket(aws_env, value):
    uploader = make_uploader(value)
    assert uploader.environment == "PRODUCTION"
    assert uploader._get_aws_credentials()[2] == "production-bucket"


@pytest.mark.parametrize("value", ["SANDBOXX", "prod", "", None])
def test_unknown_environment_is_refused(value):
    with pytest.raises(ValueError):
        make_uploader(value)
