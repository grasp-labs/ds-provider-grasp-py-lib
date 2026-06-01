"""
**File:** ``test_iam_role.py``
**Region:** ``tests/linked_service/test_iam_role``

Unit tests for GraspIAMRoleLinkedService and GraspIAMRoleLinkedServiceSettings.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import (
    AuthorizationError,
    ConnectionError,
)

from ds_provider_grasp_py_lib.enums import ResourceType
from ds_provider_grasp_py_lib.linked_service.iam_role import (
    GraspIAMRoleLinkedService,
    GraspIAMRoleLinkedServiceSettings,
)


def _make_service(
    role_arn: str = "arn:aws:iam::123456789012:role/client-role",
    region: str = "eu-north-1",
    duration_seconds: int = 3600,
) -> GraspIAMRoleLinkedService[GraspIAMRoleLinkedServiceSettings]:
    settings = GraspIAMRoleLinkedServiceSettings(
        role_arn=role_arn,
        region=region,
        duration_seconds=duration_seconds,
    )
    return GraspIAMRoleLinkedService(
        id=uuid.uuid4(),
        name="iam-role-service",
        version="1.0.0",
        settings=settings,
    )


def test_settings_defaults() -> None:
    """It exposes the expected defaults for IAM role settings."""
    settings = GraspIAMRoleLinkedServiceSettings(role_arn="arn:aws:iam::123:role/r")
    assert settings.region == "eu-north-1"
    assert settings.duration_seconds == 3600


def test_linked_service_type_property() -> None:
    """It returns the correct ResourceType for the IAM role linked service."""
    service = _make_service()
    assert service.type == ResourceType.LINKED_SERVICE_IAM_ROLE
    assert str(service.type) == "ds.linked-service.grasp-iam-role"


def test_connection_raises_when_not_connected() -> None:
    """It raises ConnectionError when connection is accessed before connect()."""
    service = _make_service()
    with pytest.raises(ConnectionError):
        _ = service.connection


@patch("ds_provider_grasp_py_lib.linked_service.iam_role.boto3")
def test_connect_assumes_role_and_builds_session(mock_boto3: MagicMock) -> None:
    """It assumes the configured role via STS and stores a boto3 session."""
    sts_client = MagicMock()
    sts_client.assume_role.return_value = {
        "Credentials": {
            "AccessKeyId": "AKIA",
            "SecretAccessKey": "SECRET",
            "SessionToken": "TOKEN",
        }
    }
    mock_boto3.client.return_value = sts_client
    mock_session = MagicMock()
    mock_boto3.Session.return_value = mock_session

    service = _make_service(role_arn="arn:aws:iam::123:role/r")
    service.connect()

    mock_boto3.client.assert_called_once_with("sts", region_name="eu-north-1")
    sts_client.assume_role.assert_called_once_with(
        RoleArn="arn:aws:iam::123:role/r",
        RoleSessionName="grasp-iam-role-session",
        DurationSeconds=3600,
    )
    mock_boto3.Session.assert_called_once_with(
        region_name="eu-north-1",
        aws_access_key_id="AKIA",
        aws_secret_access_key="SECRET",
        aws_session_token="TOKEN",
    )
    assert service.connection is mock_session


@patch("ds_provider_grasp_py_lib.linked_service.iam_role.boto3")
def test_connect_raises_authorization_error_on_client_error(mock_boto3: MagicMock) -> None:
    """It raises AuthorizationError when sts:AssumeRole fails."""
    sts_client = MagicMock()
    sts_client.assume_role.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "AssumeRole",
    )
    mock_boto3.client.return_value = sts_client
    service = _make_service()
    with pytest.raises(AuthorizationError) as exc_info:
        service.connect()
    assert "Failed to assume IAM role" in str(exc_info.value)


@patch("ds_provider_grasp_py_lib.linked_service.iam_role.boto3")
def test_test_connection_returns_success(mock_boto3: MagicMock) -> None:
    """It returns a successful tuple when connect() succeeds."""
    sts_client = MagicMock()
    sts_client.assume_role.return_value = {
        "Credentials": {
            "AccessKeyId": "AKIA",
            "SecretAccessKey": "SECRET",
            "SessionToken": "TOKEN",
        }
    }
    mock_boto3.client.return_value = sts_client
    service = _make_service()
    ok, message = service.test_connection()
    assert ok is True
    assert "successfully" in message


@patch("ds_provider_grasp_py_lib.linked_service.iam_role.boto3")
def test_test_connection_returns_failure(mock_boto3: MagicMock) -> None:
    """It returns a failure tuple when connect() raises."""
    sts_client = MagicMock()
    sts_client.assume_role.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "AssumeRole",
    )
    mock_boto3.client.return_value = sts_client
    service = _make_service()
    ok, _message = service.test_connection()
    assert ok is False


def test_close_resets_connection() -> None:
    """It clears the cached session on close()."""
    service = _make_service()
    service._connection = MagicMock()
    service.close()
    assert service._connection is None
