"""
**File:** ``iam_role.py``
**Region:** ``ds_provider_grasp_py_lib/linked_service/iam_role``

Grasp IAM Role Linked Service

This linked service is intended for internal use. When a client is
provisioned, a dedicated IAM role is created granting access to read and
write the client's S3 bucket. This linked service assumes that role via
STS and exposes a ``boto3.Session`` scoped to those temporary credentials.

The ambient credentials used to call ``sts:AssumeRole`` come from the
host's default credential chain (instance/task role, ``AWS_*`` env vars,
or shared profile). No static access keys are accepted in settings.
"""

from dataclasses import dataclass, field
from typing import Generic, TypeVar

import boto3
from botocore.exceptions import ClientError
from ds_common_logger_py_lib import Logger
from ds_resource_plugin_py_lib.common.resource.linked_service import (
    LinkedService,
    LinkedServiceSettings,
)
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import (
    AuthorizationError,
    ConnectionError,
)

from ..enums import ResourceType

logger = Logger.get_logger(__name__, package=True)

_ROLE_SESSION_NAME = "grasp-iam-role-session"


@dataclass(kw_only=True)
class GraspIAMRoleLinkedServiceSettings(LinkedServiceSettings):
    """
    Settings for the Grasp IAM Role linked service.

    Attributes:
        role_arn: ARN of the client-dedicated IAM role to assume.
        region: AWS region for the resulting boto3 session.
        duration_seconds: Lifetime of the assumed credentials, in seconds.
    """

    role_arn: str
    region: str = "eu-north-1"
    duration_seconds: int = 3600


GraspIAMRoleLinkedServiceSettingsType = TypeVar(
    "GraspIAMRoleLinkedServiceSettingsType",
    bound=GraspIAMRoleLinkedServiceSettings,
)


@dataclass(kw_only=True)
class GraspIAMRoleLinkedService(
    LinkedService[GraspIAMRoleLinkedServiceSettingsType],
    Generic[GraspIAMRoleLinkedServiceSettingsType],
):
    """
    Linked service that assumes a client-dedicated IAM role for S3 access.
    """

    settings: GraspIAMRoleLinkedServiceSettingsType
    _connection: boto3.Session | None = field(
        default=None, init=False, repr=False, metadata={"serialize": False}
    )

    @property
    def type(self) -> ResourceType:  # type: ignore[override]
        return ResourceType.LINKED_SERVICE_IAM_ROLE

    @property
    def connection(self) -> boto3.Session:
        if self._connection is None:
            raise ConnectionError("No AWS session available. Call connect() first.")
        return self._connection

    def _assume_role(self) -> dict[str, str]:
        sts_client = boto3.client("sts", region_name=self.settings.region)
        logger.debug("Assuming role %s as %s", self.settings.role_arn, _ROLE_SESSION_NAME)
        response = sts_client.assume_role(
            RoleArn=self.settings.role_arn,
            RoleSessionName=_ROLE_SESSION_NAME,
            DurationSeconds=self.settings.duration_seconds,
        )
        credentials = response["Credentials"]
        return {
            "aws_access_key_id": credentials["AccessKeyId"],
            "aws_secret_access_key": credentials["SecretAccessKey"],
            "aws_session_token": credentials["SessionToken"],
        }

    def connect(self) -> None:
        """
        Assume the configured IAM role and store the resulting boto3
        session in ``self._connection``.

        Raises:
            AuthorizationError: If ``sts:AssumeRole`` fails.
        """
        try:
            credentials = self._assume_role()
        except ClientError as exc:
            logger.error("Failed to assume IAM role %s: %s", self.settings.role_arn, exc)
            raise AuthorizationError(
                message=f"Failed to assume IAM role {self.settings.role_arn}.",
                details={
                    "type": self.type.value,
                    "role_arn": self.settings.role_arn,
                },
            ) from exc

        self._connection = boto3.Session(
            region_name=self.settings.region,
            **credentials,
        )

    def test_connection(self) -> tuple[bool, str]:
        """
        Test the connection by attempting to assume the configured role.
        """
        try:
            self.connect()
            return True, "Connection successfully tested"
        except (ClientError, AuthorizationError) as exc:
            return False, str(exc)

    def close(self) -> None:
        """
        boto3 sessions do not require explicit closing.
        """
        self._connection = None
