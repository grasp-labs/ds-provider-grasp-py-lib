"""
**File**: ``identity.py``
**Region**: ``ds_provider_grasp_py_lib/linked_service/identity``

Linked Service for Grasp Identity service.
"""

from dataclasses import dataclass, field
from typing import Generic, TypeVar

from ds_common_logger_py_lib import Logger
from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings, enums

from ..enums import ResourceType

logger = Logger.get_logger(__name__, package=True)


@dataclass(kw_only=True)
class GraspIdentityLinkedServiceSettings(HttpLinkedServiceSettings):
    """
    Settings required to connect to the Grasp Identity service.

    Attributes:
        client_id: The client ID to use when connecting to the Grasp Identity service.
        client_secret: The client secret to use when connecting to the Grasp Identity service.
        email: The email to use when connecting to the Grasp Identity service.
        password: The password to use when connecting to the Grasp Identity service.
        host: The host URL of the Grasp Identity service.
        login_endpoint: The login endpoint URL of the Grasp Identity service.
        token_endpoint: The token endpoint URL of the Grasp Identity service.
        auth_type: The authentication type to use when connecting to the Grasp Identity service.
    """

    client_id: str | None = None
    """The client ID to use when connecting to the Grasp Identity service."""

    client_secret: str | None = field(default=None, repr=False, metadata={"mask": True})
    """The client secret to use when connecting to the Grasp Identity service."""

    email: str | None = None
    """The email to use when connecting to the Grasp Identity service."""

    password: str | None = field(default=None, repr=False, metadata={"mask": True})
    """The password to use when connecting to the Grasp Identity service."""

    host: str = "https://auth.grasp-daas.com/"
    """The host URL of the Grasp Identity service."""

    login_endpoint: str = "https://auth.grasp-daas.com/rest-auth/login/"
    """The login endpoint URL of the Grasp Identity service."""

    token_endpoint: str = "https://auth.grasp-daas.com/oauth/token/"
    """The token endpoint URL of the Grasp Identity service."""

    auth_type: enums.AuthType = enums.AuthType.BEARER
    """The authentication type to use when connecting to the Grasp Identity service."""


GraspIdentityLinkedServiceSettingsType = TypeVar(
    "GraspIdentityLinkedServiceSettingsType", bound=GraspIdentityLinkedServiceSettings
)


@dataclass(kw_only=True)
class GraspIdentityLinkedService(
    HttpLinkedService[GraspIdentityLinkedServiceSettingsType], Generic[GraspIdentityLinkedServiceSettingsType]
):
    """
    Linked Service for Grasp Identity service.
    """

    settings: GraspIdentityLinkedServiceSettingsType

    @property
    def type(self) -> ResourceType:  # type: ignore[override]
        """
        The type of the linked service.

        Returns:
            ResourceType: The type of the linked service.
        """
        return ResourceType.LINKED_SERVICE_IDENTITY

    def __post_init__(self) -> None:
        """
        Post-initialization processing for the GraspIdentityLinkedService.

        This method is called automatically after the dataclass __init__ method.
        It can be used to perform any additional initialization or validation.

        Returns:
            None
        """
