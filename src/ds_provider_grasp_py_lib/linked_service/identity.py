"""
**File**: ``identity.py``
**Region**: ``ds_provider_grasp_py_lib/linked_service/identity``

Linked Service for Grasp Identity service.
"""

from dataclasses import dataclass, field
from typing import Generic, TypeVar

from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings, enums
from ds_protocol_http_py_lib.linked_service.http import BearerAuthSettings, OAuth2AuthSettings

from ..enums import ResourceType


@dataclass(kw_only=True)
class IDPOAuth2AuthSettings(OAuth2AuthSettings):
    """
    OAuth2 authentication settings for Identity Provider (IdP).

    Attributes:
        token_url: The URL to obtain the OAuth2 token.
        client_id: The client ID for OAuth2 authentication.
        client_secret: The client secret for OAuth2 authentication.
        scope: The scope of the OAuth2 token.
    """

    token_endpoint: str = field(default="https://auth.grasp-daas.com/oauth/token/")
    """The URL to obtain the OAuth2 token. Default is 'https://auth.grasp-daas.com/oauth/token/'"""


@dataclass(kw_only=True)
class IDPBearerAuthSettings(BearerAuthSettings):
    """
    Bearer authentication settings for Identity Provider (IdP).

    Attributes:
        token_endpoint: The URL to obtain the bearer token.
    """

    token_endpoint: str = field(default="https://auth.grasp-daas.com/rest-auth/login/")
    """The URL to obtain the bearer token. Default is 'https://auth.grasp-daas.com/rest-auth/login/'"""


@dataclass(kw_only=True)
class GraspIdentityLinkedServiceSettings(HttpLinkedServiceSettings):
    """
    Settings required to connect to the Grasp Identity service.

    Attributes:
        host: Grasp Identity login url.
        auth_type: The type of authentication to use (e.g., BASIC, OAUTH2).
        schema: The schema to use for the connection (e.g., "https").
        oauth: OAuth2 authentication settings (if auth_type is OAUTH2).
        basic: Basic authentication settings (if auth_type is BASIC).
    """

    host: str = field(default="auth.grasp-daas.com")
    """Grasp Identity login url. Default is 'auth.grasp-daas.com/rest-auth/login/'"""

    auth_type: enums.AuthType = field(default=enums.AuthType.BASIC)
    """The type of authentication to use. Default is BASIC."""

    schema: str = field(default="https")
    """The schema to use for the connection. Default is 'https'."""

    oauth: IDPOAuth2AuthSettings | None = None
    """OAuth2 authentication settings (if auth_type is OAUTH2)."""

    bearer: IDPBearerAuthSettings | None = None
    """Bearer authentication settings (if auth_type is BEARER)."""


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
