"""
**File:** ``test_identity.py``
**Region:** ``tests/linked_service/test_identity``

Unit tests for GraspIdentityLinkedService and GraspIdentityLinkedServiceSettings.
"""

from __future__ import annotations

import uuid

from ds_protocol_http_py_lib import enums

from ds_provider_grasp_py_lib.enums import ResourceType
from ds_provider_grasp_py_lib.linked_service.identity import (
    GraspIdentityLinkedService,
    GraspIdentityLinkedServiceSettings,
)


def test_settings_defaults() -> None:
    """Test that GraspIdentityLinkedServiceSettings has correct default values."""
    settings = GraspIdentityLinkedServiceSettings()
    assert settings.host == "auth-dev.grasp-daas.com/rest-auth/login/"
    assert settings.auth_type.name == "BASIC"
    assert settings.schema == "https"
    assert settings.oauth is None
    assert settings.basic is None


def test_settings_custom_values() -> None:
    """Test that GraspIdentityLinkedServiceSettings can be instantiated with custom values."""
    settings = GraspIdentityLinkedServiceSettings(
        host="custom-host",
        auth_type=enums.AuthType.BASIC,  # or OAUTH2 if you want to test that
        schema="http",
        oauth="oauth-settings",
        basic="basic-settings",
    )
    assert settings.host == "custom-host"
    assert settings.schema == "http"
    assert settings.oauth == "oauth-settings"
    assert settings.basic == "basic-settings"


def test_linked_service_type_property() -> None:
    """Test that GraspIdentityLinkedService has the correct type property."""
    settings = GraspIdentityLinkedServiceSettings()
    service = GraspIdentityLinkedService(
        id=uuid.uuid4(),
        name="identity-service",
        version="1.0.0",
        settings=settings,
    )
    assert service.type == ResourceType.LINKED_SERVICE_IDENTITY
    assert str(service.type) == "ds.linked-service.grasp-identity"


def test_linked_service_post_init_runs() -> None:
    """Test that GraspIdentityLinkedService can be instantiated without errors."""
    settings = GraspIdentityLinkedServiceSettings()
    # Should not raise
    GraspIdentityLinkedService(
        id=uuid.uuid4(),
        name="identity-service",
        version="1.0.0",
        settings=settings,
    )
