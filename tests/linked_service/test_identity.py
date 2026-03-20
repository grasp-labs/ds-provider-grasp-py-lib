"""
**File:** ``test_identity.py``
**Region:** ``tests/linked_service/test_identity``

Unit tests for GraspIdentityLinkedService and GraspIdentityLinkedServiceSettings.
"""

import uuid

from ds_provider_grasp_py_lib.enums import ResourceType
from ds_provider_grasp_py_lib.linked_service.identity import (
    GraspIdentityLinkedService,
    GraspIdentityLinkedServiceSettings,
)


def test_settings_defaults():
    settings = GraspIdentityLinkedServiceSettings()
    assert settings.host == "https://auth.grasp-daas.com/"
    assert settings.login_endpoint == "https://auth.grasp-daas.com/rest-auth/login/"
    assert settings.token_endpoint == "https://auth.grasp-daas.com/oauth/token/"
    assert settings.auth_type.name == "BEARER"
    assert settings.client_id is None
    assert settings.client_secret is None
    assert settings.email is None
    assert settings.password is None


def test_settings_custom_values():
    settings = GraspIdentityLinkedServiceSettings(
        client_id="abc",
        client_secret="secret",
        email="user@example.com",
        password="pw",
        host="https://custom/",
        login_endpoint="/login/",
        token_endpoint="/token/",
    )
    assert settings.client_id == "abc"
    assert settings.client_secret == "secret"
    assert settings.email == "user@example.com"
    assert settings.password == "pw"
    assert settings.host == "https://custom/"
    assert settings.login_endpoint == "/login/"
    assert settings.token_endpoint == "/token/"


def test_linked_service_type_property():
    settings = GraspIdentityLinkedServiceSettings()
    service = GraspIdentityLinkedService(
        id=uuid.uuid4(),
        name="identity-service",
        version="1.0.0",
        settings=settings,
    )
    assert service.type == ResourceType.LINKED_SERVICE_IDENTITY
    assert str(service.type) == "ds.linked-service.grasp-identity"


def test_linked_service_post_init_runs():
    settings = GraspIdentityLinkedServiceSettings()
    # Should not raise
    GraspIdentityLinkedService(
        id=uuid.uuid4(),
        name="identity-service",
        version="1.0.0",
        settings=settings,
    )
