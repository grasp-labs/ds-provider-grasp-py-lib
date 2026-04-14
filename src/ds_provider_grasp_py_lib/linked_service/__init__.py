"""
**File**: `__init__.py`
**Region**: `ds_provider_grasp_py_lib.linked_service`

Grasp Identity Linked Service.

Example:
    >>> linked_service = GraspIdentityLinkedService(
    ...     id=uuid.uuid4(),
    ...     name="grasp-identity-linked-service",
    ...     version="1.0.0",
    ...     settings=GraspIdentityLinkedServiceSettings(
    ...         auth_type=enums.AuthType.BASIC,
    ...         basic=BasicAuthSettings(
    ...             username="user",
    ...             password="pass"
    ...         )
    ...     ),
    ... )
    >>> linked_service.connect()
"""

from .identity import GraspIdentityLinkedService, GraspIdentityLinkedServiceSettings

__all__ = [
    "GraspIdentityLinkedService",
    "GraspIdentityLinkedServiceSettings",
]
