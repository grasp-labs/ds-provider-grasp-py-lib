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
    ...         client_id="client_id",
    ...         client_secret="client_secret",
    ...     ),
    ... )
    >>> linked_service.connect()
"""

from .identity import GraspIdentityLinkedService, GraspIdentityLinkedServiceSettings

__all__ = [
    "GraspIdentityLinkedService",
    "GraspIdentityLinkedServiceSettings",
]
