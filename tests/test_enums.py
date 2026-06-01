"""
**File:** ``test_enums.py``
**Region:** ``tests/test_enums``

ResourceType enum tests.

Covers:
- Enum value definitions and string representations.
- Enum membership and comparison operations.
"""

from __future__ import annotations

from ds_provider_grasp_py_lib.enums import ResourceType


def test_resource_type_dataset_value() -> None:
    """
    It exposes the correct dataset type value.
    """
    assert ResourceType.DATASET_INGRESS == "ds.resource.dataset.grasp-ingress"
    assert ResourceType.DATASET_CART == "ds.resource.dataset.grasp-cart"
    assert ResourceType.DATASET_FILE == "ds.resource.dataset.grasp-file"
    assert ResourceType.DATASET_GOLD == "ds.resource.dataset.grasp-gold"
    assert isinstance(ResourceType.DATASET_INGRESS, str)
    assert isinstance(ResourceType.DATASET_CART, str)
    assert isinstance(ResourceType.DATASET_FILE, str)
    assert isinstance(ResourceType.DATASET_GOLD, str)
    # Linked service value
    assert ResourceType.LINKED_SERVICE_IDENTITY == "ds.linked-service.grasp-identity"
    assert ResourceType.LINKED_SERVICE_IAM_ROLE == "ds.linked-service.grasp-iam-role"
    assert isinstance(ResourceType.LINKED_SERVICE_IDENTITY, str)
    assert isinstance(ResourceType.LINKED_SERVICE_IAM_ROLE, str)


def test_resource_type_enum_membership() -> None:
    """
    It allows checking enum membership.
    """
    assert ResourceType.DATASET_CART in ResourceType
    assert ResourceType.DATASET_INGRESS in ResourceType
    assert ResourceType.DATASET_FILE in ResourceType
    assert ResourceType.DATASET_GOLD in ResourceType
    assert ResourceType.LINKED_SERVICE_IDENTITY in ResourceType
    assert ResourceType.LINKED_SERVICE_IAM_ROLE in ResourceType


def test_resource_type_enum_comparison() -> None:
    """
    It supports equality comparison with strings.
    """
    assert ResourceType.DATASET_INGRESS != ResourceType.DATASET_CART
    assert ResourceType.DATASET_INGRESS != ResourceType.DATASET_FILE
    assert ResourceType.DATASET_CART != ResourceType.DATASET_FILE
    assert ResourceType.DATASET_GOLD != ResourceType.DATASET_CART
    assert ResourceType.DATASET_GOLD != ResourceType.DATASET_INGRESS
    assert ResourceType.DATASET_GOLD != ResourceType.DATASET_FILE
    # Linked service comparison
    assert ResourceType.LINKED_SERVICE_IDENTITY != ResourceType.DATASET_CART
    assert ResourceType.LINKED_SERVICE_IDENTITY != ResourceType.DATASET_INGRESS
    assert ResourceType.LINKED_SERVICE_IDENTITY != ResourceType.DATASET_FILE
    assert ResourceType.LINKED_SERVICE_IDENTITY != ResourceType.DATASET_GOLD
    assert ResourceType.LINKED_SERVICE_IAM_ROLE != ResourceType.LINKED_SERVICE_IDENTITY
