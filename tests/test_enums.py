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
    assert isinstance(ResourceType.DATASET_INGRESS, str)
    assert isinstance(ResourceType.DATASET_CART, str)
    assert isinstance(ResourceType.DATASET_FILE, str)


def test_resource_type_enum_membership() -> None:
    """
    It allows checking enum membership.
    """
    assert ResourceType.DATASET_CART in ResourceType
    assert ResourceType.DATASET_INGRESS in ResourceType
    assert ResourceType.DATASET_FILE in ResourceType


def test_resource_type_enum_comparison() -> None:
    """
    It supports equality comparison with strings.
    """
    assert ResourceType.DATASET_INGRESS != ResourceType.DATASET_CART
    assert ResourceType.DATASET_INGRESS != ResourceType.DATASET_FILE
    assert ResourceType.DATASET_CART != ResourceType.DATASET_FILE
