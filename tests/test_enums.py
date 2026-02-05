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
    assert ResourceType.DATASET_INGRESS == "DS.RESOURCE.DATASET.GRASP_INGRESS"
    assert ResourceType.DATASET_CART == "DS.RESOURCE.DATASET.GRASP_CART"
    assert isinstance(ResourceType.DATASET_INGRESS, str)
    assert isinstance(ResourceType.DATASET_CART, str)


def test_resource_type_enum_membership() -> None:
    """
    It allows checking enum membership.
    """
    assert ResourceType.DATASET_CART in ResourceType
    assert ResourceType.DATASET_INGRESS in ResourceType


def test_resource_type_enum_comparison() -> None:
    """
    It supports equality comparison with strings.
    """
    assert ResourceType.DATASET_INGRESS != ResourceType.DATASET_CART
