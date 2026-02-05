"""
**File:** ``enums.py``
**Region:** ``ds_provider_grasp_py_lib/enums``

Constants for GRASP provider.

Example:
    >>> ResourceType.DATASET_CART
    'DS.RESOURCE.DATASET.GRASP_CART'
"""

from enum import StrEnum


class ResourceType(StrEnum):
    """
    Constants for GRASP provider.
    """

    DATASET_CART = "DS.RESOURCE.DATASET.GRASP_CART"
    DATASET_INGRESS = "DS.RESOURCE.DATASET.GRASP_INGRESS"
