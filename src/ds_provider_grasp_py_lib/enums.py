"""
**File:** ``enums.py``
**Region:** ``ds_provider_grasp_py_lib/enums``

Constants for GRASP provider.

Example:
    >>> ResourceType.DATASET_CART
    'ds.resource.dataset.grasp-cart'
"""

from enum import StrEnum


class ResourceType(StrEnum):
    """
    Constants for GRASP provider.
    """

    DATASET_CART = "ds.resource.dataset.grasp-cart"
    DATASET_INGRESS = "ds.resource.dataset.grasp-ingress"
    DATASET_FILE = "ds.resource.dataset.grasp-file"
    LINKED_SERVICE_IDENTITY = "ds.linked-service.grasp-identity"
