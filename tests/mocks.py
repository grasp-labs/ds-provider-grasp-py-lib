"""
**File:** ``mocks.py``
**Region:** ``tests/mocks``

Shared mock utilities for Grasp provider tests.

Provides reusable mocks for AWS linked services, connections, and datasets
to enable isolated unit testing without requiring actual AWS connections.
"""

from __future__ import annotations

import uuid
from typing import Any, cast
from unittest.mock import MagicMock

import pandas as pd

from ds_provider_grasp_py_lib.dataset.cart import (
    GraspCartDataset,
    GraspCartDatasetSettings,
)
from ds_provider_grasp_py_lib.dataset.ingress import (
    GraspIngressDataset,
    GraspIngressDatasetSettings,
)

_UNSET = object()


class MockBoto3Session:
    """
    Mock boto3 connection for testing.

    Provides a mock connection with configurable behavior.
    """

    def __init__(self) -> None:
        self.client = MagicMock()
        self.resource = MagicMock()


class MockAWSLinkedService:
    """
    Mock AWS Linked Service for testing.

    Provides a mock linked service with a configurable connection.
    """

    def __init__(self, connection: MockBoto3Session | None = None) -> None:
        self.connection = connection
        self._closed = False

    def close(self) -> None:
        """Close the linked service."""
        self._closed = True


def create_mock_aws_linked_service(
    with_connection: bool = True,
) -> MockAWSLinkedService:
    """
    Create a mock AWSLinkedService for testing.

    Args:
        with_connection: Whether to include a mock connection.

    Returns:
        MockAWSLinkedService: A linked service instance with optional mock connection.
    """
    connection = MockBoto3Session() if with_connection else None
    return MockAWSLinkedService(connection=connection)


def create_mock_cart_dataset(
    owner_id: str = "test-owner",
    product_group_name: str = "test-product-group",
    product_name: str = "test-product",
    version: str = "1.0",
    include_history: bool = False,
    linked_service: MockAWSLinkedService | None = None,
    deserializer: Any = _UNSET,
    serializer: Any = _UNSET,
) -> GraspCartDataset[Any, Any]:
    """
    Create a mock GraspCartDataset for testing.

    Args:
        owner_id: The owner ID.
        product_group_name: The product group name.
        product_name: The product name.
        version: The version.
        include_history: Whether to include history.
        linked_service: Optional linked service. If None, creates a mock one.
        deserializer: Optional deserializer.
        serializer: Optional serializer.

    Returns:
        GraspCartDataset: A dataset instance ready for testing.
    """
    if linked_service is None:
        linked_service = create_mock_aws_linked_service()

    settings = GraspCartDatasetSettings(
        owner_id=owner_id,
        product_group_name=product_group_name,
        product_name=product_name,
        version=version,
        include_history=include_history,
    )
    dataset_kwargs: dict[str, Any] = {
        "id": uuid.uuid4(),
        "name": "test-cart-dataset",
        "version": "1.0.0",
        "linked_service": cast(Any, linked_service),
        "settings": settings,
    }
    if deserializer is not _UNSET:
        dataset_kwargs["deserializer"] = deserializer
    if serializer is not _UNSET:
        dataset_kwargs["serializer"] = serializer

    dataset = GraspCartDataset(
        **dataset_kwargs,
    )
    return dataset


def create_mock_ingress_dataset(
    linked_service: MockAWSLinkedService | None = None,
    deserializer: Any = _UNSET,
    serializer: Any = _UNSET,
) -> GraspIngressDataset[Any, Any]:
    """
    Create a mock GraspIngressDataset for testing.

    Args:
        linked_service: Optional linked service. If None, creates a mock one.
        deserializer: Optional deserializer.
        serializer: Optional serializer.

    Returns:
        GraspIngressDataset: A dataset instance ready for testing.
    """
    if linked_service is None:
        linked_service = create_mock_aws_linked_service()

    settings = GraspIngressDatasetSettings()
    dataset_kwargs: dict[str, Any] = {
        "id": uuid.uuid4(),
        "name": "test-ingress-dataset",
        "version": "1.0.0",
        "linked_service": cast("Any", linked_service),
        "settings": settings,
    }
    if deserializer is not _UNSET:
        dataset_kwargs["deserializer"] = deserializer
    if serializer is not _UNSET:
        dataset_kwargs["serializer"] = serializer

    dataset = GraspIngressDataset(**dataset_kwargs)
    return dataset


def create_test_dataframe(rows: int = 3, with_valid_to: bool = True) -> pd.DataFrame:
    """
    Create a test pandas DataFrame for testing.

    Args:
        rows: Number of rows to create.
        with_valid_to: Whether to include _valid_to column for cart dataset testing.

    Returns:
        pd.DataFrame: A DataFrame with test data.
    """
    data = {
        "id": list(range(1, rows + 1)),
        "name": [f"Name{i}" for i in range(1, rows + 1)],
        "status": ["active", "inactive", "pending"][:rows],
        "amount": [10.5, 20.0, 30.75][:rows],
        "is_active": [True, False, True][:rows],
    }
    if with_valid_to:
        data["_valid_to"] = [None, "2024-01-01", None][:rows]
    return pd.DataFrame(data)
