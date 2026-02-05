"""
**File:** ``test_dataset_unimplemented.py``
**Region:** ``tests/dataset/test_dataset_unimplemented``

Grasp Dataset unauthorized methods tests.

Covers:
- create(), delete(), update(), and rename() methods raising AuthorizationError.
"""

from __future__ import annotations

import pytest
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import (
    AuthorizationError,
)

from tests.mocks import create_mock_cart_dataset, create_mock_ingress_dataset


class TestGraspCartDatasetUnauthorized:
    """Tests for GraspCartDataset unauthorized operations."""

    def test_create_raises_authorization_error(self) -> None:
        """
        It raises AuthorizationError for create operation.
        """
        dataset = create_mock_cart_dataset()
        with pytest.raises(AuthorizationError) as exc_info:
            dataset.create()
        assert "not authorized to create" in str(exc_info.value)
        assert exc_info.value.status_code == 403

    def test_delete_raises_authorization_error(self) -> None:
        """
        It raises AuthorizationError for delete operation.
        """
        dataset = create_mock_cart_dataset()
        with pytest.raises(AuthorizationError) as exc_info:
            dataset.delete()
        assert "not authorized to delete" in str(exc_info.value)
        assert exc_info.value.status_code == 403

    def test_update_raises_authorization_error(self) -> None:
        """
        It raises AuthorizationError for update operation.
        """
        dataset = create_mock_cart_dataset()
        with pytest.raises(AuthorizationError) as exc_info:
            dataset.update()
        assert "not authorized to update" in str(exc_info.value)
        assert exc_info.value.status_code == 403

    def test_rename_raises_authorization_error(self) -> None:
        """
        It raises AuthorizationError for rename operation.
        """
        dataset = create_mock_cart_dataset()
        with pytest.raises(AuthorizationError) as exc_info:
            dataset.rename()
        assert "not authorized to rename" in str(exc_info.value)
        assert exc_info.value.status_code == 403


class TestGraspIngressDatasetUnauthorized:
    """Tests for GraspIngressDataset unauthorized operations."""

    def test_create_raises_authorization_error(self) -> None:
        """
        It raises AuthorizationError for create operation.
        """
        dataset = create_mock_ingress_dataset()
        with pytest.raises(AuthorizationError) as exc_info:
            dataset.create()
        assert "not authorized to create" in str(exc_info.value)
        assert exc_info.value.status_code == 403

    def test_delete_raises_authorization_error(self) -> None:
        """
        It raises AuthorizationError for delete operation.
        """
        dataset = create_mock_ingress_dataset()
        with pytest.raises(AuthorizationError) as exc_info:
            dataset.delete()
        assert "not authorized to delete" in str(exc_info.value)
        assert exc_info.value.status_code == 403

    def test_update_raises_authorization_error(self) -> None:
        """
        It raises AuthorizationError for update operation.
        """
        dataset = create_mock_ingress_dataset()
        with pytest.raises(AuthorizationError) as exc_info:
            dataset.update()
        assert "not authorized to update" in str(exc_info.value)
        assert exc_info.value.status_code == 403

    def test_rename_raises_authorization_error(self) -> None:
        """
        It raises AuthorizationError for rename operation.
        """
        dataset = create_mock_ingress_dataset()
        with pytest.raises(AuthorizationError) as exc_info:
            dataset.rename()
        assert "not authorized to rename" in str(exc_info.value)
        assert exc_info.value.status_code == 403
