"""
**File:** ``test_dataset_unimplemented.py``
**Region:** ``tests/dataset/test_dataset_unimplemented``

Grasp Dataset unauthorized/not supported methods tests.

Covers:
- create(), delete(), update(), and rename() methods raising AuthorizationError/NotSupportedError.
"""

from __future__ import annotations

import pytest
from ds_resource_plugin_py_lib.common.resource.errors import NotSupportedError
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import (
    AuthorizationError,
)

from ds_provider_grasp_py_lib.enums import ResourceType
from tests.mocks import create_mock_cart_dataset, create_mock_file_dataset, create_mock_ingress_dataset


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

    def test_upsert_raises_authorization_error(self) -> None:
        """
        It raises AuthorizationError for upsert operation.
        """
        dataset = create_mock_cart_dataset()
        with pytest.raises(AuthorizationError) as exc_info:
            dataset.upsert()
        assert "not authorized to upsert" in str(exc_info.value)
        assert exc_info.value.status_code == 403

    def test_purge_raises_authorization_error(self) -> None:
        """
        It raises AuthorizationError for purge operation.
        """
        dataset = create_mock_cart_dataset()
        with pytest.raises(AuthorizationError) as exc_info:
            dataset.purge()
        assert "not authorized to purge" in str(exc_info.value)
        assert exc_info.value.status_code == 403

    def test_list_raises_authorization_error(self) -> None:
        """
        It raises AuthorizationError for list operation.
        """
        dataset = create_mock_cart_dataset()
        with pytest.raises(AuthorizationError) as exc_info:
            dataset.list()
        assert "not authorized to list" in str(exc_info.value)
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

    def test_upsert_raises_authorization_error(self) -> None:
        """
        It raises AuthorizationError for upsert operation.
        """
        dataset = create_mock_ingress_dataset()
        with pytest.raises(AuthorizationError) as exc_info:
            dataset.upsert()
        assert "not authorized to upsert" in str(exc_info.value)
        assert exc_info.value.status_code == 403

    def test_purge_raises_authorization_error(self) -> None:
        """
        It raises AuthorizationError for purge operation.
        """
        dataset = create_mock_ingress_dataset()
        with pytest.raises(AuthorizationError) as exc_info:
            dataset.purge()
        assert "not authorized to purge" in str(exc_info.value)
        assert exc_info.value.status_code == 403

    def test_list_raises_authorization_error(self) -> None:
        """
        It raises AuthorizationError for list operation.
        """
        dataset = create_mock_ingress_dataset()
        with pytest.raises(AuthorizationError) as exc_info:
            dataset.list()
        assert "not authorized to list" in str(exc_info.value)
        assert exc_info.value.status_code == 403


class TestGraspFileDatasetUnsupported:
    """Tests for GraspFileDataset unsupported operations."""

    def test_delete_raises_not_supported_error(self) -> None:
        """It raises NotSupportedError for delete operation."""
        dataset = create_mock_file_dataset()
        with pytest.raises(NotSupportedError) as exc_info:
            dataset.delete()
        assert "Method 'delete' is not supported by this provider." in str(exc_info.value)
        assert exc_info.value.status_code == 501
        assert exc_info.value.details == {"method": "delete", "provider": ResourceType.DATASET_FILE.value}

    def test_update_raises_not_supported_error(self) -> None:
        """It raises NotSupportedError for update operation."""
        dataset = create_mock_file_dataset()
        with pytest.raises(NotSupportedError) as exc_info:
            dataset.update()
        assert "Method 'update' is not supported by this provider." in str(exc_info.value)
        assert exc_info.value.status_code == 501
        assert exc_info.value.details == {"method": "update", "provider": ResourceType.DATASET_FILE.value}

    def test_upsert_raises_not_supported_error(self) -> None:
        """It raises NotSupportedError for upsert operation."""
        dataset = create_mock_file_dataset()
        with pytest.raises(NotSupportedError) as exc_info:
            dataset.upsert()
        assert "Method 'upsert' is not supported by this provider." in str(exc_info.value)
        assert exc_info.value.status_code == 501
        assert exc_info.value.details == {"method": "upsert", "provider": ResourceType.DATASET_FILE.value}

    def test_purge_raises_not_supported_error(self) -> None:
        """It raises NotSupportedError for purge operation."""
        dataset = create_mock_file_dataset()
        with pytest.raises(NotSupportedError) as exc_info:
            dataset.purge()
        assert "Method 'purge' is not supported by this provider." in str(exc_info.value)
        assert exc_info.value.status_code == 501
        assert exc_info.value.details == {"method": "purge", "provider": ResourceType.DATASET_FILE.value}

    def test_rename_raises_not_supported_error(self) -> None:
        """It raises NotSupportedError for rename operation."""
        dataset = create_mock_file_dataset()
        with pytest.raises(NotSupportedError) as exc_info:
            dataset.rename()
        assert "Method 'rename' is not supported by this provider." in str(exc_info.value)
        assert exc_info.value.status_code == 501
        assert exc_info.value.details == {"method": "rename", "provider": ResourceType.DATASET_FILE.value}
