"""
**File:** ``test_cart.py``
**Region:** ``tests/dataset/test_cart``

GraspCartDataset tests.

Covers:
- Dataset type property.
- S3 path generation.
- Read operation with various error conditions.
- Close operation.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from awswrangler.exceptions import NoFilesFound
from ds_resource_plugin_py_lib.common.resource.dataset.errors import (
    NotFoundError,
    ReadError,
)

from ds_provider_grasp_py_lib.enums import ResourceType
from tests.mocks import (
    create_mock_aws_linked_service,
    create_mock_cart_dataset,
    create_test_dataframe,
)


class TestGraspCartDatasetType:
    """Tests for GraspCartDataset type property."""

    def test_type_returns_dataset_cart(self) -> None:
        """
        It returns the correct ResourceType for cart dataset.
        """
        dataset = create_mock_cart_dataset()
        assert dataset.type == ResourceType.DATASET_CART
        assert dataset.type == "ds.resource.dataset.grasp-cart"


class TestGraspCartDatasetS3Path:
    """Tests for GraspCartDataset S3 path generation."""

    @patch("ds_provider_grasp_py_lib.dataset.cart.get_bucket_name")
    def test_get_s3_path_generates_correct_path(
        self,
        mock_get_bucket: MagicMock,
    ) -> None:
        """
        It generates the correct S3 path with all components.
        """
        mock_get_bucket.return_value = "test-bucket"
        dataset = create_mock_cart_dataset(
            owner_id="owner123",
            product_group_name="group-a",
            product_name="product-x",
            version="2.0",
        )
        path = dataset._get_s3_path(tenant_id="tenant456")
        expected = "s3://test-bucket/datalake/cart/group-a/2.0/tenant456/product-x/owner123/data/"
        assert path == expected


class TestGraspCartDatasetRead:
    """Tests for GraspCartDataset read operation."""

    def test_read_raises_read_error_when_tenant_id_not_set(self) -> None:
        """
        It raises ReadError when TENANT_ID environment variable is not set.
        """
        dataset = create_mock_cart_dataset()
        with (
            patch.dict("os.environ", {}, clear=True),
            pytest.raises(ReadError) as exc_info,
        ):
            dataset.read()
        assert "TENANT_ID environment variable is required" in str(exc_info.value)
        assert exc_info.value.status_code == 400

    def test_read_raises_read_error_when_deserializer_not_set(self) -> None:
        """
        It raises ReadError when deserializer is not set.
        """
        dataset = create_mock_cart_dataset(deserializer=None)
        # Manually set deserializer to None after creation
        dataset.deserializer = None

        with (
            patch.dict("os.environ", {"TENANT_ID": "tenant123"}),
            pytest.raises(ReadError) as exc_info,
        ):
            dataset.read()
        assert "Deserializer is not set" in str(exc_info.value)
        assert exc_info.value.status_code == 400

    @patch("ds_provider_grasp_py_lib.dataset.cart.get_bucket_name")
    def test_read_raises_not_found_error_on_no_files_found(
        self,
        mock_get_bucket: MagicMock,
    ) -> None:
        """
        It raises NotFoundError when no files are found at S3 path.
        """
        mock_get_bucket.return_value = "test-bucket"
        mock_deserializer = MagicMock(side_effect=NoFilesFound("No files"))
        dataset = create_mock_cart_dataset(deserializer=mock_deserializer)

        with (
            patch.dict("os.environ", {"TENANT_ID": "tenant123"}),
            pytest.raises(NotFoundError) as exc_info,
        ):
            dataset.read()
        assert "No files found at S3 path" in str(exc_info.value)
        assert exc_info.value.status_code == 404

    @patch("ds_provider_grasp_py_lib.dataset.cart.get_bucket_name")
    def test_read_raises_read_error_on_generic_exception(
        self,
        mock_get_bucket: MagicMock,
    ) -> None:
        """
        It raises ReadError when a generic exception occurs during read.
        """
        mock_get_bucket.return_value = "test-bucket"
        mock_deserializer = MagicMock(side_effect=RuntimeError("Connection timeout"))
        dataset = create_mock_cart_dataset(deserializer=mock_deserializer)

        with (
            patch.dict("os.environ", {"TENANT_ID": "tenant123"}),
            pytest.raises(ReadError) as exc_info,
        ):
            dataset.read()
        assert "Failed to read data from table" in str(exc_info.value)
        assert exc_info.value.status_code == 500

    @patch("ds_provider_grasp_py_lib.dataset.cart.get_bucket_name")
    def test_read_success_filters_history_by_default(
        self,
        mock_get_bucket: MagicMock,
    ) -> None:
        """
        It filters out rows with _valid_to when include_history is False.
        """
        mock_get_bucket.return_value = "test-bucket"
        test_df = create_test_dataframe(rows=3, with_valid_to=True)
        mock_deserializer = MagicMock(return_value=test_df)
        dataset = create_mock_cart_dataset(
            include_history=False,
            deserializer=mock_deserializer,
        )

        with patch.dict("os.environ", {"TENANT_ID": "tenant123"}):
            dataset.read()

        # Should filter out rows where _valid_to is not None
        assert len(dataset.output) == 2

    @patch("ds_provider_grasp_py_lib.dataset.cart.get_bucket_name")
    def test_read_success_includes_history_when_enabled(
        self,
        mock_get_bucket: MagicMock,
    ) -> None:
        """
        It includes all rows when include_history is True.
        """
        mock_get_bucket.return_value = "test-bucket"
        test_df = create_test_dataframe(rows=3, with_valid_to=True)
        mock_deserializer = MagicMock(return_value=test_df)
        dataset = create_mock_cart_dataset(
            include_history=True,
            deserializer=mock_deserializer,
        )

        with patch.dict("os.environ", {"TENANT_ID": "tenant123"}):
            dataset.read()

        # Should include all rows
        assert len(dataset.output) == 3


class TestGraspCartDatasetClose:
    """Tests for GraspCartDataset close operation."""

    def test_close_calls_linked_service_close(self) -> None:
        """
        It calls close on the linked service.
        """
        linked_service = create_mock_aws_linked_service()
        dataset = create_mock_cart_dataset(linked_service=linked_service)
        dataset.close()
        assert linked_service._closed is True
