"""
**File:** ``test_ingress.py``
**Region:** ``tests/dataset/test_ingress``

GraspIngressDataset tests.

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
    create_mock_ingress_dataset,
    create_test_dataframe,
)


class TestGraspIngressDatasetType:
    """Tests for GraspIngressDataset type property."""

    def test_type_returns_dataset_ingress(self) -> None:
        """
        It returns the correct ResourceType for ingress dataset.
        """
        dataset = create_mock_ingress_dataset()
        assert dataset.type == ResourceType.DATASET_INGRESS
        assert dataset.type == "ds.resource.dataset.grasp-ingress"


class TestGraspIngressDatasetS3Path:
    """Tests for GraspIngressDataset S3 path generation."""

    @patch("ds_provider_grasp_py_lib.dataset.ingress.get_bucket_name")
    def test_get_s3_path_generates_correct_path(
        self,
        mock_get_bucket: MagicMock,
    ) -> None:
        """
        It generates the correct S3 path with all components.
        """
        mock_get_bucket.return_value = "test-bucket"
        dataset = create_mock_ingress_dataset()
        path = dataset._get_s3_path(tenant_id="tenant123", session_id="session456")
        expected = f"s3://test-bucket/datalake/workflows/tenant123/{dataset.id}/session456.json"
        assert path == expected


class TestGraspIngressDatasetRead:
    """Tests for GraspIngressDataset read operation."""

    def test_read_raises_read_error_when_tenant_id_not_set(self) -> None:
        """
        It raises ReadError when TENANT_ID environment variable is not set.
        """
        dataset = create_mock_ingress_dataset()
        with (
            patch.dict("os.environ", {"SESSION_ID": "session123"}, clear=True),
            pytest.raises(ReadError) as exc_info,
        ):
            dataset.read()
        assert "TENANT_ID and SESSION_ID environment variables are required" in str(exc_info.value)
        assert exc_info.value.status_code == 400

    def test_read_raises_read_error_when_session_id_not_set(self) -> None:
        """
        It raises ReadError when SESSION_ID environment variable is not set.
        """
        dataset = create_mock_ingress_dataset()
        with (
            patch.dict("os.environ", {"TENANT_ID": "tenant123"}, clear=True),
            pytest.raises(ReadError) as exc_info,
        ):
            dataset.read()
        assert "TENANT_ID and SESSION_ID environment variables are required" in str(exc_info.value)
        assert exc_info.value.status_code == 400

    def test_read_raises_read_error_when_both_env_vars_not_set(self) -> None:
        """
        It raises ReadError when both env variables are not set.
        """
        dataset = create_mock_ingress_dataset()
        with (
            patch.dict("os.environ", {}, clear=True),
            pytest.raises(ReadError) as exc_info,
        ):
            dataset.read()
        assert "TENANT_ID and SESSION_ID environment variables are required" in str(exc_info.value)
        assert exc_info.value.status_code == 400

    def test_read_raises_read_error_when_deserializer_not_set(self) -> None:
        """
        It raises ReadError when deserializer is not set.
        """
        dataset = create_mock_ingress_dataset(deserializer=None)
        # Manually set deserializer to None after creation
        dataset.deserializer = None

        with (
            patch.dict(
                "os.environ",
                {"TENANT_ID": "tenant123", "SESSION_ID": "session123"},
            ),
            pytest.raises(ReadError) as exc_info,
        ):
            dataset.read()
        assert "Deserializer is not set" in str(exc_info.value)
        assert exc_info.value.status_code == 400

    @patch("ds_provider_grasp_py_lib.dataset.ingress.get_bucket_name")
    def test_read_raises_not_found_error_on_no_files_found(
        self,
        mock_get_bucket: MagicMock,
    ) -> None:
        """
        It raises NotFoundError when no files are found at S3 path.
        """
        mock_get_bucket.return_value = "test-bucket"
        mock_deserializer = MagicMock(side_effect=NoFilesFound("No files"))
        dataset = create_mock_ingress_dataset(deserializer=mock_deserializer)

        with (
            patch.dict(
                "os.environ",
                {"TENANT_ID": "tenant123", "SESSION_ID": "session456"},
            ),
            pytest.raises(NotFoundError) as exc_info,
        ):
            dataset.read()
        assert "No ingress found" in str(exc_info.value)
        assert "tenant_id: tenant123" in str(exc_info.value)
        assert "session_id: session456" in str(exc_info.value)
        assert exc_info.value.status_code == 404

    @patch("ds_provider_grasp_py_lib.dataset.ingress.get_bucket_name")
    def test_read_raises_read_error_on_generic_exception(
        self,
        mock_get_bucket: MagicMock,
    ) -> None:
        """
        It raises ReadError when a generic exception occurs during read.
        """
        mock_get_bucket.return_value = "test-bucket"
        mock_deserializer = MagicMock(side_effect=RuntimeError("Connection timeout"))
        dataset = create_mock_ingress_dataset(deserializer=mock_deserializer)

        with (
            patch.dict(
                "os.environ",
                {"TENANT_ID": "tenant123", "SESSION_ID": "session123"},
            ),
            pytest.raises(ReadError) as exc_info,
        ):
            dataset.read()
        assert "Failed to read data from S3 path" in str(exc_info.value)
        assert exc_info.value.status_code == 500

    @patch("ds_provider_grasp_py_lib.dataset.ingress.get_bucket_name")
    def test_read_success(
        self,
        mock_get_bucket: MagicMock,
    ) -> None:
        """
        It successfully reads data from S3.
        """
        mock_get_bucket.return_value = "test-bucket"
        test_df = create_test_dataframe(rows=3, with_valid_to=False)
        mock_deserializer = MagicMock(return_value=test_df)
        dataset = create_mock_ingress_dataset(deserializer=mock_deserializer)

        with patch.dict(
            "os.environ",
            {"TENANT_ID": "tenant123", "SESSION_ID": "session123"},
        ):
            dataset.read()

        assert len(dataset.output) == 3


class TestGraspIngressDatasetClose:
    """Tests for GraspIngressDataset close operation."""

    def test_close_calls_linked_service_close(self) -> None:
        """
        It calls close on the linked service.
        """
        linked_service = create_mock_aws_linked_service()
        dataset = create_mock_ingress_dataset(linked_service=linked_service)
        dataset.close()
        assert linked_service._closed is True
