"""
**File:** ``test_gold.py``
**Region:** ``tests/dataset/test_gold``

GraspGoldDataset tests.

Covers:
- Dataset type property.
- S3 path generation.
- Read operation with SCD2 filtering and error conditions.
- Create operation with error conditions.
- Close operation.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from awswrangler.exceptions import NoFilesFound
from ds_resource_plugin_py_lib.common.resource.dataset.errors import (
    CreateError,
    NotFoundError,
    ReadError,
)

from ds_provider_grasp_py_lib.dataset.gold import GraspGoldDatasetSettings
from ds_provider_grasp_py_lib.enums import ResourceType
from tests.mocks import (
    create_mock_aws_linked_service,
    create_mock_gold_dataset,
    create_test_dataframe,
)


class TestGraspGoldDatasetSettings:
    """Tests for GraspGoldDatasetSettings validation."""

    def test_invalid_mode_raises_value_error(self) -> None:
        """It raises ValueError when mode is not an awswrangler write mode."""
        with pytest.raises(ValueError, match="Invalid write mode 'replace'"):
            GraspGoldDatasetSettings(dataset_id="orders", mode="replace")

    @pytest.mark.parametrize("mode", ["append", "overwrite", "overwrite_partitions"])
    def test_valid_mode_accepted(self, mode: str) -> None:
        """It accepts each documented awswrangler write mode."""
        settings = GraspGoldDatasetSettings(dataset_id="orders", mode=mode)
        assert settings.mode == mode

    def test_mode_none_accepted(self) -> None:
        """It accepts ``None`` as the default unset mode."""
        settings = GraspGoldDatasetSettings(dataset_id="orders")
        assert settings.mode is None


class TestGraspGoldDatasetType:
    """Tests for GraspGoldDataset type property."""

    def test_type_returns_dataset_gold(self) -> None:
        """It returns the correct ResourceType for gold dataset."""
        dataset = create_mock_gold_dataset()
        assert dataset.type == ResourceType.DATASET_GOLD
        assert dataset.type == "ds.resource.dataset.grasp-gold"


class TestGraspGoldDatasetS3Path:
    """Tests for GraspGoldDataset S3 path generation."""

    @patch("ds_provider_grasp_py_lib.dataset.gold.get_bucket_name")
    def test_get_s3_path_generates_correct_path(self, mock_get_bucket: MagicMock) -> None:
        """It generates the correct S3 path with tenant_id and dataset_id."""
        mock_get_bucket.return_value = "test-bucket"
        dataset = create_mock_gold_dataset(dataset_id="orders")
        path = dataset._get_s3_path(tenant_id="tenant456")
        assert path == "s3://test-bucket/tenant456/gold/orders/"


class TestGraspGoldDatasetRead:
    """Tests for GraspGoldDataset read operation."""

    def test_read_raises_read_error_when_tenant_id_not_set(self) -> None:
        """It raises ReadError when neither the setting nor the env var is set."""
        dataset = create_mock_gold_dataset()
        with (
            patch.dict("os.environ", {}, clear=True),
            pytest.raises(ReadError) as exc_info,
        ):
            dataset.read()
        assert "tenant_id setting or TENANT_ID environment variable is required" in str(
            exc_info.value,
        )
        assert exc_info.value.status_code == 400

    @patch("ds_provider_grasp_py_lib.dataset.gold.get_bucket_name")
    def test_read_uses_tenant_id_from_settings(self, mock_get_bucket: MagicMock) -> None:
        """It uses the tenant_id from settings when provided, ignoring the env var."""
        mock_get_bucket.return_value = "test-bucket"
        test_df = create_test_dataframe(rows=1, with_valid_to=False)
        mock_deserializer = MagicMock(return_value=test_df)
        dataset = create_mock_gold_dataset(
            dataset_id="orders",
            tenant_id="tenant-from-settings",
            deserializer=mock_deserializer,
        )
        with patch.dict("os.environ", {"TENANT_ID": "tenant-from-env"}):
            dataset.read()
        called_path = mock_deserializer.call_args.args[0]
        assert called_path == "s3://test-bucket/tenant-from-settings/gold/orders/"

    def test_read_raises_read_error_when_deserializer_not_set(self) -> None:
        """It raises ReadError when deserializer is not set."""
        dataset = create_mock_gold_dataset()
        dataset.deserializer = None
        with (
            patch.dict("os.environ", {"TENANT_ID": "tenant123"}),
            pytest.raises(ReadError) as exc_info,
        ):
            dataset.read()
        assert "Deserializer is not set" in str(exc_info.value)
        assert exc_info.value.status_code == 400

    @patch("ds_provider_grasp_py_lib.dataset.gold.get_bucket_name")
    def test_read_raises_not_found_error_on_no_files_found(self, mock_get_bucket: MagicMock) -> None:
        """It raises NotFoundError when no files are found at S3 path."""
        mock_get_bucket.return_value = "test-bucket"
        mock_deserializer = MagicMock(side_effect=NoFilesFound("No files"))
        dataset = create_mock_gold_dataset(deserializer=mock_deserializer)
        with (
            patch.dict("os.environ", {"TENANT_ID": "tenant123"}),
            pytest.raises(NotFoundError) as exc_info,
        ):
            dataset.read()
        assert "No files found at S3 path" in str(exc_info.value)
        assert exc_info.value.status_code == 404

    @patch("ds_provider_grasp_py_lib.dataset.gold.get_bucket_name")
    def test_read_raises_read_error_on_generic_exception(self, mock_get_bucket: MagicMock) -> None:
        """It raises ReadError when a generic exception occurs during read."""
        mock_get_bucket.return_value = "test-bucket"
        mock_deserializer = MagicMock(side_effect=RuntimeError("Connection timeout"))
        dataset = create_mock_gold_dataset(deserializer=mock_deserializer)
        with (
            patch.dict("os.environ", {"TENANT_ID": "tenant123"}),
            pytest.raises(ReadError) as exc_info,
        ):
            dataset.read()
        assert "Failed to read data from table" in str(exc_info.value)
        assert exc_info.value.status_code == 500

    @patch("ds_provider_grasp_py_lib.dataset.gold.get_bucket_name")
    def test_read_success_filters_history_by_default(self, mock_get_bucket: MagicMock) -> None:
        """It filters out rows with _valid_to when include_history is False."""
        mock_get_bucket.return_value = "test-bucket"
        test_df = create_test_dataframe(rows=3, with_valid_to=True)
        mock_deserializer = MagicMock(return_value=test_df)
        dataset = create_mock_gold_dataset(
            include_history=False, deserializer=mock_deserializer,
        )
        with patch.dict("os.environ", {"TENANT_ID": "tenant123"}):
            dataset.read()
        assert len(dataset.output) == 2

    @patch("ds_provider_grasp_py_lib.dataset.gold.get_bucket_name")
    def test_read_success_includes_history_when_enabled(self, mock_get_bucket: MagicMock) -> None:
        """It includes all rows when include_history is True."""
        mock_get_bucket.return_value = "test-bucket"
        test_df = create_test_dataframe(rows=3, with_valid_to=True)
        mock_deserializer = MagicMock(return_value=test_df)
        dataset = create_mock_gold_dataset(
            include_history=True, deserializer=mock_deserializer,
        )
        with patch.dict("os.environ", {"TENANT_ID": "tenant123"}):
            dataset.read()
        assert len(dataset.output) == 3


class TestGraspGoldDatasetCreate:
    """Tests for GraspGoldDataset create operation."""

    def test_create_raises_create_error_when_tenant_id_not_set(self) -> None:
        """It raises CreateError when neither the setting nor the env var is set."""
        dataset = create_mock_gold_dataset()
        with (
            patch.dict("os.environ", {}, clear=True),
            pytest.raises(CreateError) as exc_info,
        ):
            dataset.create()
        assert "tenant_id setting or TENANT_ID environment variable is required" in str(
            exc_info.value,
        )
        assert exc_info.value.status_code == 400

    @patch("ds_provider_grasp_py_lib.dataset.gold.get_bucket_name")
    def test_create_uses_tenant_id_from_settings(self, mock_get_bucket: MagicMock) -> None:
        """It uses the tenant_id from settings when provided, ignoring the env var."""
        mock_get_bucket.return_value = "test-bucket"
        mock_serializer = MagicMock()
        mock_serializer.kwargs = {}
        dataset = create_mock_gold_dataset(
            dataset_id="orders",
            tenant_id="tenant-from-settings",
            serializer=mock_serializer,
        )
        dataset.input = create_test_dataframe(rows=1, with_valid_to=False)
        with patch.dict("os.environ", {"TENANT_ID": "tenant-from-env"}):
            dataset.create()
        assert (
            mock_serializer.kwargs["path"]
            == "s3://test-bucket/tenant-from-settings/gold/orders/"
        )

    def test_create_raises_create_error_when_serializer_not_set(self) -> None:
        """It raises CreateError when serializer is not set."""
        dataset = create_mock_gold_dataset()
        dataset.serializer = None
        with (
            patch.dict("os.environ", {"TENANT_ID": "tenant123"}),
            pytest.raises(CreateError) as exc_info,
        ):
            dataset.create()
        assert "Serializer is not set" in str(exc_info.value)
        assert exc_info.value.status_code == 400

    @patch("ds_provider_grasp_py_lib.dataset.gold.get_bucket_name")
    def test_create_raises_create_error_on_generic_exception(self, mock_get_bucket: MagicMock) -> None:
        """It raises CreateError when the underlying write fails."""
        mock_get_bucket.return_value = "test-bucket"
        mock_serializer = MagicMock(side_effect=RuntimeError("S3 write failed"))
        mock_serializer.kwargs = {}
        dataset = create_mock_gold_dataset(serializer=mock_serializer)
        dataset.input = create_test_dataframe(rows=2, with_valid_to=False)
        with (
            patch.dict("os.environ", {"TENANT_ID": "tenant123"}),
            pytest.raises(CreateError) as exc_info,
        ):
            dataset.create()
        assert "Failed to write data to S3 path" in str(exc_info.value)
        assert exc_info.value.status_code == 500

    @patch("ds_provider_grasp_py_lib.dataset.gold.get_bucket_name")
    def test_create_success_invokes_serializer(self, mock_get_bucket: MagicMock) -> None:
        """It invokes the serializer with the resolved S3 path."""
        mock_get_bucket.return_value = "test-bucket"
        mock_serializer = MagicMock()
        mock_serializer.kwargs = {}
        dataset = create_mock_gold_dataset(dataset_id="orders", serializer=mock_serializer)
        dataset.input = create_test_dataframe(rows=2, with_valid_to=False)
        with patch.dict("os.environ", {"TENANT_ID": "tenant123"}):
            dataset.create()
        assert mock_serializer.kwargs["path"] == "s3://test-bucket/tenant123/gold/orders/"
        assert mock_serializer.kwargs["compression"] == "snappy"
        assert "partition_cols" not in mock_serializer.kwargs
        assert "mode" not in mock_serializer.kwargs
        assert "dataset" not in mock_serializer.kwargs
        mock_serializer.assert_called_once()

    @patch("ds_provider_grasp_py_lib.dataset.gold.get_bucket_name")
    def test_create_forwards_partition_cols_and_enables_dataset(
        self, mock_get_bucket: MagicMock,
    ) -> None:
        """It forwards partition_cols and enables dataset mode."""
        mock_get_bucket.return_value = "test-bucket"
        mock_serializer = MagicMock()
        mock_serializer.kwargs = {}
        dataset = create_mock_gold_dataset(
            serializer=mock_serializer,
            partition_cols=["year", "month"],
        )
        dataset.input = create_test_dataframe(rows=2, with_valid_to=False)
        with patch.dict("os.environ", {"TENANT_ID": "tenant123"}):
            dataset.create()
        assert mock_serializer.kwargs["partition_cols"] == ["year", "month"]
        assert mock_serializer.kwargs["dataset"] is True

    @patch("ds_provider_grasp_py_lib.dataset.gold.get_bucket_name")
    def test_create_forwards_mode_and_enables_dataset(
        self, mock_get_bucket: MagicMock,
    ) -> None:
        """It forwards mode and enables dataset mode."""
        mock_get_bucket.return_value = "test-bucket"
        mock_serializer = MagicMock()
        mock_serializer.kwargs = {}
        dataset = create_mock_gold_dataset(
            serializer=mock_serializer,
            mode="overwrite_partitions",
        )
        dataset.input = create_test_dataframe(rows=2, with_valid_to=False)
        with patch.dict("os.environ", {"TENANT_ID": "tenant123"}):
            dataset.create()
        assert mock_serializer.kwargs["mode"] == "overwrite_partitions"
        assert mock_serializer.kwargs["dataset"] is True

    @patch("ds_provider_grasp_py_lib.dataset.gold.get_bucket_name")
    def test_create_omits_compression_when_none(self, mock_get_bucket: MagicMock) -> None:
        """It omits compression from kwargs when explicitly set to None."""
        mock_get_bucket.return_value = "test-bucket"
        mock_serializer = MagicMock()
        mock_serializer.kwargs = {}
        dataset = create_mock_gold_dataset(serializer=mock_serializer, compression=None)
        dataset.input = create_test_dataframe(rows=2, with_valid_to=False)
        with patch.dict("os.environ", {"TENANT_ID": "tenant123"}):
            dataset.create()
        assert "compression" not in mock_serializer.kwargs

    @patch("ds_provider_grasp_py_lib.dataset.gold.get_bucket_name")
    def test_create_uses_custom_compression(self, mock_get_bucket: MagicMock) -> None:
        """It forwards a custom compression codec."""
        mock_get_bucket.return_value = "test-bucket"
        mock_serializer = MagicMock()
        mock_serializer.kwargs = {}
        dataset = create_mock_gold_dataset(serializer=mock_serializer, compression="gzip")
        dataset.input = create_test_dataframe(rows=2, with_valid_to=False)
        with patch.dict("os.environ", {"TENANT_ID": "tenant123"}):
            dataset.create()
        assert mock_serializer.kwargs["compression"] == "gzip"


class TestGraspGoldDatasetClose:
    """Tests for GraspGoldDataset close operation."""

    def test_close_calls_linked_service_close(self) -> None:
        """It calls close on the linked service."""
        linked_service = create_mock_aws_linked_service()
        dataset = create_mock_gold_dataset(linked_service=linked_service)
        dataset.close()
        assert linked_service._closed is True

