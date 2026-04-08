"""
**File:** ``test_file.py``
**Region:** ``tests/dataset/test_file``

GraspFileDataset tests.

Covers:
- Dataset type property.
- S3 path handling.
- Read, create, list, and purge behavior.
- Unsupported operations.
- Close operation.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from awswrangler.exceptions import NoFilesFound
from ds_resource_plugin_py_lib.common.resource.dataset.errors import (
    CreateError,
    NotFoundError,
    ReadError,
)
from ds_resource_plugin_py_lib.common.resource.errors import NotSupportedError
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import AuthorizationError

from ds_provider_grasp_py_lib.enums import ResourceType
from tests.mocks import (
    create_mock_aws_linked_service,
    create_mock_file_dataset,
    create_test_dataframe,
)


class TestGraspFileDatasetType:
    """Tests for GraspFileDataset type property."""

    def test_type_returns_dataset_file(self) -> None:
        """It returns the correct ResourceType for file dataset."""
        dataset = create_mock_file_dataset()
        assert dataset.type == ResourceType.DATASET_FILE
        assert dataset.type == "ds.resource.dataset.grasp-file"


class TestGraspFileDatasetS3Path:
    """Tests for GraspFileDataset S3 path handling."""

    def test_get_s3_path_returns_settings_path(self) -> None:
        """It returns the configured S3 path."""
        dataset = create_mock_file_dataset(s3_path="s3://bucket/files/example.json")
        assert dataset._get_s3_path() == "s3://bucket/files/example.json"


class TestGraspFileDatasetRead:
    """Tests for GraspFileDataset read operation."""

    def test_read_raises_read_error_when_connection_not_set(self) -> None:
        """It raises ReadError when the linked service connection is missing."""
        linked_service = create_mock_aws_linked_service(with_connection=False)
        dataset = create_mock_file_dataset(linked_service=linked_service)
        with pytest.raises(ReadError) as exc_info:
            dataset.read()
        assert "Linked service connection is not established" in str(exc_info.value)
        assert exc_info.value.status_code == 503

    def test_read_raises_read_error_when_s3_path_not_set(self) -> None:
        """It raises ReadError when settings.s3_path is not configured."""
        dataset = create_mock_file_dataset(s3_path=None)
        with pytest.raises(ReadError) as exc_info:
            dataset.read()
        assert "settings.s3_path is required" in str(exc_info.value)
        assert exc_info.value.status_code == 400

    def test_read_raises_read_error_when_deserializer_not_set(self) -> None:
        """It raises ReadError when deserializer is not set."""
        dataset = create_mock_file_dataset()
        dataset.deserializer = None

        with pytest.raises(ReadError) as exc_info:
            dataset.read()
        assert "Deserializer is not set" in str(exc_info.value)
        assert exc_info.value.status_code == 400

    def test_read_raises_not_found_error_on_no_files_found(self) -> None:
        """It raises NotFoundError when no files are found at the configured path."""
        mock_deserializer = MagicMock(side_effect=NoFilesFound("No files"))
        dataset = create_mock_file_dataset(deserializer=mock_deserializer)

        with pytest.raises(NotFoundError) as exc_info:
            dataset.read()
        assert "No files found at S3 path" in str(exc_info.value)
        assert exc_info.value.status_code == 404

    def test_read_raises_read_error_on_generic_exception(self) -> None:
        """It raises ReadError when a generic exception occurs during read."""
        mock_deserializer = MagicMock(side_effect=RuntimeError("Connection timeout"))
        dataset = create_mock_file_dataset(deserializer=mock_deserializer)

        with pytest.raises(ReadError) as exc_info:
            dataset.read()
        assert "Failed to read data from S3 path" in str(exc_info.value)
        assert exc_info.value.status_code == 500

    def test_read_success_sets_output_and_operation(self) -> None:
        """It successfully reads data and records operation metadata."""
        test_df = create_test_dataframe(rows=3, with_valid_to=False)
        mock_deserializer = MagicMock(return_value=test_df)
        dataset = create_mock_file_dataset(deserializer=mock_deserializer)

        dataset.read()

        assert len(dataset.output) == 3
        assert dataset.operation.success is True
        assert dataset.operation.row_count == 3
        assert dataset.operation.error is None


class TestGraspFileDatasetCreate:
    """Tests for GraspFileDataset create operation."""

    def test_create_noops_when_input_is_none(self) -> None:
        """It returns immediately with empty output when input is None."""
        dataset = create_mock_file_dataset()
        dataset.input = None

        dataset.create()

        assert dataset.output.empty

    def test_create_raises_create_error_when_serializer_not_set(self) -> None:
        """It raises CreateError when serializer is not set."""
        dataset = create_mock_file_dataset()
        dataset.serializer = None
        dataset.input = create_test_dataframe(rows=1, with_valid_to=False)

        with pytest.raises(CreateError) as exc_info:
            dataset.create()
        assert "Serializer is not set" in str(exc_info.value)
        assert exc_info.value.status_code == 400

    def test_create_raises_create_error_when_connection_not_set(self) -> None:
        """It raises CreateError when the linked service connection is missing."""
        linked_service = create_mock_aws_linked_service(with_connection=False)
        dataset = create_mock_file_dataset(linked_service=linked_service)
        dataset.input = create_test_dataframe(rows=1, with_valid_to=False)

        with pytest.raises(CreateError) as exc_info:
            dataset.create()
        assert "Linked service connection is not established" in str(exc_info.value)
        assert exc_info.value.status_code == 503

    def test_create_raises_create_error_when_s3_path_not_set(self) -> None:
        """It raises CreateError when settings.s3_path is not configured."""
        dataset = create_mock_file_dataset(s3_path=None)
        dataset.input = create_test_dataframe(rows=2, with_valid_to=False)

        with pytest.raises(CreateError) as exc_info:
            dataset.create()
        assert "settings.s3_path is required" in str(exc_info.value)
        assert exc_info.value.status_code == 400

    def test_create_noops_on_empty_input_without_backend_call(self) -> None:
        """It returns without calling the backend when input is empty."""
        mock_serializer = MagicMock()
        dataset = create_mock_file_dataset(serializer=mock_serializer)
        dataset.input = pd.DataFrame()

        with patch("ds_provider_grasp_py_lib.dataset.file.wr.s3.list_objects") as mock_list_objects:
            dataset.create()

        mock_list_objects.assert_not_called()
        mock_serializer.assert_not_called()
        assert dataset.output.empty

    @patch("ds_provider_grasp_py_lib.dataset.file.wr.s3.list_objects")
    def test_create_raises_when_resource_already_exists(
        self,
        mock_list_objects: MagicMock,
    ) -> None:
        """It raises CreateError when the destination path already exists."""
        mock_list_objects.return_value = ["s3://test-bucket/path/data.json"]
        mock_serializer = MagicMock()
        dataset = create_mock_file_dataset(serializer=mock_serializer)
        dataset.input = create_test_dataframe(rows=2, with_valid_to=False)

        with pytest.raises(CreateError) as exc_info:
            dataset.create()

        assert "Resource already exists at S3 path" in str(exc_info.value)
        assert exc_info.value.status_code == 409
        mock_serializer.assert_not_called()

    @patch("ds_provider_grasp_py_lib.dataset.file.wr.s3.list_objects")
    def test_create_success_writes_copy_to_s3(
        self,
        mock_list_objects: MagicMock,
    ) -> None:
        """It writes a copy of the input DataFrame and keeps self.input unchanged."""
        mock_list_objects.return_value = []
        mock_serializer = MagicMock()
        dataset = create_mock_file_dataset(serializer=mock_serializer)
        dataset.input = create_test_dataframe(rows=2, with_valid_to=False)
        original_input = dataset.input.copy(deep=True)

        dataset.create()

        called_frame = mock_serializer.call_args.args[0]
        assert called_frame.equals(original_input)
        assert called_frame is not dataset.input
        assert dataset.output.equals(original_input)
        assert dataset.output is not dataset.input
        assert dataset.input.equals(original_input)
        assert dataset.operation.success is True
        assert dataset.operation.row_count == 2

    @patch("ds_provider_grasp_py_lib.dataset.file.wr.s3.list_objects")
    def test_create_raises_create_error_on_generic_exception_after_validation(
        self,
        mock_list_objects: MagicMock,
    ) -> None:
        """It wraps backend failures during create as CreateError."""
        mock_list_objects.return_value = []
        mock_serializer = MagicMock(side_effect=RuntimeError("write failed"))
        dataset = create_mock_file_dataset(serializer=mock_serializer)
        dataset.input = create_test_dataframe(rows=1, with_valid_to=False)

        with pytest.raises(CreateError) as exc_info:
            dataset.create()
        assert "Failed to create file dataset" in str(exc_info.value)
        assert exc_info.value.status_code == 500


class TestGraspFileDatasetList:
    """Tests for GraspFileDataset list operation."""

    def test_list_raises_authorization_error(self) -> None:
        """It raises AuthorizationError for list operation."""
        dataset = create_mock_file_dataset()
        with pytest.raises(AuthorizationError) as exc_info:
            dataset.list()
        assert "not authorized to list" in str(exc_info.value)
        assert exc_info.value.status_code == 403


class TestGraspFileDatasetPurge:
    """Tests for GraspFileDataset purge operation."""

    def test_purge_raises_authorization_error(self) -> None:
        """It raises AuthorizationError for purge operation."""
        dataset = create_mock_file_dataset()
        with pytest.raises(AuthorizationError) as exc_info:
            dataset.purge()
        assert "not authorized to purge" in str(exc_info.value)
        assert exc_info.value.status_code == 403


class TestGraspFileDatasetUnsupported:
    """Tests for unsupported GraspFileDataset operations."""

    def test_update_raises_authorization_error(self) -> None:
        """It raises AuthorizationError for update operation."""
        dataset = create_mock_file_dataset()
        with pytest.raises(AuthorizationError) as exc_info:
            dataset.update()
        assert "not authorized to update" in str(exc_info.value)
        assert exc_info.value.status_code == 403

    def test_upsert_raises_authorization_error(self) -> None:
        """It raises AuthorizationError for upsert operation."""
        dataset = create_mock_file_dataset()
        with pytest.raises(AuthorizationError) as exc_info:
            dataset.upsert()
        assert "not authorized to upsert" in str(exc_info.value)
        assert exc_info.value.status_code == 403

    def test_delete_raises_authorization_error(self) -> None:
        """It raises AuthorizationError for delete operation."""
        dataset = create_mock_file_dataset()
        with pytest.raises(AuthorizationError) as exc_info:
            dataset.delete()
        assert "not authorized to delete" in str(exc_info.value)
        assert exc_info.value.status_code == 403

    def test_rename_raises_not_supported_error(self) -> None:
        """It raises NotSupportedError for rename operation."""
        dataset = create_mock_file_dataset()
        with pytest.raises(NotSupportedError) as exc_info:
            dataset.rename()
        assert "does not support rename()" in str(exc_info.value)
        assert exc_info.value.status_code == 501


class TestGraspFileDatasetClose:
    """Tests for GraspFileDataset close operation."""

    def test_close_calls_linked_service_close(self) -> None:
        """It calls close on the linked service."""
        linked_service = create_mock_aws_linked_service()
        dataset = create_mock_file_dataset(linked_service=linked_service)
        dataset.close()
        assert linked_service._closed is True
