"""Tests for the HTTP-based Grasp file dataset implementation."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from ds_resource_plugin_py_lib.common.resource.dataset.errors import CreateError
from ds_resource_plugin_py_lib.common.resource.errors import ResourceException
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import AuthorizationError, ConnectionError

from ds_provider_grasp_py_lib.enums import ResourceType
from tests.mocks import (
    MockHTTPResponse,
    create_mock_file_dataset,
    create_mock_http_linked_service,
    create_test_dataframe,
)


class TestGraspFileDatasetType:
    """Tests for GraspFileDataset type property."""

    def test_type_returns_dataset_file(self) -> None:
        """It returns the correct ResourceType for file dataset."""
        dataset = create_mock_file_dataset()
        assert dataset.type == ResourceType.DATASET_FILE
        assert dataset.type == "ds.resource.dataset.grasp-file"


class TestGraspFileDatasetRead:
    """Tests for GraspFileDataset read operation."""

    def test_read_raises_attribute_error_when_connection_not_set(self) -> None:
        """It currently fails with AttributeError when linked_service.connection is None."""
        linked_service = create_mock_http_linked_service(with_connection=False)
        dataset = create_mock_file_dataset(linked_service=linked_service)
        with pytest.raises(AttributeError):
            dataset.read()

    def test_read_returns_files_without_content_when_download_disabled(self) -> None:
        """It returns file metadata only when download_file=False."""
        linked_service = create_mock_http_linked_service()
        linked_service.connection.request.return_value = MockHTTPResponse(
            json_data={"data": [{"id": "f1", "name": "file-1"}]},
        )
        dataset = create_mock_file_dataset(linked_service=linked_service, download_file=False)

        dataset.read()

        assert len(dataset.output) == 1
        assert dataset.output.iloc[0]["id"] == "f1"
        assert "content" not in dataset.output.columns
        assert dataset.operation.success is True
        assert dataset.operation.row_count == 1

    def test_read_downloads_file_content_when_enabled(self) -> None:
        """It fetches file content for every discovered file when download_file=True."""
        linked_service = create_mock_http_linked_service()
        linked_service.connection.request.side_effect = [
            MockHTTPResponse(json_data={"data": [{"id": "f1"}, {"id": "f2"}]}),
            MockHTTPResponse(content=b"hello"),
            MockHTTPResponse(content=b"world"),
        ]
        dataset = create_mock_file_dataset(linked_service=linked_service, download_file=True)

        dataset.read()

        assert list(dataset.output["id"]) == ["f1", "f2"]
        assert list(dataset.output["content"]) == [b"hello", b"world"]

    def test_read_uses_empty_content_when_file_download_returns_404(self) -> None:
        """It sets empty content for files that return a 404 during content download."""
        linked_service = create_mock_http_linked_service()
        linked_service.connection.request.side_effect = [
            MockHTTPResponse(json_data={"data": [{"id": "f1"}]}),
            ResourceException(message="missing", status_code=404),
        ]
        dataset = create_mock_file_dataset(linked_service=linked_service, download_file=True)

        dataset.read()

        assert dataset.output.iloc[0]["content"] == b""


class TestGraspFileDatasetCreate:
    """Tests for GraspFileDataset create operation."""

    def test_create_writes_metadata_and_content_and_sets_output(self) -> None:
        """It delegates to metadata/content helpers and stores output as a DataFrame."""
        dataset = create_mock_file_dataset()
        dataset.input = create_test_dataframe(rows=1, with_valid_to=False)
        dataset._create_metadata = MagicMock(return_value={"id": "f1"})  # type: ignore[method-assign]
        dataset._upload_file_content = MagicMock(return_value=[{"id": "f1", "status": "ok"}])  # type: ignore[method-assign]

        dataset.create()

        assert dataset._create_metadata.called
        dataset._upload_file_content.assert_called_once_with({"id": "f1"})
        assert dataset.output.iloc[0]["id"] == "f1"

    def test_create_raises_when_metadata_creation_fails(self) -> None:
        """It propagates errors from metadata creation."""
        dataset = create_mock_file_dataset()
        dataset.input = create_test_dataframe(rows=1, with_valid_to=False)
        dataset._create_metadata = MagicMock(side_effect=CreateError("boom"))  # type: ignore[method-assign]

        with pytest.raises(CreateError):
            dataset.create()


class TestGraspFileDatasetInternals:
    """Tests for internal helper methods used by GraspFileDataset."""

    def test_details_contains_type_settings_and_extra(self) -> None:
        """It returns details with dataset type, serialized settings, and extra keys."""
        dataset = create_mock_file_dataset()
        details = dataset._details(operation="read")
        assert details["type"] == ResourceType.DATASET_FILE.value
        assert "settings" in details
        assert details["operation"] == "read"

    def test_get_connection_raises_when_missing(self) -> None:
        """It raises ConnectionError when linked service connection is not established."""
        linked_service = create_mock_http_linked_service(with_connection=False)
        dataset = create_mock_file_dataset(linked_service=linked_service)
        with pytest.raises(ConnectionError) as exc_info:
            dataset._get_connection()
        assert "not established" in str(exc_info.value)
        assert exc_info.value.status_code == 503

    def test_create_metadata_posts_payload_and_cleans_temp_fields(self) -> None:
        """It posts metadata to API and resets temporary settings attributes."""
        linked_service = create_mock_http_linked_service(headers={"Authorization": "Bearer token"})
        linked_service.connection.request.return_value = MockHTTPResponse(json_data={"id": "file-1"})
        dataset = create_mock_file_dataset(linked_service=linked_service)
        dataset.settings.description = "file desc"
        dataset.settings.file_path = "folder/test"

        response_payload = dataset._create_metadata()

        assert response_payload == {"id": "file-1"}
        linked_service.connection.request.assert_called_once()
        request_kwargs = linked_service.connection.request.call_args.kwargs
        assert request_kwargs["method"] == "POST"
        assert request_kwargs["url"] == "https://grasp.example/api/file/"
        assert request_kwargs["json"]["description"] == "file desc"
        assert request_kwargs["json"]["file_path"] == "folder/test"
        assert hasattr(dataset.settings, "json") and dataset.settings.json is None
        assert hasattr(dataset.settings, "headers") and dataset.settings.headers is None

    def test_upload_file_content_puts_json_bytes_and_returns_response(self) -> None:
        """It uploads JSON-encoded input content and returns API response payload."""
        linked_service = create_mock_http_linked_service(headers={"Authorization": "Bearer token"})
        linked_service.connection.request.return_value = MockHTTPResponse(json_data={"ok": True})
        dataset = create_mock_file_dataset(linked_service=linked_service)
        dataset.input = create_test_dataframe(rows=1, with_valid_to=False)

        response_payload = dataset._upload_file_content({"id": "file-1"})

        assert response_payload == {"ok": True}
        linked_service.connection.request.assert_called_once()
        request_kwargs = linked_service.connection.request.call_args.kwargs
        assert request_kwargs["method"] == "PUT"
        assert request_kwargs["url"] == "https://grasp.example/api/file/file-1/content/"
        assert isinstance(request_kwargs["data"], bytes)
        assert request_kwargs["headers"]["Content-Type"] == "application/octet-stream"
        assert request_kwargs["headers"]["accept"] == "*/*"


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

    def test_rename_raises_authorization_error(self) -> None:
        """It raises AuthorizationError for rename operation."""
        dataset = create_mock_file_dataset()
        with pytest.raises(AuthorizationError) as exc_info:
            dataset.rename()
        assert "not authorized to rename" in str(exc_info.value)
        assert exc_info.value.status_code == 403


class TestGraspFileDatasetClose:
    """Tests for GraspFileDataset close operation."""

    def test_close_calls_linked_service_close(self) -> None:
        """It calls close on the linked service."""
        linked_service = create_mock_http_linked_service()
        dataset = create_mock_file_dataset(linked_service=linked_service)
        dataset.close()
        assert linked_service._closed is True
