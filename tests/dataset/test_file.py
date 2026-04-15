"""Tests for the HTTP-based Grasp file dataset implementation."""

from __future__ import annotations

from io import BytesIO
from unittest.mock import MagicMock

import pandas as pd
import pytest
from ds_resource_plugin_py_lib.common.resource.dataset.errors import ReadError
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

    def test_read_raises_connection_error_when_connection_not_set(self) -> None:
        """It raises ConnectionError when linked_service.connection is None."""
        linked_service = create_mock_http_linked_service(with_connection=False)
        dataset = create_mock_file_dataset(linked_service=linked_service)
        with pytest.raises(ConnectionError) as exc_info:
            dataset.read()
        assert "Session is not initialized" in str(exc_info.value)

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

    def test_read_deserializes_downloaded_content_when_deserializer_is_set(self) -> None:
        """It downloads file content and deserializes it into output DataFrame."""
        linked_service = create_mock_http_linked_service()
        linked_service.connection.request.side_effect = [
            MockHTTPResponse(json_data={"data": [{"id": "f1"}, {"id": "f2"}]}),
            MockHTTPResponse(content=b"first"),
            MockHTTPResponse(content=b"second"),
        ]
        deserializer = MagicMock(
            side_effect=[
                pd.DataFrame([{"value": 1}]),
                pd.DataFrame([{"value": 2}]),
            ]
        )
        dataset = create_mock_file_dataset(
            linked_service=linked_service,
            download_file=True,
            deserializer=deserializer,
        )

        dataset.read()

        assert list(dataset.output["value"]) == [1, 2]
        assert deserializer.call_count == 2
        assert linked_service.connection.request.call_count == 3

    def test_read_raises_when_deserializer_set_and_download_is_disabled(self) -> None:
        """It enforces download_file=True when a deserializer is configured."""
        linked_service = create_mock_http_linked_service()
        dataset = create_mock_file_dataset(
            linked_service=linked_service,
            download_file=False,
            deserializer=MagicMock(),
        )

        with pytest.raises(ReadError, match=r"download_file must be true"):
            dataset.read()

        linked_service.connection.request.assert_not_called()

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

    def test_read_passes_optional_query_params(self) -> None:
        """It forwards optional read filters as query params on list request."""
        linked_service = create_mock_http_linked_service()
        linked_service.connection.request.return_value = MockHTTPResponse(json_data={"data": []})
        dataset = create_mock_file_dataset(linked_service=linked_service, download_file=False)

        dataset.settings.read.limit = 10
        dataset.settings.read.offset = 5
        dataset.settings.read.order_by = "-modified_at"
        dataset.settings.read.id = "file-123"
        dataset.settings.read.file_path = "folder/a.txt"
        dataset.settings.read.created_at_gte = "2024-01-01T00:00:00Z"
        dataset.settings.read.modified_at_lte = "2024-12-31T23:59:59Z"
        dataset.settings.read.status = "active"
        dataset.settings.read.tags = {"type": "png"}
        dataset.settings.read.meta = {"category": "data"}

        dataset.read()

        request_kwargs = linked_service.connection.request.call_args.kwargs
        assert request_kwargs["params"] == {
            "limit": 10,
            "offset": 5,
            "order_by": "-modified_at",
            "id": "file-123",
            "file_path": "folder/a.txt",
            "created_at_gte": "2024-01-01T00:00:00Z",
            "modified_at_lte": "2024-12-31T23:59:59Z",
            "status": "active",
            "tag.type": "png",
            "meta.category": "data",
        }


class TestGraspFileDatasetCreate:
    """Tests for GraspFileDataset create operation."""

    def test_create_writes_metadata_and_content_and_sets_output(self) -> None:
        """It delegates to metadata/content helpers and stores output as a DataFrame."""
        dataset = create_mock_file_dataset()
        dataset.input = create_test_dataframe(rows=1, with_valid_to=False)
        dataset._create_metadata = MagicMock(return_value={"id": "f1"})  # type: ignore[method-assign]
        dataset._resolve_create_content = MagicMock(return_value=b"payload")  # type: ignore[method-assign]
        dataset._upload_file_content = MagicMock(return_value=[{"id": "f1", "status": "ok"}])  # type: ignore[method-assign]

        dataset.create()

        assert dataset._create_metadata.called
        dataset._resolve_create_content.assert_called_once_with()
        dataset._upload_file_content.assert_called_once_with({"id": "f1"}, b"payload")
        assert dataset.output.iloc[0]["id"] == "f1"

    def test_create_uses_serializer_output_when_serializer_is_set(self) -> None:
        """It serializes dataset.input and uploads the serialized payload."""
        serializer = MagicMock(return_value=b"serialized")
        dataset = create_mock_file_dataset(serializer=serializer)
        dataset.input = create_test_dataframe(rows=1, with_valid_to=False)
        dataset._create_metadata = MagicMock(return_value={"id": "f1"})  # type: ignore[method-assign]
        dataset._upload_file_content = MagicMock(return_value=[{"id": "f1"}])  # type: ignore[method-assign]

        dataset.create()

        serializer.assert_called_once_with(dataset.input)
        dataset._upload_file_content.assert_called_once_with({"id": "f1"}, b"serialized")

    def test_create_falls_back_to_settings_content_when_input_is_missing(self) -> None:
        """It uses settings.create.content when serializer is set but dataset.input is None."""
        serializer = MagicMock(return_value=b"serialized")
        dataset = create_mock_file_dataset(serializer=serializer)
        content = BytesIO(b"binary-content")
        dataset.settings.create.content = content
        dataset._create_metadata = MagicMock(return_value={"id": "f1"})  # type: ignore[method-assign]
        dataset._upload_file_content = MagicMock(return_value=[{"id": "f1"}])  # type: ignore[method-assign]

        dataset.create()

        serializer.assert_not_called()
        dataset._upload_file_content.assert_called_once_with({"id": "f1"}, content)

    def test_create_raises_when_input_set_but_serializer_missing(self) -> None:
        """It raises CreateError when dataset.input is provided without a serializer."""
        dataset = create_mock_file_dataset()  # no serializer
        dataset.input = create_test_dataframe(rows=1, with_valid_to=False)
        dataset._create_metadata = MagicMock(return_value={"id": "f1"})  # type: ignore[method-assign]

        with pytest.raises(CreateError, match=r"serializer must be set"):
            dataset.create()

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

    def test_base_url_raises_when_settings_url_is_missing(self) -> None:
        """It raises ValueError when settings.url is not configured."""
        dataset = create_mock_file_dataset()
        dataset.settings.url = None

        with pytest.raises(ValueError, match=r"File dataset settings\.url is required"):
            dataset._base_url()

    def test_create_metadata_posts_payload_and_cleans_temp_fields(self) -> None:
        """It posts metadata to API using nested create settings."""
        linked_service = create_mock_http_linked_service(headers={"Authorization": "Bearer token"})
        linked_service.connection.request.return_value = MockHTTPResponse(json_data={"id": "file-1"})
        dataset = create_mock_file_dataset(linked_service=linked_service)
        dataset.settings.create.description = "file desc"
        dataset.settings.create.file_path = "folder/test"

        response_payload = dataset._create_metadata()

        assert response_payload == {"id": "file-1"}
        linked_service.connection.request.assert_called_once()
        request_kwargs = linked_service.connection.request.call_args.kwargs
        assert request_kwargs["method"] == "POST"
        assert request_kwargs["url"] == "https://grasp.example/api/file/"
        assert request_kwargs["json"]["description"] == "file desc"
        assert request_kwargs["json"]["file_path"] == "folder/test"

    def test_upload_file_content_puts_json_bytes_and_returns_response(self) -> None:
        """It uploads provided content and returns API response payload."""
        linked_service = create_mock_http_linked_service(headers={"Authorization": "Bearer token"})
        linked_service.connection.request.return_value = MockHTTPResponse(json_data={"ok": True})
        dataset = create_mock_file_dataset(linked_service=linked_service)
        content = BytesIO(b'{"test":"4"}')

        response_payload = dataset._upload_file_content({"id": "file-1"}, content)

        assert response_payload == {"ok": True}
        linked_service.connection.request.assert_called_once()
        request_kwargs = linked_service.connection.request.call_args.kwargs
        assert request_kwargs["method"] == "PUT"
        assert request_kwargs["url"] == "https://grasp.example/api/file/file-1/content/"
        assert request_kwargs["data"] is content
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
