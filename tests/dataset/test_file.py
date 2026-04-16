"""Tests for the HTTP-based Grasp file dataset implementation."""

from __future__ import annotations

from io import BytesIO
from unittest.mock import MagicMock

import pandas as pd
import pytest
from ds_resource_plugin_py_lib.common.resource.dataset.errors import CreateError, ReadError
from ds_resource_plugin_py_lib.common.resource.errors import NotSupportedError, ResourceException
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import ConnectionError

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
        dataset = create_mock_file_dataset(linked_service=linked_service, deserializer=MagicMock())
        with pytest.raises(ConnectionError) as exc_info:
            dataset.read()
        assert "Session is not initialized" in str(exc_info.value)

    def test_read_raises_when_deserializer_is_not_set(self) -> None:
        """It requires a deserializer and suggests list() for raw/binary output."""
        dataset = create_mock_file_dataset(deserializer=None)

        with pytest.raises(ReadError, match=r"Deserializer must be set for read\(\)") as exc_info:
            dataset.read()

        assert exc_info.value.status_code == 400

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

    def test_read_ignores_download_file_flag_when_deserializer_is_set(self) -> None:
        """It always downloads/deserializes in read() even when download_file=False."""
        linked_service = create_mock_http_linked_service()
        linked_service.connection.request.side_effect = [
            MockHTTPResponse(json_data={"data": [{"id": "f1"}]}),
            MockHTTPResponse(content=b"one"),
        ]
        deserializer = MagicMock(return_value=pd.DataFrame([{"value": 1}]))
        dataset = create_mock_file_dataset(
            linked_service=linked_service,
            download_file=False,
            deserializer=deserializer,
        )

        dataset.read()

        assert list(dataset.output["value"]) == [1]
        assert linked_service.connection.request.call_count == 2

    def test_read_skips_deserializer_when_download_returns_404(self) -> None:
        """It keeps output empty and does not call deserializer when downloaded content is missing."""
        linked_service = create_mock_http_linked_service()
        linked_service.connection.request.side_effect = [
            MockHTTPResponse(json_data={"data": [{"id": "f1"}]}),
            ResourceException(message="missing", status_code=404),
        ]
        deserializer = MagicMock()
        dataset = create_mock_file_dataset(
            linked_service=linked_service,
            download_file=True,
            deserializer=deserializer,
        )

        dataset.read()

        assert dataset.output.empty
        deserializer.assert_not_called()
        assert linked_service.connection.request.call_count == 2

    def test_read_passes_optional_query_params(self) -> None:
        """It forwards optional read filters as query params on list request."""
        linked_service = create_mock_http_linked_service()
        linked_service.connection.request.side_effect = [MockHTTPResponse(json_data={"data": []})]
        dataset = create_mock_file_dataset(
            linked_service=linked_service,
            download_file=False,
            deserializer=MagicMock(),
        )

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

    def test_read_wraps_deserialization_failures_with_read_error(self) -> None:
        """It wraps deserializer failures in ReadError with status/details."""
        linked_service = create_mock_http_linked_service()
        linked_service.connection.request.side_effect = [
            MockHTTPResponse(json_data={"data": [{"id": "f1"}]}),
            MockHTTPResponse(content=b"invalid"),
        ]
        deserializer = MagicMock(side_effect=ValueError("bad payload"))
        dataset = create_mock_file_dataset(
            linked_service=linked_service,
            download_file=True,
            deserializer=deserializer,
        )

        with pytest.raises(ReadError) as exc_info:
            dataset.read()

        assert exc_info.value.status_code == 500
        assert "Failed to deserialize content" in str(exc_info.value)


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

    def test_create_uses_settings_content_when_both_input_and_content_are_provided(self) -> None:
        """It prioritizes settings.create.content over dataset.input when both are set."""
        serializer = MagicMock(return_value=b"serialized")
        dataset = create_mock_file_dataset(serializer=serializer)
        dataset.input = create_test_dataframe(rows=1, with_valid_to=False)
        dataset.settings.create.content = BytesIO(b"binary-content")
        dataset._create_metadata = MagicMock(return_value={"id": "f1"})  # type: ignore[method-assign]
        dataset._upload_file_content = MagicMock(return_value=[{"id": "f1"}])  # type: ignore[method-assign]

        dataset.create()

        serializer.assert_not_called()
        dataset._upload_file_content.assert_called_once_with({"id": "f1"}, dataset.settings.create.content)

    def test_create_raises_when_no_content_source_is_provided(self) -> None:
        """It raises CreateError when neither create.content nor dataset.input is provided."""
        dataset = create_mock_file_dataset()
        dataset.input = None

        with pytest.raises(CreateError, match=r"No create payload provided") as exc_info:
            dataset.create()

        assert exc_info.value.status_code == 400
        assert exc_info.value.details["type"] == ResourceType.DATASET_FILE.value
        assert exc_info.value.details["settings"] == dataset.settings.serialize()

    def test_create_raises_when_input_set_but_serializer_missing(self) -> None:
        """It raises CreateError when dataset.input is provided without a serializer."""
        dataset = create_mock_file_dataset()
        dataset.input = create_test_dataframe(rows=1, with_valid_to=False)

        with pytest.raises(CreateError, match=r"serializer must be set") as exc_info:
            dataset.create()

        assert exc_info.value.status_code == 400
        assert exc_info.value.details["type"] == ResourceType.DATASET_FILE.value
        assert exc_info.value.details["settings"] == dataset.settings.serialize()

    def test_create_raises_when_metadata_creation_fails(self) -> None:
        """It propagates errors from metadata creation."""
        dataset = create_mock_file_dataset()
        dataset.input = create_test_dataframe(rows=1, with_valid_to=False)
        dataset._create_metadata = MagicMock(side_effect=CreateError("boom"))  # type: ignore[method-assign]

        with pytest.raises(CreateError):
            dataset.create()

    def test_create_uses_serializer_output_for_empty_dataframe_input(self) -> None:
        """It treats an empty DataFrame as valid input and serializes/uploads it."""
        serializer = MagicMock(return_value=b"[]")
        dataset = create_mock_file_dataset(serializer=serializer)
        dataset.input = pd.DataFrame()
        dataset._create_metadata = MagicMock(return_value={"id": "f1"})  # type: ignore[method-assign]
        dataset._upload_file_content = MagicMock(return_value=[{"id": "f1"}])  # type: ignore[method-assign]

        dataset.create()

        serializer.assert_called_once_with(dataset.input)
        dataset._upload_file_content.assert_called_once_with({"id": "f1"}, b"[]")


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

    def test_upload_file_content_does_not_mutate_linked_service_headers(self) -> None:
        """It keeps linked service headers immutable when adding upload content headers."""
        linked_service = create_mock_http_linked_service(headers={"Authorization": "Bearer token"})
        linked_service.connection.request.return_value = MockHTTPResponse(json_data={"ok": True})
        dataset = create_mock_file_dataset(linked_service=linked_service)

        dataset._upload_file_content({"id": "file-1"}, BytesIO(b"x"))

        assert linked_service.settings.headers == {"Authorization": "Bearer token"}


class TestGraspFileDatasetList:
    """Tests for GraspFileDataset list operation."""

    def test_list_returns_files_without_content_when_download_disabled(self) -> None:
        """It returns file metadata only when download_file=False."""
        linked_service = create_mock_http_linked_service()
        linked_service.connection.request.return_value = MockHTTPResponse(
            json_data={"data": [{"id": "f1", "name": "file-1"}]},
        )
        dataset = create_mock_file_dataset(linked_service=linked_service, download_file=False)

        dataset.list()

        assert len(dataset.output) == 1
        assert dataset.output.iloc[0]["id"] == "f1"
        assert "content" not in dataset.output.columns
        assert dataset.operation.success is True
        assert dataset.operation.row_count == 1

    def test_list_downloads_file_content_when_enabled(self) -> None:
        """It fetches file content for every discovered file when download_file=True."""
        linked_service = create_mock_http_linked_service()
        linked_service.connection.request.side_effect = [
            MockHTTPResponse(json_data={"data": [{"id": "f1"}, {"id": "f2"}]}),
            MockHTTPResponse(content=b"hello"),
            MockHTTPResponse(content=b"world"),
        ]
        dataset = create_mock_file_dataset(linked_service=linked_service, download_file=True)

        dataset.list()

        assert list(dataset.output["id"]) == ["f1", "f2"]
        assert list(dataset.output["content"]) == [b"hello", b"world"]

    def test_list_uses_empty_content_when_file_download_returns_404(self) -> None:
        """It sets empty content for files that return a 404 during content download."""
        linked_service = create_mock_http_linked_service()
        linked_service.connection.request.side_effect = [
            MockHTTPResponse(json_data={"data": [{"id": "f1"}]}),
            ResourceException(message="missing", status_code=404),
        ]
        dataset = create_mock_file_dataset(linked_service=linked_service, download_file=True)

        dataset.list()

        assert dataset.output.iloc[0]["content"] == b""

    def test_list_passes_optional_query_params(self) -> None:
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

        dataset.list()

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


class TestGraspFileDatasetPurge:
    """Tests for GraspFileDataset purge operation."""

    def test_purge_raises_not_supported_error(self) -> None:
        """It raises NotSupportedError for purge operation."""
        dataset = create_mock_file_dataset()
        with pytest.raises(NotSupportedError) as exc_info:
            dataset.purge()
        assert "Method 'purge' is not supported by this provider." in str(exc_info.value)
        assert exc_info.value.status_code == 501
        assert exc_info.value.details == {
            "method": "purge",
            "provider": ResourceType.DATASET_FILE.value,
        }


class TestGraspFileDatasetUnsupported:
    """Tests for unsupported GraspFileDataset operations."""

    def test_update_raises_not_supported_error(self) -> None:
        """It raises NotSupportedError for update operation."""
        dataset = create_mock_file_dataset()
        with pytest.raises(NotSupportedError) as exc_info:
            dataset.update()
        assert "Method 'update' is not supported by this provider." in str(exc_info.value)
        assert exc_info.value.status_code == 501
        assert exc_info.value.details == {
            "method": "update",
            "provider": ResourceType.DATASET_FILE.value,
        }

    def test_upsert_raises_not_supported_error(self) -> None:
        """It raises NotSupportedError for upsert operation."""
        dataset = create_mock_file_dataset()
        with pytest.raises(NotSupportedError) as exc_info:
            dataset.upsert()
        assert "Method 'upsert' is not supported by this provider." in str(exc_info.value)
        assert exc_info.value.status_code == 501
        assert exc_info.value.details == {
            "method": "upsert",
            "provider": ResourceType.DATASET_FILE.value,
        }

    def test_delete_raises_not_supported_error(self) -> None:
        """It raises NotSupportedError for delete operation."""
        dataset = create_mock_file_dataset()
        with pytest.raises(NotSupportedError) as exc_info:
            dataset.delete()
        assert "Method 'delete' is not supported by this provider." in str(exc_info.value)
        assert exc_info.value.status_code == 501
        assert exc_info.value.details == {
            "method": "delete",
            "provider": ResourceType.DATASET_FILE.value,
        }

    def test_rename_raises_not_supported_error(self) -> None:
        """It raises NotSupportedError for rename operation."""
        dataset = create_mock_file_dataset()
        with pytest.raises(NotSupportedError) as exc_info:
            dataset.rename()
        assert "Method 'rename' is not supported by this provider." in str(exc_info.value)
        assert exc_info.value.status_code == 501
        assert exc_info.value.details == {
            "method": "rename",
            "provider": ResourceType.DATASET_FILE.value,
        }


class TestGraspFileDatasetClose:
    """Tests for GraspFileDataset close operation."""

    def test_close_calls_linked_service_close(self) -> None:
        """It calls close on the linked service."""
        linked_service = create_mock_http_linked_service()
        dataset = create_mock_file_dataset(linked_service=linked_service)
        dataset.close()
        assert linked_service._closed is True
