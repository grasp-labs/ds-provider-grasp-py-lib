"""
**File:** ``file.py``
**Region:** ``ds_provider_grasp_py_lib/dataset/file``

Grasp File Dataset

This module implements a generic GRASP file dataset.
The dataset can read and create files via grasp API.
"""

import io
from dataclasses import dataclass, field
from typing import Any, Generic, NoReturn, TypeVar

import pandas as pd
from ds_common_logger_py_lib import Logger
from ds_protocol_http_py_lib.linked_service.http import HttpLinkedService
from ds_resource_plugin_py_lib.common.resource.dataset import (
    DatasetSettings,
    TabularDataset,
)
from ds_resource_plugin_py_lib.common.resource.dataset.errors import CreateError, ReadError
from ds_resource_plugin_py_lib.common.resource.errors import NotSupportedError, ResourceException
from ds_resource_plugin_py_lib.common.serde.deserialize import PandasDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import PandasSerializer

from ..enums import ResourceType

logger = Logger.get_logger(__name__, package=True)


@dataclass(kw_only=True)
class CreateSettings:
    """
    Settings for create operations.

    Content source contract:
    - `create.content` has precedence when provided.
    - Otherwise create uses `dataset.input` serialized by `dataset.serializer`.
    - If neither source is available, create raises `CreateError`.
    """

    acl: dict[str, Any] | None = field(default_factory=dict)
    description: str | None = None
    file_path: str | None = None
    metadata: dict[str, Any] | None = field(default_factory=dict)
    status: str | None = field(default="active")
    tags: dict[str, Any] | None = field(default_factory=dict)
    version: str | None = field(default="1.0.0")

    content: bytes | bytearray | io.BytesIO | None = field(default=None)
    """Raw upload payload. When set, it overrides `dataset.input` as create source."""


@dataclass(kw_only=True)
class ReadSettings:
    """
    Settings for read operation.
    """

    limit: int = 500
    offset: int = 0
    order_by: str | None = None
    tags: dict[str, str] | None = field(default_factory=dict)
    meta: dict[str, str] | None = field(default_factory=dict)
    id: str | None = None
    file_path: str | None = None
    created_at_gte: str | None = None
    modified_at_gte: str | None = None
    created_at_lte: str | None = None
    modified_at_lte: str | None = None
    status: str | None = None


@dataclass(kw_only=True)
class ListSettings:
    """Settings for list operation"""

    download_file: bool = True
    """When True, list() includes raw binary content in output rows."""


@dataclass(kw_only=True)
class GraspFileDatasetSettings(DatasetSettings):
    """Settings for Grasp file dataset create/read behavior and API base URL."""

    url: str | None = field(default="https://grasp-daas.com/api/file-dev/v2/file/")
    create: CreateSettings = field(default_factory=CreateSettings)
    read: ReadSettings = field(default_factory=ReadSettings)
    list: ListSettings = field(default_factory=ListSettings)


GraspFileDatasetSettingsType = TypeVar(
    "GraspFileDatasetSettingsType",
    bound=GraspFileDatasetSettings,
)
HttpLinkedServiceType = TypeVar(
    "HttpLinkedServiceType",
    bound=HttpLinkedService[Any],
)


@dataclass(kw_only=True)
class GraspFileDataset(
    TabularDataset[
        HttpLinkedServiceType,
        GraspFileDatasetSettingsType,
        PandasSerializer,
        PandasDeserializer,
    ],
    Generic[HttpLinkedServiceType, GraspFileDatasetSettingsType],
):
    linked_service: HttpLinkedServiceType
    settings: GraspFileDatasetSettingsType

    @property
    def type(self) -> ResourceType:
        return ResourceType.DATASET_FILE

    def _details(self, **extra: Any) -> dict[str, Any]:
        return {
            "type": self.type.value,
            "settings": self.settings.serialize(),
            **extra,
        }

    def _base_url(self) -> str:
        if not self.settings.url:
            raise ValueError("File dataset settings.url is required")
        return self.settings.url.rstrip("/") + "/"

    def _read_params(self) -> dict[str, Any]:
        """Build query parameters for file listing from read settings."""
        read = self.settings.read
        params: dict[str, Any] = {}

        for key in (
            "limit",
            "offset",
            "order_by",
            "id",
            "file_path",
            "created_at_gte",
            "modified_at_gte",
            "created_at_lte",
            "modified_at_lte",
            "status",
        ):
            value = getattr(read, key)
            if value is not None:
                params[key] = value

        for key, value in (read.tags or {}).items():
            params[f"tag.{key}"] = value

        for key, value in (read.meta or {}).items():
            params[f"meta.{key}"] = value

        return params

    def create(self) -> None:
        """
        Write the content of the dataset to the file.
        :return: None
        """
        content = self._resolve_create_content()
        metadata = self._create_metadata()
        data = self._upload_file_content(metadata, content)
        self.output = pd.DataFrame(data)

    def read(self) -> None:
        """
        Read the file from the file_uri.
        :return: None
        """
        base_url = self._base_url()
        logger.debug(f"Reading files from {base_url}")

        if self.deserializer is None:
            raise ReadError(
                message="Deserializer must be set for read(). Use list() for raw/binary output.",
                status_code=400,
                details={
                    "type": self.type.value,
                    "settings": self.settings.serialize(),
                },
            )

        response = self.linked_service.connection.request(
            method="GET",
            url=base_url,
            headers=self.linked_service.settings.headers,
            params=self._read_params(),
        )

        files = response.json()["data"]

        self._download_files(base_url, files, deserialize=True)

    def _download_files(self, base_url: str, files: list[dict[str, Any]], deserialize: bool) -> None:
        for file in files:
            file_id = file["id"]
            url = f"{base_url}{file_id}/content/"
            try:
                response = self.linked_service.connection.request(
                    method="GET",
                    url=url,
                    headers=self.linked_service.settings.headers,
                )
                file.update({"content": response.content})
            except ResourceException as exc:
                if exc.status_code == 404:
                    file.update({"content": b""})
                    continue
                raise

        if deserialize:
            deserialized_frames: list[pd.DataFrame] = []
            for file in files:
                content = file.get("content", b"")
                if not content:
                    continue
                deserialized_frames.append(self._deserialize_content(content))
            self.output = pd.concat(deserialized_frames, ignore_index=True) if deserialized_frames else pd.DataFrame()
        else:
            self.output = pd.DataFrame(files)

    def update(self) -> NoReturn:
        logger.error("Update operation is not supported by Grasp file dataset.")
        raise NotSupportedError(
            message="Method 'update' is not supported by this provider.",
            details={"method": "update", "provider": self.type.value},
        )

    def upsert(self) -> NoReturn:
        logger.error("Upsert operation is not supported by Grasp file dataset.")
        raise NotSupportedError(
            message="Method 'upsert' is not supported by this provider.",
            details={"method": "upsert", "provider": self.type.value},
        )

    def delete(self) -> NoReturn:
        logger.error("Delete operation is not supported by Grasp file dataset.")
        raise NotSupportedError(
            message="Method 'delete' is not supported by this provider.",
            details={"method": "delete", "provider": self.type.value},
        )

    def purge(self) -> NoReturn:
        logger.error("Purge operation is not supported by Grasp file dataset.")
        raise NotSupportedError(
            message="Method 'purge' is not supported by this provider.",
            details={"method": "purge", "provider": self.type.value},
        )

    def list(self) -> None:
        """List files with metadata only or metadata+binary content based on list settings."""
        base_url = self._base_url()
        logger.debug(f"Listing files from {base_url}")

        response = self.linked_service.connection.request(
            method="GET",
            url=base_url,
            headers=self.linked_service.settings.headers,
            params=self._read_params(),
        )

        files = response.json()["data"]
        if self.settings.list.download_file:
            self._download_files(base_url, files, deserialize=False)
        else:
            self.output = pd.DataFrame(files)

    def rename(self) -> NoReturn:
        logger.error("Rename operation is not supported by Grasp file dataset.")
        raise NotSupportedError(
            message="Method 'rename' is not supported by this provider.",
            details={"method": "rename", "provider": self.type.value},
        )

    def close(self) -> None:
        """Close the dataset."""
        self.linked_service.close()

    def _create_metadata(self) -> dict[str, Any]:
        """
        Create the metadata for the file.
        :return: Dict
        """
        json: dict[str, Any] = {
            "acl": self.settings.create.acl,
            "description": self.settings.create.description,
            "file_path": self.settings.create.file_path,
            "metadata": self.settings.create.metadata,
            "status": self.settings.create.status,
            "tags": self.settings.create.tags,
            "version": self.settings.create.version,
        }
        base_url = self._base_url()
        logger.info(f"Creating file metadata: {json}")
        response = self.linked_service.connection.request(
            method="POST",
            url=base_url,
            headers=self.linked_service.settings.headers,
            json=json,
        )
        data: dict[str, Any] = response.json()
        logger.info(f"File metadata created: {data}")
        return data

    def _upload_file_content(self, metadata: dict[str, Any], content: Any) -> dict[str, Any]:
        """
        Upload the file content to the file.
        :return: Dict
        """
        headers = {
            **self.linked_service.settings.headers,
            "Content-Type": "application/octet-stream",
            "accept": "*/*",
        }
        base_url = self._base_url()
        url = f"{base_url}{metadata['id']}/content/"

        response = self.linked_service.connection.request(
            method="PUT",
            url=url,
            headers=headers,
            data=content,
        )
        data: dict[str, Any] = response.json()
        return data

    def _resolve_create_content(self) -> Any:
        """Resolve payload from `create.content` first, then from serialized `dataset.input`."""
        if self.settings.create.content is not None:
            # Explicit binary content is the highest-priority source.
            return self.settings.create.content

        has_input = isinstance(self.input, pd.DataFrame)
        if not has_input:
            raise CreateError(
                message="No create payload provided. Set settings.create.content or dataset.input.",
                status_code=400,
                details={
                    "type": self.type.value,
                    "settings": self.settings.serialize(),
                },
            )

        if self.serializer is None:
            raise CreateError(
                message="serializer must be set when dataset.input is provided",
                status_code=400,
                details={
                    "type": self.type.value,
                    "settings": self.settings.serialize(),
                },
            )

        serialized = self.serializer(self.input)
        if isinstance(serialized, str):
            return serialized.encode()
        if isinstance(serialized, bytearray):
            return bytes(serialized)
        if hasattr(serialized, "getvalue"):
            value = serialized.getvalue()
            return value.encode() if isinstance(value, str) else value
        return serialized


    def _deserialize_content(self, content: bytes) -> pd.DataFrame:
        """Deserialize raw file bytes into a DataFrame using configured deserializer."""
        deserializer = self.deserializer
        if deserializer is None:
            raise ReadError(
                message="Deserializer is not set.",
                status_code=400,
                details={
                    "type": self.type.value,
                    "settings": self.settings.serialize(),
                },
            )

        try:
            return deserializer(content)
        except Exception:
            try:
                return deserializer(io.BytesIO(content))
            except Exception as exc:
                raise ReadError(
                    message=f"Failed to deserialize content: {exc!s}",
                    status_code=500,
                    details={
                        "type": self.type.value,
                        "settings": self.settings.serialize(),
                    },
                ) from exc
