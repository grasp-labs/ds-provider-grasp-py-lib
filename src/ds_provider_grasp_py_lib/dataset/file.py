"""
**File:** ``file.py``
**Region:** ``ds_provider_grasp_py_lib/dataset/file``

Grasp File Dataset

This module implements a generic GRASP file dataset.
The dataset can read and create files via grasp API.
"""

from dataclasses import dataclass, field
from typing import Any, Generic, NoReturn, TypeVar, Dict

import pandas as pd
from ds_common_logger_py_lib import Logger
from ds_protocol_http_py_lib.linked_service.http import HttpLinkedService
from ds_resource_plugin_py_lib.common.resource.dataset import (
    DatasetSettings,
    TabularDataset,
)
from ds_resource_plugin_py_lib.common.resource.errors import ResourceException
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import AuthorizationError, ConnectionError
from ds_resource_plugin_py_lib.common.serde.deserialize import PandasDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import PandasSerializer

from ..enums import ResourceType

logger = Logger.get_logger(__name__, package=True)


@dataclass(kw_only=True)
class CreateSettings:
    """
    Settings for create operations.
    """

    acl: Dict | None = field(default_factory=dict)
    description: str | None = None
    file_path: str | None = None
    metadata: Dict | None = field(default_factory=dict)
    status: str | None = field(default="active")
    tags: Dict | None = field(default_factory=dict)
    version: str | None = field(default="1.0.0")


@dataclass(kw_only=True)
class ReadSettings:
    """
    Settings for read operations.
    """
    download_file: bool = True

@dataclass(kw_only=True)
class GraspFileDatasetSettings(DatasetSettings):
    """Settings for Grasp file dataset operations."""

    endpoint: str = field(default="file/")
    create: CreateSettings = field(default_factory=CreateSettings)
    read: ReadSettings = field(default_factory=ReadSettings)


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

    def create(self) -> None:
        """
        Write the content of the dataset to the file.
        :return: None
        """
        metadata = self._create_metadata()
        data = self._upload_file_content(metadata)
        self.output = pd.DataFrame(data)

    def read(self) -> None:
        """
        Read the file from the file_uri.
        :return: None
        """
        logger.debug(f"Reading files form {self.linked_service.settings.host}")

        response = self.linked_service.connection.request(
            method="GET",
            url=f"{self.linked_service.settings.host}{self.settings.endpoint}",
            headers=self.linked_service.settings.headers,
        )

        files = response.json()["data"]
        if self.settings.read.download_file:
            for file in files:
                file_id = file["id"]
                url = f"{self.linked_service.settings.host}{self.settings.endpoint}{file_id}/content/"
                try:
                    response = self.linked_service.connection.request(
                        method="GET",
                        url=url,
                        headers=self.linked_service.settings.headers,
                    )
                except ResourceException as exc:
                    if exc.status_code == 404:
                        file.update({"content": b""})
                        continue
                file.update({"content": response.content})

            self.output = pd.DataFrame(files)
        else:
            self.output = pd.DataFrame(files)


    def update(self) -> NoReturn:
        raise AuthorizationError(
            message="You are not authorized to update a Grasp File dataset",
            status_code=403,
            details={
                "type": self.type.value,
                "settings": self.settings.serialize(),
            },
        )

    def upsert(self) -> NoReturn:
        raise AuthorizationError(
            message="You are not authorized to upsert a Grasp File dataset",
            status_code=403,
            details={
                "type": self.type.value,
                "settings": self.settings.serialize(),
            },
        )

    def delete(self) -> NoReturn:
        raise AuthorizationError(
            message="You are not authorized to delete a Grasp File dataset",
            status_code=403,
            details={
                "type": self.type.value,
                "settings": self.settings.serialize(),
            },
        )

    def purge(self) -> NoReturn:
        raise AuthorizationError(
            message="You are not authorized to purge a Grasp File dataset",
            status_code=403,
            details={
                "type": self.type.value,
                "settings": self.settings.serialize(),
            },
        )

    def list(self) -> NoReturn:
        raise AuthorizationError(
            message="You are not authorized to list a Grasp File dataset",
            status_code=403,
            details={
                "type": self.type.value,
                "settings": self.settings.serialize(),
            },
        )

    def rename(self) -> NoReturn:
        raise AuthorizationError(
            message="You are not authorized to rename a Grasp File dataset",
            status_code=403,
            details={
                "type": self.type.value,
                "settings": self.settings.serialize(),
            },
        )

    def close(self) -> None:
        """Close the dataset."""
        self.linked_service.close()

    def _create_metadata(self) -> Dict:
        """
        Create the metadata for the file.
        :return: Dict
        """
        json = {
            "acl": self.settings.create.acl,
            "description": self.settings.create.description,
            "file_path": self.settings.create.file_path,
            "metadata": self.settings.create.metadata,
            "status": self.settings.create.status,
            "tags": self.settings.create.tags,
            "version": self.settings.create.version,
        }
        logger.info(f"Creating file metadata: {json}")
        response = self.linked_service.connection.request(
            method="POST",
            url=f"{self.linked_service.settings.host}{self.settings.endpoint}",
            headers=self.linked_service.settings.headers,
            json=json,
        )
        data = response.json()
        logger.info(f"File metadata created: {data}")
        return data

    def _upload_file_content(self, metadata: Dict) -> Dict:
        """
        Upload the file content to the file.
        :return: Dict
        """
        headers = self.linked_service.settings.headers
        headers.update({
            "content_type": "application/octet-stream",
            "Content-Type": "application/octet-stream",
            "accept": "*/*",
        })
        url = f"{self.linked_service.settings.host}{self.settings.endpoint}{metadata['id']}/content/"

        response = self.linked_service.connection.request(
            method="PUT",
            url=url,
            headers=headers,
            data=self.input.to_json(orient="records").encode(),
        )
        data = response.json()
        return data
