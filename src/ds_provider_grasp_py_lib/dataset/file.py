"""
**File:** ``file.py``
**Region:** ``ds_provider_grasp_py_lib/dataset/file``

Grasp File Dataset

This module implements a generic GRASP file dataset backed by S3.
The dataset can read, create, list, and purge files addressed by a
configured S3 path.
"""

from dataclasses import dataclass, field
from typing import Any, Generic, NoReturn, TypeVar

import awswrangler as wr
import pandas as pd
from awswrangler.exceptions import NoFilesFound
from ds_common_logger_py_lib import Logger
from ds_provider_aws_py_lib.linked_service.aws import AWSLinkedService
from ds_resource_plugin_py_lib.common.resource.dataset import (
    DatasetSettings,
    DatasetStorageFormatType,
    TabularDataset,
)
from ds_resource_plugin_py_lib.common.resource.dataset.errors import (
    CreateError,
    NotFoundError,
    ReadError,
)
from ds_resource_plugin_py_lib.common.resource.errors import NotSupportedError
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import AuthorizationError, ConnectionError
from ds_resource_plugin_py_lib.common.serde.deserialize import AwsWranglerDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import AwsWranglerSerializer

from ..enums import ResourceType

logger = Logger.get_logger(__name__, package=True)


@dataclass(kw_only=True)
class GraspFileDatasetSettings(DatasetSettings):
    """Settings for Grasp file dataset operations."""

    s3_path: str | None = None
    """The S3 path or prefix representing the file resource scope."""

    format: DatasetStorageFormatType = DatasetStorageFormatType.JSON
    """The storage format used when reading or writing the file."""

    serializer_kwargs: dict[str, Any] = field(default_factory=dict)
    """Additional keyword arguments forwarded to the serializer."""

    deserializer_kwargs: dict[str, Any] = field(default_factory=dict)
    """Additional keyword arguments forwarded to the deserializer."""


GraspFileDatasetSettingsType = TypeVar(
    "GraspFileDatasetSettingsType",
    bound=GraspFileDatasetSettings,
)
AWSLinkedServiceType = TypeVar(
    "AWSLinkedServiceType",
    bound=AWSLinkedService[Any],
)


@dataclass(kw_only=True)
class GraspFileDataset(
    TabularDataset[
        AWSLinkedServiceType,
        GraspFileDatasetSettingsType,
        AwsWranglerSerializer,
        AwsWranglerDeserializer,
    ],
    Generic[AWSLinkedServiceType, GraspFileDatasetSettingsType],
):
    linked_service: AWSLinkedServiceType
    settings: GraspFileDatasetSettingsType

    def __post_init__(self) -> None:
        if self.serializer is None:
            self.serializer = AwsWranglerSerializer(
                format=self.settings.format,
                kwargs=self.settings.serializer_kwargs.copy(),
            )

        if self.deserializer is None:
            self.deserializer = AwsWranglerDeserializer(
                format=self.settings.format,
                kwargs=self.settings.deserializer_kwargs.copy(),
            )

    @property
    def type(self) -> ResourceType:
        return ResourceType.DATASET_FILE

    def _details(self, **extra: Any) -> dict[str, Any]:
        return {
            "type": self.type.value,
            "settings": self.settings.serialize(),
            **extra,
        }

    def _get_s3_path(self) -> str:
        if not self.settings.s3_path:
            raise ValueError("File dataset settings.s3_path is required")
        return self.settings.s3_path

    def _get_connection(self) -> Any:
        connection = self.linked_service.connection
        if connection is None:
            raise ConnectionError(
                message="Linked service connection is not established",
                status_code=503,
                details=self._details(),
            )
        return connection

    def _raise_not_supported(self, operation: str) -> NoReturn:
        raise NotSupportedError(
            message=f"Grasp File dataset does not support {operation}()",
            details=self._details(operation=operation),
        )

    def create(self) -> None:
        if self.input is None:
            self.output = pd.DataFrame()
            return

        if hasattr(self.input, "empty") and self.input.empty:
            self.output = self.input.copy()
            return

        if not self.serializer:
            logger.error("Serializer is not set.")
            raise CreateError(
                message="Serializer is not set.",
                status_code=400,
                details=self._details(),
            )

        input_frame = self.input.copy()

        try:
            s3_path = self._get_s3_path()
            connection = self._get_connection()
        except ValueError as exc:
            raise CreateError(
                message=str(exc),
                status_code=400,
                details=self._details(),
            ) from exc
        except ConnectionError as exc:
            raise CreateError(
                message=str(exc),
                status_code=exc.status_code,
                details=self._details(error=exc.details),
            ) from exc

        try:
            existing_paths = wr.s3.list_objects(
                path=s3_path,
                boto3_session=connection,
            )
            if existing_paths:
                raise CreateError(
                    message=f"Resource already exists at S3 path: {s3_path}",
                    status_code=409,
                    details=self._details(s3_path=s3_path, existing_paths=existing_paths),
                )

            self.serializer(
                input_frame,
                path=s3_path,
                boto3_session=connection,
            )
            self.output = input_frame
            logger.debug(f"Successfully wrote {len(self.output)} rows to {s3_path}")
        except CreateError:
            raise
        except Exception as exc:
            logger.exception(f"Failed to create file dataset at {s3_path}: {exc!s}")
            raise CreateError(
                message=f"Failed to create file dataset at {s3_path}: {exc!s}",
                status_code=500,
                details=self._details(s3_path=s3_path),
            ) from exc

    def read(self) -> None:
        if not self.deserializer:
            logger.error("Deserializer is not set.")
            raise ReadError(
                message="Deserializer is not set.",
                status_code=400,
                details=self._details(),
            )

        try:
            s3_path = self._get_s3_path()
            connection = self._get_connection()
        except ValueError as exc:
            raise ReadError(
                message=str(exc),
                status_code=400,
                details=self._details(),
            ) from exc
        except ConnectionError as exc:
            raise ReadError(
                message=str(exc),
                status_code=exc.status_code,
                details=self._details(error=exc.details),
            ) from exc

        logger.debug(f"Reading data from S3 path: {s3_path}")
        try:
            self.output = self.deserializer(
                s3_path,
                boto3_session=connection,
            )
            logger.debug(f"Successfully read {len(self.output)} rows from {s3_path}")
        except NoFilesFound as exc:
            logger.error(f"No files found at S3 path: {s3_path}")
            raise NotFoundError(
                message=f"No files found at S3 path: {s3_path}",
                status_code=404,
                details=self._details(s3_path=s3_path, error=str(exc)),
            ) from exc
        except Exception as exc:
            logger.exception(f"Failed to read data from S3 path: {s3_path}: {exc!s}")
            raise ReadError(
                message=f"Failed to read data from S3 path: {s3_path}: {exc!s}",
                status_code=500,
                details=self._details(s3_path=s3_path),
            ) from exc

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

    def purge(self) -> None:
        raise AuthorizationError(
            message="You are not authorized to purge a Grasp File dataset",
            status_code=403,
            details={
                "type": self.type.value,
                "settings": self.settings.serialize(),
            },
        )

    def list(self) -> None:
        raise AuthorizationError(
            message="You are not authorized to list a Grasp File dataset",
            status_code=403,
            details={
                "type": self.type.value,
                "settings": self.settings.serialize(),
            },
        )

    def rename(self) -> NoReturn:
        self._raise_not_supported("rename")

    def close(self) -> None:
        """Close the dataset."""
        self.linked_service.close()
