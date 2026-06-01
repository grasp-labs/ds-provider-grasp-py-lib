"""
**File:** ``gold.py``
**Region:** ``ds_provider_grasp_py_lib/dataset/gold``

Grasp Gold Dataset

This module implements an internal dataset for the Grasp Gold layer.

The Gold layer stores curated, versioned tabular data as Parquet files on S3
following the contract owned by ``ds-pipeline-gold``. Storage layout is keyed
by ``tenant_id`` and ``dataset_id``:

    s3://{bucket}/{tenant_id}/gold/{dataset_id}/

This dataset is intended for internal usage only. It supports writing a
Parquet file to the Gold location via :meth:`create` and reading the current
or full history of the dataset via :meth:`read`. All other mutating
operations are owned by the Gold pipeline service and raise
:class:`AuthorizationError`.
"""

from dataclasses import dataclass
from os import getenv
from typing import Any, Generic, NoReturn, TypeVar

from awswrangler.exceptions import NoFilesFound
from ds_common_logger_py_lib import Logger
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
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import AuthorizationError
from ds_resource_plugin_py_lib.common.serde.deserialize import AwsWranglerDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import AwsWranglerSerializer

from ..enums import ResourceType
from ..linked_service.iam_role import GraspIAMRoleLinkedService
from ..utils import get_bucket_name

logger = Logger.get_logger(__name__, package=True)

_ALLOWED_WRITE_MODES = ("append", "overwrite", "overwrite_partitions")


@dataclass(kw_only=True)
class GraspGoldDatasetSettings(DatasetSettings):
    """
    Settings for Grasp Gold dataset operations.
    """

    dataset_id: str
    """The stable logical dataset identity used as the Gold storage key."""
    tenant_id: str | None = None
    """Tenant identity for the Gold storage key; falls back to the ``TENANT_ID`` env var."""
    include_history: bool = False
    """Whether to include SCD2 history rows (``_valid_to`` not null) on read."""
    partition_cols: list[str] | None = None
    """Columns to partition the Parquet dataset by (hive-style)."""
    mode: str | None = None
    """Write mode: ``append``, ``overwrite``, or ``overwrite_partitions``."""
    compression: str | None = "snappy"
    """Parquet compression codec, e.g. ``snappy``, ``gzip``, or ``None``."""

    def __post_init__(self) -> None:
        if self.mode is not None and self.mode not in _ALLOWED_WRITE_MODES:
            raise ValueError(
                f"Invalid write mode {self.mode!r}; "
                f"must be one of {_ALLOWED_WRITE_MODES}.",
            )


GraspGoldDatasetSettingsType = TypeVar(
    "GraspGoldDatasetSettingsType",
    bound=GraspGoldDatasetSettings,
)
GraspIAMRoleLinkedServiceType = TypeVar(
    "GraspIAMRoleLinkedServiceType",
    bound=GraspIAMRoleLinkedService[Any],
)


@dataclass(kw_only=True)
class GraspGoldDataset(
    TabularDataset[
        GraspIAMRoleLinkedServiceType,
        GraspGoldDatasetSettingsType,
        AwsWranglerSerializer,
        AwsWranglerDeserializer,
    ],
    Generic[GraspIAMRoleLinkedServiceType, GraspGoldDatasetSettingsType],
):
    linked_service: GraspIAMRoleLinkedServiceType
    settings: GraspGoldDatasetSettingsType

    def __post_init__(self) -> None:
        self.serializer = AwsWranglerSerializer(format=DatasetStorageFormatType.PARQUET)
        self.deserializer = AwsWranglerDeserializer(format=DatasetStorageFormatType.PARQUET)

    @property
    def type(self) -> ResourceType:
        return ResourceType.DATASET_GOLD

    def _resolve_tenant_id(self) -> str | None:
        """Resolve the tenant id, preferring settings over the ``TENANT_ID`` env var."""
        return self.settings.tenant_id or getenv("TENANT_ID")

    def _get_s3_path(self, tenant_id: str) -> str:
        bucket = get_bucket_name()
        return f"s3://{bucket}/{tenant_id}/gold/{self.settings.dataset_id}/"

    def _build_write_kwargs(self, s3_path: str) -> dict[str, Any]:
        """Build the kwargs passed through to ``awswrangler.s3.to_parquet``."""
        kwargs: dict[str, Any] = {"path": s3_path}
        if self.settings.compression is not None:
            kwargs["compression"] = self.settings.compression
        if self.settings.partition_cols:
            kwargs["partition_cols"] = self.settings.partition_cols
            kwargs["dataset"] = True
        if self.settings.mode is not None:
            kwargs["mode"] = self.settings.mode
            kwargs["dataset"] = True
        return kwargs

    def create(self) -> None:
        """
        Write ``self.input`` as Parquet to the Gold S3 path.

        Raises:
            CreateError: If the tenant id cannot be resolved, the serializer is
                missing, or the underlying write fails.
        """
        tenant_id = self._resolve_tenant_id()
        if tenant_id is None:
            logger.error("tenant_id setting or TENANT_ID environment variable is required")
            raise CreateError(
                message="tenant_id setting or TENANT_ID environment variable is required",
                status_code=400,
                details={"type": self.type.value, "settings": self.settings.serialize()},
            )

        if not self.serializer:
            logger.error("Serializer is not set.")
            raise CreateError(
                message="Serializer is not set.",
                status_code=400,
                details={"type": self.type.value, "settings": self.settings.serialize()},
            )

        s3_path = self._get_s3_path(tenant_id=tenant_id)
        logger.debug(f"Writing data to S3 path: {s3_path}")
        try:
            self.serializer.kwargs = {
                **self.serializer.kwargs,
                **self._build_write_kwargs(s3_path),
            }
            self.serializer(
                self.input,
                boto3_session=self.linked_service.connection,
            )
        except Exception as exc:
            logger.exception(f"Failed to write data to S3 path: {s3_path}: {exc!s}")
            raise CreateError(
                message=f"Failed to write data to S3 path: {s3_path}: {exc!s}",
                status_code=500,
                details={
                    "s3_path": s3_path,
                    "type": self.type.value,
                    "settings": self.settings.serialize(),
                },
            ) from exc

        logger.debug(f"Successfully wrote {len(self.input)} rows to {s3_path}")

    def read(self) -> None:
        """
        Read Parquet data from the Gold S3 path into ``self.output``.

        When :attr:`GraspGoldDatasetSettings.include_history` is False, rows
        with a non-null ``_valid_to`` value (SCD2 history) are filtered out.

        Raises:
            ReadError: If the tenant id cannot be resolved, the deserializer is
                missing, or the underlying read fails.
            NotFoundError: If no files are found at the Gold S3 path.
        """
        tenant_id = self._resolve_tenant_id()
        if tenant_id is None:
            logger.error("tenant_id setting or TENANT_ID environment variable is required")
            raise ReadError(
                message="tenant_id setting or TENANT_ID environment variable is required",
                status_code=400,
                details={"type": self.type.value, "settings": self.settings.serialize()},
            )

        if not self.deserializer:
            logger.error("Deserializer is not set.")
            raise ReadError(
                message="Deserializer is not set.",
                status_code=400,
                details={"type": self.type.value, "settings": self.settings.serialize()},
            )

        s3_path = self._get_s3_path(tenant_id=tenant_id)
        logger.debug(f"Reading data from S3 path: {s3_path}")
        try:
            self.output = self.deserializer(
                s3_path,
                boto3_session=self.linked_service.connection,
            )
        except NoFilesFound as exc:
            logger.error(f"No files found at S3 path: {s3_path}")
            raise NotFoundError(
                message=f"No files found at S3 path: {s3_path}",
                status_code=404,
                details={
                    "s3_path": s3_path,
                    "type": self.type.value,
                    "settings": self.settings.serialize(),
                    "error": str(exc),
                },
            ) from exc
        except Exception as exc:
            logger.exception(f"Failed to read data from table: {exc!s}")
            raise ReadError(
                message=f"Failed to read data from table: {exc!s}",
                status_code=500,
                details={
                    "s3_path": s3_path,
                    "type": self.type.value,
                    "settings": self.settings.serialize(),
                },
            ) from exc

        if not self.settings.include_history and "_valid_to" in self.output.columns:
            logger.debug("Dropping _valid_to rows")
            self.output = self.output.loc[self.output["_valid_to"].isna()]

        logger.debug(f"Successfully read {len(self.output)} rows from {s3_path}")

    def delete(self) -> NoReturn:
        raise AuthorizationError(
            message="You are not authorized to delete a Grasp Gold dataset",
            status_code=403,
            details={"type": self.type.value, "settings": self.settings.serialize()},
        )

    def update(self) -> NoReturn:
        raise AuthorizationError(
            message="You are not authorized to update a Grasp Gold dataset",
            status_code=403,
            details={"type": self.type.value, "settings": self.settings.serialize()},
        )

    def upsert(self) -> NoReturn:
        raise AuthorizationError(
            message="You are not authorized to upsert a Grasp Gold dataset",
            status_code=403,
            details={"type": self.type.value, "settings": self.settings.serialize()},
        )

    def rename(self) -> NoReturn:
        raise AuthorizationError(
            message="You are not authorized to rename a Grasp Gold dataset",
            status_code=403,
            details={"type": self.type.value, "settings": self.settings.serialize()},
        )

    def purge(self) -> NoReturn:
        raise AuthorizationError(
            message="You are not authorized to purge a Grasp Gold dataset",
            status_code=403,
            details={"type": self.type.value, "settings": self.settings.serialize()},
        )

    def list(self) -> NoReturn:
        raise AuthorizationError(
            message="You are not authorized to list a Grasp Gold dataset",
            status_code=403,
            details={"type": self.type.value, "settings": self.settings.serialize()},
        )

    def close(self) -> None:
        """
        Close the dataset.
        """
        self.linked_service.close()
