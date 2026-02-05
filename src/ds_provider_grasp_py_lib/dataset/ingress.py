"""
**File:** ``ingress.py``
**Region:** ``ds_provider_grasp_py_lib/dataset/ingress``

Grasp Ingress Dataset

This module implements a dataset for Grasp Ingress.

Example:
    >>> dataset = GraspIngressDataset(
    ...     id=uuid.uuid4(),
    ...     name="ingress-dataset",
    ...     version="1.0.0",
    ...     deserializer=PandasDeserializer(format=DatasetStorageFormatType.JSON),
    ...     serializer=PandasSerializer(format=DatasetStorageFormatType.JSON),
    ...     settings=GraspIngressDatasetSettings(
    ...         owner_id="owner_id",
    ...         product_group_name="product_group_name",
    ...         product_name="product_name",
    ...         version="version",
    ...         include_history=True,
    ...     ),
    ...     linked_service=GraspAwsLinkedService(
    ...         id=uuid.uuid4(),
    ...         name="aws-linked-service",
    ...         version="1.0.0",
    ...         settings=GraspAwsLinkedServiceSettings(
    ...             access_key_id="access_key_id",
    ...             access_key_secret="access_key_secret",
    ...             region="region",
    ...         ),
    ...     ),
    ... )
    >>> dataset.read()
    >>> data = dataset.output
"""

from dataclasses import dataclass, field
from os import getenv
from typing import Any, Generic, NoReturn, TypeVar

import pandas as pd
from awswrangler.exceptions import NoFilesFound
from ds_common_logger_py_lib import Logger
from ds_provider_aws_py_lib.linked_service.aws import AWSLinkedService
from ds_resource_plugin_py_lib.common.resource.dataset import (
    DatasetSettings,
    DatasetStorageFormatType,
    TabularDataset,
)
from ds_resource_plugin_py_lib.common.resource.dataset.errors import NotFoundError, ReadError
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import AuthorizationError, ConnectionError
from ds_resource_plugin_py_lib.common.serde.deserialize import AwsWranglerDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import AwsWranglerSerializer

from ..enums import ResourceType
from ..utils import get_bucket_name

logger = Logger.get_logger(__name__, package=True)


@dataclass(kw_only=True)
class GraspIngressDatasetSettings(DatasetSettings):
    """
    Settings for Grasp Ingress dataset operations.
    """


GraspIngressDatasetSettingsType = TypeVar(
    "GraspIngressDatasetSettingsType",
    bound=GraspIngressDatasetSettings,
)
AWSLinkedServiceType = TypeVar(
    "AWSLinkedServiceType",
    bound=AWSLinkedService[Any],
)


@dataclass(kw_only=True)
class GraspIngressDataset(
    TabularDataset[
        AWSLinkedServiceType,
        GraspIngressDatasetSettingsType,
        AwsWranglerSerializer,
        AwsWranglerDeserializer,
    ],
    Generic[AWSLinkedServiceType, GraspIngressDatasetSettingsType],
):
    linked_service: AWSLinkedServiceType
    settings: GraspIngressDatasetSettingsType

    serializer: AwsWranglerSerializer | None = field(
        default_factory=lambda: AwsWranglerSerializer(format=DatasetStorageFormatType.JSON),
    )
    deserializer: AwsWranglerDeserializer | None = field(
        default_factory=lambda: AwsWranglerDeserializer(format=DatasetStorageFormatType.JSON),
    )

    @property
    def type(self) -> ResourceType:
        return ResourceType.DATASET_INGRESS

    def _get_s3_path(self, tenant_id: str, session_id: str) -> str:
        """
        Get the S3 path for the Grasp Ingress dataset.

        Returns:
            str: The S3 path for the Grasp Ingress dataset.
        """
        bucket = get_bucket_name()
        return f"s3://{bucket}/datalake/workflows/{tenant_id}/{self.id}/{session_id}.json"

    def create(self, **_kwargs: Any) -> None:
        raise AuthorizationError(
            message="You are not authorized to create a Grasp Ingress dataset",
            status_code=403,
            details={"settings": self.settings.serialize()},
        )

    def read(self, **_kwargs: Any) -> None:
        """
        Read data from the Grasp Ingress dataset.

        Raises:
            ConnectionError: If the connection fails.
            ReadError: If the read operation fails, including when no files are found
                at the S3 path or when the S3 path is invalid.
        """
        tenant_id = getenv("TENANT_ID")
        session_id = getenv("SESSION_ID")

        if tenant_id is None or session_id is None:
            logger.error("TENANT_ID and SESSION_ID environment variables are required")
            raise ReadError(
                message="TENANT_ID and SESSION_ID environment variables are required",
                status_code=400,
                details={"type": self.type.value, "settings": self.settings.serialize()},
            )

        if self.linked_service.session is None:
            logger.error("Connection is not established.")
            raise ConnectionError(
                message="Connection is not established.",
                status_code=500,
                details={
                    "type": self.type.value,
                    "settings": self.settings.serialize(),
                },
            )

        if not self.deserializer:
            logger.error("Deserializer is not set.")
            raise ReadError(
                message="Deserializer is not set.",
                status_code=400,
                details={
                    "type": self.type.value,
                    "settings": self.settings.serialize(),
                },
            )

        s3_path = self._get_s3_path(
            tenant_id=tenant_id,
            session_id=session_id,
        )
        logger.debug(f"Reading data from S3 path: {s3_path}")
        try:
            self.output = self.deserializer(
                s3_path,
                boto3_session=self.linked_service.session,
            )
            self._set_schema(self.output)
            self.next = False
        except NoFilesFound as exc:
            logger.error(f"No files found at S3 path: {s3_path}")
            raise NotFoundError(
                message=f"No ingress found for tenant_id: {tenant_id}, dataset_id: {self.id}, session_id: {session_id}",
                status_code=404,
                details={
                    "s3_path": s3_path,
                    "type": self.type.value,
                    "settings": self.settings.serialize(),
                    "error": str(exc),
                },
            ) from exc
        except Exception as exc:
            logger.exception(f"Failed to read data from S3 path: {s3_path}: {exc!s}")
            raise ReadError(
                message=f"Failed to read data from S3 path: {s3_path}: {exc!s}",
                status_code=500,
                details={
                    "s3_path": s3_path,
                    "type": self.type.value,
                    "settings": self.settings.serialize(),
                },
            ) from exc
        logger.debug(f"Successfully read {len(self.output)} rows from {s3_path}")
        logger.debug(f"Schema: {self.schema}")

    def delete(self, **_kwargs: Any) -> NoReturn:
        raise AuthorizationError(
            message="You are not authorized to delete a Grasp Ingress dataset",
            status_code=403,
            details={
                "type": self.type.value,
                "settings": self.settings.serialize(),
            },
        )

    def update(self, **_kwargs: Any) -> NoReturn:
        raise AuthorizationError(
            message="You are not authorized to update a Grasp Ingress dataset",
            status_code=403,
            details={
                "type": self.type.value,
                "settings": self.settings.serialize(),
            },
        )

    def rename(self, **_kwargs: Any) -> NoReturn:
        raise AuthorizationError(
            message="You are not authorized to rename a Grasp Ingress dataset",
            status_code=403,
            details={
                "type": self.type.value,
                "settings": self.settings.serialize(),
            },
        )

    def close(self) -> None:
        """
        Close the dataset.
        """
        self.linked_service.close()

    def _set_schema(self, content: pd.DataFrame) -> None:
        """
        Set the schema from the content.

        Args:
            content: The content to set the schema from.
        """
        converted = content.convert_dtypes(dtype_backend="pyarrow")
        self.schema = {str(col): str(dtype) for col, dtype in converted.dtypes.to_dict().items()}
