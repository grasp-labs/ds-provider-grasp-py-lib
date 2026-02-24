"""
**File:** ``cart.py``
**Region:** ``ds_provider_grasp_py_lib/dataset/cart``

Grasp Cart Dataset

This module implements a dataset for Grasp Cart.

Example:
    >>> dataset = GraspCartDataset(
    ...     id=uuid.uuid4(),
    ...     name="cart-dataset",
    ...     version="1.0.0",
    ...     deserializer=PandasDeserializer(format=DatasetStorageFormatType.JSON),
    ...     serializer=PandasSerializer(format=DatasetStorageFormatType.JSON),
    ...     settings=GraspCartDatasetSettings(
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

from awswrangler.exceptions import NoFilesFound
from ds_common_logger_py_lib import Logger
from ds_provider_aws_py_lib.linked_service.aws import AWSLinkedService
from ds_resource_plugin_py_lib.common.resource.dataset import (
    DatasetSettings,
    DatasetStorageFormatType,
    TabularDataset,
)
from ds_resource_plugin_py_lib.common.resource.dataset.errors import NotFoundError, ReadError
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import AuthorizationError
from ds_resource_plugin_py_lib.common.serde.deserialize import AwsWranglerDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import AwsWranglerSerializer

from ..enums import ResourceType
from ..utils import get_bucket_name

logger = Logger.get_logger(__name__, package=True)


@dataclass(kw_only=True)
class GraspCartDatasetSettings(DatasetSettings):
    """
    Settings for Grasp Cart dataset operations.
    """

    owner_id: str
    """The owner ID of the cart."""
    product_group_name: str
    """The product group name of the cart."""
    product_name: str
    """The product name of the cart."""
    version: str = "1.0"
    """The version of the cart."""
    include_history: bool = False
    """Whether to include history in the cart."""


GraspCartDatasetSettingsType = TypeVar(
    "GraspCartDatasetSettingsType",
    bound=GraspCartDatasetSettings,
)
AWSLinkedServiceType = TypeVar(
    "AWSLinkedServiceType",
    bound=AWSLinkedService[Any],
)


@dataclass(kw_only=True)
class GraspCartDataset(
    TabularDataset[
        AWSLinkedServiceType,
        GraspCartDatasetSettingsType,
        AwsWranglerSerializer,
        AwsWranglerDeserializer,
    ],
    Generic[AWSLinkedServiceType, GraspCartDatasetSettingsType],
):
    linked_service: AWSLinkedServiceType
    settings: GraspCartDatasetSettingsType

    serializer: AwsWranglerSerializer | None = field(
        default_factory=lambda: AwsWranglerSerializer(format=DatasetStorageFormatType.PARQUET),
    )
    deserializer: AwsWranglerDeserializer | None = field(
        default_factory=lambda: AwsWranglerDeserializer(format=DatasetStorageFormatType.PARQUET),
    )

    @property
    def type(self) -> ResourceType:
        return ResourceType.DATASET_CART

    def _get_s3_path(self, tenant_id: str) -> str:
        bucket = get_bucket_name()
        return (
            f"s3://{bucket}/datalake/cart/{self.settings.product_group_name}/"
            f"{self.settings.version}/{tenant_id}/{self.settings.product_name}/"
            f"{self.settings.owner_id}/data/"
        )

    def create(self, **_kwargs: Any) -> None:
        raise AuthorizationError(
            message="You are not authorized to create a Grasp Cart dataset",
            status_code=403,
            details={"settings": self.settings.serialize()},
        )

    def read(self, **_kwargs: Any) -> None:
        """
        Read data from the Grasp Cart dataset.

        Raises:
            ReadError: If the read operation fails, including when no files are found
                at the S3 path or when the S3 path is invalid.
        """
        tenant_id = getenv("TENANT_ID")
        if tenant_id is None:
            logger.error("TENANT_ID environment variable is required")
            raise ReadError(
                message="TENANT_ID environment variable is required",
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

        if not self.settings.include_history:
            logger.debug("Dropping _valid_to rows")
            self.output = self.output.loc[self.output["_valid_to"].isna()]

        logger.debug(f"Successfully read {len(self.output)} rows from {s3_path}")

    def delete(self, **_kwargs: Any) -> NoReturn:
        raise AuthorizationError(
            message="You are not authorized to delete a Grasp Cart dataset",
            status_code=403,
            details={
                "type": self.type.value,
                "settings": self.settings.serialize(),
            },
        )

    def update(self, **_kwargs: Any) -> NoReturn:
        raise AuthorizationError(
            message="You are not authorized to update a Grasp Cart dataset",
            status_code=403,
            details={
                "type": self.type.value,
                "settings": self.settings.serialize(),
            },
        )

    def upsert(self, **_kwargs: Any) -> NoReturn:
        raise AuthorizationError(
            message="You are not authorized to upsert a Grasp Cart dataset",
            status_code=403,
            details={
                "type": self.type.value,
                "settings": self.settings.serialize(),
            },
        )

    def rename(self, **_kwargs: Any) -> NoReturn:
        raise AuthorizationError(
            message="You are not authorized to rename a Grasp Cart dataset",
            status_code=403,
            details={
                "type": self.type.value,
                "settings": self.settings.serialize(),
            },
        )

    def purge(self, **_kwargs: Any) -> NoReturn:
        raise AuthorizationError(
            message="You are not authorized to purge a Grasp Cart dataset",
            status_code=403,
            details={
                "type": self.type.value,
                "settings": self.settings.serialize(),
            },
        )

    def list(self, **_kwargs: Any) -> NoReturn:
        raise AuthorizationError(
            message="You are not authorized to list a Grasp Cart dataset",
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
