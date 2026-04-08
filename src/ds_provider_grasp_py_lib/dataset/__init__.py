"""
**File:** ``__init__.py``
**Region:** ``ds_provider_grasp_py_lib/dataset``

Grasp Datasets

This module provides access to both Grasp Cart and Grasp Ingress datasets.

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

from .cart import GraspCartDataset, GraspCartDatasetSettings
from .file import GraspFileDataset, GraspFileDatasetSettings
from .ingress import GraspIngressDataset, GraspIngressDatasetSettings

__all__ = [
    "GraspCartDataset",
    "GraspCartDatasetSettings",
    "GraspFileDataset",
    "GraspFileDatasetSettings",
    "GraspIngressDataset",
    "GraspIngressDatasetSettings",
]
