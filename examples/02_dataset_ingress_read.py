"""
**File:** ``02_dataset_ingress_read.py``
**Region:** ``examples/02_dataset_ingress_read``

Example 02: Read data from a Grasp Ingress dataset using GraspIngressDataset.

This example demonstrates how to:
- Create a Grasp Ingress dataset
- Read data from a ingress
"""

from __future__ import annotations

import logging

from os import environ
import uuid
from ds_common_logger_py_lib import Logger
from ds_provider_aws_py_lib.linked_service.aws import (
    AWSLinkedService,
    AWSLinkedServiceSettings,
)

from ds_provider_grasp_py_lib.dataset.ingress import (
    GraspIngressDataset,
    GraspIngressDatasetSettings,
)
from ds_resource_plugin_py_lib.common.serde.deserialize import AwsWranglerDeserializer
from ds_resource_plugin_py_lib.common.resource.dataset import DatasetStorageFormatType


Logger.configure(
    level=logging.DEBUG,
    logger_levels={
        "ds.provider": logging.DEBUG,
        "ds.resource": logging.DEBUG,
    },

)
logger = Logger.get_logger(__name__)


def main() -> None:
    """Main function demonstrating Grasp Ingress dataset read operation."""
    dataset = GraspIngressDataset(
        id=uuid.uuid4(),
        name="ingress-dataset",
        version="1.0.0",
        linked_service=AWSLinkedService(
            id=uuid.uuid4(),
            name="aws-linked-service",
            version="1.0.0",
            settings=AWSLinkedServiceSettings(
                access_key_id="****************",
                access_key_secret="****************",
                region="eu-north-1",
                account_id="****************",
            ),
        ),
        deserializer=AwsWranglerDeserializer(
            format=DatasetStorageFormatType.JSON,
        ),
        settings=GraspIngressDatasetSettings(),
    )

    try:
        environ["TENANT_ID"] = "****************"
        environ["SESSION_ID"] = "****************"
        dataset.linked_service.connect()
        dataset.read()
    except Exception as exc:
        logger.error(f"Error reading ingress dataset: {exc.__dict__}")


if __name__ == "__main__":
    main()
