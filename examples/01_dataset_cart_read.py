"""
**File:** ``01_dataset_cart_read.py``
**Region:** ``examples/01_dataset_cart_read``

Example 01: Read data from a Grasp Cart dataset using GraspCartDataset.

This example demonstrates how to:
- Create a Grasp Cart dataset
- Read data from a cart
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

from ds_provider_grasp_py_lib.dataset.cart import (
    GraspCartDataset,
    GraspCartDatasetSettings,
)


Logger.configure(
    level=logging.DEBUG,
    logger_levels={
        "ds.provider": logging.DEBUG,
        "ds.resource": logging.DEBUG,
    },

)
logger = Logger.get_logger(__name__)


def main() -> None:
    """Main function demonstrating Grasp Cart dataset read operation."""
    dataset = GraspCartDataset(
        id=uuid.uuid4(),
        name="cart-dataset",
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
        settings=GraspCartDatasetSettings(
            owner_id="****************",
            product_group_name="****************",
            product_name="****************",
            version="****************",
            include_history=False,
        ),
    )

    try:
        environ["TENANT_ID"] = "****************"
        dataset.linked_service.connect()
        dataset.read()
    except Exception as exc:
        logger.error(f"Error reading cart dataset: {exc.__dict__}")


if __name__ == "__main__":
    main()
