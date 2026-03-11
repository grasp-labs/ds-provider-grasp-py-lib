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

from dotenv import load_dotenv
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

load_dotenv()


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
                access_key_id=environ.get("AWS_ACCESS_KEY_ID", ""),
                access_key_secret=environ.get("AWS_ACCESS_KEY_SECRET", ""),
                region="eu-north-1",
                account_id=environ.get("AWS_ACCOUNT_ID", ""),
            ),
        ),
        settings=GraspCartDatasetSettings(
            owner_id=environ.get("OWNER_ID"),
            product_group_name=environ.get("PRODUCT_GROUP_NAME", ""),
            product_name=environ.get("PRODUCT_NAME", ""),
            version=environ.get("VERSION", ""),
            include_history=False,
        ),
    )

    try:
        dataset.linked_service.connect()
        dataset.read()
    except Exception as exc:
        logger.error(f"Error reading cart dataset: {exc.__dict__}")


if __name__ == "__main__":
    main()
