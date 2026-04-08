"""
**File:** ``03_dataset_file_read.py``
**Region:** ``examples/03_dataset_file_read``

Example 03: Read data from a Grasp File dataset using GraspFileDataset.

This example demonstrates how to:
- Create a Grasp File dataset
- Read data from a file in S3
"""

from __future__ import annotations

import logging
import uuid

from ds_common_logger_py_lib import Logger
from ds_provider_aws_py_lib.linked_service.aws import (
    AWSLinkedService,
    AWSLinkedServiceSettings,
)
from ds_resource_plugin_py_lib.common.resource.dataset import DatasetStorageFormatType

from ds_provider_grasp_py_lib.dataset.file import (
    GraspFileDataset,
    GraspFileDatasetSettings,
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
    """Main function demonstrating Grasp File dataset read operation."""
    dataset = GraspFileDataset(
        id=uuid.uuid4(),
        name="file-dataset",
        version="1.0.0",
        linked_service=AWSLinkedService(
            id=uuid.uuid4(),
            name="aws-linked-service",
            version="1.0.0",
            settings=AWSLinkedServiceSettings(
                access_key_id="",
                access_key_secret="",
                region="eu-north-1",
                account_id="",
            ),
        ),
        settings=GraspFileDatasetSettings(
            s3_path="s3://your-bucket/path/to/file.json",
            format=DatasetStorageFormatType.JSON,
        ),
    )

    try:
        dataset.linked_service.connect()
        dataset.read()
        logger.info(f"Successfully read {len(dataset.output)} rows from {dataset.settings.s3_path}")
    except Exception as exc:
        logger.error(f"Error reading file dataset: {exc.__dict__}")


if __name__ == "__main__":
    main()
