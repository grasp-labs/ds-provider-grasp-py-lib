"""
**File:** ``03_dataset_file_read.py``
**Region:** ``examples/03_dataset_file_read``

Example 03: Read data from a Grasp File dataset using GraspFileDataset.

This example demonstrates how to:
- Create a Grasp File dataset
- Read file metadata and optional content from the Grasp File API
"""

from __future__ import annotations

import logging
import uuid

from ds_common_logger_py_lib import Logger
from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
from ds_protocol_http_py_lib.enums import AuthType
from ds_resource_plugin_py_lib.common.resource.dataset.storage_format import DatasetStorageFormatType
from ds_resource_plugin_py_lib.common.serde.deserialize.pandas import PandasDeserializer

from ds_provider_grasp_py_lib.dataset.file import (
    GraspFileDataset,
    GraspFileDatasetSettings,
    ReadSettings,
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
        deserializer=PandasDeserializer(format=DatasetStorageFormatType.JSON),
        id=uuid.uuid4(),
        name="file-dataset",
        version="1.0.0",
        linked_service=HttpLinkedService(
            id=uuid.uuid4(),
            name="http-linked-service",
            version="1.0.0",
            settings=HttpLinkedServiceSettings(
                host=None,
                auth_type=AuthType.NO_AUTH,
                headers={
                    "Cookie": "access_token=<ACCESS_TOKEN>",
                }
            ),
        ),
        settings=GraspFileDatasetSettings(
            url="https://dev.aic-project.com/api/file/file/",
            read=ReadSettings(download_file=True, limit=2),
        ),
    )

    dataset.linked_service.connect()
    dataset.read()
    logger.info(f"Successfully performed read operation")
    print(dataset.output)


if __name__ == "__main__":
    main()
