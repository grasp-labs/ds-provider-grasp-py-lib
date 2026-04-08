"""
**File:** ``04_dataset_file_create.py``
**Region:** ``examples/04_dataset_file_create``

Example 04: Create a file with GraspFileDataset via the Grasp File API.

This example demonstrates how to:
- Create a Grasp File dataset
- Create a file record and upload content through the API
"""

from __future__ import annotations

import logging
import uuid

import pandas as pd
from ds_common_logger_py_lib import Logger
from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
from ds_protocol_http_py_lib.enums import AuthType
from ds_provider_grasp_py_lib.dataset.file import (
    CreateSettings,
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
    """Main function demonstrating Grasp File dataset create operation."""
    dataset = GraspFileDataset(
        id=uuid.uuid4(),
        name="file-dataset",
        version="1.0.0",
        linked_service=HttpLinkedService(
            id=uuid.uuid4(),
            name="http-linked-service",
            version="1.0.0",
            settings=HttpLinkedServiceSettings(
                host="https://dev.aic-project.com/api/file/",
                auth_type=AuthType.NO_AUTH,
                headers={
                    "Cookie": "access_token=<ACCESS_TOKEN>",
                }
            ),
        ),
        settings=GraspFileDatasetSettings(
            create=CreateSettings(
                acl={
                    "owners": [
                        "jakub-graspdemo@test.com"
                    ],
                    "viewers": []
                },
                description="test example4",
                file_path="test4",
                version="v1.0.0",
            )
        ),
    )
    dataset.input = pd.DataFrame(
       [{"test": "4"}]
    )
    dataset.linked_service.connect()
    dataset.create()
    logger.info(f"Successfully performed create operation")
    print(dataset.output)


if __name__ == "__main__":
    main()
