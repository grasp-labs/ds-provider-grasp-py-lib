"""
**File:** ``04_dataset_file_create_from_input.py``
**Region:** ``examples/04_dataset_file_create_from_input``

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
from ds_resource_plugin_py_lib.common.resource.dataset import DatasetStorageFormatType
from ds_resource_plugin_py_lib.common.serde.serialize import PandasSerializer

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
    # Example B: create from dataset.input + serializer.
    # Keep settings.create.content unset in this flow so payload comes from input.
    dataset = GraspFileDataset(
        # Serializer is required when dataset.input is used as payload source.
        # `orient=records` gives a JSON array like [{"test":"9"}].
        serializer=PandasSerializer(format=DatasetStorageFormatType.JSON, kwargs={"orient": "records"}),
        # serializer must be set when running create from input
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
            create=CreateSettings(
                acl={
                    "owners": [
                        "test-graspdemo@test.com"
                    ],
                    "viewers": []
                },
                description="test example9",
                file_path="test9",
                version="v1.0.0",
                # Leave `content` unset to use serialized dataset.input payload.
                # If `content` is set, it overrides dataset.input.
            )
        ),
    )
    # Payload source for this example: dataset.input (serialized above).
    dataset.input = pd.DataFrame([{"test": "9"}])
    dataset.linked_service.connect()
    dataset.create()
    logger.info(f"Successfully performed create operation")
    print(dataset.output)


if __name__ == "__main__":
    main()
