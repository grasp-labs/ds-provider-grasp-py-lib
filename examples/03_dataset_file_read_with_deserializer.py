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
                    "Cookie": "access_token=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0eXAiOiJhY2Nlc3MiLCJleHAiOjE3NzYyNjA2OTAsImlhdCI6MTc3NjI1NzA5MCwianRpIjoiYTI4Y2Q1M2ZiYjNjNDJkYTkwNmY5Y2M4ODhiZGY2ZGYiLCJzdWIiOiJqYWt1Yi1ncmFzcGRlbW9AdGVzdC5jb20iLCJpc3MiOiJodHRwczovL2F1dGgtZGV2LmdyYXNwLWRhYXMuY29tIiwiYXVkIjpbImh0dHBzOi8vZ3Jhc3AtZGFhcy5jb20iXSwibmJmIjoxNzc2MjU3MDkwLCJ2ZXIiOiIyLjAuMCIsImNscyI6InVzZXIiLCJyc2MiOiJiOWI3OGU1OS03ZWUyLTQ2NTItYTg2NS1kODY4YzRiNmI0NzY6R3Jhc3AgRGVtbyIsInJvbCI6W119.KKysmYueRZyF_5vxCOfm73vANwjnyWxdVx4acu2nswICr9ZB1ylnclKiHZGWUTgw-9lV_saeU4rk7iAMsJVbqvHj-7r_t1KRpo4Fb1KZfojIcf1acvR6NZI-5QWGKQOtR3yVvytx2CTrwo7yXriFw004AdhqTK6rjA89Wu8o5H4PxDj6qqJGoZMXG6K7n3zgPQJMGS04wg4A2DriYwWSW-TU-JIy7lyLYl8RDiZy7BZbGtcV5c31-0bVUEwL5E7NtzGiebogcXlBUxO5Lelld3bUrS4cIaMjy79bYmnsRM8NojmwyQHw0HFxNLXzMM017de9bnOcmAnV7l3-b67Y2A; refresh_token=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0eXAiOiJyZWZyZXNoIiwiZXhwIjoxNzc2MzQzNDkwLCJpYXQiOjE3NzYyNTcwOTAsImp0aSI6IjcxNzEyYmRlLWRiN2ItNGRiZC1iYTQ3LTljYmI4YzcyMjRhYiIsInN1YiI6Impha3ViLWdyYXNwZGVtb0B0ZXN0LmNvbSIsImlzcyI6Imh0dHBzOi8vYXV0aC1kZXYuZ3Jhc3AtZGFhcy5jb20iLCJhdWQiOlsiaHR0cHM6Ly9ncmFzcC1kYWFzLmNvbSJdLCJuYmYiOjE3NzYyNTcwOTAsInZlciI6IjIuMC4wIiwiY2xzIjoidXNlciIsInJzYyI6ImI5Yjc4ZTU5LTdlZTItNDY1Mi1hODY1LWQ4NjhjNGI2YjQ3NjpHcmFzcCBEZW1vIiwicm9sIjpbXX0.i-w4NQKGI5RJmCQukrRIpk_A4QaOi5yT0vF2TZF4nk7hDH3-R-zg-egsNZ8xzMmBE0ssQGScll2y0lol5NneJSQEP-_QJIgjyYfNYPeLAVWgb9mETY4J1MPtQbds4hYmnarb_fstb_D5VLroxDSF0_Qxis8_1ESM62VWEkiqD-AhFy5h--e5RIhUzrDOeBLcvY1lx9BUbvXr8dv3BZhbY9mrnJzVjE3zDIhjueqVk-lPpvVW7VdYVDgB-ZrRUEuA3UE44TNag0_cwmf7nzLCZTxN5brdkSabyKzbSBImyKQ2F0RBgXCBXf2iCbt_g8PlK4fBQuwbPj-svioubY4akw",
                }
            ),
        ),
        settings=GraspFileDatasetSettings(
            url="https://dev.aic-project.com/api/file/file/",
            read=ReadSettings(download_file=True, limit=3),
        ),
    )

    dataset.linked_service.connect()
    dataset.read()
    logger.info(f"Successfully performed read operation")
    print(dataset.output)


if __name__ == "__main__":
    main()
