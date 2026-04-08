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
                    "Cookie": "access_token=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0eXAiOiJhY2Nlc3MiLCJleHAiOjE3NzU2NTMyODQsImlhdCI6MTc3NTY0OTY4NCwianRpIjoiYmRiMWQ3OTg4OGNhNDE3ZWEzZTZjMjk2M2IzOWJhMGIiLCJzdWIiOiJqYWt1Yi1ncmFzcGRlbW9AdGVzdC5jb20iLCJpc3MiOiJodHRwczovL2F1dGgtZGV2LmdyYXNwLWRhYXMuY29tIiwiYXVkIjpbImh0dHBzOi8vZ3Jhc3AtZGFhcy5jb20iXSwibmJmIjoxNzc1NjQ5Njg0LCJ2ZXIiOiIyLjAuMCIsImNscyI6InVzZXIiLCJyc2MiOiJiOWI3OGU1OS03ZWUyLTQ2NTItYTg2NS1kODY4YzRiNmI0NzY6R3Jhc3AgRGVtbyIsInJvbCI6W119.h43XZLiMkO5Qu6fJ3gGbt7Zpgq1sQGo96A5mu6F68tVhjLuvrNYK02i8L6Mkg5Vd0jyEtZqj5H3cQEs1fVV6THzA7cTPDTiBaWPXITS2l49ELHtzh_1zDpeRxo39YPorCj-DUxCqeJCMNZWOUt1PWKadZfWSdwOXG-3CYUhzOgor51wa2ZCPXbmkB2T705znBw0491r2q1VhuWtCs72yUhCldgxGGDDJs0KLm4QPGLc3o5FBOzukTJIAWcTlsiTLcOC0NRgADRj4BwUp94paQWrrYLj3slXRVIfq6dzNyAIBoNrDOfIQBPqFBlA9arN7uTaQQ9IJbquXQQbUeXQSMA; refresh_token=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0eXAiOiJyZWZyZXNoIiwiZXhwIjoxNzc1NzM2MDg0LCJpYXQiOjE3NzU2NDk2ODQsImp0aSI6ImU2OWRhNmUxLWU4ZGEtNDIyMC04NmQ0LWQzNmNkMTEyNTU2YSIsInN1YiI6Impha3ViLWdyYXNwZGVtb0B0ZXN0LmNvbSIsImlzcyI6Imh0dHBzOi8vYXV0aC1kZXYuZ3Jhc3AtZGFhcy5jb20iLCJhdWQiOlsiaHR0cHM6Ly9ncmFzcC1kYWFzLmNvbSJdLCJuYmYiOjE3NzU2NDk2ODQsInZlciI6IjIuMC4wIiwiY2xzIjoidXNlciIsInJzYyI6ImI5Yjc4ZTU5LTdlZTItNDY1Mi1hODY1LWQ4NjhjNGI2YjQ3NjpHcmFzcCBEZW1vIiwicm9sIjpbXX0.JXNJ2zi3Cg5hNG3dnLX8sArtLWT_Of8V6ryUDfYus_sElFVxiSmFj7E1akAezoFve4bQt879rn7gWY_gqqEpmytZjstdc6GcKnRYUIHMdjnlrk5ESad1VOUs1Q1gNBvdORdQo7x-l3SoTgQOnKqfOWVFVQK_FNrRa7n2nMrlXr1JlMc9IaH4CgsBp1xbZrtcG3FQ4_gClNezruDnh-yZskH430H2YYXdzZfdBeaZ-I0goFRnuawBY1pH93U1WUAvjqzuojN8EWOBvIdTgF_TlLJWfybn5xfQocC_ivHHxNeVpSIPafXv5NkJFPPTfi1B1TalO3ARUmOuzJ7iX0HQLQ",
                }
            ),
        ),
        settings=GraspFileDatasetSettings(
            read=ReadSettings(download_file=True, limit=2),
        ),
    )

    dataset.linked_service.connect()
    dataset.read()
    logger.info(f"Successfully performed read operation")
    print(dataset.output)


if __name__ == "__main__":
    main()
