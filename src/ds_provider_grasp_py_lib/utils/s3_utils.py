"""
**File:** ``s3_utils.py``
**Region:** ``ds_provider_grasp_py_lib/utils/s3_utils``
S3 utilities

This module implements utilities for S3.

Example:
    >>> get_bucket_name()
    'daas-service-dev'
"""

import os

BUILDING_MODE = os.environ.get("BUILDING_MODE", "dev")


def get_bucket_name() -> str:
    """
    Get bucket name for current stage.
    Returns:
        str: The bucket name.
    """
    bucket = {
        "dev": "daas-service-dev",
        "prod": "daas-service-prod",
    }
    return bucket[BUILDING_MODE]
