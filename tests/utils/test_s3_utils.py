"""
**File:** ``test_s3_utils.py``
**Region:** ``tests/utils/test_s3_utils``

S3 utilities tests.

Covers:
- get_bucket_name returns correct bucket based on BUILDING_MODE environment variable.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from ds_provider_grasp_py_lib.utils import s3_utils


def test_get_bucket_name_dev_mode() -> None:
    """
    It returns dev bucket when BUILDING_MODE is 'dev'.
    """
    with patch.object(s3_utils, "BUILDING_MODE", "dev"):
        assert s3_utils.get_bucket_name() == "daas-service-dev"


def test_get_bucket_name_prod_mode() -> None:
    """
    It returns prod bucket when BUILDING_MODE is 'prod'.
    """
    with patch.object(s3_utils, "BUILDING_MODE", "prod"):
        assert s3_utils.get_bucket_name() == "daas-service-prod"


def test_get_bucket_name_invalid_mode_raises_key_error() -> None:
    """
    It raises KeyError when BUILDING_MODE is an invalid value.
    """
    with (
        patch.object(s3_utils, "BUILDING_MODE", "invalid"),
        pytest.raises(KeyError),
    ):
        s3_utils.get_bucket_name()
