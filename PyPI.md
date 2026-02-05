# ds-provider-grasp-py-lib

A Python package from the ds-common library collection.

## Installation

Install the package using pip:

```bash
pip install ds-provider-grasp-py-lib
```

Or using uv (recommended):

```bash
uv pip install ds-provider-grasp-py-lib
```

## Quick Start

```python
from ds_provider_grasp_py_lib import __version__

print(f"ds-provider-grasp-py-lib version: {__version__}")
```

## Features

- **GRASP Provider**: Provider for the GRASP platform
- **AWS Provider**: Provider for the AWS platform

## Usage

```python
import uuid
import logging
from os import environ

from ds_common_logger_py_lib import Logger
from ds_provider_aws_py_lib.linked_service.aws import AWSLinkedService, AWSLinkedServiceSettings
from ds_provider_grasp_py_lib.dataset.cart import GraspCartDataset, GraspCartDatasetSettings
Logger.configure(level=logging.DEBUG)
logger = Logger.get_logger(__name__)


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

dataset.linked_service.connect()
environ["TENANT_ID"] = "****************"
dataset.read()
logger.debug(f"Dataset: {dataset.output}")
```

## Requirements

- Python 3.11 or higher

## Documentation

Full documentation is available at:

- [GitHub Repository](https://github.com/grasp-labs/ds-provider-grasp-py-lib)
- [Documentation Site](https://grasp-labs.github.io/ds-provider-grasp-py-lib/)

## Development

To contribute or set up a development environment:

```bash
# Clone the repository
git clone https://github.com/grasp-labs/ds-provider-grasp-py-lib.git
cd ds-provider-grasp-py-lib

# Install development dependencies
uv sync --all-extras --dev

# Run tests
make test
```

See the [README][readme] for more information.

[readme]: https://github.com/grasp-labs/ds-provider-grasp-py-lib#readme

## License

This package is licensed under the Apache License 2.0.
See the [LICENSE-APACHE](https://github.com/grasp-labs/ds-provider-grasp-py-lib/blob/main/LICENSE-APACHE)
file for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/grasp-labs/ds-provider-grasp-py-lib/issues)
- **Releases**: [GitHub Releases](https://github.com/grasp-labs/ds-provider-grasp-py-lib/releases)
