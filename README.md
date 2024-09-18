# prefect-fivetran

## Welcome!

Prefect integrations with Fivetran

### Disclaimer:
Please note that Fivetran no longer officially supports or maintains this repository. If a customer needs to add or modify something here, they will need to make a fork and work with it on their own. This repository is being left public and accessible for the benefit of customers who have built services around this product. Any edits or modifications are the sole responsibility of the modifying party.

## Getting Started

### Python setup

Requires an installation of Python 3.7+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://orion-docs.prefect.io/).

### Installation

Install `prefect-fivetran` with `pip`:

```bash
pip install prefect-fivetran
```

Then, register the blocks in this collection to [view them in Prefect Cloud](https://orion-docs.prefect.io/ui/blocks/):

```bash
prefect block register -m prefect_fivetran
```

Note, to use the `load` method on Blocks, you must already have a block document [saved through code](https://orion-docs.prefect.io/concepts/blocks/#saving-blocks) or [saved through the UI](https://orion-docs.prefect.io/ui/blocks/).


### Write and run a flow

```python
from prefect import flow
from prefect_fivetran import FivetranCredentials
from prefect_fivetran.connectors import trigger_fivetran_connector_sync_and_wait_for_completion

@flow
def my_flow():
    ...
    fivetran_credentials = FivetranCredentials(
        api_key="my_api_key",
        api_secret="my_api_secret",
    )
    fivetran_result = await trigger_fivetran_connector_sync_and_wait_for_completion(
        fivetran_credentials=fivetran_credentials,
        connector_id="my_connector_id",
        poll_status_every_n_seconds=30,
    )
    ...

my_flow()

```

## Resources

If you encounter any bugs while using `prefect-fivetran`, feel free to open an issue in the [prefect-fivetran](https://github.com/pubchimps/prefect-fivetran) repository.

If you have any questions or issues while using `prefect-fivetran`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

## Development

If you'd like to install a version of `prefect-fivetran` for development, clone the repository and perform an editable install with `pip`:

```bash
git clone https://github.com/pubchimps/prefect-fivetran.git

cd prefect-fivetran/

pip install -e ".[dev]"

# Install linting pre-commit hooks
pre-commit install
```
