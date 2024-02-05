# prefect-fivetran

<p align="center">
    <a href="https://pypi.python.org/pypi/prefect-fivetran/" alt="PyPI version">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/prefect-fivetran?color=26272B&labelColor=090422"></a>
    <a href="https://github.com/fivetran/prefect-fivetran/" alt="Stars">
        <img src="https://img.shields.io/github/stars/fivetran/prefect-fivetran?color=26272B&labelColor=090422"2" /></a>
    <a href="https://pepy.tech/badge/prefect-fivetran/" alt="Downloads">
        <img src="https://img.shields.io/pypi/dm/prefect-fivetran?color=26272B&labelColor=090422"" /></a>
    <a href="https://github.com/fivetran/prefect-fivetran/pulse" alt="Activity">
        <img src="https://img.shields.io/github/commit-activity/m/fivetran/prefect-fivetran?color=26272B&labelColor=090422"2" /></a>
    <br>
</p>

## Welcome!

Prefect integrations with Fivetran

## Getting Started

### Python setup

Requires an installation of Python 3.8+.

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
