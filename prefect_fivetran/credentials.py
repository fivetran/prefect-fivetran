"""Module containing credentials for interacting with Fivetran"""
from prefect.blocks.core import Block
from pydantic import SecretStr

from prefect_fivetran.client import FivetranClient


class FivetranCredentials(Block):
    """
    Credentials block for credential use across Fivetran tasks and flows.
    Args:
        api_key (SecretStr): [Fivetran API KEY](
            https://fivetran.com/docs/rest-api/faq/access-rest-api)
        api_secret (SecretStr): Fivetran API SECRET
    Examples:
        Load stored Fivetran credentials:
        ```python
        from prefect_fivetran import FivetranCredentials
        fivetran_credentials = FivetranCredentials.load("BLOCK_NAME")
        ```
        Use FivetranCredentials instance to trigger a Fivetran sync:
        ```python
        from prefect_fivetran import FivetranCredentials
        credentials = FivetranCredentials(api_key="my_api_key", api_secret="my_api_secret")
        async with fivetran_credentials.get_fivetran() as fivetran:
            fivetran.sync(connector_id="my_connector_id")
        ```
        Load saved Fivetran credentials within a flow:
        ```python
        from prefect import flow
        from prefect_fivetran import FivetranCredentials
        from prefect_fivetran import sync
        @flow
        def fivetran_sync_flow():
            credentials = FivetranCredentials.load("my-fivetran-credentials")
            trigger_dbt_cloud_job_run(fivetran_credentials=credentials, connector_id="my_connector_id")
        fivetran_sync_flow()
        ```
    """

    _block_type_name = "Fivetran Credentials"

    api_key: SecretStr
    api_secret: SecretStr

    def get_fivetran(self):
        """
        Returns api_key and api_secret for Fivetran object
        """
        return FivetranClient(
            api_key=self.api_key.get_secret_value(),
            api_secret=self.api_secret.get_secret_value(),
        )
