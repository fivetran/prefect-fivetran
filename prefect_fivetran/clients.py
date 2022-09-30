"""Clients for interacting with the Fivetran API"""

from typing import Dict

import pendulum
import prefect
from httpx import AsyncClient


class FivetranClient:
    """
    Client for interacting with the Fivetran API.
    Args:
        api_key: API key to authenticate with the Fivetran API.
        api_secret: API secret to authenticate with the Fivetran API.
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
    ):
        if not api_key:
            raise ValueError("Value for parameter `api_key` must be provided.")
        if not api_secret:
            raise ValueError("Value for parameter `api_secret` must be provided.")

        self._closed = False
        self._started = False

        self.client = AsyncClient(
            headers={"user-agent": f"prefect-{prefect.__version__}"},
        )

        self.client.hooks = {
            "response": lambda r, *args, **kwargs: r.raise_for_status()
        }
        self.client.auth = (api_key, api_secret)

    def parse_timestamp(self, api_time: str):
        """
        Returns either the pendulum-parsed actual timestamp or
        a very out-of-date timestamp if not set
        """
        return (
            pendulum.parse(api_time)
            if api_time is not None
            else pendulum.from_timestamp(-1)
        )

    async def get_connector(
        self,
        connector_id: str,
    ) -> Dict:
        """
        Retrieve Fivetran connector to details.
        Args:
            connector_id: ID of the Fivetran connector with which to interact.
        Returns:
            Dict containing the details of a Fivetran connector
        """
        URL_CONNECTOR: str = "https://api.fivetran.com/v1/connectors/{}".format(
            connector_id
        )

        return (await self.client.get(URL_CONNECTOR)).json()

    async def patch_connector(
        self,
        connector_id: str,
        data: Dict,
    ) -> Dict:
        """
        Alter Fivetran connector metadata.
        Args:
            connector_id: ID of the Fivetran connector with which to interact.
            data: Key Value pair of connector metadata to be changed.
        Returns:
            Dict containing the details of a Fivetran connector
        """
        URL_CONNECTOR: str = "https://api.fivetran.com/v1/connectors/{}".format(
            connector_id
        )

        return (
            await self.client.patch(
                URL_CONNECTOR,
                json=data,
                headers={"Content-Type": "application/json;version=2"},
            )
        ).json()

    async def force_connector(
        self,
        connector_id: str,
    ) -> str:
        """
        Start a Fivetran data sync
        Args:
            connector_id: ID of the Fivetran connector with which to interact.
        Returns:
            The timestamp of the end of the connector's last run, or now if it
            has not yet run.
        """

        return (
            await self.client.post(
                "https://api.fivetran.com/v1/connectors/" + connector_id + "/force"
            )
        ).json()

    async def __aenter__(self):
        if self._closed:
            raise RuntimeError(
                "The client cannot be started again after it has been closed."
            )
        if self._started:
            raise RuntimeError("The client cannot be started more than once.")

        self._started = True

        return self

    async def __aexit__(self, *exc):
        self._closed = True
        await self.client.__aexit__()
