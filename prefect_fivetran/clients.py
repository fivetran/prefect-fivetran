"""Module containing clients for interacting with the dbt Cloud API"""
import json
from typing import Dict

import pendulum
import requests

from prefect_fivetran import __version__


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

        self.api_user_agent = f"prefect-fivetran/{__version__}"
        headers = {"User-Agent": self.api_user_agent}

        self.session = requests.Session()
        self.session.hooks = {
            "response": lambda r, *args, **kwargs: r.raise_for_status()
        }
        self.session.auth = (api_key, api_secret)
        self.session.headers.update(headers)

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

    def check_connector(self, connector_id: str) -> requests.models.Response:
        """
        Ensure connector exists and is reachable.

        Args:
            connector_id: ID of the Fivetran connector with which to interact.

        Returns:
            The response from the Fivetran API.
        """
        if not connector_id:
            raise ValueError("Value for parameter `connector_id` must be provided.")
        URL_CONNECTOR: str = "https://api.fivetran.com/v1/connectors/{}".format(
            connector_id
        )
        # Make sure connector configuration has been completed successfully
        # and is not broken.
        resp = self.session.get(URL_CONNECTOR)
        connector_details = resp.json()["data"]
        URL_SETUP = "https://fivetran.com/dashboard/connectors/{}/{}/setup".format(
            connector_details["service"], connector_details["schema"]
        )
        setup_state = connector_details["status"]["setup_state"]
        if setup_state != "connected":
            EXC_SETUP: str = (
                'Fivetran connector "{}" not correctly configured, status: {}; '
                + "please complete setup at {}"
            )
            raise ValueError(EXC_SETUP.format(connector_id, setup_state, URL_SETUP))

        return resp

    def set_schedule_type(
        self,
        connector_id: str,
        schedule_type: str = "manual",
    ) -> requests.models.Response:
        """
        Take connector off Fivetran's schedule so that it can be controlled
        by Prefect.

        Can also be used to place connector back on Fivetran's schedule
        with schedule_type = "auto".

        Args:
            connector_id: ID of the Fivetran connector with which to interact.
            schedule_type: Connector syncs periodically on Fivetran's schedule (auto),
                or whenever called by the API (manual).

        Returns:
            The response from the Fivetran API.
        """
        if schedule_type not in ["manual", "auto"]:
            raise ValueError('schedule_type must be either "manual" or "auto"')

        URL_CONNECTOR: str = "https://api.fivetran.com/v1/connectors/{}".format(
            connector_id
        )
        resp = self.session.get(URL_CONNECTOR)
        connector_details = resp.json()["data"]

        if connector_details["schedule_type"] != schedule_type:
            resp = self.session.patch(
                URL_CONNECTOR,
                json={"schedule_type": schedule_type},
            )
        return resp

    def force_sync(
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
        URL_CONNECTOR: str = "https://api.fivetran.com/v1/connectors/{}".format(
            connector_id
        )

        resp = self.session.get(URL_CONNECTOR)
        connector_details = resp.json()["data"]
        succeeded_at = connector_details["succeeded_at"]
        failed_at = connector_details["failed_at"]

        if connector_details["paused"]:
            self.session.patch(
                URL_CONNECTOR,
                data=json.dumps({"paused": False}),
                headers={"Content-Type": "application/json;version=2"},
            )

        if succeeded_at is None and failed_at is None:
            succeeded_at = str(pendulum.now())

        last_sync = (
            succeeded_at
            if self.parse_timestamp(succeeded_at) > self.parse_timestamp(failed_at)
            else failed_at
        )
        self.session.post(
            "https://api.fivetran.com/v1/connectors/" + connector_id + "/force"
        )

        return last_sync

    def get_connector(
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

        return self.session.get(URL_CONNECTOR).json()["data"]

    def sync(
        self,
        connector_id: str,
        schedule_type: str = "manual",
        poll_status_every_n_seconds: int = 15,
    ) -> str:
        """
        Run a Fivetran connector data sync and wait for its completion.

        Args:
            connector_id: ID of the Fivetran connector with which to interact.
            schedule_type: Connector syncs periodically on Fivetran's schedule (auto),
                or whenever called by the API (manual).
            poll_status_every_n_seconds: Frequency in which Prefect will check status of
                Fivetran connector's sync completion
        Returns:
            The timestamp of the end of the connector's last run as a string, or now if
            it has not yet run.
        """
        if self.check_connector(connector_id):
            self.set_schedule_type(connector_id, schedule_type)
            return self.force_sync(connector_id)
