"""Module containing clients for interacting with the dbt Cloud API"""
import json
import time

import requests
import pendulum

from typing import Any, Dict, Optional

import prefect
from httpx import AsyncClient, Response


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
        if not self.api_key:
            raise ValueError("Value for parameter `api_key` must be provided.")
        if not self.api_secret:
            raise ValueError("Value for parameter `api_secret` must be provided.")

        self.api_user_agent = "prefect-collection/1.0.0"
        headers = {"User-Agent": self.api_user_agent}

        self.session = requests.Session()
        self.session.hooks = {
            "response": lambda r, *args, **kwargs: r.raise_for_status()
        }
        self.session.auth = (api_key, api_secret)
        self.session.headers = headers

    def parse_timestamp(api_time: str):
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
        if not self.connector_id:
            raise ValueError("Value for parameter `connector_id` must be provided.")
        URL_CONNECTOR: str = "https://api.fivetran.com/v1/connectors/{}".format(
            connector_id
        )
        # Make sure connector configuration has been completed successfully and is not broken.
        resp = session.get(URL_CONNECTOR)
        connector_details = resp.json()["data"]
        URL_LOGS = "https://fivetran.com/dashboard/connectors/{}/{}/logs".format(
            connector_details["service"], connector_details["schema"]
        )
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
        Take connector off Fivetran's schedule so that it can be controlled in Prefect.
        Can also be used to place connector back on Fivetran's schedule (schedule_type = "auto").

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
                data=json.dumps({"schedule_type": schedule_type}),
                headers={"Content-Type": "application/json;version=2"},
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
            The timestamp of the end of the connector's last run, or now if it has not yet run.
        """
        URL_CONNECTOR: str = "https://api.fivetran.com/v1/connectors/{}".format(
            connector_id
        )

        resp = self.session.get(URL_CONNECTOR)
        connector_details = resp.json()["data"]
        succeeded_at = connector_details["succeeded_at"]
        failed_at = connector_details["failed_at"]

        if connector_details["paused"] == True:
            self.session.patch(
                URL_CONNECTOR,
                data=json.dumps({"paused": False}),
                headers={"Content-Type": "application/json;version=2"},
            )

        if succeeded_at == None and failed_at == None:
            succeeded_at = str(pendulum.now())

        last_sync = (
            succeeded_at
            if self._parse_timestamp(succeeded_at) > self._parse_timestamp(failed_at)
            else failed_at
        )
        self.session.post(
            "https://api.fivetran.com/v1/connectors/" + connector_id + "/force"
        )

        return last_sync

    def finish_sync(
        self,
        connector_id: str,
        previous_completed_at: pendulum.datetime.DateTime,
        poll_status_every_n_seconds: int = 15,
    ) -> dict:
        """
        Wait for the previously started Fivetran connector to finish.

        Args:
            connector_id: ID of the Fivetran connector with which to interact.
            previous_completed_at: Time of the end of the connector's last run
            poll_status_every_n_seconds: Frequency in which Prefect will check status of
                Fivetran connector's sync completion

        Returns:
            Dict containing the timestamp of the end of the connector's run and its ID.
        """
        URL_CONNECTOR: str = "https://api.fivetran.com/v1/connectors/{}".format(
            connector_id
        )

        while loop:
            resp = self.session.get(URL_CONNECTOR)
            current_details = resp.json()["data"]
            # Failsafe, in case we missed a state transition – it is possible with a long enough
            # `poll_status_every_n_seconds` we could completely miss the 'syncing' state
            succeeded_at = parse_timestamp(current_details["succeeded_at"])
            failed_at = parse_timestamp(current_details["failed_at"])
            current_completed_at = (
                succeeded_at if succeeded_at > failed_at else failed_at
            )
            # The only way to tell if a sync failed is to check if its latest failed_at value
            # is greater than then last known "sync completed at" value.
            if failed_at > parse_timestamp(previous_completed_at):
                raise ValueError(
                    'Fivetran sync for connector "{}" failed; please see logs at {}'.format(
                        connector_id, URL_LOGS
                    )
                )
            # Started sync will spend some time in the 'scheduled' state before
            # transitioning to 'syncing'.
            # Capture the transition from 'scheduled' to 'syncing' or 'rescheduled',
            # and then back to 'scheduled' on completion.
            sync_state = current_details["status"]["sync_state"]
            self.logger.info(
                'Connector "{}" current sync_state = {}'.format(
                    connector_id, sync_state
                )
            )
            if current_completed_at > parse_timestamp(previous_completed_at):
                loop = False
            else:
                time.sleep(poll_status_every_n_seconds)

        return {
            "succeeded_at": succeeded_at.to_iso8601_string(),
            "connector_id": connector_id,
        }

    async def sync(
        self,
        connector_id: str,
        schedule_type: str = "manual",
        poll_status_every_n_seconds: int = 15,
    ) -> dict:
        """
        Run a Fivetran connector data sync and wait for its completion.

        Args:
            connector_id: ID of the Fivetran connector with which to interact.
            schedule_type: Connector syncs periodically on Fivetran's schedule (auto),
                or whenever called by the API (manual).
            poll_status_every_n_seconds: Frequency in which Prefect will check status of
                Fivetran connector's sync completion
        Returns:
            Dict containing the timestamp of the end of the connector's run and its ID.
        """
        if check_connector(connector_id):
            set_schedule_type(connector_id, schedule_type)
            previous_completed_at = force_sync(connector_id)
            return await finish_sync(connector_id, previous_completed_at)
