"""Module containing clients for interacting with the dbt Cloud API"""
import json
import time

import requests
import pendulum

from typing import Any, Dict, Optional

import prefect
from httpx import AsyncClient, Response


class FivetranClient:

api_user_agent = "prefect/1.0.1"

    def __init__(
        self, 
        api_key: str,
        api_secret: str,
    ):
        self.api_key = api_key
        self.api_secret = api_secret

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

    def check_connector(str: connector_id,fivetran_credentials: "FivetranCredentials",):

    def set_schedule_type(
        connector_id: str, 
        fivetran_credentials: "FivetranCredentials",
        schedule_type: str = "manual"
    ):


    def force_sync(connector_id: str, fivetran_credentials: "FivetranCredentials",):

    async def finish_sync(connector_id: str, fivetran_credentials: "FivetranCredentials", ):

    def get_last_sync(connector_id: str, fivetran_credentials: "FivetranCredentials",):

    async def sync(
        connector_id: str, 
        fivetran_credentials: "FivetranCredentials",
        schedule_type: str = "manual",
        poll_status_every_n_seconds: int = 15,
    ):
    """

    """
        check_connector(connector_id)
        set_schedule_type(connector_id, schedule_type)
        force_sync(connector_id)
        finish_sync(connector_id, get_last_sync(connector_id)
