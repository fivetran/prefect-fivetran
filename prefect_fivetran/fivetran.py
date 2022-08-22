"""Module for running Fivetran data syncs."""

import asyncio
from typing import Any, Dict, List, Optional, Tuple, Union

from prefect import task

from prefect_fivetran.client import FivetranClient


@task(
    name="Get Fivetran connector details",
    description="Retrieves details of a Fivetran connector",
    retries=3,
    retry_delay_seconds=10,
)
async def get_connector_info(
    connector_id: str,
    fivetran_client: FivetranClient,
) -> Dict:
    """
    A task to retrieve information about a Fivetran connector.

    Args:
        Fivetran Client: Client for interacting with Fivetran API.
        connector_id: The ID of the connector to use in Prefect.
    Returns:
        The connector data returned by the Fivetran API.
    Example:
        Get status of a Fivetran connector:
        ```python
        from prefect import flow
        from prefect_fivetran.credentials import FivetranCredentials
        from prefect_fivetran.clients import FivetranClient
        from prefect_fivetran.fivetran import get_connector_info
        @flow
        def get_connector_flow():
            fivetran = FivetranClient(
                FivetranCredentials(
                    api_key="my_api_key",
                    api_secret="my_api_secret",
                )
            )
            return get_connector_info(
                fivetran_client=fivetran_client,
                connector_id="my_connector_id",
            )
        get_connector_flow()
        ```"""
    return await fivetran_client.get_connector(connector_id)


@task
def start_fivetran_sync(
    connector_id: str,
    fivetran_client: FivetranClient,
    schedule_type: str = "manual",
) -> Dict:
    """
    Starts a Fivetran connector data sync
    Args:
        connector_id: The id of the Fivetran connector to use in Prefect.
        fivetran_credentials: The credentials to use to authenticate.
    Returns:
        Dict containing the timestamp of the end of the connector's run and its ID.
    Examples:
        Run a Fivetran connector in Prefect
        ```python
        from prefect import flow
        from prefect_fivetran.credentials import FivetranCredentials
        from prefect_fivetran.client import FivetranClient
        from prefect_fivetran.fivetran import start_fivetran_sync
        @flow
        def fivetran_sync_flow():
            fivetran_client = FivetranClient(
                FivetranCredentials(
                    api_key="my_api_key",
                    api_secret="my_api_secret",
                )
            last_sync = start_fivetran_sync(
                connector_id="my_connector_id",
                fivetran_client=fivetran_client
            )
            return result
        fivetran_sync_flow()
        ```
    """
    return fivetran_client.sync(connector_id=connector_id)


@task
async def finish_fivetran_sync(
    connector_id: str,
    fivetran_client: FivetranClient,
    previous_completed_at: pendulum.datetime.DateTime,
    poll_status_every_n_seconds: int = 15,
) -> Dict:
    """
    Wait for the previously started Fivetran connector to finish.

    Args:
        connector_id: ID of the Fivetran connector with which to interact.
        previous_completed_at: Time of the end of the connector's last run
        poll_status_every_n_seconds: Frequency in which Prefect will check status of
            Fivetran connector's sync completion

    Returns:
        Dict containing the timestamp of the end of the connector's run and its ID.
    Examples:
        Run and finish a Fivetran connector in Prefect
        ```python
        from prefect import flow
        from prefect_fivetran.credentials import FivetranCredentials
        from prefect_fivetran.client import FivetranClient
        from prefect_fivetran.fivetran import start_fivetran_sync, finish_fivetran_sync
        @flow
        def fivetran_sync_flow():
            fivetran_client = FivetranClient(
                FivetranCredentials(
                    api_key="my_api_key",
                    api_secret="my_api_secret",
                )
            last_sync = start_fivetran_sync(
                connector_id="my_connector_id",
                fivetran_client=fivetran_client,
            )
            result = finish_fivetran_sync(
                connector_id="my_connector_id",
                fivetran_client=fivetran_client,
                previous_completed_at=last_sync,
                poll_status_every_n_seconds=60,
            return result
        fivetran_sync_flow()
        ```
    """
    loop: bool = True
    while loop:
        connector_status = await get_connector_info.submit(
            connector_id=connector_id,
            fivetran_client=fivetran_client,
        )
        current_details = await connector_status.result()
        succeeded_at = fivetran_client.parse_timestamp(current_details["succeeded_at"])
        failed_at = fivetran_client.parse_timestamp(current_details["failed_at"])
        current_completed_at = succeeded_at if succeeded_at > failed_at else failed_at
        # The only way to tell if a sync failed is to check if its latest failed_at value
        # is greater than then last known "sync completed at" value.
        if failed_at > fivetran_client.parse_timestamp(previous_completed_at):
            raise ValueError(
                'Fivetran sync for connector "{}" failed; please see logs at {}'.format(
                    connector_id,
                    "https://fivetran.com/dashboard/connectors/{}/{}/logs".format(
                        current_details["service"], current_details["schema"]
                    ),
                )
            )
        # Started sync will spend some time in the 'scheduled' state before
        # transitioning to 'syncing'.
        # Capture the transition from 'scheduled' to 'syncing' or 'rescheduled',
        # and then back to 'scheduled' on completion.
        sync_state = current_details["status"]["sync_state"]
        if current_completed_at > fivetran_client.parse_timestamp(
            previous_completed_at
        ):
            loop = False
        else:
            await asyncio.sleep(poll_status_every_n_seconds)


@flow(
    name="Trigger Fivetran connector sync and wait for completion",
    description="Triggers a Fivetran connector to move data and waits for the"
    "connector to complete.",
)
async def start_and_finish_sync_flow(
    connector_id: str,
    fivetran_credentials: FivetranCredentials,
    schedule_type: str = "manual",
    poll_status_every_n_seconds: int = 15,
) -> Dict:
    """
    Flow that triggers a connector sync and waits for the sync to complete.
    Args:
        fivetran_credentials: Credentials for authenticating with Fivetran.
        connector_id: The ID of the Fivetran connector to trigger.
        schedule_type: Connector syncs periodically on Fivetran's schedule ("auto"),
                or whenever called by the API ("manual").
        poll_status_every_n_seconds: Number of seconds to wait in between checks for
            sync completion.
    Returns:
        Dict containing the timestamp of the end of the connector's run and its ID.
    Examples:
        Trigger a dbt Cloud job and wait for completion as a stand alone flow:
        ```python
        import asyncio
        from prefect_fivetran.credentials import FivetranCredentials
        from prefect_fivetran.fivetran import fivetran_sync_flow

        asyncio.run(
            fivetran_sync_flow(
                fivetran_credentials=FivetranCredentials(
                    api_key="my_api_key",
                    api_secret="my_api_secret",
                ),
                connector_id="my_connector_id"
            )
        )
        ```

        Trigger a Fivetran connector sync and wait for completion as a sub-flow:
        ```python
        from prefect import flow
        from prefect_fivetran.credentials import FivetranCredentials
        from prefect_fivetran.fivetran import fivetran_sync_flow
        @flow
        def my_flow():
            ...
            fivetran_result = fivetran_sync_flow(
                fivetran_credentials=FivetranCredentials(
                    api_key="my_api_key",
                    api_secret="my_api_secret",
                ),
                connector_id="my_connector_id"
            )
            ...
        my_flow()
        ```
    """
    logger = get_run_logger()

    fivetran_client = FivetranCredentials.get_fivetran()
    previous_completed_at = fivetran_client.sync_task(
        fivetran_credentials=fivetran_credentials, connector_id=connector_id
    )

    return await fivetran_client.finish_sync.submit(
        connector_id=connector_id,
        previous_completed_at=previous_completed_at,
    )
