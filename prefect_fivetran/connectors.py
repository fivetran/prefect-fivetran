"""Tasks and flows for managing Fivetran connectors"""

import asyncio
from typing import Dict

import pendulum
from prefect import flow, get_run_logger, task

from prefect_fivetran import FivetranCredentials


@task(
    name="Verify Fivetran connector status",
    description="Checks that a Fivetran connector is ready to sync data.",
    retries=0,
)
async def verify_fivetran_connector_status(
    connector_id: str,
    fivetran_credentials: FivetranCredentials,
) -> Dict:
    """
    Ensure that Fivetran connector is ready to sync data.

    Args:
        connector_id: The id of the Fivetran connector to use in Prefect.
        fivetran_credentials: The credentials to use to authenticate.

    Returns:
        The response from the Fivetran API.

    Examples:
        Check a Fivetran connector in Prefect
        ```python
        from prefect import flow
        from prefect_fivetran import FivetranCredentials
        from prefect_fivetran.fivetran import check_fivetran_connector

        @flow
        def example_flow():
            fivetran_credentials = FivetranCredentials(
                    api_key="my_api_key",
                    api_secret="my_api_secret",
            )
            return verify_fivetran_connector_status(
                connector_id="my_connector_id",
                fivetran_credentials=fivetran_credentials,
            )

        example_flow()
        ```
    """
    if not connector_id:
        raise ValueError("Value for parameter `connector_id` must be provided.")
    # Make sure connector configuration has been completed successfully
    # and is not broken.
    async with fivetran_credentials.get_fivetran() as fivetran_client:
        connector_details = (
            await fivetran_client.get_connector(connector_id=connector_id)
        )["data"]
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

        return connector_details


@task(
    name="Set Fivetran connector schedule",
    description="Sets the schedule for a Fivetran connector.",
    retries=0,
)
async def set_fivetran_connector_schedule(
    connector_id: str,
    fivetran_credentials: FivetranCredentials,
    schedule_type: str = "manual",
) -> Dict:
    """
    Take connector off Fivetran's schedule so that it can be controlled
    by Prefect.

    Can also be used to place connector back on Fivetran's schedule
    with schedule_type = "auto".

    Args:
        connector_id: The id of the Fivetran connector to use in Prefect.
        fivetran_credentials: The credentials to use to authenticate.
        schedule_type: Connector syncs periodically on Fivetran's schedule (auto),
                or whenever called by the API (manual).

    Returns:
        The response from the Fivetran API.

    Examples:
        Check a Fivetran connector in Prefect
        ```python
        from prefect import flow
        from prefect_fivetran import FivetranCredentials
        from prefect_fivetran.fivetran import set_fivetran_connector_schedule

        @flow
        def example_flow():
            fivetran_credentials = FivetranCredentials(
                    api_key="my_api_key",
                    api_secret="my_api_secret",
            )
            return set_fivetran_connector_schedule(
                connector_id="my_connector_id",
                fivetran_credentials=fivetran_credentials,
                schedule_type="my_schedule_type",
            )

        fivetran_sync_flow()
        ```
    """
    if schedule_type not in ["manual", "auto"]:
        raise ValueError('schedule_type must be either "manual" or "auto"')

    async with fivetran_credentials.get_fivetran() as fivetran_client:
        connector_details = (
            await fivetran_client.get_connector(connector_id=connector_id)
        )["data"]

        if connector_details["schedule_type"] != schedule_type:
            resp = await fivetran_client.patch_connector(
                connector_id=connector_id,
                data={"schedule_type": schedule_type},
            )
            return resp


@task(
    name="Start Fivetran connector sync",
    description="Starts a Fivetran connector data sync.",
    retries=3,
    retry_delay_seconds=10,
)
async def start_fivetran_connector_sync(
    connector_id: str,
    fivetran_credentials: FivetranCredentials,
) -> Dict:
    """
    Start a Fivetran data sync.

    Args:
        connector_id: The id of the Fivetran connector to use in Prefect.
        fivetran_credentials: The credentials to use to authenticate.

    Returns:
        The timestamp of the end of the connector's last run, or now if it
        has not yet run.

    Examples:
        Check a Fivetran connector in Prefect
        ```python
        from prefect import flow
        from prefect_fivetran import FivetranCredentials
        from prefect_fivetran.fivetran import start_fivetran_connector_sync

        @flow
        def example_flow():
            fivetran_credentials = FivetranCredentials(
                    api_key="my_api_key",
                    api_secret="my_api_secret",
            )
            start_fivetran_connector_sync(
                connector_id="my_connector_id",
                fivetran_credentials=fivetran_credentials,
            )

            example_flow()
        ```
    """
    async with fivetran_credentials.get_fivetran() as fivetran_client:
        connector_details = (
            await fivetran_client.get_connector(connector_id=connector_id)
        )["data"]
        succeeded_at = connector_details["succeeded_at"]
        failed_at = connector_details["failed_at"]

        if connector_details["paused"]:
            await fivetran_client.patch_connector(
                connector_id=connector_id,
                data={"paused": False},
            )

        if succeeded_at is None and failed_at is None:
            succeeded_at = str(pendulum.now())

        last_sync = (
            succeeded_at
            if fivetran_client.parse_timestamp(succeeded_at)
            > fivetran_client.parse_timestamp(failed_at)
            else failed_at
        )
        await fivetran_client.force_connector(connector_id=connector_id)

        return last_sync


@task(
    name="Wait on a Fivetran connector data sync",
    description="Halts execution of flow until Fivetran connector data sync completes",
    retries=3,
    retry_delay_seconds=10,
)
async def wait_for_fivetran_connector_sync(
    connector_id: str,
    fivetran_credentials: FivetranCredentials,
    previous_completed_at: str,
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
        from prefect_fivetran import FivetranCredentials
        from prefect_fivetran.fivetran import start_fivetran_sync, finish_fivetran_sync

        @flow
        def example_flow():
            fivetran_credentials = FivetranCredentials(
                api_key="my_api_key",
                api_secret="my_api_secret",
            )
            return wait_for_fivetran_connector_sync(
                connector_id="my_connector_id",
                fivetran_credentials=fivetran_credentials,
                previous_completed_at=last_sync,
                poll_status_every_n_seconds=60,
            )

        example_flow()
        ```
    """
    logger = get_run_logger()
    loop: bool = True
    async with fivetran_credentials.get_fivetran() as fivetran_client:
        while loop:
            current_details = (
                await fivetran_client.get_connector(connector_id=connector_id)
            )["data"]
            succeeded_at = fivetran_client.parse_timestamp(
                current_details["succeeded_at"]
            )
            failed_at = fivetran_client.parse_timestamp(current_details["failed_at"])
            current_completed_at = (
                succeeded_at if succeeded_at > failed_at else failed_at
            )
            # The only way to tell if a sync failed is to check if its latest failed_at
            # value is greater than then last known "sync completed at" value.
            if failed_at > fivetran_client.parse_timestamp(previous_completed_at):
                raise ValueError(
                    f'Fivetran sync for connector "{connector_id}" failed. '
                    f'Please see logs at https://fivetran.com/dashboard/connectors/{current_details["service"]}/{current_details["schema"]}/logs'  # noqa
                )
            # Started sync will spend some time in the 'scheduled' state before
            # transitioning to 'syncing'.
            # Capture the transition from 'scheduled' to 'syncing' or 'rescheduled',
            # and then back to 'scheduled' on completion.
            sync_state = current_details["status"]["sync_state"]
            logger.info(
                'Connector "{}" current sync_state = {}'.format(
                    connector_id, sync_state
                )
            )

            if current_completed_at > fivetran_client.parse_timestamp(
                previous_completed_at
            ):
                loop = False
            else:
                await asyncio.sleep(poll_status_every_n_seconds)
        return {
            "succeeded_at": succeeded_at.to_iso8601_string(),
            "connector_id": connector_id,
        }


@flow(
    name="Trigger Fivetran connector data sync",
    description="Starts a Fivetran data connector",
    retries=3,
    retry_delay_seconds=10,
)
async def verify_and_start_fivetran_connector_sync(
    connector_id: str,
    fivetran_credentials: FivetranCredentials,
    schedule_type: str = "manual",
) -> Dict:
    """
    Flow that triggers a connector sync.

    Ensures that Fivetran connector is correctly configured and disables connector
    schedule so that Prefect schedule can be used instead.

    Args:
        fivetran_credentials: Credentials for authenticating with Fivetran.
        connector_id: The ID of the Fivetran connector to trigger.
        schedule_type: Connector syncs periodically on Fivetran's schedule ("auto"),
                or whenever called by the API ("manual").

    Returns:
        The timestamp of the end of the connector's last run, or now if it
        has not yet run.

    Examples:
        Trigger a Fivetran data sync:
        ```python
        import asyncio
        from prefect_fivetran import FivetranCredentials
        from prefect_fivetran.fivetran import start_fivetran_sync

        fivetran_credentials = FivetranCredentials(
            api_key="my_api_key",
            api_secret="my_api_secret",
        )
        asyncio.run(
            start_fivetran_sync(
                fivetran_credentials=fivetran_credentials,
                connector_id="my_connector_id",
                schedule_type="my_schedule_type",
            )
        )
        ```
        Trigger a Fivetran connector sync as a sub-flow:
        ```python
        from prefect import flow
        from prefect_fivetran import FivetranCredentials
        from prefect_fivetran.fivetran import start_fivetran_sync
        @flow
        def my_flow():
            ...
            fivetran_credentials = FivetranCredentials(
                api_key="my_api_key",
                api_secret="my_api_secret",
            )
            last_sync = await start_fivetran_sync(
                fivetran_credentials=fivetran_credentials,
                connector_id="my_connector_id",
                schedule_type="my_schedule_type"
            )
            ...
        my_flow()
        ```
    """
    if verify_fivetran_connector_status(
        connector_id=connector_id,
        fivetran_credentials=fivetran_credentials,
    ):
        await set_fivetran_connector_schedule(
            connector_id=connector_id,
            fivetran_credentials=fivetran_credentials,
            schedule_type=schedule_type,
        )
        return await start_fivetran_connector_sync(
            connector_id=connector_id,
            fivetran_credentials=fivetran_credentials,
        )


@flow(
    name="Trigger Fivetran connector sync and wait for completion",
    description="Triggers a Fivetran connector to move data and waits for the"
    "connector to complete.",
)
async def trigger_fivetran_connector_sync_and_wait_for_completion(
    connector_id: str,
    fivetran_credentials: FivetranCredentials,
    schedule_type: str = "manual",
    poll_status_every_n_seconds: int = 30,
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
        Trigger a Fivetran data sync and wait for completion as a stand alone flow:
        ```python
        import asyncio
        from prefect_fivetran import FivetranCredentials
        from prefect_fivetran.fivetran import trigger_fivetran_connector_sync_and_wait_for_completion

        fivetran_credentials = FivetranCredentials(
            api_key="my_api_key",
            api_secret="my_api_secret",
        )
        asyncio.run(
            fivetran_sync_flow(
                fivetran_credentials=fivetran_credentials,
                connector_id="my_connector_id",
                schedule_type="my_schedule_type",
                poll_status_every_n_seconds=30,
            )
        )
        ```
        Trigger a Fivetran connector sync and wait for completion as a sub-flow:
        ```python
        from prefect import flow
        from prefect_fivetran import FivetranCredentials
        from prefect_fivetran.connectors import fivetran_sync_flow

        @flow
        def my_flow():
            ...
            fivetran_credentials = FivetranCredentials(
                api_key="my_api_key",
                api_secret="my_api_secret",
            )
            fivetran_result = await fivetran_sync_flow(
                fivetran_credentials=fivetran_credentials,
                connector_id="my_connector_id",
                schedule_type="my_schedule_type",
                poll_status_every_n_seconds=30,
            )
            ...

        my_flow()
        ```
    """  # noqa
    if await verify_fivetran_connector_status(
        connector_id=connector_id,
        fivetran_credentials=fivetran_credentials,
    ):
        await set_fivetran_connector_schedule(
            connector_id=connector_id,
            fivetran_credentials=fivetran_credentials,
            schedule_type=schedule_type,
        )
        last_sync = await start_fivetran_connector_sync(
            connector_id=connector_id,
            fivetran_credentials=fivetran_credentials,
        )
    return await wait_for_fivetran_connector_sync(
        connector_id=connector_id,
        fivetran_credentials=fivetran_credentials,
        previous_completed_at=last_sync,
        poll_status_every_n_seconds=poll_status_every_n_seconds,
    )
