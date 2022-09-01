import pendulum
import responses
from responses import matchers
from responses.registries import OrderedRegistry

from prefect_fivetran import __version__
from prefect_fivetran.credentials import FivetranCredentials
from prefect_fivetran.fivetran import fivetran_sync_flow
from tests.mocked_reponses import (
    GET_CONNECTION_MOCK_RESPONSE,
    UPDATE_CONNECTION_MOCK_RESPONSE,
)

HEADER_MATCHER = matchers.header_matcher(
    {
        "Authorization": "Basic QVBJX0tFWTpBUElfU0VDUkVU",
        "User-Agent": f"prefect-fivetran/{__version__}",
    }
)


class TestFivetranSyncFlow:
    @responses.activate(registry=OrderedRegistry)
    async def test_successful_sync_run(self):
        # Set up mock responses

        # Mock get connector response
        responses.get(
            url="https://api.fivetran.com/v1/connectors/12345",
            json=GET_CONNECTION_MOCK_RESPONSE,
            match=[HEADER_MATCHER],
            status=200,
        )

        responses.get(
            url="https://api.fivetran.com/v1/connectors/12345",
            json=GET_CONNECTION_MOCK_RESPONSE,
            match=[HEADER_MATCHER],
            status=200,
        )

        responses.get(
            url="https://api.fivetran.com/v1/connectors/12345",
            json=GET_CONNECTION_MOCK_RESPONSE,
            match=[HEADER_MATCHER],
            status=200,
        )

        # Mock update connector schedule
        responses.patch(
            url="https://api.fivetran.com/v1/connectors/12345",
            json=UPDATE_CONNECTION_MOCK_RESPONSE,
            match=[
                matchers.json_params_matcher({"schedule_type": "manual"}),
                HEADER_MATCHER,
            ],
            status=200,
        )

        responses.get(
            url="https://api.fivetran.com/v1/connectors/12345",
            json=GET_CONNECTION_MOCK_RESPONSE,
            match=[HEADER_MATCHER],
            status=200,
        )

        # Force sync mock - looks like this needs to be updated to a new
        # endpoint https://fivetran.com/docs/rest-api/connectors#syncconnectordata
        responses.post(
            url="https://api.fivetran.com/v1/connectors/12345/force",
            json={
                "code": "Success",
                "message": "Sync has been successfully triggered for connector with id 'connector_id1'",  # noqa
            },
            match=[HEADER_MATCHER],
        )

        final_get_connection_response = {
            **GET_CONNECTION_MOCK_RESPONSE,
            "data": {
                **GET_CONNECTION_MOCK_RESPONSE["data"],
                "succeeded_at": str(pendulum.now()),
            },
        }

        responses.get(
            url="https://api.fivetran.com/v1/connectors/12345",
            json=final_get_connection_response,
            match=[HEADER_MATCHER],
            status=200,
        )

        fivetran_credentials = FivetranCredentials(
            api_key="API_KEY", api_secret="API_SECRET"
        )

        await fivetran_sync_flow(
            connector_id="12345", fivetran_credentials=fivetran_credentials
        )
