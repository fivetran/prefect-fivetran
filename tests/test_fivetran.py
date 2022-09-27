import pendulum
import responses
import pytest
import respx
import httpx
from httpx import Response 
from responses import matchers
from responses.registries import OrderedRegistry

from prefect import flow
from prefect_fivetran import __version__
from prefect_fivetran.credentials import FivetranCredentials
from prefect_fivetran.fivetran import (
    check_fivetran_connector,
    set_fivetran_connector_schedule,
    force_fivetran_connector,
    finish_fivetran_sync,
    start_fivetran_sync,
    fivetran_sync_flow,
)
from tests.mocked_reponses import (
    GET_CONNECTION_MOCK_RESPONSE,
    UPDATE_CONNECTION_MOCK_RESPONSE,
)

HEADERS =  {
        "Authorization": "Basic QVBJX0tFWTpBUElfU0VDUkVU",
        "User-Agent": f"prefect-fivetran/{__version__}",
}

@pytest.fixture
def fivetran_credentials():
    return FivetranCredentials(api_key="my_api_key", api_secret="my_api_secret")

class TestCheckFivetranConnector:
    async def test_check_fivetran_connector(self, respx_mock, fivetran_credentials):
        respx_mock.get(
            url="https://api.fivetran.com/v1/connectors/12345",
        ).mock(
            return_value=Response(
                200,json=GET_CONNECTION_MOCK_RESPONSE
            )
        ) 
        
        @flow
        async def test_flow():
            return await check_fivetran_connector(
                connector_id="12345",
                fivetran_credentials=fivetran_credentials,
            )

        result = await test_flow()

class TestSetFivetranSchedule:
    async def test_set_fivetran_connector_schedule(self, respx_mock, fivetran_credentials):
        respx_mock.get(
            url="https://api.fivetran.com/v1/connectors/12345",
        ).mock(
            return_value=Response(
                200,json=GET_CONNECTION_MOCK_RESPONSE
            )
        )
        respx_mock.patch(
            url="https://api.fivetran.com/v1/connectors/12345",
        ).mock(
            return_value=Response(
                200,json=UPDATE_CONNECTION_MOCK_RESPONSE
            )
        )
        @flow
        async def test_flow():
            return await set_fivetran_connector_schedule(
                connector_id="12345",
                fivetran_credentials=fivetran_credentials,
            )

        result = await test_flow()

class TestForceFivetranConnector:
    async def test_force_fivetran_connector(self, respx_mock, fivetran_credentials):
        respx_mock.get(
            url="https://api.fivetran.com/v1/connectors/12345",
        ).mock(
            return_value=Response(
                200,json=GET_CONNECTION_MOCK_RESPONSE
            )
        )
        respx_mock.post(
            url="https://api.fivetran.com/v1/connectors/12345/force",
        ).mock(
            return_value=Response(
                200,json={
                    "code": "Success",
                    "message": "Sync has been successfully triggered for connector with id 'connector_id1'",  # noqa
                },
            )
        )
        @flow
        async def test_flow():
            return await force_fivetran_connector(
                connector_id="12345",
                fivetran_credentials=fivetran_credentials,
            )

        result = await test_flow()

class TestFinishFivetranSync:
    async def test_finish_fivetran_sync(self, respx_mock, fivetran_credentials):
        final_get_connection_response = {
            **GET_CONNECTION_MOCK_RESPONSE,
            "data": {
                **GET_CONNECTION_MOCK_RESPONSE["data"],
                "succeeded_at": str(pendulum.now()),
            },
        }
        respx_mock.get(
            url="https://api.fivetran.com/v1/connectors/12345",
        ).mock(
            return_value=Response(
                200,json=final_get_connection_response
            )
        )
        
        @flow
        async def test_flow():
            return await finish_fivetran_sync(
                connector_id="12345",
                fivetran_credentials=fivetran_credentials,
                previous_completed_at=str(pendulum.now().subtract(days=1)),
            )

        result = await test_flow()

class TestStartFivetranSync:
    async def test_start_fivetran_sync(self, respx_mock, fivetran_credentials):
        respx_mock.get(
            url="https://api.fivetran.com/v1/connectors/12345",
        ).mock(
            return_value=Response(
                200,json=GET_CONNECTION_MOCK_RESPONSE
            )
        )
        respx_mock.get(
            url="https://api.fivetran.com/v1/connectors/12345",
        ).mock(
            return_value=Response(
                200,json=GET_CONNECTION_MOCK_RESPONSE
            )
        )
        respx_mock.patch(
            url="https://api.fivetran.com/v1/connectors/12345",
        ).mock(
            return_value=Response(
                200,json=UPDATE_CONNECTION_MOCK_RESPONSE
            )
        )
        respx_mock.get(
            url="https://api.fivetran.com/v1/connectors/12345",
        ).mock(
            return_value=Response(
                200,json=GET_CONNECTION_MOCK_RESPONSE
            )
        )
        respx_mock.post(
            url="https://api.fivetran.com/v1/connectors/12345/force",
        ).mock(
            return_value=Response(
                200,json={
                    "code": "Success",
                    "message": "Sync has been successfully triggered for connector with id 'connector_id1'",  # noqa
                },
            )
        )
        
        result = await start_fivetran_sync(
            connector_id="12345",
            fivetran_credentials=fivetran_credentials,
        )

class TestFivetranSyncFlow:
    async def test_fivetran_sync_flow(self, respx_mock, fivetran_credentials):
        final_get_connection_response = {
            **GET_CONNECTION_MOCK_RESPONSE,
            "data": {
                **GET_CONNECTION_MOCK_RESPONSE["data"],
                "succeeded_at": str(pendulum.now()),
            },
        }
        respx_mock.get(
            url="https://api.fivetran.com/v1/connectors/12345",
        ).mock(
            return_value=Response(
                200,json=GET_CONNECTION_MOCK_RESPONSE
            )
        )
        respx_mock.get(
            url="https://api.fivetran.com/v1/connectors/12345",
        ).mock(
            return_value=Response(
                200,json=GET_CONNECTION_MOCK_RESPONSE
            )
        )
        respx_mock.get(
            url="https://api.fivetran.com/v1/connectors/12345",
        ).mock(
            return_value=Response(
                200,json=GET_CONNECTION_MOCK_RESPONSE
            )
        )
        respx_mock.patch(
            url="https://api.fivetran.com/v1/connectors/12345",
        ).mock(
            return_value=Response(
                200,json=UPDATE_CONNECTION_MOCK_RESPONSE
            )
        )
        respx_mock.get(
            url="https://api.fivetran.com/v1/connectors/12345",
        ).mock(
            return_value=Response(
                200,json=GET_CONNECTION_MOCK_RESPONSE
            )
        )
        respx_mock.post(
            url="https://api.fivetran.com/v1/connectors/12345/force",
        ).mock(
            return_value=Response(
                200,json={
                    "code": "Success",
                    "message": "Sync has been successfully triggered for connector with id 'connector_id1'",  # noqa
                },
            )
        )
        respx_mock.get(
            url="https://api.fivetran.com/v1/connectors/12345",
        ).mock(
            return_value=Response(
                200,json=final_get_connection_response
            )
        )
        await fivetran_sync_flow(
            connector_id="12345",
            fivetran_credentials=fivetran_credentials,
        )
