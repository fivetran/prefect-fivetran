from prefect import flow

from prefect_fivetran.tasks import (
    goodbye_prefect_fivetran,
    hello_prefect_fivetran,
)


def test_hello_prefect_fivetran():
    @flow
    def test_flow():
        return hello_prefect_fivetran()

    result = test_flow()
    assert result == "Hello, prefect-fivetran!"


def goodbye_hello_prefect_fivetran():
    @flow
    def test_flow():
        return goodbye_prefect_fivetran()

    result = test_flow()
    assert result == "Goodbye, prefect-fivetran!"
