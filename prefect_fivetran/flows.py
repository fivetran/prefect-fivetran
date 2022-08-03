"""This is an example flows module"""
from prefect import flow

from prefect_fivetran.tasks import (
    goodbye_prefect_fivetran,
    hello_prefect_fivetran,
)


@flow
def hello_and_goodbye():
    """
    Sample flow that says hello and goodbye!
    """
    print(hello_prefect_fivetran)
    print(goodbye_prefect_fivetran)
    return "Done"
