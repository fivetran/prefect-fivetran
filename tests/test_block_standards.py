import pytest
from prefect.testing.standard_test_suites import BlockStandardTestSuite

from prefect_fivetran.credentials import FivetranCredentials


@pytest.mark.parametrize("block", [FivetranCredentials])
class TestAllBlocksAdhereToStandards(BlockStandardTestSuite):
    @pytest.fixture
    def block(self, block):
        return block
