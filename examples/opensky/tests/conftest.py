import pytest
from conduit_etl.core.registry import get_registry


@pytest.fixture(autouse=True)
def clear_registry():
    get_registry().clear()
    yield
    get_registry().clear()
