import pytest
import responses
from pychronus.hooks.lastfm import LastFmHook

# @pytest.fixture(params=["cultivator-staging", "cultivator-production"])
# def conn_id(request) -> str:
#     return request.param


# @pytest.fixture(params=["batch", "cal_config_set_transitions", None])
# def resource(request) -> str:
#     return request.param


class MockLastFmHook(LastFmHook):
    def __init__(self, conn_id, username="testuser", method="test"):
        super().__init__(conn_id=conn_id)

    def get_conn(self):
        return self.connection

    def get_headers(self) -> dict:
        return {"Authorization": "Bearer mock_token"}


@pytest.fixture
def lastfm_hook(conn_id, infarm_connection) -> LastFmHook:
    mock_lastfm_hook = MockLastFmHook(conn_id)
    # mock_lastfm_hook.connection = dummy_connection
    return mock_lastfm_hook


def test_get_params(lastfm_hook):
    params = lastfm_hook.get_params(page=1)
    assert params.get("method") == lastfm_hook.method
    assert params.get("user") == lastfm_hook.username
    assert "from" not in params.keys()
    assert "api_key" in params.keys()


def test_get_params_updated_at(lastfm_hook):
    params = lastfm_hook.get_params(page=1, updated_at="91536918523")
    assert params.get("from") == "91536918523"
    assert "from" in params.keys()


@responses.activate
def test_make_request(lastfm_hook):
    resource = "get_set"
    uuid = "33745b5b-07a3-435c-85f6-a08709c7e926"
    responses.add(
        responses.GET,
        url=f"{lastfm_hook.endpoint}calendar-configuration-sets/{uuid}/",
        json={"message": "get_successful"},
    )

    result = lastfm_hook.make_request(resource=resource, uuid=uuid)
    assert result["message"] == "get_successful"

    resource = "benches"
    responses.add(
        responses.GET,
        url=f"{lastfm_hook.endpoint}benches/",
        json={"message": "get_successful"},
    )

    result = lastfm_hook.make_request(resource=resource, uuid=uuid)
    assert result["message"] == "get_successful"
