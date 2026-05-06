# import json
import os
from datetime import datetime
from unittest import mock

import pytest
from airflow.models import Connection


@pytest.fixture
def conn(request):
    connection = Connection(
        conn_type="http",
        login="foo",
        host="conn-host",
        extra='{"refresh_token": "bar"}',
    )
    with mock.patch.dict(
        os.environ, {f"AIRFLOW_CONN_{str(request.param).upper()}": connection.get_uri()}
    ):
        yield


@pytest.fixture
def token():
    def callable_(*args):
        return {"token": "foo", "created_at": datetime.now()}

    return callable_


class DummyConnection:
    def __init__(self):
        self.login = "username"
        self.password = "password"

    def get_extra(self):
        return '{"key_file":"password_file.txt", "assertion_url": "a-web-url.com"}'


@pytest.fixture
def dummy_connection() -> DummyConnection:
    return DummyConnection()


# @pytest.fixture
# def kompost_response():
#     with open(Path(__file__).resolve().parent / "fixtures" / "kompost.json") as f:
#         return json.load(f)
