import requests
from airflow.hooks.base import BaseHook
from tenacity import retry, stop_after_attempt, wait_random

pause_duration = 0.4


class LastFmHook(BaseHook):
    def __init__(
        self,
        username: str,
        method: str = "user.getrecenttracks",
        conn_id: str = "lastfm",
    ):
        self.endpoint = "https://ws.audioscrobbler.com/2.0/"
        self.username = username
        self.method = method
        self.conn_id = conn_id

    def get_conn(self):
        return self.get_connection(self.conn_id)

    def get_params(self, page):
        conn = self.get_conn()
        params = {
            "method": self.method,
            "api_key": conn.password,
            "user": self.username,
            "format": "json",
            "limit": 200,
            "extended": 0,
            "page": page,
        }
        return params

    @retry(wait=wait_random(min=2, max=15), stop=stop_after_attempt(5))
    def make_request(self, page=1, updated_at: str = None):
        request_params = self.get_params(page)
        if updated_at:
            request_params["from"] = updated_at
        resp = requests.get(url=self.endpoint, params=request_params)
        resp.raise_for_status()
        return resp.json()
