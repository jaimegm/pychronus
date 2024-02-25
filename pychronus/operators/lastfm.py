import time
from typing import Any, Dict, List, Union

import pandas as pd
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from pychronus.hooks.dbmanager import DBManager
from pychronus.hooks.lastfm import LastFmHook


class LastFmOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        database: str,
        updated_at: str,
        username: str,
        method: str = "user.getrecenttracks",
        *args,
        **kwargs,
    ):
        super(LastFmOperator, self).__init__(*args, **kwargs)
        # Inputs
        self.updated_at = updated_at
        self.tablename = f"{username}_scrobbles"
        self.database = database
        self.username = username
        self.method = method

        # Internal
        self._dbmanager = None
        self._lastfm_hook = None

    @property
    def dbmanager(self) -> DBManager:
        if self._dbmanager is None:
            self._dbmanager = DBManager(
                database=self.database,
                tablename=self.tablename,
                schema="raw",
            )
        return self._dbmanager

    @property
    def lastfm_hook(self) -> LastFmHook:
        if self._lastfm_hook is None:
            self._lastfm_hook = LastFmHook(username=self.username, method=self.method)
        return self._lastfm_hook

    def paginate(self, pause_duration: float = 0.4):
        last_updated = (
            self.dbmanager.get_last_updated_at()
            if self.dbmanager.table_exists()
            else None
        )
        init_request = self.lastfm_hook.make_request(updated_at=last_updated)
        # Get Page Count
        page_count = init_request["recenttracks"]["@attr"]["totalPages"]
        self.lastfm_hook.log.info(f"{page_count} Pages to Extract")
        df = pd.DataFrame()
        for page in range(1, int(page_count) + 1, 1):
            if page % 10 == 0:
                self.lastfm_hook.log.info(page)
            time.sleep(pause_duration)
            response = self.lastfm_hook.make_request(
                page=page, updated_at=last_updated
            )["recenttracks"]["track"]
            # Collect Parsed data
            df = df.append(
                # Parse data
                self.get_extract_method(response)
            )
        return df

    def get_extract_method(self, data: List[Dict]) -> pd.DataFrame:
        extract_methods = {"user.getrecenttracks": self.extract_getrecenttracks}
        try:
            return extract_methods[self.method](data=data)
        except Exception as e:
            self.log.error("Invalid Json Extract Method")
            raise ValueError(e)

    @staticmethod
    def extract_getrecenttracks(data: List) -> pd.DataFrame:
        goods = [
            {
                "artist": scrobble["artist"]["#text"],
                "artist_mbid": scrobble["artist"]["mbid"],
                "album": scrobble["album"]["#text"],
                "album_mbid": scrobble["album"]["mbid"],
                "track": scrobble["name"],
                "track_mbid": scrobble["mbid"],
                "timestamp": scrobble["date"]["uts"],
                "datetime": pd.to_datetime(int(scrobble["date"]["uts"]), unit="s"),
            }
            for scrobble in data
        ]
        return pd.DataFrame(goods)

    def execute(self, context: Dict[str, Any]) -> Union[None, bool]:
        self.lastfm_hook.log.info("Extracting LastFm Data...")
        data = self.paginate()
        self.dbmanager.upload(df=data)
        return self.lastfm_hook.log.info(f"Download Completed: {data.shape}")
