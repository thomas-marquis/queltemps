import datetime as dt

import boto3

from src.domain.ports.outer import AppRepository
from src.domain.value_objects import Laps


class AppS3Repository(AppRepository):
    MF_LAPS_DURATION = 3

    def __init__(self) -> None:
        self._aws_s3_bucket = ""
        self._root_key = "esquilaplu"

        self._s3_client = boto3.client(
            "s3",
            aws_access_key_id=None,
            aws_secret_access_key=None,
        )

    def get_available_laps_since(self, since: dt.datetime) -> list[Laps]:
        all_saved_data_files = self._list_existing_files()
        all_saved_dt = [
            self._parse_datetime_from_filename(file) - dt.timedelta(hours=self.MF_LAPS_DURATION)
            for file in all_saved_data_files
            if file.endswith(".csv")
        ]
        all_saved_dt = [
            Laps(start_time=saved_dt, duration_hours=self.MF_LAPS_DURATION)
            for saved_dt in all_saved_dt
            if saved_dt >= since
        ]

        return all_saved_dt

    def _list_existing_files(self) -> list[str]:
        response = self._s3_client.list_objects_v2(Bucket=self._aws_s3_bucket, Prefix=self._root_key)
        return [
            content["Key"].lstrip(f"{self._root_key}/")
            for content in response["Contents"]
            if content["Key"] != self._root_key
        ]

    @staticmethod
    def _parse_datetime_from_filename(filename: str) -> dt.datetime:
        return dt.datetime.strptime(filename, "%Y-%m-%d-%H.csv")
