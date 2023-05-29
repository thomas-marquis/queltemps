import datetime as dt

import boto3

from src.domain.entities import Record
from src.domain.ports.outer import AppRepository
from src.domain.value_objects import Laps


class AppS3Repository(AppRepository):
    MF_LAPS_DURATION = 3

    def __init__(self, bucket: str, root_key: str, secret_key: str, access_key: str) -> None:
        self._aws_s3_bucket = bucket
        self._root_key = root_key

        self._s3_client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
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

    def save_many_records(self, records: list[Record]) -> None:
        raise NotImplementedError()

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
