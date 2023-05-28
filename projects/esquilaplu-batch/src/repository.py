import io
import os

import boto3
import pandas as pd


class WeatherRepository:
    def __init__(self) -> None:
        self._aws_s3_bucket = os.getenv("S3_BUCKET")
        self._aws_access_key_id = os.getenv("ACCESS_KEY_ID")
        self._aws_secret_access_key = os.getenv("SECRET_ACCESS_KEY")

        self._root_key = "esquilaplu"

        self._s3_client = boto3.client(
            "s3",
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
        )

    def list_datasets(self) -> list[str]:
        response = self._s3_client.list_objects_v2(Bucket=self._aws_s3_bucket, Prefix=self._root_key)
        return [
            content["Key"].lstrip(f"{self._root_key}/")
            for content in response["Contents"]
            if content["Key"] != self._root_key
        ]

    def save_dataset(self, dataset_id: str, dataset: pd.DataFrame) -> None:
        data_key = f"{self._root_key}/{dataset_id}.csv"
        data_buffer = io.StringIO()
        dataset.to_csv(data_buffer, sep=";", index=False)
        data_buffer.seek(0)
        self._s3_client.put_object(Bucket=self._aws_s3_bucket, Key=data_key, Body=data_buffer.getvalue())
