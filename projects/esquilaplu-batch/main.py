import datetime as dt
import io
import os
import random
import time

import boto3
import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm import tqdm

from src.domain.value_objects import Laps

load_dotenv("secrets/.env")

MAX_SCRAPPING = -1
"""Max number of data to scrap..

Set to -1 to scrap all available data.
"""


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


def load_dataset(date: dt.datetime) -> pd.DataFrame:
    hour = date.strftime("%H")
    date_id = "".join(
        [
            str(date.year),
            date.strftime("%m"),
            date.strftime("%d"),
        ]
    )

    dt_id = date_id + hour

    url = f"https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/synop.{dt_id}.csv"
    headers = {
        "Referer": (
            f"https://donneespubliques.meteofrance.fr/?fond=donnee_libre&prefixe=Txt%2FSynop%2Fsynop&extension"
            f"=csv&date={date_id}&reseau={hour}"
        ),
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/112.0",
        "Host": "donneespubliques.meteofrance.fr",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "fr,fr-FR;q=0.8,en-US;q=0.5,en;q=0.3",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
    }

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()

    buffer = io.StringIO(resp.text)

    buffer.seek(0)
    df = pd.read_csv(buffer, sep=";", parse_dates=["date"])

    return df


def save_dataset(repository: WeatherRepository, df: pd.DataFrame, datetime: dt.datetime) -> None:
    dataset_id = f"{datetime.date().isoformat()}-{datetime.hour}"
    repository.save_dataset(dataset_id, df)


def _parse_datetime_from_filename(filename: str) -> dt.datetime:
    return dt.datetime.strptime(filename, "%Y-%m-%d-%H.csv")


def get_available_laps_since(repository: WeatherRepository, since: dt.datetime) -> list[Laps]:
    all_saved_data_files = repository.list_datasets()
    all_saved_dt = [_parse_datetime_from_filename(file) for file in all_saved_data_files if file.endswith(".csv")]
    all_saved_dt = [Laps(start_time=dt, duration_hours=3) for dt in all_saved_dt if dt >= since]

    return all_saved_dt


def get_missing_datetimes(start_dt: dt.datetime, end_dt: dt.datetime, list_dts: dt.datetime) -> list[dt.datetime]:
    HOURS = [0, 3, 6, 9, 12, 15, 18, 21]

    missing_dts = []
    current_dt = dt.datetime(start_dt.year, start_dt.month, start_dt.day, 0)

    while current_dt <= end_dt:
        if current_dt.hour not in HOURS:
            current_dt += dt.timedelta(hours=1)
            continue

        if current_dt not in list_dts:
            missing_dts.append(current_dt)
        current_dt += dt.timedelta(hours=3)

    return missing_dts


def main():
    repository = WeatherRepository()

    start_time = dt.datetime.now() - dt.timedelta(days=14)
    existing_datetimes = get_available_laps_since(repository, since=start_time)

    end_time = dt.datetime.now() - dt.timedelta(hours=5)
    missing_dts = get_missing_datetimes(start_time, end_time, existing_datetimes)

    i = 0
    for missing_dt in tqdm(missing_dts):
        df = load_dataset(missing_dt)

        wait_sec: float = random.uniform(0.5, 2.0)
        time.sleep(wait_sec)

        save_dataset(repository, df, missing_dt)

        i += 1
        if MAX_SCRAPPING > 0 and i >= MAX_SCRAPPING:
            break


if __name__ == "__main__":
    main()
