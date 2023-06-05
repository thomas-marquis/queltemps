import datetime as dt
import os

import boto3
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from src.utils import render_hide_st_burger_menu

load_dotenv()

st.set_page_config(page_title="Esquilaplu", page_icon="🌞")
render_hide_st_burger_menu()

STATION_ID = 7510


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

    def load_dataset(self, dataset_id: str) -> pd.DataFrame:
        data_key = f"{self._root_key}/raw/meteofrance/{dataset_id}.csv"
        data_object = self._s3_client.get_object(Bucket=self._aws_s3_bucket, Key=data_key)
        data = pd.read_csv(data_object["Body"], sep=";", header=0, parse_dates=["date"])

        return data

    def list_datasets(self) -> list[str]:
        response = self._s3_client.list_objects_v2(Bucket=self._aws_s3_bucket, Prefix=f"{self._root_key}/raw/meteofrance")
        return [content["Key"].lstrip(f"{self._root_key}/raw/meteofrance/") for content in response["Contents"] if content["Key"] != f"{self._root_key}/raw/meteofrance"]




class WeatherChunk:
    def __init__(self, dataset: pd.DataFrame, datetime: dt.datetime) -> None:
        data = dataset.copy()[dataset["numer_sta"] == STATION_ID]

        self._dataset = data
        self._datetime = datetime

    @property
    def end_datetime(self) -> dt.datetime:
        return self._datetime

    @property
    def start_datetime(self) -> dt.datetime:
        return self._datetime - dt.timedelta(hours=3)

    @property
    def rain_mm_last_1h(self) -> float:
        return self._get_float_value("rr1")

    @property
    def rain_mm_last_3h(self) -> float:
        return self._get_float_value("rr3")

    @property
    def rain_mm_last_6h(self) -> float:
        return self._get_float_value("rr6")

    @property
    def rain_mm_last_12h(self) -> float:
        return self._get_float_value("rr12")

    @property
    def rain_mm_last_24h(self) -> float:
        return self._get_float_value("rr24")

    def _get_float_value(self, column_name: str) -> float:
        return float(self._dataset[column_name].values[0])


class WeatherChunkFactory:
    def __init__(self) -> None:
        self._repository = WeatherRepository()

    def get_chunk_by_date_and_time(self, datetime: dt.datetime) -> WeatherChunk:
        data = self._repository.load_dataset(f"{datetime.date().isoformat()}-{datetime.strftime('%H')}")
        return WeatherChunk(data, datetime)

    def get_chunks_by_date(self, date: dt.date) -> list[WeatherChunk]:
        all_saved_data_files = self._repository.list_datasets()
        all_saved_data_files = [file for file in all_saved_data_files if file.startswith(date.isoformat())]
        all_saved_data_files.sort()

        chunks = []
        for file in all_saved_data_files:
            chunks.append(self.get_chunk_by_date_and_time(self._parse_datetime_from_filename(file)))

        return chunks

    def list_saved_dataset_datetimes(self) -> list[dt.datetime]:
        all_saved_data_files = self._repository.list_datasets()
        all_saved_dt = [
            self._parse_datetime_from_filename(file) for file in all_saved_data_files if file.endswith(".csv")
        ]
        all_saved_dt.sort()

        return all_saved_dt

    @staticmethod
    def _parse_datetime_from_filename(filename: str) -> dt.datetime:
        return dt.datetime.strptime(filename, "%Y-%m-%d-%H.csv")


class WeatherCalculator:
    @staticmethod
    def compute_rainfall(chunks: list[WeatherChunk]) -> float:
        return sum([chunk.rain_mm_last_3h for chunk in chunks])


def application():
    st.header("🌧 Esquilaplu")
    st.write("Bienvenue sur Esquilaplu, l'application qui permet de savoir quand et combien il a plu !")

    factory = WeatherChunkFactory()
    available_chunks = factory.list_saved_dataset_datetimes()

    selection: dt.date = st.date_input(
        "Pluviométrie pour une date",
        available_chunks[-1],
        min_value=available_chunks[0],
        max_value=available_chunks[-1],
    )

    chunks = factory.get_chunks_by_date(selection)

    st.metric(f"Pluviométrie pour le {selection}", f"{WeatherCalculator.compute_rainfall(chunks):.2f} mm")

    st.table(
        [
            ("Date", "Heure", "Pluviométrie"),
            *[
                (
                    chunk.start_datetime.date(),
                    f"{chunk.start_datetime.hour}h - {chunk.end_datetime.hour}h",
                    f"{chunk.rain_mm_last_3h:.2f} mm",
                )
                for chunk in chunks
            ],
        ]
    )


if __name__ == "__main__":
    application()
