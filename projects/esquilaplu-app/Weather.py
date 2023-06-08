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




class WeatherRecord:
    def __init__(self, dataset: pd.DataFrame, datetime: dt.datetime) -> None:
        data = dataset.copy()[dataset["numer_sta"] == STATION_ID]

        self._dataset = data
        self._datetime = datetime

    @property
    def end_datetime(self) -> dt.datetime:
        return self._datetime + dt.timedelta(hours=3)

    @property
    def start_datetime(self) -> dt.datetime:
        return self._datetime #- dt.timedelta(hours=3)

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
        value = float(self._dataset[column_name].values[0])
        return value if value > 0 else 0
    
    @staticmethod
    def get_icon(rainfall_mm: float) -> str:
        if rainfall_mm <= 0:
            return "🌞"
        elif rainfall_mm < 5:
            return "🌦"
        else:
            return "🌧"


class WeatherRecordFactory:
    def __init__(self) -> None:
        self._repository = WeatherRepository()

    def get_record_by_date_and_time(self, datetime: dt.datetime) -> WeatherRecord:
        data = self._repository.load_dataset(f"{datetime.date().isoformat()}-{datetime.strftime('%H')}")
        return WeatherRecord(data, datetime)

    def get_records_by_date(self, date: dt.date) -> list[WeatherRecord]:
        all_saved_data_files = self._repository.list_datasets()
        all_saved_data_files = [file for file in all_saved_data_files if file and (self._parse_datetime_from_filename(file)).date() == date]
        
        all_saved_data_files.sort()

        records = []
        for file in all_saved_data_files:
            records.append(self.get_record_by_date_and_time(self._parse_datetime_from_filename(file)))

        return records

    def list_saved_dataset_datetimes(self) -> list[dt.datetime]:
        all_saved_data_files = self._repository.list_datasets()
        all_saved_dt = [
            self._parse_datetime_from_filename(file) for file in all_saved_data_files if file.endswith(".csv")
        ]
        all_saved_dt.sort()

        return all_saved_dt

    @staticmethod
    def _parse_datetime_from_filename(filename: str) -> dt.datetime:
        return dt.datetime.strptime(filename, "%Y-%m-%d-%H.csv") - dt.timedelta(hours=3)


class WeatherCalculator:
    @staticmethod
    def compute_rainfall(records: list[WeatherRecord]) -> float:
        return sum([rec.rain_mm_last_3h for rec in records])


def application():
    st.header("Esquilaplu")
    st.write("Bienvenue sur Esquilaplu, l'application qui permet de savoir quand et combien il a plu !")

    factory = WeatherRecordFactory()
    available_records = factory.list_saved_dataset_datetimes()
    
    st.session_state.selected_date = st.session_state.selected_date if "selected_date" in st.session_state else available_records[-1].date()
    
    cols = st.columns(4)
    with cols[0]:
        if st.button("Jour précédent", disabled=st.session_state.selected_date == available_records[0].date()):
            st.session_state.selected_date -= dt.timedelta(days=1)
        
    with cols[1]:
        if st.button("Jour suivant", disabled=st.session_state.selected_date == available_records[-1].date()):
            st.session_state.selected_date += dt.timedelta(days=1)
            
    with cols[2]:
        if st.button("Le plus récent", disabled=st.session_state.selected_date == available_records[-1].date()):
            st.session_state.selected_date = available_records[-1].date()

    if st.session_state.selected_date > available_records[-1].date():
        st.session_state.selected_date = available_records[-1].date()
    elif st.session_state.selected_date < available_records[0].date():
        st.session_state.selected_date = available_records[0].date()

    with cols[3]:
        st.session_state.selected_date: dt.date = st.date_input(
            "Choisi une date",
            st.session_state.selected_date,
            min_value=available_records[0],
            max_value=available_records[-1],
        )
    
    filtered_records = factory.get_records_by_date(st.session_state.selected_date)
    
    if not filtered_records:
        st.warning("Aucune donnée disponible pour cette date... Essaye la veille !")
        return

    day_rainfall = WeatherCalculator.compute_rainfall(filtered_records)
    selected_date_formatted = st.session_state.selected_date.strftime("%A %d %B %Y")
    
    cols = st.columns(2)
    with cols[0]:
        st.subheader(f"{selected_date_formatted}")
    with cols[1]:
        st.metric("Pluviométrie du jour", f"{WeatherRecord.get_icon(day_rainfall)} {day_rainfall:.2f} mm")

    df = pd.DataFrame(
        {
            "Date": [rec.start_datetime.strftime("%d/%m/%Y") for rec in filtered_records],
            "Heure": [f"de {rec.start_datetime.hour}h à {rec.end_datetime.hour}h" for rec in filtered_records],
            "Pluviométrie": [f"{rec.get_icon(rec.rain_mm_last_3h)} {rec.rain_mm_last_3h:.2f} mm" for rec in filtered_records],
        },
        columns=["Date", "Heure", "Pluviométrie"],
    )
    st.table(df)


if __name__ == "__main__":
    application()
