import datetime as dt
import io
import logging
import random
import time

import pandas as pd
import requests
from requests.exceptions import HTTPError

from src.domain.entities import Record
from src.domain.exceptions import WeatherCollectionError
from src.domain.ports.outer import WeatherDataRepository
from src.domain.value_objects import Laps
from src.infrastructure.factories.mf_record import MeteoFranceRecordFactory
from src.infrastructure.repositories.app_s3 import AppS3Repository


class MeteoFranceRepository(WeatherDataRepository):
    def __init__(self, app_repository: AppS3Repository) -> None:
        self._logger = logging.getLogger(__name__)
        self._app_repository = app_repository

    def collect_record(self, laps: Laps) -> Record:
        end_time = laps.start_time + dt.timedelta(hours=laps.duration_hours)
        hour = end_time.strftime("%H")
        date_id = end_time.strftime("%Y%m%d")
        time_id = f"{date_id}{hour}"

        url = f"https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/synop.{time_id}.csv"
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

        response = requests.get(url, headers=headers)

        try:
            response.raise_for_status()
        except HTTPError as e:
            self._logger.error(f"Error while collecting weather data: {e}")
            raise WeatherCollectionError()

        try:
            dataframe = pd.read_csv(io.StringIO(response.text), sep=";", header=0)
        except Exception:
            print(time_id)
        dataframe = dataframe.replace("mq", 0)
        dataframe["date"] = end_time
        dataframe = dataframe.astype(
            {
                "date": "datetime64[ns]",
                "rr1": "float64",
                "rr3": "float64",
                "rr6": "float64",
                "rr12": "float64",
                "rr24": "float64",
            }
        )

        self._app_repository.save_raw_dataset(dataset=dataframe, laps=laps)

        wait_sec: float = random.uniform(0.2, 1.5)
        time.sleep(wait_sec)

        return MeteoFranceRecordFactory.from_dataframe(dataframe, laps_duration_hr=3)
