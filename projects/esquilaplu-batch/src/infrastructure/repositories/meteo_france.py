import logging

import requests
from requests.exceptions import HTTPError

from src.domain.entities import Record
from src.domain.exceptions import WeatherCollectionError
from src.domain.ports.outer import WeatherDataRepository
from src.domain.value_objects import Laps


class MeteoFranceRepository(WeatherDataRepository):
    def __init__(self) -> None:
        self._logger = logging.getLogger(__name__)

    def collect_record(self, laps: Laps) -> Record:
        hour = laps.start_time.hour + laps.duration_hours
        date_id = laps.start_time.strftime("%Y%m%d")
        time_id = f"{date_id}{hour:02d}"

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
