import datetime as dt
from unittest.mock import MagicMock

import pytest
import requests

from src.domain.exceptions import WeatherCollectionError
from src.domain.value_objects import Laps
from src.infrastructure.repositories.meteo_france import MeteoFranceRepository


class TestMeteoFranceRepository:
    @pytest.fixture(autouse=True)
    def mock_requests(self, mocker):
        return mocker.patch(f"{MeteoFranceRepository.__module__}.requests")

    @pytest.fixture
    def repository(self):
        return MeteoFranceRepository()

    class TestCollectRecord:
        def test_should_call_requests_get_with_expected_url(self, repository, mock_requests):
            # Given
            mock_requests.get.return_value = MagicMock()
            laps = Laps(start_time=dt.datetime(2021, 1, 30, 10, 0, 0), duration_hours=3)

            # When
            repository.collect_record(laps)

            # Then
            mock_requests.get.assert_called_once_with(
                "https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/synop.2021013013.csv",
                headers={
                    "Referer": (
                        "https://donneespubliques.meteofrance.fr/?fond=donnee_libre&prefixe=Txt%2FSynop%2Fsynop&"
                        "extension=csv&date=20210130&reseau=13"
                    ),
                    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/112.0",
                    "Host": "donneespubliques.meteofrance.fr",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Accept-Language": "fr,fr-FR;q=0.8,en-US;q=0.5,en;q=0.3",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                },
            )

        def test_should_raise_collection_error_when_response_status_error(self, repository, mock_requests):
            # Given
            mock_requests.get.return_value = MagicMock(
                raise_for_status=MagicMock(side_effect=requests.exceptions.HTTPError)
            )
            laps = Laps(start_time=dt.datetime(2021, 1, 30, 10, 0, 0), duration_hours=3)

            # When & Then
            with pytest.raises(WeatherCollectionError):
                repository.collect_record(laps)
