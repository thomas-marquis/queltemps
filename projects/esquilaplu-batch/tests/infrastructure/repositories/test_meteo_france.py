import datetime as dt
from unittest.mock import MagicMock

import pytest
import requests
from easy_testing import DataFrameBuilder, assert_called_once_with_frame

from src.domain.entities import Record
from src.domain.exceptions import WeatherCollectionError
from src.domain.value_objects import Laps
from src.infrastructure.repositories.app_s3 import AppS3Repository
from src.infrastructure.repositories.meteo_france import MeteoFranceRepository


class TestMeteoFranceRepository:
    @pytest.fixture(autouse=True)
    def mock_factory(self, mocker):
        return mocker.patch(f"{MeteoFranceRepository.__module__}.MeteoFranceRecordFactory")

    @pytest.fixture(autouse=True)
    def mock_requests(self, mocker):
        mock = mocker.patch(f"{MeteoFranceRepository.__module__}.requests")
        mock.get.return_value = MagicMock(
            text="date;numer_sta;rr1;rr3;rr6;rr12;rr24\n2021-01-30;7510;0.1;0.2;0.3;0.4;0.5"
        )
        return mock

    @pytest.fixture
    def mock_app_repository(self):
        return MagicMock(spec=AppS3Repository)

    @pytest.fixture
    def repository(self, mock_app_repository):
        return MeteoFranceRepository(app_repository=mock_app_repository)

    class TestCollectRecord:
        def test_should_call_requests_get_with_expected_url(self, repository, mock_requests):
            # Given
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

        def test_should_return_record_from_factory(self, repository, mock_requests, mock_factory):
            # Given
            mock_requests.get.return_value = MagicMock(
                text="date;numer_sta;rr1;rr3;rr6;rr12;rr24\n2021-01-30;7510;0.1;0.2;0.3;0.4;0.5"
            )
            dataframe = (
                DataFrameBuilder.a_dataframe()
                .with_columns(["date", "numer_sta", "rr1", "rr3", "rr6", "rr12", "rr24"])
                .with_dtypes(
                    date="datetime64[ns]", rr1="float64", rr3="float64", rr6="float64", rr12="float64", rr24="float64"
                )
                .with_row(
                    date=dt.datetime(2021, 1, 30, 13, 0, 0),
                    numer_sta=7510,
                    rr1=0.1,
                    rr3=0.2,
                    rr6=0.3,
                    rr12=0.4,
                    rr24=0.5,
                )
                .build()
            )
            laps = Laps(start_time=dt.datetime(2021, 1, 30, 10, 0, 0), duration_hours=3)
            expected = Record(
                laps=Laps(start_time=dt.datetime(2021, 1, 30, 10, 0, 0), duration_hours=3),
                rainfall_mm=0.2,
            )
            mock_factory.from_dataframe.return_value = expected

            # When
            result = repository.collect_record(laps)

            # Then
            assert_called_once_with_frame(mock_factory.from_dataframe, dataframe, laps_duration_hr=3)
            assert result == expected

        def test_should_save_raw_dataset_to_app_repository(
            self, repository, mock_requests, mock_factory, mock_app_repository
        ):
            # Given
            mock_requests.get.return_value = MagicMock(
                text="date;numer_sta;rr1;rr3;rr6;rr12;rr24\n2021-01-30;7510;0.1;0.2;0.3;0.4;0.5"
            )
            dataframe = (
                DataFrameBuilder.a_dataframe()
                .with_columns(["date", "numer_sta", "rr1", "rr3", "rr6", "rr12", "rr24"])
                .with_dtypes(
                    date="datetime64[ns]", rr1="float64", rr3="float64", rr6="float64", rr12="float64", rr24="float64"
                )
                .with_row(
                    date=dt.datetime(2021, 1, 30, 13, 0, 0),
                    numer_sta=7510,
                    rr1=0.1,
                    rr3=0.2,
                    rr6=0.3,
                    rr12=0.4,
                    rr24=0.5,
                )
                .build()
            )
            laps = Laps(start_time=dt.datetime(2021, 1, 30, 10, 0, 0), duration_hours=3)
            expected = Record(
                laps=Laps(start_time=dt.datetime(2021, 1, 30, 10, 0, 0), duration_hours=3),
                rainfall_mm=0.2,
            )
            mock_factory.from_dataframe.return_value = expected

            # When
            repository.collect_record(laps)

            # Then
            assert_called_once_with_frame(mock_app_repository.save_raw_dataset, dataset=dataframe, laps=laps)

        def test_should_replace_mq_with_zero(self, repository, mock_requests, mock_factory):
            # Given
            mock_requests.get.return_value = MagicMock(
                text="date;numer_sta;rr1;rr3;rr6;rr12;rr24\n2021-01-30;7510;0.1;0.2;mq;0.4;0.5"
            )
            dataframe = (
                DataFrameBuilder.a_dataframe()
                .with_columns(["date", "numer_sta", "rr1", "rr3", "rr6", "rr12", "rr24"])
                .with_dtypes(
                    date="datetime64[ns]", rr1="float64", rr3="float64", rr6="float64", rr12="float64", rr24="float64"
                )
                .with_row(
                    date=dt.datetime(2021, 1, 30, 13, 0, 0),
                    numer_sta=7510,
                    rr1=0.1,
                    rr3=0.2,
                    rr6=0.0,
                    rr12=0.4,
                    rr24=0.5,
                )
                .build()
            )
            laps = Laps(start_time=dt.datetime(2021, 1, 30, 10, 0, 0), duration_hours=3)
            expected = Record(
                laps=Laps(start_time=dt.datetime(2021, 1, 30, 10, 0, 0), duration_hours=3),
                rainfall_mm=0.2,
            )
            mock_factory.from_dataframe.return_value = expected

            # When
            repository.collect_record(laps)

            # Then
            assert_called_once_with_frame(mock_factory.from_dataframe, dataframe, laps_duration_hr=3)
