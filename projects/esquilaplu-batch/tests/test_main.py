import datetime as dt
from unittest.mock import MagicMock

from main import WeatherRepository, get_available_laps_since, get_missing_datetimes

from src.domain.value_objects import Laps


class TestGetAvailableLapsSince:
    def test_should_return_empty_list_when_no_file(self, tmp_path):
        # Given
        mock_repository = MagicMock(spec=WeatherRepository)
        mock_repository.list_datasets.return_value = []

        # When
        result = get_available_laps_since(mock_repository, dt.datetime(2021, 1, 1))

        # Then
        assert result == []

    def test_should_list_existing_file_as_datetime(self, tmp_path):
        # Given
        mock_repository = MagicMock(spec=WeatherRepository)
        mock_repository.list_datasets.return_value = [
            "2021-01-01-00.csv",
            "2021-01-01-01.csv",
            "2021-01-02-00.csv",
        ]

        # When
        result = get_available_laps_since(mock_repository, dt.datetime(2021, 1, 1, 1))

        # Then
        assert result == [
            Laps(start_time=dt.datetime(2021, 1, 1, 1), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 2, 0), duration_hours=3),
        ]


class TestGetMissingDatetimes:
    def test_should_return_empty_list_when_no_missing(self):
        # Given
        start_dt = dt.datetime(2021, 1, 1, 0)
        end_dt = dt.datetime(2021, 1, 1, 23)
        list_dts = [
            dt.datetime(2021, 1, 1, 0),
            dt.datetime(2021, 1, 1, 3),
            dt.datetime(2021, 1, 1, 6),
            dt.datetime(2021, 1, 1, 9),
            dt.datetime(2021, 1, 1, 12),
            dt.datetime(2021, 1, 1, 15),
            dt.datetime(2021, 1, 1, 18),
            dt.datetime(2021, 1, 1, 21),
        ]

        # When
        result = get_missing_datetimes(start_dt, end_dt, list_dts)

        # Then
        assert result == []

    def test_should_return_one_missing_dt(self):
        # Given
        start_dt = dt.datetime(2021, 1, 1, 0)
        end_dt = dt.datetime(2021, 1, 1, 23)
        list_dts = [
            dt.datetime(2021, 1, 1, 0),
            dt.datetime(2021, 1, 1, 3),
            dt.datetime(2021, 1, 1, 6),
            dt.datetime(2021, 1, 1, 9),
            dt.datetime(2021, 1, 1, 15),
            dt.datetime(2021, 1, 1, 18),
            dt.datetime(2021, 1, 1, 21),
        ]

        # When
        result = get_missing_datetimes(start_dt, end_dt, list_dts)

        # Then
        assert result == [dt.datetime(2021, 1, 1, 12)]

    def test_should_return_many_missing_dt_when_several_missing(self):
        # Given
        start_dt = dt.datetime(2021, 1, 1, 1)
        end_dt = dt.datetime(2021, 1, 1, 23)
        list_dts = [
            dt.datetime(2021, 1, 1, 0),
            dt.datetime(2021, 1, 1, 3),
            dt.datetime(2021, 1, 1, 9),
            dt.datetime(2021, 1, 1, 15),
            dt.datetime(2021, 1, 1, 18),
            dt.datetime(2021, 1, 1, 21),
        ]

        # When
        result = get_missing_datetimes(start_dt, end_dt, list_dts)

        # Then
        assert result == [
            dt.datetime(2021, 1, 1, 6),
            dt.datetime(2021, 1, 1, 12),
        ]

    def test_should_return_missing_dt_from_many_days(self):
        # Given
        start_dt = dt.datetime(2021, 1, 1, 0)
        end_dt = dt.datetime(2021, 1, 2, 21)
        list_dts = [
            dt.datetime(2021, 1, 1, 3),
            dt.datetime(2021, 1, 1, 9),
            dt.datetime(2021, 1, 1, 15),
            dt.datetime(2021, 1, 1, 18),
            dt.datetime(2021, 1, 1, 21),
            dt.datetime(2021, 1, 2, 0),
            dt.datetime(2021, 1, 2, 3),
            dt.datetime(2021, 1, 2, 9),
            dt.datetime(2021, 1, 2, 15),
            dt.datetime(2021, 1, 2, 18),
        ]

        # When
        result = get_missing_datetimes(start_dt, end_dt, list_dts)

        # Then
        assert result == [
            dt.datetime(2021, 1, 1, 0),
            dt.datetime(2021, 1, 1, 6),
            dt.datetime(2021, 1, 1, 12),
            dt.datetime(2021, 1, 2, 6),
            dt.datetime(2021, 1, 2, 12),
            dt.datetime(2021, 1, 2, 21),
        ]
