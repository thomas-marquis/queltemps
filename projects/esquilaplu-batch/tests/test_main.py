import datetime as dt
from unittest.mock import MagicMock

from main import WeatherRepository, get_available_laps_since, get_missing_laps

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
            "2021-01-01-03.csv",
            "2021-01-01-04.csv",
            "2021-01-02-03.csv",
        ]

        # When
        result = get_available_laps_since(mock_repository, dt.datetime(2021, 1, 1, 1))

        # Then
        assert result == [
            Laps(start_time=dt.datetime(2021, 1, 1, 1), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 2, 0), duration_hours=3),
        ]


class TestGetMissingLaps:
    def test_should_return_empty_list_when_no_missing(self):
        # Given
        start_dt = dt.datetime(2021, 1, 1, 0)
        end_dt = dt.datetime(2021, 1, 1, 23)
        available_laps = [
            Laps(start_time=dt.datetime(2021, 1, 1, 0), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 1, 3), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 1, 6), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 1, 9), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 1, 12), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 1, 15), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 1, 18), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 1, 21), duration_hours=3),
        ]

        # When
        result = get_missing_laps(start_dt, end_dt, available_laps)

        # Then
        assert result == []

    def test_should_return_one_missing_dt(self):
        # Given
        start_dt = dt.datetime(2021, 1, 1, 0)
        end_dt = dt.datetime(2021, 1, 1, 23)
        available_laps = [
            Laps(start_time=dt.datetime(2021, 1, 1, 0), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 1, 3), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 1, 6), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 1, 9), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 1, 15), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 1, 18), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 1, 21), duration_hours=3),
        ]

        # When
        result = get_missing_laps(start_dt, end_dt, available_laps)

        # Then
        # assert result == [dt.datetime(2021, 1, 1, 12)]
        assert result == [
            Laps(start_time=dt.datetime(2021, 1, 1, 12), duration_hours=3),
        ]

    def test_should_return_many_missing_dt_when_several_missing(self):
        # Given
        start_dt = dt.datetime(2021, 1, 1, 1)
        end_dt = dt.datetime(2021, 1, 1, 23)
        available_laps = [
            Laps(start_time=dt.datetime(2021, 1, 1, 0), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 1, 3), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 1, 9), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 1, 15), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 1, 18), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 1, 21), duration_hours=3),
        ]

        # When
        result = get_missing_laps(start_dt, end_dt, available_laps)

        # Then
        assert result == [
            Laps(start_time=dt.datetime(2021, 1, 1, 6), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 1, 12), duration_hours=3),
        ]

    def test_should_return_missing_dt_from_many_days(self):
        # Given
        start_dt = dt.datetime(2021, 1, 1, 0)
        end_dt = dt.datetime(2021, 1, 2, 21)
        available_laps = [
            Laps(start_time=dt.datetime(2021, 1, 1, 3), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 1, 9), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 1, 15), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 1, 18), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 1, 21), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 2, 0), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 2, 3), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 2, 9), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 2, 15), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 2, 18), duration_hours=3),
        ]

        # When
        result = get_missing_laps(start_dt, end_dt, available_laps)

        # Then
        assert result == [
            Laps(start_time=dt.datetime(2021, 1, 1, 0), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 1, 6), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 1, 12), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 2, 6), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 2, 12), duration_hours=3),
            Laps(start_time=dt.datetime(2021, 1, 2, 21), duration_hours=3),
        ]
