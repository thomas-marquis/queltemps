import datetime as dt
from unittest.mock import MagicMock

import pytest

from src.domain.ports.outer import AppRepository
from src.domain.services.laps import LapsService
from src.domain.value_objects import Laps


class TestLapsService:
    @pytest.fixture
    def mock_app_repository(self):
        return MagicMock(spec=AppRepository)

    @pytest.fixture
    def service(self, mock_app_repository):
        return LapsService(app_repository=mock_app_repository)

    class TestGetMissingLaps:
        def test_should_return_empty_list_when_no_missing(self, service, mock_app_repository):
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
            mock_app_repository.get_available_laps_since.return_value = available_laps

            # When
            result = service.get_missing_laps(start_dt, end_dt)

            # Then
            assert result == []
            mock_app_repository.get_available_laps_since.assert_called_once_with(since=start_dt)

        def test_should_return_one_missing_dt(self, service, mock_app_repository):
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
            mock_app_repository.get_available_laps_since.return_value = available_laps

            # When
            result = service.get_missing_laps(start_dt, end_dt)

            # Then
            # assert result == [dt.datetime(2021, 1, 1, 12)]
            assert result == [
                Laps(start_time=dt.datetime(2021, 1, 1, 12), duration_hours=3),
            ]

        def test_should_return_many_missing_dt_when_several_missing(self, service, mock_app_repository):
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
            mock_app_repository.get_available_laps_since.return_value = available_laps

            # When
            result = service.get_missing_laps(start_dt, end_dt)

            # Then
            assert result == [
                Laps(start_time=dt.datetime(2021, 1, 1, 6), duration_hours=3),
                Laps(start_time=dt.datetime(2021, 1, 1, 12), duration_hours=3),
            ]

        def test_should_return_missing_dt_from_many_days(self, service, mock_app_repository):
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
            mock_app_repository.get_available_laps_since.return_value = available_laps

            # When
            result = service.get_missing_laps(start_dt, end_dt)

            # Then
            assert result == [
                Laps(start_time=dt.datetime(2021, 1, 1, 0), duration_hours=3),
                Laps(start_time=dt.datetime(2021, 1, 1, 6), duration_hours=3),
                Laps(start_time=dt.datetime(2021, 1, 1, 12), duration_hours=3),
                Laps(start_time=dt.datetime(2021, 1, 2, 6), duration_hours=3),
                Laps(start_time=dt.datetime(2021, 1, 2, 12), duration_hours=3),
                Laps(start_time=dt.datetime(2021, 1, 2, 21), duration_hours=3),
            ]
