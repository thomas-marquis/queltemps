import datetime as dt
from unittest.mock import MagicMock, call

import pytest

from src.domain.entities import Record
from src.domain.exceptions import WeatherCollectionError
from src.domain.ports.inner import LapService
from src.domain.ports.outer import AppRepository, WeatherDataRepository
from src.domain.services.record import RecordServiceImpl
from src.domain.value_objects import Laps

fake_now = dt.datetime(2021, 1, 30, 10, 0, 0)


class TestRecordServiceImpl:
    @pytest.fixture
    def mock_lap_service(self):
        return MagicMock(spec=LapService)

    @pytest.fixture
    def mock_app_repository(self):
        return MagicMock(spec=AppRepository)

    @pytest.fixture
    def mock_weather_repository(self):
        return MagicMock(spec=WeatherDataRepository)

    @pytest.fixture
    def service(self, mock_app_repository, mock_weather_repository, mock_lap_service):
        service = RecordServiceImpl(
            weather_repository=mock_weather_repository,
            app_repository=mock_app_repository,
            now=fake_now,
            laps_service=mock_lap_service,
            max_collect_history_hr=14 * 24,
            min_collect_history_hr=5,
        )
        return service

    class TestUpdateRecords:
        def test_should_get_missing_laps_since_14_days(self, mock_lap_service, service):
            # Given
            mock_lap_service.get_missing_laps.return_value = []

            # When
            service.update_records()

            # Then
            expected_start_time = dt.datetime(2021, 1, 16, 10, 0, 0)
            expected_end_time = dt.datetime(2021, 1, 30, 5, 0, 0)
            mock_lap_service.get_missing_laps.assert_called_once_with(
                start_time=expected_start_time, end_time=expected_end_time
            )

        def test_should_collect_record_for_each_missing_laps(self, mock_lap_service, mock_weather_repository, service):
            # Given
            mock_lap_service.get_missing_laps.return_value = [
                Laps(start_time=dt.datetime(2021, 1, 16, 10, 0, 0), duration_hours=3),
                Laps(start_time=dt.datetime(2021, 1, 16, 13, 0, 0), duration_hours=3),
                Laps(start_time=dt.datetime(2021, 1, 16, 16, 0, 0), duration_hours=3),
            ]

            # When
            service.update_records()

            # Then
            assert mock_weather_repository.collect_record.call_count == 3
            mock_weather_repository.collect_record.assert_has_calls(
                [
                    call(laps=Laps(start_time=dt.datetime(2021, 1, 16, 10, 0, 0), duration_hours=3)),
                    call(laps=Laps(start_time=dt.datetime(2021, 1, 16, 13, 0, 0), duration_hours=3)),
                    call(laps=Laps(start_time=dt.datetime(2021, 1, 16, 16, 0, 0), duration_hours=3)),
                ]
            )

        def test_should_save_all_collected_records(
            self, mock_lap_service, mock_weather_repository, service, mock_app_repository
        ):
            # Given
            mock_lap_service.get_missing_laps.return_value = [
                Laps(start_time=dt.datetime(2021, 1, 16, 10, 0, 0), duration_hours=3),
                Laps(start_time=dt.datetime(2021, 1, 16, 13, 0, 0), duration_hours=3),
                Laps(start_time=dt.datetime(2021, 1, 16, 16, 0, 0), duration_hours=3),
            ]
            mock_weather_repository.collect_record.side_effect = [
                Record(laps=Laps(start_time=dt.datetime(2021, 1, 16, 10, 0, 0), duration_hours=3), rainfall_mm=0.1),
                Record(laps=Laps(start_time=dt.datetime(2021, 1, 16, 13, 0, 0), duration_hours=3), rainfall_mm=0.2),
                Record(laps=Laps(start_time=dt.datetime(2021, 1, 16, 16, 0, 0), duration_hours=3), rainfall_mm=0.3),
            ]

            # When
            service.update_records()

            # Then
            mock_app_repository.save_many_records.assert_called_once_with(
                records=[
                    Record(laps=Laps(start_time=dt.datetime(2021, 1, 16, 10, 0, 0), duration_hours=3), rainfall_mm=0.1),
                    Record(laps=Laps(start_time=dt.datetime(2021, 1, 16, 13, 0, 0), duration_hours=3), rainfall_mm=0.2),
                    Record(laps=Laps(start_time=dt.datetime(2021, 1, 16, 16, 0, 0), duration_hours=3), rainfall_mm=0.3),
                ]
            )

        def test_should_stop_after_max_collect_iterations(self, mock_lap_service, mock_weather_repository, service):
            # Given
            mock_lap_service.get_missing_laps.return_value = [
                Laps(start_time=dt.datetime(2021, 1, 16, 10, 0, 0), duration_hours=3),
                Laps(start_time=dt.datetime(2021, 1, 16, 13, 0, 0), duration_hours=3),
                Laps(start_time=dt.datetime(2021, 1, 16, 16, 0, 0), duration_hours=3),
            ]
            mock_weather_repository.collect_record.side_effect = [
                Record(laps=Laps(start_time=dt.datetime(2021, 1, 16, 10, 0, 0), duration_hours=3), rainfall_mm=0.1),
                Record(laps=Laps(start_time=dt.datetime(2021, 1, 16, 13, 0, 0), duration_hours=3), rainfall_mm=0.2),
                Record(laps=Laps(start_time=dt.datetime(2021, 1, 16, 16, 0, 0), duration_hours=3), rainfall_mm=0.3),
            ]
            service._max_collect_iterations = 2

            # When
            service.update_records()

            # Then
            assert mock_weather_repository.collect_record.call_count == 2
            mock_weather_repository.collect_record.assert_has_calls(
                [
                    call(laps=Laps(start_time=dt.datetime(2021, 1, 16, 10, 0, 0), duration_hours=3)),
                    call(laps=Laps(start_time=dt.datetime(2021, 1, 16, 13, 0, 0), duration_hours=3)),
                ]
            )

        def test_should_ignore_collection_error(
            self, mock_lap_service, mock_weather_repository, service, mock_app_repository
        ):
            # Given
            mock_lap_service.get_missing_laps.return_value = [
                Laps(start_time=dt.datetime(2021, 1, 16, 10, 0, 0), duration_hours=3),
                Laps(start_time=dt.datetime(2021, 1, 16, 13, 0, 0), duration_hours=3),
                Laps(start_time=dt.datetime(2021, 1, 16, 16, 0, 0), duration_hours=3),
            ]
            mock_weather_repository.collect_record.side_effect = [
                Record(laps=Laps(start_time=dt.datetime(2021, 1, 16, 10, 0, 0), duration_hours=3), rainfall_mm=0.1),
                WeatherCollectionError(),
                Record(laps=Laps(start_time=dt.datetime(2021, 1, 16, 16, 0, 0), duration_hours=3), rainfall_mm=0.3),
            ]

            # When
            service.update_records()

            # Then
            mock_app_repository.save_many_records.assert_called_once_with(
                records=[
                    Record(laps=Laps(start_time=dt.datetime(2021, 1, 16, 10, 0, 0), duration_hours=3), rainfall_mm=0.1),
                    Record(laps=Laps(start_time=dt.datetime(2021, 1, 16, 16, 0, 0), duration_hours=3), rainfall_mm=0.3),
                ]
            )
