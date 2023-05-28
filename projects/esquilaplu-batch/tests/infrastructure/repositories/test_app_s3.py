import datetime as dt

import pytest

from src.domain.value_objects import Laps
from src.infrastructure.repositories.app_s3 import AppS3Repository


class TestAppS3Repository:
    @pytest.fixture(autouse=True)
    def mock_boto3(self, mocker):
        return mocker.patch(f"{AppS3Repository.__module__}.boto3")

    @pytest.fixture
    def mock_s3_client(self, mock_boto3):
        return mock_boto3.client.return_value

    @pytest.fixture
    def repository(self):
        return AppS3Repository()

    class TestGetAvailableLapsSince:
        def test_should_return_empty_list_when_no_file(self, repository, mock_s3_client):
            # Given
            mock_s3_client.list_objects_v2.return_value = {
                "Contents": [],
            }

            # When
            result = repository.get_available_laps_since(dt.datetime(2021, 1, 1))

            # Then
            assert result == []

        def test_should_list_existing_file_as_datetime(self, repository, mock_s3_client):
            # Given
            mock_s3_client.list_objects_v2.return_value = {
                "Contents": [
                    {"Key": "esquilaplu/2021-01-01-03.csv"},
                    {"Key": "esquilaplu/2021-01-01-04.csv"},
                    {"Key": "esquilaplu/2021-01-02-03.csv"},
                ],
            }

            # When
            result = repository.get_available_laps_since(dt.datetime(2021, 1, 1, 1))

            # Then
            assert result == [
                Laps(start_time=dt.datetime(2021, 1, 1, 1), duration_hours=3),
                Laps(start_time=dt.datetime(2021, 1, 2, 0), duration_hours=3),
            ]