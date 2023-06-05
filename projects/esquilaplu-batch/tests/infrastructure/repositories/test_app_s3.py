import datetime as dt
from unittest.mock import call

import pytest
from easy_testing import DataFrameBuilder

from src.domain.entities import Record
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
        return AppS3Repository(bucket="mybucket", root_key="esquilaplu", secret_key="azerty", access_key="coucou")

    class TestInit:
        def test_should_init_s3_client(self, repository, mock_boto3):
            # Then
            mock_boto3.client.assert_called_once_with(
                "s3",
                aws_access_key_id="coucou",
                aws_secret_access_key="azerty",
            )

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
            mock_s3_client.list_objects_v2.assert_called_once_with(
                Bucket="mybucket", Prefix="esquilaplu/raw/meteofrance"
            )

        def test_should_list_existing_file_as_datetime(self, repository, mock_s3_client):
            # Given
            mock_s3_client.list_objects_v2.return_value = {
                "Contents": [
                    {"Key": "esquilaplu/raw/meteofrance/2021-01-01-03.csv"},
                    {"Key": "esquilaplu/raw/meteofrance/2021-01-01-04.csv"},
                    {"Key": "esquilaplu/raw/meteofrance/2021-01-02-03.csv"},
                ],
            }

            # When
            result = repository.get_available_laps_since(dt.datetime(2021, 1, 1, 1))

            # Then
            assert result == [
                Laps(start_time=dt.datetime(2021, 1, 1, 1), duration_hours=3),
                Laps(start_time=dt.datetime(2021, 1, 2, 0), duration_hours=3),
            ]

    class TestSaveRawDataset:
        def test_should_save_dataframe_to_s3_as_csv(self, repository, mock_s3_client):
            # Given
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
            expected_csv = "date;numer_sta;rr1;rr3;rr6;rr12;rr24\n2021-01-30;7510;0.1;0.2;0.3;0.4;0.5\n"
            laps = Laps(start_time=dt.datetime(2021, 1, 1, 1), duration_hours=3)

            # When
            repository.save_raw_dataset(dataframe, laps)

            # Then
            mock_s3_client.put_object.assert_called_once_with(
                Bucket="mybucket",
                Key="esquilaplu/raw/meteofrance/2021-01-01-01.csv",
                Body=expected_csv,
            )

    class TestSaveManyRecords:
        def test_should_save_records_to_s3_as_one_json_file_per_record(self, repository, mock_s3_client):
            # Given
            records = [
                Record(laps=Laps(start_time=dt.datetime(2021, 1, 29, 21), duration_hours=3), rainfall_mm=0.1),
                Record(laps=Laps(start_time=dt.datetime(2021, 1, 30, 0), duration_hours=3), rainfall_mm=0.2),
                Record(laps=Laps(start_time=dt.datetime(2021, 1, 30, 3), duration_hours=3), rainfall_mm=0.3),
            ]

            # When
            repository.save_many_records(records)

            # Then
            assert mock_s3_client.put_object.call_count == 3
            mock_s3_client.put_object.assert_has_calls(
                [
                    call(
                        Bucket="mybucket",
                        Key="esquilaplu/processed/records/2021/01/29/21.json",
                        Body='{"data": '
                        '{"laps": {"start_time": "2021-01-29 21:00:00", "duration_hours": 3}, "rainfall_mm": 0.1}}',
                    ),
                    call(
                        Bucket="mybucket",
                        Key="esquilaplu/processed/records/2021/01/30/00.json",
                        Body='{"data": '
                        '{"laps": {"start_time": "2021-01-30 00:00:00", "duration_hours": 3}, "rainfall_mm": 0.2}}',
                    ),
                    call(
                        Bucket="mybucket",
                        Key="esquilaplu/processed/records/2021/01/30/03.json",
                        Body='{"data": '
                        '{"laps": {"start_time": "2021-01-30 03:00:00", "duration_hours": 3}, "rainfall_mm": 0.3}}',
                    ),
                ]
            )
