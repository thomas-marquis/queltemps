import datetime as dt
import re

import pytest
from easy_testing import DataFrameBuilder

from src.domain.entities import Record
from src.domain.exceptions import WeatherRecordError
from src.domain.value_objects import Laps
from src.infrastructure.factories.mf_record import MeteoFranceRecordFactory


class TestMeteoFranceRecordFactory:
    @pytest.fixture
    def factory(self):
        return MeteoFranceRecordFactory

    class TestFromDataFrame:
        def test_should_build_record_from_dataframe_with_default_laps_duration(self, factory):
            # Given
            dataframe = (
                DataFrameBuilder.a_dataframe()
                .with_columns(["date", "numer_sta", "rr1", "rr3", "rr6", "rr12", "rr24"])
                .with_dtypes(
                    date="datetime64[ns]", rr1="float64", rr3="float64", rr6="float64", rr12="float64", rr24="float64"
                )
                .with_row(
                    date=dt.datetime(2021, 1, 1, 3, 0, 0), numer_sta=7510, rr1=0.1, rr3=0.2, rr6=0.3, rr12=0.4, rr24=0.5
                )
                .with_row(
                    date=dt.datetime(2021, 1, 1, 3, 0, 0), numer_sta=7520, rr1=0.0, rr3=0.0, rr6=0.0, rr12=0.0, rr24=2.5
                )
                .build()
            )
            expected_record = Record(
                laps=Laps(start_time=dt.datetime(2021, 1, 1, 0), duration_hours=3),
                rainfall_mm=0.2,
            )

            # When
            result = factory.from_dataframe(dataframe)

            # Then
            assert result == expected_record

        @pytest.mark.parametrize(
            "laps_duration_hr, expected_rainfall_mm, expected_start_date",
            [
                pytest.param(1, 0.1, dt.datetime(2021, 1, 30, 11, 0, 0), id="1h"),
                pytest.param(3, 0.2, dt.datetime(2021, 1, 30, 9, 0, 0), id="3h"),
                pytest.param(6, 0.3, dt.datetime(2021, 1, 30, 6, 0, 0), id="6h"),
                pytest.param(12, 0.4, dt.datetime(2021, 1, 30, 0, 0, 0), id="12h"),
                pytest.param(24, 0.5, dt.datetime(2021, 1, 29, 12, 0, 0), id="24h"),
            ],
        )
        def test_should_return_record_with_laps_duration_corresponding_rainfall(
            self, factory, laps_duration_hr, expected_rainfall_mm, expected_start_date
        ):
            # Given
            dataframe = (
                DataFrameBuilder.a_dataframe()
                .with_columns(["date", "numer_sta", "rr1", "rr3", "rr6", "rr12", "rr24"])
                .with_dtypes(
                    date="datetime64[ns]", rr1="float64", rr3="float64", rr6="float64", rr12="float64", rr24="float64"
                )
                .with_row(
                    date=dt.datetime(2021, 1, 30, 12, 0, 0),
                    numer_sta=7510,
                    rr1=0.1,
                    rr3=0.2,
                    rr6=0.3,
                    rr12=0.4,
                    rr24=0.5,
                )
                .with_row(
                    date=dt.datetime(2021, 1, 30, 12, 0, 0),
                    numer_sta=7520,
                    rr1=0.0,
                    rr3=0.0,
                    rr6=0.0,
                    rr12=0.0,
                    rr24=2.5,
                )
                .build()
            )
            expected_record = Record(
                laps=Laps(start_time=expected_start_date, duration_hours=laps_duration_hr),
                rainfall_mm=expected_rainfall_mm,
            )

            # When
            result = factory.from_dataframe(dataframe, laps_duration_hr=laps_duration_hr)

            # Then
            assert result == expected_record

        def test_should_raise_when_no_station_data(self, factory):
            # Given
            dataframe = (
                DataFrameBuilder.a_dataframe()
                .with_columns(["date", "numer_sta", "rr1", "rr3", "rr6", "rr12", "rr24"])
                .with_dtypes(
                    date="datetime64[ns]", rr1="float64", rr3="float64", rr6="float64", rr12="float64", rr24="float64"
                )
                .with_row(
                    date=dt.datetime(2021, 1, 1, 3, 0, 0), numer_sta=7520, rr1=0.0, rr3=0.0, rr6=0.0, rr12=0.0, rr24=2.5
                )
                .build()
            )

            # When & Then
            with pytest.raises(WeatherRecordError):
                factory.from_dataframe(dataframe)

        def test_should_raise_when_invalid_laps_duration(self, factory):
            # Given
            dataframe = (
                DataFrameBuilder.a_dataframe()
                .with_columns(["date", "numer_sta", "rr1", "rr3", "rr6", "rr12", "rr24"])
                .with_dtypes(
                    date="datetime64[ns]", rr1="float64", rr3="float64", rr6="float64", rr12="float64", rr24="float64"
                )
                .with_row(
                    date=dt.datetime(2021, 1, 1, 3, 0, 0), numer_sta=7510, rr1=0.0, rr3=0.0, rr6=0.0, rr12=0.0, rr24=2.5
                )
                .build()
            )

            # When & Then
            with pytest.raises(ValueError, match=re.escape("Invalid laps duration: 2")):
                factory.from_dataframe(dataframe, laps_duration_hr=2)
