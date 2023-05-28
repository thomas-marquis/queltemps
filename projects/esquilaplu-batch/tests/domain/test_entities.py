import datetime as dt

from src.domain.entities import Record
from src.domain.value_objects import Laps


class TestRecord:
    def test_should_equality_return_true_when_same_laps_and_rainfall(self):
        # Given
        laps = Laps(start_time=dt.datetime(2021, 1, 1, 0), duration_hours=3)
        record1 = Record(laps=laps, rainfall_mm=1.0)
        record2 = Record(laps=laps, rainfall_mm=2.0)

        # When
        result = record1 == record2

        # Then
        assert result is True

    def test_should_be_unique_when_same_laps(self):
        # Given
        laps = Laps(start_time=dt.datetime(2021, 1, 1, 0), duration_hours=3)
        record1 = Record(laps=laps, rainfall_mm=1.0)
        record2 = Record(laps=laps, rainfall_mm=2.0)

        # When
        result = {record1, record2}

        # Then
        assert len(result) == 1
        assert record1 in result
        assert record2 in result
