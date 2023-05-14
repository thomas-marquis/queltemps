from main import get_oldest_available_datetime, list_saved_dataset_datetimes, get_missing_datetimes
import datetime as dt



class TestListSavedDatasetDatetimes:
    def test_should_return_empty_list_when_no_file(self, tmp_path):
        # Given
        # When
        result = list_saved_dataset_datetimes(tmp_path, dt.datetime(2021, 1, 1))
        # Then
        assert result == []
        
    def test_should_list_existing_file_as_datetime(self, tmp_path):
        # Given
        (tmp_path / "2021-01-01-00.csv").touch()
        (tmp_path / "2021-01-01-01.csv").touch()
        (tmp_path / "2021-01-02-00.csv").touch()
        # When
        result = list_saved_dataset_datetimes(tmp_path, dt.datetime(2021, 1, 1, 1))
        # Then
        assert result == [dt.datetime(2021, 1, 1, 1), dt.datetime(2021, 1, 2, 0)]
        
        
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
