from src.domain.value_objects import Laps
import datetime as dt

class TestLaps:
    def tests_should_sort_laps_by_start_time(self):
        # Given
        lap1 = Laps(start_time=dt.datetime(2021, 1, 1, 0), duration_hours=3)
        lap2 = Laps(start_time=dt.datetime(2021, 1, 1, 3), duration_hours=3)
        lap3 = Laps(start_time=dt.datetime(2021, 1, 1, 6), duration_hours=3)
        lap4 = Laps(start_time=dt.datetime(2021, 1, 1, 9), duration_hours=3)
        lap5 = Laps(start_time=dt.datetime(2021, 1, 1, 12), duration_hours=3)
        lap6 = Laps(start_time=dt.datetime(2021, 1, 1, 15), duration_hours=3)
        lap7 = Laps(start_time=dt.datetime(2021, 1, 1, 18), duration_hours=3)
        lap8 = Laps(start_time=dt.datetime(2021, 1, 1, 21), duration_hours=3)
        
        # When
        result = sorted([lap2, lap1, lap4, lap3, lap6, lap5, lap8, lap7])
        
        # Then
        assert result == [lap1, lap2, lap3, lap4, lap5, lap6, lap7, lap8]
        
        result = [lap2, lap1, lap4, lap3, lap6, lap5, lap8, lap7]
        result.sort()
        assert result == [lap1, lap2, lap3, lap4, lap5, lap6, lap7, lap8]