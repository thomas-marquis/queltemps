import datetime as dt

from src.domain.ports.outer import AppRepository

from ..value_objects import Laps


class LapsService:
    MF_LAPS_DURATION = 3

    def __init__(self, app_repository: AppRepository) -> None:
        self._app_repository = app_repository

    def get_missing_laps(self, start_time: dt.datetime, end_time: dt.datetime) -> list[Laps]:
        laps = self._app_repository.get_available_laps_since(since=start_time)

        HOURS = [0, 3, 6, 9, 12, 15, 18, 21]

        missing_dts = []
        current_dt = dt.datetime(start_time.year, start_time.month, start_time.day, 0)

        while current_dt <= end_time:
            if current_dt.hour not in HOURS:
                current_dt += dt.timedelta(hours=1)
                continue

            if current_dt not in laps:
                missing_dts.append(Laps(start_time=current_dt, duration_hours=3))
            current_dt += dt.timedelta(hours=3)

        return missing_dts
