import datetime as dt

from src.repository import WeatherRepository

from ..value_objects import Laps


class LapsService:
    MF_LAPS_DURATION = 3

    def __init__(self, app_repository: WeatherRepository) -> None:
        self._app_repository = app_repository

    def get_missing_laps(self, start_time: dt.datetime, end_time: dt.datetime, laps: list[Laps]) -> list[Laps]:
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

    def get_available_laps_since(self, since: dt.datetime) -> list[Laps]:
        all_saved_data_files = self._app_repository.list_datasets()
        all_saved_dt = [
            self._parse_datetime_from_filename(file) - dt.timedelta(hours=self.MF_LAPS_DURATION)
            for file in all_saved_data_files
            if file.endswith(".csv")
        ]
        all_saved_dt = [
            Laps(start_time=saved_dt, duration_hours=self.MF_LAPS_DURATION)
            for saved_dt in all_saved_dt
            if saved_dt >= since
        ]

        return all_saved_dt

    @staticmethod
    def _parse_datetime_from_filename(filename: str) -> dt.datetime:
        return dt.datetime.strptime(filename, "%Y-%m-%d-%H.csv")
