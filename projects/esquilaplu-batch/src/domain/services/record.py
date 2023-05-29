import datetime as dt
import logging

from tqdm import tqdm

from ..exceptions import WeatherCollectionError
from ..ports.inner import LapService, RecordService
from ..ports.outer import AppRepository, WeatherDataRepository


class RecordServiceImpl(RecordService):
    def __init__(
        self,
        weather_repository: WeatherDataRepository,
        app_repository: AppRepository,
        now: dt.datetime,
        laps_service: LapService,
        max_collect_history_hr: int,
        min_collect_history_hr: int,
        max_collect_iterations: int = -1,
    ) -> None:
        self._logger = logging.getLogger(__name__)
        self._record_repository = weather_repository
        self._app_repository = app_repository
        self._laps_service = laps_service
        self._now = now
        self._max_collect_history_hr = max_collect_history_hr
        self._min_collect_history_hr = min_collect_history_hr
        self._max_collect_iterations = max_collect_iterations

    def update_records(self) -> None:
        start_time = self._now - dt.timedelta(hours=self._max_collect_history_hr)
        end_time = self._now - dt.timedelta(hours=self._min_collect_history_hr)

        missing_laps = self._laps_service.get_missing_laps(start_time=start_time, end_time=end_time)

        records = []
        for idx, laps in tqdm(enumerate(missing_laps)):
            try:
                record = self._record_repository.collect_record(laps=laps)
                records.append(record)
            except WeatherCollectionError:
                self._logger.error(f"Error while collecting weather data for {laps}. Skipping.")
                continue

            if self._max_collect_iterations > 0 and idx + 1 >= self._max_collect_iterations:
                break

        self._app_repository.save_many_records(records=records)
