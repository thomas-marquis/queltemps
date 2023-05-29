import datetime as dt
from abc import ABC, abstractmethod

from ..value_objects import Laps


class RecordService(ABC):
    @abstractmethod
    def update_records(self) -> None:
        pass


class LapService(ABC):
    @abstractmethod
    def get_missing_laps(self, start_time: dt.datetime, end_time: dt.datetime) -> list[Laps]:
        pass
