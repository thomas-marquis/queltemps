import datetime as dt
from abc import ABC, abstractmethod

from ..entities import Record
from ..value_objects import Laps


class AppRepository(ABC):
    @abstractmethod
    def get_available_laps_since(self, since: dt.datetime) -> list[Laps]:
        raise NotImplementedError()

    @abstractmethod
    def save_many_records(self, records: list[Record]) -> None:
        raise NotImplementedError()


class WeatherDataRepository(ABC):
    @abstractmethod
    def collect_record(self, laps: Laps) -> Record:
        raise NotImplementedError()
