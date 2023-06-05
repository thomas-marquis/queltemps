import datetime as dt
from abc import ABC, abstractmethod
from typing import Any

from ..entities import Record
from ..value_objects import Laps


class AppRepository(ABC):
    @abstractmethod
    def get_available_laps_since(self, since: dt.datetime) -> list[Laps]:
        """Get all available time laps since a given datetime.

        Args:
            since (dt.datetime): datetime to start searching from

        Returns:
            list[Laps]: list of available laps
        """

    @abstractmethod
    def save_many_records(self, records: list[Record]) -> None:
        """save many records

        Args:
            records (list[Record]): list of records to save
        """

    @abstractmethod
    def save_raw_dataset(self, dataset: Any, laps: Laps) -> None:
        """save raw weather dataset

        Args:
            dataset (Any): dataset to save
            laps (Laps): laps
        """


class WeatherDataRepository(ABC):
    @abstractmethod
    def collect_record(self, laps: Laps) -> Record:
        """Collect weather data for a given laps and return it as a Record.

        Args:
            laps (Laps): laps

        Returns:
            Record: collected record
        """
