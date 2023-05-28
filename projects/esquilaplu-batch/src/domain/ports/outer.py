import datetime as dt
from abc import ABC, abstractmethod

from ..value_objects import Laps


class AppRepository(ABC):
    @abstractmethod
    def get_available_laps_since(since: dt.datetime) -> list[Laps]:
        raise NotImplementedError()
