import datetime as dt
from dataclasses import dataclass


@dataclass(frozen=True)
class Laps:
    start_time: dt.datetime
    duration_hours: int

    def __lt__(self, other):
        return self.start_time < other.start_time
