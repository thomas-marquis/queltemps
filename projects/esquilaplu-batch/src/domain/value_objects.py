from dataclasses import dataclass
import datetime as dt


@dataclass(frozen=True)
class Laps:
    start_time: dt.datetime
    duration_hours: int

    def __lt__(self, other):
        return self.start_time < other.start_time
