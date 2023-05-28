from dataclasses import dataclass

from .value_objects import Laps


@dataclass
class Record:
    laps: Laps
    rainfall_mm: float

    def __eq__(self, other: object) -> bool:
        return self.laps == other.laps

    def __hash__(self) -> int:
        return hash(self.laps)
