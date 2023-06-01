import datetime as dt

import pandas as pd

from src.domain.entities import Record
from src.domain.exceptions import WeatherRecordError
from src.domain.value_objects import Laps


class MeteoFranceRecordFactory:
    MERIGNAC_STATION_ID = 7510

    @staticmethod
    def from_dataframe(dataframe: pd.DataFrame, laps_duration_hr: int = 3) -> Record:
        match (laps_duration_hr):
            case 1:
                rainfall_col = "rr1"
            case 3:
                rainfall_col = "rr3"
            case 6:
                rainfall_col = "rr6"
            case 12:
                rainfall_col = "rr12"
            case 24:
                rainfall_col = "rr24"
            case _:
                raise ValueError(f"Invalid laps duration: {laps_duration_hr}")

        station_row = dataframe.loc[dataframe["numer_sta"] == MeteoFranceRecordFactory.MERIGNAC_STATION_ID, :].head(1)
        if station_row.empty:
            raise WeatherRecordError("No data for Merignac station")

        # convert numpy datetime64 to datetime
        date = station_row["date"].values[0]
        date = pd.Timestamp(date)
        date = dt.datetime(date.year, date.month, date.day, date.hour)

        start_date = date - dt.timedelta(hours=laps_duration_hr)

        return Record(
            laps=Laps(start_time=start_date, duration_hours=laps_duration_hr),
            rainfall_mm=station_row[rainfall_col].values[0],
        )
