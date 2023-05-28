import datetime as dt
import io
import os
import random
import time

import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm import tqdm

from src.domain.services.laps import LapsService
from src.repository import WeatherRepository

load_dotenv("secrets/.env")

MAX_SCRAPPING = -1
"""Max number of data to scrap..

Set to -1 to scrap all available data.
"""


def load_dataset(date: dt.datetime) -> pd.DataFrame:
    hour = date.strftime("%H")
    date_id = "".join(
        [
            str(date.year),
            date.strftime("%m"),
            date.strftime("%d"),
        ]
    )

    dt_id = date_id + hour

    url = f"https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/synop.{dt_id}.csv"
    headers = {
        "Referer": (
            f"https://donneespubliques.meteofrance.fr/?fond=donnee_libre&prefixe=Txt%2FSynop%2Fsynop&extension"
            f"=csv&date={date_id}&reseau={hour}"
        ),
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/112.0",
        "Host": "donneespubliques.meteofrance.fr",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "fr,fr-FR;q=0.8,en-US;q=0.5,en;q=0.3",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
    }

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()

    buffer = io.StringIO(resp.text)

    buffer.seek(0)
    df = pd.read_csv(buffer, sep=";", parse_dates=["date"])

    return df


def save_dataset(repository: WeatherRepository, df: pd.DataFrame, datetime: dt.datetime) -> None:
    dataset_id = f"{datetime.date().isoformat()}-{datetime.hour}"
    repository.save_dataset(dataset_id, df)


def main():
    repository = WeatherRepository()
    laps_service = LapsService()

    start_time = dt.datetime.now() - dt.timedelta(days=14)
    existing_datetimes = laps_service.get_available_laps_since(repository, since=start_time)

    end_time = dt.datetime.now() - dt.timedelta(hours=5)
    missing_dts = laps_service.get_missing_laps(start_time, end_time, existing_datetimes)

    i = 0
    for missing_dt in tqdm(missing_dts):
        df = load_dataset(missing_dt)

        wait_sec: float = random.uniform(0.5, 2.0)
        time.sleep(wait_sec)

        save_dataset(repository, df, missing_dt)

        i += 1
        if MAX_SCRAPPING > 0 and i >= MAX_SCRAPPING:
            break


if __name__ == "__main__":
    main()
