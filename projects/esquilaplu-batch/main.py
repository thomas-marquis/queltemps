import time
from tqdm import tqdm
import random
import io
import datetime as dt
import pandas as pd
import requests
import os

MAX_SCRAPPING = -1
"""Max number of data to scrap..

Set to -1 to scrap all available data.
"""


def get_s3_client():
    pass


def load_dataset(date: dt.datetime) -> pd.DataFrame:
    hour = date.strftime("%H")
    date_id = "".join([
        str(date.year),
        date.strftime("%m"),
        date.strftime("%d"),
    ])
    
    dt_id = date_id + hour
    
    url = f"https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/synop.{dt_id}.csv"
    headers = {
        "Referer": f"https://donneespubliques.meteofrance.fr/?fond=donnee_libre&prefixe=Txt%2FSynop%2Fsynop&extension=csv&date={date_id}&reseau={hour}",
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



def save_dataset(dir_path: str, df: pd.DataFrame, datetime: dt.datetime) -> None:
    path = f"{dir_path}/{datetime.date().isoformat()}-{datetime.hour}.csv"
    df.to_csv(path, index=False, header=True, sep=";")




def _parse_datetime_from_filename(filename: str) -> dt.datetime:
    return dt.datetime.strptime(filename, "%Y-%m-%d-%H.csv")


def list_saved_dataset_datetimes(dir_path: str, since: dt.datetime) -> list[dt.datetime]:
    all_saved_data_files = os.listdir(dir_path)
    all_saved_dt = [_parse_datetime_from_filename(file) for file in all_saved_data_files if file.endswith(".csv")]
    all_saved_dt = [dt for dt in all_saved_dt if dt >= since]
    all_saved_dt.sort()
    return all_saved_dt


def get_missing_datetimes(start_dt: dt.datetime, end_dt: dt.datetime, list_dts: dt.datetime) -> list[dt.datetime]:
    HOURS = [0, 3, 6, 9, 12, 15, 18, 21]

    missing_dts = []
    current_dt = dt.datetime(start_dt.year, start_dt.month, start_dt.day, 0)

    while current_dt <= end_dt:
        if current_dt.hour not in HOURS:
            current_dt += dt.timedelta(hours=1)
            continue

        if current_dt not in list_dts:
            missing_dts.append(current_dt)
        current_dt += dt.timedelta(hours=3)

    return missing_dts


def get_oldest_available_datetime() -> dt.datetime:
    return dt.datetime.now() - dt.timedelta(days=14)



def main():
    data_dir_path = "data"

    oldest_dt = get_oldest_available_datetime()
    existing_datetimes = list_saved_dataset_datetimes(data_dir_path, oldest_dt)
    
    newest_dt = dt.datetime.now() - dt.timedelta(hours=5)
    missing_dts = get_missing_datetimes(oldest_dt, newest_dt, existing_datetimes)
    
    i = 0
    for missing_dt in tqdm(missing_dts):
        df = load_dataset(missing_dt)
        
        wait_sec: float = random.uniform(0.5, 2.)
        time.sleep(wait_sec)
        
        save_dataset(data_dir_path, df, missing_dt)

        i += 1
        if MAX_SCRAPPING > 0 and i >= MAX_SCRAPPING:
            break


if __name__ == '__main__':
    main()
