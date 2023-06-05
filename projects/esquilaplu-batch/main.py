import datetime as dt
import os

from dotenv import load_dotenv

from src.domain.services.laps import LapsServiceImpl
from src.domain.services.record import RecordServiceImpl
from src.infrastructure.repositories.app_s3 import AppS3Repository
from src.infrastructure.repositories.meteo_france import MeteoFranceRepository

load_dotenv("secrets/.env")


def main():
    app_repository = AppS3Repository(
        bucket=os.getenv("S3_BUCKET"),
        root_key=os.getenv("ROOT_KEY", "esquilaplu"),
        secret_key=os.getenv("SECRET_ACCESS_KEY"),
        access_key=os.getenv("ACCESS_KEY_ID"),
    )
    mf_repository = MeteoFranceRepository(app_repository=app_repository)

    laps_service = LapsServiceImpl(app_repository=app_repository)

    record_service = RecordServiceImpl(
        app_repository=app_repository,
        weather_repository=mf_repository,
        laps_service=laps_service,
        max_collect_history_hr=14 * 24,
        min_collect_history_hr=5,
        now=dt.datetime.now(),
        # max_collect_iterations=5,
    )

    record_service.update_records()


if __name__ == "__main__":
    main()
