class BaseWeatherException(Exception):
    pass


class WeatherCollectionError(BaseWeatherException):
    pass


class WeatherRecordError(WeatherCollectionError):
    pass
