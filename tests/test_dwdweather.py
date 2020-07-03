import pytest
from datetime import datetime
from dwdweather.core import DwdWeather


def test_dwdclass_init_no_args():
    """
    Test class can be initialized without arguments.
    """
    dwd = DwdWeather()


@pytest.mark.parametrize("resolution", ["daily", "hourly", "10_minutes"])
def test_dwdclass_init(resolution):
    """
    Test class can be initialized with plausible resolutions.
    """
    dwd = DwdWeather(resolution=resolution)


def test_daily_recent_base():
    # daily, 2020-06-01, Großenkneten
    dwd = DwdWeather(resolution='daily')
    result = dwd.query(44, datetime(2020, 6, 1))
    assert result['datetime'] == 20200601
    assert result['station_id'] == 44
    assert result['wind_gust_max'] is None
    assert result['wind_velocity_mean'] is None
    assert result['precipitation_height'] == 0.0
    assert result['precipitation_form'] == 0
    assert result['sunshine_duration'] == 13.933
    assert result['snow_depth'] is None
    assert result['cloud_cover'] is None
    assert result['vapor_pressure'] == 9.6
    assert result['pressure'] is None
    assert result['temperature'] == 18.0
    assert result['humidity'] == 47.92
    assert result['temperature_max_200'] == 25.4
    assert result['temperature_min_200'] == 8.7
    assert result['temperature_min_005'] == 3.3
    assert result['soil_temperature_002'] is None
    assert result['soil_temperature_005'] == 21.7
    assert result['soil_temperature_010'] == 20.6
    assert result['soil_temperature_020'] == 19.0
    assert result['soil_temperature_050'] == 16.3


def test_daily_recent_more():
    # daily, 2020-04-28, Zugspitze
    dwd = DwdWeather(resolution='daily')
    result = dwd.query(5792, datetime(2020, 4, 28))
    assert result['datetime'] == 20200428
    assert result['station_id'] == 5792
    assert result['wind_gust_max'] == 11.7
    assert result['wind_velocity_mean'] == 4.8
    assert result['precipitation_height'] == 18.8
    assert result['precipitation_form'] == 8
    assert result['sunshine_duration'] == 6.483
    assert result['snow_depth'] == 273.0
    assert result['cloud_cover'] == 6.5
    assert result['vapor_pressure'] == 4.8
    assert result['pressure'] == 703.45
    assert result['temperature'] == -1.6
    assert result['humidity'] == 88.21
    assert result['temperature_max_200'] == 0.8
    assert result['temperature_min_200'] == -3.0
    assert result['temperature_min_005'] is None
    assert result['soil_temperature_002'] is None
    assert result['soil_temperature_005'] is None
    assert result['soil_temperature_010'] is None
    assert result['soil_temperature_020'] is None
    assert result['soil_temperature_050'] is None
    assert result['solar_dhi'] == 1272.0
    assert result['solar_ghi'] == 1644.0
    assert result['solar_atmosphere'] == 2363.0
    assert result['solar_sunshine'] == 6.5


def test_daily_historical_1():
    # daily, 2018-11-29, Großenkneten
    dwd = DwdWeather(resolution='daily')
    result = dwd.query(44, datetime(2018, 11, 29))
    assert result['datetime'] == 20181129
    assert result['station_id'] == 44
    assert result['wind_gust_max'] is None
    assert result['wind_velocity_mean'] is None
    assert result['precipitation_height'] == 2.2
    assert result['precipitation_form'] == 4
    assert result['sunshine_duration'] == 0.0
    assert result['snow_depth'] == 0.0
    assert result['cloud_cover'] is None
    assert result['vapor_pressure'] == 9.1
    assert result['pressure'] is None
    assert result['temperature'] == 7.1
    assert result['humidity'] == 89.88
    assert result['temperature_max_200'] == 8.4
    assert result['temperature_min_200'] == 4.7
    assert result['temperature_min_005'] == 4.0


def test_daily_historical_2():
    # daily, 1937-01-01, Aach
    dwd = DwdWeather(resolution='daily')
    result = dwd.query(1, datetime(1937, 1, 1))
    assert result['datetime'] == 19370101
    assert result['station_id'] == 1
    assert result['wind_gust_max'] is None
    assert result['wind_velocity_mean'] is None
    assert result['precipitation_height'] == 0.0
    assert result['precipitation_form'] == 0
    assert result['sunshine_duration'] is None
    assert result['snow_depth'] == 0.0
    assert result['cloud_cover'] == 6.3
    assert result['vapor_pressure'] is None
    assert result['pressure'] is None
    assert result['temperature'] == -0.5
    assert result['humidity'] is None
    assert result['temperature_max_200'] == 2.5
    assert result['temperature_min_200'] == -1.6
    assert result['temperature_min_005'] is None


def test_hourly_recent_base():
    # hourly, 2020-06-01T08, Großenkneten
    dwd = DwdWeather(resolution='hourly')
    result = dwd.query(44, datetime(2020, 6, 1, 8))
    assert result['datetime'] == 2020060108
    assert result['station_id'] == 44
    assert result['air_temperature_200'] == 15.3
    assert result['relative_humidity_200'] == 54.0
    assert result['soil_temperature_002'] is None
    assert result['soil_temperature_005'] == 21.0
    assert result['soil_temperature_010'] == 17.7
    assert result['soil_temperature_020'] == 16.5
    assert result['soil_temperature_050'] == 16.3
    assert result['soil_temperature_100'] == 13.3
    assert result['sun_duration'] == 60.0
    assert result['precipitation_height'] == 0
    assert result['precipitation_fallen'] == 0
    assert result['precipitation_form'] is None


def test_hourly_recent_more():
    # hourly, 2020-06-01T08, Neuruppin-Alt Ruppin
    dwd = DwdWeather(resolution='hourly')
    result = dwd.query(96, datetime(2020, 6, 1, 8))
    assert result['datetime'] == 2020060108
    assert result['station_id'] == 96
    assert result['air_temperature_200'] == 18.4
    assert result['relative_humidity_200'] == 49.0
    assert result['soil_temperature_002'] is None
    assert result['soil_temperature_005'] == 19.3
    assert result['soil_temperature_010'] == 16.7
    assert result['soil_temperature_020'] == 16.1
    assert result['soil_temperature_050'] == 16.6
    assert result['soil_temperature_100'] == 14.8
    assert result['pressure_msl'] == 1025.0
    assert result['pressure_station'] == 1018.9
    assert result['visibility_source'] == "P"
    assert result['visibility_value'] == 39870
    assert result['wind_direction'] == 50
    assert result['wind_speed'] == 2.2
    assert result['cloudiness_source'] == "P"
    assert result['cloudiness_total_cover'] == 0


def test_hourly_recent_solar():
    # hourly, 2020-05-19T08, Zugspitze
    dwd = DwdWeather(resolution='hourly')
    result = dwd.query(5792, datetime(2020, 5, 19, 8))
    assert result['datetime'] == 2020051908
    assert result['station_id'] == 5792
    assert result['solar_dhi'] is None
    assert result['solar_ghi'] == 256.0
    assert result['solar_atmosphere'] == 83.0
    assert result['solar_sunshine'] == 60
    assert result['solar_zenith'] == 50.37


def test_10_minutes_recent_base():
    # 10 minutes, 2020-06-01T08:00, Großenkneten
    dwd = DwdWeather(resolution='10_minutes')
    result = dwd.query(44, datetime(2020, 6, 1, 8, 0))
    assert result['datetime'] == 202006010800
    assert result['station_id'] == 44
    assert result['air_temperature_005'] == 21.2
    assert result['air_temperature_200'] == 16.0
    assert result['relative_humidity_200'] == 52.3
    assert result['dewpoint_temperature_200'] == 6.2
    assert result['solar_sunshine'] == 0.167


def test_10_minutes_recent_more():
    # 10 minutes, 2020-06-01T08:00, Zugspitze
    dwd = DwdWeather(resolution='10_minutes')
    result = dwd.query(5792, datetime(2020, 6, 1, 8, 0))
    assert result['datetime'] == 202006010800
    assert result['station_id'] == 5792
    assert result['pressure_station'] == 713.3
    assert result['solar_dhi'] == 46.2
    assert result['solar_ghi'] == 46.8
    assert result['solar_atmosphere'] == 13.4
    assert result['solar_sunshine'] == 0.167
