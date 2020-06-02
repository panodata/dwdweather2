import pytest
from datetime import datetime
from dwdweather.core import DwdWeather


def test_dwdclass_init_no_args():
    """
    Test class can be initialized without arguments.
    """
    dwd = DwdWeather()


@pytest.mark.parametrize("resolution", ["hourly", "10_minutes"])
def test_dwdclass_init(resolution):
    """
    Test class can be initialized with plausible resolutions.
    """
    dwd = DwdWeather(resolution=resolution)


def test_hourly_recent_base():
    # hourly, 2020-06-01T08, Aachen
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
    # hourly, 2020-06-01T08
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
    assert result['pressure_normalized'] == 1025.0
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
    # 10 minutes, 2020-06-01T08:00, Aachen
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
