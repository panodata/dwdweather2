import pytest

from dwdweather.core import DwdWeather


def test_dwdclass_init_no_args():
    """
    Test class can be initialized without arguments.
    """
    dw = DwdWeather()


@pytest.mark.parametrize("resolution", ["hourly", "10_minutes"])
def test_dwdclass_init(resolution):
    """
    Test class can be initialized with plausible resolutions.
    """
    dw = DwdWeather(resolution=resolution)
