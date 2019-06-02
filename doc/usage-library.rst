###########################
Using dwdweather as library
###########################

``DwdWeather.query()`` returns a dictionary with the following keys.

* `station_id`: Station identifier, as int.
* `datetime`: The hour as int, e.g. `2013011212`.
* `precipitation_fallen`: Whether or not there has been precipitation within the hour, as int. 0 for no, 1 for yes.
* `precipitation_form`: TODO
* `precipitation_height`: Height of hourly precipitation in mm, as float.
* `precipitation_quality_level`: Data quality level, as int.
* `soiltemp_temperature_002`: Soil temperature in   2 cm depth, as float.
* `soiltemp_temperature_005`: Soil temperature in   5 cm depth, as float.
* `soiltemp_temperature_010`: Soil temperature in  10 cm depth, as float.
* `soiltemp_temperature_020`: Soil temperature in  20 cm depth, as float.
* `soiltemp_temperature_050`: Soil temperature in  50 cm depth, as float.
* `soiltemp_temperature_100`: Soil temperature in 100 cm depth, as float.
* `soiltemp_quality_level`: soil temperature quality level, as int.
* `solar_duration`: sunshine duration in minutes, as int.
* `solar_sky`: TODO
* `solar_global`: TODO
* `solar_atmosphere`: TODO
* `solar_zenith`: TODO
* `solar_quality_level`: quality level of solar data, as int.
* `sun_duration`: Duration of sunshine per hour in minutes, as float.
* `sun_quality_level`: quality level of sunshine data, as int.
* `airtemp_humidity`: relative air humidity in percent, as float.
* `airtemp_temperature`: air temperature in degrees centigrade, as float.
* `airtemp_quality_level`: data quality level of air temperature and humidity data, as int.
* `wind_direction`: wind direction in degrees, as int (0 - 360).
* `wind_speed`: wind speed in meters per second, as float.
* `wind_quality_level`: wind data quality level, as int.

TODO: Update these fields for the "hourly" resolution, add the fields for the "10_minutes" resolution.
