dwd-weather
===========

Python client to access DWD weather data (Germany)

### Install

    pip install dwdweather


### Quick reference (Python module)

```python
from dwdweather import DwdWeather
from datetime import datetime

# Your personal DWD GDS FTP credentials
user = "myuser"
passwd = "mypasswd"

# create client
dw = DwdWeather(user=user, passwd=passwd)

# Find closest station to position
closest = dw.nearest_station(lon=7, lat=51)

# The hour you're interested in.
# The example is 2014-03-22 12:00 (UTC).
query_hour = datetime(2014, 3, 22, 12)

result = dw.query(station_id=closest["station_id"], hour=query_hour)
print result
```

`DwdWeather.query()` returns a dict with the following keys:

* `station_id`: Station identifier, as int.
* `datetime`: The hour as int, e.g. `2013011212`.
* `precipitation_fallen`: Whether or not there has been precipitation within the hour, as int. 0 for no, 1 for yes.
* `precipitation_form`: TODO
* `precipitation_height`: Height of hourly precipitation in mm, as float.
* `precipitation_quality_level`: Data quality level, as int.
* `soiltemp_1_depth`: first soil temperature measurement, depth in meters, as float.
* `soiltemp_1_temperature`: first soil temperature measurement, temperature in degrees centigrade, as float.
* `soiltemp_2_depth`: second soil temperature measurement, depth in meters, as float.
* `soiltemp_2_temperature`: second soil temperature measurement, temperature in degrees centigrade, as float.
* `soiltemp_3_depth`: third soil temperature measurement, depth in meters, as float.
* `soiltemp_3_temperature`: third soil temperature measurement, temperature in degrees centigrade, as float.
* `soiltemp_4_depth`: fourth soil temperature measurement, depth in meters, as float.
* `soiltemp_4_temperature`: fourth soil temperature measurement, temperature in degrees centigrade, as float.
* `soiltemp_5_depth`: fifth soil temperature measurement, depth in meters, as float.
* `soiltemp_5_temperature`: fifth soil temperature measurement, temperature in degrees centigrade, as float.
* `soiltemp_quality_level`: soil temperature quality level, as int.
* `sun_duration`: Duration of sunshine per hour in minutes, as float.
* `sun_quality_level`: quality level of sunshine data, as int.
* `sun_structure_version`: version number, as int.
* `temphum_humidity`: relative air humidity in percent, as float.
* `temphum_quality_level`: data qqualit level of air temperature and humidity data, as int.
* `temphum_structure_version`: version number, as int.
* `temphum_temperature`: air temperature in degrees centigrade, as float.
* `wind_direction`: wind direction in degrees, as int (0 - 360).
* `wind_quality_level`: wind data quality level, as int.
* `wind_speed`: wind speed in meters per second, as float.
* `wind_structure_version`: version number, as int.


### Command line utility:

Note that the DWD FTP account credentials have to be set either via command line options (use `dwdweather -h` for details) or via environment variables DWDUSER and DWDPASS. The former take precedence.

Get closest station (first argument is longitude, second is latitude):

    $ dwdweather station 7.0 51.0

Get all stations:

    $ dwdweather stations

Export stations as CSV:

	$ dwdweather stations -t csv -f stations.csv

Export stations as GeoJSON:

	$ dwdweather stations -t geojson -f stations.geojson

Get weather at station for certain hour (UTC):

    $ dwdweather weather 2667 2014060122


### Some notes

* Personal FTP user account with DWD GDS is needed. See [here](http://www.dwd.de/bvbw/appmanager/bvbw/dwdwwwDesktop?_nfpb=true&_pageLabel=_dwdwww_spezielle_nutzer_metdienstleister_datenbezug&T26001030691160718267804gsbDocumentPath=Navigation%2FOeffentlichkeit%2FDatenservice%2FDatenanforderungen%2FDatenbezug%2FGlobalerDatensatz%2Fanmeldung__node.html%3F__nnn%3Dtrue) for details.
* Data is cached in a local sqlite3 database for fast queries.
* The Stations cache is filled upon first request to `DwdWeather.stations()` or `DwdWeather.nearest_station()`
* The Stations cache will not be refreshed automatically. Use `DwdWeather.import_stations()` to do this.
* The Measures cache is filled upon first access to measures using `DwdWeather.query()` and updated whenever a query cannot be fullfilled from the cache.
* The cache by default resides in `~/.dwd-weather` directory. This can be influenced using the `cachepath` argument of `DwdWeather()`.
* The amount of data can be ~60 MB per station for full historic extend and this will of course grow in the future.
* If weather data is queried and the query can't be fullfilled from the cache, data is loaded from the server at every query. Even if the data has been updated a second before. If the server doesn't have data for the requested time (e.g. since it's not yet available), this causes superfluous network traffic and wait time. Certainly space for improvement here.

### Status

This piece of software is in a very early stage. No test cases yet.
Only used unter Python 2.7.5. Use at your own risk.
