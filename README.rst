.. image:: https://img.shields.io/badge/Python-3-green.svg
    :target: https://github.com/panodata/dwdweather2

.. image:: https://img.shields.io/pypi/v/dwdweather2.svg
    :target: https://pypi.org/project/dwdweather2/

.. image:: https://img.shields.io/pypi/l/dwdweather2.svg
    :target: https://pypi.org/project/dwdweather2/

.. image:: https://img.shields.io/pypi/dm/dwdweather2.svg
    :target: https://pypi.org/project/dwdweather2/

.. image:: https://img.shields.io/github/tag/panodata/dwdweather2.svg
    :target: https://github.com/panodata/dwdweather2

.. image:: https://assets.okfn.org/images/ok_buttons/od_80x15_red_green.png
    :target: https://okfn.org/opendata/

.. image:: https://assets.okfn.org/images/ok_buttons/oc_80x15_blue.png
    :target: https://okfn.org/opendata/

.. image:: https://assets.okfn.org/images/ok_buttons/os_80x15_orange_grey.png
    :target: https://okfn.org/opendata/

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/python/black


.. _dwdweather2-readme:

###########
dwdweather2
###########

Python client to access weather data from Deutscher Wetterdienst
(`DWD <https://www.dwd.de/>`__), the federal meteorological service in
Germany.

.. note::

    Please note this library will gradually be phased out.
    You should consider using its successor library "`Wetterdienst <https://github.com/earthobservations/wetterdienst>`_".


************
Installation
************
::

   pip install dwdweather2


********
Synopsis
********

Command line usage
==================

Get all stations with ``daily`` resolution::

    dwdweather stations --resolution=daily

Get all stations with ``hourly`` resolution (default)::

    dwdweather stations --resolution=hourly

Get all stations with ``10_minutes`` resolution::

    dwdweather stations --resolution=10_minutes

Get closest station (first argument is longitude, second is latitude)::

    dwdweather station 7.0 51.0

Export stations as CSV::

    dwdweather stations --type csv --file stations.csv

Export stations as GeoJSON::

    dwdweather stations --type geojson --file stations.geojson

Get weather at station for certain hour (UTC)::

    dwdweather weather 2667 2019-06-01T15:00

To restrict the import to specified categories, run the program like::

    dwdweather weather 2667 2019-06-01T15:00 --categories air_temperature precipitation pressure

Finally, to drop the cache database before performing any work, use the ``--reset-cache`` option::

    dwdweather stations --reset-cache

Choose dataset with ``daily`` resolution::

    dwdweather weather 44 2020-06-01 --resolution=daily

Choose dataset with ``hourly`` resolution::

    dwdweather weather 44 2020-06-01T08 --resolution=hourly

Choose dataset with ``10_minutes`` resolution::

    dwdweather weather 2667 2019-06-01T15:20 --resolution=10_minutes


Usage as library
================

.. code:: python

   from datetime import datetime
   from dwdweather import DwdWeather

   # Create client object.
   dwd = DwdWeather(resolution="hourly")

   # Find closest station to position.
   closest = dwd.nearest_station(lon=7.0, lat=51.0)

   # The hour you're interested in.
   # The example is 2014-03-22 12:00 (UTC).
   query_hour = datetime(2014, 3, 22, 12)

   result = dwd.query(station_id=closest["station_id"], timestamp=query_hour)
   print(result)

``DwdWeather.query()`` returns a dictionary with the full set of
possible keys as outlined in ``doc/usage-library.rst``.


*****
Notes
*****

-  Data is cached in a local sqlite3 database to improve query
   performance for consecutive invocations.
-  The "stations cache" is filled upon first request to
   ``DwdWeather.stations()`` or ``DwdWeather.nearest_station()``
-  The "stations cache" will not be refreshed automatically. Use
   ``DwdWeather.import_stations()`` to do this.
-  The "measures cache" is filled upon first access to measures using
   ``DwdWeather.query()`` and updated whenever a query cannot be
   fulfilled from the cache.
-  The cache by default resides in the ``~/.dwd-weather`` directory.
   This can be controlled using the ``cachepath`` argument of
   ``DwdWeather()``.
-  The amount of data can be ~60 MB per station for full historic extent
   and will obviously increase by time.
-  If weather data is queried and the query can't be fulfilled from the
   cache, data is loaded from the server - even if the data has been
   updated a second before. If the server doesn't have data for the
   requested time (e.g. since it's not yet available), this
   unnecessarily causes network traffic and wait time. Certainly space
   for improvement here.


********
Licenses
********

Code license
============
Licensed under the MIT license. See `LICENSE <https://github.com/panodata/dwdweather2/blob/master/LICENSE>`__ for details.

Data license
============
The DWD has information about their terms of use policy in
`German <https://www.dwd.de/DE/service/copyright/copyright_node.html>`__
and
`English <https://www.dwd.de/EN/service/copyright/copyright_node.html>`__.


*******************
Project information
*******************

Credits
=======
Thanks to `Marian Steinbach <https://github.com/marians>`__, all
other contributors and the `DWD <https://www.dwd.de/>`__.

Changelog
=========
See file `CHANGES.rst <https://github.com/panodata/dwdweather2/blob/master/CHANGES.rst>`__.


**************
Other projects
**************
- https://github.com/earthobservations/wetterdienst
- https://github.com/na-boa/brightsky
- https://github.com/stephan192/dwdwfsapi
- https://github.com/jlewis91/dwdbulk
- https://github.com/domschl/python-dwd-forecast
- https://github.com/FL550/simple_dwd_weatherforecast

- https://github.com/astoeckel/pydwdapi
- https://github.com/ckaus/pydwd

- https://github.com/hiveeyes/phenodata
- https://github.com/hiveeyes/apicast

- https://github.com/brry/rdwd

- https://github.com/scoute-dich/Weather
- https://github.com/buwx/meteogram
- https://github.com/clerie/wetter
- https://github.com/dj0001/DWD-Warnmodul-2
- https://github.com/FWidm/dwd-hourly-crawler

- https://github.com/ekeih/dwdpollen
- https://github.com/marcschumacher/dwd_pollen
- https://github.com/schmupu/ioBroker.pollenflug
- https://github.com/LukeSkywalker92/MMM-DWD-WarnWeather
- https://github.com/devduisburg/MMM-pollen
- https://github.com/Pix---/vis_widget_dwd_pollenflug

- https://github.com/codeformuenster/mosmix-api
- https://github.com/codeformuenster/mosmix-processor
- https://github.com/DouglasFletcher/germanwetter

- https://github.com/wradlib
- https://github.com/jkreklow/radproc

- https://github.com/beltoforion/Synthetischer-Wetterbericht
- https://beltoforion.de/de/wetterbericht/
