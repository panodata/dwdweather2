##################
dwdweather backlog
##################


======
Prio 1
======
- [o] Use ``appdirs`` in ``get_cache_path``
- [o] Cache does not honor category selection
- [o] Retrieve information for multiple stations
- [o] Make ready for Python3


======
Prio 2
======
- [o] Also add data from "now" subfolder
- [o] Configure cache TTL
- [x] Download data for single category only
- [o] Enrich/strip JSON output payload by geojson information from station and w/o *_quality_level fields
- [o] Even if downloading croaks, no fresh data is requested when running the acquisition again
- [o] Documentation

    - ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/subdaily/standard_format/qualitaetsbytes.pdf
    - ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/subdaily/standard_format/code_kl.pdf
    - ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/subdaily/standard_format/download_legende_klkxformat.pdf


======
Prio 3
======
- [o] Add phenology information::

    def date_median(series):
        """
        https://stackoverflow.com/questions/43889611/median-of-panda-datetime64-column/43890905#43890905
        """
        import math
        return pd.to_datetime(math.floor(series.astype('int64').median()))

    def date_quantile_50(series):
        """
        https://stackoverflow.com/questions/43889611/median-of-panda-datetime64-column/48709758#48709758
        """
        return series.astype('datetime64[ns]').quantile(.5)

    import pandas as pd
    dates = [
        pd.Timestamp('2012-05-01T00:00'),
        pd.Timestamp('2014-06-01T00:00'),
        pd.Timestamp('2016-05-14T00:00'),
        pd.Timestamp('2018-07-14T00:00'),
    ]
    frame = pd.DataFrame(dates)

    print date_median(frame[0])
    print date_quantile_50(frame[0])


====
Done
====
- [x] Stations list does not honor resolution yet
- [x] Selecting specific categories does not work yet
- [x] Switch from FTP to new HTTP endpoint https://opendata.dwd.de/climate_environment/CDC/
