####################
dwdweather changelog
####################

in progress
===========
- This and that: Fix console script entrypoint. Improve imports, debugging and inline comments.
- Adapt to changes on upstream server ftp-cdc.dwd.de
- Add ``--reset-cache`` option for dropping the cache database before performing any work
- Add ``--categories`` option for specifying list of comma-separated category names to import
- Add acquisition categories "pressure", "cloudiness" and "visibility"
- Add acquisition resolution "10_minutes"
- Improve naming of some fields for the "hourly" resolution
- Add real logging instead of verbosity printing
- Add more measurement categories
- Improve FTP error handling
- Improve SQL processing by using a single UPSERT statement instead of
  running two consecutive INSERT / UPDATE statements
- Improve ISO date parsing by switching to "dateutil"
- Modularisation and refactoring
- Make station list honor selected resolution
- Fix parsing list of categories
- Improve dataset/resolution handling
- Switch from FTP to new HTTP endpoint https://opendata.dwd.de/climate_environment/CDC/

2014-07-30 0.7.0
================
- Adapted to match modified schema for sun data

2014-07-25 0.6.0
================
- Adapted to match modified schema for wind and air temperature data

2014-07-23 0.5.0
================
- Fix a problem where verbosity was not set

2014-07-18 0.4.0
================
- Use different DWD FTP server, no longer requires FTP user authentication
- Provide access to more data ("solar")
- Reading of station data much faster due to use of specific files from DWD
- Additional fixes

2014-07-18 0.3.0
================
- Add command line client functions

2014-07-17 0.2.0
================
- First working version
- Publish to PyPI

2014-07-16 0.1.0
================
- Initial commit
