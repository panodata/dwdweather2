# -*- coding: utf-8 -*-
# (c) 2014 Marian Steinbach, MIT licensed
import os
import re
import sys
import csv
import json
import math
import sqlite3
import argparse
import StringIO
import traceback
from copy import deepcopy
from ftplib import FTP
from zipfile import ZipFile
from datetime import datetime


"""
Reads weather data from DWD Germany.

See Github repository for latest version:

    https://github.com/marians/dwd-weather

Code published unter the terms of the MIT license.
See here for details.

    https://github.com/marians/dwd-weather/blob/master/LICENSE

"""

class DwdCdcKnowledge(object):
    """
    Knowledge about the data layout on the Climate Data Centers (CDC) FTP server provided by the DWD.
    """

    class climate:

        # The different measurements for climate data
        measurements = [
            {'key': 'TU', 'name': 'air_temperature'},
            {'key': 'EB', 'name': 'soil_temperature'},
            {'key': 'SD', 'name': 'sun'},
            {'key': 'ST', 'name': 'solar'},
            {'key': 'RR', 'name': 'precipitation'},
            {'key': 'FF', 'name': 'wind'},
        ]

        # The different resolutions for climate data
        class resolutions:

            class minutes_10:
                path = '10_minutes'


            # Temporal resolution: hourly
            class hourly:

                """
                ===============
                Air temperature
                ===============

                Documentation
                -------------

                - Recent

                    - Temporal coverage:    rolling: 500 days before yesterday - until yesterday
                    - Temporal resolution:  hourly
                    - ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/hourly/air_temperature/recent/DESCRIPTION_obsgermany_climate_hourly_tu_recent_en.pdf

                - Historical

                    - Temporal coverage:    01.01.1893 - 31.12.2016
                    - Temporal resolution:  hourly
                    - ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/hourly/air_temperature/historical/DESCRIPTION_obsgermany_climate_hourly_tu_historical_en.pdf

                Fields
                ------
                ::

                    Field               Description                     Format or unit
                    STATIONS_ID         Station identification number   Integer
                    MESS_DATUM          Measurement time                YYYYMMDDHH
                    QN_9                Quality level                   Integer: 1-10 and -999, for coding see paragraph "Quality information" in PDF.
                    TT_TU               Air temperature 2m              °C
                    RF_TU               Relative humidity 2m            %
                    eor                 End of record, can be ignored

                Missing values are marked as -999. All dates given are in UTC.
                """
                air_temperature = (
                    ("temphum_quality_level", "int"),   # Quality level
                    ("temphum_temperature", "real"),    # Air temperature 2m
                    ("temphum_humidity", "real"),       # Relative humidity 2m
                )


                """
                ================
                Soil temperature
                ================

                Documentation
                -------------

                - Recent

                    - Temporal coverage:    rolling: 500 days before yesterday - until yesterday
                    - Temporal resolution:  several times a day
                    - ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/hourly/soil_temperature/recent/DESCRIPTION_obsgermany_climate_hourly_soil_temperature_recent_en.pdf

                - Historical

                    - Temporal coverage:    01.01.1949 - 31.12.2016
                    - Temporal resolution:  several times a day
                    - ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/hourly/soil_temperature/historical/DESCRIPTION_obsgermany_climate_hourly_soil_temperature_historical_en.pdf

                Fields
                ------
                ::

                    Field               Description                     Format or unit
                    STATIONS_ID         Station identification number   Integer
                    MESS_DATUM          Measurement time                YYYYMMDDHH
                    QN_2                Quality level                   Integer: 1-10 and -999, for coding see paragraph "Quality information" in PDF.
                    V_TE002             Soil temperature in 2 cm depth  °C
                    V_TE005             Soil temperature in 5 cm depth  °C
                    V_TE010             Soil temperature in 10 cm depth °C
                    V_TE020             Soil temperature in 20 cm depth °C
                    V_TE050             Soil temperature in 50 cm depth °C
                    V_TE100             Soil temperature in 100 cm depth °C
                    eor                 End of record, can be ignored

                Missing values are marked as -999. All dates given are in UTC.
                """
                soil_temperature = (
                    ("soiltemp_quality_level", "int"),      # Quality level
                    ("soiltemp_temperature_002", "real"),   # Soil temperature 2cm
                    ("soiltemp_temperature_005", "real"),   # Soil temperature 5cm
                    ("soiltemp_temperature_010", "real"),   # Soil temperature 10cm
                    ("soiltemp_temperature_020", "real"),   # Soil temperature 20cm
                    ("soiltemp_temperature_050", "real"),   # Soil temperature 50cm
                    ("soiltemp_temperature_100", "real"),   # Soil temperature 100cm
                )


                """
                =============
                Precipitation
                =============

                Documentation
                -------------

                - Recent

                    - Temporal coverage:    rolling: 500 days before yesterday - until yesterday
                    - Temporal resolution:  hourly
                    - ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/hourly/precipitation/recent/DESCRIPTION_obsgermany_climate_hourly_precipitation_recent_en.pdf

                - Historical

                    - Temporal coverage:    01.09.1995 - 31.12.2016
                    - Temporal resolution:  hourly
                    - ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/hourly/precipitation/historical/DESCRIPTION_obsgermany_climate_hourly_precipitation_historical_en.pdf

                Fields
                ------
                ::

                    Field               Description                     Format or unit
                    STATIONS_ID         Station identification number   Integer
                    MESS_DATUM          Measurement time                YYYYMMDDHH
                    QN_8                Quality level                   Integer: 1-10 and -999, for coding see paragraph "Quality information" in PDF.
                    R1                  Hourly precipitation height     mm
                    RS_IND              Precipitation indicator         0 no precipitation
                                                                        1 precipitation has fallen
                    WRTR                Form of precipitation           WR-code
                    eor                 End of record, can be ignored

                Missing values are marked as -999. All dates given are in UTC.

                The WRTR form of precipitation is only given at certain times, in accordance with SYNOP definition.
                Refer to daily values for more information on precipitation type. The classification of precipitation type in the
                daily values differs from the classification for the hourly values.

                For the hourly values, the W_R definition (see Table 55, VUB 2 Band D, 2013) is used::

                    0   No fallen precipitation or too little deposition
                        (e.g., dew or frost) to form a precipitation height larger than 0.0
                    1   Precipitation height only due to deposition
                        (dew or frost) or if it cannot decided how large the part from deposition is
                    2   Precipitation height only due to liquid deposition
                    3   Precipitation height only due to solid precipitation
                    6   Precipitation height due to fallen liquid precipitation, may also include deposition of any kind
                    7   Precipitation height due to fallen solid precipitation, may also include deposition of any kind
                    8   Fallen precipitation in liquid and solid form
                    9   No precipitation measurement, form of precipitation cannot be determined.
                """
                precipitation = (
                    ("precipitation_quality_level", "int"),      # Quality level
                    ("precipitation_height", "real"),
                    ("precipitation_fallen", "bool"),
                    ("precipitation_form", "int"),
                )


                """
                ===
                Sun
                ===

                Documentation
                -------------

                - Recent

                    - Temporal coverage:    rolling: 500 days before yesterday - until yesterday
                    - Temporal resolution:  hourly
                    - ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/hourly/sun/recent/DESCRIPTION_obsgermany_climate_hourly_sun_recent_en.pdf

                - Historical

                    - Temporal coverage:    01.01.1893 - 31.12.2016
                    - Temporal resolution:  hourly
                    - ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/hourly/sun/historical/DESCRIPTION_obsgermany_climate_hourly_sun_historical_en.pdf

                Fields
                ------
                ::

                    Field               Description                     Format or unit
                    STATIONS_ID         Station identification number   Integer
                    MESS_DATUM          Measurement time                YYYYMMDDHH
                    QN_7                Quality level                   Integer: 1-10 and -999, for coding see paragraph "Quality information" in PDF.
                    SD_SO               Hourly sunshine duration        min
                    eor                 End of record, can be ignored

                Missing values are marked as -999. All dates given are in UTC.
                """
                sun = (
                    ("sun_quality_level", "int"),   # Quality level
                    ("sun_duration", "real"),       # Hourly sunshine duration
                )



                """
                ====
                Wind
                ====

                Documentation
                -------------

                - Recent

                    - Temporal coverage:    rolling: 500 days before yesterday - until yesterday
                    - Temporal resolution:  hourly
                    - ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/hourly/wind/recent/DESCRIPTION_obsgermany_climate_hourly_wind_recent_en.pdf

                - Historical

                    - Temporal coverage:    01.01.1893 - 31.12.2016
                    - Temporal resolution:  hourly
                    - ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/hourly/wind/historical/DESCRIPTION_obsgermany_climate_hourly_wind_historical_en.pdf

                Fields
                ------
                ::

                    Field               Description                     Format or unit
                    STATIONS_ID         Station identification number   Integer
                    MESS_DATUM          Measurement time                YYYYMMDDHH
                    QN_3                Quality level                   Integer: 1-10 and -999, for coding see paragraph "Quality information" in PDF.
                    F                   Mean wind speed                 m/s
                    D                   Mean wind direction             degrees
                    eor                 End of record, can be ignored

                Missing values are marked as -999. All dates given are in UTC.

                Nowadays, hourly wind speed and wind direction is given as the average of
                the six 10min intervals measured in the previous hour
                (e.g., at UTC 11, the average windspeed and average wind direction during UTC10-UTC11 is given).

                """
                wind = (
                    ("wind_quality_level", "int"),   # Quality level
                    ("wind_speed", "real"),         # Mean wind speed
                    ("wind_direction", "int"),      # Mean wind direction
                )


                """
                =====
                Solar
                =====

                Documentation
                -------------

                - Temporal coverage:    01.01.1937 - month before last month
                - Temporal resolution:  hourly
                - ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/hourly/solar/DESCRIPTION_obsgermany_climate_hourly_solar_en.pdf

                Fields
                ------
                ::

                    Field               Description                     Format or unit
                    STATIONS_ID         Station identification number   Integer
                    MESS_DATUM          Measurement time                YYYYMMDDHH
                    QN_592              Quality level                   Integer: 1-10 and -999, for coding see paragraph "Quality information" in PDF.
                    ATMO_LBERG          Hourly sum of longwave          J/cm^2
                                        downward radiation
                    FD_LBERG            Hourly sum of diffuse           J/cm^2
                                        solar radiation
                    FG_LBERG            Hourly sum of solar             J/cm^2
                                        incoming radiation
                    SD_LBERG            Hourly sum of                   min
                                        sunshine duration
                    ZENIT               Solar zenith angle at mid       degree
                                        of interval
                    MESS_DATUM_WOZ      End of interval in local        YYYYMMDDHH:mm
                                        true solar time
                    eor                 End of record, can be ignored

                Missing values are marked as -999. All dates given are in UTC.

                """
                solar = (
                    ("solar_quality_level", "int"),     # Qualitaets_Niveau
                    ("solar_duration", "int"),          # Hourly sum of longwave downward radiation
                    ("solar_sky", "real"),              # Hourly sum of diffuse solar radiation
                    ("solar_global", "real"),           # Hourly sum of solar incoming radiation
                    ("solar_atmosphere", "real"),       # Hourly sum of sunshine duration
                    ("solar_zenith", "real"),           # Solar zenith angle at mid of interval
                    ("solar_end_of_interval", "datetime"),  # End of interval in local true solar time
                )


            """
            Quality information
            The quality level "Qualitätsniveau" (QN) given here applies
            to the respective columns and describes the method of quality control.

            Quality level (column header: QN_X)::

                 1 only formal control
                 2 controlled with individually defined criteria
                 3 automatic control and correction
                 5 historic, subjective procedures
                 7 second control done, before correction
                 8 quality control outside ROUTINE
                 9 not all parameters corrected
                10 quality control finished, all corrections finished

            Erroneous or suspicious values are identified and set to -999.
            """


class DwdWeather(object):

    # DWD FTP server host name
    server = "ftp-cdc.dwd.de"

    # FTP server path for our files
    serverpath = "/pub/CDC/observations_germany/climate/hourly"

    # Database field definition:
    # key = internal field name
    # value = (sqlite type, value category, source column name)
    knowledge = DwdCdcKnowledge.climate.resolutions.hourly
    fields = {}
    for entry in dir(knowledge):
        if entry.startswith('__'): continue
        fields[entry] = getattr(knowledge, entry)

    # Categories of measurements on the server
    categories = DwdCdcKnowledge.climate.measurements

    def __init__(self, **kwargs):
        """
        Use all keyword arguments as configuration
        - user
        - passwd
        - cachepath
        """

        # Configure cache storage
        cp = None
        if "cachepath" in kwargs:
            cp = kwargs["cachepath"]
        self.cachepath = self.get_cache_path(cp)

        # Reset cache if requested
        if "reset_cache" in kwargs and kwargs["reset_cache"]:
            self.reset_cache()

        # Initialize cache
        self.init_cache()


        # Credentials for CDC FTP server
        self.user = "anonymous"
        self.passwd = "guest@example.com"

        self.verbosity = 0
        if "verbosity" in kwargs:
            self.verbosity = kwargs["verbosity"]


    def dict_factory(self, cursor, row):
        """
        For emission of dicts from sqlite3
        """
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d


    def get_cache_path(self, path):
        if path is None:
            home = os.path.expanduser("~") + os.sep + ".dwd-weather"
        else:
            home = path
        if not os.path.exists(home):
            os.mkdir(home)
        return home

    def get_cache_database(self):
        database_file = os.path.join(self.cachepath, "dwd-weather.db")
        return database_file

    def reset_cache(self):
        database_file = self.get_cache_database()
        os.remove(database_file)

    def init_cache(self):
        """
        Creates .dwd-weather directory in the current
        user's home, where a cache database and config
        file will reside
        """
        database_file = self.get_cache_database()
        self.db = sqlite3.connect(database_file)
        self.db.row_factory = self.dict_factory
        c = self.db.cursor()
        # Create measures table and index
        create = """CREATE TABLE IF NOT EXISTS measures
            (
                station_id int,
                datetime int, """
        create_fields = []
        for category in sorted(self.fields.keys()):
            for fieldname, fieldtype in self.fields[category]:
                create_fields.append("%s %s" % (fieldname, fieldtype))
        create += ",\n".join(create_fields)
        create += ")"
        c.execute(create)
        index = """CREATE UNIQUE INDEX IF NOT EXISTS unq
            ON measures (station_id, datetime)"""
        c.execute(index)
        # create stations table and index
        create = """CREATE TABLE IF NOT EXISTS stations
            (
                station_id int,
                date_start int,
                date_end int,
                geo_lon real,
                geo_lat real,
                height int,
                name text,
                state text
            )"""
        index = """CREATE UNIQUE INDEX IF NOT EXISTS station_unique
            ON stations (station_id, date_start)"""
        c.execute(create)
        c.execute(index)
        self.db.commit()


    def import_stations(self):
        """
        Load station meta data from DWD server.
        """
        if self.verbosity > 0:
            print("Importing stations data from FTP server")
        ftp = FTP(self.server)
        ftp.login(self.user, self.passwd)
        for category in self.categories:
            cat = category['name']
            if cat == "solar":
                # workaround - solar has no subdirs
                path = "%s/%s" % (self.serverpath, cat)
            else:
                path = "%s/%s/recent" % (self.serverpath, cat)
            ftp.cwd(path)
            # get directory contents
            serverfiles = []
            ftp.retrlines('NLST', serverfiles.append)
            for filename in serverfiles:
                if "Beschreibung_Stationen" not in filename:
                    continue
                if self.verbosity > 1:
                    print("Reading file %s/%s" % (path, filename))
                f = StringIO.StringIO()
                ftp.retrbinary('RETR ' + filename, f.write)
                self.import_station(f.getvalue())
                f.close()


    def import_station(self, content):
        """
        Takes the content of one station metadata file
        and imports it into the database
        """
        content = content.strip()
        content = content.replace("\r", "")
        content = content.replace("\n\n", "\n")
        content = content.decode("latin1")
        insert_sql = """INSERT OR IGNORE INTO stations
            (station_id, date_start, date_end, geo_lon, geo_lat, height, name, state)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""
        update_sql = """UPDATE stations
            SET date_end=?, geo_lon=?, geo_lat=?, height=?, name=?, state=?
            WHERE station_id=? AND date_start=?"""
        cursor = self.db.cursor()
        #print content
        linecount = 0
        for line in content.split("\n"):
            linecount += 1
            line = line.strip()
            if line == "" or line == u'\x1a':
                continue
            #print linecount, line
            if linecount > 2:
                # frist 7 fields
                parts = re.split(r"\s+", line, 6)
                # seperate name from Bundesland
                (name, bundesland) = parts[6].rsplit(" ", 1)
                name = name.strip()
                del parts[6]
                parts.append(name)
                parts.append(bundesland)
                #print parts
                for n in range(len(parts)):
                    parts[n] = parts[n].strip()
                station_id = int(parts[0])
                station_height = int(parts[3])
                station_lat = float(parts[4])
                station_lon = float(parts[5])
                station_start = int(parts[1])
                station_end = int(parts[2])
                station_name = parts[6]
                station_state = parts[7]
                # issue sql
                cursor.execute(insert_sql, (
                    station_id,
                    station_start,
                    station_end,
                    station_lon,
                    station_lat,
                    station_height,
                    station_name,
                    station_state))
                cursor.execute(update_sql, (
                    station_end,
                    station_lon,
                    station_lat,
                    station_height,
                    station_name,
                    station_state,
                    station_id,
                    station_start))
        self.db.commit()



    def import_measures(self, station_id, categories=None, latest=True, historic=False):
        """
        Load data from DWD server.
        Parameter:

        station_id: e.g. 2667 (Köln-Bonn airport)

        latest: Load most recent data (True, False)
        historic: Load older values

        We download ZIP files for several categories
        of measures. We then extract one file from
        each ZIP. This path is then handed to the
        CSV -> Sqlite import function.
        """

        # Compute timerange labels / subfolder names
        timeranges = []
        if latest:
            timeranges.append("recent")
        if historic:
            timeranges.append("historical")

        # Restrict import to specified categories
        categories_selected = deepcopy(self.categories)
        if categories:
            categories_selected = filter(lambda category: category['name'] in categories, categories_selected)

        # Connect to FTP server
        ftp = FTP(self.server)
        ftp.login(self.user, self.passwd)


        # Download data
        if self.verbosity > 0:
            station_info = self.station_info(station_id)
            print
            print("=" * 42)
            print("Downloading measurements for station %d" % station_id)
            print("=" * 42)
            if station_info:
                print(json.dumps(station_info, indent=2, sort_keys=True))
                print("=" * 42)

        importfiles = []
        for category in categories_selected:
            cat = category['name']
            if self.verbosity > 1:
                print
                print('-' * 42)
                print("Downloading %s data" % cat.replace('_', ' '))
                print('-' * 42)

            importfiles += self.download_measures(ftp, station_id, cat, timeranges)


        # Import data
        if self.verbosity > 1:
            print
            print("=" * 42)
            print("Importing measurements for station %d" % station_id)
            print("=" * 42)
            if not importfiles:
                print("WARNING: No files to import for station %s" % station_id)

        for item in importfiles:
            self.import_measures_textfile(item[0], item[1])
            os.remove(item[1])

    def download_measures(self, ftp, station_id, cat, timeranges):

        importfiles = []

        def download(path, filename, cat, timerange=None):
            output_path = self.cachepath + os.sep + filename
            if timerange is None:
                timerange = "-"
            data_filename = "data_%s_%s_%s.txt" % (station_id, timerange, cat)
            if self.verbosity > 1:
                print("Reading from FTP: %s/%s" % (path, filename))
            ftp.retrbinary('RETR ' + filename, open(output_path, 'wb').write)
            with ZipFile(output_path) as myzip:
                for f in myzip.infolist():

                    # This is the data file
                    if f.filename.startswith('produkt_'):
                        if self.verbosity > 1:
                            print("Reading from Zip: %s" % (f.filename))
                        myzip.extract(f, self.cachepath + os.sep)
                        os.rename(self.cachepath + os.sep + f.filename,
                            self.cachepath + os.sep + data_filename)
                        importfiles.append([cat, self.cachepath + os.sep + data_filename])
            os.remove(output_path)

        if cat == "solar":
            path = "%s/%s" % (self.serverpath, cat)
            ftp.cwd(path)
            # list dir content, get right file name
            serverfiles = []
            ftp.retrlines('NLST', serverfiles.append)
            filename = None
            for fn in serverfiles:
                if ("_%05d_" % station_id) in fn:
                    filename = fn
                    break
            if filename is None:
                if self.verbosity > 1:
                    print("WARNING: Station %s has no data for category '%s'" % (station_id, cat))
            else:
                download(path, filename, cat)
        else:
            for timerange in timeranges:
                timerange_suffix = "akt"
                if timerange == "historical":
                    timerange_suffix = "hist"
                path = "%s/%s/%s" % (self.serverpath, cat, timerange)
                ftp.cwd(path)
                # list dir content, get right file name
                serverfiles = []
                ftp.retrlines('NLST', serverfiles.append)
                filename = None
                for fn in serverfiles:
                    if ("_%05d_" % station_id) in fn:
                        filename = fn
                        break
                if filename is None:
                    if self.verbosity > 1:
                        print("WARNING: Station %s has no data for category '%s'" % (station_id, cat))
                else:
                    download(path, filename, cat, timerange)

        return importfiles


    def import_measures_textfile(self, category, path):
        """
        Import content of source text file into database
        """

        if self.verbosity > 1:
            print("Importing %s data from file %s" % (category, path))

        f = open(path, "rb")
        content = f.read()
        f.close()
        content = content.strip()

        # Create SQL template
        sets = []
        for fieldname, fieldtype in self.fields[category]:
            sets.append(fieldname + "=?")
        insert_template = "INSERT OR IGNORE INTO measures (station_id, datetime) VALUES (?, ?)"
        update_template = "UPDATE measures SET %s WHERE station_id=? AND datetime=?" % ", ".join(sets)

        # Create data rows
        insert_datasets = []
        update_datasets = []
        count = 0
        for line in content.split("\n"):
            count += 1
            line = line.strip()
            if line == "" or line == '\x1a':
                continue
            line = line.replace(";eor", "")
            parts = line.split(";")
            for n in range(len(parts)):
                parts[n] = parts[n].strip()
            #print parts
            if count > 1:

                # Parse station id
                parts[0] = int(parts[0])

                # Parse timestamp, ignore minutes
                if ":" in parts[1]:
                    parts[1] = parts[1].split(":")[0]
                parts[1] = int(parts[1])

                insert_datasets.append([parts[0], parts[1]])
                dataset = []
                # station_id and datetime
                #if category == "soil_temp":
                #    print fields[category]
                #    print parts
                for n in range(2, len(parts)):
                    (fieldname, fieldtype) = self.fields[category][(n - 2)]
                    if parts[n] == "-999":
                        parts[n] = None
                    elif fieldtype == "real":
                        parts[n] = float(parts[n])
                    elif fieldtype == "int":
                        try:
                            parts[n] = int(parts[n])
                        except ValueError:
                            sys.stderr.write("Error in converting field '%s', value '%s' to int.\n" % (
                                fieldname, parts[n]))
                            (t, val, trace) = sys.exc_info()
                            traceback.print_tb(trace)
                            sys.exit()
                    elif fieldtype == "datetime":
                        if ":" in parts[n]:
                            parts[n] = parts[n].split(":")[0]

                    dataset.append(parts[n])

                # station_id and datetime for WHERE clause
                dataset.append(parts[0])
                dataset.append(parts[1])
                update_datasets.append(dataset)
        c = self.db.cursor()
        c.executemany(insert_template, insert_datasets)
        c.executemany(update_template, update_datasets)
        self.db.commit()


    def get_data_age(self):
        """
        Return age of latest dataset as datetime.timedelta
        """
        sql = "SELECT MAX(datetime) AS maxdatetime FROM measures"
        c = self.db.cursor()
        c.execute(sql)
        item = c.fetchone()
        if item["maxdatetime"] is not None:
            latest =  datetime.strptime(str(item["maxdatetime"]), "%Y%m%d%H")
            return datetime.utcnow() - latest


    def query(self, station_id, hour, categories=None, recursion=0):
        """
        Get values from cache.
        station_id: Numeric station ID
        hour: datetime object
        """
        if recursion < 2 :
            sql = "SELECT * FROM measures WHERE station_id=? AND datetime=?"
            c = self.db.cursor()
            c.execute(sql, (station_id, hour.strftime("%Y%m%d%H")))
            out = c.fetchone()
            if out is None:
                # cache miss
                age = (datetime.utcnow() - hour).total_seconds() / 86400
                if age < 360:
                    self.import_measures(station_id, categories=categories, latest=True)
                elif age >= 360 and age <= 370:
                    self.import_measures(station_id, categories=categories, latest=True, historic=True)
                else:
                    self.import_measures(station_id, categories=categories, historic=True)
                return self.query(station_id, hour, categories=categories, recursion=(recursion + 1))
            c.close()
            return out

    def haversine_distance(self, origin, destination):
        lon1, lat1 = origin
        lon2, lat2 = destination
        radius = 6371000 # meters

        dlat = math.radians(lat2-lat1)
        dlon = math.radians(lon2-lon1)
        a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
            * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        d = radius * c
        return d

    def stations(self, historic=False):
        """
        Return list of dicts with all stations
        """
        out = []
        sql = """SELECT s2.*
            FROM stations s1
            LEFT JOIN stations s2 ON (s1.station_id=s2.station_id AND s1.date_end=s1.date_end)
            GROUP BY s1.station_id"""
        c = self.db.cursor()
        for row in c.execute(sql):
            out.append(row)
        c.close()
        if len(out) == 0:
            # cache miss - have to import stations.
            self.import_stations()
            out = self.stations()
        return out

    def station_info(self, station_id):
        sql = "SELECT * FROM stations WHERE station_id=?"
        c = self.db.cursor()
        c.execute(sql, (station_id,))
        return c.fetchone()

    def nearest_station(self, lon, lat):
        # select most current stations datasets
        closest = None
        closest_distance = 99999999999
        for station in self.stations():
            d = self.haversine_distance((lon, lat),
                (station["geo_lon"], station["geo_lat"]))
            if d < closest_distance:
                closest = station
                closest_distance = d
        return closest

    def stations_geojson(self):
        out = {
            "type": "FeatureCollection",
            "features": []
        }
        for station in self.stations():
            out["features"].append({
                "type": "Feature",
                "properties": {
                    "id": station["station_id"],
                    "name": station["name"]
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [station["geo_lon"], station["geo_lat"]]
                }
            })
        return json.dumps(out)

    def stations_csv(self, delimiter=","):
        """
        Return stations list as CSV
        """
        csvfile = StringIO.StringIO()
        # assemble field list
        headers = ["station_id", "date_start", "date_end",
            "geo_lon", "geo_lat", "height", "name"]
        writer = csv.writer(csvfile, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(headers)
        stations = self.stations()
        for station in stations:
            row = []
            for n in range(len(headers)):
                val = station[headers[n]]
                if val is None:
                    val = ""
                elif type(val) == int:
                    val = str(val)
                elif type(val) == float:
                    val = "%.4f" % val
                elif type(val) == unicode:
                    val = val.encode("utf8")
                row.append(val)
            writer.writerow(row)
        contents = csvfile.getvalue()
        csvfile.close()
        return contents


def main():

    def get_station(args):
        dw = DwdWeather(cachepath=args.cachepath, reset_cache=args.reset_cache, verbosity=args.verbosity)
        print json.dumps(dw.nearest_station(lon=args.lon, lat=args.lat), indent=4)

    def get_stations(args):
        dw = DwdWeather(cachepath=args.cachepath, reset_cache=args.reset_cache, verbosity=args.verbosity)
        output = ""
        if args.type == "geojson":
            output = dw.stations_geojson()
        elif args.type == "csv":
            output = dw.stations_csv()
        elif args.type == "plain":
            output = dw.stations_csv(delimiter="\t")
        if args.output_path is None:
            print output
        else:
            f = open(args.output_path, "wb")
            f.write(output)
            f.close()

    def get_weather(args):

        # Workhorse
        dw = DwdWeather(cachepath=args.cachepath, reset_cache=args.reset_cache, verbosity=args.verbosity)

        # Sanitize some input values
        hour = datetime.strptime(str(args.hour), "%Y%m%d%H")
        categories = None
        if args.categories:
            categories = [cat.strip() for cat in args.categories.split(',')]

        # Query data
        print json.dumps(dw.query(args.station_id, hour, categories=categories), indent=4, sort_keys=True)

    argparser = argparse.ArgumentParser(prog="dwdweather",
        description="Get weather information for Germany.")
    argparser.add_argument("-v", dest="verbosity", action="count",
        help="Activate verbose output. Use -vv or -vvv to increase verbosity.",
        default=0)
    argparser.add_argument("-c", dest="cachepath",
        help="Path to cache directory. Defaults to .dwd-weather in user's home dir.",
        default=os.path.expanduser("~") + os.sep + ".dwd-weather")

    subparsers = argparser.add_subparsers(title="Actions", help="Main client actions.")

    def float_range(min, max):
        def check_range(x):
            x = float(x)
            if x < min or x > max:
                raise argparse.ArgumentTypeError("%r not in range [%r, %r]"%(x,min,max))
            return x
        return check_range

    # station options
    parser_station = subparsers.add_parser('station',
        help='Find a station')
    parser_station.set_defaults(func=get_station)
    parser_station.add_argument("lon", type=float_range(-180, 180),
        help="Geographic longitude (x) component as float, e.g. 7.2")
    parser_station.add_argument("lat", type=float_range(-90, 90),
        help="Geographic latitude (y) component as float, e.g. 53.9")

    # stations options
    parser_stations = subparsers.add_parser('stations',
        help='List or export stations')
    parser_stations.set_defaults(func=get_stations)
    parser_stations.add_argument("-t", "--type", dest="type",
        choices=["geojson", "csv", "plain"], default="plain",
        help="Export format")
    parser_stations.add_argument("-f", "--file", type=str, dest="output_path",
        help="Export file path. If not given, STDOUT is used.")

    # weather options
    parser_weather = subparsers.add_parser('weather', help='Get weather data for a station and hour')
    parser_weather.set_defaults(func=get_weather)
    parser_weather.add_argument("station_id", type=int, help="Numeric ID of the station, e.g. 2667")
    parser_weather.add_argument("hour", type=int, help="Time in the form of YYYYMMDDHH")

    # "--categories" option for restricting import to specified category names
    categories_available = ', '.join([item['name'] for item in DwdCdcKnowledge.climate.measurements])
    parser_weather.add_argument("--categories", type=str,
        help="List of comma-separated categories to import, choose from: %s.\n"
             "The default setting is to import all categories per station." % categories_available)

    # "--reset-cache" option for dropping the cache database before performing any work
    for parser in [parser_station, parser_stations, parser_weather]:
        parser.add_argument("--reset-cache", action='store_true', help="Drop the cache database")

    args = argparser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
