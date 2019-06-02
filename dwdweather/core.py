# -*- coding: utf-8 -*-
# (c) 2014 Marian Steinbach, MIT licensed
# (c) 2018-2019 Andreas Motl, MIT licensed
import os
import re
import sys
import csv
import json
import math
import logging
import sqlite3
import StringIO
import traceback
from tqdm import tqdm
from copy import deepcopy
from ftplib import FTP, Error as FTPError
from zipfile import ZipFile
from datetime import datetime
from dwdweather.knowledge import DwdCdcKnowledge
"""
Python client to access weather data from Deutscher Wetterdienst (DWD),
the federal meteorological service in Germany.

See Github repository for latest version:

    https://github.com/hiveeyes/dwdweather2

Licensed under the MIT license. See file ``LICENSE`` for details.
"""


log = logging.getLogger(__name__)


class DwdWeather(object):

    # DWD FTP server host name
    server = "ftp-cdc.dwd.de"

    # FTP server path for our files
    climate_observations_path = "/pub/CDC/observations_germany/climate/{resolution}"

    def __init__(self, resolution=None, **kwargs):
        """
        Use all keyword arguments as configuration
        - user
        - passwd
        - cachepath
        """

        # =================
        # Configure context
        # =================

        # Temporal resolution
        self.resolution = resolution

        # Categories of measurements on the server
        self.categories = DwdCdcKnowledge.climate.measurements

        # ========================
        # Configure cache database
        # ========================

        # Database field definition
        knowledge = DwdCdcKnowledge.climate.get_resolution_by_name(self.resolution)
        self.fields = DwdCdcKnowledge.as_dict(knowledge)

        # Sanity checks
        if not self.fields:
            log.error('No schema information for resolution "%s" found, please check your knowledge base.' % self.resolution)
            sys.exit(1)

        # Storage location
        cp = None
        if "cachepath" in kwargs:
            cp = kwargs["cachepath"]
        self.cachepath = self.get_cache_path(cp)

        # Reset cache if requested
        if "reset_cache" in kwargs and kwargs["reset_cache"]:
            self.reset_cache()

        # Initialize
        self.init_cache()

        # =========================
        # Configure FTP data source
        # =========================

        # Path to folder on CDC FTP server
        self.serverpath = self.climate_observations_path.format(resolution=self.resolution)
        log.info('Acquiring dataset for resolution "{}" from server "{}" at path "{}"'.format(
            self.resolution, self.server, self.serverpath))

        # Credentials for CDC FTP server
        self.user = "anonymous"
        self.passwd = "guest@example.com"

        if "debug" in kwargs:
            self.debug = int(kwargs["debug"])
        else:
            self.debug = 0

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
        Creates ``.dwd-weather`` directory in the current
        user's home, where a cache database and config
        file will reside.
        """
        database_file = self.get_cache_database()
        self.db = sqlite3.connect(database_file)
        self.db.row_factory = self.dict_factory
        c = self.db.cursor()

        tablename = self.get_measurement_table()

        # Create measurement tables and index.
        create_fields = []
        for category in sorted(self.fields.keys()):
            for fieldname, fieldtype in self.fields[category]:
                create_fields.append("%s %s" % (fieldname, fieldtype))
        create = 'CREATE TABLE IF NOT EXISTS {table} (station_id int, datetime int, {sql_fields})'.format(table=tablename, sql_fields=",\n".join(create_fields))
        index = 'CREATE UNIQUE INDEX IF NOT EXISTS {table}_uniqueidx ON {table} (station_id, datetime)'.format(table=tablename)
        c.execute(create)
        c.execute(index)

        # Create station tables and index.
        tablename = self.get_stations_table()
        create = """
            CREATE TABLE IF NOT EXISTS {table}
            (
                station_id int,
                date_start int,
                date_end int,
                geo_lon real,
                geo_lat real,
                height int,
                name text,
                state text
            )""".format(table=tablename)
        index = 'CREATE UNIQUE INDEX IF NOT EXISTS {table}_uniqueidx ON {table} (station_id, date_start)'.format(table=tablename)
        c.execute(create)
        c.execute(index)

        self.db.commit()

    def import_stations(self):
        """
        Load station meta data from DWD server.
        """
        log.info("Importing stations data from FTP server")
        ftp = FTP(self.server)
        ftp.login(self.user, self.passwd)
        for category in self.categories:
            cat = category['name']
            if cat == "solar":
                # workaround - solar has no subdirs
                path = "%s/%s" % (self.serverpath, cat)
            else:
                path = "%s/%s/recent" % (self.serverpath, cat)

            try:
                ftp.cwd(path)
            except FTPError as ex:
                log.warning('Resolution "{}" has no category "{}"'.format(self.resolution, cat))
                continue

            # get directory contents
            serverfiles = []
            ftp.retrlines('NLST', serverfiles.append)
            for filename in serverfiles:
                if "Beschreibung_Stationen" not in filename:
                    continue
                log.info("Reading file %s/%s" % (path, filename))
                f = StringIO.StringIO()
                ftp.retrbinary('RETR ' + filename, f.write)
                self.import_station(f.getvalue())
                f.close()

    def import_station(self, content):
        """
        Takes the content of one station metadata file
        and imports it into the database.
        """
        content = content.strip()
        content = content.replace("\r", "")
        content = content.replace("\n\n", "\n")
        content = content.decode("latin1")

        table = self.get_stations_table()

        insert_sql = """INSERT OR IGNORE INTO {table}
            (station_id, date_start, date_end, geo_lon, geo_lat, height, name, state)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""".format(table=table)
        update_sql = """UPDATE {table}
            SET date_end=?, geo_lon=?, geo_lat=?, height=?, name=?, state=?
            WHERE station_id=? AND date_start=?""".format(table=table)
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

        # Compute timerange labels / subfolder names.
        timeranges = []
        if latest:
            timeranges.append("recent")
        if historic:
            timeranges.append("historical")

        # Restrict import to specified categories
        categories_selected = deepcopy(self.categories)
        if categories:
            categories_selected = filter(lambda category: category['name'] in categories, categories_selected)

        # Connect to FTP server.
        ftp = FTP(self.server)
        ftp.login(self.user, self.passwd)

        # Reporting.
        station_info = self.station_info(station_id)
        log.info("Downloading measurements for station %d" % station_id)
        log.info("Station information: %s" % json.dumps(station_info, indent=2, sort_keys=True))

        # Download data.
        importfiles = []
        for category in categories_selected:
            key = category['key']
            name = category['name'].replace('_', ' ')
            log.info('Downloading "{}" data ({})'.format(name, key))
            importfiles += self.download_measures(ftp, station_id, category['name'], timeranges)

        # Import data for all categories.
        log.info("Importing measurements for station %d" % station_id)
        if not importfiles:
            log.warning("No files to import for station %s" % station_id)
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
            log.info("Reading from FTP: %s/%s" % (path, filename))
            ftp.retrbinary('RETR ' + filename, open(output_path, 'wb').write)
            with ZipFile(output_path) as myzip:
                for f in myzip.infolist():

                    # This is the data file
                    if f.filename.startswith('produkt_'):
                        log.info("Reading from Zip: %s" % (f.filename))
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
                log.warning('Station "{}" has no data for category "{}"'.format(station_id, cat))
            else:
                download(path, filename, cat)
        else:
            for timerange in timeranges:
                timerange_suffix = "akt"
                if timerange == "historical":
                    timerange_suffix = "hist"
                path = "%s/%s/%s" % (self.serverpath, cat, timerange)

                try:
                    ftp.cwd(path)
                except FTPError as ex:
                    log.warning('Station "{}" has no data for category "{}"'.format(station_id, cat))
                    continue

                # list dir content, get right file name
                serverfiles = []
                ftp.retrlines('NLST', serverfiles.append)
                filename = None
                for fn in serverfiles:
                    if ("_%05d_" % station_id) in fn:
                        filename = fn
                        break
                if filename is None:
                    log.warning('Station "{}" has no data for category "{}"'.format(station_id, cat))
                else:
                    download(path, filename, cat, timerange)

        return importfiles

    def import_measures_textfile(self, category, path):
        """
        Import content of source text file into database.
        """

        category_name = category.replace('_', ' ')
        if category not in self.fields:
            log.warning('Importing "{}" data from "{}" not implemented yet'.format(category_name, path))
            return

        log.info('Importing "{}" data from file "{}"'.format(category_name, path))

        f = open(path, "rb")
        content = f.read()
        f.close()
        content = content.strip()

        # Create SQL template
        tablename = self.get_measurement_table()
        fieldnames = []
        value_placeholders = []
        sets = []

        fields = list(self.fields[category])
        for fieldname, fieldtype in fields:
            sets.append(fieldname + "=?")

        fields.append(('station_id', 'str'))
        fields.append(('datetime', 'datetime'))

        for fieldname, fieldtype in fields:
            fieldnames.append(fieldname)
            value_placeholders.append('?')

        # Build UPSERT SQL statement.
        # https://www.sqlite.org/lang_UPSERT.html
        sql_template = "INSERT INTO {table} ({fields}) VALUES ({value_placeholders}) " \
                       "ON CONFLICT (station_id, datetime) DO UPDATE SET {sets} WHERE station_id=? AND datetime=?".format(
                        table=tablename, fields=', '.join(fieldnames),
                        value_placeholders=', '.join(value_placeholders), sets=', '.join(sets))

        # Create data rows.
        c = self.db.cursor()
        count = 0
        items = content.split("\n")
        for line in tqdm(items, ncols=79):
            count += 1
            line = line.strip()
            if line == "" or line == '\x1a':
                continue
            line = line.replace(";eor", "")
            parts = line.split(";")
            for n in range(len(parts)):
                parts[n] = parts[n].strip()

            if count > 1:

                # Parse station id.
                parts[0] = int(parts[0])

                # Parse timestamp, ignore minutes.
                # Fixme: Is this also true for resolution=10_minutes?
                parts[1] = int(parts[1].replace('T', '').replace(':', ''))

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
                        parts[n] = int(parts[n].replace('T', '').replace(':', ''))

                    dataset.append(parts[n])

                # station_id and datetime for WHERE clause
                dataset.append(parts[0])
                dataset.append(parts[1])

                #log.debug('SQL template: %s', sql_template)
                #log.debug('Dataset: %s', dataset)

                c.execute(sql_template, dataset + dataset)

        self.db.commit()

    def get_data_age(self):
        """
        Return age of latest dataset as ``datetime.timedelta``.
        """
        sql = "SELECT MAX(datetime) AS maxdatetime FROM %s" % self.get_measurement_table()
        c = self.db.cursor()
        c.execute(sql)
        item = c.fetchone()
        if item["maxdatetime"] is not None:
            latest = datetime.strptime(str(item["maxdatetime"]), "%Y%m%d%H")
            return datetime.utcnow() - latest

    def get_stations_table(self):
        return 'stations_%s' % self.resolution

    def get_measurement_table(self):
        return 'measures_%s' % self.resolution

    def get_timestamp_format(self):
        knowledge = DwdCdcKnowledge.climate.get_resolution_by_name(self.resolution)
        return knowledge.__timestamp_format__

    def query(self, station_id, timestamp, categories=None, recursion=0):
        """
        Get values from cache.
        station_id: Numeric station ID
        timestamp: datetime object
        """
        if recursion < 2:
            sql = "SELECT * FROM %s WHERE station_id=? AND datetime=?" % self.get_measurement_table()
            c = self.db.cursor()
            c.execute(sql, (station_id, timestamp.strftime(self.get_timestamp_format())))
            out = c.fetchone()
            if out is None:
                # cache miss
                age = (datetime.utcnow() - timestamp).total_seconds() / 86400
                if age < 360:
                    self.import_measures(station_id, categories=categories, latest=True)
                elif age >= 360 and age <= 370:
                    self.import_measures(station_id, categories=categories, latest=True, historic=True)
                else:
                    self.import_measures(station_id, categories=categories, historic=True)
                return self.query(station_id, timestamp, categories=categories, recursion=(recursion + 1))
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
        Return list of dicts with all stations.
        """
        out = []
        table = self.get_stations_table()
        sql = """
            SELECT s2.*
            FROM {table} s1
            LEFT JOIN {table} s2 ON (s1.station_id=s2.station_id AND s1.date_end=s1.date_end)
            GROUP BY s1.station_id""".format(table=table)
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
        table = self.get_stations_table()
        sql = "SELECT * FROM {table} WHERE station_id=?".format(table=table)
        c = self.db.cursor()
        c.execute(sql, (station_id,))
        return c.fetchone()

    def nearest_station(self, lon, lat):
        # Select most current stations datasets.
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
        Return stations list as CSV.
        """
        csvfile = StringIO.StringIO()
        # assemble field list
        headers = ["station_id", "date_start", "date_end", "geo_lon", "geo_lat", "height", "name"]
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
