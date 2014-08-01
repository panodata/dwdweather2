# encoding: utf8

import sys
import os
import os.path
from ftplib import FTP
from zipfile import ZipFile
import sqlite3
from datetime import datetime
import math
import re
import StringIO


"""
Reads weather data from DWD Germany.

See Github repository for latest version:

    https://github.com/marians/dwd-weather

Code published unter the terms of the MIT license.
See here for details.

    https://github.com/marians/dwd-weather/blob/master/LICENSE

"""

class DwdWeather(object):

    # DWD FTP server host name
    server = "ftp-cdc.dwd.de"

    # FTP server path for our files
    serverpath = "/pub/CDC/observations_germany/climate/hourly"

    # database Field definition:
    # key = internal field name
    # value = (sqlite type, value category, source column name)
    fields = {
        "air_temperature": (
            ("temphum_quality_level", "int"),  # Qualitaets_Niveau
            ("temphum_structure_version", "int"),  # Struktur_Version
            ("temphum_temperature", "real"),  # LUFTTEMPERATUR
            ("temphum_humidity", "real"),  # REL_FEUCHTE
        ),
        "precipitation": (
            ("precipitation_quality_level", "int"),  # Qualitaets_Niveau
            ("precipitation_fallen", "int"),  # NIEDERSCHLAG_GEFALLEN_IND
            ("precipitation_height", "real"),  # NIEDERSCHLAGSHOEHE
            ("precipitation_form", "int"),  # NIEDERSCHLAGSFORM
        ),
        "soil_temperature": (
            ("soiltemp_quality_level", "int"),  # Qualitaets_Niveau
            ("soiltemp_1_temperature", "real"),  # ERDBODENTEMPERATUR
            ("soiltemp_1_depth", "real"),  # MESS_TIEFE
            ("soiltemp_2_temperature", "real"),  # ERDBODENTEMPERATUR
            ("soiltemp_2_depth", "real"),  # MESS_TIEFE
            ("soiltemp_3_temperature", "real"),  # ERDBODENTEMPERATUR
            ("soiltemp_3_depth", "real"),  # MESS_TIEFE
            ("soiltemp_4_temperature", "real"),  # ERDBODENTEMPERATUR
            ("soiltemp_4_depth", "real"),  # MESS_TIEFE
            ("soiltemp_5_temperature", "real"),  # ERDBODENTEMPERATUR
            ("soiltemp_5_depth", "real"),  # MESS_TIEFE
        ),
        "solar": (
            ("solar_quality_level", "int"),  # Qualitaets_Niveau
            ("solar_duration", "int"),  # SONNENSCHEINDAUER
            ("solar_sky", "real"),  # DIFFUS_HIMMEL_KW_J
            ("solar_global", "real"),  # GLOBAL_KW_J
            ("solar_atmosphere", "real"),  # ATMOSPHAERE_LW_J
            ("solar_zenith", "real"),  # SONNENZENIT
            #("solar_TODO", "int"),  # MESS_DATUM_WOZ
        ),
        "sun": (
            ("sun_quality_level", "int"),  # Qualitaets_Niveau
            ("sun_structure_version", "int"),  # Struktur_Version
            ("sun_duration", "real"),  # STUNDENSUMME_SONNENSCHEIN
        ),
        "wind": (
            ("wind_quality_level", "int"),  # Qualitaets_Niveau
            ("wind_structure_version", "int"),  # Struktur_Version
            ("wind_speed", "real"),  # WINDGESCHWINDIGKEIT
            ("wind_direction", "int"),  # WINDRICHTUNG
        )
    }

    # Categories of measurements on the server
    # key=<category (folder name)> , value=<file name code>
    categories = {
        "precipitation": "RR",
        "soil_temperature": "EB",
        "solar": "ST",
        "sun": "SD",
        "air_temperature": "TU",
        "wind": "FF"
    }


    def __init__(self, **kwargs):
        """
        Use all keyword arguments as configuration
        - user
        - passwd
        - cachepath
        """

        cp = None
        if "cachepath" in kwargs:
            cp = kwargs["cachepath"]
        self.cachepath = self.init_cache(cp)
        # fetch latest data into cache

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


    def init_cache(self, path):
        """
        Creates .dwd-weather directory in the current
        user's home, where a cache database and config
        file will reside
        """
        if path is None:
            home = os.path.expanduser("~") + os.sep + ".dwd-weather"
        else:
            home = path
        if not os.path.exists(home):
            os.mkdir(home)
        self.db = sqlite3.connect(home + os.sep + "dwd-weather.db")
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
        return home
    

    def import_stations(self):
        """
        Load station meta data from DWD server.
        """
        if self.verbosity > 0:
            print("Importing stations data from FTP server")
        ftp = FTP(self.server)
        ftp.login(self.user, self.passwd)
        for cat in self.categories:
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



    def import_measures(self, station_id, latest=True, historic=False):
        """
        Load data from DWD server.
        Parameter:
        
        station_id: e.g. 2667 (Köln-Bonn airport)

        latest: Load most recent data (True, False)
        historic: Load older values

        We download ZIP files for several categories
        of measures. We then extract one file from
        each ZIP. This path is then handed to the
        CSV -> Sqilte import function.
        """
        if self.verbosity > 0:
            print("Importing measures for station %d from FTP server" % station_id)
        # Which files to import
        timeranges = []
        if latest:
            timeranges.append("recent")
        if historic:
            timeranges.append("historical")
        ftp = FTP(self.server)
        ftp.login(self.user, self.passwd)
        importfiles = []

        def download_and_import(path, filename, cat, timerange=None):
            output_path = self.cachepath + os.sep + filename
            if timerange is None:
                timerange = "-"
            data_filename = "data_%s_%s_%s.txt" % (station_id, timerange, cat)
            if self.verbosity > 1:
                print("Reading file %s/%s from FTP server" % (path, filename))
            ftp.retrbinary('RETR ' + filename, open(output_path, 'wb').write)
            with ZipFile(output_path) as myzip:
                for f in myzip.infolist():
                    if "Terminwerte" in f.filename:
                        # this is our data file
                        myzip.extract(f, self.cachepath + os.sep)
                        os.rename(self.cachepath + os.sep + f.filename,
                            self.cachepath + os.sep + data_filename)
                        importfiles.append([cat, self.cachepath + os.sep + data_filename])
            os.remove(output_path)

        for cat in self.categories.keys():
            if self.verbosity > 1:
                print("Handling category %s" % cat)
            if cat == "solar":
                path = "%s/%s" % (self.serverpath, cat)
                ftp.cwd(path)
                # list dir content, get right file name
                serverfiles = []
                ftp.retrlines('NLST', serverfiles.append)
                filename = None
                for fn in serverfiles:
                    if ("_%05d." % station_id) in fn:
                        filename = fn
                        break
                if filename is None:
                    if self.verbosity > 1:
                        print("Station %s has no data for category '%s'" % (station_id, cat))
                    continue
                else:
                    download_and_import(path, filename, cat)
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
                            print("Station %s has no data for category '%s'" % (station_id, cat))
                        continue
                    download_and_import(path, filename, cat, timerange)
        for item in importfiles:
            self.import_measures_textfile(item[0], item[1])
            os.remove(item[1])


    def import_measures_textfile(self, category, path):
        """
        Import content of source text file into database
        """
        f = open(path, "rb")
        content = f.read()
        f.close()
        content = content.strip()
        sets = []
        # create SQL template
        for fieldname, fieldtype in self.fields[category]:
            sets.append(fieldname + "=?")
        insert_template = """INSERT OR IGNORE INTO measures (station_id, datetime) VALUES (?, ?)"""
        update_template = "UPDATE measures SET %s WHERE station_id=? AND datetime=?" % ", ".join(sets)
        # create data rows
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
                # station id
                parts[0] = int(parts[0])
                # timestamp
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
                            import traceback
                            traceback.print_tb(trace)
                            sys.exit()
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


    def query(self, station_id, hour, recursion=0):
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
                    self.import_measures(station_id, latest=True)
                elif age >= 360 and age <= 370:
                    self.import_measures(station_id, latest=True, historic=True)
                else:
                    self.import_measures(station_id, historic=True)
                return self.query(station_id, hour, recursion=(recursion + 1))
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
        import json
        return json.dumps(out)

    def stations_csv(self, delimiter=","):
        """
        Return stations list as CSV
        """
        import csv
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

if __name__ == "__main__":

    def get_station(args):
        dw = DwdWeather(cachepath=args.cachepath, verbosity=args.verbosity)
        import json
        print json.dumps(dw.nearest_station(lon=args.lon, lat=args.lat), indent=4)

    def get_stations(args):
        dw = DwdWeather(cachepath=args.cachepath, verbosity=args.verbosity)
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
        hour = datetime.strptime(str(args.hour), "%Y%m%d%H")
        dw = DwdWeather(cachepath=args.cachepath, verbosity=args.verbosity)
        import json
        print json.dumps(dw.query(args.station_id, hour), indent=4)

    import argparse
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

    args = argparser.parse_args()
    args.func(args)
