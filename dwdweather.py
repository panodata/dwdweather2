# encoding: utf8

import sys
import os
import os.path
from ftplib import FTP
from zipfile import ZipFile
import sqlite3
from datetime import datetime
import math

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
    server = "ftp-outgoing2.dwd.de"

    # FTP server path for our files
    serverpath = "/gds/gds/specials/climate/tables/germany/hourly_value"

    # database Field definition:
    # key = internal field name
    # value = (sqlite type, value category, source column name)
    fields = {
        "precipitation": (
            ("precipitation_quality_level", "int"),  # Qualitaets_Niveau
            ("precipitation_fallen", "int"),  # NIEDERSCHLAG_GEFALLEN_IND
            ("precipitation_height", "real"),  # NIEDERSCHLAGSHOEHE
            ("precipitation_form", "int"),  # NIEDERSCHLAGSFORM
        ),
        "soil_temp": (
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
        "sun": (
            ("sun_quality_level", "int"),  # Qualitaets_Niveau
            ("sun_structure_version", "int"),  # Struktur_Version
            ("sun_duration", "real"),  # STUNDENSUMME_SONNENSCHEIN
        ),
        "temp_hum": (
            ("temphum_quality_level", "int"),  # Qualitaets_Niveau
            ("temphum_structure_version", "int"),  # Struktur_Version
            ("temphum_temperature", "real"),  # LUFTTEMPERATUR
            ("temphum_humidity", "real"),  # REL_FEUCHTE
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
        "soil_temp": "EB",
        "sun": "SD",
        "temp_hum": "TU",
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

        if "user" in kwargs:
            self.user = kwargs["user"]
        if "passwd" in kwargs:
            self.passwd = kwargs["passwd"]


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
                name text
            )"""
        index = """CREATE UNIQUE INDEX IF NOT EXISTS unq
            ON stations (station_id, date_start)"""
        c.execute(create)
        c.execute(index)
        self.db.commit()
        return home
    

    def import_stations(self):
        """
        Load station meta data from DWD server.
        """
        ftp = FTP(self.server)
        ftp.login(self.user, self.passwd)
        path = self.serverpath + "/temp_hum/"
        ftp.cwd(path)

        serverfiles = []

        ftp.retrlines('NLST', serverfiles.append)
        for filename in serverfiles:
            if "stundenwerte" not in filename:
                continue
            if "akt" not in filename:
                continue
            output_path = self.cachepath + os.sep + filename
            #print output_path
            ftp.retrbinary('RETR ' + filename, open(output_path, 'wb').write)
            with ZipFile(output_path) as myzip:
                for f in myzip.infolist():
                    if "Stationsmetadaten" in f.filename:
                        myzip.extract(f, self.cachepath + os.sep)
                        fp = open(self.cachepath + os.sep + f.filename)
                        self.import_station(fp.read())
                        fp.close
                        os.remove(self.cachepath + os.sep + f.filename)
            os.remove(output_path)
        

    def import_station(self, content):
        """
        Takes the content of one station metadata file
        and imports it into the database
        """
        content = content.strip()
        content = content.replace("\r", "")
        linecount = 0
        insert_sql = """INSERT OR IGNORE INTO stations
            (station_id, date_start, date_end, geo_lon, geo_lat, height, name)
            VALUES (?, ?, ?, ?, ?, ?, ?)"""
        update_sql = """UPDATE stations
            SET date_end=?, geo_lon=?, geo_lat=?, height=?, name=?
            WHERE station_id=? AND date_start=?"""
        cursor = self.db.cursor()
        for line in content.split("\n"):
            linecount += 1
            if linecount > 1:
                line = line.strip()
                if line == "":
                    continue
                parts = line.split(";")
                for n in range(len(parts)):
                    parts[n] = parts[n].strip()
                parts[0] = int(parts[0])  # station id
                parts[1] = int(parts[1])  # height
                parts[2] = float(parts[2])  # latitude
                parts[3] = float(parts[3])  # longitude
                parts[4] = int(parts[4])  # start date
                if parts[5] == "":  # end date
                    parts[5] = None
                else:
                    parts[5] = int(parts[5])
                parts[6] = parts[6].decode("latin-1")  # name
                # issue sql
                cursor.execute(insert_sql, (
                    parts[0],
                    parts[4],
                    parts[5],
                    parts[3],
                    parts[2],
                    parts[1],
                    parts[6]))
                cursor.execute(update_sql, (
                    parts[5],
                    parts[3],
                    parts[2],
                    parts[1],
                    parts[6],
                    parts[0],
                    parts[4]))
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
        # Which files to import
        timerange = []
        if latest:
            timerange.append("akt")
        if historic:
            timerange.append("hist")
        ftp = FTP(self.server)
        ftp.login(self.user, self.passwd)
        importfiles = []
        for cat in self.categories.keys():
            path = "%s/%s/" % (self.serverpath, cat)
            ftp.cwd(path)
            for part in timerange:
                filename = "stundenwerte_%s_%05d_%s.zip" % (
                    self.categories[cat], station_id, part)
                output_path = self.cachepath + os.sep + filename
                data_filename = "data_%s_%s_%s.txt" % (station_id, cat, part)
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
                parts[0] = int(parts[0])
                parts[1] = int(parts[1])
                if category in ["wind", "sun", "temp_hum"]:
                    # remove funny redundant datetime
                    del parts[2]
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
                            sys.stderr.write("Error in converting field to int.\n")
                            print(parts)
                            print(fieldname, fieldtype)
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


    def query(self, station_id, hour):
        """
        Get values from cache.
        station_id: Numeric station ID
        hour: datetime object
        """
        sql = "SELECT * FROM measures WHERE station_id=? AND datetime=?"
        c = self.db.cursor()
        c.execute(sql, (station_id, hour.strftime("%Y%m%d%H")))
        out = c.fetchone()
        if out is None:
            # cache miss
            age = (datetime.utcnow() - hour).total_seconds() / 86400
            if age < 365:
                self.import_measures(station_id, latest=True)
            elif age >= 365 and age <= 366:
                self.import_measures(station_id, latest=True, historic=True)
            else:
                self.import_measures(station_id, historic=True)
            return self.query(station_id, hour)
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
        sql = "SELECT * FROM stations WHERE date_end IS NULL"
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
        return out

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", dest="user",
        help="DWD FTP user name")
    parser.add_argument("-p", dest="passwd",
        help="DWD FTP user password")
    parser.add_argument("--cache", dest="cachepath",
        help="Path to cache directory. Defaults to .dwd-weather in user's home dir.")

    args = parser.parse_args()

    dw = DwdWeather(user=args.user,
        passwd=args.passwd,
        cachepath=args.cachepath)

    print("Station close to Cologne:")
    print(dw.nearest_station(lon=7, lat=51))
    
    print("Latest entry age:")
    print dw.get_data_age()
    
    import json
    print("Weather at Cologne/Bonn airport at 2012-01-12 12:00 UTC:")
    print json.dumps(dw.query(2667, datetime(2012, 1, 12, 12)), indent=4, sort_keys=True)

    print("Saving stations GeoJSON file as stations.geojson.")
    fp = open("stations.geojson", "wb")
    fp.write(json.dumps(dw.stations_geojson()))
    fp.close()
