# -*- coding: utf-8 -*-
# (c) 2014 Marian Steinbach, MIT licensed
# (c) 2018-2019 Andreas Motl, MIT licensed
import io
import os
import logging
from urllib.parse import urlparse
from zipfile import ZipFile

from requests_cache import CachedSession

from dwdweather import __appname__ as APP_NAME
from dwdweather import __version__ as APP_VERSION
from dwdweather.util import parse_htmllist

log = logging.getLogger(__name__)


class DwdCdcClient:

    # DWD CDC HTTP server.
    baseuri = "https://opendata.dwd.de/climate_environment/CDC"

    # Observations in Germany.
    germany_climate_uri = baseuri + "/observations_germany/climate/{resolution}"

    def __init__(self, resolution, cache_path):

        # Data set selector by resolution (houry, 10_minutes).
        self.resolution = resolution

        # HTTP client.
        self.http = None

        # Path where response cache sqlite database is stores.
        self.cache_path = cache_path

        # Expiration time of response cache.
        self.cache_ttl = 300

        # CDC server URI.
        self.uri = self.germany_climate_uri.format(resolution=self.resolution)
        log.info(
            'Acquiring dataset for resolution "{}" from "{}"'.format(
                self.resolution, self.uri
            )
        )

        self.setup_cache()

    def setup_cache(self):
        """Setup HTTP client cache"""

        # Configure User-Agent string.
        user_agent = APP_NAME + "/" + APP_VERSION

        # Use hostname of url as cache prefix.
        cache_name = urlparse(self.uri).netloc

        # Configure cached requests session.
        self.http = CachedSession(
            backend="sqlite",
            cache_name=os.path.join(self.cache_path, cache_name),
            expire_after=self.cache_ttl,
            user_agent=user_agent,
        )

    def get_resource_index(self, uri, extension):
        log.info(u'Requesting %s', uri)
        response = self.http.get(uri + u'/')
        if response.status_code != 200:
            raise ValueError("Fetching resource {} failed".format(uri))
        resource_list = parse_htmllist(uri, extension, response.content)
        return resource_list

    def get_stations(self, categories):
        """
        Load station meta data from DWD server.
        """
        log.info("Loading station data from CDC")
        for category in categories:
            category_name = category["name"]
            if category_name == "solar" and self.resolution == "hourly":
                # workaround - solar has no subdirs
                index_uri = u"%s/%s" % (self.uri, category_name)
            else:
                index_uri = u"%s/%s/recent" % (self.uri, category_name)

            try:
                resource_list = self.get_resource_index(index_uri, "txt")
            except:
                log.warning(
                    'Resolution "{}" has no category "{}" or request failed'.format(
                        self.resolution, category_name
                    )
                )
                continue

            # Get directory contents.
            for resource_uri in resource_list:
                if "Beschreibung_Stationen" not in resource_uri:
                    continue
                log.info("Fetching resource {}".format(resource_uri))
                response = self.http.get(resource_uri)
                yield DwdCdcResult(self.resolution, category, response=response)

    def get_measurements(self, station_id, category, timeranges):

        category_name = category["name"]

        def download_zip(uri):
            log.info("Fetching resource {}".format(uri))
            response = self.http.get(uri)
            with ZipFile(io.BytesIO(response.content)) as myzip:
                for f in myzip.infolist():
                    # This is the data file
                    # print('zip content:', f.filename)
                    if f.filename.startswith("produkt_"):
                        log.info("Reading from Zip: %s" % (f.filename))
                        payload = myzip.read(f.filename)
                        real_uri = "{}/{}".format(uri, f.filename)
                        thing = DwdCdcResult(
                            self.resolution, category, uri=real_uri, payload=payload
                        )
                        yield thing

        def find_resource_file(index_uri, pattern):
            try:
                resource_list = self.get_resource_index(index_uri, "zip")
            except:
                log.exception('Could not acquire resource from {}'.format(index_uri))
                return

            # Get directory contents.
            for resource_uri in resource_list:
                if pattern in resource_uri:
                    return resource_uri

        def download_resource(uri):
            if resource_uri_effective is None:
                log.warning(
                    'Station "{}" has no data for category "{}"'.format(
                        station_id, category_name
                    )
                )
            else:
                for thing in download_zip(uri):
                    yield thing

        if category_name == "solar" and self.resolution == "hourly":
            index_uri = "%s/%s" % (self.uri, category_name)
            resource_uri_effective = find_resource_file(
                index_uri, "_%05d_" % station_id
            )
            for item in download_resource(resource_uri_effective):
                yield item

        else:
            for timerange in timeranges:
                timerange_suffix = "akt"
                if timerange == "historical":
                    timerange_suffix = "hist"

                index_uri = "%s/%s/%s" % (self.uri, category_name, timerange)
                resource_uri_effective = find_resource_file(
                    index_uri, "_%05d_" % station_id
                )
                for item in download_resource(resource_uri_effective):
                    yield item


class DwdCdcResult:
    def __init__(self, resolution, category, uri=None, payload=None, response=None):
        self.resolution = resolution
        self.category = category

        self.uri = uri
        self.payload = payload

        self.response = response
        if self.response:
            self.uri = self.response.url
            self.payload = self.response.content
