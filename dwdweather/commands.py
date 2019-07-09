# -*- coding: utf-8 -*-
# (c) 2014 Marian Steinbach, MIT licensed
# (c) 2018-2019 Andreas Motl, MIT licensed
import os
import json
import logging
import argparse

from dateutil.parser import parse as parsedate
from dwdweather.core import DwdWeather
from dwdweather.knowledge import DwdCdcKnowledge
from dwdweather.util import float_range, setup_logging

log = logging.getLogger(__name__)


def run():
    def get_station(args):
        dw = DwdWeather(
            resolution=args.resolution,
            category_names=args.categories,
            cache_path=args.cache_path,
            reset_cache=args.reset_cache,
        )
        output = json.dumps(dw.nearest_station(lon=args.lon, lat=args.lat), indent=4)
        print(output)

    def get_stations(args):
        dw = DwdWeather(
            resolution=args.resolution,
            category_names=args.categories,
            cache_path=args.cache_path,
            reset_cache=args.reset_cache,
        )
        output = ""
        if args.type == "geojson":
            output = dw.stations_geojson()
        elif args.type == "csv":
            output = dw.stations_csv()
        elif args.type == "plain":
            output = dw.stations_csv(delimiter="\t")
        if args.output_path is None:
            print(output)
        else:
            f = open(args.output_path, "wb")
            f.write(output)
            f.close()

    def get_weather(args):

        # Workhorse
        dw = DwdWeather(
            resolution=args.resolution,
            category_names=args.categories,
            cache_path=args.cache_path,
            reset_cache=args.reset_cache,
        )

        # Sanitize some input values
        timestamp = parsedate(str(args.timestamp))

        # Query data
        station_id = args.station_id
        categories = args.categories
        log.info(
            'Querying data for station "{station_id}" and categories "{categories}" at "{timestamp}"'.format(
                **locals()
            )
        )
        results = dw.query(station_id, timestamp)
        print(json.dumps(results, indent=4, sort_keys=True))

    argparser = argparse.ArgumentParser(
        prog="dwdweather", description="Get weather information for Germany."
    )

    # Add option parsers for subcommands.
    subparsers = argparser.add_subparsers(title="Actions", help="Main client actions.")

    # 1. "station" options
    parser_station = subparsers.add_parser("station", help="Find a station")
    parser_station.set_defaults(func=get_station)
    parser_station.add_argument(
        "lon",
        type=float_range(-180, 180),
        help="Geographic longitude (x) component as float, e.g. 7.2",
    )
    parser_station.add_argument(
        "lat",
        type=float_range(-90, 90),
        help="Geographic latitude (y) component as float, e.g. 53.9",
    )

    # 2. "stations" options
    parser_stations = subparsers.add_parser("stations", help="List or export stations")
    parser_stations.set_defaults(func=get_stations)
    parser_stations.add_argument(
        "-t",
        "--type",
        dest="type",
        choices=["geojson", "csv", "plain"],
        default="plain",
        help="Export format",
    )
    parser_stations.add_argument(
        "-f",
        "--file",
        type=str,
        dest="output_path",
        help="Export file path. If not given, STDOUT is used.",
    )

    # 3. "weather" options
    parser_weather = subparsers.add_parser(
        "weather", help="Get weather data for a station and hour"
    )
    parser_weather.set_defaults(func=get_weather)
    parser_weather.add_argument(
        "station_id", type=int, help="Numeric ID of the station, e.g. 2667"
    )
    parser_weather.add_argument(
        "timestamp",
        type=str,
        help="Timestamp in the format of YYYY-MM-DDTHH or YYYY-MM-DDTHH:MM",
    )

    # Add global options to all subparsers.

    for parser in [parser_station, parser_stations, parser_weather]:

        # "--resolution" option for choosing the corresponding dataset, defaults to "hourly"
        resolutions_available = DwdCdcKnowledge.climate.get_resolutions().keys()
        parser.add_argument(
            "--resolution",
            type=str,
            choices=resolutions_available,
            default="hourly",
            help='Select dataset by resolution. By default, the "hourly" dataset is used.',
        )

        # "--categories" option for restricting import to specified category names, defaults to "all"
        categories_available = [
            item["name"] for item in DwdCdcKnowledge.climate.measurements
        ]
        parser.add_argument(
            "--categories",
            type=str,
            nargs="*",
            choices=categories_available,
            help="List of comma-separated categories to import. "
            "By default, *all* categories will be imported.",
        )

        # "--reset-cache" option for dropping the cache database before performing any work
        parser.add_argument(
            "--reset-cache", action="store_true", help="Drop the cache database"
        )

        # Debugging.
        parser.add_argument(
            "-d",
            dest="debug",
            action="count",
            default=0,
            help="Activate debug output. Use -dd or -ddd to increase verbosity.",
        )

        # Path to sqlite database for caching.
        default_path = os.path.expanduser("~") + os.sep + ".dwd-weather"
        parser.add_argument(
            "-c",
            dest="cache_path",
            default=default_path,
            help="Path to cache directory. Defaults to .dwd-weather in user's home dir.",
        )

    args = argparser.parse_args()
    if args.debug > 0:
        setup_logging(logging.DEBUG)
    else:
        setup_logging()
    args.func(args)
