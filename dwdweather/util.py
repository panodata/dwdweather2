# -*- coding: utf-8 -*-
import sys
import logging
import argparse
import htmllistparse


def setup_logging(level=logging.INFO):
    log_format = "%(asctime)-15s [%(name)-20s] %(levelname)-7s: %(message)s"
    logging.basicConfig(format=log_format, stream=sys.stderr, level=level)


def float_range(min, max):
    def check_range(x):
        x = float(x)
        if x < min or x > max:
            raise argparse.ArgumentTypeError("%r not in range [%r, %r]" % (x, min, max))
        return x

    return check_range


def fetch_html_file_list(baseurl, extension):

    cwd, listing = htmllistparse.fetch_listing(baseurl, timeout=10)
    result = [
        baseurl + "/" + item.name
        for item in listing
        if item.name.endswith(extension)
    ]
    return result
