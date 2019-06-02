# -*- coding: utf-8 -*-
import sys
import logging
import argparse


def setup_logging(level=logging.INFO):
    log_format = '%(asctime)-15s [%(name)-10s] %(levelname)-7s: %(message)s'
    logging.basicConfig(
        format=log_format,
        stream=sys.stderr,
        level=level)


def float_range(min, max):
    def check_range(x):
        x = float(x)
        if x < min or x > max:
            raise argparse.ArgumentTypeError("%r not in range [%r, %r]"%(x, min, max))
        return x
    return check_range
