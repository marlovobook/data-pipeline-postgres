#!/usr/local/bin python3

import sys
import csv
import pandas as pd
import logging

def _product_to_demand(input, output):
    logging.info(f"read to {input}")
    logging.info(f"load to {output}")


input=sys.argv[1]
output=sys.argv[2]

_product_to_demand(input, output)