import datetime
import json
import os
import os.path
import re
import sqlite3
import sys
import time
import zipfile
import io
import logging
import numpy as np
import itertools
import statistics
import subprocess
import gc
import math
from collections import deque,defaultdict

sys.path.append( os.path.join( os.path.dirname(__file__), ".."))

import pl94_geofile
import pl94_dbload
import ctools.dbfile
import ctools.clogging
import ctools.timer
import das_constants
import geotree


def test_geotree() -> None:
    import argparse
    parser = argparse.ArgumentParser(description='Ingest the PL94 block-level population counts',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--db", help="Specify database location", default=pl94_dbload.DBFILE)
    parser.add_argument("--create", action='store_true', help='create the schema')
    parser.add_argument("--fill", action='store_true', help='like create, but just inserts')
    parser.add_argument("--drop", action='store_true', help='drop the schema')
    parser.add_argument("--dumpblocks",   action='store_true', help='print the blocksblocks')
    parser.add_argument("--info",    help="print info", action='store_true')
    parser.add_argument("--scheme" , help='specify partitioning scheme')
    parser.add_argument("--report", action='store_true', help="Create a report")
    parser.add_argument("--names", action='store_true', help='display names')
    parser.add_argument("--levels", type=int, help="how many levels")
    parser.add_argument("--xpr",     action='store_true', help='remove PR from reports')
    parser.add_argument("--geo_stusab",  help='Use geography table for STATE')
    parser.add_argument("--report_stusab",  help='Only report for for STATE')
    parser.add_argument("--mean_report", help="Print geometric mean report", action='store_true')
    parser.add_argument("--xempty",  action='store_true', help='remove blocks with 0 population and 0 housing')
    parser.add_argument("--limit",   help='limit expensive queries to speed debugging',  type=int)
    parser.add_argument("--upload",  help='upload via ssh to the specified URL')
    parser.add_argument("extra", nargs="*")
    ctools.clogging.add_argument(parser)
    args = parser.parse_args()
    args.name = 'table4'
    args.scheme = 'v4'
    db   = ctools.dbfile.DBSqlite3(args.db,dicts=True,debug=False)
    db.set_cache_bytes(4*1024*1024*1024)
    gt = geotree.GeoTree(db,args.name,args)

    # check aiannh
    assert gt.include_aianhh("0000")[0]==False
    assert gt.include_aianhh("5050")[0]==True

    # Check state schemes
    assert gt.state_scheme("11") == geotree.SS_DC

    # Check geocoding for DC
    gh = db.execselect("select * from geo where state='DC' and sumlev=750 and AIANHH=9999 limit 1")
    assert gt.geocode_v4(gh)[0]=='D11'

    # Check geocoding for CT
    gh = db.execselect("select * from geo where state='CT' and sumlev=750 and AIANHH=9999 limit 1")
    assert gt.geocode_v4(gh)[0]=='N09'

    gh = db.execselect("select * from geo where state='CT' and sumlev=750 and AIANHH!=9999 limit 1")
    assert gt.geocode_v4(gh)[0]=='A09'

    # Check PA
    gh = db.execselect("select * from geo where state='PA' and sumlev=750 and AIANHH=9999 limit 1")
    assert gt.geocode_v4(gh)[0]=='M'

    # Check CA
    gh = db.execselect("select * from geo where state='CA' and sumlev=750 and AIANHH!=9999 limit 1")
    assert gt.geocode_v4(gh)[0]=='A09'


    
