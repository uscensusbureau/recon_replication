#!/usr/bin/env python3
#
"""
Make the microdata on a county-by-county basis.
"""

import csv
import dbrecon
import gc
import glob
import logging
import os
import os.path
import subprocess
import sys
import time
import atexit
import multiprocessing

import dbrecon
from dbrecon import DBMySQL
from dbrecon import dopen,dmakedirs,dsystem,dpath_exists,GB,REIDENT

def make_csv_file( pair ):
    # Make sure we have a solution file for every tract
    auth = dbrecon.auth()
    (stusab, county, overwrite) = pair
    county_csv_filename = dbrecon.COUNTY_CSV_FILENAME(stusab=stusab, county=county)
    county_csv_filename_tmp = county_csv_filename+".tmp"

    if dbrecon.dpath_exists(county_csv_filename):
        if overwrite:
            logging.info(f"{county_csv_filename} exists --- overwriting")
        else:
            logging.info(f"{county_csv_filename} exists --- will not overwrite")
            return
    if dbrecon.dpath_exists(county_csv_filename_tmp):
        logging.warning(f"{county_csv_filename_tmp} exists --- another process is running?")
        return

    state_code = dbrecon.state_fips(stusab)
    tracts     = dbrecon.tracts_for_state_county(stusab=stusab, county=county)
    missing = 0
    for tract in tracts:
        solfile = dbrecon.SOLFILENAMEGZ(stusab=stusab, county=county, tract=tract)
        if not dpath_exists(solfile):
            logging.error("No solution file: {}".format(solfile))
            missing += 1
    if missing>0:
        logging.error(f"Will not make {stusab} {state_code}{county} CSV file; missing tracts: {missing}")
        return

    with dopen(county_csv_filename_tmp,"w") as outfile:
        w = csv.writer(outfile)
        w.writerow(['geoid_tract','geoid_block','sex','age','white','black','aian','asian','nhopi','sor','hisp'])
        county_total = 0
        for tract in tracts:
            tract_total = 0
            logging.info(f"Starting tract {state_code}{county}{tract}")
            dbrecon.db_start(auth, 'csv', stusab, county, tract)
            with dopen(dbrecon.SOLFILENAMEGZ(stusab=stusab, county=county, tract=tract),"r") as infile:
                for line in infile:
                    if line[0:2]=='C_': # oldstyle variable
                        (var,count) = line.strip().split()
                        count = round(float(count)) # some of the solutions are not precisely equal to 0 or 1
                        # don't count the zeros
                        if count==0:
                            continue
                        elif count==1:
                            c = var.split("_")
                            tract_ = c[1][5:11]
                            if tract != tract_:
                                raise RuntimeError(f"{infile.name}: Expecting tract {tract} read {tract_}")
                            geoid_tract = state_code + county + tract
                            w.writerow([geoid_tract, c[1], c[5], c[6], c[7], c[8], c[9], c[10], c[11], c[12], c[13]])
                            tract_total += 1
                            county_total += 1
                        else:
                            raise ValueError(f"invalid count={count} in line: {line}")
                logging.info(f"Ending {state_code}{county}{tract} tract pop: {tract_total}")
            # done with this tract
            dbrecon.db_done(auth, 'csv', stusab, county, tract)
        # done with all tracts

    dbrecon.dwait_exists(county_csv_filename_tmp)
    dbrecon.drename(county_csv_filename_tmp, county_csv_filename)
    logging.info(f"Ending {state_code}{county} county pop: {county_total}")
    print(f"{__file__} {state_code}{county} county pop: {county_total}")

if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Create the microdata file from all of the solutions for each tract in a given county.." )
    dbrecon.argparse_add_logging(parser)
    parser.add_argument("stusab", help="2-character state abbreviation; can be 'all' for all states.")
    parser.add_argument("county", help="3-digit county code; can be 'all' for all counties; must be 'all' if all states are specified")
    parser.add_argument("--j1", help="Specify number of counties to create CSV files at once", default=64, type=int)
    parser.add_argument("--overwrite", help="Overwrite the output files even if they exist", action='store_true')

    args       = parser.parse_args()
    config     = dbrecon.setup_logging_and_get_config(args=args,prefix="05micro")

    # Get a list of the state/county pairs to make
    pairs = []
    if args.stusab=='all':
        if args.county!='all':
            raise ValueError("'all' states requires 'all' counties")
        stusabs = dbrecon.all_stusabs()
    else:
        stusabs = [dbrecon.stusab(args.stusab).lower()]

    for stusab in stusabs:
        if args.county=='all':
            for county in dbrecon.counties_for_state(stusab):
                pairs.append( (stusab, county, args.overwrite) )
        else:
            pairs.append( (stusab, args.county, args.overwrite) )

    print(f"{__file__}: requested {len(pairs)} state/county pairs")
    with multiprocessing.Pool(args.j1) as p:
        p.map(make_csv_file, pairs)
