#!/usr/bin/env python3
#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)
#
"""
read_geo_file.py:

Inputs: SF1 ZIPFILES Outputs: SF1 geofile as a CSV with a file header and list
of counties in the state

Output location: $ROOT/{stusab}/

Part of the replication archive for The U.S. Census Bureau's Ex Post
Confidentiality Analysis of the 2010 Census Data Publications
(https://github.com/uscensusbureau/recon_replication)
"""

HELP="""
Read SF1 geography files, creates the counties geography files, and loads the MySQL database.
This is pretty fast and DB heavy, so it is not parallelized
"""
import json
import csv
import collections
import sys
import logging
import time
import os
import io
import multiprocessing
import zipfile
import random

import dbrecon                  # brings in path
from ctools.dbfile import DBMySQL,DBMySQLAuth

sys.path.append( os.path.join(os.path.dirname(__file__),".."))

import ctools.s3
from ctools.timer import Timer
from dbrecon import dopen,dpath_exists,dpath_expand,dmakedirs,GEOFILE_FILENAME_TEMPLATE,STATE_COUNTY_FILENAME_TEMPLATE,REIDENT,DBMySQL,sf1_zipfilename

GEO_LAYOUT_FILENAME = "$SRC/layouts/geo_layout.txt"
TRANSACTION_RECORDS = 20

def fields_str(field_count,record_count=1):
    fields = "(" + ",".join(['%s']*field_count) + ")"
    fields_str = ",".join([fields] * record_count)
    return fields_str

def make_county_list(stusab:str):
    """Given a state abbreviation, find the geo file, extract information, and store in the CSV files
    and in the SQL database."""

    auth = dbrecon.auth()

    print(f"make_county_list({stusab})")

    # Random wait of 0-10 seconds so all of the threads don't hit aws at the same time.
    time.sleep(random.uniform(0,10))
    state_code       = dbrecon.state_fips(stusab)

    # Input files
    sf1_geofilename  = f"{stusab}geo2010.sf1"

    # Open the SF1 zipfile
    zf = zipfile.ZipFile(sf1_zipfilename(stusab))
    if args.latin1:
        sf1_geofile = io.TextIOWrapper(zf.open(sf1_geofilename), encoding='latin1')
    else:
        sf1_geofile = io.TextIOWrapper(zf.open(sf1_geofilename), encoding='utf-8')

    #
    # Input Reader --- Get layout for geofile. This uses the hard-coded information in the geo_layout.txt file.
    names    = []
    colspecs = []

    #
    # Yes, SF1 in 2010 used latin1.
    for g in csv.DictReader(dopen(GEO_LAYOUT_FILENAME,'r', encoding='latin1'), delimiter=' '):
        names.append(g['field_name'])
        colspecs.append((int(g['start_pos'])-1, int(g['start_pos'])+int(g['length'])-1))

    # CSV output. We make this by default. In the future  we should be able to run it entirely out of the database
    if args.csv:
        # Output files
        geofile_csv_filename       = GEOFILE_FILENAME_TEMPLATE.format(stusab=stusab)
        state_county_list_filename = STATE_COUNTY_FILENAME_TEMPLATE.format(stusab=stusab, state_code=state_code)

        logging.info(f"Creating {geofile_csv_filename}")
        csvfile = dopen(geofile_csv_filename, 'w')
        writer  = csv.DictWriter(csvfile, fieldnames=names)
        writer.writeheader()
        f       = dopen(state_county_list_filename,'w')
        writer2 = csv.writer(f)

    # Get our own database connection
    DBMySQL.csfr(auth,f"DELETE from {REIDENT}geo where STUSAB=%s",(stusab,))

    # extract fields from the geofile and write them to the geofile_csv_file and/or the database
    # We do this because the geofile is position-specified and it was harder to use that.
    # We also extract the county list
    #
    DB_FIELDS = "STUSAB,SUMLEV,LOGRECNO,STATE,COUNTY,TRACT,BLOCK,NAME,POP100"
    DB_FIELDS_ARRAY = DB_FIELDS.split(",")

    vals = []
    count = 0
    total_count = 0

    for line in sf1_geofile:
        # convert the column-specified line to a dict
        dataline = collections.OrderedDict(list(zip(names,[line[c[0]:c[1]] for c in colspecs])))

        # Write to CSV file
        if args.csv:
            writer.writerow(dataline)
            if dataline['SUMLEV']=='050':
                row = [dataline['STATE'], dataline['COUNTY'],stusab]
                writer2.writerow(row)

        # Write to database specific geolevels
        if (dataline['SUMLEV'] in ['040', '050', '140', '750', '101']) and (dataline['GEOCOMP']=='00'):
            vals   += [dataline[field].strip() for field in DB_FIELDS_ARRAY]
            count  += 1
            total_count += 1
            if count >= TRANSACTION_RECORDS:
                DBMySQL.csfr(auth,f"INSERT INTO {REIDENT}geo ({DB_FIELDS}) VALUES " + fields_str(len(DB_FIELDS_ARRAY),count), vals)
                vals = []
                count = 0

        if total_count % 100000==0:
            print(f"{stusab} {total_count} ... ")

    # Finish up database transaction
    if count>0:
        DBMySQL.csfr(auth,f"INSERT INTO {REIDENT}geo ({DB_FIELDS}) VALUES " + fields_str(len(DB_FIELDS_ARRAY),count), vals)
        total_count += count
    print(f"{stusab} Total: {total_count}")
    assert total_count>0
    sf1_geofile.close()

    # Select georecords into the tracts
    DBMySQL.csfr(auth,
                 f"""
                 INSERT INTO {REIDENT}tracts (stusab,state,county,tract,pop100)
                 SELECT stusab,state,county,tract,pop100 FROM {REIDENT}geo where sumlev=140 and stusab=%s
                 """,(stusab,))

    if args.csv:
        csvfile.close()
        f.close()


if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description = HELP )
    parser.add_argument("--showcounties",
                        help = "Display all counties for state from files that were created", action='store_true')
    parser.add_argument("--nocsv",
                        help = "Do not make the CSV files", action='store_true')
    parser.add_argument("--j1",
                        help = 'number of threads to run in parallel', default=10,type=int)
    parser.add_argument('--force', help='make files even if they already exist', action='store_true')
    parser.add_argument("stusab", help="States to process. Say 'all' for all", nargs='*')
    parser.add_argument("--latin1", help="encoding Latin-1", action='store_true')
    dbrecon.argparse_add_logging(parser)
    args     = parser.parse_args()
    config   = dbrecon.setup_logging_and_get_config(args=args,prefix="01mak")

    args.csv = not args.nocsv

    # Verify that all of the input files that are needed exist and that we can read them
    for fn in [GEO_LAYOUT_FILENAME,
               "$SRC/layouts/layouts.json",
               "$SRC/layouts/sf1_vars_race_binaries.csv"]:
        if not dbrecon.dpath_exists(fn):
            raise FileNotFoundError(fn)

    if args.stusab==[] or args.stusab[0]=='all':
        stusabs = dbrecon.all_stusabs()
    else:
        stusabs = args.stusab

    # Are we just printing status reports?
    if args.showcounties:
        for stusab in stusabs:
            print(dbrecon.counties_for_state(stusab))
        exit(0)

    # If we are not forcing, remove the counties already done
    if not args.force:
        nstusabs = []
        for stusab in stusabs:
            state_code       = dbrecon.state_fips(stusab)
            geofile_csv_filename       = GEOFILE_FILENAME_TEMPLATE.format(stusab=stusab)
            state_county_list_filename = STATE_COUNTY_FILENAME_TEMPLATE.format(stusab=stusab, state_code=state_code)
            if dpath_exists(geofile_csv_filename) and dpath_exists(state_county_list_filename):
                print(stusab,"already exists. Skipping")
            else:
                nstusabs.append(stusab)
        stusabs = nstusabs

    # Generate the CSV files. Do this in parallel
    if stusabs:
        args.j1 = min(args.j1, len(stusabs))
        with multiprocessing.Pool(args.j1) as p:
            p.map(make_county_list, stusabs)

    print("Made geofiles for: {}".format(" ".join(stusabs)))
    #validate(dbrecon.(), args.j1, stusabs)
