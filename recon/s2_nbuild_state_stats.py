#!/usr/bin/env python3
#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)
'''
02_build_state_stats.py:
Reads the SF1 files, filter on geolevels 050, 140 and 101,  join the segment
files into a single line, and output statistics. Part of the replication
archive for The U.S. Census Bureau's Ex Post Confidentiality Analysis of the
2010 Census Data Publications
(https://github.com/uscensusbureau/recon_replication)
'''

import json
import csv
import sys
import os
import pandas as pd
import numpy as np
import psutil
from collections import OrderedDict
from functools import reduce
import gc
import time
import logging
import multiprocessing
import random
import re
from itertools import repeat
import boto3

import dbrecon                  # brings in path
from dbrecon import dopen,dpath_exists,GEOFILE_FILENAME_TEMPLATE,sf1_zipfilename,REIDENT

from ctools.total_size import total_size
import ctools.s3 as s3
from ctools.timer import Timer
from ctools.dbfile import DBMySQL,DBMySQLAuth

# The linkage variables, in the order they appear in the file
SF1_LINKAGE_VARIABLES = ['FILEID','STUSAB','CHARITER','CIFSN','LOGRECNO']

ANY="any"

class ReaderException(Exception):
    pass

class SF1SegmentReader():
    """Class to read directly out of the ZIP file."""
    def __init__(self,*,stusab,segment_filename,names,xy,cifsn):
        """ Given a state abbreviation and a segment number, open it"""
        self.infile = dopen(segment_filename, zipfilename=sf1_zipfilename(stusab), encoding='latin1')
        self.fields = None       # the last line read
        self.names  = names      # names of the fields
        self.cifsn  = cifsn
        # process the names as pandas would
        for i in range(0,4):
            self.names[i] = self.names[i]+"_"+xy
        if self.cifsn>1:
            del self.names[4]   #  don't include

    def getfields(self,*,logrecno):
        """ Reads a line, looking for logrecno. If not found, return a line of fields """
        if self.fields is None:
            line = self.infile.readline().strip()
            if line=="":
                # End of this segment. Segment 1 has all the logrecs. Otherwise, this just doesn't go to the end
                if self.cifsn==1:
                    raise ReaderException(f"Reached end of CIFSN {self.cifsn} while looking for {logrecno}")
                else:
                    return ['' for name in self.names]
            self.fields   = line.split(",")
            self.logrecno = self.fields[4]

            # Undo the broken transformations
            # Delete LOGRECNO in all but the CIFSN 1
            if self.cifsn>1:
                del self.fields[4]
            # These two fields were improperly formatted as floats...
            self.fields[2] = format(int(self.fields[2]),'.1f') # CHARITER
            self.fields[3] = format(int(self.fields[3]),'.1f') # CIFSN
            assert len(self.fields) == len(self.names)

        if (self.logrecno == logrecno) or (logrecno==ANY):
            # Map the fields we have to the broken fields that are reported
            def refmt(nv):
                (name,val) = nv
                if name=='PCT012A001':
                    print(name,val,format(float(val),'.1f'))
                if (name in ['CHARITER','CIFSN']
                    or name.startswith('PCT')
                    or name.startswith('HCT')
                    or name.startswith('PCT012')):
                    return format(float(val),'.1f')

                if '.' in val and len(val)>3:
                    if val[-3]=='.':
                        if val[-1]=='0':
                            return val[0:-1]
                        return val


                return val

            fields = ([self.fields[0],        # FILEID
                       self.fields[1],        # STUSAB
                       refmt(('CHARITER',self.fields[2])), # CHARITER
                       refmt(('CIFSN',self.fields[3])), # CIFSN
                       self.fields[4]] +      # LOGRECNO
                      [refmt(nv) for nv in zip(self.names[5:],self.fields[5:]) ])

            self.fields = None
            return fields
        return ['' for name in self.names]

def process_stusab(stusab, fsync=False):
    """For a given stusab, create all of the county file SF1 extracts"""
    logging.info(f"{stusab}: building data frame with all SF1 measurements")
    with Timer() as timer:

        # Read in layouts -- they are json created from xsd from the access database
        # on the website.  Note -- the xsd had to be modified to undo the file
        # mods due to access limitations.  It's read as a ordered dict to preserve
        # the order of the layouts to read the csv.

        layouts          = json.load(dopen('$SRC/layouts/layouts.json'), object_pairs_hook=OrderedDict)
        geo_filename     = GEOFILE_FILENAME_TEMPLATE.format(stusab=stusab)


        # Generate the CSV header that the original code used
        # This looks weird, but we are trying to match the original files exactly.
        # while we are here, open the segment files
        field_names = ['']      # PANDASID
        xy = 'x'
        open_segments = []
        for (c,l) in enumerate(layouts,1):
            if l[:3]=='SF1' and l[9:-4]!='mod' and l[10:-5]!='PT':
                cifsn = int(l[4:-4])
                names = layouts[l]
                extra = l[4:-4]
                fname = f'$ROOT/{stusab}/sf1/{stusab}{extra}2010.sf1'
                segreader = SF1SegmentReader(stusab=stusab,
                                             segment_filename=fname,
                                             names = names,
                                             xy = xy,
                                             cifsn = cifsn )
                xy = 'y' if xy=='x' else 'x'
                open_segments.append(segreader)

                # add all the field names in order to header record, with pandas-style renaming
                field_names.extend( segreader.names )

        # Add the geoid fields
        JOIN_FIELDS = "STATE,COUNTY,TRACT,BLOCK,BLKGRP,SUMLEV".split(",")
        [field_names.append(field) for field in JOIN_FIELDS]
        field_names.append("geoid")


        ################################################################

        # The SF1 directory consists of 47 or 48 segments. The first columns are defined below:
        # Summary levels at https://factfinder.census.gov/help/en/summary_level_code_list.htm
        SUMLEV_COUNTY = '050' # State-County
        SUMLEV_TRACT  = '140' # State-County-Census Tract
        SUMLEV_BLOCK  = '101' # State-County-County Sub∀division-Place/Remainder-Census Tract∀-Block Group-Block

        geo_fields = {a[1]:a[0] for a in enumerate(layouts['GEO_HEADER_SF1.xsd'])}
        assert geo_fields['FILEID']==0 and geo_fields['STUSAB']==1

        GEO_SUMLEV_FIELD  = geo_fields['SUMLEV']
        GEO_LOGRECNO_FIELD= geo_fields['LOGRECNO']
        GEO_STATE_FIELD   = geo_fields['STATE']
        GEO_COUNTY_FIELD  = geo_fields['COUNTY']
        GEO_TRACT_FIELD  = geo_fields['TRACT']
        GEO_BLOCK_FIELD  = geo_fields['BLOCK']

        # Track the logical record for the join (we really don't need the whole field)
        logrecs = {}
        from collections import defaultdict
        counts = defaultdict(int)

        with dopen(geo_filename,"r") as f:
            ct = 0
            for line in f:
                geo_data = line.split(",")
                sumlev   = geo_data[GEO_SUMLEV_FIELD]
                if sumlev in [SUMLEV_COUNTY,SUMLEV_TRACT,SUMLEV_BLOCK]:
                    logrecno = geo_data[GEO_LOGRECNO_FIELD]
                    county   = geo_data[GEO_COUNTY_FIELD]
                    geoid  = "".join(geo_data[f] for f in
                                       [GEO_STATE_FIELD,GEO_COUNTY_FIELD,GEO_TRACT_FIELD,GEO_BLOCK_FIELD])
                    logrecs[logrecno] = (sumlev, ct, county,
                                         [geo_data[ geo_fields[ field ] ] for field in JOIN_FIELDS],
                                         geoid.strip() )
                    counts[sumlev] += 1
                    ct += 1
                    if ct%1000==0:
                        logging.info("ct=%d",ct)


        print("{} geography file processed. counties:{}  tracts:{}  blocks:{}  mem:{:,}".format(
            stusab, counts[SUMLEV_COUNTY], counts[SUMLEV_TRACT], counts[SUMLEV_BLOCK],
            total_size(logrecs)))

        # Open the geofile and find all of the LOGRECNOs for the
        # summary summary levels we care about We are splitting on the
        # entire line, which is a waste. But most of the data we are
        # collecting is at the block level, so most of the time we
        # aren't wasting the work


        # Open the block, tract, and county files for every county
        # and write the first line
        output_files = {}
        state_code = dbrecon.state_fips(stusab)
        for county_code in dbrecon.counties_for_state(stusab):
            countydir = dbrecon.STATE_COUNTY_DIR(stusab=stusab, county=county_code)

            # If we are not forcing, then for each file see if it already exists. If it exists, write to /dev/null
            def special_open(path):
                if args.force or not dpath_exists(path):
                    print("creating ",path)
                    return dopen( path, 'w', fsync=False)
                return open('/dev/null', 'w')


            output_files[county_code] = {
                SUMLEV_COUNTY: special_open( dbrecon.SF1_COUNTY_DATA_FILE(stusab=stusab, county=county_code)) ,
                SUMLEV_BLOCK:  special_open( dbrecon.SF1_BLOCK_DATA_FILE(stusab=stusab, county=county_code)) ,
                SUMLEV_TRACT:  special_open( dbrecon.SF1_TRACT_DATA_FILE(stusab=stusab, county=county_code)) }
            for f in output_files[county_code].values():
                f.write(",".join(field_names))
                f.write("\n")
        if fsync:
            s2_session = boto3.Session()
        def assert_paths_exists():
            print(f"Starting Assert Path | stusab: {stusab}")
            for county_dir in output_files.values():
                for sumlev in county_dir.values():
                    if isinstance(sumlev, s3.S3File) or isinstance(sumlev, s3.s3open):
                        print(f"Closing Path : {sumlev.path} ")
                        sumlev.close()
                        wait_bucket, wait_key = s3.get_bucket_key(sumlev.path)
                        dbrecon.dwait_exists_boto3(wait_bucket, wait_key, s2_session)

        import operator
        count = 0
        while True:
            count += 1
            try:
                fields = open_segments[0].getfields(logrecno=ANY)
            except ReaderException as e:
                if fsync:
                    assert_paths_exists()
                #     output_files[f].close()
                print(f"Closing stusab: {stusab}")
                break;
            logrecno = fields[4]
            for osr in open_segments[1:]:
                fields.extend(osr.getfields(logrecno=logrecno))
            if fields[0]=='':   # end of file!
                if fsync:
                    assert_paths_exists()
                logging.debug(f"End of File | stusab: {stusab}")
                break

            if logrecno in logrecs:
                (sumlev,ct,county,joinfields,geoid) = logrecs[logrecno]

                fields.insert(0,str(ct)) # insert the pandasID
                fields.extend(joinfields) # add the fields that we join
                fields.append(geoid)

                # Check the SF1ST fields. Everyplace we have a SF1ST should be a FILEID
                errors = 0
                for i in range(len(fields)):
                    if fields[i]=='SF1ST' and not field_names[i].startswith('FILEID'):
                        print(f"fields[{i}]={fields[i]}  (should be FILEID field)")
                        errors += 1
                if len(fields) != len(field_names):
                    print(f"Expected {len(field_names)} fields, got {len(fields)}")
                    errors += 1
                if errors>0:
                    for i in range(len(fields)):
                        print(i,field_names[i],fields[i])
                    raise RuntimeError(f"errors: {errors}")

                outline = ",".join(fields) + "\n"
                output_files[county][sumlev].write(outline)
            if count%1000==0:
                logging.info("count=%d",count)

def validate_county(row):
    global validate_s2_session
    (stusab,county) = row
    stusab=stusab.lower()
    ret = []
    for fn in [dbrecon.SF1_COUNTY_DATA_FILE(stusab=stusab, county=county),
               dbrecon.SF1_BLOCK_DATA_FILE(stusab=stusab, county=county),
               dbrecon.SF1_TRACT_DATA_FILE(stusab=stusab, county=county)]:
        if not dbrecon.dpath_exists_boto3(fn, validate_s2_session):
            ret.append(fn)
    return ret

def validate(auth, j1, stusab):
    """Verify that all of the files exist for the named states"""
    global validate_s2_session
    stusab_set = ",".join([f'"{stusab}"' for stusab in stusabs])
    counties = DBMySQL.csfr(auth,
                          f"""SELECT stusab,county
                          FROM {REIDENT}geo
                          WHERE sumlev='050' AND pop100>0 AND stusab IN ({stusab_set})
                          """)
    print("validating %s counties" % len(counties))
    validate_s2_session = boto3.Session()
    with multiprocessing.Pool(j1) as p:
        res = p.map(validate_county, counties)
    res = [row for row in res if row!=[]]
    missing_states = set()
    state_re = re.compile("/work/(..)/")
    if len(res):
        print("Missing files:")
        for row in res:
            for fn in row:
                print(fn)
                m = state_re.search(fn)
                missing_states.add(m.group(1)) # better not fail
    return missing_states


def process_method(j1, fsync, stusabs):
    if j1 == 1:
        [process_stusab(stusab, fsync) for stusab in stusabs]
    else:
        with multiprocessing.Pool(args.j1) as p:
            p.starmap(process_stusab, zip(stusabs, repeat(args.fsync)))


def validate_method(auth, j1, stusabs, fsync, retry_attempts):
    missing_states = validate(auth, j1, stusabs)
    if missing_states:
        print("Rerun for these states: " + " ".join(missing_states))
        #if fsync:
        #    exit(1)
        if retry_attempts != 0:
            retry_loop(j1, missing_states, fsync, retry_attempts)
        else:
            print("Rerun for these states: " + " ".join(missing_states))
    else:
        print("step 2 Validation successful")
        exit(0)


def retry_loop(j1, stusabs, fsync, retry_attempts):
    times_run = 0
    missing_states = stusabs
    while retry_attempts != 0 and times_run < int(retry_attempts) and missing_states:
        process_method(j1, fsync, missing_states)
        times_run += 1
        missing_states = validate(auth, j1, stusabs)
    if missing_states:
        print("Rerun for these states: " + " ".join(missing_states))
        exit(1)
    else:
        print("step 2 Validation successful")
        exit(0)

if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Create per-county county, block and tract count files from the state-level SF1 files." )
    parser.add_argument("stusabs",nargs="*",help='Specify stusabs to process (or type all for all)')
    parser.add_argument("--all",action='store_true',help='All stusabs')
    parser.add_argument("--j1", type=int,
                        help='Number of stusabs to run at once (defaults to thread count in config file).')
    parser.add_argument("--just_validate", action='store_true', help='Just validate the files')
    parser.add_argument("--force", action='store_true', help="Delete every output file before creating it. Otherwise leave files if they already exist.")
    parser.add_argument("--fsync", action='store_true',
                        help="Attempt file sync will add assertion to check file exists before moving on.")
    parser.add_argument("-rt", "--retry_attempts", default=0, help="Number of times to rerun program if it finds missing files")
    parser.add_argument("--debug",
                        help="Run in debug mode. Do not update database and write output to file specified by --output",
                        action='store_true')
    dbrecon.argparse_add_logging(parser)
    args     = parser.parse_args()
    config   = dbrecon.setup_logging_and_get_config(args=args, prefix="02bld")
    logfname = logging.getLogger().handlers[0].baseFilename
    auth     = dbrecon.auth()

    if not dbrecon.dpath_exists(f"$SRC/layouts/layouts.json"):
        raise FileNotFoundError("Cannot find $SRC/layouts/layouts.json")

    if not dbrecon.dpath_exists(f"$SRC/layouts/DATA_FIELD_DESCRIPTORS_classified.csv"):
        raise FileNotFoundError("$SRC/layouts/DATA_FIELD_DESCRIPTORS_classified.csv")

    stusabs = []
    if (args.all) or (args.stusabs == ['all']):
        stusabs = dbrecon.all_stusabs()
    else:
        stusabs = [dbrecon.stusab(st).lower() for st in args.stusabs]

    if not stusabs:
        print("Specify stusabs to process or --all")
        exit(1)

    if not args.j1:
        args.j1=config['run'].getint('threads',1)

    logging.info("Running with {} threads".format(args.j1))

    if not args.just_validate:
        process_method(args.j1, args.fsync, stusabs)
    validate_method(auth, args.j1, stusabs, args.fsync, args.retry_attempts)
