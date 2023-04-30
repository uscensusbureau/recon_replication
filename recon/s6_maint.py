#!/usr/bin/env python3
#
#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)
"""
s6_main.py:
Various maintenance functions. Part of the replication archive for The U.S.
Census Bureau's Ex Post Confidentiality Analysis of the 2010 Census Data
Publications (https://github.com/uscensusbureau/recon_replication)
"""

import glob
import logging
import os
import os.path
import subprocess
import sys
import time
import atexit
import re

import dbrecon
from dbrecon import GB,MB,DBMySQL
from dbrecon import dopen,dmakedirs,dsystem,dpath_exists,GB,REIDENT

from gurobi_logfile_parser import GurobiLogfileParser

MFRE=re.compile("model_(\d\d)(\d\d\d)(\d\d\d\d\d\d)[.]log")
def model_filename_to_sct(fname):
    m = MFRE.search(fname)
    (state,county,tract) = m.group(1,2,3)
    return {'state':dbrecon.stusab(state), 'county':county, 'tract':tract}


def glog_scan_root(rootdir):
    for root, dirs, files in os.walk(rootdir):
        print(root)
        for fname in files:
            if fname.startswith("model") and fname.endswith(".log"):
                extra = model_filename_to_sct(fname)
                glog = GurobiLogfileParser(os.path.join(root,fname))
                try:
                    (cmd, vals) = glog.sql_insert(name='glog', dialect='mysql', extra=extra)
                except KeyError as e:
                    print("key error. glog:",glog.dict,"extra=",extra)
                    print(fname)
                    print(e)
                    exit(1)
                DBMySQL.csfr(dbrecon.auth(),cmd=cmd, vals=vals, quiet=True)
                print(fname)

def final_pop_scan_sct(sct):
    (stusab,county,tract) = sct
    try:
        final_pop = dbrecon.get_final_pop_from_sol(stusab, county, tract)
    except FileNotFoundError:
        logging.warning(f"UNSOLVE {stusab} {county} {tract} has no solution. Removing")
        DBMySQL.csfr(dbrecon.auth(), f"UPDATE {REIDENT}tracts set sol_start=NULL, sol_end=NULL where stusab=%s and county=%s and tract=%s",(stusab,county,tract))
    else:
        print(f"{stusab} {county} {tract} = {final_pop}")
        DBMySQL.csfr(dbrecon.auth(), f"UPDATE {REIDENT}tracts set final_pop=%s where stusab=%s and county=%s and tract=%s",(final_pop,stusab,county,tract))

def final_pop_scan():
    """This should be parallelized"""
    rows = DBMySQL.csfr(dbrecon.auth(), f"SELECT stusab, county, tract from {REIDENT}tracts where final_pop is null")
    for row in rows:
        final_pop_scan_sct(row)

if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Analyze Gurobi logs. " )
    dbrecon.argparse_add_logging(parser)
    parser.add_argument("--schema",  help="drop the glog table and recreate it", action='store_true')
    parser.add_argument("--clear",  help="delete the records in the glog database table", action='store_true')
    parser.add_argument("--glog", help="Look for gurobi logfiles and import them into the glog database", action='store_true')
    parser.add_argument("roots", help="directories to scan for logfiles", nargs="*")
    parser.add_argument("--final_pop", help="Update final_pop in the tracts database for every tract that doesn't have final pop set", action='store_true')
    parser.add_argument("--validate", help="Show those tracts where final_pop != pop100", action='store_true')
    parser.add_argument("--rm", help="Remove the bad ones", action='store_true')
    parser.add_argument("--clean", help="Clear hostlock for this host if process is not live", action='store_true')

    args       = parser.parse_args()
    auth       = dbrecon.auth()
    config     = dbrecon.setup_logging_and_get_config(args=args,prefix="06analyze")

    if args.clear or args.schema:
        glog = GurobiLogfileParser("tests/model_04001944300.log")
        if args.schema:
            DBMySQL.csfr(auth,f"DROP TABLE IF EXISTS {REIDENT}glog")
            DBMySQL.csfr(auth,glog.sql_schema())
        if args.clear:
            DBMySQL.csfr(auth,f"DELETE from {REIDENT}glog")

    if args.final_pop:
        final_pop_scan()
        exit(0)

    if args.clean:
        dbrecon.db_clean(auth)
        exit(0)

    if args.validate:
        bad = DBMySQL.csfr(auth,(
            f"""
            SELECT t.stusab,t.county,t.tract,t.final_pop,g.pop100
            FROM {REIDENT}tracts t
            LEFT JOIN {REIDENT}geo g
            ON t.state=g.state AND t.county=g.county AND t.tract=g.tract
            WHERE g.sumlev=140 AND t.final_pop != g.pop100
            """))
        for (stusab,county,tract,final_pop,pop100) in bad:
            logging.warning(f"{stusab} {county} {tract} {final_pop} != {pop100}")
            if args.rm:
                logging.warning(f"SUNSOLVE {stusab} {county} {tract} {final_pop} != {pop100}")
                DBMySQL.csfr(auth,f"""
                UPDATE {REIDENT}tracts
                SET lp_start=NULL,lp_end=NULL,sol_start=NULL,sol_end=NULL,hostlock=NULL,final_pop=NULL
                where stusab=%s and county=%s and tract=%s
                """, (stusab,county,tract))
                dbrecon.dpath_unlink(dbrecon.LPFILENAMEGZ(stusab=stusab,county=county,tract=tract))
                dbrecon.dpath_unlink(dbrecon.SOLFILENAMEGZ(stusab=stusab,county=county,tract=tract))

    if args.glog:
        for root in args.roots:
            glog_scan_root(root)
