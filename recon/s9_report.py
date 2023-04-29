#!/usr/bin/env /home/simsong/anaconda3/bin/python3
# -*- coding: utf-8 -*-

import re
import os
import sys
import datetime
import time

import dbrecon
import ctools.tydoc as tydoc
from dbrecon import DBMySQL

class Format:
    END = '\033[0m'
    UNDERLINE = '\033[4m'
    @classmethod
    def underline(self,s):
        return(self.UNDERLINE+s+self.END)

    @classmethod
    def demo(self):
        for i in range(0,64):
            print(f"\033[{i}m Mode {i}")
#
queries1 = [
    ('z','Current Time', 'SELECT now()'),
    ('a', 'tracts with population for which we have no LP file',
     'select * from {REIDENT}tracts_geostatus as a left join {REIDENT}geo_tracts as b on a.geocode=b.geocode where a.lp_end is null and b.pop100>0 limit 10'),
    ('','',
     'select * from {REIDENT}tracts_geostatus inner join {REIDENT}geo_tracts on tracts_geostatus.geocode=geo_tracts.geocode where tracts_geostatus.lp_end is null and geo_tracts.pop100>0 limit 100')
    ]


def fmt(r):
    if isinstance(r,int):
        return "{:,}".format(r)
    if isinstance(r,datetime.datetime):
        timestr = (r - datetime.timedelta(hours=4)).isoformat()
        timestr = timestr.replace("T"," ")
    return str(r)

if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Reports on the state of the reconstruction project.")
    parser.add_argument("--demo", action='store_true')
    dbrecon.argparse_add_logging(parser)
    args   = parser.parse_args()

    if args.demo:
        Format.demo()
        exit(0)

    config = dbrecon.setup_logging_and_get_config(args=args, prefix="s9report",error_alert=False)

    for (label,desc,query) in queries1:
        print(query)
        rows = DBMySQL.csfr(dbrecon.auth(), query)
        if not query.upper().startswith("SELECT"):
            continue
        if len(rows)==0:
            print(desc," No results")
        elif len(rows)==1:
            print(desc," ".join([str(val) for val in rows[0]]))
        else:
            print(Format.underline(desc))
            print("("+query+")")
            table = tydoc.tytable()
            for row in rows:
                table.add_data([fmt(r) for r in row])
            table.render(sys.stdout, format=tydoc.FORMAT_MARKDOWN)
            print("")
