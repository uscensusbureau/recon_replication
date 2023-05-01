#!/usr/bin/env python3
#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)
#
"""
00_download_data.py: Downloads all of the files from the Census WWW2 server.
This will not run inside the DAS environment because the WWW2 server is
blocked, but you could run it on your desktop under python and then scp the
files over.  Part of the replication archive for The U.S. Census Bureau's Ex
Post Confidentiality Analysis of the 2010 Census Data Publications
(https://github.com/uscensusbureau/recon_replication)
"""

import urllib
import csv
import sys
import os
import os.path
import dbrecon
import logging
import requests
import time
import zipfile
import glob

from dbrecon import dmakedirs
from dbrecon import dopen,dpath_exists,REIDENT



def download(stusab, sf1_dest_dir):
    config = dbrecon.GetConfig().get_config()
    stusab = stusab.lower()
    rec        = dbrecon.state_rec(stusab)
    print(rec)
    state_code = dbrecon.state_fips(stusab)

    ### check for file and if does not exist, download it
    url      = config['urls']['SF1_URL_TEMPLATE'].format(state_name=rec['state_name'], stusab=stusab)
    sf1_dist = dbrecon.dpath_expand("$SF1_DIST")
    filename = sf1_dist+"/"+os.path.basename(url)
    print(f"Downloading {url} -> {filename}")
    if dpath_exists(filename):
        print("  exists")
        return                  # downloaded

    bytes = 0
    t0 = time.time()
    r = requests.get(url,stream=True)

    with dopen(filename,"wb") as f:
        for chunk in r.iter_content(1024*1024):
            if bytes==0 and chunk[0:2]!=b'PK':
                print("ERROR: {} is not a zip file".format(url))
                os.unlink(filename)
                return
            f.write(chunk)
            bytes += len(chunk)
    t1 = time.time()
    print("   Downloaded {:,} bytes in {:.1f} seconds for {:.0f} Kbytes/sec".format(bytes,t1-t0, bytes/(t1-t0)/1000))



def validate(sf1_dist_dir):
    for zipfilename in glob.glob(os.path.join(sf1_dist_dir,"*.zip")):
        print(zipfilename,"...",end='')
        try:
            with zipfile.ZipFile(zipfilename) as zf:
                print("")
                continue
        except zipfile.BadZipfile as e:
            print("BadZipfile")
            continue


if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Data Migration Tool" )
    dbrecon.argparse_add_logging(parser)
    parser.add_argument("--all",action='store_true',help='All states')
    parser.add_argument("stusabs",nargs="*",help='Specify states to download on')
    parser.add_argument("--validate",action='store_true',
                        help='Validate the ZIP files and delete those that are incomplete')
    args = parser.parse_args()
    config     = dbrecon.setup_logging_and_get_config(args=args,prefix="00dwn")

    ## Make sure we have a directory for the state
    sf1_dist_dir = dbrecon.dpath_expand("$SF1_DIST")

    print(f"Downloading to {sf1_dist_dir}")
    dmakedirs(sf1_dist_dir)

    if args.validate:
        validate(sf1_dist_dir)
        exit(0)

    states = []
    if args.all:
        states = dbrecon.all_stusabs()
    else:
        states = args.stusabs

    if not states:
        print("Specify states to download or --all")
        exit(1)


    for state in states:
        download(state, sf1_dist_dir)
