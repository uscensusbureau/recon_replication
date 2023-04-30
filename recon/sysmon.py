#!/usr/bin/env python3
#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)
#
"""
sysmon.py:

Report load and free memory every 5 minutes. Part of the replication archive
for The U.S. Census Bureau's Ex Post Confidentiality Analysis of the 2010
Census Data Publications (https://github.com/uscensusbureau/recon_replication)
"""

import datetime
import os
import os.path
import psutil
import sys
import time

import dbrecon
import scheduler

sys.path.append( os.path.join(os.path.dirname(__file__),".."))

if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Report the load every 5 minutes." )
    parser.add_argument("--noisy", action='store_true')
    dbrecon.argparse_add_logging(parser)
    args   = parser.parse_args()
    config = dbrecon.get_config(filename=args.config)
    auth   = dbrecon.auth()

    while True:
        scheduler.report_load_memory(auth,quiet=not args.noisy)
        time.sleep(600)
