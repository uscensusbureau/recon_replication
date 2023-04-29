#!/usr/bin/env python3

from subprocess import check_call,call
import sys
import hashlib
import os
import os.path
from os.path import abspath,dirname,basename

MY_DIR     = dirname(abspath(__file__))
PARENT_DIR = dirname(MY_DIR)
if PARENT_DIR not in sys.path:
    sys.path.append(PARENT_DIR)

# this shouldn't be necessary
#pylint: disable=E0401
import dbrecon

TEST_CONFIG_FILE="config_test.ini"

# We test by remaking the smallest of all the LP files

SMALL_STUSAB='mo'
SMALL_STATE='29'
SMALL_COUNTY='183'
SMALL_TRACT='980000'



def test_build_tract_lp():
    return

if __name__=="__main__":
    #call([sys.executable,'s3_pandas_synth_lp_files.py','--debug','--output','/tmp/model_29183980000.lp.gz',SMALL_STUSAB,SMALL_COUNTY,SMALL_TRACT,'--stdout'])
    # Now compare the two files
    f1 = dbrecon.dopen("/tmp/model_29183980000.lp.gz","r")
    f2 = dbrecon.dopen("tests/model_29183980000.lp.gz","r")
    while True:
        line1 = f1.readline()
        line2 = f2.readline()
        if line1!=line2:
            print("line1 != line2")
        if line1=="":
            break
    print("{} and {} are the same".format(f1.name,f2.name))
