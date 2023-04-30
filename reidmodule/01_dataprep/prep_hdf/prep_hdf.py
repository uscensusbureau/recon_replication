# -*- coding: utf-8 -*- 
#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)

"""
Preps HDF data. 

Args: 

Example execution: 
    $ setsid python prep_hdf.py


Part of the replication archive for The U.S. Census Bureau's Ex Post
Confidentiality Analysis of the 2010 Census Data Publications
(https://github.com/uscensusbureau/recon_replication)
"""

import re
import csv
import time
import multiprocessing as mp
import os
import psutil
import sys
import logging
import pandas as pd
import numpy as np
import subprocess
import threading
import json
import glob


# MODULE LEVEL VARIABLES
# dir of program
myRoot = os.path.dirname(os.path.abspath(__file__))
# param dictionary
params = json.load(open(myRoot+'/../../common/config.json','r'))

# date time stamp to be put in outfile names
date = repr(os.popen('date +"%m-%d-%y"').read() +
    os.popen('date +"%T"').read()).replace(r"\n"," ").replace("'","").strip().replace(" ","_")

def main():
    """Main body of match program.
    """
    
    # Open log file
    sys.stderr = open(myRoot+'/prep_hdf_{0}.error'.format(date),'w')
    logging.basicConfig(filename=
        myRoot+'/prep_hdf_{0}.log'.format(date),
        level=logging.DEBUG)
    logging.info('date time: {0}'.format(date))
    
    start = 0
    for path in glob.iglob(params['legacyhdfdir']+'/hdf*.dta'):
        county = path[-9:-4]
        inDF=pd.read_stata(path)
        inDF['age'] = inDF.age.astype(int)
        inDF['county'] = county
        inDF['county'] = inDF.county.astype(str)
        inDF['geoid_block'] = inDF.county + inDF.tabtractce + inDF.tabblk
        inDF['sex'] = [1 if x == 'M' else 0 for x in inDF['sex']]
        varsToKeep = ['geoid_block','sex','age']+params['raceethvars']
        inDF = inDF[varsToKeep]
        for r in params['raceethvars']:
            inDF[r] = [1 if x == 'Y' else 0 for x in inDF[r]]
            inDF[r] = inDF[r].astype(int)
        inDF = inDF.sort_values(by=varsToKeep)
        inDF.to_csv(params['hdfdir']+'hdf'+county+'.csv',index=False)

if __name__ == '__main__':
    main()
