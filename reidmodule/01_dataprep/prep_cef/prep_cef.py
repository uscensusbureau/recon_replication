# -*- coding: utf-8 -*- 
#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)
"""
Launches stata count programs. 

Args: 
    NA

Example execution: 
    $ setsid python prep_cef.py

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

# set the seed
np.random.seed(20211226)
    
def main():
    """Main body of match program.
    """
    
    # Open log file
    sys.stderr = open(myRoot+'/prep_cef_{0}.error'.format(date),'w')
    logging.basicConfig(filename=
        myRoot+'/prep_cef_{0}.log'.format(date),
        level=logging.DEBUG)
    logging.info('date time: {0}'.format(date))
    
    
    # Get sets of counties to separate
    countiesToDo = []
    with open(params['geolookup']+'allcounties.txt') as f:
        for line in f:
            c = line.replace('\n','')
            if c[0:2]!='72':
                countiesToDo.append(c)

    varsToKeep = ['geoid_block','pik','sex','age']+params['raceethvars'] + ['keep','r_keep','r']
    # get input, national cef file
    inDF = pd.read_csv(params['legacycefdir']+'/cef_20220921.csv',dtype=str,engine='python')
    inDF['county'] = inDF.geoid_block.str.slice(start=0,stop=5)
    # generate the random variables we need to select records (r_keep) and sort records (r)
    inDF['r_keep'] = np.random.uniform(0,1,len(inDF))
    inDF['r'] = np.random.uniform(0,1,len(inDF))
    # make sure NaN piks are zero length strings, which is necessary for the .transform() statement below
    inDF.loc[inDF.pik.isnull(),'pik'] = ''
    
    # now go by county, break into county-level csv files
    for c in countiesToDo:
        inDFPart = inDF.loc[inDF.county==c]
        # create keep as the first record by block-pik-r_keep
        inDFPart['keep'] = inDFPart.sort_values(['geoid_block','pik','r_keep']).groupby(['geoid_block','pik'])['r_keep'].transform(lambda x: x== x.iloc[0]).astype(int)
        # keep is zero if no pik
        inDFPart.loc[inDFPart.pik=='','keep'] = 0 
        inDFPart['keep'] = inDFPart['keep'].astype(int)
        # make sure the raceeth vars are ints
        for r in params['raceethvars']:
            inDFPart[r] = inDFPart[r].astype(int)
        inDFPart = inDFPart.sort_values(by=varsToKeep)
        inDFPart = inDFPart[varsToKeep]
        inDFPart.to_csv(params['cefdir']+'cef'+c+'.csv',index=False)
    
    logging.info('\n\n###### END OF PREP CEF PROGRAM ######\n\n')
    
if __name__ == '__main__':
    main()
