# -*- coding: utf-8 -*- 
"""
Creates county-level files from the national input rHDF file

Args: 
    1 (str) - prefix of rhdf to process

Example execution: 
    $ setsid python prep_rhdf.py r00

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
#prfx = str(sys.argv[1])
# dir of program
myRoot = os.path.dirname(os.path.abspath(__file__))
# param dictionary
params = json.load(open(myRoot+'/../../common/config.json','r'))

# date time stamp to be put in outfile names
date = repr(os.popen('date +"%m-%d-%y"').read() +
    os.popen('date +"%T"').read()).replace(r"\n"," ").replace("'","").strip().replace(" ","_")

def main(prfx):
    """Main body of prep program.
    """
    
    # Open log file
    sys.stderr = open(myRoot+'/prep_rhdf_{0}_{1}.error'.format(prfx,date),'w')
    logging.info('\n\n###### BEGINNING OF PREP RHDF PROGRAM ######\n\n')
    logging.info('date time: {0}'.format(date))
    logging.info('prfx: {0}'.format(prfx))
    
    prfxDir = params['rhdfbasedir']+'/'+prfx+'/'
    logging.info('file to read: {0}'.format(prfxDir+prfx+'.csv'))
    inDF = pd.read_csv(prfxDir+prfx+'.csv',dtype=str)
    inDF['sex'] = [1 if x == 'male' else 0 for x in inDF['sex']]
    for r in params['raceethvars']:
        inDF[r] = [1 if x == 'Y' else 0 for x in inDF[r]]
        inDF[r] = inDF[r].astype(int)
    inDF['geoid_block'] = inDF.geoid_block.astype(str)
    inDF['county'] = inDF.geoid_block.str.slice(start=0,stop=5)
    counties = list(set(inDF.county))
    varsToKeep = ['geoid_block','sex','age']+params['raceethvars']
    inDF = inDF.sort_values(by=varsToKeep)
    
    for c in counties:
        cDF = inDF[inDF.county==c][varsToKeep]
        cDF.to_csv(prfxDir+prfx+c+'.csv',index=False)
    
    logging.info('\n\n###### END OF PREP RHDF PROGRAM ######\n\n')

if __name__ == '__main__':
    import sys
    main(sys.argv[1:])
