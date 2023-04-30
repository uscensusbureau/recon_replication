# -*- coding: utf-8 -*- 
#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)
"""
Cleans up legacy commercial ``mega'' data files 

Args: 
    None

Example execution: 
    $ setsid python prep_cmrcl.py

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
    sys.stderr = open(myRoot+'/prep_cmrcl_{0}.error'.format(date),'w')
    logging.basicConfig(filename=
        myRoot+'/prep_cmrcl_{0}.log'.format(date),
        level=logging.DEBUG)
    logging.info('date time: {0}'.format(date))
    
    for path in glob.iglob(params['legacycmrcldir']+'/mega*.dta'):
        county = path[-9:-4]
        inDF=pd.read_stata(path)
        inDF.loc[inDF.dob=='00000000', 'dob']=''
        inDF.loc[inDF.dob.str.len()==6, 'dob']=inDF.dob+'02'
        inDF.loc[inDF.dob.str.len()==4, 'dob']=inDF.dob+'04'+'02'
        inDF['sex']=inDF.sex.str.upper()
        inDF.loc[(inDF.sex!='M') & (inDF.sex!='F'),'sex']=''
        inDF['dob']=pd.to_datetime(inDF.dob,format='%Y%m%d',errors='coerce')
        inDF['yob']=inDF.dob.map(lambda x: x.year)
        inDF['mob']=inDF.dob.map(lambda x: x.month)
        inDF['dayob']=inDF.dob.map(lambda x: x.day)
        inDF['tuple']=list(zip(inDF.mob.values,inDF.dayob.values))
        inDF['age']=2010-inDF.yob -((4,1)<inDF.tuple)
        #inDF=inDF[(inDF.tract!='')&(inDF.block!='')&(inDF.pik!='')&(inDF.sex!='')&(inDF.age.isnull()==False)]
        inDF['pik'] = inDF.pik.astype(str)
        #inDF['age'] = inDF.age.astype(int)
        inDF['county'] = county
        inDF['county'] = inDF.county.astype(str)
        inDF['geoid_block'] = inDF.county + inDF.tract + inDF.block
        inDF['sexhold'] = inDF.sex
        inDF['sex'] = [1 if x == 'M' else 0 for x in inDF['sexhold']]
        inDF.loc[inDF.sexhold=='','sex'] = np.nan
        inDF['r'] = np.random.uniform(0,1,len(inDF))
        varsToKeep = ['geoid_block','pik','sex','age','r']
        inDF = inDF[varsToKeep]
        inDF = inDF.sort_values(by=varsToKeep)
        inDF.to_csv(params['cmrcldir']+'cmrcl'+county+'.csv',index=False)
    
if __name__ == '__main__':
    main()
