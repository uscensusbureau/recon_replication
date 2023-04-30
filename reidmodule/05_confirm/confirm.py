# -*- coding: utf-8 -*- 
#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)

"""
Module performs record-by-record match between reconstructed 2010 decennial data
and the confidential match of commercial data and CEF. Threaded to use many workers
to (1) break data into tract-level chunks and (2) perform the match.  

Args: 
    1 (int) - number of workers
    2 (str) - name of left dataset (e.g. r00plus)

Example execution: 
    $ setsid python confirm.py 55 r00cefplus

Part of the replication archive for The U.S. Census Bureau's Ex Post
Confidentiality Analysis of the 2010 Census Data Publications
(https://github.com/uscensusbureau/recon_replication)
"""

import re
import csv
import time
import multiprocessing as mp
from multiprocessing import Process
import os
import sys
import logging
import pandas as pd
import numpy as np
import subprocess
import threading
import json


# MODULE LEVEL VARIABLES
# number of worker threads
#numWorkers = int(sys.argv[1])
# left and right params
#leftarg = str.lower(str(sys.argv[2]))
#leftargprfx = leftarg.replace('plus','').replace('cef','').replace('cmrcl','')
#rightarg = 'cef'


# dir of program
myRoot = os.path.dirname(os.path.abspath(__file__))
# param dictionary
params = json.load(open(myRoot+'/../common/config.json','r'))
# import common py
sys.path.insert(0,myRoot+'/../common/')
from match import Matcher
from admin import *

# date time stamp to be put in outfile names
date = repr(os.popen('date +"%m-%d-%y"').read() +
    os.popen('date +"%T"').read()).replace(r"\n"," ").replace("'","").strip().replace(" ","_")

def main(numWorkers,leftarg,confirmmatchestodo):
    """Main body of match program.
    """
    numWorkers = int(numWorkers)
    # Convert param string to list, add to dict
    confirmmatchestodo = confirmmatchestodo.strip('[]').replace("'","").split(',')
    params['confirmmatchestodo'] = confirmmatchestodo
    rightarg = 'cef'

    # Open log file
    sys.stderr = open(myRoot+'/confirm_'+leftarg+'_'+rightarg+'_{0}.error'.format(date),'a')
    logging.info('\n\n###### BEGINNING OF CONFIRM PROGRAM ######\n\n')
    logging.info('date time: {0}'.format(date))
    logging.info('number of workers: {0}'.format(numWorkers))
    logging.info('left is: {0}'.format(leftarg))
    logging.info('right is: {0}'.format(rightarg))
    logging.info('matches: {0}'.format(params['confirmmatchestodo']))
    
    # Create output file into which match stats will be dumped
    if 'binage' in params['confirmmatchestodo']:
        statsOutFileName='confirmbinage_'+leftarg.replace('plus','').replace('binage','')+'_'+rightarg+'.csv'
    else:
        statsOutFileName='confirm_'+leftarg.replace('plus','').replace('binage','').replace('fzyage','')+'_'+rightarg+'.csv'
    # start block stats outfile
    statsFileHeader = ['county','tract','block',leftarg,'cef','exact',
                            'fzyage','binage']
    if leftarg[0:3] == 'cef' or leftarg[0:3] == 'hdf':
        leftargprfx = leftarg[0:3]
        params['outDir'] = params['rsltbase']+leftargprfx+'/'
    elif 'Gsr' in leftarg:
        params['outDir'] = params['rsltbase']+'simul/'
    else:
        leftargprfx = leftarg.replace('plus','').replace('cef','').replace('cmrcl','').replace('binage','').replace('fzyage','')
        params['outDir'] = params['rhdfbasersltdir']+leftargprfx+'/'
    
    startOutFile(statsOutFileName, statsFileHeader, params['outDir'])
    # start matched outfile 
    matchFileHeader = ['county','tract','block','sex','age','white','black',
            'aian','asian','nhopi','sor','hisp','pik','put_matchflag','cef_white',
            'cef_black','cef_aian','cef_asian','cef_nhopi','cef_sor','cef_hisp', 'conf_matchflag']
    if 'binage' in params['confirmmatchestodo']:
        matchOutFileName='confirmmatchbinage_'+leftarg.replace('plus','').replace('binage','').replace('fzyage','')+'_'+rightarg+'.csv'
    else:
        matchOutFileName='confirmmatch_'+leftarg.replace('plus','').replace('fzyage','')+'_'+rightarg+'.csv'
    
    startOutFile(matchOutFileName, matchFileHeader, params['outDir'])
    
    # Get sets of counties to compare
    countiesToDo = []
    with open(params['geolookup']+'allcounties.txt') as f:
        for line in f:
            c = line.replace('\n','')
            if c[0:2]!='72':
                countiesToDo.append(c)
    #countiesToDo = countiesToDo[0:10]
    #if len(countiesToDo)!=3143:
    #    logging.info('EXIT: not the right coutny count, {0}'.format(len(countiesToDo)))
    #    sys.exit()
    
    # log counts
    logging.info('counties to do count: {0}'.format(len(countiesToDo)))
    
    # create an instance of the match class, which creates and holds all of file references, and ques
    # using this class will help ensure that data can be shared across the  
    # different functions and modifiable by multiple processes where 
    # necessary
    lock = mp.Lock()
    matcher = Matcher(lock,params,leftarg,rightarg)
    matcher.matchType = 'confirm'

    logging.info('start time is: {0}'.format(matcher.t0))
    # initialize reporter
    #stopReport = threading.Event()
    #reporter(matcher,stopReport)
    
    # Initialize queue, loading with counties for comparison
    matcher.fillCountyQueue(countiesToDo,matcher.countyQueue)
    logging.info('starting county queue size: {0}'.format(matcher.countyQueue.qsize()))
        
    logging.info('\n\n###### LOAD CONFIRM QUEUE ######\n\n')
        
    # launch loaders to fill work queue
    loaders = []
    for w in range(numWorkers):
        p = mp.Process(target=matcher.loadDataWorker)
        p.start()
        loaders.append(p)
        time.sleep(1)
    for p in loaders:
        p.join()
        logging.info('Data load process: {0} finished and joined'.format(p))
    matcher.logTime()

    logging.info('starting work queue size: {0}'.format(matcher.workQueue.qsize()))

    # Start emptying doneQueues
    logging.info('\n\n###### STARTING DONEQ DUMP ######\n\n')
    stopDump = threading.Event()
    emptyDoneQueue(statsOutFileName,matcher.doneStatQueue,stopDump,params['outDir'])

    stopMatchDump = threading.Event()
    emptyDoneQueue(matchOutFileName, matcher.doneMatchQueue,stopMatchDump,
        params['outDir'])
        
    logging.info('\n\n###### STARTING CONFIRM MATCH ######\n\n')
    logging.info('workQueue size: {0}'.format(matcher.workQueue.qsize()))
    cols2Match = ['sex','age','pik'] + params['raceethvars']
    # Create and kick off workers
    processes = []
    for w in range(numWorkers):
        p = mp.Process(target=matcher.matchWorker, args=(matcher.workQueue,matcher.putTractsPrcd,'cnfrm',cols2Match,))
        p.start()
        processes.append(p)
        time.sleep(1)
    
    for p in processes:
        p.join()
        logging.info('Match process: {0} finished and joined'.format(p))
    matcher.logTime()
    
    # Give the sync and dump threads a chance to finish 
    while matcher.doneStatQueue.qsize()>0 or matcher.doneMatchQueue.qsize()>0:
        time.sleep(15)
        
    stopDump.set()
    stopMatchDump.set()

    matcher.logTime()
    logging.info('end work queue size: {0}'.format(matcher.workQueue.qsize()))
    logging.info('end stat queue size: {0}'.format(matcher.doneStatQueue.qsize()))
    logging.info('end match queue size: {0}'.format(matcher.doneMatchQueue.qsize()))
    logging.info('\n\n###### END OF CONFIRM PROGRAM ######\n\n')

if __name__ == '__main__':
    import sys
    main(sys.argv[1],sys.argv[2],sys.argv[3])

