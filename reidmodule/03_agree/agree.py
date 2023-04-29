# -*- coding: utf-8 -*- 
"""
Module performs record-by-record match between reconstructed 2010 decennial data
and the confidential 2010 Census data, HDF and CEF. Threaded to use many workers
to (1) break data into tract-level chunks and (2) perform the match.  

Args: 
    1 (int) - number of workers
    2 (str) - name of left dataset (hdf or cef)
    3 (str) - name of right dataset (r00) 

Example execution: 
    $ setsid python agree.py 55 hdf r00

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
import pathlib


# MODULE LEVEL VARIABLES
# number of worker threads
#numWorkers = int(sys.argv[1])
# left and right params
#leftarg = str.lower(str(sys.argv[2]))
#rightarg = str.lower(str(sys.argv[3]))

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

def main(numWorkers,leftarg,rightarg,validatematchestodo):
    """Main body of match program.
    """
    numWorkers = int(numWorkers)
    # Convert param string to list, add to dict
    validatematchestodo = validatematchestodo.strip('[]').replace("'","").split(',')
    params['validatematchestodo'] = validatematchestodo
    
    # Open log file
    if 'binage' in validatematchestodo:
        sys.stderr = open(myRoot+'/agreebinage_'+leftarg+'_'+rightarg+'_{0}.error'.format(date),'a')
    else: 
        sys.stderr = open(myRoot+'/agree_'+leftarg+'_'+rightarg+'_{0}.error'.format(date),'a')
        
    logging.info('\n\n###### BEGINNING OF AGREE PROGRAM ######\n\n')
    logging.info('date time: {0}'.format(date))
    logging.info('number of workers: {0}'.format(numWorkers))
    logging.info('left is: {0}'.format(leftarg))
    logging.info('right is: {0}'.format(rightarg))
    logging.info('matches: {0}'.format(params['validatematchestodo']))
    
    # Create output file into which match stats will be dumped
    if 'binage' in params['validatematchestodo']:
        statsOutFileName='agreebinage_'+leftarg+'_'+rightarg+'.csv'
    else:
        statsOutFileName='agree_'+leftarg+'_'+rightarg+'.csv'
    # start block stats outfile
    outFileHeader = ['county','tract','block',leftarg,rightarg,'exact','fzyage','binage','oneoff','jsresid']
    # in a typical agree match rightarg is prfx (e.g. r00)
    # if not, we need to put the results of an hdf-cef or cef-hdf agree match somewhere else 
    if rightarg == 'cef' or rightarg == 'hdf':
        params['outDir'] = params['rsltbase']+rightarg+'/'
        startOutFile(statsOutFileName, outFileHeader, params['outDir'])
    else:
        params['outDir'] = params['rhdfbasersltdir']+rightarg+'/'
        startOutFile(statsOutFileName, outFileHeader, params['outDir'])
    # start matched outfile 
    matchFileHeader = ['county','tract','block',
            'sex',
            'age',
            'white',
            'black',
            'aian',
            'asian',
            'nhopi',
            'sor',
            'hisp',
            rightarg[0:3]+'_sex',
            rightarg[0:3]+'_age',
            rightarg[0:3]+'_white',
            rightarg[0:3]+'_black',
            rightarg[0:3]+'_aian',
            rightarg[0:3]+'_asian',
            rightarg[0:3]+'_nhopi',
            rightarg[0:3]+'_sor',
            rightarg[0:3]+'_hisp',
            'val_matchflag']
            
    if 'binage' in params['validatematchestodo']:
        matchOutFileName='agreematchbinage_'+leftarg+'_'+rightarg+'.csv'
    else:
        matchOutFileName='agreematch_'+leftarg+'_'+rightarg+'.csv'
    
    # in a typical agree match rightarg is prfx (e.g. r00)
    # if not, we need to put the results of an hdf-cef or cef-hdf agree match somewhere else
    startOutFile(matchOutFileName, matchFileHeader, params['outDir'])
    
    # Get sets of counties to compare
    countiesToDo = []
    with open(params['geolookup']+'allcounties.txt') as f:
        for line in f:
            c = line.replace('\n','')
            if c[0:2]!='72':
                countiesToDo.append(c)
    #countiesToDo = countiesToDo[0:10]
    # validLeft list is valid left datasets in the putative/conf, which are right in validation match
    if leftarg not in ['cef','hdf']:
        logging.info('EXIT: not a valid left')
        sys.exit()

    #if len(countiesToDo)!=3143:
    #    logging.info('EXIT: not the right county count, {0}'.format(len(countiesToDo)))
    #    sys.exit()
    
    # log counts
    logging.info('counties to do count: {0}'.format(len(countiesToDo)))
    
    # create an instance of the match class, which creates and holds all of file references, and ques
    # using this class will help ensure that data can be shared across the 
    # different functions and modifiable by multiple processes where 
    # necessary
    lock = mp.Lock()
    matcher = Matcher(lock, params,leftarg,rightarg)
    matcher.matchType = 'agree'

    logging.info('start time is: {0}'.format(matcher.t0))
    # initialize reporter
    #stopReport = threading.Event()
    #reporter(matcher,stopReport)
    
    # Initialize queue, loading with counties for comparison
    matcher.fillCountyQueue(countiesToDo,matcher.countyQueue)
    logging.info('starting county queue size: {0}'.format(matcher.countyQueue.qsize()))
        
    logging.info('\n\n###### LOAD AGREE QUEUE ######\n\n')
        
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
        
    logging.info('\n\n###### STARTING AGREE MATCH ######\n\n')
    logging.info('workQueue size: {0}'.format(matcher.workQueue.qsize()))
    cols2Match = ['sex','age']+params['raceethvars']
    # Create and kick off workers
    processes = []
    for w in range(numWorkers):
        p = mp.Process(target=matcher.matchWorker, args=(matcher.workQueue,matcher.valTractsPrcd,'val',cols2Match,))
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
    logging.info('\n\n###### END OF AGREE PROGRAM ######\n\n')


if __name__ == '__main__':
    import sys
    main(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4])
