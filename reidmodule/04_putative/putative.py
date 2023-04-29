# -*- coding: utf-8 -*- 
"""
Module performs record-by-record match between reconstructed 2010 decennial data
and the confidential match of commercial data and CEF. Threaded to use many workers
to (1) break data into tract-level chunks and (2) perform the match.  

Args: 
    1 (int) - number of workers
    2 (str) - name of left dataset (e.g. r00)
    3 (str) - name of right dataset (cmrcl, cef)  

Example execution: 
    $ setsid python putative.py 55 r00 cmrcl

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
from splitter import Splitter


# date time stamp to be put in outfile names
date = repr(os.popen('date +"%m-%d-%y"').read() +
    os.popen('date +"%T"').read()).replace(r"\n"," ").replace("'","").strip().replace(" ","_")

def main(numWorkers,leftarg,rightarg,putativematchestodo):
    """Main body of match program.
    """
    numWorkers = int(numWorkers)
    # Convert param string to list, add to dict
    putativematchestodo = putativematchestodo.strip('[]').replace("'","").split(',')
    params['putativematchestodo'] = putativematchestodo
    
    # Open log file
    sys.stderr = open(myRoot+'/putative_'+leftarg+'_'+rightarg+'_{0}.error'.format(date),'a')
    logging.info('\n\n###### BEGINNING OF PUTATIVE PROGRAM ######\n\n')
    logging.info('date time: {0}'.format(date))
    logging.info('number of workers: {0}'.format(numWorkers))
    logging.info('left is: {0}'.format(leftarg))
    logging.info('right is: {0}'.format(rightarg))
    logging.info('matches: {0}'.format(params['putativematchestodo']))
    
    # Create output file into which match stats will be dumped 
    if 'binage' in params['putativematchestodo']:
        statsOutFileName='putativebinage_'+leftarg+'_'+rightarg+'.csv'
    else:
        statsOutFileName='putative_'+leftarg+'_'+rightarg+'.csv'
    # start block stats outfile
    statsFileHeader = ['county','tract','block',leftarg,rightarg,'exact','fzyage','binage']
    if leftarg == 'cef' or leftarg == 'hdf':
        params['outDir'] = params['rsltbase']+leftarg+'/'
        params['plusOutDir'] = params[leftarg+'dir']+'/'
    elif leftarg in ['modalGsr','propGsr']:
        params['outDir'] = params['rsltbase']+'simul/'
        params['plusOutDir'] = params['simuldir']+'/'
    else:
        params['outDir'] = params['rhdfbasersltdir']+leftarg+'/'
        params['plusOutDir'] = params['rhdfbasedir']+leftarg+'/'
        
    startOutFile(statsOutFileName, statsFileHeader, params['outDir'])
    
    # start matched outfile 
    
    matchFileHeader = ['county','tract','block','sex','age','white','black','aian','asian','nhopi','sor','hisp','pik','put_matchflag']
    if 'binage' in params['putativematchestodo']:
        matchOutFileName='putativematchbinage_'+leftarg+'_'+rightarg+'.csv'
    else:
        matchOutFileName='putativematch_'+leftarg+'_'+rightarg+'.csv'

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
    if rightarg not in ['cmrcl','cef']:
        logging.info('EXIT: not a valid right')
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
    matcher = Matcher(lock,params,leftarg,rightarg)
    matcher.matchType = 'putative'

    logging.info('start time is: {0}'.format(matcher.t0))
    # initialize reporter
    #stopReport = threading.Event()
    #reporter(matcher,stopReport)
    
    # Initialize queue, loading with counties for comparison
    matcher.fillCountyQueue(countiesToDo,matcher.countyQueue)
    logging.info('starting county queue size: {0}'.format(matcher.countyQueue.qsize()))
        
    logging.info('\n\n###### LOAD PUTATIVE QUEUE ######\n\n')
        
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
        
    logging.info('\n\n###### STARTING PUTATIVE MATCH ######\n\n')
    logging.info('workQueue size: {0}'.format(matcher.workQueue.qsize()))
    cols2Match = ['sex','age']
    # Create and kick off workers
    processes = []
    for w in range(numWorkers):
        p = mp.Process(target=matcher.matchWorker, args=(matcher.workQueue,matcher.putTractsPrcd,'atch',cols2Match,))
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

    logging.info('\n\n###### SPLIT RHDFPIK ######\n\n')
    matcher.logTime()
    # split rhdfpik into county files
    rhdfpik = pd.read_csv(params['outDir']+'/'+matchOutFileName,
                            converters={'county': lambda x: str(x),
                            'tract': lambda x: str(x), 
                            'block': lambda x: str(x),
                            'pik': lambda x: str(x)})
    rhdfpikCounties = list(set(rhdfpik.county))
    
    splitter = Splitter(params,rhdfpik,leftarg,rightarg)
    
    splitter.fillLoadQueue(rhdfpikCounties,splitter.readCountyQueue)
    logging.info('starting splitter.readCountyQueue.qsize(): {0}'.format( splitter.readCountyQueue.qsize()))
    
    # Create and kick off workers
    splitProcesses = []
    for w in range(int(numWorkers/2)):
        p = mp.Process(target=splitter.readWorker)
        p.start()
        splitProcesses.append(p)
        time.sleep(1)
    
    stop = threading.Event()
    
    logging.debug('kicked off splitProcesses')
    
    for w in range(int(numWorkers/2)):
        writeWorker(splitter,stop)
    
    for p in splitProcesses:
        p.join()
        logging.info('Splitter process: {0} finished and joined'.format(p))
    
    # Let dumpers finish, then quit them 
    while splitter.writeQueue.qsize()>0:
        time.sleep(15)
    
    stop.set()

    matcher.logTime()
    logging.info('end work queue size: {0}'.format(matcher.workQueue.qsize()))
    logging.info('end stat queue size: {0}'.format(matcher.doneStatQueue.qsize()))
    logging.info('end match queue size: {0}'.format(matcher.doneMatchQueue.qsize()))
    logging.info('\n\n###### END OF PUTATIVE PROGRAM ######\n\n')

if __name__ == '__main__':
    import sys
    main(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4])

