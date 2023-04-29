# -*- coding: utf-8 -*- 
"""
Module 

Args: 
    1 (int) - number of workers

    Example execution: 
    $ setsid python tabs.py 20 cef

"""

import re
import csv
import time
import multiprocessing as mp
import os
import glob
import psutil
import sys
import logging
import pandas as pd
import numpy as np
import subprocess
import threading
import json
import pathlib
import argparse
import itertools
import math

# MODULE LEVEL VARIABLES
# number of worker threads
#numWorkers = int(sys.argv[1])
# dir of program
myRoot = os.path.dirname(os.path.abspath(__file__))
# param dictionary
params = json.load(open(myRoot+'/../common/config.json','r'))
# import common py
sys.path.insert(0,myRoot+'/../common/')
from tabber import Tabber

# date time stamp to be put in outfile names
date = repr(os.popen('date +"%m-%d-%y"').read() +
    os.popen('date +"%T"').read()).replace(r"\n"," ").replace("'","").strip().replace(" ","_")

def main(numWorkers,src):
    """Main body of tab program.
    """
    numWorkers = int(numWorkers)
    # Open log file
    sys.stderr = open(myRoot+'/tabs_{0}.error'.format(date),'a')
    logging.info('\n\n###### BEGINNING OF TABS PROGRAM ######\n\n')
    logging.info('date time: {0}'.format(date))
    logging.info('number of workers: {0}'.format(numWorkers))
    
    # initialize tabber
    lock = mp.Lock()
    tabber = Tabber(lock, params)

    # fill queue
    with open(params['geolookup']+'allcounties.txt') as f:
        for line in f:
            county = line.replace('\n','')
            if county[0:2] != '72':
                tabber.tabQueue.put((county,src))
    
    # create and kick off workers
    processes = []
    for w in range(numWorkers):
        p = mp.Process(target=tabber.tabWorker)
        p.start()
        processes.append(p)
        time.sleep(1)
    
    for p in processes:
        p.join()
        logging.info('Tab process: {0} finished and joined'.format(p))
        tabber.logTime()
    
    # stitch together county-level tabs
    counter = 1
    if src in ['cef','hdf','cmrcl']:
        outFile = params['rsltbase']+src+'/'+src+'_block_counts.csv'
    else:
        outFile = params['rhdfbasersltdir']+'/'+src+'/'+src+'_block_counts.csv'
    try:
        os.remove(outFile)
    except:
        pass

    with open(params['geolookup']+'allcounties.txt') as f:
        for line in f:
            c = line.replace('\n','')
            if c[0:2] != '72':
                if src in ['cef','hdf','cmrcl']:
                    inFile = params['rsltbase']+src+'/'+src+'_block_counts_'+c+'.csv'
                else:
                    inFile = params['rhdfbasersltdir']+'/'+src+'/'+src+'_block_counts_'+c+'.csv'
                
                with open(outFile,'a+') as outF:
                    with open(inFile,'r') as inF:
                        if counter > 1:
                            firstLine = inF.readline()
                        for inFLine in inF:
                            outF.write(inFLine)
                if counter%100 == 0:
                    logging.info('Filling national file, counties done {0}'.format(counter))
                counter+=1
    
    if src in ['cef','hdf','cmrcl']:
        cFilesToDelete = glob.glob(params['rsltbase']+src+'/'+src+'_block_counts_*.csv')
    else:
        cFilesToDelete = glob.glob(params['rhdfbasersltdir']+'/'+src+'/'+src+'_block_counts_*.csv')
    for file in cFilesToDelete:
        os.remove(file)

    tabber.logTime()
    logging.info('\n\n###### END OF TABS PROGRAM ######\n\n')
    
if __name__ == '__main__':
    import sys
    main(sys.argv[1],sys.argv[2])
