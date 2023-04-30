# -*- coding: utf-8 -*- 
#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)

"""
Args: 
 
Example execution: 
    $ setsid python r00

Part of the replication archive for The U.S. Census Bureau's Ex Post
Confidentiality Analysis of the 2010 Census Data Publications
(https://github.com/uscensusbureau/recon_replication)
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


# MODULE LEVEL VARIABLES
# dir of program
myRoot = os.path.dirname(os.path.abspath(__file__))
# param dictionary
params = json.load(open(myRoot+'/../common/config.json','r'))
# import common py
sys.path.insert(0,myRoot+'/../common/')
from stats import Stats

# date time stamp to be put in outfile names
date = repr(os.popen('date +"%m-%d-%y"').read() +
    os.popen('date +"%T"').read()).replace(r"\n"," ").replace("'","").strip().replace(" ","_")

def main(numWorkers,src):
    """Main body of match program.
    """
    
    # Open log file
    sys.stderr = open(myRoot+'/summarize_'+src+'_{0}.error'.format(date),'a')    
    try:
        os.remove(myRoot+'/stats.log')
    except:
        pass
        
    logging.basicConfig(filename=
        myRoot+'/stats.log',
        level=logging.DEBUG)
        
    logging.info('\n\n###### BEGINNING OF SUMMARIZE PROGRAM ######\n\n')
    logging.info('date time: {0}'.format(date))
    logging.info('src is: {0}'.format(src))
    """
    try:
        os.remove(params['rhdfbasersltdir']+src+'/'+src+'_stats.xlsx')
    except:
        pass
    """
    # check what files exist 
    # we assume that if you have hdf agree you will have cef agree
    # we assume that if you have bin putative you have bin confirm
    params['doAgreeFzy'] = os.path.exists(params['rhdfbasersltdir']+src+'/'+'agree_hdf_'+src+'.csv')
    params['doAgreeBin'] = os.path.exists(params['rhdfbasersltdir']+src+'/'+'agreebinage_hdf_'+src+'.csv')
    params['doPutativeFzy'] = os.path.exists(params['rhdfbasersltdir']+src+'/'+'putative_'+src+'_cef.csv')
    params['doPutativeBin'] = os.path.exists(params['rhdfbasersltdir']+src+'/'+'putativebinage_'+src+'_cef.csv')
    # initialize stat maker class
    statMaker = Stats(params,src)
    logging.info('start time is: {0}'.format(statMaker.t0))
    
    # load nat pop by re
    natpop = pd.read_csv(params['rsltbase']+'/cef/cef_block_counts.csv')
    natpop['total'] = 'total'
    reCols = [x for x in natpop.columns if 'freq' in x]
    natpop = natpop.groupby('total')[reCols].sum()
    natpop.columns = [x.replace('freq','') for x in natpop.columns]
    natpop = natpop.melt()
    statMaker.natpop = natpop.rename({'variable':'re'}, axis=1).set_index('re').to_dict()

    # find modal re
    statMaker.findModalRE()
    # load bModal to class
    statMaker.bModal = pd.read_csv(params['rsltbase']+'/cef/cef_modalre.csv',dtype='str') 
    
    # flag block-piks with modalre
    with open(params['geolookup']+'allcounties.txt') as f:
        for line in f:
            county = line.replace('\n','')
            if county[0:2] != '72':
                statMaker.modalreQueue.put((county))
    
    # create and kick off workers
    processes = []
    for w in range(numWorkers):
        p = mp.Process(target=statMaker.modalREWorker)
        p.start()
        processes.append(p)
        time.sleep(1)
    
    for p in processes:
        p.join()
        logging.info('modalREWorker process: {0} finished and joined'.format(p))
        statMaker.logTime()
    
    # stitch together county-level block-pik flags
    counter = 1
    outFile = params['rsltbase']+'/cef/cef_over21_modalre_oneton.csv'
    # make sure we start with clean slate
    try:
        os.remove(outFile)
    except:
        pass
    
    # glue them together
    with open(params['geolookup']+'allcounties.txt') as f:
        for line in f:
            c = line.replace('\n','')
            if c[0:2] != '72':
                inFile = params['rsltbase']+'/cef/cef_over21_modalre_oneton_'+c+'.csv'
                
                with open(outFile,'a+') as outF:
                    with open(inFile,'r') as inF:
                        if counter > 1:
                            firstLine = inF.readline()
                        for inFLine in inF:
                            outF.write(inFLine)
                if counter%100 == 0:
                    logging.info('Filling national block-pik cef_over21_modalre_oneton, counties done {0}'.format(counter))
                counter+=1
    # get rid of county pieces 
    cFilesToDelete = glob.glob(params['rsltbase']+'/cef/cef_over21_modalre_oneton_*.csv')
    for file in cFilesToDelete:
        os.remove(file)

        
    # agreement rate stats
    statMaker.agreeStats()
    # putative and confirmation stats
    statMaker.putativeConfirmStats()
    statMaker.xlWriter.save()
    
    # CMRCL-CEF block-pik overlap
    statMaker.cmrclQuality()
    
    statMaker.logTime()
    logging.info('\n\n###### END OF SUMMARIZE PROGRAM ######\n\n')
    
if __name__ == '__main__':
    import sys
    main(sys.argv[1],sys.argv[2])
    
