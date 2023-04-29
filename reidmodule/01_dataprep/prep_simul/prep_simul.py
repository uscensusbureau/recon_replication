# -*- coding: utf-8 -*- 
"""
Creates simulated HDF files for "guessers". 

Args: 
    numWorkers (int): number of worker threads to use.

Example execution: 
    $ setsid /user/bin/python3 prep_simul.py 10

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
np.random.seed(20220502)

class dataSimulator(object):
    """Class to organize tabulations of county-level datasets. 
    
    Args:
        lock (mp.Lock()): lock used to modify shared number of tracts processed.
    
        params (dict): dictionary of params (from json)
    
    """
    
    def __init__(self,lock,params):
        self.lock = lock
        self.m = mp.Manager()
        self.paramDict = params
        # queues
        self.countyQueue = self.m.Queue()
        self.countiesPrcd = mp.Value('i',0) 
        self.t0 = time.time()

    def logTime(self):
        """Write time lapsed to log file.
        
        """
        
        t  =  time.time() - self.t0
        logging.info('Minutes lapsed: {0}'.format(t/60))
    
    def simulWorker(self):
        """Simul worker function, churning through counties in inQ and sending them 
        through the genSimulData function.
        
        Args:
            
        """
        
        # what each worker does, pulling data and running the match
        churn = True
        while churn:
            # Pull the next record for comparison 
            try:
                county = self.countyQueue.get(timeout = 15)
            except:
                # If no work can be pulled from queue, break
                logging.info('Worker queue timeout---------------')
                logging.info('countyQueue size: {0}'.format(self.countyQueue.qsize()))
                logging.info('Number of counties processed: {0}'.format(self.countiesPrcd.value))
                break
            # Keep track of the number of records processed
            with self.lock:
                self.countiesPrcd.value += 1
            
            # Compare record for match against all searchTerm
            self.genSimulData(county)

    def genSimulData(self,county):
        path = self.paramDict['hdfdir']+'/hdf'+county+'.csv'
        inDF=pd.read_csv(path,dtype=str,engine='python')
        county = path[-9:-4]
        inDF['geoid_blockgroup'] = inDF['geoid_block'].str.slice(0,12)
        
        # first do modal guesser
        # find block group level modals, returns multiple if ties
        bgCounts = inDF.groupby(['geoid_blockgroup']+self.paramDict['raceethvars']).size().reset_index(name='count_bg')
        bgCounts['bg_dup'] = bgCounts.groupby(['geoid_blockgroup','count_bg'])['geoid_blockgroup'].transform('count')
        bgCounts = bgCounts.sort_values(['geoid_blockgroup','count_bg'],ascending=False)
        bgModal = bgCounts.groupby('geoid_blockgroup').first().reset_index()

        # find block level modals, returns multiple if ties
        bCounts = inDF.groupby(['geoid_block']+self.paramDict['raceethvars']).size().reset_index(name='count_b') 
        bCounts['b_dup'] = bCounts.groupby(['geoid_block','count_b'])['geoid_block'].transform('count')
        bCounts = bCounts.sort_values(['geoid_block','count_b'],ascending=False)
        bModal = bCounts.groupby('geoid_block').first().reset_index()
        bModal['geoid_blockgroup'] = bModal['geoid_block'].str.slice(0,12)

        bModal = bModal.merge(bgModal, on=['geoid_blockgroup'], how='left')

        # start with block modal if its unique
        for v in self.paramDict['raceethvars']:
            bModal[v] = ''
            bModal.loc[(bModal.b_dup==1),v] = bModal.loc[(bModal.b_dup==1)][v+'_x']

        # blockgroup modal if block modal pop is 1
        for v in self.paramDict['raceethvars']:
            bModal.loc[(bModal.count_b==1) & (bModal.bg_dup==1) & (bModal.count_bg>1),v] = bModal.loc[(bModal.count_b==1) & (bModal.bg_dup==1) & (bModal.count_bg>1)][v+'_y']

        # blockgroup modal if tie for block 
        for v in self.paramDict['raceethvars']:
            bModal.loc[(bModal.b_dup>1) & (bModal.bg_dup==1),v] = bModal.loc[(bModal.b_dup>1) & (bModal.bg_dup==1)][v+'_y']

        # national modal if all else fails
        bModal.loc[(bModal.white==''),'white'] = '1'
        bModal.loc[(bModal.black==''),'black'] = '0'
        bModal.loc[(bModal.aian==''),'aian'] = '0'
        bModal.loc[(bModal.asian==''),'asian'] = '0'
        bModal.loc[(bModal.nhopi==''),'nhopi'] = '0'
        bModal.loc[(bModal.sor==''),'sor'] = '0'
        bModal.loc[(bModal.hisp==''),'hisp'] = '0'

        bModal = bModal[['geoid_block']+self.paramDict['raceethvars']]
        
        # keep block sex age from HDF, merge on the modal race eth
        inDFModalGuesser = inDF[['geoid_block','sex','age']].merge(bModal, on='geoid_block', how='left')
        inDFModalGuesser = inDFModalGuesser[['geoid_block','sex','age']+self.paramDict['raceethvars']]
        inDFModalGuesser = inDFModalGuesser.sort_values(by=['geoid_block','sex','age']+self.paramDict['raceethvars'])
        inDFModalGuesser.to_csv(self.paramDict['simuldir']+'modalGsr'+county+'.csv',index=False)
        
        varsToKeep = ['geoid_block','sex','age']+params['raceethvars']
        # now do proportional guesser
        # to do this we iterate block-by-block, generating a set of cumulative probabilities for selecting a given race-ethnicity combination, draw a random number for the HDF, then select RE using that random number
        # a much simpler approach would be to use np.random.choices(), but we want to retain the random numbers used to generate the guesses. so long as the race-ethnicity combinations remain unchanged, it should be trivial to replicate the selection with the random number stored on the final dataset. 
        inDFPropGuesser = pd.DataFrame(columns=['geoid_block','sex','age'] + self.paramDict['raceethvars'] + ['prop_r'])
        blocks = list(set(inDF.geoid_block))
        for b in blocks:
            prob = bCounts[bCounts.geoid_block==b]
            prob = prob.sort_values(by='count_b')
            prob['p'] = (prob.count_b/prob.count_b.sum()).cumsum()
            prob = prob[self.paramDict['raceethvars']+['p']]
            d = inDF[inDF.geoid_block==b][['geoid_block','sex','age']]
            d['prop_r'] = np.random.uniform(0,1,len(d))
            re = self.drawRE(prob,d.prop_r)
            dRE = pd.concat([d.reset_index(drop=True),re],axis=1)[['geoid_block','sex','age'] + self.paramDict['raceethvars'] + ['prop_r']]
            inDFPropGuesser = inDFPropGuesser.append(dRE)
        
        inDFPropGuesser = inDFPropGuesser.sort_values(by=['geoid_block','sex','age']+self.paramDict['raceethvars'])
        inDFPropGuesser.to_csv(self.paramDict['simuldir']+'propGsr'+county+'.csv',index=False)
        

    def drawRE(self,prob,r):
        """Creates a number of records equal to the length of r with race and ethnicity (RE) in proportion to he probabilities in prob. 
        
        Args:
            prob (dataframe): dataframe with columns in params['raceethvars'] and a column 'p' that holds the cumulative probability of selection for each RE combination. RE must be sorted by the probability in ascending order. probabilities must be cumulative. for example, [.2,.4,1]. the first entry in this case will be selected if r is <.2, so with .2 probability of selection, the second entry will be selected if .2>=r<.4, so again with a .2 probability. finally, the third entry will be selected if .4>=r<1, with .6 probability. 
        
            r (series): random numbers that are uniform in (0,1) that are used to draw race ethnicity.
        
        """
        returnRE = pd.DataFrame(columns=self.paramDict['raceethvars'])
        # for each records in the simulated block, draw RE
        for i in range(len(r)):
            # for each unique combo of RE in HDF for this block, compare to the random number
            for j in range(len(prob)):
                # if random is less than the prob for RE then choose that RE, otherwise move to next RE
                if r.iloc[i]<prob.iloc[j]['p']:
                    returnRE = returnRE.append(prob.iloc[j][self.paramDict['raceethvars']])
                    # if we've assigned an RE, then we move to the next row in r
                    break
        return returnRE.reset_index()

def main(numWorkers):
    """Main body of simul program.
    """
    numWorkers = int(numWorkers)
    
    # Open log file
    sys.stderr = open(myRoot+'/prep_simul_{0}.error'.format(date),'w')
    logging.basicConfig(filename=
        myRoot+'/prep_simul_{0}.log'.format(date),
        level=logging.DEBUG)
    
    logging.info('date time: {0}'.format(date))
    
    # Get sets of counties to generate
    countiesToDo = []
    with open(params['geolookup']+'allcounties.txt') as f:
        for line in f:
            c = line.replace('\n','')
            if c[0:2]!='72':
                countiesToDo.append(c)
    
    # initialize the data generator
    lock = mp.Lock()
    simulator = dataSimulator(lock,params)
    
    logging.info('\n\n###### LOAD COUNTY QUEUE ######\n\n')
        
    # put county codes into the working queue
    for c in countiesToDo:
        simulator.countyQueue.put(c)

    logging.info('starting county queue size: {0}'.format(simulator.countyQueue.qsize()))
        
    logging.info('\n\n###### RUN SIMULATOR ######\n\n')
        
    # launch simulator workers
    processes = []
    for w in range(numWorkers):
        p = mp.Process(target=simulator.simulWorker)
        p.start()
        processes.append(p)
        time.sleep(1)
    for p in processes:
        p.join()
        logging.info('Simulator process {0} finished and joined'.format(p))
    
    simulator.logTime()
    logging.info('end county queue size: {0}'.format(simulator.countyQueue.qsize()))
    logging.info('\n\n###### END OF PREP SIMUL PROGRAM ######\n\n')
        
if __name__ == '__main__':
    import sys
    main(sys.argv[1])
