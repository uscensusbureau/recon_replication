#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)

"""
.. module:: splitter
    :platform: Unix
    :synopsis: class to split plus database

.. moduleauthor:: Goldschlag

Part of the replication archive for The U.S. Census Bureau's Ex Post
Confidentiality Analysis of the 2010 Census Data Publications
(https://github.com/uscensusbureau/recon_replication)
"""

import time
import multiprocessing as mp
import os
import sys
import logging
import pandas as pd
import numpy as np

class Splitter(object):
    """Class to organize threads that split the putative matches into county files.
    Relies on writeWorker() to pull data from self.writeQueue and push that data 
    through self.writeDFChunk().
    
    """
    
    def __init__(self,params,df,leftarg,rightarg):
        self.m = mp.Manager()
        self.paramDict = params
        self.df2Split = df
        self.readCountyQueue = self.m.Queue()
        self.writeQueue = self.m.Queue()
        self.leftarg = leftarg
        self.rightarg = rightarg
        
    def fillLoadQueue(self,counties,q):
        """Fills generic queue q with counties that need to be loaded to work 
        queue. 
        
        Args:
            counties (list): list of counties to add to queue
            
            q (mp.Manager().Queue()): queue that the counties will be loaded to  
            
        """
        for c in counties:
            c = str(c)
            q.put(c)

    def putDFChunk(self,c):
        """Puts a chunk of df2Split into writeQueue, filtering by county c. 
        
        Args:
            c (str): 5 character county identifier (st fips + county fips)
            
        """
        self.writeQueue.put((c,self.df2Split[self.df2Split.county==c]))

    def writeDFChunk(self,c,df):
        """Writes data in df to csv in temp dir under plusPath.
        
        Args:
            c (str):  5 character county identifier (st fips + county fips) 
            
            df (pandas dataframe):  Dataframe of county data to be written to csv
            
        """
        #logging.debug('writeDFChunk: trying {0}'.format(c))
        if 'binage' in self.paramDict['putativematchestodo']:
            df.to_csv(self.paramDict['plusOutDir'] + '/' + self.leftarg + self.rightarg + 'plusbinage' +str(c)+'.csv', index=False)
        else:
            df.to_csv(self.paramDict['plusOutDir'] + '/' + self.leftarg + self.rightarg + 'plus' +str(c)+'.csv', index=False)
        #logging.debug('writeDFChunk: did {0}'.format(c))

    def readWorker(self):
        """Puts data for counties into writeQueue via self.putDFChunk().
        
        """
        while self.readCountyQueue.qsize()>0:
            try:
                c = self.readCountyQueue.get(timeout = 10)
            except:
                logging.info('Read worker timeout---------------')
                logging.info('readCountyQueue size: {0}'.format(self.readCountyQueue.qsize()))
                
            self.putDFChunk(c)
        else:
            logging.info('Read worker empty queue---------------')
            logging.info('readCountyQueue size: {0}'.format(self.readCountyQueue.qsize()))



if __name__ == '__main__':
    main()
