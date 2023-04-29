"""
.. module:: tabber
    :platform: Unix
    :synopsis: class to run tabs

.. moduleauthor:: Goldschlag
"""

import time
import multiprocessing as mp
import os
import sys
import logging
import pandas as pd
import numpy as np
import scipy.spatial.distance as spdist
import itertools
import math

class Tabber(object):
    """Class to organize tabulations of county-level datasets. 
    
    Args:
        lock (mp.Lock()): lock used to modify shared number of tracts processed.
    
        params (dict): dictionary of params (from json)
    
    """
    
    def __init__(self,lock,params):
        self.lock = lock
        self.m = mp.Manager()
        # validation queues
        self.tabQueue = self.m.Queue()
        self.paramDict = params
        self.t0 = time.time()
        
    def logTime(self):
        """Write time lapsed to log file.
        
        """
        
        t  =  time.time() - self.t0
        logging.info('Minutes lapsed: {0}'.format(t/60))

    def getSourceData(self,county,source):
        """Loads data to memory from county-level files.
         
        Args:
            county (string): 5-digit county FIPS code, 2-digit state and 3-digit 
            county codes 
        
        Returns:
            df (pd.DataFrame): pandas dataframe with data for county
        
        """
        # file
        if source in ['cef','hdf','cmrcl']:
            file = self.paramDict[source+'dir'] + source + county + '.csv'
        else:
            file = self.paramDict['rhdfbasedir'] + '/' + source + '/' + source + county + '.csv'
        
        # load county file
        try:
            inDF = pd.read_csv(file,dtype=str)
            inDF['geoid_tract'] = inDF.geoid_block.str.slice(start=0,stop=11)
            inDF['county'] = inDF.geoid_block.str.slice(start=0,stop=5)
            inDF['tract'] = inDF.geoid_block.str.slice(start=5,stop=11)
            inDF['block'] = inDF.geoid_block.str.slice(start=11,stop=15)
            inDF['blockid'] = inDF.tract + inDF.block
            for v in ['geoid_block']:
                inDF[v] = inDF[v].astype(str)
            if source in ['cmrcl']:
                for v in ['sex','age']:
                    inDF[v] = inDF[v].astype(float)
                inDF=inDF[(inDF.tract!='')&(inDF.block!='')&(inDF.pik!='')&(inDF.sex.isnull()==False)&(inDF.age.isnull()==False)]
            for v in ['sex','age']+self.paramDict['raceethvars']:
                try:
                    inDF[v] = inDF[v].astype(int)
                except:
                    continue
            return inDF.drop(['geoid_tract','county','tract','block','blockid'],axis=1)
        except:
            logging.info('failed file: {0}'.format(file))
            return pd.DataFrame()
    
    def tabWorker(self):
        """.
        
        Args:

        """
        
        # what each worker does, pulling data and computing tabs
        churn = True
        while churn:
            # Pull the next county
            try:
                q = self.tabQueue.get(timeout = 15)
            except:
                # If no work can be pulled from queue, break
                logging.info('Worker queue timeout---------------')
                logging.info('tabQueue size: {0}'.format(self.tabQueue.qsize()))
                break
            # tuples in queue contain county [0] and source [1]
            self.tabData(q[0],q[1])
            
    def tabData(self, county, source):
        """
         
        Args:
        
        Returns:
        
        """
        # get data
        df = self.getSourceData(county, source)
        #df['geoid_county'] = df.geoid_block.str.slice(start=0,stop=5)
        #df['geoid_tract'] = df.geoid_block.str.slice(start=0,stop=11)
        df['haspik'] = 0
        df['hasall3'] = 0
        df['keepblockpik'] = 0
        
        if source in ['cef','cmrcl']:
            df.loc[df.pik.notnull(),'haspik'] = 1
            df.loc[(df.pik.notnull()) & (df.age.notnull()) & (df.sex.notnull()),'hasall3'] = 1
        if source == 'cef':
            df.loc[df.keep=='1','keepblockpik'] = 1
            
        df['hasage'] = 0
        df.loc[df.age.notnull(),'hasage'] = 1
            
        df['hassex'] = 0
        df.loc[df.sex.notnull(),'hassex'] = 1
        df['pop'] = 1
    
        # over under 21
        df['popgt21'] = 0
        df.loc[df.age>21,'popgt21'] = 1
        df['poplteq21'] = 0
        df.loc[df.age<=21,'poplteq21'] = 1
    
        # uniques
        df['uniall'] = 0
        if source != 'cmrcl':
            gb = df.groupby(['geoid_block','sex','age']+self.paramDict['raceethvars']).size().reset_index().rename(columns={0:'freq'})
            gb=gb[gb.freq==1]
            df = pd.merge(df,gb,how='left',on=['geoid_block','sex','age']+self.paramDict['raceethvars'],indicator=True)
            df.loc[df._merge=='both','uniall'] = 1
            df = df.drop(['_merge','freq'],axis=1)
        
        gb = df.groupby(['geoid_block','sex','age']).size().reset_index().rename(columns={0:'freq'})
        gb=gb[gb.freq==1]
        df = pd.merge(df,gb,how='left',on=['geoid_block','sex','age'],indicator=True)
        df['uniagesex'] = 0
        df.loc[df._merge=='both','uniagesex'] = 1
        df = df.drop(['_merge','freq'],axis=1)
    
        if source != 'cmrcl':
            gbr = df.groupby(['geoid_block'] + self.paramDict['raceethvars']).size().reset_index().rename(columns={0:'freq'})
            gbr['r'] = gbr.white.astype(str) + gbr.black.astype(str) + gbr.aian.astype(str) + gbr.asian.astype(str) + gbr.nhopi.astype(str) + gbr.sor.astype(str) + gbr.hisp.astype(str)
            gbr = gbr[['geoid_block','r','freq']]
            gbr = gbr.set_index(['geoid_block','r']).unstack('r')
            gbr.columns = [''.join(a) for a in gbr.columns.to_flat_index()]
        
        if source == 'cmrcl':
            clps = df.groupby('geoid_block')[['sex','haspik','hasage','hassex','hasall3','pop','uniagesex','popgt21','poplteq21','keepblockpik']].sum()
        else:
            clps = df.groupby('geoid_block')[['sex','haspik','hasage','hassex','hasall3','pop','uniall','uniagesex','popgt21','poplteq21','keepblockpik']].sum()
        
        if source != 'cmrcl':
            # create empty freq race eth cols (all possible combos)
            rCols = list(itertools.product([0,1],repeat=len(self.paramDict['raceethvars'])))
            for i in range(0,len(rCols)):
                rCols[i] = [str(j) for j in rCols[i]]
                rCols[i] = 'freq'+''.join(rCols[i])
                clps[rCols[i]] = np.nan
            clps.update(gbr)

        clps = clps.reset_index()
        clps = clps.sort_values(by='geoid_block')
        
        if source in ['cef','hdf','cmrcl']:
            outFile = self.paramDict['rsltbase']+source+'/'+source+'_block_counts_'+county+'.csv'
        else:
            outFile = self.paramDict['rhdfbasersltdir'] + '/' + source + '/' + source+'_block_counts_'+county+'.csv'
        clps.to_csv(outFile,index=False)
    
    