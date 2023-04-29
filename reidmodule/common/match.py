"""
.. module:: matcher
    :platform: Unix
    :synopsis: class to run match

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

class Matcher(object):
    """Class to organize tabulations of county-level datasets. 
    
    Args:
        lock (mp.Lock()): lock used to modify shared number of tracts processed.
    
        params (dict): dictionary of params (from json)
    
    """
    
    def __init__(self,lock,params,leftarg,rightarg):
        self.lock = lock
        self.m = mp.Manager()
        # validation queues
        self.countyQueue = self.m.Queue()
        self.workQueue = self.m.Queue()
        self.doneStatQueue = self.m.Queue()
        self.doneMatchQueue = self.m.Queue()
        self.paramDict = params
        self.leftarg = leftarg
        self.rightarg = rightarg
        self.t0 = time.time()
        self.valTractsPrcd = mp.Value('i',0) 
        self.putTractsPrcd = mp.Value('i',0) 
        self.confTractsPrcd = mp.Value('i',0)
        self.matchType = ''

        
    def logTime(self):
        """Write time lapsed to log file.
        
        """
        
        t  =  time.time() - self.t0
        logging.info('Minutes lapsed: {0}'.format(t/60))
    
    def binAge(self,series):
        """Translates single-year age value into bins consistent with tabulations.  
         
        Args:
            series (list):   
        
        """
        if series <= 21 :
            return series
        elif 22 <= series <= 24:
            return 22
        elif 25 <= series <= 29:
            return 25
        elif 30 <= series <= 34:
            return 30
        elif 35 <= series <= 39:
            return 35
        elif 40 <= series <= 44:
            return 40
        elif 45 <= series <= 49:
            return 45
        elif 50 <= series <= 54:
            return 50
        elif 55 <= series <= 59:
            return 55
        elif 60 <= series <= 61:
            return 60
        elif 62 <= series <= 64:
            return 62
        elif 65 <= series <= 66:
            return 65
        elif 67 <= series <= 69:
            return 67
        elif 70 <= series <= 74:
            return 70
        elif 75 <= series <= 79:
            return 75
        elif 80 <= series <= 84:
            return 80
        elif series >= 85:
            return 85
            
    def getSourceData(self,county,source):
        """Loads data to memory from county-level files.
         
        Args:
            county (string): 5-digit county FIPS code, 2-digit state and 3-digit 
            county codes  
        
        Returns:
            df (pd.DataFrame): pandas dataframe with data for county
        
        """
        # file
        # the vanilla cef, hdf, or cmrcl data
        if source in ['cef','hdf','cmrcl']:
            file = self.paramDict[source+'dir'] + source + county + '.csv'
        # the vanilla guesser data
        elif source in ['modalGsr','propGsr']:
            file = self.paramDict['simuldir'] + '/' + source + county + '.csv'
        # the plus guesser data
        elif 'plus' in source and ('modalGsr' in source or 'propGsr' in source) and (source[0]!='r'):
            file = self.paramDict['simuldir'] + '/' + source + county + '.csv'
        # the cef or hdf plus data
        elif 'plus' in source and ('cef' in source or 'hdf' in source) and (source[0]!='r'):
            stem = source[0:3]
            file = self.paramDict[stem+'dir'] + source + county + '.csv'
        # rhdf plus data
        elif 'plus' and source[0]=='r':
            stem = source.replace('plus','').replace('cef','').replace('cmrcl','').replace('binage','').replace('fzyage','')
            file = self.paramDict['rhdfbasedir'] + '/' + stem + '/' + source.replace('fzyage','') + county + '.csv'
        # the vanilla rhdf data
        else:
            file = self.paramDict['rhdfbasedir'] + '/' + source + '/' + source + county + '.csv'
        
        # load county file
        try:
            inDF = pd.read_csv(file,dtype=str,engine='python')
            if 'plus' in source:
                inDF['geoid_block'] = inDF.county + inDF.tract + inDF.block
            inDF['geoid_tract'] = inDF.geoid_block.str.slice(start=0,stop=11)
            inDF['county'] = inDF.geoid_block.str.slice(start=0,stop=5)
            inDF['tract'] = inDF.geoid_block.str.slice(start=5,stop=11)
            inDF['block'] = inDF.geoid_block.str.slice(start=11,stop=15)
            inDF['blockid'] = inDF.tract + inDF.block
            
            # vanilla modal guesser data may be missing race-eth because no mode exists 
            if source == 'modalGsr': 
                inDF = inDF.drop(inDF[inDF.white.isna()].index)
            
            for v in ['geoid_block']:
                inDF[v] = inDF[v].astype(str)
           
            if source == 'cmrcl':
                inDF['r'] = inDF['r'].astype(float)
                for v in ['sex','age']:
                    inDF[v] = inDF[v].astype(float)
                inDF=inDF[(inDF.tract!='')&(inDF.block!='')&(inDF.pik!='')&(inDF.sex.isnull()==False)&(inDF.age.isnull()==False)]
            elif source == 'cef':
                #inDF = inDF.drop(['r'],1)
                inDF['r'] = inDF['r'].astype(float)
                for v in ['keep','sex','age']+self.paramDict['raceethvars']:
                    inDF[v] = inDF[v].astype(int)
                inDF['pik'] = inDF['pik'].astype(str)
                inDF.loc[inDF.pik=='nan','pik']=''
            elif 'plus' in source:
                for v in ['sex','age']+self.paramDict['raceethvars']:
                    inDF[v] = inDF[v].astype(int)
                inDF['pik'] = inDF['pik'].astype(str)
                inDF.loc[inDF.pik=='nan','pik']=''
            else:
                for v in ['sex','age']+self.paramDict['raceethvars']:
                    inDF[v] = inDF[v].astype(int)
                
            if source == 'cef':
                inDF= inDF.sort_values(by=['county','tract','block','sex','age','white','black','aian','asian','nhopi','sor','hisp'])
            elif source == 'cmrcl':
                inDF= inDF.sort_values(by=['county','tract','block','sex','age','r'])
            elif 'plus' in source:
                inDF= inDF.sort_values(by=['county','tract','block','sex','age','pik'])
            else:
                inDF= inDF.sort_values(by=['county','tract','block','sex','age','white','black','aian','asian','nhopi','sor','hisp'])
            inDF['agebin'] = inDF['age'].apply(self.binAge)
            return inDF
        except:
            logging.info('failed file: {0}'.format(file))
            return pd.DataFrame()

    def fillCountyQueue(self,counties,q):
        """Fills loadQueue with counties that need to be loaded to work queue. 
         
        Args:
            counties (list): list of counties to add to queue
            q (mp.Manager().Queue()):queue that the counties will be loaded to  
        
        """
        
        # put fips codes into the working queue
        for c in counties:
            q.put(c)

    def loadData(self,county):
        """Puts tuple of left and right tract-level data into workValQueue. 
         
        Args:
            county (string): 5-digit county FIPS code, 2-digit state and 3-digit 
            county codes 
         
        """
        # add the county
        l = self.getSourceData(county,self.leftarg).to_dict(orient='records')
        if self.matchType=='validate' and self.rightarg == 'cef':
            r = self.getSourceData(county,self.rightarg)
            r.age = pd.to_numeric(r.age,downcast='float')
            r = r.to_dict(orient='records')
        elif self.matchType=='putative' and self.rightarg == 'cef':
            r = self.getSourceData(county,self.rightarg).drop(self.paramDict['raceethvars'],1)
            r.age = pd.to_numeric(r.age,downcast='float')
            r = r.to_dict(orient='records')
        #elif self.rightarg == 'cef':
        #    r = self.getSourceData(county,self.rightarg)
        #    r.age = pd.to_numeric(r.age,downcast='float')
        #    r = r.to_dict(orient='records')
        else:
            r = self.getSourceData(county,self.rightarg).to_dict(orient='records')

        # break into tracts and load to queue
        tracts = list(set([x['geoid_tract'] for x in l]) & set([x['geoid_tract'] for x in r]))
        # validate tract set
        if len(tracts) < len(set([x['geoid_tract'] for x in l])) or len(tracts) < len(set([x['geoid_tract'] for x in r])):
            logging.warning('MISSING TRACTS IN COUNTY: {0}'.format(county))
        
        for t in tracts:
            l2Load = [x for x in l if x['geoid_tract']==t]
            r2Load = [x for x in r if x['geoid_tract']==t] 
            self.workQueue.put((l2Load,r2Load))

    def loadDataWorker(self):
        """Load worker thread function, churning through counties in inQ, 
        adding tract-level data from a given county via a load function.
        
        Args:
            inQ (mp.Manager().Queue()): queue from which counties will be pulled
            
            workQ (string): string that captures what load function to use 
            ('val', 'atch', or 'cnfrm') 
            
        """
        
        # what each worker does, pulling a code and running the match
        churn = True
        while churn:
            # Pull the next record for comparison 
            try:
                c = self.countyQueue.get(timeout = 15)
            except:
                # If no work can be pulled from queue, break
                logging.info('Load worker queue timeout---------------')
                logging.info('load queue size: {0}'.format(self.countyQueue.qsize()))
                logging.info('---------------Load worker queue timeout')
                break
            # put data for c in workqueue via loadfunc
            self.loadData(c)
            
    def matchWorker(self, inQ, counter, matchFunc, cols2Match):
        """Match worker function, churning through data in inQ and sending them 
        through the match functioned.
        
        Args:
            inQ (mp.Manager().Queue()): queue from which data will be pulled
            
            counter (mp.Value('i',0)): counter that captures the number of 
            tracts processed
            
            matchFunc (string): string that captures what match function to use 
            ('val', 'atch', or 'cnfrm') 
            
            cols2Match (list): list of strings with labels of the variables that
            will be matched
        """
        
        # what each worker does, pulling data and running the match
        churn = True
        while churn:
            # Pull the next record for comparison 
            try:
                t = inQ.get(timeout = 15)
            except:
                # If no work can be pulled from queue, break
                logging.info('Worker queue timeout---------------')
                logging.info('inQ size: {0}'.format(inQ.qsize()))
                logging.info('Number of tracts processed: {0}'.format(counter.value))
                break
            # Keep track of the number of records processed
            with self.lock:
                counter.value += 1
            
            # Compare record for match against all searchTerm
            if matchFunc == 'val':
                self.validate(t,cols2Match)
            elif matchFunc == 'atch':
                self.attachPIK(t,cols2Match)
            elif matchFunc == 'cnfrm':
                self.confirmPIK(t,cols2Match)

    def match(self,match2Do, cols2Match, leftBlk, rightBlk, colsFromLeft, 
            colsFromRight, matchDF, matchFlagName, compareResidual):
        """Match function that iterates over left and right, returning match 
        counts and matched records.
        
        Args:
            match2Do (dictioanry): keys are match types ('exact', 'fzyage', 
            'oneoff') with values True or False that determine what matches will
            be executed
            
            cols2Match (list): list of strings with labels of the variables that
            will be matched
            
            leftBlk (list): list of record dictionaries for left data
            
            rightBlk (list): list of record dictionaries for right data
            
            colsFromLeft (list): list of variable labels that will be recovered
            from the left data for the matched DF
            
            colsFromRight (list): list of variable labels that will be recovered
            from the right data for the matched DF
            
            matchDF (pd.DataFrame): dataframe into which matched records will be 
            stored
            
            matchFlagName (string): label to be used for the flag that captures 
            what type of match was made in the match DF
            
            compareResidual (true/false): whether to compare residual unmatched blocks
            
        Returns: 
            exactCount (int): count of exact matches
            
            fzyAgeCount (int): count of fuzzy age matches
            
            binAgeCount (int): count of age bin matches
            
            oneOffCount (int): count of one off matches
            
            matchDF (pd.DataFrame): dataframe into which matched records were
            stored
            
            js (float): jensen-shannon comparison of residuals, or -1 if compareResidual==False
        
        """
        # initialize counters
        leftCount = len(leftBlk)
        rightCount = len(rightBlk)
        exactCount = 0
        fzyAgeCount = 0
        binAgeCount = 0
        oneOffCount = 0
        
        # sort the lists of dicts
        leftBlk, rightBlk = self.sortMatchDicts(match2Do, cols2Match, leftBlk, rightBlk)
        
        #logging.debug('colsFromRight: {0}'.format(colsFromRight))
        
        # exact comparisons
        if match2Do['exact'] == True:
            # search for all exact matches first, all left records have a
            # chance of finding an exact match before we look for fuzzy
            # matches
            for i in range(len(leftBlk)-1,-1,-1):
                l = leftBlk[i]
                # compare to each record on the right until we find a match, 
                # if we do remove both
                for j, r in enumerate(rightBlk):
                    # exact match?
                    if {k: l[k] for k in cols2Match}=={k: r[k] for k in cols2Match}:
                        exactCount += 1
                        # save match records
                        newRec = {}
                        newRec.update({k: l[k] for k in colsFromLeft})
                        # rename out dict if same vars coming from right if necessary
                        if 'cef_white' in colsFromRight:
                            nr = self.renameMatchDict(r, 'cef')
                            newRec.update({k: nr[k] for k in colsFromRight})
                        elif 'syn_white' in colsFromRight:
                            nr = self.renameMatchDict(r, 'syn')
                            newRec.update({k: nr[k] for k in colsFromRight})
                        elif any('mdf' in x for x in colsFromRight):
                            nr = self.renameMatchDict(r, 'mdf')
                            newRec.update({k: nr[k] for k in colsFromRight})
                        elif any(self.rightarg[0:3] in x for x in colsFromRight):
                            nr = self.renameMatchDict(r, self.rightarg[0:3])
                            newRec.update({k: nr[k] for k in colsFromRight})
                        else:
                            newRec.update({k: r[k] for k in colsFromRight})
                        newRec.update({matchFlagName:1})
                        matchDF = matchDF.append(newRec,ignore_index=True)
                        del leftBlk[i]
                        del rightBlk[j]
                        break
        # fuzzy age comparison 
        if match2Do['fzyage'] == True:
            # search for all fuzzy age matches in residual
            for i in range(len(leftBlk)-1,-1,-1):
                l = leftBlk[i]
                # compare to each record on the right
                for j, r in enumerate(rightBlk):
                    diffs = []
                    if 'sex' in cols2Match:
                        diffs.append(l['sex']!=r['sex'])
                    if 'pik' in cols2Match:
                        diffs.append(l['pik']!=r['pik'])
                    if 'white' in cols2Match:
                        diffs.append(len({k: l[k] for k in self.paramDict['raceethvars'] if l[k]!=r[k]}))
                    ageDiff = abs(l['age'] - r['age'])>self.paramDict['agefuzdiff']
                    
                    if ageDiff==0 and sum(diffs)==0: 
                        fzyAgeCount += 1
                        newRec = {}
                        newRec.update({k: l[k] for k in colsFromLeft})
                        # rename out dict if same vars coming from left and right
                        if 'cef_white' in colsFromRight:
                            nr = self.renameMatchDict(r, 'cef')
                            newRec.update({k: nr[k] for k in colsFromRight})
                        elif 'syn_white' in colsFromRight:
                            nr = self.renameMatchDict(r, 'syn')
                            newRec.update({k: nr[k] for k in colsFromRight})
                        elif any('mdf' in x for x in colsFromRight):
                            nr = self.renameMatchDict(r, 'mdf')
                            newRec.update({k: nr[k] for k in colsFromRight})
                        elif any(self.rightarg[0:3] in x for x in colsFromRight):
                            nr = self.renameMatchDict(r, self.rightarg[0:3])
                            newRec.update({k: nr[k] for k in colsFromRight})
                        else:
                            newRec.update({k: r[k] for k in colsFromRight})
                        newRec.update({matchFlagName:2})
                        matchDF = matchDF.append(newRec,ignore_index=True) 
                        del leftBlk[i]
                        del rightBlk[j]
                        break
        # bin age comparison 
        if match2Do['binage'] == True:
            # search for all age bin matches that did not receive an exact match
            # or a fuzzy age match
            for i in range(len(leftBlk)-1,-1,-1):
                l = leftBlk[i]
                # compare to each record on the right
                for j, r in enumerate(rightBlk):
                    diffs = []
                    if 'sex' in cols2Match:
                        diffs.append(l['sex']!=r['sex'])
                    if 'pik' in cols2Match:
                        diffs.append(l['pik']!=r['pik'])
                    if 'white' in cols2Match:
                        diffs.append(len({k: l[k] for k in self.paramDict['raceethvars'] if l[k]!=r[k]}))
                    
                    # Age bin match
                    if l['agebin'] == r['agebin'] and sum(diffs) == 0:
                        binAgeCount += 1
                        # save match records
                        newRec = {}
                        newRec.update({k: l[k] for k in colsFromLeft})
                        # rename out dict if same vars coming from right if necessary
                        if 'cef_white' in colsFromRight:
                            nr = self.renameMatchDict(r, 'cef')
                            newRec.update({k: nr[k] for k in colsFromRight})
                        elif 'syn_white' in colsFromRight:
                            nr = self.renameMatchDict(r, 'syn')
                            newRec.update({k: nr[k] for k in colsFromRight})
                        elif any('mdf' in x for x in colsFromRight):
                            nr = self.renameMatchDict(r, 'mdf')
                            newRec.update({k: nr[k] for k in colsFromRight})
                        elif any(self.rightarg[0:3] in x for x in colsFromRight):
                            nr = self.renameMatchDict(r, self.rightarg[0:3])
                            newRec.update({k: nr[k] for k in colsFromRight})
                        else:
                            newRec.update({k: r[k] for k in colsFromRight})
                        newRec.update({matchFlagName:3})
                        matchDF = matchDF.append(newRec,ignore_index=True)
                        del leftBlk[i]
                        del rightBlk[j]
                        break
        
        # compare residual after exact and fzyage, we don't want to remove oneoffs for this
        if compareResidual == True and leftBlk:
            js = self.histDiff(leftBlk, rightBlk, cols2Match)
        else:
            js = -1
        # one off comparison 
        if match2Do['oneoff'] == True:
            # search for all fuzzy age matches in residual
            for i in range(len(leftBlk)-1,-1,-1):
                l = leftBlk[i]
                # compare to each record on the right
                for j, r in enumerate(rightBlk):
                    diffs = []
                    if 'sex' in cols2Match:
                        diffs.append(l['sex']!=r['sex'])
                    if 'pik' in cols2Match:
                        diffs.append(l['pik']!=r['pik'])
                    if 'white' in cols2Match:
                        diffs.append(len({k: l[k] for k in self.paramDict['raceethvars'] if l[k]!=r[k]}))
                    ageDiff = abs(l['age'] - r['age'])>self.paramDict['agefuzdiff']
                    
                    if ageDiff==0 and sum(diffs) <= self.paramDict['fuzdiff']:
                        oneOffCount += 1
                        newRec = {}
                        newRec.update({k: l[k] for k in colsFromLeft})
                        # rename out dict if same vars coming from left and right
                        if 'cef_white' in colsFromRight:
                            nr = self.renameMatchDict(r, 'cef')
                            newRec.update({k: nr[k] for k in colsFromRight})
                        elif 'syn_white' in colsFromRight:
                            nr = self.renameMatchDict(r, 'syn')
                            newRec.update({k: nr[k] for k in colsFromRight})
                        elif any('mdf' in x for x in colsFromRight):
                            nr = self.renameMatchDict(r, 'mdf')
                            newRec.update({k: nr[k] for k in colsFromRight})
                        elif any(self.rightarg[0:3] in x for x in colsFromRight):
                            nr = self.renameMatchDict(r, self.rightarg[0:3])
                            newRec.update({k: nr[k] for k in colsFromRight})
                        newRec.update({matchFlagName:4})
                        matchDF = matchDF.append(newRec,ignore_index=True) 
                        del leftBlk[i]
                        del rightBlk[j]
                        break
            
        return exactCount, fzyAgeCount, binAgeCount, oneOffCount, matchDF, js

    def validate(self,t,cols2Match):
        """Match function that performs the validation match. Puts matched 
        statistics into doneValQueue.
         
        Args:
            t (tuple): left and right tracts to be matched, (left, right)
            
            cols2Match (list): list of strings with labels of the variables that
            will be matched
        
        """
        # set of matching variables
        left = t[0]
        right = t[1]
        
        # get county, tract
        if len(left) > 0:
            county = left[0]['county']
            tract = left[0]['tract']
        else:
            county = right[0]['county']
            tract = right[0]['tract']
        
        #logging.debug('validate(), {0}, {1}'.format(county, tract))
        
        # initialize stats frame
        blockStats = pd.DataFrame(columns=['county','tract','block',self.leftarg,
            self.rightarg,'exact','fzyage','binage','oneoff','jsresid'])
        
        # initialize val match frame
        matchDFHeader = ['county',
            'tract',
            'block',
            'sex',
            'age',
            'white',
            'black',
            'aian',
            'asian',
            'nhopi',
            'sor',
            'hisp',
            self.rightarg[0:3]+'_sex',
            self.rightarg[0:3]+'_age',
            self.rightarg[0:3]+'_white',
            self.rightarg[0:3]+'_black',
            self.rightarg[0:3]+'_aian',
            self.rightarg[0:3]+'_asian',
            self.rightarg[0:3]+'_nhopi',
            self.rightarg[0:3]+'_sor',
            self.rightarg[0:3]+'_hisp',
            'val_matchflag']
        
        colsFromLeft = ['county','tract','block','sex','age','white','black',
            'aian','asian','nhopi','sor','hisp']
        
        colsFromRight = [self.rightarg[0:3]+'_sex',
            self.rightarg[0:3]+'_age',
            self.rightarg[0:3]+'_white',
            self.rightarg[0:3]+'_black',
            self.rightarg[0:3]+'_aian',
            self.rightarg[0:3]+'_asian',
            self.rightarg[0:3]+'_nhopi',
            self.rightarg[0:3]+'_sor',
            self.rightarg[0:3]+'_hisp']
        matchDF = pd.DataFrame(columns=matchDFHeader)
        
        # match vars
        match2Do = {'exact':False,'fzyage':False,'binage':False,'oneoff':False}
        for m in self.paramDict['validatematchestodo']:
            match2Do[m] = True

        # do match if left and right exist
        if len(left) > 0 and len(right) > 0:
            
            # validate array
            test = [left[0], right[0]]
            for tc in test:
                if 'blockid' not in tc:
                    logging.info('EXIT: NO blockid')
                    logging.info('{0}'.format(tc))
                    sys.exit()
                for p in cols2Match:
                    if p not in tc:
                        logging.info('EXIT: NO {0}'.format(p))
                        logging.info('{0}'.format(tc))
                        sys.exit()
            
            # identify blocks as unit of work
            bothIDs = list(set([x['blockid'] for x in left]) & set([x['blockid'] for x in right])) 
            onlyLeft = list(set([x['blockid'] for x in left]) -set([x['blockid'] for x in right]))
            onlyRight = list(set([x['blockid'] for x in right]) -set([x['blockid'] for x in left]))
            for b in onlyLeft:
                stats = {'county':county,
                        'tract':b[0:6],
                        'block':b[6:],
                        self.leftarg:len([x for x in left if x['blockid']==b]), 
                        self.rightarg:np.nan,
                        'exact':np.nan, 
                        'fzyage':np.nan, 
                        'binage':np.nan, 
                        'oneoff':np.nan,
                        'jsresid':np.nan}
                blockStats = blockStats.append(stats,ignore_index=True)
            for b in onlyRight:
                stats = {'county':county,
                        'tract':b[0:6],
                        'block':b[6:],
                        self.leftarg:np.nan,
                        self.rightarg:len([x for x in right if x['blockid']==b]),
                        'exact':np.nan, 
                        'fzyage':np.nan, 
                        'binage':np.nan, 
                        'oneoff':np.nan,
                        'jsresid':np.nan}
                blockStats = blockStats.append(stats,ignore_index=True)

            # go block by block
            for b in bothIDs:
                # subset to target block 
                leftBlk = [x for x in left if x['blockid']==b]
                rightBlk = [x for x in right if x['blockid']==b]
                #logging.debug('rightBlk[0]: {0}'.format(rightBlk[0]))
                # initialize counters
                leftCount = len(leftBlk)
                rightCount = len(rightBlk)
                
                # do match
                exactCount, fzyAgeCount, binAgeCount, oneOffCount, matchDF, js = self.match(
                    match2Do, cols2Match, leftBlk, rightBlk, colsFromLeft, 
                    colsFromRight, matchDF,'val_matchflag', False)

                # capture stats for block
                stats = {'county':county,
                        'tract':b[0:6],
                        'block':b[6:],
                        self.leftarg:leftCount,
                        self.rightarg:rightCount,
                        'exact':exactCount, 
                        'fzyage':fzyAgeCount, 
                        'binage':binAgeCount, 
                        'oneoff':oneOffCount,
                        'jsresid':js}
                blockStats = blockStats.append(stats,ignore_index=True)
            self.doneStatQueue.put(blockStats)
            self.doneMatchQueue.put(matchDF)
            
            
        # if both frames are empty, return NaN series for results
        else: 
            stats = {'county':county,
                    'tract':tract, 
                    'block':np.nan,
                    self.leftarg:np.nan,
                    self.rightarg:np.nan,
                    'exact':np.nan, 
                    'fzyage':np.nan, 
                    'binage':np.nan, 
                    'oneoff':np.nan,
                    'jsresid':np.nan}
            self.doneValQueue.put(blockStats)


    def sortMatchDicts(self, match2Do, cols2Match, leftBlk, rightBlk):
        """Sort left and right data (list of dictionaries) by the variables to
        be matched.
        
        Args:
            cols2Match (list): list of strings with labels of the variables that
            will be matched
            
            leftBlk (list): list of record dictionaries for left data
            
            rightBlk (list): list of record dictionaries for right data
        
        Returns:
            leftBlk (list): sorted list of record dictionaries for left data
            
            rightBlk (list): sorted list of record dictionaries for right 
            data
            
        """
        
        # sort the lists of dicts
        # if we are doing a putative match, sex and age, then sort by sex, 
        # either age or binage then, for left and for right also by random 
        # uniform
        if cols2Match == ['sex','age']:
            if match2Do['binage'] == True & match2Do['exact'] == False:
                leftBlk = sorted(leftBlk, key=lambda k: '%s %02d' % (k['sex'],k['agebin']))
                rightBlk = sorted(rightBlk, key=lambda k: '%s %02d %.12f' % (k['sex'],k['agebin'],k['r']))
            else:
                leftBlk = sorted(leftBlk, key=lambda k: '%s %03d' % (k['sex'],k['age']))
                rightBlk = sorted(rightBlk, key=lambda k: '%s %03d %.12f' % (k['sex'],k['age'],k['r']))
        if cols2Match == ['sex','age','pik']:
            leftBlk = sorted(leftBlk, key=lambda k: '%s %03d %s' % (k['sex'],k['age'],k['pik']))
            rightBlk = sorted(rightBlk, key=lambda k: '%s %03d %s' % (k['sex'],k['age'],k['pik']))
        if cols2Match == ['sex','age','white','black','aian','asian','nhopi','sor','hisp']:
            leftBlk = sorted(leftBlk, key=lambda k: '%s %03d %s %s %s %s %s %s %s' % 
                (k['sex'],
                k['age'],
                k['white'],
                k['black'],
                k['aian'],
                k['asian'],
                k['nhopi'],
                k['sor'],
                k['hisp']))
            rightBlk = sorted(rightBlk, key=lambda k: '%s %03d %s %s %s %s %s %s %s' % 
                (k['sex'],
                k['age'],
                k['white'],
                k['black'],
                k['aian'],
                k['asian'],
                k['nhopi'],
                k['sor'],
                k['hisp']))
        if cols2Match == ['sex','age','pik','white','black','aian','asian','nhopi','sor','hisp']:
            leftBlk = sorted(leftBlk, key=lambda k: '%s %03d %s %s %s %s %s %s %s %s' % 
                (k['sex'],
                k['age'],
                k['pik'],
                k['white'],
                k['black'],
                k['aian'],
                k['asian'],
                k['nhopi'],
                k['sor'],
                k['hisp']))
            rightBlk = sorted(rightBlk, key=lambda k: '%s %03d %s %s %s %s %s %s %s %s' % 
                (k['sex'],
                k['age'],
                k['pik'],
                k['white'],
                k['black'],
                k['aian'],
                k['asian'],
                k['nhopi'],
                k['sor'],
                k['hisp']))
        return leftBlk, rightBlk

    def renameMatchDict(self,d,prfx):
        """Rename keys in data (d). Necessary when the output data would 
        otherwise have variables with duplicate names.
        
        Args:
            d (dictionary): record dictionary
            
            prfx (string): string to signify how variables will be renamed
            
        Returns: 
            nr (dictionary): record dictionary with renamed keys
            
        """
        
        if prfx == 'cef':
            rename = {'age':'cef_age',
                'sex':'cef_sex',
                'white':'cef_white',
                'black':'cef_black',
                'aian':'cef_aian',
                'asian':'cef_asian',
                'nhopi':'cef_nhopi',
                'sor':'cef_sor',
                'hisp':'cef_hisp'}
        if prfx == 'hdf':
            rename = {'age':'hdf_age',
                'sex':'hdf_sex',
                'white':'hdf_white',
                'black':'hdf_black',
                'aian':'hdf_aian',
                'asian':'hdf_asian',
                'nhopi':'hdf_nhopi',
                'sor':'hdf_sor',
                'hisp':'hdf_hisp'}
        if prfx == 'syn':
            rename = {'age':'syn_age',
                'sex':'syn_sex',
                'white':'syn_white',
                'black':'syn_black',
                'aian':'syn_aian',
                'asian':'syn_asian',
                'nhopi':'syn_nhopi',
                'sor':'syn_sor',
                'hisp':'syn_hisp'}
        if prfx == 'mdf':
            rename = {'age':self.leftarg+'_age',
                'sex':self.leftarg+'_sex',
                'white':self.leftarg+'_white',
                'black':self.leftarg+'_black',
                'aian':self.leftarg+'_aian',
                'asian':self.leftarg+'_asian',
                'nhopi':self.leftarg+'_nhopi',
                'sor':self.leftarg+'_sor',
                'hisp':self.leftarg+'_hisp'}
        if prfx == self.rightarg[0:3]:
            rename = {'age':self.rightarg[0:3]+'_age',
                'sex':self.rightarg[0:3]+'_sex',
                'white':self.rightarg[0:3]+'_white',
                'black':self.rightarg[0:3]+'_black',
                'aian':self.rightarg[0:3]+'_aian',
                'asian':self.rightarg[0:3]+'_asian',
                'nhopi':self.rightarg[0:3]+'_nhopi',
                'sor':self.rightarg[0:3]+'_sor',
                'hisp':self.rightarg[0:3]+'_hisp'}
            
        nr = {}
        # split dict by whether it needs to be renamed
        d1 = {k: d[k] for k in d if k in rename.keys()}
        d2 = {k: d[k] for k in d if k not in rename.keys()}
        # rename
        d1 = dict((rename[k],v) for (k,v) in d1.items() if k in rename.keys())
        # combine
        nr.update(d1)
        nr.update(d2)
        return nr

    def histDiff(self,l,r,cols2Match):
        """Compute the difference in histograms of l and r. Uses jensen-shannon.
        
        Args:
            l (list): list of dictionaries
            
            r (list): list of dictionaries
            
            cols2Match (list): list of strings with labels of the variables that
            will be matched
            
        Returns: 
            js (float): jensen-shannon measure between l and r
            
        """
        #logging.debug('{0}'.format(l[0]))

        for d in [l,r]:
            df = pd.DataFrame(d)
            gdf = df.groupby(cols2Match).size().reset_index().rename(columns={0:'count'})
            gdf['sum'] = gdf['count'].sum()
            gdf['freq'] = gdf['count']/gdf['sum']
            if d == l:
                lhist = gdf[cols2Match+['freq']]
            if d == r:
                rhist = gdf[cols2Match+['freq']]
        
        #logging.debug('lhist: {0}'.format(lhist))
        #logging.debug('rhist: {0}'.format(rhist))

        join = lhist.merge(rhist, on=cols2Match, how='outer', suffixes=['_l','_r'])
        join['freq_l'] = join['freq_l'].fillna(0)
        join['freq_r'] = join['freq_r'].fillna(0)
        
        #innerJoinDebug = lhist.merge(rhist, on=cols2Match, how='inner', suffixes=['_l','_r'])
        #innerJoinDebug['freq_l'] = innerJoinDebug['freq_l'].fillna(0)
        #innerJoinDebug['freq_r'] = innerJoinDebug['freq_r'].fillna(0)
        
        #logging.debug('join: {0}'.format(join))
        #logging.debug('geoid: {0} {1} {2}'.format(l[0]['tract'],l[0]['block'],join))
        #logging.debug('innerJoinDebug: {0}'.format(innerJoinDebug))
        js = spdist.jensenshannon(join['freq_l'],join['freq_r'])
        #logging.debug('js: {0}'.format(js))
        
        return js

    def attachPIK(self,t,cols2Match):
        """Match function that performs the putative match. Puts matched 
        statistics into doneAtchPIKQueue and pik agumented reconstructed data in donePlusQueue. 
         
        Args:
            t (tuple): left and right tracts to be matched, (left, right)
            
            cols2Match (list): list of strings with labels of the variables that
            will be matched
        
        """
        
        # set of matching variables
        left = t[0]
        right = t[1]
        
        # get county, tract
        if len(left) > 0:
            county = left[0]['county']
            tract = left[0]['tract']
        else:
            county = right[0]['county']
            tract = right[0]['tract']
        
        # initialize stats frame
        blockStats = pd.DataFrame(columns=['county','tract','block',self.leftarg,
                                            self.rightarg,'exact','fzyage','binage'])
        # initialize plus frame
        matchDFHeader = ['county','tract','block','sex','age','white','black',
            'aian','asian','nhopi','sor','hisp','pik','put_matchflag']
            
        colsFromLeft = ['county','tract','block','sex','age','white','black',
            'aian','asian','nhopi','sor','hisp']
        colsFromRight = ['pik']
        matchDF = pd.DataFrame(columns=matchDFHeader)
        
        # match vars
        match2Do = {'exact':False,'fzyage':False,'binage':False,'oneoff':False}
        for m in self.paramDict['putativematchestodo']:
            match2Do[m] = True
        
        # exclude records from right that are missing piks
        #right = [x for x in right if x['pik']!='']
        # use keep flag on cef instead, this allows us to use only one block-pik combo
        if self.rightarg == 'cef':
            right = [x for x in right if x['keep']==1] 
        elif self.rightarg == 'cmrcl':
            right = [x for x in right if x['pik']!='']
        
        # do match if left and right exist
        if len(left) > 0 and len(right) > 0:
            
            # validate array
            test = [left[0], right[0]]
            for tc in test:
                if 'blockid' not in tc:
                    logging.info('EXIT: NO blockid')
                    logging.info('{0}'.format(tc))
                    sys.exit()
                for p in cols2Match:
                    if p not in tc:
                        logging.info('EXIT: NO {0}'.format(p))
                        logging.info('{0}'.format(tc))
                        sys.exit()
            
            # identify blocks as unit of work
            bothIDs = list(set([x['blockid'] for x in left]) & set([x['blockid'] for x in right])) 
            onlyLeft = list(set([x['blockid'] for x in left]) -set([x['blockid'] for x in right]))
            onlyRight = list(set([x['blockid'] for x in right]) -set([x['blockid'] for x in left]))
            for b in onlyLeft:
                stats = {'county':county,
                        'tract':b[0:6],
                        'block':b[6:],
                        self.leftarg:len([x for x in left if x['blockid']==b]), 
                        self.rightarg:np.nan,
                        'exact':np.nan, 
                        'fzyage':np.nan, 
                        'binage':np.nan}
                blockStats = blockStats.append(stats,ignore_index=True)
            for b in onlyRight:
                stats = {'county':county,
                        'tract':b[0:6],
                        'block':b[6:],
                        self.leftarg:np.nan,
                        self.rightarg:len([x for x in right if x['blockid']==b]),
                        'exact':np.nan, 
                        'fzyage':np.nan, 
                        'binage':np.nan}
                blockStats = blockStats.append(stats,ignore_index=True)

            # go block by block
            for b in bothIDs:
                # subset to target block, slice mergeVars 
                leftBlk = [x for x in left if x['blockid']==b]
                rightBlk = [x for x in right if x['blockid']==b]
                
                leftCount = len(leftBlk)
                rightCount = len(rightBlk)
                
                exactCount, fzyAgeCount, binAgeCount, oneOffCount, matchDF, js = self.match(match2Do, cols2Match, 
                    leftBlk, rightBlk, colsFromLeft, colsFromRight, matchDF,'put_matchflag',False)
                
                # capture stats for block
                stats = {'county':county,
                        'tract':b[0:6], 
                        'block':b[6:],
                        self.leftarg:leftCount,
                        self.rightarg:rightCount,
                        'exact':exactCount, 
                        'fzyage':fzyAgeCount, 
                        'binage':binAgeCount}
                blockStats = blockStats.append(stats,ignore_index=True)
            # put stats into stats doneQueue
            self.doneStatQueue.put(blockStats)
            self.doneMatchQueue.put(matchDF)
            
        # if one frame is empty, return NaN series for results
        else:
            stats = {'county':county,
                    'tract':tract, 
                    'block':np.nan,
                    self.leftarg:np.nan,
                    self.rightarg:np.nan,
                    'exact':np.nan, 
                    'fzyage':np.nan,
                    'binage':np.nan}
            self.doneStatQueue.put(blockStats)
            
        #logging.debug('attachPIK: Finished {0}, {1}'.format(county,tract))

    def confirmPIK(self,t,cols2Match):
        """Match function that performs the putative match. Puts matched 
        statistics into doneLeftConfQueue and validated data in 
        doneCnfrmPIKQueue. 
         
        Args:
            t (tuple): left and right tracts to be matched, (left, right)
            
            cols2Match (list): list of strings with labels of the variables that
            will be matched
        
        """
        
        # set of matching variables
        left = t[0]
        right = t[1]
        
        # get county
        if len(left) > 0:
            county = left[0]['county']
            tract = left[0]['tract']
        else:
            county = right[0]['county']
            tract = right[0]['tract']
        
        # initialize stats frame
        blockStats = pd.DataFrame(columns=['county','tract','block',self.leftarg+'plus',
                                            'cef','exact','fzyage','binage'])
        
        # initialize leftconf frame
        matchDFHeader = ['county','tract','block','sex','age','white','black',
            'aian','asian','nhopi','sor','hisp','pik','put_matchflag','cef_white',
            'cef_black','cef_aian','cef_asian','cef_nhopi','cef_sor','cef_hisp','conf_matchflag']
        matchDF = pd.DataFrame(columns=matchDFHeader)
            
        colsFromLeft = ['county','tract','block','sex','age','white','black',
            'aian','asian','nhopi','sor','hisp','pik','put_matchflag']
        colsFromRight = ['cef_white',
            'cef_black','cef_aian','cef_asian','cef_nhopi','cef_sor','cef_hisp']
        
        # match vars
        match2Do = {'exact':False,'fzyage':False,'binage':False,'oneoff':False}
        for m in self.paramDict['confirmmatchestodo']:
            match2Do[m] = True
            
        # exclude records from right that are missing piks
        # use keep flag on cef instead, this allows us to use only one block-pik combo
        right = [x for x in right if x['keep']==1] 
        
        # do match if left and right exist
        if len(left) > 0 and len(right) > 0:
            
            # validate array
            test = [left[0], right[0]]
            for tc in test:
                if 'blockid' not in tc:
                    logging.info('EXIT: NO blockid')
                    logging.info('{0}'.format(tc))
                    sys.exit()
                for p in cols2Match:
                    if p not in tc:
                        logging.info('EXIT: NO {0}'.format(p))
                        logging.info('{0}'.format(tc))
                        sys.exit()
            
            # identify blocks as unit of work
            bothIDs = list(set([x['blockid'] for x in left]) & set([x['blockid'] for x in right])) 
            onlyLeft = list(set([x['blockid'] for x in left]) -set([x['blockid'] for x in right]))
            onlyRight = list(set([x['blockid'] for x in right]) -set([x['blockid'] for x in left]))
            
            for b in onlyLeft:
                stats = {'county':county,
                        'tract':b[0:6],
                        'block':b[6:],
                        self.leftarg+'plus':len([x for x in left if x['blockid']==b]),
                        'cef':np.nan,
                        'exact':np.nan, 
                        'fzyage':np.nan, 
                        'binage':np.nan}
                blockStats = blockStats.append(stats,ignore_index=True)
            for b in onlyRight:
                stats = {'county':county,
                        'tract':b[0:6],
                        'block':b[6:],
                        self.leftarg+'plus':np.nan,
                        'cef':len([x for x in right if x['blockid']==b]),
                        'exact':np.nan, 
                        'fzyage':np.nan, 
                        'binage':np.nan}
                blockStats = blockStats.append(stats,ignore_index=True)
            
            # go block by block
            for b in bothIDs:
                
                # subset to target block
                leftBlk = [x for x in left if x['blockid']==b]
                rightBlk = [x for x in right if x['blockid']==b]
                
                # initialize counters
                leftCount = len(leftBlk)
                rightCount = len(rightBlk)
                
                exactCount, fzyAgeCount, binAgeCount, oneOffCount, matchDF, js = self.match(match2Do, cols2Match, 
                    leftBlk, rightBlk, colsFromLeft, colsFromRight, matchDF,'conf_matchflag', False)
                

                # capture stats for block
                stats = {'county':county,
                        'tract':b[0:6],
                        'block':b[6:],
                        self.leftarg+'plus':leftCount,
                        'cef':rightCount,
                        'exact':exactCount, 
                        'fzyage':fzyAgeCount, 
                        'binage':binAgeCount}
                blockStats = blockStats.append(stats,ignore_index=True)
            # put stats into stats doneQueue
            self.doneStatQueue.put(blockStats)
            self.doneMatchQueue.put(matchDF)
            
        # if one frame is empty, return NaN series for results
        else: 
            stats = {'county':county,
                    'tract':tract, 
                    'block':np.nan,
                    self.leftarg+'plus':np.nan,
                    'cef':np.nan,
                    'exact':np.nan, 
                    'fzyage':np.nan, 
                    'binage':np.nan}
            self.doneStatQueue.put(blockStats)
        
            
            