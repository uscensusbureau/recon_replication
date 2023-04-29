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

class Stats(object):
    """Class to organize tabulations of county-level datasets. 
    
    Args:
        lock (mp.Lock()): lock used to modify shared number of tracts processed.
    
        params (dict): dictionary of params (from json)
    
    """
    
    def __init__(self,params,rhdf):
        self.m = mp.Manager()
        self.modalreQueue = self.m.Queue()
        self.onetonQueue = self.m.Queue()
        self.rhdf = rhdf
        self.paramDict = params
        self.t0 = time.time()
        self.xlWriter = pd.ExcelWriter(self.paramDict['rhdfbasersltdir']+self.rhdf+'/'+self.rhdf+'_stats.xlsx')
        
    def logTime(self):
        """Write time lapsed to log file.
        
        """
        
        t  =  time.time() - self.t0
        logging.info('Minutes lapsed: {0}'.format(t/60))

    def binAge(self,series):
        """Translates single-year age value into bins consistent with tabulations.  
         DUPLICATED IN Matcher.binAge()
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
            
    def blocksizeGroups(self,df,popVar):
        """Write time lapsed to log file.
        
        """
        df['blocksize'] = pd.cut(df[popVar],bins=[0,9,49,99,249,499,999,1000000],labels=['1-9','10-49','50-99','100-249','250-499','500-999','1000+'])
        return df

        
    def loopRECols(self,df,reCols):
        """
        
        """
        df['modalre'] = ''
        df['maxmodal'] = 0
        df['totalmodal'] = 0 
        df['natmodalpop'] = 0
        df['natmodalpopre'] = 0
        for re in reCols:
            # if its the first positive, increment total, save the re too
            df.loc[(df[re]>0) & (df.maxmodal==0),'totalmodal'] += 1
            # save the national re too
            df.loc[(df[re]>0) & (df.maxmodal==0),'natmodalpop'] = self.natpop['value'][re]
            df.loc[(df[re]>0) & (df.maxmodal==0),'natmodalpopre'] = re
            
            # if its a tie with current max and max is positive, increment total
            df.loc[(df[re]==df.maxmodal) & (df.maxmodal>0),'totalmodal'] += 1
            df.loc[(df[re]==df.maxmodal) & (df.maxmodal>0) & (self.natpop['value'][re]>df.natmodalpop),'natmodalpopre'] = re
            df.loc[(df[re]==df.maxmodal) & (df.maxmodal>0) & (self.natpop['value'][re]>df.natmodalpop),'natmodalpop'] = self.natpop['value'][re]
            
            # if new re is greater than max, and max is positive, reset total to 1, reset list
            df.loc[(df[re]>df.maxmodal) & (df.maxmodal>0),'totalmodal'] = 1
            df.loc[(df[re]>df.maxmodal) & (df.maxmodal>0),'natmodalpopre'] = re
            df.loc[(df[re]>df.maxmodal) & (df.maxmodal>0),'natmodalpop'] = self.natpop['value'][re]
            
            # if re is greater than max, capture the varname
            df.loc[(df[re]>df.maxmodal),'modalre'] = re
            # if re is greater than max, set maxmodal to the pop count
            df.loc[(df[re]>df.maxmodal),'maxmodal'] = df[re]

        return df
    
    def findModalRE(self):
        """
        
        """
        cefTab = pd.read_csv(self.paramDict['rsltbase']+'/cef/cef_block_counts.csv',dtype={'geoid_block':'str'})
        cefTab = cefTab[cefTab.geoid_block.str.slice(0,2)!='72']
        cefTab['geoid_blockgroup'] = cefTab.geoid_block.str.slice(0,12)
        reCols = [x.replace('freq','') for x in cefTab.columns if 'freq' in x]
        cefTab.columns = [x.replace('freq','') for x in cefTab.columns]
        # zero fill re cols
        for re in reCols:
            cefTab.loc[cefTab[re].isnull(),re] = 0
        # subset to cols we need
        cefTab = cefTab[['geoid_block','geoid_blockgroup']+reCols]
        # sum to block groups
        bgTotals = cefTab.groupby('geoid_blockgroup')[reCols].sum()
        # find block group modal
        bgModal = self.loopRECols(bgTotals,reCols)
        bgModal = bgModal.rename({'maxmodal':'maxmodalbg','totalmodal':'totalmodalbg','modalre':'modalrebg'},axis=1)
        bgModal = bgModal.drop(['natmodalpop','natmodalpopre'], axis=1)
        bgModal = bgModal.reset_index().rename(columns={0:'geoid_blockgroup'})[['geoid_blockgroup','modalrebg','maxmodalbg','totalmodalbg']]
        # find block modal 
        bModal = self.loopRECols(cefTab,reCols)
        bModal = bModal[['geoid_block','geoid_blockgroup','modalre','maxmodal','totalmodal','natmodalpopre']]
        # combine block and blockgroup modal
        bModal = pd.merge(bModal,bgModal,how='left',on=['geoid_blockgroup'],indicator=False)
        bModal['type'] = 'block'
        # use bg modal if no block-level modal
        bModal.loc[(bModal.modalre=='') & (bModal.modalrebg!=''),'type'] = 'blockgroup'
        bModal.loc[(bModal.modalre=='') & (bModal.modalrebg!=''),'modalre'] = bModal.modalrebg
        # use bg modal if modal pop is 1
        bModal.loc[(bModal.maxmodal==1) & (bModal.maxmodalbg>1),'type'] = 'blockgroup'
        bModal.loc[(bModal.maxmodal==1) & (bModal.maxmodalbg>1),'modalre'] = bModal.modalrebg
        # no modal if both pop are 1
        bModal.loc[(bModal.maxmodal==1) & (bModal.maxmodalbg==1),'modalre'] = ''
        # use bg modal if tie at block-level but no tie at bg level
        bModal.loc[(bModal.totalmodal>1) & (bModal.totalmodalbg==1),'type'] = 'blockgroup'
        bModal.loc[(bModal.totalmodal>1) & (bModal.totalmodalbg==1),'modalre'] = bModal.modalrebg
        # no modal if tie at both bg and block level
        bModal.loc[(bModal.totalmodal>1) & (bModal.totalmodalbg>1),'modalre'] = ''
        # finally, use national if nothing else        
        bModal.loc[(bModal.modalre==''),'type'] = 'national'
        bModal.loc[(bModal.modalre==''),'modalre'] = bModal.natmodalpopre
        
        bModal = bModal[['geoid_block','modalre','type']]
        # output combined data
        outFile = self.paramDict['rsltbase'] + '/cef/cef_modalre.csv'
        bModal.to_csv(outFile,index=False)

    def modalREWorker(self):
        """.
        
        Args:

        """
        
        # what each worker does, pulling data and flagging modalre
        churn = True
        while churn:
            # Pull the next county
            try:
                q = self.modalreQueue.get(timeout = 15)
            except:
                # If no work can be pulled from queue, break
                logging.info('Worker queue timeout---------------')
                logging.info('modalreQueue size: {0}'.format(self.modalreQueue.qsize()))
                break
            # tuples in queue contain county [0]
            self.flagModalrePIK(q)
            
        
    def flagModalrePIK(self, county):
        """
         
        Args:
        
        Returns:
        
        """
        # get data
        inDF = pd.read_csv(self.paramDict['cefdir']+'/cef'+county+'.csv',dtype=str)     
        inDF['ageint'] = inDF['age'].astype(int)
        inDF['agebin'] = inDF['ageint'].apply(self.binAge)
        inDF['agebin'] = inDF['agebin'].astype(str)
        # modal re
        inDF['re'] = inDF.white + inDF.black + inDF.aian + inDF.asian + inDF.nhopi + inDF.sor + inDF.hisp
        inDF = pd.merge(inDF,self.bModal,how='left',on=['geoid_block'],indicator=False)
        inDF = inDF.rename({'modalre':'modalre_str'},axis=1)
        inDF['modalre'] = np.nan
        inDF.loc[(inDF.re!=inDF.modalre_str) & (inDF.modalre_str.notnull()),'modalre'] = 0
        inDF.loc[inDF.re==inDF.modalre_str,'modalre'] = 1
        # over/under 21
        inDF['over21'] = 0
        inDF.loc[inDF.ageint>21,'over21'] = 1
        # sab onetons
        inDF['sab'] = inDF.sex + inDF.age + inDF.geoid_block
        inDF['sabC'] = inDF.groupby('sab')['sab'].transform('count')
        inDF['sab_oneton'] = 0
        inDF.loc[inDF.sabC==1,'sab_oneton'] = 1 
        # sabb onetons
        inDF['sabb'] = inDF.sex + inDF.agebin + inDF.geoid_block
        inDF['sabbC'] = inDF.groupby('sabb')['sabb'].transform('count')
        inDF['sabb_oneton'] = 0
        inDF.loc[inDF.sabbC==1,'sabb_oneton'] = 1 
        # ber onetons
        inDF['ber'] = inDF.geoid_block + inDF.white + inDF.black + inDF.aian + inDF.asian + inDF.nhopi + inDF.sor + inDF.hisp
        inDF['berC'] = inDF.groupby('ber')['ber'].transform('count')
        inDF['ber_oneton'] = 0
        inDF.loc[inDF.berC==1,'ber_oneton'] = 1 
        
        # restrict to keep 
        inDF = inDF.loc[inDF.keep=='1']
        inDF = inDF[['geoid_block','pik','over21','modalre','sab_oneton','sabb_oneton','ber_oneton']]
        # output
        outFile = self.paramDict['rsltbase'] + '/cef/cef_over21_modalre_oneton_'+county+'.csv'
        inDF.to_csv(outFile,index=False)
        
        
    def agreeStats(self):
        """Agreement match statistics.
        
        """
        agreeNatStat = pd.DataFrame(columns=['lbl','insrc','inrecon','exact','fzyage','binage','oneerror'])
        if self.paramDict['doAgreeFzy']:
            ahdf = pd.read_csv(self.paramDict['rhdfbasersltdir']+self.rhdf+'/'+'agree_hdf_'+self.rhdf+'.csv')
            ahdf['lbl'] = 'HDF'
            ahdf['insrc'] = ahdf['hdf']
            ahdf = self.blocksizeGroups(ahdf,'insrc')
            acef = pd.read_csv(self.paramDict['rhdfbasersltdir']+self.rhdf+'/'+'agree_cef_'+self.rhdf+'.csv')
            acef['lbl'] = 'CEF'
            acef['insrc'] = acef['cef']
            acef = self.blocksizeGroups(acef,'insrc')
            for df in [ahdf,acef]:
                df['inrecon'] = df[self.rhdf]
                df['fzyage'] = df['exact'] + df['fzyage']
                df['oneoff'] = df['fzyage'] + df['oneoff']
                agb = df.groupby('lbl')[['insrc','inrecon','exact','fzyage','oneoff']].sum().reset_index()
                agb = agb.rename({'oneoff':'oneerror'},axis=1)
                agreeNatStat = agreeNatStat.append(agb)
                
        if self.paramDict['doAgreeBin']:
            abhdf = pd.read_csv(self.paramDict['rhdfbasersltdir']+self.rhdf+'/'+'agreebinage_hdf_'+self.rhdf+'.csv')
            abhdf['lbl'] = 'HDF'
            abhdf['insrc'] = abhdf['hdf']
            abhdf = self.blocksizeGroups(abhdf,'insrc')
            abcef = pd.read_csv(self.paramDict['rhdfbasersltdir']+self.rhdf+'/'+'agreebinage_cef_'+self.rhdf+'.csv')
            abcef['lbl'] = 'CEF'
            abcef['insrc'] = abcef['cef']
            abcef = self.blocksizeGroups(abcef,'insrc')
            for df in [abhdf,abcef]:
                df['inrecon'] = df[self.rhdf]
                df['binage'] = df['exact'] + df['binage']
                df['oneoff'] = df['binage'] + df['oneoff']
                agb = df.groupby('lbl')[['insrc','inrecon','exact','binage','oneoff']].sum().reset_index()
                agb = agb.rename({'oneoff':'oneerror'},axis=1)
                agreeNatStat = agreeNatStat.append(agb)
        agreeNatStat.to_excel(self.xlWriter,sheet_name='agreerate',index=False)
        
        if self.paramDict['doAgreeFzy']:
            agreeBlockStat = pd.DataFrame(columns=['blocksize','pop','exacthdf','fzyagehdf'])
            agb = ahdf.groupby('blocksize')[['insrc','exact','fzyage']].sum().reset_index()
            agb = agb.rename({'exact':'exacthdf','fzyage':'fzyagehdf','insrc':'pop'},axis=1)
            agreeBlockStat = agreeBlockStat.append(agb)
            agb = acef.groupby('blocksize')[['exact','fzyage']].sum().reset_index()
            agb = agb.rename({'exact':'exactcef','fzyage':'fzyagecef'},axis=1)
            agreeBlockStat = pd.merge(agreeBlockStat,agb,how='left',on='blocksize')
            agreeBlockStat.to_excel(self.xlWriter,sheet_name='agreerate_blocksize',index=False)
        
        if self.paramDict['doAgreeBin']:
            agreeBlockStat = pd.DataFrame(columns=['blocksize','pop','exacthdf','binagehdf'])
            agb = abhdf.groupby('blocksize')[['insrc','exact','binage']].sum().reset_index()
            agb = agb.rename({'exact':'exacthdf','binageage':'binagehdf','insrc':'pop'},axis=1)
            agreeBlockStat = agreeBlockStat.append(agb)
            agb = abcef.groupby('blocksize')[['exact','binage']].sum().reset_index()
            agb = agb.rename({'exact':'exactcef','binage':'binagecef'},axis=1)
            agreeBlockStat = pd.merge(agreeBlockStat,agb,how='left',on='blocksize')
            agreeBlockStat.to_excel(self.xlWriter,sheet_name='agreerate_blocksize_binage',index=False)
        
        
    def putativeConfirmStats(self):
        """Putative and confirm match statistics.
        
        """
        
        # prep tabs
        cefTab = pd.read_csv(self.paramDict['rsltbase']+'/cef/cef_block_counts.csv',dtype={'geoid_block':'str'})
        cefTab = cefTab[cefTab.geoid_block.str.slice(0,2)!='72']
        validBlocks = list(set(cefTab.geoid_block))
        cefTab['lbl'] = 'rHDF-CEF'
        cefTab['availrec'] = cefTab['pop']
        cefTab['haspik'] = cefTab['hasall3']
        cefTab = self.blocksizeGroups(cefTab,'pop')
        cefcnt = cefTab.groupby('lbl')[['haspik','availrec']].sum().reset_index()
        cefblkcnt = cefTab.groupby('blocksize')[['haspik','availrec']].sum().reset_index()
        
        cmrclTab = pd.read_csv(self.paramDict['rsltbase']+'/cmrcl/cmrcl_block_counts.csv',dtype={'geoid_block':'str'})
        cmrclTab = cmrclTab[cmrclTab['geoid_block'].isin(validBlocks)]
        cmrclTab['lbl'] = 'rHDF-CMRCL'
        cmrclTab['availrec'] = cmrclTab['pop']
        cmrclTab['haspik'] = cmrclTab['hasall3']
        cmrclTab = pd.merge(cmrclTab,cefTab[['geoid_block','blocksize']],how='left',on='geoid_block')
        cmrclcnt = cmrclTab.groupby('lbl')[['haspik','availrec']].sum().reset_index()
        cmrclblkcnt = cmrclTab.groupby('blocksize')[['haspik','availrec']].sum().reset_index()
        
        
        # prep the matched data
        if self.paramDict['doPutativeFzy']:
            pcmrcl = pd.read_csv(self.paramDict['rhdfbasersltdir']+self.rhdf+'/'+'putative_'+self.rhdf+'_cmrcl.csv',dtype={'county':'str','tract':'str','block':'str'})
            pcmrcl['lbl'] = 'rHDF-CMRCL'
            pcmrcl['geoid_block'] = pcmrcl.county + pcmrcl.tract + pcmrcl.block
            pcmrcl = pd.merge(pcmrcl,cefTab[['geoid_block','blocksize']],how='left',on='geoid_block')
            
            ccmrcl = pd.read_csv(self.paramDict['rhdfbasersltdir']+self.rhdf+'/'+'confirm_'+self.rhdf+'cmrcl_cef.csv',dtype={'county':'str','tract':'str','block':'str'})
            ccmrcl['lbl'] = 'rHDF-CMRCL'
            ccmrcl['geoid_block'] = ccmrcl.county + ccmrcl.tract + ccmrcl.block
            ccmrcl = pd.merge(ccmrcl,cefTab[['geoid_block','blocksize']],how='left',on='geoid_block')
        
            pcef = pd.read_csv(self.paramDict['rhdfbasersltdir']+self.rhdf+'/'+'putative_'+self.rhdf+'_cef.csv',dtype={'county':'str','tract':'str','block':'str'})
            pcef['lbl'] = 'rHDF-CEF'
            pcef['geoid_block'] = pcef.county + pcef.tract + pcef.block
            pcef = pd.merge(pcef,cefTab[['geoid_block','blocksize']],how='left',on='geoid_block')
            
            ccef = pd.read_csv(self.paramDict['rhdfbasersltdir']+self.rhdf+'/'+'confirm_'+self.rhdf+'cef_cef.csv',dtype={'county':'str','tract':'str','block':'str'})
            ccef['lbl'] = 'rHDF-CEF'
            ccef['geoid_block'] = ccef.county + ccef.tract + ccef.block
            ccef = pd.merge(ccef,cefTab[['geoid_block','blocksize']],how='left',on='geoid_block')
            
            for df in [pcmrcl,pcef]:
                df['inrecon'] = df[self.rhdf]
                df['putative'] = df['exact'] + df['fzyage']
            for df in [ccmrcl,ccef]:
                df['confirmed'] = df['exact'] + df['fzyage']
            for df in [pcmrcl,ccmrcl,pcef,ccef]:
                df['fzyage'] = df['exact'] + df['fzyage']
        
        if self.paramDict['doPutativeBin']:
            pbcmrcl = pd.read_csv(self.paramDict['rhdfbasersltdir']+self.rhdf+'/'+'putativebinage_'+self.rhdf+'_cmrcl.csv',dtype={'county':'str','tract':'str','block':'str'})
            pbcmrcl['lbl'] = 'rHDF-CMRCL (binage)'
            pbcmrcl['geoid_block'] = pbcmrcl.county + pbcmrcl.tract + pbcmrcl.block
            pbcmrcl = pd.merge(pbcmrcl,cefTab[['geoid_block','blocksize']],how='left',on='geoid_block')
            
            cbcmrcl = pd.read_csv(self.paramDict['rhdfbasersltdir']+self.rhdf+'/'+'confirmbinage_'+self.rhdf+'cmrcl_cef.csv',dtype={'county':'str','tract':'str','block':'str'})
            cbcmrcl['lbl'] = 'rHDF-CMRCL (binage)'
            cbcmrcl['geoid_block'] = cbcmrcl.county + cbcmrcl.tract + cbcmrcl.block
            cbcmrcl = pd.merge(cbcmrcl,cefTab[['geoid_block','blocksize']],how='left',on='geoid_block')
        
            pbcef = pd.read_csv(self.paramDict['rhdfbasersltdir']+self.rhdf+'/'+'putativebinage_'+self.rhdf+'_cef.csv',dtype={'county':'str','tract':'str','block':'str'})
            pbcef['lbl'] = 'rHDF-CEF (binage)'
            pbcef['geoid_block'] = pbcef.county + pbcef.tract + pbcef.block
            pbcef = pd.merge(pbcef,cefTab[['geoid_block','blocksize']],how='left',on='geoid_block')
            
            cbcef = pd.read_csv(self.paramDict['rhdfbasersltdir']+self.rhdf+'/'+'confirmbinage_'+self.rhdf+'cef_cef.csv',dtype={'county':'str','tract':'str','block':'str'})
            cbcef['lbl'] = 'rHDF-CEF (binage)'
            cbcef['geoid_block'] = cbcef.county + cbcef.tract + cbcef.block
            cbcef = pd.merge(cbcef,cefTab[['geoid_block','blocksize']],how='left',on='geoid_block')
            
            for df in [pbcmrcl,pbcef]:
                df['inrecon'] = df[self.rhdf]
                df['putative'] = df['exact'] + df['binage']
            for df in [cbcmrcl,cbcef]:
                df['confirmed'] = df['exact'] + df['binage']
            for df in [pbcmrcl,cbcmrcl,pbcef,cbcef]:
                df['binage'] = df['exact'] + df['binage']
            
        # collapse and sum
        putconfNatStat = pd.DataFrame(columns=['lbl','availrec','haspik','putative','confirmed'])
        if self.paramDict['doPutativeFzy']:
            putconfNatStat = putconfNatStat.append(pcmrcl.groupby('lbl')[['putative']].sum().reset_index())
            putconfNatStat = putconfNatStat.combine_first(ccmrcl.groupby('lbl')[['confirmed']].sum().reset_index())
            putconfNatStat = putconfNatStat.combine_first(cmrclcnt)
            
            putconfNatStat = putconfNatStat.append(pcef.groupby('lbl')[['putative']].sum().reset_index())
            putconfNatStat = putconfNatStat.combine_first(ccef.groupby('lbl')[['confirmed']].sum().reset_index())
            putconfNatStat = putconfNatStat.combine_first(cefcnt)
            putconfNatStat = putconfNatStat[['lbl','availrec','haspik','putative','confirmed']]
        if self.paramDict['doPutativeBin']:
            putconfNatStat = putconfNatStat.append(pbcmrcl.groupby('lbl')[['putative']].sum().reset_index())
            putconfNatStat = putconfNatStat.combine_first(cbcmrcl.groupby('lbl')[['confirmed']].sum().reset_index())
            putconfNatStat = putconfNatStat.combine_first(cmrclcnt)
            
            putconfNatStat = putconfNatStat.append(pbcef.groupby('lbl')[['putative']].sum().reset_index())
            putconfNatStat = putconfNatStat.combine_first(cbcef.groupby('lbl')[['confirmed']].sum().reset_index())
            putconfNatStat = putconfNatStat.combine_first(cefcnt)
            putconfNatStat = putconfNatStat[['lbl','availrec','haspik','putative','confirmed']]
            
        putconfNatStat.to_excel(self.xlWriter,sheet_name='putconf',index=False)
        
        if self.paramDict['doPutativeFzy']:
            putconfBlockStatCmrcl = pd.DataFrame(columns=['blocksize','pop','availrec','haspik','putative','confirmed'])
            putconfBlockStatCmrcl = putconfBlockStatCmrcl.append(pcmrcl.groupby('blocksize')[['putative']].sum().reset_index())
            putconfBlockStatCmrcl = putconfBlockStatCmrcl.combine_first(ccmrcl.groupby('blocksize')[['confirmed']].sum().reset_index())
            putconfBlockStatCmrcl = putconfBlockStatCmrcl.combine_first(cmrclblkcnt)
            putconfBlockStatCmrcl = putconfBlockStatCmrcl.combine_first(cefTab.groupby('blocksize')[['pop']].sum().reset_index())
            putconfBlockStatCmrcl = putconfBlockStatCmrcl[['blocksize','pop','availrec','haspik','putative','confirmed']]
            putconfBlockStatCmrcl.to_excel(self.xlWriter,sheet_name='putconf_cmrcl_blksz',index=False)
            
            putconfBlockStatCef = pd.DataFrame(columns=['blocksize','pop','availrec','haspik','putative','confirmed'])
            putconfBlockStatCef = putconfBlockStatCef.append(pcef.groupby('blocksize')[['putative']].sum().reset_index())
            putconfBlockStatCef = putconfBlockStatCef.combine_first(ccef.groupby('blocksize')[['confirmed']].sum().reset_index())
            putconfBlockStatCef = putconfBlockStatCef.combine_first(cefblkcnt)
            putconfBlockStatCef = putconfBlockStatCef.combine_first(cefTab.groupby('blocksize')[['pop']].sum().reset_index())
            putconfBlockStatCef = putconfBlockStatCef[['blocksize','pop','availrec','haspik','putative','confirmed']]
            putconfBlockStatCef.to_excel(self.xlWriter,sheet_name='putconf_cef_blksz',index=False)
            
        if self.paramDict['doPutativeBin']:
            putconfBlockStatCmrcl = pd.DataFrame(columns=['blocksize','pop','availrec','haspik','putative','confirmed'])
            putconfBlockStatCmrcl = putconfBlockStatCmrcl.append(pbcmrcl.groupby('blocksize')[['putative']].sum().reset_index())
            putconfBlockStatCmrcl = putconfBlockStatCmrcl.combine_first(cbcmrcl.groupby('blocksize')[['confirmed']].sum().reset_index())
            putconfBlockStatCmrcl = putconfBlockStatCmrcl.combine_first(cmrclblkcnt)
            putconfBlockStatCmrcl = putconfBlockStatCmrcl.combine_first(cefTab.groupby('blocksize')[['pop']].sum().reset_index())
            putconfBlockStatCmrcl = putconfBlockStatCmrcl[['blocksize','pop','availrec','haspik','putative','confirmed']]
            putconfBlockStatCmrcl.to_excel(self.xlWriter,sheet_name='putconf_cmrcl_blksz_binage',index=False)
            
            putconfBlockStatCef = pd.DataFrame(columns=['blocksize','pop','availrec','haspik','putative','confirmed'])
            putconfBlockStatCef = putconfBlockStatCef.append(pbcef.groupby('blocksize')[['putative']].sum().reset_index())
            putconfBlockStatCef = putconfBlockStatCef.combine_first(cbcef.groupby('blocksize')[['confirmed']].sum().reset_index())
            putconfBlockStatCef = putconfBlockStatCef.combine_first(cefblkcnt)
            putconfBlockStatCef = putconfBlockStatCef.combine_first(cefTab.groupby('blocksize')[['pop']].sum().reset_index())
            putconfBlockStatCef = putconfBlockStatCef[['blocksize','pop','availrec','haspik','putative','confirmed']]
            putconfBlockStatCef.to_excel(self.xlWriter,sheet_name='putconf_cef_blksz_binage',index=False)

    
    def cmrclQualityDataPrep(self,df):
        """
         
        Args:
        
        Returns:
        
        """
        df = df[['geoid_block','pik','sex','age']]
        for v in ['sex','age']:
            df[v] = df[v].astype(float)
        df['pik'] = df['pik'].astype(str)
        df.loc[df.pik=='nan','pik']=''   
        df['agebin'] = df['age'].apply(self.binAge)
        return df
    
    def cmrclQuality(self):
        """
         
        Args:
        
        Returns:
        
        """
        # get valid blocks
        validBlocks = pd.read_csv(self.paramDict['geolookup']+'cefblks.csv',dtype=str)
        validBlocks['geoid_block'] = validBlocks.TABBLKST + validBlocks.TABBLKCOU + validBlocks.TABTRACTCE + validBlocks.TABBLK
        validBlocks = validBlocks[['geoid_block']]
        
        # start empty csv for block-pik data
        pikFlags = pd.DataFrame(columns=['geoid_block','pik','bp','bpas','bpabs'])
        pikOutFile = self.paramDict['rsltbase'] + '/cmrcl/cmrcl_cef_bpas_match.csv'
        pikFlags.to_csv(pikOutFile,index=False)
        
        # loop through all counties
        # capture stats for block
        blockStats = pd.DataFrame(columns=['geoid_block','cef_all','cef_keep','cmrcl_all','cmrcl_has_bp','cmrcl_has_bpas','bp','bpas','bpabs'])
        blockCounter = 1
        with open(self.paramDict['geolookup']+'allcounties.txt') as f:
            for line in f:
                county = line.replace('\n','')
                if county[0:2] != '72':
                    logging.info('Starting cmrclQuality: {0}, {1}'.format(blockCounter,county))
                    # get data
                    inCMRCL = pd.read_csv(self.paramDict['cmrcldir']+'/cmrcl'+county+'.csv',dtype=str)
                    # limit to valid blocks
                    inCMRCL = inCMRCL.merge(validBlocks, on=['geoid_block'], how='inner')
                    inCMRCL = self.cmrclQualityDataPrep(inCMRCL)
                    
                    inCEF = pd.read_csv(self.paramDict['cefdir']+'/cef'+county+'.csv',dtype=str)
                    # cef counts
                    inCEFCounts = inCEF.copy(deep=True)
                    inCEFCounts['cef_all'] = 1
                    inCEFCounts['cef_keep'] = 0
                    inCEFCounts.loc[(inCEFCounts.keep=='1'),'cef_keep'] = 1
                    inCEFCountsGB = inCEFCounts.groupby('geoid_block')[['cef_all','cef_keep']].sum().reset_index()
                    
                    # only cef keep records for matching
                    inCEF = inCEF[inCEF['keep']=='1']
                    inCEF = self.cmrclQualityDataPrep(inCEF)
                    
                    # counts on both datasets
                    inCMRCLCounts = inCMRCL.copy(deep=True)
                    inCMRCLCounts['cmrcl_all'] = 1
                    inCMRCLCounts['cmrcl_has_bp'] = 0
                    inCMRCLCounts.loc[(inCMRCLCounts.geoid_block.notnull()) & (inCMRCLCounts.pik.notnull()),'cmrcl_has_bp'] = 1
                    inCMRCLCounts['cmrcl_has_bpas'] = 0
                    inCMRCLCounts.loc[(inCMRCLCounts.geoid_block.notnull()) & (inCMRCLCounts.pik.notnull()) & (inCMRCLCounts.age.notnull()) & (inCMRCLCounts.sex.notnull()),'cmrcl_has_bpas'] = 1
                    
                    inCMRCLCountsGB = inCMRCLCounts.groupby('geoid_block')[['cmrcl_all','cmrcl_has_bp','cmrcl_has_bpas']].sum().reset_index()
                    
                    
                    # matches by block-pik
                    mBP = pd.merge(inCMRCL, inCEF, on=['geoid_block','pik'], how='inner')[['geoid_block','pik']]
                    mBP['bp'] = 1
                    mBPCounts = mBP.groupby('geoid_block')['bp'].sum().reset_index()
                    
                    mBPAS = pd.merge(inCMRCL, inCEF, on=['geoid_block','pik','age','sex'], how='inner')[['geoid_block','pik']]
                    mBPAS['bpas'] = 1
                    mBPASCounts = mBPAS.groupby('geoid_block')['bpas'].sum().reset_index()
                    
                    mBPAbS = pd.merge(inCMRCL, inCEF, on=['geoid_block','pik','agebin','sex'], how='inner')[['geoid_block','pik']]
                    mBPAbS['bpabs'] = 1
                    mBPAbSCounts = mBPAbS.groupby('geoid_block')['bpabs'].sum().reset_index()
                    
                    stats = mBPCounts.merge(mBPASCounts,on='geoid_block',how='outer')
                    stats = stats.merge(mBPAbSCounts,on='geoid_block',how='outer')
                    stats = stats.merge(inCEFCountsGB,on='geoid_block',how='outer')
                    stats = stats.merge(inCMRCLCountsGB,on='geoid_block',how='outer')
                    stats = stats[['geoid_block','cef_all','cef_keep','cmrcl_all','cmrcl_has_bp','cmrcl_has_bpas','bp','bpas','bpabs']]
                    blockStats = blockStats.append(stats,ignore_index=True)
                    
                    pikFlags = mBP.merge(mBPAS, on=['geoid_block','pik'], how='outer')
                    pikFlags = pikFlags.merge(mBPAbS, on=['geoid_block','pik'], how='outer')
                    pikFlags.to_csv(pikOutFile, mode='a', index=False, header=False)
            
                blockCounter += 1
                    
        outFile = self.paramDict['rsltbase'] + '/cmrcl/cmrcl_cef_bpas_match_counts.csv'
        blockStats.to_csv(outFile,index=False)
        
        
