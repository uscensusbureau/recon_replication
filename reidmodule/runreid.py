# -*- coding: utf-8 -*- 
#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)

"""
runs all reid programs: prep, validation, putative match, confirmation match, and analysis

Args: 
    1 (int) - number of worker processes allowed
    2 (str) - prefix of rhdf to process

Example execution: 
    $ setsid /usr/bin/python3 runreid.py 40 r00

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
import glob
import pathlib

# MODULE LEVEL VARIABLES
numWorkers = int(sys.argv[1])
prfx = str(sys.argv[2])
# dir of program
myRoot = os.path.dirname(os.path.abspath(__file__))
# param dictionary
params = json.load(open(myRoot+'/common/config.json','r'))

# date time stamp to be put in outfile names
date = repr(os.popen('date +"%m-%d-%y"').read() +
    os.popen('date +"%T"').read()).replace(r"\n"," ").replace("'","").strip().replace(" ","_")

# import common py
sys.path.insert(0,myRoot+'/common/')
sys.path.insert(0,myRoot+'/01_dataprep/prep_rhdf/')
sys.path.insert(0,myRoot+'/01_dataprep/prep_simul/')
sys.path.insert(0,myRoot+'/02_tabs/')
sys.path.insert(0,myRoot+'/03_agree/')
sys.path.insert(0,myRoot+'/04_putative/')
sys.path.insert(0,myRoot+'/05_confirm/')
sys.path.insert(0,myRoot+'/06_summarize/')

from match import Matcher
from admin import *
from splitter import Splitter
import prep_rhdf
import prep_simul
import tabs
import agree
import putative
import confirm
import summarize

def main():
    """Main body of match program.
    """
    
    # Open log file
    sys.stderr = open(myRoot+'/logs/runreid_{0}_{1}.error'.format(prfx,date),'w')
    logging.basicConfig(filename=
        myRoot+'/logs/runreid_{0}_{1}.log'.format(prfx,date),
        level=logging.DEBUG)

    logging.info('\n\n###### BEGINNING OF RUNREID PROGRAM ######\n\n')

    logging.info('date time: {0}'.format(date))
    logging.info('number of workers: {0}'.format(numWorkers))

    # prep rhdf data
    prep_rhdf.main('r01')
    prep_rhdf.main('r02')
    prep_rhdf.main('r03')
    prep_rhdf.main(prfx)
    prep_simul.main(numWorkers)
    
    # tab rhdf and cef
    tabs.main(numWorkers,prfx)
    tabs.main(numWorkers,'r01')
    tabs.main(numWorkers,'r02')
    tabs.main(numWorkers,'r03')
    tabs.main(numWorkers,'cef')
    tabs.main(numWorkers,'hdf')
    tabs.main(numWorkers,'cmrcl')
    
    # agree match 
    agree.main(numWorkers,'hdf',prfx,"['exact','fzyage','oneoff']")
    agree.main(numWorkers,'cef',prfx,"['exact','fzyage','oneoff']")
    agree.main(numWorkers,'hdf',prfx,"['exact','binage','oneoff']")
    agree.main(numWorkers,'cef',prfx,"['exact','binage','oneoff']")
    agree.main(numWorkers,'cef','r01',"['exact','binage','oneoff']")
    agree.main(numWorkers,'hdf','r01',"['exact','binage','oneoff']")
    agree.main(numWorkers,'cef','r02',"['exact','binage','oneoff']")
    agree.main(numWorkers,'hdf','r02',"['exact','binage','oneoff']")
    agree.main(numWorkers,'cef','r03',"['exact','binage','oneoff']")
    agree.main(numWorkers,'hdf','r03',"['exact','binage','oneoff']")
    agree.main(numWorkers,'cef','hdf',"['exact','binage','oneoff']")
    
    # putative match 
    putative.main(numWorkers,'cef','cef',"['exact','binage']")
    putative.main(numWorkers,'cef','cmrcl',"['exact','binage']")
    putative.main(numWorkers,prfx,'cmrcl',"['exact','binage']")
    putative.main(numWorkers,prfx,'cef',"['exact','binage']")
    putative.main(numWorkers,'hdf','cef',"['exact','binage']")
    putative.main(numWorkers,'hdf','cmrcl',"['exact','binage']")
    putative.main(numWorkers,'r01','cef',"['exact','binage']")
    putative.main(numWorkers,'r01','cmrcl',"['exact','binage']")
    putative.main(numWorkers,'r02','cef',"['exact','binage']")
    putative.main(numWorkers,'r02','cmrcl',"['exact','binage']")
    putative.main(numWorkers,'r03','cef',"['exact','binage']")
    putative.main(numWorkers,'r03','cmrcl',"['exact','binage']")
    putative.main(numWorkers,'modalGsr','cmrcl',"['exact','binage']")
    putative.main(numWorkers,'propGsr','cmrcl',"['exact','binage']")
    putative.main(numWorkers,'modalGsr','cef',"['exact','binage']")
    putative.main(numWorkers,'propGsr','cef',"['exact','binage']")
    
    # confirm match
    confirm.main(numWorkers,'cefcefplusbinage',"['exact','binage']")
    confirm.main(numWorkers,'cefcmrclplusbinage',"['exact','binage']")
    confirm.main(numWorkers,prfx+'cmrclplusbinage',"['exact','binage']")
    confirm.main(numWorkers,prfx+'cefplusbinage',"['exact','binage']")
    confirm.main(numWorkers,'hdfcefplusbinage',"['exact','binage']")
    confirm.main(numWorkers,'hdfcmrclplusbinage',"['exact','binage']")
    confirm.main(numWorkers,'r01cefplusbinage',"['exact','binage']")
    confirm.main(numWorkers,'r01cmrclplusbinage',"['exact','binage']")
    confirm.main(numWorkers,'r02cefplusbinage',"['exact','binage']")
    confirm.main(numWorkers,'r02cmrclplusbinage',"['exact','binage']")
    confirm.main(numWorkers,'r03cefplusbinage',"['exact','binage']")
    confirm.main(numWorkers,'r03cmrclplusbinage',"['exact','binage']")
    confirm.main(numWorkers,'modalGsrcmrclplusbinage',"['exact','binage']")
    confirm.main(numWorkers,'propGsrcmrclplusbinage',"['exact','binage']")
    confirm.main(numWorkers,'modalGsrcefplusbinage',"['exact','binage']")
    confirm.main(numWorkers,'propGsrcefplusbinage',"['exact','binage']")
    
    # match stats
    summarize.main(numWorkers,prfx)
    
    logging.info('\n\n###### END OF RUNREID PROGRAM ######\n\n')
    
if __name__ == '__main__':
    main()
