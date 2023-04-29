# config.py
#-----------------------------
# /usr/bin/env python3 config.py
# normal execution is via call in runall.py
#-----------------------------
# Created By: Andrew Sillers, David Zhou, Paul Bartholomew, Grace Tang
# Created Date: 5/31/2021
# version = '1.0'

###############################################################################
"""
NOTICE
This (software/technical data) was produced for the U. S. Government under Contract Number TIRNO-99-D-00005, 
and is subject to Federal Acquisition Regulation Clause 52.227-14, 
Rights in Data—General, Alt. II, III and IV (DEC 2007) [Reference 27.409(a)].  
No other use other than that granted to the U. S. Government, 
or to those acting on behalf of the U. S. Government under that Clause is 
authorized without the express written permission of The MITRE Corporation. 
For further information, please contact The MITRE Corporation, 
Contracts Management Office, 
7515 Colshire Drive, 
McLean, VA  22102-7539, 
(703) 983-6000.
COPYRIGHT 2017 The MITRE Corporation
"""
###############################################################################
# 

# Description:
'''
config.py:

This code is a translation of existing research STATA code developed by MITRE for the purposes of testing
the corresponding reidentification ouputs. This python file is the translation of "0.config.do", the primary
configuration source for the subsequent operations; including global variables, helper functions, and setting
which stats to aggregate.

Previous STATA Code Header:
[There was no code header]
'''

import os, argparse
import pandas as pd
from ctools.s3 import s3open

parser = argparse.ArgumentParser()
parser.add_argument("--setup-only")
parser.add_argument("reid_root", default="/data/reid_module")
args = parser.parse_args()
REID_ROOT = args.reid_root

# DIRS
#REID_ROOT = os.environ.get("RUNNER_REID_SOURCE", "/data/reid_module")

cefdir=REID_ROOT+"/cef"
hdfdir=REID_ROOT+"/hdf"
cmrcldir=REID_ROOT+"/cmrcl"
rhdfbasedir=REID_ROOT+"/rhdf"
rhdfbasersltdir=REID_ROOT+"/results/rhdf"
rsltbase=REID_ROOT+"/results"
solvar=REID_ROOT+"/solvar"
geolookup=REID_ROOT+"/geo"
tbl="../results/tables"
fig="../results/figures"
temp="temp"
disclosure="results/CBDRB-FY22-DSEP-004"
pop2010=308745538
adoloc="./ado/"

# PARAMS
seed=20210730
rhdf="r00"
rhdfbo="r01"
rmdf="r02"
mdf="r03"

notestex="CBDRB-FY22-DSEP-004.tex"
#* if trainingWheels is blank ("") then it will process all data
#* if trainingWheels is "rowrange(:10000)" then it will only pull a small number of records for testing
trainingWheels = False
trainingWheelsCount = 100000
#--------------------------------------------------
# Helper Function: get_df
#--------------------------------------------------
# Description:
#   Common function to retrieve a dataframe that contatins data associated with a specific file path.
#
# Input:
#   A path to a specific .csv file
#
# Output:
#   A dataframe read from the provided path
def get_df(path, trainingWheels, **kwargs):
    if not path.startswith("s3:"):
        if trainingWheels:
            print("reading", trainingWheelsCount, "tw rows")
            df = pd.read_csv(path, nrows=trainingWheelsCount, **kwargs)
        else:
            df = pd.read_csv(path, **kwargs)
        return df

    if trainingWheels:
        with s3open(path) as path:
            df = pd.read_csv(path, nrows=trainingWheelsCount, **kwargs)
    else:
        with s3open(path) as path:
            df = pd.read_csv(path, **kwargs)
    return df


# EXPERIMENTS TO RUN
agree_rhdf_hdf_fz = 1
agree_rhdf_cef_fz = 1
agree_rhdf_hdf_bn = 1
agree_rhdf_cef_bn = 1
agree_rhdfbo_cef_bn = 1
agree_rhdfbo_hdf_bn = 1
agree_rmdf_cef_bn = 1
agree_rmdf_hdf_bn = 1
agree_mdf_cef_bn = 1
agree_mdf_hdf_bn = 1
agree_hdf_cef_bn = 1

putconf_rhdf_cmrcl_fz = 1
putconf_rhdf_cef_fz = 1
putconf_rhdf_cmrcl_bn = 1
putconf_rhdf_cef_bn = 1
putconf_cef_cef_bn = 1
putconf_cef_cmrcl_bn = 1
putconf_hdf_cef_bn = 1
putconf_hdf_cmrcl_bn = 1
putconf_rhdfbo_cef_bn = 1
putconf_rhdfbo_cmrcl_bn = 1
putconf_rmdf_cef_bn = 1
putconf_rmdf_cmrcl_bn = 1
putconf_mdf_cef_bn = 1
putconf_mdf_cmrcl_bn = 1
putconf_modalGsr_cef_bn = 1
putconf_modalGsr_cmrcl_bn = 1
putconf_propGsr_cef_bn = 1
putconf_propGsr_cmrcl_bn = 1

#--------------------------------------------------
# MACRO: cutPop
#--------------------------------------------------
# Description:
#   A function used to re-classify a dataframes 'pop' column into the spefied bins.
#
# Input:
#   A dataframe with 'pop' column
#
# Output:
#   A dataframe with a 'popg' column representing the population-bin to which the record belongs.
#
# Previous STATA Code:
#     capture cutPop drop cutPop
#     program define cutPop
#         args pop
#         egen popg = cut(`pop'), at(0 10 50 100 250 500 1000 100000)
#         label define popglbl 0 "0-9" 10 "10-49" 50 "50-99" 100 "100-249" 250 "250-499" 500 "500-999" 1000 "1000+"
#         label values popg popglbl
#     end
def cutPop(df):
    bins = [0, 10, 50, 100, 250, 500, 1000, 100000]
    df['popg'] = pd.cut(df['pop'], bins, right=False, include_lowest = True, labels=["0-9","10-49","50-99","100-249","250-499","500-999","1000+"])
    return df

#--------------------------------------------------
# MACRO: descDelimFile
#--------------------------------------------------
# Description:
#   Common function used in "data_inventory.py" to check the existence of a secified file.
#
# Input:
#   A file location/path
#
# Output:
#   None
#
# Previous STATA Code:
#     capture descDelimFile drop descDelimFile
#     program define descDelimFile
#         args inFile
#         shell ls -an1 -h `inFile'
#         shell wc -l `inFile'
#     end
def descDelimFile(inFile):
    os.system(f'ls -an1 -h {inFile}')
    os.system(f'wc -l {inFile}')

    
# long_to_wide: Python implementation of the native Stata `rehape` operation for long-to-wide reshapes
def long_to_wide(df, index="lbl", columns="solvar", values="put_"):
    new_df = pd.DataFrame({ index : df[index].unique() })
    for _, new_row in new_df.iterrows():
        for __, row in df.loc[df[index] == new_row[index]].iterrows():
            new_df.loc[new_df[index] == new_row[index], str(values) + str(row[columns])] = row[values]
    return new_df
