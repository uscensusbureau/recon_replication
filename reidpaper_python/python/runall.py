# runall.py
#-----------------------------
# /usr/bin/env python3 runall.py
#-----------------------------
# Created By: Andrew Sillers, David Zhou, Paul Bartholomew, Grace Tang
# Created Date: 5/31/2021
# version = '1.0'
#-----------------------------
# Description:
'''
runall.py:

This code is a translation of existing research STATA code developed by MITRE for the purposes of testing
the corresponding reidentification ouputs. This python file is the translation of "runall.do" the primary driver
for the reidentification papter stats STATA code. See Readme.md file for more details.

Previous STATA Code Header:
*--------------------------------------
* REID/RECON Paper Statistics
* 7/2021, version 1
* Nathan Goldschlag
*
* Args:
*
*
* Example execution:
*>setsid stata-se -b runall.do
*---------------------------------------
'''
# Reading Notes:
# This is the "verbose" form of the code, so you will see code comments with the STATA code that formed the
# originial code that was translated into python. This code with be labled "Previous STATA Code:"
#-----------------------------
# Imports
#-----------------------------
from datetime import datetime
import pandas as pd
import numpy as np
from config import *
import agree_stats
import putative_stats
import confirm_stats
import solvar_stats
#import make_tables
import os
import re
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--setup-only")
parser.add_argument("reid_root", default="/data/reid_module")
args = parser.parse_args()
setup_only = args.setup_only

#--------------------------------------------------
# PROGRAM SETUP, CONFIG, and LOG DATA USED
#--------------------------------------------------
# Description:
#   Initial set-up code to establish directories, log the time, and kick-off the conig python scripts.
#
# Previous STATA Code:
#   version 16 #establishes STATA verions - unneeded in python
#   set more off #stops STATA from pausing for "more" messages - unneeded in python
#   set linesize 80 #establishes the linewidth, in characters, to display - unneeded in python
#   capture log close #close a log if any is open and do nothing if no log is open - unneeded in python
#   set type double #: specifies the default storage type assigned to new variables as double - unneeded in python
#   local dt="`c(current_date)' `c(current_time)'"
#   local dt=subinstr("`dt'",":","",.)
#   local dt=subinstr("`dt'"," ","",.)
#   log using runall_`dt'.log, replace
#   include 0.config.do
#   *-----------------------------------------------
#   LOG DATA USED
#   *-----------------------------------------------
#   *include 0.data_inventory.do

# Print datetime object containing current date and time
now = datetime.now()
print(now)
print("#### START OF RUN ALL ####")
print('*--------------------------------------------------')
# REID/RECON Paper Statistics
# 7/2021, version 1
# Nathan Goldschlag
#
# Args:
#
#
# Example execution:
#>setsid stata-se -b runall.do
#
#--------------------------------------------------

#--------------------------------------------------
# PROGRAM SETUP
#--------------------------------------------------
# version 16 #establishes STATA verions - unneeded in python
# set more off #stops STATA from pausing for "more" messages - unneeded in python
#set linesize 80 #establishes the linewidth, in characters, to display - unneeded in python
#capture log close #close a log if any is open and do nothing if no log is open - unneeded in python
#set type double #: specifies the default storage type assigned to new variables as double - unneeded in python
# local dt="`c(current_date)' `c(current_time)'"
# local dt=subinstr("`dt'",":","",.)
# local dt=subinstr("`dt'"," ","",.)
# log using runall_`dt'.log, replace
# include 0.config.do

#!! Establish Log file:

# cleanup and setup temp and results directory
def directory_setup():
    os.system(f'rm -rf {temp}')
    os.system(f'mkdir {temp}')
    os.system('mkdir results')
    os.system(f'mkdir {disclosure}')

directory_setup()

#--------------------------------------------------
# CONFIG and LOG DATA USED
#--------------------------------------------------
#Running Config file
#exec(open("config.py").read())
#exec(open("data_inventory.py").read())

#--------------------------------------------------
# CLEAN OUTPUT DIR
#--------------------------------------------------
# Description:
#   Removes output Excel files (if they exist in specified directory)
#
# Previous STATA Code:
#   shell rm $disclosure/CBDRB-FY22-DSEP-004.xlsx
#   shell rm $disclosure/CBDRB-FY22-DSEP-004_tables.xlsx

# Uncomment Below if Files need to be removed
#   os.system("rm $disclosure/CBDRB-FY22-DSEP-004.xlsx")
#   os.system("rm $disclosure/CBDRB-FY22-DSEP-004_tables.xlsx")




#--------------------------------------------------
# PREP CATEGORIES
#--------------------------------------------------
# Description:
#   download data from 'temp/cef_over21_modalre_oneton.csv', ensuring columns 1 and 2 are string, then saving
#
# Previous STATA Code:
# * save flag output of reidmodule/06/ as dta so we can merge directly off disc
#   import delimited using $rsltbase/cef/cef_over21_modalre_oneton.csv, clear stringcols(1 2)
#   $save $temp/cef_over21_modalre_oneton, replace
#
#   import delimited using $rsltbase/cmrcl/cmrcl_cef_bpas_match.csv, clear stringcols(1 2)
#   save $temp/cmrcl_cef_bpas_match, replace

df = get_df(f'{rsltbase}/cef/cef_over21_modalre_oneton.csv', trainingWheels, dtype={"geoid_block":str, "pik":str})
print('cef_over21_modalre_oneton', list(df))
cef_over21_modalre_oneton = df
cef_over21_modalre_oneton.to_csv('temp/cef_over21_modalre_oneton.csv', index=False)

df = get_df(f'{rsltbase}/cmrcl/cmrcl_cef_bpas_match.csv', trainingWheels, dtype={"geoid_block":str, "pik":str})
print('cmrcl_cef_bpas_match', list(df))
cmrcl_cef_bpas_match = df
cmrcl_cef_bpas_match.to_csv('temp/cmrcl_cef_bpas_match.csv', index=False)



#--------------------------------------------------
# CEF-BASED BLOCK SIZE BINS
#--------------------------------------------------
# Description:
#   downloading from 'solvar/scaled_ivs.csv' and '/cef/cef_block_counts.csv', performing tables operations
#   changing column names and types. The solvar table is reduced to a subset of columns, saved, then merged with the
#   cef_block_counts to form 'cefblocksize'.
#
# Previous STATA Code:
#   import delimited using $solvar/scaled_ivs.csv, clear stringcols(_all)
#   rename blockid geoid_block
#   * the 202302 scaled_ivs.csv file was missing leading zeros
#   gen l = strlen(geoid_block)
#   tab l
#   replace geoid_block = "0" + geoid_block if l==14
#   destring iv, replace
#   destring siv, replace
#   rename siv s_iv
#   keep geoid_block iv s_iv
#   tempfile solvar
#   save `solvar'

df = get_df(f'{solvar}/scaled_ivs.csv', trainingWheels, dtype=str)
print(list(df.columns), 'solvar/scaled_ivs columns')
df = df.rename(columns={'Block ID':'geoid_block'})
df["geoid_block"] = df["geoid_block"].str.zfill(15)
df['iv'] = df.astype({'IV': 'float'})['IV']
print('investigate SIV', df.SIV)
print(df.SIV.min(), df.SIV.max())
df['s_iv'] = df.astype({'SIV': 'float'})['SIV']
solvar = df[['geoid_block', 'iv', 's_iv']]
print('investigate s_iv initial', df.s_iv.min(), df.s_iv.max())

# Previous STATA Code:
#   import delimited using $rsltbase/cef/cef_block_counts.csv, clear stringcols(1)
#   drop if substr(geoid_block,1,2)=="72"
#   cutPop pop
#   rename popg block_pop
#   merge 1:1 geoid_block using `solvar', nogen keep(1 3) keepusing(s_iv)
#   gen solvar = "0" if s_iv==0
#   replace solvar = ">0" if s_iv>0 & !missing(s_iv)
#   replace solvar = "NA" if missing(s_iv)
#   keep geoid_block pop keepblockpik block_pop solvar
#   rename pop cefpop
#   tempfile cefblocksize
#   save `cefblocksize'
#   save $temp/cefblocksize, replace

df = get_df(f'{rsltbase}/cef/cef_block_counts.csv', trainingWheels, dtype={"geoid_block":str})
print(list(df.columns), 'rsltbase/cef/cef_block_counts columns')
df['twochar_geoid'] = df['geoid_block'].str[0:2]
df = df[df.twochar_geoid != '72']
print('investigate pop', df['pop'])
df = cutPop(df) # doesn't do anything to blockpop
df = df.rename(columns={'popg':'block_pop'})

df_joint = pd.merge(df, solvar[['geoid_block', 's_iv']], on='geoid_block', how='left')  #!! no filtering out df2 only s_iv
print(list(df_joint.columns), 'dfjoint columns')

print('investigate s_iv after solvar merge', df_joint.s_iv.min(), df_joint.s_iv.max())

print('investigate s_iv after solvar merge', df_joint.s_iv)
print('investigate s_iv after solvar merge', df_joint.s_iv.unique())

df_joint.loc[df_joint.s_iv==0, 'solvar'] = "0"
df_joint.loc[(df_joint.s_iv>0) & (pd.notna(df_joint["s_iv"])), 'solvar'] = ">0"
print('investigate s_iv after column loc', df_joint.s_iv)
df_joint.loc[pd.isna(df_joint["s_iv"]), 'solvar'] = "NA"
print('investigate solvar after column loc', df_joint.solvar.value_counts())
cefblocksize = df_joint[['geoid_block', 'pop', 'keepblockpik', 'block_pop', 'solvar']]
cefblocksize = cefblocksize.rename(columns={'pop':'cefpop'})
print(list(cefblocksize), 'cefblocksize')
print('investigate solvar before csv', cefblocksize.solvar.value_counts())
cefblocksize.to_csv(f'{temp}/cefblocksize.csv', index=False)
#--------------------------------------------------
# COMMONLY USED AGGREGATIONS
#--------------------------------------------------
# Description:
#
#
# Previous STATA Code:
#   use $temp/cef_over21_modalre_oneton, clear #already exists as cef_over21_modalre_oneton df
#   merge m:1 geoid_block using `cefblocksize', keep(1 3) nogen keepusing(solvar)
#   gen nonmodalrepop = modalre==0
#   gen u21pop = over21==0
#   gen nmsab_oneton = sab_oneton==1 & modalre == 0
#   gen msab_oneton = sab_oneton==1 & modalre == 1
#   gen nmsabb_oneton = sabb_oneton==1 & modalre == 0
#   gen msabb_oneton = sabb_oneton==1 & modalre == 1

over21modalonetonpop = pd.merge(cef_over21_modalre_oneton, cefblocksize[['geoid_block', 'solvar']], on='geoid_block', how='left')  #!! no filtering out df2 only solvar
print(list(over21modalonetonpop.columns), 'over21modalonetonpop columns')

over21modalonetonpop['nonmodalrepop'] = over21modalonetonpop.modalre == 0
over21modalonetonpop['u21pop'] = over21modalonetonpop.over21 == 0
over21modalonetonpop['nmsab_oneton'] = (over21modalonetonpop.sab_oneton == 1) & (over21modalonetonpop.modalre == 0)
over21modalonetonpop['msab_oneton'] = (over21modalonetonpop.sab_oneton == 1) & (over21modalonetonpop.modalre == 1)
over21modalonetonpop['nmsabb_oneton'] = (over21modalonetonpop.sabb_oneton == 1) & (over21modalonetonpop.modalre == 0)
over21modalonetonpop['msabb_oneton'] = (over21modalonetonpop.sabb_oneton == 1) & (over21modalonetonpop.modalre == 1)

# Previous STATA Code:
#   foreach s in "0" ">0" "NA" {
#     if "`s'" == "0" {
#         local sfx = "zero"
#     }
#     if "`s'" == ">0" {
#         local sfx = "gtzero"
#     }
#     if "`s'" == "NA" {
#         local sfx = "NA"
#     }
#     gen modalre_sv`sfx'pop = modalre==1 & solvar=="`s'"
#     gen nonmodalre_sv`sfx'pop = modalre==0 & solvar=="`s'"
#     gen o21_sv`sfx'pop = over21==1 & solvar=="`s'"
#     gen u21_sv`sfx'pop = over21==0 & solvar=="`s'"
#     gen nmsab_oneton_sv`sfx' = sab_oneton==1 & modalre == 0  & solvar=="`s'"
#     gen msab_oneton_sv`sfx' = sab_oneton==1 & modalre == 1 & solvar=="`s'"
#     gen nmsabb_oneton_sv`sfx' = sabb_oneton==1 & modalre == 0  & solvar=="`s'"
#     gen msabb_oneton_sv`sfx' = sabb_oneton==1 & modalre == 1 & solvar=="`s'"
#   }
for s in ['0', '>0', 'NA']:
    if s == "0":
        sfx = "zero"

    if s == ">0":
        sfx = "gtzero"

    if s == "NA":
        sfx = "NA"

    over21modalonetonpop[f'modalre_sv{sfx}pop'] = (over21modalonetonpop.modalre == 1) & (over21modalonetonpop.solvar == s)
    over21modalonetonpop[f'nonmodalre_sv{sfx}pop'] = (over21modalonetonpop.modalre == 0) & (over21modalonetonpop.solvar == s)
    over21modalonetonpop[f'o21_sv{sfx}pop'] = (over21modalonetonpop.over21 == 1) & (over21modalonetonpop.solvar == s)
    over21modalonetonpop[f'u21_sv{sfx}pop'] = (over21modalonetonpop.over21 == 0) & (over21modalonetonpop.solvar == s)

    over21modalonetonpop[f'nmsab_oneton_sv{sfx}'] = (over21modalonetonpop.sab_oneton == 1) & (over21modalonetonpop.modalre == 0) & (over21modalonetonpop.solvar == s)

    over21modalonetonpop[f'msab_oneton_sv{sfx}'] = (over21modalonetonpop.sab_oneton == 1) & (over21modalonetonpop.modalre == 1) & (over21modalonetonpop.solvar == s)

    over21modalonetonpop[f'nmsabb_oneton_sv{sfx}'] = (over21modalonetonpop.sabb_oneton == 1) & (over21modalonetonpop.modalre == 0) & (over21modalonetonpop.solvar == s)

    over21modalonetonpop[f'msabb_oneton_sv{sfx}'] = (over21modalonetonpop.sabb_oneton == 1) & (over21modalonetonpop.modalre == 1) & (over21modalonetonpop.solvar == s)

# Previous STATA Code:
#   rename modalre modalrepop
#   rename over21 o21pop
#   collapse (sum) o21pop u21pop modalrepop nonmodalrepop sab_oneton sabb_oneton ber_oneton nmsab_oneton nmsabb_oneton msab_oneton msabb_oneton *_sv*, by(geoid_block)
#   tempfile over21modalonetonpop
#   save `over21modalonetonpop'
over21modalonetonpop = over21modalonetonpop.rename(columns={'modalre':'modalrepop'})
over21modalonetonpop = over21modalonetonpop.rename(columns={'over21':'o21pop'})
_sv = [x for x in over21modalonetonpop.columns if re.search('_sv',x)]
over21modalonetonpop = over21modalonetonpop.groupby(['geoid_block']).sum().reset_index()[['geoid_block', 'o21pop', 'u21pop', 'modalrepop', 'nonmodalrepop', 'sab_oneton', 'sabb_oneton', 'ber_oneton', 'nmsab_oneton', 'nmsabb_oneton', 'msab_oneton', 'msabb_oneton'] + _sv]
print(list(over21modalonetonpop), 'over21modaloneton after sv')
over21modalonetonpop.to_csv(f'{temp}/over21modalonetonpop.csv', index=False)

# Previous STATA:
# use $temp/cmrcl_cef_bpas_match, clear
# merge m:1 geoid_block using `cefblocksize', keep(1 3) nogen keepusing(solvar)
# gen ccbpaspop = bpas==1
# gen ccbpabspop = bpabs==1
# foreach s in "0" ">0" "NA" {
#    if "`s'" == "0" {
#        local sfx = "zero"
#    }
#    if "`s'" == ">0" {
#        local sfx = "gtzero"
#    }
#    if "`s'" == "NA" {
#        local sfx = "NA"
#    }
#    gen ccbpas_sv`sfx'pop = bpas==1 & solvar=="`s'"
#    gen ccbpabs_sv`sfx'pop = bpabs==1 & solvar=="`s'"
#}
df = cmrcl_cef_bpas_match
cols_to_merge = list(cefblocksize.columns.difference(df.columns)) + ['geoid_block']
df = pd.merge(df, cefblocksize[cols_to_merge], on='geoid_block', how='left')
df["ccbpaspop"] = df["bpas"]==1
df["ccbpabspop"] = df["bpabs"]==1
for s in ["0", ">0", "NA"]:
    sfx = ({ "0":"zero", ">0":"gtzero", "NA":"NA" })[s]
    df[f'ccbpas_sv{sfx}pop'] = (df["bpas"]==1) & (df["solvar"]==s)
    df[f'ccbpabs_sv{sfx}pop'] = (df["bpabs"]==1) & (df["solvar"]==s)

_sv = [x for x in df.columns if re.search('_sv',x)]
ccbpaspop = df.groupby(['geoid_block']).sum().reset_index()[['geoid_block', 'ccbpaspop', 'ccbpabspop'] + _sv]
print(list(ccbpaspop), 'ccbpaspop')
ccbpaspop.to_csv(f'{temp}/ccbpaspop.csv', index=False)

# Previous STATA Code:
#   import delimited using $rsltbase/cef/cef_block_counts.csv, clear stringcols(1)
#   drop if substr(geoid_block,1,2)=="72"
#   merge 1:1 geoid_block using `cefblocksize', keep(1 3) nogen
#   merge 1:1 geoid_block using `over21modalonetonpop', keep(1 3) nogen
#   preserve
#     collapse (sum) cefpop = pop keepblockpik o21pop u21pop modalrepop nonmodalrepop sab_onetonpop=sab_oneton sabb_onetonpop=sabb_oneton  ber_onetonpop=ber_oneton nmsab_oneton_pop=nmsab_oneton nmsabb_oneton_pop=nmsabb_oneton msab_oneton_pop=msab_oneton msabb_oneton_pop=msabb_oneton *_sv*
#     tempfile cefpopnat
#     save `cefpopnat'
#     save $temp/cefnatpop, replace
#   restore
#   collapse (sum) cefpop = pop keepblockpik o21pop u21pop modalrepop nonmodalrepop sab_onetonpop=sab_oneton sabb_onetonpop=sabb_oneton  ber_onetonpop=ber_oneton nmsab_oneton_pop=nmsab_oneton nmsabb_oneton_pop=nmsabb_oneton msab_oneton_pop=msab_oneton msabb_oneton_pop=msabb_oneton, by(block_pop)
#   tempfile cefpopblocksz
#   save `cefpopblocksz'
#   save $temp/cefblocksize_pop, replace

df = get_df(f'{rsltbase}/cef/cef_block_counts.csv', trainingWheels, dtype={"geoid_block":str})
df['twochar_geoid'] = df['geoid_block'].str[0:2]
df = df[df.twochar_geoid != '72']

# We only merge with the columns that cefblocksize has that df doesn't to avoid duplicate columns
# ex: both dfs had keepblockpik, which would result in a keepblockpik_x and keepblockpik_y, and no keepblockpik
cols_to_merge = list(cefblocksize.columns.difference(df.columns)) + ['geoid_block']
df_joint1 = pd.merge(df, cefblocksize[cols_to_merge], on='geoid_block', how='left')
print(list(df_joint1), 'df_joint1')

# Avoid duplicate columns again
cols_to_merge = list(over21modalonetonpop.columns.difference(df_joint1.columns)) + ['geoid_block']
df_joint2 = pd.merge(df_joint1, over21modalonetonpop[cols_to_merge], on='geoid_block', how='left')
print(list(df_joint2), 'df_joint2')

# Avoid duplicate columns again
cols_to_merge = list(ccbpaspop.columns.difference(df_joint2.columns)) + ['geoid_block']
df_joint3 = pd.merge(df_joint2, ccbpaspop[cols_to_merge], on='geoid_block', how='left')
print(list(df_joint3), 'df_joint3')

del ccbpaspop
del over21modalonetonpop

_sv = [x for x in df_joint3.columns if re.search('_sv',x)]

cefpopnat = df_joint3[['pop', 'keepblockpik', 'o21pop', 'u21pop', 'modalrepop', 'nonmodalrepop', 'sab_oneton', 'sabb_oneton', 'ber_oneton', 'nmsab_oneton', 'nmsabb_oneton', 'msab_oneton', 'msabb_oneton', 'ccbpaspop', 'ccbpabspop'] +  _sv].sum().to_frame().transpose()
cefpopnat= cefpopnat.rename(columns={'pop':'cefpop'})
cefpopnat= cefpopnat.rename(columns={'sab_oneton':'sab_onetonpop'})
cefpopnat= cefpopnat.rename(columns={'sabb_oneton':'sabb_onetonpop'})
cefpopnat= cefpopnat.rename(columns={'ber_oneton':'ber_onetonpop'})
cefpopnat= cefpopnat.rename(columns={'nmsab_oneton':'nmsab_oneton_pop'})
cefpopnat= cefpopnat.rename(columns={'nmsabb_oneton':'nmsabb_oneton_pop'})
cefpopnat= cefpopnat.rename(columns={'msab_oneton':'msab_oneton_pop'})
cefpopnat= cefpopnat.rename(columns={'msabb_oneton':'msabb_oneton_pop'})
print('cefpopnat', list(cefpopnat))
cefpopnat.to_csv(f'{temp}/cefpopnat.csv', index=False)
del cefpopnat

cefpopblocksz = df_joint3.groupby(['block_pop'])[['pop', 'keepblockpik', 'o21pop', 'u21pop', 'modalrepop', 'nonmodalrepop', 'sab_oneton', 'sabb_oneton', 'ber_oneton', 'nmsab_oneton', 'nmsabb_oneton', 'msab_oneton', 'msabb_oneton', 'ccbpaspop', 'ccbpabspop'] + _sv].sum().reset_index()

del df_joint1
del df_joint2
del df_joint3

cefpopblocksz= cefpopblocksz.rename(columns={'pop':'cefpop'})
cefpopblocksz= cefpopblocksz.rename(columns={'sab_oneton':'sab_onetonpop'})
cefpopblocksz= cefpopblocksz.rename(columns={'sabb_oneton':'sabb_onetonpop'})
cefpopblocksz= cefpopblocksz.rename(columns={'ber_oneton':'ber_onetonpop'})
cefpopblocksz= cefpopblocksz.rename(columns={'nmsab_oneton':'nmsab_oneton_pop'})
cefpopblocksz= cefpopblocksz.rename(columns={'nmsabb_oneton':'nmsabb_oneton_pop'})
cefpopblocksz= cefpopblocksz.rename(columns={'msab_oneton':'msab_oneton_pop'})
cefpopblocksz= cefpopblocksz.rename(columns={'msabb_oneton':'msabb_oneton_pop'})
cefpopblocksz.to_csv(f'{temp}/cefpopblocksz.csv')
del cefpopblocksz

# Previous STATA Code:
#   import delimited using $rsltbase/hdf/hdf_block_counts.csv, clear stringcols(1)
#   drop if substr(geoid_block,1,2)=="72"
#   collapse (sum) hdfpop = pop
#   tempfile hdfpopnat
#   save `hdfpopnat'
hdfblockcounts = get_df(f'{rsltbase}/hdf/hdf_block_counts.csv', trainingWheels, dtype={"geoid_block":str})
hdfpopnat = hdfblockcounts
hdfpopnat['twochar_geoid'] = hdfpopnat['geoid_block'].str[0:2]
hdfpopnat = hdfpopnat[df.twochar_geoid != '72']
hdfpopnat = hdfpopnat.sum().to_frame().transpose()
hdfpopnat= hdfpopnat.rename(columns={'pop':'hdfpop'})

# Previous STATA Code:
#   import delimited using $rsltbase/hdf/hdf_block_counts.csv, clear stringcols(1)
#   merge 1:1 geoid_block using `cefblocksize', keep(3)
#   collapse (sum) hdfpop = pop, by(block_pop)
#   tempfile hdfpopblocksz
#   save `hdfpopblocksz'

#cefblocksize.geoid_block = cefblocksize.geoid_block.astype(str)
#hdfblockcounts.geoid_block = hdfblockcounts.geoid_block.astype(str)

# merge 5
# version 1:
cols_to_merge = list(cefblocksize.columns.difference(hdfblockcounts.columns)) + ['geoid_block']
hdfpopblocksz = pd.merge(hdfblockcounts, cefblocksize[cols_to_merge], on='geoid_block', how='inner')
# version 2: 
# hdfpopblocksz = pd.merge(hdfblockcounts, cefblocksize, on='geoid_block', how='inner')

hdfpopblocksz = hdfpopblocksz.groupby(['geoid_block'])[['pop']].sum()
hdfpopblocksz = hdfpopblocksz.rename(columns={'pop':'hdfpop'})

# Previous STATA Code:
#   import delimited using $rsltbase/rhdf/`rhdf'/`rhdf'_block_counts.csv, clear stringcols(1)
#   drop if substr(geoid_block,1,2)=="72"
#   collapse (sum) rhdfpop = pop
#   tempfile rhdfpopnat
#   save `rhdfpopnat'
rhdfblockcounts = get_df(f'{rsltbase}/rhdf/{rhdf}/{rhdf}_block_counts.csv', trainingWheels, dtype={"geoid_block":str}) 
rhdfpopnat = rhdfblockcounts
rhdfpopnat['twochar_geoid'] = rhdfpopnat['geoid_block'].str[0:2]
rhdfpopnat = rhdfpopnat[df.twochar_geoid != '72']
rhdfpopnat = rhdfpopnat.sum().to_frame().transpose()
rhdfpopnat= rhdfpopnat.rename(columns={'pop':'rhdfpop'})
rhdfpopnat.to_csv(f'{temp}/rhdfpopnat.csv', index=False)
del rhdfpopnat
del rhdfblockcounts

# Previous STATA Code:
#   use $temp/cefblocksize, clear
#   collapse (sum) cefpop keepblockpik, by(solvar)
#   tempfile solvarpop
#   save `solvarpop'
solvarpop = cefblocksize.groupby(['solvar'])[['cefpop','keepblockpik']].sum()
print('solvarpop', list(solvarpop))
solvarpop.to_csv(f'{temp}/solvarpop.csv')
del solvarpop

#--------------------------------------------------
# ALL ANALYSES
#--------------------------------------------------
# Description:
#   Initial set-up code to establish directories, log the time, and kick-off the conig python scripts.
#
# Previous STATA Code:
#   include 1.agree_stats.do
#
#   include 2.putative_stats.do
#
#   include 3.confirm_stats.do

if setup_only:
    print(now)
    print("#### END OF RUN ALL SETUP PHASE ####")
    print('*--------------------------------------------------')
    sys.exit()

agree_stats.main()
putative_stats.main()
confirm_stats.main()

# --------------------------------------------------
#  PDL SOLUTION VARIABILITY STATISTICS
#--------------------------------------------------
# Description:
#   Code is depricated and not designed to perform any function, could be updated in the future.
#
# Previous STATA Code:
#  version 16 #establishes STATA verions - unneeded in python
#  taken from John A's modifications to modalre_add20210425 spreadsheet, presumably from Philip Leclerc
#  For (very crude) comparison, from PDL, solution variability statistics for Tracts with different population sizes:
#  Interpret like: in Tracts with populations <=9, reconstruction variability is small:
#  changing from the current solution to a maximally distant (in L1 / "exact matches") one
#  one changes no more than 0.86% of Person records.
#  [Hence, in tracts of this size, an attacker could compute solution variability statistics to be confident in their attack,
#  unless they believed the swap or imputation rates were high.]
#  Caveats:
#  1. This is worst-case, not average or median: it can be used to upper bound error purely from degrees-of-freedom in the reconstruction
#  but treating it as a lower bound or median/average-case measure would be heuristic. We only really need it as an upper bound, though.
#  [Note: I don't think it's even clear what distribution would be relevant if we instead tried to compute a corresponding 'average'.]
#  2. This used Tracts as the target geounit. Would need to modify either this or confirmed reid rates to use same geounits for proper comparison
#  3. This did not reload the original reconstruction solution as its starting point. Instead, it resolved the original model
#  and used that as its starting point. Gurobi is mostly deterministic, so this should not contribute noticeably to these statistics.
#  4. 1000+ Tracts category is based on incomplete data. Solution variability solves become very slow at this point.

'''
clear
set obs 7
gen block_pop = ""
replace block_pop = "IV[0-9]" if _n==1
replace block_pop = "IV[10-49]" if _n==2
replace block_pop = "IV[50-99]" if _n==3
replace block_pop = "IV[100-249]" if _n==4
replace block_pop = "IV[250-499]" if _n==5
replace block_pop = "IV[500-999]" if _n==6
replace block_pop = "IV[1000+]" if _n==7

gen solvar = .
replace solvar = 0.86 if _n==1
replace solvar = 6.68 if _n==2
replace solvar = 20.63 if _n==3
replace solvar = 29.47 if _n==4
replace solvar = 43.63 if _n==5
replace solvar = 59.11 if _n==6
replace solvar = 62.72 if _n==7
export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(solvar) firstrow(variables)  sheetmodify
'''

solvar_stats.main()

# --------------------------------------------------
#  MAKE TABLES FROM THE COUNTS
# --------------------------------------------------
# Description:
#   Executes the code that creates the summary statistics of "$disclosure"/CBDRB-FY22-DSEP-004.xlsx" into tables
#   and saves to "$disclosure"/CBDRB-FY22-DSEP-004_tables.xlsx"
#
# Previous STATA Code:
#   include 4.make_tables.do

#make_tables.main()

# --------------------------------------------------
#  END
# --------------------------------------------------
# Description:
#   End of runall.py, logs time.
#
# Previous STATA Code:
# di c(current_date) " " c(current_time)
# print('*--------------------------------------------------')

print(now)
print("#### END OF RUN ALL ####")
print('*--------------------------------------------------')
