#!/usr/bin/env python
# coding: utf-8

import argparse
parser = argparse.ArgumentParser()
parser.add_argument("-c","--config", help="pick config file", required=True)
args = parser.parse_args()

import yaml
config = yaml.load(open(f'./{args.config}'), Loader=yaml.SafeLoader)

HDFDATADIR = config['METRICS_CONFIG']['HDFDATADIR']
OUTPUTDIR = config['METRICS_CONFIG']['OUTPUTDIR']
MDFPERFILEPATH = config['METRICS_CONFIG']['MDFPERFILEPATH']
MDFPERPRFILEPATH = config['METRICS_CONFIG']['MDFPERPRFILEPATH']
separatePR = config['METRICS_CONFIG']['separatePR']
try:
    runAGEBYRACE = config['METRICS_CONFIG']['runAGEBYRACE']
except:
    runAGEBYRACE = False
try:
    runPRhere = config['METRICS_CONFIG']['runPRhere']
except:
    runPRhere = False
mergeValidation = None # or "1:1"
try:
    GETMISSIONNAMES = config['METRICS_CONFIG']['GETMISSIONNAMES']
except:
    GETMISSIONNAMES = False

print("runPRhere: ",runPRhere)

from datetime import datetime
print("{} Start".format(datetime.now()))

if GETMISSIONNAMES:
    import re
    with open(f'{MDFPERFILEPATH}','r') as f:
        perhead = [next(f) for x in range(10)]
    perheader = " ".join(perhead)
    perp = re.search(r"([A-Z][A-Z|\-|\_]+)\.log", perheader)

    if runPRhere:
        with open(f'{MDFPERPRFILEPATH}','r') as f:
            perprhead = [next(f) for x in range(10)]
        perprheader = " ".join(perprhead)
        perpr = re.search(r"([A-Z][A-Z|\-|\_]+)\.log", perprheader)
    else:
        perpr = ""

    with open(f'{OUTPUTDIR}/filemissionnames_personmetrics.csv','w') as f:
        if runPRhere:
            f.write("MetricsRunStarted,PersonMissionName,PersonPRMissionName"+'\n')
            f.write(f"{datetime.now()},{perp.groups()[0]},{perpr.groups()[0]}")
        else:
            f.write("MetricsRunStarted,PersonMissionName"+'\n')
            f.write(f"{datetime.now()},{perp.groups()[0]}")

import pandas as pd 
import numpy as np
from collections import OrderedDict
from statistics import median_grouped
print("{} Libraries Imported".format(datetime.now()))
statedict = OrderedDict({'02': 'AK', '01': 'AL', '05': 'AR', '04': 'AZ', '06': 'CA', '08': 'CO', '09': 'CT', '11': 'DC', '10': 'DE', '12': 'FL', '13': 'GA', '15': 'HI', '19': 'IA', '16': 'ID', '17': 'IL', '18': 'IN', '20': 'KS', '21': 'KY', '22': 'LA', '25': 'MA', '24': 'MD', '23': 'ME', '26': 'MI', '27': 'MN', '29': 'MO', '28': 'MS', '30': 'MT', '37': 'NC', '38': 'ND', '31': 'NE', '33': 'NH', '34': 'NJ', '35': 'NM', '32': 'NV', '36': 'NY', '39': 'OH', '40': 'OK', '41': 'OR', '42': 'PA', '44': 'RI', '45': 'SC', '46': 'SD', '47': 'TN', '48': 'TX', '49': 'UT', '51': 'VA', '50': 'VT', '53': 'WA', '55': 'WI', '54': 'WV', '56': 'WY', '72':'PR'})
racealonedict  = {1: 'White Alone',  2: 'Black Alone',  3: 'AIAN Alone',  4: 'Asian Alone',  5: 'NHOPI Alone',  6: 'Some Other Race Alone',  7: 'Two Or More Races'}
raceincombdict = {'whitealone-or-incomb':'White Alone or In Combination', 'blackalone-or-incomb':'Black Alone or In Combination', 'aianalone-or-incomb':'AIAN Alone or In Combination', 'asianalone-or-incomb':'Asian Alone or In Combination', 'nhopialone-or-incomb': 'NHOPI Alone or In Combination', 'soralone-or-incomb':'Some Other Race Alone or In Combination'}
hispdict = {'1': 'Not Hispanic', '2':'Hispanic'}
sexdict = {'1': 'Male', '2':'Female'}

def calc_perc(da):
    if (da['HDF_Population'] == 0) and (da['Diff']==0):
        return 0
    elif da['HDF_Population'] == 0:
        return da['Diff']/((da['HDF_Population']+da['MDF_Population'])/2)
    else:
        return da['Diff']/da['HDF_Population']

def calculate_stats(data):
    data = data.copy()
    data = data.fillna({'HDF_Population': 0, 'MDF_Population': 0})
    data['Diff'] = data['MDF_Population'] - data['HDF_Population']    
    data['AbsDiff'] = abs(data['Diff'])
    data['PercDiff'] = 100 * data.apply(calc_perc, axis='columns')
    data['AbsPercDiff'] = abs(data['PercDiff'])
    #data['AbsPercDiffC'] = 100 * abs(((data['MDF_Population'] + 0.1) - (data['HDF_Population'] + 0.1))/(data['HDF_Population'] + 0.1))
    return data

def calculate_ss(data, geography, sizecategory, characteristic):
    if len(data) > 0:
        odf = pd.DataFrame({
            'Geography': geography,
            'Size_Category': sizecategory,
            'Characteristic': characteristic,
            'MinDiff': np.nanmin(data['Diff']),
            'MeanDiff':np.nanmean(data['Diff']),
            'MedianDiff':np.nanmedian(data['Diff']),
            'MaxDiff': np.nanmax(data['Diff']),
            'MeanAbsDiff':np.nanmean(data['AbsDiff']),
            'MedianAbsDiff':np.nanmedian(data['AbsDiff']),
            'AbsDiff90th': np.nanpercentile(data['AbsDiff'], 90),
            'AbsDiff95th': np.nanpercentile(data['AbsDiff'], 95),
            'MinPercDiff': np.nanmin(data['PercDiff']),
            'MeanPercDiff':np.nanmean(data['PercDiff']),
            'MedianPercDiff':np.nanmedian(data['PercDiff']),
            'MaxPercDiff': np.nanmax(data['PercDiff']),
            'PercDiffNAs': np.sum(data['PercDiff'].isnull()),
            'MeanAbsPercDiff': np.nanmean(data['AbsPercDiff']),
            'MedianAbsPercDiff': np.nanmedian(data['AbsPercDiff']),
            'AbsPercDiff90th': np.nanpercentile(data['AbsPercDiff'], 90),
            'AbsPercDiff95th': np.nanpercentile(data['AbsPercDiff'], 95),
            'AbsPercDiffMax': np.nanmax(data['AbsPercDiff']),
            'AbsPercDiffNAs': np.sum(data['AbsPercDiff'].isnull()),
            'RMSE': np.sqrt((data['Diff']**2).mean()),
            'CV': 100*(np.sqrt((data['Diff']**2).mean())/np.nanmean(data['HDF_Population'])) if np.nanmean(data['HDF_Population']) != 0 else np.nan,
            'MeanHDFPop': np.nanmean(data['HDF_Population']),
            'NumCells':len(data),
            'NumberBtw2Perc5Perc':len(data[(data['AbsPercDiff'] >=2)&(data['AbsPercDiff'] <= 5)]),
            'NumberGreater5Perc':len(data[(data['AbsPercDiff'] > 5)]),
            'NumberGreater200': len(data[(data['AbsDiff'] > 200)]),
            'NumberGreater10Perc':len(data[(data['AbsPercDiff'] > 10)])
        }, index=[0])
    else:
        odf = pd.DataFrame({
            'Geography': geography,
            'Size_Category': sizecategory,
            'Characteristic': characteristic,
            'MinDiff': 0,
            'MeanDiff':0,
            'MedianDiff':0,
            'MaxDiff': 0,
            'MeanAbsDiff':0,
            'MedianAbsDiff':0,
            'AbsDiff90th': 0,
            'AbsDiff95th': 0,
            'MinPercDiff': 0,
            'MeanPercDiff':0,
            'MedianPercDiff':0,
            'MaxPercDiff': 0,
            'PercDiffNAs': 0,
            'MeanAbsPercDiff': 0,
            'MedianAbsPercDiff': 0,
            'AbsPercDiff90th': 0,
            'AbsPercDiff95th': 0,
            'AbsPercDiffMax': 0,
            'AbsPercDiffNAs':0,
            'RMSE': 0,
            'CV': 0,
            'MeanHDFPop':0,
            'NumCells':0,
            'NumberBtw2Perc5Perc':0,
            'NumberGreater5Perc':0,
            'NumberGreater200':0,
            'NumberGreater10Perc':0
        }, index=[0])
    return odf

# Output Data Frame
outputdf = pd.DataFrame(data=None, columns = ['Geography','Size_Category','Characteristic','MinDiff','MeanDiff','MedianDiff','MaxDiff','MeanAbsDiff','MedianAbsDiff', 'AbsDiff90th','AbsDiff95th','MinPercDiff','MeanPercDiff','MedianPercDiff','MaxPercDiff','PercDiffNAs', 'MeanAbsPercDiff','MedianAbsPercDiff','AbsPercDiff90th','AbsPercDiff95th','AbsPercDiffMax','AbsPercDiffNAs','RMSE', 'CV','MeanHDFPop','NumCells','NumberBtw2Perc5Perc','NumberGreater5Perc','NumberGreater200','NumberGreater10Perc','AvgAbsDiffMedAge','AvgAbsDiffSexRatio','TAES','CountMDFltHDF','MedianPctDiffWhereMDFltHDF','Number100PlusMDFLessThan20HDF','NumberLessThan20MDF100PlusHDF','NumberHDFgt50kMDFlt50k','NumberHDFlt50kMDFgt50k','NumberHDFneMDF','Inconsistent'])
print("{} Output DF Frame Created OK".format(datetime.now()))

#Set up blocks, tracts, counties from Geography file
allgeos = pd.read_csv(f"{HDFDATADIR}/blocks_to_geos_2010.csv", dtype=str)
allgeospr = allgeos[allgeos.StateGEOID == "72"]
allgeos = allgeos[allgeos.StateGEOID != "72"]
allblocks = list(allgeos.BlockGEOID.unique())
allblockgroups = list(allgeos.BlockGroupGEOID.unique())
alltracts = list(allgeos.TractGEOID.unique())
allcounties = list(allgeos.CountyGEOID.unique())
allplaces = list(allgeos.loc[allgeos['IncPlaceGEOID'].notnull(), 'IncPlaceGEOID'].unique())
allstates = list(allgeos.StateGEOID.unique())
allaiannh = list(allgeos.loc[allgeos['AIANNHGEOID'].notnull(), 'AIANNHGEOID'].unique())
allfedairs = list(allgeos.loc[allgeos['FedAIRGEOID'].notnull(), 'FedAIRGEOID'].unique())
allotsas = list(allgeos.loc[allgeos['OTSAGEOID'].notnull(), 'OTSAGEOID'].unique())
allanvsas = list(allgeos.loc[allgeos['ANVSAGEOID'].notnull(), 'ANVSAGEOID'].unique())
allelemschdists = list(allgeos.loc[allgeos['SchDistEGEOID'].notnull(), 'SchDistEGEOID'].unique())
allsecschdists = list(allgeos.loc[allgeos['SchDistSGEOID'].notnull(), 'SchDistSGEOID'].unique())
allunischdists = list(allgeos.loc[allgeos['SchDistUGEOID'].notnull(), 'SchDistUGEOID'].unique())
allmcds = list(allgeos.loc[allgeos['MCDGEOID'].notnull(), 'MCDGEOID'].unique())
alltractspr = list(allgeospr.TractGEOID.unique())
allcountiespr = list(allgeospr.CountyGEOID.unique())
del allgeospr
#
allstatesindex = pd.Index(allstates, name='GEOID')
allcountiesindex = pd.Index(allcounties, name='GEOID')
alltractsindex = pd.Index(alltracts, name='GEOID')
allblockgroupsindex = pd.Index(allblockgroups, name='GEOID')
allblocksindex = pd.Index(allblocks, name='GEOID')
allplacesindex = pd.Index(allplaces, name='IncPlaceGEOID')
allaiannhindex = pd.Index(allaiannh, name="AIANNHGEOID")
allfedairsindex = pd.Index(allfedairs, name="FedAIRGEOID")
allotsasindex = pd.Index(allotsas, name="OTSAGEOID")
allanvsasindex = pd.Index(allanvsas, name="ANVSAGEOID")
allelemschdistsindex = pd.Index(allelemschdists, name="SchDistEGEOID")
allsecschdistsindex = pd.Index(allsecschdists, name="SchDistSGEOID")
allunischdistsindex = pd.Index(allunischdists, name="SchDistUGEOID")
allmcdsindex = pd.Index(allmcds, name="MCDGEOID")
allcountiesprindex = pd.Index(allcountiespr, name='GEOID')
alltractsprindex = pd.Index(alltractspr, name='GEOID')
print("{} Geography Block Crosswalk Input OK".format(datetime.now()))

agecats = [i for i in range(0,116)]
racealonecats = [1,2,3,4,5,6,7]
sexcats = ['1','2']
hispcats = ['1','2']  # Hisp Cats as string here. Not so with PLonly due to memory.
numracescats = ['1','2','3','4','5','6']
racecats = [f'{i:02d}' for i in range(1,64)]
gqinstcats = ['INST', 'NONINST']
gqmajortypecats = ['Correctional', 'Juvenile', 'Nursing', 'OtherInst', 'College', 'Military', 'OtherNoninst']
mcdstates = ['09', '25', '23', '26', '27', '33', '34', '36', '42', '44', '50', '55']

def assign_racealone_or_incomb(df):
    df = df.copy()  
    whitecols = [1,7,8,9,10,11,22,23,24,25,26,27,28,29,30,31,42,43,44,45,46,47,48,49,50,51,57,58,59,60,61,63]
    blackcols = [2,7,12,13,14,15,22,23,24,25,32,33,34,35,36,37,42,43,44,45,46,47,52,53,54,55,57,58,59,60,62,63]
    aiancols =  [3,8,12,16,17,18,22,26,27,28,32,33,34,38,39,40,42,43,44,48,49,50,52,53,54,56,57,58,59,61,62,63]
    asiancols = [4,9,13,16,19,20,23,26,29,30,32,35,36,38,39,41,42,45,46,48,49,51,52,53,55,56,57,58,60,61,62,63]
    nhopicols = [5,10,14,17,19,21,24,27,29,31,33,35,37,38,40,41,43,45,47,48,50,51,52,54,55,56,57,59,60,61,62,63]
    sorcols =   [6,11,15,18,20,21,25,28,30,31,34,36,37,39,40,41,44,46,47,49,50,51,53,54,55,56,58,59,60,61,62,63]
    df['whitealone-or-incomb'] = np.where(df['CENRACE'].isin(whitecols),1,0)
    df['blackalone-or-incomb'] = np.where(df['CENRACE'].isin(blackcols),1,0)
    df['aianalone-or-incomb'] = np.where(df['CENRACE'].isin(aiancols),1,0)
    df['asianalone-or-incomb'] = np.where(df['CENRACE'].isin(asiancols),1,0)
    df['nhopialone-or-incomb'] = np.where(df['CENRACE'].isin(nhopicols),1,0)
    df['soralone-or-incomb'] = np.where(df['CENRACE'].isin(sorcols),1,0)
    return df

def assign_raceincomb(df):
    df = df.copy()  
    whitecols = [7,8,9,10,11,22,23,24,25,26,27,28,29,30,31,42,43,44,45,46,47,48,49,50,51,57,58,59,60,61,63]
    blackcols = [7,12,13,14,15,22,23,24,25,32,33,34,35,36,37,42,43,44,45,46,47,52,53,54,55,57,58,59,60,62,63]
    aiancols =  [8,12,16,17,18,22,26,27,28,32,33,34,38,39,40,42,43,44,48,49,50,52,53,54,56,57,58,59,61,62,63]
    asiancols = [9,13,16,19,20,23,26,29,30,32,35,36,38,39,41,42,45,46,48,49,51,52,53,55,56,57,58,60,61,62,63]
    nhopicols = [10,14,17,19,21,24,27,29,31,33,35,37,38,40,41,43,45,47,48,50,51,52,54,55,56,57,59,60,61,62,63]
    sorcols =   [11,15,18,20,21,25,28,30,31,34,36,37,39,40,41,44,46,47,49,50,51,53,54,55,56,58,59,60,61,62,63]
    df['white-incomb'] = np.where(df['CENRACE'].isin(whitecols),1,0)
    df['black-incomb'] = np.where(df['CENRACE'].isin(blackcols),1,0)
    df['aian-incomb'] = np.where(df['CENRACE'].isin(aiancols),1,0)
    df['asian-incomb'] = np.where(df['CENRACE'].isin(asiancols),1,0)
    df['nhopi-incomb'] = np.where(df['CENRACE'].isin(nhopicols),1,0)
    df['sor-incomb'] = np.where(df['CENRACE'].isin(sorcols),1,0)
    return df

print(f"Reading {MDFPERFILEPATH}")
dfmdf = pd.read_table(f"{MDFPERFILEPATH}", sep = ",", comment="#", usecols = ['TABBLKST','TABBLKCOU', 'TABTRACTCE', 'TABBLK', 'TABBLKGRPCE', 'RTYPE', 'RELSHIP', 'GQTYPE','QSEX','QAGE', 'CENHISP','CENRACE'], dtype = { 'SCHEMA_TYPE_CODE': 'object', 'SCHEMA_BUILD_ID': 'object', 'TABBLKST': 'object', 'TABBLKCOU': 'object', 'TABTRACTCE': 'object', 'TABBLKGRPCE': 'object', 'TABBLK': 'object', 'EPNUM': 'int64', 'RTYPE': 'object', 'GQTYPE': 'int64', 'RELSHIP': 'int64', 'QSEX': 'object', 'QAGE': 'int64', 'CENHISP': 'object', 'CENRACE': 'int64', 'CITIZEN': 'object', 'LIVE_ALONE': 'int64' })
if runPRhere and separatePR: # If you want to run PR from this program and it's a separate file, then input it from the MDFPERPRFILEPATH
    dfmdfpr = pd.read_table(f"{MDFPERPRFILEPATH}", sep = "|", comment="#", usecols = ['TABBLKST','TABBLKCOU', 'TABTRACTCE', 'TABBLK', 'TABBLKGRPCE', 'RTYPE', 'RELSHIP', 'GQTYPE','QSEX','QAGE', 'CENHISP','CENRACE'], dtype = { 'SCHEMA_TYPE_CODE': 'object', 'SCHEMA_BUILD_ID': 'object', 'TABBLKST': 'object', 'TABBLKCOU': 'object', 'TABTRACTCE': 'object', 'TABBLKGRPCE': 'object', 'TABBLK': 'object', 'EPNUM': 'int64', 'RTYPE': 'object', 'GQTYPE': 'int64', 'RELSHIP': 'int64', 'QSEX': 'object', 'QAGE': 'int64', 'CENHISP': 'object', 'CENRACE': 'int64', 'CITIZEN': 'object', 'LIVE_ALONE': 'int64' })
else: # If you want to run PR from this program and the data are part of the MDF, subset it from the MDF. (If PR is not available, this will return an empty data frame that will be deleted later.) 
    dfmdfpr = dfmdf[dfmdf['TABBLKST'] == '72'] 
dfmdf = dfmdf[dfmdf['TABBLKST'] != "72"]
print("{} MDF Data Loaded OK".format(datetime.now()))
dfmdf['BlockGEOID'] = dfmdf['TABBLKST'] + dfmdf['TABBLKCOU'] + dfmdf['TABTRACTCE'] + dfmdf['TABBLK']
print("{} MDF Block GEOID Created".format(datetime.now()))
dfmdf = pd.merge(dfmdf, allgeos[['BlockGEOID','IncPlaceGEOID','AIANNHGEOID','MCDGEOID','FedAIRGEOID','OTSAGEOID','ANVSAGEOID','SchDistEGEOID','SchDistSGEOID', 'SchDistUGEOID']], on="BlockGEOID", how="left", validate=mergeValidation)
print("{} MDF Non-Spine GEOIDs Merged In".format(datetime.now()))
dfhdf = pd.read_csv(f"{HDFDATADIR}/cef.csv", usecols = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLK', 'TABBLKGRPCE', 'QSEX','QAGE','CENRACE','RTYPE','GQTYPE', 'CENHISP'], dtype = { 'TABBLKST': 'object', 'TABBLKCOU': 'object', 'TABTRACTCE': 'object', 'TABBLK': 'object', 'TABBLKGRPCE': 'object', 'QSEX': 'object', 'QAGE': 'int64', 'QSPAN': 'object', 'CENRACE': 'int64', 'RTYPE': 'object', 'GQTYPE': 'int64', 'CENHISP': 'object' })
dfhdfpr = dfhdf[dfhdf['TABBLKST'] == '72']
dfhdf = dfhdf[dfhdf['TABBLKST'] != "72"] 
print("{} HDF Data Loaded OK".format(datetime.now()))
dfhdf['BlockGEOID'] = dfhdf['TABBLKST'] + dfhdf['TABBLKCOU'] + dfhdf['TABTRACTCE'] + dfhdf['TABBLK']
print("{} HDF Block GEOID Created".format(datetime.now()))
dfhdf = pd.merge(dfhdf, allgeos[['BlockGEOID','IncPlaceGEOID','AIANNHGEOID','MCDGEOID','FedAIRGEOID','OTSAGEOID','ANVSAGEOID','SchDistEGEOID','SchDistSGEOID', 'SchDistUGEOID']], on="BlockGEOID", how="left", validate=mergeValidation)
print("{} HDF Non-Spine GEOIDs Merged In".format(datetime.now()))


dfhdf['RACEALONE'] = np.where(dfhdf['CENRACE'] > 6, 7, dfhdf['CENRACE'])
dfmdf['RACEALONE'] = np.where(dfmdf['CENRACE'] > 6, 7, dfmdf['CENRACE'])
dfhdf['NUMRACES'] = pd.cut(dfhdf['CENRACE'], [1,7,22,42,57,63,64], include_lowest=True, right=False, labels=['1','2','3','4','5','6'])
dfmdf['NUMRACES'] = pd.cut(dfmdf['CENRACE'], [1,7,22,42,57,63,64], include_lowest=True, right=False, labels=['1','2','3','4','5','6'])
print("{} Created RACEALONE & NUMRACES".format(datetime.now()))

dfhdf['GQINST'] = pd.cut(dfhdf['GQTYPE'], [100,499, 999], include_lowest=True, right=False, labels=gqinstcats)
dfmdf['GQINST'] = pd.cut(dfmdf['GQTYPE'], [100,499, 999], include_lowest=True, right=False, labels=gqinstcats)
dfhdf['GQMAJORTYPE'] = pd.cut(dfhdf['GQTYPE'], [100,200,300,400,500,600,700,999], include_lowest=True, right=False, labels=gqmajortypecats)
dfmdf['GQMAJORTYPE'] = pd.cut(dfmdf['GQTYPE'], [100,200,300,400,500,600,700,999], include_lowest=True, right=False, labels=gqmajortypecats)

dfhdf['QAGE_5Y']=pd.cut(dfhdf['QAGE'],list(range(0,111,5)) + [116], include_lowest=True, right=False)
dfmdf['QAGE_5Y']=pd.cut(dfmdf['QAGE'],list(range(0,111,5)) + [116], include_lowest=True, right=False)
print("{} Created QAGE_5Y".format(datetime.now()))

dfhdf['QAGE_1Y_100']=pd.cut(dfhdf['QAGE'],list(range(0,100,1)) + list(range(100,115,5)) + [116], include_lowest=True, right=False)
dfmdf['QAGE_1Y_100']=pd.cut(dfmdf['QAGE'],list(range(0,100,1)) + list(range(100,115,5)) + [116], include_lowest=True, right=False)
print("{} Created QAGE_1Y_100".format(datetime.now()))

hdfnation_1y_100 = dfhdf.groupby(['QAGE_1Y_100']).size().reset_index(name='HDF_Population')
mdfnation_1y_100 = dfmdf.groupby(['QAGE_1Y_100']).size().reset_index(name='MDF_Population')
nation_1y_100 = pd.merge(hdfnation_1y_100, mdfnation_1y_100, on="QAGE_1Y_100", how="inner")
nation_1y_100['QAGE_1Y_100'] = nation_1y_100['QAGE_1Y_100'].astype("object") ## get around pandas < 1.4.3 bug
nation_1y_100.to_csv(f"{OUTPUTDIR}/nation_1y_100.csv", index=False)

hdfnation_gq1y_100 = dfhdf[dfhdf['GQTYPE'] > 0].groupby(['QAGE_1Y_100']).size().reset_index(name='HDF_Population')
mdfnation_gq1y_100 = dfmdf[dfmdf['GQTYPE'] > 0].groupby(['QAGE_1Y_100']).size().reset_index(name='MDF_Population')
nation_gq1y_100 = pd.merge(hdfnation_gq1y_100, mdfnation_gq1y_100, on="QAGE_1Y_100", how="inner")
nation_gq1y_100['QAGE_1Y_100'] = nation_gq1y_100['QAGE_1Y_100'].astype("object") ## get around pandas < 1.4.3 bug
nation_gq1y_100.to_csv(f"{OUTPUTDIR}/nation_gq1y_100.csv", index=False)

dfhdf['QAGE_3G']= pd.cut(dfhdf['QAGE'],[0,18,65,116], include_lowest=True, right=False)
dfmdf['QAGE_3G']= pd.cut(dfmdf['QAGE'],[0,18,65,116], include_lowest=True, right=False)
print("{} Created QAGE_3G".format(datetime.now()))

if runPRhere:
    dfhdfpr['RACEALONE'] = np.where(dfhdfpr['CENRACE'] > 6, 7, dfhdfpr['CENRACE'])
    dfhdfpr['NUMRACES'] = pd.cut(dfhdfpr['CENRACE'], [1,7,22,42,57,63,64], include_lowest=True, right=False, labels=['1','2','3','4','5','6'])
    # dfhdfpr['GQINST']
    # dfhdfpr['GQMAJORTYPE']
    dfhdfpr['QAGE_3G']= pd.cut(dfhdfpr['QAGE'],[0,18,65,116], include_lowest=True, right=False)
    dfhdfpr['QAGE_5Y']=pd.cut(dfhdfpr['QAGE'],list(range(0,111,5)) + [116], include_lowest=True, right=False)
    dfmdfpr['RACEALONE'] = np.where(dfmdfpr['CENRACE'] > 6, 7, dfmdfpr['CENRACE'])
    dfmdfpr['NUMRACES'] = pd.cut(dfmdfpr['CENRACE'], [1,7,22,42,57,63,64], include_lowest=True, right=False, labels=['1','2','3','4','5','6'])
    # dfmdfpr['GQINST']
    # dfmdfpr['GQMAJORTYPE']
    dfmdfpr['QAGE_3G']= pd.cut(dfmdfpr['QAGE'],[0,18,65,116], include_lowest=True, right=False)
    dfmdfpr['QAGE_5Y']=pd.cut(dfmdfpr['QAGE'],list(range(0,111,5)) + [116], include_lowest=True, right=False)
    print("{} Created Puerto Rico Recodes".format(datetime.now()))
else:
    print("{} Skipping Puerto Rico Recodes".format(datetime.now()))
    del dfmdfpr
    del dfhdfpr
    print("{} Deleted Puerto Rico Data Files".format(datetime.now()))

qage_5y_cats = list(dfhdf['QAGE_5Y'].cat.categories)
qage_3g_cats = list(dfhdf['QAGE_3G'].cat.categories)

state_racealone_index = pd.MultiIndex.from_product([allstates,racealonecats], names=['GEOID','RACEALONE'])
county_racealone_index = pd.MultiIndex.from_product([allcounties,racealonecats],names=['GEOID','RACEALONE'])
place_racealone_index = pd.MultiIndex.from_product([allplaces,racealonecats],names=['IncPlaceGEOID','RACEALONE'])
tract_racealone_index = pd.MultiIndex.from_product([alltracts,racealonecats],names=['GEOID','RACEALONE'])
blockgroup_racealone_index = pd.MultiIndex.from_product([allblockgroups,racealonecats],names=['GEOID','RACEALONE'])
aiannh_racealone_index = pd.MultiIndex.from_product([allaiannh,racealonecats],names=['AIANNHGEOID','RACEALONE'])

print("{} Start Total Population".format(datetime.now()))

# Counties Total Population
hdfcounties_totalpop = dfhdf.groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
mdfcounties_totalpop = dfmdf.groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
counties_totalpop =  pd.merge(hdfcounties_totalpop, mdfcounties_totalpop, on='GEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
counties_totalpop.to_csv(f"{OUTPUTDIR}/counties_totalpop.csv", index=False)
ss = counties_totalpop.pipe(calculate_ss, geography="County", sizecategory = "All", characteristic = "Total Population")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Counties Total Population By Size
counties_totalpop['Total_PopSize'] = pd.cut(counties_totalpop['HDF_Population'], [0,1000,5000,10000,50000,100000,np.inf], include_lowest=True, right=False)
for i in counties_totalpop['Total_PopSize'].cat.categories:
    ss = counties_totalpop[counties_totalpop['Total_PopSize'] == i].pipe(calculate_ss, geography="County", sizecategory = str(i), characteristic = "Total Population")
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

#
smallestpop = counties_totalpop['Total_PopSize'].cat.categories[0]
print(smallestpop)
counties_lt1000 = list(counties_totalpop.loc[counties_totalpop['Total_PopSize'] == smallestpop, 'GEOID'])
print(counties_lt1000)
counties_lt1000index = pd.Index(counties_lt1000, name="GEOID")

# Do not delete counties_totalpop

# Places Total Population
hdfplaces_totalpop = dfhdf.groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index('IncPlaceGEOID').reindex(allplacesindex, fill_value=0).reset_index()
mdfplaces_totalpop = dfmdf.groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index('IncPlaceGEOID').reindex(allplacesindex, fill_value=0).reset_index()
places_totalpop =  pd.merge(hdfplaces_totalpop, mdfplaces_totalpop, on='IncPlaceGEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
places_totalpop.to_csv(f"{OUTPUTDIR}/places_totalpop.csv", index=False)
ss = places_totalpop.pipe(calculate_ss, geography="Place", sizecategory = "All", characteristic = "Total Population")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Places Total Population By Size
places_totalpop['Total_PopSize'] = pd.cut(places_totalpop['HDF_Population'], [0,500,1000,5000,10000,50000,100000,np.inf], include_lowest=True, right=False)
for i in places_totalpop['Total_PopSize'].cat.categories:
    ss = places_totalpop[places_totalpop['Total_PopSize'] == i].pipe(calculate_ss, geography="Place", sizecategory = str(i), characteristic = "Total Population")
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

#
smallestpop = places_totalpop['Total_PopSize'].cat.categories[0]
print(smallestpop)
places_lt500 = list(places_totalpop.loc[places_totalpop['Total_PopSize'] == smallestpop, 'IncPlaceGEOID'])
print(len(places_lt500))
places_lt500index = pd.Index(places_lt500, name="IncPlaceGEOID")

# Tracts Total Population
hdftracts_totalpop = dfhdf.groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
mdftracts_totalpop = dfmdf.groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
tracts_totalpop =  pd.merge(hdftracts_totalpop, mdftracts_totalpop, on='GEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
tracts_totalpop.to_csv(f"{OUTPUTDIR}/tracts_totalpop.csv", index=False)
ss = tracts_totalpop.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "Total Population")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

del hdftracts_totalpop
del mdftracts_totalpop
del tracts_totalpop

# Blocks By Rurality
hdfblocks_totalpop = dfhdf.groupby(['TABBLKST','TABBLKCOU','TABTRACTCE', 'TABBLK']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE + x.TABBLK).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLK']).set_index('GEOID').reindex(allblocksindex, fill_value=0).reset_index()
mdfblocks_totalpop = dfmdf.groupby(['TABBLKST','TABBLKCOU','TABTRACTCE', 'TABBLK']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE + x.TABBLK).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLK']).set_index('GEOID').reindex(allblocksindex, fill_value=0).reset_index()
blocks_totalpop =  pd.merge(hdfblocks_totalpop, mdfblocks_totalpop, on='GEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
blocks_totalpop = pd.merge(blocks_totalpop, allgeos[['BlockGEOID','UR']], left_on="GEOID", right_on = "BlockGEOID", how="outer", validate=mergeValidation)
ss = blocks_totalpop[blocks_totalpop['UR'] == "U"].pipe(calculate_ss, geography="Block", sizecategory = "All", characteristic = "Total Population for Urban Blocks")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
ss = blocks_totalpop[blocks_totalpop['UR'] == "R"].pipe(calculate_ss, geography="Block", sizecategory = "All", characteristic = "Total Population for Rural Blocks")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

del hdfblocks_totalpop
del mdfblocks_totalpop
del blocks_totalpop

if runPRhere:
    # PR Counties/Municipios Total Population
    hdfcountiespr_totalpop = dfhdfpr.groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(allcountiesprindex, fill_value=0).reset_index()
    mdfcountiespr_totalpop = dfmdfpr.groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(allcountiesprindex, fill_value=0).reset_index()
    countiespr_totalpop =  pd.merge(hdfcountiespr_totalpop, mdfcountiespr_totalpop, on='GEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    countiespr_totalpop.to_csv(f"{OUTPUTDIR}/countiespr_totalpop.csv", index=False)
    ss = countiespr_totalpop.pipe(calculate_ss, geography="PR County/Municipio", sizecategory = "All", characteristic = "Total Population")
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

    # PR Tracts Total Population
    hdftractspr_totalpop = dfhdfpr.groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsprindex, fill_value=0).reset_index()
    mdftractspr_totalpop = dfmdfpr.groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsprindex, fill_value=0).reset_index()
    tractspr_totalpop =  pd.merge(hdftractspr_totalpop, mdftractspr_totalpop, on='GEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    tractspr_totalpop.to_csv(f"{OUTPUTDIR}/tractspr_totalpop.csv", index=False)
    ss = tractspr_totalpop.pipe(calculate_ss, geography="PR Tract", sizecategory = "All", characteristic = "Total Population")
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Elem School Districts Total Population
hdfelemschdists_totalpop = dfhdf.groupby(['SchDistEGEOID']).size().reset_index(name='HDF_Population').set_index('SchDistEGEOID').reindex(allelemschdistsindex, fill_value=0).reset_index()
mdfelemschdists_totalpop = dfmdf.groupby(['SchDistEGEOID']).size().reset_index(name='MDF_Population').set_index('SchDistEGEOID').reindex(allelemschdistsindex, fill_value=0).reset_index()
elemschdists_totalpop =  pd.merge(hdfelemschdists_totalpop, mdfelemschdists_totalpop, on='SchDistEGEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
elemschdists_totalpop.to_csv(f"{OUTPUTDIR}/elemschdists_totalpop.csv", index=False)
ss = elemschdists_totalpop.pipe(calculate_ss, geography="ESD", sizecategory = "All", characteristic = "Total Population")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Elem School Districts Total Population By Size
elemschdists_totalpop['Total_PopSize'] = pd.cut(elemschdists_totalpop['HDF_Population'], [0,1000,5000,10000,50000,100000,np.inf], include_lowest=True, right=False)
for i in elemschdists_totalpop['Total_PopSize'].cat.categories:
    ss = elemschdists_totalpop[elemschdists_totalpop['Total_PopSize'] == i].pipe(calculate_ss, geography="ESD", sizecategory = str(i), characteristic = "Total Population")
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Sec School Districts Total Population
hdfsecschdists_totalpop = dfhdf.groupby(['SchDistSGEOID']).size().reset_index(name='HDF_Population').set_index('SchDistSGEOID').reindex(allsecschdistsindex, fill_value=0).reset_index()
mdfsecschdists_totalpop = dfmdf.groupby(['SchDistSGEOID']).size().reset_index(name='MDF_Population').set_index('SchDistSGEOID').reindex(allsecschdistsindex, fill_value=0).reset_index()
secschdists_totalpop =  pd.merge(hdfsecschdists_totalpop, mdfsecschdists_totalpop, on='SchDistSGEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
secschdists_totalpop.to_csv(f"{OUTPUTDIR}/secschdists_totalpop.csv", index=False)
ss = secschdists_totalpop.pipe(calculate_ss, geography="SSD", sizecategory = "All", characteristic = "Total Population")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Sec School Districts Total Population By Size
secschdists_totalpop['Total_PopSize'] = pd.cut(secschdists_totalpop['HDF_Population'], [0,1000,5000,10000,50000,100000,np.inf], include_lowest=True, right=False)
for i in secschdists_totalpop['Total_PopSize'].cat.categories:
    ss = secschdists_totalpop[secschdists_totalpop['Total_PopSize'] == i].pipe(calculate_ss, geography="SSD", sizecategory = str(i), characteristic = "Total Population")
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Uni School Districts Total Population
hdfunischdists_totalpop = dfhdf.groupby(['SchDistUGEOID']).size().reset_index(name='HDF_Population').set_index('SchDistUGEOID').reindex(allunischdistsindex, fill_value=0).reset_index()
mdfunischdists_totalpop = dfmdf.groupby(['SchDistUGEOID']).size().reset_index(name='MDF_Population').set_index('SchDistUGEOID').reindex(allunischdistsindex, fill_value=0).reset_index()
unischdists_totalpop =  pd.merge(hdfunischdists_totalpop, mdfunischdists_totalpop, on='SchDistUGEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
unischdists_totalpop.to_csv(f"{OUTPUTDIR}/unischdists_totalpop.csv", index=False)
ss = unischdists_totalpop.pipe(calculate_ss, geography="USD", sizecategory = "All", characteristic = "Total Population")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Uni School Districts Total Population By Size
unischdists_totalpop['Total_PopSize'] = pd.cut(unischdists_totalpop['HDF_Population'], [0,1000,5000,10000,50000,100000,np.inf], include_lowest=True, right=False)
for i in unischdists_totalpop['Total_PopSize'].cat.categories:
    ss = unischdists_totalpop[unischdists_totalpop['Total_PopSize'] == i].pipe(calculate_ss, geography="USD", sizecategory = str(i), characteristic = "Total Population")
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Minor Civil Division Total Population
hdfmcds_totalpop = dfhdf.groupby(['MCDGEOID']).size().reset_index(name='HDF_Population').set_index('MCDGEOID').reindex(allmcdsindex, fill_value=0).reset_index()
mdfmcds_totalpop = dfmdf.groupby(['MCDGEOID']).size().reset_index(name='MDF_Population').set_index('MCDGEOID').reindex(allmcdsindex, fill_value=0).reset_index()
mcds_totalpop =  pd.merge(hdfmcds_totalpop, mdfmcds_totalpop, on='MCDGEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
mcds_totalpop.to_csv(f"{OUTPUTDIR}/mcds_totalpop.csv", index=False)
ss = mcds_totalpop.pipe(calculate_ss, geography="MCD", sizecategory = "All", characteristic = "Total Population")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Minor Civil Division Total Population By Size
mcds_totalpop['Total_PopSize'] = pd.cut(mcds_totalpop['HDF_Population'], [0,1000,5000,10000,50000,100000,np.inf], include_lowest=True, right=False)
for i in mcds_totalpop['Total_PopSize'].cat.categories:
    ss = mcds_totalpop[mcds_totalpop['Total_PopSize'] == i].pipe(calculate_ss, geography="MCD", sizecategory = str(i), characteristic = "Total Population")
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Federal AIR Total Population
hdffedairs_totalpop = dfhdf.groupby(['FedAIRGEOID']).size().reset_index(name='HDF_Population').set_index('FedAIRGEOID').reindex(allfedairsindex, fill_value=0).reset_index()
mdffedairs_totalpop = dfmdf.groupby(['FedAIRGEOID']).size().reset_index(name='MDF_Population').set_index('FedAIRGEOID').reindex(allfedairsindex, fill_value=0).reset_index()
fedairs_totalpop =  pd.merge(hdffedairs_totalpop, mdffedairs_totalpop, on='FedAIRGEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
fedairs_totalpop.to_csv(f"{OUTPUTDIR}/fedairs_totalpop.csv", index=False)
ss = fedairs_totalpop.pipe(calculate_ss, geography="Fed AIR", sizecategory = "All", characteristic = "Total Population")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Federal AIR Total Population By Size
fedairs_totalpop['Total_PopSize'] = pd.cut(fedairs_totalpop['HDF_Population'], [0,100,1000,10000,np.inf], include_lowest=True, right=False)
for i in fedairs_totalpop['Total_PopSize'].cat.categories:
    ss = fedairs_totalpop[fedairs_totalpop['Total_PopSize'] == i].pipe(calculate_ss, geography="Fed AIR", sizecategory = str(i), characteristic = "Total Population")
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# OTSA Total Population
hdfotsas_totalpop = dfhdf.groupby(['OTSAGEOID']).size().reset_index(name='HDF_Population').set_index('OTSAGEOID').reindex(allotsasindex, fill_value=0).reset_index()
mdfotsas_totalpop = dfmdf.groupby(['OTSAGEOID']).size().reset_index(name='MDF_Population').set_index('OTSAGEOID').reindex(allotsasindex, fill_value=0).reset_index()
otsas_totalpop =  pd.merge(hdfotsas_totalpop, mdfotsas_totalpop, on='OTSAGEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
otsas_totalpop.to_csv(f"{OUTPUTDIR}/otsas_totalpop.csv", index=False)
ss = otsas_totalpop.pipe(calculate_ss, geography="OTSA", sizecategory = "All", characteristic = "Total Population")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# ANVSA Total Population
hdfanvsas_totalpop = dfhdf.groupby(['ANVSAGEOID']).size().reset_index(name='HDF_Population').set_index('ANVSAGEOID').reindex(allanvsasindex, fill_value=0).reset_index()
mdfanvsas_totalpop = dfmdf.groupby(['ANVSAGEOID']).size().reset_index(name='MDF_Population').set_index('ANVSAGEOID').reindex(allanvsasindex, fill_value=0).reset_index()
anvsas_totalpop =  pd.merge(hdfanvsas_totalpop, mdfanvsas_totalpop, on='ANVSAGEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
anvsas_totalpop.to_csv(f"{OUTPUTDIR}/anvsas_totalpop.csv", index=False)
ss = anvsas_totalpop.pipe(calculate_ss, geography="ANVSA", sizecategory = "All", characteristic = "Total Population")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# ANVSA Total Population By Size
anvsas_totalpop['Total_PopSize'] = pd.cut(anvsas_totalpop['HDF_Population'], [0,100,1000,10000,np.inf], include_lowest=True, right=False)
for i in anvsas_totalpop['Total_PopSize'].cat.categories:
    ss = anvsas_totalpop[anvsas_totalpop['Total_PopSize'] == i].pipe(calculate_ss, geography="ANVSA", sizecategory = str(i), characteristic = "Total Population")
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# AIANNH Areas Total Population
hdfaiannh_totalpop = dfhdf.groupby(['AIANNHGEOID']).size().reset_index(name='HDF_Population').set_index('AIANNHGEOID').reindex(allaiannhindex, fill_value=0).reset_index()
mdfaiannh_totalpop = dfmdf.groupby(['AIANNHGEOID']).size().reset_index(name='MDF_Population').set_index('AIANNHGEOID').reindex(allaiannhindex, fill_value=0).reset_index()
aiannh_totalpop =  pd.merge(hdfaiannh_totalpop, mdfaiannh_totalpop, on='AIANNHGEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
aiannh_totalpop.to_csv(f"{OUTPUTDIR}/aiannh_totalpop.csv", index=False)
ss = aiannh_totalpop.pipe(calculate_ss, geography="AIANNH Area", sizecategory = "All", characteristic = "Total Population")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# AIANNH Areas Total Population By Size
aiannh_totalpop['Total_PopSize'] = pd.cut(aiannh_totalpop['HDF_Population'], [0,100,500,1000,np.inf], include_lowest=True, right=False)
for i in aiannh_totalpop['Total_PopSize'].cat.categories:
    ss = aiannh_totalpop[aiannh_totalpop['Total_PopSize'] == i].pipe(calculate_ss, geography="AIANNH Area", sizecategory = str(i), characteristic = "Total Population")
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

print("{} Total Population Done".format(datetime.now()))

# County Total Population 18+
hdfcounties_totalpop18p = dfhdf[(dfhdf['QAGE'] >= 18)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
mdfcounties_totalpop18p = dfmdf[(dfmdf['QAGE'] >= 18)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
counties_totalpop18p =  pd.merge(hdfcounties_totalpop18p, mdfcounties_totalpop18p, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
ss = counties_totalpop18p.pipe(calculate_ss, geography="County", sizecategory = "All", characteristic = "Total Population Aged 18+")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

counties_totalpop18p = pd.merge(counties_totalpop18p, counties_totalpop[['GEOID','Total_PopSize']], on ='GEOID', how='outer', validate= mergeValidation)
for i in counties_totalpop18p['Total_PopSize'].cat.categories:
    ss = counties_totalpop18p[counties_totalpop18p['Total_PopSize'] == i].pipe(calculate_ss, geography="County", sizecategory = str(i), characteristic = "Total Population Aged 18+")
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Places Total Population 18+
hdfplaces_totalpop18p = dfhdf[(dfhdf['QAGE'] >= 18)].groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index('IncPlaceGEOID').reindex(allplacesindex, fill_value=0).reset_index()
mdfplaces_totalpop18p = dfmdf[(dfmdf['QAGE'] >= 18)].groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index('IncPlaceGEOID').reindex(allplacesindex, fill_value=0).reset_index()
places_totalpop18p =  pd.merge(hdfplaces_totalpop18p, mdfplaces_totalpop18p, on='IncPlaceGEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
ss = places_totalpop18p.pipe(calculate_ss, geography="Place", sizecategory = "All", characteristic = "Total Population Aged 18+")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

places_totalpop18p = pd.merge(places_totalpop18p, places_totalpop[['IncPlaceGEOID','Total_PopSize']], on ='IncPlaceGEOID', how='outer', validate= mergeValidation)
for i in places_totalpop18p['Total_PopSize'].cat.categories:
    ss = places_totalpop18p[places_totalpop18p['Total_PopSize'] == i].pipe(calculate_ss, geography="Place", sizecategory = str(i), characteristic = "Total Population Aged 18+")
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Tracts Total Population 18+
hdftracts_totalpop18p = dfhdf[(dfhdf['QAGE'] >= 18)].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
mdftracts_totalpop18p = dfmdf[(dfmdf['QAGE'] >= 18)].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
tracts_totalpop18p =  pd.merge(hdftracts_totalpop18p, mdftracts_totalpop18p, on='GEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
ss = tracts_totalpop18p.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "Total Population Aged 18+")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

if runPRhere:
    # PR Counties/Municipios Total Population 18+
    hdfcountiespr_totalpop18p = dfhdfpr[(dfhdfpr['QAGE'] >= 18)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(allcountiesprindex, fill_value=0).reset_index()
    mdfcountiespr_totalpop18p = dfmdfpr[(dfmdfpr['QAGE'] >= 18)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(allcountiesprindex, fill_value=0).reset_index()
    countiespr_totalpop18p =  pd.merge(hdfcountiespr_totalpop18p, mdfcountiespr_totalpop18p, on='GEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = countiespr_totalpop18p.pipe(calculate_ss, geography="PR County/Municipio", sizecategory = "All", characteristic = "Total Population Aged 18+")
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

    # PR Tracts Total Population 18+
    hdftractspr_totalpop18p = dfhdfpr[(dfhdfpr['QAGE'] >= 18)].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsprindex, fill_value=0).reset_index()
    mdftractspr_totalpop18p = dfmdfpr[(dfmdfpr['QAGE'] >= 18)].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsprindex, fill_value=0).reset_index()
    tractspr_totalpop18p =  pd.merge(hdftractspr_totalpop18p, mdftractspr_totalpop18p, on='GEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = tractspr_totalpop18p.pipe(calculate_ss, geography="PR Tract", sizecategory = "All", characteristic = "Total Population Aged 18+")
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Minor Civil Division Total Population 18+
hdfmcds_totalpop18p = dfhdf[(dfhdf['QAGE'] >= 18)].groupby(['MCDGEOID']).size().reset_index(name='HDF_Population').set_index('MCDGEOID').reindex(allmcdsindex, fill_value=0).reset_index()
mdfmcds_totalpop18p = dfmdf[(dfmdf['QAGE'] >= 18)].groupby(['MCDGEOID']).size().reset_index(name='MDF_Population').set_index('MCDGEOID').reindex(allmcdsindex, fill_value=0).reset_index()
mcds_totalpop18p =  pd.merge(hdfmcds_totalpop18p, mdfmcds_totalpop18p, on='MCDGEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
ss = mcds_totalpop18p.pipe(calculate_ss, geography="MCD", sizecategory = "All", characteristic = "Total Population Aged 18+")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

mcds_totalpop18p = pd.merge(mcds_totalpop18p, mcds_totalpop[['MCDGEOID','Total_PopSize']], on ='MCDGEOID', how='outer', validate= mergeValidation)
for i in mcds_totalpop18p['Total_PopSize'].cat.categories:
    ss = mcds_totalpop18p[mcds_totalpop18p['Total_PopSize'] == i].pipe(calculate_ss, geography="MCD", sizecategory = str(i), characteristic = "Total Population Aged 18+")
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Federal AIR Total Population 18+
hdffedairs_totalpop18p = dfhdf[(dfhdf['QAGE'] >= 18)].groupby(['FedAIRGEOID']).size().reset_index(name='HDF_Population').set_index('FedAIRGEOID').reindex(allfedairsindex, fill_value=0).reset_index()
mdffedairs_totalpop18p = dfmdf[(dfmdf['QAGE'] >= 18)].groupby(['FedAIRGEOID']).size().reset_index(name='MDF_Population').set_index('FedAIRGEOID').reindex(allfedairsindex, fill_value=0).reset_index()
fedairs_totalpop18p =  pd.merge(hdffedairs_totalpop18p, mdffedairs_totalpop18p, on='FedAIRGEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
ss = fedairs_totalpop18p.pipe(calculate_ss, geography="Fed AIR", sizecategory = "All", characteristic = "Total Population Aged 18+")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

fedairs_totalpop18p = pd.merge(fedairs_totalpop18p, fedairs_totalpop[['FedAIRGEOID','Total_PopSize']], on ='FedAIRGEOID', how='outer', validate= mergeValidation)
for i in fedairs_totalpop18p['Total_PopSize'].cat.categories:
    ss = fedairs_totalpop18p[fedairs_totalpop18p['Total_PopSize'] == i].pipe(calculate_ss, geography="Fed AIR", sizecategory = str(i), characteristic = "Total Population Aged 18+")
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# OTSA Total Population 18+
hdfotsas_totalpop18p = dfhdf[(dfhdf['QAGE'] >= 18)].groupby(['OTSAGEOID']).size().reset_index(name='HDF_Population').set_index('OTSAGEOID').reindex(allotsasindex, fill_value=0).reset_index()
mdfotsas_totalpop18p = dfmdf[(dfmdf['QAGE'] >= 18)].groupby(['OTSAGEOID']).size().reset_index(name='MDF_Population').set_index('OTSAGEOID').reindex(allotsasindex, fill_value=0).reset_index()
otsas_totalpop18p =  pd.merge(hdfotsas_totalpop18p, mdfotsas_totalpop18p, on='OTSAGEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
ss = otsas_totalpop18p.pipe(calculate_ss, geography="OTSA", sizecategory = "All", characteristic = "Total Population Aged 18+")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# ANVSA Total Population 18+
hdfanvsas_totalpop18p = dfhdf[(dfhdf['QAGE'] >= 18)].groupby(['ANVSAGEOID']).size().reset_index(name='HDF_Population').set_index('ANVSAGEOID').reindex(allanvsasindex, fill_value=0).reset_index()
mdfanvsas_totalpop18p = dfmdf[(dfmdf['QAGE'] >= 18)].groupby(['ANVSAGEOID']).size().reset_index(name='MDF_Population').set_index('ANVSAGEOID').reindex(allanvsasindex, fill_value=0).reset_index()
anvsas_totalpop18p =  pd.merge(hdfanvsas_totalpop18p, mdfanvsas_totalpop18p, on='ANVSAGEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
ss = anvsas_totalpop18p.pipe(calculate_ss, geography="ANVSA", sizecategory = "All", characteristic = "Total Population Aged 18+")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

anvsas_totalpop18p = pd.merge(anvsas_totalpop18p, anvsas_totalpop[['ANVSAGEOID','Total_PopSize']], on ='ANVSAGEOID', how='outer', validate= mergeValidation)
for i in anvsas_totalpop18p['Total_PopSize'].cat.categories:
    ss = anvsas_totalpop18p[anvsas_totalpop18p['Total_PopSize'] == i].pipe(calculate_ss, geography="ANVSA", sizecategory = str(i), characteristic = "Total Population Aged 18+")
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

print("{} Total Population 18+ Done".format(datetime.now()))

# State Hispanic Origin
hdfstates_hisp = dfhdf[(dfhdf['CENHISP']=='2')].groupby(['TABBLKST']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index('GEOID').reindex(allstatesindex, fill_value=0).reset_index()
mdfstates_hisp = dfmdf[(dfmdf['CENHISP']=='2')].groupby(['TABBLKST']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index('GEOID').reindex(allstatesindex, fill_value=0).reset_index()
states_hisp = pd.merge(hdfstates_hisp, mdfstates_hisp, on = "GEOID", how="outer", validate = mergeValidation).pipe(calculate_stats)
ss = states_hisp.pipe(calculate_ss, geography = "State", sizecategory = "All", characteristic = "Hispanic")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

hdfstates_nonhisp = dfhdf[(dfhdf['CENHISP']=='1')].groupby(['TABBLKST']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index('GEOID').reindex(allstatesindex, fill_value=0).reset_index()
mdfstates_nonhisp = dfmdf[(dfmdf['CENHISP']=='1')].groupby(['TABBLKST']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index('GEOID').reindex(allstatesindex, fill_value=0).reset_index()
states_nonhisp = pd.merge(hdfstates_nonhisp, mdfstates_nonhisp, on = "GEOID", how="outer", validate = mergeValidation).pipe(calculate_stats)
ss = states_nonhisp.pipe(calculate_ss, geography = "State", sizecategory = "All", characteristic = "Not Hispanic")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# County Hispanic Origin
hdfcounties_hisp = dfhdf[(dfhdf['CENHISP']=='2')].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(allcountiesindex, fill_value=0).reset_index()
mdfcounties_hisp = dfmdf[(dfmdf['CENHISP']=='2')].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(allcountiesindex, fill_value=0).reset_index()
counties_hisp = pd.merge(hdfcounties_hisp, mdfcounties_hisp, on = "GEOID", how="outer", validate = mergeValidation).pipe(calculate_stats)
ss = counties_hisp.pipe(calculate_ss, geography = "County", sizecategory = "All", characteristic = "Hispanic")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

hdfcounties_nonhisp = dfhdf[(dfhdf['CENHISP']=='1')].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(allcountiesindex, fill_value=0).reset_index()
mdfcounties_nonhisp = dfmdf[(dfmdf['CENHISP']=='1')].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(allcountiesindex, fill_value=0).reset_index()
counties_nonhisp = pd.merge(hdfcounties_nonhisp, mdfcounties_nonhisp, on = "GEOID", how="outer", validate = mergeValidation).pipe(calculate_stats)
ss = counties_nonhisp.pipe(calculate_ss, geography = "County", sizecategory = "All", characteristic = "Not Hispanic")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

counties_hisp = counties_hisp.assign(Hisp_PopSize = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
for i in counties_hisp['Hisp_PopSize'].cat.categories:
    ss = counties_hisp[counties_hisp['Hisp_PopSize'] == i].pipe(calculate_ss, geography="County", sizecategory = str(i), characteristic = "Hispanic")
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

counties_nonhisp = counties_nonhisp.assign(Hisp_PopSize = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
for i in counties_nonhisp['Hisp_PopSize'].cat.categories:
    ss = counties_nonhisp[counties_nonhisp['Hisp_PopSize'] == i].pipe(calculate_ss, geography="County", sizecategory = str(i), characteristic = "Not Hispanic")
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Place Hispanic Origin
hdfplaces_hisp = dfhdf[(dfhdf['CENHISP']=='2')].groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index('IncPlaceGEOID').reindex(allplacesindex, fill_value=0).reset_index()
mdfplaces_hisp = dfmdf[(dfmdf['CENHISP']=='2')].groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index('IncPlaceGEOID').reindex(allplacesindex, fill_value=0).reset_index()
places_hisp = pd.merge(hdfplaces_hisp, mdfplaces_hisp, on = "IncPlaceGEOID", how="outer", validate = mergeValidation).pipe(calculate_stats)
ss = places_hisp.pipe(calculate_ss, geography = "Place", sizecategory = "All", characteristic = "Hispanic")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

hdfplaces_nonhisp = dfhdf[(dfhdf['CENHISP']=='1')].groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index('IncPlaceGEOID').reindex(allplacesindex, fill_value=0).reset_index()
mdfplaces_nonhisp = dfmdf[(dfmdf['CENHISP']=='1')].groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index('IncPlaceGEOID').reindex(allplacesindex, fill_value=0).reset_index()
places_nonhisp = pd.merge(hdfplaces_nonhisp, mdfplaces_nonhisp, on = "IncPlaceGEOID", how="outer", validate = mergeValidation).pipe(calculate_stats)
ss = places_nonhisp.pipe(calculate_ss, geography = "Place", sizecategory = "All", characteristic = "Not Hispanic")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

places_hisp = places_hisp.assign(Hisp_PopSize = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
for i in places_hisp['Hisp_PopSize'].cat.categories:
    ss = places_hisp[places_hisp['Hisp_PopSize'] == i].pipe(calculate_ss, geography="Place", sizecategory = str(i), characteristic = "Hispanic")
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

places_nonhisp = places_nonhisp.assign(Hisp_PopSize = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
for i in places_nonhisp['Hisp_PopSize'].cat.categories:
    ss = places_nonhisp[places_nonhisp['Hisp_PopSize'] == i].pipe(calculate_ss, geography="Place", sizecategory = str(i), characteristic = "Not Hispanic")
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Tracts Hispanic Origin
hdftracts_hisp = dfhdf[(dfhdf['CENHISP']=='2')].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
mdftracts_hisp = dfmdf[(dfmdf['CENHISP']=='2')].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
tracts_hisp =  pd.merge(hdftracts_hisp, mdftracts_hisp, on='GEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
ss = tracts_hisp.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "Hispanic")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

hdftracts_nonhisp = dfhdf[(dfhdf['CENHISP']=='1')].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
mdftracts_nonhisp = dfmdf[(dfmdf['CENHISP']=='1')].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
tracts_nonhisp =  pd.merge(hdftracts_nonhisp, mdftracts_nonhisp, on='GEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
ss = tracts_nonhisp.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "Not Hispanic")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

tracts_hisp = tracts_hisp.assign(Hisp_PopSize = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
for i in tracts_hisp['Hisp_PopSize'].cat.categories:
    ss = tracts_hisp[tracts_hisp['Hisp_PopSize'] == i].pipe(calculate_ss, geography="Tract", sizecategory = str(i), characteristic = "Hispanic")
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

tracts_nonhisp = tracts_nonhisp.assign(Hisp_PopSize = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
for i in tracts_nonhisp['Hisp_PopSize'].cat.categories:
    ss = tracts_nonhisp[tracts_nonhisp['Hisp_PopSize'] == i].pipe(calculate_ss, geography="Tract", sizecategory = str(i), characteristic = "Not Hispanic")
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

print("{} Hispanic Origin Done".format(datetime.now()))

# State Race Alone
hdfstates_racealone = dfhdf.groupby(['TABBLKST', 'RACEALONE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index(['GEOID', 'RACEALONE']).reindex(state_racealone_index, fill_value=0).reset_index()
mdfstates_racealone = dfmdf.groupby(['TABBLKST', 'RACEALONE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index(['GEOID', 'RACEALONE']).reindex(state_racealone_index, fill_value=0).reset_index()
states_racealone =  pd.merge(hdfstates_racealone, mdfstates_racealone, on=['GEOID','RACEALONE'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
for r in racealonecats:
    ss = states_racealone[states_racealone['RACEALONE'] == r].pipe(calculate_ss, geography="State", sizecategory = "All", characteristic = "{race}".format(race = racealonedict.get(r)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# County Race Alone
hdfcounties_racealone = dfhdf.groupby(['TABBLKST', 'TABBLKCOU', 'RACEALONE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID', 'RACEALONE']).reindex(county_racealone_index, fill_value=0).reset_index()
mdfcounties_racealone = dfmdf.groupby(['TABBLKST', 'TABBLKCOU', 'RACEALONE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID', 'RACEALONE']).reindex(county_racealone_index, fill_value=0).reset_index()
counties_racealone =  pd.merge(hdfcounties_racealone, mdfcounties_racealone, on=['GEOID','RACEALONE'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
for r in racealonecats:
    ss = counties_racealone[counties_racealone['RACEALONE'] == r].pipe(calculate_ss, geography="County", sizecategory = "All", characteristic = "{race}".format(race = racealonedict.get(r)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# County Race Alone By Size
for r in racealonecats:
    counties_race = counties_racealone[counties_racealone['RACEALONE'] == r]
    counties_race = counties_race.assign(Race_PopSize = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in counties_race['Race_PopSize'].cat.categories:
        temp = counties_race[counties_race['Race_PopSize'] == i]
        # temp.to_csv(f"{OUTPUTDIR}/counties_race_race{r}_{i}.csv",index=False)
        ss = counties_race[counties_race['Race_PopSize'] == i].pipe(calculate_ss, geography="County", sizecategory = str(i), characteristic = "{race}".format(race = racealonedict.get(r)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Place Race Alone
hdfplaces_racealone = dfhdf.groupby(['IncPlaceGEOID', 'RACEALONE']).size().reset_index(name='HDF_Population').set_index(['IncPlaceGEOID', 'RACEALONE']).reindex(place_racealone_index, fill_value=0).reset_index()
mdfplaces_racealone = dfmdf.groupby(['IncPlaceGEOID', 'RACEALONE']).size().reset_index(name='MDF_Population').set_index(['IncPlaceGEOID', 'RACEALONE']).reindex(place_racealone_index, fill_value=0).reset_index()
places_racealone =  pd.merge(hdfplaces_racealone, mdfplaces_racealone, on=['IncPlaceGEOID','RACEALONE'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
for r in racealonecats:
    ss = places_racealone[places_racealone['RACEALONE'] == r].pipe(calculate_ss, geography="Place", sizecategory = "All", characteristic = "{race}".format(race = racealonedict.get(r)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Place Race Alone By Size
for r in racealonecats:
    places_race = places_racealone[places_racealone['RACEALONE'] == r]
    places_race = places_race.assign(Race_PopSize = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in places_race['Race_PopSize'].cat.categories:
        temp = places_race[places_race['Race_PopSize'] == i]
        # temp.to_csv(f"{OUTPUTDIR}/places_race_race{r}_{i}.csv",index=False)
        ss = places_race[places_race['Race_PopSize'] == i].pipe(calculate_ss, geography="Place", sizecategory = str(i), characteristic = "{race}".format(race = racealonedict.get(r)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Tract Race Alone
hdftracts_racealone = dfhdf.groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE','RACEALONE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID', 'RACEALONE']).reindex(tract_racealone_index, fill_value=0).reset_index()
mdftracts_racealone = dfmdf.groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE','RACEALONE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID', 'RACEALONE']).reindex(tract_racealone_index, fill_value=0).reset_index()
tracts_racealone =  pd.merge(hdftracts_racealone, mdftracts_racealone, on=['GEOID','RACEALONE'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
for r in racealonecats:
    ss = tracts_racealone[tracts_racealone['RACEALONE'] == r].pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "{race}".format(race = racealonedict.get(r)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Tract Race Alone By Size
for r in racealonecats:
    tracts_race = tracts_racealone[tracts_racealone['RACEALONE'] == r]
    tracts_race = tracts_race.assign(Race_PopSize = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in tracts_race['Race_PopSize'].cat.categories:
        ss = tracts_race[tracts_race['Race_PopSize'] == i].pipe(calculate_ss, geography="Tract", sizecategory = str(i), characteristic = "{race}".format(race = racealonedict.get(r)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

if runPRhere:
    # PR Counties/Municipios Race Alone
    for r in racealonecats:
        hdfcountiespr_racealone = dfhdfpr[dfhdfpr['RACEALONE'] == r].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesprindex, fill_value=0).reset_index()
        mdfcountiespr_racealone = dfmdfpr[dfmdfpr['RACEALONE'] == r].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesprindex, fill_value=0).reset_index()
        countiespr_racealone =  pd.merge(hdfcountiespr_racealone, mdfcountiespr_racealone, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
        ss = countiespr_racealone.pipe(calculate_ss, geography="PR County/Municipio", sizecategory = "All", characteristic = "{race}".format(race = racealonedict.get(r)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
        countiespr_racealone = countiespr_racealone.assign(Race_PopSize = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
        for i in countiespr_racealone['Race_PopSize'].cat.categories:
            ss = countiespr_racealone[countiespr_racealone['Race_PopSize'] == i].pipe(calculate_ss, geography="PR County/Municipio", sizecategory = str(i), characteristic = "{race}".format(race = racealonedict.get(r)))
            outputdf = outputdf.append(ss, ignore_index=True, sort=False)

    # PR Tracts Race Alone
    for r in racealonecats:
        hdftractspr_racealone = dfhdfpr[dfhdfpr['RACEALONE'] == r].groupby(['TABBLKST', 'TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsprindex, fill_value=0).reset_index()
        mdftractspr_racealone = dfmdfpr[dfmdfpr['RACEALONE'] == r].groupby(['TABBLKST', 'TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsprindex, fill_value=0).reset_index()
        tractspr_racealone =  pd.merge(hdftractspr_racealone, mdftractspr_racealone, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
        ss = tractspr_racealone.pipe(calculate_ss, geography="PR Tract", sizecategory = "All", characteristic = "{race}".format(race = racealonedict.get(r)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
        tractspr_racealone = tractspr_racealone.assign(Race_PopSize = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
        for i in tractspr_racealone['Race_PopSize'].cat.categories:
            ss = tractspr_racealone[tractspr_racealone['Race_PopSize'] == i].pipe(calculate_ss, geography="PR Tract", sizecategory = str(i), characteristic = "{race}".format(race = racealonedict.get(r)))
            outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Federal AIR Race Alone
for r in racealonecats:
    hdffedairs_racealone = dfhdf[dfhdf['RACEALONE'] == r].groupby(['FedAIRGEOID']).size().reset_index(name='HDF_Population').set_index('FedAIRGEOID').reindex(allfedairsindex, fill_value=0).reset_index()
    mdffedairs_racealone = dfmdf[dfmdf['RACEALONE'] == r].groupby(['FedAIRGEOID']).size().reset_index(name='MDF_Population').set_index('FedAIRGEOID').reindex(allfedairsindex, fill_value=0).reset_index()
    fedairs_racealone =  pd.merge(hdffedairs_racealone, mdffedairs_racealone, on=['FedAIRGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = fedairs_racealone.pipe(calculate_ss, geography="Fed AIR", sizecategory = "All", characteristic = "{race}".format(race = racealonedict.get(r)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    fedairs_racealone = fedairs_racealone.assign(Race_PopSize = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in fedairs_racealone['Race_PopSize'].cat.categories:
        ss = fedairs_racealone[fedairs_racealone['Race_PopSize'] == i].pipe(calculate_ss, geography="Fed AIR", sizecategory = str(i), characteristic = "{race}".format(race = racealonedict.get(r)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# OTSA Race Alone
for r in racealonecats:
    hdfotsas_racealone = dfhdf[dfhdf['RACEALONE'] == r].groupby(['OTSAGEOID']).size().reset_index(name='HDF_Population').set_index('OTSAGEOID').reindex(allotsasindex, fill_value=0).reset_index()
    mdfotsas_racealone = dfmdf[dfmdf['RACEALONE'] == r].groupby(['OTSAGEOID']).size().reset_index(name='MDF_Population').set_index('OTSAGEOID').reindex(allotsasindex, fill_value=0).reset_index()
    otsas_racealone =  pd.merge(hdfotsas_racealone, mdfotsas_racealone, on=['OTSAGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = otsas_racealone.pipe(calculate_ss, geography="OTSA", sizecategory = "All", characteristic = "{race}".format(race = racealonedict.get(r)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    otsas_racealone = otsas_racealone.assign(Race_PopSize = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in otsas_racealone['Race_PopSize'].cat.categories:
        ss = otsas_racealone[otsas_racealone['Race_PopSize'] == i].pipe(calculate_ss, geography="OTSA", sizecategory = str(i), characteristic = "{race}".format(race = racealonedict.get(r)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# ANVSA Race Alone
for r in racealonecats:
    hdfanvsas_racealone = dfhdf[dfhdf['RACEALONE'] == r].groupby(['ANVSAGEOID']).size().reset_index(name='HDF_Population').set_index('ANVSAGEOID').reindex(allanvsasindex, fill_value=0).reset_index()
    mdfanvsas_racealone = dfmdf[dfmdf['RACEALONE'] == r].groupby(['ANVSAGEOID']).size().reset_index(name='MDF_Population').set_index('ANVSAGEOID').reindex(allanvsasindex, fill_value=0).reset_index()
    anvsas_racealone =  pd.merge(hdfanvsas_racealone, mdfanvsas_racealone, on=['ANVSAGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = anvsas_racealone.pipe(calculate_ss, geography="ANVSA", sizecategory = "All", characteristic = "{race}".format(race = racealonedict.get(r)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    anvsas_racealone = anvsas_racealone.assign(Race_PopSize = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in anvsas_racealone['Race_PopSize'].cat.categories:
        ss = anvsas_racealone[anvsas_racealone['Race_PopSize'] == i].pipe(calculate_ss, geography="ANVSA", sizecategory = str(i), characteristic = "{race}".format(race = racealonedict.get(r)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

print("{} Race Alone Done".format(datetime.now()))

# State Hispanic By Race Alone
for r in racealonecats:
    hdfstates_hispracealone = dfhdf[(dfhdf['CENHISP'] == '2')&(dfhdf['RACEALONE'] == r)].groupby(['TABBLKST']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index(['GEOID']).reindex(allstatesindex, fill_value=0).reset_index()
    mdfstates_hispracealone = dfmdf[(dfmdf['CENHISP'] == '2')&(dfmdf['RACEALONE'] == r)].groupby(['TABBLKST']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index(['GEOID']).reindex(allstatesindex, fill_value=0).reset_index()
    states_hispracealone =  pd.merge(hdfstates_hispracealone, mdfstates_hispracealone, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = states_hispracealone.pipe(calculate_ss, geography="State", sizecategory = "All", characteristic = "Hispanic {race}".format(race = racealonedict.get(r)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    hdfstates_nonhispracealone = dfhdf[(dfhdf['CENHISP'] == '1')&(dfhdf['RACEALONE'] == r)].groupby(['TABBLKST']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index(['GEOID']).reindex(allstatesindex, fill_value=0).reset_index()
    mdfstates_nonhispracealone = dfmdf[(dfmdf['CENHISP'] == '1')&(dfmdf['RACEALONE'] == r)].groupby(['TABBLKST']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index(['GEOID']).reindex(allstatesindex, fill_value=0).reset_index()
    states_nonhispracealone =  pd.merge(hdfstates_nonhispracealone, mdfstates_nonhispracealone, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = states_nonhispracealone.pipe(calculate_ss, geography="State", sizecategory = "All", characteristic = "Non-Hispanic {race}".format(race = racealonedict.get(r)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)


# County Hispanic By Race Alone
for r in racealonecats:
    hdfcounties_hispracealone = dfhdf[(dfhdf['CENHISP'] == '2')&(dfhdf['RACEALONE'] == r)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    mdfcounties_hispracealone = dfmdf[(dfmdf['CENHISP'] == '2')&(dfmdf['RACEALONE'] == r)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    counties_hispracealone =  pd.merge(hdfcounties_hispracealone, mdfcounties_hispracealone, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = counties_hispracealone.pipe(calculate_ss, geography="County", sizecategory = "All", characteristic = "Hispanic {race}".format(race = racealonedict.get(r)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    hdfcounties_nonhispracealone = dfhdf[(dfhdf['CENHISP'] == '1')&(dfhdf['RACEALONE'] == r)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    mdfcounties_nonhispracealone = dfmdf[(dfmdf['CENHISP'] == '1')&(dfmdf['RACEALONE'] == r)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    counties_nonhispracealone =  pd.merge(hdfcounties_nonhispracealone, mdfcounties_nonhispracealone, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = counties_nonhispracealone.pipe(calculate_ss, geography="County", sizecategory = "All", characteristic = "Non-Hispanic {race}".format(race = racealonedict.get(r)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    counties_hispracealone = counties_hispracealone.assign(HispRace_PopSize = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in counties_hispracealone['HispRace_PopSize'].cat.categories:
        ss = counties_hispracealone[counties_hispracealone['HispRace_PopSize'] == i].pipe(calculate_ss, geography="County", sizecategory = str(i), characteristic = "Hispanic {race}".format(race = racealonedict.get(r)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    counties_nonhispracealone = counties_nonhispracealone.assign(HispRace_PopSize = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in counties_nonhispracealone['HispRace_PopSize'].cat.categories:
        ss = counties_nonhispracealone[counties_nonhispracealone['HispRace_PopSize'] == i].pipe(calculate_ss, geography="County", sizecategory = str(i), characteristic = "Non-Hispanic {race}".format(race = racealonedict.get(r)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Place Hispanic By Race Alone
for r in racealonecats:
    hdfplaces_hispracealone = dfhdf[(dfhdf['CENHISP'] == '2')&(dfhdf['RACEALONE'] == r)].groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    mdfplaces_hispracealone = dfmdf[(dfmdf['CENHISP'] == '2')&(dfmdf['RACEALONE'] == r)].groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    places_hispracealone =  pd.merge(hdfplaces_hispracealone, mdfplaces_hispracealone, on=['IncPlaceGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = places_hispracealone.pipe(calculate_ss, geography="Place", sizecategory = "All", characteristic = "Hispanic {race}".format(race = racealonedict.get(r)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    hdfplaces_nonhispracealone = dfhdf[(dfhdf['CENHISP'] == '1')&(dfhdf['RACEALONE'] == r)].groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    mdfplaces_nonhispracealone = dfmdf[(dfmdf['CENHISP'] == '1')&(dfmdf['RACEALONE'] == r)].groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    places_nonhispracealone =  pd.merge(hdfplaces_nonhispracealone, mdfplaces_nonhispracealone, on=['IncPlaceGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = places_nonhispracealone.pipe(calculate_ss, geography="Place", sizecategory = "All", characteristic = "Non-Hispanic {race}".format(race = racealonedict.get(r)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    places_hispracealone = places_hispracealone.assign(HispRace_PopSize = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in places_hispracealone['HispRace_PopSize'].cat.categories:
        ss = places_hispracealone[places_hispracealone['HispRace_PopSize'] == i].pipe(calculate_ss, geography="Place", sizecategory = str(i), characteristic = "Hispanic {race}".format(race = racealonedict.get(r)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    places_nonhispracealone = places_nonhispracealone.assign(HispRace_PopSize = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in places_nonhispracealone['HispRace_PopSize'].cat.categories:
        ss = places_nonhispracealone[places_nonhispracealone['HispRace_PopSize'] == i].pipe(calculate_ss, geography="Place", sizecategory = str(i), characteristic = "Non-Hispanic {race}".format(race = racealonedict.get(r)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Tract Hispanic By Race Alone
for r in racealonecats:
    hdftracts_hispracealone = dfhdf[(dfhdf['CENHISP'] == '2')&(dfhdf['RACEALONE'] == r)].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    mdftracts_hispracealone = dfmdf[(dfmdf['CENHISP'] == '2')&(dfmdf['RACEALONE'] == r)].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    tracts_hispracealone =  pd.merge(hdftracts_hispracealone, mdftracts_hispracealone, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = tracts_hispracealone.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "Hispanic {race}".format(race = racealonedict.get(r)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    hdftracts_nonhispracealone = dfhdf[(dfhdf['CENHISP'] == '1')&(dfhdf['RACEALONE'] == r)].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    mdftracts_nonhispracealone = dfmdf[(dfmdf['CENHISP'] == '1')&(dfmdf['RACEALONE'] == r)].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    tracts_nonhispracealone =  pd.merge(hdftracts_nonhispracealone, mdftracts_nonhispracealone, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = tracts_nonhispracealone.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "Non-Hispanic {race}".format(race = racealonedict.get(r)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    tracts_hispracealone = tracts_hispracealone.assign(HispRace_PopSize = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in tracts_hispracealone['HispRace_PopSize'].cat.categories:
        ss = tracts_hispracealone[tracts_hispracealone['HispRace_PopSize'] == i].pipe(calculate_ss, geography="Tract", sizecategory = str(i), characteristic = "Hispanic {race}".format(race = racealonedict.get(r)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    tracts_nonhispracealone = tracts_nonhispracealone.assign(HispRace_PopSize = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in tracts_nonhispracealone['HispRace_PopSize'].cat.categories:
        ss = tracts_nonhispracealone[tracts_nonhispracealone['HispRace_PopSize'] == i].pipe(calculate_ss, geography="Tract", sizecategory = str(i), characteristic = "Non-Hispanic {race}".format(race = racealonedict.get(r)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Tract Hispanic By Race Alone Aged 18+
for r in racealonecats:
    hdftracts_hispracealone18p = dfhdf[(dfhdf['CENHISP'] == '2')&(dfhdf['QAGE'] >= 18)&(dfhdf['RACEALONE'] == r)].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    mdftracts_hispracealone18p = dfmdf[(dfmdf['CENHISP'] == '2')&(dfmdf['QAGE'] >= 18)&(dfmdf['RACEALONE'] == r)].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    tracts_hispracealone18p =  pd.merge(hdftracts_hispracealone18p, mdftracts_hispracealone18p, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = tracts_hispracealone18p.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "Hispanic {race} Aged 18+".format(race = racealonedict.get(r)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    hdftracts_nonhispracealone18p = dfhdf[(dfhdf['CENHISP'] == '1')&(dfhdf['QAGE'] >= 18)&(dfhdf['RACEALONE'] == r)].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    mdftracts_nonhispracealone18p = dfmdf[(dfmdf['CENHISP'] == '1')&(dfmdf['QAGE'] >= 18)&(dfmdf['RACEALONE'] == r)].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    tracts_nonhispracealone18p =  pd.merge(hdftracts_nonhispracealone18p, mdftracts_nonhispracealone18p, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = tracts_nonhispracealone18p.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "Non-Hispanic {race} Aged 18+".format(race = racealonedict.get(r)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    tracts_hispracealone18p = tracts_hispracealone18p.assign(HispRace_PopSize = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in tracts_hispracealone18p['HispRace_PopSize'].cat.categories:
        ss = tracts_hispracealone18p[tracts_hispracealone18p['HispRace_PopSize'] == i].pipe(calculate_ss, geography="Tract", sizecategory = str(i), characteristic = "Hispanic {race} Aged 18+".format(race = racealonedict.get(r)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    tracts_nonhispracealone18p = tracts_nonhispracealone18p.assign(HispRace_PopSize = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in tracts_nonhispracealone18p['HispRace_PopSize'].cat.categories:
        ss = tracts_nonhispracealone18p[tracts_nonhispracealone18p['HispRace_PopSize'] == i].pipe(calculate_ss, geography="Tract", sizecategory = str(i), characteristic = "Non-Hispanic {race} Aged 18+".format(race = racealonedict.get(r)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Block Group Hispanic By Race Alone Aged 18+
for r in racealonecats:
    hdfblockgroups_hispracealone18p = dfhdf[(dfhdf['CENHISP'] == '2')&(dfhdf['QAGE'] >= 18)&(dfhdf['RACEALONE'] == r)].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE + x.TABBLKGRPCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE']).set_index(['GEOID']).reindex(allblockgroupsindex, fill_value=0).reset_index()
    mdfblockgroups_hispracealone18p = dfmdf[(dfmdf['CENHISP'] == '2')&(dfmdf['QAGE'] >= 18)&(dfmdf['RACEALONE'] == r)].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE + x.TABBLKGRPCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE']).set_index(['GEOID']).reindex(allblockgroupsindex, fill_value=0).reset_index()
    blockgroups_hispracealone18p =  pd.merge(hdfblockgroups_hispracealone18p, mdfblockgroups_hispracealone18p, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = blockgroups_hispracealone18p.pipe(calculate_ss, geography="Block Group", sizecategory = "All", characteristic = "Hispanic {race} Aged 18+".format(race = racealonedict.get(r)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    hdfblockgroups_nonhispracealone18p = dfhdf[(dfhdf['CENHISP'] == '1')&(dfhdf['QAGE'] >= 18)&(dfhdf['RACEALONE'] == r)].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE + x.TABBLKGRPCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE']).set_index(['GEOID']).reindex(allblockgroupsindex, fill_value=0).reset_index()
    mdfblockgroups_nonhispracealone18p = dfmdf[(dfmdf['CENHISP'] == '1')&(dfmdf['QAGE'] >= 18)&(dfmdf['RACEALONE'] == r)].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE + x.TABBLKGRPCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE']).set_index(['GEOID']).reindex(allblockgroupsindex, fill_value=0).reset_index()
    blockgroups_nonhispracealone18p =  pd.merge(hdfblockgroups_nonhispracealone18p, mdfblockgroups_nonhispracealone18p, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = blockgroups_nonhispracealone18p.pipe(calculate_ss, geography="Block Group", sizecategory = "All", characteristic = "Non-Hispanic {race} Aged 18+".format(race = racealonedict.get(r)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    blockgroups_hispracealone18p = blockgroups_hispracealone18p.assign(HispRace_PopSize = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in blockgroups_hispracealone18p['HispRace_PopSize'].cat.categories:
        ss = blockgroups_hispracealone18p[blockgroups_hispracealone18p['HispRace_PopSize'] == i].pipe(calculate_ss, geography="Block Group", sizecategory = str(i), characteristic = "Hispanic {race} Aged 18+".format(race = racealonedict.get(r)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    blockgroups_nonhispracealone18p = blockgroups_nonhispracealone18p.assign(HispRace_PopSize = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in blockgroups_nonhispracealone18p['HispRace_PopSize'].cat.categories:
        ss = blockgroups_nonhispracealone18p[blockgroups_nonhispracealone18p['HispRace_PopSize'] == i].pipe(calculate_ss, geography="Block Group", sizecategory = str(i), characteristic = "Non-Hispanic {race} Aged 18+".format(race = racealonedict.get(r)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

print("{} Hispanic By Race Alone Done".format(datetime.now()))

# Race Alone Or In Combination
print("{} Starting Hispanic By Race Alone Or In Combination".format(datetime.now()))

dfhdf = dfhdf.pipe(assign_racealone_or_incomb)
dfmdf = dfmdf.pipe(assign_racealone_or_incomb)

if runPRhere:
    dfhdfpr = dfhdfpr.pipe(assign_racealone_or_incomb)
    dfmdfpr = dfmdfpr.pipe(assign_racealone_or_incomb)

racegroups = ['whitealone-or-incomb', 'blackalone-or-incomb', 'aianalone-or-incomb', 'asianalone-or-incomb', 'nhopialone-or-incomb', 'soralone-or-incomb']

# State Race Alone Or In Combination/ Hispanic Race Alone Or In Combination/ Non-Hispanic Race Alone Or In Combination
for rg in racegroups:
    hdfstates_raceincomb = dfhdf[(dfhdf[rg]==1)].groupby(['TABBLKST']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index(['GEOID']).reindex(allstatesindex, fill_value=0).reset_index()
    mdfstates_raceincomb = dfmdf[(dfmdf[rg]==1)].groupby(['TABBLKST']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index(['GEOID']).reindex(allstatesindex, fill_value=0).reset_index()
    states_raceincomb =  pd.merge(hdfstates_raceincomb, mdfstates_raceincomb, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = states_raceincomb.pipe(calculate_ss, geography="State", sizecategory = "All", characteristic = "{race}".format(race = raceincombdict.get(rg)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for rg in racegroups:
    hdfstates_hispraceincomb = dfhdf[(dfhdf['CENHISP'] == '2') & (dfhdf[rg]==1)].groupby(['TABBLKST']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index(['GEOID']).reindex(allstatesindex, fill_value=0).reset_index()
    mdfstates_hispraceincomb = dfmdf[(dfmdf['CENHISP'] == '2') & (dfmdf[rg]==1)].groupby(['TABBLKST']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index(['GEOID']).reindex(allstatesindex, fill_value=0).reset_index()
    states_hispraceincomb =  pd.merge(hdfstates_hispraceincomb, mdfstates_hispraceincomb, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = states_hispraceincomb.pipe(calculate_ss, geography="State", sizecategory = "All", characteristic = "Hispanic {race}".format(race = raceincombdict.get(rg)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for rg in racegroups:
    hdfstates_nonhispraceincomb = dfhdf[(dfhdf['CENHISP'] == '1') & (dfhdf[rg]==1)].groupby(['TABBLKST']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index(['GEOID']).reindex(allstatesindex, fill_value=0).reset_index()
    mdfstates_nonhispraceincomb = dfmdf[(dfmdf['CENHISP'] == '1') & (dfmdf[rg]==1)].groupby(['TABBLKST']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index(['GEOID']).reindex(allstatesindex, fill_value=0).reset_index()
    states_nonhispraceincomb =  pd.merge(hdfstates_nonhispraceincomb, mdfstates_nonhispraceincomb, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = states_nonhispraceincomb.pipe(calculate_ss, geography="State", sizecategory = "All", characteristic = "Non-Hispanic {race}".format(race = raceincombdict.get(rg)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# County Race Alone Or In Combination/ Hispanic Race Alone Or In Combination/ Non-Hispanic Race Alone Or In Combination
for rg in racegroups:
    hdfcounties_raceincomb = dfhdf[(dfhdf[rg]==1)].groupby(['TABBLKST','TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    mdfcounties_raceincomb = dfmdf[(dfmdf[rg]==1)].groupby(['TABBLKST','TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    counties_raceincomb =  pd.merge(hdfcounties_raceincomb, mdfcounties_raceincomb, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = counties_raceincomb.pipe(calculate_ss, geography="County", sizecategory = "All", characteristic = "{race}".format(race = raceincombdict.get(rg)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    counties_raceincomb = counties_raceincomb.assign(RaceInComb_SizeA = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in counties_raceincomb['RaceInComb_SizeA'].cat.categories:
        ss = counties_raceincomb[counties_raceincomb['RaceInComb_SizeA'] == i].pipe(calculate_ss, geography="County", sizecategory = str(i), characteristic = "{race}".format(race = raceincombdict.get(rg)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for rg in racegroups:
    hdfcounties_hispraceincomb = dfhdf[(dfhdf['CENHISP'] == '2') & (dfhdf[rg]==1)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    mdfcounties_hispraceincomb = dfmdf[(dfmdf['CENHISP'] == '2') & (dfmdf[rg]==1)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    counties_hispraceincomb =  pd.merge(hdfcounties_hispraceincomb, mdfcounties_hispraceincomb, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = counties_hispraceincomb.pipe(calculate_ss, geography="County", sizecategory = "All", characteristic = "Hispanic {race}".format(race = raceincombdict.get(rg)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    counties_hispraceincomb = counties_hispraceincomb.assign(RaceInComb_SizeA = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in counties_hispraceincomb['RaceInComb_SizeA'].cat.categories:
        ss = counties_hispraceincomb[counties_hispraceincomb['RaceInComb_SizeA'] == i].pipe(calculate_ss, geography="County", sizecategory = str(i), characteristic = "Hispanic {race}".format(race = raceincombdict.get(rg)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for rg in racegroups:
    hdfcounties_nonhispraceincomb = dfhdf[(dfhdf['CENHISP'] == '1') & (dfhdf[rg]==1)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    mdfcounties_nonhispraceincomb = dfmdf[(dfmdf['CENHISP'] == '1') & (dfmdf[rg]==1)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    counties_nonhispraceincomb =  pd.merge(hdfcounties_nonhispraceincomb, mdfcounties_nonhispraceincomb, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = counties_nonhispraceincomb.pipe(calculate_ss, geography="County", sizecategory = "All", characteristic = "Non-Hispanic {race}".format(race = raceincombdict.get(rg)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    counties_nonhispraceincomb = counties_nonhispraceincomb.assign(RaceInComb_SizeA = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in counties_nonhispraceincomb['RaceInComb_SizeA'].cat.categories:
        ss = counties_nonhispraceincomb[counties_nonhispraceincomb['RaceInComb_SizeA'] == i].pipe(calculate_ss, geography="County", sizecategory = str(i), characteristic = "Non-Hispanic {race}".format(race = raceincombdict.get(rg)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Place Race Alone Or In Combination/ Hispanic Race Alone Or In Combination/ Non-Hispanic Race Alone Or In Combination
for rg in racegroups:
    hdfplaces_raceincomb = dfhdf[(dfhdf[rg]==1)].groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    mdfplaces_raceincomb = dfmdf[(dfmdf[rg]==1)].groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    places_raceincomb =  pd.merge(hdfplaces_raceincomb, mdfplaces_raceincomb, on=['IncPlaceGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = places_raceincomb.pipe(calculate_ss, geography="Place", sizecategory = "All", characteristic = "{race}".format(race = raceincombdict.get(rg)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    places_raceincomb = places_raceincomb.assign(RaceInComb_SizeA = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in places_raceincomb['RaceInComb_SizeA'].cat.categories:
        ss = places_raceincomb[places_raceincomb['RaceInComb_SizeA'] == i].pipe(calculate_ss, geography="Place", sizecategory = str(i), characteristic = "{race}".format(race = raceincombdict.get(rg)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for rg in racegroups:
    hdfplaces_hispraceincomb = dfhdf[(dfhdf['CENHISP'] == '2') & (dfhdf[rg]==1)].groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    mdfplaces_hispraceincomb = dfmdf[(dfmdf['CENHISP'] == '2') & (dfmdf[rg]==1)].groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    places_hispraceincomb =  pd.merge(hdfplaces_hispraceincomb, mdfplaces_hispraceincomb, on=['IncPlaceGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = places_hispraceincomb.pipe(calculate_ss, geography="Place", sizecategory = "All", characteristic = "Hispanic {race}".format(race = raceincombdict.get(rg)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    places_hispraceincomb = places_hispraceincomb.assign(RaceInComb_SizeA = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in places_hispraceincomb['RaceInComb_SizeA'].cat.categories:
        ss = places_hispraceincomb[places_hispraceincomb['RaceInComb_SizeA'] == i].pipe(calculate_ss, geography="Place", sizecategory = str(i), characteristic = "Hispanic {race}".format(race = raceincombdict.get(rg)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for rg in racegroups:
    hdfplaces_nonhispraceincomb = dfhdf[(dfhdf['CENHISP'] == '1') & (dfhdf[rg]==1)].groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    mdfplaces_nonhispraceincomb = dfmdf[(dfmdf['CENHISP'] == '1') & (dfmdf[rg]==1)].groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    places_nonhispraceincomb =  pd.merge(hdfplaces_nonhispraceincomb, mdfplaces_nonhispraceincomb, on=['IncPlaceGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = places_nonhispraceincomb.pipe(calculate_ss, geography="Place", sizecategory = "All", characteristic = "Non-Hispanic {race}".format(race = raceincombdict.get(rg)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    places_nonhispraceincomb = places_nonhispraceincomb.assign(RaceInComb_SizeA = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in places_nonhispraceincomb['RaceInComb_SizeA'].cat.categories:
        ss = places_nonhispraceincomb[places_nonhispraceincomb['RaceInComb_SizeA'] == i].pipe(calculate_ss, geography="Place", sizecategory = str(i), characteristic = "Non-Hispanic {race}".format(race = raceincombdict.get(rg)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Tract Race Alone Or In Combination/ Hispanic Race Alone Or In Combination/ Non-Hispanic Race Alone Or In Combination
for rg in racegroups:
    hdftracts_raceincomb = dfhdf[(dfhdf[rg]==1)].groupby(['TABBLKST','TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    mdftracts_raceincomb = dfmdf[(dfmdf[rg]==1)].groupby(['TABBLKST','TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    tracts_raceincomb =  pd.merge(hdftracts_raceincomb, mdftracts_raceincomb, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = tracts_raceincomb.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "{race}".format(race = raceincombdict.get(rg)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    tracts_raceincomb = tracts_raceincomb.assign(RaceInComb_SizeA = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in tracts_raceincomb['RaceInComb_SizeA'].cat.categories:
        ss = tracts_raceincomb[tracts_raceincomb['RaceInComb_SizeA'] == i].pipe(calculate_ss, geography="Tract", sizecategory = str(i), characteristic = "{race}".format(race = raceincombdict.get(rg)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for rg in racegroups:
    hdftracts_hispraceincomb = dfhdf[(dfhdf['CENHISP'] == '2') & (dfhdf[rg]==1)].groupby(['TABBLKST', 'TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    mdftracts_hispraceincomb = dfmdf[(dfmdf['CENHISP'] == '2') & (dfmdf[rg]==1)].groupby(['TABBLKST', 'TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    tracts_hispraceincomb =  pd.merge(hdftracts_hispraceincomb, mdftracts_hispraceincomb, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = tracts_hispraceincomb.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "Hispanic {race}".format(race = raceincombdict.get(rg)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    tracts_hispraceincomb = tracts_hispraceincomb.assign(RaceInComb_SizeA = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in tracts_hispraceincomb['RaceInComb_SizeA'].cat.categories:
        ss = tracts_hispraceincomb[tracts_hispraceincomb['RaceInComb_SizeA'] == i].pipe(calculate_ss, geography="Tract", sizecategory = str(i), characteristic = "Hispanic {race}".format(race = raceincombdict.get(rg)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for rg in racegroups:
    hdftracts_nonhispraceincomb = dfhdf[(dfhdf['CENHISP'] == '1') & (dfhdf[rg]==1)].groupby(['TABBLKST', 'TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    mdftracts_nonhispraceincomb = dfmdf[(dfmdf['CENHISP'] == '1') & (dfmdf[rg]==1)].groupby(['TABBLKST', 'TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    tracts_nonhispraceincomb =  pd.merge(hdftracts_nonhispraceincomb, mdftracts_nonhispraceincomb, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = tracts_nonhispraceincomb.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "Non-Hispanic {race}".format(race = raceincombdict.get(rg)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    tracts_nonhispraceincomb = tracts_nonhispraceincomb.assign(RaceInComb_SizeA = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in tracts_nonhispraceincomb['RaceInComb_SizeA'].cat.categories:
        ss = tracts_nonhispraceincomb[tracts_nonhispraceincomb['RaceInComb_SizeA'] == i].pipe(calculate_ss, geography="Tract", sizecategory = str(i), characteristic = "Non-Hispanic {race}".format(race = raceincombdict.get(rg)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

if runPRhere:
    # PR Counties/Municipios Race Alone Or In Combination
    for rg in racegroups:
        hdfcountiespr_raceincomb = dfhdfpr[dfhdfpr[rg]==1].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesprindex, fill_value=0).reset_index()
        mdfcountiespr_raceincomb = dfmdfpr[dfmdfpr[rg]==1].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesprindex, fill_value=0).reset_index()
        countiespr_raceincomb =  pd.merge(hdfcountiespr_raceincomb, mdfcountiespr_raceincomb, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
        ss = countiespr_raceincomb.pipe(calculate_ss, geography="PR County/Municipio", sizecategory = "All", characteristic = "{race}".format(race = raceincombdict.get(rg)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
        countiespr_raceincomb = countiespr_raceincomb.assign(RaceInComb_SizeA = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
        for i in countiespr_raceincomb['RaceInComb_SizeA'].cat.categories:
            ss = countiespr_raceincomb[countiespr_raceincomb['RaceInComb_SizeA'] == i].pipe(calculate_ss, geography="PR County/Municipio", sizecategory = str(i), characteristic = "{race}".format(race = raceincombdict.get(rg)))
            outputdf = outputdf.append(ss, ignore_index=True, sort=False)

    # PR Tracts Race Alone Or In Combination
    for rg in racegroups:
        hdftractspr_racealone = dfhdfpr[dfhdfpr[rg]==1].groupby(['TABBLKST', 'TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsprindex, fill_value=0).reset_index()
        mdftractspr_racealone = dfmdfpr[dfmdfpr[rg]==1].groupby(['TABBLKST', 'TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsprindex, fill_value=0).reset_index()
        tractspr_racealone =  pd.merge(hdftractspr_racealone, mdftractspr_racealone, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
        ss = tractspr_racealone.pipe(calculate_ss, geography="PR Tract", sizecategory = "All", characteristic = "{race}".format(race = raceincombdict.get(rg)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
        tractspr_racealone = tractspr_racealone.assign(RaceInComb_SizeA = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
        for i in tractspr_racealone['RaceInComb_SizeA'].cat.categories:
            ss = tractspr_racealone[tractspr_racealone['RaceInComb_SizeA'] == i].pipe(calculate_ss, geography="PR Tract", sizecategory = str(i), characteristic = "{race}".format(race = raceincombdict.get(rg)))
            outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Federal AIR Race Alone Or In Combination
for rg in racegroups:
    hdffedairs_racealone = dfhdf[dfhdf[rg]==1].groupby(['FedAIRGEOID']).size().reset_index(name='HDF_Population').set_index('FedAIRGEOID').reindex(allfedairsindex, fill_value=0).reset_index()
    mdffedairs_racealone = dfmdf[dfmdf[rg]==1].groupby(['FedAIRGEOID']).size().reset_index(name='MDF_Population').set_index('FedAIRGEOID').reindex(allfedairsindex, fill_value=0).reset_index()
    fedairs_racealone =  pd.merge(hdffedairs_racealone, mdffedairs_racealone, on=['FedAIRGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = fedairs_racealone.pipe(calculate_ss, geography="Fed AIR", sizecategory = "All", characteristic = "{race}".format(race = raceincombdict.get(rg)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    fedairs_racealone = fedairs_racealone.assign(RaceInComb_SizeA = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in fedairs_racealone['RaceInComb_SizeA'].cat.categories:
        ss = fedairs_racealone[fedairs_racealone['RaceInComb_SizeA'] == i].pipe(calculate_ss, geography="Fed AIR", sizecategory = str(i), characteristic = "{race}".format(race = raceincombdict.get(rg)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# OTSA Race Alone Or In Combination
for rg in racegroups:
    hdfotsas_racealone = dfhdf[dfhdf[rg]==1].groupby(['OTSAGEOID']).size().reset_index(name='HDF_Population').set_index('OTSAGEOID').reindex(allotsasindex, fill_value=0).reset_index()
    mdfotsas_racealone = dfmdf[dfmdf[rg]==1].groupby(['OTSAGEOID']).size().reset_index(name='MDF_Population').set_index('OTSAGEOID').reindex(allotsasindex, fill_value=0).reset_index()
    otsas_racealone =  pd.merge(hdfotsas_racealone, mdfotsas_racealone, on=['OTSAGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = otsas_racealone.pipe(calculate_ss, geography="OTSA", sizecategory = "All", characteristic = "{race}".format(race = raceincombdict.get(rg)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    otsas_racealone = otsas_racealone.assign(RaceInComb_SizeA = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in otsas_racealone['RaceInComb_SizeA'].cat.categories:
        ss = otsas_racealone[otsas_racealone['RaceInComb_SizeA'] == i].pipe(calculate_ss, geography="OTSA", sizecategory = str(i), characteristic = "{race}".format(race = raceincombdict.get(rg)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# ANVSA Race Alone Or In Combination
for rg in racegroups:
    hdfanvsas_racealone = dfhdf[dfhdf[rg]==1].groupby(['ANVSAGEOID']).size().reset_index(name='HDF_Population').set_index('ANVSAGEOID').reindex(allanvsasindex, fill_value=0).reset_index()
    mdfanvsas_racealone = dfmdf[dfmdf[rg]==1].groupby(['ANVSAGEOID']).size().reset_index(name='MDF_Population').set_index('ANVSAGEOID').reindex(allanvsasindex, fill_value=0).reset_index()
    anvsas_racealone =  pd.merge(hdfanvsas_racealone, mdfanvsas_racealone, on=['ANVSAGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = anvsas_racealone.pipe(calculate_ss, geography="ANVSA", sizecategory = "All", characteristic = "{race}".format(race = raceincombdict.get(rg)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    anvsas_racealone = anvsas_racealone.assign(RaceInComb_SizeA = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in anvsas_racealone['RaceInComb_SizeA'].cat.categories:
        ss = anvsas_racealone[anvsas_racealone['RaceInComb_SizeA'] == i].pipe(calculate_ss, geography="ANVSA", sizecategory = str(i), characteristic = "{race}".format(race = raceincombdict.get(rg)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Tract Race Alone Or In Combination Aged 18+
for rg in racegroups:
    hdftracts_hispraceincomb18p = dfhdf[(dfhdf['QAGE'] >= 18) & (dfhdf['CENHISP'] == '2') & (dfhdf[rg]==1)].groupby(['TABBLKST', 'TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    mdftracts_hispraceincomb18p = dfmdf[(dfmdf['QAGE'] >= 18) & (dfmdf['CENHISP'] == '2') & (dfmdf[rg]==1)].groupby(['TABBLKST', 'TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    tracts_hispraceincomb18p =  pd.merge(hdftracts_hispraceincomb18p, mdftracts_hispraceincomb18p, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = tracts_hispraceincomb18p.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "Hispanic {race} Aged 18+".format(race = raceincombdict.get(rg)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for rg in racegroups:
    hdftracts_nonhispraceincomb18p = dfhdf[(dfhdf['QAGE'] >= 18) & (dfhdf['CENHISP'] == '1') & (dfhdf[rg]==1)].groupby(['TABBLKST', 'TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    mdftracts_nonhispraceincomb18p = dfmdf[(dfmdf['QAGE'] >= 18) & (dfmdf['CENHISP'] == '1') & (dfmdf[rg]==1)].groupby(['TABBLKST', 'TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    tracts_nonhispraceincomb18p =  pd.merge(hdftracts_nonhispraceincomb18p, mdftracts_nonhispraceincomb18p, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = tracts_nonhispraceincomb18p.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "Non-Hispanic {race} Aged 18+".format(race = raceincombdict.get(rg)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Block Group Race Alone Or In Combination Aged 18+
for rg in racegroups:
    hdfblockgroups_hispraceincomb18p = dfhdf[(dfhdf['QAGE'] >= 18) & (dfhdf['CENHISP'] == '2') & (dfhdf[rg]==1)].groupby(['TABBLKST', 'TABBLKCOU','TABTRACTCE', 'TABBLKGRPCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE + x.TABBLKGRPCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE']).set_index(['GEOID']).reindex(allblockgroupsindex, fill_value=0).reset_index()
    mdfblockgroups_hispraceincomb18p = dfmdf[(dfmdf['QAGE'] >= 18) & (dfmdf['CENHISP'] == '2') & (dfmdf[rg]==1)].groupby(['TABBLKST', 'TABBLKCOU','TABTRACTCE', 'TABBLKGRPCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE + x.TABBLKGRPCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE']).set_index(['GEOID']).reindex(allblockgroupsindex, fill_value=0).reset_index()
    blockgroups_hispraceincomb18p =  pd.merge(hdfblockgroups_hispraceincomb18p, mdfblockgroups_hispraceincomb18p, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = blockgroups_hispraceincomb18p.pipe(calculate_ss, geography="Block Group", sizecategory = "All", characteristic = "Hispanic {race} Aged 18+".format(race = raceincombdict.get(rg)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for rg in racegroups:
    hdfblockgroups_nonhispraceincomb18p = dfhdf[(dfhdf['QAGE'] >= 18) & (dfhdf['CENHISP'] == '1') & (dfhdf[rg]==1)].groupby(['TABBLKST', 'TABBLKCOU','TABTRACTCE', 'TABBLKGRPCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE + x.TABBLKGRPCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE']).set_index(['GEOID']).reindex(allblockgroupsindex, fill_value=0).reset_index()
    mdfblockgroups_nonhispraceincomb18p = dfmdf[(dfmdf['QAGE'] >= 18) & (dfmdf['CENHISP'] == '1') & (dfmdf[rg]==1)].groupby(['TABBLKST', 'TABBLKCOU','TABTRACTCE', 'TABBLKGRPCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE + x.TABBLKGRPCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE']).set_index(['GEOID']).reindex(allblockgroupsindex, fill_value=0).reset_index()
    blockgroups_nonhispraceincomb18p =  pd.merge(hdfblockgroups_nonhispraceincomb18p, mdfblockgroups_nonhispraceincomb18p, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = blockgroups_nonhispraceincomb18p.pipe(calculate_ss, geography="Block Group", sizecategory = "All", characteristic = "Non-Hispanic {race} Aged 18+".format(race = raceincombdict.get(rg)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

print("{} Race Alone Or In Combination (also by Hispanic) Done".format(datetime.now()))
print("{} Starting Number of Races".format(datetime.now()))

# State Number of Races/ Hispanic Number of Races/Non-Hispanic Number of Races
for n in numracescats:
    hdfstates_numraces = dfhdf[dfhdf['NUMRACES'] == n].groupby(['TABBLKST']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index(['GEOID']).reindex(allstatesindex, fill_value=0).reset_index()
    mdfstates_numraces = dfmdf[dfmdf['NUMRACES'] == n].groupby(['TABBLKST']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index(['GEOID']).reindex(allstatesindex, fill_value=0).reset_index()
    states_numraces =  pd.merge(hdfstates_numraces, mdfstates_numraces, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = states_numraces.pipe(calculate_ss, geography="State", sizecategory = "All", characteristic = "{} Race(s)".format(n))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for n in numracescats:
    hdfstates_hispnumraces = dfhdf[(dfhdf['CENHISP'] == '2') & (dfhdf['NUMRACES'] == n)].groupby(['TABBLKST']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index(['GEOID']).reindex(allstatesindex, fill_value=0).reset_index()
    mdfstates_hispnumraces = dfmdf[(dfmdf['CENHISP'] == '2') & (dfmdf['NUMRACES'] == n)].groupby(['TABBLKST']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index(['GEOID']).reindex(allstatesindex, fill_value=0).reset_index()
    states_hispnumraces =  pd.merge(hdfstates_hispnumraces, mdfstates_hispnumraces, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = states_hispnumraces.pipe(calculate_ss, geography="State", sizecategory = "All", characteristic = "Hispanic {} Race(s)".format(n))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for n in numracescats:
    hdfstates_nonhispnumraces = dfhdf[(dfhdf['CENHISP'] == '1') & (dfhdf['NUMRACES'] == n)].groupby(['TABBLKST']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index(['GEOID']).reindex(allstatesindex, fill_value=0).reset_index()
    mdfstates_nonhispnumraces = dfmdf[(dfmdf['CENHISP'] == '1') & (dfmdf['NUMRACES'] == n)].groupby(['TABBLKST']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index(['GEOID']).reindex(allstatesindex, fill_value=0).reset_index()
    states_nonhispnumraces =  pd.merge(hdfstates_nonhispnumraces, mdfstates_nonhispnumraces, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = states_nonhispnumraces.pipe(calculate_ss, geography="State", sizecategory = "All", characteristic = "Non-Hispanic {} Race(s)".format(n))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# County Number of Races/ Hispanic Number of Races/Non-Hispanic Number of Races
for n in numracescats:
    hdfcounties_numraces = dfhdf[dfhdf['NUMRACES'] == n].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    mdfcounties_numraces = dfmdf[dfmdf['NUMRACES'] == n].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    counties_numraces =  pd.merge(hdfcounties_numraces, mdfcounties_numraces, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = counties_numraces.pipe(calculate_ss, geography="County", sizecategory = "All", characteristic = "{} Race(s)".format(n))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    counties_numraces = counties_numraces.assign(NumRaces_SizeA = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in counties_numraces['NumRaces_SizeA'].cat.categories:
        ss = counties_numraces[counties_numraces['NumRaces_SizeA'] == i].pipe(calculate_ss, geography="County", sizecategory = str(i), characteristic = "{} Race(s)".format(n))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for n in numracescats:
    hdfcounties_hispnumraces = dfhdf[(dfhdf['CENHISP'] == '2') & (dfhdf['NUMRACES'] == n)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    mdfcounties_hispnumraces = dfmdf[(dfmdf['CENHISP'] == '2') & (dfmdf['NUMRACES'] == n)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    counties_hispnumraces =  pd.merge(hdfcounties_hispnumraces, mdfcounties_hispnumraces, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = counties_hispnumraces.pipe(calculate_ss, geography="County", sizecategory = "All", characteristic = "Hispanic {} Race(s)".format(n))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    counties_hispnumraces = counties_hispnumraces.assign(NumRaces_SizeA = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in counties_hispnumraces['NumRaces_SizeA'].cat.categories:
        ss = counties_hispnumraces[counties_hispnumraces['NumRaces_SizeA'] == i].pipe(calculate_ss, geography="County", sizecategory = str(i), characteristic = "Hispanic {} Race(s)".format(n))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for n in numracescats:
    hdfcounties_nonhispnumraces = dfhdf[(dfhdf['CENHISP'] == '1') & (dfhdf['NUMRACES'] == n)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    mdfcounties_nonhispnumraces = dfmdf[(dfmdf['CENHISP'] == '1') & (dfmdf['NUMRACES'] == n)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    counties_nonhispnumraces =  pd.merge(hdfcounties_nonhispnumraces, mdfcounties_nonhispnumraces, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = counties_nonhispnumraces.pipe(calculate_ss, geography="County", sizecategory = "All", characteristic = "Non-Hispanic {} Race(s)".format(n))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    counties_nonhispnumraces = counties_nonhispnumraces.assign(NumRaces_SizeA = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in counties_nonhispnumraces['NumRaces_SizeA'].cat.categories:
        ss = counties_nonhispnumraces[counties_nonhispnumraces['NumRaces_SizeA'] == i].pipe(calculate_ss, geography="County", sizecategory = str(i), characteristic = "Non-Hispanic {} Race(s)".format(n))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Place Number of Races/ Hispanic Number of Races/Non-Hispanic Number of Races
for n in numracescats:
    hdfplaces_numraces = dfhdf[dfhdf['NUMRACES'] == n].groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    mdfplaces_numraces = dfmdf[dfmdf['NUMRACES'] == n].groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    places_numraces =  pd.merge(hdfplaces_numraces, mdfplaces_numraces, on=['IncPlaceGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = places_numraces.pipe(calculate_ss, geography="Place", sizecategory = "All", characteristic = "{} Race(s)".format(n))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    places_numraces = places_numraces.assign(NumRaces_SizeA = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in places_numraces['NumRaces_SizeA'].cat.categories:
        ss = places_numraces[places_numraces['NumRaces_SizeA'] == i].pipe(calculate_ss, geography="Place", sizecategory = str(i), characteristic = "{} Race(s)".format(n))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for n in numracescats:
    hdfplaces_hispnumraces = dfhdf[(dfhdf['CENHISP'] == '2') & (dfhdf['NUMRACES'] == n)].groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    mdfplaces_hispnumraces = dfmdf[(dfmdf['CENHISP'] == '2') & (dfmdf['NUMRACES'] == n)].groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    places_hispnumraces =  pd.merge(hdfplaces_hispnumraces, mdfplaces_hispnumraces, on=['IncPlaceGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = places_hispnumraces.pipe(calculate_ss, geography="Place", sizecategory = "All", characteristic = "Hispanic {} Race(s)".format(n))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    places_hispnumraces = places_hispnumraces.assign(NumRaces_SizeA = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in places_hispnumraces['NumRaces_SizeA'].cat.categories:
        ss = places_hispnumraces[places_hispnumraces['NumRaces_SizeA'] == i].pipe(calculate_ss, geography="Place", sizecategory = str(i), characteristic = "Hispanic {} Race(s)".format(n))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for n in numracescats:
    hdfplaces_nonhispnumraces = dfhdf[(dfhdf['CENHISP'] == '1') & (dfhdf['NUMRACES'] == n)].groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    mdfplaces_nonhispnumraces = dfmdf[(dfmdf['CENHISP'] == '1') & (dfmdf['NUMRACES'] == n)].groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    places_nonhispnumraces =  pd.merge(hdfplaces_nonhispnumraces, mdfplaces_nonhispnumraces, on=['IncPlaceGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = places_nonhispnumraces.pipe(calculate_ss, geography="Place", sizecategory = "All", characteristic = "Non-Hispanic {} Race(s)".format(n))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    places_nonhispnumraces = places_nonhispnumraces.assign(NumRaces_SizeA = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in places_nonhispnumraces['NumRaces_SizeA'].cat.categories:
        ss = places_nonhispnumraces[places_nonhispnumraces['NumRaces_SizeA'] == i].pipe(calculate_ss, geography="Place", sizecategory = str(i), characteristic = "Non-Hispanic {} Race(s)".format(n))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Tract Number of Races/ Hispanic Number of Races/Non-Hispanic Number of Races
for n in numracescats:
    hdftracts_numraces = dfhdf[dfhdf['NUMRACES'] == n].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    mdftracts_numraces = dfmdf[dfmdf['NUMRACES'] == n].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    tracts_numraces =  pd.merge(hdftracts_numraces, mdftracts_numraces, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = tracts_numraces.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "{} Race(s)".format(n))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    tracts_numraces = tracts_numraces.assign(NumRaces_SizeA = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in tracts_numraces['NumRaces_SizeA'].cat.categories:
        ss = tracts_numraces[tracts_numraces['NumRaces_SizeA'] == i].pipe(calculate_ss, geography="Tract", sizecategory = str(i), characteristic = "{} Race(s)".format(n))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for n in numracescats:
    hdftracts_hispnumraces = dfhdf[(dfhdf['CENHISP'] == '2') & (dfhdf['NUMRACES'] == n)].groupby(['TABBLKST', 'TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    mdftracts_hispnumraces = dfmdf[(dfmdf['CENHISP'] == '2') & (dfmdf['NUMRACES'] == n)].groupby(['TABBLKST', 'TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    tracts_hispnumraces =  pd.merge(hdftracts_hispnumraces, mdftracts_hispnumraces, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = tracts_hispnumraces.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "Hispanic {} Race(s)".format(n))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    tracts_hispnumraces = tracts_hispnumraces.assign(NumRaces_SizeA = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in tracts_hispnumraces['NumRaces_SizeA'].cat.categories:
        ss = tracts_hispnumraces[tracts_hispnumraces['NumRaces_SizeA'] == i].pipe(calculate_ss, geography="Tract", sizecategory = str(i), characteristic = "Hispanic {} Race(s)".format(n))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for n in numracescats:
    hdftracts_nonhispnumraces = dfhdf[(dfhdf['CENHISP'] == '1') & (dfhdf['NUMRACES'] == n)].groupby(['TABBLKST', 'TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    mdftracts_nonhispnumraces = dfmdf[(dfmdf['CENHISP'] == '1') & (dfmdf['NUMRACES'] == n)].groupby(['TABBLKST', 'TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    tracts_nonhispnumraces =  pd.merge(hdftracts_nonhispnumraces, mdftracts_nonhispnumraces, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = tracts_nonhispnumraces.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "Non-Hispanic {} Race(s)".format(n))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    tracts_nonhispnumraces = tracts_nonhispnumraces.assign(NumRaces_SizeA = lambda x: pd.cut(x['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False))
    for i in tracts_nonhispnumraces['NumRaces_SizeA'].cat.categories:
        ss = tracts_nonhispnumraces[tracts_nonhispnumraces['NumRaces_SizeA'] == i].pipe(calculate_ss, geography="Tract", sizecategory = str(i), characteristic = "Non-Hispanic {} Race(s)".format(n))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Tract Hispanic Number of Races/Non-Hispanic Number of Races Aged 18+
for n in numracescats:
    hdftracts_hispnumraces18p = dfhdf[(dfhdf['QAGE'] >= 18) & (dfhdf['NUMRACES'] == n) & (dfhdf['CENHISP'] == '2')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    mdftracts_hispnumraces18p = dfmdf[(dfmdf['QAGE'] >= 18) & (dfmdf['NUMRACES'] == n) & (dfmdf['CENHISP'] == '2')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    tracts_hispnumraces18p =  pd.merge(hdftracts_hispnumraces18p, mdftracts_hispnumraces18p, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = tracts_hispnumraces18p.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "Hispanic {} Race(s) Aged 18+".format(n))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    hdftracts_nonhispnumraces18p = dfhdf[(dfhdf['QAGE'] >= 18) & (dfhdf['NUMRACES'] == n) & (dfhdf['CENHISP'] == '1')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    mdftracts_nonhispnumraces18p = dfmdf[(dfmdf['QAGE'] >= 18) & (dfmdf['NUMRACES'] == n) & (dfmdf['CENHISP'] == '1')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    tracts_nonhispnumraces18p =  pd.merge(hdftracts_nonhispnumraces18p, mdftracts_nonhispnumraces18p, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = tracts_nonhispnumraces18p.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "Non-Hispanic {} Race(s) Aged 18+".format(n))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Block Group Hispanic Number of Races/Non-Hispanic Number of Races Aged 18+
for n in numracescats:
    hdfblockgroups_hispnumraces18p = dfhdf[(dfhdf['QAGE'] >= 18) & (dfhdf['NUMRACES'] == n) & (dfhdf['CENHISP'] == '2')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE + x.TABBLKGRPCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE']).set_index(['GEOID']).reindex(allblockgroupsindex, fill_value=0).reset_index()
    mdfblockgroups_hispnumraces18p = dfmdf[(dfmdf['QAGE'] >= 18) & (dfmdf['NUMRACES'] == n) & (dfmdf['CENHISP'] == '2')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE + x.TABBLKGRPCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE']).set_index(['GEOID']).reindex(allblockgroupsindex, fill_value=0).reset_index()
    blockgroups_hispnumraces18p =  pd.merge(hdfblockgroups_hispnumraces18p, mdfblockgroups_hispnumraces18p, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = blockgroups_hispnumraces18p.pipe(calculate_ss, geography="Block Group", sizecategory = "All", characteristic = "Hispanic {} Race(s) Aged 18+".format(n))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    hdfblockgroups_nonhispnumraces18p = dfhdf[(dfhdf['QAGE'] >= 18) & (dfhdf['NUMRACES'] == n) & (dfhdf['CENHISP'] == '1')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE + x.TABBLKGRPCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE']).set_index(['GEOID']).reindex(allblockgroupsindex, fill_value=0).reset_index()
    mdfblockgroups_nonhispnumraces18p = dfmdf[(dfmdf['QAGE'] >= 18) & (dfmdf['NUMRACES'] == n) & (dfmdf['CENHISP'] == '1')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE + x.TABBLKGRPCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE']).set_index(['GEOID']).reindex(allblockgroupsindex, fill_value=0).reset_index()
    blockgroups_nonhispnumraces18p =  pd.merge(hdfblockgroups_nonhispnumraces18p, mdfblockgroups_nonhispnumraces18p, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = blockgroups_nonhispnumraces18p.pipe(calculate_ss, geography="Block Group", sizecategory = "All", characteristic = "Non-Hispanic {} Race(s) Aged 18+".format(n))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
print("{} Num Races Done".format(datetime.now()))

print("{} Start Detailed Age".format(datetime.now()))
# County Sex by 3 Age Groups
for g in qage_3g_cats:
    hdfcounties_3gage = dfhdf[dfhdf['QAGE_3G'] == g].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    mdfcounties_3gage = dfmdf[dfmdf['QAGE_3G'] == g].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    counties_3gage =  pd.merge(hdfcounties_3gage, mdfcounties_3gage, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = counties_3gage.pipe(calculate_ss, geography="County", sizecategory = "All", characteristic = "Age {age}".format(age=g))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for s in sexcats:
    hdfcounties_sex = dfhdf[dfhdf['QSEX'] == s].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    mdfcounties_sex = dfmdf[dfmdf['QSEX'] == s].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    counties_sex =  pd.merge(hdfcounties_sex, mdfcounties_sex, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = counties_sex.pipe(calculate_ss, geography="County", sizecategory = "All", characteristic = "{sex}".format(sex = sexdict.get(s)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for s in sexcats:
    for g in qage_3g_cats:
        hdfcounties_sex3gage = dfhdf[(dfhdf['QAGE_3G'] == g)&(dfhdf['QSEX'] == s)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
        mdfcounties_sex3gage = dfmdf[(dfmdf['QAGE_3G'] == g)&(dfmdf['QSEX'] == s)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
        counties_sex3gage =  pd.merge(hdfcounties_sex3gage, mdfcounties_sex3gage, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
        ss = counties_sex3gage.pipe(calculate_ss, geography="County", sizecategory = "All", characteristic = "{sex} Age {agegroup}".format(sex = sexdict.get(s), agegroup = g))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

print("{} County Sex by 3 Age Done".format(datetime.now()))

# County Sex by 3 Age Groups [0.0, 1000.0)]
for g in qage_3g_cats:
    hdfcounties_3gage = dfhdf[dfhdf['QAGE_3G'] == g].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(counties_lt1000index, fill_value=0).reset_index()
    mdfcounties_3gage = dfmdf[dfmdf['QAGE_3G'] == g].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(counties_lt1000index, fill_value=0).reset_index()
    counties_3gage =  pd.merge(hdfcounties_3gage, mdfcounties_3gage, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = counties_3gage.pipe(calculate_ss, geography="County", sizecategory = "[0.0, 1000.0)", characteristic = "Age {age}".format(age=g))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for s in sexcats:
    hdfcounties_sex = dfhdf[dfhdf['QSEX'] == s].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(counties_lt1000index, fill_value=0).reset_index()
    mdfcounties_sex = dfmdf[dfmdf['QSEX'] == s].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(counties_lt1000index, fill_value=0).reset_index()
    counties_sex =  pd.merge(hdfcounties_sex, mdfcounties_sex, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = counties_sex.pipe(calculate_ss, geography="County", sizecategory = "[0.0, 1000.0)", characteristic = "{sex}".format(sex = sexdict.get(s)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for s in sexcats:
    for g in qage_3g_cats:
        hdfcounties_sex3gage = dfhdf[(dfhdf['QAGE_3G'] == g)&(dfhdf['QSEX'] == s)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(counties_lt1000index, fill_value=0).reset_index()
        mdfcounties_sex3gage = dfmdf[(dfmdf['QAGE_3G'] == g)&(dfmdf['QSEX'] == s)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(counties_lt1000index, fill_value=0).reset_index()
        counties_sex3gage =  pd.merge(hdfcounties_sex3gage, mdfcounties_sex3gage, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
        ss = counties_sex3gage.pipe(calculate_ss, geography="County", sizecategory = "[0.0, 1000.0)", characteristic = "{sex} Age {agegroup}".format(sex = sexdict.get(s), agegroup = g))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Place Sex by 3 Age Groups
for g in qage_3g_cats:
    hdfplaces_3gage = dfhdf[dfhdf['QAGE_3G'] == g].groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    mdfplaces_3gage = dfmdf[dfmdf['QAGE_3G'] == g].groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    places_3gage =  pd.merge(hdfplaces_3gage, mdfplaces_3gage, on=['IncPlaceGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = places_3gage.pipe(calculate_ss, geography="Place", sizecategory = "All", characteristic = "Age {}".format(g))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for s in sexcats:
    hdfplaces_sex = dfhdf[dfhdf['QSEX'] == s].groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    mdfplaces_sex = dfmdf[dfmdf['QSEX'] == s].groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    places_sex =  pd.merge(hdfplaces_sex, mdfplaces_sex, on=['IncPlaceGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = places_sex.pipe(calculate_ss, geography="Place", sizecategory = "All", characteristic = "{sex}".format(sex = sexdict.get(s)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for s in sexcats:
    for g in qage_3g_cats:
        hdfplaces_sex3gage = dfhdf[(dfhdf['QAGE_3G'] == g) & (dfhdf['QSEX']== s)].groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
        mdfplaces_sex3gage = dfmdf[(dfmdf['QAGE_3G'] == g) & (dfmdf['QSEX']== s)].groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
        places_sex3gage =  pd.merge(hdfplaces_sex3gage, mdfplaces_sex3gage, on=['IncPlaceGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
        ss = places_sex3gage.pipe(calculate_ss, geography="Place", sizecategory = "All", characteristic = "{sex} Age {agegroup}".format(sex = sexdict.get(s), agegroup = g))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Place Sex by 3 Age Groups [0.0, 500.0)
for g in qage_3g_cats:
    hdfplaces_3gage = dfhdf[dfhdf['QAGE_3G'] == g].groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index(['IncPlaceGEOID']).reindex(places_lt500index, fill_value=0).reset_index()
    mdfplaces_3gage = dfmdf[dfmdf['QAGE_3G'] == g].groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index(['IncPlaceGEOID']).reindex(places_lt500index, fill_value=0).reset_index()
    places_3gage =  pd.merge(hdfplaces_3gage, mdfplaces_3gage, on=['IncPlaceGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = places_3gage.pipe(calculate_ss, geography="Place", sizecategory = "[0.0, 500.0)", characteristic = "Age {}".format(g))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for s in sexcats:
    hdfplaces_sex = dfhdf[dfhdf['QSEX'] == s].groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index(['IncPlaceGEOID']).reindex(places_lt500index, fill_value=0).reset_index()
    mdfplaces_sex = dfmdf[dfmdf['QSEX'] == s].groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index(['IncPlaceGEOID']).reindex(places_lt500index, fill_value=0).reset_index()
    places_sex =  pd.merge(hdfplaces_sex, mdfplaces_sex, on=['IncPlaceGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = places_sex.pipe(calculate_ss, geography="Place", sizecategory = "[0.0, 500.0)", characteristic = "{sex}".format(sex = sexdict.get(s)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for s in sexcats:
    for g in qage_3g_cats:
        hdfplaces_sex3gage = dfhdf[(dfhdf['QAGE_3G'] == g) & (dfhdf['QSEX']== s)].groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index(['IncPlaceGEOID']).reindex(places_lt500index, fill_value=0).reset_index()
        mdfplaces_sex3gage = dfmdf[(dfmdf['QAGE_3G'] == g) & (dfmdf['QSEX']== s)].groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index(['IncPlaceGEOID']).reindex(places_lt500index, fill_value=0).reset_index()
        places_sex3gage =  pd.merge(hdfplaces_sex3gage, mdfplaces_sex3gage, on=['IncPlaceGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
        ss = places_sex3gage.pipe(calculate_ss, geography="Place", sizecategory = "[0.0, 500.0)", characteristic = "{sex} Age {agegroup}".format(sex = sexdict.get(s), agegroup = g))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Tract Sex by 3 Age Groups
for g in qage_3g_cats:
    hdftracts_3gage = dfhdf[dfhdf['QAGE_3G'] == g].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    mdftracts_3gage = dfmdf[dfmdf['QAGE_3G'] == g].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    tracts_3gage =  pd.merge(hdftracts_3gage, mdftracts_3gage, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = tracts_3gage.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "Age {}".format(g))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for s in sexcats:
    hdftracts_sex = dfhdf[(dfhdf['QSEX']== s)].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    mdftracts_sex = dfmdf[(dfmdf['QSEX']== s)].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    tracts_sex =  pd.merge(hdftracts_sex, mdftracts_sex, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = tracts_sex.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "{sex}".format(sex = sexdict.get(s)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
for s in sexcats:
    for g in qage_3g_cats:
        hdftracts_sex3gage = dfhdf[(dfhdf['QAGE_3G'] == g) & (dfhdf['QSEX']== s)].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
        mdftracts_sex3gage = dfmdf[(dfmdf['QAGE_3G'] == g) & (dfmdf['QSEX']== s)].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
        tracts_sex3gage =  pd.merge(hdftracts_sex3gage, mdftracts_sex3gage, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
        ss = tracts_sex3gage.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "{sex} Age {agegroup}".format(sex = sexdict.get(s), agegroup = g))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
print("{} Sex By 3 Age Groups Done".format(datetime.now()))

# County 5-Year Age Groups
for g in qage_5y_cats:
    hdfcounties_5yage = dfhdf[dfhdf['QAGE_5Y'] == g].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    mdfcounties_5yage = dfmdf[dfmdf['QAGE_5Y'] == g].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    counties_5yage =  pd.merge(hdfcounties_5yage, mdfcounties_5yage, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = counties_5yage.pipe(calculate_ss, geography="County", sizecategory = "All", characteristic = "Age {}".format(g))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
# County Sex x 5-Year Age Groups
for s in sexcats:
    for g in qage_5y_cats:
        hdfcounties_sex5yage = dfhdf[(dfhdf['QAGE_5Y'] == g) & (dfhdf['QSEX']== s)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
        mdfcounties_sex5yage = dfmdf[(dfmdf['QAGE_5Y'] == g) & (dfmdf['QSEX']== s)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
        counties_sex5yage =  pd.merge(hdfcounties_sex5yage, mdfcounties_sex5yage, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
        ss = counties_sex5yage.pipe(calculate_ss, geography="County", sizecategory = "All", characteristic = "{sex} Age {agegroup}".format(sex = sexdict.get(s), agegroup = g))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

if runAGEBYRACE:
    # County 5-Year Age Groups by RACEALONE
    for r in racealonecats:
        # for g in qage_5y_cats:
        #     hdfcounties_5yage = dfhdf[(dfhdf['QAGE_5Y'] == g)&(dfhdf['RACEALONE'] == r)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
        #     mdfcounties_5yage = dfmdf[(dfmdf['QAGE_5Y'] == g)&(dfmdf['RACEALONE'] == r)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
        #     counties_5yage =  pd.merge(hdfcounties_5yage, mdfcounties_5yage, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
        #     ss = counties_5yage.pipe(calculate_ss, geography="County", sizecategory = "All", characteristic = "{race} Age {agegroup}".format(race = racealonedict.get(r),agegroup=g))
        #     outputdf = outputdf.append(ss, ignore_index=True, sort=False)
        for s in sexcats:
            for g in qage_5y_cats:
                hdfcounties_sex5yage = dfhdf[(dfhdf['QAGE_5Y'] == g) & (dfhdf['QSEX']== s)&(dfhdf['RACEALONE'] == r)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
                mdfcounties_sex5yage = dfmdf[(dfmdf['QAGE_5Y'] == g) & (dfmdf['QSEX']== s)&(dfmdf['RACEALONE'] == r)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
                counties_sex5yage =  pd.merge(hdfcounties_sex5yage, mdfcounties_sex5yage, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
                ss = counties_sex5yage.pipe(calculate_ss, geography="County", sizecategory = "All", characteristic = "{race} {sex} Age {agegroup}".format(race = racealonedict.get(r), sex = sexdict.get(s), agegroup = g))
                outputdf = outputdf.append(ss, ignore_index=True, sort=False)

    # County 5-Year Age Groups by RACE AOIC
    for rg in racegroups:
        # for g in qage_5y_cats:
        #     hdfcounties_5yage = dfhdf[(dfhdf['QAGE_5Y'] == g)&(dfhdf[rg]==1)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
        #     mdfcounties_5yage = dfmdf[(dfmdf['QAGE_5Y'] == g)&(dfmdf[rg]==1)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
        #     counties_5yage =  pd.merge(hdfcounties_5yage, mdfcounties_5yage, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
        #     ss = counties_5yage.pipe(calculate_ss, geography="County", sizecategory = "All", characteristic = "{race} Age {agegroup}".format(race = raceincombdict.get(rg),agegroup=g))
        #     outputdf = outputdf.append(ss, ignore_index=True, sort=False)
        for s in sexcats:
            for g in qage_5y_cats:
                hdfcounties_sex5yage = dfhdf[(dfhdf['QAGE_5Y'] == g) & (dfhdf['QSEX']== s)&(dfhdf[rg]==1)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
                mdfcounties_sex5yage = dfmdf[(dfmdf['QAGE_5Y'] == g) & (dfmdf['QSEX']== s)&(dfmdf[rg]==1)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
                counties_sex5yage =  pd.merge(hdfcounties_sex5yage, mdfcounties_sex5yage, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
                ss = counties_sex5yage.pipe(calculate_ss, geography="County", sizecategory = "All", characteristic = "{race} {sex} Age {agegroup}".format(race = raceincombdict.get(rg), sex = sexdict.get(s), agegroup = g))
                outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Place 5-Year Age Groups
for g in qage_5y_cats:
    hdfplaces_5yage = dfhdf[dfhdf['QAGE_5Y'] == g].groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    mdfplaces_5yage = dfmdf[dfmdf['QAGE_5Y'] == g].groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    places_5yage =  pd.merge(hdfplaces_5yage, mdfplaces_5yage, on=['IncPlaceGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = places_5yage.pipe(calculate_ss, geography="Place", sizecategory = "All", characteristic = "Age {}".format(g))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
# Place Sex x 5-Year Age Groups
for s in sexcats:
    for g in qage_5y_cats:
        hdfplaces_sex5yage = dfhdf[(dfhdf['QAGE_5Y'] == g) & (dfhdf['QSEX']== s)].groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
        mdfplaces_sex5yage = dfmdf[(dfmdf['QAGE_5Y'] == g) & (dfmdf['QSEX']== s)].groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
        places_sex5yage =  pd.merge(hdfplaces_sex5yage, mdfplaces_sex5yage, on=['IncPlaceGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
        ss = places_sex5yage.pipe(calculate_ss, geography="Place", sizecategory = "All", characteristic = "{sex} Age {agegroup}".format(sex = sexdict.get(s), agegroup = g))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)


# Tract 5-Year Age Groups
for g in qage_5y_cats:
    hdftracts_5yage = dfhdf[dfhdf['QAGE_5Y'] == g].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    mdftracts_5yage = dfmdf[dfmdf['QAGE_5Y'] == g].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    tracts_5yage =  pd.merge(hdftracts_5yage, mdftracts_5yage, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = tracts_5yage.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "Age {}".format(g))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
# Tract Sex x 5-Year Age Groups
for s in sexcats:
    for g in qage_5y_cats:
        hdftracts_sex5yage = dfhdf[(dfhdf['QAGE_5Y'] == g) & (dfhdf['QSEX']== s)].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
        mdftracts_sex5yage = dfmdf[(dfmdf['QAGE_5Y'] == g) & (dfmdf['QSEX']== s)].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
        tracts_sex5yage =  pd.merge(hdftracts_sex5yage, mdftracts_sex5yage, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
        ss = tracts_sex5yage.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "{sex} Age {agegroup}".format(sex = sexdict.get(s), agegroup = g))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

if runPRhere:
    # PR Counties/Municipios Sex and 5-Year Age Groups
    for s in sexcats:
        hdfcountiespr_sex = dfhdfpr[(dfhdfpr['QSEX']== s)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesprindex, fill_value=0).reset_index()
        mdfcountiespr_sex = dfmdfpr[(dfmdfpr['QSEX']== s)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesprindex, fill_value=0).reset_index()
        countiespr_sex =  pd.merge(hdfcountiespr_sex, mdfcountiespr_sex, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
        ss = countiespr_sex.pipe(calculate_ss, geography="PR County/Municipio", sizecategory = "All", characteristic = "{sex}".format(sex = sexdict.get(s)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    # PR Counties/Municipios 5-Year Age Groups
    for g in qage_5y_cats:
        hdfcountiespr_5yage = dfhdfpr[dfhdfpr['QAGE_5Y'] == g].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesprindex, fill_value=0).reset_index()
        mdfcountiespr_5yage = dfmdfpr[dfmdfpr['QAGE_5Y'] == g].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesprindex, fill_value=0).reset_index()
        countiespr_5yage =  pd.merge(hdfcountiespr_5yage, mdfcountiespr_5yage, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
        ss = countiespr_5yage.pipe(calculate_ss, geography="PR County/Municipio", sizecategory = "All", characteristic = "Age {}".format(g))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    # PR Counties/Municipios Sex x 5-Year Age Groups
    for s in sexcats:
        for g in qage_5y_cats:
            hdfcountiespr_sex5yage = dfhdfpr[(dfhdfpr['QAGE_5Y'] == g) & (dfhdfpr['QSEX']== s)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesprindex, fill_value=0).reset_index()
            mdfcountiespr_sex5yage = dfmdfpr[(dfmdfpr['QAGE_5Y'] == g) & (dfmdfpr['QSEX']== s)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesprindex, fill_value=0).reset_index()
            countiespr_sex5yage =  pd.merge(hdfcountiespr_sex5yage, mdfcountiespr_sex5yage, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
            ss = countiespr_sex5yage.pipe(calculate_ss, geography="PR County/Municipio", sizecategory = "All", characteristic = "{sex} Age {agegroup}".format(sex = sexdict.get(s), agegroup = g))
            outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    # PR Tract Sex and 5-Year Age Groups
    for s in sexcats:
        hdftractspr_sex = dfhdfpr[(dfhdfpr['QSEX']== s)].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsprindex, fill_value=0).reset_index()
        mdftractspr_sex = dfmdfpr[(dfmdfpr['QSEX']== s)].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsprindex, fill_value=0).reset_index()
        tractspr_sex =  pd.merge(hdftractspr_sex, mdftractspr_sex, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
        ss = tractspr_sex.pipe(calculate_ss, geography="PR Tract", sizecategory = "All", characteristic = "{sex}".format(sex = sexdict.get(s)))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    # PR Tract 5-Year Age Groups
    for g in qage_5y_cats:
        hdftractspr_5yage = dfhdfpr[dfhdfpr['QAGE_5Y'] == g].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsprindex, fill_value=0).reset_index()
        mdftractspr_5yage = dfmdfpr[dfmdfpr['QAGE_5Y'] == g].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsprindex, fill_value=0).reset_index()
        tractspr_5yage =  pd.merge(hdftractspr_5yage, mdftractspr_5yage, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
        ss = tractspr_5yage.pipe(calculate_ss, geography="PR Tract", sizecategory = "All", characteristic = "Age {}".format(g))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    # PR Tract Sex x 5-Year Age Groups
    for s in sexcats:
        for g in qage_5y_cats:
            hdftractspr_sex5yage = dfhdfpr[(dfhdfpr['QAGE_5Y'] == g) & (dfhdfpr['QSEX']== s)].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsprindex, fill_value=0).reset_index()
            mdftractspr_sex5yage = dfmdfpr[(dfmdfpr['QAGE_5Y'] == g) & (dfmdfpr['QSEX']== s)].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsprindex, fill_value=0).reset_index()
            tractspr_sex5yage =  pd.merge(hdftractspr_sex5yage, mdftractspr_sex5yage, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
            ss = tractspr_sex5yage.pipe(calculate_ss, geography="PR Tract", sizecategory = "All", characteristic = "{sex} Age {agegroup}".format(sex = sexdict.get(s), agegroup = g))
            outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Federal AIR Sex and 5-Year Age Groups
for s in sexcats:
    hdffedairs_sex = dfhdf[(dfhdf['QSEX']== s)].groupby(['FedAIRGEOID']).size().reset_index(name='HDF_Population').set_index(['FedAIRGEOID']).reindex(allfedairsindex, fill_value=0).reset_index()
    mdffedairs_sex = dfmdf[(dfmdf['QSEX']== s)].groupby(['FedAIRGEOID']).size().reset_index(name='MDF_Population').set_index(['FedAIRGEOID']).reindex(allfedairsindex, fill_value=0).reset_index()
    fedairs_sex =  pd.merge(hdffedairs_sex, mdffedairs_sex, on=['FedAIRGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = fedairs_sex.pipe(calculate_ss, geography="Fed AIR", sizecategory = "All", characteristic = "{sex}".format(sex = sexdict.get(s)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
# Federal AIR 5-Year Age Groups
for g in qage_5y_cats:
    hdffedairs_5yage = dfhdf[dfhdf['QAGE_5Y'] == g].groupby(['FedAIRGEOID']).size().reset_index(name='HDF_Population').set_index(['FedAIRGEOID']).reindex(allfedairsindex, fill_value=0).reset_index()
    mdffedairs_5yage = dfmdf[dfmdf['QAGE_5Y'] == g].groupby(['FedAIRGEOID']).size().reset_index(name='MDF_Population').set_index(['FedAIRGEOID']).reindex(allfedairsindex, fill_value=0).reset_index()
    fedairs_5yage =  pd.merge(hdffedairs_5yage, mdffedairs_5yage, on=['FedAIRGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = fedairs_5yage.pipe(calculate_ss, geography="Fed AIR", sizecategory = "All", characteristic = "Age {}".format(g))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
#    Federal AIR Sex x 5-Year Age Groups
for s in sexcats:
    for g in qage_5y_cats:
        hdffedairs_sex5yage = dfhdf[(dfhdf['QAGE_5Y'] == g) & (dfhdf['QSEX']== s)].groupby(['FedAIRGEOID']).size().reset_index(name='HDF_Population').set_index(['FedAIRGEOID']).reindex(allfedairsindex, fill_value=0).reset_index()
        mdffedairs_sex5yage = dfmdf[(dfmdf['QAGE_5Y'] == g) & (dfmdf['QSEX']== s)].groupby(['FedAIRGEOID']).size().reset_index(name='MDF_Population').set_index(['FedAIRGEOID']).reindex(allfedairsindex, fill_value=0).reset_index()
        fedairs_sex5yage =  pd.merge(hdffedairs_sex5yage, mdffedairs_sex5yage, on=['FedAIRGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
        ss = fedairs_sex5yage.pipe(calculate_ss, geography="Fed AIR", sizecategory = "All", characteristic = "{sex} Age {agegroup}".format(sex = sexdict.get(s), agegroup = g))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

if runAGEBYRACE:
    # Federal AIR 5-Year Age Groups by RACE ALONE
    for r in racealonecats:
        for s in sexcats:
            for g in qage_5y_cats:
                hdffedairs_sex5yage = dfhdf[(dfhdf['QAGE_5Y'] == g) & (dfhdf['QSEX']== s)&(dfhdf['RACEALONE'] == r)].groupby(['FedAIRGEOID']).size().reset_index(name='HDF_Population').set_index(['FedAIRGEOID']).reindex(allfedairsindex, fill_value=0).reset_index()
                mdffedairs_sex5yage = dfmdf[(dfmdf['QAGE_5Y'] == g) & (dfmdf['QSEX']== s)&(dfmdf['RACEALONE'] == r)].groupby(['FedAIRGEOID']).size().reset_index(name='MDF_Population').set_index(['FedAIRGEOID']).reindex(allfedairsindex, fill_value=0).reset_index()
                fedairs_sex5yage =  pd.merge(hdffedairs_sex5yage, mdffedairs_sex5yage, on=['FedAIRGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
                ss = fedairs_sex5yage.pipe(calculate_ss, geography="Fed AIR", sizecategory = "All", characteristic = "{race} {sex} Age {agegroup}".format(race = racealonedict.get(r), sex = sexdict.get(s), agegroup = g))
                outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    # Fed AIR 5-Year Age Groups by RACE AOIC
    for rg in racegroups:
        for s in sexcats:
            for g in qage_5y_cats:
                hdffedairs_sex5yage = dfhdf[(dfhdf['QAGE_5Y'] == g) & (dfhdf['QSEX']== s) & (dfhdf[rg]==1)].groupby(['FedAIRGEOID']).size().reset_index(name='HDF_Population').set_index(['FedAIRGEOID']).reindex(allfedairsindex, fill_value=0).reset_index()
                mdffedairs_sex5yage = dfmdf[(dfmdf['QAGE_5Y'] == g) & (dfmdf['QSEX']== s) & (dfmdf[rg]==1)].groupby(['FedAIRGEOID']).size().reset_index(name='MDF_Population').set_index(['FedAIRGEOID']).reindex(allfedairsindex, fill_value=0).reset_index()
                fedairs_sex5yage =  pd.merge(hdffedairs_sex5yage, mdffedairs_sex5yage, on=['FedAIRGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
                ss = fedairs_sex5yage.pipe(calculate_ss, geography="Fed AIR", sizecategory = "All", characteristic = "{race} {sex} Age {agegroup}".format(race = raceincombdict.get(rg), sex = sexdict.get(s), agegroup = g))
                outputdf = outputdf.append(ss, ignore_index=True, sort=False)


# OTSA Sex and 5-Year Age Groups
for s in sexcats:
    hdfotsas_sex = dfhdf[(dfhdf['QSEX']== s)].groupby(['OTSAGEOID']).size().reset_index(name='HDF_Population').set_index(['OTSAGEOID']).reindex(allotsasindex, fill_value=0).reset_index()
    mdfotsas_sex = dfmdf[(dfmdf['QSEX']== s)].groupby(['OTSAGEOID']).size().reset_index(name='MDF_Population').set_index(['OTSAGEOID']).reindex(allotsasindex, fill_value=0).reset_index()
    otsas_sex =  pd.merge(hdfotsas_sex, mdfotsas_sex, on=['OTSAGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = otsas_sex.pipe(calculate_ss, geography="OTSA", sizecategory = "All", characteristic = "{sex}".format(sex = sexdict.get(s)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
# OTSA 5-Year Age Groups
for g in qage_5y_cats:
    hdfotsas_5yage = dfhdf[(dfhdf['QAGE_5Y'] == g)].groupby(['OTSAGEOID']).size().reset_index(name='HDF_Population').set_index(['OTSAGEOID']).reindex(allotsasindex, fill_value=0).reset_index()
    mdfotsas_5yage = dfmdf[(dfmdf['QAGE_5Y'] == g)].groupby(['OTSAGEOID']).size().reset_index(name='MDF_Population').set_index(['OTSAGEOID']).reindex(allotsasindex, fill_value=0).reset_index()
    otsas_5yage =  pd.merge(hdfotsas_5yage, mdfotsas_5yage, on=['OTSAGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = otsas_5yage.pipe(calculate_ss, geography="OTSA", sizecategory = "All", characteristic = "Age {}".format(g))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
# OTSA Sex x 5-Year Age Groups
for s in sexcats:
    for g in qage_5y_cats:
        hdfotsas_sex5yage = dfhdf[(dfhdf['QAGE_5Y'] == g) & (dfhdf['QSEX']== s)].groupby(['OTSAGEOID']).size().reset_index(name='HDF_Population').set_index(['OTSAGEOID']).reindex(allotsasindex, fill_value=0).reset_index()
        mdfotsas_sex5yage = dfmdf[(dfmdf['QAGE_5Y'] == g) & (dfmdf['QSEX']== s)].groupby(['OTSAGEOID']).size().reset_index(name='MDF_Population').set_index(['OTSAGEOID']).reindex(allotsasindex, fill_value=0).reset_index()
        otsas_sex5yage =  pd.merge(hdfotsas_sex5yage, mdfotsas_sex5yage, on=['OTSAGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
        ss = otsas_sex5yage.pipe(calculate_ss, geography="OTSA", sizecategory = "All", characteristic = "{sex} Age {agegroup}".format(sex = sexdict.get(s), agegroup = g))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# ANVSA Sex and 5-Year Age Groups
for s in sexcats:
    hdfanvsas_sex = dfhdf[(dfhdf['QSEX']== s)].groupby(['ANVSAGEOID']).size().reset_index(name='HDF_Population').set_index(['ANVSAGEOID']).reindex(allanvsasindex, fill_value=0).reset_index()
    mdfanvsas_sex = dfmdf[(dfmdf['QSEX']== s)].groupby(['ANVSAGEOID']).size().reset_index(name='MDF_Population').set_index(['ANVSAGEOID']).reindex(allanvsasindex, fill_value=0).reset_index()
    anvsas_sex =  pd.merge(hdfanvsas_sex, mdfanvsas_sex, on=['ANVSAGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = anvsas_sex.pipe(calculate_ss, geography="ANVSA", sizecategory = "All", characteristic = "{sex}".format(sex = sexdict.get(s)))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
# ANVSA 5-Year Age Groups
for g in qage_5y_cats:
    hdfanvsas_5yage = dfhdf[dfhdf['QAGE_5Y'] == g].groupby(['ANVSAGEOID']).size().reset_index(name='HDF_Population').set_index(['ANVSAGEOID']).reindex(allanvsasindex, fill_value=0).reset_index()
    mdfanvsas_5yage = dfmdf[dfmdf['QAGE_5Y'] == g].groupby(['ANVSAGEOID']).size().reset_index(name='MDF_Population').set_index(['ANVSAGEOID']).reindex(allanvsasindex, fill_value=0).reset_index()
    anvsas_5yage =  pd.merge(hdfanvsas_5yage, mdfanvsas_5yage, on=['ANVSAGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = anvsas_5yage.pipe(calculate_ss, geography="ANVSA", sizecategory = "All", characteristic = "Age {}".format(g))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
# ANVSA Sex x 5-Year Age Groups
for s in sexcats:
    for g in qage_5y_cats:
        hdfanvsas_sex5yage = dfhdf[(dfhdf['QAGE_5Y'] == g) & (dfhdf['QSEX']== s)].groupby(['ANVSAGEOID']).size().reset_index(name='HDF_Population').set_index(['ANVSAGEOID']).reindex(allanvsasindex, fill_value=0).reset_index()
        mdfanvsas_sex5yage = dfmdf[(dfmdf['QAGE_5Y'] == g) & (dfmdf['QSEX']== s)].groupby(['ANVSAGEOID']).size().reset_index(name='MDF_Population').set_index(['ANVSAGEOID']).reindex(allanvsasindex, fill_value=0).reset_index()
        anvsas_sex5yage =  pd.merge(hdfanvsas_sex5yage, mdfanvsas_sex5yage, on=['ANVSAGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
        ss = anvsas_sex5yage.pipe(calculate_ss, geography="ANVSA", sizecategory = "All", characteristic = "{sex} Age {agegroup}".format(sex = sexdict.get(s), agegroup = g))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

print("{} Sex By 5 Year Age Groups Done".format(datetime.now()))

# GQ 
for g in gqinstcats:
    hdfstates_gqinst = dfhdf[dfhdf['GQINST'] == g].groupby(['TABBLKST']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index(['GEOID']).reindex(allstatesindex, fill_value=0).reset_index()
    mdfstates_gqinst = dfmdf[dfmdf['GQINST'] == g].groupby(['TABBLKST']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index(['GEOID']).reindex(allstatesindex, fill_value=0).reset_index()
    states_gqinst =  pd.merge(hdfstates_gqinst, mdfstates_gqinst, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = states_gqinst.pipe(calculate_ss, geography="State", sizecategory = "All", characteristic = "GQ {}".format(g))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

for g in gqmajortypecats:
    hdfstates_gqmajtype = dfhdf[dfhdf['GQMAJORTYPE'] == g].groupby(['TABBLKST']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index(['GEOID']).reindex(allstatesindex, fill_value=0).reset_index()
    mdfstates_gqmajtype = dfmdf[dfmdf['GQMAJORTYPE'] == g].groupby(['TABBLKST']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST']).set_index(['GEOID']).reindex(allstatesindex, fill_value=0).reset_index()
    states_gqmajtype =  pd.merge(hdfstates_gqmajtype, mdfstates_gqmajtype, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = states_gqmajtype.pipe(calculate_ss, geography="State", sizecategory = "All", characteristic = "GQ {}".format(g))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

for g in gqinstcats:
    hdfcounties_gqinst = dfhdf[dfhdf['GQINST'] == g].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    mdfcounties_gqinst = dfmdf[dfmdf['GQINST'] == g].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    counties_gqinst =  pd.merge(hdfcounties_gqinst, mdfcounties_gqinst, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = counties_gqinst.pipe(calculate_ss, geography="County", sizecategory = "All", characteristic = "GQ {}".format(g))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    counties_gqinst = pd.merge(counties_gqinst, dfhdf.groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_TotalPopulation').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']), on ="GEOID", how="outer",validate=mergeValidation)
    counties_gqinst['Total_PopSize'] = pd.cut(counties_gqinst['HDF_TotalPopulation'], [0,1000,5000,10000,50000,100000,np.inf], include_lowest=True, right=False)
    for i in counties_gqinst['Total_PopSize'].cat.categories:
        ss = counties_gqinst[counties_gqinst['Total_PopSize'] == i].pipe(calculate_ss, geography="County", sizecategory = str(i), characteristic = "GQ {}".format(g))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

for g in gqmajortypecats:
    hdfcounties_gqmajtype = dfhdf[dfhdf['GQMAJORTYPE'] == g].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    mdfcounties_gqmajtype = dfmdf[dfmdf['GQMAJORTYPE'] == g].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
    counties_gqmajtype =  pd.merge(hdfcounties_gqmajtype, mdfcounties_gqmajtype, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = counties_gqmajtype.pipe(calculate_ss, geography="County", sizecategory = "All", characteristic = "GQ {}".format(g))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    counties_gqmajtype = pd.merge(counties_gqmajtype, dfhdf.groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_TotalPopulation').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']), on ="GEOID", how="outer",validate=mergeValidation)
    counties_gqmajtype['Total_PopSize'] = pd.cut(counties_gqmajtype['HDF_TotalPopulation'], [0,1000,5000,10000,50000,100000,np.inf], include_lowest=True, right=False)
    for i in counties_gqmajtype['Total_PopSize'].cat.categories:
        ss = counties_gqmajtype[counties_gqmajtype['Total_PopSize'] == i].pipe(calculate_ss, geography="County", sizecategory = str(i), characteristic = "GQ {}".format(g))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

for g in gqinstcats:
    hdfplaces_gqinst = dfhdf[dfhdf['GQINST'] == g].groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    mdfplaces_gqinst = dfmdf[dfmdf['GQINST'] == g].groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    places_gqinst =  pd.merge(hdfplaces_gqinst, mdfplaces_gqinst, on=['IncPlaceGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = places_gqinst.pipe(calculate_ss, geography="Place", sizecategory = "All", characteristic = "GQ {}".format(g))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    places_gqinst = pd.merge(places_gqinst, dfhdf.groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_TotalPopulation').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index(), on ="IncPlaceGEOID", how="outer",validate=mergeValidation)
    places_gqinst['Total_PopSize'] = pd.cut(places_gqinst['HDF_TotalPopulation'], [0,500,1000,5000,10000,50000,100000,np.inf], include_lowest=True, right=False)
    for i in places_gqinst['Total_PopSize'].cat.categories:
        ss = places_gqinst[places_gqinst['Total_PopSize'] == i].pipe(calculate_ss, geography="Place", sizecategory = str(i), characteristic = "GQ {}".format(g))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

for g in gqmajortypecats:
    hdfplaces_gqmajtype = dfhdf[dfhdf['GQMAJORTYPE'] == g].groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    mdfplaces_gqmajtype = dfmdf[dfmdf['GQMAJORTYPE'] == g].groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
    places_gqmajtype =  pd.merge(hdfplaces_gqmajtype, mdfplaces_gqmajtype, on=['IncPlaceGEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = places_gqmajtype.pipe(calculate_ss, geography="Place", sizecategory = "All", characteristic = "GQ {}".format(g))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    places_gqmajtype = pd.merge(places_gqmajtype, dfhdf.groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_TotalPopulation').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index(), on ="IncPlaceGEOID", how="outer",validate=mergeValidation)
    places_gqmajtype['Total_PopSize'] = pd.cut(places_gqmajtype['HDF_TotalPopulation'], [0,500,1000,5000,10000,50000,100000,np.inf], include_lowest=True, right=False)
    for i in places_gqmajtype['Total_PopSize'].cat.categories:
        ss = places_gqmajtype[places_gqmajtype['Total_PopSize'] == i].pipe(calculate_ss, geography="Place", sizecategory = str(i), characteristic = "GQ {}".format(g))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

for g in gqinstcats:
    hdftracts_gqinst = dfhdf[dfhdf['GQINST'] == g].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    mdftracts_gqinst = dfmdf[dfmdf['GQINST'] == g].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    tracts_gqinst =  pd.merge(hdftracts_gqinst, mdftracts_gqinst, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = tracts_gqinst.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "GQ {}".format(g))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

for g in gqmajortypecats:
    hdftracts_gqmajtype = dfhdf[dfhdf['GQMAJORTYPE'] == g].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    mdftracts_gqmajtype = dfmdf[dfmdf['GQMAJORTYPE'] == g].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID']).reindex(alltractsindex, fill_value=0).reset_index()
    tracts_gqmajtype =  pd.merge(hdftracts_gqmajtype, mdftracts_gqmajtype, on=['GEOID'], how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = tracts_gqmajtype.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "GQ {}".format(g))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

print("{} GQ Types Done".format(datetime.now()))



# Counties Absolute Change in Median Age/Sex Ratio 
hdfcounties_medage = dfhdf.groupby(['TABBLKST', 'TABBLKCOU'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='HDF_MedianAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
mdfcounties_medage = dfmdf.groupby(['TABBLKST', 'TABBLKCOU'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='MDF_MedianAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
counties_medage =  pd.merge(hdfcounties_medage, mdfcounties_medage, on='GEOID', how = 'outer', validate = mergeValidation)
counties_medage  = counties_medage.assign(AbsDiffMedAge = lambda x: np.abs(x['HDF_MedianAge'] - x['MDF_MedianAge']))
counties_medage.to_csv(f"{OUTPUTDIR}/counties_medage.csv", index=False)
ss = pd.DataFrame({'Geography':'County', 'Size_Category':'All', 'Characteristic':'Average Absolute Change in Median Age','NumCells':len(counties_medage),'AvgAbsDiffMedAge': np.nanmean(counties_medage['AbsDiffMedAge'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
counties_medage = pd.merge(counties_medage, dfhdf.groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_TotalPopulation').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']), on ="GEOID", how="outer",validate=mergeValidation)
counties_medage['Total_PopSize'] = pd.cut(counties_medage['HDF_TotalPopulation'], [0,1000,5000,10000,50000,100000,np.inf], include_lowest=True, right=False)
for i in counties_medage['Total_PopSize'].cat.categories:
    ss = pd.DataFrame({'Geography':'County', 'Size_Category':str(i), 'Characteristic':'Average Absolute Change in Median Age','NumCells':len(counties_medage[counties_medage['Total_PopSize'] == i]),'AvgAbsDiffMedAge': np.nanmean(counties_medage.loc[counties_medage['Total_PopSize'] == i,'AbsDiffMedAge'])}, index=[0])
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

hdfcounties_sexratio = dfhdf.groupby(['TABBLKST', 'TABBLKCOU', 'QSEX']).size().unstack().fillna(0).reset_index().assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index().assign(HDF_SexRatio = lambda x: 100*x['1']/x['2']).drop(columns=['1','2'])
mdfcounties_sexratio = dfmdf.groupby(['TABBLKST', 'TABBLKCOU', 'QSEX']).size().unstack().fillna(0).reset_index().assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index().assign(MDF_SexRatio = lambda x: 100*x['1']/x['2']).drop(columns=['1','2'])
counties_sexratio =  pd.merge(hdfcounties_sexratio, mdfcounties_sexratio, on='GEOID', how = 'outer', validate = mergeValidation)
counties_sexratio  = counties_sexratio.assign(AbsDiffSexRatio = lambda x: np.abs(x['HDF_SexRatio'] - x['MDF_SexRatio']))
counties_sexratio.to_csv(f"{OUTPUTDIR}/counties_sexratio.csv", index=False)
ss = pd.DataFrame({'Geography':'County', 'Size_Category':'All', 'Characteristic':'Average Absolute Change in Sex Ratio','NumCells':len(counties_sexratio),'AvgAbsDiffSexRatio': np.nanmean(counties_sexratio['AbsDiffSexRatio'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
counties_sexratio = pd.merge(counties_sexratio, dfhdf.groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_TotalPopulation').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']), on ="GEOID", how="outer",validate=mergeValidation)
counties_sexratio['Total_PopSize'] = pd.cut(counties_sexratio['HDF_TotalPopulation'], [0,1000,5000,10000,50000,100000,np.inf], include_lowest=True, right=False)
for i in counties_sexratio['Total_PopSize'].cat.categories:
    ss = pd.DataFrame({'Geography':'County', 'Size_Category':str(i), 'Characteristic':'Average Absolute Change in Sex Ratio','NumCells':len(counties_sexratio[counties_sexratio['Total_PopSize'] == i]),'AvgAbsDiffSexRatio': np.nanmean(counties_sexratio.loc[counties_sexratio['Total_PopSize'] == i,'AbsDiffSexRatio'])}, index=[0])
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)


# Counties GQ Absolute Change in Median Age/Sex Ratio 
hdfcountiesgq_medage = dfhdf[dfhdf['GQTYPE'] > 0].groupby(['TABBLKST', 'TABBLKCOU'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='HDF_MedianAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
mdfcountiesgq_medage = dfmdf[dfmdf['GQTYPE'] > 0].groupby(['TABBLKST', 'TABBLKCOU'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='MDF_MedianAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index()
countiesgq_medage =  pd.merge(hdfcountiesgq_medage, mdfcountiesgq_medage, on='GEOID', how = 'outer', validate = mergeValidation)
countiesgq_medage  = countiesgq_medage.assign(AbsDiffMedAge = lambda x: np.abs(x['HDF_MedianAge'] - x['MDF_MedianAge']))
countiesgq_medage.to_csv(f"{OUTPUTDIR}/countiesgq_medage.csv", index=False)
ss = pd.DataFrame({'Geography':'County', 'Size_Category':'All', 'Characteristic':'Average Absolute Change in Median Age of GQ Population','NumCells':len(countiesgq_medage),'AvgAbsDiffMedAge': np.nanmean(countiesgq_medage['AbsDiffMedAge'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
countiesgq_medage = pd.merge(countiesgq_medage, dfhdf.groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_TotalPopulation').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']), on ="GEOID", how="outer",validate=mergeValidation)
countiesgq_medage['Total_PopSize'] = pd.cut(countiesgq_medage['HDF_TotalPopulation'], [0,1000,5000,10000,50000,100000,np.inf], include_lowest=True, right=False)
for i in countiesgq_medage['Total_PopSize'].cat.categories:
    ss = pd.DataFrame({'Geography':'County', 'Size_Category':str(i), 'Characteristic':'Average Absolute Change in Median Age of GQ Population','NumCells':len(countiesgq_medage[countiesgq_medage['Total_PopSize'] == i]),'AvgAbsDiffMedAge': np.nanmean(countiesgq_medage.loc[countiesgq_medage['Total_PopSize'] == i,'AbsDiffMedAge'])}, index=[0])
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

hdfcountiesgq_sexratio = dfhdf[dfhdf['GQTYPE'] > 0].groupby(['TABBLKST', 'TABBLKCOU', 'QSEX']).size().unstack().fillna(0).reset_index().assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index().assign(HDF_SexRatio = lambda x: 100*x['1']/x['2']).drop(columns=['1','2'])
mdfcountiesgq_sexratio = dfmdf[dfmdf['GQTYPE'] > 0].groupby(['TABBLKST', 'TABBLKCOU', 'QSEX']).size().unstack().fillna(0).reset_index().assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index(['GEOID']).reindex(allcountiesindex, fill_value=0).reset_index().assign(MDF_SexRatio = lambda x: 100*x['1']/x['2']).drop(columns=['1','2'])
countiesgq_sexratio =  pd.merge(hdfcountiesgq_sexratio, mdfcountiesgq_sexratio, on='GEOID', how = 'outer', validate = mergeValidation)
countiesgq_sexratio  = countiesgq_sexratio.assign(AbsDiffSexRatio = lambda x: np.abs(x['HDF_SexRatio'] - x['MDF_SexRatio']))
countiesgq_sexratio.to_csv(f"{OUTPUTDIR}/countiesgq_sexratio.csv", index=False)
ss = pd.DataFrame({'Geography':'County', 'Size_Category':'All', 'Characteristic':'Average Absolute Change in Sex Ratio of GQ Population','NumCells':len(countiesgq_sexratio),'AvgAbsDiffSexRatio': np.nanmean(countiesgq_sexratio['AbsDiffSexRatio'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
countiesgq_sexratio = pd.merge(countiesgq_sexratio, dfhdf.groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_TotalPopulation').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']), on ="GEOID", how="outer",validate=mergeValidation)
countiesgq_sexratio['Total_PopSize'] = pd.cut(countiesgq_sexratio['HDF_TotalPopulation'], [0,1000,5000,10000,50000,100000,np.inf], include_lowest=True, right=False)
for i in countiesgq_sexratio['Total_PopSize'].cat.categories:
    ss = pd.DataFrame({'Geography':'County', 'Size_Category':str(i), 'Characteristic':'Average Absolute Change in Sex Ratio of GQ Population','NumCells':len(countiesgq_sexratio[countiesgq_sexratio['Total_PopSize'] == i]),'AvgAbsDiffSexRatio': np.nanmean(countiesgq_sexratio.loc[countiesgq_sexratio['Total_PopSize'] == i,'AbsDiffSexRatio'])}, index=[0])
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

print("{} Average Absolute Change in Median Age and Sex Ratio Done".format(datetime.now()))


print("{} Starting Use Cases".format(datetime.now()))

# Tracts Aged 75+
hdftracts_over75 = dfhdf[dfhdf['QAGE']>= 75].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
mdftracts_over75 = dfmdf[dfmdf['QAGE']>= 75].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
tracts_over75 =  pd.merge(hdftracts_over75, mdftracts_over75, on='GEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
ss = tracts_over75.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "Aged 75 and Over")
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Counties and Places By State TAES
for s in allstates:
    dfhdfstate = dfhdf[dfhdf['TABBLKST'] == s]
    dfmdfstate = dfmdf[dfmdf['TABBLKST'] == s]
    hdfcounties_taes = dfhdfstate.groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
    mdfcounties_taes = dfmdfstate.groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
    hdfsize = len(dfhdfstate)
    mdfsize = len(dfmdfstate)
    counties_taes =  pd.merge(hdfcounties_taes, mdfcounties_taes, on='GEOID', how = 'outer', validate = mergeValidation)
    counties_taes = counties_taes.fillna({'HDF_Population': 0, 'MDF_Population': 0})
    counties_taes  = counties_taes.assign(AES = lambda x: np.abs(x['HDF_Population']/hdfsize - x['MDF_Population']/mdfsize))
    ss = pd.DataFrame({'Geography':'County', 'Size_Category':'All', 'Characteristic':'TAES Total Population {}'.format(statedict.get(s)),'NumCells':len(counties_taes),'TAES': np.sum(counties_taes['AES'])}, index=[0])
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    if s == "15":
        ss = pd.DataFrame({'Geography':'Place', 'Size_Category':'All', 'Characteristic':'TAES Total Population {}'.format(statedict.get(s)),'NumCells':0,'TAES': 0}, index=[0])
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    else:
        hdfplaces_taes = dfhdfstate.groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population')
        mdfplaces_taes = dfmdfstate.groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population')
        places_taes =  pd.merge(hdfplaces_taes, mdfplaces_taes, on='IncPlaceGEOID', how = 'outer', validate = mergeValidation)
        places_taes = places_taes.fillna({'HDF_Population': 0, 'MDF_Population': 0})
        places_taes  = places_taes.assign(AES = lambda x: np.abs(x['HDF_Population']/hdfsize - x['MDF_Population']/mdfsize))
        ss = pd.DataFrame({'Geography':'Place', 'Size_Category':'All', 'Characteristic':'TAES Total Population {}'.format(statedict.get(s)),'NumCells':len(places_taes),'TAES': np.sum(places_taes['AES'])}, index=[0])
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# MCDs By State TAES
for s in mcdstates:
    dfhdfstate = dfhdf[dfhdf['TABBLKST'] == s]
    dfmdfstate = dfmdf[dfmdf['TABBLKST'] == s]
    hdfmcds_taes = dfhdfstate.groupby(['MCDGEOID']).size().reset_index(name='HDF_Population')
    mdfmcds_taes = dfmdfstate.groupby(['MCDGEOID']).size().reset_index(name='MDF_Population')
    hdfsize = len(dfhdfstate)
    mdfsize = len(dfmdfstate)
    mcds_taes =  pd.merge(hdfmcds_taes, mdfmcds_taes, on='MCDGEOID', how = 'outer', validate = mergeValidation)
    mcds_taes = mcds_taes.fillna({'HDF_Population': 0, 'MDF_Population': 0})
    mcds_taes  = mcds_taes.assign(AES = lambda x: np.abs(x['HDF_Population']/hdfsize - x['MDF_Population']/mdfsize))
    ss = pd.DataFrame({'Geography':'MCD', 'Size_Category':'All', 'Characteristic':'TAES Total Population {}'.format(statedict.get(s)),'NumCells':len(mcds_taes),'TAES': np.sum(mcds_taes['AES'])}, index=[0])
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Counties Single Year of Age < 18  
for y in list(range(0,18)):
    hdfcounties_age = dfhdf[dfhdf['QAGE'] == y].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(allcountiesindex, fill_value=0).reset_index()
    mdfcounties_age = dfmdf[dfmdf['QAGE'] == y].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(allcountiesindex, fill_value=0).reset_index()
    counties_age =  pd.merge(hdfcounties_age, mdfcounties_age, on='GEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = counties_age.pipe(calculate_ss, geography="County", sizecategory = "All", characteristic = "Age {}".format(y))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    counties_age = pd.merge(counties_age, dfhdf[dfhdf['QAGE'] < 18].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_PopulationUnder18').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']), on="GEOID", how="outer", validate=mergeValidation)
    counties_age['Under18_PopSize'] = pd.cut(counties_age['HDF_PopulationUnder18'], [0,1000,10000,np.inf], include_lowest=True, right=False)
    for i in counties_age['Under18_PopSize'].cat.categories:
        ss = counties_age[counties_age['Under18_PopSize'] == i].pipe(calculate_ss, geography="County", sizecategory = str(i), characteristic = "Age {}".format(y))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Elem School Districts Single Year of Age < 18  
for y in list(range(0,18)):
    hdfelemschdists_age = dfhdf[dfhdf['QAGE'] == y].groupby(['SchDistEGEOID']).size().reset_index(name='HDF_Population').set_index('SchDistEGEOID').reindex(allelemschdistsindex, fill_value=0).reset_index()
    mdfelemschdists_age = dfmdf[dfmdf['QAGE'] == y].groupby(['SchDistEGEOID']).size().reset_index(name='MDF_Population').set_index('SchDistEGEOID').reindex(allelemschdistsindex, fill_value=0).reset_index()
    elemschdists_age =  pd.merge(hdfelemschdists_age, mdfelemschdists_age, on='SchDistEGEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = elemschdists_age.pipe(calculate_ss, geography="ESD", sizecategory = "All", characteristic = "Age {}".format(y))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    elemschdists_age = pd.merge(elemschdists_age, dfhdf[dfhdf['QAGE'] < 18].groupby(['SchDistEGEOID']).size().reset_index(name='HDF_PopulationUnder18').set_index('SchDistEGEOID').reindex(allelemschdistsindex, fill_value=0).reset_index(), on="SchDistEGEOID", how="outer", validate=mergeValidation)
    elemschdists_age['Under18_PopSize'] = pd.cut(elemschdists_age['HDF_PopulationUnder18'], [0,1000,10000,np.inf], include_lowest=True, right=False)
    for i in elemschdists_age['Under18_PopSize'].cat.categories:
        ss = elemschdists_age[elemschdists_age['Under18_PopSize'] == i].pipe(calculate_ss, geography="ESD", sizecategory = str(i), characteristic = "Age {}".format(y))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Sec School Districts Single Year of Age < 18  
for y in list(range(0,18)):
    hdfsecschdists_age = dfhdf[dfhdf['QAGE'] == y].groupby(['SchDistSGEOID']).size().reset_index(name='HDF_Population').set_index('SchDistSGEOID').reindex(allsecschdistsindex, fill_value=0).reset_index()
    mdfsecschdists_age = dfmdf[dfmdf['QAGE'] == y].groupby(['SchDistSGEOID']).size().reset_index(name='MDF_Population').set_index('SchDistSGEOID').reindex(allsecschdistsindex, fill_value=0).reset_index()
    secschdists_age =  pd.merge(hdfsecschdists_age, mdfsecschdists_age, on='SchDistSGEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = secschdists_age.pipe(calculate_ss, geography="SSD", sizecategory = "All", characteristic = "Age {}".format(y))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    secschdists_age = pd.merge(secschdists_age, dfhdf[dfhdf['QAGE'] < 18].groupby(['SchDistSGEOID']).size().reset_index(name='HDF_PopulationUnder18').set_index('SchDistSGEOID').reindex(allsecschdistsindex, fill_value=0).reset_index(), on="SchDistSGEOID", how="outer", validate=mergeValidation)
    secschdists_age['Under18_PopSize'] = pd.cut(secschdists_age['HDF_PopulationUnder18'], [0,1000,10000,np.inf], include_lowest=True, right=False)
    for i in secschdists_age['Under18_PopSize'].cat.categories:
        ss = secschdists_age[secschdists_age['Under18_PopSize'] == i].pipe(calculate_ss, geography="SSD", sizecategory = str(i), characteristic = "Age {}".format(y))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Uni School Districts Single Year of Age < 18  
for y in list(range(0,18)):
    hdfunischdists_age = dfhdf[dfhdf['QAGE'] == y].groupby(['SchDistUGEOID']).size().reset_index(name='HDF_Population').set_index('SchDistUGEOID').reindex(allunischdistsindex, fill_value=0).reset_index()
    mdfunischdists_age = dfmdf[dfmdf['QAGE'] == y].groupby(['SchDistUGEOID']).size().reset_index(name='MDF_Population').set_index('SchDistUGEOID').reindex(allunischdistsindex, fill_value=0).reset_index()
    unischdists_age =  pd.merge(hdfunischdists_age, mdfunischdists_age, on='SchDistUGEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
    ss = unischdists_age.pipe(calculate_ss, geography="USD", sizecategory = "All", characteristic = "Age {}".format(y))
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    unischdists_age = pd.merge(unischdists_age, dfhdf[dfhdf['QAGE'] < 18].groupby(['SchDistUGEOID']).size().reset_index(name='HDF_PopulationUnder18').set_index('SchDistUGEOID').reindex(allunischdistsindex, fill_value=0).reset_index(), on="SchDistUGEOID", how="outer", validate=mergeValidation)
    unischdists_age['Under18_PopSize'] = pd.cut(unischdists_age['HDF_PopulationUnder18'], [0,1000,10000,np.inf], include_lowest=True, right=False)
    for i in unischdists_age['Under18_PopSize'].cat.categories:
        ss = unischdists_age[unischdists_age['Under18_PopSize'] == i].pipe(calculate_ss, geography="USD", sizecategory = str(i), characteristic = "Age {}".format(y))
        outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Counties Nationwide AIAN Alone or In Combination TAES
hdfcounties_aianaloneorincomb_taes = dfhdf[dfhdf['aianalone-or-incomb'] == 1].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(allcountiesindex, fill_value=0).reset_index()
mdfcounties_aianaloneorincomb_taes = dfmdf[dfmdf['aianalone-or-incomb'] == 1].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(allcountiesindex, fill_value=0).reset_index()
hdfsize_aianaloneorincomb = len(dfhdf[dfhdf['aianalone-or-incomb'] == 1])
mdfsize_aianaloneorincomb = len(dfmdf[dfmdf['aianalone-or-incomb'] == 1])
counties_aianaloneorincomb_taes =  pd.merge(hdfcounties_aianaloneorincomb_taes, mdfcounties_aianaloneorincomb_taes, on='GEOID', how = 'outer', validate = mergeValidation)
counties_aianaloneorincomb_taes  = counties_aianaloneorincomb_taes.assign(AES = lambda x: np.abs(x['HDF_Population']/hdfsize_aianaloneorincomb - x['MDF_Population']/mdfsize_aianaloneorincomb))
ss = pd.DataFrame({'Geography':'County', 'Size_Category':'All', 'Characteristic':'TAES AIAN Alone or In Combination Nation','NumCells':len(counties_aianaloneorincomb_taes),'TAES': np.sum(counties_aianaloneorincomb_taes['AES'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Places Nationwide AIAN Alone or In Combination TAES
hdfplaces_aianaloneorincomb_taes = dfhdf[dfhdf['aianalone-or-incomb'] == 1].groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
mdfplaces_aianaloneorincomb_taes = dfmdf[dfmdf['aianalone-or-incomb'] == 1].groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
# hdfsize_aianaloneorincomb = len(dfhdf[dfhdf['aianalone-or-incomb'] == 1])
# mdfsize_aianaloneorincomb = len(dfmdf[dfmdf['aianalone-or-incomb'] == 1])
places_aianaloneorincomb_taes =  pd.merge(hdfplaces_aianaloneorincomb_taes, mdfplaces_aianaloneorincomb_taes, on='IncPlaceGEOID', how = 'outer', validate = mergeValidation)
places_aianaloneorincomb_taes  = places_aianaloneorincomb_taes.assign(AES = lambda x: np.abs(x['HDF_Population']/hdfsize_aianaloneorincomb - x['MDF_Population']/mdfsize_aianaloneorincomb))
ss = pd.DataFrame({'Geography':'Place', 'Size_Category':'All', 'Characteristic':'TAES AIAN Alone or In Combination Nation','NumCells':len(places_aianaloneorincomb_taes),'TAES': np.sum(places_aianaloneorincomb_taes['AES'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Counties AIAN Alone Count Where MDF < HDF
hdfcounties_aianalone = dfhdf[dfhdf['CENRACE'] == 3].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(allcountiesindex, fill_value=0).reset_index()
mdfcounties_aianalone = dfmdf[dfmdf['CENRACE'] == 3].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(allcountiesindex, fill_value=0).reset_index()
counties_aianalone =  pd.merge(hdfcounties_aianalone, mdfcounties_aianalone, on='GEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
counties_aianalone['MDFltHDF'] = np.where(counties_aianalone['MDF_Population']  < counties_aianalone['HDF_Population'], 1, 0)
ss = pd.DataFrame({'Geography':'County', 'Size_Category':'All', 'Characteristic':'AIAN Alone CountMDFltHDF', 'NumCells':len(counties_aianalone),'CountMDFltHDF': np.sum(counties_aianalone['MDFltHDF']), 'MedianPctDiffWhereMDFltHDF':np.nanmedian(counties_aianalone.loc[counties_aianalone['MDFltHDF'] == 1,'PercDiff'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
counties_aianalone['AIAN_PopSize'] = pd.cut(counties_aianalone['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False)
for i in counties_aianalone['AIAN_PopSize'].cat.categories:
    ss = pd.DataFrame({'Geography':'County', 'Size_Category':'AIAN Population Size {}'.format(i), 'Characteristic':'AIAN Alone CountMDFltHDF', 'NumCells':len(counties_aianalone[counties_aianalone['AIAN_PopSize'] == i]),'CountMDFltHDF': np.sum(counties_aianalone.loc[counties_aianalone['AIAN_PopSize'] == i, 'MDFltHDF']), 'MedianPctDiffWhereMDFltHDF':np.nanmedian(counties_aianalone.loc[(counties_aianalone['AIAN_PopSize'] == i)&(counties_aianalone['MDFltHDF'] == 1),'PercDiff'])}, index=[0])
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)


# Places AIAN Alone Count Where MDF < HDF
hdfplaces_aianalone = dfhdf[dfhdf['CENRACE'] == 3].groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index('IncPlaceGEOID').reindex(allplacesindex, fill_value=0).reset_index()
mdfplaces_aianalone = dfmdf[dfmdf['CENRACE'] == 3].groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index('IncPlaceGEOID').reindex(allplacesindex, fill_value=0).reset_index()
places_aianalone =  pd.merge(hdfplaces_aianalone, mdfplaces_aianalone, on='IncPlaceGEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
places_aianalone['MDFltHDF'] = np.where(places_aianalone['MDF_Population']  < places_aianalone['HDF_Population'], 1, 0)
ss = pd.DataFrame({'Geography':'Place', 'Size_Category':'All', 'Characteristic':'AIAN Alone CountMDFltHDF', 'NumCells':len(places_aianalone),'CountMDFltHDF': np.sum(places_aianalone['MDFltHDF']), 'MedianPctDiffWhereMDFltHDF':np.nanmedian(places_aianalone.loc[places_aianalone['MDFltHDF'] == 1,'PercDiff'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
places_aianalone['AIAN_PopSize'] = pd.cut(places_aianalone['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False)
for i in places_aianalone['AIAN_PopSize'].cat.categories:
    ss = pd.DataFrame({'Geography':'Place', 'Size_Category':'AIAN Population Size {}'.format(i), 'Characteristic':'AIAN Alone CountMDFltHDF', 'NumCells':len(places_aianalone[places_aianalone['AIAN_PopSize'] == i]),'CountMDFltHDF': np.sum(places_aianalone.loc[places_aianalone['AIAN_PopSize'] == i, 'MDFltHDF']), 'MedianPctDiffWhereMDFltHDF':np.nanmedian(places_aianalone.loc[(places_aianalone['AIAN_PopSize'] == i)&(places_aianalone['MDFltHDF'] == 1),'PercDiff'])}, index=[0])
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)


# Fed AIR AIAN Alone Count Where MDF < HDF
hdffedairs_aianalone = dfhdf[dfhdf['CENRACE'] == 3].groupby(['FedAIRGEOID']).size().reset_index(name='HDF_Population').set_index('FedAIRGEOID').reindex(allfedairsindex, fill_value=0).reset_index()
mdffedairs_aianalone = dfmdf[dfmdf['CENRACE'] == 3].groupby(['FedAIRGEOID']).size().reset_index(name='MDF_Population').set_index('FedAIRGEOID').reindex(allfedairsindex, fill_value=0).reset_index()
fedairs_aianalone =  pd.merge(hdffedairs_aianalone, mdffedairs_aianalone, on='FedAIRGEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
fedairs_aianalone['MDFltHDF'] = np.where(fedairs_aianalone['MDF_Population']  < fedairs_aianalone['HDF_Population'], 1, 0)
ss = pd.DataFrame({'Geography':'Fed AIR', 'Size_Category':'All', 'Characteristic':'AIAN Alone CountMDFltHDF', 'NumCells':len(fedairs_aianalone),'CountMDFltHDF': np.sum(fedairs_aianalone['MDFltHDF']), 'MedianPctDiffWhereMDFltHDF':np.nanmedian(fedairs_aianalone.loc[fedairs_aianalone['MDFltHDF'] == 1,'PercDiff'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
fedairs_aianalone['AIAN_PopSize'] = pd.cut(fedairs_aianalone['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False)
for i in fedairs_aianalone['AIAN_PopSize'].cat.categories:
    ss = pd.DataFrame({'Geography':'Fed AIR', 'Size_Category':'AIAN Population Size {}'.format(i), 'Characteristic':'AIAN Alone CountMDFltHDF', 'NumCells':len(fedairs_aianalone[fedairs_aianalone['AIAN_PopSize'] == i]),'CountMDFltHDF': np.sum(fedairs_aianalone.loc[fedairs_aianalone['AIAN_PopSize'] == i, 'MDFltHDF']), 'MedianPctDiffWhereMDFltHDF':np.nanmedian(fedairs_aianalone.loc[(fedairs_aianalone['AIAN_PopSize'] == i)&(fedairs_aianalone['MDFltHDF'] == 1),'PercDiff'])}, index=[0])
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)


# OTSA AIAN Alone Count Where MDF < HDF
hdfotsas_aianalone = dfhdf[dfhdf['CENRACE'] == 3].groupby(['OTSAGEOID']).size().reset_index(name='HDF_Population').set_index('OTSAGEOID').reindex(allotsasindex, fill_value=0).reset_index()
mdfotsas_aianalone = dfmdf[dfmdf['CENRACE'] == 3].groupby(['OTSAGEOID']).size().reset_index(name='MDF_Population').set_index('OTSAGEOID').reindex(allotsasindex, fill_value=0).reset_index()
otsas_aianalone =  pd.merge(hdfotsas_aianalone, mdfotsas_aianalone, on='OTSAGEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
otsas_aianalone['MDFltHDF'] = np.where(otsas_aianalone['MDF_Population']  < otsas_aianalone['HDF_Population'], 1, 0)
ss = pd.DataFrame({'Geography':'OTSA', 'Size_Category':'All', 'Characteristic':'AIAN Alone CountMDFltHDF', 'NumCells':len(otsas_aianalone),'CountMDFltHDF': np.sum(otsas_aianalone['MDFltHDF']), 'MedianPctDiffWhereMDFltHDF':np.nanmedian(otsas_aianalone.loc[otsas_aianalone['MDFltHDF'] == 1,'PercDiff'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
otsas_aianalone['AIAN_PopSize'] = pd.cut(otsas_aianalone['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False)
for i in otsas_aianalone['AIAN_PopSize'].cat.categories:
    ss = pd.DataFrame({'Geography':'OTSA', 'Size_Category':'AIAN Population Size {}'.format(i), 'Characteristic':'AIAN Alone CountMDFltHDF', 'NumCells':len(otsas_aianalone[otsas_aianalone['AIAN_PopSize'] == i]),'CountMDFltHDF': np.sum(otsas_aianalone.loc[otsas_aianalone['AIAN_PopSize'] == i, 'MDFltHDF']), 'MedianPctDiffWhereMDFltHDF':np.nanmedian(otsas_aianalone.loc[(otsas_aianalone['AIAN_PopSize'] == i)&(otsas_aianalone['MDFltHDF'] == 1),'PercDiff'])}, index=[0])
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# ANVSA AIAN Alone Count Where MDF < HDF
hdfanvsas_aianalone = dfhdf[dfhdf['CENRACE'] == 3].groupby(['ANVSAGEOID']).size().reset_index(name='HDF_Population').set_index('ANVSAGEOID').reindex(allanvsasindex, fill_value=0).reset_index()
mdfanvsas_aianalone = dfmdf[dfmdf['CENRACE'] == 3].groupby(['ANVSAGEOID']).size().reset_index(name='MDF_Population').set_index('ANVSAGEOID').reindex(allanvsasindex, fill_value=0).reset_index()
anvsas_aianalone =  pd.merge(hdfanvsas_aianalone, mdfanvsas_aianalone, on='ANVSAGEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
anvsas_aianalone['MDFltHDF'] = np.where(anvsas_aianalone['MDF_Population']  < anvsas_aianalone['HDF_Population'], 1, 0)
ss = pd.DataFrame({'Geography':'ANVSA', 'Size_Category':'All', 'Characteristic':'AIAN Alone CountMDFltHDF', 'NumCells':len(anvsas_aianalone),'CountMDFltHDF': np.sum(anvsas_aianalone['MDFltHDF']), 'MedianPctDiffWhereMDFltHDF':np.nanmedian(anvsas_aianalone.loc[anvsas_aianalone['MDFltHDF'] == 1,'PercDiff'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
anvsas_aianalone['AIAN_PopSize'] = pd.cut(anvsas_aianalone['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False)
for i in anvsas_aianalone['AIAN_PopSize'].cat.categories:
    ss = pd.DataFrame({'Geography':'ANVSA', 'Size_Category':'AIAN Population Size {}'.format(i), 'Characteristic':'AIAN Alone CountMDFltHDF', 'NumCells':len(anvsas_aianalone[anvsas_aianalone['AIAN_PopSize'] == i]),'CountMDFltHDF': np.sum(anvsas_aianalone.loc[anvsas_aianalone['AIAN_PopSize'] == i, 'MDFltHDF']), 'MedianPctDiffWhereMDFltHDF':np.nanmedian(anvsas_aianalone.loc[(anvsas_aianalone['AIAN_PopSize'] == i)&(anvsas_aianalone['MDFltHDF'] == 1),'PercDiff'])}, index=[0])
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Counties NHPI Alone Count Where MDF < HDF
hdfcounties_nhpialone = dfhdf[dfhdf['CENRACE'] == 5].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(allcountiesindex, fill_value=0).reset_index()
mdfcounties_nhpialone = dfmdf[dfmdf['CENRACE'] == 5].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(allcountiesindex, fill_value=0).reset_index()
counties_nhpialone =  pd.merge(hdfcounties_nhpialone, mdfcounties_nhpialone, on='GEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
counties_nhpialone['MDFltHDF'] = np.where(counties_nhpialone['MDF_Population']  < counties_nhpialone['HDF_Population'], 1, 0)
ss = pd.DataFrame({'Geography':'County', 'Size_Category':'All', 'Characteristic':'NHPI Alone CountMDFltHDF', 'NumCells':len(counties_nhpialone),'CountMDFltHDF': np.sum(counties_nhpialone['MDFltHDF']), 'MedianPctDiffWhereMDFltHDF':np.nanmedian(counties_nhpialone.loc[counties_nhpialone['MDFltHDF'] == 1,'PercDiff'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
counties_nhpialone['NHPI_PopSize'] = pd.cut(counties_nhpialone['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False)
for i in counties_nhpialone['NHPI_PopSize'].cat.categories:
    ss = pd.DataFrame({'Geography':'County', 'Size_Category':'NHPI Population Size {}'.format(i), 'Characteristic':'NHPI Alone CountMDFltHDF', 'NumCells':len(counties_nhpialone[counties_nhpialone['NHPI_PopSize'] == i]),'CountMDFltHDF': np.sum(counties_nhpialone.loc[counties_nhpialone['NHPI_PopSize'] == i, 'MDFltHDF']), 'MedianPctDiffWhereMDFltHDF':np.nanmedian(counties_nhpialone.loc[(counties_nhpialone['NHPI_PopSize'] == i)&(counties_nhpialone['MDFltHDF'] == 1),'PercDiff'])}, index=[0])
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Places NHPI Alone Count Where MDF < HDF
hdfplaces_nhpialone = dfhdf[dfhdf['CENRACE'] == 5].groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index('IncPlaceGEOID').reindex(allplacesindex, fill_value=0).reset_index()
mdfplaces_nhpialone = dfmdf[dfmdf['CENRACE'] == 5].groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index('IncPlaceGEOID').reindex(allplacesindex, fill_value=0).reset_index()
places_nhpialone =  pd.merge(hdfplaces_nhpialone, mdfplaces_nhpialone, on='IncPlaceGEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
places_nhpialone['MDFltHDF'] = np.where(places_nhpialone['MDF_Population']  < places_nhpialone['HDF_Population'], 1, 0)
ss = pd.DataFrame({'Geography':'Place', 'Size_Category':'All', 'Characteristic':'NHPI Alone CountMDFltHDF', 'NumCells':len(places_nhpialone),'CountMDFltHDF': np.sum(places_nhpialone['MDFltHDF']), 'MedianPctDiffWhereMDFltHDF':np.nanmedian(places_nhpialone.loc[places_nhpialone['MDFltHDF'] == 1,'PercDiff'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
places_nhpialone['NHPI_PopSize'] = pd.cut(places_nhpialone['HDF_Population'], [0,10,100,np.inf], include_lowest=True, right=False)
for i in places_nhpialone['NHPI_PopSize'].cat.categories:
    ss = pd.DataFrame({'Geography':'Place', 'Size_Category':'NHPI Population Size {}'.format(i), 'Characteristic':'NHPI Alone CountMDFltHDF', 'NumCells':len(places_nhpialone[places_nhpialone['NHPI_PopSize'] == i]),'CountMDFltHDF': np.sum(places_nhpialone.loc[places_nhpialone['NHPI_PopSize'] == i, 'MDFltHDF']), 'MedianPctDiffWhereMDFltHDF':np.nanmedian(places_nhpialone.loc[(places_nhpialone['NHPI_PopSize'] == i)&(places_nhpialone['MDFltHDF'] == 1),'PercDiff'])}, index=[0])
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Tracts AIAN Alone or in Combination 
hdftracts_aianincomb = dfhdf[(dfhdf['aianalone-or-incomb']==1)].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
mdftracts_aianincomb = dfmdf[(dfmdf['aianalone-or-incomb']==1)].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
tracts_aianincomb =  pd.merge(hdftracts_aianincomb, mdftracts_aianincomb, on='GEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
tracts_aianincomb['HundredPlusMDFLessThan20HDF'] = np.where((tracts_aianincomb['HDF_Population'] < 20) & (tracts_aianincomb['MDF_Population'] >=100), 1, 0)
tracts_aianincomb['LessThan20MDFHundredPlusHDF'] = np.where((tracts_aianincomb['HDF_Population'] >=100) & (tracts_aianincomb['MDF_Population'] < 20), 1, 0)
ss = tracts_aianincomb.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "AIAN Alone Or In Combination")
ss = ss.join(pd.DataFrame({'Number100PlusMDFLessThan20HDF':np.sum(tracts_aianincomb['HundredPlusMDFLessThan20HDF']), 'NumberLessThan20MDF100PlusHDF':np.sum(tracts_aianincomb['LessThan20MDFHundredPlusHDF'])}, index=[0]))
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Tracts NHPI
hdftracts_nhpialone = dfhdf[dfhdf['CENRACE'] == 5].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
mdftracts_nhpialone = dfmdf[dfmdf['CENRACE'] == 5].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
tracts_nhpialone =  pd.merge(hdftracts_nhpialone, mdftracts_nhpialone, on='GEOID', how = 'outer', validate = mergeValidation).pipe(calculate_stats)
tracts_nhpialone['HundredPlusMDFLessThan20HDF'] = np.where((tracts_nhpialone['HDF_Population'] < 20) & (tracts_nhpialone['MDF_Population'] >=100), 1, 0)
tracts_nhpialone['LessThan20MDFHundredPlusHDF'] = np.where((tracts_nhpialone['HDF_Population'] >=100) & (tracts_nhpialone['MDF_Population'] < 20), 1, 0)
ss = tracts_nhpialone.pipe(calculate_ss, geography="Tract", sizecategory = "All", characteristic = "NHPI Alone")
ss = ss.join(pd.DataFrame({'Number100PlusMDFLessThan20HDF':np.sum(tracts_nhpialone['HundredPlusMDFLessThan20HDF']), 'NumberLessThan20MDF100PlusHDF':np.sum(tracts_nhpialone['LessThan20MDFHundredPlusHDF'])}, index=[0]))
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Counties Total Population Cross 50000
hdfcounties_totalpop = dfhdf.groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
mdfcounties_totalpop = dfmdf.groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
counties_totalpop =  pd.merge(hdfcounties_totalpop, mdfcounties_totalpop, on='GEOID', how = 'outer', validate = mergeValidation)
counties_totalpop['HDFgt50kMDFlt50k'] = np.where((counties_totalpop['MDF_Population'] < 50000) & (counties_totalpop['HDF_Population'] > 50000), 1, 0)
counties_totalpop['HDFlt50kMDFgt50k'] = np.where((counties_totalpop['MDF_Population'] > 50000) & (counties_totalpop['HDF_Population'] < 50000), 1, 0)
ss = pd.DataFrame({'Geography':'County', 'Size_Category':'All', 'Characteristic':'Cross 50000','NumCells': len(counties_totalpop),'NumberHDFgt50kMDFlt50k': np.sum(counties_totalpop['HDFgt50kMDFlt50k']), 'NumberHDFlt50kMDFgt50k': np.sum(counties_totalpop['HDFlt50kMDFgt50k'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Places Total Population Cross 50000
hdfplaces_totalpop = dfhdf.groupby(['IncPlaceGEOID']).size().reset_index(name='HDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
mdfplaces_totalpop = dfmdf.groupby(['IncPlaceGEOID']).size().reset_index(name='MDF_Population').set_index(['IncPlaceGEOID']).reindex(allplacesindex, fill_value=0).reset_index()
places_totalpop =  pd.merge(hdfplaces_totalpop, mdfplaces_totalpop, on='IncPlaceGEOID', how = 'outer', validate = mergeValidation)
places_totalpop['HDFgt50kMDFlt50k'] = np.where((places_totalpop['MDF_Population'] < 50000) & (places_totalpop['HDF_Population'] > 50000), 1, 0)
places_totalpop['HDFlt50kMDFgt50k'] = np.where((places_totalpop['MDF_Population'] > 50000) & (places_totalpop['HDF_Population'] < 50000), 1, 0)
ss = pd.DataFrame({'Geography':'Place', 'Size_Category':'All', 'Characteristic':'Cross 50000','NumCells': len(places_totalpop),'NumberHDFgt50kMDFlt50k': np.sum(places_totalpop['HDFgt50kMDFlt50k']), 'NumberHDFlt50kMDFgt50k': np.sum(places_totalpop['HDFlt50kMDFgt50k'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Tracts Total Population Cross 50000
hdftracts_totalpop = dfhdf.groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
mdftracts_totalpop = dfmdf.groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
tracts_totalpop =  pd.merge(hdftracts_totalpop, mdftracts_totalpop, on='GEOID', how = 'outer', validate = mergeValidation)
tracts_totalpop['HDFgt50kMDFlt50k'] = np.where((tracts_totalpop['MDF_Population'] < 50000) & (tracts_totalpop['HDF_Population'] > 50000), 1, 0)
tracts_totalpop['HDFlt50kMDFgt50k'] = np.where((tracts_totalpop['MDF_Population'] > 50000) & (tracts_totalpop['HDF_Population'] < 50000), 1, 0)
ss = pd.DataFrame({'Geography':'Tract', 'Size_Category':'All', 'Characteristic':'Cross 50000','NumCells': len(tracts_totalpop),'NumberHDFgt50kMDFlt50k': np.sum(tracts_totalpop['HDFgt50kMDFlt50k']), 'NumberHDFlt50kMDFgt50k': np.sum(tracts_totalpop['HDFlt50kMDFgt50k'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)


print("{} Use Cases Done".format(datetime.now()))

print("{} Starting Improbable and Impossible Measurements".format(datetime.now()))

# States Total Population Should Be Equal
hdfstates_totalpop = dfhdf.groupby(['TABBLKST']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST'])
mdfstates_totalpop = dfmdf.groupby(['TABBLKST']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST).drop(columns = ['TABBLKST'])
states_totalpop =  pd.merge(hdfstates_totalpop, mdfstates_totalpop, on='GEOID', how = 'outer', validate = mergeValidation)
states_totalpop['HDFneMDF'] = np.where(states_totalpop['MDF_Population'] != states_totalpop['HDF_Population'], 1, 0)
ss = pd.DataFrame({'Geography':'State', 'Size_Category':'All', 'Characteristic':'Total Population','NumCells': len(states_totalpop),'NumberHDFneMDF': np.sum(states_totalpop['HDFneMDF'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Counties with at least 5 children under age 5 and no women age 18 through 44
hdfcounties_poplt5 = dfhdf[dfhdf['QAGE'] < 5].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Children').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
mdfcounties_poplt5 = dfmdf[dfmdf['QAGE'] < 5].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Children').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
hdfcounties_popfem1844 = dfhdf[(dfhdf['QSEX'] == '2')&(dfhdf['QAGE'] >= 18)&(dfhdf['QAGE'] < 45)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_MomAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
mdfcounties_popfem1844 = dfmdf[(dfmdf['QSEX'] == '2')&(dfmdf['QAGE'] >= 18)&(dfmdf['QAGE'] < 45)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_MomAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])

hdfcounties_poplt5 = hdfcounties_poplt5[hdfcounties_poplt5['HDF_Children'] >=5]
mdfcounties_poplt5 = mdfcounties_poplt5[mdfcounties_poplt5['MDF_Children'] >=5]

hdfcounties =  pd.merge(hdfcounties_poplt5, hdfcounties_popfem1844, on='GEOID', how = 'left', validate = mergeValidation)
mdfcounties =  pd.merge(mdfcounties_poplt5, mdfcounties_popfem1844, on='GEOID', how = 'left', validate = mergeValidation)

hdfcounties['ChildrenNoMoms'] = np.where((hdfcounties['HDF_Children'] >= 5)&(hdfcounties['HDF_MomAge'] == 0), 1, 0)
mdfcounties['ChildrenNoMoms'] = np.where((mdfcounties['MDF_Children'] >= 5)&(mdfcounties['MDF_MomAge'] == 0), 1, 0)

ss = pd.DataFrame({'Geography':'County', 'Size_Category':'All', 'Characteristic':'HDF Children No Moms','NumCells':len(hdfcounties), 'Inconsistent':np.sum(hdfcounties['ChildrenNoMoms'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
ss = pd.DataFrame({'Geography':'County', 'Size_Category':'All', 'Characteristic':'MDF Children No Moms','NumCells':len(mdfcounties), 'Inconsistent':np.sum(mdfcounties['ChildrenNoMoms'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Tracts with at least 5 children under age 5 and no women age 18 through 44
hdftracts_poplt5 = dfhdf[dfhdf['QAGE'] < 5].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_Children').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
mdftracts_poplt5 = dfmdf[dfmdf['QAGE'] < 5].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_Children').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
hdftracts_popfem1844 = dfhdf[(dfhdf['QSEX'] == '2')&(dfhdf['QAGE'] >= 18)&(dfhdf['QAGE'] < 45)].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_MomAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
mdftracts_popfem1844 = dfmdf[(dfmdf['QSEX'] == '2')&(dfmdf['QAGE'] >= 18)&(dfmdf['QAGE'] < 45)].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_MomAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()

hdftracts_poplt5 = hdftracts_poplt5[hdftracts_poplt5['HDF_Children'] >=5]
mdftracts_poplt5 = mdftracts_poplt5[mdftracts_poplt5['MDF_Children'] >=5]

hdftracts =  pd.merge(hdftracts_poplt5, hdftracts_popfem1844, on='GEOID', how = 'left', validate = mergeValidation)
mdftracts =  pd.merge(mdftracts_poplt5, mdftracts_popfem1844, on='GEOID', how = 'left', validate = mergeValidation)

hdftracts['ChildrenNoMoms'] = np.where((hdftracts['HDF_Children'] >= 5)&(hdftracts['HDF_MomAge'] == 0), 1, 0)
mdftracts['ChildrenNoMoms'] = np.where((mdftracts['MDF_Children'] >= 5)&(mdftracts['MDF_MomAge'] == 0), 1, 0)

ss = pd.DataFrame({'Geography':'Tract', 'Size_Category':'All', 'Characteristic':'HDF Children No Moms','NumCells':len(hdftracts), 'Inconsistent':np.sum(hdftracts['ChildrenNoMoms'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
ss = pd.DataFrame({'Geography':'Tract', 'Size_Category':'All', 'Characteristic':'MDF Children No Moms','NumCells':len(mdftracts), 'Inconsistent':np.sum(mdftracts['ChildrenNoMoms'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Counties with at least 5 children under age 5 and no women age 18 through 44 by race alone
for r in racealonecats:
    hdfcounties_poplt5 = dfhdf[(dfhdf['RACEALONE'] == r)&(dfhdf['QAGE'] < 5)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Children').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
    mdfcounties_poplt5 = dfmdf[(dfmdf['RACEALONE'] == r)&(dfmdf['QAGE'] < 5)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Children').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
    hdfcounties_popfem1844 = dfhdf[(dfhdf['RACEALONE'] == r)&(dfhdf['QSEX'] == '2')&(dfhdf['QAGE'] >= 18)&(dfhdf['QAGE'] < 45)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_MomAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
    mdfcounties_popfem1844 = dfmdf[(dfmdf['RACEALONE'] == r)&(dfmdf['QSEX'] == '2')&(dfmdf['QAGE'] >= 18)&(dfmdf['QAGE'] < 45)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_MomAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])

    hdfcounties_poplt5 = hdfcounties_poplt5[hdfcounties_poplt5['HDF_Children'] >=5]
    mdfcounties_poplt5 = mdfcounties_poplt5[mdfcounties_poplt5['MDF_Children'] >=5]

    hdfcounties =  pd.merge(hdfcounties_poplt5, hdfcounties_popfem1844, on='GEOID', how = 'left', validate = mergeValidation)
    mdfcounties =  pd.merge(mdfcounties_poplt5, mdfcounties_popfem1844, on='GEOID', how = 'left', validate = mergeValidation)

    hdfcounties['ChildrenNoMoms'] = np.where((hdfcounties['HDF_Children'] >= 5)&(hdfcounties['HDF_MomAge'] == 0), 1, 0)
    mdfcounties['ChildrenNoMoms'] = np.where((mdfcounties['MDF_Children'] >= 5)&(mdfcounties['MDF_MomAge'] == 0), 1, 0)

    ss = pd.DataFrame({'Geography':'County', 'Size_Category':'All', 'Characteristic':"HDF Children No Moms {race}".format(race = racealonedict.get(r)),'NumCells':len(hdfcounties), 'Inconsistent':np.sum(hdfcounties['ChildrenNoMoms'])}, index=[0])
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    ss = pd.DataFrame({'Geography':'County', 'Size_Category':'All', 'Characteristic':"MDF Children No Moms {race}".format(race = racealonedict.get(r)),'NumCells':len(mdfcounties), 'Inconsistent':np.sum(mdfcounties['ChildrenNoMoms'])}, index=[0])
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Counties with at least 5 children under age 5 and no women age 18 through 44 Hispanic
hdfcounties_poplt5 = dfhdf[(dfhdf['CENHISP'] == '2')&(dfhdf['QAGE'] < 5)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Children').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
mdfcounties_poplt5 = dfmdf[(dfmdf['CENHISP'] == '2')&(dfmdf['QAGE'] < 5)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Children').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
hdfcounties_popfem1844 = dfhdf[(dfhdf['CENHISP'] == '2')&(dfhdf['QSEX'] == '2')&(dfhdf['QAGE'] >= 18)&(dfhdf['QAGE'] < 45)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_MomAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
mdfcounties_popfem1844 = dfmdf[(dfmdf['CENHISP'] == '2')&(dfmdf['QSEX'] == '2')&(dfmdf['QAGE'] >= 18)&(dfmdf['QAGE'] < 45)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_MomAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])

hdfcounties_poplt5 = hdfcounties_poplt5[hdfcounties_poplt5['HDF_Children'] >=5]
mdfcounties_poplt5 = mdfcounties_poplt5[mdfcounties_poplt5['MDF_Children'] >=5]

hdfcounties =  pd.merge(hdfcounties_poplt5, hdfcounties_popfem1844, on='GEOID', how = 'left', validate = mergeValidation)
mdfcounties =  pd.merge(mdfcounties_poplt5, mdfcounties_popfem1844, on='GEOID', how = 'left', validate = mergeValidation)

hdfcounties['ChildrenNoMoms'] = np.where((hdfcounties['HDF_Children'] >= 5)&(hdfcounties['HDF_MomAge'] == 0), 1, 0)
mdfcounties['ChildrenNoMoms'] = np.where((mdfcounties['MDF_Children'] >= 5)&(mdfcounties['MDF_MomAge'] == 0), 1, 0)

ss = pd.DataFrame({'Geography':'County', 'Size_Category':'All', 'Characteristic':'HDF Children No Moms Hispanic','NumCells':len(hdfcounties), 'Inconsistent':np.sum(hdfcounties['ChildrenNoMoms'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
ss = pd.DataFrame({'Geography':'County', 'Size_Category':'All', 'Characteristic':'MDF Children No Moms Hispanic','NumCells':len(mdfcounties), 'Inconsistent':np.sum(mdfcounties['ChildrenNoMoms'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Counties with at least 5 children under age 5 and no women age 18 through 44 Non-Hispanic White
hdfcounties_poplt5 = dfhdf[(dfhdf['CENHISP'] == '1')&(dfhdf['RACEALONE'] == 1)&(dfhdf['QAGE'] < 5)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Children').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
mdfcounties_poplt5 = dfmdf[(dfmdf['CENHISP'] == '1')&(dfmdf['RACEALONE'] == 1)&(dfmdf['QAGE'] < 5)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Children').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
hdfcounties_popfem1844 = dfhdf[(dfhdf['CENHISP'] == '1')&(dfhdf['RACEALONE'] == 1)&(dfhdf['QSEX'] == '2')&(dfhdf['QAGE'] >= 18)&(dfhdf['QAGE'] < 45)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_MomAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
mdfcounties_popfem1844 = dfmdf[(dfmdf['CENHISP'] == '1')&(dfmdf['RACEALONE'] == 1)&(dfmdf['QSEX'] == '2')&(dfmdf['QAGE'] >= 18)&(dfmdf['QAGE'] < 45)].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_MomAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])

hdfcounties_poplt5 = hdfcounties_poplt5[hdfcounties_poplt5['HDF_Children'] >=5]
mdfcounties_poplt5 = mdfcounties_poplt5[mdfcounties_poplt5['MDF_Children'] >=5]

hdfcounties =  pd.merge(hdfcounties_poplt5, hdfcounties_popfem1844, on='GEOID', how = 'left', validate = mergeValidation)
mdfcounties =  pd.merge(mdfcounties_poplt5, mdfcounties_popfem1844, on='GEOID', how = 'left', validate = mergeValidation)

hdfcounties['ChildrenNoMoms'] = np.where((hdfcounties['HDF_Children'] >= 5)&(hdfcounties['HDF_MomAge'] == 0), 1, 0)
mdfcounties['ChildrenNoMoms'] = np.where((mdfcounties['MDF_Children'] >= 5)&(mdfcounties['MDF_MomAge'] == 0), 1, 0)

ss = pd.DataFrame({'Geography':'County', 'Size_Category':'All', 'Characteristic':'HDF Children No Moms Non-Hispanic White','NumCells':len(hdfcounties), 'Inconsistent':np.sum(hdfcounties['ChildrenNoMoms'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
ss = pd.DataFrame({'Geography':'County', 'Size_Category':'All', 'Characteristic':'MDF Children No Moms Non-Hispanic White','NumCells':len(mdfcounties), 'Inconsistent':np.sum(mdfcounties['ChildrenNoMoms'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Tracts with at least 5 children under age 5 and no women age 18 through 44 by race alone
for r in racealonecats:
    hdftracts_poplt5 = dfhdf[(dfhdf['RACEALONE'] == r)&(dfhdf['QAGE'] < 5)].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_Children').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
    mdftracts_poplt5 = dfmdf[(dfmdf['RACEALONE'] == r)&(dfmdf['QAGE'] < 5)].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_Children').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
    hdftracts_popfem1844 = dfhdf[(dfhdf['RACEALONE'] == r)&(dfhdf['QSEX'] == '2')&(dfhdf['QAGE'] >= 18)&(dfhdf['QAGE'] < 45)].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_MomAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
    mdftracts_popfem1844 = dfmdf[(dfmdf['RACEALONE'] == r)&(dfmdf['QSEX'] == '2')&(dfmdf['QAGE'] >= 18)&(dfmdf['QAGE'] < 45)].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_MomAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
    
    hdftracts_poplt5 = hdftracts_poplt5[hdftracts_poplt5['HDF_Children'] >=5]
    mdftracts_poplt5 = mdftracts_poplt5[mdftracts_poplt5['MDF_Children'] >=5]

    hdftracts =  pd.merge(hdftracts_poplt5, hdftracts_popfem1844, on='GEOID', how = 'left', validate = mergeValidation)
    mdftracts =  pd.merge(mdftracts_poplt5, mdftracts_popfem1844, on='GEOID', how = 'left', validate = mergeValidation)

    hdftracts['ChildrenNoMoms'] = np.where((hdftracts['HDF_Children'] >= 5)&(hdftracts['HDF_MomAge'] == 0), 1, 0)
    mdftracts['ChildrenNoMoms'] = np.where((mdftracts['MDF_Children'] >= 5)&(mdftracts['MDF_MomAge'] == 0), 1, 0)

    ss = pd.DataFrame({'Geography':'Tract', 'Size_Category':'All', 'Characteristic':"HDF Children No Moms {race}".format(race = racealonedict.get(r)),'NumCells':len(hdftracts), 'Inconsistent':np.sum(hdftracts['ChildrenNoMoms'])}, index=[0])
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    ss = pd.DataFrame({'Geography':'Tract', 'Size_Category':'All', 'Characteristic':"MDF Children No Moms {race}".format(race = racealonedict.get(r)),'NumCells':len(mdftracts), 'Inconsistent':np.sum(mdftracts['ChildrenNoMoms'])}, index=[0])
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Tracts with at least 5 children under age 5 and no women age 18 through 44 Hispanic
hdftracts_poplt5 = dfhdf[(dfhdf['CENHISP'] == '2')&(dfhdf['QAGE'] < 5)].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_Children').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
mdftracts_poplt5 = dfmdf[(dfmdf['CENHISP'] == '2')&(dfmdf['QAGE'] < 5)].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_Children').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
hdftracts_popfem1844 = dfhdf[(dfhdf['CENHISP'] == '2')&(dfhdf['QSEX'] == '2')&(dfhdf['QAGE'] >= 18)&(dfhdf['QAGE'] < 45)].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_MomAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
mdftracts_popfem1844 = dfmdf[(dfmdf['CENHISP'] == '2')&(dfmdf['QSEX'] == '2')&(dfmdf['QAGE'] >= 18)&(dfmdf['QAGE'] < 45)].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_MomAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()

hdftracts_poplt5 = hdftracts_poplt5[hdftracts_poplt5['HDF_Children'] >=5]
mdftracts_poplt5 = mdftracts_poplt5[mdftracts_poplt5['MDF_Children'] >=5]

hdftracts =  pd.merge(hdftracts_poplt5, hdftracts_popfem1844, on='GEOID', how = 'left', validate = mergeValidation)
mdftracts =  pd.merge(mdftracts_poplt5, mdftracts_popfem1844, on='GEOID', how = 'left', validate = mergeValidation)

hdftracts['ChildrenNoMoms'] = np.where((hdftracts['HDF_Children'] >= 5)&(hdftracts['HDF_MomAge'] == 0), 1, 0)
mdftracts['ChildrenNoMoms'] = np.where((mdftracts['MDF_Children'] >= 5)&(mdftracts['MDF_MomAge'] == 0), 1, 0)

ss = pd.DataFrame({'Geography':'Tract', 'Size_Category':'All', 'Characteristic':'HDF Children No Moms Hispanic','NumCells':len(hdftracts), 'Inconsistent':np.sum(hdftracts['ChildrenNoMoms'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
ss = pd.DataFrame({'Geography':'Tract', 'Size_Category':'All', 'Characteristic':'MDF Children No Moms Hispanic','NumCells':len(mdftracts), 'Inconsistent':np.sum(mdftracts['ChildrenNoMoms'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Tracts with at least 5 children under age 5 and no women age 18 through 44 Non-Hispanic White
hdftracts_poplt5 = dfhdf[(dfhdf['RACEALONE'] == 1)&(dfhdf['CENHISP'] == '1')&(dfhdf['QAGE'] < 5)].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_Children').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
mdftracts_poplt5 = dfmdf[(dfmdf['RACEALONE'] == 1)&(dfmdf['CENHISP'] == '1')&(dfmdf['QAGE'] < 5)].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_Children').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
hdftracts_popfem1844 = dfhdf[(dfhdf['RACEALONE'] == 1)&(dfhdf['CENHISP'] == '1')&(dfhdf['QSEX'] == '2')&(dfhdf['QAGE'] >= 18)&(dfhdf['QAGE'] < 45)].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_MomAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
mdftracts_popfem1844 = dfmdf[(dfmdf['RACEALONE'] == 1)&(dfmdf['CENHISP'] == '1')&(dfmdf['QSEX'] == '2')&(dfmdf['QAGE'] >= 18)&(dfmdf['QAGE'] < 45)].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_MomAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()

hdftracts_poplt5 = hdftracts_poplt5[hdftracts_poplt5['HDF_Children'] >=5]
mdftracts_poplt5 = mdftracts_poplt5[mdftracts_poplt5['MDF_Children'] >=5]

hdftracts =  pd.merge(hdftracts_poplt5, hdftracts_popfem1844, on='GEOID', how = 'left', validate = mergeValidation)
mdftracts =  pd.merge(mdftracts_poplt5, mdftracts_popfem1844, on='GEOID', how = 'left', validate = mergeValidation)

hdftracts['ChildrenNoMoms'] = np.where((hdftracts['HDF_Children'] >= 5)&(hdftracts['HDF_MomAge'] == 0), 1, 0)
mdftracts['ChildrenNoMoms'] = np.where((mdftracts['MDF_Children'] >= 5)&(mdftracts['MDF_MomAge'] == 0), 1, 0)

ss = pd.DataFrame({'Geography':'Tract', 'Size_Category':'All', 'Characteristic':'HDF Children No Moms Non-Hispanic White','NumCells':len(hdftracts), 'Inconsistent':np.sum(hdftracts['ChildrenNoMoms'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
ss = pd.DataFrame({'Geography':'Tract', 'Size_Category':'All', 'Characteristic':'MDF Children No Moms Non-Hispanic White','NumCells':len(mdftracts), 'Inconsistent':np.sum(mdftracts['ChildrenNoMoms'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Tracts with at least 5 people and all of the same sex
hdftracts_males = dfhdf[(dfhdf['QSEX'] == '1')].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_Males').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
mdftracts_males = dfmdf[(dfmdf['QSEX'] == '1')].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_Males').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
hdftracts_females = dfhdf[(dfhdf['QSEX'] == '2')].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_Females').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
mdftracts_females = dfmdf[(dfmdf['QSEX'] == '2')].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_Females').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()

hdftracts =  pd.merge(hdftracts_males, hdftracts_females, on='GEOID', how = 'outer', validate = mergeValidation)
mdftracts =  pd.merge(mdftracts_males, mdftracts_females, on='GEOID', how = 'outer', validate = mergeValidation)

hdftracts['Total'] = hdftracts['HDF_Females'] + hdftracts['HDF_Males']
mdftracts['Total'] = mdftracts['MDF_Females'] + mdftracts['MDF_Males']

hdftracts = hdftracts[hdftracts['Total'] >=5]
mdftracts = mdftracts[mdftracts['Total'] >=5]

hdftracts['AllSameSex'] = np.where((hdftracts['HDF_Males'] == 0)|(hdftracts['HDF_Females'] == 0), 1, 0)
mdftracts['AllSameSex'] = np.where((mdftracts['MDF_Males'] == 0)|(mdftracts['MDF_Females'] == 0), 1, 0)

ss = pd.DataFrame({'Geography':'Tract', 'Size_Category':'All', 'Characteristic':'HDF All Same Sex','NumCells':len(hdftracts), 'Inconsistent':np.sum(hdftracts['AllSameSex'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
ss = pd.DataFrame({'Geography':'Tract', 'Size_Category':'All', 'Characteristic':'MDF All Same Sex','NumCells':len(mdftracts), 'Inconsistent':np.sum(mdftracts['AllSameSex'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Tracts with at least one of the single years of age between 0 and 17 by sex has a zero count
hdftracts_totunder18 = dfhdf[(dfhdf['QAGE'] < 18)].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='HDF_TotUnder18').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()
mdftracts_totunder18 = dfmdf[(dfmdf['QAGE'] < 18)].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE']).size().reset_index(name='MDF_TotUnder18').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(alltractsindex, fill_value=0).reset_index()

hdftracts_totunder18gt200 = hdftracts_totunder18.loc[hdftracts_totunder18['HDF_TotUnder18'] > 200,'GEOID'].tolist()
mdftracts_totunder18gt200 = mdftracts_totunder18.loc[mdftracts_totunder18['MDF_TotUnder18'] > 200,'GEOID'].tolist()

hdftract_1yageunder18_index = pd.MultiIndex.from_product([hdftracts_totunder18gt200, list(range(0,18))], names = ['GEOID','QAGE'])
mdftract_1yageunder18_index = pd.MultiIndex.from_product([mdftracts_totunder18gt200, list(range(0,18))], names = ['GEOID','QAGE'])

hdftracts_under18 = dfhdf[(dfhdf['QAGE'] < 18)].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE','QAGE']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID','QAGE']).reindex(hdftract_1yageunder18_index, fill_value=0).reset_index()
mdftracts_under18 = dfmdf[(dfmdf['QAGE'] < 18)].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE','QAGE']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index(['GEOID','QAGE']).reindex(mdftract_1yageunder18_index, fill_value=0).reset_index()

hdftracts_under18['ZeroAge'] = np.where((hdftracts_under18['HDF_Population'] == 0), 1, 0)
mdftracts_under18['ZeroAge'] = np.where((mdftracts_under18['MDF_Population'] == 0), 1, 0)

hdftracts_anyzeros = hdftracts_under18.groupby('GEOID')['ZeroAge'].max()
mdftracts_anyzeros = mdftracts_under18.groupby('GEOID')['ZeroAge'].max()

ss = pd.DataFrame({'Geography':'Tract', 'Size_Category':'All', 'Characteristic':'HDF Zero Age','NumCells':len(hdftracts_anyzeros), 'Inconsistent':np.sum(hdftracts_anyzeros)}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
ss = pd.DataFrame({'Geography':'Tract', 'Size_Category':'All', 'Characteristic':'MDF Zero Age','NumCells':len(mdftracts_anyzeros), 'Inconsistent':np.sum(mdftracts_anyzeros)}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

hdftracts_under18.to_csv(f"{OUTPUTDIR}/hdftracts_under18.csv", index=False)
hdftracts_anyzeros.to_csv(f"{OUTPUTDIR}/hdftracts_under18anyzeros.csv", index=False)

# Blocks with population all 17 or younger 
hdfblocks_gqpop = dfhdf[dfhdf['RTYPE']=="5"].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE', 'TABBLK']).size().reset_index(name='HDF_GQPopulation').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE + x.TABBLK).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLK']).set_index('GEOID').reindex(allblocksindex, fill_value=0).reset_index()
mdfblocks_gqpop = dfmdf[dfmdf['RTYPE']=="5"].groupby(['TABBLKST','TABBLKCOU','TABTRACTCE', 'TABBLK']).size().reset_index(name='MDF_GQPopulation').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE + x.TABBLK).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLK']).set_index('GEOID').reindex(allblocksindex, fill_value=0).reset_index()

hdfblocks_nogqs = hdfblocks_gqpop.loc[hdfblocks_gqpop['HDF_GQPopulation'] == 0,'GEOID'].tolist()
mdfblocks_nogqs = mdfblocks_gqpop.loc[mdfblocks_gqpop['MDF_GQPopulation'] == 0,'GEOID'].tolist()

del mdfblocks_gqpop
del hdfblocks_gqpop

hdfblocks_allpop = dfhdf.groupby(['TABBLKST','TABBLKCOU','TABTRACTCE', 'TABBLK']).size().reset_index(name='HDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE + x.TABBLK).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLK'])
mdfblocks_allpop = dfmdf.groupby(['TABBLKST','TABBLKCOU','TABTRACTCE', 'TABBLK']).size().reset_index(name='MDF_Population').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE + x.TABBLK).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLK'])

hdfblocks_somepop = hdfblocks_allpop.loc[hdfblocks_allpop['HDF_Population'] > 0,'GEOID'].tolist()
mdfblocks_somepop = mdfblocks_allpop.loc[mdfblocks_allpop['MDF_Population'] > 0,'GEOID'].tolist()

del hdfblocks_allpop
del mdfblocks_allpop

hdfblocks_nogqs_somepop = set(hdfblocks_nogqs).intersection(hdfblocks_somepop)
mdfblocks_nogqs_somepop = set(mdfblocks_nogqs).intersection(mdfblocks_somepop)

hdfblocks_nogqs_index = pd.Index(hdfblocks_nogqs_somepop, name='BlockGEOID')
mdfblocks_nogqs_index = pd.Index(mdfblocks_nogqs_somepop, name='BlockGEOID')

del hdfblocks_nogqs
del mdfblocks_nogqs
del hdfblocks_somepop
del mdfblocks_somepop

print("HDF Blocks with >0 Population and No GQ Population", len(hdfblocks_nogqs_somepop))
print("MDF Blocks with >0 Population and No GQ Population", len(mdfblocks_nogqs_somepop))

del hdfblocks_nogqs_somepop
del mdfblocks_nogqs_somepop

hdfblocks_totpop = dfhdf.groupby(['BlockGEOID']).size().reset_index(name='HDF_Population').set_index('BlockGEOID').reindex(hdfblocks_nogqs_index, fill_value=0).reset_index()
mdfblocks_totpop = dfmdf.groupby(['BlockGEOID']).size().reset_index(name='MDF_Population').set_index('BlockGEOID').reindex(mdfblocks_nogqs_index, fill_value=0).reset_index()

hdfblocks_18 = dfhdf[dfhdf['QAGE'] < 18].groupby(['BlockGEOID']).size().reset_index(name='HDF_Under18').set_index('BlockGEOID').reindex(hdfblocks_nogqs_index, fill_value=0).reset_index()
mdfblocks_18 = dfmdf[dfmdf['QAGE'] < 18].groupby(['BlockGEOID']).size().reset_index(name='MDF_Under18').set_index('BlockGEOID').reindex(mdfblocks_nogqs_index, fill_value=0).reset_index()

hdfblocks =  pd.merge(hdfblocks_totpop, hdfblocks_18, on='BlockGEOID', how = 'inner', validate = mergeValidation)
mdfblocks =  pd.merge(mdfblocks_totpop, mdfblocks_18, on='BlockGEOID', how = 'inner', validate = mergeValidation)

del hdfblocks_18
del mdfblocks_18
del hdfblocks_totpop
del mdfblocks_totpop

hdfblocks['Zero18andOver'] = np.where((hdfblocks['HDF_Population'] > 0)&(hdfblocks['HDF_Under18'] == hdfblocks['HDF_Population']), 1, 0)
mdfblocks['Zero18andOver'] = np.where((mdfblocks['MDF_Population'] > 0)&(mdfblocks['MDF_Under18'] == mdfblocks['MDF_Population']), 1, 0)

ss = pd.DataFrame({'Geography':'Block', 'Size_Category':'All', 'Characteristic':'HDF Everyone Under 18','NumCells':len(hdfblocks), 'Inconsistent':np.sum(hdfblocks['Zero18andOver'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
ss = pd.DataFrame({'Geography':'Block', 'Size_Category':'All', 'Characteristic':'MDF Everyone Under 18','NumCells':len(mdfblocks), 'Inconsistent':np.sum(mdfblocks['Zero18andOver'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

del hdfblocks
del mdfblocks
del hdfblocks_nogqs_index
del mdfblocks_nogqs_index

# Counties where median age of the men is significantly different (equal to or greater than 20 years) from the median age of women
hdfcounties_males = dfhdf[(dfhdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Males').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
mdfcounties_males = dfmdf[(dfmdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Males').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
hdfcounties_females = dfhdf[(dfhdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Females').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
mdfcounties_females = dfmdf[(dfmdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Females').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])

hdfcounties_gt5males = hdfcounties_males.loc[hdfcounties_males['HDF_Males'] >=5, 'GEOID'].tolist()
mdfcounties_gt5males = mdfcounties_males.loc[mdfcounties_males['MDF_Males'] >=5, 'GEOID'].tolist()
hdfcounties_gt5females = hdfcounties_females.loc[hdfcounties_females['HDF_Females'] >=5, 'GEOID'].tolist()
mdfcounties_gt5females = mdfcounties_females.loc[mdfcounties_females['MDF_Females'] >=5, 'GEOID'].tolist()

hdfcounties_gt5bothsex =  list(set(hdfcounties_gt5males).intersection(hdfcounties_gt5females))
mdfcounties_gt5bothsex =  list(set(mdfcounties_gt5males).intersection(mdfcounties_gt5females))

hdfcounties_gt5bothsex_index = pd.Index(hdfcounties_gt5bothsex, name='GEOID')
mdfcounties_gt5bothsex_index = pd.Index(mdfcounties_gt5bothsex, name='GEOID')

hdfcounties_malemedage = dfhdf[(dfhdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='HDF_MaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(hdfcounties_gt5bothsex_index).reset_index()
mdfcounties_malemedage = dfmdf[(dfmdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='MDF_MaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(mdfcounties_gt5bothsex_index).reset_index()
hdfcounties_femalemedage = dfhdf[(dfhdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='HDF_FemaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(hdfcounties_gt5bothsex_index).reset_index()
mdfcounties_femalemedage = dfmdf[(dfmdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='MDF_FemaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(mdfcounties_gt5bothsex_index).reset_index()

hdfcounties =  pd.merge(hdfcounties_malemedage, hdfcounties_femalemedage, on='GEOID', how = 'inner', validate = mergeValidation)
mdfcounties =  pd.merge(mdfcounties_malemedage, mdfcounties_femalemedage, on='GEOID', how = 'inner', validate = mergeValidation)

hdfcounties['BigAgeDiff'] = np.where(np.abs(hdfcounties['HDF_MaleAge'] - hdfcounties['HDF_FemaleAge']) >= 20, 1, 0)
mdfcounties['BigAgeDiff'] = np.where(np.abs(mdfcounties['MDF_MaleAge'] - mdfcounties['MDF_FemaleAge']) >= 20, 1, 0)

ss = pd.DataFrame({'Geography':'County', 'Size_Category':'All', 'Characteristic':'HDF 20+ Year Median Age Diff','NumCells':len(hdfcounties), 'Inconsistent':np.sum(hdfcounties['BigAgeDiff'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
ss = pd.DataFrame({'Geography':'County', 'Size_Category':'All', 'Characteristic':'MDF 20+ Year Median Age Diff','NumCells':len(mdfcounties), 'Inconsistent':np.sum(mdfcounties['BigAgeDiff'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Counties where median age of the men is significantly different (equal to or greater than 20 years) from the median age of women, by major race group
for r in racealonecats:
    hdfcounties_males = dfhdf[(dfhdf['RACEALONE'] == r)&(dfhdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Males').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
    mdfcounties_males = dfmdf[(dfmdf['RACEALONE'] == r)&(dfmdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Males').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
    hdfcounties_females = dfhdf[(dfhdf['RACEALONE'] == r)&(dfhdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Females').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
    mdfcounties_females = dfmdf[(dfmdf['RACEALONE'] == r)&(dfmdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Females').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])

    hdfcounties_gt5males = hdfcounties_males.loc[hdfcounties_males['HDF_Males'] >=5, 'GEOID'].tolist()
    mdfcounties_gt5males = mdfcounties_males.loc[mdfcounties_males['MDF_Males'] >=5, 'GEOID'].tolist()
    hdfcounties_gt5females = hdfcounties_females.loc[hdfcounties_females['HDF_Females'] >=5, 'GEOID'].tolist()
    mdfcounties_gt5females = mdfcounties_females.loc[mdfcounties_females['MDF_Females'] >=5, 'GEOID'].tolist()

    hdfcounties_gt5bothsex =  list(set(hdfcounties_gt5males).intersection(hdfcounties_gt5females))
    mdfcounties_gt5bothsex =  list(set(mdfcounties_gt5males).intersection(mdfcounties_gt5females))

    hdfcounties_gt5bothsex_index = pd.Index(hdfcounties_gt5bothsex, name='GEOID')
    mdfcounties_gt5bothsex_index = pd.Index(mdfcounties_gt5bothsex, name='GEOID')

    hdfcounties_malemedage = dfhdf[(dfhdf['RACEALONE'] == r)&(dfhdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='HDF_MaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(hdfcounties_gt5bothsex_index).reset_index()
    mdfcounties_malemedage = dfmdf[(dfmdf['RACEALONE'] == r)&(dfmdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='MDF_MaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(mdfcounties_gt5bothsex_index).reset_index()
    hdfcounties_femalemedage = dfhdf[(dfhdf['RACEALONE'] == r)&(dfhdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='HDF_FemaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(hdfcounties_gt5bothsex_index).reset_index()
    mdfcounties_femalemedage = dfmdf[(dfmdf['RACEALONE'] == r)&(dfmdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='MDF_FemaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(mdfcounties_gt5bothsex_index).reset_index()

    hdfcounties =  pd.merge(hdfcounties_malemedage, hdfcounties_femalemedage, on='GEOID', how = 'inner', validate = mergeValidation)
    mdfcounties =  pd.merge(mdfcounties_malemedage, mdfcounties_femalemedage, on='GEOID', how = 'inner', validate = mergeValidation)

    hdfcounties['BigAgeDiff'] = np.where(np.abs(hdfcounties['HDF_MaleAge'] - hdfcounties['HDF_FemaleAge']) >= 20, 1, 0)
    mdfcounties['BigAgeDiff'] = np.where(np.abs(mdfcounties['MDF_MaleAge'] - mdfcounties['MDF_FemaleAge']) >= 20, 1, 0)

    ss = pd.DataFrame({'Geography':'County', 'Size_Category':'All', 'Characteristic':"HDF 20+ Year Median Age Diff {race}".format(race = racealonedict.get(r)),'NumCells':len(hdfcounties), 'Inconsistent':np.sum(hdfcounties['BigAgeDiff'])}, index=[0])
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    ss = pd.DataFrame({'Geography':'County', 'Size_Category':'All', 'Characteristic':"MDF 20+ Year Median Age Diff {race}".format(race = racealonedict.get(r)),'NumCells':len(mdfcounties), 'Inconsistent':np.sum(mdfcounties['BigAgeDiff'])}, index=[0])
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Counties where median age of the men is significantly different (equal to or greater than 20 years) from the median age of women Hispanic
hdfcounties_males = dfhdf[(dfhdf['CENHISP'] == '2')&(dfhdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Males').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
mdfcounties_males = dfmdf[(dfmdf['CENHISP'] == '2')&(dfmdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Males').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
hdfcounties_females = dfhdf[(dfhdf['CENHISP'] == '2')&(dfhdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Females').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
mdfcounties_females = dfmdf[(dfmdf['CENHISP'] == '2')&(dfmdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Females').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])

hdfcounties_gt5males = hdfcounties_males.loc[hdfcounties_males['HDF_Males'] >=5, 'GEOID'].tolist()
mdfcounties_gt5males = mdfcounties_males.loc[mdfcounties_males['MDF_Males'] >=5, 'GEOID'].tolist()
hdfcounties_gt5females = hdfcounties_females.loc[hdfcounties_females['HDF_Females'] >=5, 'GEOID'].tolist()
mdfcounties_gt5females = mdfcounties_females.loc[mdfcounties_females['MDF_Females'] >=5, 'GEOID'].tolist()

hdfcounties_gt5bothsex =  list(set(hdfcounties_gt5males).intersection(hdfcounties_gt5females))
mdfcounties_gt5bothsex =  list(set(mdfcounties_gt5males).intersection(mdfcounties_gt5females))

hdfcounties_gt5bothsex_index = pd.Index(hdfcounties_gt5bothsex, name='GEOID')
mdfcounties_gt5bothsex_index = pd.Index(mdfcounties_gt5bothsex, name='GEOID')

hdfcounties_malemedage = dfhdf[(dfhdf['CENHISP'] == '2')&(dfhdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='HDF_MaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(hdfcounties_gt5bothsex_index).reset_index()
mdfcounties_malemedage = dfmdf[(dfmdf['CENHISP'] == '2')&(dfmdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='MDF_MaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(mdfcounties_gt5bothsex_index).reset_index()
hdfcounties_femalemedage = dfhdf[(dfhdf['CENHISP'] == '2')&(dfhdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='HDF_FemaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(hdfcounties_gt5bothsex_index).reset_index()
mdfcounties_femalemedage = dfmdf[(dfmdf['CENHISP'] == '2')&(dfmdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='MDF_FemaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(mdfcounties_gt5bothsex_index).reset_index()

hdfcounties =  pd.merge(hdfcounties_malemedage, hdfcounties_femalemedage, on='GEOID', how = 'inner', validate = mergeValidation)
mdfcounties =  pd.merge(mdfcounties_malemedage, mdfcounties_femalemedage, on='GEOID', how = 'inner', validate = mergeValidation)

hdfcounties['BigAgeDiff'] = np.where(np.abs(hdfcounties['HDF_MaleAge'] - hdfcounties['HDF_FemaleAge']) >= 20, 1, 0)
mdfcounties['BigAgeDiff'] = np.where(np.abs(mdfcounties['MDF_MaleAge'] - mdfcounties['MDF_FemaleAge']) >= 20, 1, 0)

ss = pd.DataFrame({'Geography':'County', 'Size_Category':'All', 'Characteristic':"HDF 20+ Year Median Age Diff Hispanic",'NumCells':len(hdfcounties), 'Inconsistent':np.sum(hdfcounties['BigAgeDiff'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
ss = pd.DataFrame({'Geography':'County', 'Size_Category':'All', 'Characteristic':"MDF 20+ Year Median Age Diff Hispanic",'NumCells':len(mdfcounties), 'Inconsistent':np.sum(mdfcounties['BigAgeDiff'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Counties where median age of the men is significantly different (equal to or greater than 20 years) from the median age of women, Non-Hispanic White
hdfcounties_males = dfhdf[(dfhdf['RACEALONE'] == 1)&(dfhdf['CENHISP'] == '1')&(dfhdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Males').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
mdfcounties_males = dfmdf[(dfmdf['RACEALONE'] == 1)&(dfmdf['CENHISP'] == '1')&(dfmdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Males').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
hdfcounties_females = dfhdf[(dfhdf['RACEALONE'] == 1)&(dfhdf['CENHISP'] == '1')&(dfhdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='HDF_Females').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])
mdfcounties_females = dfmdf[(dfmdf['RACEALONE'] == 1)&(dfmdf['CENHISP'] == '1')&(dfmdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU']).size().reset_index(name='MDF_Females').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU'])

hdfcounties_gt5males = hdfcounties_males.loc[hdfcounties_males['HDF_Males'] >=5, 'GEOID'].tolist()
mdfcounties_gt5males = mdfcounties_males.loc[mdfcounties_males['MDF_Males'] >=5, 'GEOID'].tolist()
hdfcounties_gt5females = hdfcounties_females.loc[hdfcounties_females['HDF_Females'] >=5, 'GEOID'].tolist()
mdfcounties_gt5females = mdfcounties_females.loc[mdfcounties_females['MDF_Females'] >=5, 'GEOID'].tolist()

hdfcounties_gt5bothsex =  list(set(hdfcounties_gt5males).intersection(hdfcounties_gt5females))
mdfcounties_gt5bothsex =  list(set(mdfcounties_gt5males).intersection(mdfcounties_gt5females))

hdfcounties_gt5bothsex_index = pd.Index(hdfcounties_gt5bothsex, name='GEOID')
mdfcounties_gt5bothsex_index = pd.Index(mdfcounties_gt5bothsex, name='GEOID')

hdfcounties_malemedage = dfhdf[(dfhdf['RACEALONE'] == 1)&(dfhdf['CENHISP'] == '1')&(dfhdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='HDF_MaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(hdfcounties_gt5bothsex_index).reset_index()
mdfcounties_malemedage = dfmdf[(dfmdf['RACEALONE'] == 1)&(dfmdf['CENHISP'] == '1')&(dfmdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='MDF_MaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(mdfcounties_gt5bothsex_index).reset_index()
hdfcounties_femalemedage = dfhdf[(dfhdf['RACEALONE'] == 1)&(dfhdf['CENHISP'] == '1')&(dfhdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='HDF_FemaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(hdfcounties_gt5bothsex_index).reset_index()
mdfcounties_femalemedage = dfmdf[(dfmdf['RACEALONE'] == 1)&(dfmdf['CENHISP'] == '1')&(dfmdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='MDF_FemaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU).drop(columns = ['TABBLKST', 'TABBLKCOU']).set_index('GEOID').reindex(mdfcounties_gt5bothsex_index).reset_index()

hdfcounties =  pd.merge(hdfcounties_malemedage, hdfcounties_femalemedage, on='GEOID', how = 'inner', validate = mergeValidation)
mdfcounties =  pd.merge(mdfcounties_malemedage, mdfcounties_femalemedage, on='GEOID', how = 'inner', validate = mergeValidation)

hdfcounties['BigAgeDiff'] = np.where(np.abs(hdfcounties['HDF_MaleAge'] - hdfcounties['HDF_FemaleAge']) >= 20, 1, 0)
mdfcounties['BigAgeDiff'] = np.where(np.abs(mdfcounties['MDF_MaleAge'] - mdfcounties['MDF_FemaleAge']) >= 20, 1, 0)

ss = pd.DataFrame({'Geography':'County', 'Size_Category':'All', 'Characteristic':"HDF 20+ Year Median Age Diff Non-Hispanic White",'NumCells':len(hdfcounties), 'Inconsistent':np.sum(hdfcounties['BigAgeDiff'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
ss = pd.DataFrame({'Geography':'County', 'Size_Category':'All', 'Characteristic':"MDF 20+ Year Median Age Diff Non-Hispanic White",'NumCells':len(mdfcounties), 'Inconsistent':np.sum(mdfcounties['BigAgeDiff'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Tracts where median age of the men is significantly different (equal to or greater than 20 years) from the median age of women
hdftracts_males = dfhdf[(dfhdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Males').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU','TABTRACTCE'])
mdftracts_males = dfmdf[(dfmdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Males').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU','TABTRACTCE'])
hdftracts_females = dfhdf[(dfhdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Females').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU','TABTRACTCE'])
mdftracts_females = dfmdf[(dfmdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Females').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU','TABTRACTCE'])

hdftracts_gt5males = hdftracts_males.loc[hdftracts_males['HDF_Males'] >=5, 'GEOID'].tolist()
mdftracts_gt5males = mdftracts_males.loc[mdftracts_males['MDF_Males'] >=5, 'GEOID'].tolist()
hdftracts_gt5females = hdftracts_females.loc[hdftracts_females['HDF_Females'] >=5, 'GEOID'].tolist()
mdftracts_gt5females = mdftracts_females.loc[mdftracts_females['MDF_Females'] >=5, 'GEOID'].tolist()

hdftracts_gt5bothsex =  list(set(hdftracts_gt5males).intersection(hdftracts_gt5females))
mdftracts_gt5bothsex =  list(set(mdftracts_gt5males).intersection(mdftracts_gt5females))

hdftracts_gt5bothsex_index = pd.Index(hdftracts_gt5bothsex, name='GEOID')
mdftracts_gt5bothsex_index = pd.Index(mdftracts_gt5bothsex, name='GEOID')

hdftracts_malemedage = dfhdf[(dfhdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='HDF_MaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(hdftracts_gt5bothsex_index).reset_index()
mdftracts_malemedage = dfmdf[(dfmdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='MDF_MaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(mdftracts_gt5bothsex_index).reset_index()
hdftracts_femalemedage = dfhdf[(dfhdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='HDF_FemaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(hdftracts_gt5bothsex_index).reset_index()
mdftracts_femalemedage = dfmdf[(dfmdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='MDF_FemaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(mdftracts_gt5bothsex_index).reset_index()

hdftracts =  pd.merge(hdftracts_malemedage, hdftracts_femalemedage, on='GEOID', how = 'inner', validate = mergeValidation)
mdftracts =  pd.merge(mdftracts_malemedage, mdftracts_femalemedage, on='GEOID', how = 'inner', validate = mergeValidation)

hdftracts['BigAgeDiff'] = np.where(np.abs(hdftracts['HDF_MaleAge'] - hdftracts['HDF_FemaleAge']) >= 20, 1, 0)
mdftracts['BigAgeDiff'] = np.where(np.abs(mdftracts['MDF_MaleAge'] - mdftracts['MDF_FemaleAge']) >= 20, 1, 0)

ss = pd.DataFrame({'Geography':'Tract', 'Size_Category':'All', 'Characteristic':'HDF 20+ Year Median Age Diff','NumCells':len(hdftracts), 'Inconsistent':np.sum(hdftracts['BigAgeDiff'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
ss = pd.DataFrame({'Geography':'Tract', 'Size_Category':'All', 'Characteristic':'MDF 20+ Year Median Age Diff','NumCells':len(mdftracts), 'Inconsistent':np.sum(mdftracts['BigAgeDiff'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Tracts where median age of the men is significantly different (equal to or greater than 20 years) from the median age of women by major race group
for r in racealonecats:
    hdftracts_males = dfhdf[(dfhdf['RACEALONE'] == r)&(dfhdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Males').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU','TABTRACTCE'])
    mdftracts_males = dfmdf[(dfmdf['RACEALONE'] == r)&(dfmdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Males').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU','TABTRACTCE'])
    hdftracts_females = dfhdf[(dfhdf['RACEALONE'] == r)&(dfhdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Females').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU','TABTRACTCE'])
    mdftracts_females = dfmdf[(dfmdf['RACEALONE'] == r)&(dfmdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Females').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU','TABTRACTCE'])

    hdftracts_gt5males = hdftracts_males.loc[hdftracts_males['HDF_Males'] >=5, 'GEOID'].tolist()
    mdftracts_gt5males = mdftracts_males.loc[mdftracts_males['MDF_Males'] >=5, 'GEOID'].tolist()
    hdftracts_gt5females = hdftracts_females.loc[hdftracts_females['HDF_Females'] >=5, 'GEOID'].tolist()
    mdftracts_gt5females = mdftracts_females.loc[mdftracts_females['MDF_Females'] >=5, 'GEOID'].tolist()

    hdftracts_gt5bothsex =  list(set(hdftracts_gt5males).intersection(hdftracts_gt5females))
    mdftracts_gt5bothsex =  list(set(mdftracts_gt5males).intersection(mdftracts_gt5females))

    hdftracts_gt5bothsex_index = pd.Index(hdftracts_gt5bothsex, name='GEOID')
    mdftracts_gt5bothsex_index = pd.Index(mdftracts_gt5bothsex, name='GEOID')

    hdftracts_malemedage = dfhdf[(dfhdf['RACEALONE'] == r)&(dfhdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='HDF_MaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(hdftracts_gt5bothsex_index).reset_index()
    mdftracts_malemedage = dfmdf[(dfmdf['RACEALONE'] == r)&(dfmdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='MDF_MaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(mdftracts_gt5bothsex_index).reset_index()
    hdftracts_femalemedage = dfhdf[(dfhdf['RACEALONE'] == r)&(dfhdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='HDF_FemaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(hdftracts_gt5bothsex_index).reset_index()
    mdftracts_femalemedage = dfmdf[(dfmdf['RACEALONE'] == r)&(dfmdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='MDF_FemaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(mdftracts_gt5bothsex_index).reset_index()

    hdftracts =  pd.merge(hdftracts_malemedage, hdftracts_femalemedage, on='GEOID', how = 'inner', validate = mergeValidation)
    mdftracts =  pd.merge(mdftracts_malemedage, mdftracts_femalemedage, on='GEOID', how = 'inner', validate = mergeValidation)

    hdftracts['BigAgeDiff'] = np.where(np.abs(hdftracts['HDF_MaleAge'] - hdftracts['HDF_FemaleAge']) >= 20, 1, 0)
    mdftracts['BigAgeDiff'] = np.where(np.abs(mdftracts['MDF_MaleAge'] - mdftracts['MDF_FemaleAge']) >= 20, 1, 0)

    ss = pd.DataFrame({'Geography':'Tract', 'Size_Category':'All', 'Characteristic':"HDF 20+ Year Median Age Diff {race}".format(race = racealonedict.get(r)),'NumCells':len(hdftracts), 'Inconsistent':np.sum(hdftracts['BigAgeDiff'])}, index=[0])
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)
    ss = pd.DataFrame({'Geography':'Tract', 'Size_Category':'All', 'Characteristic':"MDF 20+ Year Median Age Diff {race}".format(race = racealonedict.get(r)),'NumCells':len(mdftracts), 'Inconsistent':np.sum(mdftracts['BigAgeDiff'])}, index=[0])
    outputdf = outputdf.append(ss, ignore_index=True, sort=False)


# Tracts where median age of the men is significantly different (equal to or greater than 20 years) from the median age of women Hispanic
hdftracts_males = dfhdf[(dfhdf['CENHISP'] == '2')&(dfhdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Males').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU','TABTRACTCE'])
mdftracts_males = dfmdf[(dfmdf['CENHISP'] == '2')&(dfmdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Males').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU','TABTRACTCE'])
hdftracts_females = dfhdf[(dfhdf['CENHISP'] == '2')&(dfhdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Females').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU','TABTRACTCE'])
mdftracts_females = dfmdf[(dfmdf['CENHISP'] == '2')&(dfmdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Females').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU','TABTRACTCE'])

hdftracts_gt5males = hdftracts_males.loc[hdftracts_males['HDF_Males'] >=5, 'GEOID'].tolist()
mdftracts_gt5males = mdftracts_males.loc[mdftracts_males['MDF_Males'] >=5, 'GEOID'].tolist()
hdftracts_gt5females = hdftracts_females.loc[hdftracts_females['HDF_Females'] >=5, 'GEOID'].tolist()
mdftracts_gt5females = mdftracts_females.loc[mdftracts_females['MDF_Females'] >=5, 'GEOID'].tolist()

hdftracts_gt5bothsex =  list(set(hdftracts_gt5males).intersection(hdftracts_gt5females))
mdftracts_gt5bothsex =  list(set(mdftracts_gt5males).intersection(mdftracts_gt5females))

hdftracts_gt5bothsex_index = pd.Index(hdftracts_gt5bothsex, name='GEOID')
mdftracts_gt5bothsex_index = pd.Index(mdftracts_gt5bothsex, name='GEOID')

hdftracts_malemedage = dfhdf[(dfhdf['CENHISP'] == '2')&(dfhdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='HDF_MaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(hdftracts_gt5bothsex_index).reset_index()
mdftracts_malemedage = dfmdf[(dfmdf['CENHISP'] == '2')&(dfmdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='MDF_MaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(mdftracts_gt5bothsex_index).reset_index()
hdftracts_femalemedage = dfhdf[(dfhdf['CENHISP'] == '2')&(dfhdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='HDF_FemaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(hdftracts_gt5bothsex_index).reset_index()
mdftracts_femalemedage = dfmdf[(dfmdf['CENHISP'] == '2')&(dfmdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='MDF_FemaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(mdftracts_gt5bothsex_index).reset_index()

hdftracts =  pd.merge(hdftracts_malemedage, hdftracts_femalemedage, on='GEOID', how = 'inner', validate = mergeValidation)
mdftracts =  pd.merge(mdftracts_malemedage, mdftracts_femalemedage, on='GEOID', how = 'inner', validate = mergeValidation)

hdftracts['BigAgeDiff'] = np.where(np.abs(hdftracts['HDF_MaleAge'] - hdftracts['HDF_FemaleAge']) >= 20, 1, 0)
mdftracts['BigAgeDiff'] = np.where(np.abs(mdftracts['MDF_MaleAge'] - mdftracts['MDF_FemaleAge']) >= 20, 1, 0)

ss = pd.DataFrame({'Geography':'Tract', 'Size_Category':'All', 'Characteristic':'HDF 20+ Year Median Age Diff Hispanic','NumCells':len(hdftracts), 'Inconsistent':np.sum(hdftracts['BigAgeDiff'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
ss = pd.DataFrame({'Geography':'Tract', 'Size_Category':'All', 'Characteristic':'MDF 20+ Year Median Age Diff Hispanic','NumCells':len(mdftracts), 'Inconsistent':np.sum(mdftracts['BigAgeDiff'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Tracts where median age of the men is significantly different (equal to or greater than 20 years) from the median age of women Non-Hispanic White
hdftracts_males = dfhdf[(dfhdf['RACEALONE'] == 1)&(dfhdf['CENHISP'] == '1')&(dfhdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Males').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU','TABTRACTCE'])
mdftracts_males = dfmdf[(dfmdf['RACEALONE'] == 1)&(dfmdf['CENHISP'] == '1')&(dfmdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Males').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU','TABTRACTCE'])
hdftracts_females = dfhdf[(dfhdf['RACEALONE'] == 1)&(dfhdf['CENHISP'] == '1')&(dfhdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='HDF_Females').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU','TABTRACTCE'])
mdftracts_females = dfmdf[(dfmdf['RACEALONE'] == 1)&(dfmdf['CENHISP'] == '1')&(dfmdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).size().reset_index(name='MDF_Females').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU','TABTRACTCE'])

hdftracts_gt5males = hdftracts_males.loc[hdftracts_males['HDF_Males'] >=5, 'GEOID'].tolist()
mdftracts_gt5males = mdftracts_males.loc[mdftracts_males['MDF_Males'] >=5, 'GEOID'].tolist()
hdftracts_gt5females = hdftracts_females.loc[hdftracts_females['HDF_Females'] >=5, 'GEOID'].tolist()
mdftracts_gt5females = mdftracts_females.loc[mdftracts_females['MDF_Females'] >=5, 'GEOID'].tolist()

hdftracts_gt5bothsex =  list(set(hdftracts_gt5males).intersection(hdftracts_gt5females))
mdftracts_gt5bothsex =  list(set(mdftracts_gt5males).intersection(mdftracts_gt5females))

hdftracts_gt5bothsex_index = pd.Index(hdftracts_gt5bothsex, name='GEOID')
mdftracts_gt5bothsex_index = pd.Index(mdftracts_gt5bothsex, name='GEOID')

hdftracts_malemedage = dfhdf[(dfhdf['RACEALONE'] == 1)&(dfhdf['CENHISP'] == '1')&(dfhdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='HDF_MaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(hdftracts_gt5bothsex_index).reset_index()
mdftracts_malemedage = dfmdf[(dfmdf['RACEALONE'] == 1)&(dfmdf['CENHISP'] == '1')&(dfmdf['QSEX'] == '1')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='MDF_MaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(mdftracts_gt5bothsex_index).reset_index()
hdftracts_femalemedage = dfhdf[(dfhdf['RACEALONE'] == 1)&(dfhdf['CENHISP'] == '1')&(dfhdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='HDF_FemaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(hdftracts_gt5bothsex_index).reset_index()
mdftracts_femalemedage = dfmdf[(dfmdf['RACEALONE'] == 1)&(dfmdf['CENHISP'] == '1')&(dfmdf['QSEX'] == '2')].groupby(['TABBLKST', 'TABBLKCOU', 'TABTRACTCE'])['QAGE'].aggregate(lambda x: median_grouped(x+0.5)).reset_index(name='MDF_FemaleAge').assign(GEOID = lambda x: x.TABBLKST + x.TABBLKCOU + x.TABTRACTCE).drop(columns = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE']).set_index('GEOID').reindex(mdftracts_gt5bothsex_index).reset_index()

hdftracts =  pd.merge(hdftracts_malemedage, hdftracts_femalemedage, on='GEOID', how = 'inner', validate = mergeValidation)
mdftracts =  pd.merge(mdftracts_malemedage, mdftracts_femalemedage, on='GEOID', how = 'inner', validate = mergeValidation)

hdftracts['BigAgeDiff'] = np.where(np.abs(hdftracts['HDF_MaleAge'] - hdftracts['HDF_FemaleAge']) >= 20, 1, 0)
mdftracts['BigAgeDiff'] = np.where(np.abs(mdftracts['MDF_MaleAge'] - mdftracts['MDF_FemaleAge']) >= 20, 1, 0)

ss = pd.DataFrame({'Geography':'Tract', 'Size_Category':'All', 'Characteristic':'HDF 20+ Year Median Age Diff Non-Hispanic White','NumCells':len(hdftracts), 'Inconsistent':np.sum(hdftracts['BigAgeDiff'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)
ss = pd.DataFrame({'Geography':'Tract', 'Size_Category':'All', 'Characteristic':'MDF 20+ Year Median Age Diff Non-Hispanic White','NumCells':len(mdftracts), 'Inconsistent':np.sum(mdftracts['BigAgeDiff'])}, index=[0])
outputdf = outputdf.append(ss, ignore_index=True, sort=False)

# Output
outputdf.to_csv(f"{OUTPUTDIR}/hdf_per_metrics_v2.csv", index=False)
print("{} All Done".format(datetime.now()))
