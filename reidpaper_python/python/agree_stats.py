# agree_stats.py
#-----------------------------
# /usr/bin/env python3 agree_stats.py
# normal execution is via call in runall.py
#-----------------------------
# Created By: David Zhou, Paul Bartholomew, Grace Tang, Andrew Sillers
# Created Date: 5/31/2021
# version = '1.0'

###############################################################################
"""
NOTICE
This (software/technical data) was produced for the U. S. Government under Contract Number TIRNO-99-D-00005, 
and is subject to Federal Acquisition Regulation Clause 52.227-14, 
Rights in Data—General, Alt. II, III and IV (DEC 2007) [Reference 27.409(a)].  
No other use other than that granted to the U. S. Government, 
or to those acting on behalf of the U. S. Government under that Clause is 
authorized without the express written permission of The MITRE Corporation. 
For further information, please contact The MITRE Corporation, 
Contracts Management Office, 
7515 Colshire Drive, 
McLean, VA  22102-7539, 
(703) 983-6000.
COPYRIGHT 2017 The MITRE Corporation
"""
###############################################################################

# Description:
'''
agree_stats.py:

This code is a translation of existing research STATA code developed by MITRE for the purposes of testing
the corresponding reidentification ouputs. This python file is the translation of "1.agree_stats.do", responsible
for computing agreement stats.

Previous STATA Code Header:
[There was no code header]
'''

from datetime import datetime
import pandas as pd
import re
from os.path import exists
from config import *

# *--------------------------------------------------
# * AGREEMENT RATES
# *--------------------------------------------------

# capture agreeStats drop agreeStats
# program define agreeStats
#     * e.g. >agreeStats "hdf" "r00" 1
#     args left right fzy
#     * fzy = 1 then fuzzy age was used, if fzy=0, then binage was used
#
#     * determine location of results
#     if "`right'"=="$rhdf" | "`right'"=="$rhdfbo"  {
#         local resultsLoc = "$rsltbase/rhdf/`right'/"
#     }
#     else {
#         local resultsLoc = "$rsltbase/`right'/"
#     }
#     if `fzy' == 1 {
#         local fileStem = "agree"
#         local ageVar = "fzyage"
#         local matchFileStem = "agreematch"
#     }
#     else {
#         local fileStem = "agreebinage"
#         local ageVar = "binage"
#         local matchFileStem = "agreematchbinage"
#     }
def agreeStats(left, right, fzy):
    # fzy = 1 then fuzzy age was used, if fzy=0, then binage was used

    # determine location of results
    if right == rhdf or right == rhdfbo or right == rmdf or right == mdf:
        resultsLoc = f"{rsltbase}/rhdf/{right}"
    else:
        resultsLoc = f"{rsltbase}/{right}"

    if fzy == 1:
        fileStem = "agree"
        ageVar = "fzyage"
        matchFileStem = "agreematch"
    else:
        fileStem = "agreebinage"
        ageVar = "binage"
        matchFileStem = "agreematchbinage"

    #     local exactmflag = 1
    #     local fzyagemflag = 2
    #     local binagemflag = 3
    #     local oneoffmflag = 4
    #
    #     di "left: `left'"
    #     di "right: `right'"
    #     di "rhdf: $rhdf"
    #     di "rhdfbo: $rhdfbo"
    #     di "resultsLoc: `resultsLoc'"
    #     di "fileStem: `fileStem'"
    #     di "ageVar: `ageVar'"
    # match flag variables

    mflags = {
        "exact": 1,
        "fzyage": 2,
        "binagemflag": 3,
        "oneoffmflag": 4
    }

    print(f"left: {left}")
    print(f"right: {right}")
    print(f"rhdf: {rhdf}")
    print(f"rhdfbo: {rhdfbo}")
    print(f"resultsLoc: {resultsLoc}")
    print(f"fileStem: {fileStem}")
    print(f"ageVar: {ageVar}")

    #
    #     * count matches
    #     import delimited using "`resultsLoc'/`fileStem'_`left'_`right'.csv", clear stringcols(1 2 3) $trainingWheels
    #     gen geoid_block = county + tract + block
    #     merge 1:1 geoid_block using $temp/cefblocksize, keep(3) nogen
    #     replace oneoff = exact + `ageVar' + oneoff
    #     replace `ageVar' = exact + `ageVar'
    #     preserve
    #         collapse (sum) exact `ageVar' oneoff
    #         gen lbl = "`right'-`left', `ageVar'"
    #         append using $temp/agree_final
    #         save $temp/agree_final, replace
    #     restore
    #     preserve
    #         collapse (sum) exact `ageVar' oneoff, by(block_pop)
    #         foreach v of varlist exact `ageVar' oneoff {
    #             rename `v' `left'`right'`ageVar'_`v'
    #         }
    #         order block_pop `left'`right'`ageVar'_exact `left'`right'`ageVar'_`ageVar' `left'`right'`ageVar'_oneoff
    #         save $temp/agreeblksz, replace
    #         merge 1:1 block_pop using $temp/agreeblksz_final, nogen
    #         save $temp/agreeblksz_final, replace
    #     restore
    #     collapse (sum) exact `ageVar' oneoff, by(solvar)
    #     foreach v of varlist exact `ageVar' oneoff {
    #         rename `v' `left'`right'`ageVar'_`v'
    #     }
    #     order solvar `left'`right'`ageVar'_exact `left'`right'`ageVar'_`ageVar' `left'`right'`ageVar'_oneoff
    #     merge 1:1 solvar using $temp/agreesolvar_final, nogen
    #     save $temp/agreesolvar_final, replace
    #
    # end
    print(f"{resultsLoc}/{fileStem}_{left}_{right}.csv")
    df = get_df(f"{resultsLoc}/{fileStem}_{left}_{right}.csv", trainingWheels, dtype={"county":str, "tract":str, "block":str})
    print('df', list(df))
    #df.iloc[:, [0, 1, 2]] = df.iloc[:, [0, 1, 2]].astype(str)    
    df["geoid_block"] = df.county + df.tract + df.block
   
    cefblocksize = pd.read_csv(f'{temp}/cefblocksize.csv', dtype={'geoid_block':str, 'solvar':str})
    print('cefblocksize', list(cefblocksize))
    print('cefblocksizesolvar', cefblocksize.solvar.value_counts())
    
    print('cefblockwize uniqueblockpop', cefblocksize.block_pop.unique())

    df = df.merge(cefblocksize, on=["geoid_block"], how='inner')
    df["oneoff"] = df["exact"] + df[ageVar] + df["oneoff"]
    df[ageVar] = df["exact"] + df[ageVar]
    print('df uniqueblockpop', df.block_pop.unique())

    print('dfsolvar', df.solvar.value_counts())
    agree_final = df[["exact", ageVar, "oneoff"]].sum().to_frame().transpose()
    agree_final["lbl"] = f'{right}-{left}, {ageVar}'
    print('agree_final', list(agree_final))
   
    if exists(f'{temp}/agree_final.csv'):
        agree_final = pd.concat([agree_final, pd.read_csv(f'{temp}/agree_final.csv')], ignore_index=True)
        #agree_final = pd.merge(agree_final, pd.read_csv(f'{temp}/agree_final.csv'), how='outer')
    agree_final.to_csv(f"{temp}/agree_final.csv", index=False)

    
    agreeblksz_final = df.groupby(["block_pop"]).sum().reset_index()[["block_pop", "exact", ageVar, "oneoff"]]
    print('agreeblksz_final', list(agreeblksz_final))
    print('agreeblksz_final block_popunique', agreeblksz_final.block_pop.unique())
    for v in ["exact", ageVar, "oneoff"]:
        agreeblksz_final = agreeblksz_final.rename(columns={v: f"{left}{right}{ageVar}_{v}"})
    
    # Sort block_pop
    agreeblksz_final['sort'] = agreeblksz_final['block_pop'].str.extract('(\d+)(-|\+)', expand=False).iloc[:,0].astype(int)
    print(agreeblksz_final.sort)
    agreeblksz_final = agreeblksz_final.sort_values('sort')
    agreeblksz_final = agreeblksz_final.drop('sort', axis=1)
    print('agreeblksz final', agreeblksz_final)

    # We already have the columns in order
    cols_to_move = [f"{left}{right}{ageVar}_exact", f"{left}{right}{ageVar}_{ageVar}", f"{left}{right}{ageVar}_oneoff"]
    agreeblksz_final = agreeblksz_final[cols_to_move + [col for col in list(agreeblksz_final) if col not in cols_to_move]]
    agreeblksz_final.to_csv(f"{temp}/agreeblksz.csv", index=False)

    if exists(f'{temp}/agreeblksz_final.csv'):
        agreeblksz_final = agreeblksz_final.merge(pd.read_csv(f"{temp}/agreeblksz_final.csv"), on=["block_pop"], how='outer')
    agreeblksz_final.to_csv(f"{temp}/agreeblksz_final.csv", index=False)

    agreesolvar_final = df.groupby(["solvar"]).sum().reset_index()[["solvar", "exact", ageVar, "oneoff"]]
    print('agreesolvar', agreesolvar_final.solvar.value_counts())
    print('agreesolvar_final', list(agreesolvar_final))
    for v in ["exact", ageVar, "oneoff"]:
        agreesolvar_final = agreesolvar_final.rename(columns={v: f"{left}{right}{ageVar}_{v}"})
    cols_to_move = [f"{left}{right}{ageVar}_exact", f"{left}{right}{ageVar}_{ageVar}", f"{left}{right}{ageVar}_oneoff"]
    agreesolvar_final = agreesolvar_final[cols_to_move + [col for col in agreesolvar_final.columns if col not in cols_to_move]]
    print('agreesolvar', agreesolvar_final.solvar.value_counts())
    if exists(f'{temp}/agreesolvar_final.csv'):
        agreesolvar_final = agreesolvar_final.merge(pd.read_csv(f"{temp}/agreesolvar_final.csv", dtype={'solvar':str}), on=["solvar"], how='outer')
    agreesolvar_final.to_csv(f"{temp}/agreesolvar_final.csv", index=False)

# *--------------------------------------------------
# * AGREEMENT RATES
# *--------------------------------------------------
# * create blank container datasets
# clear
# set obs 1
# gen lbl = ""
# save $temp/agree_final, replace
# clear
# set obs 1
# gen block_pop = .
# save $temp/agreeblksz_final, replace
# clear
# set obs 1
# gen solvar = ""
# save $temp/agreesolvar_final, replace
#
# if $agree_rhdf_hdf_fz == 1 {
#     agreeStats "hdf" "$rhdf" 1
# }
#
# if $agree_rhdf_cef_fz == 1 {
#     agreeStats "cef" "$rhdf" 1
# }
#
# if $agree_rhdf_hdf_bn == 1 {
#     agreeStats "hdf" "$rhdf" 0
# }
#
# if $agree_rhdfbo_hdf_bn == 1 {
#     agreeStats "hdf" "$rhdfbo" 0
# }
#
# if $agree_rhdf_cef_bn == 1 {
#     agreeStats "cef" "$rhdf" 0
# }
#
# if $agree_rhdfbo_cef_bn == 1 {
#     agreeStats "cef" "$rhdfbo" 0
# }
#
# if $agree_rmdf_cef_bn == 1 {
#     agreeStats "cef" "$rmdf" 0
# }
#
# if $agree_rmdf_hdf_bn == 1 {
#     agreeStats "hdf" "$rmdf" 0
# }
#
# if $agree_mdf_cef_bn == 1 {
#     agreeStats "cef" "$mdf" 0
# }
#
# if $agree_mdf_hdf_bn == 1 {
#     agreeStats "hdf" "$mdf" 0
# }
#
# if $agree_hdf_cef_bn == 1 {
#     agreeStats "cef" "hdf" 0
# }

def main():
    # di c(current_date) " " c(current_time)
    # di "#### START OF AGREEMENT STATS ####"
    print(datetime.now())
    print("#### START OF AGREEMENT STATS ####")

    '''
    df = pd.DataFrame()
    df.append(pd.Series(), ignore_index=True)
    df["lbl"] = ""
    df.to_csv(f'{temp}/agree_final.csv', index=False)

    df = pd.DataFrame()
    df.append(pd.Series(), ignore_index=True)
    df["block_pop"] = ""
    df.to_csv(f'{temp}/agreeblksz_final.csv', index=False)
    
    df = pd.DataFrame()
    df.append(pd.Series(), ignore_index=True)
    df["solvar"] = ""
    df.to_csv(f'{temp}/agreesolvar_final.csv', index=False)
    '''

    if agree_rhdf_hdf_fz == 1:
        agreeStats("hdf", rhdf, 1)
    
    if agree_rhdf_cef_fz == 1:
        agreeStats("cef", rhdf, 1)    
        
    if agree_rhdf_hdf_bn == 1:
        agreeStats("hdf", rhdf, 0)    

    if agree_rhdf_cef_bn == 1:
        agreeStats("cef", rhdf, 0) 

    if agree_rhdfbo_cef_bn == 1:
        agreeStats("cef", rhdfbo, 0)  

    if agree_rhdfbo_hdf_bn == 1:
        agreeStats("hdf", rhdfbo, 0)

    if agree_rmdf_cef_bn == 1:
        agreeStats("cef", rmdf, 0)

    if agree_rmdf_hdf_bn == 1:
        agreeStats("hdf", rmdf, 0)

    if agree_mdf_cef_bn == 1:
        agreeStats("cef", mdf, 0)

    if agree_mdf_hdf_bn == 1:
        agreeStats("hdf", mdf, 0)

    if agree_hdf_cef_bn == 1:
        agreeStats("cef", "hdf", 0)

    # use `cefpopnat', clear
    # local cefnatpop = cefpop[1]
    # gen all=1
    # tempfile denominators
    # save `denominators'
    # use `rhdfpopnat', clear
    # local rhdfnatpop = rhdfpop[1]
   
    # We save cefpopnat to temp in runall instead of importing runall to avoid circular imports 
    cefpopnat = pd.read_csv(f'{temp}/cefpopnat.csv')
    print('cefpopnat', list(cefpopnat))
    cefnatpop = cefpopnat.cefpop[0]
    print('cefnatpop', cefnatpop, type(cefnatpop))
    cefpopnat["all"] = 1
    denominators = cefpopnat
    print('denominators', list(denominators))
    rhdfpopnat = pd.read_csv(f'{temp}/rhdfpopnat.csv')
    print('rhdfpopnat', list(rhdfpopnat))
    rhdfnatpop = rhdfpopnat.rhdfpop[0] 
    print('rhdfnatpop', rhdfnatpop)

    # * SAVE NATIONAL AGREE STATS
    # use $temp/agree_final, clear
    # gen all= 1
    # merge m:1 all using `denominators', keep(1 3) nogen
    # drop all
    # gen incef = `cefnatpop'
    # gen inrecon = `rhdfnatpop'
    
    agree_final = pd.read_csv(f'{temp}/agree_final.csv')
    agree_final["all"] = 1
    agree_final = pd.merge(agree_final, denominators, on=["all"], how="left")
    agree_final = agree_final.drop('all', axis=1)
    agree_final["incef"] = cefnatpop
    agree_final["inrecon"] = rhdfnatpop

    #df = pd.read_csv(f'{temp}'/agree_final')
    #df['all'] = 1
    #df = pd.merge(df, denominators, on['all'], how='left')

    # * in case you only did fuzzy or only did binage
    # if $agree_rhdf_hdf_fz == 1 | $agree_rhdf_cef_fz==1 {
    #     order lbl incef inrecon exact fzyage binage oneoff
    #     format incef inrecon exact fzyage binage oneoff %20.0gc
    # }
    # else {
    #     order lbl incef inrecon exact binage oneoff
    #     format incef inrecon exact binage oneoff %20.0gc
    # }
    # drop if missing(lbl)
    # export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(agree) firstrow(variables) replace
  
    if agree_rhdf_hdf_fz == 1 or agree_rhdf_cef_fz == 1: 
        order = ["lbl", "incef", "inrecon", "exact", "fzyage", "binage"]
        cols = order + [x for x in list(agree_final) if x not in order]
        agree_final = agree_final[cols]
        for v in order[1:]: 
            pass #agree_final[v] = agree_final[v].map('{:,}'.format)
    else: 
        order = ["lbl", "incef", "inrecon", "exact", "binage"]
        cols = order + [x for x in list(agree_final) if x not in order]
        agree_final = agree_final[cols]
        for v in order[1:]: 
            pass #agree_final[v] = agree_final[v].map('{:,}'.format)

    agree_final = agree_final.dropna(subset=["lbl"])
    print("agree_final excel", list(agree_final))
    #with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004.xlsx", mode="w", if_sheet_exists="replace") as writer:
    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004.xlsx", mode="w") as writer:
        agree_final.to_excel(writer, sheet_name="agree", index=False)

    # * SAVE BLOCK SIZE AGREE STATS
    # use $temp/agreeblksz_final, clear
    # merge 1:1 block_pop using `cefpopblocksz', nogen
    # order block_pop cefpop
    # if $agree_rhdf_hdf_fz == 1 | $agree_rhdf_hdf_fz==1 {
    #     format *_exact *_fzyage *_binage *_oneoff %20.0gc
    # }
    # else {
    #     format *_exact *_binage *_oneoff %20.0gc
    # }
    # drop if missing(block_pop)
    # export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(agree_blksz) firstrow(variables) sheetmodify

    agreeblksz_final = pd.read_csv(f'{temp}/agreeblksz_final.csv')
    cefpopblocksz = pd.read_csv(f'{temp}/cefpopblocksz.csv')
    print('cefpopblocksz', list(cefpopblocksz))
    
    agreeblksz_final = pd.merge(agreeblksz_final, cefpopblocksz, on=["block_pop"])
    order = ["block_pop", "cefpop"]
    cols = order + [x for x in list(agreeblksz_final) if x not in order]
    agreeblksz_final = agreeblksz_final[cols]

    if agree_rhdf_hdf_fz == 1 or agree_rhdf_cef_fz == 1: 
        for v in [col for col in agreeblksz_final.columns if col.endswith('_exact') or col.endswith('_fzyage') or col.endswith('_binage')]:
            pass #agreeblksz_final[v] = agreeblksz_final[v].map('{:,}'.format)
    else:
        for v in [col for col in agreeblksz_final.columns if col.endswith('_exact') or col.endswith('_binage')]:
            pass #agreeblksz_final[v] = agreeblksz_final[v].map('{:,}'.format)

    print('agreeblksz_final excel', list(agreeblksz_final))
    agreeblksz_final = agreeblksz_final.dropna(subset=["block_pop"])
    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004.xlsx", mode="a", if_sheet_exists="replace") as writer:
        agreeblksz_final.to_excel(writer, sheet_name="agree_blksz", index=False)
   

    # * SAVE SOLVAR AGREE STATS
    # use $temp/agreesolvar_final, clear
    # merge 1:1 solvar using `solvarpop', nogen
    # order solvar cefpop
    # if $agree_rhdf_hdf_fz == 1 | $agree_rhdf_hdf_fz==1 {
    #     format *_exact *_fzyage *_binage *_oneoff %20.0gc
    # }
    # else {
    #     format *_exact *_binage *_oneoff %20.0gc
    # }
    # drop if missing(solvar)
    # export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(agree_solvar) firstrow(variables) sheetmodify
    
    agreesolvar_final = pd.read_csv(f'{temp}/agreesolvar_final.csv', dtype={'solvar':str})
    solvarpop = pd.read_csv(f'{temp}/solvarpop.csv')
    agreesolvar_final = pd.merge(agreesolvar_final, solvarpop, on=["solvar"])


    order = ["solvar", "cefpop"]
    cols = order + [col for col in agreesolvar_final if col not in order]
    agreesolvar_final = agreesolvar_final[cols]

    if agree_rhdf_hdf_fz == 1 or agree_rhdf_cef_fz == 1: 
        for v in [col for col in agreesolvar_final.columns if col.endswith('_exact') or col.endswith('_fzyage') or col.endswith('_binage')]:
            pass #agreesolvar_final[v] = agreesolvar_final[v].map('{:,}'.format)
    else:
        for v in [col for col in agreesolvar_final.columns if col.endswith('_exact') or col.endswith('_binage')]:
            pass #agreesolvar_final[v] = agreesolvar_final[v].map('{:,}'.format)
     
    agreesolvar_final = agreesolvar_final.dropna(subset=["solvar"])
    print('agreesolvar_final excel', list(agreesolvar_final))
    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004.xlsx", mode="a", if_sheet_exists="replace") as writer:
        agreesolvar_final.to_excel(writer, sheet_name="agree_solvar", index=False)

    #*--------------------------------------------------
    #* BLOCK-PIK CEF-CMRCL MATCH STATS
    #*--------------------------------------------------
    #import delimited using $rsltbase/cmrcl/cmrcl_cef_bpas_match_counts.csv, clear 
    #collapse (sum) cef_all cef_keep cmrcl_all cmrcl_has_bp cmrcl_has_bpas bp bpas bpabs
    #rename bp cmrcl_cef_mtch_blkpik
    #rename bpas cmrcl_cef_mtch_blkpikagesex
    #rename bpabs cmrcl_cef_mtch_blkpikagebsex
    #gen mtch_blkpik_of_keep = 100*(cmrcl_cef_mtch_blkpik/cef_keep)
    #gen mtch_blkpikagesex_of_keep = 100*(cmrcl_cef_mtch_blkpikagesex/cef_keep)
    #gen mtch_blkpikagebsex_of_keep = 100*(cmrcl_cef_mtch_blkpikagebsex/cef_keep)
    #order cef_all cef_keep cmrcl_all cmrcl_has_bp cmrcl_has_bpas cmrcl_cef_mtch_blkpik mtch_blkpik_of_keep cmrcl_cef_mtch_blkpikagesex mtch_blkpikagesex_of_keep cmrcl_cef_mtch_blkpikagebsex mtch_blkpikagebsex_of_keep
    #format cef_all cef_keep cmrcl_all cmrcl_has_bp cmrcl_has_bpas cmrcl_cef_mtch_blkpik cmrcl_cef_mtch_blkpikagesex cmrcl_cef_mtch_blkpikagebsex  %20.0gc 
    #format *_of_keep %20.4f
    #export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(cmrcl_cef_match) firstrow(variables) 
    df = get_df(f"{rsltbase}/cmrcl/cmrcl_cef_bpas_match_counts.csv", trainingWheels)
    df = df[["cef_all", "cef_keep", "cmrcl_all", "cmrcl_has_bp", "cmrcl_has_bpas", "bp", "bpas", "bpabs"]]
    sums = df.sum()

    sums = sums.rename({"bp": "cmrcl_cef_mtch_blkpik",
                    "bpas": "cmrcl_cef_mtch_blkpikagesex",
                    "bpabs": "cmrcl_cef_mtch_blkpikagebsex"})

    sums["mtch_blkpik_of_keep"] = 100 * (sums["cmrcl_cef_mtch_blkpik"] / sums["cef_keep"])
    sums["mtch_blkpikagesex_of_keep"] = 100*( sums["cmrcl_cef_mtch_blkpikagesex"]/sums["cef_keep"])
    sums["mtch_blkpikagebsex_of_keep"] = 100 * (sums["cmrcl_cef_mtch_blkpikagebsex"] / sums["cef_keep"])
    sums = sums[["cef_all", "cef_keep", "cmrcl_all", "cmrcl_has_bp", "cmrcl_has_bpas", "cmrcl_cef_mtch_blkpik", "mtch_blkpik_of_keep", "cmrcl_cef_mtch_blkpikagesex", "mtch_blkpikagesex_of_keep", "cmrcl_cef_mtch_blkpikagebsex", "mtch_blkpikagebsex_of_keep"]]
    for v in ["cef_all", "cef_keep", "cmrcl_all", "cmrcl_has_bp", "cmrcl_has_bpas", "cmrcl_cef_mtch_blkpik", "cmrcl_cef_mtch_blkpikagesex", "cmrcl_cef_mtch_blkpikagebsex"]:
        sums[v] = '{:,.0f}'.format(sums[v])
    for v in [x for x,_ in sums.items() if re.match(".*_of_keep", x)]:
        sums[v] = '{:,.4f}'.format(sums[v])

    # convert sum series to dataframe for output formatting
    sums = sums.to_frame().transpose()

    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004.xlsx", mode="a", if_sheet_exists="replace") as writer:
        sums.to_excel(writer, sheet_name="cmrcl_cef_match", index=False)

    print("#### END OF AGREE STATS ####")
    now = datetime.now()
    print(now)

if __name__ == "__main__":
    main()
