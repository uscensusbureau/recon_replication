# confirm_stats.py
# -----------------------------
# /usr/bin/env python3 confirm_stats.py
# normal execution is via call in runall.py
# -----------------------------
# Created By: Andrew Sillers, David Zhou, Paul Bartholomew, Grace Tang
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
# 

# Description:
'''
confirm_stats.py:

This code is a translation of existing research STATA code developed by MITRE for the purposes of testing
the corresponding reidentification ouputs. This python file is the translation of "3.confirm_stats.do", responsible
for computing reidentification confrimation statistics.

Previous STATA Code Header:
[There was no code header]
'''

from datetime import datetime
from config import *
import pandas as pd
import numpy as np
from os.path import exists
import re

# capture confirmStats drop confirmStats
# program define confirmStats
#     * e.g. >confirmStats "r00" "cmrcl" 1
#     args left right fzy
#     * fzy = 1 then fuzzy age was used, if fzy=0, then binage was used
# 
#     * determine location of results
#     if "`left'"=="$rhdf" | "`left'"=="$rhdfbo" | "`left'"=="$rmdf" | "`left'"=="$mdf"  {
#         local resultsLoc = "$rsltbase/rhdf/`left'/"
#     }
#     else {
#         local resultsLoc = "$rsltbase/`left'/"
#         if regexm("`left'","Gsr") {
#             local resultsLoc = "$rsltbase/simul/"            
#         }
#         else {
#             local resultsLoc = "$rsltbase/`left'/"
#         }
#     }
#     if `fzy' == 1 {
#         local fileStem = "confirm"
#         local ageVar = "fzyage"
#         local matchFileStem = "confirmmatch"
#     }
#     else {
#         local fileStem = "confirmbinage"
#         local ageVar = "binage"
#         local matchFileStem = "confirmmatchbinage"
#     } 
#
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
# 
#     import delimited using "`resultsLoc'/`matchFileStem'_`left'`right'_cef.csv", clear stringcols(1 2 3 13) $trainingWheels
    

def confirmStats(left, right, fzy):
    # determine location of results
    if left == rhdf or left == rhdfbo or left == rmdf or left == mdf:
        resultsLoc = f"{rsltbase}/rhdf/{left}"
    elif "Gsr" in left:
        resultsLoc =  f"{rsltbase}/simul"
    else:
        resultsLoc = f"{rsltbase}/{left}"

    if fzy == 1: 
        fileStem = "confirm"
        ageVar = "fzyage"
        matchFileStem = "confirmmatch"
    else: 
        fileStem = "confirmbinage"
        ageVar = "binage"
        matchFileStem = "confirmmatchbinage"

    #if regexm("`left'","Gsr") {
    #    local leftShrt = "`left'"
    #    local leftShrt = subinstr("`leftShrt'","Gsr","g",.)
    #    local leftShrt = subinstr("`leftShrt'","modal","md",.)
    #    local leftShrt = subinstr("`leftShrt'","prop","pr",.)
    #}
    #else {
    #    local leftShrt = "`left'"
    #}

    if "Gsr" in left:
        leftShrt = left.replace("Gsr","g").replace("modal","md").replace("prop", "pr")
    else:
        leftShrt = left

    mflags = {"exactmflag": 1,
              "fzyagemflag": 2,
              "binagemflag": 3,
              "oneoffmflag": 4}

    print(f"left: {left}")
    print(f"right: {right}")
    print(f"rhdf: {rhdf}")
    print(f"rhdfbo: {rhdfbo}")
    print(f"resultsLoc: {resultsLoc}")
    print(f"fileStem: {fileStem}")
    print(f"ageVar: {ageVar}")

    df = get_df(f"{resultsLoc}/{matchFileStem}_{left}{right}_cef.csv", trainingWheels, dtype={"county":str, "tract":str, "block": str, "pik": str })
    #df.iloc[:, [0, 1, 2, 12]] = df.iloc[:, [0, 1, 2, 12]].astype(str)

    # gen geoid_block = county + tract + block
    # merge 1:1 geoid_block pik using $temp/cef_over21_modalre_oneton, keep(1 3) nogen
    # merge 1:1 geoid_block pik using $temp/cmrcl_cef_bpas_match, keep(1 3) nogen 
    df["geoid_block"] = df["county"] + df["tract"] + df["block"]
    cef_over21_modalre_oneton = pd.read_csv(f'{temp}/cef_over21_modalre_oneton.csv', dtype={'geoid_block':str, 'pik':str})
    df = pd.merge(df, cef_over21_modalre_oneton, on=["geoid_block", "pik"], how="left") 
    cmrcl_cef_bpas_match = pd.read_csv(f'{temp}/cmrcl_cef_bpas_match.csv', dtype={'geoid_block':str, 'pik':str})
    df = pd.merge(df, cmrcl_cef_bpas_match, on=["geoid_block", "pik"], how="left")

    # foreach v in exact `ageVar' {
    #     * over/under 21 
    #     gen `leftShrt'`right'`ageVar'_o21_`v' = over21==1  & put_matchflag==``v'mflag'
    #     replace `leftShrt'`right'`ageVar'_o21_`v' = . if missing(over21)
    #     gen `leftShrt'`right'`ageVar'_u21_`v' = over21==0 & put_matchflag==``v'mflag'
    #     replace `leftShrt'`right'`ageVar'_u21_`v' = . if missing(over21)
    #     * modal/nonmodal 
    #     gen `leftShrt'`right'`ageVar'_m_`v' = modalre==1 & put_matchflag==``v'mflag' 
    #     replace `leftShrt'`right'`ageVar'_m_`v' = . if missing(modalre)
    #     gen `leftShrt'`right'`ageVar'_nm_`v' = modalre==0 & put_matchflag==``v'mflag' 
    #     replace `leftShrt'`right'`ageVar'_nm_`v' = . if missing(modalre)
    #     * onetons
    #     foreach ton in sab_oneton sabb_oneton ber_oneton {
    #         local tonabrv = subinstr("`ton'","_oneton","",.)
    #         gen `leftShrt'`right'`ageVar'_`tonabrv'_`v' = `ton'==1 & put_matchflag==``v'mflag'
    #         replace `leftShrt'`right'`ageVar'_`tonabrv'_`v' = . if missing(`ton')
    #     }
    #     * onetons by modal/nonmodal
    #     foreach ton in sab_oneton sabb_oneton {
    #         local tonabrv = subinstr("`ton'","_oneton","",.)
    #         gen `leftShrt'`right'`ageVar'_m`tonabrv'_`v' = `ton'==1 & modalre==1 & put_matchflag==``v'mflag' 
    #         replace `leftShrt'`right'`ageVar'_m`tonabrv'_`v' = . if missing(`ton') | missing(modalre)
    #         gen `leftShrt'`right'`ageVar'_nm`tonabrv'_`v' = `ton'==1 & modalre==0 & put_matchflag==``v'mflag' 
    #         replace `leftShrt'`right'`ageVar'_nm`tonabrv'_`v' = . if missing(`ton') | missing(modalre)
    #     }            
    #     gen `v' = put_matchflag==``v'mflag'
    # }
    for v in ['exact', ageVar]:
        # over/under 21
        df[f"{leftShrt}{right}{ageVar}_o21_{v}"] = (df["over21"] == 1) & (df["put_matchflag"] == mflags[f"{v}mflag"]) # depends on what v is (global vs. local/vars)
        #df.loc[df["over21"].isna(), f"{leftShrt}{right}{ageVar}_o21_{v}"] = np.nan

        df[f"{leftShrt}{right}{ageVar}_u21_{v}"] = (df["over21"] == 0) & (df["put_matchflag"] == mflags[f"{v}mflag"])
        #df.loc[df["over21"].isna(), f"{leftShrt}{right}{ageVar}_u21_{v}"] = np.nan
        
        # modal/nonmodal
        df[f"{leftShrt}{right}{ageVar}_m_{v}"] = (df["modalre"] == 1) & (df["put_matchflag"] == mflags[f"{v}mflag"]) 
        #df.loc[df["modalre"].isna(), f"{leftShrt}{right}{ageVar}_m_{v}"] = np.nan

        df[f"{leftShrt}{right}{ageVar}_nm_{v}"] = (df["modalre"] == 0) & (df["put_matchflag"] == mflags[f"{v}mflag"])
        #df.loc[df["modalre"].isna(), f"{leftShrt}{right}{ageVar}_nm_{v}"] = np.nan

        # onetons
        for ton in ["sab_oneton", "sabb_oneton", "ber_oneton"]:
            tonabrv = ton.replace("_oneton", "")
            df[f"{leftShrt}{right}{ageVar}_{tonabrv}_{v}"] = (df[ton] == 1) & (df["put_matchflag"] == mflags[f"{v}mflag"])
            #df.loc[df[ton].isna(), f"{leftShrt}{right}{ageVar}_{tonabrv}_{v}"] = np.nan
        
        # onetons by modal/nonmodal
        for ton in ["sab_oneton", "sabb_oneton"]: 
            tonabrv = ton.replace("_oneton", "")
            df[f"{leftShrt}{right}{ageVar}_m{tonabrv}_{v}"] = (df[ton] == 1) & (df["modalre"] == 1) & (df["put_matchflag"] == mflags[f"{v}mflag"])
            #df.loc[df[ton].isna() | df["modalre"].isna(), f"{leftShrt}{right}{ageVar}_m{tonabrv}_{v}"] = np.nan
            df[f"{leftShrt}{right}{ageVar}_nm{tonabrv}_{v}"] = (df[ton] == 1) & (df["modalre"] == 0) & (df["put_matchflag"] == mflags[f"{v}mflag"])
            #df.loc[df[ton].isna() | df["modalre"].isna(), f"{leftShrt}{right}{ageVar}_nm{tonabrv}_{v}"] = np.nan


        # gen `leftShrt'`right'`ageVar'_ccbpas_`v' = bpas==1  & put_matchflag==``v'mflag'
        # replace `leftShrt'`right'`ageVar'_ccbpas_`v' = . if missing(bpas)
        # gen `leftShrt'`right'`ageVar'_ccbpabs_`v' = bpabs==1  & put_matchflag==``v'mflag'
        # replace `leftShrt'`right'`ageVar'_ccbpabs_`v' = . if missing(bpabs)
        df[f"{leftShrt}{right}{ageVar}_ccbpas_{v}"] = (df["bpas"] == 1) & (df["put_matchflag"] == mflags[f"{v}mflag"])
        #df.loc[df["bpas"].isna() | df["modalre"].isna(), f"{leftShrt}{right}{ageVar}_ccbpas_{v}"] = np.nan
        df[f"{leftShrt}{right}{ageVar}_ccbpabs_{v}"] = (df["bpabs"] == 1) & (df["put_matchflag"] == mflags[f"{v}mflag"])
        #df.loc[df["bpabs"].isna() | df["modalre"].isna(), f"{leftShrt}{right}{ageVar}_ccbpabs_{v}"] = np.nan

        df[f"{v}"] = (df["put_matchflag"] == mflags[f"{v}mflag"])

    # tempfile mdata 
    # save `mdata'

    # unnecessary in Python because none of the operations we do will mutate df

    # * count over/under 21
    # use `mdata', clear
    # collapse (sum) `leftShrt'`right'`ageVar'_o21* `leftShrt'`right'`ageVar'_u21*, by(geoid_block)
    # tempfile overunder21
    # save `overunder21'
    res_overunder21 = [x for x in list(df) if re.match(f"{leftShrt}{right}{ageVar}_o21.*", x) or 
                                              re.match(f"{leftShrt}{right}{ageVar}_u21.*", x)]
    print(list(df))
    overunder21 = df.groupby('geoid_block').sum()[res_overunder21]
    print('overunder21', list(overunder21))

    # * count modal/nonmodal
    # use `mdata', clear
    # collapse (sum) `leftShrt'`right'`ageVar'_m_* `leftShrt'`right'`ageVar'_nm_*, by(geoid_block)
    # tempfile modalnonmodal 
    # save `modalnonmodal'
    res_modalnonmodal = [x for x in list(df) if re.match(f"{leftShrt}{right}{ageVar}_m_.*", x) or 
                                                re.match(f"{leftShrt}{right}{ageVar}_nm_.*", x)]
    modalnonmodal = df.groupby('geoid_block').sum()[res_modalnonmodal]

    # * count onetons
    # use `mdata', clear
    # collapse (sum) `leftShrt'`right'`ageVar'_sab* `leftShrt'`right'`ageVar'_ber*, by(geoid_block)
    # tempfile onetoncef 
    # save `onetoncef'
    res_onetoncef = [x for x in list(df) if re.match(f"{leftShrt}{right}{ageVar}_sab.*", x) or 
                                          re.match(f"{leftShrt}{right}{ageVar}_ber.*", x)]
    onetoncef = df.groupby('geoid_block')[res_onetoncef].sum()

    # * count modal/nonmodal onetons
    # use `mdata', clear
    # collapse (sum) `leftShrt'`right'`ageVar'_msab* `leftShrt'`right'`ageVar'_nmsab*, by(geoid_block)
    # tempfile modalonetoncef 
    # save `modalonetoncef'
    res_modalonetoncef = [x for x in list(df) if re.match(f"{leftShrt}{right}{ageVar}_msab.*", x) or 
                                                 re.match(f"{leftShrt}{right}{ageVar}_nmsab.*", x)]
    modalonetoncef = df.groupby('geoid_block')[res_modalonetoncef].sum()

    # * count exact and fzy using putative match flag, not confirm match flag
    # use `mdata', clear
    # collapse (sum) exact `ageVar', by(geoid_block)
    # tempfile allmatch
    # save `allmatch'
    allmatch = df.groupby('geoid_block').sum().reset_index()[["geoid_block", "exact", ageVar]]

    # * count CEF-CMRCL BPAS matches
    # use `mdata', clear
    # collapse (sum) `leftShrt'`right'`ageVar'_ccbpas_* `leftShrt'`right'`ageVar'_ccbpabs_*, by(geoid_block)
    # tempfile cefcmrclbpas
    # save `cefcmrclbpas'
    bpas_cols = [col for col in df.columns if col.startswith(f'{leftShrt}{right}{ageVar}_ccbpas')]
    bpabs_cols = [col for col in df.columns if col.startswith(f'{leftShrt}{right}{ageVar}_ccbpabs')]
    cefcmrclbpas = df.groupby("geoid_block").sum()[bpas_cols + bpabs_cols]
    cefcmrclbpas.to_csv(f"{temp}/cefcmrclbpas.csv")

    # * count matches
    # import delimited using "`resultsLoc'/`fileStem'_`left'`right'_cef.csv", clear stringcols(1 2 3) $trainingWheels
    df = get_df(f"{resultsLoc}/{fileStem}_{left}{right}_cef.csv", trainingWheels, dtype={"county":str, "tract":str, "block": str })
    print('df', list(df))
    #df.iloc[:, [0, 1, 2]] = df.iloc[:, [0, 1, 2]].astype(str)

    # gen geoid_block = county + tract + block
    df["geoid_block"] = df["county"] + df["tract"] + df["block"]

    # * drop the exact and fzy or binage count variables based upon the conf_matchflag, which tell you the type of match in the conf match. 
    # * we want to count by putative match type, stored in allmatch
    # if `fzy' == 1 {
    #     drop exact fzyage
    # }
    # else {
    #     drop exact fzyage binage
    # }
    if fzy == 1: 
        df = df.drop(["exact", "fzyage"], axis=1)
    else:
        df = df.drop(["exact", "fzyage", "binage"], axis=1)

    # merge 1:1 geoid_block using $temp/cefblocksize, keep(3) nogen
    # merge 1:1 geoid_block using `overunder21', keep(1 3) nogen
    # merge 1:1 geoid_block using `modalnonmodal', keep(1 3) nogen
    # merge 1:1 geoid_block using `onetoncef', keep(1 3) nogen
    # merge 1:1 geoid_block using `modalonetoncef', keep(1 3) nogen
    # merge 1:1 geoid_block using `allmatch', keep(1 3) nogen
    # merge 1:1 geoid_block using `cefcmrclbpas', keep(1 3) nogen
    df = pd.merge(df, pd.read_csv(f'{temp}/cefblocksize.csv', dtype={'geoid_block':str}), on=["geoid_block"], how="inner")
    df = pd.merge(df, overunder21, on=["geoid_block"], how="left")
    df = pd.merge(df, modalnonmodal, on=["geoid_block"], how="left")
    df = pd.merge(df, onetoncef, on=["geoid_block"], how="left")
    df = pd.merge(df, modalonetoncef, on=["geoid_block"], how="left")
    df = pd.merge(df, allmatch, on=["geoid_block"], how="left")
    df = pd.merge(df, cefcmrclbpas, on=["geoid_block"], how="left")
    print('df post merges', list(df))

    # * be sure all binage and fzyage are cumulative
    # replace `ageVar' = exact + `ageVar'
    # foreach i in o21 u21 m nm sab sabb ber msab msabb nmsab nmsabb ccbpas ccbpabs {
    #     replace `left'`right'`ageVar'_`i'_`ageVar' = `left'`right'`ageVar'_`i'_exact + `left'`right'`ageVar'_`i'_`ageVar'
    # }
    df[ageVar] = df["exact"] + df[ageVar]
    for i in ["o21", "u21", "m", "nm", "sab", "sabb", "ber", "msab", "msabb", "nmsab", "nmsabb", "ccbpas", "ccbpabs"]:
        df[f"{leftShrt}{right}{ageVar}_{i}_{ageVar}"] = df[f"{leftShrt}{right}{ageVar}_{i}_exact"] + df[f"{leftShrt}{right}{ageVar}_{i}_{ageVar}"]

    # * save block-level match counts
    # outsheet using "`resultsLoc'/`fileStem'_`leftShrt'`right'_cef_augmented.csv", replace 
    
    # fsspec error
    #df.to_csv(f"{resultsLoc}/{fileStem}_{leftShrt}{right}_cef_augmented.csv", index=False)

    # preserve 
    #     * strip off leftrightagevar prefix for national totals
    #     foreach v of varlist `leftShrt'`right'`ageVar'_* {
    #         local newName = subinstr("`v'","`leftShrt'`right'`ageVar'_","",.)
    #         rename `v' `newName'
    #     }
    #     collapse (sum) exact `ageVar' u21* o21* m_* nm_* sab* ber* nmsab* msab*
    #     gen lbl = "`leftShrt'-`right', `ageVar'"
    #     append using $temp/confirm_final
    #     save $temp/confirm_final, replace
    # restore
    confirm_final = df.copy()
    res_prefix = [x for x in list(confirm_final) if re.match(f"{leftShrt}{right}{ageVar}_.*", x)]
    for v in res_prefix:
        newName = v.replace(f"{leftShrt}{right}{ageVar}_", "")
        confirm_final = confirm_final.rename(columns={v: newName})

    res_prefix = [x for x in list(confirm_final) if re.match("u21.*", x) or
                                         re.match("o21.*", x) or
                                         re.match("m_.*", x) or 
                                         re.match("nm_.*", x) or
                                         re.match("sab.*", x) or
                                         re.match("ber.*", x) or 
                                         re.match("nmsab.*", x) or 
                                         re.match("msab.*", x) or
                                         re.match("ccbpas.*", x) or
                                         re.match("ccbpabs.*", x)]
    confirm_final = confirm_final[["exact", ageVar] + res_prefix].sum().to_frame().transpose()
    confirm_final['lbl'] = f"{leftShrt}-{right}, {ageVar}"

    print('confirm_final', list(confirm_final))
    if exists(f'{temp}/confirm_final.csv'):
        confirm_final = pd.concat([confirm_final, pd.read_csv(f'{temp}/confirm_final.csv')], ignore_index=True)
    confirm_final.to_csv(f'{temp}/confirm_final.csv', index=False)

    # preserve
    #     collapse (sum) exact `ageVar' `leftShrt'`right'`ageVar'_*, by(block_pop)
    #     foreach v of varlist exact `ageVar' {
    #         rename `v' `leftShrt'`right'`ageVar'_`v'
    #     }
    #     order block_pop
    #     merge 1:1 block_pop using $temp/confirmblksz_final, nogen
    #     save $temp/confirmblksz_final, replace
    # restore
    confirmblksz_final = df.groupby('block_pop').sum().reset_index()[["block_pop", "exact", ageVar] + [x for x in list(df) if re.match(f"{leftShrt}{right}{ageVar}_.*", x)]]
    res_prefix = [x for x in list(confirmblksz_final) if re.match(f"{ageVar}", x) or re.match("exact", x)]
    for v in res_prefix:
        confirmblksz_final = confirmblksz_final.rename(columns={v: f"{leftShrt}{right}{ageVar}_{v}"})

    cols = ["block_pop"] + [x for x in list(confirmblksz_final) if x != "block_pop"]
    confirmblksz_final = confirmblksz_final[cols]
    print('confirmblksz_final', list(confirmblksz_final))

    # Sort block_pop
    confirmblksz_final['sort'] = confirmblksz_final['block_pop'].str.extract('(\d+)(-|\+)', expand=False).iloc[:,0].astype(int)
    print(confirmblksz_final.sort)
    confirmblksz_final = confirmblksz_final.sort_values('sort')
    confirmblksz_final = confirmblksz_final.drop('sort', axis=1)
    print('confirmblksz final', confirmblksz_final)

    if exists(f'{temp}/confirmblksz_final.csv'):
        confirmblksz_final = confirmblksz_final.merge(pd.read_csv(f"{temp}/confirmblksz_final.csv"), on=["block_pop"], how='outer')
    confirmblksz_final.to_csv(f'{temp}/confirmblksz_final.csv', index=False)

    # collapse (sum) exact `ageVar' `leftShrt'`right'`ageVar'_*, by(solvar)
    # foreach v of varlist exact `ageVar' {
    #     rename `v' `leftShrt'`right'`ageVar'_`v'
    # }
    # order solvar
    # merge 1:1 solvar using $temp/confirmsolvar_final, nogen
    # save $temp/confirmsolvar_final, replace
    confirmsolvar_final = df.groupby('solvar').sum().reset_index()[["solvar", "exact", ageVar] + [x for x in list(df) if re.match(f"{leftShrt}{right}{ageVar}_.*", x)]]
    res_prefix = [x for x in list(confirmsolvar_final) if re.match(f"{ageVar}", x) or re.match("exact", x)]
    for v in res_prefix:
        confirmsolvar_final = confirmsolvar_final.rename(columns={v: f"{leftShrt}{right}{ageVar}_{v}"})

    cols = ["solvar"] + [x for x in list(confirmsolvar_final) if x != "solvar"]
    confirmsolvar_final = confirmsolvar_final[cols]
    print('confirmsolvar_final cols', list(confirmsolvar_final))

    if exists(f'{temp}/confirmsolvar_final.csv'):
        confirmsolvar_final = confirmsolvar_final.merge(pd.read_csv(f"{temp}/confirmsolvar_final.csv", dtype={'solvar':str}), on=["solvar"], how='outer')
    confirmsolvar_final.to_csv(f'{temp}/confirmsolvar_final.csv', index=False)

    # shell rm `mdata'
    # TODO: idk what this is 

# *--------------------------------------------------
# * CONFIRM COUNTS
# *--------------------------------------------------

def main():
    # clear
    # set obs 1
    # gen lbl = ""
    # save $temp/confirm_final, replace
    # clear
    # set obs 1
    # gen block_pop = .
    # save $temp/confirmblksz_final, replace
    # clear
    # set obs 1
    # gen solvar = ""
    # save $temp/confirmsolvar_final, replace

    '''
    confirm_final = pd.DataFrame()
    confirm_final.append(pd.Series(), ignore_index=True)
    confirm_final["lbl"] = ""
    confirm_final.to_csv(f'{temp}/confirm_final.csv')

    confirmblksz_final = pd.DataFrame()
    confirmblksz_final.append(pd.Series(), ignore_index=True)
    confirmblksz_final["block_pop"] = np.nan
    confirmblksz_final.to_csv(f'{temp}/confirmblksz_final.csv')

    confirmsolvar_final = pd.DataFrame()
    confirmsolvar_final.append(pd.Series(), ignore_index=True)
    confirmsolvar_final["solvar"] = ""
    confirmsolvar_final.to_csv(f'{temp}/confirmsolvar_final.csv')
    '''

    # di c(current_date) " " c(current_time)
    # di "#### START OF CONFIRM STATS ####"
    now = datetime.now()
    print(now)
    print("#### START OF CONFIRM STATS ####")

    # if $putconf_rhdf_cmrcl_fz == 1 {
    #     confirmStats "$rhdf" "cmrcl" 1
    # }
    if putconf_rhdf_cmrcl_fz == 1: 
        confirmStats(f"{rhdf}", "cmrcl", 1)

    # if $putconf_rhdf_cef_fz == 1 {
    #     confirmStats "$rhdf" "cef" 1
    # }
    if putconf_rhdf_cef_fz == 1: 
        confirmStats(f"{rhdf}", "cef", 1)

    # if $putconf_rhdf_cmrcl_bn == 1 {
    #     confirmStats "$rhdf" "cmrcl" 0
    # }
    if putconf_rhdf_cmrcl_bn == 1: 
        confirmStats(f"{rhdf}", "cmrcl", 0)

    # if $putconf_rhdf_cef_bn == 1 {
    #     confirmStats "$rhdf" "cef" 0
    # }
    if putconf_rhdf_cef_bn == 1: 
        confirmStats(f"{rhdf}", "cef", 0)

    # if $putconf_cef_cef_bn == 1 {
    #     confirmStats "cef" "cef" 0
    # }
    if putconf_cef_cef_bn == 1: 
        confirmStats("cef", "cef", 0)

    # if $putconf_cef_cmrcl_bn == 1 {
    #     confirmStats "cef" "cmrcl" 0
    # } 
    if putconf_cef_cmrcl_bn == 1: 
        confirmStats("cef", "cmrcl", 0)

    # if $putconf_hdf_cef_bn == 1 {
    #     confirmStats "hdf" "cef" 0
    # }
    if putconf_hdf_cef_bn == 1: 
        confirmStats("hdf", "cef", 0)

    # if $putconf_hdf_cmrcl_bn == 1 {
    #     confirmStats "hdf" "cmrcl" 0
    # }
    if putconf_hdf_cmrcl_bn == 1:
        confirmStats("hdf", "cmrcl", 0)

    # if $putconf_rhdfbo_cef_bn == 1 {
    #     confirmStats "$rhdfbo" "cef" 0
    # }
    if putconf_rhdfbo_cef_bn == 1: 
        confirmStats(f"{rhdfbo}", "cef", 0)

    # if $putconf_rhdfbo_cmrcl_bn == 1 {
    #     confirmStats "$rhdfbo" "cmrcl" 0
    # }
    if putconf_rhdfbo_cmrcl_bn == 1:
        confirmStats(rhdfbo, "cmrcl", 0)

    # if $putconf_rmdf_cef_bn == 1 {
    #     confirmStats "$rmdf" "cef" 0
    # }
    if putconf_rmdf_cef_bn == 1:
        confirmStats(rmdf, "cef", 0)

    # if $putconf_rmdf_cmrcl_bn == 1 {
    #     confirmStats "$rmdf" "cmrcl" 0
    # }
    if putconf_rmdf_cmrcl_bn == 1:
        confirmStats(rmdf, "cmrcl", 0)

    # if $putconf_mdf_cef_bn == 1 {
    #     confirmStats "$mdf" "cef" 0
    # }
    if putconf_mdf_cef_bn == 1:
        confirmStats(mdf, "cef", 0)

    # if $putconf_mdf_cmrcl_bn == 1 {
    #     confirmStats "$mdf" "cmrcl" 0
    # }
    if putconf_mdf_cmrcl_bn == 1:
        confirmStats(mdf, "cmrcl", 0)

    #if $putconf_modalGsr_cef_bn == 1 {
    #    confirmStats "modalGsr" "cef" 0
    #}
    if putconf_modalGsr_cef_bn == 1:
        confirmStats("modalGsr", "cef", 0)

    #if $putconf_modalGsr_cmrcl_bn == 1 {
    #    confirmStats "modalGsr" "cmrcl" 0
    #}
    if putconf_modalGsr_cmrcl_bn == 1:
        confirmStats("modalGsr", "cmrcl", 0)

    #if $putconf_propGsr_cef_bn == 1 {
    #    confirmStats "propGsr" "cef" 0
    #}
    if putconf_propGsr_cef_bn == 1:
        confirmStats("propGsr", "cef", 0)

    #if $putconf_propGsr_cmrcl_bn == 1 {
    #    confirmStats "propGsr" "cmrcl" 0
    #}
    if putconf_propGsr_cmrcl_bn == 1:
        confirmStats("propGsr", "cmrcl", 0)

    # use `cefpopnat', clear
    # local cefnatpop = cefpop[1]
    # gen all=1
    # tempfile denominators
    # save `denominators'
    # use `rhdfpopnat', clear
    # local rhdfnatpop = rhdfpop[1]
   
    cefpopnat = pd.read_csv(f'{temp}/cefpopnat.csv')
    print('cefpopnat', cefpopnat.columns)
    cefnatpop = cefpopnat.cefpop[0]
    cefpopnat["all"] = 1
    denominators = cefpopnat

    rhdfpopnat = pd.read_csv(f'{temp}/rhdfpopnat.csv')
    rhdfnatpop = rhdfpopnat.rhdfpop[0]

    # * SAVE NATIONAL CONFIRM STATS
    # use $temp/confirm_final, clear
    # gen all= 1
    # merge m:1 all using `denominators', keep(1 3) nogen
    # drop all
    # gen incef = `cefnatpop'
    # gen inrecon = `rhdfnatpop'
    confirm_final = pd.read_csv(f'{temp}/confirm_final.csv')
    confirm_final["all"] = 1
    confirm_final = pd.merge(confirm_final, denominators, on=["all"], how="left")

    confirm_final = confirm_final.drop('all', axis=1)
    confirm_final["incef"] = cefnatpop
    confirm_final["inrecon"] = rhdfnatpop

    # if $putconf_rhdf_cmrcl_fz == 1 | $putconf_rhdf_cef_fz==1 {
    #     order lbl incef inrecon exact fzyage binage
    #     format incef inrecon exact fzyage binage %20.0gc
    # }
    # else {
    #     order lbl incef inrecon exact binage
    #     format incef inrecon exact binage %20.0gc
    # }
    if putconf_rhdf_cmrcl_fz == 1 or putconf_rhdf_cef_fz == 1: 
        order = ["lbl", "incef", "inrecon", "exact", "fzyage", "binage"]
        cols = order + [x for x in list(confirm_final) if x not in order]
        confirm_final = confirm_final[cols]
        for v in order[1:]: 
            confirm_final[v].map('{:,}'.format)
    else: 
        order = ["lbl", "incef", "inrecon", "exact", "binage"]
        cols = order + [x for x in list(confirm_final) if x not in order]
        confirm_final = confirm_final[cols]
        for v in order[1:]: 
            confirm_final[v].map('{:,}'.format)

    # drop if missing(lbl)
    # export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(confirm) firstrow(variables) sheetmodify

    confirm_final = confirm_final.dropna(subset=["lbl"])
    print("confirm_final excel", list(confirm_final))
    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004.xlsx", mode="a", if_sheet_exists="replace") as writer:
        confirm_final.to_excel(writer, sheet_name="confirm", index=False)


    # * SAVE BLOCK SIZE CONFIRM STATS
    # use $temp/confirmblksz_final, clear
    # merge 1:1 block_pop using `cefpopblocksz', nogen 
    # order block_pop cefpop
    confirmblksz_final = pd.read_csv(f'{temp}/confirmblksz_final.csv')
    cefpopblocksz = pd.read_csv(f'{temp}/cefpopblocksz.csv')
    print('confirmblksz_final', list(confirmblksz_final))
    print('cefpopblocksz', list(cefpopblocksz))
    
    confirmblksz_final = pd.merge(confirmblksz_final, cefpopblocksz, on=["block_pop"])

    order = ["block_pop", "cefpop"]
    cols = order + [x for x in list(confirmblksz_final) if x not in order]
    confirmblksz_final = confirmblksz_final[cols]

    # if $putconf_rhdf_cmrcl_fz == 1 | $putconf_rhdf_cef_fz==1 {
    #     format *_exact *_fzyage *_binage  %20.0gc
    # }
    # else {
    #     format *_exact *_binage %20.0gc
    # }
    if putconf_rhdf_cmrcl_fz == 1 or putconf_rhdf_cef_fz == 1: 
        for v in [x for x in list(confirmblksz_final) if re.match(".*_exact", x) or re.match(".*_fzyage", x) or re.match(".*_binage", x)]:
            confirmblksz_final[v].map('{:,}'.format)
    else:
        for v in [x for x in list(confirmblksz_final) if re.match(".*_exact", x) or re.match(".*_binage", x)]:
            confirmblksz_final[v].map('{:,}'.format)

    # drop if missing(block_pop)
    # export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(confirm_blksz) firstrow(variables) sheetmodify
    confirmblksz_final = confirmblksz_final.dropna(subset=["block_pop"])
    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004.xlsx", mode="a", if_sheet_exists="replace") as writer:
        confirmblksz_final.to_excel(writer, sheet_name="confirm_blksz", index=False)


    # * SAVE SOLVAR CONFIRM STATS
    # use $temp/confirmsolvar_final, clear
    # merge 1:1 solvar using `solvarpop', nogen 
    # order solvar cefpop keepblockpik
    confirmsolvar_final = pd.read_csv(f'{temp}/confirmsolvar_final.csv', dtype={'solvar':str})
    solvarpop = pd.read_csv(f'{temp}/solvarpop.csv')
    confirmsolvar_final = pd.merge(confirmsolvar_final, solvarpop, on=["solvar"])


    order = ["solvar", "cefpop", "keepblockpik"]
    cols = order + [x for x in list(confirmsolvar_final) if x not in order]
    confirmsolvar_final = confirmsolvar_final[cols]

    # if $putconf_rhdf_cmrcl_fz == 1 | $putconf_rhdf_cef_fz==1 {
    #     format *_exact *_fzyage *_binage  %20.0gc
    # }
    # else {
    #     format *_exact *_binage %20.0gc
    # }
    if putconf_rhdf_cmrcl_fz == 1 or putconf_rhdf_cef_fz == 1: 
        for v in [x for x in list(confirmsolvar_final) if re.match(".*_exact", x) or re.match(".*_fzyage", x) or re.match(".*_binage", x)]:
            confirmsolvar_final[v].map('{:,}'.format)
    else:
        for v in [x for x in list(confirmsolvar_final) if re.match(".*_exact", x) or re.match(".*_binage", x)]:
            confirmsolvar_final[v].map('{:,}'.format)

    # drop if missing(solvar)
    # export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(confirm_solvar) firstrow(variables) sheetmodify
    confirmsolvar_final = confirmsolvar_final.dropna(subset=["solvar"])
    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004.xlsx", mode="a", if_sheet_exists="replace") as writer:
        confirmsolvar_final.to_excel(writer, sheet_name="confirm_solvar", index=False)


    # di "#### END OF CONFIRM STATS ####"
    # di c(current_date) " " c(current_time)
    print("#### END OF CONFIRM STATS ####")
    now = datetime.now()
    print(now)

if __name__ == "__main__":
    main()
