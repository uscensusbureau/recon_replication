# putative_stats.py
# -----------------------------
# /usr/bin/env python3 putative_stats.py
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

# Description:
'''
putative_stats.py:

This code is a translation of existing research STATA code developed by MITRE for the purposes of testing
the corresponding reidentification ouputs. This python file is the translation of "2.putative_stats.do", responsible
for computing putative reidentification statistics.

Previous STATA Code Header:
[There was no code header]
'''

# -----------------------------
# Imports
# -----------------------------
from config import *
from datetime import datetime
import pandas as pd
import numpy as np
from os.path import exists
import re

#--------------------------------------------------
# Function: putativeStats
#--------------------------------------------------
# Input:
#   -left:
#   -right:
#   -fzy: 1 if fuzzy age was used, fzy=0 if binage was used
#
# Output:
#   A dataframe read from the provided path
#
# Previous STATA Code is shown in chunks below, commented inline alongside Python code

def putativeStats(left, right, fzy): # function definition
    # fzy = 1 then fuzzy age was used, if fzy=0, then binage was used
    
    # determine location of results
    # if "`left'"=="$rhdf" | "`left'"=="$rhdfbo" | "`left'"=="$rmdf" | "`left'"=="$mdf"   {
    #     local resultsLoc = "$rsltbase/rhdf/`left'/"
    # }
    # else {
    #     if regexm("`left'","Gsr") {
    #         local resultsLoc = "$rsltbase/simul/"
    #     }
    #     else {
    #         local resultsLoc = "$rsltbase/`left'/"
    #     }
    # }
    if left == rhdf or left == rhdfbo or left == rmdf or left == mdf:
        resultsLoc = f"{rsltbase}/rhdf/{left}"
    elif "Gsr" in left:
        resultsLoc =  f"{rsltbase}/simul"
    else:
        resultsLoc = f"{rsltbase}/{left}"

    # if `fzy' == 1 {
    #     local fileStem = "putative"
    #     local ageVar = "fzyage"
    #     local matchFileStem = "putativematch"
    # }
    # else {
    #     local fileStem = "putativebinage"
    #     local ageVar = "binage"
    #     local matchFileStem = "putativematchbinage"
    # }

    if fzy == 1:
        fileStem = "putative"
        ageVar = "fzyage"
        matchFileStem = "putativematch"
    else:
        fileStem = "putativebinage"
        ageVar = "binage"
        matchFileStem = "putativematchbinage"

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

    # match flag variables
    # local exactmflag = 1
    # local fzyagemflag = 2
    # local binagemflag = 3
    # local oneoffmflag = 4

    mflags = {
        "exact": 1,
        "fzyage": 2,
        "binage": 3,
        "oneoff": 4
    }

    #di "left: `left'"
    #di "right: `right'"
    #di "rhdf: $rhdf"
    #di "rhdfbo: $rhdfbo"
    #di "resultsLoc: `resultsLoc'"
    #di "fileStem: `fileStem'"
    #di "ageVar: `ageVar'"

    print(f"left: {left}")
    print(f"right: {right}")
    print(f"rhdf: {rhdf}")
    print(f"rhdfbo: {rhdfbo}")
    print(f"resultsLoc: {resultsLoc}")
    print(f"fileStem: {fileStem}")
    print(f"ageVar: {ageVar}")

    # import delimited using "`resultsLoc'/`matchFileStem'_`left'_`right'.csv", clear stringcols(1 2 3 13) $trainingWheels
    # gen geoid_block = county + tract + block * this line defines a geoID variable
    # merge 1:1 geoid_block pik using $temp/cef_over21_modalre_oneton, keep(1 3) nogen
    # merge 1:1 geoid_block pik using $temp/cmrcl_cef_bpas_match, keep(1 3) nogen     

    print(f'{resultsLoc}/{matchFileStem}_{left}_{right}.csv')
    df = get_df(f'{resultsLoc}/{matchFileStem}_{left}_{right}.csv', trainingWheels, dtype={"county":str, "tract":str, "block": str, "pik": str })
    #df.iloc[:, [0, 1, 2, 12]] = df.iloc[:, [0, 1, 2, 12]].astype(str)
    df["geoid_block"] = df["county"] + df["tract"] + df["block"]

    cef_over21_modalre_oneton = pd.read_csv(f'{temp}/cef_over21_modalre_oneton.csv', dtype={'geoid_block':str, 'pik':str})
    df = pd.merge(df, cef_over21_modalre_oneton, on=["geoid_block", "pik"], how="left")
    cmrcl_cef_bpas_match = pd.read_csv(f'{temp}/cmrcl_cef_bpas_match.csv', dtype={'geoid_block':str, 'pik':str})
    df = pd.merge(df, cmrcl_cef_bpas_match, on=["geoid_block", "pik"], how="left")

    #foreach v in exact `ageVar' {
    for v in ["exact", ageVar]:
        # over/under 21
        # creates a boolean column for whether or not a particular row corresponds to an individual over/under 21 (1/0) respectively

        #gen `leftShrt'`right'`ageVar'_o21_`v' = over21==1  & put_matchflag==``v'mflag'
        #replace `leftShrt'`right'`ageVar'_o21_`v' = . if missing(over21)
        #gen `leftShrt'`right'`ageVar'_u21_`v' = over21==0 & put_matchflag==``v'mflag'
        #replace `leftShrt'`right'`ageVar'_u21_`v' = . if missing(over21)
        df[f"{leftShrt}{right}{ageVar}_o21_{v}"] = (df["over21"] == 1) & (df["put_matchflag"] == mflags[v]) # v refers to a column in df

        df[f"{leftShrt}{right}{ageVar}_u21_{v}"] = (df["over21"] == 0) & (df["put_matchflag"] == mflags[v])

        #* modal/nonmodal
        # gen `leftShrt'`right'`ageVar'_m_`v' = modalre==1 & put_matchflag==``v'mflag'
        # replace `leftShrt'`right'`ageVar'_m_`v' = . if missing(modalre)
        # gen `leftShrt'`right'`ageVar'_nm_`v' = modalre==0 & put_matchflag==``v'mflag'
        # replace `leftShrt'`right'`ageVar'_nm_`v' = . if missing(modalre)

        df[f"{leftShrt}{right}{ageVar}_m_{v}"] = (df["modalre"] == 1) & (df["put_matchflag"] == mflags[v])

        df[f"{leftShrt}{right}{ageVar}_nm_{v}"] = (df["modalre"] == 0) & (df["put_matchflag"] == mflags[v])

        #* onetons
        #* oneton is a person who has a unique combination of attributes within their geographic area
        #* this for loop creates a boolean true for which sab_oneton / sabb_oneton / ber_oneton is 1, to be summed later
        #foreach ton in sab_oneton sabb_oneton ber_oneton {
        for ton in ["sab_oneton", "sabb_oneton", "ber_oneton"]:
            # local tonabrv = subinstr("`ton'","_oneton","",.)
            # gen `leftShrt'`right'`ageVar'_`tonabrv'_`v' = `ton'==1 & put_matchflag==``v'mflag'
            # replace `leftShrt'`right'`ageVar'_`tonabrv'_`v' = . if missing(`ton')
            
            tonabrv = ton.replace("_oneton", "")
            df[f"{leftShrt}{right}{ageVar}_{tonabrv}_{v}"] = (df[ton] == 1) & (df["put_matchflag"] == mflags[v])

        #* onetons by modal/nonmodal
        #* this for loop is like the one above but tests also for modalre==1
        #foreach ton in sab_oneton sabb_oneton {
        for ton in ["sab_oneton", "sabb_oneton"]:
            # local tonabrv = subinstr("`ton'","_oneton","",.)
            # gen `leftShrt'`right'`ageVar'_m`tonabrv'_`v' = `ton'==1 & modalre==1 & put_matchflag==``v'mflag'
            # replace `leftShrt'`right'`ageVar'_m`tonabrv'_`v' = . if missing(`ton') | missing(modalre)
            # gen `leftShrt'`right'`ageVar'_nm`tonabrv'_`v' = `ton'==1 & modalre==0 & put_matchflag==``v'mflag'
            # replace `leftShrt'`right'`ageVar'_nm`tonabrv'_`v' = . if missing(`ton') | missing(modalre)
    
            tonabrv = ton.replace("_oneton","")
            df[f"{leftShrt}{right}{ageVar}_m{tonabrv}_{v}"] = (df[ton] == 1) & (df["modalre"] == 1) & (df["put_matchflag"] == mflags[v])
            df[f"{leftShrt}{right}{ageVar}_nm{tonabrv}_{v}"] = (df[ton] == 1) & (df["modalre"] == 0) & (df["put_matchflag"] == mflags[v])

        # gen `leftShrt'`right'`ageVar'_ccbpas_`v' = bpas==1  & put_matchflag==``v'mflag'
        # replace `leftShrt'`right'`ageVar'_ccbpas_`v' = . if missing(bpas)
        # gen `leftShrt'`right'`ageVar'_ccbpabs_`v' = bpabs==1  & put_matchflag==``v'mflag'
        # replace `leftShrt'`right'`ageVar'_ccbpabs_`v' = . if missing(bpabs)
        df[f"{leftShrt}{right}{ageVar}_ccbpas_{v}"] = (df["bpas"] == 1) & (df["put_matchflag"] == mflags[v])
        df[f"{leftShrt}{right}{ageVar}_ccbpabs_{v}"] = (df["bpabs"] == 1) & (df["put_matchflag"] == mflags[v])
        

    # lines 91-116 simply save sums/counts to a temporary file
    # tempfile mdata
    # save `mdata'
    df.to_csv(f"{temp}/mdata.csv") 

    # * count over/under 21
    # use `mdata', clear
    # collapse (sum) `leftShrt'`right'`ageVar'_o21* `leftShrt'`right'`ageVar'_u21*, by(geoid_block)
    # tempfile overunder21
    # save `overunder21'
    over_21_cols = [col for col in df.columns if col.startswith(f'{leftShrt}{right}{ageVar}_o21')]
    under_21_cols = [col for col in df.columns if col.startswith(f'{leftShrt}{right}{ageVar}_u21')]
    print(list(df))
    print(list(df.groupby("geoid_block").sum()))
    summed_21_series = df.groupby("geoid_block").sum().reset_index()[over_21_cols + under_21_cols + ["geoid_block"]]
    print('summed_21_series', list(summed_21_series))
    # summed_21_series.to_csv(f"{temp}/overunder21.csv")

    #* count modal/nonmodal
    # use `mdata', clear
    # collapse (sum) `leftShrt'`right'`ageVar'_m_* `leftShrt'`right'`ageVar'_nm_*, by(geoid_block)
    # tempfile modalnonmodal
    # save `modalnonmodal'
    modal_cols = [col for col in df.columns if col.startswith(f'{leftShrt}{right}{ageVar}_m_')]
    nonmodal_cols = [col for col in df.columns if col.startswith(f'{leftShrt}{right}{ageVar}_nm_')]
    modalnonmodal_series = df.groupby("geoid_block").sum()[modal_cols + nonmodal_cols]
    # modalnonmodal_series.to_csv(f"{temp}/modalnonmodal.csv")

    #* count onetons
    #use `mdata', clear
    #collapse (sum) `leftShrt'`right'`ageVar'_sab* `leftShrt'`right'`ageVar'_ber*, by(geoid_block)
    #tempfile onetoncef
    #save `onetoncef'
    sab_cols = [col for col in df.columns if col.startswith(f'{leftShrt}{right}{ageVar}_sab')]
    ber_cols = [col for col in df.columns if col.startswith(f'{leftShrt}{right}{ageVar}_ber')]
    onetoncef_series = df.groupby("geoid_block").sum()[sab_cols + ber_cols]
    # onetoncef_series.to_csv(f"{temp}/onetoncef.csv")

    #* count modal/nonmodal onetons
    #use `mdata', clear
    #collapse (sum) `leftShrt'`right'`ageVar'_msab* `leftShrt'`right'`ageVar'_nmsab*, by(geoid_block)
    #tempfile modalonetoncef
    #save `modalonetoncef'
    modalsab_cols = [col for col in df.columns if col.startswith(f'{leftShrt}{right}{ageVar}_msab')]
    nonmodalsab_cols = [col for col in df.columns if col.startswith(f'{leftShrt}{right}{ageVar}_nmsab')]
    modalonetoncef_series = df.groupby("geoid_block").sum()[modalsab_cols + nonmodalsab_cols]
    # modalonetoncef_series.to_csv(f"{temp}/modalonetoncef.csv")

    # * count CEF-CMRCL BPAS matches
    # use `mdata', clear
    # collapse (sum) `leftShrt'`right'`ageVar'_ccbpas_* `leftShrt'`right'`ageVar'_ccbpabs_*, by(geoid_block)
    # tempfile cefcmrclbpas 
    # save `cefcmrclbpas'
    bpas_cols = [col for col in df.columns if col.startswith(f'{leftShrt}{right}{ageVar}_ccbpas')]
    bpabs_cols = [col for col in df.columns if col.startswith(f'{leftShrt}{right}{ageVar}_ccbpabs')]
    cefcmrclbpas_series = df.groupby("geoid_block").sum()[bpas_cols + bpabs_cols]
    # cefcmrclbpas_series.to_csv(f"{temp}/cefcmrclbpas.csv")

    #* count matches
    # import delimited using "`resultsLoc'/`fileStem'_`left'_`right'.csv", clear stringcols(1 2 3) $trainingWheels
    # gen geoid_block = county + tract + block * this line appends a blockID to existing geoID
    
    df = get_df(f"{resultsLoc}/{fileStem}_{left}_{right}.csv", trainingWheels, dtype={"county":str, "tract":str, "block": str, "pik": str })
    #df.iloc[:, [0, 1, 2]] = df.iloc[:, [0, 1, 2]].astype(str)
    df['geoid_block'] = df['county'] + df['tract'] + df['block'] 

    # merge 1:1 geoid_block using $temp/cefblocksize, keep(3) nogen
    # merge 1:1 geoid_block using `overunder21', keep(1 3) nogen
    # merge 1:1 geoid_block using `modalnonmodal', keep(1 3) nogen
    # merge 1:1 geoid_block using `onetoncef', keep(1 3) nogen
    # merge 1:1 geoid_block using `modalonetoncef', keep(1 3) nogen
    print('df', list(df))
    read_in_df = pd.read_csv(f"{temp}/cefblocksize.csv", dtype={'geoid_block':str})


    df = df.merge(read_in_df, on=["geoid_block"], how='inner')
    print('summed_21_series', list(summed_21_series))

    df = df.merge(summed_21_series, on=["geoid_block"], how='left')
    df = df.merge(modalnonmodal_series, on=["geoid_block"], how='left')
    df = df.merge(onetoncef_series, on=["geoid_block"], how='left')
    df = df.merge(modalonetoncef_series, on=["geoid_block"], how='left')
    df = df.merge(cefcmrclbpas_series, on=["geoid_block"], how='left')    
    print('df post merges', list(df))

    # * be sure all binage and fzyage are cumulative
    # replace `ageVar' = exact + `ageVar'
    df[ageVar] = df["exact"] + df[ageVar]
    
    #foreach i in o21 u21 m nm sab sabb ber msab msabb nmsab nmsabb 
    for i in ['o21', 'u21', 'm', 'nm', 'sab', 'sabb', 'ber', 'msab', 'msabb', 'nmsab', 'nmsabb', 'ccbpas', 'ccbpabs']:
        # replace `leftShrt'`right'`ageVar'_`i'_`ageVar' = `leftShrt'`right'`ageVar'_`i'_exact + `leftShrt'`right'`ageVar'_`i'_`ageVar'
        df[f'{leftShrt}{right}{ageVar}_{i}_{ageVar}'] = df[f'{leftShrt}{right}{ageVar}_{i}_exact'] + df[f'{leftShrt}{right}{ageVar}_{i}_{ageVar}']
    #} 
    #* save block-level match counts

    # outsheet using "`resultsLoc'/`fileStem'_`leftShrt'_`right'_augmented.csv", replace
    #df.to_csv(f"{resultsLoc}/{fileStem}_{leftShrt}_{right}_augmented.csv", index=False)
    
    #preserve
    #* strip off leftrightagevar prefix for national totals
    # foreach v of varlist `leftShrt'`right'`ageVar'_* {    
    #    local newName = subinstr("`v'","`leftShrt'`right'`ageVar'_","",.)
    #    rename `v' `newName'
    # }
    prefix_regex = re.compile(f'^{leftShrt}{right}{ageVar}_')
    putative_final = df.copy()
    test_regex = re.compile(f'^_')
    putative_final = putative_final.rename(lambda n: re.sub(prefix_regex, "", n), axis=1)

    # collapse (sum) exact `ageVar' u21* o21* m_* nm_* sab* ber* nmsab* msab* ccbpas* ccbpabs*
    # gen lbl = "`leftShrt'-`right', `ageVar'" * saving consistent totals upon string extraction
    # append using $temp/putative_final
    # save $temp/putative_final, replace

    cols_to_sum = [col for col in putative_final.columns if col=="exact" or col==ageVar or col.startswith(('u21', 'o21', 'm_', 'nm_', 'sab', 'ber', 'nmsab', 'msab', 'ccbpas', 'ccbpabs'))]
    putative_final = putative_final[cols_to_sum].sum().to_frame().transpose()
    putative_final["lbl"] = f"{leftShrt}-{right}, {ageVar}"
    
    print('putativefinal', list(putative_final))
    if exists(f'{temp}/putative_final.csv'):
        putative_final = pd.concat([putative_final, pd.read_csv(f'{temp}/putative_final.csv')], ignore_index=True)
    putative_final.to_csv(f"{temp}/putative_final.csv", index=False)

    # restore
    # preserve
    # collapse (sum) exact `ageVar' `leftShrt'`right'`ageVar'_*, by(block_pop)
    # foreach v of varlist exact `ageVar' {
    #     rename `v' `leftShrt'`right'`ageVar'_`v'
    # }
    # order block_pop
    # merge 1:1 block_pop using $temp/putativeblksz_final, nogen
    # save $temp/putativeblksz_final, replace
    
    putativeblksz_final = df.groupby('block_pop').sum().reset_index()[["block_pop", "exact", ageVar] + [x for x in list(df) if re.match(f"{leftShrt}{right}{ageVar}_.*", x)]]
    res_prefix = [x for x in list(putativeblksz_final) if re.match(f"{ageVar}", x) or re.match("exact", x)]
    for v in res_prefix:
        putativeblksz_final = putativeblksz_final.rename(columns={v: f"{leftShrt}{right}{ageVar}_{v}"})

    # Sort block_pop
    putativeblksz_final['sort'] = putativeblksz_final['block_pop'].str.extract('(\d+)(-|\+)', expand=False).iloc[:,0].astype(int)
    print(putativeblksz_final.sort)
    putativeblksz_final = putativeblksz_final.sort_values('sort')
    putativeblksz_final = putativeblksz_final.drop('sort', axis=1)
    print('putativeblksz final', putativeblksz_final)


    print('putativeblkszfinal', list(putativeblksz_final))
    cols = ["block_pop"] + [x for x in list(putativeblksz_final) if x != "block_pop"]
    putativeblksz_final = putativeblksz_final[cols]

    if exists(f'{temp}/putativeblksz_final.csv'):
        putativeblksz_final = putativeblksz_final.merge(pd.read_csv(f"{temp}/putativeblksz_final.csv"), on=["block_pop"], how='outer')
    putativeblksz_final.to_csv(f'{temp}/putativeblksz_final.csv', index=False)

    # restore
    # collapse (sum) exact `ageVar' `leftShrt'`right'`ageVar'_*, by(solvar) 
    # foreach v of varlist exact `ageVar' {
    #     rename `v' `leftShrt'`right'`ageVar'_`v'
    # }
    # order solvar * order based on stat type
    # merge 1:1 solvar using $temp/putativesolvar_final, nogen
    # save $temp/putativesolvar_final, replace

    cols_to_sum = [col for col in df.columns if col=="exact" or col==ageVar or col.startswith(f"{leftShrt}{right}{ageVar}_")]
    putativesolvar_final = df.groupby("solvar").sum().reset_index()[['solvar'] + cols_to_sum]
    putativesolvar_final = putativesolvar_final.rename(columns={"exact": f"{leftShrt}{right}{ageVar}_exact", 
                                          ageVar: f"{leftShrt}{right}{ageVar}_{ageVar}"})

    print('putativesolvarfinal', list(putativesolvar_final))
    if exists(f'{temp}/putativesolvar_final.csv'):
        putativesolvar_final = putativesolvar_final.merge(pd.read_csv(f"{temp}/putativesolvar_final.csv", dtype={'solvar':str}), on=["solvar"], how='outer')
    putativesolvar_final.to_csv(f"{temp}/putativesolvar_final.csv", index=False)
    # shell rm `mdata'

#end

# *--------------------------------------------------
# * PUTATIVE RATES
# *--------------------------------------------------
def main():
    # di c(current_date) " " c(current_time)
    now = datetime.now()
    print(now)
    print("#### START OF PUTATIVE STATS ####")
    
    # * create blank container datasets
    # clear
    # set obs 1
    # gen lbl = ""
    # save $temp/putative_final, replace
    # 
    # clear
    # set obs 1
    # gen block_pop = .
    # save $temp/putativeblksz_final, replace
    # 
    # clear
    # set obs 1
    # gen solvar = ""
    # save $temp/putativesolvar_final, replace

    '''
    df = pd.DataFrame()
    df.append(pd.Series(), ignore_index=True)
    df["lbl"] = ""
    putative_final = df.copy()
    df.to_csv(f"{temp}/putative_final.csv")

    df = pd.DataFrame()
    df.append(pd.Series(), ignore_index=True)
    df["block_pop"] = np.nan
    putativeblksz_final = df.copy()
    df.to_csv(f"{temp}/putativeblksz_final.csv")

    df = pd.DataFrame()
    df.append(pd.Series(), ignore_index=True)
    df["solvar"] = ""
    putativesolvar_final = df.copy()
    df.to_csv(f"{temp}/putativesolvar_final.csv")
    '''

    #   if $putconf_rhdf_cmrcl_fz == 1 {
    #       putativeStats "$rhdf" "cmrcl" 1
    #   }
    if putconf_rhdf_cmrcl_fz == 1:
        putativeStats(f"{rhdf}", "cmrcl", 1)

    #   if $putconf_rhdf_cef_fz == 1 {
    #       putativeStats "$rhdf" "cef" 1
    #   }
    if putconf_rhdf_cef_fz == 1:
        putativeStats(f"{rhdf}", "cef", 1)

    #    if $putconf_rhdf_cmrcl_bn == 1 {
    #      putativeStats "$rhdf" "cmrcl" 0
    #   }
    if putconf_rhdf_cmrcl_bn == 1:
        putativeStats(f"{rhdf}", "cmrcl", 0)

    #   if $putconf_rhdf_cef_bn == 1 {
    #      putativeStats "$rhdf" "cef" 0
    #    }
    if putconf_rhdf_cef_bn == 1:
        putativeStats(f"{rhdf}", "cef", 0)

    #   if $putconf_cef_cef_bn == 1 {
    #      putativeStats "cef" "cef" 0
    #    }
    if putconf_cef_cef_bn == 1:
        putativeStats("cef", "cef", 0)

    #    if $putconf_cef_cmrcl_bn == 1 {
    #      putativeStats "cef" "cmrcl" 0
    #    }
    if putconf_cef_cmrcl_bn == 1:
        putativeStats("cef", "cmrcl", 0)

    #    if $putconf_hdf_cef_bn == 1 {
    #      putativeStats "hdf" "cef" 0
    #   }
    if putconf_hdf_cef_bn == 1:
        putativeStats("hdf", "cef", 0)

    # if $putconf_hdf_cmrcl_bn == 1 {
    #     putativeStats "hdf" "cmrcl" 0
    # }
    if putconf_hdf_cmrcl_bn == 1:
        putativeStats("hdf", "cmrcl", 0)

    # if $putconf_rhdfbo_cmrcl_bn == 1 {
    #     putativeStats "$rhdfbo" "cmrcl" 0
    # }
    if putconf_rhdfbo_cmrcl_bn == 1:
        putativeStats(rhdfbo, "cmrcl", 0)

    # if $putconf_rmdf_cef_bn == 1 {
    #     putativeStats "$rmdf" "cef" 0
    # }
    if putconf_rmdf_cef_bn == 1:
         putativeStats(rmdf, "cef", 0)


    # if $putconf_rmdf_cmrcl_bn == 1 {
    #     putativeStats "$rmdf" "cmrcl" 0
    # }
    if putconf_rmdf_cmrcl_bn == 1:
        putativeStats(rmdf, "cmrcl", 0)

    # if $putconf_mdf_cef_bn == 1 {
    #     putativeStats mdf "cef" 0
    # }
    if putconf_mdf_cef_bn == 1:
        putativeStats(mdf, "cef", 0)

    # if $putconf_mdf_cmrcl_bn == 1 {
    #     putativeStats "$mdf" "cmrcl" 0
    # }
    if putconf_mdf_cmrcl_bn == 1:
        putativeStats(mdf, "cmrcl", 0)

    #    if $putconf_rhdfbo_cef_bn == 1 {
    #      putativeStats "$rhdfbo" "cef" 0
    #   }
    if putconf_rhdfbo_cef_bn == 1:
        putativeStats(f"{rhdfbo}", "cef", 0)

    #if $putconf_modalGsr_cef_bn == 1 {
    #    putativeStats "modalGsr" "cef" 0
    #}
    if putconf_modalGsr_cef_bn == 1:
        putativeStats("modalGsr", "cef", 0)

    #if $putconf_modalGsr_cmrcl_bn == 1 {
    #    putativeStats "modalGsr" "cmrcl" 0
    #}
    if putconf_modalGsr_cmrcl_bn == 1:
        putativeStats("modalGsr", "cmrcl", 0)

    #if $putconf_propGsr_cef_bn == 1 {
    #    putativeStats "propGsr" "cef" 0
    #}
    if putconf_propGsr_cef_bn == 1:
        putativeStats("propGsr", "cef", 0)

    #if $putconf_propGsr_cmrcl_bn == 1 {
    #    putativeStats "propGsr" "cmrcl" 0
    #}
    if putconf_propGsr_cmrcl_bn == 1:
        putativeStats("propGsr", "cmrcl", 0)

    # use `cefpopnat', clear
    # local cefnatpoplcl = cefpop[1]
    # gen all=1
    # tempfile denominators
    # save `denominators'
    # use `rhdfpopnat', clear
    # local rhdfnatpoplcl = rhdfpop[1]
   
    cefpopnat = pd.read_csv(f'{temp}/cefpopnat.csv')
    print('cefpopnat', list(cefpopnat))
    cefnatpoplcl = cefpopnat.cefpop[0]
    cefpopnat["all"] = 1
    denominators = cefpopnat
    print('denominators', list(denominators))
    rhdfpopnat = pd.read_csv(f'{temp}/rhdfpopnat.csv')
    rhdfnatpoplcl = rhdfpopnat.rhdfpop[0] 

    # * SAVE NATIONAL PUTATIVE STATS
    # use $temp/putative_final, clear
    # gen all= 1
    # merge m:1 all using `denominators', keep(1 3) nogen * SQL equivalent of left join
    # drop all
    # gen incef = `cefnatpoplcl'
    # gen inrecon = `rhdfnatpoplcl'
     
    putative_final = pd.read_csv(f'{temp}/putative_final.csv')
    putative_final["all"] = 1
    putative_final = pd.merge(putative_final, denominators, on=["all"], how="left")
    putative_final = putative_final.drop('all', axis=1)
    putative_final["incef"] = cefnatpoplcl
    putative_final["inrecon"] = rhdfnatpoplcl

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
        cols = order + [x for x in list(putative_final) if x not in order]
        putative_final = putative_final[cols]
        for v in order[1:]:
            putative_final[v].map('{:,}'.format)
    else:
        order = ["lbl", "incef", "inrecon", "exact", "binage"]
        cols = order + [x for x in list(putative_final) if x not in order]
        putative_final = putative_final[cols]
        for v in order[1:]:
            putative_final[v].map('{:,}'.format)

    # drop if missing(lbl)
    # export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(putative) firstrow(variables) sheetmodify
    putative_final = putative_final.dropna(subset=["lbl"])
    print('putative_final excel', list(putative_final))
    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004.xlsx", mode="a", if_sheet_exists="replace") as writer:
        putative_final.to_excel(writer, sheet_name="putative", index=False)

    # * SAVE BLOCK SIZE PUTATIVE STATS
    # use $temp/putativeblksz_final, clear
    # merge 1:1 block_pop using `cefpopblocksz', nogen
    # order block_pop cefpop
    
    putativeblksz_final = pd.read_csv(f'{temp}/putativeblksz_final.csv')
    cefpopblocksz = pd.read_csv(f'{temp}/cefpopblocksz.csv') 
    
    putativeblksz_final = pd.merge(putativeblksz_final, cefpopblocksz, on=["block_pop"])
    order = ["block_pop", "cefpop"]
    cols = order + [x for x in list(putativeblksz_final) if x not in order]
    putativeblksz_final = putativeblksz_final[cols]

    # if $putconf_rhdf_cmrcl_fz == 1 | $putconf_rhdf_cef_fz==1 {
    #     format *_exact *_fzyage *_binage  %20.0gc
    # }
    # else {
    #     format *_exact *_binage %20.0gc
    # }
    # drop if missing(block_pop)
    # export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(putative_blksz) firstrow(variables) sheetmodify

    if putconf_rhdf_cmrcl_fz == 1 or putconf_rhdf_cef_fz == 1: 
        for v in [col for col in putativeblksz_final.columns if col.endswith('_exact') or col.endswith('_fzyage') or col.endswith('_binage')]:
            putativeblksz_final[v].map('{:,}'.format)
    else:
        for v in [col for col in putativeblksz_final.columns if col.endswith('_exact') or col.endswith('_binage')]:
            putativeblksz_final[v].map('{:,}'.format)

    putativeblksz_final = putativeblksz_final.dropna(subset=["block_pop"])
    print('putativeblksz_final excel', list(putativeblksz_final))
    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004.xlsx", mode="a", if_sheet_exists="replace") as writer:
        putativeblksz_final.to_excel(writer, sheet_name="putative_blksz", index=False)
   
    # * SAVE SOLVAR PUTATIVE STATS
    # use $temp/putativesolvar_final, clear
    # merge 1:1 solvar using `solvarpop', nogen
    # order solvar cefpop keepblockpik

    putativesolvar_final = pd.read_csv(f'{temp}/putativesolvar_final.csv', dtype={'solvar':str})
    solvarpop = pd.read_csv(f'{temp}/solvarpop.csv')
    putativesolvar_final = pd.merge(putativesolvar_final, solvarpop, on=["solvar"])


    order = ["solvar", "cefpop", "keepblockpik"]
    cols = order + [col for col in putativesolvar_final if col not in order]
    putativesolvar_final = putativesolvar_final[cols]

    if agree_rhdf_hdf_fz == 1 or agree_rhdf_cef_fz == 1: 
        for v in [col for col in putativesolvar_final.columns if col.endswith('_exact') or col.endswith('_fzyage') or col.endswith('_binage')]:
            putativesolvar_final[v].map('{:,}'.format)
    else:
        for v in [col for col in putativesolvar_final.columns if col.endswith('_exact') or col.endswith('_binage')]:
            putativesolvar_final[v].map('{:,}'.format)
    
    putativesolvar_final = putativesolvar_final.dropna(subset=["solvar"])
    print('putativesolvar_final', list(putativesolvar_final))
    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004.xlsx", mode="a", if_sheet_exists="replace") as writer:
        putativesolvar_final.to_excel(writer, sheet_name="putative_solvar", index=False)

    # *--------------------------------------------------
    # * UNIQUE BLOCK-PIK STATS
    # *--------------------------------------------------
    # import delimited using $rsltbase/cef/cef_block_counts.csv, clear stringcols(1 2)
    # drop if substr(geoid_block,1,2)=="72"

    df = get_df(f'{rsltbase}/cef/cef_block_counts.csv', trainingWheels, dtype={"geoid_block":str, "sex":str})
    # NOTE: I see the first and second cols of cef_block_counts are geoid_block and sex
    #       The first makes sense (it has leading zeroes) but sex appears to be an ordinary number
    #df.iloc[:, [0, 1]] = df.iloc[:, [0, 1]].astype(str)

    df = df[df['geoid_block'].str[0:2] != '72']

    # collapse (sum) haspik pop keepblockpik, by(geoid_block)
    cefpopblocksz = df.groupby('geoid_block').agg({col:"sum" for col in ['haspik', 'pop', 'keepblockpik']})

    # gen blockswithdups = keepblockpik < haspik
    # gen allblocks = 1
    df['blockswithdups'] = df.keepblockpik < df.haspik
    df["allblocks"] = 1

    # collapse(sum) haspik pop keepblockpik blockswithdups allblocks
    # gen pctblockswithdups = 100*blockswithdups/allblocks
    # gen pctpikpopdup = 100*(1- keepblockpik/haspik)
    # format pct* %20.4f
    # format haspik pop keepblockpik blockswithdups allblocks %20.0gc
    # export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(pikdups) firstrow(variables) sheetmodify
    df = df[["haspik", "pop", "keepblockpik", "blockswithdups", "allblocks"]].sum().to_frame().transpose()
    df["pctblockswithdups"] = 100*df["blockswithdups"]/df["allblocks"]
    df["pctpikpopdup"] = 100*(1 - df["keepblockpik"]/df["haspik"])
    for v in [x for x in list(df) if re.match("pct.*", x)]:
        df[v].map('{:.4f}'.format)
    for v in ["haspik", "pop", "keepblockpik", "blockswithdups", "allblocks"]:
        df[v].map('{:,}'.format)
    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004.xlsx", mode="a", if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name="pikdups", index=False)

    # di "#### END OF PUTATIVE STATS ####"
    # di c(current_date) " " c(current_time)
    print("#### END OF PUTATIVE STATS ####")
    now = datetime.now()
    print(now)

if __name__ == "__main__":
    main()

