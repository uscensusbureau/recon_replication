
# solvar_stats.py
# -----------------------------
# /usr/bin/env python3 solvar_stats.py
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
solvar.py:

This code is a translation of existing research STATA code developed by MITRE for the purposes of testing
the corresponding reidentification ouputs. This python file is the translation of "4.solvar.do", responsible
for computing statistics based on solution variability.

Previous STATA Code Header:
[There was no code header]
'''
# Reading Notes:
# This is the "verbose" form of the code, so you will see code comments with the STATA code that formed the# originial code that was translated into python. This code with be labled "Previous STATA Code:"
# -----------------------------
# Imports
# -----------------------------
from config import *
from datetime import datetime
from functools import reduce
import pandas as pd
import numpy as np
import re

def main():
    # di c(current_date) " " c(current_time)
    # di "#### START OF SOLVAR STATS ####"
    now = datetime.now()
    print(now)
    print("#### START OF SOLVAR STATS ####")

    # *--------------------------------------------------
    # * GET NON MODAL SABB ONETON DATA
    # *--------------------------------------------------

    # import delimited using $rhdfbasersltdir/r00/putativematchbinage_r00_cef.csv, clear stringcols(1 2 3 13) $trainingWheels
    # gen geoid_block = county + tract + block
    # merge 1:1 geoid_block pik using $temp/cef_over21_modalre_oneton, keep(1 3) nogen
    # gen put_nmsabb = 1 if sabb_oneton==1 & modalre == 0
    # collapse (sum) put_nmsabb, by(geoid_block)
    # tempfile nmsabbput
    # save `nmsabbput'
    df = get_df(f"{rhdfbasersltdir}/r00/putativematchbinage_r00_cef.csv", trainingWheels, dtype={"county":str, "tract":str, "block":str, "pik":str })
    #df.iloc[:, [0, 1, 2, 12]] = df.iloc[:, [0, 1, 2, 12]].astype(str)
    df["geoid_block"] = df["county"] + df["tract"] + df["block"]

    cef_over21_modalre_oneton = pd.read_csv(f'{temp}/cef_over21_modalre_oneton.csv', dtype={'geoid_block':str, 'pik':str})
    df = pd.merge(df, cef_over21_modalre_oneton, on=["geoid_block", "pik"], how="left")

    # NOTE: if false, NAN
    df['put_nmsabb'] = (df.sabb_oneton == 1) & (df.modalre == 0)
    nmsabbput = df.groupby("geoid_block").sum().reset_index()[['geoid_block', "put_nmsabb"]]

    # import delimited using $rhdfbasersltdir/r00/confirmmatchbinage_r00cef_cef.csv, clear stringcols(1 2 3 13) $trainingWheels
    # gen geoid_block = county + tract + block
    # merge 1:1 geoid_block pik using $temp/cef_over21_modalre_oneton, keep(1 3) nogen 
    # gen conf_nmsabb = 1 if sabb_oneton==1 & modalre == 0
    # collapse (sum) conf_nmsabb, by(geoid_block)
    # tempfile nmsabbconf
    # save `nmsabbconf'
    df = get_df(f"{rhdfbasersltdir}/r00/confirmmatchbinage_r00cef_cef.csv", trainingWheels, dtype={"county":str, "tract":str, "block":str, "pik":str })
    #df.iloc[:, [0, 1, 2, 12]] = df.iloc[:, [0, 1, 2, 12]].astype(str)
    df["geoid_block"] = df["county"] + df["tract"] + df["block"]
    
    df = pd.merge(df, cef_over21_modalre_oneton, on=["geoid_block", "pik"], how="left")

    del cef_over21_modalre_oneton

    # NOTE: if false, NAN
    df['conf_nmsabb'] = (df.sabb_oneton == 1) & (df.modalre == 0)
    nmsabbconf = df.groupby("geoid_block").sum().reset_index()[["geoid_block", "conf_nmsabb"]]

    # *--------------------------------------------------
    # * SOLVAR STATS
    # *--------------------------------------------------

    # import delimited using $solvar/scaled_ivs.csv, clear stringcols(_all) $trainingWheels
    # rename blockid geoid_block
    # * the 202302 scaled_ivs.csv file was missing leading zeros
    # gen l = strlen(geoid_block)
    # tab l
    # replace geoid_block = "0" + geoid_block if l==14
    # destring iv, replace
    # destring siv, replace
    # rename siv s_iv
    # keep geoid_block iv s_iv
    # tempfile iv
    # save `iv'
    df = get_df(f"{solvar}/scaled_ivs.csv", trainingWheels, dtype=str)
    print('before rename', list(df))
    df = df.rename(columns={"Block ID": "geoid_block"})
    print('after rename', list(df))

    df["geoid_block"] = df["geoid_block"].str.zfill(15)

    # NOTE: converting destring to an int
    print('before iv error', list(df))
    df["iv"] = df.astype({'IV': 'float'})['IV']
    df['s_iv'] = df.astype({'SIV': 'float'})['SIV']
    print('before geoidblock error', list(df))

    iv = df[["geoid_block", "iv", "s_iv"]]

    # import delimited using $rsltbase/cef/cef_block_counts.csv, clear stringcols(_all) $trainingWheels
    # destring keepblockpik, replace
    # destring pop, replace
    # collapse (sum) cefpop=pop keepblockpik, by(geoid_block)
    # tempfile kbp
    # save `kbp' 
    df = get_df(f"{rsltbase}/cef/cef_block_counts.csv", trainingWheels, dtype=str)
    print('before kbp', list(df))
    
    df["keepblockpik"] = df["keepblockpik"].astype(int)
    df["pop"] = df["pop"].astype(int) # rename cefpop=pop and destring

    kbp = df.groupby("geoid_block").sum()[["pop", "keepblockpik"]].reset_index()
    
    kbp = kbp.rename(columns={"pop":"cefpop"})
    
    # import delimited using $rhdfbasersltdir/r00/agreebinage_hdf_r00.csv, clear stringcols(_all) $trainingWheels
    # gen geoid_block = county+tract+block
    # destring exact, replace
    # destring binage, replace
    # gen agree = exact + binage
    # keep geoid_block agree
    # tempfile a
    # save `a'
    df = get_df(f"{rhdfbasersltdir}/r00/agreebinage_hdf_r00.csv", trainingWheels, dtype=str)
    print('before add', list(df))
    df["geoid_block"] = df["county"] + df["tract"] + df["block"]

    # NOTE: converting destring to an int
    print('before add', list(df))
    print(df)
    df["exact"] = df["exact"].astype(float)
    df["binage"] = df["binage"].astype(float)

    df["agree"] = df["exact"] + df["binage"]
    a = df[["geoid_block", "agree"]]

    # import delimited using $rhdfbasersltdir/r00/confirmbinage_r00cef_cef.csv, clear stringcols(_all) $trainingWheels
    # gen geoid_block = county+tract+block
    # destring exact, replace
    # destring binage, replace
    # gen conf = exact + binage
    # keep geoid_block conf
    # merge 1:1 geoid_block using `nmsabbconf', nogen
    # tempfile c
    # save `c'
    df = get_df(f"{rhdfbasersltdir}/r00/confirmbinage_r00cef_cef.csv", trainingWheels, dtype=str)
    df["geoid_block"] = df["county"] + df["tract"] + df["block"]

    df["exact"] = df["exact"].astype(float)
    df["binage"] = df["binage"].astype(float)

    df["conf"] = df["exact"] + df["binage"]
    df = df[["geoid_block", "conf"]]
    print(df.dtypes, 'df') 
    #nmsabbconf['geoid_block'] = nmsabbconf['geoid_block']
    print(nmsabbconf)
    print('nmsabb', nmsabbconf.dtypes)
    c = pd.merge(df, nmsabbconf, on=["geoid_block"], how="outer")

    del nmsabbconf

    # import delimited using $rhdfbasersltdir/r00/putativebinage_r00_cef.csv, clear stringcols(_all) $trainingWheels
    # gen geoid_block = county+tract+block
    # destring exact, replace
    # destring binage, replace
    # gen put = exact + binage
    # keep geoid_block put
    # merge 1:1 geoid_block using `nmsabbput', nogen
    # tempfile p
    # save `p'
    df = get_df(f"{rhdfbasersltdir}/r00/putativebinage_r00_cef.csv", trainingWheels, dtype=str)
    df["geoid_block"] = df["county"] + df["tract"] + df["block"]

    df["exact"] = df["exact"].astype(float)
    df["binage"] = df["binage"].astype(float)

    df["put"] = df["exact"] + df["binage"]
    df = df[["geoid_block", "put"]]
    p = pd.merge(df, nmsabbput, on=["geoid_block"], how="outer")

    del nmsabbput

    # use `a', clear
    # merge 1:1 geoid_block using `c', nogen keep(1 3)
    # merge 1:1 geoid_block using `p', nogen keep(1 3)
    # merge 1:1 geoid_block using `kbp',  nogen keep(1 3)
    # merge 1:1 geoid_block using `iv', nogen keep(1 3)
    # merge 1:1 geoid_block using $temp/cefblocksize, nogen keep(1 3)
    # merge 1:1 geoid_block using `over21modalonetonpop', nogen keep(1 3) keepusing(geoid_block nmsabb_oneton)
    # rename nmsabb_oneton nmsabb_oneton_pop
    # foreach v in put put_nmsabb conf conf_nmsabb nmsabb_oneton_pop keepblockpik agree {
    #     replace `v' = 0 if missing(`v')
    # }
    # gen pr = 100*put/keepblockpik
    # gen cr = 100*conf/keepblockpik
    # gen prcn = 100*conf/put
    # gen agr = 100*agree/cefpop

    print('c', list(c))
    print('a', list(a))
    print('kbp', list(kbp))
    print('iv', list(iv))

    # read in cefblocksize, but skip cefpop and keepblockpik, which are already in kbp frame
    cbs_cols = pd.read_csv(f"{temp}/cefblocksize.csv", nrows=1)
    cbs_cols = [c for c in cbs_cols if c not in ["cefpop", "keepblockpik"]]
    cefblocksize = pd.read_csv(f"{temp}/cefblocksize.csv", usecols=cbs_cols, dtype={"geoid_block":str})
    print('cefblocksize', list(cefblocksize))

    # merge each frame and delete the source frames, to save memory as we go
    print("a and c merge")
    df = pd.merge(a, c, on=['geoid_block'], how='left')
    del a
    del c
    print("p merge")
    df = pd.merge(df, p, on=['geoid_block'], how='left')
    del p
    print("kbp merge")
    df = pd.merge(df, kbp, on=['geoid_block'], how='left')
    del kbp
    print("iv merge")
    df = pd.merge(df, iv, on=['geoid_block'], how='left')
    del iv
    print("cefblocksize merge")
    df = pd.merge(df, cefblocksize, on=['geoid_block'], how='left')
    del cefblocksize

    # read in over21modalonetonpop but skip cefpop and keepblockpik, which already appear elsewhere and cause a conflict
    over21modalonetonpop = pd.read_csv(f'{temp}/over21modalonetonpop.csv', usecols=["geoid_block", "nmsabb_oneton"], dtype={"geoid_block":str})
    print("o21m merge")
    df = pd.merge(df, over21modalonetonpop[["geoid_block", "nmsabb_oneton"]], on=["geoid_block"], how="left")
    df = df.rename(columns={"nmsabb_oneton": "nmsabb_oneton_pop"})
    print(list(df))

    print('before loop', list(df))
    for v in ["put", "put_nmsabb", "conf", "conf_nmsabb", "nmsabb_oneton_pop", "keepblockpik", "agree"]:
        print("zero-filling", v)
        df[v].fillna(0)
    
    df["pr"] = 100 * df["put"] / df["keepblockpik"]
    df["cr"] = 100 * df["conf"] / df["keepblockpik"]

    df["prcn"] = 100 * df["conf"] / df["put"]
    df["agr"] = 100 * df["agree"] / df["cefpop"]

    # sum pr agr cr prcn 
    # sum s_iv, d

    # * percentile bins for standardized solvar
    # gen s_ivPct = .
    # replace s_ivPct = 0 if s_iv==0
    # sum s_iv if s_iv>0, d
    # replace s_ivPct = 1 if s_iv<=`r(p25)' & s_iv>0
    # replace s_ivPct = 2 if s_iv<=`r(p50)' & s_iv>`r(p25)'
    # replace s_ivPct = 3 if s_iv<=`r(p75)' & s_iv>`r(p50)'
    # replace s_ivPct = 4 if s_iv<=`r(p90)' & s_iv>`r(p75)'
    # replace s_ivPct = 5 if s_iv>`r(p90)' & !missing(s_iv)
    # label define s_ivlbl 0 "0" 1 "<25th" 2 ">25th <=50th" 3 ">50th <=75th" 4 ">75th <=90th" 5 ">90th"
    # label values s_ivPct s_ivlbl
    # gen s_ivPos = s_iv>0 if !missing(s_iv)

    df['s_ivPct'] = df['s_iv'] == 0
    s_iv_series = df["s_iv"]
    print(df["s_iv"].unique())    
    qnt = s_iv_series.quantile([.25, .50, .75, .90])
    print('qnt', qnt)

    # if there are many zeroes, quantiles might be duplicated
    # Pandas can't handle this, so we have to deduplicate manually
    # the Stata code appears to round up to the highest label in the case of duplicates
    #   so don't include lower quantiles/labels if there's a run of duplicates
    qnt_values = [0,qnt[0.25],qnt[0.5],qnt[0.75],qnt[0.9],float("inf")]
    new_qnt_values = []
    qnt_labels = [1,2,3,4,5]
    new_qnt_labels = []
    for i in range(len(qnt_values) - 1):
        if qnt_values[i] != qnt_values[i+1]:
            new_qnt_values.append(qnt_values[i])
            new_qnt_labels.append(qnt_labels[i])
    #always include the last quantile boundary
    new_qnt_values.append(qnt_values[-1])

    print(new_qnt_values, "from", qnt_values)
    print(new_qnt_labels, "from", qnt_labels)

    # perform quantile labeling
    df["s_ivPct"] = pd.cut(df["s_iv"], new_qnt_values, labels=new_qnt_labels)

    # * hand currated bins for std solvar
    # gen s_ivG = .
    # replace s_ivG = 0 if s_iv==0
    # replace s_ivG = 1 if s_iv>0 & s_iv<=.05
    # replace s_ivG = 2 if s_iv>.05 & s_iv<=.1
    # replace s_ivG = 3 if s_iv>.1 & s_iv<=.15
    # replace s_ivG = 4 if s_iv>.15 & s_iv<=.25
    # replace s_ivG = 5 if s_iv>.25 & !missing(s_iv)
    
    # perform quantile labeling
    df["s_ivG"] = pd.cut(df["s_iv"], [-1, 0, 0.05, 0.1, 0.15, 0.25, float("inf")], labels=[0,1,2,3,4,5])

    # label define s_ivGlbl 0 "0" 1 "(0,.05]" 2 "(.05,.1]" 3 "(.1,.15]" 4 "(.15,.25]" 5 ">.25"
    # label values s_ivG s_ivGlbl
    
    # gen block_pop2 = block_pop
    # replace block_pop2 = 500 if block_pop2 == 1000
    df["block_pop2"] = df["block_pop"]
    df.loc[df["block_pop2"] == 500, "block_pop2"] = 1000

    # label define blkpop 0 "1-9" 10 "10-49" 50 "50-99" 100 "100-249" 250 "250-499" 500 "500-999" 1000 "1000+"
    # label values block_pop blkpop
    # label define blkpop2 0 "1-9" 10 "10-49" 50 "50-99" 100 "100-249" 250 "250-499" 500 "500+"
    # label values block_pop2 blkpop2

    # bys block_pop2: egen ms_iv = mean(s_iv)
    # gen s_iv_blksz_dm = s_iv-ms_iv if !missing(s_iv)
    print("doing ms_iv")
    df["ms_iv"] = df.groupby(["block_pop2"])["s_iv"].transform("mean")
    print("doing s_iv_blksz_dm")
    df.loc[df["s_iv"].notna(), "s_iv_blksz_dm"] = df["s_iv"] - df["ms_iv"]

    # label variable agr "Agrmnt Rate"
    # label variable pr "Putv Rate"
    # label variable cr "Conf Rate"
    # label variable prcn "Prcsn Rate"
    # label variable s_iv "SolVar Norm"
    # label variable iv "SolVar"
    # label variable s_ivPct "SolVar Pctls"
    # label variable s_ivG "SolVar Bins"
    # label variable block_pop "Block Pop"
    # label variable block_pop2 "Block Pop"
    # save $temp/solvarAnalysisData, replace
    # tempfile solvarAnalysisData
    # save `solvarAnalysisData'
    df.to_csv(f"{temp}/solvarAnalysisData.csv", index=False)

    # use `solvarAnalysisData', clear
    # local s = "s_ivG"
    # collapse (sum) put conf agree cefpop keepblockpik, by(block_pop `s')
    # gen pr = 100*put/keepblockpik
    # gen cr = 100*conf/keepblockpik
    # gen prcn = 100*conf/put
    # gen agr = 100*agree/cefpop
    # keep block_pop `s' pr cr prcn agr
    # foreach v in agr pr cr prcn {
    #     preserve
    #     keep block_pop `s' `v' 
    #     reshape wide `v', i(block_pop) j(`s')
    #     rename `v'0 solvar_0 
    #     rename `v'1 solvar_0_005
    #     rename `v'2 solvar_005_01
    #     rename `v'3 solvar_01_015
    #     rename `v'4 solvar_015_025
    #     rename `v'5 solvar_025
    #     format solvar_* %20.4f
    #     export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(solvar_`v') firstrow(variables) 
    #     restore
    # }
    s = "s_ivG"
    df = df[["put", "conf", "agree", "cefpop", "keepblockpik", "block_pop", s]]
    print(list(df), "after projection")
    df = df.groupby(["block_pop", s], as_index=False)
    print(list(df), "after groupby")
    df = df.sum().reset_index()
    print(list(df), "after sum")
    df["pr"] = 100 * df["put"] / df["keepblockpik"]
    df["cr"] = 100 * df["conf"] / df["keepblockpik"]
    df["prcn"] = 100 * df["conf"] / df["put"]
    df["agr"] = 100 * df["agree"] / df["cefpop"]
    df = df[["block_pop", s, "pr", "cr", "prcn", "agr"]]
    for v in ["agr", "pr", "cr", "prcn"]:
        df_v = df[["block_pop", s, v]]
        print(df_v)
        df_v = long_to_wide(df_v, index="block_pop", columns=s, values=v)
        print(df_v)
        # NOTE: reshape adds a suffix from columns to the end of the values headers for new col headers
        for col in [f"{v}0.0", f"{v}1.0", f"{v}2.0", f"{v}3.0", f"{v}4.0", f"{v}5.0"]:
            if col not in list(df_v):
                df_v[col] = np.nan
            df_v[col].map('{:.4f}'.format)

        # order cols
        df_v = df_v[["block_pop", f"{v}0", f"{v}1", f"{v}2", f"{v}3", f"{v}4", f"{v}5"]]
        # rename cols
        df_v = df_v.rename(columns={f"{v}0": "solvar_0",
                                    f"{v}1": "solvar_0_005",
                                    f"{v}2": "solvar_005_01",
                                    f"{v}3": "solvar_01_015",
                                    f"{v}4": "solvar_015_025",
                                    f"{v}5": "solvar_025"})
        # sort rows by left-of-hyphen value in block_pop range (e.g., 100 from 100-249)
        df_v = df_v.sort_values(by='block_pop',
                                key=lambda col: col.str.split('[-|+]').str[0].astype("int"))

        with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004.xlsx", mode="a", if_sheet_exists="replace") as writer:
            df_v.to_excel(writer, sheet_name=f"solvar_{v}", index=False)

    # use `solvarAnalysisData', clear
    # collapse (sum) put_nmsabb conf_nmsabb nmsabb_oneton_pop, by(block_pop `s')
    # gen pr = 100*put_nmsabb/nmsabb_oneton_pop
    # gen cr = 100*conf_nmsabb/nmsabb_oneton_pop
    # gen prcn = 100*conf_nmsabb/put_nmsabb
    # keep block_pop `s' pr cr prcn
    # foreach v in pr cr prcn {
    #     preserve
    #     keep block_pop `s' `v' 
    #     reshape wide `v', i(block_pop) j(`s')
    #     rename `v'0 solvar_0 
    #     rename `v'1 solvar_0_005
    #     rename `v'2 solvar_005_01
    #     rename `v'3 solvar_01_015
    #     rename `v'4 solvar_015_025
    #     rename `v'5 solvar_025
    #     format solvar_* %20.4f
    #     export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(solvar_`v'_nmsabb) firstrow(variables) 
    #     restore
    # }

    df = pd.read_csv(f"{temp}/solvarAnalysisData.csv")
    df = df.groupby(["block_pop", s]).sum().reset_index()[["block_pop", s, "put_nmsabb", "conf_nmsabb", "nmsabb_oneton_pop"]]

    print(df["put_nmsabb"].unique())
    print(df["nmsabb_oneton_pop"].unique())
    print(df["conf_nmsabb"].unique())
    
    df["pr"] = 100 * df["put_nmsabb"] / df["nmsabb_oneton_pop"]
    df["cr"] = 100 * df["conf_nmsabb"] / df["nmsabb_oneton_pop"]
    df["prcn"] = 100 * df["conf_nmsabb"] / df["put_nmsabb"]
    df = df[["block_pop", s, "pr", "cr", "prcn"]]
    for v in ["pr", "cr", "prcn"]:
        df_v = df[["block_pop", s, v]]
        df_v = long_to_wide(df,index="block_pop", columns=s, values=v)

        print("df_v", list(df_v))
        # NOTE: reshape adds a suffix from columns to the end of the values headers for new col headers
        for col in [f"{v}0.0", f"{v}1.0", f"{v}2.0", f"{v}3.0", f"{v}4.0", f"{v}5.0"]:
            if col not in list(df_v):
                df_v[col] = np.nan
            df_v[col].map('{:.4f}'.format)

        # order cols
        df_v = df_v[["block_pop", f"{v}0", f"{v}1", f"{v}2", f"{v}3", f"{v}4", f"{v}5"]]
        # rename cols
        df_v = df_v.rename(columns={f"{v}0": "solvar_0",
                                    f"{v}1": "solvar_0_005",
                                    f"{v}2": "solvar_005_01",
                                    f"{v}3": "solvar_01_015",
                                    f"{v}4": "solvar_015_025",
                                    f"{v}5": "solvar_025"})
        # sort rows by left-of-hyphen value in block_pop range (e.g., 100 from 100-249)
        df_v = df_v.sort_values(by='block_pop',
                                key=lambda col: col.str.split('[-|+]').str[0].astype("int"))

        with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004.xlsx", mode="a", if_sheet_exists="replace") as writer:
            df_v.to_excel(writer, sheet_name=f"solvar_{v}_nmsabb", index=False)

    df = pd.read_csv(f"{temp}/solvarAnalysisData.csv")
    df = df[['geoid_block','s_iv','iv','cefpop']]
    df['u'] = np.random.uniform(size=len(df))
    df = df.sort_values(by=['s_iv','u'])
    df['s_iv'] = df['s_iv'] * 100
    df['n'] = df.reset_index().index + 1
    df['p'] = 100*df['n']/len(df)
    df['qtile'] = pd.cut(df.p, bins=[0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,105])
    part1 = df.groupby('qtile').sum().reset_index()[['iv','cefpop']]
    part2 = df.groupby('qtile').min().reset_index()[['s_iv']]
    clps = part1.join(part2)
    clps = clps.rename(columns={'iv':'solvar_value','cefpop':'pop','s_iv':'solvar_cutoff'})
    clps['solvar_value_cumul'] = clps.solvar_value.cumsum()
    clps['pop_cumul'] = clps['pop'].cumsum()
    clps['solvar_pct'] = 100*(clps['solvar_value'])/(2*clps['pop'])
    clps['solvar_pct_cumul'] = 100*(clps['solvar_value_cumul'])/(2*clps['pop_cumul'])
    clps = clps[['solvar_cutoff','solvar_value','solvar_value_cumul','pop','pop_cumul','solvar_pct','solvar_pct_cumul']]
    clps = clps.reset_index()

    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004.xlsx", mode="a", if_sheet_exists="replace") as writer:
        clps.to_excel(writer, sheet_name=f"solvar_qtile", index=False)

    #* statistics of solvar distribution by block pop
    #use `solvarAnalysisData', clear
    #gen zsvpop = cefpop if s_iv==0
    #gen zsvblk = 1 if s_iv==0
    #gen blk = 1
    #gen pop = cefpop
    #collapse (sum) zsvpop zsvblk blk pop, by(block_pop)
    #format zsvpop zsvblk blk pop  %20.0gc 
    #export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(solvar_blksz) firstrow(variables)

    df = pd.read_csv(f"{temp}/solvarAnalysisData.csv")
    df.loc[df["s_iv"] == 0, "zsvpop"] = df["cefpop"]
    df.loc[df["s_iv"] == 0, "zsvblk"] = 1
    df["blk"] = 1
    df["pop"] = df["cefpop"]
    df = df.groupby("block_pop").sum().reset_index()[["block_pop", "zsvpop", "zsvblk", "blk", "pop"]]
    df = df.sort_values(by='block_pop',
                        key=lambda col: col.str.split('[-|+]').str[0].astype("int"))

    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004.xlsx", mode="a", if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name="solvar_blksz", index=False)

    # di "#### END OF SOLVAR STATS ####"
    # di c(current_date) " " c(current_time)
    now = datetime.now()
    print(now)
    print("#### END OF SOLVAR STATS ####")

if __name__ == "__main__":
    main()
