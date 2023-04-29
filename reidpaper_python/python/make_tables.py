# make_tables.py
#-----------------------------
# /usr/bin/env python3 make_tables.py
# normal execution is via call in runall.py
#-----------------------------
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
make_tables.py:

This code is a translation of existing research STATA code developed by MITRE for the purposes of testing
the corresponding reidentification ouputs. This python file is the translation of "4.make_tables.do", responsible
for creating the output CSV files containing all the requested statistics.

Previous STATA Code Header:
[There was no code header]
'''

from datetime import datetime
from config import *
import pandas as pd
import re

#--------------------------------------------------
# Function: fixBlockPopSort
#--------------------------------------------------
# Description:
#   [TODO: insert description]
#
# Input:
#   -df:
#
# Output:
#   A dataframe [TODO: insert description]
#
# Previous STATA Code:
# capture fixBlockPopSort drop fixBlockPopSort
# program define fixBlockPopSort
#     gen blksrt = block_pop
#     replace blksrt = subinstr(blksrt,"+","",.)
#     split blksrt, parse("-")
#     destring blksrt1, replace
#     sort blksrt1
#     drop blksrt*
# end
def fixBlockPopSort(df):
    # block pop is already sorted
    print(df)
    '''
    df['blksrt'] = df['block_pop']
    print(df.blksrt)
    if 'blksrt1' in list(df):
        print('blksrt1', df.blksrt1)
    df['blksrt'] = df['blksrt'].str.replace("+","") #  replaces "replace blksrt = subinstr(blksrt,"+","",.)"
    print(df.blksrt)
        #scratch vector, split to df with two columns.
    print(df['blksrt'].str.split("-")) #  replaces "split blksrt, parse("-")" does this work?
    df['blksrt'] = df['blksrt'].str.split("-") #  replaces "split blksrt, parse("-")" does this work?
    df['blksrt'] = df.astype({'blksrt': 'int32'}) #!!  replaces "destring blksrt1, replace", do we want integer?
    df = df.sort_values(by=['blksrt'])
    # df = df.drop(['blksrt']) # TODO: i think they meant to drop all of the other vars that were created by split
    '''

#--------------------------------------------------
# Function: renameAge
#--------------------------------------------------
# Description:
#   Renames column headers that match *under21* or *over21* with u21 and o21 respectively.
#
# Input:
#   -df:
#
# Output:
#   A dataframe that has all headers with under21 renamed to u21 and headers with over21 renamed to o21.
#
# Previous STATA Code:
# capture renameAge drop renameAge
# program define renameAge
#     quietly desc, varlist
#     if regexm("`r(varlist)'","under21") {
#         foreach v of varlist *under21* {
#             local newName = subinstr("`v'","under21","u21",.)
#             rename `v' `newName'
#         }
#         foreach v of varlist *over21* {
#             local newName = subinstr("`v'","over21","o21",.)
#             rename `v' `newName'
#         }
#     }
# end
def renameAge(df):
    varlist = list(df)
    res_u = [x for x in varlist if re.match(".*under21", x)]
    res_o = [x for x in varlist if re.match(".*over21.*", x)]
    if "under21" in varlist:
        for v in res_u:
            newName = v.replace("under21","u21")
            df = df.rename(columns={v: newName})
        for v in res_o:
            newName = v.replace("over21","o21")
            df = df.rename(columns={v: newName})

#--------------------------------------------------
# Function: addEmptyFzyage
#--------------------------------------------------
# Description:
#   If using fzyage, create empty columns (of zeros) with specified headers. 
#
# Input:
#   -df:
#
# Output:
#   A dataframe with all empty fzyage columns added.
#
# Previous STATA Code:
# capture addEmptyFzyage drop addEmptyFzyage
# program define addEmptyFzyage
#     capture confirm variable fzyage
#     if _rc != 0 {
#         gen fzyage =0
#         gen u21_fzyage =0
#         gen o21_fzyage =0
#         gen m_fzyage =0
#         gen nm_fzyage =0
#         gen sab_fzyage =0
#         gen sabb_fzyage =0
#         gen ber_fzyage =0
#         gen nmsab_fzyage =0
#         gen nmsabb_fzyage =0
#         gen msab_fzyage =0
#         gen msabb_fzyage =0
#     }
# end
def addEmptyFzyage(df):
    column_headers = list(df)
    if ('fzyage' not in column_headers):
        df['fzyage'] = 0
        df['u21_fzyage'] = 0
        df['o21_fzyage'] = 0
        df['m_fzyage'] = 0
        df['nm_fzyage'] = 0
        df['sab_fzyage'] = 0
        df['sabb_fzyage'] = 0
        df['ber_fzyage'] = 0
        df['nmsab_fzyage'] = 0
        df['nmsabb_fzyage'] = 0
        df['msab_fzyage'] = 0
        df['msabb_fzyage'] = 0
        df['ccbpas_fzyage'] = 0
        df['ccbpabs_fzyage'] = 0
    
#--------------------------------------------------
# Function: recodeSolvar
#--------------------------------------------------
# Description:
#   Replace all occurrances of "0" and ">0" with verbose, "zero" and "gtzero" respectively.
#
# Input:
#   -df:
#
# Output:
#   A dataframe where solvar column has "0" replaced with "zero" and ">0" replaced with "gtzero".
#
# Previous STATA Code:
# capture recodeSolvar drop recodeSolvar
# program define recodeSolvar
#     replace solvar  = "zero" if solvar=="0"
#     replace solvar  = "gtzero" if solvar==">0"
# end
def recodeSolvar(df):
    df.loc[df["solvar"] == "0", "solvar"] = "zero"
    df.loc[df["solvar"] == ">0", "solvar"] = "gtzero"
    
def main():
    # datetime object containing current date and time
    now = datetime.now()
    print(now)
    print("#### START OF MAKE TABLES ####")

    print('*--------------------------------------------------')
    print('* MAKE USABLE TABLES')
    print('*--------------------------------------------------')

    #*--------------------------------------------------
    #* NATIONAL AGREEMENT RATES
    #*--------------------------------------------------
    print('*--------------------------------------------------')
    print('* NATIONAL AGREEMENT RATES')
    print('*--------------------------------------------------')
    # EXCEL import
    # import excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(agree) clear first

    df = pd.read_excel(f'{disclosure}/CBDRB-FY22-DSEP-004.xlsx',sheet_name='agree')

    # capture confirm variable fzyage
    # if _rc != 0 {
    #     gen fzyage =0
    # }
    # check shape of df, check num_row != 0  : #logically consistency
    column_headers = list(df)
    if ('fzyage' not in column_headers):
        df['fzyage'] = 0

    # keep lbl incef exact fzyage binage oneoff
    # rename incef pop
    print(df.columns)
    df = df[['lbl', 'incef', 'exact', 'fzyage', 'binage', 'oneoff']]
    df = df.rename(columns={'incef':'pop'})

    # foreach v in exact fzyage binage oneoff {
    #     gen `v'_r = 100*(`v'/pop)
    # }
    for v in ['exact', 'fzyage', 'binage', 'oneoff']:
        df[f"{v}_r"] = 100*(df[v]/df['pop'])

    #format pop exact fzyage binage oneoff %20.0gc
    for v in ['pop', 'exact', 'fzyage', 'binage', 'oneoff']:
        df[v].map('${:20,.0f}'.format)

    #format *_r %20.4f
    res_r = [x for x in list(df) if re.match('.*_r',x)]
    for v in res_r:
        df[v].map('${:20,.4f}'.format)

    #order lbl pop exact exact_r fzyage fzyage_r binage binage_r oneoff oneoff_r
    df = df[['lbl', 'pop', 'exact', 'exact_r', 'fzyage', 'fzyage_r', 'binage', 'binage_r', 'oneoff', 'oneoff_r']]

    #export excel using $disclosure/CBDRB-FY22-DSEP-004_tables.xlsx, sheet(agree_basic) firstrow(variables) replace
    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004_tables.xlsx", mode="w") as writer:
        df.to_excel(writer, sheet_name='agree_basic', index=False)

    #*--------------------------------------------------
    #* BLOCK SIZE AGREEMENT RATES
    #*--------------------------------------------------
    print('*--------------------------------------------------')
    print('* BLOCK SIZE AGREEMENT RATES')
    print('*--------------------------------------------------')

    #import excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(agree_blksz) clear first
    #rename cefpop pop
    df = pd.read_excel(f'{disclosure}/CBDRB-FY22-DSEP-004.xlsx',sheet_name='agree_blksz')
    df = df.rename(columns={'cefpop':'pop'})

    # foreach v of varlist *binage_binage {
    #     gen `v'_r = 100*(`v'/pop)
    # }
    # keep block_pop pop *_r
    # format *_r %20.4f
    # order block_pop pop
    # replace block_pop = "1-9" if block_pop=="0-9"
    # fixBlockPopSort
    # export excel using $disclosure/CBDRB-FY22-DSEP-004_tables.xlsx, sheet(agree_blksz_basic) firstrow(variables) sheetmodify

    res_binage = [x for x in list(df) if re.match('.*binage_binage$',x)]
    for v in res_binage:
        df[f"{v}_r"] = 100*(df[v]/df['pop'])

    res_r = [x for x in list(df) if re.match('.*_r',x)]
    df = df[['block_pop', 'pop'] + res_r]
    for v in res_r:
        df[v].map('${:20,.4f}'.format)

    df.loc[df["block_pop"] == "0-9",'block_pop'] = "1-9" 
    fixBlockPopSort(df)
    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004_tables.xlsx", mode="a", if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name='agree_blksz_basic', index=False)

    #*--------------------------------------------------
    #* NATIONAL PUTATIVE & CONFIRM RATES
    #*--------------------------------------------------
    print('*--------------------------------------------------')
    print('* NATIONAL PUTATIVE & CONFIRM RATES')
    print('*--------------------------------------------------')

    # import excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(putative) clear first
    # addEmptyFzyage
    # renameAge
    # rename incef pop
    #
    # foreach v in exact fzyage binage {
    #     gen put_`v' = `v'
    #     gen put_`v'_ofpopr = 100*(`v'/keepblockpik)
    # }
    df = pd.read_excel(f'{disclosure}/CBDRB-FY22-DSEP-004.xlsx',sheet_name='putative')
    addEmptyFzyage(df)
    renameAge(df)
    df = df.rename(columns={'incef':'pop'})

    for v in ['exact', 'fzyage', 'binage']:
        df[f"put_{v}"] = df[v]
        df[f"put_{v}_ofpopr"] = 100 * (df[v] / df['keepblockpik'])

    # keep lbl put_* *_ofpopr
    # tempfile p1
    # save `p1'
    df = df[["lbl"] + [x for x in list(df) if re.match("put_.*", x) or re.match(".*_ofpopr", x)]]
    p1 = df.copy()


    # import excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(confirm) clear first
    # addEmptyFzyage
    # renameAge
    # rename incef pop
    #
    # merge 1:1 lbl using `p1', nogen
    # foreach v in exact fzyage binage {
    #     gen conf_`v'_ofpopr = 100*(`v'/keepblockpik)
    #     gen conf_`v'_ofputr = 100*(`v'/put_`v')
    # }
    # keep lbl keepblockpik *_ofpopr *_ofputr
    # format keepblockpik  %20.0gc
    # format *_ofpopr *_ofputr %20.4f
    # order lbl keepblockpik put_exact_ofpopr put_fzyage_ofpopr put_binage_ofpopr conf_exact_ofpopr conf_fzyage_ofpopr conf_binage_ofpopr conf_exact_ofputr conf_fzyage_ofputr conf_binage_ofputr
    # export excel using $disclosure/CBDRB-FY22-DSEP-004_tables.xlsx, sheet(putconf_basic) firstrow(variables) sheetmodify
    df = pd.read_excel(f'{disclosure}/CBDRB-FY22-DSEP-004.xlsx',sheet_name='confirm')
    addEmptyFzyage(df)
    renameAge(df)
    df = df.rename(columns={'incef':'pop'})

    df = pd.merge(df, p1, on=["lbl"], how="outer", suffixes=('','_drop'))
    for v in ["exact", "fzyage", "binage"]:
        df[f"conf_{v}_ofpopr"] = 100 * (df[v]/df["keepblockpik"])
        df[f"conf_{v}_ofputr"] = 100 * (df[v]/df[f"put_{v}"])

    res_ofpop_ofput = [x for x in list(df) if re.match(".*_ofpopr", x) or re.match(".*_ofputr", x)]
    df = df[["lbl", "keepblockpik"] + res_ofpop_ofput]
    df["keepblockpik"].map('${:20,.0f}'.format)
    for v in res_ofpop_ofput:
        df[v].map('${:20,.4f}'.format)

    # rows alphabetical by lbl
    df = df.sort_values("lbl")

    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004_tables.xlsx", mode="a", if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name="putconf_basic", index=False)

    # import excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(putative) clear first
    # addEmptyFzyage
    # renameAge
    #
    # replace o21_binage = o21_fzyage if missing(o21_binage)
    # replace u21_binage = u21_fzyage if missing(u21_binage)
    # foreach v of varlist u21* o21* {
    #     replace `v' = 0 if missing(`v')
    # }
    # gen put_o21_ofpopr = 100*o21_binage/o21pop
    # gen put_u21_ofpopr = 100*u21_binage/u21pop
    # gen put_o21 = o21_binage
    # gen put_u21 = u21_binage
    df = pd.read_excel(f'{disclosure}/CBDRB-FY22-DSEP-004.xlsx',sheet_name='putative')
    addEmptyFzyage(df)
    renameAge(df)

    # df.loc[df["o21_binage"].isnull(), "o21_binage"] = df["o21_fzyage"]
    # df.loc[df["u21_binage"].isnull(), "u21_binage"] = df["u21_fzyage"]
    # TODO: this if filling missing values with specified values
    df["o21_binage"] = df["o21_binage"].fillna(df["o21_fzyage"])
    df["u21_binage"] = df["u21_binage"].fillna(df["u21_fzyage"])

    res_prefix = [x for x in list(df) if re.match("u21.*", x) or re.match("o21.*", x)]
    for v in res_prefix:
        # df.loc[df[v].isnull(), v] = 0
        df[v] = df[v].fillna(0)

    df["put_o21_ofpopr"] = 100 * df["o21_binage"] / df["o21pop"]
    df["put_u21_ofpopr"] = 100 * df["u21_binage"] / df["u21pop"]
    df["put_o21"] = df["o21_binage"]
    df["put_u21"] = df["u21_binage"]

    #keep lbl put_o21_ofpopr put_u21_ofpopr put_o21 put_u21
    df = df[['lbl', 'put_o21_ofpopr', 'put_u21_ofpopr', 'put_o21', 'put_u21']]

    # tempfile p1
    # save `p1'
    p1 = df.copy()

    # import excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(confirm) clear first
    # addEmptyFzyage
    # renameAge
    #
    # replace o21_binage = o21_fzyage if missing(o21_binage)
    # replace u21_binage = u21_fzyage if missing(u21_binage)
    # foreach v of varlist u21* o21* {
    #     replace `v' = 0 if missing(`v')
    # }
    # merge 1:1 lbl using `p1', nogen
    # gen conf_o21_ofpopr = 100*o21_binage/o21pop
    # gen conf_u21_ofpopr = 100*u21_binage/u21pop
    # gen conf_o21_ofputr = 100*o21_binage/put_o21
    # gen conf_u21_ofputr = 100*u21_binage/put_u21
    df = pd.read_excel(f'{disclosure}/CBDRB-FY22-DSEP-004.xlsx',sheet_name='confirm')
    addEmptyFzyage(df)
    renameAge(df)

    # df.loc[df["o21_binage"].isnull(), o21_binage] = df["o21_fzyage"]
    # df.loc[df["u21_binage"].isnull(), u21_binage] = df["u21_fzyage"]
    df["o21_binage"] = df["o21_binage"].fillna(df["o21_fzyage"])
    df["u21_binage"] = df["u21_binage"].fillna(df["u21_fzyage"])

    res_prefix = [x for x in list(df) if re.match("u21.*", x) or re.match("o21.*", x)]
    for v in res_prefix:
        # df.loc[df[v].isnull(), v] = 0
        df[v] = df[v].fillna(0)

    df = pd.merge(df, p1, on=["lbl"], how="outer", suffixes=('','_drop'))
    df["conf_o21_ofpopr"] = 100 * df["o21_binage"] / df["o21pop"]
    df["conf_u21_ofpopr"] = 100 * df["u21_binage"] / df["u21pop"]
    df["conf_o21_ofputr"] = 100 * df["o21_binage"] / df["put_o21"]
    df["conf_u21_ofputr"] = 100 * df["u21_binage"] / df["put_u21"]

    # keep lbl u21pop o21pop put_u21_ofpopr conf_u21_ofpopr conf_u21_ofputr put_o21_ofpopr conf_o21_ofpopr conf_o21_ofputr
    # order lbl u21pop o21pop put_u21_ofpopr conf_u21_ofpopr conf_u21_ofputr put_o21_ofpopr conf_o21_ofpopr conf_o21_ofputr

    # format u21pop o21pop  %20.0gc
    # format *_ofpopr *_ofputr %20.4f
    # export excel using $disclosure/CBDRB-FY22-DSEP-004_tables.xlsx, sheet(putconf_u21_o21) firstrow(variables) sheetmodify

    df = df[['lbl', 'u21pop', 'o21pop', 'put_u21_ofpopr', 'conf_u21_ofpopr', 'conf_u21_ofputr', 'put_o21_ofpopr', 'conf_o21_ofpopr', 'conf_o21_ofputr']]
    for v in ['u21pop', 'o21pop']:
        df[v].map('${:20,.0f}'.format)

    res_ofpop_ofput = [x for x in list(df) if re.match(".*_ofpopr", x) or re.match(".*_ofputr", x)]
    for v in res_ofpop_ofput:
        df[v].map('${:20,.4f}'.format)

    # rows alphabetical by lbl
    df = df.sort_values("lbl")

    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004_tables.xlsx", mode="a", if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name="putconf_u21_o21", index=False)

    # import excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(putative) clear first
    # addEmptyFzyage
    # renameAge
    #
    # replace m_binage = m_fzyage if missing(m_binage)
    # replace nm_binage = nm_fzyage if missing(nm_binage)
    # foreach v of varlist m_* nm_* {
    #     replace `v' = 0 if missing(`v')
    # }
    # gen put_m_ofpopr = 100*m_binage/modalrepop
    # gen put_nm_ofpopr = 100*nm_binage/nonmodalrepop
    # gen put_modalre = m_binage
    # gen put_nonmodalre = nm_binage
    # keep lbl put_m_ofpopr put_nm_ofpopr put_modalre put_nonmodalre
    # tempfile p1
    # save `p1'
    df = pd.read_excel(f'{disclosure}/CBDRB-FY22-DSEP-004.xlsx',sheet_name='putative')
    addEmptyFzyage(df)
    renameAge(df)

    df["m_binage"] = df["m_binage"].fillna(df["m_fzyage"])
    df["nm_binage"] = df["nm_binage"].fillna(df["nm_fzyage"])

    res_prefix = [x for x in list(df) if re.match("m_.*", x) or re.match("nm_.*", x)]
    for v in res_prefix:
        # df.loc[df[v].isnull(), v] = 0
        df[v] = df[v].fillna(0)

    df["put_m_ofpopr"] = 100 * df["m_binage"] / df["modalrepop"]
    df["put_nm_ofpopr"] = 100 * df["nm_binage"] / df["nonmodalrepop"]
    df["put_modalre"] = df["m_binage"]
    df["put_nonmodalre"] = df["nm_binage"]

    df = df[["lbl", "put_m_ofpopr", "put_nm_ofpopr", "put_modalre", "put_nonmodalre"]]

    p1 = df.copy()

    # import excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(confirm) clear first
    # addEmptyFzyage
    # renameAge
    #
    # replace m_binage = m_fzyage if missing(m_binage)
    # replace nm_binage = nm_fzyage if missing(nm_binage)
    # foreach v of varlist m_* nm_* {
    #     replace `v' = 0 if missing(`v')
    # }
    # merge 1:1 lbl using `p1', nogen
    # gen conf_m_ofpopr = 100*m_binage/modalrepop
    # gen conf_nm_ofpopr = 100*nm_binage/nonmodalrepop
    # gen conf_m_ofputr = 100*m_binage/put_modalre
    # gen conf_nm_ofputr = 100*nm_binage/put_nonmodalre
    df = pd.read_excel(f'{disclosure}/CBDRB-FY22-DSEP-004.xlsx',sheet_name='confirm')
    addEmptyFzyage(df)
    renameAge(df)

    df["m_binage"] = df["m_binage"].fillna(df["m_fzyage"])
    df["nm_binage"] = df["nm_binage"].fillna(df["nm_fzyage"])

    res_prefix = [x for x in list(df) if re.match("m_.*", x) or re.match("nm_.*", x)]
    for v in res_prefix:
        # df.loc[df[v].isnull(), v] = 0
        df[v] = df[v].fillna(0)

    df = pd.merge(df, p1, on=["lbl"], how="outer", suffixes=('','_drop'))
    df["conf_m_ofpopr"] = 100 * df["m_binage"] / df["modalrepop"]
    df["conf_nm_ofpopr"] = 100 * df["nm_binage"] / df["nonmodalrepop"]
    df["conf_m_ofputr"] = 100 * df["m_binage"] / df["put_modalre"]
    df["conf_nm_ofputr"] = 100 * df["nm_binage"] / df["put_nonmodalre"]

    # keep lbl modalrepop nonmodalrepop put_nm_ofpopr conf_nm_ofpopr conf_nm_ofputr put_m_ofpopr conf_m_ofpopr conf_m_ofputr
    # order lbl modalrepop nonmodalrepop put_nm_ofpopr conf_nm_ofpopr conf_nm_ofputr put_m_ofpopr conf_m_ofpopr conf_m_ofputr
    # format modalrepop nonmodalrepop %20.0gc
    # format *_ofpopr *_ofputr %20.4f
    # export excel using $disclosure/CBDRB-FY22-DSEP-004_tables.xlsx, sheet(putconf_modal_nonmodal) firstrow(variables) sheetmodify
    df = df[['lbl', 'modalrepop', 'nonmodalrepop', 'put_nm_ofpopr', 'conf_nm_ofpopr', 'conf_nm_ofputr', 'put_m_ofpopr', 'conf_m_ofpopr', 'conf_m_ofputr']]
    for v in ['modalrepop', 'nonmodalrepop']:
        df[v].map('${:20,.0f}'.format)
    res_ofpop_ofput = [x for x in list(df) if re.match(".*_ofpopr", x)or re.match(".*_ofputr", x)]
    for v in res_ofpop_ofput:
        df[v].map('${:20,.4f}'.format)

    # rows alphabetical by lbl
    df = df.sort_values("lbl")

    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004_tables.xlsx", mode="a", if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name="putconf_modal_nonmodal", index=False)

    # import excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(putative) clear first
    # addEmptyFzyage
    # renameAge
    #
    # foreach ton in sab sabb ber {
    #     replace `ton'_binage = `ton'_fzyage if missing(`ton'_binage)
    # }
    # foreach v of varlist sab* ber* {
    #     replace `v' = 0 if missing(`v')
    # }
    # foreach ton in sab sabb ber {
    #     gen put_`ton'_ofpopr = 100*`ton'_binage/`ton'_onetonpop
    #     gen put_`ton' = `ton'_binage
    # }
    # keep lbl put_*
    # tempfile p1
    # save `p1'
    df = pd.read_excel(f'{disclosure}/CBDRB-FY22-DSEP-004.xlsx',sheet_name='putative')
    addEmptyFzyage(df)
    renameAge(df)

    for ton in ["sab", "sabb", "ber"]:
        df[f"{ton}_binage"] =  df[f"{ton}_binage"].fillna(df[f"{ton}_fzyage"])
    res_prefix = [x for x in list(df) if re.match("sab.*", x) or re.match("ber.*", x)]
    for v in res_prefix:
        df[v] = df[v].fillna(0)
    for ton in ["sab", "sabb", "ber"]:
        df[f"put_{ton}_ofpopr"] = 100 * df[f"{ton}_binage"] / df[f"{ton}_onetonpop"]
        df[f"put_{ton}"] = df[f"{ton}_binage"]

    df = df[["lbl"] + [x for x in list(df) if re.match("put_.*", x)]]
    p1 = df.copy()

    # import excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(confirm) clear first
    # addEmptyFzyage
    # renameAge
    #
    # foreach ton in sab sabb ber {
    #     replace `ton'_binage = `ton'_fzyage if missing(`ton'_binage)
    # }
    # foreach v of varlist sab* ber* {
    #     replace `v' = 0 if missing(`v')
    # }
    # merge 1:1 lbl using `p1', nogen
    # foreach ton in sab sabb ber {
    #     gen conf_`ton'_ofpopr = 100*`ton'_binage/`ton'_onetonpop
    #     gen conf_`ton'_ofputr = 100*`ton'_binage/put_`ton'
    # }
    #
    # keep lbl sab_onetonpop sabb_onetonpop ber_onetonpop put_sab_ofpopr conf_sab_ofpopr conf_sab_ofputr  put_sabb_ofpopr conf_sabb_ofpopr conf_sabb_ofputr  put_ber_ofpopr conf_ber_ofpopr conf_ber_ofputr
    # order lbl sab_onetonpop sabb_onetonpop ber_onetonpop put_sab_ofpopr conf_sab_ofpopr conf_sab_ofputr  put_sabb_ofpopr conf_sabb_ofpopr conf_sabb_ofputr  put_ber_ofpopr conf_ber_ofpopr conf_ber_ofputr
    # format sab_onetonpop sabb_onetonpop ber_onetonpop  %20.0gc
    # format *_ofpopr *_ofputr %20.4f
    # export excel using $disclosure/CBDRB-FY22-DSEP-004_tables.xlsx, sheet(putconf_oneton) firstrow(variables) sheetmodify

    df = pd.read_excel(f'{disclosure}/CBDRB-FY22-DSEP-004.xlsx',sheet_name='confirm')
    addEmptyFzyage(df)
    renameAge(df)

    for ton in ["sab", "sabb", "ber"]:
        df[f"{ton}_binage"] = df[f"{ton}_binage"].fillna(df[f"{ton}_fzyage"])

    res_prefix = [x for x in list(df) if re.match("sab.*", x) or re.match("ber.*", x)]
    for v in res_prefix:
        df[v] = df[v].fillna(0)

    df = pd.merge(df, p1, on=["lbl"], how="outer", suffixes=('','_drop'))
    for ton in ["sab", "sabb", "ber"]:
        df[f"conf_{ton}_ofpopr"] = 100 * df[f"{ton}_binage"] / df[f"{ton}_onetonpop"]
        df[f"conf_{ton}_ofputr"] = 100 * df[f"{ton}_binage"] / df[f"put_{ton}"]

    df = df[['lbl', 'sab_onetonpop', 'sabb_onetonpop', 'ber_onetonpop', 'put_sab_ofpopr', 'conf_sab_ofpopr', 'conf_sab_ofputr', 'put_sabb_ofpopr', 'conf_sabb_ofpopr', 'conf_sabb_ofputr', 'put_ber_ofpopr', 'conf_ber_ofpopr', 'conf_ber_ofputr']]
    for v in ['sab_onetonpop', 'sabb_onetonpop', 'ber_onetonpop']:
        df[v].map('${:20,.0f}'.format)
    res_ofpop_ofput = [x for x in list(df) if re.match(".*_ofpopr", x)or re.match(".*_ofputr", x)]
    for v in res_ofpop_ofput:
        df[v].map('${:20,.4f}'.format)

    # rows alphabetical by lbl
    df = df.sort_values("lbl")

    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004_tables.xlsx", mode="a", if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name="putconf_oneton", index=False)

    # import excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(putative) clear first
    # addEmptyFzyage
    # renameAge

    # foreach ton in sab sabb {
    #     foreach g in nm m {
    #         replace `g'`ton'_binage = `g'`ton'_fzyage if missing(`g'`ton'_binage)
    #     }
    # }
    # foreach v of varlist nmsab* msab* {
    #     replace `v' = 0 if missing(`v')
    # }
    # foreach ton in sab sabb {
    #     foreach g in nm m {
    #         gen put_`g'`ton'_ofpopr = 100*`g'`ton'_binage/`g'`ton'_oneton_pop
    #         gen put_`g'`ton' = `g'`ton'_binage
    #     }
    # }
    #
    # keep lbl put*
    # tempfile p1
    # save `p1'
    df = pd.read_excel(f'{disclosure}/CBDRB-FY22-DSEP-004.xlsx',sheet_name='putative')
    addEmptyFzyage(df)
    renameAge(df)

    for ton in ["sab", "sabb"]:
        for g in ["nm", "m"]:
            df[f"{g}{ton}_binage"] = df[f"{g}{ton}_binage"].fillna(df[f"{g}{ton}_fzyage"])

    res_prefix = [x for x in list(df) if re.match("nmsab.*", x) or re.match("msab.*", x)]
    for v in res_prefix:
        df[v] = df[v].fillna(0)

    for ton in ["sab", "sabb"]:
        for g in ["nm", "m"]:
            df[f"put_{g}{ton}_ofpopr"] = 100 * df[f"{g}{ton}_binage"] / df[f"{g}{ton}_oneton_pop"]
            df[f"put_{g}{ton}"] = df[f"{g}{ton}_binage"]

    res_prefix = [x for x in list(df) if re.match("put.*", x)]
    df = df[["lbl"] + res_prefix]
    p1 = df.copy()

    # import excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(confirm) clear first
    # addEmptyFzyage
    # renameAge
    # foreach ton in sab sabb {
    #     foreach g in nm m {
    #         replace `g'`ton'_binage = `g'`ton'_fzyage if missing(`g'`ton'_binage)
    #     }
    # }
    # foreach v of varlist nmsab* msab* {
    #     replace `v' = 0 if missing(`v')
    # }
    # merge 1:1 lbl using `p1', nogen
    # foreach ton in sab sabb {
    #     foreach g in nm m {
    #         gen conf_`g'`ton'_ofpopr = 100*`g'`ton'_binage/`g'`ton'_oneton_pop
    #         gen conf_`g'`ton'_ofputr = 100*`g'`ton'_binage/put_`g'`ton'
    #     }
    # }
    # keep lbl modalrepop nonmodalrepop sab_onetonpop sabb_onetonpop nmsab_oneton_pop nmsabb_oneton_pop msab_oneton_pop msabb_oneton_pop put_nmsab_ofpopr conf_nmsab_ofpopr conf_nmsab_ofputr  put_nmsabb_ofpopr conf_nmsabb_ofpopr conf_nmsabb_ofputr  put_msab_ofpopr conf_msab_ofpopr conf_msab_ofputr  put_msabb_ofpopr conf_msabb_ofpopr conf_msabb_ofputr
    # order lbl modalrepop nonmodalrepop sab_onetonpop sabb_onetonpop nmsab_oneton_pop nmsabb_oneton_pop msab_oneton_pop msabb_oneton_pop put_nmsab_ofpopr conf_nmsab_ofpopr conf_nmsab_ofputr  put_nmsabb_ofpopr conf_nmsabb_ofpopr conf_nmsabb_ofputr  put_msab_ofpopr conf_msab_ofpopr conf_msab_ofputr  put_msabb_ofpopr conf_msabb_ofpopr conf_msabb_ofputr
    # format modalrepop nonmodalrepop sab_onetonpop sabb_onetonpop nmsab_oneton_pop nmsabb_oneton_pop msab_oneton_pop msabb_oneton_pop %20.0gc
    # format *_ofpopr *_ofputr %20.4f
    # export excel using $disclosure/CBDRB-FY22-DSEP-004_tables.xlsx, sheet(putconf_oneton_modal_nonmodal) firstrow(variables) sheetmodify
    df = pd.read_excel(f'{disclosure}/CBDRB-FY22-DSEP-004.xlsx',sheet_name='confirm')
    print('confirm', list(df))
    addEmptyFzyage(df)
    renameAge(df)
    print('confirm2', list(df))

    for ton in ["sab", "sabb"]:
        for g in ["nm", "m"]:
            # df.loc[df[f"{g}{ton}_binage"].isnull(), f"{g}{ton}_binage"] = df[f"{g}{ton}_fzyage"]
            df[f"{g}{ton}_binage"] = df[f"{g}{ton}_binage"].fillna(df[f"{g}{ton}_fzyage"])
    print('confirm3', list(df))

    res_prefix = [x for x in list(df) if re.match("nmsab.*", x) or re.match("msab.*", x)]
    for v in res_prefix:
        # df.loc[df[v].isnull(), v] = 0
        df[v] = df[v].fillna(0)
    print('confirm4', list(df))

    df = pd.merge(df, p1, on=["lbl"], how="outer", suffixes=('','_drop'))
    for ton in ["sab", "sabb"]:
        for g in ["nm", "m"]:
            df[f"conf_{g}{ton}_ofpopr"] = 100 * df[f"{g}{ton}_binage"] / df[f"{g}{ton}_oneton_pop"]
            df[f"conf_{g}{ton}_ofputr"] = 100 * df[f"{g}{ton}_binage"] / df[f"put_{g}{ton}"]

    print('confirm5', list(df))
    df = df[['lbl', 'modalrepop', 'nonmodalrepop', 'sab_onetonpop', 'sabb_onetonpop', 'nmsab_oneton_pop', 'nmsabb_oneton_pop', 'msab_oneton_pop', 'msabb_oneton_pop', 'put_nmsab_ofpopr', 'conf_nmsab_ofpopr', 'conf_nmsab_ofputr', 'put_nmsabb_ofpopr', 'conf_nmsabb_ofpopr', 'conf_nmsabb_ofputr', 'put_msab_ofpopr', 'conf_msab_ofpopr', 'conf_msab_ofputr', 'put_msabb_ofpopr', 'conf_msabb_ofpopr', 'conf_msabb_ofputr']]
    for v in ['modalrepop', 'nonmodalrepop', 'sab_onetonpop', 'sabb_onetonpop', 'nmsab_oneton_pop', 'nmsabb_oneton_pop', 'msab_oneton_pop', 'msabb_oneton_pop']:
        print('col', df[v])
        df[v].map('${:,}'.format)
    res_ofpop_ofput = [x for x in list(df) if re.match(".*_ofpopr", x)or re.match(".*_ofputr", x)]
    for v in res_ofpop_ofput:
        df[v].map('${:20,.4f}'.format)

    # rows alphabetical by lbl
    df = df.sort_values("lbl")

    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004_tables.xlsx", mode="a", if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name="putconf_oneton_modal_nonmodal", index=False)

    #import excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(putative) clear first
    #addEmptyFzyage
    #renameAge
    #replace ccbpas_binage = ccbpas_fzyage if missing(ccbpas_binage)
    #replace ccbpabs_binage = ccbpabs_fzyage if missing(ccbpabs_binage)

    df = pd.read_excel(f'{disclosure}/CBDRB-FY22-DSEP-004.xlsx',sheet_name='putative')
    addEmptyFzyage(df)
    renameAge(df)
    df["ccbpas_binage"] = df["ccbpas_binage"].fillna(df["ccbpas_fzyage"])
    df["ccbpabs_binage"] = df["ccbpabs_binage"].fillna(df["ccbpabs_fzyage"])

    # foreach v of varlist ccbpas_* ccbpabs_* {
    #     replace `v' = 0 if missing(`v')
    # }
    # gen put_ccbpas_ofpopr = 100*ccbpas_binage/ccbpaspop
    # gen put_ccbpabs_ofpopr = 100*ccbpabs_binage/ccbpabspop
    # gen put_ccbpas = ccbpas_binage
    # gen put_ccbpabs = ccbpabs_binage
    # keep lbl put_ccbpas_ofpopr put_ccbpabs_ofpopr put_ccbpas put_ccbpabs
    # tempfile p1
    # save `p1'
    for v in [x for x in list(df) if re.match("ccbpas_.*", x) or re.match("ccbpabs_.*", x)]:
        df[v] = df[v].fillna(0)
    df["put_ccbpas_ofpopr"] = 100 * df["ccbpas_binage"] / df["ccbpaspop"]
    df["put_ccbpabs_ofpopr"] = 100 * df["ccbpabs_binage"] / df["ccbpabspop"]
    df["put_ccbpas"] = df["ccbpas_binage"]
    df["put_ccbpabs"] = df["ccbpabs_binage"]
    df = df[["lbl", "put_ccbpas_ofpopr", "put_ccbpabs_ofpopr", "put_ccbpas", "put_ccbpabs"]]
    p1 = df

    #import excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(confirm) clear first    #
    #addEmptyFzyage
    #renameAge
    #replace ccbpas_binage = ccbpas_fzyage if missing(ccbpas_binage)
    #replace ccbpabs_binage = ccbpabs_fzyage if missing(ccbpabs_binage)
    df = pd.read_excel(f'{disclosure}/CBDRB-FY22-DSEP-004.xlsx',sheet_name='confirm')
    addEmptyFzyage(df)
    renameAge(df)
    df["ccbpas_binage"] = df["ccbpas_binage"].fillna(df["ccbpas_fzyage"])
    df["ccbpabs_binage"] = df["ccbpabs_binage"].fillna(df["ccbpabs_fzyage"])

    #foreach v of varlist ccbpas_* ccbpabs_* {
    #    replace `v' = 0 if missing(`v')
    #}
    #merge 1:1 lbl using `p1', nogen
    #gen conf_ccbpas_ofpopr = 100*ccbpas_binage/ccbpaspop
    #gen conf_ccbpabs_ofpopr = 100*ccbpabs_binage/ccbpabspop

    #gen conf_ccbpas_ofputr = 100*ccbpas_binage/put_ccbpas
    #gen conf_ccbpabs_ofputr = 100*ccbpabs_binage/put_ccbpabs

    for v in [x for x in list(df) if re.match("ccbpas_.*", x) or re.match("ccbpabs_.*", x)]:
        df[v] = df[v].fillna(0)

    # NOTE: is Stata 1:1 a Pandas outer-type join?
    df = pd.merge(df, p1, on=["lbl"], how="outer", suffixes=('','_drop'))

    df["conf_ccbpas_ofpopr"] = 100 * df["ccbpas_binage"] / df["ccbpaspop"]
    df["conf_ccbpabs_ofpopr"] = 100 * df["ccbpabs_binage"] / df["ccbpabspop"]
    df["conf_ccbpas_ofputr"] = 100 * df["ccbpas_binage"] / df["put_ccbpas"]
    df["conf_ccbpabs_ofputr"] = 100 * df["ccbpabs_binage"] / df["put_ccbpabs"]

    # keep lbl ccbpaspop ccbpabspop put_ccbpas_ofpopr conf_ccbpas_ofpopr conf_ccbpas_ofputr put_ccbpabs_ofpopr conf_ccbpabs_ofpopr conf_ccbpabs_ofputr
    # order lbl ccbpaspop ccbpabspop put_ccbpas_ofpopr conf_ccbpas_ofpopr conf_ccbpas_ofputr put_ccbpabs_ofpopr conf_ccbpabs_ofpopr conf_ccbpabs_ofputr
    # format ccbpaspop ccbpabspop %20.0gc
    # format *_ofpopr *_ofputr %20.4f
    # export excel using $disclosure/CBDRB-FY22-DSEP-004_tables.xlsx, sheet(putconf_ccbpas_ccbpabs) firstrow(variables) sheetmodify

    df = df[["lbl", "ccbpaspop", "ccbpabspop", "put_ccbpas_ofpopr", "conf_ccbpas_ofpopr", "conf_ccbpas_ofputr", "put_ccbpabs_ofpopr", "conf_ccbpabs_ofpopr", "conf_ccbpabs_ofputr"]]
    for v in ['ccbpaspop', 'ccbpabspop']:
        df[v].map('${:20,.0f}'.format)
    res_ofpop_ofput = [x for x in list(df) if re.match(".*_ofpopr$", x) or re.match(".*_ofputr$", x)]
    for v in res_ofpop_ofput:
        df[v].map('${:20,.4f}'.format)

    # rows alphabetical by lbl
    df = df.sort_values("lbl")

    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004_tables.xlsx", mode="a", if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name="putconf_ccbpas_ccbpabs", index=False)

    # *--------------------------------------------------
    # * BLOCK SIZE PUTATIVE & CONFIRM RATES
    # *--------------------------------------------------
    print(".*--------------------------------------------------")
    print(".* BLOCK SIZE PUTATIVE & CONFIRM RATES")
    print(".*--------------------------------------------------")

    # import excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(putative_blksz) clear first
    # addEmptyFzyage
    # renameAge
    # rename cefpop pop
    # foreach v of varlist *binage_binage {
    #     gen p_`v'_ofpopr = 100*(`v'/keepblockpik)
    #     gen p_`v' = `v'
    # }
    # keep block_pop pop keepblockpik p_*
    # tempfile p1
    # save `p1'
    df = pd.read_excel(f'{disclosure}/CBDRB-FY22-DSEP-004.xlsx',sheet_name='putative_blksz')
    addEmptyFzyage(df)
    renameAge(df)
    df = df.rename(columns={'cefpop':'pop'})

    res_binage = [x for x in list(df) if re.match(".*binage_binage$", x)]
    for v in res_binage:
        df[f"p_{v}_ofpopr"] = 100 * df[v] / df["keepblockpik"]
        df[f"p_{v}"] = df[v]

    p1 = df[["block_pop", "pop", "keepblockpik"] + [x for x in list(df) if re.match("p_.*", x)]]

    # import excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(confirm_blksz) clear first
    # renameAge
    #
    # desc *binage_binage, varlist
    # local binageVars = "`r(varlist)'"
    #
    # merge 1:1 block_pop using `p1', nogen
    # foreach v in `binageVars' {
    #     gen c_`v'_ofpopr = 100*(`v'/keepblockpik)
    #     gen c_`v'_ofputr = 100*(`v'/p_`v')
    # }
    # keep block_pop pop keepblockpik *_ofpopr *_ofputr
    # format pop keepblockpik %20.0gc
    # format *_ofpopr *_ofputr %20.4f
    # order block_pop pop keepblockpik
    # replace block_pop = "1-9" if block_pop=="0-9"
    # fixBlockPopSort
    # export excel using $disclosure/CBDRB-FY22-DSEP-004_tables.xlsx, sheet(putconf_blksz_basic) firstrow(variables) sheetmodify

    df = pd.read_excel(f'{disclosure}/CBDRB-FY22-DSEP-004.xlsx',sheet_name='confirm_blksz')
    renameAge(df)
    varlist = list(df)
    binageVars = [x for x in varlist if re.match(".*binage_binage$", x)]
    #for var in [x for x in varlist if re.match(".*binage_binage$", x)]:
        #print(df[var])
    #binageVars = list(df)

    # drop keepblockpik column to prevent duplicate on merge
    temp = df.drop(columns=['keepblockpik'])
    print('before', list(df))
    df = pd.merge(temp, p1, on=["block_pop"], how="outer", suffixes=('','_drop'))
    print('after', list(df))
    for v in binageVars:
        df[f"c_{v}_ofpopr"] = 100 * df[v] / df["keepblockpik"]
        df[f"c_{v}_ofputr"] = 100 * df[v] / df[f"p_{v}"]

    res_ofpop_ofput = [x for x in list(df) if re.match(".*_ofpopr", x) or re.match(".*_ofputr", x)]
    df = df[["block_pop", "pop", "keepblockpik"] + res_ofpop_ofput]
    print('keepblock', df.keepblockpik)

    for v in ["pop", "keepblockpik"]:
        df[v].map('${:20,.0f}'.format)
    for v in res_ofpop_ofput:
        df[v].map('${:20,.4f}'.format)

    df = df[["block_pop", "pop", "keepblockpik"] + res_ofpop_ofput]
    df.loc[df["block_pop"] == "0-9", "block_pop"] = "1-9"
    fixBlockPopSort(df)
    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004_tables.xlsx", mode="a", if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name="putconf_blksz_basic", index=False)

    # *--------------------------------------------------
    # * SOLVAR
    # *--------------------------------------------------
    print("*--------------------------------------------------")
    print("* SOLVAR")
    print("*--------------------------------------------------")

    # import excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(putative_solvar) clear first
    # renameAge
    #
    # keep solvar keepblockpik *binage
    # sort solvar
    # local solvarpopzero = keepblockpik[1]
    # local solvarpopgtzero = keepblockpik[2]
    # local solvarpopna = keepblockpik[3]
    # recodeSolvar
    df = pd.read_excel(f'{disclosure}/CBDRB-FY22-DSEP-004.xlsx',sheet_name='putative_solvar')
    print('keepblock', df.keepblockpik)
    renameAge(df)

    df = df[["solvar", "keepblockpik"] + [x for x in list(df) if re.match(".*binage$", x)]]
    df = df.sort_values(["solvar"])
    print('keepblock', df.keepblockpik)
    solvarpopzero = df["keepblockpik"][0]
    solvarpopgtzero = df["keepblockpik"][1]
    # Is this necessary? The row for solvarpopna doesn't appear in the official results
    #df["solvarpopna"] = df["keepblockpik"][2]
    recodeSolvar(df)
    print('postrecode', list(df))

    # tempfile p1
    # quietly desc *binage, varlist
    # local start = 1
    # foreach v in `r(varlist)' {
    #     preserve
    #     if regexm("`v'","_nm")==0 & regexm("`v'","_sab")==0 & regexm("`v'","_m")==0 & regexm("`v'","_ber")==0 & regexm("`v'","21")==0 & regexm("`v'","ccbpa")==0 {
    #         local stem = subinstr("`v'","binage_binage","",.)
    #         local stem = substr("`stem'",1,3) + "-" + substr("`stem'",4,3) + ", binage"
    #         keep solvar `v'
    #         gen lbl = "`stem'"
    #         rename `v' put_
    #         reshape wide put_ , i(lbl) j(solvar) string
    #         if `start' == 1 {
    #             save `p1', replace
    #         }
    #         else {
    #             append using `p1'
    #             save `p1', replace
    #         }
    #         local start = 0
    #     }
    #     restore
    # }
    start = 1
    varlist = [col for col in list(df) if col != 'solvar']
    print(varlist, 'varlist')
    for v in varlist:
        df_preserve = df.copy()
        #if re.match(".*_nm.*", f"{v}") == None and re.match(".*_sab.*", f"{v}") == None and re.match(".*_m.*", f"{v}") == None and re.match(f"{v}", "_ber") == None and re.match(f"{v}", "21") == None and re.match(f"{v}", "_ccbpa") == None:
        if all(substring not in v for substring in ["_nm", "_sab", "_m", "_ber", "21", "ccbpa"]):
            stem = v.replace("binage_binage", "")
            stem = f"{stem[:3]}-{stem[3:6]}, binage" # first 3 characters
            df = df[["solvar", f"{v}"]]
            df["lbl"] = f"{stem}"
            #print(list(df), 'dfcolumns')
            df = df.rename(columns={v: "put_"})

            df = long_to_wide(df, "lbl", "solvar", "put_")

            #print('df columns', list(df))
            #print(df)
            #df = df.unstack('lbl')
            #print(p1)

            if start == 1:
                p1 = df
            else:
                p1 = pd.concat([df, p1], ignore_index=True)
            start = 0
        #print('p1', p1)
        df = df_preserve.copy()

    # import excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(confirm_solvar) clear first
    # renameAge
    #
    # keep solvar keepblockpik *binage
    # recodeSolvar
    # tempfile p2
    # quietly desc *binage, varlist
    # local start = 1
    # foreach v in `r(varlist)' {
    #     preserve
    #     if regexm("`v'","_nm")==0 & regexm("`v'","_sab")==0 & regexm("`v'","_m")==0 & regexm("`v'","_ber")==0 & regexm("`v'","21")==0 & regexm("`v'","ccbpa")==0 {
    #         local stem = subinstr("`v'","binage_binage","",.)
    #         local stem = substr("`stem'",1,3) + "-" + substr("`stem'",4,3) + ", binage"
    #         keep solvar `v'
    #         gen lbl = "`stem'"
    #         rename `v' conf_
    #         reshape wide conf_ , i(lbl) j(solvar) string
    #         if `start' == 1 {
    #             save `p2', replace
    #         }
    #         else {
    #             append using `p2'
    #             save `p2', replace
    #         }
    #         local start = 0
    #     }
    #     restore
    # }
    df = pd.read_excel(f'{disclosure}/CBDRB-FY22-DSEP-004.xlsx',sheet_name='confirm_solvar')
    renameAge(df)

    res_binage = [x for x in list(df) if re.match(".*binage$", x)]
    df = df[["solvar", "keepblockpik"] + res_binage]
    p2 = df.copy()
    start = 1
    varlist = [col for col in list(df) if col != 'solvar']
    for v in varlist:
        df_preserve = df.copy()
        if all(substring not in v for substring in ["_nm", "_sab", "_m", "_ber", "21", "ccbpa"]):
            stem = v.replace("binage_binage", "")
            stem = f"{stem[:3]}-{stem[3:6]}, binage" # first 3 characters
            df = df[["solvar", f"{v}"]]
            df["lbl"] = f"{stem}"
            df = df.rename(columns={v: "conf_"})
            df = long_to_wide(df, "lbl", "solvar", "conf_")
            df = df.rename(columns={"conf_0": "conf_zero", "conf_>0":"conf_gtzero"})

            print('df columns', list(df))
            print(df)
            print(p1)

            if start == 1:
                p2 = df.copy()
            else:
                df = df.append(p2)
                p2 = df.copy()
            start = 0
        df = df_preserve.copy()

    # use `p1', clear
    # merge 1:1 lbl using `p2', nogen
    # gen double pop_zero = `solvarpopzero'
    # gen double pop_gtzero = `solvarpopgtzero'
    # foreach s in zero gtzero {
    #     gen put_`s'_ofpopr = 100*(put_`s'/pop_`s')
    #     gen conf_`s'_ofpopr = 100*(conf_`s'/pop_`s')
    #     gen conf_`s'_ofputr = 100*(conf_`s'/put_`s')
    # }
    # keep lbl pop_zero pop_gtzero *_ofpopr *_ofputr
    # order lbl pop_zero pop_gtzero
    # format pop* %20.0gc
    # format *_ofpopr *_ofputr %20.4f
    # export excel using $disclosure/CBDRB-FY22-DSEP-004_tables.xlsx, sheet(putconf_solvar_basic) firstrow(variables) sheetmodify
    df = p1.copy()
    #print('df', list(df))

    df = pd.merge(df, p2, on=["lbl"], suffixes=('','_drop'))

    df["pop_zero"] = solvarpopzero # should be type float (all three)
    df["pop_gtzero"] = solvarpopgtzero
    #df["pop_NA"] = df[solvarpopna]

    for s in ["zero", "gtzero"]:
        print("considering solvar",s)
        if f"put_{s}" in df and f"pop_{s}" in df: df[f"put_{s}_ofpopr"] = 100 * df[f"put_{s}"] / df[f"pop_{s}"]
        if f"conf_{s}" in df and f"pop_{s}" in df: df[f"conf_{s}_ofpopr"] = 100 * df[f"conf_{s}"] / df[f"pop_{s}"]
        if f"conf_{s}" in df and f"put_{s}" in df: df[f"conf_{s}_ofputr"] = 100 * df[f"conf_{s}"] / df[f"put_{s}"]

    res_ofpop_ofput = [x for x in list(df) if re.match(".*_ofpopr", x)or re.match(".*_ofputr", x)]
    df = df[["lbl", "pop_zero", "pop_gtzero"] + res_ofpop_ofput]

    for v in [x for x in list(df) if re.match("pop.*", x)]:
        df[v].map('${:20,.0f}'.format)
    for v in res_ofpop_ofput:
        df[v].map('${:20,.4f}'.format)


    # clear out non-data keepblockpik row
    df = df[df.lbl != "kee-pbl, binage"]

    # rows alphabetical by lbl
    df = df.sort_values("lbl")

    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004_tables.xlsx", mode="a", if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name="putconf_solvar_basic", index=False)

    # import excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(putative_solvar) clear first
    # renameAge
    # keep solvar keepblockpik *binage
    # recodeSolvar
    # tempfile p1
    # tempfile hold
    # save `hold'
    # local start = 1
    # foreach i in o21 u21 m nm msab msabb nmsab nmsabb ccbpas ccbpabs {
    #     use `hold', clear
    #     keep solvar *_`i'_binage
    #     quietly desc *_`i'_binage, varlist
    #     foreach v in `r(varlist)' {
    #         preserve
    #         local stem = subinstr("`v'","binage_binage","",.)
    #         local stem = substr("`stem'",1,3) + "-" + substr("`stem'",4,3) + ", binage"
    #         keep solvar `v'
    #         gen lbl = "`stem'"
    #         rename `v' put_`i'_
    #         reshape wide put_ , i(lbl) j(solvar) string
    #         if `start' == 1 {
    #             save `p1', replace
    #         }
    #         else {
    #             merge 1:1 lbl using `p1', nogen
    #             save `p1', replace
    #         }
    #         local start = 0
    #         restore
    #     }
    # }
    df = pd.read_excel(f'{disclosure}/CBDRB-FY22-DSEP-004.xlsx',sheet_name='putative_solvar')
    renameAge(df)

    res_binage = [x for x in list(df) if re.match(".*binage$", x)]
    df = df[["solvar", "keepblockpik"] + res_binage]
    recodeSolvar(df)

    print("detail begin")

    #p1 = df.copy()
    hold = df.copy()
    start = 1
    for i in ["o21", "u21", "m", "nm", "msab", "msabb", "nmsab", "nmsabb", "ccbpas", "ccbpabs"]:
        df = hold.copy()
        varlist = [x for x in list(df) if re.match(f".*_{i}_binage", x)]
        for v in varlist:
            print("top of varlist")
            print("p1", p1)

            df_preserve = df.copy()
            stem = v.replace("binage_binage", "")
            stem = f"{stem[:3]}-{stem[3:6]}, binage" # first 3 characters
            print("reading", v, "from df")
            #print("  ", list(df))
            df = df[["solvar", f"{v}"]]
            df["lbl"] = f"{stem}"
            df = df.rename(columns={v: f"put_{i}_"})

            df = long_to_wide(df, "lbl", "solvar", f"put_{i}_")
            if f"put_{i}_NA" not in df: df[f"put_{i}_NA"] = 0

            if start == 1:
                print("first case")
                p1 = df.copy()
                p1 = p1.set_index("lbl")
            else:
                print("second case")
                print("p1", p1)
                print("df", df)
                df = df.set_index("lbl")
                df = p1.combine_first(df)
                #df = pd.merge(df, p1, on=["lbl"], how="outer", suffixes=('','_drop'))
                p1 = df.copy()

            print("bottom of varlist loop")
            print("p1", p1)

            start = 0
            df = df_preserve.copy()

    p1 = p1.reset_index()
    print("done p1", p1)

    # import excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(confirm_solvar) clear first
    # renameAge
    # keep solvar keepblockpik *binage
    # recodeSolvar
    # tempfile p2
    # quietly desc *binage, varlist
    # tempfile hold
    # save `hold'
    # local start = 1
    # foreach i in o21 u21 m nm msab msabb nmsab nmsabb ccbpas ccbpabs {
    #     use `hold', clear
    #     keep solvar *_`i'_binage
    #     quietly desc *_`i'_binage, varlist
    #     foreach v in `r(varlist)' {
    #         preserve
    #         local stem = subinstr("`v'","binage_binage","",.)
    #         local stem = substr("`stem'",1,3) + "-" + substr("`stem'",4,3) + ", binage"
    #         keep solvar `v'
    #         gen lbl = "`stem'"
    #         rename `v' conf_`i'_
    #         reshape wide conf , i(lbl) j(solvar) string
    #         if `start' == 1 {
    #             save `p2', replace
    #         }
    #         else {
    #             merge 1:1 lbl using `p2', nogen
    #             save `p2', replace
    #         }
    #         local start = 0
    #         restore
    #     }
    # }
    df = pd.read_excel(f'{disclosure}/CBDRB-FY22-DSEP-004.xlsx',sheet_name='confirm_solvar')
    renameAge(df)

    res_binage = [x for x in list(df) if re.match(".*binage$", x)]
    df = df[["solvar", "keepblockpik"] + res_binage]
    recodeSolvar(df)

    #p2 = df.copy()
    hold = df.copy()
    start = 1
    for i in ["o21", "u21", "m", "nm", "msab", "msabb", "nmsab", "nmsabb", "ccbpas", "ccbpabs"]:
        df = hold.copy()
        varlist = [x for x in list(df) if re.match(f".*_{i}_binage$", x)]
        for v in varlist:
            df_preserve = df.copy()
            stem = v.replace("binage_binage", "")
            stem = f"{stem[:3]}-{stem[3:6]}, binage" # first 3 characters
            df = df[["solvar", f"{v}"]]
            df["lbl"] = f"{stem}"
            df = df.rename(columns={v: f"conf_{i}_"})
            df = long_to_wide(df, "lbl", "solvar", f"conf_{i}_")
            if f"conf_{i}_NA" not in df: df[f"conf_{i}_NA"] = 0

            if start == 1:
                p2 = df.copy()
                p2 = p2.set_index("lbl")
            else:
                df = df.set_index("lbl")
                df = p2.combine_first(df)
                #df = pd.merge(df, p2, on=["lbl"], how="inner", suffixes=('','_drop'))
                p2 = df.copy()
            start = 0
            df = df_preserve.copy()

    p2 = p2.reset_index()

    # use $temp/cefnatpop, clear
    # local kbp = keepblockpik[1]
    # keep keepblockpik *sv*
    # FIXME: temp gets clobbered somewhere so I am hard-coding temp
    temp="temp"
    cefnatpop = pd.read_csv(f'{temp}/cefpopnat.csv')
    df = cefnatpop.copy()
    print('before res_sv', list(df))
    print("cefpopnat", list(df))
    kbp = df["keepblockpik"][0]
    res_sv = [x for x in list(df) if re.match(".*sv.*", x)]
    df = df[["keepblockpik"] + res_sv]
    print('before replacement', list(df))

    # quietly desc modalre*, varlist
    # foreach v in `r(varlist)' {
    #     local newname = subinstr("`v'","modalre_","m_",.)
    #     rename `v' `newname'
    # }

    # quietly desc nonmodalre*, varlist
    # foreach v in `r(varlist)' {
    #     local newname = subinstr("`v'","nonmodalre_","nm_",.)
    #     rename `v' `newname'
    # }
    varlist = list(df)
    for v in varlist:
        newname = v.replace("nonmodalre_", "nm_")
        df = df.rename(columns={v: newname})

    varlist = list(df)
    for v in varlist:
        newname = v.replace("modalre_", "m_")
        df = df.rename(columns={v: newname})


    # quietly desc *oneton*, varlist
    # foreach v in `r(varlist)' {
    #     local newname = subinstr("`v'","_oneton_","_",.)
    #     rename `v' `newname'
    # }
    varlist = list(df)
    for v in varlist:
        newname = v.replace("_oneton_", "_")
        df = df.rename(columns={v: newname})

    # foreach s in zero gtzero {
    #     quietly desc *sv`s', varlist
    #     foreach v in `r(varlist)' {
    #         local newname = "`v'"+"pop"
    #         rename `v' `newname'
    #     }
    # }
    # tempfile denominators
    # save `denominators'
    
    for s in ["zero", "gtzero"]:
        varlist = [x for x in list(df) if re.match(f".*sv{s}([^p]|$)", x)]
        for v in varlist:
            newname = f"{v}pop"
            df = df.rename(columns={v: newname})
    denominators = df.copy()

    # use `p1', clear
    # merge 1:1 lbl using `p2', nogen
    # gen double keepblockpik = `kbp'
    # merge m:1 keepblockpik using `denominators', nogen
    # foreach i in o21 u21 m nm msab msabb nmsab nmsabb ccbpas ccbpabs {
    #     foreach s in zero gtzero {
    #         gen put_`i'_`s'_ofpopr = 100*(put_`i'_`s'/`i'_sv`s'pop)
    #         gen conf_`i'_`s'_ofpopr = 100*(conf_`i'_`s'/`i'_sv`s'pop)
    #         gen conf_`i'_`s'_ofputr = 100*(conf_`i'_`s'/put_`i'_`s')
    #     }
    # }
    # keep lbl *pop *_ofpopr *_ofputr
    # order lbl *pop
    # format *pop %20.0gc
    # format *_ofpopr *_ofputr %20.4f
    # export excel using $disclosure/CBDRB-FY22-DSEP-004_tables.xlsx, sheet(putconf_solvar_detail) firstrow(variables) sheetmodify
    df = p1.copy()
    print('p1 columns', list(df))
    df = pd.merge(df, p2, on=["lbl"], suffixes=('','_drop'))

    print("merge nonce")

    print(p1)
    print(p2)
    print(df)

    df["keepblockpik"] = kbp
    df = pd.merge(df, denominators, on=["keepblockpik"], how="inner", suffixes=('','_drop'))

    print(denominators)

    for i in ["o21", "u21", "m", "nm", "msab", "msabb", "nmsab", "nmsabb", "ccbpas", "ccbpabs"]:
        for s in ["zero", "gtzero"]:
            df[f"put_{i}_{s}_ofpopr"] = 100 * df[f"put_{i}_{s}"] / df[f"{i}_sv{s}pop"]
            df[f"conf_{i}_{s}_ofpopr"] = 100 * df[f"conf_{i}_{s}"] / df[f"{i}_sv{s}pop"]
            df[f"conf_{i}_{s}_ofputr"] = 100 * df[f"conf_{i}_{s}"] / df[f"put_{i}_{s}"]

    res_pop = [x for x in list(df) if re.match(".*pop$", x)]
    res_ofpop_ofput = [x for x in list(df) if re.match(".*_ofpopr", x)or re.match(".*_ofputr", x)]
    df = df[["lbl"] + res_pop + res_ofpop_ofput]

    for v in res_pop:
        df[v].map('${:20,.0f}'.format)
    for v in res_ofpop_ofput:
        df[v].map('${:20,.4f}'.format)

    # rows alphabetical by lbl
    df = df.sort_values("lbl")

    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004_tables.xlsx", mode="a", if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name="putconf_solvar_detail", index=False)

    # *--------------------------------------------------
    # * BLOCK-PIK DUP STATES
    # *--------------------------------------------------
    print("*--------------------------------------------------")
    print("* BLOCK-PIK DUP STATES")
    print("*--------------------------------------------------")

    # import excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(pikdups) clear first
    # export excel using $disclosure/CBDRB-FY22-DSEP-004_tables.xlsx, sheet(pikdups) firstrow(variables) sheetmodify
    df = pd.read_excel(f'{disclosure}/CBDRB-FY22-DSEP-004.xlsx',sheet_name='pikdups')
    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004_tables.xlsx", mode="a", if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name="pikdups", index=False)

    # *--------------------------------------------------
    # * BLOCK-PIK CEF-CMRCL MATCH STATS
    # *--------------------------------------------------
    print("*--------------------------------------------------")
    print("* BLOCK-PIK CEF-CMRCL MATCH STATS")
    print("*--------------------------------------------------")


    # import excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(putative) clear first
    # local kbp = keepblockpik[1]
    df = pd.read_excel(f'{disclosure}/CBDRB-FY22-DSEP-004.xlsx',sheet_name='putative')
    kbp = df["keepblockpik"][0]

    # import delimited using $rsltbase/cmrcl/cmrcl_cef_bpas_match_counts.csv, clear
    # collapse (sum) cef_all cef_keep cmrcl_all cmrcl_has_bp cmrcl_has_bpas bp bpas bpabs
    # rename bp cmrcl_cef_mtch_blkpik
    # rename bpas cmrcl_cef_mtch_blkpikagesex
    #rename bpabs cmrcl_cef_mtch_blkpikagebsex
    #gen mtch_blkpik_of_keep = 100*(cmrcl_cef_mtch_blkpik/cef_keep)
    #gen mtch_blkpikagesex_of_keep = 100*(cmrcl_cef_mtch_blkpikagesex/cef_keep)
    #gen mtch_blkpikagebsex_of_keep = 100*(cmrcl_cef_mtch_blkpikagebsex/cef_keep)
    #order cef_all cef_keep cmrcl_all cmrcl_has_bp cmrcl_has_bpas cmrcl_cef_mtch_blkpik mtch_blkpik_of_keep cmrcl_cef_mtch_blkpikagesex mtch_blkpikagesex_of_keep cmrcl_cef_mtch_blkpikagebsex mtch_blkpikagebsex_of_keep
    #format cef_all cef_keep cmrcl_all cmrcl_has_bp cmrcl_has_bpas cmrcl_cef_mtch_blkpik cmrcl_cef_mtch_blkpikagesex cmrcl_cef_mtch_blkpikagebsex  %20.0gc 
    #format *_of_keep %20.4f
    # export excel using $disclosure/CBDRB-FY22-DSEP-004_tables.xlsx, sheet(cmrcl_cef_match) firstrow(variables)
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

    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004_tables.xlsx", mode="a", if_sheet_exists="replace") as writer:
        sums.to_excel(writer, sheet_name="cmrcl_cef_match", index=False)

    #*--------------------------------------------------
    #* SOLVAR STATS
    #*--------------------------------------------------    

    # foreach v in agr pr cr prcn {
    #     import excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(solvar_`v') clear first
    #     export excel using $disclosure/CBDRB-FY22-DSEP-004_tables.xlsx, sheet(solvar_`v') firstrow(variables) 
    # }
    # foreach v in pr cr prcn {
    #     import excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(solvar_`v'_nmsabb) clear first
    #     export excel using $disclosure/CBDRB-FY22-DSEP-004_tables.xlsx, sheet(solvar_`v'_nmsabb) firstrow(variables) 
    # }

    for v in ["agr", "pr", "cr", "prcn"]:
        df = pd.read_excel(f'{disclosure}/CBDRB-FY22-DSEP-004.xlsx',sheet_name=f'solvar_{v}')
        with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004_tables.xlsx", mode="a", if_sheet_exists="replace") as writer:
            df.to_excel(writer, sheet_name=f"solvar_{v}", index=False)
    for v in ["pr", "cr", "prcn"]:
        df = pd.read_excel(f'{disclosure}/CBDRB-FY22-DSEP-004.xlsx',sheet_name=f'solvar_{v}_nmsabb')
        with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004_tables.xlsx", mode="a", if_sheet_exists="replace") as writer:
            df.to_excel(writer, sheet_name=f"solvar_{v}_nmsabb", index=False)
            
    df = pd.read_excel(f'{disclosure}/CBDRB-FY22-DSEP-004.xlsx',sheet_name=f'solvar_qtile')
    with pd.ExcelWriter(f"{disclosure}/CBDRB-FY22-DSEP-004_tables.xlsx", mode="a", if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name=f"solvar_qtile", index=False)
            
    # di "#### END OF MAKE TABLES ####"
    # di c(current_date) " " c(current_time)
    print("#### END OF MAKE TABLES ####")
    now = datetime.now
    print(now())

if __name__ == '__main__':
    main()
