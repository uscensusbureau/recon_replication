*--------------------------------------------------
* REID/RECON Tables and Figures
* 
* Example execution: 
*>setsid stata-se -b make_tables.do
*
* Requirements: 
* - two sub directories where the program is 
* located, one called 'in' that contains the counts 
* from `reidpaper` and the other called `out' that 
* will have the tex tables after execution 
* - the name of the counts file is in global 
* countsFile below
* 
*--------------------------------------------------

*--------------------------------------------------
* PROGRAM SETUP
*--------------------------------------------------
version 16
set more off
set linesize 80
capture log close
set type double
local dt="`c(current_date)' `c(current_time)'"
local dt=subinstr("`dt'",":","",.)
local dt=subinstr("`dt'"," ","",.)
log using make_tables_`dt'.log, replace

*--------------------------------------------------
* MACROS
*--------------------------------------------------
global out "./out/"
global in "./in/" 
global countsFile "CBDRB-FY22-DSEP-004.xlsx"

capture fourSigRound drop fourSigRound
program define fourSigRound
    gen double `1'd = `1'
    drop `1'
    rename `1'd `1'
    gen fourSigRound_neg = cond(`1'<0,-1,1)
    replace `1'=abs(`1')
    replace `1'=0  if `1'<0.00000001   
    replace `1'=round(`1',0.00000000001)  if `1'>=0.00000001 &  `1'<0.0000001  
    replace `1'=round(`1',0.0000000001)  if `1'>=0.0000001 &  `1'<0.000001  
    replace `1'=round(`1',0.000000001)  if `1'>=0.000001 &  `1'<0.00001  
    replace `1'=round(`1',0.00000001)  if `1'>=0.00001 &  `1'<0.0001 
    replace `1'=round(`1',0.0000001)  if `1'>=0.0001 &  `1'<0.001 
    replace `1'=round(`1',0.000001)  if `1'>=0.001 &  `1'<0.01  
    replace `1'=round(`1',0.00001)  if `1'>=0.01 &  `1'<0.1   
    replace `1'=round(`1',0.0001)  if `1'>=0.1 &  `1'<1  
    replace `1'=round(`1',0.001)  if `1'>=1 &  `1'<10  
    replace `1'=round(`1',0.01)  if `1'>=10 &  `1'<100 
    replace `1'=round(`1',0.1)  if `1'>=100 &  `1'<1000   
    replace `1'=round(`1',1)  if `1'>=1000 &  `1'<10000  
    replace `1'=round(`1',10) if `1'>=10000 & `1'<100000
    replace `1'=round(`1',100) if `1'>=100000 & `1'<1000000
    replace `1'=round(`1',1000) if `1'>=1000000 & `1'<10000000
    replace `1'=round(`1',10000) if `1'>=10000000 & `1'<100000000
    replace `1'=round(`1',100000) if `1'>=100000000 & `1'<1000000000
    replace `1'=round(`1',1000000) if `1'>=1000000000 & `1'<10000000000
    replace `1'=`1'*fourSigRound_neg
    drop fourSigRound_neg
end

capture sortBlockpop drop sortBlockpop
program define sortBlockpop
    gen s1 = .
    replace s1 = 0 if block_pop == "Overall"
    replace s1 = 1 if block_pop == "0-9"
    replace s1 = 1 if block_pop == "1-9"
    replace s1 = 2 if block_pop == "10-49"
    replace s1 = 3 if block_pop == "50-99"
    replace s1 = 4 if block_pop == "100-249"
    replace s1 = 5 if block_pop == "250-499"
    replace s1 = 6 if block_pop == "500-999"
    replace s1 = 7 if block_pop == "1000+"
    sort s1
end

capture insertCommas drop insertCommas
program define insertCommas
    args v
    gen S`v' = `v'
    tostring S`v', replace force

    replace S`v' = substr(S`v',1,3) + "," + substr(S`v',-6,3) + "," + substr(S`v',-3,.) if strlen(S`v') == 9
    replace S`v' = substr(S`v',1,2) + "," + substr(S`v',-6,3) + "," + substr(S`v',-3,.) if strlen(S`v') == 8
    replace S`v' = substr(S`v',1,1) + "," + substr(S`v',-6,3) + "," + substr(S`v',-3,.) if strlen(S`v') == 7

    replace S`v' = substr(S`v',1,3) + "," + substr(S`v',-3,.) if strlen(S`v') == 6
    replace S`v' = substr(S`v',1,2) + "," + substr(S`v',-3,.) if strlen(S`v') == 5
    replace S`v' = substr(S`v',1,1) + "," + substr(S`v',-3,.) if strlen(S`v') == 4

    replace S`v' = "" if S`v' == "."
    drop `v' 
    rename S`v' `v'
end

capture exportTable drop exportTable 
program define exportTable
    args outFileName
    export delimited using $out/`outFileName'.tex, replace delimit("&") dataf novarn
    export delimited using $out/`outFileName'.txt, replace delimit("|") dataf quote
end

capture labelAgreeRows drop labelAgreeRows 
program define labelAgreeRows
    replace lbl = "HDF-CEF" if lbl == "hdf-cef, binage"
    replace lbl = "\$rHDF_{b,t}\$-CEF" if lbl == "r00-cef, binage"
    replace lbl = "\$rHDF_{b}\$-CEF" if lbl == "r01-cef, binage"
    replace lbl = "\$rHDF_{b,t}\$-HDF" if lbl == "r00-hdf, binage"
    replace lbl = "\$rHDF_{b}\$-HDF" if lbl == "r01-hdf, binage"
    keep if regexm(lbl,"CEF") | regexm(lbl,"HDF")
end

capture labelMDFAgreeRows drop labelMDFAgreeRows 
program define labelMDFAgreeRows
    replace lbl = "HDF-CEF" if lbl == "hdf-cef, binage"
    replace lbl = "\$rHDF_{b,t}\$-CEF" if lbl == "r00-cef, binage"
    replace lbl = "\$rHDF_{b}\$-CEF" if lbl == "r01-cef, binage"
    replace lbl = "\$rMDF_{b,t}\$-CEF" if lbl == "r02-cef, binage"
    replace lbl = "\$MDF\$-CEF" if lbl == "r03-cef, binage"
    keep if regexm(lbl,"CEF")
end

capture getAgreeBlockPop drop getAgreeBlockPop 
program define getAgreeBlockPop
    args vars lbl
    import excel using $in/$countsFile, sheet(agree_blksz) clear first
    rename (`vars' cefpop) (exact binage denom)
    keep block_pop denom exact binage
    gen lbl = "`lbl'"
    if regexm("`lbl'","rHDF") | regexm("`lbl'","MDF") {
        replace lbl = "\$" + lbl 
        replace lbl = subinstr(lbl,"}-CEF","}\$-CEF",.)
        replace lbl = subinstr(lbl,"}-HDF","}\$-HDF",.)
    }
end

capture agreeBlockPopSortOrder drop agreeBlockPopSortOrder 
program define agreeBlockPopSortOrder
    sortBlockpop
    gen s2 = .
    replace s2 = 0 if lbl == "HDF-CEF" 
    replace s2 = 1 if regexm(lbl,"{b,t}") & regexm(lbl,"-CEF") 
    replace s2 = 2 if regexm(lbl,"{b}") & regexm(lbl,"-CEF") 
    replace s2 = 3 if regexm(lbl,"{b,t}") & regexm(lbl,"-HDF") 
    replace s2 = 4 if regexm(lbl,"{b}") & regexm(lbl,"-HDF") 
    gen s3 = .
    replace s3 = 0 if lbl == "HDF-CEF" 
    replace s3 = 1 if regexm(lbl,"rHDF")  & regexm(lbl,"-CEF") 
    replace s3 = 2 if regexm(lbl,"rMDF") & regexm(lbl,"-CEF") 
    replace s3 = 3 if regexm(lbl,"MDF") & regexm(lbl,"-CEF") & !regexm(lbl,"{b,t}")
    replace s3 = 4 if regexm(lbl,"rHDF")  & regexm(lbl,"-HDF") 
    replace s3 = 5 if regexm(lbl,"rMDF") & regexm(lbl,"-HDF") 
    replace s3 = 6 if regexm(lbl,"MDF") & regexm(lbl,"-HDF") & !regexm(lbl,"{b,t}")
end

capture labelCmrlRows drop labelCmrlRows 
program define labelCmrlRows
    replace lbl = "CEF" if lbl == "cef-cmrcl, binage"
    replace lbl = "HDF" if lbl == "hdf-cmrcl, binage"
    replace lbl = "\$rHDF_{b,t}\$" if lbl == "r00-cmrcl, binage"
    replace lbl = "\$rHDF_{b}\$" if lbl == "r01-cmrcl, binage"
    replace lbl = "MDG" if lbl == "mdg-cmrcl, binage"
    replace lbl = "PRG" if lbl == "prg-cmrcl, binage"
    replace lbl = "\$rMDF_{b,t}\$" if lbl == "r02-cmrcl, binage"
    replace lbl = "MDF" if lbl == "r03-cmrcl, binage"
    keep if lbl == "CEF" | lbl == "HDF"  | lbl =="\$rHDF_{b,t}\$" | lbl =="\$rHDF_{b}\$" | lbl == "MDG" | lbl == "PRG" | lbl == "MDF" | lbl == "\$rMDF_{b,t}\$" 
end

capture labelCefRows drop labelCefRows 
program define labelCefRows
    replace lbl = "CEF" if lbl == "cef-cef, binage"
    replace lbl = "HDF" if lbl == "hdf-cef, binage"
    replace lbl = "\$rHDF_{b,t}\$" if lbl == "r00-cef, binage"
    replace lbl = "\$rHDF_{b}\$" if lbl == "r01-cef, binage"
    replace lbl = "MDG" if lbl == "mdg-cef, binage"
    replace lbl = "PRG" if lbl == "prg-cef, binage"
    replace lbl = "\$rMDF_{b,t}\$" if lbl == "r02-cef, binage"
    replace lbl = "MDF" if lbl == "r03-cef, binage"
    keep if lbl == "CEF" | lbl == "HDF" | lbl =="\$rHDF_{b,t}\$" | lbl =="\$rHDF_{b}\$" | lbl == "MDG" | lbl == "PRG" | lbl == "MDF" | lbl == "\$rMDF_{b,t}\$" 
end

capture reidSortOrder drop reidSortOrder 
program define reidSortOrder
    gen s2 = .
    replace s2 = 0 if lbl == "CEF" 
    replace s2 = 2 if lbl == "HDF" 
    replace s2 = 3 if regexm(lbl,"{b,t}") & regexm(lbl,"rHDF")
    replace s2 = 4 if regexm(lbl,"{b}") 
    replace s2 = 5 if lbl == "MDF" 
    replace s2 = 6 if regexm(lbl,"{b,t}") & regexm(lbl,"rMDF")
    replace s2 = 7 if lbl == "MDG" 
    replace s2 = 8 if lbl == "PRG"  
end

capture reidBlockPopSortOrder drop reidBlockPopSortOrder 
program define reidBlockPopSortOrder
    sortBlockpop
    reidSortOrder 
    sort s1 s2
end

capture getBlockPop drop getBlockPop 
program define getBlockPop
    args left lbl t
    import excel using $in/$countsFile, sheet(putative_blksz) clear first
    rename `left'cmrclbinage_`t'binage put_cmrcl
    rename `left'cefbinage_`t'binage put_cef
    keep block_pop put_cmrcl put_cef
    gen lbl = "`lbl'"
    tempfile part
    save `part', replace
    import excel using $in/$countsFile, sheet(confirm_blksz) clear first
    rename `left'cmrclbinage_`t'binage conf_cmrcl
    rename `left'cefbinage_`t'binage conf_cef
    keep block_pop conf_cmrcl conf_cef
    gen lbl = "`lbl'"
    merge 1:1 lbl block_pop using `part', nogen 
    if regexm("`lbl'","rHDF") | regexm("`lbl'","rMDF") {
        replace lbl = "\$" + lbl 
        replace lbl = subinstr(lbl,"}","}\$",.)
    }
end

capture getSolvarZero drop getSolvarZero 
program define getSolvarZero
    args left lbl t
    import excel using $in/$countsFile, sheet(putative_solvar) clear first
    keep if solvar == "0"
    rename `left'cmrclbinage_`t'binage put_cmrcl
    rename `left'cefbinage_`t'binage put_cef
    keep put_cmrcl put_cef
    gen lbl = "`lbl'"
    tempfile part
    save `part'
    import excel using $in/$countsFile, sheet(confirm_solvar) clear first
    keep if solvar == "0"
    rename `left'cmrclbinage_`t'binage conf_cmrcl
    rename `left'cefbinage_`t'binage conf_cef
    keep conf_cmrcl conf_cef
    gen lbl = "`lbl'"
    merge 1:1 lbl lbl using `part', nogen
    if regexm("`lbl'","rHDF") | regexm("`lbl'","rMDF") {
        replace lbl = "\$" + lbl 
        replace lbl = subinstr(lbl,"}","}\$",.)
    }
end


*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*
* clean output
shell rm $out/?*

*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*
* initialize temp files
foreach p of numlist 0(1)9 {
    tempfile part`p'
}

*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*
* CMRCL CEF OVERLAP IN DATA DEFINED PERSONS

import excel using $in/$countsFile, sheet(cmrcl_cef_match) clear first
gen recnotincef = cmrcl_has_bpas -cmrcl_cef_mtch_blkpikagebsex
rename cmrcl_cef_mtch_blkpikagebsex recincef
keep recincef recnotincef
gen lbl = "Records in COMRCL"
save `part1'

import excel using $in/$countsFile, sheet(cmrcl_cef_match) clear first
gen recincef = cef_keep - cmrcl_cef_mtch_blkpikagebsex
gen lbl ="Records not in COMRCL"
keep lbl recincef
save `part2'

use `part1', clear
append using `part2'
order lbl recincef recnotincef
gen total = recincef + recnotincef
set obs 3
replace lbl = "Total" if _n==3
replace recincef = recincef[1] + recincef[2] if _n==3
foreach v of varlist recincef recnotincef total {
    replace `v' = round(`v'/1000,1)
    fourSigRound `v'
    format `v' %20.0gc
    insertCommas `v'
}
exportTable cef_cmrcl_match 

*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*
* SOLVAR BLOCK POP
import excel using $in/$countsFile, sheet(agree_blksz) clear first
keep block_pop sabb_onetonpop 
rename sabb_onetonpop sabb_uniq
replace block_pop = "1-9" if block_pop == "0-9"
save `part1', replace
import excel using $in/$countsFile, sheet(solvar_blksz) clear first
replace block_pop = "1-9" if block_pop == "0-9"
merge 1:1 block_pop using `part1', nogen
gen zsvblk_pct = 100*zsvblk/blk
gen zsvpop_pct = 100*zsvpop/pop
gen sabb_uniq_pct = 100*sabb_uniq/pop
foreach v of varlist *_pct {
    fourSigRound `v'
    format `v' %3.1f
    replace `v' = round(`v',.1)
}
foreach v of varlist pop zsvpop {
    replace `v' = round(`v'/1000,1)
    format `v' %20.0gc
    insertCommas `v'
}
foreach v of varlist blk zsvblk {
    format `v' %20.0gc
    insertCommas `v'
}
sortBlockpop
drop s1
keep block_pop blk pop sabb_uniq_pct zsvblk zsvpop zsvblk_pct zsvpop_pct
order block_pop blk pop sabb_uniq_pct zsvblk zsvpop zsvblk_pct zsvpop_pct
exportTable solvar_blockpop 

*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*
* SOLVAR QTILE
import excel using $in/$countsFile, sheet(solvar_qtile) clear first
foreach v of varlist solvar_cutoff solvar_pct_cumul {
    fourSigRound `v'
    format `v' %3.1f
    replace `v' = round(`v',.1)
}
foreach v of varlist pop pop_cumul solvar_value_cumul {
    replace `v' = round(`v'/1000,1)
    format `v' %20.0gc
    insertCommas `v'
}
replace qtile = qtile + 5
keep qtile solvar_cutoff pop pop_cumul solvar_value_cumul solvar_pct_cumul
order qtile solvar_cutoff pop pop_cumul solvar_value_cumul solvar_pct_cumul
exportTable solvar_qtile

*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*
* AGREEMENT RATES BY BLOCK POP
import excel using $in/$countsFile, sheet(agree) clear first
labelAgreeRows
keep lbl cefpop exact binage
rename cefpop denom
gen block_pop = "Overall"
tempfile overall
save `overall'

getAgreeBlockPop "cefhdfbinage_exact cefhdfbinage_binage" "HDF-CEF"
save `part1', replace

getAgreeBlockPop "cefr00binage_exact cefr00binage_binage" "rHDF_{b,t}-CEF"
save `part2', replace

getAgreeBlockPop "cefr01binage_exact cefr01binage_binage" "rHDF_{b}-CEF"
save `part3', replace

getAgreeBlockPop "hdfr00binage_exact hdfr00binage_binage" "rHDF_{b,t}-HDF"
save `part4', replace

getAgreeBlockPop "hdfr01binage_exact hdfr01binage_binage" "rHDF_{b}-HDF"
save `part5', replace

use `overall', clear
append using `part1'
append using `part2'
append using `part3'
append using `part4'
append using `part5'

agreeBlockPopSortOrder
sort s1 s2 

gen exact_pct = 100*(exact/denom)
gen binage_pct = 100*(binage/denom)

foreach v of varlist *_pct {
    fourSigRound `v'
    format `v' %3.1f
    replace `v' = round(`v',.1)
}
foreach v of varlist exact binage {
    replace `v' = round(`v'/1000,1)
    fourSigRound `v'
    format `v' %20.0gc
    insertCommas `v'
}
foreach v of varlist denom {
    replace `v' = round(`v'/1000,1)
    format `v' %20.0gc
    insertCommas `v'
}
drop s1 s2 s3
replace block_pop = "1-9" if block_pop == "0-9"
order lbl block_pop denom exact binage exact_pct binage_pct
exportTable agree_blockpop

*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*
* AGREEMENT MDF RATES BY BLOCK POP
import excel using $in/$countsFile, sheet(agree) clear first
labelMDFAgreeRows
keep lbl cefpop exact binage
rename cefpop denom
gen block_pop = "Overall"
tempfile overall
save `overall'

getAgreeBlockPop "cefhdfbinage_exact cefhdfbinage_binage" "HDF-CEF"
save `part1', replace

getAgreeBlockPop "cefr00binage_exact cefr00binage_binage" "rHDF_{b,t}-CEF"
save `part2', replace

getAgreeBlockPop "cefr01binage_exact cefr01binage_binage" "rHDF_{b}-CEF"
save `part3', replace

getAgreeBlockPop "cefr02binage_exact cefr02binage_binage" "rMDF_{b,t}-CEF"
save `part4', replace

getAgreeBlockPop "cefr03binage_exact cefr03binage_binage" "MDF-CEF"
save `part5', replace

use `overall', clear
append using `part1'
append using `part2'
append using `part3'
append using `part4'
append using `part5'

agreeBlockPopSortOrder
sort s1 s3

gen exact_pct = 100*(exact/denom)
gen binage_pct = 100*(binage/denom)

foreach v of varlist *_pct {
    fourSigRound `v'
    format `v' %3.1f
    replace `v' = round(`v',.1)
}
foreach v of varlist exact binage {
    replace `v' = round(`v'/1000,1)
    fourSigRound `v'
    format `v' %20.0gc
    insertCommas `v'
}
foreach v of varlist denom {
    replace `v' = round(`v'/1000,1)
    format `v' %20.0gc
    insertCommas `v'
}
drop s1 s2 s3
replace block_pop = "1-9" if block_pop == "0-9"
order lbl block_pop denom exact binage exact_pct binage_pct
exportTable agree_blockpop_mdf

*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*
* REID BY BLOCK POP ALL

import excel using $in/$countsFile, sheet(putative) clear first
labelCmrlRows
rename binage put_cmrcl 
keep lbl put_cmrcl
save `part1', replace

import excel using $in/$countsFile, sheet(putative) clear first
labelCefRows
rename binage put_cef 
keep lbl put_cef
save `part2', replace

import excel using $in/$countsFile, sheet(confirm) clear first
labelCmrlRows
rename binage conf_cmrcl 
keep lbl conf_cmrcl
tempfile part3
save `part3', replace

import excel using $in/$countsFile, sheet(confirm) clear first
labelCefRows
rename binage conf_cef 
keep lbl conf_cef
tempfile part4
save `part4', replace

use `part1', clear
merge 1:1 lbl using `part2', nogen
merge 1:1 lbl using `part3', nogen
merge 1:1 lbl using `part4', nogen
gen block_pop = "Overall"
tempfile overall
save `overall'

getBlockPop "cef" "CEF" "" 
save `part1', replace

getBlockPop "hdf" "HDF" "" 
save `part2', replace

getBlockPop "r00" "rHDF_{b,t}" "" 
save `part3', replace

getBlockPop "r01" "rHDF_{b}" "" 
save `part4', replace

getBlockPop "mdg" "MDG" "" 
save `part5', replace

getBlockPop "prg" "PRG" "" 
save `part6', replace

getBlockPop "r02" "rMDF_{b,t}" "" 
save `part7', replace

getBlockPop "r03" "MDF" "" 
save `part8', replace

use `overall', clear
append using `part1'
append using `part2'
append using `part3'
append using `part4'
append using `part5'
append using `part6'
append using `part7'
append using `part8'

reidBlockPopSortOrder

tempfile putconf_blksz
save `putconf_blksz'

gen prcn_cmrcl = 100*(conf_cmrcl/put_cmrcl)
gen prcn_cef = 100*(conf_cef/put_cef)

foreach v of varlist prcn* {
    fourSigRound `v'
    format `v' %3.1f
    replace `v' = round(`v',.1)
}
foreach v of varlist put* conf* {
    replace `v' = round(`v'/1000,1)
    fourSigRound `v'
    format `v' %20.0gc
    insertCommas `v'
}
drop s1 s2
replace block_pop = "1-9" if block_pop == "0-9"
order lbl block_pop put_cmrcl conf_cmrcl prcn_cmrcl put_cef conf_cef prcn_cef
preserve
    drop if regex(lbl,"MDF")
    exportTable reid_blockpop
restore 
drop if regexm(lbl,"rHDF_{b}") | regexm(lbl,"MDG") | regexm(lbl,"PRG") 
exportTable reid_blockpop_mdf

import excel using $in/$countsFile, sheet(putative) clear first
local panelBDenom = keepblockpik[1]
import excel using $in/$countsFile, sheet(cmrcl_cef_match) clear first
local panelADenom = cmrcl_has_bpas[1]

use `overall', clear
keep lbl put_cmrcl conf_cmrcl block_pop
rename (put_cmrcl conf_cmrcl) (put conf)
replace block_pop = "a"
gen denom = `panelADenom'
save `part1', replace
use `overall', clear
keep lbl put_cef conf_cef block_pop
rename (put_cef conf_cef) (put conf)
replace block_pop = "b"
gen denom = `panelBDenom'
append using `part1'
reidBlockPopSortOrder
sort block_pop s2

gen put_pct = 100*(put/denom)
gen conf_pct = 100*(conf/denom)
gen prcn = 100*(conf/put)

foreach v of varlist prcn *_pct {
    fourSigRound `v'
    format `v' %3.1f
    replace `v' = round(`v',.1)
}
foreach v of varlist put conf denom {
    replace `v' = round(`v'/1000,1)
    fourSigRound `v'
    format `v' %20.0gc
    insertCommas `v'
}
order lbl denom put put_pct conf conf_pct prcn 
keep lbl denom put put_pct conf conf_pct prcn
tempfile reidblockpop
save `reidblockpop'
preserve
    drop if regex(lbl,"MDF")
    exportTable reid_blockpop_overall
restore 
drop if regexm(lbl,"rHDF_{b}") | regexm(lbl,"MDG") | regexm(lbl,"PRG") 
exportTable reid_blockpop_overall_mdf

*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*
* REID BY BLOCK POP MODAL
foreach type in m nm {
    import excel using $in/$countsFile, sheet(putative) clear first
    labelCmrlRows
    rename `type'_binage put_cmrcl 
    keep lbl put_cmrcl
    save `part1', replace

    import excel using $in/$countsFile, sheet(putative) clear first
    labelCefRows
    rename `type'_binage put_cef 
    keep lbl put_cef
    save `part2', replace

    import excel using $in/$countsFile, sheet(confirm) clear first
    labelCmrlRows
    rename `type'_binage conf_cmrcl 
    keep lbl conf_cmrcl
    save `part3', replace

    import excel using $in/$countsFile, sheet(confirm) clear first
    labelCefRows
    rename `type'_binage conf_cef 
    keep lbl conf_cef
    save `part4', replace

    use `part1', clear
    merge 1:1 lbl using `part2', nogen
    merge 1:1 lbl using `part3', nogen
    merge 1:1 lbl using `part4', nogen
    gen block_pop = "Overall"
    tempfile overall
    save `overall'
    
    getBlockPop "cef" "CEF" "`type'_" 
    save `part1', replace

    getBlockPop "hdf" "HDF" "`type'_" 
    save `part2', replace

    getBlockPop "r00" "rHDF_{b,t}" "`type'_" 
    save `part3', replace

    getBlockPop "r01" "rHDF_{b}" "`type'_" 
    save `part4', replace

    getBlockPop "mdg" "MDG" "`type'_" 
    save `part5', replace

    getBlockPop "prg" "PRG" "`type'_" 
    save `part6', replace

    getBlockPop "r02" "rMDF_{b,t}" "`type'_" 
    save `part7', replace

    getBlockPop "r03" "MDF" "`type'_" 
    save `part8', replace

    use `overall', clear
    append using `part1'
    append using `part2'
    append using `part3'
    append using `part4'
    append using `part5'
    append using `part6'
    append using `part7'
    append using `part8'

    reidBlockPopSortOrder

    gen prcn_cmrcl = 100*(conf_cmrcl/put_cmrcl)
    gen prcn_cef = 100*(conf_cef/put_cef)

    foreach v of varlist prcn* {
        fourSigRound `v'
        format `v' %3.1f
        replace `v' = round(`v',.1)
    }
    foreach v of varlist put* conf* {
        replace `v' = round(`v'/1000,1)
        fourSigRound `v'
        format `v' %20.0gc
        insertCommas `v'
    }
    drop s1 s2
    replace block_pop = "1-9" if block_pop == "0-9"
    order lbl block_pop put_cmrcl conf_cmrcl prcn_cmrcl put_cef conf_cef prcn_cef
    preserve
        drop if regex(lbl,"MDF")
        exportTable reid_blockpop_`type'
    restore 
    drop if regexm(lbl,"rHDF_{b}") | regexm(lbl,"MDG") | regexm(lbl,"PRG") 
    exportTable reid_blockpop_`type'_mdf
}

*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*
* REID CEF MATCHED COMRCL
import excel using $in/$countsFile, sheet(cmrcl_cef_match) clear first
local panelADenom = cmrcl_has_bpas[1]
local panelBDenom = cmrcl_cef_mtch_blkpikagebsex[1]

import excel using $in/$countsFile, sheet(putative) clear first
labelCmrlRows
rename (binage) (put)
keep lbl put
save `part1', replace

import excel using $in/$countsFile, sheet(confirm) clear first
labelCmrlRows
rename (binage) (conf)
keep lbl conf
merge 1:1 lbl using `part1', nogen
gen panel="a"
save `part2', replace

import excel using $in/$countsFile, sheet(putative) clear first
labelCmrlRows
rename (ccbpabs_binage) (put)
keep lbl put
save `part3', replace

import excel using $in/$countsFile, sheet(confirm) clear first
labelCmrlRows
rename (ccbpabs_binage) (conf)
keep lbl conf
merge 1:1 lbl using `part3', nogen
gen panel="b" 
save `part4', replace

use `part2', clear
append using `part4'
reidSortOrder
sort panel s2

gen denom = `panelADenom' if panel == "a"
replace denom = `panelBDenom' if panel == "b"

gen put_pct = 100*(put/denom)
gen conf_pct = 100*(conf/denom)
gen prcn = 100*(conf/put)

foreach v of varlist prcn *_pct {
    fourSigRound `v'
    format `v' %3.1f
    replace `v' = round(`v',.1)
}
foreach v of varlist put conf denom {
    replace `v' = round(`v'/1000,1)
    fourSigRound `v'
    format `v' %20.0gc
    insertCommas `v'
}
order lbl denom put put_pct conf conf_pct prcn 
keep lbl denom put put_pct conf conf_pct prcn
exportTable cmrcl_cefmatchedcmrcl

*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*
* SENSITIVE BLOCKS: NM ZERO SOLVAR, ZERO SOLVAR UNIQUE
import excel using $in/$countsFile, sheet(putative) clear first
local panelnmDenom = nonmodalre_svzeropop[1]
local panelnmsabbDenom = nmsabb_oneton_svzero[1]

foreach type in nm nmsabb {

    getSolvarZero "cef" "CEF" "`type'_"
    save `part1', replace

    getSolvarZero "hdf" "HDF" "`type'_"
    save `part2', replace

    getSolvarZero "r00" "rHDF_{b,t}" "`type'_"
    save `part3', replace

    getSolvarZero "r01" "rHDF_{b}" "`type'_"
    save `part4', replace

    getSolvarZero "mdg" "MDG" "`type'_"
    save `part5', replace

    getSolvarZero "prg" "PRG" "`type'_"
    save `part6', replace

    getSolvarZero "r02" "rMDF_{b,t}" "`type'_"
    save `part7', replace

    getSolvarZero "r03" "MDF" "`type'_"
    save `part8', replace

    use `part1', clear
    append using `part2'
    append using `part3'
    append using `part4'
    append using `part5'
    append using `part6'
    append using `part7'
    append using `part8'
    gen denom = `panel`type'Denom' 
    gen type = "`type'"
    tempfile panel`type'
    save `panel`type''
}

use `panelnm', clear
append using `panelnmsabb'
reidSortOrder

gen prcn_cmrcl = 100*(conf_cmrcl/put_cmrcl)
gen prcn_cef = 100*(conf_cef/put_cef)

foreach v of varlist prcn* {
    fourSigRound `v'
    format `v' %3.1f
    replace `v' = round(`v',.1)
}
foreach v of varlist denom put* conf* {
    replace `v' = round(`v'/1000,1)
    fourSigRound `v'
    format `v' %20.0gc
    insertCommas `v'
}
drop s2 type
order lbl denom put_cmrcl conf_cmrcl prcn_cmrcl put_cef conf_cef prcn_cef
exportTable nm_solvar

*--------------------------------------------------
* TEX END OF LINES
*--------------------------------------------------
local files : dir "$out/" files "*.tex"
foreach f in `files' {
    shell sed -i '' -e 's/$/ \\\\ /' $out/`f'
}


di c(current_date) " " c(current_time)
log close
