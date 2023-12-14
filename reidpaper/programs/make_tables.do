*--------------------------------------------------
* REID/RECON Tables and Figures
* 
* Example execution: 
* with interactive stata session 
* >include make_tables.do
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
    replace s1 = 0 if block_pop == "All"
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
    replace lbl = "\rHDFBT-CEF" if lbl == "r00-cef, binage"
    replace lbl = "\rHDFB-CEF" if lbl == "r01-cef, binage"
    replace lbl = "\rHDFBT-HDF" if lbl == "r00-hdf, binage"
    replace lbl = "\rHDFB-HDF" if lbl == "r01-hdf, binage"
    keep if regexm(lbl,"CEF") | regexm(lbl,"HDF")
end

capture labelMDFAgreeRows drop labelMDFAgreeRows 
program define labelMDFAgreeRows
    replace lbl = "HDF-CEF" if lbl == "hdf-cef, binage"
    replace lbl = "\rHDFBT-CEF" if lbl == "r00-cef, binage"
    replace lbl = "\rHDFB-CEF" if lbl == "r01-cef, binage"
    replace lbl = "\$\text{rMDF}_{b,t}\$-CEF" if lbl == "r02-cef, binage"
    replace lbl = "MDF-CEF" if lbl == "r03-cef, binage"
    replace lbl = "\$\text{rSWAPHi}_{b,t}\$-CEF" if lbl == "r04-cef, binage"
    replace lbl = "\$\text{rSWAPLo}_{b,t}\$-CEF" if lbl == "r05-cef, binage"
    keep if regexm(lbl,"CEF")
end

capture getAgreeBlockPop drop getAgreeBlockPop 
program define getAgreeBlockPop
    args vars lbl
    import excel using $in/$countsFile, sheet(agree_blksz) clear first
    rename (`vars' cefpop) (exact binage denom)
    keep block_pop denom exact binage
    gen lbl = "`lbl'"
    if regexm("`lbl'","rMDF") | regexm("`lbl'","rSWAP") {
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
    replace s2 = 1 if regexm(lbl,"rHDFBT") & regexm(lbl,"-CEF") 
    replace s2 = 2 if regexm(lbl,"rHDFB") & regexm(lbl,"-CEF") & !regexm(lbl,"rHDFBT")
    replace s2 = 3 if regexm(lbl,"rHDFBT")  & regexm(lbl,"-HDF") 
    replace s2 = 4 if regexm(lbl,"rHDFB") & regexm(lbl,"-HDF") & !regexm(lbl,"rHDFBT")
    gen s3 = .
    replace s3 = 0 if lbl == "HDF-CEF" 
    replace s3 = 1 if regexm(lbl,"rHDF")  & regexm(lbl,"-CEF") 
    replace s3 = 2 if regexm(lbl,"rMDF") & regexm(lbl,"-CEF") 
    replace s3 = 3 if regexm(lbl,"MDF") & regexm(lbl,"-CEF") & !regexm(lbl,"{b,t}")
    replace s3 = 4 if regexm(lbl,"rHDF")  & regexm(lbl,"-HDF") 
    replace s3 = 5 if regexm(lbl,"rMDF") & regexm(lbl,"-HDF") 
    replace s3 = 6 if regexm(lbl,"MDF") & regexm(lbl,"-HDF") & !regexm(lbl,"{b,t}")
    replace s3 = 7 if regexm(lbl,"SWAPLo") 
    replace s3 = 8 if regexm(lbl,"SWAPHi") 
end

capture labelCmrlRows drop labelCmrlRows 
program define labelCmrlRows
    replace lbl = "CEF" if lbl == "cef-cmrcl, binage"
    replace lbl = "HDF" if lbl == "hdf-cmrcl, binage"
    replace lbl = "\rHDFBT" if lbl == "r00-cmrcl, binage"
    replace lbl = "\rHDFB" if lbl == "r01-cmrcl, binage"
    replace lbl = "MDG" if lbl == "mdg-cmrcl, binage"
    replace lbl = "PRG" if lbl == "prg-cmrcl, binage"
    replace lbl = "\$\text{rMDF}_{b,t}\$" if lbl == "r02-cmrcl, binage"
    replace lbl = "MDF" if lbl == "r03-cmrcl, binage"
    replace lbl = "\$\text{rSWAPHi}_{b,t}\$" if lbl == "r04-cmrcl, binage"
    replace lbl = "\$\text{rSWAPLo}_{b,t}\$" if lbl == "r05-cmrcl, binage"
    keep if lbl == "CEF" | lbl == "HDF"  | lbl =="\rHDFBT" | lbl =="\rHDFB" | lbl == "MDG" | lbl == "PRG" | lbl == "MDF" | lbl == "\$\text{rMDF}_{b,t}\$"  | regexm(lbl,"SWAP") 
end

capture labelCefRows drop labelCefRows 
program define labelCefRows
    replace lbl = "CEF" if lbl == "cef-cef, binage"
    replace lbl = "HDF" if lbl == "hdf-cef, binage"
    replace lbl = "\rHDFBT" if lbl == "r00-cef, binage"
    replace lbl = "\rHDFB" if lbl == "r01-cef, binage"
    replace lbl = "MDG" if lbl == "mdg-cef, binage"
    replace lbl = "PRG" if lbl == "prg-cef, binage"
    replace lbl = "\$\text{rMDF}_{b,t}\$" if lbl == "r02-cef, binage"
    replace lbl = "MDF" if lbl == "r03-cef, binage"
    replace lbl = "\$\text{rSWAPHi}_{b,t}\$" if lbl == "r04-cef, binage"
    replace lbl = "\$\text{rSWAPLo}_{b,t}\$" if lbl == "r05-cef, binage"
    keep if lbl == "CEF" | lbl == "HDF" | lbl =="\rHDFBT" | lbl =="\rHDFB" | lbl == "MDG" | lbl == "PRG" | lbl == "MDF" | lbl == "\$\text{rMDF}_{b,t}\$"  | regexm(lbl,"SWAP") 
end

capture reidSortOrder drop reidSortOrder 
program define reidSortOrder
    gen s2 = .
    replace s2 = 0 if lbl == "CEF" 
    replace s2 = 2 if lbl == "HDF" 
    replace s2 = 3 if regexm(lbl,"rHDFBT") 
    replace s2 = 4 if regexm(lbl,"rHDFB") & !regexm(lbl,"rHDFBT")
    replace s2 = 5 if lbl == "MDG" 
    replace s2 = 6 if lbl == "PRG"  
    replace s2 = 7 if lbl == "MDF" 
    replace s2 = 8 if regexm(lbl,"{b,t}") & regexm(lbl,"rMDF")
    replace s2 = 9 if regexm(lbl,"SWAPLo") 
    replace s2 = 10 if regexm(lbl,"SWAPHi") 
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
    rename (`left'cmrclbinage_binage `left'cefbinage_binage keepblockpik) (put_cmrcl put_cef denom)
    keep block_pop denom put_cmrcl put_cef
    gen lbl = "`lbl'"
    tempfile part
    save `part', replace
    import excel using $in/$countsFile, sheet(confirm_blksz) clear first
    rename (`left'cmrclbinage_binage `left'cefbinage_binage keepblockpik) (conf_cmrcl conf_cef denom)
    keep block_pop denom conf_cmrcl conf_cef
    gen lbl = "`lbl'"
    merge 1:1 lbl block_pop using `part', nogen 
    if regexm("`lbl'","rMDF") | regexm("`lbl'","rSWAP") {
        replace lbl = "\$" + lbl 
        replace lbl = subinstr(lbl,"t}","t}\$",.)
    }
end

capture getBlockPopModal drop getBlockPopModal 
program define getBlockPopModal
    args left lbl t
    import excel using $in/$countsFile, sheet(putative_blksz) clear first
    rename (`left'cmrclbinage_`t'ccbpabs_binage `left'cefbinage_`t'_binage keepblockpik) (put_cmrcl put_cef denom)
    keep block_pop denom put_cmrcl put_cef
    gen lbl = "`lbl'"
    tempfile part
    save `part', replace
    import excel using $in/$countsFile, sheet(confirm_blksz) clear first
    rename (`left'cmrclbinage_`t'ccbpabs_binage `left'cefbinage_`t'_binage keepblockpik) (conf_cmrcl conf_cef denom)
    keep block_pop denom conf_cmrcl conf_cef
    gen lbl = "`lbl'"
    merge 1:1 lbl block_pop using `part', nogen 
    if regexm("`lbl'","rMDF") | regexm("`lbl'","rSWAP") {
        replace lbl = "\$" + lbl 
        replace lbl = subinstr(lbl,"t}","t}\$",.)
    }
end

capture getSolvarZeroModal drop getSolvarZeroModal
program define getSolvarZeroModal
    args left lbl t
    import excel using $in/$countsFile, sheet(putative_solvar) clear first
    keep if solvar == "0"
    if "`t'" == "nmsabb" {
        rename `left'cmrclbinage_nmcsabb_binage put_cmrcl
    }
    else {
        rename `left'cmrclbinage_`t'ccbpabs_binage put_cmrcl
    }
    rename `left'cefbinage_`t'_binage put_cef
    keep put_cmrcl put_cef
    gen lbl = "`lbl'"
    tempfile part
    save `part'
    import excel using $in/$countsFile, sheet(confirm_solvar) clear first
    keep if solvar == "0"
    rename `left'cmrclbinage_`t'_binage conf_cmrcl
    rename `left'cefbinage_`t'_binage conf_cef
    keep conf_cmrcl conf_cef
    gen lbl = "`lbl'"
    merge 1:1 lbl lbl using `part', nogen
    if regexm("`lbl'","rMDF") | regexm("`lbl'","rSWAP") {
        replace lbl = "\$" + lbl 
        replace lbl = subinstr(lbl,"t}","t}\$",.)
    }
end


*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*
* clean output
shell rm $out/?*

*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*
* initialize temp files
foreach p of numlist 0(1)10 {
    tempfile part`p'
}

*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*
* CMRCL CEF OVERLAP IN DATA DEFINED PERSONS

* get count of cmrcl records that condition on a match by geoid_block to CEF
import excel using $in/$countsFile, sheet(putative_blksz) clear first
collapse (sum) cmrcl_total
local cmrcl_total = cmrcl_total[1]

import excel using $in/$countsFile, sheet(cmrcl_cef_match) clear first
gen recnotincef = `cmrcl_total' -cmrcl_cef_mtch_blkpikagebsex
gen total  = cmrcl_has_bpas
rename cmrcl_cef_mtch_blkpikagebsex recincef
keep recincef recnotincef total
gen lbl = "Records in COMRCL"
save `part1', replace

import excel using $in/$countsFile, sheet(cmrcl_cef_match) clear first
gen recincef = cef_keep - cmrcl_cef_mtch_blkpikagebsex
gen lbl ="Records not in COMRCL"
keep lbl recincef
save `part2', replace

use `part1', clear
append using `part2'
order lbl recincef recnotincef
set obs 3
replace lbl = "Total" if _n==3
replace recincef = recincef[1] + recincef[2] if _n==3
gen recnotincefuniv = total - `cmrcl_total'
rename (recincef recnotincef recnotincefuniv) (incef_matched incefuniv_unmatched notincefuniv)
foreach v of varlist incef_matched incefuniv_unmatched notincefuniv total {
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
gen maxsolvar = solvar_cutoff * 2
gen maxsolvar_cumul = solvar_pct_cumul * 2
foreach v of varlist solvar_cutoff maxsolvar solvar_pct_cumul maxsolvar_cumul {
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
keep qtile solvar_cutoff maxsolvar pop pop_cumul solvar_pct_cumul maxsolvar_cumul
order qtile solvar_cutoff maxsolvar pop pop_cumul solvar_pct_cumul maxsolvar_cumul
exportTable solvar_qtile

*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*
* AGREEMENT RATES BY BLOCK POP
import excel using $in/$countsFile, sheet(agree) clear first
labelAgreeRows
keep lbl cefpop exact binage
rename cefpop denom
gen block_pop = "All"
tempfile overall
save `overall'

getAgreeBlockPop "cefhdfbinage_exact cefhdfbinage_binage" "HDF-CEF"
save `part1', replace

getAgreeBlockPop "cefr00binage_exact cefr00binage_binage" "\rHDFBT-CEF"
save `part2', replace

getAgreeBlockPop "cefr01binage_exact cefr01binage_binage" "\rHDFB-CEF"
save `part3', replace

getAgreeBlockPop "hdfr00binage_exact hdfr00binage_binage" "\rHDFBT-HDF"
save `part4', replace

getAgreeBlockPop "hdfr01binage_exact hdfr01binage_binage" "\rHDFB-HDF"
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
gen block_pop = "All"
tempfile overall
save `overall'

getAgreeBlockPop "cefhdfbinage_exact cefhdfbinage_binage" "HDF-CEF"
save `part1', replace

getAgreeBlockPop "cefr00binage_exact cefr00binage_binage" "\rHDFBT-CEF"
save `part2', replace

getAgreeBlockPop "cefr01binage_exact cefr01binage_binage" "\rHDFB-CEF"
save `part3', replace

getAgreeBlockPop "cefr02binage_exact cefr02binage_binage" "\text{rMDF}_{b,t}-CEF"
save `part4', replace

getAgreeBlockPop "cefr03binage_exact cefr03binage_binage" "MDF-CEF"
save `part5', replace

getAgreeBlockPop "cefr04binage_exact cefr04binage_binage" "\text{rSWAPHi}_{b,t}-CEF"
save `part6', replace

getAgreeBlockPop "cefr05binage_exact cefr05binage_binage" "\text{rSWAPLo}_{b,t}-CEF"
save `part7', replace

use `overall', clear
append using `part1'
append using `part2'
append using `part3'
append using `part4'
append using `part5'
append using `part6'
append using `part7'

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
replace lbl = "\rowcolor{Gray} " + lbl if regexm(lbl,"MDF")
replace lbl = "\rowcolor{Medgray} " + lbl if regexm(lbl,"SWAP")
exportTable agree_blockpop_mdf

*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*
* REID BY BLOCK POP ALL
import excel using $in/$countsFile, sheet(putative_blksz) clear first
collapse (sum) cmrcl_total keepblockpik
gen block_pop = "All"
save `part1', replace
import excel using $in/$countsFile, sheet(putative_blksz) clear first
keep block_pop cmrcl_total keepblockpik
append using `part1'
rename (cmrcl_total keepblockpik) (denom_cmrcl denom_cef)
tempfile denom
save `denom'

import excel using $in/$countsFile, sheet(putative) clear first
labelCmrlRows
rename (binage) (put_cmrcl) 
keep lbl put_cmrcl
save `part1', replace

import excel using $in/$countsFile, sheet(putative) clear first
labelCefRows
rename (binage) (put_cef) 
keep lbl put_cef
save `part2', replace

import excel using $in/$countsFile, sheet(confirm) clear first
labelCmrlRows
rename (binage) (conf_cmrcl) 
keep lbl conf_cmrcl
tempfile part3
save `part3', replace

import excel using $in/$countsFile, sheet(confirm) clear first
labelCefRows
rename (binage) (conf_cef) 
keep lbl conf_cef
tempfile part4
save `part4', replace

use `part1', clear
merge 1:1 lbl using `part2', nogen
merge 1:1 lbl using `part3', nogen
merge 1:1 lbl using `part4', nogen
gen block_pop = "All"
tempfile overall
save `overall'

getBlockPop "cef" "CEF" "" 
save `part1', replace

getBlockPop "hdf" "HDF" "" 
save `part2', replace

getBlockPop "r00" "\rHDFBT" "" 
save `part3', replace

getBlockPop "r01" "\rHDFB" "" 
save `part4', replace

getBlockPop "mdg" "MDG" "" 
save `part5', replace

getBlockPop "prg" "PRG" "" 
save `part6', replace

getBlockPop "r02" "\text{rMDF}_{b,t}" "" 
save `part7', replace

getBlockPop "r03" "MDF" "" 
save `part8', replace

getBlockPop "r04" "\text{rSWAPHi}_{b,t}" "" 
save `part9', replace

getBlockPop "r05" "\text{rSWAPLo}_{b,t}" "" 
save `part10', replace

use `overall', clear
append using `part1'
append using `part2'
append using `part3'
append using `part4'
append using `part5'
append using `part6'
append using `part7'
append using `part8'
append using `part9'
append using `part10'

drop denom
merge m:1 block_pop using `denom', nogen keepusing(denom_cef denom_cmrcl)

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
foreach v of varlist put* conf* denom_cmrcl denom_cef {
    replace `v' = round(`v'/1000,1)
    fourSigRound `v'
    format `v' %20.0gc
    insertCommas `v'
}
drop s1 s2

replace block_pop = "1-9" if block_pop == "0-9"
order lbl block_pop denom_cmrcl put_cmrcl conf_cmrcl prcn_cmrcl denom_cef put_cef conf_cef prcn_cef
tempfile figures1
save `figures1'
preserve
    drop if regex(lbl,"MDF") | regex(lbl,"SWAP")
    exportTable reid_blockpop
restore 
drop if (regexm(lbl,"\rHDFB") & !regexm(lbl,"\rHDFBT")) | regexm(lbl,"MDG") | regexm(lbl,"PRG") 
replace lbl = "\rowcolor{Gray} " + lbl if regexm(lbl,"MDF")
replace lbl = "\rowcolor{Medgray} " + lbl if regexm(lbl,"SWAP")
exportTable reid_blockpop_mdf

import excel using $in/$countsFile, sheet(putative) clear first
local panelBDenom = keepblockpik[1]
import excel using $in/$countsFile, sheet(cmrcl_cef_match) clear first
local panelADenom = `cmrcl_total'

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
    drop if regex(lbl,"MDF") | regex(lbl,"SWAP")
    exportTable reid_blockpop_overall
restore 
drop if (regexm(lbl,"\rHDFB") & !regexm(lbl,"\rHDFBT"))  | regexm(lbl,"MDG") | regexm(lbl,"PRG") 
replace lbl = "\rowcolor{Gray} " + lbl if regexm(lbl,"MDF")
replace lbl = "\rowcolor{Medgray} " + lbl if regexm(lbl,"SWAP")
exportTable reid_blockpop_overall_mdf

*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*
* REID BY BLOCK POP MODAL
import excel using $in/$countsFile, sheet(putative_blksz) clear first
collapse (sum) modalrepop nonmodalrepop mccbpas_pop nmccbpas_pop mccbpabs_pop nmccbpabs_pop
gen block_pop = "All"
save `part1', replace
import excel using $in/$countsFile, sheet(putative_blksz) clear first
keep block_pop modalrepop nonmodalrepop mccbpas_pop nmccbpas_pop mccbpabs_pop nmccbpabs_pop
append using `part1'
tempfile m_nm_denom
save `m_nm_denom'

foreach type in m nm {
    import excel using $in/$countsFile, sheet(putative) clear first
    labelCmrlRows
    rename (`type'ccbpabs_binage) (put_cmrcl) 
    keep lbl put_cmrcl
    save `part1', replace

    import excel using $in/$countsFile, sheet(putative) clear first
    labelCefRows
    rename (`type'_binage) (put_cef) 
    keep lbl put_cef
    save `part2', replace

    import excel using $in/$countsFile, sheet(confirm) clear first
    labelCmrlRows
    rename (`type'ccbpabs_binage) (conf_cmrcl) 
    keep lbl conf_cmrcl
    save `part3', replace

    import excel using $in/$countsFile, sheet(confirm) clear first
    labelCefRows
    rename (`type'_binage) (conf_cef) 
    keep lbl conf_cef
    save `part4', replace

    use `part1', clear
    merge 1:1 lbl using `part2', nogen
    merge 1:1 lbl using `part3', nogen
    merge 1:1 lbl using `part4', nogen
    gen block_pop = "All"
    tempfile overall
    save `overall'
    
    getBlockPopModal "cef" "CEF" "`type'" 
    save `part1', replace

    getBlockPopModal "hdf" "HDF" "`type'" 
    save `part2', replace

    getBlockPopModal "r00" "\rHDFBT" "`type'" 
    save `part3', replace

    getBlockPopModal "r01" "\rHDFB" "`type'" 
    save `part4', replace

    getBlockPopModal "mdg" "MDG" "`type'" 
    save `part5', replace

    getBlockPopModal "prg" "PRG" "`type'" 
    save `part6', replace

    getBlockPopModal "r02" "\text{rMDF}_{b,t}" "`type'" 
    save `part7', replace

    getBlockPopModal "r03" "MDF" "`type'" 
    save `part8', replace

    getBlockPopModal "r04" "\text{rSWAPHi}_{b,t}" "`type'" 
    save `part9', replace

    getBlockPopModal "r05" "\text{rSWAPLo}_{b,t}" "`type'" 
    save `part10', replace

    use `overall', clear
    append using `part1'
    append using `part2'
    append using `part3'
    append using `part4'
    append using `part5'
    append using `part6'
    append using `part7'
    append using `part8'
    append using `part9'
    append using `part10'

    drop denom
    if "`type'" == "m" {
        merge m:1 block_pop using `m_nm_denom', nogen keepusing(modalrepop mccbpabs_pop)
        rename (modalrepop mccbpabs_pop) (denom_cef denom_cmrcl)
    }
    if "`type'" == "nm" {
        merge m:1 block_pop using `m_nm_denom', nogen keepusing(nonmodalrepop nmccbpabs_pop)
        rename (nonmodalrepop nmccbpabs_pop) (denom_cef denom_cmrcl)
    }
    
    reidBlockPopSortOrder

    gen prcn_cmrcl = 100*(conf_cmrcl/put_cmrcl)
    gen prcn_cef = 100*(conf_cef/put_cef)

    foreach v of varlist prcn* {
        fourSigRound `v'
        format `v' %3.1f
        replace `v' = round(`v',.1)
    }
    foreach v of varlist put* conf* denom_cef denom_cmrcl {
        replace `v' = round(`v'/1000,1)
        fourSigRound `v'
        format `v' %20.0gc
        insertCommas `v'
    }
    drop s1 s2
    replace block_pop = "1-9" if block_pop == "0-9"
    order lbl block_pop denom_cmrcl put_cmrcl conf_cmrcl prcn_cmrcl denom_cef put_cef conf_cef prcn_cef
    tempfile figures2`type'
    save `figures2`type''
    preserve
        drop if regex(lbl,"MDF") | regex(lbl,"SWAP")
        exportTable reid_blockpop_`type'
    restore 
    drop if (regexm(lbl,"\rHDFB") & !regexm(lbl,"\rHDFBT"))  | regexm(lbl,"MDG") | regexm(lbl,"PRG") 
    replace lbl = "\rowcolor{Gray} " + lbl if regexm(lbl,"MDF")
    replace lbl = "\rowcolor{Medgray} " + lbl if regexm(lbl,"SWAP")
    exportTable reid_blockpop_`type'_mdf
}

*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*
* REID CEF MATCHED COMRCL
import excel using $in/$countsFile, sheet(cmrcl_cef_match) clear first
local panelADenom = `cmrcl_total'
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
local panelnmDenomCEF = nonmodalre_svzeropop[1]
local panelnmsabbDenomCEF = nmsabb_oneton_svzero[1]
local panelnmDenomCMRCL = nmccbpabs_svzeropop[1]
local panelnmsabbDenomCMRCL = nmcsabb_oneton_svzero[1]

foreach type in nm nmsabb {

    getSolvarZeroModal "cef" "CEF" "`type'"
    save `part1', replace

    getSolvarZeroModal "hdf" "HDF" "`type'"
    save `part2', replace

    getSolvarZeroModal "r00" "\rHDFBT" "`type'"
    save `part3', replace

    getSolvarZeroModal "r01" "\rHDFB" "`type'"
    save `part4', replace

    getSolvarZeroModal "mdg" "MDG" "`type'"
    save `part5', replace

    getSolvarZeroModal "prg" "PRG" "`type'"
    save `part6', replace

    getSolvarZeroModal "r02" "\text{rMDF}_{b,t}" "`type'"
    save `part7', replace

    getSolvarZeroModal "r03" "MDF" "`type'"
    save `part8', replace

    getSolvarZeroModal "r04" "\text{rSWAPHi}_{b,t}" "`type'"
    save `part9', replace

    getSolvarZeroModal "r05" "\text{rSWAPLo}_{b,t}" "`type'"
    save `part10', replace

    use `part1', clear
    append using `part2'
    append using `part3'
    append using `part4'
    append using `part5'
    append using `part6'
    append using `part7'
    append using `part8'
    append using `part9'
    append using `part10'
    gen denom_cef = `panel`type'DenomCEF' 
    gen denom_cmrcl = `panel`type'DenomCMRCL' 
    gen type = "`type'"
    tempfile panel`type'
    save `panel`type''
}

use `panelnm', clear
append using `panelnmsabb'
reidSortOrder
sort type s2
gen prcn_cmrcl = 100*(conf_cmrcl/put_cmrcl)
gen prcn_cef = 100*(conf_cef/put_cef)

foreach v of varlist prcn* {
    fourSigRound `v'
    format `v' %3.1f
    replace `v' = round(`v',.1)
}
foreach v of varlist denom_cef denom_cmrcl put* conf* {
    replace `v' = round(`v'/1000,1)
    fourSigRound `v'
    format `v' %20.0gc
    insertCommas `v'
}
drop s2 type
replace lbl = "\rowcolor{Gray} " + lbl if regexm(lbl,"MDF")
replace lbl = "\rowcolor{Medgray} " + lbl if regexm(lbl,"SWAP")
order lbl denom_cmrcl put_cmrcl conf_cmrcl prcn_cmrcl denom_cef put_cef conf_cef prcn_cef
exportTable nm_solvar


*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*~~~~*
* CEF ATKR RATES FIGURE
use `figures1', clear
foreach atkr in cef cmrcl {
    foreach v in denom_`atkr' put_`atkr' conf_`atkr' {
        replace `v' = subinstr(`v',",","",.)
        destring `v', replace
    }
    gen putr_`atkr' = 100*put_`atkr'/denom_`atkr'
    gen confr_`atkr' = 100*conf_`atkr'/denom_`atkr'
}
keep lbl block_pop putr_cef confr_cef prcn_cef putr_cmrcl confr_cmrcl prcn_cmrcl
drop if block_pop=="All"
sortBlockpop
gen n = s1 * 10
by s1: gen x = n+_n
by s1: replace x = x[_n-3] if _n==7
by s1: replace x = x[_n-1]+1 if _n>7
replace block_pop = "1,000+" if block_pop == "1000+"
labmask x, values(block_pop)
format putr_* confr_* prcn_* %4.0f

local c1 "227 26 28"
local c2 "251 154 153"
local c3 "51 160 44"
local c4 "178 223 138"
local c5 "31 120 180"
local c6 "166 206 227"
local c7 "106 61 154"
local c8 "202 178 214"
local c9 "255 127 0"
local c10 "253 191 111"
graph set window fontface "Times New Roman"

foreach atkr in cef cmrcl {
    if "`atkr'" == "cef" {
        local caplbl = "CEF{subscript:atkr}"
    }
    if "`atkr'" == "cmrcl" {
        local caplbl = "COMRCL"
    }
    foreach v in putr_`atkr' confr_`atkr' prcn_`atkr'  {
        if "`v'" == "putr_`atkr'" {
            local title = "`caplbl' : All Persons : Putative Rate"
            local ytitle=`" "Putative Rate (%)" "All Persons" "'
        }
        if "`v'" == "confr_`atkr'" {
            local title = "`caplbl' : All Persons : Confirmed Rate"
            local ytitle=`" "Confirmed Rate (%)" "All Persons" "'
        }
        if "`v'" == "prcn_`atkr'" {
            local title = "`caplbl' : All Persons : Precision Rate"
            local ytitle=`" "Precision Rate (%)" "All Persons" "'
        }
        local ytitle=" "

        if "`v'" == "prcn_`atkr'" {
            #delimit;
                tw (bar `v' x if lbl=="CEF", bcolor(black))
                (bar `v' x if lbl=="HDF", bcolor(gs2))
                (bar `v' x if lbl=="\rHDFBT", bcolor(gs4))
                (bar `v' x if lbl=="\rHDFB", bcolor(gs6))
                (bar `v' x if lbl=="MDG", bcolor(gs8))
                (bar `v' x if lbl=="PRG", bcolor(gs10))
                ,
                legend(order( 1 "CEF" 2 "HDF" 3 "rHDF{subscript:b,t} " 4 "rHDF{subscript:b} " 5 "MDG" 6 "PRG") pos(6) cols(6) size(vsmall) symy(2) symx(3.5))
                ylab(0(20)100, labsize(vsmall))
                xlab(13(10)70, nogrid angle(45) valuelabels labsize(small) notick labgap(3))
                bgcolor(white)
                graphregion(color(white) lwidth(large))
                title(`title', size(small))
                ytitle("")
                xtitle("")
                plotregion(margin(2 2 0 0))
                ysize(7)
                xsize(10)
                name(`v'allgs)
            ;
            #delimit cr
            graph export $out/`v'_blockpop_all_gs.png, replace width(2000)

            #delimit;
                tw (bar `v' x if lbl=="CEF", bcolor(black))
                (bar `v' x if lbl=="HDF", bcolor(gs2))
                (bar `v' x if lbl=="\rHDFBT", bcolor(gs4))
                (bar `v' x if lbl=="MDF", bcolor(gs6))
                (bar `v' x if lbl=="$\text{rMDF}_{b,t}$", bcolor(gs8))
                (bar `v' x if lbl=="$\text{rSWAPLo}_{b,t}$", bcolor(gs10))
                (bar `v' x if lbl=="$\text{rSWAPHi}_{b,t}$", bcolor(gs12))
                ,
                legend(order( 1 "CEF" 2 "HDF" 3 "rHDF{subscript:b,t} " 4 "MDF " 5 " rMDF{subscript:b,t} " 6 " rSWAPLo{subscript:b,t} " 7 " rSWAPHi{subscript:b,t}") pos(6) cols(7) size(vsmall) symy(2) symx(3.5))
                ylab(0(20)100, labsize(vsmall))
                xlab(14(10)70, nogrid angle(45) valuelabels labsize(small) notick labgap(3))
                bgcolor(white)
                graphregion(color(white) lwidth(large))
                title(`title', size(small))
                ytitle("")
                xtitle("")
                plotregion(margin(2 2 0 0))
                ysize(7)
                xsize(10)
                name(`v'allmdfgs)
            ;
            #delimit cr
            graph export $out/`v'_blockpop_all_mdf_gs.png, replace width(2000)


            #delimit;
                tw (bar `v' x if lbl=="CEF", bcolor("`c1'"))
                (bar `v' x if lbl=="HDF", bcolor("`c2'"))
                (bar `v' x if lbl=="\rHDFBT", bcolor("`c3'"))
                (bar `v' x if lbl=="\rHDFB", bcolor("`c4'"))
                (bar `v' x if lbl=="MDG", bcolor("`c5'"))
                (bar `v' x if lbl=="PRG", bcolor("`c6'"))
                ,
                legend(order( 1 "CEF" 2 "HDF" 3 "rHDF{subscript:b,t} " 4 "rHDF{subscript:b} " 5 "MDG" 6 "PRG") pos(6) cols(6) size(vsmall) symy(2) symx(3.5))
                ylab(0(20)100, labsize(vsmall))
                xlab(13(10)70, nogrid angle(45) valuelabels labsize(small) notick labgap(3))
                bgcolor(white)
                graphregion(color(white) lwidth(large))
                title(`title', size(small))
                ytitle("")
                xtitle("")
                plotregion(margin(2 2 0 0))
                ysize(7)
                xsize(10)
                name(`v'allcolor)
            ;
            #delimit cr
            graph export $out/`v'_blockpop_all_color.png, replace width(2000)

            #delimit;
                tw (bar `v' x if lbl=="CEF", bcolor("`c1'"))
                (bar `v' x if lbl=="HDF", bcolor("`c2'"))
                (bar `v' x if lbl=="\rHDFBT", bcolor("`c3'"))
                (bar `v' x if lbl=="MDF",  bcolor("`c7'"))
                (bar `v' x if lbl=="$\text{rMDF}_{b,t}$", bcolor("`c8'"))
                (bar `v' x if lbl=="$\text{rSWAPLo}_{b,t}$", bcolor("`c9'"))
                (bar `v' x if lbl=="$\text{rSWAPHi}_{b,t}$", bcolor("`c10'"))
                ,
                legend(order( 1 "CEF" 2 "HDF" 3 "rHDF{subscript:b,t} " 4 "MDF " 5 " rMDF{subscript:b,t} " 6 " rSWAPLo{subscript:b,t} " 7 " rSWAPHi{subscript:b,t}") pos(6) cols(7) size(vsmall) symy(2) symx(3.5))
                ylab(0(20)100, labsize(vsmall))
                xlab(14(10)70, nogrid angle(45) valuelabels labsize(small) notick labgap(3))
                bgcolor(white)
                graphregion(color(white) lwidth(large))
                title(`title', size(small))
                ytitle("")
                xtitle("")
                plotregion(margin(2 2 0 0))
                ysize(7)
                xsize(10)
                name(`v'allmdfcolor)
            ;
            #delimit cr
            graph export $out/`v'_blockpop_all_mdf_color.png, replace width(2000)
        
        }
        
        else { 
            #delimit;
                tw (bar `v' x if lbl=="CEF", bcolor(black))
                (bar `v' x if lbl=="HDF", bcolor(gs2))
                (bar `v' x if lbl=="\rHDFBT", bcolor(gs4))
                (bar `v' x if lbl=="\rHDFB", bcolor(gs6))
                (bar `v' x if lbl=="MDG", bcolor(gs8))
                (bar `v' x if lbl=="PRG", bcolor(gs10))
                ,
                legend(order( 1 "CEF" 2 "HDF" 3 "rHDF{subscript:b,t} " 4 "rHDF{subscript:b} " 5 "MDG" 6 "PRG") pos(6) cols(6) size(vsmall) symy(3) symx(5))
                ylab(0(20)100, labsize(vsmall))
                xlab(none)
                bgcolor(white)
                graphregion(color(white) lwidth(large))
                title(`title', size(small))
                ytitle("")
                xtitle("")
                plotregion(margin(2 2 0 0))
                ysize(7)
                xsize(10)
                name(`v'allgs)
            ;
            #delimit cr
            graph export $out/`v'_blockpop_all_gs.png, replace width(2000)
            

            #delimit;
                tw (bar `v' x if lbl=="CEF", bcolor(black))
                (bar `v' x if lbl=="HDF", bcolor(gs2))
                (bar `v' x if lbl=="\rHDFBT", bcolor(gs4))
                (bar `v' x if lbl=="MDF", bcolor(gs6))
                (bar `v' x if lbl=="$\text{rMDF}_{b,t}$", bcolor(gs8))
                (bar `v' x if lbl=="$\text{rSWAPLo}_{b,t}$", bcolor(gs10))
                (bar `v' x if lbl=="$\text{rSWAPHi}_{b,t}$", bcolor(gs12))
                ,
                legend(order( 1 "CEF" 2 "HDF" 3 "rHDF{subscript:b,t} " 4 "MDF " 5 " rMDF{subscript:b,t} " 6 " rSWAPLo{subscript:b,t} " 7 " rSWAPHi{subscript:b,t}") pos(6) cols(7) size(vsmall) symy(2) symx(3.5))
                ylab(0(20)100, labsize(vsmall))
                xlab(none)
                bgcolor(white)
                graphregion(color(white) lwidth(large))
                title(`title', size(small))
                ytitle("")
                xtitle("")
                plotregion(margin(2 2 0 0))
                ysize(7)
                xsize(10)
                name(`v'allmdfgs)
            ;
            #delimit cr
            graph export $out/`v'_blockpop_all_mdf_gs.png, replace width(2000)

            #delimit;
                tw (bar `v' x if lbl=="CEF", bcolor("`c1'"))
                (bar `v' x if lbl=="HDF", bcolor("`c2'"))
                (bar `v' x if lbl=="\rHDFBT", bcolor("`c3'"))
                (bar `v' x if lbl=="\rHDFB", bcolor("`c4'"))
                (bar `v' x if lbl=="MDG", bcolor("`c5'"))
                (bar `v' x if lbl=="PRG", bcolor("`c6'"))
                ,
                legend(order( 1 "CEF" 2 "HDF" 3 "rHDF{subscript:b,t} " 4 "rHDF{subscript:b} " 5 "MDG" 6 "PRG") pos(6) cols(6) size(vsmall) symy(3) symx(5))
                ylab(0(20)100, labsize(vsmall))
                xlab(none)
                bgcolor(white)
                graphregion(color(white) lwidth(large))
                title(`title', size(small))
                ytitle("")
                xtitle("")
                plotregion(margin(2 2 0 0))
                ysize(7)
                xsize(10)
                name(`v'allcolor)
            ;
            #delimit cr
            graph export $out/`v'_blockpop_all_color.png, replace width(2000)

            #delimit;
                tw (bar `v' x if lbl=="CEF", bcolor("`c1'"))
                (bar `v' x if lbl=="HDF", bcolor("`c2'"))
                (bar `v' x if lbl=="\rHDFBT", bcolor("`c3'"))
                (bar `v' x if lbl=="MDF",  bcolor("`c7'"))
                (bar `v' x if lbl=="$\text{rMDF}_{b,t}$", bcolor("`c8'"))
                (bar `v' x if lbl=="$\text{rSWAPLo}_{b,t}$", bcolor("`c9'"))
                (bar `v' x if lbl=="$\text{rSWAPHi}_{b,t}$", bcolor("`c10'"))
                ,
                legend(order( 1 "CEF" 2 "HDF" 3 "rHDF{subscript:b,t} " 4 "MDF " 5 " rMDF{subscript:b,t} " 6 " rSWAPLo{subscript:b,t} " 7 " rSWAPHi{subscript:b,t}") pos(6) cols(7) size(vsmall) symy(2) symx(3.5))
                ylab(0(20)100, labsize(vsmall))
                xlab(none)
                bgcolor(white)
                graphregion(color(white) lwidth(large))
                title(`title', size(small))
                ytitle("")
                xtitle("")
                plotregion(margin(2 2 0 0))
                ysize(7)
                xsize(10)
                name(`v'allmdfcolor)
            ;
            #delimit cr
            graph export $out/`v'_blockpop_all_mdf_color.png, replace width(2000)
        }
    }
}

foreach atkr in cef cmrcl {
    if "`atkr'" == "cef" {
        local caplbl = "CEF{subscript:atkr}"
    }
    if "`atkr'" == "cmrcl" {
        local caplbl = "COMRCL"
    }
    foreach type in m nm { 
        use `figures2`type'', clear
        foreach v in denom_`atkr' put_`atkr' conf_`atkr' {
            replace `v' = subinstr(`v',",","",.)
            destring `v', replace
        }
        gen putr_`atkr' = 100*put_`atkr'/denom_`atkr'
        gen confr_`atkr' = 100*conf_`atkr'/denom_`atkr'
        keep lbl block_pop putr_`atkr' confr_`atkr' prcn_`atkr'
        drop if block_pop=="All"
        sortBlockpop
        gen n = s1 * 10
        by s1: gen x = n+_n
        by s1: replace x = x[_n-3] if _n==7
        by s1: replace x = x[_n-1]+1 if _n>7
        replace block_pop = "1,000+" if block_pop == "1000+"
        labmask x, values(block_pop)
        format putr_`atkr' confr_`atkr' prcn_`atkr' %4.0f

        foreach v in putr_`atkr' confr_`atkr' prcn_`atkr' {
            if "`type'" == "nm" {
                local sample = "Nonmodal"
            }
            if "`type'" == "m" {
                local sample = "Modal"            
            }
    
            if "`v'" == "putr_`atkr'" {
                local title = "`caplbl' : `sample' Persons : Putative Rate"
                local ytitle=`" "Putative Rate (%)" "`sample' Persons" "'
            }
            if "`v'" == "confr_`atkr'" {
                local title = "`caplbl' : `sample' Persons : Confirmed Rate"
                local ytitle=`" "Confirmed Rate (%)" "`sample' Persons" "'
            }
            if "`v'" == "prcn_`atkr'" {
                local title = "`caplbl' : `sample' Persons : Precision Rate"
                local ytitle=`" "Precision Rate (%)" "`sample' Persons" "'
            }
            local ytitle=" "
        
            if "`v'" == "prcn_`atkr'" {
                #delimit;
                    tw (bar `v' x if lbl=="CEF", bcolor(black))
                    (bar `v' x if lbl=="HDF", bcolor(gs2))
                    (bar `v' x if lbl=="\rHDFBT", bcolor(gs4))
                    (bar `v' x if lbl=="\rHDFB", bcolor(gs6))
                    (bar `v' x if lbl=="MDG", bcolor(gs8))
                    (bar `v' x if lbl=="PRG", bcolor(gs10))
                    ,
                    legend(order( 1 "CEF" 2 "HDF" 3 "rHDF{subscript:b,t} " 4 "rHDF{subscript:b} " 5 "MDG" 6 "PRG") pos(6) cols(6))
                    ylab(0(20)100, labsize(vsmall))
                    xlab(13(10)70, nogrid angle(45) valuelabels labsize(small) notick labgap(3))
                    bgcolor(white)
                    graphregion(color(white) lwidth(large))
                    title(`title', size(small))
                    ytitle("")
                    xtitle("")
                    plotregion(margin(2 2 0 0))
                    ysize(7)
                    xsize(10)
                    name(`v'`type'gs)
                ;
                #delimit cr
                graph export $out/`v'_blockpop_`type'_gs.png, replace width(2000)

                #delimit;
                    tw (bar `v' x if lbl=="CEF", bcolor(black))
                    (bar `v' x if lbl=="HDF", bcolor(gs2))
                    (bar `v' x if lbl=="\rHDFBT", bcolor(gs4))
                    (bar `v' x if lbl=="MDF", bcolor(gs6))
                    (bar `v' x if lbl=="$\text{rMDF}_{b,t}$", bcolor(gs8))
                    (bar `v' x if lbl=="$\text{rSWAPLo}_{b,t}$", bcolor(gs10))
                    (bar `v' x if lbl=="$\text{rSWAPHi}_{b,t}$", bcolor(gs12))
                    ,
                    legend(order( 1 "CEF" 2 "HDF" 3 "rHDF{subscript:b,t} " 4 "MDF " 5 " rMDF{subscript:b,t} " 6 " rSWAPLo{subscript:b,t} " 7 " rSWAPHi{subscript:b,t}") pos(6) cols(7) size(vsmall) symy(2) symx(3.5))
                    ylab(0(20)100, labsize(vsmall))
                    xlab(14(10)70, nogrid angle(45) valuelabels labsize(small) notick labgap(3))
                    bgcolor(white)
                    graphregion(color(white) lwidth(large))
                    title(`title', size(small))
                    ytitle("")
                    xtitle("")
                    plotregion(margin(2 2 0 0))
                    ysize(7)
                    xsize(10)
                    name(`v'`type'mdfgs)
                ;
                #delimit cr
                graph export $out/`v'_blockpop_`type'_mdf_gs.png, replace width(2000)

                #delimit;
                    tw (bar `v' x if lbl=="CEF", bcolor("`c1'"))
                    (bar `v' x if lbl=="HDF", bcolor("`c2'"))
                    (bar `v' x if lbl=="\rHDFBT", bcolor("`c3'"))
                    (bar `v' x if lbl=="\rHDFB", bcolor("`c4'"))
                    (bar `v' x if lbl=="MDG", bcolor("`c5'"))
                    (bar `v' x if lbl=="PRG", bcolor("`c6'"))
                    ,
                    legend(order( 1 "CEF" 2 "HDF" 3 "rHDF{subscript:b,t} " 4 "rHDF{subscript:b} " 5 "MDG" 6 "PRG") pos(6) cols(6))
                    ylab(0(20)100, labsize(vsmall))
                    xlab(13(10)70, nogrid angle(45) valuelabels labsize(small) notick labgap(3))
                    bgcolor(white)
                    graphregion(color(white) lwidth(large))
                    title(`title', size(small))
                    ytitle("")
                    xtitle("")
                    plotregion(margin(2 2 0 0))
                    ysize(7)
                    xsize(10)
                    name(`v'`type'color)
                ;
                #delimit cr
                graph export $out/`v'_blockpop_`type'_color.png, replace width(2000)
                
                #delimit;
                    tw (bar `v' x if lbl=="CEF", bcolor("`c1'"))
                    (bar `v' x if lbl=="HDF", bcolor("`c2'"))
                    (bar `v' x if lbl=="\rHDFBT", bcolor("`c3'"))
                    (bar `v' x if lbl=="MDF",  bcolor("`c7'"))
                    (bar `v' x if lbl=="$\text{rMDF}_{b,t}$", bcolor("`c8'"))
                    (bar `v' x if lbl=="$\text{rSWAPLo}_{b,t}$", bcolor("`c9'"))
                    (bar `v' x if lbl=="$\text{rSWAPHi}_{b,t}$", bcolor("`c10'"))
                    ,
                    legend(order( 1 "CEF" 2 "HDF" 3 "rHDF{subscript:b,t} " 4 "MDF " 5 " rMDF{subscript:b,t} " 6 " rSWAPLo{subscript:b,t} " 7 " rSWAPHi{subscript:b,t}") pos(6) cols(7) size(vsmall) symy(2) symx(3.5))
                    ylab(0(20)100, labsize(vsmall))
                    xlab(14(10)70, nogrid angle(45) valuelabels labsize(small) notick labgap(3))
                    bgcolor(white)
                    graphregion(color(white) lwidth(large))
                    title(`title', size(small))
                    ytitle("")
                    xtitle("")
                    plotregion(margin(2 2 0 0))
                    ysize(7)
                    xsize(10)
                    name(`v'`type'mdfcolor)
                ;
                #delimit cr
                graph export $out/`v'_blockpop_`type'_mdf_color.png, replace width(2000)
            }
            else {
                #delimit;
                    tw (bar `v' x if lbl=="CEF", bcolor(black))
                    (bar `v' x if lbl=="HDF", bcolor(gs2))
                    (bar `v' x if lbl=="\rHDFBT", bcolor(gs4))
                    (bar `v' x if lbl=="\rHDFB", bcolor(gs6))
                    (bar `v' x if lbl=="MDG", bcolor(gs8))
                    (bar `v' x if lbl=="PRG", bcolor(gs10))
                    ,
                    legend(order( 1 "CEF" 2 "HDF" 3 "rHDF{subscript:b,t} " 4 "rHDF{subscript:b} " 5 "MDG" 6 "PRG") pos(6) cols(6))
                    ylab(0(20)100, labsize(vsmall))
                    xlab(none)
                    bgcolor(white)
                    graphregion(color(white) lwidth(large))
                    title(`title', size(small))
                    ytitle("")
                    xtitle("")
                    plotregion(margin(2 2 0 0))
                    ysize(7)
                    xsize(10)
                    name(`v'`type'gs)
                ;
                #delimit cr
                graph export $out/`v'_blockpop_`type'_gs.png, replace width(2000)

                #delimit;
                    tw (bar `v' x if lbl=="CEF", bcolor(black))
                    (bar `v' x if lbl=="HDF", bcolor(gs2))
                    (bar `v' x if lbl=="\rHDFBT", bcolor(gs4))
                    (bar `v' x if lbl=="MDF", bcolor(gs6))
                    (bar `v' x if lbl=="$\text{rMDF}_{b,t}$", bcolor(gs8))
                    (bar `v' x if lbl=="$\text{rSWAPLo}_{b,t}$", bcolor(gs10))
                    (bar `v' x if lbl=="$\text{rSWAPHi}_{b,t}$", bcolor(gs12))
                    ,
                    legend(order( 1 "CEF" 2 "HDF" 3 "rHDF{subscript:b,t} " 4 "MDF " 5 " rMDF{subscript:b,t} " 6 " rSWAPLo{subscript:b,t} " 7 " rSWAPHi{subscript:b,t}") pos(6) cols(7) size(vsmall) symy(2) symx(3.5))
                    ylab(0(20)100, labsize(vsmall))
                    xlab(none)
                    bgcolor(white)
                    graphregion(color(white) lwidth(large))
                    title(`title', size(small))
                    ytitle("")
                    xtitle("")
                    plotregion(margin(2 2 0 0))
                    ysize(7)
                    xsize(10)
                    name(`v'`type'mdfgs)
                ;
                #delimit cr
                graph export $out/`v'_blockpop_`type'_mdf_gs.png, replace width(2000)

                #delimit;
                    tw (bar `v' x if lbl=="CEF", bcolor("`c1'"))
                    (bar `v' x if lbl=="HDF", bcolor("`c2'"))
                    (bar `v' x if lbl=="\rHDFBT", bcolor("`c3'"))
                    (bar `v' x if lbl=="\rHDFB", bcolor("`c4'"))
                    (bar `v' x if lbl=="MDG", bcolor("`c5'"))
                    (bar `v' x if lbl=="PRG", bcolor("`c6'"))
                    ,
                    legend(order( 1 "CEF" 2 "HDF" 3 "rHDF{subscript:b,t} " 4 "rHDF{subscript:b} " 5 "MDG" 6 "PRG") pos(6) cols(6))
                    ylab(0(20)100, labsize(vsmall))
                    xlab(none)
                    bgcolor(white)
                    graphregion(color(white) lwidth(large))
                    title(`title', size(small))
                    ytitle("")
                    xtitle("")
                    plotregion(margin(2 2 0 0))
                    ysize(7)
                    xsize(10)
                    name(`v'`type'color)
                ;
                #delimit cr
                graph export $out/`v'_blockpop_`type'_color.png, replace width(2000)

                #delimit;
                    tw (bar `v' x if lbl=="CEF", bcolor("`c1'"))
                    (bar `v' x if lbl=="HDF", bcolor("`c2'"))
                    (bar `v' x if lbl=="\rHDFBT", bcolor("`c3'"))
                    (bar `v' x if lbl=="MDF",  bcolor("`c7'"))
                    (bar `v' x if lbl=="$\text{rMDF}_{b,t}$", bcolor("`c8'"))
                    (bar `v' x if lbl=="$\text{rSWAPLo}_{b,t}$", bcolor("`c9'"))
                    (bar `v' x if lbl=="$\text{rSWAPHi}_{b,t}$", bcolor("`c10'"))
                    ,
                    legend(order( 1 "CEF" 2 "HDF" 3 "rHDF{subscript:b,t} " 4 "MDF " 5 " rMDF{subscript:b,t} " 6 " rSWAPLo{subscript:b,t} " 7 " rSWAPHi{subscript:b,t}") pos(6) cols(7) size(vsmall) symy(2) symx(3.5))
                    ylab(0(20)100, labsize(vsmall))
                    xlab(none)
                    bgcolor(white)
                    graphregion(color(white) lwidth(large))
                    title(`title', size(small))
                    ytitle("")
                    xtitle("")
                    plotregion(margin(2 2 0 0))
                    ysize(7)
                    xsize(10)
                    name(`v'`type'mdfcolor)
                ;
                #delimit cr
                graph export $out/`v'_blockpop_`type'_mdf_color.png, replace width(2000)
            }
        }
    }
}

foreach atkr in cef cmrcl {
    foreach i in 1 2 {
        if `i' == 1 {
            local type=""
            local outFile = "reid_`atkr'_blockpop"
        }
        if `i' == 2 {
            local type="mdf"
            local outFile = "reid_`atkr'_blockpop_mdf"
        }
        grc1leg putr_`atkr'all`type'gs putr_`atkr'nm`type'gs putr_`atkr'm`type'gs confr_`atkr'all`type'gs confr_`atkr'nm`type'gs confr_`atkr'm`type'gs prcn_`atkr'all`type'gs prcn_`atkr'nm`type'gs prcn_`atkr'm`type'gs, legendfrom(putr_`atkr'all`type'gs) imargin(zero)
        graph export $out/`outFile'_gs.png, replace width(2000)

        grc1leg putr_`atkr'all`type'color putr_`atkr'nm`type'color putr_`atkr'm`type'color confr_`atkr'all`type'color confr_`atkr'nm`type'color confr_`atkr'm`type'color prcn_`atkr'all`type'color prcn_`atkr'nm`type'color prcn_`atkr'm`type'color, legendfrom(putr_`atkr'all`type'color) imargin(zero)
        graph export $out/`outFile'_color.png, replace width(2000)
    }
}



*--------------------------------------------------
* TEX END OF LINES
*--------------------------------------------------
local files : dir "$out/" files "*.tex"
foreach f in `files' {
    shell sed -i '' -e 's/$/ \\\\ /' $out/`f'
}


di c(current_date) " " c(current_time)
log close
