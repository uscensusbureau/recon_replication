
di c(current_date) " " c(current_time)
di "#### START OF AGREEMENT STATS ####"

*--------------------------------------------------
* AGREEMENT RATES 
*--------------------------------------------------

capture agreeStats drop agreeStats
program define agreeStats
    * e.g. >agreeStats "hdf" "r00" 1
    args left right fzy
    * fzy = 1 then fuzzy age was used, if fzy=0, then binage was used
    
    * determine location of results
    if "`right'"=="$rhdf" | "`right'"=="$rhdfbo" | "`right'"=="$rmdf" | "`right'"=="$mdf" | "`right'"=="$swphi" | "`right'"=="$swplo" {
        local resultsLoc = "$rsltbase/rhdf/`right'/"
    }
    else {
        local resultsLoc = "$rsltbase/`right'/"
    }
    if `fzy' == 1 {
        local fileStem = "agree"
        local ageVar = "fzyage"
        local matchFileStem = "agreematch"
    }
    else {
        local fileStem = "agreebinage"
        local ageVar = "binage"
        local matchFileStem = "agreematchbinage"
    }
    
    local exactmflag = 1
    local fzyagemflag = 2
    local binagemflag = 3
    local oneoffmflag = 4
    
    di "left: `left'"
    di "right: `right'"
    di "rhdf: $rhdf"
    di "rhdfbo: $rhdfbo"
    di "resultsLoc: `resultsLoc'"
    di "fileStem: `fileStem'"
    di "ageVar: `ageVar'"
    
    * count matches
    import delimited using "`resultsLoc'/`fileStem'_`left'_`right'.csv", clear stringcols(1 2 3) $trainingWheels
    gen geoid_block = county + tract + block
    merge 1:1 geoid_block using $temp/cefblocksize, keep(3) nogen
    replace oneoff = exact + `ageVar' + oneoff
    replace `ageVar' = exact + `ageVar'
    preserve
        collapse (sum) exact `ageVar' oneoff 
        gen lbl = "`right'-`left', `ageVar'"
        append using $temp/agree_final
        save $temp/agree_final, replace
    restore
    preserve
        collapse (sum) exact `ageVar' oneoff, by(block_pop)
        foreach v of varlist exact `ageVar' oneoff {
            rename `v' `left'`right'`ageVar'_`v'
        }
        order block_pop `left'`right'`ageVar'_exact `left'`right'`ageVar'_`ageVar' `left'`right'`ageVar'_oneoff
        save $temp/agreeblksz, replace
        merge 1:1 block_pop using $temp/agreeblksz_final, nogen
        save $temp/agreeblksz_final, replace
    restore
    collapse (sum) exact `ageVar' oneoff, by(solvar)
    foreach v of varlist exact `ageVar' oneoff {
        rename `v' `left'`right'`ageVar'_`v'
    }
    order solvar `left'`right'`ageVar'_exact `left'`right'`ageVar'_`ageVar' `left'`right'`ageVar'_oneoff
    merge 1:1 solvar using $temp/agreesolvar_final, nogen
    save $temp/agreesolvar_final, replace
    
end

*--------------------------------------------------
* AGREEMENT RATES 
*--------------------------------------------------
* create blank container datasets
clear
set obs 1
gen lbl = ""
save $temp/agree_final, replace
clear
set obs 1
gen block_pop = .
save $temp/agreeblksz_final, replace
clear
set obs 1
gen solvar = ""
save $temp/agreesolvar_final, replace

if $agree_rhdf_hdf_fz == 1 {
    agreeStats "hdf" "$rhdf" 1
}

if $agree_rhdf_cef_fz == 1 {
    agreeStats "cef" "$rhdf" 1
}

if $agree_rhdf_hdf_bn == 1 {
    agreeStats "hdf" "$rhdf" 0
}

if $agree_rhdf_cef_bn == 1 {
    agreeStats "cef" "$rhdf" 0
}

if $agree_rhdfbo_cef_bn == 1 {
    agreeStats "cef" "$rhdfbo" 0
}

if $agree_rhdfbo_hdf_bn == 1 {
    agreeStats "hdf" "$rhdfbo" 0
}

if $agree_rmdf_cef_bn == 1 {
    agreeStats "cef" "$rmdf" 0
}

if $agree_rmdf_hdf_bn == 1 {
    agreeStats "hdf" "$rmdf" 0
}

if $agree_mdf_cef_bn == 1 {
    agreeStats "cef" "$mdf" 0
}

if $agree_mdf_hdf_bn == 1 {
    agreeStats "hdf" "$mdf" 0
}

if $agree_hdf_cef_bn == 1 {
    agreeStats "cef" "hdf" 0
}

if $agree_swphi_cef_bn == 1 {
    agreeStats "cef" "$swphi" 0
}

if $agree_swphi_hdf_bn == 1 {
    agreeStats "hdf" "$swphi" 0
}

if $agree_swplo_cef_bn == 1 {
    agreeStats "cef" "$swplo" 0
}

if $agree_swplo_hdf_bn == 1 {
    agreeStats "hdf" "$swplo" 0
}

use `cefpopnat', clear
local cefnatpop = cefpop[1]
gen all=1
tempfile denominators
save `denominators'
use `rhdfpopnat', clear
local rhdfnatpop = rhdfpop[1]

* SAVE NATIONAL AGREE STATS
use $temp/agree_final, clear
gen all= 1
merge m:1 all using `denominators', keep(1 3) nogen
drop all
gen incef = `cefnatpop'
gen inrecon = `rhdfnatpop'
* in case you only did fuzzy or only did binage
if $agree_rhdf_hdf_fz == 1 | $agree_rhdf_cef_fz==1 {
    order lbl incef inrecon exact fzyage binage oneoff
    format incef inrecon exact fzyage binage oneoff %20.0gc
}
else {
    order lbl incef inrecon exact binage oneoff
    format incef inrecon exact binage oneoff %20.0gc
}
drop if missing(lbl)
export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(agree) firstrow(variables) replace

* SAVE BLOCK SIZE AGREE STATS
use $temp/agreeblksz_final, clear
merge 1:1 block_pop using `cefpopblocksz', nogen 
order block_pop cefpop
if $agree_rhdf_hdf_fz == 1 | $agree_rhdf_hdf_fz==1 {
    format *_exact *_fzyage *_binage *_oneoff %20.0gc
}
else {
    format *_exact *_binage *_oneoff %20.0gc
}
drop if missing(block_pop)
export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(agree_blksz) firstrow(variables) sheetmodify

* SAVE SOLVAR AGREE STATS
use $temp/agreesolvar_final, clear
merge 1:1 solvar using `solvarpop', nogen 
order solvar cefpop 
if $agree_rhdf_hdf_fz == 1 | $agree_rhdf_hdf_fz==1 {
    format *_exact *_fzyage *_binage *_oneoff %20.0gc
}
else {
    format *_exact *_binage *_oneoff %20.0gc
}
drop if missing(solvar)
export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(agree_solvar) firstrow(variables) sheetmodify

*--------------------------------------------------
* BLOCK-PIK CEF-CMRCL MATCH STATS
*--------------------------------------------------
import delimited using $rsltbase/cmrcl/cmrcl_cef_bpas_match_counts.csv, clear 
collapse (sum) cef_all cef_keep cmrcl_all cmrcl_has_bp cmrcl_has_bpas bp bpas bpabs
rename bp cmrcl_cef_mtch_blkpik
rename bpas cmrcl_cef_mtch_blkpikagesex
rename bpabs cmrcl_cef_mtch_blkpikagebsex
gen mtch_blkpik_of_keep = 100*(cmrcl_cef_mtch_blkpik/cef_keep)
gen mtch_blkpikagesex_of_keep = 100*(cmrcl_cef_mtch_blkpikagesex/cef_keep)
gen mtch_blkpikagebsex_of_keep = 100*(cmrcl_cef_mtch_blkpikagebsex/cef_keep)
order cef_all cef_keep cmrcl_all cmrcl_has_bp cmrcl_has_bpas cmrcl_cef_mtch_blkpik mtch_blkpik_of_keep cmrcl_cef_mtch_blkpikagesex mtch_blkpikagesex_of_keep cmrcl_cef_mtch_blkpikagebsex mtch_blkpikagebsex_of_keep
format cef_all cef_keep cmrcl_all cmrcl_has_bp cmrcl_has_bpas cmrcl_cef_mtch_blkpik cmrcl_cef_mtch_blkpikagesex cmrcl_cef_mtch_blkpikagebsex  %20.0gc 
format *_of_keep %20.4f
export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(cmrcl_cef_match) firstrow(variables) 


di "#### END OF AGREEMENT STATS ####"
di c(current_date) " " c(current_time)
