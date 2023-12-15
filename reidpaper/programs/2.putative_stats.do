*As a work of the United States government, this project is in the public
*domain within the United States. Additionally, we waive copyright and related
*rights in the work worldwide through the CC0 1.0 Universal public domain
*dedication (https://creativecommons.org/publicdomain/zero/1.0/)

di c(current_date) " " c(current_time)
di "#### START OF PUTATIVE CONFIRM STATS ####"

*--------------------------------------------------
* RECORD LINKAGE SUMMARY 
*--------------------------------------------------

capture putativeStats drop putativeStats
program define putativeStats
    * e.g. >putativeStats "r00" "cmrcl" 1
    args left right fzy
    * fzy = 1 then fuzzy age was used, if fzy=0, then binage was used
    
    * determine location of results
    if "`left'"=="$rhdf" | "`left'"=="$rhdfbo" | "`left'"=="$rmdf" | "`left'"=="$mdf" | "`left'"=="$swphi" | "`left'"=="$swplo" {
        local resultsLoc = "$rsltbase/rhdf/`left'/"
    }
    else {
        if regexm("`left'","Gsr") {
            local resultsLoc = "$rsltbase/simul/"            
        }
        else {
            local resultsLoc = "$rsltbase/`left'/"
        }
    }
    if `fzy' == 1 {
        local fileStem = "putative"
        local ageVar = "fzyage"
        local matchFileStem = "putativematch"
    }
    else {
        local fileStem = "putativebinage"
        local ageVar = "binage"
        local matchFileStem = "putativematchbinage"
    }
    if regexm("`left'","Gsr") {
        local leftShrt = "`left'"
        local leftShrt = subinstr("`leftShrt'","Gsr","g",.)
        local leftShrt = subinstr("`leftShrt'","modal","md",.)
        local leftShrt = subinstr("`leftShrt'","prop","pr",.)
    }
    else {
        local leftShrt = "`left'"
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
    
    import delimited using "`resultsLoc'/`matchFileStem'_`left'_`right'.csv", clear stringcols(1 2 3 13) $trainingWheels
    gen geoid_block = county + tract + block
    merge 1:1 geoid_block pik using $temp/cef_over21_modalre_oneton, keep(1 3) nogen 
    merge 1:1 geoid_block pik using $temp/cmrcl_cef_bpas_match, keep(1 3) nogen 
    
    foreach v in exact `ageVar' {
        * over/under 21 
        gen `leftShrt'`right'`ageVar'_o21_`v' = over21==1  & put_matchflag==``v'mflag'
        replace `leftShrt'`right'`ageVar'_o21_`v' = . if missing(over21)
        gen `leftShrt'`right'`ageVar'_u21_`v' = over21==0 & put_matchflag==``v'mflag'
        replace `leftShrt'`right'`ageVar'_u21_`v' = . if missing(over21)
        * modal/nonmodal 
        gen `leftShrt'`right'`ageVar'_m_`v' = modalre==1 & put_matchflag==``v'mflag' 
        replace `leftShrt'`right'`ageVar'_m_`v' = . if missing(modalre)
        gen `leftShrt'`right'`ageVar'_nm_`v' = modalre==0 & put_matchflag==``v'mflag' 
        replace `leftShrt'`right'`ageVar'_nm_`v' = . if missing(modalre)
        * onetons
        foreach ton in sab_oneton sabb_oneton ber_oneton {
            local tonabrv = subinstr("`ton'","_oneton","",.)
            gen `leftShrt'`right'`ageVar'_`tonabrv'_`v' = `ton'==1 & put_matchflag==``v'mflag'
            replace `leftShrt'`right'`ageVar'_`tonabrv'_`v' = . if missing(`ton')
        }
        * onetons by modal/nonmodal
        foreach ton in sab_oneton sabb_oneton {
            local tonabrv = subinstr("`ton'","_oneton","",.)
            gen `leftShrt'`right'`ageVar'_m`tonabrv'_`v' = `ton'==1 & modalre==1 & put_matchflag==``v'mflag' 
            replace `leftShrt'`right'`ageVar'_m`tonabrv'_`v' = . if missing(`ton') | missing(modalre)
            gen `leftShrt'`right'`ageVar'_nm`tonabrv'_`v' = `ton'==1 & modalre==0 & put_matchflag==``v'mflag' 
            replace `leftShrt'`right'`ageVar'_nm`tonabrv'_`v' = . if missing(`ton') | missing(modalre)
            
            gen `leftShrt'`right'`ageVar'_mc`tonabrv'_`v' = `ton'==1 & modalre==1 & bpabs==1 & put_matchflag==``v'mflag' 
            replace `leftShrt'`right'`ageVar'_mc`tonabrv'_`v' = . if missing(`ton') | missing(modalre) | missing(bpabs)
            gen `leftShrt'`right'`ageVar'_nmc`tonabrv'_`v' = `ton'==1 & modalre==0 & bpabs==1 & put_matchflag==``v'mflag' 
            replace `leftShrt'`right'`ageVar'_nmc`tonabrv'_`v' = . if missing(`ton') | missing(modalre) | missing(bpabs)
            
        }
        * bpas matches
        gen `leftShrt'`right'`ageVar'_ccbpas_`v' = bpas==1  & put_matchflag==``v'mflag'
        replace `leftShrt'`right'`ageVar'_ccbpas_`v' = . if missing(bpas)
        gen `leftShrt'`right'`ageVar'_ccbpabs_`v' = bpabs==1  & put_matchflag==``v'mflag'
        replace `leftShrt'`right'`ageVar'_ccbpabs_`v' = . if missing(bpabs)
        * bpas matches by modal/nonmodal
        gen `leftShrt'`right'`ageVar'_mccbpas_`v' = bpas==1 & modalre==1 & put_matchflag==``v'mflag'
        replace `leftShrt'`right'`ageVar'_mccbpas_`v' = . if missing(bpas) | missing(modalre)
        gen `leftShrt'`right'`ageVar'_mccbpabs_`v' = bpabs==1 & modalre==1 & put_matchflag==``v'mflag'
        replace `leftShrt'`right'`ageVar'_mccbpabs_`v' = . if missing(bpabs) | missing(modalre)
        gen `leftShrt'`right'`ageVar'_nmccbpas_`v' = bpas==1 & modalre==0 & put_matchflag==``v'mflag'
        replace `leftShrt'`right'`ageVar'_nmccbpas_`v' = . if missing(bpas) | missing(modalre)
        gen `leftShrt'`right'`ageVar'_nmccbpabs_`v' = bpabs==1 & modalre==0 & put_matchflag==``v'mflag'
        replace `leftShrt'`right'`ageVar'_nmccbpabs_`v' = . if missing(bpabs) | missing(modalre)
    }
    tempfile mdata 
    save `mdata'

    * count over/under 21
    use `mdata', clear
    collapse (sum) `leftShrt'`right'`ageVar'_o21* `leftShrt'`right'`ageVar'_u21*, by(geoid_block)
    tempfile overunder21
    save `overunder21'

    * count modal/nonmodal
    use `mdata', clear
    collapse (sum) `leftShrt'`right'`ageVar'_m_* `leftShrt'`right'`ageVar'_nm_*, by(geoid_block)
    tempfile modalnonmodal 
    save `modalnonmodal'

    * count onetons
    use `mdata', clear
    collapse (sum) `leftShrt'`right'`ageVar'_sab* `leftShrt'`right'`ageVar'_ber*, by(geoid_block)
    tempfile onetoncef 
    save `onetoncef'
    
    * count modal/nonmodal onetons
    use `mdata', clear
    collapse (sum) `leftShrt'`right'`ageVar'_msab* `leftShrt'`right'`ageVar'_nmsab* `leftShrt'`right'`ageVar'_mcsab* `leftShrt'`right'`ageVar'_nmcsab*, by(geoid_block)
    tempfile modalonetoncef 
    save `modalonetoncef'
    
    * count CEF-CMRCL BPAS matches
    use `mdata', clear
    collapse (sum) `leftShrt'`right'`ageVar'_ccbpas_* `leftShrt'`right'`ageVar'_ccbpabs_*, by(geoid_block)
    tempfile cefcmrclbpas 
    save `cefcmrclbpas'
    
    * count modal/nonmodal CEF-CMRCL BPAS matches
    use `mdata', clear
    collapse (sum) `leftShrt'`right'`ageVar'_mccbpas_* `leftShrt'`right'`ageVar'_mccbpabs_* `leftShrt'`right'`ageVar'_nmccbpas_* `leftShrt'`right'`ageVar'_nmccbpabs_*, by(geoid_block)
    tempfile modalcefcmrclbpas 
    save `modalcefcmrclbpas'
    
    * count matches
    import delimited using "`resultsLoc'/`fileStem'_`left'_`right'.csv", clear stringcols(1 2 3) $trainingWheels
    gen geoid_block = county + tract + block
    merge 1:1 geoid_block using $temp/cefblocksize, keep(3) nogen
    merge 1:1 geoid_block using `overunder21', keep(1 3) nogen
    merge 1:1 geoid_block using `modalnonmodal', keep(1 3) nogen
    merge 1:1 geoid_block using `onetoncef', keep(1 3) nogen
    merge 1:1 geoid_block using `modalonetoncef', keep(1 3) nogen
    merge 1:1 geoid_block using `cefcmrclbpas', keep(1 3) nogen
    merge 1:1 geoid_block using `modalcefcmrclbpas', keep(1 3) nogen
    * be sure all binage and fzyage are cumulative
    replace `ageVar' = exact + `ageVar'
    foreach i in o21 u21 m nm sab sabb ber msab msabb nmsab nmsabb mcsab mcsabb nmcsab nmcsabb ccbpas ccbpabs mccbpas mccbpabs nmccbpas nmccbpabs {
        replace `leftShrt'`right'`ageVar'_`i'_`ageVar' = `leftShrt'`right'`ageVar'_`i'_exact + `leftShrt'`right'`ageVar'_`i'_`ageVar'
    }
    * save block-level match counts
    outsheet using "`resultsLoc'/`fileStem'_`leftShrt'_`right'_augmented.csv", replace 
    preserve 
        * strip off leftrightagevar prefix for national totals
        foreach v of varlist `leftShrt'`right'`ageVar'_* {
            local newName = subinstr("`v'","`leftShrt'`right'`ageVar'_","",.)
            rename `v' `newName'
        }
        collapse (sum) exact `ageVar' u21* o21* m_* nm_* sab* ber* nmsab* msab* nmcsab* mcsab* ccbpas* ccbpabs* mccbpas* mccbpabs* nmccbpas* nmccbpabs*
        gen lbl = "`leftShrt'-`right', `ageVar'"
        append using $temp/putative_final
        save $temp/putative_final, replace
    restore
    preserve 
        collapse (sum) exact `ageVar' `leftShrt'`right'`ageVar'_*, by(block_pop)
        foreach v of varlist exact `ageVar' {
            rename `v' `leftShrt'`right'`ageVar'_`v'
        }
        order block_pop
        merge 1:1 block_pop using $temp/putativeblksz_final, nogen
        save $temp/putativeblksz_final, replace
    restore
    collapse (sum) exact `ageVar' `leftShrt'`right'`ageVar'_*, by(solvar)
    foreach v of varlist exact `ageVar' {
        rename `v' `leftShrt'`right'`ageVar'_`v'
    }
    order solvar
    merge 1:1 solvar using $temp/putativesolvar_final, nogen
    save $temp/putativesolvar_final, replace
    
    shell rm `mdata'
end


*--------------------------------------------------
* PUTATIVE RATES 
*--------------------------------------------------
* create blank container datasets
clear
set obs 1
gen lbl = ""
save $temp/putative_final, replace
clear
set obs 1
gen block_pop = .
save $temp/putativeblksz_final, replace
clear
set obs 1
gen solvar = ""
save $temp/putativesolvar_final, replace


if $putconf_rhdf_cmrcl_fz == 1 {
    putativeStats "$rhdf" "cmrcl" 1
}

if $putconf_rhdf_cef_fz == 1 {
    putativeStats "$rhdf" "cef" 1
}

if $putconf_rhdf_cmrcl_bn == 1 {
    putativeStats "$rhdf" "cmrcl" 0
}

if $putconf_rhdf_cef_bn == 1 {
    putativeStats "$rhdf" "cef" 0
}

if $putconf_cef_cef_bn == 1 {
    putativeStats "cef" "cef" 0
}

if $putconf_cef_cmrcl_bn == 1 {
    putativeStats "cef" "cmrcl" 0
}

if $putconf_hdf_cef_bn == 1 {
    putativeStats "hdf" "cef" 0
}

if $putconf_hdf_cmrcl_bn == 1 {
    putativeStats "hdf" "cmrcl" 0
}

if $putconf_rhdfbo_cef_bn == 1 {
    putativeStats "$rhdfbo" "cef" 0
}

if $putconf_rhdfbo_cmrcl_bn == 1 {
    putativeStats "$rhdfbo" "cmrcl" 0
}

if $putconf_rmdf_cef_bn == 1 {
    putativeStats "$rmdf" "cef" 0
}

if $putconf_rmdf_cmrcl_bn == 1 {
    putativeStats "$rmdf" "cmrcl" 0
}

if $putconf_mdf_cef_bn == 1 {
    putativeStats "$mdf" "cef" 0
}

if $putconf_mdf_cmrcl_bn == 1 {
    putativeStats "$mdf" "cmrcl" 0
}

if $putconf_modalGsr_cef_bn == 1 {
    putativeStats "modalGsr" "cef" 0
}

if $putconf_modalGsr_cmrcl_bn == 1 {
    putativeStats "modalGsr" "cmrcl" 0
}

if $putconf_propGsr_cef_bn == 1 {
    putativeStats "propGsr" "cef" 0
}

if $putconf_propGsr_cmrcl_bn == 1 {
    putativeStats "propGsr" "cmrcl" 0
}

if $putconf_swphi_cef_bn == 1 {
    putativeStats "$swphi" "cef" 0
}

if $putconf_swphi_cmrcl_bn == 1 {
    putativeStats "$swphi" "cmrcl" 0
}

if $putconf_swplo_cef_bn == 1 {
    putativeStats "$swplo" "cef" 0
}

if $putconf_swplo_cmrcl_bn == 1 {
    putativeStats "$swplo" "cmrcl" 0
}

use `cefpopnat', clear
local cefnatpoplcl = cefpop[1]
gen all=1
tempfile denominators
save `denominators'
use `rhdfpopnat', clear
local rhdfnatpoplcl = rhdfpop[1]


* SAVE NATIONAL PUTATIVE STATS
use $temp/putative_final, clear
gen all= 1
merge m:1 all using `denominators', keep(1 3) nogen
drop all
gen incef = `cefnatpoplcl'
gen inrecon = `rhdfnatpoplcl'
if $putconf_rhdf_cmrcl_fz == 1 | $putconf_rhdf_cef_fz==1 {
    order lbl incef inrecon exact fzyage binage
    format incef inrecon exact fzyage binage %20.0gc
}
else {
    order lbl incef inrecon exact binage
    format incef inrecon exact binage %20.0gc
}
drop if missing(lbl)
export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(putative) firstrow(variables) sheetmodify

* SAVE BLOCK SIZE PUTATIVE STATS
use $temp/putativeblksz_final, clear
merge 1:1 block_pop using `cefpopblocksz', nogen 
order block_pop cefpop
merge 1:1 block_pop using `cmrcl_blockpop_denom', nogen
if $putconf_rhdf_cmrcl_fz == 1 | $putconf_rhdf_cef_fz==1 {
    format *_exact *_fzyage *_binage  %20.0gc
}
else {
    format *_exact *_binage %20.0gc
}
drop if missing(block_pop)
export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(putative_blksz) firstrow(variables) sheetmodify

* SAVE SOLVAR PUTATIVE STATS
use $temp/putativesolvar_final, clear
merge 1:1 solvar using `solvarpop', nogen 
order solvar cefpop keepblockpik
if $putconf_rhdf_cmrcl_fz == 1 | $putconf_rhdf_cef_fz==1 {
    format *_exact *_fzyage *_binage  %20.0gc
}
else {
    format *_exact *_binage %20.0gc
}
drop if missing(solvar)
export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(putative_solvar) firstrow(variables) sheetmodify

*--------------------------------------------------
* UNIQUE BLOCK-PIK STATS
*--------------------------------------------------
import delimited using $rsltbase/cef/cef_block_counts.csv, clear stringcols(1 2)
drop if substr(geoid_block,1,2)=="72"
collapse (sum) haspik pop keepblockpik, by(geoid_block)
gen blockswithdups = keepblockpik<haspik
gen allblocks = 1
collapse(sum) haspik pop keepblockpik blockswithdups allblocks
gen pctblockswithdups = 100*blockswithdups/allblocks
gen pctpikpopdup = 100*(1- keepblockpik/haspik)
format pct* %20.4f
format haspik pop keepblockpik blockswithdups allblocks %20.0gc
export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(pikdups) firstrow(variables) sheetmodify



di "#### END OF PUTATIVE CONFIRM STATS ####"
di c(current_date) " " c(current_time)
