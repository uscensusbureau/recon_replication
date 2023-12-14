
di c(current_date) " " c(current_time)
di "#### START OF CONFIRM STATS ####"

capture confirmStats drop confirmStats
program define confirmStats
    * e.g. >confirmStats "r00" "cmrcl" 1
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
        local fileStem = "confirm"
        local ageVar = "fzyage"
        local matchFileStem = "confirmmatch"
    }
    else {
        local fileStem = "confirmbinage"
        local ageVar = "binage"
        local matchFileStem = "confirmmatchbinage"
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
    
    import delimited using "`resultsLoc'/`matchFileStem'_`left'`right'_cef.csv", clear stringcols(1 2 3 13) $trainingWheels
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
        }
        * bpas matches
        gen `leftShrt'`right'`ageVar'_ccbpas_`v' = bpas==1  & put_matchflag==``v'mflag'
        replace `leftShrt'`right'`ageVar'_ccbpas_`v' = . if missing(bpas)
        gen `leftShrt'`right'`ageVar'_ccbpabs_`v' = bpabs==1  & put_matchflag==``v'mflag'
        replace `leftShrt'`right'`ageVar'_ccbpabs_`v' = . if missing(bpabs)

        * bpas matches by modal/nonmodal
        gen `leftShrt'`right'`ageVar'_mccbpas_`v' = bpas==1 & modalre==1 & put_matchflag==``v'mflag'
        replace `leftShrt'`right'`ageVar'_mccbpas_`v' = . if missing(bpas) | missing(modalre)
        gen `leftShrt'`right'`ageVar'_mccbpabs_`v' = bpabs==1 & modalre==1  & put_matchflag==``v'mflag'
        replace `leftShrt'`right'`ageVar'_mccbpabs_`v' = . if missing(bpabs) | missing(modalre)

        gen `leftShrt'`right'`ageVar'_nmccbpas_`v' = bpas==1 & modalre==0 & put_matchflag==``v'mflag'
        replace `leftShrt'`right'`ageVar'_nmccbpas_`v' = . if missing(bpas) | missing(modalre)
        gen `leftShrt'`right'`ageVar'_nmccbpabs_`v' = bpabs==1 & modalre==0 & put_matchflag==``v'mflag'
        replace `leftShrt'`right'`ageVar'_nmccbpabs_`v' = . if missing(bpabs) | missing(modalre)
        
        gen `v' = put_matchflag==``v'mflag'
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
    collapse (sum) `leftShrt'`right'`ageVar'_msab* `leftShrt'`right'`ageVar'_nmsab*, by(geoid_block)
    tempfile modalonetoncef 
    save `modalonetoncef'
    
    * count exact and fzy using putative match flag, not confirm match flag
    use `mdata', clear
    collapse (sum) exact `ageVar', by(geoid_block)
    tempfile allmatch
    save `allmatch'
    
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
    import delimited using "`resultsLoc'/`fileStem'_`left'`right'_cef.csv", clear stringcols(1 2 3) $trainingWheels
    gen geoid_block = county + tract + block
    * drop the exact and fzy or binage count variables based upon the conf_matchflag, which tell you the type of match in the conf match. 
    * we want to count by putative match type, stored in allmatch
    if `fzy' == 1 {
        drop exact fzyage
    }
    else {
        drop exact fzyage binage
    }
    merge 1:1 geoid_block using $temp/cefblocksize, keep(3) nogen
    merge 1:1 geoid_block using `overunder21', keep(1 3) nogen
    merge 1:1 geoid_block using `modalnonmodal', keep(1 3) nogen
    merge 1:1 geoid_block using `onetoncef', keep(1 3) nogen
    merge 1:1 geoid_block using `modalonetoncef', keep(1 3) nogen
    merge 1:1 geoid_block using `allmatch', keep(1 3) nogen
    merge 1:1 geoid_block using `cefcmrclbpas', keep(1 3) nogen
    merge 1:1 geoid_block using `modalcefcmrclbpas', keep(1 3) nogen
    * be sure all binage and fzyage are cumulative
    replace `ageVar' = exact + `ageVar'
    foreach i in o21 u21 m nm sab sabb ber msab msabb nmsab nmsabb ccbpas ccbpabs mccbpas mccbpabs nmccbpas nmccbpabs {
        replace `leftShrt'`right'`ageVar'_`i'_`ageVar' = `leftShrt'`right'`ageVar'_`i'_exact + `leftShrt'`right'`ageVar'_`i'_`ageVar'
    }
    * save block-level match counts
    outsheet using "`resultsLoc'/`fileStem'_`leftShrt'`right'_cef_augmented.csv", replace 
    preserve 
        * strip off leftrightagevar prefix for national totals
        foreach v of varlist `leftShrt'`right'`ageVar'_* {
            local newName = subinstr("`v'","`leftShrt'`right'`ageVar'_","",.)
            rename `v' `newName'
        }
        collapse (sum) exact `ageVar' u21* o21* m_* nm_* sab* ber* nmsab* msab* ccbpas* ccbpabs* mccbpas* mccbpabs* nmccbpas* nmccbpabs*
        gen lbl = "`leftShrt'-`right', `ageVar'"
        append using $temp/confirm_final
        save $temp/confirm_final, replace
    restore
    preserve
        collapse (sum) exact `ageVar' `leftShrt'`right'`ageVar'_*, by(block_pop)
        foreach v of varlist exact `ageVar' {
            rename `v' `leftShrt'`right'`ageVar'_`v'
        }
        order block_pop
        merge 1:1 block_pop using $temp/confirmblksz_final, nogen
        save $temp/confirmblksz_final, replace
    restore
    collapse (sum) exact `ageVar' `leftShrt'`right'`ageVar'_*, by(solvar)
    foreach v of varlist exact `ageVar' {
        rename `v' `leftShrt'`right'`ageVar'_`v'
    }
    order solvar
    merge 1:1 solvar using $temp/confirmsolvar_final, nogen
    save $temp/confirmsolvar_final, replace
    
    shell rm `mdata'
end

*--------------------------------------------------
* CONFIRM COUNTS
*--------------------------------------------------
clear
set obs 1
gen lbl = ""
save $temp/confirm_final, replace
clear
set obs 1
gen block_pop = .
save $temp/confirmblksz_final, replace
clear
set obs 1
gen solvar = ""
save $temp/confirmsolvar_final, replace


if $putconf_rhdf_cmrcl_fz == 1 {
    confirmStats "$rhdf" "cmrcl" 1
}

if $putconf_rhdf_cef_fz == 1 {
    confirmStats "$rhdf" "cef" 1
}

if $putconf_rhdf_cmrcl_bn == 1 {
    confirmStats "$rhdf" "cmrcl" 0
}

if $putconf_rhdf_cef_bn == 1 {
    confirmStats "$rhdf" "cef" 0
}

if $putconf_cef_cef_bn == 1 {
    confirmStats "cef" "cef" 0
}

if $putconf_cef_cmrcl_bn == 1 {
    confirmStats "cef" "cmrcl" 0
} 

if $putconf_hdf_cef_bn == 1 {
    confirmStats "hdf" "cef" 0
}

if $putconf_hdf_cmrcl_bn == 1 {
    confirmStats "hdf" "cmrcl" 0
}

if $putconf_rhdfbo_cef_bn == 1 {
    confirmStats "$rhdfbo" "cef" 0
}

if $putconf_rhdfbo_cmrcl_bn == 1 {
    confirmStats "$rhdfbo" "cmrcl" 0
}

if $putconf_rmdf_cef_bn == 1 {
    confirmStats "$rmdf" "cef" 0
}

if $putconf_rmdf_cmrcl_bn == 1 {
    confirmStats "$rmdf" "cmrcl" 0
}

if $putconf_mdf_cef_bn == 1 {
    confirmStats "$mdf" "cef" 0
}

if $putconf_mdf_cmrcl_bn == 1 {
    confirmStats "$mdf" "cmrcl" 0
}

if $putconf_modalGsr_cef_bn == 1 {
    confirmStats "modalGsr" "cef" 0
}

if $putconf_modalGsr_cmrcl_bn == 1 {
    confirmStats "modalGsr" "cmrcl" 0
}

if $putconf_propGsr_cef_bn == 1 {
    confirmStats "propGsr" "cef" 0
}

if $putconf_propGsr_cmrcl_bn == 1 {
    confirmStats "propGsr" "cmrcl" 0
}

if $putconf_swphi_cef_bn == 1 {
    confirmStats "$swphi" "cef" 0
}

if $putconf_swphi_cmrcl_bn == 1 {
    confirmStats "$swphi" "cmrcl" 0
}

if $putconf_swplo_cef_bn == 1 {
    confirmStats "$swplo" "cef" 0
}

if $putconf_swplo_cmrcl_bn == 1 {
    confirmStats "$swplo" "cmrcl" 0
}

use `cefpopnat', clear
local cefnatpop = cefpop[1]
gen all=1
tempfile denominators
save `denominators'
use `rhdfpopnat', clear
local rhdfnatpop = rhdfpop[1]

* SAVE NATIONAL CONFIRM STATS
use $temp/confirm_final, clear
gen all= 1
merge m:1 all using `denominators', keep(1 3) nogen
drop all
gen incef = `cefnatpop'
gen inrecon = `rhdfnatpop'
if $putconf_rhdf_cmrcl_fz == 1 | $putconf_rhdf_cef_fz==1 {
    order lbl incef inrecon exact fzyage binage
    format incef inrecon exact fzyage binage %20.0gc
}
else {
    order lbl incef inrecon exact binage
    format incef inrecon exact binage %20.0gc
}
drop if missing(lbl)
export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(confirm) firstrow(variables) sheetmodify

* SAVE BLOCK SIZE CONFIRM STATS
use $temp/confirmblksz_final, clear
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
export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(confirm_blksz) firstrow(variables) sheetmodify

* SAVE SOLVAR CONFIRM STATS
use $temp/confirmsolvar_final, clear
merge 1:1 solvar using `solvarpop', nogen 
order solvar cefpop keepblockpik
if $putconf_rhdf_cmrcl_fz == 1 | $putconf_rhdf_cef_fz==1 {
    format *_exact *_fzyage *_binage  %20.0gc
}
else {
    format *_exact *_binage %20.0gc
}
drop if missing(solvar)
export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(confirm_solvar) firstrow(variables) sheetmodify


di "#### END OF CONFIRM STATS ####"
di c(current_date) " " c(current_time)
