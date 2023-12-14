*--------------------------------------------------
* REID/RECON Paper Statistics
* 7/2021, version 1
* Nathan Goldschlag
*
* Args: 
*   
* 
* Example execution: 
*>setsid stata-se -b runall.do
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
log using runall_`dt'.log, replace
include 0.config.do


*--------------------------------------------------
* LOG DATA USED
*--------------------------------------------------
*include 0.data_inventory.do

*--------------------------------------------------
* CLEAN OUTPUT DIR
*--------------------------------------------------
shell rm $disclosure/CBDRB-FY22-DSEP-004.xlsx
shell rm $disclosure/CBDRB-FY22-DSEP-004_tables.xlsx

*--------------------------------------------------
* PREP CATEGORIES
*--------------------------------------------------
* save flag output of reidmodule/06/ as dta so we can merge directly off disc
import delimited using $rsltbase/cef/cef_over21_modalre_oneton.csv, clear stringcols(1 2)
save $temp/cef_over21_modalre_oneton, replace

import delimited using $rsltbase/cmrcl/cmrcl_cef_bpas_match.csv, clear stringcols(1 2)
save $temp/cmrcl_cef_bpas_match, replace


*--------------------------------------------------
* CEF-BASED BLOCK SIZE BINS
*--------------------------------------------------
import delimited using $solvar/scaled_ivs.csv, clear stringcols(_all)
rename blockid geoid_block 
* the 202302 scaled_ivs.csv file was missing leading zeros
gen l = strlen(geoid_block)
tab l
replace geoid_block = "0" + geoid_block if l==14
destring iv, replace
destring siv, replace
rename siv s_iv
keep geoid_block iv s_iv
tempfile solvar
save `solvar'

import delimited using $rsltbase/cef/cef_block_counts.csv, clear stringcols(1)
drop if substr(geoid_block,1,2)=="72"
cutPop pop
rename popg block_pop
merge 1:1 geoid_block using `solvar', nogen keep(1 3) keepusing(s_iv)
gen solvar = "0" if s_iv==0
replace solvar = ">0" if s_iv>0 & !missing(s_iv)
replace solvar = "NA" if missing(s_iv)
keep geoid_block pop keepblockpik block_pop solvar
rename pop cefpop
tempfile cefblocksize
save `cefblocksize'
save $temp/cefblocksize, replace

*--------------------------------------------------
* CMRCL DENOM BY BLOCK SIZE BINS
*--------------------------------------------------
import delimited using $rsltbase/cmrcl/cmrcl_block_counts.csv, clear stringcols(1)
keep geoid_block hasall3
rename hasall3 cmrcl_total
merge 1:1 geoid_block using $temp/cefblocksize, keep(3) nogen
collapse (sum) cmrcl_total, by(block_pop)
tempfile cmrcl_blockpop_denom
save `cmrcl_blockpop_denom'

*--------------------------------------------------
* COMMONLY USED AGGREGATIONS
*--------------------------------------------------
use $temp/cef_over21_modalre_oneton, clear
merge 1:1 geoid_block pik using $temp/cmrcl_cef_bpas_match, keep(1 3) nogen 
merge m:1 geoid_block using `cefblocksize', keep(1 3) nogen keepusing(solvar)
gen nonmodalrepop = modalre==0
gen u21pop = over21==0
gen nmsab_oneton = sab_oneton==1 & modalre == 0 
gen msab_oneton = sab_oneton==1 & modalre == 1
gen nmsabb_oneton = sabb_oneton==1 & modalre == 0 
gen msabb_oneton = sabb_oneton==1 & modalre == 1

gen mccbpas = bpas==1 & modalre==1
replace mccbpas = . if missing(bpas) | missing(modalre)
gen nmccbpas = bpas==1 & modalre==0
replace nmccbpas = . if missing(bpas) | missing(modalre)
gen mccbpabs = bpabs==1 & modalre==1
replace mccbpabs = . if missing(bpabs) | missing(modalre)
gen nmccbpabs = bpabs==1 & modalre==0
replace nmccbpabs = . if missing(bpabs) | missing(modalre)

foreach s in "0" ">0" "NA" {
    if "`s'" == "0" {
        local sfx = "zero"
    }
    if "`s'" == ">0" {
        local sfx = "gtzero"
    }
    if "`s'" == "NA" {
        local sfx = "NA"
    }
    gen modalre_sv`sfx'pop = modalre==1 & solvar=="`s'"
    gen nonmodalre_sv`sfx'pop = modalre==0 & solvar=="`s'"

    gen mccbpas_sv`sfx'pop = mccbpas==1 & solvar=="`s'"
    gen nmccbpas_sv`sfx'pop = nmccbpas==1 & solvar=="`s'"
    gen mccbpabs_sv`sfx'pop = mccbpabs==1 & solvar=="`s'"
    gen nmccbpabs_sv`sfx'pop = nmccbpabs==1 & solvar=="`s'"

    gen o21_sv`sfx'pop = over21==1 & solvar=="`s'"
    gen u21_sv`sfx'pop = over21==0 & solvar=="`s'"
    gen nmsab_oneton_sv`sfx' = sab_oneton==1 & modalre == 0  & solvar=="`s'"
    gen msab_oneton_sv`sfx' = sab_oneton==1 & modalre == 1 & solvar=="`s'"
    gen nmsabb_oneton_sv`sfx' = sabb_oneton==1 & modalre == 0  & solvar=="`s'"
    gen msabb_oneton_sv`sfx' = sabb_oneton==1 & modalre == 1 & solvar=="`s'"

    gen nmcsabb_oneton_sv`sfx' = sabb_oneton==1 & nmccbpabs == 1  & solvar=="`s'"
    gen mcsabb_oneton_sv`sfx' = sabb_oneton==1 & mccbpabs == 1 & solvar=="`s'"

}
rename modalre modalrepop
rename over21 o21pop
collapse (sum) o21pop u21pop modalrepop nonmodalrepop sab_oneton sabb_oneton ber_oneton nmsab_oneton nmsabb_oneton msab_oneton msabb_oneton *_sv* mccbpas nmccbpas mccbpabs nmccbpabs, by(geoid_block)
tempfile over21modalonetonpop
save `over21modalonetonpop'
    
use $temp/cmrcl_cef_bpas_match, clear
merge m:1 geoid_block using `cefblocksize', keep(1 3) nogen keepusing(solvar)
gen ccbpaspop = bpas==1
gen ccbpabspop = bpabs==1
foreach s in "0" ">0" "NA" {
    if "`s'" == "0" {
        local sfx = "zero"
    }
    if "`s'" == ">0" {
        local sfx = "gtzero"
    }
    if "`s'" == "NA" {
        local sfx = "NA"
    }
    gen ccbpas_sv`sfx'pop = bpas==1 & solvar=="`s'"
    gen ccbpabs_sv`sfx'pop = bpabs==1 & solvar=="`s'"
}
collapse (sum) ccbpaspop ccbpabspop *_sv*, by(geoid_block)
tempfile ccbpaspop
save `ccbpaspop'

import delimited using $rsltbase/cef/cef_block_counts.csv, clear stringcols(1)
drop if substr(geoid_block,1,2)=="72"
merge 1:1 geoid_block using `cefblocksize', keep(1 3) nogen
merge 1:1 geoid_block using `over21modalonetonpop', keep(1 3) nogen
merge 1:1 geoid_block using `ccbpaspop', keep(1 3) nogen
preserve 
    collapse (sum) cefpop = pop keepblockpik o21pop u21pop modalrepop nonmodalrepop sab_onetonpop=sab_oneton sabb_onetonpop=sabb_oneton  ber_onetonpop=ber_oneton nmsab_oneton_pop=nmsab_oneton nmsabb_oneton_pop=nmsabb_oneton msab_oneton_pop=msab_oneton msabb_oneton_pop=msabb_oneton ccbpaspop ccbpabspop *_sv* mccbpas_pop=mccbpas nmccbpas_pop=nmccbpas mccbpabs_pop=mccbpabs nmccbpabs_pop=nmccbpabs
    tempfile cefpopnat
    save `cefpopnat'
    save $temp/cefnatpop, replace
restore 
collapse (sum) cefpop = pop keepblockpik o21pop u21pop modalrepop nonmodalrepop sab_onetonpop=sab_oneton sabb_onetonpop=sabb_oneton  ber_onetonpop=ber_oneton nmsab_oneton_pop=nmsab_oneton nmsabb_oneton_pop=nmsabb_oneton msab_oneton_pop=msab_oneton msabb_oneton_pop=msabb_oneton ccbpaspop ccbpabspop mccbpas_pop=mccbpas nmccbpas_pop=nmccbpas mccbpabs_pop=mccbpabs nmccbpabs_pop=nmccbpabs, by(block_pop)
tempfile cefpopblocksz
save `cefpopblocksz'
save $temp/cefblocksize_pop, replace

import delimited using $rsltbase/hdf/hdf_block_counts.csv, clear stringcols(1)
drop if substr(geoid_block,1,2)=="72"
collapse (sum) hdfpop = pop
tempfile hdfpopnat
save `hdfpopnat'

import delimited using $rsltbase/hdf/hdf_block_counts.csv, clear stringcols(1)
merge 1:1 geoid_block using `cefblocksize', keep(3)
collapse (sum) hdfpop = pop, by(block_pop)
tempfile hdfpopblocksz
save `hdfpopblocksz'

import delimited using $rsltbase/rhdf/`rhdf'/`rhdf'_block_counts.csv, clear stringcols(1)
drop if substr(geoid_block,1,2)=="72"
collapse (sum) rhdfpop = pop
tempfile rhdfpopnat
save `rhdfpopnat'

use $temp/cefblocksize, clear
collapse (sum) cefpop keepblockpik, by(solvar)
tempfile solvarpop
save `solvarpop'

*--------------------------------------------------
* ALL ANALYSES
*--------------------------------------------------

include 1.agree_stats.do

include 2.putative_stats.do

include 3.confirm_stats.do

include 4.solvar.do


di c(current_date) " " c(current_time)
log close
