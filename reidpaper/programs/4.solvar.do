*As a work of the United States government, this project is in the public
*domain within the United States. Additionally, we waive copyright and related
*rights in the work worldwide through the CC0 1.0 Universal public domain
*dedication (https://creativecommons.org/publicdomain/zero/1.0/)

di c(current_date) " " c(current_time)
di "#### START OF SOLVAR STATS ####"

*--------------------------------------------------
* GET NON MODAL SABB ONETON DATA
*--------------------------------------------------

import delimited using $rhdfbasersltdir/r00/putativematchbinage_r00_cef.csv, clear stringcols(1 2 3 13) $trainingWheels
gen geoid_block = county + tract + block
merge 1:1 geoid_block pik using $temp/cef_over21_modalre_oneton, keep(1 3) nogen
gen put_nmsabb = 1 if sabb_oneton==1 & modalre == 0
collapse (sum) put_nmsabb, by(geoid_block)
tempfile nmsabbput
save `nmsabbput'

import delimited using $rhdfbasersltdir/r00/confirmmatchbinage_r00cef_cef.csv, clear stringcols(1 2 3 13) $trainingWheels
gen geoid_block = county + tract + block
merge 1:1 geoid_block pik using $temp/cef_over21_modalre_oneton, keep(1 3) nogen 
gen conf_nmsabb = 1 if sabb_oneton==1 & modalre == 0
collapse (sum) conf_nmsabb, by(geoid_block)
tempfile nmsabbconf
save `nmsabbconf'


*--------------------------------------------------
* SOLVAR STATS
*--------------------------------------------------

import delimited using $solvar/scaled_ivs.csv, clear stringcols(_all) $trainingWheels
rename blockid geoid_block 
* the 202302 scaled_ivs.csv file was missing leading zeros
gen l = strlen(geoid_block)
tab l
replace geoid_block = "0" + geoid_block if l==14
destring iv, replace
destring siv, replace
rename siv s_iv
keep geoid_block iv s_iv
tempfile iv
save `iv'

import delimited using $rsltbase/cef/cef_block_counts.csv, clear stringcols(_all) $trainingWheels
destring keepblockpik, replace
destring pop, replace
collapse (sum) cefpop=pop keepblockpik, by(geoid_block)
tempfile kbp
save `kbp' 

import delimited using $rhdfbasersltdir/r00/agreebinage_hdf_r00.csv, clear stringcols(_all) $trainingWheels
gen geoid_block = county+tract+block
destring exact, replace
destring binage, replace
gen agree = exact + binage
keep geoid_block agree
tempfile a
save `a'

import delimited using $rhdfbasersltdir/r00/confirmbinage_r00cef_cef.csv, clear stringcols(_all) $trainingWheels
gen geoid_block = county+tract+block
destring exact, replace
destring binage, replace
gen conf = exact + binage
keep geoid_block conf
merge 1:1 geoid_block using `nmsabbconf', nogen
tempfile c
save `c'

import delimited using $rhdfbasersltdir/r00/putativebinage_r00_cef.csv, clear stringcols(_all) $trainingWheels
gen geoid_block = county+tract+block
destring exact, replace
destring binage, replace
gen put = exact + binage
keep geoid_block put
merge 1:1 geoid_block using `nmsabbput', nogen
tempfile p
save `p'

use `a', clear
merge 1:1 geoid_block using `c', nogen keep(1 3)
merge 1:1 geoid_block using `p', nogen keep(1 3)
merge 1:1 geoid_block using `kbp',  nogen keep(1 3)
merge 1:1 geoid_block using `iv', nogen keep(1 3)
merge 1:1 geoid_block using $temp/cefblocksize, nogen keep(1 3)
merge 1:1 geoid_block using `over21modalonetonpop', nogen keep(1 3) keepusing(geoid_block nmsabb_oneton)
rename nmsabb_oneton nmsabb_oneton_pop
foreach v in put put_nmsabb conf conf_nmsabb nmsabb_oneton_pop keepblockpik agree {
    replace `v' = 0 if missing(`v')
}
gen pr = 100*put/keepblockpik
gen cr = 100*conf/keepblockpik
gen prcn = 100*conf/put
gen agr = 100*agree/cefpop

sum pr agr cr prcn 
sum s_iv, d
* percentile bins for standardized solvar
gen s_ivPct = .
replace s_ivPct = 0 if s_iv==0
sum s_iv if s_iv>0, d
replace s_ivPct = 1 if s_iv<=`r(p25)' & s_iv>0
replace s_ivPct = 2 if s_iv<=`r(p50)' & s_iv>`r(p25)'
replace s_ivPct = 3 if s_iv<=`r(p75)' & s_iv>`r(p50)'
replace s_ivPct = 4 if s_iv<=`r(p90)' & s_iv>`r(p75)'
replace s_ivPct = 5 if s_iv>`r(p90)' & !missing(s_iv)
label define s_ivlbl 0 "0" 1 "<25th" 2 ">25th <=50th" 3 ">50th <=75th" 4 ">75th <=90th" 5 ">90th"
label values s_ivPct s_ivlbl
gen s_ivPos = s_iv>0 if !missing(s_iv)
* hand currated bins for std solvar
gen s_ivG = .
replace s_ivG = 0 if s_iv==0
replace s_ivG = 1 if s_iv>0 & s_iv<=.05
replace s_ivG = 2 if s_iv>.05 & s_iv<=.1
replace s_ivG = 3 if s_iv>.1 & s_iv<=.15
replace s_ivG = 4 if s_iv>.15 & s_iv<=.25
replace s_ivG = 5 if s_iv>.25 & !missing(s_iv)
label define s_ivGlbl 0 "0" 1 "(0,.05]" 2 "(.05,.1]" 3 "(.1,.15]" 4 "(.15,.25]" 5 ">.25"
label values s_ivG s_ivGlbl
gen block_pop2 = block_pop
replace block_pop2 = 500 if block_pop2 == 1000
label define blkpop 0 "0-9" 10 "10-49" 50 "50-99" 100 "100-249" 250 "250-499" 500 "500-999" 1000 "1000+"
label values block_pop blkpop
label define blkpop2 0 "0-9" 10 "10-49" 50 "50-99" 100 "100-249" 250 "250-499" 500 "500+"
label values block_pop2 blkpop2
bys block_pop2: egen ms_iv = mean(s_iv)
gen s_iv_blksz_dm = s_iv-ms_iv if !missing(s_iv)
label variable agr "Agrmnt Rate"
label variable pr "Putv Rate"
label variable cr "Conf Rate"
label variable prcn "Prcsn Rate"
label variable s_iv "SolVar Norm"
label variable iv "SolVar"
label variable s_ivPct "SolVar Pctls"
label variable s_ivG "SolVar Bins"
label variable block_pop "Block Pop"
label variable block_pop2 "Block Pop"
save $temp/solvarAnalysisData, replace
tempfile solvarAnalysisData
save `solvarAnalysisData'

use `solvarAnalysisData', clear
local s = "s_ivG"
collapse (sum) put conf agree cefpop keepblockpik, by(block_pop `s')
gen pr = 100*put/keepblockpik
gen cr = 100*conf/keepblockpik
gen prcn = 100*conf/put
gen agr = 100*agree/cefpop
keep block_pop `s' pr cr prcn agr
foreach v in agr pr cr prcn {
    preserve
    if "$trainingWheels"!="" {
        * in a trainingwheels run some values will may be missing
        drop if missing(`s')
    }
    keep block_pop `s' `v' 
    reshape wide `v', i(block_pop) j(`s')
    rename `v'0 solvar_0 
    rename `v'1 solvar_0_005
    rename `v'2 solvar_005_01
    rename `v'3 solvar_01_015
    rename `v'4 solvar_015_025
    rename `v'5 solvar_025
    format solvar_* %20.4f
    export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(solvar_`v') firstrow(variables) 
    restore
}

use `solvarAnalysisData', clear
collapse (sum) put_nmsabb conf_nmsabb nmsabb_oneton_pop, by(block_pop `s')
gen pr = 100*put_nmsabb/nmsabb_oneton_pop
gen cr = 100*conf_nmsabb/nmsabb_oneton_pop
gen prcn = 100*conf_nmsabb/put_nmsabb
keep block_pop `s' pr cr prcn
foreach v in pr cr prcn {
    preserve
    if "$trainingWheels"!="" {
        * in a trainingwheels run some values will may be missing
        drop if missing(`s')
    }
    keep block_pop `s' `v' 
    reshape wide `v', i(block_pop) j(`s')
    rename `v'0 solvar_0 
    rename `v'1 solvar_0_005
    rename `v'2 solvar_005_01
    rename `v'3 solvar_01_015
    rename `v'4 solvar_015_025
    rename `v'5 solvar_025
    format solvar_* %20.4f
    export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(solvar_`v'_nmsabb) firstrow(variables) 
    restore
}

* statistics by block-level solvar distribution 
use `solvarAnalysisData', clear
* create uniform random variable to shuffle within s_iv values
gen u = uniform()
replace s_iv = 100*s_iv
* sort ascending by s_iv and the uniform
sort s_iv u
* compute each block's position on the solvar dist
gen p = 100*_n/_N
* bin into quantiles
egen qtile = cut(p), at(0 5 10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85 90 95 105)
* collapse to qtile bin level
collapse (sum) solvar_value=iv pop=cefpop (min) solvar_cutoff=s_iv, by(qtile)
sort qtile
* compute additional variables
gen solvar_value_cumul = sum(solvar_value)
gen pop_cumul = sum(pop)
gen solvar_pct_cumul = 100*(solvar_value_cumul/(2*pop_cumul))
gen solvar_pct = 100*(solvar_value/(2*pop))
order qtile solvar_cutoff solvar_value solvar_value_cumul pop pop_cumul solvar_pct solvar_pct_cumul
format solvar_value solvar_value_cumul pop pop_cumul  %20.0gc 
format solvar_cutoff solvar_pct solvar_pct_cumul %6.5f
export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(solvar_qtile) firstrow(variables)

* statistics of solvar distribution by block pop
use `solvarAnalysisData', clear
gen zsvpop = cefpop if s_iv==0
gen zsvblk = 1 if s_iv==0
gen blk = 1
gen pop = cefpop
collapse (sum) zsvpop zsvblk blk pop, by(block_pop)
format zsvpop zsvblk blk pop  %20.0gc 
export excel using $disclosure/CBDRB-FY22-DSEP-004.xlsx, sheet(solvar_blksz) firstrow(variables)


di "#### END OF SOLVAR STATS ####"
di c(current_date) " " c(current_time)
