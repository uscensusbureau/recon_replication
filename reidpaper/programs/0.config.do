*As a work of the United States government, this project is in the public
*domain within the United States. Additionally, we waive copyright and related
*rights in the work worldwide through the CC0 1.0 Universal public domain
*dedication (https://creativecommons.org/publicdomain/zero/1.0/)

*--------------------------------------------------
* DIRS
*--------------------------------------------------
global cefdir "/data/reid_module/cef/"
global hdfdir "/data/reid_module/hdf/"
global cmrcldir "/data/reid_module/cmrcl/"
global rhdfbasedir "/data/reid_module/rhdf/"
global rhdfbasersltdir "/data/reid_module/results/rhdf/"
global rsltbase "/data/reid_module/results/"
global geolookup "/data/reid_module/geo/"
global solvar "/data/reid_module/solvar/"
global tbl "../results/tables"
global fig "../results/figures"
global temp "../data/temp/"
global disclosure "../results/CBDRB-FY22-DSEP-004/"
global pop2010=308745538
global adoloc "./ado/"

*--------------------------------------------------
* PARAMS
*--------------------------------------------------
set seed 20210730
local rhdf="r00"
global rhdf="`rhdf'"
local rhdfbo="r01"
global rhdfbo="`rhdfbo'"
local rmdf="r02"
global rmdf="`rmdf'"
local mdf="r03"
global mdf="`mdf'"
local swphi="r04"
global swphi="`swphi'"
local swplo="r05"
global swplo="`swplo'"

global notestex "CBDRB-FY22-DSEP-004.tex"
* if trainingWheels is blank ("") then it will process all data 
* if trainingWheels is "rowrange(:10000)" then it will only pull a small number of records for testing
global trainingWheels ""

* EXPERIMENTS TO RUN
global agree_rhdf_hdf_fz = 0
global agree_rhdf_cef_fz = 0
global agree_rhdf_hdf_bn = 1
global agree_rhdf_cef_bn = 1
global agree_rhdfbo_cef_bn = 1
global agree_rhdfbo_hdf_bn = 1
global agree_rmdf_cef_bn = 1
global agree_rmdf_hdf_bn = 1
global agree_mdf_cef_bn = 1
global agree_mdf_hdf_bn = 1
global agree_hdf_cef_bn = 1
global agree_swphi_cef_bn = 1
global agree_swphi_hdf_bn = 1
global agree_swplo_cef_bn = 1
global agree_swplo_hdf_bn = 1

global putconf_rhdf_cmrcl_fz = 0
global putconf_rhdf_cef_fz = 0
global putconf_rhdf_cmrcl_bn = 1
global putconf_rhdf_cef_bn = 1
global putconf_cef_cef_bn = 1
global putconf_cef_cmrcl_bn = 1
global putconf_hdf_cef_bn = 1
global putconf_hdf_cmrcl_bn = 1
global putconf_rhdfbo_cef_bn = 1
global putconf_rhdfbo_cmrcl_bn = 1
global putconf_rmdf_cef_bn = 1
global putconf_rmdf_cmrcl_bn = 1
global putconf_mdf_cef_bn = 1
global putconf_mdf_cmrcl_bn = 1
global putconf_modalGsr_cef_bn = 1
global putconf_modalGsr_cmrcl_bn = 1
global putconf_propGsr_cef_bn = 1
global putconf_propGsr_cmrcl_bn = 1
global putconf_swphi_cmrcl_bn = 1
global putconf_swphi_cef_bn = 1
global putconf_swplo_cmrcl_bn = 1
global putconf_swplo_cef_bn = 1

*--------------------------------------------------
* ADOs
*--------------------------------------------------
quietly do $adoloc/graphexportpdf.ado
quietly do $adoloc/parmest.ado
quietly do $adoloc/parmcip.ado
quietly do $adoloc/parmby.ado
quietly do $adoloc/metaparm.ado

*--------------------------------------------------
* MACROS
*--------------------------------------------------
capture removestem drop removestem
program define removestem
    desc, varlist
    foreach v of varlist `r(varlist)' {
        local newname = subinstr("`v'","`1'","",.)
        rename `v' `newname'
    }
end

capture genAgeBin drop genAgeBin
program define genAgeBin
    args ageVar
    gen agebin = `ageVar' if `ageVar'<=21
    tostring agebin, replace
    replace agebin = "22-24" if `ageVar'>=22 & `ageVar'<=24
    replace agebin = "25-29" if `ageVar'>=25 & `ageVar'<=29
    replace agebin = "30-34" if `ageVar'>=30 & `ageVar'<=34
    replace agebin = "35-39" if `ageVar'>=35 & `ageVar'<=39
    replace agebin = "40-44" if `ageVar'>=40 & `ageVar'<=44
    replace agebin = "45-49" if `ageVar'>=45 & `ageVar'<=49
    replace agebin = "50-54" if `ageVar'>=50 & `ageVar'<=54
    replace agebin = "55-59" if `ageVar'>=55 & `ageVar'<=59
    replace agebin = "60-61" if `ageVar'>=60 & `ageVar'<=61
    replace agebin = "62-64" if `ageVar'>=62 & `ageVar'<=64
    replace agebin = "65-66" if `ageVar'>=65 & `ageVar'<=66
    replace agebin = "67-69" if `ageVar'>=67 & `ageVar'<=69
    replace agebin = "70-74" if `ageVar'>=70 & `ageVar'<=74
    replace agebin = "75-79" if `ageVar'>=75 & `ageVar'<=79
    replace agebin = "80-84" if `ageVar'>=80 & `ageVar'<=84
    replace agebin = "85+" if `ageVar'>=85
end

capture cutPop drop cutPop
program define cutPop
    args pop
    egen popg = cut(`pop'), at(0 10 50 100 250 500 1000 100000)
    label define popglbl 0 "0-9" 10 "10-49" 50 "50-99" 100 "100-249" 250 "250-499" 500 "500-999" 1000 "1000+"
    label values popg popglbl 
end

capture cutPopFine drop cutPopFine
program define cutPopFine
    args popin popout
    egen `popout' = cut(`popin'), at(0 1 2 3 4 5 6 7 8 9 10 50 100 250 500 1000 100000)
    label define popglbl 1 "1" 2 "2" 3 "3" 4 "4" 5 "5" 6 "6" 7 "7" 8 "8" 9 "9" 10 "10-49" 50 "50-99" 100 "100-249" 250 "250-499" 500 "500-999" 1000 "1000+"
    label values `popout' popglbl 
end

capture raceGroups drop raceGroups
program define raceGroups
    egen s = rowtotal(white black aian asian nhopi sor)
    gen racegrp = ""
    foreach v of varlist white black aian asian nhopi sor {
        replace racegrp = "`v' alone" if `v'==1 & s==1
    }
    replace racegrp = "other" if racegrp==""
end

* jensenshannon based on https://docs.scipy.org/doc/scipy/reference/generated/scipy.spatial.distance.jensenshannon.html
capture rel_entr drop rel_entr
program define rel_entr
    args x y v
    gen `v' = . 
    replace `v' = `x' * log((`x'/`y')) if `x'>0 & `y'>0
    replace `v' = 0 if `x'==0 & `y'>=0
end

capture jensenshannon drop jensenshannon
program define jensenshannon
    args p q by base
    gen js_BASE = "`base'"
    gen js_M = (`p' + `q')/2.0
    rel_entr `p' js_M js_P
    rel_entr `q' js_M js_Q
    bys `by': egen js_P_S = sum(js_P)
    bys `by': egen js_Q_S = sum(js_Q)
    gen js = (js_P_S + js_Q_S)
    if js_BASE[1]!=""{
        replace js = js/log(`base')
    }
    replace js = sqrt(js/2)
    drop js_M js_P js_P_S js_Q js_Q_S js_BASE 
end

capture resortblockpop drop  resortblockpop
program define resortblockpop
    args popvar
    gen sort = 0 if `popvar' == "0-9" 
    replace sort = 1 if `popvar' == "10-49" 
    replace sort = 2 if `popvar' == "50-99" 
    replace sort = 3 if `popvar' ==  "100-249" 
    replace sort = 4 if `popvar' ==  "250-499" 
    replace sort = 5 if `popvar' ==  "500-999" 
    replace sort = 6 if `popvar' ==  "1000+"
    sort sort
    drop sort
end

capture makeRegTable drop makeRegTable 
program define makeRegTable 
    * prepares regression tex table from output
    desc 
    local newObs = `r(N)' + 3
    set obs `newObs'
    replace parm = "r2" if _n==`newObs'-1
    replace estimate = rsq[1] if _n==`newObs'-1
    replace parm = "N" if _n==`newObs'
    replace estimate = n[1] if _n==`newObs'
    gen e = estimate
    tostring estimate, replace force
    replace estimate = "" if estimate == "."
    replace estimate = estimate + "***" if (abs(e/stderr)) > 3.291 & regexm(estimate,"\*") != 1 & parm != "r2" & parm != "N" & estimate != "" 
    replace estimate = estimate + "**" if (abs(e/stderr)) > 1.96 & regexm(estimate,"\*") != 1 & parm != "r2" & parm != "N" & estimate != "" 
    replace estimate = estimate + "*" if (abs(e/stderr)) > 1.645 & regexm(estimate,"\*") != 1 & parm != "r2" & parm != "N" & estimate != ""
    drop if parm == "_cons"
    replace parm = "Block Size 0 to 9" if regexm(parm,"1.block")
    replace parm = "Block Size 10 to 49" if regexm(parm,"2.block")
    replace parm = "Block Size 50 to 99" if regexm(parm,"3.block")
    replace parm = "Block Size 100 to 249" if regexm(parm,"4.block")
    replace parm = "Block Size 250 to 499" if regexm(parm,"5.block")
    replace parm = "Block Size 500 to 999" if regexm(parm,"6.block")
    replace parm = "Block Size 1000+" if regexm(parm,"7b.block")
    replace parm = "Unique Share Age-Sex-Race-Eth" if regexm(parm,"uniasr")
    tostring stderr, replace force
    replace stderr = "" if stderr == "."
    gen order = _n
    preserve
    drop if parm=="r2" | parm=="N" | parm==""
    keep order stderr
    replace stderr = "(" + stderr + ")"
    replace order = order + .5
    tempfile se
    save `se'
    restore 
    append using `se'
    sort order 
    replace estimate = stderr if substr(stderr, 1,1)=="("
    replace parm = "\$R^2\$" if parm=="r2"
    replace estimate = "(omitted)" if parm == "Block Size 1000+"
    replace estimate = "" if estimate == "(0)"
    keep parm order estimate
end

capture descDelimFile drop descDelimFile
program define descDelimFile
    args inFile
    shell ls -an1 -h `inFile'
    shell wc -l `inFile'
end

