*As a work of the United States government, this project is in the public
*domain within the United States. Additionally, we waive copyright and related
*rights in the work worldwide through the CC0 1.0 Universal public domain
*dedication (https://creativecommons.org/publicdomain/zero/1.0/)

*--------------------------------------------------
* LOG THE MATCH FILES USED IN THE ANALYSIS

*--------------------------------------------------
* AGREEMENT EXPERIMENTS
*--------------------------------------------------

* rHDF-HDF, fuzzy and binage
descDelimFile "$rsltbase/rhdf/${rhdf}/agree_hdf_${rhdf}.csv"
descDelimFile "$rsltbase/rhdf/${rhdf}/agreematch_hdf_${rhdf}.csv"

descDelimFile "$rsltbase/rhdf/${rhdf}/agreebinage_hdf_${rhdf}.csv"
descDelimFile "$rsltbase/rhdf/${rhdf}/agreematchbinage_hdf_${rhdf}.csv"

* rHDF-CEF, fuzzy and binage
descDelimFile "$rsltbase/rhdf/${rhdf}/agree_cef_${rhdf}.csv"
descDelimFile "$rsltbase/rhdf/${rhdf}/agreematch_cef_${rhdf}.csv"

descDelimFile "$rsltbase/rhdf/${rhdf}/agreebinage_cef_${rhdf}.csv"
descDelimFile "$rsltbase/rhdf/${rhdf}/agreematchbinage_cef_${rhdf}.csv"

* block-only rHDF-CEF
descDelimFile "$rsltbase/rhdf/${rhdfbo}/agreebinage_cef_${rhdfbo}.csv"
descDelimFile "$rsltbase/rhdf/${rhdfbo}/agreematchbinage_cef_${rhdfbo}.csv"

* HDF-CEF
descDelimFile "$rsltbase/hdf/agreebinage_cef_hdf.csv"
descDelimFile "$rsltbase/hdf/agreematchbinage_cef_hdf.csv"

*--------------------------------------------------
* PUTATIVE AND CONFIRMATION EXPERIMENTS
*--------------------------------------------------

* CEF-CEF
descDelimFile "$rsltbase/cef/putativebinage_cef_cef.csv"
descDelimFile "$rsltbase/cef/putativematchbinage_cef_cef.csv"
descDelimFile "$rsltbase/cef/confirmbinage_cefcef_cef.csv"
descDelimFile "$rsltbase/cef/confirmmatchbinage_cefcef_cef.csv"

* CEF-CMRCL
descDelimFile "$rsltbase/cef/putativebinage_cef_cmrcl.csv"
descDelimFile "$rsltbase/cef/putativematchbinage_cef_cmrcl.csv"
descDelimFile "$rsltbase/cef/confirmbinage_cefcmrcl_cef.csv"
descDelimFile "$rsltbase/cef/confirmmatchbinage_cefcmrcl_cef.csv"

* rHDF-CMRCL
descDelimFile "$rsltbase/rhdf/${rhdf}/putativebinage_${rhdf}_cmrcl.csv"
descDelimFile "$rsltbase/rhdf/${rhdf}/putativematchbinage_${rhdf}_cmrcl.csv"
descDelimFile "$rsltbase/rhdf/${rhdf}/confirmbinage_${rhdf}cmrcl_cef.csv"
descDelimFile "$rsltbase/rhdf/${rhdf}/confirmmatchbinage_${rhdf}cmrcl_cef.csv"

* rHDF-CEF
descDelimFile "$rsltbase/rhdf/${rhdf}/putativebinage_${rhdf}_cef.csv"
descDelimFile "$rsltbase/rhdf/${rhdf}/putativematchbinage_${rhdf}_cef.csv"
descDelimFile "$rsltbase/rhdf/${rhdf}/confirmbinage_${rhdf}cef_cef.csv"
descDelimFile "$rsltbase/rhdf/${rhdf}/confirmmatchbinage_${rhdf}cef_cef.csv"

* block-only rHDF-CEF
descDelimFile "$rsltbase/rhdf/${rhdfbo}/putativebinage_${rhdfbo}_cef.csv"
descDelimFile "$rsltbase/rhdf/${rhdfbo}/putativematchbinage_${rhdfbo}_cef.csv"
descDelimFile "$rsltbase/rhdf/${rhdfbo}/confirmbinage_${rhdfbo}cef_cef.csv"
descDelimFile "$rsltbase/rhdf/${rhdfbo}/confirmmatchbinage_${rhdfbo}cef_cef.csv"

* HDF-CEF
descDelimFile "$rsltbase/hdf/putativebinage_hdf_cef.csv"
descDelimFile "$rsltbase/hdf/putativematchbinage_hdf_cef.csv"
descDelimFile "$rsltbase/hdf/confirmbinage_hdfcef_cef.csv"
descDelimFile "$rsltbase/hdf/confirmmatchbinage_hdfcef_cef.csv"

*--------------------------------------------------
