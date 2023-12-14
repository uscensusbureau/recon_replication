# Prerequisite Inputs
* county-level csv CEF data
* county-level csv HDF data
* county-level csv commercial data
* county-level csv rHDF data
* agreement, putative, and confirmation match output from `reidmodule`

# Example Execution

>setsid stata-se -b runall.do

# Program Inventory
* runall.do - executes sequence of stata programs that compute statistics for the reid paper
* ./programs/0.config.do - parms and dir references for stata
* ./programs/1.agree_stats.do - stats related to agreement matches
* ./programs/2.putconf_stats.do - stats related to putative and confirmation matches
* ./programs/3.modal.do - stats related to matches among modal and non-modal race and ethnicities
* ./programs/4.uniques.do - stats related to counts of unique combinations of demographic characteristics
* ./programs/5.kton.do - stats related to matches among combinations of demographic characteristics shared by K individuals in a block


