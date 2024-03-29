## Overview

The code in this replication package reconstructs microdata from a subset of
2010 Census Summary File 1 tables, links those reconstructed data to
commercial data and internal 2010 Census data containing personally identifiable
information, determines if such links constitute reidentification, and computes
statistics related to the reconstruction and reidentification. 

The code uses a combination of Python, Gurobi™, SQL, Stata, SAS, and bash
scripts.  Production runs of this software were performed on Amazon Web
Services (AWS) Elastic Map Reduce (EMR) clusters and AWS Elastic Compute Cloud
(EC2) instances. Using a cluster of 30 `r5.24xlarge` nodes, the reconstruction
step takes approximately 3 full days per run. Using a cluster of 25
`r5.24xlarge` nodes, the solution variability analysis takes approximately 14
days. Using a single `r5.24xlarge` node, the reidentification step takes
approximately 14 days.

In the [instructions for running the software](#Instructions), terms contained
within angle brackets (e.g. `<term>`) are to be substituted by the user. Terms
beginning with a dollar sign (e.g. `${term}`) are environment variables and may be copied as-is
to execute code.

## Data availability and provenance statements

This project uses both publicly available and confidential data as inputs. The
publicly available data consist of the 2010 Census Summary 1 File (2010 SF1)
tabulations, which are available at:

[https://www2.census.gov/census_2010/04-Summary_File_1/](https://www2.census.gov/census_2010/04-Summary_File_1/)

The necessary files are the zipped 2010 SF1 tables, with filenames
`<state>/<st>2010.sf1.zip`. This project used data for all 50 states and the
District of Columbia.

The confidential data consist of:
- Extracts of the 2010 Census Edited File (CEF)
- Extracts of the 2010 Hundred Percent Detail File (HDF)
- Extracts of commercially available data from the following vendors:
    - Experian Marketing Solutions Incorporated
    - Infogroup Incorporated
    - Targus Information Corporation
    - VSGI LLC.

The confidential data extracts used by the reidentifiation code are stored in
an AWS S3 bucket at:
`${DAS_S3ROOT}/recon_replication/CUI__SP_CENS_T13_recon_replication_data_20231215.zip`
where `$DAS_S3ROOT` is an environment variable giving the location of the
relevant bucket[^1].

The underlying confidential data that serve as the source of the extracts are
available inside the Census Enterprise Data Lake.  These confidential data have
been available at the Census Bureau for the past 12 years, and are expected to
be available for at least another 10 years. The original locations outside of
AWS are documented in DMS Project P-7502798.

[^1]: The `DAS_S3ROOT` environment variable is correctly set in properly configured DAS EC2 instances

### Statement about rights

- [x] We certify that the author(s) of the manuscript have legitimate access to and permission to use the data used in this manuscript. 
- [x] We certify that the author(s) of the manuscript have documented permission to redistribute/publish the data contained within this replication package.

Data contained within the replication package are covered under project #P-7502798 in the Census Bureau Data Management System (DMS). Publicly released outputs made from this project were approved for release by the Census Bureau Disclosure Review Board (DRB) under the following DRB approval numbers:
- CBDRB-FY22-DSEP-003
- CBDRB-FY22-DSEP-004

### Summary of availability

- [ ] All data **are** publicly available.
- [x] Some data **cannot be made** publicly available.
- [ ] **No data can be made** publicly available.

### Details on each data source

The results of this research rely on both publicly available 2010 Census
tabulations and confidential microdata from the 2010 Census and commercial
databases. Access to the confidential data is limited to Census Bureau
employees and those others with Special Sworn Status who have a work-related
need to access the data and are a listed contributor for project P-7502798 in
the Census Bureau's Data Management System.

| Data Source | Access |
| ----------- | ---- |
| 2010 Summary File 1 | Publicly available |
| 2010 Census Edited File (CEF) Extract | Confidential |
| 2010 Hundred Percent Detail File (HDF) Extract | Confidential |
| 2010 DAS Experiment 23.1 Microdata Detail File (MDF)[^2] | Confidential |
| 2010 DAS Experiment 23.1 Reconstructed Microdata Detail File (rMDF)[^2] | Confidential |
| Merged Commercial Data[^3]: | Confidential |

[^2]: The original MDF for DAS experiment 23.1 was unintentionally deleted.
 This prevents replication of the MDF-based results from the source file;
however, the reformatted files needed for reidentification are available and
provided with the other protected data files. The authors will update code and
results to use the publicly released 
[April 3, 2023 privacy protected microdata file (PPMF)](https://www2.census.gov/programs-surveys/decennial/2020/program-management/data-product-planning/2010-demonstration-data-products/04-Demonstration_Data_Products_Suite/2023-04-03/2023-04-03_Privacy-Protected_Microdata_File/),
which used the same DAS settings as experiment 23.1.

[^3]: Although the commercial data come from multiple vendors, those data were
harmonized and merged into a single file for use in reidentifiation

## Dataset list

| Data Source | File | Storage Format | Data Format | Data Dictionary |
| ----------- | ---- | -------------- | ----------- | --------------- |
| 2010 Summary File 1 | `<st>2010.sf1.zip` | zip | fixed-width | [2010 SF1 Documentation](https://www2.census.gov/programs-surveys/decennial/2010/technical-documentation/complete-tech-docs/summary-file/sf1.pdf) |
| 2010 Census Edited File (CEF) State Extracts | `cef<st><cty>.csv` | csv | csv | [`recon_replication/doc/cef_dict.md`](doc/cef_dict.md) |
| 2010 Census Edited File (CEF) Persons Extract for Swapping | `swap_pcef.csv` | csv | csv | [`recon_replication/doc/swap_pcef_dict.md`](doc/swap_pcef_dict.md) |
| 2010 Census Edited File (CEF) Housing Extract for Swapping, CSV | `swap_hcef.csv` | csv | csv | [`recon_replication/doc/swap_hcef_dict.md`](doc/swap_hcef_dict.md) |
| 2010 Census Edited File (CEF) Housing Extract for Swapping, SAS | `swap_hcef.sas7bdat` | sas7bdat | sas7bdat | [`recon_replication/doc/swap_hcef_dict.md`](doc/swap_hcef_dict.md) |
| 2010 Hundred Percent Detail File (HDF) Extract | `hdf<st><cty>.csv` | csv | csv | [`recon_replication/doc/hdf_dict.md`](doc/hdf_dict.md) |
| 2010 DAS Experiment 23.1 Reconstructed Microdata Detail File (rMDF) | `r02<st><cty>.csv` | csv | csv | [`recon_replication/doc/mdf_dict.md`](doc/mdf_dict.md) |
| 2010 DAS Experiment 23.1 Microdata Detail File (MDF) | `r03<st><cty>.csv` | csv | csv | [`recon_replication/doc/mdf_dict.md`](doc/mdf_dict.md) |
| 2010 Swap Experiment HI Reconstructed Microdata Detail File (rMDF) | `r04<st><cty>.csv` | csv | csv | [`recon_replication/doc/mdf_dict.md`](doc/mdf_dict.md) |
| 2010 Swap Experiment LO Reconstructed Microdata Detail File (rMDF) | `r05<st><cty>.csv` | csv | csv | [`recon_replication/doc/mdf_dict.md`](doc/mdf_dict.md) |
| Merged Commercial Data | `cmrcl<st><cty>.csv` | csv | csv | [`recon_replication/doc/cmrcl_dict.md`](doc/cmrcl_dict.md) |

## Commercial data provenance
The server initially housing both data and code for the reconstruction and
reidentification experiments no longer exists. In transitioning to a new
computational environment, the individual commercial data assets used to
generate the merged commercial data in the dataset list were not maintained in a
way to guarantee versioning. As such, the merged commercial data that was
maintained is considered to be the original input file for the purposes of this
replication archive. The following list gives information on the original
commercial assets:

| Data Source | File | Storage Format | Data Format | Data Dictionary |
| ----------- | ---- | -------------- | ----------- | --------------- |
| 2010 Experian Research File | `exp_edr2010.sas7bdat` | sas7bdat | SAS V8+ | [`recon_replication/doc/cmrcl_exp.md`](doc/cmrcl_exp.md)
| 2010 InfoUSA Research File | `infousa_jun2010.sas7bdat` | sas7bdat | SAS V8+ | [`recon_replication/doc/cmrcl_infousa.md`](doc/cmrcl_infousa.md)
| 2010 Targus Fed Consumer Research File | `targus_fedconsumer2010.sas7bdat` | sas7bdat | SAS V8+ | [`recon_replication/doc/cmrcl_targus.md`](doc/cmrcl_targus.md)
| 2010 VSGI Research File | `vsgi_nar2010.sas7bdat` | sas7bdat | SAS V8+ | [`recon_replication/doc/cmrcl_vsgi.md`](doc/cmrcl_vsgi.md)

## Computational requirements
Instructions for re-executing the reconstruction and solution variability code
in this replication package assumes access to an AWS cluster, AWS S3
storage, and a MySQL server for job scheduling.

Instructions for re-executing the reidentification code, which uses data
protected under Title 13, U.S.C., assume access to the U.S. Census Bureau's
Enterprise Environment. Documenting the computer setup for the Census Bureau's
Enterprise environment is beyond the scope of this document, for security
reasons. 

The documentation above is accurate as of December 15, 2023.

### Software requirements

- Python version 3.8
- Python package requirements are given in [`recon_replication/requirements.txt`](requirements.txt)
- The reconstruction and solution variability codebases require a [Gurobi™ license](https://www.gurobi.com/solutions/licensing/) and installation of Gurobi™ version 9.1.1 or higher
- The reconstruction codebase requires [installation of a MySQL server](https://dev.mysql.com/doc/refman/8.0/en/installing.html), version 8.0 or higher
- The reidentification codebase requires [installation of Stata](https://www.stata.com) version 16 or higher
- The swap codebase requires [installation of SAS](https://www.sas.com/en_us/home.html) version 9.4 or higher


### Controlled randomness

Randomness for the various matching experiments in reidentification is controlled
by columns of stored uniform draws in the CEF and commercial datasets.

At [default settings](https://www.gurobi.com/documentation/9.5/refman/method.html), and at
times due to [unexpected bugs in its closed-source code](https://groups.google.com/g/gurobi/c/iUT-KPjKxhE/m/Uqm4nvu3srcJ),
the Gurobi™ solver used for reconstruction exhibits can exhibit some mild
non-determinism resulting in small differences in the published results and the
results from replication.

#### Summary

Approximate time needed to reproduce the analyses on a standard (2022) desktop machine:

- [ ] <10 minutes
- [ ] 10-60 minutes
- [ ] 1-8 hours
- [ ] 8-24 hours
- [ ] 1-3 days
- [ ] 3-14 days
- [ ] > 14 days
- [x] Not feasible to run on a desktop machine, as described below.

#### Details

The reconstruction code was last run on a 30 node AWS `r5.24xlarge` cluster.
Computation took approximiately 3 days for each set of input tables.  The
solution variability was last run on a 25 node AWS `r5.24xlarge` cluster.
Computation took approximatley 2 weeks. The reidentifiation code was last run
on a single AWS `r5.24xlarge` node. Computation took approximately 2 weeks.
Each `r5.24xlarge` node has 96 vCPUs and 768GiB of memory.

## Description of programs/code

Reconstruction of the 2010 HDF via the publicly-available 2010 SF1 table files
and the computation of subsequent solution variability measures do not require
access to the Census Bureau's Enterprise Environment. The instructions below
assume access to Amazon Web Services (AWS), a cluster similar in size to the
[environment details above](#Details), an S3 bucket to hold the necessary SF1
input tabulations, and the [necessary Python packages](#Software-requirements).
Additionally, these steps require a
[license](https://www.gurobi.com/solutions/licensing/) to use the Gurobi™
optimization software; a free academic license is available.

Reidentification of a reconstructed 2010 HDF file (rHDF) requires access to
sensitive data assets given in the [dataset list](#Dataset-list). The
instructions below assume access to those data, a server within the Census
Enterprise environment with resources on par with a single AWS EC2 `r5.24xlarge`
node, and that the [necessary Python packages](#Software-requirements) have been
installed.  If the rHDF and solution variability results were created outside
the Census Enterprise environment, then the replicator will need to work with
Census staff to have their data files ingested.
    
### System setup

#### AWS EMR cluster creation
Access to AWS requires [creation of an account](https://aws.amazon.com/account/). Once the account is created,
replicators should follow instructions for [creating an AWS EMR cluster](https://aws.amazon.com/emr/getting-started/).

#### S3 bucket creation
Reconstruction via an AWS cluster requires that the necessary SF1 input
files exist within an AWS Simple Storage Service (S3) bucket. Replicators
should follow instructions for [creating an S3 bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/GetStartedWithS3.html).

#### MySQL server setup
The reconstruction software uses SQL, via MySQL, to manage the workload across the AWS cluster. 
Replicators should follow instructions for [creating a MySQL server](https://dev.mysql.com/doc/mysql-getting-started/en/).
The instructions below will assume that replicators are installing MySQL on the master node of
the AWS cluster, but replicators may choose to have a dedicated AWS EMR or EC2 instance
for the MySQL server if they prefer.  Then setup the desired database using the provided schema: [recon_replication/recon/schema_common.sql](recon/schema_common.sql)  
This can be done with the following command:
`mysql -u <ROOT_USERNAME> -p <DB_NAME> < recon_replication/recon/schema_common.sql`  

### List of provided metric results

- [recon_replication/metrics/results/2023-04-23_Detailed_Summary_Metrics.xlsx](metrics/results/2023-04-23_Detailed_Summary_Metrics.xlsx): 2010 Summary Metrics Production Settings Demonstration Data Product Suite (including PPMF) Version 2023-04-23
- [recon_replication/metrics/results/metrics_swapHIbt.xlsx](metrics/results/metrics_swapHIbt.xlsx): 2010 Summary Metrics for HI Swap Experiment
- [recon_replication/metrics/results/metrics_swapLObt.xlsx](metrics/results/metrics_swapLObt.xlsx): 2010 Summary Metrics for LO Swap Experiment

### List of software files

#### Reconstruction `recon_replication/recon`

- [recon_replication/recon/dbrtool.py](recon/dbrtool.py): Script designed to run all steps of reconstruction, including subroutines for querying the SQL database for status of the run
- [recon_replication/recon/s0_download_data.py](recon/s0_download_data.py): Downloads SF1 files
- [recon_replication/recon/s1_make_geo_files.py](recon/s1_make_geo_files.py): Generates SQL tables containing information on population sizes by 2010 Census summary level (geography)
- [recon_replication/recon/s2_nbuild_state_stats.py](recon/s2_nbuild_state_stats.py): Ingests SF1 tables for use in reconstruction
- [recon_replication/recon/s3_pandas_synth_lp_files.py](recon/s3_pandas_synth_lp_files.py): Read processed SF1 tables and create the linear programming (LP) files needed for the Gurobi™ solver
- [recon_replication/recon/s4_run_gurobi.py](recon/s4_run_gurobi.py): Invokes Gurobi™ to find solutions to the systems described by the LP files, outputs solutions in Gurobi™ SOL format
- [recon_replication/recon/s5_make_microdata.py](recon/s5_make_microdata.py): Convert the Gurobi™ solution (SOL) files into census tract-level microdata CSV files
- [recon_replication/recon/s6_make_rhdf.py](recon/s6_make_rhdf.py): Merge the census tract-level CSV files into a single national file
- [recon_replication/recon/dbrecon.py](recon/dbrecon.py): Contains utilities used by the other reconstruction scripts for interacting with the SQL database
- [recon_replication/recon/dbrecon_config.json](recon/dbrecon_config.json): Config file store MYSQL, DAS, GUROBI, and AWS key variables

#### Solution variability

- [recon_replication/recon/solution_variability/block_level_rewriter.py](recon/solution_variability/block_level_rewriter.py): Splits the census tract-level Gurobi™ solution files from reconstruction into census block-level components
- [recon_replication/recon/solution_variability/solvar/](recon/solution_variability/solvar/): Python module to compute solution variability for the census block-level components

#### Zero solution variability census tract extraction

- [recon_replication/extract/extract_tracts.py](extract/extract_tracts.py): Python script to generate statistics for a subset of census tracts having only zero solution-variability census blocks

#### MDF and PPMF conversion

- [recon_replication/mdf_rhdf.py](mdf_rhdf.py): Python script to convert 2010 Microdata Files (MDFs) and 2010 Privacy Protected Microdata Files (PPMFs) to rHDF format for use in reidentification
- [recon_replication/mdf_sf1/mdf_to_hdf.py](mdf_sf1/mdf_to_hdf.py): Python script to convert 2010 Microdata Files (MDFs) and 2010 Privacy Protected Microdata Files (PPMFs) to 2010 HDF format for use in 2010 SF1 table creation
- [recon_replication/mdf_sf1/make_recon_input.py](mdf_sf1/make_recon_input.py): Python script to create 2010 SF1 tables from 2010 HDF formatted data

#### Reidentification

- [recon_replication/reidmodule/runreid.py](reidmodule/runreid.py): Main script for running the first part of reidentification.
- [recon_replication/reidmodule/common/config.json](reidmodule/common/config.json): Configuration JSON file; should require no changes if paths given in the repository are used
- [recon_replication/reidmodule/common/admin.py](reidmodule/common/admin.py): Miscellaneous data management functions
- [recon_replication/reidmodule/common/match.py](reidmodule/common/match.py): Functions for determining matches
- [recon_replication/reidmodule/common/splitter.py](reidmodule/common/splitter.py): Classes for splitting data for multiprocessing
- [recon_replication/reidmodule/common/stats.py](reidmodule/common/stats.py): Classes for producing statistics needed for stage 2 of reidentification
- [recon_replication/reidmodule/common/tabber.py](reidmodule/common/tabber.py): Classes for producing tables used in processing
- [recon_replication/reidmodule/02_tabs/tabs.py](reidmodule/02_tabs/tabs.py): Produces tables used in processing
- [recon_replication/reidmodule/03_agree/agree.py](reidmodule/03_agree/agree.py): Performs record-by-record matching between datasets
- [recon_replication/reidmodule/04_putative/putative.py](reidmodule/04_putative/putative.py): Performs putative reidentification against commercial and CEF data
- [recon_replication/reidmodule/05_confirm/confirm.py](reidmodule/05_confirm/confirm.py): Confirms putative reidentifications against the CEF
- [recon_replication/reidpaper/programs/runall.do](reidpaper/programs/runall.do): Script to generate all reidentification statistics
- [recon_replication/reidpaper/programs/0.config.do](reidpaper/programs/0.config.do): Configuration script, including directory locations, statistics to run, and macros
- [recon_replication/reidpaper/programs/1.agree_stats.do](reidpaper/programs/1.agree_stats.do): Computes agreement statistics
- [recon_replication/reidpaper/programs/2.putative_stats.do](reidpaper/programs/2.putative_stats.do): Computes putative reidentification statistics
- [recon_replication/reidpaper/programs/3.confirm_stats.do](reidpaper/programs/3.confirm_stats.do): Computes reidentification confirmation statistics
- [recon_replication/reidpaper/programs/4.solvar.do](reidpaper/programs/4.solvar.do): Computes statistics based on solution variability


#### Table creation

- [recon_replication/results/make_tables.do](results/make_tables.do): Stata script to produce tabular output of reidentification statistics

#### Suppression
- [recon_replication/suppression/recode.py](suppression/recode.py): Python script to format CEF into format needed for suppression results
- [recon_replication/suppression/suppression.py](suppression/recode.py): Python script to produce suppression results

#### Swapping
- [recon_replication/reid_swap/swap_pairs.sas](reid_swap/swap_pairs.sas): SAS code to find household pairs to swap
- [recon_replication/reid_swap/pairs_driver.py](reid_swap/pairs_driver.py): Python script to run SAS swapping pairs code for multiple experiments
- [recon_replication/reid_swap/swap.py](reid_swap/swap.py): Python script to create swapped CEF file

#### Metrics
- [recon_replication/metrics/recode.py](metrics/recode.py): Python script to convert CEF and rMDF files into format needed for metric evaluation
- [recon_replication/metrics/metrics.py](metrics/metrics.py): Python script to compute accuracy metrics for reconstruction experiments relative to the CEF
- [recon_replication/metrics/tables.py](metrics/tables.py): Python script to convert metric results into spreadsheet format
- [recon_replication/metrics/config.yml](metrics/config.yml): Configuration file for the metrics script

## Instructions

### Reconstruction using census block- and census tract-level SF1 tables
The instructions assume that the user will store reconstruction results
in an AWS S3 bucket `<S3ROOT>`

1. Log into the AWS cluster
    - `ssh -A <aws_user>@<cluster master address>`
1. Clone reconstruction repository into user home directory
    - `git clone git@github.com:uscensusbureau/recon_replication.git`
1. Pull and update submodules
    - `cd recon_replication`
    - `git pull`
    - `git submodule update --init --recursive`
1. Link the reconstruction directory for convenience
    - `cd ~`
    - `ln -s recon_replication/recon`
1. Change to recon directory and ensure that you are on the `main` branch
    - `cd recon`
    - `git checkout main`
1. Fill out recon_replication/recon/dbrecon_config.json
    - `MYSQL_HOST: <MYSQL Hostname>`
    - `MYSQL_DATABASE: <MYSQL Database Name>`
    - `MYSQL_USER: <MYSQL Username>`
    - `MYSQL_PASSWORD: <MYSQL Password>`
    - `DAS_S3ROOT: <aws location to load/read files>`
    - `GUROBI_HOME: <Gurobi™ home>`
    - `GRB_APP_NAME: <Gurobi™ App Name>`
    - `GRB_LICENSE_FILE: <Gurobi™ license file location>`
    - `GRB_ISV_NAME: <Gurobi™ ISV name>`
    - `BCC_HTTPS_PROXY: <BCC HTTPS proxy (may not be needed for release)>`
    - `BCC_HTTP_PROXY : <BCC HTTP proxy (may not be needed for release)>`
    - `AWS_DEFAULT_REGION : <DEFAULT AWS REGION ex: us-gov-west-1>`
    - `DAS_ENVIROMENT : <DAS enviroment ex: ITECB>`
1. Setup environment variables
    - `$(./dbrtool.py --env)`
1. Create new reconstruction experiment and create database tables
    - `./dbrtool.py --reident hdf_bt --register`
1. Download SF1 tables and copy to S3
    - `python s0_download_data.py --reident hdf_bt --all`
    - `aws s3 cp 2010-re/hdf_bt/dist/ <S3_ROOT>/2010-re/hdf_bt/dist/ --recursive`
1. Run step1 to create geography files
    - `./dbrtool.py --reident hdf_bt --step1 --latin1`
1. Run step2 to ingest SF1 tables
    - `./dbrtool.py --reident hdf_bt --step2`
1. Resize cluster to 30 core nodes using the directions above
1. Run steps 3 & 4 to create LP and SOL files for reconstruction
    - `./dbrtool.py --reident hdf_bt --launch_all`
1. Check on status of completed census tracts
    - `./dbrtool.py --reident hdf_bt --status`
1. Relaunch idle clusters after 1 day
    - `./dbrtool.py --reident hdf_bt --launch_all`
1. Run steps 5 & 6 to produce microdata (rHDF)
    - `./dbrtool.py --reident hdf_bt --runbg --step5 --step6`
1. Verify that microdata was copied to S3 bucket
    - `aws s3 ls <S3_ROOT>/2010-re/hdf_bt/rhdf_bt.zip`

### Reconstruction using census block-level SF1 tables only

1. Register new reconstruction experiment and create database tables
    - `./dbrtool.py --reident hdf_b --register`
1. Copy SF1 tables to S3
    - `aws s3 cp 2010-re/hdf_bt/dist/ <S3_ROOT>/2010-re/hdf_b/dist/ --recursive`
1. Run step1 to create geography files
    - `./dbrtool.py --reident hdf_b --step1 --latin1`
1. Run step2 to ingest SF1 tables
    - `./dbrtool.py --reident hdf_b --step2`
1. Resize cluster to 30 core nodes
1. Run steps 3 & 4 to create LP and SOL files for reconstruction, using `blockonly` branch of the `recon_replication` repository
    - `./dbrtool.py --reident hdf_b --launch_all --branch blockonly`
1. Check on status of completed census tracts
    - `./dbrtool.py --reident hdf_b --status`
1. Relaunch idle clusters after 1 day
    - `./dbrtool.py --reident hdf_b --launch_all --branch blockonly`
1. Run steps 5 & 6 to produce microdata (rHDF)
    - `./dbrtool.py --reident hdf_b --runbg --step5 --step6`
1. Verify that microdata was copied to S3 bucket
    - `aws s3 ls <S3_ROOT>/2010-re/hdf_b/rhdf_b.zip`

### Solution variability

1. Change to the solution variability folder
    - `cd ~/recon/solution_variability`
1. In the `config.ini` file, add the AWS S3 bucket name to the end of this line: `s3Bucket = `
1. Run the splitter
    - `export SPARK_HOME=/usr/lib/spark && export PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-src.zip`
    - `setsid python block_level_rewriter.py -t text -i <S3ROOT>/2010-re/hdf_bt/work -o solvar/hdf_bt/2010-block-results &> rewriter_out.txt`
1. Run solution variability module
    - `export SPARK_HOME=/usr/lib/spark && export PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-src.zip`
    - `python -m solvar -d -i solvar/hdf_bt/2010-block-results -o solvar/hdf_b/solvar-out-block --age --demo &> solvar_out_$(date +"%FT%H%M").txt`

### Run extract of zero-solution-variability census tracts

1. Change to the `extract` folder
    - `cd ~/recon_replication`
1. Copy the reconstructed HDF file for the census block-tract experiment to the directory and unzip
    - `aws s3 cp <S3ROOT>/2010-re/hdf_bt/rhdf_bt.zip .`
    - `unzip -j rhdf_bt.zip`
1. Run the extraction
    - `python extract_tracts.py rhdf_bt.csv`
1. Copy the census tract extract to S3 if desired
    - `aws s3 cp rhdf_bt_0solvar_extract.csv <S3ROOT>/2010-re/hdf_bt/`

### Shutdown AWS cluster
At the point where the reconstructed HDF files for both experiments and the
solution variability results are created and copied into S3, the cluster
may be shutdown.


### Reidentification
The user must work with Census Bureau staff to ingest any publicly
created files into the Census Enterprise Environment. These instructions
will assume that the files are in an AWS S3 bucket `<CROOT>` accessible from that
environment.

1. Log into an appropriate server in the Census Enterprise environment
    - `ssh -A <server address>`
1. Create envrionment variables for convenience
    - `export workdir=<workdir>`
    - `export CROOT=<CROOT>`
1. Clone reconstruction repository into user work directory <workdir>
    - `mkdir -P ${workdir}`
    - `cd ${workdir}`
    - `git clone git@github.com:uscensusbureau/recon_replication.git`
1. Copy and extract confidential data to `${workdir}/data/reid_module` on EC2 instance:
    - `mkdir ${workdir}/data/reid_module/`
    - `aws s3 cp ${DAS_S3ROOT}/recon_replication/CUI__SP_CENS_T13_recon_replication_data_20230426.zip ${workdir}`
    - `unzip -d ${workdir} ${workdir}/CUI__SP_CENS_T13_recon_replication_data_20230426.zip`
1. Copy solution variability results to required location
    - `aws s3 cp ${CROOT}/solvar/scaled_ivs.csv ${workdir}/data/reid_module/solvar/`
1. Copy rHDFs from S3 bucket, extract, and create necessary links
    - `cd ${workdir}/data/reid_module/rhdf/r00/`
    - `aws s3 cp ${CROOT}/2010-re/hdf_bt/rhdf_bt.csv.zip .`
    - `unzip -j rhdf_bt.csv.zip`
    - `ln -s rhdf_bt.csv r00.csv`
    - `cd ${workdir}/data/reid_module/rhdf/r01/`
    - `aws s3 cp ${CROOT}/2010-re/hdf_b/rhdf_b.csv.zip .`
    - `unzip -j rhdf_b.csv.zip`
    - `ln -s rhdf_b.csv r01.csv`
1. Edit the [configuration file](reidmodule/common/config.ini) to point to the working directory by modifying occurrences of `<workdir>`
1. Run first stage of reidentification
    - `cd ${workdir}/recon_replication/reidmodule/`
    - `setsid /usr/bin/python3 runreid.py 40 r00`
1. Change to directory for second stage of reidentification
    - `cd ${workdir}/recon_replication/reidpaper/programs/`
1. Run second stage of reidentification
    - `setsid stata-se -b runall.do`
1. Change to the results directory
    - `cd ${workdir}/recon_replication/reidpaper/results/`
1. Numerical result from this module are not publicly shareable and will be located in:
    - `${workdir}/recon_replication/reidpaper/results/CBDRB-FY22-DSEP-004/CBDRB-FY22-DSEP-004.xlsx`

### Tabular output
1. Change to the directory containing Stata code for tabular output
    - `cd ${workdir}/recon_replication/results/`
1. Link the outputs from reidpaper_python into the `in` folder:
    - `ln -s ${workdir}/recon_replication/reidpaper/results/CBDRB-FY22-DSEP-004/CBDRB-FY22-DSEP-004.xlsx in/CBDRB-FY22-DSEP-004.xlsx`
1. Run the table generation code
    - `stata-se -b make_tables.do`
1. Change to output directory to view tabular results:
    - `cd ${workdir}/recon_replication/results/out/`

### Suppression
1. Change to the directory containing Python code for suppression results
    - `cd ${workdir}/recon_replication/suppression/`
1. Process CEF data into the required format
    - `python recode.py`
1. Create suppression output
    - `python suppression.py > suppression_results.txt`
1. Suppression results can be found in:
   - `${workdir}/recon_replication/suppression/suppresion_results.txt`

### Swapping [^1]
[^1] Replication results for swapping are obtained by using the
reconstructed swap files found in the [dataset list](#Dataset-list).
1. Change to the directory containing the swapping code
    - `cd ${workdir}/recon_replication/reid_swap/`
1. Create swap pair lists
    - `python pairs_driver.py`
1. Create swapped person file
    - `python swap.py`
1. Swapped CEF person files for input into reconstruction can be found in:
    - `${workdir}/recon_replication/reid_swap/LO/swapped_us.csv`
    - `${workdir}/recon_replication/reid_swap/HI/swapped_us.csv`

### Metrics
1. Change to the directory containing the metrics code
    - `cd ${workdir}/recon_replication/metrics/`
1. Create recoded CEF file needed for metrics
    - `python recode.py --infile ${workdir}/data/reid_module/cef/cef.csv --outfile cef.csv --cef`
1. Run metrics for HI swap experiment
    - `python metrics.py -c HIconfig.yml`
1. Run metrics for LO swap experiment
    - `python metrics.py -c LOconfig.yml`
1. Create spreadsheet output for HI swap results
    - `python tables.py -v r04 -r False`
1. Create spreadsheet output for LO swap results
    - `python tables.py -v r05 -r False`
1. Metric results for the HI swap experiment will be in:
    - `${workdir}/recon_replication/metrics/output/r04/metrics_r04.xlsx`
1. Metric results for the LO swap experiment will be in:
    - `${workdir}/recon_replication/metrics/output/r05/metrics_r05.xlsx`

## List of tables reproduced by or found in this replication package

This replication archive reproduces tabular results listed in the
[accompanying spreadsheet](<manuscript/hdsr/20231214-HDSR submission tables and figures.xlsx>)

