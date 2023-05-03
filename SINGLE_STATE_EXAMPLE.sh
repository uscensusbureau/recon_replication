#!/bin/bash

# Example on how to run recon on a single state. This example uses Alabama.

STATE="al"
FIPST=01
FIPCOUNTY=073
FIPTRACT=005103
TRACT=01073005103
REIDENT="SINGLE_EXAMPLE"
PYTHON="python3"

$(./dbrtool.py --env);

./dbrtool.py --reident $REIDENT --register;

python3 s0_download_data.py --reident $REIDENT $STATE;

./dbrtool.py --reident $REIDENT --step1 --step2 --stusab $STATE;

$PYTHON ./s3_pandas_synth_lp_files.py --reident $REIDENT --stusab $STATE --tract $FIPTRACT --st $FIPCOUNTY ;

$PYTHON ./s4_run_gurobi.py --reident $REIDENT --stusab $STATE --tract $FIPTRACT --st $FIPCOUNTY ;

$PYTHON ./s5_make_microdata.py --reident $REIDENT --stusab $STATE --tract $FIPTRACT --st $FIPCOUNTY ;
