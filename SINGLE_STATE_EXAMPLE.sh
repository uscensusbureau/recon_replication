#!/bin/bash

# Example on how to run recon on a single state. This example uses Alabama.

STATE="al"
FIPCOUNTY=073
FIPTRACT=005103
REIDENT="SINGLE_EXAMPLE"
PYTHON="python3"

$(./dbrtool.py --env) ;

./dbrtool.py --reident $REIDENT --register ;

python3 s0_download_data.py --reident $REIDENT $STATE ;

./dbrtool.py --reident $REIDENT --step1 --step2 --stusab $STATE ;

$PYTHON ./s3_pandas_synth_lp_files.py --reident $REIDENT $STATE $FIPCOUNTY $FIPTRACT ;

$PYTHON ./s4_run_gurobi.py --reident $REIDENT $STATE $FIPCOUNTY $FIPTRACT ;
