#!/bin/bash

# Example on how to run recon on a single state. This example uses Alabama.

STATE="al"
REIDENT="SINGLE_EXAMPLE"

$(./dbrtool.py --env);

./dbrtool.py --reident $REIDENT --register;

python3 s0_download_data.py --reident $REIDENT al;

./dbrtool.py --reident $REIDENT --step1 --step2 --stusab al;

./dbrtool.py --reident $REIDENT --stusab al --runbg --launch_all --step5 --step6;