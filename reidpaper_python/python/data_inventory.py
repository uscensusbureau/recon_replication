# data_inventory.py
#-----------------------------
# /usr/bin/env python3 data_inventory.py
# normal execution is via call in runall.py
#-----------------------------
# Created By: David Zhou, Paul Bartholomew, Grace Tang, Andrew Sillers
# Created Date: 5/31/2021
# version = '1.0'
#-----------------------------
# Description:
'''
data_inventory.py:

This code is a translation of existing research STATA code developed by MITRE for the purposes of testing
the corresponding reidentification ouputs. This python file is the translation of "0.data_inventory.do", responsible
for the iterative calling of "descDelimFile" (from config.py) on the different input files needed for
running the reidpaper code.

Previous STATA Code Header:
[There was no code header]
'''

import os
from config import *

# rHDF-HDF, fuzzy and binage
descDelimFile(f"test_input/test_agree_hdf_{rhdf}.csv")
descDelimFile(f"test_input/test_agreematch_hdf_{rhdf}.csv")

descDelimFile(f"test_input/test_agreebinage_hdf_{rhdf}.csv")
descDelimFile(f"test_input/test_agreematchbinage_hdf_{rhdf}.csv")

# rHDF-CEF, fuzzy and binage
descDelimFile(f"test_input/test_agree_cef_{rhdf}.csv")
descDelimFile(f"test_input/test_agreematch_cef_{rhdf}.csv")

descDelimFile(f"test_input/test_agreebinage_cef_{rhdf}.csv")
descDelimFile(f"test_input/test_agreematchbinage_cef_{rhdf}.csv")

# block-only rHDF-CEF
descDelimFile(f"test_input/test_agreebinage_cef_{rhdfbo}.csv")
descDelimFile(f"test_input/test_agreematchbinage_cef_{rhdfbo}.csv")

# HDF-CEF
descDelimFile(f"test_input/hdf/test_agreebinage_cef_hdf.csv")
descDelimFile(f"test_input/hdf/test_agreematchbinage_cef_hdf.csv")
