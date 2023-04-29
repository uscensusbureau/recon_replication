import py.test
import os
import os.path
from os.path import abspath,dirname,basename
import sys
import pytest

MY_DIR     = dirname(abspath(__file__))
PARENT_DIR = dirname(MY_DIR)
if PARENT_DIR not in sys.path:
    sys.path.append(PARENT_DIR)

S3_TESTFILE = '$DAS_S3INPUTS/unit-test-data/stats_2010/recon/testfile.txt.gz'
S3_TESTFILE_CONTENTS='Hello World.\nThis is a test.\n'

# this shouldn't be necessary
#pylint: disable=E0401
import dbrecon


def test_validate_lpfile():
    assert dbrecon.validate_lpfile( os.path.join(MY_DIR, "shortfile_bad.lp")) == False
    assert dbrecon.validate_lpfile( os.path.join(MY_DIR, "shortfile_bad.lp.gz")) == False
    assert dbrecon.validate_lpfile( os.path.join(MY_DIR, "model_29183980000.lp.gz")) == True


def test_s3gateway():
    v1 = dbrecon.dopen(S3_TESTFILE, 'r', download=False).read()
    assert v1 == S3_TESTFILE_CONTENTS
    v2 = dbrecon.dopen(S3_TESTFILE, 'r', download=True).read()
    assert v2 == S3_TESTFILE_CONTENTS
