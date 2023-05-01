import pytest
import sys
import os
import datetime

sys.path.append( os.path.join( os.path.dirname(__file__), ".."))

from gurobi_logfile_parser import *
from datetime import datetime

def test_parser():
    p = GurobiLogfileParser(logfile= os.path.join( os.path.dirname(__file__), "model_04001944300.log"))
    assert p.gurobi_version=='8.1.1'
    assert p.start==datetime(2019, 6, 21, 10, 4, 21)
    assert p.rows == 137360
    assert p.columns == 4075777
    assert p.nonzeros == 51432411
    assert p.presolve_rows == 156
    assert p.presolve_columns == 755
    assert p.presolve_NZ == 1510
    assert p.integer_vars == 755
    assert p.binary_vars == 585
    assert p.simplex_iterations == 0
    assert p.seconds == 51.80
    assert p.nodes   == 0
    
    
    
