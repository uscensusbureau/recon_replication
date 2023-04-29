import sys,os,os.path

sys.path.append( os.path.join( os.path.dirname(__file__), ".."))


import constants


def test_STUSAB_TO_STATE() -> None:
    try:
        import das_constants
        assert constants.STUSAB_TO_STATE['WY'] == 56
    except ModuleNotFoundError:
        import pytest
        pytest.skip()
