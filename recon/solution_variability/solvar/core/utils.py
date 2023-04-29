"""
Utils for solvar
"""

import os, sys, shutil, subprocess, re
import time, logging, traceback, gzip
from functools import wraps
from pathlib import Path, PurePath
from typing import Callable, List, Dict, Union, Optional, IO, Any, TextIO, Tuple, NoReturn, Set, Iterator, Sequence
from typing_extensions import TypedDict
import numpy as np


def svCatch(message : str) -> Callable:
    def wrappedFn_outer(func):
        @wraps(func)
        def wrappedFn_inner(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                logging.debug(f"{message}.\n<{func.__name__}>: args={args}, kwargs={kwargs}")
                svExit(message,exc=exc)
        return wrappedFn_inner
    return wrappedFn_outer

def formatExc(exc : Optional[Exception]) -> str:
    """ format the exception for printing """
    try:
        return f"<{exc.__class__.__name__}>: {exc}"
    except:
        return '<Non-recognized Exception>'

def svExit(message:str, exc:Exception=None, exitCode:int=1) -> NoReturn:
    if (message or exc):
        if (not message): #empty message
            message = f"{formatExc(exc)}."
        elif (exc):
            message += f"\n{formatExc(exc)}."
    logging.error(message)
    if (exc):
        logging.debug(traceback.format_exc())
    exit(exitCode)

@svCatch("Failed to make directory tree.")
def svMkDir(xDir:Path, errorIfExist:bool=True) -> None:
    xDir.mkdir(parents=True, exist_ok=(not errorIfExist))

@svCatch("Failed to remove tree.")
def svRmTree(xTree:Path) -> None:
    shutil.rmtree(xTree)

@svCatch("Failed to copy file.")
def svCopyFile(src:Path, dest:Path) -> None:
    """ Copies one file only. <src> to <dest> """
    shutil.copy2(src.as_posix(),dest.as_posix())

@svCatch("Failed to open file.")
def svOpenFile(filepath:Path, mode:str) -> IO[Any]:
    return open(filepath, mode)

@svCatch("Failed to extract geoid from filename.")
def extract_geoid(file:PurePath) -> str:
    """
    extract the 11-digit geoid from a complete filename (can include foldername)
    Examples:
        some/folder/51009010502.lp.gz -> 51009010502
        51009010502.txt -> 51009010502
    Warning: Do not return int since that ignores leading zeros, so tract 01xxx will become 1xxx
    """
    # scan for block
    geoidMatch = re.match(r"^\S*(?P<geoid>\d{15})\S*\.lp(\.gz)?$",file.name)
    if (geoidMatch):
        return str(geoidMatch.group("geoid"))
    else:
        # scan for tract
        # FIXME: this is ugly--rework regex
        geoidMatch = re.match(r"^\S*(?P<geoid>\d{11})\S*\.lp(\.gz)?$",file.name)
        if (geoidMatch):
            return str(geoidMatch.group("geoid"))
        else:
            raise Exception(f'geoid could not be parsed from filename {file}. '
                f'Make sure the filename includes the 11-digit geoid demarcated by underscores, '
                f'e.g. <something_something_12345678901_something>')

@svCatch("Failed to unzip file.")
def svUnzip(gzFile:Path) -> Path:
    logging.info(f"Unzipping {gzFile} ...")
    lpFile = gzFile.with_suffix('')
    with gzip.open(gzFile,'rb') as lpgz:
        with open(lpFile, 'wb') as lp:
            shutil.copyfileobj(lpgz, lp)
    return lpFile

@svCatch("Failed to compute populations.")
def compute_populations(lpFile:Path) -> Tuple[float, Dict[str, float]]:
    """
    Input:
        LP file for a tract
    Working:
        Sum the right hand sides of the equalities for the population constraints
        Population constraints are denoted by `P0010001` (this is defined in `resources/Microdata_Reconstruction_Brief_Explanation.pdf`)
        Each population constraint corresponds to a block
    Return:
        A tuple with 1st element = tract population (obtained from summing the block populations), and 2nd element = a dict with keys = 4-digit block codes (as strings) and values = population for that block (as floats)
    Example test:
        Input: `resources/small_public_examples/model_parms_48029980001_vpz0.lp`
        Blocks with population constraints have IDs 48029980001"1057", 48029980001"1088", 48029980001"1261"
        Output: (4.0, {'1057': 2.0, '1088': 1.0, '1261': 1.0})
    """
    with open(lpFile,'r') as f:
        lines = f.readlines()
    block_pops = {}
    for line in lines:
        pop_constraint_match = re.match(r"^_C_P0010001_(?P<geoid>\d{15})_\d+:.*=\s*(?P<rhs>\d+\.?\d*)\s*$", line) #match constraints beginning with _C_P0010001, then there has to be a 15-digit geoid (geoids of other lengths will be ignored), then the constraint number followed by :, then the constraint followed by equal sign, then a right hand side which may or may not have a decimal point
        if (pop_constraint_match):
            rhs = float(pop_constraint_match.group('rhs'))
            geoid = pop_constraint_match.group('geoid')
            blockid = geoid[-4:]
            if blockid in block_pops:
                logging.warning(f"Multiple '_C_P0010001_{geoid}' found, i.e. multiple constraints exist for the population of block {blockid}. Only the last one will be considered.")
            block_pops[blockid] = rhs
    tract_pop = sum(p for p in block_pops.values())
    return (tract_pop, block_pops)

@svCatch("Failed to zip the solvar library.")
def zipLibrary(solvarParentDir:Path, workDir:Path) -> Path:
    return Path(shutil.make_archive(
        base_name=workDir.joinpath("solvarLib").as_posix(),
        format="zip",
        root_dir=solvarParentDir.joinpath("solvar").as_posix()
    ))

@svCatch("Failed to pre-process LP by replacing starting ages with age bins.")
def replace_startAge_with_bin(lp_text:str, age_buckets:List[int]) -> str:
    """
    Replace the startAge of all LP variables with the bin to which the age belongs
    Examples for age_buckets = [0,1,5,10]:
        Change 'C_480299800011057_male_5.0_0_male_12_Y_Y_Y_Y_Y_Y_Y' to 'C_480299800011057_male_10.0_0_male_12_Y_Y_Y_Y_Y_Y_Y'. Notice that 5.0 is replaced with 10.0 because the age 12 lies in bin 10
        Change 'C_480299800011057_male_0.0_0_male_1_Y_Y_Y_Y_Y_Y_Y' to 'C_480299800011057_male_0.0_0_male_1_Y_Y_Y_Y_Y_Y_Y'. Notice that 0.0 is replaced with 1.0 because the age 1 lies in bin 1
    """
    _age_buckets = np.sort(age_buckets) #type: ignore ## since numpy stubs are different

    def get_bin(age:int, age_buckets:Any) -> float:
        """
        Given an age and age_buckets (sorted numpy array), return the bin to which the age belongs
        Examples for age_buckets = [0,1,5,10]:
            age=12, return 10.0
            age=1, return 1.0
        """
        diffs = age - age_buckets
        return float(age_buckets[np.argmin(diffs[diffs>=0])])
        # NOTE: Since age_buckets is an array sorted according to ascending order, diffs will have elements in descending order. As a result, argmin(diffs[diffs>=0]) correctly identifies the location of the smallest positive element despite shriking the array.

    return re.sub(r"(C_\d{15}_(?:fe)?male_)(\d+\.0)(_\d+_(?:fe)?male_)(\d+)((?:_[YN]){7})", lambda m: f"{m.group(1)}{get_bin(int(m.group(4)), _age_buckets)}{m.group(3)}{m.group(4)}{m.group(5)}", lp_text)
