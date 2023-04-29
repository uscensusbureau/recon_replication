#! /usr/bin/env python3

"""
Solution Variability Utility. Please use `./solvar.py -h` for help.
"""
# path hack given cluster initial state
import os
import sys
import re
import tqdm

# spark setup needed
def setup_env_spark():
    if 'SPARK_HOME' not in os.environ:
        os.environ['SPARK_HOME'] = '/usr/lib/spark'

    sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
    sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-src.zip'))

setup_env_spark()

import argparse, datetime
from solvar.core.utils import *
from solvar.core.base import Solvar
from solvar.solvar.run_compute_radii_byCounty import RunComputeRadiiByCounty
from solvar.core.filesystem import HDFS
from solvar.core.s3_utils import S3
from solvar.config.solutions_testLPs import solutions_testLPs


def main(args:argparse.Namespace) -> None:
    solvarParentDir = Path(__file__).absolute().parent.parent

    # set up logging
    logLevel = logging.DEBUG if (args.debug) else logging.INFO
    logging.basicConfig(filename=solvarParentDir.joinpath("solvar.log"), filemode='w',
                        format='%(asctime)s: (%(levelname)s)~  %(message)s',
                        datefmt='%I:%M:%S %p',
                        level=logLevel)
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter('(%(levelname)s)~  %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger().addHandler(console)

    logging.info("Welcome to solvar!")

    # Loading config file
    if (args.configFile):
        configFile = Path(args.configFile).expanduser().absolute()
    else:
        configFile = solvarParentDir.joinpath('config.ini')
        logging.info(f"Using the default configuration file <{configFile}>.")

    # Set up working directory (to dump logs and such, as well as on the filesystem)
    ts = datetime.datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%d-%H:%M:%S')
    workDir = solvarParentDir.joinpath("workDir").joinpath(f"sv{ts}")
    if (workDir.is_dir()): # already exists (how?! wow), delete
        svRmTree(workDir)
    svMkDir(workDir)

    # Set THE class objects
    thisSolvar = Solvar(args, solvarParentDir, workDir, configFile)

    logging.info("Configuration accepted.")

    # Check input folder and file validity
    if (thisSolvar.filesystem=='hdfs'):
        fs = HDFS()
    elif (thisSolvar.filesystem=='s3'):
        fs = S3(thisSolvar.s3Bucket)
    else:
        svExit(f"Invalid <filesystem> configuration.") #This should never happen. Please add constraints to config.py
    lpFiles = fs.loadInputs()

    if (not lpFiles):
        svExit(f"No valid <.lp> or <.lp.gz> inputs in path <{args.input}> in filesystem.")
    logging.debug(f"lpFiles are: {lpFiles}")

    solvedblocksFname = "solved_blocks.txt"
    if os.path.exists(solvedblocksFname):
        preLen = len(lpFiles)
        logging.warning(f"reading {solvedblocksFname}")
        with open(solvedblocksFname, "r") as f:
            solvedBlocks = [r.strip() for r in f.read().splitlines()]
        solvedBlocks = set(solvedBlocks)
        print(lpFiles)
        print(solvedBlocks)
        lpFiles = [i for i in tqdm.tqdm(lpFiles) if re.findall(r"block_(.*)\.lp", str(i))[0] in solvedBlocks]
        postLen = len(lpFiles)
        logging.warning(f"Discarded {preLen - postLen} Blocks")

    # All tracts whose ground truth values exist in solutions_testLPs must be in the input directory of the filesystem
    if args.test:
        lpFileNames = [lpFile.name.split('/')[-1] for lpFile in lpFiles]
        for testTractID in solutions_testLPs.keys():
            lpFileName = f'model_{testTractID}.lp'
            if lpFileName not in lpFileNames:
                svExit(f'Could not find {lpFileName} in test directory {args.input}')

    # Create output directory
    fs.mkdir(thisSolvar.output)

    # Run -- let's not store fs in the class to avoid serializing it (hdfs and s3 are not fun to serialize)
    runner = RunComputeRadiiByCounty(lpFiles)
    logging.info("Starting the parallel runs...")
    if (thisSolvar.doParallelize):
        isSuccess = runner.runComputeRadiiInParallel(ts)
        fs.fetchLogs(lpFiles)
    else:
        runner.runComputeRadiiSingleThreaded()
        isSuccess = True
    runner.dumpResultsToCsv(fs)
    if (not isSuccess):
        svExit("Failed to process all lp files!")

    # Run tests
    if args.test:
        overallTestResults = runner.runTests()
        logging.info(f'{overallTestResults.count(True)}/{len(overallTestResults)} tests passed!')

    logging.info("Exiting Solvar.")

if __name__ == "__main__":
    xArgParser = argparse.ArgumentParser (prog="solvar", description="Solution Variability")

    xArgParser.add_argument ('-d','--debug', help="Enable debugging mode.", action='store_true')
    xArgParser.add_argument ('-t','--test', help="Run in testing mode.", action='store_true')
    xArgParser.add_argument ('-c', '--configFile', help='Overwrites the default config file: ./config.ini')
    # Note that the paths are relative to the filesystem:
    #   - HDFS: /user/${USER}/this/part/is/configurable
    #   - S3:   s3://${s3Bucket}/this/part/is/configurable
    xArgParser.add_argument ('-i','--input', help="Input path in filesystem. All .lp and .lp.gz in all subfolders inside this directory will be considered.", required=True)
    xArgParser.add_argument ('-o','--output', help="Output path in filesystem. Files will be written here mimicking input directory hierarchy.", required=True)
    xArgParser.add_argument ('--demo', help="Set this flag to compute demo variability.", action='store_true')
    xArgParser.add_argument ('--age', help="Set this flag to compute age variability.", action='store_true')

    args = xArgParser.parse_args()
    main(args)
    exit(0)
