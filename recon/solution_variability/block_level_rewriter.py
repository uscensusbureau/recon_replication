#! /usr/bin/env python3
"""
Block Level Model Rewriter (Spark)

TODO: this takes most of the setup and configuration code from solvar.__main__. There should be a better way
of using the same setup process for both application entry-points. Maybe this hints at a bigger refactoring
effort?

TODO: solve this by adding it as a sub-option to solvar.__main__?
"""
from solvar.core.utils import *
import argparse, datetime
from pathlib import Path
from solvar.rewriter.blockRunner import RewriteRunner
import solvar.rewriter.blockRewriter as brw
import solvar.rewriter.blockRewriterText as brt


def get_rewriter(args: argparse.Namespace):
    """given args, determine which rewriter to use"""
    if args.type == 'text':
        return brt.BlockNoPctTextRewriter() if args.no_pct else brt.BlockIneqPctTextRewriter()
    elif args.type == 'gurobi':
        return brw.BlockNoPctRewriter() if args.no_pct else brw.BlockIneqPctRewriter()
    else:
        raise ValueError(f"args type '{args.type}' is invalid")


def main_local(args: argparse.Namespace):
    """TODO: share the logging setup?"""
    import glob

    logging.info("Welcome to block level rewriter!")

    rewriter = get_rewriter(args)

    # make output directory
    svMkDir(Path(args.output), errorIfExist=False)

    # collect the input lpFile
    lpFiles = [Path(file) for file in glob.glob(str(Path(args.input) / "*.lp"))]

    # perform the rewrite
    rewriter.rewrite_lp_files(None, Path(args.output), lpFiles)

    # Run the rewriter logic here
    logging.info("Exiting Rewriter.")


def main_solvar(args:argparse.Namespace) -> None:
    from solvar.core.base import Solvar
    from solvar.core.filesystem import HDFS
    from solvar.core.s3_utils import S3
    from solvar.config.solutions_testLPs import solutions_testLPs

    # add these as solvar requires it
    args.demo = True
    args.age = True
    args.test = False

    solvarParentDir = Path(__file__).absolute().parent

    # set up logging
    logLevel = logging.DEBUG if (args.debug) else logging.INFO
    logging.basicConfig(filename=solvarParentDir.joinpath("rewriter.log"), filemode='w',
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

    logging.info("Welcome to block level rewriter!")

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

    # removed solved tracts if applicable for incremental runs
    # TODO: add this as a config item?
    solved_tracts_fname = "solved_tracts.txt"
    if os.path.exists(solved_tracts_fname):
        preLen = len(lpFiles)
        logging.warning("reading solved_tracts.txt")
        with open(solved_tracts_fname, "r") as f:
            solvedBlocks = f.read().splitlines()
        solvedBlocks = set(solvedBlocks)
        import re
        import tqdm
        lpFiles = [i for i in tqdm.tqdm(lpFiles) if re.findall(r"model_(.*)\.lp", str(i))[0] not in solvedBlocks]
        postLen = len(lpFiles)
        logging.warning(f"Discarded {preLen - postLen} Tracts")
    
    solved_tracts_fname = "whitelist_tracts.txt"
    if os.path.exists(solved_tracts_fname):
        preLen = len(lpFiles)
        logging.warning("reading whitelist_tracts.txt")
        with open(solved_tracts_fname, "r") as f:
            solvedBlocks = f.read().splitlines()
        solvedBlocks = set(solvedBlocks)
        import re
        import tqdm
        lpFiles = [i for i in tqdm.tqdm(lpFiles) if re.findall(r"model_(.*)\.lp", str(i))[0] in solvedBlocks]
        postLen = len(lpFiles)
        logging.warning(f"Remaining {postLen} Tracts")
    
    """
    print(lpFiles[:3])
    print("load sorted tracts")
    import pandas as pd
    tcsv = pd.read_csv("tracts_sorted.txt",  sep=' ', header=None)
    tcsv = dict(zip(*(tcsv[1], tcsv[0])))
    def skey(e):
        gid = int(re.findall(r"([0-9]{11})", str(e))[0])
        return tcsv[gid]
    print("sort lpFiles")
    lpFiles.sort(key=skey)
    print(lpFiles[:3])
    """

    # All tracts whose ground truth values exist in solutions_testLPs must be in the input directory of the filesystem
    if args.test:
        lpFileNames = [lpFile.name.split('/')[-1] for lpFile in lpFiles]
        for testTractID in solutions_testLPs.keys():
            lpFileName = f'model_{testTractID}.lp'
            if lpFileName not in lpFileNames:
                svExit(f'Could not find {lpFileName} in test directory {args.input}')

    # Create output directory
    fs.mkdir(thisSolvar.output)

    rewriter = get_rewriter(args)
    runner = RewriteRunner(lpFiles, rewriter)
    logging.info("Starting the parallel runs...")
    if (thisSolvar.doParallelize):
        isSuccess = runner.runRewriteInParallel(ts)
        # TODO: deal with this
        #fs.fetchLogs(lpFiles)
    else:
        runner.runRewriteSingledThreaded()
        isSuccess = True
    if (not isSuccess):
        svExit("Failed to process all lp files!")

    # Run the rewriter logic here
    logging.info("Exiting Rewriter.")


def main(args: argparse.Namespace):
    """rewriter main
    two apps are implemented:
        * one to run on a cluster with the rest of solvar
        * one to run locally (local filesystem)
    """
    available_modes = {'solvar', 'local'}
    if args.mode == 'solvar':
        main_solvar(args)
    elif args.mode == 'local':
        main_local(args)
    else:
        raise ValueError(f"Argument Mode '{args.mode}' is not understood (valid ones are {available_modes})")


if __name__ == "__main__":
    xArgParser = argparse.ArgumentParser (prog="solvar", description="Solution Variability")

    xArgParser.add_argument ('-d','--debug', help="Enable debugging mode.", action='store_true')
    xArgParser.add_argument ('-c', '--configFile', help='Overwrites the default config file: ./config.ini')
    xArgParser.add_argument ('-m', '--mode', default='solvar', help='whether to run on via "solvar" or "local"')
    xArgParser.add_argument ('-t', '--type', default='gurobi', help='whether to use the "text" or "gurobi" rewriter')
    xArgParser.add_argument ('-i','--input', help="Input path in filesystem. All .lp and .lp.gz in all subfolders inside this directory will be considered.", required=True)
    xArgParser.add_argument ('-o','--output', help="Output path in filesystem. Files will be written here mimicking input directory hierarchy.", required=True)
    xArgParser.add_argument("--no-pct", action='store_true', help='ignore tract level constraints')

    args = xArgParser.parse_args()
    main(args)
    exit(0)
