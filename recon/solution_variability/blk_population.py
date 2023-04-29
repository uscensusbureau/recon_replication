import argparse
import pathlib
import datetime
import pandas as pd
import pyspark
import tempfile
from solvar.core.utils import *
from solvar.core.filesystem import HDFS
from solvar.core.s3_utils import S3
from pyspark.context import SparkContext
from pyspark.conf import SparkConf


def compute_populations(inp) -> Tuple[str, float]:
    s3Bucket, lpFile = inp

    tempDir = tempfile.TemporaryDirectory(prefix=f"solvar-populations-")
    parentPath =  Path(tempDir.name)
    localLp = parentPath.joinpath(lpFile.name)
    # Download the lp file
    fs = S3(s3Bucket)
    fs.downloadFile(lpFile, localFilepath=localLp)

    # Unzip if needed
    if (localLp.suffix=='.gz'):
        localLp = svUnzip(localLp)

    with open(localLp, 'r') as f:
        lines = f.readlines()

    block_pops = {}
    block_pop = None
    geoid = "ERROR"
    for line in lines:
        pop_constraint_match = re.match(r"^_C_P0010001_(?P<geoid>\d{15})_\d+:.*=\s*(?P<rhs>\d+\.?\d*)\s*$", line) #match constraints beginning with _C_P0010001, then there has to be a 15-digit geoid (geoids of other lengths will be ignored), then the constraint number followed by :, then the constraint followed by equal sign, then a right hand side which may or may not have a decimal point
        if (pop_constraint_match):
            rhs = float(pop_constraint_match.group('rhs'))
            geoid = pop_constraint_match.group('geoid')
            blockid = geoid
            if blockid in block_pops:
                print(f"Multiple '_C_P0010001_{geoid}' found, i.e. multiple constraints exist for the population of block {blockid}. Only the last one will be considered.")
            block_pops[blockid] = rhs
            if block_pop is not None:
                print(f"Block Pop is not None! Are multiple blocks present ({geoid})?")
            block_pop = rhs
    tract_pop = sum(p for p in block_pops.values())
    return geoid, block_pop


def main_solvar(args: argparse.Namespace) -> None:
    from solvar.core.base import Solvar
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

    # All tracts whose ground truth values exist in solutions_testLPs must be in the input directory of the filesystem
    if args.test:
        lpFileNames = [lpFile.name.split('/')[-1] for lpFile in lpFiles]
        for testTractID in solutions_testLPs.keys():
            lpFileName = f'model_{testTractID}.lp'
            if lpFileName not in lpFileNames:
                svExit(f'Could not find {lpFileName} in test directory {args.input}')

    # Create output directory
    fs.mkdir(thisSolvar.output)

    # create the pyspark context
    sparkMaster = 'yarn'
    #sc = pyspark.SparkContext('local[*]')
    scConf = SparkConf()
    if (sparkMaster=='yarn'): # We need to take the rest of the library to the container
        zippedLib = zipLibrary(solvarParentDir, workDir)
        scConf.set("spark.yarn.dist.archives", f"{zippedLib}#solvar") # extract as "solvar"
    sc = SparkContext(
            appName=f"Solvar-{ts}",
            master=sparkMaster,
            environment={
                key : val for key,val in os.environ.items()
                if (key.startswith("AWS_") or key.startswith("DAS_CLUSTER_"))
            },
            conf=scConf
        )

    print("Processing Blocks...")

    # create the appropriate spark iterator
    pfnames = sc.parallelize([(thisSolvar.s3Bucket, lp) for lp in lpFiles], numSlices=1000)
    block_pops = pfnames.map(compute_populations).collect()
    block_df = pd.DataFrame(columns=["Block", "Population"], data=block_pops)
    out_fname = "block_populations.csv"
    print("Saving CSV...")
    block_df.to_csv(out_fname, index=False)

    # Run the rewriter logic here
    logging.info("Exiting Rewriter.")


def main(args: argparse.Namespace):
    """rewriter main
    two apps are implemented:
        * one to run on a cluster with the rest of solvar
        * one to run locally (local filesystem)
    """
    main_solvar(args)


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
