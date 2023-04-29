"""
Model Rewriter Runner
"""
import glob

from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from solvar.core.base import Solvar
from solvar.core.utils import *
from solvar.core.filesystem import Filesystem, HDFS
from solvar.core.s3_utils import S3
from solvar.rewriter.blockRewriter import BlockNoPctRewriter, BlockIneqPctRewriter

from pathlib import PurePath
from typing import List
import socket
import tempfile


class RewriteRunner(Solvar):
    """
    Application Context to Use the Block Level Rewrite Functionality

    This is made to match the interface of RunComputeRadiiByCounty, to make
    the rewrite functionality interface similarly for the solvar application.
    """
    def __init__(self, lpFiles:List[PurePath], rewriter) -> None:
        self.lpFiles = lpFiles
        self.rewriter = rewriter

    def process_lp(self, lp:PurePath, iLp:int=0, isMultiThreaded:bool=False) -> Dict:
        """process lp (rewrite model to fs output)

        TODO: NOTE: there is a lot of code duplication with RunComputeRadiiByCounty -- should we restructure?
        """
        if (isMultiThreaded):
            self.propagateSettings(self) # Enforce unpickling of settings

        # get an identifier for the model (geoid in tract case)
        elems = lp.stem.split('_')
        modelid = elems[1] if len(elems) > 1 else elems[0]

        # If it's a worker node that is sandboxed in a container, no one cares about the path, let's just
        # use relative paths and the container will get nuked anyway. However, in single-threaded mode,
        # or when sparkMaster=local[*], some extra care should be taken with the paths.
        isSandboxed = isMultiThreaded and (self.sparkMaster!='local[*]')
        if (not isSandboxed):
            tempDir = tempfile.TemporaryDirectory(prefix=f"solvar-")
        parentPath =  Path(tempDir.name) if (not isSandboxed) else Path()
        outputFileBase = parentPath.joinpath(f"{modelid}")

        # prepare the logs
        logFilename = f"{modelid}.log"
        grbLogFilename = f"{modelid}_gurobi.log"
        logFile = parentPath.joinpath(logFilename)
        grbLogFile = parentPath.joinpath(grbLogFilename)

        logLevel = logging.DEBUG if (self.debugLog) else logging.INFO
        myLoggerHandler = logging.FileHandler(logFile, mode='a')
        myLoggerHandler.setFormatter(logging.Formatter(
            '%(asctime)s: (%(levelname)s)~  %(message)s',
            datefmt='%I:%M:%S %p'))

        # For yarn -- Add modelid because they interleave
        console = logging.StreamHandler()
        console.setFormatter(logging.Formatter(
            '%(asctime)s: (%(levelname)s-{})~  %(message)s'.format(modelid),
            datefmt='%I:%M:%S %p'))

        self.logger = logging.getLogger(f"logger_{iLp}")
        self.logger.addHandler(myLoggerHandler)
        self.logger.addHandler(console)
        self.logger.setLevel(logLevel)

        self.info(f"<{lp}> executing on <{socket.gethostbyname(socket.gethostname())}>.")

        # Download the lp file
        fs = S3(self.s3Bucket, logger=self.logger) if (self.filesystem=='s3') else HDFS(logger=self.logger)
        localLp = parentPath.joinpath(lp.name)
        fs.downloadFile(lp,localFilepath=localLp)

        # Unzip if needed
        if (localLp.suffix=='.gz'):
            localLp = svUnzip(localLp)

        # do the rewrite
        self.rewriter.rewrite_lp_files(None,
                                  outputFileBase,
                                  [Path(localLp)],
                                  None)

        # upload lp files
        lpDir = fs.cleanPath(lp.parent)
        remoteLogDir = self.output.joinpath(lpDir)
        for file in glob.glob(str(outputFileBase / "*.lp")):
            fs.uploadFile(Path(file), remoteFilepath=remoteLogDir.joinpath(Path(file).name))

        # Upload log files
        fs.uploadFile(logFile, remoteFilepath=remoteLogDir.joinpath(logFilename))
        if (grbLogFile.is_file()):
            fs.uploadFile(grbLogFile, remoteFilepath=remoteLogDir.joinpath(grbLogFilename))
        else:
            self.warning(f"Cannot upload Gurobi log file. {grbLogFile} is missing.")
        if (not isMultiThreaded): # Copy the log file over
            localLogDir = self.workDir.joinpath(lpDir)
            if (not localLogDir.is_dir()): # Safe because this is single-threaded
                svMkDir(localLogDir)
            svCopyFile(logFile, localLogDir)
            if (grbLogFile.is_file()):
                svCopyFile(grbLogFile, localLogDir)
        if (not isSandboxed):
            tempDir.cleanup()
        return {}

    def spark_process_lp(self, enumedLp: Tuple[int, PurePath]) -> Dict:
        """
        wrapper of process_lp for spark (with better error handling)
        """
        iLp, lp = enumedLp
        try:
            return self.process_lp(lp, iLp=iLp, isMultiThreaded=True)
        except Exception as exc:
            try:
                runIp = socket.gethostbyname(socket.gethostname())
                containerID = os.getenv('CONTAINER_ID', "${CONTAINER_ID}")
                errorMessage = (
                    f"Running on <{runIp}>.\n"
                    f"\t{'v-'*30}v\n"
                    f"{traceback.format_exc()}\n\n"
                    f"You can: <ssh {runIp}> and check "
                    f"<~/hadoop/logs/userLogs/{self.sparkAppId}/{containerID}/stderr> for the complete log.\n"
                    f"\t{'^-'*30}^\n"
                )
                return {"error" : errorMessage}
            except Exception as exc2:
                self.error("Failed to get the information to send back.",exc=exc2)
                return {"error" : "UNKNOWN"}

    def runRewriteInParallel(self, ts: str) -> bool:
        """
        spark (parallel) method to rewrite models
        """
        #pyspark logging is intense. It smothers our logging.
        logging.getLogger('py4j').setLevel(logging.CRITICAL)
        scConf = SparkConf()
        #scConf.set("spark.yarn.nodemanager.resource.memory-mb", "700000")
        #scConf.set("spark.yarn.scheduler.maximum-allocation-mb", "700000")
        #scConf.set("spark.yarn.scheduler.minimum-allocation-mb", "200000")
        #scConf.set("spark.yarn.scheduler.maximum-allocation-vcores", "96")
        #scConf.set("spark.yarn.scheduler.minimum-allocation-vcores", "48")
        scConf.set("spark.yarn.nodemanager.resource.cpu-vcores", "3")
        scConf.set("spark.storage.memoryFraction", "0.7")
        scConf.set("spark.executor.instances", "17")
        scConf.set("spark.executor.cores", "2")
        scConf.set("spark.executor.memory", "245g")
        scConf.set("spark.yarn.executor.memoryOverhead", "40g")
        scConf.set("spark.default.parallelism", "40")
        scConf.set("spark.driver.maxResultSize", "200g")
        print(scConf.getAll())
        # due to error
        #scConf.set("spark.shuffle.service.enabled", "true")
        #scConf.set("spark.driver.maxResultSize", "32g")
        #scConf.set("spark.executor.memoryOverhead", "8g")
        #scConf.set("spark.dynamicAllocation.enabled", "true")
        #scConf.set("spark.dynamicAllocation.initialExecutors", "20")
        #scConf.set("spark.dynamicAllocation.minExecutors", "1")
        #scConf.set("spark.dynamicAllocation.maxExecutors", "100")
        if (self.sparkMaster=='yarn'): # We need to take the rest of the library to the container
            zippedLib = zipLibrary(self.solvarParentDir, self.workDir)
            scConf.set("spark.yarn.dist.archives", f"{zippedLib}#solvar") # extract as "solvar"
        sc = SparkContext(
            appName=f"Solvar-{ts}",
            master=self.sparkMaster,
            environment={
                key : val for key,val in os.environ.items()
                if (key.startswith("AWS_") or key.startswith("DAS_CLUSTER_") or key.startswith("DAS_") or key.startswith("REQUESTS_"))
            },
            conf=scConf
        )
        self.sparkAppId = sc.applicationId
        self.info(f"Spark Job ID: {self.sparkAppId}")
        logging.info(f"Running Parallel Rewriter...")
        par_lps = sc.parallelize(enumerate(self.lpFiles), numSlices=10000)
        retIvs = par_lps.map(self.spark_process_lp).collect()
        isSuccess = True
        self.ivs = []
        for lp, retIv in zip(self.lpFiles,retIvs):
            if ("error" in retIv):
                self.error(f"Failed to process <{lp}>.\n\tERROR: {retIv['error']}")
                isSuccess = False
            else:
                self.ivs.append(retIv)
        return isSuccess

    def runRewriteSingledThreaded(self) -> None:
        """
        serial method to rewrite models
        """
        logging.info(f"Running Single Threaded Rewriter...")
        for idx, lp in enumerate(self.lpFiles):
            self.process_lp(lp, idx)
