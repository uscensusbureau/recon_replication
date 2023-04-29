"""
Parallelize intrinsic variability on a group of lp files
"""

from solvar.core.utils import *
from solvar.core.base import Solvar
from solvar.solvar.intrinsic_variability_driver import IntrinsicVariability
from solvar.core.filesystem import Filesystem, HDFS
from solvar.core.s3_utils import S3
from solvar.config.solutions_testLPs import solutions_testLPs
import os, glob, socket, csv, tempfile
from pyspark.context import SparkContext
from pyspark.conf import SparkConf

class RunComputeRadiiByCounty(Solvar):
    def __init__(self, lpFiles:List[PurePath]) -> None:
        self.lpFiles = lpFiles

    def process_lp(self, lp:PurePath, iLp:int=0, isMultiThreaded:bool=False) -> Dict:
        if (isMultiThreaded):
            self.propagateSettings(self) # Enforce unpickling of settings

        geoid = extract_geoid(lp)

        # If it's a worker node that is sandboxed in a container, no one cares about the path, let's just
        # use relative paths and the container will get nuked anyway. However, in single-threaded mode,
        # or when sparkMaster=local[*], some extra care should be taken with the paths.
        isSandboxed = isMultiThreaded and (self.sparkMaster!='local[*]')
        if (not isSandboxed):
            tempDir = tempfile.TemporaryDirectory(prefix=f"solvar-")
        parentPath =  Path(tempDir.name) if (not isSandboxed) else Path()

        # write the log files to an accessible place for troubleshooting
        # TODO: add this to Solvar base
        logPath = Path("/mnt/tmp/solvar_gurobi_logs/")
        if not os.path.exists(logPath):
            self.info(f"logging path {logPath} doesn't exist! Creating it...")
            # make the directory
            os.makedirs(logPath)

        outputFileBase = parentPath.joinpath(f"{geoid}")

        # prepare the logs
        logFilename = f"{geoid}.log"

        # add hostname for additional debugging
        grbLogFilename = f"{geoid}_gurobi.log"

        logFile = parentPath.joinpath(logFilename)
        # form the logging filepath
        grbLogFile = logPath.joinpath(grbLogFilename)

        logLevel = logging.DEBUG if (self.debugLog) else logging.INFO
        myLoggerHandler = logging.FileHandler(logFile, mode='a')
        myLoggerHandler.setFormatter(logging.Formatter(
            '%(asctime)s: (%(levelname)s)~  %(message)s',
            datefmt='%I:%M:%S %p'))

        # For yarn -- Add geoid because they interleave
        console = logging.StreamHandler()
        console.setFormatter(logging.Formatter(
            '%(asctime)s: (%(levelname)s-{})~  %(message)s'.format(geoid),
            datefmt='%I:%M:%S %p'))

        self.logger = logging.getLogger(f"logger_{iLp}")
        self.logger.addHandler(myLoggerHandler)
        self.logger.addHandler(console)
        self.logger.setLevel(logLevel)

        self.info(f"<{lp}> executing on <{socket.gethostbyname(socket.gethostname())}>.")

        # upload file to indicate job submitted
        # TODO: more meta data?
        fs = S3(self.s3Bucket, logger=self.logger) if (self.filesystem=='s3') else HDFS(logger=self.logger)
        statusFilename = f"{geoid}.submit"
        statusFile = parentPath.joinpath(statusFilename)
        with open(statusFile, "w") as fp:
            pass
        lpDir = fs.cleanPath(lp.parent)
        remoteLogDir = self.output.joinpath(lpDir)
        fs.uploadFile(statusFile, remoteFilepath=remoteLogDir.joinpath(statusFilename))


        # Download the lp file
        localLp = parentPath.joinpath(lp.name)
        fs.downloadFile(lp,localFilepath=localLp)

        # Unzip if needed
        if (localLp.suffix=='.gz'):
            localLp = svUnzip(localLp)

        # Pre-process the LP file to make it compatible for age binning
        if self.useImpreciseAgeBuckets:
            self.info(f'Pre-processing <{lp}> to implement age binning')
            with svOpenFile(localLp,'r') as f:
                localLp_text = f.read()
            with svOpenFile(localLp,'w') as f:
                f.write(replace_startAge_with_bin(localLp_text, self.AGE_BUCKETS))

        ivObj = IntrinsicVariability(logger=self.logger)
        iv = ivObj.getEstimates(localLp, outputFileBase, grbLogFile)
        iv['Tract'] = geoid #augment the data with the tract info

        # Scale by total population
        if self.scaleByPopulation:
            pop = compute_populations(localLp)[0] #get the tract population
            keys = [key for key in iv.keys() if 'IV' in key]
            for key in keys:
                if iv[key] == self.TIMEOUT_RETURN_VALUE:
                    iv[f'Scaled {key}'] = iv[key]
                else:
                    try:
                        iv[f'Scaled {key}'] = iv[key]/(2*pop)
                    except ZeroDivisionError:
                        iv[f'Scaled {key}'] = float('nan')
            iv['Population'] = pop

        # Upload log files
        lpDir = fs.cleanPath(lp.parent)
        remoteLogDir = self.output.joinpath(lpDir)
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
        return iv

    def spark_process_lp(self, enumedLp:Tuple[int,PurePath]) -> Dict:
        """
        In order to have a unique ID for each parallelized process && other options while being
        able to execute the function simply in a single process mode
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

    def runComputeRadiiInParallel(self, ts:str) -> bool:
        #pyspark logging is intense. It smothers our logging.
        logging.getLogger('py4j').setLevel(logging.CRITICAL)
        scConf = SparkConf()
        scConf.set("spark.yarn.nodemanager.resource.memory-mb", "700000")
        scConf.set("spark.yarn.scheduler.maximum-allocation-mb", "700000")
        scConf.set("spark.yarn.scheduler.minimum-allocation-mb", "200000")
        scConf.set("spark.yarn.scheduler.maximum-allocation-vcores", "96")
        scConf.set("spark.yarn.scheduler.minimum-allocation-vcores", "48")
        scConf.set("spark.yarn.nodemanager.resource.cpu-vcores", "3")
        scConf.set("spark.storage.memoryFraction", "0.7")
        scConf.set("spark.executor.instances", "76")
        scConf.set("spark.executor.cores", "5")
        scConf.set("spark.executor.memory", "176g")
        scConf.set("spark.executor.memoryOverhead", "18g")
        scConf.set("spark.default.parallelism", "760")
        scConf.set("spark.driver.maxResultSize", "16g")
        if (self.sparkMaster=='yarn'): # We need to take the rest of the library to the container
            zippedLib = zipLibrary(self.solvarParentDir, self.workDir)
            scConf.set("spark.yarn.dist.archives", f"{zippedLib}#solvar") # extract as "solvar"
        sc = SparkContext(
            appName=f"Solvar-{ts}",
            master=self.sparkMaster,
            environment={
                key : val for key,val in os.environ.items()
                if (key.startswith("AWS_") or key.startswith("DAS_CLUSTER_") or key.startswith("DAS_") or key.startswith("REQUESTS_") or key.startswith("GRB_") or key.startswith("GUROBI_"))
            },
            conf=scConf
        )
        self.sparkAppId = sc.applicationId
        self.info(f"Spark Job ID: {self.sparkAppId}")
        par_lps = sc.parallelize(enumerate(self.lpFiles), numSlices=1000000)

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

    def runComputeRadiiSingleThreaded(self) -> None:
        self.ivs = [self.process_lp(lpFile, iLp=iLp, isMultiThreaded=False) for iLp, lpFile in enumerate(self.lpFiles)]

    def dumpResultsToCsv(self, fs:Filesystem) -> None:
        csvFileName = "ivs.csv"
        localCsvPath = self.workDir.joinpath(csvFileName)

        headers = ["Tract"]
        IVheaders = ["IV"]
        if self.computeDemoIV:
            IVheaders.append("Demo IV")
        if self.computeAgeIV:
            IVheaders.append("Age IV")
        if (self.scaleByPopulation):
            headers += ["Population"] #population is only computed if scaleByPopulation is set
            IVheaders += ["Scaled "+IVheader for IVheader in IVheaders]
        headers += IVheaders

        with svOpenFile(localCsvPath,'w') as fcsv:
            dict_writer = csv.DictWriter(fcsv, headers, lineterminator='\n')
            dict_writer.writeheader()
            dict_writer.writerows(self.ivs)
        self.info(f"CSV output is dumped into <{localCsvPath}>.")
        # Upload the csv file to the filesystem too
        fs.uploadFile(localCsvPath, remoteFilepath=self.output.joinpath(csvFileName))

    def runTests(self) -> List[bool]:
        overallResults = []
        for iv in self.ivs:
            tractID = iv['Tract']
            actualIV = iv['IV']
            try:
                expectedIV = solutions_testLPs[tractID][0 if not self.useImpreciseAgeBuckets else 1]
            except KeyError:
                self.warning(f"Could not find tract with ID {tractID} in the ground truth results `solutions_testLPs`. This tract will be ignored for testing.")
                continue
            if actualIV == expectedIV:
                self.info(f"Intrinsic variability value for tract {tractID} matches its expected value = {expectedIV}")
                overallResults.append(True)
            else:
                self.error(f"Intrinsic variability value for tract {tractID} should be {expectedIV}, instead got {actualIV}")
                overallResults.append(False)
        return overallResults
