"""
Core base classes for solvar -- Mainly the definition of Solvar meta class
"""
from __future__ import annotations
from solvar.core.utils import *
from solvar.config.constants import Constants
from solvar.config.config import Config
import argparse, copy

SolvarSettings = TypedDict('SolvarSettings',
    {
        # Arguments
        'output' : PurePath,
        'debugLog' : bool,
        'input' : PurePath,
        'computeDemoIV': bool,
        'computeAgeIV': bool,
        
        # Core
        'solvarParentDir' : Path,
        'workDir' : Path,
        'logger' : logging.Logger,

        # Constants
        'S3_ROOT' : PurePath,
        'S3_SOLVAR_PATH' : PurePath,
        'MIP_GAP' : float,
        'THREADS' : int,
        'TIMEOUT_RETURN_VALUE' : str,
        'WRITE_VARIABILITY_LPS' : bool,
        'SF1_2010_MAX_AGE' : int,
        'VAR_PLACEHOLDER' : str,
        'RACE_COMPONENTS' : List[str],
        'VAR_COMPONENTS' : List[str],
        'DEFAULT_SCHEMA' : List[str],
        'AGE_SCHEMA' : List[str],
        'DEMO_SCHEMA' : List[str],
        'VALID_SCHEMAS' : Tuple[None, List[str], List[str], List[str], List[str], List[str]],
        'DEFAULT_SCHEMA_IMPRECISE_AGE_BUCKETS' :  List[str],
        'AGE_SCHEMA_IMPRECISE_AGE_BUCKETS' :  List[str],
        'AGE_BUCKETS' :  List[int],
        'CODE2ST' : Dict[str,str],
        'ST2CODE' : Dict[str,str],

        # Config.ini
        'doParallelize' : bool,
        'sparkMaster' : str,
        'filesystem' : str,
        's3Bucket' : str,
        'useImpreciseAgeBuckets' : bool,
        'scaleByPopulation': bool,
        'gurobiTimeout': float
    }, total=False)

class Solvar:
    """
    All classes should inherit from this one. 
    SHOULD NOT inherit from ABCMeta because ABCMeta is unserializable (for pySpark)
    """

    #This will carry all settings -- initial values will be overwritten in runtime
    __settings : SolvarSettings = {}

    def __init__(self, args:argparse.Namespace, solvarParentDir:Path, workDir:Path, configFile:Path) -> None:
        # Load configuration
        config = Config(configFile)

        self.__settings.update(SolvarSettings(
            # Bash args
            output = PurePath(args.output),
            debugLog = args.debug,
            input = PurePath(args.input),
            computeDemoIV = args.demo,
            computeAgeIV = args.age,

            # Core
            solvarParentDir = solvarParentDir,
            workDir = workDir,
            logger = logging.getLogger(), #This is the 'root' logger,

            # constants
            S3_ROOT = Constants.S3_ROOT,
            S3_SOLVAR_PATH = Constants.S3_SOLVAR_PATH,
            MIP_GAP = Constants.MIP_GAP,
            THREADS = Constants.THREADS,
            TIMEOUT_RETURN_VALUE = Constants.TIMEOUT_RETURN_VALUE,
            WRITE_VARIABILITY_LPS = Constants.WRITE_VARIABILITY_LPS,
            SF1_2010_MAX_AGE = Constants.SF1_2010_MAX_AGE,
            VAR_PLACEHOLDER = Constants.VAR_PLACEHOLDER,
            RACE_COMPONENTS = Constants.RACE_COMPONENTS,
            VAR_COMPONENTS = Constants.VAR_COMPONENTS,
            DEFAULT_SCHEMA = Constants.DEFAULT_SCHEMA,
            AGE_SCHEMA = Constants.AGE_SCHEMA,
            DEMO_SCHEMA = Constants.DEMO_SCHEMA,
            VALID_SCHEMAS = Constants.VALID_SCHEMAS,
            DEFAULT_SCHEMA_IMPRECISE_AGE_BUCKETS = Constants.DEFAULT_SCHEMA_IMPRECISE_AGE_BUCKETS,
            AGE_SCHEMA_IMPRECISE_AGE_BUCKETS = Constants.AGE_SCHEMA_IMPRECISE_AGE_BUCKETS,
            AGE_BUCKETS = Constants.AGE_BUCKETS,
            CODE2ST = Constants.CODE2ST,
            ST2CODE = Constants.ST2CODE,

            # Configuration
            doParallelize = config.loadBool('doParallelize'),
            sparkMaster = config.loadStr('sparkMaster'),
            filesystem = config.loadStr('filesystem'),
            s3Bucket = config.loadStr('s3Bucket'),
            useImpreciseAgeBuckets = config.loadBool('useImpreciseAgeBuckets'),
            scaleByPopulation = config.loadBool('scaleByPopulation'),
            gurobiTimeout = config.loadFloat('gurobiTimeout')
        ))

        # Warn about foreign options in config.ini
        config.warnAboutForeignOptions()

    def __getstate__(self) -> Tuple[SolvarSettings,Dict]:
        """ For serialization, we just need the grandparent's __settings and the grandchild's attributes """
        del self.__settings['logger'] # don't serialize that
        return (self.__settings.copy(), self.__dict__.copy())
    
    def __setstate__(self, stateDicts:Tuple[SolvarSettings,Dict]) -> None:
        """ Recover the state when being deserialized """
        self.__settings = stateDicts[0]
        self.__settings['logger'] = logging.getLogger()
        self.__dict__.update(stateDicts[1])
    
    @classmethod
    def propagateSettings(cls, instantiatorObj:Solvar) -> None:
        """ manual propagatation of settings -- for the sake of full serialization """
        cls.__settings.update(instantiatorObj.__settings) # type: ignore ## https://github.com/python/mypy/issues/6462

    def info(self,message:str) -> None:
        self.logger.info(message)

    def logErrWarn(self,loggerFn:Callable[[str],None],message:str,exc:Exception=None) -> None:
        if (exc):
            message += f"\n{formatExc(exc)}."
        loggerFn(message)
        if (exc):
            logging.debug(traceback.format_exc())

    def warning(self,message:str,exc:Exception=None) -> None:
        self.logErrWarn(self.logger.warning, message, exc=exc)

    def error(self,message:str,exc:Exception=None) -> None:
        self.logErrWarn(self.logger.error, message, exc=exc)

    def debug(self,message:str) -> None:
        self.logger.debug(message)

    def dumpSettings(self) -> None:
        """ This is a useful debug method """
        self.debug(f"<{self.__class__.__name__}>: {self.__settings}")

    @property
    def output(self) -> PurePath:
        return self.__settings['output']

    @property
    def debugLog(self) -> bool:
        return self.__settings['debugLog']

    @property
    def input(self) -> PurePath:
        return self.__settings['input']

    @property
    def computeDemoIV(self) -> bool:
        return self.__settings['computeDemoIV']

    @property
    def computeAgeIV(self) -> bool:
        return self.__settings['computeAgeIV']

    @property
    def solvarParentDir(self) -> Path:
        return self.__settings['solvarParentDir']

    @property
    def workDir(self) -> Path:
        return self.__settings['workDir']

    @property
    def logger(self) -> logging.Logger:
        return self.__settings.get('logger', logging.getLogger())

    @logger.setter
    def logger(self, xLogger:logging.Logger) ->  None:
        self.__settings['logger'] = xLogger

    @property
    def S3_ROOT(self) -> PurePath:
        return self.__settings['S3_ROOT']

    @property
    def S3_SOLVAR_PATH(self) -> PurePath:
        return self.__settings['S3_SOLVAR_PATH']

    @property
    def MIP_GAP(self) -> float:
        return self.__settings['MIP_GAP']

    @property
    def THREADS(self) -> int:
        return self.__settings['THREADS']

    @property
    def TIMEOUT_RETURN_VALUE(self) -> str:
        return self.__settings['TIMEOUT_RETURN_VALUE']

    @property
    def WRITE_VARIABILITY_LPS(self) -> bool:
        return self.__settings['WRITE_VARIABILITY_LPS']

    @property
    def SF1_2010_MAX_AGE(self) -> int:
        return self.__settings['SF1_2010_MAX_AGE']

    @property
    def VAR_PLACEHOLDER(self) -> str:
        return self.__settings['VAR_PLACEHOLDER']

    @property
    def RACE_COMPONENTS(self) -> List[str]:
        return self.__settings['RACE_COMPONENTS']

    @property
    def VAR_COMPONENTS(self) -> List[str]:
        return self.__settings['VAR_COMPONENTS']

    @property
    def DEFAULT_SCHEMA(self) -> List[str]:
        return self.__settings['DEFAULT_SCHEMA']

    @property
    def AGE_SCHEMA(self) -> List[str]:
        return self.__settings['AGE_SCHEMA']

    @property
    def DEMO_SCHEMA(self) -> List[str]:
        return self.__settings['DEMO_SCHEMA']

    @property
    def VALID_SCHEMAS(self) -> Tuple[None, List[str], List[str], List[str], List[str], List[str]]:
        return self.__settings['VALID_SCHEMAS']

    @property
    def DEFAULT_SCHEMA_IMPRECISE_AGE_BUCKETS(self) -> List[str]:
        return self.__settings['DEFAULT_SCHEMA_IMPRECISE_AGE_BUCKETS']

    @property
    def AGE_SCHEMA_IMPRECISE_AGE_BUCKETS(self) -> List[str]:
        return self.__settings['AGE_SCHEMA_IMPRECISE_AGE_BUCKETS']

    @property
    def AGE_BUCKETS(self) -> List[int]:
        return self.__settings['AGE_BUCKETS']

    @property
    def CODE2ST(self) -> Dict[str,str]:
        return self.__settings['CODE2ST']

    @property
    def ST2CODE(self) -> Dict[str,str]:
        return self.__settings['ST2CODE']

    @property
    def doParallelize(self) -> bool:
        return self.__settings['doParallelize']

    @property
    def sparkMaster(self) -> str:
        return self.__settings['sparkMaster']

    @property
    def filesystem(self) -> str:
        return self.__settings['filesystem']

    @property
    def s3Bucket(self) -> str:
        return self.__settings['s3Bucket']

    @property
    def useImpreciseAgeBuckets(self) -> bool:
        return self.__settings['useImpreciseAgeBuckets']

    @property
    def scaleByPopulation(self) -> bool:
        return self.__settings['scaleByPopulation']

    @property
    def gurobiTimeout(self) -> float:
        return self.__settings['gurobiTimeout']
