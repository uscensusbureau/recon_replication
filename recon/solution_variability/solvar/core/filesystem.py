"""
Class Filesystem
"""

from solvar.core.base import Solvar
from solvar.core.utils import *
from abc import ABC, abstractmethod

try:
    import hdfs # type: ignore
    import getpass
except:
    pass #Do not err here if we're using S3

class Filesystem(Solvar, ABC):
    """ Base class for AWS """

    def __init__(self, logger:Optional[logging.Logger]=None) -> None:
        if (logger):
            self.logger = logger #overwrite default
    
    def loadInputs(self) -> List[PurePath]:
        return [file for file in self.walkDirectory(self.input)
                if (file.name.endswith('.lp.gz') or file.name.endswith('.lp'))]

    @classmethod
    def walkLocalDirectory(cls, localPath:Path) -> Iterator[Path]:
        """ path.iterdir, but yields a similar output to self.walkDirectory (i.e. flat strucutre) """
        for path in localPath.absolute().iterdir():
            if (path.is_dir()):
                yield from cls.walkLocalDirectory(path)
                continue
            else:
                yield path

    def fetchLogs(self, lpFiles:List[PurePath]) -> None:
        """ Fetch the logs from the remote filesystem to local """
        for lp in lpFiles:
            geoid = extract_geoid(lp)
            logFilename = f"{geoid}.log"
            grbLogFilename = f"{geoid}_gurobi.log"
            lpDir = self.cleanPath(lp.parent)
            remoteLogFile = self.output.joinpath(lpDir).joinpath(logFilename)
            remoteGrbLogFile = self.output.joinpath(lpDir).joinpath(grbLogFilename)
            localLogDir = self.workDir.joinpath(lpDir)
            if (not localLogDir.is_dir()):
                svMkDir(localLogDir)
            if (self.is_file(remoteLogFile)):
                self.downloadFile(remoteLogFile, localFilepath=localLogDir.joinpath(logFilename))
            else:
                self.warning(f"Failed to fetch <{remoteLogFile}>; it is missing.")
            if (self.is_file(remoteGrbLogFile)):
                self.downloadFile(remoteGrbLogFile, localFilepath=localLogDir.joinpath(grbLogFilename))
            else:
                self.warning(f"Failed to fetch <{remoteGrbLogFile}>; it is missing.")

    @abstractmethod
    def uploadFile(self, localFilepath:Path, remoteFilepath:Optional[PurePath]=None) -> None:
        pass

    @abstractmethod
    def downloadFile(self, remoteFilepath:PurePath, localFilepath:Optional[Path]=None) -> None:
        pass

    @abstractmethod
    def walkDirectory(self, remotePath:PurePath) -> Iterator[PurePath]:
        pass

    @abstractmethod
    def mkdir(self, xDir:PurePath) -> None:
        pass

    @abstractmethod
    def cleanPath(self, xPath:PurePath) -> PurePath:
        pass

    @abstractmethod
    def is_file(self, filePath:PurePath) -> bool:
        pass

class HDFS(Filesystem):
    """ Class for HDFS operations """

    @svCatch("Failed to instantiate an HDFS object.")
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        #hdfs logging is unneeded.
        try:
            logging.getLogger('hdfs').setLevel(logging.WARN)
            self.__hdfs = hdfs.client.Client(
                f"http://{os.getenv('DAS_CLUSTER_HOST_IP')}:{os.getenv('DAS_CLUSTER_HOST_HDFS_PORT')}"
            )
        except Exception as exc:
            svExit("Filesystem is set to <hdfs> but the hdfs module is not available.",exc=exc)
        # Sanity check -- this will throw an exception if something went wrong
        self.__hdfs.list("")
        self.__hdfs_root = PurePath(f"/user/{getpass.getuser()}")

    @property
    def hdfs(self) -> "hdfs.client.Client": # Use literal to avoid erring when on s3-only systems
        """ Mainly for cosmetic reasons """
        return self.__hdfs

    @property
    def hdfsRoot(self) -> PurePath:
        """ Mainly for cosmetic reasons """
        return self.__hdfs_root

    @svCatch("Failed to walk HDFS directory.")
    def walkDirectory(self, remotePath:PurePath) -> Iterator[PurePath]:
        """ hdfs.walk, but yields a similar output to s3.walkDirectory (i.e. flat strucutre) """
        if (self.hdfs.status(remotePath.as_posix(), strict=False) is None): #Path does not exist
            return []
        for dirname, dirs, files in self.hdfs.walk(remotePath.as_posix()):
            for file in files:
                yield PurePath(dirname).joinpath(file)

    @svCatch("Failed to upload file to HDFS.")
    def uploadFile(self, localFilepath:Path, remoteFilepath:Optional[PurePath]=None) -> None:
        """ upload <localFilepath> to HDFS at the path </user/${USER}/remoteFilepath> """
        hdfsPath = remoteFilepath.as_posix() if (remoteFilepath) else localFilepath.name
        self.hdfs.upload(hdfsPath, localFilepath.as_posix(), overwrite=True)
        self.debug(f"Uploaded {localFilepath} to HDFS.")

    @svCatch("Failed to download file from HDFS.")
    def downloadFile(self, remoteFilepath:PurePath, localFilepath:Optional[Path]=None) -> None:
        """ downloads </user/${USER}/remoteFilepath> to <localFilepath> """
        if (not localFilepath):
            localPath = remoteFilepath.name
        elif (localFilepath.is_dir()): #Should have been a file, but let's be nice
            localPath = localFilepath.joinpath(remoteFilepath.name).as_posix()
        else:
            localPath = localFilepath.as_posix()
        self.hdfs.download(remoteFilepath.as_posix(), localPath)
        self.debug(f"Downloaded {remoteFilepath} from HDFS.")

    @svCatch("Failed to makedirs on HDFS.")
    def mkdir(self, xDir:PurePath) -> None:
        self.hdfs.makedirs(xDir.as_posix())

    @svCatch("Failed to clean the HDFS path.")
    def cleanPath(self, xPath:PurePath) -> PurePath:
        return xPath.relative_to(self.hdfsRoot).relative_to(self.input)

    @svCatch("Failed to check whether an HDFS file exists.")
    def is_file(self, filePath:PurePath) -> bool:
        return (self.hdfs.status(filePath.as_posix(), strict=False) is not None)
