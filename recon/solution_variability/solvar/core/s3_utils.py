"""
Functions to use S3.

First, you need an S3 object:
    from s3_utils.py import S3
    bucketName = "uscb-decennial-ite-das" #This is the bucket they use. Only the bucket name is needed here.
    myS3 = S3(bucketName)

Then, you can do the following:
    # upload file
    myS3.uploadFile("/home/$USER/path/to/file") #This will go to: {bucketName}/file
    myS3.uploadFile("/home/$USER/path/to/file","org/2/mydir/file2") #Recommended. This will go to: {bucketName}/there/instead/file2
    myS3.uploadFile("/home/$USER/path/to/file","org/2/mydir") #BAD! will become: {bucketName}/org/2/mydir [S3 is flat!]

    #download file
    myS3.downloadFile("file") #Will fetch it to: ./file
    myS3.downloadFile("file","/path/to/file2") #Recommended. Will have "file" at /path/to/file2
    myS3.downloadFile("file","/path/to") #Will go to: /path/to/file [Ambiguous, but tolerable]

    # delete file
    myS3.deleteFile("myDir/file") #Will delete {bucket}/myDir/file
    # NOTE: this does not throw exception if file does not exist

    # download directory
    myS3.downloadDirectory("myDir") #Will download the whole tree of {bucket}/myDir/* to ./myDir/*
    myS3.downloadDirectory("myDir/myDir2","/home/$USER/myDir3") #Recommended.
    # ^ Will download the whole tree as is (with hierarchy) --> /home/$USER/myDir3/myDir/mDir2
    
    # upload directory
    myS3.uploadDirectory("/path/to/myDir") #Will upload /path/to/myDir/* --> {bucket}/myDir/*
    myS3.uploadDirectory("/path/to/myDir","newpath/there") #Recommended. --> {bucket}/newpath/there/myDir/*

    # delete directory
    myS3.deleteDirectory("path/to/myDir") # deletes {bucket}/path/to/myDir
    # Does not throw exceptions if files do not exist

"""

import os, urllib.request, traceback
import boto3, botocore
try:
    from mypy_boto3_s3.client import S3Client
except:
    pass # Do not err on systems without mypy
from solvar.core.base import Solvar
from solvar.core.utils import *
from solvar.core.filesystem import Filesystem


class AWS(Solvar):
    """ Base class for AWS """

    def __init__(self) -> None:
        self.__instanceId = None
        self.__instanceIp = None
        #botocore logging is intense. It smothers our logging.
        logging.getLogger('botocore').setLevel(logging.CRITICAL)

    @property
    def instanceId(self) -> str:
        if (not self.__instanceId):
            instanceId = self.getEC2MetaData('instance-id')
            self.__instanceId = instanceId
        return instanceId

    @property
    def instanceIp(self) -> str:
        if (not self.__instanceIp):
            instanceIp = self.getEC2MetaData('local-ipv4')
            self.__instanceIp = instanceIp
        return instanceIp

    @svCatch("Failed to get EC2 metadata.")
    @staticmethod
    def getEC2MetaData(key:str) -> str:
        """ get MetaData from an EC2 instance """
        return urllib.request.urlopen(f"http://169.254.169.254/latest/meta-data/{key}").read().decode()


class S3(AWS, Filesystem):
    """ Class for S3 operations """

    @svCatch("Failed to instantiate an S3 object.")
    def __init__(self, bucket:str, **kwargs) -> None:
        AWS.__init__(self)
        Filesystem.__init__(self,**kwargs)
        # Create an s3 client
        self.__s3 = boto3.client('s3')
        self.__bucket = bucket

        # Sanity check -- this will throw an exception if it cannot reach the bucket.
        # Non-inclusive list of reasons:
        #   - Absent AWS credentials
        #   - No AWS permissions to access that bucket
        #   - Not found, so wrong bucket name.
        self.__s3.head_bucket(Bucket=self.__bucket)

    @property
    def s3(self) -> "S3Client": # Use literal
        """ Mainly for cosmetic reasons """
        return self.__s3

    @property
    def bucket(self) -> str:
        """ Mainly for cosmetic reasons """
        return self.__bucket

    @svCatch("Failed to upload file to S3.")
    def uploadFile(self, localFilepath:Path, remoteFilepath:Optional[PurePath]=None) -> None:
        """ upload <localFilepath> to the S3 bucket to the path <bucket/remoteFilepath> """
        s3Path = remoteFilepath.as_posix() if (remoteFilepath) else localFilepath.name
        self.s3.upload_file(localFilepath.as_posix(), self.bucket, s3Path)
        self.debug(f"Uploaded {localFilepath} to S3.")

    @svCatch("Failed to download file from S3.")
    def downloadFile(self, remoteFilepath:PurePath, localFilepath:Optional[Path]=None) -> None:
        """ downloads <bucket/remoteFilepath> to <localFilepath> """
        if (not localFilepath):
            localPath = remoteFilepath.name
        elif (localFilepath.is_dir()): #Should have been a file, but let's be nice
            localPath = localFilepath.joinpath(remoteFilepath.name).as_posix()
        else:
            localPath = localFilepath.as_posix()
        self.s3.download_file(Bucket=self.bucket, Key=remoteFilepath.as_posix(), 
                            Filename=localPath)
        self.debug(f"Downloaded {remoteFilepath} from S3.")

    @svCatch("Failed to delete file from S3.")
    def deleteFile(self, remoteFilepath:PurePath) -> None:
        """ deletes <bucket/remoteFilepath> from S3 """
        # !!: This does not throw an exception if the file does not exist. 
        # Maybe add `isFile` and issue a warning/error? -- this is subjective
        self.s3.delete_object(Bucket=self.bucket, Key=remoteFilepath.as_posix())
        self.debug(f"Deleted {remoteFilepath} from S3.")

    def walkDirectory(self, remotePath:PurePath) -> Iterator[PurePath]:
        """ this yields all the objects (remoteFilePaths) in <remotePath> """
        # Note that this would work for any <prefix>, not necessarily _directory_. 
        # Actually, S3 does not have directories. What we think of as a POSIX directory, S3 thinks
        # of as objects sharing the same prefix, that happens to have forward slashes.

        # Get a paginator (instead of looping using termination tokens ourselves in case >1000 files)
        paginator = self.s3.get_paginator('list_objects_v2')
        # Use the paginator to fetch pages containing the list of all files
        pages = paginator.paginate(Bucket=self.bucket, Prefix=(remotePath.as_posix() + '/')) #1000 is the max anyway
        for page in pages:
            if ('Contents' not in page):
                continue
            for content in page['Contents']:
                yield PurePath(content['Key'])

    def downloadDirectory(self, remotePath:PurePath, localPath:Path=Path()) -> None:
        """ downloads <bucket/remotePath> to <localPath/> """
        dirsCreated = set()
        for path in self.walkDirectory(remotePath):
            dirname = path.parent.as_posix()
            if (dirname not in dirsCreated):
                svMkDir(localPath.joinpath(dirname))
                dirsCreated.add(dirname)

            self.downloadFile(path, localPath.joinpath(dirname))

    def deleteDirectory(self, remotePath:PurePath) -> None:
        """ deletes <bucket/remotePath> from S3 """
        # !!: This does not throw an exception if anything does not exist.
        for path in self.walkDirectory(remotePath):
            self.deleteFile(path)

    def syncDirectory(self, localPath:Path, remotePath:Optional[PurePath]=None) -> None:
        """ Sync the file(s) between the two paths """
        # Sync should not be part of a workflow/tool I guess. This is more involved, especially given the `--delete` concept
        # so I will leave this not implemented until we decide it makes sense to implement it.
        raise NotImplementedError

    def mkdir(self, xDir:PurePath) -> None:
        pass # Directories are fake on S3 anyway

    @svCatch("Failed to clean the S3 path.")
    def cleanPath(self, xPath:PurePath) -> PurePath:
        return xPath.relative_to(self.input)

    @svCatch("Failed to check whether an S3 file exists.")
    def is_file(self, filePath:PurePath) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=filePath.as_posix())
            return True
        except botocore.errorfactory.ClientError:
            return False
