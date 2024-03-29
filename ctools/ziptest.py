#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)

import zipfile

class VerboseFile:
    """Like a real file, but prints what's happening."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.fp  = open(name, "rb")

    def __repr__(self) -> str:
        return "VerboseFile<name:{} fp:{}>".format(self.name, self.fp)

    def __enter__(self):
        return self

    def __exit__(self, a, b, c) -> None:
        return

    def read(self, len: int = -1) -> int:
        print("read({})".format(len))
        return self.fp.read(len)

    def seek(self, offset: int, whence: int) -> bytes:
        print("will seek {},{}".format(offset, whence))
        try:
            r = self.fp.seek(offset, whence)
        except IOError as e:
            print("Exception: ", e)
        print("seek({},{})={}".format(offset, whence, r))
        return r

    def tell(self) -> int:
        r = self.fp.tell()
        print("tell()={}".format(r))
        return r

    def write(self) -> None:
        raise RuntimeError("Write not supported")

    def flush(self) -> None:
        raise RuntimeError("Flush not supported")

    def close(self) -> None:
        print("closed")
        return self.fp.close()


# Try with
# aws s3api get-object --bucket my_s3_bucket --key s3_folder/file.txt --range bytes=0-1000000 tmp_file.txt && head tmp_file.txt

if __name__=="__main__":
    import argparse
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("zipfile", help="A file that you want to test")
    args = parser.parse_args()

    with VerboseFile(args.zipfile) as vf:
        with zipfile.ZipFile(vf, mode='r', allowZip64=True) as zf:
            print("name list:", zf.namelist())
