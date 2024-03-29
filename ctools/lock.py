#!/usr/bin/env python3
#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)
"""
Tool for implementing locking of script
To assure that only one copy of a script is running, insert this in __main__:
    import ctools.lock
    ctools.lock.lock_script()

If a copy of the script is already running, a RuntimeError will be generated.
"""


import sys
import os
import fcntl
import logging


def lock_script(lockfile: str = sys.argv[0]) -> int:
    """Lock the script so that only one copy can run at once"""
    try:
        fd = os.open(lockfile, os.O_RDONLY)
    except FileNotFoundError as f:
        raise FileNotFoundError("Could not find script at {}".format(lockfile))

    if fd>0:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)  # non-blocking
        except IOError:
            raise RuntimeError(f"Could not acquire lock on {lockfile}")
        return fd
    raise RuntimeError(f"Could not open {lockfile}")
