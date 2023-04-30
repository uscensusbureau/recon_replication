#!/usr/bin/env python3
#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)
"""procfname: Open a process and return a filename. Implemented with pipes."""

from subprocess import Popen, PIPE


def procfname(cmd: str, mode: str = 'r') -> str:
    if 'r' in mode:
        p = Popen(cmd, stdout=PIPE, shell='/bin/sh')
        fd = p.stdout.fileno()
        print("fd=", fd)
        return f"/dev/fd/{fd}"

    if 'w' in mode:
        p = Popen(cmd, stdin=PIPE, shell='/bin/sh')
        fd = p.stdin.fileno()
        print("fd=", fd)
        return f"/dev/fd/{fd}"

    raise ValueError("mode ('{}') must contain a 'r' or a 'w'".format(mode))


if __name__=="__main__":
    # test by opening the calendar program
    name = procfname("cal 2018", "r")
    print("name=", name)
    with open(name, "r") as f:
        for line in f:
            print("line:", line.strip())
