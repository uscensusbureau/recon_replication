#!/usr/bin/env python3
#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)
"""
Code for running ssh on remote hosts.
"""


import os
import sys
import signal
import subprocess
from os.path import dirname,abspath

SSH_OPTIONS = ['-o','StrictHostKeyChecking=no','-o','UserKnownHostsFile=/dev/null', '-o', 'LogLevel=ERROR']
def run_command_on_host(host: str, command: str, encoding: str = 'utf-8', pipeerror: bool = None, debug: bool = False):
    """Just run a command and return the results."""
    extra = []
    if debug:
        extra = ['-v']
    error_out = subprocess.PIPE if pipeerror else sys.stdout
    if host=="" or host is None:
        p = subprocess.run(command,shell=True,stdout=subprocess.PIPE,stderr=error_out, encoding=encoding)
    else:
        p = subprocess.run(["ssh"] + SSH_OPTIONS + extra + [host,command],stdout=subprocess.PIPE,stderr=error_out, encoding=encoding)
    return p.stdout


def process_host(*,cluster: str = None, host: str, password: str = None, debug: bool = False,
                 cmd: str, user: str = None, one_line: bool = False, scp: bool = False, dry_run: bool = False,
                 stdin_file: str = None, term: str = None, outfile=sys.stdout,
                 timeout: int = 360) -> None:
    """Involve running of potentially interactive command.
    @param cmd = command to run on remote system"""

    fd = None

    def handler(signum, frame) -> None:
        print(f"PID{os.getpid()}: alarm! timeout. fd={fd}")
        if fd is not None:
            fd.close()

    signal.signal(signal.SIGALRM, handler)
    signal.alarm(timeout)


    if (stdin_file is not None) and (password is not None):
        raise RuntimeError("Both stdin_file and password cannot be set")

    if ('sudo' in cmd) and ('-S') not in " ".join(cmd):
        print("It is recommended to use -S with sudo command",file=sys.stderr)

    if user:
        host = user+"@"+host

    # this is not an effective way to determine if host in in ite
    #if 'ite' not in host:
    #    print(f"\n=== WILL NOT PROCESS {host} -- not in ITE")
    #    return

    if one_line:
        print(f"{cluster}/{host:16}: ",end='',flush=True,file=outfile)
    else:
        print("\n=== {} (cluster {}) ===".format(host,cluster),file=outfile)

    if scp:
        rcmd = ['scp'] + SSH_OPTIONS + [cmd[0],host+":"+cmd[1]]
        # rcmd = ['scp'] + SSH_OPTIONS + [host + ":" + cmd[0], cmd[1]]
    elif stdin_file=='pty':
        rcmd = ['ssh','-t'] + SSH_OPTIONS + ['-A',host] + cmd
    else:
        rcmd = ['ssh'] + SSH_OPTIONS + ['-A',host] + cmd

    if dry_run:
        print("DRY RUN: " + " ".join(rcmd),file=outfile)
        return

    if not one_line:
        print("$ " + " ".join(rcmd), file=outfile)

    if stdin_file=='pty' or (password is not None):
        if debug:
            print("Creating a pty for the ssh process.",file=sys.stderr)
        pid, fd = os.forkpty()
        if pid==0: # child
            os.execvp('ssh',rcmd)
        while True:
            try:
                data = os.read(fd, 1024)
                recv = data.decode('utf-8',errors='ignore')
                show = recv
                if password:
                    show = recv.replace(password,"<<PASSWORD>>")
                show = show.rstrip().replace("\r","")
                print(show,file=outfile,flush=True)
                if (b"password:" in data) or (b"password for" in data):
                    print("<<< SENDING PASSWORD",file=outfile)
                    os.write(fd,password.encode('utf-8') + b"\n")
                if len(data)==0:
                    break
                if (term is not None) and (term in recv):
                    break
            except OSError as e:
                break           # file handle got closed
    else:
        if stdin_file is not None:
            subprocess.check_call(rcmd, stdin=open(stdin_file))
        else:
            subprocess.check_call(rcmd)

    if not one_line:
        print("================ END ================",file=outfile)
