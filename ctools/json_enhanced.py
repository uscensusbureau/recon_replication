#!/usr/bin/env python3
# -*- mode: python; -*-
#
"""
json_enhanced.py:
This script is used to get information out of encrypted files
"""

import sys

from os.path import dirname
import os
import json
import socket
import botocore
import botocore.config
from typing import Dict

MY_DIR = dirname( os.path.abspath(__file__ ))
if MY_DIR not in sys.path:
    sys.path.append( MY_DIR )

import boc_kms
import env

BCC_HTTPS_PROXY = 'BCC_HTTPS_PROXY'
BCC_HTTP_PROXY  = 'BCC_HTTP_PROXY'

PARENT_DIR = dirname(dirname( os.path.abspath(__file__)))
if PARENT_DIR not in sys.path:
    sys.path.append( PARENT_DIR )


from ssh_remote import run_command_on_host
import ctools.emr as emr
import ctools.aws as aws

# Location to use for temporary storage on the clusters
TMP_DIR = 'TMP_DIR'


def get_config(variable_name: str, file_name: str):
    if "." in variable_name:
        (name,ext) = variable_name.split(".",1)
        val = get_config(name, file_name)
        return val[ext]

    with open(file_name, 'r') as f:
        json_file = json.loads(f.read())
    for key in json_file:
        if variable_name in json_file[key]:
            return json_file[key]
    raise KeyError(f"{variable_name} not in {file_name}")


def get_secret(variable_name: str, file_name: str):
    if os.path.exists(file_name):
        json_file = boc_kms.get_encrypted_json_file(file_name)
        for key in json_file:
            if variable_name in json_file[key]:
                return json_file[key][variable_name]
        print("",file=sys.stderr)
        print(f"The following secrets are available:",file=sys.stderr)
        for key in json_file:
            for var in json_file[key]:
                print(f" key: {key}, var: {var}")
        print("",file=sys.stderr)
    raise FileNotFoundError("Could not find secrets file")

def get_botocore_proxy_config():
    """Returns a botocre proxy config for AWS"""
    http_proxy=get_config('BCC_HTTPS_PROXY')
    if http_proxy and len(http_proxy) > 0:
        return botocore.config.Config( proxies={'https':get_config(BCC_HTTPS_PROXY).replace("https://",""),
                                                'http':get_config(BCC_HTTP_PROXY)})
    else:
        return botocore.config.Config()

def phost(host: str) -> str:
    """Print host"""
    return host if host else "Head node"


def decode_status(meminfo: str) -> Dict:
    return { line[:line.find(":")] : line[line.find(":")+1:].strip() for line in meminfo.split("\n") }


def get_file_on_host(host: str, path: str, encoding: str = 'utf-8'):
    return run_command_on_host(host,"/bin/cat "+path,encoding=encoding)


def get_instance_type(host: str = None):
    return run_command_on_host(host,"curl -s http://169.254.169.254/latest/meta-data/instance-type")


def get_ipaddr() -> str:
    return socket.gethostbyname(socket.gethostname())


def decode_kb(msg: str) -> str:
    try:
        (numbers,units) = msg.split(" ")
    except ValueError:
        return msg
    numbers = int(numbers)
    if units=='kB':
        numbers //= (1024*1024)
        units = 'GiB'
    return "{} {}".format(numbers,units)


def print_stats_for_host(host: str) -> None:
    meminfo = decode_status(get_file_on_host(host,"/proc/meminfo"))
    status  = decode_status(get_file_on_host(host,"/proc/self/status"))
    cpuinfo = get_file_on_host(host,"/proc/cpuinfo")
    cpus = 0
    total_cores = 0
    for line in cpuinfo.split("\n"):
        if line.startswith("processor"): cpus += 1
        if line.startswith("cpu cores"): total_cores += int(line[line.find(":")+2:])
    itype = get_instance_type(host)

    print("{:34} {:14} Memory: {:8}".format(phost(host),itype,decode_kb(meminfo.get('MemTotal',''))))


def cluster_hostnames(getMaster: bool = True) -> str:
    if getMaster and emr.isMaster():
        yield get_ipaddr()
    for line in run_command_on_host('','yarn node -list',pipeerror=True).split('\n'):
        if "RUNNING" in line:
            host = line[0:line.find(':')]
            yield(host)


def is_jenkins() -> bool:
    """Return true if this is the jenkins host"""
    return aws.get_ipaddr()==get_config('JENKINS_IPADDR')

if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Tools for working with the Census Cluster." )
    parser.add_argument("--file", help="the file to use", type=str, required=True)
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--list",help="List nodes in the cluster.",action='store_true')
    g.add_argument("--check",help="try to log into each host",action='store_true')
    g.add_argument("--get", help="Get a value out of a file")
    g.add_argument("--secret", help="Get a value out of secrets.json.encrypted")
    g.add_argument("--info", help="print info", action='store_true')
    args = parser.parse_args()
    if args.get:
        print(get_config(args.get, args.file))
        exit(0)

    if args.secret:
        print(get_secret(args.secret, args.file))
        exit(0)

    if emr.isMaster():
        print("Running on Master")
    else:
        print("Not running on Master")

    if args.info:
        print("clusterid:",emr.clusterId())
        print("diskEncryptionConfiguration.encryptionEnabled:",emr.encryptionEnabled())

    if args.list:
        print("Nodes in cluster:")
        for host in cluster_hostnames():
            print(host)
        exit(0)

    if args.check:
        for host in cluster_hostnames():
            print(run_command_on_host(host,command='hostname; uptime'))
        exit(0)

    if args.info:
        hosts = list(cluster_hostnames(getMaster=False))

        print("====================")
        print("== CPU AND MEMORY ==")
        print("====================")

        for host in [''] + hosts:
            print_stats_for_host(host)

        print("==================")
        print("== FILE SYSTEMS ==")
        print("==================")

        for host in [''] + hosts:
            print(phost(host ))
            print(run_command_on_host(host,"df -h"))

        print("== HDFS STATUS ==")
        print(run_command_on_host('',"hdfs dfs -df -h"))
