#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)

'''
Combines individual county reconstructed microdata records into a single
national file. Part of the replication archive for The U.S. Census Bureau's Ex
Post Confidentiality Analysis of the 2010 Census Data Publications
(https://github.com/uscensusbureau/recon_replication)
'''

import csv
import dbrecon
import gc
import glob
import logging
import os
import os.path
import subprocess
import sys
import time
import atexit
import multiprocessing
import re
import zipfile
import shutil

MY_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(MY_DIR)
if PARENT_DIR not in sys.path:
    sys.path.append(PARENT_DIR)
import ctools.s3 as s3

import dbrecon
from dbrecon import DBMySQL
from dbrecon import dopen,dmakedirs,dsystem,dpath_exists,GB,REIDENT


DAS_ROOT = os.environ["DAS_S3ROOT"]
TEMP_DIR = "./s6_temp"


def list_s3_files(bucket, reident):
    out = s3.list_objects(bucket+f'/2010-re/{reident}')
    regex = re.compile('synth(.*)csv')
    matches = [x['Key'] for x in out if re.search(regex, x['Key'])]
    return matches


def check_create_path(path):
    p = os.path.dirname(path)
    if not os.path.exists(p):
        os.makedirs(p)
        logging.info(f"Created {path} directory")


def open_sort_append(s3_path, reident, temp_directory, keep_local=False):
    bucket, key = s3.get_bucket_key(s3_path)
    directory = temp_directory
    outfile_name = reident


    cmd_find = f'find {directory} -iname "synth*.csv" | sort -t_ -k3 | '
    cmd_xargs = 'xargs -I{} sed "1d" {} >> '
    cmd_make = f"{directory}/r{outfile_name}.csv"
    cmd_head = f'find {directory} -iname "synth*.csv"' + " | sort -t_ -k3 | head -n 1 | xargs -I{} head {} -n 1 >>" + f'{cmd_make};'
    cmd = cmd_head + cmd_find + cmd_xargs + cmd_make

    if not os.path.exists(cmd_make):
        path_list = list_s3_files(s3_path, reident)
        for path in path_list:
            file = os.path.basename(path)
            out = f"{directory}/{file}"
            check_create_path(out)
            if not os.path.exists(out):
                s3.get_object(bucket, path, out)
        subprocess.run(cmd, shell=True)

    with zipfile.ZipFile(f"{directory}/r{outfile_name}.csv.zip", 'w', zipfile.ZIP_DEFLATED) as zipObj:
        zipObj.write(f"{directory}/r{outfile_name}.csv")
    zipObj.close()
    put_url = f"{s3_path}/2010-re/{reident}/r{outfile_name}.csv.zip"
    s3.put_s3url(put_url, f"{directory}/r{outfile_name}.csv.zip")
    if not keep_local:
        try:
            shutil.rmtree(f'{directory}')
        except shutil.Error as e:
            logging.error("Error: %s : %s" % (directory, e.strerror))


def main(s3_root, reident, temp_dir, keep_local):
    logging.debug(f"s3_root: {s3_root}")
    logging.debug(f"reident: {reident}")
    logging.debug(f"temp_dir: {temp_dir}")
    logging.debug(f"keep_local: {keep_local}")
    open_sort_append(s3_root, reident, temp_dir, keep_local)


if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Create the microdata file from all of the solutions for each tract in a given county.." )
    dbrecon.argparse_add_logging(parser)
    parser.add_argument("-s3", "--s3_root", default=DAS_ROOT, help="s3 root")
    parser.add_argument("-td", "--temp_dir", default=TEMP_DIR, help="directory where s3 files are copied")
    parser.add_argument("-kl", "--keep_local", action='store_false', help="Delete local files after use")
    parser.add_argument("--debug", action='store_true', help="debug mode")

    args       = parser.parse_args()
    config     = dbrecon.setup_logging_and_get_config(args=args,prefix="06rhdf")

    main(args.s3_root,args.reident, args.temp_dir, args.keep_local)
