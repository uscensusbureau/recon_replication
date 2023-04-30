#!/usr/bin/env python3
#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)
"""
Program to manipulate the database for the database reconstruciton system.
Part of the replication archive for The U.S. Census Bureau's Ex Post
Confidentiality Analysis of the 2010 Census Data Publications
(https://github.com/uscensusbureau/recon_replication)
"""

HELP="""
Manipulate the reconstruction database.

Make sure you update dbrecon_config.json with the relevant mysql and gurobi information.

Typically your sequence of operation is:

$ $(./drbtool.py --env)
$ ./drbtool.py --register db10
$ ./drbtool.py --reident db10 --step1 --step2
$ ./drbtool.py --reident db10 --run

If you run into problems, check your database with:


Use --env to generate bash statements to put them in your environment. This is
easily done with:

$(./drbtool.py --env)

Here are some things to try with spark
$ ./dbrtool.py --reident db10 --spark --rescan
$ ./dbrtool.py --reident db10 --spark --step3 --pop100 '<2500' --executor_memory 1g --executor_memoryOverhead 100g

"""

__version__='1.0.0'


import os
from os.path import abspath,dirname
import sys
import json
import io
import re
import logging
import subprocess
import glob
import multiprocessing
import datetime
from itertools import repeat

try:
    import pymysql
except ImportError:
    print("""
    You are running on a cluster that hasn't been updated to include pymysql.
    Execute this command:

    (cd /mnt/gits/das-vm-config && git checkout master && git pull && bash DAS-Bootstrap3-setup-python.sh)

    """,file=sys.stderr)
    exit(1)

NUMBER_OF_WORKERS_TIMES_40="(Number of Workers) * 40"

DAS_ROOT   = dirname(dirname(dirname(dirname(abspath(__file__)))))
if DAS_ROOT not in sys.path:
    sys.path.append(DAS_ROOT)

BIN_DIR=os.path.join(DAS_ROOT,'bin')
if BIN_DIR not in sys.path:
    sys.path.append(BIN_DIR)

MY_DIR=dirname(abspath(__file__))
if MY_DIR not in sys.path:
    sys.path.append(MY_DIR)

# Import ssh_remote if we can get it. If we cannot, we won't be able to run commands on remote host
try:
    import ctools.ssh_remote as ssh_remote
except ImportError:
    logging.warning("ssh_remote not available")

EMR_CONTROL = 'emr_control.py'


GITHUB = f'https://github.com/uscensusbureau/'
STATS_2010 = 'recon_replication'

import dbrecon

import ctools.s3
import ctools.emr as emr
import ctools.cspark
import ctools.dbfile as dbfile
from ctools.dbfile import DBMySQL
from configparser import ConfigParser

RECON_DIR    = dirname(abspath(__file__))
CONFIG_FILE_NAME = 'dbrecon_config.json'
MYSQL_GUROBI_CONFIG = os.path.join(RECON_DIR, CONFIG_FILE_NAME)
RECON_CONFIG = os.path.join(RECON_DIR, 'config.ini')
RECON_SCHEMA = os.path.join(RECON_DIR, 'schema.sql')

RECON_DIR    = dirname(abspath(__file__))
RECON_CONFIG_FILENAME = 'config.ini'
RECON_CONFIG = os.path.join(RECON_DIR, RECON_CONFIG_FILENAME)
RECON_SCHEMA = os.path.join(RECON_DIR, 'schema.sql')

dbrtool_config = ConfigParser()
dbrtool_config.read(RECON_CONFIG)

# REIDENT separator character.
SEP = dbrtool_config['dbrtool']['SEP']
SEP_TRACTS = dbrtool_config['dbrtool']['SEP_TRACTS']
DAS_ENVVAR = dbrtool_config['dbrtool']['DAS_ENVVAR']

#Allow CORE nodes
USE_CORE = dbrtool_config['dbrtool']['USE_CORE']

# Step1 Parallelism
S1_J1 = dbrtool_config['dbrtool']['S1_J1']

# Step2 Parallelism
S2_J1 = dbrtool_config['dbrtool']['S2_J1']
S2_R = dbrtool_config['dbrtool']['S2_R']

# Step3 parallelism
S3_J1 = dbrtool_config['dbrtool']['S3_J1']
S3_J2 = dbrtool_config['dbrtool']['S3_J2']

# Step4 parallelism
S4_J1 = dbrtool_config['dbrtool']['S4_J1']
S4_J2 = dbrtool_config['dbrtool']['S4_J2']

# Step5 parallelism
S5_J1 = dbrtool_config['dbrtool']['S5_J1']
REFRESH_PARALLELISM = dbrtool_config['dbrtool']['REFRESH_PARALLELISM']

FAST = dbrtool_config['dbrtool']['FAST']

def sf1_zipfile_name(reident, stusab):
    return os.path.join(os.environ['DAS_S3ROOT'],f'2010-re/{reident}/dist/{stusab.lower()}2010.sf1.zip')

def get_mysql_env():
    """Return a dictionary of the MySQL information in the dbrecon_config.json file"""
    # pylint: disable=E0401
    with open(os.path.join( dirname(__file__), MYSQL_GUROBI_CONFIG)) as f:
        return json.loads(f.read())['dbrecon']

def put_mysql_env():
    """Put the Mysql authentication variables into the environment"""
    for (key,val) in get_mysql_env().items():
        os.environ[key] = val

def get_auth():
    """Returns a dbfile authentication token from the config file"""
    if 'MYSQL_PASSWORD' in os.environ:
        env = {'MYSQL_USER':os.environ['MYSQL_USER'],
               'MYSQL_PASSWORD':os.environ['MYSQL_PASSWORD'],
               'MYSQL_HOST':os.environ['MYSQL_HOST'],
               'MYSQL_DATABASE':os.environ['MYSQL_DATABASE']}
    else:
        env = get_mysql_env()
    return dbfile.DBMySQLAuth.FromConfig(env)


def do_mysql():
    """Give the user an interactive MySQL shell."""
    env = get_mysql_env()
    cmd = ['mysql','mysql',
           '--user='+env['MYSQL_USER'], '--password='+env['MYSQL_PASSWORD'],
           '--host='+env['MYSQL_HOST'],env['MYSQL_DATABASE']]
    os.execlp(*cmd)

QUERIES = [
    ('Current Time', 'SELECT now()'),
    ("Completed States:",
     """SELECT stusab,count(*) as remaining from {reident}tracts where sol_end is NULL and pop100>0 group by stusab having remaining=0"""),

    ("Files created and remaining ",
     """SELECT * FROM
              (SELECT COUNT(*) AS lp_created   FROM {reident}tracts   WHERE pop100>0 and lp_end IS NOT NULL ) a

              LEFT JOIN
               (SELECT COUNT(*) AS lp_remaining FROM {reident}tracts  WHERE pop100>0 and lp_end IS NULL ) b
              ON 1=1

              LEFT JOIN
               (SELECT COUNT(*) AS lp_in_process FROM {reident}tracts  WHERE pop100>0 and lp_end IS NULL AND lp_start IS NOT NULL ) c
              ON 1=1

              LEFT JOIN
               (SELECT COUNT(*) AS lp_hostlocked FROM {reident}tracts  WHERE pop100>0 and lp_end IS NULL AND lp_start IS NOT NULL AND hostlock IS NOT NULL ) d
              ON 1=1

              LEFT JOIN
               (select count(*) AS sol_created FROM {reident}tracts WHERE pop100>0 and sol_end IS NOT NULL) e
              ON 1=1

              LEFT JOIN
               (select count(*) AS sol_remaining FROM {reident}tracts WHERE pop100>0 AND sol_end is NULL) f
              ON 1=1

              LEFT JOIN
               (select count(*) AS sol_ready FROM {reident}tracts WHERE pop100>0 AND sol_end is NULL AND lp_end IS NOT NULL) g
              ON 1=1

              LEFT JOIN
               (SELECT COUNT(*) AS csv_completed FROM {reident}tracts  WHERE pop100>0 and csv_end IS NOT NULL ) h
              ON 1=1

              LEFT JOIN
               (SELECT COUNT(*) AS csv_remaining FROM {reident}tracts  WHERE pop100>0 and csv_end IS NULL) i
              ON 1=1

     """),

    ("Number of LP files created in past hour:",
     """select count(*) AS `count`,lp_host FROM {reident}tracts
     WHERE timediff(now(),lp_end) < "01:00:00"
     GROUP BY lp_host ORDER BY 2
     """),

    ("Number of SOL files created in past hour:",
     """select count(*) AS `count`,sol_host FROM {reident}tracts
     WHERE timediff(now(),sol_end) < "01:00:00"
     GROUP BY sol_host ORDER BY 2"""),

    ("LP files in progress",
     """SELECT stusab,county,tract,lp_start,lp_host,timediff(lp_start,now()) AS age,hostlock
     FROM {reident}tracts WHERE pop100>0 AND lp_start IS NOT NULL and LP_END IS NULL ORDER BY hostlock,lp_start"""),

    ("SOLs in progress",
     """SELECT stusab,county,tract,sol_start,sol_host,timediff(sol_start,now()) AS age,hostlock
     FROM {reident}tracts WHERE pop100>0 AND sol_start IS NOT NULL and SOL_END IS NULL ORDER BY sol_start"""),

    ]



def get_recon_status(auth, reident_=None):
    """Perform database queries regarding the current state of the reconstruction and return results as a JSON file.
    this is used for the dashboard API but can be run from the command line as well.
    :param auth: authentication token.
    :return: a dictionary with queries.
          'tables' - all of the tables in the database.
          'reidents' - all of the reidents
    """
    ret = {}
    ret['tables'] =   [row[0] for row in dbfile.DBMySQL.csfr(auth, "SHOW TABLES")]
    ret['queries'] = []
    ret['reidents'] = [table.replace(SEP_TRACTS,"") for table in ret['tables'] if table.endswith(SEP_TRACTS)]
    for reident in ([reident_] if reident_ else get_reidents(auth)):
        tables = []
        for(name,query) in QUERIES:
            column_names = []
            rows = DBMySQL.csfr(auth, query.replace("{reident}",reident+"_"), (), asDicts=True)
            tables.append((name,rows))
        ret['queries'].append((reident,tables))
    return ret

def api(auth):
    return json.dumps(get_recon_status(auth),default=str);

def get_reidents(auth):
    """Return the reidents.
    :param auth: authentication token.
    :return: a list of the reidents, which is taken to be the prefix of every table with a suffix of `_errors.`
    """
    return [row[0].replace("_tracts","") for row in dbfile.DBMySQL.csfr(auth, f"SHOW TABLES LIKE '%{SEP_TRACTS}'") ]


def do_register(auth, reident):
    """Create a new database with reident as a prefix."""
    db = dbfile.DBMySQL(auth)
    tables = db.execselect(f"SHOW TABLES LIKE '{reident}{SEP_TRACTS}'")
    if tables:
        raise ValueError(f"{reident} already exists")

    # Make sure the zip files exist
    if not FAST:
        print(f"Verifying REIDENT={reident} distribution files")
        for stusab in states():
            path = sf1_zipfile_name(reident, stusab)
            print("\r",path,"...       ",end='')
            if not ctools.s3.s3exists(path):
                raise FileNotFoundError(path)
        print("\n\nSF1 ZIPs present")

    # Now get the schema and transform it for reident by renaming every table
    # and view to {reident}_{name}
    new_schema = io.StringIO()
    create_table_re = re.compile("CREATE TABLE `?([a-z0-9_]+)`?",re.I)
    create_view_re = re.compile("CREATE VIEW `?([a-z0-9_]+)`?",re.I)
    from_re = re.compile("FROM `?([a-z0-9_]+)`?",re.I)

    # This is for rewriting the view:
    # I wasn't able to get the view stuff to work, so we just drop the views
    table_column_re = re.compile("`([a-zA-Z_]+)`[.]`([a-zA-Z_0-9]+)`")
    incomment = False
    for line in open(RECON_SCHEMA):
        # remove all the DROPS for safety!
        if incomment:
            if line.strip().endswith("*/;"):
                incomment = False
            continue
        if " VIEW " in line:
            if not line.strip().endswith("*/;"):
                incomment = True
            continue
        if "CREATE ALGORITHM=UNDEFINED" in line:
            continue

        # Rename tables as necessary

        oline = line
        if "DROP" in line:
            continue
        # fix column in VIEWS. We have to do this in reverse because we keep making the line longer with each substitution.
        if "VIEW" in line:
            for m in reversed(list(table_column_re.finditer(line))):
                line = line[:m.start(1)] + reident + SEP + line[m.start(1):]
        # fix name prefixes
        for r in [create_table_re, create_view_re, from_re]:
            m = r.search(line)
            if m:
                line = line[:m.start(1)] + reident + SEP + line[m.start(1):]
        if oline!=line:
            logging.debug("oline: %s",oline)
            logging.debug("line: %s",line)
            logging.debug("")

        new_schema.write(line)
    print(f"Creating tables with {reident} prefix")
    db.create_schema(new_schema.getvalue())


def runpop(cmd, out=None, err=None):
    """Popen a command. Stdout gets piped through"""
    print()
    print("$ " + " ".join(cmd))
    subprocess.Popen(cmd, cwd=RECON_DIR, stdin=subprocess.PIPE, stdout=out, stderr=err)


def run(cmd, check=True):
    """Run a command. Stdout gets piped through"""
    print()
    print("$ " + " ".join(cmd))
    subprocess.run(cmd, cwd=RECON_DIR, check=check)

def do_step1(auth, reident, state, *, force=False):
    print(f"Step 1 - s1_make_geo_files.py")
    cmd = [sys.executable, 's1_make_geo_files.py', '--j1', S1_J1, '--config', RECON_CONFIG]
    if force:
        cmd.append("--force")
    if args.latin1:
        cmd.append("--latin1")
    cmd.append(state)
    run(cmd)

def do_step2(auth, reident, state):
    print(f"Step 2 - s2_nbuild_state_stats.py")
    run([sys.executable, 's2_nbuild_state_stats.py', '--retry_attempts', S2_R, '--reident', reident, '--j1', S2_J1, '--config', RECON_CONFIG, state])

def do_step3(auth, reident, state, county, tract):
    print(f"Step 3 - s3_pandas_synth_lp_files.py")
    cmd = [sys.executable, 's3_pandas_synth_lp_files.py', '--config', RECON_CONFIG, '--j1', S3_J1, '--j2', S3_J2, state, county]
    if tract:
        cmd.append(tract)
    run(cmd)

def do_step4(auth, reident, state, county, tracts):
    print(f"Step 4 - s4_run_gurobi.py")
    cmd = [sys.executable, 's4_run_gurobi.py', '--config', RECON_CONFIG, '--j1', S4_J1, '--j2', S4_J2, state, county]+tracts
    run(cmd)

def do_step5(auth, reident, state, county):
    print(f"Step 5 - s5_make_microdata.py")
    cmd = [sys.executable, 's5_make_microdata.py', '--config', RECON_CONFIG, '--j1', S5_J1, state, county]
    run(cmd)

def list_counties(auth, stusab):
    logging.error("A county must be specified. Try one of these:")
    rows = dbfile.DBMySQL.csfr(auth,f"SELECT DISTINCT county FROM {REIDENT}geo where stusab=%s and sumlev='050'",
                               (stusab,))
    for row in rows:
        logging.error("\t%s",row[0])

def list_tracts(auth,stusab, county):
    logging.error("One or more tracts must be specified. Try one of these (or type 'all'):")
    rows = dbfile.DBMySQL.csfr(auth,f"SELECT DISTINCT tract FROM {REIDENT}geo where stusab=%s and county=%s and sumlev='101'",
                               (stusab,county))
    for row in rows:
        logging.error("\t%s",row[0])

def do_drop(auth,reident):
    """Create a new database with reident as a prefix.
    :param auth: authentication token.
    :param reident: reident to use.
    """
    # note that _ is a special character and must be escaped.
    drop_tables = [row[0] for row in dbfile.DBMySQL.csfr(auth,f"SHOW TABLES LIKE '{reident}\\_%%'")]
    if not drop_tables:
        logging.error(f"{reident} is not an active REIDENT")
        exit(1)
    print("Will delete the following tables:")
    print("\n".join(drop_tables))
    print("")
    confirm = input(f"Type '{reident}' to confirm: ").strip()
    if confirm!=reident:
        logging.error("drop tables not confirmed")
        exit(1)
    for table in drop_tables:
        cmd = f"DROP TABLE IF EXISTS {table}"
        print(cmd)
        dbfile.DBMySQL.csfr(auth,cmd)
        cmd = f"DROP VIEW IF EXISTS {table}"
        print(cmd)
        dbfile.DBMySQL.csfr(auth,cmd)


def do_info(path):
    if path.endswith(".sol") or path.endswith(".sol.gz"):
        print(f"{path}:")
        final_pop = dbrecon.get_final_pop_for_gzfile(path)
        print(f"Final pop: {final_pop}")
    else:
        print(f"Don't know how to info: {path}",file=sys.stderr)
        exit(1)

def get_server():
    import boto3
    b_client = boto3.client('emr')
    out=b_client.list_instances(ClusterId=emr.clusterId(), InstanceGroupTypes=['MASTER'])
    return out["Instances"][0]["PrivateDnsName"]

def all_hosts():
    pat = re.compile("(ip-[^ :]+)")
    ret = []
    cmd = ['yarn','node','--list']
    for line in subprocess.check_output(cmd, stderr=open('/dev/null','w'), encoding='utf-8').split('\n'):
        m = pat.search(line)
        if m:
            ret.append(m.group(1))
    return ret


IDLE='IDLE'
CORE='CORE'
IN_USE='IN_USE'
def host_status(host):
    """Print the status of host and return True if it is ready to run"""
    cmd = ('grep instanceRole /emr/instance-controller/lib/info/extraInstanceData.json;ps ux;'
           'echo -n start  ; uptime -s;'
           'echo -n uptime ; uptime   ;'
           'echo -n vmstat ; vmstat | tail -1;')
    response = ssh_remote.run_command_on_host(host,cmd, pipeerror='True')
    lines = response.split('\n')

    def grep(s):
        line = [line.replace(s,'')  for line in lines if line.startswith(s)]
        if line:
            return line[0]
        return ''

    uptime   = grep('uptime')
    vmstat   = grep('vmstat')
    if vmstat != '':
        vmstat = f' Free: {int(vmstat[6:].split()[3])//(1024*1024)} GiB'

    if "TASK" in response or USE_CORE:
        if "scheduler.py" not in response:
            return (IDLE, f"idle: {host} {uptime} {vmstat}")
        else:
            return (IN_USE, f"in use: {host} {uptime} {vmstat}")
    else:
        return (CORE, f"CORE: {host} {uptime} {vmstat}")

def fast_all(callback, *args):
    """Run the callback on every machine, with the parameter being the hostname, each in their own process."""
    if args:
        with multiprocessing.Pool(50) as p:
            args_iter = zip(all_hosts(), repeat(args[0]), repeat(args[1]), repeat(args[2]), repeat(args[3]))
            p.starmap(callback, args_iter)
    else:
        with multiprocessing.Pool(50) as p:
            p.map(callback, all_hosts())

def status_all(ret=False):
    with multiprocessing.Pool(50) as p:
        rows = p.map(host_status, all_hosts())
    rows.sort()
    for row in rows:
        print(row[1])
    if ret is True:
        return rows


def show_file(path):
    if path.startswith('s3://'):
        cmd = ['aws','s3','ls',path]
    else:
        cmd = ['ls','-l',path]
    run(cmd, check=False)


# 21/04/03 23:47:35 WARN YarnSchedulerBackend$YarnSchedulerEndpoint: Requesting driver to remove executor 2 for reason Container killed by YARN for exceeding memory limits.
# 6.1 GB of 6 GB physical memory used. Consider boosting spark.yarn.executor.memoryOverhead or disabling yarn.nodemanager.vmem-check-enabled because of YARN-4714.

# It appears the memory given is a combination of --executor-memory and memoryOverhead.

def do_spark(args):
    """one of the great errors in the desing of the DAS was that we didn't use a python program to launch spark, instead we use a run_cluster script.
    Here we do not repeat that same mistake.
    """
    cmd = ['--executor-memory',args.executor_memory,
           '--executor-cores','1','--driver-cores','10',
           '--conf','spark.driver.maxResultSize=0g',
           '--conf','spark.executor.memoryOverhead='+args.executor_memoryOverhead]
    if args.disable_vmem_check:
        cmd += ['--conf','yarn.nodemanager.vmem-check-enabled=false']
    cmd += ['scheduler.py','--spark','--limit',str(args.limit),'--reident',args.reident, '--nolock','--pop100',args.pop100]
    if args.rescan:
        cmd.append('--rescan')
    elif args.step3:
        cmd.append('--step3')
    elif args.step4:
        cmd.append('--step4')
    else:
        logging.error('--spark requires --rescan, --step3 or --step4')

    print("LAUNCH: ",cmd)
    REQUIRED_FILES = glob.glob("*.py") + glob.glob("ctools/*.py") + glob.glob("dfxml/*py")

    # Let spark set the number of executors automatically. It seems to do this anyway.

    ctools.cspark.spark_submit(files_to_zip = REQUIRED_FILES, pyfiles = ['layouts/sf1_vars_race_binaries.csv'], argv = cmd)

def send_master_config(host):
    cmd1 = [os.path.join( dirname(__file__), MYSQL_GUROBI_CONFIG),  f'~/{CONFIG_FILE_NAME}']
    ssh_remote.process_host(host=host, scp=True, cmd=cmd1)
    cmd2 = [os.path.join(dirname(__file__), RECON_CONFIG), f'~/{RECON_CONFIG_FILENAME}']
    ssh_remote.process_host(host=host, scp=True, cmd=cmd2)

def do_launch(host, *, debug=False, asc=False, reident=None, branch=None, token=None, scp=False):
    if scp:
        send_master_config(host)
    cmd = (
        'cd /home/hadoop;'
        f'mv {STATS_2010} {STATS_2010}.$$;'
        f'git clone {GITHUB}{STATS_2010};'
        f'cd {STATS_2010};'
        f'git remote set-url origin {GITHUB}{STATS_2010};'
        'git pull;'
        'git submodule update --init --recursive;'
        'cd recon;'
        f'git remote set-url origin {GITHUB}{STATS_2010};'
        'echo getting most recent stats_2010;'
        'git checkout ' + branch + ' ; git pull --recurse;'
        'kill $(ps auxww | grep dbrtool.py | grep -v grep | awk "{print $2;}");'
        'kill $(ps auxww | grep scheduler.py | grep -v grep | awk "{print $2;}");'
        'echo versions:;'
        'grep __version__ *.py;'
        f'cd {STATS_2010}/recon;'
        f'cp ~/{CONFIG_FILE_NAME} {CONFIG_FILE_NAME};'
        f'cp ~/{RECON_CONFIG_FILENAME} {RECON_CONFIG_FILENAME};'
        '$(./dbrtool.py --env);'
        f'(./dbrtool.py --run --reident {reident} > output-$(date -Iseconds) 2>&1 </dev/null &);'
        'echo launched scheduler at $(date) on $(hostname);'
        )
    if asc:
        cmd = cmd.replace("--run","--run --asc ")
    # Run this in the background
    if debug:
        print(cmd)
    #
    # os.fork() makes this run in the background. Otherwise this takes too long.
    # we don't go into the background if we are running in debug mode.

    if debug or os.fork()==0:
        out = ssh_remote.run_command_on_host(host, cmd, pipeerror=True)
        if debug:
            print(out)
        exit(0)
def run_cmd(host,cmd):
    if cmd is not list:
        return ssh_remote.run_command_on_host(host, cmd, pipeerror=True)
    else:
        ssh_remote.process_host(host=host, scp=True, cmd=cmd)
        return str(cmd)

def unlock(auth,reident, auto_confirm=None):
    """remove hostlock from hosts that are no longer in the cluster"""
    host_re = re.compile('^([^.]+)')
    def hostname(x):
        m = host_re.search(x)
        if m:
            return m.group(1)
        raise ValueError('cannot find host in: '+x)

    hosts = [hostname(h) for h in all_hosts()]
    params = ",".join(['%s' for _ in range(len(hosts))])
    count = 0
    for which in ['lp', 'sol', 'csv']:
        cmd = (
            f"""SELECT stusab,county,tract,hostlock FROM {reident}_tracts WHERE ({which}_start IS NOT NULL)
            AND ({which}_end is null) AND (hostlock IS NOT NULL) and (HOSTLOCK NOT IN ({params}))""")
        rows = DBMySQL.csfr(auth,cmd,hosts)
        if rows:
            print("Locked ",which,':')
            for row in rows:
                print(row)
            print()
            count += len(rows)
    if count>0:
        if auto_confirm:
            confirm = auto_confirm
        else:
            confirm = input(f"Unlock {count} tracts locked by hosts no longer in cluster? [y/n]: ").strip().lower()
        if confirm=='y':
            print("Unlocking...")
            for which in ['lp', 'sol', 'csv']:
                cmd = (
                    f"""UPDATE {reident}_tracts set {which}_start=NULL, {which}_host=NULL, {which}_end=NULL, hostlock=NULL
                    WHERE ({which}_start IS NOT NULL) AND ({which}_end IS NULL) AND (hostlock IS NOT NULL) AND (hostlock NOT IN ({params}))""")
                DBMySQL.csfr(auth,cmd,hosts)


def launch_if_needed(host, d=None, r=None, b=None, f=None):
    if d is None:
        d = args.asc
    if r is None:
        r = args.reident
    if b is None:
        b = args.branch
    if f is None:
        f = args.force
    status = host_status(host)[0]
    if status==IDLE or (status==IN_USE and f):
        do_launch(host, asc=d, reident=r, branch=b)
        print("Launch: ",host)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=HELP,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    g = parser.add_mutually_exclusive_group()
    g.add_argument("--env", action='store_true', help='Generate export functions to put MySQL authentication into your environment')
    g.add_argument("--mysql", action='store_true', help='Provide a MySQL command line')
    g.add_argument("--dbquery", action='store_true', help='Show the results of the dbquery. Mostly for debugging')
    g.add_argument("--register", action='store_true', help="register a new REIDENT for database reconstruction.")
    g.add_argument("--drop", action='store_true', help="drop a REIDENT from database")
    g.add_argument("--show", "--reidents", '--status', action='store_true', help="Show everything known about reisdents, and optionally a stusab, county, tract")
    g.add_argument("--info", help="Provide info on a file")
    g.add_argument("--ls", action='store_true',help="Show the files")
    g.add_argument("--run", help="Run the scheduler",action='store_true')
    g.add_argument("--setup", help="Setup a driver machine to run recon")
    g.add_argument("--setup_all", help="Setup all driver machines to run recon",action='store_true')
    g.add_argument("--launch", help="Run --run on a specific machine(s) (sep by comma)")
    g.add_argument("--cluster_status", help="report each machine status", action='store_true')
    g.add_argument("--resize", help="Resize cluster", type=int)
    g.add_argument("--rescan", help="Validate all LP and SOL files in S3 against the database", action='store_true')
    g.add_argument("--step3", help="Run Step 3 and make LP files. You can run singles for testing, or with --spark", action='store_true')
    g.add_argument("--step4", help="RUn Step 4 and make SOL files. You can run singles for testing, or run with --spark", action='store_true')
    g.add_argument("--step5_t", help="manually run Step 5 and make microdata. Normall run this through the controller. This is for testing", action='store_true')
    g.add_argument("--launch_all", help="Run --run on every Task that is not running a scheduler", action='store_true')

    parser.add_argument("--runbg", help="Run the scheduler process without blocking", action='store_true')
    parser.add_argument("--step5",
                   help="run Step 5 and make microdata through scheduler",
                   action='store_true')
    parser.add_argument("--step6",
                        help="run Step 6 and make rhdf through scheduler",
                        action='store_true')

    parser.add_argument('--unlock', action='store_true', help='clear the hostlock for hosts that are no longer part of the cluster.')
    parser.add_argument("--reident", help="specify the reconstruction identification")
    parser.add_argument("--spark", help="Run certian commands under spark",action='store_true')

    parser.add_argument("--step1", help="Run step 1 - make the county list. Defaults to all states unless state is specified. Only needs to be run once per state", action='store_true')
    parser.add_argument("--step2", help="Run step 2. Defaults to all states unless state is specified", action='store_true')


    parser.add_argument("--stusab", help="which stusab to process", default='all')
    parser.add_argument("--county", help="required for step3 and step4")
    parser.add_argument("--tract",  help="required for step 4. Specify multiple tracts with commas between them")
    parser.add_argument('--debug', action='store_true', help='debug all SQL')
    parser.add_argument('--force', action='store_true', help='delete output files if they exist, and force launching on all clusters')
    parser.add_argument('--nodes', help='Show YARN nodes', action='store_true')
    parser.add_argument('--prep', help='Log into each node and prep it for the dbrecon', action='store_true')
    parser.add_argument("--limit", type=int, default=1000000, help='Limit when rescanning or launching processes')
    parser.add_argument("--asc", help="Run the scheduler, smallest tracts first", action='store_false')
    parser.add_argument("--num_executors", help="number of spark executors to use",default=NUMBER_OF_WORKERS_TIMES_40)
    parser.add_argument("--pop100", help="--spark --step3 and --step4, specifies the max(pop100) to work with",default=">0")
    parser.add_argument("--executor_memory", help="how much memory to give executors",default='1g')
    parser.add_argument("--executor_memoryOverhead", help="how much extra memory to give executors",default='100g')
    parser.add_argument('--disable-vmem-check', action='store_true', help='set yarn.nodemanager.vmem-check-enabled to false')
    parser.add_argument("--watch", help="Run the watch command to watch the cluster status and the reident status", action='store_true')
    parser.add_argument("--branch", help="which branch to pull", default='main')
    parser.add_argument("--latin1", help="encoding Latin-1", action='store_true')
    parser.add_argument("--get_master_config", help="get dbrecon_config form server", action='store_true')

    args = parser.parse_args()
    if args.get_master_config:
        get_master_config()
        exit(0)
    if args.env:
        for(k,v) in get_mysql_env().items():
            print(f"export {k}={v}")
        exit(0)

    if args.info:
        do_info(args.info)
        exit(0)

    if args.nodes:
        run(['yarn','node','--list'])
        exit(0)


    if args.cluster_status:
        status_all()
        exit(0)

    if args.resize is not None:
        confirm = input(f"Resize cluster to {args.resize} nodes? [y/n] ").strip()
        if confirm[0:1]=='y':
            subprocess.check_call([sys.executable,EMR_CONTROL,'--task',str(args.resize)])
        exit(0)

    ################################################################
    # Everything after here needs mysql

    if 'MYSQL_HOST' not in os.environ:
        logging.warning('MYSQL_HOST is not in your environment!')
        logging.warning('Please  run $(./dbrtool.py --env)')
        exit(0)

    if args.mysql:
        do_mysql()

    auth = get_auth()
    if args.debug:
        auth.debug = True
        logging.getLogger().setLevel(logging.INFO)

    if args.show:               # --status
        reidents = get_reidents(auth)
        if (not args.reident) or (args.reident not in reidents):
            print("Please specify a reident:")
            for reident in reidents:
                print(f"    --reident {reident}")
            exit(0)
        ret = get_recon_status(auth, args.reident)
        for (k,dicts) in ret['queries'][0][1]:
            print(k)
            for d in dicts:
                for (ct,(k,v)) in enumerate(d.items()):
                    print(f"   {k}: {v}",end='\n' if k!='count' else '')
                if ct>1 and k!='rows':
                    print()
            print('----------------------')
        if args.stusab and args.county and args.tract:
            if args.reident:
                reidents = [args.reident]
            for reident in reidents:
                print(f"\n================ {reident} ================")
                dbrecon.set_reident(reident)
                print(f"reident: {reident} {args.stusab} {args.county} {args.tract}")
                for fname in [dbrecon.LPFILENAMEGZ( stusab=args.stusab,county=args.county,tract=args.tract),
                              dbrecon.SOLFILENAMEGZ(stusab=args.stusab,county=args.county,tract=args.tract)]:
                    show_file(fname)
                    print()
                print("Database:")
                rows = DBMySQL.csfr(auth,f"select * from {reident}_tracts where stusab=%s and county=%s and tract=%s",
                               (args.stusab,args.county,args.tract),asDicts=True)
                for row in rows:
                    for (k,v) in row.items():
                        print(f"{k:15} {v}")
        exit(0)

    if args.dbquery:
        ret = get_recon_status(auth, args.reident)
        print(json.dumps(ret,default=str,indent=4))
        exit(0)

    ################################################################
    # Everything after here needs reident
    if args.reident:
        # Pass to subprocesses
        os.environ['REIDENT_NO_SEP'] = args.reident
        dbrecon.REIDENT = os.environ['REIDENT'] = REIDENT = args.reident+"_"
    else:
        print("Please specify --reident\n",file=sys.stderr)
        exit(1)

    if args.unlock:
        unlock(auth,args.reident)

    if args.launch:
        unlock(auth, args.reident)
        for host in args.launch.split(','):
            do_launch(host, debug=args.debug, asc=args.asc, reident=args.reident, branch=args.branch)
        exit(0)

    if args.watch:
        try:
            subprocess.call(['watch',f'python3 dbrtool.py --cluster ; python3 dbrtool.py --status --reident {args.reident}'])
        except KeyboardInterrupt as e:
            pass
        exit(0)
    if args.register:
        do_register(auth, args.reident)
        print(f"\n{args.reident} registered")
        exit(0)
    elif args.drop:
        do_drop(auth, args.reident)
        print(f"\n{args.reident} dropped")
        exit(0)
    elif args.ls:
        root = os.path.join(os.getenv('DAS_S3ROOT'),'2010-re',args.reident,'work',args.stusab)
        run(['aws','s3','ls','--recursive',root])
    elif args.run:
        if args.step1:
            do_step1(auth, args.reident, args.stusab, force=args.force)

        if args.step2:
            do_step2(auth, args.reident, args.stusab)
        cmd = [sys.executable,'scheduler.py']
        if args.stusab:
            cmd.extend(['--stusab', args.stusab])
        if args.county:
            cmd.extend(['--county', args.county])
        if args.asc:
            cmd.extend(['--asc'])
        if args.step5:
            cmd.extend(['--step5'])
        if args.step6:
            cmd.extend(['--step6'])
        if args.reident:
            cmd.extend(['--reident', args.reident])
        run(cmd)


    elif args.runbg:
        if args.step1:
            do_step1(auth, args.reident, args.stusab, force=args.force)
        if args.step2:
            do_step2(auth, args.reident, args.stusab)
        cmd = [sys.executable,'scheduler.py']
        if args.stusab:
            cmd.extend(['--stusab', args.stusab])
        if args.county:
            cmd.extend(['--county', args.county])
        if args.asc:
            cmd.extend(['--asc'])
        if args.step5:
            cmd.extend(['--step5'])
        if args.step6:
            cmd.extend(['--step6'])
        if args.reident:
            cmd.extend(['--reident', args.reident])
        if args.launch_all:
            cmd.extend(['--launch_all'])
            cmd.extend(['--branch', args.branch])
        now = str(datetime.datetime.now()).replace(" ","_")
        with open(f"output-{now}", "wb") as o, open(f"error-{now}", "wb") as e:
            runpop(cmd, out=o, err=e)

    if args.launch_all and not args.runbg:
        unlock(auth, args.reident)
        fast_all(launch_if_needed)
        exit(0)


    # Check if we are using spark!
    ################################################################
    if args.spark:
        do_spark(args)

    if args.rescan:
        run([sys.executable, 'scheduler.py', '--config', RECON_CONFIG, '--rescan', '--j1', REFRESH_PARALLELISM, '--limit', str(args.limit)])
        exit(0)

    ################################################################
    # We can run multiple steps if we want! For testing, of course
    if args.step3 or args.step4 or args.step5_t:
        if args.stusab=='step5_t':
            logging.error("cannot run step3, step4 or step5_t on all stusabs unless --spark is provided or unless you run the scheduler directly.")
            exit(1)
        if not args.county:
            list_counties(auth, args.stusab)
            exit(1)

    if args.step4:
        if not args.tract or (len(args.tract)!=6 and args.tract!='all'):
            list_tracts(auth, args.stusab, args.county)
            exit(1)

    if args.step1 and not args.runbg:
        do_step1(auth, args.reident, args.stusab, force=args.force)

    if args.step2 and not args.runbg:
        do_step2(auth, args.reident, args.stusab)

    if args.step3:
        if args.force:
            dbrecon.remove_lpfile(auth, args.stusab, args.county, args.tract)
        do_step3(auth, args.reident, args.stusab, args.county, args.tract)

    if args.step4:
        if args.force:
            dbrecon.remove_solfile(auth, args.stusab, args.county, args.tract)
        do_step4(auth, args.reident, args.stusab, args.county, args.tract.split(","))

    if args.step5_t:
        if args.force:
            dbrecon.remove_csvfile(auth, args.stusab, args.county)
        do_step5(auth, args.reident, args.stusab, args.county)
