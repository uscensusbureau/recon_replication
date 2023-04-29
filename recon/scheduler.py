#!/usr/bin/env python3
#
"""scheduler.py:

The two time-consuming parts of the reconstruction are building and
solving the LP files. This schedules the jobs on multiple machines
using a MySQL database for coordination.

This is a simple scheduler for a single system. But you can run it on multiple systems at the same time. They coordinate through the database


"""

import copy
import dbrecon
import logging
import os
import os.path
import subprocess
import sys
import time
import psutil
import socket
import fcntl
import multiprocessing
import operator
import uuid
import datetime
import json
import glob
from os.path import dirname,basename,abspath

# Set up path, among other things
import dbrecon

from dbrecon import dopen,dmakedirs,dsystem,hostname
from dfxml.writer import DFXMLWriter
from dbrecon import LP,SOL,MB,GB,MiB,GiB,get_config_int,REIDENT,LP
from ctools.dbfile import DBMySQL,DBMySQLAuth

import ctools.cspark as cspark
import ctools.lock
import s3_pandas_synth_lp_files
import dbrtool

__version__ = '1.0.1'

HELP="""
Try one of the following commands:

HALT - Immediately halt the jobs and the scheduler.
STOP - Clean shutdown of the scheduler at the end of the current.
PS   - Show the current processes
LIST - Show the current tasks
UPTIME - Show system load
DEBUG  - enable/disable debugging
SQL    - show/hide SQL
"""


# Tuning parameters

SCHEMA_FILENAME="schema.sql"
PYTHON_START_TIME = 1
MIN_LP_WAIT   = 120             # wait two minutes between launches
MIN_SOL_WAIT  = 60              # wait one minute between launch
MIN_ClUSTER_WAIT = 21600         # wait two hours before checking on clusters
#SPARK_TRACTS_PER_PARTITION = 3
SPARK_TRACTS_PER_PARTITION = 1

# Failsafes: don't start an LP or SOL unless we have this much free
MIN_FREE_MEM_FOR_LP  = 500*GiB  # we've seen LP generation take up to 500GiB
MIN_FREE_MEM_FOR_SOL = 300*GiB  # don't launch a SOL if there is less than 300GiB free

MIN_FREE_MEM_FOR_KILLER = 5*GiB  # if less than this, start killing processes

REPORT_FREQUENCY = 60           # report this often into sysload table
PROCESS_DIE_TIME = 5            # how long to wait for a process to die
LONG_SLEEP= 300          # sleep for this long (seconds) when there are no resources
PS_LIST_FREQUENCY = 30   # big status report every 30 seconds

S3_SYNTH = 's3_pandas_synth_lp_files.py'
S4_RUN   = 's4_run_gurobi.py'
S4_LOW_MEMORY_RETRIES = 60      # try for an hour before giving up
S4_BOTO_RETRIES = 15
LP_J1    = 1                    # let this program schedule
S5_J1    = 64
LP_MULTI_MAX_COUNTY_POP = 30000  # if more than this many people in the county, only run one LP file at a time


SPARK_SMALL_LP_MAX_POP100 = 3000 # anything over 3000 requires large

def load():
    return os.getloadavg()[0]

def prun(cmd):
    """Run Popen unless we are in debug mode"""
    p = psutil.Popen(cmd)
    info = f"PID{p.pid}: LAUNCH {' '.join(cmd)}"
    logging.info(info)
    return p


def get_free_mem():
    return psutil.virtual_memory().available
    #lines = subprocess.check_output(['free','-b'],encoding='utf-8').split('\n')
    #return int(lines[1].split()[3])

last_report = 0
def report_load_memory(auth):
    """Report and print the load and free memory; return free memory"""
    global last_report
    free_mem = get_free_mem()

    # print current tasks
    total_seconds = (time.time() - dbrecon.start_time)
    hours    = int(total_seconds // 3600)
    mins     = int((total_seconds % 3600) // 60)
    secs     = int(total_seconds % 60)
    if time.time() > last_report + REPORT_FREQUENCY:
        DBMySQL.csfr(auth,
            """
            INSERT INTO sysload (t, host, min1, min5, min15, freegb)
            VALUES (NOW(), %s, %s, %s, %s, %s) ON DUPLICATE KEY update min1=min1
            """,
            [hostname()] + list(os.getloadavg()) + [get_free_mem()//GiB])
        last_report = time.time()
    return free_mem


def pcmd(p):
    """Return a process command"""
    return " ".join(p.args)

def kill_tree(p):
    print("PID{} KILL TREE {} ".format(p.pid,pcmd(p)))
    for child in p.children(recursive=True):
        try:
            print("   killing",child,end='')
            child.kill()
            print("   waiting...", end='')
            child.wait()
        except psutil.NoSuchProcess as e:
            pass
        finally:
            print("")
    p.kill()
    # Don't wait anymore.

class PSTree():
    """Service class. Given a set of processes (or all of them), find parents that meet certain requirements."""
    def __init__(self,plist=psutil.process_iter()):
        self.plist = [(psutil.Process(p) if isinstance(p,int) else p) for p in plist]

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return

    def total_rss(self,p):
        """Return the total rss for the process and all of its children."""
        return (p.memory_info().rss +
                sum([child.memory_info().rss for child in p.children(recursive=True)]))

    def total_user_time(self,p):
        return (p.cpu_times().user +
                sum([child.cpu_times().user for child in p.children(recursive=True)]))

    def ps_list(self):
        """ps_list shows all of the processes that we are running."""
        print("PIDs in use: ",[p.pid for p in self.plist])
        print("")
        for p in sorted(self.plist, key=lambda p:p.pid):
            try:
                print("PID{}: {:,} MiB {} children {} ".format(
                    p.pid, int(self.total_rss(p)/MiB), len(p.children(recursive=True)), pcmd(p)))
                subprocess.call(f"ps wwx -o uname,pid,ppid,pcpu,vsz,rss,command --sort=-pcpu --ppid={p.pid}".split())
            except psutil.NoSuchProcess as e:
                continue

    def youngest(self):
        return sorted(self.plist, key=lambda p:self.total_user_time(p))[0]

#
# Note: change running() into a dictionary where the start time is the key
# Then report for each job how long it has been running.
# Modify this to track the total number of bytes by all child processes


def launch_all_timer(last_check, wait_time):
    '''wait certain amount then try to launch_all'''
    if time.time() > last_check+wait_time:
        return check_cluster_status()
    else:
        return False


def check_cluster_status():
    ret = dbrtool.status_all(True)
    print(ret)
    idles = [x[1].split(":")[0] for x in ret if "idle" in x[1].split(":")[0]]
    if idles:
        return True
    else:
        return False

def run(auth, args):
    assert args.spark == False
    debug           = args.debug
    running         = set()
    last_ps_list    = 0
    last_lp_launch  = 0
    last_sol_launch = 0
    last_county_pop = 0
    last_cluster_check = 10 #wait before first cluster_check
    halting         = False
    step5_start = False
    step6_start = False
    stusab_arg          = args.stusab
    check_lp        = args.step3
    check_sol       = args.step4
    needed_lp       = 0
    needed_sol      = 0
    limit           = args.limit

    os.set_blocking(sys.stdin.fileno(), False)

    # If neither step3 nor step4 were specified, run both!
    if (not check_lp) and (not check_sol):
        check_lp  = True
        check_sol = True

    def running_lp():
        """Return a list of the runn LP makers"""
        return [p for p in running if p.args[1]==S3_SYNTH]

    def running_lps():
        """Return the count of running lp jobs"""
        return len(running_lp())

    stop_requested = False

    while True:
        if (limit is not None) and limit < 1:
            print("Limit reached. Requesting stop")
            stop_requested = True
            limit = None
        try:
            command = sys.stdin.read(256).strip().lower()
        except OSError as e:
            command = ''
            logging.debug(f"OSerror: {e} are you running in nohup")

        if command!='':
            print("COMMAND:",command)
            if command=="halt":
                # Halt is like stop, except we kill the jobs first
                print("Killing existing...")
                [kill_tree(p) for p in running]
                stop_requested = True
                halt           = True
            elif command=='stop':
                stop_requested = True
            elif command.startswith('ps'):
                subprocess.call("ps ww -o uname,pid,ppid,pcpu,vsz,rss,command --sort=-pcpu".split())
            elif command=='list':
                last_ps_list = 0
            elif command=='uptime':
                subprocess.call(['uptime'])
            elif command=='debug':
                debug = not debug
            elif command=='sql':
                auth.debug = not auth.debug
            else:
                if command!='help':
                    print(f"UNKNOWN COMMAND: '{command}'.")
                print(HELP)

        # Clean database if necessary
        dbrecon.db_clean(auth)

        # Report system usage if necessary
        dbrecon.GetConfig().config_reload()
        free_mem = report_load_memory(auth)


        if args.launch_all and launch_all_timer(last_cluster_check, MIN_ClUSTER_WAIT):
            dbrtool.unlock(auth, args.reident, auto_confirm=True)
            d = args.asc
            r = args.reident
            b = args.branch
            dbrtool.fast_all(dbrtool.launch_if_needed, d, r, b, False, False)
            last_cluster_check = time.time()


        # Are we done yet?
        remain = {}
        for what in ['lp','sol']:
            remain[what]  = DBMySQL.csfr(auth,
                                   f""" SELECT count(*) from {REIDENT}tracts WHERE {what}_end is null and pop100>0 """)[0][0]

        if remain['sol']==0:
            if not step5_start and not step6_start:
                print("LP and SOL done!")
            if args.step5 or args.step6:
                remain['csv'] = DBMySQL.csfr(auth,
                                            f""" SELECT count(*) from {REIDENT}tracts WHERE csv_end is null and pop100>0 """)[0][0]
                if remain['csv'] == 0:
                    if step5_start is True:
                        print("CSV done!")
                    if args.step6 and step6_start is False:
                        cmd = [sys.executable, 's6_make_rhdf.py',
                               '--reident', dbrecon.reident_no_sep()]
                        p = prun(cmd)
                        running.add(p)
                        p6 = p
                        print(f"Step 6 started PID: {p.pid}")
                        step6_start = True
                    else:
                        if args.step6 and p6 in running:
                            if p6.poll() is None:
                                pass
                            else:
                                return
                        else:
                            return
                if not step5_start and args.step5:
                    cmd = [sys.executable, 's5_make_microdata.py',
                           '--reident', dbrecon.reident_no_sep(),
                           '--j1', str(S5_J1), stusab_arg, 'all']
                    p = prun(cmd)
                    running.add(p)
                    print(f"Step 5 started PID: {p.pid}")
                    step5_start = True
            else:
                return

        # See if any of the processes have finished
        # Hard fail if any of them did not exit cleanly so we can diagnose the problem.
        for p in copy.copy(running):
            if p.poll() is not None:
                logging.info(f"PID{p.pid}: EXITED {pcmd(p)} code: {p.returncode}")
                if debug:
                    print("PID{p.pid} {p.args} completed")
                if p.returncode!=0 and not halting:
                    logging.error(f"ERROR: Process {p.pid} did not exit cleanly. retcode={p.returncode} mypid={os.getpid()} ")
                    logging.error(f"***************************************************************************************")
                    exit(1)
                running.remove(p)

        SHOW_PS = False
        with PSTree(running) as ps:
            from unicodedata import lookup
            if ((time.time() > last_ps_list + PS_LIST_FREQUENCY) and SHOW_PS) or (last_ps_list==0) or (stop_requested):
                print("")
                print(lookup('BLACK DOWN-POINTING TRIANGLE')*64)
                print("{}: {} Free Memory: {} GiB  {}% Load: {}".format(
                    hostname(),
                    time.asctime(), free_mem//GiB, psutil.virtual_memory().percent, os.getloadavg()))
                print(f"Remaining LP: {remain['lp']} Remaining SOL: {remain['sol']}")
                print("Running processes:")
                ps.ps_list()
                print(lookup('BLACK UP-POINTING TRIANGLE')*64+"\n")
                last_ps_list = time.time()

            if free_mem < MIN_FREE_MEM_FOR_KILLER:
                logging.error("%%%")
                logging.error("%%% Free memory down to {:,} -- will start killing processes.".format(get_free_mem()))
                logging.error("%%%")
                subprocess.call(['./pps.sh'])
                if len(running)==0:
                    logging.error("No more processes to kill. Waiting for %s seconds and restarting",
                                  LONG_SLEEP)
                    time.sleep(LONG_SLEEP)
                    continue
                p = ps.youngest()
                logging.warning("KILL "+pcmd(p))
                DBMySQL.csfr(auth,f"INSERT INTO errors (host,file,error) VALUES (%s,%s,%s)",
                                (hostname(),__file__,"Free memory down to {}".format(get_free_mem())))
                kill_tree(p)
                running.remove(p)
                continue

        if stop_requested:
            print("STOP REQUESTED  (type 'halt' to abort)")
            if len(running)==0:
                print("NONE LEFT. STOPPING.")
                break;
            else:
                print(f"Waiting for {len(running)} processes to stop...")
                time.sleep(PROCESS_DIE_TIME)
                continue

        ################################################################
        ### LP SCHEDULER


        # See if we can create another process.
        # For stability, we create a max of one LP and one SOL each time through.

        # Figure out how many we need to launch
        #
        if step5_start is False and step6_start is False:
            if check_lp:
                needed_lp =  min(get_config_int('run','max_lp'),args.maxlp) - running_lps()

            if debug:
                print(f"needed_lp: {needed_lp}")

            # If we can run another launch in, do it.
            if (get_free_mem()>MIN_FREE_MEM_FOR_LP) and (needed_lp>0) and (last_lp_launch + MIN_LP_WAIT < time.time()):

                # We only launch one LP at a time because they take a few minutes to eat up a lot of memory.
                # We make the entire county at a time, so we group_by
                # Ignore those with hostlock, as they are being processed on another system
                if last_lp_launch + MIN_LP_WAIT > time.time():
                    continue
                direction = 'DESC' if ~args.asc else ''
                cmd = f"""
                    SELECT stusab,county,count(*) as tracts, sum(pop100) as county_pop
                    FROM {REIDENT}tracts
                    WHERE (lp_end IS NULL) AND (pop100>0) AND (hostlock IS NULL)
                    GROUP BY state,county
                    ORDER BY county_pop {direction} LIMIT 1
                    """.format()
                make_lps = DBMySQL.csfr(auth, cmd, debug=True)
                if (len(make_lps)==0 and needed_lp>0) or debug:
                    logging.warning(f"needed_lp: {needed_lp} but search produced 0. NO MORE LPS FOR NOW...")
                    last_lp_launch = time.time()

                for (stusab,county, tract_count, county_pop) in make_lps:
                    # check if current lp is large or last one loaded was large
                    if ((county_pop > LP_MULTI_MAX_COUNTY_POP) or (last_county_pop > LP_MULTI_MAX_COUNTY_POP)) and (running_lps() > 0):
                        print(f"\n{stusab} {county} tracts: {tract_count} county_pop: {county_pop}. running_lps:{running_lps()}")
                        print(f"Will only run one LP maker at a time\n")
                        break
                    # If the load average is too high, don't do it
                    lp_j2 = get_config_int('run','lp_j2')
                    stusab = stusab.lower()
                    print(f"\nLAUNCHING LP {S3_SYNTH} {stusab} {county} TRACTS: {tract_count:,} TRACT POP: {county_pop:,}")
                    cmd = [sys.executable,'s3_pandas_synth_lp_files.py',
                           '--reident', dbrecon.reident_no_sep(),
                           '--j1', str(LP_J1), '--j2', str(lp_j2), stusab, county]
                    print("$ " + " ".join(cmd))
                    p = prun(cmd)
                    running.add(p)
                    last_county_pop = county_pop
                    last_lp_launch = time.time()
                    if limit is not None:
                        limit -= 1
                        if limit==0:
                            break
                    print("Sleeping 30 seconds to let the LP launch to happen...")
                    time.sleep(30)

            ################################################################
            ## SOL scheduler.
            ## Evaluate Launching SOLs.
            ## Only evaluate solutions where we have a LP file

            max_sol    = get_config_int('run','max_sol')
            max_jobs   = get_config_int('run','max_jobs')
            max_sol_launch = get_config_int('run','max_sol_launch')
            if check_sol:
                needed_sol = min(max_jobs-len(running), max_sol)

            if debug:
                print(f"max_sol={max_sol} needed_sol={needed_sol} max_jobs={max_jobs} running={len(running)} get_free_mem()={get_free_mem()}")

            if get_free_mem()>MIN_FREE_MEM_FOR_SOL and needed_sol>0:
                # Run any solvers that we have room for
                # As before, we only launch one at a time
                # Always solve the biggest first, because they take the most time, and don't seem to take dramatically more memory.

                if last_sol_launch + MIN_SOL_WAIT > time.time() or (last_county_pop > LP_MULTI_MAX_COUNTY_POP and running_lps() > 0):
                    print(f"Can't launch again for {int(last_sol_launch+MIN_SOL_WAIT-time.time())} seconds")
                else:
                    cmd = f"""
                    SELECT stusab,county,tract FROM {REIDENT}tracts
                    WHERE (sol_end IS NULL) AND (lp_end IS NOT NULL) AND (hostlock IS NULL)
                    ORDER BY pop100 desc LIMIT %s
                    """
                    solve_lps = DBMySQL.csfr(auth,cmd,(max_sol_launch,))
                    if (len(solve_lps)==0 and needed_sol>0 and not step5_start and not step6_start) or debug:
                        print(f"No LP files ready to solve")

                    for (ct,(stusab,county,tract)) in enumerate(solve_lps,1):
                        print("LAUNCHING SOLVE {} {} {} ({}/{}) {}".format(stusab,county,tract,ct,len(solve_lps),time.asctime()))
                        gurobi_threads = get_config_int('gurobi','threads')
                        dbrecon.db_lock(auth, stusab,county,tract)
                        stusab         = stusab.lower()
                        cmd = [sys.executable,S4_RUN,
                               '--reident', dbrecon.reident_no_sep(),
                               '--low_memory_retries', str(S4_LOW_MEMORY_RETRIES),
                               '--boto_retries', str(S4_BOTO_RETRIES),
                               '--exit1','--j1','1','--j2',str(gurobi_threads),stusab,county,tract]
                        print("$ "+" ".join(cmd))
                        p = prun(cmd)
                        running.add(p)
                        time.sleep(PYTHON_START_TIME)
                        last_sol_launch = time.time()
                        if limit is not None:
                            limit -= 1
                            if limit==0:
                                break

            time.sleep( get_config_int('run', 'sleep_time' ) )
        # and repeat
    # Should never get here

################################################################
## killer stuff follows

def kill_running_s3_s4():
    """Look for any running instances of s3 or s4. If they are found, print and abort"""
    found = 0
    for p in psutil.process_iter():
        cmd = p.cmdline()
        if (len(cmd) > 2) and cmd[0]==sys.executable and cmd[1] in (S3_SYNTH, S4_RUN):
            found += 1
            if found==1:
                print("Killing Running S3 and S4 Processes:")
            print("KILL PID{} {}".format(p.pid, " ".join(cmd)))
            p.kill()
            p.wait()
    dbrecon.db_unlock_all(auth,hostname())


################################################################
## Spark support


def spark_load_env(config):
    """To be run on a spark worker. Bring over the environment variables that were stashed in the config file on the Driver node."""
    global REIDENT
    dbrecon.GetConfig().set_config(config)
    dbrecon.set_reident(config['paths']['reident'])
    REIDENT = dbrecon.REIDENT = config['paths']['reident']
    for var in config['environment']['vars'].split(','):
        os.environ[var] = config['environment'][var]

def spark_save_env(config):
    """Save the environment variables in the config file specified  by the config file."""
    for var in config['environment']['vars'].split(','):
        config['environment'][var] = os.getenv(var)
    config['paths']['reident'] = REIDENT

def get_spark_sc():
    if not cspark.spark_running():
        raise RuntimeError("--spark provided but not running under spark")

    # pylint: disable=E0401
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    sc    = spark.sparkContext
    return (spark,sc)

################################################################
## Spark step 3


def spark_output_path(what):
    return os.path.join(os.getenv('DAS_S3ROOT'),
                        f'2010-re/{REIDENT[:-1]}/spark/' + datetime.datetime.now().isoformat()[0:19] + '-' + what)


#
# 21/04/03 23:47:35 WARN YarnSchedulerBackend$YarnSchedulerEndpoint: Requesting driver to remove executor 2 for reason Container killed by YARN for exceeding memory limits.
# 6.1 GB of 6 GB physical memory used. Consider boosting spark.yarn.executor.memoryOverhead or disabling yarn.nodemanager.vmem-check-enabled because of YARN-4714.

def spark_build_lpfile(config_row):
    (config,row) = config_row
    spark_load_env(config)
    stusab = row['stusab'].lower()
    county = row['county']
    # We tried this and it didn't work:
    s3_pandas_synth_lp_files.make_state_county_files(auth, stusab, county, db_start_at_end=True, reident=REIDENT, sf1_vars=dbrecon.sf1_vars())
    #cmd = [sys.executable,'s3_pandas_synth_lp_files.py','--dry_run','--reident',REIDENT,stusab,county]
    #return glob.glob("*") + glob.glob("*/*")
    #return ["cmd="+str(cmd)]
    #subprocess.call(cmd)
    return [(stusab,county)]

def spark_step34(auth, args):
    """Run step 3 or step4 under spark. We ignore files in progress, just get them all."""

    (spark,sc) = get_spark_sc()

    t0 = time.time()
    cmd = f"""
    SELECT stusab, county,MAX(pop100) AS max_tract_pop
    FROM {REIDENT}tracts
    WHERE lp_end is NULL
    GROUP BY stusab, county
    HAVING max_tract_pop {args.pop100}
    LIMIT {args.limit}
    """
    auth = dbrecon.auth()       # need a new auth because the old one can't be serialized and sent to the worker
    rows = DBMySQL.csfr(auth, cmd, asDicts=True)
    print("*** Counties to process: ")

    for row in rows:
        print("*** "," ".join([str(x) for x in row.values()]))

    config = dbrecon.GetConfig().config
    spark_save_env(config)

    # Tuples that contain the config and the row to process
    config_rows = [ (config, row) for row in rows]
    rows_rdd    = sc.parallelize(config_rows).repartition( len(config_rows) // SPARK_TRACTS_PER_PARTITION ).persist()
    results_rdd = rows_rdd.map( spark_build_lpfile ).persist()
    results     = results_rdd.collect()
    if results:
        print("LP Files created:")
        print("\n".join([str(x) for x in results]))
    else:
        print("No LP Files Created")
    print("Elapsed time: ",time.time()-t0)


################################################################
## rescan stuff follows. It can be both local and spark. Spark runs faster, but seems less reliable.


def rescan_row(config_row):
    """Designed to be called from multiprocessing. Validate that the files referenced in the database row are present. If they are not, update the database.
    If they are present, validate them. If they do not validate, update the database.
    """
    (config,row) = config_row
    spark_load_env(config)

    stusab = row['stusab'].lower()
    county = row['county']
    tract  = row['tract']
    logging.info(f"rescan_row {row}")
    lpfilenamegz  = dbrecon.LPFILENAMEGZ(stusab=stusab,county=county,tract=tract)
    solfilenamegz = dbrecon.SOLFILENAMEGZ(stusab=stusab,county=county,tract=tract)

    ret = []
    auth = dbrecon.auth()       # need a new auth because the old one can't be serialized and sent to the worker
    if row['sol_end']:
        if dbrecon.validate_solfile(solfilenamegz):
            DBMySQL.csfr(auth,f"UPDATE {REIDENT}tracts set sol_validated=NOW()  where stusab=%s and county=%s and tract=%s",
                         (stusab,county,tract))
        else:
            logging.warning(f"{solfilenamegz} not existing or too small. Removing.")
            dbrecon.remove_solfile(auth, stusab, county, tract)
            ret.append(solfilenamegz)

    if row['lp_end']:
        if dbrecon.validate_lpfile(lpfilenamegz):
            DBMySQL.csfr(auth,f"UPDATE {REIDENT}tracts set lp_validated=NOW()  where stusab=%s and county=%s and tract=%s",
                         (stusab,county,tract))
        else:
            logging.warning(f"{lpfilenamegz} is not properly terminated. Removing")
            dbrecon.remove_lpfile(auth, stusab, county, tract)
            ret.append(lpfilenamegz)

    del auth                    # be sure its disconnected
    return ret

def rescan(auth, args):
    """Get a list of the tracts that require scanning and check each one."""
    t0 = time.time()
    stusab = args.stusab
    county = args.county

    if args.spark:
        (spark,sc) = get_spark_sc()


        print("================================================================")
        print("================================================================")
        print("================================================================")
        print("================================================================")

        print("Checking spark to see if it's working...")
        d = sc.parallelize(range(10000))
        val = d.reduce(operator.add)
        if val != 49995000:
            raise RuntimeError(f"spark not operational (got {val} wanted 49995000)")

    # Figure out what needs to be processed

    restrict = "AND ((lp_end IS NOT NULL) or (sol_end IS NOT NULL)) "
    cmd = f"SELECT * from {REIDENT}tracts where (pop100>0) {restrict} and ((lp_validated is NULL) OR (sol_validated is NULL)) "
    sqlargs = []
    if stusab:
        cmd += " and (stusab = %s)"
        sqlargs.append(stusab)
        if county:
            cmd += " and (county=%s)"
            sqlargs.append(county)
    cmd += f' LIMIT {args.limit}'
    rows = DBMySQL.csfr(auth, cmd, sqlargs, asDicts='True')

    # This idea borrowed from DAS. Put in the config file all of the information that the worker will need.
    # Move over environment variables in a special section called [environment]
    # Limit rows to
    config = dbrecon.GetConfig().config
    spark_save_env(config)

    # Tuples that contain the config and the row to process
    config_rows = [ (config, row) for row in rows]

    print(f"*** Tracts to rescan: {len(config_rows)}")

    if not args.spark:
        print(f"Processing locally with --j1={args.j1} threads. Expected completion in {len(config_rows)*3/10000} hours")
        with multiprocessing.Pool(args.j1) as p:
            p.map(rescan_row, config_rows)
        print("Elapsed time: ",time.time()-t0)
        return

    print("*** Processing with spark...")
    output_path = spark_output_path('rescan')
    output_path_txt = output_path+".txt"
    print("*** Will write to",output_path)

    # Create an RDD that has all of the rows, and partition it so that there are roughly 5 rows per partition.
    rows_rdd    = sc.parallelize(config_rows).repartition( len(config_rows) // SPARK_TRACTS_PER_PARTITION ).persist()
    results_rdd = rows_rdd.flatMap( rescan_row ).persist()

    # Get the results; they should be small

    #results_rdd.saveAsTextFile(output_path)
    #print("Combining to a single file and saving again")
    #results_1partition = results_rdd.coalesce(1)
    #results_1partition.saveAsTextFile(output_path_txt)

    results = results_rdd.collect()
    if results:
        print("Deleted files:")
        print("\n".join(results))
    else:
        print("No files deleted")

    # This would save the results on S3:
    print("Elapsed time: ",time.time()-t0)
    print("\n\n\n\n")
    print("================================================================")
    print("================================================================")
    print("================================================================")

def clean(auth):
    for root, dirs, files in os.walk( dbrecon.dpath_expand("$ROOT") ):
        for fname in files:
            # Do not clean the CSVs
            path = os.path.join(root, fname)
            sz   = os.path.getsize(path)
            if path.endswith(".csv"):
                continue
            if path.endswith(".csv-done"):
                continue
            if sz < 100:
                print(path,sz)
                m = dbrecon.extract_state_county_tract(fname)
                if m is None:
                    continue
                (stusab,county,tract) = m
                what = "sol" if "sol" in path else "lp"
                DBMySQL.csfr(auth,f"UPDATE {REIDENT}tracts SET {what}_start=NULL,{what}_end=NULL "
                        "where stusab=%s and county=%s and tract=%s",
                        (stusab, county, tract))
                dbrecon.dpath_safe_unlink(path)




if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Maintains a database of reconstruction and schedule "
                             "next work if the CPU load and memory use is not too high." )
    dbrecon.argparse_add_logging(parser)
    parser.add_argument("--testdb",  help="test database connection", action='store_true')
    parser.add_argument("--clean",   help="Look for .lp and .sol files that are too slow and delete them, then remove them from the database", action='store_true')
    parser.add_argument("--maxlp",   help="Never run more than this many LP makers", type=int, default=999)
    parser.add_argument("--set_none_running", help="Update database to indicate there are no outstanding LP files being built or Solutions being solved on this machine.",
                        action='store_true')
    parser.add_argument("--set_none_running-anywhere", help="Run if there are no outstanding LP files being built or solutions being solved on any machine.",
                        action='store_true')
    parser.add_argument("--dry_run", help="Just report what the next thing to run would be, then quit", action='store_true')
    parser.add_argument("--stusab", help="stusab for rescanning", default='all')
    parser.add_argument("--county", help="county for rescanning")
    parser.add_argument("--asc", action='store_false', help="Run least populus tracts first, otherwise do least populus tracts first")
    parser.add_argument("--debug", action='store_true', help="debug mode")
    parser.add_argument("--j1",    help="number of threads for rescan", type=int, default=10)
    parser.add_argument("--spark", help="run under spark", action='store_true')
    parser.add_argument("--rescan",  help="validate and update the database contents against files in the file system. Uses --j1 when not run under spark. SPARK ENABLED.", action='store_true')
    parser.add_argument("--step3", help='Run only step 3. SPARK ENABLED. ', action='store_true')
    parser.add_argument("--step4", help='Run only step 4. SPARK ENABLED', action='store_true')
    parser.add_argument("--step5", help='Attempt to run step 5 when all other jobs complete', action='store_true')
    parser.add_argument("--step6", help='Attempt to run step 6 when all other jobs complete', action='store_true')
    parser.add_argument("--nolock", help="Do not lock the script.", action='store_true')
    parser.add_argument("--pop100", help="for --spark, pecifies the max(pop100) to work with",default=">0")
    parser.add_argument("--limit", help="Specify a limit for the number of tracts to process in --rescan, --step3 or --step4", type=int, default=1000000)
    parser.add_argument("--launch_all", help="Launch all hosts from scheduler", action='store_true')
    parser.add_argument("--branch", help="Launch all hosts from scheduler", default = 'main')
    args   = parser.parse_args()


    config = dbrecon.setup_logging_and_get_config(args=args,prefix='sch_')
    auth = dbrecon.auth()

    if args.j1>50:
        print("Experience has shown that --j1>50 is not useful on a machine with 96 cores. Setting --j1 to 50")
        args.j1 = 50


    args   = parser.parse_args()
    config = dbrecon.setup_logging_and_get_config(args=args,prefix='sch_')
    auth   = dbrecon.auth()

    if args.testdb:
        print("Tables:")
        rows = DBMySQL.csfr(auth,f"show tables")
        for row in rows:
            print(row)
        exit(0)

    if not args.nolock:
        ctools.lock.lock_script( abspath(__file__))

    if args.spark:
        if (args.step3 and args.step4) or (args.rescan and args.step3) or (args.rescan and args.step4):
            logging.error("--spark can only be used with one of --rescan, --step2 or --step3")
            exit(1)
        if args.step3 or args.step4:
            spark_step34(auth, args)
        elif args.rescan:
            rescan(auth, args)
        else:
            logging.error("--spark requires --rescan, --step2 or --step3")
            exit(1)
        exit(0)

    if args.rescan:
        if args.step3 or args.step4:
            logging.error("--rescan conflicts with --step3 and --step4")
            exit(1)
        rescan(auth, args)
        exit(0)

    elif args.clean:
        clean(auth)

    elif args.set_none_running:
        dbrecon.db_unlock_all(auth,hostname())

    elif args.set_none_running_anywhere:
        dbrecon.db_unlock_all(auth, None)

    else:
        kill_running_s3_s4()
        run(auth, args)
