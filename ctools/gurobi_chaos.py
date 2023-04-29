import os
import argparse
import sys
import subprocess
import re
import tempfile
import re
import logging
import pandas as pd
from itertools import repeat
from shutil import copyfile
from concurrent import futures
from time import time

logger = logging.getLogger(__name__)

GUROBI_CL = '/apps/gurobi901/linux64/bin/gurobi_cl'
SERVERS   = os.environ['GRB_TOKENSERVER']
PORT      = os.environ['GRB_TOKENSERVER_PORT']
FALSE_SERVER = 'bad.server'
#GUROBI_LIC_CREATE_FNAME = '/tmp/gurobi_chaos.lic'
GUROBI_LIC_CREATE_FNAME = './gurobi.lic'
#GUROBI_LIC = '/apps/gurobi901/linux64/bin/gurobi.lic'
GRB_APP_NAME = "DAS"
GRB_ISV_NAME = "CENSUS"
GRB_ENV3 = 0
GRB_ENV4 = ""
GUROBI_HOME="/apps/gurobi901/linux64"
PATH=f":{GUROBI_HOME}/bin"
LD_LIBRARY_PATH=f"{GUROBI_HOME}/lib"
GRB_PYTHON=f"{GUROBI_HOME}/lib/python3.6_utf32/"
sys.path.insert(0, GRB_PYTHON)
sys.path.insert(0,f"{GUROBI_HOME}/lib/")
import gurobipy as gb


def multi_token(tokens,servers,gurobi_cl, grb_isv, grb_app, grb_env3, grb_env4):
    logger.debug(servers)
    server_flags = {}
    for s in servers.split(","):
        server_flags[s]=0
    tokens_list = []
    t1 = []
    t2 = []
    for tok in range(tokens):
       *info, env=test_lic(tok, servers, server_flags, gurobi_cl, grb_isv, grb_app, grb_env3, grb_env4)
       t2.append(env)
       t1.append(info)
    if logger.level == logging.DEBUG:
       subprocess.check_call([gurobi_cl, '--tokens'], encoding='utf-8')
    return t1


def create_lic(grb_file, servers, port):
    with tempfile.NamedTemporaryFile(suffix='.lic',mode='w') as tf:
        tf.write(f"TOKENSERVER={servers}\n")
        tf.write(f"PORT={port}\n")
        tf.write("VERBOSE=1\n")
        tf.flush()
        copyfile(tf.name, grb_file)
        logging.info(f"GUROBI LIC FILE: {grb_file}")
    os.environ['GRB_LICENSE_FILE'] = GUROBI_LIC_CREATE_FNAME
#    if 'GRUOBI_HOME' not in os.environ:
#    os.environ['GUROBI_HOME']=GUROBI_HOME
#    os.environ['PATH']+=PATH
#    os.environ['LD_LIBRARY_PATH']=LD_LIBRARY_PATH


def test_lic(tokens, servers, server_flags,gurobi_cl, grb_isv, grb_app, grb_env3, grb_env4):
    s_time = time()
    try:
        env = gb.Env.OtherEnv("", grb_isv, grb_app, grb_env3, grb_env4)
        e_time=time()-s_time
        out = subprocess.check_output([gurobi_cl, '--tokens'], encoding='utf-8')
        server=servers
        current_use = None
        server_all=re.findall("(?<=Gurobi token server )[^.\s\']*.*'",out)
        max_current = re.findall('Maximum allowed uses: (\d+), current: ([^.\s]*\d+)',out)
        for i, mx in enumerate(max_current):
            cserver = server_all[i].strip("'")
            if mx[0]!=mx[1] and server_flags[cserver]==0: 
                if server==servers and server_flags[cserver]==0:
                    server = server_all[i]
                    current_use = mx[1]
                elif server_flags[cserver]==0:
                    server = server_all[i]
                    current_use = mx[1]
                    server_flags[cserver]=1
        return(server, current_use, e_time, None, env)
    except gb.GurobiError as e:
        e_time=time()-s_time
        error_string = parse_e_message(e)
        return(servers, None, e_time, error_string, None)


def parse_e_message(error):
    num = error.errno
    message = error._message
    rstring = f"ErroNum: {num} | {message}"
    return rstring


def summary_stats(data,servers):
    rdata=[]
    if FALSE_SERVER in servers:
        server_fail_TTL = data.iloc[0]['TTL']
    else:
        server_fail_TTL = None
    rdata.append(['server_fail_TTL',server_fail_TTL])
    success_TTL_mean = data.loc[data['Server']!=servers,['TTL']].mean()[0]
    rdata.append(['success_TTL_mean', success_TTL_mean])
    data['prev_server']=data['Server'].shift(1)
    success_TTL_mean_work = data.loc[(data['Error'].isna()) & (data['Server']==data['prev_server']),['TTL']].mean()[0]
    rdata.append(['success_TTL_mean_work', success_TTL_mean_work])
    success_TTL_mean_change = data.loc[(data['Error'].isna()) & (data['Server']!=data['prev_server'])  & (~data['prev_server'].isna()),['TTL']].mean()[0]
    rdata.append(['success_TTL_mean_change', success_TTL_mean_change])
    error_TTL_mean = data.groupby('Error', as_index=False).mean()
    if not error_TTL_mean.empty:
        for row in error_TTL_mean.values.tolist():
            rdata.append(row)
    else:
        rdata.append(['No Token Errors', None])
    rdf = pd.DataFrame(rdata,columns=['stat','value'])
    return rdf, data


def main(args):
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='program that requests a certain number of tokens from gurobi server')
    parser.add_argument("--debug", action="store_true", default = False)
    parser.add_argument("--tokens", type=int, help="tokens number", default = 10)
    parser.add_argument("--servers",  help="gurobi client", default = SERVERS)
    #a bad port currently will stall need to research into why more
    parser.add_argument("--port",  help="gurobi client", default = PORT)
    parser.add_argument("--bad_server", help="turn on bad server check", default = 'True')
    parser.add_argument("--grb_cl",  help="gurobi client", default = GUROBI_CL)
    parser.add_argument("--grb_isv", help="gurobi isv name", default = GRB_ISV_NAME)
    parser.add_argument("--grb_app", help="gurobi app name", default = GRB_APP_NAME)
    parser.add_argument("--grb_env3", help="gurobi env3", default = GRB_ENV3)
    parser.add_argument("--grb_env4", help="gurobi env4", default = GRB_ENV4)
    parser.add_argument("--grb_lic", help="gurobi license file", default = GUROBI_LIC_CREATE_FNAME)
    parser.add_argument("--t_list_out", help="token list outfile csv")
    parser.add_argument("--stats_out", help="stats outfile csv")
    args = parser.parse_args()

    if args.debug == True:
        logger.level = logging.DEBUG
    else:
        logger.level = logging.INFO

    if bool(re.match('t', args.bad_server, re.I)) or  args.bad_server==True:
        servers=FALSE_SERVER+','+args.servers
    else:
        servers = args.servers
    create_lic(args.grb_lic,servers, args.port)
    tokens = multi_token(args.tokens, servers, args.grb_cl, args.grb_isv, args.grb_app, args.grb_env3, args.grb_env4)
    df = pd.DataFrame(tokens, columns = ['Server', 'Current_Usage', 'TTL', 'Error'])
    stat_out, tok_out=summary_stats(df, servers)
    logger.debug(stat_out)
    if args.stats_out:
        stat_out.to_csv(args.stats_out)
    if args.t_list_out:
        tok_out.to_csv(args.t_list_out)


if __name__=="__main__":
    logging.basicConfig(level=logging.DEBUG)
    
    main(sys.argv)
