'''Driver program for 2010 swap experiments'''

import sys
import os
import subprocess
from state_list import STATE_LIST
from multiprocessing import Pool

#Swap experiments to run
HI = {'target': 0.50,
      'prob_cutoff1':  0.50, 'prob_cutoff2': 0.75,
      'pprop1': 0.30, 'pprop2': 0.40}

LO = {'target': 0.05,
      'prob_cutoff1':  1.00, 'prob_cutoff2': 1.00,
      'pprop1': 1.00, 'pprop2': 0.00}

def run_state(st_list, target, prob_cutoff1, prob_cutoff2, pprop1, pprop2):
    print(st_list)
    os.environ['env_stabbr'] = st_list[1]
    os.environ['env_stfips'] = st_list[2]
    os.environ['env_stname'] = st_list[1] + st_list[2]
    os.environ['env_target'] = str(target)
    os.environ['env_prob_cutoff1'] = str(prob_cutoff1)
    os.environ['env_prob_cutoff2'] = str(prob_cutoff2)
    os.environ['env_pprop1'] = str(pprop1)
    os.environ['env_pprop2'] = str(pprop2)
    print('Swapping ' + st_list[0])
    subprocess.run(['run_sas', '-nodms', 'swap_pairs.sas'], check=False)
    return(['swap00' + st_list[1] + '.dat',
        'sum00' + st_list[1] + '.txt'])


if __name__ == "__main__":
    os.makedirs('./HI', exist_ok = True)
    os.makedirs('./LO', exist_ok = True)
    hi_arg_dict_list = []
    lo_arg_dict_list = []
    for st_list in STATE_LIST:
        hexcp = HI.copy()
        lexcp = LO.copy()
        hexcp.update({'st_list': st_list})
        lexcp.update({'st_list': st_list})
        hi_arg_dict_list.append(hexcp)
        lo_arg_dict_list.append(lexcp)

    #SAS code doesn't parallelize, so just run sequentially
    for arglist in lo_arg_dict_list:
        lofiles = run_state(**arglist)
        [os.rename(x, os.path.join('LO', x)) for x in lofiles]

    for arglist in hi_arg_dict_list:
        hifiles = run_state(**arglist)
        [os.rename(x, os.path.join('HI', x)) for x in hifiles]
