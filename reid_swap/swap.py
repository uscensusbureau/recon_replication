'''Take swap pairs list and perform swap on recon CEF file
   Can definitely set this up to parallelize states'''

import csv
import pandas as pd
from multiprocessing import Pool
from state_list import STATE_LIST
import time

#These vars need to swap between records
#the rest stay the same
#this needs to work for varying records in the 0 and 1 households
#SWAP_VARS = ('control', 'tabblkst', 'tabblkcou', 'tabtractce', 'tabblk')
SWAP_VARS = ('hid', 'tabblkst', 'tabblkcou', 'tabtractce', 'tabblk')

def state_pair_list(st_swap_path):
    with open(st_swap_path, 'r') as pair_file:
        pairs = pair_file.readlines()
    swap_pairs = [{'control0': pair[3:12], 'control1': pair[15:24]} for pair in pairs]
    return(swap_pairs)

def read_state_records(fips, pcef_file):
    state_records = []
    with open(pcef_file, 'r') as pfile:
        dict_reader = csv.DictReader(pfile)
        for row in dict_reader:
            if row['tabblkst']==fips:
                row['hid'] = row['control']
                state_records.append(row)
    return(state_records)

#Swaps HUs now, so makes assumption only 1 line per HU
def swap_state(state_records, state_pairs, swap_vars=SWAP_VARS, stabbr=None):
#    state_df = pd.DataFrame(state_records, dtype='str')
    state_df = pd.DataFrame(state_records, dtype='str').set_index('control')
    sdx = 0
    tot = len(state_pairs)
    whl = 1
    for pair in state_pairs:
        val0 = state_df.loc[pair['control0'], list(SWAP_VARS)].values
        val1 = state_df.loc[pair['control1'], list(SWAP_VARS)].values
        state_df.loc[pair['control0'], list(SWAP_VARS)] = val1
        state_df.loc[pair['control1'], list(SWAP_VARS)] = val0
        sdx += 1
        pct = 100*sdx/tot
        if stabbr is not None and pct > whl:
            print(f'{stabbr} has {whl}% swapped')
            whl += 1
    return(state_df)


def run_all(state_list, swap_vars=SWAP_VARS):
    stname = state_list[0]
    stabbr = state_list[1]
    stfips = state_list[2]
    experiment = state_list[3]
    hcef_file = state_list[4]
    pcef_file = state_list[5]
    hifile = f'{experiment}/swap00{stabbr}.dat'
    pairs = state_pair_list(hifile)
    print(f'{experiment}: Subsetting CEF to {stname}')
    records = read_state_records(stfips, hcef_file)
    print(f'{experiment}: Swapping {stname}')
    swapped = swap_state(records, pairs, stabbr=stname)
    persons = pd.DataFrame(read_state_records(stfips, pcef_file), dtype='str').set_index('control')
    #merge persons on control to swap on hid
    print(f"{experiment} {stname}: Merging persons onto units")
    out = pd.merge(persons,
                   swapped.drop(columns=['household_size', 'ten']),
                   left_index=True, right_index=True, how='left', suffixes=('_P', '_H'))
    #Handle GQs not being on HU file
    out['tabblkst'] = out['tabblkst_H']
    out['tabblkcou'] = out['tabblkcou_H']
    out['tabtractce'] = out['tabtractce_H']
    out['tabblk'] = out['tabblk_H']
    out['hid'] = out['hid_H']

    wch = out['tabblkst'].isna()
    out['tabblkst'][wch] = out['tabblkst_P'][wch]
    wch = out['tabblkcou'].isna()
    out['tabblkcou'][wch] = out['tabblkcou_P'][wch]
    wch = out['tabtractce'].isna()
    out['tabtractce'][wch] = out['tabtractce_P'][wch]
    wch = out['tabblk'].isna()
    out['tabblk'][wch] = out['tabblk_P'][wch]

    rec_file = f'{experiment}/swapped_{stabbr}.csv'
#    write_records(out[persons.columns], rec_file)
    out[persons.columns].drop(columns=['hid']).to_csv(rec_file, index=False)
    print(f'{experiment}: Finished {experiment} {stname}')
    return(rec_file)

def merge_experiment(exp_result, merge_file_path):
    with open(merge_file_path, "wb") as fout:
            # First file:
        with open(exp_result[0], "rb") as f:
            fout.writelines(f)
    with open(merge_file_path, "ab") as fout:
        # Now the rest:
        for rest in exp_result[1:]:
            with open(rest, "rb") as f:
                next(f) # Skip the header, portably
                fout.writelines(f)

if __name__ == '__main__':
    hi_list = [x + ['HI', 'swap_hcef.csv', 'swap_pcef.csv'] for x in STATE_LIST]
    lo_list = [x + ['LO', 'swap_hcef.csv', 'swap_pcef.csv'] for x in STATE_LIST]
    lo_list = [lo_list[x] for x in [1, 50]]
    hi_list = [hi_list[x] for x in [1, 50]]
    with Pool(processes=26) as pool:
        lo_res = pool.map(run_all, lo_list)
        hi_res = pool.map(run_all, hi_list)
    merge_experiment(lo_res, "LO/swapped_us.csv")
    merge_experiment(hi_res, "HI/swapped_us.csv")
