#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)
''' Convert Recon/Reid format CEF data back into format needed for suppression metrics '''
import os
import re
import pandas as pd
from itertools import product
from operator import itemgetter
from multiprocessing import Pool

#DTYPES for rhdf file
DTYPES = {'geoid_block': 'str',
          'age': 'int',
          'white': 'str',
          'black': 'str',
          'aian': 'str',
          'asian': 'str',
          'nhopi': 'str',
          'sor': 'str',
          'hisp': 'str'}

#Dictionary of race code sets
RACE_DICT = {
             'WHITE': {'01', '07', '08', '09', '10', '11', '22', '23', '24', '25', '26',
                        '27', '28', '29', '30', '31', '42', '43', '44', '45', '46', '47', '48',
                        '49', '50', '51', '57', '58', '59', '60', '61', '63'},
             'BLACK': {'02', '07', '12', '13', '14', '15', '22', '23', '24', '25', '32',
                        '33', '34', '35', '36', '37', '42', '43', '44', '45', '46', '47', '52',
                        '53', '54', '55', '57', '58', '59', '60', '62', '63'},
             'AIAN': {'03', '08', '12', '16', '17', '18', '22', '26', '27', '28', '32', '33',
                       '34', '38', '39', '40', '42', '43', '44', '48', '49', '50', '52', '53',
                       '54', '56', '57', '58', '59', '61', '62', '63'},
             'ASIAN': {'04', '09', '13', '16', '19', '20', '23', '26', '29', '30', '32',
                        '35', '36', '38', '39', '41', '42', '45', '46', '48', '49', '51', '52',
                        '53', '55', '56', '57', '58', '60', '61', '62', '63'},
             'NHOPI': {'05', '10', '14', '17', '19', '21', '24', '27', '29', '31', '33',
                        '35', '37', '38', '40', '41', '43', '45', '47', '48', '50', '51', '52',
                        '54', '55', '56', '57', '59', '60', '61', '62', '63'},
             'SOR': {'06', '11', '15', '18', '20', '21', '25', '28', '30', '31', '34', '36',
                      '37', '39', '40', '41', '44', '46', '47', '49', '50', '51', '53', '54',
                      '55', '56', '58', '59', '60', '61', '62', '63'}
            }

RACE_ORDER = ['WHITE', 'BLACK', 'AIAN', 'ASIAN', 'NHOPI', 'SOR']

def make_cenrace(race_dict, race_order):
    ''' function to define map of rhdf race categories back into cenrace values
       e.g. YNNNNN -> 01 #white only
       
       returns: a dictionary of form {'XXXXXX': 'YY', ...} giving the mapping
    '''
    ret = {}
    possibles = [''.join(x) for x in list(product('YN', repeat=6))]
    for possible in possibles:
        #location of yesses
        Ylocs = [pos for pos, char in enumerate(possible) if char=='Y']
        #location of noes
        Nlocs = [pos for pos, char in enumerate(possible) if char=='N']
        Yraces = [race_order[x] for x in Ylocs]        
        Nraces = [race_order[x] for x in Nlocs]        
        Yintersection = set()
        Nunion = set()
        if(len(Yraces) > 0):
            Ysets = itemgetter(*Yraces)(race_dict)
            Yintersection = Ysets
            if(len(Yraces) > 1):
                Yintersection = set.intersection(*Ysets)
        if(len(Nraces) > 0):
            Nsets = itemgetter(*Nraces)(race_dict)
            Nunion = Nsets
            if(len(Nraces) > 1):
                Nunion = set.union(*Nsets)
        #Category we need must satisfy all the Y values and none of the N values
        rcat = list(Yintersection - Nunion)
        if(len(rcat) > 0):
            ret[possible] = int(rcat[0])
    return(ret)

RACE_MAP = make_cenrace(RACE_DICT, RACE_ORDER)

RCAT_MAP = {1: 1, 2: 2, 7: 2, 3: 3, 8: 3, 4: 4, 9: 4, 5: 5, 10: 5, 6: 6, 11: 6}
RCAT_MAP.update(dict.fromkeys(list(range(12, 64)), 7))

AGE18_MAP = {x: int(x >= 18) for x in range(0, 116)}

def process_file(rhdf_path, outname, isCEF=False):
    print(f'Reading {rhdf_path}')
    rHDF = pd.read_csv(rhdf_path, dtype=DTYPES, usecols=DTYPES.keys())
    #column manipulation for r&r cef file
    if(isCEF):
        rHDF.white.replace({'0': 'N', '1': 'Y'}, inplace=True)
        rHDF.black.replace({'0': 'N', '1': 'Y'}, inplace=True)
        rHDF.aian.replace({'0': 'N', '1': 'Y'}, inplace=True)
        rHDF.asian.replace({'0': 'N', '1': 'Y'}, inplace=True)
        rHDF.nhopi.replace({'0': 'N', '1': 'Y'}, inplace=True)
        rHDF.sor.replace({'0': 'N', '1': 'Y'}, inplace=True)
        rHDF.hisp.replace({'0': 'N', '1': 'Y'}, inplace=True)
    rHDF['allrace'] = rHDF['white'] + rHDF['black'] + rHDF['aian'] + rHDF['asian'] + rHDF['nhopi'] + rHDF['sor']
    rHDF['CENRACE'] = rHDF.allrace.replace(RACE_MAP)
    rHDF['race'] = rHDF.CENRACE.replace(RCAT_MAP)
    rHDF['state'] = rHDF.geoid_block.str[:2]
    rHDF['county'] = rHDF.geoid_block.str[2:5]
    rHDF['tract'] = rHDF.geoid_block.str[5:11]
    rHDF['blkgrp'] = rHDF.geoid_block.str[11:12]
    rHDF['block'] = rHDF.geoid_block.str[11:]
    rHDF['age18'] = rHDF.age.replace(AGE18_MAP)
    rHDF['his'] = rHDF.hisp.replace({'N': '0', 'Y': '1'})
    rHDF['nation'] = 1
    
    OUTCOLS = ['race', 'nation', 'state', 'county', 'tract', 'blkgrp', 'block', 'age18', 'his']
    if outname is not None: rHDF[OUTCOLS].to_csv(outname, index=False)

    return(rHDF[OUTCOLS])

def mapper(x):
    return(process_file(x, None, True))

if __name__ == '__main__':
    cefdir = '../data/reid_module/cef/'
    cef_files = [cefdir + f for f in os.listdir(cefdir) if re.search(r'cef\d{5}[.]csv', f) if not 'cef72' in f]
    p = Pool(25)
    ceflist = p.map(mapper, cef_files)
    print('Concatenating and writing full CEF file')
    pd.concat(ceflist).to_csv('pcef.csv', index=False)
