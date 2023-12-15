''' Convert Recon/Reid format data back into format needed for metrics.
    Mainly this consists of recoding the individual race variables into
    the 63 category version. '''
from itertools import product
from operator import itemgetter
import pandas as pd

#DTYPES for rhdf file
DTYPES = {'geoid_block': 'str',
          'sex': 'str',
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

def process_file(rhdf_path, outname, isCEF=False):
    ''' Function to process an input reconstruction-format file
        into the metrics format. Certain variables expected in the
        metrics file, such as relationship, do not exist in the
        reconstructed files. These are filled in with stub values
        to allow for processing of metrics on existing variables'''
    print(f'Reading {rhdf_path}')
    rHDF = pd.read_csv(rhdf_path, dtype=DTYPES, usecols=DTYPES.keys())
    print('Formatting Data')
    #column manipulation for r&r cef file
    if(isCEF):
        print('Formatting CEF')
        print('Formatting white')
        rHDF.white.replace({'0': 'N', '1': 'Y'}, inplace=True)
        print('Formatting black')
        rHDF.black.replace({'0': 'N', '1': 'Y'}, inplace=True)
        print('Formatting aian')
        rHDF.aian.replace({'0': 'N', '1': 'Y'}, inplace=True)
        print('Formatting asian')
        rHDF.asian.replace({'0': 'N', '1': 'Y'}, inplace=True)
        print('Formatting nhopi')
        rHDF.nhopi.replace({'0': 'N', '1': 'Y'}, inplace=True)
        print('Formatting sor')
        rHDF.sor.replace({'0': 'N', '1': 'Y'}, inplace=True)
        print('Formatting hisp')
        rHDF.hisp.replace({'0': 'N', '1': 'Y'}, inplace=True)
        print('Formatting sex')
        rHDF.sex.replace({'0': 'female', '1': 'male'}, inplace=True)
    print('Formatting race')
    rHDF['race'] = rHDF['white'] + rHDF['black'] + rHDF['aian'] + rHDF['asian'] + rHDF['nhopi'] + rHDF['sor']
    print('Formatting CENRACE')
    rHDF['CENRACE'] = rHDF.race.replace(RACE_MAP)
    print('Formatting TABBLKST')
    rHDF['TABBLKST'] = rHDF.geoid_block.str[:2]
    print('Formatting TABBLKCOU')
    rHDF['TABBLKCOU'] = rHDF.geoid_block.str[2:5]
    print('Formatting TABTRACTCE')
    rHDF['TABTRACTCE'] = rHDF.geoid_block.str[5:11]
    print('Formatting TABLBLKGRPCE')
    rHDF['TABBLKGRPCE'] = rHDF.geoid_block.str[11:12]
    print('Formatting TABBLK')
    rHDF['TABBLK'] = rHDF.geoid_block.str[11:]
    print('Formatting QAGE')
    rHDF['QAGE'] = rHDF.age
    print('Formatting CENHISP')
    rHDF['CENHISP'] = rHDF.hisp.replace({'N': '1', 'Y': '2'})
    print('Formatting QSEX')
    rHDF['QSEX'] = rHDF.sex.replace({'male': '1', 'female': '2'})
    print('Formatting RELSHIP')
    rHDF['RELSHIP'] = 999
    print('Formatting RTYPE')
    rHDF['RTYPE'] = 'STUB'
    print('Formatting GQTYPE')
    rHDF['GQTYPE'] = 999
    
    OUTCOLS = ['CENRACE', 'TABBLKST', 'RELSHIP', 'RTYPE', 'CENHISP', 'TABBLKGRPCE', 'QSEX', 'TABBLK', 'QAGE', 'TABTRACTCE', 'GQTYPE', 'TABBLKCOU']
    print(f'Outputting {outname}')
    if(isCEF):
        rHDF[OUTCOLS].rename({'relship': 'qrel'}, axis='columns').to_csv(outname, index=False)
    else:
        rHDF[OUTCOLS].to_csv(outname, index=False)
    return(True)

if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser(description='Converts files in reconstructed data format into format needed for accuracy metric calculations')
    parser.add_argument('--infile', type=str, help='File to convert')
    parser.add_argument('--outfile', type=str, help='File to save')
    parser.add_argument('--cef', action='store_true', help='CSV is the CEF file, which has different naming conventions for metrics')

    args = parser.parse_args()
    process_file(args.infile, args.outfile, isCEF=args.cef)
