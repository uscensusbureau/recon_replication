#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)

'''
Given a file in 2010 HDF format, convert it to 2010 reconstructed HDF format.
Part of the replication archive for The U.S. Census Bureau's Ex Post
Confidentiality Analysis of the 2010 Census Data Publications
(https://github.com/uscensusbureau/recon_replication)
'''

import argparse

DEF_RHDF='rhdf.csv'
DEF_MDF='./out.mdf'

S_TO_B = {'male':1, 'female':2}
L_TO_B = {'Y':2, 'N':1}
S_TO_C = {
    "Y,N,N,N,N,N":1,
"N,Y,N,N,N,N":2,
"N,N,Y,N,N,N":3,
"N,N,N,Y,N,N":4,
"N,N,N,N,Y,N":5,
"N,N,N,N,N,Y":6,
"Y,Y,N,N,N,N":7,
"Y,N,Y,N,N,N":8,
"Y,N,N,Y,N,N":9,
"Y,N,N,N,Y,N":10,
"Y,N,N,N,N,Y":11,
"N,Y,Y,N,N,N":12,
"N,Y,N,Y,N,N":13,
"N,Y,N,N,Y,N":14,
"N,Y,N,N,N,Y":15,
"N,N,Y,Y,N,N":16,
"N,N,Y,N,Y,N":17,
"N,N,Y,N,N,Y":18,
"N,N,N,Y,Y,N":19,
"N,N,N,Y,N,Y":20,
"N,N,N,N,Y,Y":21,
"Y,Y,Y,N,N,N":22,
"Y,Y,N,Y,N,N":23,
"Y,Y,N,N,Y,N":24,
"Y,Y,N,N,N,Y":25,
"Y,N,Y,Y,N,N":26,
"Y,N,Y,N,Y,N":27,
"Y,N,Y,N,N,Y":28,
"Y,N,N,Y,Y,N":29,
"Y,N,N,Y,N,Y":30,
"Y,N,N,N,Y,Y":31,
"N,Y,Y,Y,N,N":32,
"N,Y,Y,N,Y,N":33,
"N,Y,Y,N,N,Y":34,
"N,Y,N,Y,Y,N":35,
"N,Y,N,Y,N,Y":36,
"N,Y,N,N,Y,Y":37,
"N,N,Y,Y,Y,N":38,
"N,N,Y,Y,N,Y":39,
"N,N,Y,N,Y,Y":40,
"N,N,N,Y,Y,Y":41,
"Y,Y,Y,Y,N,N":42,
"Y,Y,Y,N,Y,N":43,
"Y,Y,Y,N,N,Y":44,
"Y,Y,N,Y,Y,N":45,
"Y,Y,N,Y,N,Y":46,
"Y,Y,N,N,Y,Y":47,
"Y,N,Y,Y,Y,N":48,
"Y,N,Y,Y,N,Y":49,
"Y,N,Y,N,Y,Y":50,
"Y,N,N,Y,Y,Y":51,
"N,Y,Y,Y,Y,N":52,
"N,Y,Y,Y,N,Y":53,
"N,Y,Y,N,Y,Y":54,
"N,Y,N,Y,Y,Y":55,
"N,N,Y,Y,Y,Y":56,
"Y,Y,Y,Y,Y,N":57,
"Y,Y,Y,Y,N,Y":58,
"Y,Y,Y,N,Y,Y":59,
"Y,Y,N,Y,Y,Y":60,
"Y,N,Y,Y,Y,Y":61,
"N,Y,Y,Y,Y,Y":62,
"Y,Y,Y,Y,Y,Y":63
}

def rhdf_translate(rhdf, outfile):
    with open(rhdf, 'r') as rfile, open(outfile,'w') as out:
        for i, line in enumerate(rfile):
            if i == 0:
                out.write('TABBLKST|TABBLKCOU|TABTRACTCE|TABBLKGRPCE|TABBLK|QSEX|QAGE|RTYPE|GQTYPE|CENHISP|CENRACE\n')
            elif i > 1:
                TABBLKST = line[:2]
                TABBLKCOU =line[2:5]
                TABTRACTCE = line[5:11]
                TABBLKGRPCE = line[23:24]
                TABBLK = line[23:27]
                REST = line[28:].split(',')
                QSEX = S_TO_B[REST[0]]
                QAGE = REST[1]
                GQTYPE = '000'
                RTYPE = '1'
                CENHISP = L_TO_B[REST[8].strip()]
                CENRACE = str(S_TO_C[','.join([REST[2],REST[3],REST[4],REST[5],REST[6],REST[7]])]).zfill(2)
                # print(line, TABBLKST, TABBLKCOU, TABTRACTCE, TABBLKGRPCE, TABBLK, QSEX, QAGE, CENHISP, CENRACE)
                out.write(f'{TABBLKST}|{TABBLKCOU}|{TABTRACTCE}|{TABBLKGRPCE}|{TABBLK}|{QSEX}|{QAGE}|{RTYPE}|{GQTYPE}|{CENHISP}|{CENRACE}\n')


def main(rhdf, out_file):
    rhdf_translate(rhdf, out_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-rhdf', '--rhdf_file', default=DEF_RHDF)
    parser.add_argument('-omf', '--out_mdf_file', default=DEF_MDF)
    args = parser.parse_args()
    main(args.rhdf_file, args.out_mdf_file)
