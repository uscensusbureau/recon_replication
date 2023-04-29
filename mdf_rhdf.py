import pandas as pd
import numpy as np
import logging
from time import perf_counter
import os, psutil
import argparse
import sys
MY_DIR      = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(MY_DIR)
if PARENT_DIR not in sys.path:
    sys.path.append( PARENT_DIR )
import ctools.s3 as s3

start_time = perf_counter()
DEF_MDF = ""
DAS_ROOT = os.environ["DAS_S3ROOT"]
MDF_KEY = ""


def read_table_mdf_per_file(mdf_per_file_path: str):
    logger.info(f" + Loading MDF File: {mdf_per_file_path}")
    df = pd.read_table(f"{mdf_per_file_path}", sep="|", comment="#",
                       usecols=['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLK', 'TABBLKGRPCE', 'RTYPE',
                                'GQTYPE', 'QSEX', 'QAGE', 'CENHISP', 'CENRACE'],
                       dtype={'TABBLKST': 'object',
                              'TABBLKCOU': 'object',
                              'TABTRACTCE': 'object',
                              'TABBLKGRPCE': 'object',
                              'TABBLK': 'object',
                              'RTYPE': 'object',
                              'GQTYPE': 'uint16',
                              'QSEX': 'uint8',
                              'QAGE': 'uint8',
                              'CENHISP': 'uint8',
                              'CENRACE': 'uint8'})
    df.rename(columns=({'CENHISP': 'QSPANX'}),
              inplace=True,
              )
    logger.info(f' + mdfper {len(df)}')
    return df


def np_conditions_apply(df: pd.DataFrame, conditions: list, values: list, var: str, default=None):
    _conditions = conditions
    _values = values
    _default = default
    df[var] = np.select(_conditions, _values, default=_default)

def process_read_mdf(mdf: pd.DataFrame):
    hispanic_conditions = [(mdf['QSPANX'] == 1), (mdf['QSPANX'] > 1)]
    hispanic_values = ['N', 'Y']
    np_conditions_apply(mdf, hispanic_conditions, hispanic_values, "hisp", 'N')

    race_values = ['Y']
    race_default = 'N'

    race_white_conditions = [(mdf["CENRACE"].isin(
        [1, 7, 8, 9, 10, 11, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 57, 58, 59,
         60, 61, 63]))]
    np_conditions_apply(mdf, race_white_conditions, race_values, "white", race_default)

    race_black_conditions = [(mdf["CENRACE"].isin(
        [2, 7, 12, 13, 14, 15, 22, 23, 24, 25, 32, 33, 34, 35, 36, 37, 42, 43, 44, 45, 46, 47, 52, 53, 54, 55, 57, 58,
         59, 60, 62, 63]))]
    np_conditions_apply(mdf, race_black_conditions, race_values, "black", race_default)

    race_aian_conditions = [(mdf["CENRACE"].isin(
        [3, 8, 12, 16, 17, 18, 22, 26, 27, 28, 32, 33, 34, 38, 39, 40, 42, 43, 44, 48, 49, 50, 52, 53, 54, 56, 57, 58,
         59, 61, 62, 63]))]
    np_conditions_apply(mdf, race_aian_conditions, race_values, "aian", race_default)

    race_asian_conditions = [(mdf["CENRACE"].isin(
        [4, 9, 13, 16, 19, 20, 23, 26, 29, 30, 32, 35, 36, 38, 39, 41, 42, 45, 46, 48, 49, 51, 52, 53, 55, 56, 57, 58,
         60, 61, 62, 63]))]
    np_conditions_apply(mdf, race_asian_conditions, race_values, "asian", race_default)

    race_nhpi_conditions = [(mdf["CENRACE"].isin(
        [5, 10, 14, 17, 19, 21, 24, 27, 29, 31, 33, 35, 37, 38, 40, 41, 43, 45, 47, 48, 50, 51, 52, 54, 55, 56, 57, 59,
         60, 61, 62, 63]))]
    np_conditions_apply(mdf, race_nhpi_conditions, race_values, "nhopi", race_default)

    race_sor_conditions = [(mdf["CENRACE"].isin(
        [6, 11, 15, 18, 20, 21, 25, 28, 30, 31, 34, 36, 37, 39, 40, 41, 44, 46, 47, 49, 50, 51, 53, 54, 55, 56, 58, 59,
         60, 61, 62, 63]))]
    np_conditions_apply(mdf, race_sor_conditions, race_values, "sor", race_default)

    mdf["age"] = mdf["QAGE"]

    sex_conditions =[(mdf["QSEX"] == 1), (mdf["QSEX"] == 2)]
    sex_values = ['male', 'female']
    np_conditions_apply(mdf, sex_conditions, sex_values, "sex")

    mdf['geoid_tract'] = mdf['TABBLKST'].astype('str') + mdf['TABBLKCOU'].astype('str') + mdf['TABTRACTCE'].astype('str')
    mdf['geoid_block'] = mdf['TABBLKST'].astype('str') + mdf['TABBLKCOU'].astype('str') + mdf['TABTRACTCE'].astype('str')+ mdf['TABBLK'].astype('str')

    mdf = mdf[['geoid_tract', 'geoid_block', 'sex', 'age', 'white', 'black', 'aian', 'asian', 'nhopi', 'sor', 'hisp']]
    logger.debug(f" + rHDFper {len(mdf)}")

    process = psutil.Process(os.getpid())
    logger.debug(process.memory_info().rss)  # in bytes
    return mdf

def main(mdf_file, s3_bucket, mdf_key):
    if not os.path.exists(mdf_file):
        bucket, key = s3.get_bucket_key(s3_bucket + mdf_key)
        s3.get_object(bucket, key, mdf_file)
    mdf = read_table_mdf_per_file(mdf_file)
    rhdf = process_read_mdf(mdf)
    rhdf.to_csv(f"./rhdf.csv", index=False)

    end_time = perf_counter()
    logger.info(f"{end_time - start_time:0.4f} seconds")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-mf', '--mdf_file', default=DEF_MDF)
    parser.add_argument('-s3', '--s3_bucket', default=DAS_ROOT)
    parser.add_argument('-mdfk', '--mdf_key', default=MDF_KEY)
    parser.add_argument('-debug', action='store_true', help='Debug')
    args = parser.parse_args()

    logging.basicConfig(format='[%(filename)s:%(lineno)d] %(message)s', )
    logger = logging.getLogger()
    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    main(args.mdf_file, args.s3_bucket, args.mdf_key)
