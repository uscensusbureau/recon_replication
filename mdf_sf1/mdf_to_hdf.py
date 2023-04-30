#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)

'''
Given a Microdata Detail File (MDF) on 2010 Census data created by the
2020 Disclosure Avoidance System (DAS), convert the file to the
Hundred Percent Detail File (HDF) format needed for table creation.

Part of the replication archive for The U.S. Census Bureau's Ex Post
Confidentiality Analysis of the 2010 Census Data Publications
(https://github.com/uscensusbureau/recon_replication)
'''

#!/usr/bin/env python3
import pandas as pd
import numpy as np
import logging
from time import perf_counter
import os, psutil
import json
import argparse
import sys
import multiprocessing
from functools import partial
MY_DIR      = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(MY_DIR)
if PARENT_DIR not in sys.path:
    sys.path.append( PARENT_DIR )
import ctools.s3 as s3

start_time = perf_counter()
CHUNK_SIZE = 50
DEF_MDF = "us_dhcp_datafile-DHCP2020_MDFRevisedColumnNames.txt"
DEF_RENAME = "mdf_rename1.json"
DEF_GEO = ['TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE', 'TABBLK']
DAS_ROOT = os.environ["DAS_S3ROOT"]
MDF_KEY = "/users/cumin002/dhcp_experiments/test_20220118/us_dhcp_datafile-DHCP2020_MDFRevisedColumnNames.txt"
DEF_MP = 1

RACE_LIST = ["aian", "asian", "black", "nhpi", "sor", "white"]
AGE_VALUES = [16, 18, 21, 22, 62, 65, 67]
num2words = {1: 'one', 2: 'two', 3: 'three', 4: 'four', 5: 'five', 6: 'six'}
ST_CODES = {'53': 'WA', '10': 'DE', '11': 'DC', '55': 'WI', '54': 'WV', '15': 'HI', '12': 'FL', '56': 'WY',
               '72': 'PR', '34': 'NJ', '35': 'NM', '48': 'TX', '22': 'LA', '37': 'NC', '38': 'ND', '31': 'NE',
               '47': 'TN', '36': 'NY', '42': 'PA', '02': 'AK', '32': 'NV', '33': 'NH', '51': 'VA', '08': 'CO',
               '06': 'CA', '01': 'AL', '05': 'AR', '50': 'VT', '17': 'IL', '13': 'GA', '18': 'IN', '19': 'IA',
               '25': 'MA', '04': 'AZ', '16': 'ID', '09': 'CT', '23': 'ME', '24': 'MD', '40': 'OK', '39': 'OH',
               '49': 'UT', '29': 'MO', '27': 'MN', '26': 'MI', '44': 'RI', '20': 'KS', '30': 'MT', '28': 'MS',
               '45': 'SC', '21': 'KY', '41': 'OR', '46': 'SD'}
race_dict = {"white": 1, "black": 2, "aian": 3, "asian": 4, "nhpi": 5, "sor": 6}


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
    df[var] = np.select(_conditions, _values, default=_default).astype('int8')

def process_read_mdf(mdf: pd.DataFrame):
    hispanic_conditions = [(mdf['QSPANX'] == 1), (mdf['QSPANX'] > 1)]
    hispanic_values = [0, 1]
    np_conditions_apply(mdf, hispanic_conditions, hispanic_values, "hispanic", 0)

    non_hispanic_conditions = [(mdf['hispanic'] == 0)]
    non_hispanic_values = [1]
    np_conditions_apply(mdf, non_hispanic_conditions, non_hispanic_values, "non_hispanic", 0)

    number_of_races_conditions = [(mdf["CENRACE"] > 6)]
    number_of_races_values = [1]
    np_conditions_apply(mdf, number_of_races_conditions, number_of_races_values, "race_multiple", 0)

    race_values = [1]
    race_default = 0

    race_white_conditions = [(mdf["CENRACE"].isin(
        [1, 7, 8, 9, 10, 11, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 57, 58, 59,
         60, 61, 63]))]
    np_conditions_apply(mdf, race_white_conditions, race_values, "race_white", race_default)

    race_black_conditions = [(mdf["CENRACE"].isin(
        [2, 7, 12, 13, 14, 15, 22, 23, 24, 25, 32, 33, 34, 35, 36, 37, 42, 43, 44, 45, 46, 47, 52, 53, 54, 55, 57, 58,
         59, 60, 62, 63]))]
    np_conditions_apply(mdf, race_black_conditions, race_values, "race_black", race_default)

    race_aian_conditions = [(mdf["CENRACE"].isin(
        [3, 8, 12, 16, 17, 18, 22, 26, 27, 28, 32, 33, 34, 38, 39, 40, 42, 43, 44, 48, 49, 50, 52, 53, 54, 56, 57, 58,
         59, 61, 62, 63]))]
    np_conditions_apply(mdf, race_aian_conditions, race_values, "race_aian", race_default)

    race_asian_conditions = [(mdf["CENRACE"].isin(
        [4, 9, 13, 16, 19, 20, 23, 26, 29, 30, 32, 35, 36, 38, 39, 41, 42, 45, 46, 48, 49, 51, 52, 53, 55, 56, 57, 58,
         60, 61, 62, 63]))]
    np_conditions_apply(mdf, race_asian_conditions, race_values, "race_asian", race_default)

    race_nhpi_conditions = [(mdf["CENRACE"].isin(
        [5, 10, 14, 17, 19, 21, 24, 27, 29, 31, 33, 35, 37, 38, 40, 41, 43, 45, 47, 48, 50, 51, 52, 54, 55, 56, 57, 59,
         60, 61, 62, 63]))]
    np_conditions_apply(mdf, race_nhpi_conditions, race_values, "race_nhpi", race_default)

    race_sor_conditions = [(mdf["CENRACE"].isin(
        [6, 11, 15, 18, 20, 21, 25, 28, 30, 31, 34, 36, 37, 39, 40, 41, 44, 46, 47, 49, 50, 51, 53, 54, 55, 56, 58, 59,
         60, 61, 62, 63]))]
    np_conditions_apply(mdf, race_sor_conditions, race_values, "race_sor", race_default)

    race_one_race_conditions = [(mdf["CENRACE"].between(1, 6))]
    np_conditions_apply(mdf, race_one_race_conditions, race_values, "one_race", race_default)

    race_two_race_conditions = [(mdf["CENRACE"].between(7, 21))]
    np_conditions_apply(mdf, race_two_race_conditions, race_values, "two_race", race_default)

    race_three_race_conditions = [(mdf["CENRACE"].between(22, 41))]
    np_conditions_apply(mdf, race_three_race_conditions, race_values, "three_race", race_default)

    race_four_race_conditions = [(mdf["CENRACE"].between(42, 56))]
    np_conditions_apply(mdf, race_four_race_conditions, race_values, "four_race", race_default)

    race_five_race_conditions = [(mdf["CENRACE"].between(57, 62))]
    np_conditions_apply(mdf, race_five_race_conditions, race_values, "five_race", race_default)

    race_six_race_conditions = [(mdf["CENRACE"] == 63)]
    np_conditions_apply(mdf, race_six_race_conditions, race_values, "six_race", race_default)

    mdf["races_total"] = mdf["race_aian"] + mdf["race_asian"] + mdf["race_black"] + mdf["race_nhpi"] + mdf["race_sor"] + \
                         mdf["race_white"]

    np_conditions_apply(mdf, [mdf["hispanic"] == 1], [mdf["races_total"]], "races_total_hispanic", 0)
    np_conditions_apply(mdf, [mdf["non_hispanic"] == 1], [mdf["races_total"]], "races_total_nonhispanic", 0)

    for race in RACE_LIST + ["multiple"]:
        np_conditions_apply(mdf, [(mdf["hispanic"] == 0) & (mdf[f"race_{race}"] == 1)], [1], f"{race}_nonhispanic", 0)
        np_conditions_apply(mdf, [(mdf["hispanic"] == 1) & (mdf[f"race_{race}"] == 1)], [1], f"{race}_hispanic", 0)
        np_conditions_apply(mdf, [(mdf[f"{race}_nonhispanic"] == 1)], [mdf["races_total"]], f"races_total_{race}nonh",
                            0)
        np_conditions_apply(mdf, [(mdf[f"{race}_hispanic"] == 1)], [mdf["races_total"]], f"races_total_{race}hispanic",
                            0)

    for age in AGE_VALUES:
        np_conditions_apply(mdf, [(mdf["QAGE"] >= age)], [1], f"age{age}plus", 0)

    mdf["age"] = mdf["QAGE"]

    mdf["race"] = mdf["CENRACE"]

    mdf["race_p9"] = mdf["race"] * mdf["non_hispanic"]

    np_conditions_apply(mdf, [(mdf["hispanic"] == 1) & (mdf["age18plus"] == 1)], [1], "hispanic_age18plus", 0)
    np_conditions_apply(mdf, [(mdf["non_hispanic"] == 1) & (mdf["age18plus"] == 1)], [1], "nonhispanic_age18plus", 0)
    np_conditions_apply(mdf, [(mdf["race_multiple"] == 1)], [1], "age18_multracenh", 0)

    for n in range(1, 7):
        num = num2words[n]
        num_race = f"{num}_race"
        np_conditions_apply(mdf, [(mdf["age18plus"] == 1) & (mdf[num_race] == 1)], [1], f"{num_race}_age18plus", 0)
        np_conditions_apply(mdf, [(mdf["non_hispanic"] == 1) & (mdf[num_race] == 1)], [1], f"{num_race}_nonhispanic", 0)
        np_conditions_apply(mdf, [(mdf["nonhispanic_age18plus"] == 1) & (mdf[f"{num}_race"] == 1)], [1],
                            f"age18_{num}racenh", 0)
    np_conditions_apply(mdf, [(mdf["nonhispanic_age18plus"] == 1) & (mdf["race_multiple"] == 1)], [1],
                        "age18_multracenh", 0)
    np_conditions_apply(mdf, [(mdf["age18plus"] == 1) & (mdf["race_multiple"] == 1)], [1], f"race_multiple_age18plus", 0)

    for n in range(1, 64):
        np_conditions_apply(mdf, [(mdf["race"] == n)], [1], f"race{n}_p8", 0)
        np_conditions_apply(mdf, [(mdf["race"] == n) & (mdf["age18plus"] == 1)], [1], f"age18race{n}_p10", 0)
        np_conditions_apply(mdf, [(mdf["race_p9"] == n)], [1], f"race{n}_p9", 0)
        np_conditions_apply(mdf, [(mdf["race_p9"] == n) & (mdf["age18plus"] == 1)], [1], f"age18race{n}_p9", 0)

    mdf['sex'] = mdf["QSEX"]
    mdf['count'] = 1

    np_conditions_apply(mdf, [(mdf["sex"] == 2)], [1], "female", 0)
    np_conditions_apply(mdf, [(mdf["sex"] == 1)], [1], "male", 0)

    np_conditions_apply(mdf, [(mdf["QAGE"] < 20)], [1], "under20", 0)
    np_conditions_apply(mdf, [(mdf["QAGE"] < 20) & (mdf["female"] == 1)], [1], "female_under20", 0)
    np_conditions_apply(mdf, [(mdf["QAGE"] < 20) & (mdf["male"] == 1)], [1], "male_under20", 0)
    np_conditions_apply(mdf, [(mdf["race_multiple"] == 1) & (mdf["hispanic"] == 0)], [1], "multrace_nonh", 0)
    np_conditions_apply(mdf, [(mdf["race_multiple"] == 1) & (mdf["non_hispanic"] == 1)], [1], "multracenonhispanic", 0)
    num_list = [5, 10, 15, 17, 19, 20, 21, 61, 64, 66, 69] + list(range(25, 95, 5))
    for sex in ["male", "female"]:
        sexv = sex
        if sex == 'female':
            sexv = "fem"
        np_conditions_apply(mdf, [(mdf["race_multiple"] == 1) & (mdf[f"{sex}"] == 1)], [1], f"multrace_{sex}", 0)
        np_conditions_apply(mdf, [(mdf["hispanic"] == 1) & (mdf[f"{sex}"] == 1)], [1], f"hispanic_{sex}", 0)
        np_conditions_apply(mdf, [(mdf["race_multiple"] == 1) & (mdf["non_hispanic"] == 1) & (mdf[f"{sex}"] == 1)], [1],
                            f"multracenh_{sex}", 0)
        for i, n in enumerate(num_list):
            if i == 0:
                condition = mdf["QAGE"] < n
                var_post = f"_0to{n - 1}"
            elif n not in [17, 19, 20, 21, 25, 61, 64, 66, 69, 90]:
                condition = mdf["QAGE"].between(n - 6, n, False)
                var_post = f"_{n - 5}to{n - 1}"
            else:
                if n == 17:
                    condition = mdf["QAGE"].between(15, 17, True)
                    var_post = "_15to17"
                elif n == 19:
                    condition = mdf["QAGE"].between(17, 20, False)
                    var_post = "_18to19"
                elif n == 20:
                    condition = mdf["QAGE"] == 20
                    var_post = "_20"
                elif n == 21:
                    condition = mdf["QAGE"] == 21
                    var_post = "_21"
                elif n == 25:
                    condition = mdf["QAGE"].between(22, 24, True)
                    var_post = "_22to24"
                elif n == 61:
                    condition = mdf["QAGE"].between(60, 61, True)
                    var_post = "_60to61"
                elif n == 64:
                    condition = mdf["QAGE"].between(62, 64, True)
                    var_post = "_62to64"
                elif n == 66:
                    condition = mdf["QAGE"].between(65, 66, True)
                    var_post = "_65to66"
                elif n == 69:
                    condition = mdf["QAGE"].between(67, 69, True)
                    var_post = "_67to69"
                elif n == 90:
                    condition = mdf["QAGE"] >= 85
                    var_post = "_85plus"
            var = f"{sex}{var_post}"
            condition_pass = [condition & (mdf[sex] == 1)]
            np_conditions_apply(mdf, condition_pass, [1], var, 0)
            for race in RACE_LIST:
                race_v = race_dict[race]
                condition_pass = [condition & mdf[f"race_{race}"].eq(1) & mdf["race_multiple"].eq(0) & mdf[sex].eq(1)]
                var = f"{race}{sexv}{var_post}"
                np_conditions_apply(mdf, condition_pass, [1], var, 0)

                # non_hispanic_conditions
                condition1 = mdf[f"race"].eq(race_v) & mdf["non_hispanic"].eq(1) & mdf[sex].eq(1)

                condition_pass = [condition & condition1]
                var = f"{race}nonh{sexv}{var_post}"
                np_conditions_apply(mdf, condition_pass, [1], var, 0)
            var = f"multrace{sexv}{var_post}"
            condition_pass = [condition & mdf["race_multiple"].eq(1) & mdf[sex].eq(1)]
            np_conditions_apply(mdf, condition_pass, [1], var, 0)
            var = f"hispanic{sexv}{var_post}"
            condition_pass = [condition & mdf["hispanic"].eq(1) & mdf[sex].eq(1)]
            np_conditions_apply(mdf, condition_pass, [1], var, 0)
        for race in RACE_LIST:
            race_v = race_dict[race]
            condition_pass = [mdf[f"race_{race}"].eq(1) & mdf["race_multiple"].eq(0)]
            np_conditions_apply(mdf, condition_pass, [1], f"{race}_onerace", 0)
            condition_pass = [mdf[f"race_{race}"].eq(1) & mdf["race_multiple"].eq(0) & mdf[sex].eq(1)]
            np_conditions_apply(mdf, condition_pass, [1], f"{race}_{sex}", 0)
            condition1 = mdf[f"race"].eq(race_v) & mdf["non_hispanic"].eq(1)
            var = f"{race}nonhispanic"
            np_conditions_apply(mdf, [condition1], [1], var, 0)
            condition_pass = [condition1 & mdf[sex].eq(1)]
            var = f"{race}nonh_{sex}"
            np_conditions_apply(mdf, condition_pass, [1], var, 0)
        for n in AGE_VALUES[1:]:
            condition = mdf[f"age{n}plus"] == 1
            var = f"{sex}_age{n}plus"
            condition_pass = [condition & mdf[sex].eq(1)]
            np_conditions_apply(mdf, condition_pass, [1], var, 0)
            for race in RACE_LIST:
                condition_pass = [condition & mdf[f"race_{race}"].eq(1) & mdf["race_multiple"].eq(0) & mdf[sex].eq(1)]
                var = f"{race}{sex}_age{n}plus"
                np_conditions_apply(mdf, condition_pass, [1], var, 0)
            var = f"multrace{sexv}_age{n}plus"
            condition_pass = [condition & mdf["race_multiple"].eq(1) & mdf[sex].eq(1)]
            np_conditions_apply(mdf, condition_pass, [1], var, 0)
            var = f"hispanic{sexv}_age{n}"
            condition_pass = [condition & mdf["hispanic"].eq(1) & mdf[sex].eq(1)]
            np_conditions_apply(mdf, condition_pass, [1], var, 0)
        for n in range(0, 100):
            var = f"{sexv}_age{n}"
            condition = mdf['QAGE'] == n
            condition_pass = [condition & mdf[sex].eq(1)]
            np_conditions_apply(mdf, condition_pass, [1], var, 0)
            for race in RACE_LIST:
                var = f"{race}{sexv}_age{n}"
                condition = mdf['QAGE'].eq(n) & mdf[f"race_{race}"].eq(1) & mdf["race_multiple"].eq(0)
                condition_pass = [condition & mdf[sex].eq(1)]
                np_conditions_apply(mdf, condition_pass, [1], var, 0)
                var = f"{race}nonhisp{sexv}_age{n}"
                condition = mdf['QAGE'].eq(n) & mdf[f"race_{race}"].eq(1) & mdf["race_multiple"].eq(0) & mdf["non_hispanic"].eq(1)
                condition_pass = [condition & mdf[sex].eq(1)]
                np_conditions_apply(mdf, condition_pass, [1], var, 0)
            var = f"multracenh{sexv}_age{n}"
            condition_pass = [mdf['QAGE'].eq(n) & mdf["non_hispanic"].eq(1) & mdf["race_multiple"].eq(1) & mdf[sex].eq(1)]
            np_conditions_apply(mdf, condition_pass, [1], var, 0)
            var = f"multrace{sexv}_age{n}"
            condition_pass = [mdf['QAGE'].eq(n) & mdf["race_multiple"].eq(1) & mdf[sex].eq(1)]
            np_conditions_apply(mdf, condition_pass, [1], var, 0)
            var = f"hispanic{sexv}_age{n}"
            condition_pass = [mdf['QAGE'].eq(n) & mdf["hispanic"].eq(1) & mdf[sex].eq(1)]
            np_conditions_apply(mdf, condition_pass, [1], var, 0)

        condition = mdf['QAGE'].between(100, 104, True)
        var = f"{sexv}_age100to104"
        condition_pass = [condition & mdf[sex].eq(1)]
        np_conditions_apply(mdf, condition_pass, [1], var, 0)
        condition = mdf['QAGE'].between(105, 109, True)
        var = f"{sexv}_age105to109"
        condition_pass = [condition & mdf[sex].eq(1)]
        np_conditions_apply(mdf, condition_pass, [1], var, 0)
        condition = mdf['QAGE'].ge(110)
        var = f"{sexv}_age110plus"
        condition_pass = [condition & mdf[sex].eq(1)]
        np_conditions_apply(mdf, condition_pass, [1], var, 0)

        # multracenhmale_age110plus
        condition = mdf['QAGE'].between(100, 104, True) & mdf["non_hispanic"].eq(1) & mdf["race_multiple"].eq(1)
        var = f"multracenh{sexv}_age100to104"
        condition_pass = [condition & mdf[sex].eq(1)]
        np_conditions_apply(mdf, condition_pass, [1], var, 0)

        condition = mdf['QAGE'].between(100, 104, True) & mdf["hispanic"].eq(1)
        var = f"hispanic{sexv}_age100to104"
        condition_pass = [condition & mdf[sex].eq(1)]
        np_conditions_apply(mdf, condition_pass, [1], var, 0)

        condition = mdf['QAGE'].between(100, 104, True) & mdf["race_multiple"].eq(1)
        var = f"multrace{sexv}_age100to104"
        condition_pass = [condition & mdf[sex].eq(1)]
        np_conditions_apply(mdf, condition_pass, [1], var, 0)

        condition = mdf['QAGE'].between(105, 109, True) & mdf["non_hispanic"].eq(1) & mdf["race_multiple"].eq(1)
        var = f"multracenh{sexv}_age105to109"
        condition_pass = [condition & mdf[sex].eq(1)]
        np_conditions_apply(mdf, condition_pass, [1], var, 0)

        condition = mdf['QAGE'].between(105, 109, True) & mdf["hispanic"].eq(1)
        var = f"hispanic{sexv}_age105to109"
        condition_pass = [condition & mdf[sex].eq(1)]
        np_conditions_apply(mdf, condition_pass, [1], var, 0)

        condition = mdf['QAGE'].between(105, 109, True) & mdf["race_multiple"].eq(1)
        var = f"multrace{sexv}_age105to109"
        condition_pass = [condition & mdf[sex].eq(1)]
        np_conditions_apply(mdf, condition_pass, [1], var, 0)

        condition = mdf['QAGE'].ge(110) & mdf["non_hispanic"].eq(1) & mdf["race_multiple"].eq(1)
        var = f"multracenh{sexv}_age110plus"
        condition_pass = [condition & mdf[sex].eq(1)]
        np_conditions_apply(mdf, condition_pass, [1], var, 0)

        condition = mdf['QAGE'].ge(110) & mdf["hispanic"].eq(1)
        var = f"hispanic{sexv}_age110plus"
        condition_pass = [condition & mdf[sex].eq(1)]
        np_conditions_apply(mdf, condition_pass, [1], var, 0)

        condition = mdf['QAGE'].ge(110) & mdf["race_multiple"].eq(1)
        var = f"multrace{sexv}_age110plus"
        condition_pass = [condition & mdf[sex].eq(1)]
        np_conditions_apply(mdf, condition_pass, [1], var, 0)

        for race in RACE_LIST:
            condition = mdf['QAGE'].between(100, 104, True) & mdf[f"race_{race}"].eq(1) & mdf["race_multiple"].eq(0)
            var = f"{race}{sexv}_age100to104"
            condition_pass = [condition & mdf[sex].eq(1)]
            np_conditions_apply(mdf, condition_pass, [1], var, 0)

            condition = mdf['QAGE'].between(100, 104, True) & mdf[f"race_{race}"].eq(1) & mdf["race_multiple"].eq(0) & mdf["race_multiple"].eq(0) & mdf["non_hispanic"].eq(1)
            var = f"{race}nonhisp{sexv}_age100to104"
            condition_pass = [condition & mdf[sex].eq(1)]
            np_conditions_apply(mdf, condition_pass, [1], var, 0)

            condition = (mdf['QAGE'].between(105, 109, True)) & (mdf[f"race_{race}"] == 1) & (mdf["race_multiple"] == 0)
            var = f"{race}{sexv}_age105to109"
            condition_pass = [condition & mdf[sex].eq(1) & mdf[f"race_{race}"].eq(1) & mdf["race_multiple"].eq(0)]
            np_conditions_apply(mdf, condition_pass, [1], var, 0)

            condition = mdf['QAGE'].between(105, 109, True) & mdf[f"race_{race}"].eq(1) & mdf["race_multiple"].eq(0) & mdf["race_multiple"].eq(0) & mdf["non_hispanic"].eq(1)
            var = f"{race}nonhisp{sexv}_age105to109"
            condition_pass = [condition & mdf[sex].eq(1)]
            np_conditions_apply(mdf, condition_pass, [1], var, 0)

            condition = mdf['QAGE'].ge(110) & mdf[f"race_{race}"].eq(1) & mdf["race_multiple"].eq(0)
            var = f"{race}{sexv}_age110plus"
            condition_pass = [condition & mdf[sex].eq(1) & mdf[f"race_{race}"].eq(1) & mdf["race_multiple"].eq(0)]
            np_conditions_apply(mdf, condition_pass, [1], var, 0)

            condition = mdf['QAGE'].ge(110) & mdf[f"race_{race}"].eq(1) & mdf["race_multiple"].eq(0) & mdf["race_multiple"].eq(0) & mdf["non_hispanic"].eq(1)
            var = f"{race}nonhisp{sexv}_age110plus"
            condition_pass = [condition & mdf[sex].eq(1)]
            np_conditions_apply(mdf, condition_pass, [1], var, 0)

    process = psutil.Process(os.getpid())
    #logger.debug(process.memory_info().rss)  # in bytes
    return mdf


def create_table(df, geo_conditions, var_dict):
    sum_variables = list(var_dict.keys())
    gf = df.groupby(geo_conditions)[sum_variables].sum().reset_index()
    gf = gf.rename(var_dict, axis=1)
    return gf


def list_duplicates(seq):
    seen = set()
    seen_add = seen.add
    seen_twice = set(x for x in seq if x in seen or seen_add(x))
    return list(seen_twice)


def get_rename_json(file_path):
    with open(file_path) as f:
        dict = json.load(f)
    return dict


def process_loop(df, chunks, dict, geo_condition):
    list_df = np.array_split(df, chunks)
    df_list = []
    for n, chunk in enumerate(list_df):
        cdf = process_read_mdf(chunk)
        dfo = create_table(cdf, geo_condition, dict)
        df_list.append(dfo)
        logger.info(f"Chunk: {n}")
        process = psutil.Process(os.getpid())
        #logger.debug(process.memory_info().rss)
    out_df = pd.concat(df_list)
    out_df = out_df.groupby(geo_condition).sum().reset_index()
    return out_df


def process_state_list(state_list):
    possible_states = ['1', '2', '4', '5', '6', '8', '9', '10', '11', '12', '13', '15', '16', '17', '18', '19', '20',
                       '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36',
                       '37', '38', '39', '40', '41', '42', '44', '45', '46', '47', '48', '49', '50', '51', '53', '54',
                       '55', '56','72']
    return_states = []
    if 'all' in state_list:
        for st in possible_states:
            if len(st) == 1:
                stadd = f"0{st}"
                return_states.append(stadd)
            else:
                return_states.append(st)
    else:
        for st in state_list:
            if st in possible_states:
                if len(str(st)) == 1:
                    stadd = f"0{st}"
                    return_states.append(stadd)
                else:
                    return_states.append(st)
    logger.info(return_states)
    return return_states

class state_processor:
    def __init__(self, mp, mdf_file,state_list, json_file, geo_condition, chunk_size):
        self.mp = mp
        self.mdf_file = mdf_file
        self.state_list = state_list
        self.dict = get_rename_json(json_file)
        self.geo_condition = geo_condition
        self.chunk_size = chunk_size


    def state_dfs(self):
        ret = []
        for st in self.state_list:
            ret.append(self.create_df(st))
        return ret

    def create_df(self, state):
        df = self.mdf.loc[(self.mdf["TABBLKST"] == f'{state}')]
        self.mdf = self.mdf.loc[(self.mdf["TABBLKST"] != f'{state}')]
        return df

    def process_states(self):
        if int(self.mp) == 1:
            for state in self.state_list:
                self.run_state(state)
        elif int(self.mp) > 1:
            with multiprocessing.Pool(int(self.mp)) as p:
                p.map(self.run_state, self.state_list)
        else:
            logging.ERROR("Non-Valid Multiprocessing value of: {multiprocessing}")

    def run_state(self, state):
        #logger.debug(f"mdf shape: {self.mdf.shape}")
        df = (read_table_mdf_per_file(self.mdf_file)[lambda x: x['TABBLKST'] == f'{state}'])
        if df.empty:
            logger.info(f"State: {state} Not in Mdf: {self.mdf_file}")
            return
        #df = self.mdf.loc[(self.mdf["TABBLKST"] == f'{state}')]
        #self.mdf = self.mdf.loc[(self.mdf["TABBLKST"] != f'{state}')]
        #logger.debug(f"mdf shape no {state}: {self.mdf.shape}")
        state_abv = ST_CODES[state]
        logger.info(f"State: {state_abv}|{state}")
        out = process_loop(df, self.chunk_size, self.dict, self.geo_condition)
        logger.debug(out.dtypes)
        out['TABBLKST'] = f"{state}"
        out.to_csv(f"./mdf_hdf_{state_abv}.csv", index=False)
        process = psutil.Process(os.getpid())
        #logger.debug(process.memory_info().rss)  # in bytes


def main(mdf_file, json_file, state_list, chunk_size, geo_condition, s3_bucket, mdf_key, mp):
    if not os.path.exists(mdf_file):
        bucket, key = s3.get_bucket_key(s3_bucket+mdf_key)
        s3.get_object(bucket, key, mdf_file)

    _state_list = process_state_list(state_list)
    states_process = state_processor(mp, mdf_file, _state_list, json_file, geo_condition, chunk_size)
    states_process.process_states()
    end_time = perf_counter()
    logger.info(f"{end_time - start_time:0.4f} seconds")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-mf', '--mdf_file', default=DEF_MDF)
    parser.add_argument('-rf', '--rename_json', default=DEF_RENAME, help='json dictionary of renames')
    parser.add_argument('-c', '--chunk_size', default=CHUNK_SIZE, type=int, help='chunk size for mdf process')
    parser.add_argument('-st', '--states', nargs='+', default='all', help='state list if all in list will process all')
    parser.add_argument('-gc', '--geo_conditions', nargs='+', default=DEF_GEO,
                        help='variables to groupby')
    parser.add_argument('-s3', '--s3_bucket', default=DAS_ROOT)
    parser.add_argument('-mdfk', '--mdf_key', default=MDF_KEY)
    parser.add_argument('-mp', '--multi_process', default=DEF_MP, help='run states in multiprocessing')
    parser.add_argument('-debug', action='store_true', help='Debug')
    args = parser.parse_args()

    logging.basicConfig(format='[%(filename)s:%(lineno)d] %(message)s', )
    logger = logging.getLogger()
    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    main(args.mdf_file, args.rename_json, args.states, args.chunk_size, args.geo_conditions, args.s3_bucket, args.mdf_key, args.multi_process)
