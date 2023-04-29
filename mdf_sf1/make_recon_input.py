#!/usr/bin/env python3
import pandas as pd
import numpy as np
import re
import logging
from time import perf_counter
import os, psutil
import json
import argparse
import shutil
import errno
import zipfile
import sys
import csv
import chardet
import pathlib
import multiprocessing

MY_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(MY_DIR)
if PARENT_DIR not in sys.path:
    sys.path.append(PARENT_DIR)
import ctools.s3 as s3

DEF_GF = "geo2010.sf1"
DEF_DIR = "."
DEF_TJ = "table_dict.json"
DEF_MP = 1
DAS_ROOT = os.environ["DAS_S3ROOT"]
S3_SF1 = '/2010-re/pct12g_fix_test/dist/'
DEF_GEO = ['TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE', 'TABBLK']
ST_CODES = {'53': 'WA', '10': 'DE', '11': 'DC', '55': 'WI', '54': 'WV', '15': 'HI', '12': 'FL', '56': 'WY',
            '72': 'PR', '34': 'NJ', '35': 'NM', '48': 'TX', '22': 'LA', '37': 'NC', '38': 'ND', '31': 'NE',
            '47': 'TN', '36': 'NY', '42': 'PA', '02': 'AK', '32': 'NV', '33': 'NH', '51': 'VA', '08': 'CO',
            '06': 'CA', '01': 'AL', '05': 'AR', '50': 'VT', '17': 'IL', '13': 'GA', '18': 'IN', '19': 'IA',
            '25': 'MA', '04': 'AZ', '16': 'ID', '09': 'CT', '23': 'ME', '24': 'MD', '40': 'OK', '39': 'OH',
            '49': 'UT', '29': 'MO', '27': 'MN', '26': 'MI', '44': 'RI', '20': 'KS', '30': 'MT', '28': 'MS',
            '45': 'SC', '21': 'KY', '41': 'OR', '46': 'SD'}


class SF1_Driver:
    def __init__(self, hdf_dir, out_dir, state_list, geo_file_suffix, geo_conditions, table_json, mp, sf1_local=None, s3_location=None, debug=False):
        self.sumlev_dict = {
            '140': ["TABBLKST", "TABBLKCOU", "TABTRACTCE"],
            '101': ["TABBLKST", "TABBLKCOU", "TABTRACTCE", "TABBLKGRPCE", "TABBLK"],
            '050': ["TABBLKST", "TABBLKCOU"]
        }
        self.possible_states = ['1', '2', '4', '5', '6', '8', '9', '10', '11', '12', '13', '15', '16', '17', '18', '19',
                                '20',
                                '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34',
                                '35',
                                '36',
                                '37', '38', '39', '40', '41', '42', '44', '45', '46', '47', '48', '49', '50', '51',
                                '53',
                                '54',
                                '55', '56','72']
        self.table_dict = self.get_table_json(table_json)
        self.out_dir = out_dir.rstrip("/").rstrip("\\")
        self.geo_file_suffix = geo_file_suffix
        self.files = os.listdir(hdf_dir)
        self.geo_vars = geo_conditions
        self._state_list = self.process_state_list(state_list)
        self.s3_location = s3_location
        self.sf1_local = sf1_local
        self.sf1_dir = None
        self.mp = int(mp)
        self.debug = debug


    def run_states(self):
        if self.mp == 1:
            for state in self._state_list:
                self.run_state(state)
        elif self.mp > 1:
            with multiprocessing.Pool(self.mp) as p:
                p.map(self.run_state, self._state_list)
        else:
            logging.ERROR("Non-Valid Multiprocessing value of: {self.multiprocessing}")

    def run_state(self, state):
        state_abv = ST_CODES[state]
        logger.debug(f"mdf_hdf_{state_abv}.csv")
        if f"mdf_hdf_{state_abv}.csv" not in self.files:
            logging.info(f"mdf_hdf_{state_abv}.csv not in directory")
            return
        if self.sf1_local is None:
            self.sf1_dir = "./s3sf1tables"
            sf1_out = f'{self.sf1_dir}/{state_abv.lower()}/'
            self.check_create_path(sf1_out)
            sf1_file = f"{state_abv.lower()}2010.sf1.zip"
            self.prepare_s3_files(s3_location, sf1_file, sf1_out)
        else:
            self.sf1_dir = self.sf1_local.rstrip("/").rstrip("\\")
        geo_filepath = f"{self.sf1_dir}/{state_abv.lower()}/{state_abv.lower()}{self.geo_file_suffix}"
        logger.debug(f"geo_filepath: {geo_filepath}")
        geo = self.read_geo(geo_filepath)
        hdf_filepath = [file for file in self.files if (file == f"mdf_hdf_{state_abv}.csv")][0]
        hdf = self.read_hdf(hdf_filepath)
        self.tables_creation(hdf, geo, state_abv, geo_filepath)

    def read_geo(self, geo_file_path: str):
        logger.debug(f"read_geo: {geo_file_path}")
        _colspecs = [(8, 11), (27, 29), (29, 32), (54, 60), (60, 61), (61, 65)]
        try:
            geo = pd.read_fwf(geo_file_path, colspecs=_colspecs, header=None,
                              names=['SUMLEV', 'TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE', 'TABBLK'],
                              dtype={'SUMLEV': 'object',
                                 'TABBLKST': 'object',
                                 'TABBLKCOU': 'object',
                                 'TABTRACTCE': 'object',
                                 'TABBLKGRPCE': 'object',
                                 'TABBLK': 'object'
                                 }
                              )
        except UnicodeDecodeError:
            self.convert_encoding(geo_file_path)
            geo = pd.read_fwf(geo_file_path, colspecs=_colspecs, header=None,
                              names=['SUMLEV', 'TABBLKST', 'TABBLKCOU', 'TABTRACTCE', 'TABBLKGRPCE', 'TABBLK'],
                              dtype={'SUMLEV': 'object',
                                     'TABBLKST': 'object',
                                     'TABBLKCOU': 'object',
                                     'TABTRACTCE': 'object',
                                     'TABBLKGRPCE': 'object',
                                     'TABBLK': 'object'
                                     }
                              )

        geo.index = np.arange(1, len(geo) + 1)
        geo['rownum'] = geo.index

        return geo

    def read_hdf(self, hdf_file_path: str):
        logger.debug(f"read_hdf: {hdf_file_path}")
        hdf = pd.read_csv(hdf_file_path,
                          dtype={
                              'TABBLKST': 'object',
                              'TABBLKCOU': 'object',
                              'TABTRACTCE': 'object',
                              'TABBLKGRPCE': 'object',
                              'TABBLK': 'object'
                          })
        hdf = hdf.sort_index()
        return hdf

    def get_geo_population(self, filepath):
        logger.debug(f"get_geo_population: {filepath}")
        _colspecs = [(0, 319), (319, 327), (327, 500)]
        geo = pd.read_fwf(filepath, colspecs=_colspecs, header=None, delimiter="\n\t",
                          names=['filler', 'population', 'filler2'],
                          dtype={'filler': 'object',
                                 'population': 'object',
                                 'filler2': 'object'
                                 }
                          )
        geo.index = np.arange(1, len(geo) + 1)
        return geo

    def get_sf1_file(self, state_abv, table_name, vars):
        logger.debug(f"get_sf1_file: {state_abv}, {table_name}")
        file_path = f"{self.sf1_dir}/{state_abv.lower()}/{state_abv.lower()}{table_name}.sf1"
        vars_clean = self.append_duplicates(vars)
        colnames = ['filetype', 'tabblkst', 'zeros', 'num', 'rownum'] + vars_clean
        orig = pd.read_csv(file_path, header=None)
        orig.columns = colnames
        return orig

    def prepare_s3_files(self, location, filename, outdir):
        logger.debug(f"prepare_s3_files: {location}, {filename}, {outdir}")
        out_file = outdir + filename
        if not os.path.exists(out_file):
            bucket, prefix = s3.get_bucket_key(location)
            logger.info(bucket)
            key = prefix + filename
            s3.get_object(bucket, key, out_file)
            with zipfile.ZipFile(out_file, 'r') as zip_ref:
                zip_ref.extractall(os.path.dirname(out_file))

    def process_state_list(self, state_list):
        logger.debug(f"process_state_list: {state_list}")
        possible_states = self.possible_states
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

    def tables_creation(self, hdf, geo, state, geo_filepath):
        logger.debug(f"tables_creation: {state}")
        self.check_create_path(f"{self.out_dir}/{state.lower()}")
        for table in self.table_dict:
            sf1_name = table.split('_')[0]
            g_vars = [x for x in self.table_dict[table] if x in self.geo_vars]
            logger.debug(f"g_vars: {g_vars}")
            var_list = [var for var in self.table_dict[table] if var not in g_vars]
            key = next(key for key, value in self.sumlev_dict.items() if set(value) == set(g_vars))
            gdf = geo.loc[geo['SUMLEV'] == f"{key}"]
            path = f"{self.out_dir}/{state.lower()}"
            sf1_filepath = f"{path}/{state.lower()}{sf1_name}.sf1"
            if 'cen2010' not in table:
                if os.path.exists(sf1_filepath):
                    vars_clean = self.append_duplicates(var_list)
                    colnames = ['filetype', 'tabblkst', 'zeros', 'num', 'rownum'] + vars_clean
                    orig_sf1 = pd.read_csv(sf1_filepath, header=None)
                    orig_sf1.columns = colnames
                else:
                    orig_sf1 = self.get_sf1_file(state, sf1_name, var_list)
            for var in self.table_dict[table]:
                if var not in list(hdf):
                    if re.search("blank", var):
                        hdf[var] = .123
                    else:
                        print(f"Variable not found: {var}")
            group_hdf = hdf[self.table_dict[table]]
            group_hdf = group_hdf.groupby(g_vars).sum().reset_index()
            keep = self.table_dict[table] + ['rownum']
            sf1_crosswalk = pd.merge(gdf, group_hdf, on=g_vars, how='left')[keep]
            sf1_crosswalk = sf1_crosswalk.fillna(0)
            logger.debug(f"sf1_name: {sf1_name}")
            if sf1_name == '000012010':
                self.update_geo(sf1_crosswalk, geo_filepath, path)
            if 'cen2010' not in table:
                self.create_sf1_file(sf1_crosswalk, orig_sf1, g_vars, state, sf1_name)
            out_name = f"{state.lower()}{table}.csv"
            if self.debug:
                self.check_create_path(f"debug_tables/{state.lower()}")
                sf1_crosswalk.to_csv(f"debug_tables/{state.lower()}/{out_name}", index=True)
        if self.out_dir != ".":
            zip_file_name = f"{self.out_dir}/{state.lower()}2010.sf1.zip"
        else:
            zip_file_name = f"{state.lower()}2010.sf1.zip"
        self.zip_files_in_path(zip_file_name, f"{self.out_dir}/{state.lower()}", f"{self.sf1_dir}/{state.lower()}")
        if args.reident:
            s3url = f"{args.s3_bucket}/2010-re/{args.reident}/dist/{zip_file_name}"
            logger.debug(f"{s3url}")
            s3.put_s3url(s3url, zip_file_name)


    def create_sf1_file(self, update_table, orig_sf, drop_list, state_abv, table_name):
        for var in list(update_table):
            if re.search("blank", var):
                update_table = update_table.drop(columns=[var])
        update_table.columns = self.append_duplicates(update_table.columns)
        update_table.set_index('rownum', inplace=True)
        update_table = update_table.drop(columns=drop_list)
        logger.debug(f"drop_list: {drop_list}")

        o_dtypes = [orig_sf[x].dtypes.name for x in orig_sf.columns]


        orig_sf['rownumstr'] = orig_sf['rownum'].astype(int)
        orig_sf.set_index('rownumstr', inplace=True)

        orig_sf.update(update_table)

        for x,d in zip(orig_sf.columns, o_dtypes):
            orig_sf[x] = orig_sf[x].astype(d)

        orig_sf[list(orig_sf)] = orig_sf[list(orig_sf)].astype(str)
        orig_sf.iloc[:, 2] = orig_sf.iloc[:, 2].astype(str).str.zfill(3)
        orig_sf.iloc[:, 3] = orig_sf.iloc[:, 3].astype(str).str.zfill(2)
        orig_sf.iloc[:, 4] = orig_sf.iloc[:, 4].astype(str).str.zfill(7)

        #format fix for 000082010
        if '000082010' in table_name:
            orig_sf.iloc[:, -1] = orig_sf.iloc[:, -1].astype(float).map('{:,.2f}'.format)
            orig_sf.iloc[:, -2] = orig_sf.iloc[:, -2].astype(float).map('{:,.2f}'.format)
            orig_sf.iloc[:, -3] = orig_sf.iloc[:, -3].astype(float).map('{:,.2f}'.format)

        path = f"{self.out_dir}/{state_abv.lower()}"
        self.check_create_path(path)
        filepath = f"{path}/{state_abv.lower()}{table_name}.sf1"
        logger.info(f"Writing: {filepath}")

        orig_sf.to_csv(filepath, index=False, header=False)

    def update_geo(self, update_table, geo_file_path, out_path):
        update_table = update_table.set_index('rownum')
        update_table = update_table[['population']]
        if args.debug_tables:
            update_table.to_csv(out_path + '_update_to')
        update_table['population'] = update_table['population'].astype(str)
        logger.debug(f"geo_file_path: {geo_file_path}")
        state_abv = out_path[-2:]
        out_file = f"{out_path}/{state_abv.lower()}{self.geo_file_suffix}"
        out_file_temp = f"{out_file}_temp"
        if os.path.exists(out_file):
            geo = self.get_geo_population(out_file)
            os.rename(out_file,out_file_temp)
        else:
            geo = self.get_geo_population(geo_file_path)

        o_dtypes = [geo[x].dtypes.name for x in geo.columns]

        geo.update(update_table)
        geo['population'] = geo['population'].astype('float').astype('int').astype('str').str.rjust(8)

        for x,d in zip(geo.columns, o_dtypes):
            geo[x] = geo[x].astype(d)

        geo['filler'] = geo['filler'].str.ljust(318)
        geo['filler2'] = geo['filler2'].str.ljust(172)
        geo['outstring'] = geo['filler'] + geo['population'] + geo['filler2']
        geo = geo[['outstring']]
        logger.debug(f"geo_out: {out_file}")
        with open(out_file, "a+") as f:
            for row in geo['outstring']:
                f.write(str(row).strip('"') + "\n")
        if os.path.exists(out_file_temp):
            os.remove(out_file_temp)

    @staticmethod
    def get_table_json(file_path):
        with open(file_path) as f:
            dict = json.load(f)
        return dict

    @staticmethod
    def check_create_path(path):
        if not os.path.exists(path):
            os.makedirs(path)
            logger.info(f"Created {path} directory")

    @staticmethod
    def append_duplicates(seq):
        cols = pd.Series(seq)
        for dup in cols[cols.duplicated()].unique():
            cols[cols[cols == dup].index.values.tolist()] = [dup + '.' + str(i) if i != 0 else dup for i in
                                                             range(sum(cols == dup))]
        return list(cols)

    @staticmethod
    def zip_files_in_path(zip_file, dirName, sf1dir):
        with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zipObj:
            # Iterate over all the files in directory
            for folderName, subfolders, filenames in os.walk(dirName):
                for sf1folderName, sf1subfolders, sf1filenames in os.walk(sf1dir):
                    for file in sf1filenames:
                        if file not in filenames and not file.endswith('.zip'):
                            filePath = os.path.join(sf1folderName, file)
                            # Add file to zip
                            zipObj.write(filePath, os.path.basename(filePath))
                for filename in filenames:
                    # create complete filepath of file in directory
                    filePath = os.path.join(folderName, filename)
                    # Add file to zip
                    zipObj.write(filePath, os.path.basename(filePath))

    @staticmethod
    def convert_encoding(filename):
        f_copy = filename+"_copy"
        os.rename(filename, f_copy)
        filepath = pathlib.Path(f_copy)
        blob = filepath.read_bytes()
        detection = chardet.detect(blob)
        encoding = detection["encoding"]
        with open(f_copy, 'rb') as source_file:
            with open(filename, 'w+b') as dest_file:
                contents = source_file.read()
                dest_file.write(contents.decode(encoding).encode('utf-8'))

        os.remove(f_copy)


def main(hdf_dir, out_dir, state_list, geo_file_suffix, geo_conditions, table_json, sf1_local, s3_location, debug, multiprocessing):
    driver = SF1_Driver(hdf_dir, out_dir, state_list, geo_file_suffix, geo_conditions, table_json, multiprocessing, sf1_local, s3_location, debug)
    driver.run_states()



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-st', '--states', nargs='+', default='all', help='state list if all in list will process all')
    parser.add_argument('-hd', '--hdf_dir', default=DEF_DIR, help='directory where hdfs are stored defaults to the directory where make_recon_input.py is run from')
    parser.add_argument('-od', '--out_dir', default=DEF_DIR, help='directory where output tables are written in state folders defaults to the directory where make_recon_input.py is run from')
    parser.add_argument('-sf1l', '--sf1_local', help='directory where geo files should be search for this with make the code only try to pull from the local directory instead of s3 Ex: "/projects/projectdata/fromIRE/data/sf1"')
    parser.add_argument('-gs', '--geo_suffix', default=DEF_GF, help='file sufix which geo files use ie "geo2010.sf1" the code will add in the state and searh for the corresponding file ie "cageo2010.sf1" defaults to "geo2010.sf1" in ./s3sf1tables if pulling from s3')
    parser.add_argument('-s3', '--s3_bucket', default=DAS_ROOT, help='s3 bucket where sf1 is stored')
    parser.add_argument('-sf1p', '--sf1_key', default=S3_SF1, help='sf1 s3 key')
    parser.add_argument('-td', '--table_dict', default=DEF_TJ, help='path to table_diction json file which handles variable selection and rollup for sf1 tables defaults to "table_dict.json"')
    parser.add_argument('-gc', '--geo_conditions', nargs='+', default=DEF_GEO, help='list of variables the code will consider "grouping"')
    parser.add_argument('-mp', '--multi_process', default=DEF_MP, help='run states in multiprocessing')
    parser.add_argument('--reident', help='s3 reident to upload zip to')
    parser.add_argument('-debug', action='store_true', help='Debug')
    parser.add_argument('-debug_tables', action='store_true', help='Debug Level output tables to ./debug_tables')
    args = parser.parse_args()

    logging.basicConfig(format='[%(filename)s:%(lineno)d] %(message)s', )
    logger = logging.getLogger()
    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    s3_location = args.s3_bucket + args.sf1_key
    main(args.hdf_dir, args.out_dir, args.states, args.geo_suffix, args.geo_conditions, args.table_dict, args.sf1_local, s3_location, args.debug_tables, args.multi_process)
