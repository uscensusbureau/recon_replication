#!/usr/bin/env python3
#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)
"""
Read processed SF1 data & make LP file to be input to optimizer. Currently done
in pandas & by buffering in RAM; memory intensive.

<6/21/2018: TSA wrote initial code, performed reconstruction by manually
running independent processes on many AWS nodes. [No git repo.]

<5/1/2019: PDL performed minor cleanup after receiving code from TSA, moved
code into git repo, provided documentation, completed reconstruction where
small proportion of TSA runs had failed. [git repo: reid-reconhdf]

<5/4/2021: SLG switched from reid-reconhdf repo to stats_2010 repo [so, current
git history begins in stats_2010], altered code to use SQL server, partial
automation over a cluster, partial spark support. SLG tried to simplify this
file further, but was never able to figure out what it was actually doing.

<5/6/2021: PDL added support for 2+ race tabulations, which were thought to be,
but were not, included in original reconstruction, and performed additional
cleanup, refactoring, etc.

Part of the replication archive for The U.S. Census Bureau's Ex Post
Confidentiality Analysis of the 2010 Census Data Publications
(https://github.com/uscensusbureau/recon_replication)
"""
import math, csv, gc, itertools, json, logging, multiprocessing, os, sys, time, tempfile, subprocess, atexit, datetime
import os.path
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
from collections import defaultdict

import dbrecon
from dbrecon import DBMySQL,GB,MB,LP,SOL,CSV,REIDENT
from dbrecon import validate_lpfile,LPFILENAMEGZ,dopen,dpath_expand,dmakedirs,LPDIR,dpath_exists,dpath_unlink,mem_info,dgetsize,remove_lpfile
from ctools.total_size import total_size
from ctools.dbfile import DBMySQL,DBMySQLAuth
import ctools.s3 as s3
assert pd.__version__ > '0.19'

# Tabulations in tables that are not in ALLOWABLE_TABLES will not be considered for use in building constraints or variables
ALLOWABLE_TABLES =      ['P1', 'P6', 'P7', 'P9', 'P11', 'P14', 'P8', 'P10']
ALLOWABLE_TABLES +=     ['P12'] # Isolated for emphasis: P12 is the table from which variables are built
ALLOWABLE_TABLES +=     ['P12A', 'P12B', 'P12C', 'P12D', 'P12E', 'P12F', 'P12H', 'P12I'] # NOTE: P12G handled below
ALLOWABLE_TABLES +=     ['PCT12', 'PCT12A', 'PCT12B', 'PCT12C', 'PCT12D', 'PCT12E', 'PCT12F'] # NOTE: PCT12G handled below
ALLOWABLE_TABLES +=     ['PCT12H', 'PCT12I', 'PCT12J', 'PCT12K', 'PCT12L', 'PCT12M', 'PCT12N'] # NOTE: PCT12O handled below
TWO_PLUS_RACE_TABLES_TO_INCLUDE_AS_CONSTRAINTS = ['P12G', 'PCT12G', 'PCT12O'] # 2+ Races constraints are special case
ALLOWABLE_TABLES += TWO_PLUS_RACE_TABLES_TO_INCLUDE_AS_CONSTRAINTS

DISALLOW_ZERO_RACE_VARIABLES = True

VERBOSE = False                 # Some logging.info statements useful for debugging are controlled through this
MAX_SF1_ERRORS = 10             # Some files may have errors
DEFAULT_J1, DEFAULT_J2 = 1, 4   # SLG parallelism types, default settings

MISSING, HISPANIC                                       = 'missing', 'hispanic'
Y, N                                                    = 'Y', 'N'
MALE, FEMALE, BOTH                                      = 'male', 'female', 'both'
SEX, AGE                                                = 'sex', 'age'
WHITE, BLACK, AIAN, ASIAN, NH, SOR, HISP                = 'white', 'black', 'aian', 'asian', 'nh', 'sor', 'hisp'
GEOID, TABLE_NUM, CELL_NUMBER, TABLEVAR                 = 'geoid', 'table_num', 'cell_number', 'tablevar'
# geo_table is a col w/ cell_number, geoid concat'd [do not make category; col appears distinct in each row of tables that use it]
GEO_TABLE, ALL                                          = 'geo_table', 'all'
# These are the maximal sets (lists) of possible values that each attribute can take on:
SEX_RANGE                                               = [MALE, FEMALE]
AGE_RANGE                                               = list(range(0, 111))
WHITE_RANGE, BLACK_RANGE, AIAN_RANGE                    = [Y, N], [Y, N], [Y, N]
ASIAN_RANGE, HAWAIIAN_RANGE, SOR_RANGE, HISPANIC_RANGE  = [Y, N], [Y, N], [Y, N], [Y, N]
ATTRIBUTES                                              = [SEX, AGE, WHITE, BLACK, AIAN, ASIAN, NH, SOR, HISP]

def verboseLog(msg):
    if VERBOSE:
        logging.info(msg)

def makeSummaryNumsDefaultDict(): # Used to initialize summary metadata/data dict for defining constraints (TODO: define a Class!)
    summary_nums_default_dict = defaultdict(lambda: {
                                                        'n_people': '0',
                                                        'constraints':  { # Could use defaultdict here, but this is more explicit
                                                                            'simpleProduct':[],
                                                                            'twoPlusRaces':[],
                                                                        },
                                                        'tuple_list': [],
                                                    })
    return summary_nums_default_dict

# These are the kinds of column names that should be turned into pandas categories
CATEGORIES = ATTRIBUTES + [GEOID, TABLE_NUM, CELL_NUMBER, TABLEVAR]
def make_attributes_categories(df):
    """Change attributes that are objects to categories"""
    for a in CATEGORIES:
        if (a in df) and (df[a].dtype=='object'):
            df[a] = df[a].astype('category')
    return df

def makeSimpleProductConstraints(constraint_list):
    """For constraints that just require itertools.product to find all relevant variables types"""
    con_frame_list = []
    for c in constraint_list:
        # c has format: [constraint_value, S, A, Wh, Bl, Aian, Asian, NH, Sor, Hisp,
        #                        s['table_number'], s[GEOID], s[CELL_NUMBER]]
        verboseLog(f"simpleProduct: constraint_list c is... {c}")
        for c3 in itertools.product(*c[1:10]):
            x=[c[0]] # tabulation value (i.e., integer rhs for constraint [NOTE: strictly integer only if no medians])
            x.extend(c3) # attributes: S, A, ..., Sor, Hisp
            x.append(c[10]) # table_number
            x.append(c[11]) # geoid
            x.append(c[12]) # cell # within table
            con_frame_list.append(x)
            verboseLog(f"simpleProduct: appended to con_frame_list... {x}")
    return con_frame_list

def makeTwoPlusRaceConstraints(constraint_list):
    con_frame_list = []
    for c in constraint_list:
        # c has format: [constraint_value, S, A, Hisp, s['table_number'], s[GEOID], s[CELL_NUMBER]]
        verboseLog(f"2+Races: constraint_list c is... {c}")
        for sex, age, hisp in itertools.product(*c[1:4]):
            for numraces in range(2, 6+1):
                for race_flags_on in itertools.combinations(range(6), numraces):
                    x = [c[0]] # tabulation value (i.e., integer rhs for constraint [NOTE: strictly integer only if no medians])
                    x.extend([sex, age])
                    x.extend(['Y' if i in race_flags_on else 'N' for i in range(6)]) # Race flags
                    x.extend([hisp] + c[4:]) # hisp, table_number, geoid, cell # within table
                    con_frame_list.append(x)
                    verboseLog(f"2+Races: appended to con_frame_list... {x}")
    return con_frame_list

def update_constraints(lp_outfile, level, n_con, summary_nums, geo_id, debug=False):
    tuple_list=[]
    constraint_list_simpleProduct = []  # List reprs of constraints that correspond to simple itertools.products
    constraint_list_twoPlusRaces = []   # List reprs of constraints that require sums over 2+ races
    for s in summary_nums:
        tuple_list.extend(summary_nums[s]['tuple_list'])
        constraint_list_simpleProduct.extend(summary_nums[s]['constraints']['simpleProduct'])
        constraint_list_twoPlusRaces.extend(summary_nums[s]['constraints']['twoPlusRaces'])
    summary_nums={}
    tuple_frame=pd.DataFrame(tuple_list, columns=['TID'] + ATTRIBUTES + ['print_val', GEOID])
    make_attributes_categories(tuple_frame)
    del tuple_list

    # con_frame_list will hold a list of lists indicating variable/person types relevant to this constraint, with format:
    # [ ...,
    #   [0.0, 'male', 37, 'Y', 'N', 'N', 'Y', 'Y', 'Y', 'N', 'P6', '010010201002005', 'P0060006'],
    #   ...]
    # where each inner list gives [tabulation_count] + ATTRIBUTES + [table, geoid, table_cell_id]
    con_frame_list_simpleProduct = makeSimpleProductConstraints(constraint_list_simpleProduct)
    con_frame_list_twoPlusRaces = makeTwoPlusRaceConstraints(constraint_list_twoPlusRaces)
    con_frame_list = con_frame_list_simpleProduct + con_frame_list_twoPlusRaces
    del constraint_list_simpleProduct, constraint_list_twoPlusRaces
    gc.collect()

    con_frame = pd.DataFrame(con_frame_list, columns=['value'] + ATTRIBUTES + [TABLE_NUM,GEOID,CELL_NUMBER])
    con_frame[GEO_TABLE] = con_frame[CELL_NUMBER] + '_' + con_frame[GEOID]
    make_attributes_categories(con_frame)

    # Note: we must make con_frame into categories *after* GEO_TABLE is added, because you cannot add categories
    merge_list=[GEOID] + ATTRIBUTES if level=='block' else ATTRIBUTES

    # lp is largest dataframe. Before categories, often exceeded 50GB. Now, is a merge of 2 dfs w/ cats--typically 1GB-10GB
    lp = pd.merge(tuple_frame, con_frame, how='inner', on=merge_list, copy=False)
    if debug:
        mem_info("tuple_frame",tuple_frame)
        mem_info("con_frame",con_frame)
        mem_info('lp',lp)
    del tuple_frame
    del con_frame
    gc.collect()

    lp_short = lp[[GEO_TABLE, 'value','print_val']]
    del lp
    gc.collect()

    lp_df    = lp_short.groupby([GEO_TABLE, 'value']).agg(lambda x: ' + '.join(x))
    if debug:
        mem_info("lp_short",lp_short)
        mem_info("lp_df",lp_df)
    del lp_short
    gc.collect()

    for index, row in lp_df.iterrows():
        geo_table, group, n = index[0], row['print_val'], index[1]
        lp_outfile.write(f"_C_{geo_table}_{n_con}: {group} = {n} \n")
        n_con += 1
    del lp_df
    gc.collect()
    return n_con


class LPTractBuilder:
    """Build the LP files for a given tract"""
    def __init__(self, stusab, county, tract, sf1_tract_data, sf1_block_data, *, debug=False, db_start_at_end=False, output=None, dry_run=None):
        self.master_tuple_list=[]
        self.stusab = stusab
        self.county     = county
        self.tract      = tract
        self.sf1_tract_data = sf1_tract_data
        self.sf1_block_data = sf1_block_data
        self.debug      = debug
        self.db_start_at_end = False # run db_start at the end of the tract building, rather than the start
        self.output     = output
        self.dry_run    = dry_run
        self.tables_encountered = set() # Set that keeps track of which tables we have/have not encountered in the input
        self.tables_with_constraints = set() # Set that tracks which tables we added at least 1 constraint for

    def db_fail(self):
        # remove from the database that we started. This is used to clean up the database if the program terminates improperly
        if not self.debug:
            logging.warning("UNSOLVE LPTractBuilder db_fail %s %s %s",self.stusab, self.county, self.tract)
            DBMySQL.csfr(dbrecon.auth(),
                         f"UPDATE {REIDENT}tracts SET lp_start=NULL where stusab=%s and county=%s and tract=%s",
                         (self.stusab,self.county,self.tract))

    def get_constraint_summary(self, level, p01_data, data_dict, summary_nums):
        """
        Function to get the summary dict to create constraints
        @param summary_nums - dictionary with summary numbers that is updated.
          summary_nums[geoid] =                 --- a defaultdict of metadata & data summaries for each GEOID, w/ structure:
            {
                 {'n_people' : '0'}             --- (why is this number a string?)
                 {'constraints' : {             --- The constraints for this GEOID, by type (based on how itertools needs to be used)
                                    {'simpleProduct', []},
                                    {'twoPlusRaces', []},
                                }
                 {'tuple_list'  : []}           --- all of the tuples for this geoid
            }
           - erased by update_constraints
        The tuples consist of:
        tuple=(pid,sex,age,wh,bl,aian,asian,nh,sor,hisp,VARIABLE_NAME,GEO_ID)
          p = person ID f"{geoid}_{sex}_{start_age}_{age}"
          s = sex
          a,wh,bl,aian,nh,sor,hisp = race attributes
         master_tuple_list usage:
           - extended by get_constraint_summary() when level='block'
           - then each tuple is appended to summary_nums[geo_id]['tuple_list']
           - t[10] for each tuple is written to the LP file; this defines all of the variables as binaries.
           - TODO: make the variable list a list of generators and expanding on writing

        @param summary_nums updated.
        @param master_tuple_list - updated
        """
        if self.debug:
            logging.info("get_constraint_summary  level: {} p01_data: {:,}B "
                         "data_dict: {:,}B summary_nums: {:,}B".
                  format(level,total_size(p01_data,verbose=False),total_size(data_dict,verbose=False),
                         total_size(summary_nums,verbose=False)))

        for s in data_dict:
            if s['table_number'] == 'P1':
                summary_nums[s[GEOID]]['n_people'] = s['value']
            constraint_value = s['value'] # constraint rhs; i.e., count of persons of types relevant to this tabulation
            # Beginning from default ranges, determine allowable ranges (some of these ignored in twoPlusRaces case)
            S     = SEX_RANGE
            A     = AGE_RANGE
            Hisp  = HISPANIC_RANGE
            Wh =    [s['white']] if s['white']  != MISSING else WHITE_RANGE
            Bl =    [s['black']] if s['black']  != MISSING else BLACK_RANGE
            Aian =  [s['aian']] if s['aian']    != MISSING else AIAN_RANGE
            Asian = [s['asian']] if s['asian']  != MISSING else ASIAN_RANGE
            NH =    [s['nhopi']] if s['nhopi']  != MISSING else HAWAIIAN_RANGE
            Sor =   [s['sor']] if s['sor']      != MISSING else SOR_RANGE
            if s[HISPANIC] != MISSING:
                if s[HISPANIC]==HISPANIC:
                    Hisp=['Y']
                else: Hisp = [s[HISPANIC]]
            sex_dict = {BOTH: SEX_RANGE, MALE: [MALE], FEMALE: [FEMALE]}
            if s['sex'] in sex_dict.keys():
                S = sex_dict[s['sex']]
            elif s['sex'] == MISSING:
                pass
            else:
                raise RuntimeError('invalid sex: ', s['sex'])
            if s['start_age'] != MISSING:
                A = [x for x in range(int(s['start_age']), int(s['end_age']) + 1)]

            # Add appropriate constraint representation as list, &, if this is Table P12, tuple representations for variables
            logging.info(f"Considering whether to add constraints, variables for table_number {s['table_number']}...")
            verboseLog(f"Input data for constraint: {s}")
            self.tables_encountered.add(s['table_number'])
            if s['table_number'] in ALLOWABLE_TABLES:
                if s['two']=='two': # Case for tabulations that count 2+ races
                    if s['table_number'] in TWO_PLUS_RACE_TABLES_TO_INCLUDE_AS_CONSTRAINTS:
                        logging.info(f"Adding a constraint for table_number {s['table_number']}...")
                        constraint = [constraint_value, S, A, Hisp, s['table_number'], s[GEOID], s[CELL_NUMBER]]
                        summary_nums[s[GEOID]]['constraints']['twoPlusRaces'].append(constraint) # Add twoPlusRaces constraints
                        self.tables_with_constraints.add(s['table_number'])
                else: # Case for tabulations that do not count 2+ races
                    logging.info(f"Adding a constraint for table_number {s['table_number']}...")
                    constraint = ([constraint_value, S, A, Wh, Bl, Aian, Asian, NH, Sor, Hisp,
                                    s['table_number'], s[GEOID], s[CELL_NUMBER]])
                    summary_nums[s[GEOID]]['constraints']['simpleProduct'].append(constraint) # Add simpleProduct constraint
                    self.tables_with_constraints.add(s['table_number'])

                    if s['table_number'] == 'P12' and not (s['start_age'] == 0 and s['end_age'] == 110) and constraint_value > 0:
                        logging.info(f"Adding variables from table_number {s['table_number']}...")
                        s_tuple_list = ([f"{s[GEOID]}_{s['sex']}_{s['start_age']}_{p}",
                                        s['sex'], age, wh, bl, AI, As, nh, so, hisp]
                                        for p in range(0, int(s['value']))
                                        for wh in Wh
                                        for bl in Bl
                                        for AI in Aian
                                        for As in Asian
                                        for nh in NH
                                        for so in Sor
                                        for hisp in Hisp
                                        for age in [x for x in range(int(s['start_age']), int(s['end_age']) + 1)])
                        if level=='block':
                            for variable_tuple in s_tuple_list: # Evaluates big generator [slow!]
                                # Example element: ['010010201001000_male_5.0_0', 'male', 5, 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y']
                                if not DISALLOW_ZERO_RACE_VARIABLES or not np.all(np.array(variable_tuple[3:-1]) == 'N'):
                                    row = variable_tuple + ['C_'+'_'.join(map(str, variable_tuple))] + [s[GEOID]]
                                    summary_nums[s[GEOID]]['tuple_list'].append(row) # No benefit to master_tuple_list as generator,
                                    self.master_tuple_list.append(row) # b/c it is used twice (make tract_summary_nums, & LP write)
                                    verboseLog(f"Added variable row to master_tuple_list: {row}")
            else:
                logging.info(f"Not adding a constraint for table_number {s['table_number']}...")
            logging.info(f"Tables encountered, so far, during constraint/variable generation: {self.tables_encountered}")
            logging.info(f"Tables not encountered, but allowed: {set(ALLOWABLE_TABLES).difference(self.tables_encountered)}")
            msg = f"Tables we added at least 1 constraint for: {self.tables_with_constraints}"
            logging.info(msg)

    def build_tract_lp(self):
        """Main entry point for building new tract LP files: Modified to write gzipped LP files b/c LP files are large"""
        state_code    = dbrecon.state_fips(self.stusab)
        geo_id        = self.sf1_tract_data[0][GEOID]
        if self.output:
            outfilename = self.output
        else:
            lpfilenamegz  = LPFILENAMEGZ(stusab=self.stusab,county=self.county,tract=self.tract)
            # Check to see if file is already finished, indicate that, & indicate that we don't know when it was started
            if dbrecon.validate_lpfile(lpfilenamegz): # NOTE: if debugging, may need to comment this out to --force lp overwrite
                logging.info(f"{lpfilenamegz} at {state_code}{self.county}{self.tract} validates.")
                dbrecon.db_done(dbrecon.auth(),'lp',self.stusab, self.county, self.tract, clear_start=True)
                return

            if lpfilenamegz.startswith('s3://'): # If outputting to s3, make output file a name temp
                outfilename = tempfile.NamedTemporaryFile(suffix='.gz',delete=False).name
            else:
                outfilename = lpfilenamegz.replace(".gz",".tmp.gz")

        dmakedirs(os.path.dirname(outfilename))
        t0         = time.time()
        start      = datetime.datetime.now()
        logging.info(f"{state_code}{self.county}{self.tract} tract_data_size:{sys.getsizeof(self.sf1_tract_data):,} ; "
                     "block_data_size:{sys.getsizeof(self.sf1_block_data):,} ")

        # When SLG was developing, didn't want to update the database when debugging
        if self.debug==False:
            # db_start_at_end prevents db from being updated with db_start() until LP file is created. We do that under spark
            if self.db_start_at_end==False:
                dbrecon.db_start(dbrecon.auth(), LP, self.stusab, self.county, self.tract)
            atexit.register(self.db_fail)

        if self.dry_run:
            logging.warning(f"DRY RUN: Will not create {outfilename}")
            return

        # Block constraint building: loop over blocks to get block constraints, master tuple list
        # Pass in sf1_block_dict and sf1 tract dict, &, get constraints from the block dict for all the blocks in the tract
        block_summary_nums = makeSummaryNumsDefaultDict()
        for block in self.sf1_block_data:
            self.get_constraint_summary('block', self.sf1_block_data, self.sf1_block_data[block],
                                        block_summary_nums)
        logging.info(f"{self.stusab} {self.county} {self.tract}: done getting block summary constraints")

        # Create output LP file and writes header
        lp_outfile = dopen(outfilename,'w')
        lp_outfile.write("\* DB Recon *\ \n")
        lp_outfile.write("Minimize \n")
        lp_outfile.write("Arbitrary_Objective_Function: __dummy \n")
        lp_outfile.write("Subject To \n")

        # Loop through the block constraints and write them to LP file
        n_con = 1 # initial connection
        n_con = update_constraints(lp_outfile, 'block', n_con, block_summary_nums,geo_id)

        # Loop through blocks in the tract to get block constraints and the master tuple list
        logging.info(f"block_summary_nums: {total_size(block_summary_nums):,}")
        if self.debug:
            logging.warning(f"block_summary_nums: {total_size(block_summary_nums):,}")
        del block_summary_nums

        # Tract constraint building
        tract_summary_nums = makeSummaryNumsDefaultDict()
        self.get_constraint_summary('tract', self.sf1_tract_data, self.sf1_tract_data, tract_summary_nums)
        logging.info(f"{self.stusab} {self.county} {self.tract}: done with tract summary")

        # For tracts, need to add the master_tuple_list just once
        for i in self.master_tuple_list:
            tract_summary_nums[geo_id]['tuple_list'].append(i)

        # Loop through the tract constraints to write to file.
        n_con = update_constraints(lp_outfile, 'tract', n_con, tract_summary_nums, geo_id)
        logging.info(f"tract_summary_nums: {total_size(tract_summary_nums):,}")
        logging.info(f"master_tuple_list: {total_size(self.master_tuple_list):,}")
        if self.debug:
            logging.info(f"tract_summary_nums: {total_size(tract_summary_nums):,}") # TODO: why do this twice?
            logging.info(f"master_tuple_list: {total_size(self.master_tuple_list):,}")
        del tract_summary_nums

        # Write the collected variable names to the file, since they are all binaries
        lp_outfile.write("Bounds\n  __dummy = 0 \n Binaries \n")
        for t in self.master_tuple_list:
            lp_outfile.write(t[10])
            lp_outfile.write('\n')
        lp_outfile.write('End\n')
        lp_outfile.close()

        # If we were just asked to create the output file, notify of that and return
        if self.output:
            logging.info("%s created",self.output)
            return

        # Otherwise, rename the file (which may upload it to s3), and update the databse
        dbrecon.drename(outfilename, lpfilenamegz)
        wait_bucket, wait_key = s3.get_bucket_key(lpfilenamegz)
        dbrecon.dwait_exists_boto3(wait_bucket, wait_key)
        dbrecon.db_done(dbrecon.auth(), LP, self.stusab, self.county, self.tract, start=start)
        DBMySQL.csfr(dbrecon.auth(), f"UPDATE {REIDENT}tracts SET lp_gb=%s WHERE stusab=%s AND county=%s AND tract=%s",
                     (dbrecon.maxrss()//GB,self.stusab, self.county, self.tract))
        atexit.unregister(self.db_fail)

        if self.debug:
            logging.info("debug completed.")
            logging.info(f"Elapsed seconds: {int(time.time() - t0)}")
            dbrecon.print_maxrss()
            dbrecon.add_dfxml_tag('synth_lp_files','', {'success':'1'})
            exit(0)
        logging.info("build_tract_lp %s %s %s finished",self.stusab,self.county,self.tract)


def build_tract_lp_tuple(tracttuple):
    """Make the tract LP files; cannot be a local fxn b/c called from multiprocessing map, & multiprocessing can't call local fxns"""
    (stusab, county, tract, sf1_tract_data, sf1_block_data, db_start_at_end, output, dry_run) = tracttuple
    try:
        lptb = LPTractBuilder(stusab, county, tract, sf1_tract_data, sf1_block_data, db_start_at_end=db_start_at_end, output=output, dry_run=dry_run)
        lptb.build_tract_lp()
    except MemoryError as e:
        logging.error(f"UNSOLVED MEMORY ERROR in {stusab} {county} {tract}: {e}")
        cmd = f"""
        UPDATE {REIDENT}tracts SET hostlock=NULL,lp_start=NULL,lp_end=NULL,lp_host=NULL
        WHERE stusab=%s and county=%s and tract=%s
        """
        DBMySQL.csfr(dbrecon.auth(),cmd, (stusab, county, tract))

def tractinfo(auth, stusab, county, tract):
    sf1_tract_data_file = dpath_expand( dbrecon.SF1_TRACT_DATA_FILE(stusab=stusab,county=county) )
    if not dpath_exists(sf1_tract_data_file):
        logging.error(f"ERROR. NO TRACT DATA FILE {sf1_tract_data_file} for {stusab} {county} ")
        return
    print("sf1_tract_data_file:",sf1_tract_data_file)
    sf1_tract_reader    = csv.DictReader(dopen( sf1_tract_data_file,'r'))
    for (ct,s) in enumerate(sf1_tract_reader):
        print(f"ct={ct} columns={len(s)} s['STATE']={s['STATE']}")

def make_state_county_files(auth, stusab, county, tractgen=ALL, *, debug=False, db_start_at_end=False, force=None, output=None, dry_run=None, reident=None, sf1_vars):
    """
    Support for multi-threading. tracttuple contains the stusab, county, tract, and sf1_tract_dict
    Reads the data files for the state and county, then call build_tract_lp to build the LP files for each tract.
    All of the tract LP files are built from the same data model, so they can be built in parallel with shared memory.
    Consults the database to see which files need to be rebuilt, and only builds those files.
    For the large counties, this can take > 350GB
    """
    assert (stusab[0].isalpha()) and (len(stusab)==2)
    assert (county[0].isdigit()) and (len(county)==3)
    stusab = stusab.lower() # Make sure it is lower case
    if reident:
        dbrecon.set_reident(REIDENT)
    logging.info(f"make_state_county_files({stusab},{county},{tractgen})")

    if force: # Find the tracts in this county that do not yet have LP files
        tracts = [tractgen]
    else:
        tracts_needing_lp_files = dbrecon.get_tracts_needing_lp_files(auth, stusab, county)
        logging.info("tracts_needing_lp_files (%s,%s): %s",stusab,county,tracts_needing_lp_files)
        if tractgen==ALL:
            if len(tracts_needing_lp_files)==0:
                logging.warning(f"make_state_county_files({stusab},{county},{tractgen}) "
                                f"- No more tracts need LP files")
                return
            tracts = tracts_needing_lp_files
        else:
            if tractgen not in tracts_needing_lp_files:
                lpgz_filename = dbrecon.LPFILENAMEGZ(stusab=stusab,county=county,tract=tractgen) # Check if tract file large enough
                if dpath_exists(lpgz_filename) and dgetsize(lpgz_filename) < dbrecon.MIN_LP_SIZE:
                    logging.warning(f"{lpgz_filename} exists but is too small ({dgetsize(lpgz_filename)}); deleting")
                    remove_lpfile(auth, stusab, county, tractgen)
                else:
                    logging.warning(f"make_state_county_files({stusab},{county},{tractgen}) "
                                    f"- tract {tractgen} not in tracts needing lp files: {tracts_needing_lp_files}")
                    logging.warning(f"Use --force to force")
                    return
            tracts = [tractgen]

    state_code = dbrecon.state_fips(stusab)

    # Has the variables and the collapsing values we want (e.g, to collapse race, etc) [these data frames are all quite small]
    assert len(sf1_vars) > 0
    make_attributes_categories(sf1_vars)
    sf1_vars_block = sf1_vars[(sf1_vars['level']=='block')]
    sf1_vars_tract = sf1_vars[(sf1_vars['level']=='tract')]

    # Read the actual data -- reformatted sf1 [these files are not that large]
    sf1_block_data_file = dpath_expand( dbrecon.SF1_BLOCK_DATA_FILE(stusab=stusab,county=county) )
    if not dpath_exists(sf1_block_data_file):
        logging.error(f"ERROR. NO BLOCK DATA FILE {sf1_block_data_file} for {stusab} {county} ")
        return
    sf1_block_reader    = csv.DictReader(dopen( sf1_block_data_file,'r'))

    ## make sf1_block_list, looks like this:
    ##                   geoid  tablevar  value
    ## 0       020130001001363  P0010001   24.0
    ## 1       020130001001363  P0020001   24.0
    ## ...                 ...       ...    ...
    ## 121903  020130001003156  P039I019    0.0
    ## 121904  020130001003156  P039I020    0.0
    ##
    ## This is the cartesian product of all of the blocks and all of
    ## the tablevars in the tables that cover these blocks.  'value'
    ## is the value for that variable for that geoid, as read from the
    ## SF1 files.
    ##
    ## This table is large because the geoids are strings (80 bytes)
    ## instead of integers (which would require encoding the leading
    ## 0), and because the tablevars are strings (80 bytes). But it's
    ## not that large, since the records are by blocks.  In the 2010
    ## Census geography file, the tract with the most blocks is Idaho
    ## 015, which has 3449.  The county with the most blocks is
    ## California 37, which has 109582.
    ##
    ## TODO: Improve this by turning the geoids and tablevars into categories.
    ##

    logging.info("building sf1_block_list for %s %s",stusab,county)
    sf1_block_list = []
    for s in sf1_block_reader:
        temp_list = []
        # Only process the block records that have non-zero population
        if s['STATE'][:1].isdigit() and int(s['P0010001'])>0:
            geo_id=str(s['STATE'])+str(s['COUNTY']).zfill(3)+str(s['TRACT']).zfill(6)+str(s['BLOCK'])
            for k,v in list(s.items()):
                if k[:1]=='P' and geo_id[:1]!='S' and v.strip()!='':
                    sf1_block_list.append([geo_id,k,float(v)])

    assert len(sf1_block_list) > 0
    sf1_block     = pd.DataFrame.from_records(sf1_block_list, columns=[GEOID,TABLEVAR,'value'])
    assert len(sf1_block)>0

    make_attributes_categories(sf1_block)
    sf1_block_all = pd.merge(sf1_block, sf1_vars_block, how='inner',
                             left_on=[TABLEVAR], right_on=[CELL_NUMBER])
    sf1_block_all['value'].fillna(0)

    ## make sf1_block_dict.
    ## This is a dictionary of dictionaries of lists where:
    ## sf1_block_dict[tract][block] = list of sf1_block rows from the SF1 data that match each block
    logging.info("collecting tract and block data for %s %s",stusab,county)
    sf1_block_records = sf1_block_all.to_dict(orient='records')
    assert len(sf1_block_records) > 0

    sf1_block_dict = {}
    for d in sf1_block_records:
        tract=d[GEOID][5:11]  # tracts are six digits
        block=d[GEOID][11:15] # blocks are 4 digits (the first digit is the block group)
        if tract not in sf1_block_dict:
            sf1_block_dict[tract]={}
        if block not in sf1_block_dict[tract]:
            sf1_block_dict[tract][block]=[]
        sf1_block_dict[tract][block].append(d)

    ## To clean up, delete all of the temporary dataframes
    del sf1_block,sf1_block_all,sf1_block_records,tract,block
    gc.collect()

    ## make sf1_tract_dict: this is a dictionary of lists where
    ## sf1_block_dict[tract] = list of sf1_tract rows from the SF1 data that match each tract
    sf1_tract_data_file = dpath_expand( dbrecon.SF1_TRACT_DATA_FILE(stusab=stusab,county=county) )
    if not dpath_exists(sf1_tract_data_file):
        logging.error(f"ERROR. NO TRACT DATA FILE {sf1_tract_data_file} for {stusab} {county} ")
        return
    sf1_tract_reader    = csv.DictReader(dopen( sf1_tract_data_file,'r'))

    logging.info("getting tract data for %s %s from %s",stusab,county,sf1_tract_data_file)
    sf1_tract_list=[]
    error_count = 0
    none_errors = 0
    for (ct, s) in enumerate(sf1_tract_reader): # This repeats for every tract in the file
        if s['STATE'] is None:
            logging.error("s is none! ct:%s stusab:%s county:%s geo_id:%s ",ct,stusab,county,geo_id)
            none_errors += 1
            continue
        try:
            if s['STATE'][:1].isdigit() and int(s['P0010001'])>0:
                geo_id=str(s['STATE'])+str(s['COUNTY']).zfill(3)+str(s['TRACT']).zfill(6)
                for k,v in list(s.items()):
                    if k[:3]=='PCT' and geo_id[:1]!='S':
                        try:
                            sf1_tract_list.append([geo_id,k,float(v)])
                        except ValueError as e:
                            logging.error(f"stusab:{stusab} county:{county} geo_id:{geo_id} k:{k} v:{v}")
                            error_count += 1
                            if error_count>MAX_SF1_ERRORS:
                                return
        except TypeError as e:
            logging.error("s['STATE'] is none! ct:%s stusab:%s county:%s geo_id:%s ",ct,stusab,county,geo_id)
            logging.error(e)
            none_errors += 1
            continue

    if none_errors>0:
        print("None values read in sf1_tract_reader. In thie past, this has happened because of a memory allocation error in the Python dictReader....")
        raise RuntimeError("none_errors > 0 ")

    sf1_tract = pd.DataFrame.from_records(sf1_tract_list, columns=[GEOID,TABLEVAR,'value'])
    make_attributes_categories(sf1_tract)

    # merge data with var definitions
    sf1_tract_all = pd.merge(sf1_tract, sf1_vars_tract, how='inner',
                             left_on=[TABLEVAR], right_on=[CELL_NUMBER])
    sf1_tract_all['value'].fillna(0)

    sf1_tract_records = sf1_tract_all.to_dict(orient='records')

    # Try to reclaim the memory
    del sf1_tract_all
    gc.collect()

    sf1_tract_dict=defaultdict(list)
    for d in sf1_tract_records:
        tract=d[GEOID][5:]
        sf1_tract_dict[tract].append(d)
    logging.info("%s %s total_size(sf1_block_dict)=%s total_size(sf1_tract_dict)=%s",
                 stusab,county,total_size(sf1_block_dict),total_size(sf1_tract_dict))
    if debug:
        logging.info("sf1_block_dict total memory: {:,} bytes".format(total_size(sf1_block_dict))) # TODO: switch to f-string
        logging.info(f"sf1_tract_dict has data for {len(sf1_tract_dict)} tracts.")
        logging.info("sf1_tract_dict total memory: {:,} bytes".format(total_size(sf1_tract_dict))) # TODO: switch to f-string

    ### We have now made the data for this county. We now make LP files for a specific set of tracts, or all the tracts.
    ### 2021-03-25 patch:
    ###     Remove tracts not in the sf1_tract_dict and in the sf1_block_dict.
    ###     This was a problem for the water tracts, although we now do not pull them into the 'tracts' list from the SQL.
    ### TODO: We could just call LPBuilder here, and pass in an array of those, rather than doing the whole tuple thing
    ###     Next time, don't pass around tuples, always pass around objects.
    tracttuples = [(stusab, county, tract, sf1_tract_dict[tract], sf1_block_dict[tract], db_start_at_end, output, dry_run)
                   for tract in tracts
                   if (tract in sf1_tract_dict) and (tract in sf1_block_dict)]
    if args.j2>1:
        with multiprocessing.Pool( args.j2 ) as p:
            p.map(build_tract_lp_tuple, tracttuples)
    else:
        list(map(build_tract_lp_tuple, tracttuples))
    logging.info("make_state_county_files %s %s done",stusab,county)

if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Synthesize LP files for all of the tracts in the given state and county." )
    dbrecon.argparse_add_logging(parser)
    parser.add_argument("--j1",
                        help="Specifies number of threads for state-county parallelism. "
                        "These threads do not share memory. Specify 1 to disable parallelism.",
                        type=int,default=DEFAULT_J1)
    parser.add_argument("--j2",
                        help="Specifies number of threads for tract-level parallelism. "
                        "These threads share tract and block statistics. Specify 1 to disable parallelism.",
                        type=int,default=DEFAULT_J2)
    parser.add_argument("--dry_run", help="don't actually write out the LP files",action='store_true')
    parser.add_argument("--db_start_at_end", help="run the db_start and db_end together",action='store_true')
    parser.add_argument("--debug", help="Run in debug mode. Do not update database and write output to file specified by --output",action='store_true')
    parser.add_argument("--output", help="Specify output file. Requires that a single stusab/county/tract be specified")
    parser.add_argument("--force", help="Generate all tract files, even if they exist", action='store_true')
    parser.add_argument("--tractinfo", help="print summary information regarding sf1_tract_{state}{county}.csv - for debugging", action='store_true')

    parser.add_argument("stusab",  help="2-character stusab abbreviation.")
    parser.add_argument("county", help="3-digit county code")
    parser.add_argument("tract",  help="If provided, just synthesize for this specific 6-digit tract code. Otherwise do all in the county",nargs="?")

    args     = parser.parse_args()
    config   = dbrecon.setup_logging_and_get_config(args=args,prefix="03pan")
    args.stusab = args.stusab.lower()

    assert dbrecon.dfxml_writer is not None
    if args.debug:
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        root.addHandler(handler)
        logging.info("Info logging")
        logging.debug("Debug logging")

    if args.output or args.debug:
        logging.info("Memory debugging mode. Setting j1=1 and j2=1")
        args.j1 = 1
        args.j2 = 1

    auth = DBMySQLAuth.FromConfig(os.environ)
    if args.tractinfo:
        tractinfo(auth, args.stusab, args.county, args.tract)
        exit(0)

    # If we are doing a specific tract
    if not args.debug:
        dbrecon.db_lock(auth, args.stusab, args.county, args.tract, extra=' and lp_end is NULL')
    if args.tract:
        make_state_county_files(auth, args.stusab, args.county, args.tract, debug=args.debug, force=args.force, output=args.output, dry_run=args.dry_run, sf1_vars = dbrecon.sf1_vars())
    else:
        make_state_county_files(auth, args.stusab, args.county, debug=args.debug, force=args.force, output=args.output, dry_run=args.dry_run, sf1_vars = dbrecon.sf1_vars())
    logging.info("s3_pandas_synth_lp_files.py finished")
