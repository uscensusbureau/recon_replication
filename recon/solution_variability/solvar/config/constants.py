"""
Constants class -- translates all constants into the __iter__ method to be loaded by Solvar
"""
from pathlib import PurePath

class Constants:
    # ------------------------------------
    # S3 bucket path for solvar
    S3_ROOT = PurePath("uscb-decennial-ite-das")
    S3_SOLVAR_PATH = PurePath("title13_reid_cleanup/solution_variability")

    # Gurobi parameters
    MIP_GAP = 0.01
    THREADS = 60

    # Gurobi timeout return value
    TIMEOUT_RETURN_VALUE = 'TIMEOUT'
    #NOTE: This is just a code to indicate timeout and show that in the report. The actual timeout parameter is Solvar.gurobiTimeout.

    # we always write the lps needed for construction, but should we also write the finished sol variability lps? (*very* disk-hungry)
    WRITE_VARIABILITY_LPS = False

    #NOTE: above parameters *are* constant thru & across runs, but would be more appropriate in a config file, if we had one.
    # -----------------------------------

    # assumed max age (note that 110 corresponds to the largest age reported in table PCT12)
    SF1_2010_MAX_AGE = 110

    VAR_PLACEHOLDER = "C"
    RACE_COMPONENTS = [
        "isWhite",
        "isBlack",
        "isAian", # American Indian and Alaska Native
        "isAsian",
        "isNhopi", # Native Hawaian and Other Pacific Island
        "isSor", # Some Other Race
        "isHisp"
    ]
    VAR_COMPONENTS = [
        "placeholder",
        "blockGeocode",
        "sexUnused",
        "startAge",
        "person#",
        "sex",
        "age"
    ] + RACE_COMPONENTS

    # default schema grabs block, age, & all demo variables
    # Schemas with imprecise age buckets consider `startAge` instead of `actualAge`
    DEFAULT_SCHEMA = ["blockGeocode", "sex", "age"] + RACE_COMPONENTS
    DEFAULT_SCHEMA_IMPRECISE_AGE_BUCKETS = ["blockGeocode", "sex", "startAge"] + RACE_COMPONENTS

    AGE_SCHEMA = ["blockGeocode", "age"]
    AGE_SCHEMA_IMPRECISE_AGE_BUCKETS = ["blockGeocode", "startAge"]

    # demos schema grabs block, demo variables
    DEMO_SCHEMA = ["blockGeocode", "sex"] + RACE_COMPONENTS 

    VALID_SCHEMAS = (None, DEFAULT_SCHEMA, AGE_SCHEMA, DEMO_SCHEMA, DEFAULT_SCHEMA_IMPRECISE_AGE_BUCKETS, AGE_SCHEMA_IMPRECISE_AGE_BUCKETS)

    # The age buckets ranges
    AGE_BUCKETS = list(range(22)) + [22,25,30,35,40,45,50,55,60,62,65,67,70,75,80,85]

    CODE2ST = {}
    CODE2ST['01'] = 'al'
    CODE2ST['02'] = 'ak'
    CODE2ST['04'] = 'az'
    CODE2ST['05'] = 'ar'
    CODE2ST['06'] = 'ca'
    CODE2ST['08'] = 'co'
    CODE2ST['09'] = 'ct'
    CODE2ST['10'] = 'de'
    CODE2ST['11'] = 'dc'
    CODE2ST['12'] = 'fl'
    CODE2ST['13'] = 'ga'
    CODE2ST['15'] = 'hi'
    CODE2ST['16'] = 'id'
    CODE2ST['17'] = 'il'
    CODE2ST['18'] = 'in'
    CODE2ST['19'] = 'ia'
    CODE2ST['20'] = 'ks'
    CODE2ST['21'] = 'ky'
    CODE2ST['22'] = 'la'
    CODE2ST['23'] = 'me'
    CODE2ST['24'] = 'md'
    CODE2ST['25'] = 'ma'
    CODE2ST['26'] = 'mi'
    CODE2ST['27'] = 'mn'
    CODE2ST['28'] = 'ms'
    CODE2ST['29'] = 'mo'
    CODE2ST['30'] = 'mt'
    CODE2ST['31'] = 'ne'
    CODE2ST['32'] = 'nv'
    CODE2ST['33'] = 'nh'
    CODE2ST['34'] = 'nj'
    CODE2ST['35'] = 'nm'
    CODE2ST['36'] = 'ny'
    CODE2ST['37'] = 'nc'
    CODE2ST['38'] = 'nd'
    CODE2ST['39'] = 'oh'
    CODE2ST['40'] = 'ok'
    CODE2ST['41'] = 'or'
    CODE2ST['42'] = 'pa'
    CODE2ST['44'] = 'ri'
    CODE2ST['45'] = 'sc'
    CODE2ST['46'] = 'sd'
    CODE2ST['47'] = 'tn'
    CODE2ST['48'] = 'tx'
    CODE2ST['49'] = 'ut'
    CODE2ST['50'] = 'vt'
    CODE2ST['51'] = 'va'
    CODE2ST['53'] = 'wa'
    CODE2ST['54'] = 'wv'
    CODE2ST['55'] = 'wi'
    CODE2ST['56'] = 'wy'
    CODE2ST['72'] = 'pr'

    ST2CODE = {v: k for k, v in CODE2ST.items()}
