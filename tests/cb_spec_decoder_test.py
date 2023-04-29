#
# make sure that we can read the sf1 file

from das_constants       import *
from cb_spec_decoder import *
from decimal import Decimal
from typing import Dict

SF1_CHAPTER6_CSV = CHAPTER6_CSV_FILE.format(year=2010,product=SF1)

# LINES FROM THE 2010 SF1 that did not OCR or match properly
SF1_P6_LINE='other races                                                                              P0060007              03          9,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,'
SF1_P0090058='Race                                                                          P0090058              03          9,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,'

SF1_LINE_100='"Place (FIPS)7, 8                                                                                    PLACE'
SF1_LINE_102='FIPS Place Class Code8                                                                 PLACECC,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,2,,,,,,51,,,,,,,,A/N,'

SF1_GEO_NAME_LINE='Area Name-Legal/Statistical Area Description (LSAD) Term-Part Indicator18,,,,,,,,,,,,,,,,,,,,,,NAME,,,,,,,,,,,,,,,,,90,,,,,,227,,,,,,,,A/N,'
SF1_GEO_STUSAB_LINE = 'State/U.S. Abbreviation (USPS)                                                    STUSAB                    2                7              A,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,'
# note that this is not a hyphen:
SF2_LINE_120 = 'Census Tract7 000100–998999,,,,,,,,,,,Census tract,,,,,,,,,,,,,,,,,TRACT,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,6,,,,,,,,,,55,,,,,,A/N,,,'

AINSF_LINE_3375="Chapter 6.,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"
AINSF_LINE_3376="Data Dictionary                                                                        ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"


def test_csvspec_prepare_csv_line() -> None:
    assert csvspec_prepare_csv_line(SF1_H22_LINE)=="H22. ALLOCATION OF TENURE"


def test_VAR_FIELDS_RE() -> None:
    m = VAR_FIELDS_RE.search("P035F001")
    assert m.group('prefix')=='P'
    assert m.group('tablenumber')=='035'
    assert m.group('sequence')=='F'
    assert m.group('number')=='001'
    

def test_geo_vars() -> None:
    pline = csvspec_prepare_csv_line( SF1_GEO_STUSAB_LINE )
    print(pline)
    m = GEO_VAR_RE.search( pline )
    assert m.group('name') == 'STUSAB'

    pline = csvspec_prepare_csv_line( SF1_GEO_NAME_LINE )
    print(pline)
    m = GEO_VAR_RE.search( pline )
    assert m.group('name') == 'NAME'
    assert m.group('width') == '90'
    assert m.group('column') == '227'


def test_line_100_102() -> None:
    m = GEO_VAR_RE.search( csvspec_prepare_csv_line(SF1_LINE_102) )
    assert m.group('name') == 'PLACECC'


def test_sf2_line_102() -> None:
    line = csvspec_prepare_csv_line( SF2_LINE_120)
    print("line:",line)
    m = GEO_VAR_RE.search( line )
    assert m.group('name') == 'TRACT'

SF1_LINE_4016='FAMILIES (TWO OR MORE RACES HOUSEHOLDER) [1]",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,P035F001,,,,,,,,,,12,,,,,,,9'
def test_line_4016() -> None:
    prepared = csvspec_prepare_csv_line( SF1_LINE_4016)
    print("prepared:",prepared)
    assert 'FAMILIES (TWO OR MORE RACES HOUSEHOLDER)' in prepared
    assert "P035F001" in prepared
    assert '12' in prepared
    (name,desc,segment,maxsize) = parse_variable_desc( csvspec_prepare_csv_line( SF1_LINE_4016))
    assert name=='P035F001'
    assert desc=='FAMILIES (TWO OR MORE RACES HOUSEHOLDER)'
    assert segment==12

SF1_LINE_13306='Total:,,,,,,,,,,,,,,,,,,,,,,,,PCT0230001,,,,,,,,,,,,,,48,,,,,,,9,,,,,,,,,'
def test_line_13306() -> None:
    assert is_variable_name("PCT0230001")

    prepared = csvspec_prepare_csv_line( SF1_LINE_13306 )
    assert "Total" in prepared
    assert "PCT0230001" in prepared
    assert "48" in prepared
    print(prepared)
    (name,desc,segment,maxsize) = parse_variable_desc( prepared )
    assert name=='PCT0230001'
    assert desc=='Total'
    assert segment==48


SF1_H22_LINE='H22.,,,,"ALLOCATION OF TENURE [3]'
def test_parse_table_name() -> None:
    (table,title) = parse_table_name(csvspec_prepare_csv_line(SF1_H22_LINE))
    assert table=='H22'
    assert title=='ALLOCATION OF TENURE'

def test_H22_LINE_parses_chapter6() -> None:
    for line in csvspec_lines(SF1_CHAPTER6_CSV):
        if line.strip().startswith("H22."):
            # I have the line. Make sure we find the tables in it.
            (table,title) = parse_table_name(line)
            assert table=='H22'
            assert title=='ALLOCATION OF TENURE'
            return True
    raise RuntimeError(f"SF1_H22_LINE not found in {SF1_CHAPTER6_CSV}")


def test_P0090058_parser() -> None:
    (name,desc,segment,maxsize) = parse_variable_desc(csvspec_prepare_csv_line(SF1_P0090058))
    assert name=='P0090058'
    assert desc=='Race'
    assert segment==3
    assert maxsize==9


def tables_in_file(fname: str) -> Dict:
    """Give a chapter6 CSV file, return all of the tables in it."""
    tables = {}
    for line in csvspec_lines(fname):
        (table,title) = parse_table_name(line)
        if table:
            tables[table] = title
    return tables


def test_tables_in_sf1() -> None:
    tables = tables_in_file(SF1_CHAPTER6_CSV)
    for table in sorted(tables):
        print(table,tables[table])
    for p in range(1,50):
        assert f"P{p}" in tables
    for pct in range(1,24):
        assert f"PCT{pct}" in tables
    for i in range(ord('A'),ord('O')+1):
        ch = chr(i)
        # We don't parse PCT12G because it OCRs badly
        if ch!='G':
            assert f"PCT12{ch}" in tables
    for h in range(1,23):
        assert f"H{h}" in tables
    for i in range(ord('A'),ord('I')+1):
        ch = chr(i)
        assert f"H11{ch}" in tables


def test_schema_segment3() -> None:
    schema = schema_for_spec(SF1_CHAPTER6_CSV, year=2010, product=SF1)
    # format is table #, max variable number
    ptables = [(3,8),
               (4,3),
               (5,17),
               (6,7),
               (7,15),
               (8,71),
               (9,73)]
    for (table,maxvars) in ptables:
        tablename = f'P{table}'
        if tablename not in schema.tabledict:
            raise RuntimeError(f"table {tablename} not in {schema.tabledict.keys()}")
        t = schema.get_table(tablename)
        for v in range(1,maxvars+1):
            varname = f'P{table:03}{v:04}'
            assert varname in t.vardict


def Xtest_spottest_2010_sf1() -> None:
    year = 2010
    product = SF1
    state  = 'ak'
    ch6file = CHAPTER6_CSV_FILE.format(year=year,product=product)
    assert os.path.exists(ch6file)
    schema = schema_for_spec(ch6file, year=year, product=product)

    p3 = schema.get_table('P3')
    ypss = YPSS(year, product, state, p3.attrib[CIFSN])
    for line in open_decennial( ypss ):
        first_line = line
        break
    p3.dump()
    print("first line of ",ypss)
    print(first_line)
    d3 = p3.parse_line_to_dict(first_line)
    assert d3[FILEID]   == 'SF1ST'
    assert d3[STUSAB]   == 'AK'
    assert d3[CHARITER] == '000'
    assert d3[CIFSN]    == '03'
    assert d3[LOGRECNO] == '0000001'
    assert d3['P0030001'] == 710231
    assert d3['P0030002'] == 473576
    assert d3['P0030003'] == 23263
    assert d3['P0030004'] == 104871
    assert d3['P0030005'] == 38135
    assert d3['P0030006'] == 7409
    assert d3['P0030007'] == 11102
    assert d3['P0030008'] == 51875
    
    # Make sure the second table in a file works
    p4 = schema.get_table('P4')
    d4 = p4.parse_line_to_dict(first_line)
    assert d4[FILEID]   == 'SF1ST'
    assert d4[STUSAB]   == 'AK'
    assert d4[CHARITER] == '000'
    assert d4[CIFSN]    == '03'
    assert d4[LOGRECNO] == '0000001'
    assert d4['P0040001'] == 710231
    assert d4['P0040002'] == 670982
    assert d4['P0040003'] == 39249

    # Let's make sure decimal works!
    p17 = schema.get_table('P17')
    p17.dump()
    ypss = YPSS(year, product, state, p17.attrib[CIFSN])
    for line in open_decennial( ypss ):
        first_line = line
        break
    print("first line of ",ypss)
    print(first_line)
    d17 = p17.parse_line_to_dict(first_line)
    assert d17[FILEID]   == 'SF1ST'
    assert d17[STUSAB]   == 'AK'
    assert d17[CHARITER] == '000'
    assert d17[CIFSN]    == '05'
    assert d17[LOGRECNO] == '0000001'
    assert d17['P0170001'] == Decimal('2.65')
    assert d17['P0170002'] == Decimal('0.72')
    assert d17['P0170003'] == Decimal('1.93')

    
TEST_STATE = 'ak'
TEST_YEAR_PRODUCTS = [(2000,PL94),
                      (2010,PL94),
                      (2010,SF1)]


def test_parsed_spec_fields_correct() -> None:
    """For the each of the years and products, look at the ak files and
    make sure that we can account for every column.  Eventually we
    will want to verify that a line read with the spec scanner from
    various files match as well.
    """
    errors = 0
    for year,product in TEST_YEAR_PRODUCTS:
        chariter = '000'
        specfile = get_cvsspec(year=year,product=product)
        assert os.path.exists(specfile)
        schema = schema_for_spec(specfile, year=year, product=product)

        # Get a line from a file and make sure that the fields match
        for cifsn in range(1,SEGMENTS_FOR_YEAR_PRODUCT[year][product]+1):
            state  = TEST_STATE
            ypss   = YPSS(year, product, state, cifsn)
            line   = open_decennial( ypss ).readline().strip()
            fields = line.split(",")
            assert fields[0] == FILE_LINE_PREFIXES[year][product] # FILEID
            assert fields[1].lower() == state                     # STUSAB
            assert fields[2] == chariter                          # CHARITER
            assert int(fields[3]) == cifsn                        # CIFSN
            assert int(fields[4]) == 1                            # LOGRECNO

            # make sure that the total number of fields matches those for our spec.
            # do this by finding all of the tables that have this 
            # print the line
            total_fields = 0
            tables_in_this_file = []
            for table in schema.tables():
                if table.attrib['CIFSN']==cifsn:
                    tables_in_this_file.append(table)
                    if total_fields==0:
                        total_fields += len(table.vars())
                    else:
                        # we only have these five fields on the first table
                        total_fields += len(table.vars()) - 5 
            if len(fields) != total_fields:
                print(f"File {cifsn} Found {len(fields)} values; expected {total_fields}")
                print(f"Tables found:")
                for table in tables_in_this_file:
                    print(f"Spec says {table.name} has {len(table.varnames())} variables.")
                print()
                print(f"First line of {TEST_STATE} file part {cifsn}:")
                print(line)
                # Make a list of all the variables I think I have, and the value I found
                file_vars = []
                for (ct,table) in enumerate(tables_in_this_file):
                    for var in table.vars():
                        if ct==0 or var.name not in LINKAGE_VARIABLES:
                            file_vars.append(var.name)
                while len(file_vars) < len(fields):
                    file_vars.append("n/a")
                while len(file_vars) > len(fields):
                    fields.append("n/a")

                for (i,(a,b)) in enumerate(zip(file_vars,fields),1):
                    if a[-3:]=='001':
                        print()
                    print(f"file {cifsn} field {i}  {a}   {b}")
                errors += 1
    if errors>0:
        raise RuntimeError("Errors found")
                
            
def test_find_data_dictionary() -> None:
    assert is_chapter_line(AINSF_LINE_3375)
    assert is_data_dictionary_line(AINSF_LINE_3376)
    

def test_spottest_2010_sf2() -> None:
    year = 2010
    product = SF1
    state  = 'ak'
    ch6file = CHAPTER6_CSV_FILE.format(year=year,product=product)
    assert os.path.exists(ch6file)
    schema = schema_for_spec(ch6file, year=year, product=product)
    pco1 = schema.get_table("PCO1")

    
def test_find_data_dictionary() -> None:
    assert is_chapter_line(AINSF_LINE_3375)
    assert is_data_dictionary_line(AINSF_LINE_3376)


def test_find_all_data_dictionaries() -> None:
    for (year, product) in year_product_iterator():
        filename = get_cvsspec(year=year, product=product)
        print(filename)
        last_line = ""
        found = False
        if not any( (is_chapter_line( line) for line in csvspec_lines(filename))) :
            raise RuntimeError(f"{filename} has no chapter lines in it")

        if not any( (is_data_dictionary_line(line) for line in csvspec_lines(filename))) :
            raise RuntimeError(f"{filename} has no data dictionary lines in it")

        for line in csvspec_lines(filename):
            if is_chapter_line(last_line) and is_data_dictionary_line(line):
                found = True
                break
            last_line = line
        if not found:
            raise RuntimeError(f"{filename} does not appear to have a data dictionary")
        print("---")

