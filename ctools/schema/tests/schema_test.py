#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Test the Census ETL schema package

import io
import os
import sys

from os.path import abspath
from os.path import dirname

sys.path.append(dirname(dirname(dirname(dirname(abspath(__file__))))))

import ctools
import ctools.schema as schema
import pytest

from ctools.schema import valid_sql_name,unquote,sql_parse_create,decode_vtype
from ctools.schema.code_snippet import CodeSnippet
from ctools.schema.table import Table
from ctools.schema.schema import Schema
from ctools.schema.variable import Variable


@pytest.fixture
def s():
    """ Create a schema fixture for easier testing """
    s = Schema()
    t = Table(name="students")
    s.add_table(t)
    name = Variable(name="name",vtype='VARCHAR(4)',column=0,width=4,start=1,end=4)
    age  = Variable(name="age",vtype='INTEGER(2)',column=4,width=2,start=5,end=6)
    t.add_variable(name)
    t.add_variable(age)
    return s

def test_valid_sql_name():
    assert valid_sql_name("MDF_TabulationGeography")==True
    assert valid_sql_name("CEFVW_PP10_OP")==True

#def test_TYPE_WIDTH_RE():
#    m = TYPE_WIDTH_RE.search("VARCHAR(20)")
#    assert m is not None
#    assert m.group('name')=='VARCHAR'
#    assert m.group('width')=='20'
#
#    m = TYPE_WIDTH_RE.search("VARCHAR2")
#    assert m is not None
#    assert m.group('name')=='VARCHAR2'
#    assert m.group('width') is None

def test_decode_vtype():
    print("*** schema: ",schema)
    assert decode_vtype("VARCHAR(20)") == (schema.TYPE_VARCHAR, 20)
    assert decode_vtype("VARCHAR2") == (schema.TYPE_VARCHAR, schema.DEFAULT_VARIABLE_WIDTH)

def test_unquote():
    assert unquote("'this'") == "this"
    assert unquote("'this'") == "this"
    assert unquote("'this'") == "this"
    assert unquote("‘this’") == "this"
    assert unquote("“this”") == "this"
    assert unquote("that")   == "that"

def test_clean_int():
    assert schema.clean_int('1') == 1
    assert schema.clean_int(' 2 ') == 2
    assert schema.clean_int(' 3  ') == 3

def test_convert_value():
    assert schema.convertValue(' 1', vtype='int') == 1
    assert schema.convertValue(' null', vtype='VARCHAR') == ""
    assert schema.convertValue(' test', vtype='VARCHAR') == 'test'
    assert schema.convertValue('one ', vtype='int') == 'one'


SQL_CREATE1="""    CREATE TABLE output (
    INTEGER StudentNumber,
    VARCHAR CourseNumber,
    VARCHAR CourseName
    );
"""

def test_sql_parse_create():
    sql = sql_parse_create(SQL_CREATE1)
    assert sql['table']=='output'
    assert sql['cols'][0]=={'vtype':'INTEGER','name':'StudentNumber'}
    assert sql['cols'][1]=={'vtype':'VARCHAR','name':'CourseNumber'}
    assert sql['cols'][2]=={'vtype':'VARCHAR','name':'CourseName'}

def test_schema_output(s):
    s_dict = s.json_dict()
    name = Variable(name="name",vtype='VARCHAR(4)',column=0,width=4,start=1,end=4)
    # check that the variable is in the table that is in the schema
    assert s_dict['tables']['students']['variables'][0] == name.json_dict()
    # move sys.stdout to variable for analysis
    sys_out = sys.stdout
    output = io.StringIO()
    sys.stdout = output
    s.dump()
    output = output.getvalue()
    sys.stdout = sys_out
    assert "Tables: 1" in output
    assert "variables:2" in output

def test_schema_code_snippet(s):
    """ Test methods related to adding and using code snippets """
    assert not s.has_code_snippet('test_snippet')
    snippet = CodeSnippet(name='test_snippet')
    s.add_code_snippet(snippet)
    assert s.has_code_snippet(snippet.name)
    new_snippet = s.get_code_snippet(snippet.name)
    assert new_snippet == snippet
    print(s.code_snippet_vals())
    # ordered dict is returned, so turn into list then get first value
    assert snippet == list(s.code_snippet_vals())[0]
    assert snippet.name == list(s.code_snippet_names())[0]
    s.del_code_snippet_named('test_snippet')
    assert not s.has_code_snippet('test_snippet')
    with pytest.raises(KeyError):
        s.get_code_snippet(name='test_snippet')
    new_snippet = s.get_code_snippet(name='test2', create=True)
    assert s.has_code_snippet(new_snippet.name)
    s.add_code_snippet_named(name='test3')
    assert s.has_code_snippet('test3')

def test_schema_tables(s):
    """ Test methods related to adding and using tables """
    assert s.has_table(name='students')
    out_table = s.get_table(name='students')
    s.del_table_named(name='students')
    assert not s.has_table(name='students')
    with pytest.raises(KeyError):
        s.get_table(name='students')
    new_table = s.get_table(name='test2', create=True)
    assert s.has_table(name=new_table.name)

    # ordered dict is returned, so turn into list then get first value
    assert new_table == list(s.tables())[0]
    assert new_table.name == list(s.table_names())[0]
    s.add_table_named(name='test3')
    assert s.has_table(name='test3')

def test_schema_read_file(s, tmpdir):
    """ Check that schema can read from a file and append data """
    test_file_contents = "Students\njohn,10\nsally,11\n"
    filename = f"{tmpdir}/test_schema.txt"
    with open(filename, 'w') as f:
        for line in test_file_contents.splitlines():
            print(line, file=f)
    # test that pandas can read .txt file
    pandas_output = s.get_pandas_file_reader(filename=filename)
    p_out = pandas_output.read()
    assert 'Students' in p_out.columns
    with pytest.raises(RuntimeError):
        s.get_pandas_file_reader("test.test")

    new_schema = Schema()
    new_schema.load_schema_from_file(filename=filename)
    assert new_schema.has_table(name='test_schema')


def test_schema_recode(s):
    """ Check schema can get recode information and manipulate it """
    s.add_recode(name='test1', vtype='INT', desc='students[x]=3')
    assert s.recode_names() == ['test1']
    s.compile_recodes()
    with pytest.raises(TypeError):
        s.recode_execute('students', 'error')
    s.recode_execute('student', 'returns')
    s.create_overrides(keyword='test_description')
