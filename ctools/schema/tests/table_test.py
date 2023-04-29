#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Test the Census ETL schema package

import os
import pytest
import sys

from os.path import abspath
from os.path import dirname

sys.path.append(dirname(dirname(dirname(dirname(abspath(__file__))))))

import ctools.schema as schema

from ctools.schema import valid_sql_name,unquote,sql_parse_create,decode_vtype
from ctools.schema.table import Table
from ctools.schema.schema import Schema
from ctools.schema.variable import Variable

# We should try parsing this too.
SCHEMA_TEST = """CREATE TABLE students (
   name VARCHAR -- ,
   age INTEGER --
);"""

DATALINE1="jack10"
DATALINE2="mary25"


@pytest.fixture
def tmpdir(tmp_path_factory):
    """ Create a tmp csv file for testing """
    string = "test1, 1\ntest2, 2\ntest3, 3"
    tmpdir = tmp_path_factory.mktemp("table")
    return tmpdir

@pytest.fixture
def table():
    s = Schema()
    t = Table(name="students")
    s.add_table(t)
    name = Variable(name="name",vtype='VARCHAR(4)',column=0,width=4,start=1,end=4)
    age  = Variable(name="age",vtype='INTEGER(2)',column=4,width=2,start=5,end=6)
    t.add_variable(name)
    t.add_variable(age)
    return t


def test_Table():
    s = Schema()
    t = Table(name="students")
    s.add_table(t)
    name = Variable(name="name",vtype='VARCHAR(4)',column=0,width=4,start=1,end=4)
    assert(name.python_type==str)
    age  = Variable(name="age",vtype='INTEGER(2)',column=4,width=2,start=5,end=6)
    t.add_variable(name)
    t.add_variable(age)
    assert name.column==0
    assert name.width==4
    assert age.column==4
    assert age.width==2

    assert t.get_variable("name") == name
    assert t.get_variable("age") == age
    assert list(t.vars()) == [name,age]

    # Try SQL conversion
    sql = t.sql_schema()
    assert "CREATE TABLE students" in sql
    assert "name VARCHAR" in sql
    assert "age INTEGER" in sql

    # See if the parsers work
    data = t.parse_line_to_dict(DATALINE1)
    assert data == {"name":"jack","age":10}
    assert t.parse_line_to_row(DATALINE1) == ["jack",10]

    # Add a second table
    t = Table(name="parents")
    s.add_table(t)
    t.add_variable(Variable(name="parent",vtype=schema.TYPE_VARCHAR))

    # See if adding a recode works
    s.add_recode("recode1",schema.TYPE_VARCHAR,"parents[studentname]=students[name]")
    s.add_recode("recode2",schema.TYPE_INTEGER,"parents[three]=3")
    s.add_recode("recode3",schema.TYPE_VARCHAR,"parents[student_initials]=students[name][0:1]")
    s.compile_recodes()

    # verify that the parents table now has a student name variable of the correct type
    assert s.get_table("parents").get_variable("studentname").name == "studentname"
    assert s.get_table("parents").get_variable("studentname").vtype == schema.TYPE_VARCHAR

    # Let's load a line of data for recoding
    s.recode_load_data("students",data)

    # Now record a parent record
    parent = {"name":"xxxx"}
    s.recode_execute("parents",parent)

    # Now make sure that the recoded data is there
    assert parent['studentname']=='jack'
    assert parent['three']==3
    assert parent['student_initials']=='j'


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


def test_csv(table, tmpdir):
    filename = f"{tmpdir}/test.csv"
    table.open_csv(filename)
    test_dict = {'name': 'john', 'age': 18}
    table.write_dict(test_dict)
    fail_dict = {'invalid': 'james', 'age': 18}
    with pytest.raises(KeyError):
        table.write_dict(fail_dict)
    table.csv_file.close()
    file = open(f"{tmpdir}/test.csv")
    output = file.readlines()
    assert output[0] == 'name,age\n'
    assert output[1] == 'john,18\n'


@pytest.mark.parametrize(
    "delimiter,line,out0,out1",
    [
        (',','roger,00,11,22,33','roger',33),
        (None,'test44','test',44)
    ])
def test_parse(table, delimiter, line, out0, out1):
    table.delimiter = delimiter
    output = table.parse_line_to_row(line)
    assert output[0] == out0
    assert output[1] == out1

    output = table.parse_line_to_dict(line)
    assert output['name'] == out0
    assert output['age'] == out1

    d_out = table.unparse_dict_to_line(output)
    assert d_out == out0 + ',' + str(out1)

def test_python_class(table):
    """ check that the python class can be generated without throwing errors """
    output = table.python_class()
    assert len(output) > 0

def test_table_sql(table):
    """ Check table can generate sql code """
    out = table.sql_prepare()
    grade = Variable(name="grade",vtype='VARCHAR(5)')
    table.add_variable(grade)
    with pytest.raises(RuntimeError):
        out = table.sql_prepare()
