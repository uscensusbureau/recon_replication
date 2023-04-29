import importlib
import os
import sys
import pytest

from os.path import abspath
from os.path import dirname

sys.path.append(dirname(dirname(dirname(dirname(abspath(__file__))))))

import ctools.schema as schema
from ctools.schema.variable import Variable
from ctools.schema import variable_assignment
from ctools.schema.range import Range

@pytest.fixture
def variable():
    v = Variable(name = 'test',
                 column = 10,
                 width = 2,
                 vtype = schema.TYPE_VARCHAR
                )
    return v


@pytest.fixture(scope="session")
def tempdir(tmp_path_factory):
    """ Create a tmp directory for testing """
    tmpdir = tmp_path_factory.mktemp("variable_test")
    return tmpdir


@pytest.fixture(scope="session")
def validator_path(tempdir):
    """
    Checks that the variable can create legal validation method
    then test the method to ensure the expected outcome happens
    """
    variable = Variable(name='test', column=10, width=2, vtype=schema.TYPE_VARCHAR)
    variable.add_valid_data_description("10-20 test1")
    file = open(f'{tempdir}/validator.py', 'w')
    # setup class for usage by test_validator()
    print(schema.SCHEMA_SUPPORT_FUNCTIONS, file=file)
    print("class Validator:", file=file)
    # test the creation of different types of validators
    print(variable.python_validator(), file=file)
    variable.name = 'test_any'
    variable.allow_any = True
    print(variable.python_validator(), file=file)
    variable.name = 'test_null'
    variable.allow_any = False
    variable.allow_null = True
    print(variable.python_validator(), file=file)
    variable.name = 'test_whitespace'
    variable.allow_null = False
    variable.set_allow_whitespace()
    print(variable.python_validator(), file=file)
    variable.name = 'test_int'
    variable.allow_whitespace = False
    variable.set_vtype('int')
    print(variable.python_validator(), file=file)
    file.close()
    # return path to file
    return f"{tempdir}/validator.py"

def test_Variable(variable):
    """
    test the variable can be created and the instantiated values are as expected    """
    assert variable.width == 2
    assert variable.sql_type() == 'VARCHAR(2)'
    variable.width = None
    assert variable.sql_type() == 'VARCHAR'

@pytest.mark.parametrize(
    "vtype,python_type,width_output,vtype_output,python_type_output",
    [
        ('int', None, 8, 'INT', int),
        (None, int, 8, 'INTEGER', int),
        (None, None, None, None, None)
    ])
def test_set_vtype(variable, vtype, python_type, width_output, vtype_output, python_type_output):
    """
    test that the vtype can be set correctly depending on if vtype or python_type is given
    """
    variable.set_vtype(vtype, python_type)
    assert variable.width == width_output
    assert variable.vtype == vtype_output
    assert variable.python_type == python_type_output


def test_json_dict_output(variable):
    """
    test that the json_dict output comes back as expected based off var input
    """
    variable.desc = 'test_description'
    variable.position = 3
    variable.set_column(4, 8)
    assert variable.column == 4
    assert variable.width == 5
    variable.ranges = [Range(5, 8)]
    output = variable.json_dict()
    assert output['name'] == variable.name
    assert output['position'] == variable.position
    assert output['position'] == 3
    assert output['desc'] == variable.desc
    assert output['column'] == variable.column


def test_variable_range(variable):
    """
    test that ranges are properly being set and output for variable
    """
    r1 = Range(8,11, "test_range")
    r2 = Range(12,15)
    # check that range can be added
    variable.add_range(r1)
    variable.find_default_from_allowable_range_descriptions("test_range")
    # check that add_range throws an error if using the same range
    assert variable.default == 8
    with pytest.raises(RuntimeError):
        variable.add_range(r1)
    variable.add_range(r2)
    variable.default = None
    variable.find_default_from_allowable_range_descriptions("test_range")
    assert variable.default == 8


def test_whitespace(variable):
    variable.python_type = int
    variable.allow_whitespace = False
    variable.set_allow_whitespace()
    assert variable.python_type == str
    assert variable.allow_whitespace == True

@pytest.mark.parametrize(
    "desc, ranges",
    [
    ("1-2 test1\n 3-4 test2", [Range(1,2,'test1'), Range(3,4,'test2')]),
    ("5-6 test3,7-8 test4", [Range(5,6,'test3'), Range(7,8,'test4')]),
    ("9-10 test5", [Range(9,10,'test5')])
    ])
def test_add_description(variable, desc, ranges):
    """
    Check that ranges are correctly added to variable range list with variety of inputs
    """
    variable.add_valid_data_description(desc)
    assert len(variable.ranges) > 0
    r_set = list(variable.ranges)
    r_set.sort()
    for i in range(len(variable.ranges)):
        assert r_set[i].__repr__() == ranges[i].__repr__()


def test_random_value(variable):
    """
    check that variable can create a random value
    """
    ran = variable.random_value()
    variable.add_valid_data_description("1-2 test1")
    ran = variable.random_value()
    variable.add_valid_data_description("3-4 test2")
    ran = variable.random_value()
    variable.set_vtype('int')
    ran = variable.random_value()



def test_basic_validation(variable):
    """
    Check that basic validation methods return expected strings
    """
    name_out = variable.python_validator_name()
    assert name_out == "is_valid_test"
    variable.add_valid_data_description("1-2 test1")
    variable.add_valid_data_description("3-4 test2")
    test_out = variable.python_validation_text()
    # order is not always maintained so check both
    assert test_out in ['1-2, 3-4', '3-4, 1-2']



@pytest.mark.parametrize(
    "func_name,test_input,expected_result",
    [
    ("test", "10", True),
    ("test", "11", True),
    ("test", "20", True),
    ("test", "21", False),
    ("test", "100", False),
    ("test", None, False),
    ("test_any", "gibberish", True),
    ("test_null", "10", True),
    ("test_null", None, True),
    ("test_whitespace", "10", True),
    ("test_whitespace", "  ", True),
    ("test_whitespace", "   ", False),
    ("test_int", 10, True),
    ("test_int", "10", True)
    ])
def test_validator(tempdir, validator_path, func_name, test_input, expected_result):
    """
    files created without error, now test the logic of the created files
    """
    # importlib is needed to grab dynamically generated file
    import importlib
    importlib.invalidate_caches()
    spec = importlib.util.spec_from_file_location("validator", f"{tempdir}/validator.py")
    val = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(val)
    validator = val.Validator()
    # now that validator is created, run test
    # this is needed when dynamically running functions
    func = getattr(validator, f"is_valid_{func_name}")
    assert func(test_input) == expected_result

def test_dump(variable):
    variable.position = 0
    variable.column = 0
    variable.default = 1
    variable.set_allow_whitespace()
    variable.allow_null = True
    variable.add_valid_data_description("1-2 test_range")
    dump = variable.dump()


def test_variable_assignment():
    """ check that variable assignment can run, examine its values """
    variable_assignment.main()
    var = Variable(name='test')
    assignment = variable_assignment.VariableAssignment(var, 5)
    assert assignment.value == 5
    with pytest.raises(RuntimeError):
        assignment.check_range()
    with pytest.raises(RuntimeError):
        assignment.z3_obj = 0
        assignment.check_range()
    with pytest.raises(ValueError):
        assignment.set_value(None)
    assignment.set_value(1)
    print(assignment.variable.python_type)
    assert assignment.value == 1
    with pytest.raises(ValueError):
        newvar = Variable(name=None)
        a = variable_assignment.VariableAssignment(newvar, 2)
