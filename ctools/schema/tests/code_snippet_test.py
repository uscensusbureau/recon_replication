
from ctools.schema.code_snippet import CodeSnippet
from ctools.schema.conditional import Conditional
from ctools.schema.variable import Variable

import pytest


@pytest.fixture
def snippet():
    cond1 = Conditional()
    s = CodeSnippet(desc='for testing', name='test', expressions=[cond1])
    return s

def test_break_code_snippet_init():
    with pytest.raises(ValueError):
        CodeSnippet()
    with pytest.raises(ValueError):
        CodeSnippet(name='test name')

def test_add_var_to_code_snippet(snippet):
    snippet.add_variable('test_var')
    assert 'test_var' in snippet.variables
    snippet.set_validation_variable('test_var')
    assert 'test_var' == snippet.variable_to_validate

def test_add_expression_to_code_snippet(snippet):
    cond1 = Conditional()
    snippet.add_expression(cond1)
    assert cond1 in snippet.expressions
    with pytest.raises(TypeError):
        snippet.add_expression('break')

def test_dump(snippet):
    snippet.add_variable('test_var')
    str_out = snippet.__str__()
    assert "row['True']" in str_out
    assert "return row" in str_out
    repr_out = snippet.__repr__()
    assert "name: test" in repr_out
    dict_out = snippet.json_dict()
    assert dict_out['desc'] == 'for testing'
    assert dict_out['name'] == 'test'

def test_code_snippet_main():
    import ctools.schema.code_snippet as code_snippet
    code_snippet.main()
