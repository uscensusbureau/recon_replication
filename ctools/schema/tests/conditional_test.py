
from ctools.schema.boolean_operator import BooleanOperator
from ctools.schema.boolean_expression import BooleanExpression
from ctools.schema.conditional import Conditional
import ctools.schema.boolean_operator as boolean_operator
import ctools.schema.boolean_expression as boolean_expression
import ctools.schema.conditional as conditional


import pytest

def test_make_conditional():
    """ Make a conditional and check that the method it would create is correct"""
    bo = BooleanOperator(op_type='>')
    be = BooleanExpression(first_element='a', second_element='b', operator=bo)
    cond = Conditional(condition=be)
    string = cond.__str__().splitlines()
    assert "if row['a'] > row['b']:" == string[0]
    assert "    pass" == string[1]
    assert "else:" == string[2]
    assert "    pass" == string[3]

def test_conditional_main():
    """ conditional has a main method, check it can run """
    boolean_operator.main()
    boolean_expression.main()
    conditional.main()

def test_boolean_operator():
    """ Test boolean_operator methods """
    with pytest.raises(ValueError):
        op = BooleanOperator(op_type='@')
    op = BooleanOperator(desc='less than',op_type='<')
    dict_out = op.json_dict()
    assert dict_out['desc'] == 'less than'
    assert dict_out['type'] == '<'
    op2 = BooleanOperator(op_type='==')
    assert not op == op2
    op2 = BooleanOperator(op_type='<')
    assert op == op2
    op2 = '<'
    assert not op == op2
    expected = 'Boolean Operator(type: <)'
    assert op.__repr__() == expected

def test_boolean_expression():
    """ Test boolean_expression methods """
    bo = BooleanOperator(op_type='>')
    be = BooleanExpression(first_element='a', second_element='b', operator=bo)
    be_dict = be.json_dict()
    assert be_dict['first_element'] == 'a'
    assert be_dict['second_element'] == 'b'
