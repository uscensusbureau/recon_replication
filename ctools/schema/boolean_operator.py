#!/usr/bin/env python3

import logging
from typing import Dict

class BooleanOperator:
    """
    Boolean Operator

    desc    = description of conditional.
    attrib  = a dictionary of user-specified attributes
    op_type = operator type
    """

    __slots__ = ('desc','attrib','op_type')

    def __init__(self, *, desc: str = "", attrib: Dict = {}, op_type: str = None) -> None:
        self.desc        = desc          # description
        self.attrib      = attrib
        self.set_type(op_type)

    def set_type(self, op_type: str) -> None:
        res = ""
        if op_type is None:
            res = None
        else:
            op_type = op_type.strip()
            if op_type in ["<=", ">=", "<", ">", "==", \
                "!=", "not", "and", "or"]:
                res = op_type
            else:
                raise ValueError(f'operator type {op_type} invalid')
        self.op_type = res

    def __eq__(self, other) -> bool:
        if isinstance(other, BooleanOperator):
            return self.op_type == other.op_type
        return False

    def __str__(self) -> str:
        if self.op_type is None:
            return ''
        return self.op_type

    def __repr__(self) -> str:
        return f'Boolean Operator(type: {self.op_type})'

    def json_dict(self) -> Dict:
        return {
                "desc": self.desc,
                "attrib": self.attrib,
                "type": self.op_type
               }

    def dump(self,func=print) -> None:
        func(str(self))


def main() -> None:
    operator = BooleanOperator(op_type="<=")
    print(operator)


if __name__ == '__main__':
    main()
