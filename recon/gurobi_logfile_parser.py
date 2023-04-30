#!/usr/bin/env python3
#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)
"""
A module to parse through Gurobi logs. Part of the replication archive for The
U.S. Census Bureau's Ex Post Confidentiality Analysis of the 2010 Census Data
Publications (https://github.com/uscensusbureau/recon_replication)
"""

import re
from datetime import datetime

import dbrecon
from ctools.schema.table import Table


@dbrecon.Memoize
def compile(rexp):
    return re.compile(rexp)

class GurobiLogfileParser:
    def __init__(self, logfile=None, data=None):
        if logfile is None and data is None:
            raise ValueError("provide logfile or data")

        if logfile is not None and data is not None:
            raise ValueError("do not provide both logfile and data")

        if logfile is not None:
            data = open(logfile,"r").read()
        self.data = data
        self.lines = self.data.split("\n")

    def grep(self,rexp):
        """Find the last line that matches rexp and return it"""
        rexp_compiled = compile(rexp)
        for line in self.lines[::-1]:
            m = rexp_compiled.search(line)
            if m:
                return m
        return None

    def find(self,rexp):
        m = self.grep(rexp)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                pass
            try:
                return float(m.group(1))
            except ValueError:
                pass
            try:
                return m.group(1)
            except ValueError:
                pass
        return None

    @property
    def gurobi_version(self):
        return self.find(r'Gurobi ([0-9.]+)')

    @property
    def start(self):
        m = self.grep(r'logging started ... (.*)')
        if m:
            return datetime.strptime(m.group(1),"%b %d %H:%M:%S %Y")

    @property
    def rows(self):
        return self.find(r"Arbitrary_Objective_Function: (\d+) rows")

    @property
    def columns(self):
        return self.find(r"Arbitrary_Objective_Function: \d+ rows, (\d+) columns")

    @property
    def nonzeros(self):
        return self.find(r"Arbitrary_Objective_Function: \d+ rows, \d+ columns, (\d+) nonzeros")

    @property
    def presolve_rows(self):
        return self.find(r"Presolved: (\d+) rows")

    @property
    def presolve_columns(self):
        return self.find(r"Presolved: \d+ rows, (\d+) columns")

    @property
    def presolve_NZ(self):
        return self.find(r"Presolved: \d+ rows, \d+ columns, (\d+) nonzeros")

    @property
    def integer_vars(self):
        return self.find(r"Variable types: \d+ continuous, (\d+) integer")

    @property
    def binary_vars(self):
        return self.find(r"Variable types: \d+ continuous, \d+ integer \((\d+) binary\)")

    @property
    def simplex_iterations(self):
        return self.find(r"(\d+) simplex iterations")

    @property
    def seconds(self):
        return self.find(r"Explored.* in (\d+[.]\d+) seconds")

    @property
    def nodes(self):
        return self.find(r"Explored (\d+) node")

    @property
    def dict(self):
        ret = {field:getattr(self,field) for field in
               ['gurobi_version','rows','columns','nonzeros','presolve_rows','presolve_NZ',
                'integer_vars','binary_vars','simplex_iterations','seconds'] if getattr(self,field) is not None}
        ret['start'] = self.start.isoformat()[0:19]
        return ret

    def sql_table(self, name='glog'):
        return Table.FromDict(name=name,dict=self.dict)

    def sql_schema(self, name='glog', extra={}):
        return self.sql_table(name=name).sql_schema(extra=extra)

    def sql_insert(self, name='glog', dialect='mysql', extra={}):
        # return (cmd, args)
        cmd = self.sql_table(name=name).sql_insert(dialect='mysql', extra=extra.keys())
        args = list(self.dict.values()) + list(extra.values())
        assert len(self.dict) + len(extra) == len(args)
        return (cmd, args)
